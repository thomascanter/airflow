import base64
import logging
import os
from datetime import datetime
from typing import Optional

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from airflow.sdk import Variable

import tempfile
import subprocess
import time
import re


def combine_audio_parts(parts: list, output_path: Optional[str] = None, bitrate: str = '192k') -> str:
    """Concatenate audio files in `parts` (in order) into a single MP3 using ffmpeg.

    Returns the path to the combined MP3 file. Raises subprocess.CalledProcessError on failure.
    This function will create a temporary concat list file next to the first part and
    will remove the list file and the individual part files after combining. The
    combined output file is left in place for the caller to upload or remove.
    """
    if not parts:
        raise ValueError('No parts to combine')

    tmpdir = os.path.dirname(parts[0])
    if output_path is None:
        output_path = os.path.join(tmpdir, 'combined.mp3')

    list_path = os.path.join(tmpdir, 'parts.txt')
    with open(list_path, 'w', encoding='utf-8') as lf:
        for p in parts:
            # escape single quotes for ffmpeg concat file
            safe = p.replace("'", "'\\''")
            lf.write(f"file '{safe}'\n")

    cmd = [
        'ffmpeg', '-y', '-f', 'concat', '-safe', '0', '-i', list_path,
        '-c:a', 'libmp3lame', '-b:a', bitrate, output_path
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # remove parts and list file, keep output
    try:
        for p in parts:
            if os.path.exists(p):
                os.remove(p)
        if os.path.exists(list_path):
            os.remove(list_path)
    except Exception:
        log.warning('Failed to remove temporary part files in %s', tmpdir)

    return output_path

log = logging.getLogger(__name__)


def _extract_text_from_conf() -> str:
    context = get_current_context()
    run_conf = (context.get('dag_run') or {}).conf or {}
    text = run_conf.get('ollama_text')
    log.info(f"Extracted text: {text}")
    if not text:
        raise ValueError('No "text" found in dag_run.conf — POST must include {"text": "..."}')
    return text


def generate_with_ollama(*, ollama_url: Optional[str] = None, timeout: int = 600) -> str:
    context = get_current_context()
    text = _extract_text_from_conf()
    conf = (context.get('dag_run') or {}).conf or {}
    ollama_url = ollama_url or conf.get('ollama_url') or ''
    model = conf.get('ollama_model') or 'deepseek-r1:14b' # optional

    payload = {
        'model': model,
        'prompt': f"{text}",
        "stream": False
    }

    # Attempt POST to a common generate endpoint. Adjust if your Ollama API differs.
    try:
        ollama_url = f"{ollama_url.rstrip('/')}/api/generate"
        log.info('Sending generation request to Ollama at %s', ollama_url)
        log.info('Payload: %s', payload)
        resp = requests.post(ollama_url, json=payload, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.exception('Ollama request failed: %s', exc)
        raise

    # Try parse response for common fields
    try:
        data = resp.json()
    except Exception:
        # Fallback to raw text
        generated = resp.text.strip()
        log.info('Ollama returned raw text (non-JSON).')
        return generated

    # Search for plausible keys
    for key in ('generated_text', 'text', 'output', 'result', 'response'):
        if key in data:
            val = data[key]
            if isinstance(val, list):
                return '\n'.join(map(str, val))
            return str(val)

    # Some LLM APIs return choices
    if 'choices' in data and isinstance(data['choices'], list) and data['choices']:
        first = data['choices'][0]
        for key in ('text', 'message', 'output'):
            if key in first:
                return str(first[key])

    # As a last resort, serialize the JSON
    log.warning('Could not find generation text in Ollama response JSON; returning raw JSON string')
    return str(data)


def synthesize_chunks(*, chatterbox_url: Optional[str] = None, voice: Optional[str] = None, timeout: int = 1200) -> dict:
    """Synthesize text in chunks and return base64-encoded MP3 bytes and filename.

    Returns a dict compatible with XCom JSON serialization: {'mp3_b64': str, 'filename': str}
    """
    log.info("Starting synthesize_chunks task (chunked)")
    context = get_current_context()
    ti = context['ti']
    log.info("Pulling generated text from Ollama task XCom")
    generated_text = ti.xcom_pull(task_ids='generate_with_ollama')
    if not generated_text:
        raise ValueError('No generated text found from Ollama task XCom')

    log.info("Extracting configuration for Chatterbox TTS")
    conf = (context.get('dag_run') or {}).conf or {}
    log.info("Generating Chatterbox URL from conf or Variable")
    chatterbox_url = Variable.get('chatterbox_url') or ''
    voice = voice or conf.get('voice') or 'default'

    # chunking: split into sentences, then accumulate full sentences into chunks
    max_chars = int(conf.get('chunk_size', 3000))
    chunks = []
    text = generated_text.strip()

    # Find sentence-like units (naive regex: ends with . ! ? possibly followed by quote and whitespace)
    sentence_re = re.compile(r'.+?(?:[\.\!\?]["\']?)(?:\s+|$)', re.S)
    sentences = [m.group().strip() for m in sentence_re.finditer(text)]
    # If regex misses trailing text without terminal punctuation, append remainder
    consumed = sum(len(s) for s in sentences)
    if consumed < len(text):
        tail = text[consumed:].strip()
        if tail:
            sentences.append(tail)

    # Accumulate sentences into chunks without breaking sentences when possible
    current = ''
    for sent in sentences:
        if not current:
            # If single sentence is longer than max_chars, split it as a last resort
            if len(sent) > max_chars:
                remaining = sent
                while remaining:
                    if len(remaining) <= max_chars:
                        current = remaining.strip()
                        remaining = ''
                    else:
                        split_at = remaining.rfind(' ', 0, max_chars)
                        if split_at <= 0:
                            split_at = max_chars
                        chunks.append(remaining[:split_at].strip())
                        remaining = remaining[split_at:].strip()
                continue
            current = sent
            continue

        # Try to append sentence to current chunk
        if len(current) + 1 + len(sent) <= max_chars:
            current = current + ' ' + sent
        else:
            chunks.append(current.strip())
            # Start new chunk with this sentence (may itself be very long and be split above)
            if len(sent) > max_chars:
                remaining = sent
                while remaining:
                    if len(remaining) <= max_chars:
                        current = remaining.strip()
                        remaining = ''
                    else:
                        split_at = remaining.rfind(' ', 0, max_chars)
                        if split_at <= 0:
                            split_at = max_chars
                        chunks.append(remaining[:split_at].strip())
                        remaining = remaining[split_at:].strip()
            else:
                current = sent

    if current:
        chunks.append(current.strip())

    if not chunks:
        raise ValueError('No text chunks to synthesize')

    parts = []
    for idx, chunk in enumerate(chunks):
        log.info('Synthesizing chunk %d/%d (chars=%d)', idx + 1, len(chunks), len(chunk))
        payload = {'input': chunk, 'voice': voice}
        if 'chatterbox_exaggeration' in conf:
            payload['exaggeration'] = conf['chatterbox_exaggeration']
        if 'chatterbox_cfg_weight' in conf:
            payload['cfg_weight'] = conf['chatterbox_cfg_weight']
        if 'chatterbox_temperature' in conf:
            payload['temperature'] = conf['chatterbox_temperature']

        try:
            log.info('Requesting TTS from Chatterbox at %s', chatterbox_url)
            resp = requests.post(f"{chatterbox_url.rstrip('/')}/audio/speech", json=payload, timeout=timeout)

            # handle sync (200/201) or async accepted (202 -> poll)
            if resp.status_code in (200, 201):
                resp.raise_for_status()
                final_resp = resp
            elif resp.status_code == 202:
                # Chatterbox accepted the job but returned 202. Polling has been removed
                # due to timeout problems; try to extract immediate result (audio or audio_url)
                try:
                    j = resp.json()
                except Exception:
                    j = {}

                ct = resp.headers.get('Content-Type', '')
                # If response already contains audio bytes, accept it
                if ct.startswith('audio/'):
                    final_resp = resp
                # If response JSON includes audio or a URL to the audio, accept it
                elif isinstance(j, dict) and any(k in j for k in ('mp3_b64', 'audio_base64', 'audio_url')):
                    # If the JSON includes an audio URL, fetch it synchronously
                    if 'audio_url' in j and j.get('audio_url'):
                        r2 = requests.get(j['audio_url'], timeout=timeout)
                        r2.raise_for_status()
                        final_resp = r2
                    else:
                        # keep original response; downstream parsing will extract base64 fields
                        final_resp = resp
                else:
                    raise RuntimeError('Chatterbox accepted request but did not return immediate audio or audio_url; async polling disabled')
            else:
                resp.raise_for_status()
                final_resp = resp
        except requests.RequestException as exc:
            log.exception('Chatterbox request failed for chunk %d: %s', idx + 1, exc)
            raise

        # use final_resp (could be immediate or polled result)
        resp = final_resp
        content_type = resp.headers.get('Content-Type', '')
        audio_bytes = None
        if content_type.startswith('audio/'):
            audio_bytes = resp.content
        else:
            try:
                data = resp.json()
            except Exception:
                audio_bytes = resp.content
            log.info('Chatterbox response content-type: %s', content_type)
            if audio_bytes is None:
                if isinstance(data, dict) and 'audio_base64' in data:
                    audio_bytes = base64.b64decode(data['audio_base64'])
                elif isinstance(data, dict) and 'audio_url' in data:
                    r2 = requests.get(data['audio_url'], timeout=timeout)
                    r2.raise_for_status()
                    audio_bytes = r2.content
                else:
                    audio_bytes = str(data).encode('utf-8')
                if isinstance(data, dict):
                    # prefer explicit mp3_b64 key used by Chatterbox API
                    if 'mp3_b64' in data:
                        audio_bytes = base64.b64decode(data['mp3_b64'])
                        content_type = 'audio/mpeg'
                    elif 'audio_base64' in data:
                        audio_bytes = base64.b64decode(data['audio_base64'])
                    elif 'audio_url' in data:
                        r2 = requests.get(data['audio_url'], timeout=timeout)
                        r2.raise_for_status()
                        audio_bytes = r2.content
                    else:
                        audio_bytes = str(data).encode('utf-8')

        if not audio_bytes:
            raise RuntimeError(f'No audio produced for chunk {idx + 1}')

        fmt = None
        if content_type.startswith('audio/'):
            subtype = content_type.split('/', 1)[1].split(';', 1)[0]
            if subtype in ('mpeg', 'mp3'):
                fmt = 'mp3'
            else:
                fmt = subtype
        else:
            fmt = conf.get('expected_audio_format') or 'mp3'

        # write chunk to temp file for ffmpeg concat
        # choose extension from content type
        if content_type.startswith('audio/'):
            if 'mpeg' in content_type or 'mp3' in content_type:
                ext = 'mp3'
            else:
                ext = 'bin'
        else:
            # prefer mp3 by default for processed audio
            ext = conf.get('expected_audio_format') or 'mp3'

        tmpdir = None
        # create tmpdir once
        if not parts:
            tmpdir = tempfile.mkdtemp(prefix='virtual_thomas_')
        else:
            # reuse existing tmpdir from first part
            tmpdir = os.path.dirname(parts[0])

        part_path = os.path.join(tmpdir, f'chunk_{idx+1}.{ext}')
        with open(part_path, 'wb') as fh:
            fh.write(audio_bytes)
        parts.append(part_path)

    if not parts:
        raise RuntimeError('No audio segments produced')

    # create ffmpeg concat list file
    list_path = os.path.join(tmpdir, 'parts.txt')
    with open(list_path, 'w', encoding='utf-8') as lf:
        for p in parts:
            lf.write(f"file '{p.replace("'", "'\\'\"")}'\n")

    output_path = os.path.join(tmpdir, 'combined.mp3')
    # use ffmpeg to concat and encode to mp3
    try:
        log.info('Combining %d audio parts into final MP3 at %s', len(parts), output_path)
        cmd = [
            'ffmpeg', '-y', '-f', 'concat', '-safe', '0', '-i', list_path,
            '-c:a', 'libmp3lame', '-b:a', '192k', output_path
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(output_path, 'rb') as fh:
            final_bytes = fh.read()
    finally:
        # cleanup temp parts and list; ignore errors
        try:
            for p in parts:
                if os.path.exists(p):
                    os.remove(p)
            if os.path.exists(list_path):
                os.remove(list_path)
            if os.path.exists(output_path):
                # keep combined until uploaded; we'll remove after upload in upload_to_minio
                pass
        except Exception:
            log.warning('Failed to clean up temp files in %s', tmpdir)

    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    filename = conf.get('filename') or f'virtual_thomas_{timestamp}.mp3'

    # Return both the base64 (fallback) and the local path to the combined file so
    # downstream tasks can upload the file directly without storing large XComs.
    return {
        'filename': filename,
        'combined_path': output_path,
    }


def upload_to_minio(*, timeout: int = 60) -> str:
    """Upload base64 MP3 bytes returned by `synthesize_chunks` to MinIO and return URL."""
    log.info('Starting upload_to_minio task')
    context = get_current_context()
    ti = context['ti']
    conf = (context.get('dag_run') or {}).conf or {}

    payload = ti.xcom_pull(task_ids='synthesize_chunks')
    if not payload or not isinstance(payload, dict):
        raise ValueError('Missing XCom payload from synthesize_chunks')

    filename = payload.get('filename')
    combined_path = payload.get('combined_path')

    final_bytes = None
    # Prefer uploading the combined MP3 file produced in the temp directory
    if combined_path and os.path.exists(combined_path):
        log.info('Found combined file from synthesize_chunks at %s; uploading file directly', combined_path)
        with open(combined_path, 'rb') as fh:
            final_bytes = fh.read()
        # If filename missing, infer from path
        if not filename:
            filename = os.path.basename(combined_path)

    minio_bucket = Variable.get('minio_bucket') or ''
    minio_access_key_id = Variable.get('minio_username') or ''
    minio_secret_access_key = Variable.get('minio_password') or ''
    minio_endpoint = Variable.get('minio_endpoint') or ''

    if not minio_bucket:
        raise ValueError('Missing required DAG conf: minio_bucket')
    if not minio_access_key_id or not minio_secret_access_key:
        raise ValueError('Missing required DAG conf: minio_username and/or minio_password')

    key = f"{filename}"

    # Normalize and trim credentials to avoid stray whitespace issues
    minio_endpoint = (minio_endpoint or '').rstrip('/')
    access_key = minio_access_key_id.strip()
    secret_key = minio_secret_access_key.strip()

    # Masked logging for debugging (do not log secret)
    log.info('Using MinIO endpoint=%s access_key_prefix=%s has_secret=%s', minio_endpoint, access_key[:4] + '...', bool(secret_key))

    # Use S3v4 signing and path-style addressing which MinIO expects
    s3_config = Config(signature_version='s3v4', s3={'addressing_style': 'path'})
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=s3_config,
    )

    try:
        log.info('Checking for existence of bucket %s', minio_bucket)
        s3.head_bucket(Bucket=minio_bucket)
    except ClientError:
        try:
            log.info('Creating bucket %s', minio_bucket)
            s3.create_bucket(Bucket=minio_bucket)
        except ClientError:
            log.info('Bucket %s already exists or cannot be created', minio_bucket)
            pass

    object_url = f"{minio_endpoint.rstrip('/')}/{minio_bucket}/{key}"
    log.info('Uploading combined MP3 to bucket %s with key %s', minio_bucket, key)
    s3.put_object(Bucket=minio_bucket, Key=key, Body=final_bytes, ContentType='audio/mp3')

   
    log.info('Uploaded combined MP3 to %s', object_url)

    # Attempt to clean up the combined file and its temp directory
    try:
        if combined_path and os.path.exists(combined_path):
            tmpdir = os.path.dirname(combined_path)
            os.remove(combined_path)
            # remove tmpdir if empty
            try:
                if not os.listdir(tmpdir):
                    os.rmdir(tmpdir)
            except Exception:
                pass
    except Exception:
        log.warning('Failed to remove temporary combined file %s', combined_path)

    return object_url

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='virtual_thomas_audio',
    default_args=default_args,
    description='Generate text from Ollama then TTS via Chatterbox (trigger via API POST)',
    schedule=None,  # only run when triggered via the REST/API or UI
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['virtual', 'tts', 'ollama'],
) as dag:

    generate_with_ollama_task = PythonOperator(
        task_id='generate_with_ollama',
        python_callable=generate_with_ollama,
    )

    synthesize_chunks_task = PythonOperator(
        task_id='synthesize_chunks',
        python_callable=synthesize_chunks,
    )

    upload_to_minio_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )

    generate_with_ollama_task >> synthesize_chunks_task >> upload_to_minio_task