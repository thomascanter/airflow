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
import io
from pydub import AudioSegment

log = logging.getLogger(__name__)


def _extract_text_from_conf() -> str:
    context = get_current_context()
    run_conf = (context.get('dag_run') or {}).conf or {}
    text = run_conf.get('ollama_text')
    log.info(f"Extracted text: {text}")
    if not text:
        raise ValueError('No "text" found in dag_run.conf — POST must include {"text": "..."}')
    return text


def generate_with_ollama(*, ollama_url: Optional[str] = None, timeout: int = 300) -> str:
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


def synthesize_chunks(*, chatterbox_url: Optional[str] = None, voice: Optional[str] = None, timeout: int = 120) -> dict:
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

    # chunking
    max_chars = int(conf.get('chunk_size', 3000))
    chunks = []
    text = generated_text.strip()
    while text:
        if len(text) <= max_chars:
            chunks.append(text)
            break
        split_at = text.rfind(' ', 0, max_chars)
        if split_at <= 0:
            split_at = max_chars
        chunks.append(text[:split_at].strip())
        text = text[split_at:].strip()

    if not chunks:
        raise ValueError('No text chunks to synthesize')

    segments = []
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

            resp = requests.post(chatterbox_url, json=payload, timeout=timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.exception('Chatterbox request failed for chunk %d: %s', idx + 1, exc)
            raise

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
            elif 'wav' in subtype:
                fmt = 'wav'
            else:
                fmt = subtype
        else:
            fmt = conf.get('expected_audio_format') or 'wav'

        try:
            seg = AudioSegment.from_file(io.BytesIO(audio_bytes), format=fmt)
        except Exception:
            try:
                seg = AudioSegment.from_file(io.BytesIO(audio_bytes), format='wav')
            except Exception:
                seg = AudioSegment.from_file(io.BytesIO(audio_bytes), format='mp3')

        segments.append(seg)

    if not segments:
        raise RuntimeError('No audio segments produced')

    combined = segments[0]
    for s in segments[1:]:
        combined += s

    out_buf = io.BytesIO()
    combined.export(out_buf, format='mp3')
    out_buf.seek(0)
    final_bytes = out_buf.read()

    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    filename = conf.get('filename') or f'virtual_thomas_{timestamp}.mp3'

    return {'mp3_b64': base64.b64encode(final_bytes).decode('ascii'), 'filename': filename}


def upload_to_minio(*, timeout: int = 60) -> str:
    """Upload base64 MP3 bytes returned by `synthesize_chunks` to MinIO and return URL."""
    log.info('Starting upload_to_minio task')
    context = get_current_context()
    ti = context['ti']
    conf = (context.get('dag_run') or {}).conf or {}

    payload = ti.xcom_pull(task_ids='synthesize_chunks')
    if not payload or not isinstance(payload, dict):
        raise ValueError('Missing XCom payload from synthesize_chunks')

    mp3_b64 = payload.get('mp3_b64')
    filename = payload.get('filename')
    if not mp3_b64 or not filename:
        raise ValueError('synthesize_chunks did not return mp3_b64 and filename')

    final_bytes = base64.b64decode(mp3_b64)

    minio_bucket = Variable.get('minio_bucket') or ''
    minio_access_key_id = Variable.get('minio_username') or ''
    minio_secret_access_key = Variable.get('minio_password') or ''
    minio_endpoint = Variable.get('minio_endpoint') or ''

    if not minio_bucket:
        raise ValueError('Missing required DAG conf: minio_bucket')
    if not minio_access_key_id or not minio_secret_access_key:
        raise ValueError('Missing required DAG conf: minio_username and/or minio_password')

    key = f"{filename}"

    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=f"{minio_access_key_id}",
        aws_secret_access_key=f"{minio_secret_access_key}",
        config=Config(signature_version="s3v4"),
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

    log.info('Uploading combined MP3 to bucket %s with key %s', minio_bucket, key)
    s3.put_object(Bucket=minio_bucket, Key=key, Body=final_bytes, ContentType='audio/mp3')
    object_url = f"{minio_endpoint.rstrip('/')}/{minio_bucket}/{key}"
    log.info('Uploaded combined MP3 to %s', object_url)
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