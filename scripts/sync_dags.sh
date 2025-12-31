#!/usr/bin/env bash
set -u

# Ensure we're running under bash; if not, re-exec with bash when available.
if [ -z "${BASH_VERSION-}" ]; then
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  else
    echo "This script requires bash. Please run it with bash." >&2
    exit 1
  fi
fi

usage() {
  cat <<EOF
Usage: $(basename "$0") -s <source> -d <destination> [-r] [-f <filter>] [-n]
  -s Source directory (required)
  -d Destination directory (required)
  -r Recurse into subdirectories
  -f Filter (glob, default '*')
  -n Dry run (show actions only)
EOF
  exit 2
}

SRC=""
DEST=""
RECURSE=0
FILTER='*'
DRYRUN=0

while getopts ":s:d:rf:n" opt; do
  case $opt in
    s) SRC="$OPTARG" ;;
    d) DEST="$OPTARG" ;;
    r) RECURSE=1 ;;
    f) FILTER="$OPTARG" ;;
    n) DRYRUN=1 ;;
    *) usage ;;
  esac
done

if [[ -z "$SRC" || -z "$DEST" ]]; then usage; fi

# Resolve absolute paths (realpath fallback to python)
resolve_abs() {
  if command -v realpath >/dev/null 2>&1; then
    realpath "$1"
  else
    python -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$1"
  fi
}

SRC_ABS=$(resolve_abs "$SRC") || { echo "Source path not found: $SRC" >&2; exit 2; }
DEST_ABS=$(resolve_abs "$DEST" 2>/dev/null || true)
if [[ -z "$DEST_ABS" ]]; then
  # create destination if it doesn't exist then resolve
  mkdir -p "$DEST" || { echo "Cannot create destination: $DEST" >&2; exit 3; }
  DEST_ABS=$(resolve_abs "$DEST")
fi

# Ensure trailing slash for simpler relpath computation
SRC_ABS="${SRC_ABS%/}/"
DEST_ABS="${DEST_ABS%/}/"

# Build find parameters
if [[ $RECURSE -eq 1 ]]; then
  FIND_DEPTH_PARAM=()
else
  FIND_DEPTH_PARAM=(-maxdepth 1)
fi

# Use find with -name and null-separated output to handle special chars
# Populate files array in a portable way: prefer mapfile, fall back to read loop.
files=()

if type mapfile >/dev/null 2>&1; then
  # use find directly in process substitution to avoid storing redirections in an array
  mapfile -d '' -t files < <(find "$SRC_ABS" "${FIND_DEPTH_PARAM[@]}" -type f -name "$FILTER" -print0 2>/dev/null)
else
  # fallback for shells without mapfile
  while IFS= read -r -d '' file; do
    files+=("$file")
  done < <(find "$SRC_ABS" "${FIND_DEPTH_PARAM[@]}" -type f -name "$FILTER" -print0 2>/dev/null)
fi

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No files found in source matching filter."
  exit 0
fi

copied=0
skipped=0
errors=0

for f in "${files[@]}"; do
  # compute relative path by stripping the source prefix
  if [[ "${f#"${SRC_ABS}"}" != "$f" ]]; then
    rel="${f#${SRC_ABS}}"
  else
    echo "Warning: Skipping file outside base source: $f" >&2
    ((skipped++))
    continue
  fi

  destFile="$DEST_ABS$rel"
  needCopy=0

  if [[ ! -e "$destFile" ]]; then
    needCopy=1
  else
    # compute CRC32 via cksum (first field)
    if ! src_crc=$(cksum -- "$f" 2>/dev/null | awk '{print $1}'); then
      echo "Failed reading file: $f" >&2
      ((errors++)); continue
    fi
    if ! dst_crc=$(cksum -- "$destFile" 2>/dev/null | awk '{print $1}'); then
      echo "Failed reading destination file: $destFile" >&2
      ((errors++)); continue
    fi
    if [[ "$src_crc" != "$dst_crc" ]]; then
      needCopy=1
    fi
  fi

  if [[ $needCopy -eq 1 ]]; then
    if [[ $DRYRUN -eq 1 ]]; then
      echo "DRY-RUN: Would copy '$f' -> '$destFile'"
      ((copied++))
    else
      destDir=$(dirname -- "$destFile")
      if [[ ! -d "$destDir" ]]; then
        if ! mkdir -p -- "$destDir"; then
          echo "Failed to create directory: $destDir" >&2
          ((errors++)); continue
        fi
      fi
      if cp -p -- "$f" "$destFile"; then
        echo "Copied: '$f' -> '$destFile'"
        ((copied++))
      else
        echo "Failed to copy '$f' -> '$destFile'" >&2
        ((errors++))
      fi
    fi
  else
    ((skipped++))
  fi
done

echo "Summary: Copied=$copied Skipped=$skipped Errors=$errors"
if [[ $errors -gt 0 ]]; then exit 1; else exit 0; fi