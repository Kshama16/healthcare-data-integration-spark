#!/usr/bin/env bash
# Lands the raw CMS files in HDFS: /data/cms/raw/<source>/load_date=<date>/<file>
# Re-running for the same load date overwrites that partition (idempotent).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOAD_DATE="${LOAD_DATE:-$(date +%F)}"
BASE=/data/cms/raw
export HADOOP_CONF_DIR="$ROOT/conf/hadoop"

for f in "$ROOT"/data/raw/*.csv; do
  file="$(basename "$f")"
  source="${file%.csv}"
  dest="$BASE/$source/load_date=$LOAD_DATE"
  hdfs dfs -mkdir -p "$dest"
  hdfs dfs -put -f "$f" "$dest/$file"

  # Verify the copy: byte size in HDFS must match the local file.
  local_bytes=$(stat -f %z "$f")
  hdfs_bytes=$(hdfs dfs -stat %b "$dest/$file")
  if [ "$local_bytes" != "$hdfs_bytes" ]; then
    echo "Size mismatch for $file: local $local_bytes, HDFS $hdfs_bytes" >&2
    exit 1
  fi
  printf "%-45s %12s bytes -> %s\n" "$file" "$hdfs_bytes" "$dest"
done

hdfs dfs -du -s -h "$BASE"
hdfs dfs -count "$BASE"
