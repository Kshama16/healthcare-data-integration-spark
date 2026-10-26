#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export HADOOP_CONF_DIR="$ROOT/conf/hadoop"
export HADOOP_LOG_DIR="$HOME/.hdfs-local/logs"
hdfs --daemon stop datanode || true
hdfs --daemon stop namenode || true
