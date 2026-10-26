#!/usr/bin/env bash
# Starts a single-node HDFS (NameNode + DataNode) using the configs in conf/hadoop.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export HADOOP_CONF_DIR="$ROOT/conf/hadoop"
export HADOOP_LOG_DIR="$HOME/.hdfs-local/logs"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"

if [ ! -d "$HOME/.hdfs-local/name/current" ]; then
  echo "Formatting NameNode (first run)"
  hdfs namenode -format -nonInteractive -force >/dev/null 2>&1
fi
hdfs --daemon start namenode || true
hdfs --daemon start datanode || true
hdfs dfsadmin -safemode wait
hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity"
