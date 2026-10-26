#!/usr/bin/env bash
# Runs a HiveQL file with the spark-sql CLI against the pipeline's Hive metastore.
# Usage: scripts/spark_sql.sh -f sql/marts.hql   |   scripts/spark_sql.sh -e "SHOW TABLES IN cms_std"
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FS="${PIPELINE_FS:-local}"
HDFS_URI="${HDFS_URI:-hdfs://localhost:9000}"
if [ "$FS" = "hdfs" ]; then WAREHOUSE="$HDFS_URI/user/hive/warehouse"; else WAREHOUSE="$ROOT/spark-warehouse"; fi
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
export PYSPARK_PYTHON="$ROOT/.venv/bin/python"
export SPARK_HOME="${SPARK_HOME:-$("$PYSPARK_PYTHON" -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')}"
"$SPARK_HOME/bin/spark-sql" \
  --conf spark.sql.warehouse.dir="$WAREHOUSE" \
  --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=$ROOT/.metastore/metastore_db;create=true" \
  --conf spark.hadoop.fs.defaultFS="$( [ "$FS" = "hdfs" ] && echo "$HDFS_URI" || echo "file:///" )" \
  --conf spark.ui.enabled=false \
  --silent "$@" 2> >(grep -v -E "WARN|log4j|incubator" >&2)
