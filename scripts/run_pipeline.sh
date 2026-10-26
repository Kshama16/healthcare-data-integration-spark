#!/usr/bin/env bash
# End-to-end pipeline: download -> land in HDFS -> bronze -> standard -> data quality -> marts -> report.
# Usage: scripts/run_pipeline.sh            (HDFS, default)
#        PIPELINE_FS=local scripts/run_pipeline.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PIPELINE_FS="${PIPELINE_FS:-hdfs}"
export LOAD_DATE="${LOAD_DATE:-$(date +%F)}"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
export HADOOP_CONF_DIR="$ROOT/conf/hadoop"
export PYSPARK_PYTHON="$ROOT/.venv/bin/python"
export SPARK_HOME="${SPARK_HOME:-$("$PYSPARK_PYTHON" -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')}"

step() { printf "\n== %s ==\n" "$1"; }
submit() { "$SPARK_HOME/bin/spark-submit" --conf spark.ui.enabled=false --driver-java-options "-Dlog4j.configurationFile=$ROOT/conf/spark/log4j2.properties" --py-files "$ROOT/src/common.py,$ROOT/src/mapping.py" "$ROOT/src/$1" 2> >(grep -v -E "WARN|log4j|incubator|FutureWarning|require_minimum" >&2); }

cd "$ROOT"
step "1. Download CMS files (if missing)"
ls data/raw/*.csv >/dev/null 2>&1 || "$PYSPARK_PYTHON" src/download.py

if [ "$PIPELINE_FS" = "hdfs" ]; then
  step "2. Land raw files in HDFS (load_date=$LOAD_DATE)"
  scripts/hdfs_land.sh
fi

step "3. Bronze: raw files -> Hive tables (cms_raw)"
submit ingest.py
step "4. Standard: map sources to the standard format (cms_std)"
submit transform.py
step "5. Data quality checks (critical failures stop the run)"
submit validate.py
step "6. Marts: HiveQL via spark-sql (cms_mart)"
scripts/spark_sql.sh -f sql/marts.hql >/dev/null
step "7. Export report tables"
submit report.py
