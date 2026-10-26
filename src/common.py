"""Shared Spark session and path settings for the pipeline jobs.

PIPELINE_FS=hdfs (default in scripts/run_pipeline.sh) reads raw files from HDFS and keeps
the Hive warehouse on HDFS; PIPELINE_FS=local runs the same jobs against data/raw.
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
FS = os.environ.get("PIPELINE_FS", "local")
HDFS_URI = os.environ.get("HDFS_URI", "hdfs://localhost:9000")
LOAD_DATE = os.environ.get("LOAD_DATE")  # set by the runner; defaults to today in ingest

HDFS_RAW = "/data/cms/raw"
DATABASES = {"raw": "cms_raw", "std": "cms_std", "mart": "cms_mart"}


def warehouse_dir():
    return f"{HDFS_URI}/user/hive/warehouse" if FS == "hdfs" else str(ROOT / "spark-warehouse")


def raw_path(source, file, load_date):
    if FS == "hdfs":
        return f"{HDFS_URI}{HDFS_RAW}/{source}/load_date={load_date}/{file}"
    return str(ROOT / "data" / "raw" / file)


def spark_session(app):
    metastore = ROOT / ".metastore" / "metastore_db"
    builder = (SparkSession.builder.appName(app)
               .master(os.environ.get("SPARK_MASTER", "local[*]"))
               .config("spark.sql.warehouse.dir", warehouse_dir())
               .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                       f"jdbc:derby:;databaseName={metastore};create=true")
               .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
               .config("spark.sql.shuffle.partitions", "16")
               .enableHiveSupport())
    if FS == "hdfs":
        builder = builder.config("spark.hadoop.fs.defaultFS", HDFS_URI)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for db in DATABASES.values():
        if not spark.catalog.databaseExists(db):
            spark.sql(f"CREATE DATABASE {db}")
    return spark
