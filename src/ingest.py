"""Bronze layer: loads each raw CMS file as-is (all strings) into a Hive table in cms_raw.

Column names are made Hive-safe ('City/Town' -> 'city_town'); values are not changed.
Tables are partitioned by load_date so each monthly CMS refresh lands in its own partition.
"""
import datetime as dt

from pyspark.sql import functions as F

from common import DATABASES, LOAD_DATE, raw_path, spark_session
from mapping import load_config, raw_name


def main():
    cfg = load_config()
    load_date = LOAD_DATE or dt.date.today().isoformat()
    spark = spark_session("cms-ingest")
    for name, src in {**cfg["reference_tables"], **cfg["sources"]}.items():
        path = raw_path(name, src["file"], load_date)
        df = spark.read.csv(path, header=True, inferSchema=False, multiLine=True, escape='"')
        df = df.toDF(*[raw_name(c) for c in df.columns])
        df = (df.withColumn("_source_file", F.lit(path))
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("load_date", F.lit(load_date)))
        table = f"{DATABASES['raw']}.{name}"
        df.write.mode("overwrite").format("parquet").partitionBy("load_date").saveAsTable(table)
        print(f"{table:50s} {spark.table(table).where(F.col('load_date') == load_date).count():>9,} rows  <- {path}")
    spark.stop()


if __name__ == "__main__":
    main()
