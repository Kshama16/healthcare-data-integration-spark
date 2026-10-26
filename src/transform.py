"""Standard layer: maps every bronze source to the standard quality-measure format.

Writes (all in cms_std, Parquet):
  quality_measure          standard rows, partitioned by source_dataset
  quality_measure_rejects  rows that could not be mapped, with reason and the raw record
  dim_hospital             one row per hospital (Hospital General Information)
  dim_measure              one row per source dataset x measure
"""
from functools import reduce

from pyspark.sql import functions as F

from common import DATABASES, LOAD_DATE, spark_session
from mapping import clean, load_config, map_source


def latest(spark, table):
    load_date = LOAD_DATE or spark.table(table).agg(F.max("load_date")).first()[0]
    return spark.table(table).where(F.col("load_date") == load_date).drop("_source_file", "_ingested_at")


def main():
    cfg = load_config()
    spark = spark_session("cms-transform")
    std, raw_db = DATABASES["std"], DATABASES["raw"]

    goods, bads = [], []
    for name in cfg["sources"]:
        good, bad = map_source(latest(spark, f"{raw_db}.{name}"), name, cfg)
        goods.append(good)
        bads.append(bad)
    quality = reduce(lambda a, b: a.unionByName(b), goods)
    rejects = reduce(lambda a, b: a.unionByName(b), bads)

    quality.write.mode("overwrite").format("parquet").partitionBy("source_dataset") \
        .saveAsTable(f"{std}.quality_measure")
    rejects.write.mode("overwrite").format("parquet").saveAsTable(f"{std}.quality_measure_rejects")

    hosp = latest(spark, f"{raw_db}.hospital_general_information")
    c = lambda col: clean(col, cfg)
    dim_hospital = hosp.select(
        c("facility_id").alias("provider_id"),
        c("facility_name").alias("hospital_name"),
        c("city_town").alias("city"),
        c("state").alias("state"),
        c("zip_code").alias("zip_code"),
        c("county_parish").alias("county"),
        c("hospital_type").alias("hospital_type"),
        c("hospital_ownership").alias("ownership"),
        (c("emergency_services") == "Yes").alias("has_emergency_services"),
        c("hospital_overall_rating").try_cast("int").alias("overall_star_rating"),
    ).dropDuplicates(["provider_id"])
    dim_hospital.write.mode("overwrite").format("parquet").saveAsTable(f"{std}.dim_hospital")

    dim_measure = (spark.table(f"{std}.quality_measure")
                   .groupBy("source_dataset", "domain", "measure_id")
                   .agg(F.max("measure_name").alias("measure_name"),
                        F.max("category").alias("category"),
                        F.max("score_type").alias("score_type"),
                        F.countDistinct("provider_id").alias("hospitals_reporting")))
    dim_measure.write.mode("overwrite").format("parquet").saveAsTable(f"{std}.dim_measure")

    for t in ["quality_measure", "quality_measure_rejects", "dim_hospital", "dim_measure"]:
        print(f"{std}.{t:30s} {spark.table(f'{std}.{t}').count():>9,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
