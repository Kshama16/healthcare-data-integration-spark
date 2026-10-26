"""Exports the marts to output/ as CSV and prints the Massachusetts vs. US comparison."""
from common import ROOT, spark_session

OUT = ROOT / "output"

QUERIES = {
    "state_benchmark": "SELECT * FROM cms_mart.state_benchmark ORDER BY state",
    "ma_hospital_quality_profile": """
        SELECT * FROM cms_mart.hospital_quality_profile
        WHERE state = 'MA' AND hospital_type = 'Acute Care Hospitals'
        ORDER BY pct_better DESC NULLS LAST, hospital_name""",
    "measure_national_benchmark": "SELECT * FROM cms_mart.measure_national_benchmark ORDER BY source_dataset, measure_id",
    "dq_results_latest": """
        SELECT * FROM cms_std.dq_results
        WHERE run_id = (SELECT MAX(run_id) FROM cms_std.dq_results)""",
}

MA_VS_US = """
SELECT CASE WHEN area_key = 'MA' THEN 'Massachusetts' ELSE 'United States' END AS area,
       COUNT(*)                                            AS acute_care_hospitals,
       ROUND(AVG(overall_star_rating), 2)                  AS avg_overall_stars,
       ROUND(AVG(patient_experience_stars), 2)             AS avg_patient_experience_stars,
       ROUND(PERCENTILE_APPROX(ed_median_minutes, 0.5), 0) AS median_ed_minutes,
       ROUND(AVG(sepsis_care_pct), 1)                      AS avg_sepsis_care_pct,
       ROUND(AVG(avg_infection_sir), 3)                    AS avg_infection_sir,
       ROUND(SUM(measures_worse) / NULLIF(SUM(measures_compared), 0) * 100, 2) AS pct_measures_worse
FROM cms_mart.hospital_quality_profile
LATERAL VIEW EXPLODE(IF(state = 'MA', ARRAY('MA', 'US'), ARRAY('US'))) t AS area_key
WHERE hospital_type = 'Acute Care Hospitals'
GROUP BY CASE WHEN area_key = 'MA' THEN 'Massachusetts' ELSE 'United States' END
"""


def main():
    spark = spark_session("cms-report")
    OUT.mkdir(exist_ok=True)
    for name, sql in QUERIES.items():
        df = spark.sql(sql).toPandas()
        df.to_csv(OUT / f"{name}.csv", index=False)
        print(f"output/{name}.csv  {len(df):,} rows")
    summary = spark.sql(MA_VS_US).toPandas()
    summary.to_csv(OUT / "ma_vs_us_summary.csv", index=False)
    print(summary.set_index("area").T.to_string())
    spark.stop()


if __name__ == "__main__":
    main()
