"""Data quality checks on the standard layer.

Each check has a severity: critical checks fail the pipeline (exit code 1), warnings are
reported for follow-up, info checks profile the data. Results are written to
cms_std.dq_results (one row per check per run) and docs/dq_report.md.
"""
import datetime as dt
import sys

from pyspark.sql import functions as F

from common import DATABASES, LOAD_DATE, ROOT, spark_session
from mapping import PROVIDER_ID_PATTERN, load_config

results = []


def check(name, severity, passed, observed, detail=""):
    results.append({"check": name, "severity": severity, "status": "PASS" if passed else "FAIL",
                    "observed": str(observed), "detail": detail})


def main():
    cfg = load_config()
    spark = spark_session("cms-validate")
    std, raw_db = DATABASES["std"], DATABASES["raw"]
    q = spark.table(f"{std}.quality_measure").cache()
    rej = spark.table(f"{std}.quality_measure_rejects")
    hosp = spark.table(f"{std}.dim_hospital")

    # 1. Row reconciliation: every raw row lands in the standard table or the rejects table.
    std_counts = {r["source_dataset"]: r["n"] for r in
                  q.groupBy("source_dataset").agg(F.sum("source_row_count").alias("n")).collect()}
    rej_counts = {r["source_dataset"]: r["n"] for r in rej.groupBy("source_dataset").agg(F.count("*").alias("n")).collect()}
    for name in cfg["sources"]:
        raw = spark.table(f"{raw_db}.{name}")
        load_date = LOAD_DATE or raw.agg(F.max("load_date")).first()[0]
        n_raw = raw.where(F.col("load_date") == load_date).count()
        n_std, n_rej = std_counts.get(name, 0), rej_counts.get(name, 0)
        check(f"reconciliation_{name}", "critical", n_raw == n_std + n_rej,
              f"raw {n_raw:,} = standard {n_std:,} + rejected {n_rej:,}")

    total = q.count()
    n_rej = rej.count()
    check("reject_rate_below_1pct", "warning", n_rej <= 0.01 * (total + n_rej), f"{n_rej:,} rejected rows")

    # 2. Keys and formats.
    dupes = q.groupBy("provider_id", "source_dataset", "measure_id").count().where("count > 1").count()
    check("primary_key_unique", "critical", dupes == 0, f"{dupes:,} duplicate keys",
          "key = provider_id + source_dataset + measure_id")
    bad_ids = q.where(~F.col("provider_id").rlike(PROVIDER_ID_PATTERN)).count()
    check("provider_id_format", "critical", bad_ids == 0, f"{bad_ids:,} invalid", "6-character CMS Certification Number")
    no_type = q.where(F.col("score_type").isNull()).count()
    check("score_type_assigned", "critical", no_type == 0, f"{no_type:,} rows without score_type")

    # 3. Referential integrity to the hospital dimension.
    orphans = q.join(hosp, "provider_id", "left_anti").select("provider_id").distinct().count()
    check("provider_in_dim_hospital", "warning", orphans == 0, f"{orphans:,} provider IDs not in dim_hospital")

    # 4. Mapping completeness.
    unmapped = q.where(F.col("compared_to_national") == "UNMAPPED")
    labels = [r[0] for r in unmapped.select("compared_to_national_raw").distinct().limit(10).collect()]
    check("compared_to_national_mapped", "critical", not labels, f"{unmapped.count():,} unmapped rows",
          "; ".join(labels))
    raw_labels = q.select("compared_to_national_raw").distinct().where(F.col("compared_to_national_raw").isNotNull()).count()
    check("compared_label_variants_standardized", "info", True,
          f"{raw_labels} raw labels -> {len(cfg['compared_to_national'])} categories")

    # 5. Value ranges by score type.
    out_of_range = 0
    for stype, (lo, hi) in cfg["score_ranges"].items():
        out_of_range += q.where((F.col("score_type") == stype) & ~F.col("score").between(lo, hi)).count()
    check("score_within_range", "warning", out_of_range == 0, f"{out_of_range:,} out of range",
          ", ".join(f"{k} {v[0]}-{v[1]}" for k, v in cfg["score_ranges"].items()))
    ci_bad = q.where(F.col("lower_estimate").isNotNull() & F.col("higher_estimate").isNotNull() & F.col("score").isNotNull()
                     & ~((F.col("lower_estimate") <= F.col("score")) & (F.col("score") <= F.col("higher_estimate")))).count()
    check("confidence_interval_contains_score", "warning", ci_bad == 0, f"{ci_bad:,} rows",
          "lower_estimate <= score <= higher_estimate")
    periods = q.where(F.col("period_start") > F.col("period_end")).count()
    check("period_start_before_end", "critical", periods == 0, f"{periods:,} rows")

    # 6. HCAHPS answer shares for one question should add up to ~100%.
    hc = (q.where((F.col("source_dataset") == "hcahps_patient_survey") & (F.col("score_type") == "percent"))
          .withColumn("question", F.regexp_replace("measure_id", r"_(A|SN|U|N|Y)_P$|_(0_6|7_8|9_10)$|_(DN|DY|PY)$", ""))
          .groupBy("provider_id", "question").agg(F.sum("score").alias("total"), F.count("score").alias("n"))
          .where("n >= 2"))
    n_q = hc.count()
    off = hc.where(F.abs(F.col("total") - 100) > 3).count()
    check("hcahps_answer_shares_sum_to_100", "warning", off <= 0.001 * n_q,
          f"{off:,} of {n_q:,} hospital-questions off by >3 pts", "rounding tolerance 3 points")

    # 7. Profiling.
    for r in (q.groupBy("source_dataset").agg(F.count("*").alias("n"), F.sum(F.col("score").isNull().cast("int")).alias("nulls"))
              .orderBy("source_dataset").collect()):
        check(f"null_score_share_{r['source_dataset']}", "info", True,
              f"{r['nulls'] / r['n']:.0%} of {r['n']:,} rows have no score", "suppressed or not reported; footnote kept")
    latest_end = q.agg(F.max("period_end")).first()[0]
    check("data_freshness", "warning", latest_end >= dt.date.today() - dt.timedelta(days=730), f"latest period_end {latest_end}")

    run_id = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    df = spark.createDataFrame([{**r, "run_id": run_id} for r in results])
    df.write.mode("append").format("parquet").saveAsTable(f"{std}.dq_results")
    write_report(run_id, total)
    spark.stop()
    failed = [r for r in results if r["severity"] == "critical" and r["status"] == "FAIL"]
    print(f"{len(results)} checks: {sum(r['status'] == 'PASS' for r in results)} pass, "
          f"{len(failed)} critical failures")
    sys.exit(1 if failed else 0)


def write_report(run_id, total):
    lines = [f"# Data quality report", "", f"Run `{run_id}` on {total:,} standard rows. "
             "Critical failures stop the pipeline.", "",
             "| Check | Severity | Status | Observed | Detail |", "|---|---|---|---|---|"]
    lines += [f"| {r['check']} | {r['severity']} | {r['status']} | {r['observed']} | {r['detail']} |" for r in results]
    (ROOT / "docs" / "dq_report.md").write_text("\n".join(lines) + "\n")
    for r in results:
        print(f"[{r['status']}] {r['severity']:8s} {r['check']:45s} {r['observed']}")


if __name__ == "__main__":
    main()
