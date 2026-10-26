# Changelog

## v1.0.0 (2026-10-26)

First release.
- HDFS landing with size verification, partitioned by load date.
- Bronze Hive tables for 6 CMS files (≈805K raw rows).
- Config-driven mapping of 5 quality datasets into one standard table (655,404 rows), plus rejects, `dim_hospital` and `dim_measure`.
- 22 data quality checks with critical/warning/info severities; results stored in `cms_std.dq_results`.
- HiveQL marts run with `spark-sql`: hospital profile, state benchmark, national measure benchmark.
- Bash and PowerShell runners; 9 unit tests.

Known data issues (flagged, not changed): two psychiatric ED median times (OP_18c) above 48 hours.
