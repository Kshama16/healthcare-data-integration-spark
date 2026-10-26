# Healthcare Data Integration Pipeline (HDFS · Hive · PySpark · Spark SQL)

**Portfolio project | Data integration / healthcare data engineering**

An ETL pipeline that lands six public CMS hospital quality files (≈805K rows, 211 MB) in HDFS, loads them into Hive tables, and uses PySpark to map five differently shaped datasets into **one standard quality-measure format**. Every run is checked with data quality rules and reconciled row for row, then HiveQL marts are built with the `spark-sql` CLI. The pipeline runs from Bash or PowerShell.

```
CMS Provider Data API
   │  src/download.py
   ▼
HDFS  /data/cms/raw/<source>/load_date=YYYY-MM-DD/   ← scripts/hdfs_land.sh (hdfs dfs -put, size check)
   │  src/ingest.py (PySpark)
   ▼
Hive cms_raw.*        bronze: values as delivered, Hive-safe names, load metadata
   │  src/transform.py + src/mapping.py (PySpark, driven by conf/source_mappings.json)
   ▼
Hive cms_std.quality_measure   standard format, partitioned by source  ─┐
     cms_std.quality_measure_rejects / dim_hospital / dim_measure       │  src/validate.py
   │  sql/marts.hql (HiveQL via spark-sql)                              └→ cms_std.dq_results, docs/dq_report.md
   ▼
Hive cms_mart.hospital_quality_profile / state_benchmark / measure_national_benchmark → output/*.csv
```

## The mapping problem

The five CMS quality datasets describe the same thing (a hospital's result on a measure) in five different shapes:

| Source | Raw rows | What makes it hard | Standard rows |
|---|---|---|---|
| Complications & deaths | 95,800 | 8 "compared to national" label variants | 95,800 |
| Unplanned hospital visits | 67,060 | Labels in different case ("Number of cases too small") | 67,060 |
| Healthcare-associated infections | 172,440 | Each measure is spread over 6 rows (`HAI_1_SIR`, `_CILOWER`, `_CIUPPER`, `_NUMERATOR`, `_ELIGCASES`, `_DOPC`) → **pivoted to 1 row** | 28,740 |
| Patient survey (HCAHPS) | 325,720 | The value sits in one of 3 columns (star rating, answer %, linear mean) depending on the measure | 325,720 |
| Timely & effective care | 138,084 | Mix of percents, minutes and rates; ED volume scored as text ("low" … "very high") | 138,084 |
| **Total** | **799,104** | | **655,404** |

All of it is driven by `conf/source_mappings.json`, so a renamed CMS column is a config change. The full source-to-target spec is in [docs/data_mapping.md](docs/data_mapping.md), and the requirements, acceptance criteria and non-functional requirements are in [docs/requirements.md](docs/requirements.md).

## Data quality

22 checks run on every load ([latest report](docs/dq_report.md)). Critical failures stop the pipeline with a non-zero exit code.

- **Reconciliation (critical):** for each source, raw rows = standard rows (counting collapsed sub-rows) + rejected rows. Current run: all 5 sources reconcile exactly, with 0 rejects.
- **Keys and formats (critical):** unique hospital × dataset × measure key; 6-character CMS Certification Number; every row has a score type; period start ≤ end.
- **Mapping completeness (critical):** 14 raw "compared to national" labels map to 5 categories; an unseen label fails the run instead of disappearing.
- **Plausibility (warning):** score ranges by type, confidence interval contains the score, and HCAHPS answer shares for a question add to 100% ±3 (all 67,691 hospital-questions pass).
- **Found in the data:** two psychiatric ED median wait times of 50 and 114 hours (OP_18c). They are flagged for source review, not deleted.

Rows that fail a reject rule go to `cms_std.quality_measure_rejects` with the reason and the full raw record as JSON.

## Results: Massachusetts vs. US acute care hospitals

| | Massachusetts (55) | United States (3,107) |
|---|---|---|
| Average CMS overall star rating | 3.16 | 3.16 |
| Average patient experience stars (HCAHPS) | 2.94 | 3.11 |
| Median ED time before leaving, minutes (OP_18b) | **215** | 170 |
| Sepsis bundle compliance, % (SEP_1) | **53.9** | 63.6 |
| Average infection SIR (lower is better) | 0.580 | 0.541 |
| Share of compared measures worse than national | 2.59% | 1.94% |

Massachusetts hospitals match the national average on CMS's overall stars. But they trail on emergency department throughput (third-longest ED median among states with 20+ acute care hospitals, behind Puerto Rico and Maryland, tied with New York) and on sepsis bundle compliance. Hospital-level detail is in `output/ma_hospital_quality_profile.csv`.

## Run it

Requirements: Java 17, Python 3.11+, Hadoop 3.x (for HDFS mode), PowerShell 7 (optional).

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
scripts/hdfs_start.sh                         # single-node HDFS (configs in conf/hadoop)
scripts/run_pipeline.sh                       # HDFS → Hive → standard → DQ → marts → CSVs
pwsh ./scripts/run_pipeline.ps1 -Fs hdfs      # same pipeline from PowerShell
PIPELINE_FS=local scripts/run_pipeline.sh     # no HDFS: reads data/raw directly
.venv/bin/python -m pytest -q tests           # 9 unit tests for the mapping rules
scripts/spark_sql.sh -e "SELECT * FROM cms_mart.state_benchmark WHERE state = 'MA'"
```

Useful HDFS commands while it runs:

```bash
export HADOOP_CONF_DIR=conf/hadoop
hdfs dfs -ls -R /data/cms/raw | head
hdfs dfs -du -s -h /user/hive/warehouse/*.db
```

A full run takes about 75 seconds on an 8-core laptop.

## Project structure

```
conf/source_mappings.json   source → standard mapping, null tokens, label crosswalk, score ranges
conf/hadoop/                single-node HDFS config
src/mapping.py              mapping logic (pure PySpark functions, unit tested)
src/ingest.py               HDFS CSV → bronze Hive tables
src/transform.py            bronze → standard table, rejects, dimensions
src/validate.py             data quality checks
src/report.py               mart exports
sql/marts.hql               HiveQL reporting marts
scripts/                    hdfs_start/stop, hdfs_land, spark_sql, run_pipeline (.sh and .ps1)
docs/                       requirements, data mapping spec, data quality report
tests/                      pytest unit tests
```

## Data source

[CMS Provider Data Catalog](https://data.cms.gov/provider-data/), Hospitals topic (public, no PHI): Hospital General Information, Complications and Deaths, Unplanned Hospital Visits, Healthcare Associated Infections, Patient Survey (HCAHPS) and Timely and Effective Care. Releases from July to September 2026.

---

Built by [Kshama Upadhyay](https://github.com/Kshama16). Related project: [hospital-quality-benchmarking](https://github.com/Kshama16/hospital-quality-benchmarking) (DuckDB star schema and benchmarking on three of these files).
