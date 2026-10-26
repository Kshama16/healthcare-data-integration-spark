# Requirements

Written before the build and used to accept it. Each acceptance criterion points to the test
(`tests/test_mapping.py`) or data quality check (`src/validate.py`) that proves it.

## Business requirement

Quality analysts need one table of CMS hospital quality results that they can query across
datasets. Today each CMS file has its own layout, column names, placeholder text and labels, so
every comparison starts with one-off cleanup.

## User stories and acceptance criteria

**US-1. As an analyst, I want all five CMS quality datasets in one standard format, so I can compare any measure across hospitals.**
- AC-1.1 Every source maps to the same 19-column schema (`STD_COLUMNS`). → `test_standard_layout_maps_and_types`
- AC-1.2 One row per hospital × dataset × measure. → check `primary_key_unique`
- AC-1.3 Infection measures spread over 6 sub-rows become one row with SIR, CI, numerator and predicted cases. → `test_hai_submeasures_pivot_to_one_row`
- AC-1.4 HCAHPS values are read from whichever of the three value columns applies. → `test_hcahps_picks_the_applicable_value_column`
- AC-1.5 Text scores ("low" … "very high") become ordinal numbers; minutes, rates, percents and ratios are labeled with `score_type`. → `test_timely_care_score_types_and_categorical_scores`

**US-2. As an analyst, I want "compared to national" results in consistent categories, so I can count better/worse hospitals across datasets.**
- AC-2.1 All label variants (any case) map to `better`, `no_different`, `worse`, `too_few_cases`, `not_available`. → `test_compared_labels_case_insensitive_and_unmapped_flagged`
- AC-2.2 A new, unknown label stops the pipeline instead of being dropped silently. → check `compared_to_national_mapped` (critical)

**US-3. As a data owner, I want to trust that nothing is lost or invented between the raw files and the standard table.**
- AC-3.1 Raw rows = standard rows (counting collapsed sub-rows) + rejected rows, per source. → check `reconciliation_*` (critical), `test_every_raw_row_is_accounted_for`
- AC-3.2 Rows that cannot be mapped go to `quality_measure_rejects` with a reason and the full raw record. → `test_invalid_rows_go_to_rejects_with_reason`
- AC-3.3 CMS placeholders ("Not Available") become nulls, not rejects, and the footnote is kept. → `test_placeholder_score_is_null_not_rejected`
- AC-3.4 Out-of-range values are flagged for review, not deleted. → check `score_within_range` (warning)

**US-4. As a quality lead, I want ready-made benchmark tables by hospital, state and measure.**
- AC-4.1 `cms_mart.hospital_quality_profile`, `state_benchmark` and `measure_national_benchmark` are rebuilt on every run. → `sql/marts.hql`

## Non-functional requirements

| ID | Requirement | How it is met |
|---|---|---|
| NFR-1 | Re-runnable: running the same load date twice gives the same result | HDFS `put -f` into a `load_date=` partition; tables written with overwrite |
| NFR-2 | Traceable: every bronze row records its source file and load time | `_source_file`, `_ingested_at`, `load_date` columns |
| NFR-3 | Configurable: a column rename in a CMS file is a config change, not a code change | `conf/source_mappings.json` |
| NFR-4 | Portable: runs on HDFS or the local file system, from Bash or PowerShell | `PIPELINE_FS`, `run_pipeline.sh`, `run_pipeline.ps1` |
| NFR-5 | Fails loudly: a critical data quality failure stops the run with a non-zero exit code | `validate.py` exit code; runners stop on error |
| NFR-6 | Runtime: full run (≈800K raw rows, 211 MB) in under 5 minutes on a laptop | ≈75 seconds end to end on an 8-core laptop |
