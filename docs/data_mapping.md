# Source-to-target mapping

Target: `cms_std.quality_measure` (Parquet, partitioned by `source_dataset`). Config:
`conf/source_mappings.json`. All sources: `Facility ID` → `provider_id` (must match `^[0-9A-Z]{6}$`);
`Start Date`/`End Date` (MM/dd/yyyy) → `period_start`/`period_end` (date); "Not Available",
"Not Applicable", "N/A" and blanks → null.

## Target columns

| Column | Type | Complications & deaths | Unplanned visits | Infections (HAI) | Patient survey (HCAHPS) | Timely & effective care |
|---|---|---|---|---|---|---|
| domain | string | `outcomes` | `readmissions` | `safety` | `patient_experience` | `process` |
| measure_id | string | Measure ID | Measure ID | Measure ID minus suffix (`HAI_1_SIR` → `HAI_1`) | HCAHPS Measure ID | Measure ID |
| measure_name | string | Measure Name | Measure Name | Measure Name of the `_SIR` row | HCAHPS Question | Measure Name |
| category | string | – | – | – | – | Condition |
| score_type | string | `rate` | `rate` | `ratio` (SIR) | `star_rating` / `percent` / `linear_mean`, from the column that applies | `percent`; `minutes` for OP_18*; `rate` for HH_*; `ordinal` for EDV |
| score | double | Score | Score | Score of `_SIR` row | the applicable value column | Score; EDV text low/medium/high/very high → 1–4 |
| score_text | string | original score text, kept for audit | | | | |
| lower_estimate | double | Lower Estimate | Lower Estimate | Score of `_CILOWER` row | – | – |
| higher_estimate | double | Higher Estimate | Higher Estimate | Score of `_CIUPPER` row | – | – |
| numerator | double | – | Number of Patients Returned | Score of `_NUMERATOR` row (observed infections) | – | – |
| denominator | double | Denominator | Denominator | Score of `_ELIGCASES` row (predicted infections) | Number of Completed Surveys | Sample |
| compared_to_national | string | standardized (below) | standardized | standardized, from `_SIR` row | – | – |
| compared_to_national_raw | string | original label | | | | |
| footnote | string | Footnote | Footnote | Footnote of `_SIR` row | footnote of the applicable value column | Footnote |
| source_row_count | int | 1 | 1 | rows collapsed (normally 6) | 1 | 1 |

Not carried to the standard table: hospital address fields (kept once in `dim_hospital`), HAI `_DOPC`
rows (device/patient days, only used by CMS to compute the SIR), HCAHPS survey response rate.

## Compared-to-national labels

Matched case-insensitively. The current files contain 14 distinct labels.

| Standard | Raw labels |
|---|---|
| better | Better Than the National Rate · Better Than the National Value · Better than the National Benchmark · Better than expected |
| no_different | No Different Than the National Rate · No Different Than the National Value · No Different than National Benchmark · No Different than expected |
| worse | Worse Than the National Rate · Worse Than the National Value · Worse than the National Benchmark · Worse than expected |
| too_few_cases | Number of Cases Too Small (also "Number of cases too small") |
| not_available | Not Available |

## Reject rules

A row goes to `cms_std.quality_measure_rejects` (with `reject_reason` and the raw record as JSON) when:
`invalid_provider_id`, `missing_measure_id`, `non_numeric_score` (text that is neither a placeholder
nor a known category), `invalid_or_missing_period`, or `unknown_submeasure` (HAI suffix not in config).
