# Data quality report

Run `20261026T171039` on 655,404 standard rows. Critical failures stop the pipeline.

| Check | Severity | Status | Observed | Detail |
|---|---|---|---|---|
| reconciliation_complications_and_deaths | critical | PASS | raw 95,800 = standard 95,800 + rejected 0 |  |
| reconciliation_unplanned_hospital_visits | critical | PASS | raw 67,060 = standard 67,060 + rejected 0 |  |
| reconciliation_healthcare_associated_infections | critical | PASS | raw 172,440 = standard 172,440 + rejected 0 |  |
| reconciliation_hcahps_patient_survey | critical | PASS | raw 325,720 = standard 325,720 + rejected 0 |  |
| reconciliation_timely_and_effective_care | critical | PASS | raw 138,084 = standard 138,084 + rejected 0 |  |
| reject_rate_below_1pct | warning | PASS | 0 rejected rows |  |
| primary_key_unique | critical | PASS | 0 duplicate keys | key = provider_id + source_dataset + measure_id |
| provider_id_format | critical | PASS | 0 invalid | 6-character CMS Certification Number |
| score_type_assigned | critical | PASS | 0 rows without score_type |  |
| provider_in_dim_hospital | warning | PASS | 0 provider IDs not in dim_hospital |  |
| compared_to_national_mapped | critical | PASS | 0 unmapped rows |  |
| compared_label_variants_standardized | info | PASS | 14 raw labels -> 5 categories |  |
| score_within_range | warning | FAIL | 2 out of range | percent 0-100, star_rating 1-5, linear_mean 0-100, ordinal 1-4, ratio 0-100, minutes 0-2880 |
| confidence_interval_contains_score | warning | PASS | 0 rows | lower_estimate <= score <= higher_estimate |
| period_start_before_end | critical | PASS | 0 rows |  |
| hcahps_answer_shares_sum_to_100 | warning | PASS | 0 of 67,691 hospital-questions off by >3 pts | rounding tolerance 3 points |
| null_score_share_complications_and_deaths | info | PASS | 45% of 95,800 rows have no score | suppressed or not reported; footnote kept |
| null_score_share_hcahps_patient_survey | info | PASS | 24% of 325,720 rows have no score | suppressed or not reported; footnote kept |
| null_score_share_healthcare_associated_infections | info | PASS | 61% of 28,740 rows have no score | suppressed or not reported; footnote kept |
| null_score_share_timely_and_effective_care | info | PASS | 58% of 138,084 rows have no score | suppressed or not reported; footnote kept |
| null_score_share_unplanned_hospital_visits | info | PASS | 52% of 67,060 rows have no score | suppressed or not reported; footnote kept |
| data_freshness | warning | PASS | latest period_end 2025-09-30 |  |
