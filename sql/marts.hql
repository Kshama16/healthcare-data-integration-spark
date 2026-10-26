-- Reporting marts built from the standard layer (HiveQL, run with spark-sql -f).
-- Rebuilt in full on every pipeline run.

USE cms_mart;

-- National distribution of each measure: who reports it and what "typical" looks like.
DROP TABLE IF EXISTS measure_national_benchmark;
CREATE TABLE measure_national_benchmark STORED AS PARQUET AS
SELECT
  m.source_dataset,
  m.domain,
  m.measure_id,
  m.measure_name,
  m.score_type,
  COUNT(q.score)                              AS hospitals_with_score,
  ROUND(PERCENTILE_APPROX(q.score, 0.25), 2)  AS p25,
  ROUND(PERCENTILE_APPROX(q.score, 0.50), 2)  AS median,
  ROUND(PERCENTILE_APPROX(q.score, 0.75), 2)  AS p75,
  SUM(CASE WHEN q.compared_to_national = 'better' THEN 1 ELSE 0 END) AS hospitals_better,
  SUM(CASE WHEN q.compared_to_national = 'worse'  THEN 1 ELSE 0 END) AS hospitals_worse
FROM cms_std.dim_measure m
JOIN cms_std.quality_measure q
  ON q.source_dataset = m.source_dataset AND q.measure_id = m.measure_id
GROUP BY m.source_dataset, m.domain, m.measure_id, m.measure_name, m.score_type;

-- One row per hospital combining all five sources.
DROP TABLE IF EXISTS hospital_quality_profile;
CREATE TABLE hospital_quality_profile STORED AS PARQUET AS
WITH compared AS (
  SELECT provider_id,
         SUM(CASE WHEN compared_to_national IN ('better', 'no_different', 'worse') THEN 1 ELSE 0 END) AS measures_compared,
         SUM(CASE WHEN compared_to_national = 'better' THEN 1 ELSE 0 END) AS measures_better,
         SUM(CASE WHEN compared_to_national = 'worse'  THEN 1 ELSE 0 END) AS measures_worse
  FROM cms_std.quality_measure
  GROUP BY provider_id
),
key_measures AS (
  SELECT provider_id,
         MAX(CASE WHEN measure_id = 'H_STAR_RATING' THEN score END) AS patient_experience_stars,
         MAX(CASE WHEN measure_id = 'OP_18b'        THEN score END) AS ed_median_minutes,
         MAX(CASE WHEN measure_id = 'SEP_1'         THEN score END) AS sepsis_care_pct,
         AVG(CASE WHEN source_dataset = 'healthcare_associated_infections' THEN score END) AS avg_infection_sir
  FROM cms_std.quality_measure
  GROUP BY provider_id
)
SELECT
  h.provider_id, h.hospital_name, h.city, h.state, h.hospital_type, h.ownership,
  h.overall_star_rating,
  c.measures_compared, c.measures_better, c.measures_worse,
  ROUND(c.measures_better / NULLIF(c.measures_compared, 0) * 100, 1) AS pct_better,
  ROUND(c.measures_worse  / NULLIF(c.measures_compared, 0) * 100, 1) AS pct_worse,
  k.patient_experience_stars, k.ed_median_minutes, k.sepsis_care_pct,
  ROUND(k.avg_infection_sir, 3) AS avg_infection_sir
FROM cms_std.dim_hospital h
LEFT JOIN compared c     ON c.provider_id = h.provider_id
LEFT JOIN key_measures k ON k.provider_id = h.provider_id;

-- State benchmark for acute care hospitals.
DROP TABLE IF EXISTS state_benchmark;
CREATE TABLE state_benchmark STORED AS PARQUET AS
SELECT
  state,
  COUNT(*)                                             AS acute_care_hospitals,
  ROUND(AVG(overall_star_rating), 2)                   AS avg_overall_stars,
  ROUND(AVG(patient_experience_stars), 2)              AS avg_patient_experience_stars,
  ROUND(PERCENTILE_APPROX(ed_median_minutes, 0.5), 0)  AS median_ed_minutes,
  ROUND(AVG(sepsis_care_pct), 1)                       AS avg_sepsis_care_pct,
  ROUND(AVG(avg_infection_sir), 3)                     AS avg_infection_sir,
  ROUND(SUM(measures_worse) / NULLIF(SUM(measures_compared), 0) * 100, 2) AS pct_measures_worse
FROM hospital_quality_profile
WHERE hospital_type = 'Acute Care Hospitals'
GROUP BY state;
