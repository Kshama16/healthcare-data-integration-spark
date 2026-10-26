"""Unit tests for the source-to-standard mappings, using small hand-built raw tables."""
from mapping import STD_COLUMNS, map_source, raw_name

DATES = {"Start Date": "10/01/2024", "End Date": "09/30/2025"}


def bronze(spark, rows):
    """Builds a bronze-style frame (all strings, Hive-safe column names) from raw-header dicts."""
    cols = list(rows[0])
    schema = ", ".join(f"{raw_name(c)} string" for c in cols)
    return spark.createDataFrame([[r[c] for c in cols] for r in rows], schema)


def standard_row(**over):
    row = {"Facility ID": "220071", "Measure ID": "MORT_30_PN", "Measure Name": "Pneumonia death rate",
           "Compared to National": "No Different Than the National Rate", "Denominator": "512",
           "Score": "16.2", "Lower Estimate": "14.1", "Higher Estimate": "18.6", "Footnote": None, **DATES}
    row.update(over)
    return row


def test_standard_layout_maps_and_types(spark, cfg):
    good, bad = map_source(bronze(spark, [standard_row()]), "complications_and_deaths", cfg)
    r = good.collect()[0]
    assert good.columns == STD_COLUMNS and bad.count() == 0
    assert (r.score, r.lower_estimate, r.denominator) == (16.2, 14.1, 512.0)
    assert r.compared_to_national == "no_different"
    assert str(r.period_start) == "2024-10-01" and r.domain == "outcomes"


def test_compared_labels_case_insensitive_and_unmapped_flagged(spark, cfg):
    rows = [standard_row(**{"Measure ID": "A", "Compared to National": "Number of cases too small"}),
            standard_row(**{"Measure ID": "B", "Compared to National": "Better than expected"}),
            standard_row(**{"Measure ID": "C", "Compared to National": "Somewhat better"})]
    good, _ = map_source(bronze(spark, rows), "complications_and_deaths", cfg)
    got = {r.measure_id: r.compared_to_national for r in good.collect()}
    assert got == {"A": "too_few_cases", "B": "better", "C": "UNMAPPED"}


def test_placeholder_score_is_null_not_rejected(spark, cfg):
    good, bad = map_source(bronze(spark, [standard_row(Score="Not Available")]), "complications_and_deaths", cfg)
    assert bad.count() == 0 and good.collect()[0].score is None


def test_invalid_rows_go_to_rejects_with_reason(spark, cfg):
    rows = [standard_row(**{"Facility ID": "22OO7"}), standard_row(Score="see note"),
            standard_row(**{"Start Date": "2024-10-01"})]
    good, bad = map_source(bronze(spark, rows), "complications_and_deaths", cfg)
    reasons = sorted(r.reject_reason for r in bad.collect())
    assert good.count() == 0
    assert reasons == ["invalid_or_missing_period", "invalid_provider_id", "non_numeric_score"]
    assert any('"facility_id":"22OO7"' in r.raw_record for r in bad.collect())


def test_hai_submeasures_pivot_to_one_row(spark, cfg):
    values = {"SIR": "0.280", "CILOWER": "0.071", "CIUPPER": "0.761", "NUMERATOR": "3", "ELIGCASES": "10.733", "DOPC": "10192"}
    rows = [{"Facility ID": "010001", "Measure ID": f"HAI_1_{k}", "Measure Name": f"CLABSI {k}",
             "Compared to National": "Better than the National Benchmark", "Score": v, "Footnote": None, **DATES}
            for k, v in values.items()]
    good, bad = map_source(bronze(spark, rows), "healthcare_associated_infections", cfg)
    r = good.collect()[0]
    assert good.count() == 1 and bad.count() == 0
    assert (r.measure_id, r.measure_name, r.score_type) == ("HAI_1", "CLABSI SIR", "ratio")
    assert (r.score, r.lower_estimate, r.higher_estimate, r.numerator, r.denominator) == (0.28, 0.071, 0.761, 3.0, 10.733)
    assert r.compared_to_national == "better" and r.source_row_count == 6


def test_hai_unknown_submeasure_rejected(spark, cfg):
    rows = [{"Facility ID": "010001", "Measure ID": "HAI_1_XYZ", "Measure Name": "x",
             "Compared to National": None, "Score": "1", "Footnote": None, **DATES}]
    good, bad = map_source(bronze(spark, rows), "healthcare_associated_infections", cfg)
    assert good.count() == 0 and bad.collect()[0].reject_reason == "unknown_submeasure"


def hcahps_row(measure, star="Not Applicable", pct="Not Applicable", linear="Not Applicable"):
    return {"Facility ID": "220071", "HCAHPS Measure ID": measure, "HCAHPS Question": "q",
            "HCAHPS Answer Description": "a", "Patient Survey Star Rating": star,
            "Patient Survey Star Rating Footnote": None, "HCAHPS Answer Percent": pct,
            "HCAHPS Answer Percent Footnote": None, "HCAHPS Linear Mean Value": linear,
            "Number of Completed Surveys": "1200", "Number of Completed Surveys Footnote": None,
            "Survey Response Rate Percent": "20", "Survey Response Rate Percent Footnote": None, **DATES}


def test_hcahps_picks_the_applicable_value_column(spark, cfg):
    rows = [hcahps_row("H_COMP_1_STAR_RATING", star="4"), hcahps_row("H_COMP_1_A_P", pct="76"),
            hcahps_row("H_COMP_1_LINEAR_SCORE", linear="91"), hcahps_row("H_STAR_RATING", star="Not Available")]
    good, bad = map_source(bronze(spark, rows), "hcahps_patient_survey", cfg)
    got = {r.measure_id: (r.score_type, r.score) for r in good.collect()}
    assert bad.count() == 0
    assert got == {"H_COMP_1_STAR_RATING": ("star_rating", 4.0), "H_COMP_1_A_P": ("percent", 76.0),
                   "H_COMP_1_LINEAR_SCORE": ("linear_mean", 91.0), "H_STAR_RATING": ("star_rating", None)}


def test_timely_care_score_types_and_categorical_scores(spark, cfg):
    base = {"Facility ID": "220071", "Condition": "Emergency Department", "Measure Name": "m",
            "Sample": "300", "Footnote": None, **DATES}
    rows = [{**base, "Measure ID": "EDV", "Score": "very high"}, {**base, "Measure ID": "OP_18b", "Score": "215"},
            {**base, "Measure ID": "SEP_1", "Score": "61"}, {**base, "Measure ID": "HH_HYPO", "Score": "0.85"}]
    good, bad = map_source(bronze(spark, rows), "timely_and_effective_care", cfg)
    got = {r.measure_id: (r.score_type, r.score, r.category) for r in good.collect()}
    assert bad.count() == 0
    assert got["EDV"] == ("ordinal", 4.0, "Emergency Department")
    assert got["OP_18b"][:2] == ("minutes", 215.0)
    assert got["SEP_1"][:2] == ("percent", 61.0) and got["HH_HYPO"][:2] == ("rate", 0.85)


def test_every_raw_row_is_accounted_for(spark, cfg):
    rows = [standard_row(**{"Measure ID": f"M{i}"}) for i in range(5)] + [standard_row(**{"Facility ID": "bad"})]
    good, bad = map_source(bronze(spark, rows), "complications_and_deaths", cfg)
    assert good.groupBy().sum("source_row_count").first()[0] + bad.count() == len(rows)
