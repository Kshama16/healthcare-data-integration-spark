"""Maps raw CMS source tables to the standard quality-measure format.

Every function here takes and returns Spark DataFrames and depends only on the mapping
config (conf/source_mappings.json), so the logic can be unit tested without HDFS or Hive.

Standard format: one row per hospital x source dataset x measure (see STD_COLUMNS and
docs/data_mapping.md). Rows that cannot be mapped go to a rejects table with a reason
instead of being silently dropped.
"""
import json
import re
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

CONFIG_PATH = Path(__file__).resolve().parents[1] / "conf" / "source_mappings.json"

STD_COLUMNS = [
    "provider_id", "source_dataset", "domain", "measure_id", "measure_name", "category",
    "score_type", "score", "score_text", "lower_estimate", "higher_estimate", "numerator",
    "denominator", "compared_to_national", "compared_to_national_raw", "footnote",
    "period_start", "period_end", "source_row_count",
]
REJECT_COLUMNS = ["source_dataset", "provider_id", "measure_id", "reject_reason", "raw_record"]
PROVIDER_ID_PATTERN = r"^[0-9A-Z]{6}$"


def load_config(path=CONFIG_PATH):
    with open(path) as f:
        return json.load(f)


def raw_name(col):
    """Column name as stored in the bronze (raw) Hive tables: 'City/Town' -> 'city_town'."""
    return re.sub(r"[^0-9a-z]+", "_", col.lower()).strip("_")


def clean(col, cfg):
    """Trims a raw string and turns CMS placeholder tokens ('Not Available', ...) into null."""
    c = F.trim(F.col(col)) if isinstance(col, str) else F.trim(col)
    return F.when(c.isin(cfg["null_tokens"]) | c.isNull(), F.lit(None)).otherwise(c)


def to_double(c):
    return c.try_cast("double")


def to_date(col, cfg):
    return F.to_date(F.try_to_timestamp(clean(col, cfg), F.lit(cfg["date_format"])))


def compared_to_national(raw_col, cfg):
    """Maps the 14+ label variants CMS uses (case varies by file) to 5 standard categories.

    A non-null label missing from the config maps to 'UNMAPPED' so validation can catch it.
    """
    pairs = [(label.lower(), cat) for cat, labels in cfg["compared_to_national"].items() for label in labels]
    lookup = F.create_map(*[F.lit(x) for pair in pairs for x in pair])
    key = F.lower(F.trim(raw_col))
    return F.when(raw_col.isNull() | (F.trim(raw_col) == ""), F.lit(None)).otherwise(
        F.coalesce(lookup[key], F.lit("UNMAPPED")))


def score_type_expr(measure_id, src):
    expr = F.lit(src["score_type"])
    for pattern, stype in reversed(list(src.get("score_type_overrides", {}).items())):
        expr = F.when(measure_id.rlike(pattern), F.lit(stype)).otherwise(expr)
    return expr


def _with_rejects(df, cfg, score_text_col, score_col):
    """Adds reject_reason (empty string = row is valid)."""
    reasons = [
        F.when(~F.coalesce(F.col("provider_id").rlike(PROVIDER_ID_PATTERN), F.lit(False)), F.lit("invalid_provider_id")),
        F.when(F.col("measure_id").isNull(), F.lit("missing_measure_id")),
        F.when(score_text_col.isNotNull() & score_col.isNull(), F.lit("non_numeric_score")),
        F.when(F.col("period_start").isNull() | F.col("period_end").isNull(), F.lit("invalid_or_missing_period")),
    ]
    return df.withColumn("reject_reason", F.concat_ws(",", *reasons))


def _rejects(df, name):
    """Rows with a reject reason, keeping the original raw record as JSON for follow-up."""
    raw_cols = [c for c in df.columns if c.startswith("raw__")]
    return (df.filter(F.col("reject_reason") != "")
            .withColumn("source_dataset", F.lit(name))
            .withColumn("raw_record", F.to_json(F.struct(*[F.col(c).alias(c[5:]) for c in raw_cols])))
            .select(*REJECT_COLUMNS))


def _split(df, name):
    """Splits a mapped frame into (standard rows, reject rows)."""
    return df.filter(F.col("reject_reason") == "").select(*STD_COLUMNS), _rejects(df, name)


def _base(df, name, src, cfg):
    common = cfg["common_columns"]
    return df.select(
        *[F.col(c).alias("raw__" + c) for c in df.columns],
        clean(raw_name(common["provider_id"]), cfg).alias("provider_id"),
        F.lit(name).alias("source_dataset"),
        F.lit(src["domain"]).alias("domain"),
        to_date(raw_name(common["period_start"]), cfg).alias("period_start"),
        to_date(raw_name(common["period_end"]), cfg).alias("period_end"),
    )


def map_standard(df, name, src, cfg):
    """One raw row = one measure result (complications, readmissions, timely care)."""
    b = _base(df, name, src, cfg)
    raw = lambda field: F.col("raw__" + raw_name(src["columns"][field])) if field in src["columns"] else F.lit(None).cast("string")
    score_text = clean(raw("score"), cfg)
    numeric = to_double(score_text)
    cats = src.get("categorical_scores")
    if cats:
        lookup = F.create_map(*[F.lit(x) for k, v in cats.items() for x in (k, float(v))])
        numeric = F.coalesce(numeric, lookup[F.lower(score_text)])
    measure_id = clean(raw("measure_id"), cfg)
    m = b.select(
        "*",
        measure_id.alias("measure_id"),
        clean(raw("measure_name"), cfg).alias("measure_name"),
        clean(raw("category"), cfg).alias("category"),
        score_type_expr(measure_id, src).alias("score_type"),
        numeric.alias("score"),
        score_text.alias("score_text"),
        to_double(clean(raw("lower_estimate"), cfg)).alias("lower_estimate"),
        to_double(clean(raw("higher_estimate"), cfg)).alias("higher_estimate"),
        to_double(clean(raw("numerator"), cfg)).alias("numerator"),
        to_double(clean(raw("denominator"), cfg)).alias("denominator"),
        compared_to_national(clean(raw("compared_to_national"), cfg), cfg).alias("compared_to_national"),
        clean(raw("compared_to_national"), cfg).alias("compared_to_national_raw"),
        clean(raw("footnote"), cfg).alias("footnote"),
        F.lit(1).alias("source_row_count"),
    )
    return _split(_with_rejects(m, cfg, F.col("score_text"), F.col("score")), name)


def map_pivot_submeasure(df, name, src, cfg):
    """Healthcare-associated infections: each measure is spread over 6 rows whose IDs end in
    _SIR, _CILOWER, _CIUPPER, _NUMERATOR, _ELIGCASES, _DOPC. Pivots them into one row."""
    b = _base(df, name, src, cfg)
    raw = lambda field: F.col("raw__" + raw_name(src["columns"][field]))
    full_id = clean(raw("measure_id"), cfg)
    suffixes = src["submeasure_suffixes"]
    m = b.select(
        "*",
        F.regexp_extract(full_id, r"^(.+)_([A-Z]+)$", 1).alias("measure_id"),
        F.regexp_extract(full_id, r"^(.+)_([A-Z]+)$", 2).alias("suffix"),
        clean(raw("score"), cfg).alias("score_text"),
        clean(raw("measure_name"), cfg).alias("measure_name_raw"),
        clean(raw("compared_to_national"), cfg).alias("compared_raw"),
        clean(raw("footnote"), cfg).alias("footnote_raw"),
    ).withColumn("measure_id", F.when(F.col("measure_id") == "", None).otherwise(F.col("measure_id")))

    # Row-level rejects: unknown suffix or bad keys, before rows are collapsed.
    m = _with_rejects(m, cfg, F.col("score_text"), to_double(F.col("score_text")))
    m = m.withColumn("reject_reason", F.concat_ws(",", *[
        F.when(F.col("reject_reason") != "", F.col("reject_reason")),
        F.when(~F.col("suffix").isin(list(suffixes)), F.lit("unknown_submeasure")),
    ]))
    good_rows = m.filter(F.col("reject_reason") == "")
    bad = _rejects(m, name)

    sir = F.col("suffix") == "SIR"
    aggs = [F.first(F.when(sir, F.col(c)), ignorenulls=True).alias(a) for c, a in
            [("measure_name_raw", "measure_name"), ("score_text", "score_text"),
             ("compared_raw", "compared_to_national_raw"), ("footnote_raw", "footnote")]]
    aggs += [F.first(F.when(F.col("suffix") == sfx, to_double(F.col("score_text"))), ignorenulls=True).alias(target)
             for sfx, target in suffixes.items() if target]
    aggs.append(F.count(F.lit(1)).cast("int").alias("source_row_count"))
    good = (good_rows.groupBy("provider_id", "source_dataset", "domain", "measure_id", "period_start", "period_end")
            .agg(*aggs)
            .withColumn("category", F.lit(None).cast("string"))
            .withColumn("score_type", F.lit(src["score_type"]))
            .withColumn("compared_to_national", compared_to_national(F.col("compared_to_national_raw"), cfg))
            .select(*STD_COLUMNS))
    return good, bad


def map_multi_value_column(df, name, src, cfg):
    """HCAHPS: the value sits in one of three columns (star rating, answer percent, linear
    mean) depending on the measure; the other two say 'Not Applicable'."""
    b = _base(df, name, src, cfg)
    raw = lambda col: F.col("raw__" + raw_name(col))
    vcols = src["value_columns"]
    applies = lambda v: F.coalesce(F.trim(raw(v["column"])) != "Not Applicable", F.lit(False))
    score_type = reduce(lambda acc, v: acc.when(applies(v), F.lit(v["score_type"])),
                        vcols[1:], F.when(applies(vcols[0]), F.lit(vcols[0]["score_type"])))
    score_text = reduce(lambda acc, v: acc.when(applies(v), clean(raw(v["column"]), cfg)),
                        vcols[1:], F.when(applies(vcols[0]), clean(raw(vcols[0]["column"]), cfg)))
    footnote = reduce(lambda acc, v: acc.when(applies(v), clean(raw(v["footnote"]), cfg)),
                      vcols[1:], F.when(applies(vcols[0]), clean(raw(vcols[0]["footnote"]), cfg)))
    cols = src["columns"]
    m = b.select(
        "*",
        clean(raw(cols["measure_id"]), cfg).alias("measure_id"),
        clean(raw(cols["measure_name"]), cfg).alias("measure_name"),
        F.lit(None).cast("string").alias("category"),
        score_type.alias("score_type"),
        to_double(score_text).alias("score"),
        score_text.alias("score_text"),
        F.lit(None).cast("double").alias("lower_estimate"),
        F.lit(None).cast("double").alias("higher_estimate"),
        F.lit(None).cast("double").alias("numerator"),
        to_double(clean(raw(cols["denominator"]), cfg)).alias("denominator"),
        F.lit(None).cast("string").alias("compared_to_national"),
        F.lit(None).cast("string").alias("compared_to_national_raw"),
        footnote.alias("footnote"),
        F.lit(1).alias("source_row_count"),
    )
    return _split(_with_rejects(m, cfg, F.col("score_text"), F.col("score")), name)


LAYOUTS = {
    "standard": map_standard,
    "pivot_submeasure": map_pivot_submeasure,
    "multi_value_column": map_multi_value_column,
}


def map_source(df, name, cfg):
    """Returns (standard_df, rejects_df) for one bronze source table."""
    src = cfg["sources"][name]
    return LAYOUTS[src["layout"]](df, name, src, cfg)
