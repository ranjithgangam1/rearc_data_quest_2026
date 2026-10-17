"""
Silver layer: typed, trimmed, deduplicated, quality-enforced tables built from Bronze.

Silver tables are batch (materialized-view style) reads of the Bronze streaming tables, because
deduplicating "latest version wins" needs a window over all rows, which a streaming read can't do cheaply.

Rules
- Trim every string; turn empty strings into NULL.
- Cast with try_cast (never fails the pipeline); rows that don't meet the key rules are DROPPED by expectations.
- BLS data files overlap and get republished, so keep ONE row per (series_id, year, period): the newest
  _source_version wins.
- BLS lookup files are full replacements, so only the newest snapshot of each file is kept.
- Population: only the newest API snapshot is kept, then flattened.

Add this file to the same pipeline as bronze.py.
"""
import os
import re

import dlt as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

DEFAULT_RAW = "/Volumes/workspace/rearc_quest/raw"  # <- edit if your Step 1 ROOT differs


# Same fallback as bronze.py: read the `raw_path` pipeline setting, else use the default path.
def _conf(key: str, default: str) -> str:
    try:
        return spark.conf.get(key)
    except Exception:
        return default


# Raw files root. Silver uses it only to find which lookup folders exist, so it can define one table per file.
RAW = _conf("raw_path", DEFAULT_RAW)
BLS_DIR = f"{RAW}/bls/pr"
SKIP_FILES = {"pr.txt", "pr.contacts"}  # keep in sync with bronze.py

# Columns Bronze adds to describe the source file. Silver lookup tables drop them and keep only the data columns.
LINEAGE_COLS = ["_rescued_data", "_source_file", "_source_version", "_source_modified_at", "_ingested_at"]


# ---------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------
# Enforcing rules: a row that breaks any rule is DROPPED. The dropped count shows in the pipeline's Expectations panel.
def expect_drop_each(rules: dict):
    """Enforcing expectations: rows violating any rule are dropped (stacked dp.expect_or_drop)."""

    def decorator(fn):
        for name, constraint in rules.items():
            fn = dp.expect_or_drop(name, constraint)(fn)
        return fn

    return decorator


# Record-only rules: violations are counted, but the rows stay.
def warn_each(rules: dict):
    """Record-only expectations (stacked dp.expect)."""

    def decorator(fn):
        for name, constraint in rules.items():
            fn = dp.expect(name, constraint)(fn)
        return fn

    return decorator


def clean_str(col_name: str):
    """trim, and map '' to NULL"""
    # Trim padding spaces; an empty string then becomes NULL, so missing values look the same everywhere.
    c = F.trim(F.col(col_name))
    return F.when(c == "", F.lit(None)).otherwise(c)


# Apply clean_str to every text column; other column types and any excluded names are left unchanged.
def trim_all_strings(df, exclude=()):
    return df.select(
        *[
            clean_str(c).alias(c) if t == "string" and c not in exclude else F.col(c)
            for c, t in df.dtypes
        ]
    )


# ---------------------------------------------------------------------------------------------
# BLS time-series data
# ---------------------------------------------------------------------------------------------
# BLS period codes: M01-M12 monthly, M13 annual average; Q01-Q04 quarterly, Q05 annual average;
# S01-S02 semi-annual, S03 annual average; A01 annual.
# Only Q05 occurs in this dataset (see silver_bls_pr_period); the other codes are here defensively.
ANNUAL_AVG_PERIODS = ["M13", "Q05", "S03", "A01"]


# Silver data table: one typed, deduplicated row per (series_id, year, period).
@dp.table(
    name="silver_bls_pr_data",
    comment="Typed, trimmed, deduplicated BLS productivity data. One row per (series_id, year, period).",
)
# Rows that fail these rules are dropped: a missing series ID, an impossible year, or a malformed period.
@expect_drop_each(
    {
        "series_id_not_null": "series_id IS NOT NULL",
        "year_is_valid": "year IS NOT NULL AND year BETWEEN 1900 AND 2100",
        "period_is_valid": "period RLIKE '^[A-Z][0-9]{2}$'",
    }
)
# value_is_numeric only warns: an unparseable value is counted, but the row is kept.
@warn_each(
    {
        "value_is_numeric": "value IS NOT NULL",
    }
)
def silver_bls_pr_data():
    # Batch read of the Bronze streaming table; the window functions below need all rows at once.
    b = dp.read("bronze_bls_pr_data")

    # Clean and type each column. try_cast returns NULL for a bad number or year instead of failing the pipeline.
    typed = b.select(
        clean_str("series_id").alias("series_id"),
        F.expr("try_cast(trim(year) AS INT)").alias("year"),
        clean_str("period").alias("period"),
        F.expr("try_cast(trim(value) AS DOUBLE)").alias("value"),
        clean_str("footnote_codes").alias("footnote_codes"),
        F.col("_source_file"),
        F.col("_source_version"),
        F.col("_ingested_at"),
    )

    # 1) Within each SOURCE FILE keep only its newest version. A file is a full replacement, so a row that
    #    BLS removed in a newer version must not survive from an older version.
    typed = typed.withColumn(
        "_file_name", F.regexp_extract(F.col("_source_file"), r"/([^/]+)/v=[0-9]+/", 1)
    )
    latest_per_file = Window.partitionBy("_file_name")
    typed = (
        typed.withColumn("_latest_version", F.max("_source_version").over(latest_per_file))
        .filter(F.col("_source_version") == F.col("_latest_version"))
        .drop("_file_name", "_latest_version")
    )

    # 2) The data files overlap, so across files keep ONE row per key: newest version wins
    #    (ties broken by ingestion time, then file path for determinism)
    w = Window.partitionBy("series_id", "year", "period").orderBy(
        F.col("_source_version").desc(), F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    deduped = typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # Flag BLS's annual-summary rows. Gold filters to Q01-Q04 directly, so this column is informational.
    return deduped.withColumn("is_annual_average", F.col("period").isin(ANNUAL_AVG_PERIODS))


# ---------------------------------------------------------------------------------------------
# BLS lookup tables: keep only the newest snapshot of each file (files are full replacements)
# ---------------------------------------------------------------------------------------------
def define_silver_lookup(file_name: str):
    # Table names mirror Bronze: pr.series -> bronze_bls_pr_series -> silver_bls_pr_series.
    suffix = re.sub(r"[^a-z0-9]+", "_", file_name.lower()).strip("_")
    bronze_name = f"bronze_bls_{suffix}"
    silver_name = f"silver_bls_{suffix}"

    @dp.table(name=silver_name, comment=f"Trimmed, latest-snapshot copy of BLS lookup {file_name}.")
    def _silver_lookup():
        b = dp.read(bronze_name)
        # Lookup files are full replacements, so only rows from the newest version folder are current.
        latest = b.agg(F.max("_source_version").alias("v"))
        newest = b.join(latest, b["_source_version"] == latest["v"]).drop("v")
        # Drop Bronze's lineage columns; a lookup table keeps just its data columns.
        data_cols = [c for c in newest.columns if c not in LINEAGE_COLS]
        # Trim text and remove exact duplicate rows.
        return trim_all_strings(newest.select(*data_cols)).dropDuplicates()


# Same discovery as bronze.py: define one Silver table for each lookup file found in the Volume.
if os.path.isdir(BLS_DIR):
    for entry in sorted(os.listdir(BLS_DIR)):
        if entry.startswith("pr.data.") or entry in SKIP_FILES:
            continue
        if os.path.isdir(f"{BLS_DIR}/{entry}"):
            define_silver_lookup(entry)


# ---------------------------------------------------------------------------------------------
# Population (DataUSA): newest snapshot, flattened
# ---------------------------------------------------------------------------------------------
# Auto Loader infers JSON columns as STRING by default, so `data` arrives as a JSON string holding an array.
# We parse it into array<map<string,string>> (no field names needed up front), lower-case the keys, and pick
# year / nation / population by key. try_element_at returns NULL (instead of raising under ANSI mode) if a
# key is missing, and the expectations below then drop those rows.
@dp.table(
    name="silver_population",
    comment="US population by year, flattened from the newest DataUSA API snapshot.",
)
# Rows without a usable year or population are dropped.
@expect_drop_each(
    {
        "year_not_null": "year IS NOT NULL",
        "population_not_null": "population IS NOT NULL",
    }
)
def silver_population():
    b = dp.read("bronze_population_raw")
    # Keep only the newest API snapshot. Population files have no v= folder, so the file's modified time decides.
    latest_ts = b.agg(F.max("_source_modified_at").alias("m"))
    newest = b.join(latest_ts, b["_source_modified_at"] == latest_ts["m"]).drop("m")

    # `data` is a JSON string holding a list of records; parse it into a list of key/value maps.
    records = F.from_json(F.col("data"), "array<map<string,string>>")

    # One row per record: explode the list, lower-case the keys, then pick year, nation and population by key.
    return (
        newest.select(F.explode(records).alias("raw"), F.col("_source_file"))
        .select(F.expr("transform_keys(raw, (k, v) -> lower(k))").alias("r"), F.col("_source_file"))
        .select(
            F.expr("try_cast(try_element_at(r, 'year') AS INT)").alias("year"),
            F.trim(F.expr("try_element_at(r, 'nation')")).alias("nation"),
            # the API returns population in scientific notation as text (e.g. "3.16128839E8"), which cannot be cast
            # straight to BIGINT; go through DOUBLE (exact for integers this size) and round.
            F.expr("try_cast(round(try_cast(try_element_at(r, 'population') AS DOUBLE)) AS BIGINT)").alias("population"),
            F.col("_source_file"),
        )
        # One row per year and nation, in case a snapshot repeats a record.
        .dropDuplicates(["year", "nation"])
    )
