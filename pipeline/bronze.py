"""
Bronze layer: raw, append-only streaming tables over the Volume files landed by Step 1.

Principles
- Bronze keeps everything as STRING and never drops rows. Expectations here only *record* problems (warn);
  enforcement (drop/fail) happens in Silver.
- Lineage columns on every row: _source_file, _source_version, _source_modified_at, _ingested_at.
  _source_version is the "v=<last_modified>" folder written by the ingestion notebook; Silver uses it to
  keep the latest copy of a record when BLS republishes a file.
- Tables are discovered from the Volume at pipeline-update time, so a new BLS lookup file becomes a new
  Bronze table without a code change.

Pipeline configuration (set in the pipeline settings):
    raw_path = /Volumes/<catalog>/<schema>/<volume>
"""
import os
import re

import dlt as dp  # pyspark.pipelines on this runtime lacks expect(); the dlt module has the full API
from pyspark.sql import functions as F

DEFAULT_RAW = "/Volumes/workspace/rearc_quest/raw"  # <- edit if your Step 1 ROOT differs


def _conf(key: str, default: str) -> str:
    # On serverless / Spark Connect, spark.conf.get(key, default) can still raise CONFIG_NOT_AVAILABLE
    # when the key isn't defined, so catch it and fall back.
    try:
        return spark.conf.get(key)
    except Exception:
        return default


# Root folder of the raw files, from the pipeline setting `raw_path`.
RAW = _conf("raw_path", DEFAULT_RAW)
# Folders written by the ingestion notebook (01_ingest_sources.py).
BLS_DIR = f"{RAW}/bls/pr"
POP_DIR = f"{RAW}/population"

# Non-tabular files in the BLS folder (documentation / contact info). They stay in the Volume as raw files.
SKIP_FILES = {"pr.txt", "pr.contacts"}


# ---------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------
# Warn-only expectations: dp.expect records how many rows break a rule but keeps every row.
def expect_each(rules: dict):
    """Apply several warn-only expectations by stacking dp.expect.
    (works with any version of the module)"""

    def decorator(fn):
        for name, constraint in rules.items():
            fn = dp.expect(name, constraint)(fn)
        return fn

    return decorator


# Add columns that say where each row came from. `_metadata` is provided by file-based sources.
def add_lineage(df):
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        # The v=<digits> folder written by ingestion, for example v=202509190831. Empty for population files.
        .withColumn("_source_version", F.regexp_extract(F.col("_metadata.file_path"), r"/v=([0-9]+)/", 1))
        .withColumn("_source_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def clean_header(df):
    """BLS pads header names with whitespace; trim them. Drop unnamed trailing columns (_c0, _c1...)
    that come from a trailing tab at the end of each row."""
    # Rename every column to its trimmed name.
    df = df.toDF(*[c.strip() for c in df.columns])
    # Unnamed columns (_c0, _c1, ...) appear when each row ends with a tab; drop them.
    junk = [c for c in df.columns if re.fullmatch(r"_c\d+", c)]
    return df.drop(*junk) if junk else df


# Shared reader for BLS files: Auto Loader (cloudFiles), tab-separated, first row is the header.
# Auto Loader's checkpoint remembers which files it has read, so each new file is processed once.
def tsv_stream():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("delimiter", "\t")
        .option("header", "true")
    )


# ---------------------------------------------------------------------------------------------
# BLS time-series data (all pr.data.* files in one table)
# ---------------------------------------------------------------------------------------------
# The data files overlap (e.g. "Current" is a subset of "AllData"), so we ingest all of them and let Silver
# deduplicate on (series_id, year, period). The expectations below guard the expected columns/formats.

# One streaming table for every pr.data.* file. The files overlap, and Silver deduplicates them.
@dp.table(
    name="bronze_bls_pr_data",
    comment="Raw BLS productivity time-series rows from every pr.data.* file; all strings, append-only.",
)
# Warn-only checks: rows are never dropped here. Results show in the pipeline's Expectations panel.
@expect_each(
    {
        "series_id_present": "trim(series_id) <> ''",
        "year_is_4_digits": "trim(year) RLIKE '^[0-9]{4}$'",
        "period_is_valid": "trim(period) RLIKE '^[A-Z][0-9]{2}$'",
        "no_rescued_data": "_rescued_data IS NULL",
    }
)
def bronze_bls_pr_data():
    # Column names come from the file header (BLS pads them with spaces, e.g. "series_id        " and
    # "       value"), so we infer them as strings and trim. A positional .schema() with header=true
    # matches by NAME and sends padded columns to _rescued_data, leaving them null.
    df = (
        tsv_stream()
        # Keep every column as text; Silver does the typing.
        .option("cloudFiles.inferColumnTypes", "false")
        # Only the data files (the folder also holds lookup files). The read is recursive, so it reaches into v= folders.
        .option("pathGlobFilter", "pr.data.*")
        # Anything that does not fit the header goes to _rescued_data instead of being lost.
        .option("rescuedDataColumn", "_rescued_data")
        .load(BLS_DIR)
    )
    return add_lineage(clean_header(df))


# ---------------------------------------------------------------------------------------------
# BLS lookup / metadata files (pr.series, pr.period, pr.measure, ...): one Bronze table per file
# ---------------------------------------------------------------------------------------------
def define_lookup_table(file_name: str):
    # pr.series -> bronze_bls_pr_series (anything that is not a letter or digit becomes an underscore).
    table_name = "bronze_bls_" + re.sub(r"[^a-z0-9]+", "_", file_name.lower()).strip("_")

    # Each lookup file has its own columns, so each file gets its own table.
    @dp.table(name=table_name, comment=f"Raw BLS lookup file {file_name}; all strings, append-only.")
    @dp.expect("no_rescued_data", "_rescued_data IS NULL")
    def _lookup():
        df = (
            tsv_stream()
            .option("cloudFiles.inferColumnTypes", "false")
            .option("rescuedDataColumn", "_rescued_data")
            # Reads every version folder of this one file (Auto Loader reads recursively); Silver keeps the newest.
            .load(f"{BLS_DIR}/{file_name}")
        )
        return add_lineage(clean_header(df))


# Runs when the pipeline updates: define one table for each lookup file in the Volume.
# Ingestion must run first; with an empty Volume no lookup tables would be defined.
if os.path.isdir(BLS_DIR):
    for entry in sorted(os.listdir(BLS_DIR)):
        if entry.startswith("pr.data.") or entry in SKIP_FILES:
            continue
        if os.path.isdir(f"{BLS_DIR}/{entry}"):
            define_lookup_table(entry)


# ---------------------------------------------------------------------------------------------
# DataUSA population (JSON snapshots, kept nested; Silver flattens)
# ---------------------------------------------------------------------------------------------
@dp.table(
    name="bronze_population_raw",
    comment="Raw DataUSA population API responses, one row per landed snapshot file.",
)
@dp.expect("has_data_array", "data IS NOT NULL")
def bronze_population_raw():
    # JSON columns arrive as text by default; Silver parses `data` into records.
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        # Each snapshot file is one JSON document spread over several lines.
        .option("multiLine", "true")
        .load(POP_DIR)
    )
    return add_lineage(df)
