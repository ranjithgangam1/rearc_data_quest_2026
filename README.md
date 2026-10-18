# Rearc Data Quest: Databricks Edition

BLS productivity time series (`pr` folder) and DataUSA population data, ingested into a Unity Catalog Volume and
modeled as a Spark Declarative Pipeline (Bronze → Silver → Gold).

See [`PROCESS.md`](PROCESS.md) for the architecture, trade-offs, retrospective and AI-usage statement.

Screenshots from Databricks and the exported verification report are in [`output/`](output/).

## Repository layout

| Path | Purpose |
|---|---|
| `01_ingest_sources.py` | Databricks notebook. Incrementally syncs the BLS folder and the population API into a Volume. |
| `pipeline/bronze.py` | Pipeline source: raw, append-only streaming tables (Auto Loader). |
| `pipeline/silver.py` | Pipeline source: typed, trimmed, deduplicated tables with enforcing expectations. |
| `pipeline/gold.sql` | Pipeline source: the three Gold tables (SQL is the primary implementation). |
| `notebooks/gold_pyspark_check.py` | Notebook. PySpark alternative for the Gold logic, with a row-for-row cross-check against the SQL tables. |
| `notebooks/verification_report.py` | Notebook. Reproduces the verification numbers (counts, duplicates, Gold results, grants) with a PASS/FAIL summary. |
| `diagrams/` | Data flow and data model diagrams (PNG and SVG), used in `PROCESS.md`. |
| `output/` | Evidence from my Databricks run: `verification_report.html` (exported results of the report notebook) and `screenshots/` of the pipeline and tables. |
| `PROCESS.md` | Design decisions, trade-offs, retrospective, AI usage. |
| `databricks.yml` | Databricks Asset Bundle: the pipeline plus a job that runs ingest → pipeline → Gold verification. |
| `bonus/access_controls.sql` | Read-only Unity Catalog grants for an analyst group on the Gold tables only. |
| `bonus/genie_space_setup.md` | Configuration for a Genie space on the Gold layer (instructions, example SQL, sample questions). |

## Prerequisites

- A Databricks workspace with Unity Catalog and serverless compute (Free Edition works).
- A catalog and schema you can create objects in. The code defaults to `workspace.rearc_quest` and a volume named `raw`.
- Contact information for the BLS `User-Agent` header. BLS blocks anonymous bots
  ([data access policy](https://www.bls.gov/bls/pss.htm)).

## How to run

1. **Ingest.** Import `01_ingest_sources.py` as a notebook.
   **Required:** set the `contact` widget to your real name and email address, for example
   `Jane Doe jane@yourdomain.com`. BLS blocks requests that do not identify the caller, so the notebook stops with an
   error if the widget has no `@` or still contains the `example.com` placeholder (including the default value).
   Adjust `catalog` / `schema` / `volume` if needed, then run all cells.
   It creates the schema and volume, then lands:
   - `bls/pr/<file>/v=<last_modified>/<file>` for every file in the BLS folder
   - `population/population_<timestamp>_<hash>.json`
   - `_state/manifest.json` (what has been ingested)

   Run it again: nothing already ingested is downloaded a second time. If the source adds or changes files, only those
   are fetched; changed files land as new versions and removed files are flagged in the manifest.

2. **Create the pipeline.** In **Jobs & Pipelines → Create → ETL pipeline**, choose the target catalog and schema
   (`workspace` / `rearc_quest`) and add `pipeline/bronze.py`, `pipeline/silver.py` and `pipeline/gold.sql` as source
   files. If your volume path differs from `/Volumes/workspace/rearc_quest/raw`, either set a pipeline configuration key
   `raw_path` to the volume root or edit `DEFAULT_RAW` at the top of `bronze.py` and `silver.py`. Run the pipeline.

   Note: the pipeline files must run as pipeline source, not as notebooks or on regular compute.

3. **Verify.** Import `notebooks/gold_pyspark_check.py` as a regular notebook and run it after the pipeline. It
   recomputes the three Gold answers with PySpark and asserts they equal the SQL tables.

## Deploy with an Asset Bundle (optional)

Instead of the manual steps above, install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/), set
`workspace.host` in `databricks.yml` (or run `databricks auth login`), then:

```
databricks bundle validate -t dev --var "contact=Your Name you@yourdomain.com"
databricks bundle deploy   -t dev --var "contact=Your Name you@yourdomain.com"
databricks bundle run      -t dev refresh_job --var "contact=Your Name you@yourdomain.com"
```

The job runs ingestion, then the pipeline, then the PySpark cross-check that fails the job if the SQL and PySpark Gold
results disagree. The schedule is deployed paused.

## Access control (optional)

`bonus/access_controls.sql` grants a read-only analyst group access to the Gold tables only. Replace the group name
first.

## Tables produced

| Layer | Tables |
|---|---|
| Bronze | `bronze_bls_pr_data`, one `bronze_bls_pr_<lookup>` per lookup file, `bronze_population_raw` |
| Silver | `silver_bls_pr_data`, one `silver_bls_pr_<lookup>` per lookup file, `silver_population` |
| Gold | `gold_population_stats` (Q1), `gold_best_year_by_series` (Q2), `gold_prs30006032_q01_with_population` (Q3) |

## The three questions

1. **`gold_population_stats`**: mean and standard deviation of annual US population, 2013–2018 inclusive (sample and
   population standard deviation both provided).
2. **`gold_best_year_by_series`**: for every `series_id`, the year with the largest sum of `value` over quarters
   Q01–Q04, with a readable `series_label` (measure | sector | worker class | duration) and its components.
3. **`gold_prs30006032_q01_with_population`**: `value` by year for `PRS30006032` / `Q01`, left-joined to population
   (NULL where the API has no record for that year).

## Data-quality approach

- **Bronze** records problems but never drops rows (warn-only expectations plus a `_rescued_data` column).
- **Silver** enforces: rows with a missing `series_id`, invalid year or malformed period are dropped; unparseable
  values are counted but kept.
- Silver deduplicates on `(series_id, year, period)` with the newest source version winning.

## Known limitations

See the trade-offs section of [`PROCESS.md`](PROCESS.md). In short: the ingestion manifest is a JSON file (not safe for
concurrent runs), a whole file removed at the source is flagged but not excluded from Silver, and the best-year metric
does not exclude incomplete years.
