# Databricks notebook source
# MAGIC %md
# MAGIC # Verification report
# MAGIC Reproduces every number and check quoted in `PROCESS.md`, so the results can be exported with the notebook
# MAGIC (**File → Export → HTML** keeps the output of every cell).
# MAGIC
# MAGIC Run it as a regular notebook AFTER the pipeline has run. Set the `catalog` / `schema` widgets to the schema you
# MAGIC want to check (for example `rearc_quest` or `rearc_quest_bundle`). The last cell prints a PASS/FAIL summary.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "rearc_quest")
dbutils.widgets.text("analyst_group", "analysts")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
GROUP = dbutils.widgets.get("analyst_group")
S = f"{CATALOG}.{SCHEMA}"

checks = []  # (name, passed, detail)


# Run a SQL string and return the result as a table.
def run(sql: str):
    return spark.sql(sql)


# Run a SQL string that returns one value, and give back just that value so Python can compare it.
def scalar(sql: str):
    return run(sql).first()[0]


print(f"Checking {S}")

# COMMAND ----------
# MAGIC %md ## 1. Bronze vs Silver: row counts and duplicates

# COMMAND ----------

# Counts for the first checks: rows, and distinct (series_id, year, period) keys, in Bronze and Silver.
# Bronze values are still padded with spaces, so Bronze keys are trimmed before counting.
bronze_rows = scalar(f"SELECT COUNT(*) FROM {S}.bronze_bls_pr_data")
bronze_keys = scalar(
    f"SELECT COUNT(*) FROM (SELECT 1 FROM {S}.bronze_bls_pr_data "
    f"GROUP BY trim(series_id), trim(year), trim(period))"
)
silver_rows = scalar(f"SELECT COUNT(*) FROM {S}.silver_bls_pr_data")
silver_keys = scalar(
    f"SELECT COUNT(*) FROM (SELECT 1 FROM {S}.silver_bls_pr_data GROUP BY series_id, year, period)"
)
silver_null_values = scalar(f"SELECT COUNT(*) FROM {S}.silver_bls_pr_data WHERE value IS NULL")

# Show the numbers as a small table.
counts = spark.createDataFrame(
    [
        ("bronze rows", bronze_rows),
        ("bronze distinct (series_id, year, period)", bronze_keys),
        ("silver rows", silver_rows),
        ("silver distinct keys", silver_keys),
        ("silver rows with NULL value", silver_null_values),
        ("rows removed by deduplication", bronze_rows - silver_rows),
    ],
    "metric string, n long",
)
display(counts)

# Each check is (name, passed, detail). Section 8 prints them all.
checks.append(("Silver has no duplicate keys", silver_rows == silver_keys, f"{silver_rows} rows vs {silver_keys} keys"))
checks.append(("Silver keeps every distinct Bronze key", silver_keys == bronze_keys, f"{silver_keys} vs {bronze_keys}"))
checks.append(("No NULL values in Silver", silver_null_values == 0, f"{silver_null_values} NULLs"))

# COMMAND ----------
# MAGIC %md ### 1a. Copies per key

# COMMAND ----------

# For each key, count its copies in Bronze; then count how many keys have 1 copy, 2 copies, and so on.
display(
    run(
        f"""
WITH k AS (
  SELECT trim(series_id) AS sid, trim(year) AS y, trim(period) AS p, COUNT(*) AS copies
  FROM {S}.bronze_bls_pr_data GROUP BY 1, 2, 3
)
SELECT copies, COUNT(*) AS keys, copies * COUNT(*) AS rows FROM k GROUP BY copies ORDER BY copies
"""
    )
)

# COMMAND ----------
# MAGIC %md ### 1b. Which source files contain each key

# COMMAND ----------

# For each key, list which source files contain it; then count keys per combination of files.
display(
    run(
        f"""
SELECT array_join(files, ' + ') AS files_containing_key, COUNT(*) AS keys
FROM (
  SELECT trim(series_id) AS sid, trim(year) AS y, trim(period) AS p,
         sort_array(collect_set(regexp_extract(_source_file, '/([^/]+)/v=', 1))) AS files
  FROM {S}.bronze_bls_pr_data GROUP BY 1, 2, 3
)
GROUP BY 1 ORDER BY keys DESC
"""
    )
)

# COMMAND ----------
# MAGIC %md ### 1c. Do duplicate copies ever disagree on value? (expected: 0)

# COMMAND ----------

# Keys whose copies carry more than one distinct value. Expected: 0.
conflicts = scalar(
    f"""
SELECT COUNT(*) FROM (
  SELECT trim(series_id), trim(year), trim(period)
  FROM {S}.bronze_bls_pr_data
  GROUP BY 1, 2, 3
  HAVING COUNT(DISTINCT try_cast(trim(value) AS DOUBLE)) > 1
)
"""
)
display(spark.createDataFrame([("keys with conflicting values", conflicts)], "metric string, n long"))
checks.append(("Duplicate copies never disagree on value", conflicts == 0, f"{conflicts} conflicting keys"))

# COMMAND ----------
# MAGIC %md ### 1d. Example duplicates side by side

# COMMAND ----------

# Show some duplicate keys side by side: same key and value, different source files.
display(
    run(
        f"""
SELECT sid, y, p, v, file, copies FROM (
  SELECT trim(series_id) AS sid, trim(year) AS y, trim(period) AS p, trim(value) AS v,
         regexp_extract(_source_file, '/([^/]+)/v=', 1) AS file,
         COUNT(*) OVER (PARTITION BY trim(series_id), trim(year), trim(period)) AS copies
  FROM {S}.bronze_bls_pr_data
)
WHERE copies > 1 ORDER BY sid, y, p LIMIT 20
"""
    )
)

# COMMAND ----------
# MAGIC %md ## 2. Rows per source file

# COMMAND ----------

# Rows per source file in Bronze.
display(
    run(
        f"""
SELECT regexp_extract(_source_file, '/([^/]+)/v=', 1) AS source_file, COUNT(*) AS rows
FROM {S}.bronze_bls_pr_data GROUP BY 1 ORDER BY 2 DESC
"""
    )
)

# COMMAND ----------
# MAGIC %md ## 3. Period codes (confirm Q05 is the annual average)

# COMMAND ----------

# The BLS period lookup: confirms Q05 is the annual average.
display(run(f"SELECT * FROM {S}.silver_bls_pr_period"))

# COMMAND ----------
# MAGIC %md ## 4. Gold tables

# COMMAND ----------
# MAGIC %md ### Q1: population mean / standard deviation, 2013–2018

# COMMAND ----------

# Q1: show the Gold table, then check it covers 6 years and that sample std dev is larger than population std dev.
q1 = run(f"SELECT * FROM {S}.gold_population_stats")
display(q1)
row = q1.first()
checks.append(("Q1 covers 6 years (2013-2018)", row["years_included"] == 6 and row["first_year"] == 2013 and row["last_year"] == 2018,
               f"{row['years_included']} years, {row['first_year']}-{row['last_year']}"))
checks.append(("Q1 sample stddev is larger than population stddev", row["stddev_population_sample"] > row["stddev_population_pop"], ""))
# Independent recompute of the mean, straight from Silver.
mean_silver = scalar(f"SELECT AVG(population) FROM {S}.silver_population WHERE year BETWEEN 2013 AND 2018")
checks.append(("Q1 mean equals the mean recomputed from Silver", abs(row["mean_population"] - mean_silver) <= 1e-6 * mean_silver,
               f"gold {row['mean_population']:.1f} vs silver {mean_silver:.1f}"))

# COMMAND ----------
# MAGIC %md ### Q2: best year per series (with labels)

# COMMAND ----------

# Q2 sizes: Gold rows, distinct series, series that have quarterly data in Silver, and rows missing a label.
q2_rows = scalar(f"SELECT COUNT(*) FROM {S}.gold_best_year_by_series")
q2_series = scalar(f"SELECT COUNT(DISTINCT series_id) FROM {S}.gold_best_year_by_series")
q2_unlabeled = scalar(
    f"SELECT COUNT(*) FROM {S}.gold_best_year_by_series WHERE series_label IS NULL OR series_label = ''"
)
expected_series = scalar(
    f"SELECT COUNT(DISTINCT series_id) FROM {S}.silver_bls_pr_data WHERE period IN ('Q01','Q02','Q03','Q04')"
)
display(
    spark.createDataFrame(
        [
            ("gold rows", q2_rows),
            ("gold distinct series", q2_series),
            ("series with Q01-Q04 data in Silver", expected_series),
            ("rows without a label", q2_unlabeled),
        ],
        "metric string, n long",
    )
)
checks.append(("Q2 has one row per series", q2_rows == q2_series, f"{q2_rows} rows, {q2_series} series"))
checks.append(("Q2 covers every series with quarterly data", q2_series == expected_series, f"{q2_series} vs {expected_series}"))
checks.append(("Q2 rows all labeled", q2_unlabeled == 0, f"{q2_unlabeled} unlabeled"))

display(run(f"SELECT series_id, series_label, year, value FROM {S}.gold_best_year_by_series ORDER BY series_id LIMIT 20"))

# COMMAND ----------
# MAGIC %md ### Hand-check one series: yearly sums for PRS30006011 (top year must match Q2)

# COMMAND ----------

# Top yearly totals for PRS30006011, to read by eye. The top year must match Gold.
display(
    run(
        f"""
SELECT year, ROUND(SUM(value), 3) AS total, COUNT(*) AS quarters
FROM {S}.silver_bls_pr_data
WHERE series_id = 'PRS30006011' AND period IN ('Q01','Q02','Q03','Q04')
GROUP BY year ORDER BY total DESC LIMIT 5
"""
    )
)
display(run(f"SELECT series_id, year, value FROM {S}.gold_best_year_by_series WHERE series_id = 'PRS30006011'"))
# Hand-check one series (PRS30006011): recompute its best year from Silver and compare it with Gold.
hc_year = scalar(
    f"""
SELECT year FROM (
  SELECT year, SUM(value) AS total FROM {S}.silver_bls_pr_data
  WHERE series_id = 'PRS30006011' AND period IN ('Q01','Q02','Q03','Q04')
  GROUP BY year ORDER BY total DESC, year ASC LIMIT 1
)
"""
)
hc_gold_year = scalar(f"SELECT year FROM {S}.gold_best_year_by_series WHERE series_id = 'PRS30006011'")
checks.append(("Hand-checked series PRS30006011: best year matches Gold", hc_year == hc_gold_year,
               f"recomputed from Silver {hc_year} vs Gold {hc_gold_year}"))

# COMMAND ----------
# MAGIC %md ### Q3: PRS30006032 / Q01 with population

# COMMAND ----------

# Q3: show the Gold table, then check its counts and its consistency with Silver.
q3 = run(f"SELECT * FROM {S}.gold_prs30006032_q01_with_population ORDER BY year DESC")
display(q3)
q3_rows = q3.count()
q3_years = q3.select("year").distinct().count()
q3_with_pop = q3.where("population IS NOT NULL").count()
display(
    spark.createDataFrame(
        [("Q3 rows", q3_rows), ("Q3 distinct years", q3_years), ("Q3 rows with population", q3_with_pop)],
        "metric string, n long",
    )
)
q3_other = scalar(
    f"SELECT COUNT(*) FROM {S}.gold_prs30006032_q01_with_population "
    f"WHERE series_id <> 'PRS30006032' OR period <> 'Q01'"
)
checks.append(("Q3 has one row per year", q3_rows == q3_years, f"{q3_rows} rows, {q3_years} years"))
checks.append(("Q3 contains only PRS30006032 / Q01", q3_other == 0, f"{q3_other} other rows"))
# Compare Q3 with Silver: same number of rows, same values, and the same population for each year.
q3_silver_rows = scalar(
    f"SELECT COUNT(*) FROM {S}.silver_bls_pr_data WHERE series_id = 'PRS30006032' AND period = 'Q01'"
)
q3_value_diff = scalar(
    f"""
SELECT COUNT(*) FROM (
  SELECT year, value FROM {S}.gold_prs30006032_q01_with_population
  EXCEPT
  SELECT year, value FROM {S}.silver_bls_pr_data WHERE series_id = 'PRS30006032' AND period = 'Q01'
)
"""
)
q3_pop_diff = scalar(
    f"""
SELECT COUNT(*) FROM {S}.gold_prs30006032_q01_with_population g
LEFT JOIN {S}.silver_population p ON g.year = p.year
WHERE NOT (g.population <=> p.population)
"""
)
checks.append(("Q3 has a row for every Q01 observation in Silver", q3_rows == q3_silver_rows, f"{q3_rows} vs {q3_silver_rows}"))
checks.append(("Q3 values equal the Silver values", q3_value_diff == 0, f"{q3_value_diff} differing rows"))
checks.append(("Q3 population equals Silver population (NULL where missing)", q3_pop_diff == 0, f"{q3_pop_diff} differing rows"))

# COMMAND ----------
# MAGIC %md ## 5. Population years across layers (a year missing everywhere is missing in the source)

# COMMAND ----------

# Population years in each layer: Bronze (parsed from the raw JSON), Silver and Gold.
layers = run(
    f"""
SELECT 'bronze' AS layer, CAST(rec['Year'] AS INT) AS year, rec['Population'] AS population
FROM (SELECT explode(from_json(data, 'array<map<string,string>>')) AS rec FROM {S}.bronze_population_raw)
UNION ALL
SELECT 'silver', year, CAST(population AS STRING) FROM {S}.silver_population
UNION ALL
SELECT 'gold_q3', year, CAST(population AS STRING)
FROM {S}.gold_prs30006032_q01_with_population WHERE population IS NOT NULL
"""
)
# One row per year, with the value from each layer side by side.
display(
    layers.groupBy("year").pivot("layer", ["bronze", "silver", "gold_q3"]).agg({"population": "first"}).orderBy("year")
)
# The years present in Bronze and in Silver must be the same set.
b_years = {r.year for r in layers.where("layer = 'bronze'").collect()}
s_years = {r.year for r in layers.where("layer = 'silver'").collect()}
checks.append(("Population years identical in Bronze and Silver", b_years == s_years, f"bronze {sorted(b_years)} / silver {sorted(s_years)}"))

# COMMAND ----------
# MAGIC %md ## 6. Access controls: privileges held by the analyst group

# COMMAND ----------

# Privileges held by the analyst group, read from the information schema.
# Wrapped in try/except because the group or schema may not exist in every workspace.
try:
    grants = run(
        f"""
SELECT 'catalog' AS level, privilege_type, catalog_name AS object
FROM {CATALOG}.information_schema.catalog_privileges WHERE grantee = '{GROUP}'
UNION ALL
SELECT 'schema', privilege_type, schema_name
FROM {CATALOG}.information_schema.schema_privileges WHERE grantee = '{GROUP}' AND schema_name = '{SCHEMA}'
UNION ALL
SELECT 'table', privilege_type, table_name
FROM {CATALOG}.information_schema.table_privileges WHERE grantee = '{GROUP}' AND table_schema = '{SCHEMA}'
ORDER BY 1, 3
"""
    )
    display(grants)
    g = grants.collect()
    table_grants = {r.object for r in g if r.level == "table"}
    # Any table privilege outside the Gold tables would be a problem.
    non_gold = {t for t in table_grants if not t.startswith("gold_")}
    checks.append(("Analyst group has no grants outside the Gold tables", len(non_gold) == 0, f"non-gold: {sorted(non_gold)}"))
    checks.append(("Analyst group is read-only (SELECT only on tables)",
                   all(r.privilege_type == "SELECT" for r in g if r.level == "table"), ""))
except Exception as e:  # group may not exist in this schema / catalog
    print(f"Could not read grants for '{GROUP}': {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Pipeline expectation metrics (optional)
# MAGIC Reads the pipeline event log (the log covers the whole pipeline, so the query filters to one table's flow and to
# MAGIC events that carry expectation results). If this cell errors or returns nothing, read the same numbers from the pipeline
# MAGIC graph instead (click a table → **Expectations**).

# COMMAND ----------

# The pipeline event log holds the expectation results. Filter to one table's flow and to events that carry
# expectation numbers. Wrapped in try/except because the log may be unavailable.
for table in ["bronze_bls_pr_data", "silver_bls_pr_data"]:
    try:
        print(f"--- {table}")
        display(
            run(
                f"""
SELECT timestamp,
       origin.flow_name                                    AS flow_name,
       details:flow_progress.metrics.num_output_rows      AS output_rows,
       details:flow_progress.data_quality.dropped_records AS dropped_records,
       details:flow_progress.data_quality.expectations    AS expectations
FROM event_log(TABLE({S}.{table}))
WHERE event_type = 'flow_progress'
  AND origin.flow_name LIKE '%{table}'
  AND details:flow_progress.data_quality.expectations IS NOT NULL
ORDER BY timestamp DESC LIMIT 5
"""
            )
        )
    except Exception as e:
        print(f"Event log not available for {table}: {e}")

# COMMAND ----------
# MAGIC %md ## 8. Summary

# COMMAND ----------

# Print PASS/FAIL for every check, then an overall result.
width = max(len(n) for n, _, _ in checks)
for name, ok, detail in checks:
    print(f"{'PASS' if ok else 'FAIL'}  {name.ljust(width)}  {detail}")
failed = [n for n, ok, _ in checks if not ok]
print()
print("ALL CHECKS PASSED" if not failed else f"{len(failed)} CHECK(S) FAILED: {failed}")
