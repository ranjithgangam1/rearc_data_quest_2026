# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: PySpark implementation + cross-check
# MAGIC Re-implements the three Gold queries with the DataFrame API and compares them to the SQL materialized views
# MAGIC built by the pipeline (`gold.sql`). Run this as a normal notebook AFTER the pipeline has run.

# COMMAND ----------

# Window functions are needed to rank the best year per series.
from pyspark.sql import Window
from pyspark.sql import functions as F

# Parameters, so the job (and the bundle) can point this check at any schema.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "rearc_quest")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")


# Shortcut: read a table from the schema being checked.
def t(name: str):
    return spark.table(f"{CATALOG}.{SCHEMA}.{name}")


# Silver tables are the inputs. Everything below is computed independently of the SQL in gold.sql.
data = t("silver_bls_pr_data")
pop = t("silver_population")

# COMMAND ----------
# MAGIC %md ## Q1: population mean / stddev, 2013-2018

# COMMAND ----------

# Q1: mean and standard deviation of population for 2013-2018 (sample and population versions).
q1 = (
    pop.where(F.col("year").between(2013, 2018))
    .agg(
        F.count("*").alias("years_included"),
        F.min("year").alias("first_year"),
        F.max("year").alias("last_year"),
        F.avg("population").alias("mean_population"),
        F.stddev_samp("population").alias("stddev_population_sample"),
        F.stddev_pop("population").alias("stddev_population_pop"),
    )
)
display(q1)

# COMMAND ----------
# MAGIC %md ## Q2: best year per series_id (max sum of Q01-Q04)

# COMMAND ----------

# Q2 step 1: sum the four quarterly values (Q01-Q04) for each series and year.
yearly = (
    data.where(F.col("period").isin("Q01", "Q02", "Q03", "Q04"))
    .groupBy("series_id", "year")
    .agg(F.sum("value").alias("value"))
)
# Q2 step 2: rank each series' years from highest sum to lowest, earliest year first on ties.
w = Window.partitionBy("series_id").orderBy(F.col("value").desc(), F.col("year").asc())
# Keep the top year per series and round to 3 decimals, as the SQL does.
best = (
    yearly.withColumn("rn", F.row_number().over(w))
    .where("rn = 1")
    .select("series_id", "year", F.round("value", 3).alias("value"))
)

# Q2 step 3: add readable labels from the lookup tables (left joins, so no series is lost).
series = t("silver_bls_pr_series").alias("s")
q2 = (
    best.alias("b")
    .join(series, F.col("b.series_id") == F.col("s.series_id"), "left")
    .join(t("silver_bls_pr_measure").alias("m"), F.col("s.measure_code") == F.col("m.measure_code"), "left")
    .join(t("silver_bls_pr_sector").alias("sec"), F.col("s.sector_code") == F.col("sec.sector_code"), "left")
    .join(t("silver_bls_pr_class").alias("c"), F.col("s.class_code") == F.col("c.class_code"), "left")
    .join(t("silver_bls_pr_duration").alias("d"), F.col("s.duration_code") == F.col("d.duration_code"), "left")
    .join(t("silver_bls_pr_seasonal").alias("sea"), F.col("s.seasonal") == F.col("sea.Seasonal_code"), "left")
    .select(
        F.col("b.series_id").alias("series_id"),
        F.concat_ws(" | ", F.col("m.measure_text"), F.col("sec.sector_name"), F.col("c.class_text"), F.col("d.duration_text")).alias("series_label"),
        F.col("m.measure_text").alias("measure"),
        F.col("sec.sector_name").alias("sector"),
        F.col("c.class_text").alias("worker_class"),
        F.col("d.duration_text").alias("duration"),
        F.col("sea.Seasonal_text").alias("seasonal_adjustment"),
        F.col("s.base_year").alias("base_year"),
        F.col("b.year").alias("year"),
        F.col("b.value").alias("value"),
    )
)
display(q2.orderBy("series_id"))

# COMMAND ----------
# MAGIC %md ## Q3: PRS30006032 / Q01 with population

# COMMAND ----------

# Q3: Q01 values of PRS30006032, left-joined to population by year (NULL where there is no record).
q3 = (
    data.where((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01"))
    .alias("d")
    .join(pop.alias("p"), F.col("d.year") == F.col("p.year"), "left")
    .select("d.series_id", F.col("d.year").alias("year"), "d.period", "d.value", "p.population")
)
display(q3.orderBy("year"))

# COMMAND ----------
# MAGIC %md ## Cross-check against the SQL tables

# COMMAND ----------


# Compare two tables row for row: rows found on only one side must be zero in both directions.
def same_rows(a, b, name):
    only_a = a.exceptAll(b).count()
    only_b = b.exceptAll(a).count()
    ok = only_a == 0 and only_b == 0
    print(f"{name}: {'MATCH' if ok else 'MISMATCH'}  (only in PySpark: {only_a}, only in SQL: {only_b})")
    return ok


# Compare the PySpark results with the SQL tables the pipeline built.
results = [
    same_rows(q2, t("gold_best_year_by_series").select(*q2.columns), "Q2 best year by series (with labels)"),
    same_rows(
        q3,
        t("gold_prs30006032_q01_with_population").select("series_id", "year", "period", "value", "population"),
        "Q3 PRS30006032 Q01 + population",
    ),
]

# Q1 has floating point values, so compare with a tolerance instead of exact equality.
sql_q1 = t("gold_population_stats").first()
py_q1 = q1.first()
q1_ok = all(
    abs(sql_q1[c] - py_q1[c]) < 1e-6 * max(1.0, abs(py_q1[c]))
    for c in ["mean_population", "stddev_population_sample", "stddev_population_pop"]
) and sql_q1["years_included"] == py_q1["years_included"]
print(f"Q1 population stats: {'MATCH' if q1_ok else 'MISMATCH'}")
results.append(q1_ok)

# Fail the notebook (and the job task that runs it) if any comparison failed.
assert all(results), "SQL and PySpark implementations disagree"
print("All Gold implementations agree.")
