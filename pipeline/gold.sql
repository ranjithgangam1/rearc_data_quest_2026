-- Gold layer (Spark SQL). Add this file to the same pipeline as bronze.py and silver.py.
-- Each table answers one question from the data quest. The same three queries are re-implemented in PySpark
-- in gold_pyspark_check.py, which verifies both implementations agree.


-- ---------------------------------------------------------------------------------------------
-- Q1. Mean and standard deviation of annual US population, 2013-2018 inclusive.
-- Both the sample and the population standard deviation are provided; the quest does not say which.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_population_stats
COMMENT 'Mean and standard deviation of annual US population for 2013-2018 inclusive.'
AS
SELECT
  COUNT(*)                         AS years_included,
  MIN(year)                        AS first_year,
  MAX(year)                        AS last_year,
  AVG(population)                  AS mean_population,
  STDDEV_SAMP(population)          AS stddev_population_sample,
  STDDEV_POP(population)           AS stddev_population_pop
-- The window is fixed by the question (2013-2018); the population data extends beyond it.
-- first_year / last_year above describe this window, not the latest year of data.
FROM silver_population
WHERE year BETWEEN 2013 AND 2018;


-- ---------------------------------------------------------------------------------------------
-- Q2. Best year for every series_id: the year with the maximum sum of value across its quarters,
-- with human-readable labels so the series is understandable without knowing BLS codes.
-- Only Q01-Q04 are summed; Q05 is BLS's annual-average row and would double count the year.
-- Ties are broken by taking the earliest year so the result is deterministic.
-- Note: a year with missing quarters has a smaller sum; completeness is not checked.
-- LEFT JOINs to the lookup tables keep a series even if one of its codes has no lookup row.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_best_year_by_series
COMMENT 'For each series_id, the year with the highest sum of value over quarters Q01-Q04, with readable series labels.'
AS
-- yearly: for each series and year, the sum of the four quarterly values (the Q05 annual average is excluded).
WITH yearly AS (
  SELECT
    series_id,
    year,
    SUM(value) AS value
  FROM silver_bls_pr_data
  WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
  GROUP BY series_id, year
),
-- ranked: number each series' years from highest sum to lowest; ties go to the earlier year.
ranked AS (
  SELECT
    series_id,
    year,
    value,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY value DESC, year ASC) AS rn
  FROM yearly
),
-- best: keep the top-ranked year and round away floating-point noise (the source has at most 3 decimals).
best AS (
  SELECT series_id, year, ROUND(value, 3) AS value
  FROM ranked
  WHERE rn = 1
)
-- Final result: the best year plus readable labels from the lookup tables. LEFT JOINs keep every series.
SELECT
  b.series_id,
  CONCAT_WS(' | ', m.measure_text, sec.sector_name, c.class_text, d.duration_text) AS series_label,
  m.measure_text        AS measure,
  sec.sector_name       AS sector,
  c.class_text          AS worker_class,
  d.duration_text       AS duration,
  sea.Seasonal_text     AS seasonal_adjustment,
  s.base_year,
  b.year,
  b.value
FROM best AS b
LEFT JOIN silver_bls_pr_series   AS s   ON b.series_id    = s.series_id
LEFT JOIN silver_bls_pr_measure  AS m   ON s.measure_code  = m.measure_code
LEFT JOIN silver_bls_pr_sector   AS sec ON s.sector_code   = sec.sector_code
LEFT JOIN silver_bls_pr_class    AS c   ON s.class_code    = c.class_code
LEFT JOIN silver_bls_pr_duration AS d   ON s.duration_code = d.duration_code
LEFT JOIN silver_bls_pr_seasonal AS sea ON s.seasonal      = sea.Seasonal_code;


-- ---------------------------------------------------------------------------------------------
-- Q3. Value for series PRS30006032, period Q01, with the US population for that year when available.
-- LEFT JOIN so years that are missing from the population data are kept (population is NULL there).
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_prs30006032_q01_with_population
COMMENT 'Series PRS30006032, period Q01, joined to US population by year (NULL where unavailable).'
AS
SELECT
  d.series_id,
  d.year,
  d.period,
  d.value,
  p.population
-- LEFT JOIN on year: every Q01 year of the series is kept, and population is NULL where the source has no record.
FROM silver_bls_pr_data AS d
LEFT JOIN silver_population AS p
  ON d.year = p.year
WHERE d.series_id = 'PRS30006032'
  AND d.period = 'Q01';
