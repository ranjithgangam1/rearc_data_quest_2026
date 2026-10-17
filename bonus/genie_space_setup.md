# Genie space on the Gold layer

A Genie space lets a non-technical stakeholder ask questions about the Gold tables in plain English. This file holds
everything to paste into the space. Menu labels in the Databricks UI may differ slightly from the ones below.

## 1. Create the space

1. In the Databricks sidebar open **Genie** → **New**.
2. Add **only the three Gold tables** (not Bronze or Silver) from `workspace.rearc_quest`:
   - `gold_best_year_by_series`
   - `gold_population_stats`
   - `gold_prs30006032_q01_with_population`
3. Choose a SQL warehouse (serverless / the default one on Free Edition).

**Title:** `US Productivity & Population Explorer`

**Description:**
> Explore US labor productivity statistics from the Bureau of Labor Statistics (BLS), plus US population data.
> Ask which year was best for a measure, compare sectors, or look at population statistics.

## 2. General instructions (paste into the space's instructions box)

```
This space answers questions about US labor productivity (Bureau of Labor Statistics "pr" dataset) and US population.

Tables:
- gold_best_year_by_series: one row per BLS series. Columns: series_id, series_label (human-readable name), measure
  (e.g. Employment, Hours worked, Labor productivity (output per hour), Profits), sector (e.g. Business,
  Manufacturing, Nonfinancial Corporations), worker_class, duration, seasonal_adjustment, base_year, year (the
  "best year"), value (the sum of the series' quarterly values in that year).
- gold_population_stats: a single row with the mean and standard deviation of annual US population for 2013-2018.
  Use stddev_population_sample unless the user asks for the population standard deviation.
- gold_prs30006032_q01_with_population: for the series PRS30006032 (Manufacturing, Hours worked, % change from
  previous quarter), the first-quarter (Q01) value for each year joined with US population. population is NULL for
  years the population source does not cover.

How to interpret and present results:
- "Best year" means the year with the largest sum of quarterly values for that series. Always show series_label, not
  just series_id, so readers understand what is being measured.
- The duration column says what kind of number a series is. "Index (2017=100)" series are levels relative to 2017.
  "% Change ..." series are growth rates. When ranking or comparing series, compare like with like: do not rank
  percent-change series against index series. If a question mixes them, filter or group by duration and say so.
- value is always the SUM of the series' four quarterly values (Q01-Q04) in the best year. For percent-change series
  that is a sum of percentages. For "Index (2017=100)" series it is a sum of four index levels, so it is about four
  times the average index level that year. Treat value as a ranking number for choosing the best year. Never describe
  it as an index level, a growth rate, or cumulative growth. If asked for the average index level, divide value by 4.
- No series has 2026 as its best year because 2026 is not a complete year yet. Mention this if a user asks about
  recent years.
- Population data has no record for 2020, and none for years before 2013 or after 2024. Report NULL population as
  "not available", never as zero, and do not estimate it.
- In tables and charts, keep every year in the data, including years where population is NULL. Show population as
  blank or "not available" for those years, and do not drop the rows. Do not leave a year out of a chart that you
  mention in the text.
- Describe only what the numbers show. Do not explain causes that are not in the data (for example a pandemic or an
  economic cycle). Do not say two measures are correlated, uncorrelated, independent or unrelated unless you computed
  a statistic for it. If asked why, say the data does not say.
- Round displayed numbers sensibly. Population values are whole people.
```

## 3. Sample questions (add as the space's suggested questions)

- Which measures had their best year in 2025?
- Which five Business sector index series have the highest best-year value?
- What is the best year for labor productivity (output per hour) in Manufacturing?
- Compare the best year of Employment across the Business, Nonfarm Business and Manufacturing sectors.
- What are the mean and standard deviation of US population from 2013 to 2018?
- How did the quarterly change in manufacturing hours worked compare with population in each year since 2013?

## 4. Example SQL queries (add as "example SQL" so Genie learns the join and filtering patterns)

**Top index series by best-year value**
```sql
SELECT series_label, year, value
FROM workspace.rearc_quest.gold_best_year_by_series
WHERE duration LIKE 'Index%'
ORDER BY value DESC
LIMIT 10;
```

**How many series peaked in each year, by measure**
```sql
SELECT measure, year, COUNT(*) AS series_count
FROM workspace.rearc_quest.gold_best_year_by_series
GROUP BY measure, year
ORDER BY year DESC, series_count DESC;
```

**Best year for one measure across sectors (like with like)**
```sql
SELECT sector, series_label, year, value
FROM workspace.rearc_quest.gold_best_year_by_series
WHERE measure = 'Employment'
  AND duration LIKE 'Index%'
ORDER BY sector;
```

**Population statistics**
```sql
SELECT years_included, first_year, last_year, mean_population, stddev_population_sample
FROM workspace.rearc_quest.gold_population_stats;
```

**Manufacturing hours worked vs population (all years; population is NULL where the source has no record)**
```sql
SELECT year, value AS quarterly_pct_change, population
FROM workspace.rearc_quest.gold_prs30006032_q01_with_population
WHERE year >= 2013
ORDER BY year;
```

## 5. Improve answer quality (optional but worthwhile)

- In the space's data configuration, add short **column descriptions** (for example: `value` = "sum of the series'
  quarterly values in the best year"; `duration` = "whether the series is an index level or a % change"), and turn on
  value suggestions / entity matching for `measure` and `sector` so users can type "profits" or "manufacturing" and
  match the real values.
- Try each sample question and check Genie's generated SQL against the example queries above. Fix wrong answers by
  adding or editing instructions and example queries, not by editing the data.
- Keep a short list of "benchmark" questions with the SQL you expect, and re-run them after changing instructions.

### Column descriptions to paste (Configure → Sources → table → pencil next to each column)

**`gold_best_year_by_series`**

| Column | Description |
|---|---|
| `series_id` | BLS series identifier, for example PRS88003183. One row per series. |
| `series_label` | Human-readable name of the series: measure, sector, worker class and duration. Show this to users instead of series_id. |
| `measure` | What is being measured, for example Employment, Hours worked, Labor productivity (output per hour), Profits. |
| `sector` | Part of the economy covered, for example Business, Nonfarm Business, Manufacturing, Nonfinancial Corporations. |
| `worker_class` | Group of workers covered, for example All workers or Employees. |
| `duration` | Type of number. "Index (2017=100)" is a level relative to 2017. "% Change ..." values are growth rates. Compare like with like. |
| `seasonal_adjustment` | Whether the series is seasonally adjusted. |
| `base_year` | Base year of an index series (2017). "-" for percent-change series. |
| `year` | The best year for this series: the year with the highest sum of Q01-Q04 values. |
| `value` | Sum of the series' four quarterly values in the best year. For percent-change series this is a sum of percentages; for index series it is a sum of four index levels (about 4 times the average level). Used for ranking, not an index level or growth rate. |

**`gold_population_stats`** (one row)

| Column | Description |
|---|---|
| `years_included` | Number of years of US population data in the 2013-2018 window (6). |
| `first_year` | First year of the fixed statistics window (2013). |
| `last_year` | Last year of the fixed statistics window (2018). This is the end of the window, not the latest year of population data, which is available through 2024. |
| `mean_population` | Mean annual US population over 2013-2018, in people. |
| `stddev_population_sample` | Sample standard deviation (n-1) of annual US population, 2013-2018. Use this by default. |
| `stddev_population_pop` | Population standard deviation (n) of annual US population, 2013-2018. Use only if the user asks for the population standard deviation. |

**`gold_prs30006032_q01_with_population`**

| Column | Description |
|---|---|
| `series_id` | BLS series identifier. Always PRS30006032: Manufacturing, Hours worked, % change from previous quarter. |
| `year` | Calendar year. |
| `period` | Quarter. Always Q01 (first quarter). |
| `value` | Percent change in manufacturing hours worked from the previous quarter, for the first quarter of that year. |
| `population` | US population for that year, in people. NULL when the population source has no record for the year (for example 2020 and years before 2013). Report NULL as "not available", never as zero. |

## 6. Sharing

Analysts need access to the Gold tables (`SELECT` on the three tables plus `USE CATALOG` / `USE SCHEMA`) and permission
to use the SQL warehouse. See the access-control bonus if you want to grant this to a read-only analyst group.

## 7. What to screenshot for the submission

The space's home page with the sample questions, and two or three questions with their answers and the generated SQL
visible (for example: "Which measures had their best year in 2025?").
