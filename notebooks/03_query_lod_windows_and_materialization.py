# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Query LOD, Windows, and Materialization
# MAGIC
# MAGIC This notebook teaches how to consume the Metric View and when materialization helps.
# MAGIC
# MAGIC We intentionally keep two separate objects:
# MAGIC
# MAGIC - `finance_metric_view`: semantic definition only, no materialization.
# MAGIC - `finance_metric_view_materialized`: same business-facing surface, plus materialization for acceleration.
# MAGIC
# MAGIC That separation makes the teaching point clear: **materialization is an optimization strategy, not the metric definition itself**.

# COMMAND ----------

from time import perf_counter
import time

dbutils.widgets.text("catalog", "lakemeter_catalog", "Catalog")
dbutils.widgets.text("schema", "metric_views_lod_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

base_mv = f"{catalog}.{schema}.finance_metric_view"
mat_mv = f"{catalog}.{schema}.finance_metric_view_materialized"
base_view = f"{catalog}.{schema}.finance_semantic_base"

print(f"Base Metric View: {base_mv}")
print(f"Materialized Metric View: {mat_mv}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying at Different Levels of Detail
# MAGIC
# MAGIC The same measure can be queried at many grains without redefining it.
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC   Measure["MEASURE(actual_revenue)"]
# MAGIC   M["Month"]
# MAGIC   R["Region"]
# MAGIC   E["Entity"]
# MAGIC   PF["Product Family"]
# MAGIC   AC["Account Category"]
# MAGIC
# MAGIC   Measure --> M
# MAGIC   Measure --> R
# MAGIC   Measure --> E
# MAGIC   Measure --> PF
# MAGIC   Measure --> AC
# MAGIC ```
# MAGIC
# MAGIC In a traditional dashboard, each of these views often becomes a separate SQL query with duplicated business logic. With a Metric View, the grouping grain changes but the measure definition stays governed.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT
  fiscal_year,
  region,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(ebitda) AS ebitda,
  MEASURE(ebitda_margin_pct) AS ebitda_margin_pct
FROM {base_mv}
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixed LOD: Stable Denominators
# MAGIC
# MAGIC Fixed LOD fields are calculated at a predefined grain. They are useful for denominators that should not change when the query adds more detail.
# MAGIC
# MAGIC This example filters to APJ but still uses the fixed global denominator for `% of global revenue`.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT
  region,
  product_family,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(pct_of_global_revenue_fixed_lod) AS pct_of_global_revenue,
  MEASURE(pct_of_product_family_revenue_fixed_lod) AS pct_of_product_family_revenue,
  MEASURE(pct_of_apj_revenue_fixed_lod) AS pct_of_apj_revenue
FROM {base_mv}
WHERE fiscal_year = 2025
  AND region = 'APJ'
GROUP BY ALL
ORDER BY product_family
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coarser LOD: Filter-Aware Broader Grain
# MAGIC
# MAGIC Coarser LOD uses `range: all` to exclude fields from the denominator grain.
# MAGIC
# MAGIC Here the visible query grain is:
# MAGIC
# MAGIC ```text
# MAGIC region + entity + product_family
# MAGIC ```
# MAGIC
# MAGIC But the denominator excludes both entity and product family.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT
  region,
  entity_name,
  product_family,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(revenue_excluding_entity_and_product_family) AS broader_revenue,
  MEASURE(pct_of_entity_product_visible_total) AS pct_of_visible_total
FROM {base_mv}
WHERE fiscal_year = 2025
  AND region = 'APJ'
GROUP BY ALL
ORDER BY entity_name, product_family
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Semantics
# MAGIC
# MAGIC Window measures let us encode time intelligence once:
# MAGIC
# MAGIC - Current month
# MAGIC - Running total
# MAGIC - YTD
# MAGIC - Rolling 12
# MAGIC - Prior year
# MAGIC - YoY growth
# MAGIC - Leading period

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT
  fiscal_month,
  region,
  MEASURE(current_month_revenue) AS current_month_revenue,
  MEASURE(running_total_revenue) AS running_total_revenue,
  MEASURE(ytd_revenue) AS ytd_revenue,
  MEASURE(rolling_12_month_revenue) AS rolling_12_month_revenue,
  MEASURE(next_month_revenue) AS next_month_revenue,
  MEASURE(prior_year_revenue) AS prior_year_revenue,
  MEASURE(yoy_revenue_growth_pct) AS yoy_revenue_growth_pct
FROM {base_mv}
WHERE fiscal_month >= DATE'2025-01-01'
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inclusive vs Exclusive Trailing Windows
# MAGIC
# MAGIC This is a subtle but important teaching point:
# MAGIC
# MAGIC - `trailing 3 month exclusive` excludes the anchor month.
# MAGIC - `trailing 3 month inclusive` includes the anchor month.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT
  fiscal_month,
  region,
  MEASURE(current_month_revenue) AS current_month_revenue,
  MEASURE(trailing_3_month_revenue_exclusive) AS trailing_3_exclusive,
  MEASURE(trailing_3_month_revenue_inclusive) AS trailing_3_inclusive
FROM {base_mv}
WHERE fiscal_month BETWEEN DATE'2025-01-01' AND DATE'2025-06-01'
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semiadditive Balances
# MAGIC
# MAGIC `month_end_balance` should use the latest month in the quarter, not sum all months in the quarter.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT
  fiscal_quarter,
  entity_name,
  account_category,
  MEASURE(month_end_balance) AS month_end_balance
FROM {base_mv}
WHERE statement_section = 'Balance Sheet'
  AND fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_quarter, entity_name, account_category
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Materialized Variant
# MAGIC
# MAGIC The materialized Metric View is intentionally separate from the base Metric View.
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC   BASE["finance_metric_view<br/>semantic definition"]
# MAGIC   MAT["finance_metric_view_materialized<br/>same business-facing metrics + materialization block"]
# MAGIC   PIPE["Managed Lakeflow pipeline"]
# MAGIC   UNAGG["Unaggregated materialization<br/>semantic base snapshot"]
# MAGIC   AGG1["Aggregated materialization<br/>month + region + account category"]
# MAGIC   AGG2["Aggregated materialization<br/>drilldown grain"]
# MAGIC
# MAGIC   BASE --> MAT
# MAGIC   MAT --> PIPE
# MAGIC   PIPE --> UNAGG
# MAGIC   PIPE --> AGG1
# MAGIC   PIPE --> AGG2
# MAGIC ```
# MAGIC
# MAGIC We source this view from `finance_metric_view` to demonstrate cross-Metric-View composability. The measures simply re-expose governed source measures and then add materialization.

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {mat_mv}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Materialized variant of finance_metric_view for performance comparison.
source: {base_mv}
fields:
  - name: fiscal_month
    expr: fiscal_month
    display_name: Fiscal Month
  - name: fiscal_year
    expr: fiscal_year
    display_name: Fiscal Year
  - name: region
    expr: region
    display_name: Region
  - name: entity_name
    expr: entity_name
    display_name: Entity
  - name: product_family
    expr: product_family
    display_name: Product Family
  - name: account_category
    expr: account_category
    display_name: Account Category
  - name: statement_section
    expr: statement_section
    display_name: Statement Section
  - name: fiscal_quarter
    expr: fiscal_quarter
    display_name: Fiscal Quarter
measures:
  - name: actual_revenue
    expr: MEASURE(actual_revenue)
    display_name: Actual Revenue
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact
  - name: actual_expense
    expr: MEASURE(actual_expense)
    display_name: Actual Expense
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact
  - name: actual_cogs
    expr: MEASURE(actual_cogs)
    display_name: Actual COGS
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact
  - name: actual_opex
    expr: MEASURE(actual_opex)
    display_name: Actual Opex
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact
  - name: ebitda
    expr: MEASURE(ebitda)
    display_name: EBITDA
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact
  - name: transaction_count
    expr: MEASURE(transaction_count)
    display_name: Transaction Count
    format:
      type: number
      abbreviation: compact
  - name: month_end_balance
    expr: MEASURE(month_end_balance)
    display_name: Month-End Balance
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: semantic_snapshot
      type: unaggregated
    - name: exec_month_region_category
      type: aggregated
      dimensions:
        - fiscal_year
        - fiscal_month
        - region
        - account_category
      measures:
        - actual_revenue
        - actual_expense
        - actual_cogs
        - actual_opex
    - name: drilldown_month_region_product_account
      type: aggregated
      dimensions:
        - fiscal_year
        - fiscal_month
        - region
        - entity_name
        - product_family
        - account_category
      measures:
        - actual_revenue
        - actual_expense
        - actual_cogs
        - actual_opex
    - name: balance_month_entity_category
      type: aggregated
      dimensions:
        - fiscal_year
        - fiscal_month
        - entity_name
        - account_category
      measures:
        - month_end_balance
$$
"""
)

try:
    spark.sql(f"REFRESH MATERIALIZED VIEW {mat_mv}")
except Exception as exc:
    message = str(exc)
    if "refresh is already running" in message.lower():
        print("Initial materialization refresh is already running from CREATE OR REPLACE VIEW.")
    else:
        raise

def latest_refresh_status(metric_view_name: str) -> str | None:
    rows = spark.sql(f"DESCRIBE EXTENDED {metric_view_name}").collect()
    for row in rows:
        if row["col_name"] == "Latest Refresh Status":
            return row["data_type"]
    return None

deadline = time.time() + 600
status = latest_refresh_status(mat_mv)
while status != "Succeeded" and time.time() < deadline:
    print(f"Waiting for materialization refresh. Current status: {status}")
    time.sleep(15)
    status = latest_refresh_status(mat_mv)

if status != "Succeeded":
    raise TimeoutError(f"Materialization refresh did not succeed within timeout. Last status: {status}")

display(spark.sql(f"DESCRIBE EXTENDED {mat_mv}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Query Patterns
# MAGIC
# MAGIC We compare three patterns:
# MAGIC
# MAGIC 1. Manual grouping over the source view.
# MAGIC 2. Querying the non-materialized Metric View.
# MAGIC 3. Querying the materialized Metric View.
# MAGIC
# MAGIC This is not a formal benchmark; warehouse cache, data size, and concurrent load matter. The point is to show the user-facing SQL remains stable while materialization gives the optimizer a faster path.
# MAGIC
# MAGIC Use the elapsed time table as directional evidence only. The stronger proof is the optimizer plan in the next section, because the plan shows whether Databricks rewrote the query to a generated materialization relation.

# COMMAND ----------

manual_sql = f"""
SELECT
  fiscal_month,
  region,
  account_category,
  SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_name = 'Actual' AND account_category = 'Revenue') AS actual_revenue
FROM {base_view}
WHERE fiscal_year = 2025
GROUP BY ALL
"""

base_mv_sql = f"""
SELECT
  fiscal_month,
  region,
  account_category,
  MEASURE(actual_revenue) AS actual_revenue
FROM {base_mv}
WHERE fiscal_year = 2025
GROUP BY ALL
"""

mat_mv_sql = f"""
SELECT
  fiscal_year,
  fiscal_month,
  region,
  account_category,
  MEASURE(actual_revenue) AS actual_revenue
FROM {mat_mv}
WHERE fiscal_year = 2025
GROUP BY ALL
"""

def timed_count(label: str, query: str) -> tuple[str, int, float]:
    start = perf_counter()
    rows = spark.sql(query).collect()
    elapsed = perf_counter() - start
    return label, len(rows), elapsed

results = [
    timed_count("manual_source_grouping", manual_sql),
    timed_count("base_metric_view", base_mv_sql),
    timed_count("materialized_metric_view", mat_mv_sql),
]

display(spark.createDataFrame(results, ["query_pattern", "row_count", "elapsed_seconds"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prove the Materialization Is Used
# MAGIC
# MAGIC The `EXPLAIN EXTENDED` plan should include a generated materialization table name when query rewrite chooses a materialized path.

# COMMAND ----------

explain_rows = spark.sql(f"EXPLAIN EXTENDED {mat_mv_sql}").collect()
plan_text = "\n".join(row[0] for row in explain_rows)
print(plan_text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialization Rewrite Scenarios
# MAGIC
# MAGIC The documentation describes three important paths:
# MAGIC
# MAGIC | Scenario | Query shape | Expected behavior |
# MAGIC |---|---|---|
# MAGIC | Exact match | Same dimensions as an aggregate materialization | Read the precomputed aggregate directly |
# MAGIC | Rollup match | Fewer dimensions than the aggregate materialization | Read aggregate and roll up additive measures |
# MAGIC | Non-additive fallback | Uses `COUNT(DISTINCT)` or similar | Use exact/unaggregated/source path because partial aggregates cannot be safely rolled up |
# MAGIC
# MAGIC In each plan, search for `__materialization_mat_` and the materialization name. If the query falls back to the unaggregated materialization, that is still useful: the expensive semantic source preparation has already been computed.

# COMMAND ----------

rewrite_queries = {
    "exact_match_exec_month_region_category": {
        "expected_materialization": "exec_month_region_category",
        "sql": f"""
      SELECT fiscal_year, fiscal_month, region, account_category, MEASURE(actual_revenue) AS actual_revenue
      FROM {mat_mv}
      WHERE fiscal_year = 2025
      GROUP BY ALL
    """,
    },
    "rollup_match_month_region": {
        "expected_materialization": "exec_month_region_category",
        "sql": f"""
      SELECT fiscal_year, fiscal_month, region, MEASURE(actual_revenue) AS actual_revenue
      FROM {mat_mv}
      WHERE fiscal_year = 2025
      GROUP BY ALL
    """,
    },
    "non_additive_count_distinct_fallback": {
        "expected_materialization": "semantic_snapshot",
        "sql": f"""
      SELECT fiscal_month, region, MEASURE(transaction_count) AS transaction_count
      FROM {mat_mv}
      WHERE fiscal_year = 2025
      GROUP BY ALL
    """,
    },
}

for label, config in rewrite_queries.items():
    print(f"\n\n===== {label} =====")
    plan = "\n".join(row[0] for row in spark.sql(f"EXPLAIN EXTENDED {config['sql']}").collect())
    materialization_lines = [
        line for line in plan.splitlines() if "__materialization_mat_" in line or "metric_view_mat" in line
    ]
    if materialization_lines:
        print("\n".join(materialization_lines[:10]))
    else:
        print("No materialization reference found in the returned plan.")

evidence_rows = []
for label, config in rewrite_queries.items():
    plan = "\n".join(row[0] for row in spark.sql(f"EXPLAIN EXTENDED {config['sql']}").collect())
    lines = [line.strip() for line in plan.splitlines() if "__materialization_mat_" in line]
    evidence = "\n".join(lines[:3]) if lines else "No generated materialization relation found."
    expected = config["expected_materialization"]
    evidence_rows.append(
        (
            label,
            expected,
            len(lines) > 0,
            expected in plan,
            evidence,
        )
    )

evidence_df = spark.createDataFrame(
    evidence_rows,
    ["scenario", "expected_materialization", "has_materialization_reference", "used_expected_materialization", "evidence_lines"],
)
display(evidence_df)

missing = [row["scenario"] for row in evidence_df.collect() if not row["has_materialization_reference"]]
if missing:
    raise AssertionError(f"Expected materialization evidence was missing for: {missing}")

wrong = [row["scenario"] for row in evidence_df.collect() if not row["used_expected_materialization"]]
if wrong:
    raise AssertionError(f"Expected materialization names were not found for: {wrong}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to Look For in the Plan
# MAGIC
# MAGIC In the optimized or physical plan, look for a generated relation name containing:
# MAGIC
# MAGIC ```text
# MAGIC __materialization_mat_
# MAGIC ```
# MAGIC
# MAGIC That indicates the optimizer routed the query through a materialized structure rather than recomputing from the source view.

