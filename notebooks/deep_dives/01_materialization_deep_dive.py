# Databricks notebook source
# MAGIC %md
# MAGIC # Deep Dive 01 - Metric View Materialization
# MAGIC
# MAGIC This notebook supports the first article in the deep-dive series.
# MAGIC
# MAGIC It focuses only on **materialization for Metric Views**.
# MAGIC
# MAGIC Prerequisite:
# MAGIC
# MAGIC Run `00_materialization_base_tables` first. That notebook creates:
# MAGIC
# MAGIC - `mat_fact_finance_daily`: large daily finance fact table
# MAGIC - `mat_dim_*`: dimension tables that the Metric View joins to the fact table
# MAGIC
# MAGIC Why a separate setup notebook?
# MAGIC
# MAGIC Materialization is a performance feature. A tiny dataset hides the point. This deep dive uses a larger fact/dimension model so the optimizer rewrite is easier to reason about.

# COMMAND ----------

import time

dbutils.widgets.text("catalog", "lakemeter_catalog", "Catalog")
dbutils.widgets.text("schema", "metric_views_lod_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

base_source = f"{catalog}.{schema}.mat_fact_finance_daily"
base_mv = f"{catalog}.{schema}.mat_finance_metric_view"
full_mat_mv = f"{catalog}.{schema}.mat_finance_metric_view_materialized"
agg_only_mv = f"{catalog}.{schema}.mat_finance_metric_view_agg_only"

print(f"Base fact source: {base_source}")
print(f"Base Metric View: {base_mv}")
print(f"Full materialized Metric View: {full_mat_mv}")
print(f"Aggregated-only Metric View: {agg_only_mv}")

# COMMAND ----------

def render_mermaid(diagram: str) -> None:
    displayHTML(
        f"""
        <div class="mermaid">
        {diagram}
        </div>
        <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
        <script>
        mermaid.initialize({{startOnLoad: false, securityLevel: "loose"}});
        mermaid.run();
        </script>
        """
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Materialization Does
# MAGIC
# MAGIC Materialization lets the Metric View stay the **business contract**, while Databricks manages physical acceleration behind it.
# MAGIC
# MAGIC Key idea:
# MAGIC
# MAGIC The user still queries `MEASURE(revenue)`. The optimizer decides whether to read from an aggregated materialization, an unaggregated materialization, or the source.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  USER["User query<br/>SELECT ... MEASURE(...)"]
  MV["Metric View<br/>fields + measures"]
  PIPE["Managed Lakeflow pipeline"]
  UNAGG["Unaggregated materialization<br/>prepared source snapshot"]
  AGG["Aggregated materialization<br/>precomputed dashboard grain"]
  OPT["Optimizer<br/>aggregate-aware rewrite"]

  MV --> PIPE
  PIPE --> UNAGG
  PIPE --> AGG
  USER --> OPT
  OPT --> AGG
  OPT --> UNAGG
  OPT --> MV
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Non-Materialized Metric View
# MAGIC
# MAGIC This is the semantic contract. It has no materialization block.

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {base_mv}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Non-materialized finance Metric View for materialization comparison.
source: {base_source}
joins:
  - name: calendar
    source: {catalog}.{schema}.mat_dim_calendar
    on: source.transaction_date = calendar.calendar_date
  - name: entity
    source: {catalog}.{schema}.mat_dim_entity
    on: source.entity_id = entity.entity_id
  - name: product
    source: {catalog}.{schema}.mat_dim_product
    on: source.product_id = product.product_id
  - name: segment
    source: {catalog}.{schema}.mat_dim_segment
    on: source.segment_id = segment.segment_id
  - name: account
    source: {catalog}.{schema}.mat_dim_account
    on: source.account_id = account.account_id
fields:
  - name: fiscal_year
    expr: calendar.fiscal_year
  - name: fiscal_month
    expr: calendar.fiscal_month
  - name: fiscal_quarter
    expr: calendar.fiscal_quarter
  - name: region
    expr: entity.region
  - name: entity_name
    expr: entity.entity_name
  - name: country
    expr: entity.country
  - name: product_family
    expr: product.product_family
  - name: product_name
    expr: product.product_name
  - name: segment_group
    expr: segment.segment_group
  - name: account_category
    expr: account.account_category
  - name: sales_motion
    expr: CASE
      WHEN product.product_family = 'Digital Platforms' AND segment.segment_group = 'Enterprise' THEN 'Strategic Platform'
      WHEN product.product_family = 'Analytics' THEN 'Analytics Motion'
      ELSE 'Standard Motion'
      END
measures:
  - name: revenue
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Revenue')
    display_name: Revenue
  - name: cogs
    expr: SUM(amount) FILTER (WHERE account.account_category = 'COGS')
    display_name: COGS
  - name: opex
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Opex')
    display_name: Opex
  - name: gross_profit
    expr: MEASURE(revenue) - MEASURE(cogs)
    display_name: Gross Profit
  - name: ebitda
    expr: MEASURE(revenue) - MEASURE(cogs) - MEASURE(opex)
    display_name: EBITDA
  - name: transaction_count
    expr: COUNT(DISTINCT transaction_id)
    display_name: Transaction Count
  - name: unique_customers
    expr: COUNT(DISTINCT customer_id)
    display_name: Unique Customers
  - name: revenue_per_customer
    expr: MEASURE(revenue) / NULLIF(MEASURE(unique_customers), 0)
    display_name: Revenue per Customer
$$
"""
)

display(spark.sql(f"DESCRIBE EXTENDED {base_mv}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Materialized Variant With Both Types
# MAGIC
# MAGIC A materialized Metric View can maintain different physical structures behind the same query surface.
# MAGIC
# MAGIC In this tutorial:
# MAGIC
# MAGIC - `semantic_snapshot` is **unaggregated**: it stores prepared rows after the Metric View has applied joins and fields.
# MAGIC - `month_region_product_account` is **aggregated**: it stores precomputed metric results at a chosen dashboard grain.
# MAGIC
# MAGIC | Materialization type | Demo name | What it stores | Best for |
# MAGIC |---|---|---|---|
# MAGIC | `unaggregated` | `semantic_snapshot` | Prepared row-level model after Metric View joins and fields. | Source preparation is expensive or query shapes vary. |
# MAGIC | `aggregated` | `month_region_product_account` | Precomputed `GROUP BY` result for selected dimensions and measures. | Dashboards repeatedly query the same grain or a coarser grain. |

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  MV["Metric View"]
  UNAGG["Unaggregated<br/>prepared rows"]
  AGG["Aggregated<br/>precomputed GROUP BY"]

  MV --> UNAGG
  MV --> AGG
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {full_mat_mv}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Materialized Metric View with unaggregated and aggregated materializations.
source: {base_mv}
fields:
  - name: fiscal_year
    expr: fiscal_year
  - name: fiscal_month
    expr: fiscal_month
  - name: fiscal_quarter
    expr: fiscal_quarter
  - name: region
    expr: region
  - name: entity_name
    expr: entity_name
  - name: product_family
    expr: product_family
  - name: account_category
    expr: account_category
  - name: sales_motion
    expr: sales_motion
measures:
  - name: revenue
    expr: MEASURE(revenue)
  - name: cogs
    expr: MEASURE(cogs)
  - name: opex
    expr: MEASURE(opex)
  - name: ebitda
    expr: MEASURE(ebitda)
  - name: transaction_count
    expr: MEASURE(transaction_count)
  - name: unique_customers
    expr: MEASURE(unique_customers)
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: semantic_snapshot
      type: unaggregated
    - name: month_region_product_account
      type: aggregated
      dimensions:
        - fiscal_year
        - fiscal_month
        - region
        - product_family
        - account_category
      measures:
        - revenue
        - cogs
        - opex
$$
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Refresh
# MAGIC
# MAGIC Materializations must finish materializing before query rewrite can use them.

# COMMAND ----------

def latest_refresh_status(metric_view_name: str) -> str | None:
    rows = spark.sql(f"DESCRIBE EXTENDED {metric_view_name}").collect()
    for row in rows:
        if row["col_name"] == "Latest Refresh Status":
            return row["data_type"]
    return None


deadline = time.time() + 900
status = latest_refresh_status(full_mat_mv)
while status != "Succeeded" and time.time() < deadline:
    print(f"Waiting for refresh. Current status: {status}")
    time.sleep(15)
    status = latest_refresh_status(full_mat_mv)

if status != "Succeeded":
    raise TimeoutError(f"Materialization refresh did not succeed. Last status: {status}")

display(spark.sql(f"DESCRIBE EXTENDED {full_mat_mv}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automatic Query Rewrite: The Decision Tree
# MAGIC
# MAGIC The key feature is not just that materialized data exists. The key feature is that the optimizer can automatically choose the best materialized path for a Metric View query.
# MAGIC
# MAGIC Databricks documents the rewrite order as:
# MAGIC
# MAGIC 1. **Exact match**: use a materialization with the same grouping dimensions and requested measures.
# MAGIC 2. **Rollup match**: use a more detailed aggregate materialization and roll it up, if measures are additive.
# MAGIC 3. **Unaggregated match**: use the prepared source snapshot if no aggregate can serve the query.
# MAGIC 4. **Source fallback**: read from the original source if no materialization can serve the query.
# MAGIC
# MAGIC The rest of this notebook runs one simple SQL query per path. After each query, open Query Profile and compare the materialization name with the expected result.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  Q["Metric View query"]
  E{"Exact aggregate match?"}
  R{"Rollup from aggregate possible?"}
  U{"Unaggregated materialization exists?"}
  S["Read source tables/view"]
  EXACT["Use aggregated materialization<br/>exact match"]
  ROLLUP["Use aggregated materialization<br/>rollup match"]
  UNAGG["Use unaggregated materialization<br/>prepared source snapshot"]

  Q --> E
  E -->|yes| EXACT
  E -->|no| R
  R -->|yes| ROLLUP
  R -->|no| U
  U -->|yes| UNAGG
  U -->|no| S
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three Simple Rewrite Examples
# MAGIC
# MAGIC Instead of timing the queries, we will inspect the optimizer plan. Each example asks a simple question and checks which materialization the optimizer chooses.
# MAGIC
# MAGIC | Scenario | Business question | Why this path | Expected materialization |
# MAGIC |---|---|---|---|
# MAGIC | Exact match | Revenue by year, month, region, product family, and account category | Same grain as the aggregated materialization | `month_region_product_account` |
# MAGIC | Rollup match | Revenue by year, month, and region | Coarser than the aggregated materialization, using additive revenue | `month_region_product_account` |
# MAGIC | Unaggregated match | Unique customers by year, month, and region | Non-additive distinct count cannot roll up safely | `semantic_snapshot` |

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  EXACT["Exact match<br/>same dimensions + subset of measures"]
  ROLLUP["Rollup match<br/>fewer dimensions + additive measures"]
  UNAGG["Unaggregated fallback<br/>non-additive or varied query shape"]

  EXACT --> AGG["month_region_product_account"]
  ROLLUP --> AGG
  UNAGG --> SNAP["semantic_snapshot"]
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Exact Match
# MAGIC
# MAGIC The query groups by the **same dimensions** as the aggregated materialization:
# MAGIC
# MAGIC ```text
# MAGIC fiscal_year + fiscal_month + region + product_family + account_category
# MAGIC ```
# MAGIC
# MAGIC Because the materialization already stores `revenue` at exactly this grain, the optimizer should use `month_region_product_account` directly.
# MAGIC
# MAGIC After running the query, open **Query Profile** and look for the materialized relation name containing:
# MAGIC
# MAGIC ```text
# MAGIC month_region_product_account
# MAGIC ```
# MAGIC
# MAGIC ![Exact match Query Profile](https://raw.githubusercontent.com/CheeYuTan/metric-views-lod-finance-semantics/main/assets/query_profiles/exact_match.png)
# MAGIC
# MAGIC In this screenshot, the scan reads the generated table ending in `month_region_product_account_1`. There is no extra aggregation above the scan, because the query grain exactly matches the materialization grain.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   fiscal_year,
# MAGIC   fiscal_month,
# MAGIC   region,
# MAGIC   product_family,
# MAGIC   account_category,
# MAGIC   MEASURE(revenue) AS revenue
# MAGIC FROM lakemeter_catalog.metric_views_lod_demo.mat_finance_metric_view_materialized
# MAGIC WHERE fiscal_year = 2025
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Rollup Match
# MAGIC
# MAGIC This query asks for a **coarser grain**:
# MAGIC
# MAGIC ```text
# MAGIC fiscal_year + fiscal_month + region
# MAGIC ```
# MAGIC
# MAGIC The aggregated materialization is more detailed because it also has `product_family` and `account_category`.
# MAGIC
# MAGIC Since `revenue` is additive, Databricks can read `month_region_product_account` and roll it up.
# MAGIC
# MAGIC After running the query, open **Query Profile**. You should still see:
# MAGIC
# MAGIC ```text
# MAGIC month_region_product_account
# MAGIC ```
# MAGIC
# MAGIC ![Rollup match Query Profile](https://raw.githubusercontent.com/CheeYuTan/metric-views-lod-finance-semantics/main/assets/query_profiles/rollup_match.png)
# MAGIC
# MAGIC In this screenshot, Databricks still scans `month_region_product_account_1`, but there is an additional `Grouping Aggregate` above the scan. That is the rollup from the more detailed materialization grain to the coarser query grain.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   fiscal_year,
# MAGIC   fiscal_month,
# MAGIC   region,
# MAGIC   MEASURE(revenue) AS revenue
# MAGIC FROM lakemeter_catalog.metric_views_lod_demo.mat_finance_metric_view_materialized
# MAGIC WHERE fiscal_year = 2025
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Unaggregated Match
# MAGIC
# MAGIC This query asks for `unique_customers`, which is a `COUNT(DISTINCT ...)` measure.
# MAGIC
# MAGIC Distinct counts are not additive, so Databricks should not roll them up from the aggregated revenue materialization.
# MAGIC
# MAGIC Because this Metric View also has an unaggregated materialization, the optimizer can use `semantic_snapshot` instead of recomputing all fact-to-dimension joins from scratch.
# MAGIC
# MAGIC After running the query, open **Query Profile** and look for:
# MAGIC
# MAGIC ```text
# MAGIC semantic_snapshot
# MAGIC ```
# MAGIC
# MAGIC ![Unaggregated match Query Profile](https://raw.githubusercontent.com/CheeYuTan/metric-views-lod-finance-semantics/main/assets/query_profiles/unaggregated_match.png)
# MAGIC
# MAGIC In this screenshot, Databricks scans `semantic_snapshot_1`. This happens because `unique_customers` is a distinct count, which cannot safely roll up from the aggregated revenue materialization.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   fiscal_year,
# MAGIC   fiscal_month,
# MAGIC   region,
# MAGIC   MEASURE(unique_customers) AS unique_customers
# MAGIC FROM lakemeter_catalog.metric_views_lod_demo.mat_finance_metric_view_materialized
# MAGIC WHERE fiscal_year = 2025
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Fallback
# MAGIC
# MAGIC To show true source fallback, create an aggregated-only materialized Metric View.
# MAGIC
# MAGIC Because it has no unaggregated materialization, a query that cannot use the aggregate must fall back to the source.
# MAGIC
# MAGIC The query below asks for `unique_customers`, but the only materialization in this view is `revenue_only_aggregate`. Since that aggregate cannot answer a distinct-customer query, Query Profile should not show `revenue_only_aggregate`.
# MAGIC
# MAGIC ![Source fallback Query Profile](https://raw.githubusercontent.com/CheeYuTan/metric-views-lod-finance-semantics/main/assets/query_profiles/source_fallback.png)
# MAGIC
# MAGIC In this screenshot, the plan expands back to the source path and scans the underlying fact and dimension tables required by the Metric View joins. This is the final fallback when no available materialization can answer the query.

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {agg_only_mv}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: {base_mv}
fields:
  - name: fiscal_year
    expr: fiscal_year
  - name: fiscal_month
    expr: fiscal_month
  - name: region
    expr: region
  - name: product_family
    expr: product_family
  - name: account_category
    expr: account_category
measures:
  - name: revenue
    expr: MEASURE(revenue)
  - name: unique_customers
    expr: MEASURE(unique_customers)
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: revenue_only_aggregate
      type: aggregated
      dimensions:
        - fiscal_year
        - fiscal_month
        - region
        - product_family
        - account_category
      measures:
        - revenue
$$
"""
)

deadline = time.time() + 900
status = latest_refresh_status(agg_only_mv)
while status != "Succeeded" and time.time() < deadline:
    print(f"Waiting for aggregated-only refresh. Current status: {status}")
    time.sleep(15)
    status = latest_refresh_status(agg_only_mv)

if status != "Succeeded":
    raise TimeoutError(f"Aggregated-only refresh did not succeed. Last status: {status}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   fiscal_year,
# MAGIC   fiscal_month,
# MAGIC   region,
# MAGIC   MEASURE(unique_customers) AS unique_customers
# MAGIC FROM lakemeter_catalog.metric_views_lod_demo.mat_finance_metric_view_agg_only
# MAGIC WHERE fiscal_year = 2025
# MAGIC GROUP BY ALL
