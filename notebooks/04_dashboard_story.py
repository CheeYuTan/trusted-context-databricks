# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Dashboard Story
# MAGIC
# MAGIC This notebook turns the semantic model into a dashboard narrative.
# MAGIC
# MAGIC The dashboard is not just a visualization layer. It is the proof that business users can consume governed metrics without rewriting:
# MAGIC
# MAGIC - P&L logic
# MAGIC - LOD denominators
# MAGIC - Window calculations
# MAGIC - Semiadditive balances
# MAGIC - Cross-Metric-View composed metrics

# COMMAND ----------

dbutils.widgets.text("catalog", "lakemeter_catalog", "Catalog")
dbutils.widgets.text("schema", "metric_views_lod_demo", "Schema")
dbutils.widgets.text(
    "dashboard_url",
    "https://fe-vm-lakemeter.cloud.databricks.com/sql/dashboardsv3/01f170679f5215ceb42b30a667dcb781",
    "Dashboard URL",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
dashboard_url = dbutils.widgets.get("dashboard_url")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

mv = f"{catalog}.{schema}.finance_metric_view"
exec_mv = f"{catalog}.{schema}.finance_exec_metric_view"

print(f"Metric View: {mv}")
print(f"Executive Metric View: {exec_mv}")
print(f"Dashboard: {dashboard_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Information Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC   MV["finance_metric_view"]
# MAGIC   EXEC["finance_exec_metric_view"]
# MAGIC
# MAGIC   DASH["AI/BI Dashboard"]
# MAGIC   OVERVIEW["Page 1<br/>Executive Overview"]
# MAGIC   LOD["Page 2<br/>LOD Drilldown"]
# MAGIC   WIN["Page 3<br/>Window Semantics"]
# MAGIC   BAL["Page 4<br/>Semiadditive Balances"]
# MAGIC   COMP["Page 5<br/>Cross-View Composability"]
# MAGIC
# MAGIC   MV --> OVERVIEW
# MAGIC   MV --> LOD
# MAGIC   MV --> WIN
# MAGIC   MV --> BAL
# MAGIC   EXEC --> COMP
# MAGIC
# MAGIC   OVERVIEW --> DASH
# MAGIC   LOD --> DASH
# MAGIC   WIN --> DASH
# MAGIC   BAL --> DASH
# MAGIC   COMP --> DASH
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 1 - Executive Overview
# MAGIC
# MAGIC This page answers:
# MAGIC
# MAGIC - What is total revenue?
# MAGIC - What is EBITDA?
# MAGIC - What is EBITDA margin?
# MAGIC - How does actual revenue compare with budget?
# MAGIC
# MAGIC The page uses composed measures like `ebitda`, `ebitda_margin_pct`, and `revenue_variance_pct`.

# COMMAND ----------

executive_overview_sql = f"""
SELECT
  fiscal_month,
  region,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(budget_revenue) AS budget_revenue,
  MEASURE(revenue_variance_pct) AS revenue_variance_pct,
  MEASURE(ebitda) AS ebitda,
  MEASURE(ebitda_margin_pct) AS ebitda_margin_pct
FROM {mv}
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_month, region
"""

display(spark.sql(executive_overview_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 2 - LOD Drilldown
# MAGIC
# MAGIC This page answers:
# MAGIC
# MAGIC - What is revenue by region, entity, and product family?
# MAGIC - What percent of global revenue does each row represent?
# MAGIC - What percent of regional revenue does each entity contribute?
# MAGIC - What percent of the visible total does each entity/product row contribute?
# MAGIC
# MAGIC This page is the best visual explanation of fixed LOD versus coarser LOD.

# COMMAND ----------

lod_drilldown_sql = f"""
SELECT
  region,
  entity_name,
  product_family,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(pct_of_global_revenue_fixed_lod) AS pct_of_global_revenue,
  MEASURE(pct_of_region_revenue) AS pct_of_region_revenue,
  MEASURE(pct_of_entity_product_visible_total) AS pct_of_visible_total
FROM {mv}
WHERE fiscal_year = 2025
  AND account_category = 'Revenue'
GROUP BY ALL
ORDER BY region, entity_name, product_family
"""

display(spark.sql(lod_drilldown_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 3 - Window Semantics
# MAGIC
# MAGIC This page answers:
# MAGIC
# MAGIC - What is current-month revenue?
# MAGIC - What is YTD revenue?
# MAGIC - What is rolling 12-month revenue?
# MAGIC - What was revenue in the same month last year?
# MAGIC - What is YoY growth?

# COMMAND ----------

window_semantics_sql = f"""
SELECT
  fiscal_month,
  region,
  MEASURE(current_month_revenue) AS current_month_revenue,
  MEASURE(ytd_revenue) AS ytd_revenue,
  MEASURE(rolling_12_month_revenue) AS rolling_12_month_revenue,
  MEASURE(prior_year_revenue) AS prior_year_revenue,
  MEASURE(yoy_revenue_growth_pct) AS yoy_revenue_growth_pct
FROM {mv}
WHERE fiscal_month >= DATE'2025-01-01'
GROUP BY ALL
ORDER BY fiscal_month, region
"""

display(spark.sql(window_semantics_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 4 - Semiadditive Balances
# MAGIC
# MAGIC This page answers:
# MAGIC
# MAGIC - What is the quarter-end balance by entity and account category?
# MAGIC - Why should balances use last-value semantics across time?
# MAGIC
# MAGIC A useful screenshot is to compare this output with a naive `SUM(balance_amount)` by quarter from the raw source view. The Metric View result is the business-safe version.

# COMMAND ----------

balances_sql = f"""
SELECT
  fiscal_quarter,
  entity_name,
  account_category,
  MEASURE(month_end_balance) AS month_end_balance
FROM {mv}
WHERE statement_section = 'Balance Sheet'
  AND fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_quarter, entity_name, account_category
"""

display(spark.sql(balances_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 5 - Cross-Metric-View Composability
# MAGIC
# MAGIC This page uses `finance_exec_metric_view`, which is sourced from `finance_metric_view`.
# MAGIC
# MAGIC That proves Metric Views can be layered:
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC   BASE["finance_metric_view<br/>core governed metrics"]
# MAGIC   EXEC["finance_exec_metric_view<br/>executive metrics"]
# MAGIC   DASH["dashboard page"]
# MAGIC
# MAGIC   BASE --> EXEC
# MAGIC   EXEC --> DASH
# MAGIC ```

# COMMAND ----------

exec_sql = f"""
SELECT
  fiscal_month,
  region,
  MEASURE(revenue_per_transaction) AS revenue_per_transaction,
  MEASURE(executive_score) AS executive_score
FROM {exec_mv}
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_month, region
"""

display(spark.sql(exec_sql))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Prompt Ideas
# MAGIC
# MAGIC Agent metadata gives natural-language tools better business context. Try prompts like:
# MAGIC
# MAGIC - Show YTD sales by region for 2025.
# MAGIC - Which entity has the highest operating margin?
# MAGIC - Show closing balance by account category and fiscal quarter.
# MAGIC - Compare current month revenue with the same month last year.
# MAGIC - What percent of region revenue does each entity contribute?
# MAGIC - Show the difference between trailing 3 month inclusive and exclusive revenue.

# COMMAND ----------

print(f"Open the dashboard here: {dashboard_url}")

