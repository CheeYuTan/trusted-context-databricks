# Databricks notebook source
# MAGIC %md
# MAGIC # Deep Dive 02 - Level of Detail Expressions
# MAGIC
# MAGIC This notebook supports the second article in the Metric Views deep-dive series.
# MAGIC
# MAGIC Part 1 showed how materialization can accelerate a governed Metric View. Part 2 focuses on a different problem: **how do we make sure every dashboard, analyst, and AI/BI experience calculates percentages at the same business grain?**
# MAGIC
# MAGIC ## What Does Level of Detail Mean?
# MAGIC
# MAGIC Level of detail, or LOD, means **the level at which a calculation is performed**.
# MAGIC
# MAGIC That sounds abstract, so start with a simple finance question:
# MAGIC
# MAGIC > What percentage of revenue came from APJ?
# MAGIC
# MAGIC To answer that, we need two numbers:
# MAGIC
# MAGIC - Numerator: APJ revenue.
# MAGIC - Denominator: total revenue.
# MAGIC
# MAGIC The hard part is not the numerator. The hard part is agreeing on the denominator.
# MAGIC
# MAGIC Does "total revenue" mean:
# MAGIC
# MAGIC - Total revenue across all years?
# MAGIC - Total revenue for the selected fiscal year?
# MAGIC - Total revenue for APJ only?
# MAGIC - Total revenue after dashboard filters are applied?
# MAGIC - Total revenue across all products, or only the currently selected product family?
# MAGIC
# MAGIC Each answer is a different **level of detail**. If every dashboard author writes their own SQL, these definitions can quietly drift apart.
# MAGIC
# MAGIC Metric View LOD expressions solve this by moving the denominator grain into the semantic layer. A user can query a simple measure like `MEASURE(pct_of_region_revenue)`, while the Metric View owns the rules for what "region revenue" means.
# MAGIC
# MAGIC In this notebook, keep one question in mind:
# MAGIC
# MAGIC > Percent of what?
# MAGIC
# MAGIC LOD is how we make that answer explicit and reusable.
# MAGIC
# MAGIC We reuse the same finance star schema from `00_materialization_base_tables`:
# MAGIC
# MAGIC - `mat_fact_finance_daily`
# MAGIC - `mat_dim_calendar`
# MAGIC - `mat_dim_entity`
# MAGIC - `mat_dim_product`
# MAGIC - `mat_dim_segment`
# MAGIC - `mat_dim_account`
# MAGIC
# MAGIC The output of this notebook is a new Metric View named `lod_finance_metric_view`.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakemeter_catalog", "Catalog")
dbutils.widgets.text("schema", "metric_views_lod_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
qualified_schema = f"`{catalog}`.`{schema}`"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

lod_mv = f"{catalog}.{schema}.lod_finance_metric_view"

print(f"Using schema: {catalog}.{schema}")
print(f"LOD Metric View: {lod_mv}")

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


def require_approx(actual: float, expected: float, label: str, tolerance: float = 0.0001) -> None:
    if actual is None or abs(float(actual) - expected) > tolerance:
        raise AssertionError(f"{label}: expected {expected}, got {actual}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOD Mental Model
# MAGIC
# MAGIC A normal aggregation uses the fields in your query to decide the calculation grain.
# MAGIC
# MAGIC For example, if a query groups by `region` and `product_family`, then `MEASURE(revenue)` returns revenue at the `region + product_family` grain.
# MAGIC
# MAGIC LOD expressions let a Metric View calculate part of the metric at a **different grain** from the fields visible in the query.
# MAGIC
# MAGIC A dashboard query might group by `region`, `entity_name`, and `product_family`. The numerator should use that visible grain. The denominator might need a different grain:
# MAGIC
# MAGIC - Fixed LOD: use a predefined grain, such as fiscal-year global revenue or fiscal-year APJ revenue.
# MAGIC - Coarser LOD: use the visible query grain, then intentionally exclude one or more fields.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  QUERY["Visible query grain<br/>region + entity + product_family"]
  NUM["Numerator<br/>revenue at visible grain"]
  FIXED["Fixed LOD denominator<br/>predefined partition"]
  COARSER["Coarser LOD denominator<br/>visible grain minus selected fields"]
  RATIO["Business percentage metric"]

  QUERY --> NUM
  QUERY --> FIXED
  QUERY --> COARSER
  NUM --> RATIO
  FIXED --> RATIO
  COARSER --> RATIO
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixed vs Coarser LOD
# MAGIC
# MAGIC The two LOD patterns answer different business questions.
# MAGIC
# MAGIC | Pattern | Question it answers | Main syntax |
# MAGIC |---|---|---|
# MAGIC | Fixed LOD | "What share of a predefined total is this?" | Field expression with `SUM(...) OVER (PARTITION BY ...)` |
# MAGIC | Coarser LOD | "What share of the visible total is this after filters?" | Window measure with `range: all` |
# MAGIC
# MAGIC Fixed LOD fields are computed before query-time filters. If the denominator should include a filter such as `region = 'APJ'`, encode that logic in the LOD field expression itself.
# MAGIC
# MAGIC Coarser LOD is implemented with Metric View window measures. The Databricks documentation currently marks window measures as experimental, so check feature availability before relying on coarser LOD patterns in production.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  subgraph Fixed["Fixed LOD"]
    FDEF["Definition-time grain<br/>PARTITION BY fiscal_year"]
    FFILTER["Definition-time filter<br/>CASE WHEN region = APJ"]
    FQUERY["Query filter changes visible rows<br/>not the fixed denominator"]
  end

  subgraph Coarser["Coarser LOD"]
    CVISIBLE["Start from visible query grain"]
    CEXCLUDE["Exclude selected fields<br/>range: all"]
    CFILTER["Respect query-time filters"]
  end

  FDEF --> FQUERY
  FFILTER --> FQUERY
  CVISIBLE --> CEXCLUDE --> CFILTER
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the LOD Metric View
# MAGIC
# MAGIC This Metric View defines joins over the same base tables used in Part 1. It has no materialization block because this notebook is about semantics, not acceleration.
# MAGIC
# MAGIC The most important definitions are:
# MAGIC
# MAGIC - Fixed LOD fields such as `global_revenue_year_lod` and `apj_revenue_year_lod`.
# MAGIC - Ratio measures that use `ANY_VALUE(...)` to reference fixed LOD fields.
# MAGIC - Coarser LOD measures that use `range: all` to exclude visible fields from the denominator.
# MAGIC
# MAGIC Why `ANY_VALUE`? A fixed LOD field is still a field, not a measure. When a measure expression references it, SQL needs an aggregate wrapper. `ANY_VALUE` is appropriate here because the fixed denominator is constant within the relevant grouping grain.

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.lod_finance_metric_view
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Finance Metric View for the level-of-detail deep dive.
  It demonstrates fixed LOD denominators, query-filter behavior,
  and coarser LOD denominators using the same base tables as Part 1.
source: {catalog}.{schema}.mat_fact_finance_daily
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
    comment: Fiscal year derived from the calendar dimension.
  - name: fiscal_month
    expr: calendar.fiscal_month
    comment: Fiscal month derived from the calendar dimension.
  - name: fiscal_quarter
    expr: calendar.fiscal_quarter
    comment: Fiscal quarter derived from the calendar dimension.
  - name: region
    expr: entity.region
    comment: Global sales region.
  - name: country
    expr: entity.country
    comment: Country for the reporting entity.
  - name: entity_name
    expr: entity.entity_name
    comment: Legal or commercial reporting entity.
  - name: product_family
    expr: product.product_family
    comment: Product-family grouping used for dashboard drilldowns.
  - name: product_name
    expr: product.product_name
    comment: Product-level detail.
  - name: business_unit
    expr: product.business_unit
    comment: Higher-level product business unit.
  - name: segment_group
    expr: segment.segment_group
    comment: Customer segment rollup.
  - name: segment_name
    expr: segment.segment_name
    comment: Customer segment detail.
  - name: account_category
    expr: account.account_category
    comment: Finance account category such as Revenue, COGS, or Opex.

  - name: global_revenue_all_years_lod
    expr: SUM(CASE WHEN account.account_category = 'Revenue' THEN amount ELSE 0 END) OVER ()
    comment: Fixed LOD denominator for all revenue rows across all years. Used to show why filter-aware denominator design matters.
  - name: global_revenue_year_lod
    expr: SUM(CASE WHEN account.account_category = 'Revenue' THEN amount ELSE 0 END) OVER (PARTITION BY calendar.fiscal_year)
    comment: Fixed LOD denominator for total revenue by fiscal year.
  - name: apj_revenue_year_lod
    expr: SUM(CASE WHEN account.account_category = 'Revenue' AND entity.region = 'APJ' THEN amount ELSE 0 END) OVER (PARTITION BY calendar.fiscal_year)
    comment: Fixed LOD denominator for APJ revenue by fiscal year. The APJ filter is part of the definition, not the query.
  - name: product_family_revenue_year_lod
    expr: SUM(CASE WHEN account.account_category = 'Revenue' THEN amount ELSE 0 END) OVER (PARTITION BY calendar.fiscal_year, product.product_family)
    comment: Fixed LOD denominator for product-family revenue by fiscal year.
  - name: region_revenue_year_lod
    expr: SUM(CASE WHEN account.account_category = 'Revenue' THEN amount ELSE 0 END) OVER (PARTITION BY calendar.fiscal_year, entity.region)
    comment: Fixed LOD denominator for region revenue by fiscal year.
measures:
  - name: revenue
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Revenue')
    display_name: Revenue
    comment: Actual revenue from revenue accounts.
    format:
      type: number
      abbreviation: compact
      decimal_places:
        type: exact
        places: 2
  - name: cogs
    expr: SUM(amount) FILTER (WHERE account.account_category = 'COGS')
    display_name: COGS
    comment: Cost of goods sold.
  - name: opex
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Opex')
    display_name: Opex
    comment: Operating expense.
  - name: gross_profit
    expr: MEASURE(revenue) - MEASURE(cogs)
    display_name: Gross Profit
    comment: Revenue minus cost of goods sold.
  - name: transaction_count
    expr: COUNT(DISTINCT transaction_id)
    display_name: Transaction Count
    comment: Count of generated finance fact rows.

  - name: pct_of_global_revenue_all_years_fixed_lod
    expr: MEASURE(revenue) / NULLIF(ANY_VALUE(global_revenue_all_years_lod), 0)
    display_name: Percent of All-Years Global Revenue
    comment: Fixed LOD percentage using a denominator across all years. This is intentionally included to demonstrate filtering behavior.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: pct_of_global_revenue_year_fixed_lod
    expr: MEASURE(revenue) / NULLIF(ANY_VALUE(global_revenue_year_lod), 0)
    display_name: Percent of Fiscal-Year Global Revenue
    comment: Fixed LOD percentage using fiscal-year global revenue as the denominator.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: pct_of_apj_revenue_year_fixed_lod
    expr: MEASURE(revenue) / NULLIF(ANY_VALUE(apj_revenue_year_lod), 0)
    display_name: Percent of Fiscal-Year APJ Revenue
    comment: Fixed LOD percentage where the APJ denominator is encoded in the LOD field expression.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: pct_of_product_family_revenue_year_fixed_lod
    expr: MEASURE(revenue) / NULLIF(ANY_VALUE(product_family_revenue_year_lod), 0)
    display_name: Percent of Product-Family Revenue
    comment: Fixed LOD percentage using product-family revenue by fiscal year as the denominator.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: pct_of_region_revenue_year_fixed_lod
    expr: MEASURE(revenue) / NULLIF(ANY_VALUE(region_revenue_year_lod), 0)
    display_name: Percent of Region Revenue Fixed LOD
    comment: Fixed LOD percentage using region revenue by fiscal year as the denominator.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: region_revenue_excluding_entity
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Revenue')
    display_name: Region Revenue Excluding Entity
    comment: Coarser LOD denominator. It ignores entity_name while keeping the other visible query fields and filters.
    window:
      - order: entity_name
        range: all
        semiadditive: last
  - name: pct_of_region_revenue
    expr: MEASURE(revenue) / NULLIF(MEASURE(region_revenue_excluding_entity), 0)
    display_name: Percent of Region Revenue
    comment: Share of region revenue when the query groups by region and entity.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: revenue_excluding_entity_and_product_family
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Revenue')
    display_name: Revenue Excluding Entity and Product Family
    comment: Coarser LOD denominator that excludes entity_name and product_family from the visible query grain.
    window:
      - order: entity_name
        range: all
        semiadditive: last
      - order: product_family
        range: all
        semiadditive: last
  - name: pct_of_visible_total_excluding_entity_and_product
    expr: MEASURE(revenue) / NULLIF(MEASURE(revenue_excluding_entity_and_product_family), 0)
    display_name: Percent of Visible Total Excluding Entity and Product
    comment: Share of the broader visible total after excluding entity and product family from the grouping grain.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
$$
"""
)

print(f"Created {lod_mv}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the Metric View
# MAGIC
# MAGIC `DESCRIBE EXTENDED` is useful when teaching Metric Views because it confirms the YAML definition Databricks registered.

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {lod_mv}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixed LOD: Percent of Fiscal-Year Global Revenue
# MAGIC
# MAGIC The numerator is grouped by `region` and `product_family`.
# MAGIC
# MAGIC The denominator is fixed at fiscal-year global revenue because the `global_revenue_year_lod` field uses:
# MAGIC
# MAGIC ```yaml
# MAGIC expr: SUM(...) OVER (PARTITION BY calendar.fiscal_year)
# MAGIC ```
# MAGIC
# MAGIC The measure references that fixed value with `ANY_VALUE(global_revenue_year_lod)`.
# MAGIC
# MAGIC `ANY_VALUE` is not a shortcut for arbitrary data. It is safe here because `global_revenue_year_lod` has the same value for every row in the same fiscal year.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   fiscal_year,
# MAGIC   region,
# MAGIC   product_family,
# MAGIC   MEASURE(revenue) AS revenue,
# MAGIC   MEASURE(pct_of_global_revenue_year_fixed_lod) AS pct_of_global_revenue_2025
# MAGIC FROM lod_finance_metric_view
# MAGIC WHERE fiscal_year = 2025
# MAGIC GROUP BY ALL
# MAGIC ORDER BY region, product_family

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity Check: Global Percentages Sum to 100%
# MAGIC
# MAGIC Because the denominator is fiscal-year global revenue, the sum across all regions and product families for 2025 should be 1.0.

# COMMAND ----------

global_pct_total = spark.sql(
    """
SELECT
  SUM(pct_of_global_revenue_2025) AS pct_total
FROM (
  SELECT
    region,
    product_family,
    MEASURE(pct_of_global_revenue_year_fixed_lod) AS pct_of_global_revenue_2025
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
  GROUP BY ALL
)
"""
).collect()[0]["pct_total"]

print(f"2025 global percentage total: {global_pct_total}")
require_approx(global_pct_total, 1.0, "Global revenue percentages")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Denominator Check: Fixed LOD vs Base Tables
# MAGIC
# MAGIC The percentage sum proves the ratios add up. This check goes one level deeper: it compares the implied fixed LOD denominator from the Metric View with an independent base-table total.

# COMMAND ----------

mv_global_denominator = spark.sql(
    """
SELECT
  MAX(revenue / NULLIF(pct_of_global_revenue_2025, 0)) AS denominator
FROM (
  SELECT
    region,
    product_family,
    MEASURE(revenue) AS revenue,
    MEASURE(pct_of_global_revenue_year_fixed_lod) AS pct_of_global_revenue_2025
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
  GROUP BY ALL
)
"""
).collect()[0]["denominator"]

base_global_denominator = spark.sql(
    """
SELECT
  SUM(f.amount) AS denominator
FROM mat_fact_finance_daily f
JOIN mat_dim_calendar c
  ON f.transaction_date = c.calendar_date
JOIN mat_dim_account a
  ON f.account_id = a.account_id
WHERE c.fiscal_year = 2025
  AND a.account_category = 'Revenue'
"""
).collect()[0]["denominator"]

print(f"Metric View implied global denominator: {mv_global_denominator}")
print(f"Base-table global denominator: {base_global_denominator}")
require_approx(mv_global_denominator, base_global_denominator, "Fixed LOD global denominator", tolerance=0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixed LOD Filtering Behavior
# MAGIC
# MAGIC Fixed LOD fields are computed before query-time filters.
# MAGIC
# MAGIC This query filters visible rows to APJ in 2025 and compares three denominator choices:
# MAGIC
# MAGIC - All-years global revenue: intentionally too broad for a 2025-only numerator.
# MAGIC - Fiscal-year global revenue: correct when the business question is "share of global 2025 revenue."
# MAGIC - Fiscal-year APJ revenue: correct when the business question is "share of APJ 2025 revenue."

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   product_family,
# MAGIC   MEASURE(revenue) AS revenue,
# MAGIC   MEASURE(pct_of_global_revenue_all_years_fixed_lod) AS pct_of_all_years_global_revenue,
# MAGIC   MEASURE(pct_of_global_revenue_year_fixed_lod) AS pct_of_2025_global_revenue,
# MAGIC   MEASURE(pct_of_apj_revenue_year_fixed_lod) AS pct_of_2025_apj_revenue
# MAGIC FROM lod_finance_metric_view
# MAGIC WHERE fiscal_year = 2025
# MAGIC   AND region = 'APJ'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY product_family

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity Check: APJ Percentages
# MAGIC
# MAGIC The APJ denominator is encoded inside the fixed LOD expression, so APJ product-family percentages should sum to 1.0.
# MAGIC
# MAGIC The all-years global denominator should not sum to 1.0 because it intentionally uses a broader denominator than the query.

# COMMAND ----------

apj_totals = spark.sql(
    """
SELECT
  SUM(pct_of_all_years_global_revenue) AS pct_all_years_global,
  SUM(pct_of_2025_global_revenue) AS pct_2025_global,
  SUM(pct_of_2025_apj_revenue) AS pct_2025_apj
FROM (
  SELECT
    product_family,
    MEASURE(pct_of_global_revenue_all_years_fixed_lod) AS pct_of_all_years_global_revenue,
    MEASURE(pct_of_global_revenue_year_fixed_lod) AS pct_of_2025_global_revenue,
    MEASURE(pct_of_apj_revenue_year_fixed_lod) AS pct_of_2025_apj_revenue
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
    AND region = 'APJ'
  GROUP BY ALL
)
"""
).collect()[0]

print(f"APJ percent of all-years global revenue: {apj_totals['pct_all_years_global']}")
print(f"APJ percent of 2025 global revenue: {apj_totals['pct_2025_global']}")
print(f"APJ percent of 2025 APJ revenue: {apj_totals['pct_2025_apj']}")

require_approx(apj_totals["pct_2025_apj"], 1.0, "APJ fixed LOD percentages")
assert apj_totals["pct_all_years_global"] < 1.0, "All-years denominator should not sum to 100% for APJ 2025 rows"
assert apj_totals["pct_2025_global"] < 1.0, "Global denominator should not sum to 100% for APJ-only rows"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Denominator Check: APJ Definition-Time Filter
# MAGIC
# MAGIC This confirms that `apj_revenue_year_lod` uses the APJ condition encoded in the Metric View definition, not only the query-time `WHERE region = 'APJ'` filter.

# COMMAND ----------

mv_apj_denominator = spark.sql(
    """
SELECT
  MAX(revenue / NULLIF(pct_of_2025_apj_revenue, 0)) AS denominator
FROM (
  SELECT
    product_family,
    MEASURE(revenue) AS revenue,
    MEASURE(pct_of_apj_revenue_year_fixed_lod) AS pct_of_2025_apj_revenue
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
    AND region = 'APJ'
  GROUP BY ALL
)
"""
).collect()[0]["denominator"]

base_apj_denominator = spark.sql(
    """
SELECT
  SUM(f.amount) AS denominator
FROM mat_fact_finance_daily f
JOIN mat_dim_calendar c
  ON f.transaction_date = c.calendar_date
JOIN mat_dim_entity e
  ON f.entity_id = e.entity_id
JOIN mat_dim_account a
  ON f.account_id = a.account_id
WHERE c.fiscal_year = 2025
  AND e.region = 'APJ'
  AND a.account_category = 'Revenue'
"""
).collect()[0]["denominator"]

print(f"Metric View implied APJ denominator: {mv_apj_denominator}")
print(f"Base-table APJ denominator: {base_apj_denominator}")
require_approx(mv_apj_denominator, base_apj_denominator, "Fixed LOD APJ denominator", tolerance=0.01)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixed LOD With a Different Predefined Grain
# MAGIC
# MAGIC A fixed LOD denominator does not have to be global. Here, `product_family_revenue_year_lod` partitions by fiscal year and product family.
# MAGIC
# MAGIC That lets the query ask: "Within each product family, which regions contribute the revenue?"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   fiscal_year,
# MAGIC   product_family,
# MAGIC   region,
# MAGIC   MEASURE(revenue) AS revenue,
# MAGIC   MEASURE(pct_of_product_family_revenue_year_fixed_lod) AS pct_of_product_family_revenue
# MAGIC FROM lod_finance_metric_view
# MAGIC WHERE fiscal_year = 2025
# MAGIC GROUP BY ALL
# MAGIC ORDER BY product_family, region

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coarser LOD: Percent of Region Revenue
# MAGIC
# MAGIC Coarser LOD starts with the visible query grain, then excludes one or more fields.
# MAGIC
# MAGIC `region_revenue_excluding_entity` uses:
# MAGIC
# MAGIC ```yaml
# MAGIC window:
# MAGIC   - order: entity_name
# MAGIC     range: all
# MAGIC     semiadditive: last
# MAGIC ```
# MAGIC
# MAGIC When the query groups by `region` and `entity_name`, the denominator ignores `entity_name` and keeps the region-level total.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   entity_name,
# MAGIC   MEASURE(revenue) AS entity_revenue,
# MAGIC   MEASURE(region_revenue_excluding_entity) AS region_revenue,
# MAGIC   MEASURE(pct_of_region_revenue) AS pct_of_region_revenue
# MAGIC FROM lod_finance_metric_view
# MAGIC WHERE fiscal_year = 2025
# MAGIC GROUP BY ALL
# MAGIC ORDER BY region, entity_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity Check: Entity Percentages Sum to 100% Within Region
# MAGIC
# MAGIC Because the coarser denominator excludes `entity_name`, each region's entity percentages should sum to 1.0.

# COMMAND ----------

region_totals = spark.sql(
    """
SELECT
  region,
  SUM(pct_of_region_revenue) AS pct_total
FROM (
  SELECT
    region,
    entity_name,
    MEASURE(pct_of_region_revenue) AS pct_of_region_revenue
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
  GROUP BY ALL
)
GROUP BY region
ORDER BY region
"""
).collect()

for row in region_totals:
    print(f"{row['region']}: {row['pct_total']}")
    require_approx(row["pct_total"], 1.0, f"{row['region']} entity percentages")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Denominator Check: Coarser LOD vs Base Tables
# MAGIC
# MAGIC This verifies that the coarser LOD denominator for each region matches the independent region revenue total from the base tables.

# COMMAND ----------

mv_region_denominators = {
    row["region"]: row["denominator"]
    for row in spark.sql(
        """
SELECT
  region,
  MAX(region_revenue) AS denominator
FROM (
  SELECT
    region,
    entity_name,
    MEASURE(region_revenue_excluding_entity) AS region_revenue
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
  GROUP BY ALL
)
GROUP BY region
"""
    ).collect()
}

base_region_denominators = {
    row["region"]: row["denominator"]
    for row in spark.sql(
        """
SELECT
  e.region,
  SUM(f.amount) AS denominator
FROM mat_fact_finance_daily f
JOIN mat_dim_calendar c
  ON f.transaction_date = c.calendar_date
JOIN mat_dim_entity e
  ON f.entity_id = e.entity_id
JOIN mat_dim_account a
  ON f.account_id = a.account_id
WHERE c.fiscal_year = 2025
  AND a.account_category = 'Revenue'
GROUP BY e.region
"""
    ).collect()
}

for region, denominator in sorted(mv_region_denominators.items()):
    print(f"{region}: Metric View={denominator}, base tables={base_region_denominators[region]}")
    require_approx(
        denominator,
        base_region_denominators[region],
        f"Coarser LOD region denominator for {region}",
        tolerance=0.01,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coarser LOD With Multiple Excluded Fields
# MAGIC
# MAGIC Coarser LOD can exclude more than one field by adding multiple window entries.
# MAGIC
# MAGIC This query filters to APJ and groups by `entity_name` and `product_family`. The denominator excludes both fields, so each row is a share of the APJ visible total for 2025.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   entity_name,
# MAGIC   product_family,
# MAGIC   MEASURE(revenue) AS revenue,
# MAGIC   MEASURE(revenue_excluding_entity_and_product_family) AS apj_visible_total_revenue,
# MAGIC   MEASURE(pct_of_visible_total_excluding_entity_and_product) AS pct_of_apj_visible_total
# MAGIC FROM lod_finance_metric_view
# MAGIC WHERE fiscal_year = 2025
# MAGIC   AND region = 'APJ'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY entity_name, product_family

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity Check: Multi-Field Coarser LOD Sums to 100%
# MAGIC
# MAGIC Since the query filters to APJ and the denominator excludes both `entity_name` and `product_family`, the displayed rows should sum to 1.0.

# COMMAND ----------

apj_visible_total = spark.sql(
    """
SELECT
  SUM(pct_of_apj_visible_total) AS pct_total
FROM (
  SELECT
    entity_name,
    product_family,
    MEASURE(pct_of_visible_total_excluding_entity_and_product) AS pct_of_apj_visible_total
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
    AND region = 'APJ'
  GROUP BY ALL
)
"""
).collect()[0]["pct_total"]

print(f"APJ entity/product visible total percentage: {apj_visible_total}")
require_approx(apj_visible_total, 1.0, "APJ visible total percentages")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Denominator Check: Multi-Field Coarser LOD
# MAGIC
# MAGIC The multi-field coarser denominator should equal APJ 2025 revenue because the query filters to APJ and excludes both `entity_name` and `product_family`.

# COMMAND ----------

mv_apj_visible_denominator = spark.sql(
    """
SELECT
  MAX(apj_visible_total_revenue) AS denominator
FROM (
  SELECT
    entity_name,
    product_family,
    MEASURE(revenue_excluding_entity_and_product_family) AS apj_visible_total_revenue
  FROM lod_finance_metric_view
  WHERE fiscal_year = 2025
    AND region = 'APJ'
  GROUP BY ALL
)
"""
).collect()[0]["denominator"]

print(f"Metric View multi-field coarser denominator: {mv_apj_visible_denominator}")
print(f"Base-table APJ denominator: {base_apj_denominator}")
require_approx(
    mv_apj_visible_denominator,
    base_apj_denominator,
    "Multi-field coarser LOD APJ denominator",
    tolerance=0.01,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choosing the Right LOD Pattern
# MAGIC
# MAGIC Use fixed LOD when the denominator is a governed business definition:
# MAGIC
# MAGIC - Percent of global revenue
# MAGIC - Percent of APJ revenue
# MAGIC - Percent of product-family revenue
# MAGIC
# MAGIC Use coarser LOD when the denominator should adapt to the current query filters while ignoring selected visible fields:
# MAGIC
# MAGIC - Entity share of region revenue
# MAGIC - Product-family share of selected region revenue
# MAGIC - Drilldown shares inside a filtered dashboard
# MAGIC
# MAGIC The important design question is always: **percent of what?**
# MAGIC
# MAGIC Once that denominator is encoded in the Metric View, users can query governed percentages without rewriting subqueries, CTEs, or dashboard-specific SQL.

# COMMAND ----------

print("LOD deep dive checks completed successfully.")
