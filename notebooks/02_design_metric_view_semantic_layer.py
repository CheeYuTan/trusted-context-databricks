# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Design the Metric View Semantic Layer
# MAGIC
# MAGIC This notebook is the core teaching asset.
# MAGIC
# MAGIC It explains how to design a Metric View as a governed semantic layer, not as a bag of SQL snippets.
# MAGIC
# MAGIC We will create `finance_metric_view` without materialization. This is intentional:
# MAGIC
# MAGIC - First, teach the business model and metric semantics.
# MAGIC - Then, in the next notebook, create a materialized variant and compare performance.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakemeter_catalog", "Catalog")
dbutils.widgets.text("schema", "metric_views_lod_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
qualified_schema = f"`{catalog}`.`{schema}`"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

source_name = f"{catalog}.{schema}.finance_semantic_base"
metric_view_name = f"{catalog}.{schema}.finance_metric_view"

print(f"Source: {source_name}")
print(f"Metric View: {metric_view_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semantic Layer Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC   subgraph Source["Source data model"]
# MAGIC     BASE["finance_semantic_base<br/>normalized multi-grain source"]
# MAGIC   end
# MAGIC
# MAGIC   subgraph MV["finance_metric_view"]
# MAGIC     FIELDS["Fields<br/>business grouping/filtering surface"]
# MAGIC     ATOMIC["Atomic measures<br/>SUM, COUNT, filtered aggregates"]
# MAGIC     COMPOSED["Composed measures<br/>MEASURE(...) references"]
# MAGIC     LOD["LOD measures<br/>fixed and coarser denominators"]
# MAGIC     WINDOWS["Window measures<br/>current, YTD, rolling, prior year"]
# MAGIC     META["Agent metadata<br/>display names, formats, synonyms"]
# MAGIC   end
# MAGIC
# MAGIC   subgraph Consumers["Consumers"]
# MAGIC     SQL["SQL analysts"]
# MAGIC     DASH["AI/BI dashboards"]
# MAGIC     GENIE["Genie / natural language"]
# MAGIC     BI["External BI tools"]
# MAGIC   end
# MAGIC
# MAGIC   BASE --> FIELDS
# MAGIC   BASE --> ATOMIC
# MAGIC   FIELDS --> LOD
# MAGIC   ATOMIC --> COMPOSED
# MAGIC   ATOMIC --> WINDOWS
# MAGIC   COMPOSED --> DASH
# MAGIC   LOD --> DASH
# MAGIC   WINDOWS --> DASH
# MAGIC   META --> GENIE
# MAGIC   FIELDS --> SQL
# MAGIC   COMPOSED --> BI
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Principle 1: Fields Are the Business Surface
# MAGIC
# MAGIC Fields are not just columns. They define the dimensions users are allowed to group and filter by.
# MAGIC
# MAGIC For this tutorial, the fields expose these hierarchies:
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC   Date["event_date"] --> Month["fiscal_month"] --> Quarter["fiscal_quarter"] --> Year["fiscal_year"]
# MAGIC   Account["account_name"] --> Category["account_category"] --> Section["statement_section"]
# MAGIC   Product["product_name"] --> Family["product_family"] --> BU["business_unit"]
# MAGIC   Entity["entity_name"] --> Country["country"] --> Region["region"]
# MAGIC   Segment["segment_name"] --> SegmentGroup["segment_group"]
# MAGIC ```
# MAGIC
# MAGIC Notice the time hierarchy: window measures should group by fields that are defined from the ordering field. This matters for YTD and rolling calculations.

# COMMAND ----------

fields_yaml = """
fields:
  - name: event_date
    expr: event_date
    display_name: Event Date
    format:
      type: date
      date_format: year_month_day

  - name: fiscal_month
    expr: DATE_TRUNC('MONTH', event_date)
    display_name: Fiscal Month
    format:
      type: date
      date_format: year_month_day
    synonyms:
      - month
      - accounting month

  - name: fiscal_year_start
    expr: make_date(year(event_date), 1, 1)
    display_name: Fiscal Year Start
    format:
      type: date
      date_format: year_month_day

  - name: fiscal_quarter
    expr: concat(year(event_date), '-Q', quarter(event_date))
    display_name: Fiscal Quarter
    synonyms:
      - quarter
      - accounting quarter

  - name: fiscal_year
    expr: year(event_date)
    display_name: Fiscal Year
    synonyms:
      - year
      - accounting year

  - name: region
    expr: region
    display_name: Region

  - name: country
    expr: country
    display_name: Country

  - name: entity_name
    expr: entity_name
    display_name: Entity
    synonyms:
      - legal entity
      - company

  - name: business_unit
    expr: business_unit
    display_name: Business Unit

  - name: product_family
    expr: product_family
    display_name: Product Family
    synonyms:
      - product group

  - name: product_name
    expr: product_name
    display_name: Product

  - name: segment_group
    expr: segment_group
    display_name: Segment Group

  - name: segment_name
    expr: segment_name
    display_name: Customer Segment
    synonyms:
      - segment
      - customer type

  - name: statement_section
    expr: statement_section
    display_name: Statement Section

  - name: account_category
    expr: account_category
    display_name: Account Category
    synonyms:
      - account group
      - financial category

  - name: account_name
    expr: account_name
    display_name: Account

  - name: scenario_name
    expr: scenario_name
    display_name: Scenario
    synonyms:
      - actual budget forecast
"""

print(fields_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Principle 2: Fixed LOD Fields Create Stable Denominators
# MAGIC
# MAGIC Fixed LOD expressions are defined as fields using SQL window functions.
# MAGIC
# MAGIC They are useful when a denominator must be calculated at a stable grain regardless of how the dashboard groups the query.
# MAGIC
# MAGIC Examples:
# MAGIC
# MAGIC - `global_revenue_lod`: total actual revenue across the whole source.
# MAGIC - `account_category_revenue_lod`: revenue by account category.
# MAGIC - `product_family_revenue_lod`: revenue by product family.
# MAGIC
# MAGIC These fields are later wrapped with `ANY_VALUE(...)` inside measures because a measure expression must aggregate every field it references.

# COMMAND ----------

lod_fields_yaml = """
  - name: global_revenue_lod
    expr: SUM(CASE WHEN source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue' THEN amount ELSE 0 END) OVER ()
    comment: Fixed LOD field for total actual revenue across the full source.
    display_name: Global Revenue LOD
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: account_category_revenue_lod
    expr: SUM(CASE WHEN source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue' THEN amount ELSE 0 END) OVER (PARTITION BY account_category)
    comment: Fixed LOD field for actual revenue by account category.
    display_name: Account Category Revenue LOD
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: product_family_revenue_lod
    expr: SUM(CASE WHEN source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue' THEN amount ELSE 0 END) OVER (PARTITION BY product_family)
    comment: Fixed LOD field for actual revenue by product family.
    display_name: Product Family Revenue LOD
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: apj_revenue_lod
    expr: SUM(CASE WHEN source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue' AND region = 'APJ' THEN amount ELSE 0 END) OVER ()
    comment: Fixed LOD field with the APJ filter encoded inside the expression.
    display_name: APJ Revenue LOD
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
"""

print(lod_fields_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Principle 3: Define Atomic Measures Before Composed Measures
# MAGIC
# MAGIC Atomic measures are direct aggregations over the source.
# MAGIC
# MAGIC They should be simple, auditable, and reusable:
# MAGIC
# MAGIC - `actual_revenue`
# MAGIC - `actual_cogs`
# MAGIC - `actual_opex`
# MAGIC - `budget_revenue`
# MAGIC - `transaction_count`
# MAGIC
# MAGIC Composed measures should reference those atomic measures using `MEASURE(...)`:
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC   Revenue["actual_revenue"] --> Gross["gross_profit"]
# MAGIC   COGS["actual_cogs"] --> Gross
# MAGIC   Gross --> EBITDA["ebitda"]
# MAGIC   Opex["actual_opex"] --> EBITDA
# MAGIC   EBITDA --> Margin["ebitda_margin_pct"]
# MAGIC   Revenue --> Margin
# MAGIC ```

# COMMAND ----------

atomic_measures_yaml = """
measures:
  - name: actual_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    comment: Actual revenue from journal-line transactions.
    display_name: Actual Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - sales
      - turnover
      - net revenue

  - name: actual_cogs
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'COGS')
    display_name: Actual COGS
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: actual_opex
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Opex')
    display_name: Actual Opex
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: actual_expense
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND normal_balance = 'Expense')
    display_name: Actual Expense
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: budget_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'TARGET' AND scenario_id = 'BUDGET' AND account_category = 'Revenue')
    display_name: Budget Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: transaction_count
    expr: COUNT(DISTINCT source_record_id) FILTER (WHERE source_grain = 'GL')
    display_name: Transaction Count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      abbreviation: compact
"""

print(atomic_measures_yaml)

# COMMAND ----------

composed_measures_yaml = """
  - name: gross_profit
    expr: MEASURE(actual_revenue) - MEASURE(actual_cogs)
    display_name: Gross Profit
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: ebitda
    expr: MEASURE(actual_revenue) - MEASURE(actual_cogs) - MEASURE(actual_opex)
    display_name: EBITDA
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - operating earnings
      - operating profit

  - name: ebitda_margin_pct
    expr: MEASURE(ebitda) / NULLIF(MEASURE(actual_revenue), 0)
    display_name: EBITDA Margin %
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - operating margin
      - profitability

  - name: revenue_variance
    expr: MEASURE(actual_revenue) - MEASURE(budget_revenue)
    display_name: Revenue Variance
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: revenue_variance_pct
    expr: MEASURE(revenue_variance) / NULLIF(MEASURE(budget_revenue), 0)
    display_name: Revenue Variance %
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
"""

print(composed_measures_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Principle 4: Coarser LOD Measures Use `range: all`
# MAGIC
# MAGIC Coarser LOD expressions calculate at a broader grain than the visible query.
# MAGIC
# MAGIC Example: when grouped by entity, `region_revenue_excluding_entity` excludes `entity_name` from the calculation grain so each entity can be divided by its region total.
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC   Query["Query grain:<br/>region + entity"] --> Numerator["actual_revenue<br/>region + entity"]
# MAGIC   Query --> Denominator["region_revenue_excluding_entity<br/>region only"]
# MAGIC   Numerator --> Ratio["pct_of_region_revenue"]
# MAGIC   Denominator --> Ratio
# MAGIC ```

# COMMAND ----------

lod_measures_yaml = """
  - name: pct_of_global_revenue_fixed_lod
    expr: MEASURE(actual_revenue) / NULLIF(ANY_VALUE(global_revenue_lod), 0)
    comment: Fixed LOD percentage using a global denominator.
    display_name: Percent of Global Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: pct_of_account_category_revenue_fixed_lod
    expr: MEASURE(actual_revenue) / NULLIF(ANY_VALUE(account_category_revenue_lod), 0)
    comment: Fixed LOD percentage using account-category denominator.
    display_name: Percent of Account Category Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: pct_of_product_family_revenue_fixed_lod
    expr: MEASURE(actual_revenue) / NULLIF(ANY_VALUE(product_family_revenue_lod), 0)
    comment: Fixed LOD percentage using product-family denominator.
    display_name: Percent of Product Family Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: pct_of_apj_revenue_fixed_lod
    expr: MEASURE(actual_revenue) / NULLIF(ANY_VALUE(apj_revenue_lod), 0)
    comment: Fixed LOD percentage where the APJ denominator is encoded in the LOD field.
    display_name: Percent of APJ Revenue Fixed LOD
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: all_account_categories_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: account_category
        range: all
        semiadditive: last
    comment: Coarser LOD denominator that excludes account category from the query grain.
    display_name: All Account Categories Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: pct_of_visible_total_revenue_coarser_lod
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(all_account_categories_revenue), 0)
    comment: Coarser LOD percentage that can remain filter-aware.
    display_name: Percent of Visible Total Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: region_revenue_excluding_entity
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: entity_name
        range: all
        semiadditive: last
    comment: Coarser LOD denominator for entity-level contribution within region.
    display_name: Region Revenue Excluding Entity
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: pct_of_region_revenue
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(region_revenue_excluding_entity), 0)
    display_name: Percent of Region Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: revenue_excluding_entity_and_product_family
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: entity_name
        range: all
        semiadditive: last
      - order: product_family
        range: all
        semiadditive: last
    comment: Coarser LOD denominator that excludes multiple fields from the query grain.
    display_name: Revenue Excluding Entity and Product Family
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: pct_of_entity_product_visible_total
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(revenue_excluding_entity_and_product_family), 0)
    comment: Demonstrates excluding multiple fields with range all.
    display_name: Percent of Entity/Product Visible Total
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
"""

print(lod_measures_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Principle 5: Window Measures Encode Time Intelligence
# MAGIC
# MAGIC Window measures let the metric definition own time intelligence.
# MAGIC
# MAGIC We use:
# MAGIC
# MAGIC - `range: current` for current month values.
# MAGIC - `range: cumulative` plus a fiscal-year limiter for YTD.
# MAGIC - `range: trailing 12 month inclusive` for rolling 12-month revenue.
# MAGIC - `offset: -12 month` for prior-year comparison.
# MAGIC - `range: current` and `semiadditive: last` for month-end balances.
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC   Current["current_month_revenue<br/>range: current"]
# MAGIC   YTD["ytd_revenue<br/>cumulative fiscal_month<br/>current fiscal_year_start"]
# MAGIC   R12["rolling_12_month_revenue<br/>trailing 12 month inclusive"]
# MAGIC   PY["prior_year_revenue<br/>current + offset -12 month"]
# MAGIC   YOY["yoy_revenue_growth_pct<br/>composed from current and prior year"]
# MAGIC
# MAGIC   Current --> YOY
# MAGIC   PY --> YOY
# MAGIC ```

# COMMAND ----------

window_measures_yaml = """
  - name: current_month_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: current
        semiadditive: last
    display_name: Current Month Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: ytd_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: cumulative
        semiadditive: last
      - order: fiscal_year_start
        range: current
        semiadditive: last
    display_name: YTD Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - year to date sales
      - ytd sales

  - name: running_total_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: cumulative
        semiadditive: last
    comment: Cumulative revenue across the full available time range.
    display_name: Running Total Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: rolling_12_month_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: trailing 12 month inclusive
        semiadditive: last
    display_name: Rolling 12 Month Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - r12 revenue
      - trailing twelve month revenue

  - name: prior_year_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: current
        semiadditive: last
        offset: -12 month
    display_name: Prior Year Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: yoy_revenue_growth
    expr: MEASURE(current_month_revenue) - MEASURE(prior_year_revenue)
    display_name: YoY Revenue Growth
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: yoy_revenue_growth_pct
    expr: MEASURE(yoy_revenue_growth) / NULLIF(MEASURE(prior_year_revenue), 0)
    display_name: YoY Revenue Growth %
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: trailing_3_month_revenue_exclusive
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: trailing 3 month exclusive
        semiadditive: last
    comment: Trailing 3 months excluding the anchor month.
    display_name: Trailing 3 Month Revenue Exclusive
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: trailing_3_month_revenue_inclusive
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: trailing 3 month inclusive
        semiadditive: last
    comment: Trailing 3 months including the anchor month.
    display_name: Trailing 3 Month Revenue Inclusive
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: next_month_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: leading 1 month
        semiadditive: first
    comment: Leading window example. Returns the next month's revenue when grouped by fiscal month.
    display_name: Next Month Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
"""

print(window_measures_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design Principle 6: Semiadditive Measures Prevent Wrong Balance Rollups
# MAGIC
# MAGIC Balances can be summed across entities, products, and accounts at a point in time.
# MAGIC
# MAGIC They should not be summed across time.
# MAGIC
# MAGIC `month_end_balance` uses:
# MAGIC
# MAGIC ```yaml
# MAGIC window:
# MAGIC   - order: fiscal_month
# MAGIC     range: current
# MAGIC     semiadditive: last
# MAGIC ```
# MAGIC
# MAGIC That tells the engine: when the query does not group by month, use the latest applicable month instead of summing all months.

# COMMAND ----------

balance_measures_yaml = """
  - name: balance_additive_snapshot
    expr: SUM(balance_amount) FILTER (WHERE source_grain = 'BALANCE')
    display_name: Balance Additive Snapshot
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: month_end_balance
    expr: SUM(balance_amount) FILTER (WHERE source_grain = 'BALANCE')
    window:
      - order: fiscal_month
        range: current
        semiadditive: last
    comment: Semiadditive month-end balance. Sums across entities/products, but not across months.
    display_name: Month-End Balance
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - closing balance
      - ending balance
"""

print(balance_measures_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Base Metric View
# MAGIC
# MAGIC The base Metric View intentionally has **no materialization block**.
# MAGIC
# MAGIC This keeps the semantic definition clean and easy to teach. In the next notebook, we create a second materialized Metric View to compare query behavior and explain performance acceleration.

# COMMAND ----------

metric_view_yaml = f"""
version: 1.1
comment: |-
  Finance semantic model demonstrating LOD, window measures, and agent metadata.
  This base Metric View is intentionally not materialized.
source: {source_name}

{fields_yaml}
{lod_fields_yaml}
{atomic_measures_yaml}
{composed_measures_yaml}
{lod_measures_yaml}
{window_measures_yaml}
{balance_measures_yaml}
"""

spark.sql(
    f"""
CREATE OR REPLACE VIEW {metric_view_name}
WITH METRICS
LANGUAGE YAML
AS $$
{metric_view_yaml}
$$
"""
)

print(f"Created non-materialized Metric View: {metric_view_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the Metric View
# MAGIC
# MAGIC The output below is useful for a blog screenshot because it proves:
# MAGIC
# MAGIC - The object type is `METRIC_VIEW`.
# MAGIC - Comments and agent metadata are stored with fields and measures.
# MAGIC - The base Metric View has no materialization refresh metadata.

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {metric_view_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke-Test the Semantic Layer
# MAGIC
# MAGIC We should be able to query business metrics without rewriting the business logic.

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT
  fiscal_month,
  region,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(ebitda) AS ebitda,
  MEASURE(ebitda_margin_pct) AS ebitda_margin_pct,
  MEASURE(ytd_revenue) AS ytd_revenue
FROM {metric_view_name}
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Derived Metric View
# MAGIC
# MAGIC Metric Views are composable across views. This second view uses `finance_metric_view` as its source and adds an executive-facing score.

# COMMAND ----------

exec_metric_view_name = f"{catalog}.{schema}.finance_exec_metric_view"

spark.sql(
    f"""
CREATE OR REPLACE VIEW {exec_metric_view_name}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Derived executive Metric View that demonstrates composability across Metric Views.
source: {metric_view_name}
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
measures:
  - name: revenue_per_transaction
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(transaction_count), 0)
    comment: Derived from measures in the source Metric View.
    display_name: Revenue per Transaction
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
  - name: executive_score
    expr: MEASURE(ebitda_margin_pct) + MEASURE(revenue_variance_pct)
    comment: Toy composite score to show cross-Metric-View measure composition.
    display_name: Executive Score
    format:
      type: number
      decimal_places:
        type: exact
        places: 4
$$
"""
)

display(
    spark.sql(
        f"""
SELECT
  fiscal_month,
  region,
  MEASURE(revenue_per_transaction) AS revenue_per_transaction,
  MEASURE(executive_score) AS executive_score
FROM {exec_metric_view_name}
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

