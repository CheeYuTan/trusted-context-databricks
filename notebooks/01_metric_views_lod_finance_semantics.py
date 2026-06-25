# Databricks notebook source
# MAGIC %md
# MAGIC # Modeling Business Semantics with Databricks Metric Views
# MAGIC
# MAGIC This tutorial showcases how Databricks Metric Views can model governed business semantics across calculation grains:
# MAGIC
# MAGIC - Level of detail (LOD) expressions
# MAGIC - Window measures and composability
# MAGIC - Agent metadata for AI/BI tools
# MAGIC - Materialization and aggregate-aware query rewrite
# MAGIC
# MAGIC The scenario uses a finance analytics model with transaction-level P&L data, monthly targets, and semiadditive month-end balances.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters
# MAGIC
# MAGIC Set the catalog and schema where the tutorial assets should be created.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "metric_views_lod_demo", "Schema")
dbutils.widgets.dropdown("enable_materialization", "false", ["false", "true"], "Enable materialization")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
enable_materialization = dbutils.widgets.get("enable_materialization").lower() == "true"

qualified_schema = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"Using schema: {catalog}.{schema}")
print(f"Materialization enabled: {enable_materialization}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Model Diagrams
# MAGIC
# MAGIC The demo has three business grains:
# MAGIC
# MAGIC - Journal transaction grain: actual P&L rows
# MAGIC - Monthly planning grain: budget and forecast rows
# MAGIC - Month-end balance grain: balances that should not be summed across time

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch


def draw_box(ax, xy, text, width=2.9, height=0.7, color="#E8F1FF"):
    box = FancyBboxPatch(
        xy,
        width,
        height,
        boxstyle="round,pad=0.04,rounding_size=0.04",
        linewidth=1.2,
        edgecolor="#1F4E79",
        facecolor=color,
    )
    ax.add_patch(box)
    ax.text(
        xy[0] + width / 2,
        xy[1] + height / 2,
        text,
        ha="center",
        va="center",
        fontsize=9,
        wrap=True,
    )
    return box


def draw_arrow(ax, start, end):
    ax.add_patch(
        FancyArrowPatch(
            start,
            end,
            arrowstyle="->",
            mutation_scale=12,
            linewidth=1.0,
            color="#555555",
        )
    )


fig, ax = plt.subplots(figsize=(12, 7))
ax.axis("off")
ax.set_xlim(0, 12)
ax.set_ylim(0, 7)

draw_box(ax, (4.6, 5.7), "finance_semantic_base\ncommon semantic source", 3.2, 0.8, "#FFF3CD")
draw_box(ax, (0.5, 4.4), "fact_gl_transactions\njournal-line grain")
draw_box(ax, (4.6, 4.4), "fact_monthly_targets\nmonthly plan grain")
draw_box(ax, (8.7, 4.4), "fact_month_end_balances\nmonth-end balance grain")

draw_box(ax, (0.5, 2.7), "dim_account\naccount hierarchy", color="#EAF7EA")
draw_box(ax, (3.4, 2.7), "dim_product\nproduct hierarchy", color="#EAF7EA")
draw_box(ax, (6.3, 2.7), "dim_entity\nentity -> country -> region", color="#EAF7EA")
draw_box(ax, (9.2, 2.7), "dim_calendar\nfiscal hierarchy", color="#EAF7EA")
draw_box(ax, (3.4, 1.3), "Metric View\nLOD + windows + metadata", 3.2, 0.8, "#F8D7DA")
draw_box(ax, (7.4, 1.3), "AI/BI Dashboard\nGenie / BI / SQL", 3.2, 0.8, "#D1ECF1")

for start in [(2.0, 4.4), (6.2, 4.4), (10.2, 4.4)]:
    draw_arrow(ax, start, (6.2, 5.7))

for start in [(2.0, 2.7), (4.9, 2.7), (7.8, 2.7), (10.7, 2.7)]:
    draw_arrow(ax, start, (6.0, 5.7))

draw_arrow(ax, (6.2, 5.7), (5.0, 2.1))
draw_arrow(ax, (6.6, 1.7), (7.4, 1.7))

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Synthetic Finance Data
# MAGIC
# MAGIC This section creates a compact but realistic model. The data intentionally mixes different levels of detail so that the Metric View can demonstrate calculation-grain control.

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE dim_account AS
SELECT * FROM VALUES
  ('A4000','Product Revenue','Revenue','P&L','Revenue'),
  ('A4010','Service Revenue','Revenue','P&L','Revenue'),
  ('A5000','Cost of Goods Sold','COGS','P&L','Expense'),
  ('A6100','Sales Expense','Opex','P&L','Expense'),
  ('A6200','Technology Expense','Opex','P&L','Expense'),
  ('A1000','Cash Balance','Cash','Balance Sheet','Asset'),
  ('A1100','Accounts Receivable','Receivables','Balance Sheet','Asset'),
  ('A2000','Deferred Revenue','Deferred Revenue','Balance Sheet','Liability')
AS dim_account(account_id, account_name, account_category, statement_section, normal_balance)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_product AS
SELECT * FROM VALUES
  ('P01','Payments API','Digital Platforms','Platform'),
  ('P02','Data Exchange','Digital Platforms','Platform'),
  ('P03','Risk Analytics','Analytics','Software'),
  ('P04','Treasury Insights','Analytics','Software'),
  ('P05','Advisory Services','Services','Services')
AS dim_product(product_id, product_name, product_family, business_unit)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_entity AS
SELECT * FROM VALUES
  ('E_SG','Singapore Entity','Singapore','APJ'),
  ('E_AU','Australia Entity','Australia','APJ'),
  ('E_US','US Entity','United States','AMER'),
  ('E_UK','UK Entity','United Kingdom','EMEA')
AS dim_entity(entity_id, entity_name, country, region)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_customer_segment AS
SELECT * FROM VALUES
  ('S_ENT','Enterprise','Strategic'),
  ('S_MM','Mid-Market','Commercial'),
  ('S_SMB','SMB','Commercial')
AS dim_customer_segment(segment_id, segment_name, segment_group)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_scenario AS
SELECT * FROM VALUES
  ('ACTUAL','Actual'),
  ('BUDGET','Budget'),
  ('FORECAST','Forecast')
AS dim_scenario(scenario_id, scenario_name)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_calendar AS
WITH dates AS (
  SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) AS calendar_date
)
SELECT
  calendar_date,
  date_trunc('MONTH', calendar_date) AS fiscal_month,
  concat(year(calendar_date), '-Q', quarter(calendar_date)) AS fiscal_quarter,
  year(calendar_date) AS fiscal_year,
  make_date(year(calendar_date), 1, 1) AS fiscal_year_start,
  last_day(calendar_date) AS month_end_date
FROM dates
"""
)

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE fact_gl_transactions AS
WITH dates AS (
  SELECT calendar_date
  FROM dim_calendar
  WHERE dayofmonth(calendar_date) IN (3, 9, 15, 21, 27)
),
combos AS (
  SELECT
    d.calendar_date,
    e.entity_id,
    p.product_id,
    s.segment_id,
    a.account_id,
    row_number() OVER (ORDER BY d.calendar_date, e.entity_id, p.product_id, s.segment_id, a.account_id) AS rn
  FROM dates d
  CROSS JOIN dim_entity e
  CROSS JOIN dim_product p
  CROSS JOIN dim_customer_segment s
  CROSS JOIN dim_account a
  WHERE a.statement_section = 'P&L'
)
SELECT
  concat('TXN-', rn) AS transaction_id,
  calendar_date AS posting_date,
  entity_id,
  product_id,
  segment_id,
  account_id,
  'ACTUAL' AS scenario_id,
  CASE
    WHEN account_id IN ('A4000','A4010') THEN round(12000 + pmod(rn * 37, 7000), 2)
    WHEN account_id = 'A5000' THEN round(3500 + pmod(rn * 23, 3000), 2)
    ELSE round(1800 + pmod(rn * 19, 2400), 2)
  END AS amount
FROM combos
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE fact_monthly_targets AS
WITH months AS (
  SELECT DISTINCT fiscal_month
  FROM dim_calendar
),
combos AS (
  SELECT
    m.fiscal_month,
    e.entity_id,
    p.product_id,
    s.segment_id,
    a.account_id,
    sc.scenario_id,
    row_number() OVER (ORDER BY m.fiscal_month, e.entity_id, p.product_id, s.segment_id, a.account_id, sc.scenario_id) AS rn
  FROM months m
  CROSS JOIN dim_entity e
  CROSS JOIN dim_product p
  CROSS JOIN dim_customer_segment s
  CROSS JOIN dim_account a
  CROSS JOIN dim_scenario sc
  WHERE a.statement_section = 'P&L'
    AND sc.scenario_id IN ('BUDGET','FORECAST')
)
SELECT
  concat('TGT-', rn) AS target_id,
  fiscal_month,
  entity_id,
  product_id,
  segment_id,
  account_id,
  scenario_id,
  CASE
    WHEN account_id IN ('A4000','A4010') THEN round(370000 + pmod(rn * 113, 90000), 2)
    WHEN account_id = 'A5000' THEN round(125000 + pmod(rn * 71, 45000), 2)
    ELSE round(70000 + pmod(rn * 53, 32000), 2)
  END AS target_amount
FROM combos
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE fact_month_end_balances AS
WITH months AS (
  SELECT DISTINCT month_end_date
  FROM dim_calendar
),
combos AS (
  SELECT
    m.month_end_date,
    e.entity_id,
    p.product_id,
    s.segment_id,
    a.account_id,
    row_number() OVER (ORDER BY m.month_end_date, e.entity_id, p.product_id, s.segment_id, a.account_id) AS rn
  FROM months m
  CROSS JOIN dim_entity e
  CROSS JOIN dim_product p
  CROSS JOIN dim_customer_segment s
  CROSS JOIN dim_account a
  WHERE a.statement_section = 'Balance Sheet'
)
SELECT
  concat('BAL-', rn) AS balance_id,
  month_end_date,
  entity_id,
  product_id,
  segment_id,
  account_id,
  'ACTUAL' AS scenario_id,
  CASE
    WHEN account_id = 'A1000' THEN round(1500000 + pmod(rn * 907, 350000), 2)
    WHEN account_id = 'A1100' THEN round(900000 + pmod(rn * 577, 225000), 2)
    ELSE round(500000 + pmod(rn * 431, 160000), 2)
  END AS balance_amount
FROM combos
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the Common Semantic Source
# MAGIC
# MAGIC The source table normalizes the three business grains into one queryable semantic base. This makes the level-of-detail problem visible while keeping the Metric View definition focused on business semantics.

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE VIEW finance_semantic_base AS
WITH gl AS (
  SELECT
    transaction_id AS source_record_id,
    'GL' AS source_grain,
    posting_date AS event_date,
    date_trunc('MONTH', posting_date) AS fiscal_month,
    entity_id,
    product_id,
    segment_id,
    account_id,
    scenario_id,
    amount,
    CAST(NULL AS DOUBLE) AS balance_amount
  FROM fact_gl_transactions
),
targets AS (
  SELECT
    target_id AS source_record_id,
    'TARGET' AS source_grain,
    fiscal_month AS event_date,
    fiscal_month,
    entity_id,
    product_id,
    segment_id,
    account_id,
    scenario_id,
    target_amount AS amount,
    CAST(NULL AS DOUBLE) AS balance_amount
  FROM fact_monthly_targets
),
balances AS (
  SELECT
    balance_id AS source_record_id,
    'BALANCE' AS source_grain,
    month_end_date AS event_date,
    date_trunc('MONTH', month_end_date) AS fiscal_month,
    entity_id,
    product_id,
    segment_id,
    account_id,
    scenario_id,
    CAST(NULL AS DOUBLE) AS amount,
    balance_amount
  FROM fact_month_end_balances
)
SELECT
  b.source_record_id,
  b.source_grain,
  b.event_date,
  b.fiscal_month,
  c.fiscal_quarter,
  c.fiscal_year,
  c.fiscal_year_start,
  b.entity_id,
  e.entity_name,
  e.country,
  e.region,
  b.product_id,
  p.product_name,
  p.product_family,
  p.business_unit,
  b.segment_id,
  s.segment_name,
  s.segment_group,
  b.account_id,
  a.account_name,
  a.account_category,
  a.statement_section,
  a.normal_balance,
  b.scenario_id,
  sc.scenario_name,
  b.amount,
  b.balance_amount
FROM (
  SELECT * FROM gl
  UNION ALL
  SELECT * FROM targets
  UNION ALL
  SELECT * FROM balances
) b
JOIN dim_calendar c
  ON b.event_date = c.calendar_date
JOIN dim_entity e
  ON b.entity_id = e.entity_id
JOIN dim_product p
  ON b.product_id = p.product_id
JOIN dim_customer_segment s
  ON b.segment_id = s.segment_id
JOIN dim_account a
  ON b.account_id = a.account_id
JOIN dim_scenario sc
  ON b.scenario_id = sc.scenario_id
"""
)

display(spark.sql("SELECT * FROM finance_semantic_base LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create the Metric View
# MAGIC
# MAGIC This Metric View uses YAML 1.1 and includes:
# MAGIC
# MAGIC - Fixed LOD fields
# MAGIC - Coarser LOD window measures
# MAGIC - Advanced window measures
# MAGIC - Composed measures using `MEASURE()`
# MAGIC - Agent metadata
# MAGIC - Optional materialization

# COMMAND ----------

materialization_yaml = """
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: semantic_base_snapshot
      type: unaggregated
    - name: exec_month_region_category
      type: aggregated
      dimensions:
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
        - fiscal_month
        - entity_name
        - account_category
      measures:
        - balance_additive_snapshot
""" if enable_materialization else ""

metric_view_yaml = f"""
version: 1.1
comment: |-
  Finance semantic model demonstrating LOD, window measures, agent metadata,
  and optional materialization for Databricks Metric Views.
source: {catalog}.{schema}.finance_semantic_base

fields:
  - name: event_date
    expr: event_date
    display_name: Event Date
    format:
      type: date
      date_format: year_month_day

  - name: fiscal_month
    expr: fiscal_month
    display_name: Fiscal Month
    format:
      type: date
      date_format: year_month_day
    synonyms:
      - month
      - accounting month

  - name: fiscal_year_start
    expr: fiscal_year_start
    display_name: Fiscal Year Start
    format:
      type: date
      date_format: year_month_day

  - name: fiscal_quarter
    expr: fiscal_quarter
    display_name: Fiscal Quarter
    synonyms:
      - quarter
      - accounting quarter

  - name: fiscal_year
    expr: fiscal_year
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

  - name: transaction_count
    expr: COUNT(DISTINCT source_record_id) FILTER (WHERE source_grain = 'GL')
    display_name: Transaction Count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      abbreviation: compact

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

{materialization_yaml}
"""

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.finance_metric_view
WITH METRICS
LANGUAGE YAML
AS $$
{metric_view_yaml}
$$
"""
)

print("Created Metric View:")
print(f"{catalog}.{schema}.finance_metric_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create a Derived Metric View
# MAGIC
# MAGIC Metric View composability works both within a single Metric View and across Metric Views. This derived view uses `finance_metric_view` as its source and defines executive metrics from the governed measures.

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.finance_exec_metric_view
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Derived executive Metric View that demonstrates composability across Metric Views.
source: {catalog}.{schema}.finance_metric_view
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

print("Created derived Metric View:")
print(f"{catalog}.{schema}.finance_exec_metric_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate the Metric View Definition

# COMMAND ----------

display(spark.sql("DESCRIBE EXTENDED finance_metric_view"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. LOD: Fixed Level of Detail
# MAGIC
# MAGIC Fixed LOD uses a predefined calculation grain. In this tutorial, `global_revenue_lod` and `account_category_revenue_lod` are fields defined with SQL window functions.

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  region,
  account_category,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(pct_of_global_revenue_fixed_lod) AS pct_of_global_revenue,
  MEASURE(pct_of_account_category_revenue_fixed_lod) AS pct_of_account_category_revenue
FROM finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY region, account_category
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fixed LOD Filtering Behavior
# MAGIC
# MAGIC Fixed LOD fields are computed before query-time filters. This query filters to APJ, but the global denominator still represents the fixed global calculation defined inside the Metric View field expression.

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  region,
  product_family,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(pct_of_global_revenue_fixed_lod) AS pct_of_global_revenue,
  MEASURE(pct_of_product_family_revenue_fixed_lod) AS pct_of_product_family_revenue
FROM finance_metric_view
WHERE fiscal_year = 2025
  AND region = 'APJ'
GROUP BY ALL
ORDER BY product_family
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. LOD: Coarser Level of Detail
# MAGIC
# MAGIC Coarser LOD uses window measures with `range: all` to calculate at a broader grain than the query. This lets the denominator remain aware of query-time filters.

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  region,
  entity_name,
  MEASURE(actual_revenue) AS entity_revenue,
  MEASURE(region_revenue_excluding_entity) AS region_revenue,
  MEASURE(pct_of_region_revenue) AS pct_of_region_revenue
FROM finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY region, entity_revenue DESC
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coarser LOD With Multiple Excluded Fields
# MAGIC
# MAGIC To exclude multiple fields from the calculation grain, add multiple `window` entries with `range: all`. The denominator below excludes both `entity_name` and `product_family` while preserving filters such as fiscal year and region.

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  region,
  entity_name,
  product_family,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(revenue_excluding_entity_and_product_family) AS broader_revenue,
  MEASURE(pct_of_entity_product_visible_total) AS pct_of_entity_product_visible_total
FROM finance_metric_view
WHERE fiscal_year = 2025
  AND region = 'APJ'
GROUP BY ALL
ORDER BY entity_name, product_family
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Window Semantics
# MAGIC
# MAGIC The next query shows current, cumulative, YTD, rolling-12, prior-year, leading, and YoY revenue in one semantic model.

# COMMAND ----------

display(
    spark.sql(
        """
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
FROM finance_metric_view
WHERE fiscal_month >= DATE'2025-01-01'
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inclusive vs Exclusive Trailing Windows
# MAGIC
# MAGIC The docs call out that `trailing` and `leading` windows can include or exclude the anchor row. This query compares `trailing 3 month exclusive` with `trailing 3 month inclusive`.

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  fiscal_month,
  region,
  MEASURE(current_month_revenue) AS current_month_revenue,
  MEASURE(trailing_3_month_revenue_exclusive) AS trailing_3_exclusive,
  MEASURE(trailing_3_month_revenue_inclusive) AS trailing_3_inclusive
FROM finance_metric_view
WHERE fiscal_month BETWEEN DATE'2025-01-01' AND DATE'2025-06-01'
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cross-Metric-View Composability

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  fiscal_month,
  region,
  MEASURE(revenue_per_transaction) AS revenue_per_transaction,
  MEASURE(executive_score) AS executive_score
FROM finance_exec_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Semiadditive Balances
# MAGIC
# MAGIC Balances should sum across business dimensions but not across months. `month_end_balance` uses `range: current` and `semiadditive: last`.

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  fiscal_quarter,
  entity_name,
  account_category,
  MEASURE(month_end_balance) AS month_end_balance
FROM finance_metric_view
WHERE statement_section = 'Balance Sheet'
  AND fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_quarter, entity_name, account_category
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Dashboard-Ready Executive Query

# COMMAND ----------

display(
    spark.sql(
        """
SELECT
  fiscal_month,
  region,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(budget_revenue) AS budget_revenue,
  MEASURE(revenue_variance_pct) AS revenue_variance_pct,
  MEASURE(ebitda) AS ebitda,
  MEASURE(ebitda_margin_pct) AS ebitda_margin_pct
FROM finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_month, region
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Materialization
# MAGIC
# MAGIC If `enable_materialization` is set to `true`, the Metric View includes a materialization block with one unaggregated materialization and multiple aggregated materializations.
# MAGIC
# MAGIC Materialization requires serverless compute and Databricks Runtime 17.3 or above. The feature is in Public Preview.

# COMMAND ----------

if enable_materialization:
    spark.sql("REFRESH MATERIALIZED VIEW finance_metric_view")
    display(spark.sql("DESCRIBE EXTENDED finance_metric_view"))
else:
    print("Materialization is disabled. Re-run the notebook with enable_materialization=true to create and refresh materializations.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Verify Materialization Usage
# MAGIC
# MAGIC Run `EXPLAIN EXTENDED` after materialization refresh completes. If query rewrite uses a materialization, the plan should include a materialization name such as `exec_month_region_category`.

# COMMAND ----------

explain_rows = spark.sql(
    """
EXPLAIN EXTENDED
SELECT
  fiscal_month,
  region,
  account_category,
  MEASURE(actual_revenue) AS actual_revenue
FROM finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
"""
).collect()

print("\n".join(row[0] for row in explain_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialization Match Scenarios
# MAGIC
# MAGIC The materialization documentation describes exact match, rollup match, and unaggregated fallback. The queries below are designed to be inspected with `EXPLAIN EXTENDED` after materialization refresh:
# MAGIC
# MAGIC - Exact match: same dimensions as `exec_month_region_category`.
# MAGIC - Rollup match: fewer dimensions than the materialization, using additive measures.
# MAGIC - Unaggregated fallback: a non-additive `COUNT(DISTINCT)` measure that should not roll up from an aggregated materialization.

# COMMAND ----------

for label, query in {
    "exact_match": """
      SELECT fiscal_month, region, account_category, MEASURE(actual_revenue) AS actual_revenue
      FROM finance_metric_view
      WHERE fiscal_year = 2025
      GROUP BY ALL
    """,
    "rollup_match": """
      SELECT fiscal_month, region, MEASURE(actual_revenue) AS actual_revenue
      FROM finance_metric_view
      WHERE fiscal_year = 2025
      GROUP BY ALL
    """,
    "unaggregated_or_source_fallback": """
      SELECT fiscal_month, region, MEASURE(transaction_count) AS transaction_count
      FROM finance_metric_view
      WHERE fiscal_year = 2025
      GROUP BY ALL
    """,
}.items():
    print(f"--- {label} ---")
    rows = spark.sql(f"EXPLAIN EXTENDED {query}").collect()
    print("\n".join(row[0] for row in rows[:1]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Genie-Style Questions
# MAGIC
# MAGIC The agent metadata in this Metric View should help natural language tools interpret business terms. Try prompts like:
# MAGIC
# MAGIC - Show YTD sales by region for 2025.
# MAGIC - Which entity has the highest operating margin?
# MAGIC - Show closing balance by account category.
# MAGIC - Compare current month revenue with the same month last year.
# MAGIC - What percent of region revenue does each entity contribute?

