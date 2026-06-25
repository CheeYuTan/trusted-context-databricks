# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate Synthetic Finance Data
# MAGIC
# MAGIC This notebook builds the tutorial dataset used by the Metric Views series.
# MAGIC
# MAGIC The goal is not to create random rows for the sake of a demo. The goal is to create a data model that is intentionally rich enough to teach **level of detail**:
# MAGIC
# MAGIC - P&L actuals are generated at **journal-line grain**.
# MAGIC - Budget and forecast are generated at **monthly planning grain**.
# MAGIC - Balance sheet metrics are generated at **month-end balance grain**.
# MAGIC - Shared dimensions create rollup paths for account, product, entity, segment, scenario, and fiscal calendar analysis.
# MAGIC
# MAGIC Later notebooks use this model to teach Metric View design, LOD expressions, window semantics, materialization, and dashboard consumption.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Use a catalog and schema where you have `CREATE TABLE`, `CREATE VIEW`, and `USE SCHEMA` privileges.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakemeter_catalog", "Catalog")
dbutils.widgets.text("schema", "metric_views_lod_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
qualified_schema = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"Using schema: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Scenario
# MAGIC
# MAGIC We are modeling a finance analytics layer for an executive dashboard.
# MAGIC
# MAGIC Users want to ask questions like:
# MAGIC
# MAGIC - What is revenue by region, entity, product family, and account category?
# MAGIC - What percentage of global revenue does APJ contribute?
# MAGIC - What percentage of regional revenue does each entity contribute?
# MAGIC - How does current-month revenue compare with YTD, rolling 12-month, and prior-year revenue?
# MAGIC - What is the correct quarter-end balance for cash, receivables, and deferred revenue?
# MAGIC
# MAGIC These questions operate at different calculation grains. That is why this dataset includes multiple fact grains instead of one flattened table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Model
# MAGIC
# MAGIC ```mermaid
# MAGIC erDiagram
# MAGIC   FACT_GL_TRANSACTIONS {
# MAGIC     string transaction_id
# MAGIC     date posting_date
# MAGIC     string entity_id
# MAGIC     string product_id
# MAGIC     string segment_id
# MAGIC     string account_id
# MAGIC     string scenario_id
# MAGIC     double amount
# MAGIC   }
# MAGIC
# MAGIC   FACT_MONTHLY_TARGETS {
# MAGIC     string target_id
# MAGIC     date fiscal_month
# MAGIC     string entity_id
# MAGIC     string product_id
# MAGIC     string segment_id
# MAGIC     string account_id
# MAGIC     string scenario_id
# MAGIC     double target_amount
# MAGIC   }
# MAGIC
# MAGIC   FACT_MONTH_END_BALANCES {
# MAGIC     string balance_id
# MAGIC     date month_end_date
# MAGIC     string entity_id
# MAGIC     string product_id
# MAGIC     string segment_id
# MAGIC     string account_id
# MAGIC     string scenario_id
# MAGIC     double balance_amount
# MAGIC   }
# MAGIC
# MAGIC   DIM_ACCOUNT {
# MAGIC     string account_id
# MAGIC     string account_name
# MAGIC     string account_category
# MAGIC     string statement_section
# MAGIC     string normal_balance
# MAGIC   }
# MAGIC
# MAGIC   DIM_PRODUCT {
# MAGIC     string product_id
# MAGIC     string product_name
# MAGIC     string product_family
# MAGIC     string business_unit
# MAGIC   }
# MAGIC
# MAGIC   DIM_ENTITY {
# MAGIC     string entity_id
# MAGIC     string entity_name
# MAGIC     string country
# MAGIC     string region
# MAGIC   }
# MAGIC
# MAGIC   DIM_CUSTOMER_SEGMENT {
# MAGIC     string segment_id
# MAGIC     string segment_name
# MAGIC     string segment_group
# MAGIC   }
# MAGIC
# MAGIC   DIM_CALENDAR {
# MAGIC     date calendar_date
# MAGIC     date fiscal_month
# MAGIC     string fiscal_quarter
# MAGIC     int fiscal_year
# MAGIC     date fiscal_year_start
# MAGIC     date month_end_date
# MAGIC   }
# MAGIC
# MAGIC   DIM_SCENARIO {
# MAGIC     string scenario_id
# MAGIC     string scenario_name
# MAGIC   }
# MAGIC
# MAGIC   FACT_GL_TRANSACTIONS }o--|| DIM_ACCOUNT : account_id
# MAGIC   FACT_GL_TRANSACTIONS }o--|| DIM_PRODUCT : product_id
# MAGIC   FACT_GL_TRANSACTIONS }o--|| DIM_ENTITY : entity_id
# MAGIC   FACT_GL_TRANSACTIONS }o--|| DIM_CUSTOMER_SEGMENT : segment_id
# MAGIC   FACT_GL_TRANSACTIONS }o--|| DIM_CALENDAR : posting_date
# MAGIC   FACT_GL_TRANSACTIONS }o--|| DIM_SCENARIO : scenario_id
# MAGIC
# MAGIC   FACT_MONTHLY_TARGETS }o--|| DIM_ACCOUNT : account_id
# MAGIC   FACT_MONTHLY_TARGETS }o--|| DIM_PRODUCT : product_id
# MAGIC   FACT_MONTHLY_TARGETS }o--|| DIM_ENTITY : entity_id
# MAGIC   FACT_MONTHLY_TARGETS }o--|| DIM_CUSTOMER_SEGMENT : segment_id
# MAGIC   FACT_MONTHLY_TARGETS }o--|| DIM_CALENDAR : fiscal_month
# MAGIC   FACT_MONTHLY_TARGETS }o--|| DIM_SCENARIO : scenario_id
# MAGIC
# MAGIC   FACT_MONTH_END_BALANCES }o--|| DIM_ACCOUNT : account_id
# MAGIC   FACT_MONTH_END_BALANCES }o--|| DIM_PRODUCT : product_id
# MAGIC   FACT_MONTH_END_BALANCES }o--|| DIM_ENTITY : entity_id
# MAGIC   FACT_MONTH_END_BALANCES }o--|| DIM_CUSTOMER_SEGMENT : segment_id
# MAGIC   FACT_MONTH_END_BALANCES }o--|| DIM_CALENDAR : month_end_date
# MAGIC   FACT_MONTH_END_BALANCES }o--|| DIM_SCENARIO : scenario_id
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grain Map
# MAGIC
# MAGIC This is the most important modeling concept in the tutorial.
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC   GL["fact_gl_transactions<br/>journal-line grain<br/>date + entity + product + segment + account"]
# MAGIC   TARGET["fact_monthly_targets<br/>monthly planning grain<br/>month + entity + product + segment + account + scenario"]
# MAGIC   BAL["fact_month_end_balances<br/>month-end balance grain<br/>month-end + entity + product + segment + balance account"]
# MAGIC
# MAGIC   BASE["finance_semantic_base<br/>normalized semantic source"]
# MAGIC   MV["finance_metric_view<br/>business metrics choose their own grain"]
# MAGIC
# MAGIC   GL --> BASE
# MAGIC   TARGET --> BASE
# MAGIC   BAL --> BASE
# MAGIC   BASE --> MV
# MAGIC
# MAGIC   MV --> LOD["LOD expressions<br/>fixed and coarser denominators"]
# MAGIC   MV --> WIN["Window measures<br/>current, YTD, rolling, prior year"]
# MAGIC   MV --> SEMI["Semiadditive measures<br/>last value across time"]
# MAGIC ```
# MAGIC
# MAGIC A normal SQL view often hides this complexity but does not solve it. A Metric View lets us encode the calculation grain into reusable measures.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimensions
# MAGIC
# MAGIC These dimensions are intentionally small so the generated data is easy to inspect.
# MAGIC
# MAGIC The hierarchy paths we will use later are:
# MAGIC
# MAGIC - Account: account -> account category -> statement section
# MAGIC - Product: product -> product family -> business unit
# MAGIC - Entity: entity -> country -> region
# MAGIC - Segment: segment -> segment group
# MAGIC - Calendar: date -> fiscal month -> fiscal quarter -> fiscal year

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

display(spark.sql("SELECT * FROM dim_account ORDER BY statement_section, account_category, account_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Journal-Line Actuals
# MAGIC
# MAGIC `fact_gl_transactions` represents actual P&L activity.
# MAGIC
# MAGIC Grain:
# MAGIC
# MAGIC ```text
# MAGIC posting_date + entity + product + customer segment + account + transaction_id
# MAGIC ```
# MAGIC
# MAGIC The table includes only P&L accounts. Revenue, COGS, and Opex are all stored as positive amounts so that the Metric View can define business calculations explicitly:
# MAGIC
# MAGIC - `gross_profit = revenue - cogs`
# MAGIC - `ebitda = revenue - cogs - opex`

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

display(spark.sql("SELECT * FROM fact_gl_transactions ORDER BY transaction_id LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Monthly Targets
# MAGIC
# MAGIC `fact_monthly_targets` represents budget and forecast values.
# MAGIC
# MAGIC Grain:
# MAGIC
# MAGIC ```text
# MAGIC fiscal_month + entity + product + customer segment + account + scenario
# MAGIC ```
# MAGIC
# MAGIC This grain is intentionally coarser than journal-line actuals. Later, the Metric View compares actual revenue with budget revenue without asking dashboard authors to manually handle the grain mismatch.

# COMMAND ----------

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

display(spark.sql("SELECT * FROM fact_monthly_targets ORDER BY target_id LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Month-End Balances
# MAGIC
# MAGIC `fact_month_end_balances` stores balances for cash, receivables, and deferred revenue.
# MAGIC
# MAGIC Grain:
# MAGIC
# MAGIC ```text
# MAGIC month_end_date + entity + product + customer segment + balance account
# MAGIC ```
# MAGIC
# MAGIC This is the table that makes semiadditive modeling important. You can sum balances across entities or products for the same month, but you should not sum January balance + February balance + March balance to get a quarter balance. A quarter balance should usually mean the last month-end balance in that quarter.

# COMMAND ----------

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

display(spark.sql("SELECT * FROM fact_month_end_balances ORDER BY balance_id LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the Semantic Base View
# MAGIC
# MAGIC The Metric View will use `finance_semantic_base` as its source.
# MAGIC
# MAGIC This view does three things:
# MAGIC
# MAGIC 1. Normalizes the three fact tables into common columns.
# MAGIC 2. Adds a `source_grain` field so measures can filter to the right fact type.
# MAGIC 3. Joins shared dimensions once so the Metric View can focus on business semantics.
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC   GL["GL actuals<br/>amount populated"]
# MAGIC   TGT["Targets<br/>amount populated from target_amount"]
# MAGIC   BAL["Balances<br/>balance_amount populated"]
# MAGIC   UNION["UNION ALL<br/>source_grain marks origin"]
# MAGIC   DIMS["Join dimensions<br/>calendar, entity, product, segment, account, scenario"]
# MAGIC   BASE["finance_semantic_base"]
# MAGIC
# MAGIC   GL --> UNION
# MAGIC   TGT --> UNION
# MAGIC   BAL --> UNION
# MAGIC   UNION --> DIMS
# MAGIC   DIMS --> BASE
# MAGIC ```

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
# MAGIC ## Validate Row Counts
# MAGIC
# MAGIC These counts tell us whether each grain was generated as expected:
# MAGIC
# MAGIC - `GL`: actual transaction rows
# MAGIC - `TARGET`: monthly budget and forecast rows
# MAGIC - `BALANCE`: month-end balance rows

# COMMAND ----------

display(
    spark.sql(
        """
SELECT source_grain, COUNT(*) AS row_count
FROM finance_semantic_base
GROUP BY source_grain
ORDER BY source_grain
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What This Notebook Created
# MAGIC
# MAGIC The next notebook starts from this source view:
# MAGIC
# MAGIC ```text
# MAGIC {catalog}.{schema}.finance_semantic_base
# MAGIC ```
# MAGIC
# MAGIC It will design `finance_metric_view` on top of this source, without materialization. Materialization is intentionally introduced later as a separate performance topic.

