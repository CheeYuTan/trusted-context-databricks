# Databricks notebook source
# MAGIC %md
# MAGIC # Deep Dive 01 Setup - Large Base Tables for Metric View Materialization
# MAGIC
# MAGIC This notebook creates the base data model for the materialization deep dive.
# MAGIC
# MAGIC The materialization article should not start from a tiny sample table. To see why materialization matters, we need:
# MAGIC
# MAGIC - A fact table with enough rows to make repeated grouping meaningful.
# MAGIC - Several dimensions so the Metric View can define joins and derived fields.
# MAGIC - Both additive and non-additive measures so query rewrite behavior is easy to demonstrate.
# MAGIC
# MAGIC The next notebook, `01_materialization_deep_dive`, uses Metric View `joins` to define the relationships between the fact and dimension tables.

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
# MAGIC ## Data Model
# MAGIC
# MAGIC We will use a simple star schema: one daily finance fact table surrounded by calendar, entity, product, segment, and account dimensions.
# MAGIC
# MAGIC This gives us a realistic source model for the materialization deep dive. In the next notebook, the Metric View will define how these tables relate and then materialize the prepared model for faster queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Purpose
# MAGIC
# MAGIC Each dimension exists to make a different materialization scenario realistic:
# MAGIC
# MAGIC - `mat_dim_calendar`: provides fiscal year, fiscal month, and fiscal quarter fields. These are common grouping and filter columns, so they are good candidates for aggregated materialization dimensions.
# MAGIC - `mat_dim_entity`: provides region and country rollups. This lets us demonstrate exact and rollup matching from a detailed aggregate to a coarser regional query.
# MAGIC - `mat_dim_product`: provides product family and business unit. This creates a dashboard-style drilldown grain.
# MAGIC - `mat_dim_segment`: provides customer segment context and a derived `sales_motion` field in the Metric View.
# MAGIC - `mat_dim_account`: classifies rows as Revenue, COGS, or Opex. Measures use this dimension for filtered aggregations.

# COMMAND ----------

render_mermaid(
    """
erDiagram
  MAT_FACT_FINANCE_DAILY {
    string transaction_id
    date transaction_date
    string entity_id
    string product_id
    string segment_id
    string account_id
    string customer_id
    double amount
  }

  MAT_DIM_CALENDAR {
    date calendar_date
    date fiscal_month
    int fiscal_year
    string fiscal_quarter
  }

  MAT_DIM_ENTITY {
    string entity_id
    string entity_name
    string country
    string region
  }

  MAT_DIM_PRODUCT {
    string product_id
    string product_name
    string product_family
    string business_unit
  }

  MAT_DIM_SEGMENT {
    string segment_id
    string segment_name
    string segment_group
  }

  MAT_DIM_ACCOUNT {
    string account_id
    string account_name
    string account_category
    string statement_section
  }

  MAT_FACT_FINANCE_DAILY }o--|| MAT_DIM_CALENDAR : transaction_date
  MAT_FACT_FINANCE_DAILY }o--|| MAT_DIM_ENTITY : entity_id
  MAT_FACT_FINANCE_DAILY }o--|| MAT_DIM_PRODUCT : product_id
  MAT_FACT_FINANCE_DAILY }o--|| MAT_DIM_SEGMENT : segment_id
  MAT_FACT_FINANCE_DAILY }o--|| MAT_DIM_ACCOUNT : account_id
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grain and Scale
# MAGIC
# MAGIC The fact table grain is:
# MAGIC
# MAGIC ```text
# MAGIC transaction_date + entity + product + customer segment + account + generated transaction id
# MAGIC ```
# MAGIC
# MAGIC With the default dimensions, the fact table has just under **1 million rows**:
# MAGIC
# MAGIC ```text
# MAGIC 731 days x 6 entities x 15 products x 3 segments x 5 accounts = 986,850 rows
# MAGIC ```
# MAGIC
# MAGIC This is intentionally larger than a toy dataset, but still small enough that the tutorial runs quickly in a demo workspace.

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE mat_dim_calendar AS
WITH dates AS (
  SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) AS calendar_date
)
SELECT
  calendar_date,
  date_trunc('MONTH', calendar_date) AS fiscal_month,
  year(calendar_date) AS fiscal_year,
  concat(year(calendar_date), '-Q', quarter(calendar_date)) AS fiscal_quarter,
  dayofweek(calendar_date) AS day_of_week,
  month(calendar_date) AS month_number
FROM dates
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE mat_dim_entity AS
SELECT * FROM VALUES
  ('E01','Singapore Commercial','Singapore','APJ'),
  ('E02','Australia Commercial','Australia','APJ'),
  ('E03','US East','United States','AMER'),
  ('E04','US West','United States','AMER'),
  ('E05','UK','United Kingdom','EMEA'),
  ('E06','Germany','Germany','EMEA')
AS mat_dim_entity(entity_id, entity_name, country, region)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE mat_dim_product AS
WITH products AS (
  SELECT id
  FROM range(1, 16)
)
SELECT
  concat('P', lpad(id, 2, '0')) AS product_id,
  concat('Product ', id) AS product_name,
  CASE
    WHEN id <= 5 THEN 'Digital Platforms'
    WHEN id <= 10 THEN 'Analytics'
    ELSE 'Services'
  END AS product_family,
  CASE
    WHEN id <= 10 THEN 'Software'
    ELSE 'Services'
  END AS business_unit
FROM products
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE mat_dim_segment AS
SELECT * FROM VALUES
  ('S01','Strategic Enterprise','Enterprise'),
  ('S02','Large Enterprise','Enterprise'),
  ('S03','Mid-Market','Commercial')
AS mat_dim_segment(segment_id, segment_name, segment_group)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE mat_dim_account AS
SELECT * FROM VALUES
  ('A4000','Product Revenue','Revenue','P&L'),
  ('A4010','Service Revenue','Revenue','P&L'),
  ('A5000','Cost of Goods Sold','COGS','P&L'),
  ('A6100','Sales Expense','Opex','P&L'),
  ('A6200','Technology Expense','Opex','P&L')
AS mat_dim_account(account_id, account_name, account_category, statement_section)
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Large Fact Table
# MAGIC
# MAGIC The generated amounts are deterministic. That makes repeated runs reproducible and avoids depending on random seeds.

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE mat_fact_finance_daily AS
WITH combos AS (
  SELECT
    c.calendar_date,
    e.entity_id,
    p.product_id,
    s.segment_id,
    a.account_id,
    row_number() OVER (
      ORDER BY c.calendar_date, e.entity_id, p.product_id, s.segment_id, a.account_id
    ) AS rn
  FROM mat_dim_calendar c
  CROSS JOIN mat_dim_entity e
  CROSS JOIN mat_dim_product p
  CROSS JOIN mat_dim_segment s
  CROSS JOIN mat_dim_account a
)
SELECT
  concat('MAT-TXN-', rn) AS transaction_id,
  calendar_date AS transaction_date,
  entity_id,
  product_id,
  segment_id,
  account_id,
  concat('CUST-', lpad(pmod(rn, 100000), 6, '0')) AS customer_id,
  CASE
    WHEN account_id IN ('A4000','A4010') THEN cast(80 + pmod(rn * 17, 220) + pmod(rn, 13) * 0.1 AS DOUBLE)
    WHEN account_id = 'A5000' THEN cast(25 + pmod(rn * 11, 90) + pmod(rn, 7) * 0.1 AS DOUBLE)
    ELSE cast(12 + pmod(rn * 5, 55) + pmod(rn, 5) * 0.1 AS DOUBLE)
  END AS amount
FROM combos
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate the Base Tables

# COMMAND ----------

display(
    spark.sql(
        """
SELECT 'mat_fact_finance_daily' AS object_name, COUNT(*) AS row_count FROM mat_fact_finance_daily
"""
    )
)

display(
    spark.sql(
        """
SELECT
  fiscal_year,
  region,
  account_category,
  COUNT(*) AS rows,
  SUM(f.amount) AS amount
FROM mat_fact_finance_daily f
JOIN mat_dim_calendar c
  ON f.transaction_date = c.calendar_date
JOIN mat_dim_entity e
  ON f.entity_id = e.entity_id
JOIN mat_dim_account a
  ON f.account_id = a.account_id
GROUP BY ALL
ORDER BY fiscal_year, region, account_category
"""
    )
)

