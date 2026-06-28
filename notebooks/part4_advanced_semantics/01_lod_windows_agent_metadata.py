# Databricks notebook source
# MAGIC %md
# MAGIC # Part 4 - Advanced Metric Semantics: LOD, Windows, and Agent Metadata
# MAGIC
# MAGIC This notebook supports the fourth article in the **Building Trusted Context in Databricks** series.
# MAGIC
# MAGIC Earlier parts covered:
# MAGIC
# MAGIC - Discover and Domains: where should I look?
# MAGIC - Metric Views as the Certified KPI Layer: which KPI definition should I trust?
# MAGIC - Joins in Metric Views: how should facts and dimensions relate?
# MAGIC
# MAGIC Part 4 focuses on advanced metric semantics:
# MAGIC
# MAGIC - Level of Detail (LOD): percent of what?
# MAGIC - Window semantics: over what time frame?
# MAGIC - Composability: how do measures build on each other?
# MAGIC - Agent metadata: what language should AI/BI understand?

# COMMAND ----------

dbutils.widgets.text("catalog", "steven_discover_domains", "Catalog")
dbutils.widgets.text("schema", "advanced_metric_semantics_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
qualified_schema = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

advanced_mv = f"{catalog}.{schema}.risk_advanced_metric_semantics"

print(f"Using schema: {catalog}.{schema}")
print(f"Metric View: {advanced_mv}")

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
# MAGIC ## Mental Model
# MAGIC
# MAGIC Part 2 introduced fields and measures. Part 4 adds richer semantics to those measures.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  MV["Metric View"]
  LOD["LOD<br/>percent of what?"]
  WIN["Windows<br/>over what time frame?"]
  COMP["Composability<br/>build from measures"]
  META["Agent metadata<br/>business language"]

  MV --> LOD
  MV --> WIN
  MV --> COMP
  MV --> META

  style MV fill:#E0F2FE,stroke:#0284C7,color:#111827
  style LOD fill:#EEF2FF,stroke:#4F46E5,color:#111827
  style WIN fill:#DCFCE7,stroke:#16A34A,color:#111827
  style COMP fill:#FEF3C7,stroke:#D97706,color:#111827
  style META fill:#FCE7F3,stroke:#DB2777,color:#111827
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Synthetic Monthly Risk Data
# MAGIC
# MAGIC The data is synthetic and intentionally simple:
# MAGIC
# MAGIC - one row per month, risk area, and region
# MAGIC - exposure amount
# MAGIC - loss amount
# MAGIC - alert count
# MAGIC - month-end balance

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE advanced_risk_monthly AS
WITH months AS (
  SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-01'), interval 1 month)) AS month
),
risk_areas AS (
  SELECT * FROM VALUES
    ('Credit Risk', 1.00),
    ('Fraud Risk', 0.42),
    ('AML and KYC', 0.24),
    ('Operational Risk', 0.18)
  AS risk_areas(risk_area, risk_weight)
),
regions AS (
  SELECT * FROM VALUES
    ('APJ', 1.00),
    ('AMER', 1.22),
    ('EMEA', 0.86)
  AS regions(region, region_weight)
),
base AS (
  SELECT
    m.month,
    year(m.month) AS reporting_year,
    r.risk_area,
    g.region,
    r.risk_weight,
    g.region_weight,
    row_number() OVER (ORDER BY m.month, r.risk_area, g.region) AS rn
  FROM months m
  CROSS JOIN risk_areas r
  CROSS JOIN regions g
)
SELECT
  month,
  reporting_year,
  risk_area,
  region,
  CAST((1000000 + rn * 13000) * risk_weight * region_weight AS DOUBLE) AS exposure_amount,
  CAST((12000 + pmod(rn * 173, 9000)) * risk_weight * region_weight AS DOUBLE) AS loss_amount,
  CAST(10 + pmod(rn * 7, 45) AS INT) AS alert_count,
  CAST((800000 + rn * 9000) * risk_weight * region_weight AS DOUBLE) AS month_end_balance
FROM base
"""
)

display(spark.sql("SELECT * FROM advanced_risk_monthly ORDER BY month, risk_area, region LIMIT 12"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Advanced Semantics Metric View
# MAGIC
# MAGIC This Metric View demonstrates:
# MAGIC
# MAGIC - fixed LOD fields using SQL window functions and `ANY_VALUE`
# MAGIC - coarser LOD using window measures with `range: all`
# MAGIC - current, cumulative, trailing, offset, and semiadditive windows
# MAGIC - composed measures using `MEASURE()`
# MAGIC - agent metadata such as `display_name`, `synonyms`, `comment`, and `format`

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.risk_advanced_metric_semantics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Advanced Risk and Compliance Metric View for LOD, windows, composability, and agent metadata.
source: {catalog}.{schema}.advanced_risk_monthly
fields:
  - name: month
    expr: month
    display_name: Reporting Month
    comment: Month used to order risk metrics.
    format:
      type: date
      date_format: year_month_day
      leading_zeros: true
    synonyms:
      - reporting month
      - risk month
      - period
  - name: reporting_year
    expr: reporting_year
    display_name: Reporting Year
    comment: Calendar year of the reporting month.
    synonyms:
      - year
      - fiscal year
  - name: risk_area
    expr: risk_area
    display_name: Risk Area
    comment: Risk and compliance business area.
    synonyms:
      - risk type
      - risk category
      - control area
  - name: region
    expr: region
    display_name: Region
    comment: Reporting region.
    synonyms:
      - geography
      - market
  - name: global_loss_year_lod
    expr: SUM(loss_amount) OVER (PARTITION BY reporting_year)
    comment: Fixed LOD field for total loss by reporting year.
  - name: risk_area_loss_year_lod
    expr: SUM(loss_amount) OVER (PARTITION BY reporting_year, risk_area)
    comment: Fixed LOD field for total loss by reporting year and risk area.
measures:
  - name: exposure_amount
    expr: SUM(exposure_amount)
    display_name: Exposure Amount
    comment: Total exposure amount for the selected grain.
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - exposure
      - risk exposure
      - outstanding exposure
  - name: loss_amount
    expr: SUM(loss_amount)
    display_name: Loss Amount
    comment: Total loss amount for the selected grain.
    format:
      type: currency
      currency_code: USD
      abbreviation: compact
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - loss
      - risk loss
      - financial loss
  - name: alert_count
    expr: SUM(alert_count)
    display_name: Alert Count
    comment: Count of risk alerts.
    format:
      type: number
      decimal_places:
        type: all
    synonyms:
      - alerts
      - cases
      - monitoring alerts
  - name: loss_rate
    expr: MEASURE(loss_amount) / NULLIF(MEASURE(exposure_amount), 0)
    display_name: Loss Rate
    comment: Loss amount divided by exposure amount.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - risk rate
      - loss percentage
  - name: pct_of_global_loss_year_fixed_lod
    expr: MEASURE(loss_amount) / NULLIF(ANY_VALUE(global_loss_year_lod), 0)
    display_name: Percent of Annual Global Loss
    comment: Share of annual global loss using a fixed LOD denominator.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: pct_of_risk_area_loss_year_fixed_lod
    expr: MEASURE(loss_amount) / NULLIF(ANY_VALUE(risk_area_loss_year_lod), 0)
    display_name: Percent of Annual Risk Area Loss
    comment: Share of annual loss within a risk area using a fixed LOD denominator.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: risk_area_loss_excluding_region
    expr: SUM(loss_amount)
    display_name: Risk Area Loss Excluding Region
    comment: Coarser LOD denominator that excludes region from the visible grouping.
    window:
      - order: region
        range: all
        semiadditive: last
  - name: pct_of_visible_risk_area_loss
    expr: MEASURE(loss_amount) / NULLIF(MEASURE(risk_area_loss_excluding_region), 0)
    display_name: Percent of Visible Risk Area Loss
    comment: Region share within visible risk area context.
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: current_month_loss
    expr: SUM(loss_amount)
    display_name: Current Month Loss
    window:
      - order: month
        range: current
        semiadditive: last
  - name: running_loss
    expr: SUM(loss_amount)
    display_name: Running Loss
    window:
      - order: month
        range: cumulative
        semiadditive: last
  - name: ytd_loss
    expr: SUM(loss_amount)
    display_name: Year-to-Date Loss
    window:
      - order: month
        range: cumulative
        semiadditive: last
      - order: reporting_year
        range: current
        semiadditive: last
  - name: trailing_3_month_loss
    expr: SUM(loss_amount)
    display_name: Trailing 3 Month Loss
    window:
      - order: month
        range: trailing 3 month inclusive
        semiadditive: last
  - name: prior_year_loss
    expr: SUM(loss_amount)
    display_name: Prior Year Loss
    window:
      - order: month
        range: current
        semiadditive: last
        offset: -12 month
  - name: yoy_loss_growth
    expr: MEASURE(current_month_loss) - MEASURE(prior_year_loss)
    display_name: YoY Loss Growth
  - name: yoy_loss_growth_pct
    expr: MEASURE(yoy_loss_growth) / NULLIF(MEASURE(prior_year_loss), 0)
    display_name: YoY Loss Growth Percent
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
  - name: month_end_balance
    expr: SUM(month_end_balance)
    display_name: Month End Balance
    comment: Semiadditive balance that should use the latest month when month is not grouped.
    window:
      - order: month
        range: current
        semiadditive: last
$$
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixed LOD: Percent of What?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reporting_year,
# MAGIC   risk_area,
# MAGIC   region,
# MAGIC   MEASURE(loss_amount) AS loss_amount,
# MAGIC   MEASURE(pct_of_global_loss_year_fixed_lod) AS pct_of_global_loss,
# MAGIC   MEASURE(pct_of_risk_area_loss_year_fixed_lod) AS pct_of_risk_area_loss
# MAGIC FROM risk_advanced_metric_semantics
# MAGIC WHERE reporting_year = 2025
# MAGIC GROUP BY ALL
# MAGIC ORDER BY risk_area, region

# COMMAND ----------

global_pct_total = spark.sql(
    """
SELECT SUM(pct_of_global_loss) AS pct_total
FROM (
  SELECT
    risk_area,
    region,
    MEASURE(pct_of_global_loss_year_fixed_lod) AS pct_of_global_loss
  FROM risk_advanced_metric_semantics
  WHERE reporting_year = 2025
  GROUP BY ALL
)
"""
).collect()[0]["pct_total"]

print(f"2025 global loss percentage total: {global_pct_total}")
require_approx(global_pct_total, 1.0, "Fixed LOD global loss percentages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coarser LOD: Filter-Aware Share

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_area,
# MAGIC   region,
# MAGIC   MEASURE(loss_amount) AS region_loss,
# MAGIC   MEASURE(risk_area_loss_excluding_region) AS risk_area_loss,
# MAGIC   MEASURE(pct_of_visible_risk_area_loss) AS pct_of_visible_risk_area_loss
# MAGIC FROM risk_advanced_metric_semantics
# MAGIC WHERE reporting_year = 2025
# MAGIC   AND risk_area = 'Fraud Risk'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY region

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Semantics: YTD, Trailing, and Prior Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   month,
# MAGIC   risk_area,
# MAGIC   MEASURE(current_month_loss) AS current_month_loss,
# MAGIC   MEASURE(ytd_loss) AS ytd_loss,
# MAGIC   MEASURE(trailing_3_month_loss) AS trailing_3_month_loss,
# MAGIC   MEASURE(prior_year_loss) AS prior_year_loss,
# MAGIC   MEASURE(yoy_loss_growth_pct) AS yoy_loss_growth_pct
# MAGIC FROM risk_advanced_metric_semantics
# MAGIC WHERE risk_area = 'Credit Risk'
# MAGIC   AND region = 'APJ'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semiadditive Measure: Month-End Balance

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reporting_year,
# MAGIC   risk_area,
# MAGIC   MEASURE(month_end_balance) AS latest_month_end_balance
# MAGIC FROM risk_advanced_metric_semantics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY reporting_year, risk_area

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Metadata
# MAGIC
# MAGIC The Metric View definition includes display names, synonyms, comments, and formats.
# MAGIC
# MAGIC These help BI tools and Genie Spaces interpret the metric view in business language.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED risk_advanced_metric_semantics

# COMMAND ----------

created = spark.sql(
    """
SELECT COUNT(*) AS c
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_name = 'risk_advanced_metric_semantics'
  AND table_type = 'METRIC_VIEW'
"""
).collect()[0]["c"]

require_approx(created, 1, "Advanced semantics Metric View exists", tolerance=0)

print("Advanced Metric Semantics notebook completed successfully.")
