# Databricks notebook source
# MAGIC %md
# MAGIC # Part 4 - Advanced Metric Semantics: LOD, Windows, Composability, and Agent Metadata
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

# Keep the notebook portable: change these widgets if you want to run the demo
# in a different Unity Catalog catalog or schema.
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
    # Databricks notebooks can render Mermaid through displayHTML. Keeping this
    # helper local makes each concept diagram easy to read next to the code.
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
    # The notebook is educational, but these small assertions still protect the
    # examples from silently producing the wrong totals.
    if actual is None or abs(float(actual) - expected) > tolerance:
        raise AssertionError(f"{label}: expected {expected}, got {actual}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mental Model
# MAGIC
# MAGIC Part 2 introduced the basic shape of a Metric View:
# MAGIC
# MAGIC - **Fields** describe how people slice, filter, and group the data.
# MAGIC - **Measures** describe how governed KPIs are calculated.
# MAGIC
# MAGIC Part 4 is about the questions that appear after the first Metric View is working.
# MAGIC
# MAGIC A business user might not ask for "LOD", "window semantics", or "agent metadata" directly. They ask questions like:
# MAGIC
# MAGIC - "What percentage of total annual loss came from Fraud Risk?"
# MAGIC - "Show me year-to-date loss, trailing three-month loss, and year-over-year growth."
# MAGIC - "Can I define `loss rate` once and reuse it everywhere?"
# MAGIC - "Will Genie understand that `risk loss`, `financial loss`, and `loss amount` mean the same KPI?"
# MAGIC
# MAGIC Those are semantic questions. The point of this notebook is to show how Metric Views capture those rules once, inside the warehouse serving layer, instead of leaving every dashboard, analyst, and agent to recreate them.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  Q["Business<br/>question"]
  MV["Metric View<br/>semantics"]
  A["Consistent answers<br/>SQL + BI + agents"]

  Q --> MV

  MV --> LOD["1. LOD<br/>Denominator"]
  MV --> WIN["2. Windows<br/>Time frame"]
  MV --> COMP["3. Composability<br/>Reuse measures"]
  MV --> META["4. Agent metadata<br/>Business language"]

  LOD --> A
  COMP --> A
  WIN --> A
  META --> A

  style Q fill:#F8FAFC,stroke:#64748B,color:#111827
  style MV fill:#E0F2FE,stroke:#0284C7,color:#111827
  style A fill:#F8FAFC,stroke:#64748B,color:#111827
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
# MAGIC The demo uses monthly Risk and Compliance data. It is synthetic and intentionally simple so the metric behavior is easy to see:
# MAGIC
# MAGIC - one row per month, risk area, and region
# MAGIC - exposure amount
# MAGIC - loss amount
# MAGIC - alert count
# MAGIC - month-end balance
# MAGIC
# MAGIC The important modeling detail is the **grain** of the source table.
# MAGIC
# MAGIC Each row answers: "for this month, this risk area, and this region, what happened?"
# MAGIC
# MAGIC That grain gives us enough structure to demonstrate:
# MAGIC
# MAGIC - percentage-of-total calculations across different denominators
# MAGIC - time windows across reporting months
# MAGIC - semiadditive month-end balances, where summing across months would be misleading

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE advanced_risk_monthly AS
WITH months AS (
  -- Two full years lets us demonstrate prior-year comparisons.
  SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-01'), interval 1 month)) AS month
),
risk_areas AS (
  -- Synthetic risk areas. Fraud Risk is included because it is easy to reason about
  -- in a banking risk and compliance story.
  SELECT * FROM VALUES
    ('Credit Risk', 1.00),
    ('Fraud Risk', 0.42),
    ('AML and KYC', 0.24),
    ('Operational Risk', 0.18)
  AS risk_areas(risk_area, risk_weight)
),
regions AS (
  -- Regional weights make the output realistic enough to compare shares.
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
    -- Deterministic row number gives repeatable synthetic values without randomness.
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
# MAGIC Before we query anything, read the Metric View definition like a contract.
# MAGIC
# MAGIC It says:
# MAGIC
# MAGIC - which columns are safe fields for grouping
# MAGIC - which calculations are governed measures
# MAGIC - which measures need a specific denominator
# MAGIC - which measures need a specific time frame
# MAGIC - which names, comments, formats, and synonyms help people and agents understand the result
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

# Fields are the business attributes people can group by or filter on.
# They are similar to dimensions in BI tools.
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

  # These two fields are not meant to be selected directly by most users.
  # They create fixed denominators for LOD-style calculations:
  # - annual global loss
  # - annual loss within each risk area
  #
  # Later measures use ANY_VALUE(...) because each visible group should see
  # one repeated denominator value at its chosen level of detail.
  - name: global_loss_year_lod
    expr: SUM(loss_amount) OVER (PARTITION BY reporting_year)
    comment: Fixed LOD field for total loss by reporting year.
  - name: risk_area_loss_year_lod
    expr: SUM(loss_amount) OVER (PARTITION BY reporting_year, risk_area)
    comment: Fixed LOD field for total loss by reporting year and risk area.

# Measures are governed KPI calculations.
# Users query them with MEASURE(...) so the engine knows these are semantic
# measures, not ordinary physical columns.
measures:
  # Base measures aggregate raw facts from the monthly risk table.
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

  # Composed measure: loss_rate reuses two governed measures instead of
  # duplicating SUM(loss_amount) / SUM(exposure_amount) in every dashboard.
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

  # Fixed LOD examples.
  # Numerator: the visible loss_amount group in the query.
  # Denominator: a fixed annual total from the LOD field.
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

  # Coarser LOD example.
  # The visible query will group by risk_area and region, but this denominator
  # intentionally ignores region so each region can be compared with its
  # risk-area total.
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

  # Window measures answer time-frame questions without every BI dashboard
  # rewriting its own YTD, trailing-period, or prior-year logic.
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

  # Composed window measures reuse current_month_loss and prior_year_loss.
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

  # Semiadditive example.
  # Balances should normally use the latest period in a range, not sum every
  # month together. The semiadditive rule captures that business meaning.
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
# MAGIC
# MAGIC LOD means **Level of Detail**. In plain language, it answers:
# MAGIC
# MAGIC > At what grain should this calculation be evaluated?
# MAGIC
# MAGIC A normal grouped query changes the calculation grain based on the fields in `GROUP BY`.
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - grouping by `risk_area` gives one result per risk area
# MAGIC - grouping by `risk_area, region` gives one result per risk area and region
# MAGIC - grouping by `month, risk_area, region` gives one result per month, risk area, and region
# MAGIC
# MAGIC That is usually what you want. But percentage metrics often need a denominator that stays fixed even when the visible result becomes more detailed.
# MAGIC
# MAGIC In this section:
# MAGIC
# MAGIC - `pct_of_global_loss_year_fixed_lod` means "this row's loss divided by total annual loss across all risk areas and regions"
# MAGIC - `pct_of_risk_area_loss_year_fixed_lod` means "this row's loss divided by total annual loss for this risk area"
# MAGIC
# MAGIC Same numerator, different denominator. That is why LOD matters.
# MAGIC
# MAGIC Here is the specific part of the Metric View definition that makes this work:
# MAGIC
# MAGIC ```yaml
# MAGIC fields:
# MAGIC   - name: global_loss_year_lod
# MAGIC     expr: SUM(loss_amount) OVER (PARTITION BY reporting_year)
# MAGIC
# MAGIC   - name: risk_area_loss_year_lod
# MAGIC     expr: SUM(loss_amount) OVER (PARTITION BY reporting_year, risk_area)
# MAGIC ```
# MAGIC
# MAGIC These are modeled as fields because they prepare reusable denominator values at a fixed grain:
# MAGIC
# MAGIC - `global_loss_year_lod` repeats the same annual loss total for every row in the same year
# MAGIC - `risk_area_loss_year_lod` repeats the same annual loss total for every row in the same year and risk area
# MAGIC
# MAGIC Then the actual percentage measures reference those fixed-denominator fields:
# MAGIC
# MAGIC ```yaml
# MAGIC measures:
# MAGIC   - name: pct_of_global_loss_year_fixed_lod
# MAGIC     expr: MEASURE(loss_amount) / NULLIF(ANY_VALUE(global_loss_year_lod), 0)
# MAGIC
# MAGIC   - name: pct_of_risk_area_loss_year_fixed_lod
# MAGIC     expr: MEASURE(loss_amount) / NULLIF(ANY_VALUE(risk_area_loss_year_lod), 0)
# MAGIC ```
# MAGIC
# MAGIC The `MEASURE(loss_amount)` part is the numerator at the visible query grain. The `ANY_VALUE(...)` part picks up the repeated fixed denominator for that group. It does not mean "choose a random business value"; it works here because the LOD field is intentionally repeated at the denominator grain.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  Row["Visible query row<br/>2025 + Fraud Risk + APJ"]

  subgraph Global["Calculation 1: share of annual global loss"]
    direction LR
    NumG["Numerator<br/>loss for visible row"]
    DenG["Denominator<br/>all 2025 losses"]
    CalcG["% of annual<br/>global loss"]
    NumG --> CalcG
    DenG --> CalcG
  end

  subgraph RiskArea["Calculation 2: share of annual Fraud Risk loss"]
    direction LR
    NumR["Same numerator<br/>loss for visible row"]
    DenR["Denominator<br/>all 2025 Fraud Risk losses"]
    CalcR["% of annual<br/>risk-area loss"]
    NumR --> CalcR
    DenR --> CalcR
  end

  Row --> NumG
  Row --> NumR

  style Row fill:#F8FAFC,stroke:#64748B,color:#111827
  style Global fill:#FFFFFF,stroke:#CBD5E1,color:#111827
  style RiskArea fill:#FFFFFF,stroke:#CBD5E1,color:#111827
  style NumG fill:#FEE2E2,stroke:#DC2626,color:#111827
  style NumR fill:#FEE2E2,stroke:#DC2626,color:#111827
  style DenG fill:#DBEAFE,stroke:#2563EB,color:#111827
  style DenR fill:#DCFCE7,stroke:#16A34A,color:#111827
  style CalcG fill:#EEF2FF,stroke:#4F46E5,color:#111827
  style CalcR fill:#EEF2FF,stroke:#4F46E5,color:#111827
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The visible grain is year + risk area + region.
# MAGIC -- Both percentage measures use the same numerator, loss_amount,
# MAGIC -- but they use different governed denominators from the Metric View.
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
-- The global LOD denominator should make all visible slices add up to 100%
-- for the selected year.
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
# MAGIC
# MAGIC Fixed LOD is useful when the denominator should stay anchored to a known grain, such as "annual global loss."
# MAGIC
# MAGIC Sometimes you want a denominator that is still aware of the user's current filter, but less detailed than the visible grouping.
# MAGIC
# MAGIC In this example, the query filters to `Fraud Risk` and groups by `region`.
# MAGIC
# MAGIC The business question is:
# MAGIC
# MAGIC > Within visible Fraud Risk results for 2025, how much does each region contribute?
# MAGIC
# MAGIC The numerator is each region's loss. The denominator is the Fraud Risk total across all visible regions. That is why the measure is named `risk_area_loss_excluding_region`: it keeps the risk-area context, but removes the region detail from the denominator.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The WHERE clause keeps the business context: 2025 Fraud Risk.
# MAGIC -- The GROUP BY includes region, so the result has one row per region.
# MAGIC -- The denominator measure intentionally ignores region so we can calculate
# MAGIC -- each region's share of the visible Fraud Risk total.
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
# MAGIC
# MAGIC Window semantics answer a different class of question:
# MAGIC
# MAGIC > Over what time frame should this measure be calculated?
# MAGIC
# MAGIC Without governed window semantics, every dashboard author has to decide how to write year-to-date, trailing-period, and prior-year logic. Small differences create inconsistent answers.
# MAGIC
# MAGIC In this Metric View:
# MAGIC
# MAGIC - `current_month_loss` looks only at the current reporting month
# MAGIC - `ytd_loss` accumulates loss from the start of the year through the current month
# MAGIC - `trailing_3_month_loss` looks at the current month and prior two months
# MAGIC - `prior_year_loss` shifts the current-month calculation back 12 months
# MAGIC - `yoy_loss_growth_pct` composes the prior measures into a year-over-year growth rate

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  subgraph Top[""]
    direction LR
    Current["current_month_loss<br/><b>Input:</b> May 2025<br/><b>Window:</b> current<br/><b>Meaning:</b> May only"]
    YTD["ytd_loss<br/><b>Input:</b> Jan-May 2025<br/><b>Window:</b> cumulative<br/><b>Meaning:</b> year to date"]
  end

  subgraph Bottom[""]
    direction LR
    Trail["trailing_3_month_loss<br/><b>Input:</b> Mar-May 2025<br/><b>Window:</b> trailing 3 months<br/><b>Meaning:</b> recent trend"]
    Prior["prior_year_loss<br/><b>Input:</b> May 2024<br/><b>Window:</b> offset -12 months<br/><b>Meaning:</b> same month last year"]
  end

  Current --> YTD
  Current --> Trail
  YTD --> Prior
  Trail --> Prior

  style Current fill:#DBEAFE,stroke:#2563EB,color:#111827
  style YTD fill:#DCFCE7,stroke:#16A34A,color:#111827
  style Trail fill:#FEF3C7,stroke:#D97706,color:#111827
  style Prior fill:#FCE7F3,stroke:#DB2777,color:#111827
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This result is intentionally scoped to one risk area and one region
# MAGIC -- so the time behavior is easy to inspect month by month.
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
# MAGIC
# MAGIC Some measures are additive. Loss amount is a good example: if January loss is 10 and February loss is 20, total loss across both months is 30.
# MAGIC
# MAGIC Balances are different.
# MAGIC
# MAGIC If a month-end balance is 1M in January and 1.1M in February, the two-month balance is not 2.1M. The useful answer is usually the latest balance in the period.
# MAGIC
# MAGIC That is what **semiadditive** means:
# MAGIC
# MAGIC - additive across some dimensions, such as region or risk area
# MAGIC - not simply additive across time
# MAGIC
# MAGIC In the Metric View, `month_end_balance` uses `semiadditive: last` so the measure returns the latest month-end balance for the grouped period.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The query groups by year and risk area, not by month.
# MAGIC -- Because month_end_balance is semiadditive, the Metric View uses the
# MAGIC -- latest month in each grouped period instead of summing every month.
# MAGIC SELECT
# MAGIC   reporting_year,
# MAGIC   risk_area,
# MAGIC   MEASURE(month_end_balance) AS latest_month_end_balance
# MAGIC FROM risk_advanced_metric_semantics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY reporting_year, risk_area

# COMMAND ----------

# MAGIC %md
# MAGIC ## Composability: Measures Building on Measures
# MAGIC
# MAGIC After LOD and window semantics, composability becomes easier to understand.
# MAGIC
# MAGIC At this point, the Metric View has several governed measures:
# MAGIC
# MAGIC - base measures such as `loss_amount` and `exposure_amount`
# MAGIC - LOD measures such as `pct_of_global_loss_year_fixed_lod`
# MAGIC - window measures such as `current_month_loss` and `prior_year_loss`
# MAGIC
# MAGIC Composability means a measure can build on those governed measures instead of reaching back to raw columns or repeating formulas in every dashboard.
# MAGIC
# MAGIC Without composability, every dashboard or SQL author might write their own version of `loss_rate`:
# MAGIC
# MAGIC ```sql
# MAGIC SUM(loss_amount) / SUM(exposure_amount)
# MAGIC ```
# MAGIC
# MAGIC That looks simple, but it becomes risky when the underlying measures become more complex. Maybe exposure needs exclusions. Maybe loss needs a filter. Maybe the denominator must handle zero exposure. If every tool owns a copy of the formula, definitions drift.
# MAGIC
# MAGIC In this Metric View, `loss_rate` is defined once:
# MAGIC
# MAGIC ```yaml
# MAGIC expr: MEASURE(loss_amount) / NULLIF(MEASURE(exposure_amount), 0)
# MAGIC ```
# MAGIC
# MAGIC The same pattern is used for year-over-year growth:
# MAGIC
# MAGIC ```yaml
# MAGIC expr: MEASURE(current_month_loss) - MEASURE(prior_year_loss)
# MAGIC ```
# MAGIC
# MAGIC The key point is that composed measures reuse governed measures. Once the base, LOD, and window semantics are trusted, higher-level measures can safely build on top of them.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  Base["Base measures<br/>loss_amount<br/>exposure_amount"]
  Time["Window measures<br/>current_month_loss<br/>prior_year_loss"]
  Derived["Composed measures<br/>loss_rate<br/>yoy_loss_growth"]
  Consumers["SQL, dashboards, Genie"]

  Base --> Derived
  Time --> Derived
  Derived --> Consumers

  style Base fill:#DBEAFE,stroke:#2563EB,color:#111827
  style Time fill:#DCFCE7,stroke:#16A34A,color:#111827
  style Derived fill:#FEF3C7,stroke:#D97706,color:#111827
  style Consumers fill:#F8FAFC,stroke:#64748B,color:#111827
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The query asks for composed measures.
# MAGIC -- loss_rate composes base measures.
# MAGIC -- yoy_loss_growth_pct composes window measures.
# MAGIC SELECT
# MAGIC   risk_area,
# MAGIC   region,
# MAGIC   MEASURE(loss_amount) AS loss_amount,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(loss_rate) AS loss_rate,
# MAGIC   MEASURE(yoy_loss_growth_pct) AS yoy_loss_growth_pct
# MAGIC FROM risk_advanced_metric_semantics
# MAGIC WHERE reporting_year = 2025
# MAGIC   AND risk_area = 'Fraud Risk'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY region

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Metadata
# MAGIC
# MAGIC The calculation is only part of the semantic layer. The language around the calculation matters too.
# MAGIC
# MAGIC Agent metadata helps BI tools and Genie Spaces interpret the Metric View in business terms:
# MAGIC
# MAGIC - `display_name` gives people a readable label such as **Loss Amount**
# MAGIC - `comment` explains what the field or measure means
# MAGIC - `synonyms` teach agents alternate phrases like "risk loss" or "financial loss"
# MAGIC - `format` tells tools how to display currency, percentages, numbers, and dates
# MAGIC
# MAGIC This matters because people rarely ask questions using exact YAML names. A risk analyst might say:
# MAGIC
# MAGIC > Show me financial loss and loss rate for Fraud Risk by region.
# MAGIC
# MAGIC Metadata gives the system more context to connect that natural language to governed fields and measures.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE EXTENDED lets you inspect the Metric View metadata stored in
# MAGIC -- Unity Catalog, including the semantic definition used by SQL and AI/BI.
# MAGIC DESCRIBE EXTENDED risk_advanced_metric_semantics

# COMMAND ----------

created = spark.sql(
    """
-- Final smoke check: confirm Unity Catalog registered the object as a Metric View.
SELECT COUNT(*) AS c
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_name = 'risk_advanced_metric_semantics'
  AND table_type = 'METRIC_VIEW'
"""
).collect()[0]["c"]

require_approx(created, 1, "Advanced semantics Metric View exists", tolerance=0)

print("Advanced Metric Semantics notebook completed successfully.")
