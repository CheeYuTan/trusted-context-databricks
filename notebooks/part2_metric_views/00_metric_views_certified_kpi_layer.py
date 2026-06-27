# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 - Metric Views as the Certified KPI Layer
# MAGIC
# MAGIC This notebook supports the second article in the **Building Trusted Enterprise Context in Databricks** series.
# MAGIC
# MAGIC Part 1 showed how Discover and Domains help people, BI tools, and agents answer:
# MAGIC
# MAGIC > Where should I look?
# MAGIC
# MAGIC Part 2 answers the next question:
# MAGIC
# MAGIC > Which KPI definition should I trust?
# MAGIC
# MAGIC This notebook intentionally focuses on Metric View basics. Part 3 will go deeper into level of detail, window semantics, and agent metadata.

# COMMAND ----------

dbutils.widgets.text("catalog", "steven_discover_domains", "Catalog")
dbutils.widgets.text("schema", "risk_compliance_context_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
qualified_schema = f"`{catalog}`.`{schema}`"

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

credit_mv = f"{catalog}.{schema}.credit_risk_metrics"
fraud_mv = f"{catalog}.{schema}.fraud_risk_metrics"
exec_mv = f"{catalog}.{schema}.risk_compliance_executive_metrics"

print(f"Using schema: {catalog}.{schema}")
print(f"Credit Risk Metric View: {credit_mv}")
print(f"Fraud Risk Metric View: {fraud_mv}")
print(f"Executive Metric View: {exec_mv}")

# COMMAND ----------

def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def scalar(query: str, column: str = "value"):
    return spark.sql(query).collect()[0][column]


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
# MAGIC ## 1. From Domain to KPI
# MAGIC
# MAGIC Discover and Domains organize context.
# MAGIC
# MAGIC Metric Views define the trusted calculations inside that context.
# MAGIC
# MAGIC In this demo:
# MAGIC
# MAGIC ```text
# MAGIC Risk and Compliance -> Credit Risk -> credit_risk_metrics
# MAGIC ```
# MAGIC
# MAGIC The source table gives us data. The Metric View gives us a governed KPI contract.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  D["Domain<br/>Risk and Compliance"]
  S["Subdomain<br/>Credit Risk"]
  MV["Certified Metric View<br/>credit_risk_metrics"]
  KPI["Governed KPIs<br/>exposure_amount<br/>expected_credit_loss<br/>ecl_rate"]

  D --> S --> MV --> KPI

  style D fill:#EEF2FF,stroke:#4F46E5,color:#111827
  style S fill:#FEF3C7,stroke:#D97706,color:#111827
  style MV fill:#DCFCE7,stroke:#16A34A,color:#111827
  style KPI fill:#E0F2FE,stroke:#0284C7,color:#111827
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   table_type,
# MAGIC   comment
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_catalog = '${catalog}'
# MAGIC   AND table_schema = '${schema}'
# MAGIC   AND table_name IN (
# MAGIC     'credit_risk_exposures',
# MAGIC     'credit_risk_portfolio_summary',
# MAGIC     'credit_risk_metrics',
# MAGIC     'fraud_risk_metrics',
# MAGIC     'risk_compliance_executive_metrics'
# MAGIC   )
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What Happens Without Metric Views?
# MAGIC
# MAGIC Without a governed metric layer, teams often create physical views for every slice and dashboard need.
# MAGIC
# MAGIC Another common pattern is to define the semantic layer inside a BI tool. That can help one dashboarding workflow, but it creates a different problem: the business logic is now trapped inside a proprietary consumption layer.
# MAGIC
# MAGIC Agents, SQL users, notebooks, other BI tools, and governance workflows may not be able to reuse it directly.
# MAGIC
# MAGIC For trusted context, the semantic layer should be governed where the data is governed: in Unity Catalog.
# MAGIC
# MAGIC For credit risk, that might become:
# MAGIC
# MAGIC ```text
# MAGIC credit_exposure_by_product
# MAGIC credit_exposure_by_region
# MAGIC credit_exposure_by_risk_band
# MAGIC credit_exposure_by_month
# MAGIC credit_exposure_by_product_region
# MAGIC credit_exposure_by_product_risk_band
# MAGIC credit_exposure_by_region_risk_band
# MAGIC ```
# MAGIC
# MAGIC Each view hardcodes selected dimensions, aggregation grain, filters, and KPI formulas. Changing the KPI definition means finding every duplicated implementation.
# MAGIC
# MAGIC The issue is not that SQL cannot calculate the metric. The issue is that every query becomes another place where the metric can drift.
# MAGIC
# MAGIC And if the only trusted definition lives inside one BI tool, every other interface has to reconstruct or copy that definition.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  subgraph Without["Without Metric Views"]
    V1["credit_exposure_by_product"]
    V2["credit_exposure_by_region"]
    V3["credit_exposure_by_risk_band"]
    V4["credit_exposure_by_product_region"]
    V5["credit_exposure_by_product_risk_band"]
  end

  Problem["Duplicated KPI logic<br/>Different grains<br/>Harder lifecycle"]

  V1 --> Problem
  V2 --> Problem
  V3 --> Problem
  V4 --> Problem
  V5 --> Problem

  subgraph With["With one Metric View"]
    MV["credit_risk_metrics<br/>one governed contract"]
    Contract["Fields + Measures<br/>separate grouping from KPI logic"]
    Q["Many query groupings<br/>without new physical views"]
  end

  MV --> Contract --> Q

  style Problem fill:#FEE2E2,stroke:#DC2626,color:#111827
  style MV fill:#DCFCE7,stroke:#16A34A,color:#111827
  style Contract fill:#E0F2FE,stroke:#0284C7,color:#111827
  style Q fill:#EEF2FF,stroke:#4F46E5,color:#111827
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_sprawl_examples AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('credit_exposure_by_product', 'product_line', 'Expected credit loss by product'),
# MAGIC   ('credit_exposure_by_region', 'region', 'Expected credit loss by region'),
# MAGIC   ('credit_exposure_by_risk_band', 'risk_band', 'Expected credit loss by risk band'),
# MAGIC   ('credit_exposure_by_product_region', 'product_line + region', 'Expected credit loss by product and region'),
# MAGIC   ('credit_exposure_by_product_risk_band', 'product_line + risk_band', 'Expected credit loss by product and risk band')
# MAGIC AS view_sprawl_examples(view_name, hardcoded_grain, dashboard_question)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM view_sprawl_examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Metric View Basics: Fields and Measures
# MAGIC
# MAGIC A Metric View separates **how users slice the data** from **what the KPI means**.
# MAGIC
# MAGIC In Metric View YAML:
# MAGIC
# MAGIC - **Fields** are the dimensions users group, filter, and slice by.
# MAGIC - **Measures** are the governed metric definitions users query with `MEASURE()`.
# MAGIC
# MAGIC | Concept | Purpose | Example | Query behavior |
# MAGIC |---|---|---|---|
# MAGIC | Field / dimension | Defines grouping, filtering, and analytical grain. | `product_line`, `risk_band`, `region` | Appears directly in `SELECT`, `WHERE`, and `GROUP BY`. |
# MAGIC | Measure | Defines a governed aggregate or dependent KPI. | `expected_credit_loss`, `ecl_rate` | Queried through `MEASURE(...)`. |
# MAGIC
# MAGIC For `credit_risk_metrics`, the important fields are:
# MAGIC
# MAGIC ```text
# MAGIC reporting_date
# MAGIC product_line
# MAGIC risk_band
# MAGIC region
# MAGIC ```
# MAGIC
# MAGIC The important measures are:
# MAGIC
# MAGIC ```text
# MAGIC exposure_amount
# MAGIC expected_credit_loss
# MAGIC ecl_rate
# MAGIC ```
# MAGIC
# MAGIC Once fields and measures are defined, the same Metric View can serve many groupings at query time.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  Source["Source table<br/><b>credit_risk_exposures</b>"]
  Summary["Prepared view<br/><b>credit_risk_portfolio_summary</b>"]
  MV["Metric View<br/><b>credit_risk_metrics</b>"]
  Fields["Fields: how to slice<br/>reporting_date<br/>product_line<br/>risk_band<br/>region"]
  Measures["Measures: what KPI means<br/>exposure_amount<br/>expected_credit_loss<br/>ecl_rate"]

  Source --> Summary --> MV
  MV --> Fields
  MV --> Measures

  style Source fill:#F3F4F6,stroke:#6B7280,color:#111827
  style Summary fill:#FEF3C7,stroke:#D97706,color:#111827
  style MV fill:#E0F2FE,stroke:#0284C7,color:#111827
  style Fields fill:#EEF2FF,stroke:#4F46E5,color:#111827
  style Measures fill:#DCFCE7,stroke:#16A34A,color:#111827
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. How the Metric View Is Defined
# MAGIC
# MAGIC A Metric View is a Unity Catalog view with a `WITH METRICS` definition.
# MAGIC
# MAGIC For this tutorial, the setup notebook creates `credit_risk_metrics` from the prepared summary view `credit_risk_portfolio_summary`.
# MAGIC
# MAGIC The lineage is:
# MAGIC
# MAGIC ```text
# MAGIC credit_risk_exposures -> credit_risk_portfolio_summary -> credit_risk_metrics
# MAGIC ```
# MAGIC
# MAGIC The definition has four basic pieces:
# MAGIC
# MAGIC - `source`: the table or view the Metric View is built on.
# MAGIC - `fields`: the columns users can group, filter, and slice by.
# MAGIC - `measures`: the governed KPI calculations.
# MAGIC - metadata such as `comment`, `display_name`, and `synonyms`.
# MAGIC
# MAGIC A simplified version looks like this:
# MAGIC
# MAGIC ```yaml
# MAGIC version: 1.1
# MAGIC comment: Certified credit risk metrics for the Risk and Compliance Discover demo.
# MAGIC source: credit_risk_portfolio_summary
# MAGIC fields:
# MAGIC   - name: reporting_date
# MAGIC     expr: reporting_date
# MAGIC   - name: product_line
# MAGIC     expr: product_line
# MAGIC   - name: risk_band
# MAGIC     expr: risk_band
# MAGIC   - name: region
# MAGIC     expr: region
# MAGIC measures:
# MAGIC   - name: exposure_amount
# MAGIC     expr: SUM(exposure_amount)
# MAGIC     display_name: Exposure Amount
# MAGIC     synonyms: [exposure, outstanding balance, credit exposure]
# MAGIC   - name: expected_credit_loss
# MAGIC     expr: SUM(expected_credit_loss)
# MAGIC     display_name: Expected Credit Loss
# MAGIC     synonyms: [ECL, provisions, credit loss]
# MAGIC   - name: ecl_rate
# MAGIC     expr: MEASURE(expected_credit_loss) / NULLIF(MEASURE(exposure_amount), 0)
# MAGIC     display_name: ECL Rate
# MAGIC     synonyms: [loss rate, provision rate]
# MAGIC ```
# MAGIC
# MAGIC Notice two things:
# MAGIC
# MAGIC 1. `ecl_rate` is defined once using other measures.
# MAGIC 2. The query later decides whether to group by product, region, risk band, or another field.
# MAGIC
# MAGIC This post stays at this basic layer. Joins are important enough to deserve their own post. The later deep dives will cover relationship modeling, level of detail, window measures, and richer agent metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED credit_risk_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Manual SQL vs Metric View SQL
# MAGIC
# MAGIC The manual SQL and Metric View SQL can produce the same output.
# MAGIC
# MAGIC The difference is ownership.
# MAGIC
# MAGIC With manual SQL, the dashboard owns the formula. With a Metric View, Unity Catalog owns the formula.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_line,
# MAGIC   risk_band,
# MAGIC   SUM(exposure_amount) AS exposure_amount,
# MAGIC   SUM(expected_credit_loss) AS expected_credit_loss,
# MAGIC   SUM(expected_credit_loss) / NULLIF(SUM(exposure_amount), 0) AS ecl_rate
# MAGIC FROM credit_risk_exposures
# MAGIC GROUP BY ALL
# MAGIC ORDER BY product_line, risk_band

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_line,
# MAGIC   risk_band,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss,
# MAGIC   MEASURE(ecl_rate) AS ecl_rate
# MAGIC FROM credit_risk_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY product_line, risk_band

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Slice and Dice With One Metric View
# MAGIC
# MAGIC The core benefit is not shorter SQL. The core benefit is reusable grouping.
# MAGIC
# MAGIC The same `credit_risk_metrics` Metric View can answer different dashboard and agent questions without creating a new view for every combination of dimensions.

# COMMAND ----------

render_mermaid(
    """
flowchart TB
  MV["credit_risk_metrics<br/>one governed KPI contract"]

  Q1["Group by<br/>product_line"]
  Q2["Group by<br/>region"]
  Q3["Group by<br/>risk_band"]
  Q4["Group by<br/>product_line + risk_band"]

  MV --> Q1
  MV --> Q2
  MV --> Q3
  MV --> Q4

  Q1 --> R1["Same measure definition<br/>expected_credit_loss, ecl_rate"]
  Q2 --> R1
  Q3 --> R1
  Q4 --> R1

  style MV fill:#DCFCE7,stroke:#16A34A,color:#111827
  style R1 fill:#E0F2FE,stroke:#0284C7,color:#111827
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### By Product Line

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_line,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss,
# MAGIC   MEASURE(ecl_rate) AS ecl_rate
# MAGIC FROM credit_risk_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY expected_credit_loss DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### By Region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss,
# MAGIC   MEASURE(ecl_rate) AS ecl_rate
# MAGIC FROM credit_risk_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY expected_credit_loss DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### By Risk Band

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_band,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss,
# MAGIC   MEASURE(ecl_rate) AS ecl_rate
# MAGIC FROM credit_risk_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY ecl_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### By Product Line and Risk Band

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_line,
# MAGIC   risk_band,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss,
# MAGIC   MEASURE(ecl_rate) AS ecl_rate
# MAGIC FROM credit_risk_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY product_line, ecl_rate DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate the Metric View Contract
# MAGIC
# MAGIC If a Metric View is certified, it should be validated.
# MAGIC
# MAGIC This check compares explicit source-table logic with the Metric View output.

# COMMAND ----------

comparison = spark.sql(
    """
WITH manual AS (
  SELECT
    product_line,
    risk_band,
    SUM(exposure_amount) AS exposure_amount,
    SUM(expected_credit_loss) AS expected_credit_loss,
    SUM(expected_credit_loss) / NULLIF(SUM(exposure_amount), 0) AS ecl_rate
  FROM credit_risk_exposures
  GROUP BY ALL
),
metric_view AS (
  SELECT
    product_line,
    risk_band,
    MEASURE(exposure_amount) AS exposure_amount,
    MEASURE(expected_credit_loss) AS expected_credit_loss,
    MEASURE(ecl_rate) AS ecl_rate
  FROM credit_risk_metrics
  GROUP BY ALL
)
SELECT
  COUNT(*) AS mismatches
FROM manual m
FULL OUTER JOIN metric_view v
  ON m.product_line = v.product_line
  AND m.risk_band = v.risk_band
WHERE
  ABS(COALESCE(m.exposure_amount, 0) - COALESCE(v.exposure_amount, 0)) > 0.001
  OR ABS(COALESCE(m.expected_credit_loss, 0) - COALESCE(v.expected_credit_loss, 0)) > 0.001
  OR ABS(COALESCE(m.ecl_rate, 0) - COALESCE(v.ecl_rate, 0)) > 0.000001
"""
).collect()[0]["mismatches"]

print(f"Credit risk Metric View mismatches: {comparison}")
require(comparison == 0, "Metric View results should match manual source calculation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Certification Makes the Trust Signal Visible
# MAGIC
# MAGIC In Part 1, the domain page showed certified and deprecated assets.
# MAGIC
# MAGIC Here we verify that the Metric Views are tagged as certified and assigned to the right business context.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM information_schema.table_tags
# MAGIC WHERE catalog_name = '${catalog}'
# MAGIC   AND schema_name = '${schema}'
# MAGIC   AND table_name IN (
# MAGIC     'credit_risk_metrics',
# MAGIC     'fraud_risk_metrics',
# MAGIC     'risk_compliance_executive_metrics'
# MAGIC   )
# MAGIC ORDER BY table_name, tag_name

# COMMAND ----------

certified_metric_views = scalar(
    f"""
SELECT COUNT(DISTINCT table_name) AS value
FROM `{catalog}`.information_schema.table_tags
WHERE schema_name = '{schema}'
  AND table_name IN ('credit_risk_metrics', 'fraud_risk_metrics', 'risk_compliance_executive_metrics')
  AND tag_name = 'system.certification_status'
  AND tag_value = 'certified'
"""
)

require(certified_metric_views == 3, f"Expected 3 certified Metric Views, found {certified_metric_views}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Bonus: Promoting Dashboard-Local Metrics
# MAGIC
# MAGIC Many teams start in dashboards.
# MAGIC
# MAGIC A dashboard author or Genie Code might create a local metric definition inside an AI/BI dashboard. That is useful for prototyping, but it can become a problem when the metric becomes important.
# MAGIC
# MAGIC A good promotion path is:
# MAGIC
# MAGIC ```text
# MAGIC dashboard-local calculation
# MAGIC -> reviewed KPI definition
# MAGIC -> Unity Catalog Metric View
# MAGIC -> certified domain asset
# MAGIC ```
# MAGIC
# MAGIC This promotion is how a useful local dashboard idea becomes a reusable enterprise metric.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  Local["Dashboard-local metric<br/>prototype"]
  Review["Review and standardize<br/>definition + owner"]
  UC["Unity Catalog Metric View<br/>certified KPI"]
  Discover["Discover domain asset<br/>trusted context"]

  Local --> Review --> UC --> Discover

  style Local fill:#FEF3C7,stroke:#D97706,color:#111827
  style Review fill:#EEF2FF,stroke:#4F46E5,color:#111827
  style UC fill:#DCFCE7,stroke:#16A34A,color:#111827
  style Discover fill:#E0F2FE,stroke:#0284C7,color:#111827
"""
)

# COMMAND ----------

dashboard_local_metric_examples = spark.createDataFrame(
    [
        (
            "Dashboard-local ECL rate",
            "SUM(expected_credit_loss) / SUM(exposure_amount)",
            "ecl_rate in credit_risk_metrics",
            "Promote when multiple dashboards or agents need the same definition.",
        ),
        (
            "Dashboard-local confirmed loss rate",
            "SUM(confirmed_loss) / SUM(suspicious_amount)",
            "confirmed_loss_rate in fraud_risk_metrics",
            "Promote when fraud operations needs a shared definition.",
        ),
    ],
    ["local_metric", "local_formula", "promoted_metric_view_measure", "promotion_trigger"],
)

display(dashboard_local_metric_examples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Why Agents Need Metric Views
# MAGIC
# MAGIC A table tells an agent what data exists.
# MAGIC
# MAGIC A Metric View tells the agent what the business means.
# MAGIC
# MAGIC If an agent is asked:
# MAGIC
# MAGIC ```text
# MAGIC Which credit risk segment has the highest expected loss?
# MAGIC ```
# MAGIC
# MAGIC it should not infer the KPI formula from raw columns if a certified Metric View exists. It should use the certified KPI definition.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   risk_area,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(loss_or_exposure_metric) AS loss_or_exposure_metric,
# MAGIC   MEASURE(average_risk_rate) AS average_risk_rate
# MAGIC FROM risk_compliance_executive_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY risk_area

# COMMAND ----------

# MAGIC %md
# MAGIC ## What This Sets Up
# MAGIC
# MAGIC This notebook covered the basics:
# MAGIC
# MAGIC - fields and measures,
# MAGIC - why view sprawl happens,
# MAGIC - `MEASURE()` as the query contract,
# MAGIC - slice-and-dice from one Metric View,
# MAGIC - certification and validation,
# MAGIC - and promoting useful dashboard-local metrics into Unity Catalog Metric Views.
# MAGIC
# MAGIC The next step is advanced metric semantics:
# MAGIC
# MAGIC - level of detail,
# MAGIC - window semantics,
# MAGIC - and agent metadata.
# MAGIC
# MAGIC Materialization and production performance are intentionally deferred to Part 4, after the series has established which Metric Views should be trusted.

# COMMAND ----------

print("Metric Views certified KPI layer checks completed successfully.")
