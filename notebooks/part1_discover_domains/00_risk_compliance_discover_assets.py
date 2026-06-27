# Databricks notebook source
# MAGIC %md
# MAGIC # Setup - Risk and Compliance Discover Assets
# MAGIC
# MAGIC This notebook creates screenshot-safe synthetic assets for a **Risk and Compliance** Discover domain.
# MAGIC
# MAGIC It intentionally uses generic banking language and synthetic data only:
# MAGIC
# MAGIC - No real bank name
# MAGIC - No real customer name
# MAGIC - No PAN, account number, or personal data
# MAGIC - No production metadata
# MAGIC
# MAGIC The assets are organized for the following Discover structure:
# MAGIC
# MAGIC - Risk and Compliance
# MAGIC   - Credit Risk
# MAGIC   - Fraud Risk
# MAGIC   - AML and KYC
# MAGIC   - Operational Risk
# MAGIC   - Regulatory Reporting

# COMMAND ----------

dbutils.widgets.text("catalog", "steven_discover_domains", "Catalog")
dbutils.widgets.text("schema", "risk_compliance_context_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
qualified_schema = f"`{catalog}`.`{schema}`"

print(f"Creating Risk and Compliance demo assets in: {catalog}.{schema}")

# COMMAND ----------

def run_sql(statement: str, label: str) -> None:
    print(f"Running: {label}")
    spark.sql(statement)


def tag_object(object_type: str, object_name: str, tag_key: str, label: str, tag_value: str | None = None) -> tuple[str, str, str]:
    if tag_value is None:
        statement = f"ALTER {object_type} {object_name} SET TAGS ('{tag_key}')"
    else:
        statement = f"ALTER {object_type} {object_name} SET TAGS ('{tag_key}' = '{tag_value}')"

    try:
        spark.sql(statement)
        return (label, "SUCCESS", "")
    except Exception as error:
        return (label, "FAILED", str(error)[:1000])


def tag_table(object_name: str, tag_key: str, label: str, tag_value: str | None = None) -> tuple[str, str, str]:
    return tag_object("TABLE", object_name, tag_key, label, tag_value)


def tag_view(object_name: str, tag_key: str, label: str, tag_value: str | None = None) -> tuple[str, str, str]:
    return tag_object("VIEW", object_name, tag_key, label, tag_value)

# COMMAND ----------

run_sql(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}", "create schema")
run_sql(f"USE CATALOG `{catalog}`", "use catalog")
run_sql(f"USE SCHEMA `{schema}`", "use schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Tables

# COMMAND ----------

run_sql(
    """
CREATE OR REPLACE TABLE credit_risk_exposures AS
SELECT * FROM VALUES
  (DATE '2025-01-31', 'Mortgage', 'Prime', 'APJ', 12500000.00, 0.014, 0.22, 275000.00),
  (DATE '2025-01-31', 'Credit Card', 'Near Prime', 'APJ', 4200000.00, 0.038, 0.44, 184800.00),
  (DATE '2025-01-31', 'Personal Loan', 'Subprime', 'AMER', 3100000.00, 0.082, 0.51, 158100.00),
  (DATE '2025-02-28', 'Mortgage', 'Prime', 'APJ', 12800000.00, 0.013, 0.21, 268800.00),
  (DATE '2025-02-28', 'Credit Card', 'Near Prime', 'APJ', 4550000.00, 0.041, 0.45, 204750.00),
  (DATE '2025-02-28', 'Personal Loan', 'Subprime', 'AMER', 3350000.00, 0.087, 0.53, 177550.00)
AS credit_risk_exposures(reporting_date, product_line, risk_band, region, exposure_amount, probability_of_default, loss_given_default, expected_credit_loss)
""",
    "create credit risk exposures",
)

run_sql(
    """
CREATE OR REPLACE TABLE fraud_risk_events AS
SELECT * FROM VALUES
  (DATE '2025-02-01', 'Card Not Present', 'Digital', 'High', 180, 32000.00, 21000.00),
  (DATE '2025-02-01', 'Account Takeover', 'Mobile', 'Critical', 24, 88000.00, 54000.00),
  (DATE '2025-02-01', 'Merchant Collusion', 'Merchant', 'High', 12, 76000.00, 46000.00),
  (DATE '2025-02-02', 'Card Not Present', 'Digital', 'Medium', 146, 27000.00, 18000.00),
  (DATE '2025-02-02', 'Account Takeover', 'Mobile', 'Critical', 31, 104000.00, 68000.00),
  (DATE '2025-02-02', 'Synthetic Identity', 'Branch', 'High', 9, 59000.00, 39000.00)
AS fraud_risk_events(event_date, fraud_typology, channel, severity, alert_count, suspicious_amount, confirmed_loss)
""",
    "create fraud risk events",
)

run_sql(
    """
CREATE OR REPLACE TABLE aml_kyc_monitoring AS
SELECT * FROM VALUES
  (DATE '2025-02-01', 'Retail Individual', 'Low', 'Current', 18400, 52, 4),
  (DATE '2025-02-01', 'Retail Individual', 'High', 'Refresh Due', 920, 38, 11),
  (DATE '2025-02-01', 'Small Business', 'Medium', 'Current', 4200, 73, 7),
  (DATE '2025-02-01', 'Small Business', 'High', 'Refresh Due', 610, 41, 14),
  (DATE '2025-02-01', 'Wealth', 'High', 'Enhanced Review', 180, 26, 9)
AS aml_kyc_monitoring(snapshot_date, customer_segment, risk_rating, kyc_status, customer_count, monitoring_alerts, escalated_cases)
""",
    "create AML KYC monitoring",
)

run_sql(
    """
CREATE OR REPLACE TABLE operational_risk_incidents AS
SELECT * FROM VALUES
  (DATE '2025-02-01', 'Payments Operations', 'Process Failure', 'Medium', 8, 5, 0.82),
  (DATE '2025-02-01', 'Digital Channels', 'System Outage', 'High', 2, 2, 0.76),
  (DATE '2025-02-01', 'Third Party Services', 'Vendor SLA Breach', 'Medium', 5, 4, 0.69),
  (DATE '2025-02-02', 'Payments Operations', 'Process Failure', 'Medium', 6, 4, 0.85),
  (DATE '2025-02-02', 'Digital Channels', 'System Outage', 'Critical', 1, 1, 0.71),
  (DATE '2025-02-02', 'Branch Operations', 'Manual Control Gap', 'Low', 12, 7, 0.88)
AS operational_risk_incidents(incident_date, business_process, incident_type, severity, incident_count, open_issues, control_effectiveness_score)
""",
    "create operational risk incidents",
)

run_sql(
    """
CREATE OR REPLACE TABLE regulatory_reporting_calendar AS
SELECT * FROM VALUES
  ('Monthly Fraud Loss Report', 'Fraud Risk', DATE '2025-03-05', 'In Progress', 0.91, 'Risk Reporting Team'),
  ('Credit Portfolio Review', 'Credit Risk', DATE '2025-03-10', 'Ready for Review', 0.96, 'Credit Risk Analytics'),
  ('AML Monitoring Attestation', 'AML and KYC', DATE '2025-03-15', 'Evidence Collection', 0.88, 'Financial Crime Compliance'),
  ('Operational Risk Committee Pack', 'Operational Risk', DATE '2025-03-12', 'Ready for Review', 0.93, 'Operational Risk Office'),
  ('Board Risk Dashboard', 'Risk and Compliance', DATE '2025-03-20', 'Draft', 0.86, 'Enterprise Risk Reporting')
AS regulatory_reporting_calendar(report_name, risk_area, due_date, reporting_status, evidence_completeness, owner)
""",
    "create regulatory reporting calendar",
)

run_sql(
    """
CREATE OR REPLACE TABLE risk_kpi_definitions AS
SELECT * FROM VALUES
  ('Expected Credit Loss', 'Credit Risk', 'Estimated credit loss based on exposure, probability of default, and loss given default.', 'Credit Risk Analytics', 'certified'),
  ('Confirmed Fraud Loss', 'Fraud Risk', 'Confirmed financial loss from fraud events after investigation.', 'Fraud Operations', 'certified'),
  ('Escalated AML Cases', 'AML and KYC', 'Monitoring cases escalated for enhanced compliance review.', 'Financial Crime Compliance', 'certified'),
  ('Control Effectiveness Score', 'Operational Risk', 'Composite indicator of control operating effectiveness.', 'Operational Risk Office', 'certified'),
  ('Evidence Completeness', 'Regulatory Reporting', 'Share of required evidence available for a reporting obligation.', 'Risk Reporting Team', 'certified'),
  ('Legacy Manual Risk Rating', 'Credit Risk', 'Deprecated spreadsheet-based risk classification.', 'Credit Risk Analytics', 'deprecated')
AS risk_kpi_definitions(kpi_name, subdomain, business_definition, owner, lifecycle_status)
""",
    "create risk KPI definitions",
)

run_sql(
    """
CREATE OR REPLACE TABLE legacy_manual_fraud_extract AS
SELECT * FROM VALUES
  (DATE '2024-12-31', 'manual_extract_v1', 104, 92000.00),
  (DATE '2025-01-31', 'manual_extract_v1', 117, 103000.00)
AS legacy_manual_fraud_extract(extract_date, extract_name, suspected_cases, suspected_amount)
""",
    "create deprecated legacy fraud extract",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Views

# COMMAND ----------

run_sql(
    """
CREATE OR REPLACE VIEW credit_risk_portfolio_summary AS
SELECT
  reporting_date,
  product_line,
  risk_band,
  region,
  SUM(exposure_amount) AS exposure_amount,
  SUM(expected_credit_loss) AS expected_credit_loss,
  SUM(expected_credit_loss) / NULLIF(SUM(exposure_amount), 0) AS ecl_rate
FROM credit_risk_exposures
GROUP BY ALL
""",
    "create credit risk summary view",
)

run_sql(
    """
CREATE OR REPLACE VIEW fraud_risk_daily_summary AS
SELECT
  event_date,
  fraud_typology,
  channel,
  severity,
  SUM(alert_count) AS alert_count,
  SUM(suspicious_amount) AS suspicious_amount,
  SUM(confirmed_loss) AS confirmed_loss,
  SUM(confirmed_loss) / NULLIF(SUM(suspicious_amount), 0) AS confirmed_loss_rate
FROM fraud_risk_events
GROUP BY ALL
""",
    "create fraud risk summary view",
)

run_sql(
    """
CREATE OR REPLACE VIEW aml_kyc_monitoring_summary AS
SELECT
  snapshot_date,
  customer_segment,
  risk_rating,
  kyc_status,
  SUM(customer_count) AS customer_count,
  SUM(monitoring_alerts) AS monitoring_alerts,
  SUM(escalated_cases) AS escalated_cases,
  SUM(escalated_cases) / NULLIF(SUM(monitoring_alerts), 0) AS escalation_rate
FROM aml_kyc_monitoring
GROUP BY ALL
""",
    "create AML KYC summary view",
)

run_sql(
    """
CREATE OR REPLACE VIEW operational_risk_control_summary AS
SELECT
  incident_date,
  business_process,
  incident_type,
  severity,
  SUM(incident_count) AS incident_count,
  SUM(open_issues) AS open_issues,
  AVG(control_effectiveness_score) AS avg_control_effectiveness_score
FROM operational_risk_incidents
GROUP BY ALL
""",
    "create operational risk summary view",
)

run_sql(
    """
CREATE OR REPLACE VIEW regulatory_reporting_readiness AS
SELECT
  risk_area,
  reporting_status,
  COUNT(*) AS report_count,
  AVG(evidence_completeness) AS avg_evidence_completeness,
  MIN(due_date) AS next_due_date
FROM regulatory_reporting_calendar
GROUP BY ALL
""",
    "create regulatory reporting readiness view",
)

run_sql(
    """
CREATE OR REPLACE VIEW risk_compliance_executive_summary AS
SELECT
  'Credit Risk' AS risk_area,
  CAST(MAX(reporting_date) AS DATE) AS as_of_date,
  SUM(exposure_amount) AS exposure_amount,
  SUM(expected_credit_loss) AS loss_or_exposure_metric,
  SUM(expected_credit_loss) / NULLIF(SUM(exposure_amount), 0) AS risk_rate
FROM credit_risk_exposures
UNION ALL
SELECT
  'Fraud Risk' AS risk_area,
  CAST(MAX(event_date) AS DATE) AS as_of_date,
  SUM(suspicious_amount) AS exposure_amount,
  SUM(confirmed_loss) AS loss_or_exposure_metric,
  SUM(confirmed_loss) / NULLIF(SUM(suspicious_amount), 0) AS risk_rate
FROM fraud_risk_events
UNION ALL
SELECT
  'AML and KYC' AS risk_area,
  CAST(MAX(snapshot_date) AS DATE) AS as_of_date,
  SUM(monitoring_alerts) AS exposure_amount,
  SUM(escalated_cases) AS loss_or_exposure_metric,
  SUM(escalated_cases) / NULLIF(SUM(monitoring_alerts), 0) AS risk_rate
FROM aml_kyc_monitoring
UNION ALL
SELECT
  'Operational Risk' AS risk_area,
  CAST(MAX(incident_date) AS DATE) AS as_of_date,
  SUM(incident_count) AS exposure_amount,
  SUM(open_issues) AS loss_or_exposure_metric,
  AVG(control_effectiveness_score) AS risk_rate
FROM operational_risk_incidents
""",
    "create executive summary view",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric Views

# COMMAND ----------

run_sql(
    """
CREATE OR REPLACE VIEW credit_risk_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Certified credit risk metrics for the Risk and Compliance Discover demo.
source: credit_risk_portfolio_summary
fields:
  - name: reporting_date
    expr: reporting_date
  - name: product_line
    expr: product_line
  - name: risk_band
    expr: risk_band
  - name: region
    expr: region
measures:
  - name: exposure_amount
    expr: SUM(exposure_amount)
    display_name: Exposure Amount
    synonyms: [exposure, outstanding balance, credit exposure]
  - name: expected_credit_loss
    expr: SUM(expected_credit_loss)
    display_name: Expected Credit Loss
    synonyms: [ECL, provisions, credit loss]
  - name: ecl_rate
    expr: MEASURE(expected_credit_loss) / NULLIF(MEASURE(exposure_amount), 0)
    display_name: ECL Rate
    synonyms: [loss rate, provision rate]
$$
""",
    "create credit risk Metric View",
)

run_sql(
    """
CREATE OR REPLACE VIEW fraud_risk_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Certified fraud risk metrics for the Risk and Compliance Discover demo.
source: fraud_risk_daily_summary
fields:
  - name: event_date
    expr: event_date
  - name: fraud_typology
    expr: fraud_typology
  - name: channel
    expr: channel
  - name: severity
    expr: severity
measures:
  - name: alert_count
    expr: SUM(alert_count)
    display_name: Fraud Alerts
    synonyms: [alerts, fraud alerts, suspicious events]
  - name: suspicious_amount
    expr: SUM(suspicious_amount)
    display_name: Suspicious Amount
    synonyms: [suspicious exposure, flagged amount]
  - name: confirmed_loss
    expr: SUM(confirmed_loss)
    display_name: Confirmed Fraud Loss
    synonyms: [fraud loss, confirmed loss]
  - name: confirmed_loss_rate
    expr: MEASURE(confirmed_loss) / NULLIF(MEASURE(suspicious_amount), 0)
    display_name: Confirmed Loss Rate
$$
""",
    "create fraud risk Metric View",
)

run_sql(
    """
CREATE OR REPLACE VIEW risk_compliance_executive_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Executive Risk and Compliance metrics for board and management reporting.
source: risk_compliance_executive_summary
fields:
  - name: risk_area
    expr: risk_area
  - name: as_of_date
    expr: as_of_date
measures:
  - name: exposure_amount
    expr: SUM(exposure_amount)
    display_name: Risk Exposure
  - name: loss_or_exposure_metric
    expr: SUM(loss_or_exposure_metric)
    display_name: Loss or Escalated Exposure
  - name: average_risk_rate
    expr: AVG(risk_rate)
    display_name: Average Risk Rate
$$
""",
    "create executive Metric View",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comments

# COMMAND ----------

comments = {
    "credit_risk_exposures": "Synthetic credit risk exposure data for the Risk and Compliance Discover demo.",
    "fraud_risk_events": "Synthetic fraud alert and confirmed loss data for the Risk and Compliance Discover demo.",
    "aml_kyc_monitoring": "Synthetic AML and KYC monitoring data for customer risk and review workflows.",
    "operational_risk_incidents": "Synthetic operational incident and control effectiveness data.",
    "regulatory_reporting_calendar": "Synthetic reporting calendar for board, management, and regulator-facing obligations.",
    "risk_kpi_definitions": "Business definitions and ownership context for certified risk and compliance KPIs.",
    "legacy_manual_fraud_extract": "Deprecated synthetic legacy extract retained to show lifecycle status in Discover.",
    "credit_risk_portfolio_summary": "Certified credit risk summary view for exposure, expected credit loss, and ECL rate.",
    "fraud_risk_daily_summary": "Certified fraud risk summary view for alerts, suspicious amounts, and confirmed fraud loss.",
    "aml_kyc_monitoring_summary": "Certified AML and KYC summary view for monitoring alerts and escalated cases.",
    "operational_risk_control_summary": "Certified operational risk summary view for incidents, issues, and control effectiveness.",
    "regulatory_reporting_readiness": "Certified regulatory reporting readiness view for due dates and evidence completeness.",
    "risk_compliance_executive_summary": "Certified executive summary view across key risk and compliance areas.",
    "credit_risk_metrics": "Certified Metric View for credit risk exposure and expected credit loss.",
    "fraud_risk_metrics": "Certified Metric View for fraud risk alerts and confirmed loss.",
    "risk_compliance_executive_metrics": "Certified Metric View for executive risk and compliance reporting.",
}

for object_name, comment in comments.items():
    run_sql(f"COMMENT ON TABLE {object_name} IS '{comment}'", f"comment {object_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tags

# COMMAND ----------

object_tags = {
    "credit_risk_exposures": ("TABLE", "Risk and Compliance/Credit Risk", "certified"),
    "credit_risk_portfolio_summary": ("VIEW", "Risk and Compliance/Credit Risk", "certified"),
    "credit_risk_metrics": ("VIEW", "Risk and Compliance/Credit Risk", "certified"),
    "fraud_risk_events": ("TABLE", "Risk and Compliance/Fraud Risk", "certified"),
    "fraud_risk_daily_summary": ("VIEW", "Risk and Compliance/Fraud Risk", "certified"),
    "fraud_risk_metrics": ("VIEW", "Risk and Compliance/Fraud Risk", "certified"),
    "legacy_manual_fraud_extract": ("TABLE", "Risk and Compliance/Fraud Risk", "deprecated"),
    "aml_kyc_monitoring": ("TABLE", "Risk and Compliance/AML and KYC", "certified"),
    "aml_kyc_monitoring_summary": ("VIEW", "Risk and Compliance/AML and KYC", "certified"),
    "operational_risk_incidents": ("TABLE", "Risk and Compliance/Operational Risk", "certified"),
    "operational_risk_control_summary": ("VIEW", "Risk and Compliance/Operational Risk", "certified"),
    "regulatory_reporting_calendar": ("TABLE", "Risk and Compliance/Regulatory Reporting", "certified"),
    "regulatory_reporting_readiness": ("VIEW", "Risk and Compliance/Regulatory Reporting", "certified"),
    "risk_compliance_executive_summary": ("VIEW", "Risk and Compliance/Regulatory Reporting", "certified"),
    "risk_compliance_executive_metrics": ("VIEW", "Risk and Compliance/Regulatory Reporting", "certified"),
    "risk_kpi_definitions": ("TABLE", "Risk and Compliance", "certified"),
}

tag_results = []
for object_name, (object_type, subdomain_tag, lifecycle_status) in object_tags.items():
    tag_results.append(tag_object(object_type, object_name, "Risk and Compliance", f"{object_name} top-level domain"))
    tag_results.append(tag_object(object_type, object_name, subdomain_tag, f"{object_name} subdomain"))
    tag_results.append(
        tag_object(
            object_type,
            object_name,
            "system.certification_status",
            f"{object_name} lifecycle",
            lifecycle_status,
        )
    )

tag_results_df = spark.createDataFrame(tag_results, ["operation", "status", "error"])
display(tag_results_df)
tag_results_df.createOrReplaceTempView("tag_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curated Asset Catalog

# COMMAND ----------

run_sql(
    """
CREATE OR REPLACE TABLE risk_compliance_discover_asset_catalog AS
SELECT * FROM VALUES
  ('Credit Risk', 'Table', 'credit_risk_exposures', 'Borrower exposure, portfolio quality, and expected credit loss inputs.', 'certified'),
  ('Credit Risk', 'Metric View', 'credit_risk_metrics', 'Certified credit exposure, expected credit loss, and ECL rate metrics.', 'certified'),
  ('Fraud Risk', 'Table', 'fraud_risk_events', 'Fraud alerts, suspicious amounts, and confirmed fraud loss.', 'certified'),
  ('Fraud Risk', 'Metric View', 'fraud_risk_metrics', 'Certified fraud alert and confirmed loss metrics.', 'certified'),
  ('Fraud Risk', 'Table', 'legacy_manual_fraud_extract', 'Legacy manual extract retained only to demonstrate deprecated lifecycle status.', 'deprecated'),
  ('AML and KYC', 'Table', 'aml_kyc_monitoring', 'Customer risk, KYC status, monitoring alerts, and escalations.', 'certified'),
  ('AML and KYC', 'View', 'aml_kyc_monitoring_summary', 'AML/KYC monitoring summary by segment, risk rating, and status.', 'certified'),
  ('Operational Risk', 'Table', 'operational_risk_incidents', 'Operational incidents, control outcomes, issues, and resilience indicators.', 'certified'),
  ('Operational Risk', 'View', 'operational_risk_control_summary', 'Operational risk summary by process, incident type, and severity.', 'certified'),
  ('Regulatory Reporting', 'Table', 'regulatory_reporting_calendar', 'Reporting obligations, due dates, readiness, and evidence completeness.', 'certified'),
  ('Regulatory Reporting', 'Metric View', 'risk_compliance_executive_metrics', 'Executive metrics across risk and compliance areas.', 'certified'),
  ('Risk and Compliance', 'Table', 'risk_kpi_definitions', 'Business definitions and owners for risk and compliance KPIs.', 'certified')
AS risk_compliance_discover_asset_catalog(subdomain, asset_type, asset_name, business_purpose, lifecycle_status)
""",
    "create discover asset catalog",
)

run_sql("COMMENT ON TABLE risk_compliance_discover_asset_catalog IS 'Curated inventory for the Risk and Compliance Discover demo.'", "comment asset catalog")

tag_results_extra = [
    tag_table("risk_compliance_discover_asset_catalog", "Risk and Compliance", "asset catalog top-level domain"),
    tag_table(
        "risk_compliance_discover_asset_catalog",
        "system.certification_status",
        "asset catalog lifecycle",
        "certified",
    ),
]

extra_df = spark.createDataFrame(tag_results_extra, ["operation", "status", "error"])
extra_df.createOrReplaceTempView("tag_results_extra")
display(extra_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Outputs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   table_type,
# MAGIC   comment
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_catalog = '${catalog}'
# MAGIC   AND table_schema = '${schema}'
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   catalog_name,
# MAGIC   schema_name,
# MAGIC   table_name,
# MAGIC   tag_name,
# MAGIC   tag_value
# MAGIC FROM information_schema.table_tags
# MAGIC WHERE catalog_name = '${catalog}'
# MAGIC   AND schema_name = '${schema}'
# MAGIC ORDER BY table_name, tag_name

# COMMAND ----------

failed_tags = spark.sql(
    """
SELECT COUNT(*) AS failed
FROM (
  SELECT * FROM tag_results
  UNION ALL
  SELECT * FROM tag_results_extra
)
WHERE status <> 'SUCCESS'
"""
).collect()[0]["failed"]

if failed_tags > 0:
    raise AssertionError(f"{failed_tags} tag operation(s) failed. Review tag output above.")

asset_count = spark.sql(
    f"""
SELECT COUNT(*) AS asset_count
FROM `{catalog}`.information_schema.tables
WHERE table_schema = '{schema}'
"""
).collect()[0]["asset_count"]

if asset_count < 17:
    raise AssertionError(f"Expected at least 17 assets, found {asset_count}")

print("Risk and Compliance Discover assets are ready.")
