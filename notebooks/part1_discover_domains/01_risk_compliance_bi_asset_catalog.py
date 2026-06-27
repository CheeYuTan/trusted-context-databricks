# Databricks notebook source
# MAGIC %md
# MAGIC # Setup - Risk and Compliance BI Asset Catalog
# MAGIC
# MAGIC This notebook records screenshot-safe BI and Genie assets created for the Discover domain walkthrough.

# COMMAND ----------

dbutils.widgets.text("catalog", "steven_discover_domains", "Catalog")
dbutils.widgets.text("schema", "risk_compliance_context_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE risk_compliance_bi_asset_catalog AS
SELECT * FROM VALUES
  ('Risk and Compliance', 'Dashboard', 'Risk and Compliance Executive Overview', 'Executive risk summary across credit, fraud, AML/KYC, operational risk, and regulatory reporting.', '01f171c6144d1075b912fc39ae811cbf', 'certified'),
  ('Credit Risk', 'Dashboard', 'Credit Risk Portfolio Monitor', 'Credit exposure, expected credit loss, risk bands, and portfolio quality.', '01f171c6156219738cbea26f7d4482af', 'certified'),
  ('Fraud Risk', 'Dashboard', 'Fraud Risk Operations Monitor', 'Fraud alerts, suspicious exposure, confirmed losses, and fraud typologies.', '01f171c6166218dc9f3befc412dfcb4e', 'certified'),
  ('AML and KYC', 'Dashboard', 'AML and KYC Monitoring Dashboard', 'Customer risk ratings, KYC status, monitoring alerts, and escalated cases.', '01f171c6175e15408117713b1efd696d', 'certified'),
  ('Operational Risk', 'Dashboard', 'Operational Risk Control Dashboard', 'Operational incidents, open issues, control effectiveness, and resilience indicators.', '01f171c6186214bf954f436149ea6361', 'certified'),
  ('Regulatory Reporting', 'Dashboard', 'Regulatory Reporting Readiness Dashboard', 'Report due dates, evidence completeness, statuses, and ownership context.', '01f171c6196d1717be4c3cff8990e293', 'certified'),
  ('Risk and Compliance', 'Genie Space', 'Ask Risk and Compliance', 'Natural language exploration across the Risk and Compliance domain.', '01f171c6097415bdb9bafe2986843de9', 'certified'),
  ('Credit Risk', 'Genie Space', 'Ask Credit Risk', 'Natural language exploration of credit exposure, portfolio quality, and expected credit loss.', '01f171c60a0115e5b6cd4a326caabf69', 'certified'),
  ('Fraud Risk', 'Genie Space', 'Ask Fraud Risk', 'Natural language exploration of fraud alerts, typologies, and confirmed losses.', '01f171c60a7d19c4a0b6fe00cbd5a23f', 'certified'),
  ('AML and KYC', 'Genie Space', 'Ask AML and KYC', 'Natural language exploration of KYC status, risk ratings, monitoring alerts, and escalations.', '01f171c60afb1afab2c3355af3479d65', 'certified'),
  ('Operational Risk', 'Genie Space', 'Ask Operational Risk', 'Natural language exploration of incidents, controls, open issues, and resilience indicators.', '01f171c60b7c193f80842aef51a3e9b8', 'certified'),
  ('Regulatory Reporting', 'Genie Space', 'Ask Regulatory Reporting', 'Natural language exploration of reporting due dates, readiness, and evidence completeness.', '01f171c60bff1ba6a1071c01ae858671', 'certified')
AS risk_compliance_bi_asset_catalog(subdomain, asset_type, asset_name, business_purpose, asset_id, lifecycle_status)
"""
)

spark.sql(
    """
COMMENT ON TABLE risk_compliance_bi_asset_catalog IS
'Curated BI and Genie asset inventory for the Risk and Compliance Discover walkthrough. Contains synthetic demo metadata only.'
"""
)

# COMMAND ----------

tag_results = []
for statement, label in [
    ("ALTER TABLE risk_compliance_bi_asset_catalog SET TAGS ('Risk and Compliance')", "top-level domain tag"),
    ("ALTER TABLE risk_compliance_bi_asset_catalog SET TAGS ('system.certification_status' = 'certified')", "certification tag"),
]:
    try:
        spark.sql(statement)
        tag_results.append((label, "SUCCESS", ""))
    except Exception as error:
        tag_results.append((label, "FAILED", str(error)[:1000]))

tag_results_df = spark.createDataFrame(tag_results, ["operation", "status", "error"])
display(tag_results_df)
tag_results_df.createOrReplaceTempView("tag_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM risk_compliance_bi_asset_catalog ORDER BY subdomain, asset_type, asset_name

# COMMAND ----------

failed_tags = spark.sql("SELECT COUNT(*) AS failed FROM tag_results WHERE status <> 'SUCCESS'").collect()[0]["failed"]
if failed_tags > 0:
    raise AssertionError(f"{failed_tags} tag operation(s) failed.")

asset_count = spark.sql("SELECT COUNT(*) AS asset_count FROM risk_compliance_bi_asset_catalog").collect()[0]["asset_count"]
if asset_count != 12:
    raise AssertionError(f"Expected 12 BI/Genie assets, found {asset_count}")

print("Risk and Compliance BI asset catalog is ready.")
