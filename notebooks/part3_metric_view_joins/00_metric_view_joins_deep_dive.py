# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3 - Deep Dive Into Joins in Metric Views
# MAGIC
# MAGIC This notebook supports the third article in the **Building Trusted Context in Databricks** series.
# MAGIC
# MAGIC Part 1 answered:
# MAGIC
# MAGIC > Where should I look?
# MAGIC
# MAGIC Part 2 answered:
# MAGIC
# MAGIC > Which KPI definition should I trust?
# MAGIC
# MAGIC Part 3 answers:
# MAGIC
# MAGIC > How should the semantic layer model relationships between facts and dimensions?
# MAGIC
# MAGIC We will cover the major join patterns from the Databricks documentation:
# MAGIC
# MAGIC - Star schema joins
# MAGIC - Snowflake schema joins
# MAGIC - Many-to-one joins and `rely`
# MAGIC - One-to-many joins
# MAGIC - Nested one-to-many joins
# MAGIC - Sibling one-to-many joins
# MAGIC - Bridge-table pattern for multiple fact tables
# MAGIC
# MAGIC ## How to Read This Notebook
# MAGIC
# MAGIC If you are new to Metric View joins, think of each pattern as answering a different modeling question.
# MAGIC
# MAGIC | Pattern | Use it when | Example question |
# MAGIC |---|---|---|
# MAGIC | Star schema | One fact table needs descriptive attributes from dimension tables. | "Show expected credit loss by product line and risk band." |
# MAGIC | Snowflake schema | A dimension is normalized and needs another dimension hop. | "Show exposure by branch region." |
# MAGIC | Many-to-one | Each source row should match at most one dimension row. | "Each exposure has one product and one risk grade." |
# MAGIC | One-to-many | One source row has multiple related fact rows. | "Each customer can have many applications." |
# MAGIC | Nested one-to-many | Facts sit multiple levels below the source. | "Each customer has applications, and applications have decisions." |
# MAGIC | Sibling one-to-many | One source entity has multiple independent fact branches. | "Each customer has applications and service cases." |
# MAGIC | Bridge pattern | Multiple fact tables share dimensions but live at different grains. | "Compare exposure and fraud loss by product and branch." |
# MAGIC
# MAGIC The important lesson is not "joins exist." The important lesson is:
# MAGIC
# MAGIC > The semantic layer should own relationship logic so dashboards, SQL users, and agents do not have to rewrite it.

# COMMAND ----------

dbutils.widgets.text("catalog", "steven_discover_domains", "Catalog")
dbutils.widgets.text("schema", "metric_view_joins_demo", "Schema")

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


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Model
# MAGIC
# MAGIC We will use a synthetic banking risk model. It is small enough to run quickly, but it includes enough relationships to demonstrate the join patterns.
# MAGIC
# MAGIC There are two important "centers" in the model:
# MAGIC
# MAGIC 1. **Credit exposure fact**: each row is an exposure event. This is good for star and snowflake joins because product, risk grade, branch, and region behave like dimensions.
# MAGIC 2. **Customer spine**: each row is one customer. This is good for one-to-many examples because a customer can have many applications or service cases.
# MAGIC
# MAGIC This distinction matters because Metric View joins are not just about connecting tables. They are about choosing the right **source grain**.
# MAGIC
# MAGIC ```text
# MAGIC source grain = the row-level entity the Metric View starts from
# MAGIC ```
# MAGIC
# MAGIC If the source grain is exposure, then dimensions enrich exposures.
# MAGIC If the source grain is customer, then applications and cases become fact branches below each customer.

# COMMAND ----------

render_mermaid(
    """
erDiagram
  CREDIT_EXPOSURE_FACT {
    string exposure_id
    date as_of_date
    string customer_id
    string branch_id
    string product_id
    string risk_grade_id
    double exposure_amount
    double expected_credit_loss
  }

  DIM_PRODUCT {
    string product_id
    string product_line
    string product_family
  }

  DIM_RISK_GRADE {
    string risk_grade_id
    string risk_band
  }

  DIM_BRANCH {
    string branch_id
    string branch_name
    string region_id
  }

  DIM_REGION {
    string region_id
    string region_name
  }

  CUSTOMER_SPINE {
    string customer_id
    string customer_segment
    string region_id
  }

  LOAN_APPLICATIONS {
    string application_id
    string customer_id
    double requested_amount
    string application_status
  }

  SERVICE_CASES {
    string case_id
    string customer_id
    string case_type
  }

  CREDIT_EXPOSURE_FACT }o--|| DIM_PRODUCT : product_id
  CREDIT_EXPOSURE_FACT }o--|| DIM_RISK_GRADE : risk_grade_id
  CREDIT_EXPOSURE_FACT }o--|| DIM_BRANCH : branch_id
  DIM_BRANCH }o--|| DIM_REGION : region_id
  CUSTOMER_SPINE ||--o{ LOAN_APPLICATIONS : customer_id
  CUSTOMER_SPINE ||--o{ SERVICE_CASES : customer_id
"""
)

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TABLE dim_product AS
SELECT * FROM VALUES
  ('P_CARD', 'Credit Card', 'Cards'),
  ('P_MORT', 'Mortgage', 'Secured Lending'),
  ('P_LOAN', 'Personal Loan', 'Unsecured Lending')
AS dim_product(product_id, product_line, product_family)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_risk_grade AS
SELECT * FROM VALUES
  ('RG_LOW', 'Prime', 'Low Risk'),
  ('RG_MED', 'Near Prime', 'Medium Risk'),
  ('RG_HIGH', 'Subprime', 'High Risk')
AS dim_risk_grade(risk_grade_id, risk_band, risk_tier)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_region AS
SELECT * FROM VALUES
  ('R_APJ', 'APJ'),
  ('R_AMER', 'AMER'),
  ('R_EMEA', 'EMEA')
AS dim_region(region_id, region_name)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE dim_branch AS
SELECT * FROM VALUES
  ('B_SG', 'Singapore Main', 'R_APJ'),
  ('B_AU', 'Sydney Digital', 'R_APJ'),
  ('B_US', 'US Online', 'R_AMER'),
  ('B_UK', 'London Digital', 'R_EMEA')
AS dim_branch(branch_id, branch_name, region_id)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE credit_exposure_fact AS
SELECT * FROM VALUES
  ('E001', DATE '2025-01-31', 'C001', 'B_SG', 'P_CARD', 'RG_MED', 420000.00, 18480.00),
  ('E002', DATE '2025-01-31', 'C002', 'B_SG', 'P_MORT', 'RG_LOW', 1250000.00, 27500.00),
  ('E003', DATE '2025-01-31', 'C003', 'B_US', 'P_LOAN', 'RG_HIGH', 310000.00, 15810.00),
  ('E004', DATE '2025-02-28', 'C001', 'B_SG', 'P_CARD', 'RG_MED', 455000.00, 20475.00),
  ('E005', DATE '2025-02-28', 'C004', 'B_AU', 'P_MORT', 'RG_LOW', 1280000.00, 26880.00),
  ('E006', DATE '2025-02-28', 'C003', 'B_US', 'P_LOAN', 'RG_HIGH', 335000.00, 17755.00),
  ('E007', DATE '2025-02-28', 'C005', 'B_UK', 'P_CARD', 'RG_LOW', 210000.00, 6500.00)
AS credit_exposure_fact(exposure_id, as_of_date, customer_id, branch_id, product_id, risk_grade_id, exposure_amount, expected_credit_loss)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE customer_spine AS
SELECT * FROM VALUES
  ('C001', 'Mass Affluent', 'R_APJ'),
  ('C002', 'Private Banking', 'R_APJ'),
  ('C003', 'Mass Market', 'R_AMER'),
  ('C004', 'Mass Affluent', 'R_APJ'),
  ('C005', 'Mass Market', 'R_EMEA')
AS customer_spine(customer_id, customer_segment, region_id)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE loan_applications AS
SELECT * FROM VALUES
  ('A001', 'C001', DATE '2025-01-05', 50000.00, 'Approved'),
  ('A002', 'C001', DATE '2025-02-10', 30000.00, 'Declined'),
  ('A003', 'C002', DATE '2025-01-18', 900000.00, 'Approved'),
  ('A004', 'C003', DATE '2025-02-02', 25000.00, 'Review'),
  ('A005', 'C004', DATE '2025-02-08', 650000.00, 'Approved')
AS loan_applications(application_id, customer_id, application_date, requested_amount, application_status)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE application_decisions AS
SELECT * FROM VALUES
  ('D001', 'A001', 'Auto Approved', 50000.00),
  ('D002', 'A002', 'Manual Decline', 0.00),
  ('D003', 'A003', 'Committee Approved', 900000.00),
  ('D004', 'A004', 'Pending Review', 0.00),
  ('D005', 'A005', 'Auto Approved', 650000.00)
AS application_decisions(decision_id, application_id, decision_status, approved_amount)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE service_cases AS
SELECT * FROM VALUES
  ('S001', 'C001', 'Dispute', 2),
  ('S002', 'C001', 'Limit Increase', 1),
  ('S003', 'C003', 'Collections', 3),
  ('S004', 'C005', 'Fraud Alert', 2)
AS service_cases(case_id, customer_id, case_type, priority_score)
"""
)

spark.sql(
    """
CREATE OR REPLACE TABLE fraud_event_fact AS
SELECT * FROM VALUES
  ('F001', DATE '2025-01-31', 'B_SG', 'P_CARD', 180, 21000.00),
  ('F002', DATE '2025-01-31', 'B_US', 'P_LOAN', 24, 54000.00),
  ('F003', DATE '2025-02-28', 'B_SG', 'P_CARD', 146, 18000.00),
  ('F004', DATE '2025-02-28', 'B_UK', 'P_CARD', 31, 68000.00)
AS fraud_event_fact(fraud_id, event_date, branch_id, product_id, alert_count, confirmed_loss)
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Star Schema Joins
# MAGIC
# MAGIC In a star schema, the Metric View source is the fact table and joins bring in dimension attributes.
# MAGIC
# MAGIC The join default is `many_to_one`, which is appropriate for dimension lookups.
# MAGIC
# MAGIC Use this pattern when:
# MAGIC
# MAGIC - your source table is a fact table,
# MAGIC - joined tables are descriptive dimensions,
# MAGIC - each fact row should match at most one row in each dimension,
# MAGIC - and users want to group measures by dimension attributes.
# MAGIC
# MAGIC In this example, the exposure fact table stores `product_id` and `risk_grade_id`, but a risk user wants to group by business labels:
# MAGIC
# MAGIC ```text
# MAGIC product_id -> product_line
# MAGIC risk_grade_id -> risk_band
# MAGIC ```
# MAGIC
# MAGIC The Metric View owns those joins, so the query can use business fields directly.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  Fact["Fact table<br/><b>credit_exposure_fact</b><br/><br/>product_id<br/>risk_grade_id<br/>exposure_amount"]
  Product["Dimension<br/><b>dim_product</b><br/><br/>product_id<br/>product_line<br/>product_family"]
  Risk["Dimension<br/><b>dim_risk_grade</b><br/><br/>risk_grade_id<br/>risk_band<br/>risk_tier"]

  Fact -->|"product_id"| Product
  Fact -->|"risk_grade_id"| Risk

  style Fact fill:#E0F2FE,stroke:#0284C7,color:#111827
  style Product fill:#DCFCE7,stroke:#16A34A,color:#111827
  style Risk fill:#DCFCE7,stroke:#16A34A,color:#111827
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.credit_risk_star_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Star schema Metric View for credit risk exposure.
source: {catalog}.{schema}.credit_exposure_fact
joins:
  - name: product
    source: {catalog}.{schema}.dim_product
    on: source.product_id = product.product_id
    rely:
      at_most_one_match: true
  - name: risk_grade
    source: {catalog}.{schema}.dim_risk_grade
    on: source.risk_grade_id = risk_grade.risk_grade_id
    rely:
      at_most_one_match: true
fields:
  - name: product_line
    expr: product.product_line
  - name: product_family
    expr: product.product_family
  - name: risk_band
    expr: risk_grade.risk_band
  - name: risk_tier
    expr: risk_grade.risk_tier
measures:
  - name: exposure_amount
    expr: SUM(exposure_amount)
  - name: expected_credit_loss
    expr: SUM(expected_credit_loss)
  - name: ecl_rate
    expr: MEASURE(expected_credit_loss) / NULLIF(MEASURE(exposure_amount), 0)
$$
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_line,
# MAGIC   risk_band,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss,
# MAGIC   MEASURE(ecl_rate) AS ecl_rate
# MAGIC FROM credit_risk_star_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY product_line, risk_band

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Interpret the Result
# MAGIC
# MAGIC The output proves that users can group credit exposure measures by `product_line` and `risk_band` without writing a join.
# MAGIC
# MAGIC The measure logic comes from the fact table. The grouping labels come from dimensions. The Metric View makes them feel like one business object.
# MAGIC
# MAGIC This is the most common join pattern for Metric Views.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Snowflake Schema Joins
# MAGIC
# MAGIC A snowflake schema adds nested joins. Here the exposure fact joins to `branch`, then `branch` joins to `region`.
# MAGIC
# MAGIC This lets the Metric View expose fields like `region_name` without making every user or agent know the multi-hop join path.
# MAGIC
# MAGIC Use this pattern when your dimensions are normalized.
# MAGIC
# MAGIC In a banking model, a fact table might know the branch, but not the region name directly:
# MAGIC
# MAGIC ```text
# MAGIC credit_exposure_fact.branch_id
# MAGIC   -> dim_branch.region_id
# MAGIC   -> dim_region.region_name
# MAGIC ```
# MAGIC
# MAGIC A dashboard author should not need to know that path. They should be able to group by `region_name`.
# MAGIC
# MAGIC Metric Views support nested joins so that the semantic layer can expose the final business attribute.
# MAGIC
# MAGIC The docs support both `on` and `using` clauses for joins. This notebook uses `on` clauses because the source and dimension columns have different names in several places. If both sides share the same column name, `using` can be cleaner.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  Fact["Fact table<br/><b>credit_exposure_fact</b><br/><br/>branch_id<br/>exposure_amount"]
  Branch["Dimension<br/><b>dim_branch</b><br/><br/>branch_id<br/>branch_name<br/>region_id"]
  Region["Subdimension<br/><b>dim_region</b><br/><br/>region_id<br/>region_name"]

  Fact -->|"branch_id"| Branch
  Branch -->|"region_id"| Region

  style Fact fill:#E0F2FE,stroke:#0284C7,color:#111827
  style Branch fill:#DCFCE7,stroke:#16A34A,color:#111827
  style Region fill:#FEF3C7,stroke:#D97706,color:#111827
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.credit_risk_snowflake_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Snowflake schema Metric View for credit risk exposure by region.
source: {catalog}.{schema}.credit_exposure_fact
joins:
  - name: branch
    source: {catalog}.{schema}.dim_branch
    on: source.branch_id = branch.branch_id
    rely:
      at_most_one_match: true
    joins:
      - name: region
        source: {catalog}.{schema}.dim_region
        on: branch.region_id = region.region_id
        rely:
          at_most_one_match: true
  - name: product
    source: {catalog}.{schema}.dim_product
    on: source.product_id = product.product_id
    rely:
      at_most_one_match: true
fields:
  - name: branch_name
    expr: branch.branch_name
  - name: region_name
    expr: branch.region.region_name
  - name: product_line
    expr: product.product_line
measures:
  - name: exposure_amount
    expr: SUM(exposure_amount)
  - name: expected_credit_loss
    expr: SUM(expected_credit_loss)
$$
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region_name,
# MAGIC   product_line,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss
# MAGIC FROM credit_risk_snowflake_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY region_name, product_line

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Interpret the Result
# MAGIC
# MAGIC The query groups by `region_name`, but `region_name` does not live on the source fact table.
# MAGIC
# MAGIC It comes from a nested dimension:
# MAGIC
# MAGIC ```text
# MAGIC branch.region.region_name
# MAGIC ```
# MAGIC
# MAGIC That is the value of a snowflake join: normalized technical relationships become simple business fields.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. One-to-Many Join
# MAGIC
# MAGIC `one_to_many` lets the Metric View source act as a dimensional spine while a joined table contributes facts.
# MAGIC
# MAGIC In this example, one customer can have many loan applications.
# MAGIC
# MAGIC Use this pattern when:
# MAGIC
# MAGIC - the source table has one row per entity,
# MAGIC - a joined table has multiple related rows per entity,
# MAGIC - you want to aggregate facts from the joined branch,
# MAGIC - and you do **not** want to duplicate the source entity.
# MAGIC
# MAGIC Do not use a normal many-to-one join for this relationship. A customer with two applications is still one customer, not two customers.
# MAGIC
# MAGIC First, look at the naive join. The number of joined rows is larger than the number of customers because some customers have more than one application.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  subgraph C["customer_spine"]
    C1["C001<br/>Mass Affluent"]
    C2["C002<br/>Private Banking"]
  end

  subgraph A["loan_applications"]
    A1["A001<br/>C001<br/>Approved"]
    A2["A002<br/>C001<br/>Declined"]
    A3["A003<br/>C002<br/>Approved"]
  end

  C1 -->|"customer_id = C001"| A1
  C1 -->|"customer_id = C001"| A2
  C2 -->|"customer_id = C002"| A3

  style C fill:#E0F2FE,stroke:#0284C7,color:#111827
  style A fill:#FEF3C7,stroke:#D97706,color:#111827
  style C1 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style C2 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style A1 fill:#FFF7ED,stroke:#D97706,color:#111827
  style A2 fill:#FFF7ED,stroke:#D97706,color:#111827
  style A3 fill:#FFF7ED,stroke:#D97706,color:#111827
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT c.customer_id) AS customer_count,
# MAGIC   COUNT(*) AS rows_after_join
# MAGIC FROM customer_spine c
# MAGIC LEFT JOIN loan_applications a
# MAGIC   ON c.customer_id = a.customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC A Metric View with `cardinality: one_to_many` tells the engine that the joined branch contributes facts, but the source rows should remain the dimensional spine.

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.customer_application_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: One-to-many Metric View from customer spine to loan applications.
source: {catalog}.{schema}.customer_spine
joins:
  - name: region
    source: {catalog}.{schema}.dim_region
    on: source.region_id = region.region_id
    rely:
      at_most_one_match: true
  - name: applications
    source: {catalog}.{schema}.loan_applications
    on: applications.customer_id = source.customer_id
    cardinality: one_to_many
fields:
  - name: customer_segment
    expr: customer_segment
  - name: region_name
    expr: region.region_name
measures:
  - name: customer_count
    expr: COUNT(*)
  - name: application_count
    expr: COUNT(applications.application_id)
  - name: requested_amount
    expr: SUM(applications.requested_amount)
$$
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region_name,
# MAGIC   customer_segment,
# MAGIC   MEASURE(customer_count) AS customer_count,
# MAGIC   MEASURE(application_count) AS application_count,
# MAGIC   MEASURE(requested_amount) AS requested_amount
# MAGIC FROM customer_application_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY region_name, customer_segment

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Interpret the Result
# MAGIC
# MAGIC `customer_count` comes from the source `customer_spine`.
# MAGIC
# MAGIC `application_count` and `requested_amount` come from the one-to-many `applications` branch.
# MAGIC
# MAGIC The key test is that total customer count remains `5`, even though there are multiple application rows.
# MAGIC
# MAGIC This is exactly why cardinality matters: the Metric View can aggregate the application branch without turning one customer into many customers.

# COMMAND ----------

customer_count_check = spark.sql(
    """
SELECT SUM(customer_count) AS total_customers
FROM (
  SELECT
    customer_segment,
    MEASURE(customer_count) AS customer_count
  FROM customer_application_metrics
  GROUP BY ALL
)
"""
).collect()[0]["total_customers"]

require(customer_count_check == 5, f"Expected customer_count to remain 5, got {customer_count_check}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Nested One-to-Many Join
# MAGIC
# MAGIC Nested one-to-many joins let a Metric View measure facts that sit multiple levels below the source.
# MAGIC
# MAGIC Here customers have applications, and applications have decisions.
# MAGIC
# MAGIC Use this pattern when the business process itself is hierarchical:
# MAGIC
# MAGIC ```text
# MAGIC customer -> application -> decision
# MAGIC ```
# MAGIC
# MAGIC The important syntax detail is the dot path. A measure over the decision table must reference the nested join through its parent:
# MAGIC
# MAGIC ```text
# MAGIC applications.decisions.approved_amount
# MAGIC ```
# MAGIC
# MAGIC This path tells the Metric View exactly which branch the measure comes from.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  subgraph C["customer_spine"]
    C1["C001<br/>Mass Affluent"]
    C2["C002<br/>Private Banking"]
  end

  subgraph A["loan_applications"]
    A1["A001<br/>C001<br/>Approved"]
    A2["A002<br/>C001<br/>Declined"]
    A3["A003<br/>C002<br/>Approved"]
  end

  subgraph D["application_decisions"]
    D1["D001<br/>A001<br/>Auto Approved<br/>50,000"]
    D2["D002<br/>A002<br/>Manual Decline<br/>0"]
    D3["D003<br/>A003<br/>Committee Approved<br/>900,000"]
  end

  C1 -->|"customer_id = C001"| A1
  C1 -->|"customer_id = C001"| A2
  C2 -->|"customer_id = C002"| A3
  A1 -->|"application_id = A001"| D1
  A2 -->|"application_id = A002"| D2
  A3 -->|"application_id = A003"| D3

  style C fill:#E0F2FE,stroke:#0284C7,color:#111827
  style A fill:#FEF3C7,stroke:#D97706,color:#111827
  style D fill:#DCFCE7,stroke:#16A34A,color:#111827
  style C1 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style C2 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style A1 fill:#FFF7ED,stroke:#D97706,color:#111827
  style A2 fill:#FFF7ED,stroke:#D97706,color:#111827
  style A3 fill:#FFF7ED,stroke:#D97706,color:#111827
  style D1 fill:#F0FDF4,stroke:#16A34A,color:#111827
  style D2 fill:#F0FDF4,stroke:#16A34A,color:#111827
  style D3 fill:#F0FDF4,stroke:#16A34A,color:#111827
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.customer_application_decision_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Nested one-to-many Metric View for applications and decisions.
source: {catalog}.{schema}.customer_spine
joins:
  - name: applications
    source: {catalog}.{schema}.loan_applications
    on: applications.customer_id = source.customer_id
    cardinality: one_to_many
    joins:
      - name: decisions
        source: {catalog}.{schema}.application_decisions
        on: decisions.application_id = applications.application_id
        cardinality: one_to_many
fields:
  - name: customer_segment
    expr: customer_segment
measures:
  - name: customer_count
    expr: COUNT(*)
  - name: application_count
    expr: COUNT(DISTINCT applications.application_id)
  - name: decision_count
    expr: COUNT(applications.decisions.decision_id)
  - name: approved_amount
    expr: SUM(applications.decisions.approved_amount)
$$
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_segment,
# MAGIC   MEASURE(customer_count) AS customer_count,
# MAGIC   MEASURE(application_count) AS application_count,
# MAGIC   MEASURE(decision_count) AS decision_count,
# MAGIC   MEASURE(approved_amount) AS approved_amount
# MAGIC FROM customer_application_decision_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY customer_segment

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Interpret the Result
# MAGIC
# MAGIC This output combines three levels of logic:
# MAGIC
# MAGIC - `customer_count` from the customer spine
# MAGIC - `application_count` from the applications branch
# MAGIC - `decision_count` and `approved_amount` from the nested decisions branch
# MAGIC
# MAGIC Notice that the Metric View hides the multi-hop relationship from the query. Users group by `customer_segment` and ask for measures.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sibling One-to-Many Joins
# MAGIC
# MAGIC Sibling one-to-many joins let one Metric View measure independent fact branches without cross-multiplying rows.
# MAGIC
# MAGIC Here the customer spine joins to applications and service cases as independent sibling branches.
# MAGIC
# MAGIC Use this pattern when two fact sources share the same source entity but should not be joined to each other.
# MAGIC
# MAGIC A customer can have many applications and many service cases. If we joined applications to cases directly, we could accidentally multiply rows:
# MAGIC
# MAGIC ```text
# MAGIC applications x cases
# MAGIC ```
# MAGIC
# MAGIC Sibling one-to-many joins avoid that by aggregating each branch independently before blending results to the query grain.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  subgraph C["customer_spine"]
    C1["C001<br/>Mass Affluent"]
    C2["C002<br/>Private Banking"]
    C3["C003<br/>Mass Market"]
  end

  subgraph A["loan_applications"]
    A1["A001<br/>C001<br/>Approved"]
    A2["A002<br/>C001<br/>Declined"]
    A3["A003<br/>C002<br/>Approved"]
  end

  subgraph S["service_cases"]
    S1["S001<br/>C001<br/>Dispute"]
    S2["S002<br/>C001<br/>Limit Increase"]
    S3["S003<br/>C003<br/>Collections"]
  end

  C1 -->|"application branch"| A1
  C1 -->|"application branch"| A2
  C2 -->|"application branch"| A3
  C1 -->|"case branch"| S1
  C1 -->|"case branch"| S2
  C3 -->|"case branch"| S3

  Note["Applications and cases are siblings.<br/>They aggregate separately,<br/>so they do not multiply each other."]

  A2 -.-> Note
  S2 -.-> Note

  style C fill:#E0F2FE,stroke:#0284C7,color:#111827
  style A fill:#FEF3C7,stroke:#D97706,color:#111827
  style S fill:#FCE7F3,stroke:#DB2777,color:#111827
  style Note fill:#F3F4F6,stroke:#6B7280,color:#111827
  style C1 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style C2 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style C3 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style A1 fill:#FFF7ED,stroke:#D97706,color:#111827
  style A2 fill:#FFF7ED,stroke:#D97706,color:#111827
  style A3 fill:#FFF7ED,stroke:#D97706,color:#111827
  style S1 fill:#FDF2F8,stroke:#DB2777,color:#111827
  style S2 fill:#FDF2F8,stroke:#DB2777,color:#111827
  style S3 fill:#FDF2F8,stroke:#DB2777,color:#111827
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.customer_activity_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Sibling one-to-many Metric View for applications and service cases.
source: {catalog}.{schema}.customer_spine
joins:
  - name: applications
    source: {catalog}.{schema}.loan_applications
    on: applications.customer_id = source.customer_id
    cardinality: one_to_many
  - name: cases
    source: {catalog}.{schema}.service_cases
    on: cases.customer_id = source.customer_id
    cardinality: one_to_many
fields:
  - name: customer_segment
    expr: customer_segment
measures:
  - name: customer_count
    expr: COUNT(*)
  - name: application_count
    expr: COUNT(applications.application_id)
  - name: case_count
    expr: COUNT(cases.case_id)
  - name: applications_per_case
    expr: MEASURE(application_count) / NULLIF(MEASURE(case_count), 0)
$$
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_segment,
# MAGIC   MEASURE(customer_count) AS customer_count,
# MAGIC   MEASURE(application_count) AS application_count,
# MAGIC   MEASURE(case_count) AS case_count,
# MAGIC   MEASURE(applications_per_case) AS applications_per_case
# MAGIC FROM customer_activity_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY customer_segment

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Interpret the Result
# MAGIC
# MAGIC `application_count` and `case_count` come from different sibling branches.
# MAGIC
# MAGIC They can appear in the same result because the Metric View blends both branches back to the `customer_segment` query grain.
# MAGIC
# MAGIC This is useful for ratios such as `applications_per_case`, where both numerator and denominator come from independent fact sources.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Bridge Pattern for Multiple Fact Tables
# MAGIC
# MAGIC When two fact tables sit at different grains, a bridge can declare the valid shared dimension combinations.
# MAGIC
# MAGIC Here we bridge product and branch combinations so credit exposure and fraud loss can be aggregated independently without accidental fan-out.
# MAGIC
# MAGIC Use this pattern when there is no single fact table that should act as the semantic source.
# MAGIC
# MAGIC In this notebook:
# MAGIC
# MAGIC ```text
# MAGIC credit_exposure_fact = exposure metrics
# MAGIC fraud_event_fact = fraud metrics
# MAGIC ```
# MAGIC
# MAGIC Both share `product_id` and `branch_id`, but neither fact table should be treated as the universal source for the other.
# MAGIC
# MAGIC The bridge explicitly declares the valid product/branch combinations, then each fact table contributes measures independently.

# COMMAND ----------

render_mermaid(
    """
flowchart LR
  subgraph E["credit_exposure_fact"]
    direction TB
    E1["E001<br/>P_CARD + B_SG<br/>exposure 420k"]
    E2["E003<br/>P_LOAN + B_US<br/>exposure 310k"]
    E3["no exposure row<br/>for P_CARD + B_UK"]
  end

  subgraph B["bridge source"]
    direction TB
    B1["P_CARD + B_SG"]
    B2["P_LOAN + B_US"]
    B3["P_CARD + B_UK"]
  end

  subgraph F["fraud_event_fact"]
    direction TB
    F1["F001<br/>P_CARD + B_SG<br/>loss 21k"]
    F2["F002<br/>P_LOAN + B_US<br/>loss 54k"]
    F3["F004<br/>P_CARD + B_UK<br/>loss 68k"]
  end

  E1 --> B1 --> F1
  E2 --> B2 --> F2
  E3 -.-> B3 --> F3

  style B fill:#E0F2FE,stroke:#0284C7,color:#111827
  style E fill:#FEF3C7,stroke:#D97706,color:#111827
  style F fill:#FCE7F3,stroke:#DB2777,color:#111827
  style B1 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style B2 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style B3 fill:#F8FAFC,stroke:#0284C7,color:#111827
  style E1 fill:#FFF7ED,stroke:#D97706,color:#111827
  style E2 fill:#FFF7ED,stroke:#D97706,color:#111827
  style E3 fill:#F9FAFB,stroke:#D97706,stroke-dasharray: 5 5,color:#6B7280
  style F1 fill:#FDF2F8,stroke:#DB2777,color:#111827
  style F2 fill:#FDF2F8,stroke:#DB2777,color:#111827
  style F3 fill:#FDF2F8,stroke:#DB2777,color:#111827
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW {qualified_schema}.risk_bridge_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: Bridge Metric View combining exposure and fraud facts by product and branch.
source: |
  SELECT DISTINCT product_id, branch_id FROM {catalog}.{schema}.credit_exposure_fact
  UNION
  SELECT DISTINCT product_id, branch_id FROM {catalog}.{schema}.fraud_event_fact
joins:
  - name: product
    source: {catalog}.{schema}.dim_product
    on: source.product_id = product.product_id
    rely:
      at_most_one_match: true
  - name: branch
    source: {catalog}.{schema}.dim_branch
    on: source.branch_id = branch.branch_id
    rely:
      at_most_one_match: true
  - name: exposures
    source: {catalog}.{schema}.credit_exposure_fact
    on: source.product_id = exposures.product_id AND source.branch_id = exposures.branch_id
    cardinality: one_to_many
  - name: fraud
    source: {catalog}.{schema}.fraud_event_fact
    on: source.product_id = fraud.product_id AND source.branch_id = fraud.branch_id
    cardinality: one_to_many
fields:
  - name: product_line
    expr: product.product_line
  - name: branch_name
    expr: branch.branch_name
measures:
  - name: exposure_amount
    expr: SUM(exposures.exposure_amount)
  - name: expected_credit_loss
    expr: SUM(exposures.expected_credit_loss)
  - name: fraud_alert_count
    expr: SUM(fraud.alert_count)
  - name: confirmed_fraud_loss
    expr: SUM(fraud.confirmed_loss)
$$
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_line,
# MAGIC   branch_name,
# MAGIC   MEASURE(exposure_amount) AS exposure_amount,
# MAGIC   MEASURE(expected_credit_loss) AS expected_credit_loss,
# MAGIC   MEASURE(fraud_alert_count) AS fraud_alert_count,
# MAGIC   MEASURE(confirmed_fraud_loss) AS confirmed_fraud_loss
# MAGIC FROM risk_bridge_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY product_line, branch_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Interpret the Result
# MAGIC
# MAGIC Each row is a product/branch combination from the bridge.
# MAGIC
# MAGIC Exposure measures come from the exposure branch. Fraud measures come from the fraud branch.
# MAGIC
# MAGIC This avoids a common modeling trap: joining two fact tables directly and accidentally multiplying measures.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Restrictions and Design Guidance
# MAGIC
# MAGIC Important rules from the join documentation:
# MAGIC
# MAGIC - Fields can reference many-to-one joins, but not one-to-many joins.
# MAGIC - A single aggregation function must reference columns from one source.
# MAGIC - A one-to-many subtree cannot mix cardinalities.
# MAGIC - `rely.at_most_one_match: true` is useful only when the constraint really holds.
# MAGIC - Use a bridge when combining independent fact tables at different grains.
# MAGIC
# MAGIC This keeps query results predictable and prevents accidental double-counting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Putting It Together: Join Modeling Best Practices
# MAGIC
# MAGIC Joins are not just a YAML feature. They are part of the operating model for a trusted semantic layer.
# MAGIC
# MAGIC The goal is to make relationship logic explicit, owned, and reusable.
# MAGIC
# MAGIC ### 1. Start With the Business Question
# MAGIC
# MAGIC Before choosing a join pattern, ask:
# MAGIC
# MAGIC ```text
# MAGIC What is the entity I am measuring?
# MAGIC What is the grain of the source?
# MAGIC Which tables add descriptive context?
# MAGIC Which tables add additional facts?
# MAGIC ```
# MAGIC
# MAGIC For example:
# MAGIC
# MAGIC - If the source is an exposure fact, product and risk grade are dimensions.
# MAGIC - If the source is a customer spine, applications and service cases are one-to-many fact branches.
# MAGIC - If two fact tables share dimensions but neither should be the source spine, use a bridge.
# MAGIC
# MAGIC ### 2. Assign Clear Ownership
# MAGIC
# MAGIC Every production Metric View should have a clear owner.
# MAGIC
# MAGIC Ownership should usually align with the business meaning:
# MAGIC
# MAGIC | Metric View | Likely owner |
# MAGIC |---|---|
# MAGIC | `credit_risk_star_metrics` | Credit Risk Analytics |
# MAGIC | `customer_application_metrics` | Lending Analytics |
# MAGIC | `risk_bridge_metrics` | Enterprise Risk Data Product Team |
# MAGIC
# MAGIC The owner is responsible for:
# MAGIC
# MAGIC - source grain,
# MAGIC - join correctness,
# MAGIC - field and measure definitions,
# MAGIC - certification status,
# MAGIC - and change review.
# MAGIC
# MAGIC ### 3. Document the Grain
# MAGIC
# MAGIC Metric View comments should explain the source grain.
# MAGIC
# MAGIC Good examples:
# MAGIC
# MAGIC ```text
# MAGIC One row per credit exposure.
# MAGIC One row per customer.
# MAGIC One row per valid product and branch combination.
# MAGIC ```
# MAGIC
# MAGIC If the grain is unclear, the joins will eventually become unclear too.
# MAGIC
# MAGIC ### 4. Be Explicit About Cardinality
# MAGIC
# MAGIC Use the right cardinality:
# MAGIC
# MAGIC ```text
# MAGIC many_to_one  = dimension lookup
# MAGIC one_to_many = fact branch
# MAGIC ```
# MAGIC
# MAGIC Do not use `one_to_many` just because it works technically. Use it when the joined table really contributes facts below the source grain.
# MAGIC
# MAGIC ### 5. Use `rely` Carefully
# MAGIC
# MAGIC `rely.at_most_one_match: true` can help optimization, but it is a promise.
# MAGIC
# MAGIC Only use it when the relationship really has at most one matching row on the asserted side.
# MAGIC
# MAGIC If that promise is wrong, measures can be wrong.
# MAGIC
# MAGIC ### 6. Validate Fanout
# MAGIC
# MAGIC Add small tests for production Metric Views:
# MAGIC
# MAGIC ```sql
# MAGIC -- Does a many-to-one dimension accidentally fan out?
# MAGIC SELECT source_key, COUNT(*) AS matches
# MAGIC FROM fact_table f
# MAGIC JOIN dimension_table d
# MAGIC   ON f.dimension_id = d.dimension_id
# MAGIC GROUP BY source_key
# MAGIC HAVING COUNT(*) > 1;
# MAGIC ```
# MAGIC
# MAGIC For one-to-many patterns, validate that source-level measures stay stable.
# MAGIC
# MAGIC In this notebook, `customer_count` remains 5 even after joining to applications.
# MAGIC
# MAGIC ### 7. Split Metric Views When Ownership or Grain Changes
# MAGIC
# MAGIC A single giant Metric View is rarely the best production design.
# MAGIC
# MAGIC Split Metric Views when:
# MAGIC
# MAGIC - ownership differs,
# MAGIC - source grain differs,
# MAGIC - business meaning differs,
# MAGIC - certification lifecycle differs,
# MAGIC - or materialization needs differ.
# MAGIC
# MAGIC A good production pattern is:
# MAGIC
# MAGIC ```text
# MAGIC one Metric View per certified business subject area
# MAGIC one composed / executive Metric View for cross-area reporting
# MAGIC ```
# MAGIC
# MAGIC ### 8. Make It Easy for Agents
# MAGIC
# MAGIC Agents should not infer joins from raw table names.
# MAGIC
# MAGIC They should discover a certified Metric View and query business fields and measures.
# MAGIC
# MAGIC That is the point of modeling joins here:
# MAGIC
# MAGIC ```text
# MAGIC source relationships become trusted semantic context
# MAGIC ```

# COMMAND ----------

created_metric_views = spark.sql(
    """
SELECT COUNT(*) AS metric_view_count
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND table_type = 'METRIC_VIEW'
  AND table_name IN (
    'credit_risk_star_metrics',
    'credit_risk_snowflake_metrics',
    'customer_application_metrics',
    'customer_application_decision_metrics',
    'customer_activity_metrics',
    'risk_bridge_metrics'
  )
"""
).collect()[0]["metric_view_count"]

require(created_metric_views == 6, f"Expected 6 Metric Views, found {created_metric_views}")

print("Metric View joins deep dive completed successfully.")
