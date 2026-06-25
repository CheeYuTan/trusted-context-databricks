# Modeling Business Semantics With Databricks Metric Views

## Working Title

**Modeling Business Semantics with Databricks Metric Views: LOD, Windows, Metadata, and Materialization**

## Goal

Build a tutorial-style Databricks notebook and companion assets that show how Metric Views have evolved from reusable metric definitions into a richer semantic modeling layer for governed analytics.

The tutorial focuses on four new or improved areas:

- Level of detail (LOD) expressions
- Advanced window semantics and composability
- Agent metadata for AI/BI tools
- Materialization and aggregate-aware query rewrite

## Primary References

- [Advanced techniques for metric views](https://docs.databricks.com/aws/en/business-semantics/metric-views/advanced-techniques)
- [Use level of detail expressions in metric views](https://docs.databricks.com/aws/en/business-semantics/metric-views/level-of-detail)
- [Materialization for metric views](https://docs.databricks.com/aws/en/business-semantics/metric-views/materialization)

## Deliverables

- A tutorial-style Databricks notebook
- SQL setup scripts for synthetic finance data
- Metric View SQL/YAML definitions
- Dashboard-ready SQL queries
- Diagram source files
- A blog outline and screenshot checklist
- A documentation coverage matrix that maps each referenced Databricks scenario to a concrete repo asset
- A GitHub repository under `CheeYuTan`

## Repository Name

Suggested repository name:

`metric-views-lod-finance-semantics`

Reason: it is descriptive, search-friendly, and clearly signals the core tutorial focus: Databricks Metric Views, level of detail modeling, finance semantics, and governed analytics.

## Tutorial Story

The previous blog showed that Metric Views can model complex financial metrics with joins, nested measures, rolling windows, YTD calculations, and semiadditive balances.

This sequel should answer a different question:

> How do Metric Views help model business semantics at the right level of detail, make those metrics AI-ready, and accelerate dashboard queries without changing the user-facing SQL?

## Scenario

Use a finance analytics model where business users need to analyze profit and balances across multiple grains:

- Journal transaction grain
- Monthly budget / forecast grain
- Month-end balance grain
- Account hierarchy
- Product hierarchy
- Legal entity and region hierarchy
- Customer segment hierarchy
- Fiscal calendar hierarchy

This scenario is intentionally grain-aware so that the tutorial can show why calculation level of detail matters.

## Data Model

Create the following tables:

- `fact_gl_transactions`: journal-line transaction grain
- `fact_monthly_targets`: monthly budget and forecast grain
- `fact_month_end_balances`: semiadditive balance grain
- `dim_account`: account, subcategory, statement section
- `dim_product`: product, product family, business unit
- `dim_entity`: entity, country, region
- `dim_customer_segment`: customer segment and customer type
- `dim_calendar`: date, fiscal month, fiscal quarter, fiscal year
- `dim_scenario`: actual, budget, forecast

## Diagrams

Include diagrams in the notebook and repo:

- ERD / snowflake schema
- Grain map showing transaction grain, target grain, and balance grain
- Semantic layer architecture showing tables -> Metric View -> dashboard / Genie / BI tools
- Materialization flow showing Metric View -> Lakeflow materialization pipeline -> aggregate-aware query rewrite

## Metric View Modeling

Create one main YAML 1.1 Metric View that includes:

- Fields across multiple grains: transaction date, fiscal month, fiscal quarter, fiscal year, account, account category, product, product family, entity, region, segment, scenario
- Atomic measures: revenue, expenses, assets, liabilities, transaction count
- Composed measures: gross profit, EBITDA, EBITDA margin %, variance %, contribution %
- Conditional measures using `FILTER`
- Safe ratios using `MEASURE()` composability

## LOD Showcase

Demonstrate both LOD patterns from the Databricks LOD documentation.

### Fixed LOD

Use SQL window functions in field expressions and aggregate the fixed field with `ANY_VALUE()` inside measures.

Examples:

- Percent of total revenue by account category
- Entity revenue compared with global revenue
- Product revenue compared with product-family revenue
- Fixed global denominator behavior under query-time filters

Lesson:

Fixed LOD calculates at a predefined grain, regardless of query groupings.
Fixed LOD fields are computed before query-time filters. If a filter should affect the fixed LOD calculation, the filter condition must be encoded inside the LOD field expression with `CASE` or `FILTER`.

### Coarser LOD

Use window measures with `range: all` to exclude selected fields from the calculation grain.

Examples:

- Percent of total revenue while querying by account category
- Percent of region revenue while querying by entity
- Percent of product-family revenue while querying by product
- Percent of visible total while excluding multiple fields, such as entity and product family

Lesson:

Coarser LOD can adapt to query-time filters while calculating at a broader grain than the visible dashboard grouping.
To exclude multiple fields, define multiple `window` entries with `range: all`.

## Window Semantics Showcase

Demonstrate advanced window and composability patterns:

- Current month revenue
- Running total revenue
- YTD revenue using multiple window specifications
- Rolling 12-month revenue
- Prior-year revenue using `offset: -12 month`
- YoY growth and YoY growth %
- Inclusive vs exclusive trailing windows
- Leading window example with next-month revenue
- Semiadditive month-end balances with `range: current` and `semiadditive: last`
- Cross-Metric-View composability using a derived executive Metric View

Important modeling note:

Date hierarchy fields must be defined from the order field, not directly from the raw source column. This preserves correct behavior when grouping window measures by month, quarter, or year.

## Agent Metadata Showcase

Add semantic metadata to fields and measures:

- `display_name`
- `comment`
- `format`
- `synonyms`

Examples:

- `net_revenue`: display name `Net Revenue`, synonyms `sales`, `turnover`
- `ebitda_margin_pct`: display name `EBITDA Margin %`, percentage formatting, synonyms `operating margin`, `profitability`
- `month_end_balance`: synonyms `closing balance`, `ending balance`
- Currency metrics formatted as SGD or USD

Lesson:

Agent metadata improves dashboard labels and helps natural language tools such as Genie interpret business terminology.

## Materialization Showcase

Add a `materialization` block to the Metric View:

- One `unaggregated` materialization for expensive joins and source preparation
- Multiple `aggregated` materializations for common dashboard query shapes
- `schedule: every 6 hours`
- `mode: relaxed`

Example materializations:

- Executive summary: fiscal month, region, account category
- Drilldown: fiscal month, region, entity, product family, account category
- Balance page: fiscal month, entity, account category

Show:

- Exact match
- Rollup match
- Unaggregated fallback
- Additive vs non-additive measure behavior
- `EXPLAIN EXTENDED` for materialization verification
- `REFRESH MATERIALIZED VIEW` for manual refresh
- `DESCRIBE EXTENDED` for refresh information

Call out restrictions:

- Serverless compute is required
- Databricks Runtime 17.3 or above is required
- Feature is in Public Preview
- RLS, column masking, and ABAC policies are not supported with materialization
- Invoker-dependent expressions are not supported
- Metric views with one-to-many joins only support exact match for materialization

## Dashboard Showcase

Prepare dashboard-ready queries for:

- Executive overview
- LOD drilldown
- Window semantics
- Semiadditive balances
- Materialization verification
- Genie prompt examples

If workspace permissions allow dashboard creation, create a Databricks AI/BI dashboard. Otherwise, structure notebook outputs so the charts can be recreated easily.

## Validation Checklist

Verify:

- Demo tables are created successfully
- Metric View compiles
- LOD queries return expected denominators
- Fixed LOD and coarser LOD behavior are visibly different
- Window metrics return sensible values
- YTD resets by fiscal year
- Prior-year offset works
- Semiadditive balances do not sum incorrectly across months
- Agent metadata is present in the YAML
- Materialization definition is accepted
- Materialization refresh or status can be inspected
- Dashboard queries run against the Metric View

## Final Blog Arc

1. Recap the previous post and explain what has changed.
2. Introduce calculation grain as the central modeling problem.
3. Build the finance data model.
4. Define the Metric View semantic layer.
5. Demonstrate fixed and coarser LOD expressions.
6. Demonstrate window semantics and composability.
7. Add agent metadata for AI/BI usage.
8. Add materialization for dashboard acceleration.
9. Show dashboard-ready queries and Genie-style questions.
10. Conclude with the shift from metric definitions to governed business semantics.
