-- Creates a materialized variant of finance_metric_view for performance comparison.
-- The base finance_metric_view remains non-materialized so the semantic design is
-- taught separately from the optimization strategy.

USE CATALOG main;
USE SCHEMA metric_views_lod_demo;

CREATE OR REPLACE VIEW finance_metric_view_materialized
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Materialized variant of finance_metric_view for query acceleration demos.
source: main.metric_views_lod_demo.finance_metric_view

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

  - name: product_family
    expr: product_family
    display_name: Product Family

  - name: account_category
    expr: account_category
    display_name: Account Category

  - name: statement_section
    expr: statement_section
    display_name: Statement Section

  - name: fiscal_quarter
    expr: fiscal_quarter
    display_name: Fiscal Quarter

measures:
  - name: actual_revenue
    expr: MEASURE(actual_revenue)
    display_name: Actual Revenue
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact

  - name: actual_expense
    expr: MEASURE(actual_expense)
    display_name: Actual Expense
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact

  - name: actual_cogs
    expr: MEASURE(actual_cogs)
    display_name: Actual COGS
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact

  - name: actual_opex
    expr: MEASURE(actual_opex)
    display_name: Actual Opex
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact

  - name: ebitda
    expr: MEASURE(ebitda)
    display_name: EBITDA
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact

  - name: transaction_count
    expr: MEASURE(transaction_count)
    display_name: Transaction Count
    format:
      type: number
      abbreviation: compact

  - name: month_end_balance
    expr: MEASURE(month_end_balance)
    display_name: Month-End Balance
    format:
      type: currency
      currency_code: SGD
      abbreviation: compact

materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: semantic_snapshot
      type: unaggregated

    - name: exec_month_region_category
      type: aggregated
      dimensions:
        - fiscal_year
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
        - fiscal_year
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
        - fiscal_year
        - fiscal_month
        - entity_name
        - account_category
      measures:
        - month_end_balance
$$;
