-- Demonstrates composability across Metric Views by using finance_metric_view
-- as the source for a second, executive-oriented Metric View.

USE CATALOG main;
USE SCHEMA metric_views_lod_demo;

CREATE OR REPLACE VIEW finance_exec_metric_view
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Derived executive Metric View that demonstrates composability across Metric Views.
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

measures:
  - name: revenue_per_transaction
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(transaction_count), 0)
    comment: Derived from measures in the source Metric View.
    display_name: Revenue per Transaction
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2

  - name: executive_score
    expr: MEASURE(ebitda_margin_pct) + MEASURE(revenue_variance_pct)
    comment: Toy composite score to show cross-Metric-View measure composition.
    display_name: Executive Score
    format:
      type: number
      decimal_places:
        type: exact
        places: 4
$$;

SELECT
  fiscal_month,
  region,
  MEASURE(revenue_per_transaction) AS revenue_per_transaction,
  MEASURE(executive_score) AS executive_score
FROM finance_exec_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_month, region;
