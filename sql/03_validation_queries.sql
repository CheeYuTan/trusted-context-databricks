-- Validation queries for the Metric Views LOD finance semantics demo.

USE CATALOG main;
USE SCHEMA metric_views_lod_demo;

-- Inspect the Metric View definition and materialization status.
DESCRIBE EXTENDED finance_metric_view;

-- Fixed LOD: global and account-category denominators.
SELECT
  region,
  account_category,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(pct_of_global_revenue_fixed_lod) AS pct_of_global_revenue,
  MEASURE(pct_of_account_category_revenue_fixed_lod) AS pct_of_account_category_revenue
FROM finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY region, account_category;

-- Fixed LOD filtering behavior: the fixed global denominator is computed before
-- this query-time region filter is applied.
SELECT
  region,
  product_family,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(pct_of_global_revenue_fixed_lod) AS pct_of_global_revenue,
  MEASURE(pct_of_product_family_revenue_fixed_lod) AS pct_of_product_family_revenue,
  MEASURE(pct_of_apj_revenue_fixed_lod) AS pct_of_apj_revenue
FROM finance_metric_view
WHERE fiscal_year = 2025
  AND region = 'APJ'
GROUP BY ALL
ORDER BY product_family;

-- Coarser LOD: entity contribution inside region.
SELECT
  region,
  entity_name,
  MEASURE(actual_revenue) AS entity_revenue,
  MEASURE(region_revenue_excluding_entity) AS region_revenue,
  MEASURE(pct_of_region_revenue) AS pct_of_region_revenue
FROM finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY region, entity_revenue DESC;

-- Coarser LOD excluding multiple fields with range: all.
SELECT
  region,
  entity_name,
  product_family,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(revenue_excluding_entity_and_product_family) AS broader_revenue,
  MEASURE(pct_of_entity_product_visible_total) AS pct_of_entity_product_visible_total
FROM finance_metric_view
WHERE fiscal_year = 2025
  AND region = 'APJ'
GROUP BY ALL
ORDER BY entity_name, product_family;

-- Window semantics: current, YTD, rolling 12, prior-year, and YoY growth.
SELECT
  fiscal_month,
  region,
  MEASURE(current_month_revenue) AS current_month_revenue,
  MEASURE(running_total_revenue) AS running_total_revenue,
  MEASURE(ytd_revenue) AS ytd_revenue,
  MEASURE(rolling_12_month_revenue) AS rolling_12_month_revenue,
  MEASURE(next_month_revenue) AS next_month_revenue,
  MEASURE(prior_year_revenue) AS prior_year_revenue,
  MEASURE(yoy_revenue_growth_pct) AS yoy_revenue_growth_pct
FROM finance_metric_view
WHERE fiscal_month >= DATE'2025-01-01'
GROUP BY ALL
ORDER BY fiscal_month, region;

-- Inclusive vs exclusive trailing windows.
SELECT
  fiscal_month,
  region,
  MEASURE(current_month_revenue) AS current_month_revenue,
  MEASURE(trailing_3_month_revenue_exclusive) AS trailing_3_exclusive,
  MEASURE(trailing_3_month_revenue_inclusive) AS trailing_3_inclusive
FROM finance_metric_view
WHERE fiscal_month BETWEEN DATE'2025-01-01' AND DATE'2025-06-01'
GROUP BY ALL
ORDER BY fiscal_month, region;

-- Semiadditive validation: balances should not be summed across months.
SELECT
  fiscal_quarter,
  entity_name,
  account_category,
  MEASURE(month_end_balance) AS month_end_balance
FROM finance_metric_view
WHERE statement_section = 'Balance Sheet'
  AND fiscal_year = 2025
GROUP BY ALL
ORDER BY fiscal_quarter, entity_name, account_category;

-- Materialization verification. Look for materialization names in the plan.
-- This uses the separate materialized variant. The base finance_metric_view
-- is intentionally non-materialized.
EXPLAIN EXTENDED
SELECT
  fiscal_year,
  fiscal_month,
  region,
  account_category,
  MEASURE(actual_revenue) AS actual_revenue
FROM finance_metric_view_materialized
WHERE fiscal_year = 2025
GROUP BY ALL;

-- Rollup match candidate: fewer dimensions than the materialization, additive measure.
EXPLAIN EXTENDED
SELECT
  fiscal_year,
  fiscal_month,
  region,
  MEASURE(actual_revenue) AS actual_revenue
FROM finance_metric_view_materialized
WHERE fiscal_year = 2025
GROUP BY ALL;

-- Unaggregated/source fallback candidate: non-additive COUNT(DISTINCT) measure.
EXPLAIN EXTENDED
SELECT
  fiscal_year,
  fiscal_month,
  region,
  MEASURE(transaction_count) AS transaction_count
FROM finance_metric_view_materialized
WHERE fiscal_year = 2025
GROUP BY ALL;
