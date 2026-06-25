-- Dashboard page: Window Semantics
-- Shows monthly, YTD, rolling-12, prior-year, and YoY growth side by side.

SELECT
  fiscal_month,
  region,
  MEASURE(current_month_revenue) AS current_month_revenue,
  MEASURE(running_total_revenue) AS running_total_revenue,
  MEASURE(ytd_revenue) AS ytd_revenue,
  MEASURE(rolling_12_month_revenue) AS rolling_12_month_revenue,
  MEASURE(trailing_3_month_revenue_exclusive) AS trailing_3_exclusive,
  MEASURE(trailing_3_month_revenue_inclusive) AS trailing_3_inclusive,
  MEASURE(next_month_revenue) AS next_month_revenue,
  MEASURE(prior_year_revenue) AS prior_year_revenue,
  MEASURE(yoy_revenue_growth) AS yoy_revenue_growth,
  MEASURE(yoy_revenue_growth_pct) AS yoy_revenue_growth_pct
FROM main.metric_views_lod_demo.finance_metric_view
WHERE fiscal_month >= DATE'2025-01-01'
GROUP BY ALL
ORDER BY fiscal_month, region;
