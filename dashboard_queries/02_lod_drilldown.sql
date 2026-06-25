-- Dashboard page: Level of Detail Drilldown
-- Use this for a region -> entity -> product family -> account category view.

SELECT
  region,
  entity_name,
  product_family,
  account_category,
  MEASURE(actual_revenue) AS actual_revenue,
  MEASURE(pct_of_global_revenue_fixed_lod) AS pct_of_global_revenue,
  MEASURE(pct_of_product_family_revenue_fixed_lod) AS pct_of_product_family_revenue,
  MEASURE(pct_of_region_revenue) AS pct_of_region_revenue,
  MEASURE(pct_of_visible_total_revenue_coarser_lod) AS pct_of_visible_total_revenue,
  MEASURE(pct_of_entity_product_visible_total) AS pct_of_entity_product_visible_total
FROM main.metric_views_lod_demo.finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL
ORDER BY region, entity_name, product_family, account_category;
