# Building Trusted Context in Databricks Part 3: Deep Dive Into Metric View Joins

This file is the modular index for Blog 3.

## Section Order

1. `blogs/part3_sections/00_article_header.md`
2. `blogs/part3_sections/01_why_joins_belong_in_metric_views.md`
3. `blogs/part3_sections/02_star_schema_joins.md`
4. `blogs/part3_sections/03_snowflake_schema_joins.md`
5. `blogs/part3_sections/04_join_cardinality_and_rely.md`
6. `blogs/part3_sections/05_one_to_many_joins.md`
7. `blogs/part3_sections/06_nested_sibling_and_bridge_patterns.md`
8. `blogs/part3_sections/07_closing.md`

## Companion Notebook

`notebooks/part3_metric_view_joins/00_metric_view_joins_deep_dive.py`

## Screenshot Placeholders

- `assets/part3_metric_view_joins/figure_01_join_patterns.png`
- `assets/part3_metric_view_joins/figure_02_join_cardinality.png`
- `assets/part3_metric_view_joins/figure_03_star_schema_zoom.png`
- `assets/part3_metric_view_joins/figure_04_snowflake_schema_zoom.png`
- `assets/part3_metric_view_joins/figure_05_many_to_one_rely_zoom.png`
- `assets/part3_metric_view_joins/figure_06_one_to_many_zoom.png`
- `assets/part3_metric_view_joins/figure_07_nested_one_to_many_zoom.png`
- `assets/part3_metric_view_joins/figure_08_sibling_one_to_many_zoom.png`
- `assets/part3_metric_view_joins/figure_09_bridge_pattern_zoom.png`

Notebook outputs to capture:

- Star schema query output
- Snowflake schema query output
- Naive one-to-many fanout output
- One-to-many Metric View output
- Nested, sibling, and bridge Metric View outputs
