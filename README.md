# Metric Views Deep Dive

Companion repo for a Databricks Metric Views deep-dive blog series.

Repository link:

https://github.com/CheeYuTan/metric-views-deep-dive

## What Is Included

### Blog Drafts

- `blogs/01_materialization_deep_dive.md`
- `blogs/02_level_of_detail_deep_dive.txt`
- `blogs/03_window_semantics_deep_dive.txt`

### Modular Blog Sections

- `blogs/sections/materialization_architecture.md`
- `blogs/sections/automatic_query_rewrite.md`
- `blogs/sections/materialization_closing.md`

### Databricks Notebooks

General tutorial notebooks:

- `notebooks/01_generate_synthetic_finance_data.py`
- `notebooks/02_design_metric_view_semantic_layer.py`
- `notebooks/03_query_lod_windows_and_materialization.py`
- `notebooks/04_dashboard_story.py`

Deep-dive notebooks:

- `notebooks/deep_dives/00_materialization_base_tables.py`
- `notebooks/deep_dives/01_materialization_deep_dive.py`
- `notebooks/deep_dives/02_level_of_detail_deep_dive.py`
- `notebooks/deep_dives/03_window_semantics_deep_dive.py`

### Images

- `assets/query_profiles/`

These are Query Profile screenshots used in the materialization article.

## Recommended Run Order

For the materialization deep dive:

1. Import and run `notebooks/deep_dives/00_materialization_base_tables.py`
2. Import and run `notebooks/deep_dives/01_materialization_deep_dive.py`

For the broader Metric Views tutorial:

1. Import and run `notebooks/01_generate_synthetic_finance_data.py`
2. Import and run `notebooks/02_design_metric_view_semantic_layer.py`
3. Import and run `notebooks/03_query_lod_windows_and_materialization.py`
4. Import and run `notebooks/04_dashboard_story.py`

## Latest Validated Materialization Run

The materialization notebooks were validated on the Lakemeter Databricks workspace:

https://fe-vm-lakemeter.cloud.databricks.com/?o=335310294452632#job/352265414516911/run/30223876085768
