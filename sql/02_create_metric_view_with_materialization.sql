-- Creates the main tutorial Metric View with LOD expressions, window semantics,
-- agent metadata, and materialization.
-- Requires serverless compute for materialization.

USE CATALOG main;
USE SCHEMA metric_views_lod_demo;

CREATE OR REPLACE VIEW finance_metric_view
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: |-
  Finance semantic model demonstrating LOD, window measures, agent metadata,
  and materialization for Databricks Metric Views.
source: main.metric_views_lod_demo.finance_semantic_base

fields:
  - name: event_date
    expr: event_date
    display_name: Event Date
    format:
      type: date
      date_format: year_month_day

  - name: fiscal_month
    expr: fiscal_month
    display_name: Fiscal Month
    format:
      type: date
      date_format: year_month_day
    synonyms:
      - month
      - accounting month

  - name: fiscal_year_start
    expr: fiscal_year_start
    display_name: Fiscal Year Start
    format:
      type: date
      date_format: year_month_day

  - name: fiscal_quarter
    expr: fiscal_quarter
    display_name: Fiscal Quarter
    synonyms:
      - quarter
      - accounting quarter

  - name: fiscal_year
    expr: fiscal_year
    display_name: Fiscal Year
    synonyms:
      - year
      - accounting year

  - name: region
    expr: region
    display_name: Region

  - name: entity_name
    expr: entity_name
    display_name: Entity
    synonyms:
      - legal entity
      - company

  - name: product_family
    expr: product_family
    display_name: Product Family
    synonyms:
      - product group

  - name: product_name
    expr: product_name
    display_name: Product

  - name: segment_name
    expr: segment_name
    display_name: Customer Segment
    synonyms:
      - segment
      - customer type

  - name: statement_section
    expr: statement_section
    display_name: Statement Section

  - name: account_category
    expr: account_category
    display_name: Account Category
    synonyms:
      - account group
      - financial category

  - name: account_name
    expr: account_name
    display_name: Account

  - name: scenario_name
    expr: scenario_name
    display_name: Scenario

  - name: global_revenue_lod
    expr: SUM(CASE WHEN source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue' THEN amount ELSE 0 END) OVER ()
    comment: Fixed LOD field for total actual revenue across the full source.
    display_name: Global Revenue LOD
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: account_category_revenue_lod
    expr: SUM(CASE WHEN source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue' THEN amount ELSE 0 END) OVER (PARTITION BY account_category)
    comment: Fixed LOD field for actual revenue by account category.
    display_name: Account Category Revenue LOD
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: product_family_revenue_lod
    expr: SUM(CASE WHEN source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue' THEN amount ELSE 0 END) OVER (PARTITION BY product_family)
    comment: Fixed LOD field for actual revenue by product family.
    display_name: Product Family Revenue LOD
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

measures:
  - name: actual_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    comment: Actual revenue from journal-line transactions.
    display_name: Actual Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - sales
      - turnover
      - net revenue

  - name: actual_cogs
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'COGS')
    display_name: Actual COGS
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: actual_opex
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Opex')
    display_name: Actual Opex
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: actual_expense
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND normal_balance = 'Expense')
    display_name: Actual Expense
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: budget_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'TARGET' AND scenario_id = 'BUDGET' AND account_category = 'Revenue')
    display_name: Budget Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: gross_profit
    expr: MEASURE(actual_revenue) - MEASURE(actual_cogs)
    display_name: Gross Profit
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: ebitda
    expr: MEASURE(actual_revenue) - MEASURE(actual_cogs) - MEASURE(actual_opex)
    display_name: EBITDA
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - operating earnings
      - operating profit

  - name: ebitda_margin_pct
    expr: MEASURE(ebitda) / NULLIF(MEASURE(actual_revenue), 0)
    display_name: EBITDA Margin %
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2
    synonyms:
      - operating margin
      - profitability

  - name: revenue_variance
    expr: MEASURE(actual_revenue) - MEASURE(budget_revenue)
    display_name: Revenue Variance
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: revenue_variance_pct
    expr: MEASURE(revenue_variance) / NULLIF(MEASURE(budget_revenue), 0)
    display_name: Revenue Variance %
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: transaction_count
    expr: COUNT(DISTINCT source_record_id) FILTER (WHERE source_grain = 'GL')
    display_name: Transaction Count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      abbreviation: compact

  - name: pct_of_global_revenue_fixed_lod
    expr: MEASURE(actual_revenue) / NULLIF(ANY_VALUE(global_revenue_lod), 0)
    comment: Fixed LOD percentage using a global denominator.
    display_name: Percent of Global Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: pct_of_account_category_revenue_fixed_lod
    expr: MEASURE(actual_revenue) / NULLIF(ANY_VALUE(account_category_revenue_lod), 0)
    comment: Fixed LOD percentage using account-category denominator.
    display_name: Percent of Account Category Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: pct_of_product_family_revenue_fixed_lod
    expr: MEASURE(actual_revenue) / NULLIF(ANY_VALUE(product_family_revenue_lod), 0)
    comment: Fixed LOD percentage using product-family denominator.
    display_name: Percent of Product Family Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: all_account_categories_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: account_category
        range: all
        semiadditive: last
    comment: Coarser LOD denominator that excludes account category from the query grain.
    display_name: All Account Categories Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: pct_of_visible_total_revenue_coarser_lod
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(all_account_categories_revenue), 0)
    comment: Coarser LOD percentage that can remain filter-aware.
    display_name: Percent of Visible Total Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: region_revenue_excluding_entity
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: entity_name
        range: all
        semiadditive: last
    comment: Coarser LOD denominator for entity-level contribution within region.
    display_name: Region Revenue Excluding Entity
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: pct_of_region_revenue
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(region_revenue_excluding_entity), 0)
    display_name: Percent of Region Revenue
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: revenue_excluding_entity_and_product_family
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: entity_name
        range: all
        semiadditive: last
      - order: product_family
        range: all
        semiadditive: last
    comment: Coarser LOD denominator that excludes multiple fields from the query grain.
    display_name: Revenue Excluding Entity and Product Family
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: pct_of_entity_product_visible_total
    expr: MEASURE(actual_revenue) / NULLIF(MEASURE(revenue_excluding_entity_and_product_family), 0)
    comment: Demonstrates excluding multiple fields with range all.
    display_name: Percent of Entity/Product Visible Total
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: current_month_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: current
        semiadditive: last
    display_name: Current Month Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: ytd_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: cumulative
        semiadditive: last
      - order: fiscal_year_start
        range: current
        semiadditive: last
    display_name: YTD Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - year to date sales
      - ytd sales

  - name: running_total_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: cumulative
        semiadditive: last
    comment: Cumulative revenue across the full available time range.
    display_name: Running Total Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: rolling_12_month_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: trailing 12 month inclusive
        semiadditive: last
    display_name: Rolling 12 Month Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - r12 revenue
      - trailing twelve month revenue

  - name: trailing_3_month_revenue_exclusive
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: trailing 3 month exclusive
        semiadditive: last
    comment: Trailing 3 months excluding the anchor month.
    display_name: Trailing 3 Month Revenue Exclusive
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: trailing_3_month_revenue_inclusive
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: trailing 3 month inclusive
        semiadditive: last
    comment: Trailing 3 months including the anchor month.
    display_name: Trailing 3 Month Revenue Inclusive
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: next_month_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: leading 1 month
        semiadditive: first
    comment: Leading window example. Returns the next month's revenue when grouped by fiscal month.
    display_name: Next Month Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: prior_year_revenue
    expr: SUM(amount) FILTER (WHERE source_grain = 'GL' AND scenario_id = 'ACTUAL' AND account_category = 'Revenue')
    window:
      - order: fiscal_month
        range: current
        semiadditive: last
        offset: -12 month
    display_name: Prior Year Revenue
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: yoy_revenue_growth
    expr: MEASURE(current_month_revenue) - MEASURE(prior_year_revenue)
    display_name: YoY Revenue Growth
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: yoy_revenue_growth_pct
    expr: MEASURE(yoy_revenue_growth) / NULLIF(MEASURE(prior_year_revenue), 0)
    display_name: YoY Revenue Growth %
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 2

  - name: balance_additive_snapshot
    expr: SUM(balance_amount) FILTER (WHERE source_grain = 'BALANCE')
    display_name: Balance Additive Snapshot
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact

  - name: month_end_balance
    expr: SUM(balance_amount) FILTER (WHERE source_grain = 'BALANCE')
    window:
      - order: fiscal_month
        range: current
        semiadditive: last
    comment: Semiadditive month-end balance. Sums across entities/products, but not across months.
    display_name: Month-End Balance
    format:
      type: currency
      currency_code: SGD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
    synonyms:
      - closing balance
      - ending balance

materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: semantic_base_snapshot
      type: unaggregated
    - name: exec_month_region_category
      type: aggregated
      dimensions:
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
        - fiscal_month
        - entity_name
        - account_category
      measures:
        - balance_additive_snapshot
$$;
