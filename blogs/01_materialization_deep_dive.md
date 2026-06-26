Title:
Databricks Metric Views Deep Dive Part 1: Materialization Without Breaking the Semantic Layer

Subtitle:
How Metric View materialization accelerates governed metrics while keeping user-facing SQL unchanged.

Audience:
Analytics engineers, BI developers, and data platform teams who want governed metrics that can also support dashboard-scale workloads.

Core takeaway:
Metric View materialization separates what a metric means from how it is physically accelerated. Users keep querying the Metric View with `MEASURE()`, while Databricks can route matching queries to precomputed materializations.

## Opening: Genie Still Needs Business Context

At DAIS 2026, Databricks introduced Genie Ontology as part of the broader Genie family. The Databricks announcement describes Genie Ontology as an automatic context layer: a living graph that extracts knowledge from tables, queries, dashboards, pipelines, connected apps, and other business artifacts so Genie knows where to look, what to trust, and how to answer with business context. It includes metric definitions, business terms, unique calculations, and relationships between concepts, metrics, tables, and teams. [Databricks: Introducing Genie One, Genie Agents, and Genie Ontology](https://www.databricks.com/blog/introducing-genie-one-genie-ontology-and-genie-agents)

That framing is important because it makes one thing very clear: AI does not create business context out of nowhere.

An AI system can only reason over the context it can discover, rank, and trust:

- Which fields are safe to group by?
- What does “revenue” actually mean?
- Which filters define actuals, budget, or forecast?
- Which joins connect the fact table to product, entity, customer segment, and calendar dimensions?
- Which metrics can be rolled up safely?
- Which metrics require special handling, like distinct counts or balances?

This is where Metric Views become important.

Metric Views give the lakehouse a governed business semantic layer. They define reusable fields, measures, joins, metadata, and calculation rules in Unity Catalog. In other words, they turn tribal dashboard logic into explicit semantic context that can be reused by SQL users, dashboards, BI tools, and AI/BI experiences.

This post starts a deep-dive series on Metric Views from that perspective: not just “how do I write YAML?”, but “how do I create reliable business context that both people and AI systems can use?”

We will begin with materialization because once a semantic layer becomes useful, people will query it repeatedly. The next question is obvious: how do we keep governed metrics fast without making users choose between raw tables, aggregate tables, and dashboard-specific extracts?

## Base Tables Used in This Series

Before defining any Metric View, we need a simple business data model that is easy to reason about.

For this series, I will use a finance star schema with one daily fact table and a handful of dimensions:

- `mat_fact_finance_daily`: just under 1 million daily finance rows.
- `mat_dim_calendar`: date, month, quarter, year.
- `mat_dim_entity`: entity, country, region.
- `mat_dim_product`: product, product family, business unit.
- `mat_dim_segment`: customer segment and segment group.
- `mat_dim_account`: revenue, COGS, and Opex accounts.

The fact table scale comes from:

```text
731 days x 6 entities x 15 products x 3 segments x 5 accounts = 986,850 rows
```

This model is still small enough to run quickly in a demo workspace, but large enough to make materialization visible in Query Profile.

More importantly, the model gives us the business context we need for the whole series:

- Calendar fields for time-based grouping.
- Entity fields for region-level rollups.
- Product fields for product-family drilldowns.
- Segment fields for customer context.
- Account fields for defining revenue, COGS, and Opex measures.

The Metric View will define how these fact and dimension tables relate, then expose business-friendly fields and measures on top.

[Insert Diagram 1: Star schema showing MAT_FACT_FINANCE_DAILY joined to calendar, entity, product, segment, and account dimensions. Rendered in notebook 00.]

## The Performance Problem

A Metric View can make metric definitions consistent:

```sql
SELECT
  fiscal_year,
  fiscal_month,
  region,
  product_family,
  account_category,
  MEASURE(revenue) AS revenue
FROM mat_finance_metric_view
WHERE fiscal_year = 2025
GROUP BY ALL;
```

But if the Metric View has to join dimensions and prepare derived fields every time, repeated dashboard queries may still be expensive.

The old workaround is to create many aggregate tables manually:

- `revenue_by_month_region`
- `revenue_by_month_region_product`
- `revenue_by_month_region_product_account`

That makes queries faster, but it creates another problem: users now need to know which physical table to query.

Metric View materialization keeps the semantic surface stable.

## Step 1: Define the Non-Materialized Metric View

First create the semantic contract without materialization:

```sql
CREATE OR REPLACE VIEW mat_finance_metric_view
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: lakemeter_catalog.metric_views_lod_demo.mat_fact_finance_daily

joins:
  - name: calendar
    source: lakemeter_catalog.metric_views_lod_demo.mat_dim_calendar
    on: source.transaction_date = calendar.calendar_date
  - name: entity
    source: lakemeter_catalog.metric_views_lod_demo.mat_dim_entity
    on: source.entity_id = entity.entity_id
  - name: product
    source: lakemeter_catalog.metric_views_lod_demo.mat_dim_product
    on: source.product_id = product.product_id
  - name: segment
    source: lakemeter_catalog.metric_views_lod_demo.mat_dim_segment
    on: source.segment_id = segment.segment_id
  - name: account
    source: lakemeter_catalog.metric_views_lod_demo.mat_dim_account
    on: source.account_id = account.account_id

fields:
  - name: fiscal_year
    expr: calendar.fiscal_year
  - name: fiscal_month
    expr: calendar.fiscal_month
  - name: region
    expr: entity.region
  - name: product_family
    expr: product.product_family
  - name: account_category
    expr: account.account_category

measures:
  - name: revenue
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Revenue')
  - name: cogs
    expr: SUM(amount) FILTER (WHERE account.account_category = 'COGS')
  - name: opex
    expr: SUM(amount) FILTER (WHERE account.account_category = 'Opex')
  - name: gross_profit
    expr: MEASURE(revenue) - MEASURE(cogs)
  - name: ebitda
    expr: MEASURE(revenue) - MEASURE(cogs) - MEASURE(opex)
  - name: unique_customers
    expr: COUNT(DISTINCT customer_id)
$$;
```

This view defines meaning.

## Step 2: Add Materialization

The non-materialized Metric View defines the meaning of the metrics.

To accelerate it, we create a materialized variant and add a `materialization:` block.

This is the important part:

```yaml
materialization:
  schedule: every 6 hours
  mode: relaxed
  materialized_views:
    - name: semantic_snapshot
      type: unaggregated

    - name: month_region_product_account
      type: aggregated
      dimensions:
        - fiscal_year
        - fiscal_month
        - region
        - product_family
        - account_category
      measures:
        - revenue
        - cogs
        - opex
```

The user still queries the Metric View. They do not query `semantic_snapshot` or `month_region_product_account` directly.

## Unaggregated vs Aggregated Materialization

`semantic_snapshot` is an unaggregated materialization.

It precomputes the Metric View's source preparation:

- source fact table
- joins
- filters
- fields

This is useful when query shapes vary.

`month_region_product_account` is an aggregated materialization.

It precomputes a known dashboard grain:

- fiscal year
- fiscal month
- region
- product family
- account category

This is useful when dashboards repeatedly query the same or coarser grains.

## Step 3: Inspect Refresh Status

Materialization creates managed Spark Declarative Pipeline resources behind the Metric View.

Inspect it with:

```sql
DESCRIBE EXTENDED mat_finance_metric_view_materialized;
```

Look for:

- `Latest Refresh Status`
- `Latest Refresh`
- `Refresh Schedule`
- materialization pipeline properties

## Automatic Query Rewrite

The most important part of materialization is automatic query rewrite.

When you query a materialized Metric View, you still write normal Metric View SQL:

```sql
SELECT
  fiscal_year,
  fiscal_month,
  region,
  MEASURE(revenue) AS revenue
FROM mat_finance_metric_view_materialized
WHERE fiscal_year = 2025
GROUP BY ALL;
```

You do not query `month_region_product_account` directly.

The optimizer decides whether one of the materializations can serve the query.

The rewrite decision order is:

1. Exact match
2. Rollup match
3. Unaggregated match
4. Source fallback

[Insert Diagram 3: Automatic query rewrite decision tree: exact match, rollup match, unaggregated match, source fallback. Rendered in notebook 01.]

The rest of the tutorial proves each path with simple SQL queries and Query Profile screenshots.

## Step 4: Exact Match

Exact match means the query asks for the same dimensions as the aggregate materialization.

```sql
SELECT
  fiscal_year,
  fiscal_month,
  region,
  product_family,
  account_category,
  MEASURE(revenue) AS revenue
FROM mat_finance_metric_view_materialized
WHERE fiscal_year = 2025
GROUP BY ALL;
```

After running it, open Query Profile and look for:

```text
month_region_product_account
```

Screenshot:

![Exact match scans the aggregated materialization](/Users/steven.tan/.cursor/projects/Users-steven-tan-Metric-View-Blog/assets/Screenshot_2026-06-26_at_9.13.24_AM-2c9fa09b-8282-4ace-a40d-4cf7007afcfa.png)

What this shows:

The query profile scans the generated materialization table ending in:

```text
month_region_product_account_1
```

There is no additional grouping step above the scan. That is exactly what we want for an exact match: the query asks for the same grain that the aggregated materialization already stores.

## Step 5: Rollup Match

Rollup match means the query asks for fewer dimensions, and the measure is additive.

```sql
SELECT
  fiscal_year,
  fiscal_month,
  region,
  MEASURE(revenue) AS revenue
FROM mat_finance_metric_view_materialized
WHERE fiscal_year = 2025
GROUP BY ALL;
```

After running it, open Query Profile and look for:

```text
month_region_product_account
```

The engine can roll up from product/account grain to region grain because `revenue` is additive.

Screenshot:

![Rollup match scans the same aggregate and groups again](/Users/steven.tan/.cursor/projects/Users-steven-tan-Metric-View-Blog/assets/Screenshot_2026-06-26_at_9.14.24_AM-6a47fb0f-ced2-412e-b21c-8ab66fd0d2a5.png)

What this shows:

The query profile still scans the same generated materialization table:

```text
month_region_product_account_1
```

But this time there is a `Grouping Aggregate` operator above the scan. That is the rollup. Databricks reads the more detailed materialization and aggregates it to the coarser query grain.

## Step 6: Non-Additive Fallback

`COUNT(DISTINCT customer_id)` is non-additive.

It cannot safely roll up from partial aggregates.

```sql
SELECT
  fiscal_year,
  fiscal_month,
  region,
  MEASURE(unique_customers) AS unique_customers
FROM mat_finance_metric_view_materialized
WHERE fiscal_year = 2025
GROUP BY ALL;
```

After running it, open Query Profile and look for:

```text
semantic_snapshot
```

This is still useful. The query does not use the aggregate, but it avoids recomputing the expensive fact-to-dimension joins and field preparation.

Screenshot:

![Unaggregated match scans semantic snapshot](/Users/steven.tan/.cursor/projects/Users-steven-tan-Metric-View-Blog/assets/Screenshot_2026-06-26_at_9.15.35_AM-8b261530-e523-474a-854d-939b2e084b04.png)

What this shows:

The query profile scans the generated materialization table ending in:

```text
semantic_snapshot_1
```

This query uses `COUNT(DISTINCT customer_id)`. Distinct counts cannot safely roll up from partial aggregate rows, so Databricks chooses the unaggregated materialization instead of `month_region_product_account`.

## Step 7: Source Fallback

To prove true source fallback, create an aggregated-only Metric View:

```text
mat_finance_metric_view_agg_only
```

It has an aggregate materialization for revenue, but no unaggregated materialization.

If you query `unique_customers`, there is no exact aggregate and no unaggregated snapshot, so the query must fall back to the source path.

Screenshot:

![Source fallback scans source tables](/Users/steven.tan/.cursor/projects/Users-steven-tan-Metric-View-Blog/assets/image-eb2c8235-3a90-4d3d-b95e-a685c3fc81f8.png)

What this shows:

The query profile no longer shows `month_region_product_account` or `semantic_snapshot`.

Instead, the plan expands to the underlying source path and scans the base fact and dimension tables required by the Metric View joins. This is the final fallback path when no available materialization can answer the query.

## What We Prove

The tutorial does not rely on stopwatch timings. Each section uses a simple query and then checks Query Profile.

If Query Profile shows the expected materialization name, rewrite happened.

## Design Guidance

Use aggregated materialization when:

- Dashboard query shapes are predictable.
- Measures are additive.
- You can materialize at the most detailed useful grain.

Use unaggregated materialization when:

- The Metric View has expensive joins or transformations.
- Query shapes are unpredictable.
- You want queries to read from a consistent prepared snapshot.

Avoid materializing:

- Very high-cardinality dimensions that produce mostly single-row groups.
- Measures that cannot roll up unless you also provide exact aggregate shapes.
- Metric Views with security policies that are unsupported by materialization.

## Closing

Metric View materialization is not just “make it faster.”

It is a production semantic-layer pattern:

- Define metrics once.
- Keep user SQL stable.
- Let Databricks maintain physical acceleration.
- Verify rewrite with Query Profile.

That is the difference between manually managing aggregate tables and operating a governed business semantic layer.

The notebooks, query examples, and screenshots for this deep dive are available here:

https://github.com/CheeYuTan/metric-views-deep-dive

