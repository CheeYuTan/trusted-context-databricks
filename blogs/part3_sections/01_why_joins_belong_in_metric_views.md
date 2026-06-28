## Why Joins Belong in Metric Views

In [Part 1](https://medium.com/@cheeyutcy/building-trusted-context-in-databricks-part-1-discover-and-domains-3a669a866779?source=friends_link&sk=738faf29e871b890470767b19a976b32), I covered Discover and Domains: where people and agents should look for trusted context.

In [Part 2](https://medium.com/@cheeyutcy/building-trusted-context-in-databricks-part-2-metric-views-as-the-certified-kpi-layer-29079a952db5?source=friends_link&sk=41d5fd81e33e715c1ab57e60962dc8c9), I covered Metric Views as the certified KPI layer: which KPI definition should be trusted.

Part 2 focused on the KPI contract: fields, measures, `MEASURE()`, certification, and reuse.

But a real semantic layer also needs to know how facts connect to business context.

Without joins in the semantic layer, every dashboard author, SQL user, and agent has to know the relationship model:

```text
Which product table joins to the fact?
Which branch table gives me region?
Which customer spine should I use?
Does this relationship fan out?
Can I safely aggregate after the join?
```

That is too much logic to leave in every downstream query.

Metric View joins solve this by moving relationship logic into the governed semantic object.

That matters because joins are not just technical plumbing.

Joins decide:

- which attributes are available for grouping,
- whether a metric fans out,
- which source grain is preserved,
- and whether two fact tables can be combined safely.

If the join model is wrong, the metric can still run but return the wrong answer.

In this post, I will walk through the main join patterns supported by Metric Views:

- star schema joins for fact-to-dimension modeling,
- snowflake joins for multi-hop dimension relationships,
- join cardinality and `rely`,
- one-to-many joins for facts below the source grain,
- nested and sibling one-to-many joins,
- and bridge patterns for combining multiple fact tables safely.

**Figure 1: Metric View joins cover star schemas, snowflake schemas, one-to-many facts, and bridge patterns.**

Image to paste here:

`assets/part3_metric_view_joins/figure_01_join_patterns.png`
