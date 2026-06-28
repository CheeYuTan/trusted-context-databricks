## Join Cardinality and `rely`

Join cardinality controls how the Metric View engine interprets the relationship.

At a high level:

```text
many_to_one = dimension lookup
one_to_many = fact expansion
```

Many-to-one is the default. It is the common pattern for dimensions such as product, risk grade, branch, or region.

One-to-many is different. It allows a single source row to match multiple rows in a joined fact table. That is useful when one customer has many applications, cases, orders, or events.

The `rely.at_most_one_match: true` setting is a declaration that a uniqueness constraint holds. It can help the engine skip unnecessary work, but it must be used carefully. If the asserted relationship is not actually true, metrics can be wrong.

For many-to-one joins, `rely` means each source row should match at most one dimension row. For one-to-many joins, it means each joined row should match at most one source row.

Do not use `rely` just because you expect the relationship to be clean. Use it only when the uniqueness rule is genuinely true and enforced by the data model.

**Figure 4: Cardinality tells the Metric View whether the joined table behaves like a dimension lookup or a fact branch.**

Image to paste here:

`assets/part3_metric_view_joins/figure_02_join_cardinality.png`

**Figure 5: `rely` is a promise that the relationship has no fanout on the asserted side.**

Image to paste here:

`assets/part3_metric_view_joins/figure_05_many_to_one_rely_zoom.png`

Many-to-one joins are ideal for dimension lookup. They enrich the source row without changing its grain.

But sometimes the joined table is not a dimension. Sometimes it is another fact table below the source grain. A customer can have many applications. An account can have many events. A merchant can have many transactions.

That is where one-to-many joins come in.
