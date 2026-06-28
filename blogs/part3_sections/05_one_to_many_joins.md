## One-to-Many Joins

One-to-many joins let a Metric View measure facts that live below the source grain.

In the notebook, `customer_spine` is the source. Each customer appears once. The Metric View joins to `loan_applications`, where a customer can have zero or more applications.

The notebook first shows the naive join problem: the joined row count becomes larger than the customer count when customers have multiple applications.

**Figure 6: One customer can map to multiple loan applications, but the customer should still count once.**

Image to paste here:

`assets/part3_metric_view_joins/figure_06_one_to_many_zoom.png`

The Metric View then declares:

```yaml
source: customer_spine
joins:
  - name: applications
    source: loan_applications
    on: applications.customer_id = source.customer_id
    cardinality: one_to_many
fields:
  - name: customer_segment
    expr: customer_segment
measures:
  - name: customer_count
    expr: COUNT(*)
  - name: application_count
    expr: COUNT(applications.application_id)
  - name: requested_amount
    expr: SUM(applications.requested_amount)
```

That lets one Metric View expose:

```text
customer_count
application_count
requested_amount
```

without duplicating the source customer rows.

This is important because a naïve join would make customer counts wrong. Metric View cardinality tells the engine how to aggregate the joined fact branch correctly.

One restriction is important: fields cannot reference columns from a one-to-many join, because a field must resolve to one value per source row. Measures can reference the one-to-many branch because the engine aggregates that branch.

Screenshot to capture:

```text
Notebook output: One-to-Many Join
```

The basic one-to-many pattern handles one fact branch below one source entity.

Real models often go further. A one-to-many branch can have its own child records. A source entity can have multiple independent one-to-many branches. And sometimes two fact tables need to be joined through a shared bridge.

These are the advanced join patterns we cover next.
