## Star Schema Joins

In a star schema, the Metric View source is the fact table and the joined tables provide dimensions.

You can follow along with the companion notebook here:

```text
https://github.com/CheeYuTan/trusted-context-databricks/blob/main/notebooks/part3_metric_view_joins/00_metric_view_joins_deep_dive.py
```

In the notebook, `credit_exposure_fact` is the fact table. It joins to:

- `dim_product`
- `dim_risk_grade`

The Metric View exposes business fields such as:

```text
product_line
product_family
risk_band
risk_tier
```

and measures such as:

```text
exposure_amount
expected_credit_loss
ecl_rate
```

The user query does not join these tables. The Metric View owns that relationship.

This is the simplest and most common Metric View relationship pattern:

```text
fact table -> dimension table
```

The fact table owns the numeric events. The dimension tables provide the business language. In this example, the exposure fact only has `product_id` and `risk_grade_id`, but users want to group by `product_line` and `risk_band`.

That is exactly what the Metric View join provides.

**Figure 2: In a star schema, the Metric View starts from a fact table and joins directly to descriptive dimensions.**

Image to paste here:

`assets/part3_metric_view_joins/figure_03_star_schema_zoom.png`

The Metric View definition starts from the exposure fact and joins directly to the product and risk-grade dimensions:

```yaml
source: credit_exposure_fact
joins:
  - name: product
    source: dim_product
    on: source.product_id = product.product_id
    rely:
      at_most_one_match: true
  - name: risk_grade
    source: dim_risk_grade
    on: source.risk_grade_id = risk_grade.risk_grade_id
    rely:
      at_most_one_match: true
fields:
  - name: product_line
    expr: product.product_line
  - name: risk_band
    expr: risk_grade.risk_band
measures:
  - name: exposure_amount
    expr: SUM(exposure_amount)
```

The query can stay focused on business fields and governed measures:

```sql
SELECT
  product_line,
  risk_band,
  MEASURE(exposure_amount),
  MEASURE(expected_credit_loss)
FROM credit_risk_star_metrics
GROUP BY ALL;
```

Figure to capture:

```text
Notebook output: Star Schema Joins
```

Star schema joins are the easiest place to start because the relationship is direct: one fact table joins to one or more dimension tables.

But not every dimension model is that flat. In many enterprise models, dimensions are normalized. A fact might join to a branch, and the branch might join to a region. That is where snowflake joins become useful.
