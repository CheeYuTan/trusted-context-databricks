## Snowflake Schema Joins

A snowflake schema normalizes dimensions into multiple levels.

In the notebook, the credit exposure fact joins to `dim_branch`, and `dim_branch` joins to `dim_region`.

That lets the Metric View expose:

```text
branch_name
region_name
product_line
```

without asking every dashboard or agent to understand the multi-hop path:

```text
credit_exposure_fact -> dim_branch -> dim_region
```

This is the value of snowflake joins in a Metric View. The semantic object owns the relationship path; the user only sees the business fields.

Snowflake joins matter when dimensions are normalized. A branch might not store the region name directly. Instead, the branch points to a region table.

Without the Metric View, every query would need to know the nested path:

```sql
credit_exposure_fact
  JOIN dim_branch
  JOIN dim_region
```

With the Metric View, the query simply asks for `region_name`.

That is the pattern:

```text
technical normalized model -> business-friendly fields
```

**Figure 3: In a snowflake schema, the Metric View follows a multi-hop relationship through normalized dimensions.**

Image to paste here:

`assets/part3_metric_view_joins/figure_04_snowflake_schema_zoom.png`

The nested join is defined inside the first-level branch join:

```yaml
source: credit_exposure_fact
joins:
  - name: branch
    source: dim_branch
    on: source.branch_id = branch.branch_id
    joins:
      - name: region
        source: dim_region
        on: branch.region_id = region.region_id
fields:
  - name: branch_name
    expr: branch.branch_name
  - name: region_name
    expr: branch.region.region_name
```

Figure to capture:

```text
Notebook output: Snowflake Schema Joins
```

Star and snowflake joins both assume a dimension-style relationship: each source row should match at most one row in the joined table.

That assumption is important. If the join can fan out, measures can be duplicated. So before going into one-to-many joins, we need to talk about join cardinality.
