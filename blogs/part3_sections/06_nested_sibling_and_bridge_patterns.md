## Nested, Sibling, and Bridge Patterns

The join documentation goes beyond simple fact-to-dimension modeling.

The notebook includes three additional patterns.

### Nested one-to-many

Customers have applications. Applications have decisions.

```text
customer_spine -> loan_applications -> application_decisions
```

This lets the Metric View measure application count, decision count, and approved amount from the same customer-grain semantic object.

**Figure 7: Nested one-to-many joins follow a row-level path from customer to application to decision.**

Image to paste here:

`assets/part3_metric_view_joins/figure_07_nested_one_to_many_zoom.png`

The key detail is the full dot-path. Measures reference nested columns through the join names, such as:

```text
applications.decisions.approved_amount
```

For counts, use `COUNT(DISTINCT ...)` when the nested branch can fan out the parent entity.

The nested join is defined under the parent one-to-many join:

```yaml
source: customer_spine
joins:
  - name: applications
    source: loan_applications
    cardinality: one_to_many
    joins:
      - name: decisions
        source: application_decisions
        cardinality: one_to_many
measures:
  - name: decision_count
    expr: COUNT(applications.decisions.decision_id)
  - name: approved_amount
    expr: SUM(applications.decisions.approved_amount)
```

### Sibling one-to-many

Customers can have applications and service cases as independent branches.

```text
customer_spine -> loan_applications
customer_spine -> service_cases
```

Sibling one-to-many joins are aggregated independently, so the rows do not cross-multiply.

That independence is the point. If applications and service cases were joined naively, the rows could multiply. Metric Views aggregate each sibling fact branch separately, then blend the results at the query grain.

**Figure 8: Sibling one-to-many joins keep independent fact branches separate before blending results.**

Image to paste here:

`assets/part3_metric_view_joins/figure_08_sibling_one_to_many_zoom.png`

The sibling branches sit at the same level under the source:

```yaml
source: customer_spine
joins:
  - name: applications
    source: loan_applications
    cardinality: one_to_many
  - name: cases
    source: service_cases
    cardinality: one_to_many
measures:
  - name: application_count
    expr: COUNT(applications.application_id)
  - name: case_count
    expr: COUNT(cases.case_id)
```

### Bridge table for multiple facts

When two fact tables share dimensions but sit at different grains, a bridge declares the valid combinations.

In the notebook, a bridge over product and branch connects:

```text
credit_exposure_fact
fraud_event_fact
```

This keeps exposure and fraud measures predictable without forcing every query to infer the relationship.

The bridge is useful when no single fact table should be treated as the source of truth for the shared dimension combinations. Instead, the bridge declares which product and branch pairs are valid, and each fact table contributes measures independently.

**Figure 9: A bridge declares the shared grain so multiple fact tables can contribute measures independently.**

Image to paste here:

`assets/part3_metric_view_joins/figure_09_bridge_pattern_zoom.png`

The bridge is the Metric View source:

```yaml
source: |
  SELECT DISTINCT product_id, branch_id FROM credit_exposure_fact
  UNION
  SELECT DISTINCT product_id, branch_id FROM fraud_event_fact
joins:
  - name: exposures
    source: credit_exposure_fact
    cardinality: one_to_many
  - name: fraud
    source: fraud_event_fact
    cardinality: one_to_many
measures:
  - name: exposure_amount
    expr: SUM(exposures.exposure_amount)
  - name: confirmed_fraud_loss
    expr: SUM(fraud.confirmed_loss)
```

Screenshots to capture:

```text
Notebook output: Nested One-to-Many Join
Notebook output: Sibling One-to-Many Joins
Notebook output: Bridge Pattern for Multiple Fact Tables
```

At this point, we have covered the main relationship patterns: direct dimensions, multi-hop dimensions, one-to-many facts, nested facts, sibling facts, and bridge tables.

The syntax matters, but the bigger lesson is design discipline. In production, the hardest part is not writing the join. It is choosing the right source grain, ownership boundary, and cardinality model.
