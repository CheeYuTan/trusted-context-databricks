## Level of Detail: Percent of What?

Level of Detail, or LOD, sounds more complicated than it is.

In plain English, LOD asks:

```text
At what grain should this calculation happen?
```

For simple sums, this is usually obvious. If I group by region, I get the sum by region. If I group by risk area and region, I get the sum by risk area and region.

Percentages are where the question becomes important.

The numerator is usually easy. The harder part is the denominator:

```text
Percent of what?
```

For example, suppose we ask:

```text
What percentage of annual loss came from Fraud Risk in APJ?
```

The numerator is:

```text
Fraud Risk loss in APJ
```

But the denominator could mean several different things:

```text
all loss globally
all Fraud Risk loss globally
all APJ loss
only the currently filtered result
```

Those are different business definitions, even though the visible row is the same.

In the notebook, the Metric View defines two fixed LOD fields:

```yaml
fields:
  - name: global_loss_year_lod
    expr: SUM(loss_amount) OVER (PARTITION BY reporting_year)

  - name: risk_area_loss_year_lod
    expr: SUM(loss_amount) OVER (PARTITION BY reporting_year, risk_area)
```

Those fields prepare reusable annual denominators:

- `global_loss_year_lod` repeats the same annual loss total for every row in the same reporting year.
- `risk_area_loss_year_lod` repeats the same annual loss total for every row in the same reporting year and risk area.

The percentage measures then use those denominators:

```yaml
measures:
  - name: pct_of_global_loss_year_fixed_lod
    expr: MEASURE(loss_amount) / NULLIF(ANY_VALUE(global_loss_year_lod), 0)

  - name: pct_of_risk_area_loss_year_fixed_lod
    expr: MEASURE(loss_amount) / NULLIF(ANY_VALUE(risk_area_loss_year_lod), 0)
```

The `MEASURE(loss_amount)` part is the numerator at the visible query grain. The `ANY_VALUE(...)` part picks up the repeated fixed denominator for that group.

`ANY_VALUE` does not mean "choose a random business value" here. It is appropriate because the LOD field is intentionally repeated at the denominator grain.

Screenshot to include:

**Figure 2: Fixed LOD separates the visible row from the denominator used for the percentage.**

Paste the screenshot from the notebook's **Fixed LOD: Percent of What?** diagram here.

The notebook query then asks for both percentages side by side:

```sql
SELECT
  reporting_year,
  risk_area,
  region,
  MEASURE(loss_amount) AS loss_amount,
  MEASURE(pct_of_global_loss_year_fixed_lod) AS pct_of_global_loss,
  MEASURE(pct_of_risk_area_loss_year_fixed_lod) AS pct_of_risk_area_loss
FROM risk_advanced_metric_semantics
WHERE reporting_year = 2025
GROUP BY ALL
ORDER BY risk_area, region;
```

Screenshot to include:

**Figure 3: The same visible row can have different percentage results because the denominators are different.**

Paste the query output for **Fixed LOD: Percent of What?** here.

The notebook also shows coarser LOD with `range: all`. This is useful when the denominator should still respect the current filters, but ignore one visible grouping field.

In the example, the query filters to `Fraud Risk` and groups by `region`. The business question is:

```text
Within visible Fraud Risk results for 2025, how much does each region contribute?
```

The measure `risk_area_loss_excluding_region` keeps the risk-area context but removes the region detail from the denominator.

That gives us a practical distinction:

```text
Fixed LOD = definition-time denominator
Coarser LOD = query-aware denominator with selected fields excluded
```

Screenshot to include:

**Figure 4: Coarser LOD calculates each region's share of the visible Fraud Risk total.**

Paste the query output for **Coarser LOD: Filter-Aware Share** here.

The key idea is simple: LOD makes the denominator part of the governed metric definition.
