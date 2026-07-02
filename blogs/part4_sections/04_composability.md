## Composability: Build Measures From Measures

After LOD and window semantics, composability becomes easier to understand.

At this point, the Metric View already has several governed measures:

- base measures such as `loss_amount` and `exposure_amount`
- LOD measures such as `pct_of_global_loss_year_fixed_lod`
- window measures such as `current_month_loss` and `prior_year_loss`

Composability means a measure can reuse those governed measures instead of reaching back to raw columns or repeating formulas in every dashboard.

For example, without composability, every dashboard author might write:

```sql
SUM(loss_amount) / SUM(exposure_amount)
```

That looks simple, but it creates risk. If the definition of loss changes, or exposure needs an exclusion, or the denominator needs zero handling, every copied formula needs to be found and updated.

In the notebook, `loss_rate` is defined once in the Metric View:

```yaml
- name: loss_rate
  expr: MEASURE(loss_amount) / NULLIF(MEASURE(exposure_amount), 0)
```

The same pattern is used for year-over-year growth:

```yaml
- name: yoy_loss_growth
  expr: MEASURE(current_month_loss) - MEASURE(prior_year_loss)

- name: yoy_loss_growth_pct
  expr: MEASURE(yoy_loss_growth) / NULLIF(MEASURE(prior_year_loss), 0)
```

The important detail is that composed measures use `MEASURE(...)`. They build on governed measures, not raw columns.

Screenshot to include:

**Figure 8: Composed measures reuse trusted base and window measures instead of duplicating formulas in every dashboard.**

Paste the screenshot from the notebook's **Composability: Measures Building on Measures** diagram here.

The notebook query shows both kinds of composition:

```sql
SELECT
  risk_area,
  region,
  MEASURE(loss_amount) AS loss_amount,
  MEASURE(exposure_amount) AS exposure_amount,
  MEASURE(loss_rate) AS loss_rate,
  MEASURE(yoy_loss_growth_pct) AS yoy_loss_growth_pct
FROM risk_advanced_metric_semantics
WHERE reporting_year = 2025
  AND risk_area = 'Fraud Risk'
GROUP BY ALL
ORDER BY region;
```

Screenshot to include:

**Figure 9: A single query can consume composed measures such as loss rate and YoY loss growth percentage.**

Paste the query output for **Composability: Measures Building on Measures** here.

The rule of thumb is simple:

- Define atomic measures first.
- Build ratios, percentages, and growth metrics from governed measures.

This keeps the metric layer readable. It also avoids the common problem where every dashboard has a slightly different formula for the same ratio.
