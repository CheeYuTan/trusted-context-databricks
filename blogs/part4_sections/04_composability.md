## Composability: Build Metrics From Metrics

Metric Views can define measures that reference other measures.

That is important because business metrics are rarely isolated.

For example, the notebook defines:

```text
loss_rate = loss_amount / exposure_amount
yoy_loss_growth = current_month_loss - prior_year_loss
yoy_loss_growth_pct = yoy_loss_growth / prior_year_loss
```

Instead of repeating the same SQL expression in every dashboard, the Metric View composes measures once.

This makes the metric layer easier to audit and change.
