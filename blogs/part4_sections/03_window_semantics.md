## Window Semantics: Over What Time Frame?

Window semantics answer a different question from LOD.

LOD asks: **percent of what?**

Window semantics ask: **over what time frame?**

For Risk and Compliance reporting, the same monthly loss data can answer several different questions:

- What happened this month?
- What has happened year-to-date?
- What happened over the trailing three months?
- What happened in the same month last year?
- What is the latest month-end balance?

If each dashboard writes its own SQL for those questions, the definitions can drift. One dashboard's YTD might reset at calendar year. Another might reset at fiscal year. One trailing period might include the current month. Another might not.

Metric View window measures let the semantic layer own those time-aware definitions.

In the notebook, the Metric View defines:

```yaml
measures:
  - name: current_month_loss
    expr: SUM(loss_amount)
    window:
      - order: month
        range: current
        semiadditive: last

  - name: ytd_loss
    expr: SUM(loss_amount)
    window:
      - order: month
        range: cumulative
        semiadditive: last
      - order: reporting_year
        range: current
        semiadditive: last

  - name: trailing_3_month_loss
    expr: SUM(loss_amount)
    window:
      - order: month
        range: trailing 3 month inclusive
        semiadditive: last

  - name: prior_year_loss
    expr: SUM(loss_amount)
    window:
      - order: month
        range: current
        semiadditive: last
        offset: -12 month
```

The important pieces are:

- `order`: the field that defines the sequence
- `range`: the frame to evaluate
- `semiadditive`: what to return when the order field is not grouped
- `offset`: how to shift the frame for prior-period comparisons

Screenshot to include:

**Figure 5: Window semantics make current, YTD, trailing, and prior-year calculations explicit.**

Paste the screenshot from the notebook's **Window Semantics: YTD, Trailing, and Prior Year** diagram here.

The query then makes the time behavior visible month by month:

```sql
SELECT
  month,
  risk_area,
  MEASURE(current_month_loss) AS current_month_loss,
  MEASURE(ytd_loss) AS ytd_loss,
  MEASURE(trailing_3_month_loss) AS trailing_3_month_loss,
  MEASURE(prior_year_loss) AS prior_year_loss,
  MEASURE(yoy_loss_growth_pct) AS yoy_loss_growth_pct
FROM risk_advanced_metric_semantics
WHERE risk_area = 'Credit Risk'
  AND region = 'APJ'
GROUP BY ALL
ORDER BY month;
```

Screenshot to include:

**Figure 6: The same monthly data can produce current-month, YTD, trailing, and prior-year metrics from governed window definitions.**

Paste the query output for **Window Semantics: YTD, Trailing, and Prior Year** here.

Window semantics also matter for semiadditive measures.

Some measures are additive. Loss amount is a good example: if January loss is 10 and February loss is 20, total loss across both months is 30.

Balances are different. If a month-end balance is 1M in January and 1.1M in February, the two-month balance is not 2.1M. The useful answer is usually the latest balance in the period.

The notebook models that rule directly:

```yaml
- name: month_end_balance
  expr: SUM(month_end_balance)
  comment: Semiadditive balance that should use the latest month when month is not grouped.
  window:
    - order: month
      range: current
      semiadditive: last
```

Screenshot to include:

**Figure 7: Semiadditive measures return the latest month-end balance instead of summing balances across months.**

Paste the query output for **Semiadditive Measure: Month-End Balance** here.

The key idea is simple: **window semantics make time logic reusable.**

Instead of every dashboard deciding what "YTD", "trailing 3 months", or "latest balance" means, the Metric View defines it once.
