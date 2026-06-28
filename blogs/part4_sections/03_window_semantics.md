## Window Semantics: Over What Time Frame?

Window measures let the Metric View own time-aware calculations.

Window measures are currently documented as experimental, so teams should check runtime and feature availability before using them for production reporting.

In the notebook, the Metric View defines:

- current month loss
- running loss
- year-to-date loss
- trailing 3 month loss
- prior year loss
- year-over-year growth
- month-end balance as a semiadditive measure

This matters because time intelligence is often copied into many dashboards.

With Metric Views, the time logic is part of the governed semantic object.

The important pieces are:

```text
order = the field that defines time order
range = current, cumulative, trailing, leading, or all
semiadditive = what to return when the order field is not grouped
offset = shift the frame for prior-period comparisons
```

For example, a trailing window answers "what happened over the recent period?" while an offset answers "what happened in the comparable prior period?"

Semiadditive behavior matters for balances. A month-end balance should not be summed across months; the Metric View should return the latest relevant balance.

The output should be interpreted as a time frame decision.

For one month, the current-month loss answers:

```text
What happened in this month?
```

The YTD loss answers:

```text
What happened from the start of the year through this month?
```

The prior-year loss answers:

```text
What happened in the comparable month last year?
```

Those are different business questions. The Metric View keeps those definitions consistent across dashboards and agents.

Screenshot to capture:

```text
Notebook output: Window Semantics - YTD, Trailing, and Prior Year
```
