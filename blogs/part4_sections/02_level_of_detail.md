## Level of Detail: Percent of What?

LOD is about denominator control.

The numerator is often easy. The harder question is:

```text
Percent of what?
```

In the notebook, fixed LOD fields define annual denominators such as:

```text
global_loss_year_lod
risk_area_loss_year_lod
```

The measures then reference those fixed fields with `ANY_VALUE`.

`ANY_VALUE` is safe here because the fixed denominator is constant within the relevant group. The SQL engine still needs an aggregate wrapper when a measure references a field, and `ANY_VALUE` tells it to take that fixed value.

This lets one Metric View answer questions such as:

```text
What share of annual global loss came from Fraud Risk in APJ?
What share of annual Fraud Risk loss came from each region?
```

The notebook also shows coarser LOD using `range: all`, where the denominator respects filters but excludes selected visible fields.

That distinction matters:

```text
Fixed LOD = definition-time denominator
Coarser LOD = query-aware denominator with selected fields excluded
```

This is how the Metric View prevents dashboards from silently redefining percentages.

The output should be read carefully.

If a row shows Fraud Risk in APJ with:

```text
pct_of_global_loss = 4%
pct_of_risk_area_loss = 35%
```

those are not conflicting numbers. They answer different denominator questions:

```text
4% of all annual loss
35% of annual Fraud Risk loss
```

That is the entire point of LOD. The Metric View makes the denominator explicit instead of letting every dashboard redefine it.

Screenshots to capture:

```text
Notebook output: Fixed LOD - Percent of What?
Notebook output: Coarser LOD - Filter-Aware Share
```
