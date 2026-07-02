## Agent Metadata: Business Language for AI/BI

The calculation is only part of the semantic layer. The language around the calculation matters too.

A Metric View may define a measure called `loss_amount`.

But a person or agent might ask for the same concept in different words:

- loss
- risk loss
- financial loss

Those phrases should all point to the same governed measure.

Metric View metadata helps AI/BI tools interpret fields and measures in business terms.

In the notebook, fields and measures include metadata such as:

- `display_name`
- `comment`
- `synonyms`
- `format`

For example, the `loss_amount` measure includes business-friendly language:

```yaml
- name: loss_amount
  expr: SUM(loss_amount)
  display_name: Loss Amount
  comment: Total loss amount for the selected grain.
  format:
    type: currency
    currency_code: USD
    abbreviation: compact
  synonyms:
    - loss
    - risk loss
    - financial loss
```

This tells downstream tools several things:

- show the label as **Loss Amount**
- explain what the measure means
- display the result as currency
- map phrases like "risk loss" and "financial loss" back to the governed measure

Formats matter as well. A loss rate should display as a percentage. Exposure and loss amounts should display as currency. These are not just cosmetic choices. They shape how BI tools, dashboards, and agents present the answer.

In the notebook, `DESCRIBE EXTENDED` lets us inspect the Metric View metadata stored in Unity Catalog.

Screenshot to include:

**Figure 10: Metric View metadata stores business labels, comments, synonyms, and formats next to the governed measures.**

Paste the output for **DESCRIBE EXTENDED risk_advanced_metric_semantics** here.

Agent metadata is not decoration.

It makes the governed semantic layer understandable to both humans and AI systems.
