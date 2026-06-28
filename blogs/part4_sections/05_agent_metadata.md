## Agent Metadata: Business Language for AI/BI

Agent metadata helps tools understand the Metric View in business terms.

In the notebook, fields and measures include metadata such as:

- `display_name`
- `comment`
- `synonyms`
- `format`

For example, `loss_amount` can have synonyms such as:

```text
loss
risk loss
financial loss
```

This helps Genie Spaces, dashboards, and other AI/BI experiences map business language to governed fields and measures.

This is especially important for agents. A user might ask for "financial loss", "risk loss", or "loss amount". Synonyms help the agent map those phrases back to the same governed measure instead of guessing from raw columns.

Formats matter too. A risk rate should display as a percentage. An exposure amount should display as currency. These presentation choices are part of the semantic contract because they shape how downstream tools show the result.

Screenshot to capture:

```text
Notebook output: DESCRIBE EXTENDED risk_advanced_metric_semantics
```
