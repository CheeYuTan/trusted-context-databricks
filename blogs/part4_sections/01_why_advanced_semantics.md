## Why Advanced Semantics Matter

In Part 1, we started with Discover and Domains: the business-friendly entry point that helps people and agents find trusted assets in the lakehouse.

In Part 2, we used Metric Views to define a certified KPI layer.

In Part 3, we looked at joins and relationship modeling: how facts and dimensions should connect so the metric layer can answer questions without fanout or ambiguous joins.

Part 4 is about the next layer of trust.

Once a Metric View is discoverable, certified, and modeled against the right tables, people start asking richer questions:

> What percentage of annual loss came from Fraud Risk?
>
> Show me year-to-date loss and year-over-year growth.
>
> Can I reuse the trusted loss and exposure measures to calculate loss rate?
>
> Will Genie understand that "risk loss", "financial loss", and "loss amount" all refer to the same governed KPI?

These questions are not just about SQL syntax. They are semantic questions.

They define:

- the denominator behind a percentage
- the time frame behind a rolling, YTD, or prior-year measure
- the lower-level measures used to build a ratio or growth metric
- the business language that helps dashboards and agents understand the metric

This is where metric definitions often drift. Two dashboards might both show "loss rate", but one could divide by current exposure while another divides by average exposure. Two reports might both show "YTD", but one could reset at calendar year while another resets at fiscal year. An agent might answer from a raw column because it does not know that "financial loss" should map to a certified measure.

Advanced Metric View semantics bring those decisions into the governed semantic object.

You can follow along with the companion notebook in GitHub:

https://github.com/CheeYuTan/trusted-context-databricks/blob/main/notebooks/part4_advanced_semantics/01_lod_windows_agent_metadata.py

In the companion notebook, the flow is:

1. **Level of Detail**: percent of what?
2. **Window semantics**: over what time frame?
3. **Composability**: which trusted measures does this measure reuse?
4. **Agent metadata**: what business language should AI/BI understand?

Screenshot to include:

**Figure 1: Advanced Metric View semantics turn business questions into consistent answers across SQL, BI, and agents.**

Paste the screenshot from the notebook's **Mental Model** diagram here.
