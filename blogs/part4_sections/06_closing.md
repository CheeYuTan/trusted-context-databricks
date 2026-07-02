## Closing Thoughts

This post is about making calculation rules explicit.

The questions are simple, but easy to get wrong when every dashboard, SQL query, and agent handles them independently:

- **Percent of what?**
- **Over what time frame?**
- **Built from which measures?**
- **Understood with which business language?**

Metric Views let those answers live in the governed semantic layer.

In this part, we covered:

- **LOD**, so percentages have governed denominators
- **window semantics**, so current, YTD, trailing, prior-year, and semiadditive calculations are reusable
- **composability**, so higher-level metrics build from trusted measures
- **agent metadata**, so BI tools and agents understand the business language around the metric

The companion notebook recreates the full example end to end:

`notebooks/part4_advanced_semantics/01_lod_windows_agent_metadata.py`

You can run it to create the synthetic Risk and Compliance data, define the Metric View, render the diagrams, and inspect the query outputs used in this article.

After advanced semantics, the next production question is performance:

> How do trusted Metric Views stay fast when dashboards, SQL users, BI tools, Genie Spaces, and agents all depend on them?

That is where Part 5, Metric Views in Production, fits.
