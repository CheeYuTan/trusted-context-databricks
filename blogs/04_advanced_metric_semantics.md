# Building Trusted Context in Databricks Part 4: Advanced Metric Semantics

This is the modular index for Blog 4. Each section is intentionally stored in a separate markdown file so the article can be edited section by section.

## Section Order

1. `blogs/part4_sections/00_article_header.md`
2. `blogs/part4_sections/01_why_advanced_semantics.md`
3. `blogs/part4_sections/02_level_of_detail.md`
4. `blogs/part4_sections/03_window_semantics.md`
5. `blogs/part4_sections/04_composability.md`
6. `blogs/part4_sections/05_agent_metadata.md`
7. `blogs/part4_sections/06_closing.md`

## Companion Notebook

`notebooks/part4_advanced_semantics/01_lod_windows_agent_metadata.py`

Latest good validation run:

https://fe-vm-lakemeter.cloud.databricks.com/?o=335310294452632#job/278431141942090/run/928307432041272

## Screenshot Checklist

Use screenshots from the companion notebook and paste them into the relevant sections with these labels:

1. **Figure 1: Advanced Metric View semantics turn business questions into consistent answers across SQL, BI, and agents.**
   - Section: `01_why_advanced_semantics.md`
   - Source: notebook **Mental Model** diagram

2. **Figure 2: Fixed LOD separates the visible row from the denominator used for the percentage.**
   - Section: `02_level_of_detail.md`
   - Source: notebook **Fixed LOD: Percent of What?** diagram

3. **Figure 3: The same visible row can have different percentage results because the denominators are different.**
   - Section: `02_level_of_detail.md`
   - Source: notebook **Fixed LOD: Percent of What?** query output

4. **Figure 4: Coarser LOD calculates each region's share of the visible Fraud Risk total.**
   - Section: `02_level_of_detail.md`
   - Source: notebook **Coarser LOD: Filter-Aware Share** query output

5. **Figure 5: Window semantics make current, YTD, trailing, and prior-year calculations explicit.**
   - Section: `03_window_semantics.md`
   - Source: notebook **Window Semantics: YTD, Trailing, and Prior Year** diagram

6. **Figure 6: The same monthly data can produce current-month, YTD, trailing, and prior-year metrics from governed window definitions.**
   - Section: `03_window_semantics.md`
   - Source: notebook **Window Semantics: YTD, Trailing, and Prior Year** query output

7. **Figure 7: Semiadditive measures return the latest month-end balance instead of summing balances across months.**
   - Section: `03_window_semantics.md`
   - Source: notebook **Semiadditive Measure: Month-End Balance** query output

8. **Figure 8: Composed measures reuse trusted base and window measures instead of duplicating formulas in every dashboard.**
   - Section: `04_composability.md`
   - Source: notebook **Composability: Measures Building on Measures** diagram

9. **Figure 9: A single query can consume composed measures such as loss rate and YoY loss growth percentage.**
   - Section: `04_composability.md`
   - Source: notebook **Composability: Measures Building on Measures** query output

10. **Figure 10: Metric View metadata stores business labels, comments, synonyms, and formats next to the governed measures.**
    - Section: `05_agent_metadata.md`
    - Source: notebook **DESCRIBE EXTENDED risk_advanced_metric_semantics** output
