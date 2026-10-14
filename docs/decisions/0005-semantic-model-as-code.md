# 0005 · The semantic model as code, every measure checked against the lakehouse

**Status:** accepted · 2026-10-14

**Context.** A medallion ends where people read it: a Power BI model. Models built by hand in Desktop drift from the tables under them, and a DAX measure that is subtly wrong looks just as convincing in a report as a right one. Direct Lake only runs against a Fabric capacity, so the model can't be queried on a laptop as-is.

**Decision.** Define the model once, in `local/build_model.py`: tables with friendly names over the gold tables, relationships, and every measure with a description, format string and display folder. Generate two variants from it: **Direct Lake** TMDL (`wl_model.SemanticModel`, the deployable item) and an **Import** copy over Parquet exports of the same tables. Load the Import copy into a blank Power BI Desktop with `pbi-cli`, run every measure as a DAX query, and compare each result with the same number computed independently in DuckDB (`local/verify_model.py`). The report (`wl_report.Report`, PBIR) is checked in CI: every field a visual uses must exist in the model.

**Consequences.**
- The deployed model and the tested model can't differ in anything but storage mode; a test fails if the committed TMDL drifts from the generator.
- 190 values across all 26 measures match DuckDB, by month, by account and in total, including time intelligence and the coverage logic.
- The model needs a capacity table (`gold_fact_capacity`, person × working day) and a conformed `gold_dim_account`, so utilization and sold-vs-logged are plain aggregations, not DAX workarounds.
- DAX verification needs Power BI Desktop, so it runs on a developer machine, not in CI; the structural checks (TMDL in sync, references resolve, report bindings exist) run in CI.
