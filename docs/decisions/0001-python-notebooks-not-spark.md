# 0001 · Python notebooks (DuckDB + delta-rs), not Spark

**Status:** accepted · 2026-10-14

**Context.** The workload is small: about 30k worklog rows a quarter, a few hundred users, a few thousand CRM rows. Fabric offers Spark notebooks and Python notebooks. Python notebooks run a single-node Python kernel with `deltalake`, DuckDB, pandas and pyarrow preinstalled. The project also had to run on a Windows laptop with no Java, Docker or WSL, so a reviewer can clone it and see the tables.

**Decision.** Every notebook is a Fabric **Python notebook** (`jupyter_python` kernel). Transformations are DuckDB SQL over Arrow; reads and writes go through `deltalake` (delta-rs), including `MERGE` for SCD2. A local runner executes the same `notebook-content.py` files with a `notebookutils` stand-in.

**Consequences.**
- Sessions start in seconds and use a fraction of the capacity units a Spark pool would. That matters on an F2/F4 capacity shared with reports.
- One code path runs on the laptop, in CI (ubuntu, no Spark) and in Fabric.
- Most of the SQL is close to Spark SQL (CTEs, window functions, `QUALIFY`, `MERGE` semantics). Moving silver/gold to Spark notebooks or materialized lake views is a mechanical port if volume grows past what one node handles (roughly tens of millions of rows per run).
- The port is no longer hypothetical: [ADR 0004](0004-spark-shadow-path.md) adds Spark versions of silver and the fact table, proven equal in CI.
- What it doesn't show: Spark-specific tuning (partitioning, `OPTIMIZE`/`VACUUM` schedules, V-Order). Revisit if a source arrives that is genuinely big.
