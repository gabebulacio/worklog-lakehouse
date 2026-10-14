# 0001 · Right-size the engine: Python notebooks for the daily load

**Status:** accepted · 2026-10-14

**Context.** The workload is small: about 30k worklog rows a quarter, a few hundred users, a few thousand CRM rows. Fabric offers Spark notebooks and Python notebooks. Python notebooks run a single-node Python kernel with `deltalake`, DuckDB, pandas and pyarrow preinstalled. The project also had to run on a Windows laptop with no Java, Docker or WSL, so a reviewer can clone it and see the tables.

**Decision.** Every notebook in the daily pipelines is a Fabric **Python notebook** (`jupyter_python` kernel). Transformations are DuckDB SQL over Arrow; reads and writes go through `deltalake` (delta-rs), including `MERGE` for SCD2. A local runner executes the same `notebook-content.py` files with a `notebookutils` stand-in.

**Consequences.**
- Sessions start in seconds and use a fraction of the capacity units a Spark pool would. That matters on an F2/F4 capacity shared with reports.
- The Python path runs on the laptop, in CI and in Fabric; the Spark path runs in CI and Fabric.
- The SQL is close to Spark SQL, and [ADR 0004](0004-spark-shadow-path.md) carries the core logic over to PySpark, proven identical in CI.
- Next, for a genuinely big source: Spark tuning (partitioning, `OPTIMIZE`/`VACUUM` schedules, V-Order).
