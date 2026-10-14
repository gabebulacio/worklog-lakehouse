# 0004 · A Spark shadow path, proven equal in CI

**Status:** accepted · 2026-10-14

**Context.** [ADR 0001](0001-python-notebooks-not-spark.md) chose Python notebooks because the volume doesn't need a cluster. That stays true, but most Fabric teams run Spark, and "the SQL ports to Spark" is worth more shown than said.

**Decision.** Port the parts with real logic to PySpark and Spark SQL as Fabric Spark notebooks:
- `nb_spark_silver_worklogs`: the person-day rule
- `nb_spark_silver_users`: SCD2 with the Delta Lake `MERGE` API, plus the identity map
- `nb_spark_gold_fact`: the ranked matching rules

They read the same bronze tables and write `spark_*` tables. `nb_parity` compares each one with its Python counterpart in both directions and fails on any difference. `pl_spark_shadow` runs them after a primary run. In CI, the `spark-parity` job (Java 17, Spark 3.5, Delta 3.2: the Fabric Runtime 1.3 versions) runs the whole sequence over five runs and requires zero differences.

**Consequences.**
- Switching engines later is a cut-over, not a rewrite: run both, compare, switch.
- Dimensions, marts and the project registry stay on the primary path. The shadow fact points at the same keys, which is what makes a row-by-row comparison possible.
- What the port changed: `distinct on` and `qualify` became `row_number()` windows, `exclude` became `.drop()`, and `try_strptime` became `to_timestamp` with the CORRECTED parser policy (unparseable becomes NULL). Each engine hashes SCD2 rows its own way, so `row_hash` isn't compared.
- Day-to-day development stays on the fast Python path; the `spark-parity` job runs the Spark notebooks on every push and pull request.
