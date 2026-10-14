# 0002 · Recompute silver worklogs with the person-day rule

**Status:** accepted · 2026-10-14

**Context.** Both time sources let people edit and delete entries long after the work date. The APIs only answer "what changed since X", and they can't report a day whose entries were all deleted, because nothing is left to return. A classic incremental `MERGE` on entry id handles edits but not deletions, and its result depends on the order and batching of past runs.

**Decision.** Extracts are complete **person-days** (`source, worker_id, date`): incremental extracts carry every changed person-day in full, and the monthly full extract covers a date window. `silver_worklogs` is recomputed from bronze on every run with one rule: for each person-day, keep the rows of the latest extract that contains it, unless a later full extract covers that date without it. The result is written in one atomic overwrite.

**Consequences.**
- Edits and partial deletions are handled by the daily run. A whole deleted day disappears at the next monthly full run. That is the only gap, and it is documented and tested (`test_incremental_history_equals_one_full_extract_except_deleted_days`).
- Silver is a pure function of bronze, so replaying the landing archive in any batching gives the same table, and backfills are just another full extract.
- Recomputing costs about a second at this volume. Past a few million rows, restrict the recompute to person-days touched by the run and apply it with `MERGE`, using the same rule.
