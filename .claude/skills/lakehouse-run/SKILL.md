---
name: lakehouse-run
description: Run, inspect and troubleshoot the Worklog Lakehouse locally - demo, daily/full runs, single notebooks, querying Delta tables, reading quality results and the unmatched-worker worklist. Use when the user wants to run the Fabric medallion, see a table, check data, debug a failed run or quality check, or demo the project.
---

# Run the Worklog Lakehouse locally

Everything goes through `run.py`, which re-executes itself inside the project's `.venv`. It runs the **real** Fabric notebooks from `fabric/workspace/` through `local/fabric_local.py`. Lakehouse: `.lake/wl_lakehouse/{Tables,Files}`.

```
python run.py setup            # once: .venv + requirements.txt
python run.py demo             # reset, full backfill as of 2026-09-30, then 7 daily runs (~90 s)
python run.py day              # next daily incremental run (the day after the last run_date)
python run.py full             # monthly full run (trailing FullWindowMonths) as of the last run_date
python run.py nb nb_gold run_date=2026-10-07     # one notebook with parameters
python run.py tables           # row counts
python run.py sql "select ..."  # DuckDB over every Delta table, by table name
```
`run.py demo`/`seed` **delete** `.lake/`. Say so before running them over a lakehouse the user has been editing (e.g. their `Files/config/user_mapping.csv`).

On Windows, set `PYTHONIOENCODING=utf-8` if output is piped (run.py reconfigures stdout itself).

## Reading a run
- `ops_run_log`: one row per step per run (`status`, `seconds`, `detail` = the notebook's exit JSON). `select run_date, step, status, seconds from ops_run_log order by started_at desc limit 12`
- `ops_quality_results`: one row per check per run. Errors fail the orchestrator (the run raises); warnings don't. `select check_name, severity, value, max_allowed from ops_quality_results where not passed order by checked_at desc`
- `gold_audit_unmatched_workers`: worklogs no user was found for. The fix is a row in `Files/config/user_mapping.csv` (`email,tracker_key,alt_emails,is_timesheet_exempt,employee_id,notes`), then the next run. Never "fix" attribution by loosening the fallback rules in `nb_gold`.

## When a check fails
| Check | Usual cause |
|---|---|
| `fact_rows_match_silver` / `fact_hours_match_silver` | a gold join fans out: a dimension or snapshot has duplicate keys (look at "latest snapshot" selections, which must pick **one file**) |
| `fact_*_fk` | a dimension lost its unknown member (`UNKNOWN` / `-1`) or a key changed type |
| `registry_keys_stable` | something rewrote `silver_project_registry` instead of appending |
| `scd2_*` | the MERGE in `nb_silver_users` ran with a staged set that wasn't deduplicated per `user_key` |
| `landing_files_unprocessed` | `nb_bronze_ingest` failed partway; rerun it (ingestion is at-least-once, silver dedupes) |
| `unmatched_hours_share` (warn) | new identities to map; see the audit table |

Debug a single layer with `python run.py nb <notebook> run_id=debug ...`. Notebooks print a line per table they write and exit with a JSON summary.

## Tests
`python run.py test` = pytest (static item checks + end-to-end on temp lakehouses, ~80 s) + ruff. The end-to-end suite asserts replay equivalence (daily incrementals ≈ one full extract except whole deleted days), idempotent bronze, stable registry keys, no-op SCD2 reruns, and that ambiguous names are never guessed.

## Spark shadow path
`pl_spark_shadow` (in Fabric) runs `nb_spark_silver_users` → `nb_spark_silver_worklogs` → `nb_spark_gold_fact` → `nb_parity` after a primary run. `ops_parity_results` has one row per table pair per run (`only_python`, `only_spark`, `equal`). Locally `tests/test_spark_parity.py` skips without Java; CI's `spark-parity` job runs it. To debug a parity failure, compare the pair in DuckDB: `python run.py sql "select * from silver_worklogs except select * from spark_silver_worklogs"` (pick the compared columns from `PAIRS` in `nb_parity`). That only works on a lakehouse where the Spark notebooks have run, i.e. in Fabric or a Linux machine with Java.
