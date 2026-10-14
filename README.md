# Worklog Lakehouse: a Microsoft Fabric medallion you can run on a laptop

[![ci](https://github.com/gabebulacio/worklog-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/gabebulacio/worklog-lakehouse/actions/workflows/ci.yml) · [Case study](https://gabrielbulacio.com/projects/worklog-lakehouse)

> Rebuilt from scratch, on synthetic data, from problems I solved running a production timesheet pipeline: identity resolution across systems, late edits and deletions, keys finance relies on. None of the original code, data or names are here. The company is fictional, and every person, client and email is synthetic.

**Two time-tracking systems, a user directory and a CRM, consolidated into a governed star schema for Power BI: who logged what, against which client, compared with what was sold.** Built as Fabric items (Lakehouse, Notebooks, Data Pipelines, Variable Library) in Git format, deployable with `fabric-cicd`. The **same notebook files** also run locally against a folder-based Delta lakehouse, so you can clone it and see every table in about two minutes.

```
 SOURCES (synthetic APIs)              BRONZE (append-only, raw)        SILVER (canonical)                 GOLD (star + marts)
 timesheet_app  timesheets ──┐         bronze_timesheet_app_*           silver_worklogs ─────────────┐     gold_fact_worklog
                users ───────┤         bronze_issue_tracker_*            latest-extract-per-person-day│     gold_fact_assignment_month
 issue_tracker  worklogs ────┤ land ─▶ bronze_directory_users      ─▶   silver_user_history (SCD2)  ├──▶  gold_dim_user · _project · _deliverable · _date
                users ───────┤         bronze_crm_*                     silver_user_identities       │     gold_mart_sold_vs_utilized · _coverage_month
 directory      users ───────┤         ops_ingested_files (manifest)    silver_project_registry ─────┘     gold_mart_worker_month · _nonloggers
 crm            4 entities ──┘                                           (append-only keys)                 gold_audit_unmatched_workers
                                                                                                           ops_quality_results · ops_run_log
 pl_daily (incremental) / pl_monthly_full ──▶ nb_orchestrator ──▶ land → bronze → silver users → silver worklogs → gold → quality gate
```

## Run it locally

Python 3.11+ only. No Java, Spark, Docker or Fabric capacity needed.

```
python run.py setup      # .venv + deltalake, duckdb, pandas, pyarrow
python run.py demo       # backfill 2026 (full run), then 7 daily incremental runs, ~90 s
python run.py tables     # row counts of all 28 tables
python run.py sql "select * from gold_mart_coverage_month"
python run.py test       # item checks + end-to-end runs + ruff (Spark parity runs in CI)
```

Other commands: `python run.py day` (next daily run), `full` (monthly full run), `nb <notebook> k=v` (one notebook), `export` (.ipynb copies for manual import). The lakehouse is `.lake/wl_lakehouse/` with `Tables/` (Delta) and `Files/` (landing, config), the same layout as OneLake.

### Try the operator loop

After `demo`, the quality gate raises one warning: **2.2% of hours can't be attributed** (`unmatched_hours_share`). The worklist explains why:

```
python run.py sql "select * from gold_audit_unmatched_workers"
```

One row is a contractor whose tracker account was deleted. The other is `romero.nicolas@…`, a swapped login. The pipeline *could* have guessed `nicolas.romero@…`, but there are two Nicolas Romeros, so the match is ambiguous and goes to review instead. Tell it who it is by adding this line to `.lake/wl_lakehouse/Files/config/user_mapping.csv` (in Fabric: the same path in the lakehouse's Files):

```
nicolas.romero2@lumen.studio,,romero.nicolas@lumen.studio,0,,timesheet login is last.first - confirmed
```

Then `python run.py day`: the warning clears and those 376 entries are attributed by email.

## How it works

| Layer | Notebook | What it does |
|---|---|---|
| Land | `nb_land_synthetic` | Writes what each source API would return for this run: the timesheet SaaS, the issue tracker, the Entra-shaped directory and the CRM. The synthetic world is deterministic and changes over time: people join and leave, change departments, log late, edit and delete entries. In production, Copy jobs land the same envelopes and this notebook is switched off (`land_synthetic=false`). |
| Bronze | `nb_bronze_ingest` | One generic notebook: every unprocessed landing file → `bronze_<system>_<entity>`, append-only, all columns as strings, with lineage (`_run_id`, `_source_file`, `_extract_type`, `_extracted_at`, `_ingested_at`). `ops_ingested_files` is the manifest and keeps each extract's window. |
| Silver | `nb_silver_users` | Directory snapshot + the hand-maintained `user_mapping.csv` → `silver_user_history` (**SCD Type 2 in a single Delta MERGE**) and `silver_user_identities` (email, UPN, alternate emails, tracker keys, unique names → `user_key`). Ambiguous identifiers go to `silver_identity_conflicts`, never into the map. |
| Silver | `nb_silver_worklogs` | Both sources in one schema. For each person-day, keep the latest extract that contains it, unless a later full extract covers that date without it. Fixes source quirks: blank emails of deactivated users (backfilled), three timestamp formats, opaque tracker keys, overlapping API pages. |
| Gold | `nb_gold` | Append-only project registry (keys never change) → star schema with unknown members, CRM-based sold vs utilized (calendar-day proration × allocation), coverage, logging delay, non-loggers and the unmatched worklist. |
| Quality | `nb_quality` | 18 SQL checks per run → `ops_quality_results`: uniqueness, ranges, fact ↔ silver reconciliation, foreign keys, SCD2 invariants, registry stability across Delta versions (time travel), unprocessed files, attribution share, coverage. Any **error** fails the run. |
| Orchestration | `nb_orchestrator`, `pl_daily`, `pl_monthly_full` | One `run_id` per run, steps called by notebook name, each step logged to `ops_run_log`. |
| Spark shadow | `nb_spark_silver_worklogs`, `nb_spark_silver_users`, `nb_spark_gold_fact`, `nb_parity`, `pl_spark_shadow` | PySpark / Spark SQL ports of the person-day rule, the SCD2 `MERGE` and the matching rules, writing `spark_*` tables; `nb_parity` fails the run if they differ from the Python path in any row. |

Design decisions are in [docs/decisions/](docs/decisions/):
- [0001](docs/decisions/0001-python-notebooks-not-spark.md): Python notebooks (DuckDB + delta-rs), not Spark
- [0002](docs/decisions/0002-silver-recompute-person-day-rule.md): recompute silver with the person-day rule instead of an incremental MERGE
- [0003](docs/decisions/0003-clean-room-synthetic-rebuild.md): clean-room rebuild on synthetic sources
- [0004](docs/decisions/0004-spark-shadow-path.md): a Spark shadow path, proven equal in CI

### Identity resolution: attribution order

1. `worker_email` against the identity map (directory mail, UPN, mapping-file alternate emails)
2. the source's own id (tracker key in the mapping file)
3. `localpart@DefaultDomain` (legacy and external domains)
4. swapped `last.first@DefaultDomain`

Rules 3 and 4 are guesses, so they only apply when the candidate's name is unique. Everything else stays in the fact as `UNKNOWN`, so totals always reconcile with silver, and goes to `gold_audit_unmatched_workers`.

| Matched by | Share of hours (demo) |
|---|---|
| email | 85.9% |
| default domain | 5.4% |
| swapped name | 4.4% |
| tracker key (mapping file) | 2.2% |
| unmatched → worklist | 2.2% |

## Deploy to Fabric

```
python -m pip install fabric-cicd azure-identity
python fabric/deploy.py --workspace-id <GUID> --environment dev
```

This publishes the lakehouse, variable library, 11 notebooks (7 Python, 3 Spark, plus `nb_parity`) and 3 pipelines in place, matched by `logicalId`. `parameter.yml` swaps the pipelines' sentinel IDs for the real orchestrator and workspace IDs per environment, so nobody re-links activities by hand. Then run `pl_monthly_full` once with `date_from=2026-01-01` to backfill, and schedule `pl_daily`.

- **No secrets in Git.** The variable library holds settings only (a test enforces it). Source credentials come from Key Vault via `KeyVaultUri` and `notebookutils.credentials.getSecret`.
- **No default-lakehouse binding.** Notebooks resolve the lakehouse by name at runtime, so a Git sync into any workspace works unchanged.
- **Power BI.** Build a Direct Lake semantic model on the `gold_` tables: `fact_worklog` → `dim_user`, `dim_project`, `dim_date` (mark as date table); `fact_assignment_month` → `dim_user`, `dim_deliverable`.

### Spark shadow path

The primary path is Python notebooks ([ADR 0001](docs/decisions/0001-python-notebooks-not-spark.md)). The logic that matters is also ported to Spark ([ADR 0004](docs/decisions/0004-spark-shadow-path.md)), and the CI job `spark-parity` proves the two produce identical tables: Java 17, Spark 3.5 and Delta 3.2, the Fabric Runtime 1.3 versions, over a backfill plus four daily runs. In Fabric, run `pl_spark_shadow` after a primary run. To run them locally, install Java 17 and `pip install -r requirements-spark.txt`; `python run.py test` then includes the parity test.

## Repo layout

```
fabric/workspace/      Fabric Git-format items (what gets deployed)
fabric/shared/         bootstrap.py: the setup cell shared by every notebook (python run.py sync copies it)
fabric/deploy.py       fabric-cicd publish
local/fabric_local.py  runs notebook-content.py files locally: cell parser + notebookutils stand-in
tests/                 item checks (Git/deploy breakers) + end-to-end runs on a temp lakehouse
docs/decisions/        ADRs
run.py                 local command line
```
