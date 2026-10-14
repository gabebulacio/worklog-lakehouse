---
name: lakehouse-modeling
description: Add or change data in the Worklog Lakehouse medallion - a new source system or entity, a column, a silver table, a gold dimension/fact/mart, an SCD2 attribute, an identity-matching rule or a quality check - following its layer contracts (bronze raw + lineage, silver canonical and replayable, gold star with stable keys). Use for "add a source", "new table", "new KPI/mart", "track history of X", "star schema", "data model", or Power BI model questions on this project.
---

# Medallion modeling: Worklog Lakehouse

Read the project README's "How it works" table and the ADRs in `docs/decisions/` first. Notebook mechanics (format, bootstrap, SQL pitfalls) are in the `fabric-notebooks` skill.

## Layer contracts
| Layer | Contract | Never |
|---|---|---|
| Landing `Files/landing/<system>/<entity>/<ts>_<type>_<run_id>.json` | envelope `{system, entity, extract_type: full\|incremental\|snapshot, extracted_at, window_from, window_to, updated_since, records}` | edit or delete landed files (they're the replay archive) |
| Bronze `bronze_<system>_<entity>` | append-only, payload as strings, lineage `_run_id _source_file _extract_type _extracted_at _ingested_at`; written by the generic `nb_bronze_ingest` | type, filter or dedupe here |
| Silver `silver_*` | typed, canonical, deduplicated; a **pure function of bronze** (+ the mapping file) so replay is safe | depend on run order or on the previous silver (exception: SCD2 history, registry) |
| Gold `gold_dim_* / gold_fact_* / gold_mart_* / gold_audit_*` | star schema for Direct Lake: lean facts (keys + measures + degenerate ids), conformed dims with unknown members (`UNKNOWN`, `-1`), marts pre-aggregated for tiles | put descriptive attributes on facts; drop rows that don't match (attribute them to the unknown member) |
| Ops `ops_*` | manifest, run log, quality results; append-only | |

## Recipes
**New source entity.** Emit its envelope in `nb_land_synthetic` (synthetic quirks on *fixed* people or rows, not probabilities, so tests can rely on them). Bronze picks it up automatically. Then type it in a silver notebook, and if it's a snapshot, read the latest snapshot as one file.

**New column end to end.** Add it to the generator's record → it appears in bronze (schema merge) → cast and name it in silver → expose it in gold only if a report needs it. If it's a tracked user attribute, add it to the `row_hash` list in `nb_silver_users` so SCD2 versions on it.

**New dimension/fact.** Grain first: write it in the `nb_gold` header table. Add the unknown member, FK checks in `nb_quality` (`select count(*) from fact anti join dim using (key)`), and a reconciliation check if it can fan out.

**Stable keys.** Anything finance or reports reference (project keys) goes through an append-only registry: new natural key → `max + 1`, appended, never renumbered. `registry_keys_stable` compares with the previous Delta version.

**Identity matching.** Exact identifiers go in `silver_user_identities` (with a `kind` and `priority`). Guessing fallbacks live in `nb_gold` and only run against `guessable` (users with unique names). A new fallback must keep that guard and get a test showing it can't misattribute.

**New quality check.** One tuple in `CHECKS` in `nb_quality`: `(name, "error"|"warn", SQL returning a number, max_allowed)`. Use error for anything that makes the report wrong, warn for things a human should look at.

## Power BI model (Direct Lake)
Relationships: `gold_fact_worklog[user_key|project_key|date]` → `gold_dim_user`, `gold_dim_project`, `gold_dim_date` (mark as date table); `gold_fact_assignment_month[user_key|deliverable_id]` → `gold_dim_user`, `gold_dim_deliverable`. Marts feed tiles directly. For the semantic model and report themselves, use the `power-bi-modeling`, `power-bi-dax` and `power-bi-report` skills.

After any change: `python run.py demo` then `python run.py test`.

## Semantic model and report
The Power BI model is defined once in `local/build_model.py` (tables with friendly names, relationships, measures with description, format and display folder) and generated as Direct Lake TMDL into `fabric/workspace/wl_model.SemanticModel` (`python run.py model`). Never hand-edit the TMDL: `tests/test_model.py` fails on drift. A measure name can't equal a column name in the same table (rename the hidden column, e.g. `Worklog[Billable]`).

To verify DAX: `python run.py model --import`, ask the user to open a **blank** Power BI Desktop, find its engine port (the `msmdsrv.exe` child of that `PBIDesktop` process), `pbi connect -d localhost:<port> --name ...`, `pbi database import-tmdl .lake/model_import/definition`, refresh each table (`pbi table refresh <t> --type Full`), then `python local/verify_model.py`. Add a DuckDB counterpart there for every new measure. Load the `power-bi-modeling`, `power-bi-dax` and `power-bi-report` skills first.

The report `fabric/workspace/wl_report.Report` (PBIR) is edited with `pbi report` / `pbi visual` (`--no-sync`); bind measures with `--value`/`--field` and columns with `--row`/`--category` (table visuals treat `--value` as measures, so use matrices for column lists). `tests/test_report.py` checks every bound field exists in the model.
