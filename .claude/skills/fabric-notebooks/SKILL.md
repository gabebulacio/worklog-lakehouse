---
name: fabric-notebooks
description: Author or change Microsoft Fabric items in Git format - notebook-content.py notebooks (cells, parameters cell, shared bootstrap), .platform files, Data Pipelines, the Variable Library and parameter.yml - so they still deploy to Fabric AND run locally. Use when creating or editing anything under fabric/, adding a notebook or pipeline, or when a notebook fails locally with a SQL/Delta error.
---

# Fabric items (Git format)

Reference implementation: `fabric/workspace/`. Each item is a folder `<displayName>.<Type>/` with a `.platform` file. Read an existing notebook before writing a new one, and copy its shape.

## Notebook file format (`notebook-content.py`)
```
# Fabric notebook source

# METADATA ********************            kernel_info {"name": "jupyter"} -> Python notebook
# MARKDOWN ********************            first cell: what it reads, what it writes, the rules (tables are fine)
# PARAMETERS CELL ********************      exactly one; pipelines and nb.run() bind to these names; typed defaults
# METADATA ********************            {"language": "python", "language_group": "jupyter_python"} after every code cell
# CELL ********************                 2nd code cell = the shared bootstrap (placeholder line, then `python run.py sync`)
# CELL ********************                 the work
```
- Markers must be exact: `# CELL ********************` (20 asterisks). `local/fabric_local.py` splits on them.
- Parameter types come from the defaults (`48` int, `True` bool, `""` string). The local runner coerces string arguments the way Fabric does.
- End with `notebookutils.notebook.exit(json.dumps({...}))`: the orchestrator logs it to `ops_run_log`.

## Shared bootstrap: never edit it inside a notebook
Edit `fabric/shared/bootstrap.py`, then run `python run.py sync`. `test_bootstrap_cell_matches_shared_source` fails on drift. It gives every notebook: `var(name, default)` (Variable Library → env → default), `TABLES`/`FILES`/`STORAGE`, `read_table`, `write_table(data, name, mode)` (Arrow/pandas/DuckDB; naive timestamps → UTC; retries commit conflicts), `table_exists`, `register(alias="table", ...)`, `sql(query)` → Arrow, `scalar`, `secret(name)` (Key Vault).

Only use the `notebookutils` calls the local stand-in implements: `lakehouse.getWithProperties`, `fs.mount/getMountPath`, `variableLibrary.get`, `notebook.run/exit`, `credentials.getToken/getSecret`. A new one needs adding to `local/fabric_local.py` too.

## DuckDB SQL rules (learned the hard way)
- **Always write `as` for column aliases.** `identity`, `source`, `date`, `month`, `year`… are keywords; an implicit alias is a parser error.
- The latest snapshot of a bronze table means **one file**: `where _source_file = (select _source_file from t order by _extracted_at desc, _source_file desc limit 1)`. `= max(_extracted_at)` matches several files when two runs share a timestamp, and fans out every join.
- Bronze columns are strings: cast in silver (`cast(x as date)`, `try_strptime` for messy timestamps).
- Keep SQL portable to Spark SQL where it's free (CTEs, window functions, `qualify`). Note DuckDB-only syntax (`anti join`, `exclude`, `distinct on`) when you add it.

## Writes
Use `write_table`, not raw `write_deltalake`. Overwrite for rebuilt tables (silver/gold), `mode="append"` for bronze, ops logs and the project registry. SCD2 and upserts use `DeltaTable(...).merge(...)` (see `nb_silver_users`). Never rewrite append-only tables.

## New notebook checklist
1. `<name>.Notebook/.platform` with type, displayName and description, and a **new** `logicalId` (`python -c "import uuid; print(uuid.uuid4())"`).
2. `notebook-content.py` in the format above, then `python run.py sync`.
3. Add it to `steps` in `nb_orchestrator` in dependency order (`test_orchestrator_runs_every_notebook`).
4. Add quality checks for its tables to `nb_quality` and an end-to-end assertion in `tests/test_pipeline.py`.
5. Document its tables in the project README.

## Pipelines and settings
- `*.DataPipeline/pipeline-content.json`: `TridentNotebook` activities reference the **sentinel** IDs `1111…`/`2222…`, which `parameter.yml` swaps per environment at deploy. Pipeline parameters must exist in the orchestrator's parameters cell (tested).
- `wl_settings.VariableLibrary/variables.json`: settings only. **Never a secret**; credentials go through `secret()` and Key Vault (tested). A new variable needs `name`, `note`, `type` (`String|Integer|Number|Boolean`) and `value`.

Finish with `python run.py test` (the `lakehouse-run` skill covers reading failures).
