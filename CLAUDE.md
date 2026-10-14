# CLAUDE.md

Worklog Lakehouse: a Microsoft Fabric medallion (bronze → silver → gold) in Fabric Git format, deployable with fabric-cicd, whose notebooks also run locally against a folder-based Delta lakehouse. Public portfolio project by Gabriel Bulacio; the case study lives on gabrielbulacio.com/projects/worklog-lakehouse (in the `campo-analytics` repo, which hosts the site).

## Layout

| Path | What |
|---|---|
| `fabric/workspace/` | The Fabric items that get deployed: lakehouse, variable library, 8 Python notebooks, 3 Spark notebooks, 3 pipelines, `parameter.yml` |
| `fabric/shared/bootstrap.py`, `bootstrap_spark.py` | Setup cells shared by the Python and the Spark notebooks; `python run.py sync` copies each in |
| `fabric/deploy.py` | fabric-cicd publish |
| `local/fabric_local.py` | Runs `notebook-content.py` files locally (cell parser + `notebookutils` stand-in) |
| `tests/` | Item checks (what breaks a Git sync or deploy) + end-to-end runs on temporary lakehouses |
| `docs/decisions/` | ADRs, `NNNN-kebab-title.md`: Status · Context · Decision · Consequences |

## Commands

```
python run.py setup | demo | day | full | tables | sql "…" | nb <notebook> k=v | sync | test | export
```
`run.py` re-executes inside `.venv`. `demo`/`seed` delete `.lake/`.

## Skills

`lakehouse-run` (operate, debug, read quality results), `fabric-notebooks` (item format, bootstrap, DuckDB pitfalls), `lakehouse-modeling` (layer contracts, recipes), `fabric-deploy` (publish to a workspace; only when asked).

## Rules

- The files in `fabric/workspace/` are what deploys. Never edit a notebook's bootstrap cell: edit `fabric/shared/bootstrap.py`, then `python run.py sync`.
- Never put a secret in the variable library; credentials come from Key Vault through `secret()`.
- **Clean room.** This rebuilds kinds of problems from past employer work on synthetic data. Never add copied code, names, emails, tenant/app IDs, client names or real figures. New source quirks go in `nb_land_synthetic`, on fixed people or rows, not probabilities.
- Notebooks must run on Python 3.11, the Fabric runtime and the version CI uses.
- **Spark shadow path** (`nb_spark_*`, `nb_parity`, `pl_spark_shadow`): any change to the logic in a Python silver notebook or the fact attribution needs the same change in its Spark port. This machine can't run Spark; the CI job `spark-parity` is the proof, so say so when a Spark change hasn't been through CI yet.
- **Done means `python run.py test` passes** (35+ tests and ruff), the same as CI.
- Commit messages: one plain sentence describing the change, no `feat:` prefixes.
