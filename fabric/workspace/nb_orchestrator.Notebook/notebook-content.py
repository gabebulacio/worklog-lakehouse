# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Orchestrator
# Runs the medallion sequence **by notebook name** (no item GUIDs, so it is portable across
# workspaces) under one `run_id` that every table's lineage carries:
#
# `nb_land_synthetic` (demo only) -> `nb_bronze_ingest` -> `nb_silver_users` -> `nb_silver_worklogs` -> `nb_gold` -> `nb_quality`
#
# Each step is logged to `ops_run_log` (status, seconds, the step's exit payload). If any
# step fails, or the quality gate reports an error-level check, the run fails, so the
# calling pipeline goes red and alerts. Don't start this while another run is writing:
# Delta retries commit conflicts, but two runs would interleave their extracts.
#
# Modes: `incremental` (daily: person-days changed in the last `updated_since_hours`) and
# `full` (monthly: everything in the trailing `FullWindowMonths`, or `date_from`..`date_to`
# for a backfill).

# PARAMETERS CELL ********************

mode = "incremental"
run_date = ""             # YYYY-MM-DD; default today
date_from = ""            # full-mode window override (backfills)
date_to = ""
updated_since_hours = 48
land_synthetic = True     # demo source systems; False when Copy jobs land the extracts
timeout_per_step = 1800

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- shared bootstrap: source of truth is fabric/shared/bootstrap.py (python run.py sync) ---
# Same cell in every notebook so each one also runs on its own. Works on Fabric (abfss://
# OneLake paths + storage token) and locally (plain folders, see local/fabric_local.py).
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import duckdb
import notebookutils
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

VARLIB = "wl_settings"


def var(name, default=None):
    """A wl_settings variable; falls back to an environment variable, then the default."""
    try:
        value = notebookutils.variableLibrary.get(f"$(/**/{VARLIB}/{name})")
        if value not in (None, ""):
            return value
    except Exception:
        pass
    return os.getenv(name, default)


LAKEHOUSE = var("LakehouseName", "wl_lakehouse")
BASE = notebookutils.lakehouse.getWithProperties(LAKEHOUSE)["properties"]["abfsPath"]
ON_FABRIC = BASE.startswith("abfss://")
TABLES = f"{BASE}/Tables"
STORAGE = ({"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}
           if ON_FABRIC else None)
MOUNT = "/wl"
try:
    notebookutils.fs.mount(BASE, MOUNT)
except Exception:
    pass  # already mounted in this session
FILES = os.path.join(notebookutils.fs.getMountPath(MOUNT), "Files")


def table_exists(name):
    try:
        DeltaTable(f"{TABLES}/{name}", storage_options=STORAGE)
        return True
    except Exception:
        return False


def read_table(name):
    """Delta table as a pyarrow Table, or None if it does not exist yet."""
    try:
        return DeltaTable(f"{TABLES}/{name}", storage_options=STORAGE).to_pyarrow_table()
    except Exception as exc:
        if "not found" in str(exc).lower() or "no files in log" in str(exc).lower() or type(exc).__name__ == "TableNotFoundError":
            return None
        raise


def _to_arrow(data):
    """pandas / DuckDB relation / Arrow -> Arrow, with naive timestamps stamped as UTC."""
    if isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data, preserve_index=False)
    elif not isinstance(data, pa.Table):
        data = data.to_arrow_table() if hasattr(data, "to_arrow_table") else data.fetch_arrow_table()
    fields = []
    for f in data.schema:
        if pa.types.is_timestamp(f.type):
            f = f.with_type(pa.timestamp("us", tz="UTC"))
        elif pa.types.is_large_string(f.type):
            f = f.with_type(pa.string())
        fields.append(f)
    return data.cast(pa.schema(fields))


def write_table(data, name, mode="overwrite", attempts=4):
    """Write to a Delta table. Retries commit conflicts (concurrent writers); writes are idempotent."""
    arrow = _to_arrow(data)
    kwargs = {"mode": mode, "storage_options": STORAGE}
    kwargs["schema_mode"] = "merge" if mode == "append" else "overwrite"
    for attempt in range(1, attempts + 1):
        try:
            write_deltalake(f"{TABLES}/{name}", arrow, **kwargs)
            break
        except Exception as exc:
            conflict = "commit" in str(exc).lower() and "fail" in str(exc).lower()
            if not conflict or attempt == attempts:
                raise
            time.sleep(5 * attempt)
    print(f"{name}: {arrow.num_rows:,} rows ({mode})")
    return arrow.num_rows


db = duckdb.connect()


def register(**tables):
    """Expose Delta tables (by name) or in-memory frames to DuckDB SQL under the given aliases."""
    for alias, source in tables.items():
        if isinstance(source, str):
            arrow = read_table(source)
            if arrow is None:
                raise RuntimeError(f"table {source} does not exist yet - run the upstream notebook first")
            source = arrow
        db.register(alias, source)


def sql(query):
    """Run DuckDB SQL; returns a pyarrow Table."""
    rel = db.sql(query)
    return rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table()


def scalar(query):
    return db.sql(query).fetchone()[0]


def secret(name, default=""):
    """Credential from Key Vault (KeyVaultUri); environment variable locally."""
    vault = var("KeyVaultUri", "")
    if vault:
        return notebookutils.credentials.getSecret(vault, name)
    return os.getenv(name, default)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import uuid

if mode not in ("incremental", "full"):
    raise ValueError(f"mode must be incremental or full, not {mode!r}")
run_id = uuid.uuid4().hex[:12]
day = run_date or str(date.today())
print(f"run_id={run_id} mode={mode} run_date={day}")

log = []


def step(name, params):
    started = datetime.now(timezone.utc)
    status, payload = "succeeded", {}
    try:
        out = notebookutils.notebook.run(name, int(timeout_per_step), params)
        payload = json.loads(out) if out else {}
    except Exception as exc:
        status, payload = "failed", {"error": str(exc)[:2000]}
    ended = datetime.now(timezone.utc)
    log.append({"run_id": run_id, "mode": mode, "run_date": day, "step": name, "status": status,
                "started_at": started, "ended_at": ended,
                "seconds": round((ended - started).total_seconds(), 2), "detail": json.dumps(payload)})
    print(f"{name}: {status} {payload}")
    return status, payload


steps = []
if land_synthetic:
    steps.append(("nb_land_synthetic", {"run_id": run_id, "run_date": day, "mode": mode, "date_from": date_from,
                                        "date_to": date_to, "updated_since_hours": updated_since_hours}))
steps += [
    ("nb_bronze_ingest", {"run_id": run_id}),
    ("nb_silver_users", {"run_id": run_id}),
    ("nb_silver_worklogs", {"run_id": run_id}),
    ("nb_gold", {"run_id": run_id, "run_date": day}),
    ("nb_quality", {"run_id": run_id, "run_date": day}),
]

failure = None
for name, params in steps:
    status, payload = step(name, params)
    if status == "failed":
        failure = f"{name} failed: {payload['error']}"
        break
    if name == "nb_quality" and payload.get("errors"):
        failure = f"quality gate failed: {payload['errors']}"
        log[-1]["status"] = "failed"

write_table(pd.DataFrame(log), "ops_run_log", mode="append")
if failure:
    raise RuntimeError(failure)
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "mode": mode, "run_date": day,
                                        "steps": {r["step"]: r["status"] for r in log}}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
