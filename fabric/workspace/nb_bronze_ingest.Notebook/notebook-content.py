# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Bronze: ingest landed extracts
# Every JSON envelope under `Files/landing/<system>/<entity>/` that is not yet in
# `ops_ingested_files` becomes rows in `bronze_<system>_<entity>`:
#
# - **append-only**, one commit per table per run;
# - **all payload columns as strings** (nested objects flattened with `_`), so a source
#   that changes a type or sends an all-null column never breaks the load; silver types them;
# - **lineage on every row**: `_run_id`, `_source_file`, `_extract_type`, `_extracted_at`, `_ingested_at`.
#
# The manifest also keeps each extract's window (`window_from`/`window_to`), because an
# empty full extract still carries information: nothing was logged in that window.
# Ingestion is at-least-once (a crash between the bronze commit and the manifest commit
# re-ingests that file); silver deduplicates on `(source, external_id, _source_file)`.
# The landing files stay where they are: they are the replay archive.

# PARAMETERS CELL ********************

run_id = "manual"

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

def as_text(value):
    if value is None or (isinstance(value, float) and value != value):
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


landing = os.path.join(FILES, "landing")
found = []
for root, _, names in os.walk(landing):
    for name in sorted(names):
        if name.endswith(".json"):
            full = os.path.join(root, name)
            found.append(os.path.relpath(full, FILES).replace("\\", "/"))
found.sort()

manifest = read_table("ops_ingested_files")
done = set(manifest.column("source_file").to_pylist()) if manifest is not None else set()
todo = [f for f in found if f not in done]
print(f"landing files: {len(found)}, new: {len(todo)}")

now = datetime.now(timezone.utc)
batches, manifest_rows = {}, []
for rel in todo:
    with open(os.path.join(FILES, rel), encoding="utf-8") as fh:
        env = json.load(fh)
    table = f"bronze_{env['system']}_{env['entity']}"
    records = env.get("records") or []
    if records:
        frame = pd.json_normalize(records, sep="_")
        frame = frame.map(as_text).astype("string") if hasattr(frame, "map") else frame.applymap(as_text).astype("string")
        frame["_run_id"] = run_id
        frame["_source_file"] = rel
        frame["_extract_type"] = env["extract_type"]
        frame["_extracted_at"] = pd.Timestamp(env["extracted_at"]).tz_convert("UTC")
        frame["_ingested_at"] = pd.Timestamp(now)
        batches.setdefault(table, []).append(frame)
    manifest_rows.append({
        "source_file": rel, "system": env["system"], "entity": env["entity"], "table_name": table,
        "extract_type": env["extract_type"], "extracted_at": pd.Timestamp(env["extracted_at"]).tz_convert("UTC"),
        "window_from": pd.to_datetime(env.get("window_from")).date() if env.get("window_from") else None,
        "window_to": pd.to_datetime(env.get("window_to")).date() if env.get("window_to") else None,
        "records": len(records), "run_id": run_id, "ingested_at": pd.Timestamp(now),
    })

rows = {}
for table, frames in sorted(batches.items()):
    frame = pd.concat(frames, ignore_index=True)
    for c in frame.columns:
        if not c.startswith("_"):
            frame[c] = frame[c].astype("string")
    rows[table] = write_table(frame, table, mode="append")

if manifest_rows:
    m = pd.DataFrame(manifest_rows)
    schema = pa.schema([("source_file", pa.string()), ("system", pa.string()), ("entity", pa.string()),
                        ("table_name", pa.string()), ("extract_type", pa.string()),
                        ("extracted_at", pa.timestamp("us", tz="UTC")), ("window_from", pa.date32()),
                        ("window_to", pa.date32()), ("records", pa.int64()), ("run_id", pa.string()),
                        ("ingested_at", pa.timestamp("us", tz="UTC"))])
    write_table(pa.Table.from_pandas(m, schema=schema, preserve_index=False), "ops_ingested_files", mode="append")

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "files": len(todo), "rows": rows}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
