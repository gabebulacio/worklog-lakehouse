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
