# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Parity: Python path vs Spark shadow path
# Compares each `spark_*` table with its Python counterpart on the columns that carry meaning,
# in both directions (`except`), and appends one row per pair to `ops_parity_results`. Any
# difference fails the notebook, so `pl_spark_shadow` goes red. This is how a migration to Spark
# would be proven safe before switching: run both, compare, then cut over.
#
# Not compared: the SCD2 `row_hash` (each engine hashes its own string form; it only has to be
# consistent within one path) and `_source_file` lineage columns.

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

PAIRS = [
    ("silver_worklogs", "spark_silver_worklogs",
     "source, external_id, worker_id, worker_email, worker_name, date, hours, project_code, summary, issue_key, "
     "created_at, modified_on, email_backfilled"),
    ("silver_user_history", "spark_silver_user_history",
     "user_key, origin, display_name, email, upn, department, job_title, country, manager_key, account_enabled, "
     "employee_id, is_timesheet_exempt, hire_date, leave_date, valid_from, valid_to, is_current"),
    ("silver_user_identities", "spark_silver_user_identities", "identifier, user_key, kind, priority"),
    ("gold_fact_worklog", "spark_gold_fact_worklog",
     "date, source, user_key, project_key, external_id, hours, billable_hours, delay_days, matched, match_method"),
]

checked_at = datetime.now(timezone.utc)
results = []
for python_table, spark_table, cols in PAIRS:
    register(py=python_table, sp=spark_table)
    only_py = scalar(f"select count(*) from (select {cols} from py except select {cols} from sp)")
    only_sp = scalar(f"select count(*) from (select {cols} from sp except select {cols} from py)")
    rows = scalar("select count(*) from py")
    results.append({"run_id": run_id, "checked_at": checked_at, "python_table": python_table, "spark_table": spark_table,
                    "rows": int(rows), "only_python": int(only_py), "only_spark": int(only_sp),
                    "equal": only_py == 0 and only_sp == 0})
    print(f"{python_table:24} vs {spark_table:30} rows={rows:,}  only_python={only_py}  only_spark={only_sp}")
write_table(pd.DataFrame(results), "ops_parity_results", mode="append")

different = [r["python_table"] for r in results if not r["equal"]]
if different:
    raise RuntimeError(f"Spark shadow path differs from the Python path: {different}")
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "pairs": len(results), "equal": True}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
