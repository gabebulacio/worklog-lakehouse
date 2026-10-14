# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Quality gate
# Each check is one SQL query returning a number, compared with a threshold. Results are
# appended to `ops_quality_results` (one row per check per run) for monitoring and trend
# reports. Any failed **error** check makes the orchestrator fail the run, so a broken load
# never reaches the report silently; **warn** checks are recorded but don't stop the run.
#
# Covered: uniqueness and ranges in silver, fact-to-silver reconciliation (no fan-out, no
# lost hours), referential integrity of the star, SCD2 invariants, registry stability
# across Delta versions (time travel), unprocessed landing files, attribution rate and
# coverage.

# PARAMETERS CELL ********************

run_id = "manual"
run_date = ""

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

AS_OF = date.fromisoformat(run_date) if run_date else date.today()
register(sw="silver_worklogs", fw="gold_fact_worklog", du="gold_dim_user", dp="gold_dim_project",
         dd="gold_dim_date", uh="silver_user_history", reg="silver_project_registry",
         conf="silver_identity_conflicts", cov="gold_mart_coverage_month", manifest="ops_ingested_files")

# registry stability: every row of the previous Delta version must still exist unchanged
reg_dt = DeltaTable(f"{TABLES}/silver_project_registry", storage_options=STORAGE)
if reg_dt.version() > 0:
    db.register("reg_prev", DeltaTable(f"{TABLES}/silver_project_registry", storage_options=STORAGE,
                                       version=reg_dt.version() - 1).to_pyarrow_table())
else:
    db.register("reg_prev", sql("select * from reg where false"))

landing = os.path.join(FILES, "landing")
landed = sum(1 for _, _, names in os.walk(landing) for n in names if n.endswith(".json"))

CHECKS = [
    # (name, severity, SQL returning a number, maximum allowed)
    ("silver_entry_unique", "error",
     "select count(*) from (select 1 from sw group by source, external_id having count(*) > 1)", 0),
    ("silver_hours_in_range", "error", "select count(*) from sw where hours <= 0 or hours > 24", 0),
    ("silver_no_future_dates", "error", f"select count(*) from sw where date > DATE '{AS_OF}'", 0),
    ("silver_person_day_over_16h", "warn",
     "select count(*) from (select 1 from sw group by source, worker_id, date having sum(hours) > 16)", 0),
    ("fact_rows_match_silver", "error", "select abs((select count(*) from fw) - (select count(*) from sw))", 0),
    ("fact_hours_match_silver", "error",
     "select abs((select coalesce(sum(hours), 0) from fw) - (select coalesce(sum(hours), 0) from sw))", 0.001),
    ("fact_user_fk", "error", "select count(*) from fw anti join du using (user_key)", 0),
    ("fact_project_fk", "error", "select count(*) from fw anti join dp using (project_key)", 0),
    ("fact_date_fk", "error", "select count(*) from fw anti join dd using (date)", 0),
    ("registry_keys_unique", "error", "select count(*) - count(distinct project_key) from reg", 0),
    ("registry_codes_unique", "error",
     "select count(*) from (select 1 from reg group by source, project_code having count(*) > 1)", 0),
    ("registry_keys_stable", "error", "select count(*) from (select * from reg_prev except select * from reg)", 0),
    ("scd2_one_current_version", "error",
     "select count(*) from (select 1 from uh group by user_key having count(*) filter (where is_current) <> 1)", 0),
    ("scd2_no_overlapping_versions", "error", """
        select count(*) from uh a join uh b on a.user_key = b.user_key and a.valid_from < b.valid_from
        where a.valid_to is null or a.valid_to > b.valid_from""", 0),
    ("landing_files_unprocessed", "error", f"select {landed} - (select count(*) from manifest)", 0),
    ("identity_conflicts", "warn", "select count(*) from conf where kinds <> 'display_name'", 0),
    ("unmatched_hours_share", "warn",
     "select coalesce(sum(hours) filter (where not matched) / nullif(sum(hours), 0), 0) from fw",
     float(var("MaxUnmatchedHoursShare", 0.02))),
    ("coverage_last_full_month", "warn", f"""
        select coalesce(1 - coverage_pct, 1) from cov
        where month = cast(date_trunc('month', DATE '{AS_OF}' - interval 1 month) as date)""", 0.10),
]

checked_at = datetime.now(timezone.utc)
results = []
for name, severity, query, limit in CHECKS:
    value = float(db.sql(query).fetchone()[0] or 0)
    results.append({"run_id": run_id, "checked_at": checked_at, "as_of": AS_OF, "check_name": name,
                    "severity": severity, "value": round(value, 6), "max_allowed": float(limit),
                    "passed": value <= limit})
write_table(pd.DataFrame(results), "ops_quality_results", mode="append")

failed = [r for r in results if not r["passed"]]
for r in failed:
    print(f"{r['severity'].upper():5} {r['check_name']}: {r['value']} (max {r['max_allowed']})")
summary = {"run_id": run_id, "checks": len(results),
           "errors": [r["check_name"] for r in failed if r["severity"] == "error"],
           "warnings": [r["check_name"] for r in failed if r["severity"] == "warn"]}
print(summary)
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
