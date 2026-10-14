# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Silver: worklogs
# One canonical table from both time sources:
# `source · external_id · worker_id · worker_email · worker_name · date · hours · project_code · summary · issue_key · created_at · modified_on`.
#
# **Rule (deterministic, replayable):** for each person-day `(source, worker_id, date)`, keep
# the rows from the **latest extract that contains that person-day**, unless a later
# **full** extract covers that date without it (the day was deleted at the source).
# Incremental extracts carry complete changed person-days, so a changed day replaces the
# old one wholesale; the monthly full extract is authoritative for its window and removes
# days an incremental pull can't see (every entry of the day deleted).
#
# Silver is recomputed from bronze on every run and written in one atomic commit. At this
# volume (tens of thousands of rows a quarter) that takes about a second; extraction is
# where incremental matters. The same rule makes any replay of the landing archive produce
# the same table, which the test suite checks.
#
# Source fixes applied here:
# - timesheet rows with a blank email (deactivated users) take it from the latest
#   `users` snapshot, with the ` (INACTIVE)` suffix stripped (`email_backfilled = true`);
# - `ModifiedOn` arrives in three formats; all three are parsed;
# - the timesheet API has no creation time, so `created_at = modified_on` there (an upper
#   bound for logging delay); the tracker has the exact `dateCreated`;
# - tracker authors are opaque keys, resolved to email/name through its `users` snapshot;
# - duplicate rows from overlapping API pages are dropped.

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

parts = []
if table_exists("bronze_timesheet_app_timesheets"):
    register(ts="bronze_timesheet_app_timesheets", tsu="bronze_timesheet_app_users")
    parts.append("""
    select 'timesheet_app' as source, ts.Id as external_id, ts.UserId as worker_id,
           lower(coalesce(nullif(trim(ts.UserEmail), ''), u.email)) as worker_email,
           ts."User" as worker_name, cast(substr(ts."Day", 1, 10) as date) as date, cast(ts.Hours as double) as hours,
           nullif(trim(ts.Client), '') as project_code, ts.Job as summary, cast(null as varchar) as issue_key,
           coalesce(try_strptime(ts.ModifiedOn, '%Y-%m-%dT%H:%M:%SZ'),
                    try_strptime(ts.ModifiedOn, '%m/%d/%Y %H:%M:%S'),
                    try_strptime(ts.ModifiedOn, '%Y-%m-%d %H:%M:%S.%g')) as modified_on,
           coalesce(try_strptime(ts.ModifiedOn, '%Y-%m-%dT%H:%M:%SZ'),
                    try_strptime(ts.ModifiedOn, '%m/%d/%Y %H:%M:%S'),
                    try_strptime(ts.ModifiedOn, '%Y-%m-%d %H:%M:%S.%g')) as created_at,
           nullif(trim(ts.UserEmail), '') is null and u.email is not null as email_backfilled,
           ts._source_file, ts._extracted_at
    from ts
    left join (
        select distinct on (UserId) UserId,
               nullif(trim(regexp_replace(Email, '\\s*\\(INACTIVE\\)\\s*$', '')), '') as email
        from tsu where _source_file = (select _source_file from tsu order by _extracted_at desc, _source_file desc limit 1)
        order by UserId, _ingested_at desc
    ) u on u.UserId = ts.UserId""")
if table_exists("bronze_issue_tracker_worklogs"):
    register(tw="bronze_issue_tracker_worklogs", tku="bronze_issue_tracker_users")
    parts.append("""
    select 'issue_tracker', tw.worklogId, tw.author_accountKey, lower(nullif(u.emailAddress, '')), u.displayName,
           cast(tw.startDate as date), cast(tw.timeSpentSeconds as double) / 3600, tw.issue_projectKey,
           left(tw.issue_summary || ' | ' || tw.issue_key, 255), tw.issue_key,
           try_strptime(tw.dateUpdated, '%Y-%m-%dT%H:%M:%SZ'), try_strptime(tw.dateCreated, '%Y-%m-%dT%H:%M:%SZ'),
           false, tw._source_file, tw._extracted_at
    from tw
    left join (
        select distinct on (key) key, emailAddress, displayName from tku
        where _source_file = (select _source_file from tku order by _extracted_at desc, _source_file desc limit 1) order by key, _ingested_at desc
    ) u on u.key = tw.author_accountKey""")
if not parts:
    raise RuntimeError("no bronze worklog tables yet - run nb_bronze_ingest first")
register(manifest="ops_ingested_files")
db.register("unified", sql(" union all ".join(parts)))

silver = sql("""
with fulls as (
    select system as source, extracted_at, window_from, window_to
    from manifest where extract_type = 'full' and entity in ('timesheets', 'worklogs')
), latest as (
    select source, worker_id, date, max(_extracted_at) extracted_at from unified group by all
), kept as (
    select u.* from unified u
    join latest l on l.source = u.source and l.worker_id = u.worker_id and l.date = u.date
                 and l.extracted_at = u._extracted_at
    where not exists (
        select 1 from fulls f
        where f.source = u.source and f.extracted_at > u._extracted_at
          and u.date between f.window_from and f.window_to)
)
select * exclude (rn) from (
    select *, row_number() over (partition by source, external_id order by _extracted_at desc, _source_file desc) rn
    from kept
) where rn = 1
order by date, source, worker_id, external_id
""")
write_table(silver, "silver_worklogs")

db.register("sw", silver)
stats = db.sql("""select count(*) as n, round(sum(hours), 2) as h, count(*) filter (where email_backfilled) as backfilled,
                         count(*) filter (where worker_email is null) no_email from sw""").fetchone()
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "rows": stats[0], "hours": stats[1],
                                        "emails_backfilled": stats[2], "rows_without_email": stats[3]}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
