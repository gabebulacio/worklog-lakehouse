# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Silver: users and identities
# Inputs: the latest `bronze_directory_users` snapshot (Entra ID shape) and the
# hand-maintained `Files/config/user_mapping.csv` (tracker keys, alternate emails,
# timesheet-exempt flag, finance-owned `employee_id`).
#
# | table | grain | how |
# |---|---|---|
# | `silver_user_history` | user × version | **SCD Type 2 with Delta MERGE**: a changed row closes the current version (`valid_to`, `is_current = false`) and inserts a new one; an unchanged snapshot is a no-op, so re-runs are idempotent |
# | `silver_user_identities` | identifier | lowercased mail, UPN, alternate emails, tracker keys and unique display names -> `user_key`, with a priority; gold attributes worklogs through it |
# | `silver_identity_conflicts` | identifier | identities claimed by more than one user, kept out of the map and listed for a human |
#
# `employee_id` is owned by finance and never lost: mapping file, then directory, then the
# value already in history (a non-null is never replaced by a null). People in the mapping
# file but not in the directory (contractors) become users with `origin = 'mapping'`.

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
    except Exception:  # running locally, or the variable isn't in the library
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

mapping_file = os.path.join(FILES, "config", "user_mapping.csv")
mapping = pd.read_csv(mapping_file, dtype=str, keep_default_na=False) if os.path.exists(mapping_file) else \
    pd.DataFrame(columns=["email", "tracker_key", "alt_emails", "is_timesheet_exempt", "employee_id", "notes"])
register(dir_raw="bronze_directory_users", trk_raw="bronze_issue_tracker_users", mapping=mapping)
history = read_table("silver_user_history")
db.register("prev", history if history is not None else sql(
    "select null::varchar user_key, null::varchar employee_id, null::boolean is_current where false"))

snapshot_day = scalar("select cast(max(_extracted_at) as date) from dir_raw")

incoming = sql(f"""
with d as (
    select distinct on (id) * from dir_raw
    where _source_file = (select _source_file from dir_raw order by _extracted_at desc, _source_file desc limit 1)
    order by id, _ingested_at desc
), m as (
    select lower(trim(email)) as email, nullif(trim(tracker_key), '') as tracker_key,
           nullif(trim(alt_emails), '') as alt_emails, coalesce(nullif(trim(is_timesheet_exempt), ''), '0') = '1' as exempt,
           nullif(trim(employee_id), '') as employee_id
    from mapping
), t as (
    select distinct on (key) key, displayName as display_name from trk_raw
    where _source_file = (select _source_file from trk_raw order by _extracted_at desc, _source_file desc limit 1) order by key
), staged as (
    select d.id as user_key, 'directory' as origin, d.displayName as display_name, lower(d.mail) as email,
           lower(d.userPrincipalName) as upn, d.department, d.jobTitle as job_title, d.country,
           d.manager_id as manager_key, d.accountEnabled = 'true' as account_enabled,
           coalesce(m.employee_id, nullif(d.employeeId, ''), p.employee_id) as employee_id,
           coalesce(m.exempt, false) as is_timesheet_exempt,
           cast(substr(d.employeeHireDate, 1, 10) as date) as hire_date,
           cast(substr(d.employeeLeaveDateTime, 1, 10) as date) as leave_date
    from d
    left join m on m.email = lower(d.mail)
    left join prev p on p.user_key = d.id and p.is_current
    union all
    select 'M-' || substr(md5(m.email), 1, 12), 'mapping', coalesce(t.display_name, m.email), m.email, null,
           'External', 'Contractor', null, null, true, m.employee_id, m.exempt, null, null
    from m left join t on t.key = m.tracker_key
    where m.email not in (select lower(mail) from d where mail is not null)
)
select *, md5(concat_ws('|', origin, display_name, email, upn, department, job_title, country, manager_key,
                        account_enabled, employee_id, is_timesheet_exempt, hire_date, leave_date)) as row_hash,
       DATE '{snapshot_day}' as valid_from, cast(null as date) as valid_to, true as is_current
from staged
""")
print(f"directory snapshot {snapshot_day}: {incoming.num_rows} users")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# SCD2: close changed versions and insert new ones in ONE Delta MERGE
# Classic staging trick: every incoming row appears once keyed by user_key (matches the
# current version -> close it if the hash changed) and changed rows appear a second time
# with a NULL merge key (never matches -> inserted as the new current version).
COLS = [f.name for f in incoming.schema]
if history is None:
    write_table(incoming, "silver_user_history")
    changes = {"inserted": incoming.num_rows, "closed": 0}
else:
    db.register("incoming", incoming)
    staged = sql("""
        select i.user_key as merge_key, i.* from incoming i
        union all
        select null, i.* from incoming i
        join prev p on p.user_key = i.user_key and p.is_current and p.row_hash <> i.row_hash
    """)
    dt = DeltaTable(f"{TABLES}/silver_user_history", storage_options=STORAGE)
    result = (
        dt.merge(source=staged, predicate="t.user_key = s.merge_key and t.is_current = true",
                 source_alias="s", target_alias="t")
        .when_matched_update(predicate="t.row_hash <> s.row_hash",
                             updates={"is_current": "false", "valid_to": "s.valid_from"})
        .when_not_matched_insert(updates={c: f"s.{c}" for c in COLS})
        .execute()
    )
    changes = {"inserted": result.get("num_target_rows_inserted"), "closed": result.get("num_target_rows_updated")}
print(f"silver_user_history: {changes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Identity map
register(hist="silver_user_history")
candidates = sql("""
with cur as (select * from hist where is_current),
m as (select lower(trim(email)) as email, lower(nullif(trim(tracker_key), '')) as tracker_key,
             nullif(trim(alt_emails), '') as alt_emails from mapping),
ids as (
    select email as identifier, user_key, 'email' as kind, 1 as priority from cur where email is not null
    union all select upn, user_key, 'upn', 2 from cur where upn is not null
    union all select lower(trim(alt)), user_key, 'alt_email', 3 from (
              select unnest(string_split(m.alt_emails, ';')) as alt, c.user_key
              from m join cur c on c.email = m.email where m.alt_emails is not null)
    union all select m.tracker_key, c.user_key, 'tracker_key', 4
              from m join cur c on c.email = m.email where m.tracker_key is not null
    union all select lower(display_name), user_key, 'display_name', 9 from cur
              where display_name is not null and origin = 'directory'
)
select distinct identifier, user_key, kind, priority from ids where identifier is not null and identifier <> ''
""")
db.register("cand", candidates)
# An identifier claimed by two different users is ambiguous: audit it, never guess.
conflicts = sql("""
    select identifier, string_agg(distinct user_key, ', ') user_keys, string_agg(distinct kind, ', ') kinds,
           count(distinct user_key) as claimants
    from cand group by identifier having count(distinct user_key) > 1
""")
db.register("conf", conflicts)
identities = sql("""
    select identifier, user_key, kind, priority from cand
    where identifier not in (select identifier from conf)
    qualify row_number() over (partition by identifier order by priority) = 1
""")
write_table(identities, "silver_user_identities")
write_table(conflicts, "silver_identity_conflicts")

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "snapshot": str(snapshot_day), **changes,
                                        "identities": identities.num_rows, "conflicts": conflicts.num_rows}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
