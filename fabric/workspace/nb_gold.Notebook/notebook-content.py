# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Gold: star schema and marts (Power BI Direct Lake)
#
# | table | grain | notes |
# |---|---|---|
# | `silver_project_registry` | source × project code | **append-only**: a new code gets `max(key) + 1` and keeps it forever, so report relationships and finance references never break |
# | `gold_dim_user` | user | current version from `silver_user_history`, `is_expected` (enabled and not exempt), manager name; `UNKNOWN` member |
# | `gold_dim_project` | project key | registry + CRM account (timesheets match on client name, the tracker on project key); `-1` unknown member |
# | `gold_dim_account` | CRM account | conformed: projects and assignments both relate to it; internal projects have none |
# | `gold_dim_deliverable` | sold position | CRM deliverable + opportunity + account |
# | `gold_dim_date` | day | calendar for the model (mark as date table) |
# | `gold_fact_worklog` | worklog | **lean**: keys, hours, billable hours, logging delay, how the person was matched |
# | `gold_fact_capacity` | user × working day | hours a person could work (`CapacityHoursPerDay`), the utilization denominator |
# | `gold_fact_assignment_month` | assignment × month | CRM assignments prorated by calendar-day overlap × allocation × `AssignedHoursPerMonth` |
# | `gold_mart_sold_vs_utilized` | month × user × account | assigned vs logged hours, status |
# | `gold_mart_coverage_month` | month | expected vs logging people, unmatched hours |
# | `gold_mart_worker_month` | user × month | hours, entries, median logging delay, share logged within 7 days |
# | `gold_mart_nonloggers` | user | expected people with no worklog in the last `NonLoggerWindowDays` |
# | `gold_audit_unmatched_workers` | source identity | worklogs no user could be found for: the mapping-file worklist |
#
# Worklog attribution order: email -> source id (tracker key in the mapping file) ->
# `localpart@DefaultDomain` -> swapped `last.first@DefaultDomain`. Anything else stays in
# the fact as `UNKNOWN`, so totals always reconcile with silver.

# PARAMETERS CELL ********************

run_id = "manual"
run_date = ""   # YYYY-MM-DD (default: today); the "as of" for windows and month ranges

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
DOMAIN = var("DefaultDomain", "lumen.studio").lower()
HOURS_PER_MONTH = float(var("AssignedHoursPerMonth", 160))
WINDOW = int(var("NonLoggerWindowDays", 30))

register(sw="silver_worklogs", hist="silver_user_history", ident="silver_user_identities")
for entity in ("accounts", "opportunities", "deliverables", "assignments"):
    if table_exists(f"bronze_crm_{entity}"):
        register(**{f"raw_{entity}": f"bronze_crm_{entity}"})
        db.register(entity, sql(f"""select * from raw_{entity}
                                    where _source_file = (select _source_file from raw_{entity} order by _extracted_at desc, _source_file desc limit 1)"""))
    else:  # the CRM feed is optional: empty stand-ins keep every query valid
        db.register(entity, sql("select null::varchar x where false"))
HAS_CRM = table_exists("bronze_crm_accounts")

# --- project registry: append-only stable keys --------------------------------------------
registry = read_table("silver_project_registry")
db.register("reg", registry if registry is not None else sql(
    "select null::bigint as project_key, null::varchar as source, null::varchar as project_code, "
    "null::varchar as project_name, null::date as first_seen where false"))
new_codes = sql(f"""
    with seen as (
        select source, coalesce(lower(trim(project_code)), '__pending__') project_code,
               coalesce(min(trim(project_code)), 'PROJECT_PENDING') as project_name
        from sw group by all
    )
    select (select coalesce(max(project_key), 0) from reg)
           + row_number() over (order by s.source, s.project_code) as project_key,
           s.source, s.project_code, s.project_name, DATE '{AS_OF}' as first_seen
    from seen s anti join reg r on r.source = s.source and r.project_code = s.project_code
""")
if new_codes.num_rows:
    write_table(new_codes, "silver_project_registry", mode="append")
register(reg="silver_project_registry")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- dimensions ------------------------------------------------------------------------------
if HAS_CRM:  # timesheets name the client; the tracker uses the account's project key
    account = """left join accounts a
                   on (r.source = 'timesheet_app' and r.project_code = lower(trim(a.name)))
                   or (r.source = 'issue_tracker' and r.project_code = lower(a.tracker_project_key))"""
    account_cols = "a.accountid account_id, a.name account_name, a.owner account_owner, a.accountid is not null"
else:
    account = ""
    account_cols = "null::varchar account_id, null::varchar account_name, null::varchar account_owner, false"
dim_project = sql(f"""
    select r.project_key, r.source, r.project_code, r.project_name, {account_cols} is_billable, r.first_seen
    from reg r {account}
    union all
    select -1, 'n/a', '__unknown__', 'Unknown', null, null, null, false, null
""")
write_table(dim_project, "gold_dim_project")

# conformed account dimension: projects and CRM assignments both point at it (sold vs logged by account)
dim_account = sql("""
    select accountid as account_id, name as account_name, owner as account_owner,
           tracker_project_key, statecode as account_state
    from accounts
""" if HAS_CRM else "select null::varchar as account_id, null::varchar as account_name, null::varchar as account_owner, "
    "null::varchar as tracker_project_key, null::varchar as account_state where false")
write_table(dim_account, "gold_dim_account")

dim_user = sql("""
    with cur as (select * from hist where is_current)
    select c.user_key, c.display_name, c.email, c.department, c.job_title, c.country, c.origin,
           m.display_name as manager_name, c.employee_id, c.account_enabled, c.is_timesheet_exempt,
           c.account_enabled and not c.is_timesheet_exempt and c.origin = 'directory' as is_expected,
           c.hire_date, c.leave_date, c.valid_from as current_since
    from cur c left join cur m on m.user_key = c.manager_key
    union all
    select 'UNKNOWN', 'Unknown worker', null, null, null, null, 'n/a', null, null, null, false, false,
           null, null, null
""")
write_table(dim_user, "gold_dim_user")
db.register("du", dim_user)
db.register("dp", dim_project)

if HAS_CRM:
    dim_deliverable = sql("""
        select d.deliverableid as deliverable_id, d.role, cast(d.start_date as date) as start_date,
               cast(d.end_date as date) as end_date, cast(d.monthly_hours as double) as monthly_hours,
               cast(d.rate as double) as rate, d.approved = 'true' as approved, o.name as opportunity_name,
               a.accountid as account_id, a.name as account_name
        from deliverables d
        left join opportunities o on o.opportunityid = d.opportunityid
        left join accounts a on a.accountid = o.accountid
    """)
    write_table(dim_deliverable, "gold_dim_deliverable")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- fact_worklog: attribute every row to a person and a project --------------------------
# Fallbacks guess from the email's local part, so they only count when the candidate's name
# is unique: with two "Paula Figueroa"s, figueroa.paula@ could be either one. Ambiguous
# rows stay unmatched and land in gold_audit_unmatched_workers for a mapping-file entry.
db.register("guessable", sql("""
    select i.* from ident i where i.user_key not in (
        select user_key from hist where is_current and lower(display_name) in (
            select lower(display_name) from hist where is_current group by 1 having count(*) > 1))
"""))
resolved = sql(f"""
    with w as (
        select *, split_part(worker_email, '@', 1) as local from sw
    )
    select w.*,
           coalesce(i1.user_key, i2.user_key, i3.user_key, i4.user_key) as user_key,
           case when i1.user_key is not null then 'email' when i2.user_key is not null then 'source_id'
                when i3.user_key is not null then 'default_domain' when i4.user_key is not null then 'swapped_name'
           end as match_method
    from w
    left join ident i1 on i1.identifier = w.worker_email
    left join ident i2 on i2.identifier = lower(w.worker_id)
    left join guessable i3 on i3.identifier = w.local || '@{DOMAIN}'
    left join guessable i4 on i4.identifier = split_part(w.local, '.', 2) || '.' || split_part(w.local, '.', 1)
                                              || '@{DOMAIN}' and w.local like '%.%'
""")
db.register("resolved", resolved)
fact = sql("""
    select r.date, r.source, coalesce(r.user_key, 'UNKNOWN') as user_key, coalesce(p.project_key, -1) as project_key,
           r.external_id, r.hours, case when p.is_billable then r.hours else 0 end as billable_hours,
           date_diff('day', r.date, cast(r.created_at as date)) as delay_days,
           r.user_key is not null as matched, coalesce(r.match_method, 'unmatched') as match_method,
           r.worker_id, r.summary, r.issue_key, r.created_at, r.modified_on
    from resolved r
    left join dp p on p.source = r.source and p.project_code = coalesce(lower(trim(r.project_code)), '__pending__')
""")
write_table(fact, "gold_fact_worklog")
db.register("fw", fact)

dates = db.sql(f"select min(date), greatest(max(date), DATE '{AS_OF}') from fw").fetchone()
dim_date = sql(f"""
    select cast(d as date) as date, year(d) as year, quarter(d) as quarter, cast(date_trunc('month', d) as date) as month,
           strftime(d, '%B') as month_name, cast(date_trunc('week', d) as date) as week_start,
           isodow(d) as weekday, isodow(d) >= 6 as is_weekend
    from generate_series(DATE '{dates[0]}', DATE '{dates[1]}', interval 1 day) t(d)
""")
write_table(dim_date, "gold_dim_date")
db.register("ddate", dim_date)

# --- capacity: the denominator of utilization ------------------------------------------------
# One row per expected person (directory, not timesheet-exempt) per working day they were employed.
capacity = sql(f"""
    select d.date, u.user_key, cast({float(var("CapacityHoursPerDay", 8))} as double) as capacity_hours
    from ddate d join du u
      on u.origin = 'directory' and not u.is_timesheet_exempt
     and u.hire_date <= d.date and (u.leave_date is null or d.date < u.leave_date)
    where not d.is_weekend and d.date <= DATE '{AS_OF}'
""")
write_table(capacity, "gold_fact_capacity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- sold vs utilized (CRM assignments) ---------------------------------------------------
MONTH = f"cast(date_trunc('month', DATE '{AS_OF}') as date)"
if HAS_CRM:
    db.register("dd", dim_deliverable)
    assignment_month = sql(f"""
        with a as (
            select s.assignmentid as assignment_id, s.deliverableid as deliverable_id, cast(s.start_date as date) as s_start,
                   cast(s.end_date as date) as s_end, cast(s.allocation as double) as allocation, d.account_id,
                   coalesce(i1.user_key, i3.user_key, i2.user_key, 'UNKNOWN') as user_key
            from assignments s
            join dd d on d.deliverable_id = s.deliverableid
            left join ident i1 on i1.identifier = lower(nullif(s.resource_email, ''))
            left join guessable i3 on i3.identifier = split_part(lower(nullif(s.resource_email, '')), '@', 1)
                                                      || '@{DOMAIN}'
            left join ident i2 on i2.identifier = lower(s.resource_name)
        ), months as (
            select a.*, cast(m as date) as month
            from a, generate_series(date_trunc('month', a.s_start), date_trunc('month', a.s_end), interval 1 month) t(m)
        ), sized as (
            select *, date_diff('day', greatest(s_start, month), least(s_end, last_day(month))) + 1 as overlap_days,
                   date_diff('day', month, last_day(month)) + 1 as month_days
            from months where month <= {MONTH}
        )
        select month, user_key, deliverable_id, account_id, assignment_id, allocation,
               round(overlap_days / month_days, 4) as fraction,
               round(allocation * overlap_days / month_days * {HOURS_PER_MONTH}, 2) as assigned_hours
        from sized
    """)
    write_table(assignment_month, "gold_fact_assignment_month")
    db.register("fam", assignment_month)
    sold = sql(f"""
        with assigned as (
            select month, user_key, account_id, sum(assigned_hours) assigned_hours from fam group by all
        ), logged as (
            select cast(date_trunc('month', f.date) as date) as month, f.user_key, p.account_id, sum(f.hours) as logged_hours
            from fw f join dp p using (project_key)
            where p.account_id is not null group by all
        )
        select coalesce(a.month, l.month) as month, coalesce(a.user_key, l.user_key) as user_key,
               coalesce(a.account_id, l.account_id) as account_id, coalesce(a.assigned_hours, 0) as assigned_hours,
               coalesce(l.logged_hours, 0) as logged_hours,
               round(coalesce(l.logged_hours, 0) / nullif(a.assigned_hours, 0), 4) as utilization_pct,
               case when coalesce(a.assigned_hours, 0) = 0 then 'logging_unassigned'
                    when coalesce(l.logged_hours, 0) = 0 then 'assigned_not_logging'
                    when l.logged_hours < 0.7 * a.assigned_hours then 'under_utilized'
                    else 'ok' end as status
        from assigned a full outer join logged l
             on l.month = a.month and l.user_key = a.user_key and l.account_id = a.account_id
        where coalesce(a.month, l.month) between (select min(month) from logged) and {MONTH}
    """)
    write_table(sold, "gold_mart_sold_vs_utilized")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- operational marts ------------------------------------------------------------------------
coverage = sql(f"""
    with months as (
        select cast(m as date) as month, last_day(m) as month_end
        from generate_series((select date_trunc('month', min(date)) from fw), {MONTH}, interval 1 month) t(m)
    ), expected as (
        select m.month, u.user_key from months m join du u
          on u.origin = 'directory' and not u.is_timesheet_exempt
         and u.hire_date <= m.month_end and (u.leave_date is null or u.leave_date > m.month)
    ), logging as (
        select distinct cast(date_trunc('month', date) as date) as month, user_key from fw where matched
    ), unmatched as (
        select cast(date_trunc('month', date) as date) as month, sum(hours) as unmatched_hours,
               count(distinct source || ':' || worker_id) as unmatched_workers
        from fw where not matched group by all
    )
    select m.month, count(distinct e.user_key) as expected_users,
           count(distinct l.user_key) as logging_users,
           round(count(distinct l.user_key) / nullif(count(distinct e.user_key), 0), 4) as coverage_pct,
           coalesce(any_value(u.unmatched_hours), 0) as unmatched_hours,
           coalesce(any_value(u.unmatched_workers), 0) as unmatched_workers
    from months m
    left join expected e on e.month = m.month
    left join logging l on l.month = e.month and l.user_key = e.user_key
    left join unmatched u on u.month = m.month
    group by m.month order by m.month
""")
write_table(coverage, "gold_mart_coverage_month")

worker_month = sql("""
    select cast(date_trunc('month', date) as date) as month, user_key, sum(hours) as hours, count(*) as entries,
           median(delay_days) as median_delay_days,
           round(avg(case when delay_days <= 7 then 1 else 0 end), 4) as share_within_7d
    from fw group by all
""")
write_table(worker_month, "gold_mart_worker_month")

nonloggers = sql(f"""
    with last_log as (select user_key, max(date) last_log_date from fw where matched group by user_key)
    select u.user_key, u.display_name, u.email, u.department, u.manager_name, u.employee_id,
           l.last_log_date, date_diff('day', l.last_log_date, DATE '{AS_OF}') as days_since_last_log
    from du u left join last_log l using (user_key)
    where u.is_expected and u.hire_date <= DATE '{AS_OF}' - {WINDOW}
      and (l.last_log_date is null or l.last_log_date < DATE '{AS_OF}' - {WINDOW})
    order by days_since_last_log desc nulls first
""")
write_table(nonloggers, "gold_mart_nonloggers")

unmatched = sql("""
    select source, worker_id, any_value(worker_email) worker_email, any_value(worker_name) worker_name,
           count(*) as entries, round(sum(hours), 2) as hours, min(date) as first_date, max(date) as last_date
    from resolved where user_key is null group by source, worker_id order by hours desc
""")
write_table(unmatched, "gold_audit_unmatched_workers")

notebookutils.notebook.exit(json.dumps({
    "run_id": run_id, "as_of": str(AS_OF), "fact_rows": fact.num_rows, "new_projects": new_codes.num_rows,
    "unmatched_workers": unmatched.num_rows, "nonloggers": nonloggers.num_rows,
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
