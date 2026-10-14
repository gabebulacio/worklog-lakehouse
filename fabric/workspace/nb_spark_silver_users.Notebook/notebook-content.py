# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Spark shadow: users and identities
# PySpark port of `nb_silver_users`, writing `spark_silver_user_history`, `spark_silver_user_identities`
# and `spark_silver_identity_conflicts`. The SCD Type 2 history uses the Delta Lake **MERGE** API
# (`DeltaTable.merge`) with the same staging trick as the Python path: every incoming row keyed by
# `user_key` closes the current version when its hash changed, and changed rows appear a second time
# with a NULL merge key so they are inserted as the new current version. An unchanged snapshot is a no-op.

# PARAMETERS CELL ********************

run_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- shared spark bootstrap: source of truth is fabric/shared/bootstrap_spark.py (python run.py sync) ---
# Same cell in every Spark notebook. `spark` is the session Fabric provides (locally: local/fabric_local.py).
# Paths are resolved by lakehouse name, so no default lakehouse needs to be attached.
import json
import os

import notebookutils
from delta.tables import DeltaTable

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
FILES = f"{BASE}/Files"

# Same semantics as the Python notebooks: UTC timestamps, and an unparseable timestamp is NULL, not an error.
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
if ON_FABRIC:
    # V-Order: Fabric's read-optimized Parquet layout (Direct Lake and the SQL endpoint read it fastest).
    spark.conf.set("spark.sql.parquet.vorder.default", "true")


def table_exists(name):
    return DeltaTable.isDeltaTable(spark, f"{TABLES}/{name}")


def read(name):
    return spark.read.format("delta").load(f"{TABLES}/{name}")


def view(**tables):
    """Register Delta tables (by name) or DataFrames as temp views for Spark SQL."""
    for alias, source in tables.items():
        (read(source) if isinstance(source, str) else source).createOrReplaceTempView(alias)


def write(df, name, mode="overwrite", partition_by=None):
    writer = (df.write.format("delta").mode(mode)
              .option("overwriteSchema" if mode == "overwrite" else "mergeSchema", "true"))
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(f"{TABLES}/{name}")
    rows = read(name).count() if mode == "overwrite" else df.count()
    print(f"{name}: {rows:,} rows ({mode})")
    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

view(dir_raw="bronze_directory_users", trk_raw="bronze_issue_tracker_users")
mapping = spark.read.option("header", True).csv(f"{FILES}/config/user_mapping.csv")
mapping.createOrReplaceTempView("mapping")
HISTORY = "spark_silver_user_history"
has_history = table_exists(HISTORY)
if has_history:
    view(prev=HISTORY)
else:
    spark.sql("select cast(null as string) as user_key, cast(null as string) as employee_id, "
              "cast(null as boolean) as is_current, cast(null as string) as row_hash where false").createOrReplaceTempView("prev")

snapshot_day = spark.sql("select cast(max(_extracted_at) as date) as d from dir_raw").first()["d"]
LATEST = "_source_file = (select max_by(_source_file, struct(_extracted_at, _source_file)) from {t})"

incoming = spark.sql(f"""
with d as (
    select * from (
        select *, row_number() over (partition by id order by _ingested_at desc) as rn
        from dir_raw where {LATEST.format(t="dir_raw")}
    ) where rn = 1
), m as (
    select lower(trim(email)) as email, nullif(trim(tracker_key), '') as tracker_key,
           nullif(trim(alt_emails), '') as alt_emails,
           coalesce(nullif(trim(is_timesheet_exempt), ''), '0') = '1' as exempt,
           nullif(trim(employee_id), '') as employee_id
    from mapping
), t as (
    select `key`, displayName as display_name from (
        select `key`, displayName, row_number() over (partition by `key` order by _ingested_at desc) as rn
        from trk_raw where {LATEST.format(t="trk_raw")}
    ) where rn = 1
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
    select 'M-' || substr(md5(m.email), 1, 12), 'mapping', coalesce(t.display_name, m.email), m.email,
           cast(null as string), 'External', 'Contractor', cast(null as string), cast(null as string), true,
           m.employee_id, m.exempt, cast(null as date), cast(null as date)
    from m left join t on t.`key` = m.tracker_key
    where m.email not in (select lower(mail) from d where mail is not null)
)
select *,
       md5(concat_ws('|', origin, display_name, email, upn, department, job_title, country, manager_key,
                     cast(account_enabled as string), employee_id, cast(is_timesheet_exempt as string),
                     cast(hire_date as string), cast(leave_date as string))) as row_hash,
       date'{snapshot_day}' as valid_from, cast(null as date) as valid_to, true as is_current
from staged
""").localCheckpoint()  # materialize: the MERGE below rewrites the table `prev` reads from
print(f"directory snapshot {snapshot_day}: {incoming.count()} users")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- SCD2 with the Delta MERGE API ------------------------------------------------------------
cols = incoming.columns
if not has_history:
    write(incoming, HISTORY)
    changes = {"inserted": incoming.count(), "closed": 0}
else:
    incoming.createOrReplaceTempView("incoming")
    staged = spark.sql("""
        select i.user_key as merge_key, i.* from incoming i
        union all
        select cast(null as string), i.* from incoming i
        join prev p on p.user_key = i.user_key and p.is_current and p.row_hash <> i.row_hash
    """).localCheckpoint()
    target = DeltaTable.forPath(spark, f"{TABLES}/{HISTORY}")
    (target.alias("t")
        .merge(staged.alias("s"), "t.user_key = s.merge_key and t.is_current = true")
        .whenMatchedUpdate(condition="t.row_hash <> s.row_hash", set={"is_current": "false", "valid_to": "s.valid_from"})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in cols})
        .execute())
    metrics = target.history(1).select("operationMetrics").first()["operationMetrics"]
    changes = {"inserted": int(metrics.get("numTargetRowsInserted", 0)), "closed": int(metrics.get("numTargetRowsUpdated", 0))}
print(f"{HISTORY}: {changes}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- identity map -------------------------------------------------------------------------------
view(hist=HISTORY)
spark.sql("""
with cur as (select * from hist where is_current),
m as (select lower(trim(email)) as email, lower(nullif(trim(tracker_key), '')) as tracker_key,
             nullif(trim(alt_emails), '') as alt_emails from mapping),
ids as (
    select email as identifier, user_key, 'email' as kind, 1 as priority from cur where email is not null
    union all select upn, user_key, 'upn', 2 from cur where upn is not null
    union all select lower(trim(alt)), user_key, 'alt_email', 3 from (
              select explode(split(m.alt_emails, ';')) as alt, c.user_key
              from m join cur c on c.email = m.email where m.alt_emails is not null)
    union all select m.tracker_key, c.user_key, 'tracker_key', 4
              from m join cur c on c.email = m.email where m.tracker_key is not null
    union all select lower(display_name), user_key, 'display_name', 9 from cur
              where display_name is not null and origin = 'directory'
)
select distinct identifier, user_key, kind, priority from ids where identifier is not null and identifier <> ''
""").createOrReplaceTempView("cand")
conflicts = spark.sql("""
    select identifier, concat_ws(', ', sort_array(collect_set(user_key))) as user_keys,
           concat_ws(', ', sort_array(collect_set(kind))) as kinds, count(distinct user_key) as claimants
    from cand group by identifier having count(distinct user_key) > 1
""")
conflicts.createOrReplaceTempView("conf")
identities = spark.sql("""
    select identifier, user_key, kind, priority from (
        select c.*, row_number() over (partition by identifier order by priority) as rn
        from cand c left anti join conf using (identifier)
    ) where rn = 1
""")
n_ids = write(identities, "spark_silver_user_identities")
n_conf = write(conflicts, "spark_silver_identity_conflicts")
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "snapshot": str(snapshot_day), **changes,
                                        "identities": n_ids, "conflicts": n_conf}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
