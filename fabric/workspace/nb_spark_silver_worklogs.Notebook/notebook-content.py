# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Spark shadow: silver worklogs
# PySpark / Spark SQL port of `nb_silver_worklogs`, writing `spark_silver_worklogs`. It reads the
# same bronze tables and applies the same **person-day rule**: for each `(source, worker_id, date)`
# keep the rows of the latest extract that contains it, unless a later full extract covers that
# date without it. `nb_parity` checks the result is identical to the Python path.
#
# The table is partitioned by `work_month`; `nb_spark_maintenance` compacts and Z-orders it.
#
# What changes in the port: `distinct on` and `qualify` become `row_number()` windows, `exclude`
# becomes `.drop()`, and three timestamp formats go through `to_timestamp` with the CORRECTED
# parser policy (unparseable -> NULL, as `try_strptime` does in DuckDB).

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

from pyspark.sql import functions as F

TS = "coalesce(to_timestamp({c}, \"yyyy-MM-dd'T'HH:mm:ss'Z'\"), to_timestamp({c}, 'MM/dd/yyyy HH:mm:ss'), to_timestamp({c}, 'yyyy-MM-dd HH:mm:ss.SSS'))"

parts = []
if table_exists("bronze_timesheet_app_timesheets"):
    view(ts="bronze_timesheet_app_timesheets", tsu="bronze_timesheet_app_users")
    spark.sql(r"""
        select UserId, email from (
            select UserId, nullif(trim(regexp_replace(Email, '\\s*\\(INACTIVE\\)\\s*$', '')), '') as email,
                   row_number() over (partition by UserId order by _ingested_at desc) as rn
            from tsu
            where _source_file = (select max_by(_source_file, struct(_extracted_at, _source_file)) from tsu)
        ) where rn = 1
    """).createOrReplaceTempView("ts_users")
    parts.append(f"""
        select 'timesheet_app' as source, ts.Id as external_id, ts.UserId as worker_id,
               lower(coalesce(nullif(trim(ts.UserEmail), ''), u.email)) as worker_email,
               ts.`User` as worker_name, cast(substr(ts.`Day`, 1, 10) as date) as date,
               cast(ts.Hours as double) as hours, nullif(trim(ts.Client), '') as project_code,
               ts.Job as summary, cast(null as string) as issue_key,
               {TS.format(c="ts.ModifiedOn")} as modified_on,
               {TS.format(c="ts.ModifiedOn")} as created_at,
               nullif(trim(ts.UserEmail), '') is null and u.email is not null as email_backfilled,
               ts._source_file, ts._extracted_at
        from ts left join ts_users u on u.UserId = ts.UserId""")
if table_exists("bronze_issue_tracker_worklogs"):
    view(tw="bronze_issue_tracker_worklogs", tku="bronze_issue_tracker_users")
    spark.sql("""
        select `key`, emailAddress, displayName from (
            select `key`, emailAddress, displayName,
                   row_number() over (partition by `key` order by _ingested_at desc) as rn
            from tku
            where _source_file = (select max_by(_source_file, struct(_extracted_at, _source_file)) from tku)
        ) where rn = 1
    """).createOrReplaceTempView("trk_users")
    parts.append(f"""
        select 'issue_tracker', tw.worklogId, tw.author_accountKey, lower(nullif(u.emailAddress, '')), u.displayName,
               cast(tw.startDate as date), cast(tw.timeSpentSeconds as double) / 3600, tw.issue_projectKey,
               substring(tw.issue_summary || ' | ' || tw.issue_key, 1, 255), tw.issue_key,
               {TS.format(c="tw.dateUpdated")}, {TS.format(c="tw.dateCreated")},
               false, tw._source_file, tw._extracted_at
        from tw left join trk_users u on u.`key` = tw.author_accountKey""")
if not parts:
    raise RuntimeError("no bronze worklog tables yet - run nb_bronze_ingest first")
view(manifest="ops_ingested_files")
spark.sql(" union all ".join(parts)).createOrReplaceTempView("unified")

silver = spark.sql("""
    with fulls as (
        select system as source, extracted_at, window_from, window_to
        from manifest where extract_type = 'full' and entity in ('timesheets', 'worklogs')
    ), latest as (
        select source, worker_id, date, max(_extracted_at) as extracted_at from unified group by source, worker_id, date
    ), kept as (
        select u.* from unified u
        join latest l on l.source = u.source and l.worker_id = u.worker_id and l.date = u.date
                     and l.extracted_at = u._extracted_at
        where not exists (
            select 1 from fulls f
            where f.source = u.source and f.extracted_at > u._extracted_at
              and u.date between f.window_from and f.window_to)
    )
    select *, row_number() over (partition by source, external_id order by _extracted_at desc, _source_file desc) as rn
    from kept
""").where("rn = 1").drop("rn")
# Partitioned by work month: daily runs rewrite recent months, and month filters prune whole folders.
silver = silver.withColumn("work_month", F.trunc("date", "month"))
rows = write(silver, "spark_silver_worklogs", partition_by=["work_month"])
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "rows": rows}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
