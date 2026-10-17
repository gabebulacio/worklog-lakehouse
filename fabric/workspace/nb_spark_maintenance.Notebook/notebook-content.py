# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Spark table maintenance
# Keeps the Spark tables fast to read and cheap to store, and logs every operation to
# `ops_maintenance_log`:
#
# | table | layout | why |
# |---|---|---|
# | `spark_silver_worklogs` | partitioned by `work_month`, Z-ordered by `worker_id, date` | daily runs touch recent months; person-day lookups and month filters skip most files |
# | `spark_gold_fact_worklog` | Z-ordered by `user_key, project_key` | the report filters and joins on these keys |
# | `spark_silver_user_history`, `spark_silver_user_identities` | compacted | small tables that MERGE fragments into many files |
#
# - **OPTIMIZE** rewrites small files into large ones (bin-packing), with **Z-ORDER** co-locating rows
#   by the given columns so file skipping works. On Fabric, `VORDER` adds the V-Order layout that
#   Direct Lake and the SQL endpoint read fastest; `spark.sql.parquet.vorder.default` makes new
#   writes V-Ordered too (set in the shared bootstrap).
# - **VACUUM** removes files no longer referenced by the table, older than `vacuum_retain_hours`
#   (default 168 h = 7 days, so time travel and concurrent readers keep working).

# PARAMETERS CELL ********************

run_id = "manual"
vacuum_retain_hours = 168

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
    except Exception:  # running locally, or the variable isn't in the library
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

import time
from datetime import datetime, timezone

PLAN = [
    ("spark_silver_worklogs", ["worker_id", "date"]),
    ("spark_gold_fact_worklog", ["user_key", "project_key"]),
    ("spark_silver_user_history", []),
    ("spark_silver_user_identities", []),
]
if int(vacuum_retain_hours) < 168:
    # Short retention is for tests and one-off cleanups only: it breaks time travel past that window.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")


def file_count(name):
    return DeltaTable.forPath(spark, f"{TABLES}/{name}").detail().first()["numFiles"]


log = []
for name, zorder in PLAN:
    if not table_exists(name):
        print(f"{name}: not there yet, skipped")
        continue
    before, t0 = file_count(name), time.perf_counter()
    statement = f"OPTIMIZE delta.`{TABLES}/{name}`"
    if zorder:
        statement += f" ZORDER BY ({', '.join(zorder)})"
    if ON_FABRIC:
        statement += " VORDER"
    spark.sql(statement)
    optimized = file_count(name)
    spark.sql(f"VACUUM delta.`{TABLES}/{name}` RETAIN {int(vacuum_retain_hours)} HOURS")
    log.append({"run_id": run_id, "table_name": name, "zorder_by": ", ".join(zorder), "vorder": ON_FABRIC,
                "files_before": before, "files_after": optimized, "vacuum_retain_hours": int(vacuum_retain_hours),
                "seconds": round(time.perf_counter() - t0, 2), "maintained_at": datetime.now(timezone.utc)})
    print(f"{name}: {before} -> {optimized} files" + (f", Z-ordered by {', '.join(zorder)}" if zorder else ""))

if log:
    write(spark.createDataFrame(log), "ops_maintenance_log", mode="append")
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "tables": len(log)}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
