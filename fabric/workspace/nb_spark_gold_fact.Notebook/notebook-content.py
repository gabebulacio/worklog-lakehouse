# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Spark shadow: worklog fact
# Spark SQL port of the attribution in `nb_gold`: every row of `spark_silver_worklogs` goes to a person
# by the first rule that matches (email, the source's own ID, `localpart@DefaultDomain`, swapped
# `last.first`), the two guessing rules only for people with a unique name, and to a project key.
# Writes `spark_gold_fact_worklog`. Dimensions and the project registry are owned by the primary path
# (`gold_dim_project`), so both facts point at the same keys and `nb_parity` can compare them row by row.

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

DOMAIN = str(var("DefaultDomain", "lumen.studio")).lower()
view(sw="spark_silver_worklogs", ident="spark_silver_user_identities", hist="spark_silver_user_history",
     dp="gold_dim_project")

spark.sql("""
    select i.* from ident i where i.user_key not in (
        select user_key from hist where is_current and lower(display_name) in (
            select lower(display_name) from hist where is_current group by lower(display_name) having count(*) > 1))
""").createOrReplaceTempView("guessable")

spark.sql(f"""
    with w as (select *, split_part(worker_email, '@', 1) as local from sw)
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
""").createOrReplaceTempView("resolved")

fact = spark.sql("""
    select r.date, r.source, coalesce(r.user_key, 'UNKNOWN') as user_key, coalesce(p.project_key, -1) as project_key,
           r.external_id, r.hours, case when p.is_billable then r.hours else 0 end as billable_hours,
           datediff(cast(r.created_at as date), r.date) as delay_days,
           r.user_key is not null as matched, coalesce(r.match_method, 'unmatched') as match_method,
           r.worker_id, r.summary, r.issue_key, r.created_at, r.modified_on
    from resolved r
    left join dp p on p.source = r.source and p.project_code = coalesce(lower(trim(r.project_code)), '__pending__')
""")
rows = write(fact, "spark_gold_fact_worklog")
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "rows": rows}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
