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
TABLES = f"{BASE}/Tables"
FILES = f"{BASE}/Files"

# Same semantics as the Python notebooks: UTC timestamps, and an unparseable timestamp is NULL, not an error.
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")


def table_exists(name):
    return DeltaTable.isDeltaTable(spark, f"{TABLES}/{name}")


def read(name):
    return spark.read.format("delta").load(f"{TABLES}/{name}")


def view(**tables):
    """Register Delta tables (by name) or DataFrames as temp views for Spark SQL."""
    for alias, source in tables.items():
        (read(source) if isinstance(source, str) else source).createOrReplaceTempView(alias)


def write(df, name, mode="overwrite"):
    (df.write.format("delta").mode(mode)
       .option("overwriteSchema" if mode == "overwrite" else "mergeSchema", "true")
       .save(f"{TABLES}/{name}"))
    rows = read(name).count() if mode == "overwrite" else df.count()
    print(f"{name}: {rows:,} rows ({mode})")
    return rows
