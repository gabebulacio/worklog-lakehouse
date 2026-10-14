"""The Spark shadow path produces exactly what the Python path produces.

Needs Java and `pip install -r requirements-spark.txt`; skipped otherwise (CI's `spark-parity` job runs it).
Runs the real Spark notebooks after each primary run, so the SCD2 history sees the same sequence of
directory snapshots on both paths, then `nb_parity` compares every pair of tables.
"""

import shutil

import pytest
from conftest import BACKFILL_FROM, SEED_DAY, Lake

pytest.importorskip("pyspark", reason="Spark tests need: pip install -r requirements-spark.txt")
pytest.importorskip("delta", reason="Spark tests need delta-spark")
if not shutil.which("java"):
    pytest.skip("Spark tests need Java on PATH", allow_module_level=True)

DAYS = ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04"]  # covers department moves (SCD2 changes)
SHADOW = ["nb_spark_silver_users", "nb_spark_silver_worklogs", "nb_spark_gold_fact"]


@pytest.fixture(scope="module")
def shadow_lake(tmp_path_factory):
    lake = Lake(tmp_path_factory.mktemp("spark"))
    runs = [("full", SEED_DAY, {"date_from": BACKFILL_FROM})] + [("incremental", d, {}) for d in DAYS]
    for mode, day, extra in runs:
        lake.orchestrate(mode, day, **extra)
        for nb in SHADOW:
            lake.run(nb, run_id=f"spark-{day}")
    lake.parity = lake.run("nb_parity", run_id="parity")
    return lake


def test_parity_notebook_passes(shadow_lake):
    assert '"equal": true' in shadow_lake.parity


@pytest.mark.parametrize("python_table", ["silver_worklogs", "silver_user_history", "silver_user_identities", "gold_fact_worklog"])
def test_each_pair_is_identical(shadow_lake, python_table):
    rows = shadow_lake.sql("select rows, only_python, only_spark from r where python_table = ?".replace("?", f"'{python_table}'"),
                           r="ops_parity_results")
    assert rows, f"{python_table} was not compared"
    n, only_py, only_sp = rows[-1]
    assert n > 0 and only_py == 0 and only_sp == 0


def test_spark_scd2_recorded_the_department_moves(shadow_lake):
    moved = shadow_lake.sql("select count(*) from (select 1 from h group by user_key having count(*) > 1)",
                            h="spark_silver_user_history")[0][0]
    assert moved >= 2, "Delta MERGE should have closed and reopened versions for the October moves"
