import sys
from pathlib import Path

import duckdb
import pytest
from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from local.fabric_local import LocalFabric  # noqa: E402

BACKFILL_FROM = "2026-01-01"
SEED_DAY = "2026-09-30"
DAILY = ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04", "2026-10-05"]


class Lake:
    """A local lakehouse in a temp folder, plus helpers to run notebooks and query tables."""

    def __init__(self, root: Path):
        self.root = root
        self.fabric = LocalFabric(lake_root=root, quiet=True)

    def run(self, name, **params):
        return self.fabric.run(name, params)

    def orchestrate(self, mode, run_date, **params):
        return self.run("nb_orchestrator", mode=mode, run_date=run_date, **params)

    def path(self, table):
        return (self.root / "wl_lakehouse" / "Tables" / table).as_posix()

    def table(self, table, version=None):
        return DeltaTable(self.path(table), version=version).to_pyarrow_table()

    def version(self, table):
        return DeltaTable(self.path(table)).version()

    def sql(self, query, **tables):
        con = duckdb.connect()
        for alias, name in tables.items():
            con.register(alias, self.table(name))
        return con.sql(query).fetchall()


@pytest.fixture(scope="session")
def daily_lake(tmp_path_factory):
    """Backfill, then five daily incremental runs: the normal life of the pipeline."""
    lake = Lake(tmp_path_factory.mktemp("daily"))
    lake.orchestrate("full", SEED_DAY, date_from=BACKFILL_FROM)
    for day in DAILY:
        lake.orchestrate("incremental", day)
    return lake


@pytest.fixture(scope="session")
def full_lake(tmp_path_factory):
    """The same world extracted once, in full, on the last day."""
    lake = Lake(tmp_path_factory.mktemp("full"))
    lake.orchestrate("full", DAILY[-1], date_from=BACKFILL_FROM)
    return lake
