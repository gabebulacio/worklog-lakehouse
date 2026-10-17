"""Worklog Lakehouse: run the Fabric medallion on a laptop.

  python run.py setup                   .venv + dependencies
  python run.py demo                    reset, backfill 2026 (full), then 7 daily incremental runs
  python run.py seed [--date D]         reset and backfill from 2026-01-01 up to D (default 2026-09-30)
  python run.py day [--date D]          one daily incremental run (default: the day after the last run)
  python run.py full [--date D]         one monthly full run over the trailing quarter
  python run.py nb NAME [k=v ...]       run a single notebook with parameters
  python run.py tables                  row counts of every table in the lakehouse
  python run.py sql "select ..."        query the lakehouse with DuckDB (tables by name)
  python run.py sync                    copy fabric/shared/bootstrap.py into every notebook
  python run.py test                    pytest + ruff
  python run.py export                  write .ipynb copies to fabric/ipynb_export/ for manual import
  python run.py model [--import]        regenerate the semantic model TMDL (--import: local Import copy too)

Notebooks run unchanged from fabric/workspace/ through local/fabric_local.py.
The lakehouse lives in .lake/ (WL_LAKE_ROOT to move it).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_PY = ROOT / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
WORKSPACE = ROOT / "fabric" / "workspace"
# Marker line that opens each notebook's shared setup cell -> the file it is copied from.
BOOTSTRAPS = {
    "# --- shared bootstrap:": ROOT / "fabric" / "shared" / "bootstrap.py",
    "# --- shared spark bootstrap:": ROOT / "fabric" / "shared" / "bootstrap_spark.py",
}
SEED_FROM, SEED_TO = "2026-01-01", "2026-09-30"


def _reexec_in_venv():
    if VENV_PY.exists() and Path(sys.executable).resolve() != VENV_PY.resolve():
        sys.exit(subprocess.call([str(VENV_PY), __file__, *sys.argv[1:]]))


def lake_root() -> Path:
    return Path(os.getenv("WL_LAKE_ROOT") or ROOT / ".lake").resolve()


def fabric(**kw):
    sys.path.insert(0, str(ROOT))
    from local.fabric_local import LocalFabric

    return LocalFabric(**kw)


def orchestrate(mode: str, run_date: str, **extra) -> dict:
    out = fabric().run("nb_orchestrator", {"mode": mode, "run_date": run_date, **extra})
    result = json.loads(out)
    print(json.dumps(result, indent=2))
    return result


def last_run_date() -> date | None:
    from deltalake import DeltaTable

    try:
        dt = DeltaTable((lake_root() / "wl_lakehouse" / "Tables" / "ops_run_log").as_posix())
    except Exception:
        return None
    days = dt.to_pyarrow_table(columns=["run_date"]).column("run_date").to_pylist()
    return max(date.fromisoformat(d) for d in days) if days else None


def arg(flag: str, default: str | None = None) -> str | None:
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default


# Commands
def setup():
    if not VENV_PY.exists():
        subprocess.check_call([sys.executable, "-m", "venv", str(ROOT / ".venv")])
    subprocess.check_call([str(VENV_PY), "-m", "pip", "install", "-q", "--disable-pip-version-check",
                           "-r", str(ROOT / "requirements.txt")])


def reset():
    target = lake_root()
    if target.exists():
        shutil.rmtree(target)
    print(f"reset {target}")


def seed():
    reset()
    orchestrate("full", arg("--date", SEED_TO), date_from=SEED_FROM)


def day():
    d = arg("--date")
    if not d:
        last = last_run_date()
        d = str((last or date.fromisoformat(SEED_TO)) + timedelta(days=1))
    orchestrate("incremental", d)


def full():
    orchestrate("full", arg("--date") or str(last_run_date() or SEED_TO))


def demo():
    seed()
    start = date.fromisoformat(SEED_TO)
    for k in range(1, 8):
        orchestrate("incremental", str(start + timedelta(days=k)))
    tables()


def nb():
    name = sys.argv[2]
    params = dict(p.split("=", 1) for p in sys.argv[3:])
    print(fabric().run(name, params))


def _tables():
    base = lake_root() / "wl_lakehouse" / "Tables"
    return sorted(p for p in base.iterdir() if (p / "_delta_log").exists()) if base.exists() else []


def _duck():
    import duckdb
    from deltalake import DeltaTable

    con = duckdb.connect()
    for p in _tables():
        con.register(p.name, DeltaTable(p.as_posix()).to_pyarrow_table())
    return con


def tables():
    con = _duck()
    for p in _tables():
        n = con.sql(f"select count(*) from {p.name}").fetchone()[0]
        print(f"{p.name:38} {n:>9,}")


def query():
    rel = _duck().sql(sys.argv[2])
    if rel is not None:
        rel.show(max_rows=60, max_width=200)


def sync():
    """Rewrite the shared setup cell of every notebook from fabric/shared/bootstrap*.py."""
    changed = []
    for nb_file in sorted(WORKSPACE.glob("*.Notebook/notebook-content.py")):
        text = nb_file.read_text(encoding="utf-8")
        mark = next(m for m in BOOTSTRAPS if m in text)
        body = BOOTSTRAPS[mark].read_text(encoding="utf-8").rstrip("\n")
        start = text.index(mark)
        end = text.index("\n# METADATA ********************", start)
        new = text[:start] + body + "\n" + text[end:]
        if new != text:
            nb_file.write_text(new, encoding="utf-8", newline="\n")
            changed.append(nb_file.parent.name)
    print("bootstrap synced:", ", ".join(changed) or "all notebooks already current")


def test():
    code = subprocess.call([sys.executable, "-m", "pytest", "-q", str(ROOT / "tests")])
    code |= subprocess.call([sys.executable, "-m", "ruff", "check", str(ROOT)])
    sys.exit(code)


def export():
    """Plain .ipynb copies for Workspace -> Import -> Notebook (when neither Git nor deploy.py is an option)."""
    sys.path.insert(0, str(ROOT))
    from local.fabric_local import parse_cells

    out = ROOT / "fabric" / "ipynb_export"
    out.mkdir(exist_ok=True)
    for nb_file in sorted(WORKSPACE.glob("*.Notebook/notebook-content.py")):
        cells = []
        for kind, code in parse_cells(nb_file.read_text(encoding="utf-8")):
            if kind == "METADATA":
                continue
            src = code.strip("\n")
            if kind == "MARKDOWN":
                src = "\n".join(line[2:] if line.startswith("# ") else line.lstrip("#") for line in src.splitlines())
                cells.append({"cell_type": "markdown", "metadata": {}, "source": src.splitlines(True)})
            else:
                tags = ["parameters"] if kind == "PARAMETERS CELL" else []
                cells.append({"cell_type": "code", "metadata": {"tags": tags}, "source": src.splitlines(True),
                              "outputs": [], "execution_count": None})
        name = nb_file.parent.name.removesuffix(".Notebook")
        nbjson = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
                  "metadata": {"kernel_info": {"name": "jupyter"}, "language_info": {"name": "python"}}}
        (out / f"{name}.ipynb").write_text(json.dumps(nbjson, indent=1), encoding="utf-8")
    print(f"wrote {out}")


def model():
    subprocess.check_call([sys.executable, str(ROOT / "local" / "build_model.py"), *sys.argv[2:]])


COMMANDS = {"model": model, "setup": setup, "demo": demo, "seed": seed, "day": day, "full": full, "nb": nb, "tables": tables,
            "sql": query, "sync": sync, "test": test, "export": export, "reset": reset}

if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):  # Windows consoles default to cp1252
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    if sys.argv[1] != "setup":
        _reexec_in_venv()
    COMMANDS[sys.argv[1]]()
