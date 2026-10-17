"""Run Fabric notebooks (Git-format `notebook-content.py`) on a laptop, unchanged.

A Fabric Python notebook is plain Python split into cells by marker comments. This
module executes those cells in order in one namespace, injects pipeline parameters
right after the PARAMETERS cell (what Fabric does), and installs a small
`notebookutils` stand-in that maps the handful of calls the notebooks make onto the
local filesystem:

  notebookutils.lakehouse.getWithProperties(name)  -> <lake_root>/<name> instead of abfss://
  notebookutils.fs.mount / getMountPath            -> identity mount
  notebookutils.variableLibrary.get("$(/**/lib/X)") -> fabric/workspace/<lib>.VariableLibrary/variables.json
  notebookutils.notebook.run / exit                -> runs the sibling notebook in-process
  notebookutils.credentials.getToken / getSecret   -> dummy token / environment variable

Spark notebooks (kernel `synapse_pyspark`) also get the `spark` global Fabric provides: a local
SparkSession with Delta Lake. That needs Java and `pip install -r requirements-spark.txt`; CI has both.

Nothing here is imported by the notebooks themselves, so what runs locally is exactly
what gets deployed.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import types
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
WORKSPACE = PROJECT / "fabric" / "workspace"
DEFAULT_LAKE_ROOT = PROJECT / ".lake"

SPARK_KERNEL = '"name": "synapse_pyspark"'
MARKER = re.compile(r"^# (PARAMETERS CELL|CELL|MARKDOWN|METADATA) \*{20}\s*$")


class NotebookExit(BaseException):
    """Raised by notebookutils.notebook.exit(); carries the exit value.

    BaseException so a notebook's own `except Exception` can't swallow the exit."""

    def __init__(self, value):
        super().__init__(value)
        self.value = value


def parse_cells(source: str) -> list[tuple[str, str]]:
    """Split notebook-content.py into (kind, code) cells. kind: CELL | PARAMETERS CELL | MARKDOWN | METADATA."""
    cells, kind, buf = [], None, []
    for line in source.splitlines():
        m = MARKER.match(line)
        if m:
            if kind:
                cells.append((kind, "\n".join(buf)))
            kind, buf = m.group(1), []
        elif kind:
            buf.append(line)
    if kind:
        cells.append((kind, "\n".join(buf)))
    return cells


def _coerce(value, default):
    """Pipeline parameters arrive as strings; type them like the default (Fabric does the same)."""
    if not isinstance(value, str) or default is None:
        return value
    if isinstance(default, bool):
        return value.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(default, int):
        return int(value)
    if isinstance(default, float):
        return float(value)
    return value


class LocalFabric:
    def __init__(self, lake_root: Path | str | None = None, variables: dict | None = None, quiet: bool = False):
        self.lake_root = Path(lake_root or os.getenv("WL_LAKE_ROOT") or DEFAULT_LAKE_ROOT).resolve()
        self.overrides = dict(variables or {})
        self.quiet = quiet
        self.mounts: dict[str, str] = {}
        self.module = self._build_module()
        self._spark = None

    # Spark (only for synapse_pyspark notebooks)
    @property
    def spark(self):
        if self._spark is None:
            try:
                from delta import configure_spark_with_delta_pip
                from pyspark.sql import SparkSession
            except ImportError as exc:
                raise RuntimeError("Spark notebooks need Java and: pip install -r requirements-spark.txt "
                                   "(CI runs them; the Python notebooks don't need Spark)") from exc
            builder = (
                SparkSession.builder.master("local[2]").appName("worklog-lakehouse")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.enabled", "false")
            )
            self._spark = configure_spark_with_delta_pip(builder).getOrCreate()
            self._spark.sparkContext.setLogLevel("ERROR")
        return self._spark

    # Notebookutils stand-in
    def _build_module(self):
        nu = types.ModuleType("notebookutils")
        nu.__doc__ = "Local stand-in for Fabric notebookutils (see local/fabric_local.py)."
        nu.notebook = types.SimpleNamespace(run=self._nb_run, exit=self._nb_exit)
        nu.lakehouse = types.SimpleNamespace(getWithProperties=self._lh_get)
        nu.fs = types.SimpleNamespace(mount=self._fs_mount, getMountPath=self._fs_mount_path)
        nu.variableLibrary = types.SimpleNamespace(get=self._var_get)
        nu.credentials = types.SimpleNamespace(getToken=lambda audience: "local-token",
                                               getSecret=lambda vault, name: os.getenv(name, ""))
        return nu

    def _nb_run(self, name, timeout_seconds=0, arguments=None):
        return self.run(name, arguments or {})

    @staticmethod
    def _nb_exit(value):
        raise NotebookExit(value)

    def _lh_get(self, name):
        path = (self.lake_root / name).as_posix()
        os.makedirs(path, exist_ok=True)
        return {"displayName": name, "properties": {"abfsPath": path}}

    def _fs_mount(self, source, mount_point, extra_configs=None):
        self.mounts[mount_point] = source
        return True

    def _fs_mount_path(self, mount_point):
        return self.mounts[mount_point]

    def _var_get(self, ref):
        m = re.fullmatch(r"\$\(/\*\*/([^/]+)/([^)]+)\)", ref)
        if not m:
            raise ValueError(f"unsupported variable reference {ref!r}")
        lib, name = m.groups()
        if name in self.overrides:
            return self.overrides[name]
        spec = json.loads((WORKSPACE / f"{lib}.VariableLibrary" / "variables.json").read_text(encoding="utf-8"))
        for v in spec["variables"]:
            if v["name"] == name:
                return v["value"]
        raise KeyError(f"{lib}.{name} is not defined")

    # Runner
    def run(self, name: str, params: dict | None = None):
        """Execute a notebook by display name; returns its notebookutils.notebook.exit() value."""
        path = WORKSPACE / f"{name}.Notebook" / "notebook-content.py"
        source = path.read_text(encoding="utf-8")
        cells = parse_cells(source)
        params = dict(params or {})
        ns: dict = {"__name__": "__notebook__"}
        if SPARK_KERNEL in source:
            ns["spark"] = self.spark
        previous = sys.modules.get("notebookutils")
        sys.modules["notebookutils"] = self.module
        t0 = time.perf_counter()
        try:
            for kind, code in cells:
                if kind not in ("CELL", "PARAMETERS CELL"):
                    continue
                exec(compile(code, f"{name}.Notebook", "exec"), ns)
                if kind == "PARAMETERS CELL":
                    unknown = set(params) - set(ns)
                    if unknown:
                        raise TypeError(f"{name}: unknown parameters {sorted(unknown)}")
                    for k, v in params.items():
                        ns[k] = _coerce(v, ns.get(k))
            value = None
        except NotebookExit as done:
            value = done.value
        finally:
            if previous is None:
                sys.modules.pop("notebookutils", None)
            else:
                sys.modules["notebookutils"] = previous
        if not self.quiet:
            print(f"[local] {name} finished in {time.perf_counter() - t0:.1f}s")
        return value
