"""Static checks on the Fabric items: what would break a Git sync or a fabric-cicd deploy."""

import json
import re
from pathlib import Path

import pytest
import yaml

from local.fabric_local import parse_cells

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT / "fabric" / "workspace"
ITEMS = sorted(p for p in WORKSPACE.iterdir() if p.is_dir())
NOTEBOOKS = sorted(WORKSPACE.glob("*.Notebook/notebook-content.py"))
PIPELINES = sorted(WORKSPACE.glob("*.DataPipeline/pipeline-content.json"))
SPARK = '"name": "synapse_pyspark"'


def platform(item):
    return json.loads((item / ".platform").read_text(encoding="utf-8"))


def is_spark(nb):
    return SPARK in nb.read_text(encoding="utf-8")


def sentinel_targets():
    """parameter.yml sentinel GUID -> the notebook it stands for (None for the workspace ID)."""
    out = {}
    for rule in yaml.safe_load((WORKSPACE / "parameter.yml").read_text())["find_replace"]:
        m = re.search(r"\$items\.Notebook\.(\w+)\.id", str(rule["replace_value"]))
        out[rule["find_value"]] = m.group(1) if m else None
    return out


def declared_parameters(notebook):
    text = (WORKSPACE / f"{notebook}.Notebook" / "notebook-content.py").read_text(encoding="utf-8")
    cell = next(code for kind, code in parse_cells(text) if kind == "PARAMETERS CELL")
    return set(re.findall(r"^(\w+)\s*=", cell, re.M))


def test_every_item_has_platform_metadata_matching_its_folder():
    for item in ITEMS:
        meta = platform(item)
        name, kind = item.name.rsplit(".", 1)
        assert meta["metadata"]["type"] == kind, item.name
        assert meta["metadata"]["displayName"] == name, item.name
        assert meta["metadata"]["description"], item.name
        assert re.fullmatch(r"[0-9a-f-]{36}", meta["config"]["logicalId"]), item.name


def test_logical_ids_are_unique():
    ids = [platform(i)["config"]["logicalId"] for i in ITEMS]
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.parent.name)
def test_notebook_structure(nb):
    text = nb.read_text(encoding="utf-8")
    assert text.startswith("# Fabric notebook source\n")
    cells = parse_cells(text)
    kinds = [k for k, _ in cells]
    assert kinds.count("PARAMETERS CELL") == 1, "exactly one parameters cell (pipelines bind to it)"
    assert kinds[1] == "MARKDOWN", "first cell documents the notebook"
    for kind, code in cells:
        if kind in ("CELL", "PARAMETERS CELL"):
            compile(code, str(nb), "exec")
    group = "synapse_pyspark" if is_spark(nb) else "jupyter_python"
    assert f'"language_group": "{group}"' in text, "every cell must declare the notebook's kernel"


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.parent.name)
def test_bootstrap_cell_matches_shared_source(nb):
    """fabric/shared/bootstrap*.py is the source of truth; `python run.py sync` copies it."""
    source = "bootstrap_spark.py" if is_spark(nb) else "bootstrap.py"
    shared = (ROOT / "fabric" / "shared" / source).read_text(encoding="utf-8").strip()
    cells = [code.strip() for kind, code in parse_cells(nb.read_text(encoding="utf-8")) if kind == "CELL"]
    assert shared in cells, f"{nb.parent.name}: bootstrap drifted - run: python run.py sync"


@pytest.mark.parametrize("pl", PIPELINES, ids=lambda p: p.parent.name)
def test_pipelines_call_known_notebooks_with_known_parameters(pl):
    spec = json.loads(pl.read_text(encoding="utf-8"))
    targets = sentinel_targets()
    for activity in spec["properties"]["activities"]:
        assert activity["type"] == "TridentNotebook"
        props = activity["typeProperties"]
        assert props["notebookId"] in targets and props["workspaceId"] in targets, "IDs must be parameter.yml sentinels"
        notebook = targets[props["notebookId"]]
        assert notebook and (WORKSPACE / f"{notebook}.Notebook").exists(), f"sentinel points at a missing notebook {notebook}"
        assert set(props["parameters"]) <= declared_parameters(notebook), f"{notebook} doesn't declare a passed parameter"


def test_variable_library_holds_no_secrets():
    spec = json.loads((WORKSPACE / "wl_settings.VariableLibrary" / "variables.json").read_text(encoding="utf-8"))
    for v in spec["variables"]:
        assert not re.search(r"token|secret|password|apikey", v["name"], re.I), f"{v['name']}: use Key Vault"
        value = str(v["value"])
        assert not re.fullmatch(r"[A-Za-z0-9+/=_-]{24,}", value), f"{v['name']} looks like a credential"


def test_every_notebook_is_run_by_the_orchestrator_or_a_pipeline():
    orchestrator = (WORKSPACE / "nb_orchestrator.Notebook" / "notebook-content.py").read_text(encoding="utf-8")
    by_pipeline = {n for n in sentinel_targets().values() if n}
    for nb in NOTEBOOKS:
        name = nb.parent.name.removesuffix(".Notebook")
        assert f'"{name}"' in orchestrator or name in by_pipeline, f"{name} is never run"
