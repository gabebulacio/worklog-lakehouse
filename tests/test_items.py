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


def platform(item):
    return json.loads((item / ".platform").read_text(encoding="utf-8"))


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
    assert '"language_group": "jupyter_python"' in text, "Python notebook kernel"


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.parent.name)
def test_bootstrap_cell_matches_shared_source(nb):
    """fabric/shared/bootstrap.py is the source of truth; `python run.py sync` copies it."""
    shared = (ROOT / "fabric" / "shared" / "bootstrap.py").read_text(encoding="utf-8").strip()
    cells = [code.strip() for kind, code in parse_cells(nb.read_text(encoding="utf-8")) if kind == "CELL"]
    assert shared in cells, f"{nb.parent.name}: bootstrap drifted - run: python run.py sync"


@pytest.mark.parametrize("pl", PIPELINES, ids=lambda p: p.parent.name)
def test_pipelines_call_the_orchestrator_with_known_parameters(pl):
    spec = json.loads(pl.read_text(encoding="utf-8"))
    sentinels = {r["find_value"] for r in yaml.safe_load((WORKSPACE / "parameter.yml").read_text())["find_replace"]}
    orchestrator = (WORKSPACE / "nb_orchestrator.Notebook" / "notebook-content.py").read_text(encoding="utf-8")
    params_cell = next(code for kind, code in parse_cells(orchestrator) if kind == "PARAMETERS CELL")
    declared = set(re.findall(r"^(\w+)\s*=", params_cell, re.M))
    for activity in spec["properties"]["activities"]:
        assert activity["type"] == "TridentNotebook"
        props = activity["typeProperties"]
        assert {props["notebookId"], props["workspaceId"]} <= sentinels, "IDs must be parameter.yml sentinels"
        assert set(props["parameters"]) <= declared, "pipeline passes a parameter the orchestrator doesn't declare"


def test_variable_library_holds_no_secrets():
    spec = json.loads((WORKSPACE / "wl_settings.VariableLibrary" / "variables.json").read_text(encoding="utf-8"))
    for v in spec["variables"]:
        assert not re.search(r"token|secret|password|apikey", v["name"], re.I), f"{v['name']}: use Key Vault"
        value = str(v["value"])
        assert not re.fullmatch(r"[A-Za-z0-9+/=_-]{24,}", value), f"{v['name']} looks like a credential"


def test_orchestrator_runs_every_notebook():
    orchestrator = (WORKSPACE / "nb_orchestrator.Notebook" / "notebook-content.py").read_text(encoding="utf-8")
    for nb in NOTEBOOKS:
        name = nb.parent.name.removesuffix(".Notebook")
        if name != "nb_orchestrator":
            assert f'"{name}"' in orchestrator, f"{name} is never run"
