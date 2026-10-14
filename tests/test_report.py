"""The Power BI report: every visual binds to a column or measure that exists in the semantic model."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "local"))
import build_model  # noqa: E402

REPORT = ROOT / "fabric" / "workspace" / "wl_report.Report"
VISUALS = sorted((REPORT / "definition" / "pages").rglob("visual.json"))
COLUMNS = {t: {c[0] for c in cols} for t, (_, _, cols) in build_model.TABLES.items()}
MEASURES = {t: {m[0] for m in ms} for t, ms in build_model.MEASURES.items()}


def projections(visual):
    for role in visual.get("visual", {}).get("query", {}).get("queryState", {}).values():
        yield from role.get("projections", [])


def test_report_points_at_the_semantic_model():
    pbir = json.loads((REPORT / "definition.pbir").read_text(encoding="utf-8"))
    target = (REPORT / pbir["datasetReference"]["byPath"]["path"]).resolve()
    assert target == (ROOT / "fabric" / "workspace" / "wl_model.SemanticModel").resolve()


def test_every_page_has_visuals():
    order = json.loads((REPORT / "definition" / "pages" / "pages.json").read_text(encoding="utf-8"))["pageOrder"]
    assert order[0] == "overview"
    for page in order:
        assert list((REPORT / "definition" / "pages" / page / "visuals").glob("*/visual.json")), f"{page} is empty"


@pytest.mark.parametrize("path", VISUALS, ids=lambda p: f"{p.parents[2].name}/{p.parent.name}")
def test_visual_fields_exist_in_the_model(path):
    visual = json.loads(path.read_text(encoding="utf-8"))
    fields = list(projections(visual))
    if visual["visual"]["visualType"] not in ("textbox", "shape", "image", "actionButton"):
        assert fields, "visual has no data bound"
    for p in fields:
        (kind, ref), = p["field"].items()
        table, name = ref["Expression"]["SourceRef"]["Entity"], ref["Property"]
        known = COLUMNS if kind == "Column" else MEASURES
        assert name in known.get(table, set()), f"{kind} {table}[{name}] is not in the model"
