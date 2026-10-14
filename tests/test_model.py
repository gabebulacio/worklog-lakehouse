"""The semantic model: TMDL in sync with its generator, and structurally sound."""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "local"))
import build_model  # noqa: E402

ITEM = ROOT / "fabric" / "workspace" / "wl_model.SemanticModel"


def test_committed_tmdl_matches_the_generator(tmp_path):
    build_model.write_model(tmp_path, "directLake")
    for generated in sorted((tmp_path / "definition").rglob("*.tmdl")):
        committed = ITEM / generated.relative_to(tmp_path)
        assert committed.exists(), f"missing {committed.name}: run python run.py model"
        assert committed.read_text(encoding="utf-8") == generated.read_text(encoding="utf-8"),             f"{committed.name} drifted from local/build_model.py: run python run.py model"


def test_every_partition_is_direct_lake_on_a_gold_table():
    for f in (ITEM / "definition" / "tables").glob("*.tmdl"):
        text = f.read_text(encoding="utf-8")
        assert "mode: directLake" in text and re.search(r"entityName: gold_\w+", text), f.name


def test_relationships_and_measure_references_resolve():
    tables = {t: {c[0] for c in cols} for t, (_, _, cols) in build_model.TABLES.items()}
    measures = {m[0] for ms in build_model.MEASURES.values() for m in ms}
    for f, fc, t, tc in build_model.RELATIONSHIPS:
        assert fc in tables[f] and tc in tables[t], (f, fc, t, tc)
    for ms in build_model.MEASURES.values():
        for name, _, _, desc, dax in ms:
            assert desc, f"{name} needs a description"
            for table, col in re.findall(r"'?([A-Za-z][\w -]*?)'?\[([^\]]+)\]", dax):
                assert col in tables[table.strip()], f"{name}: {table}[{col}] does not exist"
            for ref in re.findall(r"(?<![\w'\]])\[([^\]]+)\]", dax):
                assert ref in measures, f"{name}: unknown measure [{ref}]"


def test_date_table_is_marked():
    text = (ITEM / "definition" / "tables" / "Date.tmdl").read_text(encoding="utf-8")
    assert "dataCategory: Time" in text and "isKey" in text
