import importlib.util
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "plugins" / "LumaeAnalysis" / "dj_coverage.py"
FIXTURE_PATH = ROOT / "tests" / "plugins" / "dj_coverage_fixture.json"


def load_module():
    spec = importlib.util.spec_from_file_location("lumae_dj_coverage", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def fixture():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_coverage_is_deterministic_and_identifier_free():
    coverage = load_module()
    data = fixture()
    first = coverage.evaluate_coverage(data["analyses"], data["manifest"])
    second = coverage.evaluate_coverage(reversed(data["analyses"]), data["manifest"])

    assert first == second
    assert first["analysis"] == {
        "total": 4,
        "v2": 2,
        "v3": 2,
        "unsupported": 0,
        "rhythm_ready": 4,
        "entry_ready": 3,
        "exit_ready": 3,
        "vocal_ready": 4,
        "edge_ready": 2,
    }
    assert first["pairs"]["total"] == 3
    assert first["pairs"]["tiers"]["phrase-sync"] == 1
    assert first["pairs"]["tiers"]["tempo-independent"] == 1
    assert first["pairs"]["tiers"]["edge-fx"] == 1
    rendered = json.dumps(first, sort_keys=True)
    for track_id in ("v2-a", "v2-b", "v3-a", "v3-b"):
        assert track_id not in rendered


def test_coverage_reports_rejections_and_queue_improvement():
    coverage = load_module()
    data = fixture()
    analyses = data["analyses"]
    analyses[3]["vocal_risk"]["calibration"]["cuts_authorized"] = False
    report = coverage.evaluate_coverage(analyses, data["manifest"])

    assert report["pairs"]["tiers"]["smoothfade"] >= 1
    assert report["pairs"]["rejections"]["vocal-calibration-unavailable"] >= 1
    assert report["region_rejections"] == {"unstable_tempo": 1}
    assert report["ordering"]["comparisons"] == 1


def test_unsupported_stored_analysis_counts_as_unavailable():
    coverage = load_module()
    data = fixture()
    data["analyses"].append({"schema_version": 1, "track_id": "legacy"})
    data["manifest"] = {"pairs": [{"from": "legacy", "to": "v2-a"}]}

    report = coverage.evaluate_coverage(data["analyses"], data["manifest"])

    assert report["analysis"]["total"] == 5
    assert report["analysis"]["unsupported"] == 1
    assert report["pairs"]["tiers"]["smoothfade"] == 1
    assert report["pairs"]["rejections"] == {
        "unsupported-analysis-version": 1,
    }