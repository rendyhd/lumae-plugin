"""Producer regressions from the comprehensive Radio DJ review."""

import importlib
import numpy as np
import pytest
from test_dj_analysis_v3 import v3, model_output, build, vocal, v2
from test_dj_analysis import dj
from test_dj_coverage import load_module, fixture


def test_constant_high_speech_is_never_a_speech_minimum():
    output = model_output(20)
    for key in ("low_energy", "mid_energy", "high_energy"):
        output[key][:] = 10
    risk = {
        "calibration": {"cuts_authorized": True},
        "frames": [
            {"position_ms": 5000, "calibrated_risk": 0.99, "dominant_class_index": 0}
        ],
    }
    intervals = v3._speech_safe_intervals(output, risk, 20000)
    assert intervals == []
    assert v3._safe_at(5000, risk, intervals)[0] is False


def test_low_risk_does_not_override_cut_authorization():
    risk = {
        "calibration": {"cuts_authorized": False},
        "frames": [
            {"position_ms": 5000, "calibrated_risk": 0.01, "dominant_class_index": 0}
        ],
    }
    assert v3._safe_at(5000, risk, [{"start_ms": 4900, "end_ms": 5100}])[0] is False
    rows = []
    for split, count in (("calibration", 100), ("holdout", 200)):
        for index in range(count):
            for raw, label in ((0.05, 0), (0.95, 1)):
                rows.append(
                    {
                        "track_id": f"{split}-{index}",
                        "split": split,
                        "position_ms": int(raw * 10000),
                        "raw_vocal_evidence": raw,
                        "vocal_conflict": label,
                    }
                )
    artifact = vocal.fit_artifact(
        rows,
        yamnet_model_sha256=dj.YAMNET_MODEL_SHA256,
        class_map_sha256=dj.YAMNET_CLASS_MAP_SHA256,
        reviewed=True,
        authorize_cuts=False,
    )
    positions = list(range(0, 192000, 480))
    result = v3.build_dj_analysis_v3(
        model_output(),
        catalog_instance_id="catalog-a",
        track_id="track-a",
        media_revision="sha256:" + "a" * 64,
        representation_id="sha256:" + "b" * 64,
        content_sha256="b" * 64,
        duration_seconds=192,
        source={},
        vocal_calibration=artifact,
        vocal_output={
            "positions_ms": positions,
            "class_indices": list(dj.YAMNET_VOCAL_CLASSES),
            "scores": [[0.05] * len(dj.YAMNET_VOCAL_CLASSES) for _ in positions],
        },
    )
    cues = result["cue_candidates"]
    assert cues and all(
        not cue["speech_safe"] and not cue.get("loop_verified") for cue in cues
    )
    assert not any(cue["role"] == "cut" for cue in cues)


def test_long_track_keeps_tail_cues_and_diverse_timestamps():
    result = build(model_output(1800))
    assert max(region["end_ms"] for region in result["rhythm"]["regions"]) > 1700000
    assert result["cue_rankings"]["exit"]
    cues = result["cue_candidates"]
    for role in ("entry", "exit", "loop", "cut"):
        times = [cue["timestamp_ms"] for cue in cues if cue["role"] == role]
        assert len(times) == len(set(times))
    assert (
        max(window["end_ms"] for window in result["band_energy"]["windows"]) > 1700000
    )
    assert result["natural_boundaries"]["entry_ms"] == 0
    assert result["natural_boundaries"]["exit_ms"] == 1800000
    assert result["suggested_audible_boundaries"]["trim_authorized"] is False
    assert all(cue.get("loop_verified") is not True for cue in cues)


def test_v3_projection_does_not_build_v2_first(monkeypatch):
    monkeypatch.setattr(
        v2,
        "build_dj_analysis",
        lambda *args, **kwargs: pytest.fail("V3 must be independent"),
    )
    assert build()["schema_version"] == 3


def test_raw_evidence_is_immutable():
    runtime = dj.dj_runtime
    value = runtime._freeze({"array": np.array([1.0, 2.0]), "scores": [[0.1, 0.2]]})
    with pytest.raises(ValueError):
        value["array"][0] = 3
    with pytest.raises(TypeError):
        value["scores"] = []
    assert value["scores"] == ((0.1, 0.2),)


def test_coverage_uses_cue_tempo_and_speech_not_unrelated_region():
    coverage = load_module()
    data = fixture()
    left, right = data["analyses"][2:]
    left["cue_candidates"][0].update(local_tempo_bpm=80, confidence_tier="high")
    right["cue_candidates"][0].update(local_tempo_bpm=120, confidence_tier="high")
    for item in (left, right):
        item["rhythm"]["regions"].append(
            {"normalized_tempo_bpm": 120, "confidence_tier": "high"}
        )
    assert (
        coverage.classify_pair(
            coverage.track_evidence(left), coverage.track_evidence(right)
        )[0]
        != "phrase-sync"
    )
    left["cue_candidates"][0]["speech_safe"] = False
    evidence = coverage.track_evidence(left)
    assert not evidence.cues
    assert (
        coverage.evaluate_coverage(data["analyses"])["playback_qualification"] is False
    )


def test_speech_minimum_budget_preserves_late_track_evidence():
    output = model_output(600)
    output["low_energy"][:] = 0
    output["mid_energy"][:] = 1
    output["high_energy"][:] = 1
    frames = []
    for frame in range(250, 30000, 250):
        output["mid_energy"][frame - 15 : frame + 16] = 0
        output["high_energy"][frame - 15 : frame + 16] = 0
        frames.append(
            {
                "position_ms": frame * 20,
                "calibrated_risk": 0.9,
                "dominant_class_index": 0,
            }
        )
    intervals = v3._speech_safe_intervals(
        output, {"calibration": {"cuts_authorized": True}, "frames": frames}, 600000
    )
    assert len(intervals) == 32
    assert intervals[-1]["end_ms"] > 590000
