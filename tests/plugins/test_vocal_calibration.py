import copy
import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "plugins" / "LumaeAnalysis" / "vocal_calibration.py"
spec = importlib.util.spec_from_file_location("lumae_vocal_calibration_test", SOURCE)
calibration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calibration)
builder_spec = importlib.util.spec_from_file_location(
    "lumae_vocal_calibration_builder_test",
    ROOT / "scripts" / "build_vocal_calibration.py",
)
builder = importlib.util.module_from_spec(builder_spec)
builder_spec.loader.exec_module(builder)

YAMNET_SHA = "a" * 64
CLASS_MAP_SHA = "b" * 64


def labeled_rows():
    rows = []
    for split, count in (("calibration", 100), ("holdout", 200)):
        for index in range(count):
            track_id = f"{split}-{index:03d}"
            rows.extend(
                [
                    {
                        "track_id": track_id,
                        "split": split,
                        "position_ms": 480,
                        "raw_vocal_evidence": 0.05,
                        "vocal_conflict": 0,
                    },
                    {
                        "track_id": track_id,
                        "split": split,
                        "position_ms": 960,
                        "raw_vocal_evidence": 0.95,
                        "vocal_conflict": 1,
                    },
                ]
            )
    return rows


def test_track_disjoint_held_out_artifact_can_authorize_cuts(tmp_path):
    artifact = calibration.fit_artifact(
        labeled_rows(),
        yamnet_model_sha256=YAMNET_SHA,
        class_map_sha256=CLASS_MAP_SHA,
        reviewed=True,
        authorize_cuts=True,
    )

    assert artifact["corpus"]["calibration_track_count"] == 100
    assert artifact["corpus"]["holdout_track_count"] == 200
    assert artifact["authorization"] == {"reviewed": True, "cuts_authorized": True}
    assert artifact["holdout"]["brier_score"] == 0
    assert calibration.calibrated_risk(0.05, artifact) == 0
    assert calibration.calibrated_risk(0.95, artifact) == 1

    path = tmp_path / "calibration.json"
    path.write_text(calibration.canonical_json(artifact), encoding="utf-8")
    assert calibration.load_artifact(
        path,
        yamnet_model_sha256=YAMNET_SHA,
        class_map_sha256=CLASS_MAP_SHA,
    ) == artifact


def test_artifact_fails_closed_for_overlap_tampering_and_unreviewed_output():
    rows = labeled_rows()
    rows[-1]["track_id"] = "calibration-000"
    with pytest.raises(calibration.VocalCalibrationError, match="overlap"):
        calibration.fit_artifact(
            rows,
            yamnet_model_sha256=YAMNET_SHA,
            class_map_sha256=CLASS_MAP_SHA,
        )

    artifact = calibration.fit_artifact(
        labeled_rows(),
        yamnet_model_sha256=YAMNET_SHA,
        class_map_sha256=CLASS_MAP_SHA,
        reviewed=False,
        authorize_cuts=True,
    )
    assert artifact["authorization"]["cuts_authorized"] is False

    tampered = copy.deepcopy(artifact)
    tampered["bins"][0]["risk"] = 0.2
    with pytest.raises(calibration.VocalCalibrationError, match="digest mismatch"):
        calibration.validate_artifact(
            tampered,
            yamnet_model_sha256=YAMNET_SHA,
            class_map_sha256=CLASS_MAP_SHA,
        )


def test_builder_requires_exact_review_acknowledgement(tmp_path):
    input_path = tmp_path / "labels.jsonl"
    input_path.write_text(
        "".join(json.dumps(row) + "\n" for row in labeled_rows()),
        encoding="utf-8",
    )
    output_path = tmp_path / "artifact.json"

    with pytest.raises(ValueError, match="exact review acknowledgement"):
        builder.build(
            input_path,
            output_path,
            reviewed=True,
            authorize_cuts=True,
            acknowledgement="yes",
        )

    artifact = builder.build(
        input_path,
        output_path,
        reviewed=True,
        authorize_cuts=True,
        acknowledgement=builder.REVIEW_ACKNOWLEDGEMENT,
    )
    assert output_path.exists()
    assert artifact["authorization"]["cuts_authorized"] is True
