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


def private_labeled_rows():
    rows = []
    for split in ("calibration", "holdout"):
        for index in range(10):
            track_id = f"private-{split}-{index:02d}"
            rows.extend(
                [
                    {
                        "track_id": track_id,
                        "split": split,
                        "position_ms": 6_000,
                        "raw_vocal_evidence": 0.05,
                        "vocal_conflict": 0,
                    },
                    {
                        "track_id": track_id,
                        "split": split,
                        "position_ms": 18_000,
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
    expected_low_risk = 0.25 / 100.5
    assert artifact["holdout"]["brier_score"] == pytest.approx(expected_low_risk**2, abs=1e-8)
    assert calibration.calibrated_risk(0.05, artifact) == pytest.approx(expected_low_risk, abs=1e-8)
    assert calibration.calibrated_risk(0.95, artifact) == pytest.approx(1 - expected_low_risk, abs=1e-8)
    assert artifact["regularization"] == calibration.REGULARIZATION
    assert all(0 < item["risk"] < 1 for item in artifact["bins"])

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


def test_private_audition_tier_is_small_disjoint_and_never_release_authorized():
    artifact = calibration.fit_artifact(
        private_labeled_rows(),
        yamnet_model_sha256=YAMNET_SHA,
        class_map_sha256=CLASS_MAP_SHA,
        reviewed=True,
        authorize_cuts=True,
        qualification_tier_name="private-audition",
    )

    assert artifact["qualification_tier"] == "private-audition"
    assert artifact["corpus"]["calibration_track_count"] == 10
    assert artifact["corpus"]["holdout_track_count"] == 10
    assert artifact["authorization"]["cuts_authorized"] is True

    release_claim = artifact.get("qualification_tier", "release") == "release"
    assert release_claim is False


def test_private_audition_tier_keeps_per_split_class_gates():
    rows = private_labeled_rows()
    rows = [
        row
        for row in rows
        if not (row["split"] == "holdout" and row["vocal_conflict"] == 1)
    ]
    with pytest.raises(calibration.VocalCalibrationError, match="positive or negative"):
        calibration.fit_artifact(
            rows,
            yamnet_model_sha256=YAMNET_SHA,
            class_map_sha256=CLASS_MAP_SHA,
            qualification_tier_name="private-audition",
        )


def test_private_regularization_passes_a_conservative_ambiguous_holdout():
    calibration_values = [
        (0.0, 0), (0.0, 0), (0.0, 0), (0.0, 0),
        (0.00390625, 0), (0.00390625, 0), (0.00390625, 0),
        (0.01171875, 0), (0.01953125, 0), (0.03125, 0),
        (0.05859375, 1), (0.109375, 1), (0.109375, 1), (0.109375, 1),
        (0.1484375, 1), (0.1484375, 1), (0.26171875, 1),
        (0.94140625, 1), (0.96875, 1), (0.96875, 1),
    ]
    holdout_values = [
        (0.0, 0), (0.0, 0), (0.00390625, 0), (0.00390625, 0),
        (0.00390625, 0), (0.00390625, 0), (0.00390625, 0), (0.03125, 0),
        (0.05859375, 0), (0.05859375, 1),
        (0.109375, 1), (0.109375, 1), (0.109375, 1),
        (0.1484375, 0), (0.1484375, 1), (0.1484375, 1), (0.1484375, 1),
        (0.73828125, 1), (0.73828125, 1), (0.80078125, 1),
    ]
    rows = []
    for split, values in (("calibration", calibration_values), ("holdout", holdout_values)):
        for index, (evidence, label) in enumerate(values):
            rows.append(
                {
                    "track_id": f"{split}-{index // 2}",
                    "split": split,
                    "position_ms": 6_000 + index * 10_000,
                    "raw_vocal_evidence": evidence,
                    "vocal_conflict": label,
                }
            )

    artifact = calibration.fit_artifact(
        rows,
        yamnet_model_sha256=YAMNET_SHA,
        class_map_sha256=CLASS_MAP_SHA,
        reviewed=True,
        authorize_cuts=True,
        qualification_tier_name="private-audition",
    )

    assert artifact["holdout"]["brier_score"] <= calibration.MAX_BRIER_SCORE
    assert (
        artifact["holdout"]["expected_calibration_error"]
        <= calibration.MAX_EXPECTED_CALIBRATION_ERROR
    )
    assert artifact["holdout"]["false_negative_rate_at_cut"] == 0
    assert artifact["authorization"]["cuts_authorized"] is True


def test_private_builder_emits_tier_and_accepts_exact_acknowledgement(tmp_path):
    input_path = tmp_path / "private-labels.jsonl"
    input_path.write_text(
        "".join(json.dumps(row) + "\n" for row in private_labeled_rows()),
        encoding="utf-8",
    )
    artifact = builder.build(
        input_path,
        tmp_path / "private-artifact.json",
        reviewed=True,
        authorize_cuts=True,
        acknowledgement=builder.REVIEW_ACKNOWLEDGEMENT,
        qualification_tier="private-audition",
    )
    assert artifact["qualification_tier"] == "private-audition"
