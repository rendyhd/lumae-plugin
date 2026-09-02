"""Versioned held-out calibration for YAMNet vocal-conflict evidence.

Raw YAMNet scores are never probabilities.  This module can fit and validate a
track-disjoint isotonic mapping, but it authorizes structural cuts only when the
fixed corpus and holdout gates pass and the artifact was explicitly reviewed.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path


SCHEMA_VERSION = 1
METHOD = "lumae-yamnet-vocal-risk-isotonic-v1"
LABEL_DEFINITION = "audible-vocal-conflict-v1"
MAPPING = "right-continuous-isotonic-v1"
MIN_CALIBRATION_TRACKS = 100
MIN_HOLDOUT_TRACKS = 200
MIN_POSITIVE_FRAMES_PER_SPLIT = 100
MIN_NEGATIVE_FRAMES_PER_SPLIT = 100
CUT_RISK_THRESHOLD = 0.25
MAX_BRIER_SCORE = 0.18
MAX_EXPECTED_CALIBRATION_ERROR = 0.08
MAX_FALSE_NEGATIVE_RATE_AT_CUT = 0.05
MAX_ARTIFACT_BYTES = 1_000_000


class VocalCalibrationError(ValueError):
    pass


def canonical_json(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def artifact_digest(value):
    public = {key: item for key, item in value.items() if key != "artifact_digest"}
    return hashlib.sha256(canonical_json(public).encode("utf-8")).hexdigest()


def _number(value, name, minimum=0.0, maximum=1.0):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise VocalCalibrationError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise VocalCalibrationError(f"{name} is out of range")
    return result


def _integer(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise VocalCalibrationError(f"{name} must be an integer >= {minimum}")
    return value


def validate_artifact(
    value,
    *,
    yamnet_model_sha256,
    class_map_sha256,
):
    if not isinstance(value, dict):
        raise VocalCalibrationError("calibration artifact must be an object")
    if value.get("schema_version") != SCHEMA_VERSION or value.get("method") != METHOD:
        raise VocalCalibrationError("unsupported calibration contract")
    if value.get("label_definition") != LABEL_DEFINITION or value.get("mapping") != MAPPING:
        raise VocalCalibrationError("unsupported calibration semantics")
    if value.get("yamnet_model_sha256") != yamnet_model_sha256:
        raise VocalCalibrationError("calibration YAMNet identity mismatch")
    if value.get("class_map_sha256") != class_map_sha256:
        raise VocalCalibrationError("calibration class-map identity mismatch")
    if value.get("artifact_digest") != artifact_digest(value):
        raise VocalCalibrationError("calibration artifact digest mismatch")

    corpus = value.get("corpus")
    if not isinstance(corpus, dict):
        raise VocalCalibrationError("calibration corpus is missing")
    calibration_tracks = _integer(
        corpus.get("calibration_track_count"),
        "calibration_track_count",
        MIN_CALIBRATION_TRACKS,
    )
    holdout_tracks = _integer(
        corpus.get("holdout_track_count"), "holdout_track_count", MIN_HOLDOUT_TRACKS
    )
    calibration_frames = _integer(corpus.get("calibration_frame_count"), "calibration_frame_count", 1)
    holdout_frames = _integer(corpus.get("holdout_frame_count"), "holdout_frame_count", 1)
    for name in (
        "calibration_positive_frames",
        "calibration_negative_frames",
        "holdout_positive_frames",
        "holdout_negative_frames",
    ):
        _integer(corpus.get(name), name, MIN_POSITIVE_FRAMES_PER_SPLIT)
    manifest_digest = corpus.get("manifest_sha256")
    if not isinstance(manifest_digest, str) or len(manifest_digest) != 64:
        raise VocalCalibrationError("invalid calibration manifest digest")
    try:
        int(manifest_digest, 16)
    except ValueError as exc:
        raise VocalCalibrationError("invalid calibration manifest digest") from exc
    if calibration_tracks + holdout_tracks < 300 or calibration_frames + holdout_frames < 400:
        raise VocalCalibrationError("calibration corpus is incomplete")

    bins = value.get("bins")
    if not isinstance(bins, list) or not 2 <= len(bins) <= 512:
        raise VocalCalibrationError("invalid isotonic bins")
    previous_max = -1.0
    previous_risk = -1.0
    normalized_bins = []
    for index, item in enumerate(bins):
        if not isinstance(item, dict):
            raise VocalCalibrationError("invalid isotonic bin")
        maximum = _number(item.get("max_raw_evidence"), f"bins[{index}].max_raw_evidence")
        risk = _number(item.get("risk"), f"bins[{index}].risk")
        if maximum <= previous_max or risk < previous_risk:
            raise VocalCalibrationError("isotonic bins are not monotonic")
        normalized_bins.append({"max_raw_evidence": maximum, "risk": risk})
        previous_max = maximum
        previous_risk = risk
    if normalized_bins[-1]["max_raw_evidence"] != 1.0:
        raise VocalCalibrationError("isotonic bins must cover raw evidence through 1.0")

    holdout = value.get("holdout")
    if not isinstance(holdout, dict):
        raise VocalCalibrationError("holdout measurements are missing")
    brier = _number(holdout.get("brier_score"), "brier_score")
    ece = _number(holdout.get("expected_calibration_error"), "expected_calibration_error")
    false_negative = _number(
        holdout.get("false_negative_rate_at_cut"), "false_negative_rate_at_cut"
    )
    if holdout.get("cut_risk_threshold") != CUT_RISK_THRESHOLD:
        raise VocalCalibrationError("holdout cut threshold mismatch")
    metrics_pass = (
        brier <= MAX_BRIER_SCORE
        and ece <= MAX_EXPECTED_CALIBRATION_ERROR
        and false_negative <= MAX_FALSE_NEGATIVE_RATE_AT_CUT
    )
    authorization = value.get("authorization")
    if not isinstance(authorization, dict) or not isinstance(authorization.get("reviewed"), bool):
        raise VocalCalibrationError("calibration review state is missing")
    cuts_authorized = authorization.get("cuts_authorized")
    if not isinstance(cuts_authorized, bool):
        raise VocalCalibrationError("calibration cut authorization is invalid")
    if cuts_authorized and (not authorization["reviewed"] or not metrics_pass):
        raise VocalCalibrationError("calibration cannot authorize cuts")

    normalized = json.loads(canonical_json(value))
    normalized["bins"] = normalized_bins
    return normalized


def load_artifact(path, *, yamnet_model_sha256, class_map_sha256):
    raw = str(path or "").strip()
    if not raw:
        return None
    resolved = Path(raw).expanduser().resolve(strict=True)
    info = resolved.stat()
    if not resolved.is_file() or info.st_size <= 0 or info.st_size > MAX_ARTIFACT_BYTES:
        raise VocalCalibrationError("invalid calibration artifact file")
    with resolved.open("r", encoding="utf-8") as source:
        value = json.load(source)
    return validate_artifact(
        value,
        yamnet_model_sha256=yamnet_model_sha256,
        class_map_sha256=class_map_sha256,
    )


def calibrated_risk(raw_evidence, artifact):
    score = _number(raw_evidence, "raw_vocal_evidence")
    if not artifact:
        return None
    for item in artifact["bins"]:
        if score <= item["max_raw_evidence"]:
            return float(item["risk"])
    raise VocalCalibrationError("calibration bins do not cover raw evidence")


def _normalize_rows(rows):
    normalized = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise VocalCalibrationError(f"row {index} must be an object")
        track_id = row.get("track_id")
        split = row.get("split")
        label = row.get("vocal_conflict")
        if not isinstance(track_id, str) or not track_id or len(track_id) > 512:
            raise VocalCalibrationError(f"row {index} has an invalid track_id")
        if split not in ("calibration", "holdout") or label not in (0, 1):
            raise VocalCalibrationError(f"row {index} has invalid split or label")
        normalized.append(
            {
                "track_id": track_id,
                "split": split,
                "position_ms": _integer(row.get("position_ms"), "position_ms"),
                "raw_vocal_evidence": _number(row.get("raw_vocal_evidence"), "raw_vocal_evidence"),
                "vocal_conflict": int(label),
            }
        )
    return normalized


def _fit_isotonic(rows):
    grouped = []
    for row in sorted(rows, key=lambda item: item["raw_vocal_evidence"]):
        score = row["raw_vocal_evidence"]
        if grouped and grouped[-1]["score"] == score:
            grouped[-1]["sum"] += row["vocal_conflict"]
            grouped[-1]["count"] += 1
        else:
            grouped.append({"score": score, "sum": row["vocal_conflict"], "count": 1})
    blocks = []
    for item in grouped:
        blocks.append(
            {
                "max": item["score"],
                "sum": item["sum"],
                "count": item["count"],
            }
        )
        while len(blocks) >= 2:
            left, right = blocks[-2], blocks[-1]
            if left["sum"] / left["count"] <= right["sum"] / right["count"]:
                break
            blocks[-2:] = [
                {
                    "max": right["max"],
                    "sum": left["sum"] + right["sum"],
                    "count": left["count"] + right["count"],
                }
            ]
    bins = [
        {"max_raw_evidence": round(item["max"], 8), "risk": round(item["sum"] / item["count"], 8)}
        for item in blocks
    ]
    if bins[-1]["max_raw_evidence"] < 1.0:
        bins.append({"max_raw_evidence": 1.0, "risk": bins[-1]["risk"]})
    return bins


def _risk_from_bins(score, bins):
    for item in bins:
        if score <= item["max_raw_evidence"]:
            return item["risk"]
    return bins[-1]["risk"]


def _holdout_metrics(rows, bins):
    predictions = [(_risk_from_bins(row["raw_vocal_evidence"], bins), row["vocal_conflict"]) for row in rows]
    brier = sum((risk - label) ** 2 for risk, label in predictions) / len(predictions)
    weighted_error = 0.0
    for bucket in range(10):
        lower = bucket / 10
        upper = (bucket + 1) / 10
        selected = [item for item in predictions if lower <= item[0] <= upper if bucket == 9 or item[0] < upper]
        if selected:
            mean_risk = sum(item[0] for item in selected) / len(selected)
            prevalence = sum(item[1] for item in selected) / len(selected)
            weighted_error += len(selected) / len(predictions) * abs(mean_risk - prevalence)
    positives = [item for item in predictions if item[1] == 1]
    false_negative = sum(risk <= CUT_RISK_THRESHOLD for risk, _ in positives) / len(positives)
    return {
        "frame_count": len(rows),
        "brier_score": round(brier, 8),
        "expected_calibration_error": round(weighted_error, 8),
        "false_negative_rate_at_cut": round(false_negative, 8),
        "cut_risk_threshold": CUT_RISK_THRESHOLD,
    }


def fit_artifact(
    rows,
    *,
    yamnet_model_sha256,
    class_map_sha256,
    reviewed=False,
    authorize_cuts=False,
):
    normalized = _normalize_rows(rows)
    calibration = [row for row in normalized if row["split"] == "calibration"]
    holdout = [row for row in normalized if row["split"] == "holdout"]
    calibration_tracks = {row["track_id"] for row in calibration}
    holdout_tracks = {row["track_id"] for row in holdout}
    if calibration_tracks & holdout_tracks:
        raise VocalCalibrationError("calibration and holdout tracks overlap")
    if len(calibration_tracks) < MIN_CALIBRATION_TRACKS or len(holdout_tracks) < MIN_HOLDOUT_TRACKS:
        raise VocalCalibrationError("calibration corpus does not meet the track-count gates")
    for split_name, split_rows in (("calibration", calibration), ("holdout", holdout)):
        positives = sum(row["vocal_conflict"] for row in split_rows)
        negatives = len(split_rows) - positives
        if positives < MIN_POSITIVE_FRAMES_PER_SPLIT or negatives < MIN_NEGATIVE_FRAMES_PER_SPLIT:
            raise VocalCalibrationError(f"{split_name} split lacks positive or negative labels")
    bins = _fit_isotonic(calibration)
    measurements = _holdout_metrics(holdout, bins)
    corpus = {
        "calibration_track_count": len(calibration_tracks),
        "holdout_track_count": len(holdout_tracks),
        "calibration_frame_count": len(calibration),
        "holdout_frame_count": len(holdout),
        "calibration_positive_frames": sum(row["vocal_conflict"] for row in calibration),
        "calibration_negative_frames": sum(1 - row["vocal_conflict"] for row in calibration),
        "holdout_positive_frames": sum(row["vocal_conflict"] for row in holdout),
        "holdout_negative_frames": sum(1 - row["vocal_conflict"] for row in holdout),
        "manifest_sha256": hashlib.sha256(canonical_json(sorted(normalized, key=lambda item: (item["split"], item["track_id"], item["position_ms"]))).encode("utf-8")).hexdigest(),
    }
    metrics_pass = (
        measurements["brier_score"] <= MAX_BRIER_SCORE
        and measurements["expected_calibration_error"] <= MAX_EXPECTED_CALIBRATION_ERROR
        and measurements["false_negative_rate_at_cut"] <= MAX_FALSE_NEGATIVE_RATE_AT_CUT
    )
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "method": METHOD,
        "label_definition": LABEL_DEFINITION,
        "mapping": MAPPING,
        "yamnet_model_sha256": yamnet_model_sha256,
        "class_map_sha256": class_map_sha256,
        "corpus": corpus,
        "bins": bins,
        "holdout": measurements,
        "authorization": {
            "reviewed": bool(reviewed),
            "cuts_authorized": bool(authorize_cuts and reviewed and metrics_pass),
        },
    }
    artifact["artifact_digest"] = artifact_digest(artifact)
    return validate_artifact(
        artifact,
        yamnet_model_sha256=yamnet_model_sha256,
        class_map_sha256=class_map_sha256,
    )
