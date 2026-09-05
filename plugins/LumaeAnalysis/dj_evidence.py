"""Shared, pure source/model evidence consumed independently by V2 and V3."""

import hashlib
import json
import math
import numpy as np
from .vocal_calibration import (
    calibrated_risk as apply_vocal_calibration,
    qualification_tier as vocal_calibration_tier,
)
from .dj_contract import (
    BEAT_THIS_VERSION,
    MODEL_NAME,
    MODEL_SHA256,
    YAMNET_MODEL_NAME,
    YAMNET_MODEL_SHA256,
    YAMNET_SAMPLE_RATE,
    YAMNET_WINDOW_SAMPLES,
    YAMNET_HOP_SAMPLES,
    YAMNET_CLASS_MAP_SHA256,
    YAMNET_VOCAL_CLASSES,
    MODEL_SAMPLE_RATE,
    MODEL_FPS,
    MODEL_N_FFT,
    MODEL_HOP_SAMPLES,
    MODEL_MEL_BINS,
    MODEL_WINDOW_FRAMES,
    MODEL_BORDER_FRAMES,
    MAX_SOURCE_SECONDS,
    DjAnalysisError,
)


def canonical_json(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def analysis_digest(payload):
    public = {key: value for key, value in payload.items() if key != "analysis_digest"}
    return hashlib.sha256(canonical_json(public).encode("utf-8")).hexdigest()


def finite_vector(name, value, length=None):
    result = np.asarray(value, dtype=np.float64)
    if result.ndim != 1 or (length is not None and len(result) != length):
        raise DjAnalysisError("invalid_model_output", f"invalid {name} shape")
    if not np.all(np.isfinite(result)):
        raise DjAnalysisError("invalid_model_output", f"non-finite {name}")
    return result


def peak_frames(logits, radius=3):
    peaks = []
    for index, value in enumerate(logits):
        if value <= 0:
            continue
        begin = max(0, index - radius)
        end = min(len(logits), index + radius + 1)
        local = logits[begin:end]
        if value != np.max(local):
            continue
        # A plateau has one deterministic owner.
        if begin + int(np.flatnonzero(local == value)[0]) != index:
            continue
        if peaks and index - peaks[-1] <= 1:
            if logits[index] > logits[peaks[-1]]:
                peaks[-1] = index
            continue
        peaks.append(index)
    return peaks


def sigmoid(value):
    value = float(value)
    if value >= 0:
        factor = math.exp(-value)
        return 1 / (1 + factor)
    factor = math.exp(value)
    return factor / (1 + factor)


def match_downbeats(beat_frames, raw_downbeat_frames, fps):
    matched = []
    if not beat_frames:
        return matched
    beats = np.asarray(beat_frames, dtype=np.int64)
    for raw in raw_downbeat_frames:
        position = int(np.searchsorted(beats, raw))
        choices = [
            index for index in (position - 1, position) if 0 <= index < len(beats)
        ]
        beat_index = min(
            choices, key=lambda index: (abs(int(beats[index]) - raw), index)
        )
        delta = abs(int(beats[beat_index]) - raw) / fps
        matched.append(
            {
                "beat_index": beat_index,
                "raw_frame": int(raw),
                "snapped_frame": int(beats[beat_index]),
                "alignment_seconds": delta,
            }
        )
    # Multiple raw peaks may snap to one beat. Keep the best raw alignment.
    deduped = {}
    for item in matched:
        old = deduped.get(item["beat_index"])
        if old is None or item["alignment_seconds"] < old["alignment_seconds"]:
            deduped[item["beat_index"]] = item
    return [deduped[key] for key in sorted(deduped)]


def key_evidence(value):
    value = value if isinstance(value, dict) else {}
    key = value.get("key")
    scale = value.get("scale")
    confidence = value.get("confidence")
    qualified = (
        value.get("method") == "lumae-chroma-key-v1"
        and isinstance(key, int)
        and not isinstance(key, bool)
        and 0 <= key <= 11
        and scale in ("major", "minor")
        and isinstance(confidence, (int, float))
        and not isinstance(confidence, bool)
        and math.isfinite(confidence)
        and confidence >= 0.8
        and value.get("ambiguous") is False
    )
    return {
        "method": value.get("method") if isinstance(value.get("method"), str) else None,
        "key": (
            key
            if isinstance(key, int) and not isinstance(key, bool) and 0 <= key <= 11
            else None
        ),
        "scale": scale if scale in ("major", "minor") else None,
        "confidence": (
            round(float(confidence), 8)
            if isinstance(confidence, (int, float))
            and not isinstance(confidence, bool)
            and math.isfinite(confidence)
            else None
        ),
        "qualified": qualified,
        "pitch_shift_allowed": qualified,
    }


def vocal_risk_timeline(value, duration_seconds, calibration_artifact=None):
    calibration_cache_key = (
        calibration_artifact["artifact_digest"]
        if calibration_artifact is not None
        else "uncalibrated-v1"
    )
    if value is None:
        return {
            "method": YAMNET_MODEL_NAME,
            "class_map_sha256": YAMNET_CLASS_MAP_SHA256,
            "classes": [
                {"index": index, "name": name}
                for index, name in YAMNET_VOCAL_CLASSES.items()
            ],
            "frames": [],
            "calibration": {
                "status": "missing",
                "scores_are_probabilities": False,
                "cuts_authorized": False,
                "cache_key": calibration_cache_key,
            },
        }
    positions = value.get("positions_ms")
    scores = value.get("scores")
    indices = value.get("class_indices")
    expected_indices = list(YAMNET_VOCAL_CLASSES)
    if (
        list(indices or ()) != expected_indices
        or not isinstance(positions, (list, tuple))
        or not isinstance(scores, (list, tuple))
    ):
        raise DjAnalysisError("invalid_yamnet_output")
    if len(positions) != len(scores) or len(positions) > int(duration_seconds * 4) + 8:
        raise DjAnalysisError("invalid_yamnet_output")
    frames = []
    previous = -1
    for position, row in zip(positions, scores):
        if (
            isinstance(position, bool)
            or not isinstance(position, int)
            or position < 0
            or position > int(round(duration_seconds * 1000)) + 1000
            or position <= previous
            or not isinstance(row, (list, tuple))
            or len(row) != len(expected_indices)
        ):
            raise DjAnalysisError("invalid_yamnet_output")
        numeric = np.asarray(row, dtype=np.float64)
        if (
            not np.all(np.isfinite(numeric))
            or np.any(numeric < 0)
            or np.any(numeric > 1)
        ):
            raise DjAnalysisError("invalid_yamnet_output")
        raw_evidence = round(float(np.max(numeric)), 8)
        frames.append(
            {
                "position_ms": position,
                "raw_vocal_evidence": raw_evidence,
                "dominant_class_index": expected_indices[int(np.argmax(numeric))],
                "calibrated_risk": (
                    round(
                        apply_vocal_calibration(raw_evidence, calibration_artifact), 8
                    )
                    if calibration_artifact is not None
                    else None
                ),
            }
        )
        previous = position
    calibration = {
        "status": "ready" if calibration_artifact is not None else "uncalibrated",
        "scores_are_probabilities": False,
        "cuts_authorized": bool(
            calibration_artifact
            and calibration_artifact["authorization"]["cuts_authorized"]
        ),
        "cache_key": calibration_cache_key,
    }
    if calibration_artifact is not None:
        calibration.update(
            {
                "method": calibration_artifact["method"],
                "artifact_digest": calibration_artifact["artifact_digest"],
                "label_definition": calibration_artifact["label_definition"],
                "holdout": calibration_artifact["holdout"],
                "calibration_tier": vocal_calibration_tier(calibration_artifact),
                "release_authorized": bool(
                    vocal_calibration_tier(calibration_artifact) == "release"
                    and calibration_artifact["authorization"]["cuts_authorized"]
                ),
            }
        )
    return {
        "method": YAMNET_MODEL_NAME,
        "class_map_sha256": YAMNET_CLASS_MAP_SHA256,
        "classes": [
            {"index": index, "name": name}
            for index, name in YAMNET_VOCAL_CLASSES.items()
        ],
        "frames": frames,
        "calibration": calibration,
    }


def validate_annotation(
    model_output,
    *,
    catalog_instance_id,
    track_id,
    media_revision,
    representation_id,
    content_sha256,
    duration_seconds,
):
    for token in (catalog_instance_id, track_id):
        if not isinstance(token, str) or not 0 < len(token) <= 512:
            raise DjAnalysisError("invalid_source_identity")
    if (
        not isinstance(media_revision, str)
        or not media_revision.startswith("sha256:")
        or len(media_revision) != 71
    ):
        raise DjAnalysisError("invalid_source_identity")
    if representation_id != f"sha256:{content_sha256}" or len(content_sha256) != 64:
        raise DjAnalysisError("invalid_source_identity")
    if (
        isinstance(duration_seconds, bool)
        or not isinstance(duration_seconds, (int, float))
        or not math.isfinite(duration_seconds)
        or not 0 < duration_seconds <= MAX_SOURCE_SECONDS
    ):
        raise DjAnalysisError("source_duration_unsupported")

    beat_logits = finite_vector("beat_logits", model_output.get("beat_logits"))
    frame_count = len(beat_logits)
    downbeat_logits = finite_vector(
        "downbeat_logits", model_output.get("downbeat_logits"), frame_count
    )
    energy = finite_vector("energy", model_output.get("energy"), frame_count)
    spectral_flux = finite_vector(
        "spectral_flux", model_output.get("spectral_flux"), frame_count
    )
    if abs(frame_count / MODEL_FPS - duration_seconds) > 0.5:
        raise DjAnalysisError("model_timeline_mismatch")

    return beat_logits, downbeat_logits, energy, spectral_flux


def model_identity():
    return {
        "package": f"beat-this-{BEAT_THIS_VERSION}",
        "checkpoint": MODEL_NAME,
        "checkpoint_sha256": MODEL_SHA256,
        "frame_rate": MODEL_FPS,
        "sample_rate": MODEL_SAMPLE_RATE,
        "n_fft": MODEL_N_FFT,
        "hop_samples": MODEL_HOP_SAMPLES,
        "mel_bins": MODEL_MEL_BINS,
        "window_frames": MODEL_WINDOW_FRAMES,
        "border_frames": MODEL_BORDER_FRAMES,
        "overlap_mode": "keep_first",
        "short_end_mode": "shift_to_end",
        "output_semantics": "uncalibrated_logits",
        "downbeat_snapping_authorizes_alignment": False,
        "yamnet": {
            "name": YAMNET_MODEL_NAME,
            "artifact_sha256": YAMNET_MODEL_SHA256,
            "sample_rate": YAMNET_SAMPLE_RATE,
            "window_samples": YAMNET_WINDOW_SAMPLES,
            "hop_samples": YAMNET_HOP_SAMPLES,
            "output_semantics": "uncalibrated_scores",
        },
    }
