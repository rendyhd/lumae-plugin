"""Optional, source-bound DJ analysis for Lumae.

Beat This is never imported by request/health paths and its short-name model
loader is never used. A worker must be explicitly enabled with a local,
checksum-pinned checkpoint. The pure annotation builder is kept independent of
the optional runtime so its musical guards can be tested without PyTorch.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import inspect
import json
import math
import os
import stat
import tempfile
import time
from pathlib import Path

import numpy as np

from .vocal_calibration import (
    calibrated_risk as apply_vocal_calibration,
    qualification_tier as vocal_calibration_tier,
)


SCHEMA_VERSION = 2
METHOD = "beat-this-1.1.0-yamnet-lite-1-lumae-dj-v2.3"
BEAT_THIS_VERSION = "1.1.0"
MODEL_NAME = "final0"
MODEL_URL = (
    "https://cloud.cp.jku.at/public.php/dav/files/7ik4RrBKTS273gp/final0.ckpt"
)
MODEL_BYTES = 81_058_141
MODEL_SHA256 = "8c328b45f59d8dd3dff219253ff6a8d6482be57d0133a29140e2febbf8eb8331"
YAMNET_MODEL_NAME = "yamnet-classification-tflite-1"
YAMNET_MODEL_URL = (
    "https://tfhub.dev/google/lite-model/yamnet/classification/tflite/1"
    "?lite-format=tflite"
)
YAMNET_MODEL_BYTES = 4_126_810
YAMNET_MODEL_SHA256 = "10c95ea3eb9a7bb4cb8bddf6feb023250381008177ac162ce169694d05c317de"
YAMNET_SAMPLE_RATE = 16_000
YAMNET_WINDOW_SAMPLES = 15_600
YAMNET_HOP_SAMPLES = 7_680
YAMNET_CLASS_COUNT = 521
YAMNET_CLASS_MAP_SHA256 = "cdf24d193e196d9e95912a2667051ae203e92a2ba09449218ccb40ef787c6df2"
YAMNET_VOCAL_CLASSES = {
    0: "Speech",
    1: "Child speech, kid speaking",
    2: "Conversation",
    3: "Narration, monologue",
    4: "Babbling",
    5: "Speech synthesizer",
    12: "Whispering",
    24: "Singing",
    25: "Choir",
    27: "Chant",
    28: "Mantra",
    29: "Child singing",
    30: "Synthetic singing",
    31: "Rapping",
    32: "Humming",
    63: "Chatter",
    65: "Hubbub, speech noise, speech babble",
    249: "Vocal music",
    250: "A capella",
    261: "Song",
}
MODEL_SAMPLE_RATE = 22_050
MODEL_FPS = 50
MODEL_N_FFT = 1_024
MODEL_HOP_SAMPLES = 441
MODEL_MEL_BINS = 128
MODEL_WINDOW_FRAMES = 1_500
MODEL_BORDER_FRAMES = 6
MODEL_STEP_FRAMES = MODEL_WINDOW_FRAMES - 2 * MODEL_BORDER_FRAMES
MAX_SOURCE_SECONDS = 30 * 60
JOB_DEADLINE_SECONDS = 15 * 60
DEFAULT_RSS_CAP_BYTES = 1 * 1024 * 1024 * 1024
MIN_RSS_CAP_BYTES = 512 * 1024 * 1024
MAX_RSS_CAP_BYTES = 4 * 1024 * 1024 * 1024


def configured_rss_cap_bytes(value=None):
    """Return the bounded worker RSS ceiling; invalid overrides fail to default."""
    raw = os.environ.get("LUMAE_DJ_RSS_CAP_BYTES", "") if value is None else value
    if raw in (None, ""):
        return DEFAULT_RSS_CAP_BYTES
    try:
        parsed = int(str(raw).strip())
    except (TypeError, ValueError):
        return DEFAULT_RSS_CAP_BYTES
    return max(MIN_RSS_CAP_BYTES, min(MAX_RSS_CAP_BYTES, parsed))


RSS_CAP_BYTES = configured_rss_cap_bytes()
MIN_REGION_BEATS = 32
MAX_INTERVAL_CV = 0.06
MAX_RAW_DOWNBEAT_ALIGNMENT_SECONDS = 0.05
# Every eligible eight-bar downbeat is a phrase-aligned candidate. Novelty ranks
# those boundaries and the app still requires >=1.25 for a drop-on-one; the
# other archetypes retain their beat, vocal, tempo, content, and rendered-PCM
# guards. Gating all cue families at 2.5 produced no cues on ordinary dance
# tracks because a 17-boundary local neighborhood rarely reaches that z-score.
NOVELTY_Z_THRESHOLD = 0.0
MAX_ENTRY_CANDIDATES = 8
MAX_EXIT_CANDIDATES = 8

# This is a complete runtime lock, not a set of permissive minimums. The large
# optional packages are deliberately excluded from the ordinary plugin install.
PINNED_PACKAGES = {
    "beat-this": "1.1.0",
    "torch": "2.6.0",
    "torchaudio": "2.6.0",
    "einops": "0.8.1",
    "rotary-embedding-torch": "0.8.6",
    "soxr": "0.5.0.post1",
    "av": "16.1.0",
    "numpy": "2.1.3",
    "psutil": "6.1.1",
    "ai-edge-litert": "2.2.0",
}
PINNED_PACKAGE_VARIANTS = {
    "torch": ("2.6.0", "2.6.0+cpu"),
    "torchaudio": ("2.6.0", "2.6.0+cpu"),
}


class DjAnalysisError(ValueError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


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


def _check_deadline(deadline, cancelled=None):
    if cancelled and cancelled():
        raise DjAnalysisError("cancelled")
    if deadline is not None and time.monotonic() >= deadline:
        raise DjAnalysisError("deadline_exceeded")


def _sha256_file(path, deadline=None, cancelled=None):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        while True:
            _check_deadline(deadline, cancelled)
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify_model_artifact(
    model_path,
    *,
    expected_bytes=MODEL_BYTES,
    expected_sha256=MODEL_SHA256,
    deadline=None,
):
    raw = str(model_path or "").strip()
    if not raw or raw.lower().startswith(("http://", "https://")):
        raise DjAnalysisError("model_not_local")
    path = Path(raw).expanduser()
    try:
        resolved = path.resolve(strict=True)
        info = resolved.stat()
    except (OSError, RuntimeError) as exc:
        raise DjAnalysisError("model_missing") from exc
    if not stat.S_ISREG(info.st_mode):
        raise DjAnalysisError("model_not_regular_file")
    if info.st_size != expected_bytes:
        raise DjAnalysisError("model_size_mismatch")
    if _sha256_file(resolved, deadline=deadline) != expected_sha256:
        raise DjAnalysisError("model_checksum_mismatch")
    return {
        "path": resolved,
        "device": info.st_dev,
        "inode": info.st_ino,
        "bytes": info.st_size,
        "sha256": expected_sha256,
        "mtime_ns": info.st_mtime_ns,
    }


def verify_yamnet_model_artifact(model_path, *, deadline=None):
    return verify_model_artifact(
        model_path,
        expected_bytes=YAMNET_MODEL_BYTES,
        expected_sha256=YAMNET_MODEL_SHA256,
        deadline=deadline,
    )


def runtime_status(
    model_path,
    yamnet_model_path=None,
    *,
    package_version=None,
    verify_model=True,
):
    package_version = package_version or importlib.metadata.version
    mismatches = []
    for name, expected in PINNED_PACKAGES.items():
        try:
            actual = package_version(name)
        except importlib.metadata.PackageNotFoundError:
            mismatches.append({"package": name, "reason": "missing"})
            continue
        accepted = PINNED_PACKAGE_VARIANTS.get(name, (expected,))
        if actual not in accepted:
            mismatches.append(
                {"package": name, "reason": "version", "expected": expected, "actual": actual}
            )
    model = None
    yamnet_model = None
    model_error = None
    if verify_model:
        try:
            model = verify_model_artifact(model_path)
        except DjAnalysisError as exc:
            model_error = exc.code
        if model_error is None:
            try:
                yamnet_model = verify_yamnet_model_artifact(yamnet_model_path)
            except DjAnalysisError as exc:
                model_error = f"yamnet_{exc.code}"
    available = not mismatches and model_error is None
    return {
        "schema_version": SCHEMA_VERSION,
        "method": METHOD,
        "available": available,
        "reason": None
        if available
        else model_error or "runtime_dependency_mismatch",
        "runtime_mismatches": mismatches,
        "models": {
            "beat_this": {
                "name": MODEL_NAME,
                "sha256": MODEL_SHA256,
                "bytes": MODEL_BYTES,
                "verified": model is not None,
            },
            "yamnet": {
                "name": YAMNET_MODEL_NAME,
                "sha256": YAMNET_MODEL_SHA256,
                "bytes": YAMNET_MODEL_BYTES,
                "verified": yamnet_model is not None,
                "io_type": "float32",
                "weight_quantization": "dynamic-range",
                "scores_calibrated": False,
            },
        },
        "supported_analysis_versions": [SCHEMA_VERSION],
        "supported_plan_versions": [2],
        "limits": {
            "max_concurrent_jobs": 1,
            "rss_bytes": RSS_CAP_BYTES,
            "deadline_seconds": JOB_DEADLINE_SECONDS,
            "max_source_seconds": MAX_SOURCE_SECONDS,
        },
        "reference_host_qualified": False,
    }


def _finite_vector(name, value, length=None):
    result = np.asarray(value, dtype=np.float64)
    if result.ndim != 1 or (length is not None and len(result) != length):
        raise DjAnalysisError("invalid_model_output", f"invalid {name} shape")
    if not np.all(np.isfinite(result)):
        raise DjAnalysisError("invalid_model_output", f"non-finite {name}")
    return result


def _peak_frames(logits, radius=3):
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


def _sigmoid(value):
    value = float(value)
    if value >= 0:
        factor = math.exp(-value)
        return 1 / (1 + factor)
    factor = math.exp(value)
    return factor / (1 + factor)


def _match_downbeats(beat_frames, raw_downbeat_frames, fps):
    matched = []
    if not beat_frames:
        return matched
    beats = np.asarray(beat_frames, dtype=np.int64)
    for raw in raw_downbeat_frames:
        position = int(np.searchsorted(beats, raw))
        choices = [index for index in (position - 1, position) if 0 <= index < len(beats)]
        beat_index = min(choices, key=lambda index: (abs(int(beats[index]) - raw), index))
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


def _tempo_ambiguity(beat_frames, beat_logits):
    if len(beat_frames) < MIN_REGION_BEATS:
        return []
    actual = np.asarray([_sigmoid(beat_logits[index]) for index in beat_frames])
    midpoint_frames = [
        int(round((left + right) / 2)) for left, right in zip(beat_frames, beat_frames[1:])
    ]
    midpoint = np.asarray(
        [_sigmoid(beat_logits[min(len(beat_logits) - 1, index)]) for index in midpoint_frames]
    )
    actual_mean = max(float(np.mean(actual)), 1e-9)
    reasons = []
    if len(midpoint) and float(np.mean(midpoint)) / actual_mean >= 0.80:
        reasons.append("double_tempo_ambiguity")
    even = float(np.mean(actual[::2]))
    odd = float(np.mean(actual[1::2])) if len(actual) > 1 else even
    if min(even, odd) / max(even, odd, 1e-9) <= 0.35:
        reasons.append("half_tempo_ambiguity")
    return reasons


def _regions(beat_frames, matched_downbeats, beat_logits, fps):
    meter_gaps = [
        right["beat_index"] - left["beat_index"]
        for left, right in zip(matched_downbeats, matched_downbeats[1:])
    ]
    meter_ambiguity = []
    if 8 in meter_gaps:
        meter_ambiguity.append("double_tempo_ambiguity")
    if 2 in meter_gaps:
        meter_ambiguity.append("half_tempo_ambiguity")
    runs = []
    current = []
    for item in matched_downbeats:
        if current and item["beat_index"] - current[-1]["beat_index"] != 4:
            if current:
                runs.append(current)
            current = []
        current.append(item)
    if current:
        runs.append(current)

    beat_times = np.asarray(beat_frames, dtype=np.float64) / fps
    regions = []
    for run in runs:
        start = run[0]["beat_index"]
        end = min(len(beat_frames) - 1, run[-1]["beat_index"] + 3)
        selected = beat_frames[start : end + 1]
        intervals = np.diff(beat_times[start : end + 1])
        interval_mean = float(np.mean(intervals)) if len(intervals) else 0.0
        interval_cv = (
            float(np.std(intervals) / interval_mean) if interval_mean > 0 else math.inf
        )
        alignment = max(item["alignment_seconds"] for item in run)
        ambiguity = _tempo_ambiguity(selected, beat_logits)
        tempo = 60 / float(np.median(intervals)) if len(intervals) else 0.0
        reasons = []
        if len(selected) < MIN_REGION_BEATS:
            reasons.append("too_few_beats")
        if interval_cv > MAX_INTERVAL_CV:
            reasons.append("unstable_tempo")
        if alignment > MAX_RAW_DOWNBEAT_ALIGNMENT_SECONDS:
            reasons.append("downbeat_alignment")
        if not 55 <= tempo <= 215:
            reasons.append("tempo_out_of_range")
        reasons.extend(meter_ambiguity)
        reasons.extend(ambiguity)
        strengths = [_sigmoid(beat_logits[index]) for index in selected]
        regions.append(
            {
                "start_beat_index": start,
                "end_beat_index": end,
                "start_ms": int(round(beat_times[start] * 1000)),
                "end_ms": int(round(beat_times[end] * 1000)),
                "beat_count": len(selected),
                "bar_count": len(run),
                "meter": "4/4",
                "tempo_bpm": round(tempo, 6),
                "interval_cv": round(interval_cv, 8)
                if math.isfinite(interval_cv)
                else None,
                "max_raw_downbeat_alignment_ms": int(round(alignment * 1000)),
                "beat_logit_mean": round(float(np.mean([beat_logits[i] for i in selected])), 8),
                "beat_peak_activation_mean": round(float(np.mean(strengths)), 8),
                "eligible": not reasons,
                "rejection_reasons": reasons,
            }
        )
    return regions


def _novelty_candidates(regions, matched_downbeats, energy, spectral_flux, fps):
    raw = []
    for region_index, region in enumerate(regions):
        if not region["eligible"]:
            continue
        downbeats = [
            item
            for item in matched_downbeats
            if region["start_beat_index"]
            <= item["beat_index"]
            <= region["end_beat_index"]
        ]
        for item in downbeats[8::8]:
            frame = item["snapped_frame"]
            radius = fps
            before = energy[max(0, frame - radius) : frame]
            after = energy[frame : min(len(energy), frame + radius)]
            if not len(before) or not len(after):
                continue
            energy_change = abs(float(np.mean(after)) - float(np.mean(before)))
            flux = float(
                np.max(spectral_flux[max(0, frame - fps // 2) : min(len(energy), frame + fps // 2 + 1)])
            )
            raw.append(
                {
                    "frame": frame,
                    "time_ms": int(round(frame * 1000 / fps)),
                    "region_index": region_index,
                    "raw_novelty": energy_change + flux,
                }
            )
    accepted = []
    for index, item in enumerate(raw):
        neighborhood = raw[max(0, index - 8) : min(len(raw), index + 9)]
        values = np.asarray([candidate["raw_novelty"] for candidate in neighborhood])
        deviation = float(np.std(values))
        z_score = (
            (item["raw_novelty"] - float(np.mean(values))) / deviation
            if deviation > 0
            else 0.0
        )
        if z_score >= NOVELTY_Z_THRESHOLD:
            accepted.append(
                {
                    "time_ms": item["time_ms"],
                    "region_index": item["region_index"],
                    "novelty_z": round(z_score, 8),
                    "boundary": "eight_bar_downbeat",
                }
            )
    ranked = sorted(accepted, key=lambda item: (-item["novelty_z"], item["time_ms"]))
    return {
        "entries": ranked[:MAX_ENTRY_CANDIDATES],
        "exits": ranked[:MAX_EXIT_CANDIDATES],
    }


def _key_evidence(value):
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
        "key": key if isinstance(key, int) and not isinstance(key, bool) and 0 <= key <= 11 else None,
        "scale": scale if scale in ("major", "minor") else None,
        "confidence": round(float(confidence), 8)
        if isinstance(confidence, (int, float))
        and not isinstance(confidence, bool)
        and math.isfinite(confidence)
        else None,
        "qualified": qualified,
        "pitch_shift_allowed": qualified,
    }


def _vocal_risk_timeline(value, duration_seconds, calibration_artifact=None):
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
    if indices != expected_indices or not isinstance(positions, list) or not isinstance(scores, list):
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
        if not np.all(np.isfinite(numeric)) or np.any(numeric < 0) or np.any(numeric > 1):
            raise DjAnalysisError("invalid_yamnet_output")
        raw_evidence = round(float(np.max(numeric)), 8)
        frames.append(
            {
                "position_ms": position,
                "raw_vocal_evidence": raw_evidence,
                "dominant_class_index": expected_indices[int(np.argmax(numeric))],
                "calibrated_risk": (
                    round(apply_vocal_calibration(raw_evidence, calibration_artifact), 8)
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


def _structural_timeline(
    regions,
    candidates,
    model_output,
    beat_frames,
    matched_downbeats,
    fps,
):
    band_vectors = {}
    for name in ("low_energy", "mid_energy", "high_energy"):
        raw = model_output.get(name)
        band_vectors[name] = (
            _finite_vector(name, raw, len(model_output["beat_logits"]))
            if raw is not None
            else None
        )

    band_windows = []
    if all(vector is not None for vector in band_vectors.values()):
        for region_index, region in enumerate(regions):
            downbeats = [
                item
                for item in matched_downbeats
                if region["start_beat_index"]
                <= item["beat_index"]
                <= region["end_beat_index"]
            ]
            for position, item in enumerate(downbeats):
                start = item["snapped_frame"]
                if position + 1 < len(downbeats):
                    end = downbeats[position + 1]["snapped_frame"]
                else:
                    end_beat = min(item["beat_index"] + 4, region["end_beat_index"])
                    end = beat_frames[end_beat]
                if end <= start:
                    continue
                band_windows.append(
                    {
                        "start_ms": int(round(start * 1000 / fps)),
                        "end_ms": int(round(end * 1000 / fps)),
                        "region_index": region_index,
                        "eligible": region["eligible"],
                        "low": round(float(np.mean(band_vectors["low_energy"][start:end])), 8),
                        "mid": round(float(np.mean(band_vectors["mid_energy"][start:end])), 8),
                        "high": round(float(np.mean(band_vectors["high_energy"][start:end])), 8),
                    }
                )

    phrases = []
    for kind in ("entries", "exits"):
        for candidate in candidates[kind]:
            region = regions[candidate["region_index"]]
            phrases.append(
                {
                    "position_ms": candidate["time_ms"],
                    "kind": "intro" if kind == "entries" else "outro",
                    "region_index": candidate["region_index"],
                    "downbeat_confidence": region["beat_peak_activation_mean"],
                    "structural_boundary": candidate["boundary"],
                    "eligible": bool(
                        region["eligible"]
                        and candidate["boundary"] == "eight_bar_downbeat"
                    ),
                }
            )
    phrases.sort(key=lambda item: (item["position_ms"], item["kind"]))
    return {
        "meter": {"beats_per_bar": 4, "confidence": "region-guarded"},
        "local_tempo": [
            {
                "start_ms": region["start_ms"],
                "end_ms": region["end_ms"],
                "bpm": region["tempo_bpm"],
                "interval_cv": region["interval_cv"],
                "eligible": region["eligible"],
            }
            for region in regions
        ],
        "phrase_candidates": phrases,
        "band_energy": {
            "unit": "mean-log-mel",
            "window": "bar",
            "windows": band_windows,
        },
        "chroma": {
            "available": False,
            "reason": "deterministic_chroma_calibration_pending",
            "frames": [],
        },
    }


def build_dj_analysis(
    model_output,
    *,
    catalog_instance_id,
    track_id,
    media_revision,
    representation_id,
    content_sha256,
    duration_seconds,
    source,
    key_evidence=None,
    vocal_output=None,
    vocal_calibration=None,
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

    beat_logits = _finite_vector("beat_logits", model_output.get("beat_logits"))
    frame_count = len(beat_logits)
    downbeat_logits = _finite_vector(
        "downbeat_logits", model_output.get("downbeat_logits"), frame_count
    )
    energy = _finite_vector("energy", model_output.get("energy"), frame_count)
    spectral_flux = _finite_vector(
        "spectral_flux", model_output.get("spectral_flux"), frame_count
    )
    if abs(frame_count / MODEL_FPS - duration_seconds) > 0.5:
        raise DjAnalysisError("model_timeline_mismatch")

    beat_frames = _peak_frames(beat_logits)
    raw_downbeat_frames = _peak_frames(downbeat_logits)
    matched = _match_downbeats(beat_frames, raw_downbeat_frames, MODEL_FPS)
    regions = _regions(beat_frames, matched, beat_logits, MODEL_FPS)
    candidates = _novelty_candidates(regions, matched, energy, spectral_flux, MODEL_FPS)
    vocal_risk = _vocal_risk_timeline(
        vocal_output,
        duration_seconds,
        calibration_artifact=vocal_calibration,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "method": METHOD,
        "catalog_instance_id": catalog_instance_id,
        "track_id": track_id,
        "media_revision": media_revision,
        "representation_id": representation_id,
        "content_sha256": content_sha256,
        "source": dict(source),
        "model": {
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
        },
        "beats": {
            "positions_ms": [int(round(frame * 1000 / MODEL_FPS)) for frame in beat_frames],
            "raw_downbeat_positions_ms": [
                int(round(frame * 1000 / MODEL_FPS)) for frame in raw_downbeat_frames
            ],
            "downbeat_positions_ms": [
                int(round(item["snapped_frame"] * 1000 / MODEL_FPS))
                for item in matched
                if item["alignment_seconds"] <= MAX_RAW_DOWNBEAT_ALIGNMENT_SECONDS
            ],
        },
        "regions": regions,
        "candidates": candidates,
        "structure": _structural_timeline(
            regions,
            candidates,
            model_output,
            beat_frames,
            matched,
            MODEL_FPS,
        ),
        "vocal_risk": vocal_risk,
        "natural_boundaries": {"entry_ms": 0, "exit_ms": int(round(duration_seconds * 1000))},
        "key_evidence": _key_evidence(key_evidence),
        "quality": {
            "eligible_region_count": sum(1 for region in regions if region["eligible"]),
            "region_count": len(regions),
            "beat_count": len(beat_frames),
            "raw_downbeat_count": len(raw_downbeat_frames),
            "model_outputs_are_probabilities": False,
            "vocal_calibration_ready":
                vocal_risk["calibration"]["status"] == "ready",
        },
    }
    payload["analysis_digest"] = analysis_digest(payload)
    return payload


def _rss_bytes():
    import psutil

    return int(psutil.Process().memory_info().rss)


def _beat_this_window_starts(frame_count):
    """Reproduce Beat This 1.1.0 split_piece(..., avoid_short_end=True)."""
    if isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count <= 0:
        raise DjAnalysisError("empty_audio")
    starts = list(
        range(
            -MODEL_BORDER_FRAMES,
            frame_count - MODEL_BORDER_FRAMES,
            MODEL_STEP_FRAMES,
        )
    )
    if frame_count > MODEL_STEP_FRAMES:
        starts[-1] = frame_count - (MODEL_WINDOW_FRAMES - MODEL_BORDER_FRAMES)
    return starts


def _reflect_sample_indices(start, end, sample_count):
    """Map centered-STFT padding exactly like torch's one-dimensional reflect pad."""
    if sample_count <= MODEL_N_FFT // 2:
        raise DjAnalysisError("source_too_short")
    positions = np.arange(int(start), int(end), dtype=np.int64)
    period = 2 * (sample_count - 1)
    folded = positions % period
    return np.where(folded < sample_count, folded, period - folded)


class BeatThisWindowAdapter:
    """Disk-spooled CPU adapter matching upstream's exact 1500/6 geometry."""

    def __init__(
        self,
        model_path,
        *,
        rss_reader=_rss_bytes,
        deadline=None,
        cancelled=None,
    ):
        self.artifact = verify_model_artifact(model_path)
        status = runtime_status(model_path, verify_model=False)
        if status["runtime_mismatches"]:
            raise DjAnalysisError("runtime_dependency_mismatch")
        import torch
        import torchaudio
        from beat_this.model.beat_tracker import BeatThis
        from beat_this.utils import replace_state_dict_key

        self.torch = torch
        self.rss_reader = rss_reader
        if getattr(torch.version, "cuda", None) or getattr(torch.version, "hip", None):
            raise DjAnalysisError("runtime_accelerator_build_unsupported")
        _check_deadline(deadline, cancelled)
        # Open the already-verified local artifact and load from that file
        # object. Unlike upstream load_model(), this code has no short-name or
        # URL fallback if the path is replaced between verification and use.
        with open(self.artifact["path"], "rb") as checkpoint_file:
            opened = os.fstat(checkpoint_file.fileno())
            if (
                opened.st_size != self.artifact["bytes"]
                or opened.st_mtime_ns != self.artifact["mtime_ns"]
            ):
                raise DjAnalysisError("model_changed")
            checkpoint_digest = hashlib.sha256()
            while True:
                _check_deadline(deadline, cancelled)
                chunk = checkpoint_file.read(1024 * 1024)
                if not chunk:
                    break
                checkpoint_digest.update(chunk)
            if checkpoint_digest.hexdigest() != MODEL_SHA256:
                raise DjAnalysisError("model_checksum_mismatch")
            checkpoint_file.seek(0)
            checkpoint = torch.load(
                checkpoint_file,
                map_location="cpu",
                weights_only=True,
            )
        _check_deadline(deadline, cancelled)
        hyperparameters = {
            key: value
            for key, value in checkpoint["hyper_parameters"].items()
            if key in set(inspect.signature(BeatThis).parameters)
        }
        self.model = BeatThis(**hyperparameters)
        state = replace_state_dict_key(checkpoint["state_dict"], "model.", "")
        self.model.load_state_dict(state)
        self.model = self.model.to("cpu").eval()
        self.mel = torchaudio.transforms.MelSpectrogram(
            sample_rate=MODEL_SAMPLE_RATE,
            n_fft=MODEL_N_FFT,
            hop_length=MODEL_HOP_SAMPLES,
            f_min=30,
            f_max=11_000,
            n_mels=MODEL_MEL_BINS,
            mel_scale="slaney",
            normalized="frame_length",
            power=1,
            center=False,
        ).to("cpu")
        if self.rss_reader() > RSS_CAP_BYTES:
            raise DjAnalysisError("rss_cap_exceeded")

    def _spect_frames(self, pcm, frame_start, frame_end):
        if frame_end <= frame_start:
            return np.empty((0, MODEL_MEL_BINS), dtype=np.float32)
        first_sample = frame_start * MODEL_HOP_SAMPLES - MODEL_N_FFT // 2
        last_sample = (frame_end - 1) * MODEL_HOP_SAMPLES + MODEL_N_FFT // 2
        indices = _reflect_sample_indices(first_sample, last_sample, len(pcm))
        samples = np.asarray(pcm[indices], dtype=np.float32)
        torch = self.torch
        with torch.inference_mode():
            waveform = torch.from_numpy(samples)
            spect = torch.log1p(1000 * self.mel(waveform).T)
        expected = frame_end - frame_start
        if len(spect) != expected:
            raise DjAnalysisError("model_timeline_mismatch")
        return spect.float().cpu().numpy()

    def _window(self, log_mel):
        torch = self.torch
        with torch.inference_mode():
            spect = torch.as_tensor(log_mel, dtype=torch.float32)
            prediction = self.model(spect.unsqueeze(0))
            beat = prediction["beat"][0].float().cpu().numpy()
            downbeat = prediction["downbeat"][0].float().cpu().numpy()
        if len(beat) != len(log_mel) or len(downbeat) != len(log_mel):
            raise DjAnalysisError("invalid_model_output")
        energy = np.mean(log_mel, axis=1, dtype=np.float64)
        spectral_flux = np.zeros(len(log_mel), dtype=np.float64)
        if len(log_mel) > 1:
            spectral_flux[1:] = np.mean(
                np.maximum(log_mel[1:] - log_mel[:-1], 0),
                axis=1,
                dtype=np.float64,
            )
        low_energy = np.mean(log_mel[:, :32], axis=1, dtype=np.float64)
        mid_energy = np.mean(log_mel[:, 32:80], axis=1, dtype=np.float64)
        high_energy = np.mean(log_mel[:, 80:], axis=1, dtype=np.float64)
        return (
            beat,
            downbeat,
            energy,
            spectral_flux,
            low_energy,
            mid_energy,
            high_energy,
        )

    def analyze(self, path, *, deadline, cancelled=None, progress=None):
        import av

        source_rate = None
        source_frames = 0
        resampled_frames = 0
        with tempfile.TemporaryFile() as pcm_file:
            with av.open(str(path)) as container:
                streams = [stream for stream in container.streams if stream.type == "audio"]
                if len(streams) != 1:
                    raise DjAnalysisError("unsupported_audio_streams")
                stream = streams[0]
                resampler = av.AudioResampler(
                    format="fltp", layout="mono", rate=MODEL_SAMPLE_RATE
                )
                for frame in container.decode(stream):
                    _check_deadline(deadline, cancelled)
                    rate = int(frame.sample_rate or stream.codec_context.sample_rate or 0)
                    if rate <= 0 or (source_rate is not None and source_rate != rate):
                        raise DjAnalysisError("source_timeline_changed")
                    source_rate = rate
                    source_frames += int(frame.samples)
                    for converted in resampler.resample(frame):
                        block = (
                            converted.to_ndarray()
                            .astype(np.float32, copy=False)
                            .reshape(-1)
                        )
                        if not np.all(np.isfinite(block)):
                            raise DjAnalysisError("non_finite_audio")
                        pcm_file.write(block.astype("<f4", copy=False).tobytes())
                        resampled_frames += len(block)
                        if resampled_frames > MAX_SOURCE_SECONDS * MODEL_SAMPLE_RATE:
                            raise DjAnalysisError("source_duration_unsupported")
                        if self.rss_reader() > RSS_CAP_BYTES:
                            raise DjAnalysisError("rss_cap_exceeded")
                for converted in resampler.resample(None):
                    block = (
                        converted.to_ndarray()
                        .astype(np.float32, copy=False)
                        .reshape(-1)
                    )
                    if not np.all(np.isfinite(block)):
                        raise DjAnalysisError("non_finite_audio")
                    pcm_file.write(block.astype("<f4", copy=False).tobytes())
                    resampled_frames += len(block)
            if source_rate is None or resampled_frames == 0:
                raise DjAnalysisError("empty_audio")
            if resampled_frames > MAX_SOURCE_SECONDS * MODEL_SAMPLE_RATE:
                raise DjAnalysisError("source_duration_unsupported")
            if resampled_frames <= MODEL_N_FFT // 2:
                raise DjAnalysisError("source_too_short")
            pcm_file.flush()
            frame_count = resampled_frames // MODEL_HOP_SAMPLES + 1
            outputs = {
                name: np.full(frame_count, -1000.0, dtype=np.float64)
                for name in ("beat_logits", "downbeat_logits")
            }
            for name in (
                "energy",
                "spectral_flux",
                "low_energy",
                "mid_energy",
                "high_energy",
            ):
                outputs[name] = np.zeros(frame_count, dtype=np.float64)
            owned = np.zeros(frame_count, dtype=np.bool_)
            pcm = np.memmap(
                pcm_file,
                dtype="<f4",
                mode="r",
                shape=(resampled_frames,),
            )
            try:
                for start in _beat_this_window_starts(frame_count):
                    _check_deadline(deadline, cancelled)
                    actual_start = max(0, start)
                    actual_end = min(frame_count, start + MODEL_WINDOW_FRAMES)
                    log_mel = self._spect_frames(pcm, actual_start, actual_end)
                    left = max(0, -start)
                    right = max(
                        0,
                        min(
                            MODEL_BORDER_FRAMES,
                            start + MODEL_WINDOW_FRAMES - frame_count,
                        ),
                    )
                    if left or right:
                        log_mel = np.pad(log_mel, ((left, right), (0, 0)))
                    values = self._window(log_mel)
                    target_start = max(0, start + MODEL_BORDER_FRAMES)
                    target_end = min(
                        frame_count,
                        start + MODEL_WINDOW_FRAMES - MODEL_BORDER_FRAMES,
                    )
                    source_start = target_start - start
                    source_end = target_end - start
                    available = ~owned[target_start:target_end]
                    for name, value in zip(outputs, values):
                        segment = np.asarray(
                            value[source_start:source_end], dtype=np.float64
                        )
                        outputs[name][target_start:target_end][available] = segment[
                            available
                        ]
                    owned[target_start:target_end] |= available
                    if progress:
                        progress(int(np.count_nonzero(owned)))
                    if self.rss_reader() > RSS_CAP_BYTES:
                        raise DjAnalysisError("rss_cap_exceeded")
            finally:
                del pcm
        if not np.all(owned):
            raise DjAnalysisError("model_timeline_mismatch")
        outputs["spectral_flux"][0] = 0.0
        result = outputs
        result["source_sample_rate"] = source_rate
        result["source_decoded_frames"] = source_frames
        result["source_resampled_frames"] = resampled_frames
        return result


class YamnetLiteAdapter:
    """Pinned CPU LiteRT adapter returning time-resolved uncalibrated evidence."""

    def __init__(self, model_path, *, rss_reader=_rss_bytes, deadline=None, cancelled=None):
        self.artifact = verify_yamnet_model_artifact(model_path, deadline=deadline)
        self.rss_reader = rss_reader
        _check_deadline(deadline, cancelled)
        from ai_edge_litert.interpreter import Interpreter

        self.interpreter = Interpreter(model_path=str(self.artifact["path"]), num_threads=1)
        self.interpreter.allocate_tensors()
        opened = self.artifact["path"].stat()
        if (
            opened.st_dev != self.artifact["device"]
            or opened.st_ino != self.artifact["inode"]
            or opened.st_size != self.artifact["bytes"]
            or opened.st_mtime_ns != self.artifact["mtime_ns"]
            or _sha256_file(
                self.artifact["path"], deadline=deadline, cancelled=cancelled
            )
            != YAMNET_MODEL_SHA256
        ):
            raise DjAnalysisError("yamnet_model_changed")
        inputs = self.interpreter.get_input_details()
        outputs = self.interpreter.get_output_details()
        if len(inputs) != 1 or len(outputs) != 1:
            raise DjAnalysisError("yamnet_contract_mismatch")
        input_shape = tuple(int(value) for value in inputs[0]["shape"])
        output_shape = tuple(int(value) for value in outputs[0]["shape"])
        if input_shape not in ((YAMNET_WINDOW_SAMPLES,), (1, YAMNET_WINDOW_SAMPLES)):
            raise DjAnalysisError("yamnet_contract_mismatch")
        if output_shape not in ((YAMNET_CLASS_COUNT,), (1, YAMNET_CLASS_COUNT)):
            raise DjAnalysisError("yamnet_contract_mismatch")
        if np.dtype(inputs[0]["dtype"]) != np.dtype(np.float32) or np.dtype(
            outputs[0]["dtype"]
        ) != np.dtype(np.float32):
            raise DjAnalysisError("yamnet_contract_mismatch")
        self.input = inputs[0]
        self.output = outputs[0]
        if self.rss_reader() > RSS_CAP_BYTES:
            raise DjAnalysisError("rss_cap_exceeded")

    def analyze(self, path, *, deadline, cancelled=None):
        import av

        source_rate = None
        resampled_frames = 0
        with tempfile.TemporaryFile() as pcm_file:
            with av.open(str(path)) as container:
                streams = [stream for stream in container.streams if stream.type == "audio"]
                if len(streams) != 1:
                    raise DjAnalysisError("unsupported_audio_streams")
                resampler = av.AudioResampler(
                    format="fltp", layout="mono", rate=YAMNET_SAMPLE_RATE
                )
                for frame in container.decode(streams[0]):
                    _check_deadline(deadline, cancelled)
                    rate = int(
                        frame.sample_rate
                        or streams[0].codec_context.sample_rate
                        or 0
                    )
                    if rate <= 0 or (source_rate is not None and source_rate != rate):
                        raise DjAnalysisError("source_timeline_changed")
                    source_rate = rate
                    for converted in resampler.resample(frame):
                        block = converted.to_ndarray().astype(np.float32, copy=False).reshape(-1)
                        if not np.all(np.isfinite(block)):
                            raise DjAnalysisError("non_finite_audio")
                        pcm_file.write(block.astype("<f4", copy=False).tobytes())
                        resampled_frames += len(block)
                        if resampled_frames > MAX_SOURCE_SECONDS * YAMNET_SAMPLE_RATE:
                            raise DjAnalysisError("source_duration_unsupported")
                        if self.rss_reader() > RSS_CAP_BYTES:
                            raise DjAnalysisError("rss_cap_exceeded")
                for converted in resampler.resample(None):
                    block = converted.to_ndarray().astype(np.float32, copy=False).reshape(-1)
                    if not np.all(np.isfinite(block)):
                        raise DjAnalysisError("non_finite_audio")
                    pcm_file.write(block.astype("<f4", copy=False).tobytes())
                    resampled_frames += len(block)
            if not resampled_frames:
                raise DjAnalysisError("empty_audio")
            if resampled_frames > MAX_SOURCE_SECONDS * YAMNET_SAMPLE_RATE:
                raise DjAnalysisError("source_duration_unsupported")
            pcm_file.flush()
            pcm = np.memmap(pcm_file, dtype="<f4", mode="r", shape=(resampled_frames,))
            positions = []
            rows = []
            try:
                last_start = max(0, resampled_frames - YAMNET_WINDOW_SAMPLES)
                starts = list(range(0, last_start + 1, YAMNET_HOP_SAMPLES))
                if not starts or starts[-1] != last_start:
                    starts.append(last_start)
                for start in starts:
                    _check_deadline(deadline, cancelled)
                    window = np.zeros(YAMNET_WINDOW_SAMPLES, dtype=np.float32)
                    available = min(YAMNET_WINDOW_SAMPLES, resampled_frames - start)
                    window[:available] = pcm[start : start + available]
                    tensor = window if tuple(self.input["shape"]) == (YAMNET_WINDOW_SAMPLES,) else window[None, :]
                    self.interpreter.set_tensor(self.input["index"], tensor)
                    self.interpreter.invoke()
                    scores = np.asarray(
                        self.interpreter.get_tensor(self.output["index"]), dtype=np.float32
                    ).reshape(-1)
                    if len(scores) != YAMNET_CLASS_COUNT or not np.all(np.isfinite(scores)):
                        raise DjAnalysisError("invalid_yamnet_output")
                    positions.append(
                        int(round((start + min(available, YAMNET_WINDOW_SAMPLES) / 2) * 1000 / YAMNET_SAMPLE_RATE))
                    )
                    rows.append(
                        [round(float(scores[index]), 8) for index in YAMNET_VOCAL_CLASSES]
                    )
                    if self.rss_reader() > RSS_CAP_BYTES:
                        raise DjAnalysisError("rss_cap_exceeded")
            finally:
                del pcm
        return {
            "positions_ms": positions,
            "class_indices": list(YAMNET_VOCAL_CLASSES),
            "scores": rows,
        }


def analyze_dj_file(
    path,
    *,
    catalog_instance_id,
    track_id,
    media_revision,
    model_path,
    yamnet_model_path=None,
    adapter=None,
    yamnet_adapter=None,
    deadline_seconds=JOB_DEADLINE_SECONDS,
    cancelled=None,
    progress=None,
    key_evidence=None,
    vocal_calibration=None,
):
    deadline = time.monotonic() + max(1, min(JOB_DEADLINE_SECONDS, deadline_seconds))
    source_path = Path(path).resolve(strict=True)
    before = source_path.stat()
    content_sha256 = _sha256_file(source_path, deadline=deadline, cancelled=cancelled)
    worker = adapter or BeatThisWindowAdapter(
        model_path,
        deadline=deadline,
        cancelled=cancelled,
    )
    model_output = worker.analyze(
        source_path,
        deadline=deadline,
        cancelled=cancelled,
        progress=progress,
    )
    vocal_worker = yamnet_adapter
    if vocal_worker is None and yamnet_model_path:
        vocal_worker = YamnetLiteAdapter(
            yamnet_model_path,
            deadline=deadline,
            cancelled=cancelled,
        )
    vocal_output = (
        vocal_worker.analyze(
            source_path,
            deadline=deadline,
            cancelled=cancelled,
        )
        if vocal_worker is not None
        else None
    )
    after = source_path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise DjAnalysisError("source_changed")
    frame_count = len(model_output["beat_logits"])
    resampled_frames = int(
        model_output.get(
            "source_resampled_frames",
            max(1, frame_count - 1) * MODEL_HOP_SAMPLES,
        )
    )
    duration_seconds = resampled_frames / MODEL_SAMPLE_RATE
    source = {
        "sample_rate": int(model_output["source_sample_rate"]),
        "decoded_frames": int(model_output["source_decoded_frames"]),
        "analysis_sample_rate": MODEL_SAMPLE_RATE,
        "analysis_resampled_frames": resampled_frames,
        "analysis_frames": frame_count,
        "decoder": "pyav-16.1.0-streaming",
        "timeline_verified": True,
    }
    return build_dj_analysis(
        model_output,
        catalog_instance_id=catalog_instance_id,
        track_id=track_id,
        media_revision=media_revision,
        representation_id=f"sha256:{content_sha256}",
        content_sha256=content_sha256,
        duration_seconds=duration_seconds,
        source=source,
        key_evidence=key_evidence,
        vocal_output=vocal_output,
        vocal_calibration=vocal_calibration,
    )
