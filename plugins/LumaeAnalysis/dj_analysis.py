"""DJ Analysis V2 projection over shared source-bound evidence."""

import math
import numpy as np
from .dj_contract import (
    SCHEMA_VERSION,
    METHOD,
    BEAT_THIS_VERSION,
    MODEL_NAME,
    MODEL_URL,
    MODEL_BYTES,
    MODEL_SHA256,
    YAMNET_MODEL_NAME,
    YAMNET_MODEL_URL,
    YAMNET_MODEL_BYTES,
    YAMNET_MODEL_SHA256,
    YAMNET_SAMPLE_RATE,
    YAMNET_WINDOW_SAMPLES,
    YAMNET_HOP_SAMPLES,
    YAMNET_CLASS_COUNT,
    YAMNET_CLASS_MAP_SHA256,
    YAMNET_VOCAL_CLASSES,
    MODEL_SAMPLE_RATE,
    MODEL_FPS,
    MODEL_N_FFT,
    MODEL_HOP_SAMPLES,
    MODEL_MEL_BINS,
    MODEL_WINDOW_FRAMES,
    MODEL_BORDER_FRAMES,
    MODEL_STEP_FRAMES,
    MAX_SOURCE_SECONDS,
    JOB_DEADLINE_SECONDS,
    DEFAULT_RSS_CAP_BYTES,
    MIN_RSS_CAP_BYTES,
    MAX_RSS_CAP_BYTES,
    RSS_CAP_BYTES,
    MIN_REGION_BEATS,
    MAX_INTERVAL_CV,
    MAX_RAW_DOWNBEAT_ALIGNMENT_SECONDS,
    NOVELTY_Z_THRESHOLD,
    MAX_ENTRY_CANDIDATES,
    MAX_EXIT_CANDIDATES,
    PINNED_PACKAGES,
    PINNED_PACKAGE_VARIANTS,
    configured_rss_cap_bytes,
    DjAnalysisError,
)

from .dj_evidence import (
    canonical_json,
    analysis_digest,
    finite_vector as _finite_vector,
    peak_frames as _peak_frames,
    sigmoid as _sigmoid,
    match_downbeats as _match_downbeats,
    key_evidence as _key_evidence,
    vocal_risk_timeline as _vocal_risk_timeline,
    validate_annotation,
    model_identity,
)
from .dj_runtime import (
    _check_deadline,
    _sha256_file,
    verify_model_artifact,
    verify_yamnet_model_artifact,
    runtime_status,
    _rss_bytes,
    _beat_this_window_starts,
    _reflect_sample_indices,
    BeatThisWindowAdapter,
    YamnetLiteAdapter,
)
from . import dj_runtime


def _tempo_ambiguity(beat_frames, beat_logits):
    if len(beat_frames) < MIN_REGION_BEATS:
        return []
    actual = np.asarray([_sigmoid(beat_logits[index]) for index in beat_frames])
    midpoint_frames = [
        int(round((left + right) / 2))
        for left, right in zip(beat_frames, beat_frames[1:])
    ]
    midpoint = np.asarray(
        [
            _sigmoid(beat_logits[min(len(beat_logits) - 1, index)])
            for index in midpoint_frames
        ]
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
                "interval_cv": (
                    round(interval_cv, 8) if math.isfinite(interval_cv) else None
                ),
                "max_raw_downbeat_alignment_ms": int(round(alignment * 1000)),
                "beat_logit_mean": round(
                    float(np.mean([beat_logits[i] for i in selected])), 8
                ),
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
                np.max(
                    spectral_flux[
                        max(0, frame - fps // 2) : min(
                            len(energy), frame + fps // 2 + 1
                        )
                    ]
                )
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
                        "low": round(
                            float(np.mean(band_vectors["low_energy"][start:end])), 8
                        ),
                        "mid": round(
                            float(np.mean(band_vectors["mid_energy"][start:end])), 8
                        ),
                        "high": round(
                            float(np.mean(band_vectors["high_energy"][start:end])), 8
                        ),
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
    beat_logits, downbeat_logits, energy, spectral_flux = validate_annotation(
        model_output,
        catalog_instance_id=catalog_instance_id,
        track_id=track_id,
        media_revision=media_revision,
        representation_id=representation_id,
        content_sha256=content_sha256,
        duration_seconds=duration_seconds,
    )
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
        "model": model_identity(),
        "beats": {
            "positions_ms": [
                int(round(frame * 1000 / MODEL_FPS)) for frame in beat_frames
            ],
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
        "natural_boundaries": {
            "entry_ms": 0,
            "exit_ms": int(round(duration_seconds * 1000)),
        },
        "key_evidence": _key_evidence(key_evidence),
        "quality": {
            "eligible_region_count": sum(1 for region in regions if region["eligible"]),
            "region_count": len(regions),
            "beat_count": len(beat_frames),
            "raw_downbeat_count": len(raw_downbeat_frames),
            "model_outputs_are_probabilities": False,
            "vocal_calibration_ready": vocal_risk["calibration"]["status"] == "ready",
        },
    }
    payload["analysis_digest"] = analysis_digest(payload)
    return payload


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
    evidence = dj_runtime.analyze_evidence(
        path,
        model_path=model_path,
        yamnet_model_path=yamnet_model_path,
        adapter=adapter,
        yamnet_adapter=yamnet_adapter,
        deadline_seconds=deadline_seconds,
        cancelled=cancelled,
        progress=progress,
    )
    return build_dj_analysis(
        **evidence.annotation_arguments(),
        catalog_instance_id=catalog_instance_id,
        track_id=track_id,
        media_revision=media_revision,
        key_evidence=key_evidence,
        vocal_calibration=vocal_calibration,
    )
