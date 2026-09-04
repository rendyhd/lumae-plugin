"""Source-bound DJ Analysis V3 with localized rhythm and role-specific cues."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path

import numpy as np

from . import dj_analysis as v2
from .vocal_calibration import CUT_RISK_THRESHOLD


SCHEMA_VERSION = 3
METHOD = "beat-this-1.1.0-yamnet-lite-1-lumae-dj-v3.0"
ANALYZER_VERSION = 1
CONFIDENCE_TIERS = ("high", "medium", "unavailable")
CUE_ROLES = ("entry", "exit", "drop", "breakdown", "cut", "loop")
MAX_REGIONS = 64
MAX_CUES_PER_ROLE = 8
SPEECH_CLASS_INDICES = frozenset((0, 1, 2, 3, 4, 5, 12, 63, 65))


def runtime_status(model_path, yamnet_model_path=None, **kwargs):
    """Reuse the pinned V2 runtime while advertising both additive contracts."""
    status = json.loads(
        json.dumps(v2.runtime_status(model_path, yamnet_model_path, **kwargs))
    )
    status.update(
        {
            "schema_version": SCHEMA_VERSION,
            "method": METHOD,
            "supported_analysis_versions": [2, 3],
            "supported_plan_versions": [2, 3],
        }
    )
    return status


def _normalized(values):
    values = np.asarray(values, dtype=np.float64)
    if not len(values):
        return values
    low, high = np.percentile(values, [5, 95])
    if not math.isfinite(low) or not math.isfinite(high) or high <= low:
        return np.zeros(len(values), dtype=np.float64)
    return np.clip((values - low) / (high - low), 0.0, 1.0)


def _rounded(value, digits=8):
    return round(float(value), digits) if math.isfinite(float(value)) else None


def _tempo_interpretation(raw_tempo, phase_gaps):
    factor = 1.0
    median_gap = float(np.median(phase_gaps)) if phase_gaps else 4.0
    if median_gap >= 6.0:
        factor = 0.5
    elif median_gap <= 2.5:
        factor = 2.0
    alternatives = []
    for candidate_factor in (0.5, 1.0, 2.0):
        bpm = raw_tempo * candidate_factor
        if 55.0 <= bpm <= 215.0:
            alternatives.append(
                {
                    "factor": candidate_factor,
                    "tempo_bpm": round(bpm, 6),
                    "selected": candidate_factor == factor,
                }
            )
    normalized = raw_tempo * factor
    if not 55.0 <= normalized <= 215.0:
        valid = [item for item in alternatives if item["selected"] is False]
        chosen = min(valid, key=lambda item: abs(item["tempo_bpm"] - 120.0), default=None)
        if chosen:
            for item in alternatives:
                item["selected"] = item is chosen
            normalized = chosen["tempo_bpm"]
            factor = chosen["factor"]
    return round(normalized, 6), factor, alternatives


def _localized_regions(beat_frames, matched_downbeats, beat_logits, downbeat_logits, fps):
    if len(beat_frames) < 16:
        return []
    beat_times = np.asarray(beat_frames, dtype=np.float64) / fps
    intervals = np.diff(beat_times)
    split_at = [0]
    for index, interval in enumerate(intervals):
        if interval < 0.24 or interval > 1.5:
            split_at.append(index + 1)
    split_at.append(len(beat_frames))
    regions = []
    for segment_start, segment_end in zip(split_at, split_at[1:]):
        length = segment_end - segment_start
        if length < 16:
            continue
        starts = list(range(segment_start, max(segment_start + 1, segment_end - 15), 32))
        tail_start = max(segment_start, segment_end - 65)
        if tail_start not in starts:
            starts.append(tail_start)
        for start in sorted(set(starts)):
            end = min(segment_end, start + 65)
            if end - start < 16:
                continue
            local_intervals = np.diff(beat_times[start:end])
            mean_interval = float(np.mean(local_intervals)) if len(local_intervals) else 0.0
            cv = float(np.std(local_intervals) / mean_interval) if mean_interval > 0 else math.inf
            local_downbeats = [
                item
                for item in matched_downbeats
                if start <= item["beat_index"] < end
                and item["alignment_seconds"] <= 0.1
            ]
            phase_scores = {}
            for item in local_downbeats:
                phase = item["beat_index"] % 4
                confidence = v2._sigmoid(downbeat_logits[item["raw_frame"]])
                phase_scores[phase] = phase_scores.get(phase, 0.0) + confidence
            meter_phase = (
                min(phase_scores, key=lambda phase: (-phase_scores[phase], phase))
                if phase_scores
                else None
            )
            aligned = [
                item for item in local_downbeats if item["beat_index"] % 4 == meter_phase
            ] if meter_phase is not None else []
            expected = sum(index % 4 == meter_phase for index in range(start, end)) if meter_phase is not None else 0
            missing = max(0, expected - len(aligned))
            alignment = max(
                (item["alignment_seconds"] for item in aligned), default=math.inf
            )
            raw_tempo = 60.0 / float(np.median(local_intervals)) if len(local_intervals) else 0.0
            phase_gaps = [
                right["beat_index"] - left["beat_index"]
                for left, right in zip(aligned, aligned[1:])
            ]
            normalized_tempo, selected_factor, alternatives = _tempo_interpretation(
                raw_tempo, phase_gaps
            )
            beat_count = end - start
            high = (
                beat_count >= 32
                and cv <= 0.06
                and alignment <= 0.05
                and expected >= 7
                and missing <= 1
            )
            medium = (
                beat_count >= 16
                and cv <= 0.10
                and alignment <= 0.10
                and expected >= 3
                and missing <= 1
            )
            confidence_tier = "high" if high else "medium" if medium else "unavailable"
            reasons = []
            if beat_count < 16:
                reasons.append("too_few_beats")
            if cv > 0.10:
                reasons.append("unstable_tempo")
            if alignment > 0.10:
                reasons.append("downbeat_alignment")
            if meter_phase is None or expected < 3 or missing > 1:
                reasons.append("localized_meter_unavailable")
            if not 55.0 <= normalized_tempo <= 215.0:
                reasons.append("tempo_out_of_range")
            regions.append(
                {
                    "start_beat_index": start,
                    "end_beat_index": end - 1,
                    "start_ms": int(round(beat_times[start] * 1000)),
                    "end_ms": int(round(beat_times[end - 1] * 1000)),
                    "beat_count": beat_count,
                    "bar_count": beat_count // 4,
                    "raw_tempo_bpm": round(raw_tempo, 6),
                    "normalized_tempo_bpm": normalized_tempo,
                    "selected_metrical_factor": selected_factor,
                    "tempo_alternatives": alternatives,
                    "meter_phase": meter_phase,
                    "interval_cv": _rounded(cv),
                    "max_raw_downbeat_alignment_ms": (
                        int(round(alignment * 1000)) if math.isfinite(alignment) else None
                    ),
                    "missing_downbeats": missing,
                    "confidence_tier": confidence_tier,
                    "rejection_reasons": reasons,
                }
            )
    regions.sort(
        key=lambda item: (
            item["start_ms"],
            {"high": 0, "medium": 1, "unavailable": 2}[item["confidence_tier"]],
            item["end_ms"],
        )
    )
    return regions[:MAX_REGIONS]


def _vocal_risk_at(vocal_risk, position_ms):
    frames = vocal_risk.get("frames", [])
    if not frames:
        return None
    nearest = min(frames, key=lambda item: abs(item["position_ms"] - position_ms))
    return nearest.get("calibrated_risk")


def _speech_safe_intervals(model_output, vocal_risk, duration_ms):
    if vocal_risk.get("calibration", {}).get("cuts_authorized") is not True:
        return []
    frames = vocal_risk.get("frames", [])
    risky = [
        item["position_ms"]
        for item in frames
        if item.get("dominant_class_index") in SPEECH_CLASS_INDICES
        and isinstance(item.get("calibrated_risk"), (int, float))
        and item["calibrated_risk"] >= CUT_RISK_THRESHOLD
    ]
    if not risky:
        return []
    low = v2._finite_vector("low_energy", model_output.get("low_energy"))
    mid = v2._finite_vector("mid_energy", model_output.get("mid_energy"), len(low))
    high = v2._finite_vector("high_energy", model_output.get("high_energy"), len(low))
    activity = _normalized(mid + 0.35 * high - 0.25 * low)
    if len(activity) >= 3:
        activity = np.convolve(activity, np.ones(3) / 3.0, mode="same")
    fps = v2.MODEL_FPS
    candidates = []
    for center_ms in risky:
        center = int(round(center_ms * fps / 1000))
        begin = max(4, center - int(0.75 * fps))
        end = min(len(activity) - 4, center + int(0.75 * fps) + 1)
        if end - begin < 16:
            continue
        local = activity[begin:end]
        quiet_limit = min(0.28, float(np.quantile(local, 0.20)))
        guard_limit = min(0.40, float(np.quantile(local, 0.40)))
        mask = local <= quiet_limit
        run_start = None
        for offset, is_quiet in enumerate(np.append(mask, False)):
            if is_quiet and run_start is None:
                run_start = offset
            elif not is_quiet and run_start is not None:
                run_end = offset
                if run_end - run_start >= 8:
                    absolute_start = begin + run_start
                    absolute_end = begin + run_end
                    guarded_start = absolute_start - 4
                    guarded_end = absolute_end + 4
                    if np.all(activity[guarded_start:absolute_start] <= guard_limit) and np.all(
                        activity[absolute_end:guarded_end] <= guard_limit
                    ):
                        candidates.append(
                            {
                                "start_ms": int(round(absolute_start * 1000 / fps)),
                                "end_ms": int(round(absolute_end * 1000 / fps)),
                                "cut_ms": int(round((absolute_start + absolute_end) * 500 / fps)),
                                "stable_minimum_ms": int(round((absolute_end - absolute_start) * 1000 / fps)),
                                "guard_ms": 80,
                                "method": "yamnet-guided-voice-band-minimum-v1",
                            }
                        )
                run_start = None
    deduped = {}
    for item in candidates:
        bucket = item["cut_ms"] // 160
        current = deduped.get(bucket)
        if current is None or item["stable_minimum_ms"] > current["stable_minimum_ms"]:
            deduped[bucket] = item
    return [
        item
        for item in sorted(deduped.values(), key=lambda value: value["cut_ms"])
        if 0 <= item["start_ms"] < item["end_ms"] <= duration_ms
    ][:32]


def _safe_at(position_ms, vocal_risk, safe_intervals):
    risk = _vocal_risk_at(vocal_risk, position_ms)
    if isinstance(risk, (int, float)) and risk < CUT_RISK_THRESHOLD:
        return True, round(float(risk), 8), "calibrated-low-risk"
    for interval in safe_intervals:
        if interval["start_ms"] <= position_ms <= interval["end_ms"]:
            return True, risk, "speech-safe-minimum"
    return False, risk, "vocal-risk-uncertain" if risk is None else "vocal-conflict"


def _band_windows(model_output, regions):
    vectors = {
        name: v2._finite_vector(name, model_output.get(name))
        for name in ("low_energy", "mid_energy", "high_energy")
    }
    output = []
    for region_index, region in enumerate(regions):
        start = max(0, int(round(region["start_ms"] * v2.MODEL_FPS / 1000)))
        end = min(len(vectors["low_energy"]), int(round(region["end_ms"] * v2.MODEL_FPS / 1000)) + 1)
        if end <= start:
            continue
        for frame in range(start, end, 4 * max(1, int(round(v2.MODEL_FPS * 60 / max(region["normalized_tempo_bpm"], 1))))):
            frame_end = min(end, frame + max(1, int(round(4 * v2.MODEL_FPS * 60 / max(region["normalized_tempo_bpm"], 1)))))
            output.append(
                {
                    "start_ms": int(round(frame * 1000 / v2.MODEL_FPS)),
                    "end_ms": int(round(frame_end * 1000 / v2.MODEL_FPS)),
                    "region_index": region_index,
                    "low": _rounded(np.mean(vectors["low_energy"][frame:frame_end])),
                    "mid": _rounded(np.mean(vectors["mid_energy"][frame:frame_end])),
                    "high": _rounded(np.mean(vectors["high_energy"][frame:frame_end])),
                }
            )
    return output[:256]


def _structural_and_cues(model_output, beat_frames, regions, vocal_risk, safe_intervals, duration_ms):
    energy = _normalized(v2._finite_vector("energy", model_output.get("energy")))
    flux = _normalized(v2._finite_vector("spectral_flux", model_output.get("spectral_flux"), len(energy)))
    low = _normalized(v2._finite_vector("low_energy", model_output.get("low_energy"), len(energy)))
    mid = _normalized(v2._finite_vector("mid_energy", model_output.get("mid_energy"), len(energy)))
    high = _normalized(v2._finite_vector("high_energy", model_output.get("high_energy"), len(energy)))
    boundaries = []
    cues_by_role = {role: [] for role in CUE_ROLES}
    seen = set()
    for region_index, region in enumerate(regions):
        if region["confidence_tier"] == "unavailable" or region["meter_phase"] is None:
            continue
        start = region["start_beat_index"]
        end = region["end_beat_index"]
        phase = region["meter_phase"]
        downbeat_indices = [index for index in range(start, end + 1) if index % 4 == phase]
        if not downbeat_indices:
            continue
        origin = downbeat_indices[0]
        for beat_index in downbeat_indices:
            bars_from_origin = (beat_index - origin) // 4
            supported = [bars for bars in (4, 8, 16) if bars_from_origin > 0 and bars_from_origin % bars == 0]
            if not supported:
                continue
            frame = beat_frames[beat_index]
            position_ms = int(round(frame * 1000 / v2.MODEL_FPS))
            key = (position_ms, region_index)
            if key in seen:
                continue
            seen.add(key)
            radius = max(4, v2.MODEL_FPS)
            before = energy[max(0, frame - radius):frame]
            after = energy[frame:min(len(energy), frame + radius)]
            if not len(before) or not len(after):
                continue
            slope = float(np.mean(after) - np.mean(before))
            novelty = float(np.max(flux[max(0, frame - radius // 2):min(len(flux), frame + radius // 2 + 1)]))
            spectral_change = float(
                np.mean(
                    [
                        abs(np.mean(vector[frame:min(len(vector), frame + radius)]) - np.mean(vector[max(0, frame - radius):frame]))
                        for vector in (low, mid, high)
                    ]
                )
            )
            recurrence = max(0.0, 1.0 - abs(float(energy[frame]) - float(energy[max(0, frame - 1)])))
            speech_safe, vocal_risk_value, vocal_reason = _safe_at(
                position_ms, vocal_risk, safe_intervals
            )
            elapsed_ms = position_ms
            remaining_ms = duration_ms - position_ms
            section_ms = int(round(16 * 4 * 60_000 / max(region["normalized_tempo_bpm"], 1)))
            evidence_score = min(
                1.0,
                (0.62 if region["confidence_tier"] == "high" else 0.45)
                + 0.18 * novelty
                + 0.12 * spectral_change
                + 0.08 * min(1.0, abs(slope) * 2),
            )
            boundary = {
                "position_ms": position_ms,
                "region_index": region_index,
                "bar_lengths": supported,
                "novelty": round(novelty, 8),
                "energy_slope": round(slope, 8),
                "spectral_change": round(spectral_change, 8),
                "recurrence": round(recurrence, 8),
                "vocal_entry_exit_evidence": vocal_reason,
                "confidence": round(evidence_score, 8),
            }
            boundaries.append(boundary)
            common = {
                "timestamp_ms": position_ms,
                "region_index": region_index,
                "cue_confidence": round(evidence_score, 8),
                "confidence_tier": region["confidence_tier"],
                "local_tempo_bpm": region["normalized_tempo_bpm"],
                "bar_phase": 0,
                "novelty": round(novelty, 8),
                "vocal_risk": vocal_risk_value,
                "speech_safe": speech_safe,
                "speech_evidence": vocal_reason,
                "retained_before_ms": elapsed_ms,
                "retained_after_ms": remaining_ms,
                "section_identity": f"region-{region_index}-bar-{bars_from_origin}",
                "complete_section_before": elapsed_ms >= max(60_000, section_ms),
                "complete_section_after": remaining_ms >= max(60_000, section_ms),
            }
            progress = position_ms / max(duration_ms, 1)
            if progress <= 0.35 and common["complete_section_after"]:
                cues_by_role["entry"].append({**common, "role": "entry", "role_score": round(evidence_score + (0.35 - progress) * 0.2, 8)})
            if progress >= 0.65 and common["complete_section_before"]:
                cues_by_role["exit"].append({**common, "role": "exit", "role_score": round(evidence_score + (progress - 0.65) * 0.2, 8)})
            if slope > 0.08 and common["complete_section_after"]:
                cues_by_role["drop"].append({**common, "role": "drop", "role_score": round(evidence_score + min(0.25, slope), 8)})
            if slope < -0.06 and common["complete_section_before"]:
                cues_by_role["breakdown"].append({**common, "role": "breakdown", "role_score": round(evidence_score + min(0.25, -slope), 8)})
            if speech_safe and common["complete_section_before"]:
                cues_by_role["cut"].append({**common, "role": "cut", "role_score": round(evidence_score + 0.1, 8)})
            loop_start_index = beat_index - 16
            if loop_start_index >= start and speech_safe:
                loop_start_frame = beat_frames[loop_start_index]
                seam_error = abs(float(energy[frame]) - float(energy[loop_start_frame]))
                if seam_error <= 0.20:
                    cues_by_role["loop"].append(
                        {
                            **common,
                            "role": "loop",
                            "role_score": round(evidence_score + 0.08 * (1.0 - seam_error), 8),
                            "loop_start_ms": int(round(loop_start_frame * 1000 / v2.MODEL_FPS)),
                            "loop_end_ms": position_ms,
                            "loop_bars": 4,
                            "loop_seam_error": round(seam_error, 8),
                            "loop_verified": True,
                        }
                    )
    cues = []
    rankings = {}
    for role, values in cues_by_role.items():
        reverse_time = role in ("exit", "breakdown", "cut", "loop")
        values.sort(
            key=lambda item: (
                -item["role_score"],
                -item["timestamp_ms"] if reverse_time else item["timestamp_ms"],
            )
        )
        selected = values[:MAX_CUES_PER_ROLE]
        for rank, item in enumerate(selected, 1):
            item["role_rank"] = rank
            cues.append(item)
        rankings[role] = [item["timestamp_ms"] for item in selected]
    cues.sort(key=lambda item: (item["timestamp_ms"], CUE_ROLES.index(item["role"])))
    boundaries.sort(key=lambda item: item["position_ms"])
    return boundaries[:128], cues[: MAX_CUES_PER_ROLE * len(CUE_ROLES)], rankings


def build_dj_analysis_v3(
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
    # Reuse V2's strict source/model validation and calibrated YAMNet projection.
    base = v2.build_dj_analysis(
        model_output,
        catalog_instance_id=catalog_instance_id,
        track_id=track_id,
        media_revision=media_revision,
        representation_id=representation_id,
        content_sha256=content_sha256,
        duration_seconds=duration_seconds,
        source=source,
        key_evidence=key_evidence,
        vocal_output=vocal_output,
        vocal_calibration=vocal_calibration,
    )
    beat_logits = v2._finite_vector("beat_logits", model_output.get("beat_logits"))
    downbeat_logits = v2._finite_vector("downbeat_logits", model_output.get("downbeat_logits"), len(beat_logits))
    beat_frames = v2._peak_frames(beat_logits)
    raw_downbeat_frames = v2._peak_frames(downbeat_logits)
    matched = v2._match_downbeats(beat_frames, raw_downbeat_frames, v2.MODEL_FPS)
    regions = _localized_regions(
        beat_frames, matched, beat_logits, downbeat_logits, v2.MODEL_FPS
    )
    duration_ms = int(round(duration_seconds * 1000))
    vocal_risk = base["vocal_risk"]
    safe_intervals = _speech_safe_intervals(
        model_output, vocal_risk, duration_ms
    )
    structural, cues, rankings = _structural_and_cues(
        model_output,
        beat_frames,
        regions,
        vocal_risk,
        safe_intervals,
        duration_ms,
    )
    energy = _normalized(v2._finite_vector("energy", model_output.get("energy")))
    audible = np.flatnonzero(energy > 0.03)
    natural_entry = int(round(int(audible[0]) * 1000 / v2.MODEL_FPS)) if len(audible) else 0
    natural_exit = int(round(int(audible[-1]) * 1000 / v2.MODEL_FPS)) if len(audible) else duration_ms
    payload = {
        "schema_version": SCHEMA_VERSION,
        "method": METHOD,
        "analyzer_version": ANALYZER_VERSION,
        "catalog_instance_id": catalog_instance_id,
        "track_id": track_id,
        "media_revision": media_revision,
        "representation_id": representation_id,
        "content_sha256": content_sha256,
        "source": dict(source),
        "model": base["model"],
        "calibration_identity": {
            "method": vocal_risk.get("calibration", {}).get("method"),
            "artifact_digest": vocal_risk.get("calibration", {}).get("artifact_digest"),
            "cache_key": vocal_risk.get("calibration", {}).get("cache_key"),
            "class_map_sha256": vocal_risk.get("class_map_sha256"),
        },
        "beats": {
            "timeline": [
                {
                    "position_ms": int(round(frame * 1000 / v2.MODEL_FPS)),
                    "confidence": round(v2._sigmoid(beat_logits[frame]), 8),
                }
                for frame in beat_frames[:8192]
            ],
            "downbeats": [
                {
                    "position_ms": int(round(item["snapped_frame"] * 1000 / v2.MODEL_FPS)),
                    "raw_position_ms": int(round(item["raw_frame"] * 1000 / v2.MODEL_FPS)),
                    "confidence": round(v2._sigmoid(downbeat_logits[item["raw_frame"]]), 8),
                    "alignment_ms": int(round(item["alignment_seconds"] * 1000)),
                }
                for item in matched[:2048]
            ],
        },
        "rhythm": {"regions": regions},
        "structural_boundaries": structural,
        "cue_candidates": cues,
        "cue_rankings": rankings,
        "band_energy": {
            "unit": "mean-log-mel",
            "window": "local-bar",
            "windows": _band_windows(model_output, regions),
        },
        "vocal_risk": vocal_risk,
        "speech_safe_intervals": safe_intervals,
        "natural_boundaries": {
            "entry_ms": natural_entry,
            "exit_ms": min(duration_ms, natural_exit),
            "confidence": "model-envelope",
        },
        "key_evidence": base["key_evidence"],
        "quality": {
            "high_region_count": sum(item["confidence_tier"] == "high" for item in regions),
            "medium_region_count": sum(item["confidence_tier"] == "medium" for item in regions),
            "unavailable_region_count": sum(item["confidence_tier"] == "unavailable" for item in regions),
            "cue_counts": {role: sum(item["role"] == role for item in cues) for role in CUE_ROLES},
            "beat_count": len(beat_frames),
            "raw_downbeat_count": len(raw_downbeat_frames),
            "speech_safe_interval_count": len(safe_intervals),
            "vocal_calibration_ready": vocal_risk.get("calibration", {}).get("status") == "ready",
        },
    }
    payload["analysis_digest"] = v2.analysis_digest(payload)
    return payload


def analyze_dj_file_v3(
    path,
    *,
    catalog_instance_id,
    track_id,
    media_revision,
    model_path,
    yamnet_model_path=None,
    adapter=None,
    yamnet_adapter=None,
    deadline_seconds=v2.JOB_DEADLINE_SECONDS,
    cancelled=None,
    progress=None,
    key_evidence=None,
    vocal_calibration=None,
):
    deadline = time.monotonic() + max(1, min(v2.JOB_DEADLINE_SECONDS, deadline_seconds))
    source_path = Path(path).resolve(strict=True)
    before = source_path.stat()
    content_sha256 = v2._sha256_file(source_path, deadline=deadline, cancelled=cancelled)
    beat_worker = adapter or v2.BeatThisWindowAdapter(
        model_path, deadline=deadline, cancelled=cancelled
    )
    model_output = beat_worker.analyze(
        source_path, deadline=deadline, cancelled=cancelled, progress=progress
    )
    vocal_worker = yamnet_adapter
    if vocal_worker is None and yamnet_model_path:
        vocal_worker = v2.YamnetLiteAdapter(
            yamnet_model_path, deadline=deadline, cancelled=cancelled
        )
    vocal_output = (
        vocal_worker.analyze(source_path, deadline=deadline, cancelled=cancelled)
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
        raise v2.DjAnalysisError("source_changed")
    frame_count = len(model_output["beat_logits"])
    resampled_frames = int(
        model_output.get(
            "source_resampled_frames",
            max(1, frame_count - 1) * v2.MODEL_HOP_SAMPLES,
        )
    )
    duration_seconds = resampled_frames / v2.MODEL_SAMPLE_RATE
    source = {
        "sample_rate": int(model_output["source_sample_rate"]),
        "decoded_frames": int(model_output["source_decoded_frames"]),
        "analysis_sample_rate": v2.MODEL_SAMPLE_RATE,
        "analysis_resampled_frames": resampled_frames,
        "analysis_frames": frame_count,
        "decoder": "pyav-16.1.0-streaming",
        "timeline_verified": True,
    }
    return build_dj_analysis_v3(
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
