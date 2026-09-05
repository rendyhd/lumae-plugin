"""Deterministic, identifier-free DJ transition coverage evaluation."""

from __future__ import annotations

from collections import Counter
import math
from dataclasses import dataclass
from typing import Iterable

TIER_COST = {
    "phrase-sync": 0.0,
    "tempo-independent": 0.15,
    "one-sided": 0.30,
    "edge-fx": 0.45,
    "smoothfade": 0.80,
}
_HASHABLE_ROLES = {"entry", "exit", "drop", "breakdown", "cut", "loop"}


@dataclass(frozen=True)
class TrackEvidence:
    track_id: str
    version: int
    high_tempos: tuple[float, ...]
    medium_tempos: tuple[float, ...]
    roles: frozenset[str]
    vocal_ready: bool
    edge_ready: bool
    rejection_reasons: tuple[str, ...]
    cues: tuple[tuple[str, float | None, str], ...] = ()


def _dict(value):
    return value if isinstance(value, dict) else {}


def _list(value):
    return value if isinstance(value, list) else []


def _finite(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _tempo_compatible(left, right):
    if not _finite(left) or not _finite(right) or left <= 0 or right <= 0:
        return False
    ratio = float(left) / float(right)
    return 0.94 <= ratio <= 1.06


def _vocal_ready(payload):
    calibration = _dict(_dict(payload.get("vocal_risk")).get("calibration"))
    return (
        calibration.get("status") == "ready"
        and calibration.get("cuts_authorized") is True
    )


def _qualified_cues(payload):
    if not _vocal_ready(payload):
        return ()
    result = []
    if payload.get("schema_version") == 3:
        for cue in _list(payload.get("cue_candidates")):
            cue = _dict(cue)
            role = cue.get("role")
            if (
                role not in _HASHABLE_ROLES
                or cue.get("speech_safe") is not True
                or cue.get("confidence_tier") not in ("high", "medium")
            ):
                continue
            if role == "loop" and cue.get("loop_verified") is not True:
                continue
            section = (
                "complete_section_after"
                if role in ("entry", "drop")
                else "complete_section_before"
            )
            if cue.get(section) is not True:
                continue
            tempo = cue.get("local_tempo_bpm")
            result.append(
                (
                    role,
                    float(tempo) if _finite(tempo) and tempo > 0 else None,
                    cue["confidence_tier"],
                )
            )
    else:
        regions = _list(payload.get("regions"))
        frames = [
            _dict(row) for row in _list(_dict(payload.get("vocal_risk")).get("frames"))
        ]
        for field, role in (("entries", "entry"), ("exits", "exit")):
            for cue in _list(_dict(payload.get("candidates")).get(field)):
                cue = _dict(cue)
                index = cue.get("region_index")
                position = cue.get("time_ms")
                if (
                    not isinstance(index, int)
                    or isinstance(index, bool)
                    or not 0 <= index < len(regions)
                    or not _finite(position)
                ):
                    continue
                region = _dict(regions[index])
                tempo = region.get("tempo_bpm")
                if (
                    region.get("eligible") is not True
                    or not _finite(tempo)
                    or not region.get("start_ms", 0)
                    <= position
                    <= region.get("end_ms", -1)
                ):
                    continue
                nearby = [
                    frame
                    for frame in frames
                    if _finite(frame.get("position_ms"))
                    and abs(frame["position_ms"] - position) <= 750
                ]
                if not nearby:
                    continue
                risk = min(
                    nearby, key=lambda frame: abs(frame["position_ms"] - position)
                ).get("calibrated_risk")
                if _finite(risk) and 0 <= risk < 0.25:
                    result.append((role, float(tempo), "high"))
    return tuple(result)


def _v2_evidence(payload, track_id):
    regions = _list(payload.get("regions"))
    high_tempos = tuple(
        float(region["tempo_bpm"])
        for region in regions
        if isinstance(region, dict)
        and region.get("eligible") is True
        and _finite(region.get("tempo_bpm"))
    )
    candidates = _dict(payload.get("candidates"))
    roles = set()
    if _list(candidates.get("entries")):
        roles.add("entry")
    if _list(candidates.get("exits")):
        roles.add("exit")
    reasons = []
    for region in regions:
        reasons.extend(
            str(reason) for reason in _list(_dict(region).get("rejection_reasons"))
        )
    return TrackEvidence(
        track_id=track_id,
        version=2,
        high_tempos=high_tempos,
        medium_tempos=(),
        roles=frozenset(roles),
        vocal_ready=_vocal_ready(payload),
        edge_ready=payload.get("edge_profile_ready") is True,
        rejection_reasons=tuple(reasons),
        cues=_qualified_cues(payload),
    )


def _v3_evidence(payload, track_id):
    rhythm = _dict(payload.get("rhythm"))
    regions = _list(rhythm.get("regions"))
    high_tempos = []
    medium_tempos = []
    reasons = []
    for region in regions:
        item = _dict(region)
        tempo = item.get("normalized_tempo_bpm")
        confidence = item.get("confidence_tier")
        if _finite(tempo) and confidence == "high":
            high_tempos.append(float(tempo))
        elif _finite(tempo) and confidence == "medium":
            medium_tempos.append(float(tempo))
        reasons.extend(str(reason) for reason in _list(item.get("rejection_reasons")))
    roles = frozenset(
        str(_dict(cue).get("role"))
        for cue in _list(payload.get("cue_candidates"))
        if str(_dict(cue).get("role")) in _HASHABLE_ROLES
        and _dict(cue).get("confidence_tier") in ("high", "medium")
    )
    return TrackEvidence(
        track_id=track_id,
        version=3,
        high_tempos=tuple(high_tempos),
        medium_tempos=tuple(medium_tempos),
        roles=roles,
        vocal_ready=_vocal_ready(payload),
        edge_ready=payload.get("edge_profile_ready") is True,
        rejection_reasons=tuple(reasons),
        cues=_qualified_cues(payload),
    )


def _unsupported_evidence(payload, track_id):
    return TrackEvidence(
        track_id=track_id,
        version=int(payload["schema_version"]),
        high_tempos=(),
        medium_tempos=(),
        roles=frozenset(),
        vocal_ready=False,
        edge_ready=False,
        rejection_reasons=("unsupported-analysis-version",),
    )


def track_evidence(payload):
    item = _dict(payload)
    track_id = item.get("track_id")
    version = item.get("schema_version")
    if not isinstance(track_id, str) or not track_id:
        return None
    if version == 3:
        return _v3_evidence(item, track_id)
    if version == 2:
        return _v2_evidence(item, track_id)
    if isinstance(version, int) and not isinstance(version, bool) and version > 0:
        return _unsupported_evidence(item, track_id)
    return None


def _has_outgoing(evidence, roles):
    return any(role in roles for role, tempo, tier in evidence.cues)


def _has_incoming(evidence, roles):
    return any(role in roles for role, tempo, tier in evidence.cues)


def classify_pair(outgoing, incoming):
    if outgoing.version not in (2, 3) or incoming.version not in (2, 3):
        return "smoothfade", "unsupported-analysis-version", False
    left = [
        cue for cue in outgoing.cues if cue[0] in {"exit", "loop", "cut", "breakdown"}
    ]
    right = [cue for cue in incoming.cues if cue[0] in {"entry", "drop"}]
    compatible = any(
        a[2] == "high" and b[2] == "high" and _tempo_compatible(a[1], b[1])
        for a in left
        for b in right
    )
    if compatible:
        return "phrase-sync", None, True
    if (
        outgoing.version == 3
        and incoming.version == 3
        and outgoing.vocal_ready
        and incoming.vocal_ready
    ):
        if left and right:
            return "tempo-independent", None, False
        if bool(left) != bool(right):
            return "one-sided", None, False
        if outgoing.edge_ready and incoming.edge_ready:
            return "edge-fx", None, False
    reason = (
        "vocal-calibration-unavailable"
        if not outgoing.vocal_ready or not incoming.vocal_ready
        else "no-qualified-cues" if not left or not right else "tempo-out-of-range"
    )
    return "smoothfade", reason, False


def _unique_evidence(payloads):
    output = {}
    for payload in payloads:
        evidence = track_evidence(payload)
        if evidence is not None:
            output[evidence.track_id] = evidence
    return output


def _pairs(evidence, manifest):
    if manifest:
        pairs = _list(_dict(manifest).get("pairs"))
        output = []
        for pair in pairs:
            item = _dict(pair)
            left = evidence.get(item.get("from"))
            right = evidence.get(item.get("to"))
            if (
                left is not None
                and right is not None
                and left.track_id != right.track_id
            ):
                output.append((left, right))
        return output
    values = list(evidence.values())
    return [
        (left, right)
        for left in values
        for right in values
        if left.track_id != right.track_id
    ]


def _order_cost(order, evidence):
    ids = [value for value in order if isinstance(value, str) and value in evidence]
    if len(ids) < 2:
        return None
    total = 0.0
    for left_id, right_id in zip(ids, ids[1:]):
        tier, _, _ = classify_pair(evidence[left_id], evidence[right_id])
        total += TIER_COST[tier]
    return round(total, 6)


def evaluate_coverage(payloads: Iterable[dict], manifest=None):
    evidence = _unique_evidence(payloads)
    pairs = _pairs(evidence, manifest)
    tiers = Counter()
    rejections = Counter()
    compatible = 0
    for outgoing, incoming in pairs:
        tier, reason, tempo_compatible = classify_pair(outgoing, incoming)
        tiers[tier] += 1
        compatible += int(tempo_compatible)
        if reason:
            rejections[reason] += 1

    versions = Counter(item.version for item in evidence.values())
    region_ready = sum(
        bool(item.high_tempos or item.medium_tempos) for item in evidence.values()
    )
    entry_ready = sum(
        _has_incoming(item, {"entry", "drop"}) for item in evidence.values()
    )
    exit_ready = sum(
        _has_outgoing(item, {"exit", "loop", "cut", "breakdown"})
        for item in evidence.values()
    )
    report = {
        "evaluation": "cue-eligibility-estimate-v2",
        "playback_qualification": False,
        "analysis": {
            "total": len(evidence),
            "v2": versions[2],
            "v3": versions[3],
            "unsupported": sum(
                count for version, count in versions.items() if version not in (2, 3)
            ),
            "rhythm_ready": region_ready,
            "entry_ready": entry_ready,
            "exit_ready": exit_ready,
            "vocal_ready": sum(item.vocal_ready for item in evidence.values()),
            "edge_ready": sum(item.edge_ready for item in evidence.values()),
        },
        "pairs": {
            "total": len(pairs),
            "tempo_compatible": compatible,
            "tiers": {tier: tiers[tier] for tier in TIER_COST},
            "rejections": dict(sorted(rejections.items())),
        },
        "region_rejections": dict(
            sorted(
                Counter(
                    reason
                    for item in evidence.values()
                    for reason in item.rejection_reasons
                ).items()
            )
        ),
    }
    orders = _list(_dict(manifest).get("orders")) if manifest else []
    comparisons = []
    for value in orders:
        item = _dict(value)
        original = _order_cost(_list(item.get("original")), evidence)
        reordered = _order_cost(_list(item.get("reordered")), evidence)
        if original is not None and reordered is not None:
            comparisons.append(
                {
                    "original_cost": original,
                    "reordered_cost": reordered,
                    "improvement": round(original - reordered, 6),
                }
            )
    report["ordering"] = {
        "comparisons": len(comparisons),
        "improved": sum(item["improvement"] > 0 for item in comparisons),
        "unchanged": sum(item["improvement"] == 0 for item in comparisons),
        "regressed": sum(item["improvement"] < 0 for item in comparisons),
        "total_improvement": round(sum(item["improvement"] for item in comparisons), 6),
    }
    return report
