"""Deterministic, identifier-free DJ transition coverage evaluation."""

from __future__ import annotations

from collections import Counter
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


def _dict(value):
    return value if isinstance(value, dict) else {}


def _list(value):
    return value if isinstance(value, list) else []


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _tempo_compatible(left, right):
    if not _finite(left) or not _finite(right) or left <= 0 or right <= 0:
        return False
    ratio = float(left) / float(right)
    return 0.94 <= ratio <= 1.06


def _vocal_ready(payload):
    calibration = _dict(_dict(payload.get("vocal_risk")).get("calibration"))
    return calibration.get("status") == "ready" and calibration.get("cuts_authorized") is True


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
        reasons.extend(str(reason) for reason in _list(_dict(region).get("rejection_reasons")))
    return TrackEvidence(
        track_id=track_id,
        version=2,
        high_tempos=high_tempos,
        medium_tempos=(),
        roles=frozenset(roles),
        vocal_ready=_vocal_ready(payload),
        edge_ready=payload.get("edge_profile_ready") is True,
        rejection_reasons=tuple(reasons),
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
    return bool(evidence.roles.intersection(roles))


def _has_incoming(evidence, roles):
    return bool(evidence.roles.intersection(roles))


def classify_pair(outgoing, incoming):
    if outgoing.version not in (2, 3) or incoming.version not in (2, 3):
        return "smoothfade", "unsupported-analysis-version", False

    outgoing_high = _has_outgoing(outgoing, {"exit", "loop", "cut", "breakdown"})
    incoming_high = _has_incoming(incoming, {"entry", "drop"})
    compatible = any(
        _tempo_compatible(left, right)
        for left in outgoing.high_tempos
        for right in incoming.high_tempos
    )
    if outgoing_high and incoming_high and compatible and outgoing.vocal_ready and incoming.vocal_ready:
        return "phrase-sync", None, compatible

    if outgoing.version == 3 and incoming.version == 3 and outgoing.vocal_ready and incoming.vocal_ready:
        if _has_outgoing(outgoing, {"exit", "cut", "breakdown"}) and _has_incoming(
            incoming, {"entry", "drop"}
        ):
            return "tempo-independent", None, compatible
        rhythmic_out = bool(outgoing.high_tempos or outgoing.medium_tempos) and outgoing_high
        rhythmic_in = bool(incoming.high_tempos or incoming.medium_tempos) and incoming_high
        if rhythmic_out != rhythmic_in:
            return "one-sided", None, compatible
        if outgoing.edge_ready and incoming.edge_ready:
            return "edge-fx", None, compatible

    if not outgoing.vocal_ready or not incoming.vocal_ready:
        reason = "vocal-calibration-unavailable"
    elif not outgoing_high or not incoming_high:
        reason = "no-qualified-cues"
    elif not compatible:
        reason = "tempo-out-of-range"
    else:
        reason = "no-qualified-tier"
    return "smoothfade", reason, compatible


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
            if left is not None and right is not None and left.track_id != right.track_id:
                output.append((left, right))
        return output
    values = list(evidence.values())
    return [(left, right) for left in values for right in values if left.track_id != right.track_id]


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
    region_ready = sum(bool(item.high_tempos or item.medium_tempos) for item in evidence.values())
    entry_ready = sum(_has_incoming(item, {"entry", "drop"}) for item in evidence.values())
    exit_ready = sum(
        _has_outgoing(item, {"exit", "loop", "cut", "breakdown"}) for item in evidence.values()
    )
    report = {
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
                    reason for item in evidence.values() for reason in item.rejection_reasons
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
