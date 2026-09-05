"""Lumae-compatible album dynamics fingerprints and ranking.

This is a Python port of Auralscape's ``albumDynamics.ts`` and
``albumSimilarity.ts``.  Keep changes synchronized with the TypeScript golden
fixtures: the web plugin and Lumae must assign the same ordering to the same
fingerprints.
"""

from __future__ import annotations

import base64
import math
import re
from dataclasses import dataclass

import numpy as np


FINGERPRINT_SCHEMA_VERSION = 1
FINGERPRINT_METHOD = "lumae_album_dynamics_v1"
EMBEDDING_FAMILY = "musicnn-200"

MAX_POLES = 3
MIN_POLE_DISTANCE = 0.12
OUTLIER_MIN_DISTANCE = 0.22

SIMILARITY_WEIGHTS = {
    "core": 0.38,
    "poles": 0.22,
    "spread": 0.14,
    "energy": 0.10,
    "mood": 0.08,
    "path": 0.08,
}

WIDE_ALBUM_PAIRWISE = 0.18
SEVERE_COMPRESSION_RATIO = 0.55
SEVERE_COMPRESSION_PENALTY = 0.12
REASON_THRESHOLD = 0.18
CORE_REASON_THRESHOLD = 0.12

MOOD_FEATURE_NAMES = (
    "danceable",
    "aggressive",
    "happy",
    "party",
    "relaxed",
    "sad",
)


@dataclass(frozen=True)
class AlbumTrack:
    track_id: str
    embedding: np.ndarray
    energy: float | None
    mood: np.ndarray | None
    order: int


def _float32(vector):
    result = np.asarray(vector, dtype=np.float32)
    if result.ndim != 1 or result.size == 0:
        raise ValueError("vectors must be non-empty and one-dimensional")
    return result


def cosine_distance(a, b):
    left = _float32(a)
    right = _float32(b)
    if left.size != right.size:
        raise ValueError(f"Dimension mismatch: {left.size} vs {right.size}")
    dot = float(np.dot(left.astype(np.float64), right.astype(np.float64)))
    norm_left = float(np.linalg.norm(left.astype(np.float64)))
    norm_right = float(np.linalg.norm(right.astype(np.float64)))
    denominator = norm_left * norm_right
    if denominator == 0:
        return 1.0
    return 1.0 - dot / denominator


def vector_mean(vectors):
    if not vectors:
        raise ValueError("Cannot compute mean of zero vectors")
    first = _float32(vectors[0])
    total = np.zeros(first.size, dtype=np.float32)
    for vector in vectors:
        current = _float32(vector)
        if current.size != first.size:
            raise ValueError(f"Dimension mismatch: {first.size} vs {current.size}")
        total += current
    total /= len(vectors)
    return total


def _quantile(sorted_values, q):
    if not sorted_values:
        return 0.0
    position = (len(sorted_values) - 1) * q
    base = math.floor(position)
    rest = position - base
    if base + 1 >= len(sorted_values):
        return float(sorted_values[base])
    return float(
        sorted_values[base] + rest * (sorted_values[base + 1] - sorted_values[base])
    )


def _range_stats(values):
    if not values:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "q1": 0.0, "q3": 0.0}
    sorted_values = sorted(float(value) for value in values)
    return {
        "min": sorted_values[0],
        "max": sorted_values[-1],
        "mean": sum(sorted_values) / len(sorted_values),
        "q1": _quantile(sorted_values, 0.25),
        "q3": _quantile(sorted_values, 0.75),
    }


def _pairwise_distances(tracks):
    count = len(tracks)
    distances = np.zeros((count, count), dtype=np.float64)
    for left in range(count):
        for right in range(left + 1, count):
            distance = cosine_distance(tracks[left].embedding, tracks[right].embedding)
            distances[left, right] = distance
            distances[right, left] = distance
    return distances


def _spread_stats(tracks, mean_vector, pairwise):
    center_distances = [
        cosine_distance(track.embedding, mean_vector) for track in tracks
    ]
    pair_values = [
        float(pairwise[left, right])
        for left in range(len(tracks))
        for right in range(left + 1, len(tracks))
    ]
    return {
        "meanDistanceFromCenter": sum(center_distances) / len(center_distances),
        "maxDistanceFromCenter": max(center_distances, default=0.0),
        "meanPairwiseDistance": (
            sum(pair_values) / len(pair_values) if pair_values else 0.0
        ),
        "maxPairwiseDistance": max(pair_values, default=0.0),
    }


def _path_stats(tracks, pairwise):
    if len(tracks) <= 1:
        return {
            "meanStepDistance": 0.0,
            "maxStepDistance": 0.0,
            "totalPathDistance": 0.0,
            "netPathRatio": 0.0,
        }
    steps = [float(pairwise[index, index + 1]) for index in range(len(tracks) - 1)]
    total = sum(steps)
    return {
        "meanStepDistance": total / len(steps),
        "maxStepDistance": max(steps, default=0.0),
        "totalPathDistance": total,
        "netPathRatio": (
            0.0 if total == 0 else float(pairwise[0, len(tracks) - 1]) / total
        ),
    }


def _mood_stats(tracks):
    moods = [track.mood for track in tracks if track.mood is not None]
    if not moods:
        empty = np.zeros(0, dtype=np.float32)
        return {"mean": empty, "min": empty, "max": empty}
    matrix = np.stack([_float32(mood) for mood in moods])
    return {
        "mean": np.mean(matrix, axis=0, dtype=np.float32),
        "min": np.min(matrix, axis=0).astype(np.float32),
        "max": np.max(matrix, axis=0).astype(np.float32),
    }


def _center_index(tracks, mean_vector):
    return min(
        range(len(tracks)),
        key=lambda index: cosine_distance(tracks[index].embedding, mean_vector),
    )


def _select_farthest_pole(tracks, selected, pairwise):
    best_index = None
    best_distance = -math.inf
    for index, track in enumerate(tracks):
        if index in selected:
            continue
        minimum = min(float(pairwise[index, pole]) for pole in selected)
        if minimum > best_distance or (
            minimum == best_distance
            and best_index is not None
            and track.order < tracks[best_index].order
        ):
            best_index = index
            best_distance = minimum
    if best_index is None or best_distance < MIN_POLE_DISTANCE:
        return None
    return best_index


def _assign_clusters(medoids, pairwise):
    clusters = [[] for _ in medoids]
    for index in range(pairwise.shape[0]):
        best = min(
            range(len(medoids)),
            key=lambda pole_index: float(pairwise[index, medoids[pole_index]]),
        )
        clusters[best].append(index)
    return clusters


def _refine_medoid(cluster, pairwise):
    if not cluster:
        return -1
    if len(cluster) == 1:
        return cluster[0]
    return min(
        cluster,
        key=lambda index: sum(
            float(pairwise[index, other]) for other in cluster if other != index
        )
        / (len(cluster) - 1),
    )


def _build_poles(tracks, mean_vector, center_index, pairwise):
    if len(tracks) == 1:
        return [
            {
                "embedding": tracks[0].embedding,
                "weight": 1.0,
                "distanceFromCenter": cosine_distance(tracks[0].embedding, mean_vector),
            }
        ]
    selected = [center_index]
    while len(selected) < MAX_POLES:
        next_index = _select_farthest_pole(tracks, selected, pairwise)
        if next_index is None:
            break
        selected.append(next_index)
    medoids = list(selected)
    for _pass in range(2):
        clusters = _assign_clusters(medoids, pairwise)
        current = list(medoids)
        refined = [_refine_medoid(cluster, pairwise) for cluster in clusters]
        medoids = [
            index if index >= 0 else current[pos] for pos, index in enumerate(refined)
        ]
    clusters = _assign_clusters(medoids, pairwise)
    return [
        {
            "embedding": tracks[medoid].embedding,
            "weight": len(clusters[index]) / len(tracks),
            "distanceFromCenter": cosine_distance(
                tracks[medoid].embedding, mean_vector
            ),
        }
        for index, medoid in enumerate(medoids)
    ]


def _build_outliers(tracks, mean_vector):
    distances = [cosine_distance(track.embedding, mean_vector) for track in tracks]
    sorted_distances = sorted(distances)
    q1 = _quantile(sorted_distances, 0.25)
    q3 = _quantile(sorted_distances, 0.75)
    threshold = max(OUTLIER_MIN_DISTANCE, q3 + (q3 - q1) * 0.5)
    candidates = [
        (distances[index], tracks[index].order, tracks[index].track_id)
        for index in range(len(tracks))
        if distances[index] >= threshold
    ]
    candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
    return [item[2] for item in candidates[:3]]


def build_fingerprint(album_key, tracks):
    if not tracks:
        return None
    ordered = sorted(tracks, key=lambda track: (track.order, track.track_id))
    mean_vector = vector_mean([track.embedding for track in ordered])
    pairwise = _pairwise_distances(ordered)
    center_index = _center_index(ordered, mean_vector)
    return {
        "albumKey": album_key,
        "meanVector": mean_vector,
        "poles": _build_poles(ordered, mean_vector, center_index, pairwise),
        "spread": _spread_stats(ordered, mean_vector, pairwise),
        "path": _path_stats(ordered, pairwise),
        "energy": _range_stats(
            [track.energy for track in ordered if track.energy is not None]
        ),
        "mood": _mood_stats(ordered),
        "outlierTrackIds": _build_outliers(ordered, mean_vector),
    }


def _normalize_distance(value):
    if math.isnan(value):
        return 1.0
    return max(0.0, min(1.0, value))


def _edition_identity(value):
    result = str(value or "").strip().lower()
    suffix = re.compile(
        r"\s*\(([^)]*(deluxe|japanese|remastered|expanded)[^)]*)\)\s*$", re.I
    )
    while suffix.search(result):
        result = suffix.sub("", result).strip()
    return result


def _same_album(source, candidate):
    return _edition_identity(source["album"]) == _edition_identity(
        candidate["album"]
    ) and _edition_identity(source["artist"]) == _edition_identity(candidate["artist"])


def _interval(stats):
    if stats["q1"] == 0 and stats["q3"] == 0:
        return stats["min"], stats["max"]
    return stats["q1"], stats["q3"]


def _interval_distance(left, right):
    if left == (0, 0) or right == (0, 0):
        return 0.0
    if left[0] == left[1] and right[0] == right[1]:
        return _normalize_distance(abs(left[0] - right[0]))
    start = max(left[0], right[0])
    end = min(left[1], right[1])
    if end < start:
        return _normalize_distance(start - end)
    overlap = end - start
    span = max(left[1], right[1]) - min(left[0], right[0])
    return 0.0 if span <= 0 else _normalize_distance(1 - overlap / span)


def _poles_distance(source, candidate):
    if not source["poles"]:
        return 0.0
    if not candidate["poles"]:
        return 1.0
    distances = []
    for source_pole in source["poles"]:
        distances.append(
            min(
                cosine_distance(source_pole["embedding"], candidate_pole["embedding"])
                for candidate_pole in candidate["poles"]
            )
        )
    return _normalize_distance(sum(distances) / len(distances))


def score_similar_album(source, candidate):
    source_fp = source["fingerprint"]
    candidate_fp = candidate["fingerprint"]
    distances = {
        "core": _normalize_distance(
            cosine_distance(source_fp["meanVector"], candidate_fp["meanVector"])
        ),
        "poles": _poles_distance(source_fp, candidate_fp),
        "spread": _normalize_distance(
            abs(
                source_fp["spread"]["meanPairwiseDistance"]
                - candidate_fp["spread"]["meanPairwiseDistance"]
            )
            + (
                SEVERE_COMPRESSION_PENALTY
                if source_fp["spread"]["meanPairwiseDistance"] >= WIDE_ALBUM_PAIRWISE
                and candidate_fp["spread"]["meanPairwiseDistance"]
                < source_fp["spread"]["meanPairwiseDistance"] * SEVERE_COMPRESSION_RATIO
                else 0.0
            )
        ),
        "energy": _interval_distance(
            _interval(source_fp["energy"]), _interval(candidate_fp["energy"])
        ),
        "mood": 0.0,
        "path": _normalize_distance(
            abs(
                source_fp["path"]["meanStepDistance"]
                - candidate_fp["path"]["meanStepDistance"]
            )
        ),
    }
    if source_fp["mood"]["mean"].size and candidate_fp["mood"]["mean"].size:
        distances["mood"] = _normalize_distance(
            cosine_distance(source_fp["mood"]["mean"], candidate_fp["mood"]["mean"])
        )
    reasons = [
        reason
        for reason, _distance in sorted(distances.items(), key=lambda item: item[1])
        if distances[reason] < REASON_THRESHOLD
    ][:2]
    if distances["core"] < CORE_REASON_THRESHOLD and "core" not in reasons:
        reasons.insert(0, "core")
    score = sum(distances[name] * weight for name, weight in SIMILARITY_WEIGHTS.items())
    return score, list(dict.fromkeys(reasons))


def rank_similar_albums(
    source, candidates, limit=12, max_per_artist=1, fallback_max_per_artist=3
):
    scored = []
    for candidate in candidates:
        if _same_album(source, candidate):
            continue
        score, reasons = score_similar_album(source, candidate)
        item = dict(candidate)
        item["score"] = score
        item["reasons"] = reasons
        scored.append(item)
    scored.sort(
        key=lambda item: (
            item["score"],
            item["artist"].lower(),
            item["album"].lower(),
            item["albumKey"],
        )
    )
    output = []
    chosen = set()
    artist_counts = {}
    for cap in range(max_per_artist, max(fallback_max_per_artist, max_per_artist) + 1):
        for candidate in scored:
            key = (candidate.get("instanceId", "local"), candidate["albumKey"])
            if key in chosen:
                continue
            artist_key = _edition_identity(candidate["artist"])
            if artist_counts.get(artist_key, 0) >= cap:
                continue
            chosen.add(key)
            artist_counts[artist_key] = artist_counts.get(artist_key, 0) + 1
            output.append(candidate)
            if len(output) >= limit:
                return output
    return output


def rank_for_sonic_fingerprint(centroid, candidates, owned_album_keys, limit=3):
    taste = _float32(centroid)
    scored = []
    for candidate in candidates:
        if candidate["albumKey"] in owned_album_keys:
            continue
        fingerprint = candidate["fingerprint"]
        core = _normalize_distance(cosine_distance(taste, fingerprint["meanVector"]))
        pole = min(
            (
                cosine_distance(taste, item["embedding"])
                for item in fingerprint["poles"]
            ),
            default=core,
        )
        item = dict(candidate)
        item["score"] = core * 0.8 + _normalize_distance(pole) * 0.2
        item["reasons"] = ["core"] if core < CORE_REASON_THRESHOLD else ["poles"]
        scored.append(item)
    scored.sort(
        key=lambda item: (
            item["score"],
            item["artist"].lower(),
            item["album"].lower(),
            item["albumKey"],
        )
    )
    output = []
    artists = set()
    for item in scored:
        artist = _edition_identity(item["artist"])
        if artist in artists:
            continue
        artists.add(artist)
        output.append(item)
        if len(output) >= limit:
            break
    return output


def encode_vector(vector):
    raw = _float32(vector).astype("<f4", copy=False).tobytes()
    return base64.b64encode(raw).decode("ascii")


def decode_vector(value):
    raw = base64.b64decode(str(value or ""), validate=True)
    if not raw or len(raw) % 4:
        raise ValueError("invalid float32 vector payload")
    return np.frombuffer(raw, dtype="<f4").copy()


def serialize_fingerprint(fingerprint):
    return {
        "schemaVersion": FINGERPRINT_SCHEMA_VERSION,
        "method": FINGERPRINT_METHOD,
        "embeddingFamily": EMBEDDING_FAMILY,
        "meanVector": encode_vector(fingerprint["meanVector"]),
        "poles": [
            {
                "embedding": encode_vector(pole["embedding"]),
                "weight": pole["weight"],
                "distanceFromCenter": pole["distanceFromCenter"],
            }
            for pole in fingerprint["poles"]
        ],
        "spread": fingerprint["spread"],
        "path": fingerprint["path"],
        "energy": fingerprint["energy"],
        "mood": {
            "mean": (
                encode_vector(fingerprint["mood"]["mean"])
                if fingerprint["mood"]["mean"].size
                else ""
            ),
            "min": (
                encode_vector(fingerprint["mood"]["min"])
                if fingerprint["mood"]["min"].size
                else ""
            ),
            "max": (
                encode_vector(fingerprint["mood"]["max"])
                if fingerprint["mood"]["max"].size
                else ""
            ),
        },
    }


def deserialize_fingerprint(payload, album_key):
    if int(payload.get("schemaVersion", 0)) != FINGERPRINT_SCHEMA_VERSION:
        raise ValueError("unsupported album fingerprint schema")
    if payload.get("method") != FINGERPRINT_METHOD:
        raise ValueError("unsupported album fingerprint method")
    if payload.get("embeddingFamily") != EMBEDDING_FAMILY:
        raise ValueError("unsupported embedding family")
    return {
        "albumKey": album_key,
        "meanVector": decode_vector(payload["meanVector"]),
        "poles": [
            {
                "embedding": decode_vector(pole["embedding"]),
                "weight": float(pole["weight"]),
                "distanceFromCenter": float(pole["distanceFromCenter"]),
            }
            for pole in payload.get("poles", [])
        ],
        "spread": payload["spread"],
        "path": payload["path"],
        "energy": payload["energy"],
        "mood": {
            name: (
                decode_vector(payload["mood"][name])
                if payload.get("mood", {}).get(name)
                else np.zeros(0, dtype=np.float32)
            )
            for name in ("mean", "min", "max")
        },
    }


def parse_feature_map(value):
    output = {}
    for pair in str(value or "").split(","):
        label, separator, raw_score = pair.rpartition(":")
        if not separator or not label.strip():
            continue
        try:
            output[label.strip()] = float(raw_score)
        except (TypeError, ValueError):
            continue
    return output


def mood_features(mood_vector, other_features):
    moods = parse_feature_map(mood_vector)
    others = parse_feature_map(other_features)
    values = []
    for name in MOOD_FEATURE_NAMES:
        values.append(others.get(name, moods.get(name, 0.0)))
    return np.asarray(values, dtype=np.float32)


def normalize_energy(raw):
    if raw is None:
        return None
    return max(0.0, min(1.0, (float(raw) - 0.01) / 0.14))
