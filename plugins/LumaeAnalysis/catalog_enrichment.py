"""Server-owned Lumae playback and library enrichment streams.

The mobile app owns private listening state, but deterministic library-wide
work belongs beside AudioMuse's provider catalogue and analysis projection.
This module publishes:

* completed loudness/MixRamp profiles as a cursor stream; and
* album/artist relationship snapshots calculated with Lumae's native scoring
  model after each published analysis generation.

Neither stream is a catalogue-readiness gate.  Mobile clients bootstrap what
is currently ready, advance opaque cursors atomically with local writes, and
pick up later work during an ordinary sync.
"""

from datetime import datetime, timezone
import base64
import hashlib
import json
import math
import re
import uuid

import numpy as np

from plugin.api import get_db, table

from .catalog import (
    CatalogScanError,
    canonical_json,
    opaque_cursor,
    parse_opaque_cursor,
    resolve_catalog_source,
)


RELATIONSHIP_SCHEMA_VERSION = 1
RELATIONSHIP_ALGORITHM_VERSION = 1
RELATIONSHIP_LIMIT = 12
RELATIONSHIP_CANDIDATE_TRACKS_PER_VECTOR = 96
RELATIONSHIP_MAX_CANDIDATE_ENTITIES = 384
RELATIONSHIP_ENTITY_SAMPLE_LIMIT = 160
ENRICHMENT_STALE_HOURS = 2
MOOD_FEATURE_NAMES = ("danceable", "aggressive", "happy", "party", "relaxed", "sad")

_ALBUM_WEIGHTS = {
    "core": 0.38,
    "poles": 0.22,
    "spread": 0.14,
    "energy": 0.10,
    "mood": 0.08,
    "path": 0.08,
}
_ARTIST_WEIGHTS = {
    "poles": 0.32,
    "core": 0.22,
    "spread": 0.14,
    "outliers": 0.12,
    "energyMood": 0.12,
    "trajectory": 0.08,
}
_REASON_THRESHOLD = 0.18
_CORE_REASON_THRESHOLD = 0.12
_EDITION_SUFFIX = re.compile(
    r"\s*\(([^)]*(deluxe|japanese|remastered|expanded)[^)]*)\)\s*$",
    re.IGNORECASE,
)


class RelationshipIndexUnavailable(RuntimeError):
    pass


def t(name):
    return table(name)


def _json(value, fallback=None):
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return fallback


def _iso(value):
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    return str(value)


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _fingerprint(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _stream_state(cur, table_name, catalog_instance_id):
    cur.execute(
        f"SELECT epoch, head_seq, floor_seq FROM {t(table_name)} "
        "WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    if row:
        return str(row[0]), int(row[1]), int(row[2])
    epoch = str(uuid.uuid4())
    cur.execute(
        f"INSERT INTO {t(table_name)} "
        "(catalog_instance_id, epoch, head_seq, floor_seq, updated_at) "
        "VALUES (%s, %s, 0, 0, now())",
        (catalog_instance_id, epoch),
    )
    return epoch, 0, 0


def migrate_enrichment(db):
    """Create source-scoped, independently cursorable enrichment storage."""
    cur = db.cursor()
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {t("profile_stream_state")} (
            catalog_instance_id TEXT PRIMARY KEY
                REFERENCES {t("catalog_sources")}(catalog_instance_id) ON DELETE CASCADE,
            epoch TEXT NOT NULL,
            head_seq BIGINT NOT NULL DEFAULT 0,
            floor_seq BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("profile_changes")} (
            catalog_instance_id TEXT NOT NULL,
            epoch TEXT NOT NULL,
            seq BIGINT NOT NULL,
            track_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, epoch, seq)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {t("profile_changes_track_idx")}
        ON {t("profile_changes")} (catalog_instance_id, track_id)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("relationship_state")} (
            catalog_instance_id TEXT PRIMARY KEY
                REFERENCES {t("catalog_sources")}(catalog_instance_id) ON DELETE CASCADE,
            relationship_schema_version INTEGER NOT NULL
                DEFAULT {RELATIONSHIP_SCHEMA_VERSION},
            algorithm_version INTEGER NOT NULL DEFAULT {RELATIONSHIP_ALGORITHM_VERSION},
            source_catalog_generation BIGINT NOT NULL DEFAULT 0,
            source_analysis_generation BIGINT NOT NULL DEFAULT 0,
            result_generation BIGINT NOT NULL DEFAULT 0,
            epoch TEXT NOT NULL,
            head_seq BIGINT NOT NULL DEFAULT 0,
            floor_seq BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'not_initialized',
            album_count BIGINT NOT NULL DEFAULT 0,
            artist_count BIGINT NOT NULL DEFAULT 0,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("relationship_results")} (
            catalog_instance_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            result_generation BIGINT NOT NULL,
            result_fp TEXT NOT NULL,
            payload JSONB NOT NULL,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, entity_type, entity_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("relationship_changes")} (
            catalog_instance_id TEXT NOT NULL,
            epoch TEXT NOT NULL,
            seq BIGINT NOT NULL,
            generation BIGINT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, epoch, seq)
        )
        """,
    ]
    for statement in statements:
        cur.execute(statement)

    cur.execute(f"SELECT catalog_instance_id FROM {t('catalog_sources')}")
    source_ids = [str(row[0]) for row in cur.fetchall()]
    for catalog_instance_id in source_ids:
        _stream_state(cur, "profile_stream_state", catalog_instance_id)
        cur.execute(
            f"""
            INSERT INTO {t("relationship_state")}
                (catalog_instance_id, relationship_schema_version,
                 algorithm_version, epoch)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (catalog_instance_id) DO UPDATE SET
                relationship_schema_version=CASE
                    WHEN {t("relationship_state")}.result_generation = 0
                    THEN EXCLUDED.relationship_schema_version
                    ELSE {t("relationship_state")}.relationship_schema_version
                END,
                algorithm_version=CASE
                    WHEN {t("relationship_state")}.result_generation = 0
                    THEN EXCLUDED.algorithm_version
                    ELSE {t("relationship_state")}.algorithm_version
                END,
                status=CASE
                    WHEN {t("relationship_state")}.relationship_schema_version
                           <> EXCLUDED.relationship_schema_version
                      OR {t("relationship_state")}.algorithm_version
                           <> EXCLUDED.algorithm_version
                    THEN 'stale'
                    ELSE {t("relationship_state")}.status
                END,
                updated_at=CASE
                    WHEN {t("relationship_state")}.relationship_schema_version
                           <> EXCLUDED.relationship_schema_version
                      OR {t("relationship_state")}.algorithm_version
                           <> EXCLUDED.algorithm_version
                    THEN now()
                    ELSE {t("relationship_state")}.updated_at
                END
            """,
            (
                catalog_instance_id,
                RELATIONSHIP_SCHEMA_VERSION,
                RELATIONSHIP_ALGORITHM_VERSION,
                str(uuid.uuid4()),
            ),
        )
    cur.close()


def _bytes(value):
    if value is None:
        return b""
    if isinstance(value, memoryview):
        return value.tobytes()
    return bytes(value)


def serialize_profile(
    track_id,
    sample_rate,
    duration_ms,
    ref_lufs,
    start_ramp,
    end_ramp,
    analyzer_ver,
    analyzed_at,
    media_signature,
):
    return {
        "track_id": str(track_id),
        "source": "waveform",
        "sample_rate": int(sample_rate),
        "duration_ms": int(duration_ms),
        "ref_lufs": float(ref_lufs),
        "start_ramp": base64.b64encode(_bytes(start_ramp)).decode("ascii"),
        "end_ramp": base64.b64encode(_bytes(end_ramp)).decode("ascii"),
        "analyzer_ver": int(analyzer_ver),
        "analyzed_at": _iso(analyzed_at),
        "media_signature": media_signature,
    }


def record_profile_change(cur, catalog_instance_id, track_id, status, payload=None):
    """Append a profile upsert/delete in the same transaction as its profile."""
    epoch, head_seq, _floor_seq = _stream_state(
        cur, "profile_stream_state", catalog_instance_id
    )
    seq = head_seq + 1
    operation = "upsert" if status == "ready" and payload is not None else "delete"
    cur.execute(
        f"""
        INSERT INTO {t("profile_changes")}
            (catalog_instance_id, epoch, seq, track_id, operation, payload)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            catalog_instance_id,
            epoch,
            seq,
            str(track_id),
            operation,
            canonical_json(payload) if payload is not None and operation == "upsert" else None,
        ),
    )
    cur.execute(
        f"UPDATE {t('profile_stream_state')} "
        "SET head_seq=%s, updated_at=now() WHERE catalog_instance_id=%s",
        (seq, catalog_instance_id),
    )
    return seq


def _profile_rows(cur, catalog_instance_id, after_track_id, limit):
    cur.execute(
        f"""
        SELECT track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
               analyzer_ver, analyzed_at, media_signature
          FROM {t("source_profiles")}
         WHERE catalog_instance_id=%s AND status='ready' AND track_id > %s
         ORDER BY track_id LIMIT %s
        """,
        (catalog_instance_id, after_track_id or "", limit),
    )
    return [
        serialize_profile(*row)
        for row in cur.fetchall()
    ]


def _encode_page_token(value):
    raw = canonical_json(value).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_page_token(value):
    try:
        padded = str(value or "") + "=" * (-len(str(value or "")) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
        result = json.loads(decoded.decode("utf-8"))
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError("Invalid enrichment page token") from exc
    if not isinstance(result, dict):
        raise ValueError("Invalid enrichment page token")
    return result


def profile_bootstrap_page(db, catalog_instance_id, page_token=None, limit=250):
    sources = resolve_catalog_source(db, catalog_instance_id=catalog_instance_id)
    if len(sources) != 1:
        raise ValueError("An explicit catalogue source is required")
    catalog_instance_id = sources[0]["catalog_instance_id"]
    limit = max(1, min(int(limit), 500))
    cur = db.cursor()
    epoch, head_seq, _floor_seq = _stream_state(
        cur, "profile_stream_state", catalog_instance_id
    )
    token = _decode_page_token(page_token) if page_token else None
    if token:
        if token.get("catalog_instance_id") != catalog_instance_id or token.get("epoch") != epoch:
            cur.close()
            raise KeyError("bootstrap_required")
        pinned_head = int(token.get("head_seq", 0))
        after = str(token.get("after") or "")
    else:
        pinned_head = head_seq
        after = ""
    rows = _profile_rows(cur, catalog_instance_id, after, limit + 1)
    cur.close()
    has_more = len(rows) > limit
    rows = rows[:limit]
    next_token = None
    if has_more and rows:
        next_token = _encode_page_token(
            {
                "catalog_instance_id": catalog_instance_id,
                "epoch": epoch,
                "head_seq": pinned_head,
                "after": rows[-1]["track_id"],
            }
        )
    return {
        "schema_version": 1,
        "catalog_instance_id": catalog_instance_id,
        "profiles": rows,
        "cursor": opaque_cursor(catalog_instance_id, epoch, pinned_head),
        "next_page_token": next_token,
        "has_more": has_more,
    }


def read_profile_changes(db, cursor_value, catalog_instance_id=None, limit=250):
    cursor = parse_opaque_cursor(cursor_value)
    expected_id = catalog_instance_id or cursor["catalog_instance_id"]
    sources = resolve_catalog_source(db, catalog_instance_id=expected_id)
    if len(sources) != 1 or sources[0]["catalog_instance_id"] != cursor["catalog_instance_id"]:
        raise ValueError("Cursor belongs to another profile source")
    cur = db.cursor()
    epoch, head_seq, floor_seq = _stream_state(
        cur, "profile_stream_state", expected_id
    )
    if cursor["epoch"] != epoch or cursor["seq"] < floor_seq:
        cur.close()
        raise KeyError("bootstrap_required")
    if cursor["seq"] > head_seq:
        cur.close()
        raise ValueError("Cursor is ahead of the profile head")
    cur.execute(
        f"""
        SELECT seq, track_id, operation, payload, created_at
          FROM {t("profile_changes")}
         WHERE catalog_instance_id=%s AND epoch=%s AND seq>%s AND seq<=%s
         ORDER BY seq LIMIT %s
        """,
        (
            expected_id,
            epoch,
            cursor["seq"],
            head_seq,
            max(1, min(int(limit), 1000)),
        ),
    )
    rows = cur.fetchall()
    cur.close()
    changes = [
        {
            "seq": int(row[0]),
            "track_id": str(row[1]),
            "operation": row[2],
            "payload": _json(row[3]),
            "created_at": _iso(row[4]),
        }
        for row in rows
    ]
    next_seq = changes[-1]["seq"] if changes else cursor["seq"]
    return {
        "schema_version": 1,
        "catalog_instance_id": expected_id,
        "changes": changes,
        "cursor": opaque_cursor(expected_id, epoch, next_seq),
        "head_cursor": opaque_cursor(expected_id, epoch, head_seq),
        "has_more": next_seq < head_seq,
    }


def _vector(blob, dimensions):
    raw = _bytes(blob)
    dimensions = int(dimensions or 0)
    if dimensions <= 0 or len(raw) != dimensions * 4:
        return None
    return np.frombuffer(raw, dtype="<f4")


def _parse_features(value):
    result = {}
    if isinstance(value, dict):
        values = value.items()
    else:
        values = []
        for part in str(value or "").split(","):
            name, separator, raw = part.partition(":")
            if separator:
                values.append((name, raw))
    for name, raw in values:
        try:
            number = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            result[str(name).strip()] = number
    return result


def _mood_vector(scalar):
    mood = _parse_features((scalar or {}).get("mood_vector"))
    other = _parse_features((scalar or {}).get("other_features"))
    return [float(other.get(name, mood.get(name, 0.0))) for name in MOOD_FEATURE_NAMES]


def _energy(value):
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, (raw - 0.01) / 0.14))


def _average(values):
    return sum(values) / len(values) if values else 0.0


def _quantile(sorted_values, q):
    if not sorted_values:
        return 0.0
    pos = (len(sorted_values) - 1) * q
    base = int(math.floor(pos))
    rest = pos - base
    if base + 1 >= len(sorted_values):
        return sorted_values[base]
    return sorted_values[base] + rest * (sorted_values[base + 1] - sorted_values[base])


def _range_stats(values):
    values = sorted(value for value in values if value is not None)
    if not values:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "q1": 0.0, "q3": 0.0}
    return {
        "min": values[0],
        "max": values[-1],
        "mean": _average(values),
        "q1": _quantile(values, 0.25),
        "q3": _quantile(values, 0.75),
    }


def _vector_mean(vectors):
    if not vectors:
        return np.asarray([], dtype=np.float32)
    mean = np.zeros(len(vectors[0]), dtype=np.float64)
    for vector in vectors:
        current = np.asarray(vector, dtype=np.float32)
        if len(current) != len(mean):
            raise ValueError("embedding dimensions changed within one entity")
        np.add(mean, current, out=mean)
    return (mean / len(vectors)).astype(np.float32)


def _cosine_distance(left, right):
    if left is None or right is None or len(left) == 0 or len(right) == 0:
        return 1.0
    if len(left) != len(right):
        return 1.0
    left = np.asarray(left, dtype=np.float32)
    right = np.asarray(right, dtype=np.float32)
    dot = float(np.dot(left, right))
    left_norm = float(np.linalg.norm(left))
    right_norm = float(np.linalg.norm(right))
    if left_norm == 0 or right_norm == 0:
        return 1.0
    return max(0.0, min(2.0, 1.0 - dot / (left_norm * right_norm)))


def _pairwise(tracks):
    count = len(tracks)
    result = [[0.0] * count for _ in range(count)]
    for left in range(count):
        for right in range(left + 1, count):
            distance = _cosine_distance(
                tracks[left]["embedding"], tracks[right]["embedding"]
            )
            result[left][right] = distance
            result[right][left] = distance
    return result


def _mood_stats(tracks, artist=False):
    moods = [track["mood"] for track in tracks if track.get("mood") is not None]
    if not moods:
        empty = {"min": [], "max": [], "mean": []}
        if artist:
            empty.update({"q1": [], "q3": []})
        return empty
    by_dimension = [sorted(values) for values in zip(*moods)]
    result = {
        "min": [values[0] for values in by_dimension],
        "max": [values[-1] for values in by_dimension],
        "mean": [_average(values) for values in by_dimension],
    }
    if artist:
        result.update(
            {
                "q1": [_quantile(values, 0.25) for values in by_dimension],
                "q3": [_quantile(values, 0.75) for values in by_dimension],
            }
        )
    return result


def _assign_clusters(selected, pairwise):
    clusters = [[] for _value in selected]
    for track_index in range(len(pairwise)):
        best = min(
            range(len(selected)),
            key=lambda pole_index: pairwise[track_index][selected[pole_index]],
        )
        clusters[best].append(track_index)
    return clusters


def _refine_medoid(cluster, pairwise, tracks):
    def score(index):
        others = [pairwise[index][other] for other in cluster if other != index]
        return (_average(others), tracks[index]["order"], tracks[index]["id"])

    return min(cluster, key=score)


def _select_farthest(tracks, selected, pairwise, threshold):
    candidates = []
    for index, track in enumerate(tracks):
        if index in selected:
            continue
        distance = min(pairwise[index][other] for other in selected)
        candidates.append((-distance, track["order"], track["id"], index, distance))
    if not candidates:
        return None
    selected_row = min(candidates)
    return selected_row[3] if selected_row[4] >= threshold else None


def _album_fingerprint(key, tracks):
    if not tracks:
        return None
    tracks = sorted(tracks, key=lambda row: (row["order"], row["id"]))
    tracks = _representative_tracks(tracks)
    mean = _vector_mean([track["embedding"] for track in tracks])
    pairwise = _pairwise(tracks)
    center = min(
        range(len(tracks)),
        key=lambda index: _cosine_distance(tracks[index]["embedding"], mean),
    )
    selected = [center]
    while len(selected) < 3:
        next_index = _select_farthest(tracks, selected, pairwise, 0.12)
        if next_index is None:
            break
        selected.append(next_index)
    medoids = list(selected)
    for _pass in range(2):
        clusters = _assign_clusters(medoids, pairwise)
        medoids = [
            _refine_medoid(cluster, pairwise, tracks) if cluster else medoids[index]
            for index, cluster in enumerate(clusters)
        ]
    clusters = _assign_clusters(medoids, pairwise)
    poles = [
        {
            "trackId": tracks[medoid]["id"],
            "embedding": tracks[medoid]["embedding"],
            "weight": len(clusters[index]) / len(tracks),
        }
        for index, medoid in enumerate(medoids)
    ]
    center_distances = [
        _cosine_distance(track["embedding"], mean) for track in tracks
    ]
    distances = [
        pairwise[left][right]
        for left in range(len(tracks))
        for right in range(left + 1, len(tracks))
    ]
    steps = [pairwise[index][index + 1] for index in range(len(tracks) - 1)]
    total_path = sum(steps)
    return {
        "key": key,
        "mean": mean,
        "poles": poles,
        "spread": {
            "meanDistanceFromCenter": _average(center_distances),
            "maxDistanceFromCenter": max(center_distances, default=0.0),
            "meanPairwiseDistance": _average(distances),
            "maxPairwiseDistance": max(distances, default=0.0),
        },
        "path": {
            "meanStepDistance": _average(steps),
            "maxStepDistance": max(steps, default=0.0),
            "totalPathDistance": total_path,
            "netPathRatio": (
                pairwise[0][-1] / total_path if len(tracks) > 1 and total_path else 0.0
            ),
        },
        "energy": _range_stats([track.get("energy") for track in tracks]),
        "mood": _mood_stats(tracks),
    }


def _representative_tracks(tracks):
    if len(tracks) <= RELATIONSHIP_ENTITY_SAMPLE_LIMIT:
        return tracks
    mean = _vector_mean([track["embedding"] for track in tracks])
    edges = sorted(
        range(len(tracks)),
        key=lambda index: (
            -_cosine_distance(tracks[index]["embedding"], mean),
            tracks[index]["order"],
            tracks[index]["id"],
        ),
    )[:12]
    selected = set(edges)
    remaining = RELATIONSHIP_ENTITY_SAMPLE_LIMIT - len(selected)
    for index in range(remaining):
        ratio = 0 if remaining == 1 else index / (remaining - 1)
        selected.add(round(ratio * (len(tracks) - 1)))
    return [
        tracks[index]
        for index in sorted(selected)[:RELATIONSHIP_ENTITY_SAMPLE_LIMIT]
    ]


def _artist_fingerprint(key, tracks):
    if not tracks:
        return None
    tracks = sorted(tracks, key=lambda row: (row["order"], row["id"]))
    representative = _representative_tracks(tracks)
    pairwise = _pairwise(representative)
    core = sorted(
        range(len(representative)),
        key=lambda index: (
            _average(
                [
                    pairwise[index][other]
                    for other in range(len(representative))
                    if other != index
                ]
            ),
            representative[index]["order"],
            representative[index]["id"],
        ),
    )[: min(3, len(representative))]
    distances = [
        pairwise[left][right]
        for left in range(len(representative))
        for right in range(left + 1, len(representative))
    ]
    sorted_distances = sorted(distances)
    spread = {
        "meanPairwiseDistance": _average(sorted_distances),
        "p50PairwiseDistance": _quantile(sorted_distances, 0.5),
        "p90PairwiseDistance": _quantile(sorted_distances, 0.9),
        "maxPairwiseDistance": max(sorted_distances, default=0.0),
    }
    selected = [core[0]]
    threshold = max(0.08, spread["p50PairwiseDistance"] * 0.85)
    while len(selected) < min(4, len(representative)):
        next_index = _select_farthest(
            representative, selected, pairwise, threshold
        )
        if next_index is None:
            break
        selected.append(next_index)
    medoids = list(selected)
    for _pass in range(2):
        clusters = _assign_clusters(medoids, pairwise)
        medoids = [
            _refine_medoid(cluster, pairwise, representative)
            if cluster
            else medoids[index]
            for index, cluster in enumerate(clusters)
        ]
    clusters = _assign_clusters(medoids, pairwise)
    poles = sorted(
        [
            {
                "trackId": representative[medoid]["id"],
                "embedding": representative[medoid]["embedding"],
                "weight": len(clusters[index]) / len(representative),
            }
            for index, medoid in enumerate(medoids)
        ],
        key=lambda pole: (-pole["weight"], pole["trackId"]),
    )
    outlier_threshold = max(
        0.12,
        spread["meanPairwiseDistance"] * 1.1,
        spread["p50PairwiseDistance"] * 0.9,
    )
    outliers = sorted(
        (
            (
                -min(pairwise[index][core_index] for core_index in core),
                track["order"],
                track["id"],
                index,
            )
            for index, track in enumerate(representative)
            if index not in core
            and min(pairwise[index][core_index] for core_index in core)
            >= outlier_threshold
        )
    )[:5]

    albums = {}
    for index, track in enumerate(tracks):
        if track.get("album") is None or track.get("year") is None:
            continue
        album_key = f"{track['year']}:{str(track['album']).strip().lower()}"
        albums.setdefault(album_key, []).append(index)
    trajectory = None
    ordered_albums = sorted(
        albums,
        key=lambda key_value: (
            int(key_value.partition(":")[0]),
            key_value.partition(":")[2],
        ),
    )
    if len(ordered_albums) >= 3:
        medoid_indices = []
        for album_key in ordered_albums:
            indices = albums[album_key]
            album_mean = _vector_mean([tracks[index]["embedding"] for index in indices])
            medoid_indices.append(
                min(
                    indices,
                    key=lambda index: (
                        _cosine_distance(tracks[index]["embedding"], album_mean),
                        tracks[index]["order"],
                        tracks[index]["id"],
                    ),
                )
            )
        steps = [
            _cosine_distance(
                tracks[medoid_indices[index]]["embedding"],
                tracks[medoid_indices[index + 1]]["embedding"],
            )
            for index in range(len(medoid_indices) - 1)
        ]
        trajectory = {
            "meanStepDistance": _average(steps),
            "p50StepDistance": _quantile(sorted(steps), 0.5),
            "maxStepDistance": max(steps),
            "netDistance": _cosine_distance(
                tracks[medoid_indices[0]]["embedding"],
                tracks[medoid_indices[-1]]["embedding"],
            ),
        }
    return {
        "key": key,
        "core": [representative[index]["embedding"] for index in core],
        "poles": poles,
        "outliers": [representative[row[3]]["embedding"] for row in outliers],
        "spread": spread,
        "energy": _range_stats([track.get("energy") for track in tracks]),
        "mood": _mood_stats(tracks, artist=True),
        "trajectory": trajectory,
    }


def _normalize_distance(value):
    if not math.isfinite(value):
        return 1.0
    return max(0.0, min(1.0, value))


def _interval(stats):
    return (
        (stats["min"], stats["max"])
        if stats["q1"] == 0 and stats["q3"] == 0
        else (stats["q1"], stats["q3"])
    )


def _interval_distance(left, right):
    if left == (0.0, 0.0) or right == (0.0, 0.0):
        return 0.0
    if left[0] == left[1] and right[0] == right[1]:
        return _normalize_distance(abs(left[0] - right[0]))
    start = max(left[0], right[0])
    end = min(left[1], right[1])
    if end < start:
        return _normalize_distance(start - end)
    span = max(left[1], right[1]) - min(left[0], right[0])
    return 0.0 if span <= 0 else _normalize_distance(1 - (end - start) / span)


def _nearest_average(left, right):
    if not left or not right:
        return 1.0
    return _average(
        [min(_cosine_distance(source, target) for target in right) for source in left]
    )


def _symmetric_embedding_distance(left, right):
    if not left and not right:
        return 0.0
    if not left or not right:
        return 1.0
    return _normalize_distance(
        (_nearest_average(left, right) + _nearest_average(right, left)) / 2
    )


def _album_score(source, candidate):
    core = _normalize_distance(_cosine_distance(source["mean"], candidate["mean"]))
    if not source["poles"]:
        poles = 0.0
    elif not candidate["poles"]:
        poles = 1.0
    else:
        poles = _normalize_distance(
            _average(
                [
                    min(
                        _cosine_distance(pole["embedding"], other["embedding"])
                        for other in candidate["poles"]
                    )
                    for pole in source["poles"]
                ]
            )
        )
    spread = abs(
        source["spread"]["meanPairwiseDistance"]
        - candidate["spread"]["meanPairwiseDistance"]
    )
    if (
        source["spread"]["meanPairwiseDistance"] >= 0.18
        and candidate["spread"]["meanPairwiseDistance"]
        < source["spread"]["meanPairwiseDistance"] * 0.55
    ):
        spread += 0.12
    spread = _normalize_distance(spread)
    energy = _interval_distance(_interval(source["energy"]), _interval(candidate["energy"]))
    mood = (
        0.0
        if not source["mood"]["mean"] or not candidate["mood"]["mean"]
        else _normalize_distance(
            _cosine_distance(source["mood"]["mean"], candidate["mood"]["mean"])
        )
    )
    path = _normalize_distance(
        abs(
            source["path"]["meanStepDistance"]
            - candidate["path"]["meanStepDistance"]
        )
    )
    dimensions = {
        "core": core,
        "poles": poles,
        "spread": spread,
        "energy": energy,
        "mood": mood,
        "path": path,
    }
    reasons = [
        name
        for name, _distance in sorted(
            (
                (name, distance)
                for name, distance in dimensions.items()
                if distance < _REASON_THRESHOLD
            ),
            key=lambda item: (item[1], item[0]),
        )[:2]
    ]
    if core < _CORE_REASON_THRESHOLD and "core" not in reasons:
        reasons.insert(0, "core")
    return (
        sum(dimensions[name] * _ALBUM_WEIGHTS[name] for name in dimensions),
        reasons,
    )


def _mood_distance(left, right):
    if not left["mean"] or not right["mean"]:
        return 0.0
    dimensions = min(len(left["mean"]), len(right["mean"]))
    intervals = []
    for index in range(dimensions):
        left_interval = (
            (left["min"][index], left["max"][index])
            if left["q1"][index] == 0 and left["q3"][index] == 0
            else (left["q1"][index], left["q3"][index])
        )
        right_interval = (
            (right["min"][index], right["max"][index])
            if right["q1"][index] == 0 and right["q3"][index] == 0
            else (right["q1"][index], right["q3"][index])
        )
        intervals.append(_interval_distance(left_interval, right_interval))
    return _normalize_distance(
        (_cosine_distance(left["mean"], right["mean"]) + _average(intervals)) / 2
    )


def _artist_score(source, candidate):
    def pole_distance(left, right):
        if not left and not right:
            return 0.0
        if not left or not right:
            return 1.0

        def nearest(first, second):
            return _average(
                [
                    min(
                        _cosine_distance(pole["embedding"], other["embedding"])
                        + abs(pole["weight"] - other["weight"]) * 0.25
                        for other in second
                    )
                    for pole in first
                ]
            )

        return _normalize_distance((nearest(left, right) + nearest(right, left)) / 2)

    spread = _average(
        [
            abs(source["spread"][name] - candidate["spread"][name])
            for name in (
                "meanPairwiseDistance",
                "p50PairwiseDistance",
                "p90PairwiseDistance",
                "maxPairwiseDistance",
            )
        ]
    )
    if (
        source["spread"]["meanPairwiseDistance"] >= 0.18
        and candidate["spread"]["meanPairwiseDistance"]
        < source["spread"]["meanPairwiseDistance"] * 0.65
    ):
        spread += 0.12
    energy_mood = _normalize_distance(
        (
            _interval_distance(
                _interval(source["energy"]), _interval(candidate["energy"])
            )
            + _mood_distance(source["mood"], candidate["mood"])
        )
        / 2
    )
    trajectory = None
    if source["trajectory"] is not None and candidate["trajectory"] is not None:
        trajectory = _normalize_distance(
            _average(
                [
                    abs(source["trajectory"][name] - candidate["trajectory"][name])
                    for name in (
                        "meanStepDistance",
                        "p50StepDistance",
                        "maxStepDistance",
                        "netDistance",
                    )
                ]
            )
        )
    dimensions = [
        ("poles", pole_distance(source["poles"], candidate["poles"])),
        ("core", _symmetric_embedding_distance(source["core"], candidate["core"])),
        ("spread", _normalize_distance(spread)),
        (
            "outliers",
            _symmetric_embedding_distance(source["outliers"], candidate["outliers"]),
        ),
        ("energyMood", energy_mood),
        ("trajectory", trajectory),
    ]
    active = [(name, distance) for name, distance in dimensions if distance is not None]
    total_weight = sum(_ARTIST_WEIGHTS[name] for name, _distance in active) or 1.0
    score = sum(
        distance * (_ARTIST_WEIGHTS[name] / total_weight)
        for name, distance in active
    )
    reasons = [
        name
        for name, _distance in sorted(
            (
                (name, distance)
                for name, distance in active
                if distance < _REASON_THRESHOLD
            ),
            key=lambda item: (item[1], item[0]),
        )[:2]
    ]
    core = next(distance for name, distance in active if name == "core")
    if core < _CORE_REASON_THRESHOLD and "core" not in reasons:
        reasons.insert(0, "core")
    return score, list(dict.fromkeys(reasons))


def _edition_text(value):
    result = str(value or "").strip().lower()
    while _EDITION_SUFFIX.search(result):
        result = _EDITION_SUFFIX.sub("", result).strip()
    return result


def _rank_albums(source, albums):
    scored = []
    for candidate in albums:
        if (
            _edition_text(source["album"]) == _edition_text(candidate["album"])
            and _edition_text(source["artist"]) == _edition_text(candidate["artist"])
        ):
            continue
        score, reasons = _album_score(source["fingerprint"], candidate["fingerprint"])
        scored.append(
            {
                "albumKey": candidate["key"],
                "album": candidate["album"],
                "artist": candidate["artist"],
                "coverItemId": candidate["cover"],
                "score": score,
                "reasons": reasons,
                "_normalized_artist": _edition_text(candidate["artist"]),
            }
        )
    scored.sort(
        key=lambda row: (
            row["score"],
            row["artist"].casefold(),
            row["album"].casefold(),
            row["albumKey"],
        )
    )
    output = []
    selected = set()
    artist_counts = {}
    for cap in range(1, 4):
        for candidate in scored:
            if candidate["albumKey"] in selected:
                continue
            artist = candidate["_normalized_artist"]
            if artist_counts.get(artist, 0) >= cap:
                continue
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
            selected.add(candidate["albumKey"])
            output.append({key: value for key, value in candidate.items() if not key.startswith("_")})
            if len(output) >= RELATIONSHIP_LIMIT:
                return output
    return output


def _rank_artists(source, artists):
    output = []
    for candidate in artists:
        if candidate["key"] == source["key"]:
            continue
        score, reasons = _artist_score(source["fingerprint"], candidate["fingerprint"])
        output.append(
            {
                "artistKey": candidate["key"],
                "artist": candidate["artist"],
                "coverItemId": candidate["cover"],
                "score": score,
                "reasons": reasons,
            }
        )
    return sorted(
        output,
        key=lambda row: (row["score"], row["artist"].casefold(), row["artistKey"]),
    )[:RELATIONSHIP_LIMIT]


def _load_relationship_inputs(cur, source):
    catalog_instance_id = source["catalog_instance_id"]
    catalog_generation = int(source["catalog"]["generation"])
    analysis_generation = int(source["analysis"]["generation"])
    cur.execute(
        f"""
        SELECT tr.track_id, tr.album_id, al.name, al.album_artist_display,
               tr.artist_display, tr.disc_number, tr.track_number, tr.cover_art_id,
               tr.payload,
               (SELECT MIN(ar.cover_art_id)
                  FROM {t("catalog_artists")} ar
                 WHERE ar.catalog_instance_id=tr.catalog_instance_id
                   AND ar.published_generation=tr.published_generation
                   AND lower(ar.name)=lower(COALESCE(al.album_artist_display, tr.artist_display))),
               ai.scalar_payload,
               ai.musicnn_vector, ai.musicnn_dimensions
          FROM {t("catalog_tracks")} tr
          JOIN {t("track_analysis_links")} ln
            ON ln.catalog_instance_id=tr.catalog_instance_id
           AND ln.projection_generation=%s
           AND ln.provider_track_id=tr.track_id
           AND ln.status='ready'
          JOIN {t("analysis_items")} ai
            ON ai.catalog_instance_id=ln.catalog_instance_id
           AND ai.projection_generation=ln.projection_generation
           AND ai.analysis_id=ln.analysis_id
          LEFT JOIN {t("catalog_albums")} al
            ON al.catalog_instance_id=tr.catalog_instance_id
           AND al.published_generation=tr.published_generation
           AND al.album_id=tr.album_id
         WHERE tr.catalog_instance_id=%s AND tr.published_generation=%s
           AND tr.available=TRUE AND tr.analysis_eligible=TRUE
         ORDER BY lower(COALESCE(al.album_artist_display, tr.artist_display)),
                  lower(COALESCE(al.name, '')), COALESCE(tr.disc_number, 0),
                  COALESCE(tr.track_number, 9999), tr.track_id
        """,
        (analysis_generation, catalog_instance_id, catalog_generation),
    )
    rows = cur.fetchall()
    tracks = []
    for order, row in enumerate(rows):
        embedding = _vector(row[11], row[12])
        if embedding is None or len(embedding) == 0:
            continue
        scalar = _json(row[10], {}) or {}
        payload = _json(row[8], {}) or {}
        rich = payload.get("_lumae") if isinstance(payload.get("_lumae"), dict) else {}
        album = str(row[2] or rich.get("album") or "").strip()
        artist = str(row[3] or row[4] or "").strip()
        if not artist:
            continue
        tracks.append(
            {
                "id": str(row[0]),
                "album_id": str(row[1]) if row[1] else None,
                "album": album or None,
                "artist": artist,
                "order": order,
                "embedding": embedding,
                "energy": _energy(scalar.get("energy")),
                "mood": _mood_vector(scalar),
                "year": rich.get("year"),
                "track_cover": str(row[7]) if row[7] else None,
                "artist_cover": str(row[9]) if row[9] else None,
            }
        )
    return tracks


def _build_entities(tracks):
    album_groups = {}
    artist_groups = {}
    for track in tracks:
        artist_key = track["artist"].lower()
        artist_groups.setdefault(artist_key, []).append(track)
        if track["album"]:
            album_key = f"{artist_key}::{track['album'].lower()}"
            album_groups.setdefault(album_key, []).append(track)
    albums = []
    for key, rows in sorted(album_groups.items()):
        albums.append(
            {
                "key": key,
                "album": rows[0]["album"],
                "artist": rows[0]["artist"],
                "cover": rows[0]["track_cover"] or min(row["id"] for row in rows),
                "fingerprint": _album_fingerprint(key, rows),
            }
        )
    artists = []
    for key, rows in sorted(artist_groups.items()):
        artists.append(
            {
                "key": key,
                "artist": rows[0]["artist"],
                "cover": rows[0]["artist_cover"] or min(row["id"] for row in rows),
                "fingerprint": _artist_fingerprint(key, rows),
            }
        )
    return albums, artists


def _fingerprint_query_vectors(entity_type, fingerprint):
    if entity_type == "album":
        vectors = [fingerprint.get("mean")]
        vectors.extend(pole.get("embedding") for pole in fingerprint.get("poles", []))
    else:
        vectors = list(fingerprint.get("core", []))
        vectors.extend(pole.get("embedding") for pole in fingerprint.get("poles", []))
        vectors.extend(fingerprint.get("outliers", []))
    return [
        np.asarray(vector, dtype=np.float32)
        for vector in vectors
        if vector is not None and len(vector)
    ][:8]


def _ivf_candidate_track_ids(query_vectors, per_vector_n):
    """Return a bounded provider-track shortlist from AudioMuse's paged IVF."""
    try:
        from tasks import ivf_manager
    except (AttributeError, ImportError) as exc:
        raise RelationshipIndexUnavailable(
            "AudioMuse's MusicNN IVF index is not available"
        ) from exc
    if getattr(ivf_manager, "ivf_index", None) is None:
        loader = getattr(ivf_manager, "load_ivf_index_for_querying", None)
        if callable(loader):
            try:
                loader()
            except Exception as exc:
                raise RelationshipIndexUnavailable(
                    "AudioMuse's MusicNN IVF index could not be loaded"
                ) from exc
    index = getattr(ivf_manager, "ivf_index", None)
    if index is None:
        raise RelationshipIndexUnavailable(
            "AudioMuse's MusicNN IVF index is not ready"
        )
    try:
        index_size = len(index)
    except (TypeError, AttributeError):
        index_size = int(per_vector_n)
    if index_size <= 0:
        raise RelationshipIndexUnavailable(
            "AudioMuse's MusicNN IVF index is empty"
        )
    ids = ivf_manager.multi_query_ids(
        query_vectors,
        min(max(1, int(per_vector_n)), index_size),
    )
    return [str(item_id) for item_id in ids]


def _relationship_candidates(
    source,
    entity_type,
    entities_by_key,
    track_to_entity,
    candidate_lookup,
):
    query_vectors = _fingerprint_query_vectors(entity_type, source["fingerprint"])
    if not query_vectors:
        return []
    track_ids = candidate_lookup(
        query_vectors,
        RELATIONSHIP_CANDIDATE_TRACKS_PER_VECTOR,
    )
    candidate_keys = []
    seen = set()
    for track_id in track_ids:
        key = track_to_entity.get(str(track_id))
        if key is None or key == source["key"] or key in seen:
            continue
        seen.add(key)
        candidate_keys.append(key)
        if len(candidate_keys) >= RELATIONSHIP_MAX_CANDIDATE_ENTITIES:
            break
    return [entities_by_key[key] for key in candidate_keys if key in entities_by_key]


def relationship_status(db, catalog_instance_id):
    sources = resolve_catalog_source(db, catalog_instance_id=catalog_instance_id)
    if len(sources) != 1:
        raise ValueError("An explicit catalogue source is required")
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT relationship_schema_version, algorithm_version,
               source_catalog_generation, source_analysis_generation,
               result_generation, epoch, head_seq, floor_seq, status,
               album_count, artist_count, started_at, completed_at,
               last_error, updated_at
          FROM {t("relationship_state")} WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return {
            "catalog_instance_id": catalog_instance_id,
            "status": "not_initialized",
            "schema_version": RELATIONSHIP_SCHEMA_VERSION,
            "algorithm_version": RELATIONSHIP_ALGORITHM_VERSION,
        }
    return {
        "catalog_instance_id": catalog_instance_id,
        "schema_version": int(row[0]),
        "algorithm_version": int(row[1]),
        "source_catalog_generation": int(row[2]),
        "source_analysis_generation": int(row[3]),
        "generation": int(row[4]),
        "cursor": opaque_cursor(catalog_instance_id, str(row[5]), int(row[6])),
        "floor_seq": int(row[7]),
        "status": str(row[8]),
        "album_count": int(row[9]),
        "artist_count": int(row[10]),
        "started_at": _iso(row[11]) if row[11] else None,
        "completed_at": _iso(row[12]) if row[12] else None,
        "last_error": str(row[13]) if row[13] else None,
        "updated_at": _iso(row[14]) if row[14] else None,
    }


def claim_relationship_preparation(db, catalog_instance_id):
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t("relationship_state")}
           SET status='queued', last_error=NULL, started_at=now(),
               completed_at=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
           AND (status NOT IN ('queued', 'running')
                OR updated_at < now() - interval '{ENRICHMENT_STALE_HOURS} hours')
        RETURNING catalog_instance_id
        """,
        (catalog_instance_id,),
    )
    claimed = cur.fetchone() is not None
    db.commit()
    cur.close()
    return claimed


def prepare_relationships(catalog_instance_id, db=None, candidate_lookup=None):
    """Build one bounded, atomically published relationship generation."""
    db = db or get_db()
    candidate_lookup = candidate_lookup or _ivf_candidate_track_ids
    sources = resolve_catalog_source(db, catalog_instance_id=catalog_instance_id)
    if len(sources) != 1:
        raise ValueError("An explicit catalogue source is required")
    source = sources[0]
    if source["catalog"]["status"] != "complete" or source["analysis"]["status"] != "complete":
        raise CatalogScanError("Catalogue and sonic analysis must be published first")
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t("relationship_state")}
           SET status='running', started_at=COALESCE(started_at, now()),
               last_error=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    db.commit()
    try:
        tracks = _load_relationship_inputs(cur, source)
        albums, artists = _build_entities(tracks)
        albums_by_key = {row["key"]: row for row in albums}
        artists_by_key = {row["key"]: row for row in artists}
        track_to_album = {
            row["id"]: f"{row['artist'].lower()}::{row['album'].lower()}"
            for row in tracks
            if row.get("album")
        }
        track_to_artist = {row["id"]: row["artist"].lower() for row in tracks}

        cur.execute(
            f"SELECT result_generation, epoch, head_seq FROM {t('relationship_state')} "
            "WHERE catalog_instance_id=%s FOR UPDATE",
            (catalog_instance_id,),
        )
        state = cur.fetchone()
        if state is None:
            raise CatalogScanError("Relationship state is missing")
        generation = int(state[0]) + 1
        epoch = str(state[1])
        next_seq = int(state[2])
        cur.execute(
            f"SELECT entity_type, entity_id, result_fp FROM {t('relationship_results')} "
            "WHERE catalog_instance_id=%s",
            (catalog_instance_id,),
        )
        old = {(str(row[0]), str(row[1])): str(row[2]) for row in cur.fetchall()}
        current = set()
        changed = 0

        def publish(entity_type, entity, candidates):
            nonlocal changed, next_seq
            entity_id = entity["key"]
            if entity_type == "album":
                payload = {
                    "entity_type": "album",
                    "entity_id": entity_id,
                    "album": entity["album"],
                    "artist": entity["artist"],
                    "coverItemId": entity["cover"],
                    "candidates": _rank_albums(entity, candidates),
                    "algorithm_version": RELATIONSHIP_ALGORITHM_VERSION,
                }
            else:
                payload = {
                    "entity_type": "artist",
                    "entity_id": entity_id,
                    "artist": entity["artist"],
                    "coverItemId": entity["cover"],
                    "candidates": _rank_artists(entity, candidates),
                    "algorithm_version": RELATIONSHIP_ALGORITHM_VERSION,
                }
            current.add((entity_type, entity_id))
            result_fp = _fingerprint(payload)
            cur.execute(
                f"""
                INSERT INTO {t("relationship_results")}
                    (catalog_instance_id, entity_type, entity_id, result_generation,
                     result_fp, payload, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, now())
                ON CONFLICT (catalog_instance_id, entity_type, entity_id) DO UPDATE SET
                    result_generation=EXCLUDED.result_generation,
                    result_fp=EXCLUDED.result_fp,
                    payload=EXCLUDED.payload,
                    computed_at=EXCLUDED.computed_at
                """,
                (
                    catalog_instance_id,
                    entity_type,
                    entity_id,
                    generation,
                    result_fp,
                    canonical_json(payload),
                ),
            )
            if old.get((entity_type, entity_id)) == result_fp:
                return
            next_seq += 1
            changed += 1
            cur.execute(
                f"""
                INSERT INTO {t("relationship_changes")}
                    (catalog_instance_id, epoch, seq, generation, entity_type,
                     entity_id, operation, payload)
                VALUES (%s, %s, %s, %s, %s, %s, 'upsert', %s::jsonb)
                """,
                (
                    catalog_instance_id,
                    epoch,
                    next_seq,
                    generation,
                    entity_type,
                    entity_id,
                    canonical_json(payload),
                ),
            )

        for album in albums:
            candidates = _relationship_candidates(
                album,
                "album",
                albums_by_key,
                track_to_album,
                candidate_lookup,
            )
            publish("album", album, candidates)
        for artist in artists:
            candidates = _relationship_candidates(
                artist,
                "artist",
                artists_by_key,
                track_to_artist,
                candidate_lookup,
            )
            publish("artist", artist, candidates)

        latest_sources = resolve_catalog_source(
            db, catalog_instance_id=catalog_instance_id
        )
        if len(latest_sources) != 1:
            raise CatalogScanError("Catalogue identity changed during relationship preparation")
        latest = latest_sources[0]
        if (
            int(latest["catalog"]["generation"])
            != int(source["catalog"]["generation"])
            or int(latest["analysis"]["generation"])
            != int(source["analysis"]["generation"])
        ):
            raise CatalogScanError(
                "Catalogue or analysis generation changed during relationship preparation"
            )

        for entity_type, entity_id in sorted(set(old) - current):
            next_seq += 1
            changed += 1
            cur.execute(
                f"""
                INSERT INTO {t("relationship_changes")}
                    (catalog_instance_id, epoch, seq, generation, entity_type,
                     entity_id, operation)
                VALUES (%s, %s, %s, %s, %s, %s, 'delete')
                """,
                (
                    catalog_instance_id,
                    epoch,
                    next_seq,
                    generation,
                    entity_type,
                    entity_id,
                ),
            )
            cur.execute(
                f"DELETE FROM {t('relationship_results')} "
                "WHERE catalog_instance_id=%s AND entity_type=%s AND entity_id=%s",
                (catalog_instance_id, entity_type, entity_id),
            )
        cur.execute(
            f"""
            UPDATE {t("relationship_state")}
               SET relationship_schema_version=%s, algorithm_version=%s,
                   source_catalog_generation=%s, source_analysis_generation=%s,
                   result_generation=%s, head_seq=%s, status='complete',
                   album_count=%s, artist_count=%s, completed_at=now(),
                   last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (
                RELATIONSHIP_SCHEMA_VERSION,
                RELATIONSHIP_ALGORITHM_VERSION,
                int(source["catalog"]["generation"]),
                int(source["analysis"]["generation"]),
                generation,
                next_seq,
                len(albums),
                len(artists),
                catalog_instance_id,
            ),
        )
        db.commit()
        return {
            "catalog_instance_id": catalog_instance_id,
            "status": "complete",
            "generation": generation,
            "album_count": len(albums),
            "artist_count": len(artists),
            "track_count": len(tracks),
            "changes": changed,
            "cursor": opaque_cursor(catalog_instance_id, epoch, next_seq),
        }
    except RelationshipIndexUnavailable as exc:
        db.rollback()
        waiting_cur = db.cursor()
        waiting_cur.execute(
            f"""
            UPDATE {t("relationship_state")}
               SET status='waiting_for_index', last_error=%s,
                   completed_at=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (str(exc)[:2000], catalog_instance_id),
        )
        db.commit()
        waiting_cur.close()
        return {
            "catalog_instance_id": catalog_instance_id,
            "status": "waiting_for_index",
            "reason": str(exc),
        }
    except Exception as exc:
        db.rollback()
        failure_cur = db.cursor()
        failure_cur.execute(
            f"""
            UPDATE {t("relationship_state")}
               SET status='failed', last_error=%s, completed_at=now(), updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (str(exc)[:2000], catalog_instance_id),
        )
        db.commit()
        failure_cur.close()
        raise
    finally:
        cur.close()


def relationship_bootstrap_page(
    db, catalog_instance_id, page_token=None, limit=100
):
    status = relationship_status(db, catalog_instance_id)
    # A stale, queued, running, paused, waiting, or failed replacement must not
    # hide the last atomically published generation from clients.
    if int(status.get("generation") or 0) <= 0:
        return {
            **status,
            "relationships": [],
            "has_more": False,
            "next_page_token": None,
        }
    limit = max(1, min(int(limit), 250))
    token = _decode_page_token(page_token) if page_token else None
    generation = int(status["generation"])
    cursor = parse_opaque_cursor(status["cursor"])
    after_type = str(token.get("entity_type") or "") if token else ""
    after_id = str(token.get("entity_id") or "") if token else ""
    if token and (
        token.get("catalog_instance_id") != catalog_instance_id
        or token.get("epoch") != cursor["epoch"]
        or int(token.get("generation", -1)) != generation
    ):
        raise KeyError("bootstrap_required")
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT entity_type, entity_id, payload, computed_at
          FROM {t("relationship_results")}
         WHERE catalog_instance_id=%s
           AND result_generation=%s
           AND (entity_type > %s OR (entity_type=%s AND entity_id>%s))
         ORDER BY entity_type, entity_id LIMIT %s
        """,
        (
            catalog_instance_id,
            generation,
            after_type,
            after_type,
            after_id,
            limit + 1,
        ),
    )
    rows = cur.fetchall()
    cur.close()
    # The builder publishes results and its new generation atomically, but it
    # can finish between this request's status read and results query. Never
    # let that race produce a mixed (or apparently empty) bootstrap snapshot.
    verified = relationship_status(db, catalog_instance_id)
    if (
        int(verified["generation"]) != generation
        or verified["cursor"] != status["cursor"]
    ):
        raise KeyError("bootstrap_required")
    has_more = len(rows) > limit
    rows = rows[:limit]
    relationships = [
        {**(_json(row[2], {}) or {}), "computed_at": _iso(row[3])}
        for row in rows
    ]
    next_token = None
    if has_more and rows:
        next_token = _encode_page_token(
            {
                "catalog_instance_id": catalog_instance_id,
                "epoch": cursor["epoch"],
                "generation": generation,
                "entity_type": str(rows[-1][0]),
                "entity_id": str(rows[-1][1]),
            }
        )
    return {
        **status,
        "relationships": relationships,
        "has_more": has_more,
        "next_page_token": next_token,
    }


def read_relationship_changes(db, cursor_value, catalog_instance_id=None, limit=250):
    cursor = parse_opaque_cursor(cursor_value)
    expected_id = catalog_instance_id or cursor["catalog_instance_id"]
    status = relationship_status(db, expected_id)
    head = parse_opaque_cursor(status["cursor"])
    if cursor["catalog_instance_id"] != expected_id:
        raise ValueError("Cursor belongs to another relationship source")
    if cursor["epoch"] != head["epoch"] or cursor["seq"] < status["floor_seq"]:
        raise KeyError("bootstrap_required")
    if cursor["seq"] > head["seq"]:
        raise ValueError("Cursor is ahead of the relationship head")
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT seq, generation, entity_type, entity_id, operation, payload, created_at
          FROM {t("relationship_changes")}
         WHERE catalog_instance_id=%s AND epoch=%s AND seq>%s AND seq<=%s
         ORDER BY seq LIMIT %s
        """,
        (
            expected_id,
            head["epoch"],
            cursor["seq"],
            head["seq"],
            max(1, min(int(limit), 1000)),
        ),
    )
    rows = cur.fetchall()
    cur.close()
    changes = [
        {
            "seq": int(row[0]),
            "generation": int(row[1]),
            "entity_type": str(row[2]),
            "entity_id": str(row[3]),
            "operation": str(row[4]),
            "payload": _json(row[5]),
            "created_at": _iso(row[6]),
        }
        for row in rows
    ]
    next_seq = changes[-1]["seq"] if changes else cursor["seq"]
    return {
        "schema_version": status["schema_version"],
        "algorithm_version": status["algorithm_version"],
        "catalog_instance_id": expected_id,
        "changes": changes,
        "cursor": opaque_cursor(expected_id, head["epoch"], next_seq),
        "head_cursor": status["cursor"],
        "has_more": next_seq < head["seq"],
    }
