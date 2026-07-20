"""Provider-occurrence to AudioMuse analysis projection.

The catalogue owns music identity.  AudioMuse canonical rows are reusable
analysis assets only, linked explicitly to every provider occurrence.
"""

from collections import defaultdict
import hashlib
import json
import struct

from plugin.api import config, get_db, table

from .catalog import (
    CatalogScanError,
    canonical_json,
    fingerprint,
    opaque_cursor,
    parse_opaque_cursor,
    resolve_catalog_source,
)
from .core_compat import get_core_adapter


def t(name):
    return table(name)


def dedup_policy():
    threshold = getattr(config, "DUPLICATE_DISTANCE_THRESHOLD_COSINE", None)
    scheme = getattr(config, "CATALOGUE_ID_SCHEME_VERSION", None)
    duration_tolerance = getattr(config, "DURATION_TOLERANCE_SECONDS", None)
    chromaprint_collection = getattr(config, "CHROMAPRINT_COLLECTION_ENABLED", None)
    chromaprint_gate = getattr(config, "CHROMAPRINT_GATE_ENABLED", None)
    chromaprint_threshold = getattr(config, "CHROMAPRINT_MATCH_THRESHOLD", None)
    chromaprint_min_overlap = getattr(config, "CHROMAPRINT_MIN_OVERLAP", None)

    try:
        scheme = int(scheme) if scheme is not None else None
    except (TypeError, ValueError):
        scheme = None
    try:
        duration_tolerance = (
            float(duration_tolerance) if duration_tolerance is not None else None
        )
    except (TypeError, ValueError):
        duration_tolerance = None
    try:
        chromaprint_threshold = (
            float(chromaprint_threshold) if chromaprint_threshold is not None else None
        )
    except (TypeError, ValueError):
        chromaprint_threshold = None
    try:
        chromaprint_min_overlap = (
            int(chromaprint_min_overlap) if chromaprint_min_overlap is not None else None
        )
    except (TypeError, ValueError):
        chromaprint_min_overlap = None

    return {
        "algorithm": (
            f"audiomuse_catalogue_fp_{scheme}"
            if scheme is not None
            else ("musicnn_cosine" if threshold is not None else "unknown")
        ),
        "catalogue_id_scheme_version": scheme,
        "configured_threshold": float(threshold) if threshold is not None else None,
        "duration_tolerance_seconds": duration_tolerance,
        "folder_aware": scheme is not None and scheme >= 4,
        "chromaprint_collection_enabled": chromaprint_collection,
        "chromaprint_gate_enabled": chromaprint_gate,
        "chromaprint_match_threshold": chromaprint_threshold,
        "chromaprint_min_overlap": chromaprint_min_overlap,
        "per_link_distance_available": False,
        "per_link_chromaprint_evidence_available": False,
        "evidence_status": "configured_policy_only" if threshold is not None else "unknown",
    }


def _bytes(value):
    if value is None:
        return None
    return bytes(value)


def _vector_fp(value):
    blob = _bytes(value)
    return hashlib.sha256(blob).hexdigest() if blob else None


def _json_value(value):
    if value is None or isinstance(value, (dict, list, int, float, bool)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _projection_lookup(cur):
    cur.execute(
        "SELECT projection_data, id_map_json, embedding_dimension "
        "FROM map_projection_data WHERE index_name=%s",
        ("main_map",),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return {}
    blob, raw_ids, dimensions = row
    dimensions = int(dimensions or 0)
    if dimensions < 2:
        return {}
    ids = _json_value(raw_ids) or []
    raw = _bytes(blob)
    if len(raw) != len(ids) * dimensions * 4:
        return {}
    values = struct.unpack(f"<{len(ids) * dimensions}f", raw)
    return {
        str(item_id): (float(values[index * dimensions]), float(values[index * dimensions + 1]))
        for index, item_id in enumerate(ids)
    }


def _active_catalog_tracks(cur, catalog_instance_id, generation):
    cur.execute(
        f"""
        SELECT track_id, title, artist_display, album_id, duration_ms, payload
          FROM {t('catalog_tracks')}
         WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE
           AND analysis_eligible=TRUE
         ORDER BY track_id
        """,
        (catalog_instance_id, generation),
    )
    return {
        str(row[0]): {
            "track_id": str(row[0]),
            "title": row[1],
            "artist": row[2],
            "album_id": row[3],
            "duration_ms": int(row[4]) if row[4] is not None else None,
            "payload": _json_value(row[5]) or {},
        }
        for row in cur.fetchall()
    }


def _analysis_mapping(cur, adapter, server_id):
    sql = adapter.analysis_mapping_sql()
    params = (server_id,) if "%s" in sql else None
    cur.execute(sql, params)
    return {
        str(row[0]): {
            "provider_track_id": str(row[0]),
            "analysis_id": str(row[1]) if row[1] is not None else None,
            "match_tier": row[2] if len(row) > 2 else "direct",
        }
        for row in cur.fetchall()
    }


def _analysis_rows(cur, analysis_ids):
    if not analysis_ids:
        return {}
    cur.execute(
        """
        SELECT s.item_id, s.tempo, s.key, s.scale, s.mood_vector, s.energy,
               s.other_features, e.embedding, c.embedding
          FROM score s
          LEFT JOIN embedding e ON e.item_id=s.item_id
          LEFT JOIN clap_embedding c ON c.item_id=s.item_id
         WHERE s.item_id = ANY(%s)
        """,
        (list(analysis_ids),),
    )
    score_rows = cur.fetchall()
    umap = _projection_lookup(cur)
    result = {}
    for row in score_rows:
        analysis_id = str(row[0])
        scalar = {
            "tempo": row[1],
            "key": row[2],
            "scale": row[3],
            "mood_vector": row[4],
            "energy": row[5],
            "other_features": row[6],
        }
        xy = umap.get(analysis_id)
        result[analysis_id] = {
            "analysis_id": analysis_id,
            "scalar_payload": scalar,
            "scalar_fp": fingerprint(scalar),
            "umap": {"x": xy[0], "y": xy[1]} if xy else None,
            "umap_fp": fingerprint(xy) if xy else None,
            "musicnn_vector": _bytes(row[7]),
            "musicnn_fp": _vector_fp(row[7]),
            "clap_vector": _bytes(row[8]),
            "clap_fp": _vector_fp(row[8]),
        }
    return result


def _normalized_identity(value):
    return " ".join(str(value or "").casefold().split())


def _recording_ids(track):
    payload = track.get("payload") or {}
    provider_ids = payload.get("ProviderIds") or payload.get("providerIds") or {}
    result = set()
    for key in ("MusicBrainzTrack", "MusicBrainzRecording", "ISRC", "isrc"):
        value = provider_ids.get(key) or payload.get(key)
        if value:
            result.add(str(value).casefold())
    return result


def _suspect_analysis_ids(tracks, links, policy=None):
    policy = policy or dedup_policy()
    tolerance_seconds = policy.get("duration_tolerance_seconds")
    tolerance_ms = 3000 if tolerance_seconds is None else max(0, tolerance_seconds * 1000)
    grouped = defaultdict(list)
    for track_id, link in links.items():
        if link.get("analysis_id") and track_id in tracks:
            grouped[link["analysis_id"]].append(tracks[track_id])
    suspect = set()
    for analysis_id, occurrences in grouped.items():
        if len(occurrences) < 2:
            continue
        recording_sets = [ids for ids in map(_recording_ids, occurrences) if ids]
        recording_conflict = (
            len(recording_sets) > 1
            and not set.intersection(*recording_sets)
        )
        durations = [row["duration_ms"] for row in occurrences if row["duration_ms"] is not None]
        duration_conflict = bool(durations) and max(durations) - min(durations) > tolerance_ms
        titles = {_normalized_identity(row["title"]) for row in occurrences}
        artists = {_normalized_identity(row["artist"]) for row in occurrences if row["artist"]}
        text_conflict = len(titles) > 1 and len(artists) > 1
        if recording_conflict or duration_conflict or text_conflict:
            suspect.add(analysis_id)
    return suspect


def _old_items(cur, catalog_instance_id, generation):
    cur.execute(
        f"SELECT analysis_id, scalar_fp, umap_fp, musicnn_fp, clap_fp "
        f"FROM {t('analysis_items')} WHERE catalog_instance_id=%s AND projection_generation=%s",
        (catalog_instance_id, generation),
    )
    return {str(row[0]): tuple(row[1:]) for row in cur.fetchall()}


def _old_links(cur, catalog_instance_id, generation):
    cur.execute(
        f"""
        SELECT provider_track_id, analysis_id, status, match_tier, algorithm,
               decision_threshold, distance, evidence_complete, conflict_flags
          FROM {t('track_analysis_links')}
         WHERE catalog_instance_id=%s AND projection_generation=%s
        """,
        (catalog_instance_id, generation),
    )
    return {
        str(row[0]): fingerprint(
            {
                "analysis_id": row[1],
                "status": row[2],
                "match_tier": row[3],
                "algorithm": row[4],
                "decision_threshold": row[5],
                "distance": row[6],
                "evidence_complete": row[7],
                "conflict_flags": _json_value(row[8]) or [],
            }
        )
        for row in cur.fetchall()
    }


def project_analysis(server_id=None, db=None, adapter=None):
    db = db or get_db()
    adapter = adapter or get_core_adapter()
    server_id = server_id or adapter.active_server_id()
    sources = resolve_catalog_source(db, server_id=server_id)
    if len(sources) != 1:
        raise CatalogScanError("Analysis projection requires one explicit catalogue source")
    source = sources[0]
    if source["catalog"]["status"] != "complete":
        raise CatalogScanError("Provider catalogue must be complete before analysis projection")
    catalog_instance_id = source["catalog_instance_id"]
    catalog_generation = source["catalog"]["generation"]
    cur = db.cursor()
    tracks = _active_catalog_tracks(cur, catalog_instance_id, catalog_generation)
    mapped = _analysis_mapping(cur, adapter, server_id)
    mapped = {track_id: row for track_id, row in mapped.items() if track_id in tracks}
    analysis = _analysis_rows(
        cur, {row["analysis_id"] for row in mapped.values() if row["analysis_id"]}
    )
    policy = dedup_policy()
    links = {}
    for track_id in tracks:
        mapping = mapped.get(track_id)
        analysis_id = mapping.get("analysis_id") if mapping else None
        ready = bool(analysis_id and analysis_id in analysis)
        links[track_id] = {
            "provider_track_id": track_id,
            "analysis_id": analysis_id,
            "status": "ready" if ready else ("pending" if mapping else "missing"),
            "match_tier": mapping.get("match_tier") if mapping else None,
            "algorithm": policy["algorithm"] if mapping else None,
            "decision_threshold": policy["configured_threshold"] if mapping else None,
            "distance": None,
            "evidence_complete": False,
            "conflict_flags": [],
            "review_state": None,
        }
    for analysis_id in _suspect_analysis_ids(tracks, links, policy):
        for link in links.values():
            if link["analysis_id"] == analysis_id:
                link["status"] = "suspect"
                link["conflict_flags"] = ["provider_evidence_conflict"]
                link["review_state"] = "needs_review"

    cur.execute(
        f"SELECT projection_generation, analysis_epoch, analysis_head_seq "
        f"FROM {t('analysis_state')} WHERE catalog_instance_id=%s FOR UPDATE",
        (catalog_instance_id,),
    )
    state = cur.fetchone()
    if state is None:
        raise CatalogScanError("Analysis projection state is missing")
    previous_generation, epoch, head_seq = int(state[0]), str(state[1]), int(state[2])
    generation = previous_generation + 1
    old_items = _old_items(cur, catalog_instance_id, previous_generation)
    old_links = _old_links(cur, catalog_instance_id, previous_generation)
    item_changes = []
    for analysis_id, item in analysis.items():
        fps = (item["scalar_fp"], item["umap_fp"], item["musicnn_fp"], item["clap_fp"])
        if old_items.get(analysis_id) != fps:
            item_changes.append(("analysis_item", analysis_id, "upsert", item))
        musicnn = item["musicnn_vector"]
        clap = item["clap_vector"]
        cur.execute(
            f"""
            INSERT INTO {t('analysis_items')}
                (catalog_instance_id, projection_generation, analysis_id, scalar_fp,
                 umap_fp, musicnn_fp, clap_fp, scalar_payload, musicnn_vector,
                 clap_vector, musicnn_dimensions, clap_dimensions, model_metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                catalog_instance_id,
                generation,
                analysis_id,
                item["scalar_fp"],
                item["umap_fp"],
                item["musicnn_fp"],
                item["clap_fp"],
                canonical_json({**item["scalar_payload"], "umap": item["umap"]}),
                musicnn,
                clap,
                len(musicnn) // 4 if musicnn else None,
                len(clap) // 4 if clap else None,
                canonical_json(
                    {
                        "musicnn": {"family": "musicnn", "dimensions": len(musicnn) // 4 if musicnn else None},
                        "clap": {"family": "clap", "dimensions": len(clap) // 4 if clap else None},
                    }
                ),
            ),
        )
    for removed_id in sorted(set(old_items) - set(analysis)):
        item_changes.append(("analysis_item", removed_id, "delete", None))

    link_changes = []
    for track_id, link in links.items():
        link_fp = fingerprint(link)
        if old_links.get(track_id) != link_fp:
            link_changes.append(("analysis_link", track_id, "upsert", link))
        cur.execute(
            f"""
            INSERT INTO {t('track_analysis_links')}
                (catalog_instance_id, projection_generation, provider_track_id,
                 analysis_id, status, match_tier, algorithm, decision_threshold,
                 distance, evidence_complete, conflict_flags, review_state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            """,
            (
                catalog_instance_id,
                generation,
                track_id,
                link["analysis_id"],
                link["status"],
                link["match_tier"],
                link["algorithm"],
                link["decision_threshold"],
                link["distance"],
                link["evidence_complete"],
                canonical_json(link["conflict_flags"]),
                link["review_state"],
            ),
        )
    for removed_id in sorted(set(old_links) - set(links)):
        link_changes.append(("analysis_link", removed_id, "delete", None))

    next_seq = head_seq
    for entity_type, entity_id, operation, payload in item_changes + link_changes:
        next_seq += 1
        public_payload = payload
        if payload and entity_type == "analysis_item":
            public_payload = {
                "analysis_id": payload["analysis_id"],
                **payload["scalar_payload"],
                "umap": payload["umap"],
                "scalar_fp": payload["scalar_fp"],
                "umap_fp": payload["umap_fp"],
                "musicnn_fp": payload["musicnn_fp"],
                "clap_fp": payload["clap_fp"],
            }
        cur.execute(
            f"""
            INSERT INTO {t('analysis_changes')}
                (catalog_instance_id, epoch, seq, generation, entity_type,
                 entity_id, operation, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                catalog_instance_id,
                epoch,
                next_seq,
                generation,
                entity_type,
                entity_id,
                operation,
                canonical_json(public_payload) if public_payload is not None else None,
            ),
        )
    cur.execute(
        f"""
        UPDATE {t('analysis_state')}
           SET projection_generation=%s, analysis_head_seq=%s, status='complete',
               item_count=%s, mapped_track_count=%s, completed_at=now(),
               last_error=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (generation, next_seq, len(analysis), len(links), catalog_instance_id),
    )
    cur.close()
    db.commit()
    return {
        "catalog_instance_id": catalog_instance_id,
        "server_id": server_id,
        "generation": generation,
        "cursor": opaque_cursor(catalog_instance_id, epoch, next_seq),
        "item_count": len(analysis),
        "link_count": len(links),
        "suspect_count": sum(link["status"] == "suspect" for link in links.values()),
        "changes": len(item_changes) + len(link_changes),
    }


def scalar_batch(db, catalog_instance_id, provider_track_ids):
    ids = list(dict.fromkeys(str(value) for value in provider_track_ids))
    if len(ids) > 500:
        raise ValueError("At most 500 provider track IDs are allowed")
    source = resolve_catalog_source(db, catalog_instance_id=catalog_instance_id)[0]
    generation = source["analysis"]["generation"]
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT l.provider_track_id, l.analysis_id, l.status, l.match_tier,
               l.algorithm, l.decision_threshold, l.distance, l.evidence_complete,
               l.conflict_flags, i.scalar_payload, i.scalar_fp, i.umap_fp
          FROM {t('track_analysis_links')} l
          LEFT JOIN {t('analysis_items')} i
            ON i.catalog_instance_id=l.catalog_instance_id
           AND i.projection_generation=l.projection_generation
           AND i.analysis_id=l.analysis_id
         WHERE l.catalog_instance_id=%s AND l.projection_generation=%s
           AND l.provider_track_id = ANY(%s)
        """,
        (catalog_instance_id, generation, ids),
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "provider_track_id": str(row[0]),
            "analysis_id": str(row[1]) if row[1] else None,
            "status": row[2],
            "match_tier": row[3],
            "algorithm": row[4],
            "decision_threshold": row[5],
            "distance": row[6],
            "evidence_complete": bool(row[7]),
            "conflict_flags": _json_value(row[8]) or [],
            "analysis": _json_value(row[9]),
            "scalar_fp": row[10],
            "umap_fp": row[11],
        }
        for row in rows
    ]


def vector_batch(db, catalog_instance_id, analysis_ids, family="musicnn", generation=None):
    ids = list(dict.fromkeys(str(value) for value in analysis_ids))
    if len(ids) > 250:
        raise ValueError("At most 250 analysis IDs are allowed")
    if family not in ("musicnn", "clap"):
        raise ValueError("Unknown vector family")
    source = resolve_catalog_source(db, catalog_instance_id=catalog_instance_id)[0]
    current_generation = source["analysis"]["generation"]
    generation = int(generation) if generation is not None else current_generation
    if generation < 0 or generation > current_generation:
        raise ValueError("Unknown analysis generation")
    column = "musicnn_vector" if family == "musicnn" else "clap_vector"
    dimensions_column = "musicnn_dimensions" if family == "musicnn" else "clap_dimensions"
    checksum_column = "musicnn_fp" if family == "musicnn" else "clap_fp"
    cur = db.cursor()
    cur.execute(
        f"SELECT analysis_id, {column}, {dimensions_column}, {checksum_column} "
        f"FROM {t('analysis_items')} WHERE catalog_instance_id=%s "
        "AND projection_generation=%s AND analysis_id = ANY(%s) ORDER BY analysis_id",
        (catalog_instance_id, generation, ids),
    )
    rows = cur.fetchall()
    cur.close()
    data = bytearray()
    index = []
    for analysis_id, blob, dimensions, checksum in rows:
        vector = _bytes(blob)
        if not vector:
            continue
        if len(vector) != int(dimensions) * 4:
            raise CatalogScanError(f"Stored {family} vector has an invalid byte length")
        index.append(
            {
                "analysis_id": str(analysis_id),
                "offset": len(data),
                "byte_length": len(vector),
                "dimensions": int(dimensions),
                "checksum": checksum,
            }
        )
        data.extend(vector)
    header = canonical_json(
        {
            "format": "lumae-f32le-v1",
            "family": family,
            "generation": generation,
            "vectors": index,
        }
    ).encode("utf-8")
    return struct.pack("<I", len(header)) + header + bytes(data)


def read_analysis_changes(db, cursor_value, server_id=None, catalog_instance_id=None, limit=500):
    cursor = parse_opaque_cursor(cursor_value)
    expected_id = catalog_instance_id or cursor["catalog_instance_id"]
    sources = resolve_catalog_source(
        db, server_id=server_id, catalog_instance_id=None if server_id else expected_id
    )
    if len(sources) != 1:
        raise ValueError("An explicit server_id is required when multiple sources exist")
    source = sources[0]
    if source["catalog_instance_id"] != cursor["catalog_instance_id"]:
        raise ValueError("Cursor belongs to another analysis source")
    state = source["analysis"]
    if cursor["epoch"] != state["epoch"] or cursor["seq"] < state["floor_seq"]:
        raise KeyError("bootstrap_required")
    if cursor["seq"] > state["head_seq"]:
        raise ValueError("Cursor is ahead of the analysis head")
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT seq, generation, entity_type, entity_id, operation, payload, created_at
          FROM {t('analysis_changes')}
         WHERE catalog_instance_id=%s AND epoch=%s AND seq > %s
         ORDER BY seq LIMIT %s
        """,
        (
            source["catalog_instance_id"],
            state["epoch"],
            cursor["seq"],
            max(1, min(int(limit), 1000)),
        ),
    )
    rows = cur.fetchall()
    cur.close()
    changes = [
        {
            "seq": int(row[0]),
            "generation": int(row[1]),
            "entity_type": row[2],
            "entity_id": str(row[3]),
            "operation": row[4],
            "payload": _json_value(row[5]),
            "created_at": row[6].isoformat().replace("+00:00", "Z")
            if hasattr(row[6], "isoformat")
            else str(row[6]),
        }
        for row in rows
    ]
    next_seq = changes[-1]["seq"] if changes else cursor["seq"]
    return {
        "catalog_instance_id": source["catalog_instance_id"],
        "server_id": source["server_id"],
        "changes": changes,
        "cursor": opaque_cursor(source["catalog_instance_id"], state["epoch"], next_seq),
        "head_cursor": opaque_cursor(
            source["catalog_instance_id"], state["epoch"], state["head_seq"]
        ),
        "has_more": next_seq < state["head_seq"],
    }
