"""P2-2 (LUM-012): the incremental analysis projection against its oracle.

The pre-P2-2 projector is kept below, verbatim, as the oracle. Only its
relative imports were made absolute so it can run from this module. Both
projectors run on identical fixtures in two schemas of the same database, and
every observable output is compared:

* ``analysis_changes``: every row and column except ``created_at``, a
  ``now()`` default that differs between two runs by construction. One
  relaxation, see ``journal_view``: which ``seq`` inside its block an item
  upsert gets;
* ``analysis_state``: every column except ``completed_at`` and ``updated_at``,
  the same kind of timestamp (``completed_at`` is checked for presence);
* ``analysis_items`` and ``track_analysis_links``: every row and column of
  every retained generation, vectors included;
* relationship inputs: ``_load_relationship_inputs`` for the published
  generation, and the relationship input identity;
* the return value of ``project_analysis``.
"""

import json
import math
import sys
import types
import uuid

import numpy as np
import pytest
from psycopg2.extras import execute_values

from pg_helpers import connect, drop_schema
from test_lumae_analysis import load_plugin  # installs the plugin.api host stub

from plugins.LumaeAnalysis import catalog_analysis
from plugins.LumaeAnalysis import catalog_enrichment
from plugins.LumaeAnalysis import relationship_build
from plugins.LumaeAnalysis.core_v2 import AudioMuseV2Adapter
from plugins.LumaeAnalysis.core_v3 import AudioMuseV3Adapter


# ---------------------------------------------------------------------------
# Oracle: plugins/LumaeAnalysis/catalog_analysis.py lines 7-669 at origin/main
# 38c11a4 (before P2-2), verbatim except for the absolute imports.
# ---------------------------------------------------------------------------
_ORACLE_SOURCE = r'''
from collections import defaultdict
import hashlib
import json
import struct

from plugin.api import config, get_db, table

from plugins.LumaeAnalysis.catalog import (
    CatalogScanError,
    canonical_json,
    change_journal_retention_limit,
    compact_change_journal,
    fingerprint,
    opaque_cursor,
    parse_opaque_cursor,
    prune_snapshot_generations,
    resolve_catalog_source,
)
from plugins.LumaeAnalysis.core_compat import get_core_adapter
from plugins.LumaeAnalysis.catalog_providers import ProviderCatalogBridge
from plugins.LumaeAnalysis.provider_identity_guard import (
    ProviderIdentityTransitionPending,
    assert_analysis_projection_allowed,
)


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

    progressive_evidence = bool(
        scheme is not None
        and scheme >= 4
        and chromaprint_collection is True
        and chromaprint_gate is True
    )
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
        "per_link_chromaprint_evidence_available": progressive_evidence,
        "evidence_status": (
            "per_link_progressive"
            if progressive_evidence
            else ("configured_policy_only" if threshold is not None else "unknown")
        ),
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


def _analysis_chromaprints(cur, adapter, server_id, provider_track_ids):
    """Return v3 provider fingerprints used to qualify dedup groups.

    AudioMuse 2.6 has provider-keyed score IDs and no v3 canonical merge map,
    so its direct links do not require this evidence path.
    """
    if getattr(adapter, "mode", None) != "v3_registry" or not provider_track_ids:
        return None
    cur.execute(
        """
        SELECT provider_track_id, fingerprint
          FROM chromaprint
         WHERE server_id=%s AND provider_track_id = ANY(%s)
        """,
        (server_id, list(provider_track_ids)),
    )
    return {
        str(row[0]): _bytes(row[1])
        for row in cur.fetchall()
        if row[1] is not None
    }


def _chromaprints_agree(left, right):
    if left == right:
        return True
    from tasks.chromaprint import chromaprints_agree

    return chromaprints_agree(left, right)


REPAIR_CONFLICT_FLAGS = frozenset(
    ("chromaprint_disagreement", "provider_evidence_conflict")
)


def _link_requires_repair(link):
    return link.get("status") == "suspect" or bool(
        REPAIR_CONFLICT_FLAGS.intersection(link.get("conflict_flags") or ())
    )


def _apply_progressive_evidence(links, fingerprints, policy=None, compare=None):
    """Keep mapped sonic data usable while recording Chromaprint uncertainty."""
    policy = policy or dedup_policy()
    compare = compare or _chromaprints_agree
    candidates = defaultdict(list)
    for track_id, link in links.items():
        if link.get("status") == "ready" and link.get("analysis_id"):
            candidates[link["analysis_id"]].append(track_id)

    # V2 links are direct provider IDs. There is no v3 probabilistic merge to
    # qualify, so each available analysis row is complete evidence by design.
    if fingerprints is None:
        for track_ids in candidates.values():
            for track_id in track_ids:
                links[track_id]["evidence_complete"] = True
        return

    progressive_enabled = (
        policy.get("per_link_chromaprint_evidence_available") is True
    )
    for track_ids in candidates.values():
        # A false dedup merge is represented by N provider occurrences sharing
        # one canonical analysis ID. A singleton has no current collision group
        # and can be used while unrelated fingerprints continue backfilling.
        if len(track_ids) == 1:
            links[track_ids[0]]["evidence_complete"] = True
            continue

        if not progressive_enabled:
            for track_id in track_ids:
                links[track_id]["status"] = "pending"
                links[track_id]["conflict_flags"] = [
                    "chromaprint_validation_unavailable"
                ]
            continue

        missing = [track_id for track_id in track_ids if not fingerprints.get(track_id)]
        if missing:
            for track_id in track_ids:
                links[track_id]["conflict_flags"] = [
                    "chromaprint_evidence_pending"
                ]
                links[track_id]["review_state"] = "provisional"
            continue

        verdicts = []
        for index, left_id in enumerate(track_ids):
            for right_id in track_ids[index + 1 :]:
                verdicts.append(compare(fingerprints[left_id], fingerprints[right_id]))
        if any(verdict is False for verdict in verdicts):
            for track_id in track_ids:
                links[track_id]["conflict_flags"] = ["chromaprint_disagreement"]
                links[track_id]["review_state"] = "needs_repair"
        elif any(verdict is None for verdict in verdicts):
            for track_id in track_ids:
                links[track_id]["conflict_flags"] = [
                    "chromaprint_evidence_inconclusive"
                ]
                links[track_id]["review_state"] = "provisional"
        else:
            for track_id in track_ids:
                links[track_id]["evidence_complete"] = True


def _apply_provider_conflicts(links, analysis_ids):
    """Flag questionable shared identities without withholding their sonic assets."""
    analysis_ids = set(analysis_ids)
    for link in links.values():
        if link["analysis_id"] not in analysis_ids:
            continue
        link["evidence_complete"] = False
        link["conflict_flags"] = sorted(
            set(link["conflict_flags"] + ["provider_evidence_conflict"])
        )
        link["review_state"] = (
            "needs_repair"
            if "chromaprint_disagreement" in link["conflict_flags"]
            else "needs_review"
        )


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
               decision_threshold, distance, evidence_complete, conflict_flags,
               review_state
          FROM {t('track_analysis_links')}
         WHERE catalog_instance_id=%s AND projection_generation=%s
        """,
        (catalog_instance_id, generation),
    )
    return {
        str(row[0]): fingerprint(
            {
                "provider_track_id": str(row[0]),
                "analysis_id": row[1],
                "status": row[2],
                "match_tier": row[3],
                "algorithm": row[4],
                "decision_threshold": row[5],
                "distance": row[6],
                "evidence_complete": row[7],
                "conflict_flags": _json_value(row[8]) or [],
                "review_state": row[9],
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
    if callable(getattr(adapter, "provider_module", None)):
        try:
            assert_analysis_projection_allowed(
                db,
                ProviderCatalogBridge(core_adapter=adapter),
                server_id,
            )
        except ProviderIdentityTransitionPending as exc:
            raise CatalogScanError(str(exc)) from exc
    cur = db.cursor()
    tracks = _active_catalog_tracks(cur, catalog_instance_id, catalog_generation)
    mapped = _analysis_mapping(cur, adapter, server_id)
    mapped = {track_id: row for track_id, row in mapped.items() if track_id in tracks}
    chromaprints = _analysis_chromaprints(cur, adapter, server_id, mapped)
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
    _apply_progressive_evidence(links, chromaprints, policy)
    _apply_provider_conflicts(
        links,
        _suspect_analysis_ids(tracks, links, policy),
    )

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
    for removed_id in sorted(set(old_items) - set(analysis)):
        item_changes.append(("analysis_item", removed_id, "delete", None))

    link_changes = []
    for track_id, link in links.items():
        link_fp = fingerprint(link)
        if old_links.get(track_id) != link_fp:
            link_changes.append(("analysis_link", track_id, "upsert", link))
    for removed_id in sorted(set(old_links) - set(links)):
        link_changes.append(("analysis_link", removed_id, "delete", None))

    # A version install or analysis finalizer may ask for a projection even
    # though its material inputs are unchanged. Do not manufacture a new
    # generation: that used to duplicate every projected row and force an
    # otherwise unnecessary quadratic relationship rebuild.
    if (
        previous_generation > 0
        and source.get("analysis", {}).get("status") == "complete"
        and not item_changes
        and not link_changes
    ):
        cur.close()
        db.commit()
        return {
            "catalog_instance_id": catalog_instance_id,
            "server_id": server_id,
            "generation": previous_generation,
            "cursor": opaque_cursor(catalog_instance_id, epoch, head_seq),
            "item_count": len(analysis),
            "link_count": len(links),
            "ready_count": sum(link["status"] == "ready" for link in links.values()),
            "pending_count": sum(link["status"] == "pending" for link in links.values()),
            "missing_count": sum(link["status"] == "missing" for link in links.values()),
            "suspect_count": sum(_link_requires_repair(link) for link in links.values()),
            "evidence_complete_count": sum(
                link["evidence_complete"] for link in links.values()
            ),
            "changes": 0,
            "unchanged": True,
        }

    for analysis_id, item in analysis.items():
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

    for track_id, link in links.items():
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
    prune_snapshot_generations(cur, catalog_instance_id, "analysis", generation)
    compact_change_journal(
        cur,
        catalog_instance_id=catalog_instance_id,
        state_table="analysis_state",
        changes_table="analysis_changes",
        epoch_column="analysis_epoch",
        floor_column="analysis_floor_seq",
        epoch=epoch,
        head_seq=next_seq,
        retention_limit=change_journal_retention_limit(len(analysis) + len(links)),
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
        "ready_count": sum(link["status"] == "ready" for link in links.values()),
        "pending_count": sum(link["status"] == "pending" for link in links.values()),
        "missing_count": sum(link["status"] == "missing" for link in links.values()),
        "suspect_count": sum(_link_requires_repair(link) for link in links.values()),
        "evidence_complete_count": sum(
            link["evidence_complete"] for link in links.values()
        ),
        "changes": len(item_changes) + len(link_changes),
    }
'''
oracle = types.ModuleType("lumae_analysis_projection_oracle")
exec(compile(_ORACLE_SOURCE, "<pre-P2-2 catalog_analysis oracle>", "exec"), oracle.__dict__)


T = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
SERVER = "server-a"
HOST_SCHEMA = """
CREATE TABLE music_servers (server_id TEXT PRIMARY KEY, name TEXT);
INSERT INTO music_servers VALUES ('server-a', 'Main');
CREATE TABLE score (item_id TEXT PRIMARY KEY, title TEXT, author TEXT, album TEXT,
    album_artist TEXT, tempo REAL, key TEXT, scale TEXT, mood_vector TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT now(), energy REAL, other_features TEXT,
    year INTEGER, rating INTEGER, file_path TEXT, duration DOUBLE PRECISION);
CREATE TABLE embedding (item_id TEXT PRIMARY KEY REFERENCES score(item_id)
    ON DELETE CASCADE, embedding BYTEA);
CREATE TABLE clap_embedding (item_id TEXT PRIMARY KEY REFERENCES score(item_id)
    ON DELETE CASCADE, embedding BYTEA);
-- No foreign key to score here, so a mapping can point at an item without a
-- score row, which the projector publishes as a pending link.
CREATE TABLE track_server_map (item_id TEXT NOT NULL, server_id TEXT NOT NULL
    REFERENCES music_servers(server_id) ON DELETE CASCADE,
    provider_track_id TEXT NOT NULL, match_tier TEXT, file_path TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE UNIQUE INDEX ON track_server_map (server_id, provider_track_id);
CREATE TABLE chromaprint (server_id TEXT NOT NULL REFERENCES music_servers(server_id)
    ON DELETE CASCADE, provider_track_id TEXT NOT NULL, fingerprint BYTEA,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (server_id, provider_track_id));
CREATE TABLE map_projection_data (index_name VARCHAR(255) PRIMARY KEY,
    projection_data BYTEA NOT NULL, id_map_json TEXT NOT NULL,
    embedding_dimension INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
"""


class V3Adapter(AudioMuseV3Adapter):
    # No provider module: the provider-identity guard (an upstream ping) is
    # skipped, as in scripts/perf/proj_bench.py.
    provider_module = None


class V2Adapter(AudioMuseV2Adapter):
    provider_module = None


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _new_schema(run_plugin_migration):
    schema = f"lumae_projection_{uuid.uuid4().hex}"
    db = connect("public")
    with db.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}, public")
        cur.execute(
            "CREATE TABLE cron (name TEXT, task_type TEXT UNIQUE, "
            "cron_expr TEXT, enabled BOOLEAN)"
        )
    db.commit()
    run_plugin_migration(db)
    return schema, db


@pytest.fixture
def twin_dbs(migrated_db, run_plugin_migration):
    """Two freshly migrated schemas: one for the oracle, one for the new code."""
    schema, other = _new_schema(run_plugin_migration)
    try:
        yield migrated_db, other
    finally:
        try:
            other.rollback()
            other.close()
        finally:
            drop_schema(schema)


@pytest.fixture
def projection_config(monkeypatch):
    config = sys.modules["plugin.api"].config

    def apply(**values):
        defaults = {
            "CATALOGUE_ID_SCHEME_VERSION": 4,
            "CHROMAPRINT_COLLECTION_ENABLED": True,
            "CHROMAPRINT_GATE_ENABLED": True,
            "DUPLICATE_DISTANCE_THRESHOLD_COSINE": 0.01,
            "DURATION_TOLERANCE_SECONDS": 2,
            "CHROMAPRINT_MATCH_THRESHOLD": 0.8,
            "CHROMAPRINT_MIN_OVERLAP": 10,
        }
        defaults.update(values)
        for key, value in defaults.items():
            monkeypatch.setattr(config, key, value, raising=False)

    apply()
    # Deterministic stand-in for the host's Chromaprint comparison: True,
    # False, or None (inconclusive) depending on the fingerprint bytes.
    chromaprint = types.ModuleType("tasks.chromaprint")
    chromaprint.chromaprints_agree = lambda left, right: (
        None if left[:1] == b"?" or right[:1] == b"?" else left[:4] == right[:4]
    )
    tasks = sys.modules.get("tasks") or types.ModuleType("tasks")
    monkeypatch.setitem(sys.modules, "tasks", tasks)
    monkeypatch.setitem(sys.modules, "tasks.chromaprint", chromaprint)
    return apply


def item_id(index):
    return f"it{index:06d}"


def track_id(index):
    return f"tr{index:06d}"


def _vector(rng, size):
    return rng.standard_normal(size).astype("<f4").tobytes()


def seed(db, *, tracks=900, v2=False, seed_value=7):
    """Seed catalogue generation 1 and AudioMuse data; deterministic per seed.

    v3 layout, by track index:
    * 0-449 map one-to-one to an item;
    * 450-629 map in groups of two or three to one item, with agreeing,
      disagreeing, inconclusive and missing Chromaprints, and some groups with
      contradictory metadata (suspect): durations, titles and artists, or
      recording IDs that only a top-level ``MusicBrainzTrack`` and a
      lower-case ``providerIds`` carry;
    * 630-689 map to items that have no ``score`` row (pending); 680 and 681
      share one such item and contradict each other (a suspect pending group);
    * the rest are unmapped (missing), a few are unavailable or ineligible.
    v2 maps provider IDs directly, so catalogue track IDs are item IDs there.
    """
    rng = np.random.default_rng(seed_value)
    tid = item_id if v2 else track_id
    with db.cursor() as cur:
        cur.execute(HOST_SCHEMA)
        cur.execute(
            f"""INSERT INTO {T}catalog_sources (catalog_instance_id, current_core_server_id,
                   provider_type, server_name, is_default, rebind_status)
               VALUES (%s, %s, 'navidrome', 'Main', TRUE, 'active')""",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"""INSERT INTO {T}catalog_state (catalog_instance_id, provider_type,
                   current_core_server_id, published_generation, catalog_epoch, status)
               VALUES (%s, 'navidrome', %s, 1, 'catalog-epoch', 'complete')""",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"""INSERT INTO {T}analysis_state (catalog_instance_id, projection_generation,
                   analysis_epoch, status)
               VALUES (%s, 0, 'analysis-epoch', 'not_initialized')""",
            (SOURCE,),
        )
        catalog_rows = []
        groups = {}
        for index in range(tracks):
            group = None
            if 450 <= index < 540:
                group = 450 + (index - 450) // 3 * 3  # groups of three
            elif 540 <= index < 630:
                group = 540 + (index - 540) // 2 * 2  # pairs
            groups[index] = group
        group_numbers = {
            group: number
            for number, group in enumerate(sorted(set(groups.values()) - {None}))
        }
        for index in range(tracks):
            group = groups[index]
            member = index - group if group is not None else 0
            payload = {"Suffix": "flac", "Path": f"/music/{index}.flac"}
            title = f"Song {index // 3 if group is not None else index}"
            artist = f"Artist {index % 40}"
            duration = 180000 + (index % 7) * 1000
            if group is not None and group_numbers[group] % 7 == 4:
                # Contradictory recording IDs are this group's only conflict,
                # and only these two spellings carry them.
                if member == 0:
                    payload["MusicBrainzTrack"] = f"rec-a-{group}"
                elif member == 1:
                    payload["providerIds"] = {"MusicBrainzRecording": f"rec-b-{group}"}
            else:
                if index % 5 == 0:
                    payload["ProviderIds"] = {"MusicBrainzTrack": f"mb-{index // 3}"}
                if index % 11 == 0:
                    payload["isrc"] = f"ISRC{index % 7}"
                if group is not None and group % 4 == 0 and member == 1:
                    duration += 60000  # a contradictory duration: suspect group
                if group is not None and group % 5 == 0 and member == 1:
                    title, artist = f"Other song {index}", f"Other artist {index}"
            if index == 681:
                duration += 60000  # the pending pair 680/681 contradicts itself
            catalog_rows.append(
                (SOURCE, 1, tid(index), f"al{index // 10}", title, artist, duration,
                 json.dumps(payload), index % 97 != 13, index % 89 != 17)
            )
        execute_values(
            cur,
            f"""INSERT INTO {T}catalog_tracks (catalog_instance_id, published_generation,
                   track_id, album_id, title, artist_display, duration_ms, payload, available,
                   analysis_eligible, metadata_fp, first_seen_at, last_seen_at) VALUES %s""",
            catalog_rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, 'fp', now(), now())",
        )
        execute_values(
            cur,
            f"""INSERT INTO {T}catalog_albums (catalog_instance_id, published_generation,
                   album_id, name, album_artist_display, metadata_fp, payload,
                   first_seen_at, last_seen_at) VALUES %s""",
            [(SOURCE, 1, f"al{index}", f"Album {index}", f"Artist {index % 40}")
             for index in range(tracks // 10 + 1)],
            template="(%s, %s, %s, %s, %s, 'fp', '{}', now(), now())",
        )

        # AudioMuse rows. Pending items (630-689) have no score row.
        if v2:
            item_indexes = [index for index in range(tracks) if index < 690 and not 630 <= index < 690]
            item_indexes += list(range(tracks, tracks + 20))  # never catalogued
        else:
            item_indexes = sorted(
                {index if groups[index] is None else 100000 + groups[index]
                 for index in range(630)} | set(range(200000, 200020))
            )
        keys = ["C", "D", "E", None, "F#"]
        score_rows, musicnn_rows, clap_rows = [], [], []
        for position, index in enumerate(item_indexes):
            iid = item_id(index)
            score_rows.append((
                iid,
                float("nan") if position == 5 else float(90 + position % 60) + 0.25,
                keys[position % len(keys)],
                "major" if position % 2 else "minor",
                "rock:0.51,pop:0.32" if position % 13 else "cafe\u0301:0.4",
                0.1 + (position % 50) / 100.0,
                f"danceable:{position % 10 / 10},happy:0.4",
            ))
            if position % 17 != 3:
                musicnn_rows.append((iid, None if position % 23 == 4 else _vector(rng, 16)))
            if position % 19 != 2:
                clap_rows.append((iid, b"" if position % 29 == 6 else _vector(rng, 32)))
        execute_values(
            cur,
            "INSERT INTO score (item_id, tempo, key, scale, mood_vector, energy, other_features) VALUES %s",
            score_rows,
        )
        execute_values(cur, "INSERT INTO embedding (item_id, embedding) VALUES %s", musicnn_rows)
        execute_values(cur, "INSERT INTO clap_embedding (item_id, embedding) VALUES %s", clap_rows)

        if not v2:
            mapping, prints = [], []
            for index in range(690):
                group = groups[index]
                member = index - group if group is not None else 0
                if index in (680, 681):
                    target = item_id(300680)  # a shared item without a score row
                elif index >= 630:
                    target = item_id(300000 + index)  # no score row: pending
                elif group is None:
                    target = item_id(index)
                else:
                    target = item_id(100000 + group)
                mapping.append((target, SERVER, track_id(index),
                                "direct" if group is None else "fingerprint"))
                kind = group_numbers[group] % 6 if group is not None else 0
                if kind == 1 and member == 1:
                    continue  # one member without a fingerprint: evidence pending
                fp = b"same" if group is not None else bytes([index % 250]) * 4
                if kind == 2 and member == 1:
                    fp = b"diff"  # disagreement
                if kind == 3 and member == 0:
                    fp = b"?inc"  # inconclusive
                prints.append((SERVER, track_id(index), fp + bytes([index % 256])))
            execute_values(
                cur,
                "INSERT INTO track_server_map (item_id, server_id, provider_track_id, match_tier) VALUES %s",
                mapping,
            )
            execute_values(
                cur,
                "INSERT INTO chromaprint (server_id, provider_track_id, fingerprint) VALUES %s",
                prints,
            )
        write_umap(cur, [item_id(index) for index in item_indexes if index % 7 != 3], rng)
    db.commit()


def write_umap(cur, ids, rng):
    values = rng.random(len(ids) * 2).astype("<f4")
    values[3] = float("nan")  # sanitised to null in the payload
    cur.execute(
        "INSERT INTO map_projection_data (index_name, projection_data, id_map_json, "
        "embedding_dimension) VALUES ('main_map', %s, %s, 2)",
        (values.tobytes(), json.dumps(ids)),
    )


def mutate(db, *, v2=False):
    """A multi-row delta touching every change class the projector detects."""
    rng = np.random.default_rng(99)
    with db.cursor() as cur:
        cur.execute("SELECT item_id FROM score WHERE tempo IS NOT NULL ORDER BY item_id")
        ids = [row[0] for row in cur.fetchall()]
        # vector changes in both families, and a NULL vector that becomes
        # empty and an empty one that becomes NULL (same fingerprint,
        # different stored row), again in both families. Each on an item that
        # nothing else here touches, among the projected singletons.
        untouched = (
            "item_id BETWEEN 'it000100' AND 'it000449' AND item_id <> ALL(%s)"
        )
        out_of_scope = [item_id(index) for index in range(450)
                        if index % 97 == 13 or index % 89 == 17]
        cur.execute("UPDATE embedding SET embedding=%s WHERE item_id=%s", (_vector(rng, 16), ids[1]))
        for sql, params in (
            ("UPDATE clap_embedding SET embedding=%s WHERE item_id=(SELECT min(item_id) "
             f"FROM clap_embedding WHERE octet_length(embedding) > 0 AND {untouched})",
             (_vector(rng, 32), out_of_scope)),
            ("UPDATE embedding SET embedding='' WHERE item_id=(SELECT min(item_id) "
             f"FROM embedding WHERE embedding IS NULL AND {untouched})", (out_of_scope,)),
            ("UPDATE clap_embedding SET embedding=NULL WHERE item_id=(SELECT min(item_id) "
             f"FROM clap_embedding WHERE embedding='' AND {untouched})", (out_of_scope,)),
        ):
            cur.execute(sql, params)
            assert cur.rowcount == 1, sql
        # scalar changes: a real one, whitespace-only and NFC-only ones whose
        # sanitised fingerprints are unchanged, and a NULL
        cur.execute("UPDATE score SET energy=energy+0.5 WHERE item_id=%s", (ids[3],))
        cur.execute("UPDATE score SET key=key||' ' WHERE item_id=%s AND key IS NOT NULL", (ids[4],))
        cur.execute(
            "UPDATE score SET mood_vector=normalize(mood_vector, NFC) "
            "WHERE mood_vector LIKE 'cafe%%'"
        )
        cur.execute("UPDATE score SET scale=NULL WHERE item_id=%s", (ids[6],))
        # a removed item and a new one
        cur.execute("DELETE FROM score WHERE item_id=%s", (ids[7],))
        cur.execute(
            "INSERT INTO score (item_id, tempo, key, scale, mood_vector, energy, other_features) "
            "VALUES (%s, 101.5, 'A', 'minor', 'jazz:0.9', 0.3, 'happy:0.1')",
            (item_id(630) if v2 else item_id(300000 + 631),),
        )
        if not v2:
            # a remapped occurrence, and a fingerprint that completes evidence
            cur.execute(
                "UPDATE track_server_map SET item_id=%s WHERE provider_track_id=%s",
                (ids[8], track_id(10)),
            )
            cur.execute("SELECT provider_track_id FROM track_server_map WHERE provider_track_id "
                        "NOT IN (SELECT provider_track_id FROM chromaprint) ORDER BY 1 LIMIT 1")
            missing = cur.fetchone()
            if missing:
                cur.execute(
                    "INSERT INTO chromaprint (server_id, provider_track_id, fingerprint) "
                    "VALUES (%s, %s, 'same!')",
                    (SERVER, missing[0]),
                )
        # a catalogue track leaves the analysis scope
        cur.execute(
            f"UPDATE {T}catalog_tracks SET available=FALSE WHERE track_id=%s",
            ((item_id if v2 else track_id)(20),),
        )
        # one UMAP point moves, another moves only in y; the others keep
        # their exact coordinates
        cur.execute("SELECT projection_data FROM map_projection_data")
        values = np.frombuffer(bytes(cur.fetchone()[0]), dtype="<f4").copy()
        values[0], values[1] = 0.5, 0.25
        values[5] = 0.75
        cur.execute("UPDATE map_projection_data SET projection_data=%s", (values.tobytes(),))
    db.commit()


def one_row_delta(db):
    with db.cursor() as cur:
        cur.execute("UPDATE score SET tempo=tempo+1 WHERE item_id=(SELECT min(item_id) FROM score WHERE tempo IS NOT NULL)")
    db.commit()


# ---------------------------------------------------------------------------
# Observed outputs
# ---------------------------------------------------------------------------


def _plain(value):
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    if isinstance(value, np.ndarray):
        return value.tobytes()
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _rows(cur, sql):
    cur.execute(sql)
    return [_plain(row) for row in cur.fetchall()]


def observe(db):
    """Every output of a projection that clients or relationships can see."""
    with db.cursor() as cur:
        state = _rows(
            cur,
            f"""SELECT catalog_instance_id, analysis_schema_version, projection_generation,
                       analysis_epoch, analysis_head_seq, analysis_floor_seq, status,
                       item_count, mapped_track_count, last_error, completed_at IS NOT NULL
                  FROM {T}analysis_state""",
        )
        observed = {
            # created_at is a now() default; everything else is compared.
            "changes": _rows(
                cur,
                f"""SELECT catalog_instance_id, epoch, seq, generation, entity_type,
                           entity_id, operation, payload
                      FROM {T}analysis_changes ORDER BY epoch, seq""",
            ),
            "state": state,
            "items": _rows(
                cur,
                f"""SELECT catalog_instance_id, projection_generation, analysis_id, scalar_fp,
                           umap_fp, musicnn_fp, clap_fp, scalar_payload, musicnn_vector,
                           clap_vector, musicnn_dimensions, clap_dimensions, model_metadata
                      FROM {T}analysis_items ORDER BY projection_generation, analysis_id""",
            ),
            "links": _rows(
                cur,
                f"""SELECT catalog_instance_id, projection_generation, provider_track_id,
                           analysis_id, status, match_tier, algorithm, decision_threshold,
                           distance, evidence_complete, conflict_flags, review_state
                      FROM {T}track_analysis_links
                     ORDER BY projection_generation, provider_track_id""",
            ),
        }
        generation = state[0][2]
        if generation > 0:
            source = {
                "catalog_instance_id": SOURCE,
                "catalog": {"generation": 1},
                "analysis": {"generation": generation},
            }
            observed["relationship_inputs"] = _plain(
                catalog_enrichment._load_relationship_inputs(cur, source)
            )
            observed["relationship_identity"] = relationship_build.input_identity(cur, SOURCE)
    db.rollback()
    return observed


def project(db, implementation, adapter):
    result = implementation(SERVER, db=db, adapter=adapter)
    return result, observe(db)


def journal_view(changes):
    """The journal with the only unspecified part of the old order removed.

    The oracle emits item upserts in the output order of an unordered
    ``score ⟕ embedding ⟕ clap_embedding`` read. That order is the plan's: a
    hash join emits its NULL-extended rows last, so items without a vector
    come after the rest. The new projector orders items by ID. Within a block
    of item upserts of one generation, only which ``seq`` an item got is
    therefore dropped; the block's position, its ``seq`` range, and every
    other column of every entry are still compared. Link entries (catalogue
    order in both) and deletions (sorted in both) keep their ``seq``.
    """
    seqs = [row[2] for row in changes]
    shape = [(row[3], row[4], row[6]) for row in changes]
    entries = sorted(
        (
            [*row[:2], None, *row[3:]]
            if (row[4], row[6]) == ("analysis_item", "upsert")
            else row
            for row in changes
        ),
        key=lambda row: (row[3], row[2] if row[2] is not None else -1, row[5]),
    )
    return seqs, shape, entries


def assert_equivalent(old_db, new_db, adapter, label):
    old_result, old_seen = project(old_db, oracle.project_analysis, adapter)
    new_result, new_seen = project(new_db, catalog_analysis.project_analysis, adapter)
    assert new_result == old_result, label
    assert journal_view(new_seen["changes"]) == journal_view(old_seen["changes"]), (
        f"{label}: changes differ"
    )
    for key in old_seen:
        if key != "changes":
            assert new_seen[key] == old_seen[key], f"{label}: {key} differs"
    return old_result, old_seen


# ---------------------------------------------------------------------------
# Equivalence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "variant",
    ["v3_progressive", "v3_gate_off", "v3_lossy_threshold", "v2"],
)
def test_incremental_projection_matches_the_oracle(twin_dbs, projection_config, variant):
    old_db, new_db = twin_dbs
    v2 = variant == "v2"
    if variant == "v3_gate_off":
        projection_config(CHROMAPRINT_GATE_ENABLED=False)
    if variant == "v3_lossy_threshold":
        # REAL storage rounds this threshold, so every stored link differs
        # from the recomputed one: the oracle rewrites all links each run.
        projection_config(DUPLICATE_DISTANCE_THRESHOLD_COSINE=0.123456789)
    adapter = V2Adapter() if v2 else V3Adapter()
    for db in twin_dbs:
        seed(db, v2=v2)

    result, seen = assert_equivalent(old_db, new_db, adapter, "full")
    assert result["generation"] == 1 and result["changes"] > 500
    assert seen["relationship_inputs"]
    if not v2:
        # the fixture exercises every evidence path
        flags = {flag for row in seen["links"] for flag in row[10]}
        assert "provider_evidence_conflict" in flags
        assert flags >= (
            {"chromaprint_validation_unavailable"}
            if variant == "v3_gate_off"
            else {"chromaprint_disagreement", "chromaprint_evidence_pending",
                  "chromaprint_evidence_inconclusive"}
        )
        statuses = {row[4] for row in seen["links"]}
        assert statuses == {"ready", "pending", "missing"}
        by_track = {row[2]: row for row in seen["links"]}
        # the pending pair sharing one item is a suspect group
        for index in (680, 681):
            assert by_track[track_id(index)][4] == "pending"
            assert "provider_evidence_conflict" in by_track[track_id(index)][10]
        # recording IDs spelled only as top-level MusicBrainzTrack and
        # lower-case providerIds make a group suspect
        # (group number 11; group 462, number 4, has an ineligible member 0)
        for index in (483, 484, 485):
            assert "provider_evidence_conflict" in by_track[track_id(index)][10]

    result, _ = assert_equivalent(old_db, new_db, adapter, "no change")
    if variant == "v3_lossy_threshold":
        assert result["generation"] == 2 and not result.get("unchanged")
    else:
        assert result["unchanged"] is True

    for db in twin_dbs:
        one_row_delta(db)
    result, _ = assert_equivalent(old_db, new_db, adapter, "one-row delta")
    if variant != "v3_lossy_threshold":
        assert result["changes"] == 1

    for db in twin_dbs:
        mutate(db, v2=v2)
    result, seen = assert_equivalent(old_db, new_db, adapter, "multi-row delta")
    assert result["changes"] >= 6
    kinds = {(row[4], row[6]) for row in seen["changes"] if row[3] == seen["state"][0][2]}
    assert kinds >= {("analysis_item", "upsert"), ("analysis_item", "delete"),
                     ("analysis_link", "upsert"), ("analysis_link", "delete")}

    assert_equivalent(old_db, new_db, adapter, "no change after delta")


def test_one_item_serves_every_provider_occurrence_of_an_agreeing_group(
    migrated_db, projection_config
):
    seed(migrated_db)
    result = catalog_analysis.project_analysis(SERVER, db=migrated_db, adapter=V3Adapter())
    group = item_id(100486)  # three occurrences, agreeing fingerprints, consistent metadata
    with migrated_db.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {T}analysis_items WHERE analysis_id=%s "
            "AND projection_generation=%s",
            (group, result["generation"]),
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            f"""SELECT provider_track_id, status, evidence_complete, conflict_flags, review_state
                  FROM {T}track_analysis_links WHERE analysis_id=%s ORDER BY 1""",
            (group,),
        )
        assert cur.fetchall() == [
            (track_id(index), "ready", True, [], None) for index in (486, 487, 488)
        ]
    migrated_db.rollback()


def test_failed_generation_is_republished_even_without_changes(twin_dbs, projection_config):
    old_db, new_db = twin_dbs
    for db in twin_dbs:
        seed(db)
    assert_equivalent(old_db, new_db, V3Adapter(), "full")
    for db in twin_dbs:
        with db.cursor() as cur:
            cur.execute(f"UPDATE {T}analysis_state SET status='failed'")
        db.commit()
    result, seen = assert_equivalent(old_db, new_db, V3Adapter(), "after failure")
    assert result["generation"] == 2 and result["changes"] == 0
    assert seen["state"][0][6] == "complete"


# ---------------------------------------------------------------------------
# Work done (red on the oracle)
# ---------------------------------------------------------------------------


class CountingCursor:
    """Counts statements and the vector bytes returned to Python."""

    def __init__(self):
        self.statements = []
        self.byte_values = 0

    def factory(self):
        import psycopg2.extensions

        counter = self

        class Cursor(psycopg2.extensions.cursor):
            def execute(self, query, params=None):
                counter.statements.append(" ".join(str(query).split())[:120])
                return super().execute(query, params)

            def _count(self, rows):
                for row in rows:
                    for value in row:
                        if isinstance(value, (bytes, memoryview)):
                            counter.byte_values += len(value)
                return rows

            def fetchall(self):
                return self._count(super().fetchall())

            def fetchmany(self, size=None):
                return self._count(super().fetchmany(size) if size else super().fetchmany())

            def fetchone(self):
                row = super().fetchone()
                if row is not None:
                    self._count([row])
                return row

        return Cursor


@pytest.mark.parametrize(
    "implementation",
    [
        "new",
        pytest.param(
            "oracle",
            marks=pytest.mark.xfail(
                strict=True,
                reason="the pre-P2-2 projector reads every vector and writes row by row",
            ),
        ),
    ],
)
def test_projection_reads_and_writes_only_what_changed(migrated_db, projection_config, implementation):
    project_analysis = {
        "new": catalog_analysis.project_analysis,
        "oracle": oracle.project_analysis,
    }[implementation]
    db = migrated_db
    seed(db)
    project_analysis(SERVER, db=db, adapter=V3Adapter())
    with db.cursor() as cur:
        cur.execute("SELECT octet_length(projection_data) FROM map_projection_data")
        umap_bytes = cur.fetchone()[0]
    db.commit()
    # Shared-group members' fingerprints (5 bytes each) and the UMAP blob are
    # the only binary values a no-change run needs.
    allowance = umap_bytes + 180 * 5

    counter = CountingCursor()
    db.cursor_factory = counter.factory()
    result = project_analysis(SERVER, db=db, adapter=V3Adapter())
    assert result["unchanged"] is True
    # No change: no vector and no Chromaprint beyond the shared groups, a
    # fixed number of statements, and no catalogue payload of a singleton.
    assert len(counter.statements) <= 12, counter.statements
    assert not [sql for sql in counter.statements if sql.startswith(("INSERT", "UPDATE", "DELETE"))]
    assert counter.byte_values <= allowance

    one_row_delta(db)
    counter.statements.clear()
    counter.byte_values = 0
    result = project_analysis(SERVER, db=db, adapter=V3Adapter())
    assert result["changes"] == 1
    # One changed item: its vectors are the only ones read, and unchanged
    # rows are copied set-based, so the statement count does not grow with
    # the library.
    assert len(counter.statements) <= 30, counter.statements
    assert counter.byte_values <= allowance + 16 * 4 + 32 * 4
    db.cursor_factory = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_sql_vector_hash_equals_the_python_fingerprint(migrated_db):
    with migrated_db.cursor() as cur:
        for blob in (b"", b"\x00", bytes(range(256)) * 9, np.arange(512, dtype="<f4").tobytes()):
            cur.execute(
                "SELECT CASE WHEN octet_length(%s::bytea) > 0 "
                "THEN encode(sha256(%s::bytea), 'hex') END",
                (blob, blob),
            )
            assert cur.fetchone()[0] == catalog_analysis._vector_fp(blob)
    migrated_db.rollback()


def test_link_comparison_keeps_the_fingerprint_semantics():
    from plugins.LumaeAnalysis.catalog import fingerprint

    link = {
        "provider_track_id": "track-a",
        "analysis_id": "analysis-a",
        "status": "ready",
        "match_tier": "provider_occurrence",
        "algorithm": "audiomuse_catalogue_fp_4",
        "decision_threshold": 0.01,
        "distance": None,
        "evidence_complete": False,
        "conflict_flags": ["provider_evidence_conflict"],
        "review_state": "needs_review",
    }
    base = catalog_analysis._link_tuple(link)

    def unchanged(**changes):
        other = catalog_analysis._link_tuple({**link, **changes})
        expected = fingerprint(link) == fingerprint({**link, **changes})
        assert catalog_analysis._link_unchanged(base, other) is expected
        return expected

    assert unchanged()
    assert unchanged(analysis_id=" analysis-a ")  # sanitised alike: no change
    assert not unchanged(status="pending")
    assert not unchanged(decision_threshold=1)  # 0.01 != 1
    assert not unchanged(evidence_complete=True)
    assert not unchanged(conflict_flags=[])
    # equal values of different types serialise differently
    typed = {**link, "decision_threshold": 1.0}
    assert catalog_analysis._link_unchanged(
        catalog_analysis._link_tuple(typed),
        catalog_analysis._link_tuple({**typed, "decision_threshold": 1}),
    ) is (fingerprint(typed) == fingerprint({**typed, "decision_threshold": 1}))


def test_text_array_literal_round_trips(migrated_db):
    values = ["plain", 'quo"te', "back\\slash", "comma,brace}", " space ", "é"]
    with migrated_db.cursor() as cur:
        cur.execute("SELECT %s::text[]", (catalog_analysis._text_array(values),))
        assert cur.fetchone()[0] == sorted(values)
    migrated_db.rollback()


# ---------------------------------------------------------------------------
# Concurrent changes during a projection
# ---------------------------------------------------------------------------


def _hook(monkeypatch, name, action):
    """Run ``action`` once, just before ``catalog_analysis.<name>`` runs."""
    original = getattr(catalog_analysis, name)
    fired = []

    def wrapper(*args, **kwargs):
        if not fired:
            fired.append(True)
            action()
        return original(*args, **kwargs)

    monkeypatch.setattr(catalog_analysis, name, wrapper)
    return fired


def _commit_on(connection, sql, params=()):
    with connection.cursor() as cur:
        cur.execute(sql, params)
        assert cur.rowcount >= 1, sql
    connection.commit()


def _state_is_unlocked_and_unchanged(second_connection, generation):
    with second_connection.cursor() as cur:
        # NOWAIT fails at once if the failed projection still held the row.
        cur.execute(
            f"SELECT projection_generation FROM {T}analysis_state FOR UPDATE NOWAIT"
        )
        assert cur.fetchone()[0] == generation
    second_connection.rollback()


def test_a_changed_score_row_deleted_mid_projection_is_retried(
    migrated_db, second_connection, projection_config, monkeypatch
):
    db = migrated_db
    seed(db)
    catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())
    one_row_delta(db)
    with db.cursor() as cur:
        cur.execute("SELECT min(item_id) FROM score WHERE tempo IS NOT NULL")
        changed = cur.fetchone()[0]
    db.commit()
    # After the comparison saw the change, before the full row is re-read.
    fired = _hook(monkeypatch, "_old_item_ids", lambda: _commit_on(
        second_connection, "DELETE FROM score WHERE item_id=%s", (changed,)))

    with pytest.raises(catalog_analysis.CatalogScanError, match="retry"):
        catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())
    assert fired
    _state_is_unlocked_and_unchanged(second_connection, 1)


def test_a_catalogue_track_pruned_mid_projection_is_retried(
    migrated_db, second_connection, projection_config, monkeypatch
):
    db = migrated_db
    seed(db)
    # A shared-group member disappears after the track IDs were read, as when
    # a catalogue generation is replaced and pruned meanwhile.
    fired = _hook(monkeypatch, "_catalog_track_details", lambda: _commit_on(
        second_connection,
        f"DELETE FROM {T}catalog_tracks WHERE track_id=%s",
        (track_id(451),),
    ))
    with pytest.raises(catalog_analysis.CatalogScanError, match="retry"):
        catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())
    assert fired
    _state_is_unlocked_and_unchanged(second_connection, 0)


def test_a_rewrite_only_item_that_changes_before_its_re_read_is_journaled(
    migrated_db, second_connection, projection_config, monkeypatch
):
    db = migrated_db
    seed(db)
    catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())
    with db.cursor() as cur:
        # NULL -> empty: same fingerprints, so the row is rewritten without a
        # journal entry, unless it changes again before it is re-read.
        cur.execute(
            "UPDATE embedding SET embedding='' WHERE item_id="
            "(SELECT min(item_id) FROM embedding WHERE embedding IS NULL) RETURNING item_id"
        )
        flipped = cur.fetchone()[0]
    db.commit()
    one_row_delta(db)  # something else changes, so a generation is written
    vector = np.arange(16, dtype="<f4").tobytes()
    _hook(monkeypatch, "_analysis_rows", lambda: _commit_on(
        second_connection, "UPDATE embedding SET embedding=%s WHERE item_id=%s", (vector, flipped)))

    result = catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())
    assert result["changes"] == 2
    with db.cursor() as cur:
        cur.execute(
            f"SELECT payload->>'musicnn_fp' FROM {T}analysis_changes "
            "WHERE generation=%s AND entity_id=%s AND operation='upsert'",
            (result["generation"], flipped),
        )
        assert cur.fetchall() == [(catalog_analysis._vector_fp(vector),)]
        cur.execute(
            f"SELECT musicnn_fp, musicnn_vector FROM {T}analysis_items "
            "WHERE projection_generation=%s AND analysis_id=%s",
            (result["generation"], flipped),
        )
        assert _plain(cur.fetchone()) == [catalog_analysis._vector_fp(vector), vector]
    db.rollback()


def test_statistics_never_wait_for_a_conflicting_lock(
    migrated_db, second_connection, projection_config, monkeypatch
):
    db = migrated_db
    seed(db)
    monkeypatch.setattr(catalog_analysis, "ANALYZE_LOCK_TIMEOUT", "100ms")
    with db.cursor() as cur:
        # A regression waits for the lock; fail instead of hanging the suite.
        cur.execute("SET statement_timeout = '20s'")
    db.commit()
    with second_connection.cursor() as cur:
        # Held as ANALYZE or an anti-wraparound autovacuum would hold it.
        cur.execute(f"LOCK TABLE {T}analysis_items IN SHARE UPDATE EXCLUSIVE MODE")
    try:
        result = catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())
    finally:
        second_connection.rollback()
    assert result["generation"] == 1 and result["changes"] > 500
    with db.cursor() as cur:
        cur.execute(f"SELECT projection_generation, status FROM {T}analysis_state")
        assert cur.fetchone() == (1, "complete")
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone()[0] == "0"
    db.rollback()


def test_statistics_are_refreshed_with_the_publication(migrated_db, projection_config):
    db = migrated_db
    seed(db)
    result = catalog_analysis.project_analysis(SERVER, db=db, adapter=V3Adapter())
    with db.cursor() as cur:
        cur.execute(
            "SELECT relname, reltuples FROM pg_class WHERE relname IN (%s, %s) ORDER BY 1",
            (f"{T}analysis_items", f"{T}track_analysis_links"),
        )
        assert cur.fetchall() == [
            (f"{T}analysis_items", result["item_count"]),
            (f"{T}track_analysis_links", result["link_count"]),
        ]
    db.rollback()
