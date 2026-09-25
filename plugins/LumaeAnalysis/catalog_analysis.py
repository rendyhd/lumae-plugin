"""Provider-occurrence to AudioMuse analysis projection.

The catalogue owns music identity.  AudioMuse canonical rows are reusable
analysis assets only, linked explicitly to every provider occurrence.
"""

from collections import defaultdict
from contextlib import contextmanager
import gc
import hashlib
import json
from operator import itemgetter
import struct

import psycopg2.errors
from psycopg2.extras import execute_values

from plugin.api import config, get_db, table

from .catalog import (
    CatalogScanError,
    canonical_json,
    change_journal_retention_limit,
    compact_change_journal,
    fingerprint,
    opaque_cursor,
    parse_opaque_cursor,
    prune_snapshot_generations,
    read_change_page,
    resolve_catalog_source,
)
from .core_compat import get_core_adapter
from .catalog_providers import ProviderCatalogBridge
from .provider_identity_guard import (
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


def _text_array(values):
    """Return a ``text[]`` literal for ``%s::text[]``.

    psycopg2 renders a Python list as ``ARRAY['a', 'b', ...]``, one parsed
    expression per element, which costs seconds for a library-sized list. One
    array literal is parsed as a single constant.
    """
    return "{%s}" % ",".join(
        '"%s"' % str(value).replace("\\", "\\\\").replace('"', '\\"')
        for value in sorted(values)
    )


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


def _active_catalog_track_ids(cur, catalog_instance_id, generation):
    cur.execute(
        f"""
        SELECT track_id
          FROM {t('catalog_tracks')}
         WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE
           AND analysis_eligible=TRUE
         ORDER BY track_id
        """,
        (catalog_instance_id, generation),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _catalog_track_details(cur, catalog_instance_id, generation, track_ids):
    """Read metadata only for provider occurrences that share an analysis ID.

    Only dedup groups with more than one occurrence can be suspect, so no other
    track's metadata is loaded. Of the payload, only the members that
    ``_recording_ids`` reads are selected; a missing member reads as None
    either way.
    """
    if not track_ids:
        return {}
    cur.execute(
        f"""
        SELECT track_id, title, artist_display, album_id, duration_ms,
               jsonb_build_object(
                   'ProviderIds', payload->'ProviderIds',
                   'providerIds', payload->'providerIds',
                   'MusicBrainzTrack', payload->'MusicBrainzTrack',
                   'MusicBrainzRecording', payload->'MusicBrainzRecording',
                   'ISRC', payload->'ISRC',
                   'isrc', payload->'isrc')
          FROM {t('catalog_tracks')}
         WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE
           AND analysis_eligible=TRUE AND track_id = ANY(%s::text[])
        """,
        (catalog_instance_id, generation, _text_array(track_ids)),
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
         WHERE server_id=%s AND provider_track_id = ANY(%s::text[])
        """,
        (server_id, _text_array(provider_track_ids)),
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


_SCALAR_KEYS = ("tempo", "key", "scale", "mood_vector", "energy", "other_features")


def _analysis_item(row, umap):
    """Build one projected item from a full ``score`` row and its vectors."""
    analysis_id = str(row[0])
    scalar = dict(zip(_SCALAR_KEYS, row[1:7]))
    xy = umap.get(analysis_id)
    return {
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


def _analysis_rows(cur, analysis_ids, umap):
    """Read full rows, including vector bytes, for new or changed items only."""
    if not analysis_ids:
        return {}
    cur.execute(
        """
        SELECT s.item_id, s.tempo, s.key, s.scale, s.mood_vector, s.energy,
               s.other_features, e.embedding, c.embedding
          FROM score s
          LEFT JOIN embedding e ON e.item_id=s.item_id
          LEFT JOIN clap_embedding c ON c.item_id=s.item_id
         WHERE s.item_id = ANY(%s::text[])
        """,
        (_text_array(analysis_ids),),
    )
    return {str(row[0]): _analysis_item(row, umap) for row in cur.fetchall()}


def _analysis_comparison(cur, catalog_instance_id, previous_generation, analysis_ids):
    """Compare current AudioMuse rows with the previous generation in SQL.

    NOTE: this trusts stored fingerprints. Any change to ``fingerprint``,
    ``_safe_payload`` or ``canonical_json`` (catalog.py), or to what an item
    fingerprints, must force a full recompute, or the old values are carried
    forward for every unchanged row. Nothing does that automatically: ship
    such a change with a migration that sets ``scalar_fp`` and ``umap_fp`` to
    NULL in the current generation, which makes every row recompute here.

    No vector bytes and, for unchanged rows, no scalar values reach Python:

    * Vectors are hashed in SQL with ``encode(sha256(v), 'hex')``, which
      equals ``_vector_fp`` (NULL for a NULL or empty vector), and compared
      with the stored fingerprints. The NULL flags tell a NULL vector from an
      empty one, which hash alike but are stored differently.
    * The stored scalar payload is ``_safe_payload`` of the previous raw
      values. When the raw columns, as JSONB, equal it, the raw values equal
      the previous ones, so their fingerprint is the stored ``scalar_fp`` (the
      sanitiser is idempotent). Otherwise the raw values are returned and
      fingerprinted in Python exactly as before.
    * The stored UMAP coordinates come back as float8, which is how Python
      parses them too, for comparison with the current map.

    ``OFFSET 0`` keeps the inner query from being flattened, so each vector is
    detoasted and hashed once.
    """
    if not analysis_ids:
        return {}
    cur.execute(
        f"""
        SELECT item_id, has_old, scalar_same,
               CASE WHEN NOT scalar_same THEN old_scalar_fp END,
               CASE WHEN NOT scalar_same THEN tempo END,
               CASE WHEN NOT scalar_same THEN key END,
               CASE WHEN NOT scalar_same THEN scale END,
               CASE WHEN NOT scalar_same THEN mood_vector END,
               CASE WHEN NOT scalar_same THEN energy END,
               CASE WHEN NOT scalar_same THEN other_features END,
               old_umap_fp, old_x, old_y, vector_nulls_same,
               musicnn_fp IS NOT DISTINCT FROM old_musicnn_fp
               AND clap_fp IS NOT DISTINCT FROM old_clap_fp
          FROM (
            SELECT s.item_id, s.tempo, s.key, s.scale, s.mood_vector, s.energy,
                   s.other_features,
                   old.analysis_id IS NOT NULL AS has_old,
                   COALESCE(old.scalar_fp IS NOT NULL
                            AND old.scalar_payload - 'umap' = jsonb_build_object(
                                'tempo', s.tempo, 'key', s.key, 'scale', s.scale,
                                'mood_vector', s.mood_vector, 'energy', s.energy,
                                'other_features', s.other_features), FALSE) AS scalar_same,
                   old.scalar_fp AS old_scalar_fp,
                   old.umap_fp AS old_umap_fp,
                   CASE WHEN jsonb_typeof(old.scalar_payload->'umap'->'x')='number'
                        THEN (old.scalar_payload->'umap'->>'x')::float8 END AS old_x,
                   CASE WHEN jsonb_typeof(old.scalar_payload->'umap'->'y')='number'
                        THEN (old.scalar_payload->'umap'->>'y')::float8 END AS old_y,
                   (e.embedding IS NULL) = (old.musicnn_vector IS NULL)
                   AND (c.embedding IS NULL) = (old.clap_vector IS NULL) AS vector_nulls_same,
                   CASE WHEN octet_length(e.embedding) > 0
                        THEN encode(sha256(e.embedding), 'hex') END AS musicnn_fp,
                   CASE WHEN octet_length(c.embedding) > 0
                        THEN encode(sha256(c.embedding), 'hex') END AS clap_fp,
                   old.musicnn_fp AS old_musicnn_fp,
                   old.clap_fp AS old_clap_fp
              FROM score s
              LEFT JOIN embedding e ON e.item_id=s.item_id
              LEFT JOIN clap_embedding c ON c.item_id=s.item_id
              LEFT JOIN {t('analysis_items')} old
                ON old.catalog_instance_id=%s AND old.projection_generation=%s
               AND old.analysis_id=s.item_id
             WHERE s.item_id = ANY(%s::text[])
            OFFSET 0
          ) compared
        """,
        (catalog_instance_id, previous_generation, _text_array(analysis_ids)),
    )
    return {str(row[0]): row[1:] for row in cur.fetchall()}


def _item_state(comparison, xy):
    """Return ``(changed, rewrite)`` for one compared item.

    ``changed`` is the previous rule, a differing fingerprint tuple, and adds a
    journal entry. ``rewrite`` also covers a stored row that differs without a
    fingerprint change (a NULL vector became empty, or the reverse).
    """
    (has_old, scalar_same, old_scalar_fp, tempo, key, scale, mood_vector, energy,
     other_features, old_umap_fp, old_x, old_y, vector_nulls_same,
     vector_fps_same) = comparison
    if not has_old:
        return True, True
    if scalar_same:
        scalar_fp_same = True
    else:
        scalar = dict(
            zip(_SCALAR_KEYS, (tempo, key, scale, mood_vector, energy, other_features))
        )
        scalar_fp_same = fingerprint(scalar) == old_scalar_fp
    if not xy:
        umap_fp_same = old_umap_fp is None
    elif old_umap_fp is not None and xy[0] == old_x and xy[1] == old_y:
        umap_fp_same = True
    else:
        umap_fp_same = fingerprint(xy) == old_umap_fp
    changed = not (scalar_fp_same and umap_fp_same and vector_fps_same)
    return changed, changed or not vector_nulls_same


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


def _changed_during_projection(cur, db, what):
    """Release the analysis_state row lock and return a retryable error."""
    cur.close()
    db.rollback()
    return CatalogScanError(f"{what} changed during the analysis projection; retry it")


def _item_fps(item):
    return (item["scalar_fp"], item["umap_fp"], item["musicnn_fp"], item["clap_fp"])


def _old_item_fps(cur, catalog_instance_id, generation, analysis_ids):
    cur.execute(
        f"SELECT analysis_id, scalar_fp, umap_fp, musicnn_fp, clap_fp "
        f"FROM {t('analysis_items')} WHERE catalog_instance_id=%s "
        "AND projection_generation=%s AND analysis_id = ANY(%s::text[])",
        (catalog_instance_id, generation, _text_array(analysis_ids)),
    )
    return {str(row[0]): tuple(row[1:]) for row in cur.fetchall()}


def _old_item_ids(cur, catalog_instance_id, generation):
    cur.execute(
        f"SELECT analysis_id FROM {t('analysis_items')} "
        "WHERE catalog_instance_id=%s AND projection_generation=%s",
        (catalog_instance_id, generation),
    )
    return {str(row[0]) for row in cur.fetchall()}


_LINK_FIELDS = (
    "provider_track_id",
    "analysis_id",
    "status",
    "match_tier",
    "algorithm",
    "decision_threshold",
    "distance",
    "evidence_complete",
    "conflict_flags",
    "review_state",
)


def _old_links(cur, catalog_instance_id, generation, batch_size=5000):
    """Yield the previous generation's links as raw field tuples, in batches.

    A server-side cursor keeps only one batch in memory. ``conflict_flags``
    has few distinct values, so each distinct text is parsed once.
    """
    named = cur.connection.cursor(name="lumae_analysis_old_links")
    named.itersize = batch_size
    flags = {}
    try:
        named.execute(
            f"""
            SELECT provider_track_id, analysis_id, status, match_tier, algorithm,
                   decision_threshold, distance, evidence_complete, conflict_flags::text,
                   review_state
              FROM {t('track_analysis_links')}
             WHERE catalog_instance_id=%s AND projection_generation=%s
            """,
            (catalog_instance_id, generation),
        )
        for row in named:
            text = row[8]
            if text not in flags:
                flags[text] = _json_value(text) or []
            # A fresh list per link: callers may mutate it, as they may a
            # decoded JSONB value.
            yield (str(row[0]),) + tuple(row[1:8]) + (list(flags[text]), row[9])
    finally:
        named.close()


_link_tuple = itemgetter(*_LINK_FIELDS)


def _link_unchanged(old, new):
    """Decide link changes as the fingerprint comparison did, without hashing.

    Equal values of equal types serialize identically, so equal tuples need no
    fingerprint. Only a differing tuple is fingerprinted, which keeps the
    ``_safe_payload`` normalisation (whitespace, NaN) exactly as before.
    """
    if old == new and tuple(map(type, old)) == tuple(map(type, new)):
        return True
    return fingerprint(dict(zip(_LINK_FIELDS, old))) == fingerprint(
        dict(zip(_LINK_FIELDS, new))
    )


def _item_row(catalog_instance_id, generation, item):
    musicnn = item["musicnn_vector"]
    clap = item["clap_vector"]
    return (
        catalog_instance_id,
        generation,
        item["analysis_id"],
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
    )


def _link_row(catalog_instance_id, generation, link):
    return (
        catalog_instance_id,
        generation,
        link["provider_track_id"],
        link["analysis_id"],
        link["status"],
        link["match_tier"],
        link["algorithm"],
        link["decision_threshold"],
        link["distance"],
        link["evidence_complete"],
        canonical_json(link["conflict_flags"]),
        link["review_state"],
    )


_ITEM_COLUMNS = (
    "catalog_instance_id, projection_generation, analysis_id, scalar_fp, umap_fp, "
    "musicnn_fp, clap_fp, scalar_payload, musicnn_vector, clap_vector, "
    "musicnn_dimensions, clap_dimensions, model_metadata"
)
_LINK_COLUMNS = (
    "catalog_instance_id, projection_generation, provider_track_id, analysis_id, "
    "status, match_tier, algorithm, decision_threshold, distance, "
    "evidence_complete, conflict_flags, review_state"
)
_CHANGE_COLUMNS = (
    "catalog_instance_id, epoch, seq, generation, entity_type, entity_id, "
    "operation, payload"
)
WRITE_PAGE_SIZE = 500


ANALYZE_LOCK_TIMEOUT = "2s"


def _analyze_generation_keys(cur):
    """ANALYZE the key columns of the projection tables, never waiting long.

    Only the key columns are analyzed: vectors and payloads are never filtered
    on and are costly to sample. The caller holds the ``analysis_state`` row
    lock, so a conflicting lock (another ANALYZE, or an anti-wraparound
    autovacuum, which does not yield) must not make it wait: after
    ``ANALYZE_LOCK_TIMEOUT`` the statistics are left to autovacuum.

    Lock order: ``analysis_items``, then ``track_analysis_links``. Any other
    code that analyzes both (e.g. the provider-ID rekey) must use the same
    order.
    """
    cur.execute("SELECT current_setting('lock_timeout')")
    previous_timeout = cur.fetchone()[0]
    cur.execute("SAVEPOINT lumae_analysis_statistics")
    cur.execute("SELECT set_config('lock_timeout', %s, true)", (ANALYZE_LOCK_TIMEOUT,))
    try:
        cur.execute(
            f"ANALYZE {t('analysis_items')} "
            "(catalog_instance_id, projection_generation, analysis_id), "
            f"{t('track_analysis_links')} (catalog_instance_id, projection_generation, "
            "provider_track_id, analysis_id, status)"
        )
    except psycopg2.errors.LockNotAvailable:
        # Also reverts the lock_timeout set inside the savepoint.
        cur.execute("ROLLBACK TO SAVEPOINT lumae_analysis_statistics")
    else:
        cur.execute("SELECT set_config('lock_timeout', %s, true)", (previous_timeout,))
    cur.execute("RELEASE SAVEPOINT lumae_analysis_statistics")


def _copy_unchanged_rows(
    cur, catalog_instance_id, previous_generation, generation, rewritten_items, rewritten_links
):
    """Carry unchanged rows into the new generation, one statement per table.

    The same ``INSERT ... SELECT`` as
    ``provider_identity_rekey._copy_analysis_generation``, minus the rows that
    are rewritten or removed.
    """
    if previous_generation <= 0:
        return
    cur.execute(
        f"""
        INSERT INTO {t('analysis_items')} ({_ITEM_COLUMNS})
        SELECT catalog_instance_id, %s, analysis_id, scalar_fp, umap_fp, musicnn_fp,
               clap_fp, scalar_payload, musicnn_vector, clap_vector,
               musicnn_dimensions, clap_dimensions, model_metadata
          FROM {t('analysis_items')}
         WHERE catalog_instance_id=%s AND projection_generation=%s
           AND analysis_id <> ALL(%s::text[])
        """,
        (generation, catalog_instance_id, previous_generation, _text_array(rewritten_items)),
    )
    cur.execute(
        f"""
        INSERT INTO {t('track_analysis_links')} ({_LINK_COLUMNS})
        SELECT catalog_instance_id, %s, provider_track_id, analysis_id, status,
               match_tier, algorithm, decision_threshold, distance,
               evidence_complete, conflict_flags, review_state
          FROM {t('track_analysis_links')}
         WHERE catalog_instance_id=%s AND projection_generation=%s
           AND provider_track_id <> ALL(%s::text[])
        """,
        (generation, catalog_instance_id, previous_generation, _text_array(rewritten_links)),
    )


@contextmanager
def _cyclic_gc_paused():
    """Pause the cyclic garbage collector; reference counting still frees.

    A projection keeps a few hundred thousand small dicts, tuples and lists
    alive at once, none of them in reference cycles. Each allocation burst
    otherwise triggers full collections that rescan all of them, which cost
    about a fifth of a no-change run.
    """
    enabled = gc.isenabled()
    gc.disable()
    try:
        yield
    finally:
        if enabled:
            gc.enable()


def project_analysis(server_id=None, db=None, adapter=None):
    with _cyclic_gc_paused():
        return _project_analysis(server_id=server_id, db=db, adapter=adapter)


def _project_analysis(server_id=None, db=None, adapter=None):
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
    track_ids = _active_catalog_track_ids(cur, catalog_instance_id, catalog_generation)
    track_set = set(track_ids)
    mapped = _analysis_mapping(cur, adapter, server_id)
    mapped = {track_id: row for track_id, row in mapped.items() if track_id in track_set}
    del track_set
    umap = _projection_lookup(cur)

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

    # Compared with the previous generation in SQL: vector bytes are read
    # later, and only for new or changed items.
    comparisons = _analysis_comparison(
        cur,
        catalog_instance_id,
        previous_generation,
        {row["analysis_id"] for row in mapped.values() if row["analysis_id"]},
    )
    policy = dedup_policy()
    links = {}
    for track_id in track_ids:
        mapping = mapped.get(track_id)
        analysis_id = mapping.get("analysis_id") if mapping else None
        ready = bool(analysis_id and analysis_id in comparisons)
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
    del track_ids
    has_mapping = bool(mapped)
    del mapped

    # Group first. Only an analysis ID shared by more than one provider
    # occurrence can need Chromaprint evidence or be a suspect dedup group, so
    # only those occurrences' fingerprints and catalogue payloads are read.
    groups = defaultdict(list)
    ready_groups = defaultdict(list)
    for track_id, link in links.items():
        if link["analysis_id"]:
            groups[link["analysis_id"]].append(track_id)
            if link["status"] == "ready":
                ready_groups[link["analysis_id"]].append(track_id)
    shared = [track_id for members in groups.values() if len(members) > 1 for track_id in members]
    shared_ready = [
        track_id for members in ready_groups.values() if len(members) > 1 for track_id in members
    ]
    if getattr(adapter, "mode", None) != "v3_registry" or not has_mapping:
        chromaprints = None
    elif shared_ready and policy.get("per_link_chromaprint_evidence_available") is True:
        chromaprints = _analysis_chromaprints(cur, adapter, server_id, shared_ready) or {}
    else:
        # V3 without a shared, progressively checked group reads no evidence.
        chromaprints = {}
    _apply_progressive_evidence(links, chromaprints, policy)
    details = _catalog_track_details(cur, catalog_instance_id, catalog_generation, shared)
    if len(details) != len(shared):
        # The catalogue generation was replaced and pruned since its track IDs
        # were read. Suspect detection needs every occurrence of a group.
        raise _changed_during_projection(cur, db, "The provider catalogue")
    _apply_provider_conflicts(links, _suspect_analysis_ids(details, links, policy))
    del details

    changed_items = []
    rewritten_items = set()
    for analysis_id in sorted(comparisons):
        # Fingerprints are recomputed only for rows whose raw values changed;
        # the algorithm is unchanged because clients compare these values.
        changed, rewrite = _item_state(comparisons[analysis_id], umap.get(analysis_id))
        if changed:
            changed_items.append(analysis_id)
        if rewrite:
            rewritten_items.add(analysis_id)
    removed_items = sorted(
        _old_item_ids(cur, catalog_instance_id, previous_generation) - set(comparisons)
    )
    item_count = len(comparisons)
    del comparisons

    changed_links = set()
    rewritten_links = set()
    removed_links = []
    carried_links = set()
    for old in _old_links(cur, catalog_instance_id, previous_generation):
        track_id = old[0]
        link = links.get(track_id)
        if link is None:
            removed_links.append(track_id)
            continue
        carried_links.add(track_id)
        new = _link_tuple(link)
        if not _link_unchanged(old, new):
            changed_links.add(track_id)
            rewritten_links.add(track_id)
        elif old != new:
            rewritten_links.add(track_id)
    for track_id in links:
        if track_id not in carried_links:
            changed_links.add(track_id)
            rewritten_links.add(track_id)
    del carried_links
    removed_links.sort()
    # Journal order is catalogue order, as before.
    changed_links = [track_id for track_id in links if track_id in changed_links]

    # A version install or analysis finalizer may ask for a projection even
    # though its material inputs are unchanged. Do not manufacture a new
    # generation: that used to duplicate every projected row and force an
    # otherwise unnecessary quadratic relationship rebuild.
    if (
        previous_generation > 0
        and source.get("analysis", {}).get("status") == "complete"
        and not changed_items
        and not removed_items
        and not changed_links
        and not removed_links
    ):
        cur.close()
        db.commit()
        return {
            "catalog_instance_id": catalog_instance_id,
            "server_id": server_id,
            "generation": previous_generation,
            "cursor": opaque_cursor(catalog_instance_id, epoch, head_seq),
            "item_count": item_count,
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

    items = _analysis_rows(cur, rewritten_items, umap)
    if set(items) != rewritten_items:
        raise _changed_during_projection(cur, db, "AudioMuse analysis")
    # Items rewritten only because a NULL vector became empty (or back) were
    # compared before this re-read. Should their values have changed since,
    # journal them from what is actually written.
    rewrite_only = rewritten_items.difference(changed_items)
    if rewrite_only:
        old_fps = _old_item_fps(cur, catalog_instance_id, previous_generation, rewrite_only)
        moved = [
            analysis_id
            for analysis_id in rewrite_only
            if old_fps.get(analysis_id) != _item_fps(items[analysis_id])
        ]
        if moved:
            changed_items = sorted(changed_items + moved)
    _copy_unchanged_rows(
        cur,
        catalog_instance_id,
        previous_generation,
        generation,
        rewritten_items.union(removed_items),
        rewritten_links.union(removed_links),
    )
    if items:
        execute_values(
            cur,
            f"INSERT INTO {t('analysis_items')} ({_ITEM_COLUMNS}) VALUES %s",
            [
                _item_row(catalog_instance_id, generation, items[analysis_id])
                for analysis_id in sorted(items)
            ],
            template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s::jsonb)",
            page_size=WRITE_PAGE_SIZE,
        )
    if rewritten_links:
        execute_values(
            cur,
            f"INSERT INTO {t('track_analysis_links')} ({_LINK_COLUMNS}) VALUES %s",
            [
                _link_row(catalog_instance_id, generation, link)
                for track_id, link in links.items()
                if track_id in rewritten_links
            ],
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
            page_size=WRITE_PAGE_SIZE,
        )

    changes = (
        [("analysis_item", analysis_id, "upsert", items[analysis_id]) for analysis_id in changed_items]
        + [("analysis_item", analysis_id, "delete", None) for analysis_id in removed_items]
        + [("analysis_link", track_id, "upsert", links[track_id]) for track_id in changed_links]
        + [("analysis_link", track_id, "delete", None) for track_id in removed_links]
    )
    next_seq = head_seq
    journal = []
    for entity_type, entity_id, operation, payload in changes:
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
        journal.append(
            (
                catalog_instance_id,
                epoch,
                next_seq,
                generation,
                entity_type,
                entity_id,
                operation,
                canonical_json(public_payload) if public_payload is not None else None,
            )
        )
    if journal:
        execute_values(
            cur,
            f"INSERT INTO {t('analysis_changes')} ({_CHANGE_COLUMNS}) VALUES %s",
            journal,
            template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
            page_size=WRITE_PAGE_SIZE,
        )
    cur.execute(
        f"""
        UPDATE {t('analysis_state')}
           SET projection_generation=%s, analysis_head_seq=%s, status='complete',
               item_count=%s, mapped_track_count=%s, completed_at=now(),
               last_error=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (generation, next_seq, item_count, len(links), catalog_instance_id),
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
        retention_limit=change_journal_retention_limit(item_count + len(links)),
    )
    # A new generation is invisible to the planner statistics until autovacuum
    # analyzes it. Readers that join links to items by generation (relationship
    # inputs, their digest, the next projection) then see an estimate of one row
    # and can choose a nested loop that filters instead of probing the key,
    # which is quadratic in the library size. Refresh the statistics with the
    # publication; they commit together.
    _analyze_generation_keys(cur)
    cur.close()
    db.commit()
    return {
        "catalog_instance_id": catalog_instance_id,
        "server_id": server_id,
        "generation": generation,
        "cursor": opaque_cursor(catalog_instance_id, epoch, next_seq),
        "item_count": item_count,
        "link_count": len(links),
        "ready_count": sum(link["status"] == "ready" for link in links.values()),
        "pending_count": sum(link["status"] == "pending" for link in links.values()),
        "missing_count": sum(link["status"] == "missing" for link in links.values()),
        "suspect_count": sum(_link_requires_repair(link) for link in links.values()),
        "evidence_complete_count": sum(
            link["evidence_complete"] for link in links.values()
        ),
        "changes": len(changes),
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
    cur = db.cursor()
    try:
        # State and events from one snapshot, checked for density (P1-7).
        epoch, head_seq, rows, _state = read_change_page(
            cur,
            catalog_instance_id=source["catalog_instance_id"],
            cursor=cursor,
            limit=limit,
            state_table="analysis_state",
            epoch_column="analysis_epoch",
            head_column="analysis_head_seq",
            floor_column="analysis_floor_seq",
            changes_table="analysis_changes",
            columns=(
                "seq", "generation", "entity_type", "entity_id", "operation",
                "payload", "created_at",
            ),
            ahead_message="Cursor is ahead of the analysis head",
        )
    finally:
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
        "cursor": opaque_cursor(source["catalog_instance_id"], epoch, next_seq),
        "head_cursor": opaque_cursor(source["catalog_instance_id"], epoch, head_seq),
        "has_more": next_seq < head_seq,
    }
