"""Exact, atomic publication of Navidrome's uniform canonical provider IDs.

The resolver deliberately uses only the last complete Lumae generation, the
deterministic Navidrome codec, and two identical provider scans. AudioMuse is
inspected only after the Lumae-owned catalogue and analysis identity have been
made safe; it can never authorize this write.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from plugin.api import table

from .provider_identity import canonicalize_navidrome_id


REKEY_REASON = "navidrome_uniform_canonical_ids_v1"
REKEY_ENTITY_TYPES = ("artist", "album", "track")


def t(name):
    return table(name)


_AUDIOMUSE_REQUIRED_ACTION_BY_HEALTH = {
    "ready": None,
    "migration_required": "run_audiomuse_provider_migration",
    "busy": "wait_for_audiomuse_work",
    "repair_required": "investigate_audiomuse_mapping",
}


def audiomuse_required_action(health):
    """Return the one operator action that matches AudioMuse's live health."""

    try:
        return _AUDIOMUSE_REQUIRED_ACTION_BY_HEALTH[health]
    except KeyError as exc:
        raise ValueError(f"Unsupported AudioMuse health: {health!r}") from exc


@dataclass(frozen=True)
class RekeyEvent:
    entity_type: str
    entity_id: str
    operation: str
    payload: dict | None
    old_entity_id: str | None = None


@dataclass(frozen=True)
class ProviderIdentityRekeyPlan:
    events: tuple[RekeyEvent, ...]
    mappings: tuple[dict, ...]
    counts: dict


def _row_fingerprints(entity_type, row):
    values = [row["metadata_fp"]]
    if entity_type == "album":
        values.append(row["artwork_fp"])
    elif entity_type == "track":
        values.extend((row["media_fp"], row["artwork_fp"]))
    return tuple(values)


def build_provider_identity_rekey_plan(previous, normalized):
    """Build a one-to-one plan; ambiguous convergence is a hard failure."""

    from .catalog import ENTITY_COLLECTIONS, ENTITY_ORDER, ENTITY_TABLES

    rekeys = []
    upserts = []
    deletions = []
    mappings = []
    unchanged = 0

    for entity_type in ENTITY_ORDER:
        id_column = ENTITY_TABLES[entity_type][1]
        target = {
            str(row[id_column]): row
            for row in normalized[ENTITY_COLLECTIONS[entity_type]]
        }
        old = previous.get(entity_type, {})
        claimed_targets = set()

        candidates = {}
        if entity_type in REKEY_ENTITY_TYPES:
            for old_id in old:
                converted = canonicalize_navidrome_id(old_id)
                if converted.recognized and converted.changed and converted.value in target:
                    candidates[old_id] = converted.value

        reverse = {}
        for old_id, new_id in candidates.items():
            reverse.setdefault(new_id, []).append(old_id)
        conflicts = {
            new_id: old_ids
            for new_id, old_ids in reverse.items()
            if len(old_ids) > 1 or (new_id in old and new_id != old_ids[0])
        }
        if conflicts:
            raise ValueError("Provider identity transform is not one-to-one")

        for old_id, old_fingerprints in old.items():
            if old_id in target:
                claimed_targets.add(old_id)
                if _row_fingerprints(entity_type, target[old_id]) != tuple(old_fingerprints):
                    upserts.append(
                        RekeyEvent(entity_type, old_id, "upsert", target[old_id])
                    )
                else:
                    unchanged += 1
                continue
            new_id = candidates.get(old_id)
            if new_id:
                if new_id in claimed_targets:
                    raise ValueError("Provider identity target is claimed more than once")
                claimed_targets.add(new_id)
                rekeys.append(
                    RekeyEvent(
                        entity_type,
                        new_id,
                        "rekey",
                        target[new_id],
                        old_entity_id=old_id,
                    )
                )
                mappings.append(
                    {
                        "entity_type": entity_type,
                        "old_id": old_id,
                        "new_id": new_id,
                    }
                )
            else:
                deletions.append(
                    RekeyEvent(entity_type, old_id, "delete", None)
                )

        for entity_id, row in target.items():
            if entity_id not in claimed_targets:
                upserts.append(RekeyEvent(entity_type, entity_id, "upsert", row))

    entity_order = {name: index for index, name in enumerate(ENTITY_ORDER)}
    rekeys.sort(key=lambda event: (entity_order[event.entity_type], event.entity_id))
    upserts.sort(key=lambda event: (entity_order[event.entity_type], event.entity_id))
    deletions.sort(key=lambda event: (-entity_order[event.entity_type], event.entity_id))
    mappings.sort(key=lambda row: (entity_order[row["entity_type"]], row["old_id"]))
    events = tuple([*rekeys, *upserts, *deletions])
    return ProviderIdentityRekeyPlan(
        events=events,
        mappings=tuple(mappings),
        counts={
            "rekey": len(rekeys),
            "unchanged": unchanged,
            # The wire contract calls every full-payload upsert in the atomic
            # range an addition, including a rare same-ID metadata refresh.
            "addition": len(upserts),
            "confirmed_removal": len(deletions),
            "conflict": 0,
        },
    )


def target_scan_fingerprint(normalized):
    from .catalog import canonical_json

    stable = {
        key: sorted(list(rows), key=canonical_json)
        for key, rows in normalized.items()
    }
    return hashlib.sha256(canonical_json(stable).encode("utf-8")).hexdigest()


def _analysis_baseline(
    cur,
    catalog_instance_id,
    catalog_generation,
    generation,
    state_counts,
    state_status,
):
    cur.execute(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {t('analysis_items')}
            WHERE catalog_instance_id=%s AND projection_generation=%s),
          (SELECT COUNT(*) FROM {t('track_analysis_links')}
            WHERE catalog_instance_id=%s AND projection_generation=%s),
          (SELECT COUNT(*)
             FROM {t('track_analysis_links')} l
             LEFT JOIN {t('analysis_items')} i
               ON i.catalog_instance_id=l.catalog_instance_id
              AND i.projection_generation=l.projection_generation
              AND i.analysis_id=l.analysis_id
            WHERE l.catalog_instance_id=%s AND l.projection_generation=%s
              AND l.analysis_id IS NOT NULL AND i.analysis_id IS NULL),
          (SELECT COUNT(*)
             FROM {t('track_analysis_links')} l
             LEFT JOIN {t('catalog_tracks')} c
               ON c.catalog_instance_id=l.catalog_instance_id
              AND c.published_generation=%s
              AND c.track_id=l.provider_track_id
              AND c.available=TRUE AND c.analysis_eligible=TRUE
            WHERE l.catalog_instance_id=%s AND l.projection_generation=%s
              AND c.track_id IS NULL)
        """,
        (
            catalog_instance_id,
            generation,
            catalog_instance_id,
            generation,
            catalog_instance_id,
            generation,
            catalog_generation,
            catalog_instance_id,
            generation,
        ),
    )
    row = cur.fetchone() or (0, 0, 0, 0)
    actual_items, actual_links, missing_items, invalid_links = (
        int(row[0]),
        int(row[1]),
        int(row[2]),
        int(row[3]),
    )
    expected_items, expected_links = state_counts
    baseline = {
        "projection_generation": int(generation),
        "expected_item_count": int(expected_items),
        "actual_item_count": actual_items,
        "expected_link_count": int(expected_links),
        "actual_link_count": actual_links,
        "orphan_link_count": missing_items,
        "out_of_catalog_link_count": invalid_links,
        "state_status": str(state_status),
    }
    baseline["integrity"] = (
        actual_items == int(expected_items)
        and actual_links == int(expected_links)
        and missing_items == 0
        and invalid_links == 0
        and (
            str(state_status) == "complete"
            or (actual_items == 0 and actual_links == 0 and int(generation) == 0)
        )
    )
    return baseline


def _exact_mapping(mappings):
    result = {}
    for mapping in mappings:
        old_id = mapping["old_id"]
        new_id = mapping["new_id"]
        existing = result.get(old_id)
        if existing is not None and existing != new_id:
            raise ValueError("One provider ID maps to different canonical targets")
        result[old_id] = new_id
    return result


def _replace_exact(value, mapping):
    if isinstance(value, str):
        return mapping.get(value, value)
    if isinstance(value, list):
        return [_replace_exact(item, mapping) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            next_key = mapping.get(key, key)
            if next_key in result:
                raise ValueError("Provider-ID rewrite collides in stored JSON")
            result[next_key] = _replace_exact(item, mapping)
        return result
    return value


def _update_by_mapping(cur, table_name, column, mappings, where_sql="", where_params=()):
    if not mappings:
        return
    old_ids = [row["old_id"] for row in mappings]
    new_ids = [row["new_id"] for row in mappings]
    cur.execute(
        f"""
        UPDATE {t(table_name)} target
           SET {column}=mapped.new_id
          FROM unnest(%s::text[], %s::text[]) AS mapped(old_id, new_id)
         WHERE target.{column}=mapped.old_id {where_sql}
        """,
        (old_ids, new_ids, *where_params),
    )


def _rekey_plugin_owned_state(cur, catalog_instance_id, mappings):
    tracks = [row for row in mappings if row["entity_type"] == "track"]
    albums = [row for row in mappings if row["entity_type"] == "album"]
    exact = _exact_mapping(mappings)
    combined = [
        {"old_id": old_id, "new_id": new_id}
        for old_id, new_id in sorted(exact.items())
    ]

    _update_by_mapping(
        cur,
        "source_profiles",
        "track_id",
        tracks,
        "AND target.catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    # The legacy table belongs to the sole pre-registry Navidrome source. Do
    # not guess ownership on a multi-source installation.
    if tracks:
        old_ids = [row["old_id"] for row in tracks]
        new_ids = [row["new_id"] for row in tracks]
        cur.execute(
            f"""
            UPDATE {t('profiles')} target
               SET track_id=mapped.new_id
              FROM unnest(%s::text[], %s::text[]) AS mapped(old_id, new_id)
             WHERE target.track_id=mapped.old_id
               AND (SELECT COUNT(*) FROM {t('catalog_sources')}
                     WHERE rebind_status='active')=1
            """,
            (old_ids, new_ids),
        )

    _update_by_mapping(cur, "collection_items", "track_id", tracks)
    _update_by_mapping(cur, "collection_items", "provider_album_id", albums)
    _update_by_mapping(cur, "collection_items", "cover_item_id", combined)

    json_tables = (
        ("collection_changes", ("seq",), "payload"),
        ("collection_mutations", ("principal", "idempotency_key"), "response_payload"),
    )
    for table_name, key_columns, payload_column in json_tables:
        cur.execute(
            f"SELECT {', '.join(key_columns)}, {payload_column} FROM {t(table_name)}"
        )
        for row in cur.fetchall():
            keys = row[: len(key_columns)]
            raw_payload = row[len(key_columns)]
            payload = raw_payload
            if isinstance(raw_payload, str):
                payload = json.loads(raw_payload)
            rewritten = _replace_exact(payload, exact)
            if rewritten != payload:
                cur.execute(
                    f"UPDATE {t(table_name)} SET {payload_column}=%s::jsonb "
                    f"WHERE {' AND '.join(f'{column}=%s' for column in key_columns)}",
                    (
                        json.dumps(rewritten, sort_keys=True, separators=(",", ":")),
                        *keys,
                    ),
                )


def _load_analysis_links(cur, catalog_instance_id, generation):
    cur.execute(
        f"""
        SELECT provider_track_id, analysis_id, status, match_tier, algorithm,
               decision_threshold, distance, evidence_complete, conflict_flags,
               review_state
          FROM {t('track_analysis_links')}
         WHERE catalog_instance_id=%s AND projection_generation=%s
         ORDER BY provider_track_id
        """,
        (catalog_instance_id, generation),
    )
    return {
        str(row[0]): {
            "provider_track_id": str(row[0]),
            "analysis_id": str(row[1]) if row[1] is not None else None,
            "status": row[2],
            "match_tier": row[3],
            "algorithm": row[4],
            "decision_threshold": row[5],
            "distance": row[6],
            "evidence_complete": bool(row[7]),
            "conflict_flags": row[8] if isinstance(row[8], list) else [],
            "review_state": row[9],
        }
        for row in cur.fetchall()
    }


def _copy_analysis_generation(
    cur,
    catalog_instance_id,
    previous_generation,
    next_generation,
    epoch,
    head_seq,
    track_mapping,
    target_track_ids,
):
    cur.execute(
        f"""
        INSERT INTO {t('analysis_items')}
            (catalog_instance_id, projection_generation, analysis_id, scalar_fp,
             umap_fp, musicnn_fp, clap_fp, scalar_payload, musicnn_vector,
             clap_vector, musicnn_dimensions, clap_dimensions, model_metadata)
        SELECT catalog_instance_id, %s, analysis_id, scalar_fp, umap_fp,
               musicnn_fp, clap_fp, scalar_payload, musicnn_vector, clap_vector,
               musicnn_dimensions, clap_dimensions, model_metadata
          FROM {t('analysis_items')}
         WHERE catalog_instance_id=%s AND projection_generation=%s
        """,
        (next_generation, catalog_instance_id, previous_generation),
    )
    old_links = _load_analysis_links(cur, catalog_instance_id, previous_generation)
    new_links = {}
    for old_id, link in old_links.items():
        new_id = track_mapping.get(old_id, old_id)
        if new_id not in target_track_ids:
            continue
        if new_id in new_links:
            raise ValueError("Analysis links collide after provider-ID rekey")
        new_links[new_id] = {**link, "provider_track_id": new_id}

    sql = f"""
        INSERT INTO {t('track_analysis_links')}
            (catalog_instance_id, projection_generation, provider_track_id,
             analysis_id, status, match_tier, algorithm, decision_threshold,
             distance, evidence_complete, conflict_flags, review_state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    """
    params = [
        (
            catalog_instance_id,
            next_generation,
            track_id,
            link["analysis_id"],
            link["status"],
            link["match_tier"],
            link["algorithm"],
            link["decision_threshold"],
            link["distance"],
            link["evidence_complete"],
            json.dumps(link["conflict_flags"], sort_keys=True, separators=(",", ":")),
            link["review_state"],
        )
        for track_id, link in sorted(new_links.items())
    ]
    if params:
        cur.executemany(sql, params)

    changes = []
    for old_id, new_id in sorted(track_mapping.items()):
        if old_id in old_links and new_id in new_links:
            changes.append(("analysis_link", new_id, "upsert", new_links[new_id]))
            changes.append(("analysis_link", old_id, "delete", None))
    for old_id in sorted(set(old_links) - set(target_track_ids) - set(track_mapping)):
        changes.append(("analysis_link", old_id, "delete", None))

    next_seq = int(head_seq)
    for entity_type, entity_id, operation, payload in changes:
        next_seq += 1
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
                next_generation,
                entity_type,
                entity_id,
                operation,
                json.dumps(payload, sort_keys=True, separators=(",", ":"))
                if payload is not None
                else None,
            ),
        )
    return next_seq, new_links


def inspect_audiomuse_health(cur, adapter, server_id, expected_links):
    """Classify AudioMuse independently from Lumae's publication decision."""

    cur.execute(
        """
        SELECT COUNT(*) FROM task_status
         WHERE status NOT IN ('SUCCESS', 'FAILURE', 'REVOKED')
           AND (task_type ILIKE '%migration%'
                OR task_type IN ('main_analysis', 'cleaning'))
        """
    )
    active_tasks = int((cur.fetchone() or (0,))[0] or 0)
    if active_tasks:
        return "busy"

    expected = {
        track_id: link["analysis_id"]
        for track_id, link in expected_links.items()
        if link.get("analysis_id")
    }
    if not expected:
        return "ready"
    sql = adapter.analysis_mapping_sql()
    cur.execute(sql, (server_id,) if "%s" in sql else None)
    actual = {
        str(row[0]): str(row[1]) if row[1] is not None else None
        for row in cur.fetchall()
    }
    if any(actual.get(track_id) not in (None, analysis_id) for track_id, analysis_id in expected.items()):
        return "repair_required"
    if any(actual.get(track_id) is None for track_id in expected):
        return "migration_required"
    return "ready"


def _manifest_hash(manifest):
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _publish_provider_identity_rekey(
    db,
    *,
    catalog_instance_id,
    server_id,
    normalized,
    target_fingerprint,
    current_provider_version,
    adapter,
    scan_id=None,
    scan_duration_ms=None,
):
    """Publish catalogue, plugin state, and carried analysis in one transaction."""

    from .catalog import (
        CATALOG_FINGERPRINT_SCHEMA_VERSION,
        CATALOG_SCHEMA_VERSION,
        ENTITY_COLLECTIONS,
        ENTITY_ORDER,
        _coverage,
        _estimate_snapshot_bytes,
        _insert_generation_rows,
        _insert_relationship_rows,
        _published_fingerprints,
        canonical_json,
        catalog_scope_evidence,
        opaque_cursor,
        utc_now,
    )

    if target_scan_fingerprint(normalized) != str(target_fingerprint):
        raise ValueError("Normalized target no longer matches its stable-scan fingerprint")

    cur = db.cursor()
    cur.execute(
        f"""
        SELECT published_generation, catalog_epoch, catalog_head_seq,
               fingerprint_schema_version
          FROM {t('catalog_state')}
         WHERE catalog_instance_id=%s FOR UPDATE
        """,
        (catalog_instance_id,),
    )
    catalog_state = cur.fetchone()
    if catalog_state is None:
        raise ValueError("Catalogue state is missing")
    previous_generation = int(catalog_state[0] or 0)
    epoch = str(catalog_state[1])
    head_seq = int(catalog_state[2] or 0)

    cur.execute(
        f"""
        SELECT projection_generation, analysis_epoch, analysis_head_seq,
               item_count, mapped_track_count, status
          FROM {t('analysis_state')}
         WHERE catalog_instance_id=%s FOR UPDATE
        """,
        (catalog_instance_id,),
    )
    analysis_state = cur.fetchone()
    if analysis_state is None:
        raise ValueError("Analysis state is missing")
    previous_analysis_generation = int(analysis_state[0] or 0)

    cur.execute(
        f"""
        SELECT transition_id, state, previous_provider_version,
               current_provider_version, baseline_catalog_generation,
               baseline_analysis_generation, target_fingerprint,
               target_scan_count
          FROM {t('provider_identity_transitions')}
         WHERE catalog_instance_id=%s FOR UPDATE
        """,
        (catalog_instance_id,),
    )
    transition = cur.fetchone()
    if transition is None or str(transition[1]) != "transition_pending":
        raise ValueError("Provider identity transition is not pending")
    transition_id = str(transition[0] or "")
    if not transition_id:
        raise ValueError("Provider identity transition lacks an identity")
    if int(transition[4] or 0) != previous_generation:
        raise ValueError("Baseline catalogue generation moved during identity proof")
    if int(transition[5] or 0) != previous_analysis_generation:
        raise ValueError("Baseline analysis generation moved during identity proof")
    if str(transition[6] or "") != str(target_fingerprint):
        raise ValueError("Target catalogue fingerprint changed during identity proof")
    if int(transition[7] or 0) < 2:
        raise ValueError("Two identical target scans are required")
    if str(transition[3] or "") != str(current_provider_version or ""):
        raise ValueError("Provider version evidence changed during identity proof")

    baseline = _analysis_baseline(
        cur,
        catalog_instance_id,
        previous_generation,
        previous_analysis_generation,
        (int(analysis_state[3] or 0), int(analysis_state[4] or 0)),
        str(analysis_state[5] or "not_initialized"),
    )
    if not baseline["integrity"]:
        raise ValueError("Stored analysis baseline is incomplete or internally inconsistent")

    previous = {
        entity_type: _published_fingerprints(
            cur, entity_type, catalog_instance_id, previous_generation
        )
        for entity_type in ENTITY_ORDER
    }
    plan = build_provider_identity_rekey_plan(previous, normalized)
    if not plan.mappings:
        raise ValueError("Identity proof contains no exact provider-ID rekeys")

    next_generation = previous_generation + 1
    next_analysis_generation = previous_analysis_generation + 1
    now = utc_now()
    for entity_type in ENTITY_ORDER:
        _insert_generation_rows(
            cur,
            entity_type,
            catalog_instance_id,
            next_generation,
            normalized[ENTITY_COLLECTIONS[entity_type]],
            now,
        )
    _insert_relationship_rows(cur, catalog_instance_id, next_generation, normalized)
    _rekey_plugin_owned_state(cur, catalog_instance_id, plan.mappings)

    track_mapping = {
        row["old_id"]: row["new_id"]
        for row in plan.mappings
        if row["entity_type"] == "track"
    }
    target_track_ids = {str(row["track_id"]) for row in normalized["tracks"]}
    next_analysis_head, carried_links = _copy_analysis_generation(
        cur,
        catalog_instance_id,
        previous_analysis_generation,
        next_analysis_generation,
        str(analysis_state[1]),
        int(analysis_state[2] or 0),
        track_mapping,
        target_track_ids,
    )

    evidence = {
        "transition_id": transition_id,
        "provider_version_before": str(transition[2]) if transition[2] else None,
        "provider_version_after": str(transition[3]) if transition[3] else None,
        "deterministic": True,
        "analysis_identity_preserved": True,
    }
    next_seq = head_seq
    first_seq = head_seq + 1
    for event in plan.events:
        next_seq += 1
        cur.execute(
            f"""
            INSERT INTO {t('catalog_changes')}
                (catalog_instance_id, epoch, seq, generation, entity_type,
                 entity_id, operation, change_reason, old_entity_id, payload,
                 evidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
            """,
            (
                catalog_instance_id,
                epoch,
                next_seq,
                next_generation,
                event.entity_type,
                event.entity_id,
                event.operation,
                REKEY_REASON,
                event.old_entity_id,
                canonical_json(event.payload) if event.payload is not None else None,
                canonical_json(evidence),
            ),
        )
    last_seq = next_seq

    counts = {
        entity: len(normalized[ENTITY_COLLECTIONS[entity]]) for entity in ENTITY_ORDER
    }
    coverage = _coverage(normalized)
    scope = catalog_scope_evidence(normalized, "navidrome")
    field_support = {
        name: "observed" if value["present"] else "not_observed"
        for name, value in coverage.items()
    }
    snapshot_estimated_bytes = _estimate_snapshot_bytes(normalized)
    cur.execute(
        f"""
        UPDATE {t('catalog_state')}
           SET catalog_schema_version=%s, published_generation=%s,
               catalog_head_seq=%s, status='complete',
               fingerprint_schema_version=%s, entity_counts=%s::jsonb,
               field_support=%s::jsonb, field_coverage=%s::jsonb,
               scope_summary=%s::jsonb, snapshot_estimated_bytes=%s,
               last_scan_change_counts=%s::jsonb,
               last_scan_change_reason=%s, last_scan_duration_ms=%s,
               completed_at=now(), last_error=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (
            CATALOG_SCHEMA_VERSION,
            next_generation,
            last_seq,
            CATALOG_FINGERPRINT_SCHEMA_VERSION,
            canonical_json(counts),
            canonical_json(field_support),
            canonical_json(coverage),
            canonical_json(scope["scope_summary"]),
            snapshot_estimated_bytes,
            canonical_json(plan.counts),
            REKEY_REASON,
            int(scan_duration_ms or 0),
            catalog_instance_id,
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
        (
            next_analysis_generation,
            next_analysis_head,
            baseline["actual_item_count"],
            len(carried_links),
            catalog_instance_id,
        ),
    )
    cur.execute(
        f"UPDATE {t('catalog_sources')} SET provider_instance_fp=%s, "
        "library_scope_fp=%s, updated_at=now() WHERE catalog_instance_id=%s",
        (scope["provider_instance_fp"], scope["library_scope_fp"], catalog_instance_id),
    )
    cur.execute(
        f"""
        INSERT INTO {t('catalog_generation_pins')}
            (catalog_instance_id, published_generation, reason, transition_id)
        VALUES (%s, %s, 'provider_identity_pre_transition', %s)
        ON CONFLICT DO NOTHING
        """,
        (catalog_instance_id, previous_generation, transition_id),
    )

    audiomuse_health = inspect_audiomuse_health(cur, adapter, server_id, carried_links)
    manifest = {
        "contract": "provider_identity_rekey_v1",
        "transition_id": transition_id,
        "catalog_instance_id": catalog_instance_id,
        "baseline_catalog_generation": previous_generation,
        "published_catalog_generation": next_generation,
        "baseline_analysis_generation": previous_analysis_generation,
        "published_analysis_generation": next_analysis_generation,
        "provider_version_before": evidence["provider_version_before"],
        "provider_version_after": evidence["provider_version_after"],
        "target_fingerprint": target_fingerprint,
        "first_seq": first_seq,
        "last_seq": last_seq,
        "counts": plan.counts,
        "analysis_baseline": baseline,
        "mappings": list(plan.mappings),
    }
    manifest_sha256 = _manifest_hash(manifest)
    cur.execute(
        f"""
        INSERT INTO {t('provider_identity_manifests')}
            (transition_id, catalog_instance_id, baseline_catalog_generation,
             published_catalog_generation, baseline_analysis_generation,
             published_analysis_generation, provider_version_before,
             provider_version_after, target_fingerprint, first_seq, last_seq,
             counts, analysis_baseline, mappings, manifest_sha256)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s)
        """,
        (
            transition_id,
            catalog_instance_id,
            previous_generation,
            next_generation,
            previous_analysis_generation,
            next_analysis_generation,
            evidence["provider_version_before"],
            evidence["provider_version_after"],
            target_fingerprint,
            first_seq,
            last_seq,
            canonical_json(plan.counts),
            canonical_json(baseline),
            canonical_json(list(plan.mappings)),
            manifest_sha256,
        ),
    )
    required_action = audiomuse_required_action(audiomuse_health)
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET state='applied', required_action=%s, counts=%s::jsonb,
               first_seq=%s, last_seq=%s, analysis_baseline=%s::jsonb,
               baseline_integrity=TRUE, audiomuse_health=%s,
               manifest_sha256=%s, last_checked_provider_version=%s,
               applied_at=now(), checked_at=now(), last_error=NULL,
               updated_at=now()
         WHERE catalog_instance_id=%s AND transition_id=%s
        """,
        (
            required_action,
            canonical_json(plan.counts),
            first_seq,
            last_seq,
            canonical_json(baseline),
            audiomuse_health,
            manifest_sha256,
            current_provider_version,
            catalog_instance_id,
            transition_id,
        ),
    )
    if scan_id:
        cur.execute(
            f"""
            UPDATE {t('catalog_scans')}
               SET status='complete', completed_at=now(), progress=%s::jsonb
             WHERE scan_id=%s
            """,
            (
                canonical_json(
                    {
                        "input_counts": counts,
                        "change_counts": plan.counts,
                        "change_reason": REKEY_REASON,
                        "duration_ms": int(scan_duration_ms or 0),
                        "generation": next_generation,
                        "head_seq": last_seq,
                        "target_scan_count": 2,
                        "analysis_baseline_integrity": True,
                        "audiomuse_health": audiomuse_health,
                    }
                ),
                scan_id,
            ),
        )
    cur.close()
    db.commit()
    return {
        "catalog_instance_id": catalog_instance_id,
        "server_id": server_id,
        "generation": next_generation,
        "cursor": {"epoch": epoch, "seq": last_seq},
        "counts": counts,
        "field_coverage": coverage,
        "scope_summary": scope["scope_summary"],
        "snapshot_estimated_bytes": snapshot_estimated_bytes,
        "fingerprint_schema_version": CATALOG_FINGERPRINT_SCHEMA_VERSION,
        "change_counts": plan.counts,
        "change_reason": REKEY_REASON,
        "duration_ms": int(scan_duration_ms or 0),
        "changes": len(plan.events),
        "provider_identity_transition": {
            "state": "applied",
            "transition_id": transition_id,
            "first_seq": first_seq,
            "last_seq": last_seq,
            "counts": plan.counts,
            "manifest_sha256": manifest_sha256,
            "audiomuse_health": audiomuse_health,
        },
        "analysis_cursor": opaque_cursor(
            catalog_instance_id, str(analysis_state[1]), next_analysis_head
        ),
    }


def publish_provider_identity_rekey(db, **kwargs):
    """Rollback every partial write if any proof or publication step fails."""

    try:
        return _publish_provider_identity_rekey(db, **kwargs)
    except Exception:
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        raise


def refresh_audiomuse_health(db, catalog_instance_id, server_id, adapter, commit=True):
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT p.state, a.projection_generation
          FROM {t('provider_identity_transitions')} p
          JOIN {t('analysis_state')} a USING (catalog_instance_id)
         WHERE p.catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    state = cur.fetchone()
    if state is None or str(state[0]) != "applied":
        cur.close()
        return None
    links = _load_analysis_links(cur, catalog_instance_id, int(state[1] or 0))
    health = inspect_audiomuse_health(cur, adapter, server_id, links)
    required_action = audiomuse_required_action(health)
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET audiomuse_health=%s,
               required_action=%s,
               checked_at=now(), updated_at=now()
         WHERE catalog_instance_id=%s AND state='applied'
        """,
        (health, required_action, catalog_instance_id),
    )
    cur.close()
    if commit:
        db.commit()
    return health


def read_transition_manifest(db, *, transition_id=None, catalog_instance_id=None):
    if not transition_id and not catalog_instance_id:
        raise ValueError("A transition or catalogue identity is required")
    cur = db.cursor()
    if transition_id:
        where = "transition_id=%s"
        params = (transition_id,)
    else:
        where = "catalog_instance_id=%s"
        params = (catalog_instance_id,)
    cur.execute(
        f"""
        SELECT transition_id, catalog_instance_id, baseline_catalog_generation,
               published_catalog_generation, baseline_analysis_generation,
               published_analysis_generation, provider_version_before,
               provider_version_after, target_fingerprint, first_seq, last_seq,
               counts, analysis_baseline, mappings, manifest_sha256, created_at
          FROM {t('provider_identity_manifests')}
         WHERE {where}
         ORDER BY created_at DESC LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        raise KeyError("transition_manifest_not_found")
    return {
        "contract": "provider_identity_rekey_v1",
        "transition_id": str(row[0]),
        "catalog_instance_id": str(row[1]),
        "baseline_catalog_generation": int(row[2]),
        "published_catalog_generation": int(row[3]),
        "baseline_analysis_generation": int(row[4]),
        "published_analysis_generation": int(row[5]),
        "provider_version_before": str(row[6]) if row[6] else None,
        "provider_version_after": str(row[7]) if row[7] else None,
        "target_fingerprint": str(row[8]),
        "first_seq": int(row[9]),
        "last_seq": int(row[10]),
        "counts": row[11] if isinstance(row[11], dict) else json.loads(row[11]),
        "analysis_baseline": row[12]
        if isinstance(row[12], dict)
        else json.loads(row[12]),
        "mappings": row[13] if isinstance(row[13], list) else json.loads(row[13]),
        "manifest_sha256": str(row[14]),
        "created_at": row[15].isoformat() if hasattr(row[15], "isoformat") else row[15],
    }
