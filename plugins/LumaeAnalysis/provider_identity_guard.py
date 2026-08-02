"""Durable admission shield for Navidrome provider-ID transitions."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Iterable

from plugin.api import table

from .provider_identity import (
    ProviderIdentityTransitionState,
    canonicalize_navidrome_id,
    is_after_last_known_pre_canonical_version,
)


TRANSITION_BLOCKER = "provider_identity_transition"


def t(name):
    return table(name)


class ProviderIdentityTransitionPending(RuntimeError):
    pass


@dataclass(frozen=True)
class IdSetInspection:
    status: str
    changed_candidates: int
    matched_rekeys: int
    missing_candidates: int
    duplicate_targets: int
    old_count: int
    current_count: int

    @property
    def transition_detected(self):
        return self.status == "transition_detected"

    @property
    def blocked(self):
        return self.status in ("transition_detected", "incomplete", "conflict")

    def counts(self):
        return {
            "rekey": self.matched_rekeys,
            "unchanged": max(0, self.old_count - self.changed_candidates),
            "addition": 0,
            "confirmed_removal": 0,
            "conflict": self.duplicate_targets,
        }


def inspect_track_id_sets(previous_ids: Iterable[str], current_ids: Iterable[str]) -> IdSetInspection:
    """Classify exact transform evidence without consulting AudioMuse mappings."""

    previous = {str(value) for value in previous_ids}
    current = {str(value) for value in current_ids}
    candidates = {}
    for old_id in previous:
        converted = canonicalize_navidrome_id(old_id)
        if converted.recognized and converted.changed:
            candidates[old_id] = converted.value

    matched = 0
    missing = 0
    duplicates = 0
    for old_id, new_id in candidates.items():
        old_present = old_id in current
        new_present = new_id in current
        if old_present and new_present:
            duplicates += 1
        elif not old_present and new_present:
            matched += 1
        elif not old_present:
            missing += 1

    if duplicates:
        status = "conflict"
    elif matched:
        status = "transition_detected"
    else:
        suspicious_missing = missing >= 25 or (
            bool(previous) and missing / len(previous) >= 0.10
        )
        status = "incomplete" if suspicious_missing else "unchanged"

    return IdSetInspection(
        status=status,
        changed_candidates=len(candidates),
        matched_rekeys=matched,
        missing_candidates=missing,
        duplicate_targets=duplicates,
        old_count=len(previous),
        current_count=len(current),
    )


def migrate_provider_identity(db):
    cur = db.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {t('provider_identity_transitions')} (
            catalog_instance_id TEXT PRIMARY KEY
                REFERENCES {t('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            transition_id TEXT,
            state TEXT NOT NULL DEFAULT 'normal',
            previous_provider_version TEXT,
            current_provider_version TEXT,
            last_checked_provider_version TEXT,
            baseline_catalog_generation BIGINT NOT NULL DEFAULT 0,
            baseline_analysis_generation BIGINT NOT NULL DEFAULT 0,
            detection_reason TEXT,
            required_action TEXT,
            counts JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            target_fingerprint TEXT,
            detected_at TIMESTAMPTZ,
            checked_at TIMESTAMPTZ,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CHECK (state IN ('normal', 'transition_pending', 'applied', 'blocked'))
        )
        """
    )
    cur.execute(
        f"""
        INSERT INTO {t('provider_identity_transitions')}
            (catalog_instance_id, baseline_catalog_generation, baseline_analysis_generation)
        SELECT s.catalog_instance_id,
               COALESCE(c.published_generation, 0),
               COALESCE(a.projection_generation, 0)
          FROM {t('catalog_sources')} s
          LEFT JOIN {t('catalog_state')} c USING (catalog_instance_id)
          LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
        ON CONFLICT (catalog_instance_id) DO NOTHING
        """
    )
    cur.close()


def _source_state(db, server_id, for_update=False):
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT s.catalog_instance_id,
               COALESCE(c.published_generation, 0),
               COALESCE(a.projection_generation, 0),
               p.transition_id, p.state, p.previous_provider_version,
               p.current_provider_version, p.last_checked_provider_version,
               p.detection_reason, p.required_action, p.counts,
               p.target_fingerprint, p.last_error
          FROM {t('catalog_sources')} s
          LEFT JOIN {t('catalog_state')} c USING (catalog_instance_id)
          LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
          LEFT JOIN {t('provider_identity_transitions')} p USING (catalog_instance_id)
         WHERE s.current_core_server_id=%s AND s.rebind_status='active'
         {'FOR UPDATE' if for_update else ''}
        """,
        (server_id,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return {
        "catalog_instance_id": str(row[0]),
        "catalog_generation": int(row[1] or 0),
        "analysis_generation": int(row[2] or 0),
        "transition_id": str(row[3]) if row[3] else None,
        "state": str(row[4] or "normal"),
        "previous_provider_version": str(row[5]) if row[5] else None,
        "current_provider_version": str(row[6]) if row[6] else None,
        "last_checked_provider_version": str(row[7]) if row[7] else None,
        "detection_reason": str(row[8]) if row[8] else None,
        "required_action": str(row[9]) if row[9] else None,
        "counts": row[10] if isinstance(row[10], dict) else {},
        "target_fingerprint": str(row[11]) if row[11] else None,
        "last_error": str(row[12]) if row[12] else None,
    }


def _ensure_transition_row(db, source):
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {t('provider_identity_transitions')}
            (catalog_instance_id, baseline_catalog_generation, baseline_analysis_generation)
        VALUES (%s, %s, %s)
        ON CONFLICT (catalog_instance_id) DO NOTHING
        """,
        (
            source["catalog_instance_id"],
            source["catalog_generation"],
            source["analysis_generation"],
        ),
    )
    cur.close()


def _update_observation(
    db,
    source,
    *,
    state,
    current_version,
    detection_reason,
    required_action,
    last_error=None,
):
    transition_id = source.get("transition_id")
    if state == ProviderIdentityTransitionState.TRANSITION_PENDING.value and not transition_id:
        transition_id = str(uuid.uuid4())
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET transition_id=%s,
               state=%s,
               previous_provider_version=CASE
                   WHEN current_provider_version IS DISTINCT FROM %s
                   THEN current_provider_version
                   ELSE previous_provider_version
               END,
               current_provider_version=%s,
               baseline_catalog_generation=CASE
                   WHEN state='normal' THEN %s ELSE baseline_catalog_generation END,
               baseline_analysis_generation=CASE
                   WHEN state='normal' THEN %s ELSE baseline_analysis_generation END,
               detection_reason=%s,
               required_action=%s,
               detected_at=CASE WHEN %s='transition_pending'
                   THEN COALESCE(detected_at, now()) ELSE detected_at END,
               last_error=%s,
               updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (
            transition_id,
            state,
            current_version,
            current_version,
            source["catalog_generation"],
            source["analysis_generation"],
            detection_reason,
            required_action,
            state,
            str(last_error)[:1000] if last_error else None,
            source["catalog_instance_id"],
        ),
    )
    cur.close()


def observe_provider_version(db, bridge, server_id, commit=True):
    """Probe the credential-contained provider and close admission if it changed."""

    source = _source_state(db, server_id)
    if source is None:
        return None
    _ensure_transition_row(db, source)
    source = _source_state(db, server_id) or source

    probe = getattr(bridge, "probe_server_identity", None)
    if not callable(probe):
        # Unit-test and legacy bridge doubles do not expose the new API. The
        # production ProviderCatalogBridge always does.
        return {**source, "observation": "bridge_unavailable"}

    try:
        identity = probe(server_id)
        current_version = str(identity.get("server_version") or "").strip()
        if not current_version:
            raise RuntimeError("Navidrome ping did not expose serverVersion")
    except Exception as exc:
        state = (
            ProviderIdentityTransitionState.TRANSITION_PENDING.value
            if source["catalog_generation"] > 0
            else ProviderIdentityTransitionState.NORMAL.value
        )
        _update_observation(
            db,
            source,
            state=state,
            current_version=source.get("current_provider_version"),
            detection_reason="provider_version_unverified",
            required_action="retry_provider_identity_check" if state != "normal" else None,
            last_error=exc,
        )
        if commit:
            db.commit()
        return {
            **source,
            "state": state,
            "observation": "unverified",
            "last_error": str(exc),
        }

    if source["catalog_generation"] == 0:
        next_state = ProviderIdentityTransitionState.NORMAL.value
        reason = "fresh_catalogue"
        action = None
    else:
        after_boundary = is_after_last_known_pre_canonical_version(current_version)
        already_checked = source.get("last_checked_provider_version") == current_version
        unresolved = source.get("state") in (
            ProviderIdentityTransitionState.TRANSITION_PENDING.value,
            ProviderIdentityTransitionState.BLOCKED.value,
        )
        if after_boundary is False:
            next_state = ProviderIdentityTransitionState.NORMAL.value
            reason = "pre_transition_version"
            action = None
        elif already_checked and not unresolved:
            next_state = source.get("state") or ProviderIdentityTransitionState.NORMAL.value
            reason = source.get("detection_reason") or "provider_ids_checked"
            action = source.get("required_action")
        else:
            next_state = ProviderIdentityTransitionState.TRANSITION_PENDING.value
            reason = "provider_version_boundary" if after_boundary else "provider_version_uncertain"
            action = "inspect_provider_identity"

    _update_observation(
        db,
        source,
        state=next_state,
        current_version=current_version,
        detection_reason=reason,
        required_action=action,
    )
    if commit:
        db.commit()
    return {
        **source,
        "state": next_state,
        "current_provider_version": current_version,
        "observation": "verified",
        "provider_identity": identity,
        "detection_reason": reason,
        "required_action": action,
    }


def inspect_catalog_identity(db, catalog_instance_id, current_track_ids, current_version=None):
    """Inspect a normalized target before ordinary diff construction."""

    cur = db.cursor()
    cur.execute(
        f"""
        SELECT COALESCE(c.published_generation, 0),
               COALESCE(a.projection_generation, 0),
               p.transition_id, p.state, p.current_provider_version
          FROM {t('catalog_state')} c
          LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
          JOIN {t('provider_identity_transitions')} p USING (catalog_instance_id)
         WHERE c.catalog_instance_id=%s
         FOR UPDATE
        """,
        (catalog_instance_id,),
    )
    state_row = cur.fetchone()
    if state_row is None:
        cur.close()
        return {"state": "normal", "inspection": "not_initialized"}
    generation = int(state_row[0] or 0)
    analysis_generation = int(state_row[1] or 0)
    transition_id = str(state_row[2]) if state_row[2] else None
    observed_version = str(current_version or state_row[4] or "").strip() or None
    if generation == 0:
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET state='normal', last_checked_provider_version=%s,
                   checked_at=now(), last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (observed_version, catalog_instance_id),
        )
        cur.close()
        return {"state": "normal", "inspection": "fresh_catalogue"}

    cur.execute(
        f"SELECT track_id FROM {t('catalog_tracks')} "
        "WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE",
        (catalog_instance_id, generation),
    )
    previous_ids = [str(row[0]) for row in cur.fetchall()]
    inspection = inspect_track_id_sets(previous_ids, current_track_ids)
    target_fingerprint = hashlib.sha256(
        json.dumps(sorted({str(value) for value in current_track_ids}), separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()

    if inspection.status == "unchanged":
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET state='normal', transition_id=NULL,
                   previous_provider_version=CASE
                       WHEN current_provider_version IS DISTINCT FROM %s
                       THEN current_provider_version ELSE previous_provider_version END,
                   current_provider_version=COALESCE(%s, current_provider_version),
                   last_checked_provider_version=COALESCE(%s, current_provider_version),
                   baseline_catalog_generation=%s,
                   baseline_analysis_generation=%s,
                   detection_reason='provider_ids_unchanged', required_action=NULL,
                   counts=%s::jsonb, target_fingerprint=%s, checked_at=now(),
                   detected_at=NULL, last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (
                observed_version,
                observed_version,
                observed_version,
                generation,
                analysis_generation,
                json.dumps(inspection.counts(), sort_keys=True),
                target_fingerprint,
                catalog_instance_id,
            ),
        )
        next_state = "normal"
        action = None
    else:
        next_state = "blocked" if inspection.status in ("conflict", "incomplete") else "transition_pending"
        transition_id = transition_id or str(uuid.uuid4())
        action = (
            "resolve_provider_identity_conflict"
            if next_state == "blocked"
            else "install_lumae_rekey_publisher"
        )
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET transition_id=%s, state=%s,
                   current_provider_version=COALESCE(%s, current_provider_version),
                   detection_reason=%s, required_action=%s,
                   counts=%s::jsonb, target_fingerprint=%s,
                   detected_at=COALESCE(detected_at, now()), checked_at=now(),
                   last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (
                transition_id,
                next_state,
                observed_version,
                inspection.status,
                action,
                json.dumps(inspection.counts(), sort_keys=True),
                target_fingerprint,
                catalog_instance_id,
            ),
        )
    cur.close()
    return {
        "state": next_state,
        "transition_id": transition_id,
        "inspection": inspection.status,
        "required_action": action,
        "counts": inspection.counts(),
        "target_fingerprint": target_fingerprint,
    }


def assert_analysis_projection_allowed(db, bridge, server_id):
    observation = observe_provider_version(db, bridge, server_id, commit=True)
    if not observation or observation.get("observation") == "bridge_unavailable":
        return observation
    if observation.get("state") in ("transition_pending", "blocked"):
        raise ProviderIdentityTransitionPending(
            "Navidrome provider identity is unresolved; the previous Lumae analysis projection is preserved"
        )
    return observation


def provider_transition_health(db, catalog_instance_id):
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT transition_id, state, previous_provider_version,
               current_provider_version, detection_reason, required_action,
               counts, target_fingerprint, checked_at, last_error
          FROM {t('provider_identity_transitions')}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    state = str(row[1] or "normal")
    blocked = state in ("transition_pending", "blocked")
    return {
        "transition_id": str(row[0]) if row[0] else None,
        "state": state,
        "previous_provider_version": str(row[2]) if row[2] else None,
        "current_provider_version": str(row[3]) if row[3] else None,
        "cutoff_version": None,
        "detection_reason": str(row[4]) if row[4] else None,
        "required_action": str(row[5]) if row[5] else None,
        "counts": row[6] if isinstance(row[6], dict) else {},
        "target_fingerprint": str(row[7]) if row[7] else None,
        "checked_at": row[8].isoformat() if hasattr(row[8], "isoformat") else row[8],
        "last_error": str(row[9]) if row[9] else None,
        "catalog_sync_allowed": not blocked,
        "analysis_sync_allowed": not blocked,
        "audiomuse_projection_ingest_allowed": not blocked,
        "provider_mutations_allowed": not blocked,
    }
