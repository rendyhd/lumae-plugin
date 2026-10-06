"""Durable admission shield for provider-ID transitions.

Navidrome: the canonical-ID version boundary and its exact codec
(``provider_identity``). Jellyfin (1.6.0, JF.8): the server ``Id`` from
``/System/Info/Public`` is bound to the catalogue and a different ``Id``
blocks it; the release must be 12.0 or later. No Navidrome canonical-ID logic
ever runs for a Jellyfin source.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Iterable

from plugin.api import table

from . import migrations
from .provider_identity import (
    ProviderIdentityTransitionState,
    canonicalize_navidrome_id,
    is_after_last_known_pre_canonical_version,
)


TRANSITION_BLOCKER = "provider_identity_transition"


def t(name):
    return table(name)


# What a failed ping wrote up to 1.5.0. It no longer writes them; a verified
# ping clears them from rows that still carry them.
_UNVERIFIED_REASON = "provider_version_unverified"
_UNVERIFIED_ACTION = "retry_provider_identity_check"


class ProviderIdentityTransitionPending(RuntimeError):
    """Analysis projection is held back until the provider identity is safe.

    ``reason`` says why: ``provider_identity_unverified`` (the provider did
    not answer the version check; nothing is stored), ``provider_identity_pending``
    (a stored transition is pending or blocked) or ``audiomuse_migration_pending``
    (the Lumae rekey is applied, AudioMuse's own migration is not ready).
    """

    def __init__(self, message, reason="provider_identity_pending"):
        super().__init__(message)
        self.reason = reason


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
            target_scan_count INTEGER NOT NULL DEFAULT 0,
            first_seq BIGINT,
            last_seq BIGINT,
            analysis_baseline JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            baseline_integrity BOOLEAN,
            audiomuse_health TEXT,
            projection_reconcile_required BOOLEAN NOT NULL DEFAULT FALSE,
            manifest_sha256 TEXT,
            collection_deferrals JSONB NOT NULL DEFAULT '[]'::jsonb,
            detected_at TIMESTAMPTZ,
            applied_at TIMESTAMPTZ,
            checked_at TIMESTAMPTZ,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CHECK (state IN ('normal', 'transition_pending', 'applied', 'blocked'))
        )
        """
    )
    migrations.ensure_columns(
        cur, t('provider_identity_transitions'),
        "target_scan_count INTEGER NOT NULL DEFAULT 0",
        "first_seq BIGINT",
        "last_seq BIGINT",
        "analysis_baseline JSONB NOT NULL DEFAULT '{}'::jsonb",
        "baseline_integrity BOOLEAN",
        "audiomuse_health TEXT",
        "projection_reconcile_required BOOLEAN NOT NULL DEFAULT FALSE",
        "manifest_sha256 TEXT",
        "applied_at TIMESTAMPTZ",
        # P3-4c: principals whose collection rekey was deferred, with why.
        "collection_deferrals JSONB NOT NULL DEFAULT '[]'::jsonb",
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {t('provider_identity_manifests')} (
            transition_id TEXT PRIMARY KEY,
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            baseline_catalog_generation BIGINT NOT NULL,
            published_catalog_generation BIGINT NOT NULL,
            baseline_analysis_generation BIGINT NOT NULL,
            published_analysis_generation BIGINT NOT NULL,
            provider_version_before TEXT,
            provider_version_after TEXT,
            target_fingerprint TEXT NOT NULL,
            first_seq BIGINT NOT NULL,
            last_seq BIGINT NOT NULL,
            counts JSONB NOT NULL,
            analysis_baseline JSONB NOT NULL,
            mappings JSONB NOT NULL,
            manifest_sha256 TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_generation_pins')} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            published_generation BIGINT NOT NULL,
            reason TEXT NOT NULL,
            transition_id TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            released_at TIMESTAMPTZ,
            PRIMARY KEY (catalog_instance_id, published_generation, transition_id)
        )
        """
    )
    # 1.6.0 (JF.8): the Jellyfin server Id bound to a catalogue. Also added
    # by migrate_catalog; repeated here because this migration is re-run at
    # Flask startup to repair an update whose install hook rolled back.
    migrations.ensure_columns(cur, t('catalog_sources'), "provider_server_id TEXT")
    # 1.6.0 (JF.9): held-missing Jellyfin tracks and the rekey v2 columns.
    from .jellyfin_continuity import migrate as migrate_jellyfin_continuity

    migrate_jellyfin_continuity(cur)
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


def require_projection_reconcile(db):
    """Persist an upgrade-safe request to compare carried and current projections."""

    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET projection_reconcile_required=TRUE, updated_at=now()
         WHERE state='applied'
        """
    )
    changed = max(int(getattr(cur, "rowcount", 0) or 0), 0)
    cur.close()
    return changed


def projection_reconcile_required(db, catalog_instance_id):
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT COALESCE(projection_reconcile_required, FALSE)
          FROM {t('provider_identity_transitions')}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    cur.close()
    return bool(row and row[0])


def complete_projection_reconcile(db, catalog_instance_id, commit=True):
    """Clear the durable request only after projection publication succeeds."""

    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET projection_reconcile_required=FALSE, updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    cur.close()
    if commit:
        db.commit()


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
               p.target_fingerprint, p.last_error, p.target_scan_count,
               p.first_seq, p.last_seq, p.analysis_baseline,
               p.baseline_integrity, p.audiomuse_health, p.manifest_sha256,
               p.catalog_instance_id IS NOT NULL, p.baseline_catalog_generation,
               p.baseline_analysis_generation, p.detected_at IS NOT NULL,
               s.provider_type, s.provider_server_id
          FROM {t('catalog_sources')} s
          LEFT JOIN {t('catalog_state')} c USING (catalog_instance_id)
          LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
          LEFT JOIN {t('provider_identity_transitions')} p USING (catalog_instance_id)
         WHERE s.current_core_server_id=%s AND s.rebind_status='active'
         {'FOR UPDATE OF s' if for_update else ''}
        """,
        (server_id,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    source = {
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
        "target_scan_count": int(row[13] or 0),
        "first_seq": int(row[14]) if row[14] is not None else None,
        "last_seq": int(row[15]) if row[15] is not None else None,
        "analysis_baseline": row[16] if isinstance(row[16], dict) else {},
        "baseline_integrity": bool(row[17]) if row[17] is not None else None,
        "audiomuse_health": str(row[18]) if row[18] else None,
        "manifest_sha256": str(row[19]) if row[19] else None,
    }
    if len(row) > 23:
        # What an observation compares before it writes (P2-1).
        source.update(
            {
                "transition_exists": bool(row[20]),
                "baseline_catalog_generation": int(row[21]) if row[21] is not None else None,
                "baseline_analysis_generation": int(row[22]) if row[22] is not None else None,
                "detected": bool(row[23]),
            }
        )
    if len(row) > 25:
        source.update(
            {
                "provider_type": str(row[24] or "").strip().lower() or None,
                "provider_server_id": str(row[25]) if row[25] else None,
            }
        )
    return source


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


def _starts_new_transition(source, state):
    return state == ProviderIdentityTransitionState.TRANSITION_PENDING.value and (
        not source.get("transition_id")
        or source.get("state") == ProviderIdentityTransitionState.APPLIED.value
    )


_OBSERVED_KEYS = (
    "transition_exists",
    "state",
    "current_provider_version",
    "detection_reason",
    "required_action",
    "last_error",
    "baseline_catalog_generation",
    "baseline_analysis_generation",
    "detected",
)


def _observation_changes(
    source,
    *,
    state,
    current_version,
    detection_reason,
    required_action,
    last_error=None,
):
    """Whether ``_update_observation`` would change the stored transition row.

    Only ``updated_at`` is ignored. A source read without the compared columns
    (an older caller or a test double) counts as changed.
    """
    if any(key not in source for key in _OBSERVED_KEYS) or not source["transition_exists"]:
        return True
    if _starts_new_transition(source, state):
        return True
    stored_state = source["state"] or ProviderIdentityTransitionState.NORMAL.value
    if stored_state == ProviderIdentityTransitionState.NORMAL.value and (
        source["baseline_catalog_generation"] != source["catalog_generation"]
        or source["baseline_analysis_generation"] != source["analysis_generation"]
    ):
        return True
    if state == ProviderIdentityTransitionState.TRANSITION_PENDING.value and not source["detected"]:
        return True
    return (
        stored_state != state
        or (source["current_provider_version"] or None) != (current_version or None)
        or (source["detection_reason"] or None) != (detection_reason or None)
        or (source["required_action"] or None) != (required_action or None)
        or (source["last_error"] or None) != (str(last_error)[:1000] if last_error else None)
    )


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
    starts_new_transition = _starts_new_transition(source, state)
    if starts_new_transition:
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
                   WHEN state='normal' OR %s THEN %s ELSE baseline_catalog_generation END,
               baseline_analysis_generation=CASE
                   WHEN state='normal' OR %s THEN %s ELSE baseline_analysis_generation END,
               detection_reason=%s,
               required_action=%s,
               target_scan_count=CASE WHEN %s THEN 0 ELSE target_scan_count END,
               first_seq=CASE WHEN %s THEN NULL ELSE first_seq END,
               last_seq=CASE WHEN %s THEN NULL ELSE last_seq END,
               analysis_baseline=CASE WHEN %s THEN '{{}}'::jsonb ELSE analysis_baseline END,
               baseline_integrity=CASE WHEN %s THEN NULL ELSE baseline_integrity END,
               audiomuse_health=CASE WHEN %s THEN NULL ELSE audiomuse_health END,
               manifest_sha256=CASE WHEN %s THEN NULL ELSE manifest_sha256 END,
               applied_at=CASE WHEN %s THEN NULL ELSE applied_at END,
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
            starts_new_transition,
            source["catalog_generation"],
            starts_new_transition,
            source["analysis_generation"],
            detection_reason,
            required_action,
            starts_new_transition,
            starts_new_transition,
            starts_new_transition,
            starts_new_transition,
            starts_new_transition,
            starts_new_transition,
            starts_new_transition,
            starts_new_transition,
            state,
            str(last_error)[:1000] if last_error else None,
            source["catalog_instance_id"],
        ),
    )
    cur.close()


def observe_provider_version(db, bridge, server_id, commit=True, refresh_health=True):
    """Probe the credential-contained provider and close admission if it changed.

    The provider is pinged on every call. The transition row is written only
    when the observed state or evidence changed; ``written`` in the result says
    whether anything was. ``commit=False`` leaves the transaction to the
    caller, so a GET route owns it (P2-1). ``refresh_health=False`` skips the
    AudioMuse health inspection of an applied transition, which the
    ``provider_identity_recheck`` cron keeps current.
    """

    source = _source_state(db, server_id)
    if source is None:
        return None
    written = False
    if source.get("transition_exists") is not True:
        _ensure_transition_row(db, source)
        source = _source_state(db, server_id) or source
        written = True

    probe = getattr(bridge, "probe_server_identity", None)
    if not callable(probe):
        # Unit-test and legacy bridge doubles do not expose the new API. The
        # production ProviderCatalogBridge always does.
        return {**source, "observation": "bridge_unavailable", "written": written}

    if source.get("provider_type") == "jellyfin":
        return _observe_jellyfin(
            db, bridge, probe, server_id, source, written,
            commit=commit, refresh_health=refresh_health,
        )

    try:
        identity = probe(server_id)
        current_version = str(identity.get("server_version") or "").strip()
        if not current_version:
            raise RuntimeError("Navidrome ping did not expose serverVersion")
    except Exception as exc:
        # A failed ping is not evidence: only the error is recorded. The
        # stored state, reason and action stay as the last verified
        # observation left them. Up to 1.5.0 a timed-out ping stored
        # transition_pending, which closed catalogue and analysis admission
        # for every client and failed the analysis projection until a
        # catalogue refresh re-inspected the IDs (the provider_identity_recheck
        # cron, up to 30 minutes later); a later successful ping kept it
        # pending, because an unresolved state is never trusted on version.
        #
        # This call still fails closed. A published catalogue whose provider
        # version cannot be confirmed is gated as transition_pending for the
        # caller only: a refresh inspects the ID sets before it publishes, and
        # a projection waits for a verified version.
        stored_state = source.get("state") or ProviderIdentityTransitionState.NORMAL.value
        gate_state = stored_state
        if (
            source["catalog_generation"] > 0
            and stored_state == ProviderIdentityTransitionState.NORMAL.value
        ):
            gate_state = ProviderIdentityTransitionState.TRANSITION_PENDING.value
        observation = {
            "state": stored_state,
            "current_version": source.get("current_provider_version"),
            "detection_reason": source.get("detection_reason"),
            "required_action": source.get("required_action"),
            "last_error": exc,
        }
        if _observation_changes(source, **observation):
            _update_observation(db, source, **observation)
            written = True
        if commit:
            db.commit()
        return {
            **source,
            "state": gate_state,
            "stored_state": stored_state,
            "observation": "unverified",
            "last_error": str(exc),
            "written": written,
        }

    live_type = str(identity.get("provider_type") or "").strip().lower()
    if source.get("provider_type") and live_type and live_type != source["provider_type"]:
        # The AudioMuse server row now names another server type. Its version
        # says nothing about this catalogue; never adopt it (D-JF.2).
        observation = {
            "state": ProviderIdentityTransitionState.BLOCKED.value,
            "current_version": source.get("current_provider_version"),
            "detection_reason": "provider_type_changed",
            "required_action": "set_up_new_catalogue",
        }
        if _observation_changes(source, **observation):
            _update_observation(db, source, **observation)
            written = True
        if commit:
            db.commit()
        return {
            **source,
            "state": observation["state"],
            "observation": "verified",
            "detection_reason": observation["detection_reason"],
            "required_action": observation["required_action"],
            "written": written,
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
            reason = source.get("detection_reason")
            action = source.get("required_action")
            # A verified ping retires what a failed one wrote up to 1.5.0. An
            # applied transition's real action is restored by its AudioMuse
            # health check (below, or the provider_identity_recheck cron).
            if reason == _UNVERIFIED_REASON:
                reason = None
            if action == _UNVERIFIED_ACTION:
                action = None
            reason = reason or "provider_ids_checked"
        else:
            next_state = ProviderIdentityTransitionState.TRANSITION_PENDING.value
            reason = "provider_version_boundary" if after_boundary else "provider_version_uncertain"
            action = "inspect_provider_identity"

    observation = {
        "state": next_state,
        "current_version": current_version,
        "detection_reason": reason,
        "required_action": action,
    }
    if _observation_changes(source, **observation):
        _update_observation(db, source, **observation)
        written = True
    audiomuse_health = source.get("audiomuse_health")
    if refresh_health and next_state == ProviderIdentityTransitionState.APPLIED.value:
        adapter = getattr(bridge, "core", None)
        if adapter is not None and callable(getattr(adapter, "analysis_mapping_sql", None)):
            from .provider_identity_rekey import refresh_audiomuse_health

            audiomuse_health = refresh_audiomuse_health(
                db,
                source["catalog_instance_id"],
                server_id,
                adapter,
                commit=False,
            )
            written = True
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
        "audiomuse_health": audiomuse_health,
        "written": written,
    }


JELLYFIN_REKEY_PENDING_REASON = "provider_ids_moved"
JELLYFIN_VERIFIED_REASON = "provider_identity_verified"


def jellyfin_identity_verdict(source, identity):
    """``(state, detection_reason, required_action)`` for a probed Jellyfin.

    ``None`` when the probed server is the catalogue's own Jellyfin 12.0+
    server (or the first one seen, which is then bound). Any other answer
    blocks the catalogue: a different server ``Id`` is never adopted as the
    same catalogue, whatever its library looks like.
    """
    from .jellyfin_provider import version_supported

    blocked = ProviderIdentityTransitionState.BLOCKED.value
    if str(identity.get("provider_type") or "").strip().lower() != "jellyfin":
        return blocked, "provider_type_changed", "set_up_new_catalogue"
    if "jellyfin" not in str(identity.get("product_name") or "").lower():
        return blocked, "provider_product_mismatch", "restore_provider_server"
    if not version_supported(identity.get("server_version")):
        return blocked, "provider_version_unsupported", "upgrade_jellyfin"
    bound = source.get("provider_server_id")
    if bound and str(identity.get("server_id") or "") != bound:
        return blocked, "provider_server_changed", "restore_provider_server"
    return None


def _jellyfin_observation_changes(source, *, state, current_version, detection_reason,
                                  required_action, last_error=None):
    if any(key not in source for key in _OBSERVED_KEYS) or not source["transition_exists"]:
        return True
    stored_state = source["state"] or ProviderIdentityTransitionState.NORMAL.value
    if stored_state != state:
        return True
    if state == ProviderIdentityTransitionState.NORMAL.value and (
        source.get("transition_id")
        or source["baseline_catalog_generation"] != source["catalog_generation"]
        or source["baseline_analysis_generation"] != source["analysis_generation"]
    ):
        return True
    if state == ProviderIdentityTransitionState.TRANSITION_PENDING.value and not source["detected"]:
        return True
    return (
        (source["current_provider_version"] or None) != (current_version or None)
        or (source["detection_reason"] or None) != (detection_reason or None)
        or (source["required_action"] or None) != (required_action or None)
        or (source["last_error"] or None) != (str(last_error)[:1000] if last_error else None)
    )


def _update_jellyfin_observation(db, source, *, state, current_version, detection_reason,
                                 required_action, last_error=None):
    """Write a Jellyfin observation.

    ``normal`` clears the transition id, so the next rekey always opens with
    a new one. Entering ``transition_pending`` from any other state opens a
    new transition id. ``blocked`` resets any target proof: a rekey must be
    proven again by two scans of the verified server.
    """
    stored_state = source.get("state") or ProviderIdentityTransitionState.NORMAL.value
    normal = state == ProviderIdentityTransitionState.NORMAL.value
    opens = (
        state == ProviderIdentityTransitionState.TRANSITION_PENDING.value
        and stored_state != state
    )
    resets_target = normal or opens or state == ProviderIdentityTransitionState.BLOCKED.value
    transition_id = None if normal else (str(uuid.uuid4()) if opens else source.get("transition_id"))
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET transition_id=%s,
               state=%s,
               previous_provider_version=CASE
                   WHEN %s::text IS NOT NULL AND current_provider_version IS DISTINCT FROM %s
                   THEN current_provider_version
                   ELSE previous_provider_version
               END,
               current_provider_version=COALESCE(%s, current_provider_version),
               baseline_catalog_generation=CASE WHEN %s OR %s THEN %s
                   ELSE baseline_catalog_generation END,
               baseline_analysis_generation=CASE WHEN %s OR %s THEN %s
                   ELSE baseline_analysis_generation END,
               detection_reason=%s,
               required_action=%s,
               target_fingerprint=CASE WHEN %s THEN NULL ELSE target_fingerprint END,
               target_scan_count=CASE WHEN %s THEN 0 ELSE target_scan_count END,
               first_seq=CASE WHEN %s THEN NULL ELSE first_seq END,
               last_seq=CASE WHEN %s THEN NULL ELSE last_seq END,
               manifest_sha256=CASE WHEN %s THEN NULL ELSE manifest_sha256 END,
               applied_at=CASE WHEN %s THEN NULL ELSE applied_at END,
               detected_at=CASE WHEN %s THEN NULL
                                WHEN %s='transition_pending' THEN COALESCE(detected_at, now())
                                ELSE detected_at END,
               rekey_contract=CASE WHEN %s THEN NULL ELSE rekey_contract END,
               checked_at=now(),
               last_error=%s,
               updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (
            transition_id,
            state,
            current_version,
            current_version,
            current_version,
            normal,
            opens,
            source["catalog_generation"],
            normal,
            opens,
            source["analysis_generation"],
            detection_reason,
            required_action,
            resets_target,
            resets_target,
            opens or normal,
            opens or normal,
            opens or normal,
            opens or normal,
            normal,
            state,
            opens or normal,
            str(last_error)[:1000] if last_error else None,
            source["catalog_instance_id"],
        ),
    )
    cur.close()
    return transition_id


def _bind_jellyfin_server(db, source, server_identity_id):
    """Bind the first verified Jellyfin server ``Id`` to the catalogue, once."""
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t('catalog_sources')}
           SET provider_server_id=%s, updated_at=now()
         WHERE catalog_instance_id=%s AND provider_server_id IS NULL
        """,
        (server_identity_id, source["catalog_instance_id"]),
    )
    bound = max(int(getattr(cur, "rowcount", 0) or 0), 0)
    cur.close()
    return bound > 0


def _observe_jellyfin(db, bridge, probe, server_id, source, written, *, commit, refresh_health):
    """Probe ``/System/Info/Public`` and gate the Jellyfin catalogue on it."""

    stored_state = source.get("state") or ProviderIdentityTransitionState.NORMAL.value
    try:
        identity = probe(server_id)
        current_version = str(identity.get("server_version") or "").strip()
        if not current_version:
            raise RuntimeError("Jellyfin did not report its version")
    except Exception as exc:
        # As for Navidrome from 1.5.1: a failed probe is not evidence. Only
        # the error is stored; the state, reason and action stay as the last
        # verified probe left them, so a blocked catalogue stays blocked, an
        # applied one keeps its action and a proven move keeps its pending
        # rekey (provider_ids_moved) and target proof. Client admission is
        # not closed by an unanswered probe.
        #
        # This call still fails closed: a published catalogue is gated as
        # transition_pending for the caller only, a refresh never reads an
        # unverified Jellyfin (catalog._require_jellyfin_identity) and a
        # projection waits for a verified probe.
        gate_state = stored_state
        if (
            source["catalog_generation"] > 0
            and stored_state == ProviderIdentityTransitionState.NORMAL.value
        ):
            gate_state = ProviderIdentityTransitionState.TRANSITION_PENDING.value
        observation = {
            "state": stored_state,
            "current_version": source.get("current_provider_version"),
            "detection_reason": source.get("detection_reason"),
            "required_action": source.get("required_action"),
            "last_error": exc,
        }
        if _jellyfin_observation_changes(source, **observation):
            _update_jellyfin_observation(db, source, **observation)
            written = True
        if commit:
            db.commit()
        return {
            **source,
            "state": gate_state,
            "stored_state": stored_state,
            "observation": "unverified",
            "last_error": str(exc),
            "written": written,
        }

    verdict = jellyfin_identity_verdict(source, identity)
    provider_server_id = source.get("provider_server_id")
    if verdict is not None:
        next_state, reason, action = verdict
        # The version of another server is not this catalogue's version.
        observed_version = (
            current_version
            if reason in ("provider_version_unsupported",)
            else source.get("current_provider_version")
        )
    else:
        observed_version = current_version
        if not provider_server_id:
            if _bind_jellyfin_server(db, source, identity["server_id"]):
                written = True
            provider_server_id = identity["server_id"]
        stored_reason = source.get("detection_reason")
        if source["catalog_generation"] == 0:
            next_state, reason, action = (
                ProviderIdentityTransitionState.NORMAL.value, "fresh_catalogue", None
            )
        elif (
            stored_state == ProviderIdentityTransitionState.TRANSITION_PENDING.value
            and stored_reason == JELLYFIN_REKEY_PENDING_REASON
        ):
            next_state, reason, action = (
                stored_state, JELLYFIN_REKEY_PENDING_REASON, "wait_for_lumae_rekey"
            )
        elif stored_state == ProviderIdentityTransitionState.APPLIED.value:
            next_state = stored_state
            reason = (
                stored_reason
                if stored_reason not in (None, _UNVERIFIED_REASON)
                else JELLYFIN_VERIFIED_REASON
            )
            action = source.get("required_action")
            if action == _UNVERIFIED_ACTION:
                # Its real action comes back with the AudioMuse health check.
                action = None
        else:
            next_state, reason, action = (
                ProviderIdentityTransitionState.NORMAL.value, JELLYFIN_VERIFIED_REASON, None
            )
    observation = {
        "state": next_state,
        "current_version": observed_version,
        "detection_reason": reason,
        "required_action": action,
    }
    if _jellyfin_observation_changes(source, **observation):
        _update_jellyfin_observation(db, source, **observation)
        written = True
    audiomuse_health = source.get("audiomuse_health")
    if refresh_health and next_state == ProviderIdentityTransitionState.APPLIED.value:
        adapter = getattr(bridge, "core", None)
        if adapter is not None and callable(getattr(adapter, "analysis_mapping_sql", None)):
            from .provider_identity_rekey import refresh_audiomuse_health

            audiomuse_health = refresh_audiomuse_health(
                db, source["catalog_instance_id"], server_id, adapter, commit=False,
            )
            written = True
    if commit:
        db.commit()
    return {
        **source,
        "state": next_state,
        "current_provider_version": observed_version,
        "provider_server_id": provider_server_id,
        "observation": "verified",
        "provider_identity": identity,
        "detection_reason": reason,
        "required_action": action,
        "audiomuse_health": audiomuse_health,
        "written": written,
    }


def inspect_catalog_identity(
    db,
    catalog_instance_id,
    current_track_ids,
    current_version=None,
    target_fingerprint=None,
):
    """Inspect a normalized target before ordinary diff construction."""

    cur = db.cursor()
    cur.execute(
        f"""
        SELECT COALESCE(c.published_generation, 0),
               COALESCE(a.projection_generation, 0),
               p.transition_id, p.state, p.current_provider_version,
               p.target_fingerprint, p.target_scan_count
          FROM {t('catalog_state')} c
          LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
          JOIN {t('provider_identity_transitions')} p USING (catalog_instance_id)
         WHERE c.catalog_instance_id=%s
         FOR UPDATE OF c, p
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
    previous_target_fingerprint = str(state_row[5]) if state_row[5] else None
    previous_target_scan_count = int(state_row[6] or 0)
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
    target_fingerprint = target_fingerprint or hashlib.sha256(
        json.dumps(sorted({str(value) for value in current_track_ids}), separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()

    if inspection.status == "unchanged":
        preserve_applied = str(state_row[3] or "normal") == "applied" and bool(transition_id)
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET state=%s, transition_id=CASE WHEN %s THEN transition_id ELSE NULL END,
                   previous_provider_version=CASE
                       WHEN current_provider_version IS DISTINCT FROM %s
                       THEN current_provider_version ELSE previous_provider_version END,
                   current_provider_version=COALESCE(%s, current_provider_version),
                   last_checked_provider_version=COALESCE(%s, current_provider_version),
                   baseline_catalog_generation=%s,
                   baseline_analysis_generation=%s,
                   detection_reason='provider_ids_unchanged', required_action=NULL,
                   counts=CASE WHEN %s THEN counts ELSE %s::jsonb END,
                   target_fingerprint=CASE WHEN %s THEN target_fingerprint ELSE %s END,
                   target_scan_count=CASE WHEN %s THEN target_scan_count ELSE 0 END,
                   checked_at=now(),
                   detected_at=CASE WHEN %s THEN detected_at ELSE NULL END,
                   last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (
                "applied" if preserve_applied else "normal",
                preserve_applied,
                observed_version,
                observed_version,
                observed_version,
                generation,
                analysis_generation,
                preserve_applied,
                json.dumps(inspection.counts(), sort_keys=True),
                preserve_applied,
                target_fingerprint,
                preserve_applied,
                preserve_applied,
                catalog_instance_id,
            ),
        )
        next_state = "applied" if preserve_applied else "normal"
        action = None
        stable_scan_count = previous_target_scan_count if preserve_applied else 0
    else:
        next_state = "blocked" if inspection.status in ("conflict", "incomplete") else "transition_pending"
        transition_id = transition_id or str(uuid.uuid4())
        stable_scan_count = (
            previous_target_scan_count + 1
            if previous_target_fingerprint == target_fingerprint
            else 1
        )
        action = (
            "resolve_provider_identity_conflict"
            if next_state == "blocked"
            else "wait_for_lumae_rekey"
        )
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET transition_id=%s, state=%s,
                   current_provider_version=COALESCE(%s, current_provider_version),
                   detection_reason=%s, required_action=%s,
                   counts=%s::jsonb, target_fingerprint=%s,
                   target_scan_count=%s,
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
                stable_scan_count,
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
        "target_scan_count": stable_scan_count,
    }


def assert_analysis_projection_allowed(db, bridge, server_id):
    observation = observe_provider_version(db, bridge, server_id, commit=True)
    if not observation or observation.get("observation") == "bridge_unavailable":
        return observation
    if observation.get("state") in ("transition_pending", "blocked"):
        if (
            observation.get("observation") == "unverified"
            and observation.get("stored_state") == ProviderIdentityTransitionState.NORMAL.value
        ):
            server = "Jellyfin" if observation.get("provider_type") == "jellyfin" else "Navidrome"
            raise ProviderIdentityTransitionPending(
                f"{server} did not answer the provider identity check; the previous "
                "Lumae analysis projection is preserved until it does",
                reason="provider_identity_unverified",
            )
        raise ProviderIdentityTransitionPending(
            "Provider identity is unresolved; the previous Lumae analysis projection is preserved"
        )
    source = _source_state(db, server_id)
    if (
        source
        and source.get("state") == ProviderIdentityTransitionState.APPLIED.value
        and source.get("audiomuse_health") != "ready"
    ):
        raise ProviderIdentityTransitionPending(
            "Lumae provider IDs are safe, but AudioMuse migration is not ready; "
            "the carried-forward analysis projection is preserved",
            reason="audiomuse_migration_pending",
        )
    return observation


def _transition_history(db, catalog_instance_id):
    from .provider_identity_rekey import transition_history

    return transition_history(db, catalog_instance_id)


def provider_transition_health(db, catalog_instance_id):
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT transition_id, state, previous_provider_version,
                   current_provider_version, detection_reason, required_action,
                   counts, target_fingerprint, checked_at, last_error,
                   first_seq, last_seq, target_scan_count, analysis_baseline,
                   baseline_integrity, audiomuse_health, manifest_sha256,
                   rekey_contract
              FROM {t('provider_identity_transitions')}
             WHERE catalog_instance_id=%s
            """,
            (catalog_instance_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
    if row is None:
        return None
    state = str(row[1] or "normal")
    blocked = state in ("transition_pending", "blocked")
    stored_counts = row[6] if isinstance(row[6], dict) else {}
    counts = {
        name: int(stored_counts.get(name, 0) or 0)
        for name in ("rekey", "unchanged", "addition", "confirmed_removal", "conflict")
    }
    return {
        "transition_id": str(row[0]) if row[0] else None,
        "state": state,
        "previous_provider_version": str(row[2]) if row[2] else None,
        "current_provider_version": str(row[3]) if row[3] else None,
        "cutoff_version": None,
        "detection_reason": str(row[4]) if row[4] else None,
        "required_action": str(row[5]) if row[5] else None,
        "counts": counts,
        "target_fingerprint": str(row[7]) if row[7] else None,
        "checked_at": row[8].isoformat() if hasattr(row[8], "isoformat") else row[8],
        "last_error": str(row[9]) if row[9] else None,
        "first_seq": int(row[10]) if row[10] is not None else None,
        "last_seq": int(row[11]) if row[11] is not None else None,
        "target_scan_count": int(row[12] or 0),
        "analysis_baseline": row[13] if isinstance(row[13], dict) else {},
        "baseline_integrity": bool(row[14]) if row[14] is not None else None,
        "audiomuse_health": str(row[15]) if row[15] else None,
        "manifest_sha256": str(row[16]) if row[16] else None,
        # 1.6.0: "provider_identity_rekey_v2" for a Jellyfin fingerprint
        # rekey; null for Navidrome (v1) and when no rekey is under way.
        "rekey_contract": str(row[17]) if len(row) > 17 and row[17] else None,
        # 1.6.0: the last applied transition and the retained chain, kept
        # whatever the current state (abandoned, blocked, back to normal).
        **_transition_history(db, catalog_instance_id),
        "catalog_sync_allowed": not blocked,
        "analysis_sync_allowed": not blocked,
        "audiomuse_projection_ingest_allowed": not blocked
        and (state != "applied" or str(row[15] or "") == "ready"),
        "provider_mutations_allowed": not blocked,
    }
