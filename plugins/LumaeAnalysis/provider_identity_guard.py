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


def _ids_reverted_to_legacy(published_ids, scanned_ids) -> bool:
    """Whether the scan carries published canonical IDs in their legacy form.

    The rekey check run backwards: a scanned legacy ID whose canonical form
    is published, while that canonical ID is no longer in the scan, is exact
    codec evidence that the server's IDs changed back (its pre-rekey database
    restored). A restore reverts the library, not one track: the reverted
    IDs must outnumber the published canonical IDs still in the scan
    unchanged, so a single legacy ID among canonical ones is not a reversal.
    A scan that has both forms of a track is a conflict, not a reversal, and
    is not counted as one. A binary-only downgrade keeps the canonical IDs
    and has none.
    """
    published = {str(value) for value in published_ids}
    scanned = {str(value) for value in scanned_ids}
    still_canonical = 0
    for kept_id in scanned & published:
        kept = canonicalize_navidrome_id(kept_id)
        if kept.recognized and not kept.changed:
            still_canonical += 1
    reverted = both_forms = 0
    for scanned_id in scanned - published:
        converted = canonicalize_navidrome_id(scanned_id)
        if not (converted.recognized and converted.changed) or converted.value not in published:
            continue
        if converted.value in scanned:
            both_forms += 1
        else:
            reverted += 1
    return both_forms == 0 and reverted > still_canonical


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
        # W1 (runner plan 2026-10-25): when the recheck last took this source.
        "recheck_attempted_at TIMESTAMPTZ",
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


# The states an applied transition is stored in while it waits for an
# inspection: a new (or pre-boundary) provider version, or a block, sends it
# back without discarding its id, manifest and ``applied_at``
# (_starts_new_transition), and inspect_catalog_identity restores ``applied``
# when the IDs are unchanged.
_APPLIED_AWAITING_INSPECTION = (
    ProviderIdentityTransitionState.TRANSITION_PENDING.value,
    ProviderIdentityTransitionState.BLOCKED.value,
)


def _published_sql(alias):
    """SQL: the row carries a published transition (manifest and ``applied_at``)."""
    return f"({alias}.applied_at IS NOT NULL AND {alias}.manifest_sha256 IS NOT NULL)"


def transition_was_applied(state, transition_id, published):
    """Whether the row's transition was applied and still stands.

    Stored ``applied``, or stored ``transition_pending``/``blocked`` with its
    transition id and a published transition (``published``: ``applied_at``
    and ``manifest_sha256`` both set, as ``_published_sql`` reads them).
    ``_was_applied_sql`` is the same rule for a statement.
    """
    return bool(transition_id) and (
        state == ProviderIdentityTransitionState.APPLIED.value
        or (state in _APPLIED_AWAITING_INSPECTION and bool(published))
    )


def _was_applied_sql(alias):
    """``transition_was_applied`` as an SQL condition on the row ``alias``."""
    awaiting = ", ".join(f"'{state}'" for state in _APPLIED_AWAITING_INSPECTION)
    return (
        f"({alias}.transition_id IS NOT NULL"
        f" AND ({alias}.state='{ProviderIdentityTransitionState.APPLIED.value}'"
        f" OR ({alias}.state IN ({awaiting}) AND {_published_sql(alias)})))"
    )


def require_projection_reconcile(db):
    """Persist an upgrade-safe request to compare carried and current projections.

    Every applied transition gets it, also one a version observation or a
    block has sent back to inspection: the inspection restores ``applied``
    (the projection is then owed) or opens the next transition (one extra
    projection check, which is harmless).

    Locks in the order of a refresh: when a transition was applied, all
    ``catalog_state`` rows first (by ``catalog_instance_id``, as
    prune_catalog_storage does later in the same install transaction), then
    the applied transition rows. A refresh
    inspecting a transition_pending or blocked row holds ``catalog_state``
    and waits for the transition row (inspect_catalog_identity,
    catalog._confirm_catalog_identity); locking the transition row first
    and then reaching prune_catalog_storage closed a cycle with it. A row
    that becomes applied after the catalogue rows are locked is not flagged:
    its own publication leaves AudioMuse health to be checked, and the
    recheck projects then.
    """

    cur = db.cursor()
    cur.execute(
        f"""
        SELECT c.catalog_instance_id
          FROM {t('catalog_state')} c
          JOIN {t('provider_identity_transitions')} p USING (catalog_instance_id)
         WHERE {_was_applied_sql('p')}
         ORDER BY c.catalog_instance_id
        """
    )
    locked = [str(row[0]) for row in cur.fetchall() or ()]
    if not locked:
        cur.close()
        return 0
    # All catalog_state rows, in id order, as prune_catalog_storage locks them
    # later in the install: locking only the applied sources' rows first (S2,
    # then S1 in prune) inverted the order against a process locking by id.
    cur.execute(
        f"SELECT catalog_instance_id FROM {t('catalog_state')} "
        "ORDER BY catalog_instance_id FOR UPDATE"
    )
    cur.fetchall()
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')} p
           SET projection_reconcile_required=TRUE, updated_at=now()
         WHERE p.catalog_instance_id = ANY(%s) AND {_was_applied_sql('p')}
        """,
        (locked,),
    )
    changed = max(int(getattr(cur, "rowcount", 0) or 0), 0)
    cur.close()
    return changed


def claim_identity_recheck(db, spacing_seconds, server_id=None):
    """W1: the server of a source whose transition needs the identity recheck
    (``transition_pending``, or ``applied`` with AudioMuse not ready or a
    projection owed: what ``provider_identity_recheck_task`` acts on) and that
    it has not taken in the last ``spacing_seconds``, marked taken; None when
    no source needs it. The caller commits."""
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')} tr SET recheck_attempted_at=now()
          FROM (SELECT pending.catalog_instance_id, s.current_core_server_id
                  FROM {t('provider_identity_transitions')} pending
                  JOIN {t('catalog_sources')} s USING (catalog_instance_id)
                 WHERE (pending.state='transition_pending'
                        OR (pending.state='applied'
                            AND (COALESCE(pending.audiomuse_health, '')<>'ready'
                                 OR pending.projection_reconcile_required)))
                   AND (pending.recheck_attempted_at IS NULL
                        OR pending.recheck_attempted_at <= now() - make_interval(secs => %s))
                   AND (%s::text IS NULL OR s.current_core_server_id=%s)
                 ORDER BY pending.recheck_attempted_at NULLS FIRST, pending.catalog_instance_id
                 LIMIT 1 FOR UPDATE OF pending SKIP LOCKED) due
         WHERE tr.catalog_instance_id=due.catalog_instance_id
        RETURNING due.current_core_server_id
        """,
        (int(spacing_seconds), server_id, server_id),
    )
    row = cur.fetchone()
    cur.close()
    return str(row[0]) if row and row[0] else None


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
               s.provider_type, s.provider_server_id,
               {_published_sql('p')}
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
    if len(row) > 26:
        # For transition_was_applied: a transition sent back to inspection.
        source["transition_published"] = bool(row[26])
    return source


def _source_was_applied(source):
    return transition_was_applied(
        source.get("state"), source.get("transition_id"), source.get("transition_published")
    )


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
    # An applied transition that a new provider version sends back to
    # inspection keeps its identity, manifest and AudioMuse health:
    # inspect_catalog_identity restores ``applied`` when the IDs are
    # unchanged and opens the next transition only when they changed. Up to
    # 1.10.2 this opened it at once, and the unchanged inspection then set
    # ``normal`` while the AudioMuse migration was still outstanding.
    return (
        state == ProviderIdentityTransitionState.TRANSITION_PENDING.value
        and not source.get("transition_id")
    )


def _caller_gate(source, state, superseded):
    """The state the caller is gated on.

    ``superseded``: the stored state changed between this call's read and its
    write (a rekey publication, another observer), so the write was skipped
    and that change stands. The call still fails closed, as after a failed
    probe: a published catalogue is gated as ``transition_pending`` for this
    caller only, so a refresh inspects the ID sets under the row lock and a
    projection waits.
    """
    if (
        superseded
        and source["catalog_generation"] > 0
        and state
        not in (
            ProviderIdentityTransitionState.TRANSITION_PENDING.value,
            ProviderIdentityTransitionState.BLOCKED.value,
        )
    ):
        return ProviderIdentityTransitionState.TRANSITION_PENDING.value
    return state


def _row_written(cur):
    """Whether a compare-and-set UPDATE matched its row (a cursor double
    without a row count is taken to have written)."""
    return getattr(cur, "rowcount", 1) != 0


def _probe_error_changes(source, last_error):
    """Whether a failed probe's error differs from the stored one."""
    if any(key not in source for key in _OBSERVED_KEYS) or not source["transition_exists"]:
        return True
    return (source["last_error"] or None) != (str(last_error)[:1000] if last_error else None)


# The row an observation read, by state and transition id. The state alone
# is not enough: an inspection that opens the next transition of a
# was-applied ``transition_pending`` row gives it a new id and keeps the
# state, and a write keyed on the state alone put the old id back over the
# new transition's baselines and proof (its publication then failed on the
# manifest primary key for good).
_COMPARE_AND_SET = (
    "WHERE catalog_instance_id=%s AND state=%s AND transition_id IS NOT DISTINCT FROM %s"
)


def _compare_and_set_params(source):
    return (
        source["catalog_instance_id"],
        source.get("state") or ProviderIdentityTransitionState.NORMAL.value,
        source.get("transition_id"),
    )


def _record_probe_error(db, source, last_error):
    """Store a failed probe's error and nothing else.

    The rest of the row was read without a lock and may be stale by now, so
    none of it is written back; a state that changed since the read keeps its
    own ``last_error`` too. Returns whether the row was written.
    """
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET last_error=%s, updated_at=now()
         {_COMPARE_AND_SET}
        """,
        (
            str(last_error)[:1000] if last_error else None,
            *_compare_and_set_params(source),
        ),
    )
    written = _row_written(cur)
    cur.close()
    return written


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
    """Write an observation; returns whether the row was written.

    Compare-and-set on the state ``source`` was read with: the read takes no
    lock, so a state committed since (a rekey publication's ``applied``)
    wins and nothing is written. A failed probe (``last_error``) is not
    evidence and stores only its error.
    """
    if last_error:
        return _record_probe_error(db, source, last_error)
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
               last_error=NULL,
               updated_at=now()
         {_COMPARE_AND_SET}
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
            *_compare_and_set_params(source),
        ),
    )
    written = _row_written(cur)
    cur.close()
    return written


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
        # Only the error is written, and only onto the state this call read
        # (_record_probe_error): a rekey publication that committed
        # ``applied`` while the ping was failing is not overwritten.
        if _probe_error_changes(source, exc):
            if _update_observation(db, source, **observation) is not False:
                written = True
            else:
                # The row changed since this call read it (a health check
                # sent an applied transition back to inspection, a rekey
                # publication): its read state is stale, so gate closed.
                gate_state = _caller_gate(source, gate_state, True)
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
        # A lost compare-and-set needs nothing more here: the caller is
        # gated as blocked either way.
        if _observation_changes(source, **observation):
            if _update_observation(db, source, **observation) is not False:
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
        if after_boundary is False and not _source_was_applied(source):
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
            # A pre-boundary version reaches this branch only for an applied
            # transition: a server that moved to canonical IDs reports a
            # pre-canonical release (a downgrade, or a misreported version).
            # Writing ``normal`` lifted the projection hold while the AudioMuse
            # migration could still be owed; like any other new version it is
            # sent back to inspection, keeping its identity, and the
            # inspection restores ``applied`` when the IDs are unchanged. Once
            # an inspection has checked that version, the branch above keeps
            # ``applied``.
            next_state = ProviderIdentityTransitionState.TRANSITION_PENDING.value
            if after_boundary is False:
                reason = "pre_transition_version_after_rekey"
            elif after_boundary:
                reason = "provider_version_boundary"
            else:
                reason = "provider_version_uncertain"
            action = "inspect_provider_identity"

    observation = {
        "state": next_state,
        "current_version": current_version,
        "detection_reason": reason,
        "required_action": action,
    }
    superseded = False
    if _observation_changes(source, **observation):
        if _update_observation(db, source, **observation) is False:
            superseded = True
        else:
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
        "state": _caller_gate(source, next_state, superseded),
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

    Compare-and-set on the state and transition id ``source`` was read with,
    as ``_update_observation``; returns whether the row was written.
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
         {_COMPARE_AND_SET}
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
            *_compare_and_set_params(source),
        ),
    )
    written = _row_written(cur)
    cur.close()
    return written


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
        # Only the error, and only onto the state this call read: a rekey
        # publication that committed meanwhile is not overwritten.
        if _probe_error_changes(source, exc):
            if _record_probe_error(db, source, exc):
                written = True
            else:
                # The row changed since the read: gate closed, as above.
                gate_state = _caller_gate(source, gate_state, True)
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
        elif _source_was_applied(source):
            # An applied transition blocked for a while (another server, an
            # unsupported release) by the catalogue's own server again: the
            # rekey it applied stands, and so does the projection hold until
            # AudioMuse is ready. ``normal`` discarded both. The real action
            # comes back with the AudioMuse health check.
            next_state, reason, action = (
                ProviderIdentityTransitionState.APPLIED.value, JELLYFIN_VERIFIED_REASON, None
            )
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
    superseded = False
    if _jellyfin_observation_changes(source, **observation):
        if _update_jellyfin_observation(db, source, **observation):
            written = True
        else:
            superseded = True
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
        "state": _caller_gate(source, next_state, superseded),
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
               p.target_fingerprint, p.target_scan_count,
               {_published_sql('p')},
               p.audiomuse_health
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
    stored_state = str(state_row[3] or "normal")
    # The row's transition was published: it is still ``applied``, or a new
    # provider version (or a block) sent it back to inspection, which keeps
    # its id, manifest and ``applied_at`` (_starts_new_transition).
    was_applied = transition_was_applied(
        stored_state, transition_id, len(state_row) > 7 and bool(state_row[7])
    )
    stored_health = str(state_row[8]) if len(state_row) > 8 and state_row[8] else None
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
        # The same IDs: an applied transition stays applied, with the
        # AudioMuse health it had and the action that health still requires
        # (as refresh_audiomuse_health writes it), so the analysis projection
        # stays held until AudioMuse has migrated.
        #
        # A pre-canonical version can also mean the server's old database
        # was restored: the legacy IDs are back. The check above cannot see
        # that (it looks for legacy IDs that became canonical), and restoring
        # ``applied`` then held every projection for good: AudioMuse's
        # mapping is keyed on the legacy IDs again and never matches the
        # carried canonical links. Such a row goes ``normal`` and drops its
        # applied identity, as up to 1.10.2, so the ordinary diff and the
        # projection rebuild from the server's IDs.
        reverted = (
            was_applied
            and is_after_last_known_pre_canonical_version(observed_version) is False
            and _ids_reverted_to_legacy(previous_ids, current_track_ids)
        )
        preserve_applied = was_applied and not reverted
        action = (
            "run_audiomuse_provider_migration"
            if preserve_applied and stored_health != "ready"
            else None
        )
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
                   detection_reason='provider_ids_unchanged', required_action=%s,
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
                action,
                preserve_applied,
                json.dumps(inspection.counts(), sort_keys=True),
                preserve_applied,
                target_fingerprint,
                preserve_applied,
                preserve_applied,
                catalog_instance_id,
            ),
        )
        if reverted:
            # The manifest table keeps the transition (its history).
            cur.execute(
                f"""
                UPDATE {t('provider_identity_transitions')}
                   SET detection_reason='provider_ids_reverted',
                       first_seq=NULL, last_seq=NULL,
                       analysis_baseline='{{}}'::jsonb, baseline_integrity=NULL,
                       audiomuse_health=NULL, manifest_sha256=NULL, applied_at=NULL
                 WHERE catalog_instance_id=%s
                """,
                (catalog_instance_id,),
            )
        next_state = "applied" if preserve_applied else "normal"
        stable_scan_count = previous_target_scan_count if preserve_applied else 0
    else:
        next_state = "blocked" if inspection.status in ("conflict", "incomplete") else "transition_pending"
        # A published transition is finished: IDs that changed again open the
        # next one, with its own id (the manifest keeps the old one), the
        # current generations as baselines and a proof that starts over. A
        # block keeps the applied identity, so IDs that turn out unchanged
        # restore ``applied`` above.
        opens = was_applied and next_state == "transition_pending"
        # A ``normal`` row reaches this branch through a caller-only gate (a
        # failed ping, a lost compare-and-set); its baselines are set here,
        # under the row lock, as a verified observation sets them, and it has
        # no proof under way either.
        rebase = opens or stored_state == "normal"
        # A block of an applied transition keeps the counts and target proof
        # of the transition it applied: IDs found unchanged later restore
        # ``applied`` with them, and they must agree with its manifest and seq
        # range, not describe the scan that blocked.
        keeps_applied_proof = was_applied and next_state == "blocked"
        transition_id = str(uuid.uuid4()) if opens or not transition_id else transition_id
        if keeps_applied_proof:
            stable_scan_count = previous_target_scan_count
        else:
            stable_scan_count = (
                previous_target_scan_count + 1
                if previous_target_fingerprint == target_fingerprint and not rebase
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
                   counts=COALESCE(%s::jsonb, counts), target_fingerprint=%s,
                   target_scan_count=%s,
                   baseline_catalog_generation=CASE
                       WHEN %s THEN %s ELSE baseline_catalog_generation END,
                   baseline_analysis_generation=CASE
                       WHEN %s THEN %s ELSE baseline_analysis_generation END,
                   first_seq=CASE WHEN %s THEN NULL ELSE first_seq END,
                   last_seq=CASE WHEN %s THEN NULL ELSE last_seq END,
                   analysis_baseline=CASE WHEN %s THEN '{{}}'::jsonb ELSE analysis_baseline END,
                   baseline_integrity=CASE WHEN %s THEN NULL ELSE baseline_integrity END,
                   audiomuse_health=CASE WHEN %s THEN NULL ELSE audiomuse_health END,
                   manifest_sha256=CASE WHEN %s THEN NULL ELSE manifest_sha256 END,
                   applied_at=CASE WHEN %s THEN NULL ELSE applied_at END,
                   detected_at=CASE WHEN %s THEN now() ELSE COALESCE(detected_at, now()) END,
                   checked_at=now(),
                   last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (
                transition_id,
                next_state,
                observed_version,
                inspection.status,
                action,
                # Read under the row lock above, so kept as they are stored.
                None if keeps_applied_proof else json.dumps(inspection.counts(), sort_keys=True),
                previous_target_fingerprint if keeps_applied_proof else target_fingerprint,
                stable_scan_count,
                rebase,
                generation,
                rebase,
                analysis_generation,
                opens,
                opens,
                opens,
                opens,
                opens,
                opens,
                opens,
                opens,
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
    if source and source.get("state") in _APPLIED_AWAITING_INSPECTION:
        # Stored since the observation (a health check sent an applied
        # transition back to inspection while this call's ping hung): the
        # observation's gate is stale, and the stored state holds.
        raise ProviderIdentityTransitionPending(
            "Provider identity is unresolved; the previous Lumae analysis projection is preserved"
        )
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
