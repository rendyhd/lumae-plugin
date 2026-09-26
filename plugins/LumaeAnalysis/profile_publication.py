"""Fenced source-profile attempts and atomic public profile publication."""

from uuid import uuid4


RETRY_LIMIT = 3
RETRY_DELAYS_SECONDS = (60, 300, 1800)
TRANSIENT_FAILURES = frozenset((
    "download_unavailable", "media_unavailable", "analysis_timeout",
    "analysis_error", "queue_unavailable",
))
REVISION_FAILURES = frozenset(("silent_audio", "unsupported_media", "resource_limit"))
SAFE_FAILURES = TRANSIENT_FAILURES | REVISION_FAILURES


from plugin.api import table

from . import migrations
from .catalog_enrichment import (
    float4,
    journal_edge_ref,
    record_profile_change,
    record_profile_deletions,
    serialize_profile,
)
from .edge_profile_store import edge_join


def migrate_attempts(cur):
    migrations.ensure_columns(
        cur, table('source_profiles'),
        "attempt_token TEXT",
        "attempt_media_signature TEXT",
        "attempt_catalog_epoch TEXT",
        "attempt_started_at TIMESTAMP",
        "attempt_analyzer_ver INTEGER",
        "attempt_profile_schema_ver INTEGER",
        "retry_category TEXT",
        "retry_count INTEGER NOT NULL DEFAULT 0",
        "retry_after TIMESTAMP",
        "retry_media_signature TEXT",
        "retry_analyzer_ver INTEGER",
        "retry_profile_schema_ver INTEGER",
    )


def _source_state(cur, source):
    cur.execute(
        f"""SELECT c.published_generation, c.catalog_epoch, s.rebind_status
              FROM {table('catalog_state')} c
              JOIN {table('catalog_sources')} s USING (catalog_instance_id)
             WHERE c.catalog_instance_id=%s FOR UPDATE OF c""",
        (source,),
    )
    row = cur.fetchone()
    if row is None or row[2] != "active":
        return None
    return int(row[0]), str(row[1])


def _current_revision(cur, source, generation, track_id):
    cur.execute(
        f"""SELECT media_fp FROM {table('catalog_tracks')}
             WHERE catalog_instance_id=%s AND published_generation=%s
               AND track_id=%s AND available=TRUE""",
        (source, generation, track_id),
    )
    row = cur.fetchone()
    return f"catalog-media:{row[0]}" if row and row[0] else None


def _withdraw(cur, source, track_id):
    cur.execute(
        f"DELETE FROM {table('published_source_profiles')} "
        "WHERE catalog_instance_id=%s AND track_id=%s RETURNING track_id",
        (source, track_id),
    )
    removed = cur.fetchone() is not None
    if removed:
        cur.execute(
            f"DELETE FROM {table('edge_profiles')} WHERE catalog_instance_id=%s AND track_id=%s",
            (source, track_id),
        )
        record_profile_change(cur, source, track_id, "deleted")
    return removed



def _known_different_signature(stored, current):
    """Only two nonempty, unequal revisions prove media replacement."""
    return bool(
        stored and current and stored not in (
            current, current.removeprefix("catalog-media:")
        )
    )


def _withdraw_changed(cur, source, track_id, revision):
    cur.execute(
        f"""SELECT media_signature FROM {table('published_source_profiles')}
             WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
        (source, track_id),
    )
    published = cur.fetchone()
    if published and _known_different_signature(published[0], revision):
        return _withdraw(cur, source, track_id)
    return False

def admit_attempts(db, source, ids, priority="background",
                   analyzer_version=1, schema_version=1, recovery_arm=None):
    """Claim exact published occurrences; a known new revision withdraws old data."""
    cur = db.cursor()
    tokens = {}
    try:
        state = _source_state(cur, source)
        if state is None:
            db.rollback()
            return tokens
        generation, epoch = state
        status = "pending_interactive" if priority == "interactive" else "pending"
        for track_id in sorted(set(ids)):
            revision = _current_revision(cur, source, generation, track_id)
            if revision is None:
                # An existing occurrence without a fingerprint is deferred once.
                # Its previous public baseline remains valid until evidence
                # establishes a changed revision or authoritative deletion.
                cur.execute(
                    f"""SELECT 1 FROM {table('catalog_tracks')}
                         WHERE catalog_instance_id=%s AND published_generation=%s
                           AND track_id=%s AND available=TRUE""",
                    (source, generation, track_id),
                )
                if cur.fetchone():
                    cur.execute(
                        f"""INSERT INTO {table('source_profiles')}
                            (catalog_instance_id, track_id, sample_rate, duration_ms,
                             ref_lufs, start_ramp, end_ramp, analyzer_ver,
                             profile_schema_ver, analyzed_at, status, last_error)
                           VALUES (%s, %s, 0, 0, 0, decode('', 'hex'),
                                   decode('', 'hex'), %s, %s, now(),
                                   'deferred_no_media_revision',
                                   'Catalogue media fingerprint unavailable')
                           ON CONFLICT (catalog_instance_id, track_id)
                           DO UPDATE SET status=EXCLUDED.status,
                               last_error=EXCLUDED.last_error,
                               analyzed_at=EXCLUDED.analyzed_at,
                               attempt_token=NULL, attempt_media_signature=NULL,
                               attempt_catalog_epoch=NULL,
                               attempt_analyzer_ver=NULL,
                               attempt_profile_schema_ver=NULL""",
                        (source, track_id, analyzer_version, schema_version),
                    )
                continue
            token = str(uuid4())
            cur.execute(
                f"""INSERT INTO {table('source_profiles')}
                    (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                     start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                     media_signature, analyzed_at, status, last_error,
                     attempt_token, attempt_media_signature, attempt_catalog_epoch,
                     attempt_started_at, attempt_analyzer_ver,
                     attempt_profile_schema_ver)
                   VALUES (%s, %s, 0, 0, 0, decode('', 'hex'), decode('', 'hex'),
                           %s, %s, %s, now(), %s, NULL, %s, %s, %s, now(),
                           %s, %s)
                   ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                       status=EXCLUDED.status, last_error=NULL,
                       analyzed_at=EXCLUDED.analyzed_at,
                       attempt_token=EXCLUDED.attempt_token,
                       attempt_media_signature=EXCLUDED.attempt_media_signature,
                       attempt_catalog_epoch=EXCLUDED.attempt_catalog_epoch,
                       attempt_started_at=EXCLUDED.attempt_started_at,
                       attempt_analyzer_ver=EXCLUDED.attempt_analyzer_ver,
                       attempt_profile_schema_ver=EXCLUDED.attempt_profile_schema_ver,
                       retry_count=CASE
                           WHEN {table('source_profiles')}.retry_media_signature
                                   IS DISTINCT FROM EXCLUDED.attempt_media_signature
                             OR {table('source_profiles')}.retry_analyzer_ver
                                   IS DISTINCT FROM EXCLUDED.attempt_analyzer_ver
                             OR {table('source_profiles')}.retry_profile_schema_ver
                                   IS DISTINCT FROM EXCLUDED.attempt_profile_schema_ver
                           THEN 0 ELSE {table('source_profiles')}.retry_count END,
                       retry_category=CASE
                           WHEN {table('source_profiles')}.retry_media_signature
                                   IS DISTINCT FROM EXCLUDED.attempt_media_signature
                           THEN NULL ELSE {table('source_profiles')}.retry_category END,
                       retry_after=NULL""",
                (source, track_id, analyzer_version, schema_version, revision, status,
                 token, revision, epoch, analyzer_version, schema_version),
            )
            cur.execute(
                f"""SELECT media_signature FROM {table('published_source_profiles')}
                    WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
                (source, track_id),
            )
            published = cur.fetchone()
            if published and _known_different_signature(published[0], revision):
                _withdraw(cur, source, track_id)
            tokens[track_id] = token
        if tokens and recovery_arm:
            recovery_arm(source, db=db, commit=False)
        db.commit()
        return tokens
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def release_attempts(db, source, tokens, reason, count_failure=True):
    if not tokens:
        return 0
    cur = db.cursor()
    try:
        count = 0
        for track_id, token in tokens.items():
            cur.execute(
                f"""UPDATE {table('source_profiles')}
                       SET status='stale', last_error='queue_unavailable',
                           analyzed_at=now(), attempt_token=NULL,
                           retry_category='queue_unavailable',
                           retry_count=retry_count + %s,
                           retry_after=CASE WHEN retry_count + %s < %s
                               THEN now() + interval '60 seconds' ELSE NULL END,
                           retry_media_signature=attempt_media_signature,
                           retry_analyzer_ver=attempt_analyzer_ver,
                           retry_profile_schema_ver=attempt_profile_schema_ver
                     WHERE catalog_instance_id=%s AND track_id=%s AND attempt_token=%s
                       AND status IN ('pending', 'pending_interactive')""",
                (int(count_failure), int(count_failure), RETRY_LIMIT,
                 source, track_id, token),
            )
            count += max(0, cur.rowcount)
        db.commit()
        return count
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def published_profile_current(db, source, track_id, analyzer_version, schema_version):
    """Whether the published row already represents the current media.

    True only for an active source whose published row has the current media
    fingerprint, analyzer and schema version, and whose attempt row has not
    failed. Anything else (new media, a new analyzer or schema, a failed or
    unpublished row) still goes through admission, so LUM-007 requalification
    is unchanged. Read-only.
    """
    cur = db.cursor()
    try:
        cur.execute(
            f"""SELECT 1
                  FROM {table('catalog_state')} c
                  JOIN {table('catalog_sources')} src USING (catalog_instance_id)
                  JOIN {table('catalog_tracks')} t
                    ON t.catalog_instance_id=c.catalog_instance_id
                   AND t.published_generation=c.published_generation
                  JOIN {table('published_source_profiles')} p
                    ON p.catalog_instance_id=t.catalog_instance_id
                   AND p.track_id=t.track_id
                  JOIN {table('source_profiles')} s
                    ON s.catalog_instance_id=p.catalog_instance_id
                   AND s.track_id=p.track_id
                 WHERE c.catalog_instance_id=%s AND t.track_id=%s
                   AND src.rebind_status='active' AND t.available=TRUE
                   AND COALESCE(t.media_fp, '') <> ''
                   AND p.media_signature='catalog-media:' || t.media_fp
                   AND p.analyzer_ver=%s AND p.profile_schema_ver=%s
                   AND s.status NOT IN ('failed', 'skipped_no_file')""",
            (source, track_id, analyzer_version, schema_version),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def complete_attempt(db, source, track_id, token, result, status, error, media_sig,
                     analyzer_version, schema_version, failure_code=None):
    """Publish only the currently admitted revision, row and journal together."""
    if not token:
        return False
    cur = db.cursor()
    try:
        state = _source_state(cur, source)
        if state is None:
            db.rollback()
            return False
        generation, epoch = state
        cur.execute(
            f"""SELECT attempt_token, attempt_media_signature, attempt_catalog_epoch,
                         attempt_analyzer_ver, attempt_profile_schema_ver
                  FROM {table('source_profiles')}
                 WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
            (source, track_id),
        )
        attempt = cur.fetchone()
        if (
            not attempt or attempt[0] != token or attempt[2] != epoch
            or attempt[3] != analyzer_version or attempt[4] != schema_version
        ):
            db.rollback()
            return False
        revision = _current_revision(cur, source, generation, track_id)
        if not revision or revision != attempt[1]:
            cur.execute(
                f"""UPDATE {table('source_profiles')}
                       SET status='stale', last_error='Catalogue media revision changed',
                           attempt_token=NULL
                     WHERE catalog_instance_id=%s AND track_id=%s""",
                (source, track_id),
            )
            # A temporarily missing fingerprint is not proof that the media
            # changed. Catalogue publication handles authoritative deletion.
            cur.execute(
                f"""SELECT available FROM {table('catalog_tracks')}
                     WHERE catalog_instance_id=%s AND published_generation=%s
                       AND track_id=%s""",
                (source, generation, track_id),
            )
            current_track = cur.fetchone()
            if current_track is None or not current_track[0]:
                _withdraw(cur, source, track_id)
            elif revision:
                _withdraw_changed(cur, source, track_id, revision)
            db.commit()
            return False
        if status == "ready" and media_sig != revision:
            db.rollback()
            return False
        values = (
            int(getattr(result, "sample_rate", 0)),
            int(getattr(result, "duration_ms", 0)),
            # Stored as REAL: compare and insert at that precision (AUD-03).
            float4(getattr(result, "ref_lufs", 0.0)),
            bytes(getattr(result, "start_ramp_blob", b"")),
            bytes(getattr(result, "end_ramp_blob", b"")),
            analyzer_version, schema_version, media_sig,
        )
        code = failure_code if failure_code in SAFE_FAILURES else "analysis_error"
        safe_error = None if status == "ready" else code
        cur.execute(
            f"""UPDATE {table('source_profiles')}
                   SET sample_rate=%s, duration_ms=%s, ref_lufs=%s,
                       start_ramp=%s, end_ramp=%s, analyzer_ver=%s,
                       profile_schema_ver=%s, media_signature=%s, analyzed_at=now(),
                       status=%s, last_error=%s, attempt_token=NULL
                 WHERE catalog_instance_id=%s AND track_id=%s""",
            (*values, status, safe_error, source, track_id),
        )
        if status == "ready":
            cur.execute(
                f"""UPDATE {table('source_profiles')}
                       SET retry_category=NULL, retry_count=0, retry_after=NULL,
                           retry_media_signature=NULL, retry_analyzer_ver=NULL,
                           retry_profile_schema_ver=NULL
                     WHERE catalog_instance_id=%s AND track_id=%s""",
                (source, track_id),
            )
        else:
            cur.execute(
                f"""UPDATE {table('source_profiles')}
                       SET retry_category=%s, retry_count=retry_count+1,
                           retry_after=CASE
                               WHEN %s AND retry_count+1 < %s
                               THEN now() + make_interval(secs => CASE retry_count
                                   WHEN 0 THEN %s WHEN 1 THEN %s ELSE %s END)
                               ELSE NULL END,
                           retry_media_signature=%s, retry_analyzer_ver=%s,
                           retry_profile_schema_ver=%s
                     WHERE catalog_instance_id=%s AND track_id=%s""",
                (code, code in TRANSIENT_FAILURES, RETRY_LIMIT,
                 *RETRY_DELAYS_SECONDS, attempt[1], analyzer_version,
                 schema_version, source, track_id),
            )
        if status == "ready":
            cur.execute(
                f"""SELECT sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                           analyzer_ver, profile_schema_ver, media_signature, analyzed_at
                      FROM {table('published_source_profiles')}
                     WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
                (source, track_id),
            )
            previous = cur.fetchone()
            same = previous is not None and (
                (int(previous[0]), int(previous[1]), float4(previous[2]),
                 bytes(previous[3]), bytes(previous[4]), int(previous[5]),
                 int(previous[6]), previous[7]) == values
            )
            # The edge profile depends on the media, not on the waveform row.
            media_changed = previous is None or previous[7] != media_sig
            if not same and media_changed:
                # New media removes the old edge representation and its
                # queued token before emitting the new public payload.
                cur.execute(
                    f"DELETE FROM {table('edge_profiles')} "
                    "WHERE catalog_instance_id=%s AND track_id=%s",
                    (source, track_id),
                )
                cur.execute(
                    f"DELETE FROM {table('edge_profile_jobs')} "
                    "WHERE catalog_instance_id=%s AND track_id=%s",
                    (source, track_id),
                )
            if not same:
                cur.execute(
                    f"""INSERT INTO {table('published_source_profiles')}
                        (catalog_instance_id, track_id, sample_rate, duration_ms,
                         ref_lufs, start_ramp, end_ramp, analyzer_ver,
                         profile_schema_ver, media_signature, analyzed_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                       ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                           sample_rate=EXCLUDED.sample_rate,
                           duration_ms=EXCLUDED.duration_ms,
                           ref_lufs=EXCLUDED.ref_lufs,
                           start_ramp=EXCLUDED.start_ramp,
                           end_ramp=EXCLUDED.end_ramp,
                           analyzer_ver=EXCLUDED.analyzer_ver,
                           profile_schema_ver=EXCLUDED.profile_schema_ver,
                           media_signature=EXCLUDED.media_signature,
                           analyzed_at=EXCLUDED.analyzed_at
                       RETURNING analyzed_at""",
                    (source, track_id, *values),
                )
                stamp = cur.fetchone()[0]
                payload = serialize_profile(track_id, *values[:6], stamp, media_sig)
                edge_ref = None
                if not media_changed:
                    # Clients delete their edge on an upsert without one, so a
                    # waveform-only change carries the still-current edge: the
                    # journal references it (K6) and readers embed it, or send
                    # the reference to clients that opted in. Only its key is
                    # read; the edge payload is never detoasted here.
                    cur.execute(
                        f"""SELECT edge.media_revision, edge.profile_digest
                              FROM {table('published_source_profiles')} p
                              {edge_join(columns='e.media_revision, e.profile_digest')}
                             WHERE p.catalog_instance_id=%s AND p.track_id=%s""",
                        (source, track_id),
                    )
                    revision, digest = cur.fetchone()
                    # serialize_profile embedded an edge only for the row's
                    # own revision (edge columns equal their payload's).
                    if digest and revision and revision == payload["media_revision"]:
                        edge_ref = journal_edge_ref(digest, kept=True)
                record_profile_change(cur, source, track_id, "ready", payload,
                                      edge_ref=edge_ref)
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def _revision_lookup(cur, source, generation, changed):
    """What the generation says about each changed track (one statement).

    Returns ``(track_ids, gone, media_fps)``: ``gone`` when the change is a
    deletion or the track is missing from or unavailable in the generation.
    """
    track_ids = list(changed)
    cur.execute(
        f"""SELECT c.track_id,
                   c.deleted OR t.track_id IS NULL OR NOT t.available,
                   t.media_fp
              FROM unnest(%s::text[], %s::boolean[]) WITH ORDINALITY
                   AS c(track_id, deleted, ordinality)
              LEFT JOIN LATERAL (
                  SELECT track_id, available, media_fp
                    FROM {table('catalog_tracks')}
                   WHERE catalog_instance_id=%s AND published_generation=%s
                     AND track_id=c.track_id
                   LIMIT 1
              ) t ON TRUE
             ORDER BY c.ordinality""",
        (track_ids, [changed[track_id] == "delete" for track_id in track_ids],
         source, generation),
    )
    rows = cur.fetchall()
    return [row[0] for row in rows], [bool(row[1]) for row in rows], [row[2] for row in rows]


def plan_catalog_invalidation(cur, source, generation, track_changes, *, full_reconcile=False):
    """Read, for a catalogue publication, what its generation says (P2-3).

    The generation's rows are the publisher's own, so this can run before
    ``catalog_state`` is locked; ``withdraw_catalog_changes`` then compares
    them with the profiles under the lock. A fingerprint-schema rebase
    (``full_reconcile``) compares every published profile, and those can
    change until the lock is held, so its lookup is left to the withdrawal.
    """
    cur.execute("SELECT to_regclass(%s)", (table("published_source_profiles"),))
    publication_table = cur.fetchone()
    if not publication_table or publication_table[0] is None:
        return None
    changed = {}
    for entity_type, track_id, operation, *_rest in track_changes:
        if entity_type == "track":
            changed[str(track_id)] = operation
    plan = {
        "source": source,
        "generation": generation,
        "full_reconcile": bool(full_reconcile),
        "changed": changed,
        "lookup": None,
    }
    if changed and not full_reconcile:
        plan["lookup"] = _revision_lookup(cur, source, generation, changed)
    return plan


# An occurrence is known stale when its track is gone from the generation, or
# _known_different_signature(signature, 'catalog-media:' || media_fp).
_STALE_OCCURRENCE = """(
    c.gone OR (COALESCE({signature}, '') <> '' AND COALESCE(c.media_fp, '') <> ''
               AND {signature} <> 'catalog-media:' || c.media_fp
               AND {signature} <> c.media_fp))"""


def withdraw_catalog_changes(cur, plan):
    """Stale the attempts and withdraw the publications a plan finds stale.

    Runs under the publication's ``catalog_state`` row lock, set-based: a
    fixed number of statements for any number of changed tracks. It writes
    the rows and journal events the per-track version (before P2-3) wrote,
    except the withdrawn tracks' edge payloads, which ``purge_withdrawn_edges``
    deletes. Attempt rows are locked in track-ID order, as before. Returns
    the withdrawn track IDs in that order.
    """
    if plan is None:
        return []
    source = plan["source"]
    changed = dict(plan["changed"])
    lookup = plan["lookup"]
    if plan["full_reconcile"]:
        # A fingerprint-schema rebase suppresses ordinary catalog events, so
        # compare every existing publication against the new generation.
        cur.execute(
            f"SELECT track_id FROM {table('published_source_profiles')} "
            "WHERE catalog_instance_id=%s",
            (source,),
        )
        for (track_id,) in cur.fetchall():
            changed.setdefault(str(track_id), "upsert")
        cur.execute(
            f"""UPDATE {table('source_profiles')}
                   SET status='stale', last_error='Catalogue epoch changed',
                       attempt_token=NULL
                 WHERE catalog_instance_id=%s AND attempt_token IS NOT NULL""",
            (source,),
        )
        if changed:
            lookup = _revision_lookup(cur, source, plan["generation"], changed)
    if not changed:
        return []
    track_ids, gone, media_fps = lookup
    cur.execute(
        f"""WITH c AS MATERIALIZED (
                SELECT * FROM unnest(%(track_ids)s::text[], %(gone)s::boolean[],
                                     %(media_fps)s::text[]) AS c(track_id, gone, media_fp)
            ), stale AS MATERIALIZED (
                SELECT s.track_id
                  FROM {table('source_profiles')} s
                  JOIN c ON s.catalog_instance_id=%(source)s AND s.track_id=c.track_id
                 WHERE {_STALE_OCCURRENCE.format(
                     signature="COALESCE(NULLIF(s.attempt_media_signature, ''), s.media_signature)"
                 )}
                 ORDER BY s.track_id COLLATE "C"
                   FOR UPDATE OF s
            ), staled AS (
                UPDATE {table('source_profiles')} s
                   SET status='stale',
                       last_error='Catalogue media revision changed or track removed',
                       attempt_token=NULL
                  FROM stale
                 WHERE s.catalog_instance_id=%(source)s AND s.track_id=stale.track_id
            ), withdrawn AS (
                DELETE FROM {table('published_source_profiles')} p
                 USING c
                 WHERE p.catalog_instance_id=%(source)s AND p.track_id=c.track_id
                   AND {_STALE_OCCURRENCE.format(signature="p.media_signature")}
             RETURNING p.track_id
            )
            SELECT track_id FROM withdrawn ORDER BY track_id COLLATE "C" """,
        {"source": source, "track_ids": track_ids, "gone": gone, "media_fps": media_fps},
    )
    withdrawn = [row[0] for row in cur.fetchall()]
    record_profile_deletions(cur, source, withdrawn)
    return withdrawn


def purge_withdrawn_edges(cur, source, track_ids=None):
    """Delete edge payloads that no published waveform row reaches.

    An edge is published only for a published waveform row with the same
    media signature and is read only through one (``edge_join``). Once that
    row is withdrawn or replaced the edge is unreachable, and it never becomes
    reachable again: a waveform published for a track without a published
    row drops the track's edges first (``complete_attempt``). So the deletion
    can follow a withdrawal in a later transaction, after catalog_state is
    released (P2-3), and the signature guard keeps any edge that is current.

    The one exception is the upgrade from 1.2.5, which had no published rows:
    ``migrate`` seeds them from the ready source profiles (marker
    ``published_source_profiles_seed_v1``), and that makes the existing edges
    reachable again. So the sweep may run only after that seed, which is
    where ``migrate`` calls ``compact_enrichment_storage``.

    ``track_ids`` limits it to those tracks. ``None`` sweeps the whole source,
    which repairs a purge that never ran; the publication runs that sweep
    after it commits, and ``compact_enrichment_storage`` in maintenance.
    """
    if track_ids is not None:
        track_ids = [str(track_id) for track_id in track_ids]
        if not track_ids:
            return 0
    cur.execute(
        f"""DELETE FROM {table('edge_profiles')} e
             WHERE e.catalog_instance_id=%s
               AND (%s::text[] IS NULL OR e.track_id = ANY(%s::text[]))
               AND NOT EXISTS (
                   SELECT 1 FROM {table('published_source_profiles')} p
                    WHERE p.catalog_instance_id=e.catalog_instance_id
                      AND p.track_id=e.track_id
                      AND p.media_signature=e.media_signature)""",
        (source, track_ids, track_ids),
    )
    return max(0, int(getattr(cur, "rowcount", 0) or 0))


def invalidate_catalog_changes(cur, source, generation, track_changes, *, full_reconcile=False):
    """Withdraw known changed/deleted occurrences inside catalogue publication.

    Plan, withdrawal and edge purge in the caller's transaction: the rows and
    journal events of the per-track version, from a fixed number of
    statements. Returns the number of withdrawn publications.
    """
    plan = plan_catalog_invalidation(
        cur, source, generation, track_changes, full_reconcile=full_reconcile
    )
    withdrawn = withdraw_catalog_changes(cur, plan)
    purge_withdrawn_edges(cur, source, withdrawn)
    return len(withdrawn)


def rekey_published_profiles(cur, source, tracks):
    """Move exact-source public identity and journal old/new keys atomically."""
    moved = 0
    for mapping in sorted(tracks, key=lambda item: str(item["old_id"])):
        old_id = str(mapping["old_id"])
        new_id = str(mapping["new_id"])
        if old_id == new_id:
            continue
        cur.execute(
            f"""SELECT sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                       analyzer_ver, analyzed_at, media_signature
                  FROM {table('published_source_profiles')}
                 WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
            (source, old_id),
        )
        row = cur.fetchone()
        # Old edge jobs and payloads contain the old track identity and digest.
        # Re-analysis can publish a correctly keyed edge representation later.
        cur.execute(
            f"DELETE FROM {table('edge_profile_jobs')} "
            "WHERE catalog_instance_id=%s AND track_id=%s",
            (source, old_id),
        )
        cur.execute(
            f"DELETE FROM {table('edge_profiles')} "
            "WHERE catalog_instance_id=%s AND track_id=%s",
            (source, old_id),
        )
        if row is None:
            continue
        cur.execute(
            f"""UPDATE {table('published_source_profiles')}
                   SET track_id=%s
                 WHERE catalog_instance_id=%s AND track_id=%s""",
            (new_id, source, old_id),
        )
        record_profile_change(cur, source, old_id, "deleted")
        record_profile_change(
            cur, source, new_id, "ready",
            serialize_profile(new_id, *row),
        )
        moved += 1
    return moved
