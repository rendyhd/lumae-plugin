"""The loudness backfill (Lumae LN.4c; contract §3.9 "Publication", Auralscape D8).

A library pass per source, one turn per reconcile tick (until its deadline) after the tick's other
track work, so
it runs only when no interactive or analysis work is waiting: music tracks of the published
catalogue without a loudness record of the current version for their current media are measured on
the standalone path (``loudness_v1.measure_file``; above 48 kHz through a conversion): one download
and one decode per track. A track the edge pass will still decode waits for it (the edge pass
measures in the same decode), so with edges on the backfill takes a track only once the edge pass is done with its
current media: its edge exists, the edge pass cannot take it (no published waveform profile of that
media) or gave up on it (unsupported, or failed and backing off). An edge flagged for its v3
recheck (SF.3) is still due in the edge pass, so it waits too. These are the edge pass's own
predicates (``edge_profile_store``), legacy raw-fingerprint signatures included. A turn publishes its
measurements together, so clients see few large pages rather than many small ones.

Each turn first follows the published catalogue (``loudness_stream.reconcile``) when its generation
moved since the last turn: records of tracks that left it are withdrawn, album records pooled again.
A failed track backs off (``RETRY_HOURS``); a finished pass sweeps again after ``RESWEEP_HOURS``.

W3 (runner plan 2026-10-25): a track the catalogue says is longer than the meter's limit
(``loudness_v1.MAX_SECONDS``) is never downloaded; a failure the analyzer calls unsupported parks
at once, a decode error after its second attempt, any other after the last step of
``RETRY_HOURS``. A parked track is due again only when its media changes (a new revision).
Before, each was downloaded whole and retried every 72 hours forever (four 1–2 GB FLACs on the
owner's server).
"""

from plugin.api import table

from . import migrations
from .loudness_v1 import MAX_SECONDS, VERSION

# A turn works until the reconcile tick's deadline, a page of the admin batch size at a time.
RESWEEP_HOURS = 6
# Back-off after the 1st, 2nd, 3rd and 4th failure of the same media; the next one parks.
RETRY_HOURS = (1, 6, 24, 72)
# W3: a decode error (``InvalidDataError``) parks after this many attempts.
DECODE_ERROR_ATTEMPTS = 2
DECODE_ERROR_HINTS = ("edge-media-error", "InvalidDataError")
# The catalogue's duration above which the meter cannot measure a file (W3).
MAX_DURATION_MS = int(MAX_SECONDS * 1000)


_RUNTIME = None


def t(name):
    return table(name)


def runtime_available():
    """PyAV imports in this process (the standalone path's decoder); checked once."""
    global _RUNTIME
    if _RUNTIME is None:
        try:
            import av  # noqa: F401
            _RUNTIME = True
        except Exception:  # noqa: BLE001 - any import failure means no decoder
            _RUNTIME = False
    return _RUNTIME


def migrate_backfill(db):
    cur = db.cursor()
    migrations.apply(cur, [
        # ``retry_count`` and ``next_retry_at`` follow the reconcile watchdog's retry contract.
        f"""
        CREATE TABLE IF NOT EXISTS {t('loudness_backfill_state')} (
            catalog_instance_id TEXT PRIMARY KEY
                REFERENCES {t('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            cursor TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'queued',
            processed BIGINT NOT NULL DEFAULT 0,
            reconciled_generation BIGINT NOT NULL DEFAULT -1,
            retry_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at TIMESTAMP,
            pass_started_at TIMESTAMP,
            completed_at TIMESTAMP,
            last_error TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('loudness_attempts')} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            media_revision TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            next_attempt_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, track_id)
        )
        """,
    ])
    cur.close()


_REVISION = "'sha256:' || encode(sha256(convert_to('catalog-media:' || m.media_fp, 'UTF8')), 'hex')"
# A waveform profile published before catalogue signatures carries the bare fingerprint, and so do its
# edge and edge job (``edge_profile_store`` accepts both).
_RAW_REVISION = "'sha256:' || encode(sha256(convert_to(m.media_fp, 'UTF8')), 'hex')"


def _due_sql(edges_enabled):
    """Music tracks of the published generation that need a measurement now (``%s``: the source)."""
    edge_settled = "TRUE" if not edges_enabled else f"""(
        EXISTS (SELECT 1 FROM {t('edge_profiles')} e
                 WHERE e.catalog_instance_id=m.catalog_instance_id AND e.track_id=m.track_id
                   AND e.media_revision IN ({_REVISION}, {_RAW_REVISION}) AND NOT e.v3_recheck)
        OR NOT EXISTS (SELECT 1 FROM {t('published_source_profiles')} p
                        WHERE p.catalog_instance_id=m.catalog_instance_id AND p.track_id=m.track_id
                          AND p.media_signature IN (m.media_fp, 'catalog-media:' || m.media_fp))
        OR EXISTS (SELECT 1 FROM {t('edge_profile_jobs')} j
                    WHERE j.catalog_instance_id=m.catalog_instance_id AND j.track_id=m.track_id
                      AND j.media_revision IN ({_REVISION}, {_RAW_REVISION})
                      AND (j.status='unsupported'
                           OR (j.status='failed' AND j.last_error IS DISTINCT FROM 'edge-enqueue-failed'))))"""
    return f"""{t('catalog_state')} c
        JOIN {t('catalog_tracks')} m
          ON m.catalog_instance_id=c.catalog_instance_id AND m.published_generation=c.published_generation
       WHERE c.catalog_instance_id=%s AND m.available AND m.analysis_eligible IS TRUE
         AND m.media_fp IS NOT NULL AND m.media_fp <> ''
         AND (m.duration_ms IS NULL OR m.duration_ms <= {MAX_DURATION_MS})
         AND NOT EXISTS (SELECT 1 FROM {t('loudness_tracks')} l
                          WHERE l.catalog_instance_id=m.catalog_instance_id AND l.track_id=m.track_id
                            AND l.version={int(VERSION)} AND l.media_revision={_REVISION})
         AND NOT EXISTS (SELECT 1 FROM {t('loudness_attempts')} a
                          WHERE a.catalog_instance_id=m.catalog_instance_id AND a.track_id=m.track_id
                            AND a.media_revision={_REVISION} AND a.next_attempt_at > now())
         AND {edge_settled}"""


def candidates(db, catalog_instance_id, after="", limit=10, *, edges_enabled=True):
    """``[(track_id, media_revision)]`` due after ``after`` in track id order."""
    cur = db.cursor()
    cur.execute(
        f"SELECT m.track_id, {_REVISION} FROM {_due_sql(edges_enabled)} AND m.track_id > %s "
        "ORDER BY m.track_id LIMIT %s",
        (catalog_instance_id, after or "", max(1, int(limit))),
    )
    rows = [(str(track), str(revision)) for track, revision in cur.fetchall()]
    cur.close()
    return rows


def ensure_sources(db):
    """Give every active source a pass row (a new row is due now); returns rows added."""
    cur = db.cursor()
    cur.execute(f"""INSERT INTO {t('loudness_backfill_state')} (catalog_instance_id)
        SELECT catalog_instance_id FROM {t('catalog_sources')} WHERE rebind_status='active'
        ON CONFLICT (catalog_instance_id) DO NOTHING""")
    added = max(0, cur.rowcount or 0)
    db.commit()
    cur.close()
    return added


def next_due(db, server_id=None):
    """``(server_id, catalog_instance_id)`` of the next source with a due pass or a catalogue to
    follow, or None."""
    ensure_sources(db)
    cur = db.cursor()
    cur.execute(f"""SELECT s.current_core_server_id, st.catalog_instance_id
        FROM {t('loudness_backfill_state')} st
        JOIN {t('catalog_sources')} s USING (catalog_instance_id)
        JOIN {t('catalog_state')} c USING (catalog_instance_id)
        WHERE s.rebind_status='active'
          AND (%s IS NULL OR s.current_core_server_id=%s)
          AND ((c.published_generation <> st.reconciled_generation
                AND (st.status <> 'failed' OR st.next_retry_at IS NULL OR st.next_retry_at <= now()))
               OR ((st.status IN ('queued', 'failed', 'complete')
                    OR (st.status='running' AND st.updated_at < now()-interval '30 minutes'))
                   AND (st.next_retry_at IS NULL OR st.next_retry_at <= now())))
        ORDER BY st.next_retry_at NULLS FIRST, st.catalog_instance_id LIMIT 1""",
                (server_id, server_id))
    row = cur.fetchone()
    cur.close()
    db.commit()
    return tuple(row) if row else None


def state(db, catalog_instance_id):
    """The pass row, with the published generation and whether the pass itself is due."""
    cur = db.cursor()
    cur.execute(f"""SELECT st.status, st.cursor, st.processed, st.reconciled_generation, c.published_generation,
               ((st.status IN ('queued', 'failed', 'complete')
                 OR (st.status='running' AND st.updated_at < now()-interval '30 minutes'))
                AND (st.next_retry_at IS NULL OR st.next_retry_at <= now()))
        FROM {t('loudness_backfill_state')} st JOIN {t('catalog_state')} c USING (catalog_instance_id)
        WHERE st.catalog_instance_id=%s""", (catalog_instance_id,))
    row = cur.fetchone()
    cur.close()
    db.commit()
    keys = ("status", "cursor", "processed", "reconciled_generation", "published_generation", "pass_due")
    return dict(zip(keys, row)) if row else None


def update(db, catalog_instance_id, status, *, cursor=None, processed=0, next_retry_hours=None, error=None,
           started=False, completed=False, reconciled_generation=None):
    cur = db.cursor()
    cur.execute(f"""UPDATE {t('loudness_backfill_state')} SET status=%s,
            cursor=COALESCE(%s, cursor), processed=processed+%s,
            reconciled_generation=COALESCE(%s, reconciled_generation),
            retry_count=CASE WHEN %s='failed' THEN retry_count ELSE 0 END,
            next_retry_at=CASE WHEN %s IS NULL THEN NULL ELSE now()+make_interval(hours => %s) END,
            last_error=%s,
            pass_started_at=CASE WHEN %s THEN now() ELSE pass_started_at END,
            completed_at=CASE WHEN %s THEN now() ELSE completed_at END,
            updated_at=now()
        WHERE catalog_instance_id=%s""",
                (status, cursor, int(processed), reconciled_generation, status, next_retry_hours,
                 next_retry_hours or 0, str(error)[:500] if error else None, bool(started), bool(completed),
                 catalog_instance_id))
    db.commit()
    cur.close()


def arm(db, catalog_instance_id=None, include_complete=True):
    """Make a parked pass, and unless ``include_complete`` is False a finished one, due now (a
    catalogue refresh: new tracks; a worker's runtime report: parked passes only)."""
    statuses = ["complete", "waiting_runtime"] if include_complete else ["waiting_runtime"]
    cur = db.cursor()
    cur.execute(f"""UPDATE {t('loudness_backfill_state')} SET status='queued', next_retry_at=NULL,
            cursor=CASE WHEN status='complete' THEN '' ELSE cursor END, updated_at=now()
        WHERE status=ANY(%s) AND (%s IS NULL OR catalog_instance_id=%s)""",
                (statuses, catalog_instance_id, catalog_instance_id))
    armed = max(0, cur.rowcount or 0)
    db.commit()
    cur.close()
    return armed


def park(db, catalog_instance_id):
    """No PyAV on this worker: wait for a worker's runtime report (``arm``)."""
    update(db, catalog_instance_id, "waiting_runtime")


def record_failure(db, catalog_instance_id, track_id, media_revision, error, *, unsupported=False):
    """Back the track off; the same media's next failure waits longer, and W3 parks it
    (``next_attempt_at`` infinity) once it cannot succeed: ``unsupported`` at once, a decode
    error after DECODE_ERROR_ATTEMPTS, any failure after the last step of RETRY_HOURS."""
    reason = str(error)[:300]
    decode = any(hint in reason for hint in DECODE_ERROR_HINTS)
    attempts = "CASE WHEN a.media_revision=EXCLUDED.media_revision THEN a.attempts + 1 ELSE 1 END"
    cur = db.cursor()
    cur.execute(f"""
        INSERT INTO {t('loudness_attempts')} AS a
            (catalog_instance_id, track_id, media_revision, attempts, last_error, next_attempt_at)
        VALUES (%(source)s, %(track)s, %(revision)s, 1, %(reason)s,
                CASE WHEN %(unsupported)s THEN 'infinity'::timestamp
                     ELSE now() + make_interval(hours => %(first)s) END)
        ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
            attempts={attempts},
            media_revision=EXCLUDED.media_revision, last_error=EXCLUDED.last_error, updated_at=now(),
            next_attempt_at=CASE
                WHEN %(unsupported)s OR {attempts} > %(steps)s
                     OR (%(decode)s AND {attempts} >= %(decode_attempts)s)
                    THEN 'infinity'::timestamp
                ELSE now() + make_interval(hours => (%(hours)s::int[])[LEAST({attempts}, %(steps)s)])
            END
        """, {"source": catalog_instance_id, "track": track_id, "revision": media_revision,
              "reason": reason, "unsupported": bool(unsupported), "first": RETRY_HOURS[0],
              "steps": len(RETRY_HOURS), "decode": decode,
              "decode_attempts": DECODE_ERROR_ATTEMPTS, "hours": list(RETRY_HOURS)})
    db.commit()
    cur.close()


def parked_counts(db, catalog_instance_id):
    """W3's parked work for the status page: ``too_long`` (never downloaded) and ``parked``
    (failed for good on its current media)."""
    cur = db.cursor()
    cur.execute(f"""
        SELECT (SELECT count(*) FROM {t('catalog_state')} c
                  JOIN {t('catalog_tracks')} m ON m.catalog_instance_id=c.catalog_instance_id
                   AND m.published_generation=c.published_generation
                 WHERE c.catalog_instance_id=%s AND m.available AND m.analysis_eligible IS TRUE
                   AND m.duration_ms > {MAX_DURATION_MS}),
               (SELECT count(*) FROM {t('loudness_attempts')}
                 WHERE catalog_instance_id=%s AND next_attempt_at='infinity'::timestamp)
        """, (catalog_instance_id, catalog_instance_id))
    too_long, parked = cur.fetchone()
    cur.close()
    return {"too_long": int(too_long or 0), "parked": int(parked or 0)}


def clear_failures(cur, catalog_instance_id, track_ids):
    """Forget the back-off of tracks just measured. The caller commits (with their publication)."""
    cur.execute(f"DELETE FROM {t('loudness_attempts')} WHERE catalog_instance_id=%s AND track_id = ANY(%s::text[])",
                (catalog_instance_id, [str(track) for track in track_ids]))


def follow_catalogue(db, catalog_instance_id, published_generation):
    """``loudness_stream.reconcile`` for a moved generation, then remember it; returns its events.
    Back-off rows of tracks that left the catalogue go with it."""
    from .loudness_stream import reconcile

    cur = db.cursor()
    try:
        events = reconcile(cur, catalog_instance_id)
        cur.execute(f"""DELETE FROM {t('loudness_attempts')} a
            WHERE a.catalog_instance_id=%s AND NOT EXISTS (
                SELECT 1 FROM {t('catalog_state')} c JOIN {t('catalog_tracks')} m
                  ON m.catalog_instance_id=c.catalog_instance_id AND m.published_generation=c.published_generation
                 WHERE c.catalog_instance_id=a.catalog_instance_id AND m.track_id=a.track_id AND m.available)""",
                    (catalog_instance_id,))
        cur.execute(f"UPDATE {t('loudness_backfill_state')} SET reconciled_generation=%s, updated_at=now() "
                    "WHERE catalog_instance_id=%s", (int(published_generation), catalog_instance_id))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()
    return events
