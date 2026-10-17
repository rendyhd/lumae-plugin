"""Additive edge storage and independently retryable upgrade jobs.

Never changes the legacy profile's readiness. The legacy row is locked before
publishing a measurement, so a completed old job cannot attach to a new source.
"""
import json
import time
import uuid
from datetime import timedelta, timezone

from plugin.api import table

from . import migrations
from .edge_profiles import (
    METHOD,
    SCHEMA_VERSION,
    V3_ENDING_KEY,
    canonical_json,
    edge_profile_v3,
    opaque_revision,
    profile_digest,
    tail_opens_below_floor,
)


def migrate_edge_profiles(db):
    cur = db.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('edge_profiles')} (
        catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
        track_id TEXT NOT NULL,
        media_revision TEXT NOT NULL,
        representation_id TEXT NOT NULL,
        media_signature TEXT NOT NULL,
        profile_digest TEXT NOT NULL,
        payload JSONB NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (catalog_instance_id, track_id, media_revision, representation_id)
    )""")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('edge_profile_jobs')} (
        catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
        track_id TEXT NOT NULL,
        media_revision TEXT NOT NULL,
        job_token TEXT NOT NULL,
        status TEXT NOT NULL,
        last_error TEXT,
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (catalog_instance_id, track_id)
    )""")
    # The settings poll counts a source's edges and reads the newest one
    # (edge_profile_status): an index-only scan of this narrow index instead
    # of two scans of the wide table.
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS {table('edge_profiles')}_updated_idx "
        f"ON {table('edge_profiles')} (catalog_instance_id, updated_at)",
    )
    # On-demand requests promote a queued job; background batches serve
    # interactive jobs first.
    migrations.ensure_columns(
        cur, table('edge_profile_jobs'), "priority TEXT NOT NULL DEFAULT 'background'",
    )
    # The analysis worker reports its own edge runtime: the web process that
    # answers health and queues jobs usually has no PyAV.
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('edge_runtime_state')} (
        worker_id TEXT PRIMARY KEY,
        available BOOLEAN NOT NULL,
        status JSONB NOT NULL,
        reported_at TIMESTAMP NOT NULL DEFAULT now()
    )""")
    # The server-driven library pass, one row per source. ``retry_count`` and
    # ``next_retry_at`` follow the reconcile watchdog's retry contract.
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('edge_backfill_state')} (
        catalog_instance_id TEXT PRIMARY KEY REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
        cursor TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'queued',
        processed BIGINT NOT NULL DEFAULT 0,
        retry_count INTEGER NOT NULL DEFAULT 0,
        next_retry_at TIMESTAMP,
        pass_started_at TIMESTAMP,
        completed_at TIMESTAMP,
        last_error TEXT,
        updated_at TIMESTAMP NOT NULL DEFAULT now()
    )""")
    # On-demand work waiting for a worker, and health's queue counts, read
    # these small partial indexes instead of scanning the jobs table.
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS {table('edge_profile_jobs')}_interactive_idx "
        f"ON {table('edge_profile_jobs')} (updated_at) "
        f"WHERE status='pending' AND priority='interactive'",
    )
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS {table('edge_profile_jobs')}_active_idx "
        f"ON {table('edge_profile_jobs')} (status) WHERE status IN ('pending', 'running')",
    )
    # Schema v3 (1.7.0, SF.3): ``payload`` stays the v2 edge exactly. Beside it,
    # the v3 digest (``edge_profile_v3``; NULL until computed, see
    # upgrade_stored_edges), the whole-file music end when there is one (D9:
    # ``{"music_end_frame", "ending"}``), and ``v3_recheck``: an edge measured
    # before 1.7.0 whose tail window opens below the excerpt floor, analysed
    # again so its music end can be found. Additive; 1.6.1 ignores them.
    migrations.ensure_columns(
        cur, table('edge_profiles'),
        "profile_digest_v3 TEXT",
        "v3_ending JSONB",
        "v3_recheck BOOLEAN NOT NULL DEFAULT FALSE",
    )
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS {table('edge_profiles')}_v3_pending_idx "
        f"ON {table('edge_profiles')} (catalog_instance_id, track_id) WHERE profile_digest_v3 IS NULL",
    )
    cur.close()


def edge_payload_sql(alias='e', edge_schema=2):
    """SQL: the served edge of the ``edge_profiles`` row ``alias``.

    Schema 2 is the stored payload. Schema 3 (contract §4.5) is its projection:
    the windows without ``peak_cdb`` and ``spectral_flux_q15``, the D9 music end
    when stored, and the v3 digest; NULL while the v3 digest is not computed
    (health lists schema 3 only once every edge has one) and for a stored edge
    that is not a measurable v2 edge (an empty v3 digest).
    """
    if edge_schema != 3:
        return f"{alias}.payload"
    return f"""(CASE WHEN COALESCE({alias}.profile_digest_v3, '') = '' THEN NULL ELSE
        ({alias}.payload #- '{{head,peak_cdb}}' #- '{{head,spectral_flux_q15}}'
                         #- '{{tail,peak_cdb}}' #- '{{tail,spectral_flux_q15}}')
        || jsonb_build_object('schema_version', 3, 'profile_digest', {alias}.profile_digest_v3)
        || CASE WHEN {alias}.v3_ending IS NULL THEN '{{}}'::jsonb ELSE jsonb_build_object(
               'ending', {alias}.v3_ending->'ending',
               'landmarks', ({alias}.payload->'landmarks')
                   || jsonb_build_object('music_end_frame', {alias}.v3_ending->'music_end_frame'))
           END
    END)"""


def edge_v3_digest_sql(track, media_revision, digest, catalog):
    """SQL: the v3 digest of the edge a K6 reference names by its v2 digest,
    or that v2 digest when the edge is gone or has none yet (the client then
    fetches it, as for any reference it does not hold)."""
    return f"""COALESCE((SELECT NULLIF(e.profile_digest_v3, '') FROM {table('edge_profiles')} e
         WHERE e.catalog_instance_id={catalog} AND e.track_id={track}
           AND e.media_revision={media_revision} AND e.profile_digest={digest}
         LIMIT 1), {digest})"""


def _v3_ending(value):
    """The analyzer's D9 result, checked, or None."""
    if value is None:
        return None
    if (not isinstance(value, dict) or set(value) != {'music_end_frame', 'ending'}
            or type(value['music_end_frame']) is not int or value['music_end_frame'] <= 0
            or not isinstance(value['ending'], dict)
            or value['ending'].get('boundaries', [None])[-1] != value['music_end_frame']):
        raise ValueError('edge publication music end is malformed')
    return value


EDGE_RUNTIME_FRESH_HOURS = 6


def _utc_iso(value):
    """UTC ``...Z`` text for a ``timestamptz`` value, else None.

    The edge tables use ``TIMESTAMP`` columns written with ``now()``, so they
    hold the session's local time. Callers select them cast to
    ``timestamptz`` (interpreted in the session time zone, as written) and
    this converts to real UTC instead of appending ``Z`` to local time.
    """
    if not hasattr(value, 'astimezone'):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
# Refusals that a retry of the same media can never change: never re-queued
# for the same revision.
UNSUPPORTED_REASONS = (
    "unqualified channel layout", "unsupported source sample rate",
    "edge duration limit exceeded", "no audio stream",
)
UNSUPPORTED = "unsupported"


def is_unsupported_failure(exc):
    message = str(exc)
    return any(reason in message for reason in UNSUPPORTED_REASONS)


def record_edge_runtime(db, worker_id, status):
    """Store this worker's edge runtime verdict (``edge_runtime_status()``)."""
    cur = db.cursor()
    cur.execute(f"""INSERT INTO {table('edge_runtime_state')} (worker_id, available, status, reported_at)
        VALUES (%s, %s, %s::jsonb, now())
        ON CONFLICT (worker_id) DO UPDATE SET available=EXCLUDED.available,
            status=EXCLUDED.status, reported_at=EXCLUDED.reported_at""",
                (str(worker_id)[:200], bool(status.get('available')), canonical_json(status)))
    db.commit()
    cur.close()


def edge_worker_runtime(db):
    """The freshest worker verdict: ``(analyzable, status)``.

    ``analyzable`` is True when any worker reported a qualified runtime within
    ``EDGE_RUNTIME_FRESH_HOURS``, False when fresh reports exist but none is
    qualified, None when no worker reported recently. ``status`` is the newest
    qualified report (else the newest report) with ``reported_at``.
    """
    cur = db.cursor()
    cur.execute(f"""SELECT available, status, reported_at::timestamptz FROM {table('edge_runtime_state')}
        WHERE reported_at > now() - make_interval(hours => %s)
        ORDER BY available DESC, reported_at DESC LIMIT 1""", (EDGE_RUNTIME_FRESH_HOURS,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None, None
    status = dict(row[1] or {})
    reported = row[2]
    status['reported_at'] = _utc_iso(reported)
    return bool(row[0]), status


def edge_join(profile_alias='p', columns='e.payload'):
    # Internal signatures stay on the server. Only their opaque revision is public.
    # ``columns`` lets a caller read only key columns of the chosen edge row
    # (the v2 capture stores a reference and never detoasts the payload).
    return f"""LEFT JOIN LATERAL (
        SELECT {columns} FROM {table('edge_profiles')} e
         WHERE e.catalog_instance_id={profile_alias}.catalog_instance_id
           AND e.track_id={profile_alias}.track_id
           AND e.media_signature={profile_alias}.media_signature
         ORDER BY e.updated_at DESC, e.profile_digest LIMIT 1
    ) edge ON TRUE"""


def claim_edge_jobs(db, catalog_id, ids):
    """``(jobs, ready)`` for a background request (see ``claim_edge_requests``)."""
    jobs, ready, _pending, _unsupported = claim_edge_requests(db, catalog_id, ids)
    return jobs, ready


def claim_edge_requests(db, catalog_id, ids, priority='background'):
    """Bounded request; ready/current rows are retained and failed upgrades back off.

    Returns ``(jobs, ready, pending, unsupported)``. ``jobs`` are the jobs to
    queue: new claims, plus pending jobs an interactive request promoted (same
    token; whichever task marks one running first wins). ``pending`` names
    tracks already queued or running, ``unsupported`` tracks whose current
    media the analyzer can never measure.
    """
    priority = 'interactive' if priority == 'interactive' else 'background'
    cur = db.cursor()
    cur.execute(f"""SELECT p.track_id, p.media_signature, edge.payload, edge.v3_recheck
        FROM {table('published_source_profiles')} p {edge_join(columns='e.payload, e.v3_recheck')}
        WHERE p.catalog_instance_id=%s AND p.track_id=ANY(%s)
        ORDER BY p.track_id""", (catalog_id, list(dict.fromkeys(ids))[:100]))
    rows = cur.fetchall()
    accepted, ready, pending, unsupported = [], [], [], []
    for track_id, signature, payload, recheck in rows:
        revision = opaque_revision(signature)
        if not revision:
            continue
        # An edge to measure again for its music end (v3_recheck) is not ready.
        if (payload and not recheck and payload.get('schema_version') == SCHEMA_VERSION
                and payload.get('measurement', {}).get('method') == METHOD):
            ready.append(track_id)
            continue
        token = str(uuid.uuid4())
        cur.execute(f"""INSERT INTO {table('edge_profile_jobs')} AS job
            (catalog_instance_id, track_id, media_revision, job_token, status, priority)
            VALUES (%s, %s, %s, %s, 'pending', %s)
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                media_revision=EXCLUDED.media_revision, job_token=EXCLUDED.job_token,
                status='pending', last_error=NULL, priority=EXCLUDED.priority, updated_at=now()
            WHERE job.media_revision<>EXCLUDED.media_revision
               OR (job.status IN ('pending', 'running') AND job.updated_at < now()-interval '30 minutes')
               OR (job.status='failed' AND job.last_error='edge-enqueue-failed'
                   AND job.updated_at < now()-interval '2 seconds')
               OR (job.status NOT IN ('pending', 'running', 'unsupported')
                   AND job.updated_at < now()-interval '6 hours')
            RETURNING job_token""", (catalog_id, track_id, revision, token, priority))
        if cur.fetchone():
            accepted.append({'track_id': track_id, 'media_revision': revision, 'job_token': token})
            continue
        cur.execute(f"""SELECT status, job_token, priority FROM {table('edge_profile_jobs')}
            WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s""",
                    (catalog_id, track_id, revision))
        job = cur.fetchone()
        if job and job[0] == UNSUPPORTED:
            unsupported.append(track_id)
        elif job and job[0] in ('pending', 'running'):
            pending.append(track_id)
            if priority == 'interactive' and job[0] == 'pending':
                if job[2] != 'interactive':
                    cur.execute(f"""UPDATE {table('edge_profile_jobs')} SET priority='interactive'
                        WHERE catalog_instance_id=%s AND track_id=%s AND job_token=%s""",
                                (catalog_id, track_id, job[1]))
                accepted.append({'track_id': track_id, 'media_revision': revision, 'job_token': job[1]})
    db.commit()
    cur.close()
    return accepted, ready, pending, unsupported


def mark_edge_jobs_deferred(db, catalog_id, jobs, marker):
    """Keep refused on-demand jobs pending, marked for the next worker task."""
    cur = db.cursor()
    for job in jobs:
        cur.execute(f"""UPDATE {table('edge_profile_jobs')} SET last_error=%s, priority='interactive', updated_at=now()
            WHERE catalog_instance_id=%s AND track_id=%s AND job_token=%s AND status='pending'""",
                    (marker, catalog_id, job['track_id'], job['job_token']))
    db.commit()
    cur.close()


def claim_deferred_edge_jobs(db, marker, limit):
    """Take up to ``limit`` refused on-demand jobs, oldest first, for this task."""
    cur = db.cursor()
    cur.execute(f"""UPDATE {table('edge_profile_jobs')} j SET last_error=NULL, updated_at=now()
        FROM (SELECT catalog_instance_id, track_id FROM {table('edge_profile_jobs')}
               WHERE status='pending' AND priority='interactive' AND last_error=%s
               ORDER BY updated_at, track_id LIMIT %s FOR UPDATE SKIP LOCKED) d
        WHERE j.catalog_instance_id=d.catalog_instance_id AND j.track_id=d.track_id
        RETURNING j.catalog_instance_id, j.track_id, j.media_revision, j.job_token""",
                (marker, int(limit)))
    rows = cur.fetchall()
    db.commit()
    cur.close()
    return [{'catalog_instance_id': r[0], 'track_id': r[1], 'media_revision': r[2], 'job_token': r[3]}
            for r in rows]


def edge_queue_counts(db):
    """Health's edge queue depth: pending, running and interactive jobs."""
    cur = db.cursor()
    cur.execute(f"""SELECT count(*) FILTER (WHERE status='pending'),
                           count(*) FILTER (WHERE status='running'),
                           count(*) FILTER (WHERE status='pending' AND priority='interactive')
                      FROM {table('edge_profile_jobs')} WHERE status IN ('pending', 'running')""")
    row = cur.fetchone() or (0, 0, 0)
    cur.close()
    return {'pending': int(row[0] or 0), 'running': int(row[1] or 0), 'interactive': int(row[2] or 0)}


def interactive_edge_demand(db):
    """Whether any on-demand edge job is waiting."""
    cur = db.cursor()
    cur.execute(f"""SELECT EXISTS(SELECT 1 FROM {table('edge_profile_jobs')}
        WHERE status='pending' AND priority='interactive')""")
    row = cur.fetchone()
    cur.close()
    return bool(row and row[0])


def update_edge_job(db, catalog_id, job, status, reason=None):
    cur = db.cursor()
    cur.execute(f"""UPDATE {table('edge_profile_jobs')} SET status=%s, last_error=%s, updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s AND job_token=%s
          AND status=ANY(%s)
        RETURNING job_token""", (status, str(reason)[:160] if reason else None, catalog_id,
                                  job['track_id'], job['media_revision'], job['job_token'],
                                  ['pending'] if status == 'running' else ['pending', 'running']))
    applied = cur.fetchone() is not None
    db.commit()
    cur.close()
    return applied


def publish_edge_profile(db, catalog_id, job, payload, signature):
    from .catalog_enrichment import journal_edge_ref, record_profile_change, serialize_profile

    payload = dict(payload)
    ending = _v3_ending(payload.pop(V3_ENDING_KEY, None))
    if (payload.get('schema_version') != SCHEMA_VERSION or payload.get('catalog_instance_id') != catalog_id or
            payload.get('track_id') != job['track_id'] or payload.get('media_revision') != job['media_revision'] or
            opaque_revision(signature) != job['media_revision'] or profile_digest(payload) != payload.get('profile_digest')):
        raise ValueError('edge publication identity/digest mismatch')
    cur = db.cursor()
    cur.execute(
        f"""SELECT c.published_generation
              FROM {table('catalog_state')} c
              JOIN {table('catalog_sources')} s USING (catalog_instance_id)
             WHERE c.catalog_instance_id=%s AND s.rebind_status='active'
             FOR UPDATE OF c""",
        (catalog_id,),
    )
    state = cur.fetchone()
    if state is None:
        db.rollback()
        cur.close()
        return False
    cur.execute(
        f"""SELECT media_fp FROM {table('catalog_tracks')}
             WHERE catalog_instance_id=%s AND published_generation=%s
               AND track_id=%s AND available=TRUE""",
        (catalog_id, state[0], job['track_id']),
    )
    media = cur.fetchone()
    if not media or not media[0] or signature not in (
        str(media[0]), f"catalog-media:{media[0]}"
    ):
        db.rollback()
        cur.close()
        return False
    cur.execute(f"""SELECT track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                analyzer_ver, analyzed_at, media_signature FROM {table('published_source_profiles')}
        WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
                (catalog_id, job['track_id']))
    legacy = cur.fetchone()
    cur.execute(f"""SELECT job_token FROM {table('edge_profile_jobs')}
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s AND job_token=%s
          AND status IN ('pending', 'running') FOR UPDATE""",
                (catalog_id, job['track_id'], job['media_revision'], job['job_token']))
    current = cur.fetchone()
    if not legacy or legacy[8] != signature or not current:
        db.rollback()
        cur.close()
        return False
    cur.execute(f"DELETE FROM {table('edge_profiles')} WHERE catalog_instance_id=%s AND track_id=%s",
                (catalog_id, job['track_id']))
    cur.execute(f"""INSERT INTO {table('edge_profiles')}
        (catalog_instance_id, track_id, media_revision, representation_id, media_signature, profile_digest, payload,
         profile_digest_v3, v3_ending)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb)""",
                (catalog_id, job['track_id'], job['media_revision'], payload['representation_id'],
                 signature, payload['profile_digest'], canonical_json(payload),
                 edge_profile_v3(payload, ending)['profile_digest'],
                 canonical_json(ending) if ending else None))
    # The event journals the waveform part and a reference to the edge just
    # stored (K6); an edge publication is served in full to every client.
    record_profile_change(cur, catalog_id, job['track_id'], 'ready', serialize_profile(*legacy),
                          edge_ref=journal_edge_ref(payload['profile_digest'], kept=False))
    cur.execute(f"""UPDATE {table('edge_profile_jobs')} SET status='ready', last_error=NULL, updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND job_token=%s""",
                (catalog_id, job['track_id'], job['job_token']))
    db.commit()
    cur.close()
    return True


def edge_profile_status(db, catalog_id):
    """Read-only counts and freshness for one source's edge upgrade (P3-9).

    Source-scoped aggregates: published edge profiles, jobs in progress,
    failed jobs, the most recent failed job's ``last_error`` (free text;
    callers redact it) and the latest published edge's ``updated_at``. The
    edge count and the latest edge come from one index-only scan of
    ``(catalog_instance_id, updated_at)``; the jobs table is small. Never
    mutates anything.
    """
    cur = db.cursor()
    cur.execute(f"""
        SELECT edges.ready, jobs.active, jobs.failed, jobs.last_error, edges.last_success_at
          FROM (SELECT count(*) AS ready, max(updated_at) AS last_success_at
                  FROM {table('edge_profiles')} WHERE catalog_instance_id=%s) edges,
               (SELECT count(*) FILTER (WHERE status IN ('pending', 'running')) AS active,
                       count(*) FILTER (WHERE status='failed') AS failed,
                       (SELECT last_error FROM {table('edge_profile_jobs')}
                         WHERE catalog_instance_id=%s AND status='failed'
                         ORDER BY updated_at DESC LIMIT 1) AS last_error
                  FROM {table('edge_profile_jobs')} WHERE catalog_instance_id=%s) jobs
    """, (catalog_id, catalog_id, catalog_id))
    row = cur.fetchone()
    cur.close()
    ready, active, failed, last_error, last_success_at = row or (0, 0, 0, None, None)
    return {
        "ready": int(ready or 0),
        "active": int(active or 0),
        "failed": int(failed or 0),
        "last_error": str(last_error) if last_error else None,
        "last_success_at": last_success_at.isoformat() if last_success_at else None,
    }


def _edge_due_sql():
    # Published waveform rows without an edge for their media (P3-7): an edge
    # is published only for, and read only through, such a row. An attempt
    # row may be unpublished or describe other media than what is published.
    # Anti-joins let the pass walk the primary key from its cursor and stop at
    # its batch; a left join with an "edge is NULL" filter was estimated at
    # one row and seq-scanned the jobs per track (AUD-14). The payload is NOT
    # NULL and a track has at most one job and one edge, so the meaning is
    # unchanged. An edge flagged for a music-end recheck (SF.3) is due like a
    # missing one.
    return f"""{table('published_source_profiles')} p
        WHERE p.catalog_instance_id=%s AND p.media_signature IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {table('edge_profiles')} e
                           WHERE e.catalog_instance_id=p.catalog_instance_id
                             AND e.track_id=p.track_id AND e.media_signature=p.media_signature
                             AND NOT e.v3_recheck)
          AND NOT EXISTS (SELECT 1 FROM {table('edge_profile_jobs')} j
                           WHERE j.catalog_instance_id=p.catalog_instance_id
                             AND j.track_id=p.track_id
                             AND j.updated_at >= now()-interval '6 hours'
                             AND NOT (j.status='failed'
                                      AND j.last_error IS NOT DISTINCT FROM 'edge-enqueue-failed'))"""


def edge_backfill_candidates(db, catalog_id, after='', limit=100):
    cur = db.cursor()
    cur.execute(f"""SELECT p.track_id FROM {_edge_due_sql()} AND p.track_id>%s
        ORDER BY p.track_id LIMIT %s""", (catalog_id, after, max(1, min(100, int(limit)))))
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return ids


# ---------------------------------------------------------------- library pass
# The server walks the published library by track ID, one turn per
# reconcile tick until its deadline, after the waveform backfill. A finished pass sweeps again
# after EDGE_BACKFILL_RESWEEP_HOURS so failed jobs past their back-off, and
# tracks published since, are covered.
EDGE_BACKFILL_RESWEEP_HOURS = 6


def ensure_edge_backfill_sources(db):
    """Give every active source a pass row (a new row is due now); returns rows added."""
    cur = db.cursor()
    cur.execute(f"""INSERT INTO {table('edge_backfill_state')} (catalog_instance_id)
        SELECT catalog_instance_id FROM {table('catalog_sources')} WHERE rebind_status='active'
        ON CONFLICT (catalog_instance_id) DO NOTHING""")
    added = max(0, cur.rowcount or 0)
    db.commit()
    cur.close()
    return added


def next_edge_backfill(db, server_id=None):
    """``(server_id, catalog_instance_id, cursor)`` of the next due pass, or None."""
    ensure_edge_backfill_sources(db)
    cur = db.cursor()
    cur.execute(f"""SELECT s.current_core_server_id, st.catalog_instance_id, st.cursor
        FROM {table('edge_backfill_state')} st
        JOIN {table('catalog_sources')} s USING (catalog_instance_id)
        WHERE s.rebind_status='active'
          AND (%s IS NULL OR s.current_core_server_id=%s)
          AND (st.status IN ('queued', 'failed', 'complete')
               OR (st.status='running' AND st.updated_at < now()-interval '30 minutes'))
          AND (st.next_retry_at IS NULL OR st.next_retry_at <= now())
        ORDER BY st.next_retry_at NULLS FIRST, st.catalog_instance_id LIMIT 1""",
                (server_id, server_id))
    row = cur.fetchone()
    cur.close()
    db.commit()
    return tuple(row) if row else None


def update_edge_backfill(db, catalog_id, status, *, cursor=None, processed=0,
                         next_retry_hours=None, error=None, started=False, completed=False):
    cur = db.cursor()
    cur.execute(f"""UPDATE {table('edge_backfill_state')} SET status=%s,
            cursor=COALESCE(%s, cursor), processed=processed+%s,
            retry_count=CASE WHEN %s='failed' THEN retry_count ELSE 0 END,
            next_retry_at=CASE WHEN %s IS NULL THEN NULL ELSE now()+make_interval(hours => %s) END,
            last_error=%s,
            pass_started_at=CASE WHEN %s THEN now() ELSE pass_started_at END,
            completed_at=CASE WHEN %s THEN now() ELSE completed_at END,
            updated_at=now()
        WHERE catalog_instance_id=%s""",
                (status, cursor, int(processed), status, next_retry_hours, next_retry_hours or 0,
                 str(error)[:500] if error else None, bool(started), bool(completed), catalog_id))
    db.commit()
    cur.close()


def arm_edge_backfill(db, catalog_id=None, include_complete=True):
    """Make a parked pass, and unless ``include_complete`` is False a finished
    one, due now; returns the passes armed.

    A catalogue refresh re-arms finished passes (new tracks). A worker's
    periodic runtime report only resumes parked ones, so it never cancels a
    finished pass's re-sweep delay.
    """
    statuses = ['complete', 'waiting_runtime'] if include_complete else ['waiting_runtime']
    cur = db.cursor()
    cur.execute(f"""UPDATE {table('edge_backfill_state')} SET status='queued', next_retry_at=NULL,
            cursor=CASE WHEN status='complete' THEN '' ELSE cursor END, updated_at=now()
        WHERE status=ANY(%s) AND (%s IS NULL OR catalog_instance_id=%s)""",
                (statuses, catalog_id, catalog_id))
    armed = max(0, cur.rowcount or 0)
    db.commit()
    cur.close()
    return armed


def rewind_edge_backfill(db, catalog_id, track_id):
    """Make the source's pass due and reach ``track_id``; returns whether it changed.

    Only when the track is a pass candidate. A finished or parked pass starts
    again; a queued pass already past the track restarts from the beginning,
    which costs one scan because candidates skip tracks that have an edge. A
    running or failed pass is left as it is.
    """
    cur = db.cursor()
    cur.execute(f"""UPDATE {table('edge_backfill_state')} SET status='queued', next_retry_at=NULL,
            cursor='', updated_at=now()
        WHERE catalog_instance_id=%s
          AND (status IN ('complete', 'waiting_runtime') OR (status='queued' AND cursor>=%s))
          AND EXISTS (SELECT 1 FROM {_edge_due_sql()} AND p.track_id=%s)""",
                (catalog_id, track_id, catalog_id, track_id))
    rewound = (cur.rowcount or 0) > 0
    db.commit()
    cur.close()
    return rewound


def park_edge_backfill(db):
    """No qualified worker: park due passes so the watchdog can go idle."""
    cur = db.cursor()
    cur.execute(f"""UPDATE {table('edge_backfill_state')} SET status='waiting_runtime', updated_at=now()
        WHERE status IN ('queued', 'failed')""")
    db.commit()
    cur.close()


def edge_backfill_row(db, catalog_id):
    cur = db.cursor()
    cur.execute(f"""SELECT status, cursor, processed, pass_started_at::timestamptz,
                           completed_at::timestamptz, next_retry_at::timestamptz, last_error
        FROM {table('edge_backfill_state')} WHERE catalog_instance_id=%s""", (catalog_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return {'status': row[0], 'cursor': row[1], 'processed': int(row[2] or 0),
            'pass_started_at': _utc_iso(row[3]), 'completed_at': _utc_iso(row[4]),
            'next_retry_at': _utc_iso(row[5]), 'last_error': row[6]}


def edge_statuses(db, catalog_id, ids):
    """Per-track edge state for ``/api/profiles?edge_status=1``.

    ``ready`` (a current edge is served), ``pending`` (queued or running),
    ``failed`` with ``retry_at``, ``unsupported`` (never measurable for this
    media), or ``absent``.
    """
    ids = list(dict.fromkeys(ids))
    if not ids:
        return {}
    cur = db.cursor()
    cur.execute(f"""SELECT p.track_id, edge.payload IS NOT NULL, j.status, j.updated_at::timestamptz, j.media_revision,
                           p.media_signature, j.last_error
        FROM {table('published_source_profiles')} p {edge_join(columns='1 AS payload')}
        LEFT JOIN {table('edge_profile_jobs')} j
          ON j.catalog_instance_id=p.catalog_instance_id AND j.track_id=p.track_id
        WHERE p.catalog_instance_id=%s AND p.track_id=ANY(%s)""", (catalog_id, ids))
    rows = cur.fetchall()
    cur.close()
    result = {}
    for track_id, has_edge, status, updated_at, revision, signature, last_error in rows:
        current = revision is not None and revision == opaque_revision(signature)
        if has_edge:
            result[track_id] = {'status': 'ready'}
        elif current and status in ('pending', 'running'):
            result[track_id] = {'status': 'pending'}
        elif current and status == UNSUPPORTED:
            result[track_id] = {'status': 'unsupported'}
        elif current and status == 'failed' and updated_at is not None:
            # Mirrors claim_edge_requests: a refused enqueue is due again at
            # once, any other failure after its six-hour back-off.
            delay = timedelta(seconds=2) if last_error == 'edge-enqueue-failed' else timedelta(hours=6)
            retry_at = updated_at + delay
            result[track_id] = {'status': 'failed', 'retry_at': _utc_iso(retry_at)}
        else:
            result[track_id] = {'status': 'absent'}
    return result


# ------------------------------------------------------------ schema v3 upgrade
# Edges stored before 1.7.0 get their v3 digest in short batches after the
# migration and on reconcile ticks; health lists schema 3 only when none is
# missing. The same pass flags edges whose tail window opens below the excerpt
# floor for a music-end recheck (D9, about 0.1% of a library).
EDGE_V3_BATCH = 200


def edge_schema_v3_ready(db):
    """Whether every stored edge has its v3 digest (one partial-index probe)."""
    cur = db.cursor()
    try:
        cur.execute(f"SELECT 1 FROM {table('edge_profiles')} WHERE profile_digest_v3 IS NULL LIMIT 1")
        return cur.fetchone() is None
    finally:
        cur.close()


def upgrade_stored_edges(db, batch_size=EDGE_V3_BATCH, budget_seconds=None):
    """Computes missing v3 digests and recheck flags; returns ``(upgraded, flagged)``.

    One short transaction per batch; a row is updated only while it is still
    the edge that was read (same v2 digest). Stops after ``budget_seconds``
    (None: when done) and resumes where it stopped. Flagged sources' library
    passes are made due.
    """
    deadline = None if budget_seconds is None else time.monotonic() + budget_seconds
    upgraded = flagged = 0
    sources = set()
    cur = db.cursor()
    try:
        while deadline is None or time.monotonic() < deadline:
            cur.execute(f"""SELECT catalog_instance_id, track_id, media_revision, representation_id,
                                   profile_digest, payload::text
                              FROM {table('edge_profiles')} WHERE profile_digest_v3 IS NULL
                             ORDER BY catalog_instance_id, track_id LIMIT %s""", (batch_size,))
            rows = cur.fetchall()
            for catalog_id, track_id, revision, representation, digest, text in rows:
                payload = json.loads(text)
                try:
                    v3_digest = edge_profile_v3(payload)['profile_digest']
                    recheck = tail_opens_below_floor(payload['head'], payload['tail'])
                except (KeyError, TypeError, ValueError):
                    # Not a measurable v2 edge: serve no v3 edge for it, and
                    # never flag it again.
                    v3_digest, recheck = '', False
                cur.execute(f"""UPDATE {table('edge_profiles')} SET profile_digest_v3=%s, v3_recheck=%s
                    WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
                      AND representation_id=%s AND profile_digest=%s AND profile_digest_v3 IS NULL""",
                            (v3_digest, recheck, catalog_id, track_id, revision, representation, digest))
                if cur.rowcount:
                    upgraded += 1
                    if recheck:
                        flagged += 1
                        sources.add(catalog_id)
            if rows:
                db.commit()
            if len(rows) < batch_size:
                break
    finally:
        cur.close()
    for catalog_id in sources:
        arm_edge_backfill(db, catalog_id)
    return upgraded, flagged
