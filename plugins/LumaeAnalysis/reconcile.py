"""Durable work accounting and observability for Lumae's background work.

The runners (``runner.py``, runner plan 2026-10-25) do the work; this module
keeps what they share: the work summary and retry ladder of the durable work
tables, the event journal, throughput and remaining work, AudioMuse's live
jobs (R4), and the wake (``wake_runners``)."""

from contextvars import ContextVar
from datetime import datetime, timezone
import json
import time

from plugin.api import get_db, logger, table

from . import migrations, runner


LUMAE_TASK_TYPE_PREFIX = "plugin.lumae_analysis."
# On-demand work recorded for the runners: the ``last_error`` of a pending
# interactive attempt or edge job, cleared when a runner takes it (R6).
# 1.8.0 wrote DEFERRED_MARKER for work AudioMuse refused to queue; it is
# served the same way, for one release.
RUNNER_WAITING_MARKER = "waiting_runner"
DEFERRED_MARKER = "deferred_host_busy"
WAITING_MARKERS = (RUNNER_WAITING_MARKER, DEFERRED_MARKER)
# What a runner writes there instead when it takes the work, by role: the
# claims of a runner that went away wait again at once, not for the stale
# rules' hour (waveforms) or half hour (edges).
RUNNER_CLAIM_MARKERS = {runner.ROLE_SERVER: "claimed_server_runner",
                        runner.ROLE_WORKER: "claimed_worker_runner"}


def runner_claim_marker():
    """This thread's runner's claim marker; None outside a runner."""
    return RUNNER_CLAIM_MARKERS.get(runner.ROLE.get())


def _claim_markers_sql():
    return ", ".join(f"'{marker}'" for marker in RUNNER_CLAIM_MARKERS.values())


# The control row 1.8.0's watchdog reads; kept as it is so a downgrade finds it.
LEGACY_IDLE_CRON = "11 * * * *"
# The journal keeps a day of events per source, at least the latest
# EVENT_RETENTION_MIN and at most EVENT_RETENTION_MAX; the settings page shows
# the latest EVENT_JOURNAL_LIMIT. 25 events were about 25 minutes at minute
# cadence, which rotated a long tick out before anyone looked (AUD-18).
EVENT_RETENTION_HOURS = 24
EVENT_RETENTION_MIN = 25
EVENT_RETENTION_MAX = 1000
EVENT_JOURNAL_LIMIT = 25
# AudioMuse shows a task's message in Recent tasks: a job an older version
# queued still runs one of these functions once.
TICK_MESSAGE_MAX = 100

_current_event_id = ContextVar("lumae_reconcile_event_id", default=None)


def control_table():
    return table("reconcile_control")


def events_table():
    return table("reconcile_events")


def _work_table(name):
    return table(name)


def migrate_reconcile(db):
    """Create additive operational state without changing app-facing schemas."""
    from .credits_store import migrate as migrate_credits
    from .music_metadata import migrate as migrate_metadata
    # _work_summary and read_reconcile_status read both job tables.
    migrate_credits(db)
    migrate_metadata(db)
    with db.cursor() as runner_cur:
        runner.migrate_runner_state(runner_cur)
    cur = db.cursor()
    for name in ("analysis_runs", "preparation_state", "profile_backfill_state", "relationship_state"):
        migrations.ensure_columns(
            cur, _work_table(name),
            "retry_count INTEGER NOT NULL DEFAULT 0",
            "next_retry_at TIMESTAMPTZ",
        )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {control_table()} (
            name TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            cron_expr TEXT NOT NULL,
            reason TEXT,
            last_sweep_at TIMESTAMPTZ,
            next_retry_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CHECK (name='catalog_reconcile'),
            CHECK (mode IN ('active', 'waiting', 'backoff', 'idle', 'paused'))
        )
        """
    )
    cur.execute(
        f"""
        INSERT INTO {control_table()} (name, mode, cron_expr, reason)
        VALUES ('catalog_reconcile', 'idle', %s, 'migration')
        ON CONFLICT (name) DO NOTHING
        """,
        (LEGACY_IDLE_CRON,),
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {events_table()} (
            event_id BIGSERIAL PRIMARY KEY,
            server_id TEXT,
            catalog_instance_id TEXT,
            action TEXT NOT NULL,
            work_key TEXT,
            status TEXT NOT NULL,
            phase TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1,
            progress_current INTEGER,
            progress_total INTEGER,
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            duration_ms BIGINT,
            summary JSONB,
            last_error TEXT,
            next_retry_at TIMESTAMPTZ,
            CHECK (status IN ('running', 'success', 'deferred', 'failed', 'interrupted'))
        )
        """
    )
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS {table('reconcile_events_source_idx')} "
        f"ON {events_table()} (catalog_instance_id, event_id DESC)",
    )
    cur.close()


def wake_runners(db, reason="work_admitted", commit=False):
    """Wake the runners for work just recorded: ``pg_notify`` in the caller's
    transaction, delivered with its commit (R2)."""
    cur = db.cursor()
    runner.notify(cur, reason)
    cur.close()
    if commit:
        db.commit()


def _work_summary(cur):
    cur.execute(
        f"""
        WITH candidates AS (
            SELECT
                CASE
                    WHEN r.next_retry_at > now() THEN 'retry'
                    WHEN parent.status IN ('SUCCESS', 'FAILURE', 'FAIL', 'REVOKED') THEN 'ready'
                    WHEN parent.task_id IS NULL
                         AND r.last_seen_at < now() - interval '2 minutes'
                         AND NOT EXISTS (
                             SELECT 1 FROM task_status live
                              WHERE live.parent_task_id IS NULL
                                AND live.task_type='main_analysis'
                                AND live.status IN (
                                    'NEW', 'QUEUED', 'PENDING', 'STARTED',
                                    'RUNNING', 'PROGRESS'
                                )
                         ) THEN 'ready'
                    ELSE 'waiting'
                END AS kind,
                r.retry_count,
                r.next_retry_at
              FROM {_work_table('analysis_runs')} r
              LEFT JOIN task_status parent
                ON parent.task_id=r.run_id AND parent.task_type='main_analysis'
             WHERE r.status IN ('pending', 'registering', 'queued',
                                'enqueue_failed', 'failed')
                OR (r.status='running' AND r.updated_at < now() - interval '30 minutes')
            UNION ALL
            SELECT CASE WHEN next_retry_at > now() THEN 'retry' ELSE 'ready' END,
                   retry_count, next_retry_at
              FROM {_work_table('preparation_state')}
             WHERE status='queued'
                OR status='failed'
                OR (status='running' AND updated_at < now() - interval '1 hour')
            UNION ALL
            SELECT CASE WHEN next_retry_at > now() THEN 'retry' ELSE 'ready' END,
                   retry_count, next_retry_at
              FROM {_work_table('relationship_state')}
             WHERE status IN ('queued', 'failed', 'waiting_for_index')
                OR (status='running' AND updated_at < now() - interval '2 minutes')
            UNION ALL
            SELECT CASE WHEN next_retry_at > now() THEN 'retry' ELSE 'ready' END,
                   retry_count, next_retry_at
              FROM {_work_table('profile_backfill_state')}
             WHERE status IN ('queued', 'failed')
                OR (status='running' AND updated_at < now() - interval '30 minutes')
            UNION ALL
            SELECT CASE WHEN next_retry_at > now() THEN 'retry' ELSE 'ready' END,
                   retry_count, next_retry_at
              FROM {_work_table('edge_backfill_state')}
             WHERE status IN ('queued', 'failed')
                OR (status='running' AND updated_at < now() - interval '30 minutes')
            UNION ALL
            SELECT CASE WHEN next_retry_at > now() THEN 'retry' ELSE 'ready' END,
                   retry_count, next_retry_at
              FROM {_work_table('loudness_backfill_state')}
             WHERE status IN ('queued', 'failed')
                OR (status='running' AND updated_at < now() - interval '30 minutes')
            UNION ALL
            -- SQ.1: a catalogue refresh an app asked for (R6).
            SELECT 'ready', 0, NULL::timestamp
              FROM {_work_table('catalog_state')}
             WHERE refresh_wanted_at IS NOT NULL
            UNION ALL
            SELECT 'ready', 0, NULL::timestamp
             WHERE EXISTS (SELECT 1 FROM {_work_table('source_profiles')}
                            WHERE status='pending_interactive'
                              AND last_error IN ('{WAITING_MARKERS[0]}', '{WAITING_MARKERS[1]}'))
                OR EXISTS (SELECT 1 FROM {_work_table('edge_profile_jobs')}
                            WHERE status='pending' AND priority='interactive'
                              AND last_error IN ('{WAITING_MARKERS[0]}', '{WAITING_MARKERS[1]}'))
            UNION ALL
            -- On-demand work a runner holds: the server looks again soon, so
            -- a worker that went away holding it hands it on (R6).
            SELECT 'waiting', 0, NULL::timestamp
             WHERE EXISTS (SELECT 1 FROM {_work_table('source_profiles')}
                            WHERE status='pending_interactive'
                              AND last_error IN ({_claim_markers_sql()}))
                OR EXISTS (SELECT 1 FROM {_work_table('edge_profile_jobs')}
                            WHERE status IN ('pending', 'running') AND priority='interactive'
                              AND last_error IN ({_claim_markers_sql()}))
            UNION ALL
            -- A re-check of an unchanged album (S7) is idle work: it never
            -- makes a runner due; a sweep takes it.
            SELECT CASE WHEN rechecks>0 THEN 'idle'
                        WHEN not_before>now() THEN 'retry' ELSE 'ready' END,
                   attempts,not_before
              FROM {_work_table('credits_jobs')}
             WHERE status IN('pending','failed') OR (status='running' AND lease_until<now())
            UNION ALL
            SELECT CASE WHEN not_before>now() THEN 'retry' ELSE 'ready' END,
                   attempts, not_before
              FROM {_work_table('metadata_jobs')}
             WHERE status IN ('pending', 'deferred')
                OR (status='running' AND lease_until<now())
        ), totals AS (
            SELECT count(*) FILTER (WHERE kind='ready') AS ready_count,
                   count(*) FILTER (WHERE kind='waiting') AS waiting_count
              FROM candidates
        ), retry AS (
            SELECT retry_count, next_retry_at
              FROM candidates
             WHERE kind='retry'
             ORDER BY next_retry_at, retry_count
             LIMIT 1
        )
        SELECT totals.ready_count, totals.waiting_count,
               retry.retry_count, retry.next_retry_at,
               -- GREATEST skips a NULL: without a retry it would say 0, and
               -- an idle runner would turn every second.
               CASE WHEN retry.next_retry_at IS NOT NULL
                    THEN GREATEST(0, EXTRACT(EPOCH FROM (retry.next_retry_at - now()))) END
          FROM totals LEFT JOIN retry ON TRUE
        """
    )
    row = cur.fetchone() or (0, 0, None, None, None)
    return {
        "ready": int(row[0] or 0),
        "waiting": int(row[1] or 0),
        "retry_count": int(row[2] or 0),
        "next_retry_at": row[3],
        "retry_in_seconds": float(row[4]) if row[4] is not None else None,
    }


def recover_interrupted_events(db):
    """Running events whose heartbeat stopped two hours ago: the process
    that ran them is gone (a restart, a kill)."""
    with db.cursor() as cur:
        _recover_interrupted_events(cur)
    db.commit()


def _recover_interrupted_events(cur):
    cur.execute(
        f"""
        UPDATE {events_table()}
           SET status='interrupted', phase='worker heartbeat expired',
               completed_at=now(), heartbeat_at=now(),
               duration_ms=GREATEST(
                   0, (EXTRACT(EPOCH FROM (now()-started_at))*1000)::BIGINT
               ),
               last_error=COALESCE(last_error, 'Worker stopped before reporting completion')
         WHERE status='running'
           AND heartbeat_at < now() - interval '2 hours'
        """
    )


def _retry_target(action):
    if action == "analysis_run":
        return "analysis_runs"
    if action == "catalog_preparation":
        return "preparation_state"
    if action == "relationships":
        return "relationship_state"
    if action == "profile_backfill":
        return "profile_backfill_state"
    if action == "edge_backfill":
        return "edge_backfill_state"
    if action == "loudness_backfill":
        return "loudness_backfill_state"
    raise ValueError(f"Unknown reconciliation action: {action}")


def update_work_retry(db, action, catalog_instance_id, work_key=None, *, failed=True,
                      keep_next_retry=False):
    """``keep_next_retry``: a success that stored its own ``next_retry_at``."""
    table_name = _retry_target(action)
    cur = db.cursor()
    if failed:
        where = "catalog_instance_id=%s"
        params = [catalog_instance_id]
        if action == "analysis_run":
            where += " AND run_id=%s"
            params.append(work_key)
        cur.execute(
            f"""
            UPDATE {_work_table(table_name)}
               SET retry_count=retry_count + 1,
                   next_retry_at=now() + CASE retry_count + 1
                       WHEN 1 THEN interval '1 minute'
                       WHEN 2 THEN interval '5 minutes'
                       WHEN 3 THEN interval '15 minutes'
                       ELSE interval '60 minutes'
                   END,
                   updated_at=now()
             WHERE {where}
            RETURNING retry_count, next_retry_at
            """,
            tuple(params),
        )
    else:
        where = "catalog_instance_id=%s"
        params = [catalog_instance_id]
        if action == "analysis_run":
            where += " AND run_id=%s"
            params.append(work_key)
        reset = "retry_count=0" if keep_next_retry else "retry_count=0, next_retry_at=NULL"
        cur.execute(
            f"UPDATE {_work_table(table_name)} SET {reset} WHERE {where}",
            tuple(params),
        )
        cur.close()
        db.commit()
        return {"retry_count": 0, "next_retry_at": None}
    row = cur.fetchone() or (1, None)
    cur.close()
    db.commit()
    return {"retry_count": int(row[0] or 1), "next_retry_at": row[1]}


def defer_work(db, action, catalog_instance_id, work_key=None, minutes=60):
    table_name = _retry_target(action)
    where = "catalog_instance_id=%s"
    params = [int(minutes), catalog_instance_id]
    if action == "analysis_run":
        where += " AND run_id=%s"
        params.append(work_key)
    cur = db.cursor()
    cur.execute(
        f"UPDATE {_work_table(table_name)} "
        f"SET retry_count=GREATEST(retry_count, 4), "
        f"next_retry_at=now() + (%s * interval '1 minute'), updated_at=now() "
        f"WHERE {where} RETURNING next_retry_at",
        tuple(params),
    )
    row = cur.fetchone()
    cur.close()
    db.commit()
    return row[0] if row else None


def begin_event(db, action, server_id, catalog_instance_id, work_key=None, attempt=1):
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {events_table()}
            (server_id, catalog_instance_id, action, work_key, status, phase, attempt)
        VALUES (%s, %s, %s, %s, 'running', 'claiming', %s)
        RETURNING event_id
        """,
        (server_id, catalog_instance_id, action, work_key, max(1, int(attempt or 1))),
    )
    row = cur.fetchone()
    cur.close()
    db.commit()
    event_id = int(row[0]) if row else None
    _current_event_id.set(event_id)
    return event_id


def progress_event(phase, current=None, total=None, db=None):
    event_id = _current_event_id.get()
    if event_id is None:
        return False
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"UPDATE {events_table()} SET phase=%s, progress_current=%s, "
        "progress_total=%s, heartbeat_at=now() WHERE event_id=%s AND status='running'",
        (str(phase)[:200], current, total, event_id),
    )
    cur.close()
    db.commit()
    return True


def finish_event(db, event_id, status, *, phase, summary=None, error=None, next_retry_at=None):
    if event_id is None:
        return
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {events_table()}
           SET status=%s, phase=%s, completed_at=now(), heartbeat_at=now(),
               duration_ms=GREATEST(0, (EXTRACT(EPOCH FROM (now()-started_at))*1000)::BIGINT),
               summary=%s::jsonb, last_error=%s, next_retry_at=%s
         WHERE event_id=%s
        """,
        (
            status,
            str(phase)[:200],
            json.dumps(summary or {}, default=str),
            str(error)[:2000] if error else None,
            next_retry_at,
            event_id,
        ),
    )
    _trim_events(cur)
    cur.close()
    db.commit()
    _current_event_id.set(None)


def record_event(db, action, server_id, catalog_instance_id, status, *, phase,
                 duration_ms, summary=None, error=None):
    """A finished event in one statement, for work that knows it ran only
    once it has (credits, MusicBrainz lookups, on-demand work)."""
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {events_table()}
            (server_id, catalog_instance_id, action, status, phase, started_at,
             heartbeat_at, completed_at, duration_ms, summary, last_error)
        VALUES (%s, %s, %s, %s, %s, now() - make_interval(secs => %s / 1000.0),
                now(), now(), %s, %s::jsonb, %s)
        """,
        (
            server_id,
            catalog_instance_id,
            action,
            status,
            str(phase)[:200],
            int(duration_ms),
            int(duration_ms),
            json.dumps(summary or {}, default=str),
            str(error)[:2000] if error else None,
        ),
    )
    _trim_events(cur)
    cur.close()
    db.commit()


def _trim_events(cur):
    cur.execute(
        f"""
        DELETE FROM {events_table()} e
         USING (
             SELECT event_id FROM (
                 SELECT event_id, COALESCE(completed_at, started_at) AS finished_at,
                        row_number() OVER (
                            PARTITION BY COALESCE(catalog_instance_id, '')
                            ORDER BY event_id DESC
                        ) AS position
                   FROM {events_table()}
                  WHERE status <> 'running'
             ) ranked
             WHERE position > %s
                OR (position > %s AND finished_at < now() - make_interval(hours => %s))
         ) expired
         WHERE e.event_id=expired.event_id
        """,
        (EVENT_RETENTION_MAX, EVENT_RETENTION_MIN, EVENT_RETENTION_HOURS),
    )


def discard_event(db, event_id):
    if event_id is None:
        return
    cur = db.cursor()
    cur.execute(f"DELETE FROM {events_table()} WHERE event_id=%s", (event_id,))
    cur.close()
    db.commit()
    _current_event_id.set(None)


def read_reconcile_status(db):
    cur = db.cursor()
    runners = runner.read_states(cur)
    cur.execute(
        f"""
        SELECT action, count(*)
          FROM (
              SELECT 'analysis finalization' AS action FROM {_work_table('analysis_runs')}
               WHERE status IN ('pending','registering','queued','enqueue_failed','failed','running')
              UNION ALL
              SELECT 'catalogue preparation' FROM {_work_table('preparation_state')}
               WHERE status IN ('queued','failed','running')
              UNION ALL
              SELECT 'relationships' FROM {_work_table('relationship_state')}
               WHERE status IN ('queued','failed','waiting_for_index','running')
              UNION ALL
              SELECT 'volume and ramps' FROM {_work_table('profile_backfill_state')}
               WHERE status IN ('queued','failed','running')
              UNION ALL
              SELECT 'MusicBrainz credits' FROM {_work_table('credits_jobs')}
               WHERE status IN ('pending','failed','running')
              UNION ALL
              SELECT 'MusicBrainz lookups' FROM {_work_table('metadata_jobs')}
               WHERE status IN ('pending','deferred','running')
          ) work
         GROUP BY action ORDER BY action
        """
    )
    pending = {str(row[0]): int(row[1] or 0) for row in cur.fetchall()}
    cur.execute(
        f"""
        SELECT event_id, server_id, catalog_instance_id, action, status, phase,
               attempt, progress_current, progress_total, started_at, heartbeat_at,
               completed_at, duration_ms, summary, last_error, next_retry_at
          FROM {events_table()}
         ORDER BY (status='running') DESC, event_id DESC
         LIMIT %s
        """,
        (EVENT_JOURNAL_LIMIT,),
    )
    rows = cur.fetchall()
    cur.close()
    keys = (
        "event_id", "server_id", "catalog_instance_id", "action", "status", "phase",
        "attempt", "progress_current", "progress_total", "started_at", "heartbeat_at",
        "completed_at", "duration_ms", "summary", "last_error", "next_retry_at",
    )
    events = [dict(zip(keys, row)) for row in rows]
    return {
        # One row per runner role that has led: runner.read_states.
        "runners": runners,
        "pending": pending,
        "events": events,
    }


def iso(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        current = value
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


# The queues the settings page reports throughput for (S12, AUD-18):
# (action, label, how a journal event counts). "ready" counts the summary's
# ready tracks, "published" the turns that published, "runs" every turn.
THROUGHPUT_QUEUES = (
    ("profile_backfill", "Volume and ramps", "ready"),
    ("edge_backfill", "Edge profiles", "ready"),
    ("loudness_backfill", "Loudness", "ready"),
    ("relationships", "Relationship steps", "runs"),
    ("credits", "MusicBrainz credits", "published"),
)
# The queues whose files K analysis threads analyse (performance plan WP1b).
ANALYSIS_QUEUES = ("profile_backfill", "edge_backfill", "loudness_backfill")


def _estimated_rows(name):
    """SQL for the planner's row estimate of a table: the status route is
    polled, and counting 100k+ rows each time is not worth an "about"."""
    return (f"(SELECT GREATEST(0, COALESCE(max(reltuples), 0))::bigint FROM pg_class "
            f"WHERE oid=to_regclass('{_work_table(name)}'))")


def queue_throughput(db):
    """Per queue: units done in the last hour and the last 24 hours (from the
    journal, which keeps a day), units left and the ETA at the last hour's
    rate (S12). What is left is approximate for edges and loudness: published
    waveforms without an edge, the summary's eligible tracks without a
    loudness record, from the planner's row estimates. Read-only, and it never scans the library: the settings
    status route renders it (P2-1)."""
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT action,
               COALESCE(sum(units) FILTER (WHERE completed_at > now() - interval '1 hour'), 0),
               COALESCE(sum(units), 0)
          FROM (SELECT action, completed_at,
                       CASE WHEN action IN ('profile_backfill', 'edge_backfill', 'loudness_backfill')
                            THEN CASE WHEN jsonb_typeof(summary->'ready')='number'
                                      THEN (summary->>'ready')::bigint ELSE 0 END
                            WHEN action='credits'
                            THEN CASE WHEN summary->>'status'='published' THEN 1 ELSE 0 END
                            ELSE 1 END AS units
                  FROM {events_table()}
                 WHERE action = ANY(%s) AND status IN ('success', 'deferred')
                   AND completed_at > now() - interval '24 hours') recent
         GROUP BY action
        """,
        ([action for action, _label, _unit in THROUGHPUT_QUEUES],),
    )
    done = {str(row[0]): (int(row[1] or 0), int(row[2] or 0)) for row in cur.fetchall()}
    cur.execute(
        f"""
        SELECT
            (SELECT COALESCE(sum(GREATEST(0, ss.profile_total_count - ss.profile_ready_count
                                             - ss.profile_failed_count - ss.profile_skipped_count)), 0)
               FROM {table('status_summary')} ss
               JOIN {table('catalog_sources')} s USING (catalog_instance_id)
              WHERE s.rebind_status='active'),
            (SELECT GREATEST(0, {_estimated_rows('published_source_profiles')}
                               - {_estimated_rows('edge_profiles')})),
            (SELECT GREATEST(0,
                (SELECT COALESCE(sum(ss.eligible_track_count), 0) FROM {table('status_summary')} ss
                   JOIN {table('catalog_sources')} s USING (catalog_instance_id)
                  WHERE s.rebind_status='active')
                - {_estimated_rows('loudness_tracks')})),
            NULL,
            (SELECT count(*) FROM {_work_table('credits_jobs')}
              WHERE (status IN ('pending', 'failed') OR (status='running' AND lease_until<now()))
                AND rechecks=0)
        """
    )
    left = cur.fetchone() or ()
    cur.close()
    queues = []
    for index, (action, label, _unit) in enumerate(THROUGHPUT_QUEUES):
        last_hour, last_day = done.get(action, (0, 0))
        remaining = int(left[index]) if index < len(left) and left[index] is not None else None
        eta = remaining / last_hour if remaining is not None and last_hour > 0 else None
        queues.append({"action": action, "label": label, "last_hour": last_hour, "last_day": last_day,
                       "remaining": remaining, "eta_hours": eta})
    return queues


def action_counts(action, result):
    """``(done, failed)`` units of one action's result."""
    result = result if isinstance(result, dict) else {}
    if action == "interactive":
        return int(result.get("served") or 0), 0
    if action in ("profile_backfill", "edge_backfill", "loudness_backfill"):
        return int(result.get("ready") or 0), int(result.get("failed") or 0)
    if action == "edge_schema_v3":
        return int(result.get("upgraded") or 0), 0
    status = str(result.get("status") or "")
    if status in ("failed", "failure", "error"):
        return 0, 1
    if status in ("current", "coalesced", "paused"):
        return 0, 0
    return 1, 0


class TurnReport:
    """What one runner turn did, per action (the journal has the detail)."""

    def __init__(self):
        self.started = time.monotonic()
        self.actions = []

    def add(self, action, result, duration_ms):
        """One run of ``action``; runs of the same action share one entry."""
        done, failed = action_counts(action, result)
        status = (str((result or {}).get("status") or "complete")
                  if isinstance(result, dict) or result is None else str(result))
        entry = next((entry for entry in self.actions if entry["action"] == action), None)
        if entry is None:
            entry = {"action": action, "status": "", "runs": 0, "count": 0, "failed": 0, "ms": 0}
            self.actions.append(entry)
        entry["status"] = status[:40]
        entry["runs"] += 1
        entry["count"] += done
        entry["failed"] += failed
        entry["ms"] += int(duration_ms)


def _plural(count, singular, plural=None):
    return f"{count:,} {singular if count == 1 else (plural or singular + 's')}"


def task_message(result, sentence):
    """``result`` with ``sentence`` as AudioMuse's task message.

    Lumae's queue tasks say what they did (LA-3); without
    a message AudioMuse shows "Plugin task completed successfully.". They set
    no ``tracks_analyzed``, so a host that notes "Songs analyzed" still does.
    """
    if len(sentence) > TICK_MESSAGE_MAX:
        sentence = sentence[:TICK_MESSAGE_MAX - 1] + "…"
    return {**result, "message": sentence, "status_message": sentence}


def catalog_refresh_sentence(result):
    reason = result.get("change_reason")
    if reason == "provider_identity_wait":
        return "Lumae: catalogue held for a second scan of the media server ID change"
    tracks = int((result.get("counts") or {}).get("track") or 0)
    if reason == "no_change":
        return f"Lumae: catalogue unchanged, {_plural(tracks, 'track')}"
    changes = int(result.get("changes") or 0)
    sentence = (f"Lumae: catalogue refreshed, {_plural(changes, 'change')}, "
                f"{_plural(tracks, 'track')}")
    if (result.get("analysis") or {}).get("status") == "projected":
        sentence += ", analysis projected"
    return sentence


def analysis_projection_sentence(result):
    ready = f"{int(result.get('ready_count') or 0):,} tracks ready"
    if result.get("unchanged"):
        return f"Lumae: analysis projection unchanged, {ready}"
    return f"Lumae: analysis projected, {_plural(int(result.get('changes') or 0), 'change')}, {ready}"


def identity_recheck_sentence(result):
    checked = int(result.get("checked") or 0)
    if not checked:
        return "Lumae: no media server ID change to recheck"
    failed = sum(1 for entry in result.get("results") or () if (entry or {}).get("error"))
    return (f"Lumae: rechecked {_plural(checked, 'media server ID change')}"
            + (f", {failed:,} failed" if failed else ""))


def collection_retention_sentence(result):
    if result.get("status") == "error":
        return "Lumae: collection cleanup failed; see the worker log"
    removed = sum(int(value or 0) for key, value in result.items() if key.endswith("_deleted"))
    if not removed:
        return "Lumae: no expired collection entries"
    return f"Lumae: removed {_plural(removed, 'expired collection entry', 'expired collection entries')}"


# Runner plan R4. AudioMuse jobs that rewrite or delete track IDs and mappings
# (cleaning prunes track_server_map and may delete score rows, a provider
# migration repoints the default server's mappings, the sweep writes and prunes
# track_server_map): every runner waits for them.
ID_REWRITING_TASK_TYPES = ("cleaning", "provider_migration", "server_sweep")
# AudioMuse's provider migration holds this advisory lock while it executes
# (tasks/provider_migration_tasks.py, the same key since 3.2.0).
PROVIDER_MIGRATION_LOCK_KEY = 7421536190082003
# A job a worker has started. A queued one (NEW) conflicts with nothing yet,
# and without a worker it never starts: a runner that waited for it would stop
# for good, as AudioMuse's own gate does behind a queued plugin task.
RUNNING_TASK_STATUSES = ("STARTED", "RUNNING", "PROGRESS")


def live_host_task(db, task_types):
    """The first running AudioMuse root task among ``task_types``, or None.
    Fails open: a raising query is rolled back and reads as none."""
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT task_type FROM task_status WHERE parent_task_id IS NULL "
                "AND task_type = ANY(%s) AND status = ANY(%s) ORDER BY task_type LIMIT 1",
                (list(task_types), list(RUNNING_TASK_STATUSES)),
            )
            row = cur.fetchone()
        db.commit()
        return str(row[0]) if row else None
    except Exception:
        _rollback_quietly(db)
        logger.debug("lumae_analysis could not read AudioMuse's live jobs", exc_info=True)
        return None


def id_rewrite_in_progress(cur):
    """R4's check inside a publishing transaction, just before its commit: an
    AudioMuse job that rewrites track IDs is live, or a provider migration
    holds its lock. The lock is looked up in ``pg_locks``, never taken: a
    probe that took it could refuse the owner's migration click for those
    milliseconds."""
    cur.execute(
        """
        SELECT EXISTS (SELECT 1 FROM pg_locks
                        WHERE locktype='advisory' AND granted AND objsubid=1
                          AND pid <> pg_backend_pid()
                          AND ((classid::bigint << 32) | objid::bigint) = %s)
        """,
        (PROVIDER_MIGRATION_LOCK_KEY,),
    )
    if cur.fetchone()[0]:
        return "provider_migration"
    cur.execute(
        "SELECT task_type FROM task_status WHERE parent_task_id IS NULL "
        "AND task_type = ANY(%s) AND status = ANY(%s) ORDER BY task_type LIMIT 1",
        (list(ID_REWRITING_TASK_TYPES), list(RUNNING_TASK_STATUSES)),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def _rollback_quietly(db):
    rollback = getattr(db, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:
            pass


