"""Adaptive, durable scheduling and observability for catalogue reconciliation."""

from contextvars import ContextVar
from datetime import datetime, timezone
import importlib
import json
import random
import time

from plugin.api import get_db, logger, table

from . import migrations


LUMAE_TASK_TYPE_PREFIX = "plugin.lumae_analysis."
CATALOG_RECONCILE_TASK_TYPE = "plugin.lumae_analysis.catalog_reconcile"
PROVIDER_IDENTITY_RECHECK_TASK_TYPE = "plugin.lumae_analysis.provider_identity_recheck"
ACTIVE_CRON = "* * * * *"
# On-demand work AudioMuse refused to queue (``last_error`` of a pending
# interactive attempt or edge job). It is runnable work for the watchdog.
DEFERRED_MARKER = "deferred_host_busy"
WAITING_CRON = "*/5 * * * *"
BACKOFF_15_CRON = "*/15 * * * *"
IDLE_CRON = "11 * * * *"
# Retry cadences, fastest first, each with the longest gap it can leave
# before its next tick.
BACKOFF_LADDER = (
    (ACTIVE_CRON, 60),
    (WAITING_CRON, 5 * 60),
    (BACKOFF_15_CRON, 15 * 60),
    (IDLE_CRON, 60 * 60),
)
# While it yields (plan S1), the watchdog ticks once, at a random minute this
# many minutes ahead, re-picked on every yielding tick. AudioMuse's retry pass
# runs about every 11 minutes (CRON_RETRY_INTERVAL_MINUTES, stamped after the
# pass), so a fixed cadence could meet it on every pass.
YIELD_MINUTES = (5, 15)
# A tick works for at most this long, over as many actions as fit (plan S2),
# so it never covers the next minute's host fire. Each action stops at the
# deadline itself; an action starts only when its recent average fits. The
# exception is a file already being analysed: it finishes, within its
# per-file limit (one at a time or K at once alike), so a file longer than
# the budget delays the next fires instead of restarting every tick.
TICK_BUDGET_SECONDS = 45
ACTION_AVERAGE_RUNS = 10
# Actions that take whatever is left of the tick and stop at the deadline
# themselves, a track at a time (performance plan WP1a): their recent average
# is the share of a tick they got, not what they need, so one starts once
# ELASTIC_MIN_SECONDS (or its whole average, when shorter) are left.
ELASTIC_ACTIONS = frozenset(("profile_backfill", "edge_backfill", "loudness_backfill"))
ELASTIC_MIN_SECONDS = 5
# AudioMuse's main jobs and side jobs: its cron gate refuses every start
# while one is live. A host with task_types adds BATCH_GATE_TASK_TYPES.
HOST_BLOCKING_TASK_TYPES = (
    "main_analysis", "main_clustering", "cleaning", "provider_migration",
    "server_sweep", "naming_preview",
)
# The journal keeps a day of events per source, at least the latest
# EVENT_RETENTION_MIN and at most EVENT_RETENTION_MAX; the settings page shows
# the latest EVENT_JOURNAL_LIMIT. 25 events were about 25 minutes at minute
# cadence, which rotated a long tick out before anyone looked (AUD-18).
EVENT_RETENTION_HOURS = 24
EVENT_RETENTION_MIN = 25
EVENT_RETENTION_MAX = 1000
EVENT_JOURNAL_LIMIT = 25
# AudioMuse shows the tick's message as its task message, and with AM-2 as
# its Recent tasks note.
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
        (IDLE_CRON,),
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
    cur.execute(
        """
        INSERT INTO cron (name, task_type, cron_expr, enabled)
        VALUES (%s, %s, %s, TRUE)
        ON CONFLICT (task_type) DO NOTHING
        """,
        (CATALOG_RECONCILE_TASK_TYPE, CATALOG_RECONCILE_TASK_TYPE, IDLE_CRON),
    )
    cur.close()


def _lock_control(cur):
    cur.execute(
        f"SELECT mode, cron_expr, reason FROM {control_table()} "
        "WHERE name='catalog_reconcile' FOR UPDATE"
    )
    return cur.fetchone()


def _yielding(control):
    """Whether the locked control row waits on a yield (plan S1)."""
    return bool(control) and control[0] == "waiting" and str(control[2] or "").startswith("yield:")


def cron_minutes(expr):
    """The minutes a cron expression's minute field names, as AudioMuse's
    matcher reads it (``*``, numbers, lists, ``a-b``, ``*/n``, ``a-b/n``,
    ``a/n``); None when the field is unparseable."""
    try:
        field = str(expr).split()[0]
        minutes = set()
        for part in field.split(","):
            base, slash, step = part.strip().partition("/")
            step = int(step) if slash else 1
            if base == "*":
                start, end = 0, 59
            elif "-" in base:
                start, end = (int(value) for value in base.split("-", 1))
            else:
                start = int(base)
                end = 59 if slash else start
            if step < 1 or not 0 <= start <= end <= 59:
                return None
            minutes.update(range(start, end + 1, step))
        return minutes
    except (ValueError, IndexError):
        return None


def _minute_field(minutes):
    """``0,1,3-31,33-59`` for a set of minutes: runs of three or more as ranges."""
    parts, ordered, index = [], sorted(minutes), 0
    while index < len(ordered):
        end = index
        while end + 1 < len(ordered) and ordered[end + 1] == ordered[end] + 1:
            end += 1
        if end - index >= 2:
            parts.append(f"{ordered[index]}-{ordered[end]}")
        else:
            parts.extend(str(value) for value in ordered[index:end + 1])
        index = end + 1
    return ",".join(parts)


def lumae_row_minutes(cur):
    """The minutes at which Lumae's other enabled cron rows are due.

    AudioMuse's minute pass dispatches due rows in cron.id order, and the
    watchdog's row comes first, so a watchdog due in the same minute was
    queued first and the other row refused (the reporter's 06:02). Minutes
    only: a daily row costs the watchdog one minute an hour. A row due every
    minute, or with an unparseable minute field, excludes nothing.
    """
    cur.execute(
        "SELECT cron_expr FROM cron WHERE starts_with(task_type, %s) AND task_type <> %s "
        "AND enabled",
        (LUMAE_TASK_TYPE_PREFIX, CATALOG_RECONCILE_TASK_TYPE),
    )
    excluded = set()
    for (expr,) in cur.fetchall() or ():
        minutes = cron_minutes(expr)
        if minutes and len(minutes) < 60:
            excluded |= minutes
    return excluded


def avoid_minutes(cron_expr, excluded):
    """``cron_expr`` without the ``excluded`` minutes; unchanged when it
    would have none left or its minute field is unparseable."""
    minutes = cron_minutes(cron_expr)
    if not minutes or not excluded or not minutes & excluded or not minutes - excluded:
        return cron_expr
    return " ".join([_minute_field(minutes - excluded)] + str(cron_expr).split()[1:])


def yield_cron(excluded=(), now=None, rng=None):
    """One random minute YIELD_MINUTES ahead (local time, as the host's cron
    matcher reads it), off Lumae's other rows' minutes when it can be."""
    rng = rng or random
    current = time.localtime(now).tm_min
    ahead = [(current + offset) % 60 for offset in range(YIELD_MINUTES[0], YIELD_MINUTES[1] + 1)]
    minute = rng.choice([value for value in ahead if value not in excluded] or ahead)
    return f"{minute} * * * *"


def _set_schedule_locked(cur, mode, cron_expr, reason, next_retry_at=None, *, avoid=True):
    """Store the cadence; returns the cron expression written. ``avoid``
    takes Lumae's other rows' minutes out of it (plan S1)."""
    if avoid:
        cron_expr = avoid_minutes(cron_expr, lumae_row_minutes(cur))
    cur.execute(
        f"""
        UPDATE {control_table()}
           SET mode=%s, cron_expr=%s, reason=%s, next_retry_at=%s, updated_at=now()
         WHERE name='catalog_reconcile'
           AND (mode IS DISTINCT FROM %s
                OR cron_expr IS DISTINCT FROM %s
                OR reason IS DISTINCT FROM %s
                OR next_retry_at IS DISTINCT FROM %s)
        """,
        (
            mode,
            cron_expr,
            str(reason or "")[:500] or None,
            next_retry_at,
            mode,
            cron_expr,
            str(reason or "")[:500] or None,
            next_retry_at,
        ),
    )
    # The cadence only: an administrator's disable on AudioMuse's Scheduled
    # Tasks page stays (1.6.1 re-enabled the row on every change, AUD-18).
    cur.execute(
        "UPDATE cron SET name=%s, cron_expr=%s "
        "WHERE task_type=%s AND (name IS DISTINCT FROM %s "
        "OR cron_expr IS DISTINCT FROM %s)",
        (
            CATALOG_RECONCILE_TASK_TYPE,
            cron_expr,
            CATALOG_RECONCILE_TASK_TYPE,
            CATALOG_RECONCILE_TASK_TYPE,
            cron_expr,
        ),
    )
    return cron_expr


def arm_reconcile(db, reason="work_admitted", commit=False):
    """Switch to minute cadence inside the caller's work-admission transaction.

    A yield (plan S1) holds: the work is durable, and the tick that finds
    nothing to yield to takes it. Minute cadence here put a fire back on every
    minute, ahead of the host's retry pass, whenever a song was analysed or
    the app asked for work."""
    cur = db.cursor()
    if not _yielding(_lock_control(cur)):
        _set_schedule_locked(cur, "active", ACTIVE_CRON, reason)
    cur.close()
    if commit:
        db.commit()


def set_paused(db, paused, commit=True):
    if not paused:
        return reconcile_schedule_from_state(db, paused=False, commit=commit)
    cur = db.cursor()
    _lock_control(cur)
    cron_expr = _set_schedule_locked(cur, "paused", IDLE_CRON, "maintenance_paused")
    cur.close()
    if commit:
        db.commit()
    return {"mode": "paused", "cron_expr": cron_expr, "next_retry_at": None}


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
            -- SQ.1: a catalogue refresh an app asked for while the queue was busy.
            SELECT 'ready', 0, NULL::timestamp
              FROM {_work_table('catalog_state')}
             WHERE refresh_wanted_at IS NOT NULL
            UNION ALL
            SELECT 'ready', 0, NULL::timestamp
             WHERE EXISTS (SELECT 1 FROM {_work_table('source_profiles')}
                            WHERE status='pending_interactive' AND last_error='{DEFERRED_MARKER}')
                OR EXISTS (SELECT 1 FROM {_work_table('edge_profile_jobs')}
                            WHERE status='pending' AND priority='interactive'
                              AND last_error='{DEFERRED_MARKER}')
            UNION ALL
            -- A re-check of an unchanged album (S7) is idle work: it runs on
            -- the idle cadence and never arms minute cadence.
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
               GREATEST(0, EXTRACT(EPOCH FROM (retry.next_retry_at - now())))
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


def _backoff_cron(attempt, retry_in_seconds):
    """Back off by attempt, and never tick faster than the soonest retry needs.

    A cadence whose gap fits in the time left before the retry is due still
    ticks at or before that moment, so a retry an hour away (a MusicBrainz
    allowance deferral) waits on the hourly sweep and the cadence tightens as
    it nears, instead of an empty AudioMuse task every minute meanwhile.
    """
    step = min(max(int(attempt or 0) - 1, 0), len(BACKOFF_LADDER) - 1)
    if retry_in_seconds is not None:
        for index, (_cron_expr, gap_seconds) in enumerate(BACKOFF_LADDER):
            if retry_in_seconds >= gap_seconds:
                step = max(step, index)
    return BACKOFF_LADDER[step][0]


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


def reconcile_schedule_from_state(db, paused=False, commit=True, yield_to=None,
                                  host_retries=True, keep_yield=False):
    """Choose cadence from durable state while excluding concurrent lost wakes.

    ``yield_to``: the tick left the queue to that host work (plan S1); the
    watchdog waits on the yield cadence until a tick finds nothing to yield to.
    ``host_retries``: the host retries a refused fire (``cron_retry``).
    ``keep_yield``: a caller outside a tick (startup, the install) leaves a
    yield as it is; only a tick ends one.
    """
    cur = db.cursor()
    control = _lock_control(cur)
    _recover_interrupted_events(cur)
    if keep_yield and not paused and _yielding(control):
        cur.close()
        if commit:
            db.commit()
        return {"mode": control[0], "cron_expr": control[1], "next_retry_at": None}
    avoid = True
    if paused:
        mode, cron_expr, reason, next_retry_at = (
            "paused",
            IDLE_CRON,
            "maintenance_paused",
            None,
        )
    elif yield_to:
        mode, reason, next_retry_at = "waiting", f"yield:{yield_to}", None
        if host_retries:
            cron_expr, avoid = yield_cron(lumae_row_minutes(cur)), False
        else:
            # Before cron_retry (AudioMuse 3.2.x) a fire refused while the
            # host's job runs is not retried: a yield minute would wait an
            # hour after the job, and there is no retry pass to meet.
            cron_expr = WAITING_CRON
    else:
        state = _work_summary(cur)
        next_retry_at = state["next_retry_at"]
        if state["ready"]:
            mode, cron_expr, reason = "active", ACTIVE_CRON, "runnable_work"
            next_retry_at = None
        elif state["waiting"]:
            mode, cron_expr, reason = "waiting", WAITING_CRON, "analysis_parent_running"
            next_retry_at = None
        elif next_retry_at is not None:
            cron_expr = _backoff_cron(state["retry_count"], state["retry_in_seconds"])
            mode, reason = "backoff", "retry_backoff"
        else:
            mode, cron_expr, reason = "idle", IDLE_CRON, "catalog_current"
    cron_expr = _set_schedule_locked(cur, mode, cron_expr, reason, next_retry_at, avoid=avoid)
    cur.execute(
        f"UPDATE {control_table()} SET last_sweep_at=now() "
        "WHERE name='catalog_reconcile'"
    )
    cur.close()
    if commit:
        db.commit()
    return {"mode": mode, "cron_expr": cron_expr, "next_retry_at": next_retry_at}


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
    cur.execute(
        f"SELECT mode, cron_expr, reason, last_sweep_at, next_retry_at, updated_at "
        f"FROM {control_table()} WHERE name='catalog_reconcile'"
    )
    control = cur.fetchone()
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
    cur.execute(
        "SELECT task_type, enabled FROM cron WHERE starts_with(task_type, %s)",
        (LUMAE_TASK_TYPE_PREFIX,),
    )
    schedules = {str(task_type): enabled for task_type, enabled in cur.fetchall()}
    cur.close()
    keys = (
        "event_id", "server_id", "catalog_instance_id", "action", "status", "phase",
        "attempt", "progress_current", "progress_total", "started_at", "heartbeat_at",
        "completed_at", "duration_ms", "summary", "last_error", "next_retry_at",
    )
    events = [dict(zip(keys, row)) for row in rows]
    return {
        "control": {
            "mode": str(control[0]) if control else "unknown",
            "cron_expr": str(control[1]) if control else "unknown",
            "reason": str(control[2] or "") if control else "",
            "last_sweep_at": control[3] if control else None,
            "next_retry_at": control[4] if control else None,
            "updated_at": control[5] if control else None,
        },
        "pending": pending,
        "events": events,
        # task_type -> enabled for every Lumae cron row.
        "schedules": schedules,
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


def remaining_work(db):
    """Per-queue counts for the tick report: waveform tracks left on active
    sources (from the settings snapshot), unfinished edge passes and
    relationship builds, and due credits albums and MusicBrainz lookups."""
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT
            (SELECT COALESCE(sum(GREATEST(0, ss.profile_total_count - ss.profile_ready_count
                                             - ss.profile_failed_count - ss.profile_skipped_count)), 0)
               FROM {table('status_summary')} ss
               JOIN {table('catalog_sources')} s USING (catalog_instance_id)
              WHERE s.rebind_status='active'),
            (SELECT count(*) FROM {_work_table('edge_backfill_state')}
              WHERE status IN ('queued', 'running', 'failed')),
            (SELECT count(*) FROM {_work_table('relationship_state')}
              WHERE status IN ('queued', 'failed', 'waiting_for_index', 'running')),
            (SELECT count(*) FROM {_work_table('credits_jobs')}
              WHERE status IN ('pending', 'failed', 'running') AND not_before <= now()),
            (SELECT count(*) FROM {_work_table('metadata_jobs')}
              WHERE status IN ('pending', 'deferred', 'running') AND not_before <= now())
        """
    )
    row = cur.fetchone()
    cur.close()
    db.commit()
    keys = ("waveforms", "edge_passes", "relationships", "credits_albums", "lookups")
    return {key: int(value or 0) for key, value in zip(keys, row or ())}


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


def yield_status(db):
    """The watchdog's current yield (S12): what it waits for, and each
    schedule AudioMuse refused and will retry with how long ago it was
    refused (``cron_retry``, AudioMuse 3.3.0+; None without it)."""
    cur = db.cursor()
    cur.execute(f"SELECT mode, reason FROM {control_table()} WHERE name='catalog_reconcile'")
    control = cur.fetchone()
    waiting_for = None
    if control and str(control[0]) == "waiting" and str(control[1] or "").startswith("yield:"):
        waiting_for = str(control[1])[len("yield:"):]
    refused = None
    cur.execute("SELECT to_regclass('cron_retry') IS NOT NULL")
    if cur.fetchone()[0]:
        cur.execute(
            """SELECT task_type, EXTRACT(EPOCH FROM now())
                      - NULLIF(to_jsonb(r)->>'first_blocked_at', '')::double precision
                 FROM cron_retry r ORDER BY task_type LIMIT 10"""
        )
        refused = [(str(task_type), float(age) if age is not None else None)
                   for task_type, age in cur.fetchall()]
    cur.close()
    return {"waiting_for": waiting_for, "refused": refused}


# (singular, plural) of what one unit of each action is, in report order:
# tracks first.
ACTION_UNITS = {
    "profile_backfill": ("waveform", "waveforms"),
    "edge_backfill": ("edge", "edges"),
    "loudness_backfill": ("loudness measurement", "loudness measurements"),
    "edge_schema_v3": ("edge upgraded to v3", "edges upgraded to v3"),
    "interactive": ("on-demand track", "on-demand tracks"),
    "credits": ("credits album", "credits albums"),
    "music_metadata": ("MusicBrainz lookup", "MusicBrainz lookups"),
    "relationships": ("relationship step", "relationship steps"),
    "analysis_run": ("analysis run", "analysis runs"),
    "catalog_preparation": ("catalogue preparation", "catalogue preparations"),
}
# Their counts are tracks analysed: the host's "Songs analyzed" note.
TRACK_ACTIONS = ("interactive", "profile_backfill", "edge_backfill")


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


def _units(count, action):
    singular, plural = ACTION_UNITS.get(action, (action, action))
    return f"{count:,} {singular if count == 1 else plural}"


class TickReport:
    """What one watchdog tick did, returned as the host's task details.

    AudioMuse takes ``status_message``, then ``message``, as the task's
    message, and labels a type containing "analysis" with ``tracks_analyzed``
    ("Songs analyzed: N") unless AM-2 shows the message instead.
    """

    def __init__(self):
        self.started = time.monotonic()
        self.actions = []
        self.requested = 0
        self.yielded_to = None
        self.remaining = None
        self.cadence = None

    def add(self, action, result, duration_ms):
        """One run of ``action``; runs of the same action share one entry, so
        the details stay small however many runs a tick has."""
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

    def tracks_analyzed(self):
        return sum(entry["count"] for entry in self.actions if entry["action"] in TRACK_ACTIONS)

    def message(self, status):
        if status == "paused":
            return "Lumae: paused"
        if status == "yielded":
            return f"Lumae: yielded to {self.yielded_to}"[:TICK_MESSAGE_MAX]
        if status == "same_dispatch":
            return "Lumae: ran earlier in this task"
        phrases = []
        totals = {entry["action"]: (entry["count"], entry["failed"]) for entry in self.actions}
        for action in ACTION_UNITS:
            done, failed = totals.get(action, (0, 0))
            if done or failed:
                phrases.append(_units(done, action) + (f" ({failed:,} failed)" if failed else ""))
        left = (self.remaining or {}).get("waveforms") or 0
        if phrases:
            body = ", ".join(phrases) + f" in {round(time.monotonic() - self.started)} s"
            if status == "failed":
                body += ", then an action failed"
            elif self.yielded_to:
                body += f", then yielded to {self.yielded_to}"
        elif status == "failed":
            body = "an action failed"
        else:
            body = "nothing ready" if left else "up to date"
        tail = f"; {left:,} tracks left" if left else ""
        for message in (f"Lumae: {body}{tail}", f"Lumae: {body}"):
            if len(message) <= TICK_MESSAGE_MAX:
                return message
        return f"Lumae: {body}"[:TICK_MESSAGE_MAX - 1] + "…"

    def result(self, status, **extra):
        message = self.message(status)
        return {
            "status": status,
            "message": message,
            "status_message": message,
            "tracks_analyzed": self.tracks_analyzed(),
            "actions": list(self.actions),
            "requested": int(self.requested or 0),
            "remaining": self.remaining,
            "yielded_to": self.yielded_to,
            "cadence": self.cadence,
            **extra,
        }

    def log(self, status):
        """The tick's one INFO line."""
        actions = ", ".join(
            f"{entry['action']} {entry['status']} {entry['count']}"
            + (f"/{entry['failed']} failed" if entry["failed"] else "")
            + (f" in {entry['runs']} runs" if entry["runs"] > 1 else "") + f" {entry['ms']} ms"
            for entry in self.actions
        ) or "none"
        remaining = ", ".join(f"{key} {value:,}" for key, value in (self.remaining or {}).items())
        logger.info(
            "lumae_analysis watchdog: %s [status %s; actions %s; remaining %s; cadence %s%s]",
            self.message(status), status, actions, remaining or "unknown",
            self.cadence or "unchanged",
            f"; yielded to {self.yielded_to}" if self.yielded_to else "",
        )


def _plural(count, singular, plural=None):
    return f"{count:,} {singular if count == 1 else (plural or singular + 's')}"


def task_message(result, sentence):
    """``result`` with ``sentence`` as AudioMuse's task message.

    Lumae's other schedules say what they did, like the tick (LA-3); without
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


def host_blocking_task_types():
    """The root task types AudioMuse's cron gate waits for, besides plugins."""
    types = set(HOST_BLOCKING_TASK_TYPES)
    try:
        host = importlib.import_module("task_types")
        types.update(str(name) for name in getattr(host, "BATCH_GATE_TASK_TYPES", ())
                     if not str(name).startswith("plugin."))
    except Exception:
        pass
    return sorted(types)


class HostYield:
    """What a tick leaves AudioMuse's queue to (plan S1, AUD-13).

    - A ``cron_retry`` row other than the watchdog's own: a schedule the host
      refused while a plugin task was live (its own, another plugin's, or one
      of Lumae's other rows). At minute cadence its retry always lost to the
      next watchdog fire until it expired. ``cron_retry`` exists from
      AudioMuse 3.3.0; before, the host's gate ignores plugin tasks.
    - A live AudioMuse main job or side job: the host's gate refuses every
      start behind it.
    Each check fails open: a missing table or a raising query turns only that
    check off for the rest of the tick, logged once at DEBUG.
    """

    def __init__(self):
        self.cron_retry = None  # None: not checked yet this tick
        self.live_tasks = True

    def check(self, db):
        target = None
        if self.cron_retry is not False:
            target = self._safely(db, "cron_retry", self._retry_row)
        if target is None and self.live_tasks:
            target = self._safely(db, "live_tasks", self._live_root)
        return target

    def _safely(self, db, check, query):
        cur = None
        try:
            cur = db.cursor()
            target = query(cur)
            cur.close()
            db.commit()
            return target
        except Exception:
            rollback = getattr(db, "rollback", None)
            if callable(rollback):
                try:
                    rollback()
                except Exception:
                    pass
            setattr(self, check, False)
            logger.debug("lumae_analysis turned off the %s yield check for this tick", check,
                         exc_info=True)
            return None

    def _retry_row(self, cur):
        if self.cron_retry is None:
            cur.execute("SELECT to_regclass('cron_retry') IS NOT NULL")
            self.cron_retry = bool(cur.fetchone()[0])
            if not self.cron_retry:
                return None
        cur.execute(
            "SELECT task_type FROM cron_retry WHERE task_type <> %s ORDER BY task_type LIMIT 1",
            (CATALOG_RECONCILE_TASK_TYPE,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None

    def _live_root(self, cur):
        cur.execute(
            """
            SELECT task_type FROM task_status
             WHERE parent_task_id IS NULL AND task_type = ANY(%s)
               AND status IN ('NEW', 'QUEUED', 'PENDING', 'STARTED', 'RUNNING', 'PROGRESS')
             ORDER BY task_type LIMIT 1
            """,
            (host_blocking_task_types(),),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None


def action_averages(db):
    """Average milliseconds of each action's last ACTION_AVERAGE_RUNS finished runs."""
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT action, avg(duration_ms) FROM (
            SELECT action, duration_ms,
                   row_number() OVER (PARTITION BY action ORDER BY event_id DESC) AS position
              FROM {events_table()}
             WHERE status IN ('success', 'deferred') AND duration_ms IS NOT NULL
        ) recent
         WHERE position <= %s
         GROUP BY action
        """,
        (ACTION_AVERAGE_RUNS,),
    )
    averages = {str(action): float(value) for action, value in cur.fetchall() if value is not None}
    cur.close()
    db.commit()
    return averages


class TickBudget:
    """The tick's deadline (plan S2), on the monotonic clock."""

    def __init__(self, seconds=None, averages=None):
        self.deadline = time.monotonic() + (TICK_BUDGET_SECONDS if seconds is None else seconds)
        self.averages = dict(averages or {})
        self.started = 0

    def remaining(self):
        return max(0.0, self.deadline - time.monotonic())

    def fits(self, action):
        """Whether ``action`` may start: the tick's first action always may,
        a later one only when its recent average fits in what is left (an
        elastic action: ELASTIC_MIN_SECONDS of it)."""
        if not self.started:
            return True
        remaining = self.remaining()
        average = self.averages.get(action)
        if average is not None and action in ELASTIC_ACTIONS:
            average = min(average, ELASTIC_MIN_SECONDS * 1000.0)
        return remaining > 0 and (average is None or average / 1000.0 <= remaining)

    def start(self):
        self.started += 1
