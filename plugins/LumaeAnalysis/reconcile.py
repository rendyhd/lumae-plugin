"""Adaptive, durable scheduling and observability for catalogue reconciliation."""

from contextvars import ContextVar
from datetime import datetime, timezone
import json

from plugin.api import get_db, table


CATALOG_RECONCILE_TASK_TYPE = "plugin.lumae_analysis.catalog_reconcile"
ACTIVE_CRON = "* * * * *"
WAITING_CRON = "*/5 * * * *"
BACKOFF_15_CRON = "*/15 * * * *"
IDLE_CRON = "11 * * * *"
EVENT_RETENTION_PER_SOURCE = 25

_current_event_id = ContextVar("lumae_reconcile_event_id", default=None)


def control_table():
    return table("reconcile_control")


def events_table():
    return table("reconcile_events")


def _work_table(name):
    return table(name)


def migrate_reconcile(db):
    """Create additive operational state without changing app-facing schemas."""
    cur = db.cursor()
    for name in ("analysis_runs", "preparation_state", "profile_backfill_state", "relationship_state"):
        cur.execute(
            f"ALTER TABLE {_work_table(name)} "
            "ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0"
        )
        cur.execute(
            f"ALTER TABLE {_work_table(name)} "
            "ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ"
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
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {table('reconcile_events_source_idx')} "
        f"ON {events_table()} (catalog_instance_id, event_id DESC)"
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
        f"SELECT mode, cron_expr FROM {control_table()} "
        "WHERE name='catalog_reconcile' FOR UPDATE"
    )
    return cur.fetchone()


def _set_schedule_locked(cur, mode, cron_expr, reason, next_retry_at=None):
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
    cur.execute(
        "UPDATE cron SET name=%s, cron_expr=%s, enabled=TRUE "
        "WHERE task_type=%s AND (name IS DISTINCT FROM %s "
        "OR cron_expr IS DISTINCT FROM %s OR enabled IS DISTINCT FROM TRUE)",
        (
            CATALOG_RECONCILE_TASK_TYPE,
            cron_expr,
            CATALOG_RECONCILE_TASK_TYPE,
            CATALOG_RECONCILE_TASK_TYPE,
            cron_expr,
        ),
    )


def arm_reconcile(db, reason="work_admitted", commit=False):
    """Switch to minute cadence inside the caller's work-admission transaction."""
    cur = db.cursor()
    _lock_control(cur)
    _set_schedule_locked(cur, "active", ACTIVE_CRON, reason)
    cur.close()
    if commit:
        db.commit()


def set_paused(db, paused, commit=True):
    if not paused:
        return reconcile_schedule_from_state(db, paused=False, commit=commit)
    cur = db.cursor()
    _lock_control(cur)
    _set_schedule_locked(cur, "paused", IDLE_CRON, "maintenance_paused")
    cur.close()
    if commit:
        db.commit()
    return {"mode": "paused", "cron_expr": IDLE_CRON, "next_retry_at": None}


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
                OR (status='running' AND updated_at < now() - interval '2 hours')
            UNION ALL
            SELECT CASE WHEN next_retry_at > now() THEN 'retry' ELSE 'ready' END,
                   retry_count, next_retry_at
              FROM {_work_table('profile_backfill_state')}
             WHERE status IN ('queued', 'failed')
                OR (status='running' AND updated_at < now() - interval '30 minutes')
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
               retry.retry_count, retry.next_retry_at
          FROM totals LEFT JOIN retry ON TRUE
        """
    )
    row = cur.fetchone() or (0, 0, None, None)
    return {
        "ready": int(row[0] or 0),
        "waiting": int(row[1] or 0),
        "retry_count": int(row[2] or 0),
        "next_retry_at": row[3],
    }


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


def reconcile_schedule_from_state(db, paused=False, commit=True):
    """Choose cadence from durable state while excluding concurrent lost wakes."""
    cur = db.cursor()
    _lock_control(cur)
    _recover_interrupted_events(cur)
    if paused:
        mode, cron_expr, reason, next_retry_at = (
            "paused",
            IDLE_CRON,
            "maintenance_paused",
            None,
        )
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
            attempt = state["retry_count"]
            if attempt <= 1:
                cron_expr = ACTIVE_CRON
            elif attempt == 2:
                cron_expr = WAITING_CRON
            elif attempt == 3:
                cron_expr = BACKOFF_15_CRON
            else:
                cron_expr = IDLE_CRON
            mode, reason = "backoff", "retry_backoff"
        else:
            mode, cron_expr, reason = "idle", IDLE_CRON, "catalog_current"
    _set_schedule_locked(cur, mode, cron_expr, reason, next_retry_at)
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
    raise ValueError(f"Unknown reconciliation action: {action}")


def update_work_retry(db, action, catalog_instance_id, work_key=None, *, failed=True):
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
        cur.execute(
            f"UPDATE {_work_table(table_name)} "
            f"SET retry_count=0, next_retry_at=NULL WHERE {where}",
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
    cur.execute(
        f"""
        DELETE FROM {events_table()} e
         USING (
             SELECT event_id FROM (
                 SELECT event_id,
                        row_number() OVER (
                            PARTITION BY COALESCE(catalog_instance_id, '')
                            ORDER BY event_id DESC
                        ) AS position
                   FROM {events_table()}
                  WHERE status <> 'running'
             ) ranked
             WHERE position > %s
         ) expired
         WHERE e.event_id=expired.event_id
        """,
        (EVENT_RETENTION_PER_SOURCE,),
    )
    cur.close()
    db.commit()
    _current_event_id.set(None)


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
        (EVENT_RETENTION_PER_SOURCE,),
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
