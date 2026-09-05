"""Durable DJ dispatcher and worker-owned model maintenance."""

from plugin.api import table

TASK_TYPE = "plugin.lumae_analysis.dj_reconcile"
HOST_CONTRACT = "lumae-dj-host-v1"
RUNTIME_PREPARE_LOCK_ID = 0x4C554D4145444A32


def migrate(db):
    cur = db.cursor()
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table('dj_control')} (
        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
        remove_token TEXT, remove_status TEXT, remove_error TEXT,
        removed_files INTEGER, next_dispatch_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"""
    )
    cur.execute(
        f"INSERT INTO {table('dj_control')}(singleton) VALUES(TRUE) ON CONFLICT DO NOTHING"
    )
    cur.close()


def configure_schedule(db, enabled):
    cur = db.cursor()
    cur.execute(
        """INSERT INTO cron(name,task_type,cron_expr,enabled)
        VALUES(%s,%s,%s,%s) ON CONFLICT(task_type) DO UPDATE SET
        cron_expr=EXCLUDED.cron_expr, enabled=EXCLUDED.enabled""",
        (TASK_TYPE, TASK_TYPE, "* * * * *", bool(enabled)),
    )
    db.commit()
    cur.close()


def state(db):
    cur = db.cursor()
    try:
        cur.execute(
            f"""SELECT remove_token,remove_status,remove_error,removed_files
            FROM {table('dj_control')} WHERE singleton=TRUE"""
        )
        row = cur.fetchone()
        return (
            dict(zip(("token", "status", "error", "removed_files"), row)) if row else {}
        )
    finally:
        cur.close()


def request_removal(db):
    import uuid
    from .dj_jobs import contract

    token = str(uuid.uuid4())
    cur = db.cursor()
    try:
        cur.execute(
            f"""UPDATE {table('dj_control')} SET remove_token=%s,remove_status='pending',
            remove_error=NULL,removed_files=NULL,next_dispatch_at=NULL,updated_at=now()
            WHERE singleton=TRUE""",
            (token,),
        )
        for version in (2, 3):
            cur.execute(
                f"""UPDATE {contract(version).jobs} SET status='cancelled',
                error_code='models_removal_requested',next_retry_at=NULL,completed_at=now(),updated_at=now()
                WHERE status IN ('pending','running','failed')"""
            )
        db.commit()
    finally:
        cur.close()
    configure_schedule(db, True)
    return token


def complete_removal(db, token, removed_files=None, error=None):
    cur = db.cursor()
    try:
        cur.execute(
            f"""UPDATE {table('dj_control')} SET remove_status=%s,remove_error=%s,
            removed_files=%s,next_dispatch_at=NULL,updated_at=now()
            WHERE singleton=TRUE AND remove_token=%s""",
            ("failed" if error else "complete", error, removed_files, token),
        )
        db.commit()
    finally:
        cur.close()


def claim_dispatch(db):
    cur = db.cursor()
    try:
        cur.execute(
            f"""UPDATE {table('dj_control')} SET next_dispatch_at=now()+interval '2 minutes'
            WHERE singleton=TRUE AND (next_dispatch_at IS NULL OR next_dispatch_at<=now())
            RETURNING singleton"""
        )
        claimed = cur.fetchone() is not None
        db.commit()
        return claimed
    finally:
        cur.close()


def worker_started(db):
    cur = db.cursor()
    try:
        cur.execute(
            f"UPDATE {table('dj_control')} SET next_dispatch_at=NULL WHERE singleton=TRUE"
        )
        db.commit()
    finally:
        cur.close()


def execution_busy(db):
    from .dj_jobs import LOCK_CLASS, LOCK_KEY

    cur = db.cursor()
    try:
        cur.execute(
            """SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'
            AND ((classid=%s AND objid=%s AND objsubid=2)
                 OR (classid=%s AND objid=%s AND objsubid=1)) AND granted)""",
            (
                LOCK_CLASS,
                LOCK_KEY,
                RUNTIME_PREPARE_LOCK_ID >> 32,
                RUNTIME_PREPARE_LOCK_ID & 0xFFFFFFFF,
            ),
        )
        row = cur.fetchone()
        return bool(row and row[0])
    finally:
        cur.close()
