"""Durable owner-bound synchronization, fenced by a PostgreSQL session lock."""

import uuid
from psycopg2.extras import DictCursor
from plugin.api import get_db, table

LOCK_CLASS = 0x46414C42
TASK_TYPE = "plugin.federated_albums.sync_reconcile"


def migrate(db):
    with db.cursor() as cur:
        for definition in (
            "sync_token TEXT",
            "sync_status TEXT NOT NULL DEFAULT 'idle'",
            "sync_retry_at TIMESTAMPTZ",
            "sync_attempts INTEGER NOT NULL DEFAULT 0",
        ):
            cur.execute(
                f"ALTER TABLE {table('connections')} ADD COLUMN IF NOT EXISTS {definition}"
            )
        cur.execute(
            """INSERT INTO cron(name,task_type,cron_expr,enabled) VALUES(%s,%s,'* * * * *',TRUE)
            ON CONFLICT(task_type) DO NOTHING""",
            (TASK_TYPE, TASK_TYPE),
        )


def request_sync(db, connection_id, owner):
    with db.cursor() as cur:
        cur.execute(
            f"""UPDATE {table('connections')} SET sync_status='pending',sync_token=%s,
            sync_retry_at=NULL,sync_attempts=0,last_error=NULL WHERE id=%s AND owner=%s
            AND sync_status NOT IN ('pending','running') RETURNING id""",
            (str(uuid.uuid4()), connection_id, owner),
        )
        if not cur.fetchone():
            cur.execute(
                f"SELECT id FROM {table('connections')} WHERE id=%s AND owner=%s",
                (connection_id, owner),
            )
            if not cur.fetchone():
                db.rollback()
                raise LookupError("Friend connection not found")
    db.commit()
    return {"status": "queued", "id": connection_id}


def run_one(fetch_and_publish):
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            f"""SELECT id FROM {table('connections')} WHERE sync_status IN ('pending','running')
            OR (sync_status='failed' AND sync_retry_at<=now() AND sync_attempts<5)
            ORDER BY id LIMIT 32"""
        )
        ids = [row[0] for row in cur.fetchall()]
    db.commit()
    for connection_id in ids:
        with db.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(%s,hashtext(%s))",
                (LOCK_CLASS, str(connection_id)),
            )
            acquired = cur.fetchone()[0]
        db.commit()
        if not acquired:
            continue
        try:
            token = str(uuid.uuid4())
            with db.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    f"""UPDATE {table('connections')} SET sync_token=%s,sync_status='running',
                    sync_attempts=sync_attempts+1 WHERE id=%s AND
                    (sync_status IN ('pending','running') OR (sync_status='failed' AND sync_retry_at<=now() AND sync_attempts<5))
                    RETURNING id,owner""",
                    (token, connection_id),
                )
                row = cur.fetchone()
            db.commit()
            if not row:
                continue
            try:
                result = fetch_and_publish(connection_id, row["owner"], token)
                return {"status": "complete", **result}
            except Exception:
                db.rollback()
                with db.cursor() as cur:
                    # Never store credentials/URLs from remote exception text.
                    cur.execute(
                        f"""UPDATE {table('connections')} SET sync_status='failed',last_error='Friend sync failed; check connection and retry',
                        sync_retry_at=now()+interval '5 minutes' WHERE id=%s AND owner=%s AND sync_token=%s""",
                        (connection_id, row["owner"], token),
                    )
                db.commit()
                return {"status": "failed", "id": connection_id}
        finally:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(%s,hashtext(%s))",
                    (LOCK_CLASS, str(connection_id)),
                )
            db.commit()
    return {"status": "idle"}
