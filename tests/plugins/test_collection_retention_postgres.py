"""F2 (collections growth): bounded retention for the rows collections keep
forever otherwise — idempotency receipts (``collection_mutations``,
``shelf_mutations``) and chunked-restore progress (``collection_restores``).

Uses the real migration (``migrated_db``); every table, column and index
these tests rely on comes from ``plugins.LumaeAnalysis.migrate``.
"""

import importlib
import uuid

import pytest
from flask import Flask


def _load():
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    shelves = importlib.import_module("plugins.LumaeAnalysis.shelves")
    return manager, shelves


# _begin_mutation calls flask.jsonify, which needs an app context (but no
# request): a bare one, pushed only around that call, is enough.
_APP = Flask(__name__)


def _insert_mutation(cur, manager, principal, key, age_days=None, fingerprint="fp",
                      payload='{"ok": true}', status=200, collection_id=None):
    if age_days is None:
        created = "now()"
    else:
        created = f"now() - interval '{age_days} days'"
    cur.execute(
        f"INSERT INTO {manager.collection_mutations_table()} "
        "(principal, idempotency_key, response_payload, status_code, "
        "request_fingerprint, fingerprint_version, collection_id, created_at) "
        f"VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, {created})",
        (principal, key, payload, status, fingerprint, manager.FINGERPRINT_VERSION, collection_id),
    )


def _insert_restore(cur, manager, principal, key, chunks_done, chunk_count,
                     age_days=None, fingerprint="fp"):
    if age_days is None:
        updated = "now()"
    else:
        updated = f"now() - interval '{age_days} days'"
    cur.execute(
        f"INSERT INTO {manager.collection_restores_table()} "
        "(principal, idempotency_key, request_fingerprint, restore_id, chunk_rows, "
        f"chunk_count, chunks_done, updated_at) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s, {updated})",
        (principal, key, fingerprint, str(uuid.uuid4()), 2000, chunk_count, chunks_done),
    )


def _count(cur, relation):
    cur.execute(f"SELECT count(*) FROM {relation}")
    return cur.fetchone()[0]


# --- collection_mutations receipt TTL -----------------------------------

def test_old_receipts_are_deleted_recent_ones_are_kept(migrated_db):
    manager, _ = _load()
    with migrated_db.cursor() as cur:
        _insert_mutation(cur, manager, "alice", "old-1", age_days=manager.RECEIPT_RETENTION_DAYS + 1)
        _insert_mutation(cur, manager, "alice", "old-2", age_days=manager.RECEIPT_RETENTION_DAYS + 10)
        _insert_mutation(cur, manager, "alice", "recent-1", age_days=1)
        _insert_mutation(cur, manager, "alice", "boundary", age_days=manager.RECEIPT_RETENTION_DAYS - 1)
    migrated_db.commit()

    deleted = manager.purge_expired_collection_mutations(migrated_db)
    assert deleted == 2

    with migrated_db.cursor() as cur:
        cur.execute(
            f"SELECT idempotency_key FROM {manager.collection_mutations_table()} ORDER BY idempotency_key"
        )
        remaining = {row[0] for row in cur.fetchall()}
    assert remaining == {"recent-1", "boundary"}


def test_replay_with_an_expired_key_reapplies(migrated_db):
    manager, _ = _load()
    principal, key, fingerprint = "alice", "expired-key", "fp-1"
    with migrated_db.cursor() as cur:
        _insert_mutation(
            cur, manager, principal, key,
            age_days=manager.RECEIPT_RETENTION_DAYS + 1, fingerprint=fingerprint,
        )
    migrated_db.commit()

    manager.purge_expired_collection_mutations(migrated_db)

    with _APP.app_context(), migrated_db.cursor() as cur:
        early = manager._begin_mutation(migrated_db, cur, principal, key, fingerprint)
    migrated_db.rollback()
    # No receipt found: _begin_mutation lets the request through to re-apply.
    assert early is None


def test_replay_within_the_ttl_returns_the_stored_response(migrated_db):
    manager, _ = _load()
    principal, key, fingerprint = "alice", "fresh-key", "fp-2"
    with migrated_db.cursor() as cur:
        _insert_mutation(
            cur, manager, principal, key, age_days=1, fingerprint=fingerprint,
            payload='{"stored": "yes"}', status=201,
        )
    migrated_db.commit()

    manager.purge_expired_collection_mutations(migrated_db)

    with _APP.app_context(), migrated_db.cursor() as cur:
        early = manager._begin_mutation(migrated_db, cur, principal, key, fingerprint)
        assert early is not None
        body, status, headers = early
        body = body.get_json()
    migrated_db.rollback()
    assert status == 201
    assert body == {"stored": "yes"}
    assert headers["Idempotency-Replayed"] == "true"


# --- collection_restores stale-progress cleanup -------------------------

def test_finished_restores_older_than_cutoff_are_deleted(migrated_db):
    manager, _ = _load()
    with migrated_db.cursor() as cur:
        # "Finished" here means abandoned partway (a restore that actually
        # finishes deletes its own row atomically, see _RestoreRun.step) and
        # not touched since well past the cutoff.
        _insert_restore(cur, manager, "alice", "stale-1", chunks_done=1, chunk_count=3,
                         age_days=manager.RESTORE_STALE_DAYS + 1)
    migrated_db.commit()

    deleted = manager.purge_stale_collection_restores(migrated_db)
    assert deleted == 1
    with migrated_db.cursor() as cur:
        assert _count(cur, manager.collection_restores_table()) == 0


def test_an_in_progress_restore_is_never_deleted(migrated_db):
    manager, _ = _load()
    with migrated_db.cursor() as cur:
        _insert_restore(cur, manager, "alice", "active", chunks_done=5, chunk_count=6,
                         age_days=0)
    migrated_db.commit()

    deleted = manager.purge_stale_collection_restores(migrated_db)
    assert deleted == 0
    with migrated_db.cursor() as cur:
        assert _count(cur, manager.collection_restores_table()) == 1


def test_a_resumable_restore_survives(migrated_db):
    manager, _ = _load()
    principal, key, fingerprint = "alice", "resumable", "fp-3"
    with migrated_db.cursor() as cur:
        # Recently touched, still well within the window: a client could
        # legitimately come back with the next chunk any moment.
        _insert_restore(cur, manager, principal, key, chunks_done=0, chunk_count=4,
                         age_days=0, fingerprint=fingerprint)
    migrated_db.commit()

    deleted = manager.purge_stale_collection_restores(migrated_db)
    assert deleted == 0

    # And it is still recognised as the row backing that key's resume.
    with _APP.app_context(), migrated_db.cursor() as cur:
        early = manager._begin_mutation(migrated_db, cur, principal, key, fingerprint)
    migrated_db.rollback()
    assert early is None  # No receipt yet; the restore is free to continue.
    with migrated_db.cursor() as cur:
        assert _count(cur, manager.collection_restores_table()) == 1


# --- batch cap -----------------------------------------------------------

def test_the_batch_cap_is_respected(migrated_db):
    manager, _ = _load()
    with migrated_db.cursor() as cur:
        for i in range(5):
            _insert_mutation(
                cur, manager, "bob", f"old-{i}",
                age_days=manager.RECEIPT_RETENTION_DAYS + 1,
            )
    migrated_db.commit()

    # One batch of at most 2, capped at one batch this call: 2 removed, 3 left.
    deleted = manager._purge_expired_rows(
        migrated_db, manager.collection_mutations_table(), "created_at",
        manager.RECEIPT_RETENTION_DAYS, batch_rows=2, max_batches=1,
    )
    assert deleted == 2
    with migrated_db.cursor() as cur:
        assert _count(cur, manager.collection_mutations_table()) == 3

    # A second call drains the rest, two more batches (2 then 1).
    deleted = manager._purge_expired_rows(
        migrated_db, manager.collection_mutations_table(), "created_at",
        manager.RECEIPT_RETENTION_DAYS, batch_rows=2, max_batches=5,
    )
    assert deleted == 3
    with migrated_db.cursor() as cur:
        assert _count(cur, manager.collection_mutations_table()) == 0


# --- shelf_mutations receipt TTL -----------------------------------------

def test_shelf_receipts_old_deleted_recent_kept(migrated_db):
    manager, shelves = _load()
    relation = shelves.table("shelf_mutations")
    with migrated_db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {relation} (principal, catalog_id, id, response, "
            "request_fingerprint, created_at) VALUES "
            "(%s,%s,%s,%s::jsonb,%s, now() - interval '%s days')",
            ("alice", "cat-1", "old", '{"records": []}', "fp", shelves.RECEIPT_RETENTION_DAYS + 1),
        )
        cur.execute(
            f"INSERT INTO {relation} (principal, catalog_id, id, response, "
            "request_fingerprint, created_at) VALUES "
            "(%s,%s,%s,%s::jsonb,%s, now())",
            ("alice", "cat-1", "recent", '{"records": []}', "fp"),
        )
        # A legacy receipt with no created_at (predates this column): must
        # never be swept, since its true age is unknown.
        cur.execute(
            f"INSERT INTO {relation} (principal, catalog_id, id, response, "
            "request_fingerprint) VALUES (%s,%s,%s,%s::jsonb,%s)",
            ("alice", "cat-1", "legacy", '{"records": []}', None),
        )
    migrated_db.commit()

    deleted = shelves.purge_expired_shelf_mutations(migrated_db)
    assert deleted == 1

    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT id FROM {relation} ORDER BY id")
        remaining = {row[0] for row in cur.fetchall()}
    assert remaining == {"legacy", "recent"}


class _RecordingDb:
    """``db`` for the purge helpers that records every statement they run."""

    def __init__(self, db):
        self._db = db
        self.statements = []

    def cursor(self):
        real, statements = self._db.cursor(), self.statements

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                real.close()

            def execute(self, sql, params=None):
                statements.append(sql)
                return real.execute(sql, params)

            def __getattr__(self, name):
                return getattr(real, name)

        return Cursor()

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()


def test_receipt_purge_takes_no_key_locks_and_restore_batches_stay_small(migrated_db):
    """Each advisory lock holds a slot in the server-wide lock table until
    commit, so a 5,000-row receipt batch must not take one per row; restore
    rows, which do take their key lock, are purged in small batches."""
    manager, _shelves = _load()
    with migrated_db.cursor() as cur:
        for n in range(5):
            _insert_mutation(cur, manager, "user:a", f"old-{n}", age_days=40)
        for n in range(manager.RESTORE_BATCH_ROWS + 3):
            _insert_restore(cur, manager, "user:a", f"r-{n}", 1, 3, age_days=10)
    migrated_db.commit()

    recording = _RecordingDb(migrated_db)
    assert manager.purge_expired_collection_mutations(recording) == 5
    assert not [sql for sql in recording.statements if "advisory" in sql]

    recording = _RecordingDb(migrated_db)
    assert manager.purge_stale_collection_restores(recording) == manager.RESTORE_BATCH_ROWS + 3
    deletes = [sql for sql in recording.statements if sql.startswith("DELETE")]
    locks = [sql for sql in recording.statements if "pg_advisory_xact_lock" in sql]
    assert len(deletes) == 2 and len(locks) == manager.RESTORE_BATCH_ROWS + 3
    assert manager.RESTORE_BATCH_ROWS <= 500


def test_a_receipt_touched_after_selection_is_not_deleted(migrated_db):
    """The delete repeats the age filter: a row refreshed between the batch's
    SELECT and its DELETE survives."""
    manager, _shelves = _load()
    with migrated_db.cursor() as cur:
        _insert_mutation(cur, manager, "user:a", "old", age_days=40)
    migrated_db.commit()

    class Refreshing(_RecordingDb):
        def cursor(self):
            inner = super().cursor()
            db = self._db
            original = inner.execute

            def execute(sql, params=None):
                if sql.startswith("DELETE"):
                    with db.cursor() as other:
                        other.execute(f"UPDATE {manager.collection_mutations_table()} "
                                      "SET created_at = now()")
                return original(sql, params)

            inner.execute = execute
            return inner

    assert manager.purge_expired_collection_mutations(Refreshing(migrated_db)) == 0
    with migrated_db.cursor() as cur:
        assert _count(cur, manager.collection_mutations_table()) == 1


def test_migration_schedules_the_retention_task_and_keeps_an_admin_change(
        migrated_db, run_plugin_migration):
    task_type = "plugin.lumae_analysis.collection_retention"
    with migrated_db.cursor() as cur:
        cur.execute("SELECT cron_expr, enabled FROM cron WHERE task_type=%s", (task_type,))
        assert cur.fetchall() == [("23 3 * * *", True)]
        cur.execute("UPDATE cron SET enabled=FALSE WHERE task_type=%s", (task_type,))
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute("SELECT enabled FROM cron WHERE task_type=%s", (task_type,))
        assert cur.fetchall() == [(False,)]
