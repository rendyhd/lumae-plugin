"""Disposable PostgreSQL 17 regressions for the collection commit frontier."""
import threading

import pytest

from test_collection_mutations_postgres import collection_api, _observe_blocked_backend


def _page(call, cursor=0, limit=200, user="alice"):
    response = call("GET", f"/api/collections/changes?cursor={cursor}&limit={limit}", user=user)
    assert response.status_code == 200
    return response.get_json()


def test_late_commit_global_frontier_and_principal_isolation(collection_api, monkeypatch):
    manager, call, connect = collection_api
    first_inserted = threading.Event()
    release = threading.Event()
    second_seen = threading.Event()
    second_pid = {}
    result = {}
    original_record = manager._record_change
    original_get_db = manager.get_db

    def pause_after_event(*args):
        original_record(*args)
        if threading.current_thread().name == "first":
            first_inserted.set()
            assert release.wait(5)

    def capture_db():
        db = original_get_db()
        if threading.current_thread().name == "second":
            second_pid["pid"] = db.get_backend_pid()
            second_seen.set()
        return db

    monkeypatch.setattr(manager, "_record_change", pause_after_event)
    monkeypatch.setattr(manager, "get_db", capture_db)
    a = threading.Thread(target=lambda: result.update(a=call(
        "POST", "/api/collections", {"id": "alice", "name": "Alice"}, key="a")), name="first")
    b = threading.Thread(target=lambda: result.update(b=call(
        "POST", "/api/collections", {"id": "bob", "name": "Bob"}, key="b", user="bob")), name="second")
    a.start()
    assert first_inserted.wait(5)
    assert _page(call)["changes"] == []
    b.start()
    try:
        assert second_seen.wait(5)
        assert _observe_blocked_backend(connect, second_pid["pid"])
        assert _page(call, user="bob")["changes"] == []
    finally:
        release.set()
        a.join(7)
        b.join(7)
    assert not a.is_alive() and not b.is_alive()
    assert result["a"].status_code == result["b"].status_code == 201
    alice = _page(call)
    bob = _page(call, user="bob")
    assert [row["collection_id"] for row in alice["changes"]] == ["alice"]
    assert [row["collection_id"] for row in bob["changes"]] == ["bob"]
    assert alice["changes"][0]["seq"] < bob["changes"][0]["seq"]
    assert _page(call, alice["next_cursor"])["changes"] == []


def test_rollback_reuses_frontier_number_and_missing_version_fails_closed(collection_api, monkeypatch):
    manager, call, connect = collection_api
    original = manager._record_change

    def fail_after_change(*args):
        original(*args)
        raise RuntimeError("injected rollback")

    monkeypatch.setattr(manager, "_record_change", fail_after_change)
    with pytest.raises(RuntimeError, match="injected rollback"):
        call("POST", "/api/collections", {"id": "failed", "name": "Failed"}, key="failed")
    db = connect()
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {manager.collection_feed_state_table()}")
        assert cur.fetchone()[0] == 0
        cur.execute(f"SELECT count(*) FROM {manager.collection_changes_table()}")
        assert cur.fetchone()[0] == 0
    db.rollback()
    db.close()
    monkeypatch.setattr(manager, "_record_change", original)
    assert call("POST", "/api/collections", {"id": "next", "name": "Next"}).status_code == 201
    assert _page(call)["changes"][0]["seq"] == 1
    db = connect()
    with db.cursor() as cur:
        cur.execute(f"UPDATE {manager.collection_feed_state_table()} SET protocol_version = 999")
    db.commit()
    db.close()
    assert call("GET", "/api/collections/changes").status_code == 503
    assert call("POST", "/api/collections", {"id": "blocked", "name": "Blocked"}).status_code == 503
    db = connect()
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {manager.collection_feed_state_table()}")
    db.commit()
    db.close()
    assert call("GET", "/api/collections/changes").status_code == 503
    assert call("POST", "/api/collections", {"id": "missing", "name": "Missing"}).status_code == 503


def test_multi_event_restore_pagination_tombstone_and_receipt_replay(collection_api):
    manager, call, connect = collection_api
    backup = manager._backup_envelope([
        {"name": "Restored", "items": [
            {"kind": "track", "track_id": "one"},
            {"kind": "track", "track_id": "two"},
        ]},
        {"name": "Another", "items": [{"kind": "track", "track_id": "three"}]},
    ], "personal")
    first = call("POST", "/api/collections/restore", backup, key="restore")
    assert first.status_code == 201
    replay = call("POST", "/api/collections/restore", backup, key="restore")
    assert replay.status_code == 201 and replay.get_json() == first.get_json()
    assert replay.headers["Idempotency-Replayed"] == "true"
    pages = []
    cursor = 0
    for _ in range(5):
        page = _page(call, cursor, 1)
        assert len(page["changes"]) == 1
        assert page["next_cursor"] > cursor
        pages.extend(page["changes"])
        cursor = page["next_cursor"]
    assert [r["entity_kind"] for r in pages] == ["collection", "item", "item", "collection", "item"]
    # 1.2.5 keys are unchanged; K8 (1.3.0) only adds epoch, head_seq,
    # floor_seq and has_more.
    end = _page(call, cursor, 1)
    assert {key: end[key] for key in ("changes", "next_cursor")} == {
        "changes": [], "next_cursor": cursor}
    assert end["has_more"] is False and end["head_seq"] == cursor
    cid = first.get_json()["collections"][0]["id"]
    deleted = call("DELETE", f"/api/collections/{cid}", {})
    assert deleted.status_code == 200
    tail = _page(call, cursor)
    assert len(tail["changes"]) == 1
    assert tail["changes"][0]["operation"] == "delete"
    db = connect()
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {manager.collection_feed_state_table()}")
        assert cur.fetchone()[0] == 6
    db.rollback()
    db.close()


def test_populated_cache_sequence_migration_twice_preserves_epoch_and_head(collection_api):
    manager, call, connect = collection_api
    db = connect()
    with db.cursor() as cur:
        cur.execute(f"DROP TABLE {manager.collection_feed_state_table()}")
        cur.execute(f"ALTER SEQUENCE {manager.collection_changes_table()}_seq_seq CACHE 32")
        # Rebuild the 1.2.5 schema: seq was a BIGSERIAL with a nextval default
        # (1.3.0 migration drops it, AUD-05).
        cur.execute(f"ALTER TABLE {manager.collection_changes_table()} ALTER COLUMN seq "
                    f"SET DEFAULT nextval('{manager.collection_changes_table()}_seq_seq')")
        cur.execute(f"INSERT INTO {manager.collection_changes_table()} "
                    "(principal, collection_id, entity_kind, entity_id, operation, payload) "
                    "VALUES ('user:alice', 'legacy', 'collection', 'legacy', 'upsert', '{}'::jsonb) "
                    "RETURNING seq")
        legacy_seq = cur.fetchone()[0]
    db.commit()
    manager.migrate_collections(db)
    with db.cursor() as cur:
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone()[0] == "4s"
        cur.execute("SHOW statement_timeout")
        assert cur.fetchone()[0] == "8s"
    db.commit()
    with db.cursor() as cur:
        cur.execute(f"SELECT epoch::text, head_seq FROM {manager.collection_feed_state_table()}")
        state = cur.fetchone()
    db.rollback()
    assert state[1] == legacy_seq
    manager.migrate_collections(db)
    db.commit()
    with db.cursor() as cur:
        cur.execute("SELECT column_default FROM information_schema.columns "
                    "WHERE table_schema=current_schema() AND table_name=%s "
                    "AND column_name='seq'", (manager.collection_changes_table(),))
        assert cur.fetchone() == (None,)
    with db.cursor() as cur:
        cur.execute(f"SELECT epoch::text, head_seq FROM {manager.collection_feed_state_table()}")
        assert cur.fetchone() == state
    db.rollback()
    db.close()
    assert call("POST", "/api/collections", {"id": "new", "name": "New"}).status_code == 201
    assert [r["seq"] for r in _page(call)["changes"]] == [legacy_seq, legacy_seq + 1]


def test_reader_page_excludes_commit_after_captured_head(collection_api, monkeypatch):
    manager, call, _ = collection_api
    assert call("POST", "/api/collections", {"id": "before", "name": "Before"}).status_code == 201
    head_captured = threading.Event()
    release = threading.Event()
    original_get_db = manager.get_db
    result = {}

    class ReaderCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def execute(self, sql, params=None):
            if f"FROM {manager.collection_changes_table()}" in sql and "seq <= %s" in sql:
                head_captured.set()
                assert release.wait(5)
            return self.cursor.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self.cursor, name)

    class ReaderDb:
        def __init__(self, db):
            self.db = db

        def cursor(self):
            return ReaderCursor(self.db.cursor())

        def __getattr__(self, name):
            return getattr(self.db, name)

    def get_db():
        db = original_get_db()
        return ReaderDb(db) if threading.current_thread().name == "reader" else db

    monkeypatch.setattr(manager, "get_db", get_db)
    reader = threading.Thread(target=lambda: result.update(page=_page(call)), name="reader")
    reader.start()
    assert head_captured.wait(5)
    try:
        assert call("POST", "/api/collections", {"id": "after", "name": "After"}).status_code == 201
    finally:
        release.set()
        reader.join(7)
    assert not reader.is_alive()
    first_page = result["page"]
    assert [row["collection_id"] for row in first_page["changes"]] == ["before"]
    next_page = _page(call, first_page["next_cursor"])
    assert [row["collection_id"] for row in next_page["changes"]] == ["after"]
