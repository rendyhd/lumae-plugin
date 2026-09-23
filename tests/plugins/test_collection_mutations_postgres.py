"""Disposable PostgreSQL regressions for LUM-002/003 collection mutations."""
import importlib
import json
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from flask import Flask, g

from test_lumae_analysis import load_plugin


DSN = os.environ.get("LUMAE_POSTGRES_TEST_DSN")


@pytest.fixture
def collection_api(monkeypatch):
    if not DSN:
        pytest.skip("set LUMAE_POSTGRES_TEST_DSN")
    psycopg2 = pytest.importorskip("psycopg2")
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    schema = "lum_mut_" + uuid.uuid4().hex
    admin = psycopg2.connect(DSN, connect_timeout=3)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")

    def connect():
        db = psycopg2.connect(DSN, connect_timeout=3)
        db.autocommit = False
        with db.cursor() as cur:
            cur.execute(f"SET search_path TO {schema}, public")
            cur.execute("SET lock_timeout TO '4s'")
            cur.execute("SET statement_timeout TO '8s'")
        db.commit()
        return db

    setup = connect()
    manager.migrate_collections(setup)
    setup.commit()
    setup.close()
    local = threading.local()
    monkeypatch.setattr(manager, "get_db", lambda: local.db)
    monkeypatch.setattr(manager, "get_setting", lambda key, default=None: True)
    app = Flask(__name__)
    app.testing = True

    @app.before_request
    def authenticate():
        local.db = connect()
        user = __import__("flask").request.headers.get("X-Test-User")
        g.auth_method = "session" if user else "bearer"
        g.auth_user = user

    @app.teardown_request
    def close_db(_exc):
        db = getattr(local, "db", None)
        if db:
            db.rollback()
            db.close()
            local.db = None

    app.register_blueprint(load_plugin().bp)

    def call(method, path, body=None, key=None, user="alice", if_match=None):
        headers = {}
        if user is not None:
            headers["X-Test-User"] = user
        if key:
            headers["Idempotency-Key"] = key
        if if_match is not None:
            headers["If-Match"] = if_match
        with app.test_client() as client:
            return client.open(path, method=method, json=body, headers=headers)

    yield manager, call, connect
    with admin.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    admin.close()


def _counts(db, manager, principal="user:alice"):
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {manager.collection_changes_table()} WHERE principal = %s", (principal,))
        changes = cur.fetchone()[0]
        cur.execute(f"SELECT count(*) FROM {manager.collection_mutations_table()} WHERE principal = %s", (principal,))
        receipts = cur.fetchone()[0]
    db.rollback()
    return changes, receipts


def test_fingerprint_canonicalization_and_bound_replay(collection_api):
    manager, call, connect = collection_api
    path = "/api/collections"
    first = call("POST", path, {"id": "c1", "name": "One", "description": "x"}, key="k")
    assert first.status_code == 201
    same = call("POST", path, {"description": "x", "name": "One", "id": "c1"}, key="k")
    assert same.status_code == 201
    assert same.get_json() == first.get_json()
    assert same.headers["Idempotency-Replayed"] == "true"
    for method, url, body, match in [
        ("POST", path, {"id": "c1", "name": "Other", "description": "x"}, None),
        ("POST", "/api/collections/restore", {"id": "c1", "name": "One"}, None),
        ("POST", path, {"id": "c1", "name": "One", "description": "x"}, "1"),
    ]:
        conflict = call(method, url, body, key="k", if_match=match)
        # Invalid restore input is rejected before admission.
        if url.endswith("/restore"):
            assert conflict.status_code == 400
        else:
            assert conflict.status_code == 409
            assert conflict.get_json()["error"] == "idempotency_key_conflict"
    db = connect()
    assert _counts(db, manager) == (1, 1)
    db.close()


def test_revision_conflict_key_retry_and_foreign_item_rollback(collection_api):
    manager, call, connect = collection_api
    for cid in ("a", "b"):
        assert call("POST", "/api/collections", {"id": cid, "name": cid}).status_code == 201
    path = "/api/collections/a"
    assert call("PATCH", path, {"name": "A2", "base_revision": 1}).status_code == 200
    stale = call("PATCH", path, {"name": "A3", "base_revision": 1}, key="retry")
    assert stale.status_code == 409
    good = call("PATCH", path, {"name": "A3", "base_revision": 2}, key="retry")
    assert good.status_code == 200
    assert call("PATCH", path, {"name": "A3", "base_revision": 2}, key="retry").headers["Idempotency-Replayed"] == "true"
    item = {"kind": "track", "track_id": "track-1"}
    assert call("PUT", "/api/collections/a/items/owned", item).status_code == 200
    before = call("GET", "/api/collections/a").get_json()
    bad = call("PUT", "/api/collections/b/items/owned", item, key="foreign")
    assert bad.status_code == 409
    assert bad.get_json()["error"] == "item_id_collection_conflict"
    batch = call("POST", "/api/collections/b/items/batch",
                 {"items": [{"id": "valid", "kind": "track", "track_id": "track-2"},
                            {"id": "owned", "kind": "track", "track_id": "track-3"}]},
                 key="foreign-batch")
    assert batch.status_code == 409
    assert call("GET", "/api/collections/a").get_json() == before
    assert call("GET", "/api/collections/b").get_json()["items"] == []
    db = connect()
    assert _counts(db, manager) == (5, 1)
    db.close()


def _observe_blocked_backend(connect, pid):
    # Inspect the database lock graph rather than inferring contention from timing.
    db = connect()
    try:
        for _ in range(80):
            with db.cursor() as cur:
                cur.execute("SELECT pg_blocking_pids(%s)", (pid,))
                blockers = cur.fetchone()[0]
            db.rollback()
            if blockers:
                return blockers
            threading.Event().wait(0.025)
        pytest.fail("request did not wait on a PostgreSQL lock")
    finally:
        db.close()


def test_locked_revision_waiter_sees_winner(collection_api, monkeypatch):
    manager, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "cas", "name": "One"}).status_code == 201
    locked = threading.Event()
    release = threading.Event()
    original = manager._lock_collection

    def pause_after_lock(cur, principal, cid, include_deleted=False):
        row = original(cur, principal, cid, include_deleted)
        if threading.current_thread().name == "winner" and cid == "cas":
            locked.set()
            assert release.wait(5)
        return row

    monkeypatch.setattr(manager, "_lock_collection", pause_after_lock)
    result = {}
    waiter_seen = threading.Event()
    waiter_pid = {}
    original_get_db = manager.get_db

    def record_waiter():
        db = original_get_db()
        if threading.current_thread().name == "loser":
            waiter_pid["pid"] = db.get_backend_pid()
            waiter_seen.set()
        return db

    monkeypatch.setattr(manager, "get_db", record_waiter)

    def winner():
        result["a"] = call("PATCH", "/api/collections/cas", {"name": "Winner", "base_revision": 1}, key="a")

    def loser():
        result["b"] = call("PATCH", "/api/collections/cas", {"name": "Loser", "base_revision": 1}, key="b")

    a = threading.Thread(target=winner, name="winner")
    b = threading.Thread(target=loser, name="loser")
    a.start()
    assert locked.wait(5)
    b.start()
    try:
        assert waiter_seen.wait(5)
        assert _observe_blocked_backend(connect, waiter_pid["pid"])
        assert b.is_alive()
    finally:
        release.set()
        a.join(7)
        b.join(7)
    assert not a.is_alive() and not b.is_alive()
    assert result["a"].status_code == 200
    assert result["b"].status_code == 409
    assert result["b"].get_json()["current"]["revision"] == 2
    db = connect()
    assert _counts(db, manager) == (2, 1)
    db.close()


def test_legacy_receipt_migration_is_additive(collection_api):
    manager, call, connect = collection_api
    db = connect()
    # Recreate only this disposable test table in its pre-fingerprint form.
    with db.cursor() as cur:
        cur.execute(f"DROP TABLE {manager.collection_mutations_table()}")
        cur.execute(
            f"CREATE TABLE {manager.collection_mutations_table()} ("
            "principal TEXT NOT NULL, idempotency_key TEXT NOT NULL, "
            "response_payload JSONB NOT NULL, status_code INTEGER NOT NULL, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
            "PRIMARY KEY (principal, idempotency_key))"
        )
        cur.execute(
            f"INSERT INTO {manager.collection_mutations_table()} "
            "(principal, idempotency_key, response_payload, status_code) "
            "VALUES (%s, %s, %s::jsonb, 201)",
            ("user:alice", "old", json.dumps({"collection": {"id": "historical"}})),
        )
    db.commit()
    manager.migrate_collections(db)
    db.commit()
    manager.migrate_collections(db)
    db.commit()
    with db.cursor() as cur:
        cur.execute(
            f"SELECT response_payload::text, request_fingerprint, fingerprint_version "
            f"FROM {manager.collection_mutations_table()} WHERE idempotency_key = 'old'"
        )
        payload, digest, version = cur.fetchone()
        assert json.loads(payload)["collection"]["id"] == "historical"
        assert digest is None and version is None
        with pytest.raises(Exception):
            cur.execute(
                f"INSERT INTO {manager.collection_mutations_table()} "
                "(principal, idempotency_key, response_payload, status_code, request_fingerprint) "
                "VALUES ('user:alice', 'partial', '{}'::jsonb, 200, 'abc')"
            )
    db.rollback()
    db.close()
    old = call("POST", "/api/collections", {"id": "new", "name": "New"}, key="old")
    assert old.status_code == 201
    assert old.get_json()["collection"]["id"] == "historical"
    assert old.headers["Idempotency-Fingerprint"] == "legacy-unbound"
    assert call("GET", "/api/collections/new").status_code == 404
    assert call("POST", "/api/collections", {"id": "new", "name": "New"}, key="fresh").status_code == 201


def test_restore_fault_rolls_back_and_lost_response_replays_ids(collection_api, monkeypatch):
    manager, call, connect = collection_api
    backup = manager._backup_envelope(
        [{"name": "Restored", "items": [{"kind": "track", "track_id": "restore-track"}]}],
        "personal",
    )
    original = manager._record_change
    fired = []

    def fail_after_write(cur, *args):
        original(cur, *args)
        if not fired:
            fired.append(True)
            raise RuntimeError("injected restore fault")

    monkeypatch.setattr(manager, "_record_change", fail_after_write)
    with pytest.raises(RuntimeError, match="injected"):
        call("POST", "/api/collections/restore", backup, key="restore")
    db = connect()
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {manager.collections_table()}")
        assert cur.fetchone()[0] == 0
        cur.execute(f"SELECT count(*) FROM {manager.collection_items_table()}")
        assert cur.fetchone()[0] == 0
    assert _counts(db, manager) == (0, 0)
    db.close()
    monkeypatch.setattr(manager, "_record_change", original)
    first = call("POST", "/api/collections/restore", backup, key="restore")
    assert first.status_code == 201
    replay = call("POST", "/api/collections/restore", backup, key="restore")
    assert replay.status_code == 201
    assert replay.get_json() == first.get_json()
    assert replay.headers["Idempotency-Replayed"] == "true"
    db = connect()
    assert _counts(db, manager) == (2, 1)
    db.close()


def test_receipt_scope_personal_and_intentionally_shared_bearer(collection_api):
    manager, call, connect = collection_api
    body = {"id": "shared-id", "name": "Same"}
    alice = call("POST", "/api/collections", body, key="same", user="alice")
    bob = call("POST", "/api/collections", body, key="same", user="bob")
    assert alice.status_code == bob.status_code == 201
    assert "Idempotency-Replayed" not in bob.headers
    assert call("GET", "/api/collections/shared-id", user="alice").status_code == 200
    assert call("GET", "/api/collections/shared-id", user="bob").status_code == 200
    bearer_a = call("POST", "/api/collections", body, key="global", user=None)
    bearer_b = call("POST", "/api/collections", body, key="global", user=None)
    assert bearer_a.status_code == bearer_b.status_code == 201
    assert bearer_b.headers["Idempotency-Replayed"] == "true"
    assert bearer_b.get_json() == bearer_a.get_json()
    db = connect()
    assert _counts(db, manager, "__global__") == (1, 1)
    db.close()


def test_same_key_concurrent_request_replays_after_first_commit(collection_api, monkeypatch):
    manager, call, connect = collection_api
    entered = threading.Event()
    release = threading.Event()
    original = manager._clean_collection_body

    def pause_first(body, partial=False):
        if threading.current_thread().name == "first":
            entered.set()
            assert release.wait(5)
        return original(body, partial)

    monkeypatch.setattr(manager, "_clean_collection_body", pause_first)
    result = {}
    waiter_seen = threading.Event()
    waiter_pid = {}
    original_get_db = manager.get_db

    def record_waiter():
        db = original_get_db()
        if threading.current_thread().name == "second":
            waiter_pid["pid"] = db.get_backend_pid()
            waiter_seen.set()
        return db

    monkeypatch.setattr(manager, "get_db", record_waiter)
    body = {"name": "One"}
    first = threading.Thread(
        target=lambda: result.update(first=call("POST", "/api/collections", body, key="shared")),
        name="first",
    )
    second = threading.Thread(
        target=lambda: result.update(second=call("POST", "/api/collections", body, key="shared")),
        name="second",
    )
    first.start()
    assert entered.wait(5)
    second.start()
    try:
        assert waiter_seen.wait(5)
        assert _observe_blocked_backend(connect, waiter_pid["pid"])
        assert second.is_alive()
    finally:
        release.set()
        first.join(7)
        second.join(7)
    assert not first.is_alive() and not second.is_alive()
    assert result["first"].status_code == result["second"].status_code == 201
    assert result["second"].get_json() == result["first"].get_json()
    assert result["second"].headers["Idempotency-Replayed"] == "true"
    db = connect()
    assert _counts(db, manager) == (1, 1)
    db.close()


@pytest.mark.parametrize("route", [
    "create", "patch", "delete", "item_put", "item_delete",
    "batch_upsert", "batch_delete", "restore",
])
def test_each_mutation_route_replays_and_rejects_changed_fingerprint(collection_api, route):
    manager, call, connect = collection_api
    if route != "create" and route != "restore":
        assert call("POST", "/api/collections", {"id": "parent", "name": "Parent"}).status_code == 201
    if route in {"item_delete", "batch_delete"}:
        assert call("PUT", "/api/collections/parent/items/old",
                    {"kind": "track", "track_id": "old"}).status_code == 200
    cases = {
        "create": ("POST", "/api/collections", {"id": "new", "name": "New"}),
        "patch": ("PATCH", "/api/collections/parent", {"name": "Changed", "base_revision": 1}),
        "delete": ("DELETE", "/api/collections/parent", {"base_revision": 1}),
        "item_put": ("PUT", "/api/collections/parent/items/new",
                     {"kind": "track", "track_id": "new", "base_revision": 1}),
        "item_delete": ("DELETE", "/api/collections/parent/items/old", {"base_revision": 2}),
        "batch_upsert": ("POST", "/api/collections/parent/items/batch",
                         {"items": [{"id": "new", "kind": "track", "track_id": "new"}],
                          "base_revision": 1}),
        "batch_delete": ("DELETE", "/api/collections/parent/items/batch",
                         {"item_ids": ["old"], "base_revision": 2}),
        "restore": ("POST", "/api/collections/restore",
                    manager._backup_envelope([{"name": "Restored", "items": []}], "personal")),
    }
    method, path, body = cases[route]
    first = call(method, path, body, key="route")
    assert 200 <= first.status_code < 300
    db = connect()
    before = _counts(db, manager)
    db.close()
    replay = call(method, path, body, key="route")
    assert replay.status_code == first.status_code
    assert replay.get_json() == first.get_json()
    assert replay.headers["Idempotency-Replayed"] == "true"
    changed = call(method, path, body, key="route", if_match="999")
    assert changed.status_code == 409
    assert changed.get_json()["error"] == "idempotency_key_conflict"
    db = connect()
    assert _counts(db, manager) == before
    db.close()


def test_fingerprint_golden_vector_and_array_order(collection_api):
    manager, _, _ = collection_api
    app = Flask(__name__)
    with app.test_request_context(
        "/api/collections?ignored=1", method="POST",
        json={"b": [2, 1], "a": 1}, headers={"If-Match": " 7 "},
    ):
        assert manager._request_fingerprint() == (
            "85453a25003a23435feef5a9328f262d1bba9dba537072d1afc490ca849e758e"
        )
    with app.test_request_context(
        "/api/collections", method="POST",
        json={"a": 1, "b": [1, 2]}, headers={"If-Match": "7"},
    ):
        assert manager._request_fingerprint() != (
            "85453a25003a23435feef5a9328f262d1bba9dba537072d1afc490ca849e758e"
        )


def test_connection_reuse_after_failure_replay_and_conflict(collection_api, monkeypatch):
    manager, _, connect = collection_api
    from psycopg2 import extensions

    db = connect()
    monkeypatch.setattr(manager, "get_db", lambda: db)
    app = Flask(__name__)

    def invoke(body, handler):
        with app.test_request_context(
            "/api/collections", method="POST", json=body,
            headers={"Idempotency-Key": "reuse"},
        ):
            g.auth_method = "session"
            g.auth_user = "alice"
            result = manager._mutation_response(handler)
            assert db.get_transaction_status() == extensions.TRANSACTION_STATUS_IDLE
            return result

    failed = invoke({"name": "bad"}, lambda cur, principal: ({"error": "validation"}, 400))
    assert failed[1] == 400
    applied = invoke({"name": "good"}, lambda cur, principal: ({"ok": True}, 201))
    assert applied[1] == 201
    replay = invoke({"name": "good"}, lambda cur, principal: pytest.fail("replay ran handler"))
    assert replay[1] == 201 and replay[2]["Idempotency-Replayed"] == "true"
    conflict = invoke({"name": "changed"}, lambda cur, principal: pytest.fail("conflict ran handler"))
    assert conflict[1] == 409
    db.close()


def test_prior_settings_select_on_request_connection(collection_api, monkeypatch):
    manager, call, connect = collection_api

    def settings_read(_key, default=None):
        db = manager.get_db()
        with db.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
        return True

    monkeypatch.setattr(manager, "get_setting", settings_read)
    first = call("POST", "/api/collections", {"id": "after-settings", "name": "Safe"}, key="settings")
    assert first.status_code == 201
    replay = call("POST", "/api/collections", {"id": "after-settings", "name": "Safe"}, key="settings")
    assert replay.status_code == 201
    assert replay.headers["Idempotency-Replayed"] == "true"
    db = connect()
    assert _counts(db, manager) == (1, 1)
    db.close()


def test_stricter_isolation_fails_before_mutation(collection_api, monkeypatch):
    manager, call, connect = collection_api

    def strict_settings_read(_key, default=None):
        db = manager.get_db()
        with db.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SELECT 1")
        return True

    monkeypatch.setattr(manager, "get_setting", strict_settings_read)
    blocked = call("POST", "/api/collections", {"id": "no-write", "name": "No"}, key="strict")
    assert blocked.status_code == 503
    assert blocked.get_json()["error"] == "unsupported_transaction_isolation"
    db = connect()
    assert _counts(db, manager) == (0, 0)
    db.close()


def test_failure_after_receipt_insert_rolls_back_state_event_and_receipt(collection_api, monkeypatch):
    manager, call, connect = collection_api
    original_get_db = manager.get_db

    class FailingCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def execute(self, sql, params=None):
            self.cursor.execute(sql, params)
            if "INSERT INTO" in sql and manager.collection_mutations_table() in sql:
                raise RuntimeError("after receipt insertion")

        def __getattr__(self, name):
            return getattr(self.cursor, name)

    class FailingDb:
        def __init__(self, db):
            self.db = db

        def cursor(self):
            return FailingCursor(self.db.cursor())

        def __getattr__(self, name):
            return getattr(self.db, name)

    monkeypatch.setattr(manager, "get_db", lambda: FailingDb(original_get_db()))
    with pytest.raises(RuntimeError, match="after receipt"):
        call("POST", "/api/collections", {"id": "atomic", "name": "Atomic"}, key="atomic")
    db = connect()
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {manager.collections_table()}")
        assert cur.fetchone()[0] == 0
    assert _counts(db, manager) == (0, 0)
    db.close()
    monkeypatch.setattr(manager, "get_db", original_get_db)
    assert call("POST", "/api/collections", {"id": "atomic", "name": "Atomic"}, key="atomic").status_code == 201


@pytest.mark.parametrize("route", [
    "patch", "delete", "item_put", "item_delete", "batch_upsert", "batch_delete",
])
def test_concurrent_cas_route_matrix(collection_api, monkeypatch, route):
    manager, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "matrix", "name": "Start"}).status_code == 201
    base = 1
    if route in {"item_delete", "batch_delete"}:
        assert call("PUT", "/api/collections/matrix/items/old",
                    {"kind": "track", "track_id": "old"}).status_code == 200
        base = 2
    cases = {
        "patch": ("PATCH", "/api/collections/matrix", {"name": "Winner", "base_revision": base}),
        "delete": ("DELETE", "/api/collections/matrix", {"base_revision": base}),
        "item_put": ("PUT", "/api/collections/matrix/items/new",
                     {"kind": "track", "track_id": "new", "base_revision": base}),
        "item_delete": ("DELETE", "/api/collections/matrix/items/old", {"base_revision": base}),
        "batch_upsert": ("POST", "/api/collections/matrix/items/batch",
                         {"items": [{"id": "new", "kind": "track", "track_id": "new"}],
                          "base_revision": base}),
        "batch_delete": ("DELETE", "/api/collections/matrix/items/batch",
                         {"item_ids": ["old"], "base_revision": base}),
    }
    method, path, body = cases[route]
    db = connect()
    baseline_changes, _ = _counts(db, manager)
    db.close()
    locked = threading.Event()
    release = threading.Event()
    waiter_seen = threading.Event()
    waiter_pid = {}
    result = {}
    original_lock = manager._lock_collection
    original_get_db = manager.get_db

    def lock_and_pause(cur, principal, cid, include_deleted=False):
        row = original_lock(cur, principal, cid, include_deleted)
        if threading.current_thread().name == "winner":
            locked.set()
            assert release.wait(5)
        return row

    def capture_waiter_db():
        db = original_get_db()
        if threading.current_thread().name == "loser":
            waiter_pid["pid"] = db.get_backend_pid()
            waiter_seen.set()
        return db

    monkeypatch.setattr(manager, "_lock_collection", lock_and_pause)
    monkeypatch.setattr(manager, "get_db", capture_waiter_db)
    a = threading.Thread(target=lambda: result.update(a=call(method, path, body, key="winner")), name="winner")
    b = threading.Thread(target=lambda: result.update(b=call(method, path, body, key="loser")), name="loser")
    a.start()
    assert locked.wait(5)
    b.start()
    try:
        assert waiter_seen.wait(5)
        assert _observe_blocked_backend(connect, waiter_pid["pid"])
    finally:
        release.set()
        a.join(7)
        b.join(7)
    assert not a.is_alive() and not b.is_alive()
    assert 200 <= result["a"].status_code < 300
    assert result["b"].status_code == 409
    assert result["b"].get_json()["error"] == "revision_conflict"
    db = connect()
    with db.cursor() as cur:
        cur.execute(
            f"SELECT revision FROM {manager.collections_table()} "
            "WHERE principal = 'user:alice' AND id = 'matrix'"
        )
        assert cur.fetchone()[0] == base + 1
    changes, receipts = _counts(db, manager)
    assert (changes, receipts) == (baseline_changes + 1, 1)
    db.close()
