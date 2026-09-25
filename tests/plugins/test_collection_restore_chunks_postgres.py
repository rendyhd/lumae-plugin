"""Bounded collection writes (P3-4a, LUM-004): block allocation of feed seqs,
bounded lock waits, and chunked, resumable restores."""

import random
import threading
import time

import pytest
from flask import Flask, g

from test_collection_feed_epoch_postgres import _feed, collections_api  # noqa: F401 (fixture)


def _backup(manager, collections):
    return manager._backup_envelope(
        [{"name": name, "items": [{"kind": "track", "track_id": f"{name}-{i}", "title": f"T{i}"}
                                   for i in range(count)]}
         for name, count in collections],
        "personal",
    )


def _journal(api):
    """(every seq in order, head_seq, integrity) straight from the database."""
    manager = api.manager
    db = api.connect()
    try:
        with db.cursor() as cur:
            cur.execute(f"SELECT seq FROM {manager.collection_changes_table()} ORDER BY seq")
            seqs = [row[0] for row in cur.fetchall()]
            cur.execute(f"SELECT head_seq FROM {manager.collection_feed_state_table()}")
            head = cur.fetchone()[0]
            integrity = manager.collection_feed_integrity(cur)
        return seqs, head, integrity
    finally:
        db.close()


def _count_head_updates(api, monkeypatch):
    """Count the feed-head UPDATEs each request issues."""
    manager = api.manager
    counts = []
    original_get_db = manager.get_db

    class Cursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def execute(self, sql, params=None):
            if sql.lstrip().startswith("UPDATE") and manager.collection_feed_state_table() in sql:
                counts.append(sql)
            return self.cursor.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self.cursor, name)

    class Db:
        def __init__(self, db):
            self.db = db

        def cursor(self):
            return Cursor(self.db.cursor())

        def __getattr__(self, name):
            return getattr(self.db, name)

    monkeypatch.setattr(manager, "get_db", lambda: Db(original_get_db()))
    return counts


def test_each_mutation_allocates_one_contiguous_block_ending_at_head(collections_api, monkeypatch):
    api = collections_api
    manager = api.manager
    head_updates = _count_head_updates(api, monkeypatch)
    expected = 0

    def check(response, events, status=200):
        nonlocal expected
        assert response.status_code == status, response.get_json()
        expected += events
        assert len(head_updates) == 1
        head_updates.clear()
        seqs, head, integrity = _journal(api)
        # Gapless from 1 to the head: no seq past the head, none reused.
        assert seqs == list(range(1, expected + 1))
        assert (head, integrity) == (expected, True)

    check(api.call("POST", "/api/collections", {"id": "a", "name": "A"}), 1, 201)
    check(api.call("POST", "/api/collections/a/items/batch", {"items": [
        {"id": f"i{n}", "kind": "track", "track_id": f"t{n}"} for n in range(5)]}), 5)
    check(api.call("POST", "/api/collections", {"id": "b", "name": "B"}, user="bob"), 1, 201)
    check(api.call("DELETE", "/api/collections/a/items/batch",
                   {"item_ids": ["i0", "i1", "i4", "missing"]}), 3)
    check(api.call("PUT", "/api/collections/a/items/i9", {"kind": "track", "track_id": "t9"}), 1)
    check(api.call("POST", "/api/collections/restore",
                   _backup(manager, [("R", 3), ("S", 0)]), key="r"), 5, 201)
    # The feed serves every event, in order, up to the head.
    feed = _feed(api, 0, limit=500).get_json()
    assert [c["seq"] for c in feed["changes"]] == [1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    assert sorted(c["entity_id"] for c in feed["changes"][6:9]) == ["i0", "i1", "i4"]
    assert feed["head_seq"] == 16 and not feed["has_more"]


def test_mutation_sets_a_transaction_scoped_lock_timeout(collections_api, monkeypatch):
    api = collections_api
    manager = api.manager
    db = api.connect()
    seen = []

    def lock_timeout():
        with db.cursor() as cur:
            cur.execute("SHOW lock_timeout")
            return cur.fetchone()[0]

    def handler(cur, principal):
        seen.append(lock_timeout())
        return {"ok": True}, 200

    before = lock_timeout()
    db.commit()
    assert before != "3s"
    app = Flask(__name__)
    monkeypatch.setattr(manager, "get_db", lambda: db)
    with app.test_request_context("/api/collections", method="POST", json={}):
        g.auth_method = "bearer"
        response = manager._mutation_response(handler)
    # SET LOCAL: the host connection's own setting is back after the commit.
    assert response[1] == 200 and seen == ["3s"]
    assert lock_timeout() == before
    db.close()


@pytest.mark.parametrize("held", ["collection_row", "feed_head"])
def test_lock_wait_is_bounded_and_answers_503_with_retry_after(
        collections_api, second_connection, held):
    api = collections_api
    manager = api.manager
    assert api.call("POST", "/api/collections", {"id": "a", "name": "A"}).status_code == 201
    _, head, _ = _journal(api)
    with second_connection.cursor() as cur:
        if held == "collection_row":
            cur.execute(f"SELECT 1 FROM {manager.collections_table()} "
                        "WHERE principal = 'user:alice' AND id = 'a' FOR UPDATE")
        else:
            cur.execute(f"SELECT 1 FROM {manager.collection_feed_state_table()} FOR UPDATE")
    result = {}

    def patch():
        started = time.monotonic()
        result["response"] = api.call("PATCH", "/api/collections/a", {"name": "B"}, key="k")
        result["elapsed"] = time.monotonic() - started

    writer = threading.Thread(target=patch)
    writer.start()
    writer.join(10)
    # Without the bound, the writer would still wait here; release it either way.
    second_connection.rollback()
    writer.join(10)
    assert not writer.is_alive()
    response = result["response"]
    assert response.status_code == 503, response.get_json()
    assert response.get_json() == {"error": "collection_busy"}
    assert response.headers["Retry-After"] == "5"
    assert 2.5 <= result["elapsed"] < 8
    # Nothing was written and no receipt holds the key: the retry applies.
    assert _journal(api)[1] == head
    retry = api.call("PATCH", "/api/collections/a", {"name": "B"}, key="k")
    assert retry.status_code == 200 and "Idempotency-Replayed" not in retry.headers
    assert retry.get_json()["collection"]["name"] == "B"


def _principal_state(api, user):
    """Content of a principal's collections by name, without ids, times or revisions."""
    snapshot = api.call("GET", "/api/collections/snapshot", user=user).get_json()
    names = {c["id"]: c["name"] for c in snapshot["collections"]}
    content = {}
    for collection in snapshot["collections"]:
        content[collection["name"]] = {
            "description": collection["description"],
            "counts": (collection["album_count"], collection["track_count"]),
            "items": [
                (i["kind"], i["track_id"], i["title"], i["position"])
                for i in snapshot["items"] if names[i["collection_id"]] == collection["name"]
            ],
        }
    revisions = {c["name"]: c["revision"] for c in snapshot["collections"]}
    return content, revisions


def _event_shape(api, user):
    """The principal's feed with ids canonicalised and revisions/times dropped."""
    changes = []
    cursor = 0
    while True:
        page = _feed(api, cursor, limit=500, user=user).get_json()
        changes += page["changes"]
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    ids = {}
    volatile = {"revision", "collection_revision", "collection_updated_at", "created_at",
                "updated_at", "album_count", "track_count"}
    shape = []
    for change in changes:
        payload = {k: v for k, v in change["payload"].items() if k not in volatile}
        for name in ("id", "collection_id"):
            if name in payload:
                payload[name] = ids.setdefault(payload[name], len(ids))
        shape.append((change["entity_kind"], change["operation"],
                       ids.setdefault(change["entity_id"], len(ids)),
                       ids.setdefault(change["collection_id"], len(ids)), payload))
    return shape


def _restores(api):
    db = api.connect()
    try:
        with db.cursor() as cur:
            cur.execute(f"SELECT principal, chunks_done, chunk_count "
                        f"FROM {api.manager.collection_restores_table()} ORDER BY principal")
            return cur.fetchall()
    finally:
        db.close()


def test_chunked_restore_equals_unchunked_and_resumes_idempotently(collections_api, monkeypatch):
    api = collections_api
    manager = api.manager
    backup = _backup(manager, [("Big", 5_000), ("Small", 3), ("Empty", 0)])
    chunks = []
    original = manager._restore_principal_collections
    fail_on = {}

    def counted(cur, principal, segments):
        chunks.append((principal, sum(len(s["items"]) + s["create"] for s in segments)))
        result = original(cur, principal, segments)
        if fail_on.get(principal) == len([c for c in chunks if c[0] == principal]):
            raise RuntimeError("injected restore fault")
        return result

    monkeypatch.setattr(manager, "_restore_principal_collections", counted)

    # Unchunked reference (one transaction), then the default 2,000-row chunks.
    monkeypatch.setattr(manager, "RESTORE_CHUNK_ROWS", 100_000)
    whole = api.call("POST", "/api/collections/restore", backup, key="r", user="carol")
    monkeypatch.setattr(manager, "RESTORE_CHUNK_ROWS", 2_000)
    chunked = api.call("POST", "/api/collections/restore", backup, key="r", user="alice")
    assert whole.status_code == chunked.status_code == 201
    assert [rows for _, rows in chunks] == [5_006, 2_000, 2_000, 1_006]
    for body in (whole.get_json(), chunked.get_json()):
        assert (body["restored"], body["collection_count"], body["item_count"]) == (True, 3, 5_003)
        assert [c["name"] for c in body["collections"]] == ["Big", "Small", "Empty"]
        assert [c["track_count"] for c in body["collections"]] == [5_000, 3, 0]
    content, revisions = _principal_state(api, "alice")
    assert (content, revisions) == (_principal_state(api, "carol")[0],
                                    {"Big": 4, "Small": 2, "Empty": 1})
    assert _principal_state(api, "carol")[1] == {"Big": 2, "Small": 2, "Empty": 1}
    assert _event_shape(api, "alice") == _event_shape(api, "carol")
    assert [c["revision"] for c in chunked.get_json()["collections"]] == [4, 2, 1]
    assert _restores(api) == []

    # Interrupted after chunk 1 committed: the retry with the same key resumes.
    chunks.clear()
    fail_on["user:dave"] = 2
    with pytest.raises(RuntimeError, match="injected"):
        api.call("POST", "/api/collections/restore", backup, key="d", user="dave")
    assert _restores(api) == [("user:dave", 1, 3)]
    partial = api.call("GET", "/api/collections/snapshot", user="dave").get_json()
    assert [(c["name"], c["track_count"]) for c in partial["collections"]] == [("Big", 1_999)]
    # The same key with another body is a conflict, and changes nothing.
    other = _backup(manager, [("Else", 1)])
    conflict = api.call("POST", "/api/collections/restore", other, key="d", user="dave")
    assert (conflict.status_code, conflict.get_json()) == (409, {"error": "idempotency_key_conflict"})
    fail_on.clear()
    resumed = api.call("POST", "/api/collections/restore", backup, key="d", user="dave")
    assert resumed.status_code == 201

    def summary(body):
        return ([(c["name"], c["revision"], c["album_count"], c["track_count"])
                 for c in body["collections"]], body["collection_count"], body["item_count"])

    assert summary(resumed.get_json()) == summary(chunked.get_json())
    # Chunk 2 was rolled back once, then chunks 2 and 3 applied once each.
    assert [rows for principal, rows in chunks if principal == "user:dave"] == [
        2_000, 2_000, 2_000, 1_006]
    assert _principal_state(api, "dave") == _principal_state(api, "alice")
    assert _event_shape(api, "dave") == _event_shape(api, "alice")
    assert _restores(api) == []
    replay = api.call("POST", "/api/collections/restore", backup, key="d", user="dave")
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.get_json() == resumed.get_json()
    seqs, head, integrity = _journal(api)
    assert (seqs, integrity) == (list(range(1, head + 1)), True)

    # Without a key, a failure after chunk 1 keeps chunk 1 (documented).
    fail_on["user:erin"] = 2
    with pytest.raises(RuntimeError, match="injected"):
        api.call("POST", "/api/collections/restore", backup, user="erin")
    erin = api.call("GET", "/api/collections/snapshot", user="erin").get_json()
    assert [(c["name"], c["track_count"]) for c in erin["collections"]] == [("Big", 1_999)]
    assert _restores(api) == []


def test_concurrent_retries_with_one_key_apply_each_chunk_once(collections_api, monkeypatch):
    api = collections_api
    manager = api.manager
    monkeypatch.setattr(manager, "RESTORE_CHUNK_ROWS", 40)
    backup = _backup(manager, [("A", 300), ("B", 50), ("C", 0)])
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(
            api.call("POST", "/api/collections/restore", backup, key="same")))
        for _ in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert len(results) == 3 and {r.status_code for r in results} == {201}
    assert len({r.get_data() for r in results}) == 1
    assert sum("Idempotency-Replayed" in r.headers for r in results) >= 1
    shape = _event_shape(api, "alice")
    assert len([s for s in shape if s[0] == "item"]) == 350
    assert len([s for s in shape if s[0] == "collection"]) == 3
    content, revisions = _principal_state(api, "alice")
    assert {name: len(c["items"]) for name, c in content.items()} == {"A": 300, "B": 50, "C": 0}
    assert _restores(api) == []


def test_other_principal_write_waits_briefly_during_a_20k_restore(collections_api, monkeypatch):
    """docs/audit/.../collections/test_probe_restore_hold.py (and the frontier
    probe's restore case), inverted. LUM-004 budget: another principal's write
    waits <= 500 ms during a 20k-item restore (audit: 9.85 s; f36e087 on the
    test database: the feed head held 17.6 s, the write timed out at 4 s)."""
    api = collections_api
    manager = api.manager
    assert api.call("POST", "/api/collections", {"id": "bobc", "name": "b"}, user="bob").status_code == 201
    backup = _backup(manager, [("Big", 20_000)])
    reached = threading.Event()
    done = threading.Event()
    original = manager._record_changes

    def signal_frontier(cur, changes):
        # The restore is about to take the feed head for one chunk's events.
        if threading.current_thread().name == "restore":
            reached.set()
        return original(cur, changes)

    monkeypatch.setattr(manager, "_record_changes", signal_frontier)
    result = {}
    latencies, statuses = [], []

    def restore():
        try:
            result["restore"] = api.call("POST", "/api/collections/restore", backup, key="big")
        finally:
            done.set()

    def bob():
        n = 0
        while not done.is_set():
            if not reached.wait(0.05):
                continue
            reached.clear()
            started = time.perf_counter()
            response = api.call("PATCH", "/api/collections/bobc", {"name": f"b{n}"}, user="bob")
            latencies.append(time.perf_counter() - started)
            statuses.append(response.status_code)
            n += 1

    threads = [threading.Thread(target=restore, name="restore"), threading.Thread(target=bob)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(600)
    assert result["restore"].status_code == 201
    assert len(latencies) >= 5 and set(statuses) == {200}
    print(f"bob write latency while the 20k restore appends events: n={len(latencies)} "
          f"max={max(latencies) * 1000:.0f} ms median={sorted(latencies)[len(latencies) // 2] * 1000:.0f} ms")
    assert max(latencies) <= 0.5


def test_concurrent_block_writers_leave_no_gap_or_skip(collections_api, monkeypatch):
    """docs/audit/.../collections/test_probe_frontier.py (no skip, gapless),
    inverted for block allocation, chunked restores and K8 paging."""
    api = collections_api
    manager = api.manager
    monkeypatch.setattr(manager, "RESTORE_CHUNK_ROWS", 7)
    original = manager._record_changes

    def jitter(cur, changes):
        original(cur, changes)
        # Widen the window between allocation and commit.
        if random.random() < 0.4:
            time.sleep(random.random() * 0.004)

    monkeypatch.setattr(manager, "_record_changes", jitter)
    errors = []
    stop = threading.Event()

    def writer(user, cid, n):
        try:
            assert api.call("POST", "/api/collections", {"id": cid, "name": cid}, user=user).status_code == 201
            for j in range(n):
                if j % 5 == 0:
                    body = {"items": [{"kind": "track", "track_id": f"{cid}-b{j}-{k}"} for k in range(4)]}
                    response = api.call("POST", f"/api/collections/{cid}/items/batch", body, user=user)
                elif j % 5 == 1:
                    response = api.call("POST", "/api/collections/restore",
                                        _backup(manager, [(f"{cid}r{j}", 9)]), key=f"{cid}{j}", user=user)
                elif j % 5 == 2:
                    response = api.call("DELETE", f"/api/collections/{cid}/items/batch",
                                        {"item_ids": [f"{cid}-x{j - 4}", f"{cid}-x{j - 3}", "none"]},
                                        user=user)
                else:
                    response = api.call("PUT", f"/api/collections/{cid}/items/{cid}-x{j}",
                                        {"kind": "track", "track_id": f"{cid}-{j}"}, user=user)
                assert 200 <= response.status_code < 300, response.get_json()
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(repr(exc))

    seen = {"u0": [], "u1": []}
    epochs = set()

    def drain(user, cursor, epoch):
        while True:
            response = _feed(api, cursor, limit=7, epoch=epoch, user=user)
            assert response.status_code == 200, response.get_json()
            body = response.get_json()
            epochs.add(body["epoch"])
            seen[user].extend(c["seq"] for c in body["changes"])
            cursor = body["next_cursor"]
            if not body["has_more"]:
                return cursor, body["epoch"]

    def reader(user):
        try:
            cursor, epoch = drain(user, 0, None)
            while not stop.is_set():
                cursor, epoch = drain(user, cursor, epoch)
            drain(user, cursor, epoch)
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(repr(exc))

    users = [f"u{i}" for i in range(4)]
    writers = [threading.Thread(target=writer, args=(u, f"{u}c", 15)) for u in users]
    writers += [threading.Thread(target=writer, args=("u0", f"u0x{i}", 15)) for i in range(2)]
    readers = [threading.Thread(target=reader, args=(u,)) for u in seen]
    for thread in readers + writers:
        thread.start()
    for thread in writers:
        thread.join(120)
    stop.set()
    for thread in readers:
        thread.join(60)
    assert not errors, errors
    assert len(epochs) == 1
    db = api.connect()
    with db.cursor() as cur:
        for user, got in seen.items():
            cur.execute(f"SELECT seq FROM {manager.collection_changes_table()} "
                        "WHERE principal = %s ORDER BY seq", (f"user:{user}",))
            assert got == [row[0] for row in cur.fetchall()]
    db.close()
    seqs, head, integrity = _journal(api)
    assert (seqs, integrity) == (list(range(1, head + 1)), True)
