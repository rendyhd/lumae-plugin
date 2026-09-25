"""K8 collections feed epoch, snapshot and old-client compatibility (P3-4a).

Every test runs the real routes against the ``migrated_db`` schema: each
request gets its own connection to that schema, and the snapshot's owned
connection reaches it through ``config.DATABASE_URL``.
"""

import json
import pathlib
import re
import threading
import time
import types

import pytest
from flask import Flask, g, request

import pg_helpers
from test_lumae_analysis import load_plugin, plugin_api_module


GOLDEN = pathlib.Path(__file__).with_name("collection_feed_v1_golden.json")
UUID_RE = re.compile(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
TS_RE = re.compile(rb"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})")
TRANSCRIPT_HEADERS = ("Idempotency-Replayed", "Idempotency-Fingerprint", "Retry-After")


@pytest.fixture
def collections_api(migrated_db, monkeypatch):
    """(manager, call, connect): the collection routes on the migrated schema."""
    psycopg2 = pytest.importorskip("psycopg2")
    manager = load_plugin().collection_manager
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    migrated_db.commit()
    opened = []

    def connect():
        db = pg_helpers.connect(schema)
        opened.append(db)
        return db

    local = threading.local()
    monkeypatch.setattr(manager, "get_db", lambda: local.db)
    monkeypatch.setattr(manager, "get_setting", lambda key, default=None: True)
    monkeypatch.setattr(
        plugin_api_module.config, "DATABASE_URL",
        psycopg2.extensions.make_dsn(pg_helpers.dsn(), options=f"-c search_path={schema},public"),
        raising=False,
    )
    app = Flask(__name__)
    app.testing = True

    @app.before_request
    def authenticate():
        local.db = connect()
        user = request.headers.get("X-Test-User")
        g.auth_method = "session" if user else "bearer"
        g.auth_user = user

    @app.teardown_request
    def close_db(_exc):
        db = getattr(local, "db", None)
        if db is not None:
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

    yield types.SimpleNamespace(manager=manager, call=call, connect=connect, schema=schema)
    for db in opened:
        if not db.closed:
            try:
                db.rollback()
                db.close()
            except Exception:
                pass


def _transcript(call):
    """A fixed old-client session: mutations, then feed pages without epoch.

    Returns [(label, status, raw body bytes, {header: value})]. Every value is
    deterministic except timestamps and restore-generated ids, which
    ``_normalized`` replaces.
    """
    out = []

    def record(label, method, path, body=None, **options):
        response = call(method, path, body, **options)
        headers = {name: response.headers[name] for name in TRANSCRIPT_HEADERS
                   if name in response.headers}
        out.append((label, response.status_code, response.get_data(), headers))
        return response

    record("create c1", "POST", "/api/collections",
           {"id": "c1", "name": "One", "description": "First"}, key="k1")
    record("create c2", "POST", "/api/collections", {"id": "c2", "name": "Two"})
    record("put i1", "PUT", "/api/collections/c1/items/i1",
           {"kind": "track", "track_id": "t1", "title": "Song", "artist": "A", "position": 3})
    record("batch c1", "POST", "/api/collections/c1/items/batch", {
        "items": [
            {"id": "i2", "kind": "track", "track_id": "t2"},
            {"id": "i3", "kind": "album", "provider_album_id": "al1", "title": "Alb"},
            {"id": "i4", "kind": "album", "album_key": "x::y"},
        ],
        "base_revision": 2,
    })
    record("remapped duplicate", "PUT", "/api/collections/c1/items/i9",
           {"kind": "track", "track_id": "t1"})
    record("patch c2", "PATCH", "/api/collections/c2",
           {"name": "Two!", "description": "d"}, if_match="1")
    record("stale patch", "PATCH", "/api/collections/c2", {"name": "x", "base_revision": 1})
    record("batch delete", "DELETE", "/api/collections/c1/items/batch",
           {"item_ids": ["i2", "i3", "missing"]})
    record("item delete", "DELETE", "/api/collections/c1/items/i4", {})
    record("missing item delete", "DELETE", "/api/collections/c1/items/nope", {})
    record("bob create", "POST", "/api/collections", {"id": "b1", "name": "Bob"}, user="bob")
    record("delete c2", "DELETE", "/api/collections/c2", {})
    backup = {
        "format": "lumae-living-collections",
        "version": 1,
        "collections": [
            {"name": "Restored", "description": "r", "items": [
                {"kind": "track", "track_id": "rt1", "title": "R1"},
                {"kind": "album", "album_key": "r::a"},
                {"kind": "track", "track_id": "rt2"},
            ]},
            {"name": "Empty", "items": []},
        ],
    }
    backup["checksum"] = _checksum(backup["collections"])
    record("restore", "POST", "/api/collections/restore", backup, key="r1")
    record("restore replay", "POST", "/api/collections/restore", backup, key="r1")
    record("key conflict", "POST", "/api/collections", {"id": "c1", "name": "Other"}, key="k1")
    record("missing patch", "PATCH", "/api/collections/none", {"name": "x"})
    record("list", "GET", "/api/collections")
    record("detail c1", "GET", "/api/collections/c1")

    record("feed default", "GET", "/api/collections/changes")
    cursor = 0
    for page in range(20):
        response = record(f"feed page {page}", "GET",
                          f"/api/collections/changes?cursor={cursor}&limit=3")
        body = response.get_json()
        if not body["changes"]:
            break
        cursor = body["next_cursor"]
    record("feed limit 1", "GET", "/api/collections/changes?cursor=0&limit=1")
    record("feed limit 0", "GET", "/api/collections/changes?cursor=2&limit=0")
    record("feed limit 9999", "GET", "/api/collections/changes?cursor=0&limit=9999")
    record("feed negative", "GET", "/api/collections/changes?cursor=-5&limit=2")
    record("feed past head", "GET", "/api/collections/changes?cursor=1000")
    record("feed bad cursor", "GET", "/api/collections/changes?cursor=abc")
    record("feed bad limit", "GET", "/api/collections/changes?limit=x")
    record("feed empty epoch", "GET", "/api/collections/changes?cursor=1000&epoch=")
    record("feed bob", "GET", "/api/collections/changes", user="bob")
    record("feed bearer", "GET", "/api/collections/changes", user=None)
    return out


def _checksum(collections):
    import hashlib

    encoded = json.dumps(collections, ensure_ascii=False, separators=(",", ":"),
                         sort_keys=True).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _normalized(transcript):
    ids = {}

    def uuid_placeholder(match):
        return b"<uuid%d>" % ids.setdefault(match.group(0), len(ids) + 1)

    rows = []
    for label, status, body, headers in transcript:
        text = TS_RE.sub(b"<ts>", body)
        text = UUID_RE.sub(uuid_placeholder, text)
        rows.append({"request": label, "status": status,
                     "body": text.decode("utf-8"), "headers": headers})
    return rows


def _feed(api, cursor=0, limit=200, epoch=None, user="alice"):
    path = f"/api/collections/changes?cursor={cursor}&limit={limit}"
    if epoch is not None:
        path += f"&epoch={epoch}"
    return api.call("GET", path, user=user)


def _db_state(api):
    db = api.connect()
    try:
        with db.cursor() as cur:
            cur.execute(
                f"SELECT epoch::text, head_seq, floor_seq "
                f"FROM {api.manager.collection_feed_state_table()}"
            )
            return cur.fetchone()
    finally:
        db.close()


def _batch(api, collection_id, track_ids, user="alice"):
    response = api.call(
        "POST", f"/api/collections/{collection_id}/items/batch",
        {"items": [{"id": f"{collection_id}-{t}", "kind": "track", "track_id": t,
                    "position": position}
                   for position, t in enumerate(track_ids)]},
        user=user,
    )
    assert response.status_code == 200, response.get_json()
    return response


def test_old_client_transcript_is_byte_identical_to_the_pre_k8_golden(collections_api):
    """Without an epoch, every response is 1.2.5's, byte for byte, except the
    four additive feed keys.

    The golden was recorded from phase/3-semantics f36e087, before K8, by this
    module's ``_transcript``; timestamps and restore-generated ids are
    normalised. Each feed 200 must carry each new key exactly once, and
    removing those bytes must give the old body.
    """
    new_keys = (
        re.compile(rb'"epoch":"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",'),
        re.compile(rb'"floor_seq":\d+,'),
        re.compile(rb'"has_more":(?:true|false),'),
        re.compile(rb'"head_seq":\d+,'),
    )
    stripped = []
    for label, status, body, headers in _transcript(collections_api.call):
        if label.startswith("feed") and status == 200:
            for pattern in new_keys:
                body, found = pattern.subn(b"", body)
                assert found == 1, (label, pattern.pattern)
        stripped.append((label, status, body, headers))
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert _normalized(stripped) == golden


def test_feed_reports_epoch_head_floor_and_pages_by_has_more(collections_api):
    api = collections_api
    epoch, head, floor = _db_state(api)
    assert (head, floor) == (0, 0)
    assert api.call("POST", "/api/collections", {"id": "a", "name": "A"}).status_code == 201
    _batch(api, "a", ["t1", "t2", "t3"])                                     # 2, 3, 4
    assert api.call("POST", "/api/collections", {"id": "b", "name": "B"},
                    user="bob").status_code == 201                          # 5 (bob)
    _batch(api, "a", ["t4", "t5", "t6"])                                     # 6, 7, 8
    assert _db_state(api) == (epoch, 8, 0)

    def page(cursor, **options):
        response = _feed(api, cursor, **options)
        assert response.status_code == 200
        body = response.get_json()
        assert (body["epoch"], body["head_seq"], body["floor_seq"]) == (epoch, 8, 0)
        return [row["seq"] for row in body["changes"]], body["next_cursor"], body["has_more"]

    # Without and with the echo, paging is identical.
    assert page(0, limit=3) == ([1, 2, 3], 3, True)
    assert page(3, limit=3, epoch=epoch) == ([4, 6, 7], 7, True)
    assert page(7, limit=3, epoch=epoch) == ([8], 8, False)
    # Exactly `limit` events left: has_more is false, with no extra round trip.
    assert page(4, limit=3, epoch=epoch) == ([6, 7, 8], 8, False)
    assert page(8, limit=3, epoch=epoch) == ([], 8, False)
    assert page(0, limit=1, user="bob") == ([5], 5, False)
    # has_more only counts the principal's own events: bob's gap is skipped.
    assert page(4, limit=1) == ([6], 6, True)


def test_resync_410_only_for_clients_that_echo_the_epoch(collections_api):
    api = collections_api
    assert api.call("POST", "/api/collections", {"id": "a", "name": "A"}).status_code == 201
    epoch, head, _ = _db_state(api)
    other = "00000000-0000-4000-8000-000000000000"

    def resync(response, reason):
        assert response.status_code == 410
        assert response.get_json() == {"error": "collections_resync_required", "reason": reason}

    resync(_feed(api, 0, epoch=other), "epoch_mismatch")
    resync(_feed(api, 0, epoch="not-a-uuid"), "epoch_mismatch")
    resync(_feed(api, head + 5, epoch=other), "epoch_mismatch")
    resync(_feed(api, head + 1, epoch=epoch), "cursor_ahead")
    # The epoch is compared as a UUID.
    assert _feed(api, 0, epoch=epoch.upper()).status_code == 200
    assert _feed(api, head, epoch=epoch).get_json()["changes"] == []
    # No echo (absent or empty): the 1.2.5 empty 200 that echoes the cursor.
    for response in (_feed(api, head + 1), _feed(api, head + 1, epoch="")):
        assert response.status_code == 200
        body = response.get_json()
        assert (body["changes"], body["next_cursor"], body["has_more"]) == ([], head + 1, False)
    # A cut-over feed (new epoch, as after a re-seed) rejects the old epoch.
    db = api.connect()
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {api.manager.collection_feed_state_table()} "
            "SET epoch = gen_random_uuid(), floor_seq = head_seq"
        )
    db.commit()
    db.close()
    resync(_feed(api, head, epoch=epoch), "epoch_mismatch")
    fresh = _feed(api, head).get_json()
    assert fresh["epoch"] != epoch and fresh["floor_seq"] == head
    assert _feed(api, head, epoch=fresh["epoch"]).status_code == 200


def _replay(changes):
    """Collections and items rebuilt from feed events, as a client would."""
    collections, items = {}, {}
    for change in changes:
        payload, cid = change["payload"], change["collection_id"]
        if change["entity_kind"] == "collection":
            if change["operation"] == "delete":
                collections.pop(cid, None)
                items = {k: v for k, v in items.items() if v["collection_id"] != cid}
            else:
                collections[cid] = dict(payload)
            continue
        if cid in collections:
            collections[cid]["revision"] = payload["collection_revision"]
        if change["operation"] == "delete":
            items.pop(change["entity_id"], None)
        else:
            items[change["entity_id"]] = {**payload, "collection_id": cid}
    return collections, items


def _item_key(item):
    return (item["id"], item["collection_id"], item["kind"], item["track_id"],
            item["provider_album_id"], item["album_key"], item["title"],
            item["artist"], item["album"], item["cover_item_id"], item["position"])


def _snapshot(api, user="alice"):
    response = api.call("GET", "/api/collections/snapshot", user=user)
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def test_snapshot_returns_the_principals_state_matching_the_feed(collections_api):
    api = collections_api
    for cid in ("a", "b", "gone"):
        assert api.call("POST", "/api/collections", {"id": cid, "name": cid.upper()}).status_code == 201
    _batch(api, "a", ["t1", "t2", "t3"])
    assert api.call("PUT", "/api/collections/b/items/alb", {
        "kind": "album", "provider_album_id": "p1", "title": "Alb", "artist": "X",
        "position": 4}).status_code == 200
    _batch(api, "gone", ["g1"])
    assert api.call("DELETE", "/api/collections/a/items/a-t2", {}).status_code == 200
    assert api.call("DELETE", "/api/collections/gone", {}).status_code == 200
    assert api.call("POST", "/api/collections", {"id": "bobs", "name": "Bob"},
                    user="bob").status_code == 201
    _batch(api, "bobs", ["b1"], user="bob")
    epoch, head, floor = _db_state(api)

    snapshot = _snapshot(api)
    assert set(snapshot) == {
        "schema_version", "scope", "epoch", "head_seq", "floor_seq",
        "collections", "items", "collection_count", "item_count"}
    assert (snapshot["schema_version"], snapshot["scope"]) == (1, "personal")
    assert (snapshot["epoch"], snapshot["head_seq"], snapshot["floor_seq"]) == (epoch, head, floor)
    assert [c["id"] for c in snapshot["collections"]] == ["a", "b"]
    assert snapshot["collection_count"] == 2 and snapshot["item_count"] == 3
    # Same objects as the detail route.
    for collection in snapshot["collections"]:
        detail = api.call("GET", f"/api/collections/{collection['id']}").get_json()
        assert collection == detail["collection"]
        assert [i for i in snapshot["items"] if i["collection_id"] == collection["id"]] == detail["items"]
    # A client that replays the whole feed up to head_seq reaches the same state.
    feed = _feed(api, 0, limit=500).get_json()
    assert not feed["has_more"]
    collections, items = _replay(feed["changes"])
    assert {cid: c["revision"] for cid, c in collections.items()} == {
        c["id"]: c["revision"] for c in snapshot["collections"]}
    assert sorted(map(_item_key, items.values())) == sorted(map(_item_key, snapshot["items"]))
    # Principal-scoped like the feed.
    bob = _snapshot(api, user="bob")
    assert [c["id"] for c in bob["collections"]] == ["bobs"]
    assert [i["id"] for i in bob["items"]] == ["bobs-b1"]
    shared = _snapshot(api, user=None)
    assert (shared["scope"], shared["collections"], shared["items"]) == ("shared", [], [])


def test_snapshot_is_one_repeatable_read_view_under_concurrent_writes(collections_api, monkeypatch):
    api = collections_api
    manager = api.manager
    assert api.call("POST", "/api/collections", {"id": "a", "name": "A"}).status_code == 201
    _batch(api, "a", ["t1", "t2", "t3"])
    _, head_before, _ = _db_state(api)
    paused, release = threading.Event(), threading.Event()
    original = manager._feed_state

    def pause_after_head(cur):
        state = original(cur)
        if threading.current_thread().name == "snapshot":
            paused.set()
            assert release.wait(10)
        return state

    monkeypatch.setattr(manager, "_feed_state", pause_after_head)
    result = {}
    reader = threading.Thread(target=lambda: result.update(snap=_snapshot(api)), name="snapshot")
    reader.start()
    try:
        assert paused.wait(10)
        # A writer commits between the snapshot's head read and its row reads.
        _batch(api, "a", ["t4", "t5"])
        assert api.call("POST", "/api/collections", {"id": "c", "name": "C"}).status_code == 201
    finally:
        release.set()
        reader.join(15)
    assert not reader.is_alive()
    snap = result["snap"]
    assert snap["head_seq"] == head_before
    assert [c["id"] for c in snap["collections"]] == ["a"]
    assert snap["collections"][0]["revision"] == 2
    assert sorted(i["track_id"] for i in snap["items"]) == ["t1", "t2", "t3"]
    # The next snapshot shows all of it, with the head that includes it.
    after = _snapshot(api)
    assert after["head_seq"] == _db_state(api)[1] == head_before + 3
    assert [c["id"] for c in after["collections"]] == ["a", "c"]
    assert sorted(i["track_id"] for i in after["items"]) == ["t1", "t2", "t3", "t4", "t5"]
    # Resuming the feed from the first snapshot's head brings exactly the writer's events.
    tail = _feed(api, snap["head_seq"], epoch=snap["epoch"]).get_json()
    assert [c["entity_id"] for c in tail["changes"]] == ["a-t4", "a-t5", "c"]


def test_snapshot_503s_with_retry_after_when_unavailable(collections_api, monkeypatch):
    api = collections_api
    db = api.connect()
    with db.cursor() as cur:
        cur.execute(f"UPDATE {api.manager.collection_feed_state_table()} SET protocol_version = 999")
    db.commit()
    db.close()
    response = api.call("GET", "/api/collections/snapshot")
    assert response.status_code == 503
    assert response.get_json() == {"error": "collection_feed_unavailable"}
    assert response.headers["Retry-After"] == "5"
    for dsn in (None, "host=127.0.0.1 port=1 dbname=none user=none", "not a dsn"):
        monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL", dsn)
        response = api.call("GET", "/api/collections/snapshot")
        assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
        assert response.get_json() == {"error": "collection_feed_unavailable"}
    monkeypatch.setattr(api.manager, "get_setting", lambda key, default=None: False)
    assert api.call("GET", "/api/collections/snapshot").status_code == 404


def test_snapshot_builds_one_at_a_time_per_process(collections_api, monkeypatch):
    """A 100k-item snapshot costs about 250 MB while it is built, so a worker
    builds one at a time; a request that cannot start within 2 s gets 503."""
    api = collections_api
    manager = api.manager
    assert api.call("POST", "/api/collections", {"id": "a", "name": "A"}).status_code == 201
    assert manager.SNAPSHOT_WAIT_S == 2
    original = manager._read_snapshot
    active, peak = [0], [0]
    guard = threading.Lock()

    def slow_read(cur, principal):
        with guard:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        try:
            time.sleep(0.15)
            return original(cur, principal)
        finally:
            with guard:
                active[0] -= 1

    monkeypatch.setattr(manager, "_read_snapshot", slow_read)
    results = []
    readers = [threading.Thread(target=lambda: results.append(
        api.call("GET", "/api/collections/snapshot"))) for _ in range(4)]
    for reader in readers:
        reader.start()
    for reader in readers:
        reader.join(20)
    assert [r.status_code for r in results] == [200] * 4
    assert peak[0] == 1
    # Held elsewhere: 503 after the wait, and the slot is released after
    # every outcome, including an error.
    monkeypatch.setattr(manager, "SNAPSHOT_WAIT_S", 0.2)
    assert manager._SNAPSHOT_SLOT.acquire(timeout=1)
    try:
        started = time.monotonic()
        busy = api.call("GET", "/api/collections/snapshot")
        waited = time.monotonic() - started
    finally:
        manager._SNAPSHOT_SLOT.release()
    assert (busy.status_code, busy.get_json(), busy.headers["Retry-After"]) == (
        503, {"error": "collection_feed_unavailable"}, "5")
    assert 0.15 <= waited < 2
    monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL", None)
    assert api.call("GET", "/api/collections/snapshot").status_code == 503
    assert manager._SNAPSHOT_SLOT.acquire(blocking=False)
    manager._SNAPSHOT_SLOT.release()


def test_floor_seq_is_the_head_at_cutover(migrated_db, run_plugin_migration):
    manager = load_plugin().collection_manager
    state, changes = manager.collection_feed_state_table(), manager.collection_changes_table()
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT floor_seq FROM {state}")
        assert cur.fetchone() == (0,)
        # A pre-K8 installation: no floor_seq, three events.
        cur.execute(f"ALTER TABLE {state} DROP COLUMN floor_seq")
        cur.execute(
            f"INSERT INTO {changes} (seq, principal, collection_id, entity_kind, entity_id, "
            "operation, payload) SELECT n, 'user:a', 'c', 'collection', 'c', 'upsert', "
            "'{}'::jsonb FROM generate_series(1, 3) n"
        )
        cur.execute(f"UPDATE {state} SET head_seq = 3 RETURNING epoch::text")
        epoch = cur.fetchone()[0]
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT epoch::text, head_seq, floor_seq FROM {state}")
        assert cur.fetchone() == (epoch, 3, 3)
        cur.execute(f"UPDATE {state} SET head_seq = 9")
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT epoch::text, head_seq, floor_seq FROM {state}")
        assert cur.fetchone() == (epoch, 9, 3)
        cur.execute(
            "SELECT attnotnull FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attname = 'floor_seq'", (state,))
        assert cur.fetchone() == (True,)
    migrated_db.rollback()
