"""K9 collections conflicts and shelf receipt fingerprints (P3-4b).

``X-Lumae-Collections-Contract: 2`` opts a request in. Without it, every
response is 1.2.5's byte for byte: ``collection_conflicts_v1_golden.json``
was recorded from phase/3-semantics 8742700, before K9, by this module's
``_transcript``.
"""

import importlib
import json
import pathlib
import threading
import time
import types
import uuid

import pytest

from test_collection_feed_epoch_postgres import (  # noqa: F401 (fixture)
    TRANSCRIPT_HEADERS,
    _checksum,
    _normalized,
    collections_api,
)


GOLDEN = pathlib.Path(__file__).with_name("collection_conflicts_v1_golden.json")
CONTRACT = {"X-Lumae-Collections-Contract": "2"}
SHELVES = "/api/shelves/mutations?catalog_id=catalog-a"


@pytest.fixture
def conflicts_api(collections_api, monkeypatch):
    """``collections_api`` with the shelf routes on the same per-request connection."""
    shelves = importlib.import_module("plugins.LumaeAnalysis.shelves")
    manager = collections_api.manager
    monkeypatch.setattr(shelves, "get_db", lambda: manager.get_db())
    collections_api.shelves = shelves
    return collections_api


def _backup(collections):
    body = {"format": "lumae-living-collections", "version": 1, "collections": collections}
    body["checksum"] = _checksum(collections)
    return body


def _shelf_add(identifier, title=None, at=10):
    return {"id": f"add-{identifier}", "operation": "add", "member": {
        "id": identifier, "entityId": identifier, "kind": "album",
        "title": title or identifier, "artist": "Artist", "addedAt": at}}


def _transcript(call, headers=None):
    """Every K9-relevant request of an old client, then its feeds.

    Returns [(label, status, raw body bytes, {header: value})]. Timestamps
    and generated ids are replaced by ``_normalized``.
    """
    out = []

    def record(label, method, path, body=None, **options):
        response = call(method, path, body, headers=headers, **options)
        kept = {name: response.headers[name] for name in TRANSCRIPT_HEADERS
                if name in response.headers}
        out.append((label, response.status_code, response.get_data(), kept))
        return response

    # Create: an existing id, a tombstoned id, key replays and key conflicts.
    record("create c1", "POST", "/api/collections", {"id": "c1", "name": "One"}, key="k-create")
    record("create c1 replay", "POST", "/api/collections", {"id": "c1", "name": "One"},
           key="k-create")
    record("create existing c1", "POST", "/api/collections", {"id": "c1", "name": "Again"})
    record("create existing c1 keyed", "POST", "/api/collections",
           {"id": "c1", "name": "Again"}, key="k-again")
    record("create key conflict", "POST", "/api/collections", {"id": "c1", "name": "Changed"},
           key="k-create")
    record("create c2", "POST", "/api/collections", {"id": "c2", "name": "Two"})
    record("delete c2", "DELETE", "/api/collections/c2", {})
    record("create deleted c2", "POST", "/api/collections", {"id": "c2", "name": "Two again"})
    record("create generated", "POST", "/api/collections", {"name": "Generated"}, key="k-gen")
    record("create generated key conflict", "POST", "/api/collections", {"name": "Other"},
           key="k-gen")
    record("create c3", "POST", "/api/collections", {"id": "c3", "name": "Three"})
    # Memberships: an update of the same item, duplicates by track, provider
    # album id and album key, from other requests and within one batch.
    record("put i1", "PUT", "/api/collections/c1/items/i1",
           {"kind": "track", "track_id": "t1", "title": "One"}, key="k-put")
    record("put i1 update", "PUT", "/api/collections/c1/items/i1",
           {"kind": "track", "track_id": "t1", "title": "One!", "position": 2})
    record("put duplicate track", "PUT", "/api/collections/c1/items/i2",
           {"kind": "track", "track_id": "t1", "title": "Dup"})
    record("put key conflict", "PUT", "/api/collections/c1/items/i1",
           {"kind": "track", "track_id": "t1", "title": "Other"}, key="k-put")
    record("batch", "POST", "/api/collections/c1/items/batch", {"items": [
        {"id": "i3", "kind": "track", "track_id": "t3"},
        {"id": "i4", "kind": "track", "track_id": "t1"},
        {"id": "a1", "kind": "album", "provider_album_id": "al1", "title": "Album"},
        {"id": "a2", "kind": "album", "provider_album_id": "al1"},
        {"id": "k1", "kind": "album", "album_key": "x::y"},
        {"id": "k2", "kind": "album", "album_key": "x::y"},
    ]}, key="k-batch")
    record("batch key conflict", "POST", "/api/collections/c1/items/batch",
           {"items": [{"id": "i5", "kind": "track", "track_id": "t5"}]}, key="k-batch")
    record("batch duplicates", "POST", "/api/collections/c1/items/batch", {"items": [
        {"id": "b1", "kind": "album", "provider_album_id": "al1"},
        {"id": "b2", "kind": "album", "album_key": "x::y"},
        {"id": "b3", "kind": "track", "track_id": "t6"},
    ], "base_revision": 5})
    record("same track elsewhere", "PUT", "/api/collections/c3/items/c3-t1",
           {"kind": "track", "track_id": "t1"})
    record("foreign item id", "PUT", "/api/collections/c3/items/i1",
           {"kind": "track", "track_id": "t9"})
    # Other routes: revision conflicts, key replays and key conflicts.
    record("stale patch", "PATCH", "/api/collections/c1", {"name": "x", "base_revision": 1})
    record("patch", "PATCH", "/api/collections/c1", {"name": "Uno"}, key="k-patch")
    record("patch key conflict", "PATCH", "/api/collections/c1", {"name": "Ein"}, key="k-patch")
    record("patch key on another collection", "PATCH", "/api/collections/c3", {"name": "x"},
           key="k-patch")
    record("item delete", "DELETE", "/api/collections/c1/items/i3", {}, key="k-idel")
    record("item delete key conflict", "DELETE", "/api/collections/c1/items/i3",
           {"base_revision": 99}, key="k-idel")
    record("batch delete", "DELETE", "/api/collections/c1/items/batch",
           {"item_ids": ["a1", "missing"]}, key="k-bdel")
    record("batch delete key conflict", "DELETE", "/api/collections/c1/items/batch",
           {"item_ids": ["k1"]}, key="k-bdel")
    record("delete c3", "DELETE", "/api/collections/c3", {}, key="k-del")
    record("delete key conflict", "DELETE", "/api/collections/c3", {"base_revision": 1},
           key="k-del")
    record("delete c3 again", "DELETE", "/api/collections/c3", {})
    record("delete missing", "DELETE", "/api/collections/none", {}, key="k-delnone")
    record("delete missing key conflict", "DELETE", "/api/collections/none",
           {"base_revision": 5}, key="k-delnone")
    record("patch deleted", "PATCH", "/api/collections/c3", {"name": "x"})
    # Restores: fresh collections, so no membership can collide.
    backup = _backup([
        {"name": "Restored", "items": [
            {"kind": "track", "track_id": "t1", "title": "R1"},
            {"kind": "album", "provider_album_id": "al1"},
            {"kind": "album", "album_key": "x::y"},
        ]},
        {"name": "Empty", "items": []},
    ])
    record("restore", "POST", "/api/collections/restore", backup, key="k-restore")
    record("restore replay", "POST", "/api/collections/restore", backup, key="k-restore")
    record("restore key conflict", "POST", "/api/collections/restore",
           _backup([{"name": "Else", "items": []}]), key="k-restore")
    record("create with a restore key", "POST", "/api/collections", {"id": "c4", "name": "Four"},
           key="k-restore")
    record("restore with a patch key", "POST", "/api/collections/restore", backup, key="k-patch")
    record("restore duplicate items", "POST", "/api/collections/restore", _backup([
        {"name": "Dup", "items": [{"kind": "track", "track_id": "t1"},
                                  {"kind": "track", "track_id": "t1"}]}]))
    # Reads and the feed.
    record("detail c1", "GET", "/api/collections/c1")
    record("list", "GET", "/api/collections")
    cursor = 0
    for page in range(20):
        response = record(f"feed page {page}", "GET",
                          f"/api/collections/changes?cursor={cursor}&limit=25")
        body = response.get_json()
        if not body["changes"]:
            break
        cursor = body["next_cursor"]
    # Shelves: the mutation id is the idempotency key.
    record("shelf add", "POST", SHELVES, _shelf_add("m1"))
    record("shelf add replay", "POST", SHELVES, _shelf_add("m1"))
    record("shelf add changed body", "POST", SHELVES, _shelf_add("m1", title="Changed"))
    record("shelf remove under the add id", "POST", SHELVES,
           {"id": "add-m1", "operation": "remove", "memberId": "m1", "at": 20})
    record("shelf add m2", "POST", SHELVES, _shelf_add("m2"))
    order = {"id": "arrange", "operation": "order", "kind": "album", "ids": ["m2", "m1"],
             "baseRevision": 0}
    record("shelf order", "POST", SHELVES, order)
    record("shelf order changed body", "POST", SHELVES, {**order, "ids": ["m1", "m2"]})
    record("shelf order conflict", "POST", SHELVES, {**order, "id": "arrange-2"})
    record("shelf changes", "GET", "/api/shelves/changes?catalog_id=catalog-a")
    return out


def test_old_client_transcript_is_byte_identical_to_the_pre_k9_golden(conflicts_api):
    """Without the header, every K9-relevant response is 1.2.5's, byte for
    byte: existing and deleted ids on create, silent membership remaps, key
    conflicts without ``current``, restores, the feed, and shelf receipts that
    replay whatever the body."""
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert _normalized(_transcript(conflicts_api.call)) == golden


def _c2(api, method, path, body=None, **options):
    """A contract-2 request."""
    return api.call(method, path, body, headers=CONTRACT, **options)


def _head(api):
    db = api.connect()
    try:
        with db.cursor() as cur:
            cur.execute(f"SELECT head_seq FROM {api.manager.collection_feed_state_table()}")
            return cur.fetchone()[0]
    finally:
        db.close()


def _detail(api, collection_id, user="alice"):
    response = api.call("GET", f"/api/collections/{collection_id}", user=user)
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def _receipts(api):
    db = api.connect()
    try:
        with db.cursor() as cur:
            cur.execute(f"SELECT idempotency_key, collection_id "
                        f"FROM {api.manager.collection_mutations_table()} ORDER BY idempotency_key")
            return dict(cur.fetchall())
    finally:
        db.close()


def test_contract_2_create_with_a_taken_id_is_a_conflict(conflicts_api):
    api = conflicts_api
    created = _c2(api, "POST", "/api/collections", {"id": "c1", "name": "One"}, key="k1")
    assert created.status_code == 201
    one = created.get_json()["collection"]
    head = _head(api)

    exists = _c2(api, "POST", "/api/collections", {"id": "c1", "name": "Again"})
    assert (exists.status_code, exists.get_json()) == (
        409, {"error": "collection_exists", "current": one})
    # The same create under its key still replays its 201.
    replay = _c2(api, "POST", "/api/collections", {"id": "c1", "name": "One"}, key="k1")
    assert replay.status_code == 201 and replay.headers["Idempotency-Replayed"] == "true"
    # A 409 stores no receipt.
    keyed = _c2(api, "POST", "/api/collections", {"id": "c1", "name": "Again"}, key="k2")
    assert keyed.get_json()["error"] == "collection_exists"
    assert "k2" not in _receipts(api)

    assert api.call("DELETE", "/api/collections/c1", {}).status_code == 200
    tombstone = _c2(api, "POST", "/api/collections", {"id": "c1", "name": "Again"})
    body = tombstone.get_json()
    assert tombstone.status_code == 409 and set(body) == {"error", "current"}
    assert body["error"] == "collection_deleted"
    assert body["current"]["id"] == "c1" and body["current"]["deleted_at"] is not None
    assert (body["current"]["revision"], body["current"]["name"]) == (2, "One")
    # The conflicts wrote nothing: only the delete moved the head.
    assert _head(api) == head + 1
    # Without the header the 1.2.5 answer stays.
    old = api.call("POST", "/api/collections", {"id": "c1", "name": "x"})
    assert (old.status_code, old.get_json()) == (201, {"collection": None})
    # A new id is created as before.
    fresh = _c2(api, "POST", "/api/collections", {"id": "c2", "name": "Two"})
    assert fresh.status_code == 201 and fresh.get_json()["collection"]["id"] == "c2"


def test_contract_2_key_conflicts_carry_the_keyed_collection(conflicts_api):
    api = conflicts_api
    assert api.call("POST", "/api/collections", {"id": "c1", "name": "One"}).status_code == 201
    assert api.call("POST", "/api/collections", {"id": "c3", "name": "Three"}).status_code == 201
    assert api.call("PATCH", "/api/collections/c1", {"name": "Uno"}, key="kp").status_code == 200

    def conflict(method, path, body, key):
        head = _head(api)
        response = _c2(api, method, path, body, key=key)
        assert response.status_code == 409, response.get_json()
        payload = response.get_json()
        assert set(payload) == {"error", "current"}
        assert payload["error"] == "idempotency_key_conflict"
        # Old clients keep the 1.2.5 body, byte for byte.
        old = api.call(method, path, body, key=key)
        assert (old.status_code, old.get_data()) == (409, b'{"error":"idempotency_key_conflict"}\n')
        assert _head(api) == head
        return payload["current"]

    c1 = _detail(api, "c1")["collection"]
    assert conflict("PATCH", "/api/collections/c1", {"name": "Ein"}, "kp") == c1
    # The collection the key applied to, not the one this request names.
    assert conflict("PATCH", "/api/collections/c3", {"name": "x"}, "kp") == c1
    # The state now, not the stored response.
    assert api.call("PATCH", "/api/collections/c1", {"name": "Later"}).status_code == 200
    now = conflict("PATCH", "/api/collections/c1", {"name": "Ein"}, "kp")
    assert (now["name"], now["revision"]) == ("Later", 3)

    # A create without an id: the key finds the collection it created.
    generated = api.call("POST", "/api/collections", {"name": "G"}, key="kg").get_json()["collection"]
    assert conflict("POST", "/api/collections", {"name": "H"}, "kg") == generated
    # Every item route records its collection.
    assert api.call("PUT", "/api/collections/c1/items/i1", {"kind": "track", "track_id": "t1"},
                    key="kput").status_code == 200
    assert api.call("POST", "/api/collections/c1/items/batch",
                    {"items": [{"id": "i2", "kind": "track", "track_id": "t2"}]},
                    key="kbatch").status_code == 200
    assert api.call("DELETE", "/api/collections/c1/items/i2", {}, key="kidel").status_code == 200
    assert api.call("DELETE", "/api/collections/c1/items/batch", {"item_ids": ["i1"]},
                    key="kbdel").status_code == 200
    c1 = _detail(api, "c1")["collection"]
    assert conflict("PUT", "/api/collections/c1/items/i1",
                    {"kind": "track", "track_id": "t9"}, "kput") == c1
    assert conflict("POST", "/api/collections/c1/items/batch", {"items": []}, "kbatch") == c1
    assert conflict("DELETE", "/api/collections/c1/items/i2", {"base_revision": 1}, "kidel") == c1
    assert conflict("DELETE", "/api/collections/c1/items/batch", {"item_ids": ["x"]}, "kbdel") == c1
    # A deleted collection is reported as its tombstone; a missing one as null.
    assert api.call("DELETE", "/api/collections/c3", {}, key="kdel").status_code == 200
    tombstone = conflict("DELETE", "/api/collections/c3", {"base_revision": 1}, "kdel")
    assert tombstone["id"] == "c3" and tombstone["deleted_at"] is not None
    assert api.call("DELETE", "/api/collections/none", {}, key="knone").status_code == 200
    assert conflict("DELETE", "/api/collections/none", {"base_revision": 1}, "knone") is None
    # A restore has no single collection: null, whichever route reuses its key.
    backup = _backup([{"name": "R", "items": [{"kind": "track", "track_id": "t1"}]}])
    assert api.call("POST", "/api/collections/restore", backup, key="kr").status_code == 201
    assert conflict("POST", "/api/collections/restore", _backup([{"name": "S"}]), "kr") is None
    assert conflict("POST", "/api/collections", {"id": "c4", "name": "Four"}, "kr") is None
    # A restore that reuses another route's key reports that key's collection.
    assert conflict("POST", "/api/collections/restore", backup, "kp")["id"] == "c1"

    assert _receipts(api) == {
        "kbatch": "c1", "kbdel": "c1", "kdel": "c3", "kg": generated["id"], "kidel": "c1",
        "knone": "none", "kp": "c1", "kput": "c1", "kr": None,
    }


def test_contract_2_key_held_by_an_unfinished_restore_reports_null(conflicts_api):
    api = conflicts_api
    db = api.connect()
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {api.manager.collection_restores_table()} "
            "(principal, idempotency_key, request_fingerprint, restore_id, chunk_rows, "
            "chunk_count, chunks_done) VALUES ('user:alice', 'live', 'another', "
            "gen_random_uuid(), 2000, 2, 1)"
        )
    db.commit()
    db.close()
    response = _c2(api, "POST", "/api/collections", {"id": "c1", "name": "One"}, key="live")
    assert (response.status_code, response.get_json()) == (
        409, {"error": "idempotency_key_conflict", "current": None})
    old = api.call("POST", "/api/collections", {"id": "c1", "name": "One"}, key="live")
    assert (old.status_code, old.get_json()) == (409, {"error": "idempotency_key_conflict"})
    assert api.call("GET", "/api/collections/c1").status_code == 404


def test_contract_2_membership_conflicts_write_nothing(conflicts_api):
    api = conflicts_api
    assert api.call("POST", "/api/collections", {"id": "c1", "name": "One"}).status_code == 201
    assert api.call("POST", "/api/collections", {"id": "c2", "name": "Two"}).status_code == 201
    assert _c2(api, "PUT", "/api/collections/c1/items/i1",
               {"kind": "track", "track_id": "t1"}).status_code == 200
    # Updating the item that holds the membership is not a conflict.
    update = _c2(api, "PUT", "/api/collections/c1/items/i1",
                 {"kind": "track", "track_id": "t1", "title": "Renamed", "position": 4})
    assert update.status_code == 200 and update.get_json()["items"][0]["id"] == "i1"
    assert _c2(api, "POST", "/api/collections/c1/items/batch", {"items": [
        {"id": "a1", "kind": "album", "provider_album_id": "al1"},
        {"id": "k1", "kind": "album", "album_key": "x::y"},
    ]}).status_code == 200
    before = _detail(api, "c1")
    head = _head(api)

    duplicate = _c2(api, "PUT", "/api/collections/c1/items/i2",
                    {"kind": "track", "track_id": "t1", "title": "Dup"})
    assert (duplicate.status_code, duplicate.get_json()) == (409, {
        "error": "membership_conflict",
        "item_id": "i2",
        "existing_item_id": "i1",
        "conflicts": [{"item_id": "i2", "existing_item_id": "i1"}],
        "current": before["collection"],
    })
    # Every conflict of a batch is reported, in request order, including an
    # item that repeats an earlier item of the same request.
    batch = _c2(api, "POST", "/api/collections/c1/items/batch", {"items": [
        {"id": "i3", "kind": "track", "track_id": "t3"},
        {"id": "i4", "kind": "track", "track_id": "t1"},
        {"id": "a2", "kind": "album", "provider_album_id": "al1"},
        {"id": "k2", "kind": "album", "album_key": "x::y"},
        {"id": "i5", "kind": "track", "track_id": "t5"},
        {"id": "i6", "kind": "track", "track_id": "t5"},
    ]})
    assert (batch.status_code, batch.get_json()) == (409, {
        "error": "membership_conflict",
        "item_id": "i4",
        "existing_item_id": "i1",
        "conflicts": [
            {"item_id": "i4", "existing_item_id": "i1"},
            {"item_id": "a2", "existing_item_id": "a1"},
            {"item_id": "k2", "existing_item_id": "k1"},
            {"item_id": "i6", "existing_item_id": "i5"},
        ],
        "current": before["collection"],
    })
    # Moving an item onto a membership that another item holds conflicts too.
    moved = _c2(api, "PUT", "/api/collections/c1/items/a1", {"kind": "album", "album_key": "x::y"})
    assert (moved.status_code, moved.get_json()["existing_item_id"]) == (409, "k1")
    # Nothing was written: no item, no revision, no event.
    assert _detail(api, "c1") == before
    assert _head(api) == head
    # The same track in another collection is no conflict.
    assert _c2(api, "PUT", "/api/collections/c2/items/j1",
               {"kind": "track", "track_id": "t1"}).status_code == 200
    # item_id_collection_conflict is unchanged.
    foreign = _c2(api, "PUT", "/api/collections/c2/items/i1", {"kind": "track", "track_id": "t8"})
    assert (foreign.status_code, foreign.get_json()) == (409, {"error": "item_id_collection_conflict"})
    # A 409 stores no receipt.
    assert _c2(api, "PUT", "/api/collections/c1/items/i9", {"kind": "track", "track_id": "t1"},
               key="kdup").status_code == 409
    assert "kdup" not in _receipts(api)
    # Adopting the existing id applies the write.
    adopted = _c2(api, "PUT", "/api/collections/c1/items/i1",
                  {"kind": "track", "track_id": "t1", "title": "Dup"})
    assert adopted.status_code == 200 and adopted.get_json()["items"][0]["id"] == "i1"
    assert adopted.get_json()["collection"]["revision"] == before["collection"]["revision"] + 1


@pytest.mark.parametrize("value,strict", [
    ("2", True), (" 2 ", True), ("1", False), ("3", False), ("two", False), ("", False),
    (None, False), ("2.0", False), ("02", False),
])
def test_only_the_contract_header_value_2_opts_in(conflicts_api, value, strict):
    api = conflicts_api
    assert api.call("POST", "/api/collections", {"id": "c1", "name": "One"}).status_code == 201
    assert api.call("PUT", "/api/collections/c1/items/i1",
                    {"kind": "track", "track_id": "t1"}).status_code == 200
    headers = {} if value is None else {"X-Lumae-Collections-Contract": value}
    response = api.call("PUT", "/api/collections/c1/items/i2",
                        {"kind": "track", "track_id": "t1"}, headers=headers)
    if strict:
        assert response.status_code == 409
        assert response.get_json()["existing_item_id"] == "i1"
    else:
        assert response.status_code == 200
        assert response.get_json()["items"][0]["id"] == "i1"


def test_contract_2_leaves_restores_unchanged(conflicts_api, monkeypatch):
    """A restore writes fresh collections, so the header changes nothing for
    it. Between chunks, a restore item whose membership a client has added
    meanwhile adopts that item's id, as in 1.2.5."""
    api = conflicts_api
    manager = api.manager
    backup = _backup([
        {"name": "Restored", "items": [{"kind": "track", "track_id": f"t{n}"} for n in range(4)]},
        {"name": "Empty", "items": []},
    ])

    def summary(response):
        body = response.get_json()
        return (response.status_code, body["collection_count"], body["item_count"],
                [(c["name"], c["revision"], c["track_count"]) for c in body["collections"]])

    plain = api.call("POST", "/api/collections/restore", backup, key="r", user="bob")
    strict = _c2(api, "POST", "/api/collections/restore", backup, key="r")
    assert summary(plain) == summary(strict) == (
        201, 2, 4, [("Restored", 2, 4), ("Empty", 1, 0)])

    # Chunks of 3 rows: [create Restored, t0, t1], [t2, t3, create Empty].
    monkeypatch.setattr(manager, "RESTORE_CHUNK_ROWS", 3)
    original = manager._restore_principal_collections
    chunks = []

    def client_adds_t2_before_chunk_two(cur, principal, segments):
        chunks.append(segments)
        if len(chunks) == 2:
            added = []
            client = threading.Thread(target=lambda: added.append(_c2(
                api, "PUT", f"/api/collections/{segments[0]['id']}/items/client-t2",
                {"kind": "track", "track_id": "t2"}, user="carol")))
            client.start()
            client.join(20)
            assert added[0].status_code == 200
        return original(cur, principal, segments)

    monkeypatch.setattr(manager, "_restore_principal_collections", client_adds_t2_before_chunk_two)
    chunked = _c2(api, "POST", "/api/collections/restore", backup, key="rc", user="carol")
    assert chunked.status_code == 201, chunked.get_json()
    assert len(chunks) == 2
    restored = chunked.get_json()["collections"][0]
    items = _detail(api, restored["id"], user="carol")["items"]
    assert sorted((i["track_id"], i["id"] == "client-t2") for i in items) == [
        ("t0", False), ("t1", False), ("t2", True), ("t3", False)]


def test_shelf_receipts_bind_the_request_body_under_contract_2(conflicts_api):
    api = conflicts_api
    shelves = api.shelves
    first = api.call("POST", SHELVES, _shelf_add("m1"))
    assert first.status_code == 200
    replay = api.call("POST", SHELVES, _shelf_add("m1"), headers=CONTRACT)
    assert (replay.status_code, replay.get_json()) == (200, first.get_json())
    changed = _shelf_add("m1", title="Changed")
    conflict = api.call("POST", SHELVES, changed, headers=CONTRACT)
    assert (conflict.status_code, conflict.get_json()) == (409, {"error": "idempotency_key_conflict"})
    other_operation = {"id": "add-m1", "operation": "remove", "memberId": "m1", "at": 20}
    assert api.call("POST", SHELVES, other_operation, headers=CONTRACT).status_code == 409
    # Without the header the stored success replays, as in 1.2.5.
    stale = api.call("POST", SHELVES, changed)
    assert (stale.status_code, stale.get_json()) == (200, first.get_json())
    records = api.call("GET", "/api/shelves/changes?catalog_id=catalog-a").get_json()["records"]
    assert [(r["id"], r["value"]["title"], r["value"]["deletedAt"]) for r in records] == [
        ("m1", "m1", None)]

    db = api.connect()
    with db.cursor() as cur:
        cur.execute(f"SELECT id, request_fingerprint FROM {shelves.table('shelf_mutations')}")
        assert dict(cur.fetchall()) == {"add-m1": shelves.request_fingerprint(_shelf_add("m1"))}
        # A receipt from before 1.3.0 has no fingerprint and replays in both modes.
        cur.execute(f"UPDATE {shelves.table('shelf_mutations')} SET request_fingerprint = NULL")
    db.commit()
    db.close()
    for headers in (None, CONTRACT):
        legacy = api.call("POST", SHELVES, changed, headers=headers)
        assert (legacy.status_code, legacy.get_json()) == (200, first.get_json())


@pytest.mark.parametrize("headers", [None, CONTRACT])
def test_a_rejected_shelf_mutation_is_never_stored_or_replayed(conflicts_api, headers):
    """A 409 is never stored: its retry is applied again, never replayed as 200."""
    api = conflicts_api
    shelves = api.shelves
    remove = {"id": "remove-ghost", "operation": "remove", "memberId": "ghost", "at": 20}
    for _attempt in range(2):
        rejected = api.call("POST", SHELVES, remove, headers=headers)
        assert (rejected.status_code, rejected.get_json()) == (
            409, {"error": "unknown_membership_period"})
    db = api.connect()
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {shelves.table('shelf_mutations')}")
        assert cur.fetchone()[0] == 0
    db.rollback()
    db.close()
    # Once the member exists, the same mutation id applies instead of replaying.
    assert api.call("POST", SHELVES, _shelf_add("ghost"), headers=headers).status_code == 200
    applied = api.call("POST", SHELVES, remove, headers=headers)
    assert applied.status_code == 200
    assert applied.get_json()["records"][0]["value"]["deletedAt"] == 20


def _shelf_member(identifier, entity):
    return {"id": f"add-{identifier}", "operation": "add", "member": {
        "id": identifier, "entityId": entity, "kind": "album",
        "title": identifier, "artist": "Artist", "addedAt": 10}}


def _shelf_page(api, cursor, user="alice"):
    return api.call("GET", f"/api/shelves/changes?catalog_id=catalog-a&cursor={cursor}",
                    user=user).get_json()


def test_shelf_rekey_allocates_its_seq_under_the_scope_lock(
        conflicts_api, second_connection, monkeypatch):
    """LUM-004 for shelves: a rekey that commits after a later mutation must
    not hide its record from a reader paging by ``seq > cursor``.

    A client mutation holds the scope lock; the rekey starts and must wait for
    that lock before it allocates a seq. Had it allocated first, the mutation
    would take the higher seq and commit first, and a reader that saw it would
    skip the rekeyed record forever.
    """
    api = conflicts_api
    shelves = api.shelves
    assert api.call("POST", SHELVES, _shelf_member("m1", "old-id")).status_code == 200
    cursor = _shelf_page(api, 0)["cursor"]
    locked, go = threading.Event(), threading.Event()
    original = shelves.apply_mutation

    def paused(cur, scope, body):
        # Called with the scope locked; the seq is allocated after ``go``.
        locked.set()
        assert go.wait(20)
        return original(cur, scope, body)

    monkeypatch.setattr(shelves, "apply_mutation", paused)
    result = {}
    writer = threading.Thread(target=lambda: result.update(
        add=api.call("POST", SHELVES, _shelf_member("m2", "other"))))
    writer.start()
    assert locked.wait(20)

    rekeyed, commit = threading.Event(), threading.Event()

    def rekey():
        with second_connection.cursor() as cur:
            shelves.rekey_shelves(cur, "catalog-a", {"old-id": "new-id"})
        rekeyed.set()
        assert commit.wait(20)
        second_connection.commit()

    rekeying = threading.Thread(target=rekey)
    rekeying.start()
    # Wait until the rekey waits on the writer's scope lock (without the lock
    # it finishes at once instead).
    pid = second_connection.get_backend_pid()
    observer = api.connect()
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not rekeyed.is_set():
            with observer.cursor() as cur:
                cur.execute("SELECT cardinality(pg_blocking_pids(%s))", (pid,))
                blocked = cur.fetchone()[0] > 0
            observer.rollback()
            if blocked:
                break
            time.sleep(0.02)
        else:
            assert rekeyed.is_set(), "the rekey neither waited nor finished"
    finally:
        observer.close()
    go.set()
    writer.join(20)
    assert result["add"].status_code == 200
    assert rekeyed.wait(20)
    # The writer has committed; the rekey has written but not committed.
    first = _shelf_page(api, cursor)
    commit.set()
    rekeying.join(20)
    second = _shelf_page(api, first["cursor"])
    seen = {r["id"]: r["value"] for r in first["records"] + second["records"]}
    assert seen["m2"]["entityId"] == "other"
    assert seen.get("m1", {}).get("entityId") == "new-id", "the rekeyed record was skipped"


def test_shelf_rekey_locks_only_the_scopes_it_rewrites(conflicts_api, second_connection):
    api = conflicts_api
    shelves = api.shelves
    assert api.call("POST", SHELVES, _shelf_member("a", "old-id")).status_code == 200
    assert api.call("POST", SHELVES, _shelf_member("b", "kept"), user="bob").status_code == 200
    with second_connection.cursor() as cur:
        shelves.rekey_shelves(cur, "catalog-a", {"old-id": "new-id"})
    # The open rekey holds alice's scope only: bob's writes do not wait.
    observer = api.connect()
    with observer.cursor() as cur:
        cur.execute(
            f"SELECT principal FROM {shelves.table('shelf_scopes')} "
            "WHERE catalog_id = 'catalog-a' FOR UPDATE SKIP LOCKED"
        )
        assert cur.fetchall() == [("user:bob",)]
    observer.rollback()
    observer.close()
    second_connection.commit()
    assert [r["value"]["entityId"] for r in _shelf_page(api, 0)["records"]] == ["new-id"]
    assert [r["value"]["entityId"] for r in _shelf_page(api, 0, user="bob")["records"]] == ["kept"]


def test_unknown_auth_method_is_denied_not_mapped_to_a_principal(conflicts_api, monkeypatch):
    from flask import Flask, g, request

    from test_lumae_analysis import load_plugin

    api = conflicts_api
    manager = api.manager
    db = api.connect()
    monkeypatch.setattr(manager, "get_db", lambda: db)
    app = Flask(__name__)

    @app.before_request
    def authenticate():
        g.auth_method = request.headers.get("X-Auth-Method")
        g.auth_user = request.headers.get("X-Auth-User")

    app.register_blueprint(load_plugin().bp)
    client = app.test_client()
    shared = client.post("/api/collections", json={"id": "shared", "name": "S"},
                         headers={"X-Auth-Method": "bearer"})
    assert shared.status_code == 201
    for user in (None, "alice"):
        headers = {"X-Auth-Method": "plugin_bearer"}
        if user:
            headers["X-Auth-User"] = user
        assert client.get("/api/collections", headers=headers).status_code == 401
        assert client.get("/api/collections/changes", headers=headers).status_code == 401
        assert client.post("/api/collections", json={"id": "x", "name": "X"},
                           headers=headers).status_code == 401
        assert client.patch("/api/collections/shared", json={"name": "Mine"},
                            headers=headers).status_code == 401
        assert client.get("/api/shelves/changes?catalog_id=catalog-a",
                          headers=headers).status_code == 401
        assert client.post(SHELVES, json=_shelf_add("m1"), headers=headers).status_code == 401
        db.rollback()
    with db.cursor() as cur:
        cur.execute(f"SELECT principal, id, name FROM {manager.collections_table()}")
        assert cur.fetchall() == [("__global__", "shared", "S")]
        cur.execute(f"SELECT count(*) FROM {api.shelves.table('shelf_records')}")
        assert cur.fetchone()[0] == 0
    db.rollback()
    # The host's own methods are unchanged.
    def ids(headers):
        return [c["id"] for c in client.get("/api/collections", headers=headers).get_json()["collections"]]

    assert ids({}) == ["shared"]
    assert ids({"X-Auth-Method": "bearer"}) == ["shared"]
    assert ids({"X-Auth-Method": "session", "X-Auth-User": "alice"}) == []
    assert client.get("/api/collections", headers={"X-Auth-Method": "session"}).status_code == 401
    db.close()


def test_health_answers_200_with_a_null_scope_for_an_unknown_auth_method(monkeypatch):
    """Health is the capability probe: a host auth method the plugin does not
    know must not break it. The routes that need a principal answer 401; health
    answers 200 with every principal ``scope`` null and nothing else changed.
    A malformed session still gets 401 from health, as in 1.2.5."""
    from flask import Flask, g, request

    from test_lumae_analysis import load_plugin

    mod = load_plugin()
    manager = mod.collection_manager
    monkeypatch.setattr(mod.host_api.config, "DATABASE_URL", None, raising=False)
    monkeypatch.setattr(manager, "collections_enabled", lambda: True)
    monkeypatch.setattr(manager, "get_db", lambda: pytest.fail("no database access expected"))
    app = Flask(__name__)

    @app.before_request
    def authenticate():
        g.auth_method = request.headers.get("X-Auth-Method")
        g.auth_user = request.headers.get("X-Auth-User")

    app.register_blueprint(mod.bp)
    client = app.test_client()
    scoped = ("collections", "shelves", "personal_discovery")

    def health(headers):
        response = client.get("/api/health", headers=headers)
        return response.status_code, response.get_json()

    status, shared = health({"X-Auth-Method": "bearer"})
    assert status == 200 and {shared["capabilities"][n]["scope"] for n in scoped} == {"shared"}
    status, personal = health({"X-Auth-Method": "session", "X-Auth-User": "alice"})
    assert status == 200 and {personal["capabilities"][n]["scope"] for n in scoped} == {"personal"}
    assert health({})[1]["capabilities"]["collections"]["scope"] == "shared"
    for user in (None, "alice"):
        headers = {"X-Auth-Method": "plugin_bearer"}
        if user:
            headers["X-Auth-User"] = user
        status, body = health(headers)
        assert status == 200
        expected = json.loads(json.dumps(shared))
        for name in scoped:
            expected["capabilities"][name]["scope"] = None
        assert body == expected
        # The routes that need the principal deny it.
        assert client.post("/api/collections", json={"name": "X"}, headers=headers).status_code == 401
        assert client.get("/api/shelves/changes?catalog_id=catalog-a",
                          headers=headers).status_code == 401
    # A malformed session keeps its 1.2.5 answer on health.
    assert client.get("/api/health", headers={"X-Auth-Method": "session"}).status_code == 401
    with app.test_request_context("/"):
        g.auth_method = "plugin_bearer"
        g.auth_user = None
        assert manager.health_scope_mode() is None


def test_current_principal_maps_only_known_auth_methods():
    from flask import Flask, g
    from werkzeug.exceptions import Unauthorized

    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    app = Flask(__name__)
    cases = [
        ("bearer", None, "__global__"),
        ("bearer", "alice", "__global__"),
        ("session", "alice", "user:alice"),
        (None, None, "__global__"),
        (None, "alice", "user:alice"),
        ("session", None, Unauthorized),
        ("plugin_bearer", None, Unauthorized),
        ("plugin_bearer", "alice", Unauthorized),
        ("api_key", None, Unauthorized),
        ("", None, Unauthorized),
    ]
    for method, user, expected in cases:
        with app.test_request_context("/"):
            g.auth_method = method
            g.auth_user = user
            if expected is Unauthorized:
                with pytest.raises(Unauthorized):
                    manager.current_principal()
            else:
                assert manager.current_principal() == expected, (method, user)


def test_library_item_ids_refuse_dot_only_ids(monkeypatch):
    from flask import Blueprint, Flask

    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    for bad in (".", "..", "...", "....", "", "a/b", "a b", "a" * 257):
        assert library._ITEM_ID_RE.fullmatch(bad) is None, bad
    for good in ("a", "a.b", ".a", "a.", "..a", "a..", "track-7", "x~y_z.flac", "a" * 256):
        assert library._ITEM_ID_RE.fullmatch(good) is not None, good
    called = []
    monkeypatch.setattr(library, "_resolve_stream_target",
                        lambda item_id: called.append(item_id) or (None, ("unsupported", 501)))
    monkeypatch.setattr(library, "_resolve_art_target",
                        lambda item_id, size: called.append(item_id))
    app = Flask(__name__)
    blueprint = Blueprint("library_ids", __name__)
    library.register_collection_library_routes(blueprint, lambda view: view)
    app.register_blueprint(blueprint)
    client = app.test_client()
    assert client.get("/api/collections/library/stream/...").status_code == 400
    assert client.get("/api/collections/library/art/...").status_code == 404
    assert called == []
    assert client.get("/api/collections/library/stream/a.b").status_code == 501
    assert called == ["a.b"]


def test_album_detail_label_reads_the_provider_catalogue_source():
    ui = importlib.import_module("plugins.LumaeAnalysis.collection_ui")
    body = ui.render_collection_workbench("Label", "Detail")
    render = body[body.index("function renderAlbumDetail("):]
    render = render[:render.index("\n")]
    # album_detail answers metadata_source "provider_catalog" (the published
    # mirror); the label used to test for "media_server" and so always said
    # track numbers were missing.
    assert "catalogued=['provider_catalog','media_server'].includes(body.metadata_source)" in render
    assert "metadata_source==='media_server'" not in body
    assert "${esc(catalogued?'media-server catalogue':'analysis metadata')}" in render
    assert ("textContent=catalogued?'Track and disc numbers loaded from your media server "
            "catalogue.':") in render


def test_unaccent_is_created_when_permitted_and_skipped_when_not(migrated_db, monkeypatch):
    migrations = importlib.import_module("plugins.LumaeAnalysis.migrations")
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    warnings = []
    monkeypatch.setattr(migrations, "logger",
                        types.SimpleNamespace(warning=lambda *args: warnings.append(args)))
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
        # The migration installed it (the test role may) or found it.
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'unaccent'")
        assert cur.fetchone() is not None
        assert migrations.ensure_extension(cur, "unaccent") is True
        # Absent and permitted: created (rolled back below).
        cur.execute("DROP EXTENSION unaccent")
        assert migrations.ensure_extension(cur, "unaccent") is True
        cur.execute("SELECT to_regprocedure('unaccent(text)') IS NOT NULL")
        assert cur.fetchone()[0] is True
    migrated_db.rollback()
    assert warnings == []

    role = f"lumae_noext_{uuid.uuid4().hex[:12]}"
    with migrated_db.cursor() as cur:
        cur.execute(f"CREATE ROLE {role} NOLOGIN")
    migrated_db.commit()
    try:
        with migrated_db.cursor() as cur:
            cur.execute("DROP EXTENSION unaccent")
            cur.execute(f"GRANT USAGE, CREATE ON SCHEMA {schema} TO {role}")
            cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO {role}")
            cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA {schema} TO {role}")
            cur.execute(f"SET LOCAL ROLE {role}")
            # The collections migration completes without the extension.
            manager.migrate_collections(migrated_db)
            assert len(warnings) == 1
            message = warnings[0][0] % warnings[0][1:]
            assert "extension unaccent" in message and "SQLSTATE 42501" in message
            cur.execute("SELECT count(*) FROM pg_extension WHERE extname = 'unaccent'")
            assert cur.fetchone()[0] == 0
            cur.execute(f"SELECT count(*) FROM {manager.collections_table()}")
            assert cur.fetchone()[0] == 0
    finally:
        migrated_db.rollback()
        with migrated_db.cursor() as cur:
            cur.execute(f"DROP ROLE {role}")
        migrated_db.commit()
