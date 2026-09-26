"""P3-4c: a provider-ID rekey reaches collections through their protocol.

Items are rekeyed under their parents' locks, collisions merge with a delete
event, revisions are bumped and the rekey is appended to the feed as new
events. Delivered events and receipts are never rewritten. A principal whose
collision cannot be merged is deferred with a diagnostic; the others proceed.
"""

import json
from types import SimpleNamespace

from test_collection_feed_epoch_postgres import collections_api  # noqa: F401 (fixture)


P = "plugin_lumae_analysis__"
OLD = {
    "track": "e3b7fc2ae9447bbec37a13bf916e3cf6",
    "album": "0123456789abcdef0123456789abcdef",
    "artist": "11111111111111111111111111111111",
}
ITEM_FIELDS = (
    "id", "kind", "track_id", "provider_album_id", "album_key", "title", "artist",
    "album", "cover_item_id", "position",
)


def _new(kind):
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id

    return canonicalize_navidrome_id(OLD[kind]).value


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _publish_old_catalogue(db):
    from test_lumae_analysis import RefreshBridge, _identity_fixture_catalog
    from plugins.LumaeAnalysis import catalog

    published = catalog.refresh_catalog(
        "server-a", db=db,
        bridge=RefreshBridge(_identity_fixture_catalog(OLD["track"], OLD["album"], OLD["artist"])),
    )
    return published["catalog_instance_id"]


def _rekey(db, source, analysis_generation=0):
    """The real provider-ID rekey publication, old ids to canonical ones."""
    from test_lumae_analysis import _identity_fixture_catalog
    from plugins.LumaeAnalysis import catalog
    from plugins.LumaeAnalysis import provider_identity_rekey as rekey

    target = catalog.normalize_provider_catalog(
        _identity_fixture_catalog(_new("track"), _new("album"), _new("artist")), "navidrome")
    target_fp = rekey.target_scan_fingerprint(target)
    with db.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS task_status (status TEXT, task_type TEXT)")
        cur.execute(f"SELECT published_generation FROM {P}catalog_state "
                    "WHERE catalog_instance_id=%s", (source,))
        generation = cur.fetchone()[0]
        cur.execute(
            f"""INSERT INTO {P}provider_identity_transitions
                (catalog_instance_id, transition_id, state, previous_provider_version,
                 current_provider_version, baseline_catalog_generation,
                 baseline_analysis_generation, target_fingerprint, target_scan_count)
                VALUES (%s, 'transition-a', 'transition_pending', '0.63.0', '0.64.0',
                        %s, %s, %s, 2)
                ON CONFLICT (catalog_instance_id) DO UPDATE SET
                    transition_id=EXCLUDED.transition_id, state=EXCLUDED.state,
                    previous_provider_version=EXCLUDED.previous_provider_version,
                    current_provider_version=EXCLUDED.current_provider_version,
                    baseline_catalog_generation=EXCLUDED.baseline_catalog_generation,
                    baseline_analysis_generation=EXCLUDED.baseline_analysis_generation,
                    target_fingerprint=EXCLUDED.target_fingerprint,
                    target_scan_count=EXCLUDED.target_scan_count""",
            (source, generation, analysis_generation, target_fp),
        )
    db.commit()
    result = rekey.publish_provider_identity_rekey(
        db, catalog_instance_id=source, server_id="server-a", normalized=target,
        target_fingerprint=target_fp, current_provider_version="0.64.0",
        adapter=SimpleNamespace(analysis_mapping_sql=lambda: "SELECT 1 WHERE FALSE"),
    )
    assert result["provider_identity_transition"]["state"] == "applied"
    return result


def _seed(api, user, collections):
    """Create ``{collection_id: [item bodies with an "id"]}`` through the API."""
    for collection_id, items in collections.items():
        response = api.call("POST", "/api/collections", {"id": collection_id, "name": collection_id},
                            key=f"create-{collection_id}", user=user)
        assert response.status_code == 201
        for item in items:
            body = {key: value for key, value in item.items() if key != "id"}
            response = api.call("PUT", f"/api/collections/{collection_id}/items/{item['id']}",
                                body, key=f"put-{collection_id}-{item['id']}", user=user)
            assert response.status_code == 200, response.get_json()


def _track(item_id, track_id, position=0):
    return {"id": item_id, "kind": "track", "track_id": track_id, "title": item_id,
            "position": position}


def _album(item_id, album_id):
    return {"id": item_id, "kind": "album", "provider_album_id": album_id, "title": item_id,
            "cover_item_id": album_id}


def _principal(db, collection_id):
    return _rows(db, f"SELECT principal FROM {P}collections WHERE id=%s", (collection_id,))[0][0]


def _history(db):
    changes = _rows(db, f"""SELECT seq, principal, collection_id, entity_kind, entity_id,
                                   operation, payload::text, created_at
                              FROM {P}collection_changes ORDER BY seq""")
    receipts = _rows(db, f"""SELECT principal, idempotency_key, response_payload::text,
                                    status_code, request_fingerprint
                               FROM {P}collection_mutations ORDER BY 1, 2""")
    return changes, receipts


def _head(db):
    return _rows(db, f"SELECT head_seq FROM {P}collection_feed_state")[0][0]


def _revisions(db):
    return dict(((row[0], row[1]), row[2]) for row in _rows(
        db, f"SELECT principal, id, revision FROM {P}collections"))


def _items(db, principal):
    rows = _rows(db, f"""SELECT collection_id, {', '.join(ITEM_FIELDS)}
                           FROM {P}collection_items WHERE principal=%s""", (principal,))
    return {(row[0], row[1]): dict(zip(ITEM_FIELDS, row[1:])) for row in rows}


def test_rekey_emits_one_upsert_per_item_and_never_rewrites_history(collections_api, migrated_db):
    db = migrated_db
    source = _publish_old_catalogue(db)
    _seed(collections_api, "alice", {
        "c1": [_track("i1", OLD["track"]), _album("a1", OLD["album"])],
        "c2": [_track("i2", OLD["track"])],
    })
    alice = _principal(db, "c1")
    before_changes, before_receipts = _history(db)
    assert any(OLD["track"] in row[6] for row in before_changes)
    assert any(OLD["track"] in row[2] for row in before_receipts)
    revisions, head = _revisions(db), _head(db)

    _rekey(db, source)

    after_changes, after_receipts = _history(db)
    # History is immutable: delivered events and receipts are byte-identical.
    assert after_changes[:len(before_changes)] == before_changes
    assert after_receipts == before_receipts
    appended = after_changes[len(before_changes):]
    assert [row[0] for row in appended] == [head + 1, head + 2, head + 3]
    assert _head(db) == head + 3
    assert [row[1:6] for row in appended] == [
        (alice, "c1", "item", "a1", "upsert"),
        (alice, "c1", "item", "i1", "upsert"),
        (alice, "c2", "item", "i2", "upsert"),
    ]
    assert _revisions(db) == {key: value + 1 for key, value in revisions.items()}
    payloads = [json.loads(row[6]) for row in appended]
    items = _items(db, alice)
    for row, payload in zip(appended, payloads):
        assert {name: payload[name] for name in ITEM_FIELDS} == items[(row[2], row[4])]
        assert payload["collection_revision"] == revisions[(alice, row[2])] + 1
    assert payloads[0]["provider_album_id"] == payloads[0]["cover_item_id"] == _new("album")
    assert payloads[1]["track_id"] == payloads[2]["track_id"] == _new("track")
    assert not _rows(db, f"SELECT 1 FROM {P}collection_items WHERE track_id=%s", (OLD["track"],))


def test_a_collision_merges_by_deleting_the_duplicate_with_a_delete_event(
    collections_api, migrated_db
):
    db = migrated_db
    source = _publish_old_catalogue(db)
    # The client already holds the new id in c1: the rekeyed i1 is the duplicate.
    _seed(collections_api, "alice", {
        "c1": [_track("i1", OLD["track"]), _track("i3", _new("track"), position=1)],
    })
    alice = _principal(db, "c1")
    head, revision = _head(db), _revisions(db)[(alice, "c1")]
    kept = _items(db, alice)[("c1", "i3")]

    _rekey(db, source)

    appended = _rows(db, f"""SELECT entity_id, operation, payload FROM {P}collection_changes
                              WHERE seq > %s ORDER BY seq""", (head,))
    updated_at = _rows(db, f"SELECT updated_at FROM {P}collections WHERE id='c1'")[0][0]
    assert appended == [("i1", "delete", {
        "id": "i1", "collection_id": "c1", "collection_revision": revision + 1,
        "collection_updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    })]
    assert _items(db, alice) == {("c1", "i3"): kept}
    assert _revisions(db)[(alice, "c1")] == revision + 1


def test_an_unresolvable_collision_defers_only_that_principal(collections_api, migrated_db):
    db = migrated_db
    source = _publish_old_catalogue(db)
    _seed(collections_api, "alice", {"ca": []})
    _seed(collections_api, "bob", {"cb": [_track("i1", OLD["track"])]})
    alice, bob = _principal(db, "ca"), _principal(db, "cb")
    with db.cursor() as cur:
        # Two items of one collection would converge on the new id with no
        # item already holding it: the merge rule cannot choose a survivor.
        cur.execute("DROP INDEX lumae_collection_track_scoped_unique_idx")
        for item_id in ("i1", "i2"):
            cur.execute(f"""INSERT INTO {P}collection_items
                                (principal, id, collection_id, kind, track_id)
                            VALUES (%s, %s, 'ca', 'track', %s)""",
                        (alice, item_id, OLD["track"]))
    db.commit()
    alice_items, revisions, head = _items(db, alice), _revisions(db), _head(db)

    _rekey(db, source)

    deferrals = _rows(db, f"SELECT collection_deferrals FROM {P}provider_identity_transitions")
    assert deferrals == [([{
        "principal": alice, "reason": "unresolved_membership_collision",
        "transition_id": "transition-a",
        "collisions": [{"collection_id": "ca", "kind": "track", "provider_id": _new("track"),
                        "item_ids": ["i1", "i2"]}],
    }],)]
    assert _items(db, alice) == alice_items
    assert _revisions(db)[(alice, "ca")] == revisions[(alice, "ca")]
    assert _items(db, bob)[("cb", "i1")]["track_id"] == _new("track")
    assert _revisions(db)[(bob, "cb")] == revisions[(bob, "cb")] + 1
    assert _rows(db, f"""SELECT principal, entity_id, operation FROM {P}collection_changes
                          WHERE seq > %s ORDER BY seq""", (head,)) == [(bob, "i1", "upsert")]


def test_a_rekey_touches_only_its_catalogue_and_unscoped_items(collections_api, migrated_db):
    """K10 (P3-5b): two catalogues hold the same old ids. The rekey of one
    rewrites its items and unscoped (NULL) ones, never the other's."""
    db = migrated_db
    source = _publish_old_catalogue(db)

    def scoped(item, catalog):
        return {**item, "catalog_instance_id": catalog}

    _seed(collections_api, "alice", {
        "c1": [scoped(_track("mine", OLD["track"]), source),
               scoped({**_track("theirs", OLD["track"], position=1),
                       "cover_item_id": OLD["track"]}, "catalog-other"),
               scoped(_track("legacy", OLD["track"], position=2), "unscoped-below")],
        "c2": [scoped(_album("their-album", OLD["album"]), "catalog-other")],
    })
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}collection_items SET catalog_instance_id=NULL WHERE id='legacy'")
    db.commit()
    alice = _principal(db, "c1")
    before, head, revisions = _items(db, alice), _head(db), _revisions(db)

    _rekey(db, source)

    after = _items(db, alice)
    assert after[("c1", "theirs")] == before[("c1", "theirs")]
    assert after[("c2", "their-album")] == before[("c2", "their-album")]
    assert after[("c1", "mine")]["track_id"] == after[("c1", "legacy")]["track_id"] == _new("track")
    appended = _rows(db, f"""SELECT entity_id, operation, payload FROM {P}collection_changes
                              WHERE seq > %s ORDER BY seq""", (head,))
    # The same new id in two scopes (source and NULL) is no collision (K10).
    assert [(row[0], row[1], row[2]["catalog_instance_id"]) for row in appended] == [
        ("legacy", "upsert", None), ("mine", "upsert", source)]
    assert _revisions(db) == {**revisions, (alice, "c1"): revisions[(alice, "c1")] + 1}
    assert _rows(db, f"SELECT collection_deferrals FROM {P}provider_identity_transitions") == [
        ([],)]


def test_a_tombstoned_collection_is_rekeyed_without_events_or_revision(
    collections_api, migrated_db
):
    db = migrated_db
    api = collections_api
    source = _publish_old_catalogue(db)
    _seed(api, "alice", {"gone": [_track("i1", OLD["track"])], "live": [_track("i2", OLD["track"])]})
    assert api.call("DELETE", "/api/collections/gone", key="delete-gone").status_code == 200
    alice = _principal(db, "gone")
    revisions, head = _revisions(db), _head(db)

    _rekey(db, source)

    items = _items(db, alice)
    assert items[("gone", "i1")]["track_id"] == items[("live", "i2")]["track_id"] == _new("track")
    assert _revisions(db) == {**revisions, (alice, "live"): revisions[(alice, "live")] + 1}
    assert _rows(db, f"""SELECT collection_id, entity_id, operation FROM {P}collection_changes
                          WHERE seq > %s ORDER BY seq""", (head,)) == [("live", "i2", "upsert")]


def test_a_restore_chunk_and_a_rekey_lock_collections_in_one_order(collections_api, migrated_db):
    """A chunk spanning {b, a} and a rekey of {a, b} both finish: no deadlock.

    A third transaction holds ``a`` so that the rekey queues on it first; the
    chunk then queues behind it. Locking one collection at a time in chunk
    order, the chunk would hold ``b`` while the rekey, next to get ``a``,
    waits for ``b``.
    """
    import threading
    import time

    import psycopg2

    manager = collections_api.manager
    db = migrated_db
    with db.cursor() as cur:
        for collection_id in ("a", "b"):
            cur.execute(f"INSERT INTO {P}collections (principal, id, name) VALUES ('alice', %s, %s)",
                        (collection_id, collection_id))
            cur.execute(f"""INSERT INTO {P}collection_items (principal, id, collection_id, kind,
                                                             track_id)
                            VALUES ('alice', %s, %s, 'track', %s)""",
                        (f"old-{collection_id}", collection_id, OLD["track"]))
    db.commit()
    holder, rekeyer, restorer = (collections_api.connect() for _ in range(3))
    with holder.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {P}collections WHERE principal='alice' AND id='a' FOR UPDATE")
    chunk = [
        {"id": collection_id, "create": False, "items": [manager._normalize_item(
            {"id": f"restored-{collection_id}", "kind": "track", "track_id": f"t-{collection_id}"})]}
        for collection_id in ("b", "a")
    ]
    errors = []

    def run(connection, work):
        try:
            with connection.cursor() as cur:
                cur.execute("SET lock_timeout = '10s'")
                work(cur)
            connection.commit()
        except psycopg2.Error as error:
            connection.rollback()
            errors.append(type(error).__name__)

    def rekey(cur):
        changes, _ = manager.rekey_collection_items(
            cur, "catalog-a", {OLD["track"]: _new("track")}, {}, {OLD["track"]: _new("track")})
        manager._record_changes(cur, changes)

    def waiting(connection):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if _rows(db, "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s",
                     (connection.get_backend_pid(),)) == [("Lock",)]:
                return
            time.sleep(0.02)
        raise AssertionError("the transaction never queued on the lock")

    threads = [
        threading.Thread(target=run, args=(rekeyer, rekey)),
        threading.Thread(target=run, args=(restorer, lambda cur: manager._restore_principal_collections(
            cur, "alice", chunk))),
    ]
    for thread, connection in zip(threads, (rekeyer, restorer)):
        thread.start()
        waiting(connection)
    holder.rollback()
    for thread in threads:
        thread.join(30)
    assert errors == []
    assert sorted(_items(db, "alice")) == [
        ("a", "old-a"), ("a", "restored-a"), ("b", "old-b"), ("b", "restored-b")]
    assert _rows(db, f"SELECT DISTINCT track_id FROM {P}collection_items WHERE id LIKE 'old-%%'") == [
        (_new("track"),)]


def _sync(api, state, cursor=0, epoch=""):
    """Page the feed like a client, applying every event to ``state``."""
    while True:
        response = api.call(
            "GET", f"/api/collections/changes?cursor={cursor}&limit=2&epoch={epoch}")
        assert response.status_code == 200
        page = response.get_json()
        for change in page["changes"]:
            payload = change["payload"]
            if change["entity_kind"] == "item":
                key = (change["collection_id"], change["entity_id"])
                if change["operation"] == "delete":
                    state["items"].pop(key, None)
                else:
                    state["items"][key] = {name: payload[name] for name in ITEM_FIELDS}
                state["revisions"][change["collection_id"]] = payload["collection_revision"]
            elif "revision" in payload:
                state["revisions"][change["collection_id"]] = payload["revision"]
        cursor, epoch = page["next_cursor"], page["epoch"]
        if not page["has_more"]:
            return cursor, epoch


def test_the_feed_replays_the_rekey_to_the_server_state(collections_api, migrated_db):
    db = migrated_db
    api = collections_api
    source = _publish_old_catalogue(db)
    _seed(api, "alice", {
        "c1": [_track("i1", OLD["track"]), _track("i3", _new("track"), position=1),
               _album("a1", OLD["album"])],
        "c2": [_track("i2", OLD["track"])],
    })
    alice = _principal(db, "c1")
    synced = {"items": {}, "revisions": {}}
    cursor, epoch = _sync(api, synced)

    _rekey(db, source)

    _sync(api, synced, cursor, epoch)
    fresh = {"items": {}, "revisions": {}}
    _sync(api, fresh)
    server = {
        "items": _items(db, alice),
        "revisions": {key[1]: value for key, value in _revisions(db).items() if key[0] == alice},
    }
    assert ("c1", "i1") not in server["items"]
    assert server["items"][("c2", "i2")]["track_id"] == _new("track")
    assert synced == server
    assert fresh == server


def test_the_rekey_copy_analyzes_the_new_generation_key_columns(migrated_db):
    db = migrated_db
    source = _publish_old_catalogue(db)
    with db.cursor() as cur:
        cur.execute(f"""INSERT INTO {P}analysis_items
                            (catalog_instance_id, projection_generation, analysis_id)
                        VALUES (%s, 1, 'analysis-1')""", (source,))
        cur.execute(f"""INSERT INTO {P}track_analysis_links
                            (catalog_instance_id, projection_generation, provider_track_id,
                             analysis_id, status)
                        VALUES (%s, 1, %s, 'analysis-1', 'ready')""", (source, OLD["track"]))
        cur.execute(f"""UPDATE {P}analysis_state
                           SET projection_generation=1, item_count=1, mapped_track_count=1,
                               status='complete'
                         WHERE catalog_instance_id=%s""", (source,))
    db.commit()
    stats_sql = """SELECT tablename, attname, COALESCE(histogram_bounds::text, '')
                               || COALESCE(most_common_vals::text, '')
                     FROM pg_stats
                    WHERE schemaname=current_schema() AND tablename=ANY(%s)"""
    tables = [f"{P}analysis_items", f"{P}track_analysis_links"]
    assert _rows(db, stats_sql, (tables,)) == []

    _rekey(db, source, analysis_generation=1)

    stats = {(row[0], row[1]): row[2] for row in _rows(db, stats_sql, (tables,))}
    for column in ("catalog_instance_id", "projection_generation", "analysis_id"):
        assert (f"{P}analysis_items", column) in stats
    for column in ("catalog_instance_id", "projection_generation", "provider_track_id",
                   "analysis_id", "status"):
        assert (f"{P}track_analysis_links", column) in stats
    assert _new("track") in stats[(f"{P}track_analysis_links", "provider_track_id")]
    assert _rows(db, f"""SELECT provider_track_id FROM {P}track_analysis_links
                          WHERE projection_generation=2""") == [(_new("track"),)]


def test_a_rekey_without_collection_items_records_no_events(migrated_db):
    db = migrated_db
    source = _publish_old_catalogue(db)
    head = _head(db)
    _rekey(db, source)
    assert _head(db) == head
    assert _rows(db, f"SELECT collection_deferrals FROM {P}provider_identity_transitions") == [
        ([],)]
