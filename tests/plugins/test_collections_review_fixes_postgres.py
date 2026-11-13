"""Review fixes for the per-user sync features, against PostgreSQL.

Malformed collection input, the membership probe and its catalogue lookup,
credits priority, discovery receipt retention, the shelf purge, journey
rekeying and page size, and the mirrored Navidrome cover id.
"""
import importlib
import json
import math

import pytest

import test_journeys as tj
import test_shelves as ts
import test_vibes as tv
from test_collection_feed_epoch_postgres import _checksum, collections_api  # noqa: F401 (fixture)
from test_credits import credits_db, payload  # noqa: F401 (fixture)
from test_journeys import journeys  # noqa: F401 (fixture)
from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401 (fixture)
from test_shelves import shelves  # noqa: F401 (fixture)
from test_vibes import vibes  # noqa: F401 (fixture)


def _module(name):
    load_plugin()
    return importlib.import_module(f"plugins.LumaeAnalysis.{name}")


def _q(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else None
    db.commit()
    return rows


class _Recorder:
    """A cursor that records each statement and the rows it returned."""

    def __init__(self, cur):
        self.cur, self.statements, self.fetched = cur, [], []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        return self.cur.execute(sql, params)

    def fetchall(self):
        rows = self.cur.fetchall()
        self.fetched.extend(rows)
        return rows

    def fetchone(self):
        row = self.cur.fetchone()
        if row is not None:
            self.fetched.append(row)
        return row

    def __getattr__(self, name):
        return getattr(self.cur, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.cur.close()


# ------------------------------------------------------------------ 10. malformed input


def test_malformed_collection_requests_are_400(collections_api):
    call = collections_api.call
    assert call("POST", "/api/collections", [1, 2]).status_code == 400
    assert call("POST", "/api/collections", {"id": "c1", "name": "One"}).status_code == 201
    batch = "/api/collections/c1/items/batch"
    track = {"kind": "track", "track_id": "t1"}
    for body in ([1], {"items": [1]}, {"items": [{**track, "position": math.inf}]},
                 {"items": [{**track, "position": 2**31}]}):
        response = call("POST", batch, body)
        assert response.status_code == 400, body
        assert "error" in response.get_json()
    assert call("PUT", "/api/collections/c1/items/i1", "text").status_code == 400
    assert call("PATCH", "/api/collections/c1", [1]).status_code == 400
    assert call("DELETE", "/api/collections/c1/items/batch", [1]).status_code == 400
    collections = [{"name": "R", "items": [{**track, "position": math.inf}]}]
    restore = {"format": "lumae-living-collections", "version": 1, "collections": collections,
               "checksum": _checksum(collections)}
    assert call("POST", "/api/collections/restore", restore).status_code == 400
    # A valid write still lands.
    assert call("POST", batch, {"items": [{**track, "position": 2**31 - 1}]}).status_code == 200


def test_non_finite_shelf_and_vibe_bodies_are_400(shelves, vibes):
    shelf = ts.add("a")
    shelf["member"]["note"] = math.nan
    assert ts.post(shelves, shelf).status_code == 400
    vibe = tv.put("m", vibe=tv.palette(recipe={"weight": math.inf}))
    assert tv.post(vibes, vibe).status_code == 400
    assert tv.post(vibes, tv.put("ok")).status_code == 200


# ------------------------------------------------------------------ 4. membership probe


def test_a_batch_resolves_the_sole_catalogue_once(collections_api, monkeypatch):
    manager = collections_api.manager
    original = manager.sole_catalog_sql
    calls = []
    monkeypatch.setattr(manager, "sole_catalog_sql", lambda: calls.append(1) or original())
    call = collections_api.call
    assert call("POST", "/api/collections", {"id": "c1", "name": "One"}).status_code == 201
    items = [{"kind": "track", "track_id": f"t{i}"} for i in range(5)]
    assert call("POST", "/api/collections/c1/items/batch", {"items": items}).status_code == 200
    assert len(calls) == 1


def test_the_membership_probe_is_an_index_lookup(collections_api):
    manager = collections_api.manager
    call = collections_api.call
    assert call("POST", "/api/collections", {"id": "c1", "name": "One"}).status_code == 201
    items = [{"kind": "track", "track_id": f"t{i}"} for i in range(50)]
    assert call("POST", "/api/collections/c1/items/batch", {"items": items}).status_code == 200
    db = collections_api.connect()
    with db.cursor() as raw:
        raw.execute(f"SELECT principal FROM {manager.collections_table()} WHERE id='c1'")
        [principal] = raw.fetchone()
        cur = _Recorder(raw)
        item = manager._normalize_item({"kind": "track", "track_id": "t7", "catalog_instance_id": "cat"})
        manager._upsert_item(cur, principal, "c1", item)
        sql, params = next((sql, params) for sql, params in cur.statements
                           if "track_id = %s" in sql and sql.lstrip().startswith("SELECT"))
        raw.execute("SET LOCAL enable_seqscan = off")
        raw.execute("EXPLAIN (FORMAT JSON) " + sql, params)
        plan = raw.fetchone()[0][0]["Plan"]
    db.rollback()

    def nodes(node):
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    [scan] = [node for node in nodes(plan) if node.get("Index Name")]
    assert scan["Index Name"] == "lumae_collection_track_scoped_unique_idx"
    # Every key of the index is a lookup condition, none a filter over its entries.
    condition = scan["Index Cond"]
    assert "COALESCE" in condition and "track_id" in condition, condition
    assert "Filter" not in scan, scan.get("Filter")


# ------------------------------------------------------------------ 5. credits priority


JOBS = "plugin_lumae_analysis__credits_jobs"


def _job(db, name):
    [row] = _q(db, f"SELECT xmin::text, status, priority, rechecks FROM {JOBS} "
                   "WHERE catalog_instance_id='c' AND album_id=%s", (name,))
    return row


def test_preparing_a_current_album_leaves_no_priority_behind(credits_db):
    db = credits_db
    store = _module("credits_store")
    store.request_album(db, "c", "a")
    job = store.claim(db, "c")
    assert store.finish(db, job, payload(db, "a"))
    before = _job(db, "a")
    assert before[2] == 0
    store.request_album(db, "c", "a", priority=10)  # the phone asks: already current
    assert _job(db, "a") == before  # not even rewritten
    _q(db, f"UPDATE {JOBS} SET expires_at=now()-interval '1 second' WHERE album_id='a'")
    store.request_album(db, "c", "a")  # the sweep finds it expired: a background re-check
    _, status, priority, rechecks = _job(db, "a")
    assert (status, priority, rechecks) == ("pending", 0, 1)
    assert store.claim(db, "c", rechecks=False) is None


def test_an_expiry_requeue_drops_a_stale_priority(credits_db):
    db = credits_db
    store = _module("credits_store")
    store.request_album(db, "c", "a")
    assert store.finish(db, store.claim(db, "c"), payload(db, "a"))
    # A row left at priority 10 by the earlier behaviour.
    _q(db, f"UPDATE {JOBS} SET priority=10, expires_at=now()-interval '1 second' WHERE album_id='a'")
    store.request_album(db, "c", "a")
    assert _job(db, "a")[1:3] == ("pending", 0)
    # An app request for a pending album still raises it.
    store.request_album(db, "c", "a", priority=10)
    assert _job(db, "a")[1:3] == ("pending", 10)


# ------------------------------------------------------------------ 6. discovery receipts


def test_discovery_receipts_are_dated_and_expire_after_30_days(lumae_postgres_db):
    db = lumae_postgres_db
    discovery = _module("personal_discovery")
    discovery.migrate(db)
    discovery.migrate(db)  # idempotent
    db.commit()
    receipts = discovery.table("discovery_receipts")
    for identifier in ("old", "new"):
        _q(db, f"INSERT INTO {receipts}(principal,id,fingerprint,payload,status) "
               "VALUES('alice',%s,'f','{}'::jsonb,200)", (identifier,))
    assert _q(db, f"SELECT count(*) FROM {receipts} WHERE created_at IS NULL") == [(0,)]
    _q(db, f"UPDATE {receipts} SET created_at=now()-interval '31 days' WHERE id='old'")
    with db.cursor() as cur:
        assert discovery.purge_expired_discovery_receipts(cur) == 1
    db.commit()
    assert _q(db, f"SELECT id FROM {receipts}") == [("new",)]


# ------------------------------------------------------------------ 7. shelf purge


def test_the_shelf_purge_delete_repeats_the_age_filter(shelves, lumae_postgres_db):
    mod = ts.shelf_module()
    assert ts.post(shelves, ts.add("old")).status_code == 200
    assert ts.post(shelves, ts.add("new")).status_code == 200
    relation = mod.table("shelf_mutations")
    _q(lumae_postgres_db, f"UPDATE {relation} SET created_at=now()-interval '31 days' WHERE id='add-old'")
    recorders = []

    class Db:
        def cursor(self):
            recorders.append(_Recorder(lumae_postgres_db.cursor()))
            return recorders[-1]

        def commit(self):
            lumae_postgres_db.commit()

    assert mod.purge_expired_shelf_mutations(Db()) == 1
    deletes = [sql for recorder in recorders for sql, _ in recorder.statements if sql.startswith("DELETE")]
    assert deletes and all("created_at < now()" in sql for sql in deletes)
    assert _q(lumae_postgres_db, f"SELECT id FROM {relation}") == [("add-new",)]


# ------------------------------------------------------------------ 8. journeys


def test_rekey_loads_only_the_journeys_and_receipts_that_name_an_old_id(journeys, lumae_postgres_db):
    mod = tj.journey_module()
    named = tj.journey(entries=[tj.entry("e1", track="old-id"), tj.entry("e2")])
    other = tj.journey(name="Elsewhere", jid="jny_other",
                       entries=[tj.entry("e1", track="unrelated-track")])
    assert tj.post(journeys, tj.upsert("a", body=named)).status_code == 200
    assert tj.post(journeys, tj.upsert("b", body=other, jid="jny_other"), user="bob").status_code == 200
    # An old id used as an object key is rewritten too.
    keyed = tj.journey(jid="jny_keyed")
    keyed["builtFrom"]["sameYearOrder"] = [{"old-id": 1}]
    assert tj.post(journeys, tj.upsert("k", body=keyed, jid="jny_keyed")).status_code == 200
    with lumae_postgres_db.cursor() as raw:
        cur = _Recorder(raw)
        mod.rekey_journeys(cur, "catalog-a", {"old-id": "new-id"})
    lumae_postgres_db.commit()
    assert "unrelated-track" not in json.dumps([list(row) for row in cur.fetched], default=str)
    records = {r["journeyId"]: r for r in tj.changes(journeys)["records"]}
    assert records["jny_g"]["journey"]["entries"][0]["trackId"] == "new-id"
    assert records["jny_keyed"]["journey"]["builtFrom"]["sameYearOrder"] == [{"new-id": 1}]
    replay = tj.post(journeys, tj.upsert("a", body=named)).get_json()
    assert replay["record"]["journey"]["entries"][0]["trackId"] == "new-id"
    assert tj.changes(journeys, user="bob")["records"][0]["journey"]["entries"][0]["trackId"] == "unrelated-track"


def test_a_changes_page_is_capped_by_bytes_and_continues(journeys, monkeypatch):
    mod = tj.journey_module()
    for index in range(4):
        jid = f"jny_{index}"
        assert tj.post(journeys, tj.upsert(f"m{index}", body=tj.journey(jid=jid), jid=jid)).status_code == 200
    whole = tj.changes(journeys)
    assert len(whole["records"]) == 4 and not whole["hasMore"]
    size = len(json.dumps(whole["records"][0]["journey"]))
    monkeypatch.setattr(mod, "MAX_CHANGES_PAGE_BYTES", int(size * 2.5))
    seen, cursor, pages = [], 0, 0
    while True:
        page = tj.changes(journeys, cursor=cursor)
        pages += 1
        assert 1 <= len(page["records"]) <= 2
        seen += [record["journeyId"] for record in page["records"]]
        cursor = page["cursor"]
        if not page["hasMore"]:
            break
    assert seen == [record["journeyId"] for record in whole["records"]] and pages == 2
    # One record larger than the cap still comes, alone.
    monkeypatch.setattr(mod, "MAX_CHANGES_PAGE_BYTES", 1)
    page = tj.changes(journeys)
    assert len(page["records"]) == 1 and page["hasMore"]
    # The count limit still applies under the cap.
    monkeypatch.setattr(mod, "MAX_CHANGES_PAGE_BYTES", 10**9)
    page = tj.changes(journeys, limit=3)
    assert len(page["records"]) == 3 and page["hasMore"]
    assert tj.changes(journeys, cursor=whole["cursor"]) == {"records": [], "cursor": whole["cursor"],
                                                            "hasMore": False}


# ------------------------------------------------------------------ 9. mirrored cover id


def test_the_mirrored_cover_art_id_is_read_from_the_published_generation(credits_db, monkeypatch):
    library = _module("collection_library")
    monkeypatch.setattr(library, "get_db", lambda: credits_db)
    _q(credits_db, "UPDATE plugin_lumae_analysis__catalog_tracks SET cover_art_id='al-a' "
                   "WHERE catalog_instance_id='c' AND track_id='a0'")
    assert library._mirror_cover_art_id("c", "a0") == "al-a"
    assert library._mirror_cover_art_id("c", "a1") is None
    assert library._mirror_cover_art_id("c", "missing") is None
    assert library._mirror_cover_art_id(None, "a0") is None
