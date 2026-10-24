"""Journey sync (Lumae JN.10): contract and PostgreSQL transport tests in disposable schemas."""
import copy
import importlib
import json

import pytest
from flask import Flask, g
from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401


def journey_module():
    load_plugin()
    return importlib.import_module("plugins.LumaeAnalysis.journeys")


def entry(entry_id, state="pending", at=None, by=None, track=None):
    return {"entryId": entry_id, "position": int(entry_id[1:]), "trackId": track or f"track-{entry_id}",
            "chapterId": "c1", "chapterTitle": "Duke", "chapterKind": "album", "chapterAlbumId": "album-duke",
            "chapterSource": "artist:genesis", "title": f"Song {entry_id}", "artist": "Genesis", "album": "Duke",
            "durationSeconds": 240, "year": 1980, "state": state, "stateBy": by, "stateAt": at}


def journey(name="Genesis", entries=None, place=None, last_played=None, status="in-progress", jid="jny_g"):
    return {"version": 1, "id": jid, "name": name, "status": status,
            "builtFrom": {"version": 1, "sources": [{"kind": "artist", "artistId": "artist-genesis"}],
                          "sort": "release-asc", "sameYearOrder": []},
            "place": place or {"entryId": None, "positionSeconds": 0},
            "options": {"keepNextChapterOffline": False},
            "createdAt": 1_761_000_000_000, "updatedAt": 1_761_000_000_000,
            "lastPlayedAt": last_played, "finishedAt": None,
            "entries": entries if entries is not None else [entry("e1"), entry("e2"), entry("e3")]}


def credit(occurrence, entry_id, outcome="ended", heard=240):
    return {"occurrenceId": occurrence, "entryId": entry_id, "outcome": outcome, "heardSeconds": heard}


def upsert(mutation_id, base=None, body=None, credits=(), jid="jny_g"):
    return {"id": mutation_id, "journeyId": jid, "operation": "upsert", "baseRevision": base,
            "journey": body if body is not None else journey(jid=jid), "credits": list(credits)}


def delete(mutation_id, base, at=1_761_100_000_000, jid="jny_g"):
    return {"id": mutation_id, "journeyId": jid, "operation": "delete", "baseRevision": base, "at": at}


@pytest.fixture
def journeys(lumae_postgres_db, monkeypatch):
    mod = journey_module()
    mod.migrate_journeys(lumae_postgres_db)
    lumae_postgres_db.commit()
    monkeypatch.setattr(mod, "get_db", lambda: lumae_postgres_db)
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    monkeypatch.setattr(manager, "collections_enabled", lambda: True)
    app = Flask(__name__)
    app.register_blueprint(load_plugin().bp)

    @app.before_request
    def auth():
        from flask import request
        g.auth_method = request.headers.get("Test-Auth", "session")
        g.auth_user = request.headers.get("Test-User", "alice")

    return app.test_client()


def post(client, body, catalog="catalog-a", user="alice"):
    return client.post(f"/api/journeys/mutations?catalog_id={catalog}", json=body, headers={"Test-User": user})


def changes(client, catalog="catalog-a", user="alice", cursor=0, limit=100):
    return client.get(f"/api/journeys/changes?catalog_id={catalog}&cursor={cursor}&limit={limit}",
                      headers={"Test-User": user}).get_json()


def states(record):
    return {e["entryId"]: e["state"] for e in record["journey"]["entries"]}


def test_validation():
    mod = journey_module()
    validate = mod.validate_mutation
    assert validate(upsert("m"))["operation"] == "upsert"
    assert validate(upsert("m", base=3, credits=[credit("o1", "e1")]))["baseRevision"] == 3
    assert validate(delete("m", 2))["operation"] == "delete"
    big = journey(entries=[{**entry(f"e{n}"), "title": "x" * 1000} for n in range(1, 1600)])
    with_entry = lambda **kw: journey(entries=[{**entry("e1"), **kw}])  # noqa: E731
    bad_bodies = [
        None, {}, upsert(""), upsert("m", jid=""), {**upsert("m"), "baseRevision": 0},
        {**upsert("m"), "baseRevision": True}, {**upsert("m"), "baseRevision": 1.0},
        {k: v for k, v in upsert("m").items() if k != "baseRevision"},
        {**upsert("m"), "operation": "patch"}, upsert("m", body=journey(jid="jny_other")),
        upsert("m", body={**journey(), "version": 2}), upsert("m", body={**journey(), "status": "paused"}),
        upsert("m", body={**journey(), "place": {"entryId": None}}),
        upsert("m", body={**journey(), "lastPlayedAt": -1}), upsert("m", body={**journey(), "entries": {}}),
        upsert("m", body=journey(entries=[entry("e1"), entry("e1")])),
        upsert("m", body=with_entry(state="skipped")), upsert("m", body=with_entry(stateAt="today")),
        upsert("m", body={**journey(), "options": {"x": float("nan")}}),
        upsert("m", credits=[credit("o1", "e1"), credit("o1", "e2")]),
        upsert("m", credits=[credit("o1", "e1", outcome="liked")]),
        upsert("m", credits=[credit("o1", "e1", heard=float("inf"))]),
        {**upsert("m"), "credits": None}, upsert("m", body=big),
        {**delete("m", 1), "at": float("nan")}, {k: v for k, v in delete("m", 1).items() if k != "at"},
    ]
    for body in bad_bodies:
        with pytest.raises(ValueError):
            validate(body)


def test_revisions_conflicts_tombstones_and_resurrection(journeys):
    created = post(journeys, upsert("create"))
    assert created.status_code == 200
    record = created.get_json()["record"]
    assert record == {"journeyId": "jny_g", "revision": 1, "deleted": False, "journey": journey(), "credits": []}

    stale = post(journeys, upsert("other-phone", body=journey("Genesis again")))
    assert stale.status_code == 409
    assert stale.get_json() == {"error": "journey_conflict", "record": record}

    renamed = post(journeys, upsert("rename", base=1, body=journey("Genesis, in order"))).get_json()["record"]
    assert renamed["revision"] == 2 and renamed["journey"]["name"] == "Genesis, in order"

    removed = post(journeys, delete("delete", base=2)).get_json()["record"]
    assert removed == {"journeyId": "jny_g", "revision": 3, "deleted": True, "journey": None, "credits": []}

    # A phone that played it meanwhile writes over the tombstone: the journey comes back.
    back = post(journeys, upsert("played-apart", base=3, credits=[credit("o1", "e1")])).get_json()["record"]
    assert back["revision"] == 4 and back["deleted"] is False and back["credits"] == [credit("o1", "e1")]

    missing = post(journeys, upsert("phantom", base=2, jid="jny_other"))
    assert missing.status_code == 409
    assert missing.get_json() == {"error": "journey_conflict", "record": None}


def test_credits_only_grow_and_a_tombstone_drops_them(journeys):
    post(journeys, upsert("a", credits=[credit("o1", "e1"), credit("o2", "e2", "skipped", 12)]))
    merged = post(journeys, upsert("b", base=1, credits=[credit("o3", "e3"), credit("o1", "e1", heard=1)])).get_json()["record"]
    assert merged["credits"] == [credit("o1", "e1"), credit("o2", "e2", "skipped", 12), credit("o3", "e3")]
    # A write that sends no credits keeps them all.
    assert len(post(journeys, upsert("c", base=2)).get_json()["record"]["credits"]) == 3
    post(journeys, delete("d", base=3))
    assert post(journeys, upsert("e", base=4)).get_json()["record"]["credits"] == []


def test_the_state_changed_last_wins_per_entry(journeys):
    # Phone A plays e1 at t=10 and marks e3 played at t=30.
    post(journeys, upsert("a", body=journey(entries=[entry("e1", "played", 10, "journey"), entry("e2"),
                                                     entry("e3", "played", 30, "you")])))
    # Phone B, which never saw A's plays, played e2 at t=20 and marked e3 not played at t=40.
    b = journey(entries=[entry("e1"), entry("e2", "played", 20, "journey"), entry("e3", "pending", 40)])
    merged = post(journeys, upsert("b", base=1, body=b)).get_json()["record"]
    assert states(merged) == {"e1": "played", "e2": "played", "e3": "pending"}
    assert merged["journey"]["entries"][0] == entry("e1", "played", 10, "journey")
    # An older change never overwrites a newer one: A's stale copy marks e3 played at t=30 again.
    again = journey(entries=[entry("e1", "played", 10, "journey"), entry("e2"), entry("e3", "played", 30, "you")])
    assert states(post(journeys, upsert("a2", base=2, body=again)).get_json()["record"]) == \
        {"e1": "played", "e2": "played", "e3": "pending"}
    # Entries follow the incoming body: one it removed stays removed, one it added is kept.
    grown = journey(entries=[entry("e2"), entry("e4")])
    assert states(post(journeys, upsert("a3", base=3, body=grown)).get_json()["record"]) == \
        {"e2": "played", "e4": "pending"}


def test_the_place_played_last_is_kept(journeys):
    post(journeys, upsert("a", body=journey(place={"entryId": "e3", "positionSeconds": 61.5}, last_played=2_000)))
    older = journey("Renamed", place={"entryId": "e1", "positionSeconds": 5}, last_played=1_000)
    merged = post(journeys, upsert("b", base=1, body=older)).get_json()["record"]["journey"]
    assert merged["name"] == "Renamed"
    assert merged["place"] == {"entryId": "e3", "positionSeconds": 61.5} and merged["lastPlayedAt"] == 2_000
    newer = journey(place={"entryId": "e2", "positionSeconds": 0}, last_played=3_000)
    merged = post(journeys, upsert("c", base=2, body=newer)).get_json()["record"]["journey"]
    assert merged["place"] == {"entryId": "e2", "positionSeconds": 0} and merged["lastPlayedAt"] == 3_000
    # A stored place on a song the incoming body removed is not kept.
    post(journeys, upsert("d", base=3, body=journey(place={"entryId": "e3", "positionSeconds": 9}, last_played=9_000)))
    without = journey(entries=[entry("e1"), entry("e2")], place={"entryId": "e1", "positionSeconds": 0}, last_played=4_000)
    merged = post(journeys, upsert("e", base=4, body=without)).get_json()["record"]["journey"]
    assert merged["place"] == {"entryId": "e1", "positionSeconds": 0} and merged["lastPlayedAt"] == 4_000


def test_receipts_replay_and_bind_their_body(journeys):
    first = post(journeys, upsert("create"))
    replay = post(journeys, upsert("create"))
    assert replay.status_code == 200 and replay.get_json() == first.get_json()
    assert [r["revision"] for r in changes(journeys)["records"]] == [1]

    reused = post(journeys, upsert("create", body=journey("Different")))
    assert reused.status_code == 409
    assert reused.get_json() == {"error": "idempotency_key_conflict"}

    # A refused write stores no receipt: the phone's retry over the revision applies.
    assert post(journeys, upsert("late")).status_code == 409
    assert post(journeys, upsert("late:1", base=1)).status_code == 200
    # A delete carries the time it was queued, so its retry is byte-identical.
    assert post(journeys, delete("gone", 2)).get_json() == post(journeys, delete("gone", 2)).get_json()


def test_records_are_scoped_by_principal_and_catalogue(journeys):
    post(journeys, upsert("a"))
    assert len(changes(journeys)["records"]) == 1
    assert changes(journeys, user="bob")["records"] == []
    assert changes(journeys, catalog="catalog-b")["records"] == []
    shared = journeys.post("/api/journeys/mutations?catalog_id=catalog-a", json=upsert("a"),
                           headers={"Test-Auth": "bearer"})
    assert shared.status_code == 200 and shared.get_json()["record"]["revision"] == 1
    shared_feed = journeys.get("/api/journeys/changes?catalog_id=catalog-a", headers={"Test-Auth": "bearer"}).get_json()
    assert len(shared_feed["records"]) == 1


def test_feed_is_compact_and_pages_by_cursor(journeys):
    post(journeys, upsert("one", jid="jny_one"))
    post(journeys, upsert("two", jid="jny_two"))
    post(journeys, upsert("one-again", base=1, jid="jny_one", body=journey("One again", jid="jny_one")))
    feed = changes(journeys)
    assert [r["journeyId"] for r in feed["records"]] == ["jny_two", "jny_one"]
    assert feed["records"][1]["revision"] == 2 and feed["hasMore"] is False
    page1 = changes(journeys, limit=1)
    assert page1["hasMore"] is True and [r["journeyId"] for r in page1["records"]] == ["jny_two"]
    page2 = changes(journeys, cursor=page1["cursor"], limit=1)
    assert page2["hasMore"] is False and [r["journeyId"] for r in page2["records"]] == ["jny_one"]
    assert changes(journeys, cursor=page2["cursor"]) == {"records": [], "cursor": page2["cursor"], "hasMore": False}
    post(journeys, delete("drop", base=1, jid="jny_two"))
    tail = changes(journeys, cursor=page2["cursor"])["records"]
    assert tail == [{"journeyId": "jny_two", "revision": 2, "deleted": True, "journey": None, "credits": []}]


def test_journeys_are_stored_as_sent(journeys):
    body = journey("夜 ✨ Peter Gabriel")
    body["builtFrom"]["futureField"] = {"nested": [1, 2.5, None]}
    body["options"]["offerRatingAfterAlbum"] = True
    body["extra"] = "kept"
    post(journeys, upsert("unicode", body=body))
    assert changes(journeys)["records"][0]["journey"] == body


def test_disabled_malformed_session_and_oversized_requests(journeys, monkeypatch):
    assert journeys.get("/api/journeys/changes?catalog_id=catalog-a", headers={"Test-User": ""}).status_code == 401
    assert journeys.get("/api/journeys/changes").status_code == 400
    assert journeys.get("/api/journeys/changes?catalog_id=catalog-a&cursor=-1").status_code == 400
    assert post(journeys, {**upsert("m"), "baseRevision": "1"}).status_code == 400
    mod = journey_module()
    oversized = journeys.post("/api/journeys/mutations?catalog_id=catalog-a",
                              data=json.dumps(upsert("big", body={**journey(), "name": "x" * (mod.MAX_REQUEST_BYTES + 1)})),
                              content_type="application/json")
    assert oversized.status_code == 400
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    monkeypatch.setattr(manager, "collections_enabled", lambda: False)
    assert journeys.get("/api/journeys/changes?catalog_id=catalog-a").status_code == 404
    assert post(journeys, upsert("off")).status_code == 404


def test_a_large_journey_fits(journeys):
    # 2,000 songs with long titles: the size a long box-set journey reaches.
    entries = [{**entry(f"e{n}"), "title": "t" * 200} for n in range(1, 2001)]
    body = journey(entries=entries)
    credits = [credit(f"jny:jny_g:e{n}:abcdef", f"e{n}") for n in range(1, 2001)]
    response = post(journeys, upsert("big", body=body, credits=credits))
    assert response.status_code == 200
    assert len(changes(journeys)["records"][0]["credits"]) == 2000


def test_rekey_rewrites_song_ids_without_a_new_revision(journeys, lumae_postgres_db):
    mod = journey_module()
    body = journey(entries=[entry("e1", track="old-id"), entry("e2")])
    post(journeys, upsert("a", body=body))
    post(journeys, upsert("b", body=body), catalog="catalog-b")
    post(journeys, upsert("gone", jid="jny_gone"))
    post(journeys, delete("gone-delete", 1, jid="jny_gone"))
    before = changes(journeys)["cursor"]
    with lumae_postgres_db.cursor() as cur:
        mod.rekey_journeys(cur, "catalog-a", {"old-id": "new-id"})
    lumae_postgres_db.commit()
    changed = changes(journeys, cursor=before)["records"]
    assert [(r["journeyId"], r["revision"]) for r in changed] == [("jny_g", 1)]
    assert changed[0]["journey"]["entries"][0]["trackId"] == "new-id"
    assert changes(journeys, catalog="catalog-b")["records"][0]["journey"]["entries"][0]["trackId"] == "old-id"
    # The receipt replays the rewritten record, and a write based on revision 1 applies.
    assert post(journeys, upsert("a", body=body)).get_json()["record"]["journey"]["entries"][0]["trackId"] == "new-id"
    assert post(journeys, upsert("edit", base=1, body=journey("Edited"))).status_code == 200


def test_expired_receipts_are_purged_and_recent_ones_kept(journeys, lumae_postgres_db):
    mod = journey_module()
    post(journeys, upsert("old", jid="jny_old"))
    post(journeys, upsert("recent", jid="jny_recent"))
    with lumae_postgres_db.cursor() as cur:
        cur.execute(f"UPDATE {mod.table('journey_mutations')} SET created_at = now() - interval '31 days' WHERE id='old'")
    lumae_postgres_db.commit()
    assert mod.purge_expired_journey_mutations(lumae_postgres_db) == 1
    with lumae_postgres_db.cursor() as cur:
        cur.execute(f"SELECT id FROM {mod.table('journey_mutations')} ORDER BY id")
        assert [row[0] for row in cur.fetchall()] == ["recent"]
    # Without its receipt the old id is checked by revision instead of replayed.
    assert post(journeys, upsert("old", jid="jny_old")).status_code == 409


def test_merge_leaves_its_inputs_alone():
    mod = journey_module()
    stored = journey(entries=[entry("e1", "played", 10, "journey")], place={"entryId": "e1", "positionSeconds": 3},
                     last_played=50)
    incoming = journey(entries=[entry("e1")], last_played=20)
    snapshot = copy.deepcopy((stored, incoming))
    merged = mod.merge_journey(stored, incoming)
    assert (stored, incoming) == snapshot
    assert merged["entries"][0]["state"] == "played" and merged["place"]["entryId"] == "e1"
    assert mod.merge_journey(None, incoming) is incoming


def test_migration_is_idempotent(lumae_postgres_db):
    mod = journey_module()
    mod.migrate_journeys(lumae_postgres_db)
    mod.migrate_journeys(lumae_postgres_db)
    lumae_postgres_db.commit()
    with lumae_postgres_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname IN "
                    "('lumae_journey_changes_idx','lumae_journey_mutations_created_idx')")
        assert cur.fetchone()[0] == 2
