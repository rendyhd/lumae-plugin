"""Saved Vibe sync: contract and PostgreSQL transport tests in disposable schemas."""
import importlib
import json

import pytest
from flask import Flask, g
from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401


def vibe_module():
    load_plugin()
    return importlib.import_module("plugins.LumaeAnalysis.vibes")


def palette(name="Chill", recipe=None):
    return {"kind": "palette", "name": name, "createdAt": "2026-10-03T10:00:00.000Z",
            "recipe": recipe if recipe is not None else {"items": [{"kind": "track", "trackId": "t1", "weight": 0.5}]}}


def put(mutation_id, vibe_id="palette:chill", base=0, vibe=None):
    return {"id": mutation_id, "operation": "put", "vibeId": vibe_id, "baseRevision": base,
            "vibe": vibe if vibe is not None else palette()}


def delete(mutation_id, vibe_id="palette:chill", base=1, at=1_759_500_000_000):
    return {"id": mutation_id, "operation": "delete", "vibeId": vibe_id, "baseRevision": base, "at": at}


@pytest.fixture
def vibes(lumae_postgres_db, monkeypatch):
    mod = vibe_module()
    mod.migrate_vibes(lumae_postgres_db)
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
    return client.post(f"/api/vibes/mutations?catalog_id={catalog}", json=body, headers={"Test-User": user})


def changes(client, catalog="catalog-a", user="alice", cursor=0, limit=250):
    return client.get(f"/api/vibes/changes?catalog_id={catalog}&cursor={cursor}&limit={limit}",
                      headers={"Test-User": user}).get_json()


def test_validation():
    mod = vibe_module()
    validate = mod.validate_mutation
    assert validate(put("m"))["operation"] == "put"
    assert validate(delete("m", vibe_id="dna_vibe:calm"))["operation"] == "delete"
    assert validate(put("m", "compass_preset:rain", vibe={**palette(), "kind": "compass"}))
    too_large = palette(recipe={"blob": "x" * mod.MAX_VIBE_BYTES})
    bad_bodies = [
        None, {}, put(""), put("m", vibe_id="playlist:chill"), put("m", vibe_id="dna_vibe:chill"),
        {**put("m"), "baseRevision": True}, {**put("m"), "baseRevision": -1},
        {**put("m"), "operation": "patch"}, put("m", vibe={**palette(), "recipe": []}),
        put("m", vibe={**palette(), "name": ""}), put("m", vibe={**palette(), "name": "n" * 501}),
        put("m", vibe={**palette(), "kind": "mood"}), put("m", vibe=too_large),
        {**delete("m"), "at": float("nan")}, {k: v for k, v in delete("m").items() if k != "at"},
    ]
    for body in bad_bodies:
        with pytest.raises(ValueError):
            validate(body)


def test_revisions_conflicts_tombstones_and_restore(vibes):
    created = post(vibes, put("create"))
    assert created.status_code == 200
    record = created.get_json()["record"]
    assert record == {"id": "palette:chill", "revision": 1, "deletedAt": None, "vibe": palette()}

    stale = post(vibes, put("other-device", vibe=palette("Chill v2")))
    assert stale.status_code == 409
    assert stale.get_json() == {"error": "vibe_conflict", "record": record}

    edited = post(vibes, put("edit", base=1, vibe=palette("Chill v2"))).get_json()["record"]
    assert edited["revision"] == 2 and edited["vibe"]["name"] == "Chill v2"

    removed = post(vibes, delete("delete", base=2)).get_json()["record"]
    assert removed == {"id": "palette:chill", "revision": 3, "deletedAt": 1_759_500_000_000, "vibe": None}

    restored = post(vibes, put("restore", base=3)).get_json()["record"]
    assert restored["revision"] == 4 and restored["deletedAt"] is None and restored["vibe"] == palette()

    conflict_on_missing = post(vibes, put("phantom", vibe_id="palette:other", base=2))
    assert conflict_on_missing.status_code == 409
    assert conflict_on_missing.get_json() == {"error": "vibe_conflict", "record": None}


def test_receipts_replay_and_bind_their_body(vibes):
    first = post(vibes, put("create"))
    replay = post(vibes, put("create"))
    assert replay.status_code == 200 and replay.get_json() == first.get_json()
    assert changes(vibes)["records"][0]["revision"] == 1

    reused = post(vibes, put("create", vibe=palette("Different")))
    assert reused.status_code == 409
    assert reused.get_json() == {"error": "idempotency_key_conflict"}

    # A refused write stores no receipt: the same id applies once it is valid.
    assert post(vibes, put("late", base=0, vibe=palette("Late"))).status_code == 409
    assert post(vibes, put("late", base=1, vibe=palette("Late"))).status_code == 200


def test_records_are_scoped_by_principal_and_catalogue(vibes):
    post(vibes, put("a"))
    assert len(changes(vibes)["records"]) == 1
    assert changes(vibes, user="bob")["records"] == []
    assert changes(vibes, catalog="catalog-b")["records"] == []
    shared = vibes.post("/api/vibes/mutations?catalog_id=catalog-a", json=put("a"), headers={"Test-Auth": "bearer"})
    assert shared.status_code == 200 and shared.get_json()["record"]["revision"] == 1
    shared_feed = vibes.get("/api/vibes/changes?catalog_id=catalog-a", headers={"Test-Auth": "bearer"}).get_json()
    assert len(shared_feed["records"]) == 1


def test_feed_is_compact_and_pages_by_cursor(vibes):
    post(vibes, put("one", "palette:one"))
    post(vibes, put("two", "dna_vibe:two", vibe={"kind": "dna", "name": "Two", "recipe": {"dimensions": {"energy": 0.4}}}))
    post(vibes, put("one-again", "palette:one", base=1, vibe=palette("One again")))
    feed = changes(vibes)
    assert [r["id"] for r in feed["records"]] == ["dna_vibe:two", "palette:one"]
    assert feed["hasMore"] is False
    page1 = changes(vibes, limit=1)
    assert page1["hasMore"] is True and [r["id"] for r in page1["records"]] == ["dna_vibe:two"]
    page2 = changes(vibes, cursor=page1["cursor"], limit=1)
    assert page2["hasMore"] is False and [r["id"] for r in page2["records"]] == ["palette:one"]
    assert changes(vibes, cursor=page2["cursor"])["records"] == []
    assert changes(vibes, cursor=page2["cursor"])["cursor"] == page2["cursor"]


def test_vibes_are_stored_as_sent(vibes):
    vibe = palette("夜 ✨", {"items": [{"kind": "album", "albumKey": "artist::album", "weight": 1}],
                            "futureField": {"nested": [1, 2.5, None]}})
    vibe["extra"] = "kept"
    post(vibes, put("unicode", vibe=vibe))
    assert changes(vibes)["records"][0]["vibe"] == vibe


def test_disabled_malformed_session_and_oversized_requests(vibes, monkeypatch):
    assert vibes.get("/api/vibes/changes?catalog_id=catalog-a", headers={"Test-User": ""}).status_code == 401
    assert vibes.get("/api/vibes/changes").status_code == 400
    assert vibes.get("/api/vibes/changes?catalog_id=catalog-a&cursor=-1").status_code == 400
    oversized = vibes.post("/api/vibes/mutations?catalog_id=catalog-a",
                           data=json.dumps(put("big", vibe=palette(recipe={"x": "y" * 140_000}))),
                           content_type="application/json")
    assert oversized.status_code == 400
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    monkeypatch.setattr(manager, "collections_enabled", lambda: False)
    assert vibes.get("/api/vibes/changes?catalog_id=catalog-a").status_code == 404
    assert post(vibes, put("off")).status_code == 404


def test_rekey_rewrites_song_ids_without_a_new_revision(vibes, lumae_postgres_db):
    mod = vibe_module()
    recipe = {"items": [{"kind": "track", "trackId": "old-id", "weight": 1}], "seedTrackIds": ["old-id"]}
    post(vibes, put("a", vibe=palette(recipe=recipe)))
    post(vibes, put("b", vibe=palette(recipe=recipe)), catalog="catalog-b")
    before = changes(vibes)["cursor"]
    with lumae_postgres_db.cursor() as cur:
        mod.rekey_vibes(cur, "catalog-a", {"old-id": "new-id"})
    lumae_postgres_db.commit()
    changed = changes(vibes, cursor=before)["records"]
    assert len(changed) == 1
    assert changed[0]["revision"] == 1
    assert changed[0]["vibe"]["recipe"] == {"items": [{"kind": "track", "trackId": "new-id", "weight": 1}],
                                            "seedTrackIds": ["new-id"]}
    assert changes(vibes, catalog="catalog-b")["records"][0]["vibe"]["recipe"] == recipe
    # The receipt replays the rewritten record, and an edit based on revision 1 applies.
    assert post(vibes, put("a", vibe=palette(recipe=recipe))).get_json()["record"]["vibe"]["recipe"]["seedTrackIds"] == ["new-id"]
    assert post(vibes, put("edit", base=1, vibe=palette("Edited", recipe))).status_code == 200


def test_expired_receipts_are_purged_and_recent_ones_kept(vibes, lumae_postgres_db):
    mod = vibe_module()
    post(vibes, put("old", "palette:old"))
    post(vibes, put("recent", "palette:recent"))
    with lumae_postgres_db.cursor() as cur:
        cur.execute(f"UPDATE {mod.table('vibe_mutations')} SET created_at = now() - interval '31 days' WHERE id='old'")
    lumae_postgres_db.commit()
    assert mod.purge_expired_vibe_mutations(lumae_postgres_db) == 1
    with lumae_postgres_db.cursor() as cur:
        cur.execute(f"SELECT id FROM {mod.table('vibe_mutations')} ORDER BY id")
        assert [row[0] for row in cur.fetchall()] == ["recent"]
    # Without its receipt the old id is checked by revision instead of replayed.
    assert post(vibes, put("old", "palette:old")).status_code == 409


def test_migration_is_idempotent(lumae_postgres_db):
    mod = vibe_module()
    mod.migrate_vibes(lumae_postgres_db)
    mod.migrate_vibes(lumae_postgres_db)
    lumae_postgres_db.commit()
    with lumae_postgres_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname IN ('lumae_vibe_changes_idx','lumae_vibe_mutations_created_idx')")
        assert cur.fetchone()[0] == 2
