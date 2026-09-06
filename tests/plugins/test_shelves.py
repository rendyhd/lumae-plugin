"""Contracts and PostgreSQL transport tests in disposable schemas."""
import importlib
import pytest
from flask import Flask, g
from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401


def shelf_module():
    load_plugin()
    return importlib.import_module("plugins.LumaeAnalysis.shelves")


def add(identifier, entity=None, kind="album", at=10):
    return {"id": f"add-{identifier}", "operation": "add", "member": {
        "id": identifier, "entityId": entity or identifier, "kind": kind,
        "title": identifier, "artist": "Artist", "addedAt": at}}


@pytest.fixture
def shelves(lumae_postgres_db, monkeypatch):
    mod = shelf_module()
    mod.migrate_shelves(lumae_postgres_db)
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
    return client.post(f"/api/shelves/mutations?catalog_id={catalog}", json=body, headers={"Test-User": user})


def records(client, catalog="catalog-a", user="alice"):
    return client.get(f"/api/shelves/changes?catalog_id={catalog}", headers={"Test-User": user}).get_json()["records"]


def test_validation():
    validate = shelf_module().validate_mutation
    assert validate(add("a"))["operation"] == "add"
    for body in [None, {}, {"id": "x", "operation": "order", "kind": "track", "ids": [], "baseRevision": 0},
                 {"id": "x", "operation": "evidence", "records": []}]:
        with pytest.raises(ValueError):
            validate(body)
    bad = add("a")
    bad["member"]["addedAt"] = float("nan")
    with pytest.raises(ValueError):
        validate(bad)


def test_periods_duplicates_and_idempotency(shelves):
    first = post(shelves, add("one")).get_json()
    assert post(shelves, add("one")).get_json() == first
    duplicate = post(shelves, add("duplicate", "one")).get_json()["records"][0]["value"]
    assert duplicate["id"] == "one"
    assert duplicate["aliases"] == ["duplicate"]
    assert post(shelves, {"id": "remove", "operation": "remove", "memberId": "duplicate", "at": 20}).status_code == 200
    restored = post(shelves, {"id": "undo", "operation": "restore", "memberId": "one", "at": 21}).get_json()
    assert restored["records"][0]["value"]["addedAt"] == 10
    post(shelves, {"id": "remove-again", "operation": "remove", "memberId": "one", "at": 22})
    post(shelves, add("two", "one", at=30))
    post(shelves, {"id": "stale-remove", "operation": "remove", "memberId": "one", "at": 23})
    post(shelves, {"id": "stale-undo", "operation": "restore", "memberId": "one", "at": 24})
    active = [r["value"] for r in records(shelves) if r["type"] == "member" and r["value"]["deletedAt"] is None]
    assert [m["id"] for m in active] == ["two"]
    assert active[0]["position"] == 1


def test_scope_and_independent_kinds(shelves):
    post(shelves, add("album"))
    post(shelves, add("artist", kind="artist"))
    assert len(records(shelves)) == 2
    assert records(shelves, user="bob") == []
    assert records(shelves, catalog="catalog-b") == []
    shared = shelves.post("/api/shelves/mutations?catalog_id=catalog-a", json=add("shared"), headers={"Test-Auth": "bearer"})
    assert shared.status_code == 200
    assert len(records(shelves)) == 2
    assert len(shelves.get("/api/shelves/changes?catalog_id=catalog-a", headers={"Test-Auth": "bearer"}).get_json()["records"]) == 1


def test_order_conflict_keeps_remote_additions(shelves):
    for identifier in ("a", "b", "remote"):
        post(shelves, add(identifier))
    operation = {"id": "arrange", "operation": "order", "kind": "album", "ids": ["b", "a"], "baseRevision": 0}
    arranged = post(shelves, operation).get_json()["records"][0]["value"]
    assert arranged["ids"] == ["b", "a", "remote"]
    local = {**operation, "id": "other-device", "ids": ["a", "b"]}
    conflict = post(shelves, local)
    assert conflict.status_code == 409
    assert conflict.get_json()["order"] == arranged
    local["baseRevision"] = 1
    assert post(shelves, local).status_code == 200


def test_evidence_delivery_removal_and_paging(shelves):
    event = {"id": "listen", "type": "listen", "entityKind": "track", "entityId": "track", "at": 10}
    rating = {**event, "id": "rating", "type": "rating", "rating": 5}
    body = {"id": "batch1", "operation": "evidence", "records": [event, rating]}
    assert post(shelves, body).status_code == 200
    assert post(shelves, {**body, "id": "batch2"}).status_code == 200
    rating["rating"], rating["at"] = None, 20
    post(shelves, {**body, "id": "batch3"})
    assert len(records(shelves)) == 2
    assert next(r for r in records(shelves) if r["id"] == "rating")["value"]["rating"] is None
    page1 = shelves.get("/api/shelves/snapshot?catalog_id=catalog-a&limit=1").get_json()
    assert page1["hasMore"]
    page2 = shelves.get(f"/api/shelves/changes?catalog_id=catalog-a&cursor={page1['cursor']}&limit=1").get_json()
    assert not page2["hasMore"]
    assert page1["records"][0]["id"] != page2["records"][0]["id"]


def test_disabled_and_malformed_session_fail_closed(shelves, monkeypatch):
    assert shelves.get("/api/shelves/changes?catalog_id=catalog-a", headers={"Test-User": ""}).status_code == 401
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    monkeypatch.setattr(manager, "collections_enabled", lambda: False)
    assert shelves.get("/api/shelves/changes?catalog_id=catalog-a").status_code == 404
