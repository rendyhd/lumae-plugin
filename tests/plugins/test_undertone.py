"""Undertone record sync: contract and PostgreSQL transport tests in disposable schemas."""
import importlib

import pytest
from flask import Flask, g
from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401


def undertone_module():
    load_plugin()
    return importlib.import_module("plugins.LumaeAnalysis.undertone")


def preset(name="Deep sleep"):
    return {"kind": "preset", "body": {"name": name, "between": "gapless", "plays": {"kind": "album", "albumId": "a1"}}}


def put(mutation_id, record_id="preset:p1", base=0, record=None):
    return {"id": mutation_id, "operation": "put", "recordId": record_id, "baseRevision": base,
            "record": record if record is not None else preset()}


def delete(mutation_id, record_id="preset:p1", base=1, at=1_762_000_000_000):
    return {"id": mutation_id, "operation": "delete", "recordId": record_id, "baseRevision": base, "at": at}


@pytest.fixture
def client(lumae_postgres_db, monkeypatch):
    mod = undertone_module()
    mod.migrate_undertone(lumae_postgres_db)
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


def post(client, body, source="src-a", user="alice"):
    return client.post(f"/api/undertone/mutations?source={source}", json=body, headers={"Test-User": user})


def changes(client, source="src-a", user="alice", cursor=0, limit=500):
    return client.get(f"/api/undertone/changes?source={source}&cursor={cursor}&limit={limit}",
                      headers={"Test-User": user}).get_json()


def test_validation():
    mod = undertone_module()
    validate = mod.validate_mutation
    assert validate(put("m"))["operation"] == "put"
    assert validate(delete("m", record_id="detail:track:t1:bpm"))["operation"] == "delete"
    assert validate(put("m", "item:t:t1", record={"kind": "item", "body": {"title": "Rain"}}))
    too_large = {"kind": "preset", "body": {"blob": "x" * mod.MAX_RECORD_BYTES}}
    bad = [
        None, {}, put(""), put("m", record_id="vibe:p1"), put("m", record_id="preset:"),
        put("m", record_id="field:bpm"), {**put("m"), "baseRevision": True}, {**put("m"), "baseRevision": -1},
        {**put("m"), "operation": "patch"}, put("m", record={"kind": "preset", "body": []}),
        put("m", record=too_large), {**delete("m"), "at": float("nan")},
        {k: v for k, v in delete("m").items() if k != "at"},
    ]
    for body in bad:
        with pytest.raises(ValueError):
            validate(body)
    capability = mod.capability(True, "personal")
    assert capability["kinds"] == ["category", "detail", "field", "group", "item", "preset"]


def test_revisions_conflicts_tombstones_and_feed(client):
    created = post(client, put("create"))
    assert created.status_code == 200
    assert created.get_json()["record"]["revision"] == 1
    # A replay answers the stored response; another body under the same id is refused.
    assert post(client, put("create")).get_json() == created.get_json()
    assert post(client, put("create", record=preset("Nap"))).status_code == 409
    stale = post(client, put("stale", base=0, record=preset("Nap")))
    assert stale.status_code == 409
    assert stale.get_json()["record"]["record"]["body"]["name"] == "Deep sleep"
    assert post(client, delete("gone")).get_json()["record"]["deletedAt"] == 1_762_000_000_000
    feed = changes(client)
    assert [r["revision"] for r in feed["records"]] == [2]
    assert feed["records"][0]["record"] is None
    # Another principal and another source see none of it.
    assert changes(client, user="bob")["records"] == []
    assert changes(client, source="src-b")["records"] == []


def test_batch_applies_in_order_with_per_record_answers(client):
    response = client.post("/api/undertone/mutations/batch?source=src-a", json={"mutations": [
        put("b1", "detail:track:t1:bpm", record={"kind": "detail", "body": {"value": 60}}),
        put("b2", "detail:track:t2:bpm", record={"kind": "detail", "body": {"value": 62}}),
        put("b3", "detail:track:t1:bpm", record={"kind": "detail", "body": {"value": 64}}),
    ]}, headers={"Test-User": "alice"})
    assert response.status_code == 200
    statuses = [r["status"] for r in response.get_json()["results"]]
    assert statuses == [200, 200, 409]
    assert len(changes(client)["records"]) == 2
    bad = client.post("/api/undertone/mutations/batch?source=src-a", json={"mutations": [put("x"), put("x")]},
                      headers={"Test-User": "alice"})
    assert bad.status_code == 400


def item(mutation_id, record_id, body):
    return put(mutation_id, record_id, record={"kind": "item", "body": body})


def test_sources_list_each_library_with_its_name_and_size(client):
    batch = [
        item("i0", "item:source", {"kind": "source", "server": "second", "serverType": "jellyfin", "libraryName": "Sleep"}),
        item("i1", "item:t:t1", {"kind": "track", "id": "t1", "title": "Rain"}),
        item("i2", "item:t:t2", {"kind": "track", "id": "t2", "title": "Wind"}),
        item("i3", "item:a:a1", {"kind": "album", "id": "a1", "title": "Calm"}),
        put("p1", "preset:p1"),
    ]
    assert client.post("/api/undertone/mutations/batch?source=src-a", json={"mutations": batch},
                       headers={"Test-User": "alice"}).status_code == 200
    post(client, delete("gone", record_id="item:t:t2", base=1))
    post(client, put("other", "preset:p9"), source="src-b")
    response = client.get("/api/undertone/sources", headers={"Test-User": "alice"})
    assert response.headers["Cache-Control"] == "private, no-store"
    body = response.get_json()
    assert body["max_batch"] == 200
    assert [s["source"] for s in body["sources"]] == ["src-b", "src-a"]
    assert body["sources"][1] == {"source": "src-a", "tracks": 1, "albums": 1, "presets": 1,
                                  "seq": body["sources"][1]["seq"], "library": "Sleep", "serverType": "jellyfin"}
    assert client.get("/api/undertone/sources", headers={"Test-User": "bob"}).get_json()["sources"] == []


def test_page_is_served_beside_living_collections(client):
    body = client.get("/undertone", headers={"Test-User": "alice"}).get_data(as_text=True)
    assert "Personal library · alice" in body
    assert "Only this signed-in AudioMuse user can see it." in body
    # Reads and writes only through the Undertone record routes, relative to the plugin.
    assert "const api='api/undertone'" in body
    assert "/mutations/batch?source=" in body and "/changes?source=" in body
    # The phone's own file, and the phone's matching ladder for imports.
    assert "format:'lumae-undertone',version:1" in body
    assert "musicBrainzId" in body and "Math.abs(t.seconds-m.seconds)<=2" in body
    # A record changed elsewhere is written once more over its new revision.
    assert "r.error==='undertone_conflict'" in body and "w.tries<1" in body
    # Released before every Lumae has Undertone, so the empty page says what it needs.
    assert body.count("With a version of Lumae that has Undertone") == 2
    # A preset's filter is the phone's: the page edits around it and imports it whole.
    assert "const {id:_id,updatedAt:_u,...rest}=p" in body
    assert "mine={kind:'filter',filter:plays.filter}" in body


def test_a_preset_filter_comes_back_as_the_phone_wrote_it(client):
    # Lumae UT.12's filter keys aren't the plugin's to know: all of, leaving out, a track's length.
    record = {
        "kind": "preset",
        "body": {
            "name": "Rain, no thunder",
            "between": "gapless",
            "plays": {
                "kind": "filter",
                "filter": {
                    "clauses": [
                        {"fieldId": "f_moods", "op": "any", "values": ["Calm", "Serene"], "matchAll": True},
                        {"fieldId": "f_style", "op": "any", "values": [], "not": ["Thunder bursts"]},
                        {"fieldId": "length", "op": "range", "min": 45, "max": None},
                    ]
                },
            },
        },
    }
    assert post(client, put("filter", record=record)).status_code == 200
    assert changes(client)["records"][0]["record"] == record


def test_page_escapes_the_principal():
    ui = importlib.import_module("plugins.LumaeAnalysis.undertone_ui")
    body = ui.render_undertone_page("<b>x</b>", "a & b")
    assert "&lt;b&gt;x&lt;/b&gt;" in body and "a &amp; b" in body
