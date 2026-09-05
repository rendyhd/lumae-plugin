"""Federation authorization and background sync integration against PostgreSQL."""

import importlib.util
import json
import sys
import types
from pathlib import Path
import numpy as np
import pytest
from flask import Flask, g
from psycopg2.extras import Json
from test_lumae_analysis import lumae_postgres_db


@pytest.fixture
def federation(lumae_postgres_db, monkeypatch):
    db = lumae_postgres_db
    import plugin.api as api

    monkeypatch.setattr(api, "get_db", lambda: db)
    monkeypatch.setattr(api, "table", lambda name: f"friend_{name}")
    tasks = types.ModuleType("tasks")
    media = types.ModuleType("tasks.mediaserver")
    sonic = types.ModuleType("tasks.sonic_fingerprint_manager")
    media.get_all_songs = lambda: []
    sonic.calculate_sonic_fingerprint_vector = lambda: np.ones(200)
    for key, value in [
        ("tasks", tasks),
        ("tasks.mediaserver", media),
        ("tasks.sonic_fingerprint_manager", sonic),
    ]:
        monkeypatch.setitem(sys.modules, key, value)
    root = Path(__file__).resolve().parents[2] / "plugins" / "FederatedAlbums"
    spec = importlib.util.spec_from_file_location(
        "_federation_review_test",
        root / "__init__.py",
        submodule_search_locations=[str(root)],
    )
    mod = importlib.util.module_from_spec(spec)
    # A fresh namespace includes its repositories on every fixture.
    for key in list(sys.modules):
        if key.startswith("_federation_review_test"):
            monkeypatch.delitem(sys.modules, key)
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    with db.cursor() as cur:
        cur.execute(
            "CREATE TABLE cron(name TEXT,task_type TEXT UNIQUE,cron_expr TEXT,enabled BOOLEAN)"
        )
    mod.migrate(db)
    app = Flask(__name__)
    app.register_blueprint(mod.bp)

    @app.before_request
    def owner():
        from flask import request

        g.auth_user = request.headers.get("X-Test-User", "alice")
        g.auth_method = "session"
        g.auth_role = "user"

    mod.test_client = app.test_client()
    mod.test_db = db
    yield mod


def connection(mod, owner="alice"):
    with mod.test_db.cursor() as cur:
        cur.execute(
            "INSERT INTO friend_connections(owner,name,base_url,access_token,remote_instance_id) VALUES(%s,'Friend',%s,'afa_secret','remote') RETURNING id",
            (owner, "https://" + owner + ".example"),
        )
        value = cur.fetchone()[0]
    mod.test_db.commit()
    return value


def fingerprint(mod):
    track = mod.AlbumTrack(
        "track", np.ones(200, dtype=np.float32), 0.5, np.zeros(6, dtype=np.float32), 1
    )
    return mod.serialize_fingerprint(mod.build_fingerprint("album", [track]))


def remote_album(mod, connection_id, key="remote-album", title="Private album"):
    fp = fingerprint(mod)
    with mod.test_db.cursor() as cur:
        cur.execute(
            "INSERT INTO friend_remote_albums(connection_id,remote_instance_id,album_key,album,artist,track_count,fingerprint,buckets,updated_at) VALUES(%s,'remote',%s,%s,'Artist',1,%s,%s,now())",
            (
                connection_id,
                key,
                title,
                Json(fp),
                mod.catalog_store.buckets(np.ones(200)),
            ),
        )
    mod.test_db.commit()


def test_install_accepts_host_db_and_registration_handles_missing_auth(federation):
    mod = federation
    mod.migrate(mod.test_db)

    class Host:
        def __getattr__(self, name):
            if name == "set_bearer_authenticator":
                raise AttributeError(name)
            return lambda *args, **kwargs: None

    mod.register(Host())
    assert not mod.HOST_CAPABILITIES["scoped_bearer_auth"]
    response = mod.test_client.post("/api/pairing-tokens", json={"label": "Friend"})
    assert response.status_code == 503
    assert (
        mod.test_client.get("/api/health").json["hostCapabilities"][
            "scoped_bearer_auth"
        ]
        is False
    )


def test_connection_reads_delete_sync_search_and_artwork_are_owner_scoped(
    federation, monkeypatch
):
    mod = federation
    cid = connection(mod)
    remote_album(mod, cid)
    client = mod.test_client
    bob = {"X-Test-User": "bob"}
    assert len(client.get("/api/connections").json["connections"]) == 1
    assert client.get("/api/connections", headers=bob).json["connections"] == []
    assert client.post(f"/api/connections/{cid}/sync", headers=bob).status_code == 404
    assert client.delete(f"/api/connections/{cid}", headers=bob).status_code == 404
    assert (
        client.get("/api/friend-artwork/remote/remote-album", headers=bob).status_code
        == 404
    )
    assert client.get("/api/albums/search?q=Private", headers=bob).json["albums"] == []
    assert (
        client.post(
            "/api/similar-albums",
            json={"instanceId": "remote", "albumKey": "remote-album"},
            headers=bob,
        ).status_code
        == 404
    )
    assert (
        client.post(
            "/api/album-recommendations", json={"source": "server"}, headers=bob
        ).json["albums"]
        == []
    )
    assert (
        client.get("/api/albums/search?q=Private").json["albums"][0]["album"]
        == "Private album"
    )
    assert client.delete(f"/api/connections/{cid}").status_code == 200
    with mod.test_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM friend_remote_albums")
        assert cur.fetchone() == (0,)


def test_sync_http_only_queues_and_recovers_after_enqueue_failure(
    federation, monkeypatch
):
    mod = federation
    cid = connection(mod)
    monkeypatch.setattr(
        mod, "enqueue", lambda *args: (_ for _ in ()).throw(RuntimeError("queue down"))
    )
    monkeypatch.setattr(
        mod,
        "_fetch_remote_catalog",
        lambda *args: pytest.fail("HTTP must not fetch a catalogue"),
    )
    result = mod.test_client.post(f"/api/connections/{cid}/sync")
    assert result.status_code == 202 and result.json["status"] == "queued"
    monkeypatch.setattr(mod, "_validate_base_url", lambda url: url)
    monkeypatch.setattr(mod, "_fetch_remote_catalog", lambda *args: ("remote", []))
    assert mod.sync_reconcile_task()["status"] == "complete"
    assert (
        mod.test_client.get("/api/connections").json["connections"][0]["syncStatus"]
        == "complete"
    )


def test_sync_does_not_publish_after_connection_deleted(federation, monkeypatch):
    mod = federation
    cid = connection(mod)
    mod.sync_jobs.request_sync(mod.test_db, cid, "alice")
    monkeypatch.setattr(mod, "_validate_base_url", lambda url: url)

    def fetch(*args):
        with mod.test_db.cursor() as cur:
            cur.execute("DELETE FROM friend_connections WHERE id=%s", (cid,))
        mod.test_db.commit()
        return "remote", []

    monkeypatch.setattr(mod, "_fetch_remote_catalog", fetch)
    assert mod.sync_reconcile_task()["status"] == "failed"
    with mod.test_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM friend_remote_albums")
        assert cur.fetchone() == (0,)


@pytest.mark.parametrize(
    "pages",
    [
        [{"albums": [], "nextCursor": "repeat"}],
        [
            {"albums": [{"albumKey": "a"}], "nextCursor": "repeat"},
            {"albums": [{"albumKey": "b"}], "nextCursor": "repeat"},
        ],
    ],
)
def test_remote_cursor_must_progress(federation, monkeypatch, pages):
    mod = federation
    called = []
    fp = fingerprint(mod)
    for page in pages:
        page.update(capability=mod.CAPABILITY, instanceId="remote")
        for item in page["albums"]:
            item["fingerprint"] = fp

    class Response:
        status_code = 200

        def __init__(self, value):
            self.value = value

        def iter_content(self, *args):
            yield json.dumps(self.value).encode()

        def close(self):
            pass

    def get(*args, **kwargs):
        called.append(1)
        return Response(pages[min(len(called) - 1, len(pages) - 1)])

    monkeypatch.setattr(mod.requests, "get", get)
    with pytest.raises(ValueError, match="progress"):
        mod._fetch_remote_catalog("https://friend.example", "afa_secret")
    assert len(called) <= 2


def test_remote_total_deadline_checked_during_response(federation, monkeypatch):
    mod = federation
    now = [0]
    monkeypatch.setattr(mod.time, "monotonic", lambda: now[0])

    class Response:
        def iter_content(self, *args):
            yield b"{"
            now[0] = 301
            yield b"}"

    with pytest.raises(ValueError, match="deadline"):
        mod._read_limited_json(Response(), deadline=300)


def test_search_projects_metadata_and_similarity_shortlist_is_bounded(federation):
    mod = federation
    cid = connection(mod)
    for i in range(520):
        remote_album(mod, cid, key=f"album-{i:04}", title="Private album")
    with mod.test_client.application.test_request_context():
        g.auth_user = "alice"
        rows = mod.catalog_store.read(
            "alice", remote=True, query="Priv", fingerprint=False
        )
        assert len(rows) == 50 and all("fingerprint" not in row for row in rows)
        candidates = mod.catalog_store.read(
            "alice", remote=True, vector=np.ones(200), fingerprint=True, limit=10000
        )
        assert len(candidates) == 512


def test_source_registry_requires_selection_and_maps_provider_ids(
    federation, monkeypatch
):
    mod = federation
    api = mod.source_projection.api
    monkeypatch.setattr(
        api, "list_servers", lambda: [{"id": "one"}, {"id": "two"}], raising=False
    )
    monkeypatch.setattr(api, "active_server_id", lambda: "one", raising=False)
    monkeypatch.setattr(api, "use_server", lambda server: None, raising=False)
    with pytest.raises(ValueError, match="Select"):
        mod.source_projection.source(mod.test_db, "friend_meta")
    monkeypatch.setenv("FEDERATED_ALBUMS_SERVER_ID", "two")
    join, params, key = mod.source_projection.mapping(mod.test_db, "friend_meta")
    assert (
        params == ["two"] and key == "m.provider_track_id" and "m.server_id=%s" in join
    )
    monkeypatch.setenv("FEDERATED_ALBUMS_SERVER_ID", "one")
    assert mod.source_projection.source(mod.test_db, "friend_meta") == ("two", True)


def test_similarity_shortlist_prioritizes_multi_band_evidence(federation):
    mod = federation
    cid = connection(mod)
    remote_album(mod, cid, key="a-weak")
    remote_album(mod, cid, key="z-strong")
    with mod.test_db.cursor() as cur:
        cur.execute(
            "UPDATE friend_remote_albums SET buckets=buckets[1:1] WHERE album_key='a-weak'"
        )
    mod.test_db.commit()
    result = mod.catalog_store.read("alice", remote=True, vector=np.ones(200), limit=1)
    assert result[0]["album_key"] == "z-strong"
