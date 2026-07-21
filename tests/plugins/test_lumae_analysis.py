import importlib
import json
import pathlib
import struct
import sys
import math
import types

import numpy as np
import pytest

from flask import Flask, g


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

plugin_module = types.ModuleType("plugin")
plugin_api_module = types.ModuleType("plugin.api")
plugin_api_module.config = types.SimpleNamespace(
    APP_VERSION="v2.6.2",
    MEDIASERVER_TYPE="navidrome",
)
plugin_api_module.enqueue = lambda *args, **kwargs: None
plugin_api_module.get_db = lambda: None
plugin_api_module.get_setting = lambda _key, default=None: default
plugin_api_module.logger = types.SimpleNamespace(
    warning=lambda *args, **kwargs: None,
    exception=lambda *args, **kwargs: None,
)
plugin_api_module.render_page = lambda body, title=None: body
plugin_api_module.set_setting = lambda _key, _value: None
plugin_api_module.table = lambda name: f"plugin_lumae_analysis__{name}"
sys.modules.setdefault("plugin", plugin_module)
sys.modules.setdefault("plugin.api", plugin_api_module)

PLUGIN_TABLE = "plugin_lumae_analysis__profiles"


def load_plugin():
    return importlib.import_module("plugins.LumaeAnalysis")


def plugin_client(mod):
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    return app.test_client()


def expect_v3_readiness(core_version):
    qualified = core_version == "v3.0.3"
    blocker = "catalog_not_initialized" if qualified else "core_release_unqualified"
    return {
        "qualified_core_version": "v3.0.3",
        "detected_core_version": core_version,
        "applicable": True,
        "status": blocker,
        "ready": False,
        "verification_mode": None,
        "administrator_acknowledged": False,
        "acknowledged_at": None,
        "blockers": [blocker],
    }


def test_plugin_manifest_has_lumae_identity():
    with open("plugins/LumaeAnalysis/plugin.json", "r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    assert manifest["id"] == "lumae_analysis"
    assert manifest["name"] == "Lumae Analysis"
    assert manifest["requirements"] == []
    assert manifest["versions"][0]["version"] == "0.8.0"
    assert manifest["versions"][0]["min_core_version"] == "2.6.0"
    assert manifest["capabilities"]["lumae_analysis_profiles"] == {
        "schema_version": 1,
        "analyzer_version": 1,
        "profile_source": "waveform",
        "features": ["loudness", "mix_ramp"],
    }
    assert manifest["capabilities"]["living_collections"] == {
        "schema_version": 1,
        "features": [
            "mixed_album_track_membership",
            "per_user_storage",
            "incremental_sync",
            "web_manager",
            "library_browser",
            "album_track_numbers",
            "preview_playback",
            "bulk_management",
            "principal_scoped_backup",
            "additive_restore",
        ],
    }
    assert manifest["capabilities"]["catalog_mirror"] == {
        "catalog_schema_version": 2,
        "analysis_schema_version": 2,
        "supported_core_range": ">=2.6.0,<4.0.0",
        "features": [
            "dual_core_compat",
            "stable_catalog_instance",
            "provider_occurrences",
            "rich_metadata",
            "complete_generations",
            "bootstrap_leases",
            "cursor_changes",
            "refresh_on_demand",
            "library_scope",
            "album_ids",
            "artist_credits",
            "soft_deletions",
            "shared_analysis",
            "binary_vectors",
            "v3_release_readiness",
            "provider_track_scope_verification",
        ],
    }


def test_health_endpoint_reports_schema_and_analyzer_versions(monkeypatch):
    mod = load_plugin()
    client = plugin_client(mod)

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.get_json() == {
        "plugin": "lumae_analysis",
        "plugin_version": "0.8.0",
        "core_version": "v2.6.2",
        "core_adapter": "v2_single_server",
        "supported_core_range": ">=2.6.0,<4.0.0",
        "schema_version": 1,
        "analyzer_version": 1,
        "capabilities": {
            "collections": {
                "schema_version": 1,
                "backup_version": 1,
                "enabled": False,
                "scope": "shared",
            },
            "catalog_mirror": mod.catalog_capability(),
        },
        "status": "ok",
    }


def test_catalog_health_uses_v2_single_server_adapter():
    mod = load_plugin()
    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    body = response.get_json()
    assert body["core_adapter"] == "v2_single_server"
    assert body["supported"] is True
    assert body["servers"] == [
        {
            "server_id": "legacy-default",
            "catalog_instance_id": None,
            "name": "Default music server",
            "provider_type": "navidrome",
            "is_default": True,
            "status": "not_initialized",
        }
    ]


@pytest.mark.parametrize("core_version", ["v3.0.0", "v3.0.3"])
def test_catalog_health_sanitizes_v3_server_credentials(monkeypatch, core_version):
    mod = load_plugin()
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", core_version)
    monkeypatch.setattr(plugin_api_module, "active_server_id", lambda: "server-a", raising=False)
    monkeypatch.setattr(plugin_api_module, "use_server", lambda _server_id: None, raising=False)
    monkeypatch.setattr(
        plugin_api_module,
        "list_servers",
        lambda: [
            {
                "server_id": "server-a",
                "name": "Main",
                "server_type": "jellyfin",
                "is_default": True,
                "creds": {"token": "secret"},
                "url": "https://internal.invalid",
            }
        ],
        raising=False,
    )

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    body = response.get_json()
    assert body["core_adapter"] == "v3_registry"
    assert body["servers"][0] == {
        "server_id": "server-a",
        "catalog_instance_id": None,
        "name": "Main",
        "provider_type": "jellyfin",
        "is_default": True,
        "status": "not_initialized",
        "v3_readiness": expect_v3_readiness(core_version),
    }


def test_catalog_health_exposes_persisted_v3_0_3_source_readiness(monkeypatch):
    mod = load_plugin()
    source = readiness_source()
    db = object()
    captured = {}
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", "v3.0.3")
    monkeypatch.setattr(plugin_api_module, "active_server_id", lambda: "server-a", raising=False)
    monkeypatch.setattr(plugin_api_module, "use_server", lambda _server_id: None, raising=False)
    monkeypatch.setattr(
        plugin_api_module,
        "list_servers",
        lambda: [{"server_id": "server-a", "server_type": "navidrome"}],
        raising=False,
    )
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])

    def release_readiness(selected_db, compatibility, selected_source, policy):
        captured.update(
            {
                "db": selected_db,
                "core": compatibility.core_version,
                "source": selected_source,
                "policy": policy,
            }
        )
        return {
            "qualified_core_version": "v3.0.3",
            "detected_core_version": "v3.0.3",
            "applicable": True,
            "status": "ready",
            "ready": True,
            "verification_mode": "upgraded",
            "administrator_acknowledged": True,
            "acknowledged_at": "2026-07-20T12:00:00Z",
            "blockers": [],
        }

    monkeypatch.setattr(mod, "v3_release_readiness", release_readiness)

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    body = response.get_json()
    assert body["plugin_version"] == "0.8.0"
    assert body["servers"][0]["v3_readiness"]["ready"] is True
    assert captured["db"] is db
    assert captured["core"] == "v3.0.3"
    assert captured["source"] == source
    assert captured["policy"]["catalogue_id_scheme_version"] is None


def test_v2_catalog_health_never_executes_v3_readiness_queries(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(
        mod,
        "v3_release_readiness",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("v3-only")),
    )

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    assert response.get_json()["core_adapter"] == "v2_single_server"


def test_catalog_cursor_is_opaque_round_trippable_and_source_bound():
    from plugins.LumaeAnalysis.catalog import opaque_cursor, parse_opaque_cursor

    cursor = opaque_cursor("catalog-a", "epoch-a", 42)

    assert "catalog-a" not in cursor
    assert parse_opaque_cursor(cursor) == {
        "catalog_instance_id": "catalog-a",
        "epoch": "epoch-a",
        "seq": 42,
    }
    with pytest.raises(ValueError, match="Malformed"):
        parse_opaque_cursor("not-a-cursor")


def test_bootstrap_session_api_keeps_token_out_of_url_and_private_cache(monkeypatch):
    mod = load_plugin()
    captured = {}
    monkeypatch.setattr(mod, "get_db", lambda: object())

    def create(db, principal, **kwargs):
        captured.update({"db": db, "principal": principal, **kwargs})
        return {
            "session_token": "secret-session-token",
            "catalog_instance_id": "catalog-a",
            "server_id": "server-a",
        }

    monkeypatch.setattr(mod, "create_bootstrap_session", create)
    response = plugin_client(mod).post(
        "/api/catalog/bootstrap-sessions",
        json={"server_id": "server-a", "stream": "catalog"},
    )

    assert response.status_code == 201
    assert response.get_json()["session_token"] == "secret-session-token"
    assert response.headers["Cache-Control"] == "private, no-store"
    assert captured["server_id"] == "server-a"
    assert captured["principal"].startswith("client:")


def test_bootstrap_page_requires_token_header_and_returns_410_after_expiry(monkeypatch):
    mod = load_plugin()
    client = plugin_client(mod)

    assert client.get("/api/catalog/bootstrap").status_code == 400
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(
        mod,
        "bootstrap_page",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyError("bootstrap_required")),
    )
    response = client.get(
        "/api/catalog/bootstrap?stream=catalog",
        headers={"X-Lumae-Bootstrap-Token": "expired"},
    )

    assert response.status_code == 410
    assert response.get_json()["error"] == "bootstrap_required"


def test_catalog_changes_rejects_malformed_cursor_without_reading_database(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: object())
    response = plugin_client(mod).get("/api/catalog/changes?cursor=malformed")

    assert response.status_code == 400
    assert response.get_json()["error"] == "invalid_cursor"


def test_catalog_refresh_coalesces_to_selected_source(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(
        mod,
        "resolve_catalog_source",
        lambda *_args, **_kwargs: [
            {
                "server_id": "server-a",
                "catalog_instance_id": "catalog-a",
                "catalog": {
                    "generation": 3,
                    "status": "complete",
                    "completed_at": None,
                },
            }
        ],
    )
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, server_id, queue="default": calls.append((func, server_id, queue)),
    )

    response = plugin_client(mod).post(
        "/api/catalog/refresh",
        json={"server_id": "server-a", "catalog_instance_id": "catalog-a"},
    )

    assert response.status_code == 202
    assert response.get_json()["status"] == "queued"
    assert calls == [(mod.catalog_refresh_task, "server-a", "default")]
    assert "secret" not in response.get_data(as_text=True)
    assert "internal.invalid" not in response.get_data(as_text=True)


@pytest.mark.parametrize(
    ("version", "expected_status"),
    [("v2.5.0", "core_too_old"), ("v4.0.0", "core_untested")],
)
def test_catalog_health_rejects_unsupported_core_before_server_work(monkeypatch, version, expected_status):
    mod = load_plugin()
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", version)

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 409
    assert response.get_json()["status"] == expected_status
    assert response.get_json()["servers"] == []


def test_collections_api_is_hidden_until_enabled():
    mod = load_plugin()
    client = plugin_client(mod)

    response = client.get("/api/collections")

    assert response.status_code == 404
    assert response.get_json() == {"error": "collection_manager_disabled"}


def test_collection_principal_is_per_user_but_bearer_is_global():
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    app = Flask(__name__)
    with app.test_request_context("/"):
        g.auth_method = "session"
        g.auth_user = "alice"
        assert collections.current_principal() == "user:alice"
        g.auth_method = "bearer"
        g.auth_user = None
        assert collections.current_principal() == collections.GLOBAL_PRINCIPAL


def test_collection_principal_fails_closed_for_session_without_username():
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    app = Flask(__name__)
    with app.test_request_context("/"):
        g.auth_method = "session"
        g.auth_user = None
        with pytest.raises(Exception) as exc_info:
            collections.current_principal()
        assert getattr(exc_info.value, "code", None) == 401


def _collection_backup_fixture(collections):
    rows = [
        {
            "id": "collection-source",
            "name": "Sunday Records",
            "description": "Slow mornings",
            "created_at": "2026-07-18T10:00:00Z",
            "updated_at": "2026-07-18T11:00:00Z",
            "items": [
                {
                    "id": "item-source",
                    "collection_id": "collection-source",
                    "kind": "track",
                    "track_id": "track-1",
                    "provider_album_id": None,
                    "album_key": None,
                    "title": "Roads",
                    "artist": "Portishead",
                    "album": "Dummy",
                    "cover_item_id": "track-1",
                    "position": 0,
                    "added_at": "2026-07-18T10:00:00Z",
                    "updated_at": "2026-07-18T10:00:00Z",
                }
            ],
        }
    ]
    return collections._backup_envelope(rows, "personal", "2026-07-18T12:00:00Z")


def test_collection_backup_is_versioned_checksummed_and_tamper_evident():
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    backup = _collection_backup_fixture(collections)

    assert backup["format"] == "lumae-living-collections"
    assert backup["version"] == 1
    assert backup["scope"] == "personal"
    assert backup["collection_count"] == 1
    assert backup["item_count"] == 1
    assert backup["checksum"].startswith("sha256:")

    restored = collections._normalize_backup_document(backup)
    assert restored[0]["name"] == "Sunday Records"
    assert restored[0]["items"][0]["track_id"] == "track-1"
    assert restored[0]["items"][0]["id"] != "item-source"

    missing_checksum = dict(backup)
    missing_checksum.pop("checksum")
    with pytest.raises(ValueError, match="checksum"):
        collections._normalize_backup_document(missing_checksum)

    backup["collections"][0]["name"] = "Tampered"
    with pytest.raises(ValueError, match="checksum"):
        collections._normalize_backup_document(backup)


def test_collection_backup_restore_rejects_duplicate_membership():
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    backup = _collection_backup_fixture(collections)
    duplicate = dict(backup["collections"][0]["items"][0])
    duplicate["id"] = "item-duplicate"
    backup["collections"][0]["items"].append(duplicate)
    backup["checksum"] = collections._backup_checksum(backup["collections"])

    with pytest.raises(ValueError, match="same media item"):
        collections._normalize_backup_document(backup)


def test_collection_backup_routes_are_scoped_to_the_authenticated_user(monkeypatch):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    seen = []

    def exported(principal, collection_id=None):
        seen.append((principal, collection_id))
        document = _collection_backup_fixture(collections)["collections"]
        document[0]["name"] = principal
        return document

    monkeypatch.setattr(collections, "get_setting", lambda key, default=None: True)
    monkeypatch.setattr(collections, "_export_principal_collections", exported)
    app = Flask(__name__)

    @app.before_request
    def authenticate_test_user():
        g.auth_method = "session"
        g.auth_user = "alice"

    app.register_blueprint(mod.bp)
    client = app.test_client()

    response = client.get("/api/collections/backup")
    single = client.get("/api/collections/collection-7/export")

    assert response.status_code == 200
    assert response.get_json()["collections"][0]["name"] == "user:alice"
    assert response.get_json()["scope"] == "personal"
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Content-Disposition"].startswith("attachment;")
    assert single.status_code == 200
    assert seen == [("user:alice", None), ("user:alice", "collection-7")]


def test_collection_restore_route_validates_checksum_and_uses_current_principal(
    monkeypatch,
):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    restored = []
    monkeypatch.setattr(collections, "get_setting", lambda key, default=None: True)
    monkeypatch.setattr(
        collections,
        "_restore_principal_collections",
        lambda principal, payload: (
            restored.append((principal, payload))
            or {
                "collections": [{"id": "restored-1", "name": payload[0]["name"]}],
                "collection_count": 1,
                "item_count": 1,
            }
        ),
    )
    app = Flask(__name__)

    @app.before_request
    def authenticate_test_user():
        g.auth_method = "session"
        g.auth_user = "bob"

    app.register_blueprint(mod.bp)
    client = app.test_client()
    backup = _collection_backup_fixture(collections)

    response = client.post("/api/collections/restore", json=backup)

    assert response.status_code == 201
    assert response.get_json()["restored"] is True
    assert restored[0][0] == "user:bob"
    assert restored[0][1][0]["items"][0]["id"] != "item-source"

    backup["collections"][0]["description"] = "changed after export"
    rejected = client.post("/api/collections/restore", json=backup)
    assert rejected.status_code == 400
    assert "checksum" in rejected.get_json()["error"]
    assert len(restored) == 1


def test_collection_restore_adds_new_records_and_sync_changes_without_overwrite(
    monkeypatch,
):
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")

    class RestoreCursor:
        def __init__(self):
            self.executed = []

        def execute(self, sql, params=None):
            self.executed.append((sql, params))

        def close(self):
            pass

    class RestoreDb:
        def __init__(self):
            self.cursor_obj = RestoreCursor()
            self.commits = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commits += 1

    db = RestoreDb()
    upserts = []
    changes = []
    monkeypatch.setattr(collections, "get_db", lambda: db)
    monkeypatch.setattr(
        collections,
        "_upsert_item",
        lambda cur, principal, collection_id, item: upserts.append((principal, collection_id, item.copy())),
    )
    monkeypatch.setattr(
        collections,
        "_fetch_collection",
        lambda cur, principal, collection_id: {
            "id": collection_id,
            "name": "Restored",
            "description": None,
            "revision": 2,
            "created_at": "created",
            "updated_at": "updated",
            "deleted_at": None,
            "album_count": 0,
            "track_count": 1,
        },
    )
    monkeypatch.setattr(
        collections,
        "_record_change",
        lambda cur, principal, collection_id, entity_kind, entity_id, operation, payload: changes.append(
            (principal, collection_id, entity_kind, entity_id, operation, payload)
        ),
    )
    payload = collections._normalize_backup_document(_collection_backup_fixture(collections))

    result = collections._restore_principal_collections("user:alice", payload)

    collection_inserts = [
        call for call in db.cursor_obj.executed if "INSERT INTO" in call[0] and "collections" in call[0]
    ]
    assert len(collection_inserts) == 1
    assert "ON CONFLICT" not in collection_inserts[0][0]
    assert collection_inserts[0][1][0] == "user:alice"
    assert collection_inserts[0][1][1] != "collection-source"
    assert upserts[0][0] == "user:alice"
    assert upserts[0][1] == collection_inserts[0][1][1]
    assert changes[0][2:5] == ("collection", collection_inserts[0][1][1], "upsert")
    assert changes[1][2:5] == ("item", upserts[0][2]["id"], "upsert")
    assert changes[1][5]["collection_revision"] == 2
    assert result["collection_count"] == 1
    assert result["item_count"] == 1
    assert db.commits == 1


def test_collection_library_normalizes_live_track_and_disc_numbers():
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")

    track = library._normalize_provider_track(
        {
            "Id": "track-7",
            "Name": "Reckoner",
            "AlbumArtist": "Radiohead",
            "Album": "In Rainbows",
            "IndexNumber": 7,
            "ParentIndexNumber": 2,
            "RunTimeTicks": 310_000_000,
        }
    )

    assert track["track_id"] == "track-7"
    assert track["track_number"] == 7
    assert track["disc_number"] == 2
    assert track["duration_seconds"] == 31


def test_album_detail_uses_provider_catalog_order_and_analysis_links(monkeypatch):
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    monkeypatch.setattr(
        library,
        "_score_album_tracks",
        lambda *args, **kwargs: [
            {
                "track_id": "track-2",
                "title": "Second",
                "artist": "Artist",
                "album": "Album",
                "track_number": 2,
                "disc_number": 1,
                "analyzed": False,
                "album_id": "album-1",
                "provider_type": "navidrome",
            },
            {
                "track_id": "track-1",
                "title": "First",
                "artist": "Artist",
                "album": "Album",
                "track_number": 1,
                "disc_number": 1,
                "analyzed": True,
                "album_id": "album-1",
                "provider_type": "navidrome",
            },
        ],
    )

    detail = library.album_detail("Album", "Artist", provider_album_id="album-1")

    assert detail["metadata_source"] == "provider_catalog"
    assert detail["album"]["provider_album_id"] == "album-1"
    assert [track["track_id"] for track in detail["tracks"]] == ["track-1", "track-2"]
    assert detail["tracks"][0]["track_number"] == 1
    assert detail["tracks"][0]["analyzed"] is True
    assert detail["tracks"][1]["analyzed"] is False


def test_lyrion_album_detail_requests_documented_track_and_disc_order(monkeypatch):
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    calls = []
    lyrion = types.ModuleType("tasks.mediaserver.lyrion")
    lyrion._jsonrpc_request = lambda command, params: (
        calls.append((command, params)) or {"titles_loop": [{"id": "7", "title": "Track", "track": 3, "disc": 2}]}
    )
    lyrion._lyrion_is_remote = lambda row: False
    mediaserver = types.ModuleType("tasks.mediaserver")
    mediaserver.lyrion = lyrion
    tasks = types.ModuleType("tasks")
    tasks.mediaserver = mediaserver
    monkeypatch.setitem(sys.modules, "tasks", tasks)
    monkeypatch.setitem(sys.modules, "tasks.mediaserver", mediaserver)
    monkeypatch.setitem(sys.modules, "tasks.mediaserver.lyrion", lyrion)

    rows = library._provider_album_tracks("lyrion", "album-4")

    assert rows[0]["track"] == 3
    assert rows[0]["disc"] == 2
    assert calls == [
        (
            "titles",
            [
                0,
                999999,
                "album_id:album-4",
                "tags:galduAyRJ",
                "sort:tracknum",
            ],
        )
    ]


def test_collection_library_route_forwards_scope_search_sort_and_artist(monkeypatch):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    captured = {}

    def fake_browse(**kwargs):
        captured.update(kwargs)
        return {"sections": {"albums": {"items": [], "total": 0}}}

    monkeypatch.setattr(collections, "get_setting", lambda key, default=None: True)
    monkeypatch.setattr(library, "browse_library", fake_browse)
    response = plugin_client(mod).get(
        "/api/collections/library?scope=albums&q=rain&artist=Radiohead&sort=year&page=2&limit=24"
    )

    assert response.status_code == 200
    assert captured == {
        "scope": "albums",
        "query": "rain",
        "artist": "Radiohead",
        "sort": "year",
        "page": "2",
        "limit": "24",
    }


def test_collection_library_rejects_broad_partial_queries_before_database_work(
    monkeypatch,
):
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    monkeypatch.setattr(
        library,
        "get_db",
        lambda: (_ for _ in ()).throw(AssertionError("short searches must not query score")),
    )

    result = library.browse_library(scope="all", query="ra")

    assert result["query"] == "ra"
    assert result["sections"] == {
        "albums": {"items": [], "total": 0},
        "tracks": {"items": [], "total": 0},
        "artists": {"items": [], "total": 0},
    }


def test_collection_track_sorts_use_source_columns_not_nested_select_aliases():
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")

    class CaptureCursor:
        description = []

        def __init__(self):
            self.queries = []

        def execute(self, sql, params):
            self.queries.append(sql)

        def fetchall(self):
            return []

    cursor = CaptureCursor()
    for sort in library.LIBRARY_SORTS:
        library._browse_tracks(cursor, "", None, sort, 12, 0)

    orders = [sql.rsplit("ORDER BY", 1)[1].split("LIMIT", 1)[0] for sql in cursor.queries]
    assert all("lower(artist)" not in order for order in orders)
    assert all("author" in order for order in orders)


def test_collection_batch_remove_applies_one_revision_and_one_commit(monkeypatch):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")

    class BatchDeleteCursor:
        def __init__(self):
            self.description = []
            self.rows = []
            self.collection_reads = 0

        def execute(self, sql, params=None):
            if "SELECT c.id, c.name" in sql:
                self.collection_reads += 1
                self.description = [
                    ("id",),
                    ("name",),
                    ("description",),
                    ("revision",),
                    ("created_at",),
                    ("updated_at",),
                    ("deleted_at",),
                    ("album_count",),
                    ("track_count",),
                ]
                revision = 1 if self.collection_reads == 1 else 2
                self.rows = [
                    (
                        "collection-1",
                        "Test",
                        None,
                        revision,
                        "created",
                        "updated",
                        None,
                        0,
                        0,
                    )
                ]
            elif "DELETE FROM" in sql and "id = ANY" in sql:
                self.rows = [("item-1",), ("item-2",)]
                self.description = [("id",)]
            else:
                self.rows = []

        def fetchone(self):
            return self.rows[0] if self.rows else None

        def fetchall(self):
            return list(self.rows)

        def close(self):
            pass

    class BatchDeleteDb:
        def __init__(self):
            self.cursor_obj = BatchDeleteCursor()
            self.commits = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commits += 1

    db = BatchDeleteDb()
    monkeypatch.setattr(collections, "get_setting", lambda key, default=None: True)
    monkeypatch.setattr(collections, "get_db", lambda: db)

    response = plugin_client(mod).delete(
        "/api/collections/collection-1/items/batch",
        json={"item_ids": ["item-1", "item-2"], "base_revision": 1},
    )

    assert response.status_code == 200
    assert response.get_json()["deleted"] == ["item-1", "item-2"]
    assert response.get_json()["collection"]["revision"] == 2
    assert db.commits == 1


def test_collection_batch_remove_rejects_non_list_ids(monkeypatch):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    monkeypatch.setattr(collections, "get_setting", lambda key, default=None: True)

    response = plugin_client(mod).delete(
        "/api/collections/collection-1/items/batch",
        json={"item_ids": "item-1"},
    )

    assert response.status_code == 400
    assert response.get_json() == {"error": "item_ids must be a list"}


def test_collection_preview_target_keeps_provider_credentials_server_side(monkeypatch):
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    monkeypatch.setattr(library.config, "MEDIASERVER_TYPE", "jellyfin", raising=False)
    monkeypatch.setattr(library.config, "JELLYFIN_URL", "https://music.example", raising=False)
    monkeypatch.setattr(
        library.config,
        "HEADERS",
        {"Authorization": 'MediaBrowser Token="secret"'},
        raising=False,
    )

    target, error = library._resolve_stream_target("track-1")

    assert error is None
    assert target[0] == "https://music.example/Items/track-1/Download"
    assert target[1] == {"Authorization": 'MediaBrowser Token="secret"'}
    assert "secret" not in target[0]


def test_collection_preview_uses_emby_base_url_without_legacy_prefix(monkeypatch):
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    monkeypatch.setattr(library.config, "MEDIASERVER_TYPE", "emby", raising=False)
    monkeypatch.setattr(library.config, "EMBY_URL", "https://emby.example", raising=False)
    monkeypatch.setattr(
        library.config,
        "HEADERS",
        {"X-Emby-Token": "server-secret"},
        raising=False,
    )

    target, error = library._resolve_stream_target("track-2")
    art_target = library._resolve_art_target("track-2", 480)

    assert error is None
    assert target[0] == "https://emby.example/Items/track-2/Download"
    assert art_target[0] == "https://emby.example/Items/track-2/Images/Primary"
    assert target[1] == {"X-Emby-Token": "server-secret"}


def test_enabled_collections_api_lists_mixed_item_counts(monkeypatch):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    db = FakeDb(
        rows=[
            (
                "collection-1",
                "Sunday Records",
                "Slow mornings",
                4,
                "2026-07-15T10:00:00Z",
                "2026-07-15T11:00:00Z",
                None,
                2,
                3,
            )
        ]
    )
    db.cursor_obj.description = [
        ("id",),
        ("name",),
        ("description",),
        ("revision",),
        ("created_at",),
        ("updated_at",),
        ("deleted_at",),
        ("album_count",),
        ("track_count",),
    ]
    monkeypatch.setattr(collections, "get_setting", lambda key, default=None: True)
    monkeypatch.setattr(collections, "get_db", lambda: db)
    client = plugin_client(mod)

    response = client.get("/api/collections")

    assert response.status_code == 200
    assert response.get_json()["collections"][0] == {
        "id": "collection-1",
        "name": "Sunday Records",
        "description": "Slow mornings",
        "revision": 4,
        "created_at": "2026-07-15T10:00:00Z",
        "updated_at": "2026-07-15T11:00:00Z",
        "deleted_at": None,
        "album_count": 2,
        "track_count": 3,
    }


def test_profiles_endpoint_splits_ready_missing_and_failed(monkeypatch):
    mod = load_plugin()
    rows = [
        {
            "track_id": "ready-1",
            "sample_rate": 44100,
            "duration_ms": 123000,
            "ref_lufs": -13.25,
            "start_ramp": b"\xe9\x03\x00",
            "end_ramp": b"\xe9\x04\x00",
            "analyzer_ver": 1,
            "analyzed_at": "2026-07-06T12:00:00Z",
            "media_signature": "sig-ready",
            "status": "ready",
            "last_error": None,
        },
        {
            "track_id": "failed-1",
            "sample_rate": 0,
            "duration_ms": 0,
            "ref_lufs": 0,
            "start_ramp": b"",
            "end_ramp": b"",
            "analyzer_ver": 1,
            "analyzed_at": "2026-07-06T12:00:00Z",
            "media_signature": "sig-failed",
            "status": "failed",
            "last_error": "decode failed",
        },
        {
            "track_id": "skipped-1",
            "sample_rate": 0,
            "duration_ms": 0,
            "ref_lufs": 0,
            "start_ramp": b"",
            "end_ramp": b"",
            "analyzer_ver": 1,
            "analyzed_at": "2026-07-06T12:00:00Z",
            "media_signature": None,
            "status": "skipped_no_file",
            "last_error": "missing file path",
        },
    ]
    monkeypatch.setattr(mod, "fetch_profile_rows", lambda ids: rows)
    client = plugin_client(mod)

    response = client.get("/api/profiles?ids=ready-1,missing-1,failed-1,skipped-1")

    assert response.status_code == 200
    body = response.get_json()
    assert body["schema_version"] == 1
    assert body["analyzer_version"] == 1
    assert body["profiles"][0]["track_id"] == "ready-1"
    assert body["profiles"][0]["source"] == "waveform"
    assert body["profiles"][0]["start_ramp"] == "6QMA"
    assert body["missing"] == ["missing-1"]
    assert body["failed"] == [
        {"track_id": "failed-1", "reason": "decode failed"},
        {"track_id": "skipped-1", "reason": "missing file path"},
    ]


def test_analyze_endpoint_enqueues_only_missing_or_stale_ids(monkeypatch):
    mod = load_plugin()
    calls = []
    rows = [
        {"track_id": "ready-1", "status": "ready"},
        {"track_id": "pending-1", "status": "pending"},
        {"track_id": "stale-1", "status": "stale"},
    ]
    monkeypatch.setattr(mod, "fetch_profile_rows", lambda ids: rows)
    monkeypatch.setattr(mod, "mark_pending", lambda ids: calls.append(("mark_pending", ids, "default")))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, ids, queue="default": calls.append((func.__name__, ids, queue)),
    )
    client = plugin_client(mod)

    response = client.post(
        "/api/analyze",
        json={"ids": ["ready-1", "pending-1", "stale-1", "missing-1"]},
    )

    assert response.status_code == 202
    assert response.get_json() == {
        "accepted": ["stale-1", "missing-1"],
        "already_ready": ["ready-1"],
        "already_pending": ["pending-1"],
    }
    assert calls == [
        ("mark_pending", ["stale-1", "missing-1"], "default"),
        ("analyze_tracks_task", ["stale-1", "missing-1"], "default"),
    ]


def test_analyze_song_hook_uses_analysis_audio_path_and_raw_media_item(monkeypatch, tmp_path):
    mod = load_plugin()
    audio = tmp_path / "analysis-hook.flac"
    audio.write_bytes(b"hook audio")
    db = FakeDb(rows=[])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    class Result:
        sample_rate = 44100
        duration_ms = 2500
        ref_lufs = -15.5
        start_ramp_blob = b"\x01\x02\x03"
        end_ramp_blob = b"\x04\x05\x06"

    seen = {}

    def fake_analyze_file(path):
        seen["path"] = path
        return Result()

    monkeypatch.setattr(mod, "analyze_file", fake_analyze_file)

    result = mod.analyze_song_hook(
        {
            "item_id": "track-a",
            "audio_path": str(audio),
            "metadata": {"file_path": "/metadata/path.flac"},
            "media_item": {"Id": "raw-track", "FilePath": "/music/raw-song.flac"},
        }
    )

    assert result == {"track_id": "track-a", "status": "ready"}
    assert seen["path"] == str(audio)
    assert audio.exists()
    params = db.cursor_obj.executed[-1][1]
    assert params[0] == "track-a"
    assert params[1] == 44100
    assert params[8].startswith("analysis-hook|track-a|/music/raw-song.flac|")
    assert params[10] == "ready"


def test_encode_ramp_matches_lumae_byte_layout():
    from plugins.LumaeAnalysis.ramp_codec import encode_ramp

    assert encode_ramp([(-17, 3), (0, 513)]) == bytes([239, 3, 0, 0, 1, 2])


def test_analyze_buffer_produces_waveform_profile():
    from plugins.LumaeAnalysis.loudness import analyze_buffer

    sr = 48000
    t = np.arange(sr * 2, dtype=np.float32) / sr
    audio = np.sin(2 * np.pi * 440 * t).astype(np.float32) * 0.25

    result = analyze_buffer(audio, sr)

    assert result.sample_rate == sr
    assert result.duration_ms == 2000
    assert math.isfinite(result.ref_lufs)
    assert result.start_ramp
    assert result.end_ramp
    assert result.start_ramp_blob
    assert result.end_ramp_blob


def test_apply_biquad_uses_vectorized_scipy(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    calls = []

    def fake_lfilter(b, a, samples):
        calls.append((b, a, samples.copy()))
        return np.asarray(samples, dtype=np.float64)

    monkeypatch.setattr(loudness.scipy_signal, "lfilter", fake_lfilter)
    samples = np.linspace(-0.5, 0.5, 32, dtype=np.float32)

    result = loudness._apply_biquad(samples, loudness.KWEIGHT_STAGE1)

    assert len(calls) == 1
    assert result.dtype == np.float64


def test_vectorized_biquad_matches_reference_recurrence():
    import plugins.LumaeAnalysis.loudness as loudness

    samples = np.random.default_rng(42).normal(0, 0.2, 4096).astype(np.float32)
    coefs = loudness.KWEIGHT_STAGE1
    b0, b1, b2, a1, a2 = coefs
    expected = np.empty(samples.shape[0], dtype=np.float64)
    x1 = x2 = y1 = y2 = 0.0
    for index, x0 in enumerate(samples.astype(np.float64, copy=False)):
        y0 = b0 * x0 + b1 * x1 + b2 * x2 - a1 * y1 - a2 * y2
        expected[index] = y0
        x2, x1 = x1, x0
        y2, y1 = y1, y0

    actual = loudness._apply_biquad(samples, coefs)

    np.testing.assert_allclose(actual, expected, rtol=1e-11, atol=1e-11)


def test_vectorized_analysis_matches_reference_profile(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    def reference_apply(channel, coefs):
        b0, b1, b2, a1, a2 = coefs
        output = np.empty(channel.shape[0], dtype=np.float64)
        x1 = x2 = y1 = y2 = 0.0
        for index, x0 in enumerate(channel.astype(np.float64, copy=False)):
            y0 = b0 * x0 + b1 * x1 + b2 * x2 - a1 * y1 - a2 * y2
            output[index] = y0
            x2, x1 = x1, x0
            y2, y1 = y1, y0
        return output

    def reference_k_weight(channel):
        stage1 = reference_apply(channel, loudness.KWEIGHT_STAGE1)
        return reference_apply(stage1, loudness.KWEIGHT_STAGE2)

    sample_rate = 48000
    seconds = 2
    time_axis = np.arange(sample_rate * seconds, dtype=np.float32) / sample_rate
    envelope = np.linspace(0.02, 0.5, time_axis.size, dtype=np.float32)
    audio = np.stack(
        [
            envelope * np.sin(2 * np.pi * 220 * time_axis),
            envelope[::-1] * np.sin(2 * np.pi * 440 * time_axis),
        ]
    ).astype(np.float32)

    vectorized = loudness.analyze_buffer(audio, sample_rate)
    monkeypatch.setattr(loudness, "_k_weight", reference_k_weight)
    reference = loudness.analyze_buffer(audio, sample_rate)

    assert vectorized.sample_rate == reference.sample_rate
    assert vectorized.duration_ms == reference.duration_ms
    assert math.isclose(vectorized.ref_lufs, reference.ref_lufs, rel_tol=0, abs_tol=1e-10)
    assert vectorized.start_ramp == reference.start_ramp
    assert vectorized.end_ramp == reference.end_ramp
    assert vectorized.start_ramp_blob == reference.start_ramp_blob
    assert vectorized.end_ramp_blob == reference.end_ramp_blob


def test_analyze_buffer_uses_100ms_chunks_and_expected_ramp_encoding(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    monkeypatch.setattr(
        loudness,
        "_k_weight",
        lambda channel: channel.astype(np.float64, copy=False),
    )
    monkeypatch.setattr(loudness, "_integrated_lufs", lambda chunk_lufs: -20.0)

    audio = np.array([0.0, 0.1, 0.31622777, 1.0, 1.9952623], dtype=np.float32)

    result = loudness.analyze_buffer(audio, 10)

    assert result.sample_rate == 10
    assert result.duration_ms == 500
    assert result.start_ramp == [
        (-90, 1),
        (-60, 1),
        (-40, 1),
        (-30, 1),
        (-24, 1),
        (-21, 1),
        (-18, 1),
        (-15, 1),
        (-12, 1),
        (-9, 1),
        (-6, 1),
        (-3, 1),
        (0, 2),
        (3, 2),
        (6, 2),
    ]
    assert result.end_ramp == [
        (-90, 0),
        (-60, 0),
        (-40, 0),
        (-30, 0),
        (-24, 0),
        (-21, 0),
        (-18, 0),
        (-15, 0),
        (-12, 0),
        (-9, 0),
        (-6, 0),
        (-3, 0),
        (0, 0),
        (3, 0),
        (6, 0),
    ]
    assert result.start_ramp_blob == bytes(
        [
            166,
            1,
            0,
            196,
            1,
            0,
            216,
            1,
            0,
            226,
            1,
            0,
            232,
            1,
            0,
            235,
            1,
            0,
            238,
            1,
            0,
            241,
            1,
            0,
            244,
            1,
            0,
            247,
            1,
            0,
            250,
            1,
            0,
            253,
            1,
            0,
            0,
            2,
            0,
            3,
            2,
            0,
            6,
            2,
            0,
        ]
    )
    assert result.end_ramp_blob == bytes(
        [
            166,
            0,
            0,
            196,
            0,
            0,
            216,
            0,
            0,
            226,
            0,
            0,
            232,
            0,
            0,
            235,
            0,
            0,
            238,
            0,
            0,
            241,
            0,
            0,
            244,
            0,
            0,
            247,
            0,
            0,
            250,
            0,
            0,
            253,
            0,
            0,
            0,
            0,
            0,
            3,
            0,
            0,
            6,
            0,
            0,
        ]
    )


def test_analyze_buffer_includes_final_partial_chunk(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    monkeypatch.setattr(
        loudness,
        "_k_weight",
        lambda channel: channel.astype(np.float64, copy=False),
    )
    monkeypatch.setattr(loudness, "_integrated_lufs", lambda chunk_lufs: -20.0)

    audio = np.array([0.0, 0.0, 0.1, 0.1, 1.9952623], dtype=np.float32)

    result = loudness.analyze_buffer(audio, 20)

    assert result.duration_ms == 250
    assert result.start_ramp[-3:] == [(0, 2), (3, 2), (6, 2)]
    assert result.end_ramp[:3] == [(-90, 0), (-60, 0), (-40, 0)]
    assert result.end_ramp[-3:] == [(0, 0), (3, 0), (6, 0)]


def test_analyze_buffer_rejects_silent_audio():
    from plugins.LumaeAnalysis.loudness import SilentAudioError, analyze_buffer

    audio = np.zeros(48000, dtype=np.float32)

    try:
        analyze_buffer(audio, 48000)
    except SilentAudioError as exc:
        assert "silent or sub-gate" in str(exc)
    else:
        raise AssertionError("silent audio should fail")


def test_analyze_file_loads_audio_and_delegates_to_buffer(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    captured = {}
    audio = np.array([0.25, -0.25], dtype=np.float32)
    sentinel = object()

    def fake_load(path, sr=None, mono=False):
        captured["load"] = (path, sr, mono)
        return audio, 44100

    def fake_analyze_buffer(buffer, sample_rate):
        captured["analyze"] = (buffer, sample_rate)
        return sentinel

    monkeypatch.setattr(loudness, "librosa", types.SimpleNamespace(load=fake_load))
    monkeypatch.setattr(loudness, "analyze_buffer", fake_analyze_buffer)

    result = loudness.analyze_file("fixture.wav")

    assert result is sentinel
    assert captured["load"] == ("fixture.wav", None, False)
    assert captured["analyze"] == (audio, 44100)


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.description = [("item_id",), ("file_path",)]
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeDb:
    def __init__(self, rows=None):
        self.cursor_obj = FakeCursor(rows)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class LimitAwareCursor(FakeCursor):
    def fetchall(self):
        if not self.executed:
            return self.rows
        _, params = self.executed[-1]
        if params:
            return self.rows[: int(params[0])]
        return self.rows


class LimitAwareDb(FakeDb):
    def __init__(self, rows=None):
        self.cursor_obj = LimitAwareCursor(rows)
        self.commits = 0


class CronCursor(FakeCursor):
    def __init__(self, existing=None):
        super().__init__(rows=[])
        self.existing = existing

    def fetchone(self):
        return self.existing


class CronDb(FakeDb):
    def __init__(self, existing=None):
        self.cursor_obj = CronCursor(existing)
        self.commits = 0


class FakeCtx:
    def __init__(self):
        self.blueprints = []
        self.settings_endpoint = None
        self.install_hooks = []
        self.song_hooks = []
        self.cron_tasks = []
        self.tasks = []
        self.menu_items = []

    def add_blueprint(self, blueprint):
        self.blueprints.append(blueprint)

    def set_settings_page(self, endpoint):
        self.settings_endpoint = endpoint

    def add_menu_item(self, label, endpoint, admin_only=False):
        self.menu_items.append({"label": label, "endpoint": endpoint, "admin_only": admin_only})

    def on_install(self, func):
        self.install_hooks.append(func)

    def on_song_analyzed(self, func):
        self.song_hooks.append(func)

    def add_cron_task(self, name, func, queue="default"):
        self.cron_tasks.append((name, func, queue))

    def add_task(self, name, func, queue="default"):
        self.tasks.append((name, func, queue))


def test_analyze_one_track_marks_missing_file(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=[]))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    result = mod.analyze_one_track("missing")

    assert result == {"track_id": "missing", "status": "skipped_no_file"}


def test_analyze_one_track_downloads_from_media_server_when_local_file_missing(monkeypatch, tmp_path):
    mod = load_plugin()
    library_path = tmp_path / "not-mounted" / "album" / "song.flac"
    downloaded = tmp_path / "downloaded.flac"
    downloaded.write_bytes(b"downloaded media")
    db = FakeDb(rows=[("track-a", str(library_path), "Song Title", "Artist Name")])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)

    class Result:
        sample_rate = 48000
        duration_ms = 1000
        ref_lufs = -14.0
        start_ramp_blob = b"\xe9\x03\x00"
        end_ramp_blob = b"\xe9\x04\x00"

    downloaded_items = []
    seen = {}

    def fake_download_track_to_temp(item):
        downloaded_items.append(item)
        return str(downloaded)

    def fake_analyze_file(path):
        seen["path"] = path
        return Result()

    monkeypatch.setattr(mod, "download_track_to_temp", fake_download_track_to_temp, raising=False)
    monkeypatch.setattr(mod, "analyze_file", fake_analyze_file)

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "ready"}
    assert seen["path"] == str(downloaded)
    assert downloaded_items[0]["id"] == "track-a"
    assert downloaded_items[0]["Id"] == "track-a"
    assert downloaded_items[0]["path"] == str(library_path)
    assert downloaded_items[0]["Path"] == str(library_path)
    assert downloaded_items[0]["Name"] == "Song Title"
    assert downloaded_items[0]["suffix"] == "flac"
    assert not downloaded.exists()


def test_analyze_one_track_marks_media_server_download_failure_as_failed(monkeypatch, tmp_path):
    mod = load_plugin()
    library_path = tmp_path / "not-mounted" / "song.flac"
    db = FakeDb(rows=[("track-a", str(library_path), "Song Title", "Artist Name")])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)
    monkeypatch.setattr(mod, "download_track_to_temp", lambda item: None, raising=False)

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "failed"}
    assert db.cursor_obj.executed[-1][1][-1] == "media server download failed"


def test_analyze_one_track_cleans_downloaded_file_when_analysis_fails(monkeypatch, tmp_path):
    mod = load_plugin()
    library_path = tmp_path / "not-mounted" / "song.flac"
    downloaded = tmp_path / "downloaded.flac"
    downloaded.write_bytes(b"downloaded media")
    db = FakeDb(rows=[("track-a", str(library_path), "Song Title", "Artist Name")])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)
    monkeypatch.setattr(mod, "download_track_to_temp", lambda item: str(downloaded), raising=False)
    monkeypatch.setattr(
        mod,
        "analyze_file",
        lambda path: (_ for _ in ()).throw(RuntimeError("decode failed")),
    )

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "failed"}
    assert db.cursor_obj.executed[-1][1][-1] == "decode failed"
    assert not downloaded.exists()


def test_analyze_one_track_persists_ready_profile_with_pr721_score_shape(monkeypatch, tmp_path):
    mod = load_plugin()
    audio = tmp_path / "song.wav"
    audio.write_bytes(b"not really decoded in this test")
    db = FakeDb(rows=[("track-a", str(audio))])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    class Result:
        sample_rate = 48000
        duration_ms = 1000
        ref_lufs = -14.0
        start_ramp_blob = b"\xe9\x03\x00"
        end_ramp_blob = b"\xe9\x04\x00"

    seen = {}

    def fake_analyze_file(path):
        seen["path"] = path
        return Result()

    monkeypatch.setattr(mod, "analyze_file", fake_analyze_file)

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "ready"}
    assert seen["path"] == str(audio)
    select_sql = " ".join(db.cursor_obj.executed[0][0].split())
    assert "catalog_tracks" in select_sql
    assert "FROM score" not in select_sql
    assert db.commits == 1
    sql, params = db.cursor_obj.executed[-1]
    assert "INSERT INTO" in sql
    assert params[0] == "track-a"
    assert params[1] == 48000
    assert params[6] == mod.ANALYZER_VERSION
    assert params[7] == mod.SCHEMA_VERSION
    assert params[10] == "ready"


def test_analyze_one_track_persists_failed_profile(monkeypatch, tmp_path):
    mod = load_plugin()
    audio = tmp_path / "song.wav"
    audio.write_bytes(b"x")
    db = FakeDb(rows=[("track-a", str(audio))])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(
        mod,
        "analyze_file",
        lambda path: (_ for _ in ()).throw(RuntimeError("decode failed")),
    )

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "failed"}
    assert db.cursor_obj.executed[-1][1][-1] == "decode failed"


def test_find_backfill_ids_includes_missing_old_and_signature_changed_but_not_failed(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    unchanged = tmp_path / "unchanged.wav"
    unchanged.write_bytes(b"same media")
    unchanged_sig = mod.media_signature(str(unchanged))
    rows = [
        ("missing-profile", str(current), None, None, None),
        ("old-analyzer", str(current), "old-sig", 0, "ready"),
        ("changed-media", str(current), "old-sig", mod.ANALYZER_VERSION, "ready"),
        ("failed-once", str(current), "old-sig", mod.ANALYZER_VERSION, "failed"),
        (
            "unchanged-ready",
            str(unchanged),
            unchanged_sig,
            mod.ANALYZER_VERSION,
            "ready",
        ),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    assert mod.find_backfill_ids(limit=25) == [
        "missing-profile",
        "old-analyzer",
        "changed-media",
    ]


def test_find_backfill_ids_includes_explicit_stale_rows(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    missing = tmp_path / "not-mounted.wav"
    rows = [
        ("stale-track", str(current), "same-sig", mod.ANALYZER_VERSION, "stale"),
        ("failed-once", str(current), "same-sig", mod.ANALYZER_VERSION, "failed"),
        (
            "skipped-once",
            str(missing),
            "same-sig",
            mod.ANALYZER_VERSION,
            "skipped_no_file",
        ),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.find_backfill_ids(limit=25) == ["stale-track"]


def test_find_backfill_ids_retries_skipped_no_file_when_downloader_configured(monkeypatch, tmp_path):
    mod = load_plugin()
    missing = tmp_path / "not-mounted.wav"
    rows = [
        ("skipped-once", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)

    assert mod.find_backfill_ids(limit=25) == ["skipped-once"]


def test_find_backfill_ids_retries_skipped_no_file_when_local_file_appears(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    rows = [
        ("skipped-once", str(current), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.find_backfill_ids(limit=25) == ["skipped-once"]


def test_find_backfill_ids_applies_limit_after_eligibility_filtering(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    missing = tmp_path / "not-mounted.wav"
    sig = mod.media_signature(str(current))
    rows = [
        ("ready-current-1", str(current), sig, mod.ANALYZER_VERSION, "ready"),
        ("failed-once", str(current), "old-sig", mod.ANALYZER_VERSION, "failed"),
        ("skipped-once", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
        ("eligible-missing", str(current), None, None, None),
        ("eligible-stale", str(current), sig, mod.ANALYZER_VERSION, "stale"),
        ("eligible-old", str(current), sig, 0, "ready"),
    ]
    db = LimitAwareDb(rows=rows)
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.find_backfill_ids(limit=2) == ["eligible-missing", "eligible-stale"]


def test_backfill_uses_configured_batch_size(monkeypatch):
    mod = load_plugin()
    seen_limits = []
    monkeypatch.setattr(
        mod,
        "get_setting",
        lambda key, default=None: 7 if key == "backfill_batch_size" else default,
    )
    monkeypatch.setattr(mod, "find_backfill_ids", lambda limit: seen_limits.append(limit) or [])

    assert mod.backfill_missing_profiles() == {"ready": 0, "failed": 0, "skipped": 0}
    assert seen_limits == [7]


def test_analysis_status_counts_current_pending_failed_and_needed(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    missing = tmp_path / "not-mounted.wav"
    unchanged = tmp_path / "unchanged.wav"
    unchanged.write_bytes(b"same media")
    unchanged_sig = mod.media_signature(str(unchanged))
    rows = [
        ("ready-current", str(unchanged), unchanged_sig, mod.ANALYZER_VERSION, "ready"),
        ("missing-profile", str(current), None, None, None),
        ("old-analyzer", str(current), "old-sig", 0, "ready"),
        ("changed-media", str(current), "old-sig", mod.ANALYZER_VERSION, "ready"),
        ("pending-track", str(current), None, mod.ANALYZER_VERSION, "pending"),
        ("failed-track", str(current), None, mod.ANALYZER_VERSION, "failed"),
        ("skipped-track", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.analysis_status_counts() == {
        "total_with_files": 7,
        "ready_current": 1,
        "pending": 1,
        "failed": 1,
        "skipped": 1,
        "needs_analysis": 3,
    }


def test_analysis_status_counts_treats_retryable_skipped_rows_as_needed(monkeypatch, tmp_path):
    mod = load_plugin()
    missing = tmp_path / "not-mounted.wav"
    rows = [
        ("skipped-track", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)

    assert mod.analysis_status_counts() == {
        "total_with_files": 1,
        "ready_current": 0,
        "pending": 0,
        "failed": 0,
        "skipped": 0,
        "needs_analysis": 1,
    }


def test_queue_backfill_batch_marks_pending_and_enqueues_next_batch(monkeypatch):
    mod = load_plugin()
    calls = []

    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)
    monkeypatch.setattr(
        mod,
        "find_backfill_ids",
        lambda limit: calls.append(("find", limit)) or ["a", "b"],
    )
    monkeypatch.setattr(mod, "mark_pending", lambda ids: calls.append(("mark_pending", ids)))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, ids, queue="default": calls.append((func.__name__, ids, queue)),
    )

    assert mod.queue_backfill_batch() == {"queued": 2, "limit": 3}
    assert calls == [
        ("find", 3),
        ("mark_pending", ["a", "b"]),
        ("analyze_tracks_task", ["a", "b"], "default"),
    ]


def test_queue_whole_library_splits_every_candidate_into_250_track_jobs(monkeypatch):
    mod = load_plugin()
    calls = []
    ids = [f"track-{i}" for i in range(601)]

    monkeypatch.setattr(mod, "find_all_backfill_ids", lambda: ids)
    monkeypatch.setattr(mod, "mark_pending", lambda chunk: calls.append(("mark_pending", chunk)))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, chunk, queue="default": calls.append((func.__name__, chunk, queue)),
    )

    assert mod.queue_whole_library() == {"queued": 601, "jobs": 3, "chunk_size": 250}
    assert calls == [
        ("mark_pending", ids[:250]),
        ("analyze_tracks_task", ids[:250], "default"),
        ("mark_pending", ids[250:500]),
        ("analyze_tracks_task", ids[250:500], "default"),
        ("mark_pending", ids[500:]),
        ("analyze_tracks_task", ids[500:], "default"),
    ]


def test_queue_whole_library_does_not_enqueue_when_no_candidates(monkeypatch):
    mod = load_plugin()
    calls = []

    monkeypatch.setattr(mod, "find_all_backfill_ids", lambda: [])
    monkeypatch.setattr(mod, "mark_pending", lambda chunk: calls.append(("mark_pending", chunk)))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, chunk, queue="default": calls.append((func.__name__, chunk, queue)),
    )

    assert mod.queue_whole_library() == {"queued": 0, "jobs": 0, "chunk_size": 250}
    assert calls == []


def test_migrate_disables_legacy_backfill_schedule(monkeypatch):
    mod = load_plugin()
    db = CronDb(existing=None)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    mod.migrate(db)

    assert db.commits == 1
    assert db.cursor_obj.executed[-1] == (
        "UPDATE cron SET enabled=FALSE WHERE task_type=%s",
        (mod.BACKFILL_TASK_TYPE,),
    )
    cron_inserts = [params for sql, params in db.cursor_obj.executed if "INSERT INTO cron" in sql]
    assert cron_inserts == [
        (
            mod.CATALOG_REFRESH_TASK_TYPE,
            mod.CATALOG_REFRESH_TASK_TYPE,
            "17 */6 * * *",
        ),
        (
            mod.ANALYSIS_PROJECTION_TASK_TYPE,
            mod.ANALYSIS_PROJECTION_TASK_TYPE,
            "47 */6 * * *",
        ),
    ]
    migration_sql = "\n".join(sql for sql, _params in db.cursor_obj.executed)
    assert "plugin_lumae_analysis__collections" in migration_sql
    assert "plugin_lumae_analysis__collection_items" in migration_sql
    assert "plugin_lumae_analysis__collection_changes" in migration_sql
    for table_name in (
        "catalog_sources",
        "catalog_state",
        "catalog_libraries",
        "catalog_artists",
        "catalog_albums",
        "catalog_tracks",
        "catalog_track_artists",
        "catalog_album_artists",
        "catalog_entity_libraries",
        "catalog_changes",
        "catalog_scans",
        "stream_bootstrap_sessions",
        "analysis_state",
        "analysis_items",
        "track_analysis_links",
        "analysis_changes",
    ):
        assert f"plugin_lumae_analysis__{table_name}" in migration_sql


def test_migrate_is_idempotent_and_preserves_existing_plugin_tables(monkeypatch):
    mod = load_plugin()
    db = CronDb(existing=None)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    mod.migrate(db)
    first_sql = [sql for sql, _params in db.cursor_obj.executed]
    mod.migrate(db)

    assert db.commits == 2
    assert all("DROP TABLE" not in sql.upper() for sql, _params in db.cursor_obj.executed)
    assert sum("CREATE TABLE IF NOT EXISTS" in sql.upper() for sql, _ in db.cursor_obj.executed) == (
        2 * sum("CREATE TABLE IF NOT EXISTS" in sql.upper() for sql in first_sql)
    )


def test_core_adapters_normalize_equivalent_v2_and_v3_analysis_events(monkeypatch):
    from plugins.LumaeAnalysis.core_v2 import AudioMuseV2Adapter
    from plugins.LumaeAnalysis.core_v3 import AudioMuseV3Adapter

    v2 = AudioMuseV2Adapter().normalize_analysis_hook({"item_id": "provider-track"})
    v3 = AudioMuseV3Adapter().normalize_analysis_hook({"item_id": "provider-track", "server_id": "server-a"})

    assert v2["provider_track_id"] == v3["provider_track_id"] == "provider-track"
    assert v2["server_id"] == "legacy-default"
    assert v3["server_id"] == "server-a"


def test_provider_bridge_never_exposes_credentials_or_urls():
    from plugins.LumaeAnalysis.catalog_providers import ProviderCatalogBridge

    class Adapter:
        def list_servers(self):
            return [
                {
                    "server_id": "one",
                    "name": "Private",
                    "provider_type": "navidrome",
                    "is_default": True,
                    "url": "https://secret.invalid",
                    "creds": {"token": "secret"},
                }
            ]

    assert ProviderCatalogBridge(Adapter()).list_servers() == [
        {
            "server_id": "one",
            "name": "Private",
            "provider_type": "navidrome",
            "is_default": True,
            "supported": True,
        }
    ]


class RebindCursor(FakeCursor):
    def __init__(self, source_rows, selected_source=None):
        super().__init__(source_rows)
        self.source_rows = source_rows
        self.selected_source = selected_source

    def execute(self, sql, params=None):
        super().execute(sql, params)
        if "FOR UPDATE" in sql:
            self.rows = [self.selected_source] if self.selected_source else []
        elif "SELECT catalog_instance_id" in sql:
            self.rows = self.source_rows


class RebindDb(FakeDb):
    def __init__(self, source_rows, selected_source=None):
        self.cursor_obj = RebindCursor(source_rows, selected_source)
        self.commits = 0


def test_v2_source_requires_proven_continuity_before_v3_rebind(monkeypatch):
    from plugins.LumaeAnalysis.catalog import (
        accept_legacy_rebind,
        ensure_catalog_sources,
    )

    source_id = "stable-catalog-id"
    db = RebindDb(
        [(source_id, "legacy-default", "navidrome", "active")],
        selected_source=("legacy-default", "rebind_required"),
    )

    class V3Bridge:
        def list_servers(self):
            return [
                {
                    "server_id": "server-a",
                    "name": "Same server",
                    "provider_type": "navidrome",
                    "is_default": True,
                }
            ]

    sources = ensure_catalog_sources(db, bridge=V3Bridge())
    assert sources[0]["catalog_instance_id"] == source_id
    assert sources[0]["candidate_core_server_id"] == "server-a"

    with pytest.raises(ValueError, match="continuity evidence"):
        accept_legacy_rebind(db, source_id, "server-a", {"provider_type": True})

    accepted = accept_legacy_rebind(
        db,
        source_id,
        "server-a",
        {
            "provider_type": True,
            "provider_instance": True,
            "library_scope": True,
            "provider_sample": True,
        },
    )
    assert accepted is True
    assert any(params == ("server-a", source_id) and "catalog_sources" in sql for sql, params in db.cursor_obj.executed)


def test_catalog_scope_evidence_is_order_independent_and_scope_sensitive():
    from plugins.LumaeAnalysis.catalog import (
        catalog_scope_evidence,
        verify_library_scope,
    )

    first = {
        "libraries": [{"library_id": "library-b"}, {"library_id": "library-a"}],
        "tracks": [{"track_id": "track-2"}, {"track_id": "track-1"}],
        "entity_libraries": [
            {"entity_type": "track", "entity_id": "track-2", "library_id": "library-b"},
            {"entity_type": "track", "entity_id": "track-1", "library_id": "library-a"},
        ],
    }
    reordered = {
        "libraries": list(reversed(first["libraries"])),
        "tracks": list(reversed(first["tracks"])),
        "entity_libraries": list(reversed(first["entity_libraries"])),
    }
    changed_scope = {**reordered, "entity_libraries": [*reordered["entity_libraries"]]}
    changed_scope["entity_libraries"][0] = {
        "entity_type": "track",
        "entity_id": "track-1",
        "library_id": "library-b",
    }

    expected = catalog_scope_evidence(first, "navidrome")
    assert catalog_scope_evidence(reordered, "navidrome") == expected
    assert catalog_scope_evidence(changed_scope, "navidrome")["library_scope_fp"] != expected["library_scope_fp"]
    assert "track-1" not in str(expected)
    assert "library-a" not in str(expected)

    db = FakeDb([(expected["scope_summary"],)])
    assert verify_library_scope(db, "catalog-a", ["library-b", "library-a"]) == {
        "verified": True,
        "library_verified": True,
        "expected_count": 2,
        "submitted_count": 2,
        "evidence_available": True,
    }
    assert verify_library_scope(db, "catalog-a", ["library-a"])["verified"] is False


def test_catalog_scope_requires_a_sufficient_direct_provider_track_sample():
    from plugins.LumaeAnalysis.catalog import verify_library_scope

    summary = {
        "library_count": 1,
        "track_count": 20,
        "library_ids_fp": "",
    }
    from plugins.LumaeAnalysis.catalog import fingerprint

    summary["library_ids_fp"] = fingerprint({"library_ids": ["1"]})

    class ScopeCursor(FakeCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "COUNT(DISTINCT ct.track_id)" in sql:
                submitted = params[0]
                matches = len([track_id for track_id in submitted if track_id != "wrong-server-track"])
                self.rows = [(7, matches)]
            else:
                self.rows = [(summary,)]

    class ScopeDb(FakeDb):
        def __init__(self):
            self.cursor_obj = ScopeCursor()
            self.commits = 0

    matching = [f"track-{index}" for index in range(12)]
    verified = verify_library_scope(ScopeDb(), "catalog-a", ["1"], matching)
    assert verified == {
        "verified": True,
        "library_verified": True,
        "expected_count": 1,
        "submitted_count": 1,
        "evidence_available": True,
        "track_evidence_available": True,
        "tracks_verified": True,
        "expected_track_count": 20,
        "required_track_count": 12,
        "submitted_track_count": 12,
        "matched_track_count": 12,
        "sample_sufficient": True,
    }

    wrong_server = verify_library_scope(
        ScopeDb(), "catalog-a", ["1"], [*matching[:-1], "wrong-server-track"]
    )
    assert wrong_server["verified"] is False
    assert wrong_server["library_verified"] is True
    assert wrong_server["matched_track_count"] == 11
    assert wrong_server["tracks_verified"] is False

    too_small = verify_library_scope(ScopeDb(), "catalog-a", ["1"], matching[:4])
    assert too_small["verified"] is False
    assert too_small["sample_sufficient"] is False


def test_catalog_scope_endpoint_forwards_direct_provider_track_evidence(monkeypatch):
    mod = load_plugin()
    captured = {}

    def verify(db, catalog_instance_id, library_ids, provider_track_ids=None):
        captured.update(
            {
                "db": db,
                "catalog_instance_id": catalog_instance_id,
                "library_ids": library_ids,
                "provider_track_ids": provider_track_ids,
            }
        )
        return {"verified": True, "tracks_verified": True}

    db = object()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "verify_library_scope", verify)

    response = plugin_client(mod).post(
        "/api/catalog/verify-scope",
        json={
            "catalog_instance_id": "catalog-a",
            "library_ids": ["1"],
            "provider_track_ids": ["track-a", "track-b"],
        },
    )

    assert response.status_code == 200
    assert response.get_json() == {"verified": True, "tracks_verified": True}
    assert captured == {
        "db": db,
        "catalog_instance_id": "catalog-a",
        "library_ids": ["1"],
        "provider_track_ids": ["track-a", "track-b"],
    }


def test_automatic_rebind_accepts_only_an_exact_provider_projection(monkeypatch):
    import plugins.LumaeAnalysis.catalog as catalog

    raw = {
        "libraries": [{"id": "library-1", "name": "Music"}],
        "tracks": [{"id": "track-1", "title": "Song", "musicFolderId": "library-1"}],
    }
    normalized = catalog.normalize_provider_catalog(raw, "navidrome")
    stored = catalog.catalog_scope_evidence(normalized, "navidrome")

    class AttemptCursor(FakeCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "SELECT current_core_server_id" in sql:
                self.rows = [("legacy-default", "server-a", "navidrome", "rebind_required")]
            else:
                self.rows = []

    class AttemptDb(FakeDb):
        def __init__(self):
            self.cursor_obj = AttemptCursor()
            self.commits = 0

    class Bridge:
        def require_server(self, server_id):
            assert server_id == "server-a"
            return {"server_id": server_id, "provider_type": "navidrome"}

        def fetch_catalog(self, server_id):
            assert server_id == "server-a"
            return raw

    accepted = []
    monkeypatch.setattr(catalog, "_persisted_scope_evidence", lambda _db, _id: stored)
    monkeypatch.setattr(
        catalog,
        "accept_legacy_rebind",
        lambda _db, source_id, server_id, evidence: accepted.append((source_id, server_id, evidence)) or True,
    )
    db = AttemptDb()

    result = catalog.attempt_legacy_rebind(db, "catalog-a", "server-a", bridge=Bridge())

    assert result == {
        "status": "active",
        "rebound": True,
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
    }
    assert all(accepted[0][2].values())
    assert db.commits == 1

    changed = catalog.catalog_scope_evidence(
        catalog.normalize_provider_catalog(
            {**raw, "tracks": [*raw["tracks"], {"id": "track-2", "title": "New"}]},
            "navidrome",
        ),
        "navidrome",
    )
    monkeypatch.setattr(catalog, "_persisted_scope_evidence", lambda _db, _id: changed)
    accepted.clear()
    blocked = catalog.attempt_legacy_rebind(AttemptDb(), "catalog-a", "server-a", bridge=Bridge())
    assert blocked["status"] == "rebind_required"
    assert accepted == []


@pytest.mark.parametrize(
    ("provider_type", "track"),
    [
        (
            "navidrome",
            {
                "id": "track-1",
                "title": "Song",
                "albumId": "album-1",
                "album": "Record",
                "artistId": "artist-1",
                "artist": "Artist",
                "track": 4,
                "discNumber": 2,
                "duration": 201.25,
                "suffix": "flac",
                "musicFolderId": "library-1",
                "path": "/never/send/this.flac",
            },
        ),
        (
            "jellyfin",
            {
                "Id": "track-1",
                "Name": "Song",
                "AlbumId": "album-1",
                "Album": "Record",
                "ArtistItems": [{"Id": "artist-1", "Name": "Artist"}],
                "IndexNumber": 4,
                "ParentIndexNumber": 2,
                "RunTimeTicks": 2_012_500_000,
                "MediaSources": [{"Container": "flac"}],
                "LibraryId": "library-1",
                "Path": "C:\\never-send\\this.flac",
                "UserData": {"PlayCount": 99},
            },
        ),
        (
            "emby",
            {
                "Id": "track-1",
                "Name": "Song",
                "ParentId": "album-1",
                "Album": "Record",
                "Artists": ["Artist"],
                "IndexNumber": 4,
                "ParentIndexNumber": 2,
                "RunTimeTicks": 2_012_500_000,
                "LibraryId": "library-1",
            },
        ),
        (
            "lyrion",
            {
                "id": "track-1",
                "title": "Song",
                "album_id": "album-1",
                "album": "Record",
                "artist": "Artist",
                "tracknum": 4,
                "discnumber": 2,
                "duration": 201.25,
                "url": "file:///never/send/this.flac",
            },
        ),
    ],
)
def test_provider_catalog_normalization_keeps_rich_order_and_strips_private_fields(provider_type, track):
    from plugins.LumaeAnalysis.catalog import canonical_json, normalize_provider_catalog

    normalized = normalize_provider_catalog(
        {
            "libraries": [{"id": "library-1", "name": "Music"}],
            "albums": [{"id": "album-1", "name": "Record", "AlbumArtist": "Artist"}],
            "tracks": [track],
        },
        provider_type,
    )
    row = normalized["tracks"][0]
    assert row["track_id"] == "track-1"
    assert row["album_id"] == "album-1"
    assert row["track_number"] == 4
    assert row["disc_number"] == 2
    assert row["duration_ms"] == 201250
    assert row["content_kind"] == "music"
    assert "never-send" not in canonical_json(row["payload"])
    assert "PlayCount" not in canonical_json(row["payload"])


def test_provider_catalog_accepts_v3_duration_seconds_field():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    normalized = normalize_provider_catalog(
        {
            "albums": [{"id": "album-1", "name": "Record"}],
            "tracks": [
                {
                    "id": "track-1",
                    "title": "Song",
                    "albumId": "album-1",
                    "DurationSeconds": 201.25,
                }
            ],
        },
        "navidrome",
    )

    assert normalized["tracks"][0]["duration_ms"] == 201250


def test_provider_catalog_publishes_relationships_and_rich_enrichment_in_stream_payloads():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    normalized = normalize_provider_catalog(
        {
            "libraries": [{"id": "library-1", "name": "Music"}],
            "albums": [
                {
                    "id": "album-1",
                    "name": "Record",
                    "AlbumArtist": "Artist",
                    "ProductionYear": 2026,
                    "Genres": ["Ambient"],
                }
            ],
            "tracks": [
                {
                    "id": "track-1",
                    "title": "Song",
                    "albumId": "album-1",
                    "album": "Record",
                    "ArtistItems": [{"Id": "artist-1", "Name": "Artist"}],
                    "musicFolderId": "library-1",
                    "tracknum": 7,
                    "discnumber": 2,
                    "discTitle": "Bonus Disc",
                    "trackTotal": 9,
                    "discTotal": 2,
                    "suffix": "flac",
                    "bitRate": 921000,
                    "sampleRate": 48000,
                    "bitDepth": 24,
                    "channelCount": 2,
                    "replayGain": {
                        "trackGain": -4.25,
                        "trackPeak": 0.91,
                        "albumGain": -3.75,
                    },
                    "musicBrainzId": "mb-track-1",
                    "isExplicit": True,
                }
            ],
        },
        "navidrome",
    )

    track = normalized["tracks"][0]
    rich = track["payload"]["_lumae"]
    assert track["track_number"] == 7
    assert track["disc_number"] == 2
    assert rich["disc_title"] == "Bonus Disc"
    assert rich["track_total"] == 9
    assert rich["disc_total"] == 2
    assert rich["audio_properties"] == {
        "duration_ms": None,
        "container": "flac",
        "bit_rate": 921000,
        "sample_rate": 48000,
        "bit_depth": 24,
        "channels": 2,
        "size": None,
    }
    assert rich["replay_gain"]["track_gain_db"] == -4.25
    assert rich["replay_gain"]["track_peak"] == 0.91
    assert rich["external_ids"]["musicbrainz"] == "mb-track-1"
    assert rich["track_total"] == 9
    assert normalized["albums"][0]["payload"]["_lumae"]["track_count"] == 1
    assert rich["artist_credits"][0]["artist_id"] == "artist-1"
    assert rich["library_ids"] == ["library-1"]
    assert normalized["albums"][0]["payload"]["_lumae"]["library_ids"] == ["library-1"]
    assert any(
        row == {"entity_type": "track", "entity_id": "track-1", "library_id": "library-1"}
        for row in normalized["entity_libraries"]
    )


def test_relationship_only_catalogue_edits_change_metadata_fingerprint():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    def normalized(library_id, artist_id):
        return normalize_provider_catalog(
            {
                "tracks": [
                    {
                        "id": "track-1",
                        "title": "Song",
                        "ArtistItems": [{"Id": artist_id, "Name": "Artist"}],
                        "musicFolderId": library_id,
                    }
                ]
            },
            "navidrome",
        )["tracks"][0]

    original = normalized("library-1", "artist-1")
    moved_library = normalized("library-2", "artist-1")
    rebound_artist = normalized("library-1", "artist-2")
    assert moved_library["metadata_fp"] != original["metadata_fp"]
    assert rebound_artist["metadata_fp"] != original["metadata_fp"]
    assert moved_library["media_fp"] == original["media_fp"]


def test_jellyfin_nested_audio_stream_properties_are_normalized():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    track = normalize_provider_catalog(
        {
            "tracks": [
                {
                    "Id": "track-1",
                    "Name": "Song",
                    "MediaSources": [
                        {
                            "Container": "flac",
                            "Size": 123456,
                            "MediaStreams": [
                                {
                                    "Type": "Audio",
                                    "SampleRate": 96000,
                                    "BitDepth": 24,
                                    "Channels": 2,
                                    "BitRate": 1800000,
                                }
                            ],
                        }
                    ],
                }
            ]
        },
        "jellyfin",
    )["tracks"][0]

    assert track["audio_properties"] == {
        "duration_ms": None,
        "container": "flac",
        "bit_rate": 1800000,
        "sample_rate": 96000,
        "bit_depth": 24,
        "channels": 2,
        "size": 123456,
    }


def test_catalog_fingerprints_separate_metadata_media_and_artwork_changes():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    base = {
        "id": "track-1",
        "title": "Song",
        "albumId": "album-1",
        "album": "Record",
        "duration": 100,
        "suffix": "flac",
        "coverArt": "cover-a",
    }

    def normalized(**changes):
        track = {**base, **changes}
        return normalize_provider_catalog({"tracks": [track]}, "navidrome")["tracks"][0]

    original = normalized()
    title_edit = normalized(title="Song (Edit)")
    media_edit = normalized(duration=101)
    art_edit = normalized(coverArt="cover-b")

    assert title_edit["metadata_fp"] != original["metadata_fp"]
    assert title_edit["media_fp"] == original["media_fp"]
    assert media_edit["media_fp"] != original["media_fp"]
    assert media_edit["artwork_fp"] == original["artwork_fp"]
    assert art_edit["artwork_fp"] != original["artwork_fp"]
    assert art_edit["media_fp"] == original["media_fp"]


def test_catalog_keeps_distinct_provider_occurrences_with_identical_media():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    tracks = [
        {
            "id": "occurrence-a",
            "title": "Same Audio",
            "albumId": "album-a",
            "album": "Edition A",
            "duration": 180,
        },
        {
            "id": "occurrence-b",
            "title": "Same Audio",
            "albumId": "album-b",
            "album": "Edition B",
            "duration": 180,
        },
    ]
    normalized = normalize_provider_catalog({"tracks": tracks}, "navidrome")

    assert [row["track_id"] for row in normalized["tracks"]] == [
        "occurrence-a",
        "occurrence-b",
    ]
    assert normalized["tracks"][0]["media_fp"] == normalized["tracks"][1]["media_fp"]


class RefreshCursor(FakeCursor):
    def __init__(self, db):
        super().__init__([])
        self.db = db

    def execute(self, sql, params=None):
        super().execute(sql, params)
        self.db.executed.append((sql, params))
        if "SELECT catalog_instance_id, current_core_server_id" in sql:
            self.rows = [("catalog-a", "server-a", "navidrome", "active")]
        elif "SELECT catalog_instance_id FROM" in sql and "catalog_sources" in sql:
            self.rows = [("catalog-a",)]
        elif "published_generation, catalog_epoch, catalog_head_seq, entity_counts" in sql:
            self.rows = [(0, "epoch-a", 0, self.db.previous_counts)]
        elif "published_generation, catalog_epoch, catalog_head_seq" in sql:
            self.rows = [(0, "epoch-a", 0)]
        elif sql.lstrip().startswith("SELECT") and "available=TRUE" in sql:
            self.rows = []
        else:
            self.rows = []

    def executemany(self, sql, params):
        materialized = list(params)
        self.db.executed.append((sql, materialized))


class RefreshDb:
    def __init__(self, previous_counts=None):
        self.previous_counts = previous_counts or {}
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return RefreshCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class RefreshBridge:
    def __init__(self, payload=None, error=None):
        self.payload = payload or {"tracks": []}
        self.error = error

    def list_servers(self):
        return [self.require_server("server-a")]

    def require_server(self, server_id):
        assert server_id == "server-a"
        return {
            "server_id": "server-a",
            "name": "Server",
            "provider_type": "navidrome",
            "is_default": True,
            "supported": True,
        }

    def fetch_catalog(self, server_id):
        if self.error:
            raise self.error
        return self.payload


def test_refresh_catalog_publishes_complete_generation_and_coverage():
    from plugins.LumaeAnalysis.catalog import refresh_catalog

    db = RefreshDb()
    bridge = RefreshBridge(
        {
            "libraries": [{"id": "library-1", "name": "Music"}],
            "tracks": [
                {
                    "id": "track-1",
                    "title": "Song",
                    "track": 1,
                    "discNumber": 1,
                    "duration": 123,
                    "musicFolderId": "library-1",
                }
            ],
        }
    )

    result = refresh_catalog("server-a", db=db, bridge=bridge)

    assert result["generation"] == 1
    assert result["counts"] == {"library": 1, "artist": 0, "album": 0, "track": 1}
    assert result["field_coverage"]["track_number"]["ratio"] == 1.0
    assert "replay_gain" in result["field_coverage"]
    assert "sample_rate" in result["field_coverage"]
    assert db.commits == 2
    assert db.rollbacks == 0
    assert any("catalog_changes" in sql for sql, _params in db.executed)


def test_refresh_catalog_failure_keeps_prior_generation_and_records_error():
    from plugins.LumaeAnalysis.catalog import refresh_catalog

    db = RefreshDb(previous_counts={"track": 3})
    bridge = RefreshBridge(error=RuntimeError("provider unavailable"))

    with pytest.raises(RuntimeError, match="provider unavailable"):
        refresh_catalog("server-a", db=db, bridge=bridge)

    assert db.rollbacks == 1
    assert db.commits == 2
    assert not any("SET published_generation" in sql for sql, _params in db.executed)
    assert any(
        "status='failed'" in sql and params[0] == "provider unavailable" for sql, params in db.executed if params
    )


class ProjectionCursor(FakeCursor):
    def __init__(self, db):
        super().__init__([])
        self.db = db

    def execute(self, sql, params=None):
        super().execute(sql, params)
        self.db.executed.append((sql, params))
        if "JOIN plugin_lumae_analysis__catalog_state" in sql:
            self.rows = [
                (
                    "catalog-a",
                    "server-a",
                    "navidrome",
                    "Server",
                    True,
                    "active",
                    1,
                    "catalog-epoch",
                    0,
                    0,
                    "complete",
                    {"track": 2},
                    {},
                    {},
                    None,
                    None,
                    None,
                    0,
                    "analysis-epoch",
                    0,
                    0,
                    "not_initialized",
                    0,
                    0,
                    None,
                    None,
                )
            ]
        elif "FROM plugin_lumae_analysis__catalog_tracks" in sql:
            self.rows = [
                ("copy-a", "Same song", "Artist", "album-a", 180000, {}),
                ("copy-b", "Same song", "Artist", "album-b", 180000, {}),
            ]
        elif "FROM fake_mapping" in sql:
            self.rows = [
                ("copy-a", "canonical-1", "fingerprint"),
                ("copy-b", "canonical-1", "fingerprint"),
            ]
        elif "FROM score s" in sql:
            self.rows = [
                (
                    "canonical-1",
                    120.0,
                    "C",
                    "major",
                    "happy:0.8",
                    0.08,
                    "danceable:0.7",
                    struct.pack("<2f", 0.1, 0.2),
                    None,
                )
            ]
        elif "FROM map_projection_data" in sql:
            self.rows = [(struct.pack("<2f", 1.5, -2.5), '["canonical-1"]', 2)]
        elif "FROM plugin_lumae_analysis__analysis_state" in sql:
            self.rows = [(0, "analysis-epoch", 0)]
        elif "SELECT analysis_id, scalar_fp" in sql:
            self.rows = []
        elif "SELECT provider_track_id, analysis_id, status" in sql:
            self.rows = []
        else:
            self.rows = []


class ProjectionDb:
    def __init__(self):
        self.executed = []
        self.commits = 0

    def cursor(self):
        return ProjectionCursor(self)

    def commit(self):
        self.commits += 1


class ProjectionAdapter:
    def active_server_id(self):
        return "server-a"

    def analysis_mapping_sql(self):
        return "SELECT provider_track_id, analysis_id, match_tier FROM fake_mapping WHERE server_id=%s"


def test_analysis_projection_reuses_one_vector_for_two_provider_occurrences():
    from plugins.LumaeAnalysis.catalog_analysis import project_analysis

    db = ProjectionDb()

    result = project_analysis("server-a", db=db, adapter=ProjectionAdapter())

    assert result["item_count"] == 1
    assert result["link_count"] == 2
    assert result["suspect_count"] == 0
    assert db.commits == 1
    assert sum("INSERT INTO plugin_lumae_analysis__analysis_items" in sql for sql, _ in db.executed) == 1
    assert sum("INSERT INTO plugin_lumae_analysis__track_analysis_links" in sql for sql, _ in db.executed) == 2


def test_analysis_projection_marks_contradictory_dedup_group_suspect():
    from plugins.LumaeAnalysis.catalog_analysis import _suspect_analysis_ids

    tracks = {
        "a": {
            "title": "Song A",
            "artist": "Artist A",
            "duration_ms": 180000,
            "payload": {},
        },
        "b": {
            "title": "Song B",
            "artist": "Artist B",
            "duration_ms": 240000,
            "payload": {},
        },
    }
    links = {
        "a": {"analysis_id": "canonical-1"},
        "b": {"analysis_id": "canonical-1"},
    }

    assert _suspect_analysis_ids(tracks, links) == {"canonical-1"}


def test_v3_0_3_dedup_policy_and_duration_backstop(monkeypatch):
    from plugins.LumaeAnalysis.catalog_analysis import _suspect_analysis_ids, dedup_policy

    values = {
        "DUPLICATE_DISTANCE_THRESHOLD_COSINE": 0.02,
        "CATALOGUE_ID_SCHEME_VERSION": 4,
        "DURATION_TOLERANCE_SECONDS": 1.0,
        "CHROMAPRINT_COLLECTION_ENABLED": True,
        "CHROMAPRINT_GATE_ENABLED": True,
        "CHROMAPRINT_MATCH_THRESHOLD": 0.95,
        "CHROMAPRINT_MIN_OVERLAP": 40,
    }
    for name, value in values.items():
        monkeypatch.setattr(plugin_api_module.config, name, value, raising=False)

    policy = dedup_policy()

    assert policy == {
        "algorithm": "audiomuse_catalogue_fp_4",
        "catalogue_id_scheme_version": 4,
        "configured_threshold": 0.02,
        "duration_tolerance_seconds": 1.0,
        "folder_aware": True,
        "chromaprint_collection_enabled": True,
        "chromaprint_gate_enabled": True,
        "chromaprint_match_threshold": 0.95,
        "chromaprint_min_overlap": 40,
        "per_link_distance_available": False,
        "per_link_chromaprint_evidence_available": False,
        "evidence_status": "configured_policy_only",
    }
    tracks = {
        "a": {"title": "Song", "artist": "Artist", "duration_ms": 180000, "payload": {}},
        "b": {"title": "Song", "artist": "Artist", "duration_ms": 181500, "payload": {}},
    }
    links = {
        "a": {"analysis_id": "canonical-1"},
        "b": {"analysis_id": "canonical-1"},
    }
    assert _suspect_analysis_ids(tracks, links, policy) == {"canonical-1"}


class ReadinessCursor:
    def __init__(self, db):
        self.db = db
        self.rows = []

    def execute(self, sql, params=None):
        self.db.executed.append((sql, params))
        if "FROM plugin_lumae_analysis__catalog_tracks" in sql:
            self.rows = [self.db.coverage]
        elif "FROM task_status" in sql:
            self.rows = list(self.db.tasks)
        else:
            raise AssertionError(f"Unexpected readiness SQL: {sql}")

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        pass


class ReadinessDb:
    def __init__(self, coverage=(10, 9, 9, 150.0), tasks=None):
        self.coverage = coverage
        self.tasks = tasks or []
        self.executed = []

    def cursor(self):
        return ReadinessCursor(self)


def readiness_source():
    return {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "name": "Main",
        "catalog": {"generation": 4, "status": "complete"},
        "analysis": {"status": "complete"},
    }


def readiness_policy():
    return {
        "catalogue_id_scheme_version": 4,
        "duration_tolerance_seconds": 1.0,
        "folder_aware": True,
        "chromaprint_collection_enabled": True,
        "chromaprint_gate_enabled": True,
    }


def readiness_tasks():
    return [
        ("analysis-after", "main_analysis", "SUCCESS", 300.0, {"failed_servers": []}, None),
        ("cleaning", "cleaning", "SUCCESS", 200.0, {}, None),
        ("analysis-before", "main_analysis", "SUCCESS", 100.0, {"failed_servers": []}, None),
    ]


def test_v3_readiness_requires_source_scoped_admin_acknowledgement(monkeypatch):
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.3",
        adapter="v3_registry",
    )
    settings = {}
    monkeypatch.setattr(
        readiness,
        "get_setting",
        lambda key, default=None: settings.get(key, default),
    )
    monkeypatch.setattr(readiness, "set_setting", lambda key, value: settings.__setitem__(key, value))
    db = ReadinessDb(tasks=readiness_tasks())

    before = readiness.v3_release_readiness(
        db,
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert before["status"] == "acknowledgement_required"
    assert before["blockers"] == ["administrator_acknowledgement_required"]
    assert before["missing_mapping_count"] == 1
    assert before["chromaprint_coverage"] == 1.0
    assert before["task_evidence"]["upgrade_sequence_complete"] is True
    assert before["task_evidence"]["chromaprint_complete_before_cleaning"] is True

    after = readiness.acknowledge_v3_release(
        db,
        compatibility,
        readiness_source(),
        readiness_policy(),
        "upgraded",
    )

    assert after["ready"] is True
    assert after["status"] == "ready"
    assert after["verification_mode"] == "upgraded"
    saved = settings[readiness.ACKNOWLEDGEMENT_SETTING]["catalog-a"]
    assert saved["server_id"] == "server-a"
    assert saved["cleaning_task_id"] == "cleaning"
    assert saved["post_clean_analysis_task_id"] == "analysis-after"
    archived = readiness.v3_release_readiness(
        ReadinessDb(tasks=[]),
        compatibility,
        readiness_source(),
        readiness_policy(),
    )
    assert archived["ready"] is True
    rebound_source = {**readiness_source(), "server_id": "server-b"}
    rebound = readiness.v3_release_readiness(
        ReadinessDb(tasks=[]),
        compatibility,
        rebound_source,
        readiness_policy(),
    )
    assert rebound["ready"] is False
    assert rebound["blockers"] == ["administrator_acknowledgement_required"]


def test_v3_readiness_rejects_incomplete_backfill_and_upgrade_sequence(monkeypatch):
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.3",
        adapter="v3_registry",
    )
    monkeypatch.setattr(readiness, "get_setting", lambda _key, default=None: default)
    db = ReadinessDb(coverage=(10, 9, 8, 150.0), tasks=[])

    with pytest.raises(ValueError, match="chromaprint_backfill_incomplete") as exc:
        readiness.acknowledge_v3_release(
            db,
            compatibility,
            readiness_source(),
            readiness_policy(),
            "upgraded",
        )

    assert "upgrade_repair_sequence_incomplete" in str(exc.value)


def test_v3_readiness_requires_cleaning_after_chromaprint_completion(monkeypatch):
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.3",
        adapter="v3_registry",
    )
    monkeypatch.setattr(readiness, "get_setting", lambda _key, default=None: default)
    db = ReadinessDb(coverage=(10, 10, 10, 250.0), tasks=readiness_tasks())

    with pytest.raises(ValueError, match="cleaning_predates_chromaprint_completion"):
        readiness.acknowledge_v3_release(
            db,
            compatibility,
            readiness_source(),
            readiness_policy(),
            "upgraded",
        )


def test_v3_fresh_install_can_be_confirmed_without_legacy_repair_tasks(monkeypatch):
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.3",
        adapter="v3_registry",
    )
    settings = {}
    monkeypatch.setattr(
        readiness,
        "get_setting",
        lambda key, default=None: settings.get(key, default),
    )
    monkeypatch.setattr(readiness, "set_setting", lambda key, value: settings.__setitem__(key, value))

    result = readiness.acknowledge_v3_release(
        ReadinessDb(coverage=(10, 10, 10, 150.0), tasks=[]),
        compatibility,
        readiness_source(),
        readiness_policy(),
        "fresh",
    )

    assert result["ready"] is True
    assert result["verification_mode"] == "fresh"


def test_v2_readiness_is_not_applicable_without_database_access():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v2.6.2",
        adapter="v2_single_server",
    )

    result = readiness.v3_release_readiness(
        None,
        compatibility,
        readiness_source(),
        {},
    )

    assert result["applicable"] is False
    assert result["ready"] is True
    assert result["status"] == "not_applicable"


def test_settings_acknowledges_v3_readiness_only_with_explicit_confirmation(monkeypatch):
    mod = load_plugin()
    source = readiness_source()
    compatibility = types.SimpleNamespace(core_version="v3.0.3", adapter="v3_registry")
    captured = {}
    monkeypatch.setattr(mod, "detect_core", lambda: compatibility)
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(mod, "dedup_policy", readiness_policy)
    monkeypatch.setattr(
        mod,
        "acknowledge_v3_release",
        lambda db, compat, selected, policy, mode: captured.update(
            {
                "db": db,
                "compatibility": compat,
                "source": selected,
                "policy": policy,
                "mode": mode,
            }
        )
        or {"verification_mode": mode},
    )
    monkeypatch.setattr(
        mod,
        "render_settings",
        lambda message=None, error=None: message or error or "settings",
    )
    client = plugin_client(mod)

    rejected = client.post(
        "/settings",
        data={
            "action": "ack_v3_readiness",
            "server_id": "server-a",
            "catalog_instance_id": "catalog-a",
            "verification_mode": "upgraded",
        },
    )
    accepted = client.post(
        "/settings",
        data={
            "action": "ack_v3_readiness",
            "server_id": "server-a",
            "catalog_instance_id": "catalog-a",
            "verification_mode": "upgraded",
            "confirm": "on",
        },
    )

    assert rejected.status_code == 200
    assert "Explicit AudioMuse 3.0.3 confirmation is required" in rejected.get_data(
        as_text=True
    )
    assert accepted.status_code == 200
    assert "sync readiness confirmed for Main" in accepted.get_data(as_text=True)
    assert captured["source"] == source
    assert captured["mode"] == "upgraded"


def test_settings_page_explains_v3_readiness_modes_and_blockers(monkeypatch):
    mod = load_plugin()
    source = readiness_source()
    readiness = {
        "status": "repair_incomplete",
        "ready": False,
        "administrator_acknowledged": False,
        "eligible_track_count": 12,
        "mapped_track_count": 10,
        "missing_mapping_count": 2,
        "chromaprint_track_count": 8,
        "chromaprint_coverage": 0.8,
        "task_evidence": {"upgrade_sequence_complete": False},
        "blockers": [
            "chromaprint_backfill_incomplete",
            "administrator_acknowledgement_required",
        ],
    }
    monkeypatch.setattr(mod, "_v3_readiness_sources", lambda: [(source, readiness)])

    body = mod.render_v3_readiness_panel()
    compact = " ".join(body.split())

    assert "AudioMuse 3.0.3 sync readiness" in body
    assert "Chromaprint: 8 of 10 mapped tracks (80.00%)" in compact
    assert "without analysis mapping: 2" in compact
    assert "Mapped tracks are still missing Chromaprint fingerprints." in body
    assert "Confirm fresh installation" in body
    assert "Confirm upgraded installation" in body
    assert "Analysis, Cleaning, then Analysis again" in body


def test_vector_batch_endpoint_returns_versioned_little_endian_payload(monkeypatch):
    import struct

    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: object())
    header = b'{"format":"lumae-f32le-v1"}'
    binary = struct.pack("<I", len(header)) + header + struct.pack("<2f", 0.1, 0.2)
    captured = {}

    def vector_batch(*_args, **kwargs):
        captured.update(kwargs)
        return binary

    monkeypatch.setattr(mod, "vector_batch", vector_batch)

    response = plugin_client(mod).post(
        "/api/catalog/analysis/vectors",
        json={
            "catalog_instance_id": "catalog-a",
            "analysis_ids": ["canonical-1"],
            "family": "musicnn",
            "generation": 4,
        },
    )

    assert response.status_code == 200
    assert response.mimetype == "application/vnd.lumae.f32le-v1"
    assert response.data == binary
    assert response.headers["Cache-Control"] == "private, no-store"
    assert captured == {"family": "musicnn", "generation": 4}


def test_register_uses_analysis_hook_and_catalog_refresh_worker(monkeypatch):
    mod = load_plugin()
    ctx = FakeCtx()

    mod.register(ctx)

    assert ctx.blueprints == [mod.bp]
    assert ctx.settings_endpoint == "lumae_analysis.settings"
    assert ctx.install_hooks == [mod.migrate]
    assert ctx.song_hooks == [mod.analyze_song_hook]
    assert ctx.tasks == [("analysis_projection", mod.analysis_projection_task, "default")]
    assert ctx.cron_tasks == [
        ("catalog_refresh", mod.catalog_refresh_task, "default"),
        ("analysis_projection", mod.analysis_projection_task, "default"),
    ]
    assert ctx.menu_items == []


def test_register_exposes_enabled_collections_in_plugins_menu(monkeypatch):
    mod = load_plugin()
    ctx = FakeCtx()
    monkeypatch.setattr(mod, "collections_enabled", lambda: True)

    mod.register(ctx)

    assert ctx.menu_items == [
        {
            "label": "Living Collections",
            "endpoint": "lumae_analysis.collection_manager_page",
            "admin_only": False,
        }
    ]


def test_sync_collections_menu_updates_live_plugin_record():
    mod = load_plugin()
    manager = types.SimpleNamespace(
        records={
            "lumae_analysis": {
                "menu_items": [
                    {
                        "label": "Other",
                        "endpoint": "lumae_analysis.other",
                        "admin_only": True,
                    }
                ]
            }
        }
    )

    assert mod.sync_collections_menu(True, manager) is True
    assert manager.records["lumae_analysis"]["menu_items"] == [
        {"label": "Other", "endpoint": "lumae_analysis.other", "admin_only": True},
        {
            "label": "Living Collections",
            "endpoint": "lumae_analysis.collection_manager_page",
            "admin_only": False,
        },
    ]

    assert mod.sync_collections_menu(False, manager) is True
    assert manager.records["lumae_analysis"]["menu_items"] == [
        {"label": "Other", "endpoint": "lumae_analysis.other", "admin_only": True}
    ]


def test_settings_page_exposes_manual_catch_up_and_status(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 250)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda: {
            "total_with_files": 16000,
            "ready_current": 100,
            "pending": 2,
            "failed": 1,
            "skipped": 3,
            "needs_analysis": 15894,
        },
    )
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.get("/settings")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'class="lumae-status-grid"' in body
    assert 'class="lumae-meter-fill" style="width: 1%;"' in body
    assert "Analyze next batch" in body
    assert "Queue all missing tracks" in body
    assert "Tracks per batch" in body
    assert "Needs analysis" in body
    assert "15,894" in body
    assert "Enable scheduled catch-up" not in body
    assert "Cron expression" not in body
    assert "Scheduled Tasks" not in body
    assert "Living Collections" in body
    assert "Enable the collection manager" in body


def test_collection_setting_must_be_enabled_before_manager_is_available(monkeypatch):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    saved = []
    menu_states = []
    monkeypatch.setattr(mod, "set_setting", lambda key, value: saved.append((key, value)))
    monkeypatch.setattr(mod, "sync_collections_menu", lambda enabled: menu_states.append(enabled))
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 25)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda: {
            "total_with_files": 0,
            "ready_current": 0,
            "pending": 0,
            "failed": 0,
            "skipped": 0,
            "needs_analysis": 0,
        },
    )
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.post(
        "/settings",
        data={"action": "save_collections", "collection_manager_enabled": "on"},
    )

    assert response.status_code == 200
    assert saved == [("collection_manager_enabled", True)]
    assert menu_states == [True]
    assert "Living Collections enabled." in response.get_data(as_text=True)

    monkeypatch.setattr(collections, "get_setting", lambda key, default=None: True)
    monkeypatch.setattr(collections, "render_page", lambda body, title=None: body)
    enabled_response = client.get("/collections")
    assert enabled_response.status_code == 200
    body = enabled_response.get_data(as_text=True)
    assert "Living Collections" in body
    assert "New collection" in body
    assert "Backup &amp; restore" in body
    assert "Download full backup" in body
    assert "Restore always creates new copies" in body
    assert "Shared bearer-token library" in body
    assert "Everyone using the AudioMuse installation token" in body
    assert 'href="api/collections/backup"' in body
    assert "lumae-living-collections" in body
    assert "restoreDocument" in body
    assert "/export" in body
    assert "Add selected" in body
    assert "Duplicate" in body
    assert 'id="collection-toast"' in body
    assert "data-move-item=" in body
    assert "@media(max-width:760px)" in body
    assert 'class="collections-page"' in body
    assert ".collections-page dialog" in body
    assert 'id="library-dialog"' in body
    assert 'id="preview-player"' in body
    assert ".collections-page [hidden]{display:none!important}" in body
    assert 'data-scope="artists"' in body
    assert "Track and disc numbers loaded from your media server" in body
    assert "Type at least three characters to search" in body
    assert "delete rest.headers" in body
    assert "'Content-Type':'application/json',...headers" in body
    assert "new AbortController()" in body
    assert "browser.controller?.abort()" in body
    assert "searchTimer=setTimeout(()=>loadBrowser(),450)" in body
    assert "clearTimeout(searchTimer);browser.controller?.abort()" in body
    assert "if(event.key==='Escape'){event.preventDefault();closeLibrary()}" in body
    assert "Adding ${count}" in body
    assert "const copies=items.map(({id,collection_id,added_at,updated_at,...item})=>item)" in body


def test_settings_page_renders_coverage_meter_and_action_context(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 50)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda: {
            "total_with_files": 100,
            "ready_current": 82,
            "pending": 4,
            "failed": 2,
            "skipped": 1,
            "needs_analysis": 11,
        },
    )
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.get("/settings")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "82% profile coverage" in body
    assert 'aria-valuenow="82"' in body
    assert "11 tracks can be queued now." in body
    assert "Runs one controlled batch using the current batch size." in body
    assert "Queues all missing, stale, or changed tracks in 250-track jobs." in body


def test_settings_page_queue_whole_library_posts_action_and_reports_job_count(
    monkeypatch,
):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 250)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda: {
            "total_with_files": 16000,
            "ready_current": 100,
            "pending": 2,
            "failed": 1,
            "skipped": 3,
            "needs_analysis": 15894,
        },
    )
    monkeypatch.setattr(
        mod,
        "queue_whole_library",
        lambda: {"queued": 15894, "jobs": 64, "chunk_size": 250},
    )
    monkeypatch.setattr(mod, "set_setting", lambda key, value: None)
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.post(
        "/settings",
        data={"backfill_batch_size": "25", "action": "queue_all"},
    )

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'class="lumae-notice lumae-notice-success" role="status"' in body
    assert "Queued 15,894 tracks across 64 jobs for Lumae analysis." in body
