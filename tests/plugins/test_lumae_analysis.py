import importlib
import json
import os
import pathlib
import struct
import sys
import math
import types
import uuid

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


@pytest.fixture
def lumae_postgres_db():
    dsn = os.environ.get("LUMAE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("set LUMAE_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
    psycopg2 = pytest.importorskip("psycopg2")
    schema = f"lumae_analysis_integration_{uuid.uuid4().hex}"
    db = psycopg2.connect(dsn)
    try:
        cur = db.cursor()
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}, public")
        cur.close()
        db.commit()
        yield db
    finally:
        db.rollback()
        cur = db.cursor()
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.close()
        db.commit()
        db.close()


def plugin_client(mod):
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    return app.test_client()


def settings_catalog_source():
    return {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "provider_type": "navidrome",
        "name": "Main Navidrome",
        "catalog": {"status": "complete", "entity_counts": {"track": 100}},
        "analysis": {"status": "complete"},
    }


def expect_v3_readiness(core_version):
    return {
        "qualified_core_version": core_version,
        "detected_core_version": core_version,
        "applicable": True,
        "status": "catalog_not_initialized",
        "ready": False,
        "verification_mode": None,
        "administrator_acknowledged": False,
        "acknowledged_at": None,
        "blockers": ["catalog_not_initialized"],
    }


def test_plugin_manifest_has_lumae_identity():
    with open("plugins/LumaeAnalysis/plugin.json", "r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    assert manifest["id"] == "lumae_analysis"
    assert manifest["name"] == "Lumae Analysis"
    assert manifest["requirements"] == []
    assert manifest["versions"][0]["version"] == "1.1.6"
    assert manifest["versions"][0]["min_core_version"] == "2.6.0"
    assert manifest["capabilities"]["lumae_analysis_profiles"] == {
        "schema_version": 1,
        "analyzer_version": 1,
        "profile_source": "waveform",
        "features": [
            "loudness",
            "mix_ramp",
            "source_scoped_profiles",
            "prepare_lumae",
            "interactive_profile_priority",
            "bounded_profile_backfill",
            "profile_cursor_stream",
        ],
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
        "contract_revision": 1,
        "catalog_schema_version": 3,
        "analysis_schema_version": 2,
        "catalog_builder_version": 5,
        "supported_core_range": ">=2.6.0,<4.0.0",
        "supported_provider_types": ["navidrome"],
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
            "contract_admission_v1",
            "independent_stream_admission",
            "automatic_sonic_verification",
            "semantic_contracts_v1",
            "progressive_analysis_admission",
            "repair_flagged_analysis_admission",
            "database_state_dashboard",
            "provider_track_scope_verification",
            "source_scoped_profiles",
            "prepare_lumae",
            "catalog_prepare_api",
            "analysis_run_finalization",
            "catalog_ready_before_profile_backfill",
            "interactive_profile_priority",
            "bounded_profile_backfill",
            "automatic_catalog_preparation",
            "catalog_builder_versioning",
            "durable_catalog_reconciliation",
            "preparation_worker_attestation",
            "profile_cursor_stream",
            "server_album_artist_relationships",
            "relationship_cursor_stream",
            "nonblocking_enrichment",
            "provider_identity_transition_shield_v1",
            "provider_identity_rekey_v1",
            "bounded_storage_retention",
        ],
    }


def test_health_endpoint_reports_schema_and_analyzer_versions(monkeypatch):
    mod = load_plugin()
    client = plugin_client(mod)

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.get_json() == {
        "plugin": "lumae_analysis",
        "plugin_version": "1.1.6",
        "core_version": "v2.6.2",
        "core_adapter": "v2_single_server",
        "supported_core_range": ">=2.6.0,<4.0.0",
        "sync_contract": mod.sync_contract(mod.detect_core()),
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
    assert body["core_api_contract"] == "audiomuse_v2_single_server_v1"
    assert body["sync_contract"]["revision"] == 1
    assert body["sync_contract"]["core_api_contract"] == body["core_api_contract"]
    assert body["sync_contract"]["streams"]["catalog"]["semantic_contracts"] == [
        "provider_track_ids_v1",
        "complete_catalog_generation_v1",
        "contiguous_change_journal_v1",
    ]
    assert body["supported"] is True
    assert body["servers"] == [
        {
            "server_id": "legacy-default",
            "catalog_instance_id": None,
            "name": "Default music server",
            "provider_type": "navidrome",
            "is_default": True,
            "status": "not_initialized",
            "supported": True,
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
        "status": "provider_unsupported",
        "supported": False,
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
            "verification_mode": "automatic",
            "administrator_acknowledged": False,
            "acknowledged_at": None,
            "blockers": [],
        }

    monkeypatch.setattr(mod, "v3_release_readiness", release_readiness)

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    body = response.get_json()
    assert body["plugin_version"] == "1.1.6"
    assert body["servers"][0]["v3_readiness"]["ready"] is True
    assert captured["db"] is db
    assert captured["core"] == "v3.0.3"
    assert captured["source"] == {**source, "supported": True}
    assert captured["policy"]["catalogue_id_scheme_version"] is None


def test_catalog_health_denies_both_streams_while_provider_identity_is_pending(monkeypatch):
    mod = load_plugin()
    source = readiness_source()
    db = object()
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", "v3.1.1")
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
    monkeypatch.setattr(mod, "observe_provider_version", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        mod,
        "provider_transition_health",
        lambda *_args, **_kwargs: {
            "state": "transition_pending",
            "catalog_sync_allowed": False,
            "analysis_sync_allowed": False,
            "audiomuse_projection_ingest_allowed": False,
            "provider_mutations_allowed": False,
        },
    )
    monkeypatch.setattr(
        mod,
        "v3_release_readiness",
        lambda *_args, **_kwargs: {
            "qualified_core_version": "v3.1.1",
            "detected_core_version": "v3.1.1",
            "applicable": True,
            "status": "ready",
            "ready": True,
            "analysis_sync_allowed": True,
            "verification_mode": "upgraded",
            "administrator_acknowledged": True,
            "acknowledged_at": "2026-08-02T12:00:00Z",
            "blockers": [],
            "admission": {
                "catalog": {"admitted": True, "status": "admitted", "blockers": []},
                "analysis": {"admitted": True, "status": "admitted", "blockers": []},
            },
        },
    )

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    server = response.get_json()["servers"][0]
    assert server["provider_identity_transition"]["state"] == "transition_pending"
    assert server["catalog_sync_allowed"] is False
    assert server["analysis_sync_allowed"] is False
    assert server["v3_readiness"]["ready"] is False
    assert server["v3_readiness"]["admission"]["catalog"] == {
        "admitted": False,
        "status": "denied",
        "blockers": ["provider_identity_transition"],
    }
    assert server["v3_readiness"]["admission"]["analysis"] == {
        "admitted": False,
        "status": "denied",
        "blockers": ["provider_identity_transition"],
    }


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


def test_profile_stream_serializes_waveform_payload_without_device_analysis():
    from plugins.LumaeAnalysis.catalog_enrichment import serialize_profile

    payload = serialize_profile(
        "track-a",
        44100,
        123000,
        -14.25,
        b"\x01\x02",
        b"\x03\x04",
        1,
        "2026-07-27T12:00:00Z",
        "catalog-media:abc",
    )

    assert payload == {
        "track_id": "track-a",
        "source": "waveform",
        "sample_rate": 44100,
        "duration_ms": 123000,
        "ref_lufs": -14.25,
        "start_ramp": "AQI=",
        "end_ramp": "AwQ=",
        "analyzer_ver": 1,
        "analyzed_at": "2026-07-27T12:00:00Z",
        "media_signature": "catalog-media:abc",
    }


def test_server_album_and_artist_relationships_use_lumae_native_rankers():
    from plugins.LumaeAnalysis.catalog_enrichment import (
        _album_fingerprint,
        _artist_fingerprint,
        _rank_albums,
        _rank_artists,
    )

    def track(track_id, embedding, order, album, year):
        return {
            "id": track_id,
            "embedding": embedding,
            "energy": 0.5,
            "mood": [0.5] * 6,
            "order": order,
            "album": album,
            "year": year,
        }

    album_rows = [
        {
            "key": "artist-a::source",
            "album": "Source",
            "artist": "Artist A",
            "cover": "a1",
            "fingerprint": _album_fingerprint(
                "artist-a::source",
                [track("a1", [1.0, 0.0], 0, "Source", 2020)],
            ),
        },
        {
            "key": "artist-b::near",
            "album": "Near",
            "artist": "Artist B",
            "cover": "b1",
            "fingerprint": _album_fingerprint(
                "artist-b::near",
                [track("b1", [0.99, 0.1], 0, "Near", 2021)],
            ),
        },
        {
            "key": "artist-c::far",
            "album": "Far",
            "artist": "Artist C",
            "cover": "c1",
            "fingerprint": _album_fingerprint(
                "artist-c::far",
                [track("c1", [0.0, 1.0], 0, "Far", 2022)],
            ),
        },
    ]
    assert [row["albumKey"] for row in _rank_albums(album_rows[0], album_rows)] == [
        "artist-b::near",
        "artist-c::far",
    ]
    assert _rank_albums(album_rows[0], album_rows)[0]["reasons"][0] == "core"

    artist_rows = [
        {
            "key": "artist-a",
            "artist": "Artist A",
            "cover": "a1",
            "fingerprint": _artist_fingerprint(
                "artist-a",
                [track("a1", [1.0, 0.0], 0, "Source", 2020)],
            ),
        },
        {
            "key": "artist-b",
            "artist": "Artist B",
            "cover": "b1",
            "fingerprint": _artist_fingerprint(
                "artist-b",
                [track("b1", [0.99, 0.1], 0, "Near", 2021)],
            ),
        },
        {
            "key": "artist-c",
            "artist": "Artist C",
            "cover": "c1",
            "fingerprint": _artist_fingerprint(
                "artist-c",
                [track("c1", [0.0, 1.0], 0, "Far", 2022)],
            ),
        },
    ]
    assert [row["artistKey"] for row in _rank_artists(artist_rows[0], artist_rows)] == [
        "artist-b",
        "artist-c",
    ]


def test_relationship_candidate_scoring_is_bounded_by_ivf_shortlist(monkeypatch):
    import plugins.LumaeAnalysis.catalog_enrichment as enrichment

    def entity(index):
        vector = np.zeros(200, dtype=np.float32)
        vector[index % 200] = 1.0
        return {
            "key": f"artist-{index}::album-{index}",
            "album": f"Album {index}",
            "artist": f"Artist {index}",
            "cover": f"track-{index}",
            "fingerprint": {
                "mean": vector,
                "poles": [],
            },
        }

    entities = [entity(index) for index in range(1000)]
    entities_by_key = {row["key"]: row for row in entities}
    track_to_entity = {
        f"track-{index}": row["key"] for index, row in enumerate(entities)
    }
    candidates = enrichment._relationship_candidates(
        entities[0],
        "album",
        entities_by_key,
        track_to_entity,
        lambda _vectors, _limit: [f"track-{index}" for index in range(1, 1000)],
    )

    calls = []
    monkeypatch.setattr(
        enrichment,
        "_album_score",
        lambda _source, _candidate: (calls.append(1) or 0.1, ["core"]),
    )
    enrichment._rank_albums(entities[0], candidates)

    assert len(candidates) == enrichment.RELATIONSHIP_MAX_CANDIDATE_ENTITIES
    assert len(calls) == enrichment.RELATIONSHIP_MAX_CANDIDATE_ENTITIES
    assert len(calls) < len(entities) * len(entities)


def test_relationship_builder_waits_instead_of_falling_back_without_ivf(monkeypatch):
    import plugins.LumaeAnalysis.catalog_enrichment as enrichment

    fake_manager = types.SimpleNamespace(
        ivf_index=None,
        multi_query_ids=lambda _vectors, _limit: [],
    )
    monkeypatch.setitem(
        sys.modules,
        "tasks",
        types.SimpleNamespace(ivf_manager=fake_manager),
    )

    with pytest.raises(enrichment.RelationshipIndexUnavailable, match="not ready"):
        enrichment._ivf_candidate_track_ids(
            [np.ones(200, dtype=np.float32)],
            10,
        )


def test_relationship_builder_loads_ivf_and_caps_queries_to_small_indexes(monkeypatch):
    import plugins.LumaeAnalysis.catalog_enrichment as enrichment

    calls = []

    class Index:
        def __len__(self):
            return 7

    fake_manager = types.SimpleNamespace(ivf_index=None)

    def load():
        calls.append("load")
        fake_manager.ivf_index = Index()

    def query(_vectors, limit):
        calls.append(("query", limit))
        return ["track-a"]

    fake_manager.load_ivf_index_for_querying = load
    fake_manager.multi_query_ids = query
    monkeypatch.setitem(
        sys.modules,
        "tasks",
        types.SimpleNamespace(ivf_manager=fake_manager),
    )

    assert enrichment._ivf_candidate_track_ids(
        [np.ones(200, dtype=np.float32)],
        96,
    ) == ["track-a"]
    assert calls == ["load", ("query", 7)]


def test_relationship_builder_publishes_only_bounded_shortlist_candidates(monkeypatch):
    import plugins.LumaeAnalysis.catalog_enrichment as enrichment

    source = {
        "catalog_instance_id": "catalog-a",
        "catalog": {"status": "complete", "generation": 4},
        "analysis": {"status": "complete", "generation": 6},
    }
    tracks = [
        {"id": "track-a", "album": "Album A", "artist": "Artist A"},
        {"id": "track-b", "album": "Album B", "artist": "Artist B"},
    ]
    vector = np.ones(200, dtype=np.float32)
    albums = [
        {
            "key": "artist a::album a",
            "album": "Album A",
            "artist": "Artist A",
            "cover": "track-a",
            "fingerprint": {"mean": vector, "poles": []},
        },
        {
            "key": "artist b::album b",
            "album": "Album B",
            "artist": "Artist B",
            "cover": "track-b",
            "fingerprint": {"mean": vector, "poles": []},
        },
    ]
    artists = [
        {
            "key": "artist a",
            "artist": "Artist A",
            "cover": "track-a",
            "fingerprint": {"core": [vector], "poles": [], "outliers": []},
        },
        {
            "key": "artist b",
            "artist": "Artist B",
            "cover": "track-b",
            "fingerprint": {"core": [vector], "poles": [], "outliers": []},
        },
    ]

    class Cursor:
        def __init__(self):
            self.executed = []
            self.one = None
            self.all = []

        def execute(self, sql, args=None):
            normalized = " ".join(sql.split())
            self.executed.append((normalized, args))
            self.one = None
            self.all = []
            if "SELECT result_generation, epoch, head_seq" in normalized:
                self.one = (0, "epoch-a", 0)
            elif "SELECT entity_type, entity_id, result_fp" in normalized:
                self.all = []

        def fetchone(self):
            return self.one

        def fetchall(self):
            return self.all

        def close(self):
            return None

    class Db:
        def __init__(self):
            self.cursor_obj = Cursor()
            self.commits = 0
            self.rollbacks = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    db = Db()
    monkeypatch.setattr(
        enrichment,
        "resolve_catalog_source",
        lambda _db, **_kwargs: [source],
    )
    monkeypatch.setattr(
        enrichment,
        "_load_relationship_inputs",
        lambda _cur, _source: tracks,
    )
    monkeypatch.setattr(
        enrichment,
        "_build_entities",
        lambda _tracks: (albums, artists),
    )
    scored = []
    monkeypatch.setattr(
        enrichment,
        "_rank_albums",
        lambda entity, candidates: scored.append(
            ("album", entity["key"], [row["key"] for row in candidates])
        )
        or [],
    )
    monkeypatch.setattr(
        enrichment,
        "_rank_artists",
        lambda entity, candidates: scored.append(
            ("artist", entity["key"], [row["key"] for row in candidates])
        )
        or [],
    )
    lookup_calls = []

    def lookup(_vectors, limit):
        lookup_calls.append(limit)
        return ["track-a", "track-b"]

    result = enrichment.prepare_relationships(
        "catalog-a",
        db=db,
        candidate_lookup=lookup,
    )

    assert result == {
        "catalog_instance_id": "catalog-a",
        "status": "complete",
        "generation": 1,
        "album_count": 2,
        "artist_count": 2,
        "track_count": 2,
        "changes": 4,
        "cursor": enrichment.opaque_cursor("catalog-a", "epoch-a", 4),
    }
    assert lookup_calls == [enrichment.RELATIONSHIP_CANDIDATE_TRACKS_PER_VECTOR] * 4
    assert scored == [
        ("album", "artist a::album a", ["artist b::album b"]),
        ("album", "artist b::album b", ["artist a::album a"]),
        ("artist", "artist a", ["artist b"]),
        ("artist", "artist b", ["artist a"]),
    ]
    assert db.commits == 2
    assert db.rollbacks == 0
    statements = [sql for sql, _args in db.cursor_obj.executed]
    assert sum("INSERT INTO plugin_lumae_analysis__relationship_results" in sql for sql in statements) == 4
    assert sum("INSERT INTO plugin_lumae_analysis__relationship_changes" in sql for sql in statements) == 4
    assert any("status='complete'" in sql for sql in statements)


def test_relationship_fingerprints_bound_pathological_album_pairwise_work(monkeypatch):
    import plugins.LumaeAnalysis.catalog_enrichment as enrichment

    pairwise_sizes = []

    def bounded_pairwise(tracks):
        pairwise_sizes.append(len(tracks))
        return [[0.0] * len(tracks) for _ in tracks]

    monkeypatch.setattr(enrichment, "_pairwise", bounded_pairwise)
    tracks = [
        {
            "id": f"track-{index:04d}",
            "order": index,
            "embedding": np.full(200, index / 500, dtype=np.float32),
            "energy": 0.5,
            "mood": None,
        }
        for index in range(500)
    ]

    fingerprint = enrichment._album_fingerprint("artist::album", tracks)

    assert fingerprint is not None
    assert 0 < pairwise_sizes[0] <= enrichment.RELATIONSHIP_ENTITY_SAMPLE_LIMIT
    assert len(pairwise_sizes) == 1
    assert pairwise_sizes[0] < len(tracks)


def test_relationship_bootstrap_rejects_a_page_token_from_an_old_generation(
    monkeypatch,
):
    from plugins.LumaeAnalysis import catalog_enrichment
    from plugins.LumaeAnalysis.catalog import opaque_cursor

    cursor = opaque_cursor("catalog-a", "epoch-a", 12)
    monkeypatch.setattr(
        catalog_enrichment,
        "relationship_status",
        lambda _db, _catalog_id: {
            "catalog_instance_id": "catalog-a",
            "schema_version": 1,
            "algorithm_version": 1,
            "generation": 3,
            "cursor": cursor,
            "status": "complete",
        },
    )
    page_token = catalog_enrichment._encode_page_token(
        {
            "catalog_instance_id": "catalog-a",
            "epoch": "epoch-a",
            "generation": 2,
            "entity_type": "album",
            "entity_id": "artist::album",
        }
    )

    with pytest.raises(KeyError, match="bootstrap_required"):
        catalog_enrichment.relationship_bootstrap_page(
            object(), "catalog-a", page_token=page_token
        )


def test_relationship_bootstrap_keeps_last_published_generation_visible_while_stale(
    monkeypatch,
):
    from plugins.LumaeAnalysis import catalog_enrichment
    from plugins.LumaeAnalysis.catalog import opaque_cursor

    status = {
        "catalog_instance_id": "catalog-a",
        "schema_version": 1,
        "algorithm_version": 1,
        "generation": 3,
        "cursor": opaque_cursor("catalog-a", "epoch-a", 12),
        "status": "waiting_for_index",
    }
    monkeypatch.setattr(
        catalog_enrichment,
        "relationship_status",
        lambda _db, _catalog_id: dict(status),
    )

    class Cursor:
        def __init__(self):
            self.executed = []

        def execute(self, sql, args):
            self.executed.append((" ".join(sql.split()), args))

        def fetchall(self):
            return [
                (
                    "album",
                    "album-a",
                    {"entity_type": "album", "entity_id": "album-a", "similar": []},
                    "2026-07-30T12:00:00Z",
                )
            ]

        def close(self):
            return None

    cursor = Cursor()
    db = types.SimpleNamespace(cursor=lambda: cursor)

    page = catalog_enrichment.relationship_bootstrap_page(
        db, "catalog-a", limit=10
    )

    assert page["status"] == "waiting_for_index"
    assert page["generation"] == 3
    assert page["algorithm_version"] == 1
    assert page["relationships"] == [
        {
            "entity_type": "album",
            "entity_id": "album-a",
            "similar": [],
            "computed_at": "2026-07-30T12:00:00Z",
        }
    ]
    assert cursor.executed[0][1][1] == 3


def test_enrichment_change_pages_do_not_read_past_their_pinned_head(monkeypatch):
    from plugins.LumaeAnalysis import catalog_enrichment
    from plugins.LumaeAnalysis.catalog import opaque_cursor

    class Cursor:
        def __init__(self, state_row=None):
            self.state_row = state_row
            self.calls = []

        def execute(self, sql, args):
            self.calls.append((" ".join(sql.split()), args))

        def fetchone(self):
            return self.state_row

        def fetchall(self):
            return []

        def close(self):
            return None

    profile_cursor = Cursor(("profile-epoch", 10, 0))
    profile_db = type("Db", (), {"cursor": lambda _self: profile_cursor})()
    monkeypatch.setattr(
        catalog_enrichment,
        "resolve_catalog_source",
        lambda _db, **_kwargs: [{"catalog_instance_id": "catalog-a"}],
    )

    profile_page = catalog_enrichment.read_profile_changes(
        profile_db,
        opaque_cursor("catalog-a", "profile-epoch", 2),
        catalog_instance_id="catalog-a",
    )

    assert profile_page["has_more"] is True
    assert "seq>%s AND seq<=%s" in profile_cursor.calls[-1][0]
    assert profile_cursor.calls[-1][1][2:4] == (2, 10)

    relationship_cursor = Cursor()
    relationship_db = type(
        "Db", (), {"cursor": lambda _self: relationship_cursor}
    )()
    monkeypatch.setattr(
        catalog_enrichment,
        "relationship_status",
        lambda _db, _catalog_id: {
            "catalog_instance_id": "catalog-a",
            "schema_version": 1,
            "algorithm_version": 1,
            "generation": 4,
            "cursor": opaque_cursor("catalog-a", "relationship-epoch", 20),
            "floor_seq": 0,
            "status": "complete",
        },
    )

    relationship_page = catalog_enrichment.read_relationship_changes(
        relationship_db,
        opaque_cursor("catalog-a", "relationship-epoch", 3),
        catalog_instance_id="catalog-a",
    )

    assert relationship_page["has_more"] is True
    assert "seq>%s AND seq<=%s" in relationship_cursor.calls[-1][0]
    assert relationship_cursor.calls[-1][1][2:4] == (3, 20)


def test_enrichment_stream_endpoints_are_source_scoped_and_nonblocking(monkeypatch):
    mod = load_plugin()
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
    }
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(
        mod,
        "start_relationship_preparation",
        lambda **_kwargs: {"queued": True, "coalesced": False, "job_id": "job-a"},
    )
    monkeypatch.setattr(
        mod,
        "relationship_status",
        lambda _db, catalog_instance_id: {
            "catalog_instance_id": catalog_instance_id,
            "status": "queued",
        },
    )
    monkeypatch.setattr(
        mod,
        "profile_bootstrap_page",
        lambda _db, catalog_instance_id, **_kwargs: {
            "catalog_instance_id": catalog_instance_id,
            "profiles": [{"track_id": "track-a"}],
            "cursor": "profile-cursor",
            "has_more": False,
            "next_page_token": None,
        },
    )
    client = plugin_client(mod)

    queued = client.post(
        "/api/catalog/relationships/prepare",
        json={"catalog_instance_id": "catalog-a"},
    )
    profiles = client.get(
        "/api/profiles/bootstrap?catalog_instance_id=catalog-a"
    )

    assert queued.status_code == 200
    assert queued.get_json()["job_id"] == "job-a"
    assert queued.get_json()["relationships"]["status"] == "queued"
    assert profiles.status_code == 200
    assert profiles.get_json()["profiles"] == [{"track_id": "track-a"}]
    assert profiles.headers["Cache-Control"] == "private, no-cache"


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


def test_bootstrap_session_uses_published_generation_during_refresh(monkeypatch):
    import plugins.LumaeAnalysis.catalog as catalog

    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "catalog": {
            "generation": 7,
            "epoch": "epoch-a",
            "head_seq": 42,
            "status": "scanning",
            "entity_counts": {"track": 100},
        },
        "analysis": {
            "generation": 0,
            "epoch": "analysis-a",
            "head_seq": 0,
            "status": "not_initialized",
        },
    }
    monkeypatch.setattr(catalog, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    db = FakeDb([(0,)])

    result = catalog.create_bootstrap_session(
        db,
        "user:alice",
        stream="catalog",
        server_id="server-a",
        catalog_instance_id="catalog-a",
    )

    assert result["generation"] == 7
    assert result["snapshot_seq"] == 42
    assert result["totals"] == {"track": 100}


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


def test_catalog_changes_report_remaining_events_and_estimated_bytes(monkeypatch):
    import plugins.LumaeAnalysis.catalog as catalog

    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "catalog": {
            "epoch": "epoch-a",
            "floor_seq": 0,
            "head_seq": 10,
        },
    }
    monkeypatch.setattr(catalog, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    db = FakeDb(
        [
            (
                3,
                8,
                "track",
                "track-1",
                "upsert",
                None,
                {"track_id": "track-1", "title": "Song"},
                None,
                "2026-07-29T12:00:00Z",
                "provider_diff",
            )
        ]
    )

    result = catalog.read_catalog_changes(
        db,
        catalog.opaque_cursor("catalog-a", "epoch-a", 2),
    )

    assert result["remaining_events"] == 7
    assert result["estimated_remaining_bytes"] > 0
    assert result["page_estimated_bytes"] > 0
    assert result["fingerprint_schema_version"] == 1
    assert result["snapshot_generation"] == 0
    assert result["snapshot_entity_counts"] == {}
    assert result["snapshot_estimated_bytes"] == 0
    assert result["changes"][0]["change_reason"] == "provider_diff"


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
                "rebind_status": "active",
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


def test_catalog_refresh_blocks_pending_v2_to_v3_rebind(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(
        mod,
        "resolve_catalog_source",
        lambda *_args, **_kwargs: [
            {
                "server_id": "legacy-default",
                "candidate_server_id": "server-a",
                "catalog_instance_id": "catalog-a",
                "rebind_status": "rebind_required",
                "catalog": {
                    "generation": 3,
                    "status": "complete",
                    "completed_at": None,
                },
            }
        ],
    )
    monkeypatch.setattr(mod, "enqueue", lambda *args, **kwargs: calls.append((args, kwargs)))

    response = plugin_client(mod).post(
        "/api/catalog/refresh",
        json={"server_id": "legacy-default", "catalog_instance_id": "catalog-a"},
    )

    assert response.status_code == 409
    assert response.get_json()["error"] == "rebind_required"
    assert calls == []


def test_catalog_prepare_api_queues_first_publication_and_returns_poll_location(monkeypatch):
    mod = load_plugin()
    source = {
        "server_id": "server-a",
        "catalog_instance_id": "catalog-a",
        "rebind_status": "active",
        "catalog": {
            "generation": 0,
            "status": "not_initialized",
            "entity_counts": {},
            "builder_version": 0,
            "refresh_required": True,
            "refresh_reason": "catalog_builder_upgrade",
        },
        "analysis": {"generation": 0, "status": "not_initialized"},
    }
    state = {"value": None}
    queued = []
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(mod, "preparation_state", lambda *_args, **_kwargs: state["value"])

    def claim(_source, db=None):
        state["value"] = {
            "status": "queued",
            "phase": "queued",
            "last_error": None,
            "updated_at": "2099-07-27T12:00:00Z",
        }
        return True

    monkeypatch.setattr(mod, "claim_preparation", claim)
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, server_id, catalog_id, queue="default": queued.append(
            (func, server_id, catalog_id, queue)
        ),
    )

    response = plugin_client(mod).post(
        "/api/catalog/prepare",
        json={"server_id": "server-a", "catalog_instance_id": "catalog-a"},
    )

    assert response.status_code == 202
    assert response.headers["Retry-After"] == "2"
    assert response.headers["Location"].endswith("/api/catalog/prepare/catalog-a")
    assert response.get_json()["status"] == "queued"
    assert response.get_json()["catalog_ready"] is False
    assert queued == [(mod.prepare_lumae_task, "server-a", "catalog-a", "default")]


def test_catalog_prepare_api_coalesces_active_operation(monkeypatch):
    mod = load_plugin()
    source = {
        "server_id": "server-a",
        "catalog_instance_id": "catalog-a",
        "rebind_status": "active",
        "catalog": {
            "generation": 0,
            "status": "scanning",
            "entity_counts": {},
            "builder_version": 0,
            "refresh_required": True,
        },
        "analysis": {"generation": 0, "status": "not_initialized"},
    }
    state = {
        "status": "running",
        "phase": "catalog_refresh",
        "last_error": None,
        "updated_at": "2099-07-27T12:00:00Z",
    }
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(mod, "preparation_state", lambda *_args, **_kwargs: state)
    monkeypatch.setattr(
        mod,
        "claim_preparation",
        lambda *_args, **_kwargs: pytest.fail("active preparation must coalesce"),
    )
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda *_args, **_kwargs: pytest.fail("active preparation must not enqueue twice"),
    )

    response = plugin_client(mod).post(
        "/api/catalog/prepare",
        json={"server_id": "server-a", "catalog_instance_id": "catalog-a"},
    )

    assert response.status_code == 202
    assert response.get_json()["status"] == "running"


def test_catalog_prepare_status_exposes_catalogue_before_analysis_finishes(monkeypatch):
    mod = load_plugin()
    source = {
        "server_id": "server-a",
        "catalog_instance_id": "catalog-a",
        "rebind_status": "active",
        "catalog": {
            "generation": 4,
            "status": "complete",
            "entity_counts": {"track": 21_397},
            "builder_version": mod.CATALOG_BUILDER_VERSION,
            "fingerprint_schema_version": mod.CATALOG_FINGERPRINT_SCHEMA_VERSION,
            "refresh_required": False,
        },
        "analysis": {"generation": 0, "status": "projecting"},
    }
    state = {
        "status": "running",
        "phase": "analysis_projection",
        "last_error": None,
        "updated_at": "2099-07-27T12:00:00Z",
    }
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(mod, "preparation_state", lambda *_args, **_kwargs: state)

    response = plugin_client(mod).get("/api/catalog/prepare/catalog-a")

    assert response.status_code == 200
    assert response.get_json()["catalog_ready"] is True
    assert response.get_json()["analysis_ready"] is False
    assert response.get_json()["phase"] == "analysis_projection"
    assert response.get_json()["counts"]["track"] == 21_397


def test_catalog_prepare_is_idempotent_when_catalogue_is_current_but_analysis_is_pending(
    monkeypatch,
):
    mod = load_plugin()
    source = {
        "server_id": "server-a",
        "catalog_instance_id": "catalog-a",
        "rebind_status": "active",
        "catalog": {
            "generation": 4,
            "status": "complete",
            "entity_counts": {"track": 21_397},
            "builder_version": mod.CATALOG_BUILDER_VERSION,
            "fingerprint_schema_version": mod.CATALOG_FINGERPRINT_SCHEMA_VERSION,
            "refresh_required": False,
        },
        "analysis": {"generation": 0, "status": "projecting"},
    }
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(mod, "preparation_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda *_args, **_kwargs: pytest.fail("a current catalogue must not rebuild"),
    )

    response = plugin_client(mod).post(
        "/api/catalog/prepare",
        json={"server_id": "server-a", "catalog_instance_id": "catalog-a"},
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "operation_id": "catalog-a",
        "status": "ready",
        "phase": "ready",
        "server_id": "server-a",
        "catalog_instance_id": "catalog-a",
        "catalog_ready": True,
        "publication_current": True,
        "generation": 4,
        "counts": {"track": 21_397},
        "published_builder_version": mod.CATALOG_BUILDER_VERSION,
        "current_builder_version": mod.CATALOG_BUILDER_VERSION,
        "fingerprint_schema_version": mod.CATALOG_FINGERPRINT_SCHEMA_VERSION,
        "current_fingerprint_schema_version": mod.CATALOG_FINGERPRINT_SCHEMA_VERSION,
        "snapshot_estimated_bytes": 0,
        "last_scan_change_counts": {},
        "last_scan_change_reason": None,
        "last_scan_duration_ms": None,
        "refresh_required": False,
        "refresh_reason": None,
        "analysis_ready": False,
        "target_plugin_version": None,
        "target_catalog_builder_version": None,
        "worker_plugin_version": None,
        "worker_catalog_builder_version": None,
        "worker_attested": True,
        "last_error": None,
        "updated_at": None,
    }


def test_catalog_health_marks_unattested_worker_publication_for_repair(monkeypatch):
    mod = load_plugin()
    source = settings_catalog_source()
    source["catalog"].update(
        {
            "generation": 4,
            "builder_version": mod.CATALOG_BUILDER_VERSION,
            "refresh_required": False,
        }
    )
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(
        mod,
        "preparation_state",
        lambda *_args, **_kwargs: {
            "status": "ready",
            "phase": "catalog_ready",
            "target_plugin_version": mod.PLUGIN_VERSION,
            "target_catalog_builder_version": mod.CATALOG_BUILDER_VERSION,
            "worker_plugin_version": "0.8.9",
            "worker_catalog_builder_version": mod.CATALOG_BUILDER_VERSION,
        },
    )

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    catalog = response.get_json()["servers"][0]["catalog"]
    assert catalog["refresh_required"] is True
    assert catalog["refresh_reason"] == "worker_version_mismatch"


@pytest.mark.parametrize(
    ("version", "expected_status"),
    [("v2.5.0", "core_too_old"), ("v4.0.0", "core_api_incomplete")],
)
def test_catalog_health_rejects_unsupported_core_before_server_work(monkeypatch, version, expected_status):
    mod = load_plugin()
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", version)

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 409
    assert response.get_json()["status"] == expected_status
    assert response.get_json()["servers"] == []


@pytest.mark.parametrize("version", ["v3.0.6", "v4.0.0", "development-build"])
def test_catalog_health_admits_future_core_when_live_v3_contract_passes(monkeypatch, version):
    mod = load_plugin()
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", version)
    monkeypatch.setattr(plugin_api_module, "active_server_id", lambda: "server-a", raising=False)
    monkeypatch.setattr(plugin_api_module, "use_server", lambda _server_id: None, raising=False)
    monkeypatch.setattr(
        plugin_api_module,
        "list_servers",
        lambda: [{"server_id": "server-a", "server_type": "navidrome"}],
        raising=False,
    )

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 200
    body = response.get_json()
    assert body["supported"] is True
    assert body["core_api_contract"] == "audiomuse_v3_registry_v1"
    assert body["sync_contract"]["core_api_contract"] == body["core_api_contract"]
    assert body["capability"]["contract_revision"] == 1


def test_catalog_health_rejects_malformed_live_v3_registry(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", "v3.99.0")
    monkeypatch.setattr(plugin_api_module, "active_server_id", lambda: "server-a", raising=False)
    monkeypatch.setattr(plugin_api_module, "use_server", lambda _server_id: None, raising=False)
    monkeypatch.setattr(
        plugin_api_module,
        "list_servers",
        lambda: {"server-a": {"server_type": "navidrome"}},
        raising=False,
    )

    response = plugin_client(mod).get("/api/catalog/health")

    assert response.status_code == 409
    assert response.get_json()["status"] == "core_api_incomplete"


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


def test_collection_catalogue_search_projection_normalizes_accents():
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")

    sql = " ".join(library.catalog_track_view_sql().split())

    assert "lower(unaccent(concat_ws(" in sql


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
    source = {"catalog_instance_id": "catalog-a", "server_id": "server-a"}
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(
        mod,
        "fetch_profile_rows",
        lambda ids, catalog_instance_id=None: rows,
    )
    client = plugin_client(mod)

    response = client.get("/api/profiles?ids=ready-1,missing-1,failed-1,skipped-1")

    assert response.status_code == 200
    body = response.get_json()
    assert body["schema_version"] == 1
    assert body["analyzer_version"] == 1
    assert body["catalog_instance_id"] == "catalog-a"
    assert body["profiles"][0]["track_id"] == "ready-1"
    assert body["profiles"][0]["source"] == "waveform"
    assert body["profiles"][0]["start_ramp"] == "6QMA"
    assert body["missing"] == ["missing-1"]
    assert body["failed"] == [
        {"track_id": "failed-1", "reason": "decode failed"},
        {"track_id": "skipped-1", "reason": "missing file path"},
    ]


def test_profiles_endpoint_fails_closed_when_source_is_ambiguous(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(
        mod,
        "resolve_profile_source",
        lambda **_kwargs: (_ for _ in ()).throw(
            ValueError("An explicit catalog_instance_id is required")
        ),
    )

    response = plugin_client(mod).get("/api/profiles?ids=track-a")

    assert response.status_code == 409
    assert response.get_json()["error"] == "source_required"


def test_scoped_profile_writes_use_catalogue_and_track_composite_key(monkeypatch):
    mod = load_plugin()
    db = FakeDb(rows=[])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(
        mod,
        "source_profiles_table",
        lambda: "plugin_lumae_analysis__source_profiles",
    )

    mod.mark_pending(["same-track-id"], catalog_instance_id="catalog-b")

    sql, params = db.cursor_obj.executed[-1]
    assert "ON CONFLICT (catalog_instance_id, track_id)" in sql
    assert params[:2] == ("catalog-b", ["same-track-id"])


def test_analyze_endpoint_promotes_pending_and_enqueues_small_high_priority_chunks(monkeypatch):
    mod = load_plugin()
    calls = []
    rows = [
        {"track_id": "ready-1", "status": "ready"},
        {"track_id": "pending-1", "status": "pending"},
        {"track_id": "stale-1", "status": "stale"},
    ]
    source = {"catalog_instance_id": "catalog-a", "server_id": "server-a"}
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(
        mod,
        "fetch_profile_rows",
        lambda ids, catalog_instance_id=None: rows,
    )
    monkeypatch.setattr(
        mod,
        "mark_pending",
        lambda ids, catalog_instance_id=None, priority="background": calls.append(
            ("mark_pending", ids, catalog_instance_id, priority)
        ),
    )
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, *args, queue="default": calls.append((func.__name__, args, queue)),
    )
    client = plugin_client(mod)

    response = client.post(
        "/api/analyze",
        json={
            "catalog_instance_id": "catalog-a",
            "ids": ["ready-1", "pending-1", "stale-1", "missing-1"],
        },
    )

    assert response.status_code == 202
    assert response.get_json() == {
        "accepted": ["stale-1", "missing-1"],
        "already_ready": ["ready-1"],
        "already_pending": ["pending-1"],
    }
    assert calls == [
        (
            "mark_pending",
            ["stale-1", "missing-1", "pending-1"],
            "catalog-a",
            "interactive",
        ),
        (
            "analyze_tracks_task",
            (
                ["stale-1", "missing-1", "pending-1"],
                "catalog-a",
                "server-a",
                "interactive",
            ),
            "high",
        ),
    ]


def test_analyze_song_hook_uses_analysis_audio_path_and_raw_media_item(monkeypatch, tmp_path):
    mod = load_plugin()
    audio = tmp_path / "analysis-hook.flac"
    audio.write_bytes(b"hook audio")
    db = FakeDb(rows=[])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(
        mod,
        "source_profiles_table",
        lambda: "plugin_lumae_analysis__source_profiles",
    )
    monkeypatch.setattr(
        mod,
        "resolve_profile_source",
        lambda **_kwargs: {"catalog_instance_id": "catalog-a", "server_id": "legacy-default"},
    )

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
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("per-song hooks must not enqueue full projections")
        ),
    )
    monkeypatch.setattr(
        mod,
        "record_analysis_run",
        lambda server_id, catalog_instance_id, run_id: seen.update(
            {
                "run": (server_id, catalog_instance_id, run_id),
            }
        ),
    )

    result = mod.analyze_song_hook(
        {
            "item_id": "track-a",
            "run_id": "analysis-run-a",
            "audio_path": str(audio),
            "metadata": {"file_path": "/metadata/path.flac"},
            "media_item": {"Id": "raw-track", "FilePath": "/music/raw-song.flac"},
        }
    )

    assert result == {"track_id": "track-a", "status": "ready"}
    assert seen["path"] == str(audio)
    assert seen["run"] == ("legacy-default", "catalog-a", "analysis-run-a")
    assert audio.exists()
    params = next(
        params
        for sql, params in db.cursor_obj.executed
        if "INSERT INTO plugin_lumae_analysis__source_profiles" in sql
    )
    assert params[0] == "catalog-a"
    assert params[1] == "track-a"
    assert params[2] == 44100
    assert params[9].startswith("analysis-hook|track-a|/music/raw-song.flac|")
    assert params[11] == "ready"


def test_analysis_run_events_coalesce_to_one_source_finalizer(monkeypatch):
    mod = load_plugin()
    db = AnalysisRunDb()
    queued = []

    def enqueue_finalizer(server_id, catalog_instance_id, run_id):
        queued.append((server_id, catalog_instance_id, run_id))
        return types.SimpleNamespace(id="finalizer-a")

    monkeypatch.setattr(mod, "enqueue_analysis_run_finalizer", enqueue_finalizer)

    first = mod.record_analysis_run("server-a", "catalog-a", "run-a", db=db)
    second = mod.record_analysis_run("server-a", "catalog-a", "run-a", db=db)

    assert first == {"queued": True, "coalesced": False, "job_id": "finalizer-a"}
    assert second == {"queued": False, "coalesced": True}
    assert queued == [("server-a", "catalog-a", "run-a")]
    assert db.runs[("catalog-a", "run-a")]["songs_seen"] == 2
    assert db.runs[("catalog-a", "run-a")]["status"] == "queued"


def test_one_multiserver_analysis_run_gets_one_finalizer_per_source(monkeypatch):
    mod = load_plugin()
    db = AnalysisRunDb()
    queued = []
    monkeypatch.setattr(
        mod,
        "enqueue_analysis_run_finalizer",
        lambda server_id, catalog_id, run_id: queued.append(
            (server_id, catalog_id, run_id)
        )
        or types.SimpleNamespace(id=f"finalizer-{catalog_id}"),
    )

    mod.record_analysis_run("server-a", "catalog-a", "shared-run", db=db)
    mod.record_analysis_run("server-b", "catalog-b", "shared-run", db=db)
    mod.record_analysis_run("server-a", "catalog-a", "shared-run", db=db)
    mod.record_analysis_run("server-b", "catalog-b", "shared-run", db=db)

    assert queued == [
        ("server-a", "catalog-a", "shared-run"),
        ("server-b", "catalog-b", "shared-run"),
    ]
    assert db.runs[("catalog-a", "shared-run")]["songs_seen"] == 2
    assert db.runs[("catalog-b", "shared-run")]["songs_seen"] == 2


def test_analysis_run_enqueue_failure_is_retryable_by_the_next_song(monkeypatch):
    mod = load_plugin()
    db = AnalysisRunDb()
    attempts = []

    def enqueue_finalizer(*args):
        attempts.append(args)
        if len(attempts) == 1:
            raise RuntimeError("redis unavailable")
        return types.SimpleNamespace(id="finalizer-retry")

    monkeypatch.setattr(mod, "enqueue_analysis_run_finalizer", enqueue_finalizer)

    with pytest.raises(RuntimeError, match="redis unavailable"):
        mod.record_analysis_run("server-a", "catalog-a", "run-a", db=db)
    assert db.runs[("catalog-a", "run-a")]["status"] == "enqueue_failed"

    result = mod.record_analysis_run("server-a", "catalog-a", "run-a", db=db)

    assert result["job_id"] == "finalizer-retry"
    assert len(attempts) == 2
    assert db.runs[("catalog-a", "run-a")]["songs_seen"] == 2
    assert db.runs[("catalog-a", "run-a")]["status"] == "queued"


def test_analysis_run_finalizer_refreshes_projects_then_queues_only_needed_profiles(monkeypatch):
    mod = load_plugin()
    db = AnalysisRunDb()
    db.runs[("catalog-a", "run-a")] = {
        "server_id": "server-a",
        "status": "queued",
        "songs_seen": 37,
        "job_id": "finalizer-a",
        "queued_profiles": 0,
        "profile_jobs": 0,
        "last_error": None,
    }
    calls = []
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(
        mod,
        "resolve_profile_source",
        lambda **kwargs: calls.append(("resolve", kwargs))
        or {"catalog_instance_id": "catalog-a", "server_id": "server-a"},
    )
    monkeypatch.setattr(
        mod,
        "refresh_catalog",
        lambda server_id=None: calls.append(("refresh", server_id))
        or {"catalog_instance_id": "catalog-a", "generation": 8},
    )
    adapter = object()
    monkeypatch.setattr(mod, "get_core_adapter", lambda: adapter)
    monkeypatch.setattr(
        mod,
        "project_analysis",
        lambda **kwargs: calls.append(("project", kwargs))
        or {"catalog_instance_id": "catalog-a", "generation": 12},
    )
    monkeypatch.setattr(
        mod,
        "start_profile_backfill",
        lambda **kwargs: calls.append(("profiles", kwargs))
        or {"queued": True, "coalesced": False, "batch_size": 4},
    )
    monkeypatch.setattr(
        mod,
        "start_relationship_preparation",
        lambda **kwargs: calls.append(("relationships", kwargs))
        or {"queued": True, "coalesced": False},
    )

    result = mod.finalize_analysis_run_task("server-a", "catalog-a", "run-a")

    assert [name for name, _value in calls] == [
        "resolve",
        "refresh",
        "project",
        "profiles",
        "relationships",
    ]
    assert result["songs_seen"] == 37
    assert result["status"] == "complete"
    assert db.runs[("catalog-a", "run-a")]["status"] == "complete"
    assert db.runs[("catalog-a", "run-a")]["queued_profiles"] == 4


def test_analysis_run_finalizer_failure_is_recorded_and_can_be_retried(monkeypatch):
    mod = load_plugin()
    db = AnalysisRunDb()
    db.runs[("catalog-a", "run-a")] = {
        "server_id": "server-a",
        "status": "queued",
        "songs_seen": 1,
        "job_id": "finalizer-a",
        "queued_profiles": 0,
        "profile_jobs": 0,
        "last_error": None,
    }
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(
        mod,
        "resolve_profile_source",
        lambda **_kwargs: {"catalog_instance_id": "catalog-a", "server_id": "server-a"},
    )
    attempts = []

    def refresh(server_id=None):
        attempts.append(server_id)
        if len(attempts) == 1:
            raise RuntimeError("provider temporarily unavailable")
        return {"catalog_instance_id": "catalog-a"}

    monkeypatch.setattr(mod, "refresh_catalog", refresh)
    monkeypatch.setattr(mod, "get_core_adapter", lambda: object())
    monkeypatch.setattr(
        mod,
        "project_analysis",
        lambda **_kwargs: {"catalog_instance_id": "catalog-a"},
    )
    monkeypatch.setattr(
        mod,
        "start_profile_backfill",
        lambda **_kwargs: {"queued": False, "coalesced": True, "batch_size": 10},
    )
    monkeypatch.setattr(
        mod,
        "start_relationship_preparation",
        lambda **_kwargs: {"queued": False, "coalesced": True},
    )

    with pytest.raises(RuntimeError, match="temporarily unavailable"):
        mod.finalize_analysis_run_task("server-a", "catalog-a", "run-a")
    assert db.runs[("catalog-a", "run-a")]["status"] == "failed"
    assert "temporarily unavailable" in db.runs[("catalog-a", "run-a")]["last_error"]

    result = mod.finalize_analysis_run_task("server-a", "catalog-a", "run-a")

    assert result["status"] == "complete"
    assert len(attempts) == 2


def test_running_analysis_finalizer_can_only_be_reclaimed_by_the_same_rq_job():
    mod = load_plugin()
    db = AnalysisRunDb()
    db.runs[("catalog-a", "run-a")] = {
        "server_id": "server-a",
        "status": "running",
        "songs_seen": 9,
        "job_id": "finalizer-a",
        "queued_profiles": 0,
        "profile_jobs": 0,
        "last_error": None,
    }

    assert (
        mod.claim_analysis_run(
            "catalog-a",
            "run-a",
            finalizer_job_id="different-job",
            db=db,
        )
        is None
    )
    assert (
        mod.claim_analysis_run(
            "catalog-a",
            "run-a",
            finalizer_job_id="finalizer-a",
            db=db,
        )
        == 9
    )


def test_analysis_run_finalizer_identity_is_scoped_by_catalogue():
    mod = load_plugin()

    first = mod.analysis_run_finalizer_job_id("catalog-a", "shared-run")
    same = mod.analysis_run_finalizer_job_id("catalog-a", "shared-run")
    other_source = mod.analysis_run_finalizer_job_id("catalog-b", "shared-run")

    assert first == same
    assert first != other_source


def test_analysis_run_finalizer_waits_for_parent_even_when_parent_fails(monkeypatch):
    from rq.exceptions import NoSuchJobError
    from rq.job import Job

    mod = load_plugin()
    captured = {}

    class Queue:
        connection = object()

        def enqueue(self, func, **kwargs):
            captured["func"] = func
            captured.update(kwargs)
            return types.SimpleNamespace(id=kwargs["job_id"])

    monkeypatch.setattr(plugin_api_module, "rq_queue_default", Queue(), raising=False)
    monkeypatch.setattr(
        plugin_api_module,
        "dotted_path",
        lambda func: f"{func.__module__}.{func.__name__}",
        raising=False,
    )
    monkeypatch.setattr(
        Job,
        "fetch",
        classmethod(
            lambda _cls, _job_id, connection=None: (_ for _ in ()).throw(NoSuchJobError())
        ),
    )

    job = mod.enqueue_analysis_run_finalizer("server-a", "catalog-a", "run-a")

    assert job.id == mod.analysis_run_finalizer_job_id("catalog-a", "run-a")
    assert captured["func"] == "plugin.manager.run_plugin_task"
    assert captured["args"][1:] == ("server-a", "catalog-a", "run-a")
    assert captured["depends_on"].dependencies == ["run-a"]
    assert captured["depends_on"].allow_failure is True
    assert captured["retry"].max == 2


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


def test_streaming_analysis_matches_v1_whole_buffer_reference_profile():
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

    result = loudness.analyze_buffer(audio, sample_rate)
    weighted = np.stack([reference_k_weight(channel) for channel in audio])
    chunk_size = int(sample_rate * loudness.CHUNK_DURATION_MS / 1000)
    chunk_lufs = [
        loudness._mean_square_to_lufs(
            float(np.mean(weighted[:, start : start + chunk_size] ** 2))
        )
        for start in range(0, weighted.shape[1], chunk_size)
    ]
    reference_lufs = loudness._integrated_lufs(chunk_lufs)
    relative = [value - reference_lufs for value in chunk_lufs]
    reference_start = loudness._scan_forward(relative)
    reference_end = loudness._scan_backward(relative)

    assert result.sample_rate == sample_rate
    assert result.duration_ms == 2000
    assert math.isclose(result.ref_lufs, reference_lufs, rel_tol=0, abs_tol=1e-10)
    assert result.start_ramp == reference_start
    assert result.end_ramp == reference_end
    assert result.start_ramp_blob == loudness.encode_ramp(reference_start)
    assert result.end_ramp_blob == loudness.encode_ramp(reference_end)


def test_analyze_buffer_uses_100ms_chunks_and_expected_ramp_encoding(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    monkeypatch.setattr(
        loudness.scipy_signal,
        "lfilter",
        lambda _b, _a, samples, zi=None: (
            samples.astype(np.float64, copy=False),
            np.asarray(zi, dtype=np.float64),
        ),
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
        loudness.scipy_signal,
        "lfilter",
        lambda _b, _a, samples, zi=None: (
            samples.astype(np.float64, copy=False),
            np.asarray(zi, dtype=np.float64),
        ),
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


def test_analyze_file_streams_pyav_frames_into_bounded_analyzer(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    captured = {}
    audio = np.array([[0.25, -0.25]], dtype=np.float32)
    sentinel = object()

    class Frame:
        def to_ndarray(self):
            return audio

    class Container:
        streams = types.SimpleNamespace(
            audio=[
                types.SimpleNamespace(
                    codec_context=types.SimpleNamespace(
                        sample_rate=44100,
                        layout=types.SimpleNamespace(name="mono", channels=("front",)),
                    )
                )
            ]
        )

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def decode(self, _stream):
            return [Frame()]

    class Resampler:
        def __init__(self, **kwargs):
            captured["resampler"] = kwargs

        def resample(self, frame):
            return [] if frame is None else [frame]

    def fake_open(path):
        captured["path"] = path
        return Container()

    fake_av = types.SimpleNamespace(
        open=fake_open,
        audio=types.SimpleNamespace(
            resampler=types.SimpleNamespace(AudioResampler=Resampler)
        ),
    )

    def fake_analyze_blocks(blocks, sample_rate, **kwargs):
        captured["analyze"] = (list(blocks), sample_rate, kwargs)
        return sentinel

    monkeypatch.setitem(sys.modules, "av", fake_av)
    monkeypatch.setattr(loudness, "analyze_blocks", fake_analyze_blocks)

    result = loudness.analyze_file("fixture.wav")

    assert result is sentinel
    assert captured["path"] == "fixture.wav"
    assert captured["resampler"] == {
        "format": "fltp",
        "layout": "mono",
        "rate": 44100,
    }
    blocks, sample_rate, kwargs = captured["analyze"]
    assert np.array_equal(blocks[0], audio)
    assert sample_rate == 44100
    assert kwargs["channel_count"] == 1


def test_streaming_loudness_is_invariant_to_decoder_block_boundaries():
    import plugins.LumaeAnalysis.loudness as loudness

    sample_rate = 48000
    time_axis = np.arange(sample_rate * 3 + 117, dtype=np.float32) / sample_rate
    audio = np.stack(
        [
            np.sin(2 * np.pi * 220 * time_axis) * 0.2,
            np.sin(2 * np.pi * 440 * time_axis) * 0.1,
        ]
    ).astype(np.float32)

    whole = loudness.analyze_blocks(
        [audio],
        sample_rate,
        channel_count=2,
    )
    split = loudness.analyze_blocks(
        (
            audio[:, start : start + 7777]
            for start in range(0, audio.shape[1], 7777)
        ),
        sample_rate,
        channel_count=2,
    )

    assert split.duration_ms == whole.duration_ms
    assert split.ref_lufs == whole.ref_lufs
    assert split.start_ramp_blob == whole.start_ramp_blob
    assert split.end_ramp_blob == whole.end_ramp_blob


def test_streaming_loudness_enforces_duration_and_deadline_limits():
    import plugins.LumaeAnalysis.loudness as loudness

    with pytest.raises(loudness.ProfileResourceLimitError, match="duration"):
        loudness.analyze_blocks(
            [np.ones((1, 3), dtype=np.float32)],
            10,
            channel_count=1,
            max_duration_seconds=0.2,
        )

    with pytest.raises(loudness.ProfileAnalysisTimeout, match="deadline"):
        loudness.analyze_blocks(
            [np.ones((1, 1), dtype=np.float32)],
            10,
            channel_count=1,
            deadline=0,
        )


def test_streaming_loudness_caps_channels_and_sample_rate():
    import plugins.LumaeAnalysis.loudness as loudness

    with pytest.raises(loudness.ProfileResourceLimitError, match="channel count"):
        loudness.analyze_blocks(
            [np.ones((loudness.MAX_PROFILE_CHANNELS + 1, 10), dtype=np.float32)],
            48000,
            channel_count=loudness.MAX_PROFILE_CHANNELS + 1,
        )
    with pytest.raises(loudness.ProfileResourceLimitError, match="sample rate"):
        loudness.analyze_blocks(
            [np.ones((1, 10), dtype=np.float32)],
            loudness.MAX_PROFILE_SAMPLE_RATE + 1,
            channel_count=1,
        )


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


class AnalysisRunCursor:
    def __init__(self, db):
        self.db = db
        self.current_row = None
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        normalized = " ".join(sql.split())
        self.current_row = None
        if normalized.startswith("INSERT INTO plugin_lumae_analysis__analysis_runs"):
            catalog_id, run_id, server_id = params
            key = (catalog_id, run_id)
            if key not in self.db.runs:
                self.db.runs[key] = {
                    "server_id": server_id,
                    "status": "registering",
                    "songs_seen": 1,
                    "job_id": None,
                    "queued_profiles": 0,
                    "profile_jobs": 0,
                    "last_error": None,
                }
                self.current_row = (run_id,)
        elif "SET songs_seen=songs_seen + 1" in normalized:
            row = self.db.runs[(params[0], params[1])]
            row["songs_seen"] += 1
        elif "SET status='registering'" in normalized:
            row = self.db.runs[(params[0], params[1])]
            if row["status"] == "enqueue_failed":
                row["status"] = "registering"
                row["last_error"] = None
                self.current_row = (params[1],)
        elif "SET status='running'" in normalized:
            row = self.db.runs[(params[0], params[1])]
            same_retry = (
                row["status"] == "running"
                and params[2] is not None
                and row["job_id"] == params[2]
            )
            if row["status"] in {"registering", "queued", "enqueue_failed", "failed"} or same_retry:
                row["status"] = "running"
                row["last_error"] = None
                self.current_row = (row["songs_seen"],)
        elif normalized.startswith("UPDATE plugin_lumae_analysis__analysis_runs"):
            status, job_id, queued_profiles, profile_jobs, error = params[:5]
            key = (params[-2], params[-1])
            row = self.db.runs[key]
            row["status"] = status
            if job_id is not None:
                row["job_id"] = job_id
            if queued_profiles is not None:
                row["queued_profiles"] = queued_profiles
            if profile_jobs is not None:
                row["profile_jobs"] = profile_jobs
            row["last_error"] = error

    def fetchone(self):
        return self.current_row

    def close(self):
        pass


class AnalysisRunDb:
    def __init__(self):
        self.runs = {}
        self.cursor_obj = AnalysisRunCursor(self)
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
        self.flask_hooks = []
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

    def on_flask_start(self, func):
        self.flask_hooks.append(func)

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
    sig = mod.media_signature(str(current))
    rows = [
        ("eligible-missing", str(current), None, None, None),
        ("eligible-stale", str(current), sig, mod.ANALYZER_VERSION, "stale"),
    ]
    db = FakeDb(rows=rows)
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.find_backfill_ids(limit=2) == ["eligible-missing", "eligible-stale"]
    sql, params = db.cursor_obj.executed[-1]
    assert "LIMIT %s" in sql
    assert params[-2] is True
    assert params[-1] == 2


def test_explicit_prepare_retry_includes_failed_profiles(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(
        mod,
        "fetch_analysis_rows",
        lambda **_kwargs: [("failed-track", "catalog-media:sig", "sig", 1, "failed")],
    )

    assert mod.find_all_backfill_ids(
        catalog_instance_id="catalog-a",
        server_id="server-a",
        include_failed=True,
    ) == ["failed-track"]


def test_backfill_uses_configured_batch_size(monkeypatch):
    mod = load_plugin()
    seen_limits = []
    monkeypatch.setattr(
        mod,
        "get_setting",
        lambda key, default=None: 7 if key == "backfill_batch_size" else default,
    )
    monkeypatch.setattr(mod, "find_backfill_ids", lambda limit: seen_limits.append(limit) or [])

    assert mod.backfill_missing_profiles() == {
        "ready": 0,
        "already_ready": 0,
        "promoted": 0,
        "failed": 0,
        "skipped": 0,
        "deferred": 0,
    }
    assert seen_limits == [7]


def test_analysis_status_counts_are_aggregated_in_postgres(monkeypatch):
    mod = load_plugin()
    db = FakeDb(rows=[(7, 1, 1, 1, 0)])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.analysis_status_counts() == {
        "total_with_files": 7,
        "ready_current": 1,
        "pending": 1,
        "failed": 1,
        "skipped": 0,
        "needs_analysis": 4,
    }
    sql, params = db.cursor_obj.executed[-1]
    assert "COUNT(*) FILTER" in sql
    assert params == (mod.ANALYZER_VERSION, True)


def test_analysis_status_counts_treats_retryable_skipped_rows_as_needed(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=[(1, 0, 0, 0, 0)]))
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


def test_postgres_backfill_retries_registry_backed_skipped_profiles(
    monkeypatch,
    lumae_postgres_db,
):
    mod = load_plugin()
    from plugins.LumaeAnalysis import catalog

    catalog.migrate_catalog(lumae_postgres_db)
    cur = lumae_postgres_db.cursor()
    cur.execute(
        """
        CREATE TABLE plugin_lumae_analysis__source_profiles (
            catalog_instance_id TEXT NOT NULL,
            track_id TEXT NOT NULL,
            media_signature TEXT,
            analyzer_ver INTEGER,
            status TEXT,
            PRIMARY KEY (catalog_instance_id, track_id)
        )
        """
    )
    cur.execute(
        """
        INSERT INTO plugin_lumae_analysis__catalog_sources
            (catalog_instance_id, current_core_server_id, provider_type,
             server_name, is_default, rebind_status)
        VALUES ('catalog-a', 'server-a', 'navidrome', 'Registry source', TRUE, 'active')
        """
    )
    cur.execute(
        """
        INSERT INTO plugin_lumae_analysis__catalog_state
            (catalog_instance_id, current_core_server_id, provider_type,
             published_generation, catalog_epoch, status)
        VALUES ('catalog-a', 'server-a', 'navidrome', 4, 'epoch-a', 'complete')
        """
    )
    cur.execute(
        """
        INSERT INTO plugin_lumae_analysis__catalog_tracks
            (catalog_instance_id, published_generation, track_id, title,
             analysis_eligible, metadata_fp, media_fp, payload,
             first_seen_at, last_seen_at)
        VALUES
            ('catalog-a', 4, 'retry-me', 'Retry me', TRUE, 'meta-a', 'media-a',
             '{}'::jsonb, now(), now()),
            ('catalog-a', 4, 'pending', 'Pending', TRUE, 'meta-b', 'media-b',
             '{}'::jsonb, now(), now())
        """
    )
    cur.execute(
        """
        INSERT INTO plugin_lumae_analysis__source_profiles
            (catalog_instance_id, track_id, media_signature, analyzer_ver, status)
        VALUES
            ('catalog-a', 'retry-me', NULL, 1, 'skipped_no_file'),
            ('catalog-a', 'pending', NULL, 1, 'pending')
        """
    )
    cur.close()
    lumae_postgres_db.commit()
    monkeypatch.setattr(mod, "get_db", lambda: lumae_postgres_db)

    rows = mod.fetch_backfill_rows(
        3,
        catalog_instance_id="catalog-a",
        server_id="server-a",
    )
    counts = mod.analysis_status_counts(
        catalog_instance_id="catalog-a",
        server_id="server-a",
    )

    assert [row[0] for row in rows] == ["retry-me"]
    assert counts == {
        "total_with_files": 2,
        "ready_current": 0,
        "pending": 1,
        "failed": 0,
        "skipped": 0,
        "needs_analysis": 1,
    }


def test_postgres_relationship_migration_preserves_published_generation(
    lumae_postgres_db,
):
    from plugins.LumaeAnalysis import catalog, catalog_enrichment

    catalog.migrate_catalog(lumae_postgres_db)
    cur = lumae_postgres_db.cursor()
    cur.execute(
        """
        INSERT INTO plugin_lumae_analysis__catalog_sources
            (catalog_instance_id, current_core_server_id, provider_type, server_name)
        VALUES ('catalog-a', 'server-a', 'navidrome', 'Main source')
        """
    )
    cur.close()
    catalog_enrichment.migrate_enrichment(lumae_postgres_db)
    cur = lumae_postgres_db.cursor()
    cur.execute(
        """
        UPDATE plugin_lumae_analysis__relationship_state
           SET relationship_schema_version=1,
               algorithm_version=0,
               result_generation=5,
               status='complete'
         WHERE catalog_instance_id='catalog-a'
        """
    )
    cur.close()
    lumae_postgres_db.commit()

    catalog_enrichment.migrate_enrichment(lumae_postgres_db)
    cur = lumae_postgres_db.cursor()
    cur.execute(
        """
        SELECT relationship_schema_version, algorithm_version,
               result_generation, status
          FROM plugin_lumae_analysis__relationship_state
         WHERE catalog_instance_id='catalog-a'
        """
    )
    state = cur.fetchone()
    cur.close()

    assert state == (1, 0, 5, "stale")


def test_queue_backfill_batch_marks_pending_and_enqueues_next_batch(monkeypatch):
    mod = load_plugin()
    calls = []

    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)
    monkeypatch.setattr(
        mod,
        "find_backfill_ids",
        lambda limit: calls.append(("find", limit)) or ["a", "b"],
    )
    monkeypatch.setattr(
        mod,
        "mark_pending",
        lambda ids, priority="background": calls.append(("mark_pending", ids, priority)),
    )
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, *args, queue="default": calls.append((func.__name__, args, queue)),
    )

    assert mod.queue_backfill_batch() == {"queued": 2, "limit": 3}
    assert calls == [
        ("find", 3),
        ("mark_pending", ["a", "b"], "background"),
        ("analyze_tracks_task", (["a", "b"], None, None, "background"), "default"),
    ]


def test_start_profile_backfill_claims_one_bounded_default_queue_chain(monkeypatch):
    mod = load_plugin()
    calls = []
    source = {"catalog_instance_id": "catalog-a", "server_id": "server-a"}
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(mod, "claim_profile_backfill", lambda selected: selected == source)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 10)
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, *args, queue="default": calls.append((func.__name__, args, queue))
        or types.SimpleNamespace(id="backfill-a"),
    )

    assert mod.start_profile_backfill("catalog-a", "server-a") == {
        "queued": True,
        "coalesced": False,
        "batch_size": 10,
        "job_id": "backfill-a",
    }
    assert calls == [("profile_backfill_task", ("server-a", "catalog-a"), "default")]


def test_start_profile_backfill_coalesces_while_chain_is_active(monkeypatch):
    mod = load_plugin()
    calls = []
    source = {"catalog_instance_id": "catalog-a", "server_id": "server-a"}
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(mod, "claim_profile_backfill", lambda _source: False)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 10)
    monkeypatch.setattr(mod, "enqueue", lambda *args, **kwargs: calls.append((args, kwargs)))

    assert mod.start_profile_backfill("catalog-a", "server-a") == {
        "queued": False,
        "coalesced": True,
        "batch_size": 10,
    }
    assert calls == []


def test_profile_backfill_task_processes_one_batch_then_yields_worker(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(
        mod,
        "resolve_profile_source",
        lambda **kwargs: calls.append(("resolve", kwargs))
        or {"catalog_instance_id": "catalog-b", "server_id": "server-b"},
    )
    monkeypatch.setattr(
        mod,
        "update_profile_backfill_state",
        lambda *args, **kwargs: calls.append(("state", args, kwargs)),
    )
    monkeypatch.setattr(mod, "recover_stale_pending_profiles", lambda catalog_id: 0)
    batches = iter([["a", "b"], ["c"]])
    monkeypatch.setattr(
        mod,
        "find_backfill_ids",
        lambda *args, **kwargs: next(batches),
    )
    monkeypatch.setattr(
        mod,
        "mark_pending",
        lambda ids, catalog_instance_id=None, priority="background": calls.append(
            ("pending", ids, catalog_instance_id, priority)
        ),
    )
    monkeypatch.setattr(
        mod,
        "analyze_tracks_task",
        lambda *args, **kwargs: calls.append(("analyze", args, kwargs))
        or {"ready": 2, "already_ready": 0, "promoted": 0, "failed": 0, "skipped": 0},
    )
    monkeypatch.setattr(
        mod,
        "enqueue_next_profile_backfill",
        lambda *args: calls.append(("next", args)),
    )

    result = mod.profile_backfill_task("server-b", "catalog-b")

    assert result["status"] == "queued"
    assert result["processed"] == 2
    assert result["queued_next"] is True
    assert ("pending", ["a", "b"], "catalog-b", "background") in calls
    assert ("next", ("server-b", "catalog-b")) in calls


def test_profile_backfill_task_releases_claimed_rows_when_batch_crashes(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(
        mod,
        "resolve_profile_source",
        lambda **_kwargs: {"catalog_instance_id": "catalog-a", "server_id": "server-a"},
    )
    monkeypatch.setattr(
        mod,
        "update_profile_backfill_state",
        lambda *args, **kwargs: calls.append(("state", args, kwargs)),
    )
    monkeypatch.setattr(mod, "recover_stale_pending_profiles", lambda _catalog_id: 0)
    monkeypatch.setattr(mod, "find_backfill_ids", lambda *_args, **_kwargs: ["track-a"])
    monkeypatch.setattr(mod, "mark_pending", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        mod,
        "analyze_tracks_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("decoder crashed")),
    )
    monkeypatch.setattr(
        mod,
        "release_pending",
        lambda ids, catalog_instance_id=None, reason=None: calls.append(
            ("release", ids, catalog_instance_id, reason)
        ),
    )

    with pytest.raises(RuntimeError, match="decoder crashed"):
        mod.profile_backfill_task("server-a", "catalog-a")

    assert calls[-2][0:3] == ("release", ["track-a"], "catalog-a")
    assert "decoder crashed" in calls[-2][3]
    assert calls[-1][0] == "state"
    assert calls[-1][1][2] == "failed"


def test_legacy_oversized_background_job_is_drained_into_bounded_chain(monkeypatch):
    mod = load_plugin()
    ids = [f"track-{index}" for index in range(mod.MAX_BACKFILL_BATCH_SIZE + 1)]
    calls = []
    monkeypatch.setattr(
        mod,
        "release_pending",
        lambda selected, catalog_instance_id=None, reason=None: calls.append(
            ("release", selected, catalog_instance_id, reason)
        ),
    )
    monkeypatch.setattr(
        mod,
        "start_profile_backfill",
        lambda **kwargs: calls.append(("start", kwargs)) or {"queued": True},
    )

    result = mod.analyze_tracks_task(ids, "catalog-a", "server-a")

    assert result["deferred"] == len(ids)
    assert calls[0][0:3] == ("release", ids, "catalog-a")
    assert "bounded 0.8.1" in calls[0][3]
    assert calls[1] == (
        "start",
        {"catalog_instance_id": "catalog-a", "server_id": "server-a"},
    )


def test_background_task_skips_ready_and_interactively_promoted_tracks(monkeypatch):
    mod = load_plugin()
    rows = {
        "ready": {
            "track_id": "ready",
            "status": "ready",
            "analyzer_ver": mod.ANALYZER_VERSION,
            "media_signature": "catalog-media:same",
        },
        "promoted": {
            "track_id": "promoted",
            "status": "pending_interactive",
            "analyzer_ver": mod.ANALYZER_VERSION,
            "media_signature": None,
        },
    }
    monkeypatch.setattr(
        mod,
        "fetch_profile_rows",
        lambda ids, catalog_instance_id=None: [rows[ids[0]]],
    )
    monkeypatch.setattr(mod, "catalog_media_signature", lambda *_args: "catalog-media:same")
    monkeypatch.setattr(
        mod,
        "analyze_one_track",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not analyze")),
    )
    monkeypatch.setattr(mod, "finalize_preparation_if_settled", lambda _catalog_id: None)

    result = mod.analyze_tracks_task(["ready", "promoted"], "catalog-a", "server-a")

    assert result == {
        "ready": 0,
        "already_ready": 1,
        "promoted": 1,
        "failed": 0,
        "skipped": 0,
        "deferred": 0,
    }


def test_profile_enqueue_failure_releases_pending_rows_for_retry(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(
        mod,
        "mark_pending",
        lambda ids, catalog_instance_id=None, priority="background": calls.append(
            ("pending", ids, catalog_instance_id, priority)
        ),
    )
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("redis down")),
    )
    monkeypatch.setattr(
        mod,
        "release_pending",
        lambda ids, catalog_instance_id=None, reason=None: calls.append(
            ("released", ids, catalog_instance_id, reason)
        ),
    )

    with pytest.raises(RuntimeError, match="redis down"):
        mod.enqueue_profile_analysis(["track-a"], "catalog-a", "server-a")

    assert calls[0] == ("pending", ["track-a"], "catalog-a", "background")
    assert calls[1][:3] == ("released", ["track-a"], "catalog-a")
    assert "redis down" in calls[1][3]


def test_prepare_lumae_marks_catalog_ready_before_background_profiles(monkeypatch):
    mod = load_plugin()
    calls = []
    source = settings_catalog_source()
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(
        mod,
        "update_preparation_state",
        lambda *args, **kwargs: calls.append(("state", args[2], args[3])),
    )
    monkeypatch.setattr(
        mod,
        "refresh_catalog",
        lambda server_id=None: calls.append(("catalog", server_id))
        or {
            "catalog_instance_id": "catalog-a",
            "builder_version": mod.CATALOG_BUILDER_VERSION,
            "refresh_required": False,
        },
    )
    monkeypatch.setattr(
        mod,
        "project_analysis",
        lambda server_id=None, adapter=None: calls.append(("projection", server_id))
        or {"catalog_instance_id": "catalog-a"},
    )
    monkeypatch.setattr(mod, "get_core_adapter", lambda: object())
    monkeypatch.setattr(
        mod,
        "start_profile_backfill",
        lambda **kwargs: calls.append(("profiles", kwargs))
        or {"queued": True, "coalesced": False, "batch_size": 10},
    )
    monkeypatch.setattr(
        mod,
        "preparation_state",
        lambda catalog_id: {"catalog_instance_id": catalog_id, "status": "ready"},
    )

    result = mod.prepare_lumae_task("server-a", "catalog-a")

    assert result["profiles"]["queued"] is True
    assert result["preparation"]["status"] == "ready"
    assert calls == [
        ("state", "running", "catalog_refresh"),
        ("catalog", "server-a"),
        ("state", "running", "analysis_projection"),
        ("projection", "server-a"),
        ("state", "ready", "catalog_ready"),
        (
            "profiles",
            {
                "catalog_instance_id": "catalog-a",
                "server_id": "server-a",
            },
        ),
    ]


def test_prepare_lumae_records_failure_and_does_not_queue_profiles(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: settings_catalog_source())
    monkeypatch.setattr(
        mod,
        "update_preparation_state",
        lambda *args, **kwargs: calls.append((args[2], args[3])),
    )
    monkeypatch.setattr(
        mod,
        "refresh_catalog",
        lambda server_id=None: {
            "catalog_instance_id": "catalog-a",
            "builder_version": mod.CATALOG_BUILDER_VERSION,
            "refresh_required": False,
        },
    )
    monkeypatch.setattr(mod, "preparation_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        mod,
        "project_analysis",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("projection failed")),
    )
    monkeypatch.setattr(mod, "get_core_adapter", lambda: object())
    monkeypatch.setattr(
        mod,
        "start_profile_backfill",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not queue")),
    )

    with pytest.raises(RuntimeError, match="projection failed"):
        mod.prepare_lumae_task("server-a", "catalog-a")

    assert calls[-1] == ("failed", "failed")


def test_preparation_active_guard_only_blocks_short_catalog_publication():
    mod = load_plugin()
    now = mod.datetime(2026, 7, 21, 12, 0, tzinfo=mod.timezone.utc)
    state = {"status": "running", "updated_at": "2026-07-21T11:30:01+00:00"}

    assert mod.preparation_is_active(state, now=now) is True
    state["updated_at"] = "2026-07-21T10:59:59+00:00"
    assert mod.preparation_is_active(state, now=now) is False
    state["status"] = "ready"
    state["updated_at"] = "2026-07-21T11:59:59+00:00"
    assert mod.preparation_is_active(state, now=now) is False


def test_profile_backfill_active_guard_makes_stalled_queue_retryable():
    mod = load_plugin()
    now = mod.datetime(2026, 7, 21, 12, 0, tzinfo=mod.timezone.utc)
    state = {"status": "queued", "updated_at": "2026-07-21T11:30:01+00:00"}

    assert mod.profile_backfill_is_active(state, now=now) is True
    state["updated_at"] = "2026-07-21T11:29:59+00:00"
    assert mod.profile_backfill_is_active(state, now=now) is False
    state["status"] = "complete"
    state["updated_at"] = "2026-07-21T11:59:59+00:00"
    assert mod.profile_backfill_is_active(state, now=now) is False


def test_catalog_preparation_claim_does_not_inherit_legacy_profile_queue_lock():
    mod = load_plugin()
    db = FakeDb([("catalog-a",)])
    source = {"catalog_instance_id": "catalog-a", "server_id": "server-a"}

    assert mod.claim_preparation(source, db=db) is True
    sql, _params = db.cursor_obj.executed[-1]
    assert "status NOT IN ('queued', 'running')" in sql
    assert "profiles_queued" not in sql


def test_migrate_disables_legacy_backfill_schedule(monkeypatch):
    mod = load_plugin()
    db = CronDb(existing=None)
    queued = []
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda *args, **kwargs: queued.append((args, kwargs)),
    )

    mod.migrate(db)

    assert db.commits == 1
    assert queued == [((mod.provider_identity_recheck_task,), {"queue": "default"})]
    assert (
        "UPDATE cron SET enabled=FALSE WHERE task_type=%s",
        (mod.BACKFILL_TASK_TYPE,),
    ) in db.cursor_obj.executed
    cron_inserts = [params for sql, params in db.cursor_obj.executed if "INSERT INTO cron" in sql]
    assert cron_inserts == [
        (
            mod.CATALOG_REFRESH_TASK_TYPE,
            mod.CATALOG_REFRESH_TASK_TYPE,
            "17 */6 * * *",
        ),
        (
            mod.CATALOG_RECONCILE_TASK_TYPE,
            mod.CATALOG_RECONCILE_TASK_TYPE,
            "*/15 * * * *",
        ),
        (
            mod.PROVIDER_IDENTITY_RECHECK_TASK_TYPE,
            mod.PROVIDER_IDENTITY_RECHECK_TASK_TYPE,
            "2,32 * * * *",
        ),
        (
            mod.ANALYSIS_PROJECTION_TASK_TYPE,
            mod.ANALYSIS_PROJECTION_TASK_TYPE,
            "47 */6 * * *",
        ),
    ]
    migration_sql = "\n".join(sql for sql, _params in db.cursor_obj.executed)
    assert "rebind_status='active' AND provider_type='navidrome'" in migration_sql
    assert "relationship_state" in migration_sql
    enrichment_source = pathlib.Path(
        "plugins/LumaeAnalysis/catalog_enrichment.py"
    ).read_text(encoding="utf-8")
    assert "result_generation = 0" in enrichment_source
    assert "fingerprint_schema_version" in migration_sql
    assert "snapshot_estimated_bytes" in migration_sql
    assert "last_scan_change_counts" in migration_sql
    assert "last_scan_change_reason" in migration_sql
    assert "last_scan_duration_ms" in migration_sql
    assert "change_reason" in migration_sql
    assert "plugin_lumae_analysis__collections" in migration_sql
    assert "plugin_lumae_analysis__profile_backfill_state" in migration_sql
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
        "source_profiles",
        "profile_migrations",
        "preparation_state",
        "analysis_runs",
        "analysis_changes",
        "provider_identity_transitions",
        "provider_identity_manifests",
        "catalog_generation_pins",
    ):
        assert f"plugin_lumae_analysis__{table_name}" in migration_sql


def test_bounded_enqueue_uses_the_app_context_wrapper_and_finite_timeout(monkeypatch):
    mod = load_plugin()
    captured = {}

    class Queue:
        def enqueue(self, function, **kwargs):
            captured.update({"function": function, **kwargs})
            return types.SimpleNamespace(id="job-a")

    monkeypatch.setattr(
        plugin_api_module,
        "dotted_path",
        lambda function: f"{function.__module__}.{function.__name__}",
        raising=False,
    )
    monkeypatch.setattr(plugin_api_module, "rq_queue_default", Queue(), raising=False)
    monkeypatch.setattr(plugin_api_module, "rq_queue_high", Queue(), raising=False)

    job = mod.enqueue_bounded(
        mod.profile_backfill_task,
        "server-a",
        "catalog-a",
        timeout=321,
    )

    assert job.id == "job-a"
    assert captured["function"] == "plugin.manager.run_plugin_task"
    assert captured["args"][1:] == ("server-a", "catalog-a")
    assert captured["job_timeout"] == 321
    assert captured["job_timeout"] > 0


def test_v1_0_1_has_no_infinite_plugin_job_timeout():
    source = pathlib.Path("plugins/LumaeAnalysis/__init__.py").read_text(
        encoding="utf-8"
    )
    assert "job_timeout=-1" not in source


def test_maintenance_pause_blocks_background_work_but_preserves_control_state(
    monkeypatch,
):
    mod = load_plugin()
    monkeypatch.setattr(
        mod,
        "get_setting",
        lambda key, default=None: True if key == "maintenance_paused" else default,
    )
    queued = []
    released = []
    monkeypatch.setattr(mod, "enqueue", lambda *args, **kwargs: queued.append((args, kwargs)))
    monkeypatch.setattr(
        mod,
        "release_pending",
        lambda ids, **kwargs: released.append((ids, kwargs)),
    )

    assert mod.enqueue_required_catalog_preparations(db=object()) == 0
    assert mod.start_profile_backfill("catalog-a", "server-a") == {
        "queued": False,
        "coalesced": True,
        "paused": True,
        "batch_size": mod.DEFAULT_BACKFILL_BATCH_SIZE,
    }
    assert mod.start_relationship_preparation("catalog-a", "server-a") == {
        "queued": False,
        "coalesced": True,
        "paused": True,
        "reason": "maintenance_paused",
    }
    assert mod.analyze_one_track("track-a", catalog_instance_id="catalog-a") == {
        "track_id": "track-a",
        "status": "skipped_maintenance_paused",
    }
    assert mod.analyze_tracks_task(
        ["track-a", "track-b"],
        catalog_instance_id="catalog-a",
    ) == {
        "ready": 0,
        "already_ready": 0,
        "promoted": 0,
        "failed": 0,
        "skipped": 2,
        "deferred": 2,
        "paused": True,
    }
    assert mod.finalize_analysis_run_task("server-a", "catalog-a", "run-a") == {
        "status": "paused",
        "reason": "maintenance_paused",
        "run_id": "run-a",
    }
    assert released == [
        (
            ["track-a"],
            {
                "catalog_instance_id": "catalog-a",
                "reason": "Lumae background maintenance is paused",
            },
        ),
        (
            ["track-a", "track-b"],
            {
                "catalog_instance_id": "catalog-a",
                "reason": "Lumae background maintenance is paused",
            },
        ),
    ]
    assert queued == []


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


def test_builder_upgrade_queues_one_coalesced_catalogue_preparation(monkeypatch):
    mod = load_plugin()
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "rebind_status": "active",
        "catalog": {
            "generation": 3,
            "builder_version": mod.CATALOG_BUILDER_VERSION - 1,
            "refresh_required": True,
        },
    }
    claimed = []
    queued = []
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(mod, "preparation_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        mod,
        "claim_preparation",
        lambda selected, db=None: claimed.append((selected, db)) or True,
    )
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, server_id, catalog_id, queue="default": queued.append(
            (func, server_id, catalog_id, queue)
        ),
    )

    result = mod.enqueue_required_catalog_preparations(db=object())

    assert result == 1
    assert claimed[0][0] is source
    assert queued == [(mod.prepare_lumae_task, "server-a", "catalog-a", "default")]


def test_reconcile_retries_an_unattested_catalogue_even_when_generation_is_current(
    monkeypatch,
):
    mod = load_plugin()
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "rebind_status": "active",
        "catalog": {
            "generation": 4,
            "builder_version": mod.CATALOG_BUILDER_VERSION,
            "refresh_required": False,
        },
    }
    queued = []
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(
        mod,
        "preparation_state",
        lambda *_args, **_kwargs: {
            "status": "ready",
            "target_plugin_version": mod.PLUGIN_VERSION,
            "target_catalog_builder_version": mod.CATALOG_BUILDER_VERSION,
            "worker_plugin_version": "0.8.9",
            "worker_catalog_builder_version": mod.CATALOG_BUILDER_VERSION,
        },
    )
    monkeypatch.setattr(mod, "claim_preparation", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, server_id, catalog_id, queue="default": queued.append(
            (func, server_id, catalog_id, queue)
        ),
    )

    assert mod.enqueue_required_catalog_preparations(db=object()) == 1
    assert queued == [(mod.prepare_lumae_task, "server-a", "catalog-a", "default")]


def test_reconcile_watchdog_is_a_noop_for_current_attested_catalogues(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "enqueue_required_catalog_preparations", lambda: 0)

    assert mod.catalog_reconcile_task() == {
        "status": "current",
        "queued": 0,
        "plugin_version": mod.PLUGIN_VERSION,
        "catalog_builder_version": mod.CATALOG_BUILDER_VERSION,
    }


def test_worker_attestation_rejects_a_stale_audio_muse_worker(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(
        mod,
        "preparation_state",
        lambda *_args, **_kwargs: {
            "target_plugin_version": "0.8.11",
            "target_catalog_builder_version": mod.CATALOG_BUILDER_VERSION + 1,
            "worker_plugin_version": None,
            "worker_catalog_builder_version": None,
        },
    )

    with pytest.raises(RuntimeError, match="worker is still running"):
        mod.assert_preparation_worker_current("catalog-a")


def test_core_adapters_normalize_equivalent_v2_and_v3_analysis_events(monkeypatch):
    from plugins.LumaeAnalysis.core_v2 import AudioMuseV2Adapter
    from plugins.LumaeAnalysis.core_v3 import AudioMuseV3Adapter

    v2 = AudioMuseV2Adapter().normalize_analysis_hook(
        {"item_id": "provider-track", "run_id": "run-a"}
    )
    v3 = AudioMuseV3Adapter().normalize_analysis_hook(
        {"item_id": "provider-track", "server_id": "server-a", "run_id": "run-a"}
    )

    assert v2["provider_track_id"] == v3["provider_track_id"] == "provider-track"
    assert v2["server_id"] == "legacy-default"
    assert v3["server_id"] == "server-a"
    assert v2["run_id"] == v3["run_id"] == "run-a"


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


def test_provider_bridge_admits_only_navidrome_sources():
    from plugins.LumaeAnalysis.catalog_providers import (
        CatalogProviderError,
        ProviderCatalogBridge,
        SUPPORTED_PROVIDER_TYPES,
    )

    class Adapter:
        def list_servers(self):
            return [
                {
                    "server_id": "nav",
                    "provider_type": "navidrome",
                    "is_default": True,
                },
                {
                    "server_id": "jelly",
                    "provider_type": "jellyfin",
                    "is_default": False,
                },
            ]

    bridge = ProviderCatalogBridge(Adapter())

    assert SUPPORTED_PROVIDER_TYPES == frozenset({"navidrome"})
    assert [server["supported"] for server in bridge.list_servers()] == [True, False]
    assert bridge.require_server("nav")["provider_type"] == "navidrome"
    with pytest.raises(CatalogProviderError, match="not supported"):
        bridge.require_server("jelly")


def test_navidrome_catalog_uses_folder_album_queries_when_song_rows_lack_folder_ids():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.catalog_providers import _fetch_navidrome

    calls = []

    class Module:
        @staticmethod
        def list_libraries():
            return [
                {"id": "folder-a", "name": "Included"},
                {"id": "folder-b", "name": "Excluded"},
            ]

        @staticmethod
        def _get_target_music_folder_ids():
            return {"folder-a"}

        @staticmethod
        def _navidrome_request(endpoint, params=None):
            calls.append((endpoint, params))
            if endpoint == "getAlbumList2":
                assert params["musicFolderId"] == "folder-a"
                return {
                    "albumList2": {
                        "album": [{"id": "album-a", "name": "Included album"}]
                    }
                }
            if endpoint == "getAlbum":
                return {
                    "album": {
                        "id": "album-a",
                        "name": "Included album",
                        "song": [
                            {
                                "id": "track-a",
                                "title": "Included song",
                                "albumId": "album-a",
                                "album": "Included album",
                            }
                        ],
                    }
                }
            raise AssertionError(f"Unexpected Navidrome endpoint: {endpoint}")

    result = _fetch_navidrome(Module(), object(), "server-a")

    assert [row["id"] for row in result["tracks"]] == ["track-a"]
    assert [row["id"] for row in result["albums"]] == ["album-a"]
    assert result["tracks"][0]["_lumae_library_ids"] == ["folder-a"]
    assert result["albums"][0]["_lumae_library_ids"] == ["folder-a"]
    assert all(endpoint != "search3" for endpoint, _params in calls)
    normalized = normalize_provider_catalog(result, "navidrome")
    assert normalized["tracks"][0]["payload"]["_lumae"]["library_ids"] == ["folder-a"]
    assert normalized["albums"][0]["payload"]["_lumae"]["library_ids"] == ["folder-a"]
    assert [
        (row["entity_type"], row["entity_id"], row["library_id"])
        for row in normalized["entity_libraries"]
    ] == [
        ("album", "album-a", "folder-a"),
        ("track", "track-a", "folder-a"),
    ]


def test_navidrome_catalog_maps_unfiltered_music_folders_when_song_rows_omit_folder_ids():
    from plugins.LumaeAnalysis.catalog_providers import _fetch_navidrome

    class Module:
        @staticmethod
        def list_libraries():
            return [{"id": "folder-a", "name": "Music"}]

        @staticmethod
        def _get_target_music_folder_ids():
            return None

        @staticmethod
        def _navidrome_request(endpoint, params=None):
            if endpoint == "getAlbumList2":
                assert params["musicFolderId"] == "folder-a"
                return {
                    "albumList2": {
                        "album": [{"id": "album-a", "name": "Album", "songCount": 1}]
                    }
                }
            if endpoint == "getAlbum":
                return {
                    "album": {
                        "id": "album-a",
                        "name": "Album",
                        "song": [
                            {
                                "id": "track-a",
                                "title": "Hydrated title",
                                "albumId": "album-a",
                            }
                        ],
                    }
                }
            raise AssertionError(f"Unexpected Navidrome endpoint: {endpoint}")

    result = _fetch_navidrome(Module(), object(), "server-a")

    assert result["tracks"] == [
        {
            "id": "track-a",
            "title": "Hydrated title",
            "albumId": "album-a",
            "_lumae_library_ids": ["folder-a"],
        }
    ]
    assert result["albums"][0]["_lumae_library_ids"] == ["folder-a"]


def test_navidrome_catalog_joins_large_folder_scope_onto_search_rows_without_n_plus_one():
    from plugins.LumaeAnalysis.catalog_providers import _fetch_navidrome

    album_rows = [
        {"id": f"album-{index}", "name": f"Album {index}", "songCount": 1}
        for index in range(33)
    ]
    calls = []

    class Module:
        @staticmethod
        def list_libraries():
            return [{"id": "folder-a", "name": "Music"}]

        @staticmethod
        def _get_target_music_folder_ids():
            return None

        @staticmethod
        def _navidrome_request(endpoint, params=None):
            calls.append((endpoint, params))
            if endpoint == "getAlbumList2":
                return {"albumList2": {"album": album_rows}}
            if endpoint == "search3":
                return {
                    "searchResult3": {
                        "song": [
                            {
                                "id": f"track-{index}",
                                "title": f"Track {index}",
                                "albumId": f"album-{index}",
                            }
                            for index in range(33)
                        ]
                    }
                }
            raise AssertionError(f"Unexpected Navidrome endpoint: {endpoint}")

    result = _fetch_navidrome(Module(), object(), "server-a")

    assert len(result["tracks"]) == 33
    assert all(row["_lumae_library_ids"] == ["folder-a"] for row in result["tracks"])
    assert [endpoint for endpoint, _params in calls].count("getAlbumList2") == 1
    assert [endpoint for endpoint, _params in calls].count("search3") == 1
    assert all(endpoint != "getAlbum" for endpoint, _params in calls)


def test_navidrome_catalog_rejects_an_unmatched_music_folder_filter():
    from plugins.LumaeAnalysis.catalog_providers import CatalogProviderError, _fetch_navidrome

    module = types.SimpleNamespace(
        list_libraries=lambda: [{"id": "folder-a", "name": "Available"}],
        _get_target_music_folder_ids=lambda: set(),
        _navidrome_request=lambda *_args, **_kwargs: pytest.fail(
            "No catalogue request should run for an invalid folder selection"
        ),
    )

    with pytest.raises(CatalogProviderError, match="did not match any music folder"):
        _fetch_navidrome(module, object(), "server-a")


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


def test_catalogue_source_resolution_hides_persisted_non_navidrome_sources():
    from plugins.LumaeAnalysis.catalog import resolve_catalog_source

    db = FakeDb(rows=[("catalog-jelly", "server-jelly", "jellyfin")])

    with pytest.raises(KeyError, match="Unknown catalogue source"):
        resolve_catalog_source(db, server_id="server-jelly")


def _catalog_source_row(
    server_id="server-a",
    rebind_status="active",
    continuity_from=None,
    candidate_server_id=None,
):
    return (
        "catalog-a",
        server_id,
        "navidrome",
        "Main Navidrome",
        True,
        rebind_status,
        17,
        "catalog-epoch",
        100,
        0,
        "complete",
        {"track": 10},
        {},
        {},
        None,
        None,
        None,
        3,
        "analysis-epoch",
        50,
        0,
        "complete",
        10,
        10,
        None,
        None,
        continuity_from,
        candidate_server_id,
        "provider-fp",
        "scope-fp",
        {"track_count": 10},
    )


def test_catalogue_source_accepts_only_a_proven_legacy_server_alias():
    from plugins.LumaeAnalysis.catalog import resolve_catalog_source

    db = FakeDb(
        rows=[
            _catalog_source_row(
                server_id="server-a",
                rebind_status="active",
                continuity_from="legacy-default",
            )
        ]
    )

    source = resolve_catalog_source(
        db,
        server_id="legacy-default",
        catalog_instance_id="catalog-a",
    )[0]

    assert source["server_id"] == "server-a"
    assert source["catalog_instance_id"] == "catalog-a"
    assert "catalog_instance_id=%s" in db.cursor_obj.executed[0][0]
    with pytest.raises(KeyError, match="Unknown catalogue source"):
        resolve_catalog_source(
            db,
            server_id="another-server",
            catalog_instance_id="catalog-a",
        )


def test_catalogue_source_migration_does_not_create_non_navidrome_sources():
    from plugins.LumaeAnalysis.catalog import ensure_catalog_sources

    db = RebindDb([])

    class Bridge:
        def list_servers(self):
            return [
                {
                    "server_id": "server-jelly",
                    "name": "Jellyfin",
                    "provider_type": "jellyfin",
                    "is_default": True,
                }
            ]

    assert ensure_catalog_sources(db, bridge=Bridge()) == []
    assert not any(
        sql.lstrip().startswith("INSERT INTO")
        for sql, _params in db.cursor_obj.executed
    )


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


def test_profile_source_accepts_proven_legacy_alias_but_rejects_other_servers(
    monkeypatch,
):
    mod = load_plugin()
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "rebind_status": "active",
        "continuity_from": "legacy-default",
    }
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])

    assert (
        mod.resolve_profile_source(
            catalog_instance_id="catalog-a",
            server_id="legacy-default",
            db=object(),
        )
        == source
    )
    with pytest.raises(ValueError, match="music-server identity changed"):
        mod.resolve_profile_source(
            catalog_instance_id="catalog-a",
            server_id="another-server",
            db=object(),
        )


def test_stale_v2_worker_task_uses_only_the_proven_rebound_server(monkeypatch):
    mod = load_plugin()
    adapter = types.SimpleNamespace(
        mode="v3_registry",
        active_server_id=lambda: "server-a",
        list_servers=lambda: [{"server_id": "server-a"}],
    )
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "rebind_status": "active",
        "continuity_from": "legacy-default",
    }
    calls = []
    monkeypatch.setattr(mod, "get_core_adapter", lambda: adapter)
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [source])
    monkeypatch.setattr(
        mod,
        "refresh_catalog",
        lambda server_id=None: calls.append(("catalog", server_id))
        or {"status": "complete"},
    )
    monkeypatch.setattr(
        mod,
        "project_analysis",
        lambda server_id=None, adapter=None: calls.append(
            ("analysis", server_id, adapter)
        )
        or {"status": "complete"},
    )

    assert mod.catalog_refresh_task("legacy-default") == {"status": "complete"}
    assert mod.analysis_projection_task("legacy-default") == {"status": "complete"}
    assert calls == [
        ("catalog", "server-a"),
        ("analysis", "server-a", adapter),
    ]

    pending = {
        **source,
        "server_id": "legacy-default",
        "rebind_status": "rebind_required",
        "continuity_from": None,
    }
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [pending])
    assert mod.catalog_refresh_task("legacy-default") == {
        "status": "skipped",
        "reason": "source_rebind_required",
    }
    assert len(calls) == 2


def test_analysis_projection_clears_durable_reconcile_only_after_success(monkeypatch):
    mod = load_plugin()
    db = object()
    adapter = types.SimpleNamespace(
        mode="v2_single_server",
        active_server_id=lambda: "server-a",
    )
    completed = []
    monkeypatch.setattr(mod, "get_core_adapter", lambda: adapter)
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(
        mod,
        "project_analysis",
        lambda **_kwargs: {
            "catalog_instance_id": "catalog-a",
            "server_id": "server-a",
            "generation": 12,
        },
    )
    monkeypatch.setattr(
        mod,
        "complete_projection_reconcile",
        lambda selected_db, catalog_instance_id: completed.append(
            (selected_db, catalog_instance_id)
        ),
    )
    monkeypatch.setattr(
        mod,
        "start_relationship_preparation",
        lambda **_kwargs: {"queued": False, "coalesced": False},
    )

    result = mod.analysis_projection_task("server-a")

    assert result["generation"] == 12
    assert completed == [(db, "catalog-a")]


def test_flask_start_requeues_argument_compatible_identity_recheck(monkeypatch):
    mod = load_plugin()
    queued = []
    migrations = []

    class Db:
        def __init__(self):
            self.commits = 0

        def commit(self):
            self.commits += 1

    db = Db()

    class Bridge:
        @staticmethod
        def list_servers():
            return []

    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(
        mod,
        "migrate_provider_identity",
        lambda selected_db: migrations.append(selected_db),
    )
    monkeypatch.setattr(mod, "ProviderCatalogBridge", Bridge)
    monkeypatch.setattr(
        mod,
        "enqueue_bounded",
        lambda func, *args, **kwargs: queued.append((func, args, kwargs)),
    )

    mod.observe_provider_identities_on_start()

    assert migrations == [db]
    assert db.commits == 1
    assert queued == [
        (
            mod.provider_identity_recheck_task,
            (),
            {"queue": "default", "timeout": mod.PROJECTION_JOB_TIMEOUT_SECONDS},
        )
    ]


def test_flask_start_rolls_back_a_failed_identity_schema_repair(monkeypatch):
    mod = load_plugin()

    class Db:
        def __init__(self):
            self.rollbacks = 0

        def rollback(self):
            self.rollbacks += 1

    db = Db()
    queued = []
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(
        mod,
        "migrate_provider_identity",
        lambda _db: (_ for _ in ()).throw(RuntimeError("schema migration failed")),
    )
    monkeypatch.setattr(
        mod,
        "ProviderCatalogBridge",
        lambda: (_ for _ in ()).throw(AssertionError("must not probe an invalid schema")),
    )
    monkeypatch.setattr(
        mod,
        "enqueue_bounded",
        lambda *args, **kwargs: queued.append((args, kwargs)),
    )

    mod.observe_provider_identities_on_start()

    assert db.rollbacks == 1
    assert queued == []


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


def test_provider_catalog_normalizes_structured_navidrome_artist_identities():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    normalized = normalize_provider_catalog(
        {
            "albums": [
                {
                    "id": "album-1",
                    "name": "Lux",
                    "AlbumArtist": {
                        "id": "7na6296tJwTG4kzEPL94VM",
                        "name": "ROSALÍA",
                    },
                }
            ],
            "tracks": [
                {
                    "id": "track-1",
                    "title": "Berghain",
                    "albumId": "album-1",
                    "album": "Lux",
                    "artist": {
                        "id": "7na6296tJwTG4kzEPL94VM",
                        "name": "Rosalía",
                    },
                    "albumArtist": [
                        {
                            "id": "7na6296tJwTG4kzEPL94VM",
                            "name": "ROSALÍA",
                        }
                    ],
                }
            ],
        },
        "navidrome",
    )

    assert normalized["albums"][0]["album_artist_display"] == "ROSALÍA"
    assert normalized["tracks"][0]["artist_display"] == "Rosalía"
    assert normalized["tracks"][0]["album_artist_display"] == "ROSALÍA"
    assert len(normalized["artists"]) == 1
    assert normalized["artists"][0]["artist_id"] == "7na6296tJwTG4kzEPL94VM"
    assert normalized["artists"][0]["name"] == "ROSALÍA"
    assert normalized["artists"][0]["identity_provenance"] == "provider_id"
    assert "{'id':" not in json.dumps(normalized, ensure_ascii=False)


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
            self.rows = [
                (
                    self.db.previous_generation,
                    self.db.epoch,
                    self.db.head_seq,
                    self.db.previous_counts,
                    self.db.fingerprint_schema_version,
                )
            ]
        elif "published_generation, catalog_epoch, catalog_head_seq" in sql:
            self.rows = [
                (
                    self.db.previous_generation,
                    self.db.epoch,
                    self.db.head_seq,
                    self.db.fingerprint_schema_version,
                )
            ]
        elif sql.lstrip().startswith("SELECT DISTINCT"):
            self.rows = next(
                (
                    [(entity_id,) for entity_id in entity_ids]
                    for table_name, entity_ids in self.db.historical_entity_ids.items()
                    if table_name in sql
                ),
                [],
            )
        elif sql.lstrip().startswith("SELECT") and "available=TRUE" in sql:
            self.rows = next(
                (
                    rows
                    for table_name, rows in self.db.published_fingerprints.items()
                    if table_name in sql
                ),
                [],
            )
        else:
            self.rows = []

    def executemany(self, sql, params):
        materialized = list(params)
        self.db.executed.append((sql, materialized))


class RefreshDb:
    def __init__(
        self,
        previous_counts=None,
        previous_generation=0,
        epoch="epoch-a",
        head_seq=0,
        fingerprint_schema_version=2,
        published_fingerprints=None,
        historical_entity_ids=None,
    ):
        self.previous_counts = previous_counts or {}
        self.previous_generation = previous_generation
        self.epoch = epoch
        self.head_seq = head_seq
        self.fingerprint_schema_version = fingerprint_schema_version
        self.published_fingerprints = published_fingerprints or {}
        self.historical_entity_ids = historical_entity_ids or {}
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return RefreshCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_catalog_generation_parameters_are_materialized_one_batch_at_a_time():
    import plugins.LumaeAnalysis.catalog as catalog

    class Cursor:
        def __init__(self):
            self.batch_sizes = []

        def executemany(self, _sql, params):
            self.batch_sizes.append(len(params))

    rows = (
        {
            "track_id": f"track-{index}",
            "title": "Track",
            "payload": {},
        }
        for index in range(2501)
    )
    cursor = Cursor()

    catalog._insert_generation_rows(
        cursor,
        "track",
        "catalog-a",
        2,
        rows,
        catalog.utc_now(),
    )

    assert cursor.batch_sizes == [1000, 1000, 501]


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

    db = RefreshDb(previous_counts={"track": 3}, previous_generation=7)
    bridge = RefreshBridge(error=RuntimeError("provider unavailable"))

    with pytest.raises(RuntimeError, match="provider unavailable"):
        refresh_catalog("server-a", db=db, bridge=bridge)

    assert db.rollbacks == 1
    assert db.commits == 2
    assert not any("SET published_generation" in sql for sql, _params in db.executed)
    assert any(
        "CASE WHEN published_generation=0 THEN 'failed' ELSE status END" in sql
        and params[0] == "provider unavailable"
        for sql, params in db.executed
        if params
    )


def test_no_change_refresh_keeps_generation_and_emits_no_catalogue_writes():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog, refresh_catalog

    payload = {"tracks": [{"id": "track-1", "title": "Song", "duration": 123}]}
    normalized = normalize_provider_catalog(payload, "navidrome")
    track = normalized["tracks"][0]
    db = RefreshDb(
        previous_counts={"track": 1},
        previous_generation=7,
        epoch="epoch-a",
        head_seq=75_098,
        published_fingerprints={
            "catalog_tracks": [
                (
                    track["track_id"],
                    track["metadata_fp"],
                    track["media_fp"],
                    track["artwork_fp"],
                )
            ]
        },
    )

    result = refresh_catalog("server-a", db=db, bridge=RefreshBridge(payload))

    assert result["generation"] == 7
    assert result["cursor"] == {"epoch": "epoch-a", "seq": 75_098}
    assert result["changes"] == 0
    assert result["change_reason"] == "no_change"
    assert not any(
        sql.lstrip().startswith(("INSERT INTO", "DELETE FROM"))
        and any(
            table_name in sql
            for table_name in (
                "catalog_tracks",
                "catalog_albums",
                "catalog_artists",
                "catalog_libraries",
                "catalog_changes",
            )
        )
        for sql, _params in db.executed
    )


@pytest.mark.parametrize(
    "changed_payload",
    [
        {"id": "track-1", "title": "Renamed", "duration": 123},
        {"id": "track-1", "title": "Song", "duration": 124},
        {"id": "track-1", "title": "Song", "duration": 123, "coverArt": "cover-b"},
    ],
)
def test_one_track_fingerprint_change_emits_one_scoped_event(changed_payload):
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog, refresh_catalog

    original = normalize_provider_catalog(
        {"tracks": [{"id": "track-1", "title": "Song", "duration": 123}]},
        "navidrome",
    )["tracks"][0]
    db = RefreshDb(
        previous_counts={"track": 1},
        previous_generation=7,
        published_fingerprints={
            "catalog_tracks": [
                (
                    original["track_id"],
                    original["metadata_fp"],
                    original["media_fp"],
                    original["artwork_fp"],
                )
            ]
        },
    )

    result = refresh_catalog(
        "server-a",
        db=db,
        bridge=RefreshBridge({"tracks": [changed_payload]}),
    )

    change_inserts = [
        params
        for sql, params in db.executed
        if sql.lstrip().startswith("INSERT INTO") and "catalog_changes" in sql
    ]
    assert result["changes"] == 1
    assert result["change_counts"]["by_entity"]["track"]["total"] == 1
    assert len(change_inserts) == 1
    assert change_inserts[0][4:8] == ("track", "track-1", "upsert", "provider_diff")


def test_refresh_records_exact_deletion_and_reactivation_counts():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog, refresh_catalog

    original_rows = normalize_provider_catalog(
        {
            "tracks": [
                {"id": "track-1", "title": "One"},
                {"id": "track-2", "title": "Two"},
            ]
        },
        "navidrome",
    )["tracks"]
    published = {
        "catalog_tracks": [
            (
                track["track_id"],
                track["metadata_fp"],
                track["media_fp"],
                track["artwork_fp"],
            )
            for track in original_rows
        ]
    }
    deletion_db = RefreshDb(
        previous_counts={"track": 2},
        previous_generation=7,
        published_fingerprints=published,
    )

    deleted = refresh_catalog(
        "server-a",
        db=deletion_db,
        bridge=RefreshBridge({"tracks": [{"id": "track-1", "title": "One"}]}),
    )

    assert deleted["change_counts"]["deletions"] == 1
    assert deleted["change_counts"]["reactivations"] == 0
    assert deleted["change_counts"]["by_entity"]["track"]["deletions"] == 1

    reactivation_db = RefreshDb(
        previous_counts={},
        previous_generation=8,
        historical_entity_ids={"catalog_tracks": ["track-2"]},
    )
    reactivated = refresh_catalog(
        "server-a",
        db=reactivation_db,
        bridge=RefreshBridge({"tracks": [{"id": "track-2", "title": "Two"}]}),
    )

    assert reactivated["change_counts"]["upserts"] == 1
    assert reactivated["change_counts"]["reactivations"] == 1
    assert reactivated["change_counts"]["by_entity"]["track"]["reactivations"] == 1


def test_fingerprint_schema_rebase_rotates_epoch_without_ordinary_change_events():
    from plugins.LumaeAnalysis.catalog import refresh_catalog

    db = RefreshDb(
        previous_counts={"track": 1},
        previous_generation=7,
        epoch="old-epoch",
        head_seq=75_098,
        fingerprint_schema_version=1,
    )
    bridge = RefreshBridge(
        {
            "tracks": [
                {
                    "id": "track-1",
                    "title": "Song",
                    "duration": 123,
                }
            ]
        }
    )

    result = refresh_catalog("server-a", db=db, bridge=bridge)

    assert result["generation"] == 8
    assert result["change_reason"] == "fingerprint_schema_rebase"
    assert result["changes"] == 0
    assert result["cursor"]["seq"] == 0
    assert result["cursor"]["epoch"] != "old-epoch"
    assert not any(
        sql.lstrip().startswith("INSERT INTO") and "catalog_changes" in sql
        for sql, _params in db.executed
    )
    assert any(
        sql.lstrip().startswith("DELETE FROM") and "catalog_changes" in sql
        for sql, _params in db.executed
    )
    assert any(
        "stream_bootstrap_sessions" in sql and "completed_at=now()" in sql
        for sql, _params in db.executed
    )


def test_interrupted_fingerprint_rebase_keeps_previous_generation_and_epoch(monkeypatch):
    import plugins.LumaeAnalysis.catalog as catalog

    db = RefreshDb(
        previous_counts={"track": 1},
        previous_generation=7,
        epoch="old-epoch",
        head_seq=75_098,
        fingerprint_schema_version=1,
    )
    bridge = RefreshBridge({"tracks": [{"id": "track-1", "title": "Song"}]})
    monkeypatch.setattr(
        catalog,
        "_insert_generation_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publication interrupted")),
    )

    with pytest.raises(RuntimeError, match="publication interrupted"):
        catalog.refresh_catalog("server-a", db=db, bridge=bridge)

    assert db.rollbacks == 1
    assert not any("SET published_generation=" in sql for sql, _params in db.executed)
    assert not any("SET catalog_epoch=" in sql for sql, _params in db.executed)
    assert any(
        "CASE WHEN published_generation=0 THEN 'failed' ELSE status END" in sql
        and params[0] == "publication interrupted"
        for sql, params in db.executed
        if params
    )


def test_refresh_catalog_rejects_an_empty_first_generation():
    from plugins.LumaeAnalysis.catalog import CatalogScanError, refresh_catalog

    db = RefreshDb()

    with pytest.raises(CatalogScanError, match="empty catalogue was not published"):
        refresh_catalog("server-a", db=db, bridge=RefreshBridge({"tracks": []}))

    assert db.rollbacks == 1
    assert not any("SET published_generation" in sql for sql, _params in db.executed)
    assert any(
        "status='failed'" in sql
        and "Navidrome returned no usable tracks" in params[0]
        for sql, params in db.executed
        if params
    )


def test_refresh_catalog_rejects_tracks_without_library_membership():
    from plugins.LumaeAnalysis.catalog import CatalogScanError, refresh_catalog

    db = RefreshDb()
    payload = {
        "libraries": [{"id": "library-1", "name": "Music"}],
        "tracks": [{"id": "track-1", "title": "Song"}],
    }

    with pytest.raises(CatalogScanError, match="0 of 1 tracks had valid"):
        refresh_catalog("server-a", db=db, bridge=RefreshBridge(payload))

    assert db.rollbacks == 1
    assert not any("SET published_generation" in sql for sql, _params in db.executed)


def test_refresh_catalog_rejects_partial_or_unknown_library_memberships():
    from plugins.LumaeAnalysis.catalog import CatalogScanError, refresh_catalog

    db = RefreshDb()
    payload = {
        "libraries": [{"id": "library-1", "name": "Music"}],
        "tracks": [
            {
                "id": "track-1",
                "title": "Mapped",
                "_lumae_library_ids": ["library-1"],
            },
            {
                "id": "track-2",
                "title": "Wrong library",
                "_lumae_library_ids": ["library-2"],
            },
            {"id": "track-3", "title": "Unmapped"},
        ],
    }

    with pytest.raises(
        CatalogScanError,
        match="1 of 3 tracks had valid.*1 unknown library IDs",
    ):
        refresh_catalog("server-a", db=db, bridge=RefreshBridge(payload))

    assert db.rollbacks == 1
    assert not any("SET published_generation" in sql for sql, _params in db.executed)


def test_refresh_failure_preserves_published_readiness():
    from plugins.LumaeAnalysis.catalog import refresh_catalog

    db = RefreshDb(previous_counts={"track": 3})

    with pytest.raises(RuntimeError, match="provider unavailable"):
        refresh_catalog(
            "server-a",
            db=db,
            bridge=RefreshBridge(error=RuntimeError("provider unavailable")),
        )

    failure_sql = next(
        sql
        for sql, params in db.executed
        if params and params[0] == "provider unavailable" and "catalog_state" in sql
    )
    assert "CASE WHEN published_generation=0 THEN 'failed' ELSE status END" in failure_sql


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
        elif "FROM chromaprint" in sql:
            self.rows = [
                ("copy-a", b"same-fingerprint"),
                ("copy-b", b"same-fingerprint"),
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
    mode = "v3_registry"

    def active_server_id(self):
        return "server-a"

    def analysis_mapping_sql(self):
        return "SELECT provider_track_id, analysis_id, match_tier FROM fake_mapping WHERE server_id=%s"


def test_analysis_projection_reuses_one_vector_for_two_provider_occurrences(monkeypatch):
    from plugins.LumaeAnalysis.catalog_analysis import project_analysis

    monkeypatch.setattr(
        plugin_api_module.config, "CATALOGUE_ID_SCHEME_VERSION", 4, raising=False
    )
    monkeypatch.setattr(
        plugin_api_module.config, "CHROMAPRINT_COLLECTION_ENABLED", True, raising=False
    )
    monkeypatch.setattr(
        plugin_api_module.config, "CHROMAPRINT_GATE_ENABLED", True, raising=False
    )
    db = ProjectionDb()

    result = project_analysis("server-a", db=db, adapter=ProjectionAdapter())

    assert result["item_count"] == 1
    assert result["link_count"] == 2
    assert result["ready_count"] == 2
    assert result["evidence_complete_count"] == 2
    assert result["suspect_count"] == 0
    assert db.commits == 1
    assert sum("INSERT INTO plugin_lumae_analysis__analysis_items" in sql for sql, _ in db.executed) == 1
    assert sum("INSERT INTO plugin_lumae_analysis__track_analysis_links" in sql for sql, _ in db.executed) == 2


@pytest.mark.parametrize(
    ("analysis_status", "expect_unchanged"),
    (("complete", True), ("failed", False)),
)
def test_no_change_analysis_projection_reuses_only_a_complete_generation(
    monkeypatch,
    analysis_status,
    expect_unchanged,
):
    import plugins.LumaeAnalysis.catalog_analysis as projection
    from plugins.LumaeAnalysis.catalog import fingerprint

    item = {
        "analysis_id": "analysis-a",
        "scalar_payload": {"tempo": 120},
        "scalar_fp": "scalar-fp",
        "umap": None,
        "umap_fp": None,
        "musicnn_vector": struct.pack("<2f", 0.1, 0.2),
        "musicnn_fp": "musicnn-fp",
        "clap_vector": None,
        "clap_fp": None,
    }
    link = {
        "provider_track_id": "track-a",
        "analysis_id": "analysis-a",
        "status": "ready",
        "match_tier": "direct",
        "algorithm": "bounded-test",
        "decision_threshold": 0.1,
        "distance": None,
        "evidence_complete": False,
        "conflict_flags": [],
        "review_state": None,
    }

    class Cursor:
        def __init__(self):
            self.row = None
            self.executed = []

        def execute(self, sql, params=None):
            self.executed.append((sql, params))
            self.row = (
                (7, "analysis-epoch", 42)
                if "FROM plugin_lumae_analysis__analysis_state" in sql
                else None
            )

        def fetchone(self):
            return self.row

        def close(self):
            pass

    class Db:
        def __init__(self):
            self.cursor_obj = Cursor()
            self.commits = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commits += 1

    db = Db()
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "catalog": {"status": "complete", "generation": 3},
        "analysis": {"status": analysis_status, "generation": 7},
    }
    monkeypatch.setattr(projection, "resolve_catalog_source", lambda *_a, **_k: [source])
    monkeypatch.setattr(
        projection,
        "_active_catalog_tracks",
        lambda *_a: {
            "track-a": {
                "track_id": "track-a",
                "title": "Track",
                "artist": "Artist",
                "album_id": "album-a",
                "duration_ms": 180000,
                "payload": {},
            }
        },
    )
    monkeypatch.setattr(
        projection,
        "_analysis_mapping",
        lambda *_a: {
            "track-a": {
                "analysis_id": "analysis-a",
                "match_tier": "direct",
            }
        },
    )
    monkeypatch.setattr(projection, "_analysis_chromaprints", lambda *_a: {})
    monkeypatch.setattr(projection, "_analysis_rows", lambda *_a: {"analysis-a": item})
    monkeypatch.setattr(
        projection,
        "dedup_policy",
        lambda: {"algorithm": "bounded-test", "configured_threshold": 0.1},
    )
    monkeypatch.setattr(projection, "_apply_progressive_evidence", lambda *_a: None)
    monkeypatch.setattr(projection, "_apply_provider_conflicts", lambda *_a: None)
    monkeypatch.setattr(projection, "_suspect_analysis_ids", lambda *_a: set())
    monkeypatch.setattr(
        projection,
        "_old_items",
        lambda *_a: {
            "analysis-a": (
                item["scalar_fp"],
                item["umap_fp"],
                item["musicnn_fp"],
                item["clap_fp"],
            )
        },
    )
    monkeypatch.setattr(
        projection,
        "_old_links",
        lambda *_a: {"track-a": fingerprint(link)},
    )

    result = projection.project_analysis(
        "server-a",
        db=db,
        adapter=types.SimpleNamespace(active_server_id=lambda: "server-a"),
    )

    assert result["generation"] == (7 if expect_unchanged else 8)
    assert result["changes"] == 0
    assert db.commits == 1
    writes = [
        sql
        for sql, _params in db.cursor_obj.executed
        if sql.lstrip().startswith(("INSERT", "UPDATE"))
    ]
    if expect_unchanged:
        assert result["unchanged"] is True
        assert writes == []
    else:
        assert "unchanged" not in result
        assert len(writes) == 3
        assert any("analysis_items" in sql for sql in writes)
        assert any("track_analysis_links" in sql for sql in writes)
        assert any("status='complete'" in sql for sql in writes)


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


def test_progressive_evidence_keeps_disagreements_usable_and_flagged_for_repair():
    from plugins.LumaeAnalysis.catalog_analysis import _apply_progressive_evidence

    policy = {"per_link_chromaprint_evidence_available": True}
    links = {
        "single": {
            "analysis_id": "single-analysis",
            "status": "ready",
            "evidence_complete": False,
            "conflict_flags": [],
            "review_state": None,
        },
        "pending-a": {
            "analysis_id": "pending-analysis",
            "status": "ready",
            "evidence_complete": False,
            "conflict_flags": [],
            "review_state": None,
        },
        "pending-b": {
            "analysis_id": "pending-analysis",
            "status": "ready",
            "evidence_complete": False,
            "conflict_flags": [],
            "review_state": None,
        },
        "suspect-a": {
            "analysis_id": "suspect-analysis",
            "status": "ready",
            "evidence_complete": False,
            "conflict_flags": [],
            "review_state": None,
        },
        "suspect-b": {
            "analysis_id": "suspect-analysis",
            "status": "ready",
            "evidence_complete": False,
            "conflict_flags": [],
            "review_state": None,
        },
    }
    fingerprints = {
        "pending-a": b"pending-a",
        "suspect-a": b"suspect-a",
        "suspect-b": b"suspect-b",
    }

    _apply_progressive_evidence(
        links,
        fingerprints,
        policy,
        compare=lambda left, right: False,
    )

    assert links["single"]["status"] == "ready"
    assert links["single"]["evidence_complete"] is True
    assert links["pending-a"]["status"] == "ready"
    assert links["pending-b"]["status"] == "ready"
    assert links["pending-a"]["evidence_complete"] is False
    assert links["pending-a"]["conflict_flags"] == ["chromaprint_evidence_pending"]
    assert links["pending-a"]["review_state"] == "provisional"
    assert links["suspect-a"]["status"] == "ready"
    assert links["suspect-b"]["status"] == "ready"
    assert links["suspect-a"]["evidence_complete"] is False
    assert links["suspect-a"]["conflict_flags"] == ["chromaprint_disagreement"]
    assert links["suspect-a"]["review_state"] == "needs_repair"


def test_provider_conflicts_keep_sonic_data_usable_and_preserve_stronger_repair_flag():
    from plugins.LumaeAnalysis.catalog_analysis import _apply_provider_conflicts

    links = {
        "chromaprint-conflict": {
            "analysis_id": "analysis-a",
            "status": "ready",
            "evidence_complete": False,
            "conflict_flags": ["chromaprint_disagreement"],
            "review_state": "needs_repair",
        },
        "provider-conflict": {
            "analysis_id": "analysis-b",
            "status": "ready",
            "evidence_complete": True,
            "conflict_flags": [],
            "review_state": None,
        },
    }

    _apply_provider_conflicts(links, {"analysis-a", "analysis-b"})

    assert {link["status"] for link in links.values()} == {"ready"}
    assert links["chromaprint-conflict"]["conflict_flags"] == [
        "chromaprint_disagreement",
        "provider_evidence_conflict",
    ]
    assert links["chromaprint-conflict"]["review_state"] == "needs_repair"
    assert links["provider-conflict"]["evidence_complete"] is False
    assert links["provider-conflict"]["conflict_flags"] == [
        "provider_evidence_conflict"
    ]
    assert links["provider-conflict"]["review_state"] == "needs_review"


def test_old_link_fingerprint_uses_the_same_fields_as_new_projection_payload():
    from plugins.LumaeAnalysis.catalog import fingerprint
    from plugins.LumaeAnalysis.catalog_analysis import _old_links

    link = {
        "provider_track_id": "track-a",
        "analysis_id": "analysis-a",
        "status": "ready",
        "match_tier": "provider_occurrence",
        "algorithm": "audiomuse_catalogue_fp_4",
        "decision_threshold": 0.01,
        "distance": None,
        "evidence_complete": False,
        "conflict_flags": ["provider_evidence_conflict"],
        "review_state": "needs_review",
    }

    class Cursor:
        def __init__(self):
            self.sql = ""

        def execute(self, sql, params):
            self.sql = " ".join(sql.split())
            assert params == ("catalog-a", 4)

        def fetchall(self):
            return [
                (
                    link["provider_track_id"],
                    link["analysis_id"],
                    link["status"],
                    link["match_tier"],
                    link["algorithm"],
                    link["decision_threshold"],
                    link["distance"],
                    link["evidence_complete"],
                    link["conflict_flags"],
                    link["review_state"],
                )
            ]

    cur = Cursor()
    old = _old_links(cur, "catalog-a", 4)

    assert "review_state" in cur.sql
    assert old == {"track-a": fingerprint(link)}


def test_progressive_evidence_uses_inconclusive_fingerprints_provisionally():
    from plugins.LumaeAnalysis.catalog_analysis import _apply_progressive_evidence

    links = {
        track_id: {
            "analysis_id": "analysis-a",
            "status": "ready",
            "evidence_complete": False,
            "conflict_flags": [],
            "review_state": None,
        }
        for track_id in ("track-a", "track-b")
    }

    _apply_progressive_evidence(
        links,
        {"track-a": b"a", "track-b": b"b"},
        {"per_link_chromaprint_evidence_available": True},
        compare=lambda _left, _right: None,
    )

    assert {link["status"] for link in links.values()} == {"ready"}
    assert {
        tuple(link["conflict_flags"]) for link in links.values()
    } == {("chromaprint_evidence_inconclusive",)}
    assert {link["review_state"] for link in links.values()} == {"provisional"}


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
        "per_link_chromaprint_evidence_available": True,
        "evidence_status": "per_link_progressive",
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
        elif "FROM plugin_lumae_analysis__track_analysis_links" in sql:
            self.rows = [self.db.link_counts]
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
    def __init__(
        self,
        coverage=(10, 9, 9, 150.0),
        tasks=None,
        link_counts=(8, 1, 0, 1, 7, 1),
    ):
        self.coverage = coverage
        self.tasks = tasks or []
        self.link_counts = link_counts
        self.executed = []

    def cursor(self):
        return ReadinessCursor(self)


def readiness_source():
    return {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "provider_type": "navidrome",
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
        "per_link_chromaprint_evidence_available": True,
    }


def readiness_tasks():
    return [
        ("analysis-after", "main_analysis", "SUCCESS", 300.0, {"failed_servers": []}, None),
        ("cleaning", "cleaning", "SUCCESS", 200.0, {}, None),
        ("analysis-before", "main_analysis", "SUCCESS", 100.0, {"failed_servers": []}, None),
    ]


def test_v3_readiness_blocks_pending_source_rebind_before_database_queries():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.5",
        adapter="v3_registry",
    )
    source = {
        **readiness_source(),
        "server_id": "legacy-default",
        "candidate_server_id": "server-a",
        "rebind_status": "rebind_required",
    }

    result = readiness.v3_release_readiness(
        None,
        compatibility,
        source,
        readiness_policy(),
    )

    assert result["status"] == "source_rebind_required"
    assert result["ready"] is False
    assert result["blockers"] == ["source_rebind_required"]
    assert result["admission"]["catalog"]["admitted"] is False
    assert result["admission"]["analysis"]["admitted"] is False
    assert result["admission"]["analysis"]["semantic_contracts"]


def test_v3_readiness_is_derived_automatically_from_complete_evidence():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.3",
        adapter="v3_registry",
    )
    result = readiness.v3_release_readiness(
        ReadinessDb(
            coverage=(10, 10, 10, 150.0),
            tasks=[],
            link_counts=(10, 0, 0, 0, 10, 0),
        ),
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert result["status"] == "ready"
    assert result["ready"] is True
    assert result["fully_verified"] is True
    assert result["analysis_sync_allowed"] is True
    assert result["progressive_analysis"] is False
    assert result["verification_mode"] == "automatic"
    assert result["administrator_acknowledged"] is False
    assert result["acknowledged_at"] is None
    assert result["blockers"] == []
    assert result["admission"]["catalog"]["admitted"] is True
    assert result["admission"]["analysis"]["admitted"] is True


def test_v3_readiness_keeps_incomplete_evidence_progressively_usable():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.3",
        adapter="v3_registry",
    )
    db = ReadinessDb(coverage=(10, 9, 8, 150.0), tasks=[])

    result = readiness.v3_release_readiness(
        db,
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert result["status"] == "progressive"
    assert result["ready"] is False
    assert result["fully_verified"] is False
    assert result["analysis_sync_allowed"] is True
    assert result["progressive_analysis"] is True
    assert result["verification_mode"] == "automatic"
    assert result["admission"]["catalog"]["admitted"] is True
    assert result["admission"]["analysis"]["admitted"] is True
    assert result["admission"]["analysis"]["status"] == "progressive"
    assert result["admission"]["analysis"]["blockers"] == []
    assert result["ready_link_count"] == 8
    assert result["pending_link_count"] == 1
    assert result["missing_link_count"] == 1
    assert result["verified_link_count"] == 7
    assert result["provisional_link_count"] == 1
    assert result["usable_analysis_coverage"] == 0.8
    assert result["blockers"] == [
        "analysis_mapping_incomplete",
        "chromaprint_backfill_incomplete",
        "analysis_links_pending",
        "analysis_links_missing",
        "provisional_links_remaining",
    ]
    link_query = next(
        sql
        for sql, _params in db.executed
        if "track_analysis_links" in sql
    )
    assert "review_state IN ('needs_repair', 'needs_review')" in link_query


def test_v3_historical_upgrade_sequence_is_diagnostic_only():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.3",
        adapter="v3_registry",
    )
    result = readiness.v3_release_readiness(
        ReadinessDb(
            coverage=(10, 10, 10, 250.0),
            tasks=readiness_tasks(),
            link_counts=(10, 0, 0, 0, 10, 0),
        ),
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert result["ready"] is True
    assert result["task_evidence"]["chromaprint_complete_before_cleaning"] is False
    assert result["task_evidence"]["upgrade_sequence_complete"] is False


def test_v3_readiness_does_not_depend_on_historical_task_diagnostics():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.5",
        adapter="v3_registry",
    )
    db = ReadinessDb(
        coverage=(10, 10, 10, 150.0),
        link_counts=(10, 0, 0, 0, 10, 0),
    )

    class MissingTaskHistoryCursor(ReadinessCursor):
        def execute(self, sql, params=None):
            if "FROM task_status" in sql:
                raise RuntimeError("task history unavailable")
            super().execute(sql, params)

    db.cursor = lambda: MissingTaskHistoryCursor(db)

    result = readiness.v3_release_readiness(
        db,
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert result["ready"] is True
    assert result["verification_mode"] == "automatic"
    assert result["task_evidence"]["diagnostics_available"] is False


@pytest.mark.parametrize(
    "core_version",
    ["v3.0.3", "v3.0.5", "v3.0.6", "v4.0.0"],
)
def test_v3_complete_evidence_is_automatic_across_core_releases(core_version):
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version=core_version,
        adapter="v3_registry",
    )
    result = readiness.v3_release_readiness(
        ReadinessDb(
            coverage=(10, 10, 10, 150.0),
            tasks=[],
            link_counts=(10, 0, 0, 0, 10, 0),
        ),
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert result["ready"] is True
    assert result["verification_mode"] == "automatic"
    assert result["qualified_core_version"] == core_version
    assert result["admission"]["analysis"]["admitted"] is True


def test_v3_readiness_ignores_obsolete_acknowledgement_settings(monkeypatch):
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.5",
        adapter="v3_registry",
    )
    monkeypatch.setattr(
        plugin_api_module,
        "get_setting",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("readiness must not load manual acknowledgements")
        ),
    )

    result = readiness.v3_release_readiness(
        ReadinessDb(
            coverage=(10, 10, 10, 150.0),
            tasks=[],
            link_counts=(10, 0, 0, 0, 10, 0),
        ),
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert result["ready"] is True
    assert result["administrator_acknowledged"] is False
    assert result["qualified_core_version"] == "v3.0.5"
    assert result["verification_mode"] == "automatic"


def test_v3_readiness_admits_future_patch_from_complete_runtime_evidence():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    compatibility = types.SimpleNamespace(
        core_version="v3.0.6",
        adapter="v3_registry",
    )

    result = readiness.v3_release_readiness(
        ReadinessDb(
            coverage=(10, 10, 10, 150.0),
            tasks=[],
            link_counts=(10, 0, 0, 0, 10, 0),
        ),
        compatibility,
        readiness_source(),
        readiness_policy(),
    )

    assert result["qualified_core_version"] == "v3.0.6"
    assert result["detected_core_version"] == "v3.0.6"
    assert result["status"] == "ready"
    assert result["ready"] is True
    assert result["blockers"] == []
    assert result["admission"]["catalog"]["admitted"] is True
    assert result["admission"]["analysis"]["admitted"] is True


def test_v3_readiness_blocks_analysis_but_keeps_catalogue_when_safety_contract_fails():
    readiness = importlib.import_module("plugins.LumaeAnalysis.catalog_readiness")
    policy = {**readiness_policy(), "chromaprint_gate_enabled": False}

    result = readiness.v3_release_readiness(
        ReadinessDb(
            coverage=(10, 10, 10, 150.0),
            tasks=[],
            link_counts=(10, 0, 0, 0, 10, 0),
        ),
        types.SimpleNamespace(core_version="v3.99.0", adapter="v3_registry"),
        readiness_source(),
        policy,
    )

    assert result["admission"]["catalog"]["admitted"] is True
    assert result["admission"]["analysis"]["admitted"] is False
    assert result["admission"]["analysis"]["blockers"] == [
        "chromaprint_gate_disabled"
    ]


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


def test_settings_treats_stale_manual_verification_posts_as_noop(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(
        mod,
        "render_settings",
        lambda message=None, error=None: message or error or "settings",
    )
    client = plugin_client(mod)

    acknowledge = client.post(
        "/settings",
        data={
            "action": "ack_v3_readiness",
            "server_id": "server-a",
            "catalog_instance_id": "catalog-a",
            "verification_mode": "upgraded",
        },
    )
    clear = client.post(
        "/settings",
        data={
            "action": "clear_v3_readiness",
            "catalog_instance_id": "catalog-a",
        },
    )

    assert acknowledge.status_code == 200
    assert clear.status_code == 200
    assert "no longer required" in acknowledge.get_data(as_text=True)
    assert "verifies sonic readiness automatically" in clear.get_data(as_text=True)


def test_settings_page_explains_automatic_sonic_status_and_blockers(monkeypatch):
    mod = load_plugin()
    source = readiness_source()
    readiness = {
        "detected_core_version": "v3.0.5",
        "status": "repair_incomplete",
        "ready": False,
        "administrator_acknowledged": False,
        "eligible_track_count": 12,
        "mapped_track_count": 10,
        "missing_mapping_count": 2,
        "chromaprint_track_count": 8,
        "chromaprint_coverage": 0.8,
        "ready_link_count": 7,
        "verified_link_count": 5,
        "provisional_link_count": 2,
        "pending_link_count": 2,
        "suspect_link_count": 1,
        "missing_link_count": 2,
        "analysis_sync_allowed": True,
        "task_evidence": {"upgrade_sequence_complete": False},
        "blockers": [
            "analysis_mapping_incomplete",
            "chromaprint_backfill_incomplete",
            "analysis_links_pending",
            "analysis_links_need_repair",
            "analysis_links_missing",
            "provisional_links_remaining",
        ],
    }
    monkeypatch.setattr(mod, "_v3_readiness_sources", lambda: [(source, readiness)])

    body = mod.render_v3_readiness_panel()
    compact = " ".join(body.split())

    assert "2. AudioMuse source analysis" in body
    assert "AudioMuse source analysis is still filling in" in body
    assert "Chromaprint: 8 of 10 mapped tracks (80.00%)" in compact
    assert "without analysis mapping: 2" in compact
    assert "Full-library verification is still waiting for Chromaprint" in body
    assert "Source-analysis links: 7 usable (5 verified; 2 provisional)" in compact
    assert "1 flagged for repair" in compact
    assert "Analysis task or schedule produces the missing source" in compact
    assert "Technical details" in body
    assert "diagnostic only; it does not gate readiness" in compact
    assert "Confirm fresh installation" not in body
    assert "Confirm upgraded installation" not in body
    assert "<form" not in body


def test_settings_page_explains_automatic_source_rebind(monkeypatch):
    mod = load_plugin()
    source = {
        **readiness_source(),
        "server_id": "legacy-default",
        "candidate_server_id": "server-a",
        "rebind_status": "rebind_required",
    }
    readiness = {
        "detected_core_version": "v3.0.5",
        "status": "source_rebind_required",
        "ready": False,
        "administrator_acknowledged": False,
        "blockers": ["source_rebind_required"],
    }
    monkeypatch.setattr(mod, "_v3_readiness_sources", lambda: [(source, readiness)])

    body = mod.render_v3_readiness_panel()

    assert "verify the AudioMuse source identity during app sync" in body
    assert "Waiting for Lumae app sync to verify this source automatically" in body
    assert "No manual confirmation is needed" in body
    assert "Confirm fresh installation" not in body
    assert "Confirm upgraded installation" not in body


def test_source_analysis_status_remains_visible_on_legacy_core(monkeypatch):
    mod = load_plugin()
    source = {
        **readiness_source(),
        "analysis": {
            "status": "complete",
            "mapped_track_count": 96,
            "item_count": 94,
        },
    }
    monkeypatch.setattr(mod, "_v3_readiness_sources", lambda: [])
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [source])

    body = mod.render_v3_readiness_panel()
    compact = " ".join(body.split())

    assert "2. AudioMuse source analysis" in body
    assert "published for 96 provider tracks" in compact
    assert "AudioMuse analysis items: 94" in compact


def test_settings_page_reports_lumae_relationship_generation_separately(monkeypatch):
    mod = load_plugin()
    source = {
        **readiness_source(),
        "catalog": {"generation": 7, "status": "complete"},
        "analysis": {"generation": 9, "status": "complete"},
    }
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [source])
    monkeypatch.setattr(
        mod,
        "relationship_status",
        lambda _db, _catalog_id: {
            "status": "complete",
            "schema_version": mod.RELATIONSHIP_SCHEMA_VERSION,
            "algorithm_version": mod.RELATIONSHIP_ALGORITHM_VERSION,
            "source_catalog_generation": 7,
            "source_analysis_generation": 9,
            "generation": 4,
            "album_count": 2400,
            "artist_count": 870,
        },
    )

    body = mod.render_relationship_status_panel()
    compact = " ".join(body.split())

    assert "4. Similar albums &amp; artists" in body
    assert "Similarities are ready for 2,400 albums and 870 artists" in compact
    assert "Lumae’s own ranking algorithm" in body
    assert "does no relationship matching on the phone" in compact
    assert "Built from library generation 7 of 7" in compact


def test_relationship_status_refreshes_while_automatic_build_runs(monkeypatch):
    mod = load_plugin()
    source = {
        **readiness_source(),
        "catalog": {"generation": 7, "status": "complete"},
        "analysis": {"generation": 9, "status": "complete"},
    }
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [source])
    monkeypatch.setattr(
        mod,
        "relationship_status",
        lambda _db, _catalog_id: {
            "status": "running",
            "schema_version": mod.RELATIONSHIP_SCHEMA_VERSION,
            "algorithm_version": mod.RELATIONSHIP_ALGORITHM_VERSION,
            "source_catalog_generation": 6,
            "source_analysis_generation": 8,
            "generation": 3,
            "album_count": 2300,
            "artist_count": 840,
        },
    )

    body = mod.render_relationship_status_panel()

    assert "Similar album and artist relationships are being prepared automatically" in body
    assert "currently published relationship generation" in body
    assert "_lumae_relationship_refresh" in body


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
    assert ctx.flask_hooks == [mod.observe_provider_identities_on_start]
    assert ctx.song_hooks == [mod.analyze_song_hook]
    assert ctx.tasks == [
        ("prepare", mod.prepare_lumae_task, "default"),
        ("profile_backfill", mod.profile_backfill_task, "default"),
        ("analysis_projection", mod.analysis_projection_task, "default"),
        ("relationship_preparation", mod.relationship_preparation_task, "default"),
        ("provider_identity_recheck", mod.provider_identity_recheck_task, "default"),
    ]
    assert ctx.cron_tasks == [
        ("catalog_reconcile", mod.catalog_reconcile_task, "default"),
        ("catalog_refresh", mod.catalog_refresh_task, "default"),
        ("provider_identity_recheck", mod.provider_identity_recheck_task, "default"),
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
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [settings_catalog_source()])
    monkeypatch.setattr(mod, "preparation_state", lambda _catalog_id: None)
    monkeypatch.setattr(mod, "profile_backfill_state", lambda _catalog_id: None)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda **_kwargs: {
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
    assert 'class="lumae-meter-fill" style="width: 1%;"' in body
    assert "Refresh required data" in body
    assert "Prepare missing volume &amp; ramps" in body
    assert "Queue all profiles" not in body
    assert "Tracks per background batch" in body
    assert "15,894 need analysis" in body
    assert "15,894" in body
    assert "Enable scheduled catch-up" not in body
    assert "Cron expression" not in body
    assert "Scheduled Tasks" not in body
    assert "Living Collections" in body
    assert "Enable the collection manager" in body
    assert "View database state" in body


def test_database_state_snapshot_is_source_scoped_and_generation_aware():
    state = importlib.import_module("plugins.LumaeAnalysis.database_state")
    compatibility_module = importlib.import_module("plugins.LumaeAnalysis.core_compat")
    compatibility = compatibility_module.CoreCompatibility(
        "v3.0.5",
        (3, 0, 5),
        "v3_registry",
        "compatible",
        True,
    )
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "provider_type": "navidrome",
        "name": "Main Navidrome",
        "is_default": True,
        "rebind_status": "active",
        "catalog": {
            "generation": 7,
            "epoch": "catalog-epoch",
            "head_seq": 101,
            "floor_seq": 4,
            "status": "complete",
            "entity_counts": {
                "library": 1,
                "artist": 100,
                "album": 50,
                "track": 1000,
            },
            "field_coverage": {"track_number": {"ratio": 0.98}},
        },
        "analysis": {
            "generation": 9,
            "epoch": "analysis-epoch",
            "head_seq": 88,
            "floor_seq": 3,
            "status": "complete",
            "item_count": 900,
            "mapped_track_count": 950,
        },
    }

    class Cursor:
        def __init__(self, db):
            self.db = db
            self.result = None

        def execute(self, sql, params=()):
            normalized = " ".join(sql.split())
            self.db.executed.append((normalized, params))
            if "AS total" in normalized and "track_analysis_links" in normalized:
                self.result = [(1000, 930, 800, 130, 10, 20, 40, 850)]
            elif "AS items" in normalized and "analysis_items" in normalized:
                self.result = [(850, 840, 700)]
            elif "AS analysis_groups" in normalized:
                self.result = [(850, 40, 4)]
            elif "AS catalogue_tracks" in normalized:
                self.result = [(1000, 600, 550, 20, 5, 25, 400)]
            elif "FROM plugin_lumae_analysis__preparation_state" in normalized:
                self.result = [
                    (
                        "ready",
                        "catalog_ready",
                        20,
                        2,
                        None,
                        "2026-07-26T10:00:00Z",
                        "2026-07-26T10:01:00Z",
                        "2026-07-26T10:01:00Z",
                    )
                ]
            elif "FROM plugin_lumae_analysis__profile_backfill_state" in normalized:
                self.result = [
                    (
                        "running",
                        550,
                        20,
                        None,
                        "2026-07-26T10:01:00Z",
                        None,
                        "2026-07-26T10:02:00Z",
                    )
                ]
            elif "FROM plugin_lumae_analysis__analysis_runs" in normalized:
                self.result = [("complete", 3, "2026-07-26T10:00:00Z")]
            elif "FROM plugin_lumae_analysis__catalog_changes" in normalized:
                self.result = [(101,)]
            elif "FROM plugin_lumae_analysis__analysis_changes" in normalized:
                self.result = [(88,)]
            elif "FROM plugin_lumae_analysis__stream_bootstrap_sessions" in normalized:
                self.result = [(1, 4)]
            elif "AS mapping_rows" in normalized:
                self.result = [(950, 850, 850, 840, 700, 725)]
            else:
                raise AssertionError(f"Unexpected diagnostic query: {normalized}")

        def fetchone(self):
            return self.result[0] if self.result else None

        def fetchall(self):
            return list(self.result or [])

        def close(self):
            return None

    class Db:
        def __init__(self):
            self.executed = []
            self.rollbacks = 0

        def cursor(self):
            return Cursor(self)

        def rollback(self):
            self.rollbacks += 1

    db = Db()
    snapshot = state.collect_database_state(
        db,
        compatibility,
        [source],
        readiness_by_source={"catalog-a": {"status": "progressive"}},
    )

    assert snapshot["status"] == "ready"
    assert snapshot["errors"] == []
    assert db.rollbacks == 0
    result = snapshot["sources"][0]
    assert result["links"] == {
        "total": 1000,
        "usable": 930,
        "verified": 800,
        "provisional": 130,
        "pending": 10,
        "suspect": 20,
        "missing": 40,
        "usable_analysis_ids": 850,
    }
    assert result["items"]["shared_groups"] == 40
    assert result["profiles"]["ready"] == 550
    assert result["core"]["chromaprint"] == 725
    assert result["journals"]["bootstrap_leases"]["active"] == 1
    assert result["readiness"]["status"] == "progressive"
    projection_queries = [
        (sql, params)
        for sql, params in db.executed
        if "track_analysis_links" in sql or "analysis_items" in sql
    ]
    assert projection_queries
    assert all(params == ("catalog-a", 9) for _sql, params in projection_queries)
    assert "review_state IN ('needs_repair', 'needs_review')" in projection_queries[0][0]
    core_query = next(
        (sql, params) for sql, params in db.executed if "AS mapping_rows" in sql
    )
    assert core_query[1] == ("server-a",)


def test_database_state_reads_v2_core_as_one_direct_provider():
    state = importlib.import_module("plugins.LumaeAnalysis.database_state")
    compatibility_module = importlib.import_module("plugins.LumaeAnalysis.core_compat")
    compatibility = compatibility_module.CoreCompatibility(
        "v2.6.2",
        (2, 6, 2),
        "v2_single_server",
        "compatible",
        True,
    )

    class Cursor:
        def __init__(self):
            self.sql = ""

        def execute(self, sql, params=()):
            self.sql = " ".join(sql.split())
            assert params == ()

        def fetchone(self):
            return (100, 99, 75)

        def close(self):
            return None

    class Db:
        def __init__(self):
            self.cursor_instance = Cursor()

        def cursor(self):
            return self.cursor_instance

    db = Db()
    errors = []
    result = state._core_state(db, compatibility, {}, errors)

    assert errors == []
    assert result == {
        "mode": "single_server",
        "mapping_rows": 100,
        "canonical_analysis_ids": 100,
        "scored": 100,
        "musicnn_vectors": 99,
        "clap_vectors": 75,
        "chromaprint": None,
    }
    assert "(SELECT count(*) FROM score)" in db.cursor_instance.sql
    assert "track_server_map" not in db.cursor_instance.sql


def test_database_state_explains_an_uninitialized_database():
    state = importlib.import_module("plugins.LumaeAnalysis.database_state")
    compatibility_module = importlib.import_module("plugins.LumaeAnalysis.core_compat")
    compatibility = compatibility_module.CoreCompatibility(
        "v2.6.2",
        (2, 6, 2),
        "v2_single_server",
        "compatible",
        True,
    )

    snapshot = state.collect_database_state(None, compatibility, [])
    body = state.render_database_state(snapshot)

    assert snapshot["status"] == "database_unavailable"
    assert snapshot["errors"] == [
        {
            "section": "database",
            "message": "AudioMuse did not provide a database connection.",
        }
    ]
    assert "No published Lumae catalogue yet" in body
    assert "run Prepare Lumae" in body
    assert "diagnostic queries database_unavailable" in body
    assert "app sync not ready" in body


def test_database_state_page_renders_partial_state_without_exposing_rows(monkeypatch):
    mod = load_plugin()
    source = settings_catalog_source()
    source["catalog"].update(
        {
            "generation": 2,
            "epoch": "cat-epoch",
            "head_seq": 12,
            "floor_seq": 0,
            "entity_counts": {"track": 100, "album": 12, "artist": 30, "library": 1},
            "field_coverage": {"track_number": {"ratio": 1.0}},
            "completed_at": "2026-07-26T10:00:00Z",
        }
    )
    source["analysis"].update(
        {
            "generation": 3,
            "epoch": "analysis-epoch",
            "head_seq": 7,
            "floor_seq": 0,
            "completed_at": "2026-07-26T10:01:00Z",
        }
    )
    snapshot = {
        "captured_at": "2026-07-26T10:02:00Z",
        "status": "partial",
        "core": {
            "core_version": "v3.0.5",
            "core_adapter": "v3_registry",
        },
        "sources": [
            {
                "identity": {
                    "catalog_instance_id": "catalog-a",
                    "server_id": "server-a",
                    "provider_type": "navidrome",
                    "name": "Main Navidrome",
                    "is_default": True,
                    "rebind_status": "active",
                },
                "catalog": source["catalog"],
                "analysis": source["analysis"],
                "links": {
                    "total": 100,
                    "usable": 95,
                    "verified": 80,
                    "provisional": 15,
                    "pending": 1,
                    "suspect": 2,
                    "missing": 2,
                    "usable_analysis_ids": 90,
                },
                "items": {
                    "items": 90,
                    "musicnn_vectors": 89,
                    "clap_vectors": 70,
                    "analysis_groups": 90,
                    "shared_groups": 5,
                    "largest_group": 3,
                },
                "profiles": {
                    "catalogue_tracks": 100,
                    "stored": 60,
                    "ready": 55,
                    "pending": 2,
                    "failed": 1,
                    "skipped": 2,
                    "needs_attention": 40,
                },
                "workflow": {
                    "preparation": None,
                    "backfill": None,
                    "analysis_runs": [],
                },
                "journals": {
                    "catalog": {"rows": 12, "head": 12, "floor": 0},
                    "analysis": {"rows": 7, "head": 7, "floor": 0},
                    "bootstrap_leases": {"active": 0, "completed": 1},
                },
                "core": {
                    "mode": "source_scoped",
                    "mapping_rows": 98,
                    "canonical_analysis_ids": 90,
                    "scored": 90,
                    "musicnn_vectors": 89,
                    "clap_vectors": 70,
                    "chromaprint": 75,
                },
                "readiness": {"status": "progressive"},
                "errors": [
                    {
                        "section": "AudioMuse core",
                        "message": "<private> failed",
                    }
                ],
            }
        ],
        "errors": [{"section": "AudioMuse core", "message": "<private> failed"}],
    }
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [source])
    monkeypatch.setattr(mod, "detect_core", lambda: types.SimpleNamespace(
        adapter="v3_registry",
        as_dict=lambda: snapshot["core"],
    ))
    monkeypatch.setattr(mod, "dedup_policy", lambda: {})
    monkeypatch.setattr(mod, "v3_release_readiness", lambda *_args: {"status": "progressive"})
    monkeypatch.setattr(mod, "collect_database_state", lambda *_args, **_kwargs: snapshot)
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.get("/database-state")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    compact = " ".join(body.split())
    assert "Lumae database state" in body
    assert "Required for app sync" in body
    assert "1. Navidrome catalogue" in body
    assert "Usable sonic coverage" in body
    assert "Provisional" in body
    assert "Usable but flagged" in body
    assert "repair-flagged links stay usable" in compact
    assert "Chromaprint coverage" in body
    assert "profiles never remove tracks" in body
    assert "Journals &amp; leases" in body
    assert "&lt;private&gt; failed" in body
    assert "<private> failed" not in body
    assert 'href="settings"' in body


def test_database_state_marks_a_completed_empty_catalogue_not_ready():
    state = importlib.import_module("plugins.LumaeAnalysis.database_state")
    zero_counts = {
        "total": 0,
        "usable": 0,
        "verified": 0,
        "provisional": 0,
        "pending": 0,
        "suspect": 0,
        "missing": 0,
        "usable_analysis_ids": 0,
    }
    snapshot = {
        "captured_at": "2026-07-26T18:09:35Z",
        "status": "ready",
        "core": {"core_version": "v3.0.5", "core_adapter": "v3_registry"},
        "errors": [],
        "sources": [
            {
                "identity": {
                    "catalog_instance_id": "catalog-a",
                    "server_id": "server-a",
                    "provider_type": "navidrome",
                    "name": "Navidrome",
                    "is_default": True,
                    "rebind_status": "active",
                },
                "catalog": {
                    "generation": 1,
                    "head_seq": 0,
                    "floor_seq": 0,
                    "status": "complete",
                    "entity_counts": {
                        "library": 1,
                        "artist": 0,
                        "album": 0,
                        "track": 0,
                    },
                    "field_coverage": {},
                },
                "analysis": {"generation": 1, "status": "complete"},
                "links": zero_counts,
                "items": {
                    "items": 0,
                    "musicnn_vectors": 0,
                    "clap_vectors": 0,
                    "shared_groups": 0,
                    "largest_group": 0,
                },
                "profiles": {
                    "catalogue_tracks": 0,
                    "stored": 0,
                    "ready": 0,
                    "pending": 0,
                    "failed": 0,
                    "skipped": 0,
                    "needs_attention": 0,
                },
                "workflow": {
                    "preparation": {
                        "status": "ready",
                        "phase": "catalog_ready",
                        "updated_at": "2026-07-26T18:09:00Z",
                    },
                    "backfill": None,
                    "analysis_runs": [],
                },
                "journals": {
                    "catalog": {"rows": 0, "head": 0, "floor": 0},
                    "analysis": {"rows": 0, "head": 0, "floor": 0},
                    "bootstrap_leases": {"active": 0, "completed": 0},
                },
                "core": {
                    "mode": "source_scoped",
                    "mapping_rows": 0,
                    "canonical_analysis_ids": 0,
                    "scored": 0,
                    "musicnn_vectors": 0,
                    "clap_vectors": 0,
                    "chromaprint": 0,
                },
                "readiness": {"status": "no_analysis_mappings", "blockers": []},
                "errors": [],
            }
        ],
    }

    body = state.render_database_state(snapshot)
    compact = " ".join(body.split())

    assert "Lumae is not ready: the published catalogue is empty" in body
    assert "App-ready sources" in body
    assert "0 / 1" in body
    assert "empty - not ready" in body
    assert "blocked by empty catalogue" in body
    assert "invalid (recorded ready)" in body
    assert "Query / workflow errors" in body
    assert compact.index("1. Navidrome catalogue") < compact.index("4. Loudness &amp; SmoothFade")


def test_collection_setting_must_be_enabled_before_manager_is_available(monkeypatch):
    mod = load_plugin()
    collections = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    saved = []
    menu_states = []
    monkeypatch.setattr(mod, "set_setting", lambda key, value: saved.append((key, value)))
    monkeypatch.setattr(mod, "sync_collections_menu", lambda enabled: menu_states.append(enabled))
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 25)
    monkeypatch.setattr(mod, "render_source_preparation_sections", lambda _batch_size: ("", ""))
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
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [settings_catalog_source()])
    monkeypatch.setattr(mod, "preparation_state", lambda _catalog_id: None)
    monkeypatch.setattr(
        mod,
        "profile_backfill_state",
        lambda _catalog_id: {"status": "running", "last_error": None},
    )
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda **_kwargs: {
            "total_with_files": 100,
            "ready_current": 82,
            "pending": 4,
            "failed": 2,
            "skipped": 1,
            "needs_analysis": 11,
        },
    )
    monkeypatch.setattr(
        mod,
        "render_v3_readiness_panel",
        lambda: '<section><h3>2. AudioMuse source analysis</h3></section>',
    )
    monkeypatch.setattr(
        mod,
        "relationship_status",
        lambda _db, _catalog_id: {
            "status": "complete",
            "schema_version": mod.RELATIONSHIP_SCHEMA_VERSION,
            "algorithm_version": mod.RELATIONSHIP_ALGORITHM_VERSION,
            "source_catalog_generation": 0,
            "source_analysis_generation": 0,
            "generation": 1,
            "album_count": 50,
            "artist_count": 20,
        },
    )
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.get("/settings")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    compact = " ".join(body.split())
    assert 'aria-valuenow="82"' in body
    assert "82 of 100 volume and ramp profiles ready" in body
    assert "11 need analysis" in body
    assert "1. Library status" in body
    assert "2. AudioMuse source analysis" in body
    assert "3. Volume &amp; ramp status" in body
    assert "4. Similar albums &amp; artists" in body
    assert "App sync index" in body
    assert "Ready for app sync: 100 Navidrome tracks are published" in body
    assert "do not block library sync, AudioMuse source analysis, or Lumae relationships" in compact
    assert "location.reload" not in body
    assert 'u.searchParams.set("_lumae_refresh",Date.now().toString())' in body
    assert "location.replace(u.href)" in body
    assert (
        body.index("1. Library status")
        < body.index("2. AudioMuse source analysis")
        < body.index("3. Volume &amp; ramp status")
        < body.index("4. Similar albums &amp; artists")
    )


def test_settings_page_recovers_transaction_after_identity_status_query_fails(monkeypatch):
    mod = load_plugin()

    class Db:
        def __init__(self):
            self.aborted = False
            self.rollbacks = 0

        def rollback(self):
            self.aborted = False
            self.rollbacks += 1

    db = Db()
    source = settings_catalog_source()

    def fail_identity_status(*_args):
        db.aborted = True
        raise RuntimeError(
            'relation "plugin_lumae_analysis__provider_identity_transitions" does not exist'
        )

    def render_collections():
        assert db.aborted is False
        return '<section id="collections-recovered"></section>'

    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [source])
    monkeypatch.setattr(mod, "provider_transition_health", fail_identity_status)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 25)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "render_v3_readiness_panel", lambda: "")
    monkeypatch.setattr(mod, "render_relationship_status_panel", lambda: "")
    monkeypatch.setattr(mod, "render_source_preparation_sections", lambda _size: ("", ""))
    monkeypatch.setattr(mod, "render_collections_settings_panel", render_collections)
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)

    body = mod.render_settings()

    assert db.rollbacks == 1
    assert 'id="collections-recovered"' in body


def test_settings_page_never_labels_a_completed_empty_catalogue_ready(monkeypatch):
    mod = load_plugin()
    source = settings_catalog_source()
    source["catalog"]["entity_counts"] = {"track": 0}
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 25)
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [source])
    monkeypatch.setattr(
        mod,
        "preparation_state",
        lambda _catalog_id: {"status": "ready", "phase": "catalog_ready", "last_error": None},
    )
    monkeypatch.setattr(mod, "profile_backfill_state", lambda _catalog_id: None)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda **_kwargs: {
            "total_with_files": 0,
            "ready_current": 0,
            "pending": 0,
            "failed": 0,
            "skipped": 0,
            "needs_analysis": 0,
        },
    )
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)

    body = plugin_client(mod).get("/settings").get_data(as_text=True)

    assert "Not ready - empty catalogue" in body
    assert "Not ready: no Navidrome tracks were published" in body
    assert "A completed job with zero tracks is not ready" in body
    assert "Refresh required data" in body
    assert body.index("1. Library status") < body.index("3. Volume &amp; ramp status")


def test_settings_page_starts_bounded_background_enrichment(
    monkeypatch,
):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 250)
    source = settings_catalog_source()
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(
        mod,
        "start_profile_backfill",
        lambda **_kwargs: {"queued": True, "coalesced": False, "batch_size": 25},
    )
    monkeypatch.setattr(mod, "set_setting", lambda key, value: None)
    monkeypatch.setattr(mod, "render_source_preparation_sections", lambda _batch_size: ("", ""))
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.post(
        "/settings",
        data={
            "backfill_batch_size": "25",
            "action": "start_backfill",
            "server_id": "server-a",
            "catalog_instance_id": "catalog-a",
        },
    )

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'class="lumae-notice lumae-notice-success" role="status"' in body
    assert "Started background enrichment for Main Navidrome in batches of 25" in body
    assert "Playback requests are prioritized separately" in body


def test_settings_prepare_action_claims_and_enqueues_exact_source_once(monkeypatch):
    mod = load_plugin()
    source = settings_catalog_source()
    calls = []
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_kwargs: source)
    monkeypatch.setattr(mod, "preparation_state", lambda _catalog_id: None)
    monkeypatch.setattr(mod, "claim_preparation", lambda selected: selected == source)
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, *args, queue="default": calls.append((func.__name__, args, queue)),
    )
    monkeypatch.setattr(mod, "set_setting", lambda *_args: None)
    monkeypatch.setattr(mod, "render_source_preparation_sections", lambda _batch_size: ("", ""))
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)

    response = plugin_client(mod).post(
        "/settings",
        data={
            "action": "prepare_lumae",
            "server_id": "server-a",
            "catalog_instance_id": "catalog-a",
        },
    )

    assert response.status_code == 200
    assert calls == [
        ("prepare_lumae_task", ("server-a", "catalog-a"), "default"),
    ]
    assert "Preparing Main Navidrome" in response.get_data(as_text=True)


def test_provider_bridge_probe_returns_only_sanitized_navidrome_identity():
    from contextlib import nullcontext

    from plugins.LumaeAnalysis.catalog_providers import ProviderCatalogBridge

    class Module:
        @staticmethod
        def _navidrome_request(endpoint, timeout=None):
            assert endpoint == "ping"
            assert timeout == 5
            return {
                "status": "ok",
                "type": "navidrome",
                "serverVersion": "0.64.0 (abcdef0)",
                "ignoredSecret": "must-not-leak",
            }

    class Core:
        @staticmethod
        def list_servers():
            return [
                {
                    "server_id": "server-a",
                    "name": "Main",
                    "provider_type": "navidrome",
                    "is_default": True,
                }
            ]

        @staticmethod
        def bind(_server_id):
            return nullcontext()

        @staticmethod
        def provider_module(_provider_type):
            return Module()

    assert ProviderCatalogBridge(core_adapter=Core()).probe_server_identity("server-a") == {
        "provider_type": "navidrome",
        "server_type": "navidrome",
        "server_version": "0.64.0 (abcdef0)",
    }


def test_identity_inspection_distinguishes_rekey_incomplete_and_conflict():
    from plugins.LumaeAnalysis.provider_identity_guard import inspect_track_id_sets

    old = [
        "e3b7fc2ae9447bbec37a13bf916e3cf6",
        "zzzzzzzzzzzzzzzzzzzzzz",
        "5cLJPkLA5DK2BADhoeotPk",
    ]
    transformed = [
        "6VHl3uR4kss6sUPKA8Cwnk",
        "3LyqmwQBm5IRqlVjNYASwb",
        "5cLJPkLA5DK2BADhoeotPk",
    ]

    detected = inspect_track_id_sets(old, transformed)
    assert detected.status == "transition_detected"
    assert detected.matched_rekeys == 2
    assert detected.blocked is True

    unchanged = inspect_track_id_sets(old, old)
    assert unchanged.status == "unchanged"
    assert unchanged.blocked is False

    conflict = inspect_track_id_sets(old, [*old, *transformed])
    assert conflict.status == "conflict"
    assert conflict.duplicate_targets == 2

    many_old = [f"{value:032x}" for value in range(30)]
    incomplete = inspect_track_id_sets(many_old, [])
    assert incomplete.status == "incomplete"


def test_refresh_shield_stops_before_provider_diff_publication(monkeypatch):
    from plugins.LumaeAnalysis import catalog

    db = RefreshDb(
        previous_counts={"track": 1},
        previous_generation=1,
        published_fingerprints={
            "catalog_tracks": [("old-id", "metadata", "media", None)],
        },
    )
    bridge = RefreshBridge(
        {
            "libraries": [{"id": "library-1", "name": "Music"}],
            "tracks": [
                {
                    "id": "new-id",
                    "title": "Song",
                    "_lumae_library_ids": ["library-1"],
                }
            ],
        }
    )
    monkeypatch.setattr(
        catalog,
        "observe_provider_version",
        lambda *_args, **_kwargs: {
            "observation": "verified",
            "current_provider_version": "0.64.0",
        },
    )
    monkeypatch.setattr(
        catalog,
        "inspect_catalog_identity",
        lambda *_args, **_kwargs: {"state": "transition_pending"},
    )

    result = catalog.refresh_catalog("server-a", db=db, bridge=bridge)

    assert result["change_reason"] == "provider_identity_wait"
    assert not any("INSERT INTO plugin_lumae_analysis__catalog_changes" in sql for sql, _ in db.executed)
    assert not any("SET published_generation=" in sql for sql, _ in db.executed)


def test_analysis_shield_runs_before_reading_current_audiomuse_mapping(monkeypatch):
    from plugins.LumaeAnalysis import catalog_analysis
    from plugins.LumaeAnalysis.provider_identity_guard import ProviderIdentityTransitionPending

    class GuardedProjectionAdapter(ProjectionAdapter):
        @staticmethod
        def provider_module(_provider_type):
            return object()

    db = ProjectionDb()
    monkeypatch.setattr(
        catalog_analysis,
        "assert_analysis_projection_allowed",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ProviderIdentityTransitionPending("identity pending")
        ),
    )

    with pytest.raises(catalog_analysis.CatalogScanError, match="identity pending"):
        catalog_analysis.project_analysis(
            "server-a",
            db=db,
            adapter=GuardedProjectionAdapter(),
        )

    assert not any("FROM fake_mapping" in sql for sql, _ in db.executed)


def _identity_fixture_catalog(track_id, album_id, artist_id):
    return {
        "libraries": [{"id": "library-1", "name": "Music"}],
        "albums": [
            {
                "id": album_id,
                "name": "Record",
                "artistItems": [{"id": artist_id, "name": "Artist"}],
            }
        ],
        "tracks": [
            {
                "id": track_id,
                "title": "Song",
                "albumId": album_id,
                "album": "Record",
                "artist": "Artist",
                "artistId": artist_id,
                "musicFolderId": "library-1",
                "duration": 180,
            }
        ],
    }


def _fingerprints_by_entity(normalized):
    from plugins.LumaeAnalysis.catalog import ENTITY_COLLECTIONS, ENTITY_ORDER, ENTITY_TABLES

    result = {}
    for entity_type in ENTITY_ORDER:
        id_column = ENTITY_TABLES[entity_type][1]
        rows = {}
        for row in normalized[ENTITY_COLLECTIONS[entity_type]]:
            values = [row["metadata_fp"]]
            if entity_type == "album":
                values.append(row["artwork_fp"])
            elif entity_type == "track":
                values.extend((row["media_fp"], row["artwork_fp"]))
            rows[row[id_column]] = tuple(values)
        result[entity_type] = rows
    return result


def test_rekey_plan_uses_exact_codec_for_all_provider_entities():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id
    from plugins.LumaeAnalysis.provider_identity_rekey import (
        build_provider_identity_rekey_plan,
    )

    old_ids = {
        "track": "e3b7fc2ae9447bbec37a13bf916e3cf6",
        "album": "0123456789abcdef0123456789abcdef",
        "artist": "11111111111111111111111111111111",
    }
    new_ids = {
        kind: canonicalize_navidrome_id(value).value for kind, value in old_ids.items()
    }
    old = normalize_provider_catalog(
        _identity_fixture_catalog(old_ids["track"], old_ids["album"], old_ids["artist"]),
        "navidrome",
    )
    target = normalize_provider_catalog(
        _identity_fixture_catalog(new_ids["track"], new_ids["album"], new_ids["artist"]),
        "navidrome",
    )

    plan = build_provider_identity_rekey_plan(_fingerprints_by_entity(old), target)

    assert plan.counts == {
        "rekey": 3,
        "unchanged": 1,
        "addition": 0,
        "confirmed_removal": 0,
        "conflict": 0,
    }
    assert [(row["entity_type"], row["old_id"], row["new_id"]) for row in plan.mappings] == [
        ("artist", old_ids["artist"], new_ids["artist"]),
        ("album", old_ids["album"], new_ids["album"]),
        ("track", old_ids["track"], new_ids["track"]),
    ]
    assert all(event.payload[event.entity_type + "_id"] == event.entity_id for event in plan.events)


def test_rekey_plan_rejects_old_and_new_identity_collision():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id
    from plugins.LumaeAnalysis.provider_identity_rekey import (
        build_provider_identity_rekey_plan,
    )

    old_id = "e3b7fc2ae9447bbec37a13bf916e3cf6"
    new_id = canonicalize_navidrome_id(old_id).value
    target = normalize_provider_catalog(
        {"tracks": [{"id": new_id, "title": "Song"}]}, "navidrome"
    )
    previous = _fingerprints_by_entity(target)
    previous["track"][old_id] = previous["track"][new_id]

    with pytest.raises(ValueError, match="not one-to-one"):
        build_provider_identity_rekey_plan(previous, target)


def test_exact_json_rewrite_never_changes_substrings_and_blocks_key_collisions():
    from plugins.LumaeAnalysis.provider_identity_rekey import _replace_exact

    assert _replace_exact(
        {
            "old": "old",
            "nested": ["old", "prefix-old", {"id": "old"}],
        },
        {"old": "new"},
    ) == {
        "new": "new",
        "nested": ["new", "prefix-old", {"id": "new"}],
    }
    with pytest.raises(ValueError, match="collides"):
        _replace_exact({"old": 1, "new": 2}, {"old": "new"})


class IdentityInspectionCursor(FakeCursor):
    def __init__(self, db):
        super().__init__([])
        self.db = db

    def execute(self, sql, params=None):
        super().execute(sql, params)
        self.db.executed.append((sql, params))
        if "SELECT COALESCE(c.published_generation" in sql:
            self.rows = [
                (
                    7,
                    5,
                    self.db.transition_id,
                    "transition_pending",
                    "0.64.0",
                    self.db.target_fingerprint,
                    self.db.target_scan_count,
                )
            ]
        elif "SELECT track_id FROM" in sql:
            self.rows = [(self.db.old_id,)]
        elif "target_scan_count=%s" in sql:
            self.db.transition_id = params[0]
            self.db.target_fingerprint = params[6]
            self.db.target_scan_count = params[7]
            self.rows = []
        else:
            self.rows = []


class IdentityInspectionDb:
    def __init__(self, old_id):
        self.old_id = old_id
        self.transition_id = "transition-a"
        self.target_fingerprint = None
        self.target_scan_count = 0
        self.executed = []

    def cursor(self):
        return IdentityInspectionCursor(self)


def test_identity_publication_requires_two_identical_full_target_scans():
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id
    from plugins.LumaeAnalysis.provider_identity_guard import inspect_catalog_identity

    old_id = "e3b7fc2ae9447bbec37a13bf916e3cf6"
    new_id = canonicalize_navidrome_id(old_id).value
    db = IdentityInspectionDb(old_id)

    first = inspect_catalog_identity(db, "catalog-a", [new_id], "0.64.0", "full-fp")
    second = inspect_catalog_identity(db, "catalog-a", [new_id], "0.64.0", "full-fp")

    assert first["state"] == second["state"] == "transition_pending"
    assert first["target_scan_count"] == 1
    assert second["target_scan_count"] == 2


def test_target_scan_fingerprint_treats_contributor_order_as_unordered():
    from copy import deepcopy

    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.provider_identity_rekey import target_scan_fingerprint

    first = normalize_provider_catalog(
        {
            "tracks": [
                {
                    "id": "track-a",
                    "title": "Song",
                    "contributors": [
                        {"role": "composer", "artist": {"id": "a", "name": "A"}},
                        {"role": "producer", "artist": {"id": "b", "name": "B"}},
                    ],
                    "genres": ["Rock", "Alternative"],
                }
            ]
        },
        "navidrome",
    )
    reordered = deepcopy(first)
    reordered["tracks"][0]["payload"]["contributors"].reverse()

    assert target_scan_fingerprint(first) == target_scan_fingerprint(reordered)

    changed = deepcopy(reordered)
    changed["tracks"][0]["payload"]["contributors"][0]["role"] = "lyricist"
    assert target_scan_fingerprint(first) != target_scan_fingerprint(changed)

    reordered_genres = deepcopy(first)
    reordered_genres["tracks"][0]["payload"]["genres"].reverse()
    assert target_scan_fingerprint(first) != target_scan_fingerprint(reordered_genres)


def test_provider_identity_queries_lock_only_non_nullable_join_rows():
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    source_db = FakeDb(
        [
            (
                "catalog-a",
                7,
                5,
                "transition-a",
                "normal",
                None,
                "0.63.2",
                "0.63.2",
                "pre_transition_version",
                None,
                {},
                None,
                None,
                0,
                None,
                None,
                {},
                None,
                None,
                None,
            )
        ]
    )
    source = guard._source_state(source_db, "server-a", for_update=True)
    source_query = " ".join(source_db.cursor_obj.executed[0][0].split())

    inspection_db = IdentityInspectionDb("e3b7fc2ae9447bbec37a13bf916e3cf6")
    guard.inspect_catalog_identity(
        inspection_db,
        "catalog-a",
        ["e3b7fc2ae9447bbec37a13bf916e3cf6"],
        "0.63.2",
        "full-fp",
    )
    inspection_query = next(
        " ".join(sql.split())
        for sql, _params in inspection_db.executed
        if "SELECT COALESCE(c.published_generation" in sql
    )

    assert source["catalog_instance_id"] == "catalog-a"
    assert "LEFT JOIN" in source_query
    assert "FOR UPDATE OF s" in source_query
    assert "LEFT JOIN" in inspection_query
    assert "FOR UPDATE OF c, p" in inspection_query


def test_transient_probe_failure_does_not_discard_an_applied_transition(monkeypatch):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    source = {
        "catalog_instance_id": "catalog-a",
        "catalog_generation": 8,
        "analysis_generation": 6,
        "transition_id": "transition-a",
        "state": "applied",
        "current_provider_version": "0.64.0",
    }
    captured = {}
    monkeypatch.setattr(guard, "_source_state", lambda *_args, **_kwargs: source)
    monkeypatch.setattr(guard, "_ensure_transition_row", lambda *_args: None)
    monkeypatch.setattr(
        guard,
        "_update_observation",
        lambda *_args, **kwargs: captured.update(kwargs),
    )

    class FailingBridge:
        @staticmethod
        def probe_server_identity(_server_id):
            raise RuntimeError("provider offline")

    db = types.SimpleNamespace(commit=lambda: None)
    result = guard.observe_provider_version(db, FailingBridge(), "server-a")

    assert result["state"] == "applied"
    assert captured["state"] == "applied"
    assert captured["required_action"] == "retry_provider_identity_check"


def test_identity_recheck_schedule_skips_normal_sources(monkeypatch):
    mod = load_plugin()

    class Bridge:
        @staticmethod
        def list_servers():
            return [{"server_id": "server-a", "supported": True}]

    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "ProviderCatalogBridge", Bridge)
    monkeypatch.setattr(
        mod,
        "resolve_catalog_source",
        lambda *_args, **_kwargs: [{"catalog_instance_id": "catalog-a"}],
    )
    monkeypatch.setattr(
        mod,
        "provider_transition_health",
        lambda *_args: {"state": "normal"},
    )
    refresh = pytest.MonkeyPatch()
    try:
        refresh.setattr(
            mod,
            "refresh_catalog",
            lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not scan")),
        )
        assert mod.provider_identity_recheck_task() == {"checked": 0, "results": []}
    finally:
        refresh.undo()


def test_identity_recheck_releases_completed_migration_and_queues_projection(monkeypatch):
    mod = load_plugin()

    class Bridge:
        @staticmethod
        def list_servers():
            return [{"server_id": "server-a", "supported": True}]

    class Db:
        commits = 0
        rollbacks = 0

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    db = Db()
    queued = []
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "get_core_adapter", lambda: "adapter-a")
    monkeypatch.setattr(mod, "ProviderCatalogBridge", Bridge)
    monkeypatch.setattr(
        mod,
        "resolve_catalog_source",
        lambda *_args, **_kwargs: [{"catalog_instance_id": "catalog-a"}],
    )
    monkeypatch.setattr(
        mod,
        "provider_transition_health",
        lambda *_args: {"state": "applied", "audiomuse_health": "repair_required"},
    )
    monkeypatch.setattr(mod, "projection_reconcile_required", lambda *_args: False)
    monkeypatch.setattr(mod, "refresh_audiomuse_health", lambda *_args, **_kwargs: "ready")
    monkeypatch.setattr(
        mod,
        "enqueue_bounded",
        lambda func, *args, **kwargs: queued.append((func, args, kwargs)),
    )

    result = mod.provider_identity_recheck_task()

    assert result == {
        "checked": 1,
        "results": [
            {
                "catalog_instance_id": "catalog-a",
                "server_id": "server-a",
                "previous_health": "repair_required",
                "reconcile_required": False,
                "audiomuse_health": "ready",
                "projection_queued": True,
            }
        ],
    }
    assert queued == [
        (
            mod.analysis_projection_task,
            ("server-a",),
            {"queue": "default", "timeout": mod.PROJECTION_JOB_TIMEOUT_SECONDS},
        )
    ]
    assert db.commits == 1
    assert db.rollbacks == 0


def test_identity_recheck_durable_request_queues_projection_when_startup_was_already_ready(
    monkeypatch,
):
    mod = load_plugin()

    class Bridge:
        @staticmethod
        def list_servers():
            return [{"server_id": "server-a", "supported": True}]

    class Db:
        commits = 0

        def commit(self):
            self.commits += 1

    db = Db()
    queued = []
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "get_core_adapter", lambda: "adapter-a")
    monkeypatch.setattr(mod, "ProviderCatalogBridge", Bridge)
    monkeypatch.setattr(
        mod,
        "resolve_catalog_source",
        lambda *_args, **_kwargs: [{"catalog_instance_id": "catalog-a"}],
    )
    monkeypatch.setattr(
        mod,
        "provider_transition_health",
        lambda *_args: {"state": "applied", "audiomuse_health": "ready"},
    )
    reconcile = {"required": False}
    monkeypatch.setattr(
        mod,
        "projection_reconcile_required",
        lambda *_args: reconcile["required"],
    )
    monkeypatch.setattr(
        mod,
        "refresh_audiomuse_health",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("ready health must not be rewritten")
        ),
    )
    monkeypatch.setattr(
        mod,
        "enqueue_bounded",
        lambda func, *args, **kwargs: queued.append((func, args, kwargs)),
    )

    assert mod.provider_identity_recheck_task() == {"checked": 0, "results": []}
    reconcile["required"] = True
    result = mod.provider_identity_recheck_task()

    assert result["results"] == [
        {
            "catalog_instance_id": "catalog-a",
            "server_id": "server-a",
            "previous_health": "ready",
            "reconcile_required": True,
            "audiomuse_health": "ready",
            "projection_queued": True,
        }
    ]
    assert queued == [
        (
            mod.analysis_projection_task,
            ("server-a",),
            {"queue": "default", "timeout": mod.PROJECTION_JOB_TIMEOUT_SECONDS},
        )
    ]
    assert db.commits == 1


class PublisherCursor(FakeCursor):
    def __init__(self, db):
        super().__init__([])
        self.db = db

    def execute(self, sql, params=None):
        super().execute(sql, params)
        self.db.executed.append((sql, params))
        normalized = " ".join(sql.split())
        if "SELECT published_generation, catalog_epoch" in normalized:
            self.rows = [(7, "catalog-epoch", 100, 2)]
        elif "SELECT projection_generation, analysis_epoch" in normalized:
            self.rows = [(5, "analysis-epoch", 200, 1, 1, "complete")]
        elif "SELECT transition_id, state, previous_provider_version" in normalized:
            self.rows = [
                (
                    "transition-a",
                    "transition_pending",
                    "0.63.2",
                    "0.64.0",
                    7,
                    5,
                    self.db.target_fingerprint,
                    2,
                )
            ]
        elif "SELECT (SELECT COUNT(*)" in normalized:
            self.rows = [(1, 1, 0, 0)]
        elif "available=TRUE" in normalized and normalized.startswith("SELECT"):
            self.rows = next(
                (
                    rows
                    for table_name, rows in self.db.fingerprint_rows.items()
                    if table_name in normalized
                ),
                [],
            )
        elif "FROM fake_mapping" in normalized:
            self.rows = [(self.db.new_track_id, "analysis-1", "fingerprint")]
        elif "SELECT provider_track_id, analysis_id" in normalized:
            self.rows = [
                (
                    self.db.old_track_id,
                    "analysis-1",
                    "ready",
                    "fingerprint",
                    "audiomuse_catalogue_fp_4",
                    0.02,
                    None,
                    True,
                    [],
                    None,
                )
            ]
        elif normalized.startswith("SELECT seq, payload FROM"):
            self.rows = []
        elif normalized.startswith("SELECT principal, idempotency_key"):
            self.rows = []
        elif "SELECT COUNT(*) FROM task_status" in normalized:
            self.rows = [(0,)]
        else:
            self.rows = []

    def executemany(self, sql, params):
        materialized = list(params)
        self.db.executed.append((sql, materialized))


class PublisherDb:
    def __init__(self, old_normalized, target_fingerprint, old_track_id, new_track_id):
        self.target_fingerprint = target_fingerprint
        self.old_track_id = old_track_id
        self.new_track_id = new_track_id
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        fingerprints = _fingerprints_by_entity(old_normalized)
        self.fingerprint_rows = {}
        for entity_type, table_name in {
            "library": "catalog_libraries",
            "artist": "catalog_artists",
            "album": "catalog_albums",
            "track": "catalog_tracks",
        }.items():
            self.fingerprint_rows[table_name] = [
                (entity_id, *values)
                for entity_id, values in fingerprints[entity_type].items()
            ]

    def cursor(self):
        return PublisherCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class PublisherAdapter:
    @staticmethod
    def analysis_mapping_sql():
        return "SELECT provider_track_id, analysis_id, match_tier FROM fake_mapping WHERE server_id=%s"


def test_atomic_publisher_carries_analysis_and_retains_recovery_evidence():
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id
    from plugins.LumaeAnalysis.provider_identity_rekey import (
        publish_provider_identity_rekey,
        target_scan_fingerprint,
    )

    old_ids = {
        "track": "e3b7fc2ae9447bbec37a13bf916e3cf6",
        "album": "0123456789abcdef0123456789abcdef",
        "artist": "11111111111111111111111111111111",
    }
    new_ids = {
        kind: canonicalize_navidrome_id(value).value for kind, value in old_ids.items()
    }
    old = normalize_provider_catalog(
        _identity_fixture_catalog(old_ids["track"], old_ids["album"], old_ids["artist"]),
        "navidrome",
    )
    target = normalize_provider_catalog(
        _identity_fixture_catalog(new_ids["track"], new_ids["album"], new_ids["artist"]),
        "navidrome",
    )
    target_fp = target_scan_fingerprint(target)
    db = PublisherDb(old, target_fp, old_ids["track"], new_ids["track"])

    result = publish_provider_identity_rekey(
        db,
        catalog_instance_id="catalog-a",
        server_id="server-a",
        normalized=target,
        target_fingerprint=target_fp,
        current_provider_version="0.64.0",
        adapter=PublisherAdapter(),
        scan_id="scan-a",
        scan_duration_ms=123,
    )

    assert result["provider_identity_transition"] == {
        "state": "applied",
        "transition_id": "transition-a",
        "first_seq": 101,
        "last_seq": 103,
        "counts": {
            "rekey": 3,
            "unchanged": 1,
            "addition": 0,
            "confirmed_removal": 0,
            "conflict": 0,
        },
        "manifest_sha256": result["provider_identity_transition"]["manifest_sha256"],
        "audiomuse_health": "ready",
    }
    assert db.commits == 1
    assert db.rollbacks == 0
    sql = "\n".join(statement for statement, _params in db.executed)
    assert "INSERT INTO plugin_lumae_analysis__analysis_items" in sql
    assert "SELECT catalog_instance_id, %s, analysis_id" in sql
    assert "INSERT INTO plugin_lumae_analysis__catalog_generation_pins" in sql
    assert "INSERT INTO plugin_lumae_analysis__provider_identity_manifests" in sql
    assert "SET state='applied'" in sql
    change_params = [
        params
        for statement, params in db.executed
        if "INSERT INTO plugin_lumae_analysis__catalog_changes" in statement
    ]
    assert [params[6] for params in change_params] == ["rekey", "rekey", "rekey"]
    assert all(json.loads(params[10])["analysis_identity_preserved"] for params in change_params)


def test_atomic_publisher_rolls_back_before_writes_when_target_proof_changes():
    from plugins.LumaeAnalysis.provider_identity_rekey import publish_provider_identity_rekey

    db = PublisherDb(
        {"libraries": [], "artists": [], "albums": [], "tracks": []},
        "persisted-fingerprint",
        "old",
        "new",
    )
    with pytest.raises(ValueError, match="stable-scan fingerprint"):
        publish_provider_identity_rekey(
            db,
            catalog_instance_id="catalog-a",
            server_id="server-a",
            normalized={
                "libraries": [],
                "artists": [],
                "albums": [],
                "tracks": [],
                "track_artists": [],
                "album_artists": [],
                "entity_libraries": [],
            },
            target_fingerprint="wrong",
            current_provider_version="0.64.0",
            adapter=PublisherAdapter(),
        )
    assert db.rollbacks == 1
    assert db.commits == 0


def test_atomic_publisher_rolls_back_generation_rows_after_mid_publish_failure(monkeypatch):
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id
    from plugins.LumaeAnalysis import provider_identity_rekey as rekey

    old_id = "e3b7fc2ae9447bbec37a13bf916e3cf6"
    new_id = canonicalize_navidrome_id(old_id).value
    old = normalize_provider_catalog(
        {"tracks": [{"id": old_id, "title": "Song"}]}, "navidrome"
    )
    target = normalize_provider_catalog(
        {"tracks": [{"id": new_id, "title": "Song"}]}, "navidrome"
    )
    target_fp = rekey.target_scan_fingerprint(target)
    db = PublisherDb(old, target_fp, old_id, new_id)
    monkeypatch.setattr(
        rekey,
        "_rekey_plugin_owned_state",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("state rewrite interrupted")),
    )

    with pytest.raises(RuntimeError, match="state rewrite interrupted"):
        rekey.publish_provider_identity_rekey(
            db,
            catalog_instance_id="catalog-a",
            server_id="server-a",
            normalized=target,
            target_fingerprint=target_fp,
            current_provider_version="0.64.0",
            adapter=PublisherAdapter(),
        )

    assert db.rollbacks == 1
    assert db.commits == 0
    assert any(
        statement.lstrip().startswith("INSERT INTO") and "catalog_tracks" in statement
        for statement, _params in db.executed
    )
    assert not any("SET catalog_schema_version=" in statement for statement, _ in db.executed)


class AudioMuseHealthCursor(FakeCursor):
    def __init__(self, active_tasks, mappings):
        super().__init__([])
        self.active_tasks = active_tasks
        self.mappings = mappings

    def execute(self, sql, params=None):
        super().execute(sql, params)
        if "FROM task_status" in sql:
            self.rows = [(self.active_tasks,)]
        elif "FROM fake_mapping" in sql:
            self.rows = [(track_id, analysis_id, "fingerprint") for track_id, analysis_id in self.mappings]
        else:
            self.rows = []


@pytest.mark.parametrize(
    ("active_tasks", "mappings", "expected"),
    [
        (1, [], "busy"),
        (0, [], "migration_required"),
        (0, [("track-new", "different-analysis")], "ready"),
        (
            0,
            [("track-new", "analysis-1"), ("track-new", "different-analysis")],
            "repair_required",
        ),
        (0, [("track-new", "analysis-1")], "ready"),
    ],
)
def test_audiomuse_health_requires_new_provider_ids_but_allows_analysis_relinking(
    active_tasks, mappings, expected
):
    from plugins.LumaeAnalysis.provider_identity_rekey import inspect_audiomuse_health

    links = {"track-new": {"analysis_id": "analysis-1"}}

    assert (
        inspect_audiomuse_health(
            AudioMuseHealthCursor(active_tasks, mappings),
            PublisherAdapter(),
            "server-a",
            links,
        )
        == expected
    )


def test_transition_manifest_route_downloads_recovery_evidence(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(
        mod,
        "read_transition_manifest",
        lambda *_args, **_kwargs: {
            "contract": "provider_identity_rekey_v1",
            "transition_id": "transition-a",
            "mappings": [{"entity_type": "track", "old_id": "old", "new_id": "new"}],
        },
    )

    response = plugin_client(mod).get(
        "/api/catalog/provider-identity/manifest?transition_id=transition-a"
    )

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "private, no-store"
    assert "lumae-provider-rekey-transition-a.json" in response.headers["Content-Disposition"]
    assert response.get_json()["mappings"][0] == {
        "entity_type": "track",
        "old_id": "old",
        "new_id": "new",
    }


def test_snapshot_pruning_preserves_generations_pinned_by_active_leases():
    from plugins.LumaeAnalysis import catalog

    class Cursor:
        def __init__(self):
            self.executed = []

        def execute(self, sql, params=None):
            self.executed.append((sql, params))

    cur = Cursor()
    catalog.prune_snapshot_generations(cur, "catalog-a", "catalog", 27)

    deletes = [
        (sql, params)
        for sql, params in cur.executed
        if " AS stale" in sql and sql.lstrip().startswith("DELETE FROM")
    ]
    assert len(deletes) == len(catalog.CATALOG_GENERATION_TABLES)
    assert all(params == ("catalog-a", 27, "catalog") for _sql, params in deletes)
    assert all("lease.pinned_generation=stale.published_generation" in sql for sql, _ in deletes)
    assert all("lease.expires_at>now()" in sql for sql, _ in deletes)
    assert all("lease.completed_at IS NULL" in sql for sql, _ in deletes)


def test_change_journal_compaction_advances_floor_and_deletes_expired_events():
    from plugins.LumaeAnalysis import catalog

    class Cursor:
        def __init__(self):
            self.executed = []

        def execute(self, sql, params=None):
            self.executed.append((sql, params))

    cur = Cursor()
    floor = catalog.compact_change_journal(
        cur,
        catalog_instance_id="catalog-a",
        state_table="analysis_state",
        changes_table="analysis_changes",
        epoch_column="analysis_epoch",
        floor_column="analysis_floor_seq",
        epoch="epoch-a",
        head_seq=212_002,
        retention_limit=84_398,
    )

    assert floor == 127_604
    assert cur.executed[0][1] == ("catalog-a", "epoch-a", "epoch-a", 127_604)
    assert "seq<=%s" in cur.executed[0][0]
    assert cur.executed[1][1] == (127_604, "catalog-a", "epoch-a")
    assert "GREATEST(analysis_floor_seq, %s)" in cur.executed[1][0]


def test_install_cleanup_prunes_backup_generations_and_bounds_core_journals():
    from plugins.LumaeAnalysis import catalog

    class Cursor:
        def __init__(self):
            self.executed = []
            self.rows = []

        def execute(self, sql, params=None):
            self.executed.append((sql, params))
            if "SELECT c.catalog_instance_id" in sql:
                self.rows = [
                    (
                        "catalog-a",
                        27,
                        "catalog-epoch",
                        0,
                        {"library": 2, "artist": 3478, "album": 1328, "track": 21569},
                        10,
                        "analysis-epoch",
                        212_002,
                        20_630,
                        21_569,
                    )
                ]
            else:
                self.rows = []

        def fetchall(self):
            return list(self.rows)

        def close(self):
            pass

    class Db:
        def __init__(self):
            self.cursor_obj = Cursor()

        def cursor(self):
            return self.cursor_obj

    db = Db()
    catalog.prune_catalog_storage(db)

    stale_deletes = [
        params
        for sql, params in db.cursor_obj.executed
        if " AS stale" in sql and sql.lstrip().startswith("DELETE FROM")
    ]
    assert ("catalog-a", 27, "catalog") in stale_deletes
    assert ("catalog-a", 10, "analysis") in stale_deletes
    assert any(
        params == (127_604, "catalog-a", "analysis-epoch")
        and "analysis_floor_seq" in sql
        for sql, params in db.cursor_obj.executed
    )


def test_enrichment_cleanup_bounds_relationship_history_to_two_snapshots():
    from plugins.LumaeAnalysis import catalog_enrichment

    class Cursor:
        def __init__(self):
            self.executed = []
            self.rows = []

        def execute(self, sql, params=None):
            self.executed.append((sql, params))
            if "SELECT p.catalog_instance_id" in sql:
                self.rows = [
                    (
                        "catalog-a",
                        "profile-epoch",
                        0,
                        21_709,
                        "relationship-epoch",
                        7_620,
                        1_324,
                        581,
                    )
                ]
            else:
                self.rows = []

        def fetchall(self):
            return list(self.rows)

        def close(self):
            pass

    class Db:
        def __init__(self):
            self.cursor_obj = Cursor()

        def cursor(self):
            return self.cursor_obj

    db = Db()
    catalog_enrichment.compact_enrichment_storage(db)

    assert any(
        params == (3_810, "catalog-a", "relationship-epoch")
        and "relationship_state" in sql
        for sql, params in db.cursor_obj.executed
    )
