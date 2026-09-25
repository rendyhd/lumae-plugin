"""P1-4 / K1: gzip transport for plugin JSON and private profile headers.

The route-level tests stub the data layer. The end-to-end test runs every
large profile route against the production schema (``migrated_db``) with
real-sized edge payloads and also records the bytes of a 50-row page.
"""
import base64
import gzip
import json
import os
import random
from pathlib import Path

import pytest
from flask import Flask, jsonify

from test_lumae_analysis import load_plugin, plugin_api_module


PREFIX = "/plugins/lumae_analysis"
SOURCE = "catalog-a"
SERVER = "server-a"
P = "plugin_lumae_analysis__"
EDGE_TEMPLATE = Path(__file__).resolve().parents[2] / (
    "docs/audit/2026-09-24/probes/lum010/edge.json"
)
GZIP = {"Accept-Encoding": "gzip, deflate, br"}


def _app(mod):
    app = Flask(__name__)
    app.register_blueprint(mod.bp, url_prefix=PREFIX)

    @app.get("/host/large")
    def host_large():  # a host route outside the plugin blueprint
        return jsonify({"rows": ["x" * 64] * 64})

    return app


def _decoded(response):
    raw = response.get_data()
    if response.headers.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _vary(response):
    return [item.strip() for item in response.headers.get("Vary", "").split(",") if item.strip()]


def _assert_gzipped(response):
    assert response.status_code == 200
    assert response.headers["Content-Encoding"] == "gzip"
    assert int(response.headers["Content-Length"]) == len(response.get_data())
    assert "accept-encoding" in [item.lower() for item in _vary(response)]
    assert response.get_data()[:2] == b"\x1f\x8b"


def _assert_plain(response):
    assert "Content-Encoding" not in response.headers
    assert int(response.headers["Content-Length"]) == len(response.get_data())


def _large_profiles(count=40):
    return [{"track_id": f"track-{i:04d}", "start_ramp": "A" * 128} for i in range(count)]


@pytest.fixture
def stubbed(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: object())
    return mod


def _stub_profiles(mod, monkeypatch, count):
    rows = [{"track_id": f"track-{i:04d}"} for i in range(count)]
    monkeypatch.setattr(
        mod, "resolve_profile_source",
        lambda **_kw: {"catalog_instance_id": SOURCE, "server_id": SERVER},
    )
    monkeypatch.setattr(mod, "fetch_published_profile_rows", lambda ids, _source: rows)
    monkeypatch.setattr(mod, "fetch_profile_rows", lambda ids, catalog_instance_id=None: [])
    monkeypatch.setattr(
        mod, "serialize_ready_profile",
        lambda row: {"track_id": row["track_id"], "start_ramp": "A" * 128},
    )
    return ",".join(row["track_id"] for row in rows)


def test_large_profiles_response_is_gzipped_and_round_trips(stubbed, monkeypatch):
    ids = _stub_profiles(stubbed, monkeypatch, 40)
    client = _app(stubbed).test_client()

    plain = client.get(f"{PREFIX}/api/profiles?ids={ids}")
    packed = client.get(f"{PREFIX}/api/profiles?ids={ids}", headers=GZIP)

    _assert_plain(plain)
    _assert_gzipped(packed)
    assert len(plain.get_data()) >= 1024
    assert _decoded(packed) == plain.get_json()
    assert len(packed.get_data()) < len(plain.get_data())


def test_profiles_route_has_private_cache_headers(stubbed, monkeypatch):
    ids = _stub_profiles(stubbed, monkeypatch, 1)
    response = _app(stubbed).test_client().get(f"{PREFIX}/api/profiles?ids={ids}")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "private, no-store"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert _vary(response)[:2] == ["Authorization", "Cookie"]


def test_vary_is_merged_not_replaced(stubbed, monkeypatch):
    ids = _stub_profiles(stubbed, monkeypatch, 40)
    response = _app(stubbed).test_client().get(f"{PREFIX}/api/profiles?ids={ids}", headers=GZIP)

    _assert_gzipped(response)
    assert _vary(response) == ["Authorization", "Cookie", "Accept-Encoding"]


def _hook(mod, response, accept="gzip"):
    """Run the blueprint's after_request hook on a hand-built response."""
    with Flask(__name__).test_request_context(headers={"Accept-Encoding": accept}):
        return mod._compress_json_response(response)


def _big_json(mod):
    with Flask(__name__).app_context():
        return jsonify({"rows": ["y" * 64] * 64})


def test_vary_accept_encoding_is_not_duplicated(stubbed):
    response = _big_json(stubbed)
    response.headers["Vary"] = "accept-encoding, Cookie"

    response = _hook(stubbed, response)

    _assert_gzipped(response)
    assert _vary(response) == ["accept-encoding", "Cookie"]


def test_hook_sets_vary_when_none_exists(stubbed):
    response = _hook(stubbed, _big_json(stubbed))

    _assert_gzipped(response)
    assert _vary(response) == ["Accept-Encoding"]


@pytest.mark.parametrize("path", [
    "/api/profiles/bootstrap/sessions",
    "/api/profiles/bootstrap/sessions/page",
    "/api/profiles/bootstrap/sessions/catchup",
])
def test_v2_bootstrap_pages_are_gzipped(stubbed, monkeypatch, path):
    monkeypatch.setattr(stubbed.host_api.config, "DATABASE_URL", "postgresql://stub", raising=False)
    page = {"protocol_version": 2, "profiles": _large_profiles(), "has_more": False}
    for name in ("create_session", "snapshot_page", "catchup_page"):
        monkeypatch.setattr(stubbed.profile_bootstrap, name, lambda _body, **_options: page)
    client = _app(stubbed).test_client()

    plain = client.post(PREFIX + path, json={})
    packed = client.post(PREFIX + path, json={}, headers=GZIP)

    _assert_plain(plain)
    _assert_gzipped(packed)
    assert packed.headers["Cache-Control"] == "private, no-store"
    assert _decoded(packed) == plain.get_json() == page


@pytest.mark.parametrize("path,name", [
    ("/api/profiles/bootstrap?catalog_instance_id=catalog-a", "profile_bootstrap_page"),
    ("/api/profiles/changes?cursor=c", "read_profile_changes"),
])
def test_legacy_profile_streams_are_gzipped(stubbed, monkeypatch, path, name):
    page = {"schema_version": 1, "profiles": _large_profiles(), "has_more": False}
    monkeypatch.setattr(stubbed, name, lambda *_a, **_kw: page)
    client = _app(stubbed).test_client()

    packed = client.get(PREFIX + path, headers=GZIP)

    _assert_gzipped(packed)
    assert packed.headers["Cache-Control"] == "private, no-cache"
    assert _decoded(packed) == page


def test_small_responses_are_not_gzipped(stubbed, monkeypatch):
    ids = _stub_profiles(stubbed, monkeypatch, 1)
    response = _app(stubbed).test_client().get(f"{PREFIX}/api/profiles?ids={ids}", headers=GZIP)

    assert response.status_code == 200
    assert len(response.get_data()) < 1024
    _assert_plain(response)
    assert response.get_json()["profiles"][0]["track_id"] == "track-0000"


def test_non_200_responses_are_not_gzipped(stubbed, monkeypatch):
    monkeypatch.setattr(stubbed, "read_profile_changes",
                        lambda *_a, **_kw: (_ for _ in ()).throw(ValueError("x" * 2048)))
    response = _app(stubbed).test_client().get(f"{PREFIX}/api/profiles/changes?cursor=c",
                                               headers=GZIP)

    assert response.status_code == 400
    assert len(response.get_data()) >= 1024
    _assert_plain(response)
    assert response.get_json()["error"] == "invalid_cursor"


def test_non_json_responses_are_not_gzipped(stubbed, monkeypatch):
    monkeypatch.setattr(stubbed, "vector_batch", lambda *_a, **_kw: b"\x00" * 4096)
    response = _app(stubbed).test_client().post(
        f"{PREFIX}/api/catalog/analysis/vectors",
        json={"catalog_instance_id": SOURCE, "analysis_ids": ["a"]},
        headers=GZIP,
    )

    assert response.status_code == 200
    assert response.mimetype == "application/vnd.lumae.f32le-v1"
    _assert_plain(response)
    assert response.get_data() == b"\x00" * 4096


@pytest.mark.parametrize("header", [
    "gzip;q=0",
    "gzip; q=0.000, deflate",
    "*;q=1, gzip;q=0",
    "identity",
    "deflate, br",
    "",
    "x-gzipper",
])
def test_gzip_refused_or_not_offered_is_not_compressed(stubbed, monkeypatch, header):
    ids = _stub_profiles(stubbed, monkeypatch, 40)
    response = _app(stubbed).test_client().get(
        f"{PREFIX}/api/profiles?ids={ids}", headers={"Accept-Encoding": header})

    assert response.status_code == 200
    _assert_plain(response)
    assert len(response.get_data()) >= 1024
    assert "accept-encoding" in [item.lower() for item in _vary(response)]
    assert len(response.get_json()["profiles"]) == 40


@pytest.mark.parametrize("header", ["gzip", "GZIP;q=0.5", "br;q=1, gzip;q=0.1", "*", "x-gzip"])
def test_gzip_accepted_forms_are_compressed(stubbed, monkeypatch, header):
    ids = _stub_profiles(stubbed, monkeypatch, 40)
    response = _app(stubbed).test_client().get(
        f"{PREFIX}/api/profiles?ids={ids}", headers={"Accept-Encoding": header})

    _assert_gzipped(response)


def test_existing_content_encoding_is_left_alone(stubbed):
    response = _big_json(stubbed)
    response.headers["Content-Encoding"] = "identity"
    body = response.get_data()

    response = _hook(stubbed, response)

    assert response.headers["Content-Encoding"] == "identity"
    assert response.get_data() == body


def test_streamed_responses_are_left_alone(stubbed):
    payload = json.dumps({"rows": ["s" * 64] * 64}).encode()
    response = stubbed.Response((chunk for chunk in [payload]), mimetype="application/json")

    response = _hook(stubbed, response)

    assert "Content-Encoding" not in response.headers
    assert b"".join(response.response) == payload


def test_direct_passthrough_responses_are_left_alone(stubbed):
    response = _big_json(stubbed)
    body = response.get_data()
    response.direct_passthrough = True

    response = _hook(stubbed, response)

    assert "Content-Encoding" not in response.headers
    assert response.get_data() == body


def test_host_routes_outside_the_blueprint_are_not_compressed(stubbed):
    response = _app(stubbed).test_client().get("/host/large", headers=GZIP)

    assert response.status_code == 200
    assert len(response.get_data()) >= 1024
    _assert_plain(response)


def test_health_advertises_transport_gzip(stubbed):
    response = _app(stubbed).test_client().get(f"{PREFIX}/api/health")

    assert response.get_json()["capabilities"]["transport"] == {"gzip": True}


# --- end to end on the production schema ------------------------------------------------


def _distinct_edge(template, track_id, revision, rng):
    """The template with its per-bin blobs shuffled, so rows differ like real tracks."""
    edge = json.loads(json.dumps(template))
    edge.update(track_id=track_id, media_revision=revision)
    for window in ("head", "tail"):
        for key, value in edge[window].items():
            if key.endswith(("_cdb", "_q15")) and isinstance(value, str):
                raw = base64.b64decode(value)
                cells = [raw[i:i + 2] for i in range(0, len(raw), 2)]
                rng.shuffle(cells)
                edge[window][key] = base64.b64encode(b"".join(cells)).decode("ascii")
    return edge


def _seed_edge_library(db, count, distinct):
    from psycopg2.extras import execute_values
    from plugins.LumaeAnalysis import catalog_enrichment as enrichment
    from plugins.LumaeAnalysis.edge_profiles import opaque_revision

    template = json.loads(EDGE_TEMPLATE.read_text())
    rng = random.Random(14)
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type,
                 server_name, is_default, rebind_status)
                VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')""",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_state
                (catalog_instance_id, current_core_server_id, provider_type,
                 published_generation, catalog_epoch, status)
                VALUES (%s, %s, 'navidrome', 1, 'epoch-a', 'complete')""",
            (SOURCE, SERVER),
        )
        rows, edges = [], []
        for i in range(count):
            track_id, signature = f"track-{i:07d}", f"sig-{i}"
            revision = opaque_revision(signature)
            edge = (_distinct_edge(template, track_id, revision, rng) if distinct
                    else dict(template, track_id=track_id, media_revision=revision))
            rows.append((SOURCE, track_id, 44100, 240000, -14.0, bytes(rng.randrange(256) for _ in range(45)),
                         bytes(rng.randrange(256) for _ in range(45)), 1, 1, signature))
            edges.append((SOURCE, track_id, revision, edge["representation_id"], signature,
                          f"{i:064x}", json.dumps(edge)))
        execute_values(cur, f"""INSERT INTO {P}published_source_profiles
            (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs, start_ramp,
             end_ramp, analyzer_ver, profile_schema_ver, media_signature, analyzed_at)
            VALUES %s""", rows, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())")
        execute_values(cur, f"""INSERT INTO {P}edge_profiles
            (catalog_instance_id, track_id, media_revision, representation_id,
             media_signature, profile_digest, payload) VALUES %s""", edges)
        start_seq = enrichment._profile_stream_state(cur, SOURCE, for_update=True)[1]
        for payload in enrichment._profile_rows(cur, SOURCE, "", count):
            enrichment.record_profile_change(cur, SOURCE, payload["track_id"], "ready", payload)
        epoch = enrichment._profile_stream_state(cur, SOURCE)[0]
    db.commit()
    from plugins.LumaeAnalysis.catalog import opaque_cursor
    return opaque_cursor(SOURCE, epoch, start_seq)


@pytest.fixture
def edge_library(migrated_db, monkeypatch):
    mod = load_plugin()
    import psycopg2

    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    migrated_db.rollback()
    dsn = psycopg2.extensions.make_dsn(
        os.environ["LUMAE_POSTGRES_TEST_DSN"], options=f"-c search_path={schema},public")
    monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL", dsn, raising=False)
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    return mod, migrated_db


def _pair(client, method, path, **kwargs):
    plain = getattr(client, method)(PREFIX + path, **kwargs)
    packed = getattr(client, method)(PREFIX + path, headers=GZIP, **kwargs)
    return plain, packed


@pytest.mark.parametrize("distinct", [False, True], ids=["template-edges", "distinct-edges"])
def test_every_profile_route_gzips_real_edge_pages(edge_library, distinct, capsys):
    mod, db = edge_library
    cursor = _seed_edge_library(db, 50, distinct)
    client = _app(mod).test_client()
    body = {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": mod.profile_bootstrap.TRANSFER_CONTRACT,
            "catalog_instance_id": SOURCE}

    created = client.post(PREFIX + "/api/profiles/bootstrap/sessions",
                          json=dict(body, page_size=50), headers=GZIP)
    assert created.status_code == 200, created.get_data()[:200]
    token = _decoded(created)["session_token"]
    page_body = dict(body, session_token=token)
    ids = ",".join(f"track-{i:07d}" for i in range(50))
    results = {
        "v2 page": _pair(client, "post", "/api/profiles/bootstrap/sessions/page", json=page_body),
        "legacy bootstrap": _pair(
            client, "get", f"/api/profiles/bootstrap?catalog_instance_id={SOURCE}&limit=50"),
        "changes": _pair(client, "get", f"/api/profiles/changes?cursor={cursor}&limit=50"),
        "direct": _pair(client, "get", f"/api/profiles?ids={ids}"),
    }

    for name, (plain, packed) in results.items():
        assert plain.status_code == 200, (name, plain.get_data()[:200])
        _assert_plain(plain)
        _assert_gzipped(packed)
        decoded = _decoded(packed)
        assert decoded == plain.get_json(), name
        rows = decoded.get("profiles") or [c["payload"] for c in decoded["changes"]]
        assert len(rows) == 50 and all("edge_profile" in row for row in rows), name
        with capsys.disabled():
            print(f"\nP1-4 {'distinct' if distinct else 'template'} 50-row {name}: "
                  f"{len(plain.get_data())} B -> {len(packed.get_data())} B "
                  f"({len(plain.get_data()) / len(packed.get_data()):.2f}x)")
        assert len(packed.get_data()) * 1.5 < len(plain.get_data()), name
