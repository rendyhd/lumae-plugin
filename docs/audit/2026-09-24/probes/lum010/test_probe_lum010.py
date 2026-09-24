"""Read-only audit probes for LUM-010 (run against disposable audit_lum010 DB)."""
import hashlib
import json
import os
import sys
import threading
import time
import uuid

sys.path.insert(0, "/home/user/lumae-plugin/tests/plugins")
sys.path.insert(0, "/home/user/lumae-plugin")

import pytest
import psycopg2
from psycopg2.extras import execute_values

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, load_plugin, plugin_api_module  # noqa
from plugins.LumaeAnalysis import catalog_enrichment, profile_bootstrap
from plugins.LumaeAnalysis.edge_profiles import opaque_revision

SOURCE = "catalog-a"
SESSIONS = "plugin_lumae_analysis__profile_bootstrap_sessions"
STATE = "plugin_lumae_analysis__profile_stream_state"
PUBLISHED = "plugin_lumae_analysis__published_source_profiles"
EDGE = "plugin_lumae_analysis__edge_profiles"
EDGE_PAYLOAD = json.load(open(os.path.join(os.path.dirname(__file__), "edge.json")))


@pytest.fixture(autouse=True)
def host_dsn(request, monkeypatch):
    if "edge_publication_db" not in request.fixturenames:
        return
    db = request.getfixturevalue("edge_publication_db")
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    db.rollback()
    dsn = psycopg2.extensions.make_dsn(
        os.environ["LUMAE_POSTGRES_TEST_DSN"], options=f"-c search_path={schema},public")
    monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL", dsn, raising=False)


def body(**u):
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": profile_bootstrap.TRANSFER_CONTRACT,
            "catalog_instance_id": SOURCE, **u}


def seed(db, n, edge=False):
    rows = []
    edges = []
    for i in range(n):
        tid = f"track-{i:07d}"
        sig = f"sig-{i}"
        rows.append((SOURCE, tid, 44100, 240000, -14.0, b"\x01" * 45, b"\x02" * 45, 3, 1, sig))
        if edge:
            rev = opaque_revision(sig)
            p = dict(EDGE_PAYLOAD, track_id=tid, media_revision=rev)
            edges.append((SOURCE, tid, rev, p["representation_id"], sig, "d" * 64, json.dumps(p)))
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {PUBLISHED}")
        execute_values(cur, f"""INSERT INTO {PUBLISHED} (catalog_instance_id,track_id,sample_rate,
            duration_ms,ref_lufs,start_ramp,end_ramp,analyzer_ver,profile_schema_ver,media_signature,
            analyzed_at) VALUES %s""", rows,
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())", page_size=2000)
        if edges:
            execute_values(cur, f"""INSERT INTO {EDGE} (catalog_instance_id,track_id,media_revision,
                representation_id,media_signature,profile_digest,payload) VALUES %s""", edges,
                page_size=1000)
    db.commit()


def test_probe_edge_profiles_hit_byte_cap(edge_publication_db):
    seed(edge_publication_db, 7000, edge=True)
    t0 = time.monotonic()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    print(f"\nPROBE edge 7000 -> {exc.value.code}/{exc.value.status} after {time.monotonic()-t0:.2f}s")
    assert exc.value.status == 413


def test_probe_capture_time_94k_no_edge(edge_publication_db):
    seed(edge_publication_db, 94_000, edge=False)
    t0 = time.monotonic()
    created = profile_bootstrap.create_session(body(page_size=500))
    t1 = time.monotonic()
    print(f"\nPROBE 94k no-edge create {t1-t0:.2f}s count={created['snapshot_count']}")
    tok = created["next_page_token"]
    t2 = time.monotonic()
    page = profile_bootstrap.snapshot_page(body(session_token=created["session_token"]))
    print(f"PROBE page(500) {time.monotonic()-t2:.3f}s")


def test_probe_capture_time_6k_edge(edge_publication_db):
    seed(edge_publication_db, 6000, edge=True)
    t0 = time.monotonic()
    created = profile_bootstrap.create_session(body(page_size=500))
    print(f"\nPROBE 6000 edge create {time.monotonic()-t0:.2f}s count={created['snapshot_count']}")


def test_probe_concurrent_create_503_on_lock_timeout(edge_publication_db):
    dsn = plugin_api_module.config.DATABASE_URL
    holder = psycopg2.connect(dsn)
    with holder.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(110094, 10)")
    holder.commit()
    try:
        t0 = time.monotonic()
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            profile_bootstrap.create_session(body(catalog_instance_id=SOURCE))
        print(f"\nPROBE concurrent create -> {exc.value.code}/{exc.value.status} after {time.monotonic()-t0:.2f}s")
        assert exc.value.status == 503
    finally:
        holder.close()


def test_probe_epoch_change_locks_out_source(edge_publication_db):
    db = edge_publication_db
    tokens = [profile_bootstrap.create_session(body())["session_token"] for _ in range(4)]
    with db.cursor() as cur:
        cur.execute(f"UPDATE {STATE} SET epoch='next-epoch' WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    statuses = []
    for tok in tokens:
        try:
            profile_bootstrap.release_session(body(session_token=tok))
            statuses.append(200)
        except profile_bootstrap.BootstrapError as e:
            statuses.append(e.status)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    print(f"\nPROBE after epoch change: release={statuses} fresh create -> {exc.value.code}/{exc.value.status}")
    assert exc.value.status == 429


def test_probe_account_era_principal_not_null_without_default(edge_publication_db):
    db = edge_publication_db
    with db.cursor() as cur:
        cur.execute(f"DROP TABLE {SESSIONS} CASCADE")
        cur.execute(f"""CREATE TABLE {SESSIONS} (
            session_id UUID PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE,
            signing_secret TEXT NOT NULL, principal TEXT NOT NULL,
            principal_contract_version INTEGER NOT NULL DEFAULT 2,
            catalog_instance_id TEXT NOT NULL, core_server_id TEXT NOT NULL,
            catalog_epoch TEXT NOT NULL, profile_epoch TEXT NOT NULL,
            schema_version INTEGER NOT NULL, page_size INTEGER NOT NULL,
            snapshot_seq BIGINT NOT NULL, head_seq BIGINT, snapshot_count INTEGER NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
    db.commit()
    catalog_enrichment.migrate_enrichment(db)
    catalog_enrichment.migrate_enrichment(db)
    db.commit()
    with db.cursor() as cur:
        cur.execute("SELECT is_nullable FROM information_schema.columns WHERE table_schema=current_schema() "
                    "AND table_name=%s AND column_name='principal'", (SESSIONS,))
        print("\nPROBE principal is_nullable after migrate:", cur.fetchone())
    db.rollback()
    created = profile_bootstrap.create_session(body())
    assert created["session_token"]


def test_probe_timezone_format(edge_publication_db):
    dsn = plugin_api_module.config.DATABASE_URL
    base = psycopg2.extensions.parse_dsn(dsn)
    opts = base.get("options", "") + " -c TimeZone=Europe/Amsterdam"
    plugin_api_module.config.DATABASE_URL = psycopg2.extensions.make_dsn(dsn, options=opts)
    created = profile_bootstrap.create_session(body())
    print("\nPROBE expires_at with server TimeZone=Europe/Amsterdam:", created["expires_at"])


def test_probe_serializer_valueerror_unlogged(edge_publication_db, monkeypatch, caplog):
    mod = load_plugin()
    from flask import Flask
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    monkeypatch.setattr(profile_bootstrap, "serialize_profile",
                        lambda *a: (_ for _ in ()).throw(ValueError("bad ramp")))
    logged = []
    monkeypatch.setattr(plugin_api_module.logger, "exception", lambda *a, **k: logged.append(a), raising=False)
    monkeypatch.setattr(plugin_api_module.logger, "warning", lambda *a, **k: logged.append(a), raising=False)
    with app.test_client() as c:
        r = c.post("/api/profiles/bootstrap/sessions", json=body())
    print("\nPROBE ValueError in serializer ->", r.status_code, r.json, "logged:", logged, "caplog:", len(caplog.records))




def test_probe_expired_purge_cost(edge_publication_db):
    db = edge_publication_db
    seed(db, 94_000, edge=False)
    for _ in range(4):
        profile_bootstrap.create_session(body(page_size=500))
    with db.cursor() as cur:
        cur.execute(f"UPDATE {SESSIONS} SET expires_at=now()-interval '1 second'")
        cur.execute("SELECT count(*) FROM plugin_lumae_analysis__profile_bootstrap_snapshot")
        n = cur.fetchone()[0]
    db.commit()
    t0 = time.monotonic()
    profile_bootstrap.create_session(body(page_size=500))
    print(f"\nPROBE create incl. purge of {n} expired snapshot rows: {time.monotonic()-t0:.2f}s")
    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM plugin_lumae_analysis__profile_bootstrap_snapshot")
        print("PROBE snapshot rows after:", cur.fetchone()[0])
    db.rollback()
