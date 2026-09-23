"""Disposable PostgreSQL 17 acceptance checks for durable profile bootstrap."""

import os
import hashlib
from concurrent.futures import ThreadPoolExecutor

import pytest
from flask import Flask, g

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, load_plugin
from plugins.LumaeAnalysis import catalog_enrichment, profile_bootstrap


SOURCE = "catalog-a"
STATE = "plugin_lumae_analysis__profile_stream_state"
CHANGES = "plugin_lumae_analysis__profile_changes"
PUBLISHED = "plugin_lumae_analysis__published_source_profiles"
SESSIONS = "plugin_lumae_analysis__profile_bootstrap_sessions"


def body(**updates):
    return {"protocol_version": 2, "schema_version": 1,
            "catalog_instance_id": SOURCE, **updates}


def peer(db):
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    db.commit()
    connection = psycopg2.connect(os.environ["LUMAE_POSTGRES_TEST_DSN"])
    with connection.cursor() as cur:
        cur.execute(f"SET search_path TO {schema}, public")
    connection.commit()
    return connection


def publish(db, track, operation="upsert"):
    with db.cursor() as cur:
        if operation == "upsert":
            cur.execute(
                f"""INSERT INTO {PUBLISHED}
                    (catalog_instance_id,track_id,sample_rate,duration_ms,ref_lufs,
                     start_ramp,end_ramp,analyzer_ver,profile_schema_ver,
                     media_signature,analyzed_at)
                    VALUES (%s,%s,48000,210,-13,%s,%s,1,1,%s,now())""",
                (SOURCE, track, b"first", b"last", f"revision-{track}"))
            catalog_enrichment.record_profile_change(
                cur, SOURCE, track, "ready", {"track_id": track})
        else:
            cur.execute(f"DELETE FROM {PUBLISHED} WHERE catalog_instance_id=%s AND track_id=%s",
                        (SOURCE, track))
            catalog_enrichment.record_profile_change(cur, SOURCE, track, "deleted")
    db.commit()


def test_restart_snapshot_replay_and_finite_catchup(edge_publication_db):
    db = edge_publication_db
    created = profile_bootstrap.create_session(db, body(page_size=1), "user:alice")
    token = created["session_token"]
    assert created["total_profiles"] == 1
    assert created["snapshot_seq"] == 0
    assert created["catalog_epoch"] == "epoch-a"
    assert created["profile_epoch"]
    assert created["snapshot_cursor"] == created["cursor"]
    assert created["expires_at"].endswith("Z")
    first = profile_bootstrap.snapshot_page(db, body(session_token=token), "user:alice")
    for key in ("catalog_epoch", "profile_epoch", "snapshot_cursor", "expires_at"):
        assert first[key] == created[key]
    assert [p["track_id"] for p in first["profiles"]] == ["track-a"]
    assert first["has_more"] is False
    assert profile_bootstrap.snapshot_page(db, body(session_token=token), "user:alice") == first
    other = peer(db)
    try:
        publish(other, "track-b")
        publish(other, "track-c")
        first_change = profile_bootstrap.catchup_page(db, body(session_token=token), "user:alice")
        for key in ("catalog_epoch", "profile_epoch", "snapshot_cursor", "expires_at"):
            assert first_change[key] == created[key]
        assert [e["seq"] for e in first_change["changes"]] == [1]
        assert first_change["has_more"]
        publish(other, "track-d")
        second = profile_bootstrap.catchup_page(
            db, body(session_token=token, page_token=first_change["next_page_token"]),
            "user:alice")
        assert [e["seq"] for e in second["changes"]] == [2]
        assert second["cursor"] == second["head_cursor"] == first_change["head_cursor"]
        assert profile_bootstrap.catchup_page(db, body(session_token=token), "user:alice") == first_change
        with other.cursor() as cur:
            cur.execute(f"DELETE FROM {CHANGES} WHERE catalog_instance_id=%s AND seq<=2",
                        (SOURCE,))
            cur.execute(f"UPDATE {STATE} SET floor_seq=2 WHERE catalog_instance_id=%s",
                        (SOURCE,))
        other.commit()
        assert profile_bootstrap.catchup_page(db, body(session_token=token), "user:alice") == first_change
        assert profile_bootstrap.snapshot_page(db, body(session_token=token), "user:alice") == first
    finally:
        other.close()


def test_floor_expiry_release_identity_and_tokens(edge_publication_db):
    db = edge_publication_db
    created = profile_bootstrap.create_session(db, body(), "user:alice")
    token = created["session_token"]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(db, body(session_token=token), "user:bob")
    assert (exc.value.code, exc.value.status) == ("bootstrap_required", 410)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(db, body(session_token=token,
                                                page_token="bad"), "user:alice")
    assert exc.value.status == 400
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(db, body(session_token=token,
                                                protocol_version=3), "user:alice")
    assert exc.value.status == 400
    publish(db, "track-b")
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {CHANGES} WHERE catalog_instance_id=%s AND seq=1", (SOURCE,))
        cur.execute(f"UPDATE {STATE} SET floor_seq=1 WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.catchup_page(db, body(session_token=token), "user:alice")
    assert exc.value.status == 410
    assert profile_bootstrap.release_session(db, body(session_token=token), "user:alice")["released"]
    assert profile_bootstrap.release_session(db, body(session_token=token), "user:alice")["released"]
    expired = profile_bootstrap.create_session(db, body(), "user:alice")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {SESSIONS} SET expires_at=now()-interval '1 second' "
                    "WHERE token_hash=%s", (
                        hashlib.sha256(expired["session_token"].encode()).hexdigest(),))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(db, body(session_token=expired["session_token"]), "user:alice")
    assert exc.value.status == 410


def test_empty_snapshot_and_limit_rollback(edge_publication_db, monkeypatch):
    db = edge_publication_db
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {PUBLISHED} WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    created = profile_bootstrap.create_session(db, body(), "user:alice")
    page = profile_bootstrap.snapshot_page(db, body(session_token=created["session_token"]), "user:alice")
    assert page["profiles"] == [] and not page["has_more"]
    publish(db, "track-b")
    monkeypatch.setattr(profile_bootstrap, "MAX_SNAPSHOT_ROWS", 0)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(db, body(), "user:alice")
    assert exc.value.status == 413
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 1
    db.rollback()


def test_routes_require_authenticated_user(edge_publication_db):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    with app.test_client() as client:
        response = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert response.status_code == 401
        assert response.json["error"] == "authentication_required"
        assert response.headers["Cache-Control"] == "private, no-store"


def test_bearer_admin_without_account_fails_before_database_access(monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)

    @app.before_request
    def bearer_admin():
        g.auth_method = "bearer"
        g.auth_role = "admin"
        g.auth_user = None

    def unexpected_access(*args, **kwargs):
        pytest.fail("unauthenticated bearer request reached bootstrap storage")

    monkeypatch.setattr(mod, "get_db", unexpected_access)
    monkeypatch.setattr(profile_bootstrap, "create_session", unexpected_access)
    with app.test_client() as client:
        response = client.post("/api/profiles/bootstrap/sessions", json=body())
    assert response.status_code == 401
    assert response.json["error"] == "authentication_required"
    assert response.headers["Cache-Control"] == "private, no-store"


def test_duplicate_concurrent_capture_and_interruption(edge_publication_db, monkeypatch):
    db = edge_publication_db
    created = profile_bootstrap.create_session(db, body(page_size=1), "user:alice")
    publish(db, "track-b")
    publish(db, "track-c")
    first = peer(db)
    second = peer(db)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(profile_bootstrap.catchup_page, connection,
                                   body(session_token=created["session_token"]), "user:alice")
                       for connection in (first, second)]
            results = [future.result(timeout=10) for future in futures]
        assert results[0] == results[1]
        assert [event["seq"] for event in results[0]["changes"]] == [1]
    finally:
        first.close()
        second.close()
    before = profile_bootstrap.create_session(db, body(), "user:alice")
    publish(db, "track-d")
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_EVENTS", 0)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.catchup_page(db, body(session_token=before["session_token"]),
                                       "user:alice")
    assert exc.value.status == 413
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {SESSIONS} WHERE token_hash=%s", (
            hashlib.sha256(before["session_token"].encode()).hexdigest(),))
        assert cur.fetchone()[0] is None
    db.rollback()
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_EVENTS", 50_000)
    assert [event["seq"] for event in profile_bootstrap.catchup_page(
        db, body(session_token=before["session_token"]), "user:alice")["changes"]] == [3]


def test_session_limits_epoch_and_rollback(edge_publication_db, monkeypatch):
    db = edge_publication_db
    tokens = [profile_bootstrap.create_session(db, body(), "user:alice")["session_token"]
              for _ in range(4)]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(db, body(), "user:alice")
    assert (exc.value.code, exc.value.status) == ("bootstrap_session_limit", 429)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {STATE} SET epoch='next-epoch' WHERE catalog_instance_id=%s",
                    (SOURCE,))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(db, body(session_token=tokens[0]), "user:alice")
    assert exc.value.status == 410
    for token in tokens:
        profile_bootstrap.release_session(db, body(session_token=token), "user:alice")
    old = profile_bootstrap.serialize_profile

    def interrupted(*args):
        raise RuntimeError("injected interruption")

    monkeypatch.setattr(profile_bootstrap, "serialize_profile", interrupted)
    with pytest.raises(RuntimeError, match="injected interruption"):
        profile_bootstrap.create_session(db, body(), "user:alice")
    monkeypatch.setattr(profile_bootstrap, "serialize_profile", old)
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.rollback()


def test_snapshot_and_cursor_share_one_mvcc_view(edge_publication_db, monkeypatch):
    db = edge_publication_db
    other = peer(db)
    original = profile_bootstrap.serialize_profile
    published = False

    def concurrent_publication(*args):
        nonlocal published
        if not published:
            published = True
            publish(other, "track-b")
        return original(*args)

    monkeypatch.setattr(profile_bootstrap, "serialize_profile", concurrent_publication)
    try:
        created = profile_bootstrap.create_session(db, body(), "user:alice")
        assert published
        assert created["snapshot_count"] == 1
        page = profile_bootstrap.snapshot_page(
            db, body(session_token=created["session_token"]), "user:alice")
        assert [row["track_id"] for row in page["profiles"]] == ["track-a"]
        catchup = profile_bootstrap.catchup_page(
            db, body(session_token=created["session_token"]), "user:alice")
        assert [(event["seq"], event["track_id"]) for event in catchup["changes"]] == [
            (1, "track-b")]
    finally:
        other.close()


def test_snapshot_page_release_race_holds_session_row(edge_publication_db, monkeypatch):
    db = edge_publication_db
    created = profile_bootstrap.create_session(db, body(), "user:alice")
    other = peer(db)
    original = profile_bootstrap._ordinal
    blocked = False

    def release_during_validation(session, phase, token):
        nonlocal blocked
        try:
            with other.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '200ms'")
                cur.execute(f"DELETE FROM {SESSIONS} WHERE token_hash=%s", (
                    hashlib.sha256(created["session_token"].encode()).hexdigest(),))
            other.commit()
        except psycopg2.errors.LockNotAvailable:
            blocked = True
            other.rollback()
        return original(session, phase, token)

    monkeypatch.setattr(profile_bootstrap, "_ordinal", release_during_validation)
    try:
        page = profile_bootstrap.snapshot_page(
            db, body(session_token=created["session_token"]), "user:alice")
        assert blocked
        assert [row["track_id"] for row in page["profiles"]] == ["track-a"]
    finally:
        other.close()


def test_malformed_signature_and_changed_page_size_are_400(edge_publication_db):
    db = edge_publication_db
    created = profile_bootstrap.create_session(db, body(), "user:alice")
    token = created["session_token"]
    tampered = created["next_page_token"].split(".")[0] + "." + "é" * 64
    for operation in (profile_bootstrap.snapshot_page, profile_bootstrap.catchup_page):
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(db, body(session_token=token, page_token=tampered), "user:alice")
        assert (exc.value.code, exc.value.status) == ("invalid_profile_bootstrap", 400)
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(db, body(session_token=token, page_size=50), "user:alice")
        assert exc.value.status == 400


@pytest.mark.parametrize("inherited_timeout", [None, 0])
def test_owned_connection_forces_finite_connect_timeout(
        edge_publication_db, monkeypatch, inherited_timeout):
    db = edge_publication_db
    created = profile_bootstrap.create_session(db, body(), "user:alice")
    real_connect = psycopg2.connect
    observed = []

    class HostConnection:
        info = db.info

        def cursor(self):
            return db.cursor()

        def get_dsn_parameters(self):
            parameters = db.get_dsn_parameters()
            if inherited_timeout is not None:
                parameters["connect_timeout"] = inherited_timeout
            return parameters

    def connect(**parameters):
        observed.append(parameters["connect_timeout"])
        return real_connect(**parameters)

    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect", connect)
    page = profile_bootstrap.snapshot_page(
        HostConnection(), body(session_token=created["session_token"]), "user:alice")
    assert page["profiles"]
    assert observed == [5]


def test_internal_serialization_error_has_safe_route_response(edge_publication_db, monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)

    @app.before_request
    def authenticate():
        g.auth_user = "alice"

    def broken(*args):
        raise ValueError("sensitive internal serializer detail")

    monkeypatch.setattr(profile_bootstrap, "serialize_profile", broken)
    with app.test_client() as client:
        response = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert response.status_code == 503
        assert response.json == {"error": "bootstrap_unavailable", "message": "bootstrap_unavailable"}
