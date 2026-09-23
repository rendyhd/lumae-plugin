"""Disposable PostgreSQL 17 acceptance checks for durable profile bootstrap."""

import os
import hashlib
import uuid
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor

import pytest
from flask import Flask, g

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, load_plugin, plugin_api_module
from plugins.LumaeAnalysis import catalog_enrichment, profile_bootstrap


SOURCE = "catalog-a"
STATE = "plugin_lumae_analysis__profile_stream_state"
CHANGES = "plugin_lumae_analysis__profile_changes"
PUBLISHED = "plugin_lumae_analysis__published_source_profiles"
SESSIONS = "plugin_lumae_analysis__profile_bootstrap_sessions"
SUBJECT_A = str(uuid.uuid5(uuid.NAMESPACE_DNS, "lumae-test-alice"))
SUBJECT_B = str(uuid.uuid5(uuid.NAMESPACE_DNS, "lumae-test-bob"))
GEN_1 = str(uuid.uuid5(uuid.NAMESPACE_DNS, "lumae-test-generation-one"))
GEN_2 = str(uuid.uuid5(uuid.NAMESPACE_DNS, "lumae-test-generation-two"))


def binding(subject=SUBJECT_A, generation=GEN_1):
    return profile_bootstrap.principal_binding(SimpleNamespace(
        kind="account", subject=subject, authorization_generation=generation))


plugin_api_module.get_principal = lambda: None
plugin_api_module.open_db_connection = lambda **_kwargs: None


@pytest.fixture(autouse=True)
def host_api_for_disposable_schema(request, monkeypatch):
    """Test host lease uses the fixture schema as its configured default path."""
    if "edge_publication_db" not in request.fixturenames:
        return
    db = request.getfixturevalue("edge_publication_db")
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    db.rollback()

    def open_owned(*, isolation="read_committed", connect_timeout=30,
                   statement_timeout_ms=60000, lock_timeout_ms=5000):
        connection = psycopg2.connect(
            os.environ["LUMAE_POSTGRES_TEST_DSN"],
            connect_timeout=connect_timeout,
            options=f"-c search_path={schema},public -c statement_timeout={statement_timeout_ms} "
                    f"-c lock_timeout={lock_timeout_ms}")
        connection.set_isolation_level({"read_committed": 1, "repeatable_read": 2}[isolation])
        return connection

    monkeypatch.setattr(plugin_api_module, "open_db_connection", open_owned, raising=False)


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
    created = profile_bootstrap.create_session(body(page_size=1), binding())
    token = created["session_token"]
    assert created["total_profiles"] == 1
    assert created["snapshot_seq"] == 0
    assert created["catalog_epoch"] == "epoch-a"
    assert created["profile_epoch"]
    assert created["snapshot_cursor"] == created["cursor"]
    assert created["expires_at"].endswith("Z")
    first = profile_bootstrap.snapshot_page(body(session_token=token), binding())
    for key in ("catalog_epoch", "profile_epoch", "snapshot_cursor", "expires_at"):
        assert first[key] == created[key]
    assert [p["track_id"] for p in first["profiles"]] == ["track-a"]
    assert first["has_more"] is False
    assert profile_bootstrap.snapshot_page(body(session_token=token), binding()) == first
    other = peer(db)
    try:
        publish(other, "track-b")
        publish(other, "track-c")
        first_change = profile_bootstrap.catchup_page(body(session_token=token), binding())
        for key in ("catalog_epoch", "profile_epoch", "snapshot_cursor", "expires_at"):
            assert first_change[key] == created[key]
        assert [e["seq"] for e in first_change["changes"]] == [1]
        assert first_change["has_more"]
        publish(other, "track-d")
        second = profile_bootstrap.catchup_page(
            body(session_token=token, page_token=first_change["next_page_token"]),
            binding())
        assert [e["seq"] for e in second["changes"]] == [2]
        assert second["cursor"] == second["head_cursor"] == first_change["head_cursor"]
        assert profile_bootstrap.catchup_page(body(session_token=token), binding()) == first_change
        with other.cursor() as cur:
            cur.execute(f"DELETE FROM {CHANGES} WHERE catalog_instance_id=%s AND seq<=2",
                        (SOURCE,))
            cur.execute(f"UPDATE {STATE} SET floor_seq=2 WHERE catalog_instance_id=%s",
                        (SOURCE,))
        other.commit()
        assert profile_bootstrap.catchup_page(body(session_token=token), binding()) == first_change
        assert profile_bootstrap.snapshot_page(body(session_token=token), binding()) == first
    finally:
        other.close()


def test_floor_expiry_release_identity_and_tokens(edge_publication_db):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body(), binding())
    token = created["session_token"]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=token), binding(SUBJECT_B))
    assert (exc.value.code, exc.value.status) == ("bootstrap_required", 410)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=token,
                                                page_token="bad"), binding())
    assert exc.value.status == 400
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=token,
                                                protocol_version=3), binding())
    assert exc.value.status == 400
    publish(db, "track-b")
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {CHANGES} WHERE catalog_instance_id=%s AND seq=1", (SOURCE,))
        cur.execute(f"UPDATE {STATE} SET floor_seq=1 WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.catchup_page(body(session_token=token), binding())
    assert exc.value.status == 410
    assert profile_bootstrap.release_session(body(session_token=token), binding())["released"]
    assert profile_bootstrap.release_session(body(session_token=token), binding())["released"]
    expired = profile_bootstrap.create_session(body(), binding())
    with db.cursor() as cur:
        cur.execute(f"UPDATE {SESSIONS} SET expires_at=now()-interval '1 second' "
                    "WHERE token_hash=%s", (
                        hashlib.sha256(expired["session_token"].encode()).hexdigest(),))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=expired["session_token"]), binding())
    assert exc.value.status == 410


def test_empty_snapshot_and_limit_rollback(edge_publication_db, monkeypatch):
    db = edge_publication_db
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {PUBLISHED} WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    created = profile_bootstrap.create_session(body(), binding())
    page = profile_bootstrap.snapshot_page(body(session_token=created["session_token"]), binding())
    assert page["profiles"] == [] and not page["has_more"]
    publish(db, "track-b")
    monkeypatch.setattr(profile_bootstrap, "MAX_SNAPSHOT_ROWS", 0)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body(), binding())
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
    created = profile_bootstrap.create_session(body(page_size=1), binding())
    publish(db, "track-b")
    publish(db, "track-c")
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(profile_bootstrap.catchup_page,
                               body(session_token=created["session_token"]), binding())
                   for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0] == results[1]
    assert [event["seq"] for event in results[0]["changes"]] == [1]
    before = profile_bootstrap.create_session(body(), binding())
    publish(db, "track-d")
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_EVENTS", 0)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.catchup_page(body(session_token=before["session_token"]),
                                       binding())
    assert exc.value.status == 413
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {SESSIONS} WHERE token_hash=%s", (
            hashlib.sha256(before["session_token"].encode()).hexdigest(),))
        assert cur.fetchone()[0] is None
    db.rollback()
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_EVENTS", 50_000)
    assert [event["seq"] for event in profile_bootstrap.catchup_page(
        body(session_token=before["session_token"]), binding())["changes"]] == [3]


def test_session_limits_epoch_and_rollback(edge_publication_db, monkeypatch):
    db = edge_publication_db
    tokens = [profile_bootstrap.create_session(body(), binding())["session_token"]
              for _ in range(4)]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body(), binding())
    assert (exc.value.code, exc.value.status) == ("bootstrap_session_limit", 429)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {STATE} SET epoch='next-epoch' WHERE catalog_instance_id=%s",
                    (SOURCE,))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=tokens[0]), binding())
    assert exc.value.status == 410
    for token in tokens:
        profile_bootstrap.release_session(body(session_token=token), binding())
    old = profile_bootstrap.serialize_profile

    def interrupted(*args):
        raise RuntimeError("injected interruption")

    monkeypatch.setattr(profile_bootstrap, "serialize_profile", interrupted)
    with pytest.raises(RuntimeError, match="injected interruption"):
        profile_bootstrap.create_session(body(), binding())
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
        created = profile_bootstrap.create_session(body(), binding())
        assert published
        assert created["snapshot_count"] == 1
        page = profile_bootstrap.snapshot_page(
            body(session_token=created["session_token"]), binding())
        assert [row["track_id"] for row in page["profiles"]] == ["track-a"]
        catchup = profile_bootstrap.catchup_page(
            body(session_token=created["session_token"]), binding())
        assert [(event["seq"], event["track_id"]) for event in catchup["changes"]] == [
            (1, "track-b")]
    finally:
        other.close()


def test_snapshot_page_release_race_holds_session_row(edge_publication_db, monkeypatch):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body(), binding())
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
            body(session_token=created["session_token"]), binding())
        assert blocked
        assert [row["track_id"] for row in page["profiles"]] == ["track-a"]
    finally:
        other.close()


def test_malformed_signature_and_changed_page_size_are_400(edge_publication_db):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body(), binding())
    token = created["session_token"]
    tampered = created["next_page_token"].split(".")[0] + "." + "é" * 64
    for operation in (profile_bootstrap.snapshot_page, profile_bootstrap.catchup_page):
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(body(session_token=token, page_token=tampered), binding())
        assert (exc.value.code, exc.value.status) == ("invalid_profile_bootstrap", 400)
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(body(session_token=token, page_size=50), binding())
        assert exc.value.status == 400


def test_owned_connection_uses_public_finite_limits_and_ignores_request_db(
        edge_publication_db, monkeypatch):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body(), binding())
    actual_open = plugin_api_module.open_db_connection
    observed = {}

    def open_owned(**options):
        observed.update(options)
        return actual_open(**options)

    monkeypatch.setattr(plugin_api_module, "open_db_connection", open_owned)
    page = profile_bootstrap.snapshot_page(
        body(session_token=created["session_token"]), binding())
    assert page["profiles"]
    assert observed == {"isolation": "read_committed", "connect_timeout": 5,
                        "statement_timeout_ms": 20_000, "lock_timeout_ms": 5_000}


def test_internal_serialization_error_has_safe_route_response(edge_publication_db, monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)

    @app.before_request
    def authenticate():
        g.auth_user = "alice"

    monkeypatch.setattr(plugin_api_module, "get_principal", lambda: SimpleNamespace(
        kind="account", subject=SUBJECT_A, authorization_generation=GEN_1))

    def broken(*args):
        raise ValueError("sensitive internal serializer detail")

    monkeypatch.setattr(profile_bootstrap, "serialize_profile", broken)
    with app.test_client() as client:
        response = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert response.status_code == 503
        assert response.json == {"error": "bootstrap_unavailable", "message": "bootstrap_unavailable"}


def test_account_route_binding_generation_recreation_and_release(edge_publication_db, monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    current = {"subject": SUBJECT_A, "generation": GEN_1}
    monkeypatch.setattr(plugin_api_module, "get_principal", lambda: SimpleNamespace(
        kind="account", subject=current["subject"],
        authorization_generation=current["generation"]))
    with app.test_client() as client:
        created = client.post("/api/profiles/bootstrap/sessions", json=body()).json
        token = created["session_token"]
        page_request = body(session_token=token)
        first = client.post("/api/profiles/bootstrap/sessions/page", json=page_request)
        assert first.status_code == 200
        assert client.post("/api/profiles/bootstrap/sessions/page", json=page_request).json == first.json
        assert client.post("/api/profiles/bootstrap/sessions/catchup", json=page_request).status_code == 200
        current["subject"] = SUBJECT_B
        assert client.post("/api/profiles/bootstrap/sessions/page", json=page_request).status_code == 410
        current["subject"] = SUBJECT_A
        current["generation"] = GEN_2
        for suffix in ("page", "catchup", "release"):
            response = client.post(f"/api/profiles/bootstrap/sessions/{suffix}", json=page_request)
            assert response.status_code == 410
        fresh = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert fresh.status_code == 200
        assert client.post("/api/profiles/bootstrap/sessions/release",
                           json=body(session_token=fresh.json["session_token"])).status_code == 200
        current["subject"] = SUBJECT_B  # Username can be recreated with a new UUID.
        assert client.post("/api/profiles/bootstrap/sessions/page", json=page_request).status_code == 410


def test_old_host_fails_closed_while_legacy_endpoint_imports(monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    for absent in ("get_principal", "open_db_connection"):
        with monkeypatch.context() as patch:
            patch.delattr(plugin_api_module, absent)
            with app.test_client() as client:
                response = client.post("/api/profiles/bootstrap/sessions", json=body())
                assert response.status_code == 503
                assert response.json["message"] == "host_api_unavailable"
                assert client.get("/api/profiles/bootstrap").status_code != 404


def test_principal_absence_and_bearer_rejected_before_lease(monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    monkeypatch.setattr(plugin_api_module, "open_db_connection",
                        lambda **_kw: pytest.fail("lease opened without account"))
    with app.test_client() as client:
        for principal in (None, SimpleNamespace(kind="installation", subject=SUBJECT_A,
                                                authorization_generation=GEN_1)):
            monkeypatch.setattr(plugin_api_module, "get_principal", lambda: principal)
            response = client.post("/api/profiles/bootstrap/sessions", json=body())
            assert response.status_code == 401


def test_provisional_sessions_invalidated_by_idempotent_migration(edge_publication_db):
    db = edge_publication_db
    with db.cursor() as cur:
        cur.execute(f"ALTER TABLE {SESSIONS} DROP COLUMN principal_contract_version")
        cur.execute(f"INSERT INTO {SESSIONS} (session_id, token_hash, signing_secret, principal, "
                    "catalog_instance_id, core_server_id, catalog_epoch, profile_epoch, "
                    "schema_version, page_size, snapshot_seq, snapshot_count, expires_at) "
                    "VALUES (%s,%s,%s,'user:alice',%s,%s,%s,%s,1,250,0,0,now()+interval '1 hour')",
                    (str(uuid.uuid4()), "legacy-token", "secret", SOURCE, "server-a", "epoch-a",
                     "old-profile-epoch"))
    db.commit()
    catalog_enrichment.migrate_enrichment(db)
    catalog_enrichment.migrate_enrichment(db)
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.rollback()


def test_owned_backend_lock_cleanup_and_request_transaction(edge_publication_db, monkeypatch):
    db = edge_publication_db
    observer = peer(db)
    with db.cursor() as cur:
        cur.execute("SELECT pg_backend_pid(), txid_current()")
        request_pid, request_txid = cur.fetchone()
    owned_pid = []
    original = profile_bootstrap.serialize_profile

    def inspect_capture(*row):
        with observer.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(110094, 10)")
            assert cur.fetchone()[0] is False
            cur.execute("SELECT pid FROM pg_locks WHERE locktype='advisory' "
                        "AND classid=110094 AND objid=10 AND granted")
            owned_pid.append(cur.fetchone()[0])
        observer.rollback()
        return original(*row)

    monkeypatch.setattr(profile_bootstrap, "serialize_profile", inspect_capture)
    try:
        profile_bootstrap.create_session(body(), binding())
        assert owned_pid and owned_pid[0] != request_pid
        with db.cursor() as cur:
            cur.execute("SELECT pg_backend_pid(), txid_current()")
            assert cur.fetchone() == (request_pid, request_txid)
        with observer.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(110094, 10)")
            assert cur.fetchone()[0] is True
            cur.execute("SELECT pg_advisory_unlock(110094, 10)")
        observer.commit()
    finally:
        observer.close()


def test_owned_connection_rolls_back_and_closes_on_capture_error(edge_publication_db, monkeypatch):
    db = edge_publication_db
    actual_open = plugin_api_module.open_db_connection
    leases = []

    def track_open(**options):
        lease = actual_open(**options)
        leases.append(lease)
        return lease

    monkeypatch.setattr(plugin_api_module, "open_db_connection", track_open)
    monkeypatch.setattr(profile_bootstrap, "serialize_profile",
                        lambda *_row: (_ for _ in ()).throw(RuntimeError("capture failed")))
    with pytest.raises(RuntimeError, match="capture failed"):
        profile_bootstrap.create_session(body(), binding())
    assert leases and leases[0].closed
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.rollback()


@pytest.mark.parametrize("failure", ["connection refused", "connection timed out"])
def test_owned_lease_failure_maps_to_safe_503(edge_publication_db, monkeypatch, failure):
    def unavailable(**_options):
        raise psycopg2.OperationalError(failure)

    monkeypatch.setattr(plugin_api_module, "open_db_connection", unavailable)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body(), binding())
    assert (exc.value.code, exc.value.status) == ("bootstrap_unavailable", 503)
