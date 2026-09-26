"""Disposable PostgreSQL 17 acceptance checks for durable profile bootstrap."""

import os
import hashlib
import re
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from flask import Flask, request

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, load_plugin, plugin_api_module
from plugins.LumaeAnalysis import catalog_enrichment, profile_bootstrap


SOURCE = "catalog-a"
STATE = "plugin_lumae_analysis__profile_stream_state"
CHANGES = "plugin_lumae_analysis__profile_changes"
PUBLISHED = "plugin_lumae_analysis__published_source_profiles"
SESSIONS = "plugin_lumae_analysis__profile_bootstrap_sessions"
@pytest.fixture(autouse=True)
def host_api_for_disposable_schema(request, monkeypatch):
    """The public host config points the owned connection at disposable tables."""
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


def body(**updates):
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": profile_bootstrap.TRANSFER_CONTRACT,
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
    created = profile_bootstrap.create_session(body(page_size=1))
    assert created["transfer_contract"] == "source_scoped_v1"
    token = created["session_token"]
    assert created["total_profiles"] == 1
    assert created["snapshot_seq"] == 0
    assert created["catalog_epoch"] == "epoch-a"
    assert created["profile_epoch"]
    assert created["snapshot_cursor"] == created["cursor"]
    assert created["expires_at"].endswith("Z")
    first = profile_bootstrap.snapshot_page(body(session_token=token))
    for key in ("catalog_epoch", "profile_epoch", "snapshot_cursor", "expires_at"):
        assert first[key] == created[key]
    assert [p["track_id"] for p in first["profiles"]] == ["track-a"]
    assert first["has_more"] is False
    assert profile_bootstrap.snapshot_page(body(session_token=token)) == first
    other = peer(db)
    try:
        publish(other, "track-b")
        publish(other, "track-c")
        first_change = profile_bootstrap.catchup_page(body(session_token=token))
        for key in ("catalog_epoch", "profile_epoch", "snapshot_cursor", "expires_at"):
            assert first_change[key] == created[key]
        assert [e["seq"] for e in first_change["changes"]] == [1]
        assert first_change["has_more"]
        publish(other, "track-d")
        second = profile_bootstrap.catchup_page(
            body(session_token=token, page_token=first_change["next_page_token"]))
        assert [e["seq"] for e in second["changes"]] == [2]
        assert second["cursor"] == second["head_cursor"] == first_change["head_cursor"]
        assert profile_bootstrap.catchup_page(body(session_token=token)) == first_change
        with other.cursor() as cur:
            cur.execute(f"DELETE FROM {CHANGES} WHERE catalog_instance_id=%s AND seq<=2",
                        (SOURCE,))
            cur.execute(f"UPDATE {STATE} SET floor_seq=2 WHERE catalog_instance_id=%s",
                        (SOURCE,))
        other.commit()
        assert profile_bootstrap.catchup_page(body(session_token=token)) == first_change
        assert profile_bootstrap.snapshot_page(body(session_token=token)) == first
    finally:
        other.close()


def test_floor_expiry_release_identity_and_tokens(edge_publication_db):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body())
    token = created["session_token"]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=token,
                                             catalog_instance_id="catalog-b"))
    assert (exc.value.code, exc.value.status) == ("bootstrap_required", 410)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=token,
                                                page_token="bad"))
    assert exc.value.status == 400
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=token,
                                                protocol_version=3))
    assert exc.value.status == 400
    publish(db, "track-b")
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {CHANGES} WHERE catalog_instance_id=%s AND seq=1", (SOURCE,))
        cur.execute(f"UPDATE {STATE} SET floor_seq=1 WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.catchup_page(body(session_token=token))
    assert exc.value.status == 410
    assert profile_bootstrap.release_session(body(session_token=token))["released"]
    assert profile_bootstrap.release_session(body(session_token=token))["released"]
    expired = profile_bootstrap.create_session(body())
    with db.cursor() as cur:
        cur.execute(f"UPDATE {SESSIONS} SET expires_at=now()-interval '1 second' "
                    "WHERE token_hash=%s", (
                        hashlib.sha256(expired["session_token"].encode()).hexdigest(),))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=expired["session_token"]))
    assert exc.value.status == 410
    # P1-6: release deletes an expired session and answers 200.
    assert profile_bootstrap.release_session(
        body(session_token=expired["session_token"]))["released"] is True
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.rollback()


def test_tokens_are_opaque_unique_and_only_hashes_are_stored(edge_publication_db):
    first = profile_bootstrap.create_session(body())["session_token"]
    second = profile_bootstrap.create_session(body())["session_token"]
    assert first != second
    assert re.fullmatch(r"[0-9a-f]{64}", first)
    assert re.fullmatch(r"[0-9a-f]{64}", second)
    with edge_publication_db.cursor() as cur:
        cur.execute(f"SELECT token_hash FROM {SESSIONS}")
        hashes = {row[0] for row in cur.fetchall()}
    edge_publication_db.rollback()
    assert hashes == {hashlib.sha256(value.encode()).hexdigest() for value in (first, second)}
    assert first not in hashes and second not in hashes


def test_contract_and_catalog_epoch_changes_fail_safely(edge_publication_db):
    created = profile_bootstrap.create_session(body())
    token = created["session_token"]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=token,
                                             transfer_contract="account_bound_v1"))
    assert (exc.value.code, exc.value.status) == ("invalid_profile_bootstrap", 400)
    with edge_publication_db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__catalog_state SET catalog_epoch='new-epoch' "
                    "WHERE catalog_instance_id=%s", (SOURCE,))
    edge_publication_db.commit()
    for operation in (profile_bootstrap.snapshot_page, profile_bootstrap.catchup_page):
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(body(session_token=token))
        assert (exc.value.code, exc.value.status) == ("bootstrap_required", 410)
    # P1-6: release of a stale session deletes it and answers 200.
    assert profile_bootstrap.release_session(body(session_token=token))["released"] is True
    with edge_publication_db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    edge_publication_db.rollback()


def test_empty_snapshot_and_limit_rollback(edge_publication_db, monkeypatch):
    db = edge_publication_db
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {PUBLISHED} WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    created = profile_bootstrap.create_session(body())
    page = profile_bootstrap.snapshot_page(body(session_token=created["session_token"]))
    assert page["profiles"] == [] and not page["has_more"]
    publish(db, "track-b")
    monkeypatch.setattr(profile_bootstrap, "MAX_SNAPSHOT_ROWS", 0)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert exc.value.status == 413
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 1
    db.rollback()


def test_host_admitted_bearer_and_account_use_the_same_source_session(edge_publication_db):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)

    @app.before_request
    def simulated_host_barrier():
        if request.headers.get("X-Test-Host-Auth") not in ("bearer", "account"):
            return {"error": "authentication_required"}, 401

    with app.test_client() as client:
        unauthorized = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert unauthorized.status_code == 401
        bearer = client.post("/api/profiles/bootstrap/sessions", json=body(),
                             headers={"X-Test-Host-Auth": "bearer"})
        assert bearer.status_code == 200
        token = bearer.json["session_token"]
        page = client.post("/api/profiles/bootstrap/sessions/page",
                           json=body(session_token=token),
                           headers={"X-Test-Host-Auth": "account"})
        assert page.status_code == 200
        assert [p["track_id"] for p in page.json["profiles"]] == ["track-a"]


def test_duplicate_concurrent_capture_and_interruption(edge_publication_db, monkeypatch):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body(page_size=1))
    publish(db, "track-b")
    publish(db, "track-c")
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(profile_bootstrap.catchup_page,
                               body(session_token=created["session_token"]))
                   for _ in range(2)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0] == results[1]
    assert [event["seq"] for event in results[0]["changes"]] == [1]
    before = profile_bootstrap.create_session(body())
    publish(db, "track-d")
    monkeypatch.setattr(profile_bootstrap, "CATCHUP_RETENTION_MULTIPLIER", 0)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.catchup_page(body(session_token=before["session_token"]))
    assert exc.value.status == 413
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {SESSIONS} WHERE token_hash=%s", (
            hashlib.sha256(before["session_token"].encode()).hexdigest(),))
        assert cur.fetchone()[0] is None
    db.rollback()
    monkeypatch.setattr(profile_bootstrap, "CATCHUP_RETENTION_MULTIPLIER",
                        profile_bootstrap.MAX_HELD_RETENTION_MULTIPLIER)
    assert [event["seq"] for event in profile_bootstrap.catchup_page(
        body(session_token=before["session_token"]))["changes"]] == [3]


def test_session_limits_epoch_and_rollback(edge_publication_db, monkeypatch):
    db = edge_publication_db
    tokens = [profile_bootstrap.create_session(body())["session_token"]
              for _ in range(4)]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert (exc.value.code, exc.value.status) == ("bootstrap_session_limit", 429)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {STATE} SET epoch='next-epoch' WHERE catalog_instance_id=%s",
                    (SOURCE,))
    db.commit()
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=tokens[0]))
    assert exc.value.status == 410
    # P1-6: releasing the stale sessions frees their slots (no manual cleanup).
    for token in tokens:
        assert profile_bootstrap.release_session(body(session_token=token))["released"] is True
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.commit()
    old = profile_bootstrap._snapshot_batch

    def interrupted(*args):
        raise RuntimeError("injected interruption")

    monkeypatch.setattr(profile_bootstrap, "_snapshot_batch", interrupted)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert (exc.value.code, exc.value.status) == ("bootstrap_unavailable", 503)
    monkeypatch.setattr(profile_bootstrap, "_snapshot_batch", old)
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.rollback()


def test_snapshot_and_cursor_share_one_mvcc_view(edge_publication_db, monkeypatch):
    db = edge_publication_db
    other = peer(db)
    original = profile_bootstrap._snapshot_batch
    published = False

    def concurrent_publication(*args):
        nonlocal published
        if not published:
            published = True
            publish(other, "track-b")
        return original(*args)

    monkeypatch.setattr(profile_bootstrap, "_snapshot_batch", concurrent_publication)
    try:
        created = profile_bootstrap.create_session(body())
        assert published
        assert created["snapshot_count"] == 1
        page = profile_bootstrap.snapshot_page(
            body(session_token=created["session_token"]))
        assert [row["track_id"] for row in page["profiles"]] == ["track-a"]
        catchup = profile_bootstrap.catchup_page(
            body(session_token=created["session_token"]))
        assert [(event["seq"], event["track_id"]) for event in catchup["changes"]] == [
            (1, "track-b")]
    finally:
        other.close()


def test_snapshot_page_release_race_holds_session_row(edge_publication_db, monkeypatch):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body())
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
            body(session_token=created["session_token"]))
        assert blocked
        assert [row["track_id"] for row in page["profiles"]] == ["track-a"]
    finally:
        other.close()


def test_malformed_signature_and_changed_page_size_are_400(edge_publication_db):
    db = edge_publication_db
    created = profile_bootstrap.create_session(body())
    token = created["session_token"]
    tampered = created["next_page_token"].split(".")[0] + "." + "é" * 64
    for operation in (profile_bootstrap.snapshot_page, profile_bootstrap.catchup_page):
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(body(session_token=token, page_token=tampered))
        assert (exc.value.code, exc.value.status) == ("invalid_profile_bootstrap", 400)
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(body(session_token=token, page_size=50))
        assert exc.value.status == 400


def test_owned_connection_uses_public_finite_limits_and_ignores_request_db(
        edge_publication_db, monkeypatch):
    db = edge_publication_db
    actual_open = psycopg2.connect
    observed = {}

    def open_owned(dsn, **options):
        observed.update({"dsn": dsn, **options})
        return actual_open(dsn, **options)

    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect", open_owned)
    created = profile_bootstrap.create_session(body())
    page = profile_bootstrap.snapshot_page(
        body(session_token=created["session_token"]))
    assert page["profiles"]
    assert observed == {"dsn": plugin_api_module.config.DATABASE_URL, "connect_timeout": 5,
                        "application_name": "lumae-profile-bootstrap",
                        "keepalives": 1, "keepalives_idle": 30}
    with profile_bootstrap._connection(repeatable=True) as owned:
        with owned.cursor() as cur:
            cur.execute("SELECT pg_backend_pid(), current_setting('statement_timeout'), "
                        "current_setting('lock_timeout'), current_setting('transaction_isolation')")
            pid, statement, lock, isolation = cur.fetchone()
    with db.cursor() as cur:
        cur.execute("SELECT pg_backend_pid()")
        assert pid != cur.fetchone()[0]
    assert (statement, lock, isolation) == ("20s", "5s", "repeatable read")
    db.rollback()


def test_internal_serialization_error_has_safe_route_response(edge_publication_db, monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)

    def broken(*args):
        raise ValueError("sensitive internal serializer detail")

    monkeypatch.setattr(profile_bootstrap, "_snapshot_batch", broken)
    with app.test_client() as client:
        response = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert response.status_code == 503
        assert response.json == {"error": "bootstrap_unavailable", "message": "bootstrap_unavailable"}


def test_cross_source_token_rejected_on_every_route(edge_publication_db):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    with app.test_client() as client:
        created_response = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert created_response.status_code == 200
        created = created_response.json
        token = created["session_token"]
        page_request = body(session_token=token)
        first = client.post("/api/profiles/bootstrap/sessions/page", json=page_request)
        assert first.status_code == 200
        assert client.post("/api/profiles/bootstrap/sessions/page", json=page_request).json == first.json
        assert client.post("/api/profiles/bootstrap/sessions/catchup", json=page_request).status_code == 200
        for suffix in ("page", "catchup"):
            response = client.post(f"/api/profiles/bootstrap/sessions/{suffix}",
                                   json=body(session_token=token, catalog_instance_id="catalog-b"))
            assert response.status_code == 410
        # P1-6: release answers 200 but deletes only a session of the named source.
        response = client.post("/api/profiles/bootstrap/sessions/release",
                               json=body(session_token=token, catalog_instance_id="catalog-b"))
        assert (response.status_code, response.json["released"]) == (200, True)
        assert client.post("/api/profiles/bootstrap/sessions/page", json=page_request).json == first.json
        assert client.post("/api/profiles/bootstrap/sessions/release", json=page_request).status_code == 200
        assert client.post("/api/profiles/bootstrap/sessions/page", json=page_request).status_code == 410


def test_missing_public_database_url_fails_closed_while_legacy_endpoint_imports(monkeypatch):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    monkeypatch.delattr(plugin_api_module.config, "DATABASE_URL", raising=False)
    with app.test_client() as client:
        response = client.post("/api/profiles/bootstrap/sessions", json=body())
        assert response.status_code == 503
        assert response.json["message"] == "bootstrap_unavailable"
        assert client.get("/api/profiles/bootstrap").status_code != 404


def test_provisional_sessions_invalidated_by_idempotent_migration(edge_publication_db):
    db = edge_publication_db
    with db.cursor() as cur:
        cur.execute(f"ALTER TABLE {SESSIONS} DROP COLUMN transfer_contract_version")
        cur.execute(f"ALTER TABLE {SESSIONS} DROP COLUMN source_scope")
        cur.execute(f"ALTER TABLE {SESSIONS} ADD COLUMN principal TEXT NOT NULL DEFAULT 'user:alice'")
        cur.execute(f"ALTER TABLE {SESSIONS} ADD COLUMN principal_contract_version INTEGER "
                    "NOT NULL DEFAULT 2")
        cur.execute(f"INSERT INTO {SESSIONS} (session_id, token_hash, signing_secret, "
                    "catalog_instance_id, core_server_id, catalog_epoch, profile_epoch, "
                    "schema_version, page_size, snapshot_seq, snapshot_count, expires_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,1,250,0,0,now()+interval '1 hour')",
                    (str(uuid.uuid4()), "legacy-token", "secret", SOURCE, "server-a", "epoch-a",
                     "old-profile-epoch"))
    db.commit()
    catalog_enrichment.migrate_enrichment(db)
    catalog_enrichment.migrate_enrichment(db)
    db.commit()
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.rollback()
    assert profile_bootstrap.create_session(body())["transfer_contract"] == "source_scoped_v1"


def test_owned_backend_lock_cleanup_and_request_transaction(edge_publication_db, monkeypatch):
    """The capture holds its source's lock on the owned backend and unlocks it
    explicitly before create returns.

    Before P1-6 the capture held the global lock (110094, 10) as a session
    lock that only the owned backend's exit released. That exit is
    asynchronous to the client's close(), so under heavy load the check right
    after create could still find the lock held (an intermittent failure).
    Admission now takes the global lock per transaction (released by COMMIT)
    and the per-source lock is unlocked synchronously, so both checks below
    are deterministic."""
    db = edge_publication_db
    observer = peer(db)
    with db.cursor() as cur:
        cur.execute("SELECT pg_backend_pid(), txid_current()")
        request_pid, request_txid = cur.fetchone()
    owned_pid = []
    original = profile_bootstrap._snapshot_batch

    def inspect_capture(*args):
        with observer.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(110094, hashtext(%s))", (SOURCE,))
            assert cur.fetchone()[0] is False
            cur.execute("SELECT pg_try_advisory_lock(110094, 10)")
            assert cur.fetchone()[0] is True
            cur.execute("SELECT pg_advisory_unlock(110094, 10)")
            cur.execute("SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted "
                        "AND database=(SELECT oid FROM pg_database "
                        "WHERE datname=current_database()) "
                        "AND classid=110094 AND objid=hashtext(%s)::oid AND objsubid=2",
                        (SOURCE,))
            owned_pid.append(cur.fetchone()[0])
        observer.rollback()
        return original(*args)

    monkeypatch.setattr(profile_bootstrap, "_snapshot_batch", inspect_capture)
    try:
        profile_bootstrap.create_session(body())
        assert owned_pid and owned_pid[0] != request_pid
        with db.cursor() as cur:
            cur.execute("SELECT pg_backend_pid(), txid_current()")
            assert cur.fetchone() == (request_pid, request_txid)
        with observer.cursor() as cur:
            for key in ("hashtext(%s)", "10"):
                params = (SOURCE,) if "%s" in key else ()
                cur.execute(f"SELECT pg_try_advisory_lock(110094, {key})", params)
                assert cur.fetchone()[0] is True
                cur.execute(f"SELECT pg_advisory_unlock(110094, {key})", params)
        observer.commit()
    finally:
        observer.close()


def test_owned_connection_rolls_back_and_closes_on_capture_error(edge_publication_db, monkeypatch):
    db = edge_publication_db
    actual_open = psycopg2.connect
    leases = []

    def track_open(dsn, **options):
        lease = actual_open(dsn, **options)
        leases.append(lease)
        return lease

    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect", track_open)
    monkeypatch.setattr(profile_bootstrap, "_snapshot_batch",
                        lambda *_args: (_ for _ in ()).throw(RuntimeError("capture failed")))
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert (exc.value.code, exc.value.status) == ("bootstrap_unavailable", 503)
    assert leases and leases[0].closed
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 0
    db.rollback()


@pytest.mark.parametrize("failure", ["connection refused", "connection timed out"])
def test_owned_lease_failure_maps_to_safe_503(edge_publication_db, monkeypatch, failure):
    def unavailable(*_args, **_options):
        raise psycopg2.OperationalError(failure)

    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect", unavailable)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert (exc.value.code, exc.value.status) == ("bootstrap_unavailable", 503)


def test_statement_and_lock_timeouts_are_enforced(edge_publication_db, monkeypatch):
    monkeypatch.setattr(profile_bootstrap, "STATEMENT_TIMEOUT_MS", 50)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        with profile_bootstrap._connection() as owned:
            with owned.cursor() as cur:
                cur.execute("SELECT pg_sleep(1)")
    assert (exc.value.code, exc.value.status) == ("bootstrap_unavailable", 503)

    created = profile_bootstrap.create_session(body())
    monkeypatch.setattr(profile_bootstrap, "LOCK_TIMEOUT_MS", 50)
    with edge_publication_db.cursor() as cur:
        cur.execute(f"SELECT session_id FROM {SESSIONS} WHERE token_hash=%s FOR UPDATE",
                    (hashlib.sha256(created["session_token"].encode()).hexdigest(),))
    try:
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            profile_bootstrap.snapshot_page(body(session_token=created["session_token"]))
        assert (exc.value.code, exc.value.status) == ("bootstrap_unavailable", 503)
    finally:
        edge_publication_db.rollback()


def test_connection_error_hides_credentials(edge_publication_db, monkeypatch):
    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(
                            psycopg2.OperationalError("secret-password-in-dsn")))
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert str(exc.value) == "bootstrap_unavailable"
    assert exc.value.__cause__ is None
