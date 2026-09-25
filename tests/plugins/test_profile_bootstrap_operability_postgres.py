"""P1-6 (AUD-11, AUD-12 server side, K3/K4/K5): v2 bootstrap operability.

Admission is a short transaction under the global advisory lock; the capture
runs under a per-source lock. Identity-stale, expired and abandoned rows never
hold a slot or the journal floor, release always deletes, a duplicate
``client_request_id`` replaces its unclaimed session, sliding sessions extend
on every page, errors are logged, 429/503 carry ``Retry-After``, timestamps are
UTC and health reports what it can actually serve. Runs on the real migrated
schema. The lum010 audit probe's lock, lockout and logging cases are inverted
here (``test_probe_*`` in ``docs/audit/2026-09-24/probes/lum010``).
"""

import datetime
import hashlib
import io
import json
import os
import threading
import time
import uuid

import pytest
from flask import Flask, g, request

psycopg2 = pytest.importorskip("psycopg2")

from werkzeug.test import EnvironBuilder, run_wsgi_app

from test_lumae_analysis import load_plugin, plugin_api_module
from plugins.LumaeAnalysis import catalog_enrichment, profile_bootstrap


SOURCE = "catalog-a"
OTHER = "catalog-b"
P = "plugin_lumae_analysis__"
SESSIONS = P + "profile_bootstrap_sessions"
STATE = P + "profile_stream_state"
CHANGES = P + "profile_changes"
PUBLISHED = P + "published_source_profiles"
ROUTE = "/api/profiles/bootstrap/sessions"
# Small enough for a fast test; compact_change_journal keeps at least this many.
RETAINED = 1_000


def _add_source(cur, source, server, *, default=False):
    cur.execute(
        f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
        "provider_type, server_name, is_default, rebind_status) "
        "VALUES (%s, %s, 'navidrome', %s, %s, 'active')", (source, server, source, default))
    cur.execute(
        f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, "
        "provider_type, published_generation, catalog_epoch, status) "
        "VALUES (%s, %s, 'navidrome', 1, 'epoch-a', 'complete')", (source, server))
    catalog_enrichment._profile_stream_state(cur, source, for_update=True)
    _profile(cur, source, f"{source}-track")


def _profile(cur, source, track):
    cur.execute(
        f"INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate, duration_ms, "
        "ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, media_signature, "
        "analyzed_at) VALUES (%s, %s, 44100, 240000, -14.5, %s, %s, 1, 1, %s, "
        "'2026-09-01 12:00:00')",
        (source, track, b"\x01\x02\x03", b"\x04\x05\x06", f"sig-{track}"))


def _schema(db):
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    db.commit()
    return schema


def _dsn(schema, extra=""):
    return psycopg2.extensions.make_dsn(
        os.environ["LUMAE_POSTGRES_TEST_DSN"],
        options=f"-c search_path={schema},public{extra}")


@pytest.fixture
def db(migrated_db, monkeypatch):
    with migrated_db.cursor() as cur:
        _add_source(cur, SOURCE, "server-a", default=True)
        _add_source(cur, OTHER, "server-b")
    migrated_db.commit()
    monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL",
                        _dsn(_schema(migrated_db)), raising=False)
    monkeypatch.setattr(profile_bootstrap, "_availability_cache", None, raising=False)
    return migrated_db


def body(**updates):
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": profile_bootstrap.TRANSFER_CONTRACT,
            "catalog_instance_id": SOURCE, **updates}


def _hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def _query(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _execute(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
    db.commit()


def _app(caller_header=False):
    mod = load_plugin()
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    if caller_header:
        @app.before_request
        def simulated_host_auth():
            user = request.headers.get("X-Test-User")
            if user == "bearer":
                g.auth_method = "bearer"
            elif user:
                g.auth_method = "session"
                g.auth_user = user
    return app


def _raiser(error):
    def raise_error(*_args, **_kwargs):
        raise error
    return raise_error


class _Blocker:
    """Hold the first capture that serializes a row of ``source`` until released."""

    def __init__(self, monkeypatch, source):
        self.capturing = threading.Event()
        self.release = threading.Event()
        self._source = source
        self._original = profile_bootstrap.serialize_profile
        monkeypatch.setattr(profile_bootstrap, "serialize_profile", self)

    def __call__(self, *row):
        if str(row[0]).startswith(self._source) and not self.capturing.is_set():
            self.capturing.set()
            assert self.release.wait(20)
        return self._original(*row)


def _in_thread(function, *args):
    outcome = {}

    def run():
        started = time.monotonic()
        try:
            outcome["result"] = function(*args)
        except BaseException as exc:  # recorded for the assertion
            outcome["error"] = exc
        outcome["elapsed"] = time.monotonic() - started

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def _outcome(outcome):
    error = outcome.get("error")
    if error is not None:
        raise AssertionError(f"create failed: {error!r}") from error
    return outcome["result"]


def _await_capture_lock_waiter(connection, source, timeout=20):
    """Return once a backend waits for ``source``'s per-source capture lock
    (so the test never depends on how fast a thread reaches that point)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' "
                        "AND NOT granted AND classid=110094 AND objid=hashtext(%s)::oid "
                        "AND objsubid=2", (source,))
            waiting = cur.fetchone()[0]
        connection.commit()
        if waiting:
            return
        time.sleep(0.02)
    raise AssertionError("no create waited for the source's capture lock")


# --- Locking (AUD-11) --------------------------------------------------------


def test_creates_on_different_sources_run_concurrently(db, second_connection, monkeypatch):
    """Inverts probe ``test_probe_concurrent_create_503_on_lock_timeout``: a
    capture no longer holds the global lock, so another source's create is
    admitted and captured while it runs."""
    monkeypatch.setattr(profile_bootstrap, "LOCK_TIMEOUT_MS", 500)
    blocker = _Blocker(monkeypatch, SOURCE)
    thread, first = _in_thread(profile_bootstrap.create_session, body())
    try:
        assert blocker.capturing.wait(20)
        with second_connection.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(110094, 10)")
            assert cur.fetchone()[0] is True
            cur.execute("SELECT pg_advisory_unlock(110094, 10)")
        second_connection.commit()
        # Completes while the first capture is still held (it would otherwise
        # 503 after LOCK_TIMEOUT_MS, or wait until the blocker's release).
        other = profile_bootstrap.create_session(body(catalog_instance_id=OTHER))
        assert other["snapshot_count"] == 1
        assert not blocker.release.is_set() and thread.is_alive()
    finally:
        blocker.release.set()
        thread.join(30)
    assert _outcome(first)["snapshot_count"] == 1


def test_same_source_create_waits_for_the_capture_instead_of_503(
        db, second_connection, monkeypatch):
    monkeypatch.setattr(profile_bootstrap, "LOCK_TIMEOUT_MS", 300)
    blocker = _Blocker(monkeypatch, SOURCE)
    thread, first = _in_thread(profile_bootstrap.create_session, body())
    waiter = None
    try:
        assert blocker.capturing.wait(20)
        waiter, second = _in_thread(profile_bootstrap.create_session, body())
        _await_capture_lock_waiter(second_connection, SOURCE)
        # Hold the first capture well past LOCK_TIMEOUT_MS.
        time.sleep(0.6)
        assert waiter.is_alive()
    finally:
        blocker.release.set()
        thread.join(30)
        if waiter is not None:
            waiter.join(30)
    assert _outcome(first)["snapshot_count"] == 1
    assert _outcome(second)["snapshot_count"] == 1
    # It waited for the first capture (per-source lock), within the create budget.
    assert 0.6 <= second["elapsed"] < 5


def test_capture_holds_only_its_source_lock_and_always_releases_it(
        db, second_connection, monkeypatch):
    """Inverts the global-lock half of the lum010 lock probe. The per-source
    session lock is unlocked explicitly, so it is free as soon as create
    returns, without waiting for the owned backend to exit."""
    with db.cursor() as cur:
        cur.execute("SELECT pg_backend_pid(), txid_current()")
        request_pid, request_txid = cur.fetchone()
    observed = []
    original = profile_bootstrap.serialize_profile

    def inspect_capture(*row):
        with second_connection.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(110094, 10)")
            global_free = cur.fetchone()[0]
            if global_free:
                cur.execute("SELECT pg_advisory_unlock(110094, 10)")
            cur.execute("SELECT pg_try_advisory_lock(110094, hashtext(%s))", (SOURCE,))
            source_free = cur.fetchone()[0]
            cur.execute(
                "SELECT l.pid, a.application_name FROM pg_locks l "
                "JOIN pg_stat_activity a USING (pid) WHERE l.locktype='advisory' "
                "AND l.classid=110094 AND l.objid<>10 AND l.granted")
            observed.append((global_free, source_free, cur.fetchall()))
        second_connection.rollback()
        return original(*row)

    # Right after each capture, while the owned connection is still open (so
    # its backend exit cannot be what frees the lock), the source lock must
    # already be free: it was unlocked explicitly, on success and on failure.
    original_capture = profile_bootstrap._capture
    free_after_capture = []

    def checked_capture(owned, *args):
        try:
            return original_capture(owned, *args)
        finally:
            assert not owned.closed
            with second_connection.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(110094, hashtext(%s))", (SOURCE,))
                acquired = cur.fetchone()[0]
                if acquired:
                    cur.execute("SELECT pg_advisory_unlock(110094, hashtext(%s))", (SOURCE,))
            second_connection.commit()
            free_after_capture.append(acquired)

    monkeypatch.setattr(profile_bootstrap, "_capture", checked_capture)
    monkeypatch.setattr(profile_bootstrap, "serialize_profile", inspect_capture)
    profile_bootstrap.create_session(body())
    assert free_after_capture == [True]
    (global_free, source_free, holders), = observed
    assert global_free is True and source_free is False
    assert len(holders) == 1
    owned_pid, application_name = holders[0]
    assert owned_pid != request_pid
    assert application_name == "lumae-profile-bootstrap"
    with db.cursor() as cur:
        cur.execute("SELECT pg_backend_pid(), txid_current()")
        assert cur.fetchone() == (request_pid, request_txid)
    db.rollback()
    # Released before create returned: no dependence on backend exit timing.
    for key in ("10", "hashtext(%s)"):
        with second_connection.cursor() as cur:
            cur.execute(f"SELECT pg_try_advisory_lock(110094, {key})",
                        (SOURCE,) if "%s" in key else ())
            assert cur.fetchone()[0] is True
            cur.execute(f"SELECT pg_advisory_unlock(110094, {key})",
                        (SOURCE,) if "%s" in key else ())
        second_connection.commit()

    monkeypatch.setattr(profile_bootstrap, "serialize_profile",
                        _raiser(RuntimeError("capture failed")))
    with pytest.raises(profile_bootstrap.BootstrapError):
        profile_bootstrap.create_session(body())
    assert free_after_capture == [True, True]
    with second_connection.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(110094, hashtext(%s))", (SOURCE,))
        assert cur.fetchone()[0] is True
        cur.execute("SELECT pg_advisory_unlock(110094, hashtext(%s))", (SOURCE,))
    second_connection.commit()


def test_admitted_session_holds_the_floor_during_capture(db, second_connection, monkeypatch):
    """The session row is committed at admission, before the capture, so a
    publication pass during the capture cannot compact the events the new
    session's catch-up has to replay (P1-2 review F2)."""
    monkeypatch.setattr(catalog_enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", 10)
    epoch = _query(db, f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s",
                   (SOURCE,))[0][0]
    _execute(db, f"UPDATE {STATE} SET retention_limit=10, head_seq=5 "
                 "WHERE catalog_instance_id=%s", (SOURCE,))
    _execute(db, f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, "
                 "operation, writer_generation) SELECT %s, %s, n, 'track-' || n, 'delete', 2 "
                 "FROM generate_series(1, 5) n", (SOURCE, epoch))
    original = profile_bootstrap.serialize_profile
    seen = {}

    def publish_during_capture(*row):
        if not seen:
            with second_connection.cursor() as cur:
                cur.execute(f"SELECT state, snapshot_seq FROM {SESSIONS}")
                seen["rows"] = cur.fetchall()
                cur.execute(
                    f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, "
                    "operation, writer_generation) SELECT %s, %s, n, 'track-' || n, "
                    "'delete', 2 FROM generate_series(6, %s) n",
                    (SOURCE, epoch, RETAINED + 200))
                cur.execute(f"UPDATE {STATE} SET head_seq=%s WHERE catalog_instance_id=%s",
                            (RETAINED + 200, SOURCE))
                catalog_enrichment.record_profile_change(
                    cur, SOURCE, "published", "ready", {"track_id": "published"})
                cur.execute(f"SELECT floor_seq FROM {STATE} WHERE catalog_instance_id=%s",
                            (SOURCE,))
                seen["floor"] = cur.fetchone()[0]
            second_connection.commit()
        return original(*row)

    monkeypatch.setattr(profile_bootstrap, "serialize_profile", publish_during_capture)
    created = profile_bootstrap.create_session(body(page_size=500))
    assert seen["rows"] == [("capturing", 5)]
    assert seen["floor"] <= 5
    assert created["snapshot_seq"] == 5
    catchup = profile_bootstrap.catchup_page(body(session_token=created["session_token"]))
    assert catchup["changes"][0]["seq"] == 6
    assert _query(db, f"SELECT state, snapshot_seq FROM {SESSIONS}") == [("ready", 5)]


def _session_row(db, source, *, snapshot_seq, server="server-a", catalog_epoch="epoch-a",
                 profile_epoch=None, state="ready", created="now()"):
    if profile_epoch is None:
        profile_epoch = _query(db, f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s",
                               (source,))[0][0]
    _execute(db, f"""INSERT INTO {SESSIONS}
        (session_id, token_hash, signing_secret, source_scope, catalog_instance_id,
         core_server_id, catalog_epoch, profile_epoch, schema_version, page_size,
         snapshot_seq, snapshot_count, expires_at, state, created_at)
        VALUES (%s, %s, 'secret', %s, %s, %s, %s, %s, 1, 50, %s, 0,
                now() + interval '1 hour', %s, {created})""",
             (str(uuid.uuid4()), uuid.uuid4().hex, source, source, server, catalog_epoch,
              profile_epoch, snapshot_seq, state))


def test_floor_hold_ignores_identity_stale_and_abandoned_sessions(db, monkeypatch):
    """Sessions that would 410 anyway (catalogue epoch or core server changed,
    source inactive) and abandoned captures do not hold the floor (P1-2
    review F3)."""
    monkeypatch.setattr(catalog_enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", 10)
    _execute(db, f"UPDATE {STATE} SET retention_limit=10 WHERE catalog_instance_id=%s",
             (SOURCE,))
    _session_row(db, SOURCE, snapshot_seq=1, catalog_epoch="old-epoch")
    _session_row(db, SOURCE, snapshot_seq=2, server="server-old")
    _session_row(db, SOURCE, snapshot_seq=3, state="capturing",
                 created="now() - interval '11 minutes'")
    epoch = _query(db, f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s",
                   (SOURCE,))[0][0]
    _execute(db, f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, "
                 "operation, writer_generation) SELECT %s, %s, n, 'track-' || n, 'delete', 2 "
                 "FROM generate_series(1, %s) n", (SOURCE, epoch, RETAINED + 50))
    _execute(db, f"UPDATE {STATE} SET head_seq=%s WHERE catalog_instance_id=%s",
             (RETAINED + 50, SOURCE))
    with db.cursor() as cur:
        assert catalog_enrichment._profile_floor_hold(cur, SOURCE, epoch) is None
        catalog_enrichment.record_profile_change(cur, SOURCE, "p", "ready", {"track_id": "p"})
    db.commit()
    assert _query(db, f"SELECT floor_seq FROM {STATE} WHERE catalog_instance_id=%s",
                  (SOURCE,))[0][0] == 51

    # A live capturing session (admitted 1 minute ago) and an inactive source.
    _session_row(db, SOURCE, snapshot_seq=60, state="capturing",
                 created="now() - interval '1 minute'")
    with db.cursor() as cur:
        assert catalog_enrichment._profile_floor_hold(cur, SOURCE, epoch) == 60
        cur.execute(f"UPDATE {P}catalog_sources SET rebind_status='rebinding' "
                    "WHERE catalog_instance_id=%s", (SOURCE,))
        assert catalog_enrichment._profile_floor_hold(cur, SOURCE, epoch) is None
    db.rollback()


# --- Slots, release and K5 ---------------------------------------------------


def test_epoch_change_no_longer_locks_the_source_out(db):
    """Inverts probe ``test_probe_epoch_change_locks_out_source``: release of an
    identity-stale session answers 200 and deletes it, and stale rows never
    count toward the slot limit."""
    tokens = [profile_bootstrap.create_session(body())["session_token"] for _ in range(4)]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert exc.value.status == 429
    _execute(db, f"UPDATE {STATE} SET epoch='next-epoch' WHERE catalog_instance_id=%s",
             (SOURCE,))
    for token in tokens[:2]:
        assert profile_bootstrap.release_session(body(session_token=token)) == {
            "protocol_version": 2, "schema_version": 1,
            "transfer_contract": "source_scoped_v1", "released": True}
    assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(2,)]
    # The other two stale rows are purged by the next create, not counted.
    created = profile_bootstrap.create_session(body())
    assert created["profile_epoch"] == "next-epoch"
    assert _query(db, f"SELECT token_hash FROM {SESSIONS}") == [
        (_hash(created["session_token"]),)]


def test_release_always_deletes_the_matching_session(db):
    expired = profile_bootstrap.create_session(body())["session_token"]
    _execute(db, f"UPDATE {SESSIONS} SET expires_at=now() - interval '1 second' "
                 "WHERE token_hash=%s", (_hash(expired),))
    kept = profile_bootstrap.create_session(body())["session_token"]
    for token, source in ((expired, SOURCE), (kept, OTHER), (uuid.uuid4().hex * 2, SOURCE)):
        assert profile_bootstrap.release_session(
            body(session_token=token, catalog_instance_id=source))["released"] is True
    # The expired row is gone; another source cannot release this source's session.
    assert _query(db, f"SELECT token_hash FROM {SESSIONS}") == [(_hash(kept),)]
    assert profile_bootstrap.snapshot_page(body(session_token=kept))["profiles"]
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.release_session(body(session_token="A" * 64))
    assert exc.value.status == 400


def test_duplicate_client_request_id_replaces_the_unclaimed_session(db):
    request_id = str(uuid.uuid4())
    first = profile_bootstrap.create_session(body(client_request_id=request_id))
    second = profile_bootstrap.create_session(body(client_request_id=request_id.upper()))
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.snapshot_page(body(session_token=first["session_token"]))
    assert exc.value.status == 410
    assert _query(db, f"SELECT token_hash, client_request_id::text FROM {SESSIONS}") == [
        (_hash(second["session_token"]), request_id)]

    # A claimed (paged) session is never replaced; other ids and sources are separate.
    assert profile_bootstrap.snapshot_page(body(session_token=second["session_token"]))
    third = profile_bootstrap.create_session(body(client_request_id=request_id))
    other = profile_bootstrap.create_session(
        body(client_request_id=request_id, catalog_instance_id=OTHER))
    for created, source in ((second, SOURCE), (third, SOURCE), (other, OTHER)):
        assert profile_bootstrap.snapshot_page(body(
            session_token=created["session_token"], catalog_instance_id=source))
    assert _query(db, f"SELECT pages_served FROM {SESSIONS} WHERE token_hash=%s",
                  (_hash(second["session_token"]),)) == [(2,)]


def test_duplicate_client_request_id_is_replaced_even_when_slots_are_full(db):
    request_id = str(uuid.uuid4())
    for _ in range(3):
        profile_bootstrap.create_session(body())
    stale = profile_bootstrap.create_session(body(client_request_id=request_id))
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body(client_request_id=str(uuid.uuid4())))
    assert exc.value.status == 429
    fresh = profile_bootstrap.create_session(body(client_request_id=request_id))
    hashes = {row[0] for row in _query(db, f"SELECT token_hash FROM {SESSIONS}")}
    assert _hash(fresh["session_token"]) in hashes
    assert _hash(stale["session_token"]) not in hashes and len(hashes) == 4


def test_retried_create_replaces_a_session_that_is_still_capturing(
        db, second_connection, monkeypatch):
    """A client that timed out retries with the same id while the first
    capture still runs: the retry replaces it, the first capture gives up
    (410) and removes its rows, and only the retry's session remains."""
    request_id = str(uuid.uuid4())
    blocker = _Blocker(monkeypatch, SOURCE)
    thread, first = _in_thread(profile_bootstrap.create_session,
                               body(client_request_id=request_id))
    retry = None
    try:
        assert blocker.capturing.wait(20)
        retry, retried = _in_thread(profile_bootstrap.create_session,
                                    body(client_request_id=request_id))
        # The retry is admitted (replacing the first) and waits for the capture.
        _await_capture_lock_waiter(second_connection, SOURCE)
    finally:
        blocker.release.set()
        thread.join(30)
        if retry is not None:
            retry.join(30)
    second = _outcome(retried)
    error = first.get("error")
    assert isinstance(error, profile_bootstrap.BootstrapError), first
    assert (error.code, error.status) == ("bootstrap_required", 410)
    assert _query(db, f"SELECT token_hash, state FROM {SESSIONS}") == [
        (_hash(second["session_token"]), "ready")]
    assert _query(db, f"SELECT count(*) FROM {P}profile_bootstrap_snapshot") == [(1,)]
    assert profile_bootstrap.snapshot_page(body(session_token=second["session_token"]))["profiles"]


@pytest.mark.parametrize("field,value", [
    ("client_request_id", "not-a-uuid"), ("client_request_id", 7),
    ("client_request_id", "a" * 100), ("expiry_mode", "forever"), ("expiry_mode", True)])
def test_invalid_create_options_are_400(db, field, value):
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body(**{field: value}))
    assert (exc.value.code, exc.value.status) == ("invalid_profile_bootstrap", 400)
    assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(0,)]


def test_abandoned_captures_are_purged_and_hold_no_slot(db):
    for _ in range(4):
        _session_row(db, SOURCE, snapshot_seq=0, state="capturing",
                     created="now() - interval '11 minutes'")
    first = profile_bootstrap.create_session(body())
    # Each create purges at most PURGE_MAX_SESSIONS dead rows; they never count.
    assert profile_bootstrap.PURGE_MAX_SESSIONS == 2
    assert _query(db, f"SELECT count(*) FILTER (WHERE state='capturing'), count(*) "
                      f"FROM {SESSIONS}") == [(2, 3)]
    second = profile_bootstrap.create_session(body())
    assert sorted(_query(db, f"SELECT token_hash, state FROM {SESSIONS}")) == sorted([
        (_hash(first["session_token"]), "ready"), (_hash(second["session_token"]), "ready")])
    for _ in range(2):
        _session_row(db, SOURCE, snapshot_seq=0, state="capturing",
                     created="now() - interval '1 minute'")
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert exc.value.status == 429


def test_failed_purge_is_logged_and_the_create_still_succeeds(db, monkeypatch):
    log = _Log(monkeypatch)
    _session_row(db, SOURCE, snapshot_seq=0, catalog_epoch="old-epoch")

    def failing_purge(owned):
        with owned.cursor() as cur:
            cur.execute("SELECT 1/0")  # leaves the transaction aborted

    monkeypatch.setattr(profile_bootstrap, "_delete_dead", failing_purge)
    created = profile_bootstrap.create_session(body())
    assert profile_bootstrap.snapshot_page(body(session_token=created["session_token"]))
    assert log.records == [
        ("warning", "lumae_analysis profile bootstrap purge failed (DivisionByZero)")]
    # The stale row stays until a later purge, but holds no slot.
    assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(2,)]


# --- K4: Retry-After, rate limit, health -------------------------------------


def test_retry_after_on_429_and_503(db, monkeypatch):
    app = _app()
    with app.test_client() as client:
        for _ in range(4):
            assert client.post(ROUTE, json=body()).status_code == 200
        full = client.post(ROUTE, json=body())
        assert full.status_code == 429
        assert full.json == {"error": "bootstrap_session_limit",
                             "message": "bootstrap_session_limit"}
        assert full.headers["Retry-After"] == "300"  # 60 minutes away, capped
        _execute(db, f"UPDATE {SESSIONS} SET expires_at=now() + interval '42 seconds' "
                     f"WHERE session_id=(SELECT session_id FROM {SESSIONS} LIMIT 1)")
        soon = client.post(ROUTE, json=body())
        assert soon.status_code == 429
        assert 40 <= int(soon.headers["Retry-After"]) <= 43

        monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                            _raiser(psycopg2.OperationalError("connection refused")))
        for path in (ROUTE, ROUTE + "/page", ROUTE + "/catchup", ROUTE + "/release"):
            down = client.post(path, json=body(session_token="a" * 64))
            assert down.status_code == 503
            assert down.headers["Retry-After"] == "5"
        invalid = client.post(ROUTE, json=[])
        assert (invalid.status_code, invalid.headers.get("Retry-After")) == (400, None)
        monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL", None)
        unset = client.post(ROUTE, json=body())
        assert (unset.status_code, unset.headers["Retry-After"]) == (503, "5")


def test_create_rate_limit_is_per_source_and_caller(db):
    app = _app(caller_header=True)
    with app.test_client() as client:
        def create(user, source=SOURCE):
            return client.post(ROUTE, json=body(catalog_instance_id=source),
                               headers={"X-Test-User": user} if user else {})

        for _ in range(6):
            created = create("alice")
            assert created.status_code == 200
            assert client.post(ROUTE + "/release", json=body(
                session_token=created.json["session_token"])).status_code == 200
        limited = create("alice")
        assert limited.status_code == 429
        assert limited.json["error"] == "bootstrap_session_limit"
        assert limited.headers["Retry-After"] == "300"
        assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(0,)]
        for user, source in (("bob", SOURCE), ("bearer", SOURCE), (None, SOURCE),
                             ("alice", OTHER)):
            assert create(user, source).status_code == 200
        _execute(db, f"UPDATE {P}profile_bootstrap_creates "
                     "SET created_at=created_at - interval '9 minutes 30 seconds'")
        again = create("alice")
        assert again.status_code == 429
        assert 25 <= int(again.headers["Retry-After"]) <= 31


def _health():
    app = _app()
    with app.test_client() as client:
        return client.get("/api/health").json["capabilities"]["profile_bootstrap"]


def test_health_profile_bootstrap_is_truthful(db, monkeypatch):
    capability = _health()
    assert capability == {"protocol_version": 2, "schema_version": 1,
                          "auth": "host_authenticated", "auth_enabled": False,
                          "transfer_contract": "source_scoped_v1", "available": True,
                          "sliding_expiry": True, "idempotent_create": True}
    for configured, expected in ((True, True), ("true", True), ("False", False), (False, False)):
        monkeypatch.setattr(plugin_api_module.config, "AUTH_ENABLED", configured, raising=False)
        assert _health()["auth_enabled"] is expected
        assert _health()["auth"] == "host_authenticated"

    # A successful probe is cached for 60 s; after that a failing probe is reported.
    clock = [1_000.0]
    monkeypatch.setattr(profile_bootstrap, "_monotonic", lambda: clock[0])
    monkeypatch.setattr(profile_bootstrap, "_availability_cache", None)
    assert _health()["available"] is True
    actual_connect = profile_bootstrap.psycopg2.connect
    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                        _raiser(psycopg2.OperationalError("connection refused")))
    clock[0] += 59
    assert _health()["available"] is True
    clock[0] += 2
    assert _health()["available"] is False
    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect", actual_connect)
    clock[0] += 61
    assert _health()["available"] is True

    # Reachable but not migrated to 1.3.0: not available.
    _execute(db, f"ALTER TABLE {SESSIONS} DROP COLUMN pages_served")
    clock[0] += 61
    assert _health()["available"] is False


def test_health_available_is_false_without_database_url(monkeypatch):
    monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL", None, raising=False)
    monkeypatch.setattr(profile_bootstrap, "_availability_cache", None, raising=False)
    assert _health()["available"] is False


# --- K3: sliding expiry ------------------------------------------------------


def _expires(db, token):
    return _query(db, f"SELECT expires_at, pages_served FROM {SESSIONS} WHERE token_hash=%s",
                  (_hash(token),))[0]


def _utc(value):
    assert value.endswith("Z") and "+" not in value
    return datetime.datetime.fromisoformat(value[:-1] + "+00:00")


def test_sliding_expiry_extends_and_absolute_does_not(db):
    sliding = profile_bootstrap.create_session(body(expiry_mode="sliding", page_size=1))
    absolute = profile_bootstrap.create_session(body(expiry_mode="absolute"))
    default = profile_bootstrap.create_session(body())
    now = _query(db, "SELECT now()")[0][0]
    for created in (sliding, absolute, default):
        assert abs((_utc(created["expires_at"]) - now).total_seconds() - 3600) < 60

    _execute(db, f"UPDATE {SESSIONS} SET created_at=now() - interval '2 hours', "
                 "expires_at=now() + interval '5 minutes'")
    token = sliding["session_token"]
    page = profile_bootstrap.snapshot_page(body(session_token=token))
    stored, served = _expires(db, token)
    assert _utc(page["expires_at"]) == stored and served == 1
    assert 59 * 60 < (stored - now).total_seconds() < 61 * 60 + 30
    catchup = profile_bootstrap.catchup_page(body(session_token=token))
    assert _utc(catchup["expires_at"]) >= stored
    assert _expires(db, token)[1] == 2

    # Capped at created_at + 24 h.
    _execute(db, f"UPDATE {SESSIONS} SET created_at=now() - interval '23 hours 50 minutes', "
                 "expires_at=now() + interval '5 minutes' WHERE token_hash=%s", (_hash(token),))
    capped = profile_bootstrap.snapshot_page(body(session_token=token))
    created_at = _query(db, f"SELECT created_at FROM {SESSIONS} WHERE token_hash=%s",
                        (_hash(token),))[0][0]
    assert _utc(capped["expires_at"]) == created_at + datetime.timedelta(hours=24)

    for created in (absolute, default):
        before, _served = _expires(db, created["session_token"])
        page = profile_bootstrap.snapshot_page(body(session_token=created["session_token"]))
        after, served = _expires(db, created["session_token"])
        assert before == after == _utc(page["expires_at"]) and served == 1


# --- Errors, logging, timestamps, body cap -----------------------------------


class _Log:
    def __init__(self, monkeypatch):
        self.records = []
        for level in ("exception", "warning"):
            monkeypatch.setattr(plugin_api_module.logger, level, self._recorder(level),
                                raising=False)

    def _recorder(self, level):
        def record(message, *args, **_kwargs):
            self.records.append((level, message % args if args else message))
        return record


def test_unexpected_error_is_logged_with_its_class_and_no_secrets(db, monkeypatch):
    """Inverts probe ``test_probe_serializer_valueerror_unlogged``."""
    log = _Log(monkeypatch)
    monkeypatch.setattr(profile_bootstrap, "serialize_profile",
                        _raiser(ValueError("bad ramp")))
    app = _app()
    with app.test_client() as client:
        response = client.post(ROUTE, json=body())
    assert response.status_code == 503
    assert response.json == {"error": "bootstrap_unavailable", "message": "bootstrap_unavailable"}
    assert response.headers["Retry-After"] == "5"
    assert [level for level, _ in log.records] == ["exception"]
    assert "ValueError" in log.records[0][1]
    secret_parts = (plugin_api_module.config.DATABASE_URL, "lumae_test@")
    assert not any(part in message for _, message in log.records for part in secret_parts)
    # The admitted session was removed with the failed capture.
    assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(0,)]


def test_route_level_unexpected_error_is_logged(db, monkeypatch):
    log = _Log(monkeypatch)
    monkeypatch.setattr(profile_bootstrap, "snapshot_page", _raiser(KeyError("internal")))
    with _app().test_client() as client:
        response = client.post(ROUTE + "/page", json=body(session_token="a" * 64))
    assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
    assert len(log.records) == 1 and "KeyError" in log.records[0][1]


class _BrokenRollback(psycopg2.extensions.connection):
    def rollback(self):
        raise psycopg2.InterfaceError("rollback failed")


def test_failed_rollback_never_masks_the_original_error(db, monkeypatch):
    log = _Log(monkeypatch)
    actual_connect = psycopg2.connect
    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                        lambda dsn, **options: actual_connect(
                            dsn, connection_factory=_BrokenRollback, **options))
    monkeypatch.setattr(profile_bootstrap, "serialize_profile",
                        _raiser(ValueError("bad ramp")))
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert (exc.value.code, exc.value.status) == ("bootstrap_unavailable", 503)
    assert [level for level, _ in log.records] == ["exception"]
    assert "ValueError" in log.records[0][1]


def test_availability_errors_are_503_without_a_traceback(db, monkeypatch):
    log = _Log(monkeypatch)
    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                        _raiser(psycopg2.OperationalError("password=hunter2 refused")))
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert exc.value.status == 503 and exc.value.__cause__ is None
    assert all(level == "warning" for level, _ in log.records)
    assert not any("hunter2" in message for _, message in log.records)


@pytest.mark.parametrize("dsn,secret", [
    ("host=127.0.0.1 password=my secret", "secret"),
    ("postgresql://user:s3cretpw@[bad/db", "s3cretpw"),
])
def test_malformed_database_url_never_reaches_the_log(dsn, secret, monkeypatch):
    """libpq puts the DSN (with the password) into malformed-DSN errors
    (ProgrammingError). Creating, the route and the health probe log only the
    error class, as a warning without a traceback."""
    log = _Log(monkeypatch)
    monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL", dsn, raising=False)
    monkeypatch.setattr(profile_bootstrap, "_availability_cache", None, raising=False)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert (exc.value.status, exc.value.__cause__) == (503, None)
    with _app().test_client() as client:
        response = client.post(ROUTE, json=body())
        assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
        assert secret not in response.get_data(as_text=True)
    assert _health()["available"] is False
    assert [level for level, _ in log.records] == ["warning"] * 3
    assert all("ProgrammingError" in message for _, message in log.records)
    assert not any(secret in message for _, message in log.records)


def test_owned_connection_is_named_and_keeps_alive(db, monkeypatch):
    actual_connect = psycopg2.connect
    observed = []
    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                        lambda dsn, **options: observed.append(options)
                        or actual_connect(dsn, **options))
    with profile_bootstrap._connection() as owned:
        with owned.cursor() as cur:
            cur.execute("SELECT current_setting('application_name')")
            name, = cur.fetchone()
    assert name == "lumae-profile-bootstrap"
    options = {"application_name": "lumae-profile-bootstrap", "keepalives": 1,
               "keepalives_idle": 30}
    assert observed == [{"connect_timeout": 5, **options}]
    # The health probe connects with a short timeout, so an unreachable
    # database cannot stall /api/health for 5 s.
    assert profile_bootstrap.availability() is True
    assert observed[1:] == [{"connect_timeout": 2, **options}]


def test_timestamps_are_utc_on_a_non_utc_server(db, monkeypatch):
    monkeypatch.setattr(plugin_api_module.config, "DATABASE_URL",
                        _dsn(_schema(db), " -c TimeZone=Europe/Amsterdam"))
    with db.cursor() as cur:
        catalog_enrichment.record_profile_change(cur, SOURCE, "t", "ready", {"track_id": "t"})
    db.commit()
    created = profile_bootstrap.create_session(body())
    # Seq 1 was published before the capture; publish one more for the catch-up.
    with db.cursor() as cur:
        catalog_enrichment.record_profile_change(cur, SOURCE, "u", "ready", {"track_id": "u"})
    db.commit()
    stored = _query(db, f"SELECT expires_at FROM {SESSIONS}")[0][0]
    assert _utc(created["expires_at"]) == stored
    token = created["session_token"]
    page = profile_bootstrap.snapshot_page(body(session_token=token))
    catchup = profile_bootstrap.catchup_page(body(session_token=token))
    assert _utc(page["expires_at"]) == _utc(catchup["expires_at"]) == stored
    assert [_utc(event["created_at"]) for event in catchup["changes"]]

    # The legacy enrichment formatter converts zoned values; naive stays naive.
    amsterdam = datetime.timezone(datetime.timedelta(hours=2))
    aware = datetime.datetime(2026, 9, 24, 12, 0, 0, 5, tzinfo=amsterdam)
    assert catalog_enrichment._iso(aware) == "2026-09-24T10:00:00.000005Z"
    assert catalog_enrichment._iso(datetime.datetime(2026, 9, 24, 12, 0)) == "2026-09-24T12:00:00"
    other = psycopg2.connect(_dsn(_schema(db), " -c TimeZone=Europe/Amsterdam"))
    try:
        changes = catalog_enrichment.read_profile_changes(
            other, catalog_enrichment.opaque_cursor(SOURCE, created["profile_epoch"], 0),
            catalog_instance_id=SOURCE)
    finally:
        other.close()
    assert [_utc(event["created_at"]) for event in changes["changes"]]


class _ShortReads(io.RawIOBase):
    """A request stream that returns at most 700 bytes per read, as a socket may."""

    def __init__(self, payload):
        self._data = io.BytesIO(payload)

    def readable(self):
        return True

    def read(self, size=-1):
        return self._data.read(700 if size is None or size < 0 else min(size, 700))


def _chunked(app, payload, stream=None):
    builder = EnvironBuilder(path=ROUTE, method="POST", input_stream=io.BytesIO(payload),
                             content_type="application/json",
                             headers={"Transfer-Encoding": "chunked"})
    environ = builder.get_environ()
    environ.pop("CONTENT_LENGTH", None)
    environ["wsgi.input_terminated"] = True
    if stream is not None:
        environ["wsgi.input"] = stream
    app_iter, status, _headers = run_wsgi_app(app, environ, buffered=True)
    return int(status.split()[0]), json.loads(b"".join(app_iter))


def test_chunked_body_over_16_kib_is_rejected(db):
    app = _app()
    status, payload = _chunked(app, json.dumps(body(padding="x" * 20_480)).encode())
    assert status == 400
    assert payload == {"error": "invalid_profile_bootstrap",
                       "message": "Invalid bootstrap request."}
    assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(0,)]
    status, payload = _chunked(app, json.dumps(body(padding="x" * 1_000)).encode())
    assert status == 200 and payload["session_token"]
    # Short reads are collected until EOF, and still capped.
    small = json.dumps(body(padding="x" * 3_000)).encode()
    status, payload = _chunked(app, small, _ShortReads(small))
    assert status == 200 and payload["session_token"]
    large = json.dumps(body(padding="x" * 20_480)).encode()
    status, payload = _chunked(app, large, _ShortReads(large))
    assert (status, payload["error"]) == (400, "invalid_profile_bootstrap")
    with app.test_client() as client:
        big = client.post(ROUTE, data=json.dumps(body(padding="x" * 20_480)),
                          content_type="application/json")
        assert big.status_code == 400
        text = client.post(ROUTE, data=json.dumps(body()), content_type="text/plain")
        assert text.status_code == 400


# --- Migration (LUM-010 P3) --------------------------------------------------


def test_account_era_principal_without_default_migrates(db, run_plugin_migration):
    """The 9914d85 account-era table had ``principal TEXT NOT NULL`` with no
    default and none of the source-scoped or P1-6 columns."""
    _execute(db, f"""ALTER TABLE {SESSIONS}
        DROP COLUMN source_scope, DROP COLUMN transfer_contract_version,
        DROP COLUMN IF EXISTS state, DROP COLUMN IF EXISTS expiry_mode,
        DROP COLUMN IF EXISTS client_request_id, DROP COLUMN IF EXISTS pages_served,
        ADD COLUMN principal TEXT NOT NULL,
        ADD COLUMN principal_contract_version INTEGER NOT NULL DEFAULT 2""")
    _execute(db, f"INSERT INTO {SESSIONS} (session_id, token_hash, signing_secret, principal, "
                 "catalog_instance_id, core_server_id, catalog_epoch, profile_epoch, "
                 "schema_version, page_size, snapshot_seq, snapshot_count, expires_at) "
                 "VALUES (%s, 'legacy', 'secret', 'user:alice', %s, 'server-a', 'epoch-a', "
                 "'e', 1, 250, 0, 0, now() + interval '1 hour')", (str(uuid.uuid4()), SOURCE))
    run_plugin_migration(db)
    run_plugin_migration(db)
    assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(0,)]
    assert _query(db, "SELECT is_nullable FROM information_schema.columns "
                      "WHERE table_schema=current_schema() AND table_name=%s "
                      "AND column_name='principal'", (SESSIONS,)) == [("YES",)]
    created = profile_bootstrap.create_session(body(expiry_mode="sliding",
                                                    client_request_id=str(uuid.uuid4())))
    token = created["session_token"]
    assert profile_bootstrap.snapshot_page(body(session_token=token))["profiles"]
    assert profile_bootstrap.catchup_page(body(session_token=token))["has_more"] is False
    assert profile_bootstrap.release_session(body(session_token=token))["released"]
