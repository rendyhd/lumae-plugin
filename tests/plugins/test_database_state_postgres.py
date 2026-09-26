"""P3-10 (LUM-021): ``/database-state`` diagnostics closure, on PostgreSQL.

* Stored ``last_error`` text (catalogue, projection, preparation, backfill) is
  shown through the redactor: no password, token, API key or file path. So is
  every stored error the settings page (``/settings``, ``/settings/status``),
  ``/api/catalog/health`` and ``/api/catalog/prepare`` show.
* Every diagnostic read runs in a savepoint under ``SET LOCAL
  statement_timeout``. A read blocked on a lock renders "unavailable" for its
  section while the rest of the page renders, and neither the host's
  ``statement_timeout`` nor its transaction changes.
* The waveform work-state counts select exactly the rows the background
  scheduler (``fetch_backfill_rows``) selects, in every retry state.
* The GET route writes nothing (P2-1's statement counter).
"""

import re
import threading
import time
import types

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import load_plugin, plugin_client  # noqa: E402  (host stub)
from test_status_summary_postgres import RecordingConnection  # noqa: E402

from plugins.LumaeAnalysis import database_state  # noqa: E402
from plugins.LumaeAnalysis.core_compat import CoreCompatibility  # noqa: E402
from plugins.LumaeAnalysis.profile_publication import (  # noqa: E402
    RETRY_LIMIT,
    REVISION_FAILURES,
    TRANSIENT_FAILURES,
)


P = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
SERVER = "server-a"
V3 = CoreCompatibility("v3.1.1", (3, 1, 1), "v3_registry", "compatible", True)

HOST_SCHEMA = """
CREATE TABLE score (item_id TEXT PRIMARY KEY);
CREATE TABLE embedding (item_id TEXT PRIMARY KEY, embedding BYTEA);
CREATE TABLE clap_embedding (item_id TEXT PRIMARY KEY, embedding BYTEA);
CREATE TABLE track_server_map (item_id TEXT NOT NULL, server_id TEXT NOT NULL,
    provider_track_id TEXT NOT NULL);
CREATE TABLE chromaprint (server_id TEXT NOT NULL, provider_track_id TEXT NOT NULL,
    fingerprint BYTEA, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE task_status (task_id TEXT PRIMARY KEY, parent_task_id TEXT,
    task_type TEXT, status TEXT, end_time DOUBLE PRECISION, details JSONB,
    timestamp TIMESTAMP);
"""

# Stored last_error values, as str(exc) writes them, and what must not show.
STORED_ERRORS = {
    "catalog_state": "refresh failed: postgresql://lumae:Sup3rS3cret@db.internal:5432/audiomuse",
    "analysis_state": (
        "HTTP 500 from http://navidrome:4533/rest/getSong.view?u=admin"
        "&t=26719a1196d2a940705a&s=c19b2d&v=1.16.1"
    ),
    "preparation_state": (
        "provider said 401 to Authorization: Bearer "
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl"
    ),
    "profile_backfill_state": (
        "lookup https://api.example.com/v1?api_key=AKfake0123456789secret failed "
        "reading /home/alice/Music/Artist/01 - Song.flac: No such file"
    ),
}
SECRETS = (
    "Sup3rS3cret", "lumae:", "26719a1196d2a940705a", "c19b2d", "eyJhbGciOiJIUzI1NiJ9",
    "AKfake0123456789secret", "/home/alice", "Song.flac",
)


def revision(track):
    return f"catalog-media:fp-{track}"


def _profile_rows():
    """(track, track columns, profile columns or None, published, expected state)."""
    future, past = "now() + interval '1 hour'", "now() - interval '1 hour'"
    rows = [
        ("t-new", {}, None, False, "due"),
        ("t-ready", {}, {"status": "ready"}, True, "ready"),
        ("t-ready-new-media", {}, {"status": "ready", "media_signature": "catalog-media:old"},
         True, "due"),
        ("t-ready-old-analyzer", {}, {"status": "ready", "analyzer_ver": 0}, False, "due"),
        ("t-ready-no-fingerprint", {"media_fp": ""},
         {"status": "ready", "media_signature": "catalog-media:old"}, True, "ready"),
        ("t-pending", {}, {"status": "pending"}, True, "pending"),
        ("t-pending-interactive", {}, {"status": "pending_interactive"}, False, "pending"),
        ("t-no-fingerprint", {"media_fp": ""}, {"status": "deferred_no_media_revision"},
         False, "deferred_no_media_revision"),
        ("t-fingerprinted", {}, {"status": "deferred_no_media_revision"}, False, "due"),
        ("t-queue-cooling", {}, {"status": "stale", "retry_category": "queue_unavailable",
                                 "retry_count": 1, "retry_after": future}, False, "deferred"),
        ("t-queue-due", {}, {"status": "stale", "retry_category": "queue_unavailable",
                             "retry_count": 1, "retry_after": past}, False, "due"),
        ("t-queue-exhausted", {}, {"status": "stale", "retry_category": "queue_unavailable",
                                   "retry_count": RETRY_LIMIT}, False, "exhausted"),
        # The attempt limit decides even when a past cooldown is left behind.
        ("t-queue-exhausted-past", {}, {"status": "stale", "retry_category": "queue_unavailable",
                                        "retry_count": RETRY_LIMIT, "retry_after": past},
         False, "exhausted"),
        ("t-queue-exhausted-future", {}, {"status": "stale",
                                          "retry_category": "queue_unavailable",
                                          "retry_count": RETRY_LIMIT, "retry_after": future},
         False, "exhausted"),
        # Re-queued with a published baseline: published, but not ready.
        ("t-stale", {}, {"status": "stale"}, True, "due"),
        # LUM-007: a stale transition that kept an earlier category.
        ("t-stale-stranded", {}, {"status": "stale", "retry_category": "analysis_error",
                                  "retry_count": 1, "retry_after": past}, False, "unscheduled"),
        ("t-stale-new-media", {}, {"status": "stale", "retry_category": "analysis_error",
                                   "retry_count": 1,
                                   "retry_media_signature": "catalog-media:old"}, False, "due"),
        ("t-skipped-cooling", {}, {"status": "skipped_no_file",
                                   "retry_category": "media_unavailable", "retry_count": 1,
                                   "retry_after": future}, False, "cooling"),
        ("t-new-analyzer", {}, {"status": "failed", "retry_category": "analysis_error",
                                "retry_count": RETRY_LIMIT, "retry_analyzer_ver": 0},
         False, "due"),
        ("t-new-schema", {}, {"status": "failed", "retry_category": "analysis_error",
                              "retry_count": RETRY_LIMIT, "retry_profile_schema_ver": 0},
         False, "due"),
        ("t-failed-legacy", {}, {"status": "failed", "retry_count": 0}, False, "due"),
        ("t-failed-uncategorized", {}, {"status": "failed", "retry_count": 1}, False,
         "unscheduled"),
        ("t-failed-unknown", {}, {"status": "failed", "retry_category": "mystery_code",
                                  "retry_count": 1, "retry_after": past}, False, "unscheduled"),
        ("t-missing-row-status", {}, {"status": "missing"}, False, "unscheduled"),
    ]
    # Every category of the retry model, so a new one is checked too.
    for category in sorted(TRANSIENT_FAILURES):
        failed = {"status": "failed", "retry_category": category, "retry_count": 1}
        rows += [
            (f"t-{category}-cooling", {}, {**failed, "retry_after": future}, False, "cooling"),
            (f"t-{category}-due", {}, {**failed, "retry_after": past}, False, "due"),
            (f"t-{category}-exhausted", {}, {**failed, "retry_count": RETRY_LIMIT},
             False, "exhausted"),
            (f"t-{category}-exhausted-past", {},
             {**failed, "retry_count": RETRY_LIMIT, "retry_after": past}, True, "exhausted"),
            # A future cooldown does not make a used-up row "cooling".
            (f"t-{category}-exhausted-future", {},
             {**failed, "retry_count": RETRY_LIMIT, "retry_after": future}, False, "exhausted"),
        ]
    for category in sorted(REVISION_FAILURES):
        failed = {"status": "failed", "retry_category": category, "retry_count": 1}
        rows += [
            (f"t-{category}", {}, failed, False, "awaiting_revision"),
            (f"t-{category}-new-media", {},
             {**failed, "retry_media_signature": "catalog-media:old"}, False, "due"),
        ]
    # Not scheduled and not counted in the work states.
    rows += [
        ("t-ineligible", {"analysis_eligible": False}, None, False, None),
        ("t-eligibility-unknown", {"analysis_eligible": None}, None, False, None),
    ]
    return rows


PROFILE_ROWS = _profile_rows()
PUBLISHED = sum(1 for row in PROFILE_ROWS if row[3])


def _insert_track(cur, track, columns):
    values = {"media_fp": f"fp-{track}", "analysis_eligible": True, "available": True,
              **columns}
    cur.execute(
        f"""INSERT INTO {P}catalog_tracks
            (catalog_instance_id, published_generation, track_id, title, metadata_fp,
             media_fp, analysis_eligible, available, payload, first_seen_at, last_seen_at)
            VALUES (%s, 1, %s, %s, 'metadata', %s, %s, %s, '{{}}'::jsonb, now(), now())""",
        (SOURCE, track, track, values["media_fp"], values["analysis_eligible"],
         values["available"]),
    )


def _insert_profile(cur, track, columns):
    values = {
        "analyzer_ver": 1, "profile_schema_ver": 1, "media_signature": revision(track),
        "retry_category": None, "retry_count": 0, "retry_media_signature": revision(track),
        "retry_analyzer_ver": 1, "retry_profile_schema_ver": 1, **columns,
    }
    retry_after = values.pop("retry_after", "NULL")
    cur.execute(
        f"""INSERT INTO {P}source_profiles
            (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs, start_ramp,
             end_ramp, analyzer_ver, profile_schema_ver, media_signature, status,
             retry_category, retry_count, retry_after, retry_media_signature,
             retry_analyzer_ver, retry_profile_schema_ver)
            VALUES (%s, %s, 48000, 1000, -12, '\\x00', '\\x00', %s, %s, %s, %s, %s, %s,
                    {retry_after}, %s, %s, %s)""",
        (SOURCE, track, values["analyzer_ver"], values["profile_schema_ver"],
         values["media_signature"], values["status"], values["retry_category"],
         values["retry_count"], values["retry_media_signature"],
         values["retry_analyzer_ver"], values["retry_profile_schema_ver"]),
    )


def _publish(cur, track):
    cur.execute(
        f"""INSERT INTO {P}published_source_profiles
            (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs, start_ramp,
             end_ramp, analyzer_ver, profile_schema_ver, media_signature, analyzed_at)
            VALUES (%s, %s, 48000, 1000, -12, '\\x00', '\\x00', 1, 1, %s, now())""",
        (SOURCE, track, revision(track)),
    )


@pytest.fixture
def fixture_db(migrated_db):
    """One active, complete source with rows in every profile work state."""
    db = migrated_db
    with db.cursor() as cur:
        cur.execute(HOST_SCHEMA)
        cur.execute(
            f"""INSERT INTO {P}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type, server_name,
                 is_default, rebind_status)
                VALUES (%s, %s, 'navidrome', 'Main', TRUE, 'active')""",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_state
                (catalog_instance_id, current_core_server_id, provider_type,
                 published_generation, catalog_epoch, status, entity_counts, last_error)
                VALUES (%s, %s, 'navidrome', 1, 'cat-epoch', 'complete', %s::jsonb, %s)""",
            (SOURCE, SERVER, '{"track": %d}' % len(PROFILE_ROWS),
             STORED_ERRORS["catalog_state"]),
        )
        cur.execute(
            f"""INSERT INTO {P}analysis_state
                (catalog_instance_id, projection_generation, analysis_epoch, status,
                 last_error)
                VALUES (%s, 1, 'analysis-epoch', 'complete', %s)""",
            (SOURCE, STORED_ERRORS["analysis_state"]),
        )
        cur.execute(
            f"""INSERT INTO {P}preparation_state
                (catalog_instance_id, server_id, status, phase, last_error)
                VALUES (%s, %s, 'failed', 'catalog_refresh', %s)""",
            (SOURCE, SERVER, STORED_ERRORS["preparation_state"]),
        )
        cur.execute(
            f"""INSERT INTO {P}profile_backfill_state
                (catalog_instance_id, server_id, status, last_error)
                VALUES (%s, %s, 'failed', %s)""",
            (SOURCE, SERVER, STORED_ERRORS["profile_backfill_state"]),
        )
        for index in range(3):
            cur.execute(
                f"""INSERT INTO {P}analysis_items
                    (catalog_instance_id, projection_generation, analysis_id)
                    VALUES (%s, 1, %s)""",
                (SOURCE, f"item-{index}"),
            )
            cur.execute(
                f"""INSERT INTO {P}track_analysis_links
                    (catalog_instance_id, projection_generation, provider_track_id,
                     analysis_id, status, evidence_complete)
                    VALUES (%s, 1, %s, %s, 'ready', TRUE)""",
                (SOURCE, f"t-link-{index}", f"item-{index}"),
            )
        for track, track_columns, profile, published, _state in PROFILE_ROWS:
            _insert_track(cur, track, track_columns)
            if profile is not None:
                _insert_profile(cur, track, profile)
            if published:
                _publish(cur, track)
        # A withdrawn occurrence is not in the work states.
        _insert_track(cur, "t-withdrawn", {"available": False})
    db.commit()
    return db


def _route(monkeypatch, connection):
    mod = load_plugin()
    monkeypatch.setattr(mod, "detect_core", lambda: V3)
    monkeypatch.setattr(mod, "get_db", lambda: connection)
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    return mod, plugin_client(mod)


def _metric(body, label):
    match = re.search(
        rf"<span>{re.escape(label)}</span><strong>([^<]*)</strong>", body
    )
    assert match, label
    return match.group(1)


def _hold_lock(connection, name):
    """Hold the lock a migration's ALTER TABLE takes, as another backend.

    A read of the table waits for it. Without the diagnostic bound it would
    wait until the returned timer releases the lock (after 15 s), so a missing
    bound fails the test instead of hanging it.
    """
    with connection.cursor() as cur:
        cur.execute(f"LOCK TABLE {P}{name} IN ACCESS EXCLUSIVE MODE")
    release = threading.Timer(15, connection.rollback)
    release.start()
    return release


def _show(db, name):
    with db.cursor() as cur:
        cur.execute(f"SHOW {name}")
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_stored_error_secrets_are_redacted_on_the_page(fixture_db, monkeypatch):
    _mod, client = _route(monkeypatch, fixture_db)

    response = client.get("/database-state")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    for secret in SECRETS:
        assert secret not in body, secret
    # The errors stay readable around the masks.
    assert "postgresql://[redacted]@db.internal:5432/audiomuse" in body
    assert "&amp;t=[redacted]&amp;s=[redacted]" in body
    assert "Authorization: [redacted]" in body
    assert "api_key=[redacted]" in body
    assert "reading [path]: No such file" in body


def test_a_retry_category_is_shown_as_is(fixture_db, monkeypatch):
    with fixture_db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}profile_backfill_state SET last_error='queue_unavailable'"
        )
    fixture_db.commit()
    _mod, client = _route(monkeypatch, fixture_db)

    body = client.get("/database-state").get_data(as_text=True)

    assert "<strong>profile backfill:</strong> queue_unavailable" in body


# ---------------------------------------------------------------------------
# Bounded reads
# ---------------------------------------------------------------------------


def test_a_blocked_read_renders_unavailable_and_the_rest_renders(
    fixture_db, second_connection, monkeypatch
):
    db = fixture_db
    monkeypatch.setattr(database_state, "diagnostic_timeout_ms", lambda: 300)
    logged = []
    monkeypatch.setattr(database_state, "logger", types.SimpleNamespace(
        warning=lambda message, *args, **_kwargs: logged.append(message % args),
        exception=lambda message, *args, **_kwargs: logged.append(message % args),
    ))
    with db.cursor() as cur:
        cur.execute("SET statement_timeout = '123s'")
    db.commit()
    before = _show(db, "statement_timeout")
    release = _hold_lock(second_connection, "analysis_items")
    # The host's own transaction, which the diagnostics must not end.
    with db.cursor() as cur:
        cur.execute("SELECT set_config('lumae.p310_marker', 'kept', true)")
    _mod, client = _route(monkeypatch, db)
    try:
        started = time.monotonic()
        response = client.get("/database-state")
        elapsed = time.monotonic() - started
    finally:
        release.cancel()
        second_connection.rollback()

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert elapsed < 10, elapsed
    # The blocked section is unavailable, not zero.
    for label in ("Analysis items", "MusiCNN vectors", "CLAP vectors"):
        assert _metric(body, label) == "unavailable", label
    assert "analysis_items_summary · timeout · 57014" in body
    assert "Some diagnostic queries were unavailable." in body
    # Everything else still renders from its own read.
    assert _metric(body, "Usable links") == "3"
    assert _metric(body, "Shared groups") == "0"
    assert _metric(body, "Published") == str(PUBLISHED)
    assert _metric(body, "Catalogue journal rows") == "0"
    assert "1. Navidrome catalogue" in body
    # Logged with its class, without the statement or any secret.
    assert any(
        "analysis_items_summary unavailable (QueryCanceled, timeout, SQLSTATE 57014)" in line
        for line in logged
    ), logged
    assert not any("SELECT" in line or "Sup3rS3cret" in line for line in logged)
    # The host transaction and its statement_timeout are untouched.
    with db.cursor() as cur:
        cur.execute("SELECT current_setting('lumae.p310_marker', true)")
        assert cur.fetchone()[0] == "kept"
    assert _show(db, "statement_timeout") == before == "123s"
    db.rollback()
    assert _show(db, "statement_timeout") == before


def _timeout_ms(db):
    with db.cursor() as cur:
        cur.execute("SELECT setting FROM pg_settings WHERE name='statement_timeout'")
        return int(cur.fetchone()[0])


@pytest.mark.parametrize("autocommit", [False, True])
def test_statement_timeout_does_not_leak_to_the_host_connection(fixture_db, autocommit):
    db = fixture_db
    with db.cursor() as cur:
        cur.execute("SET statement_timeout = '45s'")
    db.commit()
    before = _show(db, "statement_timeout")
    db.rollback()
    db.autocommit = autocommit
    seen = []

    def readiness(_source):
        seen.append(_timeout_ms(db))
        return {"status": "progressive"}

    snapshot = database_state.collect_database_state(
        db, V3, load_plugin().resolve_catalog_source(db), readiness=readiness
    )

    assert snapshot["errors"] == []
    # Inside a bounded read the diagnostic timeout applies ...
    assert seen == [database_state.DEFAULT_DIAGNOSTIC_STATEMENT_TIMEOUT_MS]
    # ... and afterwards the connection has its own again.
    assert _show(db, "statement_timeout") == before == "45s"
    if autocommit:
        # The transaction the diagnostics owned is closed.
        assert db.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE
    db.rollback()
    db.autocommit = False
    assert _show(db, "statement_timeout") == before


# ---------------------------------------------------------------------------
# Truthful counts: the scheduler's predicates
# ---------------------------------------------------------------------------


def _work_states(db):
    with db.cursor() as cur:
        cur.execute(
            database_state._profile_work_sql(
                "SELECT track_id, state FROM work WHERE state IS NOT NULL"
            ),
            database_state.profile_work_params(SOURCE),
        )
        states = dict(cur.fetchall())
    db.rollback()
    return states


def _scheduler_rows(db, monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    rows = mod.fetch_backfill_rows(10**6, catalog_instance_id=SOURCE, server_id=SERVER)
    db.rollback()
    return [row[0] for row in rows]


def test_work_states_select_exactly_what_the_scheduler_selects(fixture_db, monkeypatch):
    db = fixture_db
    states = _work_states(db)
    expected = {track: state for track, _t, _p, _pub, state in PROFILE_ROWS if state}
    assert states == expected

    selected = _scheduler_rows(db, monkeypatch)
    due = sorted(track for track, state in states.items() if state == "due")
    assert sorted(selected) == due
    assert len(selected) == len(due)

    snapshot = database_state.collect_database_state(
        db, V3, load_plugin().resolve_catalog_source(db)
    )
    db.rollback()
    profiles = snapshot["sources"][0]["profiles"]
    assert profiles["due"] == len(selected)
    assert profiles["schedulable"] is True
    for state in database_state.PROFILE_WORK_STATES:
        assert profiles[state] == list(expected.values()).count(state), state
    assert sum(profiles[state] for state in database_state.PROFILE_WORK_STATES) == (
        profiles["eligible_tracks"]
    )
    assert profiles["catalogue_tracks"] == len(PROFILE_ROWS)
    assert profiles["eligible_tracks"] == len(expected)
    assert profiles["published"] == PUBLISHED
    # Failed and skipped rows by category; classified by the retry model.
    failed = {}
    for _track, _t, profile, _pub, state in PROFILE_ROWS:
        if state and profile and profile["status"] in ("failed", "skipped_no_file"):
            category = profile.get("retry_category") or "uncategorized"
            failed[category] = failed.get(category, 0) + 1
    retry = {
        **{category: "transient" for category in TRANSIENT_FAILURES},
        **{category: "revision" for category in REVISION_FAILURES},
    }
    assert profiles["failure_categories"] == sorted(
        (
            {"category": category, "tracks": tracks,
             "retry": retry.get(category, "unknown")}
            for category, tracks in failed.items()
        ),
        key=lambda row: "" if row["category"] == "uncategorized" else row["category"],
    )
    assert {"mystery_code", "uncategorized"} <= set(failed)


def test_cooldowns_expire_into_the_scheduler_selection(fixture_db, monkeypatch):
    db = fixture_db
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}source_profiles SET retry_after=now() - interval '1 second' "
            "WHERE retry_after > now()"
        )
    db.commit()
    states = _work_states(db)
    assert "cooling" not in states.values() and "deferred" not in states.values()
    due = sorted(track for track, state in states.items() if state == "due")
    assert sorted(_scheduler_rows(db, monkeypatch)) == due


def test_nothing_is_due_while_the_source_is_not_schedulable(fixture_db, monkeypatch):
    db = fixture_db
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_state SET status='refreshing'")
    db.commit()

    assert _scheduler_rows(db, monkeypatch) == []
    snapshot = database_state.collect_database_state(
        db, V3, load_plugin().resolve_catalog_source(db)
    )
    db.rollback()
    profiles = snapshot["sources"][0]["profiles"]
    assert profiles["schedulable"] is False
    assert profiles["due"] > 0
    body = database_state.render_database_state(snapshot)
    assert "The background scheduler selects nothing" in body


# ---------------------------------------------------------------------------
# Read-only GET
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("blocked", [False, True])
def test_the_route_writes_nothing(fixture_db, second_connection, monkeypatch, blocked):
    db = fixture_db
    connection = RecordingConnection(db)
    release = None
    if blocked:
        monkeypatch.setattr(database_state, "diagnostic_timeout_ms", lambda: 300)
        release = _hold_lock(second_connection, "source_profiles")
    _mod, client = _route(monkeypatch, connection)
    try:
        response = client.get("/database-state")
    finally:
        if release is not None:
            release.cancel()
        second_connection.rollback()

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    if blocked:
        assert _metric(body, "Published") == "unavailable"
        assert "waveform_profiles_summary · timeout · 57014" in body
    else:
        assert _metric(body, "Published") == str(PUBLISHED)
    assert connection.statements
    assert connection.writes() == []
    assert connection.commits == 0
    assert connection.transaction_id() is None
    timeouts = [
        sql for sql in connection.statements if sql.startswith("SET LOCAL statement_timeout")
    ]
    savepoints = [sql for sql in connection.statements if sql.startswith("SAVEPOINT")]
    # One bound per read: source resolution plus the twelve diagnostic reads.
    assert len(timeouts) == len(savepoints) == 13


# ---------------------------------------------------------------------------
# Settings page, catalogue health and preparation status (P3-10 follow-up)
# ---------------------------------------------------------------------------

V2 = CoreCompatibility("v2.6.2", (2, 6, 2), "v2_single_server", "compatible", True)

# One distinct secret per site, so a site without redaction leaks its own.
RELATIONSHIP_ERROR = "relationship build failed: password=RelPass123 for db"
RECONCILE_ERROR = "catalog refresh failed: https://bob:EvtPass456@proxy.local:3128 refused"
POST_ERROR = "provider rejected token=PostTok789xyz"
TRANSITION_ERROR = "provider recheck failed: PGPASSWORD=TransPass77 psql exited 2"
DISCOVERY_ERROR = "list_servers failed: Authorization: Basic ZGlzYzpwYXNzd29yZA=="
SITE_SECRETS = ("RelPass123", "EvtPass456", "bob:", "PostTok789xyz", "TransPass77",
                "ZGlzYzpwYXNzd29yZA==")


def _settings(monkeypatch, db, reconcile_error=RECONCILE_ERROR):
    mod, client = _route(monkeypatch, db)
    monkeypatch.setattr(mod, "detect_core", lambda: V2)
    monkeypatch.setattr(mod, "relationship_status", lambda _db, source: {
        "catalog_instance_id": source, "status": "failed", "last_error": RELATIONSHIP_ERROR,
    })
    monkeypatch.setattr(mod, "read_reconcile_status", lambda _db: {
        "control": {"mode": "backoff"},
        "pending": {},
        "events": [{"action": "catalog_refresh", "status": "failed", "phase": "scan",
                    "duration_ms": 5, "summary": "{}", "last_error": reconcile_error}],
    })
    return mod, client


def test_the_settings_page_redacts_every_stored_error(fixture_db, monkeypatch):
    mod, client = _settings(monkeypatch, fixture_db)

    panels = client.get("/settings/status").get_json()["panels"]
    # A settings action whose failure text (str(exc)) is shown on the page.
    def fail_claim(_source):
        raise RuntimeError(POST_ERROR)

    monkeypatch.setattr(mod, "claim_preparation", fail_claim)
    page = client.post("/settings", data={
        "action": "prepare_lumae", "catalog_instance_id": SOURCE, "server_id": SERVER,
    }).get_data(as_text=True)

    for body in ("".join(panels.values()), page):
        for secret in (*SECRETS, *SITE_SECRETS):
            assert secret not in body, secret
        # Each site rendered, readable around the masks.
        assert "The last relationship build failed." in body
        assert "password=[redacted] for db" in body
        assert "Authorization: [redacted]" in body
        assert "Volume and ramp preparation: lookup https://api.example.com/v1?api_key=" \
            "[redacted] failed reading [path]: No such file" in body
        assert "https://[redacted]@proxy.local:3128 refused" in body
    assert "provider rejected token=[redacted]" in page


def test_the_settings_page_shows_a_safe_code_unchanged(fixture_db, monkeypatch):
    with fixture_db.cursor() as cur:
        cur.execute(f"UPDATE {P}profile_backfill_state SET last_error='queue_unavailable'")
    fixture_db.commit()
    _mod, client = _settings(monkeypatch, fixture_db, reconcile_error="analysis_timeout")

    body = "".join(client.get("/settings/status").get_json()["panels"].values())

    assert "Volume and ramp preparation: queue_unavailable</p>" in body
    assert '<div class="lumae-help">analysis_timeout</div>' in body


def _health(monkeypatch, db):
    mod, client = _route(monkeypatch, db)
    monkeypatch.setattr(mod, "detect_core", lambda: V2)
    monkeypatch.setattr(mod, "ProviderCatalogBridge", lambda: types.SimpleNamespace(
        list_servers=lambda: []
    ))
    return mod, client


def test_catalogue_health_redacts_stored_errors_and_keeps_the_shape(fixture_db, monkeypatch):
    db = fixture_db
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}provider_identity_transitions
                (catalog_instance_id, state, detection_reason, required_action, last_error)
                VALUES (%s, 'normal', 'provider_version_unchanged', 'none', %s)""",
            (SOURCE, TRANSITION_ERROR),
        )
    db.commit()
    _mod, client = _health(monkeypatch, db)

    response = client.get("/api/catalog/health")

    assert response.status_code == 200
    text = response.get_data(as_text=True)
    for secret in (*SECRETS, "TransPass77"):
        assert secret not in text, secret
    server = response.get_json()["servers"][0]
    assert server["catalog"]["last_error"] == (
        "refresh failed: postgresql://[redacted]@db.internal:5432/audiomuse"
    )
    assert server["analysis"]["last_error"].endswith("&t=[redacted]&s=[redacted]&v=1.16.1")
    assert server["preparation"]["last_error"] == (
        "provider said 401 to Authorization: [redacted]"
    )
    transition = server["provider_identity_transition"]
    assert transition["last_error"] == (
        "provider recheck failed: PGPASSWORD=[redacted] psql exited 2"
    )
    # Structured codes are untouched.
    assert transition["detection_reason"] == "provider_version_unchanged"
    assert transition["required_action"] == "none"
    assert server["preparation"]["status"] == "failed"
    assert server["preparation"]["phase"] == "catalog_refresh"


def test_catalogue_health_keeps_a_safe_code_and_an_empty_error(fixture_db, monkeypatch):
    with fixture_db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_state SET last_error=''")
        cur.execute(f"UPDATE {P}analysis_state SET last_error=NULL")
        cur.execute(f"UPDATE {P}preparation_state SET last_error='queue_unavailable'")
    fixture_db.commit()
    _mod, client = _health(monkeypatch, fixture_db)

    server = client.get("/api/catalog/health").get_json()["servers"][0]

    # The field keeps its type: a string stays a string, null stays null.
    assert server["catalog"]["last_error"] == ""
    assert server["analysis"]["last_error"] is None
    assert server["preparation"]["last_error"] == "queue_unavailable"


def test_catalogue_health_redacts_a_server_discovery_failure(fixture_db, monkeypatch):
    mod, client = _health(monkeypatch, fixture_db)

    def fail(_compatibility):
        raise RuntimeError(DISCOVERY_ERROR)

    monkeypatch.setattr(mod, "sanitized_server_summaries", fail)

    response = client.get("/api/catalog/health")

    assert response.status_code == 503
    payload = response.get_json()
    assert payload["status"] == "server_discovery_failed"
    assert payload["reason"] == "list_servers failed: Authorization: [redacted]"


def test_the_preparation_status_redacts_its_stored_error(fixture_db, monkeypatch):
    _mod, client = _route(monkeypatch, fixture_db)

    response = client.get(f"/api/catalog/prepare/{SOURCE}")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["last_error"] == "provider said 401 to Authorization: [redacted]"
    assert "eyJhbGciOiJIUzI1NiJ9" not in response.get_data(as_text=True)


def test_the_timeout_setting_is_read_before_any_savepoint(fixture_db, monkeypatch):
    connection = RecordingConnection(fixture_db)

    def get_setting(key, default=None):
        connection.statements.append(f"GET_SETTING {key}")
        return "12000"

    monkeypatch.setattr(database_state, "get_setting", get_setting)
    _mod, client = _route(monkeypatch, connection)

    assert client.get("/database-state").status_code == 200

    statements = connection.statements
    lookups = [index for index, sql in enumerate(statements) if sql.startswith("GET_SETTING")]
    # Once for source resolution and once for the snapshot, not per read.
    assert [statements[index] for index in lookups] == [
        "GET_SETTING diagnostic_statement_timeout_ms"
    ] * 2
    for index in lookups:
        opened = sum(1 for sql in statements[:index] if sql.startswith("SAVEPOINT"))
        released = sum(1 for sql in statements[:index] if sql.startswith("RELEASE"))
        assert opened == released, "a setting lookup ran inside a bounded read"
    timeouts = [sql for sql in statements if sql.startswith("SET LOCAL")]
    assert timeouts and set(timeouts) == {"SET LOCAL statement_timeout = 12000"}
