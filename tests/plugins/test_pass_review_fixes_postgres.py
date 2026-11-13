"""Pass review fixes on the real migrated schema: a repeated ``/api/analyze``
keeps the on-demand attempt a runner is analysing, an explicit waveform
batch limit is honoured, helpers skip a source cooling down, relationship
``waiting_for_inputs`` keeps its retry, and the state readers return real UTC
whatever the session's time zone."""
from datetime import datetime, timedelta, timezone

import pytest

from test_lumae_analysis import load_plugin, plugin_client
from test_reconcile_scheduler_postgres import P, SERVER, SOURCE, _track  # noqa: F401
from plugins.LumaeAnalysis import profile_publication as publication
from plugins.LumaeAnalysis import reconcile, runner


@pytest.fixture
def source_db(migrated_db, monkeypatch):
    mod = load_plugin()
    db = migrated_db
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
            "provider_type, server_name, is_default, rebind_status) "
            "VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')", (SOURCE, SERVER))
        cur.execute(
            f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, "
            "provider_type, published_generation, catalog_epoch, status) "
            "VALUES (%s, %s, 'navidrome', 1, 'epoch-a', 'complete')", (SOURCE, SERVER))
    db.commit()
    for module in (mod, reconcile):
        monkeypatch.setattr(module, "get_db", lambda: db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_k: {"catalog_instance_id": SOURCE, "server_id": SERVER})
    return mod, db


def _attempt(db, track):
    with db.cursor() as cur:
        cur.execute(f"SELECT status, last_error, attempt_token FROM {P}source_profiles "
                    "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, track))
        row = cur.fetchone()
    db.commit()
    return row


def _claimed(db, track, role):
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}source_profiles SET last_error=%s "
                    "WHERE catalog_instance_id=%s AND track_id=%s",
                    (reconcile.RUNNER_CLAIM_MARKERS[role], SOURCE, track))
    db.commit()


def _heartbeat(db, role, age_seconds):
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {runner.state_table()} (role, heartbeat_at) "
                    "VALUES (%s, now() - make_interval(secs => %s)) "
                    "ON CONFLICT (role) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at",
                    (role, age_seconds))
    db.commit()


# --- 1 ------------------------------------------------------------------------

def test_a_repeated_analyze_keeps_the_attempt_a_live_runner_is_analysing(source_db):
    mod, db = source_db
    _track(db, "t1")
    token = publication.admit_attempts(db, SOURCE, ["t1"], priority="interactive")["t1"]
    _claimed(db, "t1", runner.ROLE_WORKER)
    _heartbeat(db, runner.ROLE_WORKER, 5)
    response = plugin_client(mod).post("/api/analyze", json={"ids": ["t1"]})
    assert response.status_code == 202 and response.get_json()["already_pending"] == ["t1"]
    assert _attempt(db, "t1") == ("pending_interactive",
                                  reconcile.RUNNER_CLAIM_MARKERS[runner.ROLE_WORKER], token)


def test_a_repeated_analyze_rewaits_a_claim_whose_runner_went_away(source_db):
    mod, db = source_db
    _track(db, "t1")
    token = publication.admit_attempts(db, SOURCE, ["t1"], priority="interactive")["t1"]
    _claimed(db, "t1", runner.ROLE_WORKER)
    _heartbeat(db, runner.ROLE_WORKER, 600)
    assert plugin_client(mod).post("/api/analyze", json={"ids": ["t1"]}).status_code == 202
    assert _attempt(db, "t1") == ("pending_interactive", reconcile.RUNNER_WAITING_MARKER, token)


def test_a_repeated_analyze_still_promotes_a_background_attempt(source_db):
    mod, db = source_db
    _track(db, "t1")
    old = publication.admit_attempts(db, SOURCE, ["t1"])["t1"]
    assert plugin_client(mod).post("/api/analyze", json={"ids": ["t1"]}).status_code == 202
    status, marker, token = _attempt(db, "t1")
    assert (status, marker) == ("pending_interactive", reconcile.RUNNER_WAITING_MARKER)
    assert token != old


# --- 5, 7 ---------------------------------------------------------------------

def test_an_explicit_waveform_batch_limit_is_honoured(source_db, monkeypatch):
    mod, db = source_db
    for index in range(25):
        _track(db, f"t{index:02d}")
    assert len(mod.find_backfill_ids(25, catalog_instance_id=SOURCE, server_id=SERVER)) == 25
    monkeypatch.setattr(mod, "get_setting",
                        lambda key, default=None: 500 if key == "backfill_batch_size" else default)
    assert mod.configured_backfill_limit() == mod.MAX_BACKFILL_BATCH_SIZE


def test_a_helper_skips_a_waveform_source_cooling_down(source_db):
    mod, db = source_db
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}profile_backfill_state (catalog_instance_id, server_id, status, "
                    "next_retry_at) VALUES (%s, %s, 'queued', now() + interval '5 minutes')",
                    (SOURCE, SERVER))
    db.commit()
    assert mod._next_shared_source(db, "profile_backfill") is None
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}profile_backfill_state SET next_retry_at=now() - interval '1 second'")
    db.commit()
    assert mod._next_shared_source(db, "profile_backfill") == (SERVER, SOURCE)


# --- 12 -----------------------------------------------------------------------

def _relationship_waiting(db, seconds):
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}relationship_state (catalog_instance_id, epoch, status, "
                    "next_retry_at) VALUES (%s, 'e', 'waiting_for_inputs', "
                    "now() + make_interval(secs => %s))", (SOURCE, seconds))
    db.commit()


def test_a_relationship_build_waiting_for_inputs_runs_at_its_retry(source_db):
    mod, db = source_db
    _relationship_waiting(db, 300)
    assert mod.next_relationship_run(db) is None
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}relationship_state SET next_retry_at=now() - interval '1 second'")
    db.commit()
    assert mod.next_relationship_run(db)[:2] == (SERVER, SOURCE)


def test_waiting_for_inputs_is_deferred_and_keeps_its_retry(source_db):
    mod, db = source_db
    _relationship_waiting(db, 300)
    with db.cursor() as cur:
        cur.execute(f"SELECT next_retry_at FROM {P}relationship_state")
        stored = cur.fetchone()[0]
    db.commit()
    result = mod._run_reconcile_action(
        db, "relationships", SERVER, SOURCE, None, 0,
        lambda *_a, **_k: {"catalog_instance_id": SOURCE, "status": "waiting_for_inputs"})
    assert result["status"] == "waiting_for_inputs"
    with db.cursor() as cur:
        cur.execute(f"SELECT status, next_retry_at FROM {P}relationship_state")
        assert cur.fetchone() == ("waiting_for_inputs", stored)
        cur.execute(f"SELECT status FROM {reconcile.events_table()} WHERE action='relationships'")
        assert [row[0] for row in cur.fetchall()] == ["deferred"]
    db.commit()


# --- 11 -----------------------------------------------------------------------

def test_state_times_are_real_utc_in_a_non_utc_session(source_db):
    mod, db = source_db
    with db.cursor() as cur:
        cur.execute("SET TIME ZONE 'Asia/Tokyo'")
        cur.execute(f"INSERT INTO {P}profile_backfill_state (catalog_instance_id, server_id, status, "
                    "started_at, updated_at, next_retry_at) "
                    "VALUES (%s, %s, 'running', now(), now(), now() + interval '1 minute')",
                    (SOURCE, SERVER))
        cur.execute(f"INSERT INTO {P}preparation_state (catalog_instance_id, server_id, status, phase, "
                    "started_at, updated_at) VALUES (%s, %s, 'running', 'catalog_refresh', now(), now())",
                    (SOURCE, SERVER))
    db.commit()
    now = datetime.now(timezone.utc)
    for state in (mod.profile_backfill_state(SOURCE, db=db), mod.preparation_state(SOURCE, db=db)):
        updated = datetime.fromisoformat(state["updated_at"].replace("Z", "+00:00"))
        assert state["updated_at"].endswith("Z")
        assert abs(updated - now) < timedelta(minutes=1)
    assert mod.profile_backfill_is_active(mod.profile_backfill_state(SOURCE, db=db))
    assert mod.preparation_is_active(mod.preparation_state(SOURCE, db=db))
    retry = mod.profile_backfill_state(SOURCE, db=db)["next_retry_at"]
    assert mod._future_retry_at(retry) == retry
    # A long-dead job does not read as active either.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}profile_backfill_state SET updated_at=now() - interval '3 hours'")
    db.commit()
    assert not mod.profile_backfill_is_active(mod.profile_backfill_state(SOURCE, db=db))


# --- the download breaker's canary and back-off ----------------------------------

def _published(db, track):
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}published_source_profiles (catalog_instance_id, track_id, sample_rate, "
                    "duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, "
                    "media_signature, analyzed_at) "
                    "VALUES (%s, %s, 44100, 240000, -14, '\\x00', '\\x00', 1, 1, 'sig', now())",
                    (SOURCE, track))
    db.commit()


def _canaries(mod, db, exclude, times=40):
    found = {mod._canary_track(db, SOURCE, exclude) for _ in range(times)}
    db.commit()
    return found


def test_the_canary_is_a_track_with_a_published_waveform_never_a_held_one(source_db):
    mod, db = source_db
    # Ids spread over the random start's hex range, so both are drawn.
    for track in ("2a", "8a", "4b", "c4"):
        _track(db, track)
    _published(db, "4b")
    _published(db, "c4")
    assert _canaries(mod, db, ["2a"]) == {"4b", "c4"}  # a published one first, from a random start
    assert _canaries(mod, db, ["4b"]) == {"c4"}


def test_without_a_published_waveform_the_canary_comes_from_the_catalogue(source_db):
    mod, db = source_db
    for track in ("t1", "t2", "t3", "t4", "t5"):
        _track(db, track)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET available=FALSE WHERE track_id='t3'")
        # Another generation's row is not the published catalogue.
        cur.execute(f"UPDATE {P}catalog_tracks SET published_generation=2 WHERE track_id='t4'")
        # A podcast or video item: available, but no pass analyses it.
        cur.execute(f"UPDATE {P}catalog_tracks SET analysis_eligible=FALSE WHERE track_id='t5'")
    db.commit()
    assert _canaries(mod, db, ["t1"]) == {"t2"}


def test_a_catalogue_of_only_held_tracks_has_no_canary(source_db):
    mod, db = source_db
    for track in ("t1", "t2", "t3"):
        _track(db, track)
    _published(db, "t2")
    assert _canaries(mod, db, ["t1", "t2", "t3"], times=3) == {None}


def _pass_state(db, state_table, retry_sql):
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {P}{state_table}")
        cur.execute(f"INSERT INTO {P}{state_table} (catalog_instance_id, status, next_retry_at) "
                    f"VALUES (%s, 'running', {retry_sql})", (SOURCE,))
    db.commit()


def _retry_in(db, state_table):
    with db.cursor() as cur:
        cur.execute(f"SELECT status, next_retry_at = 'infinity', "
                    f"CASE WHEN next_retry_at <> 'infinity' "
                    f"THEN EXTRACT(EPOCH FROM next_retry_at - LOCALTIMESTAMP) END FROM {P}{state_table}")
        row = cur.fetchone()
    db.commit()
    return row


@pytest.mark.parametrize("state_table", ["loudness_backfill_state", "edge_backfill_state"])
def test_a_tripped_pass_backs_off_a_minute_and_keeps_a_later_retry(source_db, state_table):
    mod, db = source_db
    limit = mod.BACKFILL_ABORT_RETRY_SECONDS
    _pass_state(db, state_table, "NULL")
    mod._back_off_pass(db, state_table, SOURCE)
    status, infinite, seconds = _retry_in(db, state_table)
    assert status == "running" and not infinite and limit - 5 < float(seconds) <= limit
    _pass_state(db, state_table, "LOCALTIMESTAMP + interval '1 hour'")
    mod._back_off_pass(db, state_table, SOURCE)
    status, infinite, seconds = _retry_in(db, state_table)
    assert status == "running" and 3500 < float(seconds) <= 3600
    _pass_state(db, state_table, "'infinity'")
    mod._back_off_pass(db, state_table, SOURCE)
    assert _retry_in(db, state_table)[:2] == ("running", True)
