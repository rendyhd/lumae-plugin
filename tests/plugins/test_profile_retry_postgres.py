"""Real PostgreSQL retry lifecycle regressions."""
import pytest

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, load_plugin
from test_profile_publication_postgres import _result, _state, _track
from plugins.LumaeAnalysis import profile_publication as publication

SOURCE = "catalog-a"
PROFILES = "plugin_lumae_analysis__source_profiles"


def row(db, track):
    with db.cursor() as cur:
        cur.execute(
            f"SELECT status,last_error,retry_category,retry_count,retry_after,"
            f"retry_media_signature,attempt_token FROM {PROFILES} "
            "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, track),
        )
        return cur.fetchone()


def fail(db, track, code="analysis_error", status="failed"):
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    assert publication.complete_attempt(
        db, SOURCE, track, token, object(), status, "PRIVATE_PATH secret",
        None, 1, 1, failure_code=code,
    )
    return row(db, track)


def eligible(track):
    return track in load_plugin().find_backfill_ids(
        25, catalog_instance_id=SOURCE, server_id="server-a",
    )


def test_transient_failure_is_safe_and_waits_for_due_time(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-transient")
    state = fail(db, "retry-transient", "download_unavailable")
    assert state[:4] == ("failed", "download_unavailable", "download_unavailable", 1)
    assert state[4] is not None and state[5] == "catalog-media:revision-a"
    assert not eligible("retry-transient")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET retry_after=now()-interval '1 second' WHERE track_id='retry-transient'")
    db.commit()
    assert eligible("retry-transient")


def test_transient_exhaustion_and_revision_repair(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-exhaust")
    for count in range(1, 4):
        assert fail(db, "retry-exhaust")[3] == count
    assert row(db, "retry-exhaust")[4] is None
    assert not eligible("retry-exhaust")
    _track(db, "retry-exhaust", "revision-b")
    assert eligible("retry-exhaust")
    token = publication.admit_attempts(db, SOURCE, ["retry-exhaust"])["retry-exhaust"]
    assert row(db, "retry-exhaust")[3] == 0
    assert publication.complete_attempt(db, SOURCE, "retry-exhaust", token,
        _result(), "ready", None, "catalog-media:revision-b", 1, 1)
    assert row(db, "retry-exhaust")[:5] == ("ready", None, None, 0, None)


@pytest.mark.parametrize("field", ["retry_analyzer_ver", "retry_profile_schema_ver"])
def test_new_contract_reenables_exhausted_failure(edge_publication_db, field):
    db = edge_publication_db
    _track(db, "retry-contract")
    for _ in range(3):
        fail(db, "retry-contract")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET {field}=0 WHERE track_id='retry-contract'")
    db.commit()
    assert eligible("retry-contract")


@pytest.mark.parametrize("code", ["silent_audio", "unsupported_media", "resource_limit"])
def test_permanent_media_failure_waits_for_revision(edge_publication_db, code):
    db = edge_publication_db
    track = "retry-" + code
    _track(db, track)
    assert fail(db, track, code)[2:5] == (code, 1, None)
    assert not eligible(track)
    _track(db, track, "revision-b")
    assert eligible(track)


def test_missing_file_has_bounded_retry(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-missing")
    state = fail(db, "retry-missing", "media_unavailable", "skipped_no_file")
    assert state[4] is not None and not eligible("retry-missing")
    _track(db, "retry-missing", "revision-b")
    assert eligible("retry-missing")


def test_failure_preserves_published_baseline(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-baseline")
    token = publication.admit_attempts(db, SOURCE, ["retry-baseline"])["retry-baseline"]
    assert publication.complete_attempt(db, SOURCE, "retry-baseline", token,
        _result(), "ready", None, "catalog-media:revision-a", 1, 1)
    baseline = _state(db, "retry-baseline")
    fail(db, "retry-baseline", "analysis_timeout")
    assert _state(db, "retry-baseline") == baseline


def test_queue_release_is_fenced_and_cools_down(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-queue")
    old = publication.admit_attempts(db, SOURCE, ["retry-queue"])["retry-queue"]
    current = publication.admit_attempts(db, SOURCE, ["retry-queue"])["retry-queue"]
    assert publication.release_attempts(db, SOURCE, {"retry-queue": old}, "secret") == 0
    assert row(db, "retry-queue")[6] == current
    assert publication.release_attempts(db, SOURCE, {"retry-queue": current}, "secret") == 1
    assert row(db, "retry-queue")[:4] == ("stale", "queue_unavailable", "queue_unavailable", 1)
    assert not eligible("retry-queue")


def test_pending_recovery_cools_down(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-recovery")
    publication.admit_attempts(db, SOURCE, ["retry-recovery"])
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET analyzed_at=now()-interval '2 days' WHERE track_id='retry-recovery'")
    db.commit()
    assert load_plugin().recover_stale_pending_profiles(SOURCE) == 1
    assert row(db, "retry-recovery")[:4] == ("stale", "queue_unavailable", "queue_unavailable", 1)
    assert not eligible("retry-recovery")


def test_old_completion_cannot_change_retry_count(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-fence")
    old = publication.admit_attempts(db, SOURCE, ["retry-fence"])["retry-fence"]
    current = publication.admit_attempts(db, SOURCE, ["retry-fence"])["retry-fence"]
    assert not publication.complete_attempt(db, SOURCE, "retry-fence", old,
        object(), "failed", "secret", None, 1, 1, failure_code="analysis_error")
    assert row(db, "retry-fence")[3] == 0
    assert row(db, "retry-fence")[6] == current


def test_missing_revision_defers_until_known(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-deferred", None)
    assert publication.admit_attempts(db, SOURCE, ["retry-deferred"]) == {}
    assert not eligible("retry-deferred")
    _track(db, "retry-deferred", "revision-a")
    assert eligible("retry-deferred")


def test_due_selection_skips_cooling_row_before_limit(edge_publication_db):
    db = edge_publication_db
    _track(db, "a-cooling")
    _track(db, "b-due")
    fail(db, "a-cooling")
    fail(db, "b-due")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET retry_after=now()-interval '1 second' WHERE track_id='b-due'")
    db.commit()
    assert load_plugin().find_backfill_ids(1, catalog_instance_id=SOURCE,
        server_id="server-a") == ["b-due"]


@pytest.mark.parametrize("error,code", [
    (ValueError("PRIVATE_PATH invalid"), "unsupported_media"),
    (RuntimeError("PRIVATE_PATH unexpected"), "analysis_error"),
])
def test_worker_classifies_exceptions_without_leaking_text(edge_publication_db, monkeypatch, error, code):
    db = edge_publication_db
    track = "retry-exception-" + code
    _track(db, track)
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    mod = load_plugin()
    monkeypatch.setattr(mod, "load_track_file", lambda *args, **kwargs: {
        "file_path": "unused.flac", "media_signature": "catalog-media:revision-a",
        "cleanup_path": None,
    })
    monkeypatch.setattr(mod, "analyze_file", lambda path: (_ for _ in ()).throw(error))
    assert mod.analyze_one_track(track, SOURCE, "server-a", token)["status"] == "failed"
    state = row(db, track)
    assert state[1:3] == (code, code)
    assert "PRIVATE_PATH" not in str(state)

def test_aged_interactive_claim_recovers(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-interactive")
    publication.admit_attempts(db, SOURCE, ["retry-interactive"], priority="interactive")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET analyzed_at=now()-interval '2 days' WHERE track_id='retry-interactive'")
    db.commit()
    assert load_plugin().recover_stale_pending_profiles(SOURCE) == 1
    assert row(db, "retry-interactive")[:4] == (
        "stale", "queue_unavailable", "queue_unavailable", 1,
    )
    assert not eligible("retry-interactive")


def test_delayed_retry_rearms_and_wakes_durable_backfill(edge_publication_db):
    db = edge_publication_db
    _track(db, "retry-wakeup")
    fail(db, "retry-wakeup")
    with db.cursor() as cur:
        cur.execute("""CREATE TABLE plugin_lumae_analysis__profile_backfill_state (
            catalog_instance_id TEXT PRIMARY KEY, server_id TEXT, status TEXT,
            processed_profiles INTEGER DEFAULT 0, queued_profiles INTEGER DEFAULT 0,
            last_error TEXT, started_at TIMESTAMP, completed_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now(), retry_count INTEGER DEFAULT 0,
            next_retry_at TIMESTAMPTZ, refresh_wake_pending BOOLEAN NOT NULL DEFAULT FALSE)""")
        cur.execute("""INSERT INTO plugin_lumae_analysis__profile_backfill_state
            (catalog_instance_id,server_id,status) VALUES ('catalog-a','server-a','queued')""")
    db.commit()
    mod = load_plugin()
    assert mod.profile_backfill_task("server-a", SOURCE)["status"] == "waiting_retry"
    with db.cursor() as cur:
        cur.execute("SELECT status,next_retry_at FROM plugin_lumae_analysis__profile_backfill_state WHERE catalog_instance_id=%s", (SOURCE,))
        state = cur.fetchone()
    assert state[0] == "queued" and state[1] is not None
    assert mod.next_profile_backfill_run(db=db) is None
    with db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__profile_backfill_state SET next_retry_at=now()-interval '1 second' WHERE catalog_instance_id=%s", (SOURCE,))
        cur.execute(f"UPDATE {PROFILES} SET retry_after=now()-interval '1 second' WHERE track_id='retry-wakeup'")
    db.commit()
    assert mod.next_profile_backfill_run(db=db)[:2] == ("server-a", SOURCE)
    assert eligible("retry-wakeup")

def _backfill_state(db, status="complete"):
    with db.cursor() as cur:
        cur.execute("""CREATE TABLE plugin_lumae_analysis__profile_backfill_state (
            catalog_instance_id TEXT PRIMARY KEY, server_id TEXT, status TEXT,
            processed_profiles INTEGER DEFAULT 0, queued_profiles INTEGER DEFAULT 0,
            last_error TEXT, started_at TIMESTAMP, completed_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now(), retry_count INTEGER DEFAULT 0,
            next_retry_at TIMESTAMPTZ,
            refresh_wake_pending BOOLEAN NOT NULL DEFAULT FALSE)""")
        cur.execute("INSERT INTO plugin_lumae_analysis__profile_backfill_state (catalog_instance_id,server_id,status) VALUES (%s,%s,%s)", (SOURCE,"server-a",status))
    db.commit()


def test_catalog_revision_rearms_completed_repair(edge_publication_db, monkeypatch):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "retry-catalog-repair")
    for _ in range(3):
        fail(db, "retry-catalog-repair", "unsupported_media")
    _backfill_state(db)
    _track(db, "retry-catalog-repair", "revision-b")
    assert eligible("retry-catalog-repair")
    monkeypatch.setattr(mod, "arm_reconcile", lambda *_args: None)
    assert mod.wake_profile_backfill_after_catalog_refresh({
        "catalog_instance_id": SOURCE, "server_id": "server-a", "changes": 1,
    }, db=db)["queued"]
    assert mod.next_profile_backfill_run(db=db)[:2] == ("server-a", SOURCE)


def test_running_batch_keeps_concurrent_catalog_wake(edge_publication_db, monkeypatch):
    db = edge_publication_db
    mod = load_plugin()
    _backfill_state(db, "running")
    monkeypatch.setattr(mod, "arm_reconcile", lambda *_args: None)
    assert mod.wake_profile_backfill_after_catalog_refresh({
        "catalog_instance_id": SOURCE, "server_id": "server-a", "changes": 1,
    }, db=db) is True
    mod.update_profile_backfill_state(SOURCE, "server-a", "complete", completed=True)
    with db.cursor() as cur:
        cur.execute("SELECT status,refresh_wake_pending,next_retry_at FROM plugin_lumae_analysis__profile_backfill_state WHERE catalog_instance_id=%s", (SOURCE,))
        assert cur.fetchone() == ("queued", False, None)
    assert mod.next_profile_backfill_run(db=db)[:2] == ("server-a", SOURCE)


def test_due_boundary_cannot_complete_workflow(edge_publication_db, monkeypatch):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "retry-boundary")
    fail(db, "retry-boundary")
    _backfill_state(db, "queued")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET retry_after=now()-interval '1 second' WHERE track_id='retry-boundary'")
    db.commit()
    monkeypatch.setattr(mod, "find_backfill_ids", lambda *args, **kwargs: [])
    assert mod.profile_backfill_task("server-a", SOURCE)["status"] == "waiting_retry"
    assert mod.next_profile_backfill_run(db=db)[:2] == ("server-a", SOURCE)

def test_due_retry_runs_through_claim_and_publication(edge_publication_db, monkeypatch):
    db = edge_publication_db
    mod = load_plugin()
    monkeypatch.setattr(mod, "arm_reconcile", lambda *_args: None)
    _track(db, "retry-actual")
    fail(db, "retry-actual")
    _backfill_state(db, "queued")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET retry_after=now()-interval '1 second' WHERE track_id='retry-actual'")
    db.commit()
    seen = {}

    def analyze(ids, **kwargs):
        token = kwargs["attempt_tokens"]["retry-actual"]
        seen["ids"] = ids
        seen["token"] = token
        assert publication.complete_attempt(db, SOURCE, "retry-actual", token,
            _result(), "ready", None, "catalog-media:revision-a", 1, 1)
        return {"attempted": 1, "ready": 1, "failed": 0, "skipped": 0}

    monkeypatch.setattr(mod, "analyze_tracks_task", analyze)
    assert mod.next_profile_backfill_run(db=db)[:2] == ("server-a", SOURCE)
    result = mod.profile_backfill_task("server-a", SOURCE)
    assert result["processed"] == 1 and seen["ids"] == ["retry-actual"]
    assert row(db, "retry-actual")[:5] == ("ready", None, None, 0, None)
    assert _state(db, "retry-actual")[0] == 1

def test_removed_track_does_not_rearm_cooling_retry(edge_publication_db):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "retry-removed")
    fail(db, "retry-removed")
    with db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__catalog_tracks SET available=FALSE WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, "retry-removed"))
    db.commit()
    assert mod.next_profile_retry_at(SOURCE, db=db) is None

def test_interactive_only_claim_arms_stale_recovery(edge_publication_db, monkeypatch):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "retry-hook-crash")
    _backfill_state(db, "complete")
    monkeypatch.setattr(mod, "arm_reconcile", lambda *_args: None)
    token = mod.mark_pending(["retry-hook-crash"], SOURCE, priority="interactive")["retry-hook-crash"]
    with db.cursor() as cur:
        cur.execute("SELECT status,next_retry_at FROM plugin_lumae_analysis__profile_backfill_state WHERE catalog_instance_id=%s", (SOURCE,))
        state = cur.fetchone()
    assert state[0] == "queued" and state[1] is not None
    assert mod.next_profile_backfill_run(db=db) is None
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET analyzed_at=now()-interval '2 hours' WHERE track_id='retry-hook-crash'")
        cur.execute("UPDATE plugin_lumae_analysis__profile_backfill_state SET next_retry_at=now()-interval '1 second' WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    assert mod.next_profile_backfill_run(db=db)[:2] == ("server-a", SOURCE)
    assert mod.profile_backfill_task("server-a", SOURCE)["status"] == "waiting_retry"
    assert row(db, "retry-hook-crash")[:4] == (
        "stale", "queue_unavailable", "queue_unavailable", 1,
    )
    assert not publication.complete_attempt(db, SOURCE, "retry-hook-crash", token,
        _result(), "ready", None, "catalog-media:revision-a", 1, 1)


def test_crashed_batch_at_30_minutes_keeps_one_hour_claim_timer(edge_publication_db):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "retry-half-stale")
    publication.admit_attempts(db, SOURCE, ["retry-half-stale"])
    _backfill_state(db, "running")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET analyzed_at=now()-interval '31 minutes' WHERE track_id='retry-half-stale'")
        cur.execute("UPDATE plugin_lumae_analysis__profile_backfill_state SET updated_at=now()-interval '31 minutes' WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    assert mod.next_profile_backfill_run(db=db)[:2] == ("server-a", SOURCE)
    assert mod.profile_backfill_task("server-a", SOURCE)["status"] == "waiting_retry"
    assert row(db, "retry-half-stale")[0] == "pending"
    with db.cursor() as cur:
        cur.execute("SELECT status,next_retry_at FROM plugin_lumae_analysis__profile_backfill_state WHERE catalog_instance_id=%s", (SOURCE,))
        state = cur.fetchone()
    assert state[0] == "queued" and state[1] is not None

def test_no_change_refresh_rechecks_stranded_repair(monkeypatch):
    mod = load_plugin()
    seen = []
    class Db:
        def commit(self):
            pass
    db = Db()
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "get_core_adapter", lambda: object())
    monkeypatch.setattr(mod, "_resolve_task_server_id", lambda adapter, server_id: "server-a")
    result = {"catalog_instance_id": SOURCE, "server_id": "server-a",
              "changes": 0, "change_reason": "no_change"}
    monkeypatch.setattr(mod, "refresh_catalog", lambda **kwargs: result)
    monkeypatch.setattr(mod, "find_backfill_ids", lambda *args, **kwargs: ["repaired"])
    monkeypatch.setattr(mod, "wake_profile_backfill_after_catalog_refresh",
                        lambda value: seen.append(value))
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod.optional_storage, "prune", lambda value: None)
    assert mod.catalog_refresh_task("server-a") == result
    assert seen == [result]

def test_recovery_arm_failure_rolls_back_token_admission(edge_publication_db, monkeypatch):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "retry-atomic-claim")
    def broken_arm(*args, **kwargs):
        raise RuntimeError("reconcile unavailable")
    monkeypatch.setattr(mod, "arm_profile_claim_recovery", broken_arm)
    with pytest.raises(RuntimeError, match="reconcile unavailable"):
        mod.mark_pending(["retry-atomic-claim"], SOURCE, priority="interactive")
    assert row(db, "retry-atomic-claim") is None

def test_inactive_source_does_not_rearm_cooling_retry(edge_publication_db):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "retry-inactive")
    fail(db, "retry-inactive")
    with db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__catalog_sources SET rebind_status='quarantined' WHERE catalog_instance_id=%s", (SOURCE,))
    db.commit()
    assert mod.next_profile_retry_at(SOURCE, db=db) is None
