"""On-demand priority, saved refused requests and the server edge pass.

Plan: docs/plan/EDGE_PRIORITY_AND_VECTORS_PLAN.md (Phases 1-3).

* An on-demand edge request promotes a job a background batch already holds.
* An on-demand request is recorded as waiting for the runners, wakes them and
  is served first in their next pass (runner plan R6); 1.8.0's refused
  requests are served the same way. A background request makes the library
  pass due.
* A queued background batch yields after its budget; unsupported media is
  terminal for its revision.
* Workers report their edge runtime, which decides ``analyzable``; the web
  process no longer needs PyAV to queue work.
* The library pass walks in small resumable slices.
* Credits and metadata yield to source-scoped on-demand work.
"""
import time

import pytest

from test_lumae_analysis import (  # noqa: F401  (fixtures)
    edge_publication_db,
    load_plugin,
    lumae_postgres_db,
    plugin_client,
)

JOBS = "plugin_lumae_analysis__edge_profile_jobs"
PROFILES = "plugin_lumae_analysis__source_profiles"
STATE = "plugin_lumae_analysis__edge_backfill_state"


def _q(db, sql, params=()):
    cur = db.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall() if cur.description else []
    cur.close()
    db.commit()
    return rows


def _store():
    from plugins.LumaeAnalysis import edge_profile_store

    return edge_profile_store


def _wakes(mod, monkeypatch):
    """The reasons the plugin wakes the runners with; the wake still runs."""
    woken, real = [], mod.wake_runners

    def wake(db, reason="work_admitted", commit=False):
        woken.append(reason)
        real(db, reason, commit=commit)

    monkeypatch.setattr(mod, "wake_runners", wake)
    return woken


def test_interactive_request_promotes_a_queued_background_job(edge_publication_db):
    store, db = _store(), edge_publication_db
    jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a"])
    assert len(jobs) == 1

    promoted, ready, pending, unsupported = store.claim_edge_requests(
        db, "catalog-a", ["track-a"], "interactive"
    )

    assert (ready, pending, unsupported) == ([], ["track-a"], [])
    assert [job["job_token"] for job in promoted] == [jobs[0]["job_token"]]
    assert _q(db, f"SELECT status, priority FROM {JOBS}") == [("pending", "interactive")]
    # A background request neither re-queues nor promotes a pending job.
    assert store.claim_edge_requests(db, "catalog-a", ["track-a"])[0] == []


def test_on_demand_edge_request_waits_for_the_runner_and_is_served_next(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)
    woken = _wakes(mod, monkeypatch)

    response = plugin_client(mod).post(
        "/api/profiles/edges/analyze", json={"catalog_instance_id": "catalog-a", "ids": ["track-a"]}
    )

    assert response.status_code == 202
    body = response.get_json()
    assert body["accepted"] == ["track-a"] and body["deferred"] == []
    assert woken == ["interactive_edges"]
    assert _q(db, f"SELECT status, priority, last_error FROM {JOBS}") == [
        ("pending", "interactive", mod.RUNNER_WAITING_MARKER)
    ]

    served = []
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(
        mod, "analyze_edges_task",
        lambda jobs, source, server, priority="background", **_kw:
            served.append((source, server, priority, [job["track_id"] for job in jobs])) or [],
    )
    assert mod.serve_interactive() == 1
    assert served == [("catalog-a", "server-a", "interactive", ["track-a"])]
    assert _q(db, f"SELECT last_error FROM {JOBS}") == [(None,)]
    assert mod.serve_interactive() == 0


@pytest.mark.parametrize("status", ["complete", "waiting_runtime", "queued"])
def test_background_edge_request_makes_the_library_pass_due(edge_publication_db, monkeypatch, status):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)
    store.ensure_edge_backfill_sources(db)
    _q(db, f"UPDATE {STATE} SET status=%s, next_retry_at=now() + interval '6 hours'", (status,))
    woken = _wakes(mod, monkeypatch)

    result = mod.request_edge_profiles(["track-a"], "catalog-a")

    # No job per track: the pass covers them (R6).
    assert result["already_pending"] == ["track-a"] and result["deferred"] == []
    assert _q(db, f"SELECT count(*) FROM {JOBS}") == [(0,)]
    assert woken == ([] if status == "queued" else ["edge_backfill_ready"])
    if status != "queued":
        assert _q(db, f"SELECT status, next_retry_at FROM {STATE}") == [("queued", None)]
    assert store.edge_backfill_candidates(db, "catalog-a") == ["track-a"]


def test_requests_saved_by_1_8_0_are_served(edge_publication_db, monkeypatch):
    # 1.8.0 saved work AudioMuse refused to queue under its own marker.
    mod, db = load_plugin(), edge_publication_db
    _store().claim_edge_requests(db, "catalog-a", ["track-a"], "interactive")
    _q(db, f"UPDATE {JOBS} SET last_error=%s", (mod.WAITING_MARKERS[1],))
    _q(db, f"UPDATE {PROFILES} SET status='pending_interactive', attempt_token='tok-a', last_error=%s",
       (mod.WAITING_MARKERS[1],))
    runs, served = [], []
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **kw: runs.append((ids, kw["attempt_tokens"])))
    monkeypatch.setattr(mod, "analyze_edges_task", lambda jobs, *_a, **_k: served.extend(
        job["track_id"] for job in jobs) or [])

    assert mod.serve_interactive() == 2
    assert runs == [(["track-a"], {"track-a": "tok-a"})] and served == ["track-a"]
    assert mod.serve_interactive() == 0


def test_on_demand_waveform_request_waits_for_the_runner_and_is_served_next(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    _q(db, f"UPDATE {PROFILES} SET status='pending_interactive', attempt_token='tok-a'")
    monkeypatch.setattr(mod, "mark_pending", lambda *_args, **_kwargs: {"track-a": "tok-a"})
    woken = _wakes(mod, monkeypatch)

    assert mod.record_interactive_profiles(["track-a"], "catalog-a") == {"track-a": "tok-a"}
    assert woken == ["interactive_profiles"]
    # Still pending on demand, marked for the runner.
    assert _q(db, f"SELECT status, attempt_token, last_error FROM {PROFILES}") == [
        ("pending_interactive", "tok-a", mod.RUNNER_WAITING_MARKER)
    ]

    runs = []
    monkeypatch.setattr(
        mod, "analyze_tracks_task",
        lambda ids, catalog_instance_id=None, server_id=None, priority="background",
        attempt_tokens=None: runs.append((ids, catalog_instance_id, server_id, priority, attempt_tokens)),
    )
    assert mod.serve_interactive() == 1
    assert runs == [(["track-a"], "catalog-a", "server-a", "interactive", {"track-a": "tok-a"})]
    assert mod.serve_interactive() == 0


def test_queued_background_edge_batch_yields_after_its_budget(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    jobs, _ready = _store().claim_edge_jobs(db, "catalog-a", ["track-a"])
    served = []
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "serve_interactive", lambda *_a, **_k: served.append(1) or 0)

    outcomes = mod.analyze_edges_task(jobs, "catalog-a", "server-a", budget_seconds=-1)

    assert served == [1]
    assert outcomes == [{"track_id": "track-a", "status": "released"}]
    assert _q(db, f"SELECT status, last_error FROM {JOBS}") == [("failed", "edge-enqueue-failed")]


def test_unsupported_media_is_terminal_for_its_revision(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    from plugins.LumaeAnalysis.edge_profiles import EdgeProfileError

    jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a"])
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(
        mod, "load_track_file",
        lambda *_a, **_k: {"file_path": "x.flac", "media_signature": "private/path:123:456",
                           "cleanup_path": None},
    )

    def refuse_layout(*_args, **_kwargs):
        raise EdgeProfileError("unqualified channel layout")

    monkeypatch.setattr(mod, "run_file_analysis", refuse_layout)
    outcomes = mod.analyze_edges_task(jobs, "catalog-a", "server-a", "interactive")

    assert outcomes == [{"track_id": "track-a", "status": "unsupported"}]
    _q(db, f"UPDATE {JOBS} SET updated_at=now()-interval '7 hours'")
    requeued, _ready, pending, unsupported = store.claim_edge_requests(
        db, "catalog-a", ["track-a"], "interactive"
    )
    assert (requeued, pending, unsupported) == ([], [], ["track-a"])
    assert store.edge_statuses(db, "catalog-a", ["track-a"]) == {"track-a": {"status": "unsupported"}}


def test_a_decode_error_twice_on_the_same_media_is_terminal(edge_publication_db, monkeypatch):
    """W3: the first decode error backs off as before; the retry keeps it, and a
    second one on the same media retires the job (it was downloaded whole and
    retried every six hours). Other failures keep backing off."""
    mod, db, store = load_plugin(), edge_publication_db, _store()
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(
        mod, "load_track_file",
        lambda *_a, **_k: {"file_path": "x.flac", "media_signature": "private/path:123:456",
                           "cleanup_path": None},
    )
    failure = {"exc": None}

    def fail(*_args, **_kwargs):
        raise failure["exc"]

    monkeypatch.setattr(mod, "run_file_analysis", fail)
    from plugins.LumaeAnalysis.analysis_isolation import IsolatedAnalysisError

    failure["exc"] = IsolatedAnalysisError("media_error", "InvalidDataError")
    jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a"])
    assert mod.analyze_edges_task(jobs, "catalog-a", "server-a", "background") == [
        {"track_id": "track-a", "status": "failed"}]
    assert _q(db, f"SELECT status, left(last_error, 16) FROM {JOBS}") == [("failed", "edge-media-error")]

    _q(db, f"UPDATE {JOBS} SET updated_at=now()-interval '7 hours'")
    jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a"])
    assert len(jobs) == 1
    # The retry keeps the last failure of the same media.
    assert _q(db, f"SELECT status, left(last_error, 16) FROM {JOBS}") == [("pending", "edge-media-error")]
    assert mod.analyze_edges_task(jobs, "catalog-a", "server-a", "background") == [
        {"track_id": "track-a", "status": "unsupported"}]
    assert store.edge_statuses(db, "catalog-a", ["track-a"]) == {"track-a": {"status": "unsupported"}}


def test_other_edge_failures_keep_backing_off(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(
        mod, "load_track_file",
        lambda *_a, **_k: {"file_path": "x.flac", "media_signature": "private/path:123:456",
                           "cleanup_path": None},
    )
    from plugins.LumaeAnalysis.analysis_isolation import IsolatedAnalysisError

    def timeout(*_args, **_kwargs):
        raise IsolatedAnalysisError("analysis_timeout", "slow")

    monkeypatch.setattr(mod, "run_file_analysis", timeout)
    for _ in range(3):
        jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a"])
        assert mod.analyze_edges_task(jobs, "catalog-a", "server-a", "background") == [
            {"track_id": "track-a", "status": "failed"}]
        _q(db, f"UPDATE {JOBS} SET updated_at=now()-interval '7 hours'")


def test_edge_statuses_report_absent_pending_and_failed(edge_publication_db):
    store, db = _store(), edge_publication_db
    assert store.edge_statuses(db, "catalog-a", ["track-a", "unknown"]) == {
        "track-a": {"status": "absent"}
    }
    jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a"])
    assert store.edge_statuses(db, "catalog-a", ["track-a"]) == {"track-a": {"status": "pending"}}
    store.update_edge_job(db, "catalog-a", jobs[0], "failed", "edge-analysis-timeout")
    status = store.edge_statuses(db, "catalog-a", ["track-a"])["track-a"]
    assert status["status"] == "failed" and status["retry_at"].endswith("Z")


def test_profiles_route_reports_edge_status_only_on_request(edge_publication_db):
    mod = load_plugin()
    client = plugin_client(mod)
    plain = client.get("/api/profiles?catalog_instance_id=catalog-a&ids=track-a").get_json()
    assert "edge_status" not in plain
    opted = client.get("/api/profiles?catalog_instance_id=catalog-a&ids=track-a&edge_status=1").get_json()
    assert opted["edge_status"] == {"track-a": {"status": "absent"}}


def test_worker_reports_decide_analyzable_for_a_web_process_without_pyav(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    monkeypatch.setattr(mod, "edge_runtime_available", lambda: False)
    monkeypatch.setattr(mod, "edge_setting_enabled", lambda: True)
    assert mod.edge_profiles_analyzable() is None

    store.record_edge_runtime(db, "worker-1", {"available": False, "reason": "pyav_missing"})
    assert mod.edge_profiles_analyzable() is False
    store.record_edge_runtime(db, "worker-2", {"available": True, "pyav": "17.1.0", "reason": None})
    assert mod.edge_profiles_analyzable() is True

    result = mod.request_edge_profiles(["track-a"], "catalog-a", priority="interactive")
    assert result["accepted"] == ["track-a"]

    capability = mod.edge_profiles_capability()
    assert capability["analyzable"] is True and capability["available"] is False
    assert capability["worker_runtime"]["pyav"] == "17.1.0"
    assert capability["queue"] == {"pending": 1, "running": 0, "interactive": 1}
    assert capability["server_backfill"] is True

    _q(db, "UPDATE plugin_lumae_analysis__edge_runtime_state SET reported_at=now()-interval '7 hours'")
    assert mod.edge_profiles_analyzable() is None

    monkeypatch.setattr(mod, "edge_setting_enabled", lambda: False)
    assert mod.edge_profiles_analyzable() is False


def test_first_qualified_report_starts_the_pass_and_wakes_the_runners(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    woken = []
    monkeypatch.setattr(mod, "wake_runners", lambda _db, reason, commit=False: woken.append(reason))
    monkeypatch.setattr(mod, "edge_runtime_status", lambda: {"available": True, "reason": None})

    mod.report_edge_runtime(force=True)

    assert _q(db, f"SELECT status FROM {STATE}") == [("queued",)]
    assert woken == ["edge_backfill_ready"]
    # Nothing new to start: later reports do not wake the runners.
    mod.report_edge_runtime(force=True)
    assert woken == ["edge_backfill_ready"]


def test_periodic_report_keeps_a_finished_pass_resweep_delay(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    monkeypatch.setattr(mod, "wake_runners", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "edge_runtime_status", lambda: {"available": True, "reason": None})
    store.ensure_edge_backfill_sources(db)
    store.update_edge_backfill(db, "catalog-a", "complete", cursor="", next_retry_hours=6, completed=True)

    mod.report_edge_runtime(force=True)

    assert _q(db, f"SELECT status, next_retry_at > now() + interval '5 hours' FROM {STATE}") == [
        ("complete", True)
    ]
    # A catalogue refresh with changes does re-arm it.
    assert store.arm_edge_backfill(db, "catalog-a") == 1
    assert _q(db, f"SELECT status FROM {STATE}") == [("queued",)]


def test_runtime_report_parks_and_arms_the_library_pass(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    monkeypatch.setattr(mod, "wake_runners", lambda *_a, **_k: None)
    assert store.next_edge_backfill(db) == ("server-a", "catalog-a", "")

    monkeypatch.setattr(mod, "edge_runtime_status", lambda: {"available": False, "reason": "pyav_missing"})
    mod.report_edge_runtime(force=True)
    assert _q(db, f"SELECT status FROM {STATE}") == [("waiting_runtime",)]
    assert store.next_edge_backfill(db) is None

    monkeypatch.setattr(mod, "edge_runtime_status", lambda: {"available": True, "reason": None})
    mod.report_edge_runtime(force=True)
    assert _q(db, f"SELECT status FROM {STATE}") == [("queued",)]
    assert _q(db, "SELECT worker_id IS NOT NULL, available FROM plugin_lumae_analysis__edge_runtime_state") == [
        (True, True)
    ]


def test_library_pass_walks_in_slices_completes_and_resweeps(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    runs = []
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(
        mod, "analyze_edges_task",
        lambda jobs, source, server, priority="background", budget_seconds=None:
            runs.append(([job["track_id"] for job in jobs], priority, budget_seconds))
            or [{"track_id": job["track_id"], "status": "ready"} for job in jobs],
    )
    assert store.next_edge_backfill(db) == ("server-a", "catalog-a", "")

    first = mod.edge_backfill_task("server-a", "catalog-a", deadline=time.monotonic() + 60)
    # The library fits in the turn (WP1a): the pass reaches its end and finishes.
    assert first["status"] == "complete" and first["processed"] == 1
    # The track gets what is left of the slice (plan S2, WP1a), never None.
    [(tracks, priority, budget)] = runs
    assert (tracks, priority) == (["track-a"], "background")
    assert 55 < budget <= 60
    assert _q(db, f"SELECT status, cursor, processed, next_retry_at > now() + interval '5 hours' "
                  f"FROM {STATE}") == [("complete", "", 1, True)]
    assert store.next_edge_backfill(db) is None

    store.arm_edge_backfill(db, "catalog-a")
    assert store.next_edge_backfill(db) == ("server-a", "catalog-a", "")


def test_library_pass_parks_when_this_worker_cannot_analyse(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
    store.ensure_edge_backfill_sources(db)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    assert mod.edge_backfill_task("server-a", "catalog-a")["status"] == "paused"
    assert _q(db, f"SELECT status FROM {STATE}") == [("waiting_runtime",)]


def test_credits_and_metadata_yield_to_source_scoped_demand(edge_publication_db):
    from plugins.LumaeAnalysis import credits_service

    db = edge_publication_db
    _q(db, "CREATE TABLE plugin_lumae_analysis__profiles (track_id TEXT PRIMARY KEY, status TEXT)")
    assert credits_service.playback_pending(db) is False

    _q(db, f"UPDATE {PROFILES} SET status='pending_interactive'")
    assert credits_service.playback_pending(db) is True
    _q(db, f"UPDATE {PROFILES} SET status='ready'")

    _store().claim_edge_requests(db, "catalog-a", ["track-a"], "interactive")
    assert credits_service.playback_pending(db) is True


def test_reconcile_runner_keeps_the_edge_pass_resweep_time(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(mod, "begin_event", lambda *_a, **_k: "event-1")
    monkeypatch.setattr(mod, "_safe_progress", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "finish_event", lambda _db, event, status, **_k: calls.append((event, status)))
    monkeypatch.setattr(mod, "update_work_retry", lambda *_a, **_k: calls.append("reset"))

    result = mod._run_reconcile_action(
        object(), "edge_backfill", "server-a", "catalog-a", "catalog-a", 0,
        lambda *_a: {"status": "complete", "processed": 0}, "server-a", "catalog-a",
    )

    assert result["status"] == "complete"
    assert calls == [("event-1", "success")]


# A UTC session cannot tell local time from UTC; zones on either side of it can.
@pytest.mark.parametrize("zone", ["Asia/Kolkata", "America/Los_Angeles"])
def test_edge_timestamps_are_real_utc_whatever_the_session_time_zone(edge_publication_db, zone):
    from datetime import datetime, timedelta, timezone

    store, db = _store(), edge_publication_db
    _q(db, f"SET TIME ZONE '{zone}'")
    jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a"])
    store.update_edge_job(db, "catalog-a", jobs[0], "failed", "edge-analysis-timeout")
    store.record_edge_runtime(db, "worker-1", {"available": True})
    store.ensure_edge_backfill_sources(db)
    store.update_edge_backfill(db, "catalog-a", "complete", next_retry_hours=6, completed=True)
    now = datetime.now(timezone.utc)

    def parse(text):
        assert text.endswith("Z")
        return datetime.fromisoformat(text.replace("Z", "+00:00"))

    retry_at = parse(store.edge_statuses(db, "catalog-a", ["track-a"])["track-a"]["retry_at"])
    assert abs(retry_at - (now + timedelta(hours=6))) < timedelta(minutes=1)
    reported_at = parse(store.edge_worker_runtime(db)[1]["reported_at"])
    assert abs(reported_at - now) < timedelta(minutes=1)
    row = store.edge_backfill_row(db, "catalog-a")
    assert abs(parse(row["completed_at"]) - now) < timedelta(minutes=1)
    assert abs(parse(row["next_retry_at"]) - (now + timedelta(hours=6))) < timedelta(minutes=1)


@pytest.mark.parametrize("marker", ["RUNNER_WAITING_MARKER", "DEFERRED_MARKER"])
def test_waiting_on_demand_work_counts_as_ready(migrated_db, marker):
    from plugins.LumaeAnalysis import reconcile

    db = migrated_db
    with db.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS task_status (task_id TEXT, parent_task_id TEXT,
            task_type TEXT, status TEXT)""")
    db.commit()

    def summary():
        with db.cursor() as cur:
            result = reconcile._work_summary(cur)
        db.rollback()
        return result["ready"]

    baseline = summary()
    with db.cursor() as cur:
        cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_sources
            (catalog_instance_id, current_core_server_id, provider_type, server_name, is_default, rebind_status)
            VALUES ('any', 'server-a', 'navidrome', 'Test', TRUE, 'active')""")
        cur.execute(f"""INSERT INTO {PROFILES} (catalog_instance_id, track_id, sample_rate, duration_ms,
            ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, analyzed_at, status,
            last_error) VALUES ('any', 't', 0, 0, 0, '', '', 1, 1, now(), 'pending_interactive', %s)""",
                    (getattr(reconcile, marker),))
    db.commit()
    assert summary() == baseline + 1
