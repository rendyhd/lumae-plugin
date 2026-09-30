"""On-demand priority, saved refused requests and the server edge pass.

Plan: docs/plan/EDGE_PRIORITY_AND_VECTORS_PLAN.md (Phases 1-3).

* An on-demand edge request promotes a job a background batch already holds.
* A request the host refuses (``ERR_TASK_IN_PROGRESS``) is saved, reported as
  ``deferred`` and served by the next worker task; other errors still fail.
* A queued background batch yields after its budget; unsupported media is
  terminal for its revision.
* Workers report their edge runtime, which decides ``analyzable``; the web
  process no longer needs PyAV to queue work.
* The server walks the library in small resumable ticks.
* Credits and metadata yield to source-scoped on-demand work.
"""
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


class HostBusy(Exception):
    """AudioMuse's refusal: another root task is running."""

    code = 1201


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


def _refuse(*_args, **_kwargs):
    raise HostBusy("Another queue job (plugin.x) is still running.")


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


def test_refused_on_demand_edge_request_is_saved_and_served_next(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)
    monkeypatch.setattr(mod, "enqueue_bounded", _refuse)

    response = plugin_client(mod).post(
        "/api/profiles/edges/analyze", json={"catalog_instance_id": "catalog-a", "ids": ["track-a"]}
    )

    assert response.status_code == 202
    body = response.get_json()
    assert body["deferred"] == ["track-a"] and body["accepted"] == []
    assert _q(db, f"SELECT status, priority, last_error FROM {JOBS}") == [
        ("pending", "interactive", mod.DEFERRED_MARKER)
    ]

    served = []
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(
        mod, "analyze_edges_task",
        lambda jobs, source, server, priority="background", **_kw:
            served.append((source, server, priority, [job["track_id"] for job in jobs])) or [],
    )
    assert mod.serve_deferred_interactive() == 1
    assert served == [("catalog-a", "server-a", "interactive", ["track-a"])]
    assert _q(db, f"SELECT last_error FROM {JOBS}") == [(None,)]
    assert mod.serve_deferred_interactive() == 0


def test_refused_background_edge_request_is_released_for_retry(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)
    monkeypatch.setattr(mod, "enqueue_bounded", _refuse)

    result = mod.enqueue_edge_profiles(["track-a"], "catalog-a", "server-a")

    assert result["deferred"] == ["track-a"]
    assert _q(db, f"SELECT status, last_error FROM {JOBS}") == [("failed", "edge-enqueue-failed")]
    # Due again at once, for the library pass and for a later request.
    assert _store().edge_backfill_candidates(db, "catalog-a") == ["track-a"]


def test_other_enqueue_errors_still_fail_the_request(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)

    def broken(*_args, **_kwargs):
        raise RuntimeError("queue database unavailable")

    monkeypatch.setattr(mod, "enqueue_bounded", broken)
    with pytest.raises(RuntimeError):
        mod.enqueue_edge_profiles(["track-a"], "catalog-a", "server-a", priority="interactive")
    assert _q(db, f"SELECT status, last_error FROM {JOBS}") == [("failed", "edge-enqueue-failed")]


def test_refused_on_demand_waveform_request_is_saved_and_served_next(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    _q(db, f"UPDATE {PROFILES} SET status='pending_interactive', attempt_token='tok-a'")
    monkeypatch.setattr(mod, "mark_pending", lambda *_args, **_kwargs: {"track-a": "tok-a"})
    monkeypatch.setattr(mod, "enqueue_bounded", _refuse)

    assert mod.enqueue_profile_analysis(
        ["track-a"], "catalog-a", "server-a", priority="interactive"
    ) == mod.DEFERRED
    # Still pending on demand, not released.
    assert _q(db, f"SELECT status, attempt_token, last_error FROM {PROFILES}") == [
        ("pending_interactive", "tok-a", mod.DEFERRED_MARKER)
    ]

    runs = []
    monkeypatch.setattr(
        mod, "analyze_tracks_task",
        lambda ids, catalog_instance_id=None, server_id=None, priority="background",
        attempt_tokens=None: runs.append((ids, catalog_instance_id, server_id, priority, attempt_tokens)),
    )
    assert mod.serve_deferred_interactive() == 1
    assert runs == [(["track-a"], "catalog-a", "server-a", "interactive", {"track-a": "tok-a"})]


def test_background_waveform_refusal_keeps_the_old_release(edge_publication_db, monkeypatch):
    mod = load_plugin()
    released = []
    monkeypatch.setattr(mod, "mark_pending", lambda *_args, **_kwargs: {"track-a": "tok-a"})
    monkeypatch.setattr(mod, "enqueue_bounded", _refuse)
    monkeypatch.setattr(mod, "release_pending", lambda ids, **_kwargs: released.extend(ids))

    with pytest.raises(HostBusy):
        mod.enqueue_profile_analysis(["track-a"], "catalog-a", "server-a")
    assert released == ["track-a"]


def test_queued_background_edge_batch_yields_after_its_budget(edge_publication_db, monkeypatch):
    mod, db = load_plugin(), edge_publication_db
    jobs, _ready = _store().claim_edge_jobs(db, "catalog-a", ["track-a"])
    served = []
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "serve_deferred_interactive", lambda *_a, **_k: served.append(1) or 0)

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

    submitted = []
    monkeypatch.setattr(mod, "enqueue_bounded", lambda *a, **k: submitted.append(k["queue"]))
    result = mod.enqueue_edge_profiles(["track-a"], "catalog-a", "server-a", priority="interactive")
    assert result["accepted"] == ["track-a"] and submitted == ["high"]

    capability = mod.edge_profiles_capability()
    assert capability["analyzable"] is True and capability["available"] is False
    assert capability["worker_runtime"]["pyav"] == "17.1.0"
    assert capability["queue"] == {"pending": 1, "running": 0, "interactive": 1}
    assert capability["server_backfill"] is True

    _q(db, "UPDATE plugin_lumae_analysis__edge_runtime_state SET reported_at=now()-interval '7 hours'")
    assert mod.edge_profiles_analyzable() is None

    monkeypatch.setattr(mod, "edge_setting_enabled", lambda: False)
    assert mod.edge_profiles_analyzable() is False


def test_runtime_report_parks_and_arms_the_library_pass(edge_publication_db, monkeypatch):
    mod, db, store = load_plugin(), edge_publication_db, _store()
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


def test_library_pass_walks_in_ticks_completes_and_resweeps(edge_publication_db, monkeypatch):
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

    first = mod.edge_backfill_task("server-a", "catalog-a")
    assert first["status"] == "queued" and first["processed"] == 1
    assert runs == [(["track-a"], "background", None)]
    assert _q(db, f"SELECT status, cursor, processed FROM {STATE}") == [("queued", "track-a", 1)]

    second = mod.edge_backfill_task("server-a", "catalog-a")
    assert second["status"] == "complete"
    assert _q(db, f"SELECT status, cursor, next_retry_at > now() + interval '5 hours' FROM {STATE}") == [
        ("complete", "", True)
    ]
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


@pytest.mark.parametrize("zone", ["UTC", "Asia/Kolkata", "America/Los_Angeles"])
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
