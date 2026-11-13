"""Pass review fixes without a database: ``/api/analyze`` refuses a malformed
body, a media-server outage stops a waveform batch without charging its
tracks, pausing edge work releases the job instead of failing it, a walk's
cursor stays before a released unit, failure handlers roll back first, a
failed on-demand serve hands its claims back, a running preparation is not
finished, relationship ``waiting_for_inputs`` is deferred work, and the
retention task purges discovery receipts."""
import types

import pytest

from test_lumae_analysis import load_plugin, plugin_client

SOURCE = "catalog-a"


# --- 2: /api/analyze body ----------------------------------------------------

@pytest.mark.parametrize("body", [{"ids": None}, {"ids": [1, 2]}, {"ids": "abc"}, ["a", "b"],
                                  {"ids": {"a": 1}}])
def test_a_malformed_analyze_body_is_400_not_500(monkeypatch, body):
    mod = load_plugin()
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_kwargs: {"catalog_instance_id": SOURCE, "server_id": "server-a"})
    monkeypatch.setattr(mod, "split_analyze_ids",
                        lambda *_a, **_k: pytest.fail("a malformed body reached the database"))
    response = plugin_client(mod).post("/api/analyze", json=body)
    assert response.status_code == 400
    assert response.get_json()["error"] == "invalid_analyze_request"


def test_a_valid_analyze_body_is_unchanged(monkeypatch):
    mod = load_plugin()
    recorded = []
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_kwargs: {"catalog_instance_id": SOURCE, "server_id": "server-a"})
    monkeypatch.setattr(mod, "split_analyze_ids", lambda ids, **_k: (ids, [], []))
    monkeypatch.setattr(mod, "fetch_profile_status_rows", lambda ids, **_k: [])
    monkeypatch.setattr(mod, "record_interactive_profiles", lambda ids, source: recorded.append(ids))
    monkeypatch.setattr(mod, "rewait_interactive_profiles", lambda db, source, ids: [])
    response = plugin_client(mod).post("/api/analyze", json={"ids": ["a,b", "c"]})
    assert response.status_code == 202
    assert response.get_json()["accepted"] == ["a", "b", "c"]
    assert recorded == [["a", "b", "c"]]
    assert plugin_client(mod).post("/api/analyze", json={}).status_code == 202


# --- 3: the download breaker -------------------------------------------------

@pytest.fixture
def waveform_batch(monkeypatch):
    """A serial background batch whose host downloads fail for the ids in ``down``."""
    mod = load_plugin()
    down, charged, released = set(), [], []

    def load(track_id, **_kwargs):
        if track_id in down:
            raise mod.MediaDownloadError("media server download failed")
        return {"track_id": track_id, "file_path": "x.flac", "media_signature": "sig", "cleanup_path": None}

    def upsert(track_id, result, status, error, media_sig, **kwargs):
        charged.append((track_id, status, kwargs.get("failure_code")))
        return True

    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod.parallel_analysis, "current", lambda: None)
    monkeypatch.setattr(mod, "serve_interactive", lambda *a, **k: 0)
    monkeypatch.setattr(mod, "heartbeat_profile_backfill", lambda *a, **k: True)
    monkeypatch.setattr(mod, "_safe_progress", lambda *a, **k: None)
    monkeypatch.setattr(mod, "finalize_preparation_if_settled", lambda *a: None)
    monkeypatch.setattr(mod, "profile_task_disposition", lambda *a, **k: "analyze")
    monkeypatch.setattr(mod, "load_track_file", load)
    monkeypatch.setattr(mod, "upsert_profile", upsert)
    monkeypatch.setattr(mod, "_bundle_edges", lambda _sig: False)
    monkeypatch.setattr(mod.file_tags, "capture", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_analyse_track_file", lambda state, *a: object())
    monkeypatch.setattr(mod, "_leave_edge_to_pass", lambda *a: None)
    monkeypatch.setattr(mod, "release_pending", lambda ids, **kwargs: released.append(
        (list(ids), kwargs.get("count_failure"), dict(kwargs.get("tokens") or {}))))

    def run(ids):
        tokens = {track_id: f"tok-{track_id}" for track_id in ids}
        return mod.analyze_tracks_task(ids, catalog_instance_id=SOURCE, server_id="server-a",
                                       attempt_tokens=tokens)
    return mod, down, charged, released, run


def test_an_outage_stops_the_batch_without_charging_its_tracks(waveform_batch):
    mod, down, charged, released, run = waveform_batch
    down.update({"t1", "t2", "t3", "t4", "t5"})
    summary = run(["t1", "t2", "t3", "t4", "t5"])
    assert charged == []  # no retry budget spent
    assert released == [(["t1", "t2", "t3"], False, {"t1": "tok-t1", "t2": "tok-t2", "t3": "tok-t3"}),
                        (["t4", "t5"], False, {f"t{i}": f"tok-t{i}" for i in range(1, 6)})]
    assert summary["download_breaker"] is True
    assert summary["failed"] == 0 and summary["deferred"] == 5


def test_an_unreadable_file_among_good_ones_is_still_charged(waveform_batch):
    mod, down, charged, released, run = waveform_batch
    down.update({"t1", "t3", "t4"})
    summary = run(["t1", "t2", "t3", "t4"])
    assert ("t1", "failed", "download_unavailable") in charged
    # t3 and t4 failed last: only two in a row, charged when the batch ends.
    assert ("t3", "failed", "download_unavailable") in charged
    assert ("t4", "failed", "download_unavailable") in charged
    assert ("t2", "ready", None) in charged
    assert released == []
    assert summary["failed"] == 3 and "download_breaker" not in summary


def test_a_tripped_batch_backs_the_pass_off(monkeypatch):
    mod = load_plugin()
    states = []
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "claim_profile_backfill_batch", lambda _source: True)
    monkeypatch.setattr(mod, "_safe_progress", lambda *a, **k: None)
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_k: {})
    monkeypatch.setattr(mod, "recover_stale_pending_profiles", lambda _source: 0)
    monkeypatch.setattr(mod, "find_backfill_ids", lambda *a, **k: ["t1", "t2", "t3"])
    monkeypatch.setattr(mod, "mark_pending", lambda ids, **k: {i: "tok" for i in ids})
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **k: {
        "attempted": 3, "ready": 0, "failed": 0, "deferred": 3, "download_breaker": True})
    monkeypatch.setattr(mod, "update_profile_backfill_state",
                        lambda *a, **k: states.append((a[2], k.get("retry_after_seconds"))))
    result = mod.profile_backfill_task("server-a", SOURCE)
    assert result["status"] == "waiting_retry"
    assert states == [("queued", mod.BACKFILL_ABORT_RETRY_SECONDS)]


def test_the_loudness_breaker_records_nothing_when_it_trips():
    mod = load_plugin()
    result, released, recorded = {"ready": 0, "failed": 0}, [], []
    breaker = mod._loudness_breaker(result, released.extend)
    for index in range(2):
        breaker.hold((f"t{index}", "rev"), lambda: (recorded.append(1), breaker.count("failed")))
    breaker.settle()
    assert result["failed"] == 2 and len(recorded) == 2
    for index in range(3):
        breaker.hold((f"u{index}", "rev"), lambda: recorded.append(1))
    assert breaker.tripped and released == [("u0", "rev"), ("u1", "rev"), ("u2", "rev")]
    breaker.settle()
    assert len(recorded) == 2


# --- 14: the download's overall deadline -------------------------------------

def test_a_deferred_download_gets_the_per_file_limit_as_its_deadline(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(mod.analysis_download, "fetch",
                        lambda target, **kwargs: calls.append(kwargs.get("deadline_seconds")))
    info = {"download": object(), "file_path": "x.flac"}
    assert mod.fetch_track_file(info, 90) == "x.flac"
    assert mod.fetch_track_file(info) == "x.flac"
    assert calls == [90, None]


# --- 4: pausing edge work ----------------------------------------------------

def test_a_paused_edge_job_is_released_not_failed(monkeypatch):
    mod = load_plugin()
    updates = []
    monkeypatch.setattr(mod, "get_db", lambda: None)
    monkeypatch.setattr(mod, "update_edge_job",
                        lambda db, source, job, status, reason=None: updates.append((status, reason)) or True)
    monkeypatch.setattr(mod, "fail_edge_job", lambda *a: pytest.fail("a pause failed the job"))
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    job = {"track_id": "t1", "media_revision": "rev", "job_token": "tok"}
    outcome, state = mod._prepare_edge(job, SOURCE, "server-a")
    assert state is None and outcome == {"track_id": "t1", "status": "released"}
    assert updates == [("running", None), ("failed", "edge-enqueue-failed")]


def test_a_paused_edge_batch_releases_the_rest_without_starting_them(monkeypatch):
    mod = load_plugin()
    updates = []
    monkeypatch.setattr(mod, "get_db", lambda: None)
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *a, **k: None)
    monkeypatch.setattr(mod, "serve_interactive", lambda *a, **k: 0)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    monkeypatch.setattr(mod, "update_edge_job",
                        lambda db, source, job, status, reason=None: updates.append((job["track_id"], status, reason)))
    jobs = [{"track_id": t, "media_revision": "rev", "job_token": "tok"} for t in ("t1", "t2")]
    outcomes = mod.analyze_edges_task(jobs, SOURCE, "server-a")
    assert outcomes == [{"track_id": "t1", "status": "released"}, {"track_id": "t2", "status": "released"}]
    assert updates == [("t1", "failed", "edge-enqueue-failed"), ("t2", "failed", "edge-enqueue-failed")]


def test_the_edge_walk_keeps_its_cursor_before_a_released_track(monkeypatch):
    mod = load_plugin()
    saved = []
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "get_db", lambda: None)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(mod.parallel_analysis, "current", lambda: None)
    monkeypatch.setattr(mod, "edge_backfill_row", lambda db, source: {"cursor": "a"})
    monkeypatch.setattr(mod, "edge_backfill_candidates", lambda db, source, after, size: ["t1", "t2", "t3"])
    monkeypatch.setattr(mod, "update_edge_backfill", lambda db, source, status, **k: saved.append((status, k)))
    monkeypatch.setattr(mod, "claim_edge_jobs", lambda db, source, ids: ([{"track_id": ids[0]}], []))
    outcomes = {"t1": "ready", "t2": "released"}
    monkeypatch.setattr(mod, "analyze_edges_task", lambda jobs, *a, **k: [
        {"track_id": jobs[0]["track_id"], "status": outcomes[jobs[0]["track_id"]]}])
    result = mod.edge_backfill_task("server-a", SOURCE, deadline=None)
    assert result["status"] == "queued"
    assert saved[-1] == ("queued", {"cursor": "t1", "processed": 2})


def test_reached_stops_before_a_unit_released_in_finish():
    mod = load_plugin()
    report = mod.parallel_analysis.Report()
    report.taken = [("t1", "r"), ("t2", "r"), ("t3", "r")]
    report.done = {0, 1, 2}
    assert report.reached() == ("t3", "r")
    assert mod._reached_unreleased(report, {"t2"}, lambda unit: unit[0]) == ("t1", "r")
    assert mod._reached_unreleased(report, {"t1"}, lambda unit: unit[0]) is None


# --- 8: failure handlers roll back first -------------------------------------

class _AbortedDb:
    """A connection in an aborted transaction until it is rolled back."""

    def __init__(self):
        self.aborted = True

    def rollback(self):
        self.aborted = False


def test_a_failed_backfill_batch_rolls_back_before_recording_it(monkeypatch):
    mod = load_plugin()
    db = _AbortedDb()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "claim_profile_backfill_batch", lambda _source: True)
    monkeypatch.setattr(mod, "_safe_progress", lambda *a, **k: None)
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_k: (_ for _ in ()).throw(RuntimeError("database error")))

    def update(*_a, **_k):
        if db.aborted:
            raise AssertionError("current transaction is aborted")
    monkeypatch.setattr(mod, "update_profile_backfill_state", update)
    with pytest.raises(RuntimeError, match="database error"):
        mod.profile_backfill_task("server-a", SOURCE)


def test_a_failed_preparation_rolls_back_before_recording_it(monkeypatch):
    mod = load_plugin()
    db = _AbortedDb()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_k: {"catalog_instance_id": SOURCE, "server_id": "server-a"})
    monkeypatch.setattr(mod, "claim_preparation_run", lambda _source: True)
    monkeypatch.setattr(mod, "assert_preparation_worker_current",
                        lambda _source: (_ for _ in ()).throw(RuntimeError("database error")))
    seen = []
    monkeypatch.setattr(mod, "update_preparation_state",
                        lambda *a, **k: seen.append(db.aborted))
    try:
        mod.prepare_lumae_task("server-a", SOURCE)
    except RuntimeError:
        pass
    assert seen and not any(seen)


# --- 9: a failed on-demand serve ---------------------------------------------

def test_a_failed_serve_hands_its_claims_back_and_waits(monkeypatch):
    mod = load_plugin()
    released = []
    monkeypatch.setattr(mod, "_interactive_failed_at", None)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "get_db", lambda: _AbortedDb())
    monkeypatch.setattr(mod, "claim_waiting_profiles", lambda db, limit: [("s1", "t1", "tok1"),
                                                                          ("s2", "t2", "tok2")])
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_k: (_ for _ in ()).throw(RuntimeError("source gone")))
    monkeypatch.setattr(mod, "_safe_release_runner_claims", lambda db, role: released.append(role))
    token = mod.runner.ROLE.set(mod.runner.ROLE_WORKER)
    try:
        assert mod.serve_interactive() == 0
        assert released == [mod.runner.ROLE_WORKER]
        # No hot retry between every file: the next call waits.
        monkeypatch.setattr(mod, "claim_waiting_profiles", lambda *a: pytest.fail("retried at once"))
        assert mod.serve_interactive() == 0
    finally:
        mod.runner.ROLE.reset(token)


# --- 10, 12, 13 --------------------------------------------------------------

def test_a_running_preparation_is_not_finished_by_a_waveform_batch(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "preparation_state", lambda _source: {"status": "running", "server_id": "s"})
    monkeypatch.setattr(mod, "analysis_status_counts", lambda **_k: pytest.fail("counted the library"))
    monkeypatch.setattr(mod, "update_preparation_state", lambda *a, **k: pytest.fail("finished it"))
    assert mod.finalize_preparation_if_settled(SOURCE)["status"] == "running"


def test_waiting_for_inputs_is_settled_without_progress():
    mod = load_plugin()
    assert "waiting_for_inputs" in mod._SETTLED_STATUSES
    assert "waiting_for_inputs" in mod._NO_PROGRESS_STATUSES


def test_retention_purges_discovery_receipts(monkeypatch):
    mod = load_plugin()

    class Db:
        commits = 0

        def cursor(self):
            return types.SimpleNamespace(__enter__=None)

        def commit(self):
            Db.commits += 1

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    db = Db()
    db.cursor = lambda: Cursor()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    for name in ("purge_expired_collection_mutations", "purge_stale_collection_restores",
                 "purge_expired_shelf_mutations"):
        monkeypatch.setattr(mod, name, lambda _db: 0)
    for module, name in ((mod.vibes, "purge_expired_vibe_mutations"),
                         (mod.undertone, "purge_expired_undertone_mutations"),
                         (mod.journeys, "purge_expired_journey_mutations"),
                         (mod.covers, "purge_expired_cover_mutations"),
                         (mod.covers, "purge_unused_cover_images")):
        monkeypatch.setattr(module, name, lambda _db: 0)
    monkeypatch.setattr(mod.personal_discovery, "purge_expired_discovery_receipts",
                        lambda cur: 4, raising=False)
    captured = {}
    monkeypatch.setattr(mod, "task_message", lambda result, _sentence: captured.update(result))
    monkeypatch.setattr(mod, "collection_retention_sentence", lambda _result: "")
    mod.collection_retention_task()
    assert captured["discovery_receipts_deleted"] == 4 and Db.commits == 1
