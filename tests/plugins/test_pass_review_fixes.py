"""Pass review fixes without a database: ``/api/analyze`` refuses a malformed
body, a media-server outage stops a waveform batch without charging its
tracks, pausing edge work releases the job instead of failing it, a walk's
cursor stays before a released unit, failure handlers roll back first, a
failed on-demand serve hands its claims back, a running preparation is not
finished, relationship ``waiting_for_inputs`` is deferred work, and the
retention task purges discovery receipts."""
import os
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
    serving = [False]  # the canary's answer (``_downloads_serving``)
    monkeypatch.setattr(mod, "test_serving", serving, raising=False)
    monkeypatch.setattr(mod, "_downloads_serving", lambda *a: serving[0])
    monkeypatch.setattr(mod, "refresh_status_snapshot", lambda **k: None)

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


def test_missing_files_with_the_server_up_are_charged_and_the_batch_goes_on(waveform_batch):
    mod, down, charged, released, run = waveform_batch
    mod.test_serving[0] = True  # the canary downloads: the server serves files
    down.update({"t1", "t2", "t3"})  # three files gone, the only due rows
    summary = run(["t1", "t2", "t3"])
    assert [c for c in charged if c[1] == "failed"] == [
        (t, "failed", "download_unavailable") for t in ("t1", "t2", "t3")]
    assert released == []
    assert summary["failed"] == 3 and "download_breaker" not in summary


def test_an_on_demand_waveform_batch_has_no_breaker(waveform_batch):
    mod, down, charged, released, run = waveform_batch
    down.update({"t1", "t2", "t3", "t4"})  # an outage, the canary failing too
    tokens = {t: f"tok-{t}" for t in ("t1", "t2", "t3", "t4")}
    summary = mod.analyze_tracks_task(["t1", "t2", "t3", "t4"], catalog_instance_id=SOURCE,
                                      server_id="server-a", priority="interactive", attempt_tokens=tokens)
    assert charged == [(t, "failed", "download_unavailable") for t in ("t1", "t2", "t3", "t4")]
    assert released == []
    assert summary["failed"] == 4 and "download_breaker" not in summary


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


@pytest.fixture
def loudness_pass(monkeypatch):
    """A serial loudness walk over t1..t3 whose host downloads fail for the ids
    in ``down``; ``serving`` is the canary's answer."""
    mod = load_plugin()
    lb = mod.loudness_backfill
    down, recorded, updates, backoffs, serving = set(), [], [], [], [False]
    rows = [("t1", "r1"), ("t2", "r2"), ("t3", "r3")]
    db = types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

    def load(track_id, **_kwargs):
        if track_id in down:
            raise mod.MediaDownloadError("media server download failed")
        raise AssertionError("only failed downloads in this walk")

    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod.parallel_analysis, "current", lambda: None)
    monkeypatch.setattr(mod, "serve_interactive", lambda *a, **k: 0)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 10)
    monkeypatch.setattr(mod, "load_track_file", load)
    monkeypatch.setattr(mod, "_publish_loudness", lambda db, source, measured: 0)
    monkeypatch.setattr(mod, "_downloads_serving", lambda *a: serving[0])
    monkeypatch.setattr(mod, "_back_off_pass", lambda db, state, source: backoffs.append((state, source)))
    monkeypatch.setattr(lb, "state", lambda db, source: {"status": "queued", "cursor": "", "pass_due": True})
    monkeypatch.setattr(lb, "runtime_available", lambda: True)
    monkeypatch.setattr(lb, "candidates", lambda db, source, after, size, edges_enabled=False: [
        row for row in rows if row[0] > (after or "") and row[0] not in recorded])
    monkeypatch.setattr(lb, "update", lambda db, source, status, **k: updates.append((status, k)))
    monkeypatch.setattr(lb, "record_failure", lambda db, source, track_id, *a, **k: recorded.append(track_id))

    def run():
        return mod.loudness_backfill_task("server-a", SOURCE)
    return down, recorded, updates, backoffs, serving, run


def test_missing_loudness_files_with_the_server_up_are_charged_and_the_pass_completes(loudness_pass):
    down, recorded, updates, backoffs, serving, run = loudness_pass
    serving[0] = True
    down.update({"t1", "t2", "t3"})
    result = run()
    assert recorded == ["t1", "t2", "t3"]
    assert result["status"] == "complete" and result["failed"] == 3
    assert backoffs == [] and updates[-1][0] == "complete"


def test_a_loudness_outage_records_nothing_and_waits_for_a_retry(loudness_pass):
    down, recorded, updates, backoffs, serving, run = loudness_pass
    down.update({"t1", "t2", "t3"})
    result = run()
    assert recorded == []
    assert result["status"] == "waiting_retry"  # settled: not run again this turn
    assert backoffs == [("loudness_backfill_state", SOURCE)]
    assert updates[-1] == ("queued", {"cursor": "", "processed": 0})


class _CanaryDb:
    def __init__(self, track):
        self.track, self.rolled_back = track, 0

    def cursor(self):
        canary = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, params):
                self.params = params

            def fetchone(self):
                return (canary.track,) if canary.track else None
        return Cursor()

    def rollback(self):
        self.rolled_back += 1


def test_the_canary_downloads_a_published_track_and_removes_it(monkeypatch):
    mod = load_plugin()
    removed, loaded = [], []
    monkeypatch.setattr(mod, "get_db", lambda: _CanaryDb("good"))
    monkeypatch.setattr(mod, "remove_downloaded_file", removed.append)

    def load(track_id, **_kwargs):
        loaded.append(track_id)
        return {"file_path": "/tmp/good", "cleanup_path": "/tmp/good"}
    monkeypatch.setattr(mod, "load_track_file", load)
    assert mod._downloads_serving(SOURCE, "server-a", ["t1", "t2", "t3"]) is True
    assert loaded == ["good"] and removed == ["/tmp/good"]

    monkeypatch.setattr(mod, "load_track_file", lambda *a, **k: (_ for _ in ()).throw(
        mod.MediaDownloadError("down")))
    assert mod._downloads_serving(SOURCE, "server-a", ["t1"]) is False


def test_without_a_canary_the_provider_ping_decides(monkeypatch):
    mod = load_plugin()
    pinged = []
    monkeypatch.setattr(mod, "get_db", lambda: _CanaryDb(None))
    monkeypatch.setattr(mod, "load_track_file", lambda *a, **k: pytest.fail("no canary to download"))
    monkeypatch.setattr(mod, "get_core_adapter", lambda: types.SimpleNamespace(active_server_id=lambda: "srv"))

    class Bridge:
        answers = True

        def __init__(self, adapter=None):
            pass

        def probe_server_identity(self, server_id, timeout_seconds=5):
            pinged.append(server_id)
            if not Bridge.answers:
                raise OSError("unreachable")
            return {"server_version": "1"}
    monkeypatch.setattr(mod, "ProviderCatalogBridge", Bridge)
    assert mod._downloads_serving(SOURCE, "server-a", ["t1"]) is True
    Bridge.answers = False
    assert mod._downloads_serving(SOURCE, None, ["t1"]) is False
    assert pinged == ["server-a", "srv"]


class _TieredCanaryDb(_CanaryDb):
    """No track has a published waveform; the catalogue has ``track``."""

    def cursor(self):
        cursor = super().cursor()
        execute = cursor.execute

        def tiered(sql, params):
            execute(sql, params)
            cursor.published = "published_source_profiles" in sql
        cursor.execute = tiered
        fetch = cursor.fetchone
        cursor.fetchone = lambda: None if cursor.published else fetch()
        return cursor


def _pinging(monkeypatch, mod, pinged):
    monkeypatch.setattr(mod, "get_core_adapter", lambda: types.SimpleNamespace(active_server_id=lambda: "srv"))

    class Bridge:
        def __init__(self, adapter=None):
            pass

        def probe_server_identity(self, server_id, timeout_seconds=5):
            pinged.append(server_id)
            return {"server_version": "1"}  # the ping answers whatever the files do
    monkeypatch.setattr(mod, "ProviderCatalogBridge", Bridge)


def test_without_a_published_waveform_a_catalogue_canary_decides(monkeypatch):
    mod = load_plugin()
    pinged, loaded = [], []
    monkeypatch.setattr(mod, "get_db", lambda: _TieredCanaryDb("new"))
    monkeypatch.setattr(mod, "remove_downloaded_file", lambda path: None)
    _pinging(monkeypatch, mod, pinged)

    def load(track_id, **_kwargs):
        loaded.append(track_id)
        return {"file_path": "/tmp/new", "cleanup_path": "/tmp/new"}
    monkeypatch.setattr(mod, "load_track_file", load)
    assert mod._downloads_serving(SOURCE, "server-a", ["t1", "t2", "t3"]) is True
    assert loaded == ["new"] and pinged == []

    # Storage offline or the API key revoked: the download fails, the ping
    # would answer, and it is an outage.
    monkeypatch.setattr(mod, "load_track_file", lambda *a, **k: (_ for _ in ()).throw(
        mod.MediaDownloadError("down")))
    assert mod._downloads_serving(SOURCE, "server-a", ["t1", "t2", "t3"]) is False
    assert pinged == []


@pytest.mark.parametrize("works", [True, False])
def test_the_canary_downloads_into_a_directory_of_its_own(monkeypatch, tmp_path, works):
    mod = load_plugin()
    monkeypatch.setattr(mod.config, "TEMP_DIR", str(tmp_path / "host-temp"), raising=False)
    monkeypatch.setattr(mod.analysis_download, "_root", lambda host_temp_dir=None: str(tmp_path / "downloads"))
    monkeypatch.setattr(mod, "get_db", lambda: _CanaryDb("busy"))
    monkeypatch.setattr(mod, "remove_downloaded_file", lambda path: None)
    pass_directory = mod.analysis_download.task_directory(mod.config.TEMP_DIR)
    # The pass's own download of the same track, being read by another thread.
    in_flight = os.path.join(pass_directory, "busy.flac")
    with open(in_flight, "wb") as out:
        out.write(b"in flight")
    used = []

    def load(track_id, directory=None, **_kwargs):
        used.append(directory)
        path = os.path.join(directory, f"{track_id}.flac")  # the host names files by track id
        with open(path, "wb") as out:
            out.write(b"canary")
        if not works:
            raise mod.MediaDownloadError("down")
        return {"file_path": path, "cleanup_path": path}
    monkeypatch.setattr(mod, "load_track_file", load)
    assert mod._downloads_serving(SOURCE, "server-a", ["t1", "t2", "t3"]) is works
    [directory] = used
    assert directory and directory not in (pass_directory, mod.config.TEMP_DIR)
    assert os.path.dirname(directory) == pass_directory  # under the root the pass uses
    assert not os.path.exists(directory)  # removed with its contents
    with open(in_flight, "rb") as kept:
        assert kept.read() == b"in flight"


@pytest.mark.parametrize("works", [True, False])
def test_a_canary_whose_rollback_raises_still_cleans_up(monkeypatch, tmp_path, works):
    """Red before the fix: the rollback ran first in the finally; a session
    that died during the download skipped the cleanup and raised."""
    mod = load_plugin()
    monkeypatch.setattr(mod.config, "TEMP_DIR", str(tmp_path / "host-temp"), raising=False)
    monkeypatch.setattr(mod.analysis_download, "_root", lambda host_temp_dir=None: str(tmp_path / "downloads"))

    class DeadDb(_CanaryDb):
        def rollback(self):
            raise RuntimeError("the database session died")
    monkeypatch.setattr(mod, "get_db", lambda: DeadDb("busy"))
    removed, used = [], []
    monkeypatch.setattr(mod, "remove_downloaded_file", removed.append)

    def load(track_id, directory=None, **_kwargs):
        used.append(directory)
        path = os.path.join(directory, f"{track_id}.flac")
        with open(path, "wb") as out:
            out.write(b"canary")
        if not works:
            raise mod.MediaDownloadError("down")
        return {"file_path": path, "cleanup_path": path}
    monkeypatch.setattr(mod, "load_track_file", load)
    assert mod._downloads_serving(SOURCE, "server-a", ["t1"]) is works
    [directory] = used
    assert not os.path.exists(directory)
    assert removed == ([os.path.join(directory, "busy.flac")] if works else [])


def test_a_breaker_whose_check_raises_treats_it_as_an_outage():
    mod = load_plugin()
    released = []
    breaker = mod._DownloadBreaker(released.extend, lambda units: 1 / 0)
    for unit in ("a", "b", "c"):
        breaker.hold(unit, lambda: pytest.fail("charged in an outage"))
    assert breaker.tripped and released == ["a", "b", "c"]


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


@pytest.fixture
def edge_batch(monkeypatch):
    """Edge jobs whose host downloads fail for the ids in ``down``; records the
    job updates, and which jobs reached ``fail_edge_job`` (the counted cap)."""
    mod = load_plugin()
    down, updates, counted = set(), [], []

    def load(track_id, **_kwargs):
        if track_id in down:
            raise mod.MediaDownloadError("media server download failed")
        return {"track_id": track_id, "file_path": "x.flac", "media_signature": "sig", "cleanup_path": None}

    monkeypatch.setattr(mod, "get_db", lambda: None)
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *a, **k: None)
    monkeypatch.setattr(mod, "serve_interactive", lambda *a, **k: 0)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(mod, "opaque_revision", lambda signature: "rev")
    monkeypatch.setattr(mod, "load_track_file", load)
    monkeypatch.setattr(mod, "update_edge_job", lambda db, source, job, status, reason=None: updates.append(
        (job["track_id"], status, reason)) or True)
    monkeypatch.setattr(mod, "fail_edge_job", lambda db, source, job, reason: counted.append(
        (job["track_id"], reason)) or "failed")
    monkeypatch.setattr(mod, "run_file_analysis", lambda *a, **k: (_ for _ in ()).throw(ValueError("decode")))
    serving, backoffs = [False], []  # the canary's answer; passes backed off
    monkeypatch.setattr(mod, "test_serving", serving, raising=False)
    monkeypatch.setattr(mod, "test_backoffs", backoffs, raising=False)
    monkeypatch.setattr(mod, "_downloads_serving", lambda *a: serving[0])
    monkeypatch.setattr(mod, "_back_off_pass", lambda db, state, source: backoffs.append((state, source)))

    def jobs(*ids):
        return [{"track_id": t, "media_revision": "rev", "job_token": "tok"} for t in ids]
    return mod, down, updates, counted, jobs


def test_an_edge_outage_releases_the_batch_without_counting_toward_the_cap(edge_batch):
    mod, down, updates, counted, jobs = edge_batch
    down.update({"t1", "t2", "t3", "t4", "t5"})
    outcomes = mod.analyze_edges_task(jobs("t1", "t2", "t3", "t4", "t5"), SOURCE, "server-a")
    assert counted == []  # EDGE_FAILURE_CAP untouched
    assert sorted(o["track_id"] for o in outcomes) == ["t1", "t2", "t3", "t4", "t5"]
    assert {o["status"] for o in outcomes} == {"released"}
    failed = [u for u in updates if u[1] == "failed"]
    assert failed == [(t, "failed", "edge-enqueue-failed") for t in ("t1", "t2", "t3", "t4", "t5")]
    # t4 and t5 were never started.
    assert [u[0] for u in updates if u[1] == "running"] == ["t1", "t2", "t3"]


def test_a_failed_edge_download_backs_off_uncounted_but_a_decode_error_counts(edge_batch):
    mod, down, updates, counted, jobs = edge_batch
    down.update({"t1"})
    outcomes = mod.analyze_edges_task(jobs("t1", "t2"), SOURCE, "server-a")
    # t2's download settles t1's held failure: the file's, not an outage's.
    assert outcomes == [{"track_id": "t1", "status": "failed"}, {"track_id": "t2", "status": "failed"}]
    assert ("t1", "failed", mod.EDGE_DOWNLOAD_UNAVAILABLE) in updates
    # t2 downloaded and failed to decode: that one still counts toward the cap.
    assert [c[0] for c in counted] == ["t2"]

    # An on-demand single job (no breaker) is not counted either.
    updates.clear()
    outcome, state = mod._prepare_edge(jobs("t1")[0], SOURCE, "server-a")
    assert state is None and outcome == {"track_id": "t1", "status": "failed"}
    assert updates == [("t1", "running", None), ("t1", "failed", mod.EDGE_DOWNLOAD_UNAVAILABLE)]
    assert [c[0] for c in counted] == ["t2"]


def test_an_edge_outage_stops_the_walk_with_its_cursor_before_the_held_tracks(edge_batch, monkeypatch):
    mod, down, updates, counted, jobs = edge_batch
    saved = []
    down.update({"t2", "t3", "t4", "t5"})
    monkeypatch.setattr(mod.parallel_analysis, "current", lambda: None)
    monkeypatch.setattr(mod, "edge_backfill_row", lambda db, source: {"cursor": "a"})
    monkeypatch.setattr(mod, "edge_backfill_candidates",
                        lambda db, source, after, size: ["t1", "t2", "t3", "t4", "t5"])
    monkeypatch.setattr(mod, "update_edge_backfill", lambda db, source, status, **k: saved.append((status, k)))
    monkeypatch.setattr(mod, "claim_edge_jobs", lambda db, source, ids: (jobs(ids[0]), []))
    result = mod.edge_backfill_task("server-a", SOURCE, deadline=None)
    assert result["status"] == "waiting_retry"  # settled, backing off
    assert mod.test_backoffs == [("edge_backfill_state", SOURCE)]
    assert saved[-1][0] == "queued" and saved[-1][1]["cursor"] == "t1"
    assert counted == [("t1", counted[0][1])]  # only t1's decode error counts
    assert [u[0] for u in updates if u[1] == "running"] == ["t1", "t2", "t3", "t4"]
    assert [u for u in updates if u[1] == "failed"] == [
        (t, "failed", "edge-enqueue-failed") for t in ("t2", "t3", "t4")]


def test_missing_edge_files_with_the_server_up_back_off_and_the_walk_completes(edge_batch, monkeypatch):
    mod, down, updates, counted, jobs = edge_batch
    mod.test_serving[0] = True
    saved, rows = [], ["t1", "t2", "t3"]
    down.update(rows)
    monkeypatch.setattr(mod.parallel_analysis, "current", lambda: None)
    monkeypatch.setattr(mod, "edge_backfill_row", lambda db, source: {"cursor": ""})
    monkeypatch.setattr(mod, "edge_backfill_candidates",
                        lambda db, source, after, size: [t for t in rows if t > (after or "")])
    monkeypatch.setattr(mod, "update_edge_backfill", lambda db, source, status, **k: saved.append((status, k)))
    monkeypatch.setattr(mod, "claim_edge_jobs", lambda db, source, ids: (jobs(ids[0]), []))
    result = mod.edge_backfill_task("server-a", SOURCE, deadline=None)
    assert result["status"] == "complete" and result["failed"] == 3
    assert counted == []  # never toward EDGE_FAILURE_CAP
    assert [u for u in updates if u[1] == "failed"] == [
        (t, "failed", mod.EDGE_DOWNLOAD_UNAVAILABLE) for t in rows]
    assert mod.test_backoffs == []


def test_an_on_demand_edge_batch_has_no_breaker(edge_batch):
    mod, down, updates, counted, jobs = edge_batch
    down.update({"t1", "t2", "t3", "t4"})
    outcomes = mod.analyze_edges_task(jobs("t1", "t2", "t3", "t4"), SOURCE, "server-a", "interactive")
    assert outcomes == [{"track_id": t, "status": "failed"} for t in ("t1", "t2", "t3", "t4")]
    assert [u for u in updates if u[1] == "failed"] == [
        (t, "failed", mod.EDGE_DOWNLOAD_UNAVAILABLE) for t in ("t1", "t2", "t3", "t4")]
    assert counted == []


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
