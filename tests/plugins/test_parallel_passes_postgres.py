"""Performance plan WP1b: the waveform batch, the edge pass and the loudness pass
on K analysis threads inside one reconcile tick, on the real migrated schema
and through real analysis children.

* K threads never analyse or publish the same track: each track is
  downloaded once and published once.
* Every database and host call stays on the tick's thread (``get_db`` and the
  download assert it); the threads run only the analysis.
* A file in flight at the deadline runs to completion: a file slower than
  the whole tick is downloaded and published once, and the pass that met it
  reaches the end of the library (1.8.0's test build killed it 5 s after
  every deadline, so it restarted every tick and its pass never finished).
* Host shapes (the scheduler plan's matrix): on 3.3.0-3.5.1 (one-thread
  jobs) and 3.5.2+ (a heartbeat thread beside the plugin call) K > 1 analyses
  every file once on the exec path and leaves no thread or worker behind; K = 1
  on a single-threaded process still takes the fork path.
"""
import os
import shutil
import threading
import time

import pytest

import analysis_isolation_fakes as fakes
from test_analysis_isolation import _flac
from test_reconcile_scheduler_postgres import (  # noqa: F401  (fixtures)
    P, SOURCE, SERVER, _queued_backfill, _rows, _track, tick,
)
from test_reconcile_budget_postgres import _edge_library
from test_host_compatibility_postgres import SHAPES, _host
from plugins.LumaeAnalysis import analysis_isolation as iso
from plugins.LumaeAnalysis import loudness_backfill, loudness_stream
from plugins.LumaeAnalysis import parallel_analysis as par
from plugins.LumaeAnalysis import reconcile

MAIN = threading.main_thread()


@pytest.fixture(autouse=True, scope="module")
def _stop_workers_at_end():
    yield
    iso.shutdown()


def _on_main():
    assert threading.current_thread() is MAIN, "a database or host call from an analysis thread"


@pytest.fixture
def parallel(tick, monkeypatch, tmp_path):
    """The tick with K = 3; downloads hand out a fresh copy of one real FLAC."""
    mod, db = tick
    source = _flac(tmp_path / "library", seconds=8.0)
    downloads = []

    def get_db():
        _on_main()
        return db

    for module in (mod, reconcile):
        monkeypatch.setattr(module, "get_db", get_db)

    def load(track_id, catalog_instance_id=None, server_id=None, defer=False):
        _on_main()
        downloads.append(track_id)
        copy = tmp_path / "downloads" / f"{track_id}-{len(downloads)}.flac"
        copy.parent.mkdir(exist_ok=True)
        shutil.copy(source, copy)
        signature = mod.test_signature(track_id)
        return {"track_id": track_id, "file_path": str(copy), "media_signature": signature,
                "cleanup_path": str(copy)}

    mod.test_signature = lambda _track_id: "catalog-media:revision-a"
    monkeypatch.setattr(mod, "load_track_file", load)
    monkeypatch.setattr(mod, "background_workers", lambda: 3)
    served = []

    def serve(*_args, **_kwargs):
        _on_main()
        served.append(time.monotonic())
        return 0

    monkeypatch.setattr(mod, "serve_deferred_interactive", serve)
    yield mod, db, downloads, served, tmp_path
    # Every download was removed, cancelled files' too.
    assert not list(tmp_path.glob("downloads/*"))


def _edge_tracks(db, names):
    """Published waveform profiles (the edge pass's input) of catalogue tracks
    whose media fingerprint is their id."""
    _edge_library(db, names)
    with db.cursor() as cur:
        for name in names:
            cur.execute(
                f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, track_id, "
                "title, metadata_fp, media_fp, analysis_eligible, payload, first_seen_at, last_seen_at) "
                "VALUES (%s, 1, %s, %s, 'metadata', %s, TRUE, '{}'::jsonb, now(), now())",
                (SOURCE, name, name, name))
    db.commit()


def _slow(mod, monkeypatch, track_id, seconds):
    """The analysis of ``track_id``'s download takes ``seconds`` longer
    (analysis threads only); returns the calls' track ids."""
    real, calls = mod.run_file_analysis, []

    def analyse(analyzer, path, **kwargs):
        assert threading.current_thread() is not MAIN
        name = os.path.basename(path)
        calls.append(name.rsplit("-", 1)[0])
        if name.startswith(f"{track_id}-"):
            time.sleep(seconds)
        return real(analyzer, path, **kwargs)

    monkeypatch.setattr(mod, "run_file_analysis", analyse)
    # 1.8.0's 5 s drain window, scaled to these ticks (gone since).
    monkeypatch.setattr(par, "DRAIN_SECONDS", 0.5, raising=False)
    return calls


# The fork and exec paths exist only where analysis is isolated (POSIX);
# elsewhere analysis runs in-process and neither is called.
posix_isolation = pytest.mark.skipif(os.name != "posix", reason="POSIX process isolation")


def _until_complete(task, ticks=6, tick_seconds=1.0):
    """Turns of a library pass, each with a short tick, until one completes."""
    results = []
    for _ in range(ticks):
        with par.tick_scope(3):
            results.append(task(SERVER, SOURCE, deadline=time.monotonic() + tick_seconds))
        if results[-1]["status"] == "complete":
            break
    return results


def _tracks(db, count):
    names = [f"track-{index:02d}" for index in range(count)]
    for name in names:
        _track(db, name)
    return names


def test_a_background_waveform_batch_runs_on_k_threads_once_per_track(parallel):
    mod, db, downloads, served, _tmp = parallel
    names = _tracks(db, 12)
    _queued_backfill(db)

    result = mod.catalog_reconcile_task()

    assert [entry["action"] for entry in result["actions"]] == ["profile_backfill"]
    assert _rows(db, f"SELECT track_id, status FROM {P}source_profiles ORDER BY track_id") == [
        (name, "ready") for name in names]
    assert sorted(downloads) == names  # one download per track, none twice
    assert len(served) >= len(names)  # on-demand work went first whenever a thread freed
    assert _rows(db, f"SELECT count(*) FROM {P}published_source_profiles")[0][0] == 12
    assert not [thread for thread in threading.enumerate() if thread.name.startswith("lumae-analysis")]


def test_a_waveform_slower_than_the_tick_finishes_in_the_tick_that_started_it(parallel, monkeypatch):
    mod, db, downloads, served, _tmp = parallel
    names = _tracks(db, 6)
    _queued_backfill(db)
    _slow(mod, monkeypatch, "track-01", 3.0)
    monkeypatch.setattr(par, "WAIT_SERVE_SECONDS", 0.5)

    began = time.monotonic()
    with par.tick_scope(3):
        result = mod.profile_backfill_task(SERVER, SOURCE, deadline=began + 1)
    elapsed = time.monotonic() - began

    assert elapsed >= 3.0, elapsed  # it ran past the deadline to its end
    assert downloads.count("track-01") == 1
    assert _rows(db, f"SELECT status FROM {P}source_profiles WHERE track_id='track-01'") == [("ready",)]
    assert result["failed"] == 0 and result["ready"] == len(downloads), result
    # Tracks it never started are released, not failed: due again at once.
    assert result["deferred"] == len(names) - len(downloads)
    assert not _rows(db, f"SELECT 1 FROM {P}source_profiles WHERE status='failed'")
    assert _rows(db, f"SELECT DISTINCT retry_count FROM {P}source_profiles") == [(0,)]
    # On-demand work was served while the tick waited for it.
    assert [at for at in served if at > began + 1.5]


def test_the_edge_pass_runs_on_k_threads_and_finishes_the_library(parallel, monkeypatch):
    mod, db, downloads, _served, _tmp = parallel
    names = [f"track-{index:02d}" for index in range(7)]
    _edge_tracks(db, names)
    mod.test_signature = lambda track_id: f"catalog-media:{track_id}"
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(mod, "EDGE_BACKFILL_PAGE_SIZE", 3)  # pages read as threads free

    with par.tick_scope(3):
        result = mod.edge_backfill_task(SERVER, SOURCE, deadline=time.monotonic() + 60)

    assert result["status"] == "complete" and result.get("ready") == 7, result
    assert sorted(downloads) == names
    assert _rows(db, f"SELECT track_id FROM {P}edge_profiles ORDER BY track_id") == [(n,) for n in names]
    assert _rows(db, f"SELECT status, cursor FROM {P}edge_backfill_state") == [("complete", "")]
    # LN.4a: each edge's decode measured its loudness too.
    assert _rows(db, f"SELECT count(*) FROM {P}loudness_tracks")[0][0] == 7


def test_an_edge_slower_than_the_tick_completes_and_the_edge_pass_reaches_its_end(parallel, monkeypatch):
    mod, db, downloads, _served, _tmp = parallel
    names = [f"track-{index:02d}" for index in range(5)]
    _edge_tracks(db, names)
    mod.test_signature = lambda track_id: f"catalog-media:{track_id}"
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    _slow(mod, monkeypatch, "track-01", 3.0)

    results = _until_complete(mod.edge_backfill_task)

    assert results[-1]["status"] == "complete", results
    assert not any(result.get("released") for result in results), results
    assert downloads.count("track-01") == 1 and sorted(set(downloads)) == names
    assert _rows(db, f"SELECT track_id FROM {P}edge_profiles ORDER BY track_id") == [(n,) for n in names]
    assert _rows(db, f"SELECT status, cursor FROM {P}edge_backfill_state") == [("complete", "")]


def test_the_loudness_pass_runs_on_k_threads_and_publishes_by_page(parallel, monkeypatch):
    mod, db, downloads, _served, _tmp = parallel
    names = _tracks(db, 7)
    loudness_backfill.ensure_sources(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)
    publications = []
    publish = loudness_stream.publish_tracks
    monkeypatch.setattr(loudness_stream, "publish_tracks", lambda cur, source, measured: (
        _on_main(), publications.append(len(measured)), publish(cur, source, measured))[-1])

    with par.tick_scope(3):
        result = mod.loudness_backfill_task(SERVER, SOURCE, deadline=time.monotonic() + 60)

    assert (result["status"], result["ready"], result["failed"]) == ("complete", 7, 0), result
    assert sorted(downloads) == names
    assert publications == [3, 3, 1]
    page = loudness_stream.bootstrap_page(db, SOURCE)
    assert sorted(i["track_id"] for i in page["items"] if i["kind"] == "track") == names
    assert loudness_backfill.state(db, SOURCE)["cursor"] == ""


def test_a_loudness_measurement_slower_than_the_tick_completes_and_the_pass_reaches_its_end(
        parallel, monkeypatch):
    mod, db, downloads, _served, _tmp = parallel
    names = _tracks(db, 5)
    loudness_backfill.ensure_sources(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)
    _slow(mod, monkeypatch, "track-01", 3.0)

    results = _until_complete(mod.loudness_backfill_task)

    assert results[-1]["status"] == "complete", results
    assert sum(result["ready"] for result in results) == 5
    assert not any(result["failed"] for result in results), results
    assert downloads.count("track-01") == 1 and sorted(set(downloads)) == names
    page = loudness_stream.bootstrap_page(db, SOURCE)
    assert sorted(i["track_id"] for i in page["items"] if i["kind"] == "track") == names
    assert loudness_backfill.state(db, SOURCE)["cursor"] == ""


@posix_isolation
@pytest.mark.parametrize("name", ["3.3.0-3.5.1", "3.5.2-3.6.1"])
@pytest.mark.parametrize("workers", [1, 2])
def test_k_threads_on_the_host_shapes(parallel, monkeypatch, name, workers):
    mod, db, downloads, _served, _tmp = parallel
    shape = SHAPES[name]
    _host(db, monkeypatch, shape)
    names = _tracks(db, 4)
    _queued_backfill(db)
    monkeypatch.setattr(mod, "background_workers", lambda: workers)
    paths = []
    for path in ("_run_forked", "_run_exec"):
        real = getattr(iso, path)
        monkeypatch.setattr(iso, path, lambda *a, _real=real, _name=path, **k: (
            paths.append(_name), _real(*a, **k))[1])
    stop = threading.Event()
    heartbeat = threading.Thread(target=stop.wait, daemon=True) if shape["heartbeat"] else None
    if heartbeat is not None:
        heartbeat.start()
    try:
        result = mod.catalog_reconcile_task()
    finally:
        stop.set()
        if heartbeat is not None:
            heartbeat.join()

    assert result["status"] == "processed"
    assert sorted(downloads) == names
    assert _rows(db, f"SELECT DISTINCT status FROM {P}source_profiles") == [("ready",)]
    one_thread = workers == 1 and not shape["heartbeat"]
    assert set(paths) == ({"_run_forked"} if one_thread else {"_run_exec"})
    assert not [thread for thread in threading.enumerate() if thread.name.startswith("lumae-analysis")]
    assert not iso._dedicated_slots
