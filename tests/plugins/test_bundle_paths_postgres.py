"""Performance plan WP2 on the real migrated schema: one download and one
analysis call make the waveform, the edge and the loudness of a track.

* The waveform batch, on-demand work and the AudioMuse song hook publish all
  three from one file: with edges on each track is downloaded once, and the
  edge pass and loudness backfill find nothing left to do.
* A 7.1 FLAC: the waveform is ready, the edge unsupported, the loudness from
  the standalone meter, in the same call.
* Edges off: the waveform only.
* Jellyfin tags are read once, from the shared file.
* Lumae's own original-file download: a Navidrome server is asked for
  ``format=raw``; without credentials (or when the server refuses) the host's
  download takes over.
"""
import contextlib
import shutil
import threading
import time
import types

import pytest

from test_analysis_bundle import _write
from test_reconcile_scheduler_postgres import (  # noqa: F401  (fixtures)
    P, SOURCE, SERVER, _queued_backfill, _rows, _track, tick,
)
from plugins.LumaeAnalysis import analysis_download, analysis_isolation as iso
from plugins.LumaeAnalysis import collection_library, edge_profile_store, loudness_backfill
from plugins.LumaeAnalysis import parallel_analysis as par


@pytest.fixture(autouse=True, scope="module")
def _stop_workers_at_end():
    yield
    iso.shutdown()


@pytest.fixture
def library(tick, monkeypatch, tmp_path):
    """The tick with edges on; downloads hand out a copy of ``library.source``."""
    mod, db = tick
    state = types.SimpleNamespace(mod=mod, db=db, downloads=[], tags=[], tmp=tmp_path,
                                  source=_write(tmp_path / "song.flac", seconds=8.0))

    def load(track_id, catalog_instance_id=None, server_id=None, defer=False):
        state.downloads.append(track_id)
        copy = tmp_path / "downloads" / f"{track_id}-{len(state.downloads)}.flac"
        copy.parent.mkdir(exist_ok=True)
        shutil.copy(state.source, copy)
        return {"track_id": track_id, "file_path": str(copy), "media_signature": "catalog-media:revision-a",
                "cleanup_path": str(copy)}

    monkeypatch.setattr(mod, "load_track_file", load)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)
    monkeypatch.setattr(mod.file_tags, "capture", lambda db, source, track, path: state.tags.append(
        (track, path)))
    monkeypatch.setattr(mod, "enqueue_bounded", lambda *a, **k: pytest.fail("a root task per track"))
    return state


def _edges(db):
    return _rows(db, f"SELECT track_id FROM {P}edge_profiles ORDER BY track_id")


def _loudness(db):
    return _rows(db, f"SELECT track_id FROM {P}loudness_tracks ORDER BY track_id")


@pytest.mark.parametrize("workers", [1, 3])
def test_one_download_per_track_makes_all_three(library, monkeypatch, workers):
    mod, db = library.mod, library.db
    names = [f"track-{index:02d}" for index in range(5)]
    for name in names:
        _track(db, name)
    _queued_backfill(db)
    monkeypatch.setattr(mod, "background_workers", lambda: workers)

    mod.catalog_reconcile_task()

    assert _rows(db, f"SELECT DISTINCT status FROM {P}source_profiles") == [("ready",)]
    assert _edges(db) == [(name,) for name in names]
    assert _loudness(db) == [(name,) for name in names]
    assert sorted(library.downloads) == names  # once each: no edge or loudness download
    assert sorted(track for track, _path in library.tags) == names  # tags read once, same file
    assert edge_profile_store.edge_backfill_candidates(db, SOURCE) == []
    assert loudness_backfill.candidates(db, SOURCE, "", 100) == []


def test_a_surround_file_gets_its_waveform_an_unsupported_edge_and_loudness(library):
    mod, db = library.mod, library.db
    library.source = _write(library.tmp / "surround.flac", seconds=8.0, channels=8)
    _track(db, "track-71")
    _queued_backfill(db)

    mod.catalog_reconcile_task()

    assert _rows(db, f"SELECT status FROM {P}source_profiles") == [("ready",)]
    [(status, reason)] = _rows(db, f"SELECT status, last_error FROM {P}edge_profile_jobs")
    assert status == "unsupported" and reason.startswith("edge-analysis-unavailable")
    if iso._isolation_available():
        # The analysis child's decode probe adds the stream facts; without
        # isolation (Windows) the analyzer runs in-process and has none.
        assert "layout=7.1" in reason
    assert _edges(db) == [] and _loudness(db) == [("track-71",)]
    assert library.downloads == ["track-71"]


def test_edges_off_makes_the_waveform_only(library, monkeypatch):
    mod, db = library.mod, library.db
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    _track(db, "track-a")
    _queued_backfill(db)
    analysed = []
    real = mod.run_file_analysis
    monkeypatch.setattr(mod, "run_file_analysis", lambda analyzer, path, **kw: (
        analysed.append(analyzer.__name__), real(analyzer, path, **kw))[1])

    mod.catalog_reconcile_task()

    assert analysed == ["analyze_file"]
    assert _edges(db) == [] and _loudness(db) == []
    assert library.downloads == ["track-a"]


def test_on_demand_work_makes_its_edge_in_the_same_call(library):
    mod, db = library.mod, library.db
    _track(db, "track-a")
    tokens = mod.mark_pending(["track-a"], catalog_instance_id=SOURCE, priority="interactive")

    outcome = mod.analyze_one_track("track-a", SOURCE, SERVER, tokens["track-a"], priority="interactive")

    assert outcome == {"track_id": "track-a", "status": "ready"}
    assert _edges(db) == [("track-a",)] and _loudness(db) == [("track-a",)]
    assert library.downloads == ["track-a"]


def test_the_song_hook_writes_all_three_from_the_hooks_file(library, monkeypatch):
    mod, db = library.mod, library.db
    _track(db, "track-a")
    adapter = types.SimpleNamespace(
        active_server_id=lambda: SERVER,
        normalize_analysis_hook=lambda song: {"server_id": SERVER, "run_id": ""})
    monkeypatch.setattr(mod, "get_core_adapter", lambda: adapter)
    monkeypatch.setattr(mod, "arm_profile_claim_recovery", lambda *a, **k: False)

    outcome = mod.analyze_song_hook({"item_id": "track-a", "audio_path": library.source})

    assert outcome == {"track_id": "track-a", "status": "ready"}
    assert _edges(db) == [("track-a",)] and _loudness(db) == [("track-a",)]
    assert library.downloads == []  # the hook's own file


# ------------------------------------------------------------ own download


class _Bridge:
    def __init__(self, provider_type, module):
        self.provider_type, self.module = provider_type, module

    @contextlib.contextmanager
    def bound_module(self, server_id):
        yield {"server_id": server_id, "provider_type": self.provider_type}, self.module


@pytest.fixture
def own_download(tick, monkeypatch, tmp_path):
    """load_track_file against a Navidrome server with these credentials."""
    mod, db = tick
    _track(db, "track-a")
    fetched, hosted = [], []

    def fetch(target, cancelled=None, session=None):
        fetched.append(dict(target.params))
        with open(target.path, "wb") as out:
            out.write(b"fLaC")
        return target.path

    monkeypatch.setattr(analysis_download, "fetch", fetch)

    class Bridge:
        def __init__(self, _adapter):
            pass

        def download_track(self, server_id, temp_dir, item):
            hosted.append(item["id"])
            path = tmp_path / "hosted.flac"
            path.write_bytes(b"fLaC")
            return str(path)

    monkeypatch.setattr(mod, "ProviderCatalogBridge", Bridge)
    monkeypatch.setattr(mod.config, "TEMP_DIR", str(tmp_path / "host-temp"), raising=False)
    monkeypatch.setattr(analysis_download.tempfile, "gettempdir", lambda: str(tmp_path))
    mod._own_download_refused.clear()

    def configure(auth):
        module = types.SimpleNamespace()
        monkeypatch.setattr(collection_library, "_provider_bridge", lambda: _Bridge("navidrome", module))
        monkeypatch.setattr(collection_library, "_navidrome_access",
                            lambda _module: ("http://navidrome:4533", auth, _module))

    return mod, fetched, hosted, configure


def test_the_original_file_is_fetched_with_format_raw(own_download):
    mod, fetched, hosted, configure = own_download
    configure({"u": "lumae", "t": "token", "s": "salt"})

    info = mod.load_track_file("track-a", catalog_instance_id=SOURCE, server_id=SERVER)

    assert fetched == [{"id": "track-a", "u": "lumae", "t": "token", "s": "salt", "format": "raw"}]
    assert hosted == [] and info["media_signature"] == "catalog-media:revision-a"
    mod.remove_downloaded_file(info["cleanup_path"])


def test_without_credentials_the_host_downloads(own_download):
    mod, fetched, hosted, configure = own_download
    configure({})

    info = mod.load_track_file("track-a", catalog_instance_id=SOURCE, server_id=SERVER)

    assert fetched == [] and hosted == ["track-a"] and info["file_path"].endswith("hosted.flac")


def test_a_refused_download_falls_back_to_the_host_for_the_rest_of_the_tick(own_download, monkeypatch):
    mod, _fetched, hosted, configure = own_download
    configure({"u": "lumae", "t": "token", "s": "salt"})
    attempts = []

    def refuse(target, cancelled=None, session=None):
        attempts.append(1)
        raise analysis_download.DownloadError("download refused (HTTP 501)")

    monkeypatch.setattr(analysis_download, "fetch", refuse)
    for _ in range(2):
        mod.load_track_file("track-a", catalog_instance_id=SOURCE, server_id=SERVER)

    assert attempts == [1] and hosted == ["track-a", "track-a"]


def test_a_parallel_pass_defers_the_fetch_to_the_analysis_thread(own_download):
    mod, fetched, hosted, configure = own_download
    configure({"u": "lumae", "t": "token", "s": "salt"})

    info = mod.load_track_file("track-a", catalog_instance_id=SOURCE, server_id=SERVER, defer=True)

    assert fetched == [] and isinstance(info["download"], analysis_download.Target)
    seen = {}
    thread = threading.Thread(target=lambda: seen.update(path=mod.fetch_track_file(info)))
    thread.start()
    thread.join(10)
    assert seen["path"] == info["file_path"] and fetched[0]["format"] == "raw"
    mod.remove_downloaded_file(info["cleanup_path"])
