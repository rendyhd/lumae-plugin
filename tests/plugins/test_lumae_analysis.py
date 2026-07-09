import importlib
import json
import pathlib
import sys
import math
import types

import numpy as np

from flask import Flask


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

plugin_module = types.ModuleType("plugin")
plugin_api_module = types.ModuleType("plugin.api")
plugin_api_module.config = types.SimpleNamespace()
plugin_api_module.enqueue = lambda *args, **kwargs: None
plugin_api_module.get_db = lambda: None
plugin_api_module.get_setting = lambda _key, default=None: default
plugin_api_module.logger = types.SimpleNamespace(
    warning=lambda *args, **kwargs: None,
    exception=lambda *args, **kwargs: None,
)
plugin_api_module.render_page = lambda body, title=None: body
plugin_api_module.set_setting = lambda _key, _value: None
plugin_api_module.table = lambda name: f"plugin_lumae_analysis__{name}"
sys.modules.setdefault("plugin", plugin_module)
sys.modules.setdefault("plugin.api", plugin_api_module)

PLUGIN_TABLE = "plugin_lumae_analysis__profiles"


def load_plugin():
    return importlib.import_module("plugins.LumaeAnalysis")


def plugin_client(mod):
    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    return app.test_client()


def test_plugin_manifest_has_lumae_identity():
    with open("plugins/LumaeAnalysis/plugin.json", "r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    assert manifest["id"] == "lumae_analysis"
    assert manifest["name"] == "Lumae Analysis"
    assert manifest["requirements"] == []
    assert manifest["versions"][0]["version"] == "0.2.1"
    assert manifest["versions"][0]["min_core_version"] == "2.6.0"
    assert manifest["capabilities"] == {
        "lumae_analysis_profiles": {
            "schema_version": 1,
            "analyzer_version": 1,
            "profile_source": "waveform",
            "features": ["loudness", "mix_ramp"],
        },
    }


def test_health_endpoint_reports_schema_and_analyzer_versions(monkeypatch):
    mod = load_plugin()
    client = plugin_client(mod)

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.get_json() == {
        "plugin": "lumae_analysis",
        "schema_version": 1,
        "analyzer_version": 1,
        "status": "ok",
    }


def test_profiles_endpoint_splits_ready_missing_and_failed(monkeypatch):
    mod = load_plugin()
    rows = [
        {
            "track_id": "ready-1",
            "sample_rate": 44100,
            "duration_ms": 123000,
            "ref_lufs": -13.25,
            "start_ramp": b"\xe9\x03\x00",
            "end_ramp": b"\xe9\x04\x00",
            "analyzer_ver": 1,
            "analyzed_at": "2026-07-06T12:00:00Z",
            "media_signature": "sig-ready",
            "status": "ready",
            "last_error": None,
        },
        {
            "track_id": "failed-1",
            "sample_rate": 0,
            "duration_ms": 0,
            "ref_lufs": 0,
            "start_ramp": b"",
            "end_ramp": b"",
            "analyzer_ver": 1,
            "analyzed_at": "2026-07-06T12:00:00Z",
            "media_signature": "sig-failed",
            "status": "failed",
            "last_error": "decode failed",
        },
        {
            "track_id": "skipped-1",
            "sample_rate": 0,
            "duration_ms": 0,
            "ref_lufs": 0,
            "start_ramp": b"",
            "end_ramp": b"",
            "analyzer_ver": 1,
            "analyzed_at": "2026-07-06T12:00:00Z",
            "media_signature": None,
            "status": "skipped_no_file",
            "last_error": "missing file path",
        },
    ]
    monkeypatch.setattr(mod, "fetch_profile_rows", lambda ids: rows)
    client = plugin_client(mod)

    response = client.get("/api/profiles?ids=ready-1,missing-1,failed-1,skipped-1")

    assert response.status_code == 200
    body = response.get_json()
    assert body["schema_version"] == 1
    assert body["analyzer_version"] == 1
    assert body["profiles"][0]["track_id"] == "ready-1"
    assert body["profiles"][0]["source"] == "waveform"
    assert body["profiles"][0]["start_ramp"] == "6QMA"
    assert body["missing"] == ["missing-1"]
    assert body["failed"] == [
        {"track_id": "failed-1", "reason": "decode failed"},
        {"track_id": "skipped-1", "reason": "missing file path"},
    ]


def test_analyze_endpoint_enqueues_only_missing_or_stale_ids(monkeypatch):
    mod = load_plugin()
    calls = []
    rows = [
        {"track_id": "ready-1", "status": "ready"},
        {"track_id": "pending-1", "status": "pending"},
        {"track_id": "stale-1", "status": "stale"},
    ]
    monkeypatch.setattr(mod, "fetch_profile_rows", lambda ids: rows)
    monkeypatch.setattr(mod, "mark_pending", lambda ids: calls.append(("mark_pending", ids, "default")))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, ids, queue="default": calls.append((func.__name__, ids, queue)),
    )
    client = plugin_client(mod)

    response = client.post(
        "/api/analyze",
        json={"ids": ["ready-1", "pending-1", "stale-1", "missing-1"]},
    )

    assert response.status_code == 202
    assert response.get_json() == {
        "accepted": ["stale-1", "missing-1"],
        "already_ready": ["ready-1"],
        "already_pending": ["pending-1"],
    }
    assert calls == [
        ("mark_pending", ["stale-1", "missing-1"], "default"),
        ("analyze_tracks_task", ["stale-1", "missing-1"], "default"),
    ]


def test_analyze_song_hook_uses_analysis_audio_path_and_raw_media_item(monkeypatch, tmp_path):
    mod = load_plugin()
    audio = tmp_path / "analysis-hook.flac"
    audio.write_bytes(b"hook audio")
    db = FakeDb(rows=[])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    class Result:
        sample_rate = 44100
        duration_ms = 2500
        ref_lufs = -15.5
        start_ramp_blob = b"\x01\x02\x03"
        end_ramp_blob = b"\x04\x05\x06"

    seen = {}

    def fake_analyze_file(path):
        seen["path"] = path
        return Result()

    monkeypatch.setattr(mod, "analyze_file", fake_analyze_file)

    result = mod.analyze_song_hook(
        {
            "item_id": "track-a",
            "audio_path": str(audio),
            "metadata": {"file_path": "/metadata/path.flac"},
            "media_item": {"Id": "raw-track", "FilePath": "/music/raw-song.flac"},
        }
    )

    assert result == {"track_id": "track-a", "status": "ready"}
    assert seen["path"] == str(audio)
    assert audio.exists()
    params = db.cursor_obj.executed[-1][1]
    assert params[0] == "track-a"
    assert params[1] == 44100
    assert params[8].startswith("analysis-hook|track-a|/music/raw-song.flac|")
    assert params[10] == "ready"


def test_encode_ramp_matches_lumae_byte_layout():
    from plugins.LumaeAnalysis.ramp_codec import encode_ramp

    assert encode_ramp([(-17, 3), (0, 513)]) == bytes([239, 3, 0, 0, 1, 2])


def test_analyze_buffer_produces_waveform_profile():
    from plugins.LumaeAnalysis.loudness import analyze_buffer

    sr = 48000
    t = np.arange(sr * 2, dtype=np.float32) / sr
    audio = np.sin(2 * np.pi * 440 * t).astype(np.float32) * 0.25

    result = analyze_buffer(audio, sr)

    assert result.sample_rate == sr
    assert result.duration_ms == 2000
    assert math.isfinite(result.ref_lufs)
    assert result.start_ramp
    assert result.end_ramp
    assert result.start_ramp_blob
    assert result.end_ramp_blob


def test_analyze_buffer_uses_100ms_chunks_and_expected_ramp_encoding(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    monkeypatch.setattr(
        loudness,
        "_k_weight",
        lambda channel: channel.astype(np.float64, copy=False),
    )
    monkeypatch.setattr(loudness, "_integrated_lufs", lambda chunk_lufs: -20.0)

    audio = np.array([0.0, 0.1, 0.31622777, 1.0, 1.9952623], dtype=np.float32)

    result = loudness.analyze_buffer(audio, 10)

    assert result.sample_rate == 10
    assert result.duration_ms == 500
    assert result.start_ramp == [
        (-90, 1),
        (-60, 1),
        (-40, 1),
        (-30, 1),
        (-24, 1),
        (-21, 1),
        (-18, 1),
        (-15, 1),
        (-12, 1),
        (-9, 1),
        (-6, 1),
        (-3, 1),
        (0, 2),
        (3, 2),
        (6, 2),
    ]
    assert result.end_ramp == [
        (-90, 0),
        (-60, 0),
        (-40, 0),
        (-30, 0),
        (-24, 0),
        (-21, 0),
        (-18, 0),
        (-15, 0),
        (-12, 0),
        (-9, 0),
        (-6, 0),
        (-3, 0),
        (0, 0),
        (3, 0),
        (6, 0),
    ]
    assert result.start_ramp_blob == bytes(
        [
            166, 1, 0, 196, 1, 0, 216, 1, 0, 226, 1, 0, 232, 1, 0,
            235, 1, 0, 238, 1, 0, 241, 1, 0, 244, 1, 0, 247, 1, 0,
            250, 1, 0, 253, 1, 0, 0, 2, 0, 3, 2, 0, 6, 2, 0,
        ]
    )
    assert result.end_ramp_blob == bytes(
        [
            166, 0, 0, 196, 0, 0, 216, 0, 0, 226, 0, 0, 232, 0, 0,
            235, 0, 0, 238, 0, 0, 241, 0, 0, 244, 0, 0, 247, 0, 0,
            250, 0, 0, 253, 0, 0, 0, 0, 0, 3, 0, 0, 6, 0, 0,
        ]
    )


def test_analyze_buffer_includes_final_partial_chunk(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    monkeypatch.setattr(
        loudness,
        "_k_weight",
        lambda channel: channel.astype(np.float64, copy=False),
    )
    monkeypatch.setattr(loudness, "_integrated_lufs", lambda chunk_lufs: -20.0)

    audio = np.array([0.0, 0.0, 0.1, 0.1, 1.9952623], dtype=np.float32)

    result = loudness.analyze_buffer(audio, 20)

    assert result.duration_ms == 250
    assert result.start_ramp[-3:] == [(0, 2), (3, 2), (6, 2)]
    assert result.end_ramp[:3] == [(-90, 0), (-60, 0), (-40, 0)]
    assert result.end_ramp[-3:] == [(0, 0), (3, 0), (6, 0)]


def test_analyze_buffer_rejects_silent_audio():
    from plugins.LumaeAnalysis.loudness import SilentAudioError, analyze_buffer

    audio = np.zeros(48000, dtype=np.float32)

    try:
        analyze_buffer(audio, 48000)
    except SilentAudioError as exc:
        assert "silent or sub-gate" in str(exc)
    else:
        raise AssertionError("silent audio should fail")


def test_analyze_file_loads_audio_and_delegates_to_buffer(monkeypatch):
    import plugins.LumaeAnalysis.loudness as loudness

    captured = {}
    audio = np.array([0.25, -0.25], dtype=np.float32)
    sentinel = object()

    def fake_load(path, sr=None, mono=False):
        captured["load"] = (path, sr, mono)
        return audio, 44100

    def fake_analyze_buffer(buffer, sample_rate):
        captured["analyze"] = (buffer, sample_rate)
        return sentinel

    monkeypatch.setattr(loudness, "librosa", types.SimpleNamespace(load=fake_load))
    monkeypatch.setattr(loudness, "analyze_buffer", fake_analyze_buffer)

    result = loudness.analyze_file("fixture.wav")

    assert result is sentinel
    assert captured["load"] == ("fixture.wav", None, False)
    assert captured["analyze"] == (audio, 44100)


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.description = [("item_id",), ("file_path",)]
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeDb:
    def __init__(self, rows=None):
        self.cursor_obj = FakeCursor(rows)
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class LimitAwareCursor(FakeCursor):
    def fetchall(self):
        if not self.executed:
            return self.rows
        _, params = self.executed[-1]
        if params:
            return self.rows[: int(params[0])]
        return self.rows


class LimitAwareDb(FakeDb):
    def __init__(self, rows=None):
        self.cursor_obj = LimitAwareCursor(rows)
        self.commits = 0


class CronCursor(FakeCursor):
    def __init__(self, existing=None):
        super().__init__(rows=[])
        self.existing = existing

    def fetchone(self):
        return self.existing


class CronDb(FakeDb):
    def __init__(self, existing=None):
        self.cursor_obj = CronCursor(existing)
        self.commits = 0


class FakeCtx:
    def __init__(self):
        self.blueprints = []
        self.settings_endpoint = None
        self.install_hooks = []
        self.song_hooks = []
        self.cron_tasks = []

    def add_blueprint(self, blueprint):
        self.blueprints.append(blueprint)

    def set_settings_page(self, endpoint):
        self.settings_endpoint = endpoint

    def on_install(self, func):
        self.install_hooks.append(func)

    def on_song_analyzed(self, func):
        self.song_hooks.append(func)

    def add_cron_task(self, name, func, queue="default"):
        self.cron_tasks.append((name, func, queue))


def test_analyze_one_track_marks_missing_file(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=[]))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    result = mod.analyze_one_track("missing")

    assert result == {"track_id": "missing", "status": "skipped_no_file"}


def test_analyze_one_track_downloads_from_media_server_when_local_file_missing(monkeypatch, tmp_path):
    mod = load_plugin()
    library_path = tmp_path / "not-mounted" / "album" / "song.flac"
    downloaded = tmp_path / "downloaded.flac"
    downloaded.write_bytes(b"downloaded media")
    db = FakeDb(rows=[("track-a", str(library_path), "Song Title", "Artist Name")])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)

    class Result:
        sample_rate = 48000
        duration_ms = 1000
        ref_lufs = -14.0
        start_ramp_blob = b"\xe9\x03\x00"
        end_ramp_blob = b"\xe9\x04\x00"

    downloaded_items = []
    seen = {}

    def fake_download_track_to_temp(item):
        downloaded_items.append(item)
        return str(downloaded)

    def fake_analyze_file(path):
        seen["path"] = path
        return Result()

    monkeypatch.setattr(mod, "download_track_to_temp", fake_download_track_to_temp, raising=False)
    monkeypatch.setattr(mod, "analyze_file", fake_analyze_file)

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "ready"}
    assert seen["path"] == str(downloaded)
    assert downloaded_items[0]["id"] == "track-a"
    assert downloaded_items[0]["Id"] == "track-a"
    assert downloaded_items[0]["path"] == str(library_path)
    assert downloaded_items[0]["Path"] == str(library_path)
    assert downloaded_items[0]["Name"] == "Song Title"
    assert downloaded_items[0]["suffix"] == "flac"
    assert not downloaded.exists()


def test_analyze_one_track_marks_media_server_download_failure_as_failed(monkeypatch, tmp_path):
    mod = load_plugin()
    library_path = tmp_path / "not-mounted" / "song.flac"
    db = FakeDb(rows=[("track-a", str(library_path), "Song Title", "Artist Name")])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)
    monkeypatch.setattr(mod, "download_track_to_temp", lambda item: None, raising=False)

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "failed"}
    assert db.cursor_obj.executed[-1][1][-1] == "media server download failed"


def test_analyze_one_track_cleans_downloaded_file_when_analysis_fails(monkeypatch, tmp_path):
    mod = load_plugin()
    library_path = tmp_path / "not-mounted" / "song.flac"
    downloaded = tmp_path / "downloaded.flac"
    downloaded.write_bytes(b"downloaded media")
    db = FakeDb(rows=[("track-a", str(library_path), "Song Title", "Artist Name")])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)
    monkeypatch.setattr(mod, "download_track_to_temp", lambda item: str(downloaded), raising=False)
    monkeypatch.setattr(mod, "analyze_file", lambda path: (_ for _ in ()).throw(RuntimeError("decode failed")))

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "failed"}
    assert db.cursor_obj.executed[-1][1][-1] == "decode failed"
    assert not downloaded.exists()


def test_analyze_one_track_persists_ready_profile_with_pr721_score_shape(monkeypatch, tmp_path):
    mod = load_plugin()
    audio = tmp_path / "song.wav"
    audio.write_bytes(b"not really decoded in this test")
    db = FakeDb(rows=[("track-a", str(audio))])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    class Result:
        sample_rate = 48000
        duration_ms = 1000
        ref_lufs = -14.0
        start_ramp_blob = b"\xe9\x03\x00"
        end_ramp_blob = b"\xe9\x04\x00"

    seen = {}

    def fake_analyze_file(path):
        seen["path"] = path
        return Result()

    monkeypatch.setattr(mod, "analyze_file", fake_analyze_file)

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "ready"}
    assert seen["path"] == str(audio)
    select_sql = " ".join(db.cursor_obj.executed[0][0].split())
    assert select_sql == "SELECT item_id, file_path, title, author FROM score WHERE item_id = %s"
    assert db.commits == 1
    sql, params = db.cursor_obj.executed[-1]
    assert "INSERT INTO" in sql
    assert params[0] == "track-a"
    assert params[1] == 48000
    assert params[6] == mod.ANALYZER_VERSION
    assert params[7] == mod.SCHEMA_VERSION
    assert params[10] == "ready"


def test_analyze_one_track_persists_failed_profile(monkeypatch, tmp_path):
    mod = load_plugin()
    audio = tmp_path / "song.wav"
    audio.write_bytes(b"x")
    db = FakeDb(rows=[("track-a", str(audio))])
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "analyze_file", lambda path: (_ for _ in ()).throw(RuntimeError("decode failed")))

    result = mod.analyze_one_track("track-a")

    assert result == {"track_id": "track-a", "status": "failed"}
    assert db.cursor_obj.executed[-1][1][-1] == "decode failed"


def test_find_backfill_ids_includes_missing_old_and_signature_changed_but_not_failed(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    unchanged = tmp_path / "unchanged.wav"
    unchanged.write_bytes(b"same media")
    unchanged_sig = mod.media_signature(str(unchanged))
    rows = [
        ("missing-profile", str(current), None, None, None),
        ("old-analyzer", str(current), "old-sig", 0, "ready"),
        ("changed-media", str(current), "old-sig", mod.ANALYZER_VERSION, "ready"),
        ("failed-once", str(current), "old-sig", mod.ANALYZER_VERSION, "failed"),
        ("unchanged-ready", str(unchanged), unchanged_sig, mod.ANALYZER_VERSION, "ready"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    assert mod.find_backfill_ids(limit=25) == [
        "missing-profile",
        "old-analyzer",
        "changed-media",
    ]


def test_find_backfill_ids_includes_explicit_stale_rows(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    missing = tmp_path / "not-mounted.wav"
    rows = [
        ("stale-track", str(current), "same-sig", mod.ANALYZER_VERSION, "stale"),
        ("failed-once", str(current), "same-sig", mod.ANALYZER_VERSION, "failed"),
        ("skipped-once", str(missing), "same-sig", mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.find_backfill_ids(limit=25) == ["stale-track"]


def test_find_backfill_ids_retries_skipped_no_file_when_downloader_configured(monkeypatch, tmp_path):
    mod = load_plugin()
    missing = tmp_path / "not-mounted.wav"
    rows = [
        ("skipped-once", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)

    assert mod.find_backfill_ids(limit=25) == ["skipped-once"]


def test_find_backfill_ids_retries_skipped_no_file_when_local_file_appears(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    rows = [
        ("skipped-once", str(current), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.find_backfill_ids(limit=25) == ["skipped-once"]


def test_find_backfill_ids_applies_limit_after_eligibility_filtering(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    missing = tmp_path / "not-mounted.wav"
    sig = mod.media_signature(str(current))
    rows = [
        ("ready-current-1", str(current), sig, mod.ANALYZER_VERSION, "ready"),
        ("failed-once", str(current), "old-sig", mod.ANALYZER_VERSION, "failed"),
        ("skipped-once", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
        ("eligible-missing", str(current), None, None, None),
        ("eligible-stale", str(current), sig, mod.ANALYZER_VERSION, "stale"),
        ("eligible-old", str(current), sig, 0, "ready"),
    ]
    db = LimitAwareDb(rows=rows)
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.find_backfill_ids(limit=2) == ["eligible-missing", "eligible-stale"]


def test_backfill_uses_configured_batch_size(monkeypatch):
    mod = load_plugin()
    seen_limits = []
    monkeypatch.setattr(
        mod,
        "get_setting",
        lambda key, default=None: 7 if key == "backfill_batch_size" else default,
    )
    monkeypatch.setattr(mod, "find_backfill_ids", lambda limit: seen_limits.append(limit) or [])

    assert mod.backfill_missing_profiles() == {"ready": 0, "failed": 0, "skipped": 0}
    assert seen_limits == [7]


def test_analysis_status_counts_current_pending_failed_and_needed(monkeypatch, tmp_path):
    mod = load_plugin()
    current = tmp_path / "current.wav"
    current.write_bytes(b"new media")
    missing = tmp_path / "not-mounted.wav"
    unchanged = tmp_path / "unchanged.wav"
    unchanged.write_bytes(b"same media")
    unchanged_sig = mod.media_signature(str(unchanged))
    rows = [
        ("ready-current", str(unchanged), unchanged_sig, mod.ANALYZER_VERSION, "ready"),
        ("missing-profile", str(current), None, None, None),
        ("old-analyzer", str(current), "old-sig", 0, "ready"),
        ("changed-media", str(current), "old-sig", mod.ANALYZER_VERSION, "ready"),
        ("pending-track", str(current), None, mod.ANALYZER_VERSION, "pending"),
        ("failed-track", str(current), None, mod.ANALYZER_VERSION, "failed"),
        ("skipped-track", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: False, raising=False)

    assert mod.analysis_status_counts() == {
        "total_with_files": 7,
        "ready_current": 1,
        "pending": 1,
        "failed": 1,
        "skipped": 1,
        "needs_analysis": 3,
    }


def test_analysis_status_counts_treats_retryable_skipped_rows_as_needed(monkeypatch, tmp_path):
    mod = load_plugin()
    missing = tmp_path / "not-mounted.wav"
    rows = [
        ("skipped-track", str(missing), None, mod.ANALYZER_VERSION, "skipped_no_file"),
    ]
    monkeypatch.setattr(mod, "get_db", lambda: FakeDb(rows=rows))
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)
    monkeypatch.setattr(mod, "media_server_download_available", lambda: True, raising=False)

    assert mod.analysis_status_counts() == {
        "total_with_files": 1,
        "ready_current": 0,
        "pending": 0,
        "failed": 0,
        "skipped": 0,
        "needs_analysis": 1,
    }


def test_queue_backfill_batch_marks_pending_and_enqueues_next_batch(monkeypatch):
    mod = load_plugin()
    calls = []

    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)
    monkeypatch.setattr(mod, "find_backfill_ids", lambda limit: calls.append(("find", limit)) or ["a", "b"])
    monkeypatch.setattr(mod, "mark_pending", lambda ids: calls.append(("mark_pending", ids)))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, ids, queue="default": calls.append((func.__name__, ids, queue)),
    )

    assert mod.queue_backfill_batch() == {"queued": 2, "limit": 3}
    assert calls == [
        ("find", 3),
        ("mark_pending", ["a", "b"]),
        ("analyze_tracks_task", ["a", "b"], "default"),
    ]


def test_queue_whole_library_splits_every_candidate_into_250_track_jobs(monkeypatch):
    mod = load_plugin()
    calls = []
    ids = [f"track-{i}" for i in range(601)]

    monkeypatch.setattr(mod, "find_all_backfill_ids", lambda: ids)
    monkeypatch.setattr(mod, "mark_pending", lambda chunk: calls.append(("mark_pending", chunk)))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, chunk, queue="default": calls.append((func.__name__, chunk, queue)),
    )

    assert mod.queue_whole_library() == {"queued": 601, "jobs": 3, "chunk_size": 250}
    assert calls == [
        ("mark_pending", ids[:250]),
        ("analyze_tracks_task", ids[:250], "default"),
        ("mark_pending", ids[250:500]),
        ("analyze_tracks_task", ids[250:500], "default"),
        ("mark_pending", ids[500:]),
        ("analyze_tracks_task", ids[500:], "default"),
    ]


def test_queue_whole_library_does_not_enqueue_when_no_candidates(monkeypatch):
    mod = load_plugin()
    calls = []

    monkeypatch.setattr(mod, "find_all_backfill_ids", lambda: [])
    monkeypatch.setattr(mod, "mark_pending", lambda chunk: calls.append(("mark_pending", chunk)))
    monkeypatch.setattr(
        mod,
        "enqueue",
        lambda func, chunk, queue="default": calls.append((func.__name__, chunk, queue)),
    )

    assert mod.queue_whole_library() == {"queued": 0, "jobs": 0, "chunk_size": 250}
    assert calls == []


def test_migrate_disables_legacy_backfill_schedule(monkeypatch):
    mod = load_plugin()
    db = CronDb(existing=None)
    monkeypatch.setattr(mod, "profiles_table", lambda: PLUGIN_TABLE)

    mod.migrate(db)

    assert db.commits == 1
    assert db.cursor_obj.executed[-1] == (
        "UPDATE cron SET enabled=FALSE WHERE task_type=%s",
        (mod.BACKFILL_TASK_TYPE,),
    )
    assert all("INSERT INTO cron" not in sql for sql, _params in db.cursor_obj.executed)


def test_register_uses_analysis_hook_without_default_cron(monkeypatch):
    mod = load_plugin()
    ctx = FakeCtx()

    mod.register(ctx)

    assert ctx.blueprints == [mod.bp]
    assert ctx.settings_endpoint == "lumae_analysis.settings"
    assert ctx.install_hooks == [mod.migrate]
    assert ctx.song_hooks == [mod.analyze_song_hook]
    assert ctx.cron_tasks == []


def test_settings_page_exposes_manual_catch_up_and_status(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 250)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda: {
            "total_with_files": 16000,
            "ready_current": 100,
            "pending": 2,
            "failed": 1,
            "skipped": 3,
            "needs_analysis": 15894,
        },
    )
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.get("/settings")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'class="lumae-status-grid"' in body
    assert 'class="lumae-meter-fill" style="width: 1%;"' in body
    assert "Analyze next batch" in body
    assert "Queue all missing tracks" in body
    assert "Tracks per batch" in body
    assert "Needs analysis" in body
    assert "15,894" in body
    assert "Enable scheduled catch-up" not in body
    assert "Cron expression" not in body
    assert "Scheduled Tasks" not in body


def test_settings_page_renders_coverage_meter_and_action_context(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 50)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda: {
            "total_with_files": 100,
            "ready_current": 82,
            "pending": 4,
            "failed": 2,
            "skipped": 1,
            "needs_analysis": 11,
        },
    )
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.get("/settings")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "82% profile coverage" in body
    assert 'aria-valuenow="82"' in body
    assert "11 tracks can be queued now." in body
    assert "Runs one controlled batch using the current batch size." in body
    assert "Queues all missing, stale, or changed tracks in 250-track jobs." in body


def test_settings_page_queue_whole_library_posts_action_and_reports_job_count(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 250)
    monkeypatch.setattr(
        mod,
        "analysis_status_counts",
        lambda: {
            "total_with_files": 16000,
            "ready_current": 100,
            "pending": 2,
            "failed": 1,
            "skipped": 3,
            "needs_analysis": 15894,
        },
    )
    monkeypatch.setattr(
        mod,
        "queue_whole_library",
        lambda: {"queued": 15894, "jobs": 64, "chunk_size": 250},
    )
    monkeypatch.setattr(mod, "set_setting", lambda key, value: None)
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)

    response = client.post(
        "/settings",
        data={"backfill_batch_size": "25", "action": "queue_all"},
    )

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'class="lumae-notice lumae-notice-success" role="status"' in body
    assert "Queued 15,894 tracks across 64 jobs for Lumae analysis." in body
