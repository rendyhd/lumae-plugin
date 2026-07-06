import importlib
import json
import pathlib
import sys
import math

import numpy as np

from flask import Flask


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

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
    assert manifest["min_core_version"] == "2.5.0"


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

    monkeypatch.setattr(loudness.librosa, "load", fake_load)
    monkeypatch.setattr(loudness, "analyze_buffer", fake_analyze_buffer)

    result = loudness.analyze_file("fixture.wav")

    assert result is sentinel
    assert captured["load"] == ("fixture.wav", None, False)
    assert captured["analyze"] == (audio, 44100)
