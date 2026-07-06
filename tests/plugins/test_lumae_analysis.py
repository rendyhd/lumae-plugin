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


def test_analyze_buffer_rejects_silent_audio():
    from plugins.LumaeAnalysis.loudness import SilentAudioError, analyze_buffer

    audio = np.zeros(48000, dtype=np.float32)

    try:
        analyze_buffer(audio, 48000)
    except SilentAudioError as exc:
        assert "silent or sub-gate" in str(exc)
    else:
        raise AssertionError("silent audio should fail")
