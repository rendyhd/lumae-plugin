import importlib.util
import sys
import types
from pathlib import Path

import numpy as np
import pytest


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "plugins" / "LumaeAnalysis"


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SOURCE / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


plugin = types.ModuleType("plugin")
plugin_api = types.ModuleType("plugin.api")
plugin_api.table = lambda name: f"plugin_lumae_analysis__{name}"
package_name = "_lumae_dj_v3_contract_test"
package = types.ModuleType(package_name)
package.__path__ = [str(SOURCE)]
previous_plugin = sys.modules.get("plugin")
previous_plugin_api = sys.modules.get("plugin.api")
try:
    sys.modules["plugin"] = plugin
    sys.modules["plugin.api"] = plugin_api
    sys.modules[package_name] = package
    vocal = _load(f"{package_name}.vocal_calibration", "vocal_calibration.py")
    edge = _load(f"{package_name}.edge_profiles", "edge_profiles.py")
    v2 = _load(f"{package_name}.dj_analysis", "dj_analysis.py")
    v3 = _load(f"{package_name}.dj_analysis_v3", "dj_analysis_v3.py")
    store = _load(f"{package_name}.dj_analysis_v3_store", "dj_analysis_v3_store.py")
finally:
    if previous_plugin is None:
        sys.modules.pop("plugin", None)
    else:
        sys.modules["plugin"] = previous_plugin
    if previous_plugin_api is None:
        sys.modules.pop("plugin.api", None)
    else:
        sys.modules["plugin.api"] = previous_plugin_api


def model_output(duration_seconds=192, beat_step=25):
    frames = duration_seconds * v2.MODEL_FPS
    beat = np.full(frames, -8.0)
    downbeat = np.full(frames, -8.0)
    beats = list(range(0, frames, beat_step))
    beat[beats] = 4.0
    for frame in beats[::4]:
        if frame + 1 < frames:
            downbeat[frame + 1] = 4.0
    energy = np.linspace(0.0, 1.0, frames)
    flux = np.zeros(frames)
    for frame in beats[16::16]:
        flux[frame] = 1.0
    return {
        "beat_logits": beat,
        "downbeat_logits": downbeat,
        "energy": energy,
        "spectral_flux": flux,
        "low_energy": energy * 0.8,
        "mid_energy": energy,
        "high_energy": energy * 1.2,
        "source_sample_rate": 48_000,
        "source_decoded_frames": duration_seconds * 48_000,
    }


def build(output=None):
    output = output or model_output()
    return v3.build_dj_analysis_v3(
        output,
        catalog_instance_id="catalog-a",
        track_id="track-a",
        media_revision="sha256:" + "a" * 64,
        representation_id="sha256:" + "b" * 64,
        content_sha256="b" * 64,
        duration_seconds=len(output["beat_logits"]) / v2.MODEL_FPS,
        source={
            "sample_rate": 48_000,
            "decoded_frames": 9_216_000,
            "analysis_sample_rate": v2.MODEL_SAMPLE_RATE,
            "analysis_resampled_frames": len(output["beat_logits"])
            * v2.MODEL_HOP_SAMPLES,
            "analysis_frames": len(output["beat_logits"]),
            "decoder": "test",
            "timeline_verified": True,
        },
    )


def test_v3_builds_local_regions_and_distinct_role_rankings():
    result = build()

    assert result["schema_version"] == 3
    assert result["method"] == v3.METHOD
    assert result["analysis_digest"] == v2.analysis_digest(result)
    assert result["rhythm"]["regions"]
    assert result["rhythm"]["regions"][0]["confidence_tier"] == "high"
    assert result["rhythm"]["regions"][0]["normalized_tempo_bpm"] == pytest.approx(120)
    assert result["cue_rankings"]["entry"]
    assert result["cue_rankings"]["exit"]
    assert set(result["cue_rankings"]["entry"]).isdisjoint(
        result["cue_rankings"]["exit"]
    )
    assert {4, 8, 16}.issubset(
        {
            bars
            for boundary in result["structural_boundaries"]
            for bars in boundary["bar_lengths"]
        }
    )


def test_v3_local_region_tolerates_one_missing_downbeat():
    output = model_output()
    output["downbeat_logits"][101] = -8.0

    result = build(output)

    assert any(
        region["confidence_tier"] == "high" and region["missing_downbeats"] == 1
        for region in result["rhythm"]["regions"]
    )


def test_v3_publishes_local_half_time_alternative_without_audio_stretch():
    output = model_output()
    output["downbeat_logits"][:] = -8.0
    beats = np.flatnonzero(output["beat_logits"] > 0)
    for frame in beats[::8]:
        if frame + 1 < len(output["downbeat_logits"]):
            output["downbeat_logits"][frame + 1] = 4.0

    result = build(output)
    interpreted = [
        region
        for region in result["rhythm"]["regions"]
        if region["selected_metrical_factor"] == 0.5
    ]

    assert interpreted
    assert interpreted[0]["raw_tempo_bpm"] == pytest.approx(120)
    assert interpreted[0]["normalized_tempo_bpm"] == pytest.approx(60)


def test_speech_safe_cut_requires_160ms_minimum_and_80ms_guards():
    output = model_output(duration_seconds=20)
    vocal_risk = {
        "calibration": {"cuts_authorized": True},
        "frames": [{"position_ms": 5_000, "calibrated_risk": 0.9, "dominant_class_index": 0}],
    }
    output["mid_energy"][:] = 1.0
    output["low_energy"][:] = 0.0
    output["high_energy"][:] = 1.0
    output["mid_energy"][235:266] = 0.0
    output["high_energy"][235:266] = 0.0

    intervals = v3._speech_safe_intervals(output, vocal_risk, 20_000)

    assert intervals
    assert intervals[0]["stable_minimum_ms"] >= 160
    assert intervals[0]["guard_ms"] == 80


def test_runtime_capability_advertises_v2_and_v3(monkeypatch):
    monkeypatch.setattr(
        v3.runtime,
        "runtime_status",
        lambda *_args, **_kwargs: {
            "schema_version": 2,
            "method": v2.METHOD,
            "supported_analysis_versions": [2],
            "supported_plan_versions": [2],
        },
    )

    result = v3.runtime_status("beat", "yamnet")

    assert result["schema_version"] == 3
    assert result["supported_analysis_versions"] == [2, 3]
    assert result["supported_plan_versions"] == [2, 3]


class RecordingCursor:
    def __init__(self, fetchone_values=None, fetchall_values=None):
        self.statements = []
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchone(self):
        return self.fetchone_values.pop(0) if self.fetchone_values else None

    def fetchall(self):
        return self.fetchall_values.pop(0) if self.fetchall_values else []

    def close(self):
        pass


class RecordingDb:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def rollback(self):
        pass

    def commit(self):
        self.commits += 1


def test_v3_migration_is_additive_and_global_claim_checks_both_versions():
    cursor = RecordingCursor()
    db = RecordingDb(cursor)
    store.migrate_dj_analysis_v3(db)
    sql = "\n".join(statement for statement, _ in cursor.statements)

    assert "dj_analyses_v3" in sql
    assert "dj_analysis_jobs_v3" in sql
    assert "CREATE TABLE IF NOT EXISTS plugin_lumae_analysis__dj_analyses (" not in sql

    claim_cursor = RecordingCursor(fetchone_values=[(True,), None, (True,)])
    store.claim_next_dj_job_any(RecordingDb(claim_cursor))
    claim_sql = "\n".join(statement for statement, _ in claim_cursor.statements)
    assert "dj_analysis_jobs_v3" in claim_sql
    assert "dj_analysis_jobs" in claim_sql
    recovery_sql = [
        statement
        for statement, _params in claim_cursor.statements
        if "error_code='worker_restarted'" in statement
    ]
    assert len(recovery_sql) == 2
    assert all("WHERE status='running'" in statement for statement in recovery_sql)
    assert "priority DESC, requested_at" in claim_sql


def test_pending_queue_job_is_promoted_without_replacement():
    revision = store.opaque_revision("media-signature")
    cursor = RecordingCursor(
        fetchone_values=[None, (revision, "pending", 10, store.jobs.producer_key(3), True, 0)],
        fetchall_values=[[('track-a', "media-signature")]],
    )
    db = RecordingDb(cursor)

    accepted, promoted, ready, queued = store.claim_dj_v3_requests(
        db,
        "catalog-a",
        ["track-a"],
        priority_tier="boundary",
    )

    assert accepted == []
    assert promoted == ["track-a"]
    assert ready == []
    assert queued == []
    assert any(params and params[:2] == (100, "boundary") for _, params in cursor.statements)
