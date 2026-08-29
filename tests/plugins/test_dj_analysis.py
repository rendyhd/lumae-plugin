import hashlib
import importlib.util
from pathlib import Path
import sys
import types

import numpy as np
import pytest


ROOT = Path(__file__).resolve().parents[3] / "lumae-plugin"
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
package_name = "_lumae_dj_contract_test"
package = types.ModuleType(package_name)
package.__path__ = [str(SOURCE)]
previous_plugin = sys.modules.get("plugin")
previous_plugin_api = sys.modules.get("plugin.api")
try:
    sys.modules["plugin"] = plugin
    sys.modules["plugin.api"] = plugin_api
    sys.modules[package_name] = package
    edge = _load(f"{package_name}.edge_profiles", "edge_profiles.py")
    dj = _load(f"{package_name}.dj_analysis", "dj_analysis.py")
    store = _load(f"{package_name}.dj_analysis_store", "dj_analysis_store.py")
finally:
    if previous_plugin is None:
        sys.modules.pop("plugin", None)
    else:
        sys.modules["plugin"] = previous_plugin
    if previous_plugin_api is None:
        sys.modules.pop("plugin.api", None)
    else:
        sys.modules["plugin.api"] = previous_plugin_api


def model_output(duration_seconds=192, beat_step=25, downbeat_offset=1):
    frames = duration_seconds * dj.MODEL_FPS
    beat = np.full(frames, -8.0)
    downbeat = np.full(frames, -8.0)
    beats = list(range(0, frames, beat_step))
    beat[beats] = 4.0
    for frame in beats[::4]:
        raw = frame + downbeat_offset
        if raw < frames:
            downbeat[raw] = 4.0
    energy = np.zeros(frames)
    flux = np.zeros(frames)
    # The fifth 8-bar boundary is an isolated structural change. With at
    # least nine neighbors its local z-score exceeds the registered 2.5 gate.
    structural = beats[4 * 8 * 5]
    flux[structural] = 20.0
    return {
        "beat_logits": beat,
        "downbeat_logits": downbeat,
        "energy": energy,
        "spectral_flux": flux,
        "source_sample_rate": 48_000,
        "source_decoded_frames": duration_seconds * 48_000,
    }


def build(output=None, **kwargs):
    output = output or model_output()
    return dj.build_dj_analysis(
        output,
        catalog_instance_id="catalog-a",
        track_id="track-a",
        media_revision="sha256:" + "a" * 64,
        representation_id="sha256:" + "b" * 64,
        content_sha256="b" * 64,
        duration_seconds=len(output["beat_logits"]) / dj.MODEL_FPS,
        source={
            "sample_rate": 48_000,
            "decoded_frames": 9_216_000,
            "analysis_sample_rate": 22_050,
            "analysis_frames": len(output["beat_logits"]),
            "decoder": "test",
            "timeline_verified": True,
        },
        **kwargs,
    )


def test_model_artifact_is_local_size_and_checksum_pinned(tmp_path):
    path = tmp_path / "model.ckpt"
    path.write_bytes(b"pinned model")
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    result = dj.verify_model_artifact(
        path,
        expected_bytes=path.stat().st_size,
        expected_sha256=expected,
    )
    assert result["sha256"] == expected
    with pytest.raises(dj.DjAnalysisError, match="model_not_local"):
        dj.verify_model_artifact("https://example.invalid/model.ckpt")
    with pytest.raises(dj.DjAnalysisError, match="model_size_mismatch"):
        dj.verify_model_artifact(path, expected_bytes=1, expected_sha256=expected)


def test_runtime_requires_every_exact_pin_and_never_resolves_a_remote_model():
    versions = dict(dj.PINNED_PACKAGES)

    def package_version(name):
        return versions[name]

    ready = dj.runtime_status("unused", package_version=package_version, verify_model=False)
    assert ready["available"] is True
    assert ready["reference_host_qualified"] is False
    versions["torch"] = "2.6.0+cpu"
    versions["torchaudio"] = "2.6.0+cpu"
    cpu_ready = dj.runtime_status(
        "unused", package_version=package_version, verify_model=False
    )
    assert cpu_ready["available"] is True
    versions["torchaudio"] = "2.6.0"
    versions["torch"] = "2.6.1"
    rejected = dj.runtime_status("unused", package_version=package_version, verify_model=False)
    assert rejected["available"] is False
    assert rejected["runtime_mismatches"] == [
        {
            "package": "torch",
            "reason": "version",
            "expected": "2.6.0",
            "actual": "2.6.1",
        }
    ]


def test_window_geometry_matches_upstream_split_and_keep_first_ownership():
    # This length makes upstream append a near-duplicate final window before
    # shifting it to the end. Keeping the earlier owner is intentional.
    starts = dj._beat_this_window_starts(2_977)
    assert starts == [-dj.MODEL_BORDER_FRAMES, 1_482, 1_483]
    owners = np.full(2_977, -1, dtype=np.int64)
    for owner, start in enumerate(starts):
        begin = max(0, start + dj.MODEL_BORDER_FRAMES)
        end = min(2_977, start + dj.MODEL_WINDOW_FRAMES - dj.MODEL_BORDER_FRAMES)
        unowned = owners[begin:end] < 0
        owners[begin:end][unowned] = owner
    assert np.all(owners >= 0)
    assert owners[1_488] == 1
    assert owners[-1] == 2


def test_centered_stft_context_uses_global_reflect_padding():
    indices = dj._reflect_sample_indices(-3, 604, 600)
    assert indices[:7].tolist() == [3, 2, 1, 0, 1, 2, 3]
    assert indices[602:607].tolist() == [599, 598, 597, 596, 595]


def test_stable_four_four_regions_keep_raw_alignment_and_structural_candidates():
    result = build()
    eligible = [region for region in result["regions"] if region["eligible"]]
    assert eligible
    assert min(region["beat_count"] for region in eligible) >= 32
    assert max(region["interval_cv"] for region in eligible) <= 0.06
    assert max(region["max_raw_downbeat_alignment_ms"] for region in eligible) == 20
    assert result["candidates"]["entries"]
    assert len(result["candidates"]["entries"]) <= 8
    assert result["natural_boundaries"] == {"entry_ms": 0, "exit_ms": 192_000}
    assert result["model"]["output_semantics"] == "uncalibrated_logits"
    assert result["model"]["downbeat_snapping_authorizes_alignment"] is False
    assert result["analysis_digest"] == dj.analysis_digest(result)


def test_unstable_or_misaligned_regions_are_not_track_level_ready():
    unstable = model_output()
    original = np.flatnonzero(unstable["beat_logits"] > 0)
    intervals = [20, 30] * (len(original) // 2 + 1)
    moved = np.cumsum([0] + intervals[: len(original) - 1])
    unstable["beat_logits"][:] = -8
    unstable["downbeat_logits"][:] = -8
    moved = moved[moved < len(unstable["beat_logits"])]
    unstable["beat_logits"][moved] = 4
    unstable["downbeat_logits"][moved[::4] + 1] = 4
    result = build(unstable)
    assert result["quality"]["eligible_region_count"] == 0
    assert any("unstable_tempo" in region["rejection_reasons"] for region in result["regions"])

    misaligned = build(model_output(downbeat_offset=4))
    assert misaligned["quality"]["eligible_region_count"] == 0
    assert any("downbeat_alignment" in region["rejection_reasons"] for region in misaligned["regions"])


def test_sparse_model_output_remains_strict_json_instead_of_publishing_infinity():
    output = model_output()
    output["beat_logits"][:] = -8
    output["downbeat_logits"][:] = -8
    output["beat_logits"][100] = 4
    output["downbeat_logits"][101] = 4
    result = build(output)
    assert result["regions"][0]["interval_cv"] is None
    assert "Infinity" not in dj.canonical_json(result)
    assert result["analysis_digest"] == dj.analysis_digest(result)


def test_half_or_double_tempo_evidence_rejects_instead_of_guessing():
    output = model_output()
    beats = np.flatnonzero(output["beat_logits"] > 0)
    for left, right in zip(beats, beats[1:]):
        output["beat_logits"][int(round((left + right) / 2))] = 4
    result = build(output)
    assert result["quality"]["eligible_region_count"] == 0
    assert any(
        "double_tempo_ambiguity" in region["rejection_reasons"]
        for region in result["regions"]
    )


def test_pitch_requires_qualified_unambiguous_key_evidence():
    unqualified = build(
        key_evidence={
            "method": "audiomuse-metadata",
            "key": 5,
            "scale": "minor",
            "confidence": 1,
            "ambiguous": False,
        }
    )
    assert unqualified["key_evidence"]["pitch_shift_allowed"] is False
    qualified = build(
        key_evidence={
            "method": "lumae-chroma-key-v1",
            "key": 5,
            "scale": "minor",
            "confidence": 0.9,
            "ambiguous": False,
        }
    )
    assert qualified["key_evidence"]["pitch_shift_allowed"] is True


def test_timeline_and_identity_errors_fail_closed():
    output = model_output()
    output["downbeat_logits"] = output["downbeat_logits"][:-1]
    with pytest.raises(dj.DjAnalysisError, match="invalid downbeat_logits shape"):
        build(output)
    output = model_output()
    with pytest.raises(dj.DjAnalysisError, match="model_timeline_mismatch"):
        dj.build_dj_analysis(
            output,
            catalog_instance_id="catalog-a",
            track_id="track-a",
            media_revision="sha256:" + "a" * 64,
            representation_id="sha256:" + "b" * 64,
            content_sha256="b" * 64,
            duration_seconds=1,
            source={},
        )


class FakeAdapter:
    def __init__(self, output):
        self.output = output

    def analyze(self, _path, *, deadline, cancelled, progress):
        assert deadline > 0
        assert cancelled() is False
        progress(100)
        return self.output


def test_file_adapter_publication_contains_no_raw_path(tmp_path):
    path = tmp_path / "private-song.flac"
    path.write_bytes(b"authorized durable source")
    progress = []
    result = dj.analyze_dj_file(
        path,
        catalog_instance_id="catalog-a",
        track_id="track-a",
        media_revision="sha256:" + "a" * 64,
        model_path="unused",
        adapter=FakeAdapter(model_output()),
        cancelled=lambda: False,
        progress=progress.append,
    )
    assert progress == [100]
    assert str(path) not in dj.canonical_json(result)
    assert result["representation_id"].startswith("sha256:")


class Cursor:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def close(self):
        pass


class Database:
    def __init__(self, rows=None):
        self.cur = Cursor(rows)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_store_migration_has_one_running_worker_and_durable_progress():
    db = Database()
    store.migrate_dj_analysis(db)
    sql = "\n".join(query for query, _params in db.cur.executed)
    assert "WHERE status='running'" in sql
    assert "progress_frames BIGINT NOT NULL" in sql
    assert "worker_restarted" in sql


@pytest.fixture
def postgres_db():
    import os
    import uuid

    dsn = os.environ.get("LUMAE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("set LUMAE_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
    psycopg2 = pytest.importorskip("psycopg2")
    schema = f"lumae_dj_{uuid.uuid4().hex}"
    db = psycopg2.connect(dsn)
    cur = db.cursor()
    cur.execute(f"CREATE SCHEMA {schema}")
    cur.execute(f"SET search_path TO {schema}, public")
    cur.execute(
        """CREATE TABLE plugin_lumae_analysis__catalog_sources (
        catalog_instance_id TEXT PRIMARY KEY)"""
    )
    cur.execute(
        """CREATE TABLE plugin_lumae_analysis__source_profiles (
        catalog_instance_id TEXT NOT NULL,
        track_id TEXT NOT NULL,
        status TEXT NOT NULL,
        media_signature TEXT,
        PRIMARY KEY (catalog_instance_id, track_id))"""
    )
    db.commit()
    try:
        yield db
    finally:
        db.rollback()
        cur = db.cursor()
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        db.commit()
        cur.close()
        db.close()


def test_postgres_job_claim_publish_cancel_and_read(postgres_db):
    db = postgres_db
    cur = db.cursor()
    cur.execute("INSERT INTO plugin_lumae_analysis__catalog_sources VALUES ('catalog-a')")
    cur.execute(
        """INSERT INTO plugin_lumae_analysis__source_profiles VALUES
        ('catalog-a', 'track-a', 'ready', 'signature-a'),
        ('catalog-a', 'track-b', 'ready', 'signature-b')"""
    )
    db.commit()
    store.migrate_dj_analysis(db)
    db.commit()

    accepted, ready = store.claim_dj_requests(db, "catalog-a", ["track-a", "track-b"])
    assert ready == []
    assert [job["track_id"] for job in accepted] == ["track-a", "track-b"]
    job = store.claim_next_dj_job(db)
    assert job["track_id"] == "track-a"
    assert store.claim_next_dj_job(db) is None
    store.update_dj_progress(db, job, 1234)

    payload = build()
    payload["media_revision"] = edge.opaque_revision("signature-a")
    payload["analysis_digest"] = dj.analysis_digest(payload)
    job["media_revision"] = payload["media_revision"]
    assert store.publish_dj_analysis(db, job, payload, "signature-a") is True
    state = store.read_dj_analysis(db, "catalog-a", ["track-a", "track-b", "absent"])
    assert [item["track_id"] for item in state["ready"]] == ["track-a"]
    assert state["pending"] == [
        {"track_id": "track-b", "status": "pending", "progress_frames": 0}
    ]
    assert state["missing"] == ["absent"]
    assert store.cancel_dj_jobs(db, "catalog-a", ["track-b"]) == ["track-b"]
