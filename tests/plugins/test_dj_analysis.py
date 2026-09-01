import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import zipfile

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


def _load_path(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
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
    provisioner = _load(f"{package_name}.provision_dj_model", "provision_dj_model.py")
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


def test_private_prerelease_builder_isolated_from_public_channel(tmp_path):
    builder = _load_path(
        "lumae_private_dj_builder",
        ROOT / "scripts/build_private_dj_prerelease.py",
    )
    public_before = (ROOT / "plugins/LumaeAnalysis/plugin.json").read_bytes()

    result = builder.build_private_prerelease(
        repository_root=ROOT,
        output_root=tmp_path,
        version="1.2.0-djtest.7",
        base_url="https://private.example/lumae/djtest.7",
    )

    destination = Path(result["directory"])
    private_metadata = json.loads((destination / "plugin.json").read_text())
    private_manifest = json.loads((destination / "manifest.json").read_text())
    with zipfile.ZipFile(result["zip"]) as archive:
        assert "plugin.json" not in archive.namelist()
        runtime = archive.read("__init__.py").decode("utf-8")
    assert 'PLUGIN_VERSION = "1.2.0-djtest.7"' in runtime
    assert private_metadata["channel"] == "private-dj-test"
    assert [entry["version"] for entry in private_metadata["versions"]] == [
        "1.2.0-djtest.7"
    ]
    assert private_manifest["channel"] == "private-dj-test"
    assert (ROOT / "plugins/LumaeAnalysis/plugin.json").read_bytes() == public_before


@pytest.mark.parametrize(
    "version",
    ["1.2.0", "1.2.0-djtest.0", "1.2.0-djtest", "1.2.1-djtest.1"],
)
def test_private_prerelease_builder_rejects_non_test_versions(tmp_path, version):
    builder = _load_path(
        "lumae_private_dj_builder_invalid",
        ROOT / "scripts/build_private_dj_prerelease.py",
    )
    with pytest.raises(ValueError, match="1.2.0-djtest.N"):
        builder.build_private_prerelease(
            repository_root=ROOT,
            output_root=tmp_path,
            version=version,
            base_url="https://private.example/lumae",
        )


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
        "low_energy": np.zeros(frames),
        "mid_energy": np.zeros(frames),
        "high_energy": np.zeros(frames),
        "source_sample_rate": 48_000,
        "source_decoded_frames": duration_seconds * 48_000,
    }


def test_provisioner_reuses_an_already_verified_model(monkeypatch, tmp_path):
    target = tmp_path / "beat-this.ckpt"
    target.write_bytes(b"verified")
    monkeypatch.setattr(
        provisioner,
        "verify_model_artifact",
        lambda path: {"path": Path(path), "verified": True},
    )

    def unexpected_download(*_args, **_kwargs):
        pytest.fail("an already verified model must not be downloaded again")

    assert provisioner.provision(target, opener=unexpected_download) == target.resolve()


def test_model_download_resumes_a_verified_partial(monkeypatch, tmp_path):
    payload = b"0123456789"
    partial = tmp_path / "model.bin.partial"
    partial.write_bytes(payload[:4])
    requests = []

    class Response(io.BytesIO):
        status = 206

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    def opener(request, timeout):
        assert timeout == 60
        requests.append(request)
        return Response(payload[4:])

    def verifier(path):
        if not Path(path).is_file() or Path(path).read_bytes() != payload:
            raise dj.DjAnalysisError("model_checksum_mismatch")

    result = provisioner._verified_download(
        "https://example.invalid/model",
        tmp_path / "model.bin",
        expected_bytes=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        verifier=verifier,
        opener=opener,
    )
    assert result.read_bytes() == payload
    assert requests[0].get_header("Range") == "bytes=4-"
    assert not partial.exists()


def test_model_removal_deletes_only_configured_artifacts_and_partials(tmp_path):
    beat = tmp_path / "beat-this.ckpt"
    yamnet = tmp_path / "yamnet.tflite"
    normal_analysis = tmp_path / "normal-analysis.sqlite"
    for path in (beat, yamnet, normal_analysis):
        path.write_bytes(path.name.encode())
    beat_partial = beat.with_name(beat.name + ".partial")
    yamnet_partial = yamnet.with_name(yamnet.name + ".partial")
    beat_partial.write_bytes(b"partial")
    yamnet_partial.write_bytes(b"partial")

    removed = provisioner.remove_stack(beat, yamnet)

    assert sorted(removed) == sorted(
        map(str, (beat.resolve(), beat_partial.resolve(), yamnet.resolve(), yamnet_partial.resolve()))
    )
    assert normal_analysis.read_bytes() == b"normal-analysis.sqlite"


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


def test_yamnet_adapter_requires_the_pinned_float32_litert_contract(monkeypatch, tmp_path):
    model = tmp_path / "yamnet.tflite"
    model.write_bytes(b"pinned-yamnet")
    info = model.stat()
    monkeypatch.setattr(
        dj,
        "verify_yamnet_model_artifact",
        lambda *_args, **_kwargs: {
            "path": model.resolve(),
            "device": info.st_dev,
            "inode": info.st_ino,
            "bytes": info.st_size,
            "mtime_ns": info.st_mtime_ns,
        },
    )
    monkeypatch.setattr(
        dj,
        "_sha256_file",
        lambda *_args, **_kwargs: dj.YAMNET_MODEL_SHA256,
    )

    class FakeInterpreter:
        def __init__(self, *, model_path, num_threads):
            assert model_path == str(model.resolve())
            assert num_threads == 1

        def allocate_tensors(self):
            return None

        def get_input_details(self):
            return [
                {
                    "shape": np.asarray([dj.YAMNET_WINDOW_SAMPLES]),
                    "dtype": np.float32,
                    "index": 0,
                }
            ]

        def get_output_details(self):
            return [
                {
                    "shape": np.asarray([dj.YAMNET_CLASS_COUNT]),
                    "dtype": np.float32,
                    "index": 1,
                }
            ]

    package = types.ModuleType("ai_edge_litert")
    interpreter_module = types.ModuleType("ai_edge_litert.interpreter")
    interpreter_module.Interpreter = FakeInterpreter
    monkeypatch.setitem(sys.modules, "ai_edge_litert", package)
    monkeypatch.setitem(sys.modules, "ai_edge_litert.interpreter", interpreter_module)

    adapter = dj.YamnetLiteAdapter(model, rss_reader=lambda: 1)

    assert adapter.input["dtype"] == np.float32
    assert adapter.output["dtype"] == np.float32


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
    assert result["schema_version"] == 2
    assert result["vocal_risk"]["calibration"]["cuts_authorized"] is False
    assert result["structure"]["meter"] == {
        "beats_per_bar": 4,
        "confidence": "region-guarded",
    }
    band_energy = result["structure"]["band_energy"]
    assert band_energy["unit"] == "mean-log-mel"
    assert band_energy["window"] == "bar"
    assert 0 < len(band_energy["windows"]) < len(model_output()["beat_logits"]) // 10
    assert all(
        set(window) == {
            "start_ms",
            "end_ms",
            "region_index",
            "eligible",
            "low",
            "mid",
            "high",
        }
        and window["end_ms"] > window["start_ms"]
        for window in band_energy["windows"]
    )


def test_yamnet_scores_remain_uncalibrated_evidence_and_never_authorize_cuts():
    indices = list(dj.YAMNET_VOCAL_CLASSES)
    scores = [0.0] * len(indices)
    scores[indices.index(31)] = 0.91
    result = build(
        vocal_output={
            "positions_ms": [480, 960],
            "class_indices": indices,
            "scores": [scores, [0.1] * len(indices)],
        }
    )
    risk = result["vocal_risk"]
    assert risk["frames"][0] == {
        "position_ms": 480,
        "raw_vocal_evidence": 0.91,
        "dominant_class_index": 31,
        "calibrated_risk": None,
    }
    assert risk["calibration"] == {
        "status": "uncalibrated",
        "scores_are_probabilities": False,
        "cuts_authorized": False,
    }
    assert result["quality"]["vocal_calibration_ready"] is False

    with pytest.raises(dj.DjAnalysisError, match="invalid_yamnet_output"):
        build(
            vocal_output={
                "positions_ms": [480],
                "class_indices": indices,
                "scores": [[2.0] * len(indices)],
            }
        )


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
    assert "dj_worker_capability" in sql
    assert "plugin_version TEXT NOT NULL" in sql


def test_worker_capability_store_is_version_bound_and_path_free():
    payload = {
        "schema_version": 2,
        "method": dj.METHOD,
        "worker_available": True,
        "models": {"beat_this": {"verified": True}, "yamnet": {"verified": True}},
    }
    writer = Database()

    published = store.write_dj_worker_capability(writer, "1.2.0-djtest.2", payload)

    sql, params = writer.cur.executed[-1]
    assert "ON CONFLICT (singleton)" in sql
    assert params[0:2] == ("1.2.0-djtest.2", dj.METHOD)
    assert writer.commits == 1
    assert published == payload
    assert "/" not in json.dumps(published)

    reader = Database(rows=[(payload,)])
    assert store.read_dj_worker_capability(reader, "1.2.0-djtest.2") == payload
    read_sql, read_params = reader.cur.executed[-1]
    assert "updated_at >= now()-interval '10 minutes'" in read_sql
    assert read_params == ("1.2.0-djtest.2", dj.METHOD)


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
