"""Performance plan WP2 phase A: one download and one child call per track.

``analysis_bundle.analyze_track`` returns the waveform profile, the edge
payload and the loudness record, each equal to what its own call gives today
(parity), through a real analysis child on both paths. A file the edge
analyzer refuses (7.1) still gets its waveform and its loudness from the
standalone meter in the same call; with edges off only the waveform is made.
"""
import os
import threading

import numpy as np
import pytest

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import analysis_bundle as bundle
from plugins.LumaeAnalysis import analysis_isolation as iso
from plugins.LumaeAnalysis import edge_profile_store
from plugins.LumaeAnalysis import loudness, loudness_v1

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX process isolation")
EDGE = dict(catalog_instance_id="catalog-a", track_id="track-a", media_revision="sha256:" + "a" * 64)


@pytest.fixture(autouse=True, scope="module")
def _stop_worker_at_end():
    yield
    iso.shutdown()


@pytest.fixture(params=["fork", "exec"])
def mode(request, monkeypatch):
    if request.param == "exec":
        monkeypatch.setattr(iso, "FORK_FAST_PATH", False)
    else:
        assert iso._use_fork(), threading.enumerate()
    return request.param


def _write(path, seconds=8.0, rate=44100, channels=2, codec="flac"):
    import av

    frames = int(seconds * rate)
    t = np.arange(frames) / rate
    rng = np.random.default_rng(11)
    mono = 0.3 * np.sin(2 * np.pi * 220 * t) * np.minimum(1.0, t) + 0.03 * rng.standard_normal(frames)
    pcm = (np.stack([mono * (1 - 0.05 * index) for index in range(channels)]) * 20000).astype(np.int16)
    layout = {1: "mono", 2: "stereo", 8: "7.1"}[channels]
    with av.open(str(path), "w") as container:
        stream = container.add_stream(codec, rate=rate)
        stream.layout = layout
        for offset in range(0, frames, 4096):
            chunk = np.ascontiguousarray(pcm[:, offset:offset + 4096].T.reshape(1, -1))
            frame = av.AudioFrame.from_ndarray(chunk, format="s16", layout=layout)
            frame.sample_rate = rate
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode(None):
            container.mux(packet)
    return str(path)


def _run(path, **kwargs):
    return iso.run_isolated(bundle.analyze_track, path, limit_seconds=bundle.limit_for(kwargs.get("edge"), 120),
                            **kwargs)


@pytest.mark.parametrize("rate,channels", [(44100, 2), (96000, 2), (48000, 1)])
def test_the_bundle_equals_todays_separate_calls(tmp_path, mode, rate, channels):
    path = _write(tmp_path / "a.flac", rate=rate, channels=channels)
    answer = _run(path, edge=True, **EDGE)

    waveform = bundle.part_value(answer["waveform"])
    edge = bundle.part_value(answer["edge"])
    assert "loudness" not in answer  # the edge's own decode measured it
    assert waveform == loudness.analyze_file(path, deadline_seconds=120)
    assert edge == loudness_v1.analyze_edge_file(path, deadline_seconds=120, **EDGE)
    assert edge[loudness_v1.LOUDNESS_KEY]["integrated_lufs"] is not None


def test_a_refused_edge_still_gets_its_waveform_and_the_standalone_loudness(tmp_path, mode):
    path = _write(tmp_path / "surround.flac", channels=8)
    answer = _run(path, edge=True, **EDGE)

    assert bundle.part_value(answer["waveform"]) == loudness.analyze_file(path, deadline_seconds=120)
    with pytest.raises(iso.IsolatedAnalysisError) as refused:
        bundle.part_value(answer["edge"])
    # The refusal crosses the process boundary, so the job is marked unsupported.
    assert refused.value.refusal == "unqualified channel layout"
    assert edge_profile_store.is_unsupported_failure(refused.value)
    assert bundle.part_value(answer["loudness"]) == loudness_v1.measure_file(path, deadline_seconds=120)


def test_edges_off_makes_the_waveform_only(tmp_path, mode):
    path = _write(tmp_path / "a.flac")
    answer = _run(path, edge=False)
    assert set(answer) == {"waveform"}
    assert bundle.part_value(answer["waveform"]) == loudness.analyze_file(path, deadline_seconds=120)


def test_a_part_that_fails_does_not_lose_the_others(tmp_path, mode):
    path = tmp_path / "noise.flac"
    path.write_bytes(b"not audio at all" * 100)
    answer = _run(str(path), edge=True, **EDGE)
    for part in ("waveform", "edge"):
        with pytest.raises(iso.IsolatedAnalysisError) as failed:
            bundle.part_value(answer[part])
        assert failed.value.category and failed.value.refusal is None
        assert str(tmp_path) not in str(failed.value) and str(tmp_path) not in str(failed.value.diagnostics)


def test_each_analysis_gets_the_per_file_limit():
    assert bundle.limit_for(True, 900) == 1800 and bundle.limit_for(False, 900) == 900


def test_the_refusal_allowlist_is_the_edge_stores():
    assert tuple(iso.REFUSALS) == tuple(edge_profile_store.UNSUPPORTED_REASONS)


def test_a_refused_edge_through_the_plain_edge_call_is_unsupported_too(tmp_path, mode):
    """Before WP2 the child sent only the category and the class name, so a
    refusal through the real child was retried as a failure, every 6 hours."""
    path = _write(tmp_path / "surround.flac", channels=8)
    with pytest.raises(iso.IsolatedAnalysisError) as refused:
        iso.run_isolated(loudness_v1.analyze_edge_file, path, limit_seconds=120, **EDGE)
    assert edge_profile_store.is_unsupported_failure(refused.value)
    assert iso.edge_failure_reason(refused.value).startswith("edge-analysis-unavailable")
