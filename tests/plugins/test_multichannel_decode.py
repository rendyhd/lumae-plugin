"""F1: waveform analysis of surround PCM must not crash in native code.

PyAV 16's ``AudioFrame.planes`` walks ``extended_data`` up to a NULL pointer.
A planar frame with 8 channels has none, so the old planar ("fltp") decode of
7.1 input died with SIGSEGV in ``to_ndarray()``. Every case runs the real
analyzer on a synthesized 16-bit WAV in a child process, so a regression fails
the test (the child's signal is in the message) and not the whole run.
"""
import json
import math
import signal
import struct
import subprocess
import sys
import textwrap
from pathlib import Path

import numpy as np
import pytest


PLUGIN_DIR = Path(__file__).resolve().parents[2] / "plugins" / "LumaeAnalysis"
RATE = 48000
PCM_SUBFORMAT = bytes.fromhex("0100000000001000800000aa00389b71")
MASK_5_1 = 0x3F    # FL FR FC LFE BL BR
MASK_7_1 = 0x63F   # FL FR FC LFE BL BR SL SR

# Loads the plugin modules without the package ``__init__`` (which needs the
# host's ``plugin.api``), runs one analyzer and prints a JSON summary.
CHILD = textwrap.dedent(
    """
    import dataclasses, faulthandler, importlib, json, sys, types
    import numpy as np
    faulthandler.enable()
    plugin_dir, kind, wav, pcm = sys.argv[1:5]
    package = types.ModuleType("lumae_analysis_f1")
    package.__path__ = [plugin_dir]
    sys.modules[package.__name__] = package
    if kind == "loudness":
        loudness = importlib.import_module("lumae_analysis_f1.loudness")
        result = loudness.analyze_file(wav)
        audio = np.load(pcm).astype(np.float32) / 32768
        buffered = loudness.analyze_buffer(audio, result.sample_rate)
        print(json.dumps({
            "sample_rate": result.sample_rate,
            "duration_ms": result.duration_ms,
            "ref_lufs": result.ref_lufs,
            "start_ramp": len(result.start_ramp),
            "end_ramp": len(result.end_ramp),
            "matches_buffer": dataclasses.astuple(result) == dataclasses.astuple(buffered),
        }))
    else:
        edge = importlib.import_module("lumae_analysis_f1.edge_profiles")
        try:
            edge.analyze_edge_file(wav, catalog_instance_id="catalog-a", track_id="track-a",
                                   media_revision="sha256:" + "a" * 64)
        except edge.EdgeProfileError as exc:
            print(json.dumps({"error": type(exc).__name__, "message": str(exc)}))
        else:
            print(json.dumps({"error": None}))
    """
)


def _surround_pcm(channels, seconds=2.0):
    """Channel-first int16: a distinct tone and gain per channel, 0.5 s fades."""
    time = np.arange(int(RATE * seconds)) / RATE
    fade = np.minimum(1.0, time / 0.5) * np.minimum(1.0, (seconds - time) / 0.5)
    tones = [(0.1 + 0.05 * c) * fade * np.sin(2 * np.pi * (110 + 70 * c) * time)
             for c in range(channels)]
    return np.round(np.asarray(tones) * 32767).astype("<i2")


def _write_wav(path, pcm, channel_mask=None):
    """16-bit PCM WAV. With a mask it is WAVE_FORMAT_EXTENSIBLE (a named layout
    such as "7.1"); without one the decoder reports "N channels"."""
    channels = pcm.shape[0]
    block_align = 2 * channels
    header = (1 if channel_mask is None else 0xFFFE, channels, RATE, RATE * block_align, block_align, 16)
    if channel_mask is None:
        fmt = struct.pack("<HHIIHH", *header)
    else:
        fmt = struct.pack("<HHIIHHHHI16s", *header, 22, 16, channel_mask, PCM_SUBFORMAT)
    samples = np.ascontiguousarray(pcm.T).tobytes()
    data = b"fmt " + struct.pack("<I", len(fmt)) + fmt + b"data" + struct.pack("<I", len(samples)) + samples
    path.write_bytes(b"RIFF" + struct.pack("<I", len(data) + 4) + b"WAVE" + data)


def _run_child(tmp_path, kind, channels, channel_mask):
    pcm = _surround_pcm(channels)
    wav = tmp_path / "surround.wav"
    _write_wav(wav, pcm, channel_mask)
    np.save(tmp_path / "pcm.npy", pcm)
    child = subprocess.run(
        [sys.executable, "-c", CHILD, str(PLUGIN_DIR), kind, str(wav), str(tmp_path / "pcm.npy")],
        capture_output=True, text=True, timeout=120,
    )
    if child.returncode < 0:
        died = signal.Signals(-child.returncode).name
        pytest.fail(f"{kind} analysis died with {died}:\n{child.stderr[-1500:]}")
    assert child.returncode == 0, child.stderr[-1500:]
    return json.loads(child.stdout.strip().splitlines()[-1])


SURROUND = [
    pytest.param(8, MASK_7_1, id="7.1"),
    pytest.param(8, None, id="8-channels-unmasked"),
    pytest.param(6, MASK_5_1, id="5.1"),
    pytest.param(6, None, id="6-channels-unmasked"),
]


@pytest.mark.parametrize("channels, channel_mask", [
    *SURROUND,
    pytest.param(2, None, id="stereo"),
    pytest.param(1, None, id="mono"),
])
def test_loudness_analyzes_surround_wav_without_crashing(tmp_path, channels, channel_mask):
    summary = _run_child(tmp_path, "loudness", channels, channel_mask)

    assert summary["sample_rate"] == RATE
    assert summary["duration_ms"] == 2000
    assert math.isfinite(summary["ref_lufs"]) and -40 < summary["ref_lufs"] < 0
    assert summary["start_ramp"] > 0 and summary["end_ramp"] > 0
    # The decoded file measures exactly like the same PCM handed over as a
    # buffer, so every channel was de-interleaved in order.
    assert summary["matches_buffer"] is True


@pytest.mark.parametrize("channels, channel_mask", SURROUND)
def test_edge_profile_rejects_surround_wav_cleanly(tmp_path, channels, channel_mask):
    # Edge profiles qualify mono and stereo only and refuse anything wider
    # before decoding: a Python exception, not a native crash.
    summary = _run_child(tmp_path, "edge", channels, channel_mask)

    assert summary == {"error": "EdgeProfileError", "message": "unqualified channel layout"}
