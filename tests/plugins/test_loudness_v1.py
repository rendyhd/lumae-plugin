"""LN.4a: whole-track loudness (loudness_v1, contract §3.9) against EBU Tech 3341/3342, analytic
true peaks and channel weights, the edge pass's 48 kHz series against the native path, and an edge
payload that stays byte-identical with the measurement in the same decode."""
import importlib
import math
import struct
import sys
import types
from pathlib import Path

import numpy as np
import pytest

PLUGIN_DIR = Path(__file__).resolve().parents[2] / "plugins" / "LumaeAnalysis"
# The plugin modules without the package ``__init__`` (which needs the host's ``plugin.api``).
_package = types.ModuleType("lumae_analysis_ln4")
_package.__path__ = [str(PLUGIN_DIR)]
sys.modules.setdefault(_package.__name__, _package)
lv1 = importlib.import_module("lumae_analysis_ln4.loudness_v1")
edge = importlib.import_module("lumae_analysis_ln4.edge_profiles")

PCM_SUBFORMAT = bytes.fromhex("0100000000001000800000aa00389b71")


def sine_levels(levels, rate, channels=2, hz=997.0, chunk_seconds=1.0):
    """Channel-first blocks of a 997 Hz sine at each (dBFS peak, seconds) step."""
    plan, position = [], 0
    for db, seconds in levels:
        frames = int(round(seconds * rate))
        plan.append((position, position + frames, 10 ** (db / 20)))
        position += frames
    chunk = int(chunk_seconds * rate)
    for start in range(0, position, chunk):
        index = np.arange(start, min(position, start + chunk))
        amplitude = np.zeros(index.size)
        for begin, end, value in plan:
            amplitude[(index >= begin) & (index < end)] = value
        tone = amplitude * np.sin(2 * np.pi * hz * index / rate)
        yield np.repeat(tone[None, :], channels, axis=0)


def native(blocks, rate, weights=(1.0, 1.0)):
    meter = lv1.NativeMeter(rate, weights)
    for block in blocks:
        meter.add(block)
    return meter.result()


@pytest.mark.parametrize("rate", [44100, 48000, 96000, 192000])
@pytest.mark.parametrize("case,levels", [
    (1, [(-23, 20)]),
    (2, [(-33, 20)]),
    (3, [(-36, 10), (-23, 60), (-36, 10)]),
    (4, [(-72, 10), (-36, 10), (-23, 60), (-36, 10), (-72, 10)]),
    (5, [(-26, 20), (-20, 20.1), (-26, 20)]),
])
def test_tech_3341_integrated_loudness(rate, case, levels):
    expected = -33.0 if case == 2 else -23.0
    record = native(sine_levels(levels, rate), rate)
    assert abs(record["integrated_lufs"] - expected) <= 0.1, record["integrated_lufs"]
    assert record["method"] == "bs1770-4-native" and record["version"] == 1
    assert (record["channels"], record["sample_rate"]) == (2, rate)


@pytest.mark.parametrize("rate", [44100, 48000, 96000])
@pytest.mark.parametrize("levels,expected", [
    ([(-20, 20), (-30, 20)], 10),
    ([(-20, 20), (-15, 20)], 5),
    ([(-40, 20), (-20, 20)], 20),
    ([(-50, 20), (-35, 20), (-20, 20), (-35, 20), (-50, 20)], 15),
])
def test_tech_3342_loudness_range(rate, levels, expected):
    assert abs(native(sine_levels(levels, rate), rate)["lra_lu"] - expected) <= 0.5


def faded_tone(rate, hz, phase, db=-1.0, seconds=5.0):
    t = np.arange(int(seconds * rate)) / rate
    ramp = 0.05
    envelope = np.minimum(1.0, np.minimum(t, seconds - t) / ramp)
    envelope = 0.5 - 0.5 * np.cos(np.pi * np.clip(envelope, 0, 1))
    tone = 10 ** (db / 20) * envelope * np.sin(2 * np.pi * hz * t + phase)
    return np.stack([tone, tone])


@pytest.mark.parametrize("rate", [44100, 48000, 96000, 192000])
def test_true_peak_of_faded_tones(rate):
    # fs/4 at a 45 degree phase: every sample reads 3 dB under the peak.
    record = native([faded_tone(rate, rate / 4, math.pi / 4)], rate)
    assert abs(record["true_peak_dbtp"] - -1.0) <= 0.3
    assert record["sample_peak_dbfs"] < -3.9
    record = native([faded_tone(rate, 19000.0, math.pi / 4)], rate)
    assert abs(record["true_peak_dbtp"] - -1.0) <= 0.3


def test_channel_weights_follow_bs1770_and_ffmpeg():
    assert lv1.channel_weights(["FC"]).tolist() == [2.0]
    assert lv1.channel_weights(["FL", "FR"]).tolist() == [1.0, 1.0]
    assert lv1.channel_weights(["", ""]).tolist() == [1.0, 1.0]
    assert lv1.channel_weights(["FL", "FR", "FC", "LFE", "BL", "BR", "SL", "SR"]).tolist() == [1, 1, 1, 0, 1.41, 1.41, 1.41, 1.41]
    assert lv1.channel_weights(["FL", "FR", "FC", "LFE", "SL", "SR"]).tolist() == [1, 1, 1, 0, 1.41, 1.41]


def wav_bytes(pcm, rate, mask):
    channels = pcm.shape[0]
    samples = np.round(np.clip(pcm.T, -1, 32767 / 32768) * 32768).astype("<i2")
    fmt = struct.pack("<HHIIHHHHI16s", 0xfffe, channels, rate, rate * channels * 2, channels * 2, 16, 22, 16, mask, PCM_SUBFORMAT)
    data = b"fmt " + struct.pack("<I", len(fmt)) + fmt + b"data" + struct.pack("<I", samples.nbytes) + samples.tobytes()
    return b"RIFF" + struct.pack("<I", len(data) + 4) + b"WAVE" + data


def sines(levels_db, rate, seconds=10.0):
    t = np.arange(int(seconds * rate)) / rate
    return np.stack([10 ** (db / 20) * np.sin(2 * np.pi * 997 * t) for db in levels_db])


def analytic(levels_db, weights):
    return 10 * math.log10(sum(w * 10 ** (db / 10) / 2 for db, w in zip(levels_db, weights)))


@pytest.mark.parametrize("mask,levels,weights", [
    (0x63F, [-28, -28, -28, -20, -30, -30, -30, -30], [1, 1, 1, 0, 1.41, 1.41, 1.41, 1.41]),  # 7.1
    (0x60F, [-28, -28, -28, -20, -28, -28], [1, 1, 1, 0, 1.41, 1.41]),  # 5.1 (side)
    (0x4, [-23], [2]),  # mono, as dual-mono
    (0x3, [-23, -23], [1, 1]),
])
def test_measure_file_weights_each_layout(tmp_path, mask, levels, weights):
    pytest.importorskip("av")
    path = tmp_path / "layout.wav"
    path.write_bytes(wav_bytes(sines(levels, 48000), 48000, mask))
    record = lv1.measure_file(str(path))
    assert record["channels"] == len(levels)
    assert abs(record["integrated_lufs"] - analytic(levels, weights)) <= 0.05


def dsf_bytes(tone, dsd_rate):
    """A stereo DSF (first-order delta-sigma, LSB first) carrying ``tone`` on both channels."""
    bits = np.diff(np.floor(np.cumsum((tone + 1) / 2)), prepend=0).astype(np.uint8)
    count = bits.size - bits.size % (8 * 4096)
    packed = np.packbits(bits[:count], bitorder="little").reshape(-1, 4096)
    data = np.stack([packed, packed], axis=1).tobytes()  # 4096-byte blocks, channel after channel
    fmt = b"fmt " + struct.pack("<QIIIIIIQII", 52, 1, 0, 2, 2, dsd_rate, 1, count, 4096, 0)
    body = fmt + b"data" + struct.pack("<Q", 12 + len(data)) + data
    return b"DSD " + struct.pack("<QQQ", 28, 28 + len(body), 0) + body


def test_sources_above_48k_are_measured_through_48k(tmp_path):
    """Ultrasonic content never counts on the standalone path: DSD shapes its noise above 20 kHz
    (Auralscape LN.1: two DSD256 files read 4.7-5.7 LU loud at their decoded rate), and DSD-derived
    PCM keeps it."""
    pytest.importorskip("av")
    assert [lv1.measured_rate(rate) for rate in (32000, 44100, 48000, 88200, 352800)] == [
        32000, 44100, 48000, 48000, 48000]
    # A DSD64 DSF decodes at 352.8 kHz with its modulator's noise above the audio band.
    dsd_rate = 2_822_400
    tone = 0.5 * np.sin(2 * np.pi * 997 * np.arange(2 * dsd_rate) / dsd_rate)
    path = tmp_path / "tone.dsf"
    path.write_bytes(dsf_bytes(tone, dsd_rate))
    record = lv1.measure_file(str(path))
    assert (record["method"], record["sample_rate"], record["channels"]) == ("bs1770-4-48k", 352800, 2)
    assert abs(record["integrated_lufs"] - analytic([-6.0206, -6.0206], [1, 1])) <= 0.05
    # 96 kHz PCM with a loud 30 kHz tone (DSD-derived noise below the 48 kHz Nyquist) reads as its
    # audible part. The tone fades in and out: an abrupt start would put a real transient in band.
    rate = 96000
    t = np.arange(4 * rate) / rate
    audible = 10 ** (-23 / 20) * np.sin(2 * np.pi * 997 * t)
    fade = np.minimum(1.0, np.minimum(t, t[-1] - t) / 0.1)
    pcm = np.stack([audible + fade * 10 ** (-12 / 20) * np.sin(2 * np.pi * 30000 * t)] * 2)
    path = tmp_path / "shaped.wav"
    path.write_bytes(wav_bytes(pcm, rate, 0x3))
    record = lv1.measure_file(str(path))
    assert (record["method"], record["sample_rate"]) == ("bs1770-4-48k", rate)
    assert abs(record["integrated_lufs"] - analytic([-23, -23], [1, 1])) <= 0.05
    assert abs(record["true_peak_dbtp"] - -23) <= 0.1
    assert native([pcm], rate)["integrated_lufs"] > record["integrated_lufs"] + 3  # what it would read
    # 48 kHz and below stay native.
    path = tmp_path / "plain.wav"
    path.write_bytes(wav_bytes(sines([-23, -23], 44100, seconds=4.0), 44100, 0x3))
    assert lv1.measure_file(str(path))["method"] == "bs1770-4-native"


def test_the_duration_limit_counts_metered_frames(tmp_path, monkeypatch):
    pytest.importorskip("av")
    monkeypatch.setattr(lv1, "MAX_SECONDS", 1.0)
    path = tmp_path / "long.wav"
    path.write_bytes(wav_bytes(sines([-23, -23], 96000, seconds=1.5), 96000, 0x3))
    with pytest.raises(lv1.ProfileResourceLimitError):
        lv1.measure_file(str(path))


def test_unmeasurable_values_are_null():
    rate = 48000
    short = native(sine_levels([(-20, 0.3)], rate), rate)
    assert short["integrated_lufs"] is None and short["max_momentary_lufs"] is None
    assert short["lra_lu"] is None and short["max_short_term_lufs"] is None
    assert short["true_peak_dbtp"] is not None
    under_three = native(sine_levels([(-20, 2.0)], rate), rate)
    assert under_three["integrated_lufs"] is not None
    assert under_three["lra_lu"] is None and under_three["max_short_term_lufs"] is None
    silence = native([np.zeros((2, 5 * rate))], rate)
    assert {k: silence[k] for k in ("integrated_lufs", "true_peak_dbtp", "sample_peak_dbfs", "lra_lu", "max_short_term_lufs", "max_momentary_lufs")} == dict.fromkeys(
        ("integrated_lufs", "true_peak_dbtp", "sample_peak_dbfs", "lra_lu", "max_short_term_lufs", "max_momentary_lufs"))
    assert (silence["channels"], silence["sample_rate"]) == (2, rate)
    # Under the absolute gate (a 20 Hz tone K-weighting all but removes): no integrated loudness and
    # no maxima, but a peak.
    quiet = native(sine_levels([(-58, 5.0)], rate, hz=20.0), rate)
    assert quiet["integrated_lufs"] is None and abs(quiet["true_peak_dbtp"] - -58) < 0.01
    assert quiet["max_momentary_lufs"] is None and quiet["max_short_term_lufs"] is None
    # Dither-level: every value outside the clients' ranges (§3.9 rule 1) is null, never published.
    dither = native(sine_levels([(-85, 5.0)], rate), rate)
    assert dither["true_peak_dbtp"] is None and dither["sample_peak_dbfs"] is not None
    assert dither["max_momentary_lufs"] is None and dither["integrated_lufs"] is None


def music(rate, seconds=12.0, channels=2, seed=7):
    """Tones, noise and a swell: content with a loudness range and high-frequency peaks.

    The noise is low-passed at 8 kHz, as a recording's spectrum falls: white noise would put most
    of a 96 kHz file's energy above 24 kHz, which the edge pass's 48 kHz series removes by design.
    """
    from scipy.signal import butter, lfilter

    rng = np.random.default_rng(seed)
    t = np.arange(int(seconds * rate)) / rate
    swell = 0.25 + 0.2 * np.sin(2 * np.pi * t / seconds)
    low_b, low_a = butter(2, 8000, btype="low", fs=rate)
    rows = []
    for channel in range(channels):
        tone = sum(a * np.sin(2 * np.pi * hz * t + channel) for a, hz in ((0.3, 110.0), (0.15, 880.0), (0.06, 7040.0)))
        noise = lfilter(low_b, low_a, rng.standard_normal(t.size))
        rows.append(np.clip(swell * tone + 0.05 * noise, -0.99, 0.99))
    return np.asarray(rows, dtype=np.float32)


def series(pcm, rate, block=7919):
    meter = lv1.SeriesMeter()
    payload = edge.analyze_edge_blocks(
        (pcm[:, i:i + block] for i in range(0, pcm.shape[1], block)), rate,
        catalog_instance_id="catalog-a", track_id="track-a", media_revision="sha256:" + "a" * 64,
        content_sha256="b" * 64, channel_layout="mono" if pcm.shape[0] == 1 else "stereo", loudness=meter)
    return payload, meter


@pytest.mark.parametrize("rate,channels", [(44100, 2), (48000, 2), (96000, 2), (44100, 1)])
def test_series_path_agrees_with_the_native_path(rate, channels):
    pytest.importorskip("av")
    pcm = music(rate, channels=channels)
    _payload, meter = series(pcm, rate)
    record = meter.result(channels=channels, sample_rate=rate)
    reference = native([pcm.astype(np.float64)], rate, [2.0] if channels == 1 else [1.0, 1.0])
    assert record["method"] == "bs1770-4-48k"
    # The resampler's flush completes the last bins: as many 100 ms bins as the native path.
    assert len(meter.bins) == pcm.shape[1] // (rate // 10)
    assert abs(record["integrated_lufs"] - reference["integrated_lufs"]) <= 0.05
    assert abs(record["lra_lu"] - reference["lra_lu"]) <= 0.1
    assert abs(record["max_short_term_lufs"] - reference["max_short_term_lufs"]) <= 0.05
    assert abs(record["true_peak_dbtp"] - reference["true_peak_dbtp"]) <= 0.15


def test_edge_payload_is_byte_identical_with_loudness_in_the_same_decode():
    pytest.importorskip("av")
    pcm = music(44100, seconds=40.0)
    pcm[:, -44100 * 6:] *= np.linspace(1, 0, 44100 * 6, dtype=np.float32)  # a fade the music end may find
    payload, _meter = series(pcm, 44100)
    plain = edge.analyze_edge_blocks(
        (pcm[:, i:i + 7919] for i in range(0, pcm.shape[1], 7919)), 44100,
        catalog_instance_id="catalog-a", track_id="track-a", media_revision="sha256:" + "a" * 64,
        content_sha256="b" * 64, channel_layout="stereo")
    assert edge.canonical_json(payload) == edge.canonical_json(plain)


def test_the_edge_file_wrapper_rides_beside_the_payload(tmp_path):
    pytest.importorskip("av")
    path = tmp_path / "song.wav"
    path.write_bytes(wav_bytes(music(44100, seconds=8.0).astype(np.float64), 44100, 0x3))
    identity = dict(catalog_instance_id="catalog-a", track_id="track-a", media_revision="sha256:" + "a" * 64)
    measured = lv1.analyze_edge_file(str(path), **identity)
    record = measured.pop(lv1.LOUDNESS_KEY)
    assert edge.canonical_json(measured) == edge.canonical_json(edge.analyze_edge_file(str(path), **identity))
    assert record["method"] == "bs1770-4-48k" and record["channels"] == 2 and record["sample_rate"] == 44100
    assert abs(record["integrated_lufs"] - lv1.measure_file(str(path))["integrated_lufs"]) <= 0.05
    assert isinstance(record["histogram"], bytes)
    assert "histogram" not in lv1.track_record_fields(record)


def _raise(*_args, **_kwargs):
    raise FloatingPointError("meter bug")


@pytest.mark.parametrize("target", ["true_peak", "derive"])
def test_a_loudness_error_never_fails_the_edge(tmp_path, monkeypatch, target):
    """The edge (SmoothFade's data) comes first: a meter that raises leaves the payload as it is and
    no record beside it, and the backfill measures the track later."""
    pytest.importorskip("av")
    path = tmp_path / "song.wav"
    path.write_bytes(wav_bytes(music(44100, seconds=8.0).astype(np.float64), 44100, 0x3))
    identity = dict(catalog_instance_id="catalog-a", track_id="track-a", media_revision="sha256:" + "a" * 64)
    plain = edge.canonical_json(edge.analyze_edge_file(str(path), **identity))
    if target == "true_peak":
        monkeypatch.setattr(lv1._TruePeak, "add", _raise)
    else:
        monkeypatch.setattr(lv1, "derive", _raise)
    measured = lv1.analyze_edge_file(str(path), **identity)
    assert lv1.LOUDNESS_KEY not in measured and edge.canonical_json(measured) == plain


def test_a_series_meter_bin_error_means_no_record():
    meter = lv1.SeriesMeter()
    meter.canonical(0.1 * np.ones((2, 4800)))
    meter.bins = None  # a bin can't be kept
    meter.bin(1e-3)
    meter.canonical(0.1 * np.ones((2, 4800)))  # and the meter stays stopped
    assert meter.result(channels=2, sample_rate=48000) is None


def test_album_pooling_matches_the_album_played_whole():
    rate = 48000
    tracks = [music(rate, seconds=20.0, seed=seed).astype(np.float64) * gain for seed, gain in ((1, 1.0), (2, 0.3), (3, 0.6))]
    meters = []
    for pcm in tracks:
        meters.append(lv1.NativeMeter(rate, (1.0, 1.0)))
        meters[-1].add(pcm)
    members = [meter.result() for meter in meters]
    whole = native([np.concatenate(tracks, axis=1)], rate)
    album = lv1.pool_album(members)
    assert abs(album["integrated_lufs"] - whole["integrated_lufs"]) <= 0.05
    # A window never spans two tracks: the range is the members' own 3 s windows gated together
    # (the concatenation's windows across a level change read up to 0.4 LU wider here).
    windows = np.concatenate([lv1._windows(np.asarray(meter.bins), lv1.SHORT_TERM_BINS) for meter in meters])
    assert abs(album["lra_lu"] - lv1._loudness_range(windows)) <= 0.1
    assert album["true_peak_dbtp"] == max(m["true_peak_dbtp"] for m in members)
    assert album["loudest_track_lufs"] == max(m["integrated_lufs"] for m in members)
    assert (album["members"], album["exact"]) == (3, True)


def test_album_peaks_skip_silent_and_dither_members():
    rate = 48000
    loud = native([music(rate, seconds=10.0).astype(np.float64)], rate)
    silent = native([np.zeros((2, 10 * rate))], rate)
    dither = native(sine_levels([(-85, 10.0)], rate), rate)
    album = lv1.pool_album([loud, silent, dither])
    assert album["true_peak_dbtp"] == loud["true_peak_dbtp"]
    assert album["integrated_lufs"] is not None and album["members"] == 3
    assert lv1.pool_album([silent])["integrated_lufs"] is None
    assert lv1.pool_album([silent, dither])["true_peak_dbtp"] is None


def test_album_gate_bin_follows_libebur128():
    counts = np.zeros(lv1.HISTOGRAM_BINS, dtype=np.int64)
    counts[500] = 1  # one block at -20.0..-19.9: mean -19.95, gate -29.95, bin 400 (-30.0..-29.9)
    assert lv1._histogram_gate(counts, lv1.RELATIVE_GATE) == 400  # at the bin's centre: included
    counts[:] = 0
    counts[501] = 1  # gate -29.85, exactly bin 401's centre: included
    assert lv1._histogram_gate(counts, lv1.RELATIVE_GATE) == 401
    counts[:] = 0
    counts[[501, 502]] = 1  # mean of -19.85 and -19.75: the gate is over bin 402's centre
    gate = float(lv1.lufs(np.dot(counts, lv1._histogram_energy()) / 2)) + lv1.RELATIVE_GATE
    start = int((gate + 70) * 10)
    expected = start + 1 if gate > -70 + (start + 0.5) / 10 else start
    assert lv1._histogram_gate(counts, lv1.RELATIVE_GATE) == expected
    assert lv1._histogram_gate(np.zeros(lv1.HISTOGRAM_BINS, dtype=np.int64), lv1.RELATIVE_GATE) is None


def test_album_pooling_is_within_three_hundredths_of_gating_every_block():
    rng = np.random.default_rng(3)
    worst = 0.0
    for _ in range(150):
        levels = rng.normal(-20, rng.uniform(2, 10), rng.integers(1000, 20000))
        energies = 10 ** ((levels - lv1.LUFS_OFFSET) / 10)
        exact = float(lv1.lufs(np.mean(lv1._gated_mean(energies, lv1.RELATIVE_GATE))))
        blob = lv1.encode_histogram(lv1._histogram(energies), np.zeros(lv1.HISTOGRAM_BINS, dtype=np.uint32))
        worst = max(worst, abs(lv1.pool_album([{"histogram": blob}])["integrated_lufs"] - exact))
    assert worst <= 0.03, worst


def test_histograms_round_trip_and_refuse_other_formats():
    blocks = np.arange(lv1.HISTOGRAM_BINS, dtype=np.uint32)
    windows = np.zeros(lv1.HISTOGRAM_BINS, dtype=np.uint32)
    decoded = lv1.decode_histogram(lv1.encode_histogram(blocks, windows))
    assert decoded[0].tolist() == blocks.tolist() and decoded[1].sum() == 0
    with pytest.raises(ValueError):
        lv1.decode_histogram(b"\x02" + lv1.encode_histogram(blocks, windows)[1:])
