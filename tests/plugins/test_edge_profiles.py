import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import struct
import wave

import numpy as np
import pytest

_path = Path(__file__).resolve().parents[2] / "plugins/LumaeAnalysis/edge_profiles.py"
_spec = importlib.util.spec_from_file_location("edge_profiles_under_test", _path)
edge = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = edge
_spec.loader.exec_module(edge)


def measure(pcm, rate, block=7919):
    return edge.analyze_edge_blocks(
        (pcm[:, i:i + block] for i in range(0, pcm.shape[1], block)), rate,
        catalog_instance_id="catalog-a", track_id="track-a",
        media_revision="sha256:" + "a" * 64, content_sha256="b" * 64,
        channel_layout="mono" if pcm.shape[0] == 1 else "stereo",
    )


def values(profile, name="head", series="level_cdb"):
    return np.frombuffer(base64.b64decode(profile[name][series]), dtype="<i2")


def test_published_cross_language_golden_contract():
    time = np.arange(10003, dtype=np.float64) / 48000
    pcm = np.asarray([.1 * np.sin(2 * np.pi * 1000 * time), .08 * np.sin(2 * np.pi * 400 * time)], dtype=np.float32)
    pcm[:, :403] = 0
    result = edge.analyze_edge_blocks([pcm], 48000, catalog_instance_id='catalog-a', track_id='track-a',
        media_revision='sha256:' + 'a' * 64, content_sha256='b' * 64, timeline_verified=True)
    golden = json.loads((Path(__file__).parent / 'edge_profile_v2_golden.json').read_text(encoding='utf-8'))
    assert result == golden


def test_v2_contract_series_are_bounded_exact_and_true_peak_is_conservative():
    rate = 48000
    time = np.arange(rate * 2 + 17, dtype=np.float64) / rate
    pcm = np.asarray([
        .85 * np.sin(2 * np.pi * 80 * time),
        .65 * np.sin(2 * np.pi * 6000 * time),
    ], dtype=np.float32)
    result = measure(pcm, rate)
    assert result['schema_version'] == 2
    assert result['analyzer_version'] == 'lumae-edge-v2.0.0'
    assert result['measurement'] == {
        'method': 'lumae-edge-kweighted-bands-48k-k4-v2',
        'sample_rate': 48000,
        'channel_rule': 'mean-power',
        'quantization': 's16le-centidb-u16le-q15-v2',
        'resampler': 'pyav-16.1.0-swr-6.1.100',
        'true_peak_oversample': 4,
        'crossover_hz': [150, 2500],
    }
    for window_name in ('head', 'tail'):
        window = result[window_name]
        count = window['bin_count']
        assert count <= 300
        for name in ('level_cdb', 'peak_cdb', 'true_peak_cdb', 'low_power_cdb',
                     'mid_power_cdb', 'high_power_cdb', 'spectral_flux_q15',
                     'onset_density_q15'):
            assert len(base64.b64decode(window[name])) == count * 2
        sample_peak = values(result, window_name, 'peak_cdb').astype(int)
        true_peak = values(result, window_name, 'true_peak_cdb').astype(int)
        assert np.all(true_peak >= sample_peak)


def test_hidden_track_guard_and_uncertain_quiet_are_never_padding():
    rate = 48000
    pcm = np.zeros((1, rate * 12), dtype=np.float32)
    tone = .04 * np.sin(2 * np.pi * 440 * np.arange(rate * 3) / rate)
    pcm[0, :rate * 3] = tone
    pcm[0, rate * 7:rate * 10] = tone
    # Analog noise after the late material is quiet, but not verified padding.
    pcm[0, rate * 10:] = 1e-6
    result = measure(pcm, rate)
    assert result['landmarks']['hidden_content_guard'] is True
    assert result['landmarks']['trailing_padding_frames'] == 0
    assert result['landmarks']['terminal_silence_confidence_q15'] == 0
    assert result['landmarks']['audible_end_frame'] >= rate * 10


def test_exact_terminal_zero_is_the_only_authorized_trim():
    rate = 48000
    pcm = np.full((1, rate * 4), .01, dtype=np.float32)
    pcm[:, -1733:] = 0
    verified = edge.analyze_edge_blocks(
        [pcm], rate, catalog_instance_id='catalog-a', track_id='track-a',
        media_revision='sha256:' + 'a' * 64, content_sha256='b' * 64,
        channel_layout='mono', timeline_verified=True,
    )
    assert verified['landmarks']['trailing_padding_frames'] == 1733
    assert verified['landmarks']['audible_end_frame'] == pcm.shape[1] - 1733
    assert verified['landmarks']['terminal_silence_confidence_q15'] == 32768


def test_low_and_high_tones_land_in_the_expected_complementary_bands():
    rate = 48000
    time = np.arange(rate * 2, dtype=np.float64) / rate
    low = measure(np.asarray([.1 * np.sin(2 * np.pi * 70 * time)], dtype=np.float32), rate)
    high = measure(np.asarray([.1 * np.sin(2 * np.pi * 8000 * time)], dtype=np.float32), rate)
    low_low = np.median(values(low, series='low_power_cdb'))
    low_high = np.median(values(low, series='high_power_cdb'))
    high_low = np.median(values(high, series='low_power_cdb'))
    high_high = np.median(values(high, series='high_power_cdb'))
    assert low_low > low_high + 1200
    assert high_high > high_low + 1200


def test_peak_quantization_never_rounds_below_original_at_centidb_boundaries():
    for db in np.linspace(-12000, 0, 2401):
        exact = 10 ** (float(db) / 2000)
        for value in [exact, np.nextafter(exact, np.inf), np.nextafter(exact, 0)]:
            quantized = edge.quantize_db(value, peak=True)
            assert 10 ** (quantized / 2000) >= value
    with pytest.raises(edge.EdgeProfileError):
        edge.quantize_db(1e300, peak=True)


def test_real_flac_decoder_timeline_silence_and_source_replacement(tmp_path, monkeypatch):
    av = edge._av()
    path = tmp_path / 'source.flac'
    pcm = (3000 * np.sin(2 * np.pi * 440 * np.arange(10003) / 48000)).astype(np.int16)[None, :]
    pcm[:, :403] = 0
    with av.open(str(path), 'w') as output:
        stream = output.add_stream('flac', rate=48000)
        stream.layout = 'mono'
        frame = av.AudioFrame.from_ndarray(pcm, format='s16', layout='mono')
        frame.sample_rate = 48000
        for packet in stream.encode(frame):
            output.mux(packet)
        for packet in stream.encode(None):
            output.mux(packet)
    args = dict(catalog_instance_id='catalog-a', track_id='track-a', media_revision='sha256:' + 'a' * 64)
    result = edge.analyze_edge_file(path, **args)
    assert result['source']['decoded_frames'] == 10003
    assert result['source']['timeline_verified'] is True
    assert result['source']['decoder'] == 'pyav-16.1.0:flac'
    assert result['leading_silence']['frames'] == 403
    original = edge.analyze_edge_blocks
    def replace_during_measurement(*args, **kwargs):
        result = original(*args, **kwargs)
        with path.open('ab') as changed:
            changed.write(b'source replacement')
        return result
    monkeypatch.setattr(edge, 'analyze_edge_blocks', replace_during_measurement)
    with pytest.raises(edge.EdgeProfileError, match='source changed'):
        edge.analyze_edge_file(path, **args)


@pytest.mark.parametrize("rate", [44100, 48000, 96000, 192000])
def test_equivalent_bandlimited_sources_within_point_two_db(rate):
    def signal(sr):
        t = np.arange(sr * 2, dtype=np.float64) / sr
        return np.asarray([0.1 * np.sin(2 * np.pi * 1000 * t), 0.08 * np.sin(2 * np.pi * 400 * t)], dtype=np.float32)
    reference = measure(signal(48000), 48000)
    actual = measure(signal(rate), rate)
    assert np.max(np.abs(values(actual).astype(int) - values(reference))) <= 20
    assert actual["source"]["decoded_frames"] == rate * 2


def test_block_partition_does_not_restart_resampler_or_tail_filters():
    rate = 44100
    t = np.arange(rate * 32 + 713, dtype=np.float64) / rate
    pcm = np.asarray([0.09 * np.sin(2 * np.pi * 73 * t)], dtype=np.float32)
    a = measure(pcm, rate, 7919)
    b = measure(pcm, rate, 32000)
    assert a == b
    assert a["tail"]["origin_frame"] == len(t) - rate * 30
    assert a["tail"]["boundaries"][-1] == len(t)
    assert a["tail"]["bin_count"] == 300


def test_short_track_partial_bin_overlap_and_digital_zero_are_exact():
    pcm = np.full((1, 4800 + 701), 0.0001, dtype=np.float32)
    pcm[:, :701] = 0
    result = measure(pcm, 48000)
    assert result["head"] == result["tail"]
    assert result["head"]["boundaries"] == [0, 4800, 5501]
    assert result["leading_silence"] == {"frames": 701, "method": "digital-zero", "verified": True}
    assert result["source"]["timeline_verified"] is False
    assert result['landmarks']['leading_padding_frames'] == 701
    assert result['landmarks']['trailing_padding_frames'] == 0
    assert base64.b64decode(result["head"]["valid"]) == bytes([3])


def test_quantized_source_peaks_never_understate_samples():
    rng = np.random.default_rng(7)
    pcm = rng.uniform(-0.89, 0.91, (2, 48000 + 31)).astype(np.float32)
    result = measure(pcm, 48000)
    for begin, end, cdb in zip(result["head"]["boundaries"], result["head"]["boundaries"][1:], values(result, series="peak_cdb")):
        actual = float(np.max(np.abs(pcm[:, begin:end])))
        bound = 10 ** (int(cdb) / 2000)
        assert bound >= actual
        assert bound <= actual * 10 ** (0.010001 / 20)


def test_exact_zero_is_not_a_missing_measurement():
    result = measure(np.zeros((1, 4800), dtype=np.float32), 48000)
    assert values(result).tolist() == [-32768]
    assert values(result, series="peak_cdb").tolist() == [-32768]
    assert result["head"]["valid"] == "AQ=="
    assert result["leading_silence"]["frames"] == 4800
    assert result['landmarks']['audible_start_frame'] == 4800
    assert result['landmarks']['body_start_frame'] == 4800


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1, 1e40, 1e-100])
def test_quantization_rejects_unrepresentable_values(bad):
    with pytest.raises(edge.EdgeProfileError):
        edge.quantize_db(bad)


def test_digest_is_independent_and_contains_no_raw_media_path():
    revision = edge.opaque_revision("C:/private/music/source.wav:100:123")
    assert revision.startswith("sha256:") and "private" not in revision
    result = measure(np.full((1, 4800), 0.01, dtype=np.float32), 48000)
    advertised = result.pop("profile_digest")
    canonical = json.dumps(result, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert advertised == hashlib.sha256(canonical.encode()).hexdigest()


def test_real_pcm_decoder_content_hash_and_valid_frame_count(tmp_path):
    path = tmp_path / "source.wav"
    samples = np.full((10003, 2), 3200, dtype="<i2")
    samples[:403] = 0
    with wave.open(str(path), "wb") as out:
        out.setnchannels(2)
        out.setsampwidth(2)
        out.setframerate(44100)
        out.writeframes(samples.tobytes())
    # WAVEFORMATEX without a channel mask is reported as "2 channels" by
    # this decoder. Do not silently certify that unknown layout as stereo.
    with pytest.raises(edge.EdgeProfileError, match="channel layout"):
        edge.analyze_edge_file(path, catalog_instance_id="catalog-a", track_id="track-a", media_revision="sha256:" + "a" * 64)
    fmt = struct.pack("<HHIIHHHHI16s", 0xfffe, 2, 44100, 44100 * 4, 4, 16, 22, 16, 3,
                      bytes.fromhex("0100000000001000800000aa00389b71"))
    data = b"fmt " + struct.pack("<I", len(fmt)) + fmt + b"data" + struct.pack("<I", samples.nbytes) + samples.tobytes()
    path.write_bytes(b"RIFF" + struct.pack("<I", len(data) + 4) + b"WAVE" + data)
    result = edge.analyze_edge_file(path, catalog_instance_id="catalog-a", track_id="track-a", media_revision="sha256:" + "a" * 64)
    assert result["content_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert result["source"]["decoded_frames"] == len(samples)
    assert result["source"]["timeline_verified"] is True
    assert result["leading_silence"]["frames"] == 403


def test_channel_and_nonfinite_pcm_rejection():
    with pytest.raises(edge.EdgeProfileError):
        measure(np.ones((3, 1000), dtype=np.float32), 48000)
    with pytest.raises(edge.EdgeProfileError):
        measure(np.full((1, 1000), np.nan, dtype=np.float32), 48000)


def test_bounded_storage_does_not_grow_with_source_duration():
    samples = edge._EdgeSamples(100, np.float32)
    size = samples.head.nbytes + samples.tail.nbytes
    for _ in range(1000):
        samples.push(np.arange(77, dtype=np.float32))
    assert samples.head.nbytes + samples.tail.nbytes == size
    assert len(samples.window(samples.total - 100, samples.total)) == 100
