"""EdgeProfileV2: bounded source-timeline measurements for transition planning.

The decoder, resampler and all recursive filters remain continuous for the whole
source. Only the first and last 30 seconds of measurements and canonical PCM are
retained. Exact digital zero is reported separately from adaptive quiet regions;
only the former can ever authorize padding trim.
"""

import base64
import hashlib
import json
import math
import os
import time

import numpy as np
from scipy.signal import butter, lfilter, resample_poly

SCHEMA_VERSION = 2
ANALYZER_VERSION = "lumae-edge-v2.0.0"
METHOD = "lumae-edge-kweighted-bands-48k-k4-v2"
QUANTIZATION = "s16le-centidb-u16le-q15-v2"
CANONICAL_RATE = 48_000
EDGE_SECONDS = 30
ZERO_CDB = -32768
MAX_SOURCE_RATE = 384_000
MAX_SECONDS = 6553.6
BLOCK_FRAMES = 65_536
TRUE_PEAK_OVERSAMPLE = 4
CROSSOVER_HZ = (150, 2500)
PYAV_VERSION = "16.1.0"
SWR_VERSION = (6, 1, 100)
_B1 = (1.53512485958697, -2.69169618940638, 1.19839281085285)
_A1 = (1.0, -1.69065929318241, 0.73248077421585)
_B2 = (1.0, -2.0, 1.0)
_A2 = (1.0, -1.99004745483398, 0.99007225036621)
_LOW_B, _LOW_A = butter(4, CROSSOVER_HZ[0], btype="low", fs=CANONICAL_RATE)
_MID_B, _MID_A = butter(4, CROSSOVER_HZ[1], btype="low", fs=CANONICAL_RATE)


class EdgeProfileError(ValueError):
    pass


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def opaque_revision(media_signature):
    if not media_signature:
        return None
    return "sha256:" + hashlib.sha256(str(media_signature).encode("utf-8")).hexdigest()


def profile_digest(payload):
    return hashlib.sha256(canonical_json({k: v for k, v in payload.items() if k != "profile_digest"}).encode("utf-8")).hexdigest()


def quantize_db(value, *, peak=False):
    if not math.isfinite(value) or value < 0:
        raise EdgeProfileError("non-finite or negative measurement")
    if value == 0:
        return ZERO_CDB
    db = (20 if peak else 10) * math.log10(value) * 100
    quantized = math.ceil(db) if peak else math.floor(db + 0.5)
    if not -32767 <= quantized <= 32767:
        raise EdgeProfileError("measurement outside centidB range")
    if peak and 10 ** (quantized / 2000) < value:
        quantized += 1
    if not -32767 <= quantized <= 32767:
        raise EdgeProfileError("measurement outside centidB range")
    return quantized


def _q15(value):
    if not math.isfinite(value):
        raise EdgeProfileError("non-finite normalized measurement")
    return max(0, min(32768, int(round(value * 32768))))


def _check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise EdgeProfileError("edge analysis deadline exceeded")


def _av():
    import av
    if av.__version__ != PYAV_VERSION or av.library_versions.get("libswresample") != SWR_VERSION:
        raise EdgeProfileError("edge analysis requires PyAV 16.1.0 with libswresample 6.1.100")
    return av


def edge_runtime_available():
    try:
        _av()
        return True
    except (ImportError, EdgeProfileError):
        return False


class _EdgeSamples:
    """Head plus circular tail; allocation never grows with recording duration."""

    def __init__(self, capacity, dtype):
        self.capacity = capacity
        self.head = np.empty(capacity, dtype=dtype)
        self.tail = np.empty(capacity, dtype=dtype)
        self.total = 0

    def push(self, values):
        values = np.asarray(values)
        count = len(values)
        first = min(count, max(0, self.capacity - self.total))
        if first:
            self.head[self.total:self.total + first] = values[:first]
        offset = 0
        while offset < count:
            pos = (self.total + offset) % self.capacity
            take = min(count - offset, self.capacity - pos)
            self.tail[pos:pos + take] = values[offset:offset + take]
            offset += take
        self.total += count

    def window(self, start, end):
        if start < 0 or end > self.total or end <= start:
            raise EdgeProfileError("invalid edge sample coverage")
        if end <= min(self.total, self.capacity):
            return self.head[start:end]
        if start < self.total - self.capacity:
            raise EdgeProfileError("edge sample coverage unavailable")
        pos = start % self.capacity
        length = end - start
        if pos + length <= self.capacity:
            return self.tail[pos:pos + length]
        return np.concatenate((self.tail[pos:], self.tail[:length - (self.capacity - pos)]))


def _pack_i16(values):
    return base64.b64encode(np.asarray(values, dtype="<i2").tobytes()).decode("ascii")


def _pack_u16(values):
    return base64.b64encode(np.asarray(values, dtype="<u2").tobytes()).decode("ascii")


def _boundaries(sample_rate, origin, end):
    result = [origin]
    index = 1
    while result[-1] < end:
        result.append(min(end, origin + index * sample_rate // 10))
        index += 1
    return result


def _canonical_range(begin, finish, source_rate, canonical_total, source_total):
    start = begin * CANONICAL_RATE // source_rate
    end = min(canonical_total, finish * CANONICAL_RATE // source_rate)
    if finish == source_total:
        end = canonical_total
    if end <= start:
        end = min(canonical_total, start + 1)
    return start, end


def _spectral_features(pcm):
    """Bounded evidence, not a probability: spectral change and onset density."""
    mono = np.mean(pcm.astype(np.float64), axis=0)
    if not len(mono) or np.max(np.abs(mono)) == 0:
        return 0, 0
    split = len(mono) // 2
    if split >= 64:
        previous = np.abs(np.fft.rfft(mono[:split] * np.hanning(split)))
        current = np.abs(np.fft.rfft(mono[-split:] * np.hanning(split)))
        count = min(len(previous), len(current))
        flux = float(np.sum(np.maximum(0.0, current[:count] - previous[:count]))) / max(
            1e-12, float(np.sum(current[:count]))
        )
    else:
        flux = 0.0
    hop = max(1, CANONICAL_RATE // 100)
    rms = []
    for offset in range(0, len(mono), hop):
        part = mono[offset:offset + hop]
        if len(part):
            rms.append(math.sqrt(float(np.mean(part * part))))
    if len(rms) < 2:
        density = 0.0
    else:
        floor = max(1e-9, float(np.percentile(rms, 20)))
        onsets = sum(1 for before, after in zip(rms, rms[1:]) if after > max(before * 1.8, floor * 2.5))
        density = onsets / (len(rms) - 1)
    return _q15(min(1.0, flux * 2.0)), _q15(density)


def _true_peak(pcm, sample_peak):
    if pcm.size == 0 or sample_peak == 0:
        return sample_peak
    measured = sample_peak
    for channel in pcm:
        oversampled = resample_poly(channel.astype(np.float64), TRUE_PEAK_OVERSAMPLE, 1, padtype="line")
        if len(oversampled):
            measured = max(measured, float(np.max(np.abs(oversampled))))
    return measured


def _edge(peaks, powers, true_pcm, band_powers, sample_rate, origin, end):
    boundaries = _boundaries(sample_rate, origin, end)
    levels, peak_bounds, true_peaks = [], [], []
    low_levels, mid_levels, high_levels = [], [], []
    flux_values, onset_values = [], []
    for begin, finish in zip(boundaries, boundaries[1:]):
        sample_peak = float(np.max(peaks.window(begin, finish)))
        canonical_begin, canonical_end = _canonical_range(begin, finish, sample_rate, powers.total, peaks.total)
        power = float(np.mean(powers.window(canonical_begin, canonical_end)))
        low = float(np.mean(band_powers[0].window(canonical_begin, canonical_end)))
        mid = float(np.mean(band_powers[1].window(canonical_begin, canonical_end)))
        high = float(np.mean(band_powers[2].window(canonical_begin, canonical_end)))
        # Recursive filter residue below -300 dBFS is numerical zero, not a
        # meaningful source measurement and not representable in centidB i16.
        power = 0.0 if power < 1e-30 else power
        low = 0.0 if low < 1e-30 else low
        mid = 0.0 if mid < 1e-30 else mid
        high = 0.0 if high < 1e-30 else high
        pcm = np.stack([channel.window(canonical_begin, canonical_end) for channel in true_pcm])
        flux, onset = _spectral_features(pcm)
        levels.append(quantize_db(power))
        peak_bounds.append(quantize_db(sample_peak, peak=True))
        true_peaks.append(quantize_db(_true_peak(pcm, sample_peak), peak=True))
        low_levels.append(quantize_db(low))
        mid_levels.append(quantize_db(mid))
        high_levels.append(quantize_db(high))
        flux_values.append(flux)
        onset_values.append(onset)
    count = len(levels)
    mask = bytearray((count + 7) // 8)
    for index in range(count):
        mask[index // 8] |= 1 << (index % 8)
    wire = {
        "origin_frame": origin,
        "covered_frames": end - origin,
        "bin_count": count,
        "boundaries": boundaries,
        "valid": base64.b64encode(mask).decode("ascii"),
        "level_cdb": _pack_i16(levels),
        "peak_cdb": _pack_i16(peak_bounds),
        "true_peak_cdb": _pack_i16(true_peaks),
        "low_power_cdb": _pack_i16(low_levels),
        "mid_power_cdb": _pack_i16(mid_levels),
        "high_power_cdb": _pack_i16(high_levels),
        "spectral_flux_q15": _pack_u16(flux_values),
        "onset_density_q15": _pack_u16(onset_values),
    }
    # Raw source peaks protect quiet full-band or DC-like material from the
    # K-weighted high-pass being mistaken for terminal silence. A 6 dB crest
    # allowance keeps isolated low-level impulses from defining the body.
    evidence_levels = [max(level, peak - 600) for level, peak in zip(levels, peak_bounds)]
    return wire, evidence_levels


def _landmarks(head, head_levels, tail, tail_levels, total, leading_zeros, trailing_zeros, timeline_verified):
    represented = [value for value in head_levels + tail_levels if value != ZERO_CDB]
    if not represented:
        position = min(total, leading_zeros)
        return -12000, {
            "audible_start_frame": position,
            "body_start_frame": position,
            "body_end_frame": position,
            "audible_end_frame": position,
            "leading_padding_frames": position,
            "trailing_padding_frames": 0,
            "confidence_q15": 32768 if timeline_verified else 16384,
            "terminal_silence_confidence_q15": 0,
            "hidden_content_guard": False,
        }
    noise_floor = max(-12000, min(0, int(round(float(np.percentile(represented, 10))))))
    maximum = max(represented)
    audible_threshold = max(-12000, min(noise_floor + 1000, maximum - 1200))
    body_threshold = max(audible_threshold, maximum - 600)
    head_audible = [value != ZERO_CDB and value >= audible_threshold for value in head_levels]
    tail_audible = [value != ZERO_CDB and value >= audible_threshold for value in tail_levels]
    head_body = [value != ZERO_CDB and value >= body_threshold for value in head_levels]
    tail_body = [value != ZERO_CDB and value >= body_threshold for value in tail_levels]
    first_audible = next((i for i, yes in enumerate(head_audible) if yes), 0)
    last_audible = max((i for i, yes in enumerate(tail_audible) if yes), default=len(tail_levels) - 1)
    first_body = next((i for i, yes in enumerate(head_body) if yes), first_audible)
    last_body = max((i for i, yes in enumerate(tail_body) if yes), default=last_audible)
    audible_start = max(leading_zeros, head["boundaries"][first_audible])
    audible_end = min(total - trailing_zeros, tail["boundaries"][last_audible + 1])
    body_start = max(audible_start, head["boundaries"][first_body])
    body_end = min(audible_end, tail["boundaries"][last_body + 1])
    if body_start > body_end:
        body_start, body_end = audible_start, audible_end
    hidden_guard = False
    seen_audible = False
    quiet_run = 0
    for audible in tail_audible:
        if audible:
            if seen_audible and quiet_run >= 20:
                hidden_guard = True
            seen_audible = True
            quiet_run = 0
        elif seen_audible:
            quiet_run += 1
    separation = max(0, maximum - noise_floor)
    confidence = min(1.0, (0.84 if timeline_verified else 0.58) + min(0.12, separation / 10000))
    terminal_confidence = 1.0 if trailing_zeros > 0 and timeline_verified and not hidden_guard else 0.5 if trailing_zeros > 0 and not hidden_guard else 0.0
    return noise_floor, {
        "audible_start_frame": int(audible_start),
        "body_start_frame": int(body_start),
        "body_end_frame": int(body_end),
        "audible_end_frame": int(audible_end),
        "leading_padding_frames": int(leading_zeros),
        "trailing_padding_frames": int(trailing_zeros),
        "confidence_q15": _q15(confidence),
        "terminal_silence_confidence_q15": _q15(terminal_confidence),
        "hidden_content_guard": hidden_guard,
    }


def analyze_edge_blocks(blocks, sample_rate, *, catalog_instance_id, track_id,
                        media_revision, content_sha256, channel_layout="stereo",
                        decoder="pcm-input-v1", timeline_verified=False, deadline=None):
    """Measure channel-first float PCM. Identity must be verified by the caller."""
    av = _av()
    if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or not 10 <= sample_rate <= MAX_SOURCE_RATE:
        raise EdgeProfileError("unsupported source sample rate")
    if channel_layout not in ("mono", "stereo"):
        raise EdgeProfileError("unqualified channel layout")
    for token in (catalog_instance_id, track_id, media_revision):
        if not isinstance(token, str) or not 0 < len(token) <= 512:
            raise EdgeProfileError("invalid source identity")
    if not media_revision.startswith("sha256:") or len(media_revision) != 71 or any(c not in "0123456789abcdef" for c in media_revision[7:]):
        raise EdgeProfileError("invalid media revision")
    if not isinstance(content_sha256, str) or len(content_sha256) != 64 or any(c not in "0123456789abcdef" for c in content_sha256):
        raise EdgeProfileError("invalid content hash")
    channels = 1 if channel_layout == "mono" else 2
    resampler = av.AudioResampler(format="fltp", layout=channel_layout, rate=CANONICAL_RATE)
    peaks = _EdgeSamples(EDGE_SECONDS * sample_rate, np.float32)
    canonical_capacity = (EDGE_SECONDS + 1) * CANONICAL_RATE
    powers = _EdgeSamples(canonical_capacity, np.float64)
    true_pcm = [_EdgeSamples(canonical_capacity, np.float32) for _ in range(channels)]
    band_powers = [_EdgeSamples(canonical_capacity, np.float64) for _ in range(3)]
    z1 = np.zeros((channels, 2), dtype=np.float64)
    z2 = np.zeros((channels, 2), dtype=np.float64)
    low_z = np.zeros((channels, len(_LOW_A) - 1), dtype=np.float64)
    mid_z = np.zeros((channels, len(_MID_A) - 1), dtype=np.float64)
    leading_zero_frames = 0
    trailing_zero_frames = 0
    found_signal = False

    def consume_canonical(frame):
        nonlocal z1, z2, low_z, mid_z
        for converted in resampler.resample(frame):
            pcm = converted.to_ndarray().astype(np.float64)
            weighted, z1 = lfilter(_B1, _A1, pcm, axis=1, zi=z1)
            weighted, z2 = lfilter(_B2, _A2, weighted, axis=1, zi=z2)
            low, low_z = lfilter(_LOW_B, _LOW_A, pcm, axis=1, zi=low_z)
            upper = pcm - low
            mid, mid_z = lfilter(_MID_B, _MID_A, upper, axis=1, zi=mid_z)
            high = upper - mid
            powers.push(np.mean(weighted * weighted, axis=0))
            band_powers[0].push(np.mean(low * low, axis=0))
            band_powers[1].push(np.mean(mid * mid, axis=0))
            band_powers[2].push(np.mean(high * high, axis=0))
            for channel in range(channels):
                true_pcm[channel].push(pcm[channel].astype(np.float32))

    for raw in blocks:
        _check_deadline(deadline)
        block = np.asarray(raw, dtype=np.float32)
        if block.ndim != 2 or block.shape[0] != channels:
            raise EdgeProfileError("source channel layout changed")
        for offset in range(0, block.shape[1], BLOCK_FRAMES):
            _check_deadline(deadline)
            pcm = np.ascontiguousarray(block[:, offset:offset + BLOCK_FRAMES])
            if not np.all(np.isfinite(pcm)):
                raise EdgeProfileError("non-finite source PCM")
            if peaks.total + pcm.shape[1] > int(MAX_SECONDS * sample_rate):
                raise EdgeProfileError("edge duration limit exceeded")
            peak = np.max(np.abs(pcm), axis=0)
            nonzero = np.flatnonzero(peak)
            if not found_signal:
                leading_zero_frames += int(nonzero[0]) if len(nonzero) else len(peak)
                found_signal = len(nonzero) > 0
            trailing_zero_frames = len(peak) - int(nonzero[-1]) - 1 if len(nonzero) else trailing_zero_frames + len(peak)
            frame = av.AudioFrame.from_ndarray(pcm, format="fltp", layout=channel_layout)
            frame.sample_rate = sample_rate
            frame.pts = peaks.total
            peaks.push(peak)
            consume_canonical(frame)
    consume_canonical(None)
    _check_deadline(deadline)
    if peaks.total == 0 or abs(powers.total - peaks.total * CANONICAL_RATE / sample_rate) > 1:
        raise EdgeProfileError("invalid decoded/resampled timeline")
    head, head_levels = _edge(peaks, powers, true_pcm, band_powers, sample_rate, 0, min(peaks.total, EDGE_SECONDS * sample_rate))
    tail, tail_levels = _edge(peaks, powers, true_pcm, band_powers, sample_rate, max(0, peaks.total - EDGE_SECONDS * sample_rate), peaks.total)
    noise_floor, landmarks = _landmarks(head, head_levels, tail, tail_levels, peaks.total, leading_zero_frames, trailing_zero_frames, timeline_verified)
    swr = ".".join(str(value) for value in av.library_versions["libswresample"])
    payload = {
        "schema_version": SCHEMA_VERSION,
        "analyzer_version": ANALYZER_VERSION,
        "catalog_instance_id": catalog_instance_id,
        "track_id": track_id,
        "media_revision": media_revision,
        "representation_id": "sha256:" + content_sha256,
        "content_sha256": content_sha256,
        "source": {"sample_rate": sample_rate, "channel_layout": channel_layout, "decoded_frames": peaks.total, "decoder": decoder, "padding": "decoder-output-v1", "timeline_verified": timeline_verified},
        "measurement": {"method": METHOD, "sample_rate": CANONICAL_RATE, "channel_rule": "mean-power", "quantization": QUANTIZATION, "resampler": "pyav-" + PYAV_VERSION + "-swr-" + swr, "true_peak_oversample": TRUE_PEAK_OVERSAMPLE, "crossover_hz": list(CROSSOVER_HZ)},
        "leading_silence": {"frames": leading_zero_frames, "method": "digital-zero", "verified": True},
        "noise_floor_cdb": noise_floor,
        "landmarks": landmarks,
        "head": head,
        "tail": tail,
    }
    payload["profile_digest"] = profile_digest(payload)
    return payload


def analyze_edge_file(path, *, catalog_instance_id, track_id, media_revision, deadline_seconds=900):
    av = _av()
    deadline = time.monotonic() + max(1, deadline_seconds)
    with open(path, "rb") as source:
        before = os.fstat(source.fileno())

        def hash_source():
            source.seek(0)
            digest = hashlib.sha256()
            while chunk := source.read(1024 * 1024):
                _check_deadline(deadline)
                digest.update(chunk)
            return digest.hexdigest()

        content_hash = hash_source()
        source.seek(0)
        with av.open(source) as container:
            if not container.streams.audio:
                raise EdgeProfileError("no audio stream")
            stream = container.streams.audio[0]
            rate = stream.codec_context.sample_rate
            layout = stream.codec_context.layout.name
            codec = stream.codec_context.name
            converter = av.AudioResampler(format="fltp", layout=layout, rate=rate)

            def blocks():
                for frame in container.decode(stream):
                    _check_deadline(deadline)
                    for converted in converter.resample(frame):
                        yield converted.to_ndarray()
                for converted in converter.resample(None):
                    yield converted.to_ndarray()

            result = analyze_edge_blocks(blocks(), rate, catalog_instance_id=catalog_instance_id, track_id=track_id, media_revision=media_revision or "sha256:" + content_hash, content_sha256=content_hash, channel_layout=layout, decoder="pyav-" + PYAV_VERSION + ":" + codec, timeline_verified=codec in ("flac", "pcm_s16le", "pcm_s24le", "pcm_s32le", "pcm_f32le"), deadline=deadline)
        after = os.fstat(source.fileno())
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or hash_source() != content_hash:
            raise EdgeProfileError("source changed during analysis")
        return result
