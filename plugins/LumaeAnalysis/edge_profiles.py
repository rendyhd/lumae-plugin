"""EdgeProfileV1: bounded, source-timeline measurements, independent of legacy LUFS.

No network or database access. Decoder/resampler and both IIR states live for the
whole source. Only 30 seconds at either edge are retained; source peaks are taken
before resampling. The wire digest includes every field except profile_digest.
"""

import base64
import hashlib
import json
import math
import os
import time

import numpy as np
from scipy.signal import lfilter

METHOD = "lumae-weighted-power-48k-k2-v1"
QUANTIZATION = "s16le-centidb-v1"
CANONICAL_RATE = 48_000
EDGE_SECONDS = 30
ZERO_CDB = -32768
MAX_SOURCE_RATE = 384_000
MAX_SECONDS = 6553.6
BLOCK_FRAMES = 65_536
PYAV_VERSION = "16.1.0"
SWR_VERSION = (6, 1, 100)
_B1 = (1.53512485958697, -2.69169618940638, 1.19839281085285)
_A1 = (1.0, -1.69065929318241, 0.73248077421585)
_B2 = (1.0, -2.0, 1.0)
_A2 = (1.0, -1.99004745483398, 0.99007225036621)


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
    # Guard a logarithm rounding down at an exact centidB boundary.
    if peak and 10 ** (quantized / 2000) < value:
        quantized += 1
    if not -32767 <= quantized <= 32767:
        raise EdgeProfileError("measurement outside centidB range")
    return quantized


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
    """Head plus circular tail; does not grow with recording duration."""

    def __init__(self, capacity, dtype):
        self.capacity = capacity
        self.head = np.empty(capacity, dtype=dtype)
        self.tail = np.empty(capacity, dtype=dtype)
        self.total = 0

    def push(self, values):
        count = len(values)
        first = min(count, max(0, self.capacity - self.total))
        if first:
            self.head[self.total : self.total + first] = values[:first]
        offset = 0
        while offset < count:
            pos = (self.total + offset) % self.capacity
            take = min(count - offset, self.capacity - pos)
            self.tail[pos : pos + take] = values[offset : offset + take]
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
            return self.tail[pos : pos + length]
        return np.concatenate((self.tail[pos:], self.tail[: length - (self.capacity - pos)]))


def _pack(values):
    return base64.b64encode(np.asarray(values, dtype="<i2").tobytes()).decode("ascii")


def _edge(peaks, powers, sample_rate, origin, end):
    boundaries = [origin]
    k = 1
    while boundaries[-1] < end:
        boundaries.append(min(end, origin + k * sample_rate // 10))
        k += 1
    levels, peak_bounds = [], []
    for begin, finish in zip(boundaries, boundaries[1:]):
        peak = float(np.max(peaks.window(begin, finish)))
        weighted_begin = begin * CANONICAL_RATE // sample_rate
        weighted_end = min(powers.total, finish * CANONICAL_RATE // sample_rate)
        # A final fraction of a canonical sample belongs to the last bin.
        if finish == peaks.total:
            weighted_end = powers.total
        power = float(np.mean(powers.window(weighted_begin, weighted_end)))
        levels.append(quantize_db(power))
        peak_bounds.append(quantize_db(peak, peak=True))
    count = len(levels)
    mask = bytearray((count + 7) // 8)
    for i in range(count):
        mask[i // 8] |= 1 << (i % 8)
    return {
        "origin_frame": origin,
        "covered_frames": end - origin,
        "bin_count": count,
        "boundaries": boundaries,
        "valid": base64.b64encode(mask).decode("ascii"),
        "level_cdb": _pack(levels),
        "peak_cdb": _pack(peak_bounds),
    }


def analyze_edge_blocks(blocks, sample_rate, *, catalog_instance_id, track_id,
                        media_revision, content_sha256, channel_layout="stereo",
                        decoder="pcm-input-v1", timeline_verified=False, deadline=None):
    """Measure channel-first float PCM. Caller supplies independently verified identity."""
    av = _av()
    if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or not 10 <= sample_rate <= MAX_SOURCE_RATE:
        raise EdgeProfileError("unsupported source sample rate")
    if channel_layout not in ("mono", "stereo"):
        raise EdgeProfileError("unqualified channel layout")
    for token in (catalog_instance_id, track_id, media_revision):
        if not isinstance(token, str) or not 0 < len(token) <= 512:
            raise EdgeProfileError("invalid source identity")
    if not media_revision.startswith('sha256:') or len(media_revision) != 71 or any(c not in '0123456789abcdef' for c in media_revision[7:]):
        raise EdgeProfileError("invalid media revision")
    if not isinstance(content_sha256, str) or len(content_sha256) != 64 or any(c not in "0123456789abcdef" for c in content_sha256):
        raise EdgeProfileError("invalid content hash")
    channels = 1 if channel_layout == "mono" else 2
    resampler = av.AudioResampler(format="fltp", layout=channel_layout, rate=CANONICAL_RATE)
    # One spare canonical bin handles rational-rate rounding at tail origin.
    peaks = _EdgeSamples(EDGE_SECONDS * sample_rate, np.float32)
    powers = _EdgeSamples((EDGE_SECONDS + 1) * CANONICAL_RATE, np.float64)
    z1 = np.zeros((channels, 2), dtype=np.float64)
    z2 = np.zeros((channels, 2), dtype=np.float64)
    leading_zero_frames = 0
    found_signal = False

    def consume_weighted(frame):
        nonlocal z1, z2
        for converted in resampler.resample(frame):
            pcm = converted.to_ndarray().astype(np.float64)
            weighted, z1 = lfilter(_B1, _A1, pcm, axis=1, zi=z1)
            weighted, z2 = lfilter(_B2, _A2, weighted, axis=1, zi=z2)
            powers.push(np.mean(weighted * weighted, axis=0))

    for raw in blocks:
        _check_deadline(deadline)
        block = np.asarray(raw, dtype=np.float32)
        if block.ndim != 2 or block.shape[0] != channels:
            raise EdgeProfileError("source channel layout changed")
        for offset in range(0, block.shape[1], BLOCK_FRAMES):
            _check_deadline(deadline)
            pcm = np.ascontiguousarray(block[:, offset : offset + BLOCK_FRAMES])
            if not np.all(np.isfinite(pcm)):
                raise EdgeProfileError("non-finite source PCM")
            if peaks.total + pcm.shape[1] > int(MAX_SECONDS * sample_rate):
                raise EdgeProfileError("edge duration limit exceeded")
            peak = np.max(np.abs(pcm), axis=0)
            if not found_signal:
                nonzero = np.flatnonzero(peak)
                leading_zero_frames += int(nonzero[0]) if len(nonzero) else len(peak)
                found_signal = len(nonzero) > 0
            frame = av.AudioFrame.from_ndarray(pcm, format="fltp", layout=channel_layout)
            frame.sample_rate = sample_rate
            frame.pts = peaks.total
            peaks.push(peak)
            consume_weighted(frame)
    consume_weighted(None)
    _check_deadline(deadline)
    if peaks.total == 0 or abs(powers.total - peaks.total * CANONICAL_RATE / sample_rate) > 1:
        raise EdgeProfileError("invalid decoded/resampled timeline")
    swr = ".".join(str(v) for v in av.library_versions["libswresample"])
    representation_id = "sha256:" + content_sha256
    payload = {
        "schema_version": 1,
        "catalog_instance_id": catalog_instance_id,
        "track_id": track_id,
        "media_revision": media_revision,
        "representation_id": representation_id,
        "content_sha256": content_sha256,
        "source": {"sample_rate": sample_rate, "channel_layout": channel_layout,
                   "decoded_frames": peaks.total, "decoder": decoder,
                   "padding": "decoder-output-v1", "timeline_verified": timeline_verified},
        "measurement": {"method": METHOD, "sample_rate": CANONICAL_RATE,
                        "channel_rule": "mean-power", "quantization": QUANTIZATION,
                        "resampler": "pyav-" + PYAV_VERSION + "-swr-" + swr},
        "leading_silence": {"frames": leading_zero_frames, "method": "digital-zero", "verified": True},
        "head": _edge(peaks, powers, sample_rate, 0, min(peaks.total, EDGE_SECONDS * sample_rate)),
        "tail": _edge(peaks, powers, sample_rate, max(0, peaks.total - EDGE_SECONDS * sample_rate), peaks.total),
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

            result = analyze_edge_blocks(
                blocks(), rate, catalog_instance_id=catalog_instance_id, track_id=track_id,
                media_revision=media_revision or "sha256:" + content_hash, content_sha256=content_hash,
                channel_layout=layout, decoder="pyav-" + PYAV_VERSION + ":" + codec,
                timeline_verified=codec in ("flac", "pcm_s16le", "pcm_s24le", "pcm_s32le", "pcm_f32le"),
                deadline=deadline,
            )
        after = os.fstat(source.fileno())
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or hash_source() != content_hash:
            raise EdgeProfileError("source changed during analysis")
        return result
