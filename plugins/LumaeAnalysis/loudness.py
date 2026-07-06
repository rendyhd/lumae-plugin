from dataclasses import dataclass

import librosa
import numpy as np

from .ramp_codec import encode_ramp

THRESHOLDS_DB = [-90, -60, -40, -30, -24, -21, -18, -15, -12, -9, -6, -3, 0, 3, 6]
CHUNK_DURATION_MS = 100
LUFS_OFFSET_DB = -0.691
ABSOLUTE_GATE_LUFS = -70
MAX_CHUNK_INDEX = 0xFFFF

KWEIGHT_STAGE1 = (
    1.53512485958697,
    -2.69169618940638,
    1.19839281085285,
    -1.69065929318241,
    0.73248077421585,
)
KWEIGHT_STAGE2 = (1.0, -2.0, 1.0, -1.99004745483398, 0.99007225036621)


class SilentAudioError(RuntimeError):
    pass


@dataclass
class AnalysisResult:
    sample_rate: int
    duration_ms: int
    ref_lufs: float
    start_ramp: list
    end_ramp: list
    start_ramp_blob: bytes
    end_ramp_blob: bytes


def _as_channels(audio):
    arr = np.asarray(audio, dtype=np.float32)
    if arr.ndim == 1:
        return arr.reshape(1, -1)
    if arr.ndim == 2 and arr.shape[0] <= arr.shape[1]:
        return arr
    if arr.ndim == 2:
        return arr.T
    raise ValueError("audio must be mono or channel-first/channel-last stereo")


def _apply_biquad(channel, coefs):
    b0, b1, b2, a1, a2 = coefs
    out = np.empty(channel.shape[0], dtype=np.float64)
    x1 = x2 = y1 = y2 = 0.0
    for i, x0 in enumerate(channel.astype(np.float64, copy=False)):
        y0 = b0 * x0 + b1 * x1 + b2 * x2 - a1 * y1 - a2 * y2
        out[i] = y0
        x2, x1 = x1, x0
        y2, y1 = y1, y0
    return out


def _k_weight(channel):
    return _apply_biquad(_apply_biquad(channel, KWEIGHT_STAGE1), KWEIGHT_STAGE2)


def _mean_square_to_lufs(ms):
    if ms <= 0:
        return float("-inf")
    return 10 * np.log10(ms) + LUFS_OFFSET_DB


def _integrated_lufs(chunk_lufs):
    gated = [x for x in chunk_lufs if x > ABSOLUTE_GATE_LUFS]
    if not gated:
        return float("-inf")
    ms_values = [10 ** ((x - LUFS_OFFSET_DB) / 10) for x in gated]
    return 10 * np.log10(float(np.mean(ms_values))) + LUFS_OFFSET_DB


def _scan_forward(relative_db):
    entries = []
    for threshold in THRESHOLDS_DB:
        for i, value in enumerate(relative_db):
            if value >= threshold:
                entries.append((threshold, min(i, MAX_CHUNK_INDEX)))
                break
    return entries


def _scan_backward(relative_db):
    entries = []
    count = len(relative_db)
    for threshold in THRESHOLDS_DB:
        for i in range(count - 1, -1, -1):
            if relative_db[i] >= threshold:
                entries.append((threshold, min(count - 1 - i, MAX_CHUNK_INDEX)))
                break
    return entries


def analyze_buffer(audio, sample_rate):
    channels = _as_channels(audio)
    chunk_size = max(1, int(sample_rate * CHUNK_DURATION_MS / 1000))
    chunks = channels.shape[1] // chunk_size
    if chunks <= 0:
        raise SilentAudioError("silent or sub-gate")

    weighted = np.stack([_k_weight(channels[i]) for i in range(channels.shape[0])])
    chunk_lufs = []
    for i in range(chunks):
        start = i * chunk_size
        end = min(start + chunk_size, weighted.shape[1])
        window = weighted[:, start:end]
        ms = float(np.mean(window * window))
        chunk_lufs.append(_mean_square_to_lufs(ms))

    ref_lufs = _integrated_lufs(chunk_lufs)
    if not np.isfinite(ref_lufs):
        raise SilentAudioError("silent or sub-gate")

    relative = [x - ref_lufs for x in chunk_lufs]
    start_ramp = _scan_forward(relative)
    end_ramp = _scan_backward(relative)
    return AnalysisResult(
        sample_rate=int(sample_rate),
        duration_ms=int(channels.shape[1] / sample_rate * 1000),
        ref_lufs=float(ref_lufs),
        start_ramp=start_ramp,
        end_ramp=end_ramp,
        start_ramp_blob=encode_ramp(start_ramp),
        end_ramp_blob=encode_ramp(end_ramp),
    )


def analyze_file(path):
    audio, sample_rate = librosa.load(path, sr=None, mono=False)
    return analyze_buffer(audio, sample_rate)
