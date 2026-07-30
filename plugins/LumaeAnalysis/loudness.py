"""Bounded loudness and MixRamp analysis.

The public profile contract is intentionally unchanged from analyzer v1.  Audio
is decoded and filtered incrementally so peak memory depends on the decoder
block size, not on the duration or native sample rate of the media file.
"""

from dataclasses import dataclass
import time

import numpy as np
from scipy import signal as scipy_signal

from .ramp_codec import encode_ramp


THRESHOLDS_DB = [-90, -60, -40, -30, -24, -21, -18, -15, -12, -9, -6, -3, 0, 3, 6]
CHUNK_DURATION_MS = 100
LUFS_OFFSET_DB = -0.691
ABSOLUTE_GATE_LUFS = -70
MAX_CHUNK_INDEX = 0xFFFF

# A ramp stores a uint16 count of 100 ms windows. Refuse media beyond the
# representable range instead of silently producing a permanently clamped
# profile. Music outside this range remains playable without a waveform profile.
MAX_PROFILE_DURATION_SECONDS = (MAX_CHUNK_INDEX + 1) * CHUNK_DURATION_MS / 1000
MAX_PROFILE_SAMPLE_RATE = 384_000
MAX_PROFILE_CHANNELS = 8
MAX_FILTER_BLOCK_FRAMES = 131_072
DEFAULT_ANALYSIS_DEADLINE_SECONDS = 15 * 60

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


class ProfileResourceLimitError(RuntimeError):
    """The source is valid media but outside the bounded profile contract."""


class ProfileAnalysisTimeout(ProfileResourceLimitError):
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


def _as_channels(audio, expected_channels=None):
    arr = np.asarray(audio, dtype=np.float32)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    elif arr.ndim == 2 and expected_channels is not None:
        if arr.shape[0] == expected_channels:
            pass
        elif arr.shape[1] == expected_channels:
            arr = arr.T
        else:
            raise ValueError("decoded audio changed channel count")
    elif arr.ndim == 2 and arr.shape[0] > arr.shape[1]:
        arr = arr.T
    elif arr.ndim != 2:
        raise ValueError("audio must be mono or channel-first/channel-last PCM")
    return np.ascontiguousarray(arr, dtype=np.float32)


def _filter_coefficients(coefs):
    b0, b1, b2, a1, a2 = coefs
    return (
        np.asarray([b0, b1, b2], dtype=np.float64),
        np.asarray([1.0, a1, a2], dtype=np.float64),
    )


_STAGE1_B, _STAGE1_A = _filter_coefficients(KWEIGHT_STAGE1)
_STAGE2_B, _STAGE2_A = _filter_coefficients(KWEIGHT_STAGE2)


def _apply_biquad(channel, coefs):
    """Compatibility helper used by the analyzer's numerical golden tests."""
    numerator, denominator = _filter_coefficients(coefs)
    samples = np.asarray(channel, dtype=np.float64)
    return scipy_signal.lfilter(numerator, denominator, samples)


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
        for index, value in enumerate(relative_db):
            if value >= threshold:
                entries.append((threshold, min(index, MAX_CHUNK_INDEX)))
                break
    return entries


def _scan_backward(relative_db):
    entries = []
    count = len(relative_db)
    for threshold in THRESHOLDS_DB:
        for index in range(count - 1, -1, -1):
            if relative_db[index] >= threshold:
                entries.append((threshold, min(count - 1 - index, MAX_CHUNK_INDEX)))
                break
    return entries


def _check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise ProfileAnalysisTimeout("waveform analysis exceeded its per-track deadline")


def analyze_blocks(
    blocks,
    sample_rate,
    *,
    channel_count=None,
    deadline=None,
    max_duration_seconds=MAX_PROFILE_DURATION_SECONDS,
):
    """Analyze channel-first float PCM blocks with bounded working memory."""
    try:
        sample_rate = int(sample_rate)
    except (TypeError, ValueError) as exc:
        raise ProfileResourceLimitError("invalid audio sample rate") from exc
    if sample_rate <= 0 or sample_rate > MAX_PROFILE_SAMPLE_RATE:
        raise ProfileResourceLimitError(
            f"sample rate {sample_rate} exceeds the supported profile range"
        )

    chunk_size = max(1, int(sample_rate * CHUNK_DURATION_MS / 1000))
    max_frames = int(max_duration_seconds * sample_rate)
    channels = int(channel_count) if channel_count is not None else None
    if channels is not None and (channels <= 0 or channels > MAX_PROFILE_CHANNELS):
        raise ProfileResourceLimitError(
            f"channel count {channels} exceeds the supported profile range"
        )

    stage1_state = (
        np.zeros((channels, 2), dtype=np.float64) if channels is not None else None
    )
    stage2_state = (
        np.zeros((channels, 2), dtype=np.float64) if channels is not None else None
    )
    window = (
        np.empty((channels, chunk_size), dtype=np.float64)
        if channels is not None
        else None
    )
    window_fill = 0
    total_frames = 0
    chunk_lufs = []

    for raw_block in blocks:
        _check_deadline(deadline)
        block = _as_channels(raw_block, expected_channels=channels)
        if block.shape[1] == 0:
            continue
        if channels is None:
            channels = int(block.shape[0])
            if channels <= 0 or channels > MAX_PROFILE_CHANNELS:
                raise ProfileResourceLimitError(
                    f"channel count {channels} exceeds the supported profile range"
                )
            stage1_state = np.zeros((channels, 2), dtype=np.float64)
            stage2_state = np.zeros((channels, 2), dtype=np.float64)
            window = np.empty((channels, chunk_size), dtype=np.float64)
        elif int(block.shape[0]) != channels:
            raise ValueError("decoded audio changed channel count")

        for block_start in range(0, block.shape[1], MAX_FILTER_BLOCK_FRAMES):
            _check_deadline(deadline)
            pcm = block[:, block_start : block_start + MAX_FILTER_BLOCK_FRAMES]
            frame_count = int(pcm.shape[1])
            if total_frames + frame_count > max_frames:
                raise ProfileResourceLimitError(
                    "media duration exceeds the MixRamp profile limit"
                )

            weighted = np.empty((channels, frame_count), dtype=np.float64)
            for channel_index in range(channels):
                samples = pcm[channel_index].astype(np.float64, copy=False)
                stage1, stage1_state[channel_index] = scipy_signal.lfilter(
                    _STAGE1_B,
                    _STAGE1_A,
                    samples,
                    zi=stage1_state[channel_index],
                )
                weighted[channel_index], stage2_state[channel_index] = scipy_signal.lfilter(
                    _STAGE2_B,
                    _STAGE2_A,
                    stage1,
                    zi=stage2_state[channel_index],
                )

            offset = 0
            while offset < frame_count:
                take = min(chunk_size - window_fill, frame_count - offset)
                window[:, window_fill : window_fill + take] = weighted[
                    :, offset : offset + take
                ]
                window_fill += take
                offset += take
                if window_fill == chunk_size:
                    chunk_lufs.append(
                        _mean_square_to_lufs(float(np.mean(window * window)))
                    )
                    window_fill = 0
            total_frames += frame_count

    if channels is None or total_frames <= 0:
        raise SilentAudioError("silent or sub-gate")
    if window_fill:
        tail = window[:, :window_fill]
        chunk_lufs.append(_mean_square_to_lufs(float(np.mean(tail * tail))))

    ref_lufs = _integrated_lufs(chunk_lufs)
    if not np.isfinite(ref_lufs):
        raise SilentAudioError("silent or sub-gate")

    relative = [value - ref_lufs for value in chunk_lufs]
    start_ramp = _scan_forward(relative)
    end_ramp = _scan_backward(relative)
    return AnalysisResult(
        sample_rate=sample_rate,
        duration_ms=int(total_frames / sample_rate * 1000),
        ref_lufs=float(ref_lufs),
        start_ramp=start_ramp,
        end_ramp=end_ramp,
        start_ramp_blob=encode_ramp(start_ramp),
        end_ramp_blob=encode_ramp(end_ramp),
    )


def analyze_buffer(audio, sample_rate):
    """Analyze an existing PCM buffer through the same streaming state machine."""
    channels = _as_channels(audio)
    return analyze_blocks(
        (channels,),
        sample_rate,
        channel_count=int(channels.shape[0]),
    )


def _frame_blocks(resampler, frame):
    for converted in resampler.resample(frame):
        yield converted.to_ndarray()


def analyze_file(path, *, deadline_seconds=DEFAULT_ANALYSIS_DEADLINE_SECONDS):
    """Decode one media file incrementally with PyAV and bounded memory."""
    try:
        import av
    except ImportError as exc:  # pragma: no cover - AudioMuse images include PyAV.
        raise RuntimeError("AudioMuse's PyAV runtime is required for waveform analysis") from exc

    started = time.monotonic()
    deadline = started + max(1, int(deadline_seconds)) if deadline_seconds else None
    with av.open(path) as container:
        if not container.streams.audio:
            raise ValueError("media file does not contain an audio stream")
        stream = container.streams.audio[0]
        sample_rate = int(
            getattr(stream.codec_context, "sample_rate", 0)
            or getattr(stream, "rate", 0)
            or 0
        )
        layout = getattr(stream.codec_context, "layout", None) or getattr(
            stream, "layout", None
        )
        layout_name = getattr(layout, "name", None)
        channels = len(getattr(layout, "channels", ()) or ())
        if not layout_name or channels <= 0:
            raise ProfileResourceLimitError("audio stream has no supported channel layout")
        if channels > MAX_PROFILE_CHANNELS:
            raise ProfileResourceLimitError(
                f"channel count {channels} exceeds the supported profile range"
            )
        if sample_rate <= 0 or sample_rate > MAX_PROFILE_SAMPLE_RATE:
            raise ProfileResourceLimitError(
                f"sample rate {sample_rate} exceeds the supported profile range"
            )

        resampler = av.audio.resampler.AudioResampler(
            format="fltp",
            layout=layout_name,
            rate=sample_rate,
        )

        def decoded_blocks():
            for frame in container.decode(stream):
                _check_deadline(deadline)
                yield from _frame_blocks(resampler, frame)
            yield from _frame_blocks(resampler, None)

        return analyze_blocks(
            decoded_blocks(),
            sample_rate,
            channel_count=channels,
            deadline=deadline,
        )
