"""Whole-track loudness measured the industry's way (Lumae LN.4; contract §3.9, measurement version 1).

ITU-R BS.1770-4/5 integrated loudness, true peak, EBU Tech 3342 loudness range and the largest
short-term and momentary loudness, from one derivation over 100 ms bins of channel-weighted
K-filtered energy:

- ``bs1770-4-48k``, a 48 kHz series: inside the edge pass (``edge_profiles._stream``), from its
  canonical 48 kHz K-weighted series (mono and stereo; the edge pass refuses the rest).
  ``SeriesMeter`` receives each canonical block (sample peak, true peak 4x) and each 100 ms bin's
  mean power. And ``measure_file``, the standalone path, on any source above 48 kHz, converted to
  48 kHz first (``NativeMeter`` at 48 kHz, any channel count): ultrasonic content then doesn't
  count. DSD shapes its noise above 20 kHz, which read 4.7-5.7 LU loud at the decoded rate on two
  DSD256 files (Auralscape LN.1), DSD-derived PCM keeps that noise, and every Lumae player hears a
  hi-res source converted; hi-res PCM loses at most 0.03 LU (LN.1, 23 files).
- ``bs1770-4-native``: ``measure_file`` on a source at 48 kHz or below, at its own rate (files the
  edge pass refuses: more than two channels, unnamed WAV layouts, an unqualified runtime; and the
  backfill's tracks): K-weighting designed per sample rate from libebur128's analog prototype, true
  peak 4x.

Rules (contract §3.9): 400 ms blocks with 75 % overlap (four bins), the -70 LUFS absolute and
-10 LU relative gates; channel weights L/R/C 1.0, back, side and top-back 1.41, LFE 0 (as ffmpeg's
``ebur128``); mono measured as dual-mono. Loudness range: 3 s windows at 10 Hz, the -70 absolute
and -20 LU relative gates, p95 - p10. A value the measurement can't give (under 0.4 s, under 3 s,
digital silence) is ``None``, never a stand-in number.

A record also carries ``histogram``: the 0.1 dB counts of its gated 400 ms blocks and 3 s windows
(internal, never published), which ``pool_album`` sums to measure an album played whole without
keeping every block (within 0.03 LU of gating every block).
"""

import logging
import math
import time
import zlib

import numpy as np
from scipy.signal import firwin, sosfilt, upfirdn

from .loudness import ProfileAnalysisTimeout, ProfileResourceLimitError

logger = logging.getLogger(__name__)

VERSION = 1
METHOD_48K = "bs1770-4-48k"
METHOD_NATIVE = "bs1770-4-native"
METHODS = (METHOD_48K, METHOD_NATIVE)
# The analyzer's result rides on the edge payload under this key, outside its digest, and is
# popped before the edge is published (as edge_profiles.V3_ENDING_KEY).
LOUDNESS_KEY = "loudness_v1"
LUFS_OFFSET = -0.691
ABSOLUTE_GATE = -70.0
RELATIVE_GATE = -10.0
LRA_RELATIVE_GATE = -20.0
# The clients' range for a true peak (contract §3.9 rule 1): a lower one is published as null.
TRUE_PEAK_FLOOR = -60.0
BLOCK_BINS = 4
SHORT_TERM_BINS = 30
MAX_CHANNELS = 8
# DSF decodes at a 1/8 of its DSD rate: DSD512 is 2,822,400 Hz.
MAX_SAMPLE_RATE = 2_822_400
# The standalone path meters a source above this rate through a conversion to it.
CONVERTED_RATE = 48_000
MAX_SECONDS = 6553.6
DEFAULT_DEADLINE_SECONDS = 900
# The 0.1 dB histogram of libebur128: bin i covers [-70 + i/10, -70 + (i+1)/10) LUFS.
HISTOGRAM_BINS = 1000
HISTOGRAM_FORMAT = 1
# Rounding of published values (ffmpeg's precision).
DECIMALS = 3
# ffmpeg's ebur128 weights (libavfilter/f_ebur128.c BACK_MASK): back, side, top-back and
# surround-direct channels 1.41, LFE 0, every other named or unnamed channel 1.
_SURROUND = frozenset({"BL", "BC", "BR", "SL", "SR", "TBL", "TBC", "TBR", "SDL", "SDR"})
_LFE = frozenset({"LFE", "LFE2"})


def kweighting_sos(sample_rate):
    """The two K-weighting biquads at ``sample_rate`` (libebur128's ebur128_init_filter)."""
    f0, gain_db, q = 1681.974450955533, 3.999843853973347, 0.7071752369554196
    k = math.tan(math.pi * f0 / sample_rate)
    vh = 10.0 ** (gain_db / 20.0)
    vb = vh ** 0.4996667741545416
    a0 = 1.0 + k / q + k * k
    shelf = [(vh + vb * k / q + k * k) / a0, 2.0 * (k * k - vh) / a0, (vh - vb * k / q + k * k) / a0,
             1.0, 2.0 * (k * k - 1.0) / a0, (1.0 - k / q + k * k) / a0]
    f0, q = 38.13547087602444, 0.5003270373238773
    k = math.tan(math.pi * f0 / sample_rate)
    a0 = 1.0 + k / q + k * k
    high_pass = [1.0, -2.0, 1.0, 1.0, 2.0 * (k * k - 1.0) / a0, (1.0 - k / q + k * k) / a0]
    return np.array([shelf, high_pass], dtype=np.float64)


def channel_weights(names):
    """BS.1770-4 weights for a layout's channel names (PyAV ``layout.channels[i].name``)."""
    names = [str(name or "") for name in names]
    if len(names) == 1:
        return np.array([2.0])  # mono plays on two speakers: measured as dual-mono
    return np.array([0.0 if name in _LFE else 1.41 if name in _SURROUND else 1.0 for name in names])


def true_peak_factor(sample_rate):
    return 4 if sample_rate < 96_000 else 2


def lufs(energy):
    with np.errstate(divide="ignore"):
        return LUFS_OFFSET + 10.0 * np.log10(energy)


class _TruePeak:
    """Streaming polyphase oversampling (Kaiser-windowed sinc), continuous across blocks."""

    def __init__(self, factor, channels, half_taps_per_phase=16):
        self.factor = factor
        taps = 2 * half_taps_per_phase * factor + 1
        self.filter = firwin(taps, 1.0 / factor, window=("kaiser", 7.0)) * factor
        self.history_frames = int(math.ceil(len(self.filter) / factor)) + 1
        self.history = np.zeros((channels, self.history_frames))
        self.peak = 0.0

    def add(self, pcm):
        extended = np.concatenate([self.history, pcm], axis=1)
        start, stop = self.history_frames * self.factor, (self.history_frames + pcm.shape[1]) * self.factor
        for channel in extended:
            upsampled = upfirdn(self.filter, channel, up=self.factor)[start:stop]
            if upsampled.size:
                self.peak = max(self.peak, float(np.max(np.abs(upsampled))))
        self.history = extended[:, -self.history_frames:]

    def finish(self):
        self.add(np.zeros((self.history.shape[0], self.history_frames)))
        return self.peak


def _windows(bins, width):
    """Mean of every run of ``width`` consecutive bins (100 ms hop); empty when too short."""
    if len(bins) < width:
        return np.zeros(0)
    total = np.concatenate([[0.0], np.cumsum(bins)])
    return (total[width:] - total[:-width]) / width


def _histogram(energies):
    loudness = lufs(energies)
    kept = loudness[loudness >= ABSOLUTE_GATE]
    index = np.clip(np.floor((kept - ABSOLUTE_GATE) * 10.0).astype(np.int64), 0, HISTOGRAM_BINS - 1)
    return np.bincount(index, minlength=HISTOGRAM_BINS).astype(np.uint32)


def encode_histogram(blocks, windows):
    """``histogram``: format byte, then zlib of the block and window counts (uint32 LE)."""
    payload = np.concatenate([blocks, windows]).astype("<u4").tobytes()
    return bytes([HISTOGRAM_FORMAT]) + zlib.compress(payload, 9)


def decode_histogram(blob):
    if not blob or blob[0] != HISTOGRAM_FORMAT:
        raise ValueError("unknown loudness histogram format")
    counts = np.frombuffer(zlib.decompress(bytes(blob[1:])), dtype="<u4").astype(np.int64)
    if counts.size != 2 * HISTOGRAM_BINS:
        raise ValueError("malformed loudness histogram")
    return counts[:HISTOGRAM_BINS], counts[HISTOGRAM_BINS:]


def _rounded(value):
    if value is None:
        return None
    value = float(value)
    return round(value, DECIMALS) if math.isfinite(value) else None


def _gated_mean(energies, relative_gate):
    """The energies over both gates (absolute -70, then ``relative_gate`` under the mean)."""
    loudness = lufs(energies)
    above = energies[loudness >= ABSOLUTE_GATE]
    if above.size == 0:
        return None
    gate = float(lufs(np.mean(above))) + relative_gate
    return energies[(loudness >= ABSOLUTE_GATE) & (loudness >= gate)]


def _loudness_range(windows):
    gated = _gated_mean(windows, LRA_RELATIVE_GATE) if windows.size else None
    if gated is None or gated.size == 0:
        return None
    values = np.sort(lufs(gated))
    count = values.size
    return float(values[int((count - 1) * 0.95 + 0.5)] - values[int((count - 1) * 0.10 + 0.5)])


def _loudest(energies):
    """The largest loudness of ``energies``; under the absolute gate it is no loudness (and outside
    the clients' range, §3.9 rule 1)."""
    top = float(np.max(lufs(energies)))
    return top if top >= ABSOLUTE_GATE else None


def derive(bins, sample_peak, true_peak, *, method, channels, sample_rate):
    """The contract's track record (and its internal histogram) from 100 ms bin energies.

    ``bins`` are channel-weighted energies (the sum over channels of weight x mean square).
    """
    bins = np.asarray(bins, dtype=np.float64)
    blocks = _windows(bins, BLOCK_BINS)
    windows = _windows(bins, SHORT_TERM_BINS)
    gated = _gated_mean(blocks, RELATIVE_GATE) if blocks.size else None
    integrated = float(lufs(np.mean(gated))) if gated is not None and gated.size else None
    silent = not (sample_peak > 0)
    max_momentary = _loudest(blocks) if blocks.size and not silent else None
    max_short_term = _loudest(windows) if windows.size and not silent else None
    db20 = lambda value: 20.0 * math.log10(value) if value > 0 else None  # noqa: E731
    # A true peak under the clients' floor is published as null, like digital silence: never a
    # value every client skips the record for.
    peak_db = db20(true_peak)
    if peak_db is not None and peak_db < TRUE_PEAK_FLOOR:
        peak_db = None
    return {
        "version": VERSION,
        "method": method,
        "integrated_lufs": _rounded(integrated),
        "true_peak_dbtp": _rounded(peak_db),
        "sample_peak_dbfs": _rounded(db20(sample_peak)),
        "lra_lu": _rounded(_loudness_range(windows)),
        "max_short_term_lufs": _rounded(max_short_term),
        "max_momentary_lufs": _rounded(max_momentary),
        "channels": int(channels),
        "sample_rate": int(sample_rate),
        "histogram": encode_histogram(_histogram(blocks), _histogram(windows)),
    }


class SeriesMeter:
    """``bs1770-4-48k``: fed by the edge pass with its canonical 48 kHz PCM and 100 ms mean powers.

    The edge pass's power is the K-weighted mean over its one or two channels; the BS.1770 sum is
    twice that for stereo (L + R) and for mono as dual-mono (2 x mono).

    It never fails the edge pass: an error stops the measurement, ``result`` is then None, and the
    backfill measures the track on the standalone path.
    """

    def __init__(self, canonical_rate=48_000):
        self.canonical_rate = canonical_rate
        self.bins = []
        self.sample_peak = 0.0
        self._true_peak = None
        self.failed = None

    def canonical(self, pcm):
        if self.failed is not None:
            return
        try:
            pcm = np.asarray(pcm, dtype=np.float64)
            if pcm.size == 0:
                return
            if self._true_peak is None:
                self._true_peak = _TruePeak(true_peak_factor(self.canonical_rate), pcm.shape[0])
            self.sample_peak = max(self.sample_peak, float(np.max(np.abs(pcm))))
            self._true_peak.add(pcm)
        except Exception as exc:  # noqa: BLE001 - loudness never fails the edge pass
            self.failed = exc

    def bin(self, mean_power):
        if self.failed is not None:
            return
        try:
            self.bins.append(2.0 * float(mean_power))
        except Exception as exc:  # noqa: BLE001
            self.failed = exc

    def result(self, *, channels, sample_rate):
        """The record, or None when the measurement failed."""
        if self.failed is None:
            try:
                true_peak = self._true_peak.finish() if self._true_peak is not None else 0.0
                return derive(self.bins, self.sample_peak, true_peak, method=METHOD_48K, channels=channels,
                              sample_rate=sample_rate)
            except Exception as exc:  # noqa: BLE001
                self.failed = exc
        logger.warning("lumae_analysis loudness in the edge pass failed; the backfill measures the track",
                       exc_info=self.failed)
        return None


class NativeMeter:
    """Channel-first float blocks at one rate: the source's own (``bs1770-4-native``) or the
    standalone path's 48 kHz conversion (``bs1770-4-48k``). True peak 4x below 96 kHz, 2x from it."""

    def __init__(self, sample_rate, weights):
        self.sample_rate = int(sample_rate)
        self.weights = np.asarray(weights, dtype=np.float64)
        channels = self.weights.size
        self.sos = kweighting_sos(self.sample_rate)
        self.state = np.zeros((self.sos.shape[0], channels, 2))
        self.bin_frames = (self.sample_rate + 5) // 10  # libebur128's samples in 100 ms
        self.partial = 0.0
        self.filled = 0
        self.bins = []
        self.frames = 0
        self.sample_peak = 0.0
        self._true_peak = _TruePeak(true_peak_factor(self.sample_rate), channels)

    def add(self, pcm):
        pcm = np.asarray(pcm, dtype=np.float64)
        if pcm.ndim != 2 or pcm.shape[0] != self.weights.size:
            raise ValueError("decoded audio changed its channel count")
        count = pcm.shape[1]
        if count == 0:
            return
        if not np.all(np.isfinite(pcm)):
            raise ValueError("non-finite decoded audio")
        self.frames += count
        self.sample_peak = max(self.sample_peak, float(np.max(np.abs(pcm))))
        self._true_peak.add(pcm)
        weighted, self.state = sosfilt(self.sos, pcm, axis=1, zi=self.state)
        energy = np.einsum("c,cn->n", self.weights, weighted * weighted)
        offset = 0
        if self.filled:
            take = min(self.bin_frames - self.filled, count)
            self.partial += float(np.sum(energy[:take]))
            self.filled += take
            offset = take
            if self.filled == self.bin_frames:
                self.bins.append(self.partial / self.bin_frames)
                self.partial, self.filled = 0.0, 0
        whole = (count - offset) // self.bin_frames
        if whole:
            run = energy[offset:offset + whole * self.bin_frames].reshape(whole, self.bin_frames)
            self.bins.extend((np.sum(run, axis=1) / self.bin_frames).tolist())
            offset += whole * self.bin_frames
        if offset < count:
            self.partial += float(np.sum(energy[offset:]))
            self.filled += count - offset

    def result(self, *, method=METHOD_NATIVE, sample_rate=None):
        return derive(self.bins, self.sample_peak, self._true_peak.finish(), method=method,
                      channels=self.weights.size, sample_rate=sample_rate or self.sample_rate)


def measured_rate(sample_rate):
    """The rate ``measure_file`` meters at: the source's own up to 48 kHz, 48 kHz above it."""
    return CONVERTED_RATE if int(sample_rate) > CONVERTED_RATE else int(sample_rate)


def _check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise ProfileAnalysisTimeout("loudness measurement exceeded its per-track deadline")


def measure_file(path, *, deadline_seconds=DEFAULT_DEADLINE_SECONDS, observer=None):
    """``bs1770-4-native`` for one media file: PyAV, its own rate and layout, bounded memory; a DSD
    source above 48 kHz (DSD among them) is converted to 48 kHz first (``bs1770-4-48k``, ``measured_rate``).

    ``observer`` (``analysis_isolation.DecodeProbe``) records the stream and the decode position
    for failure diagnostics, as ``loudness.analyze_file``.
    """
    try:
        import av
    except ImportError as exc:  # pragma: no cover - AudioMuse images include PyAV.
        raise RuntimeError("AudioMuse's PyAV runtime is required for loudness measurement") from exc
    from .loudness import _decode_sample_format, _frame_blocks

    deadline = time.monotonic() + max(1, int(deadline_seconds)) if deadline_seconds else None
    with av.open(path) as container:
        if not container.streams.audio:
            raise ValueError("media file does not contain an audio stream")
        stream = container.streams.audio[0]
        if observer is not None:
            observer.opened(container, stream)
        sample_rate = int(getattr(stream.codec_context, "sample_rate", 0) or getattr(stream, "rate", 0) or 0)
        layout = getattr(stream.codec_context, "layout", None) or getattr(stream, "layout", None)
        layout_name = getattr(layout, "name", None)
        names = [getattr(channel, "name", "") for channel in (getattr(layout, "channels", ()) or ())]
        if not layout_name or not names:
            raise ProfileResourceLimitError("audio stream has no supported channel layout")
        if len(names) > MAX_CHANNELS:
            raise ProfileResourceLimitError(f"channel count {len(names)} exceeds the loudness range")
        if sample_rate <= 0 or sample_rate > MAX_SAMPLE_RATE:
            raise ProfileResourceLimitError(f"sample rate {sample_rate} exceeds the loudness range")
        rate = measured_rate(sample_rate)
        meter = NativeMeter(rate, channel_weights(names))
        resampler = av.audio.resampler.AudioResampler(
            format=_decode_sample_format(len(names)), layout=layout_name, rate=rate)
        limit = int(MAX_SECONDS * rate)
        for frame in container.decode(stream):
            _check_deadline(deadline)
            if observer is not None:
                observer.decoded(frame)
            for block in _frame_blocks(resampler, frame, len(names)):
                meter.add(block)
            if meter.frames > limit:
                raise ProfileResourceLimitError("loudness duration limit exceeded")
        for block in _frame_blocks(resampler, None, len(names)):
            meter.add(block)
        _check_deadline(deadline)
    if rate != sample_rate:
        return meter.result(method=METHOD_48K, sample_rate=sample_rate)
    return meter.result()


def analyze_edge_file(path, *, catalog_instance_id, track_id, media_revision, deadline_seconds=900,
                      observer=None):
    """``edge_profiles.analyze_edge_file`` with ``bs1770-4-48k`` measured in the same decode.

    The loudness record rides beside the edge payload under ``LOUDNESS_KEY``, outside its digest;
    the payload itself is byte-identical to the edge pass without it.
    """
    from . import edge_profiles

    meter = SeriesMeter(edge_profiles.CANONICAL_RATE)
    payload = edge_profiles.analyze_edge_file(
        path, catalog_instance_id=catalog_instance_id, track_id=track_id, media_revision=media_revision,
        deadline_seconds=deadline_seconds, observer=observer, loudness=meter)
    source = payload["source"]
    record = meter.result(channels=1 if source["channel_layout"] == "mono" else 2, sample_rate=source["sample_rate"])
    if record is not None:
        payload[LOUDNESS_KEY] = record
    return payload


def _histogram_energy():
    centres = ABSOLUTE_GATE + (np.arange(HISTOGRAM_BINS) + 0.5) / 10.0
    return 10.0 ** ((centres - LUFS_OFFSET) / 10.0)


def _histogram_gate(counts, relative_gate):
    """The first bin at or over the relative gate, from bin-centre energies (libebur128)."""
    total = int(counts.sum())
    if total == 0:
        return None
    gate = float(lufs(float(np.dot(counts, _histogram_energy())) / total)) + relative_gate
    if gate < ABSOLUTE_GATE:
        return 0
    start = min(HISTOGRAM_BINS - 1, int((gate - ABSOLUTE_GATE) * 10.0))
    # libebur128: the gate's own bin counts only when the gate is at or under its centre.
    if gate > ABSOLUTE_GATE + (start + 0.5) / 10.0:
        start += 1
    return min(start, HISTOGRAM_BINS - 1)


def pool_album(members):
    """The contract's album values from its members' track records (each with ``histogram``).

    Integrated loudness and loudness range of the album played whole: the members' gated block and
    window counts summed and gated once (a window never spans two tracks). ``None`` integrated
    loudness means no album record.
    """
    blocks = np.zeros(HISTOGRAM_BINS, dtype=np.int64)
    windows = np.zeros(HISTOGRAM_BINS, dtype=np.int64)
    peaks, loudest = [], []
    # A member without a true peak is digitally silent or under -60 dBTP: it never holds the maximum.
    for member in members:
        member_blocks, member_windows = decode_histogram(member["histogram"])
        blocks += member_blocks
        windows += member_windows
        if member.get("integrated_lufs") is not None:
            loudest.append(float(member["integrated_lufs"]))
        if member.get("true_peak_dbtp") is not None:
            peaks.append(member["true_peak_dbtp"])
    integrated = None
    start = _histogram_gate(blocks, RELATIVE_GATE)
    if start is not None and blocks[start:].sum():
        selected = blocks[start:]
        integrated = float(lufs(float(np.dot(selected, _histogram_energy()[start:])) / selected.sum()))
    lra = None
    start = _histogram_gate(windows, LRA_RELATIVE_GATE)
    if start is not None and windows[start:].sum():
        cumulative = np.cumsum(windows[start:])
        count = int(cumulative[-1])
        centre = lambda rank: ABSOLUTE_GATE + (start + int(np.searchsorted(cumulative, rank + 1)) + 0.5) / 10.0  # noqa: E731
        lra = centre(int((count - 1) * 0.95 + 0.5)) - centre(int((count - 1) * 0.10 + 0.5))
    true_peak = max(peaks) if peaks else None
    return {
        "integrated_lufs": _rounded(integrated),
        "true_peak_dbtp": _rounded(true_peak),
        "loudest_track_lufs": _rounded(max(loudest)) if loudest else None,
        "lra_lu": _rounded(lra),
        "members": len(members),
        "exact": True,
    }


def track_record_fields(record):
    """A stored measurement as the contract's track item fields (``histogram`` stays internal)."""
    return {key: value for key, value in record.items() if key != "histogram"}
