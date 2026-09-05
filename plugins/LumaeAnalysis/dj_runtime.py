"""Optional CPU model adapters. Imported cheaply; model packages load only in workers."""

import hashlib
import importlib.metadata
import inspect
import math
import os
import stat
import tempfile
import time
from pathlib import Path
from dataclasses import dataclass
from types import MappingProxyType
from collections.abc import Mapping
import numpy as np
from .dj_control import CancellationPoller
from .dj_contract import (
    SCHEMA_VERSION,
    METHOD,
    MODEL_NAME,
    MODEL_BYTES,
    MODEL_SHA256,
    YAMNET_MODEL_NAME,
    YAMNET_MODEL_BYTES,
    YAMNET_MODEL_SHA256,
    YAMNET_SAMPLE_RATE,
    YAMNET_WINDOW_SAMPLES,
    YAMNET_HOP_SAMPLES,
    YAMNET_CLASS_COUNT,
    YAMNET_VOCAL_CLASSES,
    MODEL_SAMPLE_RATE,
    MODEL_N_FFT,
    MODEL_HOP_SAMPLES,
    MODEL_MEL_BINS,
    MODEL_WINDOW_FRAMES,
    MODEL_BORDER_FRAMES,
    MODEL_STEP_FRAMES,
    MAX_SOURCE_SECONDS,
    JOB_DEADLINE_SECONDS,
    RSS_CAP_BYTES,
    PINNED_PACKAGES,
    PINNED_PACKAGE_VARIANTS,
    DjAnalysisError,
)


def _check_deadline(deadline, cancelled=None):
    if cancelled and cancelled():
        raise DjAnalysisError("cancelled")
    if deadline is not None and time.monotonic() >= deadline:
        raise DjAnalysisError("deadline_exceeded")


def _sha256_file(path, deadline=None, cancelled=None):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        while True:
            _check_deadline(deadline, cancelled)
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify_model_artifact(
    model_path,
    *,
    expected_bytes=MODEL_BYTES,
    expected_sha256=MODEL_SHA256,
    deadline=None,
):
    raw = str(model_path or "").strip()
    if not raw or raw.lower().startswith(("http://", "https://")):
        raise DjAnalysisError("model_not_local")
    path = Path(raw).expanduser()
    try:
        resolved = path.resolve(strict=True)
        info = resolved.stat()
    except (OSError, RuntimeError) as exc:
        raise DjAnalysisError("model_missing") from exc
    if not stat.S_ISREG(info.st_mode):
        raise DjAnalysisError("model_not_regular_file")
    if info.st_size != expected_bytes:
        raise DjAnalysisError("model_size_mismatch")
    if _sha256_file(resolved, deadline=deadline) != expected_sha256:
        raise DjAnalysisError("model_checksum_mismatch")
    return {
        "path": resolved,
        "device": info.st_dev,
        "inode": info.st_ino,
        "bytes": info.st_size,
        "sha256": expected_sha256,
        "mtime_ns": info.st_mtime_ns,
    }


def verify_yamnet_model_artifact(model_path, *, deadline=None):
    return verify_model_artifact(
        model_path,
        expected_bytes=YAMNET_MODEL_BYTES,
        expected_sha256=YAMNET_MODEL_SHA256,
        deadline=deadline,
    )


def runtime_status(
    model_path,
    yamnet_model_path=None,
    *,
    package_version=None,
    verify_model=True,
):
    package_version = package_version or importlib.metadata.version
    mismatches = []
    for name, expected in PINNED_PACKAGES.items():
        try:
            actual = package_version(name)
        except importlib.metadata.PackageNotFoundError:
            mismatches.append({"package": name, "reason": "missing"})
            continue
        accepted = PINNED_PACKAGE_VARIANTS.get(name, (expected,))
        if actual not in accepted:
            mismatches.append(
                {
                    "package": name,
                    "reason": "version",
                    "expected": expected,
                    "actual": actual,
                }
            )
    model = None
    yamnet_model = None
    model_error = None
    if verify_model:
        try:
            model = verify_model_artifact(model_path)
        except DjAnalysisError as exc:
            model_error = exc.code
        if model_error is None:
            try:
                yamnet_model = verify_yamnet_model_artifact(yamnet_model_path)
            except DjAnalysisError as exc:
                model_error = f"yamnet_{exc.code}"
    available = not mismatches and model_error is None
    return {
        "schema_version": SCHEMA_VERSION,
        "method": METHOD,
        "available": available,
        "reason": None if available else model_error or "runtime_dependency_mismatch",
        "runtime_mismatches": mismatches,
        "models": {
            "beat_this": {
                "name": MODEL_NAME,
                "sha256": MODEL_SHA256,
                "bytes": MODEL_BYTES,
                "verified": model is not None,
            },
            "yamnet": {
                "name": YAMNET_MODEL_NAME,
                "sha256": YAMNET_MODEL_SHA256,
                "bytes": YAMNET_MODEL_BYTES,
                "verified": yamnet_model is not None,
                "io_type": "float32",
                "weight_quantization": "dynamic-range",
                "scores_calibrated": False,
            },
        },
        "supported_analysis_versions": [SCHEMA_VERSION],
        "supported_plan_versions": [2],
        "limits": {
            "max_concurrent_jobs": 1,
            "rss_bytes": RSS_CAP_BYTES,
            "deadline_seconds": JOB_DEADLINE_SECONDS,
            "max_source_seconds": MAX_SOURCE_SECONDS,
        },
        "reference_host_qualified": False,
    }


def _rss_bytes():
    import psutil

    return int(psutil.Process().memory_info().rss)


def _beat_this_window_starts(frame_count):
    """Reproduce Beat This 1.1.0 split_piece(..., avoid_short_end=True)."""
    if (
        isinstance(frame_count, bool)
        or not isinstance(frame_count, int)
        or frame_count <= 0
    ):
        raise DjAnalysisError("empty_audio")
    starts = list(
        range(
            -MODEL_BORDER_FRAMES,
            frame_count - MODEL_BORDER_FRAMES,
            MODEL_STEP_FRAMES,
        )
    )
    if frame_count > MODEL_STEP_FRAMES:
        starts[-1] = frame_count - (MODEL_WINDOW_FRAMES - MODEL_BORDER_FRAMES)
    return starts


def _reflect_sample_indices(start, end, sample_count):
    """Map centered-STFT padding exactly like torch's one-dimensional reflect pad."""
    if sample_count <= MODEL_N_FFT // 2:
        raise DjAnalysisError("source_too_short")
    positions = np.arange(int(start), int(end), dtype=np.int64)
    period = 2 * (sample_count - 1)
    folded = positions % period
    return np.where(folded < sample_count, folded, period - folded)


class BeatThisWindowAdapter:
    """Disk-spooled CPU adapter matching upstream's exact 1500/6 geometry."""

    def __init__(
        self,
        model_path,
        *,
        rss_reader=_rss_bytes,
        deadline=None,
        cancelled=None,
    ):
        self.artifact = verify_model_artifact(model_path, deadline=deadline)
        status = runtime_status(model_path, verify_model=False)
        if status["runtime_mismatches"]:
            raise DjAnalysisError("runtime_dependency_mismatch")
        import torch
        import torchaudio
        from beat_this.model.beat_tracker import BeatThis
        from beat_this.utils import replace_state_dict_key

        self.torch = torch
        self.rss_reader = rss_reader
        if getattr(torch.version, "cuda", None) or getattr(torch.version, "hip", None):
            raise DjAnalysisError("runtime_accelerator_build_unsupported")
        _check_deadline(deadline, cancelled)
        # Open the already-verified local artifact and load from that file
        # object. Unlike upstream load_model(), this code has no short-name or
        # URL fallback if the path is replaced between verification and use.
        with open(self.artifact["path"], "rb") as checkpoint_file:
            opened = os.fstat(checkpoint_file.fileno())
            if (
                opened.st_size != self.artifact["bytes"]
                or opened.st_mtime_ns != self.artifact["mtime_ns"]
            ):
                raise DjAnalysisError("model_changed")
            checkpoint_digest = hashlib.sha256()
            while True:
                _check_deadline(deadline, cancelled)
                chunk = checkpoint_file.read(1024 * 1024)
                if not chunk:
                    break
                checkpoint_digest.update(chunk)
            if checkpoint_digest.hexdigest() != MODEL_SHA256:
                raise DjAnalysisError("model_checksum_mismatch")
            checkpoint_file.seek(0)
            checkpoint = torch.load(
                checkpoint_file,
                map_location="cpu",
                weights_only=True,
            )
        _check_deadline(deadline, cancelled)
        hyperparameters = {
            key: value
            for key, value in checkpoint["hyper_parameters"].items()
            if key in set(inspect.signature(BeatThis).parameters)
        }
        self.model = BeatThis(**hyperparameters)
        state = replace_state_dict_key(checkpoint["state_dict"], "model.", "")
        self.model.load_state_dict(state)
        self.model = self.model.to("cpu").eval()
        self.mel = torchaudio.transforms.MelSpectrogram(
            sample_rate=MODEL_SAMPLE_RATE,
            n_fft=MODEL_N_FFT,
            hop_length=MODEL_HOP_SAMPLES,
            f_min=30,
            f_max=11_000,
            n_mels=MODEL_MEL_BINS,
            mel_scale="slaney",
            normalized="frame_length",
            power=1,
            center=False,
        ).to("cpu")
        if self.rss_reader() > RSS_CAP_BYTES:
            raise DjAnalysisError("rss_cap_exceeded")

    def _spect_frames(self, pcm, frame_start, frame_end):
        if frame_end <= frame_start:
            return np.empty((0, MODEL_MEL_BINS), dtype=np.float32)
        first_sample = frame_start * MODEL_HOP_SAMPLES - MODEL_N_FFT // 2
        last_sample = (frame_end - 1) * MODEL_HOP_SAMPLES + MODEL_N_FFT // 2
        indices = _reflect_sample_indices(first_sample, last_sample, len(pcm))
        samples = np.asarray(pcm[indices], dtype=np.float32)
        torch = self.torch
        with torch.inference_mode():
            waveform = torch.from_numpy(samples)
            spect = torch.log1p(1000 * self.mel(waveform).T)
        expected = frame_end - frame_start
        if len(spect) != expected:
            raise DjAnalysisError("model_timeline_mismatch")
        return spect.float().cpu().numpy()

    def _window(self, log_mel):
        torch = self.torch
        with torch.inference_mode():
            spect = torch.as_tensor(log_mel, dtype=torch.float32)
            prediction = self.model(spect.unsqueeze(0))
            beat = prediction["beat"][0].float().cpu().numpy()
            downbeat = prediction["downbeat"][0].float().cpu().numpy()
        if len(beat) != len(log_mel) or len(downbeat) != len(log_mel):
            raise DjAnalysisError("invalid_model_output")
        energy = np.mean(log_mel, axis=1, dtype=np.float64)
        spectral_flux = np.zeros(len(log_mel), dtype=np.float64)
        if len(log_mel) > 1:
            spectral_flux[1:] = np.mean(
                np.maximum(log_mel[1:] - log_mel[:-1], 0),
                axis=1,
                dtype=np.float64,
            )
        low_energy = np.mean(log_mel[:, :32], axis=1, dtype=np.float64)
        mid_energy = np.mean(log_mel[:, 32:80], axis=1, dtype=np.float64)
        high_energy = np.mean(log_mel[:, 80:], axis=1, dtype=np.float64)
        return (
            beat,
            downbeat,
            energy,
            spectral_flux,
            low_energy,
            mid_energy,
            high_energy,
        )

    def analyze(self, path, *, deadline, cancelled=None, progress=None):
        import av

        source_rate = None
        source_frames = 0
        resampled_frames = 0
        with tempfile.TemporaryFile() as pcm_file:
            with av.open(str(path)) as container:
                streams = [
                    stream for stream in container.streams if stream.type == "audio"
                ]
                if len(streams) != 1:
                    raise DjAnalysisError("unsupported_audio_streams")
                stream = streams[0]
                source_codec = stream.codec_context.name
                resampler = av.AudioResampler(
                    format="fltp", layout="mono", rate=MODEL_SAMPLE_RATE
                )
                for frame in container.decode(stream):
                    _check_deadline(deadline, cancelled)
                    rate = int(
                        frame.sample_rate or stream.codec_context.sample_rate or 0
                    )
                    if rate <= 0 or (source_rate is not None and source_rate != rate):
                        raise DjAnalysisError("source_timeline_changed")
                    source_rate = rate
                    source_frames += int(frame.samples)
                    for converted in resampler.resample(frame):
                        block = (
                            converted.to_ndarray()
                            .astype(np.float32, copy=False)
                            .reshape(-1)
                        )
                        if not np.all(np.isfinite(block)):
                            raise DjAnalysisError("non_finite_audio")
                        pcm_file.write(block.astype("<f4", copy=False).tobytes())
                        resampled_frames += len(block)
                        if resampled_frames > MAX_SOURCE_SECONDS * MODEL_SAMPLE_RATE:
                            raise DjAnalysisError("source_duration_unsupported")
                        if self.rss_reader() > RSS_CAP_BYTES:
                            raise DjAnalysisError("rss_cap_exceeded")
                for converted in resampler.resample(None):
                    block = (
                        converted.to_ndarray()
                        .astype(np.float32, copy=False)
                        .reshape(-1)
                    )
                    if not np.all(np.isfinite(block)):
                        raise DjAnalysisError("non_finite_audio")
                    pcm_file.write(block.astype("<f4", copy=False).tobytes())
                    resampled_frames += len(block)
            if source_rate is None or resampled_frames == 0:
                raise DjAnalysisError("empty_audio")
            if resampled_frames > MAX_SOURCE_SECONDS * MODEL_SAMPLE_RATE:
                raise DjAnalysisError("source_duration_unsupported")
            if resampled_frames <= MODEL_N_FFT // 2:
                raise DjAnalysisError("source_too_short")
            pcm_file.flush()
            frame_count = resampled_frames // MODEL_HOP_SAMPLES + 1
            outputs = {
                name: np.full(frame_count, -1000.0, dtype=np.float64)
                for name in ("beat_logits", "downbeat_logits")
            }
            for name in (
                "energy",
                "spectral_flux",
                "low_energy",
                "mid_energy",
                "high_energy",
            ):
                outputs[name] = np.zeros(frame_count, dtype=np.float64)
            owned = np.zeros(frame_count, dtype=np.bool_)
            pcm = np.memmap(
                pcm_file,
                dtype="<f4",
                mode="r",
                shape=(resampled_frames,),
            )
            try:
                for start in _beat_this_window_starts(frame_count):
                    _check_deadline(deadline, cancelled)
                    actual_start = max(0, start)
                    actual_end = min(frame_count, start + MODEL_WINDOW_FRAMES)
                    log_mel = self._spect_frames(pcm, actual_start, actual_end)
                    left = max(0, -start)
                    right = max(
                        0,
                        min(
                            MODEL_BORDER_FRAMES,
                            start + MODEL_WINDOW_FRAMES - frame_count,
                        ),
                    )
                    if left or right:
                        log_mel = np.pad(log_mel, ((left, right), (0, 0)))
                    values = self._window(log_mel)
                    target_start = max(0, start + MODEL_BORDER_FRAMES)
                    target_end = min(
                        frame_count,
                        start + MODEL_WINDOW_FRAMES - MODEL_BORDER_FRAMES,
                    )
                    source_start = target_start - start
                    source_end = target_end - start
                    available = ~owned[target_start:target_end]
                    for name, value in zip(outputs, values):
                        segment = np.asarray(
                            value[source_start:source_end], dtype=np.float64
                        )
                        outputs[name][target_start:target_end][available] = segment[
                            available
                        ]
                    owned[target_start:target_end] |= available
                    if progress:
                        progress(int(np.count_nonzero(owned)))
                    if self.rss_reader() > RSS_CAP_BYTES:
                        raise DjAnalysisError("rss_cap_exceeded")
            finally:
                del pcm
        if not np.all(owned):
            raise DjAnalysisError("model_timeline_mismatch")
        outputs["spectral_flux"][0] = 0.0
        result = outputs
        result["source_sample_rate"] = source_rate
        result["source_decoded_frames"] = source_frames
        result["source_resampled_frames"] = resampled_frames
        result["source_codec"] = source_codec
        # Frame counts alone do not prove the player's seek/padding contract.
        result["timeline_verified"] = False
        return result


class YamnetLiteAdapter:
    """Pinned CPU LiteRT adapter returning time-resolved uncalibrated evidence."""

    def __init__(
        self, model_path, *, rss_reader=_rss_bytes, deadline=None, cancelled=None
    ):
        self.artifact = verify_yamnet_model_artifact(model_path, deadline=deadline)
        self.rss_reader = rss_reader
        _check_deadline(deadline, cancelled)
        from ai_edge_litert.interpreter import Interpreter

        self.interpreter = Interpreter(
            model_path=str(self.artifact["path"]), num_threads=1
        )
        self.interpreter.allocate_tensors()
        opened = self.artifact["path"].stat()
        if (
            opened.st_dev != self.artifact["device"]
            or opened.st_ino != self.artifact["inode"]
            or opened.st_size != self.artifact["bytes"]
            or opened.st_mtime_ns != self.artifact["mtime_ns"]
            or _sha256_file(
                self.artifact["path"], deadline=deadline, cancelled=cancelled
            )
            != YAMNET_MODEL_SHA256
        ):
            raise DjAnalysisError("yamnet_model_changed")
        inputs = self.interpreter.get_input_details()
        outputs = self.interpreter.get_output_details()
        if len(inputs) != 1 or len(outputs) != 1:
            raise DjAnalysisError("yamnet_contract_mismatch")
        input_shape = tuple(int(value) for value in inputs[0]["shape"])
        output_shape = tuple(int(value) for value in outputs[0]["shape"])
        if input_shape not in ((YAMNET_WINDOW_SAMPLES,), (1, YAMNET_WINDOW_SAMPLES)):
            raise DjAnalysisError("yamnet_contract_mismatch")
        if output_shape not in ((YAMNET_CLASS_COUNT,), (1, YAMNET_CLASS_COUNT)):
            raise DjAnalysisError("yamnet_contract_mismatch")
        if np.dtype(inputs[0]["dtype"]) != np.dtype(np.float32) or np.dtype(
            outputs[0]["dtype"]
        ) != np.dtype(np.float32):
            raise DjAnalysisError("yamnet_contract_mismatch")
        self.input = inputs[0]
        self.output = outputs[0]
        if self.rss_reader() > RSS_CAP_BYTES:
            raise DjAnalysisError("rss_cap_exceeded")

    def analyze(self, path, *, deadline, cancelled=None):
        import av

        source_rate = None
        resampled_frames = 0
        with tempfile.TemporaryFile() as pcm_file:
            with av.open(str(path)) as container:
                streams = [
                    stream for stream in container.streams if stream.type == "audio"
                ]
                if len(streams) != 1:
                    raise DjAnalysisError("unsupported_audio_streams")
                resampler = av.AudioResampler(
                    format="fltp", layout="mono", rate=YAMNET_SAMPLE_RATE
                )
                for frame in container.decode(streams[0]):
                    _check_deadline(deadline, cancelled)
                    rate = int(
                        frame.sample_rate or streams[0].codec_context.sample_rate or 0
                    )
                    if rate <= 0 or (source_rate is not None and source_rate != rate):
                        raise DjAnalysisError("source_timeline_changed")
                    source_rate = rate
                    for converted in resampler.resample(frame):
                        block = (
                            converted.to_ndarray()
                            .astype(np.float32, copy=False)
                            .reshape(-1)
                        )
                        if not np.all(np.isfinite(block)):
                            raise DjAnalysisError("non_finite_audio")
                        pcm_file.write(block.astype("<f4", copy=False).tobytes())
                        resampled_frames += len(block)
                        if resampled_frames > MAX_SOURCE_SECONDS * YAMNET_SAMPLE_RATE:
                            raise DjAnalysisError("source_duration_unsupported")
                        if self.rss_reader() > RSS_CAP_BYTES:
                            raise DjAnalysisError("rss_cap_exceeded")
                for converted in resampler.resample(None):
                    block = (
                        converted.to_ndarray()
                        .astype(np.float32, copy=False)
                        .reshape(-1)
                    )
                    if not np.all(np.isfinite(block)):
                        raise DjAnalysisError("non_finite_audio")
                    pcm_file.write(block.astype("<f4", copy=False).tobytes())
                    resampled_frames += len(block)
            if not resampled_frames:
                raise DjAnalysisError("empty_audio")
            if resampled_frames > MAX_SOURCE_SECONDS * YAMNET_SAMPLE_RATE:
                raise DjAnalysisError("source_duration_unsupported")
            pcm_file.flush()
            pcm = np.memmap(pcm_file, dtype="<f4", mode="r", shape=(resampled_frames,))
            positions = []
            rows = []
            try:
                last_start = max(0, resampled_frames - YAMNET_WINDOW_SAMPLES)
                starts = list(range(0, last_start + 1, YAMNET_HOP_SAMPLES))
                if not starts or starts[-1] != last_start:
                    starts.append(last_start)
                for start in starts:
                    _check_deadline(deadline, cancelled)
                    window = np.zeros(YAMNET_WINDOW_SAMPLES, dtype=np.float32)
                    available = min(YAMNET_WINDOW_SAMPLES, resampled_frames - start)
                    window[:available] = pcm[start : start + available]
                    tensor = (
                        window
                        if tuple(self.input["shape"]) == (YAMNET_WINDOW_SAMPLES,)
                        else window[None, :]
                    )
                    self.interpreter.set_tensor(self.input["index"], tensor)
                    self.interpreter.invoke()
                    scores = np.asarray(
                        self.interpreter.get_tensor(self.output["index"]),
                        dtype=np.float32,
                    ).reshape(-1)
                    if len(scores) != YAMNET_CLASS_COUNT or not np.all(
                        np.isfinite(scores)
                    ):
                        raise DjAnalysisError("invalid_yamnet_output")
                    positions.append(
                        int(
                            round(
                                (start + min(available, YAMNET_WINDOW_SAMPLES) / 2)
                                * 1000
                                / YAMNET_SAMPLE_RATE
                            )
                        )
                    )
                    rows.append(
                        [
                            round(float(scores[index]), 8)
                            for index in YAMNET_VOCAL_CLASSES
                        ]
                    )
                    if self.rss_reader() > RSS_CAP_BYTES:
                        raise DjAnalysisError("rss_cap_exceeded")
            finally:
                del pcm
        return {
            "positions_ms": positions,
            "class_indices": list(YAMNET_VOCAL_CLASSES),
            "scores": rows,
        }


def _freeze(value):
    if isinstance(value, np.ndarray):
        result = value.copy()
        result.setflags(write=False)
        return result
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True)
class RawDjEvidence:
    """One immutable measurement; contract projections do not rerun inference."""

    model_output: Mapping
    vocal_output: Mapping | None
    content_sha256: str
    duration_seconds: float
    source: Mapping
    timings: Mapping

    def annotation_arguments(self):
        return {
            "model_output": self.model_output,
            "vocal_output": self.vocal_output,
            "content_sha256": self.content_sha256,
            "representation_id": f"sha256:{self.content_sha256}",
            "duration_seconds": self.duration_seconds,
            "source": dict(self.source),
        }


def analyze_evidence(
    path,
    *,
    model_path,
    yamnet_model_path=None,
    adapter=None,
    yamnet_adapter=None,
    deadline_seconds=JOB_DEADLINE_SECONDS,
    cancelled=None,
    progress=None,
):
    started = time.monotonic()
    deadline = started + max(1, min(JOB_DEADLINE_SECONDS, deadline_seconds))
    poll = (
        cancelled
        if isinstance(cancelled, CancellationPoller)
        else CancellationPoller(cancelled)
    )
    source_path = Path(path).resolve(strict=True)
    before = source_path.stat()
    content_hash = _sha256_file(source_path, deadline=deadline, cancelled=poll)
    hashed = time.monotonic()
    _check_deadline(deadline, poll)
    beat = adapter or BeatThisWindowAdapter(
        model_path, deadline=deadline, cancelled=poll
    )
    beat_loaded = time.monotonic()
    model_output = beat.analyze(
        source_path, deadline=deadline, cancelled=poll, progress=progress
    )
    beat_done = time.monotonic()
    if poll(force=True):
        raise DjAnalysisError("cancelled")
    _check_deadline(deadline)
    vocal = yamnet_adapter
    if vocal is None and yamnet_model_path:
        vocal = YamnetLiteAdapter(yamnet_model_path, deadline=deadline, cancelled=poll)
    vocal_loaded = time.monotonic()
    vocal_output = (
        vocal.analyze(source_path, deadline=deadline, cancelled=poll) if vocal else None
    )
    vocal_done = time.monotonic()
    after = source_path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) or _sha256_file(source_path, deadline=deadline, cancelled=poll) != content_hash:
        raise DjAnalysisError("source_changed")
    if poll(force=True):
        raise DjAnalysisError("cancelled")
    _check_deadline(deadline)
    count = len(model_output["beat_logits"])
    samples = int(
        model_output.get(
            "source_resampled_frames", max(1, count - 1) * MODEL_HOP_SAMPLES
        )
    )
    source = {
        "sample_rate": int(model_output["source_sample_rate"]),
        "decoded_frames": int(model_output["source_decoded_frames"]),
        "analysis_sample_rate": MODEL_SAMPLE_RATE,
        "analysis_resampled_frames": samples,
        "analysis_frames": count,
        "decoder": "pyav-16.1.0-streaming",
        "codec": model_output.get("source_codec", "unknown"),
        "timeline_verified": model_output.get("timeline_verified") is True,
        "representation_kind": "encoded-source-sha256",
        "playback_representation_verified": False,
    }
    timings = {
        "source_hash_seconds": hashed - started,
        "beat_setup_seconds": beat_loaded - hashed,
        "beat_analysis_seconds": beat_done - beat_loaded,
        "vocal_setup_seconds": vocal_loaded - beat_done,
        "vocal_analysis_seconds": vocal_done - vocal_loaded,
        "total_seconds": time.monotonic() - started,
        "rss_bytes_at_completion": _rss_bytes(),
    }
    return RawDjEvidence(
        _freeze(model_output),
        _freeze(vocal_output),
        content_hash,
        samples / MODEL_SAMPLE_RATE,
        _freeze(source),
        _freeze(timings),
    )
