"""Review fixes in the analysis runners and signal code (no database).

* An Opus file's denormal residue after silence is digital zero for the edge
  pass: the edge is measured, and is the one exact zeros give.
* Joining small decoded frames (``coalesce_blocks``) leaves every
  measurement bit-identical.
* The waveform analyzer refuses non-finite samples.
* True peak skips the oversampling of a block that cannot raise it, with the
  same result.
* The runner's wake wait uses poll where there is one.
* A ``finish`` that raises while files are cancelled leaves none unfinished.
* A worker that failed to start is tried again after a cool-down.
* Leftover downloads of a dead process whose PID was reused are removed; a
  download has an overall deadline.
"""
import os
import socket
import time
import types

import numpy as np
import pytest
from scipy.signal import upfirdn

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import analysis_download as download
from plugins.LumaeAnalysis import analysis_isolation as iso
from plugins.LumaeAnalysis import edge_profiles as edge
from plugins.LumaeAnalysis import loudness
from plugins.LumaeAnalysis import loudness_v1 as lv1
from plugins.LumaeAnalysis import parallel_analysis as par
from plugins.LumaeAnalysis import runner

IDENTITY = dict(catalog_instance_id="catalog-a", track_id="track-a",
                media_revision="sha256:" + "a" * 64, content_sha256="b" * 64)


def _music(rate, seconds, seed=5):
    rng = np.random.default_rng(seed)
    frames = int(rate * seconds)
    t = np.arange(frames) / rate
    tone = 0.3 * np.sin(2 * np.pi * 330 * t) + 0.05 * rng.standard_normal(frames)
    return np.stack([tone, np.roll(tone, 11)]).astype(np.float32)


def _blocks(pcm, size):
    return [pcm[:, start:start + size] for start in range(0, pcm.shape[1], size)]


# ------------------------------------------------------------- finding 1


def test_denormal_residue_after_silence_is_digital_zero_for_the_edge():
    pytest.importorskip("av")
    rate = 48000
    music = _music(rate, 12)
    head, tail = int(0.5 * rate), 3 * rate
    zeros = np.concatenate([np.zeros((2, head), np.float32), music, np.zeros((2, tail), np.float32)], axis=1)
    residue = zeros.copy()
    denormal = np.float32(1.4e-45)
    assert 0 < denormal < np.finfo(np.float32).tiny
    residue[:, :head:7] = denormal
    residue[:, -tail::5] = -denormal
    residue[0, -tail + 100] = np.float32(1e-20)  # a normal float, still -400 dBFS
    expected = edge.analyze_edge_blocks(_blocks(zeros, 960), rate, **IDENTITY)
    measured = edge.analyze_edge_blocks(_blocks(residue, 960), rate, **IDENTITY)
    assert measured == expected
    assert measured["leading_silence"]["frames"] == head
    assert measured["landmarks"]["trailing_padding_frames"] == tail


def test_quiet_audio_above_the_floor_is_kept():
    pytest.importorskip("av")
    rate = 48000
    music = _music(rate, 6)
    quiet = np.full((2, rate), np.float32(2e-15))
    pcm = np.concatenate([music, quiet], axis=1)
    measured = edge.analyze_edge_blocks([pcm], rate, **IDENTITY)
    assert measured["landmarks"]["trailing_padding_frames"] == 0


# ------------------------------------------------------------- finding 8


def test_coalesce_blocks_joins_frames_and_closes_its_source():
    closed = []

    def source():
        try:
            for _ in range(5):
                yield np.ones((2, 960), np.float32)
            yield np.ones((1, 10), np.float32)  # another shape: passed on alone
            yield np.ones((2, 40), np.float32)
        finally:
            closed.append(True)

    out = list(edge.coalesce_blocks(source(), 2000))
    assert [block.shape for block in out] == [(2, 2880), (2, 1920), (1, 10), (2, 40)]
    assert closed == [True]
    early = edge.coalesce_blocks(source(), 2000)
    next(early)
    closed.clear()
    early.close()
    assert closed == [True]


def test_joined_frames_measure_the_same_edge_and_loudness():
    pytest.importorskip("av")
    rate = 44100
    pcm = _music(rate, 40)
    pcm[:, -rate * 2:] = 0
    small = _blocks(pcm, 1152)
    meter_small, meter_joined = lv1.SeriesMeter(edge.CANONICAL_RATE), lv1.SeriesMeter(edge.CANONICAL_RATE)
    alone = edge.analyze_edge_blocks(small, rate, **IDENTITY, loudness=meter_small)
    joined = edge.analyze_edge_blocks(list(edge.coalesce_blocks(iter(small), 8192)), rate,
                                      **IDENTITY, loudness=meter_joined)
    assert alone == joined
    assert (meter_small.result(channels=2, sample_rate=rate)
            == meter_joined.result(channels=2, sample_rate=rate))


def test_joined_frames_give_the_same_waveform_profile():
    rate = 44100
    pcm = _music(rate, 30)
    small = loudness.analyze_blocks(_blocks(pcm, 1152), rate, channel_count=2)
    joined = loudness.analyze_blocks(edge.coalesce_blocks(iter(_blocks(pcm, 1152)), 8192), rate,
                                     channel_count=2)
    assert small == joined


# ------------------------------------------------------------- finding 6


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_the_waveform_analyzer_refuses_non_finite_samples(bad):
    rate = 8000
    pcm = _music(rate, 4)
    pcm[1, rate * 2 + 3] = bad
    with pytest.raises(ValueError, match="non-finite"):
        loudness.analyze_blocks(_blocks(pcm, 1000), rate, channel_count=2)


# ------------------------------------------------------------- finding 7


class _AlwaysOversampled(lv1._TruePeak):
    """The true peak before the skip: every block oversampled."""

    def add(self, pcm):
        extended = np.concatenate([self.history, pcm], axis=1)
        start, stop = self.history_frames * self.factor, (self.history_frames + pcm.shape[1]) * self.factor
        for channel in extended:
            upsampled = upfirdn(self.filter, channel, up=self.factor)[start:stop]
            if upsampled.size:
                self.peak = max(self.peak, float(np.max(np.abs(upsampled))))
        self.history = extended[:, -self.history_frames:]


def test_the_true_peak_bound_comes_from_the_filter_taps():
    for factor in (2, 4):
        meter = lv1._TruePeak(factor, 1)
        branches = [np.sum(np.abs(meter.filter[phase::factor])) for phase in range(factor)]
        assert meter.gain >= max(branches) > 1.0
        # The worst case is reached: an input of signs matching a branch's taps.
        phase = int(np.argmax(branches))
        taps = meter.filter[phase::factor]
        assert abs(float(np.dot(taps, np.sign(taps)))) <= meter.gain


def test_skipping_blocks_that_cannot_raise_the_true_peak_changes_nothing(monkeypatch):
    rng = np.random.default_rng(11)
    calls = []
    real = lv1.upfirdn
    monkeypatch.setattr(lv1, "upfirdn", lambda *args, **kwargs: calls.append(1) or real(*args, **kwargs))
    for trial in range(60):
        factor = (4, 2)[trial % 2]
        frames = int(rng.integers(5000, 40000))
        index = np.arange(frames)
        if trial % 3 == 0:  # a loud start, then a decay: most blocks skip
            pcm = rng.standard_normal((2, frames)) * np.exp(-index / 4000.0)
        elif trial % 3 == 1:  # near-Nyquist tones, whose inter-sample peaks exceed the samples
            pcm = np.repeat(np.sin(2 * np.pi * rng.uniform(0.4, 0.499) * index)[None, :], 2, axis=0)
        else:
            pcm = np.sign(rng.standard_normal((2, frames))) * rng.uniform(0, 1, (2, frames)) ** 3
        pcm = pcm.astype(np.float32).astype(np.float64)
        skipping, always = lv1._TruePeak(factor, 2), _AlwaysOversampled(factor, 2)
        position = 0
        while position < frames:
            size = int(rng.integers(1, 6000))
            skipping.add(pcm[:, position:position + size])
            always.add(pcm[:, position:position + size])
            assert skipping.peak == always.peak
            assert np.array_equal(skipping.history, always.history)
            position += size
        assert skipping.finish() == always.finish()
    calls_with_skip = len(calls)
    calls.clear()
    meter = lv1._TruePeak(4, 2)
    loud = np.full((2, 100), 0.9)
    meter.add(loud)
    meter.add(loud * 0.1)  # its history still holds loud samples: oversampled
    first = len(calls)
    assert first == 4
    meter.add(loud * 0.1)  # cannot raise the peak: not oversampled
    assert len(calls) == first and 0 < calls_with_skip


# ------------------------------------------------------------- finding 9


def test_the_wake_wait_reads_a_readable_connection():
    left, right = socket.socketpair()
    try:
        began = time.monotonic()
        assert runner._readable(left, 0.05) is False
        assert time.monotonic() - began < 2
        right.sendall(b"x")
        assert runner._readable(left, 1.0) is True
    finally:
        left.close()
        right.close()


@pytest.mark.skipif(not hasattr(__import__("select"), "poll"), reason="no poll() here")
def test_the_wake_wait_works_above_fd_setsize():
    resource = pytest.importorskip("resource")
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if max(soft, hard if hard != resource.RLIM_INFINITY else 4096) < 1200:
        pytest.skip("RLIMIT_NOFILE too low")
    if soft < 1200:
        resource.setrlimit(resource.RLIMIT_NOFILE, (1200, hard))
    held = []
    try:
        while not held or held[-1] < 1030:
            held.append(os.open(os.devnull, os.O_RDONLY))
        left, right = socket.socketpair()
        try:
            assert left.fileno() >= 1024
            right.sendall(b"x")
            assert runner._readable(left, 1.0) is True
        finally:
            left.close()
            right.close()
    finally:
        for fd in held:
            os.close(fd)
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


# ------------------------------------------------------------- finding 5


class _FakePool:
    """Three files in flight; the first completes once the third starts, cancel ends the rest."""

    parallel = True
    workers = 3

    def __init__(self):
        self.done = []
        self.submitted = []

    def submit(self, job):
        self.submitted.append(job)
        if job.unit == 2:
            first = self.submitted[0]
            first.result = "done"
            self.done.append(first)

    def cancel(self, job):
        job.error = par.Cancelled()
        self.done.append(job)

    def collect(self, _timeout):
        return self.done.pop(0) if self.done else None


@pytest.mark.parametrize("also_failing", [None, 1])
def test_a_finish_that_raises_still_finishes_every_file_in_flight(also_failing):
    finished = []

    def finish(unit, _context, result, error):
        finished.append((unit, result, type(error).__name__ if error else None))
        if unit == 0:
            raise RuntimeError("first finish failed")
        if unit == also_failing:
            raise RuntimeError("a cancelled finish failed")

    with pytest.raises(RuntimeError, match="first finish failed"):
        par.run(range(3), lambda unit: (lambda: unit, None), finish, pool=_FakePool())
    assert sorted(finished) == [(0, "done", None), (1, None, "Cancelled"), (2, None, "Cancelled")]


def test_cancelling_raises_the_first_finish_error_after_finishing_all():
    pool, finished = _FakePool(), []
    jobs = {}
    for unit in (1, 2, 3):
        job = par._Job(unit, None, None)
        jobs[id(job)] = (unit, job)

    def finish(unit, _context, _result, _error):
        finished.append(unit)
        if unit in (1, 2):
            raise RuntimeError(f"finish {unit}")

    def settle(job):
        jobs.pop(id(job))
        finish(job.unit, job.context, job.result, job.error)

    with pytest.raises(RuntimeError, match="finish 1"):
        par._cancel_in_flight(pool, jobs, settle, finish, par.Report())
    assert finished == [1, 2, 3] and not jobs


# ------------------------------------------------------------- finding 4


class _FakeWorker:
    def __init__(self, *_args, **_kwargs):
        self.creator = None
        self.owner_pid = os.getpid()
        self.healthy = False
        self.killed = False

    def alive(self):
        return True

    def kill(self, _grace):
        self.killed = True

    def close(self):
        pass


def test_a_worker_start_is_tried_again_after_the_cool_down(monkeypatch):
    iso.shutdown()
    monkeypatch.setattr(iso, "_Worker", _FakeWorker)
    try:
        iso._unavailable = ("no worker", time.monotonic())
        with pytest.raises(iso._WorkerUnavailable, match="no worker"):
            iso._checkout(1)
        iso._unavailable = ("no worker", time.monotonic() - iso.UNAVAILABLE_RETRY_SECONDS - 1)
        worker, pooled = iso._checkout(1)
        assert isinstance(worker, _FakeWorker) and pooled is True and iso._unavailable is None
        iso._checkin(worker, pooled, 0)
    finally:
        iso.shutdown()


def test_a_failed_worker_start_is_remembered_with_its_time(monkeypatch):
    iso.shutdown()

    def unavailable(_startup):
        raise iso._WorkerUnavailable("worker did not start in time")

    monkeypatch.setattr(iso, "_checkout", unavailable)
    try:
        before = time.monotonic()
        result = iso._run_exec(lambda path, **_kwargs: ("in-process", path), "a.flac", {}, None, "",
                               {}, 1, 0, 0, 1)
        assert result == ("in-process", "a.flac")
        reason, failed_at = iso._unavailable
        assert reason == "worker did not start in time" and failed_at >= before
        assert iso._unavailable_reason() == reason
        monkeypatch.setattr(iso.time, "monotonic", lambda: failed_at + iso.UNAVAILABLE_RETRY_SECONDS)
        assert iso._unavailable_reason() is None and iso._unavailable is None
    finally:
        iso.shutdown()


# ------------------------------------------------------------- finding 3


def test_leftovers_of_a_reused_pid_are_removed_and_this_processs_own_kept(tmp_path, monkeypatch):
    monkeypatch.setattr(download.tempfile, "gettempdir", lambda: str(tmp_path))
    me, other = os.getpid(), os.getpid() + 1
    started = {me: 500, other: 900}
    monkeypatch.setattr(download, "_start_time", lambda pid: started.get(int(pid)))
    monkeypatch.setattr(download, "_alive", lambda pid: pid in started)
    mine = download.task_directory()
    assert os.path.basename(mine) == f"task-{me}-500"
    root = tmp_path / download.ROOT_NAME
    names = {
        f"task-{me}": True,           # a restart that got this PID, older name
        f"task-{me}-400": True,       # a restart that got this PID
        f"task-{other}-900": False,   # the live process that has that PID
        f"task-{other}-899": True,    # its PID's earlier owner
        f"task-{other}": False,       # a live PID, no start time: kept
        f"task-{other + 1}-7": True,  # no such process
        "not-ours": False,
    }
    for name in names:
        (root / name).mkdir()
        (root / name / "left.audio").write_bytes(b"x")
    (root.joinpath(os.path.basename(mine)) / "current.audio").write_bytes(b"x")
    assert download.remove_leftovers() == sum(names.values())
    for name, removed in names.items():
        assert (root / name).exists() is not removed, name
    assert os.path.exists(os.path.join(mine, "current.audio"))


def test_without_a_start_time_the_directory_keeps_the_pid_name(tmp_path, monkeypatch):
    monkeypatch.setattr(download.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(download, "_start_time", lambda _pid: None)
    monkeypatch.setattr(download, "_alive", lambda pid: pid == os.getpid())
    mine = download.task_directory()
    assert os.path.basename(mine) == f"task-{os.getpid()}"
    assert download.remove_leftovers() == 0 and os.path.isdir(mine)


@pytest.mark.skipif(not os.path.exists("/proc/self/stat"), reason="no /proc")
def test_the_start_time_is_read_from_proc():
    assert download._start_time(os.getpid()) == download._start_time(os.getpid()) > 0
    assert download._start_time(2 ** 22 + 12345) is None


# ------------------------------------------------------------- finding 12


class _Trickle:
    status_code = 200
    headers = {"Content-Type": "audio/flac"}

    def iter_content(self, chunk_size):
        while True:
            yield b"x"

    def close(self):
        pass


def test_a_download_stops_at_its_overall_deadline(tmp_path, monkeypatch):
    clock = [1000.0]

    def monotonic():
        clock[0] += 0.5
        return clock[0]

    monkeypatch.setattr(download, "time", types.SimpleNamespace(monotonic=monotonic))
    session = types.SimpleNamespace(get=lambda *args, **kwargs: _Trickle())
    target = download.Target("http://example.invalid/a", {}, {}, str(tmp_path / "a.audio"))
    with pytest.raises(download.DownloadError, match="per-file analysis limit"):
        download.fetch(target, session=session, deadline_seconds=30)
    assert not os.path.exists(target.path)
    assert clock[0] < 1000.0 + 40
