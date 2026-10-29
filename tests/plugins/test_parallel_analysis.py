"""Performance plan WP1b: parallel analysis inside the one reconcile tick.

``parallel_analysis.run`` prepares and finishes every unit on the calling
thread and runs only the work on the tick's analysis threads, each with its
own exec worker. It starts no unit once the deadline is nearer than the
pass's average and lets the files in flight finish, however long past the
deadline, serving on-demand work meanwhile; only a pass that raises cancels
them (their children killed). One worker runs everything on the calling
thread, as before WP1b.
"""
import logging
import os
import threading
import time

import psutil
import pytest

import analysis_isolation_fakes as fakes
from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import analysis_isolation as iso
from plugins.LumaeAnalysis import parallel_analysis as par

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX process isolation")
MAIN = threading.main_thread()


@pytest.fixture(autouse=True, scope="module")
def _stop_workers_at_end():
    yield
    iso.shutdown()


@pytest.fixture
def pool():
    with par.tick_scope(3) as tick_pool:
        yield tick_pool


def _gone(pid, within=5.0):
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                return True
        except psutil.NoSuchProcess:
            return True
        time.sleep(0.05)
    return False


class Recorder:
    """prepare/finish that assert the calling thread and record what happened."""

    def __init__(self, work_for):
        self.work_for = work_for
        self.prepared, self.finished = [], []

    def prepare(self, unit):
        assert threading.current_thread() is MAIN
        self.prepared.append(unit)
        work = self.work_for(unit)
        return None if work is None else (work, {"unit": unit})

    def finish(self, unit, context, result, error):
        assert threading.current_thread() is MAIN
        assert context == {"unit": unit}
        self.finished.append((unit, result, type(error).__name__ if error else None))


# ---------------------------------------------------------------- sizing


@pytest.mark.parametrize("cpus,memory,expected", [
    (8, 16 << 30, 4),       # capped at MAX_AUTO_WORKERS
    (4, 8 << 30, 2),        # cores minus the reserve
    (2, 16 << 30, 1),       # never below one
    (16, 1 << 30, 1),       # a quarter of 1 GiB holds no 512 MiB child
    (16, 3 << 30, 1),
    (16, 4 << 30, 2),
    (8, None, 4),           # memory unknown: cores only
])
def test_auto_workers_is_bounded_by_cores_memory_and_the_cap(cpus, memory, expected):
    assert par.auto_workers(cpus=cpus, memory=memory) == expected


@pytest.mark.parametrize("raw,expected", [
    (None, "auto"), ("", "auto"), ("AUTO", "auto"), ("x", "auto"), (True, "auto"),
    ("3", 3), (0, 1), (99, par.MAX_WORKERS), (2, 2),
])
def test_the_setting_is_auto_or_a_bounded_count(raw, expected):
    assert par.normalize_setting(raw) == expected


def test_auto_resolves_from_this_host():
    assert 1 <= par.resolve_workers("auto") <= par.MAX_AUTO_WORKERS
    assert par.resolve_workers(5) == 5


def test_available_memory_is_the_lower_of_the_hosts_and_the_containers_headroom(monkeypatch):
    files = {"/proc/meminfo": "MemTotal: 16777216 kB\nMemAvailable: 8388608 kB"}
    monkeypatch.setattr(par, "_read", files.get)
    assert par.available_memory_bytes() == 8 << 30
    files.update({"/sys/fs/cgroup/memory.max": str(3 << 30), "/sys/fs/cgroup/memory.current": str(1 << 30)})
    assert par.available_memory_bytes() == 2 << 30
    # Reclaimable page cache (media files read) is not in use.
    files["/sys/fs/cgroup/memory.stat"] = f"anon {512 << 20}\ninactive_file {512 << 20}\nactive_file 0"
    assert par.available_memory_bytes() == (2 << 30) + (512 << 20)
    files["/sys/fs/cgroup/memory.max"] = "max"
    assert par.available_memory_bytes() == 8 << 30
    files.clear()
    assert par.available_memory_bytes() is None


def test_the_host_is_described_as_auto_sizes_it(monkeypatch):
    files = {"/proc/meminfo": "MemAvailable: 20971520 kB",
             "/sys/fs/cgroup/memory.max": str(4 << 30), "/sys/fs/cgroup/memory.current": str(1 << 30)}
    monkeypatch.setattr(par, "_read", files.get)
    monkeypatch.setattr(par, "available_cpus", lambda: 6)
    monkeypatch.setattr(par, "_host_memory_bytes", lambda: 32 << 30)
    assert par.describe_host() == "6 cores, 32.0 GB memory (container limit 4.0 GB), 3.0 GB available"
    assert par.memory_bytes() == 4 << 30  # auto sizes from the limit
    del files["/sys/fs/cgroup/memory.max"]
    assert par.describe_host() == "6 cores, 32.0 GB memory, 20.0 GB available"
    monkeypatch.setattr(par, "_host_memory_bytes", lambda: None)
    files.clear()
    assert par.describe_host() == "6 cores, unknown memory, unknown available"


# ---------------------------------------------------------------- serial


def test_one_worker_runs_everything_on_the_calling_thread_in_order():
    order = []

    def work_for(unit):
        if unit == "b":
            return None  # handled in prepare

        def work():
            assert threading.current_thread() is MAIN
            order.append(unit)
            if unit == "c":
                raise ValueError("bad file")
            return unit.upper()
        return work

    recorder = Recorder(work_for)
    report = par.run("abcd", recorder.prepare, recorder.finish, pool=par.Pool(1))
    assert order == ["a", "c", "d"]
    assert recorder.finished == [("a", "A", None), ("c", None, "ValueError"), ("d", "D", None)]
    assert report.reached() == "d" and report.stopped is None


def test_serial_stops_at_the_deadline_and_when_between_says_so():
    recorder = Recorder(lambda unit: (lambda: unit))
    report = par.run("abc", recorder.prepare, recorder.finish, deadline=time.monotonic() - 1)
    assert recorder.prepared == [] and report.stopped == "deadline" and report.reached() is None

    calls = []
    report = par.run("abc", recorder.prepare, recorder.finish,
                     between=lambda: calls.append(1) or len(calls) < 2)
    assert recorder.prepared == ["a"] and report.stopped == "stopped" and report.reached() == "a"


def test_outside_a_tick_there_is_no_pool():
    assert par.current() is None
    with par.tick_scope(2) as pool:
        assert par.current() is pool
    assert par.current() is None


# -------------------------------------------------------------- parallel


def test_k_files_run_at_once_and_everything_else_stays_on_the_calling_thread(pool):
    running, peak, lock = [0], [0], threading.Lock()

    def work_for(unit):
        def work():
            assert threading.current_thread() is not MAIN
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
            time.sleep(0.15)
            with lock:
                running[0] -= 1
            return unit
        return work

    recorder = Recorder(work_for)
    began = time.monotonic()
    report = par.run(range(9), recorder.prepare, recorder.finish, pool=pool)
    elapsed = time.monotonic() - began
    assert peak[0] == 3
    assert sorted(unit for unit, _r, _e in recorder.finished) == list(range(9))
    assert elapsed < 9 * 0.15 * 0.6, elapsed  # about three rounds, not nine
    assert report.reached() == 8


def test_threads_keep_their_exec_worker_across_files_and_passes(pool, tmp_path):
    media = tmp_path / "a.flac"
    media.write_bytes(b"\0")
    pids = []

    def work_for(_unit):
        return lambda: iso.run_isolated(fakes.describe_process, str(media), limit_seconds=60)

    for _ in range(2):
        recorder = Recorder(work_for)
        par.run(range(6), recorder.prepare, recorder.finish, pool=pool)
        pids.append({result["pid"] for _u, result, _e in recorder.finished})
        assert all(result["exec_worker"] for _u, result, _e in recorder.finished)
    # At most one worker per thread, the same ones in the second pass.
    assert len(pids[0]) <= 3 and pids[1] <= pids[0]
    pool.close()
    assert all(_gone(pid) for pid in pids[0])


def _turn_lines(caplog):
    return [record.getMessage() for record in caplog.records if record.name == par.logger.name]


def test_a_turn_logs_k_and_why_at_its_first_file_and_its_workers_at_its_end(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger=par.logger.name)
    media = tmp_path / "a.flac"
    media.write_bytes(b"\0")
    recorder = Recorder(lambda _unit: lambda: iso.run_isolated(fakes.describe_process, str(media),
                                                               limit_seconds=60))
    with par.tick_scope(2, reason="setting auto") as pool:
        assert _turn_lines(caplog) == []  # nothing analysed yet
        par.run(range(4), recorder.prepare, recorder.finish, pool=pool)
        assert len(_turn_lines(caplog)) == 1
    pids = {result["pid"] for _u, result, _e in recorder.finished}
    start, end = _turn_lines(caplog)
    assert start.startswith("lumae_analysis analysis turn: up to 2 file(s) at once (setting auto; ")
    assert " cores, " in start and start.endswith(" available)")
    assert f"{len(pids)} analysis worker(s) (pids " in end
    assert all(str(pid) in end for pid in pids) and all(_gone(pid) for pid in pids)
    assert "(each since it started); kept for reuse: none; left running: none" in end
    assert "largest peak memory unknown" not in end or not os.path.isdir("/proc/self")


def test_a_turn_that_analyses_nothing_logs_nothing(caplog):
    caplog.set_level(logging.INFO, logger=par.logger.name)
    with par.tick_scope(3, reason="setting auto") as pool:
        par.run(range(3), lambda _unit: None, lambda *_args: None, pool=pool)
    assert _turn_lines(caplog) == []


def test_a_one_file_turn_logs_the_pooled_worker_as_kept(tmp_path, caplog, monkeypatch):
    # A runner turn is never alone on its process: the exec path, as in production.
    monkeypatch.setattr(iso, "FORK_FAST_PATH", False)
    caplog.set_level(logging.INFO, logger=par.logger.name)
    media = tmp_path / "a.flac"
    media.write_bytes(b"\0")
    recorder = Recorder(lambda _unit: lambda: iso.run_isolated(fakes.describe_process, str(media),
                                                               limit_seconds=60))
    try:
        with par.tick_scope(1, reason="setting 1") as pool:
            par.run(range(2), recorder.prepare, recorder.finish, pool=pool)
        (pid,) = {result["pid"] for _u, result, _e in recorder.finished}
        start, end = _turn_lines(caplog)
        assert start.startswith("lumae_analysis analysis turn: up to 1 file(s) at once (setting 1; ")
        assert f"1 analysis worker(s) (pids {pid})" in end
        assert f"kept for reuse: {pid}; left running: none" in end
    finally:
        iso.shutdown()


def test_no_file_starts_once_the_deadline_is_nearer_than_the_average(pool):
    starts = []

    def work_for(unit):
        def work():
            starts.append(time.monotonic())
            time.sleep(0.3)
            return unit
        return work

    recorder = Recorder(work_for)
    began = time.monotonic()
    report = par.run(range(20), recorder.prepare, recorder.finish, pool=pool,
                     deadline=began + 0.5)
    # Three at once, then three more after 0.3 s (0.2 s left < the 0.3 s average: none).
    assert len(recorder.prepared) == 3, recorder.prepared
    assert report.stopped == "deadline" and not report.cancelled
    assert [error for _u, _r, error in recorder.finished] == [None] * 3


def test_files_in_flight_at_the_deadline_run_to_completion(pool, monkeypatch):
    monkeypatch.setattr(par, "DRAIN_SECONDS", 0.2, raising=False)  # 1.8.0's window, scaled

    def work_for(unit):
        def work():
            time.sleep(1.0)  # several times the time left at the start
            return unit
        return work

    recorder = Recorder(work_for)
    began = time.monotonic()
    report = par.run(range(5), recorder.prepare, recorder.finish, pool=pool, deadline=began + 0.3)
    assert time.monotonic() - began >= 1.0
    assert recorder.prepared == [0, 1, 2]  # none started after the deadline
    assert sorted(recorder.finished) == [(0, 0, None), (1, 1, None), (2, 2, None)]
    assert report.stopped == "deadline" and not report.cancelled and report.reached() == 2


def test_on_demand_work_is_served_while_the_pass_waits_past_the_deadline(pool, monkeypatch):
    monkeypatch.setattr(par, "WAIT_SERVE_SECONDS", 0.2)
    served = []

    def between():
        assert threading.current_thread() is MAIN
        served.append(time.monotonic())
        return True

    recorder = Recorder(lambda unit: (lambda: time.sleep(1.5) or unit))
    began = time.monotonic()
    report = par.run(range(3), recorder.prepare, recorder.finish, pool=pool,
                     deadline=began + 0.3, between=between)
    # Once before each of the three units, then every 0.2 s of the 1.5 s wait.
    assert len(served) >= 3 + 4, served
    assert not report.cancelled and report.reached() == 2


def test_a_file_slower_than_the_tick_completes_and_the_pass_reaches_its_end(pool, monkeypatch, tmp_path):
    """The 1.8.0 livelock: a pass that keeps its cursor before a file that
    did not finish, run tick after tick, with one file longer than a tick.
    Cancelled at every deadline, the file restarted every tick and the pass
    never completed; now it finishes in the tick that started it."""
    media = tmp_path / "a.flac"
    media.write_bytes(b"\0")
    units = list(range(8))
    slow, tick_seconds = 2, 0.4
    # 1.8.0's 5 s drain window, scaled to these ticks (gone since).
    monkeypatch.setattr(par, "DRAIN_SECONDS", 0.2, raising=False)
    started, cursor, ticks = [], -1, []

    def work_for(unit):
        def work():
            started.append(unit)
            if unit == slow:  # an analysis child that outlasts the tick
                iso.run_isolated(fakes.stall, str(media), limit_seconds=60, stall_seconds=2.0)
            else:
                time.sleep(0.05)
            return unit
        return work

    for _tick in range(6):
        recorder = Recorder(work_for)
        report = par.run([unit for unit in units if unit > cursor], recorder.prepare, recorder.finish,
                         pool=pool, deadline=time.monotonic() + tick_seconds)
        ticks.append(report)
        if report.stopped is None and not report.cancelled:
            break  # "complete"
        reached = report.reached()
        cursor = cursor if reached is None else reached

    assert ticks[-1].stopped is None and not ticks[-1].cancelled, "the pass never completed"
    assert not any(report.cancelled for report in ticks)
    assert started.count(slow) == 1  # never restarted (nor downloaded again)
    assert sorted(started) == units


def test_reached_never_passes_a_unit_that_did_not_finish():
    report = par.Report()
    report.taken = [0, 1, 2]
    report.done = {0, 2}
    assert report.reached() == 0
    report.done.add(1)
    assert report.reached() == 2


def test_a_failing_pass_kills_the_files_in_flight_and_finishes_them_cancelled(pool, tmp_path):
    media = tmp_path / "a.flac"
    media.write_bytes(b"\0")
    pid_files = [tmp_path / f"pid-{index}" for index in range(2)]

    def work_for(unit):
        if unit == 2:
            for path in pid_files:  # both children are running
                for _ in range(200):
                    if path.exists() and path.read_text():
                        break
                    time.sleep(0.05)
            raise RuntimeError("claim failed")
        return lambda: iso.run_isolated(fakes.sleep_forever, str(media), limit_seconds=60,
                                        pid_file=str(pid_files[unit]))

    recorder = Recorder(work_for)
    began = time.monotonic()
    with pytest.raises(RuntimeError, match="claim failed"):
        par.run(range(5), recorder.prepare, recorder.finish, pool=pool)
    assert time.monotonic() - began < 15  # not the children's 60 s limit
    assert sorted(recorder.finished) == [(0, None, "Cancelled"), (1, None, "Cancelled")]
    assert all(_gone(int(path.read_text())) for path in pid_files)


def test_a_failing_prepare_cancels_and_finishes_the_files_in_flight(pool):
    started = threading.Event()

    def work_for(unit):
        if unit == 2:
            raise RuntimeError("claim failed")

        def work():
            started.set()
            time.sleep(0.2)
            return unit
        return work

    recorder = Recorder(work_for)
    with pytest.raises(RuntimeError, match="claim failed"):
        par.run(range(4), recorder.prepare, recorder.finish, pool=pool)
    # Units 0 and 1 were in flight: each is finished (done or cancelled), none lost.
    assert sorted(unit for unit, _r, _e in recorder.finished) == [0, 1]


def test_between_stops_taking_units_and_the_files_in_flight_finish(pool):
    calls = []

    def work_for(unit):
        def work():
            time.sleep(0.1)
            return unit
        return work

    recorder = Recorder(work_for)
    report = par.run(range(10), recorder.prepare, recorder.finish, pool=pool,
                     between=lambda: calls.append(1) or len(calls) <= 2)
    assert report.stopped == "stopped"
    assert sorted(recorder.finished) == [(0, 0, None), (1, 1, None)]


def test_a_closed_pool_refuses_work():
    pool = par.Pool(2)
    pool.close()
    with pytest.raises(RuntimeError):
        pool.submit(object())
