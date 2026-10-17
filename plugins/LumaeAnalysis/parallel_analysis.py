"""Parallel file analysis inside the one reconcile tick (performance plan WP1b).

The tick stays a single root task. Only the work inside an action runs in
parallel: K long-lived threads, each with its own exec analysis worker
(``analysis_isolation.dedicated_worker``) at a lower CPU priority, run the
analysis of one file at a time. The calling thread keeps every database and
host call: it claims, downloads (until the analysis's own download, WP2),
publishes, serves deferred on-demand work and reports progress. A worker
thread holds no database connection and never calls the host, whose server
binding may not be thread-local.

``run`` drives one pass: it takes units in order, prepares each on the
calling thread (``prepare`` returns the work for a thread, or None when it
handled the unit itself), and finishes each on the calling thread as its
work completes (``finish``). It starts no unit once the deadline is closer
than the pass's average time per file. The files in flight at the deadline
run to completion, each bounded by its own per-file limit, as the serial path
runs its file past the deadline; meanwhile ``between`` still serves on-demand
work every ``WAIT_SERVE_SECONDS``. Up to 1.8.0's test build they were
cancelled ``DRAIN_SECONDS`` (5 s) after the deadline and due again from the
start: a file longer than the tick restarted every tick, never completed and
held its thread for good, and a pass with one never reached its end. A file
is cancelled only when the pass itself raises: its child is killed and
``finish`` gets ``Cancelled`` (the caller releases the track uncounted).
With one worker, or outside a tick, the work runs on the calling thread,
exactly as before WP1b.

K is the setting ``background_workers``: ``auto`` (the default) takes the
cores this process may use minus ``CPU_RESERVE``, the memory a quarter of
the host's (or the container's) memory gives children of ``CHILD_RAM_BYTES``
each, and at most ``MAX_AUTO_WORKERS``; a number from 1 to ``MAX_WORKERS``
fixes it. This module has no database or host imports.
"""

import logging
import math
import os
import queue
import threading
import time

from . import analysis_isolation

logger = logging.getLogger("lumae_analysis.parallel")

DEFAULT_SETTING = "auto"
MAX_WORKERS = 8
MAX_AUTO_WORKERS = 4
# Cores left to AudioMuse's web process, the media server and PostgreSQL.
CPU_RESERVE = 2
# A child's budget: an exec worker measured 124 MB, 250 MB at its peak, on a
# 4-minute FLAC (STATUS P3-8); hi-res sources (up to 384 kHz) take more.
CHILD_RAM_BYTES = 512 * 1024 * 1024
RAM_SHARE = 0.25
# The children compete with playback and transcoding on the media host.
NICE = 10
# While a pass only waits for files in flight (past the deadline, or with no
# unit left), it serves on-demand work this often: AudioMuse refuses
# on-demand enqueues while the tick runs (E3).
WAIT_SERVE_SECONDS = 5
# How long a cancelled thread gets to hand its file back.
CANCEL_JOIN_SECONDS = 10


class Cancelled(Exception):
    """The file was in flight when the pass raised (or its thread did not
    hand it back): nothing about it is counted."""


# ---------------------------------------------------------------- sizing


def _read(path):
    try:
        with open(path, encoding="ascii") as handle:
            return handle.read().strip()
    except (OSError, ValueError):
        return None


def available_cpus():
    """Cores this process may use: its affinity and a cgroup CPU quota."""
    try:
        count = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        count = os.cpu_count() or 1
    quota = _read("/sys/fs/cgroup/cpu.max")
    if quota:
        parts = quota.split()
        if len(parts) == 2 and parts[0] != "max":
            try:
                count = min(count, max(1, math.ceil(int(parts[0]) / int(parts[1]))))
            except (ValueError, ZeroDivisionError):
                pass
    return max(1, count)


def memory_bytes():
    """The host's memory, or the container's limit when lower; None if unknown."""
    total = None
    try:
        total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        total = None
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        raw = _read(path)
        if raw and raw.isdigit():
            limit = int(raw)
            # cgroup v1 reports "no limit" as a number near 2**63.
            if limit < (1 << 60) and (total is None or limit < total):
                total = limit
    return total if total and total > 0 else None


def auto_workers(cpus=None, memory=None):
    """K for ``auto``: bounded by cores, memory and MAX_AUTO_WORKERS; at least 1."""
    cpus = available_cpus() if cpus is None else cpus
    memory = memory_bytes() if memory is None else memory
    by_cpu = cpus - CPU_RESERVE
    by_memory = int(memory * RAM_SHARE // CHILD_RAM_BYTES) if memory else MAX_AUTO_WORKERS
    return max(1, min(MAX_AUTO_WORKERS, by_cpu, by_memory))


def normalize_setting(raw):
    """``"auto"`` or an int from 1 to MAX_WORKERS; anything else is ``auto``."""
    if isinstance(raw, str) and raw.strip().lower() in ("", DEFAULT_SETTING):
        return DEFAULT_SETTING
    if isinstance(raw, bool):
        return DEFAULT_SETTING
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_SETTING
    return min(max(value, 1), MAX_WORKERS)


def resolve_workers(raw):
    setting = normalize_setting(raw)
    return auto_workers() if setting == DEFAULT_SETTING else setting


# Speed-up over one file at a time, measured with a CPU-bound analysis on
# 8 cores (scripts/perf/parallel_bench.py, 2026-10-17). The settings page's
# ETA at a higher K is an estimate from it, not a measurement of this host.
MEASURED_SPEEDUP = {1: 1.0, 2: 1.86, 4: 3.38, 6: 4.32}


def speedup(workers):
    """The measured speed-up at ``workers``, interpolated between measurements;
    None above the last one."""
    points = sorted(MEASURED_SPEEDUP)
    workers = max(1, int(workers))
    if workers in MEASURED_SPEEDUP:
        return MEASURED_SPEEDUP[workers]
    if workers > points[-1]:
        return None
    low = max(point for point in points if point < workers)
    high = min(point for point in points if point > workers)
    share = (workers - low) / (high - low)
    return MEASURED_SPEEDUP[low] + share * (MEASURED_SPEEDUP[high] - MEASURED_SPEEDUP[low])


def next_workers(workers, cpus=None):
    """The next measured K above ``workers`` that this host's cores allow
    (with the reserve), or None."""
    cpus = available_cpus() if cpus is None else cpus
    higher = [point for point in sorted(MEASURED_SPEEDUP)
              if point > workers and point <= max(1, cpus - CPU_RESERVE) and point <= MAX_WORKERS]
    return higher[0] if higher else None


# ------------------------------------------------------------------ pool


class _Job:
    __slots__ = ("unit", "work", "context", "result", "error", "seconds", "slot", "cancelled")

    def __init__(self, unit, work, context):
        self.unit = unit
        self.work = work
        self.context = context
        self.result = None
        self.error = None
        self.seconds = None
        self.slot = None
        self.cancelled = False


class Pool:
    """``workers`` analysis threads for one tick; ``close`` stops them and their workers.

    Threads start on first use and live until ``close``, so each exec worker
    is started once a tick and reused across files and actions.
    """

    def __init__(self, workers, nice=NICE):
        self.workers = max(1, int(workers))
        self.nice = nice
        self._jobs = queue.Queue()
        self._done = queue.Queue()
        self._threads = []
        self._lock = threading.Lock()
        self._closed = False

    @property
    def parallel(self):
        return self.workers > 1

    def _start(self):
        with self._lock:
            if self._closed:
                raise RuntimeError("the analysis pool is closed")
            while len(self._threads) < self.workers:
                thread = threading.Thread(target=self._loop, name=f"lumae-analysis-{len(self._threads) + 1}",
                                          daemon=True)
                thread.start()
                self._threads.append(thread)

    def _loop(self):
        with analysis_isolation.dedicated_worker(nice=self.nice) as slot:
            while True:
                job = self._jobs.get()
                if job is None:
                    return
                job.slot = slot
                slot.begin(job)
                started = time.monotonic()
                try:
                    if job.cancelled:
                        raise Cancelled()
                    job.result = job.work()
                except analysis_isolation.AnalysisCancelled:
                    job.error = Cancelled()
                except BaseException as exc:  # noqa: BLE001 - handed to the caller
                    job.error = Cancelled() if job.cancelled else exc
                finally:
                    slot.end()
                    job.seconds = time.monotonic() - started
                    self._done.put(job)

    def submit(self, job):
        self._start()
        self._jobs.put(job)

    def cancel(self, job):
        job.cancelled = True
        slot = job.slot
        if slot is not None:
            slot.cancel(job)

    def collect(self, timeout):
        """The next finished job, or None after ``timeout`` seconds (0: don't wait)."""
        try:
            return self._done.get(timeout=timeout) if timeout else self._done.get_nowait()
        except queue.Empty:
            return None

    def close(self):
        """Stop the threads (each stops its worker); never waits for long."""
        with self._lock:
            self._closed = True
            threads, self._threads = self._threads, []
        for _ in threads:
            self._jobs.put(None)
        for thread in threads:
            thread.join(CANCEL_JOIN_SECONDS)
            if thread.is_alive():
                logger.warning("lumae_analysis analysis thread %s did not stop", thread.name)


_scope = threading.local()


class tick_scope:
    """The reconcile tick's pool: ``current()`` inside, closed on leaving."""

    def __init__(self, workers):
        self.pool = Pool(workers)

    def __enter__(self):
        self.previous = getattr(_scope, "pool", None)
        _scope.pool = self.pool
        return self.pool

    def __exit__(self, *_exc):
        _scope.pool = self.previous
        self.pool.close()
        return False


def current():
    """The tick's pool on this thread, or None (outside a tick: serial)."""
    return getattr(_scope, "pool", None)


# ---------------------------------------------------------------- runner


class Report:
    """What ``run`` did: units taken in order and how each ended."""

    def __init__(self):
        self.taken = []        # units prepared, in order
        self.done = set()      # indexes finished (inline, completed or failed)
        self.cancelled = []    # units whose work was cancelled
        self.stopped = None    # "deadline", "stopped" or None (units ran out)

    def reached(self):
        """The last unit of the finished run of units from the first, or None.

        A cursor kept there skips no unit that was cancelled or never
        finished.
        """
        last = None
        for index, unit in enumerate(self.taken):
            if index not in self.done:
                break
            last = unit
        return last


def run(units, prepare, finish, *, pool=None, deadline=None, between=None):
    """Prepare, work and finish ``units`` in order until they run out or the deadline.

    ``prepare(unit)`` (calling thread) returns ``(work, context)``, where
    ``work()`` is the part a worker thread may run, or None when it handled
    the unit itself. ``finish(unit, context, result, error)`` runs on the
    calling thread; ``error`` is the work's exception or ``Cancelled``.
    ``between()`` runs before each unit is prepared and returns False to stop
    taking units; the files in flight at a stop or the deadline still finish,
    and while ``run`` only waits for them ``between()`` runs every
    ``WAIT_SERVE_SECONDS`` (its answer then changes nothing). Returns a
    ``Report``.
    """
    report = Report()
    units = iter(units)
    if pool is None or not pool.parallel:
        for unit in units:
            if deadline is not None and time.monotonic() >= deadline:
                report.stopped = "deadline"
                break
            if between is not None and between() is False:
                report.stopped = "stopped"
                break
            index = len(report.taken)
            report.taken.append(unit)
            prepared = prepare(unit)
            if prepared is not None:
                work, context = prepared
                try:
                    result, error = work(), None
                except Exception as exc:  # noqa: BLE001 - handed to finish
                    result, error = None, exc
                finish(unit, context, result, error)
            report.done.add(index)
        return report

    in_flight = {}  # id(job): (index, job)
    average = [None, 0]  # seconds per file in this pass, files counted

    def settle(job):
        entry = in_flight.pop(id(job), None)
        if entry is None or entry[1] is not job:
            return  # a file abandoned by an earlier pass
        index = entry[0]
        if isinstance(job.error, Cancelled):
            report.cancelled.append(job.unit)
        else:
            if job.seconds is not None:
                total = (average[0] or 0.0) * average[1] + job.seconds
                average[1] += 1
                average[0] = total / average[1]
            report.done.add(index)
        finish(job.unit, job.context, job.result, job.error)

    def can_start():
        if deadline is None:
            return True
        remaining = deadline - time.monotonic()
        return remaining > 0 and (average[0] is None or remaining >= average[0])

    try:
        exhausted = False
        serve_at = None  # while only waiting for files in flight: the next between()
        while True:
            while not exhausted and report.stopped is None and len(in_flight) < pool.workers:
                if not can_start():
                    report.stopped = "deadline"
                    break
                if between is not None and between() is False:
                    report.stopped = "stopped"
                    break
                unit = next(units, _END)
                if unit is _END:
                    exhausted = True
                    break
                index = len(report.taken)
                report.taken.append(unit)
                prepared = prepare(unit)
                if prepared is None:
                    report.done.add(index)
                else:
                    job = _Job(unit, prepared[0], prepared[1])
                    in_flight[id(job)] = (index, job)
                    pool.submit(job)
                # Publish what finished meanwhile before taking the next unit.
                job = pool.collect(0)
                while job is not None:
                    settle(job)
                    job = pool.collect(0)
            if not in_flight:
                break
            wait, now = 1.0, time.monotonic()
            if between is not None and (exhausted or report.stopped is not None
                                        or (deadline is not None and now >= deadline)):
                # No unit starts now: wait for the files in flight, however
                # long past the deadline they run (each stops at its own
                # per-file limit), serving on-demand work meanwhile.
                if serve_at is None:
                    serve_at = now + WAIT_SERVE_SECONDS
                elif now >= serve_at:
                    between()
                    serve_at = time.monotonic() + WAIT_SERVE_SECONDS
                wait = max(0.0, min(wait, serve_at - time.monotonic()))
            elif between is not None and deadline is not None:
                wait = min(wait, deadline - now)  # start serving at the deadline
            job = pool.collect(wait)
            if job is not None:
                settle(job)
    finally:
        _cancel_in_flight(pool, in_flight, settle, finish, report)
    return report


_END = object()


def _cancel_in_flight(pool, in_flight, settle, finish, report):
    """Cancel the files still in flight and finish each: a file that completed
    meanwhile keeps its result, the rest end ``Cancelled``. A file that does
    not stop within CANCEL_JOIN_SECONDS is abandoned and finished as
    ``Cancelled`` too; its thread's late answer is ignored."""
    if not in_flight:
        return
    for _index, job in list(in_flight.values()):
        pool.cancel(job)
    give_up = time.monotonic() + CANCEL_JOIN_SECONDS
    while in_flight:
        left = give_up - time.monotonic()
        if left <= 0:
            break
        job = pool.collect(min(left, 0.5))
        if job is not None:
            settle(job)
    if in_flight:
        logger.warning("lumae_analysis %d analysis file(s) did not stop when cancelled", len(in_flight))
        for _index, job in list(in_flight.values()):
            in_flight.pop(id(job), None)
            report.cancelled.append(job.unit)
            finish(job.unit, job.context, None, Cancelled())
