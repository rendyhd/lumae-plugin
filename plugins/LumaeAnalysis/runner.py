"""Lumae's runners: background work outside AudioMuse's task queue (runner plan R1–R4).

AudioMuse refuses every start while any ``plugin.*`` root task is live, queued
included, and a queued task waits for a worker that may be off for days
(runner plan 2026-10-25, evidence). So Lumae's background work runs in loops
of its own and never writes a ``task_status`` row:

* the **server runner**, in AudioMuse's web process (``on_flask_start``),
  always up: the light lane (catalogue, finalization, credits, lookups);
* the **worker runner**, in a queue worker's main process
  (``on_worker_start``), up while a worker is: the heavy lane (per-track
  analysis, relationships). A light server with a heavier worker keeps the
  heavy work on the worker; the plugin's ``heavy_work`` setting decides what
  the server takes while no worker is up.

Each role has one leader at a time: the thread holding the role's PostgreSQL
advisory lock on a connection of its own (``database.connect_raw``,
autocommit, as AudioMuse's listener threads do). The others stand by and try
again every STANDBY_SECONDS. A leader ``LISTEN``s on NOTIFY_CHANNEL,
heartbeats its ``runner_state`` row from a small thread of its own, and runs
the turn function (``__init__.run_runner_turn``) inside its Flask application
context, so each action's ``get_db()`` is a fresh connection the context's
teardown closes. Whoever records work calls ``notify()`` before committing,
which wakes the leaders within a second wherever they run.

No host imports at module level, so it loads in tests without a host.
"""

import logging
import os
import select
import socket
import threading
import time
from collections import namedtuple

logger = logging.getLogger("lumae_analysis.runner")

ROLE_SERVER = "server"
ROLE_WORKER = "worker"
ROLES = (ROLE_SERVER, ROLE_WORKER)
# Advisory lock keys, one per role, apart from AudioMuse's own (its schema
# lock, the provider migration's 7421536190082003, the 7263548xx start-up
# family, the task-liveness class 0x41554449).
LOCK_KEYS = {ROLE_SERVER: 5501021337001001, ROLE_WORKER: 5501021337001002}
NOTIFY_CHANNEL = "lumae_runner"
# The web process's hook runs before AudioMuse loads its indexes, so the
# first turn waits as AudioMuse's own blob reclaim does.
STARTUP_DELAY_SECONDS = {ROLE_SERVER: 120, ROLE_WORKER: 30}
STANDBY_SECONDS = 60
HEARTBEAT_SECONDS = 30
# A worker counts as online while its runner's heartbeat is this recent.
WORKER_ONLINE_SECONDS = 90
# With nothing due, a leader still looks this often.
SWEEP_SECONDS = 15 * 60
# Between turns that worked, so AudioMuse's request threads get the interpreter.
PAUSE_SECONDS = 1.0
RESTART_DELAYS = (30, 60, 120, 240, 480, 900)

Turn = namedtuple("Turn", "worked mode reason next_due_seconds action")


def idle_turn(next_due_seconds=None, reason="nothing_due"):
    return Turn(False, "idle", reason, next_due_seconds, None)


def state_table():
    from plugin.api import table

    return table("runner_state")


def migrate_runner_state(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {state_table()} (
            role TEXT PRIMARY KEY,
            host TEXT,
            pid INTEGER,
            mode TEXT NOT NULL DEFAULT 'idle',
            reason TEXT,
            current_action TEXT,
            last_action TEXT,
            actions BIGINT NOT NULL DEFAULT 0,
            next_wake_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ,
            action_started_at TIMESTAMPTZ,
            heartbeat_at TIMESTAMPTZ,
            CHECK (role IN ('server', 'worker'))
        )
        """
    )


def notify(cur, reason):
    """Wake every runner: ``pg_notify`` on ``cur``, delivered when the
    caller commits, and this process's leaders at once."""
    cur.execute("SELECT pg_notify(%s, %s)", (NOTIFY_CHANNEL, str(reason or "work")[:200]))
    wake_local()


def wake_local():
    for runner in list(_runners.values()):
        runner.wake()


def worker_online(cur, seconds=WORKER_ONLINE_SECONDS):
    """Whether a worker runner's heartbeat is under ``seconds`` old."""
    cur.execute(
        f"SELECT heartbeat_at > now() - make_interval(secs => %s) FROM {state_table()} "
        "WHERE role='worker'",
        (int(seconds),),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def read_states(cur):
    """Every role's row, for the status page and health."""
    cur.execute(
        f"""
        SELECT role, host, pid, mode, reason, current_action, last_action, actions,
               next_wake_at, started_at, action_started_at, heartbeat_at,
               EXTRACT(EPOCH FROM (now() - heartbeat_at))
          FROM {state_table()} ORDER BY role
        """
    )
    keys = ("role", "host", "pid", "mode", "reason", "current_action", "last_action",
            "actions", "next_wake_at", "started_at", "action_started_at", "heartbeat_at",
            "heartbeat_age_seconds")
    return [dict(zip(keys, row)) for row in cur.fetchall()]


class Runner:
    """One role's loop in this process. ``turn(runner)`` does a bounded
    amount of work and returns a ``Turn``; ``still_installed()`` says whether
    this process's plugin code is still the installed one (R2)."""

    def __init__(self, role, *, app, turn, connect, still_installed=None,
                 startup_delay=None, clock=time.monotonic):
        if role not in ROLES:
            raise ValueError(f"unknown runner role {role!r}")
        self.role = role
        self.app = app
        self.turn = turn
        self.connect = connect
        self.still_installed = still_installed or (lambda: True)
        self.startup_delay = (STARTUP_DELAY_SECONDS[role] if startup_delay is None
                              else startup_delay)
        self.clock = clock
        self.pid = os.getpid()
        self.thread = None
        self.stopped = threading.Event()
        self._wake = threading.Event()
        self._conn = None
        self._conn_lock = threading.Lock()
        self._beating = None
        self.leading = False
        self.mode = "starting"
        self.reason = None
        self.action = None
        self.last_action = None
        self.actions = 0
        self.next_wake_in = None
        self._action_changed = False

    # -- lifecycle ---------------------------------------------------------
    def start(self):
        if self.thread is not None and self.thread.is_alive():
            return False
        self.stopped.clear()
        self.thread = threading.Thread(target=self._main, name=f"lumae-runner-{self.role}",
                                       daemon=True)
        self.thread.start()
        return True

    def stop(self):
        self.stopped.set()
        self._wake.set()

    def wake(self):
        if os.getpid() == self.pid:
            self._wake.set()

    def _wait(self, seconds):
        """Sleep up to ``seconds``; a stop ends it early. True when stopped."""
        return self.stopped.wait(max(0.0, seconds))

    def _main(self):
        if self._wait(self.startup_delay):
            return
        failures = 0
        while not self.stopped.is_set():
            try:
                self._lead()
                failures = 0
            except Exception:
                delay = RESTART_DELAYS[min(failures, len(RESTART_DELAYS) - 1)]
                failures += 1
                logger.exception("Lumae's %s runner stopped; it starts again in %d s",
                                 self.role, delay)
                self._release()
                if self._wait(delay):
                    break
        self._release()

    # -- what the turn reports ---------------------------------------------
    def begin_action(self, action):
        """The turn starts ``action``: the status page shows it."""
        self.action = action
        self._action_changed = True

    def end_action(self):
        if self.action:
            self.last_action = self.action
            self.actions += 1
        self.action = None
        self._action_changed = True

    # -- leadership --------------------------------------------------------
    def _lead(self):
        conn = self.connect(f"lumae-runner-{self.role}-{os.getpid()}")
        conn.autocommit = True
        self._conn = conn
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEYS[self.role],))
        if not cur.fetchone()[0]:
            cur.close()
            self._release()
            self.mode = "standby"
            self._wait(STANDBY_SECONDS)
            return
        self.leading = True
        cur.execute(f"LISTEN {NOTIFY_CHANNEL}")
        cur.close()
        self.mode, self.reason = "working", "leader"
        self._heartbeat(started=True)
        self._beating = threading.Event()
        beat = threading.Thread(target=self._beat, args=(self._beating,),
                                name=f"lumae-runner-{self.role}-heartbeat", daemon=True)
        beat.start()
        try:
            while not self.stopped.is_set():
                with self.app.app_context():
                    installed = self.still_installed()
                if not installed:
                    logger.warning("Lumae's %s runner stops: the plugin changed since this "
                                   "process started, and the restarted process runs the new "
                                   "code", self.role)
                    self.stop()
                    break
                turn = self._turn()
                if turn.worked:
                    self._drain()
                    if self._wait(PAUSE_SECONDS):
                        break
                    continue
                wait = SWEEP_SECONDS if turn.next_due_seconds is None else max(
                    PAUSE_SECONDS, min(SWEEP_SECONDS, float(turn.next_due_seconds)))
                self.next_wake_in = wait
                self._heartbeat()
                self._wait_for_wake(wait)
                self.next_wake_in = None
        finally:
            self._beating.set()
            beat.join(timeout=5)
            self._release()

    def _turn(self):
        self.mode, self.reason = "working", "turn"
        try:
            with self.app.app_context():
                turn = self.turn(self)
        finally:
            if self.action:
                self.end_action()
        self.mode, self.reason = turn.mode, turn.reason
        return turn

    def _drain(self):
        with self._conn_lock:
            conn = self._conn
            if conn is None:
                return
            conn.poll()
            del conn.notifies[:]
        self._wake.clear()

    def _wait_for_wake(self, seconds):
        """Until a NOTIFY, a local wake, a stop or ``seconds``."""
        deadline = self.clock() + seconds
        while not self.stopped.is_set():
            remaining = deadline - self.clock()
            if remaining <= 0:
                return "timeout"
            if self._wake.is_set():
                self._wake.clear()
                return "wake"
            readable, _, _ = select.select([self._conn], [], [], min(1.0, remaining))
            if readable:
                with self._conn_lock:
                    self._conn.poll()
                    notified = bool(self._conn.notifies)
                    del self._conn.notifies[:]
                if notified:
                    return "notify"
        return "stopped"

    def _beat(self, done):
        """The heartbeat thread: every HEARTBEAT_SECONDS, and soon after the
        turn starts or ends an action, so a long action never looks like a
        worker that went away."""
        last = self.clock()
        while not done.wait(1.0):
            if self._action_changed or self.clock() - last >= HEARTBEAT_SECONDS:
                try:
                    self._heartbeat()
                except Exception:
                    logger.debug("Lumae's %s runner missed a heartbeat", self.role,
                                 exc_info=True)
                last = self.clock()

    def _heartbeat(self, started=False):
        self._action_changed = False
        with self._conn_lock:
            if self._conn is None:
                return
            cur = self._conn.cursor()
            try:
                cur.execute(
                    f"""
                    INSERT INTO {state_table()} AS s
                        (role, host, pid, mode, reason, current_action, last_action, actions,
                         next_wake_at, started_at, action_started_at, heartbeat_at)
                    VALUES (%(role)s, %(host)s, %(pid)s, %(mode)s, %(reason)s, %(action)s,
                            %(last)s, %(actions)s,
                            CASE WHEN %(wake)s::float IS NULL THEN NULL
                                 ELSE now() + make_interval(secs => %(wake)s::float) END,
                            now(), CASE WHEN %(action)s::text IS NULL THEN NULL ELSE now() END,
                            now())
                    ON CONFLICT (role) DO UPDATE SET
                        host=EXCLUDED.host, pid=EXCLUDED.pid, mode=EXCLUDED.mode,
                        reason=EXCLUDED.reason, current_action=EXCLUDED.current_action,
                        last_action=EXCLUDED.last_action, actions=EXCLUDED.actions,
                        next_wake_at=EXCLUDED.next_wake_at,
                        started_at=CASE WHEN %(started)s THEN now() ELSE s.started_at END,
                        action_started_at=CASE
                            WHEN EXCLUDED.current_action IS NULL THEN NULL
                            WHEN s.current_action IS NOT DISTINCT FROM EXCLUDED.current_action
                                 AND NOT %(started)s THEN s.action_started_at
                            ELSE now() END,
                        heartbeat_at=now()
                    """,
                    {"role": self.role, "host": socket.gethostname()[:200], "pid": os.getpid(),
                     "mode": str(self.mode)[:40],
                     "reason": str(self.reason)[:200] if self.reason else None,
                     "action": self.action, "last": self.last_action, "actions": self.actions,
                     "wake": self.next_wake_in, "started": bool(started)},
                )
            finally:
                cur.close()

    def _release(self):
        with self._conn_lock:
            conn, self._conn = self._conn, None
            leading, self.leading = self.leading, False
            if conn is None:
                return
            try:
                if leading:
                    cur = conn.cursor()
                    cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEYS[self.role],))
                    cur.close()
            except Exception:
                logger.debug("Lumae's %s runner could not release its lock", self.role,
                             exc_info=True)
            try:
                conn.close()
            except Exception:
                pass


_runners = {}
_lock = threading.Lock()


def start(role, **kwargs):
    """Start this process's runner for ``role`` once; returns it."""
    with _lock:
        runner = _runners.get(role)
        if runner is None or runner.pid != os.getpid():
            runner = _runners[role] = Runner(role, **kwargs)
        runner.start()
        return runner


def current(role):
    runner = _runners.get(role)
    return runner if runner is not None and runner.pid == os.getpid() else None


def stop_all():
    for runner in list(_runners.values()):
        runner.stop()
