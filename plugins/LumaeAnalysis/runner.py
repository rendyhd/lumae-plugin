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

Multi-worker plan (2026-10-30): with ``can_help()`` true (the setting
``worker_runners: every``), a worker runner that misses the lock **helps**
instead of standing by. A helper runs the same loop and turn function with
``HELPING`` set, so the turn does only the per-track library passes, and it
tries the lock again every STANDBY_SECONDS: when the leader goes away, one
helper leads. Every runner, leader or helper, heartbeats a row of its own in
``runner_workers`` (``runner_id``); only the leader writes ``runner_state``.

No host imports at module level, so it loads in tests without a host.
"""

import logging
import os
import select
import socket
import threading
import time
import uuid
from collections import namedtuple
from contextvars import ContextVar

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
# Multi-worker plan R1: an AudioMuse worker container runs two task-queue
# workers (queues ``default`` and ``high``) and each starts a worker runner,
# while ``background_workers`` sizes the files analysed at once for the whole
# container. With helpers, a runner first takes its host's lock (this class,
# and a hash of the role and host name): one runner per host leads or helps,
# and the others there stand by.
HOST_LOCK_CLASS = 550102133
# With nothing due, a leader still looks this often.
SWEEP_SECONDS = 15 * 60
# Between turns that worked, so AudioMuse's request threads get the interpreter.
PAUSE_SECONDS = 1.0
RESTART_DELAYS = (30, 60, 120, 240, 480, 900)

Turn = namedtuple("Turn", "worked mode reason next_due_seconds action")
# The role whose turn this thread runs; None outside a runner (a queued task,
# a request, the song hook). R5: the web process sizes its analysis smaller and
# downloads outside AudioMuse's TEMP_DIR.
ROLE = ContextVar("lumae_runner_role", default=None)
# Multi-worker plan: whether this thread's turn is a helper's (the per-track
# passes only, never on-demand work), and the id of the runner whose turn it
# is (the owner of the batches and analysis slots it takes).
HELPING = ContextVar("lumae_runner_helping", default=False)
RUNNER_ID = ContextVar("lumae_runner_id", default=None)


def idle_turn(next_due_seconds=None, reason="nothing_due"):
    return Turn(False, "idle", reason, next_due_seconds, None)


def new_runner_id():
    """``host:pid:nonce``: unique per runner, readable on the status page."""
    return f"{socket.gethostname()[:120]}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


_process_runner_id = (None, None)  # (pid, id): a fork gets an id of its own


def current_runner_id():
    """The id of this thread's runner, or one id per process outside a runner
    (a test, a request): what owns a batch or slot taken here."""
    global _process_runner_id
    runner_id = RUNNER_ID.get()
    if runner_id:
        return runner_id
    if _process_runner_id[0] != os.getpid():
        _process_runner_id = (os.getpid(), new_runner_id())
    return _process_runner_id[1]


def state_table():
    from plugin.api import table

    return table("runner_state")


def workers_table():
    from plugin.api import table

    return table("runner_workers")


def live_runner_sql(column):
    """SQL: the runner ``column`` names heartbeated within WORKER_ONLINE_SECONDS."""
    return (f"EXISTS (SELECT 1 FROM {workers_table()} live_runner "
            f"WHERE live_runner.runner_id={column} "
            f"AND live_runner.heartbeat_at > now() - make_interval(secs => {int(WORKER_ONLINE_SECONDS)}))")


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
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {marks_table()} (
            name TEXT PRIMARY KEY,
            done_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {workers_table()} (
            runner_id TEXT PRIMARY KEY,
            role TEXT NOT NULL,
            leader BOOLEAN NOT NULL DEFAULT FALSE,
            host TEXT,
            pid INTEGER,
            mode TEXT NOT NULL DEFAULT 'idle',
            reason TEXT,
            current_action TEXT,
            last_action TEXT,
            actions BIGINT NOT NULL DEFAULT 0,
            files_at_once INTEGER,
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CHECK (role IN ('server', 'worker'))
        )
        """
    )


# A runner's row stays this long after its last heartbeat (the status page
# shows the recent ones; the server's bookkeeping removes the rest).
WORKER_ROW_RETENTION_SECONDS = 24 * 60 * 60


def read_workers(cur, seconds=WORKER_ROW_RETENTION_SECONDS):
    """Every runner that heartbeated within ``seconds``, leaders first."""
    cur.execute(
        f"""
        SELECT runner_id, role, leader, host, pid, mode, reason, current_action,
               last_action, actions, files_at_once, started_at, heartbeat_at,
               EXTRACT(EPOCH FROM (now() - heartbeat_at))
          FROM {workers_table()}
         WHERE heartbeat_at > now() - make_interval(secs => %s)
         ORDER BY role, leader DESC, host, runner_id
        """,
        (int(seconds),),
    )
    keys = ("runner_id", "role", "leader", "host", "pid", "mode", "reason", "current_action",
            "last_action", "actions", "files_at_once", "started_at", "heartbeat_at",
            "heartbeat_age_seconds")
    return [dict(zip(keys, row)) for row in cur.fetchall()]


def prune_workers(cur, seconds=WORKER_ROW_RETENTION_SECONDS):
    """Remove the rows of runners gone longer than ``seconds``; returns how many."""
    cur.execute(
        f"DELETE FROM {workers_table()} WHERE heartbeat_at < now() - make_interval(secs => %s)",
        (int(seconds),),
    )
    return max(0, cur.rowcount or 0)


def marks_table():
    from plugin.api import table

    return table("runner_marks")


def claim_mark(cur, name, every_seconds):
    """Take ``name``'s turn when it last ran ``every_seconds`` ago or never:
    True once per period, whichever runner asks first."""
    cur.execute(
        f"""
        INSERT INTO {marks_table()} AS m (name, done_at) VALUES (%s, now())
        ON CONFLICT (name) DO UPDATE SET done_at=now()
         WHERE m.done_at <= now() - make_interval(secs => %s)
        RETURNING name
        """,
        (name, int(every_seconds)),
    )
    return cur.fetchone() is not None


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


def record_unavailable(cur, role, reason):
    """R10: this host cannot run ``role``'s runner; the status page says why.
    A leader elsewhere overwrites the row with its next heartbeat."""
    cur.execute(
        f"""
        INSERT INTO {state_table()} AS s (role, host, pid, mode, reason, heartbeat_at)
        VALUES (%s, %s, %s, 'unavailable', %s, NULL)
        ON CONFLICT (role) DO UPDATE SET host=EXCLUDED.host, pid=EXCLUDED.pid,
            mode='unavailable', reason=EXCLUDED.reason, current_action=NULL,
            heartbeat_at=NULL
        """,
        (role, socket.gethostname(), os.getpid(), str(reason)[:200]),
    )


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
    this process's plugin code is still the installed one (R2); ``can_help()``
    whether a runner that misses the lock helps (multi-worker plan)."""

    def __init__(self, role, *, app, turn, connect, still_installed=None,
                 startup_delay=None, clock=time.monotonic, can_help=None, host=None):
        if role not in ROLES:
            raise ValueError(f"unknown runner role {role!r}")
        self.role = role
        self.app = app
        self.turn = turn
        self.connect = connect
        self.still_installed = still_installed or (lambda: True)
        self.can_help = can_help or (lambda: False)
        self.startup_delay = (STARTUP_DELAY_SECONDS[role] if startup_delay is None
                              else startup_delay)
        self.clock = clock
        self.pid = os.getpid()
        self.host = (host or socket.gethostname())[:200]
        self.runner_id = new_runner_id()
        self.thread = None
        self.stopped = threading.Event()
        self._wake = threading.Event()
        self._conn = None
        self._conn_lock = threading.Lock()
        self._beating = None
        self.leading = False
        self.helping = False
        # Until its first turn as leader ends: the turn releases what the
        # role's previous leader took (R6).
        self.new_leader = False
        self.mode = "starting"
        self.reason = None
        self.action = None
        self.last_action = None
        self.actions = 0
        self.next_wake_in = None
        # Files the current turn analyses at once (its granted slots); the
        # status page shows it.
        self.files_at_once = None
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
        """The turn looks for ``action``'s work or does it: the status page shows it."""
        self.action = action
        self._action_changed = True

    def end_action(self, ran=True):
        """The action ends; one that ran is the last action and counted."""
        if self.action and ran:
            self.last_action = self.action
            self.actions += 1
        self.action = None
        self._action_changed = True

    # -- leadership --------------------------------------------------------
    def _lead(self):
        conn = self.connect(f"lumae-runner-{self.role}-{os.getpid()}")
        conn.autocommit = True
        self._conn = conn
        helps = self._may_help()
        if helps and not self._try_host_lock():
            # Another runner on this host leads or helps.
            self._release()
            self.mode = "standby"
            self._wait(STANDBY_SECONDS)
            return
        if not self._try_lock() and not (helps and not self._leader_here()):
            self._release()
            self.mode = "standby"
            self._wait(STANDBY_SECONDS)
            return
        self.helping = not self.leading
        cur = conn.cursor()
        cur.execute(f"LISTEN {NOTIFY_CHANNEL}")
        cur.close()
        self.mode, self.reason = "working", "leader" if self.leading else "helper"
        self._heartbeat(started=True)
        self._beating = threading.Event()
        beat = threading.Thread(target=self._beat, args=(self._beating,),
                                name=f"lumae-runner-{self.role}-heartbeat", daemon=True)
        beat.start()
        tried = self.clock()
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
                if self.helping and self.clock() - tried >= STANDBY_SECONDS:
                    # A helper leads once the leader went away, and stands by
                    # once helping is switched off or the leader is on its host.
                    tried = self.clock()
                    if self._try_lock():
                        self.helping = False
                        logger.info("Lumae's %s runner %s leads now", self.role, self.runner_id)
                        self._heartbeat(started=True)
                    elif not self._may_help() or self._leader_here():
                        break
                turn = self._turn()
                if turn.worked:
                    self._drain()
                    if self._wait(PAUSE_SECONDS):
                        break
                    continue
                wait = SWEEP_SECONDS if turn.next_due_seconds is None else max(
                    PAUSE_SECONDS, min(SWEEP_SECONDS, float(turn.next_due_seconds)))
                if self.helping:
                    wait = min(wait, max(PAUSE_SECONDS, STANDBY_SECONDS - (self.clock() - tried)))
                self.next_wake_in = wait
                self._heartbeat()
                self._wait_for_wake(wait)
                self.next_wake_in = None
        finally:
            self._beating.set()
            beat.join(timeout=5)
            self.helping = False
            self._release()

    def _try_lock(self):
        """Take the role's lock on this runner's connection; True when it leads."""
        with self._conn_lock:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEYS[self.role],))
                won = bool(cur.fetchone()[0])
            finally:
                cur.close()
        if won:
            self.leading = True
            self.new_leader = True
        return won

    def _try_host_lock(self):
        """Take this host's lock for the role on this runner's connection (R1)."""
        with self._conn_lock:
            cur = self._conn.cursor()
            try:
                cur.execute("SELECT pg_try_advisory_lock(%s, hashtext(%s))",
                            (HOST_LOCK_CLASS, f"{self.role}:{self.host}"))
                return bool(cur.fetchone()[0])
            finally:
                cur.close()

    def _leader_here(self):
        """Whether the role's leader is another runner on this host: it works there,
        so this one does not help (it led before helping was switched on, without the
        host's lock). True when it cannot tell."""
        with self._conn_lock:
            cur = self._conn.cursor()
            try:
                cur.execute(
                    f"""SELECT 1 FROM {workers_table()}
                         WHERE role=%s AND leader AND host=%s AND runner_id<>%s
                           AND heartbeat_at > now() - make_interval(secs => %s)
                         LIMIT 1""",
                    (self.role, self.host, self.runner_id, WORKER_ONLINE_SECONDS),
                )
                return cur.fetchone() is not None
            except Exception:
                logger.debug("Lumae's %s runner could not read where its leader runs", self.role,
                             exc_info=True)
                return True
            finally:
                cur.close()

    def _may_help(self):
        """Whether a runner that does not lead helps (``can_help``); False when it cannot tell."""
        try:
            with self.app.app_context():
                return bool(self.can_help())
        except Exception:
            logger.debug("Lumae's %s runner could not read whether it helps", self.role,
                         exc_info=True)
            return False

    def _turn(self):
        self.mode, self.reason = "working", "turn"
        tokens = (ROLE.set(self.role), HELPING.set(self.helping), RUNNER_ID.set(self.runner_id))
        try:
            with self.app.app_context():
                turn = self.turn(self)
            if self.leading:
                self.new_leader = False
        finally:
            RUNNER_ID.reset(tokens[2])
            HELPING.reset(tokens[1])
            ROLE.reset(tokens[0])
            if self.action:
                self.end_action()
            self.files_at_once = None
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
        """Until a NOTIFY, a local wake, a stop or ``seconds``. A heartbeat
        shares the LISTEN connection, and its query reads a NOTIFY off the
        socket into ``notifies``: the socket is then not readable, so those
        are looked at before each wait on it."""
        deadline = self.clock() + seconds
        while not self.stopped.is_set():
            remaining = deadline - self.clock()
            if remaining <= 0:
                return "timeout"
            if self._wake.is_set():
                self._wake.clear()
                return "wake"
            if self._take_notifies(poll=False):
                return "notify"
            readable, _, _ = select.select([self._conn], [], [], min(1.0, remaining))
            if readable and self._take_notifies(poll=True):
                return "notify"
        return "stopped"

    def _take_notifies(self, poll):
        with self._conn_lock:
            if poll:
                self._conn.poll()
            notified = bool(self._conn.notifies)
            del self._conn.notifies[:]
        return notified

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
                if self.leading:
                    self._heartbeat_role(cur, started)
            finally:
                cur.close()
            cur = self._conn.cursor()
            try:
                self._heartbeat_worker(cur, started)
            except Exception:
                # A database the plugin has not migrated yet (no
                # ``runner_workers``): the role's row above is what counts.
                logger.debug("Lumae's %s runner could not write its own row", self.role,
                             exc_info=True)
            finally:
                cur.close()

    def _heartbeat_worker(self, cur, started):
        """This runner's ``runner_workers`` row, leader or helper."""
        cur.execute(
            f"""
            INSERT INTO {workers_table()} AS w
                (runner_id, role, leader, host, pid, mode, reason, current_action, last_action,
                 actions, files_at_once, started_at, heartbeat_at)
            VALUES (%(id)s, %(role)s, %(leader)s, %(host)s, %(pid)s, %(mode)s, %(reason)s,
                    %(action)s, %(last)s, %(actions)s, %(files)s, now(), now())
            ON CONFLICT (runner_id) DO UPDATE SET
                leader=EXCLUDED.leader, mode=EXCLUDED.mode, reason=EXCLUDED.reason,
                current_action=EXCLUDED.current_action, last_action=EXCLUDED.last_action,
                actions=EXCLUDED.actions, files_at_once=EXCLUDED.files_at_once,
                started_at=CASE WHEN %(started)s THEN now() ELSE w.started_at END,
                heartbeat_at=now()
            """,
            {"id": self.runner_id, "role": self.role, "leader": bool(self.leading),
             "host": self.host, "pid": os.getpid(), "mode": str(self.mode)[:40],
             "reason": str(self.reason)[:200] if self.reason else None, "action": self.action,
             "last": self.last_action, "actions": self.actions, "files": self.files_at_once,
             "started": bool(started)},
        )

    def _heartbeat_role(self, cur, started):
        """The role's ``runner_state`` row: the leader's alone."""
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
            {"role": self.role, "host": self.host, "pid": os.getpid(),
             "mode": str(self.mode)[:40],
             "reason": str(self.reason)[:200] if self.reason else None,
             "action": self.action, "last": self.last_action, "actions": self.actions,
             "wake": self.next_wake_in, "started": bool(started)},
        )

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
                if leading:
                    # Its host's other runner need not wait for this row to go stale (R1).
                    cur = conn.cursor()
                    cur.execute(f"UPDATE {workers_table()} SET leader=false WHERE runner_id=%s",
                                (self.runner_id,))
                    cur.close()
            except Exception:
                logger.debug("Lumae's %s runner could not clear its leader row", self.role,
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
