"""The 1.7.0 scheduler on every supported AudioMuse host (plan: host matrix).

The floor stays AudioMuse 3.2.0. Newer host features are detected at run
time, never by version string, and every host-dependent path fails open:

| Shape          | Host                                                         |
|----------------|--------------------------------------------------------------|
| 3.2.x          | no cron_retry; the cron gate ignores plugin tasks            |
| 3.3.0-3.5.1    | cron_retry and the gate; one-thread jobs (fork path)         |
| 3.5.2-3.6.1    | task_types; a heartbeat thread around plugin calls           |
| >=3.6.2        | the message is taken in taskqueue (same keys)                |
| >=3.6.2 + AM   | AM-1 retry pass first; AM-2 notes are the plugin's message   |

A small host loop drives the real tick: the minute pass dispatches due rows
in cron.id order (the watchdog's first), the retry pass (every 11 minutes)
runs after it, or before it with AM-1, and a main_analysis runs 10 minutes.
Every shape also runs with each host call raising: the tick's result and
cadence then equal the feature-absent ones.
"""
import random
import sys
import threading
import time
import types

import pytest

from test_lumae_analysis import load_plugin  # noqa: F401
from test_reconcile_scheduler_postgres import RECONCILE, _cron, _rows, tick  # noqa: F401
from plugins.LumaeAnalysis import reconcile  # noqa: E402


SHAPES = {
    "3.2.x": {"cron_retry": False, "gate_counts_plugins": False, "task_types": False,
              "heartbeat": False, "retry_first": False, "plugin_notes": False},
    "3.3.0-3.5.1": {"cron_retry": True, "gate_counts_plugins": True, "task_types": False,
                    "heartbeat": False, "retry_first": False, "plugin_notes": False},
    "3.5.2-3.6.1": {"cron_retry": True, "gate_counts_plugins": True, "task_types": True,
                    "heartbeat": True, "retry_first": False, "plugin_notes": False},
    ">=3.6.2": {"cron_retry": True, "gate_counts_plugins": True, "task_types": True,
                "heartbeat": True, "retry_first": False, "plugin_notes": False},
    ">=3.6.2+AM": {"cron_retry": True, "gate_counts_plugins": True, "task_types": True,
                   "heartbeat": True, "retry_first": True, "plugin_notes": True},
}
MAIN = ("main_analysis", "main_clustering", "cleaning", "provider_migration", "server_sweep",
        "naming_preview")
RETRY_EVERY = 11


class _Raising(types.ModuleType):
    def __getattr__(self, name):
        raise RuntimeError(f"host {name} unavailable")


def _host(db, monkeypatch, shape, *, raising=False):
    """Install the shape's host modules and tables."""
    task_types = types.ModuleType("task_types")
    task_types.BATCH_GATE_TASK_TYPES = MAIN
    if raising:
        monkeypatch.setitem(sys.modules, "task_types", _Raising("task_types"))
    elif shape["task_types"]:
        monkeypatch.setitem(sys.modules, "task_types", task_types)
    else:
        monkeypatch.delitem(sys.modules, "task_types", raising=False)
    queue = types.SimpleNamespace(current_task_id=lambda: None)
    if raising:
        queue = _Raising("taskqueue")
    monkeypatch.setitem(sys.modules, "taskqueue", queue)
    with db.cursor() as cur:
        if shape["cron_retry"] and not raising:
            cur.execute("CREATE TABLE cron_retry (task_type TEXT PRIMARY KEY, "
                        "retry_until DOUBLE PRECISION, attempts INTEGER DEFAULT 0, "
                        "first_blocked_at DOUBLE PRECISION, blocker_task_id TEXT, "
                        "blocker_task_type TEXT)")
        elif raising:
            # The table is there, but reading it fails (a permission change,
            # a renamed column): the yield check turns itself off.
            cur.execute("CREATE FUNCTION lumae_test_unavailable() RETURNS boolean "
                        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'unavailable'; END $$")
            cur.execute("CREATE VIEW cron_retry AS SELECT 'analysis'::text AS task_type "
                        "WHERE lumae_test_unavailable()")
            cur.execute("ALTER TABLE task_status RENAME TO task_status_rows")
            cur.execute("CREATE VIEW task_status AS SELECT task_id, parent_task_id, task_type, "
                        "status FROM task_status_rows WHERE lumae_test_unavailable()")
    db.commit()


def _always_working(mod, monkeypatch):
    """The watchdog always has work; a tick's work itself is not what is tested."""
    worked = []
    monkeypatch.setattr(reconcile, "_work_summary", lambda cur: {
        "ready": 1, "waiting": 0, "retry_count": 0, "next_retry_at": None,
        "retry_in_seconds": None})

    def work(db, report, host_yield, every_source=False):
        worked.append(True)
        return "processed"

    monkeypatch.setattr(mod, "_reconcile_tick", work)
    return worked


class HostLoop:
    """AudioMuse's cron loop, per minute, over the test schema's tables."""

    def __init__(self, mod, db, shape, monkeypatch, start):
        self.mod, self.db, self.shape = mod, db, shape
        self.start, self.minute = start, 0
        self.main_until = None
        self.main_started = None
        self.lumae_ticks = []
        clock = self.clock = {"now": start}
        real_yield_cron = reconcile.yield_cron
        monkeypatch.setattr(reconcile, "yield_cron",
                            lambda excluded=(), now=None, rng=None: real_yield_cron(
                                excluded, now=clock["now"], rng=random.Random(4)))
        with db.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS host_cron_order (task_type TEXT PRIMARY KEY, "
                        "id INTEGER)")
            cur.execute("INSERT INTO host_cron_order VALUES (%s, 1), ('analysis', 2)", (RECONCILE,))
            cur.execute("INSERT INTO cron (name, task_type, cron_expr, enabled) "
                        "VALUES ('analysis', 'analysis', '0 2 * * *', TRUE)")
        db.commit()

    def _live(self, plugins):
        types_ = list(MAIN) + (["plugin.%"] if plugins else [])
        return _rows(self.db, "SELECT count(*) FROM task_status WHERE parent_task_id IS NULL "
                              "AND status IN ('NEW', 'RUNNING') AND task_type LIKE ANY(%s)",
                     (types_,))[0][0]

    def _blocked(self):
        return self._live(self.shape["gate_counts_plugins"]) > 0

    def _start(self, task_type):
        if task_type == "analysis":
            self.main_started = self.minute
            self.main_until = self.minute + 10
            task = ("main", "main_analysis")
        else:
            task = (f"tick-{self.minute}", RECONCILE)
        with self.db.cursor() as cur:
            cur.execute("INSERT INTO task_status (task_id, parent_task_id, task_type, status) "
                        "VALUES (%s, NULL, %s, 'NEW')", task)
        self.db.commit()

    def _refuse(self, task_type):
        if not self.shape["cron_retry"]:
            return
        with self.db.cursor() as cur:
            cur.execute("INSERT INTO cron_retry (task_type) VALUES (%s) ON CONFLICT DO NOTHING",
                        (task_type,))
        self.db.commit()

    def _minute_pass(self):
        hour, minute = divmod(120 + self.minute, 60)
        rows = _rows(self.db, "SELECT c.task_type, c.cron_expr FROM cron c JOIN host_cron_order o "
                              "USING (task_type) WHERE c.enabled ORDER BY o.id")
        for task_type, expr in rows:
            minutes = reconcile.cron_minutes(expr)
            hours = expr.split()[1]
            if minute not in (minutes or ()) or (hours != "*" and int(hours) != hour):
                continue
            if self._blocked():
                self._refuse(task_type)
            else:
                self._start(task_type)

    def _retry_pass(self):
        if not self.shape["cron_retry"] or self.minute % RETRY_EVERY != 5:
            return
        for (task_type,) in _rows(self.db, "SELECT task_type FROM cron_retry ORDER BY task_type"):
            if self._blocked():
                continue
            with self.db.cursor() as cur:
                cur.execute("DELETE FROM cron_retry WHERE task_type=%s", (task_type,))
            self.db.commit()
            self._start(task_type)

    def _run(self):
        with self.db.cursor() as cur:
            if self.main_until is not None and self.minute >= self.main_until:
                cur.execute("UPDATE task_status SET status='SUCCESS' WHERE task_id='main'")
            cur.execute("UPDATE task_status SET status='RUNNING' WHERE task_id='main' "
                        "AND status='NEW'")
        self.db.commit()
        queued = _rows(self.db, "SELECT task_id FROM task_status WHERE task_type=%s "
                                "AND status='NEW'", (RECONCILE,))
        for (task_id,) in queued:
            result = self.mod.catalog_reconcile_task()
            self.lumae_ticks.append((self.minute, result["status"], self._live(False) > 0))
            with self.db.cursor() as cur:
                cur.execute("UPDATE task_status SET status='SUCCESS' WHERE task_id=%s", (task_id,))
            self.db.commit()

    def run(self, minutes, each_minute=None):
        """``each_minute``: called after each minute's ticks (work admitted
        meanwhile)."""
        heartbeat = None
        if self.shape["heartbeat"]:
            # 3.5.2+: a heartbeat thread runs beside the plugin call.
            stop = threading.Event()
            heartbeat = threading.Thread(target=stop.wait, daemon=True)
            heartbeat.start()
        try:
            for self.minute in range(minutes):
                self.clock["now"] = self.start + self.minute * 60
                if self.shape["retry_first"]:
                    self._retry_pass()
                self._minute_pass()
                if not self.shape["retry_first"]:
                    self._retry_pass()
                self._run()
                if each_minute is not None:
                    each_minute()
        finally:
            if heartbeat is not None:
                stop.set()
                heartbeat.join()


@pytest.mark.parametrize("name", list(SHAPES))
def test_host_schedules_start_and_lumae_resumes_on_every_host(tick, monkeypatch, name):
    mod, db = tick
    shape = SHAPES[name]
    _host(db, monkeypatch, shape)
    _always_working(mod, monkeypatch)
    reconcile.arm_reconcile(db, "work_admitted", commit=True)
    loop = HostLoop(mod, db, shape, monkeypatch, time.mktime((2026, 10, 11, 2, 0, 0, 0, 0, -1)))

    loop.run(45)

    # main_analysis starts within one retry interval of its fire: at once
    # where the gate ignores plugin tasks, else at the first retry pass.
    assert loop.main_started == (0 if not shape["gate_counts_plugins"] else 5), loop.lumae_ticks
    # No Lumae work while it runs.
    assert not [tick for tick in loop.lumae_ticks if tick[2] and tick[1] == "processed"]
    # Lumae works again within about one retry interval of it ending, or
    # within five minutes where the host does not retry (3.2.x: */5 while
    # yielding, a refused yield minute would wait an hour).
    resumed = [minute for minute, status, _live in loop.lumae_ticks
               if status == "processed" and minute >= loop.main_until]
    assert resumed and resumed[0] - loop.main_until <= (
        RETRY_EVERY + 1 if shape["cron_retry"] else 5), loop.lumae_ticks


@pytest.mark.parametrize("name", list(SHAPES))
def test_work_admitted_every_minute_leaves_the_yield_in_place(tick, monkeypatch, name):
    """Seen on the test instance after the 1.7.0 install: every analysed song,
    on-demand request or credits lookup admits work. Arming minute cadence on
    each one ended the yield, a fire was queued ahead of every retry pass, and
    the refused schedule never started."""
    mod, db = tick
    shape = SHAPES[name]
    _host(db, monkeypatch, shape)
    _always_working(mod, monkeypatch)
    reconcile.arm_reconcile(db, "work_admitted", commit=True)
    loop = HostLoop(mod, db, shape, monkeypatch, time.mktime((2026, 10, 11, 2, 0, 0, 0, 0, -1)))

    loop.run(45, each_minute=lambda: reconcile.arm_reconcile(db, "analysis_hook", commit=True))

    assert loop.main_started == (0 if not shape["gate_counts_plugins"] else 5), loop.lumae_ticks
    assert not [tick for tick in loop.lumae_ticks if tick[2] and tick[1] == "processed"]
    resumed = [minute for minute, status, _live in loop.lumae_ticks
               if status == "processed" and minute >= loop.main_until]
    assert resumed and resumed[0] - loop.main_until <= (
        RETRY_EVERY + 1 if shape["cron_retry"] else 5), loop.lumae_ticks


def _one_tick(mod, db, monkeypatch, shape, *, raising):
    _host(db, monkeypatch, shape, raising=raising)
    worked = _always_working(mod, monkeypatch)
    if shape["cron_retry"] and not raising:
        with db.cursor() as cur:
            cur.execute("INSERT INTO cron_retry (task_type) VALUES ('analysis')")
        db.commit()
    result = mod.catalog_reconcile_task()
    keys = ("status", "yielded_to", "actions", "tracks_analyzed", "requested")
    return {key: result[key] for key in keys}, _cron(db), worked


@pytest.mark.parametrize("name", list(SHAPES))
def test_every_host_call_raising_ticks_as_with_the_features_absent(tick, monkeypatch, name):
    mod, db = tick
    raised, raised_cron, raised_work = _one_tick(mod, db, monkeypatch, SHAPES[name], raising=True)
    absent, absent_cron, absent_work = _one_tick(mod, db, monkeypatch, SHAPES["3.2.x"],
                                                 raising=False)
    assert raised == absent and raised_work == absent_work == [True]
    assert raised["status"] == "processed" and raised["yielded_to"] is None
    assert raised_cron == absent_cron


def _host_details(shape, task_type, result):
    """What AudioMuse shows for a finished plugin task: (message, note)."""
    summary = dict(result) if isinstance(result, dict) else {}
    summary.setdefault("message", "Plugin task completed successfully.")
    message = summary.get("status_message") or summary.get("message")
    if shape["plugin_notes"]:
        note = message
    elif "analysis" in task_type:
        songs = summary.get("tracks_analyzed")
        note = f"Songs analyzed: {int(songs) if isinstance(songs, (int, float)) else 0}"
    else:
        note = ""
    return message, note


@pytest.mark.parametrize("name", list(SHAPES))
def test_every_host_shows_the_ticks_sentence_and_count(tick, monkeypatch, name):
    mod, db = tick
    shape = SHAPES[name]
    _host(db, monkeypatch, shape)
    monkeypatch.setattr(mod, "serve_deferred_interactive", (lambda served: (
        lambda *args, **kwargs: served.pop() if served else 0))([4]))

    result = mod.catalog_reconcile_task()

    message, note = _host_details(shape, RECONCILE, result)
    assert message.startswith("Lumae: 4 on-demand tracks in ")
    assert note == (message if shape["plugin_notes"] else "Songs analyzed: 4")


def test_several_servers_in_one_task_do_the_work_once(tick, monkeypatch):
    """With server_scope='all' the host stores a list of per-server results
    and its default message; the first call did the work and logged it."""
    mod, db = tick
    shape = SHAPES[">=3.6.2"]
    _host(db, monkeypatch, shape)
    monkeypatch.setitem(sys.modules, "taskqueue",
                        types.SimpleNamespace(current_task_id=lambda: "task-1"))
    monkeypatch.setattr(mod, "_reconcile_dispatch_id", None)
    monkeypatch.setattr(mod, "serve_deferred_interactive", (lambda served: (
        lambda *args, **kwargs: served.pop() if served else 0))([2]))
    lines = []
    monkeypatch.setattr(reconcile, "logger", types.SimpleNamespace(
        info=lambda message, *args: lines.append(message % args),
        debug=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None))

    results = [mod.catalog_reconcile_task() for _server in ("server-a", "server-b")]

    assert [result["status"] for result in results] == ["processed", "same_dispatch"]
    assert _host_details(shape, RECONCILE, results)[0] == "Plugin task completed successfully."
    [line] = lines
    assert "2 on-demand tracks" in line
