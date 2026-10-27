"""Runner plan RN.1: the runners' lifecycle on a real PostgreSQL.

One leader per role through the advisory lock, a standby that takes over, a
NOTIFY that wakes an idle leader, the heartbeat row (and a worker counted
online by it), a stop when the plugin changed under the process, and a restart
after a turn fails.
"""
import contextlib
import os
import threading
import time
import uuid

import pytest

from test_lumae_analysis import load_plugin

load_plugin()
from plugins.LumaeAnalysis import runner  # noqa: E402

psycopg2 = pytest.importorskip("psycopg2")


@pytest.fixture
def schema():
    dsn = os.environ.get("LUMAE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("set LUMAE_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
    name = f"lumae_runner_{uuid.uuid4().hex}"
    db = psycopg2.connect(dsn)
    db.autocommit = True
    with db.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {name}")
        cur.execute(f"SET search_path TO {name}, public")
        runner.migrate_runner_state(cur)
    try:
        yield dsn, name, db
    finally:
        runner.stop_all()
        for active in list(runner._runners.values()):
            if active.thread is not None:
                active.thread.join(timeout=5)
        runner._runners.clear()
        with db.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        db.close()


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(runner, "STANDBY_SECONDS", 0.2)
    monkeypatch.setattr(runner, "HEARTBEAT_SECONDS", 0.5)
    monkeypatch.setattr(runner, "PAUSE_SECONDS", 0.05)
    monkeypatch.setattr(runner, "RESTART_DELAYS", (0.2,))


class App:
    def app_context(self):
        return contextlib.nullcontext()


def make(schema, role, turn, **kwargs):
    dsn, name, _db = schema

    def connect(application_name):
        return psycopg2.connect(dsn, application_name=application_name,
                                options=f"-c search_path={name},public")

    return runner.Runner(role, app=App(), turn=turn, connect=connect, startup_delay=0, **kwargs)


def until(condition, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql.format(T=runner.state_table()), params)
        return cur.fetchall()


def idle(_active):
    return runner.idle_turn(None)


def test_one_leader_per_role_and_a_standby_takes_over(schema):
    first, second = make(schema, "server", idle), make(schema, "server", idle)
    first.start()
    assert until(lambda: first.leading)
    second.start()
    assert until(lambda: second.mode == "standby")
    assert not second.leading
    worker = make(schema, "worker", idle)
    worker.start()
    assert until(lambda: worker.leading)  # another role, another lock
    first.stop()
    first.thread.join(timeout=5)
    assert until(lambda: second.leading)
    for active in (second, worker):
        active.stop()
        active.thread.join(timeout=5)


def test_a_notify_wakes_an_idle_leader_within_a_second(schema):
    _dsn, _name, db = schema
    turns = []
    active = make(schema, "server", lambda a: turns.append(time.monotonic()) or runner.idle_turn(None))
    active.start()
    assert until(lambda: len(turns) == 1)
    time.sleep(0.3)
    assert len(turns) == 1  # idle: it waits (SWEEP_SECONDS)
    sent = time.monotonic()
    with db.cursor() as cur:
        runner.notify(cur, "test")  # autocommit: delivered now
    assert until(lambda: len(turns) == 2, 3)
    assert turns[1] - sent < 1.5
    active.stop()
    active.thread.join(timeout=5)


def test_a_notify_from_another_process_during_a_turn_still_wakes_the_leader(schema, monkeypatch):
    """Work another process records while the turn runs (the worker's song
    hook for the server, a web route for the worker) wakes the leader when the
    turn ends idle. The heartbeat before the wait shares the LISTEN connection
    and reads the NOTIFY off it into ``notifies``: on the owner's server a
    forced rebuild waited 35 s, until the next heartbeat's query made the
    socket readable, instead of a second."""
    monkeypatch.setattr(runner, "HEARTBEAT_SECONDS", 30)  # the shipped interval
    _dsn, _name, db = schema
    turns = []

    def turn(_active):
        turns.append(time.monotonic())
        if len(turns) == 1:
            with db.cursor() as cur:  # another process: no local wake
                cur.execute("SELECT pg_notify(%s, 'request')", (runner.NOTIFY_CHANNEL,))
            time.sleep(0.2)  # it reaches the leader's connection during the turn
        return runner.idle_turn(None)

    active = make(schema, "server", turn)
    active.start()
    assert until(lambda: len(turns) == 2, 3)
    assert turns[1] - turns[0] < 1.5
    active.stop()
    active.thread.join(timeout=5)


def test_a_turn_that_worked_runs_the_next_at_once(schema):
    turns = []

    def turn(_active):
        turns.append(1)
        if len(turns) < 3:
            return runner.Turn(True, "working", "runnable_work", None, "credits")
        return runner.idle_turn(None)

    active = make(schema, "server", turn)
    active.start()
    assert until(lambda: len(turns) == 3, 3)
    active.stop()
    active.thread.join(timeout=5)


def test_the_heartbeat_row_shows_the_action_and_counts_a_worker_online(schema):
    _dsn, _name, db = schema
    started = threading.Event()
    release = threading.Event()

    def turn(active):
        active.begin_action("loudness_backfill")
        started.set()
        release.wait(5)
        return runner.Turn(True, "working", "runnable_work", None, "loudness_backfill")

    active = make(schema, "worker", turn)
    active.start()
    assert started.wait(5)
    assert until(lambda: rows(db, "SELECT current_action FROM {T} WHERE role='worker'")
                 == [("loudness_backfill",)])
    with db.cursor() as cur:
        assert runner.worker_online(cur)
    release.set()
    assert until(lambda: rows(db, "SELECT last_action, actions > 0 FROM {T} "
                                  "WHERE role='worker'")[0] == ("loudness_backfill", True))
    active.stop()
    active.thread.join(timeout=5)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {runner.state_table()} SET heartbeat_at=now()-interval '2 minutes'")
        assert not runner.worker_online(cur)
        states = runner.read_states(cur)
    assert states[0]["role"] == "worker" and states[0]["heartbeat_age_seconds"] >= 119


def test_an_action_is_shown_while_it_looks_and_counted_only_when_it_ran():
    active = runner.Runner("server", app=App(), turn=idle, connect=None)
    active.begin_action("credits")
    assert active.action == "credits"
    active.end_action(ran=False)  # it found nothing to do
    assert (active.action, active.last_action, active.actions) == (None, None, 0)
    active.begin_action("loudness_backfill")
    active.end_action()
    assert (active.action, active.last_action, active.actions) == (None, "loudness_backfill", 1)


def test_a_leader_is_new_until_its_first_turn_ends(schema):
    seen = []

    def turn(active):
        seen.append(active.new_leader)
        if len(seen) < 3:
            return runner.Turn(True, "working", "runnable_work", None, "credits")
        return runner.idle_turn(None)

    active = make(schema, "server", turn)
    active.start()
    assert until(lambda: len(seen) >= 3)
    active.stop()
    active.thread.join(timeout=5)
    assert seen[:3] == [True, False, False]


def test_a_changed_plugin_stops_the_runner_and_frees_its_lock(schema):
    installed = {"current": True}
    active = make(schema, "server", idle, still_installed=lambda: installed["current"])
    active.start()
    assert until(lambda: active.leading)
    installed["current"] = False
    active.wake()
    active.thread.join(timeout=5)
    assert not active.thread.is_alive() and not active.leading
    other = make(schema, "server", idle)
    other.start()
    assert until(lambda: other.leading)
    other.stop()
    other.thread.join(timeout=5)


def test_a_failed_turn_restarts_the_loop(schema):
    turns = []

    def turn(_active):
        turns.append(1)
        if len(turns) == 1:
            raise RuntimeError("boom")
        return runner.idle_turn(None)

    active = make(schema, "server", turn)
    active.start()
    assert until(lambda: len(turns) == 2 and active.leading, 5)
    active.stop()
    active.thread.join(timeout=5)


def test_start_is_once_per_process_and_role(schema):
    kwargs = dict(app=App(), turn=idle, connect=lambda name: None, startup_delay=60)
    first = runner.start("server", **kwargs)
    assert runner.start("server", **kwargs) is first
    assert runner.current("server") is first
    first.stop()


def test_the_install_creates_runner_state(migrated_db):
    with migrated_db.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (runner.state_table(),))
        assert cur.fetchone()[0]
    migrated_db.commit()
