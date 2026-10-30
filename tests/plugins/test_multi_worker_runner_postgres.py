"""Multi-worker plan (2026-10-30) R1, R2: helpers on a real PostgreSQL.

With ``can_help()`` true a worker runner that misses the role's lock helps
instead of standing by: its turns run with ``helping`` (and ``HELPING``) set,
it heartbeats a row of its own in ``runner_workers`` and never the role's
``runner_state`` row, it leads once the leader goes away, and it stands by
again once helping is switched off. Without ``can_help`` a second worker
stands by, as in 1.9.
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
    name = f"lumae_helpers_{uuid.uuid4().hex}"
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
    monkeypatch.setattr(runner, "HEARTBEAT_SECONDS", 0.3)
    monkeypatch.setattr(runner, "PAUSE_SECONDS", 0.05)
    monkeypatch.setattr(runner, "RESTART_DELAYS", (0.2,))


class App:
    def app_context(self):
        return contextlib.nullcontext()


def make(schema, turn, **kwargs):
    dsn, name, _db = schema

    def connect(application_name):
        return psycopg2.connect(dsn, application_name=application_name,
                                options=f"-c search_path={name},public")

    return runner.Runner("worker", app=App(), turn=turn, connect=connect, startup_delay=0, **kwargs)


def until(condition, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


def stop(*actives):
    for active in actives:
        active.stop()
    for active in actives:
        active.thread.join(timeout=5)


def workers(db):
    with db.cursor() as cur:
        return {row["runner_id"]: row for row in runner.read_workers(cur)}


def role_row(db):
    with db.cursor() as cur:
        cur.execute(f"SELECT current_action, actions FROM {runner.state_table()} WHERE role='worker'")
        return cur.fetchone()


class Recorder:
    """A turn that records who ran it and how; ``hold`` keeps helper turns busy."""

    def __init__(self):
        self.seen = []
        self.hold = threading.Event()
        self.hold.set()

    def __call__(self, active):
        self.seen.append({"runner_id": active.runner_id, "helping": active.helping,
                          "HELPING": runner.HELPING.get(), "RUNNER_ID": runner.RUNNER_ID.get(),
                          "leading": active.leading})
        if active.helping:
            active.begin_action("helper-pass")
            self.hold.wait(5)
        return runner.idle_turn(0.05)

    def of(self, active):
        return [entry for entry in self.seen if entry["runner_id"] == active.runner_id]


def test_with_help_a_worker_that_misses_the_lock_helps_and_keeps_its_own_row(schema):
    _dsn, _name, db = schema
    turns = Recorder()
    leader = make(schema, turns, can_help=lambda: True)
    leader.start()
    assert until(lambda: leader.leading)
    turns.hold.clear()
    helper = make(schema, turns, can_help=lambda: True)
    helper.start()
    assert until(lambda: turns.of(helper))
    entry = turns.of(helper)[0]
    assert entry["helping"] and entry["HELPING"] and not entry["leading"]
    assert entry["RUNNER_ID"] == helper.runner_id != leader.runner_id
    assert all(not e["helping"] and not e["HELPING"] and e["leading"] for e in turns.of(leader))
    # The helper's row shows its action; the role's row stays the leader's.
    assert until(lambda: workers(db).get(helper.runner_id, {}).get("current_action") == "helper-pass")
    rows = workers(db)
    assert rows[leader.runner_id]["leader"] and not rows[helper.runner_id]["leader"]
    assert role_row(db)[0] != "helper-pass"
    turns.hold.set()
    stop(helper, leader)


def test_without_help_a_second_worker_stands_by(schema):
    turns = Recorder()
    leader = make(schema, turns)
    leader.start()
    assert until(lambda: leader.leading)
    second = make(schema, turns)
    second.start()
    assert until(lambda: second.mode == "standby")
    time.sleep(0.3)
    assert not turns.of(second)
    stop(second, leader)


def test_a_helper_leads_once_the_leader_goes_away(schema):
    turns = Recorder()
    leader = make(schema, turns, can_help=lambda: True)
    leader.start()
    assert until(lambda: leader.leading)
    helper = make(schema, turns, can_help=lambda: True)
    helper.start()
    assert until(lambda: turns.of(helper))
    stop(leader)
    assert until(lambda: helper.leading)
    assert until(lambda: turns.of(helper)[-1]["leading"] and not turns.of(helper)[-1]["helping"])
    assert helper.new_leader is False  # its first turn as leader ran
    stop(helper)


def test_a_helper_stands_by_once_helping_is_switched_off(schema):
    turns = Recorder()
    allowed = {"help": True}
    leader = make(schema, turns, can_help=lambda: allowed["help"])
    leader.start()
    assert until(lambda: leader.leading)
    helper = make(schema, turns, can_help=lambda: allowed["help"])
    helper.start()
    assert until(lambda: turns.of(helper))
    allowed["help"] = False
    assert until(lambda: helper.mode == "standby" and not helper.helping)
    count = len(turns.of(helper))
    time.sleep(0.4)
    assert len(turns.of(helper)) == count
    stop(helper, leader)


def test_a_helper_that_cannot_read_the_setting_stands_by(schema):
    turns = Recorder()
    leader = make(schema, turns)
    leader.start()
    assert until(lambda: leader.leading)

    def broken():
        raise RuntimeError("settings unreadable")

    second = make(schema, turns, can_help=broken)
    second.start()
    assert until(lambda: second.mode == "standby")
    assert not turns.of(second)
    stop(second, leader)


def test_rows_of_runners_gone_for_a_day_are_removed(schema):
    _dsn, _name, db = schema
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {runner.workers_table()} (runner_id, role, heartbeat_at) VALUES "
                    "('gone', 'worker', now() - interval '2 days'), ('recent', 'worker', now())")
        assert runner.prune_workers(cur) == 1
        assert [row["runner_id"] for row in runner.read_workers(cur)] == ["recent"]


def test_a_runner_id_names_its_host_and_process():
    first, second = runner.new_runner_id(), runner.new_runner_id()
    assert first != second
    assert first.split(":")[1] == str(os.getpid())
    assert runner.current_runner_id() == runner.current_runner_id()
    token = runner.RUNNER_ID.set("runner-x")
    try:
        assert runner.current_runner_id() == "runner-x"
    finally:
        runner.RUNNER_ID.reset(token)
