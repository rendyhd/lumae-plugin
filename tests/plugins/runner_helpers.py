"""Running one runner turn in a test (runner plan RN.6), and what the runner
lifecycle tests share: a schema of their own, a stand-in app, a polling wait."""
import contextlib
import os
import threading
import time
import uuid

import pytest


class Active:
    """What a turn reads of its runner (runner.Runner)."""

    def __init__(self, role="server"):
        self.role = role
        self.stopped = threading.Event()
        self.new_leader = False
        self.begun = []  # every action shown
        self.ran = []  # the ones that ran
        self.action = None

    def begin_action(self, action):
        self.begun.append(action)
        self.action = action

    def end_action(self, ran=True):
        if ran and self.action:
            self.ran.append(self.action)
        self.action = None


def turn(mod, role="server"):
    """``(Turn, actions)``: one ``run_runner_turn`` of ``role`` and the turn's
    per-action report (``TurnReport.actions``)."""
    reports = []
    real = mod.TurnReport

    class Capture(real):
        def __init__(self):
            super().__init__()
            reports.append(self)

    mod.TurnReport = Capture
    token = mod.runner.ROLE.set(role)  # as runner.Runner._turn does
    try:
        result = mod.run_runner_turn(Active(role))
    finally:
        mod.runner.ROLE.reset(token)
        mod.TurnReport = real
    return result, (reports[0].actions if reports else [])


@pytest.fixture
def schema():
    """``(dsn, schema name, autocommit connection)``, with the runner's own tables."""
    # The test module has loaded the plugin; a fixture only runs after that.
    from plugins.LumaeAnalysis import runner

    psycopg2 = pytest.importorskip("psycopg2")
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


class App:
    def app_context(self):
        return contextlib.nullcontext()


def until(condition, seconds=5.0):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False
