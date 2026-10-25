"""Runner plan RN.2: a runner's turn over today's actions.

R1: each role runs its lane, the server takes heavy work only as
``heavy_work`` and the worker's heartbeat allow. R4: a runner steps aside for
AudioMuse's jobs (the worker for all of them, the server for those that
rewrite track IDs; per-track passes while AudioMuse analyses), and the
projection checks again at its commit. R5: the web process analyses at most
two files at once and downloads outside TEMP_DIR. R8: the own-download
refusal expires.
"""
import threading
import time

import pytest

from test_reconcile_scheduler_postgres import P, SERVER, SOURCE, _rows, tick  # noqa: F401
from plugins.LumaeAnalysis import reconcile, runner


class Active:
    """What a turn reads of its runner."""

    def __init__(self, role):
        self.role = role
        self.stopped = threading.Event()
        self.begun = []

    def begin_action(self, action):
        self.begun.append(action)

    def end_action(self):
        pass


@pytest.fixture
def turn(tick, monkeypatch):
    mod, db = tick
    calls = []
    monkeypatch.setattr(runner, "PAUSE_SECONDS", 0)
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *a, **k: calls.append("report_edge_runtime"))
    credits = {"left": 1}

    def credit_turn(db, server_id, before_background, deadline=None):
        if before_background or not credits["left"]:
            return None
        credits["left"] -= 1
        calls.append("credits")
        return {"status": "published", "catalog_instance_id": SOURCE}

    monkeypatch.setattr(mod, "_safe_credits_reconcile", credit_turn)

    def relationship(db=None, server_id=None):
        calls.append("relationships")
        return None

    monkeypatch.setattr(mod, "next_relationship_run", relationship)
    monkeypatch.setattr(mod, "serve_deferred_interactive", lambda *a, **k: calls.append("interactive") or 0)
    settings = {}
    real = mod.get_setting
    monkeypatch.setattr(mod, "get_setting",
                        lambda key, default=None: settings.get(key, real(key, default)))
    return mod, db, calls, settings


def _worker_heartbeat(db, age_seconds=0):
    with db.cursor() as cur:
        cur.execute(f"""INSERT INTO {runner.state_table()} (role, mode, heartbeat_at)
            VALUES ('worker', 'idle', now() - make_interval(secs => %s))
            ON CONFLICT (role) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at""", (age_seconds,))
    db.commit()


def _live(db, task_type, status="RUNNING"):
    with db.cursor() as cur:
        cur.execute("INSERT INTO task_status (task_id, parent_task_id, task_type, status) "
                    "VALUES (%s, NULL, %s, %s)", (f"t-{task_type}", task_type, status))
    db.commit()


@pytest.mark.parametrize("role, setting, worker_online, heavy, light", [
    ("server", "auto", True, set(), True),
    ("server", "auto", False, {"interactive"}, True),
    ("server", "server", True, {"interactive", "relationships"}, True),
    ("server", "worker", False, set(), True),
    ("worker", "auto", True, {"interactive", "relationships"}, False),
    ("worker", "worker", True, {"interactive", "relationships"}, False),
    ("worker", "server", True, set(), False),
])
def test_each_runner_runs_its_lane(turn, role, setting, worker_online, heavy, light):
    mod, db, calls, settings = turn
    settings["heavy_work"] = setting
    if worker_online:
        _worker_heartbeat(db)
    result = mod.run_runner_turn(Active(role))
    assert {"interactive", "relationships"} & set(calls) == heavy
    assert ("credits" in calls) is light
    assert result.worked is light
    assert ("report_edge_runtime" in calls) is (role == "worker" and setting != "server")


def test_a_stale_worker_heartbeat_is_no_worker(turn):
    mod, db, calls, _settings = turn
    _worker_heartbeat(db, age_seconds=runner.WORKER_ONLINE_SECONDS + 5)
    mod.run_runner_turn(Active("server"))
    assert "interactive" in calls and "relationships" not in calls


@pytest.mark.parametrize("task_type, server_waits, worker_waits", [
    ("cleaning", True, True),
    ("provider_migration", True, True),
    ("server_sweep", True, True),
    ("main_clustering", False, True),
    ("main_analysis", False, True),
    ("provider_migration_planner", False, False),
])
def test_runners_step_aside_for_audiomuse(turn, task_type, server_waits, worker_waits):
    mod, db, _calls, _settings = turn
    _live(db, task_type)
    server = mod.run_runner_turn(Active("server"))
    worker = mod.run_runner_turn(Active("worker"))
    assert (server.mode == "waiting_host") is server_waits
    assert (worker.mode == "waiting_host") is worker_waits
    if server_waits:
        assert server.reason == f"audiomuse:{task_type}"
        assert server.next_due_seconds == mod.RUNNER_HOST_WAIT_SECONDS


def test_a_finished_job_holds_nothing_back(turn):
    mod, db, calls, _settings = turn
    _live(db, "cleaning", status="SUCCESS")
    assert mod.run_runner_turn(Active("server")).worked
    assert "credits" in calls


def test_per_track_passes_wait_while_audiomuse_analyses(turn, monkeypatch):
    mod, db, calls, settings = turn
    settings["heavy_work"] = "server"
    passes = []
    monkeypatch.setattr(mod, "next_profile_backfill_run",
                        lambda db=None, server_id=None: passes.append("profile") or None)
    _live(db, "main_analysis")
    mod.run_runner_turn(Active("server"))
    assert passes == [] and "credits" in calls and "relationships" in calls
    with db.cursor() as cur:
        cur.execute("UPDATE task_status SET status='SUCCESS'")
    db.commit()
    mod.run_runner_turn(Active("server"))
    assert passes == ["profile"]


def test_a_paused_plugin_does_nothing(turn, monkeypatch):
    mod, _db, calls, _settings = turn
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    assert mod.run_runner_turn(Active("server")).mode == "paused"
    assert calls == []


def test_an_idle_turn_sleeps_until_the_soonest_retry(turn, monkeypatch):
    mod, _db, _calls, _settings = turn
    monkeypatch.setattr(mod, "_safe_credits_reconcile", lambda *a, **k: None)
    monkeypatch.setattr(mod.reconcile, "_work_summary", lambda cur: {
        "ready": 0, "waiting": 0, "retry_count": 1, "next_retry_at": None, "retry_in_seconds": 420.0})
    result = mod.run_runner_turn(Active("server"))
    assert (result.worked, result.mode, result.next_due_seconds) == (False, "idle", 420.0)
    monkeypatch.setattr(mod.reconcile, "_work_summary", lambda cur: {
        "ready": 0, "waiting": 1, "retry_count": 0, "next_retry_at": None, "retry_in_seconds": None})
    assert mod.run_runner_turn(Active("server")).next_due_seconds == mod.RUNNER_WAITING_SECONDS


def test_the_turn_shows_the_action_it_runs(turn):
    mod, _db, _calls, _settings = turn
    active = Active("server")
    mod.run_runner_turn(active)
    assert active.begun == ["credits"]


def test_a_failing_action_does_not_stop_the_turn(turn, monkeypatch):
    mod, _db, calls, _settings = turn

    def broken(db=None, server_id=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_safe_next_wanted_catalog_refresh", lambda db, server_id: ("s", "c"))
    monkeypatch.setattr(mod, "wanted_catalog_refresh_task", lambda *a: (_ for _ in ()).throw(RuntimeError("boom")))
    result = mod.run_runner_turn(Active("server"))
    assert result.worked and "credits" in calls  # the refresh failed, the rest ran


def test_the_web_process_analyses_two_files_at_most(monkeypatch):
    from test_lumae_analysis import load_plugin

    mod = load_plugin()
    monkeypatch.setattr(mod.parallel_analysis, "auto_workers", lambda: 6)
    monkeypatch.setattr(mod, "get_setting", lambda key, default=None: default)
    assert mod.background_workers() == 6
    token = runner.ROLE.set(runner.ROLE_SERVER)
    try:
        assert mod.background_workers() == mod.WEB_AUTO_WORKERS
        monkeypatch.setattr(mod, "get_setting",
                            lambda key, default=None: 4 if key == "background_workers" else default)
        assert mod.background_workers() == 4  # the administrator's number stands
    finally:
        runner.ROLE.reset(token)
    token = runner.ROLE.set(runner.ROLE_WORKER)
    try:
        monkeypatch.setattr(mod, "get_setting", lambda key, default=None: default)
        assert mod.background_workers() == 6
    finally:
        runner.ROLE.reset(token)


def test_a_runner_downloads_outside_temp_dir(monkeypatch, tmp_path):
    from test_lumae_analysis import load_plugin

    mod = load_plugin()
    monkeypatch.setattr(mod.config, "TEMP_DIR", str(tmp_path / "temp"), raising=False)
    assert mod._host_download_directory() == str(tmp_path / "temp")
    token = runner.ROLE.set(runner.ROLE_SERVER)
    try:
        directory = mod._host_download_directory()
    finally:
        runner.ROLE.reset(token)
    assert "lumae-analysis" in directory and not directory.startswith(str(tmp_path / "temp"))


def test_the_own_download_refusal_expires(monkeypatch):
    from test_lumae_analysis import load_plugin

    mod = load_plugin()
    mod._own_download_refused.clear()
    now = {"t": 1000.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: now["t"])
    mod._refuse_own_download("c", "test")
    assert mod._own_download_refused_now("c")
    now["t"] += mod.OWN_DOWNLOAD_RETRY_SECONDS - 1
    assert mod._own_download_refused_now("c")
    now["t"] += 2
    assert not mod._own_download_refused_now("c")
    mod._own_download_refused.clear()


def test_the_commit_check_sees_a_live_job_and_a_held_migration_lock(migrated_db, second_connection):
    db = migrated_db
    with db.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS task_status (task_id TEXT, parent_task_id TEXT, "
                    "task_type TEXT, status TEXT)")
        assert reconcile.id_rewrite_in_progress(cur) is None
        cur.execute("INSERT INTO task_status VALUES ('t', NULL, 'cleaning', 'NEW')")
        assert reconcile.id_rewrite_in_progress(cur) == "cleaning"
        cur.execute("DELETE FROM task_status")
    db.commit()
    other = second_connection
    with other.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (reconcile.PROVIDER_MIGRATION_LOCK_KEY,))
    other.commit()
    with db.cursor() as cur:
        assert reconcile.id_rewrite_in_progress(cur) == "provider_migration"
        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (reconcile.PROVIDER_MIGRATION_LOCK_KEY,))
        assert cur.fetchone()[0] is False  # still the migration's: the probe took nothing
    db.rollback()
    with other.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (reconcile.PROVIDER_MIGRATION_LOCK_KEY,))
    other.commit()
    with db.cursor() as cur:
        assert reconcile.id_rewrite_in_progress(cur) is None
    db.commit()


def test_the_projection_check_fails_open_without_task_status(migrated_db):
    from plugins.LumaeAnalysis import catalog_analysis

    db = migrated_db
    token = runner.ROLE.set(runner.ROLE_SERVER)
    try:
        with db.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS task_status")
            assert catalog_analysis._id_rewrite_in_progress(cur) is None
            cur.execute("SELECT 1")  # the transaction is still usable
        db.rollback()
    finally:
        runner.ROLE.reset(token)


def test_the_projection_checks_only_in_a_runner_turn(migrated_db):
    from plugins.LumaeAnalysis import catalog_analysis

    db = migrated_db
    with db.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS task_status (task_id TEXT, parent_task_id TEXT, "
                    "task_type TEXT, status TEXT)")
        cur.execute("INSERT INTO task_status VALUES ('t', NULL, 'cleaning', 'RUNNING')")
        assert catalog_analysis._id_rewrite_in_progress(cur) is None  # a queued task
        token = runner.ROLE.set(runner.ROLE_SERVER)
        try:
            assert catalog_analysis._id_rewrite_in_progress(cur) == "cleaning"
        finally:
            runner.ROLE.reset(token)
    db.rollback()
