"""Runner plan RN.1: the hooks start the runners only when asked, and a
runner stops when this process no longer runs the installed plugin (R2)."""
import sys
import types

from test_lumae_analysis import load_plugin


def test_the_runners_stay_off_by_default(monkeypatch):
    mod = load_plugin()
    started = []
    monkeypatch.setattr(mod.runner, "start", lambda role, **kwargs: started.append(role))
    monkeypatch.setattr(mod, "get_setting", lambda key, default=None: default)
    assert mod.start_runner(mod.runner.ROLE_SERVER) is None
    assert started == []


def test_an_enabled_runner_starts_with_the_hosts_connection(monkeypatch):
    mod = load_plugin()
    started = {}
    monkeypatch.setattr(mod.runner, "enabled", lambda: True)
    monkeypatch.setattr(mod.runner, "start", lambda role, **kwargs: started.update(role=role, **kwargs))
    monkeypatch.setitem(sys.modules, "database", types.SimpleNamespace(
        connect_raw=lambda application_name=None: ("connection", application_name)))
    from flask import Flask

    app = Flask(__name__)
    with app.app_context():
        mod.start_runner(mod.runner.ROLE_WORKER)
    assert started["role"] == "worker"
    assert started["app"] is app
    assert started["turn"] is mod.run_runner_turn
    assert started["connect"]("lumae-runner-worker-1") == ("connection", "lumae-runner-worker-1")
    assert started["still_installed"] is mod._runner_code_current


def test_without_an_application_the_runner_does_not_start(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod.runner, "enabled", lambda: True)
    monkeypatch.setattr(mod.runner, "start", lambda role, **kwargs: (_ for _ in ()).throw(AssertionError))
    assert mod.start_runner(mod.runner.ROLE_SERVER) is None  # outside an app context


def _manager(monkeypatch, record, snapshot):
    manager = types.SimpleNamespace(records={"lumae_analysis": record} if record else {},
                                    _boot_snapshot=snapshot)
    monkeypatch.setitem(sys.modules, "plugin.manager", types.SimpleNamespace(plugin_manager=manager))


def test_the_runner_knows_when_its_code_is_no_longer_the_installed_one(monkeypatch):
    mod = load_plugin()

    class Db:
        def commit(self):
            pass

        def rollback(self):
            pass

    monkeypatch.setattr(mod, "get_db", lambda: Db())
    installed = {"checksum": "a", "enabled": True}
    monkeypatch.setitem(sys.modules, "database", types.SimpleNamespace(
        get_plugin=lambda plugin_id, conn=None: installed if plugin_id == "lumae_analysis" else None))
    _manager(monkeypatch, {"enabled": True}, {"lumae_analysis": ("a", True)})
    assert mod._runner_code_current()
    installed["checksum"] = "b"  # upgraded, not restarted yet
    assert not mod._runner_code_current()
    installed.update(checksum="a", enabled=False)  # disabled
    assert not mod._runner_code_current()
    _manager(monkeypatch, {"enabled": False}, {"lumae_analysis": ("a", True)})
    installed["enabled"] = True
    assert not mod._runner_code_current()  # the manager unloaded it
    _manager(monkeypatch, {"enabled": True}, None)
    assert mod._runner_code_current()  # the host does not say


def test_the_hooks_still_do_their_old_work_when_the_runner_fails(monkeypatch):
    mod = load_plugin()
    calls = []
    monkeypatch.setattr(mod, "observe_provider_identities_on_start", lambda: calls.append("observe"))
    monkeypatch.setattr(mod, "report_edge_runtime_on_start", lambda: calls.append("report"))
    monkeypatch.setattr(mod, "start_runner", lambda role: calls.append(role))
    mod.flask_start()
    mod.worker_start()
    assert calls == ["observe", "server", "report", "worker"]

