"""Runner plan R5: catalogue actions in a child process (``action_isolation``).

A real child for each case, with ``action_child_fakes`` standing in for the
plugin; the real plugin in a fresh interpreter for ``child_action``; and the
runner's routing (``_call_action``) with ``run`` replaced.
"""
import contextlib
import os
import time

import pytest

from test_lumae_analysis import load_plugin
from plugins.LumaeAnalysis import action_isolation as isolation

pytestmark = pytest.mark.skipif(os.name != "posix", reason="child processes need POSIX")
FAKES = "action_child_fakes:load"
REAL = "action_child_fakes:load_plugin"


def test_the_child_answers_with_the_entrys_result_at_a_lower_priority():
    result = isolation.run("echo", {"name": "a", "args": [1, "b"]}, loader=FAKES)
    assert result["call"] == {"name": "a", "args": [1, "b"]}
    assert result["pid"] != os.getpid() and result["parent"] == os.getpid()
    assert result["nice"] == min(19, os.nice(0) + isolation.NICE)


def test_an_exception_comes_back_by_name_with_its_reason():
    with pytest.raises(isolation.ChildRaised) as caught:
        isolation.run("deferred", {"reason": "provider_identity_pending"}, loader=FAKES)
    assert (caught.value.type_name, caught.value.reason) == (
        "AnalysisProjectionDeferred", "provider_identity_pending")
    with pytest.raises(isolation.ChildRaised, match="KeyError"):
        isolation.run("boom", {}, loader=FAKES)


def test_a_stray_print_does_not_break_the_answer():
    assert isolation.run("noisy", {}, loader=FAKES) == {"status": "complete"}


def test_a_child_that_dies_fails_the_action_instead_of_running_it_here():
    with pytest.raises(isolation.ChildFailed, match="SIGKILL"):
        isolation.run("crash", {}, loader=FAKES)


def test_a_child_past_its_limit_is_killed():
    started = time.monotonic()
    with pytest.raises(isolation.ChildFailed, match="ran longer"):
        isolation.run("slow", {"seconds": 60}, loader=FAKES, hard_limit=1)
    assert time.monotonic() - started < 30


def test_a_child_that_cannot_load_leaves_the_action_to_the_caller():
    with pytest.raises(isolation.ChildUnavailable, match="no AudioMuse here"):
        isolation.run("echo", {}, loader="action_child_fakes:broken")


def test_outside_audiomuse_there_is_no_child():
    assert not isolation.available()  # the tests load the plugin as plugins.LumaeAnalysis
    with pytest.raises(isolation.ChildUnavailable):
        isolation.run("child_action", {})


def test_the_memory_goes_with_the_child():
    psutil = pytest.importorskip("psutil")
    process = psutil.Process()
    before = process.memory_info().rss
    assert isolation.run("big", {"megabytes": 300}, loader=FAKES) == {"held": 300 * 1024 * 1024}
    assert process.memory_info().rss - before < 50 * 1024 * 1024


def test_the_real_plugin_runs_an_isolated_action_in_a_fresh_interpreter():
    result = isolation.run("child_action", {"name": "project_servers_task", "args": [[]],
                                            "role": "server"}, loader=REAL)
    assert result == {"status": "complete", "projected": 0, "servers": {}}


def test_the_child_runs_only_the_isolated_actions():
    with pytest.raises(isolation.ChildRaised, match="ValueError"):
        isolation.run("child_action", {"name": "migrate", "args": [None], "role": "server"},
                      loader=REAL)


# ------------------------------------------------------------ the routing


@pytest.fixture
def routed(monkeypatch):
    mod = load_plugin()
    calls = []

    def run(entry, call, **_kwargs):
        calls.append((entry, call))
        return {"status": "complete", "via": "child"}

    monkeypatch.setattr(mod.action_isolation, "available", lambda: True)
    monkeypatch.setattr(mod.action_isolation, "run", run)
    monkeypatch.setattr(mod, "_source_binding", lambda _server_id: contextlib.nullcontext())
    return mod, calls


@contextlib.contextmanager
def _in_turn(mod, role="server", event_id=None):
    role_token = mod.runner.ROLE.set(role)
    event_token = mod.reconcile._current_event_id.set(event_id)
    try:
        yield
    finally:
        mod.reconcile._current_event_id.reset(event_token)
        mod.runner.ROLE.reset(role_token)


def test_a_catalogue_action_in_a_runner_turn_runs_in_a_child(routed):
    mod, calls = routed
    with _in_turn(mod, event_id=41):
        result = mod._call_action(mod.wanted_catalog_refresh_task, None, "server-a", "catalog-a")
    assert result == {"status": "complete", "via": "child"}
    assert calls == [("child_action", {"name": "wanted_catalog_refresh_task",
                                       "args": ["server-a", "catalog-a"], "role": "server",
                                       "server_id": None, "event_id": 41})]


def test_other_actions_and_calls_outside_a_runner_run_here(routed):
    mod, calls = routed

    def profile_backfill_task(*args):
        return {"status": "here", "args": list(args)}

    def wanted_catalog_refresh_task(*args):
        return {"status": "here"}

    with _in_turn(mod):
        assert mod._call_action(profile_backfill_task, "s", 1) == {"status": "here", "args": [1]}
    assert mod._call_action(wanted_catalog_refresh_task, None, "s", "c") == {"status": "here"}
    assert calls == []


def test_a_deferral_in_the_child_is_a_deferral_here(routed, monkeypatch):
    mod, _calls = routed

    def raising(entry, call, **_kwargs):
        raise mod.action_isolation.ChildRaised("AnalysisProjectionDeferred", "held",
                                               "provider_identity_pending")

    monkeypatch.setattr(mod.action_isolation, "run", raising)
    with _in_turn(mod), pytest.raises(mod.AnalysisProjectionDeferred) as caught:
        mod._call_action(mod.finalize_analysis_run_task, "s", "s", "c", "r")
    assert caught.value.reason == "provider_identity_pending"


def test_a_child_that_cannot_start_leaves_the_action_to_the_runner(routed, monkeypatch):
    mod, _calls = routed

    def unavailable(entry, call, **_kwargs):
        raise mod.action_isolation.ChildUnavailable("no interpreter")

    def prepare_lumae_task(server_id, catalog_instance_id):
        return {"status": "here"}

    monkeypatch.setattr(mod.action_isolation, "run", unavailable)
    with _in_turn(mod):
        assert mod._call_action(prepare_lumae_task, "s", "s", "c") == {"status": "here"}


def test_a_child_that_died_fails_the_action(routed, monkeypatch):
    mod, _calls = routed

    def died(entry, call, **_kwargs):
        raise mod.action_isolation.ChildFailed("the child exited before it answered (SIGKILL)")

    monkeypatch.setattr(mod.action_isolation, "run", died)
    with _in_turn(mod), pytest.raises(mod.action_isolation.ChildFailed):
        mod._call_action(mod.provider_identity_recheck_task, None, "s")


def test_child_action_carries_the_role_event_and_binding(monkeypatch):
    mod = load_plugin()
    seen = {}

    @contextlib.contextmanager
    def binding(server_id):
        seen["bound"] = server_id
        yield

    def project_servers_task(servers):
        seen.update(role=mod.runner.ROLE.get(), event=mod.reconcile._current_event_id.get(),
                    servers=servers)
        return {"status": "complete"}

    monkeypatch.setattr(mod, "_source_binding", binding)
    monkeypatch.setattr(mod, "project_servers_task", project_servers_task)
    assert mod.child_action("project_servers_task", [["s"]], "worker", "server-a", 7) == {
        "status": "complete"}
    assert seen == {"bound": "server-a", "role": "worker", "event": 7, "servers": ["s"]}
    assert mod.runner.ROLE.get() is None and mod.reconcile._current_event_id.get() is None
