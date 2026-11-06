"""Multi-worker plan (2026-10-30): the ``worker_runners`` and ``max_files_in_flight``
settings, the helper permission they give, and the status page's worker lines."""
import pytest

from test_lumae_analysis import load_plugin, plugin_client


@pytest.fixture
def settings(monkeypatch):
    mod = load_plugin()
    stored, saved, woken = {}, [], []

    def set_setting(key, value):
        saved.append((key, value))
        stored[key] = value

    monkeypatch.setattr(mod, "set_setting", set_setting)
    monkeypatch.setattr(mod, "get_setting", lambda key, default=None: stored.get(key, default))
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "wake_runners", lambda _db, reason, commit=False: woken.append(reason))
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 25)
    monkeypatch.setattr(mod, "render_settings_status_panels", lambda _batch_size: dict.fromkeys((
        "readiness", "relationships", "catalogue", "waveform", "reconcile", "identity",
        "stream_status"), ""))
    monkeypatch.setattr(mod, "render_publication_repair_form", lambda: "")
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    return mod, stored, saved, woken


def test_one_worker_is_the_default_and_gives_no_helper(settings):
    mod, _stored, _saved, _woken = settings
    assert mod.worker_runners_setting() == "one"
    assert mod.max_files_in_flight() == mod.DEFAULT_MAX_FILES_IN_FLIGHT == 16
    assert not mod.shared_passes()
    assert not mod._worker_may_help()


def test_every_worker_is_saved_with_its_limit_and_wakes_the_runners(settings):
    mod, stored, saved, woken = settings

    response = plugin_client(mod).post("/settings", data={
        "action": "save_worker_runners", "worker_runners": "Every", "max_files_in_flight": "12"})

    body = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Every AudioMuse worker analyzes library tracks, at most 12 files at once" in body
    assert saved == [("worker_runners", "every"), ("max_files_in_flight", 12)]
    assert woken == ["worker_runners_changed"]
    assert '<option value="every" selected>' in body
    assert 'name="max_files_in_flight" value="12"' in body
    assert mod.shared_passes() and mod._worker_may_help()
    # With heavy work on the server the worker lane is empty: nobody helps.
    stored["heavy_work"] = "server"
    assert not mod._worker_may_help()


def test_an_unknown_choice_is_refused_and_nothing_is_saved(settings):
    mod, _stored, saved, woken = settings

    response = plugin_client(mod).post("/settings", data={
        "action": "save_worker_runners", "worker_runners": "all", "max_files_in_flight": "4"})

    assert response.status_code == 200
    assert "Worker runners must be one or every." in response.get_data(as_text=True)
    assert saved == [] and woken == []


@pytest.mark.parametrize(("raw", "value"), [
    ("0", 1), ("1", 1), ("64", 64), ("500", 64), ("", 16), (None, 16), ("x", 16)])
def test_the_files_in_flight_limit_is_kept_in_range(raw, value):
    mod = load_plugin()
    assert mod.normalize_max_files_in_flight(raw) == value


def _worker(runner_id, leader, age, **extra):
    return {"runner_id": runner_id, "role": "worker", "leader": leader, "host": f"host-{runner_id}",
            "pid": 7, "mode": "working", "reason": "helper", "current_action": None,
            "actions": 3, "files_at_once": 2, "heartbeat_age_seconds": age, **extra}


def test_the_status_page_lists_every_worker_runner_once_several_run(settings):
    mod, _stored, _saved, _woken = settings
    render = mod.settings_render._worker_runners_html
    workers = [
        _worker("a", True, 5, current_action="relationships"),
        _worker("b", False, 10, current_action="edge_backfill"),
        _worker("c", False, 400),
    ]

    body = render(workers)

    assert "<strong>Workers</strong>: 2 online." in body
    assert "host-a (pid 7), leads, 2 files at once, 3 actions: working: relationships." in body
    assert "host-b (pid 7), helps, 2 files at once, 3 actions: working: edge backfill." in body
    assert "host-c (pid 7), helps" in body and "gone (last seen" in body


def test_the_status_page_shows_no_worker_lines_for_one_worker(settings):
    mod, _stored, _saved, _woken = settings
    assert mod.settings_render._worker_runners_html([_worker("a", True, 5)]) == ""
