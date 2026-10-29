"""Performance plan WP1b: the ``background_workers`` setting (files analysed at
once by a reconcile tick): the settings form shows and saves it, and the tick
reads it."""
import pytest

from test_lumae_analysis import load_plugin, plugin_client, settings_catalog_source
from plugins.LumaeAnalysis import parallel_analysis as par


def _settings_page(mod, monkeypatch, stored):
    monkeypatch.setattr(mod, "get_setting", lambda key, default=None: stored.get(key, default))
    monkeypatch.setattr(mod, "set_setting", lambda key, value: stored.__setitem__(key, value))
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [settings_catalog_source()])
    monkeypatch.setattr(mod, "preparation_state", lambda _catalog_id: None)
    monkeypatch.setattr(mod, "profile_backfill_state", lambda _catalog_id: None)
    monkeypatch.setattr(mod, "analysis_status_counts", lambda **_kwargs: {
        "total_with_files": 10, "ready_current": 1, "pending": 0, "failed": 0, "skipped": 0,
        "needs_analysis": 9})
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    return plugin_client(mod)


@pytest.mark.parametrize("stored,shown", [({}, "auto"), ({"background_workers": 3}, "3")])
def test_the_settings_page_shows_the_setting(monkeypatch, stored, shown):
    mod = load_plugin()
    body = _settings_page(mod, monkeypatch, dict(stored)).get("/settings").get_data(as_text=True)
    assert f"Files analyzed at once (auto or 1-{par.MAX_WORKERS})" in body
    assert f'name="background_workers" value="{shown}"' in body


@pytest.mark.parametrize("sent,saved", [("3", 3), ("auto", "auto"), ("many", "auto"), ("40", par.MAX_WORKERS)])
def test_saving_stores_it_normalized(monkeypatch, sent, saved):
    mod = load_plugin()
    stored = {}
    client = _settings_page(mod, monkeypatch, stored)
    response = client.post("/settings", data={"action": "save", "backfill_batch_size": "3",
                                              "background_workers": sent})
    assert response.status_code == 200
    assert stored["background_workers"] == saved
    assert mod.configured_background_workers() == saved


def test_a_form_without_the_field_leaves_it(monkeypatch):
    mod = load_plugin()
    stored = {"background_workers": 2}
    _settings_page(mod, monkeypatch, stored).post("/settings", data={"action": "save", "backfill_batch_size": "3"})
    assert stored["background_workers"] == 2


def test_the_tick_reads_it_and_falls_back_to_one(monkeypatch):
    mod = load_plugin()
    stored = {"background_workers": 2}
    monkeypatch.setattr(mod, "get_setting", lambda key, default=None: stored.get(key, default))
    assert mod.background_workers() == 2
    stored["background_workers"] = "auto"
    assert mod.background_workers() == 1  # the suite's autouse fixture: one core past the reserve
    monkeypatch.setattr(mod, "get_setting", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db")))
    assert mod.background_workers() == 1


def test_the_turn_is_told_the_setting_behind_k(monkeypatch):
    mod = load_plugin()
    stored = {"background_workers": 3}
    monkeypatch.setattr(mod, "get_setting", lambda key, default=None: stored.get(key, default))
    assert mod.background_sizing() == (3, "setting 3")
    stored["background_workers"] = "auto"
    monkeypatch.setattr(par, "available_cpus", lambda: 16)
    monkeypatch.setattr(par, "memory_bytes", lambda: 64 << 30)
    assert mod.background_sizing() == (par.MAX_AUTO_WORKERS, "setting auto")
    token = mod.runner.ROLE.set(mod.runner.ROLE_SERVER)
    try:
        assert mod.background_sizing() == (
            mod.WEB_AUTO_WORKERS, f"setting auto, at most {mod.WEB_AUTO_WORKERS} in AudioMuse's web process")
    finally:
        mod.runner.ROLE.reset(token)
    monkeypatch.setattr(mod, "get_setting", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db")))
    assert mod.background_sizing() == (1, "the setting could not be read")


def test_a_parallel_tick_claims_k_times_the_batch(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)
    assert mod.background_batch_limit() == 3
    with par.tick_scope(4):
        assert mod.background_batch_limit() == 4 * mod.MAX_BACKFILL_BATCH_SIZE
    with par.tick_scope(1):
        assert mod.background_batch_limit() == 3
