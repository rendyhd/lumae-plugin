"""Guard test for LUM-020 (P3-11): dotted paths the host/task-queue persist.

``register(ctx)`` hands the host every task/cron/hook callback as a plain
Python callable. The host and the task queue do not keep that callable in
memory forever: a running worker or a scheduled cron row instead persists
its dotted import path (``module.qualname``) and re-imports it later to run
the job. A refactor that moves one of these functions to another module
without leaving a same-named re-export at its old dotted path silently
breaks every already-queued or already-scheduled job.

This test does two things:

* it proves every callback ``register`` currently hands out resolves back to
  the exact same object via ``getattr(import_module(f.__module__),
  f.__qualname__)`` -- the same lookup the host/queue performs; and
* it pins today's dotted paths as a golden list, generated from the current
  code (not hand typed -- see the module docstring), so a later move that
  changes one of these paths fails this test loudly instead of failing
  silently in production the next time a queued task tries to resolve it.
"""

import importlib

from test_lumae_analysis import load_plugin


class RecordingCtx:
    """Records every callback ``register`` hands to the host."""

    def __init__(self):
        self.recorded = {}

    def add_blueprint(self, blueprint):
        pass

    def set_settings_page(self, endpoint):
        pass

    def add_menu_item(self, *args, **kwargs):
        pass

    def on_install(self, func):
        self.recorded["on_install"] = func

    def on_flask_start(self, func):
        self.recorded["on_flask_start"] = func

    def on_song_analyzed(self, func):
        self.recorded["on_song_analyzed"] = func

    def add_task(self, name, func, queue="default"):
        self.recorded[f"task:{name}"] = func

    def add_cron_task(self, name, func, queue="default"):
        self.recorded[f"cron:{name}"] = func


# Golden list of dotted paths, generated from the current code by calling
# ``register`` with ``RecordingCtx`` above and recording, for every
# callback, ``f"{func.__module__}.{func.__qualname__}"``. Do not hand-edit
# this dict to make a failing test pass -- if a refactor legitimately moves
# one of these callables, re-export it from its old dotted path (a plugin
# package's ``__init__.py`` re-export is enough) so already-persisted task
# and cron rows keep resolving, then regenerate this list.
EXPECTED_DOTTED_PATHS = {
    "cron:analysis_projection": "plugins.LumaeAnalysis.analysis_projection_task",
    "cron:catalog_reconcile": "plugins.LumaeAnalysis.catalog_reconcile_task",
    "cron:catalog_refresh": "plugins.LumaeAnalysis.catalog_refresh_task",
    "cron:music_metadata": "plugins.LumaeAnalysis.music_metadata.run_one",
    "cron:provider_identity_recheck": "plugins.LumaeAnalysis.provider_identity_recheck_task",
    "on_flask_start": "plugins.LumaeAnalysis.observe_provider_identities_on_start",
    "on_install": "plugins.LumaeAnalysis.migrate",
    "on_song_analyzed": "plugins.LumaeAnalysis.analyze_song_hook",
    "task:analysis_projection": "plugins.LumaeAnalysis.analysis_projection_task",
    "task:credits": "plugins.LumaeAnalysis.credits_service.run_one",
    "task:prepare": "plugins.LumaeAnalysis.prepare_lumae_task",
    "task:profile_backfill": "plugins.LumaeAnalysis.profile_backfill_task",
    "task:provider_identity_recheck": "plugins.LumaeAnalysis.provider_identity_recheck_task",
    "task:relationship_preparation": "plugins.LumaeAnalysis.relationship_preparation_task",
}


def test_registered_task_and_hook_paths_resolve_and_are_pinned():
    mod = load_plugin()
    ctx = RecordingCtx()

    mod.register(ctx)

    assert ctx.recorded.keys() == EXPECTED_DOTTED_PATHS.keys()

    resolved = {}
    for key, func in ctx.recorded.items():
        resolved[key] = f"{func.__module__}.{func.__qualname__}"
        target_module = importlib.import_module(func.__module__)
        assert getattr(target_module, func.__qualname__) is func, (
            f"{key} does not resolve back to the same object via "
            f"{func.__module__}.{func.__qualname__} -- a queued task or "
            "cron row persisting this dotted path would fail to resolve it"
        )

    assert resolved == EXPECTED_DOTTED_PATHS
