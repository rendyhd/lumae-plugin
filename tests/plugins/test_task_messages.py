"""Every Lumae schedule says what it did (LA-3).

AudioMuse takes ``status_message``, then ``message``, as a task's message, and
falls back to "Plugin task completed successfully." when a plugin task returns
neither. Hosts that build a Recent tasks note from ``tracks_analyzed`` still
do; these tasks do not set it.
"""
import types

import pytest

from test_lumae_analysis import load_plugin
from plugins.LumaeAnalysis import reconcile  # noqa: E402


def _assert_sentence(result, expected):
    assert result["message"] == expected
    assert result["status_message"] == expected
    assert len(expected) <= reconcile.TICK_MESSAGE_MAX
    assert "tracks_analyzed" not in result


@pytest.fixture
def mod(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    adapter = types.SimpleNamespace(mode="v3_registry", active_server_id=lambda: "server-a")
    monkeypatch.setattr(mod, "get_core_adapter", lambda: adapter)
    return mod


def _quiet_catalog_refresh(monkeypatch, mod, result):
    monkeypatch.setattr(mod, "refresh_catalog", lambda server_id=None: dict(result))
    monkeypatch.setattr(mod, "get_db", lambda: types.SimpleNamespace(commit=lambda: None))
    monkeypatch.setattr(mod, "find_backfill_ids", lambda *_a, **_k: [])
    monkeypatch.setattr(mod, "next_profile_retry_at", lambda *_a: None)
    monkeypatch.setattr(mod, "arm_edge_backfill", lambda *_a: False)
    monkeypatch.setattr(mod, "wake_profile_backfill_after_catalog_refresh", lambda *_a: None)
    monkeypatch.setattr(mod.optional_storage, "prune", lambda _db: None)
    monkeypatch.setattr(mod, "refresh_status_snapshot", lambda *_a, **_k: None)


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ({"change_reason": "provider_diff", "changes": 5471,
          "counts": {"track": 36275, "album": 2283}},
         "Lumae: catalogue refreshed, 5,471 changes, 36,275 tracks"),
        ({"change_reason": "provider_diff", "changes": 1, "counts": {"track": 1}},
         "Lumae: catalogue refreshed, 1 change, 1 track"),
        ({"change_reason": "no_change", "changes": 0, "counts": {"track": 36275}},
         "Lumae: catalogue unchanged, 36,275 tracks"),
        ({"change_reason": "provider_identity_wait", "changes": 0, "counts": {}},
         "Lumae: catalogue held for a second scan of the media server ID change"),
    ],
)
def test_catalog_refresh_says_what_changed(monkeypatch, mod, result, expected):
    _quiet_catalog_refresh(monkeypatch, mod, {
        "catalog_instance_id": "catalog-a", "server_id": "server-a", **result})

    task_result = mod.catalog_refresh_task("server-a")

    _assert_sentence(task_result, expected)
    # The refresh's own result is still all there.
    assert task_result["change_reason"] == result["change_reason"]


def test_catalog_refresh_says_why_it_did_nothing(monkeypatch, mod):
    monkeypatch.setattr(mod, "_resolve_task_server_id", lambda *_a: None)
    _assert_sentence(mod.catalog_refresh_task("legacy-default"),
                     "Lumae: catalogue refresh skipped; the source awaits rebinding")
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    _assert_sentence(mod.catalog_refresh_task("server-a"), "Lumae: paused")


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ({"changes": 12, "ready_count": 34438},
         "Lumae: analysis projected, 12 changes, 34,438 tracks ready"),
        ({"changes": 0, "unchanged": True, "ready_count": 34438},
         "Lumae: analysis projection unchanged, 34,438 tracks ready"),
    ],
)
def test_analysis_projection_says_what_changed(monkeypatch, mod, result, expected):
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "project_analysis", lambda **_k: {
        "catalog_instance_id": "catalog-a", "server_id": "server-a", **result})
    monkeypatch.setattr(mod, "complete_projection_reconcile", lambda *_a: None)
    monkeypatch.setattr(mod, "start_relationship_preparation",
                        lambda **_k: {"queued": False, "coalesced": True})

    _assert_sentence(mod.analysis_projection_task("server-a"), expected)


def test_analysis_projection_deferral_is_one_short_sentence(monkeypatch, mod):
    from plugins.LumaeAnalysis.catalog_analysis import AnalysisProjectionDeferred

    monkeypatch.setattr(mod, "get_db", lambda: types.SimpleNamespace(rollback=lambda: None))
    monkeypatch.setattr(mod, "project_analysis", lambda **_k: (_ for _ in ()).throw(
        AnalysisProjectionDeferred("Navidrome did not answer", "provider_identity_unverified")))
    _assert_sentence(mod.analysis_projection_task("server-a"),
                     "Lumae: analysis projection deferred: Navidrome did not answer")

    monkeypatch.setattr(mod, "project_analysis", lambda **_k: (_ for _ in ()).throw(
        AnalysisProjectionDeferred("x" * 300, "catalog_not_published")))
    result = mod.analysis_projection_task("server-a")
    assert result["message"] == result["status_message"]
    assert len(result["message"]) == reconcile.TICK_MESSAGE_MAX
    assert result["message"].endswith("…")


@pytest.mark.parametrize(
    ("results", "expected"),
    [
        ([], "Lumae: no media server ID change to recheck"),
        ([{"change_reason": "provider_identity_wait"}],
         "Lumae: rechecked 1 media server ID change"),
        ([{"projection_processed": True}, {"error": "boom"}],
         "Lumae: rechecked 2 media server ID changes, 1 failed"),
    ],
)
def test_provider_identity_recheck_says_what_it_checked(monkeypatch, mod, results, expected):
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "ProviderCatalogBridge", lambda: types.SimpleNamespace(
        list_servers=lambda: [
            {"server_id": f"server-{index}", "supported": True}
            for index in range(len(results))
        ]))
    queue = list(results)
    monkeypatch.setattr(mod, "resolve_catalog_source",
                        lambda *_a, **_k: [{"catalog_instance_id": "catalog-a"}])
    monkeypatch.setattr(mod, "provider_transition_health",
                        lambda *_a: {"state": "transition_pending"})
    monkeypatch.setattr(mod, "refresh_catalog", lambda **_k: queue.pop(0))

    result = mod.provider_identity_recheck_task()

    _assert_sentence(result, expected)
    assert result["checked"] == len(results)


@pytest.mark.parametrize(
    ("deleted", "expected"),
    [
        ((0, 0, 0, 0, 0, 0, 0), "Lumae: no expired collection entries"),
        ((3, 1, 0, 0, 2, 1, 0), "Lumae: removed 7 expired collection entries"),
        ((1, 0, 0, 0, 0, 0, 0), "Lumae: removed 1 expired collection entry"),
        ((0, 0, 0, 0, 0, 0, 2), "Lumae: removed 2 expired collection entries"),
    ],
)
def test_collection_retention_says_what_it_removed(monkeypatch, mod, deleted, expected):
    monkeypatch.setattr(mod, "get_db", lambda: object())
    counts = iter(deleted)
    for name in ("purge_expired_collection_mutations", "purge_stale_collection_restores",
                 "purge_expired_shelf_mutations"):
        monkeypatch.setattr(mod, name, lambda _db, value=next(counts): value)
    monkeypatch.setattr(mod.vibes, "purge_expired_vibe_mutations",
                        lambda _db, value=next(counts): value)
    monkeypatch.setattr(mod.journeys, "purge_expired_journey_mutations",
                        lambda _db, value=next(counts): value)
    monkeypatch.setattr(mod.covers, "purge_expired_cover_mutations",
                        lambda _db, value=next(counts): value)
    monkeypatch.setattr(mod.covers, "purge_unused_cover_images",
                        lambda _db, value=next(counts): value)

    result = mod.collection_retention_task()

    _assert_sentence(result, expected)
    assert result["status"] == "ok"


def test_collection_retention_failure_and_pause_say_so(monkeypatch, mod):
    monkeypatch.setattr(mod, "get_db", lambda: types.SimpleNamespace(rollback=lambda: None))
    monkeypatch.setattr(mod, "purge_expired_collection_mutations",
                        lambda _db: (_ for _ in ()).throw(RuntimeError("boom")))
    _assert_sentence(mod.collection_retention_task(),
                     "Lumae: collection cleanup failed; see the worker log")
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    _assert_sentence(mod.collection_retention_task(), "Lumae: paused")
