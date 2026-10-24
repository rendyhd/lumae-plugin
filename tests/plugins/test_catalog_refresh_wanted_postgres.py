"""A catalogue refresh an app asks for is never lost to a busy queue (SQ.1).

Auralscape RCA 2026-10-24 (docs/new-song-sync-and-queue-end-rca-2026-10-24.md):
AudioMuse refuses a plugin task while another queue job runs, the refresh API
let that refusal out as a 409, and the phone read the old catalogue twice. A
refresh that published a new song also never projected its analysis, so the
phone got the song without an embedding.

* The refresh API keeps a refused refresh (``refresh_wanted_at``), answers
  202 ``deferred`` and arms the watchdog; the tick runs it once the queue is
  free, one a tick, and not while a refresh task is on the queue.
* A refresh task already on the queue serves a new request (``coalesced``).
* A finished scan serves every request made before it started, whichever
  path ran it; a request made during the scan is kept.
* A refresh that changed the catalogue projects the analysis inline.
"""
import importlib
from types import SimpleNamespace

import pytest

from test_lumae_analysis import load_plugin, plugin_client


P = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
SERVER = "server-a"
REFRESH_TASK = "plugin.audiomuse_plugins.lumae_analysis.catalog_refresh_task"


class HostBusy(Exception):
    """AudioMuse's refusal: another root task is running."""

    code = 1201


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else []
    db.commit()
    return rows


def _reconcile():
    return importlib.import_module("plugins.LumaeAnalysis.reconcile")


def _catalog():
    return importlib.import_module("plugins.LumaeAnalysis.catalog")


def _source():
    return {
        "server_id": SERVER,
        "catalog_instance_id": SOURCE,
        "rebind_status": "active",
        "catalog": {"generation": 1, "status": "complete", "completed_at": None},
    }


@pytest.fixture
def plugin(migrated_db, monkeypatch):
    mod = load_plugin()
    db = migrated_db
    with db.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS task_status (task_id TEXT, parent_task_id TEXT, "
            "task_type TEXT, status TEXT)"
        )
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
            "provider_type, server_name, is_default, rebind_status) "
            "VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, "
            "provider_type, published_generation, catalog_epoch, status) "
            "VALUES (%s, %s, 'navidrome', 1, 'epoch-a', 'complete')",
            (SOURCE, SERVER),
        )
    db.commit()
    for module in (mod, _reconcile()):
        monkeypatch.setattr(module, "get_db", lambda: db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [_source()])
    return mod, db


@pytest.fixture
def tick(plugin, monkeypatch):
    """The real tick; actions this module does not exercise report no work."""
    mod, db = plugin
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "serve_deferred_interactive", lambda *args, **kwargs: 0)
    monkeypatch.setattr(mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0)
    for name in ("next_settled_analysis_run", "next_preparation_run", "next_relationship_run",
                 "next_profile_backfill_run"):
        monkeypatch.setattr(mod, name, lambda db=None, server_id=None: None)
    monkeypatch.setattr(mod, "_safe_credits_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_safe_metadata_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_safe_edge_v3_upgrade", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    monkeypatch.setattr(mod, "next_loudness_backfill", lambda db=None, server_id=None: None)
    monkeypatch.setattr(mod, "get_core_adapter", lambda: SimpleNamespace(
        active_server_id=lambda: SERVER, mode="v3_registry", list_servers=lambda: []))
    return mod, db


def _wanted_at(db):
    return _rows(db, f"SELECT refresh_wanted_at FROM {P}catalog_state "
                     "WHERE catalog_instance_id=%s", (SOURCE,))[0][0]


def _want(db, age_minutes=0):
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}catalog_state SET refresh_wanted_at=now() - make_interval(mins => %s) "
            "WHERE catalog_instance_id=%s",
            (age_minutes, SOURCE),
        )
    db.commit()


def _live_refresh_task(db, status="RUNNING"):
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO task_status (task_id, parent_task_id, task_type, status) "
            "VALUES ('refresh-1', NULL, %s, %s)",
            (REFRESH_TASK, status),
        )
    db.commit()


def _refresh(client):
    return client.post(
        "/api/catalog/refresh",
        json={"server_id": SERVER, "catalog_instance_id": SOURCE,
              "if_stale_for_seconds": 21_600},
    )


def test_a_refused_refresh_is_kept_and_answered_deferred(plugin, monkeypatch):
    mod, db = plugin

    def refuse(*_args, **_kwargs):
        raise HostBusy("Another queue job (main_analysis) is still running.")

    monkeypatch.setattr(mod, "enqueue", refuse)

    response = _refresh(plugin_client(mod))

    assert response.status_code == 202
    body = response.get_json()
    assert (body["status"], body["reason"]) == ("deferred", "host_busy")
    assert response.headers["Retry-After"] == "60"
    assert _wanted_at(db) is not None
    # The watchdog is armed for it, and counts it as work.
    assert _rows(db, f"SELECT reason FROM {P}reconcile_control")[0][0] == "catalog_refresh_wanted"
    assert _reconcile().reconcile_schedule_from_state(db)["mode"] == "active"


def test_a_second_refusal_keeps_the_first_request_time(plugin, monkeypatch):
    mod, db = plugin
    _want(db, age_minutes=10)
    first = _wanted_at(db)
    monkeypatch.setattr(mod, "enqueue", lambda *_a, **_k: (_ for _ in ()).throw(HostBusy()))

    assert _refresh(plugin_client(mod)).get_json()["status"] == "deferred"
    assert _wanted_at(db) == first


def test_a_refresh_task_on_the_queue_serves_a_new_request(plugin, monkeypatch):
    mod, db = plugin
    _live_refresh_task(db, status="NEW")
    monkeypatch.setattr(mod, "enqueue", lambda *_a, **_k: pytest.fail("one refresh is enough"))

    response = _refresh(plugin_client(mod))

    assert response.status_code == 202
    assert response.get_json() == {
        "status": "queued", "coalesced": True,
        "server_id": SERVER, "catalog_instance_id": SOURCE,
    }
    assert _wanted_at(db) is None


def test_a_kept_request_is_queued_directly_once_the_queue_takes_it(plugin, monkeypatch):
    mod, db = plugin
    _want(db, age_minutes=3)
    queued = []
    monkeypatch.setattr(mod, "enqueue", lambda func, *args, **_kwargs: queued.append(args))

    response = _refresh(plugin_client(mod))

    assert response.status_code == 202 and response.get_json()["status"] == "queued"
    assert queued == [(SERVER,)]
    # Kept until a scan serves it.
    assert _wanted_at(db) is not None


def test_another_enqueue_failure_is_not_kept(plugin, monkeypatch):
    mod, db = plugin
    monkeypatch.setattr(mod, "enqueue", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("x")))

    assert _refresh(plugin_client(mod)).status_code == 500
    assert _wanted_at(db) is None


def test_a_finished_scan_serves_requests_made_before_it_started_only(plugin):
    _mod, db = plugin
    catalog = _catalog()
    _want(db, age_minutes=1)
    started = catalog.catalog_refresh_clock(db)

    assert catalog.satisfy_catalog_refresh_wants(db, SOURCE, started) is True
    assert _wanted_at(db) is None

    # A request made while the scan ran is kept for the next one.
    _want(db, age_minutes=-1)
    assert catalog.satisfy_catalog_refresh_wants(db, SOURCE, started) is False
    assert _wanted_at(db) is not None


def _refresh_result(changes):
    return {
        "catalog_instance_id": SOURCE,
        "server_id": SERVER,
        "generation": 2 if changes else 1,
        "changes": changes,
        "change_reason": "provider_diff" if changes else "no_change",
        "counts": {"track": 10},
    }


@pytest.fixture
def refresh_task(plugin, monkeypatch):
    mod, db = plugin
    calls = {"refresh": 0, "projection": []}

    def refresh(server_id=None, **_kwargs):
        calls["refresh"] += 1
        return _refresh_result(calls.get("changes", 0))

    monkeypatch.setattr(mod, "refresh_catalog", refresh)
    monkeypatch.setattr(mod, "analysis_projection_task", lambda server_id=None: (
        calls["projection"].append(server_id)
        or {"catalog_instance_id": SOURCE, "generation": 7, "changes": 1, "ready_count": 10}))
    monkeypatch.setattr(mod, "get_core_adapter", lambda: SimpleNamespace(
        active_server_id=lambda: SERVER, mode="v3_registry", list_servers=lambda: []))
    for name in ("find_backfill_ids", "next_profile_retry_at", "arm_edge_backfill"):
        monkeypatch.setattr(mod, name, lambda *_a, **_k: None)
    monkeypatch.setattr(mod.loudness_backfill, "ensure_sources", lambda *_a, **_k: None)
    monkeypatch.setattr(mod.loudness_backfill, "arm", lambda *_a, **_k: False)
    monkeypatch.setattr(mod, "refresh_status_snapshot", lambda *_a, **_k: None)
    return mod, db, calls


def test_a_refresh_that_changed_the_catalogue_projects_the_analysis(refresh_task):
    mod, db, calls = refresh_task
    calls["changes"] = 3
    _want(db, age_minutes=2)

    result = mod.catalog_refresh_task(SERVER)

    assert calls["projection"] == [SERVER]
    assert result["analysis"] == {"status": "projected", "generation": 7, "changes": 1,
                                  "ready_count": 10}
    assert result["message"].endswith(", analysis projected")
    assert _wanted_at(db) is None


def test_an_unchanged_refresh_does_not_project(refresh_task):
    mod, db, calls = refresh_task
    calls["changes"] = 0

    result = mod.catalog_refresh_task(SERVER)

    assert calls["projection"] == [] and "analysis" not in result


def test_a_failed_projection_keeps_the_published_catalogue(refresh_task, monkeypatch):
    mod, db, calls = refresh_task
    calls["changes"] = 1
    monkeypatch.setattr(mod, "analysis_projection_task",
                        lambda server_id=None: (_ for _ in ()).throw(RuntimeError("down")))

    result = mod.catalog_refresh_task(SERVER)

    assert result["analysis"] == {"status": "failed", "error": "down"}
    assert result["generation"] == 2


def _events(db):
    return _rows(db, f"SELECT action, status FROM {P}reconcile_events ORDER BY event_id")


def test_the_tick_runs_a_kept_refresh_once_the_queue_is_free(tick, refresh_task):
    mod, db, calls = refresh_task
    calls["changes"] = 1
    _want(db, age_minutes=5)

    result = mod.catalog_reconcile_task()

    assert result["status"] == "processed"
    assert calls["refresh"] == 1 and calls["projection"] == [SERVER]
    assert _wanted_at(db) is None
    assert ("catalog_refresh", "success") in _events(db)
    # Served: the next tick has nothing to refresh.
    assert mod.catalog_reconcile_task()["status"] == "current"
    assert calls["refresh"] == 1


def test_the_tick_leaves_a_kept_refresh_to_a_refresh_task_on_the_queue(tick, refresh_task):
    mod, db, calls = refresh_task
    _want(db, age_minutes=5)
    _live_refresh_task(db)

    mod.catalog_reconcile_task()

    assert calls["refresh"] == 0 and _wanted_at(db) is not None


def test_a_failed_kept_refresh_is_journaled_and_left_to_the_repair(tick, refresh_task,
                                                                  monkeypatch):
    mod, db, calls = refresh_task
    _want(db, age_minutes=5)
    monkeypatch.setattr(mod, "refresh_catalog",
                        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("navidrome down")))

    mod.catalog_reconcile_task()

    assert ("catalog_refresh", "failed") in _events(db)
    assert _wanted_at(db) is None


def test_the_tick_waits_out_a_main_job_before_a_kept_refresh(tick, refresh_task):
    mod, db, calls = refresh_task
    _want(db, age_minutes=5)
    with db.cursor() as cur:
        cur.execute("INSERT INTO task_status VALUES ('main', NULL, 'main_analysis', 'RUNNING')")
    db.commit()

    assert mod.catalog_reconcile_task()["status"] == "yielded"
    assert calls["refresh"] == 0 and _wanted_at(db) is not None
