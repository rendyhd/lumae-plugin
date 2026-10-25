"""Runner plan RN.3 (R6): a web route records its work, wakes the runners
and answers at once. Nothing is queued, so nothing can be
refused or strand a task; the ``deferred`` lists stay, empty."""
import select

import pytest

from test_catalog_refresh_wanted_postgres import P, SERVER, SOURCE, _refresh, _rows, _wanted_at, plugin  # noqa: F401
from test_lumae_analysis import plugin_client
from plugins.LumaeAnalysis import reconcile, runner


@pytest.fixture
def runners_on(plugin, monkeypatch, second_connection):
    mod, db = plugin
    listener = second_connection
    listener.autocommit = True
    with listener.cursor() as cur:
        cur.execute(f"LISTEN {runner.NOTIFY_CHANNEL}")
    return mod, db, listener


def _notified(listener, timeout=2.0):
    if select.select([listener], [], [], timeout)[0]:
        listener.poll()
    payloads = [note.payload for note in listener.notifies]
    del listener.notifies[:]
    return payloads


def test_a_refresh_request_is_kept_and_wakes_the_runners(runners_on):
    mod, db, listener = runners_on
    response = _refresh(plugin_client(mod))
    assert response.status_code == 202
    assert response.get_json()["status"] == "queued"
    assert "Retry-After" not in response.headers
    assert _wanted_at(db) is not None
    assert "catalog_refresh_wanted" in _notified(listener)
    # The watchdog is not armed: the runners do the work.
    assert _rows(db, f"SELECT reason FROM {P}reconcile_control")[0][0] != "catalog_refresh_wanted"


def test_every_track_asked_for_is_accepted_and_none_deferred(runners_on, monkeypatch):
    mod, db, listener = runners_on
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_k: {"catalog_instance_id": SOURCE, "server_id": SERVER})
    monkeypatch.setattr(mod, "split_analyze_ids",
                        lambda ids, catalog_instance_id=None: (["t1", "t2", "t3", "t4"], ["t5"], ["t6"]))
    monkeypatch.setattr(mod, "mark_pending", lambda ids, catalog_instance_id=None, priority=None:
                        {track: f"token-{track}" for track in ids})
    recorded = []
    monkeypatch.setattr(mod, "mark_profiles_waiting",
                        lambda db, catalog, tokens: recorded.append(dict(tokens)))
    response = plugin_client(mod).post("/api/analyze", json={
        "catalog_instance_id": SOURCE, "ids": ["t1", "t2", "t3", "t4", "t5", "t6"]})
    assert response.status_code == 202
    body = response.get_json()
    assert body["accepted"] == ["t1", "t2", "t3", "t4"]
    assert body["deferred"] == []
    assert body["already_pending"] == ["t6"]
    # One record for the runner, chunks no longer matter.
    assert recorded == [{f"t{i}": f"token-t{i}" for i in (1, 2, 3, 4, 6)}]
    assert "interactive_profiles" in _notified(listener)


def test_edges_asked_for_are_recorded_for_the_runner(runners_on, monkeypatch):
    mod, db, listener = runners_on
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_k: {"catalog_instance_id": SOURCE, "server_id": SERVER})
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)
    jobs = [{"track_id": "t1", "media_revision": "r", "job_token": "j1"}]
    monkeypatch.setattr(mod, "claim_edge_requests",
                        lambda db, catalog, ids, priority: (jobs, ["t2"], [], []))
    marked = []
    monkeypatch.setattr(mod, "mark_edge_jobs_waiting",
                        lambda db, catalog, jobs, marker: marked.append((catalog, jobs, marker)))
    response = plugin_client(mod).post("/api/profiles/edges/analyze", json={
        "catalog_instance_id": SOURCE, "ids": ["t1", "t2"]})
    assert response.status_code == 202
    body = response.get_json()
    assert (body["accepted"], body["already_ready"], body["deferred"]) == (["t1"], ["t2"], [])
    assert marked == [(SOURCE, jobs, mod.RUNNER_WAITING_MARKER)]
    assert "interactive_edges" in _notified(listener)


def test_the_compatibility_edge_backfill_leaves_the_tracks_to_the_pass(runners_on, monkeypatch):
    mod, db, listener = runners_on
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_k: {"catalog_instance_id": SOURCE, "server_id": SERVER})
    monkeypatch.setattr(mod, "edge_scheduling_allowed", lambda: True)
    monkeypatch.setattr(mod, "edge_backfill_candidates", lambda db, catalog, after, limit: ["a", "b"])
    monkeypatch.setattr(mod, "claim_edge_requests", lambda *a, **k: pytest.fail("no jobs"))
    armed = []
    monkeypatch.setattr(mod, "arm_edge_backfill",
                        lambda db, catalog, include_complete=True: armed.append(catalog) or 1)
    response = plugin_client(mod).post("/api/profiles/edges/backfill", json={
        "catalog_instance_id": SOURCE})
    body = response.get_json()
    assert response.status_code == 202
    assert (body["accepted"], body["already_pending"], body["deferred"]) == ([], ["a", "b"], [])
    assert body["next_after"] == "b"
    assert armed == [SOURCE]
    assert "edge_backfill_ready" in _notified(listener)


def test_a_preparation_request_is_claimed_not_queued(runners_on, monkeypatch):
    mod, db, _listener = runners_on
    source = {"catalog_instance_id": SOURCE, "server_id": SERVER}
    monkeypatch.setattr(mod, "_resolve_preparation_source", lambda body: source)
    monkeypatch.setattr(mod, "_preparation_api_payload",
                        lambda source, state=None: {"publication_current": False, "status": "queued"})
    monkeypatch.setattr(mod, "preparation_state", lambda catalog: None)
    monkeypatch.setattr(mod, "preparation_is_active", lambda state: False)
    claims = []
    monkeypatch.setattr(mod, "claim_preparation", lambda source: claims.append(source) or True)
    response = plugin_client(mod).post("/api/catalog/prepare", json={})
    assert response.status_code == 202
    assert response.headers["Retry-After"] == "2"
    assert claims == [source]


def test_a_relationship_build_and_a_waveform_pass_are_claimed_not_queued(runners_on, monkeypatch):
    mod, _db, _listener = runners_on
    source = {"catalog_instance_id": SOURCE, "server_id": SERVER,
              "catalog": {"generation": 2}, "analysis": {"generation": 2}}
    monkeypatch.setattr(mod, "resolve_profile_source", lambda **_k: source)
    monkeypatch.setattr(mod, "claim_profile_backfill", lambda source: True)
    assert mod.start_profile_backfill(SOURCE, SERVER)["deferred"] is True
    monkeypatch.setattr(mod, "relationship_status", lambda db, catalog: {"status": "queued"})
    monkeypatch.setattr(mod, "claim_relationship_preparation", lambda db, catalog, force=False: True)
    assert mod.start_relationship_preparation(SOURCE, SERVER, force=True)["deferred"] is True


def test_recorded_work_wakes_the_runners_with_its_commit(runners_on):
    _mod, db, listener = runners_on
    before = _rows(db, f"SELECT mode, cron_expr FROM {P}reconcile_control")
    reconcile.wake_runners(db, "credits_requested")
    assert _notified(listener, timeout=0.3) == []  # not before the commit
    db.commit()
    assert "credits_requested" in _notified(listener)
    # 1.8.0's control row stays as it was, for a downgrade.
    assert _rows(db, f"SELECT mode, cron_expr FROM {P}reconcile_control") == before


def test_a_tick_queued_by_an_older_version_only_wakes_the_runners(runners_on):
    mod, _db, listener = runners_on
    assert mod.catalog_reconcile_task()["status"] == "runner"
    assert "queued_tick" in _notified(listener)


