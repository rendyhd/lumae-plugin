"""Runner plan R6: on-demand work a runner took names the runner's role, so
the work of a runner that went away waits again at once, when the role's next
leader starts or, for a worker, when the server sees its heartbeat stop.
The stale rules (an hour for waveforms, half an hour for edges) stay the
backstop for anything else.
"""
from runner_helpers import Active
from test_reconcile_scheduler_postgres import (  # noqa: F401  (fixtures)
    P, SOURCE, _track, tick,
)
from plugins.LumaeAnalysis import edge_profile_store


def _waiting(mod, db, tracks):
    for track in tracks:
        _track(db, track)
    tokens = mod.mark_pending(tracks, catalog_instance_id=SOURCE, priority="interactive")
    mod.mark_profiles_waiting(db, SOURCE, tokens)
    return tokens


def _as(mod, role, call, *args):
    token = mod.runner.ROLE.set(role)  # as runner.Runner._turn does
    try:
        return call(*args)
    finally:
        mod.runner.ROLE.reset(token)


def _markers(db, table="source_profiles"):
    with db.cursor() as cur:
        cur.execute(f"SELECT track_id, last_error FROM {P}{table} ORDER BY track_id")
        rows = dict(cur.fetchall())
    db.commit()
    return rows


def test_a_runner_marks_the_work_it_takes_with_its_role(tick):
    mod, db = tick
    _waiting(mod, db, ["a", "b", "c"])
    _as(mod, "worker", mod.claim_waiting_profiles, db, 1)
    _as(mod, "server", mod.claim_waiting_profiles, db, 1)
    mod.claim_waiting_profiles(db, 1)  # outside a runner, as before
    assert _markers(db) == {"a": "claimed_worker_runner", "b": "claimed_server_runner", "c": None}


def test_a_released_claim_waits_again_with_its_attempt(tick):
    mod, db = tick
    tokens = _waiting(mod, db, ["a", "b", "c"])
    _as(mod, "worker", mod.claim_waiting_profiles, db, 2)
    _as(mod, "server", mod.claim_waiting_profiles, db, 1)

    assert mod.release_runner_claims(db, "worker") == 2

    assert _markers(db) == {"a": "waiting_runner", "b": "waiting_runner",
                            "c": "claimed_server_runner"}
    again = _as(mod, "server", mod.claim_waiting_profiles, db, 5)
    assert sorted((track, token) for _source, track, token in again) == [
        ("a", tokens["a"]), ("b", tokens["b"])]


def test_a_new_leaders_first_turn_releases_its_roles_claims(tick, monkeypatch):
    mod, _db = tick
    released = []
    monkeypatch.setattr(mod, "_safe_release_runner_claims",
                        lambda _db, role, gone_for_seconds=None: released.append(role))
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)  # the turn ends there
    active = Active("worker")
    active.new_leader = True
    mod.run_runner_turn(active)
    active.new_leader = False
    mod.run_runner_turn(active)
    assert released == ["worker"]


def test_the_server_releases_the_claims_of_a_worker_that_went_away(tick, monkeypatch):
    mod, db = tick
    for name in ("recover_interrupted_events", "refresh_status_snapshot"):
        monkeypatch.setattr(mod, name, lambda *_args, **_kwargs: None)
    _waiting(mod, db, ["a", "b"])
    _as(mod, "worker", mod.claim_waiting_profiles, db, 2)
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}runner_state (role, mode, heartbeat_at) "
                    "VALUES ('worker', 'working', now())")
    db.commit()

    mod._runner_bookkeeping(db)
    assert set(_markers(db).values()) == {"claimed_worker_runner"}  # still online

    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}runner_state SET heartbeat_at=now()-interval '5 minutes'")
    db.commit()
    monkeypatch.setattr(mod, "_runner_bookkeeping_at", None)
    mod._runner_bookkeeping(db)
    assert set(_markers(db).values()) == {"waiting_runner"}


def test_edge_jobs_a_runner_took_wait_again_started_or_not(tick):
    mod, db = tick
    with db.cursor() as cur:
        for track in ("a", "b", "c"):  # as request_edge_profiles leaves an on-demand job
            cur.execute(f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, "
                        "media_revision, job_token, status, priority, last_error) "
                        "VALUES (%s, %s, 'r', %s, 'pending', 'interactive', %s)",
                        (SOURCE, track, f"token-{track}", mod.RUNNER_WAITING_MARKER))
    db.commit()
    jobs = _as(mod, "worker", lambda: edge_profile_store.claim_waiting_edge_jobs(
        db, mod.WAITING_MARKERS, 2, mod.runner_claim_marker()))
    assert edge_profile_store.update_edge_job(db, SOURCE, jobs[0], "running")
    assert _markers(db, "edge_profile_jobs") == {
        "a": "claimed_worker_runner", "b": "claimed_worker_runner", "c": "waiting_runner"}

    assert mod.release_runner_claims(db, "server") == 0
    assert mod.release_runner_claims(db, "worker") == 2

    with db.cursor() as cur:
        cur.execute(f"SELECT DISTINCT status, last_error FROM {P}edge_profile_jobs")
        assert cur.fetchall() == [("pending", "waiting_runner")]
    db.commit()
    again = edge_profile_store.claim_waiting_edge_jobs(db, mod.WAITING_MARKERS, 5)
    assert sorted(job["job_token"] for job in again) == ["token-a", "token-b", "token-c"]


def test_held_on_demand_work_has_the_server_look_again_within_a_minute(tick):
    mod, db = tick
    _waiting(mod, db, ["a"])
    _as(mod, "worker", mod.claim_waiting_profiles, db, 1)
    with db.cursor() as cur:
        summary = mod.reconcile._work_summary(cur)
    db.commit()
    assert summary["waiting"] == 1
    assert mod._runner_next_due_seconds(db) == mod.RUNNER_WAITING_SECONDS
