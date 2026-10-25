"""Runner plan, Tests: two loops at once.

No plugin-wide lock serializes Lumae's actions; AudioMuse's queue gate did
that before 1.9.0. The server and worker runners run in different processes,
and the song hook in AudioMuse's analysis jobs, so the per-action claims keep
each action single. Here two threads, each with a database connection of its
own, take the same work at the same moment: every request, track and mark is
taken once.
"""
import threading
import time

import pytest

from test_reconcile_scheduler_postgres import (  # noqa: F401  (fixtures)
    P, SOURCE, SERVER, _queued_backfill, _track, tick,
)
from plugins.LumaeAnalysis import edge_profile_store
from plugins.LumaeAnalysis import profile_publication as publication

@pytest.fixture
def two_connections(tick, second_connection, monkeypatch):
    """The tick fixture's plugin, with ``get_db`` answering each thread its
    own connection: the main thread's and a second one on the same schema."""
    mod, db = tick
    local = threading.local()

    def get_db():
        return getattr(local, "db", db)

    for module in (mod, mod.reconcile):
        monkeypatch.setattr(module, "get_db", get_db)
    return mod, db, second_connection, local


def _together(local, connections, work):
    """Run ``work(db)`` on one thread per connection, started together;
    returns each thread's result."""
    start = threading.Barrier(len(connections))
    results = [None] * len(connections)
    errors = []

    def run(index, conn):
        local.db = conn
        try:
            start.wait(timeout=10)
            results[index] = work(conn)
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(index, conn))
               for index, conn in enumerate(connections)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not errors, errors
    return results


def _drain(claim):
    """Claim in small batches until nothing is left; every claimed key."""
    def work(conn):
        taken = []
        while True:
            batch = claim(conn)
            if not batch:
                return taken
            taken.extend(batch)
            time.sleep(0.01)  # let the other thread in
    return work


def test_two_runners_take_each_waiting_waveform_request_once(two_connections):
    mod, db, other, local = two_connections
    tracks = [f"track-{index:02d}" for index in range(30)]
    for track in tracks:
        _track(db, track)
    tokens = mod.mark_pending(tracks, catalog_instance_id=SOURCE, priority="interactive")
    mod.mark_profiles_waiting(db, SOURCE, tokens)

    first, second = _together(local, [db, other], _drain(
        lambda conn: [track for _source, track, _token in mod.claim_waiting_profiles(conn, 3)]))

    assert first and second  # both took some
    assert sorted(first + second) == tracks  # and none twice


def test_two_runners_take_each_waiting_edge_request_once(two_connections):
    mod, db, other, local = two_connections
    tracks = [f"track-{index:02d}" for index in range(30)]
    with db.cursor() as cur:
        for track in tracks:  # as request_edge_profiles leaves an on-demand job
            cur.execute(f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, "
                        "media_revision, job_token, status, priority, last_error) "
                        "VALUES (%s, %s, 'r', %s, 'pending', 'interactive', %s)",
                        (SOURCE, track, f"token-{track}", mod.RUNNER_WAITING_MARKER))
    db.commit()

    first, second = _together(local, [db, other], _drain(lambda conn: [
        job["track_id"] for job in edge_profile_store.claim_waiting_edge_jobs(
            conn, mod.WAITING_MARKERS, 3)]))

    assert first and second
    assert sorted(first + second) == tracks


def test_one_backfill_batch_at_a_time_and_each_track_once(two_connections, monkeypatch):
    mod, db, other, local = two_connections
    tracks = [f"track-{index:02d}" for index in range(12)]
    for track in tracks:
        _track(db, track)
    _queued_backfill(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 2)
    analysed, lock = [], threading.Lock()

    def analyze(ids, catalog_instance_id=None, attempt_tokens=None, **_kwargs):
        with lock:
            analysed.extend(ids)
        time.sleep(0.05)  # the other thread tries while this batch runs
        conn = mod.get_db()
        for track_id in ids:
            publication.complete_attempt(
                conn, catalog_instance_id, track_id, attempt_tokens[track_id], object(),
                "failed", "stub", None, 1, 1, failure_code="download_unavailable")
        return {"processed": len(ids), "attempted": len(ids), "ready": 0, "failed": len(ids),
                "skipped": 0}

    monkeypatch.setattr(mod, "analyze_tracks_task", analyze)
    statuses = []

    def work(conn):
        seen = []
        for _ in range(20):
            result = mod.profile_backfill_task(SERVER, SOURCE)
            seen.append(result["status"])
            if result["status"] in ("complete", "waiting_retry"):
                break
            time.sleep(0.01)
        statuses.append(seen)
        return seen

    _together(local, [db, other], work)

    assert sorted(analysed) == tracks  # each once
    assert any("coalesced" in seen for seen in statuses)  # the other thread waited its turn


def test_a_daily_mark_goes_to_one_runner(two_connections):
    mod, db, other, local = two_connections

    def claim(conn):
        with conn.cursor() as cur:
            taken = mod.runner.claim_mark(cur, "concurrency_probe", 3600)
        conn.commit()
        return taken

    assert sorted(_together(local, [db, other], claim)) == [False, True]
