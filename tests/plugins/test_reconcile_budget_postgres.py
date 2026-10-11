"""A time-boxed watchdog tick that runs several actions (plan S2).

AUD-13, AUD-16, AUD-17, AUD-19, AUD-20: one action per tick and no bound on
its wall-clock time (8-minute ticks, 81 s edge passes against 30 s, 160
MusicBrainz requests per credits turn) meant a tick could cover the next
minute's host fire, and minute cadence lasted far longer than the work.

* The tick works for TICK_BUDGET_SECONDS over as many actions as fit; an
  action starts only when its recent average fits in what is left, and the
  first always starts. Each action stops at the deadline itself: the
  waveform batch starts no track after it, the edge pass checks it before
  every track (the first too), the relationship build gets what is left,
  and a credits or MusicBrainz turn defers before an uncached request past
  it and resumes from the cache.
* Credits' before-background share is measured from the end of a turn.
* With several media servers the host calls the tick once per server in one
  task; the first call does the work, later ones return at once.
"""
import sys
import time
from types import SimpleNamespace

import pytest

from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401
from test_credits import credits_db, uid  # noqa: F401
from test_reconcile_scheduler_postgres import (  # noqa: F401
    P, SOURCE, SERVER, _events, _queued_backfill, _rows, _track, tick,
)
from plugins.LumaeAnalysis import credits_musicbrainz as mb  # noqa: E402
from plugins.LumaeAnalysis import credits_service as service  # noqa: E402
from plugins.LumaeAnalysis import credits_store as store  # noqa: E402
from plugins.LumaeAnalysis import edge_profile_store  # noqa: E402
from plugins.LumaeAnalysis import reconcile  # noqa: E402


def _budget(monkeypatch, seconds):
    monkeypatch.setattr(reconcile, "TICK_BUDGET_SECONDS", seconds)


def _slow_tracks(mod, monkeypatch, seconds):
    """Every waveform analysis takes ``seconds``; returns the tracks started."""
    started = []
    monkeypatch.setattr(mod, "profile_task_disposition", lambda *args, **kwargs: "analyze")

    def analyse(track_id, **_kwargs):
        started.append(track_id)
        time.sleep(seconds)
        return {"track_id": track_id, "status": "ready"}

    monkeypatch.setattr(mod, "analyze_one_track", analyse)
    return started


def _relationship_steps(mod, monkeypatch, steps, seconds):
    """A relationship build of ``steps`` checkpointed steps of ``seconds``."""
    budgets = []
    left = [steps]
    monkeypatch.setattr(mod, "next_relationship_run", lambda db=None, server_id=None: (
        (SERVER, SOURCE, 0) if left[0] else None))

    def step(server_id, catalog_id, time_budget_seconds=20):
        budgets.append(time_budget_seconds)
        time.sleep(min(seconds, time_budget_seconds))
        left[0] -= 1
        return {"status": "queued" if left[0] else "complete"}

    monkeypatch.setattr(mod, "relationship_preparation_task", step)
    return budgets


def _credits_turn(mod, monkeypatch, seconds):
    turns = []

    def turn(db, server_id, before_background, deadline=None):
        if not before_background or turns:
            return None
        turns.append(deadline)
        time.sleep(seconds)
        return {"status": "published", "album_id": "album-a", "match_status": "matched",
                "credits": 2}

    monkeypatch.setattr(mod, "_safe_credits_reconcile", turn)
    return turns


def test_a_tick_runs_several_actions_and_ends_within_its_budget_plus_one_item(
    tick, monkeypatch,
):
    mod, db = tick
    _budget(monkeypatch, 3)
    for index in range(10):
        _track(db, f"track-{index:02d}")
    _queued_backfill(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 10)
    started_tracks = _slow_tracks(mod, monkeypatch, 0.4)
    budgets = _relationship_steps(mod, monkeypatch, steps=2, seconds=0.3)
    turns = _credits_turn(mod, monkeypatch, 0.2)

    began = time.monotonic()
    result = mod.catalog_reconcile_task()
    elapsed = time.monotonic() - began

    assert [entry["action"] for entry in result["actions"]] == [
        "credits", "relationships", "profile_backfill"]
    assert [entry["runs"] for entry in result["actions"]] == [1, 2, 1]
    # Ended within the budget plus one track (and the tick's bookkeeping).
    assert elapsed < 3 + 0.4 + 0.5, elapsed
    assert 2 <= len(started_tracks) < 10
    batch = [summary for action, _s, _p, summary in _events(db) if action == "profile_backfill"]
    assert batch[0]["deferred"] == 10 - len(started_tracks)
    # Each action got what was left of the budget, not a fixed allowance.
    assert turns[0] is not None
    assert 2 < budgets[0] < 3 and budgets[1] < budgets[0]
    # The tracks not started are due again at once, uncounted.
    unstarted = sorted(set(f"track-{index:02d}" for index in range(10)) - set(started_tracks))
    assert sorted(mod.find_backfill_ids(25, catalog_instance_id=SOURCE, server_id=SERVER,
                                        include_failed=False)) == unstarted
    assert _rows(db, f"SELECT DISTINCT retry_count FROM {P}source_profiles "
                     "WHERE track_id = ANY(%s)", (unstarted,)) == [(0,)]


def test_an_action_whose_recent_average_does_not_fit_is_not_started(tick, monkeypatch):
    mod, db = tick
    _budget(monkeypatch, 2)
    for index in range(2):
        _track(db, f"track-{index}")
    _queued_backfill(db)
    started_tracks = _slow_tracks(mod, monkeypatch, 0.05)
    budgets = _relationship_steps(mod, monkeypatch, steps=1, seconds=0.1)
    _credits_turn(mod, monkeypatch, 0.2)
    for _ in range(3):
        reconcile.record_event(db, "relationships", SERVER, SOURCE, "deferred",
                               phase="relationship build checkpointed", duration_ms=5000)

    result = mod.catalog_reconcile_task()

    # Credits went first; a 5 s relationship step does not fit in what was
    # left of 2 s, and the waveform batch (no history) does.
    assert [entry["action"] for entry in result["actions"]] == ["credits", "profile_backfill"]
    assert budgets == [] and len(started_tracks) == 2


def test_the_first_action_starts_whatever_its_average(tick, monkeypatch):
    mod, db = tick
    _budget(monkeypatch, 2)
    budgets = _relationship_steps(mod, monkeypatch, steps=1, seconds=0.1)
    for _ in range(3):
        reconcile.record_event(db, "relationships", SERVER, SOURCE, "deferred",
                               phase="relationship build checkpointed", duration_ms=50000)

    result = mod.catalog_reconcile_task()

    assert [entry["action"] for entry in result["actions"]] == ["relationships"]
    assert len(budgets) == 1


def _edge_library(db, tracks):
    with db.cursor() as cur:
        for track in tracks:
            cur.execute(
                f"INSERT INTO {P}published_source_profiles (catalog_instance_id, track_id, "
                "sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, "
                "profile_schema_ver, media_signature, analyzed_at) VALUES (%s, %s, 44100, "
                "240000, -14, '\\x00', '\\x00', 1, 1, %s, now())",
                (SOURCE, track, f"catalog-media:{track}"),
            )
    db.commit()
    edge_profile_store.ensure_edge_backfill_sources(db)


def _slow_edges(mod, monkeypatch, seconds):
    runs = []

    def analyse(jobs, source, server, priority="background", budget_seconds=None):
        runs.append(([job["track_id"] for job in jobs], budget_seconds))
        time.sleep(seconds)
        return [{"track_id": job["track_id"], "status": "ready"} for job in jobs]

    monkeypatch.setattr(mod, "analyze_edges_task", analyse)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    return runs


def _edge_pass(db):
    return _rows(db, f"SELECT status, cursor, processed FROM {P}edge_backfill_state")[0]


def test_the_edge_pass_starts_no_track_after_the_deadline_not_even_its_first(
    tick, monkeypatch,
):
    mod, db = tick
    _edge_library(db, ["track-a", "track-b"])
    runs = _slow_edges(mod, monkeypatch, 0)

    result = mod.edge_backfill_task(SERVER, SOURCE, deadline=time.monotonic() - 1)

    assert runs == []
    assert result == {"status": "queued", "processed": 0}
    assert _edge_pass(db) == ("queued", "", 0)


def test_the_edge_pass_stops_at_the_deadline_and_gives_each_track_what_is_left(
    tick, monkeypatch,
):
    mod, db = tick
    _edge_library(db, ["track-a", "track-b", "track-c"])
    runs = _slow_edges(mod, monkeypatch, 0.3)

    result = mod.edge_backfill_task(SERVER, SOURCE, deadline=time.monotonic() + 0.5)

    assert [tracks for tracks, _budget in runs] == [["track-a"], ["track-b"]]
    assert 0 < runs[1][1] < runs[0][1] <= 0.5
    assert result["processed"] == 2
    assert _edge_pass(db) == ("queued", "track-b", 2)


# ------------------------------------------------------------------- credits
class _Clock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now


def _musicbrainz(monkeypatch, clock, per_request=0.1):
    calls = []

    def get(url, **_kwargs):
        calls.append(url)
        clock.now += per_request
        return SimpleNamespace(status_code=200, headers={}, content=b"{}", json=lambda: {})

    monkeypatch.setattr(mb, "time", clock)
    http = SimpleNamespace(get=get)

    def client(db, **kwargs):
        return mb.Client(db, http=http, sleep=lambda _seconds: None, **kwargs)

    return calls, client


def _album_of_300_recordings(monkeypatch):
    def resolve(_album, client):
        for index in range(300):
            client.get("recording", uid(1000 + index))
        return {"status": "unresolved", "evidence": {}}

    monkeypatch.setattr(service, "resolve", resolve)


def _job(db, album="a"):
    return _rows(db, f"SELECT status, attempts, last_error FROM {P}credits_jobs "
                     "WHERE catalog_instance_id='c' AND album_id=%s", (album,))[0]


def _due_now(db):
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}credits_jobs SET not_before=now()")
    db.commit()


def test_a_credits_album_defers_at_the_tick_deadline_and_resumes_from_the_cache(
    credits_db, monkeypatch,
):
    db = credits_db
    clock = _Clock()
    calls, client = _musicbrainz(monkeypatch, clock)
    _album_of_300_recordings(monkeypatch)
    store.request_album(db, "c", "a", priority=10)

    first = service.run_one("c", db=db, client_factory=client, critical=lambda db: False,
                            deadline=clock.now + 6)

    assert first["status"] == "deferred" and first["retry_seconds"] == 1
    assert len(calls) == 60
    assert _job(db)[0] == "failed" and "tick ended" in _job(db)[2]
    # The next turns read those 60 answers from the cache; a turn still stops
    # at 160 uncached requests.
    _due_now(db)
    second = service.run_one("c", db=db, client_factory=client, critical=lambda db: False,
                             deadline=clock.now + 100)
    assert second["status"] == "deferred" and len(calls) == 220
    _due_now(db)
    third = service.run_one("c", db=db, client_factory=client, critical=lambda db: False,
                            deadline=clock.now + 100)
    assert third["status"] == "published" and len(calls) == 300
    assert _job(db)[0] == "unresolved"


def test_a_cached_album_still_publishes_after_the_tick_deadline(credits_db, monkeypatch):
    """The deadline is checked before uncached requests only, never in
    run_one's final allowed() check, which would discard a finished album."""
    db = credits_db
    clock = _Clock()
    calls, client = _musicbrainz(monkeypatch, clock)
    _album_of_300_recordings(monkeypatch)
    for first in (0, 150):
        warm = client(db)
        for index in range(first, first + 150):
            warm.get("recording", uid(1000 + index))
    store.request_album(db, "c", "a", priority=10)

    result = service.run_one("c", db=db, client_factory=client, critical=lambda db: False,
                             deadline=clock.now - 1)

    assert result["status"] == "published" and len(calls) == 300
    assert _job(db)[0] == "unresolved"


def test_the_credits_share_is_measured_from_the_end_of_a_turn(credits_db, monkeypatch):
    db = credits_db
    claimed = []

    def resolve(_album, _client):
        claimed.append(_rows(db, f"SELECT lease_until - interval '20 minutes' "
                                 f"FROM {P}credits_jobs WHERE album_id='a'")[0][0])
        time.sleep(1.1)
        return {"status": "unresolved", "evidence": {}}

    monkeypatch.setattr(service, "resolve", resolve)
    store.request_album(db, "c", "a", priority=10)

    assert service.run_one("c", db=db, client_factory=lambda db, **_: None,
                           critical=lambda db: False)["status"] == "published"

    [(last_work_at,)] = _rows(
        db, f"SELECT last_work_at FROM {P}credits_stream WHERE catalog_instance_id='c'")
    assert (last_work_at - claimed[0]).total_seconds() >= 1.0


# ------------------------------------------------------------------ dispatch
@pytest.fixture
def dispatch(monkeypatch):
    task = {"id": "task-1"}

    def current_task_id():
        if isinstance(task["id"], Exception):
            raise task["id"]
        return task["id"]

    monkeypatch.setitem(sys.modules, "taskqueue", SimpleNamespace(current_task_id=current_task_id))
    monkeypatch.setattr(load_plugin(), "_reconcile_dispatch_id", None)
    return task


def _scopes(mod, monkeypatch):
    seen = []
    monkeypatch.setattr(mod, "next_settled_analysis_run",
                        lambda db=None, server_id=None: seen.append(server_id) or None)
    return seen


def test_a_second_per_server_call_in_one_dispatch_returns_at_once(tick, monkeypatch, dispatch):
    mod, db = tick
    seen = _scopes(mod, monkeypatch)

    first = mod.catalog_reconcile_task()
    # The first call works for every source, not only the bound server.
    assert seen == [None] and first["status"] == "current"
    monkeypatch.setattr(mod, "get_db", lambda: pytest.fail("a later call does no work"))
    second = mod.catalog_reconcile_task()
    assert (second["status"], second["message"]) == (
        "same_dispatch", "Lumae: ran earlier in this task")
    assert seen == [None]
    # The next dispatch works again.
    monkeypatch.setattr(mod, "get_db", lambda: db)
    dispatch["id"] = "task-2"
    assert mod.catalog_reconcile_task()["status"] == "current" and seen == [None, None]


@pytest.mark.parametrize("task_id", [None, RuntimeError("no task context")])
def test_without_a_task_id_each_call_ticks_for_its_server(tick, monkeypatch, dispatch, task_id):
    mod, _db = tick
    seen = _scopes(mod, monkeypatch)
    dispatch["id"] = task_id
    assert mod.catalog_reconcile_task()["status"] == "current"
    assert mod.catalog_reconcile_task()["status"] == "current"
    assert seen == [SERVER, SERVER]
