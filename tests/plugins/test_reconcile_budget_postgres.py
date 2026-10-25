"""Each runner action in a slice of its own (runner plan R3; scheduler plan S2).

AUD-13, AUD-16, AUD-17, AUD-19, AUD-20: one action without a bound on its
wall-clock time (8-minute ticks, 81 s edge passes against 30 s, 160
MusicBrainz requests per credits turn) held everything behind it.

* A turn runs as many actions as are due, each with RUNNER_SLICE_SECONDS from
  when it is chosen. Each action stops at its deadline itself: the waveform
  batch starts no track after it, the edge pass checks it before every
  track (the first too), the relationship build gets the slice, and a
  credits or MusicBrainz turn defers before an uncached request past it and
  resumes from the cache.
* Credits' before-background share is measured from the end of a turn.
"""
import time
from types import SimpleNamespace


from test_lumae_analysis import lumae_postgres_db  # noqa: F401
from test_credits import credits_db, uid  # noqa: F401
from runner_helpers import turn
from test_reconcile_scheduler_postgres import (  # noqa: F401
    P, SOURCE, SERVER, _events, _queued_backfill, _rows, _track, tick,
)
from plugins.LumaeAnalysis import credits_musicbrainz as mb  # noqa: E402
from plugins.LumaeAnalysis import credits_service as service  # noqa: E402
from plugins.LumaeAnalysis import credits_store as store  # noqa: E402
from plugins.LumaeAnalysis import edge_profile_store  # noqa: E402


def _slice(mod, monkeypatch, seconds):
    monkeypatch.setattr(mod, "RUNNER_SLICE_SECONDS", seconds)


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


def test_a_turn_runs_every_due_action_each_in_a_slice_of_its_own(tick, monkeypatch):
    mod, db = tick
    _slice(mod, monkeypatch, 1)
    for index in range(10):
        _track(db, f"track-{index:02d}")
    _queued_backfill(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 10)
    started_tracks = _slow_tracks(mod, monkeypatch, 0.3)
    budgets = _relationship_steps(mod, monkeypatch, steps=2, seconds=0.3)
    turns = _credits_turn(mod, monkeypatch, 0.2)

    _result, actions = turn(mod)

    assert [entry["action"] for entry in actions] == ["credits", "relationships", "profile_backfill"]
    assert [entry["runs"] for entry in actions][:2] == [1, 2]
    # Each action had a slice of its own from when it was chosen.
    assert turns[0] is not None
    assert all(0.9 < budget <= 1 for budget in budgets), budgets
    # Each waveform batch started no track after its slice; the turn ran
    # batches until the library was done.
    batches = [summary for action, _s, _p, summary in _events(db) if action == "profile_backfill"]
    assert len(batches) >= 2 and batches[0]["deferred"] > 0
    assert sorted(started_tracks) == [f"track-{index:02d}" for index in range(10)]


def _stored_v2_edges(db, tracks):
    """Edges stored before 1.7.0: no v3 digest yet (SF.3)."""
    with db.cursor() as cur:
        for track in tracks:
            cur.execute(
                f"INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision, "
                "representation_id, media_signature, profile_digest, payload) "
                "VALUES (%s, %s, 'rev', 'rep', 'sig', 'digest', '{}')",
                (SOURCE, track),
            )
    db.commit()


def test_the_stored_edge_v3_upgrade_is_one_batch_a_turn_and_journaled(tick, monkeypatch):
    """SF.3's upgrade batch goes after on-demand work, once a turn, within
    EDGE_V3_UPGRADE_BUDGET_SECONDS or its slice; the turn goes on to other
    work, and the next turn finds nothing to upgrade."""
    mod, db = tick
    _stored_v2_edges(db, ["t1", "t2", "t3"])
    budgets = _relationship_steps(mod, monkeypatch, steps=1, seconds=0)

    _result, actions = turn(mod)

    assert [(entry["action"], entry["runs"], entry["count"]) for entry in actions] == [
        ("edge_schema_v3", 1, 3), ("relationships", 1, 1)]
    [summary] = [summary for action, _s, _p, summary in _events(db) if action == "edge_schema_v3"]
    assert summary == {"status": "processed", "upgraded": 3, "flagged": 0}
    assert edge_profile_store.edge_schema_v3_ready(db) and len(budgets) == 1

    _result, actions = turn(mod)
    assert "edge_schema_v3" not in [entry["action"] for entry in actions]


def test_the_stored_edge_v3_upgrade_gets_at_most_its_slice(tick, monkeypatch):
    mod, db = tick
    _stored_v2_edges(db, ["t1"])
    _slice(mod, monkeypatch, 3)
    budgets = []

    def upgrade(_db, budget_seconds=None):
        budgets.append(budget_seconds)
        return 1, 0  # more left: still one batch this turn

    monkeypatch.setattr(mod, "upgrade_stored_edges", upgrade)
    _result, actions = turn(mod)
    assert [entry["action"] for entry in actions] == ["edge_schema_v3"]
    assert len(budgets) == 1 and 2 < budgets[0] <= 3
    _slice(mod, monkeypatch, 60)
    turn(mod)
    assert budgets[1] == mod.EDGE_V3_UPGRADE_BUDGET_SECONDS


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


def test_the_edge_pass_pages_on_until_the_deadline(tick, monkeypatch):
    """WP1a: no fixed 30 s and no 25-track cap; the pass reads the next page
    from where it got to and works until its slice's deadline."""
    mod, db = tick
    _edge_library(db, [f"track-{index:02d}" for index in range(7)])
    monkeypatch.setattr(mod, "EDGE_BACKFILL_PAGE_SIZE", 3)
    runs = _slow_edges(mod, monkeypatch, 0.1)
    pages = []
    monkeypatch.setattr(mod, "edge_backfill_candidates", lambda db, source, after, limit, _read=mod.edge_backfill_candidates: (
        pages.append(after) or _read(db, source, after, limit)))

    result = mod.edge_backfill_task(SERVER, SOURCE, deadline=time.monotonic() + 0.45)

    assert pages == ["", "track-02"]
    assert [tracks for tracks, _budget in runs] == [[f"track-{index:02d}"] for index in range(5)]
    assert result == {"status": "queued", "processed": 5, "ready": 5}
    assert _edge_pass(db) == ("queued", "track-04", 5)


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


def test_a_credits_album_defers_at_its_deadline_and_resumes_from_the_cache(
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


def test_a_cached_album_still_publishes_after_its_deadline(credits_db, monkeypatch):
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
