"""Multi-worker plan (2026-10-30) R4, R6: passes every worker runner walks at once.

``shared_pass`` on the real migrated schema: takers that race get disjoint
batches, a batch handed back or left by a runner that went away goes to the
next taker first (only its tracks still due), a pass ends only once no batch
is out, and the analysis slots never exceed their cap across runners.

Then the real passes, each walked by two runners at once (two threads, each
with a connection of its own, one of them a helper): every track is
downloaded, analysed and published once, the pass completes, and no batch is
left. A runner that dies holding a batch loses no track of it.
"""
import shutil
import threading
import time

import pytest

from test_analysis_isolation import _flac
from test_reconcile_scheduler_postgres import (  # noqa: F401  (fixtures)
    P, SOURCE, SERVER, _queued_backfill, _rows, _track, tick,
)
from test_reconcile_budget_postgres import _edge_library
from runner_helpers import Active
from plugins.LumaeAnalysis import analysis_isolation as iso
from plugins.LumaeAnalysis import loudness_backfill, loudness_stream
from plugins.LumaeAnalysis import parallel_analysis as par
from plugins.LumaeAnalysis import profile_publication as publication
from plugins.LumaeAnalysis import reconcile, runner, shared_pass

STATE = f"{P}shared_test_state"


@pytest.fixture(autouse=True, scope="module")
def _stop_workers_at_end():
    yield
    iso.shutdown()


def _alive(db, *runner_ids, dead=()):
    with db.cursor() as cur:
        for runner_id in runner_ids:
            cur.execute(f"INSERT INTO {runner.workers_table()} (runner_id, role) VALUES (%s, 'worker') "
                        "ON CONFLICT (runner_id) DO UPDATE SET heartbeat_at=now()", (runner_id,))
        for runner_id in dead:
            cur.execute(f"INSERT INTO {runner.workers_table()} (runner_id, role, heartbeat_at) "
                        "VALUES (%s, 'worker', now() - interval '10 minutes') "
                        "ON CONFLICT (runner_id) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at",
                        (runner_id,))
    db.commit()


@pytest.fixture
def pass_state(migrated_db):
    db = migrated_db
    with db.cursor() as cur:
        cur.execute(f"""CREATE TABLE {STATE} (catalog_instance_id TEXT PRIMARY KEY, cursor TEXT,
            status TEXT, pass_started_at TIMESTAMPTZ, last_error TEXT, processed INTEGER NOT NULL DEFAULT 0,
            next_retry_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)""")
        cur.execute(f"INSERT INTO {STATE} (catalog_instance_id, cursor, status) VALUES (%s, '', 'queued')",
                    (SOURCE,))
    db.commit()
    return db


class Library:
    """A pass over ``count`` tracks, all due until ``done``."""

    def __init__(self, count):
        self.tracks = [f"track-{index:02d}" for index in range(count)]
        self.done = set()
        self.completed = []

    def take(self, db, runner_id, size=3):
        return shared_pass.take(
            db, "test", STATE, SOURCE, size, runner_id=runner_id,
            next_units=lambda after, n: [t for t in self.tracks if t > after and t not in self.done][:n],
            track_id=lambda unit: unit,
            refilter=lambda ids: [t for t in ids if t not in self.done],
            complete=lambda cur: (self.completed.append(runner_id),
                                  cur.execute(f"UPDATE {STATE} SET status='complete', cursor='', "
                                              "next_retry_at=now()+interval '6 hours' "
                                              "WHERE catalog_instance_id=%s", (SOURCE,))))


def test_takers_that_race_get_disjoint_batches_and_one_ends_the_pass(pass_state, second_connection):
    db, other = pass_state, second_connection
    _alive(db, "r1", "r2")
    library, lock = Library(40), threading.Lock()
    taken = {"r1": [], "r2": []}
    start = threading.Barrier(2)

    def walk(conn, runner_id):
        start.wait(timeout=10)
        while True:
            batch, why = library.take(conn, runner_id)
            if batch is None:
                if why == shared_pass.ENDED:
                    return
                time.sleep(0.01)  # the other runner still holds a batch
                continue
            with lock:
                taken[runner_id].extend(batch.units)
                library.done.update(batch.units)
            shared_pass.finish(conn, "test", STATE, SOURCE, [batch], [])

    threads = [threading.Thread(target=walk, args=(conn, runner_id))
               for conn, runner_id in ((db, "r1"), (other, "r2"))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    everything = taken["r1"] + taken["r2"]
    assert sorted(everything) == library.tracks  # each track once
    assert taken["r1"] and taken["r2"]
    assert len(library.completed) == 1  # one taker ended the pass, the other saw it ended
    assert _rows(db, f"SELECT status, cursor FROM {STATE}") == [("complete", "")]
    assert shared_pass.batches_out(db, "test", SOURCE) == 0


def test_a_batch_handed_back_goes_first_and_keeps_only_its_unfinished_tracks(pass_state):
    db = pass_state
    _alive(db, "r1", "r2")
    library = Library(10)
    first, _ = library.take(db, "r1", size=4)
    assert first.units == library.tracks[:4]
    library.done.update(first.units[:2])
    assert shared_pass.finish(db, "test", STATE, SOURCE, [first], first.units[2:]) == 2

    again, _ = library.take(db, "r2", size=4)
    assert again.units == library.tracks[2:4]  # handed back, before the cursor's next tracks
    following, _ = library.take(db, "r2", size=4)
    assert following.units == library.tracks[4:8]


def test_a_batch_of_a_runner_that_went_away_is_taken_over_with_its_finished_tracks_dropped(pass_state):
    db = pass_state
    _alive(db, "r2", dead=["r1"])
    library = Library(6)
    orphan, _ = library.take(db, "r1", size=3)
    library.done.add(orphan.units[0])  # r1 finished one track before it went away

    taken, _ = library.take(db, "r2", size=3)
    assert taken.batch_id == orphan.batch_id
    assert taken.units == orphan.units[1:]
    assert _rows(db, f"SELECT runner_id, track_ids FROM {shared_pass.batches_table()}") == [
        ("r2", orphan.units[1:])]


def test_a_live_runners_batch_is_never_taken_over(pass_state):
    db = pass_state
    _alive(db, "r1", "r2")
    library = Library(3)
    held, _ = library.take(db, "r1", size=3)

    batch, why = library.take(db, "r2", size=3)
    assert (batch, why) == (None, shared_pass.OUT)  # r1 holds the rest: the pass is not over
    assert library.completed == []
    library.done.update(held.units)
    shared_pass.finish(db, "test", STATE, SOURCE, [held], [])
    assert library.take(db, "r2", size=3) == (None, shared_pass.ENDED)
    assert library.completed == ["r2"]


def test_end_slice_counts_and_queues_a_running_pass_but_keeps_a_finished_one(pass_state):
    db = pass_state
    _alive(db, "r1")
    library = Library(2)
    library.take(db, "r1")
    assert _rows(db, f"SELECT status FROM {STATE}") == [("running",)]
    shared_pass.end_slice(db, STATE, SOURCE, 2)
    assert _rows(db, f"SELECT status, processed FROM {STATE}") == [("queued", 2)]
    with db.cursor() as cur:
        cur.execute(f"UPDATE {STATE} SET status='complete'")
    db.commit()
    shared_pass.end_slice(db, STATE, SOURCE, 1)
    assert _rows(db, f"SELECT status, processed FROM {STATE}") == [("complete", 3)]


def test_slots_never_exceed_their_cap_and_a_runner_that_went_away_loses_its_own(migrated_db):
    db = migrated_db
    _alive(db, "r1", "r2", "r3")
    assert shared_pass.acquire_slots(db, 3, 4, runner_id="r1") == 3
    assert shared_pass.acquire_slots(db, 3, 4, runner_id="r2") == 1
    assert shared_pass.acquire_slots(db, 3, 4, runner_id="r3") == 0
    assert shared_pass.slots_in_use(db) == 4
    assert shared_pass.release_slots(db, runner_id="r1") == 3
    assert shared_pass.acquire_slots(db, 3, 4, runner_id="r2") == 3  # keeps its one, takes two
    _alive(db, dead=["r2"])
    assert shared_pass.acquire_slots(db, 2, 4, runner_id="r3") == 2  # r2's went with it
    assert shared_pass.acquire_slots(db, 8, 2, runner_id="r1") == 0  # a smaller cap
    assert shared_pass.slots_in_use(db) == 2


def test_slots_taken_at_the_same_moment_stay_under_the_cap(migrated_db, second_connection):
    db, other = migrated_db, second_connection
    _alive(db, "r1", "r2")
    start, got = threading.Barrier(2), {}

    def take(conn, runner_id):
        start.wait(timeout=10)
        got[runner_id] = shared_pass.acquire_slots(conn, 4, 5, runner_id=runner_id)

    threads = [threading.Thread(target=take, args=args) for args in ((db, "r1"), (other, "r2"))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert got["r1"] + got["r2"] <= 5
    assert shared_pass.slots_in_use(db) == got["r1"] + got["r2"]


# ------------------------------------------------------------ the real passes


@pytest.fixture
def runners(tick, second_connection, monkeypatch, tmp_path):
    """Two runners on the tick fixture's plugin: each thread its own connection
    (``get_db``), runner id and ``HELPING``; downloads hand out a fresh copy of
    one real FLAC and are counted per track."""
    mod, db = tick
    local = threading.local()

    def get_db():
        return getattr(local, "db", db)

    for module in (mod, reconcile):
        monkeypatch.setattr(module, "get_db", get_db)
    source = _flac(tmp_path / "library", seconds=6.0)
    downloads, lock = [], threading.Lock()

    def load(track_id, catalog_instance_id=None, server_id=None, defer=False, **_kwargs):
        with lock:
            downloads.append(track_id)
            copy = tmp_path / "downloads" / f"{track_id}-{len(downloads)}.flac"
        copy.parent.mkdir(exist_ok=True)
        shutil.copy(source, copy)
        return {"track_id": track_id, "file_path": str(copy),
                "media_signature": mod.test_signature(track_id), "cleanup_path": str(copy)}

    mod.test_signature = lambda track_id: f"catalog-media:{track_id}"
    monkeypatch.setattr(mod, "load_track_file", load)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    _alive(db, "leader", "helper")

    def both(leader, helper):
        """``leader(conn)`` and ``helper(conn)`` at once, each on a thread with its
        own connection, runner id and ``HELPING``."""
        start, errors = threading.Barrier(2), []

        def run(conn, runner_id, work):
            local.db = conn
            tokens = (runner.RUNNER_ID.set(runner_id), runner.HELPING.set(runner_id == "helper"))
            try:
                start.wait(timeout=10)
                work(conn)
            except Exception as exc:  # pragma: no cover - reported below
                errors.append(exc)
            finally:
                runner.HELPING.reset(tokens[1])
                runner.RUNNER_ID.reset(tokens[0])

        threads = [threading.Thread(target=run, args=args)
                   for args in ((db, "leader", leader), (second_connection, "helper", helper))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=180)
        assert not errors, errors

    def together(task):
        """Slices of ``task(conn, deadline)`` on both runners until the pass is over."""
        results = {"leader": [], "helper": []}

        def walk(name):
            def work(conn):
                for _ in range(40):
                    with par.tick_scope(1):
                        result = task(conn, time.monotonic() + 1.5)
                    results[name].append(result)
                    if result["status"] == "complete":
                        return
                    if result["status"] == "coalesced":
                        if not (shared_pass.batches_out(conn, "edge", SOURCE)
                                or shared_pass.batches_out(conn, "loudness", SOURCE)):
                            return
                        time.sleep(0.05)  # the other runner still holds a batch
            return work

        both(walk("leader"), walk("helper"))
        return results

    yield mod, db, downloads, together, both
    assert not list(tmp_path.glob("downloads/*"))


def _waveforms_done(mod, monkeypatch):
    """The waveform pass has nothing due: these tests walk the later passes alone."""
    monkeypatch.setattr(mod, "find_backfill_ids", lambda *_args, **_kwargs: [])


def _edge_tracks(db, names):
    _edge_library(db, names)
    with db.cursor() as cur:
        for name in names:
            cur.execute(
                f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, track_id, "
                "title, metadata_fp, media_fp, analysis_eligible, payload, first_seen_at, last_seen_at) "
                "VALUES (%s, 1, %s, %s, 'metadata', %s, TRUE, '{}'::jsonb, now(), now())",
                (SOURCE, name, name, name))
    db.commit()


def test_two_runners_walk_the_edge_pass_and_analyse_each_track_once(runners, monkeypatch):
    mod, db, downloads, together, _both = runners
    _waveforms_done(mod, monkeypatch)
    names = [f"track-{index:02d}" for index in range(10)]
    _edge_tracks(db, names)

    results = together(lambda conn, deadline: mod.edge_backfill_task(SERVER, SOURCE, deadline, True))

    assert sorted(downloads) == names  # each track once, across both runners
    assert sum(r.get("ready", 0) for rs in results.values() for r in rs) == 10, results
    assert any(r.get("ready") for r in results["helper"]), results  # the helper did its share
    assert _rows(db, f"SELECT track_id FROM {P}edge_profiles ORDER BY track_id") == [(n,) for n in names]
    assert _rows(db, f"SELECT status, cursor FROM {P}edge_backfill_state") == [("complete", "")]
    assert shared_pass.batches_out(db, "edge", SOURCE) == 0


def test_two_runners_walk_the_loudness_pass_and_measure_each_track_once(runners, monkeypatch):
    mod, db, downloads, together, _both = runners
    _waveforms_done(mod, monkeypatch)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    mod.test_signature = lambda _track_id: "catalog-media:revision-a"  # _track's media
    names = [f"track-{index:02d}" for index in range(10)]
    for name in names:
        _track(db, name)
    loudness_backfill.ensure_sources(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)

    results = together(lambda conn, deadline: mod.loudness_backfill_task(SERVER, SOURCE, deadline, True))

    assert sorted(downloads) == names
    assert sum(r.get("ready", 0) for rs in results.values() for r in rs) == 10, results
    page = loudness_stream.bootstrap_page(db, SOURCE)
    assert sorted(i["track_id"] for i in page["items"] if i["kind"] == "track") == names
    assert loudness_backfill.state(db, SOURCE)["status"] == "complete"
    assert shared_pass.batches_out(db, "loudness", SOURCE) == 0


def test_a_runner_that_dies_holding_a_batch_loses_none_of_its_tracks(runners, monkeypatch):
    mod, db, downloads, together, _both = runners
    _waveforms_done(mod, monkeypatch)
    names = [f"track-{index:02d}" for index in range(6)]
    _edge_tracks(db, names)
    _alive(db, dead=["gone"])
    orphan, _ = shared_pass.take(
        db, "edge", f"{P}edge_backfill_state", SOURCE, 3, runner_id="gone",
        next_units=lambda after, n: mod.edge_backfill_candidates(db, SOURCE, after, n),
        track_id=lambda unit: unit, refilter=lambda ids: ids,
        complete=lambda cur: None)
    assert orphan.units == names[:3]

    together(lambda conn, deadline: mod.edge_backfill_task(SERVER, SOURCE, deadline, True))

    assert sorted(downloads) == names
    assert _rows(db, f"SELECT track_id FROM {P}edge_profiles ORDER BY track_id") == [(n,) for n in names]
    assert _rows(db, f"SELECT status FROM {P}edge_backfill_state") == [("complete",)]


def test_a_leader_batch_and_a_helper_batch_never_admit_the_same_waveform_track(runners, monkeypatch):
    mod, db, _downloads, _together, both = runners
    tracks = [f"track-{index:02d}" for index in range(16)]
    for track in tracks:
        _track(db, track)
    _queued_backfill(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 3)
    monkeypatch.setattr(mod, "background_batch_limit", lambda: 3)
    analysed, lock = [], threading.Lock()

    def analyze(ids, catalog_instance_id=None, attempt_tokens=None, **_kwargs):
        with lock:
            analysed.extend(ids)
        time.sleep(0.05)  # the other runner admits while this batch runs
        conn = mod.get_db()
        for track_id in ids:
            publication.complete_attempt(
                conn, catalog_instance_id, track_id, attempt_tokens[track_id], object(),
                "failed", "stub", None, 1, 1, failure_code="download_unavailable")
        return {"processed": len(ids), "attempted": len(ids), "ready": 0, "failed": len(ids),
                "skipped": 0}

    monkeypatch.setattr(mod, "analyze_tracks_task", analyze)

    def leader(_conn):
        for _ in range(20):
            if mod.profile_backfill_task(SERVER, SOURCE, None, True)["status"] in ("complete", "waiting_retry"):
                return

    def helper(_conn):
        for _ in range(20):
            if mod.helper_profile_batch_task(SERVER, SOURCE)["status"] == "coalesced":
                return

    both(leader, helper)

    assert sorted(analysed) == tracks  # each track admitted and analysed once


def test_a_helper_turn_serves_no_on_demand_work_and_only_walks_track_passes(tick, monkeypatch):
    mod, db = tick
    calls = []
    monkeypatch.setattr(mod, "_safe_shared_passes", lambda: True)
    monkeypatch.setattr(mod, "background_sizing", lambda: (2, "test"))
    monkeypatch.setattr(mod, "_runner_blocker", lambda *_args: None)
    monkeypatch.setattr(mod, "runner_lane", lambda *_args: (mod.HEAVY_ACTIONS, "heavy_lane"))

    def source(_db, action):
        calls.append(action)
        return None

    monkeypatch.setattr(mod, "_next_shared_source", source)
    _alive(db, "helper-1")
    active = Active("worker")
    active.helping = True
    tokens = (runner.ROLE.set("worker"), runner.HELPING.set(True), runner.RUNNER_ID.set("helper-1"))
    try:
        assert mod.serve_interactive() == 0
        result = mod.run_runner_turn(active)
    finally:
        runner.RUNNER_ID.reset(tokens[2])
        runner.HELPING.reset(tokens[1])
        runner.ROLE.reset(tokens[0])
    assert calls == list(mod.HELPER_ACTIONS)
    assert result.reason == "helper_nothing_due"
    assert active.files_at_once == 2
    assert shared_pass.slots_in_use(db) == 0  # given back at the end of the turn


def test_a_helper_turn_waits_while_every_slot_is_taken(tick, monkeypatch):
    mod, db = tick
    monkeypatch.setattr(mod, "_safe_shared_passes", lambda: True)
    monkeypatch.setattr(mod, "_runner_blocker", lambda *_args: None)
    monkeypatch.setattr(mod, "runner_lane", lambda *_args: (mod.HEAVY_ACTIONS, "heavy_lane"))
    monkeypatch.setattr(mod, "max_files_in_flight", lambda: 2)
    _alive(db, "other", "helper-1")
    assert shared_pass.acquire_slots(db, 2, 2, runner_id="other") == 2
    active = Active("worker")
    active.helping = True
    token = runner.RUNNER_ID.set("helper-1")
    try:
        result = mod.run_runner_turn(active)
    finally:
        runner.RUNNER_ID.reset(token)
    assert (result.mode, result.reason) == ("waiting", "files_in_flight_cap")


def test_a_helper_turn_with_one_worker_runner_does_nothing(tick, monkeypatch):
    mod, _db = tick
    monkeypatch.setattr(mod, "worker_runners_setting", lambda: "one")
    active = Active("worker")
    active.helping = True
    assert mod.run_runner_turn(active).reason == "one_worker_runner"


def test_with_every_runner_the_later_passes_wait_for_the_waveform_pass(tick, monkeypatch):
    # A waveform analysis measures the track's edge and loudness from its own download: while
    # the waveform pass has the track due or in flight, the edge and loudness passes leave it.
    mod, db = tick
    _track(db, "track-00")
    loudness_backfill.ensure_sources(db)

    def waits(action):
        return mod._earlier_pass_running(db, action, SERVER, SOURCE)

    assert waits("edge_backfill") and waits("loudness_backfill")  # due for its waveform
    downloads = []
    monkeypatch.setattr(mod, "load_track_file", lambda *args, **kwargs: downloads.append(args))
    result = mod.loudness_backfill_task(SERVER, SOURCE, time.monotonic() + 5, True)
    assert (result["status"], result["reason"]) == ("coalesced", mod.EARLIER_PASS_RUNNING)
    assert downloads == [] and shared_pass.batches_out(db, "loudness", SOURCE) == 0

    assert mod.mark_pending(["track-00"], catalog_instance_id=SOURCE, priority="background")
    assert waits("edge_backfill") and waits("loudness_backfill")  # in flight on a runner
    # Admitted longer ago than a turn and a file's time limit: its runner went away.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}source_profiles SET analyzed_at=now()-interval '30 minutes'")
    db.commit()
    assert not waits("edge_backfill") and not waits("loudness_backfill")

    # An edge batch out holds the loudness pass back (the edge measures loudness too).
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}pass_batches (pass_name, catalog_instance_id, runner_id, track_ids) "
                    "VALUES ('edge', %s, 'leader', ARRAY['track-00'])", (SOURCE,))
    db.commit()
    assert waits("loudness_backfill") and not waits("edge_backfill")


@pytest.mark.parametrize("helping", [True, False])
def test_a_turn_whose_pass_waited_comes_back_soon(tick, monkeypatch, helping):
    mod, db = tick
    monkeypatch.setattr(mod, "_safe_shared_passes", lambda: True)
    monkeypatch.setattr(mod, "background_sizing", lambda: (1, "test"))
    monkeypatch.setattr(mod, "_runner_blocker", lambda *_args: None)
    monkeypatch.setattr(mod, "runner_lane", lambda *_args: (mod.PER_TRACK_ACTIONS, "heavy_lane"))
    monkeypatch.setattr(mod, "report_edge_runtime", lambda **_kwargs: None)
    answers = [("edge_backfill", {"status": "coalesced", "reason": mod.EARLIER_PASS_RUNNING}),
               (None, None)]
    name = "_next_helper_action" if helping else "_next_runner_action"
    monkeypatch.setattr(mod, name, lambda *_args: answers.pop(0))
    _alive(db, "runner-1")
    active = Active("worker")
    active.helping = helping
    token = runner.RUNNER_ID.set("runner-1")
    try:
        result = mod.run_runner_turn(active)
    finally:
        runner.RUNNER_ID.reset(token)
    assert (result.reason, result.next_due_seconds) == (mod.EARLIER_PASS_RUNNING, mod.PASS_WAIT_SECONDS)


def test_a_gone_runners_waveform_attempts_are_due_again_at_once(tick):
    # Without the record they would wait PREPARATION_STALE_HOURS as stale attempts.
    mod, db = tick
    for name in ("track-00", "track-01", "track-02"):
        _track(db, name)
    _alive(db, "live", dead=["gone"])

    def admitted(runner_id, names):
        tokens = mod.mark_pending(names, catalog_instance_id=SOURCE, priority="background")
        token = runner.RUNNER_ID.set(runner_id)
        try:
            assert mod._hold_waveform(SOURCE, tokens)
        finally:
            runner.RUNNER_ID.reset(token)

    admitted("gone", ["track-00", "track-01"])
    admitted("live", ["track-02"])
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}profile_backfill_state SET status='complete', "
                    "next_retry_at=now()+interval '6 hours'")
    db.commit()

    assert mod._release_gone_waveform(db) == 2
    assert _rows(db, f"SELECT track_id, status FROM {P}source_profiles ORDER BY track_id") == [
        ("track-00", "stale"), ("track-01", "stale"), ("track-02", "pending")]
    assert sorted(mod.find_backfill_ids(5, catalog_instance_id=SOURCE, server_id=SERVER)) == [
        "track-00", "track-01"]
    assert _rows(db, f"SELECT status, next_retry_at FROM {P}profile_backfill_state") == [("queued", None)]
    # The live runner's record stays; a second taker finds nothing.
    assert _rows(db, f"SELECT runner_id, track_ids FROM {P}pass_batches") == [("live", ["track-02"])]
    assert mod._release_gone_waveform(db) == 0


def test_a_helper_turn_ends_in_time_to_try_the_lock(tick, monkeypatch):
    # A helper tries the role's lock between turns: a leader that went away is
    # replaced within about a minute, not after a whole turn of work.
    mod, db = tick
    monkeypatch.setattr(mod, "_safe_shared_passes", lambda: True)
    monkeypatch.setattr(mod, "background_sizing", lambda: (1, "test"))
    monkeypatch.setattr(mod, "_runner_blocker", lambda *_args: None)
    monkeypatch.setattr(mod, "runner_lane", lambda *_args: (mod.PER_TRACK_ACTIONS, "heavy_lane"))
    monkeypatch.setattr(mod, "report_edge_runtime", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "HELPER_TURN_SECONDS", 0.3)
    monkeypatch.setattr(mod, "_next_helper_action",
                        lambda *_args: ("profile_backfill", {"status": "queued", "processed": 1}))
    assert mod.HELPER_TURN_SECONDS < mod.RUNNER_TURN_SECONDS
    _alive(db, "helper-1")
    active = Active("worker")
    active.helping = True
    token = runner.RUNNER_ID.set("helper-1")
    started = time.monotonic()
    try:
        result = mod.run_runner_turn(active)
    finally:
        runner.RUNNER_ID.reset(token)
    assert result.worked and time.monotonic() - started < 3


def test_a_leader_holds_analysis_slots_only_while_a_per_track_pass_runs(tick, monkeypatch):
    # The owner's server: a relationship build in the leader's turn held 8 of the 16 slots
    # for minutes, so the helpers analysed half the files they could.
    mod, db = tick
    monkeypatch.setattr(mod, "_safe_shared_passes", lambda: True)
    monkeypatch.setattr(mod, "background_sizing", lambda: (3, "test"))
    monkeypatch.setattr(mod, "max_files_in_flight", lambda: 4)
    monkeypatch.setattr(mod, "_runner_blocker", lambda *_args: None)
    monkeypatch.setattr(mod, "runner_lane",
                        lambda *_args: (mod.PER_TRACK_ACTIONS | {"relationships"}, "heavy_lane"))
    monkeypatch.setattr(mod, "report_edge_runtime", lambda **_kwargs: None)
    runs = {"relationships": [(SERVER, SOURCE, 0)], "profile_backfill": [(SERVER, SOURCE, 0)]}
    monkeypatch.setattr(mod, "next_relationship_run",
                        lambda **_kwargs: runs["relationships"].pop() if runs["relationships"] else None)
    monkeypatch.setattr(mod, "next_profile_backfill_run",
                        lambda **_kwargs: runs["profile_backfill"].pop() if runs["profile_backfill"] else None)
    monkeypatch.setattr(mod, "next_edge_backfill", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "next_loudness_backfill", lambda *_args, **_kwargs: None)
    seen = []
    active = Active("worker")

    def step(_db, _report, action, *_args):
        pool = par.current()
        seen.append((action, shared_pass.slots_in_use(db), pool.workers, active.files_at_once))
        return {"status": "complete"}

    monkeypatch.setattr(mod, "_reconcile_step", step)
    _alive(db, "leader-1", "helper-1")
    assert shared_pass.acquire_slots(db, 1, 4, runner_id="helper-1") == 1
    token = runner.RUNNER_ID.set("leader-1")
    try:
        mod.run_runner_turn(active)
    finally:
        runner.RUNNER_ID.reset(token)
    # Only the helper's slot while the leader builds relationships; three more for its pass.
    assert seen == [("relationships", 1, 3, None), ("profile_backfill", 4, 3, 3)]
    assert shared_pass.slots_in_use(db) == 1
    assert active.files_at_once is None


def test_a_leader_whose_helpers_hold_every_slot_leaves_the_passes_to_them(tick, monkeypatch):
    mod, db = tick
    monkeypatch.setattr(mod, "_safe_shared_passes", lambda: True)
    monkeypatch.setattr(mod, "background_sizing", lambda: (3, "test"))
    monkeypatch.setattr(mod, "max_files_in_flight", lambda: 2)
    monkeypatch.setattr(mod, "_runner_blocker", lambda *_args: None)
    monkeypatch.setattr(mod, "runner_lane", lambda *_args: (mod.PER_TRACK_ACTIONS, "heavy_lane"))
    monkeypatch.setattr(mod, "report_edge_runtime", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "next_profile_backfill_run", lambda **_kwargs: (SERVER, SOURCE, 0))
    asked = []
    monkeypatch.setattr(mod, "next_edge_backfill", lambda *_args, **_kwargs: asked.append("edge"))
    monkeypatch.setattr(mod, "next_loudness_backfill", lambda *_args, **_kwargs: asked.append("loudness"))
    monkeypatch.setattr(mod, "_reconcile_step", lambda *_args: pytest.fail("a pass ran without a slot"))
    _alive(db, "leader-1", "helper-1")
    assert shared_pass.acquire_slots(db, 2, 2, runner_id="helper-1") == 2
    active = Active("worker")
    token = runner.RUNNER_ID.set("leader-1")
    try:
        result = mod.run_runner_turn(active)
    finally:
        runner.RUNNER_ID.reset(token)
    assert not result.worked and asked == []
    assert active.files_at_once is None
    assert shared_pass.slots_in_use(db) == 2


@pytest.mark.parametrize("shared", [True, False])
def test_a_new_leader_with_every_runner_queues_its_predecessors_waveform_claim(tick, monkeypatch, shared):
    # Only a leader claims a waveform pass; the lock says the previous one is gone.
    # With one runner the claim waits BACKFILL_STALE_MINUTES, as in 1.9.
    mod, db = tick
    _queued_backfill(db)
    assert mod.claim_profile_backfill_batch(SOURCE)
    assert _rows(db, f"SELECT status FROM {P}profile_backfill_state") == [("running",)]
    monkeypatch.setattr(mod, "_safe_shared_passes", lambda: shared)
    monkeypatch.setattr(mod, "_runner_blocker", lambda *_args: None)
    monkeypatch.setattr(mod, "runner_lane", lambda *_args: (mod.PER_TRACK_ACTIONS, "heavy_lane"))
    monkeypatch.setattr(mod, "report_edge_runtime", lambda **_kwargs: None)
    monkeypatch.setattr(mod, "_next_runner_action", lambda *_args: (None, None))
    _alive(db, "leader-2")
    active = Active("worker")
    active.new_leader = True
    token = runner.RUNNER_ID.set("leader-2")
    try:
        mod.run_runner_turn(active)
    finally:
        runner.RUNNER_ID.reset(token)
    status = "queued" if shared else "running"
    assert _rows(db, f"SELECT status, next_retry_at FROM {P}profile_backfill_state") == [(status, None)]
