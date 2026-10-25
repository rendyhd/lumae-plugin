"""Performance plan WP4: less host blocking while idle (scheduler plan S7, S8).

S7 (AUD-16): an album not found (unresolved) or without credits (empty) is
re-checked after 30 days, then 90, then 180 (W4 of the runner plan; S7 had 7,
30, 90), each ±25 %, not every week; a re-check of an unchanged album is idle
work and never moves the watchdog off its idle cadence. A changed album still
does. W4: background re-checks run only after background work, at most 60 an
hour, and the ones due at upgrade spread over 14 days.

S8 (AUD-19): an idle tick does almost nothing: the credits walk rests an
hour after it reaches the end (or until a new catalogue generation), and a
tick sweeps once however many credits turns it asks for; an unchanged album
is not rewritten; the settings snapshot is counted at most every 5 minutes
by ticks that changed no profile; one edge runtime report per 10 minutes per
worker across forked jobs.
"""
import pytest

from test_lumae_analysis import lumae_postgres_db  # noqa: F401  (fixture)
from test_credits import credits_db, payload, uid  # noqa: F401  (fixtures)
from test_reconcile_scheduler_postgres import P, SOURCE, SERVER, _rows, tick  # noqa: F401
from plugins.LumaeAnalysis import credits_service
from plugins.LumaeAnalysis import credits_store as store
from plugins.LumaeAnalysis import reconcile

JOBS = "plugin_lumae_analysis__credits_jobs"


def _q(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else None
    db.commit()
    return rows


def _unresolved(db, name):
    data = payload(db, name)
    data["match_status"] = "unresolved"
    data["subjects"] = [{"subject_id": name, "subject_type": "album", "credits": []}]
    return data


def _run(db, name, result):
    job = store.claim(db, "c")
    assert job and job["album_id"] == name
    assert store.finish(db, job, result)


def _expire(db, name):
    _q(db, f"UPDATE {JOBS} SET expires_at=now()-interval '1 second' WHERE catalog_instance_id='c' AND album_id=%s",
       (name,))


def _ttl_days(db, name):
    [(days,)] = _q(db, f"SELECT EXTRACT(EPOCH FROM expires_at-now())/86400 FROM {JOBS} "
                       "WHERE catalog_instance_id='c' AND album_id=%s", (name,))
    return float(days)


def _about(days, ttl):
    """W4: ``days`` is ``ttl`` with its ±25 % jitter."""
    return 0.75 * ttl - 0.01 <= days <= 1.25 * ttl + 0.01


# ------------------------------------------------------------------ S7


def test_an_unresolved_album_is_rechecked_after_30_then_90_then_180_days(credits_db):
    db = credits_db
    store.request_album(db, "c", "a")
    _run(db, "a", _unresolved(db, "a"))
    assert _about(_ttl_days(db, "a"), 30)
    for expected in (90, 180, 180):
        _expire(db, "a")
        store.request_album(db, "c", "a")  # the sweep finds it expired: a re-check
        _run(db, "a", _unresolved(db, "a"))
        assert _about(_ttl_days(db, "a"), expected)
    # A found album keeps 30 days.
    store.request_album(db, "c", "b")
    _run(db, "b", payload(db, "b"))
    assert _about(_ttl_days(db, "b"), 30)


# ------------------------------------------------------------------ W4


def test_albums_checked_together_do_not_expire_together(credits_db):
    db = credits_db
    expiries = set()
    for _ in range(12):
        store.request_album(db, "c", "a")
        _run(db, "a", _unresolved(db, "a"))
        expiries.add(round(_ttl_days(db, "a"), 3))
        assert _about(_ttl_days(db, "a"), 30)
        _q(db, f"DELETE FROM {JOBS}")
    assert len(expiries) > 6


def _recheck(db, name):
    """``name`` checked unresolved, expired and found again by the sweep: a re-check."""
    store.request_album(db, "c", name)
    _run(db, name, _unresolved(db, name))
    _expire(db, name)
    store.request_album(db, "c", name)


def test_a_recheck_waits_for_background_work_and_an_app_request_does_not(credits_db):
    db = credits_db
    _recheck(db, "a")
    assert store.claim(db, "c", rechecks=False) is None
    job = store.claim(db, "c")
    assert job["album_id"] == "a"
    store.finish(db, job, _unresolved(db, "a"))
    _expire(db, "a")
    store.request_album(db, "c", "a", priority=10)  # the phone asks for it
    assert store.claim(db, "c", rechecks=False)["album_id"] == "a"


def test_new_work_goes_before_a_recheck(credits_db):
    db = credits_db
    _recheck(db, "a")
    store.request_album(db, "c", "b")
    assert store.claim(db, "c")["album_id"] == "b"


def test_at_most_60_rechecks_start_an_hour(credits_db, monkeypatch):
    db = credits_db
    _recheck(db, "a")
    _recheck(db, "b")
    monkeypatch.setattr(store, "RECHECKS_PER_HOUR", 1)
    job = store.claim(db, "c")
    assert job is not None
    assert store.claim(db, "c") is None  # the hour's one re-check started
    store.finish(db, job, _unresolved(db, job["album_id"]))
    assert store.claim(db, "c") is None  # finished, it still counts this hour
    _q(db, f"UPDATE {JOBS} SET rechecked_at=now()-interval '61 minutes'")
    assert store.claim(db, "c") is not None
    monkeypatch.undo()
    assert store.RECHECKS_PER_HOUR == 60


def test_reconcile_offers_a_recheck_only_after_background_work(credits_db, monkeypatch):
    db = credits_db
    _recheck(db, "a")
    monkeypatch.setattr(credits_service, "resolve_catalog_source", lambda db, server_id=None: [
        {"catalog_instance_id": "c", "rebind_status": "active"}])
    monkeypatch.setattr(store, "sweep", lambda db, catalog_id, limit=32: 0)
    turns = []
    monkeypatch.setattr(credits_service, "run_one",
                        lambda catalog_id, **kwargs: turns.append(kwargs["rechecks"]) or {"status": "x"})
    _q(db, "UPDATE plugin_lumae_analysis__credits_stream SET last_work_at=now()-interval '1 day'")
    assert credits_service.reconcile(db, before_background=True) is None
    assert credits_service.reconcile(db, before_background=False) == {"status": "x"}
    assert turns == [True]


def test_the_upgrade_spreads_due_rechecks_over_14_days(credits_db):
    db = credits_db
    _recheck(db, "a")  # a pending re-check, due now
    store.request_album(db, "c", "b")
    _run(db, "b", _unresolved(db, "b"))
    _expire(db, "b")  # an expired result the sweep has not reached
    _q(db, "DROP INDEX plugin_lumae_analysis__credits_jobs_rechecked")
    _q(db, f"ALTER TABLE {JOBS} DROP COLUMN rechecked_at")
    store.migrate(db)
    [(not_before,)] = _q(db, f"SELECT EXTRACT(EPOCH FROM not_before-now())/86400 FROM {JOBS} WHERE album_id='a' AND catalog_instance_id='c'")
    assert 0 <= not_before <= 14
    assert 0 <= _ttl_days(db, "b") <= 14
    _q(db, f"UPDATE {JOBS} SET not_before=now()")
    store.migrate(db)  # once: the column is there now
    assert _q(db, f"SELECT not_before<=now() FROM {JOBS} WHERE album_id='a' AND catalog_instance_id='c'") == [(True,)]


def test_a_recheck_is_counted_once_however_often_the_sweep_sees_it(credits_db):
    db = credits_db
    store.request_album(db, "c", "a")
    _run(db, "a", _unresolved(db, "a"))
    _expire(db, "a")
    for _ in range(3):
        store.request_album(db, "c", "a")
    assert _q(db, f"SELECT status, rechecks, expires_at FROM {JOBS} WHERE album_id='a' "
                  "AND catalog_instance_id='c'") == [("pending", 1, None)]


def test_a_changed_album_starts_over(credits_db):
    db = credits_db
    store.request_album(db, "c", "a")
    _run(db, "a", _unresolved(db, "a"))
    _expire(db, "a")
    store.request_album(db, "c", "a")
    _q(db, "UPDATE plugin_lumae_analysis__catalog_albums SET name='Renamed' WHERE album_id='a'")
    store.request_album(db, "c", "a")
    assert _q(db, f"SELECT status, rechecks FROM {JOBS} WHERE album_id='a' AND catalog_instance_id='c'") == [
        ("pending", 0)]


def _work(db):
    with db.cursor() as cur:
        summary = reconcile._work_summary(cur)
    db.commit()
    return summary


def test_a_recheck_is_idle_work_and_a_changed_album_is_not(migrated_db):
    db = migrated_db
    _albums(db)
    for name in ("a", "b"):
        store.request_album(db, "c", name)
        _run(db, name, _unresolved(db, name))
    _expire(db, "a")
    store.request_album(db, "c", "a")
    assert _work(db)["ready"] == 0  # the re-check runs on the idle cadence
    _q(db, f"UPDATE {P}catalog_albums SET name='Renamed' WHERE album_id='b'")
    store.request_album(db, "c", "b")
    assert _work(db)["ready"] == 1


def _albums(db):
    """credits_db's source "c" with albums a and b, on the full migrated schema."""
    with db.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS task_status (task_id TEXT, parent_task_id TEXT, "
                    "task_type TEXT, status TEXT)")  # the host's
        cur.execute(f"""INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id,
            provider_type, server_name) VALUES ('c', 'c', 'navidrome', 'Test')""")
        cur.execute(f"""INSERT INTO {P}catalog_state (catalog_instance_id, provider_type,
            published_generation, catalog_epoch, status) VALUES ('c', 'navidrome', 1, %s, 'complete')""",
                    (uid(700),))
        for name in ("a", "b"):
            cur.execute(f"""INSERT INTO {P}catalog_albums (catalog_instance_id, published_generation, album_id,
                name, album_artist_display, metadata_fp, payload, first_seen_at, last_seen_at)
                VALUES ('c', 1, %s, 'Album', 'Artist', 'fp', '{{}}', now(), now())""", (name,))
            for i in range(3):
                cur.execute(f"""INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation,
                    track_id, album_id, title, artist_display, disc_number, track_number, duration_ms,
                    metadata_fp, payload, first_seen_at, last_seen_at)
                    VALUES ('c', 1, %s, %s, %s, 'Artist', 1, %s, 200000, 'fp', '{{}}', now(), now())""",
                            (name + str(i), name, "Song " + str(i), i + 1))
    db.commit()


# ------------------------------------------------------------------ S8


def test_an_unchanged_album_is_not_rewritten(credits_db):
    db = credits_db
    store.request_album(db, "c", "a")
    [(before,)] = _q(db, f"SELECT xmin::text FROM {JOBS} WHERE album_id='a' AND catalog_instance_id='c'")
    store.request_album(db, "c", "a")
    store.request_album(db, "c", "a", priority=0)
    assert _q(db, f"SELECT xmin::text FROM {JOBS} WHERE album_id='a' AND catalog_instance_id='c'") == [(before,)]
    store.request_album(db, "c", "a", priority=5)  # a raised priority is a change
    assert _q(db, f"SELECT xmin::text, priority FROM {JOBS} WHERE album_id='a' "
                  "AND catalog_instance_id='c'")[0] != (before, 0)


def test_a_finished_walk_rests_an_hour_or_until_a_new_generation(credits_db):
    db = credits_db
    assert store.sweep(db, "c", limit=1) == 1  # a, more to come
    assert store.sweep(db, "c", limit=1) == 1  # b
    assert store.sweep(db, "c", limit=1) == 0  # the end: the walk wraps
    assert store.sweep(db, "c", limit=1) == 0  # resting
    _q(db, "UPDATE plugin_lumae_analysis__credits_stream SET swept_at=now()-interval '61 minutes' "
           "WHERE catalog_instance_id='c'")
    assert store.sweep(db, "c", limit=1) == 1
    store.sweep(db, "c", limit=1)
    store.sweep(db, "c", limit=1)
    assert store.sweep(db, "c", limit=1) == 0
    _q(db, "UPDATE plugin_lumae_analysis__catalog_state SET published_generation=2 WHERE catalog_instance_id='c'")
    _q(db, "UPDATE plugin_lumae_analysis__catalog_albums SET published_generation=2 WHERE catalog_instance_id='c'")
    assert store.sweep(db, "c", limit=1) == 1  # a new catalogue: at once


def test_a_tick_sweeps_once_however_many_credits_turns_it_asks_for(credits_db, monkeypatch):
    db = credits_db
    sweeps = []
    monkeypatch.setattr(store, "sweep", lambda db, catalog_id, limit=32: sweeps.append(catalog_id) or 0)
    monkeypatch.setattr(credits_service, "resolve_catalog_source", lambda db, server_id=None: [
        {"catalog_instance_id": "c", "rebind_status": "active"}])
    credits_service.begin_tick()
    try:
        credits_service.reconcile(db, before_background=True)
        credits_service.reconcile(db, before_background=False)
    finally:
        credits_service.end_tick()
    assert sweeps == ["c"]
    credits_service.reconcile(db)  # outside a tick (tests, tools): sweeps
    assert sweeps == ["c", "c"]


def test_one_edge_runtime_report_per_ten_minutes_across_forked_jobs(migrated_db, monkeypatch):
    from test_lumae_analysis import load_plugin

    mod, db = load_plugin(), migrated_db
    monkeypatch.setattr(mod, "get_db", lambda: db)
    reports = []
    record = mod.record_edge_runtime
    monkeypatch.setattr(mod, "record_edge_runtime", lambda db, worker, status: (
        reports.append(worker), record(db, worker, status)))
    monkeypatch.setattr(mod, "edge_runtime_status", lambda: {"available": False, "reason": "pyav_missing"})
    for _job in range(3):
        monkeypatch.setattr(mod, "_edge_runtime_reported_at", None)  # a fresh fork per job
        mod.report_edge_runtime()
    assert len(reports) == 1
    _q(db, f"UPDATE {P}edge_runtime_state SET reported_at=now()-interval '11 minutes'")
    monkeypatch.setattr(mod, "_edge_runtime_reported_at", None)
    mod.report_edge_runtime()
    assert len(reports) == 2
    mod.report_edge_runtime(force=True)  # a worker start always reports
    assert len(reports) == 3


def test_an_idle_tick_counts_the_settings_snapshot_at_most_every_five_minutes(tick, monkeypatch):
    mod, db = tick
    refreshes = []
    monkeypatch.setattr(mod, "refresh_status_snapshot", lambda db=None, catalog_instance_id=None,
                        min_age_seconds=0, publications=False: refreshes.append(min_age_seconds))
    mod.catalog_reconcile_task()
    assert refreshes == [mod.STATUS_SNAPSHOT_IDLE_SECONDS]
    monkeypatch.setattr(mod, "serve_deferred_interactive", (lambda served: (
        lambda *a, **k: served.pop() if served else 0))([2]))
    mod.catalog_reconcile_task()
    assert refreshes[-1] == 0  # on-demand work changed profiles: counted now


class _Counting:
    """Counts the statements run through ``db``'s cursors."""

    def __init__(self, db, monkeypatch):
        import psycopg2.extensions

        self.count = 0
        counter = self

        class Cursor(psycopg2.extensions.cursor):
            def execute(self, query, vars=None):
                counter.count += 1
                return super().execute(query, vars)

        monkeypatch.setattr(db, "cursor_factory", Cursor)


# Statements of an idle tick with an idle credits library (one source, two
# albums, both current). Before WP4 it ran 59: a page of the credits walk and
# an upsert per album twice a tick, and the settings snapshot counted every
# tick. SQ.1 added one indexed read: a catalogue refresh kept for the tick.
IDLE_TICK_STATEMENTS = 27


def test_an_idle_tick_runs_few_statements(tick, monkeypatch):
    mod, db = tick
    with db.cursor() as cur:
        for name in ("a", "b"):
            cur.execute(f"""INSERT INTO {P}catalog_albums (catalog_instance_id, published_generation, album_id,
                name, album_artist_display, metadata_fp, payload, first_seen_at, last_seen_at)
                VALUES (%s, 1, %s, 'Album', 'Artist', 'fp', '{{}}', now(), now())""", (SOURCE, name))
    db.commit()
    monkeypatch.setattr(mod, "_safe_credits_reconcile", lambda db, server_id, before_background, deadline=None:
                        credits_service.reconcile(db, server_id, before_background=before_background,
                                                  deadline=deadline))
    monkeypatch.setattr(credits_service, "run_one", lambda *a, **k: pytest.fail("no album is due"))
    mod.catalog_reconcile_task()  # walks the albums once
    _q(db, f"UPDATE {JOBS} SET status='complete', expires_at=now()+interval '30 days'")
    mod.catalog_reconcile_task()  # the walk reaches its end
    counting = _Counting(db, monkeypatch)

    result = mod.catalog_reconcile_task()

    assert result["actions"] == []
    assert counting.count <= IDLE_TICK_STATEMENTS, counting.count
