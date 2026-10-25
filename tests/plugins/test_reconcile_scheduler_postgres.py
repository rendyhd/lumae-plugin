"""The runners' actions, through ``run_runner_turn`` (runner plan RN.6).

Plan: docs/plan/LUMAE_RUNNER_PLAN_2026-10-25.md. The watchdog's tick and its
cadence, yield and schedules are gone (R8); the actions and their journal
stay (scheduler plan S4, S5):

* S4 (AUD-15): a waveform backfill that only waits for retry-armed tracks
  keeps its own retry time, and the runner sleeps until it.
* S5 (AUD-18): each turn journals credits, MusicBrainz and on-demand turns
  and the waveform, loudness and edge passes; the journal keeps a day of
  events.
* W1: the identity recheck runs while a transition needs it. R8: expired
  collection receipts go once a day; an administrator's catalogue refresh
  schedule became ``catalog_refresh_hours``.
"""
import contextlib
import importlib
import time

import pytest

from runner_helpers import turn
from test_lumae_analysis import load_plugin
from plugins.LumaeAnalysis import profile_publication as publication  # noqa: E402


P = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
SERVER = "server-a"


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else []
    db.commit()
    return rows


def _reconcile():
    return importlib.import_module("plugins.LumaeAnalysis.reconcile")


@pytest.fixture
def tick(migrated_db, monkeypatch):
    """A server runner's turn on one source, heavy work included
    (``heavy_work=server``); actions this module does not exercise report
    no work."""
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
    monkeypatch.setattr(mod.runner, "PAUSE_SECONDS", 0)
    monkeypatch.setattr(mod, "_runner_bookkeeping_at", None)
    monkeypatch.setattr(mod, "heavy_work_setting", lambda: "server")
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "serve_interactive", lambda *args, **kwargs: 0)
    monkeypatch.setattr(mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0)
    for name in ("next_settled_analysis_run", "next_preparation_run", "next_relationship_run"):
        monkeypatch.setattr(mod, name, lambda db=None, server_id=None: None)
    monkeypatch.setattr(mod, "_safe_credits_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_safe_metadata_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    monkeypatch.setattr(mod, "next_loudness_backfill", lambda db=None, server_id=None: None)
    monkeypatch.setattr(mod, "collection_retention_task", lambda: {"status": "ok"})
    monkeypatch.setattr(
        mod, "resolve_profile_source",
        lambda **_kwargs: {"catalog_instance_id": SOURCE, "server_id": SERVER},
    )
    monkeypatch.setattr(mod, "get_core_adapter", lambda: type(
        "Adapter", (), {"active_server_id": staticmethod(lambda: SERVER)})())
    # A day's retention ran already; the tests that want it clear the mark.
    with db.cursor() as cur:
        assert mod.runner.claim_mark(cur, "collection_retention", 0)
    db.commit()
    return mod, db


def _track(db, track):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, "
            "track_id, title, metadata_fp, media_fp, analysis_eligible, payload, "
            "first_seen_at, last_seen_at) "
            "VALUES (%s, 1, %s, %s, 'metadata', 'revision-a', TRUE, '{}'::jsonb, now(), now())",
            (SOURCE, track, track),
        )
    db.commit()


def _retry_armed(db, track, minutes=20):
    """A transient failure, due again in ``minutes``."""
    _track(db, track)
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    assert publication.complete_attempt(
        db, SOURCE, track, token, object(), "failed", "download failed",
        None, 1, 1, failure_code="download_unavailable",
    )
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}source_profiles SET retry_after=now()+make_interval(mins => %s) "
            "WHERE catalog_instance_id=%s AND track_id=%s",
            (minutes, SOURCE, track),
        )
    db.commit()


def _queued_backfill(db, retry_count=0):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}profile_backfill_state (catalog_instance_id, server_id, status, "
            "retry_count) VALUES (%s, %s, 'queued', %s)",
            (SOURCE, SERVER, retry_count),
        )
    db.commit()


def _backfill_row(db):
    return _rows(
        db,
        f"SELECT status, next_retry_at, retry_count FROM {P}profile_backfill_state "
        "WHERE catalog_instance_id=%s",
        (SOURCE,),
    )[0]


@pytest.mark.parametrize("retry_count", [2])  # 0 takes the same branch
def test_waiting_retry_keeps_the_backfill_retry_time(tick, monkeypatch, retry_count):
    mod, db = tick
    for track in ("track-a", "track-b"):
        _retry_armed(db, track)
    _queued_backfill(db, retry_count)
    analysed = []
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: analysed.append(ids))

    result, actions = turn(mod)

    assert [(entry["action"], entry["status"]) for entry in actions] == [
        ("profile_backfill", "waiting_retry")]
    assert analysed == []
    status, next_retry_at, stored_retry_count = _backfill_row(db)
    assert status == "queued"
    assert next_retry_at is not None
    # The soonest track retry, as the batch stored it.
    assert _rows(
        db,
        f"SELECT abs(extract(epoch FROM (min(retry_after)::timestamptz - %s))) < 1 "
        f"FROM {P}source_profiles WHERE catalog_instance_id=%s",
        (next_retry_at, SOURCE),
    ) == [(True,)]
    # The batch itself succeeded: its own failure count starts again.
    assert stored_retry_count == 0
    with db.cursor() as cur:
        summary = _reconcile()._work_summary(cur)
    db.commit()
    assert summary["ready"] == 0
    assert summary["next_retry_at"] is not None
    # Until the retry is due the next turn finds nothing, and sleeps until it
    # (20 minutes), not a minute.
    assert mod.next_profile_backfill_run(db=db) is None
    idle, actions = turn(mod)
    assert (idle.worked, actions) == (False, [])
    assert 18 * 60 < idle.next_due_seconds <= 20 * 60
    assert _backfill_row(db)[1] == next_retry_at


def test_complete_backfill_stays_complete(tick, monkeypatch):
    mod, db = tick
    _queued_backfill(db, retry_count=1)
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: pytest.fail(ids))

    _result, actions = turn(mod)

    assert actions[0]["status"] == "complete"
    assert _backfill_row(db) == ("complete", None, 0)


def _events(db):
    return _rows(
        db,
        f"SELECT action, status, phase, summary FROM {P}reconcile_events "
        "WHERE status <> 'running' ORDER BY event_id",
    )


def test_a_waveform_batch_reports_its_tracks_and_matches_its_event(tick, monkeypatch):
    mod, db = tick
    for track in ("track-a", "track-b", "track-c"):
        _track(db, track)
    _queued_backfill(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 2)
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: {
        "processed": len(ids), "attempted": len(ids), "ready": len(ids) - 1, "failed": 1,
        "skipped": 0})

    result, actions = turn(mod)

    # Batches of two and one in one turn, each journaled; each batch's stub
    # fails one track.
    events = _events(db)
    assert [(action, status) for action, status, _phase, _summary in events] == [
        ("profile_backfill", "success")] * 2
    [entry] = actions
    assert (entry["action"], entry["runs"]) == ("profile_backfill", 2)
    assert entry["count"] == sum(summary["ready"] for *_rest, summary in events) == 1
    assert entry["failed"] == sum(summary["failed"] for *_rest, summary in events) == 2
    assert result.worked and result.action == "profile_backfill"


@pytest.mark.parametrize(
    ("stub", "value", "action"),
    [
        ("serve_interactive", 3, "interactive"),
        ("_safe_credits_reconcile",
         {"status": "published", "album_id": "album-a", "match_status": "matched", "credits": 4},
         "credits"),
        ("_safe_metadata_reconcile", {"status": "verified", "job_id": "job-a"}, "music_metadata"),
        ("_safe_credits_reconcile", {"status": "deferred", "album_id": "album-a",
                                     "retry_seconds": 60}, "credits"),
    ],
)
def test_credits_lookups_and_on_demand_turns_are_journaled(tick, monkeypatch, stub, value, action):
    mod, db = tick
    queued = [value]
    monkeypatch.setattr(mod, stub, lambda *args, **kwargs: queued.pop() if queued else (
        0 if stub == "serve_interactive" else None))

    _result, actions = turn(mod)

    [(event_action, status, phase, summary)] = _events(db)
    assert event_action == action
    assert status == ("deferred" if phase == "deferred" else "success")
    assert summary["status"] == phase
    assert [entry["action"] for entry in actions] == [action]
    if action == "interactive":
        assert summary["served"] == 3 and actions[0]["count"] == 3
    if action == "credits" and phase == "published":
        assert (summary["match_status"], summary["credits"]) == ("matched", 4)


def _batch_result(ids, **_kwargs):
    return {"processed": len(ids), "attempted": len(ids), "ready": len(ids), "failed": 0,
            "skipped": 0}


def test_a_turn_runs_due_actions_highest_priority_first(tick, monkeypatch):
    """Plan S2: one turn runs several actions, highest priority first, and
    starts the order again after each."""
    mod, db = tick
    calls = []
    runs = [{"server_id": SERVER, "catalog_instance_id": SOURCE, "run_id": "run-a"}]
    monkeypatch.setattr(mod, "next_settled_analysis_run",
                        lambda db=None, server_id=None: runs.pop(0) if runs else None)
    monkeypatch.setattr(mod, "finalize_analysis_run_task",
                        lambda *args: calls.append(("analysis", args)) or {"status": "complete"})
    for track in ("track-a", "track-b", "track-c"):
        _track(db, track)
    _queued_backfill(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 2)
    monkeypatch.setattr(mod, "analyze_tracks_task",
                        lambda ids, **kw: calls.append(("waveforms", ids)) or _batch_result(ids))

    result, actions = turn(mod)

    assert result.worked
    assert [(entry["action"], entry["runs"], entry["count"]) for entry in actions] == [
        ("analysis_run", 1, 1), ("profile_backfill", 2, 3)]
    assert calls[0] == ("analysis", (SERVER, SOURCE, "run-a"))
    assert [name for name, _ids in calls[1:]] == ["waveforms", "waveforms"]


@pytest.mark.parametrize(
    ("background", "metadata_results", "expected_actions", "expected_turns"),
    [
        # A due lookup whose turn has come runs before the library backfill,
        # and the backfill runs after it in the same turn.
        (True, {True: [{"status": "verified", "job_id": "job-a"}]},
         ["music_metadata", "profile_backfill"], [True, False]),
        # A lookup that ran recently waits; the backfill takes the turn.
        (True, {}, ["profile_backfill"], [True, False]),
        # With no background work left, due lookups run one after another.
        (False, {False: [{"status": "verified", "job_id": "job-a"},
                         {"status": "verified", "job_id": "job-b"}]},
         ["music_metadata"], [True, False, False, False]),
        (False, {}, [], [True, False]),
    ],
)
def test_lookups_take_turns_with_background_work(
        tick, monkeypatch, background, metadata_results, expected_actions, expected_turns):
    mod, db = tick
    turns = []
    if background:
        _track(db, "track-a")
        _queued_backfill(db)
        monkeypatch.setattr(mod, "analyze_tracks_task", _batch_result)

    def metadata(_db, before_background, deadline=None):
        turns.append(before_background)
        assert deadline is not None and deadline > time.monotonic()
        queued = metadata_results.get(before_background) or []
        return queued.pop(0) if queued else None

    monkeypatch.setattr(mod, "_safe_metadata_reconcile", metadata)
    lookups = sum(len(results) for results in metadata_results.values())

    result, actions = turn(mod)

    assert [entry["action"] for entry in actions] == expected_actions
    assert turns == expected_turns
    if expected_actions[:1] == ["music_metadata"]:
        assert (actions[0]["status"], actions[0]["runs"]) == ("verified", lookups)
    assert result.worked == bool(expected_actions)


def test_each_action_runs_bound_to_its_source_server(tick, monkeypatch):
    """R3: a runner thread starts without AudioMuse's server binding, so each
    action binds its source's server, as the queue bound a cron task."""
    mod, db = tick
    bound, seen = [], []

    class Adapter:
        @staticmethod
        def active_server_id():
            return bound[-1] if bound else None

        @staticmethod
        def bind(server_id):
            @contextlib.contextmanager
            def scope():
                bound.append(server_id)
                try:
                    yield
                finally:
                    bound.pop()
            return scope()

    monkeypatch.setattr(mod, "get_core_adapter", lambda: Adapter())
    _track(db, "track-a")
    _queued_backfill(db)
    monkeypatch.setattr(mod, "analyze_tracks_task",
                        lambda ids, **kw: seen.append(list(bound)) or _batch_result(ids))

    turn(mod)

    assert seen == [[SERVER]] and bound == []
    # Outside a runner the caller's binding stands.
    with mod._source_binding(SERVER):
        assert bound == []


def test_idle_and_paused_turns_do_nothing(tick, monkeypatch):
    mod, db = tick
    idle, actions = turn(mod)
    assert (idle.worked, idle.mode, actions) == (False, "idle", [])
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    paused, actions = turn(mod)
    assert (paused.mode, paused.reason, actions) == ("paused", "maintenance_paused", [])
    assert _events(db) == []


def _loudness_on(mod, monkeypatch):
    monkeypatch.setattr(mod, "next_loudness_backfill", lambda db=None, server_id=None: (
        mod.loudness_backfill.next_due(db, server_id=server_id)))


def test_the_loudness_turn_comes_after_the_waveform_batch(tick, monkeypatch):
    """LN.4c: last of the track work. A measurement that fails backs off, and the pass finishes."""
    mod, db = tick
    _loudness_on(mod, monkeypatch)
    _track(db, "track-a")
    _queued_backfill(db)
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: {
        "processed": len(ids), "attempted": len(ids), "ready": len(ids), "failed": 0, "skipped": 0})
    downloads = []
    monkeypatch.setattr(mod, "load_track_file", lambda track_id, **_: downloads.append(track_id))

    _result, actions = turn(mod)

    journal = [(action, status) for action, status, _phase, _summary in _events(db)]
    assert journal.index(("loudness_backfill", "success")) > journal.index(("profile_backfill", "success"))
    entry = next(entry for entry in actions if entry["action"] == "loudness_backfill")
    assert (entry["count"], entry["failed"]) == (0, 1) and downloads == ["track-a"]
    assert mod.loudness_backfill.state(db, SOURCE)["status"] == "complete"


def test_a_finished_loudness_pass_rests_until_its_resweep(tick, monkeypatch):
    mod, db = tick
    _loudness_on(mod, monkeypatch)
    _first, actions = turn(mod)  # follows the catalogue; nothing to measure
    assert [entry["action"] for entry in actions] == ["loudness_backfill"]
    idle, actions = turn(mod)
    assert (idle.worked, actions) == (False, [])


def test_a_failed_action_is_journaled_and_the_turn_goes_on(tick, monkeypatch):
    mod, db = tick
    _queued_backfill(db)
    monkeypatch.setattr(mod, "profile_backfill_task", lambda *_args: (_ for _ in ()).throw(
        RuntimeError("decoder crashed")))
    credits = [{"status": "published", "album_id": "album-a"}]
    monkeypatch.setattr(mod, "_safe_credits_reconcile",
                        lambda *a, before_background, **k: None if before_background or not credits
                        else credits.pop())
    result, actions = turn(mod)
    assert _events(db)[0][:2] == ("profile_backfill", "failed")
    assert [entry["action"] for entry in actions] == ["profile_backfill", "credits"]
    assert result.worked


def test_the_journal_keeps_a_day_per_source_within_its_bounds(tick):
    mod, db = tick
    reconcile = _reconcile()
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
            "provider_type, server_name, is_default, rebind_status) "
            "VALUES ('catalog-b', 'server-b', 'navidrome', 'B', FALSE, 'active')"
        )
        # catalog-a: 1,100 events in the last day and 100 older; catalog-b:
        # 30 events older than a day; one event still running.
        cur.execute(
            f"""INSERT INTO {P}reconcile_events (catalog_instance_id, action, status, phase,
                    started_at, completed_at)
                SELECT 'catalog-a', 'profile_backfill', 'success', 'complete',
                       now() - make_interval(hours => CASE WHEN i <= 100 THEN 30 ELSE 1 END),
                       now() - make_interval(hours => CASE WHEN i <= 100 THEN 30 ELSE 1 END)
                  FROM generate_series(1, 1200) i
                UNION ALL
                SELECT 'catalog-b', 'credits', 'success', 'published',
                       now() - interval '30 hours', now() - interval '30 hours'
                  FROM generate_series(1, 30)"""
        )
        cur.execute(
            f"INSERT INTO {P}reconcile_events (catalog_instance_id, action, status, phase, "
            "started_at) VALUES ('catalog-b', 'edge_backfill', 'running', 'claiming', "
            "now() - interval '3 days')"
        )
    db.commit()

    reconcile.record_event(db, "credits", SERVER, "catalog-a", "success", phase="published",
                           duration_ms=1200)

    counts = dict(_rows(
        db,
        f"SELECT catalog_instance_id || ':' || (status = 'running')::text, count(*) "
        f"FROM {P}reconcile_events GROUP BY 1",
    ))
    assert counts == {"catalog-a:false": 1000, "catalog-b:false": 25, "catalog-b:true": 1}
    # catalog-a keeps its newest: the one just recorded and the last day's.
    assert _rows(
        db,
        f"SELECT count(*) FROM {P}reconcile_events WHERE catalog_instance_id='catalog-a' "
        "AND completed_at < now() - interval '1 day'",
    ) == [(0,)]
    assert _rows(
        db,
        f"SELECT duration_ms, completed_at - started_at FROM {P}reconcile_events "
        "WHERE catalog_instance_id='catalog-a' ORDER BY event_id DESC LIMIT 1",
    )[0][0] == 1200
    # The settings journal still shows the latest 25.
    assert len(reconcile.read_reconcile_status(db)["events"]) == 25


def test_the_identity_recheck_runs_while_a_transition_needs_it(tick, monkeypatch):
    """W1: a runner action while a transition needs it, at most every 30 minutes."""
    mod, db = tick
    runs = []
    monkeypatch.setattr(mod, "provider_identity_recheck_task",
                        lambda server_id=None: runs.append(server_id) or {"checked": 1, "results": []})

    def transition(sql, params=()):
        with db.cursor() as cur:
            cur.execute(sql.format(table=f"{P}provider_identity_transitions"), params)
        db.commit()

    turn(mod)
    assert runs == []  # no transition
    transition("INSERT INTO {table} (catalog_instance_id, state) VALUES (%s, 'transition_pending')",
               (SOURCE,))
    turn(mod)
    assert runs == [SERVER]
    turn(mod)
    assert runs == [SERVER]  # not again within 30 minutes
    transition("UPDATE {table} SET recheck_attempted_at=now()-interval '31 minutes'")
    turn(mod)
    assert runs == [SERVER, SERVER]
    transition("UPDATE {table} SET state='applied', audiomuse_health='ready', "
               "recheck_attempted_at=NULL")
    turn(mod)
    assert len(runs) == 2  # applied, AudioMuse ready: nothing to do
    transition("UPDATE {table} SET projection_reconcile_required=TRUE")
    turn(mod)
    assert len(runs) == 3  # a projection owed


def test_expired_collection_receipts_go_once_a_day(tick, monkeypatch):
    mod, db = tick
    runs = []
    monkeypatch.setattr(mod, "collection_retention_task", lambda: runs.append(1) or {"status": "ok"})
    turn(mod)
    assert runs == []  # the fixture's mark: today's ran
    _rows(db, f"UPDATE {P}runner_marks SET done_at=now()-interval '25 hours'")
    result, actions = turn(mod)
    assert runs == [1] and [entry["action"] for entry in actions] == ["collection_retention"]
    turn(mod)
    assert runs == [1]


def test_a_catalogue_refresh_cadence_wants_old_catalogues_refreshed(tick, monkeypatch):
    mod, db = tick
    refreshed = []
    monkeypatch.setattr(mod, "wanted_catalog_refresh_task",
                        lambda server, catalog: refreshed.append(catalog) or {"status": "complete"})
    _rows(db, f"UPDATE {P}catalog_state SET completed_at=now()-interval '7 hours'")
    turn(mod)
    assert refreshed == []  # no cadence by default: refreshes only when asked
    monkeypatch.setattr(mod, "catalog_refresh_hours", lambda: 6.0)
    turn(mod)
    assert refreshed == [SOURCE]


def test_the_install_leaves_no_lumae_schedule_and_keeps_a_refresh_cadence(
    migrated_db, run_plugin_migration, monkeypatch,
):
    mod = load_plugin()
    db = migrated_db
    with db.cursor() as cur:
        cur.execute("INSERT INTO cron (name, task_type, cron_expr, enabled) VALUES "
                    "('r', 'plugin.lumae_analysis.catalog_reconcile', '* * * * *', TRUE), "
                    "('c', 'plugin.lumae_analysis.catalog_refresh', '17 */6 * * *', TRUE), "
                    "('o', 'plugin.other.sync', '7 * * * *', TRUE)")
        cur.execute("CREATE TABLE cron_retry (task_type TEXT PRIMARY KEY)")
        cur.execute("INSERT INTO cron_retry VALUES ('plugin.lumae_analysis.catalog_refresh'), "
                    "('plugin.other.sync')")
    db.commit()
    saved = {}
    monkeypatch.setattr(mod, "set_setting", lambda key, value: saved.update({key: value}))
    run_plugin_migration(db)
    assert _rows(db, "SELECT task_type FROM cron ORDER BY task_type") == [("plugin.other.sync",)]
    assert _rows(db, "SELECT task_type FROM cron_retry") == [("plugin.other.sync",)]
    assert saved == {"catalog_refresh_hours": 6}
