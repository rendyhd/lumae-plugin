"""The catalogue watchdog's tick, through ``catalog_reconcile_task``.

Plan: docs/plan/LUMAE_SCHEDULER_PLAN_2026-10-11.md (1.6.2: S1-S6).

* S4 (AUD-15): a waveform backfill that only waits for retry-armed tracks
  keeps its own retry time. The tick used to reset it, so the row was due at
  once, the watchdog stayed on minute cadence after the backfill had
  finished, and the edge pass ordered after it never ran.
* S5 (AUD-18): the tick returns what it did (``tracks_analyzed``, one
  sentence as ``message``/``status_message``, actions, remaining work,
  cadence), logs one INFO line, journals credits, MusicBrainz and on-demand
  turns, and the journal keeps a day of events.
* S6 (AUD-18): an administrator's disable of Lumae's cron rows on AudioMuse's
  Scheduled Tasks page stays, and the settings page says what stops.
"""
import importlib

import pytest

from test_lumae_analysis import load_plugin
from plugins.LumaeAnalysis import profile_publication as publication  # noqa: E402


P = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
SERVER = "server-a"
RECONCILE = "plugin.lumae_analysis.catalog_reconcile"
RECHECK = "plugin.lumae_analysis.provider_identity_recheck"


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _reconcile():
    return importlib.import_module("plugins.LumaeAnalysis.reconcile")


@pytest.fixture
def tick(migrated_db, monkeypatch):
    """The real tick on one source; actions this module does not exercise
    report no work."""
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
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "serve_deferred_interactive", lambda *args, **kwargs: 0)
    monkeypatch.setattr(mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0)
    for name in ("next_settled_analysis_run", "next_preparation_run", "next_relationship_run"):
        monkeypatch.setattr(mod, name, lambda db=None, server_id=None: None)
    monkeypatch.setattr(mod, "_safe_credits_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "_safe_metadata_reconcile", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    monkeypatch.setattr(
        mod, "resolve_profile_source",
        lambda **_kwargs: {"catalog_instance_id": SOURCE, "server_id": SERVER},
    )
    monkeypatch.setattr(mod, "get_core_adapter", lambda: type(
        "Adapter", (), {"active_server_id": staticmethod(lambda: SERVER)})())
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


def _cron(db):
    return _rows(db, "SELECT cron_expr FROM cron WHERE task_type=%s", (RECONCILE,))[0][0]


@pytest.mark.parametrize("retry_count", [0, 2])
def test_waiting_retry_keeps_the_backfill_retry_time(tick, monkeypatch, retry_count):
    mod, db = tick
    for track in ("track-a", "track-b"):
        _retry_armed(db, track)
    _queued_backfill(db, retry_count)
    analysed = []
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: analysed.append(ids))

    result = mod.catalog_reconcile_task()

    assert [(entry["action"], entry["status"]) for entry in result["actions"]] == [
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
    # The cadence follows the backoff ladder to the retry 20 minutes away,
    # not minute ticks.
    assert _rows(db, f"SELECT mode, cron_expr FROM {P}reconcile_control")[0] == (
        "backoff", "*/15 * * * *")
    assert _cron(db) == "*/15 * * * *"
    # Until the retry is due, the next tick finds no waveform batch to run.
    assert mod.next_profile_backfill_run(db=db) is None
    assert mod.catalog_reconcile_task()["status"] == "current"
    assert _backfill_row(db)[1] == next_retry_at


def test_complete_backfill_stays_complete(tick, monkeypatch):
    mod, db = tick
    _queued_backfill(db, retry_count=1)
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: pytest.fail(ids))

    result = mod.catalog_reconcile_task()

    assert result["actions"][0]["status"] == "complete"
    assert _backfill_row(db) == ("complete", None, 0)
    assert _rows(db, f"SELECT mode FROM {P}reconcile_control")[0][0] == "idle"


def _enabled(db, task_type):
    return _rows(db, "SELECT enabled FROM cron WHERE task_type=%s", (task_type,))


def _disable(db, task_type):
    with db.cursor() as cur:
        cur.execute("UPDATE cron SET enabled=FALSE WHERE task_type=%s", (task_type,))
    db.commit()


def test_a_disabled_watchdog_stays_disabled_through_every_cadence_and_a_reinstall(
    tick, monkeypatch, run_plugin_migration,
):
    mod, db = tick
    reconcile = _reconcile()
    _disable(db, RECONCILE)

    reconcile.arm_reconcile(db, "work_admitted", commit=True)
    assert _cron(db) == reconcile.ACTIVE_CRON
    reconcile.set_paused(db, True)
    assert _cron(db) == reconcile.IDLE_CRON
    _queued_backfill(db)
    assert reconcile.reconcile_schedule_from_state(db)["mode"] == "active"
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: pytest.fail(ids))
    mod.catalog_reconcile_task()
    assert _rows(db, f"SELECT mode FROM {P}reconcile_control")[0][0] == "idle"
    assert _enabled(db, RECONCILE) == [(False,)]

    run_plugin_migration(db)
    assert _enabled(db, RECONCILE) == [(False,)]


def test_the_identity_recheck_is_installed_enabled_and_keeps_a_later_change(
    migrated_db, run_plugin_migration,
):
    db = migrated_db
    assert _rows(db, "SELECT cron_expr, enabled FROM cron WHERE task_type=%s", (RECHECK,)) == [
        ("2,32 * * * *", True)]
    with db.cursor() as cur:
        cur.execute("UPDATE cron SET enabled=FALSE, cron_expr='5 * * * *' WHERE task_type=%s",
                    (RECHECK,))
    db.commit()
    run_plugin_migration(db)
    assert _rows(db, "SELECT cron_expr, enabled FROM cron WHERE task_type=%s", (RECHECK,)) == [
        ("5 * * * *", False)]
    with db.cursor() as cur:
        cur.execute("DELETE FROM cron WHERE task_type=%s", (RECHECK,))
    db.commit()
    run_plugin_migration(db)
    assert _enabled(db, RECHECK) == [(True,)]


@pytest.mark.parametrize("disabled", [(), (RECONCILE,), (RECHECK,), (RECONCILE, RECHECK)])
def test_the_settings_page_says_what_stops_while_a_row_is_disabled(tick, disabled):
    mod, db = tick
    for task_type in disabled:
        _disable(db, task_type)

    html = mod.render_reconcile_status_panel()

    assert ("The catalogue watchdog is disabled" in html) is (RECONCILE in disabled)
    assert ("Lumae does no background work" in html) is (RECONCILE in disabled)
    assert ("The provider identity recheck is disabled" in html) is (RECHECK in disabled)
    for task_type in disabled:
        assert f"<code>{task_type}</code>" in html


# ---------------------------------------------------------------- S5 report
def _events(db):
    return _rows(
        db,
        f"SELECT action, status, phase, summary FROM {P}reconcile_events "
        "WHERE status <> 'running' ORDER BY event_id",
    )


def _info_lines(monkeypatch):
    lines = []
    monkeypatch.setattr(_reconcile(), "logger", type("Logger", (), {
        "info": staticmethod(lambda message, *args: lines.append(message % args)),
        "debug": staticmethod(lambda *args, **kwargs: None),
        "warning": staticmethod(lambda *args, **kwargs: None),
        "exception": staticmethod(lambda *args, **kwargs: None),
    })())
    return lines


def test_a_waveform_batch_reports_its_tracks_and_matches_its_event(tick, monkeypatch):
    mod, db = tick
    for track in ("track-a", "track-b", "track-c"):
        _track(db, track)
    _queued_backfill(db)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 2)
    monkeypatch.setattr(mod, "analyze_tracks_task", lambda ids, **_: {
        "processed": len(ids), "attempted": len(ids), "ready": len(ids) - 1, "failed": 1,
        "skipped": 0})
    lines = _info_lines(monkeypatch)

    result = mod.catalog_reconcile_task()

    [(action, status, phase, summary)] = _events(db)
    assert (action, status, phase) == ("profile_backfill", "success", "completed with warnings")
    assert result["tracks_analyzed"] == summary["ready"] == 1
    [entry] = result["actions"]
    assert (entry["action"], entry["count"], entry["failed"]) == ("profile_backfill", 1, 1)
    assert result["remaining"]["waveforms"] == 3
    assert result["message"] == result["status_message"]
    assert result["message"].startswith("Lumae: 1 waveform (1 failed) in ")
    assert result["message"].endswith("; 3 tracks left")
    assert result["cadence"] == "* * * * *"
    [line] = lines
    assert line.startswith("lumae_analysis watchdog: " + result["message"])
    assert "profile_backfill queued 1/1 failed" in line and "cadence * * * * *" in line


@pytest.mark.parametrize(
    ("stub", "value", "action", "message", "tracks"),
    [
        ("serve_deferred_interactive", 3, "interactive", "Lumae: 3 on-demand tracks in ", 3),
        ("_safe_credits_reconcile",
         {"status": "published", "album_id": "album-a", "match_status": "matched", "credits": 4},
         "credits", "Lumae: 1 credits album in ", 0),
        ("_safe_metadata_reconcile", {"status": "verified", "job_id": "job-a"},
         "music_metadata", "Lumae: 1 MusicBrainz lookup in ", 0),
        ("_safe_credits_reconcile", {"status": "deferred", "album_id": "album-a",
                                     "retry_seconds": 60}, "credits", "Lumae: 1 credits album in ", 0),
    ],
)
def test_credits_lookups_and_on_demand_turns_are_journaled(
    tick, monkeypatch, stub, value, action, message, tracks,
):
    mod, db = tick
    monkeypatch.setattr(mod, stub, lambda *args, **kwargs: value)

    result = mod.catalog_reconcile_task()

    [(event_action, status, phase, summary)] = _events(db)
    assert event_action == action
    assert status == ("deferred" if phase == "deferred" else "success")
    assert summary["status"] == phase
    assert [entry["action"] for entry in result["actions"]] == [action]
    assert result["message"].startswith(message)
    assert result["tracks_analyzed"] == tracks
    if action == "interactive":
        assert summary["served"] == 3
    if action == "credits" and phase == "published":
        assert (summary["match_status"], summary["credits"]) == ("matched", 4)


def test_idle_and_paused_ticks_say_so(tick, monkeypatch):
    mod, db = tick
    lines = _info_lines(monkeypatch)
    idle = mod.catalog_reconcile_task()
    assert (idle["status"], idle["message"], idle["cadence"]) == (
        "current", "Lumae: up to date", "11 * * * *")
    assert idle["actions"] == [] and idle["tracks_analyzed"] == 0
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    paused = mod.catalog_reconcile_task()
    assert (paused["status"], paused["message"], paused["cadence"]) == (
        "paused", "Lumae: paused", "11 * * * *")
    assert len(lines) == 2 and lines[1].startswith("lumae_analysis watchdog: Lumae: paused")
    assert _events(db) == []


def test_a_failed_action_still_logs_its_tick(tick, monkeypatch):
    mod, db = tick
    _queued_backfill(db)
    monkeypatch.setattr(mod, "profile_backfill_task", lambda *_args: (_ for _ in ()).throw(
        RuntimeError("decoder crashed")))
    lines = _info_lines(monkeypatch)
    with pytest.raises(RuntimeError):
        mod.catalog_reconcile_task()
    [line] = lines
    assert line.startswith("lumae_analysis watchdog: Lumae: an action failed")
    assert "actions profile_backfill failed 0 " in line
    assert _events(db)[0][:2] == ("profile_backfill", "failed")


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
