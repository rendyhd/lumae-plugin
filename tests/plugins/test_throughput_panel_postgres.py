"""Performance plan WP5 (scheduler plan S12, AUD-18): the settings page shows,
for each queue, its throughput per hour from the journal, what is left and
an ETA; for the analysis queues the ETA at the current number of files
analysed at once and at a higher one. Since the runners (R9) the panel shows
each runner's state, not a schedule's or a refused AudioMuse schedule's.
Since 1.10.1 the counts are the throughput table's, not the journal's.
"""
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest

from test_lumae_analysis import load_plugin, plugin_client  # noqa: F401
from test_reconcile_scheduler_postgres import P, SOURCE, SERVER, _rows, tick  # noqa: F401
from plugins.LumaeAnalysis import parallel_analysis as par
from plugins.LumaeAnalysis import reconcile
from plugins.LumaeAnalysis import settings_render


def _event(db, action, minutes_ago, **summary):
    reconcile.record_event(db, action, SERVER, SOURCE, "success", phase="done", duration_ms=40000,
                           summary=summary)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}reconcile_events SET completed_at=now()-make_interval(mins => %s) "
                    "WHERE event_id=(SELECT max(event_id) FROM {P}reconcile_events)".replace("{P}", P),
                    (minutes_ago,))
        # Its units went to this minute's count: the test's minute is when it finished.
        cur.execute(f"""
            WITH moved AS (DELETE FROM {P}throughput_counts
                            WHERE minute > now() - interval '2 minutes' AND action=%s RETURNING units)
            INSERT INTO {P}throughput_counts AS counts (minute, action, units)
            SELECT date_trunc('minute', now() - make_interval(mins => %s)), %s, units FROM moved
            ON CONFLICT (minute, action) DO UPDATE SET units = counts.units + EXCLUDED.units
            """, (action, minutes_ago, action))
    db.commit()


def _counts(db):
    with db.cursor() as cur:
        cur.execute(f"SELECT action, sum(units) FROM {P}throughput_counts WHERE units > 0 GROUP BY action")
        return {action: int(units) for action, units in cur.fetchall()}


def _before_the_counts(db):
    with db.cursor() as cur:
        cur.execute(f"DROP TABLE {P}throughput_counts")
    db.commit()


def _upgrade(db):
    reconcile.migrate_reconcile(db)
    db.commit()


def _summary(db, ready, total):
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}status_summary (catalog_instance_id, profile_total_count, "
                    "profile_ready_count, profile_failed_count, profile_skipped_count, profile_counted_at) "
                    "VALUES (%s, %s, %s, 0, 0, now())", (SOURCE, total, ready))
    db.commit()


def test_throughput_counts_the_last_hour_and_day_per_queue(tick):
    mod, db = tick
    _event(db, "profile_backfill", 10, ready=40)
    _event(db, "profile_backfill", 50, ready=60)
    _event(db, "profile_backfill", 300, ready=1000)  # earlier today
    _event(db, "profile_backfill", 60 * 30, ready=9999)  # yesterday: not counted
    _event(db, "edge_backfill", 5, ready=12)
    _event(db, "credits", 20, status="published")
    _event(db, "credits", 25, status="current")  # a turn that found nothing
    _event(db, "relationships", 30, status="queued")

    queues = {queue["action"]: queue for queue in reconcile.queue_throughput(db)}

    assert (queues["profile_backfill"]["last_hour"], queues["profile_backfill"]["last_day"]) == (100, 1100)
    assert queues["edge_backfill"]["last_hour"] == 12
    assert queues["credits"]["last_hour"] == 1
    assert queues["relationships"]["last_hour"] == 1
    assert queues["loudness_backfill"]["last_hour"] == 0


@pytest.mark.parametrize("minutes,eta", [
    (59, 2.0),  # about an hour's work: 250 an hour
    (10, 0.35),  # 250 in ten minutes is 1,500 an hour, not 250
    (2, 500 * (5 / 60) / 250),  # under five minutes says little: counted as five
])
def test_remaining_and_eta(tick, minutes, eta):
    mod, db = tick
    _summary(db, ready=1000, total=1500)
    _event(db, "profile_backfill", minutes, ready=250)

    [waveforms] = [q for q in reconcile.queue_throughput(db) if q["action"] == "profile_backfill"]

    assert waveforms["remaining"] == 500
    assert waveforms["eta_hours"] == pytest.approx(eta, abs=0.04)


def test_no_throughput_means_no_eta(tick):
    mod, db = tick
    _summary(db, ready=1000, total=1500)
    [waveforms] = [q for q in reconcile.queue_throughput(db) if q["action"] == "profile_backfill"]
    assert waveforms["remaining"] == 500 and waveforms["eta_hours"] is None


def _eligible(db, tracks):
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}status_summary (catalog_instance_id, eligible_track_count) "
                    "VALUES (%s, %s)", (SOURCE, tracks))
    db.commit()


def _flush_statistics(db):
    """A backend hands its row counts to the statistics system when it goes
    idle, at most once a second: force it, or an ANALYZE that follows counts
    the rows and the late hand-over counts them again."""
    with db.cursor() as cur:
        cur.execute("SELECT pg_stat_force_next_flush()")
    db.commit()


def _loudness_rows(db, start, stop):
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}loudness_tracks (catalog_instance_id, track_id, media_revision, "
                    "version, record, histogram) SELECT %s, 't-'||i, 'r', 1, '{}'::jsonb, '\\x00'::bytea "
                    "FROM generate_series(%s, %s - 1) i", (SOURCE, start, stop))
    db.commit()
    _flush_statistics(db)


def _analyze_loudness(db):
    with db.cursor() as cur:
        cur.execute(f"ANALYZE {P}loudness_tracks")
    db.commit()


def _loudness_left(db):
    [loudness] = [q for q in reconcile.queue_throughput(db) if q["action"] == "loudness_backfill"]
    return loudness["remaining"]


def test_what_is_left_follows_rows_added_since_the_last_analyze(tick):
    """Loudness and edges count what is left against the live row count. The
    planner's reltuples waits for an ANALYZE, which autovacuum skips until a
    tenth of the table has changed: on the owner's 38k-track library the panel
    said about 2,890 loudness records were left for nine days while 14 were."""
    mod, db = tick
    _eligible(db, 100)
    _loudness_rows(db, 0, 90)
    _analyze_loudness(db)
    _loudness_rows(db, 90, 95)

    assert _loudness_left(db) == 5


def test_after_a_statistics_reset_what_is_left_uses_the_larger_count(tick):
    """A crash or pg_stat_reset zeroes the live count until the table's next
    vacuum or analyze: the planner's figure stands in meanwhile."""
    mod, db = tick
    _eligible(db, 100)
    _loudness_rows(db, 0, 90)
    _analyze_loudness(db)
    try:
        with db.cursor() as cur:
            cur.execute(f"SELECT pg_stat_reset_single_table_counters('{P}loudness_tracks'::regclass)")
        db.commit()
    except psycopg2.errors.InsufficientPrivilege:
        db.rollback()
        pytest.skip("resetting table statistics needs a superuser")
    _loudness_rows(db, 90, 92)

    # The live count says 2; the planner's 90 stands in.
    assert _loudness_left(db) == 10


def test_the_counts_outlive_a_full_journal(tick, monkeypatch):
    # Six worker runners fill a source's 1,000 events in about an hour: the
    # panel's span fell from 3.6 hours to 1.0 as a loudness pass ran on.
    mod, db = tick
    monkeypatch.setattr(reconcile, "EVENT_RETENTION_MAX", 4)
    for minutes in (600, 300, 240, 180, 120, 50, 10):  # oldest first: the 4 newest stay in the journal
        _event(db, "relationships", minutes, status="queued")
    monkeypatch.setattr(mod, "background_workers", lambda: 2)
    monkeypatch.setattr(par, "available_cpus", lambda: 8)

    [relationships] = [q for q in reconcile.queue_throughput(db) if q["action"] == "relationships"]
    body = settings_render.render_reconcile_status_panel()

    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {P}reconcile_events")
        assert cur.fetchone()[0] == 4
    assert (relationships["last_hour"], relationships["last_day"]) == (2, 7)
    assert relationships["day_hours"] is None
    assert "2 in the last hour, 7 in 24 hours" in body


def test_a_finished_event_counts_its_units(tick):
    mod, db = tick
    running = reconcile.begin_event(db, "loudness_backfill", SERVER, SOURCE)
    assert _counts(db) == {}

    reconcile.finish_event(db, running, "success", phase="done", summary={"ready": 7})
    second = reconcile.begin_event(db, "loudness_backfill", SERVER, SOURCE)
    reconcile.finish_event(db, second, "deferred", phase="done", summary={"ready": 5})
    failed = reconcile.begin_event(db, "loudness_backfill", SERVER, SOURCE)
    reconcile.finish_event(db, failed, "failed", phase="failed", error="no", summary={"ready": 100})
    empty = reconcile.begin_event(db, "edge_backfill", SERVER, SOURCE)
    reconcile.finish_event(db, empty, "success", phase="done", summary={"ready": 0})
    other = reconcile.begin_event(db, "catalog_preparation", SERVER, SOURCE)
    reconcile.finish_event(db, other, "success", phase="done", summary={"ready": 9})
    for status in ("published", "current"):
        reconcile.record_event(db, "credits", SERVER, None, "success", phase=status, duration_ms=10,
                               summary={"status": status})

    assert _counts(db) == {"loudness_backfill": 12, "credits": 1}


def test_runners_finishing_at_once_lose_no_counts(tick, second_connection):
    # Every runner writes this minute's row.
    mod, db = tick

    def finish(connection):
        for _ in range(30):
            reconcile.record_event(connection, "loudness_backfill", SERVER, SOURCE, "success",
                                   phase="done", duration_ms=10, summary={"ready": 2})

    with ThreadPoolExecutor(2) as pool:
        list(pool.map(finish, (db, second_connection)))

    assert _counts(db) == {"loudness_backfill": 120}


def test_a_count_that_fails_does_not_cost_the_event(tick):
    mod, db = tick
    _before_the_counts(db)

    event = reconcile.begin_event(db, "loudness_backfill", SERVER, SOURCE)
    reconcile.finish_event(db, event, "success", phase="done", summary={"ready": 3})
    reconcile.record_event(db, "credits", SERVER, SOURCE, "success", phase="done", duration_ms=10,
                           summary={"status": "published"})

    with db.cursor() as cur:
        cur.execute(f"SELECT action, status FROM {P}reconcile_events ORDER BY event_id")
        assert cur.fetchall() == [("loudness_backfill", "success"), ("credits", "success")]


def test_counts_older_than_a_day_go(tick):
    mod, db = tick
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}throughput_counts (minute, action, units) VALUES "
                    "(now() - interval '26 hours', 'loudness_backfill', 5), "
                    "(now() - interval '24 hours', 'loudness_backfill', 6)")
    db.commit()

    reconcile.record_event(db, "relationships", SERVER, SOURCE, "success", phase="done", duration_ms=10)

    assert _counts(db) == {"loudness_backfill": 6, "relationships": 1}


def _full_journal(db, monkeypatch, minutes):
    """A full journal from 1.10.0: its 4 newest events stay."""
    monkeypatch.setattr(reconcile, "EVENT_RETENTION_MAX", 4)
    for finished in minutes:
        _event(db, "loudness_backfill", finished, ready=100)


def _full_journal_upgraded(db, monkeypatch, minutes):
    _full_journal(db, monkeypatch, minutes)
    _before_the_counts(db)
    _upgrade(db)


def test_an_upgrade_counts_from_a_full_journal_and_says_how_far_back(tick, monkeypatch):
    mod, db = tick
    _full_journal(db, monkeypatch, (600, 300, 240, 180, 120, 50, 10))
    reconcile.record_event(db, "credits", SERVER, None, "success", phase="done", duration_ms=10,
                           summary={"status": "published"})
    with db.cursor() as cur:  # another source's journal, not full, from before the cut
        cur.execute(f"UPDATE {P}reconcile_events SET completed_at=now()-interval '400 minutes' "
                    "WHERE catalog_instance_id IS NULL")
    db.commit()
    _before_the_counts(db)
    _upgrade(db)
    monkeypatch.setattr(mod, "background_workers", lambda: 2)
    monkeypatch.setattr(par, "available_cpus", lambda: 8)

    queues = {queue["action"]: queue for queue in reconcile.queue_throughput(db)}
    body = settings_render.render_reconcile_status_panel()

    assert (queues["loudness_backfill"]["last_hour"], queues["loudness_backfill"]["last_day"]) == (200, 400)
    assert queues["loudness_backfill"]["day_hours"] == pytest.approx(3.0, abs=0.05)
    assert queues["credits"]["last_day"] == 0  # every queue counts from the same moment
    assert "200 in the last hour, 400 in the last 3.0 hours" in body


def test_an_upgrade_counts_once(tick, monkeypatch):
    mod, db = tick
    _full_journal_upgraded(db, monkeypatch, (300, 200, 40, 30, 20, 10))
    before = _counts(db)

    _upgrade(db)

    assert before == {"loudness_backfill": 400} == _counts(db)


def test_the_span_goes_when_the_counts_have_run_a_day(tick, monkeypatch):
    mod, db = tick
    _full_journal_upgraded(db, monkeypatch, (300, 200, 40, 30, 20, 10))
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}throughput_counts SET minute=now() - interval '24 hours 1 minute' "
                    "WHERE action=%s", (reconcile.COUNTED_SINCE,))
    db.commit()

    [loudness] = [q for q in reconcile.queue_throughput(db) if q["action"] == "loudness_backfill"]

    assert loudness["day_hours"] is None


def test_counts_under_an_hour_say_so_and_rate_what_they_hold(tick, monkeypatch):
    # 400 in 36 minutes said "400 in the last hour" and, of 1,000 left, 2.5
    # hours instead of 1.5.
    mod, db = tick
    _full_journal_upgraded(db, monkeypatch, (300, 200, 36, 27, 18, 9))
    _eligible(db, 1000)
    monkeypatch.setattr(mod, "background_workers", lambda: 2)
    monkeypatch.setattr(par, "available_cpus", lambda: 8)

    [loudness] = [q for q in reconcile.queue_throughput(db) if q["action"] == "loudness_backfill"]
    body = settings_render.render_reconcile_status_panel()

    assert loudness["eta_hours"] == pytest.approx(1.5, abs=0.05)
    assert "400 in the last 36 minutes; about 1,000 left, about 1.5 hours at this rate" in body
    assert "400 in the last hour" not in body


def test_an_upgrade_from_a_journal_that_holds_the_day_says_24_hours(tick):
    mod, db = tick
    _event(db, "relationships", 600, status="queued")
    _before_the_counts(db)
    _upgrade(db)
    [relationships] = [q for q in reconcile.queue_throughput(db) if q["action"] == "relationships"]
    assert (relationships["last_day"], relationships["day_hours"]) == (1, None)


@pytest.mark.parametrize("hours,text", [
    (0.2, "about 12 minutes"), (2.0, "about 2.0 hours"), (36, "about 1.5 days"), (None, ""),
])
def test_eta_wording(hours, text):
    assert settings_render._eta_text(hours) == text


@pytest.mark.parametrize("hours,text", [
    (None, "24 hours"), (5.96, "the last 6.0 hours"), (0.5, "the last 30 minutes"),
])
def test_day_window_wording(hours, text):
    assert settings_render._window_text(hours) == text


def test_the_eta_at_more_files_at_once_follows_the_measured_scaling():
    # 8 cores: K=2 1.86x, K=4 3.38x, K=6 4.32x over K=1 (parallel_bench).
    assert par.next_workers(1, cpus=8) == 2 and par.next_workers(2, cpus=8) == 4
    assert par.next_workers(6, cpus=8) is None  # nothing measured above 6
    assert par.next_workers(2, cpus=3) is None  # not past the cores
    assert par.speedup(4) == pytest.approx(3.38) and par.speedup(3) == pytest.approx((1.86 + 3.38) / 2)


def test_the_panel_renders_throughput_and_eta(tick, monkeypatch):
    mod, db = tick
    _summary(db, ready=1000, total=1500)
    _event(db, "profile_backfill", 59, ready=250)
    monkeypatch.setattr(mod, "background_workers", lambda: 2)
    monkeypatch.setattr(par, "available_cpus", lambda: 8)

    body = settings_render.render_reconcile_status_panel()

    assert "Throughput" in body
    assert "Volume and ramps" in body and "250 in the last hour" in body
    assert "500 left, about 2.0 hours at this rate" in body
    assert "Files analyzed at once: 2 (auto)." in body
    assert "At 4 at once: about 1.1 hours" in body  # 2.0 h x 1.86 / 3.38


def test_with_every_worker_the_panel_gives_the_cap_and_no_single_runner_estimate(tick, monkeypatch):
    # Multi-worker plan R6: the rates are every runner's together; one runner's K says little.
    mod, db = tick
    _summary(db, ready=1000, total=1500)
    _event(db, "profile_backfill", 59, ready=250)
    monkeypatch.setattr(mod, "background_workers", lambda: 2)
    monkeypatch.setattr(par, "available_cpus", lambda: 8)
    monkeypatch.setattr(mod, "shared_passes", lambda: True)
    monkeypatch.setattr(mod, "max_files_in_flight", lambda: 12)

    body = settings_render.render_reconcile_status_panel()

    assert "500 left, about 2.0 hours at this rate" in body
    assert "Files analyzed at once: 2 per worker (auto), at most 12 across all workers." in body
    assert "at once: about" not in body


@pytest.mark.parametrize(("state", "server_line"), [
    ("working", "<strong>Server</strong>: working: profile backfill."),
    ("waiting_host", "<strong>Server</strong>: stepping aside: waiting for AudioMuse’s main analysis."),
    ("idle", "<strong>Server</strong>: idle: nothing due."),
    ("paused", "<strong>Server</strong>: paused by the administrator."),
    ("stopped", "<strong>Server</strong>: not running (last seen 10 minutes ago)."),
    ("unavailable", "<strong>Server</strong>: cannot run: this AudioMuse has no "
                    "database.connect_raw (AudioMuse 3.2.0 or newer)."),
])
def test_the_panel_renders_in_every_state_and_stays_accessible(tick, monkeypatch, state, server_line):
    from test_lumae_analysis import _a11y_lint

    mod, db = tick
    _summary(db, ready=1000, total=1500)
    _event(db, "profile_backfill", 10, ready=250)
    mode, reason, action = {
        "working": ("working", "light_lane", "profile_backfill"),
        "waiting_host": ("waiting_host", "audiomuse:main_analysis", None),
        "idle": ("idle", "nothing_due", None),
        "paused": ("paused", "maintenance_paused", None),
        "stopped": ("idle", "nothing_due", None),
        "unavailable": ("unavailable", "host_without_connect_raw", None),
    }[state]
    seen = {"stopped": "now() - interval '10 minutes'", "unavailable": "NULL"}.get(state, "now()")
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}runner_state (role, host, pid, mode, reason, current_action, "
                    f"actions, heartbeat_at) VALUES ('server', 'web-1', 7, %s, %s, %s, 3, {seen})",
                    (mode, reason, action))
        if state == "working":
            cur.execute(f"INSERT INTO {P}reconcile_events (action, status, phase) "
                        "VALUES ('profile_backfill', 'running', 'analyzing volume and ramps')")
    db.commit()

    body = settings_render.render_reconcile_status_panel()

    assert "Throughput" in body and "250 in the last hour" in body
    text = " ".join(body.split())
    assert server_line in text
    assert "<strong>Worker</strong>: no worker has run Lumae’s worker runner yet." in text
    assert f'data-lumae-active="{str(state == "working").lower()}"' in body
    # The page's live region is elsewhere; the panel adds no other violation.
    assert [v for v in _a11y_lint(body) if "aria-live" not in v] == []


def test_the_panel_lists_the_files_lumae_stopped_trying(tick, monkeypatch):
    """W3: too long to measure, loudness failed for good, edges unsupported."""
    mod, db = tick
    monkeypatch.setattr(mod, "_parked_files", None)
    assert "Files Lumae stopped trying" not in settings_render.render_reconcile_status_panel()
    monkeypatch.setattr(mod, "_parked_files", None)
    with db.cursor() as cur:
        for track, duration in (("long-a", 7_000_000), ("long-b", 6_600_000), ("short", 200_000)):
            cur.execute(
                f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, "
                "track_id, title, metadata_fp, media_fp, analysis_eligible, duration_ms, payload, "
                "first_seen_at, last_seen_at) VALUES (%s, 1, %s, %s, 'm', 'r', TRUE, %s, '{}'::jsonb, "
                "now(), now())", (SOURCE, track, track, duration))
        cur.execute(f"INSERT INTO {P}loudness_attempts (catalog_instance_id, track_id, media_revision, "
                    "attempts, next_attempt_at) VALUES (%s, 'short', 'r', 5, 'infinity')", (SOURCE,))
        cur.execute(f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, media_revision, "
                    "job_token, status) VALUES (%s, 'short', 'r', 't', 'unsupported')", (SOURCE,))
    db.commit()

    text = " ".join(settings_render.render_reconcile_status_panel().split())

    assert ("Files Lumae stopped trying: 2 too long to measure (over 109 minutes; never "
            "downloaded); 1 whose loudness failed for good on the current file; 1 without an edge "
            "profile (an unsupported format or layout, or a file that failed to decode twice). "
            "Each is tried again when its file changes.") in text
    # Counted at most every five minutes: the settings panel is polled.
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {P}edge_profile_jobs")
    db.commit()
    assert "1 without an edge profile" in settings_render.render_reconcile_status_panel()
