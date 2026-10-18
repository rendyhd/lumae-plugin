"""Performance plan WP5 (scheduler plan S12, AUD-18): the settings page shows,
for each queue, its throughput per hour from the journal, what is left and
an ETA; for the analysis queues the ETA at the current number of files
analysed at once and at a higher one; and the last yield with the refused
AudioMuse schedule it waits for and how long ago that was refused.
"""
import time

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


def test_remaining_and_eta(tick):
    mod, db = tick
    _summary(db, ready=1000, total=1500)
    _event(db, "profile_backfill", 10, ready=250)

    [waveforms] = [q for q in reconcile.queue_throughput(db) if q["action"] == "profile_backfill"]

    assert waveforms["remaining"] == 500
    assert waveforms["eta_hours"] == pytest.approx(2.0)


def test_no_throughput_means_no_eta(tick):
    mod, db = tick
    _summary(db, ready=1000, total=1500)
    [waveforms] = [q for q in reconcile.queue_throughput(db) if q["action"] == "profile_backfill"]
    assert waveforms["remaining"] == 500 and waveforms["eta_hours"] is None


@pytest.mark.parametrize("hours,text", [
    (0.2, "about 12 minutes"), (2.0, "about 2.0 hours"), (36, "about 1.5 days"), (None, ""),
])
def test_eta_wording(hours, text):
    assert settings_render._eta_text(hours) == text


def test_the_eta_at_more_files_at_once_follows_the_measured_scaling():
    # 8 cores: K=2 1.86x, K=4 3.38x, K=6 4.32x over K=1 (parallel_bench).
    assert par.next_workers(1, cpus=8) == 2 and par.next_workers(2, cpus=8) == 4
    assert par.next_workers(6, cpus=8) is None  # nothing measured above 6
    assert par.next_workers(2, cpus=3) is None  # not past the cores
    assert par.speedup(4) == pytest.approx(3.38) and par.speedup(3) == pytest.approx((1.86 + 3.38) / 2)


def test_the_last_yield_names_the_refused_schedule_and_its_age(tick):
    mod, db = tick
    with db.cursor() as cur:
        cur.execute("CREATE TABLE cron_retry (task_type TEXT PRIMARY KEY, first_blocked_at DOUBLE PRECISION)")
        cur.execute("INSERT INTO cron_retry VALUES ('analysis', %s)", (time.time() - 14 * 60,))
        cur.execute(f"UPDATE {P}reconcile_control SET mode='waiting', reason='yield:cron_retry:analysis', "
                    "cron_expr='17 * * * *'")
    db.commit()
    status = reconcile.yield_status(db)
    assert status["waiting_for"] == "cron_retry:analysis"
    assert status["refused"] == [("analysis", pytest.approx(14 * 60, abs=5))]


def test_without_cron_retry_the_yield_line_has_no_refusals(tick):
    mod, db = tick
    assert reconcile.yield_status(db)["refused"] is None


def test_the_panel_renders_throughput_eta_and_the_yield(tick, monkeypatch):
    mod, db = tick
    _summary(db, ready=1000, total=1500)
    _event(db, "profile_backfill", 10, ready=250)
    monkeypatch.setattr(mod, "background_workers", lambda: 2)
    monkeypatch.setattr(par, "available_cpus", lambda: 8)

    body = settings_render.render_reconcile_status_panel()

    assert "Throughput" in body
    assert "Volume and ramps" in body and "250 in the last hour" in body
    assert "500 left, about 2.0 hours at this rate" in body
    assert "Files analyzed at once: 2 (auto)." in body
    assert "At 4 at once: about 1.1 hours" in body  # 2.0 h x 1.86 / 3.38


@pytest.mark.parametrize("state", ["working", "yielded", "idle", "paused"])
def test_the_panel_renders_in_every_state_and_stays_accessible(tick, monkeypatch, state):
    from test_lumae_analysis import _a11y_lint

    mod, db = tick
    _summary(db, ready=1000, total=1500)
    _event(db, "profile_backfill", 10, ready=250)
    mode, reason = {"working": ("active", "work_ready"), "yielded": ("waiting", "yield:cron_retry:analysis"),
                    "idle": ("idle", "no_work"), "paused": ("paused", "maintenance")}[state]
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}reconcile_control SET mode=%s, reason=%s, cron_expr='17 * * * *'",
                    (mode, reason))
        if state == "working":
            cur.execute(f"INSERT INTO {P}reconcile_events (action, status, phase) "
                        "VALUES ('profile_backfill', 'running', 'analyzing volume and ramps')")
        if state == "yielded":
            cur.execute("CREATE TABLE cron_retry (task_type TEXT PRIMARY KEY, first_blocked_at DOUBLE PRECISION)")
            # Mid-way through "2 minutes": the age is PostgreSQL's now() minus
            # this machine's clock, and 120 s sat on the edge of "1 minute".
            cur.execute("INSERT INTO cron_retry VALUES ('analysis', %s)", (time.time() - 150,))
    db.commit()

    body = settings_render.render_reconcile_status_panel()

    assert "Throughput" in body and "250 in the last hour" in body
    assert ("analysis (refused 2 minutes ago)" in body) == (state == "yielded")
    # The page's live region is elsewhere; the panel adds no other violation.
    assert [v for v in _a11y_lint(body) if "aria-live" not in v] == []
