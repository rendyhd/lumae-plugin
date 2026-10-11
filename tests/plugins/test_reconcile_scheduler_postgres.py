"""The catalogue watchdog's tick, through ``catalog_reconcile_task``.

Plan: docs/plan/LUMAE_SCHEDULER_PLAN_2026-10-11.md (1.6.2: S1-S6).

* S4 (AUD-15): a waveform backfill that only waits for retry-armed tracks
  keeps its own retry time. The tick used to reset it, so the row was due at
  once, the watchdog stayed on minute cadence after the backfill had
  finished, and the edge pass ordered after it never ran.
"""
import importlib

import pytest

from test_lumae_analysis import load_plugin
from plugins.LumaeAnalysis import profile_publication as publication  # noqa: E402


P = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
SERVER = "server-a"
RECONCILE = "plugin.lumae_analysis.catalog_reconcile"


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
    monkeypatch.setattr(mod, "refresh_status_snapshot", lambda *args, **kwargs: 0)
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

    assert result["action"] == "profile_backfill"
    assert result["result"]["status"] == "waiting_retry"
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

    assert result["result"]["status"] == "complete"
    assert _backfill_row(db) == ("complete", None, 0)
    assert _rows(db, f"SELECT mode FROM {P}reconcile_control")[0][0] == "idle"
