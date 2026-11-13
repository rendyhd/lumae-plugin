"""Runner retry review fixes against the migrated schema.

* The bookkeeping's preparation request leaves a failed preparation's retry
  ladder alone; an explicit request still re-queues it at once.
* The ``catalog_refresh_hours`` cadence asks once per period per catalogue,
  whether the last refresh published or failed.
* A refresh skipped because the source awaits rebinding drops its request.
* The narrow attempt read carries no ramps and no edge payload.
"""
import importlib

import pytest

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


def _source(refresh_required=True):
    return {
        "server_id": SERVER,
        "catalog_instance_id": SOURCE,
        "rebind_status": "active",
        "catalog": {"generation": 1, "status": "complete", "refresh_required": refresh_required},
    }


@pytest.fixture
def plugin(migrated_db, monkeypatch):
    mod = load_plugin()
    db = migrated_db
    with db.cursor() as cur:
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
    for module in (mod, importlib.import_module("plugins.LumaeAnalysis.reconcile")):
        monkeypatch.setattr(module, "get_db", lambda: db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda *_args, **_kwargs: [_source()])
    return mod, db


def _preparation(db):
    return _rows(db, f"SELECT status, retry_count, next_retry_at > now() "
                     f"FROM {P}preparation_state WHERE catalog_instance_id=%s", (SOURCE,))


def test_bookkeeping_keeps_a_failed_preparations_retry_ladder(plugin):
    mod, db = plugin
    assert mod.enqueue_required_catalog_preparations(db=db) == 1
    _rows(db, f"UPDATE {P}preparation_state SET status='failed', retry_count=2, "
              "next_retry_at=now()+interval '5 minutes'")

    assert mod.enqueue_required_catalog_preparations(db=db) == 0
    assert _preparation(db) == [("failed", 2, True)]
    assert mod.next_preparation_run(db=db) is None  # waits for its retry
    _rows(db, f"UPDATE {P}preparation_state SET next_retry_at=now()-interval '1 second'")
    assert mod.enqueue_required_catalog_preparations(db=db) == 0
    assert mod.next_preparation_run(db=db) == (SERVER, SOURCE, 2)  # the ladder goes on

    # The settings button and /api/catalog/prepare re-queue at once.
    assert mod.claim_preparation(_source(), db=db) is True
    assert _preparation(db) == [("queued", 0, None)]


def test_the_refresh_cadence_asks_once_a_period_whatever_the_outcome(plugin, monkeypatch):
    mod, db = plugin
    monkeypatch.setattr(mod, "catalog_refresh_hours", lambda: 6.0)
    _rows(db, f"UPDATE {P}catalog_state SET completed_at=now()-interval '7 hours'")

    assert mod.want_scheduled_catalog_refreshes(db) == 1
    # The refresh failed: the request is cleared and completed_at stays old.
    _rows(db, f"UPDATE {P}catalog_state SET refresh_wanted_at=NULL")
    assert mod.want_scheduled_catalog_refreshes(db) == 0
    assert _rows(db, f"SELECT refresh_wanted_at FROM {P}catalog_state") == [(None,)]

    _rows(db, f"UPDATE {P}runner_marks SET done_at=now()-interval '7 hours' "
              "WHERE name=%s", (f"catalog_refresh:{SOURCE}",))
    assert mod.want_scheduled_catalog_refreshes(db) == 1


def test_a_skipped_requested_refresh_drops_its_request(plugin, monkeypatch):
    mod, db = plugin
    _rows(db, f"UPDATE {P}catalog_state SET refresh_wanted_at=now()-interval '1 minute'")
    monkeypatch.setattr(mod, "catalog_refresh_task",
                        lambda server_id=None: {"status": "skipped", "reason": "source_rebind_required"})

    result = mod.wanted_catalog_refresh_task(SERVER, SOURCE)

    assert result["status"] == "skipped"
    assert _rows(db, f"SELECT refresh_wanted_at FROM {P}catalog_state") == [(None,)]


def test_the_status_read_carries_no_ramps_or_edge_payload(plugin):
    mod, db = plugin
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, "
            "track_id, title, metadata_fp, media_fp, analysis_eligible, payload, "
            "first_seen_at, last_seen_at) "
            "VALUES (%s, 1, 't1', 't1', 'metadata', 'revision-a', TRUE, '{}'::jsonb, now(), now())",
            (SOURCE,),
        )
    db.commit()
    publication.admit_attempts(db, SOURCE, ["t1"])

    [row] = mod.fetch_profile_status_rows(["t1", "absent"], catalog_instance_id=SOURCE)

    assert set(row) == {"track_id", "status", "last_error", "analyzer_ver", "media_signature"}
    assert row["track_id"] == "t1" and mod.is_pending_profile_status(row["status"])
    assert mod.split_analyze_ids(["t1", "absent"], catalog_instance_id=SOURCE) == (
        ["absent"], [], ["t1"])
