"""MusicBrainz lookups run on the catalogue watchdog, not an every-minute cron.

1.2.2-1.5.0 installed ``plugin.lumae_analysis.music_metadata`` at ``* * * * *``.
It fired 1,440 times a day, nearly always on an empty queue, and AudioMuse
recorded every firing in its ten-row "Recent tasks" history. Since then:

* the migration deletes that cron row (and only that row);
* an accepted lookup arms the watchdog's minute cadence in its own
  transaction, and the cadence drops back to the hourly sweep once the
  queue is empty;
* a deferred lookup is a retry, and a retry an hour away waits on the hourly
  sweep instead of ticking every minute until it is due.
"""
import importlib
import uuid

from test_lumae_analysis import load_plugin

LEGACY = "plugin.lumae_analysis.music_metadata"
RECONCILE = "plugin.lumae_analysis.catalog_reconcile"


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _host_task_status(db):
    # _work_summary reads the host's task_status for analysis parents.
    with db.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS task_status (task_id TEXT, parent_task_id TEXT, "
            "task_type TEXT, status TEXT)"
        )
    db.commit()


def _schedule(db):
    reconcile = importlib.import_module("plugins.LumaeAnalysis.reconcile")
    result = reconcile.reconcile_schedule_from_state(db)
    cron = _rows(db, "SELECT cron_expr FROM cron WHERE task_type=%s", (RECONCILE,))
    assert cron == [(result["cron_expr"],)]
    return result["mode"], result["cron_expr"]


def test_migration_deletes_only_the_every_minute_metadata_schedule(
    migrated_db, run_plugin_migration
):
    db = migrated_db
    assert _rows(db, "SELECT 1 FROM cron WHERE task_type=%s", (LEGACY,)) == []
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO cron (name, task_type, cron_expr, enabled) VALUES "
            "('Lumae album metadata', %s, '* * * * *', TRUE), "
            "('Someone else', 'plugin.other.music_metadata', '* * * * *', TRUE)",
            (LEGACY,),
        )
    db.commit()

    run_plugin_migration(db)

    task_types = {row[0] for row in _rows(db, "SELECT task_type FROM cron")}
    assert LEGACY not in task_types
    assert {"plugin.other.music_metadata", RECONCILE} <= task_types


def test_lookups_arm_the_watchdog_and_let_it_idle_once_served(migrated_db, monkeypatch):
    load_plugin()
    meta = importlib.import_module("plugins.LumaeAnalysis.music_metadata")
    db = migrated_db
    _host_task_status(db)
    assert _schedule(db) == ("idle", "11 * * * *")

    entity = {"id": str(uuid.uuid4()), "kind": "artist", "title": "Artist"}
    assert meta.submit(db, "user:alice", [entity])[1] == 202
    db.commit()
    assert _rows(db, "SELECT cron_expr FROM cron WHERE task_type=%s", (RECONCILE,)) == [
        ("* * * * *",)
    ]
    assert _schedule(db) == ("active", "* * * * *")
    status = importlib.import_module("plugins.LumaeAnalysis.reconcile").read_reconcile_status(db)
    db.commit()
    assert status["pending"]["MusicBrainz lookups"] == 1

    class FakeClient:
        def __init__(self, db, **kwargs):
            pass

        def get(self, kind, entity_id=None, **kwargs):
            return {"artists": []}

    run_one = meta.run_one
    monkeypatch.setattr(
        meta, "run_one",
        lambda db: run_one(db=db, client_factory=FakeClient, critical=lambda db: False),
    )
    assert meta.reconcile(db)["status"] == "unresolved"
    assert _schedule(db) == ("idle", "11 * * * *")


def test_a_deferred_lookup_backs_off_by_how_far_away_its_retry_is(migrated_db):
    load_plugin()
    meta = importlib.import_module("plugins.LumaeAnalysis.music_metadata")
    db = migrated_db
    _host_task_status(db)
    entity = {"id": str(uuid.uuid4()), "kind": "artist", "title": "Artist"}
    meta.submit(db, "user:alice", [entity])
    db.commit()

    def defer(minutes):
        # As run_one leaves a job MusicBrainz deferred on its first attempt.
        with db.cursor() as cur:
            cur.execute(
                "UPDATE plugin_lumae_analysis__metadata_jobs SET status='deferred', "
                "attempts=1, not_before=now()+(%s*interval '1 minute')",
                (minutes,),
            )
        db.commit()
        return _schedule(db)

    # The daily allowance defers for an hour: the hourly sweep still ticks
    # before then, and the cadence tightens as the retry nears.
    assert defer(61) == ("backoff", "11 * * * *")
    assert defer(20) == ("backoff", "*/15 * * * *")
    assert defer(6) == ("backoff", "*/5 * * * *")
    assert defer(0.5) == ("backoff", "* * * * *")
    assert defer(0) == ("active", "* * * * *")
