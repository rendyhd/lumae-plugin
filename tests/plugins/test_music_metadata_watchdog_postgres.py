"""MusicBrainz lookups run in the server runner, not an every-minute cron.

1.2.2-1.5.0 installed ``plugin.lumae_analysis.music_metadata`` at ``* * * * *``.
It fired 1,440 times a day, nearly always on an empty queue, and AudioMuse
recorded every firing in its ten-row "Recent tasks" history. Since then:

* the migration deletes that cron row, with Lumae's other schedules (runner
  plan R8), and leaves other plugins' rows alone;
* an accepted lookup wakes the runners when its transaction commits, and
  counts as pending until it is served;
* a deferred lookup is a retry: an idle runner sleeps until it is due.
"""
import importlib
import select
import uuid

from test_lumae_analysis import load_plugin

LEGACY = "plugin.lumae_analysis.music_metadata"


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


def _notified(listener, timeout=2.0):
    """The runner wakes ``listener`` has received (LISTEN lumae_runner)."""
    if select.select([listener], [], [], timeout) != ([], [], []):
        listener.poll()
    payloads = [notify.payload for notify in listener.notifies]
    del listener.notifies[:]
    return payloads


def _pending_lookups(db):
    status = importlib.import_module("plugins.LumaeAnalysis.reconcile").read_reconcile_status(db)
    db.commit()
    return status["pending"].get("MusicBrainz lookups", 0)


def test_migration_deletes_the_metadata_schedule_and_leaves_other_plugins_rows(
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

    assert {row[0] for row in _rows(db, "SELECT task_type FROM cron")} == {
        "plugin.other.music_metadata"}


def test_a_lookup_wakes_the_runners_on_commit_and_is_pending_until_served(
    migrated_db, second_connection, monkeypatch
):
    mod = load_plugin()
    meta = importlib.import_module("plugins.LumaeAnalysis.music_metadata")
    db = migrated_db
    _host_task_status(db)
    # Nothing due: an idle runner sleeps until a wake or its sweep, never
    # 0 s (GREATEST skips a NULL retry time).
    assert mod._runner_next_due_seconds(db) is None

    # Not psycopg2.connect(db.dsn): connection.dsn masks the password, so it
    # fails wherever PostgreSQL asks for one (CI).
    listener = second_connection
    listener.autocommit = True
    try:
        with listener.cursor() as cur:
            cur.execute("LISTEN lumae_runner")
        entity = {"id": str(uuid.uuid4()), "kind": "artist", "title": "Artist"}
        assert meta.submit(db, "user:alice", [entity])[1] == 202
        assert _notified(listener, timeout=0.2) == []  # not before the commit (R2)
        db.commit()
        assert _notified(listener) == ["metadata_requested"]
    finally:
        listener.close()
    assert _pending_lookups(db) == 1

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
    assert _pending_lookups(db) == 0
    assert mod._runner_next_due_seconds(db) is None


def test_an_idle_runner_sleeps_until_a_deferred_lookup_is_due(migrated_db):
    mod = load_plugin()
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
        return mod._runner_next_due_seconds(db)

    # The daily allowance defers for an hour: the runner sleeps that long
    # (or until a wake), not a minute at a time.
    assert 3600 < defer(61) <= 3660
    assert 1140 < defer(20) <= 1200
    assert 0 < defer(0.5) <= 30
    # Due now: a turn serves it, so there is no retry to sleep until.
    assert defer(0) is None
