"""Review fixes of the operator diagnostics that need PostgreSQL: the credits
re-check in the scheduler's work summary, the advisory-lock probe's database scope,
the event journal's trim and the analysis summary's newest-wins guard."""

import pytest

import pg_helpers
from test_idle_cost_postgres import (  # noqa: F401  (fixtures and helpers)
    P, _albums, _expire, _q, _run, _unresolved, _work,
)
from test_credits import credits_db, payload, uid  # noqa: F401
from test_lumae_analysis import lumae_postgres_db  # noqa: F401
from plugins.LumaeAnalysis import credits_store as store
from plugins.LumaeAnalysis import reconcile
from plugins.LumaeAnalysis import status_model


def test_an_app_requested_recheck_is_ready_work_not_idle(migrated_db):
    db = migrated_db
    _albums(db)
    store.request_album(db, "c", "a")
    _run(db, "a", _unresolved(db, "a"))
    _expire(db, "a")
    store.request_album(db, "c", "a")  # a background re-check: idle
    assert _work(db)["ready"] == 0
    store.request_album(db, "c", "a", priority=10)  # the phone asks: ready, as ready_sql says
    assert _work(db)["ready"] == 1


def _probe(db):
    with db.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS task_status (task_id TEXT, parent_task_id TEXT, "
                    "task_type TEXT, status TEXT)")
        result = reconcile.id_rewrite_in_progress(cur)
    db.commit()
    return result


def test_the_migration_lock_probe_ignores_another_databases_lock(migrated_db, second_connection):
    key = reconcile.PROVIDER_MIGRATION_LOCK_KEY
    admin = pg_helpers.admin_connection()
    name = "lumae_probe_other_db"
    other = None
    try:
        pg_helpers.create_database(admin, name, "template0")
        psycopg2 = pytest.importorskip("psycopg2")
        other = psycopg2.connect(pg_helpers.database_dsn(name))
        other.autocommit = True
        with other.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (key,))
        assert _probe(migrated_db) is None  # another database's lock is not ours

        with second_connection.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (key,))  # this database's is
        assert _probe(migrated_db) == "provider_migration"
    finally:
        if other is not None:
            other.close()
        pg_helpers.drop_database(admin, name)
        admin.close()


def _event_ids(db):
    with db.cursor() as cur:
        cur.execute(f"SELECT event_id, status FROM {reconcile.events_table()} ORDER BY event_id")
        rows = cur.fetchall()
    db.commit()
    return dict(rows)


def test_trimming_the_journal_keeps_a_running_event_and_the_one_just_finished(
        migrated_db, monkeypatch):
    db = migrated_db
    monkeypatch.setattr(reconcile, "EVENT_RETENTION_MAX", 3)
    monkeypatch.setattr(reconcile, "EVENT_RETENTION_MIN", 2)
    running = reconcile.begin_event(db, "profile_backfill", "s", "c")
    finishing = reconcile.begin_event(db, "edge_backfill", "s", "c")
    for _ in range(6):  # newer events of the same source outrank both
        reconcile.record_event(db, "relationships", "s", "c", "success", phase="done",
                               duration_ms=1)
    assert _event_ids(db)[running] == "running"
    reconcile.finish_event(db, finishing, "success", phase="done")
    ids = _event_ids(db)
    assert ids[running] == "running"
    assert ids[finishing] == "success"  # not ranked out before its own units are counted
    # The journal still trims finished events beyond the cap.
    assert sum(1 for status in ids.values() if status != "running") <= 4


def test_an_older_generations_summary_never_replaces_a_newer_one(migrated_db):
    db = migrated_db
    column = status_model._LINK_COUNT_COLUMNS[0]
    with db.cursor() as cur:
        cur.execute(f"""INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id,
            provider_type, server_name) VALUES ('c', 'c', 'navidrome', 'Test')""")
        cur.execute(f"""INSERT INTO {P}analysis_state (catalog_instance_id, analysis_epoch,
            projection_generation, summary_generation, {column}) VALUES ('c', %s, 5, 5, 777)""",
                    (uid(701),))
    db.commit()

    def stored():
        rows = _q(db, f"SELECT summary_generation, {column} FROM {P}analysis_state "
                      "WHERE catalog_instance_id='c'")
        return rows[0]

    with db.cursor() as cur:
        status_model.persist_analysis_summary(cur, "c", 4)  # a stale refresh
    db.commit()
    assert stored() == (5, 777)

    _q(db, f"UPDATE {P}analysis_state SET summary_generation=4 WHERE catalog_instance_id='c'")
    with db.cursor() as cur:
        status_model.persist_analysis_summary(cur, "c", 5)  # the current one writes
    db.commit()
    assert stored()[0] == 5
