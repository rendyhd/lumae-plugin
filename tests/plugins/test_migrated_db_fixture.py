"""The shared ``migrated_db`` fixture yields the production schema."""

import threading
import uuid

import pytest

import pg_helpers
from pg_helpers import backend_alive, connect, drop_schema, schema_exists

PREFIX = "plugin_lumae_analysis__"
# At least one table per sub-migration run by ``migrate(db)``.
PRODUCTION_TABLES = (
    "profiles",
    "profile_backfill_state",
    "analysis_runs",
    "preparation_state",
    "edge_profiles",
    "edge_profile_jobs",
    "analysis_items",
    "analysis_state",
    "track_analysis_links",
    "relationship_state",
    "relationship_results",
    "credits_jobs",
    "credits_results",
    "shelf_scopes",
    "shelf_records",
    "discovery_scopes",
    "discovery_records",
    "metadata_jobs",
    "catalog_generation_pins",
    "profile_bootstrap_sessions",
    "profile_stream_state",
    "stream_bootstrap_sessions",
    "provider_identity_manifests",
    "reconcile_control",
    "reconcile_events",
    "source_profiles",
    "published_source_profiles",
    "profile_changes",
    "profile_migrations",
    "catalog_sources",
    "catalog_tracks",
    "collections",
    "collection_items",
    "collection_changes",
    "collection_mutations",
    "collection_feed_state",
)


def _schema_snapshot(db):
    with db.cursor() as cur:
        cur.execute(
            """SELECT table_name, column_name, data_type, is_nullable
                 FROM information_schema.columns
                WHERE table_schema=current_schema()
                ORDER BY table_name, column_name"""
        )
        columns = cur.fetchall()
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname=current_schema() ORDER BY indexname"
        )
        indexes = cur.fetchall()
        cur.execute(
            """SELECT conname, pg_get_constraintdef(c.oid)
                 FROM pg_constraint c
                 JOIN pg_namespace n ON n.oid=c.connamespace
                WHERE n.nspname=current_schema()
                ORDER BY conname"""
        )
        constraints = cur.fetchall()
        cur.execute(f"SELECT name FROM {PREFIX}profile_migrations ORDER BY name")
        markers = cur.fetchall()
    db.commit()
    return columns, indexes, constraints, markers


def test_migrated_db_has_production_tables(migrated_db):
    with migrated_db.cursor() as cur:
        cur.execute(
            "SELECT current_schema(), table_name FROM information_schema.tables "
            "WHERE table_schema=current_schema()"
        )
        rows = cur.fetchall()
    schema = rows[0][0]
    assert schema.startswith("lumae_migrated_")
    tables = {name for _schema, name in rows}
    missing = [name for name in PRODUCTION_TABLES if PREFIX + name not in tables]
    assert missing == []
    with migrated_db.cursor() as cur:
        cur.execute(
            "SELECT relname FROM pg_class "
            "WHERE relnamespace=current_schema()::regnamespace AND relkind='S'"
        )
        sequences = {row[0] for row in cur.fetchall()}
    assert PREFIX + "shelf_sequence" in sequences


def test_plugin_migration_is_idempotent(migrated_db, run_plugin_migration):
    before = _schema_snapshot(migrated_db)
    run_plugin_migration(migrated_db)
    run_plugin_migration(migrated_db)
    assert _schema_snapshot(migrated_db) == before


def test_second_connection_shares_the_schema(migrated_db, second_connection):
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
        cur.execute(
            f"""INSERT INTO {PREFIX}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type,
                 server_name, is_default, rebind_status)
                VALUES ('catalog-a', 'server-a', 'navidrome', 'A', TRUE, 'active')"""
        )
    with second_connection.cursor() as cur:
        cur.execute("SELECT current_schema()")
        assert cur.fetchone()[0] == schema
        cur.execute(f"SELECT count(*) FROM {PREFIX}catalog_sources")
        assert cur.fetchone()[0] == 0  # uncommitted on the first connection
    second_connection.commit()
    migrated_db.commit()
    with second_connection.cursor() as cur:
        cur.execute(f"SELECT catalog_instance_id FROM {PREFIX}catalog_sources")
        assert cur.fetchall() == [("catalog-a",)]
    second_connection.commit()


def _leak(schema, statement):
    """Leave a connection idle in transaction, holding locks in ``schema``."""
    stray = connect(schema)
    with stray.cursor() as cur:
        cur.execute(statement)
    return stray


LEAKS = {
    "lock_existing_table": "LOCK TABLE {table} IN ACCESS SHARE MODE",
    "uncommitted_create_table": "CREATE TABLE zz_new (id INTEGER)",
}


@pytest.mark.parametrize("path", ["targeted", "blocking_pids_fallback"])
@pytest.mark.parametrize("leak", sorted(LEAKS))
def test_drop_schema_does_not_wait_on_an_idle_transaction(leak, path, monkeypatch):
    if path == "blocking_pids_fallback":
        # Find no lock holders up front: only pg_blocking_pids() can clear them.
        monkeypatch.setattr(
            pg_helpers, "_SCHEMA_LOCK_HOLDERS", "SELECT NULL::int AS pid WHERE false"
        )
    schema = f"lumae_migrated_{uuid.uuid4().hex}"
    owner = connect("public")
    with owner.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"CREATE TABLE {schema}.held (id INTEGER)")
    owner.commit()
    owner.close()
    stray = _leak(schema, LEAKS[leak].format(table="held"))
    stray_pid = stray.get_backend_pid()
    failures = []

    def drop():
        try:
            drop_schema(schema)
        except Exception as exc:  # pragma: no cover - reported below
            failures.append(exc)

    worker = threading.Thread(target=drop, daemon=True)
    worker.start()
    worker.join(timeout=90)
    try:
        assert not worker.is_alive(), "schema teardown hung on a stray lock"
        assert failures == []
        assert not schema_exists(schema)
        assert not backend_alive(stray_pid)
    finally:
        stray.close()
        if schema_exists(schema):  # pragma: no cover - cleanup after failure
            drop_schema(schema)


# Connections deliberately leaked past a test so the real ``migrated_db``
# teardown has to deal with them; the follow-up test checks the outcome.
_LEAKED = []


def test_migrated_db_teardown_survives_leaked_locking_connections(migrated_db):
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    migrated_db.commit()
    for statement in LEAKS.values():
        stray = _leak(schema, statement.format(table=f"{PREFIX}catalog_sources"))
        _LEAKED.append((schema, stray.get_backend_pid(), stray))


def test_leaked_connection_schemas_were_dropped():
    if not _LEAKED:
        pytest.skip("runs after test_migrated_db_teardown_survives_leaked_locking_connections")
    try:
        for schema, pid, _stray in _LEAKED:
            assert not schema_exists(schema)
            assert not backend_alive(pid)
    finally:
        for _schema, _pid, stray in _LEAKED:
            stray.close()
        _LEAKED.clear()
