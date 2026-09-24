"""Shared PostgreSQL fixtures for the LumaeAnalysis plugin tests.

Rule: every new PostgreSQL test uses ``migrated_db`` instead of hand-built
tables. Hand-written ``CREATE TABLE`` statements drift from production; this
fixture runs the real ``plugins.LumaeAnalysis.migrate(db)`` so a test sees the
same tables, columns, constraints and indexes an installed plugin has.

Fixtures
--------
``migrated_db``
    A psycopg2 connection whose ``search_path`` points at a fresh, per-test
    schema that the real plugin migration has already populated. The schema
    is dropped at teardown. Tests are skipped when ``LUMAE_POSTGRES_TEST_DSN``
    is unset (or psycopg2 is missing), like the other PostgreSQL tests.

``second_connection``
    Another psycopg2 connection to the same schema, for concurrency tests
    (row locks, ``SKIP LOCKED``, serialization, visibility of uncommitted
    work). It is closed before ``migrated_db`` drops the schema.

``run_plugin_migration``
    A callable ``run_plugin_migration(db)`` that runs ``mod.migrate(db)`` again
    with the same stubs, e.g. to check idempotency or to migrate after seeding
    legacy rows.

What is stubbed
---------------
Only work that reaches outside the schema or the host: provider source
discovery (``ensure_catalog_sources``), the post-install preparation request
(``enqueue_required_catalog_preparations``, which resolves sources through
the host) and the adaptive reconcile schedule update
(``_safe_reconcile_schedule``, which reads host settings). The stubs apply only while the migration runs, so the
test body calls the production functions. The schedule installers
(``ensure_*_schedule``, ``music_metadata.ensure_schedule``,
``disable_legacy_backfill_schedule``) run for real against a per-schema
stand-in for the host ``cron`` table (``name``, ``task_type`` UNIQUE,
``cron_expr``, ``enabled``); ``ensure_catalog_reconcile_schedule`` also owns
schema work (``migrate_reconcile``), so stubbing it would hide tables.

Caveats
-------
* The ``plugin.api`` host stub (installed by ``test_lumae_analysis``) has
  ``get_db()`` return ``None``. A test whose code path calls ``get_db()``
  must monkeypatch it, e.g.
  ``monkeypatch.setattr(mod, "get_db", lambda: migrated_db)`` (patch the name
  in each plugin module that imported it), or return ``second_connection``.
* Each test gets its own schema, but PostgreSQL advisory locks are
  database-wide, not per schema. Parallel test runs against one database can
  block each other on the plugin's advisory locks; give each run its own
  database.
* Teardown (``pg_helpers.drop_schema``) drops the schema from a fresh
  connection with a lock timeout. If a leftover connection still holds locks
  on the schema or its relations (including an uncommitted ``CREATE TABLE``
  in it), that backend is terminated and the drop retried once, terminating
  anything still blocking it, so teardown neither hangs nor leaks.

Because source discovery is stubbed, ``catalog_sources`` starts empty. Insert
the sources a test needs::

    def test_example(migrated_db, second_connection):
        with migrated_db.cursor() as cur:
            cur.execute(
                "INSERT INTO plugin_lumae_analysis__catalog_sources "
                "(catalog_instance_id, current_core_server_id, provider_type, "
                " server_name, is_default, rebind_status) "
                "VALUES ('catalog-a', 'server-a', 'navidrome', 'A', TRUE, 'active')"
            )
        migrated_db.commit()
        with second_connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM plugin_lumae_analysis__catalog_sources")
            assert cur.fetchone()[0] == 1
"""

import uuid

import pytest

from pg_helpers import connect, drop_schema, dsn


def _load_plugin():
    # test_lumae_analysis installs the ``plugin.api`` host stub the plugin
    # imports; import it lazily so its fixtures are not re-registered here.
    from test_lumae_analysis import load_plugin

    return load_plugin()


def _run_plugin_migration(db):
    mod = _load_plugin()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(mod, "ensure_catalog_sources", lambda *_args: None)
        patch.setattr(
            mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0
        )
        patch.setattr(mod, "_safe_reconcile_schedule", lambda *_args, **_kw: None)
        mod.migrate(db)
    db.commit()


def _quietly(action):
    try:
        action()
    except Exception:
        pass


@pytest.fixture
def run_plugin_migration():
    return _run_plugin_migration


@pytest.fixture
def migrated_db():
    dsn()
    pytest.importorskip("psycopg2")
    schema = f"lumae_migrated_{uuid.uuid4().hex}"
    db = connect("public")
    try:
        with db.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(f"SET search_path TO {schema}, public")
            cur.execute(
                "CREATE TABLE cron (name TEXT, task_type TEXT UNIQUE, "
                "cron_expr TEXT, enabled BOOLEAN)"
            )
        db.commit()
        _run_plugin_migration(db)
        yield db
    finally:
        if not db.closed:
            _quietly(db.rollback)
            _quietly(db.close)
        drop_schema(schema)


@pytest.fixture
def second_connection(migrated_db):
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    migrated_db.commit()
    other = connect(schema)
    try:
        yield other
    finally:
        if not other.closed:
            _quietly(other.rollback)
            _quietly(other.close)
