"""Shared PostgreSQL fixtures for the LumaeAnalysis plugin tests.

Rule: every new PostgreSQL test uses ``migrated_db`` instead of hand-built
tables. Hand-written ``CREATE TABLE`` statements drift from production; this
fixture runs the real ``plugins.LumaeAnalysis.migrate(db)`` so a test sees the
same tables, columns, constraints and indexes an installed plugin has.

Fixtures
--------
``migrated_db``
    A psycopg2 connection to a database of the test's own, whose
    ``search_path`` points at a schema the real plugin migration has
    populated. The migration runs once per session into a template database;
    each test gets a copy (``CREATE DATABASE ... TEMPLATE``, about a third of
    the cost of migrating), dropped at teardown. While the test runs,
    ``LUMAE_POSTGRES_TEST_DSN`` (and so ``pg_helpers.dsn()``/``connect()``)
    points at that database. Tests are skipped when ``LUMAE_POSTGRES_TEST_DSN``
    is unset (or psycopg2 is missing), like the other PostgreSQL tests.

``@pytest.mark.fresh_schema``
    The test's database starts without the plugin schema and ``migrated_db``
    runs the migration inside the test, for a test that patches something the
    migration reads before ``migrated_db`` is set up.

``second_connection``
    Another psycopg2 connection to the same schema, for concurrency tests
    (row locks, ``SKIP LOCKED``, serialization, visibility of uncommitted
    work). It is closed before ``migrated_db`` drops the schema.

``run_plugin_migration``
    A callable ``run_plugin_migration(db)`` that runs ``mod.migrate(db)`` again
    with the same stubs, e.g. to check idempotency or to migrate after seeding
    legacy rows.

``_one_analysis_thread`` (autouse)
    ``background_workers=auto`` resolves to one analysis thread, so a tick
    runs every file on the calling thread as before WP1b. Tests of the
    parallel passes open ``parallel_analysis.tick_scope(k)`` themselves.

What is stubbed
---------------
Only work that reaches outside the schema or the host: provider source
discovery (``ensure_catalog_sources``), the post-install preparation request
(``enqueue_required_catalog_preparations``, which resolves sources through
the host). The stubs apply only while the migration runs, so the test body
calls the production functions. The removal of Lumae's schedules
(``retire_lumae_schedules``) runs for real
against a per-schema stand-in for the host ``cron`` table (``name``,
``task_type`` UNIQUE, ``cron_expr``, ``enabled``).

Caveats
-------
* The ``plugin.api`` host stub (installed by ``test_lumae_analysis``) has
  ``get_db()`` return ``None``. A test whose code path calls ``get_db()``
  must monkeypatch it, e.g.
  ``monkeypatch.setattr(mod, "get_db", lambda: migrated_db)`` (patch the name
  in each plugin module that imported it), or return ``second_connection``.
* Each test gets its own database, so the plugin's advisory locks (which
  are database-wide) and extensions (``pg_trgm`` and ``unaccent``, created
  once in ``public``) never reach another test. The role in
  ``LUMAE_POSTGRES_TEST_DSN`` needs CREATEDB.
* Teardown drops the database ``WITH (FORCE)``, terminating any connection
  the test left open, so teardown neither hangs nor leaks.
* Schemas a test creates itself in the base database are dropped with
  ``pg_helpers.drop_schema``, which terminates stray lock holders.

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

import pg_helpers
from pg_helpers import connect, dsn

# The plugin schema in every test database; a copy of the template's.
SCHEMA = "lumae_migrated_test"
EXTENSIONS = ("pg_trgm", "unaccent")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "fresh_schema: migrated_db migrates inside the test instead of copying the template",
    )


def _load_plugin():
    # test_lumae_analysis installs the ``plugin.api`` host stub the plugin
    # imports; import it lazily so its fixtures are not re-registered here.
    from test_lumae_analysis import load_plugin

    return load_plugin()


@pytest.fixture(autouse=True)
def _one_analysis_thread():
    """A tick analyses one file at a time unless a test asks for more
    (performance plan WP1b): ``background_workers=auto`` would size K from
    the machine running the suite. Tests of K threads set it themselves.

    Its own MonkeyPatch, not the ``monkeypatch`` fixture: an autouse fixture
    is set up first and torn down last, so sharing it would keep a test's
    patches (a refused ``psycopg2.connect``) through ``migrated_db``'s
    teardown."""
    _load_plugin()
    from plugins.LumaeAnalysis import parallel_analysis

    patch = pytest.MonkeyPatch()
    patch.setattr(parallel_analysis, "available_cpus", lambda: parallel_analysis.CPU_RESERVE + 1)
    yield
    patch.undo()


def _run_plugin_migration(db):
    mod = _load_plugin()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(mod, "ensure_catalog_sources", lambda *_args: None)
        patch.setattr(
            mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0
        )
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


def _create_extensions(db):
    """``pg_trgm`` and ``unaccent`` in ``public``, where dropping a test's
    schema can't take them along (and with them an opclass another backend
    is using: "could not find tuple for opclass")."""
    with db.cursor() as cur:
        for name in EXTENSIONS:
            cur.execute(f"CREATE EXTENSION IF NOT EXISTS {name} SCHEMA public")
    db.commit()


def _prepare(db):
    with db.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {SCHEMA}")
        cur.execute(f"SET search_path TO {SCHEMA}, public")
        cur.execute(
            "CREATE TABLE cron (name TEXT, task_type TEXT UNIQUE, "
            "cron_expr TEXT, enabled BOOLEAN)"
        )
    db.commit()
    _run_plugin_migration(db)


@pytest.fixture(scope="session")
def _template_databases():
    """Two templates for the session: one migrated, one with the extensions
    only (for ``fresh_schema``). Built on first use, dropped at the end."""
    dsn()
    psycopg2 = pytest.importorskip("psycopg2")
    run = uuid.uuid4().hex[:12]
    names = {"migrated": f"lumae_tpl_{run}", "empty": f"lumae_tpl_{run}_empty"}
    admin = pg_helpers.admin_connection()
    base = psycopg2.connect(pg_helpers.base_dsn())
    try:
        _create_extensions(base)
    finally:
        base.close()
    try:
        for migrate, name in ((True, names["migrated"]), (False, names["empty"])):
            with admin.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{name}"')
            db = psycopg2.connect(pg_helpers.database_dsn(name))
            try:
                _create_extensions(db)
                if migrate:
                    _prepare(db)
            finally:
                db.close()
        yield admin, names
    finally:
        for name in names.values():
            _quietly(lambda name=name: pg_helpers.drop_database(admin, name))
        admin.close()


@pytest.fixture
def migrated_db(request, _template_databases):
    admin, templates = _template_databases
    fresh = request.node.get_closest_marker("fresh_schema") is not None
    name = f"lumae_t_{uuid.uuid4().hex}"
    pg_helpers.create_database(admin, name, templates["empty" if fresh else "migrated"])
    env = pytest.MonkeyPatch()
    db = None
    try:
        env.setenv("LUMAE_POSTGRES_TEST_DSN", pg_helpers.database_dsn(name))
        db = connect(SCHEMA)
        if fresh:
            _prepare(db)
        yield db
    finally:
        if db is not None and not db.closed:
            _quietly(db.rollback)
            _quietly(db.close)
        env.undo()
        pg_helpers.drop_database(admin, name)


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
