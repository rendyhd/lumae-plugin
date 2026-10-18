"""PostgreSQL connection, database and schema-teardown helpers for the
plugin tests.

Used by ``conftest.py`` (``migrated_db``/``second_connection``) and by the
tests that check teardown itself.

``dsn()`` reads ``LUMAE_POSTGRES_TEST_DSN`` when called. While a
``migrated_db`` test runs, the fixture points that variable at the test's own
database, so ``dsn()``, ``connect()`` and tests that read the variable
directly all reach it. ``base_dsn()`` is the database the run was given.
"""

import os
import threading

import pytest


DROP_LOCK_TIMEOUT = "5s"
# Upper bound for the second attempt, while blockers are being terminated.
DROP_RETRY_LOCK_TIMEOUT = "30s"

# Backends in this database that hold a lock on a relation in the schema, or
# on the schema itself (e.g. an uncommitted CREATE TABLE in the schema holds
# a lock on the namespace object while its new pg_class row is invisible).
_SCHEMA_LOCK_HOLDERS = """
SELECT DISTINCT l.pid
  FROM pg_locks l
  LEFT JOIN pg_class c ON c.oid=l.relation
 WHERE l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
   AND l.pid<>pg_backend_pid()
   AND ((l.locktype='relation' AND c.relnamespace=%(schema)s::regnamespace)
     OR (l.locktype='object'
         AND l.classid='pg_namespace'::regclass
         AND l.objid=%(schema)s::regnamespace))
"""


_BASE_DSN = os.environ.get("LUMAE_POSTGRES_TEST_DSN")


def dsn():
    value = os.environ.get("LUMAE_POSTGRES_TEST_DSN")
    if not value:
        pytest.skip("set LUMAE_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
    return value


def base_dsn():
    """The DSN the run was started with, even inside a ``migrated_db`` test."""
    if not _BASE_DSN:
        pytest.skip("set LUMAE_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
    return _BASE_DSN


def database_dsn(name):
    """``base_dsn()`` with its database replaced by ``name``."""
    psycopg2 = pytest.importorskip("psycopg2")
    return psycopg2.extensions.make_dsn(base_dsn(), dbname=name)


def admin_connection():
    """An autocommit connection to the base database, for CREATE/DROP DATABASE."""
    psycopg2 = pytest.importorskip("psycopg2")
    admin = psycopg2.connect(base_dsn())
    admin.autocommit = True
    return admin


def create_database(admin, name, template):
    """Copy ``template`` (which must have no open connections) to ``name``.

    WAL_LOG (PostgreSQL 15's default) is spelled out: for a database this
    small it took ~50 ms against ~350 ms for FILE_COPY, whose checkpoint
    dominates.
    """
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{name}" TEMPLATE "{template}" STRATEGY WAL_LOG')


def drop_database(admin, name):
    """Drop ``name``, terminating any connection a test left open in it."""
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def connect(schema):
    """Open a psycopg2 connection whose search_path is ``schema, public``."""
    psycopg2 = pytest.importorskip("psycopg2")
    db = psycopg2.connect(dsn())
    with db.cursor() as cur:
        cur.execute(f"SET search_path TO {schema}, public")
    db.commit()
    return db


def _terminate_blockers_while(worker, blocked_pid):
    """Terminate backends blocking ``blocked_pid`` until ``worker`` ends."""
    psycopg2 = pytest.importorskip("psycopg2")
    watcher = psycopg2.connect(dsn())
    try:
        watcher.autocommit = True
        with watcher.cursor() as cur:
            while worker.is_alive():
                cur.execute(
                    "SELECT pg_terminate_backend(b) "
                    "FROM unnest(pg_blocking_pids(%s)) AS b",
                    (blocked_pid,),
                )
                worker.join(timeout=0.2)
    finally:
        watcher.close()


def drop_schema(schema):
    """Drop ``schema`` CASCADE from a fresh connection without hanging.

    The first attempt waits at most ``DROP_LOCK_TIMEOUT``. If it times out,
    backends holding locks on the schema or its relations are terminated and
    the drop is retried once; during the retry, any backend still blocking
    it (``pg_blocking_pids``) is terminated too.
    """
    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2 import errors

    admin = psycopg2.connect(dsn())
    try:
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(f"SET lock_timeout='{DROP_LOCK_TIMEOUT}'")
            try:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                return
            except errors.LockNotAvailable:
                pass
            cur.execute(
                f"SELECT pg_terminate_backend(pid) FROM ({_SCHEMA_LOCK_HOLDERS}) h",
                {"schema": schema},
            )
            cur.execute(f"SET lock_timeout='{DROP_RETRY_LOCK_TIMEOUT}'")
            failure = []

            def retry():
                try:
                    cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                except Exception as exc:
                    failure.append(exc)

            worker = threading.Thread(target=retry, daemon=True)
            worker.start()
            _terminate_blockers_while(worker, admin.get_backend_pid())
            if failure:
                raise failure[0]
    finally:
        admin.close()


def schema_exists(schema):
    db = connect("public")
    try:
        with db.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_namespace WHERE nspname=%s", (schema,))
            return cur.fetchone()[0] == 1
    finally:
        db.close()


def backend_alive(pid):
    db = connect("public")
    try:
        with db.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_stat_activity WHERE pid=%s", (pid,))
            return cur.fetchone()[0] == 1
    finally:
        db.close()
