"""P2-8: re-running the FederatedAlbums migrate takes no strong locks.

The plugin migrates on every install and web start. ``ALTER TABLE ... ADD
COLUMN IF NOT EXISTS`` takes ACCESS EXCLUSIVE and ``CREATE INDEX IF NOT
EXISTS`` takes SHARE before either checks whether there is anything to do, so
each start queued behind open readers or writers. The plugin's own copy of the
P2-5 helpers (``plugins/FederatedAlbums/migrations.py``) checks the catalog
first and bounds the wait when DDL is needed. These tests watch the real
migration from a second connection, as the P2-5 tests do.
"""

import importlib.util
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg2.extras import Json

from pg_helpers import connect
from test_federated_lifecycle import connection, federation, fingerprint, remote_album  # noqa: F401
from test_lumae_analysis import lumae_postgres_db  # noqa: F401
from test_migration_locks_postgres import WEAK_MODES, LockProbe, _schema

# The federation fixture maps table(name) to friend_<name>.
TABLES = (
    "friend_meta",
    "friend_albums",
    "friend_track_order",
    "friend_share_tokens",
    "friend_connections",
    "friend_remote_albums",
)
# Modes that block writers (SHARE, from CREATE INDEX) or readers (ACCESS
# EXCLUSIVE, from ALTER TABLE), and the ones in between.
STRONG_MODES = {
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
}
# Each undo takes one object the helpers own back to an older shape: a column
# set from before catalog_store/sync_jobs (some columns present, some
# missing), or a missing index.
UNDO_TO_OLDER_SCHEMA = (
    "ALTER TABLE friend_connections DROP COLUMN sync_status, DROP COLUMN sync_attempts",
    "ALTER TABLE friend_albums DROP COLUMN buckets, DROP COLUMN search_document",
    "ALTER TABLE friend_remote_albums DROP COLUMN search_document",
    "DROP INDEX friend_remote_albums_buckets_idx",
    "DROP INDEX friend_remote_albums_instance_idx",
    "DROP INDEX friend_connections_owner_idx",
)


@pytest.fixture
def observer(federation):
    """A second connection to the federation schema, closed before teardown."""
    db = federation.test_db
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    db.commit()
    other = connect(schema)
    try:
        yield other
    finally:
        if not other.closed:
            other.rollback()
            other.close()


@pytest.fixture
def fast_retries(federation, monkeypatch):
    """Short lock waits; ``sleeps`` records each backoff between attempts."""
    sleeps = []
    monkeypatch.setattr(federation.migrations, "DDL_LOCK_TIMEOUT", "150ms")
    monkeypatch.setattr(federation.migrations, "_sleep", sleeps.append)
    return sleeps


def _populate(mod):
    remote_album(mod, connection(mod))
    with mod.test_db.cursor() as cur:
        cur.execute(
            "INSERT INTO friend_albums(album_key,album,artist,track_count,"
            "source_signature,fingerprint) VALUES('owned','Owned album','Artist',1,"
            "'sig',%s)",
            (Json(fingerprint(mod)),),
        )
    mod.test_db.commit()


def _hold(connection, mode, *tables):
    with connection.cursor() as cur:
        for name in tables:
            cur.execute(f"LOCK TABLE {name} IN {mode} MODE")


def _column_count(db, table, column):
    with db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema=current_schema() AND table_name=%s AND column_name=%s",
            (table, column),
        )
        count = cur.fetchone()[0]
    db.rollback()
    return count


@pytest.mark.parametrize("statement", [
    "CREATE INDEX IF NOT EXISTS x_idx ON public.x (y)",
    "CREATE INDEX x_idx ON x (y)",
])
def test_ensure_index_rejects_what_it_cannot_check(statement):
    # The plugin's own copy (it is packaged without LumaeAnalysis).
    path = Path(__file__).resolve().parents[2] / "plugins" / "FederatedAlbums" / "migrations.py"
    spec = importlib.util.spec_from_file_location("_federated_migrations_copy", path)
    migrations = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migrations)
    with pytest.raises(ValueError):
        migrations.ensure_index(None, statement)


def test_rerun_migrate_takes_no_strong_locks(federation, observer):
    mod = federation
    _populate(mod)
    probe = LockProbe(mod.test_db, observer)

    mod.migrate(probe)

    held = {(name, mode) for name, mode in probe.locks if name.startswith("friend_")}
    # The probe saw the migration's own table locks (it is not vacuous).
    assert ("friend_meta", "RowExclusiveLock") in held
    strong = sorted((n, m) for n, m in held if m in STRONG_MODES)
    assert not strong, f"strong locks on FederatedAlbums relations: {strong}"
    # Nor on any other relation (e.g. cron).
    stronger = sorted((n, m) for n, m in probe.locks if m not in WEAK_MODES)
    assert not stronger, f"locks stronger than ROW EXCLUSIVE: {stronger}"


def test_rerun_migrate_does_not_wait_behind_open_writers(federation, observer):
    mod = federation
    _populate(mod)
    # An open writer on every table: ROW EXCLUSIVE conflicts with the SHARE
    # lock of CREATE INDEX and with the ACCESS EXCLUSIVE lock of ALTER TABLE.
    _hold(observer, "ROW EXCLUSIVE", *TABLES)
    with mod.test_db.cursor() as cur:
        # Fail fast instead of hanging if migrate ever needs these locks.
        cur.execute("SET lock_timeout = '1s'")
    mod.test_db.commit()
    started = time.monotonic()

    mod.migrate(mod.test_db)

    assert time.monotonic() - started < 10
    observer.rollback()


def test_rerun_migrate_leaves_schema_identical(federation):
    mod = federation
    _populate(mod)
    before = _schema(mod.test_db)
    mod.migrate(mod.test_db)
    assert _schema(mod.test_db) == before


def test_partially_migrated_schema_is_brought_up_to_date(federation, observer):
    mod = federation
    _populate(mod)
    db = mod.test_db
    expected = _schema(db)
    with db.cursor() as cur:
        for statement in UNDO_TO_OLDER_SCHEMA:
            cur.execute(statement)
    db.commit()
    assert _schema(db) != expected
    probe = LockProbe(db, observer)

    mod.migrate(probe)

    assert _schema(db) == expected
    # The needed DDL still ran, and the probe sees the locks it takes.
    assert ("friend_connections", "AccessExclusiveLock") in probe.locks
    assert ("friend_connections", "ShareLock") in probe.locks
    with db.cursor() as cur:
        cur.execute("SELECT sync_status, sync_attempts FROM friend_connections")
        assert cur.fetchall() == [("idle", 0)]
        cur.execute(
            "SELECT count(*) FROM friend_albums "
            "WHERE search_document @@ to_tsquery('simple', 'owned')"
        )
        assert cur.fetchone() == (1,)
    db.rollback()


def test_needed_alter_retries_until_a_reader_releases(
    federation, observer, fast_retries, monkeypatch
):
    mod = federation
    _populate(mod)
    with mod.test_db.cursor() as cur:
        cur.execute("ALTER TABLE friend_connections DROP COLUMN sync_attempts")
        # Fail instead of hanging if the ALTER ever waits without a bound.
        cur.execute("SET lock_timeout = '5s'")
    mod.test_db.commit()
    _hold(observer, "ACCESS SHARE", "friend_connections")

    def release(seconds):
        fast_retries.append(seconds)
        observer.rollback()  # the reader finishes during the backoff

    monkeypatch.setattr(mod.migrations, "_sleep", release)

    mod.migrate(mod.test_db)

    assert fast_retries == [mod.migrations.DDL_RETRY_BACKOFF_S[0]]
    assert _column_count(mod.test_db, "friend_connections", "sync_attempts") == 1


def test_needed_alter_fails_cleanly_after_bounded_attempts(
    federation, observer, fast_retries, monkeypatch
):
    psycopg2 = pytest.importorskip("psycopg2")
    mod = federation
    _populate(mod)
    db = mod.test_db
    with db.cursor() as cur:
        cur.execute("ALTER TABLE friend_connections DROP COLUMN sync_attempts")
        cur.execute("SET lock_timeout = '3s'")
    db.commit()
    before = _schema(db)
    _hold(observer, "ACCESS SHARE", "friend_connections")
    logged = []
    monkeypatch.setattr(
        mod.migrations, "logger",
        SimpleNamespace(warning=lambda message, *args: logged.append(message % args)),
    )
    started = time.monotonic()

    with pytest.raises(psycopg2.errors.LockNotAvailable):
        mod.migrate(db)

    assert time.monotonic() - started < 10
    attempts = mod.migrations.DDL_LOCK_ATTEMPTS
    assert attempts == 3
    assert fast_retries == list(mod.migrations.DDL_RETRY_BACKOFF_S[: attempts - 1])
    assert len(logged) == attempts
    assert "gave up waiting for a lock after 3 attempts" in logged[-1]
    assert "sync_attempts" in logged[-1]
    # Rolled back to before the statement, with the caller's lock_timeout.
    with db.cursor() as cur:
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone() == ("3s",)
    db.rollback()
    observer.rollback()
    assert _schema(db) == before
