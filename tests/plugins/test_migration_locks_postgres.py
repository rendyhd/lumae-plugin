"""P2-5: re-running migrate takes no exclusive locks; needed DDL waits boundedly.

``ALTER TABLE`` takes its ACCESS EXCLUSIVE lock before it evaluates
``IF NOT EXISTS``, and ``CREATE INDEX IF NOT EXISTS`` takes a SHARE lock (it
blocks writers) before it checks the name. An install hook that re-runs them on
an up-to-date database queues behind any open reader, and every later query on
that table queues behind the install. These tests watch the real migration
from a second connection.
"""

import threading
import time
from types import SimpleNamespace

import pytest

from test_lumae_analysis import load_plugin

P = "plugin_lumae_analysis__"
# Row-level modes: they never conflict with readers or with each other's writers.
WEAK_MODES = {"AccessShareLock", "RowShareLock", "RowExclusiveLock"}


class LockProbe:
    """A connection proxy that records, before every commit, the relation
    locks its backend holds, as the observer connection sees them in
    ``pg_locks``. Heavyweight relation locks are held to transaction end, so
    the snapshot before commit covers every lock the transaction took."""

    def __init__(self, db, observer):
        self._db = db
        self._observer = observer
        self._pid = db.get_backend_pid()
        self.locks = set()

    def snapshot(self):
        with self._observer.cursor() as cur:
            cur.execute(
                """SELECT COALESCE(c.relname, l.relation::text), l.mode
                     FROM pg_locks l
                     LEFT JOIN pg_class c ON c.oid=l.relation
                    WHERE l.pid=%s AND l.locktype='relation'""",
                (self._pid,),
            )
            self.locks.update(cur.fetchall())
        self._observer.rollback()

    def commit(self):
        self.snapshot()
        self._db.commit()

    def __getattr__(self, name):
        return getattr(self._db, name)


def _populate(db, run_plugin_migration):
    """Rows in the key tables, then a migrate that seeds per-source state."""
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type,
                 server_name, is_default, rebind_status)
                VALUES ('catalog-a', 'server-a', 'navidrome', 'A', TRUE, 'active')"""
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_state
                (catalog_instance_id, current_core_server_id, provider_type,
                 published_generation, catalog_epoch, status)
                VALUES ('catalog-a', 'server-a', 'navidrome', 1, 'epoch-a', 'complete')"""
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_tracks
                (catalog_instance_id, published_generation, track_id, title,
                 metadata_fp, media_fp, payload, first_seen_at, last_seen_at)
                SELECT 'catalog-a', 1, 'track-' || n, 'Track ' || n, 'm' || n,
                       'media:' || n, '{{}}'::jsonb, now(), now()
                  FROM generate_series(1, 50) AS n"""
        )
        for target in ("source_profiles", "published_source_profiles"):
            status = ", 'ready'" if target == "source_profiles" else ""
            cur.execute(
                f"""INSERT INTO {P}{target}
                    (catalog_instance_id, track_id, sample_rate, duration_ms,
                     ref_lufs, start_ramp, end_ramp, analyzer_ver,
                     profile_schema_ver, media_signature, analyzed_at
                     {', status' if status else ''})
                    SELECT 'catalog-a', 'track-' || n, 48000, 1000, -14,
                           '\\x01'::bytea, '\\x02'::bytea, 1, 1, 'media:' || n,
                           now(){status}
                      FROM generate_series(1, 50) AS n"""
            )
        cur.execute(
            f"""INSERT INTO {P}collections (principal, id, name)
                VALUES ('user:alice', 'c1', 'One')"""
        )
        cur.execute(
            f"""INSERT INTO {P}collection_items
                (principal, id, collection_id, kind, track_id, position)
                VALUES ('user:alice', 'i1', 'c1', 'track', 'track-1', 0)"""
        )
        cur.execute(
            f"""INSERT INTO {P}collection_changes
                (seq, principal, collection_id, entity_kind, entity_id, operation, payload)
                SELECT n, 'user:alice', 'c1', 'collection', 'c1', 'upsert', '{{}}'::jsonb
                  FROM generate_series(1, 3) AS n"""
        )
        cur.execute(f"UPDATE {P}collection_feed_state SET head_seq=3")
    db.commit()
    run_plugin_migration(db)


def _plugin_tables(db):
    with db.cursor() as cur:
        cur.execute(
            "SELECT relname FROM pg_class "
            "WHERE relnamespace=current_schema()::regnamespace AND relkind='r' "
            "ORDER BY relname"
        )
        names = [row[0] for row in cur.fetchall()]
    db.rollback()
    return names


def _schema(db):
    """Columns, defaults, indexes and constraints, keyed by name (not attnum)."""
    with db.cursor() as cur:
        cur.execute(
            """SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
                      a.attnotnull, pg_get_expr(d.adbin, d.adrelid)
                 FROM pg_attribute a
                 JOIN pg_class c ON c.oid=a.attrelid
                 LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
                WHERE c.relnamespace=current_schema()::regnamespace
                  AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
                ORDER BY 1, 2"""
        )
        columns = cur.fetchall()
        cur.execute(
            """SELECT i.relname, pg_get_indexdef(x.indexrelid)
                 FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
                WHERE i.relnamespace=current_schema()::regnamespace
                ORDER BY 1"""
        )
        indexes = cur.fetchall()
        cur.execute(
            """SELECT c.relname, k.conname, pg_get_constraintdef(k.oid)
                 FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
                WHERE c.relnamespace=current_schema()::regnamespace
                ORDER BY 1, 2"""
        )
        constraints = cur.fetchall()
    db.rollback()
    return columns, indexes, constraints


@pytest.fixture
def migrations_module():
    load_plugin()
    from plugins.LumaeAnalysis import migrations

    return migrations


@pytest.fixture
def fast_retries(migrations_module, monkeypatch):
    """Short lock waits; ``sleeps`` records each backoff between attempts."""
    sleeps = []
    monkeypatch.setattr(migrations_module, "DDL_LOCK_TIMEOUT", "150ms")
    monkeypatch.setattr(migrations_module, "_sleep", sleeps.append)
    return sleeps


@pytest.mark.parametrize("statement", [
    "CREATE INDEX IF NOT EXISTS x_idx ON public.x (y)",
    "CREATE INDEX IF NOT EXISTS x_idx ON public . x (y)",
    "CREATE INDEX x_idx ON x (y)",
])
def test_ensure_index_rejects_what_it_cannot_check(migrations_module, statement):
    # A qualified table would be looked up as "public"; without IF NOT EXISTS
    # there is no name to compare. Neither reaches the database.
    with pytest.raises(ValueError):
        migrations_module.ensure_index(None, statement)


def _hold_access_share(connection, *tables):
    """An open reader: ACCESS SHARE only conflicts with ACCESS EXCLUSIVE."""
    with connection.cursor() as cur:
        for name in tables:
            cur.execute(f"LOCK TABLE {name} IN ACCESS SHARE MODE")


def test_rerun_migrate_takes_no_exclusive_locks(
    migrated_db, second_connection, run_plugin_migration
):
    _populate(migrated_db, run_plugin_migration)
    probe = LockProbe(migrated_db, second_connection)

    run_plugin_migration(probe)

    tables = set(_plugin_tables(migrated_db))
    held = {(name, mode) for name, mode in probe.locks if name in tables}
    # The probe saw the migration's own table locks (it is not vacuous).
    assert (P + "catalog_state", "RowExclusiveLock") in held
    exclusive = sorted(n for n, m in held if m == "AccessExclusiveLock")
    assert not exclusive, f"ACCESS EXCLUSIVE on {len(exclusive)} tables: {exclusive}"
    # Nor any lock that blocks writers (e.g. SHARE from CREATE INDEX), on
    # any relation.
    strong = sorted((n, m) for n, m in probe.locks if m not in WEAK_MODES)
    assert not strong, f"locks stronger than ROW EXCLUSIVE: {strong}"


def test_rerun_migrate_does_not_wait_behind_open_readers(
    migrated_db, second_connection, run_plugin_migration
):
    _populate(migrated_db, run_plugin_migration)
    tables = _plugin_tables(migrated_db)
    assert len(tables) > 60
    # A reader holds every plugin table (e.g. a long /changes page).
    _hold_access_share(second_connection, *tables)
    with migrated_db.cursor() as cur:
        # Fail fast instead of hanging if migrate ever needs these locks.
        cur.execute("SET lock_timeout = '1s'")
    migrated_db.commit()
    started = time.monotonic()

    run_plugin_migration(migrated_db)

    assert time.monotonic() - started < 10
    second_connection.rollback()


def test_rerun_migrate_leaves_schema_identical(migrated_db, run_plugin_migration):
    _populate(migrated_db, run_plugin_migration)
    before = _schema(migrated_db)
    run_plugin_migration(migrated_db)
    assert _schema(migrated_db) == before


# Each undo takes the schema back to an older release's shape for one object
# the helpers own; re-running migrate must still apply the DDL it skips when
# nothing is missing.
UNDO_TO_OLDER_SCHEMA = (
    # ensure_columns (single, batched, NOT NULL DEFAULT, constraint-dependent)
    f"ALTER TABLE {P}catalog_state DROP COLUMN refresh_reason",
    f"ALTER TABLE {P}catalog_state DROP COLUMN last_scan_duration_ms",
    f"ALTER TABLE {P}source_profiles DROP COLUMN retry_count",
    f"ALTER TABLE {P}source_profiles DROP COLUMN attempt_token",
    f"ALTER TABLE {P}profile_bootstrap_snapshot DROP COLUMN edge_ref",
    f"ALTER TABLE {P}profile_bootstrap_sessions DROP COLUMN pages_served",
    f"ALTER TABLE {P}profile_bootstrap_sessions DROP COLUMN client_request_id",
    f"ALTER TABLE {P}provider_identity_transitions DROP COLUMN applied_at",
    f"ALTER TABLE {P}analysis_runs DROP COLUMN next_retry_at",
    f"ALTER TABLE {P}preparation_state DROP COLUMN worker_plugin_version",
    f"ALTER TABLE {P}profile_backfill_state DROP COLUMN refresh_wake_pending",
    f"ALTER TABLE {P}edge_profiles DROP COLUMN orphaned_at",
    f"ALTER TABLE {P}collection_mutations DROP COLUMN fingerprint_version",
    # K8 floor_seq (P3-4a): re-added, backfilled to the head, then NOT NULL
    f"ALTER TABLE {P}collection_feed_state DROP COLUMN floor_seq",
    # P2-1 status summary columns (status_model.migrate_status_summary)
    f"ALTER TABLE {P}analysis_state DROP COLUMN summary_updated_at",
    f"ALTER TABLE {P}analysis_state DROP COLUMN summary_generation",
    f"ALTER TABLE {P}analysis_state DROP COLUMN evidence_complete_link_count",
    f"ALTER TABLE {P}profile_stream_state DROP COLUMN retention_limit",
    # ensure_no_default / ensure_default / ensure_not_null
    f"ALTER TABLE {P}catalog_changes ALTER COLUMN writer_generation SET DEFAULT 2",
    f"ALTER TABLE {P}profile_changes ALTER COLUMN writer_generation SET DEFAULT 2",
    f"ALTER TABLE {P}collection_changes ALTER COLUMN seq "
    f"SET DEFAULT nextval('{P}collection_changes_seq_seq')",
    f"ALTER TABLE {P}catalog_state ALTER COLUMN catalog_schema_version SET DEFAULT 1",
    f"ALTER TABLE {P}catalog_state ALTER COLUMN fingerprint_schema_version SET DEFAULT 1",
    f"ALTER TABLE {P}profile_bootstrap_sessions "
    "ALTER COLUMN transfer_contract_version SET DEFAULT 0",
    f"ALTER TABLE {P}profile_bootstrap_sessions ALTER COLUMN source_scope DROP NOT NULL",
    # ensure_index (plain, unique partial, unprefixed name, DESC key)
    f"DROP INDEX {P}idx_catalog_source_core",
    f"DROP INDEX {P}profile_changes_track_idx",
    f"DROP INDEX {P}credits_versions_subject",
    f"DROP INDEX {P}source_profiles_status_idx",
    f"DROP INDEX {P}edge_profile_jobs_orphan_idx",
    "DROP INDEX lumae_shelf_changes_idx",
    "DROP INDEX lumae_collection_album_key_unique_idx",
    "DROP INDEX lumae_metadata_due",
    "DROP INDEX lumae_discovery_changes",
)


def test_migrate_still_applies_needed_ddl(migrated_db, run_plugin_migration):
    _populate(migrated_db, run_plugin_migration)
    expected = _schema(migrated_db)
    with migrated_db.cursor() as cur:
        for statement in UNDO_TO_OLDER_SCHEMA:
            cur.execute(statement)
    migrated_db.commit()
    assert _schema(migrated_db) != expected

    run_plugin_migration(migrated_db)

    assert _schema(migrated_db) == expected


def test_account_era_principal_is_relaxed_once(
    migrated_db, second_connection, run_plugin_migration
):
    table = P + "profile_bootstrap_sessions"
    with migrated_db.cursor() as cur:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN principal TEXT NOT NULL DEFAULT 'x'")
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(
            "SELECT attnotnull FROM pg_attribute WHERE attrelid=%s::regclass "
            "AND attname='principal'",
            (table,),
        )
        assert cur.fetchone() == (False,)
    migrated_db.rollback()
    probe = LockProbe(migrated_db, second_connection)
    run_plugin_migration(probe)
    assert (table, "AccessExclusiveLock") not in probe.locks


def test_needed_alter_retries_until_a_reader_releases(
    migrated_db, second_connection, run_plugin_migration, fast_retries,
    migrations_module, monkeypatch,
):
    _populate(migrated_db, run_plugin_migration)
    with migrated_db.cursor() as cur:
        cur.execute(f"ALTER TABLE {P}catalog_state DROP COLUMN last_scan_duration_ms")
    migrated_db.commit()
    _hold_access_share(second_connection, P + "catalog_state")

    def release(seconds):
        fast_retries.append(seconds)
        second_connection.rollback()  # the reader finishes during the backoff

    monkeypatch.setattr(migrations_module, "_sleep", release)

    run_plugin_migration(migrated_db)

    assert fast_retries == [migrations_module.DDL_RETRY_BACKOFF_S[0]]
    with migrated_db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema=current_schema() AND table_name=%s "
            "AND column_name='last_scan_duration_ms'",
            (P + "catalog_state",),
        )
        assert cur.fetchone() == (1,)
    migrated_db.rollback()


def test_needed_alter_fails_cleanly_after_bounded_attempts(
    migrated_db, second_connection, run_plugin_migration, fast_retries,
    migrations_module, monkeypatch,
):
    psycopg2 = pytest.importorskip("psycopg2")
    _populate(migrated_db, run_plugin_migration)
    with migrated_db.cursor() as cur:
        cur.execute(f"ALTER TABLE {P}catalog_state DROP COLUMN last_scan_duration_ms")
    migrated_db.commit()
    before = _schema(migrated_db)
    _hold_access_share(second_connection, P + "catalog_state")
    logged = []
    monkeypatch.setattr(
        migrations_module, "logger",
        SimpleNamespace(warning=lambda message, *args: logged.append(message % args)),
    )
    started = time.monotonic()

    with pytest.raises(psycopg2.errors.LockNotAvailable):
        run_plugin_migration(migrated_db)

    assert time.monotonic() - started < 10
    attempts = migrations_module.DDL_LOCK_ATTEMPTS
    assert attempts == 3
    assert fast_retries == list(migrations_module.DDL_RETRY_BACKOFF_S[: attempts - 1])
    assert len(logged) == attempts
    assert "gave up waiting for a lock after 3 attempts" in logged[-1]
    assert "last_scan_duration_ms" in logged[-1]
    migrated_db.rollback()
    second_connection.rollback()
    assert _schema(migrated_db) == before


def test_failed_attempts_restore_the_callers_lock_timeout(
    migrated_db, second_connection, fast_retries, migrations_module
):
    psycopg2 = pytest.importorskip("psycopg2")
    table = P + "catalog_state"
    _hold_access_share(second_connection, table)
    with migrated_db.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '3s'")
        with pytest.raises(psycopg2.errors.LockNotAvailable):
            migrations_module.ensure_columns(cur, table, "p25_probe TEXT")
        # Rolled back to before the statement; the transaction is usable.
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone() == ("3s",)
        second_connection.rollback()
        assert migrations_module.ensure_columns(cur, table, "p25_probe TEXT") == ["p25_probe"]
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone() == ("3s",)
        assert migrations_module.ensure_columns(cur, table, "p25_probe TEXT") == []
    migrated_db.rollback()
    assert len(fast_retries) == migrations_module.DDL_LOCK_ATTEMPTS - 1


def test_collection_feed_fence_lock_retries_until_released(
    migrated_db, second_connection, fast_retries, migrations_module, monkeypatch
):
    manager = load_plugin().collection_manager
    changes = manager.collection_changes_table()
    with migrated_db.cursor() as cur:
        # The 1.2.5 shape: a BIGSERIAL default the fence must drop.
        cur.execute(
            f"ALTER TABLE {changes} ALTER COLUMN seq "
            f"SET DEFAULT nextval('{changes}_seq_seq')"
        )
        cur.execute("SET lock_timeout = '4s'")
        cur.execute("SET statement_timeout = '8s'")
    migrated_db.commit()
    _hold_access_share(second_connection, changes)
    released = threading.Event()

    def release(seconds):
        fast_retries.append(seconds)
        second_connection.rollback()
        released.set()

    monkeypatch.setattr(migrations_module, "_sleep", release)

    manager.migrate_collections(migrated_db)

    assert released.is_set()
    with migrated_db.cursor() as cur:
        cur.execute(
            "SELECT column_default FROM information_schema.columns "
            "WHERE table_schema=current_schema() AND table_name=%s "
            "AND column_name='seq'",
            (changes,),
        )
        assert cur.fetchone() == (None,)
        cur.execute("SHOW lock_timeout")
        assert cur.fetchone() == ("4s",)
        cur.execute("SHOW statement_timeout")
        assert cur.fetchone() == ("8s",)
    migrated_db.commit()


def test_collection_feed_fence_fails_cleanly_after_bounded_attempts(
    migrated_db, second_connection, fast_retries, migrations_module
):
    psycopg2 = pytest.importorskip("psycopg2")
    manager = load_plugin().collection_manager
    changes = manager.collection_changes_table()
    with migrated_db.cursor() as cur:
        cur.execute(
            f"ALTER TABLE {changes} ALTER COLUMN seq "
            f"SET DEFAULT nextval('{changes}_seq_seq')"
        )
    migrated_db.commit()
    _hold_access_share(second_connection, changes)

    with pytest.raises(psycopg2.errors.LockNotAvailable):
        manager.migrate_collections(migrated_db)

    assert len(fast_retries) == migrations_module.DDL_LOCK_ATTEMPTS - 1
    migrated_db.rollback()
    second_connection.rollback()
