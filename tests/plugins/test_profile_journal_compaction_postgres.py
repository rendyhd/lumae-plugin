"""P1-2 (AUD-04): profile journal range compaction, library-scaled retention
and the v2 bootstrap floor hold, on the real migrated schema."""

import hashlib
import os
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import plugin_api_module
from plugins.LumaeAnalysis import catalog, catalog_enrichment, profile_bootstrap


SOURCE = "catalog-a"
STATE = "plugin_lumae_analysis__profile_stream_state"
CHANGES = "plugin_lumae_analysis__profile_changes"
SESSIONS = "plugin_lumae_analysis__profile_bootstrap_sessions"
MINIMUM = catalog_enrichment.PROFILE_CHANGE_RETENTION_EVENTS


def _source(db, track_count=0):
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO plugin_lumae_analysis__catalog_sources "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " server_name, is_default, rebind_status) "
            "VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')",
            (SOURCE,),
        )
        cur.execute(
            "INSERT INTO plugin_lumae_analysis__catalog_state "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, 'server-a', 'navidrome', 1, 'epoch-a', 'complete', "
            "        jsonb_build_object('track', %s::int))",
            (SOURCE, track_count),
        )
        epoch, _head, _floor = catalog_enrichment._profile_stream_state(
            cur, SOURCE, for_update=True
        )
    db.commit()
    return epoch


def _seed_events(db, epoch, first, last):
    """Bulk-append events first..last as a long publication pass would."""
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, operation) "
            "SELECT %s, %s, n, 'track-' || n, 'delete' FROM generate_series(%s, %s) AS n",
            (SOURCE, epoch, first, last),
        )
        cur.execute(
            f"UPDATE {STATE} SET head_seq=%s WHERE catalog_instance_id=%s",
            (last, SOURCE),
        )
    db.commit()


def _publish(db, track_id="published"):
    with db.cursor() as cur:
        seq = catalog_enrichment.record_profile_change(
            cur, SOURCE, track_id, "ready", {"track_id": track_id}
        )
    db.commit()
    return seq


def _frontier(db):
    with db.cursor() as cur:
        cur.execute(
            f"SELECT head_seq, floor_seq, retention_limit FROM {STATE} "
            "WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
        head, floor, retention = cur.fetchone()
        cur.execute(
            f"SELECT min(seq), max(seq), count(*) FROM {CHANGES} "
            "WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
        low, high, count = cur.fetchone()
    db.commit()
    return head, floor, retention, low, high, count


def _open_session(db, epoch, snapshot_seq, *, head_seq=None, expires="1 hour"):
    token_hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {SESSIONS}
                (session_id, token_hash, signing_secret, source_scope,
                 catalog_instance_id, core_server_id, catalog_epoch, profile_epoch,
                 schema_version, page_size, snapshot_seq, head_seq, snapshot_count,
                 expires_at)
                VALUES (%s, %s, 'secret', %s, %s, 'server-a', 'epoch-a', %s,
                        1, 50, %s, %s, 0, now() + %s::interval)""",
            (str(uuid.uuid4()), token_hash, SOURCE, SOURCE, epoch, snapshot_seq,
             head_seq, expires),
        )
    db.commit()


def test_retention_is_two_libraries_with_a_50k_minimum():
    limit = catalog_enrichment.profile_change_retention_limit
    assert MINIMUM == 50_000
    assert limit(0) == limit(None) == 50_000
    assert limit(20_000) == 50_000
    assert limit(30_000) == 60_000
    assert limit(94_000) == 188_000


def test_retention_limit_column_is_additive_and_idempotent(migrated_db, run_plugin_migration):
    _source(migrated_db)
    run_plugin_migration(migrated_db)
    assert _frontier(migrated_db)[2] == MINIMUM


def test_cursor_from_before_a_94k_pass_stays_readable(migrated_db):
    """A 94k library keeps 188k events, so a pass of 94k+ events does not
    force the client that synced just before it to bootstrap again."""
    epoch = _source(migrated_db, track_count=94_000)
    catalog_enrichment.compact_enrichment_storage(migrated_db, SOURCE)
    migrated_db.commit()
    assert _frontier(migrated_db)[2] == 188_000

    before = _publish(migrated_db, "before-pass")
    client_cursor = catalog.opaque_cursor(SOURCE, epoch, before)
    _seed_events(migrated_db, epoch, before + 1, before + 94_000)
    for index in range(3):
        _publish(migrated_db, f"pass-{index}")

    head, floor, _retention, low, _high, count = _frontier(migrated_db)
    assert head == before + 94_003
    assert floor == 0 and low == 1 and count == head
    page = catalog_enrichment.read_profile_changes(migrated_db, client_cursor, SOURCE, 1000)
    migrated_db.commit()
    assert page["changes"][0]["seq"] == before + 1
    assert page["has_more"] is True


def test_publication_uses_the_persisted_limit_without_counting(migrated_db):
    """Retention comes from profile_stream_state.retention_limit (no count(*)
    per publication) and is enforced once the head passes it."""
    epoch = _source(migrated_db, track_count=30_000)
    catalog_enrichment.compact_enrichment_storage(migrated_db, SOURCE)
    migrated_db.commit()
    assert _frontier(migrated_db)[2] == 60_000
    _seed_events(migrated_db, epoch, 1, 60_010)

    statements = []

    class Spy:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            statements.append(sql)
            return self._cur.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    with migrated_db.cursor() as cur:
        catalog_enrichment.record_profile_change(Spy(cur), SOURCE, "t", "ready", {"track_id": "t"})
    migrated_db.commit()
    assert not [sql for sql in statements if "count(" in sql.lower()]
    head, floor, _retention, low, high, count = _frontier(migrated_db)
    assert (head, floor, low, high, count) == (60_011, 11, 12, 60_011, 60_000)


def test_publication_leaves_other_epochs_to_maintenance(migrated_db):
    """Per publication only the retained epoch's expired range is deleted;
    rows of another epoch go only in compact_enrichment_storage."""
    epoch = _source(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, operation) "
            "SELECT %s, 'retired-epoch', n, 'old-' || n, 'delete' FROM generate_series(1, 5) AS n",
            (SOURCE,),
        )
    migrated_db.commit()
    _seed_events(migrated_db, epoch, 1, MINIMUM + 5)
    _publish(migrated_db)

    def retired():
        with migrated_db.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {CHANGES} WHERE catalog_instance_id=%s "
                "AND epoch='retired-epoch'",
                (SOURCE,),
            )
            value = cur.fetchone()[0]
        migrated_db.commit()
        return value

    assert _frontier(migrated_db)[1] == 6
    assert retired() == 5
    catalog_enrichment.compact_enrichment_storage(migrated_db, SOURCE)
    migrated_db.commit()
    assert retired() == 0
    assert _frontier(migrated_db)[1] == 6


def test_publication_delete_is_an_index_range_scan(migrated_db):
    epoch = _source(migrated_db)
    _seed_events(migrated_db, epoch, 1, MINIMUM + 1_000)
    with migrated_db.cursor() as cur:
        cur.execute(f"ANALYZE {CHANGES}")
    migrated_db.commit()

    captured = []

    class Capture:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            if sql.lstrip().startswith("DELETE FROM") and CHANGES in sql:
                captured.append((sql, params))
            return self._cur.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    with migrated_db.cursor() as cur:
        catalog_enrichment.record_profile_change(
            Capture(cur), SOURCE, "t", "ready", {"track_id": "t"}
        )
    migrated_db.rollback()
    assert len(captured) == 1
    sql, params = captured[0]
    assert " OR " not in sql.upper()
    with migrated_db.cursor() as cur:
        cur.execute("EXPLAIN " + sql, params)
        plan = "\n".join(row[0] for row in cur.fetchall())
    migrated_db.rollback()
    assert "Seq Scan" not in plan, plan
    assert "profile_changes_pkey" in plan, plan


def test_open_v2_session_holds_the_floor_until_catchup_captures_head(
    migrated_db, monkeypatch
):
    """End to end: a real v2 session created before a long pass can still
    capture its catch-up, and releases the hold once it has."""
    epoch = _source(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    migrated_db.commit()
    monkeypatch.setattr(
        plugin_api_module.config, "DATABASE_URL",
        psycopg2.extensions.make_dsn(
            os.environ["LUMAE_POSTGRES_TEST_DSN"],
            options=f"-c search_path={schema},public"),
        raising=False,
    )
    # P1-5 dependency: MAX_CATCHUP_EVENTS is still 50k here, so a held session
    # more than 50k events behind would get 413 at catch-up. P1-5 raises it to
    # at least 4 x retention_limit (the hold cap); until then the test lifts it.
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_EVENTS", 1_000_000)
    _seed_events(migrated_db, epoch, 1, 10)
    body = {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": profile_bootstrap.TRANSFER_CONTRACT,
            "catalog_instance_id": SOURCE}
    created = profile_bootstrap.create_session({**body, "page_size": 50})
    assert created["snapshot_seq"] == 10

    _seed_events(migrated_db, epoch, 11, MINIMUM + 5_000)
    _publish(migrated_db)
    head, floor, _retention, low, _high, _count = _frontier(migrated_db)
    assert head == MINIMUM + 5_001
    assert floor == 10 and low == 11

    token = {**body, "session_token": created["session_token"]}
    first = profile_bootstrap.catchup_page(token)
    assert first["changes"][0]["seq"] == 11
    assert first["has_more"] is True

    # The head is captured: the hold is released and the next publication
    # compacts back to the retention limit.
    _publish(migrated_db)
    head, floor, _retention, low, _high, count = _frontier(migrated_db)
    assert floor == head - MINIMUM and count == MINIMUM
    assert profile_bootstrap.catchup_page(token) == first


def test_floor_hold_ignores_expired_and_captured_sessions(migrated_db):
    epoch = _source(migrated_db)
    _open_session(migrated_db, epoch, 3, expires="-1 minute")
    _open_session(migrated_db, epoch, 4, head_seq=9)
    _open_session(migrated_db, "other-epoch", 5)
    _seed_events(migrated_db, epoch, 1, MINIMUM + 20)
    _publish(migrated_db)
    assert _frontier(migrated_db)[1] == 21

    _open_session(migrated_db, epoch, 30)
    _seed_events(migrated_db, epoch, MINIMUM + 22, MINIMUM + 100)
    _publish(migrated_db)
    head, floor, _retention, low, _high, _count = _frontier(migrated_db)
    assert head == MINIMUM + 101
    assert floor == 30 and low == 31


def test_floor_hold_is_capped_at_four_retention_limits(migrated_db):
    epoch = _source(migrated_db)
    _open_session(migrated_db, epoch, 7)
    _seed_events(migrated_db, epoch, 1, 4 * MINIMUM + 1_000)
    _publish(migrated_db)
    head, floor, _retention, low, _high, count = _frontier(migrated_db)
    assert head == 4 * MINIMUM + 1_001
    assert floor == head - 4 * MINIMUM == 1_001
    assert low == 1_002 and count == 4 * MINIMUM

    # Maintenance applies the same hold and cap.
    catalog_enrichment.compact_enrichment_storage(migrated_db, SOURCE)
    migrated_db.commit()
    assert _frontier(migrated_db)[1] == 1_001


def test_catalogue_publication_refreshes_the_retention_limit(migrated_db, monkeypatch):
    """The limit follows the library as each catalogue generation publishes,
    not only when maintenance runs at plugin start."""
    from test_lumae_analysis import RefreshBridge

    # Scale the per-track multiplier so a three-track catalogue maps to a
    # library-sized limit above the 50k minimum.
    monkeypatch.setattr(catalog, "CHANGE_EVENT_SNAPSHOT_MULTIPLIER", 20_000)

    def tracks(count):
        return {"tracks": [{"id": f"track-{index}", "title": f"Song {index}",
                            "duration": 100 + index} for index in range(count)]}

    def limit():
        with migrated_db.cursor() as cur:
            cur.execute(f"SELECT catalog_instance_id, retention_limit FROM {STATE}")
            rows = cur.fetchall()
        migrated_db.commit()
        return rows

    first = catalog.refresh_catalog("server-a", db=migrated_db, bridge=RefreshBridge(tracks(3)))
    source = first["catalog_instance_id"]
    assert limit() == [(source, 60_000)]

    catalog.refresh_catalog("server-a", db=migrated_db, bridge=RefreshBridge(tracks(4)))
    assert limit() == [(source, 80_000)]

    # A shrinking library lowers it, never below the 50k minimum.
    catalog.refresh_catalog("server-a", db=migrated_db, bridge=RefreshBridge(tracks(1)))
    assert limit() == [(source, 50_000)]


def test_publication_compaction_is_bounded_and_maintenance_catches_up(migrated_db):
    """Releasing a hold with more than K expired events does not delete them
    all under the publication locks: one publication advances the floor by at
    most K, and maintenance finishes the job."""
    step = catalog_enrichment.PROFILE_COMPACTION_MAX_ADVANCE
    assert step == 5_000
    epoch = _source(migrated_db)
    _open_session(migrated_db, epoch, 7)
    _seed_events(migrated_db, epoch, 1, MINIMUM + 3 * step)
    _publish(migrated_db)
    assert _frontier(migrated_db)[1] == 7

    # The session captures its head: the hold is released.
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {SESSIONS} SET head_seq=snapshot_seq")
    migrated_db.commit()
    _publish(migrated_db)
    head, floor, _retention, low, _high, count = _frontier(migrated_db)
    assert head == MINIMUM + 3 * step + 2
    assert floor == 7 + step and low == floor + 1
    assert count == head - floor

    catalog_enrichment.compact_enrichment_storage(migrated_db, SOURCE)
    migrated_db.commit()
    head, floor, _retention, low, _high, count = _frontier(migrated_db)
    assert floor == head - MINIMUM and count == MINIMUM

    # Steady state: each publication advances the floor by exactly one.
    _publish(migrated_db)
    assert _frontier(migrated_db)[1] == floor + 1
    assert _frontier(migrated_db)[5] == MINIMUM
