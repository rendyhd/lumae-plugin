"""P1-7 (LUM-001 gap F5): the v1 ``/changes`` readers must never skip events.

Each reader takes the stream state (epoch, head, floor) and the page of events
from one snapshot, and verifies the page is dense: when ``cursor < head`` the
first returned seq is ``cursor + 1`` and the seqs are contiguous. A violation
is ``bootstrap_required`` (410), never a silently short page.

The concurrency cases port the audit probe "v1 changes skip under concurrent
compaction": the real compaction commits on ``second_connection`` between the
reader's statements, deterministically, through a statement hook on the
reader's connection.
"""

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import catalog, catalog_analysis, catalog_enrichment


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
PROFILE_RETENTION = catalog_enrichment.PROFILE_CHANGE_RETENTION_EVENTS
CATALOG_RETENTION = catalog.MIN_RETAINED_CHANGE_EVENTS


# --------------------------------------------------------------------------
# Statement hook: run ``action`` once, at a chosen point between statements.


class _HookedCursor:
    def __init__(self, cursor, owner):
        self._cursor = cursor
        self._owner = owner

    def execute(self, sql, params=None):
        self._owner.before(str(sql))
        result = self._cursor.execute(sql, params)
        self._owner.after(str(sql))
        return result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._cursor.close()
        return False

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _HookedConnection:
    """Proxy connection that fires ``action`` once around a matching statement.

    ``when="after"`` fires right after the first statement whose SQL contains
    ``marker`` (e.g. just after the reader read the stream state);
    ``when="before"`` fires right before it (e.g. just before the journal
    read). Either way the action commits on another connection, so the
    reader's next statement sees it.
    """

    def __init__(self, conn, marker, when, action):
        self._conn = conn
        self._marker = marker
        self._when = when
        self._action = action
        self.fired = False

    def _maybe(self, sql, when):
        if not self.fired and self._when == when and self._marker in sql:
            self.fired = True
            self._action()

    def before(self, sql):
        self._maybe(sql, "before")

    def after(self, sql):
        self._maybe(sql, "after")

    def cursor(self, *args, **kwargs):
        return _HookedCursor(self._conn.cursor(*args, **kwargs), self)

    def __getattr__(self, name):
        return getattr(self._conn, name)


# --------------------------------------------------------------------------
# Fixtures on the real migrated schema.


def _source(db):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " server_name, is_default, rebind_status) "
            "VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')",
            (SOURCE,),
        )
        cur.execute(
            f"INSERT INTO {P}catalog_state "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, 'server-a', 'navidrome', 1, 'catalog-epoch', 'complete', "
            "        '{}'::jsonb)",
            (SOURCE,),
        )
        cur.execute(
            f"INSERT INTO {P}analysis_state "
            "(catalog_instance_id, projection_generation, analysis_epoch, status) "
            "VALUES (%s, 1, 'analysis-epoch', 'complete')",
            (SOURCE,),
        )
        cur.execute(
            f"INSERT INTO {P}relationship_state "
            "(catalog_instance_id, relationship_schema_version, epoch, status) "
            "VALUES (%s, %s, 'relationship-epoch', 'complete')",
            (SOURCE, catalog_enrichment.RELATIONSHIP_SCHEMA_VERSION),
        )
        catalog_enrichment._profile_stream_state(cur, SOURCE, for_update=True)
    db.commit()


# stream -> (changes table, extra column values, state table, epoch, head, floor)
STREAMS = {
    "profile": (
        "profile_changes", "track_id, operation, writer_generation",
        "'track-' || n, 'delete', 2",
        "profile_stream_state", "epoch", "head_seq", "floor_seq",
    ),
    "catalog": (
        "catalog_changes", "generation, entity_type, entity_id, operation, writer_generation",
        "1, 'track', 'track-' || n, 'delete', 2",
        "catalog_state", "catalog_epoch", "catalog_head_seq", "catalog_floor_seq",
    ),
    "analysis": (
        "analysis_changes", "generation, entity_type, entity_id, operation",
        "1, 'analysis_item', 'item-' || n, 'delete'",
        "analysis_state", "analysis_epoch", "analysis_head_seq", "analysis_floor_seq",
    ),
    "relationship": (
        "relationship_changes", "generation, entity_type, entity_id, operation",
        "1, 'album', 'album-' || n, 'delete'",
        "relationship_state", "epoch", "head_seq", "floor_seq",
    ),
}


def _epoch(db, stream):
    _c, _cols, _vals, state, epoch_col, _h, _f = STREAMS[stream]
    with db.cursor() as cur:
        cur.execute(
            f"SELECT {epoch_col} FROM {P}{state} WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
        epoch = cur.fetchone()[0]
    db.commit()
    return epoch


def _seed(db, stream, last):
    """Append dense events 1..last and move the head, as a publisher would."""
    changes, cols, vals, state, epoch_col, head_col, _f = STREAMS[stream]
    epoch = _epoch(db, stream)
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}{changes} (catalog_instance_id, epoch, seq, {cols}) "
            f"SELECT %s, %s, n, {vals} FROM generate_series(1, %s) AS n",
            (SOURCE, epoch, last),
        )
        cur.execute(
            f"UPDATE {P}{state} SET {head_col}=%s WHERE catalog_instance_id=%s",
            (last, SOURCE),
        )
    db.commit()
    return epoch


def _delete_event(db, stream, seq):
    changes = STREAMS[stream][0]
    with db.cursor() as cur:
        cur.execute(
            f"DELETE FROM {P}{changes} WHERE catalog_instance_id=%s AND seq=%s",
            (SOURCE, seq),
        )
    db.commit()


def _floor(db, stream):
    _c, _cols, _vals, state, _e, _h, floor_col = STREAMS[stream]
    with db.cursor() as cur:
        cur.execute(
            f"SELECT {floor_col} FROM {P}{state} WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
        floor = cur.fetchone()[0]
    db.commit()
    return floor


def _read(stream, db, cursor, limit=5):
    if stream == "profile":
        return catalog_enrichment.read_profile_changes(db, cursor, SOURCE, limit=limit)
    if stream == "catalog":
        return catalog.read_catalog_changes(db, cursor, catalog_instance_id=SOURCE, limit=limit)
    if stream == "analysis":
        return catalog_analysis.read_analysis_changes(
            db, cursor, catalog_instance_id=SOURCE, limit=limit
        )
    return catalog_enrichment.read_relationship_changes(db, cursor, SOURCE, limit=limit)


def _seqs(page):
    return [change["seq"] for change in page["changes"]]


def _assert_no_skip(page, cursor_seq):
    seqs = _seqs(page)
    assert seqs, "a cursor behind the head must return events or 410"
    assert seqs == list(range(cursor_seq + 1, cursor_seq + 1 + len(seqs))), (
        f"events skipped after cursor {cursor_seq}: {seqs}"
    )


# --------------------------------------------------------------------------
# Concurrent compaction between the reader's statements (the audit probe).


def _compact(stream, peer):
    def action():
        if stream == "profile":
            catalog_enrichment.compact_enrichment_storage(peer, SOURCE)
        else:
            catalog.prune_catalog_storage(peer, SOURCE)
        peer.commit()

    return action


@pytest.mark.parametrize(
    "stream, retention, marker, when",
    [
        # Compaction commits right after the reader read the stream state.
        ("profile", PROFILE_RETENTION, "profile_stream_state", "after"),
        # ... or right before the reader reads the journal.
        ("profile", PROFILE_RETENTION, "profile_changes", "before"),
        ("catalog", CATALOG_RETENTION, "catalog_state", "after"),
        ("catalog", CATALOG_RETENTION, "catalog_changes", "before"),
    ],
)
def test_concurrent_compaction_never_skips_an_event(
    migrated_db, second_connection, stream, retention, marker, when
):
    _source(migrated_db)
    # Two events beyond retention: compaction moves the floor from 0 to 2 and
    # deletes seqs 1 and 2. A client at cursor 1 still needs seq 2.
    epoch = _seed(migrated_db, stream, retention + 2)
    cursor = catalog.opaque_cursor(SOURCE, epoch, 1)
    hooked = _HookedConnection(
        migrated_db, marker, when, _compact(stream, second_connection)
    )

    try:
        page = _read(stream, hooked, cursor)
    except KeyError as exc:
        # The reader saw the advanced floor: resync (410), nothing skipped.
        assert "bootstrap_required" in str(exc)
        page = None
    migrated_db.rollback()

    assert hooked.fired, "the compaction hook must run during the read"
    assert _floor(migrated_db, stream) == 2, "compaction really advanced the floor"
    if page is not None:
        # The reader's snapshot predates the compaction: a full, dense page.
        _assert_no_skip(page, 1)
        assert _seqs(page) == [2, 3, 4, 5, 6]


def test_profile_reader_sees_compaction_as_410_not_a_short_page(
    migrated_db, second_connection
):
    """The exact audit probe timing: compaction commits before the journal read."""
    _source(migrated_db)
    epoch = _seed(migrated_db, "profile", PROFILE_RETENTION + 2)
    hooked = _HookedConnection(
        migrated_db, "profile_changes", "before", _compact("profile", second_connection)
    )
    with pytest.raises(KeyError, match="bootstrap_required"):
        catalog_enrichment.read_profile_changes(
            hooked, catalog.opaque_cursor(SOURCE, epoch, 1), SOURCE, limit=5
        )
    migrated_db.rollback()


# --------------------------------------------------------------------------
# Density, paging and cursor validation for every v1 journal reader.


ALL_STREAMS = ["profile", "catalog", "analysis", "relationship"]


@pytest.mark.parametrize("stream", ALL_STREAMS)
def test_gap_inside_the_page_is_410(migrated_db, stream):
    _source(migrated_db)
    epoch = _seed(migrated_db, stream, 20)
    _delete_event(migrated_db, stream, 4)
    with pytest.raises(KeyError, match="bootstrap_required"):
        _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, epoch, 1))
    migrated_db.rollback()


@pytest.mark.parametrize("stream", ALL_STREAMS)
def test_missing_first_event_is_410(migrated_db, stream):
    _source(migrated_db)
    epoch = _seed(migrated_db, stream, 20)
    _delete_event(migrated_db, stream, 6)
    with pytest.raises(KeyError, match="bootstrap_required"):
        _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, epoch, 5))
    migrated_db.rollback()


@pytest.mark.parametrize("stream", ALL_STREAMS)
def test_missing_tail_events_are_410(migrated_db, stream):
    _source(migrated_db)
    epoch = _seed(migrated_db, stream, 20)
    _delete_event(migrated_db, stream, 20)
    # Short page (19 < min(limit, head - cursor)): the tail is missing.
    with pytest.raises(KeyError, match="bootstrap_required"):
        _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, epoch, 15), limit=50)
    migrated_db.rollback()
    # Only the head event exists after cursor 19, and it is gone.
    with pytest.raises(KeyError, match="bootstrap_required"):
        _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, epoch, 19))
    migrated_db.rollback()


@pytest.mark.parametrize("stream", ALL_STREAMS)
def test_normal_paging_is_unchanged(migrated_db, stream):
    _source(migrated_db)
    epoch = _seed(migrated_db, stream, 12)
    cursor = catalog.opaque_cursor(SOURCE, epoch, 0)
    head_cursor = catalog.opaque_cursor(SOURCE, epoch, 12)
    seen = []
    pages = 0
    while True:
        page = _read(stream, migrated_db, cursor, limit=5)
        migrated_db.rollback()
        pages += 1
        seen.extend(_seqs(page))
        assert page["head_cursor"] == head_cursor
        assert page["cursor"] == catalog.opaque_cursor(SOURCE, epoch, seen[-1])
        cursor = page["cursor"]
        if not page["has_more"]:
            break
    assert pages == 3
    assert seen == list(range(1, 13))
    # At the head: an empty page, same cursor, nothing more.
    page = _read(stream, migrated_db, head_cursor)
    migrated_db.rollback()
    assert page["changes"] == []
    assert page["cursor"] == head_cursor
    assert page["has_more"] is False


def test_profile_page_shape_is_unchanged(migrated_db):
    _source(migrated_db)
    epoch = _seed(migrated_db, "profile", 3)
    page = catalog_enrichment.read_profile_changes(
        migrated_db, catalog.opaque_cursor(SOURCE, epoch, 0), SOURCE, limit=2
    )
    migrated_db.rollback()
    assert set(page) == {
        "schema_version", "catalog_instance_id", "changes", "cursor",
        "head_cursor", "has_more",
    }
    assert page["schema_version"] == 1
    assert page["catalog_instance_id"] == SOURCE
    assert page["has_more"] is True
    assert set(page["changes"][0]) == {
        "seq", "track_id", "operation", "payload", "created_at",
    }
    assert page["changes"][0]["track_id"] == "track-1"


@pytest.mark.parametrize("stream", ALL_STREAMS)
def test_cursor_ahead_of_head_is_still_400(migrated_db, stream):
    _source(migrated_db)
    epoch = _seed(migrated_db, stream, 5)
    with pytest.raises(ValueError, match="ahead"):
        _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, epoch, 6))
    migrated_db.rollback()


@pytest.mark.parametrize("stream", ALL_STREAMS)
def test_old_epoch_and_below_floor_are_still_410(migrated_db, stream):
    _source(migrated_db)
    epoch = _seed(migrated_db, stream, 5)
    with pytest.raises(KeyError, match="bootstrap_required"):
        _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, "other-epoch", 1))
    migrated_db.rollback()
    _c, _cols, _vals, state, _e, _h, floor_col = STREAMS[stream]
    with migrated_db.cursor() as cur:
        cur.execute(
            f"UPDATE {P}{state} SET {floor_col}=3 WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
    migrated_db.commit()
    with pytest.raises(KeyError, match="bootstrap_required"):
        _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, epoch, 2))
    migrated_db.rollback()


@pytest.mark.parametrize("stream", ALL_STREAMS)
def test_cursor_at_a_raised_floor_reads_on(migrated_db, stream):
    """cursor == floor (> 0) is valid: the next event is the first retained."""
    _source(migrated_db)
    epoch = _seed(migrated_db, stream, 5)
    changes, _cols, _vals, state, _e, _h, floor_col = STREAMS[stream]
    with migrated_db.cursor() as cur:
        cur.execute(
            f"DELETE FROM {P}{changes} WHERE catalog_instance_id=%s AND seq<=3",
            (SOURCE,),
        )
        cur.execute(
            f"UPDATE {P}{state} SET {floor_col}=3 WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
    migrated_db.commit()
    page = _read(stream, migrated_db, catalog.opaque_cursor(SOURCE, epoch, 3))
    migrated_db.rollback()
    assert _seqs(page) == [4, 5]
    assert page["has_more"] is False


def test_catalog_snapshot_metadata_matches_the_page_snapshot(
    migrated_db, second_connection
):
    """A publication committing after source resolution still reports the
    generation, counts and bytes of the snapshot the page was read from."""
    _source(migrated_db)
    epoch = _seed(migrated_db, "catalog", 3)

    def publish():
        with second_connection.cursor() as cur:
            cur.execute(
                f"UPDATE {P}catalog_state SET published_generation=2, "
                "entity_counts='{\"track\": 7}'::jsonb, snapshot_estimated_bytes=99 "
                "WHERE catalog_instance_id=%s",
                (SOURCE,),
            )
        second_connection.commit()

    hooked = _HookedConnection(migrated_db, "catalog_state", "after", publish)
    page = catalog.read_catalog_changes(
        hooked, catalog.opaque_cursor(SOURCE, epoch, 0), catalog_instance_id=SOURCE
    )
    migrated_db.rollback()
    assert hooked.fired
    assert _seqs(page) == [1, 2, 3]
    assert page["snapshot_generation"] == 2
    assert page["snapshot_entity_counts"] == {"track": 7}
    assert page["snapshot_estimated_bytes"] == 99
    assert page["fingerprint_schema_version"] >= 1
