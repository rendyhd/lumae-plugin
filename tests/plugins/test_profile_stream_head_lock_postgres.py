"""P3-13 (LUM-001 test strength): the profile stream head is serialised by a row lock.

Each profile publisher allocates ``seq = head_seq + 1`` from
``profile_stream_state`` with ``SELECT ... FOR UPDATE``
(``catalog_enrichment._profile_stream_state``). It writes the event and the
new head later in the same transaction. The audit found that removing that
``FOR UPDATE`` failed only 1 of 39 tests, for two reasons:

* the real publishers (``complete_attempt``, the edge publisher, catalogue
  invalidation) first lock the source's ``catalog_state`` row, and that lock
  already serialises them;
* the two-connection tests park the first publisher after it has updated the
  head. The second publisher's ``INSERT ... ON CONFLICT DO NOTHING`` on the
  state row then waits for that pending update anyway, so the row lock is
  never what orders them.

These tests close both gaps. They call the journal append path
(``record_profile_change``) and maintenance compaction
(``compact_enrichment_storage``, which never takes ``catalog_state``) directly,
on different tracks. Each test parks the first writer in the one window that
only the row lock covers: after it read the head and before it wrote
anything. The next writer must then wait on the stream-state row the first
one holds. The tests prove this by polling ``pg_locks`` and
``pg_blocking_pids`` with a bounded timeout, not with sleeps. After both
commit, the journal must be dense: unique, contiguous seqs from ``floor + 1``
to ``head``, all in the current epoch.

Without the ``FOR UPDATE``, the next writer reads the same head without
waiting. The parked publisher then collides on the same seq. A parked
compaction instead computes the floor from a stale head or retention limit.
"""

import threading
import time

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from pg_helpers import connect
from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import catalog, catalog_enrichment as enrichment


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
STATE = f"{P}profile_stream_state"
CHANGES = f"{P}profile_changes"
# The head read in _profile_stream_state, with or without its FOR UPDATE.
HEAD_READ = f"SELECT epoch, head_seq, floor_seq FROM {STATE}"
RETAINED = catalog.MIN_RETAINED_CHANGE_EVENTS
# Bound for every poll and hand-off. Nothing here waits this long when the
# code is correct; it only turns a hang into a failure.
WAIT_SECONDS = 10
POLL_SECONDS = 0.01


# --------------------------------------------------------------------------
# Parking a writer between its head read and its first write.


class _Park:
    """Hold a writer's open transaction right after it read the stream head."""

    def __init__(self, name):
        self.name = name
        self.reached = threading.Event()
        self.release = threading.Event()

    def after(self, sql):
        if HEAD_READ in sql and not self.reached.is_set():
            self.reached.set()
            if not self.release.wait(WAIT_SECONDS * 6):
                raise RuntimeError(f"{self.name} was never released")


class _ParkingCursor:
    def __init__(self, cursor, park):
        self._cursor = cursor
        self._park = park

    def execute(self, sql, params=None):
        result = self._cursor.execute(sql, params)
        self._park.after(str(sql))
        return result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._cursor.close()
        return False

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _ParkingConnection:
    def __init__(self, conn, park):
        self._conn = conn
        self._park = park

    def cursor(self, *args, **kwargs):
        return _ParkingCursor(self._conn.cursor(*args, **kwargs), self._park)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _Writer(threading.Thread):
    """One writer transaction on its own connection and thread."""

    def __init__(self, name, conn, action, finish="commit"):
        super().__init__(name=name, daemon=True)
        self.conn = conn
        self.pid = conn.get_backend_pid()
        self.action = action
        self.finish = finish
        self.error = None

    def run(self):
        try:
            self.action(self.conn)
            if self.finish == "commit":
                self.conn.commit()
            else:
                self.conn.rollback()
        except Exception as exc:  # reported by _assert_no_errors
            self.error = exc
            try:
                self.conn.rollback()
            except Exception:
                pass


def _publish(track, park=None):
    """The journal append path, with no catalog_state lock in front of it."""

    def action(conn):
        cur = conn.cursor()
        if park is not None:
            cur = _ParkingCursor(cur, park)
        with cur:
            enrichment.record_profile_change(
                cur, SOURCE, track, "ready", {"track_id": track}
            )

    return action


def _compact(park=None):
    """Maintenance compaction: the stream-head lock is its only serialisation."""

    def action(conn):
        db = conn if park is None else _ParkingConnection(conn, park)
        enrichment.compact_enrichment_storage(db, SOURCE)

    return action


def _lock_wait(observer, writer):
    """Poll ``pg_locks`` until ``writer`` waits for a lock.

    Returns the lock it waits for, or None if it finished without waiting.
    """
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        with observer.cursor() as cur:
            cur.execute(
                """
                SELECT l.locktype, l.mode, pg_blocking_pids(l.pid), a.query
                  FROM pg_locks l
                  JOIN pg_stat_activity a ON a.pid = l.pid
                 WHERE l.pid = %s AND NOT l.granted
                """,
                (writer.pid,),
            )
            row = cur.fetchone()
        if row is not None:
            return {
                "locktype": row[0],
                "mode": row[1],
                "blockers": set(row[2]),
                "query": row[3],
            }
        if not writer.is_alive():
            return None
        writer.join(POLL_SECONDS)
    pytest.fail(
        f"{writer.name} neither waited for a lock nor finished in {WAIT_SECONDS}s"
    )


def _interleave(observer, park, first, others):
    """Park ``first`` after its head read, then start each of ``others``.

    Each of ``others`` starts only after the previous one waits for a lock or
    finishes. Then ``first`` is released and every writer finishes. Returns
    the lock wait recorded for each writer in ``others``.
    """
    waits = {}
    writers = [first, *others]
    try:
        first.start()
        assert park.reached.wait(WAIT_SECONDS), f"{first.name} never read the stream head"
        for writer in others:
            writer.start()
            waits[writer.name] = _lock_wait(observer, writer)
    finally:
        park.release.set()
        for writer in writers:
            if writer.ident is not None:
                writer.join(WAIT_SECONDS * 3)
    assert not [writer.name for writer in writers if writer.is_alive()]
    return waits


# --------------------------------------------------------------------------
# Schema, fixtures and assertions.


def _bound_lock_waits(conn):
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '30s'")
    conn.commit()


@pytest.fixture
def open_connection(migrated_db):
    """Open more connections to the test schema, closed before it is dropped."""
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    migrated_db.commit()
    opened = []

    def factory(autocommit=False):
        conn = connect(schema)
        _bound_lock_waits(conn)
        conn.autocommit = autocommit
        opened.append(conn)
        return conn

    yield factory
    for conn in opened:
        for step in (conn.rollback, conn.close):
            try:
                step()
            except Exception:
                pass


@pytest.fixture
def first_connection(second_connection):
    _bound_lock_waits(second_connection)
    return second_connection


@pytest.fixture
def observer(open_connection):
    return open_connection(autocommit=True)


def _source(db, *, head=0, retention_limit=None):
    """A committed source with its stream row and events 1..head."""
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " server_name, is_default, rebind_status) "
            "VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')",
            (SOURCE,),
        )
        epoch, _head, _floor = enrichment._profile_stream_state(
            cur, SOURCE, for_update=True
        )
        cur.execute(
            f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, "
            "operation, writer_generation) "
            "SELECT %s, %s, n, 'seed-' || n, 'delete', %s "
            "FROM generate_series(1, %s) AS n",
            (SOURCE, epoch, enrichment.JOURNAL_WRITER_GENERATION, head),
        )
        cur.execute(
            f"UPDATE {STATE} SET head_seq=%s, "
            "retention_limit=COALESCE(%s, retention_limit) "
            "WHERE catalog_instance_id=%s",
            (head, retention_limit, SOURCE),
        )
    db.commit()
    return epoch


def _journal(db):
    with db.cursor() as cur:
        cur.execute(
            f"SELECT epoch, head_seq, floor_seq, retention_limit FROM {STATE} "
            "WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
        epoch, head, floor, retention = cur.fetchone()
        cur.execute(
            f"SELECT seq, epoch, track_id FROM {CHANGES} "
            "WHERE catalog_instance_id=%s ORDER BY seq",
            (SOURCE,),
        )
        rows = cur.fetchall()
    db.commit()
    state = {"epoch": epoch, "head": head, "floor": floor, "retention": retention}
    return state, rows


def _assert_no_errors(*writers):
    errors = {
        writer.name: f"{type(writer.error).__name__}: {writer.error}".strip()
        for writer in writers
        if writer.error is not None
    }
    assert errors == {}


def _assert_dense(state, rows, epoch):
    """Unique, contiguous seqs floor+1..head in one epoch, so head == max(seq)."""
    assert state["epoch"] == epoch
    assert {row[1] for row in rows} <= {epoch}
    seqs = [row[0] for row in rows]
    expected = list(range(state["floor"] + 1, state["head"] + 1))
    assert seqs == expected, (
        f"journal is not dense: floor={state['floor']} head={state['head']} "
        f"seqs={seqs[:3]}..{seqs[-3:]} ({len(seqs)} rows, "
        f"{len(set(seqs))} distinct)"
    )


def _assert_waited_for(wait, writer, holders):
    assert wait is not None, (
        f"{writer.name} ran to completion while another writer held the "
        f"stream head it had read: it never waited for the {STATE} row lock"
    )
    assert STATE in wait["query"], f"{writer.name} waited elsewhere: {wait}"
    holder_pids = {holder.pid for holder in holders}
    assert wait["blockers"] and wait["blockers"] <= holder_pids, (
        f"{writer.name} was blocked by {wait['blockers']}, not by {holder_pids}"
    )


# --------------------------------------------------------------------------
# Publishers on different tracks.


@pytest.mark.parametrize("first_outcome", ["commit", "rollback"])
def test_publisher_waits_for_a_head_another_publisher_read(
    migrated_db, first_connection, open_connection, observer, first_outcome
):
    epoch = _source(migrated_db)
    park = _Park("track-first")
    first = _Writer(
        "track-first", first_connection, _publish("track-first", park),
        finish=first_outcome,
    )
    second = _Writer("track-second", open_connection(), _publish("track-second"))

    waits = _interleave(observer, park, first, [second])

    _assert_no_errors(first, second)
    state, rows = _journal(migrated_db)
    _assert_dense(state, rows, epoch)
    expected = (
        ["track-first", "track-second"] if first_outcome == "commit"
        else ["track-second"]
    )
    assert [row[2] for row in rows] == expected
    assert (state["head"], state["floor"]) == (len(expected), 0)
    _assert_waited_for(waits["track-second"], second, holders=[first])


def test_publishers_on_different_tracks_queue_behind_a_read_head(
    migrated_db, first_connection, open_connection, observer
):
    epoch = _source(migrated_db)
    park = _Park("track-0")
    first = _Writer("track-0", first_connection, _publish("track-0", park))
    queued = [
        _Writer(f"track-{i}", open_connection(), _publish(f"track-{i}"))
        for i in (1, 2, 3)
    ]

    waits = _interleave(observer, park, first, queued)

    _assert_no_errors(first, *queued)
    state, rows = _journal(migrated_db)
    _assert_dense(state, rows, epoch)
    assert (state["head"], state["floor"]) == (4, 0)
    assert rows[0][2] == "track-0"
    assert sorted(row[2] for row in rows) == [f"track-{i}" for i in range(4)]
    for writer in queued:
        _assert_waited_for(waits[writer.name], writer, holders=[first, *queued])


# --------------------------------------------------------------------------
# Publisher and maintenance compaction (P1-2 retention and bounded advance).


def test_compaction_waits_for_a_head_a_publisher_read(
    migrated_db, first_connection, open_connection, observer, monkeypatch
):
    monkeypatch.setattr(enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", RETAINED)
    # A backlog larger than one publication may compact (P1-2 bounded advance).
    monkeypatch.setattr(enrichment, "PROFILE_COMPACTION_MAX_ADVANCE", 1)
    epoch = _source(migrated_db, head=RETAINED + 5, retention_limit=RETAINED)
    park = _Park("publisher")
    publisher = _Writer("publisher", first_connection, _publish("track-new", park))
    compactor = _Writer("compactor", open_connection(), _compact())

    waits = _interleave(observer, park, publisher, [compactor])

    _assert_no_errors(publisher, compactor)
    state, rows = _journal(migrated_db)
    # The publication appends 1006 and advances the floor 0 -> 1 (bounded).
    # Compaction then starts from that committed head, so the floor is
    # 1006 - 1000. With a stale head (1005) it would stop at 5.
    assert (state["head"], state["floor"], state["retention"]) == (
        RETAINED + 6, 6, RETAINED
    )
    _assert_dense(state, rows, epoch)
    assert rows[-1] == (RETAINED + 6, epoch, "track-new")
    _assert_waited_for(waits["compactor"], compactor, holders=[publisher])


def test_publisher_waits_for_a_head_compaction_read(
    migrated_db, first_connection, open_connection, observer, monkeypatch
):
    monkeypatch.setattr(enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", RETAINED)
    # The persisted limit is stale (a larger library); compaction lowers it.
    epoch = _source(migrated_db, head=RETAINED + 5, retention_limit=RETAINED * 100)
    park = _Park("compactor")
    compactor = _Writer("compactor", first_connection, _compact(park))
    publisher = _Writer("publisher", open_connection(), _publish("track-new"))

    waits = _interleave(observer, park, compactor, [publisher])

    _assert_no_errors(compactor, publisher)
    state, rows = _journal(migrated_db)
    # Compaction persists the lowered limit and moves the floor to 5. The
    # publication then reads both, appends 1006 and moves the floor to 6.
    # With the stale limit and floor it would compact nothing (floor 5).
    assert (state["head"], state["floor"], state["retention"]) == (
        RETAINED + 6, 6, RETAINED
    )
    _assert_dense(state, rows, epoch)
    assert rows[-1] == (RETAINED + 6, epoch, "track-new")
    _assert_waited_for(waits["publisher"], publisher, holders=[compactor])
