"""P3-13 (LUM-001 test strength): the profile stream head is serialised by a row lock.

Both journal append paths, ``record_profile_change`` and (P2-3)
``record_profile_deletions``, read the head from ``profile_stream_state`` with
``SELECT ... FOR UPDATE`` (``catalog_enrichment._profile_stream_state``) and
allocate the next seqs from it. They write the events and the new head later
in the same transaction. The audit found that removing that ``FOR UPDATE``
failed only 1 of 39 tests, for two reasons:

* the real publishers (``complete_attempt``, the edge publisher, catalogue
  invalidation) first lock the source's ``catalog_state`` row, and that lock
  already serialises them;
* the two-connection tests park the first publisher after it has updated the
  head. The second publisher's ``INSERT ... ON CONFLICT DO NOTHING`` on the
  state row then waits for that pending update anyway, so the row lock is
  never what orders them.

These tests close both gaps. They call the append paths and maintenance
compaction (``compact_enrichment_storage``, which never takes
``catalog_state``) directly, on different tracks. Each test parks the first
writer in the one window that only the row lock covers: after it read the head
and before it wrote anything. The next writer must then wait, in its head
read, on the stream-state row the first one holds. The tests prove this by
polling ``pg_locks`` and ``pg_blocking_pids`` with a bounded timeout, not with
sleeps. After both commit, the journal must be dense: unique, contiguous seqs
from ``floor + 1`` to ``head``, all in the current epoch.

Without the lock, the next writer reads the same head without waiting. The
parked append then collides on the same seq. A parked compaction instead
computes the floor from a stale head or retention limit.
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
# The journal append paths: record_profile_change, record_profile_deletions.
APPENDS = ("change", "deletions")
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


def _journaled(kind, name):
    """The (track, operation) events one append by ``kind`` journals, in order."""
    if kind == "change":
        return [(name, "upsert")]
    return [(f"{name}/gone-1", "delete"), (f"{name}/gone-2", "delete")]


def _append(kind, name, park=None):
    """One journal append, with no catalog_state lock in front of it."""

    def action(conn):
        cur = conn.cursor()
        if park is not None:
            cur = _ParkingCursor(cur, park)
        with cur:
            if kind == "change":
                enrichment.record_profile_change(
                    cur, SOURCE, name, "ready", {"track_id": name}
                )
            else:
                enrichment.record_profile_deletions(
                    cur, SOURCE, [track for track, _op in _journaled(kind, name)]
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

    Returns the lock it waits for and the statement that waits, or None if it
    finished without waiting. The statement is read only after the wait is
    seen, in a second query: one query that joined ``pg_stat_activity`` could
    take its activity snapshot before the writer started the statement, and
    report the previous one. A writer blocked on a lock cannot move on while
    the holder stays parked.
    """
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        with observer.cursor() as cur:
            cur.execute(
                "SELECT locktype, mode, pg_blocking_pids(pid) FROM pg_locks "
                "WHERE pid = %s AND NOT granted",
                (writer.pid,),
            )
            row = cur.fetchone()
            if row is not None:
                cur.execute(
                    "SELECT query FROM pg_stat_activity WHERE pid = %s",
                    (writer.pid,),
                )
                return {
                    "locktype": row[0],
                    "mode": row[1],
                    "blockers": set(row[2]),
                    "query": cur.fetchone()[0],
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


def _bound_waits(conn):
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '8s'")
        cur.execute("SET statement_timeout = '8s'")
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
        _bound_waits(conn)
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
    _bound_waits(second_connection)
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
            f"SELECT seq, epoch, track_id, operation FROM {CHANGES} "
            "WHERE catalog_instance_id=%s ORDER BY seq",
            (SOURCE,),
        )
        rows = cur.fetchall()
    db.commit()
    state = {"epoch": epoch, "head": head, "floor": floor, "retention": retention}
    return state, rows


def _events(rows):
    return [(row[2], row[3]) for row in rows]


def _assert_waited_for(wait, writer, holders):
    assert wait is not None, (
        f"{writer.name} ran to completion while another writer held the "
        f"stream head it had read: it never waited for the {STATE} row lock"
    )
    assert HEAD_READ in wait["query"], (
        f"{writer.name} did not wait in its head read: {wait}"
    )
    holder_pids = {holder.pid for holder in holders}
    assert wait["blockers"] and wait["blockers"] <= holder_pids, (
        f"{writer.name} was blocked by {wait['blockers']}, not by {holder_pids}"
    )


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


# --------------------------------------------------------------------------
# Appends on different tracks.


@pytest.mark.parametrize("first_outcome", ["commit", "rollback"])
@pytest.mark.parametrize("second_kind", APPENDS)
@pytest.mark.parametrize("first_kind", APPENDS)
def test_append_waits_for_a_head_another_append_read(
    migrated_db, first_connection, open_connection, observer,
    first_kind, second_kind, first_outcome,
):
    epoch = _source(migrated_db)
    park = _Park("first")
    first = _Writer(
        "first", first_connection, _append(first_kind, "first", park),
        finish=first_outcome,
    )
    second = _Writer("second", open_connection(), _append(second_kind, "second"))

    waits = _interleave(observer, park, first, [second])

    _assert_waited_for(waits["second"], second, holders=[first])
    _assert_no_errors(first, second)
    state, rows = _journal(migrated_db)
    _assert_dense(state, rows, epoch)
    expected = (
        _journaled(first_kind, "first") if first_outcome == "commit" else []
    ) + _journaled(second_kind, "second")
    assert _events(rows) == expected
    assert (state["head"], state["floor"]) == (len(expected), 0)


@pytest.mark.parametrize("first_kind", APPENDS)
def test_appends_on_different_tracks_queue_behind_a_read_head(
    migrated_db, first_connection, open_connection, observer, first_kind
):
    epoch = _source(migrated_db)
    park = _Park("first")
    first = _Writer("first", first_connection, _append(first_kind, "first", park))
    kinds = {"first": first_kind}
    queued = []
    for i, kind in enumerate(("deletions", "change", "deletions"), 1):
        name = f"queued-{i}"
        kinds[name] = kind
        queued.append(_Writer(name, open_connection(), _append(kind, name)))

    waits = _interleave(observer, park, first, queued)

    for writer in queued:
        _assert_waited_for(waits[writer.name], writer, holders=[first, *queued])
    _assert_no_errors(first, *queued)
    state, rows = _journal(migrated_db)
    _assert_dense(state, rows, epoch)
    total = sum(len(_journaled(kind, name)) for name, kind in kinds.items())
    assert (state["head"], state["floor"]) == (total, 0)
    # The first append keeps the seqs it read; every append's events are
    # consecutive and in its own order.
    by_writer = {}
    for seq, _epoch, track, operation in rows:
        by_writer.setdefault(track.split("/")[0], []).append((seq, (track, operation)))
    assert sorted(by_writer) == sorted(kinds)
    assert by_writer["first"][0][0] == 1
    for name, kind in kinds.items():
        seqs = [seq for seq, _event in by_writer[name]]
        assert [event for _seq, event in by_writer[name]] == _journaled(kind, name)
        assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))


# --------------------------------------------------------------------------
# An append and maintenance compaction (P1-2 retention and bounded advance).


@pytest.mark.parametrize("kind", APPENDS)
def test_compaction_waits_for_a_head_an_append_read(
    migrated_db, first_connection, open_connection, observer, monkeypatch, kind
):
    monkeypatch.setattr(enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", RETAINED)
    # A backlog larger than one append may compact (P1-2 bounded advance).
    monkeypatch.setattr(enrichment, "PROFILE_COMPACTION_MAX_ADVANCE", 1)
    epoch = _source(migrated_db, head=RETAINED + 5, retention_limit=RETAINED)
    events = _journaled(kind, "new")
    park = _Park("append")
    append = _Writer("append", first_connection, _append(kind, "new", park))
    compactor = _Writer("compactor", open_connection(), _compact())

    waits = _interleave(observer, park, append, [compactor])

    _assert_waited_for(waits["compactor"], compactor, holders=[append])
    _assert_no_errors(append, compactor)
    state, rows = _journal(migrated_db)
    # The append takes the head to 1005 + n and its own compaction moves the
    # floor from 0 by at most one seq per event (bounded). Compaction then
    # starts from that committed head, so the floor is head - 1000 = 5 + n.
    # From a stale head (1005) it would stop at 5.
    head = RETAINED + 5 + len(events)
    assert (state["head"], state["floor"], state["retention"]) == (
        head, head - RETAINED, RETAINED
    )
    _assert_dense(state, rows, epoch)
    assert _events(rows[-len(events):]) == events


@pytest.mark.parametrize("kind", APPENDS)
def test_append_waits_for_a_head_compaction_read(
    migrated_db, first_connection, open_connection, observer, monkeypatch, kind
):
    monkeypatch.setattr(enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", RETAINED)
    monkeypatch.setattr(enrichment, "PROFILE_COMPACTION_MAX_ADVANCE", 1)
    # The persisted limit is stale (a larger library); compaction lowers it.
    epoch = _source(migrated_db, head=RETAINED + 5, retention_limit=RETAINED * 100)
    events = _journaled(kind, "new")
    park = _Park("compactor")
    compactor = _Writer("compactor", first_connection, _compact(park))
    append = _Writer("append", open_connection(), _append(kind, "new"))

    waits = _interleave(observer, park, compactor, [append])

    # This wait assertion is what pins the append's own lock. An append that
    # read the head without the lock still waits for the parked compactor,
    # but in its head UPDATE, and that UPDATE returns the retention limit the
    # compactor committed.
    _assert_waited_for(waits["append"], append, holders=[compactor])
    _assert_no_errors(compactor, append)
    state, rows = _journal(migrated_db)
    # Compaction persists the lowered limit and moves the floor to 5. The
    # append then reads that floor, takes the head to 1005 + n and moves the
    # floor by at most one seq per event, to head - 1000 = 5 + n. An append
    # that computed from the floor it read before compaction (0) would stop
    # at 5. One that also kept the stale limit would compact nothing.
    head = RETAINED + 5 + len(events)
    assert (state["head"], state["floor"], state["retention"]) == (
        head, head - RETAINED, RETAINED
    )
    _assert_dense(state, rows, epoch)
    assert _events(rows[-len(events):]) == events
