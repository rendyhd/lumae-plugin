"""P2-3 (LUM-008 gap F4): catalogue publication holds catalog_state briefly.

Admission, completion and edge publication lock the source's catalog_state
row (FOR UPDATE), so every analysis request waits while a catalogue
publication holds it. These tests pin the P2-3 shape on the real migrated
schema:

* ``invalidate_catalog_changes`` is set-based: a fixed number of statements
  for any number of changed tracks, with rows and journal events identical to
  the per-track implementation, which is kept below as the oracle. Its bulk
  journal append waits for the profile_stream_state head like any publisher;
* ``refresh_catalog`` builds the diff, the generation, its catalogue journal
  and the invalidation plan before it locks catalog_state, and deletes old
  generations and unreachable edge payloads after the commit. Under the lock
  it only re-checks its base, publishes and withdraws. A refresh that waited
  for another publisher diffs against that one's publication; edges a failed
  purge leaves go with the next publication or maintenance;
* the LUM-008 fence still holds for every interleaving of a completion or an
  admission with that shorter publication, including a track that had no
  profile rows when the refresh started;
* admission and completion keep ``FOR UPDATE`` on catalog_state. ``FOR SHARE``
  was evaluated and rejected: two share holders can deadlock on
  source_profiles rows and profile_stream_state (the last test runs that
  interleaving and fails with a deadlock under ``FOR SHARE``).
"""

import itertools
import threading
import time
from types import SimpleNamespace

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2 import errors  # noqa: E402

from pg_helpers import connect  # noqa: E402
from test_lumae_analysis import RefreshBridge  # noqa: E402  (installs the plugin.api stub)
from plugins.LumaeAnalysis import (  # noqa: E402
    catalog,
    catalog_enrichment,
    profile_publication,
)
from plugins.LumaeAnalysis.catalog_enrichment import record_profile_change  # noqa: E402
from plugins.LumaeAnalysis.profile_publication import (  # noqa: E402
    _known_different_signature,
    _withdraw,
    table,
)


SOURCE = "catalog-a"
OTHER = "catalog-b"
GENERATION = 2
P = "plugin_lumae_analysis__"


# ---------------------------------------------------------------------------
# Oracle: invalidate_catalog_changes as it was before P2-3 (per track).
# ---------------------------------------------------------------------------
def per_track_invalidation(cur, source, generation, track_changes, *, full_reconcile=False):
    """Withdraw known changed/deleted occurrences inside catalogue publication."""
    cur.execute("SELECT to_regclass(%s)", (table("published_source_profiles"),))
    publication_table = cur.fetchone()
    if not publication_table or publication_table[0] is None:
        return 0
    changed = {}
    for entity_type, track_id, operation, *_rest in track_changes:
        if entity_type == "track":
            changed[str(track_id)] = operation
    if full_reconcile:
        # A fingerprint-schema rebase suppresses ordinary catalog events, so
        # compare every existing publication against the new generation.
        cur.execute(
            f"SELECT track_id FROM {table('published_source_profiles')} "
            "WHERE catalog_instance_id=%s",
            (source,),
        )
        for (track_id,) in cur.fetchall():
            changed.setdefault(str(track_id), "upsert")
        cur.execute(
            f"""UPDATE {table('source_profiles')}
                   SET status='stale', last_error='Catalogue epoch changed',
                       attempt_token=NULL
                 WHERE catalog_instance_id=%s AND attempt_token IS NOT NULL""",
            (source,),
        )
    withdrawn = 0
    for track_id, operation in sorted(changed.items()):
        cur.execute(
            f"""SELECT media_fp, available FROM {table('catalog_tracks')}
                 WHERE catalog_instance_id=%s AND published_generation=%s
                   AND track_id=%s""",
            (source, generation, track_id),
        )
        track = cur.fetchone()
        known_deleted = operation == "delete" or track is None or not track[1]
        revision = f"catalog-media:{track[0]}" if track and track[0] else None
        cur.execute(
            f"""SELECT attempt_media_signature, media_signature
                  FROM {table('source_profiles')}
                 WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
            (source, track_id),
        )
        attempt = cur.fetchone()
        attempted_revision = (attempt[0] or attempt[1]) if attempt else None
        if attempt and (
            known_deleted
            or _known_different_signature(attempted_revision, revision)
        ):
            cur.execute(
                f"""UPDATE {table('source_profiles')}
                       SET status='stale',
                           last_error='Catalogue media revision changed or track removed',
                           attempt_token=NULL
                     WHERE catalog_instance_id=%s AND track_id=%s""",
                (source, track_id),
            )
        cur.execute(
            f"""SELECT media_signature FROM {table('published_source_profiles')}
                 WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
            (source, track_id),
        )
        published = cur.fetchone()
        if published and (
            known_deleted or _known_different_signature(published[0], revision)
        ):
            withdrawn += int(_withdraw(cur, source, track_id))
    return withdrawn


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------
def _source(cur, source, server, generation=GENERATION):
    cur.execute(
        f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
        "provider_type, server_name, is_default, rebind_status) "
        "VALUES (%s, %s, 'navidrome', %s, %s, 'active')",
        (source, server, server, source == SOURCE),
    )
    cur.execute(
        f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, "
        "provider_type, published_generation, catalog_epoch, status) "
        "VALUES (%s, %s, 'navidrome', %s, 'epoch-a', 'complete')",
        (source, server, generation),
    )


def _track(cur, source, track_id, media_fp, available=True, generation=GENERATION):
    cur.execute(
        f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, "
        "track_id, title, metadata_fp, media_fp, payload, available, first_seen_at, "
        "last_seen_at) VALUES (%s, %s, %s, %s, 'meta', %s, '{}'::jsonb, %s, now(), now())",
        (source, generation, track_id, track_id, media_fp, available),
    )


def _profile(cur, table_name, source, track_id, media_signature, **extra):
    columns = {
        "catalog_instance_id": source, "track_id": track_id, "sample_rate": 48000,
        "duration_ms": 1234, "ref_lufs": -12.5, "start_ramp": b"head",
        "end_ramp": b"tail", "analyzer_ver": 1, "profile_schema_ver": 1,
        "media_signature": media_signature, "analyzed_at": "2026-09-01 00:00:00",
        **extra,
    }
    cur.execute(
        f"INSERT INTO {P}{table_name} ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))})",
        tuple(columns.values()),
    )


def _edge(cur, source, track_id, media_signature="sig"):
    cur.execute(
        f"INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision, "
        "representation_id, media_signature, profile_digest, payload) "
        "VALUES (%s, %s, 'rev', 'rep', %s, 'digest', '{}'::jsonb)",
        (source, track_id, media_signature),
    )
    cur.execute(
        f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, media_revision, "
        "job_token, status) VALUES (%s, %s, 'rev', 'job', 'ready')",
        (source, track_id),
    )


SNAPSHOT_TABLES = {
    "source_profiles": "catalog_instance_id, track_id",
    "published_source_profiles": "catalog_instance_id, track_id",
    "edge_profiles": "catalog_instance_id, track_id",
    "edge_profile_jobs": "catalog_instance_id, track_id",
    "profile_changes": "catalog_instance_id, epoch, seq",
    "profile_stream_state": "catalog_instance_id",
    "catalog_tracks": "catalog_instance_id, published_generation, track_id",
}


def _snapshot(cur):
    snapshot = {}
    for name, order in SNAPSHOT_TABLES.items():
        cur.execute(f"SELECT * FROM {P}{name} ORDER BY {order}")
        snapshot[name] = [
            tuple(bytes(value) if isinstance(value, memoryview) else value for value in row)
            for row in cur.fetchall()
        ]
    return snapshot


def _compare_with_oracle(db, changes, *, full_reconcile=False, generation=GENERATION):
    """Run the oracle and the implementation on the same uncommitted state."""
    with db.cursor() as cur:
        cur.execute("SAVEPOINT oracle")
        expected = per_track_invalidation(
            cur, SOURCE, generation, changes, full_reconcile=full_reconcile
        )
        expected_rows = _snapshot(cur)
        cur.execute("ROLLBACK TO SAVEPOINT oracle")
        actual = profile_publication.invalidate_catalog_changes(
            cur, SOURCE, generation, changes, full_reconcile=full_reconcile
        )
        actual_rows = _snapshot(cur)
    db.rollback()
    return expected, expected_rows, actual, actual_rows


# Every combination the per-track rule distinguishes.
CATALOG_STATES = ("absent", "current", "blank", "null", "unavailable")
ATTEMPT_SIGNATURES = (None, "", "catalog-media:new", "catalog-media:old", "new")
STORED_SIGNATURES = (None, "", "catalog-media:new", "old")
SOURCE_STATES = ("absent",) + tuple(itertools.product(ATTEMPT_SIGNATURES, STORED_SIGNATURES))
PUBLISHED_STATES = ("absent", None, "", "catalog-media:new", "catalog-media:old", "new", "old")
OPERATIONS = (None, "upsert", "delete")


def _combinatorial_fixture(db):
    """One track per combination, plus a second source that must stay untouched."""
    changes = [("album", "album-1", "upsert", None, False)]
    with db.cursor() as cur:
        _source(cur, SOURCE, "server-a")
        _source(cur, OTHER, "server-b")
        combinations = itertools.product(
            CATALOG_STATES, SOURCE_STATES, PUBLISHED_STATES, OPERATIONS
        )
        # Case and punctuation differ in C and linguistic collations: the
        # delete events must still follow code-point (Python) order.
        prefixes = ("t", "T", "a_", "Z", "é")
        for index, (state, attempt, published, operation) in enumerate(combinations):
            track_id = f"{prefixes[index % len(prefixes)]}{index:04d}"
            for source in (SOURCE, OTHER):
                # The previous generation has a row for every track, so a
                # lookup that ignored the generation would find it.
                _track(cur, source, track_id, "stale-generation", generation=GENERATION - 1)
                if state != "absent":
                    media_fp = {"current": "new", "blank": "", "null": None}.get(state, "new")
                    _track(cur, source, track_id, media_fp, available=state != "unavailable")
                if attempt != "absent":
                    attempt_signature, stored = attempt
                    _profile(
                        cur, "source_profiles", source, track_id, stored,
                        status="pending" if attempt_signature is not None else "ready",
                        attempt_token=None if attempt_signature is None else f"token-{index}",
                        attempt_media_signature=attempt_signature,
                        attempt_catalog_epoch="epoch-a",
                    )
                if published != "absent":
                    _profile(cur, "published_source_profiles", source, track_id, published)
                    if index % 2:
                        _edge(cur, source, track_id)
            if operation:
                changes.append(("track", track_id, operation, None, False))
                # An artist sharing a track's ID is not a track change.
                changes.append(("artist", track_id, "delete", None, False))
        # The last operation for a track wins, as in a dict.
        changes.append(("track", "t0000", "upsert", None, False))
        changes.append(("track", "t0000", "delete", None, False))
        for source in (SOURCE, OTHER):
            cur.execute(
                f"INSERT INTO {P}profile_stream_state (catalog_instance_id, epoch, head_seq, "
                "floor_seq) VALUES (%s, 'profile-epoch', 7, 0)",
                (source,),
            )
    db.commit()
    return changes


class CountingCursor(psycopg2.extensions.cursor):
    statements = 0

    def execute(self, query, vars=None):
        CountingCursor.statements += 1
        return super().execute(query, vars)


def _published_tracks(db, count, *, source=SOURCE):
    """``count`` published tracks whose media changed in the current generation."""
    with db.cursor() as cur:
        _source(cur, source, "server-a")
        for index in range(count):
            track_id = f"track-{index:03d}"
            _track(cur, source, track_id, "new")
            _profile(cur, "source_profiles", source, track_id, "catalog-media:old",
                     status="ready")
            _profile(cur, "published_source_profiles", source, track_id,
                     "catalog-media:old")
            _edge(cur, source, track_id)
    db.commit()
    return [("track", f"track-{index:03d}", "upsert", None, False) for index in range(count)]


# ---------------------------------------------------------------------------
# invalidate_catalog_changes
# ---------------------------------------------------------------------------
def test_invalidation_runs_a_fixed_number_of_statements(migrated_db):
    """Red before P2-3: three or more round trips per changed track."""
    changes = _published_tracks(migrated_db, 60)
    counts = {}
    for size in (6, 60):
        with migrated_db.cursor(cursor_factory=CountingCursor) as cur:
            CountingCursor.statements = 0
            withdrawn = profile_publication.invalidate_catalog_changes(
                cur, SOURCE, GENERATION, changes[:size]
            )
            counts[size] = CountingCursor.statements
        migrated_db.rollback()
        assert withdrawn == size
    assert counts[6] == counts[60] <= 12, counts


@pytest.mark.parametrize("full_reconcile", [False, True])
def test_set_based_invalidation_matches_the_per_track_oracle(migrated_db, full_reconcile):
    changes = _combinatorial_fixture(migrated_db)
    expected, expected_rows, actual, actual_rows = _compare_with_oracle(
        migrated_db, changes, full_reconcile=full_reconcile
    )
    # The fixture exercises every branch: withdrawals, stale attempts and
    # rows that must stay.
    assert 0 < expected < len(changes)
    assert actual == expected
    for name in SNAPSHOT_TABLES:
        assert actual_rows[name] == expected_rows[name], name
    events = [row for row in actual_rows["profile_changes"] if row[0] == SOURCE]
    assert [row[2] for row in events] == list(range(8, 8 + expected))
    assert [row[3] for row in events] == sorted(row[3] for row in events)


@pytest.mark.parametrize("scope", ["listed", "source"])
def test_edge_purge_keeps_an_edge_that_is_current_again(migrated_db, scope):
    """The purge deletes only edges no published waveform row of the same
    signature reaches: for the listed tracks, or for the whole source (the
    sweep). Another source's edges stay."""
    _published_tracks(migrated_db, 3)
    with migrated_db.cursor() as cur:
        _source(cur, OTHER, "server-b")
        _edge(cur, OTHER, "track-000")
        cur.execute(f"UPDATE {P}edge_profiles SET media_signature='catalog-media:old' "
                    "WHERE catalog_instance_id=%s AND track_id='track-001'", (SOURCE,))
        cur.execute(f"DELETE FROM {P}published_source_profiles WHERE track_id='track-002'")
        track_ids = ["track-000", "track-001", "track-002"] if scope == "listed" else None
        assert profile_publication.purge_withdrawn_edges(cur, SOURCE, track_ids) == 2
        cur.execute(f"SELECT catalog_instance_id, track_id FROM {P}edge_profiles ORDER BY 1, 2")
        assert cur.fetchall() == [(SOURCE, "track-001"), (OTHER, "track-000")]
        assert profile_publication.purge_withdrawn_edges(cur, SOURCE, []) == 0
        assert profile_publication.purge_withdrawn_edges(cur, SOURCE) == 0
    migrated_db.rollback()


def test_empty_and_unpublished_changes_touch_nothing(migrated_db):
    changes = _published_tracks(migrated_db, 2)
    with migrated_db.cursor() as cur:
        cur.execute(f"DELETE FROM {P}profile_stream_state")
        cur.execute(f"DELETE FROM {P}published_source_profiles")
    migrated_db.commit()
    for batch in ([], [("album", "album-1", "delete", None, False)], changes):
        expected, expected_rows, actual, actual_rows = _compare_with_oracle(migrated_db, batch)
        assert actual == expected == 0
        assert actual_rows == expected_rows
        # No withdrawal: the profile stream is neither created nor locked.
        assert actual_rows["profile_stream_state"] == []


@pytest.mark.parametrize(
    "floor, hold, retention, max_advance",
    [
        (0, None, 10, 3),     # floor far behind: bounded catch-up per event
        (100, 150, 100, 3),   # a live bootstrap session holds the floor
        (100, 120, 100, 50),  # the hold is reached within the batch
        (0, None, 1_000, 5_000),  # nothing expires
    ],
)
def test_bulk_withdrawal_compacts_like_one_publication_per_event(
    migrated_db, monkeypatch, floor, hold, retention, max_advance,
):
    monkeypatch.setattr(catalog, "MIN_RETAINED_CHANGE_EVENTS", 1)
    monkeypatch.setattr(catalog_enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", retention)
    monkeypatch.setattr(catalog_enrichment, "PROFILE_COMPACTION_MAX_ADVANCE", max_advance)
    changes = _published_tracks(migrated_db, 40)
    with migrated_db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}profile_stream_state (catalog_instance_id, epoch, head_seq, "
            "floor_seq, retention_limit) VALUES (%s, 'profile-epoch', 300, %s, %s)",
            (SOURCE, floor, retention),
        )
        cur.execute(
            f"INSERT INTO {P}profile_changes (catalog_instance_id, epoch, seq, track_id, "
            "operation, writer_generation) "
            "SELECT %s, 'profile-epoch', n, 'old-' || n, 'delete', 2 "
            "FROM generate_series(%s, 300) AS n",
            (SOURCE, floor + 1),
        )
        if hold is not None:
            cur.execute(
                f"""INSERT INTO {P}profile_bootstrap_sessions
                    (session_id, token_hash, signing_secret, source_scope,
                     catalog_instance_id, core_server_id, catalog_epoch, profile_epoch,
                     schema_version, page_size, snapshot_seq, head_seq, snapshot_count,
                     expires_at)
                    VALUES (gen_random_uuid(), 'hash', 'secret', %s, %s, 'server-a',
                            'epoch-a', 'profile-epoch', 1, 50, %s, NULL, 0,
                            now() + interval '1 hour')""",
                (SOURCE, SOURCE, hold),
            )
    migrated_db.commit()
    expected, expected_rows, actual, actual_rows = _compare_with_oracle(migrated_db, changes)
    assert actual == expected == 40
    assert actual_rows == expected_rows
    state = actual_rows["profile_stream_state"][0]
    assert state[2] == 340
    if hold is None and retention == 10:
        assert state[3] == floor + 40 * max_advance  # the bound, not the target
    elif hold is not None:
        assert state[3] == min(hold, floor + 40 * max_advance)
    else:
        assert state[3] == floor


def test_bulk_withdrawal_waits_for_the_profile_stream_head(migrated_db, second_connection):
    """record_profile_deletions appends under the profile_stream_state row
    lock, as record_profile_change does. A publisher that holds the head makes
    it wait on that row, and the seqs stay dense and unique afterwards; without
    the lock it would take the publisher's seq (UniqueViolation)."""
    with migrated_db.cursor() as cur:
        _source(cur, SOURCE, "server-a")
        cur.execute(
            f"INSERT INTO {P}profile_stream_state (catalog_instance_id, epoch, head_seq, "
            "floor_seq) VALUES (%s, 'profile-epoch', 7, 0)",
            (SOURCE,),
        )
    migrated_db.commit()
    with second_connection.cursor() as cur:
        cur.execute("SET lock_timeout='10s'")
        cur.execute("SET statement_timeout='20s'")
    second_connection.commit()
    publisher_pid = migrated_db.get_backend_pid()
    withdrawer_pid = second_connection.get_backend_pid()
    # An ordinary publication parks inside its transaction, holding the head.
    with migrated_db.cursor() as cur:
        assert record_profile_change(
            cur, SOURCE, "published", "ready", {"track_id": "published"}
        ) == 8
    outcome = {}

    def withdraw():
        try:
            with second_connection.cursor() as cur:
                outcome["head"] = catalog_enrichment.record_profile_deletions(
                    cur, SOURCE, ["gone-a", "gone-b", "gone-c"]
                )
            second_connection.commit()
        except Exception as exc:
            second_connection.rollback()
            outcome["error"] = exc

    worker = threading.Thread(target=withdraw)
    worker.start()
    try:
        query, blockers = _waiting_statement(withdrawer_pid)
    finally:
        migrated_db.commit()
        worker.join(30)
    assert not worker.is_alive()
    # It waited on the stream-state row, behind the publisher, before
    # appending anything.
    assert f"{P}profile_stream_state" in query and "profile_changes" not in query, query
    assert publisher_pid in blockers
    assert "error" not in outcome, outcome
    assert outcome["head"] == 11
    assert _row(migrated_db, f"SELECT seq, track_id, operation FROM {P}profile_changes "
                "WHERE catalog_instance_id=%s ORDER BY seq", (SOURCE,)) == [
        (8, "published", "upsert"),
        (9, "gone-a", "delete"),
        (10, "gone-b", "delete"),
        (11, "gone-c", "delete"),
    ]
    assert _row(migrated_db, f"SELECT head_seq FROM {P}profile_stream_state "
                "WHERE catalog_instance_id=%s", (SOURCE,)) == [(11,)]


# ---------------------------------------------------------------------------
# Concurrency helpers
# ---------------------------------------------------------------------------
def _schema(db):
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    db.commit()
    return schema


LOCK_TIMEOUT = "15s"


def _connection(schema, lock_timeout=LOCK_TIMEOUT):
    """Another connection to the test schema, closed after the test."""
    other = connect(schema)
    _EXTRA_CONNECTIONS.append(other)
    if lock_timeout:
        with other.cursor() as cur:
            cur.execute(f"SET lock_timeout='{lock_timeout}'")
        other.commit()
    return other


_EXTRA_CONNECTIONS = []


@pytest.fixture
def bounded(migrated_db, second_connection):
    """Lock waits of the test's own connections fail after LOCK_TIMEOUT, so a
    regression that blocks fails the test instead of hanging the module."""
    for connection in (migrated_db, second_connection):
        with connection.cursor() as cur:
            cur.execute(f"SET lock_timeout='{LOCK_TIMEOUT}'")
        connection.commit()


@pytest.fixture(autouse=True)
def _close_extra_connections(migrated_db):
    # Depends on migrated_db, so these close before its schema is dropped.
    yield
    while _EXTRA_CONNECTIONS:
        connection = _EXTRA_CONNECTIONS.pop()
        try:
            connection.rollback()
            connection.close()
        except Exception:
            pass


def _wait_until_waiting(pid, timeout=20, event=None):
    """Wait until backend ``pid`` waits for a heavyweight lock (of ``event``,
    e.g. ``advisory``, when given)."""
    watcher = connect("public")
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with watcher.cursor() as cur:
                cur.execute(
                    "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid=%s",
                    (pid,),
                )
                row = cur.fetchone()
            watcher.rollback()
            if row and row[0] == "Lock" and event in (None, row[1]):
                return
            time.sleep(0.02)
        raise AssertionError(f"backend {pid} never waited for a lock")
    finally:
        watcher.close()


def _waiting_statement(pid, timeout=10):
    """Once backend ``pid`` waits for a lock (a not-granted row in pg_locks),
    return its statement and the backends blocking it."""
    watcher = connect("public")
    watcher.autocommit = True
    try:
        deadline = time.monotonic() + timeout
        with watcher.cursor() as cur:
            while time.monotonic() < deadline:
                cur.execute("SELECT count(*) FROM pg_locks WHERE pid=%s AND NOT granted", (pid,))
                if cur.fetchone()[0]:
                    cur.execute(
                        "SELECT query, pg_blocking_pids(pid) FROM pg_stat_activity WHERE pid=%s",
                        (pid,),
                    )
                    query, blockers = cur.fetchone()
                    return " ".join(query.split()), list(blockers)
                time.sleep(0.02)
        raise AssertionError(f"backend {pid} never waited for a lock")
    finally:
        watcher.close()


def _result():
    return SimpleNamespace(
        sample_rate=48000, duration_ms=1234, ref_lufs=-12.5,
        start_ramp_blob=b"new-wave", end_ramp_blob=b"tail",
    )


def _published_signature(db, source, track_id):
    rows = _row(db, f"SELECT media_signature FROM {P}published_source_profiles "
                "WHERE catalog_instance_id=%s AND track_id=%s", (source, track_id))
    return rows[0][0] if rows else None


def _last_event(db, source, track_id):
    rows = _row(db, f"SELECT operation FROM {P}profile_changes "
                "WHERE catalog_instance_id=%s AND track_id=%s ORDER BY seq DESC LIMIT 1",
                (source, track_id))
    return rows[0][0] if rows else None


# ---------------------------------------------------------------------------
# refresh_catalog: what runs while catalog_state is held
# ---------------------------------------------------------------------------
class ProbedDb:
    """Connection proxy: probes the catalog_state row before each statement.

    ``held`` lists the statements run while another connection could not lock
    the row (``FOR UPDATE NOWAIT``), i.e. while an admission would wait.
    ``before``/``after`` hooks run around statements that match a predicate.
    """

    def __init__(self, db, probe, source_ref):
        self._db = db
        self._probe = probe
        self._source_ref = source_ref
        self.held = []
        self.hooks = []

    def is_held(self):
        source = self._source_ref()
        if source is None:
            return False
        with self._probe.cursor() as cur:
            try:
                cur.execute(
                    f"SELECT 1 FROM {P}catalog_state WHERE catalog_instance_id=%s "
                    "FOR UPDATE NOWAIT",
                    (source,),
                )
                held = False
            except errors.LockNotAvailable:
                held = True
        self._probe.rollback()
        return held

    def cursor(self, *args, **kwargs):
        return ProbedCursor(self._db.cursor(*args, **kwargs), self)

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()

    def __getattr__(self, name):
        return getattr(self._db, name)


class ProbedCursor:
    def __init__(self, cur, owner):
        self._cur = cur
        self._owner = owner

    def execute(self, sql, params=None):
        text = sql if isinstance(sql, str) else sql.decode()
        for when, predicate, action in list(self._owner.hooks):
            if when == "before" and predicate(text):
                self._owner.hooks.remove((when, predicate, action))
                action()
        if self._owner.is_held():
            self._owner.held.append(" ".join(text.split()))
        result = self._cur.execute(sql, params)
        for when, predicate, action in list(self._owner.hooks):
            if when == "after" and predicate(text):
                self._owner.hooks.remove((when, predicate, action))
                action()
        return result

    def executemany(self, sql, params_list):
        for params in params_list:
            self.execute(sql, params)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._cur.close()

    def __getattr__(self, name):
        return getattr(self._cur, name)


def _is_publication_lock(sql):
    return (
        "catalog_state" in sql and "FOR UPDATE" in sql
        and "fingerprint_schema_version" in sql and "entity_counts" not in sql
    )


def _catalogue(count, changed=(), removed=()):
    return {"tracks": [
        {"id": f"track-{index:03d}", "title": f"Song {index}", "duration": 100 + index,
         "size": 1_000 + index + (500 if f"track-{index:03d}" in changed else 0)}
        for index in range(count) if f"track-{index:03d}" not in removed
    ]}


def _publish_baseline(db, count, unpublished=()):
    """Generation 1 with ``count`` tracks, each published at its revision with
    an edge, except ``unpublished`` tracks, which have no profile rows."""
    first = catalog.refresh_catalog("server-a", db=db, bridge=RefreshBridge(_catalogue(count)))
    source = first["catalog_instance_id"]
    with db.cursor() as cur:
        cur.execute(
            f"SELECT track_id, media_fp FROM {P}catalog_tracks "
            "WHERE catalog_instance_id=%s AND published_generation=1 ORDER BY track_id",
            (source,),
        )
        for track_id, media_fp in cur.fetchall():
            if track_id in unpublished:
                continue
            revision = f"catalog-media:{media_fp}"
            _profile(cur, "source_profiles", source, track_id, revision, status="ready")
            _profile(cur, "published_source_profiles", source, track_id, revision)
            _edge(cur, source, track_id, revision)
    db.commit()
    return source


def _generation_revision(db, source, track_id, generation=1):
    rows = _row(db, f"SELECT media_fp FROM {P}catalog_tracks WHERE catalog_instance_id=%s "
                "AND published_generation=%s AND track_id=%s", (source, generation, track_id))
    return f"catalog-media:{rows[0][0]}"


def _events(db, source, track_id):
    return [row[0] for row in _row(
        db, f"SELECT operation FROM {P}profile_changes "
        "WHERE catalog_instance_id=%s AND track_id=%s ORDER BY seq", (source, track_id))]


def _edge_tracks(db, source):
    return [row[0] for row in _row(
        db, f"SELECT track_id FROM {P}edge_profiles WHERE catalog_instance_id=%s "
        "ORDER BY track_id", (source,))]


def _row(db, sql, params):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def test_catalog_state_is_held_only_to_publish_and_withdraw(migrated_db, second_connection):
    """Red before P2-3: the generation rows, the catalogue journal and the
    per-track invalidation all ran under the catalog_state row lock."""
    source = _publish_baseline(migrated_db, 40)
    changed = {f"track-{index:03d}" for index in range(30)}
    probed = ProbedDb(migrated_db, second_connection, lambda: source)
    result = catalog.refresh_catalog(
        "server-a", db=probed, bridge=RefreshBridge(_catalogue(40, changed=changed))
    )
    assert result["generation"] == 2 and result["changes"] == 30
    assert _row(migrated_db, f"SELECT count(*) FROM {P}published_source_profiles "
                "WHERE catalog_instance_id=%s", (source,)) == [(10,)]
    assert _row(migrated_db, f"SELECT count(*) FROM {P}profile_changes "
                "WHERE catalog_instance_id=%s AND operation='delete'", (source,)) == [(30,)]
    held = probed.held
    # Neither the new generation nor its journal is written, and no old
    # generation is deleted, while the row is held.
    assert not [
        sql for sql in held
        if sql.startswith(("INSERT INTO", "DELETE FROM"))
        and any(name in sql for name in catalog.CATALOG_GENERATION_TABLES)
    ], held
    assert not [sql for sql in held if sql.startswith(f"INSERT INTO {P}catalog_changes")], held
    # Nor are withdrawn edge payloads deleted, or the generation read.
    assert not [sql for sql in held if f"{P}edge_profiles" in sql], held
    assert not [sql for sql in held if f"{P}catalog_tracks" in sql], held
    assert len(held) <= 16, held
    # Both still happen, after the commit.
    assert _row(migrated_db, f"SELECT DISTINCT published_generation FROM {P}catalog_tracks "
                "WHERE catalog_instance_id=%s", (source,)) == [(2,)]
    assert _row(migrated_db, f"SELECT track_id FROM {P}edge_profiles "
                "WHERE catalog_instance_id=%s ORDER BY track_id", (source,)) == [
        (f"track-{index:03d}",) for index in range(30, 40)
    ]


def test_no_change_refresh_holds_catalog_state_briefly(migrated_db, second_connection):
    source = _publish_baseline(migrated_db, 40)
    probed = ProbedDb(migrated_db, second_connection, lambda: source)
    result = catalog.refresh_catalog(
        "server-a", db=probed, bridge=RefreshBridge(_catalogue(40))
    )
    assert result["change_reason"] == "no_change" and result["generation"] == 1
    assert len(probed.held) <= 6, probed.held


def test_a_publisher_waits_for_another_without_holding_catalog_state(
    migrated_db, second_connection, bounded,
):
    schema = _schema(migrated_db)
    source = _publish_baseline(migrated_db, 5)
    probed = ProbedDb(migrated_db, _connection(schema), lambda: source)
    admission = _connection(schema, lock_timeout="2s")
    with second_connection.cursor() as cur:
        catalog._lock_catalog_publication(cur, source)
    outcome = {}

    def publish():
        try:
            outcome["result"] = catalog.refresh_catalog(
                "server-a", db=probed,
                bridge=RefreshBridge(_catalogue(5, changed={"track-001"})),
            )
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    publisher_pid = migrated_db.get_backend_pid()
    worker = threading.Thread(target=publish)
    worker.start()
    try:
        _wait_until_waiting(publisher_pid)
        # The waiting publisher holds no catalog_state lock: an admission
        # proceeds meanwhile.
        tokens = profile_publication.admit_attempts(admission, source, ["track-002"])
        assert set(tokens) == {"track-002"}
    finally:
        second_connection.commit()
        worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome
    assert outcome["result"]["generation"] == 2


@pytest.mark.parametrize("second", ["same_catalogue", "one_more_change"])
def test_a_refresh_started_during_another_build_diffs_against_its_publication(
    migrated_db, second_connection, bounded, second,
):
    """Refresh B starts while A builds, waits for the publisher lock, then
    diffs against the generation A published: a no-op or exactly its own
    delta. Neither fails as "moved" nor leaves refresh_required set."""
    schema = _schema(migrated_db)
    source = _publish_baseline(migrated_db, 5)
    first_catalogue = _catalogue(5, changed={"track-001"})
    second_catalogue = (
        first_catalogue if second == "same_catalogue"
        else _catalogue(5, changed={"track-001", "track-003"})
    )
    other = _connection(schema)
    other_pid = other.get_backend_pid()
    outcome = {}

    def refresh_second():
        try:
            outcome["result"] = catalog.refresh_catalog(
                "server-a", db=other, bridge=RefreshBridge(second_catalogue)
            )
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    worker = threading.Thread(target=refresh_second)

    def start_second():
        # A has built generation 2 and holds the publisher lock; B scans,
        # fetches and then waits for that lock.
        worker.start()
        _wait_until_waiting(other_pid, event="advisory")

    probed = ProbedDb(migrated_db, _connection(schema), lambda: None)
    probed.hooks.append(("before", _is_publication_lock, start_second))
    first = catalog.refresh_catalog(
        "server-a", db=probed, bridge=RefreshBridge(first_catalogue)
    )
    worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome
    result = outcome["result"]
    assert (first["generation"], first["changes"]) == (2, 1)
    if second == "same_catalogue":
        assert (result["generation"], result["change_reason"]) == (2, "no_change")
        withdrawn = ["track-001"]
    else:
        assert (result["generation"], result["changes"]) == (3, 1)
        assert result["change_counts"]["by_entity"]["track"]["upserts"] == 1
        withdrawn = ["track-001", "track-003"]
    assert _row(migrated_db, f"SELECT published_generation, status, refresh_required, "
                f"refresh_reason, last_error FROM {P}catalog_state "
                "WHERE catalog_instance_id=%s", (source,)) == [
        (result["generation"], "complete", False, None, None)
    ]
    assert _row(migrated_db, f"SELECT DISTINCT status FROM {P}catalog_scans "
                "WHERE catalog_instance_id=%s", (source,)) == [("complete",)]
    for index in range(5):
        track = f"track-{index:03d}"
        published = _published_signature(migrated_db, source, track)
        assert (published is None) is (track in withdrawn), track
        assert _events(migrated_db, source, track) == (["delete"] if track in withdrawn else [])
    assert _edge_tracks(migrated_db, source) == [
        f"track-{index:03d}" for index in range(5) if f"track-{index:03d}" not in withdrawn
    ]


def test_state_moved_under_the_row_lock_publishes_nothing(
    migrated_db, second_connection, bounded,
):
    """The row lock re-checks the base. A catalog_state change the publisher
    lock does not cover (here the epoch, from another connection just before
    the lock) fails the refresh and rolls the whole build back."""
    schema = _schema(migrated_db)
    source = _publish_baseline(migrated_db, 3)
    with migrated_db.cursor() as cur:
        before = _snapshot(cur)
        cur.execute(f"SELECT * FROM {P}catalog_changes ORDER BY catalog_instance_id, epoch, seq")
        journal = cur.fetchall()
    migrated_db.commit()

    def move_epoch():
        with second_connection.cursor() as cur:
            cur.execute(
                f"UPDATE {P}catalog_state SET catalog_epoch='epoch-moved' "
                "WHERE catalog_instance_id=%s",
                (source,),
            )
        second_connection.commit()

    probed = ProbedDb(migrated_db, _connection(schema), lambda: None)
    probed.hooks.append(("before", _is_publication_lock, move_epoch))
    with pytest.raises(catalog.CatalogScanError, match="moved"):
        catalog.refresh_catalog(
            "server-a", db=probed,
            bridge=RefreshBridge(_catalogue(3, changed={"track-001"}, removed={"track-002"})),
        )
    with migrated_db.cursor() as cur:
        assert _snapshot(cur) == before  # profiles, edges, profile journal, tracks
        cur.execute(f"SELECT * FROM {P}catalog_changes ORDER BY catalog_instance_id, epoch, seq")
        assert cur.fetchall() == journal
        for table_name in catalog.CATALOG_GENERATION_TABLES:
            cur.execute(f"SELECT count(*) FROM {P}{table_name} WHERE published_generation<>1")
            assert cur.fetchone() == (0,), table_name
        cur.execute(
            f"SELECT published_generation, catalog_epoch, refresh_required, last_error "
            f"FROM {P}catalog_state WHERE catalog_instance_id=%s",
            (source,),
        )
        generation, epoch, refresh_required, last_error = cur.fetchone()
    migrated_db.commit()
    assert (generation, epoch, refresh_required) == (1, "epoch-moved", True)
    assert "moved" in last_error


def test_edges_left_by_a_failed_purge_go_with_maintenance_or_the_next_publication(
    migrated_db, monkeypatch,
):
    """The edge purge runs after the publication commits, best effort. When it
    fails, the withdrawn tracks' edges stay until maintenance
    (compact_enrichment_storage) or the next publication sweeps the source."""
    source = _publish_baseline(migrated_db, 6)

    def fail(*_args, **_kwargs):
        raise RuntimeError("purge interrupted")

    monkeypatch.setattr(profile_publication, "purge_withdrawn_edges", fail)
    catalog.refresh_catalog(
        "server-a", db=migrated_db,
        bridge=RefreshBridge(_catalogue(6, changed={"track-001"}, removed={"track-002"})),
    )
    assert _published_signature(migrated_db, source, "track-001") is None
    assert _published_signature(migrated_db, source, "track-002") is None
    assert len(_edge_tracks(migrated_db, source)) == 6  # two unreachable
    # The prune of superseded generations still ran.
    assert _row(migrated_db, f"SELECT DISTINCT published_generation FROM {P}catalog_tracks "
                "WHERE catalog_instance_id=%s", (source,)) == [(2,)]

    monkeypatch.undo()
    catalog_enrichment.compact_enrichment_storage(migrated_db, source)
    migrated_db.commit()
    assert _edge_tracks(migrated_db, source) == [
        "track-000", "track-003", "track-004", "track-005",
    ]

    # The purge fails again; then a publication that withdraws no profile
    # itself sweeps the leftover edge.
    monkeypatch.setattr(profile_publication, "purge_withdrawn_edges", fail)
    moved = _catalogue(6, changed={"track-001", "track-003"}, removed={"track-002"})
    catalog.refresh_catalog("server-a", db=migrated_db, bridge=RefreshBridge(moved))
    assert "track-003" in _edge_tracks(migrated_db, source)
    monkeypatch.undo()
    added = {"tracks": moved["tracks"] + [{"id": "track-new", "title": "New", "duration": 90}]}
    result = catalog.refresh_catalog("server-a", db=migrated_db, bridge=RefreshBridge(added))
    assert result["changes"] == 1
    assert _edge_tracks(migrated_db, source) == ["track-000", "track-004", "track-005"]


# ---------------------------------------------------------------------------
# LUM-008 fence with the shorter publication
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("interleaving", ["during_build", "while_locked"])
@pytest.mark.parametrize("change", ["media_changed", "removed"])
def test_completion_of_an_old_revision_never_survives_publication(
    migrated_db, second_connection, bounded, interleaving, change,
):
    """A completion for revision A races a publication that changes or removes
    the track. Whichever interleaving, nothing stays published for the old
    revision, the withdrawal is journaled last, and the attempt is fenced."""
    schema = _schema(migrated_db)
    source = _publish_baseline(migrated_db, 3)
    track = "track-001"
    old_revision = _published_signature(migrated_db, source, track)
    token = profile_publication.admit_attempts(second_connection, source, [track])[track]
    probed = ProbedDb(migrated_db, _connection(schema), lambda: source)
    completion = {}

    def complete():
        completion["published"] = profile_publication.complete_attempt(
            second_connection, source, track, token, _result(), "ready", None,
            old_revision, 1, 1,
        )

    if interleaving == "during_build":
        # The new generation is already written (uncommitted) and the row is
        # free: the completion publishes against the previous generation.
        probed.hooks.append(("before", _is_publication_lock, complete))
    else:
        worker = threading.Thread(target=complete)

        def start_blocked_completion():
            worker.start()
            _wait_until_waiting(second_connection.get_backend_pid())

        probed.hooks.append(("after", _is_publication_lock, start_blocked_completion))
    bridge = RefreshBridge(
        _catalogue(3, changed={track}) if change == "media_changed"
        else _catalogue(3, removed={track})
    )
    result = catalog.refresh_catalog("server-a", db=probed, bridge=bridge)
    if interleaving == "while_locked":
        worker.join(30)
        assert not worker.is_alive()
    assert result["generation"] == 2
    assert completion["published"] is (interleaving == "during_build")
    assert _published_signature(migrated_db, source, track) is None
    assert _last_event(migrated_db, source, track) == "delete"
    assert _row(migrated_db, f"SELECT status, attempt_token FROM {P}source_profiles "
                "WHERE catalog_instance_id=%s AND track_id=%s", (source, track)) == [
        ("stale", None)
    ]
    # Untouched tracks keep their publication.
    assert _published_signature(migrated_db, source, "track-000") is not None


@pytest.mark.parametrize("change", ["media_changed", "removed"])
def test_a_track_first_published_during_the_build_is_withdrawn(
    migrated_db, second_connection, bounded, change,
):
    """The track has no attempt or published row when the refresh starts; it
    is admitted and published for the old revision while the new generation
    is built. The withdrawal under the row lock must still find it, so which
    tracks have profile rows cannot be decided before the lock."""
    schema = _schema(migrated_db)
    track = "track-001"
    source = _publish_baseline(migrated_db, 3, unpublished={track})
    assert _row(migrated_db, f"SELECT count(*) FROM {P}source_profiles "
                "WHERE catalog_instance_id=%s AND track_id=%s", (source, track)) == [(0,)]
    assert _published_signature(migrated_db, source, track) is None
    old_revision = _generation_revision(migrated_db, source, track)
    probed = ProbedDb(migrated_db, _connection(schema), lambda: source)
    completion = {}

    def admit_and_complete():
        token = profile_publication.admit_attempts(second_connection, source, [track])[track]
        completion["published"] = profile_publication.complete_attempt(
            second_connection, source, track, token, _result(), "ready", None,
            old_revision, 1, 1,
        )

    probed.hooks.append(("before", _is_publication_lock, admit_and_complete))
    bridge = RefreshBridge(
        _catalogue(3, changed={track}) if change == "media_changed"
        else _catalogue(3, removed={track})
    )
    result = catalog.refresh_catalog("server-a", db=probed, bridge=bridge)
    assert result["generation"] == 2
    assert completion["published"] is True
    assert _published_signature(migrated_db, source, track) is None
    assert _events(migrated_db, source, track) == ["upsert", "delete"]
    assert _row(migrated_db, f"SELECT status, attempt_token FROM {P}source_profiles "
                "WHERE catalog_instance_id=%s AND track_id=%s", (source, track)) == [
        ("stale", None)
    ]
    assert _published_signature(migrated_db, source, "track-000") is not None


def test_admission_during_the_build_is_fenced_by_the_publication(
    migrated_db, second_connection, bounded,
):
    schema = _schema(migrated_db)
    source = _publish_baseline(migrated_db, 3)
    track = "track-002"
    old_revision = _published_signature(migrated_db, source, track)
    probed = ProbedDb(migrated_db, _connection(schema), lambda: source)
    admitted = {}

    def admit():
        admitted.update(profile_publication.admit_attempts(second_connection, source, [track]))

    probed.hooks.append(("before", _is_publication_lock, admit))
    catalog.refresh_catalog(
        "server-a", db=probed, bridge=RefreshBridge(_catalogue(3, changed={track}))
    )
    assert track in admitted
    assert not profile_publication.complete_attempt(
        second_connection, source, track, admitted[track], _result(), "ready", None,
        old_revision, 1, 1,
    )
    assert _published_signature(migrated_db, source, track) is None
    assert _last_event(migrated_db, source, track) == "delete"


# ---------------------------------------------------------------------------
# Admission and completion keep FOR UPDATE on catalog_state
# ---------------------------------------------------------------------------
def test_admission_batch_and_completion_serialize_without_deadlock(
    migrated_db, second_connection, bounded,
):
    """The interleaving that deadlocks if admission and completion only share
    catalog_state (FOR SHARE): completion C holds track Y's attempt row and is
    about to append to the profile journal; admission A withdraws X (taking
    profile_stream_state) and then needs Y's row. With FOR UPDATE, A waits
    for C on catalog_state before it takes anything, so both succeed."""
    schema = _schema(migrated_db)
    with migrated_db.cursor() as cur:
        _source(cur, SOURCE, "server-a")
        # The journal row exists, so each side takes it with FOR UPDATE.
        catalog_enrichment._profile_stream_state(cur, SOURCE, for_update=True)
        _track(cur, SOURCE, "x-track", "x-new")
        _track(cur, SOURCE, "y-track", "y-current")
        _profile(cur, "published_source_profiles", SOURCE, "x-track", "catalog-media:x-old")
        _profile(cur, "source_profiles", SOURCE, "x-track", "catalog-media:x-old",
                 status="ready")
    migrated_db.commit()
    token = profile_publication.admit_attempts(migrated_db, SOURCE, ["y-track"])["y-track"]
    admission = {}

    def admit():
        try:
            admission["tokens"] = profile_publication.admit_attempts(
                second_connection, SOURCE, ["x-track", "y-track"]
            )
        except Exception as exc:
            admission["error"] = exc

    worker = threading.Thread(target=admit)

    def start_admission():
        worker.start()
        _wait_until_waiting(second_connection.get_backend_pid())

    probed = ProbedDb(migrated_db, _connection(schema), lambda: None)
    probed.hooks.append(
        ("before", lambda sql: "profile_stream_state" in sql, start_admission)
    )
    assert profile_publication.complete_attempt(
        probed, SOURCE, "y-track", token, _result(), "ready", None,
        "catalog-media:y-current", 1, 1,
    )
    worker.join(30)
    assert not worker.is_alive()
    assert "error" not in admission, admission
    assert set(admission["tokens"]) == {"x-track", "y-track"}
    assert _published_signature(migrated_db, SOURCE, "x-track") is None
    assert _published_signature(migrated_db, SOURCE, "y-track") == "catalog-media:y-current"
