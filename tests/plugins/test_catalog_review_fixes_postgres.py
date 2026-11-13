"""Review fixes in catalog.py and catalog_readiness.py, on the migrated schema.

1.  An analysis bootstrap lease is created under the analysis_state row lock,
    so it never pins a generation an uncommitted projection already pruned.
2.  Session creation locks catalog_state only, and a publication updates
    catalog_sources before it locks catalog_state.
3.  A refresh that inspects the provider identity builds its generation
    without the catalog_state row lock; the verdict still commits with it.
4.  The snapshot estimate is measured once per written generation and kept on
    a refresh without changes.
5.  The catalogue journal is written a page per statement and trimmed after
    the publication commits.
6.  ``ensure_catalog_sources`` writes only a changed name or default, and a
    refresh holds no catalog_sources row lock across the provider ping.
7.  ``/changes`` accepts the proven v2 alias ``server_id``.
8.  A catalogue bootstrap track carries no ``search_text``.
9.  A failed readiness read keeps the transaction and what the caller wrote.
10. Shared principals may hold more bootstrap sessions than one user.
11. The post-publication journal compaction and prune_catalog_storage both
    lock catalog_state before journal rows; the compaction gives up when
    catalog_state is busy.
12. An analysis session creation waits for a running projection only
    ANALYSIS_LEASE_LOCK_TIMEOUT, and then answers "not ready".
13. One principal's session creations are serialized for both streams, so
    the per-principal cap holds under concurrency.
"""

import threading
import types

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2 import errors  # noqa: E402

from pg_helpers import connect  # noqa: E402
from test_lumae_analysis import RefreshBridge  # noqa: E402  (installs the plugin.api stub)
from test_catalog_publication_lock_postgres import (  # noqa: E402
    ProbedDb,
    _catalogue,
    _is_publication_lock,
    _row,
    _wait_until_waiting,
)
from plugins.LumaeAnalysis import catalog, catalog_readiness, status_model  # noqa: E402


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
LOCK_TIMEOUT = "15s"


@pytest.fixture
def bounded(migrated_db, second_connection):
    """A lock wait of the test's own connections fails instead of hanging."""
    for connection in (migrated_db, second_connection):
        with connection.cursor() as cur:
            cur.execute(f"SET lock_timeout='{LOCK_TIMEOUT}'")
        connection.commit()


@pytest.fixture
def third_connection(migrated_db):
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    migrated_db.commit()
    other = connect(schema)
    try:
        with other.cursor() as cur:
            cur.execute(f"SET lock_timeout='{LOCK_TIMEOUT}'")
        other.commit()
        yield other
    finally:
        other.rollback()
        other.close()


def _seed_source(db, *, analysis_generation=0, continuity_from=None, source=SOURCE,
                 server="server-a", default=True):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
            "provider_type, server_name, is_default, rebind_status, continuity_from) "
            "VALUES (%s, %s, 'navidrome', 'A', %s, 'active', %s)",
            (source, server, default, continuity_from),
        )
        cur.execute(
            f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, "
            "provider_type, published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, %s, 'navidrome', 1, 'catalog-epoch', 'complete', "
            "        '{\"track\": 1}'::jsonb)",
            (source, server),
        )
        cur.execute(
            f"INSERT INTO {P}analysis_state (catalog_instance_id, projection_generation, "
            "analysis_epoch, analysis_head_seq, status, item_count, mapped_track_count) "
            "VALUES (%s, %s, 'analysis-epoch', 5, 'complete', 1, 1)",
            (source, analysis_generation),
        )
    db.commit()


def _analysis_generation(cur, generation, analysis_id):
    cur.execute(
        f"INSERT INTO {P}analysis_items (catalog_instance_id, projection_generation, "
        "analysis_id) VALUES (%s, %s, %s)",
        (SOURCE, generation, analysis_id),
    )
    cur.execute(
        f"INSERT INTO {P}track_analysis_links (catalog_instance_id, projection_generation, "
        "provider_track_id, analysis_id, status) VALUES (%s, %s, 'track-1', %s, 'ready')",
        (SOURCE, generation, analysis_id),
    )


def _all_pages(db, session, principal, stream):
    items, token = [], None
    while True:
        page = catalog.bootstrap_page(
            db, session["session_token"], principal, stream=stream, page_token=token
        )
        items.extend((page["entity_type"], item) for item in page["items"])
        token = page["next_page_token"]
        if page["completed"]:
            return items


# ---------------------------------------------------------------------------
# 1. Analysis lease and the projection's prune
# ---------------------------------------------------------------------------
def test_analysis_lease_waits_for_a_projection_that_already_pruned(
    migrated_db, second_connection, bounded,
):
    """Red before the fix: the lease pinned generation 1 while the projection's
    transaction had deleted its rows, and the bootstrap completed empty."""
    _seed_source(migrated_db, analysis_generation=1)
    with migrated_db.cursor() as cur:
        _analysis_generation(cur, 1, "item-old")
    migrated_db.commit()

    # The projection, as catalog_analysis._project_analysis runs it: the row
    # lock, the new generation, the state, the prune; not yet committed.
    projection = migrated_db.cursor()
    projection.execute(
        f"SELECT projection_generation FROM {P}analysis_state "
        "WHERE catalog_instance_id=%s FOR UPDATE",
        (SOURCE,),
    )
    _analysis_generation(projection, 2, "item-new")
    projection.execute(
        f"UPDATE {P}analysis_state SET projection_generation=2, analysis_head_seq=9 "
        "WHERE catalog_instance_id=%s",
        (SOURCE,),
    )
    catalog.prune_snapshot_generations(projection, SOURCE, "analysis", 2)

    outcome = {}

    def create():
        try:
            outcome["session"] = catalog.create_bootstrap_session(
                second_connection, "user:a", stream="analysis", catalog_instance_id=SOURCE
            )
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    worker = threading.Thread(target=create)
    worker.start()
    try:
        _wait_until_waiting(second_connection.get_backend_pid())
    finally:
        migrated_db.commit()
        worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome
    session = outcome["session"]
    assert (session["generation"], session["snapshot_seq"]) == (2, 9)
    items = _all_pages(second_connection, session, "user:a", "analysis")
    assert [(kind, item.get("analysis_id")) for kind, item in items] == [
        ("item", "item-new"), ("link", "item-new"),
    ]


def test_a_committed_analysis_lease_is_seen_by_the_projections_prune(
    migrated_db, second_connection, bounded,
):
    _seed_source(migrated_db, analysis_generation=1)
    with migrated_db.cursor() as cur:
        _analysis_generation(cur, 1, "item-old")
    migrated_db.commit()
    # While a session creation holds its share lock, a projection cannot
    # take the row; other session creations can.
    with second_connection.cursor() as cur:
        assert catalog._locked_analysis_state(cur, SOURCE)["generation"] == 1
    with migrated_db.cursor() as cur:
        assert catalog._locked_analysis_state(cur, SOURCE)["generation"] == 1
        with pytest.raises(errors.LockNotAvailable):
            cur.execute(
                f"SELECT 1 FROM {P}analysis_state WHERE catalog_instance_id=%s "
                "FOR UPDATE NOWAIT",
                (SOURCE,),
            )
    migrated_db.rollback()
    second_connection.rollback()

    session = catalog.create_bootstrap_session(
        second_connection, "user:a", stream="analysis", catalog_instance_id=SOURCE
    )
    assert session["generation"] == 1 and session["totals"] == {"item": 1, "link": 1}
    with migrated_db.cursor() as cur:
        _analysis_generation(cur, 2, "item-new")
        catalog.prune_snapshot_generations(cur, SOURCE, "analysis", 2)
    migrated_db.commit()
    items = _all_pages(second_connection, session, "user:a", "analysis")
    assert [item.get("analysis_id") for _kind, item in items] == ["item-old", "item-old"]


# ---------------------------------------------------------------------------
# 2. Lock order of session creation and publication
# ---------------------------------------------------------------------------
def test_session_creation_takes_only_its_streams_state_row(
    migrated_db, second_connection, third_connection, bounded,
):
    _seed_source(migrated_db, analysis_generation=1)
    # What an insert referencing the source holds (its foreign key check),
    # and what relationship_build's FOR SHARE takes.
    with second_connection.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {P}catalog_sources WHERE catalog_instance_id=%s FOR SHARE",
            (SOURCE,),
        )
    with third_connection.cursor() as cur:
        cur.execute("SET lock_timeout='2s'")
        # Red before the fix: FOR UPDATE OF s, c waited for that lock.
        session = catalog.create_bootstrap_session(
            third_connection, "user:a", stream="catalog", catalog_instance_id=SOURCE
        )
        assert session["generation"] == 1
    second_connection.rollback()

    # An analysis lease does not take catalog_state: an admission or a
    # publication holding it does not delay the lease, and a lease waiting
    # for a projection would not hold it.
    with second_connection.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {P}catalog_state WHERE catalog_instance_id=%s FOR UPDATE",
            (SOURCE,),
        )
    assert catalog.create_bootstrap_session(
        third_connection, "user:a", stream="analysis", catalog_instance_id=SOURCE
    )["generation"] == 1
    # The catalogue lease still does.
    with pytest.raises(errors.LockNotAvailable):
        catalog.create_bootstrap_session(
            third_connection, "user:a", stream="catalog", catalog_instance_id=SOURCE
        )
    third_connection.rollback()
    second_connection.rollback()


def test_publication_locks_catalog_sources_before_catalog_state(
    migrated_db, second_connection, third_connection, bounded,
):
    """A statement that share-locks the source and then the state rows
    (relationship_build) must not meet a publisher holding them in the other
    order. Red before the fix: the publisher waited for catalog_sources while
    it held catalog_state."""
    first = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(4))
    )
    source = first["catalog_instance_id"]

    def share_lock_the_source():
        with second_connection.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {P}catalog_sources WHERE catalog_instance_id=%s FOR SHARE",
                (source,),
            )

    # The share lock arrives just before the publication writes the source.
    probed = ProbedDb(migrated_db, third_connection, lambda: None)
    probed.hooks.append(
        ("before", lambda sql: "SET provider_instance_fp" in sql, share_lock_the_source)
    )
    outcome = {}

    def publish():
        try:
            outcome["result"] = catalog.refresh_catalog(
                "server-a", db=probed,
                bridge=RefreshBridge(_catalogue(4, changed={"track-001"})),
            )
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    worker = threading.Thread(target=publish)
    worker.start()
    try:
        _wait_until_waiting(migrated_db.get_backend_pid())
        # The publisher waits for catalog_sources and holds no catalog_state
        # lock meanwhile: the share-locker could take it next.
        with third_connection.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {P}catalog_state WHERE catalog_instance_id=%s "
                "FOR UPDATE NOWAIT",
                (source,),
            )
        third_connection.rollback()
    finally:
        second_connection.rollback()
        worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome
    assert outcome["result"]["generation"] == 2
    assert _row(migrated_db, f"SELECT provider_instance_fp IS NOT NULL FROM "
                f"{P}catalog_sources WHERE catalog_instance_id=%s", (source,)) == [(True,)]


# ---------------------------------------------------------------------------
# 3. Identity inspection without the row lock during the build
# ---------------------------------------------------------------------------
class UnansweredProbeBridge(RefreshBridge):
    """The provider does not answer the identity ping: a published catalogue
    is then gated as transition_pending for this refresh, which inspects the
    scanned IDs before it publishes."""

    core = None

    def probe_server_identity(self, _server_id):
        raise RuntimeError("provider did not answer")


@pytest.mark.parametrize("change", ["changed", "no_change"])
def test_identity_inspection_does_not_hold_catalog_state_for_the_build(
    migrated_db, second_connection, monkeypatch, change,
):
    first = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(30))
    )
    source = first["catalog_instance_id"]
    inspections = []
    inspect = catalog.inspect_catalog_identity
    monkeypatch.setattr(
        catalog, "inspect_catalog_identity",
        lambda *args, **kwargs: inspections.append(1) or inspect(*args, **kwargs),
    )
    probed = ProbedDb(migrated_db, second_connection, lambda: source)
    held_at_build = []
    probed.hooks.append(
        ("before", _is_publication_lock, lambda: held_at_build.append(probed.is_held()))
    )
    changed = {f"track-{index:03d}" for index in range(20)} if change == "changed" else set()
    result = catalog.refresh_catalog(
        "server-a", db=probed, bridge=UnansweredProbeBridge(_catalogue(30, changed=changed))
    )
    # Inspected before the build, and again under the row lock.
    assert inspections == [1, 1]
    # Red before the fix: the row was held from the first inspection on.
    assert held_at_build == [False]
    if change == "changed":
        assert (result["generation"], result["changes"]) == (2, 20)
        assert not [
            sql for sql in probed.held
            if sql.startswith(("INSERT INTO", "DELETE FROM"))
            and any(name in sql for name in catalog.CATALOG_GENERATION_TABLES)
        ], probed.held
        assert not [
            sql for sql in probed.held if sql.startswith(f"INSERT INTO {P}catalog_changes")
        ], probed.held
    else:
        assert (result["generation"], result["change_reason"]) == (1, "no_change")
    # The verdict committed with the publication, as before.
    assert _row(
        migrated_db,
        f"SELECT state, detection_reason, baseline_catalog_generation "
        f"FROM {P}provider_identity_transitions WHERE catalog_instance_id=%s",
        (source,),
    ) == [("normal", "provider_ids_unchanged", 1)]


def test_identity_verdict_is_not_kept_when_the_publication_fails(migrated_db, monkeypatch):
    first = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(5))
    )
    source = first["catalog_instance_id"]
    before = _row(
        migrated_db,
        f"SELECT state, detection_reason, checked_at FROM {P}provider_identity_transitions "
        "WHERE catalog_instance_id=%s",
        (source,),
    )

    def fail(*_args, **_kwargs):
        raise RuntimeError("build interrupted")

    monkeypatch.setattr(catalog, "_insert_relationship_rows", fail)
    with pytest.raises(RuntimeError, match="build interrupted"):
        catalog.refresh_catalog(
            "server-a", db=migrated_db,
            bridge=UnansweredProbeBridge(_catalogue(5, changed={"track-001"})),
        )
    after = _row(
        migrated_db,
        f"SELECT state, detection_reason, checked_at FROM {P}provider_identity_transitions "
        "WHERE catalog_instance_id=%s",
        (source,),
    )
    # The rolled-back inspection wrote nothing; only the failed ping's error
    # may have been recorded by the observation.
    assert [row[:2] for row in after] == [row[:2] for row in before]
    assert after[0][1] != "provider_ids_unchanged"


# ---------------------------------------------------------------------------
# 4. Snapshot estimate
# ---------------------------------------------------------------------------
def test_snapshot_estimate_is_measured_once_per_written_generation(migrated_db, monkeypatch):
    calls = []
    estimate = catalog._estimate_snapshot_bytes
    monkeypatch.setattr(
        catalog, "_estimate_snapshot_bytes",
        lambda normalized: calls.append(1) or estimate(normalized),
    )
    first = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(12))
    )
    source = first["catalog_instance_id"]
    expected = estimate(catalog.normalize_provider_catalog(_catalogue(12), "navidrome"))
    assert calls == [1]
    assert first["snapshot_estimated_bytes"] == expected > 0

    def stored():
        return _row(migrated_db, f"SELECT snapshot_estimated_bytes FROM {P}catalog_state "
                    "WHERE catalog_instance_id=%s", (source,))[0][0]

    assert stored() == expected
    # Nothing changed: the stored value is kept and nothing is measured.
    del calls[:]
    unchanged = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(12))
    )
    assert unchanged["change_reason"] == "no_change"
    assert calls == []
    assert unchanged["snapshot_estimated_bytes"] == stored() == expected

    # A catalogue published before the estimate existed is measured once.
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_state SET snapshot_estimated_bytes=0")
    migrated_db.commit()
    catalog.refresh_catalog("server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(12)))
    assert calls == [1]
    assert stored() == expected

    del calls[:]
    grown = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(13))
    )
    assert calls == [1]
    assert grown["snapshot_estimated_bytes"] == stored() > expected


# ---------------------------------------------------------------------------
# 5. Catalogue journal
# ---------------------------------------------------------------------------
class RecordingDb:
    """Connection proxy listing each statement the server runs."""

    def __init__(self, db):
        self._db = db
        self.statements = []

    def cursor(self, *args, **kwargs):
        return RecordingCursor(self._db.cursor(*args, **kwargs), self)

    def __getattr__(self, name):
        return getattr(self._db, name)


class RecordingCursor:
    def __init__(self, cur, owner):
        self._cur = cur
        self._owner = owner

    def execute(self, sql, params=None):
        text = sql if isinstance(sql, str) else sql.decode()
        self._owner.statements.append(" ".join(text.split()))
        return self._cur.execute(sql, params)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._cur.close()

    def __getattr__(self, name):
        return getattr(self._cur, name)


def test_catalogue_journal_is_written_a_page_per_statement(migrated_db):
    catalog.refresh_catalog("server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(1200)))
    recording = RecordingDb(migrated_db)
    changed = {f"track-{index:03d}" for index in range(1100)}
    removed = {"track-1150", "track-1151"}
    result = catalog.refresh_catalog(
        "server-a", db=recording,
        bridge=RefreshBridge(_catalogue(1200, changed=changed, removed=removed)),
    )
    source = result["catalog_instance_id"]
    assert result["changes"] == 1102
    inserts = [
        sql for sql in recording.statements if sql.startswith(f"INSERT INTO {P}catalog_changes")
    ]
    # Red before the fix: one statement per event (1102).
    assert len(inserts) == 2
    events = _row(
        migrated_db,
        f"SELECT seq, entity_type, entity_id, operation, generation, change_reason, "
        f"payload IS NOT NULL, writer_generation FROM {P}catalog_changes "
        "WHERE catalog_instance_id=%s AND generation=2 ORDER BY seq",
        (source,),
    )
    # Dense seqs after the first generation's 1200, upserts in ID order and
    # the deletions last, as one statement per event wrote them.
    assert [row[0] for row in events] == list(range(1201, 2303))
    assert [row[2] for row in events[:1100]] == sorted(changed)
    assert [(row[2], row[3], row[6]) for row in events[1100:]] == [
        ("track-1150", "delete", False), ("track-1151", "delete", False),
    ]
    assert {(row[1], row[4], row[5], row[7]) for row in events} == {
        ("track", 2, "provider_diff", catalog.JOURNAL_WRITER_GENERATION)
    }
    assert {row[3] for row in events[:1100]} == {"upsert"}
    assert all(row[6] for row in events[:1100])
    assert result["cursor"]["seq"] == 2302
    assert _row(migrated_db, f"SELECT catalog_head_seq FROM {P}catalog_state "
                "WHERE catalog_instance_id=%s", (source,)) == [(2302,)]


def _journal(db, source):
    floor = _row(db, f"SELECT catalog_floor_seq, catalog_head_seq FROM {P}catalog_state "
                 "WHERE catalog_instance_id=%s", (source,))[0]
    seqs = _row(db, f"SELECT min(seq), max(seq), count(*) FROM {P}catalog_changes "
                "WHERE catalog_instance_id=%s", (source,))[0]
    return floor, seqs


def test_catalogue_journal_is_trimmed_after_the_publication_commits(
    migrated_db, second_connection, monkeypatch,
):
    monkeypatch.setattr(catalog, "MIN_RETAINED_CHANGE_EVENTS", 1)
    first = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(4))
    )
    source = first["catalog_instance_id"]
    # Four entities: the journal keeps eight events (two snapshots).
    for round_ in range(1, 4):
        probed = ProbedDb(migrated_db, second_connection, lambda: source)
        compaction = []
        probed.hooks.append((
            "before", lambda sql: "SAVEPOINT lumae_catalog_compaction" in sql,
            lambda: compaction.append(len(probed.held)),
        ))
        changed = {f"track-{index:03d}" for index in range(4)} if round_ % 2 else set()
        catalog.refresh_catalog(
            "server-a", db=probed, bridge=RefreshBridge(_catalogue(4, changed=changed))
        )
        # Never deleted while the publication holds catalog_state: only in
        # the compaction's own short transaction after it commits, which
        # locks catalog_state before the journal rows (the install's order).
        assert len(compaction) == 1
        publication = probed.held[: compaction[0]]
        assert not [sql for sql in publication if f"DELETE FROM {P}catalog_changes" in sql], (
            publication
        )
    (floor, head), (low, high, count) = _journal(migrated_db, source)
    assert head == 16
    # The floor and the trimmed events moved together, to the retention.
    assert (floor, low, high, count) == (8, 9, 16, 8)

    # Another publisher at work: this publication leaves the journal to it.
    probed = ProbedDb(migrated_db, second_connection, lambda: None)
    third = connect(_row(migrated_db, "SELECT current_schema()", None)[0][0])
    try:
        def other_publisher():
            with third.cursor() as cur:
                catalog._lock_catalog_publication(cur, source)

        probed.hooks.append(
            ("before", lambda sql: "pg_try_advisory_xact_lock" in sql, other_publisher)
        )
        catalog.refresh_catalog(
            "server-a", db=probed, bridge=RefreshBridge(_catalogue(4))
        )
        (floor, head), (low, _high, count) = _journal(migrated_db, source)
        assert (floor, head, low, count) == (8, 20, 9, 12)
    finally:
        third.rollback()
        third.close()
    # The next publication catches up in full.
    catalog.refresh_catalog(
        "server-a", db=migrated_db,
        bridge=RefreshBridge(_catalogue(4, changed={"track-000"})),
    )
    (floor, head), (low, high, count) = _journal(migrated_db, source)
    assert (floor, head, low, high, count) == (13, 21, 14, 21, 8)
    # A reader at the floor continues densely; one below it bootstraps.
    page = catalog.read_catalog_changes(
        migrated_db, catalog.opaque_cursor(source, first["cursor"]["epoch"], 13)
    )
    assert [change["seq"] for change in page["changes"]] == list(range(14, 22))
    with pytest.raises(KeyError):
        catalog.read_catalog_changes(
            migrated_db, catalog.opaque_cursor(source, first["cursor"]["epoch"], 12)
        )


# ---------------------------------------------------------------------------
# 6. ensure_catalog_sources
# ---------------------------------------------------------------------------
class NamedBridge(RefreshBridge):
    def __init__(self, payload=None, name="Server", default=True, probe=None):
        super().__init__(payload)
        self.name = name
        self.default = default
        self.probe = probe

    def require_server(self, server_id):
        return {**super().require_server(server_id), "name": self.name,
                "is_default": self.default}

    def __getattr__(self, name):
        # Only a bridge given a probe answers the identity ping.
        if name == "probe_server_identity" and self.__dict__.get("probe"):
            return self.__dict__["probe"]
        raise AttributeError(name)


def test_ensure_catalog_sources_writes_only_a_changed_name_or_default(migrated_db):
    sources, written = catalog._ensure_catalog_sources(migrated_db, NamedBridge())
    assert written and len(sources) == 1
    migrated_db.commit()
    source = sources[0]["catalog_instance_id"]

    def row():
        return _row(migrated_db, f"SELECT server_name, is_default, updated_at, xmin::text "
                    f"FROM {P}catalog_sources WHERE catalog_instance_id=%s", (source,))[0]

    before = row()
    # Red before the fix: every call rewrote (and locked) the row.
    assert catalog._ensure_catalog_sources(migrated_db, NamedBridge()) == (
        [{"catalog_instance_id": source, "current_core_server_id": "server-a",
          "provider_type": "navidrome", "rebind_status": "active"}],
        False,
    )
    assert catalog.ensure_catalog_sources(migrated_db, bridge=NamedBridge()) == [
        {"catalog_instance_id": source, "current_core_server_id": "server-a",
         "provider_type": "navidrome", "rebind_status": "active"},
    ]
    migrated_db.commit()
    assert row() == before

    for bridge, expected in (
        (NamedBridge(name="Renamed"), ("Renamed", True)),
        (NamedBridge(name="Renamed", default=False), ("Renamed", False)),
    ):
        assert catalog._ensure_catalog_sources(migrated_db, bridge)[1] is True
        migrated_db.commit()
        assert row()[:2] == expected
    assert row()[3] != before[3]


def test_refresh_holds_no_source_row_lock_across_the_provider_ping(
    migrated_db, second_connection, bounded,
):
    catalog.refresh_catalog("server-a", db=migrated_db, bridge=NamedBridge(_catalogue(3)))
    locked_during_ping = []

    def probe(_server_id):
        # The ping: another connection must be able to lock every source row.
        with second_connection.cursor() as cur:
            try:
                cur.execute(f"SELECT 1 FROM {P}catalog_sources FOR UPDATE NOWAIT")
                locked_during_ping.append(False)
            except errors.LockNotAvailable:
                locked_during_ping.append(True)
        second_connection.rollback()
        return {"provider_type": "navidrome", "server_version": "0.58.0"}

    # The server was renamed in AudioMuse, so this refresh writes the row.
    result = catalog.refresh_catalog(
        "server-a", db=migrated_db,
        bridge=NamedBridge(_catalogue(3), name="Renamed", probe=probe),
    )
    assert result["change_reason"] == "no_change"
    assert locked_during_ping == [False]
    assert _row(migrated_db, f"SELECT server_name FROM {P}catalog_sources", None) == [
        ("Renamed",)
    ]


# ---------------------------------------------------------------------------
# 7. /changes and the v2 alias server_id
# ---------------------------------------------------------------------------
def test_changes_accept_the_proven_v2_alias_server_id(migrated_db):
    _seed_source(migrated_db, continuity_from="legacy-default")
    _seed_source(migrated_db, source="catalog-b", server="server-b", default=False)
    cursor = catalog.opaque_cursor(SOURCE, "catalog-epoch", 0)
    # Red before the fix: KeyError (410) for the alias, although
    # create_bootstrap_session accepts it.
    page = catalog.read_catalog_changes(migrated_db, cursor, server_id="legacy-default")
    assert (page["catalog_instance_id"], page["server_id"]) == (SOURCE, "server-a")
    session = catalog.create_bootstrap_session(
        migrated_db, "user:a", server_id="legacy-default", catalog_instance_id=SOURCE
    )
    assert session["server_id"] == "server-a"
    for kwargs in ({}, {"server_id": "server-a"},
                   {"server_id": "legacy-default", "catalog_instance_id": SOURCE}):
        assert catalog.read_catalog_changes(migrated_db, cursor, **kwargs)[
            "catalog_instance_id"] == SOURCE
    # Unchanged answers: an unknown server is 410, another catalogue's
    # server is "cursor belongs to another source" (400).
    with pytest.raises(KeyError):
        catalog.read_catalog_changes(migrated_db, cursor, server_id="server-unknown")
    with pytest.raises(ValueError, match="another catalogue source"):
        catalog.read_catalog_changes(migrated_db, cursor, server_id="server-b")
    # The alias of one catalogue never opens another's journal.
    other = catalog.opaque_cursor("catalog-b", "catalog-epoch", 0)
    with pytest.raises(KeyError):
        catalog.read_catalog_changes(migrated_db, other, server_id="legacy-default")
    migrated_db.rollback()


# ---------------------------------------------------------------------------
# 8. Catalogue bootstrap DTO
# ---------------------------------------------------------------------------
def test_catalogue_bootstrap_track_carries_no_search_text(migrated_db):
    result = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(3))
    )
    source = result["catalog_instance_id"]
    assert _row(migrated_db, f"SELECT count(*) FROM {P}catalog_tracks "
                "WHERE search_text IS NOT NULL", None) == [(3,)]
    session = catalog.create_bootstrap_session(
        migrated_db, "user:a", catalog_instance_id=source
    )
    items = _all_pages(migrated_db, session, "user:a", "catalog")
    tracks = [item for kind, item in items if kind == "track"]
    assert [track["track_id"] for track in tracks] == ["track-000", "track-001", "track-002"]
    for track in tracks:
        assert "search_text" not in track
        # The rest of the row is still there.
        assert {"title", "duration_ms", "metadata_fp", "media_fp", "payload",
                "available"} <= set(track)
        assert "catalog_instance_id" not in track and "published_generation" not in track


# ---------------------------------------------------------------------------
# 9. Readiness after a failed read
# ---------------------------------------------------------------------------
def test_failed_readiness_read_keeps_the_transaction_usable(migrated_db, monkeypatch):
    _seed_source(migrated_db)
    compatibility = types.SimpleNamespace(core_version="v3.0.5", adapter="v3_registry")
    source = catalog.resolve_catalog_source(migrated_db, catalog_instance_id=SOURCE)[0]
    source["catalog"]["refresh_required"] = False  # an admitted catalogue
    migrated_db.commit()

    def failing_read(db, _source):
        with db.cursor() as cur:
            cur.execute("SELECT 1/0")

    with migrated_db.cursor() as cur:
        # Work the caller has not committed yet.
        cur.execute(f"UPDATE {P}catalog_sources SET server_name='uncommitted'")
        monkeypatch.setattr(status_model, "read_summary", failing_read)
        result = catalog_readiness.v3_release_readiness(migrated_db, compatibility, source, {})
        assert result["status"] == "readiness_unavailable"
        assert result["admission"]["catalog"]["admitted"] is True
        # Red before the fix: InFailedSqlTransaction on the next statement.
        cur.execute(f"SELECT server_name FROM {P}catalog_sources")
        assert cur.fetchall() == [("uncommitted",)]
        # The next readiness reads normally, in a savepoint of its own (the
        # committed summary, so no AudioMuse table is read here).
        monkeypatch.setattr(
            status_model, "read_summary",
            lambda _db, _source: {"links": (0,) * 6, "coverage": (0, 0, 0, None)},
        )
        result = catalog_readiness.v3_release_readiness(migrated_db, compatibility, source, {})
        assert result["status"] != "readiness_unavailable"
    migrated_db.commit()
    assert _row(migrated_db, f"SELECT server_name FROM {P}catalog_sources", None) == [
        ("uncommitted",)
    ]


# ---------------------------------------------------------------------------
# 10. Bootstrap sessions per principal
# ---------------------------------------------------------------------------
def test_shared_principals_may_hold_more_bootstrap_sessions(migrated_db, monkeypatch):
    _seed_source(migrated_db, analysis_generation=1)

    def create(principal, stream="catalog"):
        return catalog.create_bootstrap_session(
            migrated_db, principal, stream=stream, catalog_instance_id=SOURCE
        )

    for _ in range(4):
        create("user:alice")
    with pytest.raises(catalog.CatalogScanError, match="Too many active"):
        create("user:alice")
    # A refusal leaves no lock behind and does not count against others.
    create("user:bob")
    # Red before the fix: the fifth phone of a bearer or no-auth install
    # was refused.
    monkeypatch.setattr(catalog, "MAX_SHARED_BOOTSTRAP_SESSIONS", 7)
    for principal in ("bearer", "anonymous"):
        sessions = [create(principal, "analysis" if n % 2 else "catalog") for n in range(7)]
        with pytest.raises(catalog.CatalogScanError, match="Too many active"):
            create(principal)
        # A released session frees its place.
        assert catalog.release_bootstrap_session(
            migrated_db, sessions[0]["session_token"], principal
        )
        create(principal)


# ---------------------------------------------------------------------------
# 11. Journal compaction and the install's prune: one lock order
# ---------------------------------------------------------------------------
def _published_journal(db, monkeypatch):
    monkeypatch.setattr(catalog, "MIN_RETAINED_CHANGE_EVENTS", 1)
    first = catalog.refresh_catalog("server-a", db=db, bridge=RefreshBridge(_catalogue(4)))
    source = first["catalog_instance_id"]
    (floor, head), (_low, _high, count) = _journal(db, source)
    assert head > 1 and count > 1 and floor < head - 1
    return source


def _lock_timeout(db):
    with db.cursor() as cur:
        cur.execute("SELECT current_setting('lock_timeout')")
        return cur.fetchone()[0]


def test_compaction_skips_while_an_install_holds_catalog_state(
    migrated_db, second_connection, bounded, monkeypatch,
):
    source = _published_journal(migrated_db, monkeypatch)
    before = _journal(migrated_db, source)
    monkeypatch.setattr(catalog, "CATALOG_COMPACTION_LOCK_TIMEOUT", "200ms")
    # migrate(): a version bump locks catalog_state early in its transaction.
    with second_connection.cursor() as cur:
        cur.execute(
            f"UPDATE {P}catalog_state SET updated_at=now() WHERE catalog_instance_id=%s",
            (source,),
        )
    with migrated_db.cursor() as cur:
        catalog._compact_catalog_journal(cur, source, 1)
    # Skipped without raising; the transaction is usable and the caller's
    # lock_timeout is back.
    assert _lock_timeout(migrated_db) == LOCK_TIMEOUT
    migrated_db.commit()
    # The install then prunes the journal itself, without a deadlock.
    catalog.prune_catalog_storage(second_connection, source)
    second_connection.rollback()
    assert _journal(migrated_db, source) == before

    # Free, the compaction trims to the retention and moves the floor with it.
    with migrated_db.cursor() as cur:
        catalog._compact_catalog_journal(cur, source, 1)
    migrated_db.commit()
    (floor, head), (low, high, count) = _journal(migrated_db, source)
    assert (floor, low, high, count) == (head - 1, head, head, 1)


def test_compaction_locks_catalog_state_before_journal_rows(
    migrated_db, second_connection, bounded, monkeypatch,
):
    """Red before the fix: the compaction deleted the journal rows and then
    waited for catalog_state, so the install's delete of the same rows (under
    catalog_state) waited for the compaction: a deadlock."""
    source = _published_journal(migrated_db, monkeypatch)
    monkeypatch.setattr(catalog, "CATALOG_COMPACTION_LOCK_TIMEOUT", "15s")
    with second_connection.cursor() as cur:
        cur.execute(
            f"UPDATE {P}catalog_state SET updated_at=now() WHERE catalog_instance_id=%s",
            (source,),
        )
    outcome = {}

    def compact():
        try:
            with migrated_db.cursor() as cur:
                catalog._compact_catalog_journal(cur, source, 1)
            migrated_db.commit()
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
            migrated_db.rollback()

    worker = threading.Thread(target=compact)
    worker.start()
    try:
        _wait_until_waiting(migrated_db.get_backend_pid())
        # The waiting compaction holds no journal row: the install deletes
        # them all at once.
        with second_connection.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='1s'")
            cur.execute(
                f"DELETE FROM {P}catalog_changes WHERE catalog_instance_id=%s", (source,)
            )
        second_connection.commit()
    finally:
        second_connection.rollback()
        worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome
    (_floor, _head), (_low, _high, count) = _journal(migrated_db, source)
    assert count == 0


def test_prune_catalog_storage_locks_catalog_state_before_journal_rows(
    migrated_db, second_connection, third_connection, bounded, monkeypatch,
):
    """Red before the fix: prune deleted journal rows (here: waited for one)
    before it locked catalog_state, for the floor update only."""
    source = _published_journal(migrated_db, monkeypatch)
    # An event of a past epoch, which the prune purges; another transaction
    # holds it.
    with third_connection.cursor() as cur:
        cur.execute(
            f"UPDATE {P}catalog_changes SET epoch='past-epoch' "
            "WHERE catalog_instance_id=%s AND seq=1",
            (source,),
        )
        assert cur.rowcount == 1
    third_connection.commit()
    with third_connection.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {P}catalog_changes WHERE catalog_instance_id=%s "
            "AND epoch='past-epoch' FOR UPDATE",
            (source,),
        )
    outcome = {}

    def prune():
        try:
            catalog.prune_catalog_storage(second_connection, source)
            second_connection.commit()
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc
            second_connection.rollback()

    worker = threading.Thread(target=prune)
    worker.start()
    try:
        _wait_until_waiting(second_connection.get_backend_pid())
        with migrated_db.cursor() as cur:
            with pytest.raises(errors.LockNotAvailable):
                cur.execute(
                    f"SELECT 1 FROM {P}catalog_state WHERE catalog_instance_id=%s "
                    "FOR UPDATE NOWAIT",
                    (source,),
                )
        migrated_db.rollback()
    finally:
        third_connection.rollback()
        worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome


# ---------------------------------------------------------------------------
# 12. An analysis session creation waits for a projection only briefly
# ---------------------------------------------------------------------------
def test_analysis_session_creation_gives_up_on_a_running_projection(
    migrated_db, second_connection, bounded, monkeypatch,
):
    assert catalog.ANALYSIS_LEASE_LOCK_TIMEOUT == "3s"
    monkeypatch.setattr(catalog, "ANALYSIS_LEASE_LOCK_TIMEOUT", "200ms")
    _seed_source(migrated_db, analysis_generation=1)
    with migrated_db.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {P}analysis_state WHERE catalog_instance_id=%s FOR UPDATE",
            (SOURCE,),
        )
    # Red before the fix: this waited (here: the test's 15s) for the projection.
    with pytest.raises(catalog.CatalogScanError, match="not ready"):
        catalog.create_bootstrap_session(
            second_connection, "user:a", stream="analysis", catalog_instance_id=SOURCE
        )
    # Rolled back: no lock kept, the connection's own timeout in force, and
    # the connection usable.
    assert (
        second_connection.get_transaction_status()
        == psycopg2.extensions.TRANSACTION_STATUS_IDLE
    )
    assert _lock_timeout(second_connection) == LOCK_TIMEOUT
    second_connection.rollback()
    migrated_db.commit()
    # No orphan lease once the projection commits.
    assert _row(migrated_db, f"SELECT count(*) FROM {P}stream_bootstrap_sessions", None) == [(0,)]
    assert catalog.create_bootstrap_session(
        second_connection, "user:a", stream="analysis", catalog_instance_id=SOURCE
    )["generation"] == 1


def test_locked_analysis_state_restores_the_lock_timeout(migrated_db, bounded):
    _seed_source(migrated_db, analysis_generation=1)
    with migrated_db.cursor() as cur:
        assert catalog._locked_analysis_state(cur, SOURCE)["generation"] == 1
    assert _lock_timeout(migrated_db) == LOCK_TIMEOUT
    migrated_db.rollback()


# ---------------------------------------------------------------------------
# 13. The per-principal cap under concurrency
# ---------------------------------------------------------------------------
def test_concurrent_analysis_creations_respect_the_principal_cap(
    migrated_db, second_connection, third_connection, bounded, monkeypatch,
):
    """Red before the fix: both creations counted three sessions while they
    waited for the projection, and both inserted (five sessions, cap four)."""
    monkeypatch.setattr(catalog, "ANALYSIS_LEASE_LOCK_TIMEOUT", "15s")
    _seed_source(migrated_db, analysis_generation=1)
    for _ in range(3):
        catalog.create_bootstrap_session(
            migrated_db, "user:a", stream="analysis", catalog_instance_id=SOURCE
        )
    # A projection holds analysis_state, so both creations line up behind it.
    with third_connection.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {P}analysis_state WHERE catalog_instance_id=%s FOR UPDATE",
            (SOURCE,),
        )
    outcomes = []

    def create(db):
        try:
            outcomes.append(catalog.create_bootstrap_session(
                db, "user:a", stream="analysis", catalog_instance_id=SOURCE
            ))
        except Exception as exc:
            outcomes.append(exc)

    workers = []
    try:
        for db in (migrated_db, second_connection):
            worker = threading.Thread(target=create, args=(db,))
            worker.start()
            workers.append(worker)
            _wait_until_waiting(db.get_backend_pid())
    finally:
        third_connection.commit()
        for worker in workers:
            worker.join(30)
    assert not any(worker.is_alive() for worker in workers)
    created = [item for item in outcomes if isinstance(item, dict)]
    refused = [item for item in outcomes if isinstance(item, catalog.CatalogScanError)]
    assert (len(created), len(refused)) == (1, 1), outcomes
    assert "Too many active" in str(refused[0])
    assert _row(
        migrated_db,
        f"SELECT count(*) FROM {P}stream_bootstrap_sessions WHERE principal_key='user:a'",
        None,
    ) == [(4,)]
