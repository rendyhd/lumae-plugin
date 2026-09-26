"""P3-7 (LUM-008 gaps): orphaned publications, edge backfill, ready repair.

* A catalogue publication withdraws the profiles of the tracks its diff
  deletes, so a published profile whose track was missing from the previous
  generation too is never withdrawn: the 1.2.5 upgrade seeds such rows from
  the ready profiles of tracks 1.2.5 had already dropped. The orphan pass
  (``withdraw_orphaned_profiles``) compares every published profile with the
  generation published now, after each catalogue refresh and in maintenance
  (``compact_enrichment_storage``), withdrawing like a publication does:
  under the catalog_state row lock, stale attempt, delete event, edge sweep.
* ``edge_backfill_candidates`` reads the published profiles: an edge is
  published only for a published waveform row of the same media.
* ``republish_ready_profiles`` repairs what ``profiles_unpublished_ready``
  counts through ``complete_attempt``'s checks and journal event, and never
  a 'ready' row for other media.

Every lock wait here is bounded (``lock_timeout``/``statement_timeout``), so a
regression that deadlocks or blocks fails instead of hanging.
"""
import threading
from types import SimpleNamespace

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from pg_helpers import connect  # noqa: E402
from test_lumae_analysis import RefreshBridge, load_plugin, plugin_client  # noqa: E402
from test_catalog_publication_lock_postgres import (  # noqa: E402
    ProbedDb,
    _catalogue,
    _edge,
    _is_publication_lock,
    _profile,
    _publish_baseline,
    _source,
    _track,
    _wait_until_waiting,
)
from test_upgrade_fences_postgres import plugin_125, unmigrated_db  # noqa: E402,F401
from plugins.LumaeAnalysis import (  # noqa: E402
    catalog,
    catalog_enrichment,
    edge_profile_store,
    profile_publication as publication,
)


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
LOCK_TIMEOUT = "15s"
STATEMENT_TIMEOUT = "60s"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _published(db, source=SOURCE):
    return [row[0] for row in _rows(
        db, f"SELECT track_id FROM {P}published_source_profiles "
        "WHERE catalog_instance_id=%s ORDER BY track_id", (source,))]


def _events(db, track, source=SOURCE):
    return [row[0] for row in _rows(
        db, f"SELECT operation FROM {P}profile_changes "
        "WHERE catalog_instance_id=%s AND track_id=%s ORDER BY seq", (source, track))]


def _journal(db, source=SOURCE):
    return _rows(db, f"SELECT epoch, seq, track_id, operation, payload, edge_ref "
                 f"FROM {P}profile_changes WHERE catalog_instance_id=%s "
                 "ORDER BY epoch, seq", (source,))


def _head(db, source=SOURCE):
    return _rows(db, f"SELECT head_seq FROM {P}profile_stream_state "
                 "WHERE catalog_instance_id=%s", (source,))[0][0]


def _edges(db, source=SOURCE):
    return [row[0] for row in _rows(
        db, f"SELECT track_id FROM {P}edge_profiles WHERE catalog_instance_id=%s "
        "ORDER BY track_id", (source,))]


def _attempt(db, track, source=SOURCE):
    rows = _rows(db, f"SELECT status, attempt_token FROM {P}source_profiles "
                 "WHERE catalog_instance_id=%s AND track_id=%s", (source, track))
    return rows[0] if rows else None


def _result():
    return SimpleNamespace(
        sample_rate=48000, duration_ms=1234, ref_lufs=-12.5,
        start_ramp_blob=b"new-wave", end_ramp_blob=b"tail",
    )


def _library(db, tracks, *, orphans=(), generation=1, edges=True):
    """``SOURCE`` publishes ``generation`` with ``tracks`` (media ``m-<id>``).
    Each track, and each of ``orphans`` (which the generation lacks), has a
    'ready' attempt and a published row for that media and an edge."""
    with db.cursor() as cur:
        _source(cur, SOURCE, "server-a", generation=generation)
        catalog_enrichment._profile_stream_state(cur, SOURCE, for_update=True)
        for track in tracks:
            _track(cur, SOURCE, track, f"m-{track}", generation=generation)
        for track in (*tracks, *orphans):
            signature = f"catalog-media:m-{track}"
            _profile(cur, "source_profiles", SOURCE, track, signature, status="ready")
            _profile(cur, "published_source_profiles", SOURCE, track, signature)
            if edges:
                _edge(cur, SOURCE, track, signature)
    db.commit()


def _publish_generation_without_withdrawal(db, generation, tracks):
    """Publish ``generation`` with ``tracks`` the way 1.2.5 did: new
    catalogue rows and state, and no profile withdrawal."""
    with db.cursor() as cur:
        for track in tracks:
            _track(cur, SOURCE, track, f"m-{track}", generation=generation)
        cur.execute(
            f"UPDATE {P}catalog_state SET published_generation=%s "
            "WHERE catalog_instance_id=%s",
            (generation, SOURCE),
        )
    db.commit()


def _schema(db):
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
    db.commit()
    return schema


def _bound(connection):
    with connection.cursor() as cur:
        cur.execute(f"SET lock_timeout='{LOCK_TIMEOUT}'")
        cur.execute(f"SET statement_timeout='{STATEMENT_TIMEOUT}'")
    connection.commit()
    return connection


@pytest.fixture
def connections(migrated_db, second_connection):
    """Bounded connections to the test schema; closed before it is dropped."""
    _bound(migrated_db)
    _bound(second_connection)
    schema = _schema(migrated_db)
    opened = []

    def open_connection():
        connection = _bound(connect(schema))
        opened.append(connection)
        return connection

    yield open_connection
    for connection in opened:
        try:
            connection.rollback()
            connection.close()
        except Exception:
            pass


def _consistent(db, source=SOURCE):
    """No mixed state: a withdrawal's delete is the last event of a track
    without a published row, a published row is not followed by a delete, a
    'ready' attempt is published, no edge outlives its published row, and
    the head is the last journalled seq."""
    published = set(_published(db, source))
    last = dict(_rows(db, f"""
        SELECT DISTINCT ON (track_id) track_id, operation FROM {P}profile_changes
         WHERE catalog_instance_id=%s ORDER BY track_id, seq DESC""", (source,)))
    for track, operation in last.items():
        assert (operation == "delete") is (track not in published), (track, operation)
    unpublished_ready = _rows(db, f"""
        SELECT s.track_id FROM {P}source_profiles s
         WHERE s.catalog_instance_id=%s AND s.status='ready' AND NOT EXISTS (
               SELECT 1 FROM {P}published_source_profiles p
                WHERE p.catalog_instance_id=s.catalog_instance_id
                  AND p.track_id=s.track_id)""", (source,))
    assert unpublished_ready == []
    assert _rows(db, f"""
        SELECT e.track_id FROM {P}edge_profiles e
         WHERE e.catalog_instance_id=%s AND NOT EXISTS (
               SELECT 1 FROM {P}published_source_profiles p
                WHERE p.catalog_instance_id=e.catalog_instance_id
                  AND p.track_id=e.track_id
                  AND p.media_signature=e.media_signature)""", (source,)) == []
    assert _rows(db, f"SELECT COALESCE(MAX(seq), 0) FROM {P}profile_changes c "
                 f"JOIN {P}profile_stream_state s USING (catalog_instance_id, epoch) "
                 "WHERE catalog_instance_id=%s", (source,)) == [(_head(db, source),)]


# ---------------------------------------------------------------------------
# 1. Orphaned publications
# ---------------------------------------------------------------------------
def test_1_2_5_upgrade_withdraws_the_orphans_it_seeds(
    unmigrated_db, plugin_125, run_plugin_migration, monkeypatch
):
    """A real 1.2.5 install analyses four tracks, then its catalogue drops two
    of them without withdrawing their ready profiles. The 1.3.0 migration
    seeds published rows from every ready profile; its maintenance pass then
    withdraws the two the generation lacks: delete event last, attempt
    staled, edge swept. The second install changes nothing."""
    mod = load_plugin()
    db, old = unmigrated_db, plugin_125
    monkeypatch.setattr(old, "enqueue_required_catalog_preparations", lambda **_kw: 0)
    monkeypatch.setattr(old, "_safe_reconcile_schedule", lambda *_a, **_kw: None)
    monkeypatch.setattr(old, "get_db", lambda: db)
    old.migrate(db)
    db.commit()
    with db.cursor() as cur:
        for name in ("catalog_sources", "catalog_state"):
            cur.execute(f"UPDATE {P}{name} SET current_core_server_id='server-a' "
                        "WHERE current_core_server_id='legacy-default'")
    db.commit()
    tracks = [{"id": f"t{index}", "title": f"Song {index}", "duration": 100 + index}
              for index in range(4)]
    source = old.catalog.refresh_catalog(
        "server-a", db=db, bridge=RefreshBridge({"tracks": tracks})
    )["catalog_instance_id"]
    revisions = dict(_rows(db, f"SELECT track_id, 'catalog-media:' || media_fp "
                               f"FROM {P}catalog_tracks WHERE catalog_instance_id=%s",
                           (source,)))
    for track in ("t0", "t1", "t2", "t3"):
        old.upsert_profile(track, _result(), "ready", media_sig=revisions[track],
                           catalog_instance_id=source)
    with db.cursor() as cur:
        for track in ("t0", "t1", "t2", "t3"):
            cur.execute(
                f"""INSERT INTO {P}edge_profiles
                    (catalog_instance_id, track_id, media_revision, representation_id,
                     media_signature, profile_digest, payload)
                    VALUES (%s, %s, %s, 'rep', %s, 'digest', '{{}}'::jsonb)""",
                (source, track, "rev:" + revisions[track], revisions[track]),
            )
    db.commit()
    # 1.2.5 publishes a generation without t2 and t3 and withdraws nothing.
    old.catalog.refresh_catalog("server-a", db=db, bridge=RefreshBridge({"tracks": tracks[:2]}))
    assert [row[0] for row in _rows(
        db, f"SELECT DISTINCT operation FROM {P}profile_changes")] == ["upsert"]

    run_plugin_migration(db)
    assert _published(db, source) == ["t0", "t1"]
    for track in ("t2", "t3"):
        assert _events(db, track, source) == ["upsert", "delete"]
        assert _attempt(db, track, source) == ("stale", None)
    for track in ("t0", "t1"):
        assert _events(db, track, source) == ["upsert"]
        assert _attempt(db, track, source) == ("ready", None)
    assert _edges(db, source) == ["t0", "t1"]
    status = mod.integrity_status(db)
    db.rollback()
    assert (status["profiles_orphaned"], status["profiles_unpublished_ready"]) == (0, 0)
    _consistent(db, source)

    before = (_published(db, source), _edges(db, source), _journal(db, source))
    run_plugin_migration(db)
    assert (_published(db, source), _edges(db, source), _journal(db, source)) == before


@pytest.mark.parametrize("refresh", ["changes", "no_change"])
def test_a_refresh_withdraws_a_profile_missing_from_both_generations(migrated_db, refresh):
    """The diff of a refresh never names a track that the previous generation
    already lacked; the pass after it still withdraws its publication."""
    source = _publish_baseline(migrated_db, 4)
    with migrated_db.cursor() as cur:
        _profile(cur, "source_profiles", source, "track-gone", "catalog-media:gone",
                 status="ready")
        _profile(cur, "published_source_profiles", source, "track-gone", "catalog-media:gone")
        _edge(cur, source, "track-gone", "catalog-media:gone")
    migrated_db.commit()
    head = _head(migrated_db, source)
    changed = {"track-001"} if refresh == "changes" else set()
    result = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_catalogue(4, changed=changed))
    )
    assert result["change_reason"] == ("provider_diff" if changed else "no_change")
    kept = [f"track-{index:03d}" for index in range(4) if f"track-{index:03d}" not in changed]
    assert _published(migrated_db, source) == kept
    assert _events(migrated_db, "track-gone", source) == ["delete"]
    assert _attempt(migrated_db, "track-gone", source) == ("stale", None)
    assert _edges(migrated_db, source) == kept
    assert _head(migrated_db, source) == head + 1 + len(changed)
    _consistent(migrated_db, source)


def test_the_orphan_predicate_reads_the_generation_published_now(migrated_db):
    """Generation 1 has y but not z; generation 2, published without a
    withdrawal, has z but not y. Only y is orphaned now."""
    _library(migrated_db, ["a", "y"], orphans=["z"], generation=1)
    _publish_generation_without_withdrawal(migrated_db, 2, ["a", "z"])
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == ["y"]
    assert _published(migrated_db) == ["a", "z"]
    assert _events(migrated_db, "y") == ["delete"]
    assert _events(migrated_db, "z") == []
    assert _attempt(migrated_db, "y") == ("stale", None)
    assert _attempt(migrated_db, "z") == ("ready", None)
    assert _edges(migrated_db) == ["a", "z"]
    _consistent(migrated_db)


def test_an_unavailable_track_is_an_orphan(migrated_db):
    """The generation keeps y's row with ``available=false`` (the provider
    no longer serves it): as orphaned as a track the generation lacks."""
    _library(migrated_db, ["a", "y"])
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET available=FALSE "
                    "WHERE catalog_instance_id=%s AND track_id='y'", (SOURCE,))
        assert publication.orphaned_publication_count(cur) == 1
    migrated_db.commit()
    head = _head(migrated_db)
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == ["y"]
    assert _published(migrated_db) == ["a"]
    assert _events(migrated_db, "y") == ["delete"]
    assert _attempt(migrated_db, "y") == ("stale", None)
    assert _edges(migrated_db) == ["a"]
    assert _head(migrated_db) == head + 1
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == []
    _consistent(migrated_db)


def test_orphan_withdrawal_is_bounded_per_run_and_idempotent(migrated_db):
    _library(migrated_db, ["a", "b"], orphans=["x1", "x2", "x3"])
    head = _head(migrated_db)
    # A run examines at most ``limit`` candidates, in batches.
    assert publication.withdraw_orphaned_profiles(
        migrated_db, SOURCE, batch_size=1, limit=2) == ["x1", "x2"]
    assert _published(migrated_db) == ["a", "b", "x3"]
    assert _head(migrated_db) == head + 2
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == ["x3"]
    for track in ("x1", "x2", "x3"):
        assert _events(migrated_db, track) == ["delete"]
        assert _attempt(migrated_db, track) == ("stale", None)
    assert _edges(migrated_db) == ["a", "b"]
    with migrated_db.cursor() as cur:
        assert publication.orphaned_publication_count(cur) == 0
    migrated_db.commit()
    journal = _journal(migrated_db)
    # Again: nothing to withdraw, no lock taken, no event.
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == []
    assert _journal(migrated_db) == journal
    _consistent(migrated_db)


def test_a_catalogue_without_rows_or_an_inactive_source_withdraws_nothing(migrated_db):
    """Only a published generation with rows says which tracks are gone: not
    generation 0, and not one whose rows are missing (a publication is never
    empty). An inactive source is left alone, as admission and completion
    leave it."""
    _library(migrated_db, ["a"], orphans=["x"], generation=0)

    def orphans():
        with migrated_db.cursor() as cur:
            count = publication.orphaned_publication_count(cur)
        migrated_db.commit()
        return count

    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == []
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_state SET published_generation=1")
    migrated_db.commit()
    assert orphans() == 0
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == []
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET published_generation=1")
        cur.execute(f"UPDATE {P}catalog_sources SET rebind_status='rebind_required'")
    migrated_db.commit()
    assert orphans() == 0
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == []
    assert _published(migrated_db) == ["a", "x"]
    assert _events(migrated_db, "x") == []
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_sources SET rebind_status='active'")
    migrated_db.commit()
    assert orphans() == 1
    assert publication.withdraw_orphaned_profiles(migrated_db, SOURCE) == ["x"]
    assert orphans() == 0


@pytest.mark.parametrize("first", ["withdrawal", "completion"])
def test_orphan_withdrawal_and_a_completion_serialize(
    migrated_db, second_connection, connections, first,
):
    """x is orphaned (generation 2 dropped it without a withdrawal) while an
    attempt for it is in flight. The withdrawal and the completion queue on
    catalog_state in either order: exactly one delete, attempt fenced."""
    _library(migrated_db, ["a", "x"], generation=1)
    token = publication.admit_attempts(migrated_db, SOURCE, ["x"])["x"]
    _publish_generation_without_withdrawal(migrated_db, 2, ["a"])
    holder = connections()
    withdrawer = connections()
    with holder.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {P}catalog_state WHERE catalog_instance_id=%s "
                    "FOR UPDATE", (SOURCE,))
    outcome = {}

    def withdraw():
        try:
            outcome["withdrawn"] = publication.withdraw_orphaned_profiles(withdrawer, SOURCE)
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    def complete():
        try:
            outcome["published"] = publication.complete_attempt(
                second_connection, SOURCE, "x", token, _result(), "ready", None,
                "catalog-media:m-x", 1, 1,
            )
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    workers = {
        "withdrawal": (threading.Thread(target=withdraw), withdrawer.get_backend_pid()),
        "completion": (threading.Thread(target=complete), second_connection.get_backend_pid()),
    }
    order = [first] + [name for name in workers if name != first]
    try:
        for name in order:
            worker, pid = workers[name]
            worker.start()
            _wait_until_waiting(pid)
    finally:
        holder.commit()
        for worker, _pid in workers.values():
            worker.join(30)
    assert not any(worker.is_alive() for worker, _pid in workers.values())
    assert "error" not in outcome, outcome
    assert outcome["published"] is False
    assert outcome["withdrawn"] == (["x"] if first == "withdrawal" else [])
    assert _published(migrated_db) == ["a"]
    assert _events(migrated_db, "x") == ["delete"]
    assert _attempt(migrated_db, "x") == ("stale", None)
    assert _edges(migrated_db) == ["a"]
    _consistent(migrated_db)


def _orphan_at_next_generation_media(db):
    """Baseline tracks 000-002 published by refresh_catalog, plus track-003:
    published at the media the next catalogue (4 tracks) gives it, but
    missing from generation 1, so it is orphaned until that publication."""
    source = _publish_baseline(db, 3)
    normalized = catalog.normalize_provider_catalog(_catalogue(4), "navidrome")
    media = {row["track_id"]: row["media_fp"] for row in normalized["tracks"]}
    signature = f"catalog-media:{media['track-003']}"
    with db.cursor() as cur:
        _profile(cur, "source_profiles", source, "track-003", signature, status="ready")
        _profile(cur, "published_source_profiles", source, "track-003", signature)
        _edge(cur, source, "track-003", signature)
    db.commit()
    return source


def test_a_withdrawal_waiting_for_a_publication_rechecks_the_new_generation(
    migrated_db, connections,
):
    """The pass reads track-003 as orphaned, then waits for catalog_state
    while a publication adds the track back. Under the lock it re-checks the
    generation published now and withdraws nothing."""
    source = _orphan_at_next_generation_media(migrated_db)
    withdrawer = connections()
    outcome = {}

    def withdraw():
        try:
            outcome["withdrawn"] = publication.withdraw_orphaned_profiles(withdrawer, source)
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    worker = threading.Thread(target=withdraw)

    def start_waiting_withdrawal():
        worker.start()
        _wait_until_waiting(withdrawer.get_backend_pid())

    probed = ProbedDb(migrated_db, connections(), lambda: None)
    probed.hooks.append(("after", _is_publication_lock, start_waiting_withdrawal))
    result = catalog.refresh_catalog(
        "server-a", db=probed, bridge=RefreshBridge(_catalogue(4, changed={"track-001"}))
    )
    worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome
    assert result["generation"] == 2
    assert outcome["withdrawn"] == []
    assert _published(migrated_db, source) == ["track-000", "track-002", "track-003"]
    assert _events(migrated_db, "track-003", source) == []
    assert _events(migrated_db, "track-001", source) == ["delete"]
    assert _edges(migrated_db, source) == ["track-000", "track-002", "track-003"]
    _consistent(migrated_db, source)


def test_a_publication_waiting_for_a_withdrawal_publishes_after_it(
    migrated_db, connections,
):
    """The pass holds catalog_state and withdraws track-003 (orphaned in
    generation 1); the publication that adds it back waits, re-checks its
    base and publishes. No deadlock, each track in one state."""
    source = _orphan_at_next_generation_media(migrated_db)
    publisher = connections()
    outcome = {}

    def publish():
        try:
            outcome["result"] = catalog.refresh_catalog(
                "server-a", db=publisher,
                bridge=RefreshBridge(_catalogue(4, changed={"track-001"})),
            )
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    worker = threading.Thread(target=publish)

    def start_waiting_publication():
        worker.start()
        _wait_until_waiting(publisher.get_backend_pid())

    probed = ProbedDb(connections(), connections(), lambda: None)
    probed.hooks.append((
        "after", lambda sql: "catalog_state" in sql and "FOR UPDATE OF c" in sql,
        start_waiting_publication,
    ))
    withdrawn = publication.withdraw_orphaned_profiles(probed, source)
    worker.join(30)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome
    assert withdrawn == ["track-003"]
    assert outcome["result"]["generation"] == 2
    assert _published(migrated_db, source) == ["track-000", "track-002"]
    assert _events(migrated_db, "track-003", source) == ["delete"]
    assert _attempt(migrated_db, "track-003", source) == ("stale", None)
    assert _events(migrated_db, "track-001", source) == ["delete"]
    assert _edges(migrated_db, source) == ["track-000", "track-002"]
    _consistent(migrated_db, source)


# ---------------------------------------------------------------------------
# 2. Edge backfill reads the publication
# ---------------------------------------------------------------------------
def test_edge_backfill_selects_published_profiles_without_their_edge(migrated_db):
    with migrated_db.cursor() as cur:
        _source(cur, SOURCE, "server-a", generation=1)
        for track in ("a", "b", "c", "d", "e", "f"):
            _track(cur, SOURCE, track, f"m-{track}", generation=1)
        # a: published with its edge.
        _profile(cur, "published_source_profiles", SOURCE, "a", "catalog-media:m-a")
        _profile(cur, "source_profiles", SOURCE, "a", "catalog-media:m-a", status="ready")
        _edge(cur, SOURCE, "a", "catalog-media:m-a")
        # b: published, no edge.
        _profile(cur, "published_source_profiles", SOURCE, "b", "catalog-media:m-b")
        _profile(cur, "source_profiles", SOURCE, "b", "catalog-media:m-b", status="ready")
        # c: a ready attempt that was never published.
        _profile(cur, "source_profiles", SOURCE, "c", "catalog-media:m-c", status="ready")
        # d: the attempt failed on newer media, which has an edge; the
        # published row (older media) has none.
        _profile(cur, "published_source_profiles", SOURCE, "d", "catalog-media:old-d")
        _profile(cur, "source_profiles", SOURCE, "d", "catalog-media:m-d", status="failed")
        _edge(cur, SOURCE, "d", "catalog-media:m-d")
        # e: the published row has its edge; the attempt is on other media.
        _profile(cur, "published_source_profiles", SOURCE, "e", "catalog-media:m-e")
        _profile(cur, "source_profiles", SOURCE, "e", "catalog-media:new-e", status="stale")
        _edge(cur, SOURCE, "e", "catalog-media:m-e")
        # f: published, no edge, but its edge job ran recently.
        _profile(cur, "published_source_profiles", SOURCE, "f", "catalog-media:m-f")
        cur.execute(
            f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, "
            "media_revision, job_token, status) VALUES (%s, 'f', 'rev', 'job', 'failed')",
            (SOURCE,),
        )
        cur.execute(f"UPDATE {P}edge_profile_jobs SET updated_at=now()-interval '7 hours' "
                    "WHERE track_id='d'")
    migrated_db.commit()
    candidates = edge_profile_store.edge_backfill_candidates
    assert candidates(migrated_db, SOURCE) == ["b", "d"]
    # Ordering and batches are unchanged: by track, after a cursor, limited.
    assert candidates(migrated_db, SOURCE, limit=1) == ["b"]
    assert candidates(migrated_db, SOURCE, after="b", limit=1) == ["d"]
    assert candidates(migrated_db, SOURCE, after="d") == []


# ---------------------------------------------------------------------------
# 3. Ready but unpublished
# ---------------------------------------------------------------------------
def _ready_unpublished_fixture(db):
    """a is published. r is 'ready' for the generation's media without a
    published row (what a pre-fence 1.2.5 worker left), with a 1.2.5 edge
    and job. s is 'ready' for older media, g for a track the generation
    lacks, o for an older analyzer: none of these three is current."""
    _library(db, ["a"])
    with db.cursor() as cur:
        for track in ("r", "s", "o"):
            _track(cur, SOURCE, track, f"m-{track}", generation=1)
        _profile(cur, "source_profiles", SOURCE, "r", "catalog-media:m-r", status="ready",
                 ref_lufs=-9.123456789, analyzed_at="2026-08-01 12:34:56")
        _edge(cur, SOURCE, "r", "catalog-media:m-r")
        _profile(cur, "source_profiles", SOURCE, "s", "catalog-media:old-s", status="ready")
        _profile(cur, "source_profiles", SOURCE, "g", "catalog-media:m-g", status="ready")
        _profile(cur, "source_profiles", SOURCE, "o", "catalog-media:m-o", status="ready",
                 analyzer_ver=0)
    db.commit()


def test_repair_republishes_ready_unpublished_rows_through_the_journal(migrated_db):
    mod = load_plugin()
    _ready_unpublished_fixture(migrated_db)
    mod.refresh_integrity_snapshot(migrated_db)
    migrated_db.commit()
    assert mod.integrity_status(migrated_db)["profiles_unpublished_ready"] == 1
    migrated_db.rollback()
    with migrated_db.cursor() as cur:
        # The candidates are exactly what the integrity count counts.
        assert publication._ready_unpublished(cur, SOURCE, 1, 1, 100) == ["r"]
    migrated_db.commit()
    head = _head(migrated_db)

    result = mod.repair_profile_publications(migrated_db)
    assert result == {"withdrawn": 0, "republished": 1,
                      "profiles_unpublished_ready": 0, "profiles_orphaned": 0}
    assert _published(migrated_db) == ["a", "r"]
    # One upsert, K6 format: the waveform payload, no edge (the 1.2.5 edge and
    # its job went first, as for any first publication), writer generation 2.
    stored = _rows(migrated_db, f"""
        SELECT sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
               analyzer_ver, analyzed_at, media_signature
          FROM {P}source_profiles WHERE catalog_instance_id=%s AND track_id='r'""",
        (SOURCE,))[0]
    published = _rows(migrated_db, f"""
        SELECT sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
               analyzer_ver, analyzed_at, media_signature
          FROM {P}published_source_profiles WHERE catalog_instance_id=%s
           AND track_id='r'""", (SOURCE,))[0]
    assert published == stored
    assert _rows(migrated_db, f"""
        SELECT seq, operation, payload, edge_ref, writer_generation
          FROM {P}profile_changes WHERE catalog_instance_id=%s AND track_id='r'""",
        (SOURCE,)) == [(
            head + 1, "upsert",
            catalog_enrichment.serialize_profile("r", *stored),
            None, 2,
        )]
    assert _head(migrated_db) == head + 1
    assert _edges(migrated_db) == ["a"]
    assert _rows(migrated_db, f"SELECT track_id FROM {P}edge_profile_jobs "
                 "ORDER BY track_id") == [("a",)]
    # The stale, missing and old rows are left for the backfill.
    for track in ("s", "g", "o"):
        assert _attempt(migrated_db, track) == ("ready", None)
    assert _published(migrated_db) == ["a", "r"]
    status = mod.integrity_status(migrated_db)
    migrated_db.rollback()
    assert (status["profiles_unpublished_ready"], status["profiles_orphaned"]) == (0, 0)

    # Twice: nothing to do, nothing journalled.
    journal = _journal(migrated_db)
    assert mod.repair_profile_publications(migrated_db) == {
        "withdrawn": 0, "republished": 0,
        "profiles_unpublished_ready": 0, "profiles_orphaned": 0,
    }
    assert _journal(migrated_db) == journal


def test_repair_never_republishes_a_row_whose_media_changed_meanwhile(
    migrated_db, monkeypatch,
):
    """The candidates are read before the lock. When a publication changed
    r's media (and s's row was always for older media) by then, the check
    under the lock refuses both."""
    _ready_unpublished_fixture(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET media_fp='m-r2' WHERE track_id='r'")
    migrated_db.commit()
    monkeypatch.setattr(publication, "_ready_unpublished", lambda *_args: ["r", "s"])
    head = _head(migrated_db)
    assert publication.republish_ready_profiles(migrated_db, SOURCE, 1, 1) == []
    assert _published(migrated_db) == ["a"]
    assert _head(migrated_db) == head
    assert _attempt(migrated_db, "r") == ("ready", None)


def test_repair_skips_a_row_admitted_after_the_candidates_were_read(
    migrated_db, second_connection, connections, monkeypatch,
):
    """An admission between the candidate read and the lock owns the row:
    it is no longer 'ready', and its completion publishes it."""
    _ready_unpublished_fixture(migrated_db)
    real = publication._ready_unpublished
    tokens = {}

    def candidates_then_admission(cur, source, analyzer, schema, limit):
        found = real(cur, source, analyzer, schema, limit)
        if found:
            tokens.update(publication.admit_attempts(second_connection, SOURCE, ["r"]))
        return found

    monkeypatch.setattr(publication, "_ready_unpublished", candidates_then_admission)
    head = _head(migrated_db)
    assert publication.republish_ready_profiles(migrated_db, SOURCE, 1, 1) == []
    assert _head(migrated_db) == head
    assert publication.complete_attempt(
        second_connection, SOURCE, "r", tokens["r"], _result(), "ready", None,
        "catalog-media:m-r", 1, 1,
    )
    assert _events(migrated_db, "r") == ["upsert"]
    assert _published(migrated_db) == ["a", "r"]


def test_repair_skips_a_row_another_repair_publishes_meanwhile(
    migrated_db, second_connection, connections, monkeypatch,
):
    """Between the candidate read and the batch, a second connection
    publishes r and holds its transaction open. The batch queues behind it
    on catalog_state; after the commit, the published-row check (``SELECT
    ... FOR UPDATE``) finds r published and leaves it: one upsert, no
    duplicate key, no second event."""
    _ready_unpublished_fixture(migrated_db)
    real = publication._ready_unpublished
    outcome = {}

    def candidates_then_publication(cur, source, analyzer, schema, limit):
        found = real(cur, source, analyzer, schema, limit)
        if found:
            with second_connection.cursor() as other:
                generation = publication._source_state(other, SOURCE)[0]
                outcome["other"] = publication._republish_ready(
                    other, SOURCE, generation, "r", 1, 1)
            waiter.start()
        return found

    def commit_when_waiting():
        try:
            _wait_until_waiting(migrated_db.get_backend_pid())
        finally:
            second_connection.commit()

    waiter = threading.Thread(target=commit_when_waiting)
    monkeypatch.setattr(publication, "_ready_unpublished", candidates_then_publication)
    head = _head(migrated_db)
    try:
        assert publication.republish_ready_profiles(migrated_db, SOURCE, 1, 1) == []
    finally:
        waiter.join(30)
    assert not waiter.is_alive()
    assert outcome["other"] is True
    assert _events(migrated_db, "r") == ["upsert"]
    assert _head(migrated_db) == head + 1
    assert _published(migrated_db) == ["a", "r"]
    assert _edges(migrated_db) == ["a"]


def test_install_maintenance_runs_the_repair(migrated_db, run_plugin_migration):
    _ready_unpublished_fixture(migrated_db)
    with migrated_db.cursor() as cur:
        _profile(cur, "published_source_profiles", SOURCE, "x", "catalog-media:m-x")
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    assert _published(migrated_db) == ["a", "r"]
    assert _events(migrated_db, "r") == ["upsert"]
    assert _events(migrated_db, "x") == ["delete"]
    mod = load_plugin()
    status = mod.integrity_status(migrated_db)
    migrated_db.rollback()
    assert (status["profiles_unpublished_ready"], status["profiles_orphaned"]) == (0, 0)


def test_settings_offers_and_runs_the_repair(migrated_db, monkeypatch):
    mod = load_plugin()
    _ready_unpublished_fixture(migrated_db)
    with migrated_db.cursor() as cur:
        _profile(cur, "published_source_profiles", SOURCE, "x", "catalog-media:m-x")
    migrated_db.commit()
    mod.refresh_integrity_snapshot(migrated_db)
    migrated_db.commit()
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    monkeypatch.setattr(mod, "render_settings_status_panels", lambda _size: dict.fromkeys(
        ("readiness", "relationships", "catalogue", "waveform", "reconcile", "identity",
         "stream_status"), ""))
    monkeypatch.setattr(mod, "render_page", lambda body, title=None: body)
    client = plugin_client(mod)
    page = client.get("/settings").get_data(as_text=True)
    assert 'value="repair_profile_publications"' in page
    assert "1 published profiles belong to" in " ".join(page.split())
    body = client.post(
        "/settings", data={"action": "repair_profile_publications"}
    ).get_data(as_text=True)
    text = " ".join(body.split())
    assert "Withdrew 1 published profiles" in text
    assert "republished 1 ready profiles" in text
    assert "Left: 0 orphaned and 0 unpublished ready profiles" in text
    assert 'value="repair_profile_publications"' not in body
    assert _published(migrated_db) == ["a", "r"]


def _plan_parents(plan, relation, parent=None):
    """Node types of the parents of every scan of ``relation`` in a plan."""
    found = []
    if plan.get("Relation Name", "").endswith(relation):
        found.append(parent)
    for child in plan.get("Plans", []):
        found.extend(_plan_parents(child, relation, plan.get("Node Type")))
    return found


@pytest.mark.parametrize("query", ["count", "candidates"])
def test_unpublished_ready_lookups_are_per_row(migrated_db, query):
    """Joined on the source alone, the planner compared every unpublished row
    with every catalogue track (1,000 rows: 20 s at 132k tracks). Each row
    now looks its track up by key, under a LIMIT the planner cannot flatten."""
    mod = load_plugin()
    _ready_unpublished_fixture(migrated_db)
    statements = []
    real_cursor = migrated_db.cursor

    class Recording:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            statements.append(self._cur.mogrify(sql, params).decode())
            return self._cur.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    db = SimpleNamespace(cursor=lambda: Recording(real_cursor()))
    if query == "count":
        assert mod.profiles_unpublished_ready_count(db) == 1
    else:
        cur = db.cursor()
        assert publication._ready_unpublished(cur, SOURCE, 1, 1, 100) == ["r"]
        cur.close()
    sql = next(sql for sql in statements if "unpublished" in sql)
    plan = _rows(migrated_db, "EXPLAIN (FORMAT JSON) " + sql)[0][0][0]["Plan"]
    assert _plan_parents(plan, "catalog_tracks") == ["Limit"]


def test_runbook_repair_b_inspect_counts_the_current_rows(migrated_db):
    import pathlib

    _ready_unpublished_fixture(migrated_db)
    text = (pathlib.Path(__file__).resolve().parents[2]
            / "docs/runbooks/UPGRADE_1.3.md").read_text(encoding="utf-8")
    start = text.index("-- Inspect.\nWITH unpublished")
    sql = text[start:text.index("-- Repair: re-admit them", start)]
    assert _rows(migrated_db, sql) == [(SOURCE, 1)]
