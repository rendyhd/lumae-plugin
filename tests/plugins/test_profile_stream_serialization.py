"""Two-connection PostgreSQL regressions for source-local profile publication."""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, local
from types import SimpleNamespace

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, _edge_payload_for_job, load_plugin  # isolated disposable schema
from plugins.LumaeAnalysis import catalog_enrichment as enrichment, edge_profile_store, profile_publication
from plugins.LumaeAnalysis.catalog import opaque_cursor

SOURCE = "catalog-a"
STATE = "plugin_lumae_analysis__profile_stream_state"
CHANGES = "plugin_lumae_analysis__profile_changes"
PROFILES = "plugin_lumae_analysis__source_profiles"
PUBLISHED = "plugin_lumae_analysis__published_source_profiles"


def _peer(db):
    cur = db.cursor()
    cur.execute("SELECT current_schema()")
    schema = cur.fetchone()[0]
    cur.close()
    db.commit()
    peer = psycopg2.connect(os.environ["LUMAE_POSTGRES_TEST_DSN"])
    cur = peer.cursor()
    cur.execute(f"SET search_path TO {schema}, public")
    cur.execute("SET statement_timeout TO '8s'")
    cur.close()
    peer.commit()
    return peer


def _publish(db, track, source=SOURCE):
    cur = db.cursor()
    cur.execute(
        f"INSERT INTO {PROFILES} "
        "(catalog_instance_id, track_id, status, media_signature) "
        "VALUES (%s, %s, 'ready', %s)",
        (source, track, f"signature-{track}"),
    )
    cur.execute(
        f"""INSERT INTO {PUBLISHED}
            (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
             start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
             media_signature, analyzed_at)
            VALUES (%s, %s, 48000, 210, -13, %s, %s, 1, 1, %s, now())""",
        (source, track, b"first", b"last", f"signature-{track}"),
    )
    enrichment.record_profile_change(
        cur, source, track, "ready", {"track_id": track, "branch": track}
    )
    cur.close()


def _frontier(db):
    cur = db.cursor()
    cur.execute(f"SELECT epoch, head_seq, floor_seq FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
    state = cur.fetchone()
    # Since K6 (P3-2) an event journals its waveform part and a reference to
    # its edge; the payload is read as /changes serves it, with the referenced
    # edge row joined back (this also pins that the reference names it).
    cur.execute(
        f"""SELECT c.seq, c.track_id,
                   CASE WHEN e.payload IS NOT NULL
                        THEN c.payload || jsonb_build_object('edge_profile', e.payload)
                        ELSE c.payload END
              FROM {CHANGES} c
              LEFT JOIN plugin_lumae_analysis__edge_profiles e
                ON c.edge_ref IS NOT NULL AND e.catalog_instance_id=c.catalog_instance_id
               AND e.track_id=c.track_id AND e.media_revision=c.payload->>'media_revision'
               AND e.profile_digest=c.edge_ref->>'profile_digest'
             WHERE c.catalog_instance_id=%s ORDER BY c.seq""", (SOURCE,))
    events = cur.fetchall()
    cur.execute(f"SELECT track_id FROM {PUBLISHED} WHERE catalog_instance_id=%s AND track_id LIKE 'branch-%%' ORDER BY track_id", (SOURCE,))
    profiles = [row[0] for row in cur.fetchall()]
    cur.close()
    return state, events, profiles


def _fail_after_compaction(monkeypatch, fault):
    if fault != "after_compaction":
        return
    original = enrichment.compact_change_journal

    def compact_then_fail(cur, **kwargs):
        floor = original(cur, **kwargs)
        if type(cur).__name__ == "FailingCursor":
            raise RuntimeError(f"fault {fault}")
        return floor

    monkeypatch.setattr(enrichment, "compact_change_journal", compact_then_fail)


def _wait_for_lock(observer, pid, started):
    assert started.wait(5), "peer did not start"
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        cur = observer.cursor()
        cur.execute("SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s", (pid,))
        row = cur.fetchone()
        cur.close()
        if row and row[0] == "Lock":
            return
        started.wait(0.01)
    pytest.fail("peer never reached a PostgreSQL lock wait")


@pytest.mark.parametrize("rollback", [False, True])
def test_profile_publications_serialize_and_rollback_reuses_seq(edge_publication_db, rollback):
    first = _peer(edge_publication_db)
    second = _peer(edge_publication_db)
    try:
        _publish(first, "branch-first")
        pid = second.get_backend_pid()
        started = Event()

        def publish_second():
            started.set()
            _publish(second, "branch-second")
            second.commit()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(publish_second)
            try:
                _wait_for_lock(edge_publication_db, pid, started)
                state, events, profiles = _frontier(edge_publication_db)
                assert state[1] == 0 and events == [] and profiles == []
            finally:
                if rollback:
                    first.rollback()
                else:
                    first.commit()
            future.result(timeout=5)
        state, events, profiles = _frontier(edge_publication_db)
        expected = ["branch-second"] if rollback else ["branch-first", "branch-second"]
        assert state[1:] == (len(expected), 0)
        assert [(row[0], row[1], row[2]["branch"]) for row in events] == [
            (i, track, track) for i, track in enumerate(expected, 1)
        ]
        assert profiles == sorted(expected)
    finally:
        first.rollback()
        second.rollback()
        first.close()
        second.close()


def test_simultaneous_first_profile_state_uses_one_epoch(edge_publication_db):
    cur = edge_publication_db.cursor()
    cur.execute(f"DELETE FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
    cur.close()
    edge_publication_db.commit()
    first = _peer(edge_publication_db)
    second = _peer(edge_publication_db)
    try:
        _publish(first, "branch-first")
        started = Event()

        def publish_second():
            started.set()
            _publish(second, "branch-second")
            second.commit()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(publish_second)
            try:
                _wait_for_lock(edge_publication_db, second.get_backend_pid(), started)
                assert _frontier(edge_publication_db)[0] is None
            finally:
                first.commit()
            future.result(timeout=5)
        state, events, profiles = _frontier(edge_publication_db)
        assert state[0] and state[1:] == (2, 0)
        assert [(row[0], row[1]) for row in events] == [
            (1, "branch-first"), (2, "branch-second")
        ]
        assert profiles == ["branch-first", "branch-second"]
    finally:
        first.rollback()
        second.rollback()
        first.close()
        second.close()


def test_reader_sees_event_and_head_only_after_commit(edge_publication_db, monkeypatch):
    monkeypatch.setattr(enrichment, "resolve_catalog_source", lambda *_a, **_k: [{"catalog_instance_id": SOURCE}])
    writer = _peer(edge_publication_db)
    reader = _peer(edge_publication_db)
    try:
        cur = edge_publication_db.cursor()
        cur.execute(f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
        epoch = cur.fetchone()[0]
        cur.close()
        edge_publication_db.commit()
        cursor = opaque_cursor(SOURCE, epoch, 0)
        _publish(writer, "branch-first")
        before = enrichment.read_profile_changes(reader, cursor, SOURCE)
        reader.commit()
        assert before["changes"] == [] and before["head_cursor"] == cursor
        writer.commit()
        after = enrichment.read_profile_changes(reader, cursor, SOURCE)
        reader.commit()
        assert [(change["seq"], change["track_id"]) for change in after["changes"]] == [
            (1, "branch-first")
        ]
        assert after["head_cursor"] == opaque_cursor(SOURCE, epoch, 1)
    finally:
        writer.rollback()
        reader.rollback()
        writer.close()
        reader.close()




def test_different_sources_publish_without_waiting(edge_publication_db):
    cur = edge_publication_db.cursor()
    cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_sources
        (catalog_instance_id, current_core_server_id, provider_type, server_name, is_default, rebind_status)
        VALUES ('catalog-b','server-b','navidrome','Second',FALSE,'active')""")
    cur.close()
    enrichment.migrate_enrichment(edge_publication_db)
    edge_publication_db.commit()
    first = _peer(edge_publication_db)
    second = _peer(edge_publication_db)
    try:
        cur = first.cursor()
        cur.execute(f"SELECT head_seq FROM {STATE} WHERE catalog_instance_id=%s FOR UPDATE", (SOURCE,))
        assert cur.fetchone()[0] == 0
        cur.close()

        def publish_other_source():
            _publish(second, "branch-other", "catalog-b")
            second.commit()

        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(publish_other_source).result(timeout=5)
        cur = edge_publication_db.cursor()
        cur.execute(f"SELECT head_seq FROM {STATE} WHERE catalog_instance_id='catalog-b'")
        assert cur.fetchone()[0] == 1
        cur.close()
    finally:
        first.rollback()
        second.rollback()
        first.close()
        second.close()


def test_standalone_compaction_rereads_locked_head(edge_publication_db, monkeypatch):
    monkeypatch.setattr(enrichment, "resolve_catalog_source", lambda *_a, **_k: [{"catalog_instance_id": SOURCE}])
    monkeypatch.setattr(enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", 1_000)
    cur = edge_publication_db.cursor()
    cur.execute(f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
    epoch = cur.fetchone()[0]
    cur.execute(
        f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, operation, "
            "writer_generation) "
        "SELECT %s, %s, n, 'seed-' || n, 'delete', 2 FROM generate_series(1, 1001) AS n",
        (SOURCE, epoch),
    )
    cur.execute(f"UPDATE {STATE} SET head_seq=1001 WHERE catalog_instance_id=%s", (SOURCE,))
    cur.close()
    edge_publication_db.commit()
    first = _peer(edge_publication_db)
    second = _peer(edge_publication_db)
    try:
        cur = first.cursor()
        cur.execute(f"SELECT head_seq FROM {STATE} WHERE catalog_instance_id=%s FOR UPDATE", (SOURCE,))
        assert cur.fetchone()[0] == 1001
        cur.execute(
            f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, operation, "
            "writer_generation) "
            "VALUES (%s, %s, 1002, 'seed-1002', 'delete', 2)",
            (SOURCE, epoch),
        )
        cur.execute(f"UPDATE {STATE} SET head_seq=1002 WHERE catalog_instance_id=%s", (SOURCE,))
        cur.close()
        started = Event()

        def compact():
            started.set()
            enrichment.compact_enrichment_storage(second, SOURCE)
            second.commit()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(compact)
            try:
                _wait_for_lock(edge_publication_db, second.get_backend_pid(), started)
                cur = edge_publication_db.cursor()
                cur.execute(f"SELECT head_seq, floor_seq FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
                assert cur.fetchone() == (1001, 0)
                cur.close()
            finally:
                first.commit()
            future.result(timeout=5)
        state, events, _profiles = _frontier(edge_publication_db)
        assert state[1:] == (1002, 2)
        assert [row[0] for row in events] == list(range(3, 1003))
        with pytest.raises(KeyError, match="bootstrap_required"):
            enrichment.read_profile_changes(edge_publication_db, opaque_cursor(SOURCE, epoch, 1), SOURCE)
        valid = enrichment.read_profile_changes(edge_publication_db, opaque_cursor(SOURCE, epoch, 2), SOURCE)
        assert valid["changes"][0]["seq"] == 3
        assert valid["head_cursor"] == opaque_cursor(SOURCE, epoch, 1002)
    finally:
        first.rollback()
        second.rollback()
        first.close()
        second.close()



def test_legacy_and_edge_publishers_share_source_order(edge_publication_db):
    jobs, _ready = edge_profile_store.claim_edge_jobs(edge_publication_db, SOURCE, ["track-a"])
    assert len(jobs) == 1
    job = jobs[0]
    payload = _edge_payload_for_job(job)
    legacy = _peer(edge_publication_db)
    edge = _peer(edge_publication_db)
    try:
        _publish(legacy, "branch-legacy")
        started = Event()

        def publish_edge():
            started.set()
            assert edge_profile_store.publish_edge_profile(
                edge, SOURCE, job, payload, "private/path:123:456"
            )

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(publish_edge)
            try:
                _wait_for_lock(edge_publication_db, edge.get_backend_pid(), started)
                assert _frontier(edge_publication_db)[0][1] == 0
            finally:
                legacy.commit()
            future.result(timeout=5)
        state, events, _profiles = _frontier(edge_publication_db)
        assert state[1:] == (2, 0)
        assert [(row[0], row[1]) for row in events] == [
            (1, "branch-legacy"), (2, "track-a")
        ]
        assert events[1][2]["edge_profile"] == payload
        cur = edge_publication_db.cursor()
        cur.execute("SELECT status FROM plugin_lumae_analysis__edge_profile_jobs WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, "track-a"))
        assert cur.fetchone()[0] == "ready"
        cur.execute("SELECT payload FROM plugin_lumae_analysis__edge_profiles WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, "track-a"))
        assert cur.fetchone()[0] == payload
        cur.close()
    finally:
        legacy.rollback()
        edge.rollback()
        legacy.close()
        edge.close()



def _legacy_result():
    return SimpleNamespace(
        sample_rate=48000, duration_ms=210, ref_lufs=-13.0,
        start_ramp_blob=b"first", end_ramp_blob=b"last",
    )


def _enable_legacy_upsert(db):
    # The shared fixture already has the attempt and publication schemas.
    pass


def _claim_track(db, track):
    cur = db.cursor()
    cur.execute(
        """INSERT INTO plugin_lumae_analysis__catalog_tracks
           (catalog_instance_id, published_generation, track_id, title,
            metadata_fp, media_fp, payload, first_seen_at, last_seen_at)
           VALUES (%s, 1, %s, %s, 'metadata', %s, '{}'::jsonb, now(), now())
           ON CONFLICT (catalog_instance_id, published_generation, track_id)
           DO UPDATE SET media_fp=EXCLUDED.media_fp""",
        (SOURCE, track, track, f"signature-{track}"),
    )
    db.commit()
    return profile_publication.admit_attempts(db, SOURCE, [track])[track]


def test_actual_upsert_profile_serializes_two_independent_connections(
    edge_publication_db, monkeypatch
):
    _enable_legacy_upsert(edge_publication_db)
    mod = load_plugin()
    active = local()
    monkeypatch.setattr(mod, "get_db", lambda: active.db)
    original = profile_publication.record_profile_change
    first_staged = Event()
    release_first = Event()

    def pause_after_publication(cur, source, track, status, payload, **kwargs):
        result = original(cur, source, track, status, payload, **kwargs)
        if track == "branch-first":
            first_staged.set()
            assert release_first.wait(5), "first publisher was not released"
        return result

    monkeypatch.setattr(profile_publication, "record_profile_change", pause_after_publication)
    tokens = {
        track: _claim_track(edge_publication_db, track)
        for track in ("branch-first", "branch-second")
    }
    first = _peer(edge_publication_db)
    second = _peer(edge_publication_db)
    try:
        def upsert(db, track):
            active.db = db
            mod.upsert_profile(
                track, _legacy_result(), "ready",
                media_sig=f"catalog-media:signature-{track}", catalog_instance_id=SOURCE,
                attempt_token=tokens[track],
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            first_future = pool.submit(upsert, first, "branch-first")
            assert first_staged.wait(5), "first upsert did not stage a profile event"
            second_future = pool.submit(upsert, second, "branch-second")
            try:
                _wait_for_lock(edge_publication_db, second.get_backend_pid(), Event_set())
                assert _frontier(edge_publication_db)[0][1] == 0
            finally:
                release_first.set()
            first_future.result(timeout=5)
            second_future.result(timeout=5)
        state, events, profiles = _frontier(edge_publication_db)
        assert state[1:] == (2, 0)
        assert [(row[0], row[1]) for row in events] == [
            (1, "branch-first"), (2, "branch-second")
        ]
        assert profiles == ["branch-first", "branch-second"]
        assert all(row[2]["sample_rate"] == 48000 for row in events)
    finally:
        release_first.set()
        first.rollback()
        second.rollback()
        first.close()
        second.close()


def Event_set():
    event = Event()
    event.set()
    return event


def test_edge_first_blocks_legacy_publication(edge_publication_db, monkeypatch):
    jobs, _ = edge_profile_store.claim_edge_jobs(edge_publication_db, SOURCE, ["track-a"])
    job = jobs[0]
    payload = _edge_payload_for_job(job)
    original = enrichment.record_profile_change
    edge_staged = Event()
    release_edge = Event()

    def pause_after_edge(cur, source, track, status, public_payload, **kwargs):
        seq = original(cur, source, track, status, public_payload, **kwargs)
        if track == "track-a":
            edge_staged.set()
            assert release_edge.wait(5), "edge publisher was not released"
        return seq

    monkeypatch.setattr(enrichment, "record_profile_change", pause_after_edge)
    edge = _peer(edge_publication_db)
    legacy = _peer(edge_publication_db)
    try:
        def publish_edge():
            assert edge_profile_store.publish_edge_profile(
                edge, SOURCE, job, payload, "private/path:123:456"
            )

        def publish_legacy():
            _publish(legacy, "branch-legacy")
            legacy.commit()

        with ThreadPoolExecutor(max_workers=2) as pool:
            edge_future = pool.submit(publish_edge)
            assert edge_staged.wait(5), "edge publication did not stage"
            legacy_future = pool.submit(publish_legacy)
            try:
                _wait_for_lock(edge_publication_db, legacy.get_backend_pid(), Event_set())
                assert _frontier(edge_publication_db)[0][1] == 0
            finally:
                release_edge.set()
            edge_future.result(timeout=5)
            legacy_future.result(timeout=5)
        state, events, _ = _frontier(edge_publication_db)
        assert state[1:] == (2, 0)
        assert [(row[0], row[1]) for row in events] == [
            (1, "track-a"), (2, "branch-legacy")
        ]
        assert events[0][2]["edge_profile"] == payload
    finally:
        release_edge.set()
        edge.rollback()
        legacy.rollback()
        edge.close()
        legacy.close()



def test_compactor_first_serializes_following_profile_publication(
    edge_publication_db, monkeypatch
):
    cur = edge_publication_db.cursor()
    cur.execute(f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
    epoch = cur.fetchone()[0]
    cur.execute(
        f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, operation, "
            "writer_generation) "
        "SELECT %s, %s, n, 'seed-' || n, 'delete', 2 FROM generate_series(1, 1001) AS n",
        (SOURCE, epoch),
    )
    cur.execute(f"UPDATE {STATE} SET head_seq=1001 WHERE catalog_instance_id=%s", (SOURCE,))
    cur.close()
    edge_publication_db.commit()
    monkeypatch.setattr(enrichment, "PROFILE_CHANGE_RETENTION_EVENTS", 1_000)
    compactor = _peer(edge_publication_db)
    publisher = _peer(edge_publication_db)
    original = enrichment.compact_change_journal
    compact_staged = Event()
    release_compact = Event()

    def pause_after_lock(cur, **kwargs):
        if kwargs["state_table"] == "profile_stream_state":
            compact_staged.set()
            assert release_compact.wait(5), "compactor was not released"
        return original(cur, **kwargs)

    monkeypatch.setattr(enrichment, "compact_change_journal", pause_after_lock)
    try:
        def compact():
            enrichment.compact_enrichment_storage(compactor, SOURCE)
            compactor.commit()

        def publish():
            _publish(publisher, "branch-after-compaction")
            publisher.commit()

        with ThreadPoolExecutor(max_workers=2) as pool:
            compact_future = pool.submit(compact)
            assert compact_staged.wait(5), "compactor did not obtain state lock"
            publish_future = pool.submit(publish)
            try:
                _wait_for_lock(edge_publication_db, publisher.get_backend_pid(), Event_set())
                assert _frontier(edge_publication_db)[0][1:] == (1001, 0)
            finally:
                release_compact.set()
            compact_future.result(timeout=5)
            publish_future.result(timeout=5)
        state, events, profiles = _frontier(edge_publication_db)
        # The publication runs after the compactor committed, so it sees the
        # retention limit the compactor persisted (P1-2) and keeps 1000 events.
        assert state[1:] == (1002, 2)
        assert [row[0] for row in events] == list(range(3, 1003))
        assert profiles == ["branch-after-compaction"]
    finally:
        release_compact.set()
        compactor.rollback()
        publisher.rollback()
        compactor.close()
        publisher.close()


def test_reader_frontier_stays_old_after_publisher_rollback(
    edge_publication_db, monkeypatch
):
    monkeypatch.setattr(
        enrichment, "resolve_catalog_source",
        lambda *_a, **_k: [{"catalog_instance_id": SOURCE}],
    )
    cur = edge_publication_db.cursor()
    cur.execute(f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
    epoch = cur.fetchone()[0]
    cur.close()
    edge_publication_db.commit()
    cursor = opaque_cursor(SOURCE, epoch, 0)
    writer = _peer(edge_publication_db)
    reader = _peer(edge_publication_db)
    try:
        _publish(writer, "branch-rolled-back")
        before = enrichment.read_profile_changes(reader, cursor, SOURCE)
        reader.commit()
        assert before["changes"] == [] and before["head_cursor"] == cursor
        writer.rollback()
        after = enrichment.read_profile_changes(reader, cursor, SOURCE)
        reader.commit()
        assert after["changes"] == [] and after["head_cursor"] == cursor
        assert _frontier(edge_publication_db)[1:] == ([], [])
    finally:
        writer.rollback()
        reader.rollback()
        writer.close()
        reader.close()


def test_populated_enrichment_migration_keeps_epoch_and_history(edge_publication_db):
    original_state, original_events, _ = _frontier(edge_publication_db)
    assert original_state[1:] == (0, 0) and original_events == []
    enrichment.migrate_enrichment(edge_publication_db)
    edge_publication_db.commit()
    assert _frontier(edge_publication_db)[:2] == (original_state, [])
    writer = _peer(edge_publication_db)
    try:
        _publish(writer, "branch-after-migration")
        writer.commit()
    finally:
        writer.rollback()
        writer.close()
    published_state, published_events, published_profiles = _frontier(edge_publication_db)
    enrichment.migrate_enrichment(edge_publication_db)
    edge_publication_db.commit()
    assert _frontier(edge_publication_db) == (
        published_state, published_events, published_profiles
    )
    assert published_state[0] == original_state[0]
    assert published_state[1:] == (1, 0)
    assert [(row[0], row[1]) for row in published_events] == [
        (1, "branch-after-migration")
    ]



@pytest.mark.parametrize(
    "fault",
    [
        "after_profile", "after_event", "after_head",
        "after_compaction", "before_commit",
    ],
)
def test_actual_upsert_rolls_back_at_each_publication_boundary(
    edge_publication_db, monkeypatch, fault
):
    _enable_legacy_upsert(edge_publication_db)
    mod = load_plugin()
    failed_token = _claim_track(edge_publication_db, "branch-failed")
    recovery_token = _claim_track(edge_publication_db, "branch-recovery")
    first = _peer(edge_publication_db)
    second = _peer(edge_publication_db)
    markers = {
        "after_profile": f"INSERT INTO {PUBLISHED}".lower(),
        "after_event": f"INSERT INTO {CHANGES}".lower(),
        "after_head": f"UPDATE {STATE}".lower(),
    }
    _fail_after_compaction(monkeypatch, fault)

    class FailingCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def execute(self, sql, params=None):
            self.cursor.execute(sql, params)
            if fault in markers and markers[fault] in sql.lower():
                raise RuntimeError(f"fault {fault}")

        def __getattr__(self, name):
            return getattr(self.cursor, name)

    class FailingDb:
        def cursor(self):
            return FailingCursor(first.cursor())

        def commit(self):
            if fault == "before_commit":
                raise RuntimeError(f"fault {fault}")
            first.commit()

        def rollback(self):
            first.rollback()

    try:
        monkeypatch.setattr(mod, "get_db", lambda: FailingDb())
        with pytest.raises(RuntimeError, match=f"fault {fault}"):
            mod.upsert_profile(
                "branch-failed", _legacy_result(), "ready",
                media_sig="catalog-media:signature-branch-failed", catalog_instance_id=SOURCE,
                attempt_token=failed_token,
            )
        first.rollback()
        state, events, profiles = _frontier(edge_publication_db)
        assert state[1:] == (0, 0) and events == [] and profiles == []
        monkeypatch.setattr(mod, "get_db", lambda: second)
        mod.upsert_profile(
            "branch-recovery", _legacy_result(), "ready",
            media_sig="catalog-media:signature-branch-recovery", catalog_instance_id=SOURCE,
            attempt_token=recovery_token,
        )
        state, events, profiles = _frontier(edge_publication_db)
        assert state[1:] == (1, 0)
        assert [(row[0], row[1]) for row in events] == [(1, "branch-recovery")]
        assert profiles == ["branch-recovery"]
    finally:
        first.rollback()
        second.rollback()
        first.close()
        second.close()


def test_old_unlocked_allocator_collides_then_new_writer_succeeds(edge_publication_db):
    current = _peer(edge_publication_db)
    old = _peer(edge_publication_db)
    try:
        _publish(current, "branch-current")
        started = Event()

        def old_publication():
            cur = old.cursor()
            cur.execute(
                f"INSERT INTO {PROFILES} (catalog_instance_id, track_id, status, media_signature) "
                "VALUES (%s, %s, 'ready', %s)",
                (SOURCE, "branch-old", "signature-old"),
            )
            cur.execute(
                f"SELECT epoch, head_seq FROM {STATE} WHERE catalog_instance_id=%s",
                (SOURCE,),
            )
            epoch, head = cur.fetchone()
            assert head == 0
            started.set()
            cur.execute(
                f"INSERT INTO {CHANGES} "
                "(catalog_instance_id, epoch, seq, track_id, operation, payload, "
                "writer_generation) "
                "VALUES (%s, %s, %s, %s, 'upsert', %s::jsonb, 2)",
                (SOURCE, epoch, head + 1, "branch-old",
                 json.dumps({"track_id": "branch-old"})),
            )
            cur.execute(
                f"UPDATE {STATE} SET head_seq=%s WHERE catalog_instance_id=%s",
                (head + 1, SOURCE),
            )
            old.commit()
            cur.close()

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(old_publication)
            try:
                _wait_for_lock(edge_publication_db, old.get_backend_pid(), started)
                assert _frontier(edge_publication_db)[0][1] == 0
            finally:
                current.commit()
            with pytest.raises(psycopg2.errors.UniqueViolation):
                future.result(timeout=5)
        old.rollback()
        state, events, profiles = _frontier(edge_publication_db)
        assert state[1:] == (1, 0)
        assert [(row[0], row[1]) for row in events] == [(1, "branch-current")]
        assert profiles == ["branch-current"]
        _publish(old, "branch-old")
        old.commit()
        state, events, profiles = _frontier(edge_publication_db)
        assert state[1:] == (2, 0)
        assert [(row[0], row[1]) for row in events] == [
            (1, "branch-current"), (2, "branch-old")
        ]
        assert profiles == ["branch-current", "branch-old"]
    finally:
        current.rollback()
        old.rollback()
        current.close()
        old.close()



@pytest.mark.parametrize("first_kind", ["legacy", "edge"])
def test_actual_upsert_and_edge_publishers_share_one_order(
    edge_publication_db, monkeypatch, first_kind
):
    _enable_legacy_upsert(edge_publication_db)
    legacy_token = _claim_track(edge_publication_db, "branch-legacy")
    jobs, _ = edge_profile_store.claim_edge_jobs(edge_publication_db, SOURCE, ["track-a"])
    job = jobs[0]
    payload = _edge_payload_for_job(job)
    mod = load_plugin()
    active = local()
    monkeypatch.setattr(mod, "get_db", lambda: active.db)
    first_staged = Event()
    release_first = Event()

    def stage_after_record(original):
        def record(cur, source, track, status, public_payload, **kwargs):
            seq = original(cur, source, track, status, public_payload, **kwargs)
            if track == ("branch-legacy" if first_kind == "legacy" else "track-a"):
                first_staged.set()
                assert release_first.wait(5), "first publisher was not released"
            return seq
        return record

    if first_kind == "legacy":
        monkeypatch.setattr(
            profile_publication, "record_profile_change",
            stage_after_record(profile_publication.record_profile_change),
        )
    else:
        monkeypatch.setattr(
            enrichment, "record_profile_change",
            stage_after_record(enrichment.record_profile_change),
        )
    legacy = _peer(edge_publication_db)
    edge = _peer(edge_publication_db)
    try:
        def publish_legacy():
            active.db = legacy
            mod.upsert_profile(
                "branch-legacy", _legacy_result(), "ready",
                media_sig="catalog-media:signature-branch-legacy", catalog_instance_id=SOURCE,
                attempt_token=legacy_token,
            )

        def publish_edge():
            assert edge_profile_store.publish_edge_profile(
                edge, SOURCE, job, payload, "private/path:123:456"
            )

        first_fn, second_fn = (
            (publish_legacy, publish_edge)
            if first_kind == "legacy" else (publish_edge, publish_legacy)
        )
        second_db = edge if first_kind == "legacy" else legacy
        with ThreadPoolExecutor(max_workers=2) as pool:
            first_future = pool.submit(first_fn)
            assert first_staged.wait(5), "first publisher did not stage"
            second_future = pool.submit(second_fn)
            try:
                _wait_for_lock(
                    edge_publication_db, second_db.get_backend_pid(), Event_set()
                )
                assert _frontier(edge_publication_db)[0][1] == 0
            finally:
                release_first.set()
            first_future.result(timeout=5)
            second_future.result(timeout=5)
        state, events, profiles = _frontier(edge_publication_db)
        expected = (
            ["branch-legacy", "track-a"] if first_kind == "legacy"
            else ["track-a", "branch-legacy"]
        )
        assert state[1:] == (2, 0)
        assert [(row[0], row[1]) for row in events] == [
            (i, track) for i, track in enumerate(expected, 1)
        ]
        assert profiles == ["branch-legacy"]
        edge_event = next(row[2] for row in events if row[1] == "track-a")
        legacy_event = next(row[2] for row in events if row[1] == "branch-legacy")
        assert edge_event["edge_profile"] == payload
        assert legacy_event["sample_rate"] == 48000
        cur = edge_publication_db.cursor()
        cur.execute(
            "SELECT status FROM plugin_lumae_analysis__edge_profile_jobs "
            "WHERE catalog_instance_id=%s AND track_id='track-a'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == "ready"
        cur.execute(
            "SELECT payload FROM plugin_lumae_analysis__edge_profiles "
            "WHERE catalog_instance_id=%s AND track_id='track-a'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == payload
        cur.close()
    finally:
        release_first.set()
        legacy.rollback()
        edge.rollback()
        legacy.close()
        edge.close()



@pytest.mark.parametrize(
    "fault",
    [
        "after_edge_row", "after_event", "after_head",
        "after_compaction", "after_job_ready", "before_commit",
    ],
)
def test_edge_publisher_rolls_back_every_boundary(
    edge_publication_db, monkeypatch, fault
):
    jobs, _ = edge_profile_store.claim_edge_jobs(edge_publication_db, SOURCE, ["track-a"])
    job = jobs[0]
    payload = _edge_payload_for_job(job)
    first = _peer(edge_publication_db)
    second = _peer(edge_publication_db)
    markers = {
        "after_edge_row": "insert into plugin_lumae_analysis__edge_profiles",
        "after_event": f"insert into {CHANGES}",
        "after_head": f"update {STATE}",
        "after_job_ready": "update plugin_lumae_analysis__edge_profile_jobs set status='ready'",
    }

    class FailingCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def execute(self, sql, params=None):
            self.cursor.execute(sql, params)
            if fault in markers and markers[fault] in sql.lower():
                raise RuntimeError(f"fault {fault}")

        def __getattr__(self, name):
            return getattr(self.cursor, name)

    class FailingDb:
        def cursor(self):
            return FailingCursor(first.cursor())

        def commit(self):
            if fault == "before_commit":
                raise RuntimeError(f"fault {fault}")
            first.commit()

        def rollback(self):
            first.rollback()

    _fail_after_compaction(monkeypatch, fault)
    try:
        with pytest.raises(RuntimeError, match=f"fault {fault}"):
            edge_profile_store.publish_edge_profile(
                FailingDb(), SOURCE, job, payload, "private/path:123:456"
            )
        first.rollback()
        cur = edge_publication_db.cursor()
        cur.execute(
            "SELECT status FROM plugin_lumae_analysis__edge_profile_jobs "
            "WHERE catalog_instance_id=%s AND track_id='track-a'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == "pending"
        cur.execute(
            "SELECT COUNT(*) FROM plugin_lumae_analysis__edge_profiles "
            "WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            f"SELECT status FROM {PROFILES} "
            "WHERE catalog_instance_id=%s AND track_id='track-a'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == "ready"
        cur.close()
        state, events, _ = _frontier(edge_publication_db)
        assert state[1:] == (0, 0) and events == []
        assert edge_profile_store.publish_edge_profile(
            second, SOURCE, job, payload, "private/path:123:456"
        )
        state, events, _ = _frontier(edge_publication_db)
        assert state[1:] == (1, 0)
        assert [(row[0], row[1]) for row in events] == [(1, "track-a")]
        assert events[0][2]["edge_profile"] == payload
    finally:
        first.rollback()
        second.rollback()
        first.close()
        second.close()
