"""Audit probes (read-only against repo; disposable schema per test)."""
import os
import sys
import pathlib

REPO = pathlib.Path("/home/user/lumae-plugin")
sys.path.insert(0, str(REPO / "tests" / "plugins"))
sys.path.insert(0, str(REPO))

import psycopg2
import pytest

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, load_plugin  # noqa: F401
from test_profile_publication_postgres import _result, _track
from plugins.LumaeAnalysis import catalog_enrichment as enrichment
from plugins.LumaeAnalysis import profile_publication as publication

SOURCE = "catalog-a"
PROFILES = "plugin_lumae_analysis__source_profiles"
TRACKS = "plugin_lumae_analysis__catalog_tracks"
STATE = "plugin_lumae_analysis__profile_stream_state"


def row(db, track):
    with db.cursor() as cur:
        cur.execute(
            f"SELECT status,retry_category,retry_count,retry_after,retry_media_signature,"
            f"attempt_token FROM {PROFILES} WHERE catalog_instance_id=%s AND track_id=%s",
            (SOURCE, track),
        )
        r = cur.fetchone()
    db.commit()
    return r


def fail(db, track, code="analysis_error"):
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    assert publication.complete_attempt(
        db, SOURCE, track, token, object(), "failed", "x", None, 1, 1, failure_code=code,
    )


def eligible(track):
    return track in load_plugin().find_backfill_ids(25, catalog_instance_id=SOURCE, server_id="server-a")


def make_due(db, track):
    with db.cursor() as cur:
        cur.execute(f"UPDATE {PROFILES} SET retry_after=now()-interval '1 second' WHERE track_id=%s", (track,))
    db.commit()


def set_fp(db, track, fp):
    with db.cursor() as cur:
        cur.execute(f"UPDATE {TRACKS} SET media_fp=%s WHERE catalog_instance_id=%s AND track_id=%s", (fp, SOURCE, track))
    db.commit()


def test_probe_retry_row_stranded_after_transient_missing_fingerprint(edge_publication_db):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "stuck", "rev-a")
    _track(db, "control", "rev-a")
    fail(db, "stuck")                      # retry_count 1, category analysis_error
    make_due(db, "stuck")
    assert eligible("stuck")
    stuck_tok = publication.admit_attempts(db, SOURCE, ["stuck"])["stuck"]
    ctl_tok = publication.admit_attempts(db, SOURCE, ["control"])["control"]
    set_fp(db, "stuck", None)              # catalogue briefly lacks fingerprint
    set_fp(db, "control", None)
    assert not publication.complete_attempt(db, SOURCE, "stuck", stuck_tok, _result(), "ready", None, "catalog-media:rev-a", 1, 1)
    assert not publication.complete_attempt(db, SOURCE, "control", ctl_tok, _result(), "ready", None, "catalog-media:rev-a", 1, 1)
    set_fp(db, "stuck", "rev-a")           # same media returns
    set_fp(db, "control", "rev-a")
    print("stuck row:", row(db, "stuck"))
    print("control row:", row(db, "control"))
    assert eligible("control")
    stuck_eligible = eligible("stuck")
    next_retry = mod.next_profile_retry_at(SOURCE, db=db)
    print("stuck eligible:", stuck_eligible, "next_retry_at:", next_retry)
    assert not stuck_eligible and next_retry is None  # stranded: never retried, no wake


def test_probe_retry_row_stranded_after_epoch_rebase(edge_publication_db):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "rebase", "rev-a")
    fail(db, "rebase")
    make_due(db, "rebase")
    publication.admit_attempts(db, SOURCE, ["rebase"])  # due retry in flight
    with db.cursor() as cur:
        publication.invalidate_catalog_changes(cur, SOURCE, 1, [], full_reconcile=True)
    db.commit()
    print("rebase row:", row(db, "rebase"))
    assert not eligible("rebase") and mod.next_profile_retry_at(SOURCE, db=db) is None


def test_probe_maintenance_pause_consumes_retry_budget(edge_publication_db, monkeypatch):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, "paused", "rev-a")
    monkeypatch.setattr(mod, "maintenance_paused", lambda: True)
    for _ in range(3):
        tokens = publication.admit_attempts(db, SOURCE, ["paused"])
        mod.analyze_tracks_task(["paused"], SOURCE, "server-a", "background", tokens)
        make_due(db, "paused")
    r = row(db, "paused")
    print("after 3 pauses, no analysis ever ran:", r)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    assert r[2] == 3 and not eligible("paused") and mod.next_profile_retry_at(SOURCE, db=db) is None


def test_probe_add_column_if_not_exists_takes_access_exclusive(edge_publication_db):
    db = edge_publication_db
    cur = db.cursor()
    publication.migrate_attempts(cur)  # all columns already exist
    cur.execute(
        "SELECT mode FROM pg_locks WHERE pid=pg_backend_pid() AND relation=%s::regclass",
        (PROFILES,),
    )
    modes = sorted({r[0] for r in cur.fetchall()})
    db.rollback()
    print("locks held by no-op migrate_attempts:", modes)
    assert "AccessExclusiveLock" in modes


def test_probe_v1_reader_skips_event_compacted_between_statements(edge_publication_db, monkeypatch):
    db = edge_publication_db
    cur = db.cursor()
    for i in range(1002):
        enrichment.record_profile_change(cur, SOURCE, f"t{i:05d}", "ready", {"track_id": f"t{i:05d}"})
    cur.execute(f"SELECT epoch, head_seq, floor_seq FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
    epoch, head, floor = cur.fetchone()
    db.commit()
    assert (head, floor) == (1002, 0)
    from plugins.LumaeAnalysis.catalog import opaque_cursor
    client_cursor = opaque_cursor(SOURCE, epoch, 1)  # valid cursor (>= floor 0)
    peer = psycopg2.connect(os.environ["LUMAE_POSTGRES_TEST_DSN"])
    with db.cursor() as c:
        c.execute("SELECT current_schema()")
        schema = c.fetchone()[0]
    db.commit()
    with peer.cursor() as c:
        c.execute(f"SET search_path TO {schema}, public")
    peer.commit()
    original = enrichment._profile_stream_state
    fired = []

    def reader_state(cur, source, **kw):
        result = original(cur, source, **kw)
        if not kw.get("for_update") and not fired:
            fired.append(True)
            # standalone locked compaction (real code path) commits between the
            # reader's frontier read and its event read
            enrichment.compact_enrichment_storage(peer, SOURCE)
            peer.commit()
        return result

    monkeypatch.setattr(enrichment, "_profile_stream_state", reader_state)
    page = enrichment.read_profile_changes(db, client_cursor, SOURCE, limit=5)
    db.commit()
    peer.close()
    seqs = [c["seq"] for c in page["changes"]]
    with db.cursor() as c:
        c.execute(f"SELECT floor_seq FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
        print("cursor 1, floor now", c.fetchone()[0], "returned seqs", seqs, "next cursor ok:", page["cursor"])
    db.commit()
    assert seqs[0] == 3  # event 2 silently skipped; no bootstrap_required raised


def test_probe_identical_reanalysis_republishes_and_drops_edge(edge_publication_db):
    from types import SimpleNamespace
    db = edge_publication_db
    _track(db, "lufs", "rev-a")
    res = SimpleNamespace(sample_rate=48000, duration_ms=1234, ref_lufs=-14.123456789,
                          start_ramp_blob=b"wave", end_ramp_blob=b"tail")
    tok = publication.admit_attempts(db, SOURCE, ["lufs"])["lufs"]
    assert publication.complete_attempt(db, SOURCE, "lufs", tok, res, "ready", None, "catalog-media:rev-a", 1, 1)
    with db.cursor() as cur:  # simulate an existing edge representation for this revision
        cur.execute("""INSERT INTO plugin_lumae_analysis__edge_profiles
            (catalog_instance_id, track_id, media_revision, representation_id, media_signature, profile_digest, payload)
            VALUES (%s, 'lufs', 'r', 'rep', 'catalog-media:rev-a', 'd', '{}'::jsonb)""", (SOURCE,))
        cur.execute(f"SELECT head_seq FROM {STATE}")
        head_before = cur.fetchone()[0]
    db.commit()
    tok = publication.admit_attempts(db, SOURCE, ["lufs"])["lufs"]
    assert publication.complete_attempt(db, SOURCE, "lufs", tok, res, "ready", None, "catalog-media:rev-a", 1, 1)
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {STATE}")
        head_after = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM plugin_lumae_analysis__edge_profiles WHERE track_id='lufs'")
        edges = cur.fetchone()[0]
    db.commit()
    print("identical re-analysis: head", head_before, "->", head_after, "edge rows left:", edges)
    assert head_after == head_before + 1 and edges == 0


def test_probe_admission_blocks_behind_catalog_publication_lock(edge_publication_db):
    import threading, time
    db = edge_publication_db
    _track(db, "blocked", "rev-a")
    peer = psycopg2.connect(os.environ["LUMAE_POSTGRES_TEST_DSN"])
    with db.cursor() as c:
        c.execute("SELECT current_schema()"); schema = c.fetchone()[0]
    db.commit()
    with peer.cursor() as c:
        c.execute(f"SET search_path TO {schema}, public")
        # stand-in for refresh_catalog's publication transaction
        c.execute("SELECT 1 FROM plugin_lumae_analysis__catalog_state WHERE catalog_instance_id=%s FOR UPDATE", (SOURCE,))
    done = []
    t = threading.Thread(target=lambda: done.append(publication.admit_attempts(db, SOURCE, ["blocked"])))
    t.start(); time.sleep(1.0)
    blocked = not done
    peer.commit(); t.join(5); peer.close()
    print("interactive/hook admission blocked while catalogue publication holds catalog_state:", blocked)
    assert blocked and done


def test_probe_lum006_every_sql_row_is_python_accepted(edge_publication_db, monkeypatch):
    import itertools
    from psycopg2.extras import execute_values
    db = edge_publication_db
    mod = load_plugin()
    statuses = [None, "ready", "pending", "pending_interactive", "stale", "failed",
                "skipped_no_file", "deferred_no_media_revision"]
    combos = list(itertools.product(
        statuses, [0, 1], [None, "", "catalog-media:X", "catalog-media:Y"], [None, "", "X"],
        [None, "analysis_error", "queue_unavailable", "silent_audio"], [0, 3],
        [None, "past", "future"], [None, "catalog-media:X", "catalog-media:Y"]))
    tracks, profiles = [], []
    for i, (st, av, sig, fp, cat, cnt, after, rsig) in enumerate(combos):
        tid = f"p{i:06d}"
        tracks.append((SOURCE, 1, tid, tid, "m", fp, True))
        if st is not None:
            profiles.append((SOURCE, tid, sig, 0, 0, 0, b"", b"", av, 1, st, cat, cnt,
                             {"past": "-1 hour", "future": "1 hour"}.get(after), rsig))
    with db.cursor() as cur:
        execute_values(cur, f"""INSERT INTO {TRACKS} (catalog_instance_id, published_generation, track_id,
            title, metadata_fp, media_fp, analysis_eligible, payload, first_seen_at, last_seen_at)
            VALUES %s""", tracks, template="(%s,%s,%s,%s,%s,%s,%s,'{}'::jsonb,now(),now())")
        execute_values(cur, f"""INSERT INTO {PROFILES} (catalog_instance_id, track_id, media_signature,
            sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
            analyzed_at, status, retry_category, retry_count, retry_after, retry_media_signature) VALUES %s""",
            profiles, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,%s,%s,now()+%s::interval,%s)")
    db.commit()
    rows = mod.fetch_backfill_rows(10**6, catalog_instance_id=SOURCE, server_id="server-a")
    rejected = [r for r in rows if not (r[4] in ("failed", "skipped_no_file")
                or mod.is_backfill_candidate(r[1], r[2], r[3], r[4]))]
    print("sql rows:", len(rows), "python-rejected:", len(rejected), rejected[:5])
    assert rejected == []


def test_probe_inline_compaction_cost_at_retention(edge_publication_db):
    import time
    db = edge_publication_db
    with db.cursor() as cur:
        cur.execute(f"SELECT epoch FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
        epoch = cur.fetchone()[0]
        cur.execute(f"""INSERT INTO plugin_lumae_analysis__profile_changes
            (catalog_instance_id, epoch, seq, track_id, operation, payload)
            SELECT %s, %s, g, 't'||g, 'upsert', '{{"x":1}}'::jsonb FROM generate_series(1, 50000) g""", (SOURCE, epoch))
        cur.execute(f"UPDATE {STATE} SET head_seq=50000 WHERE catalog_instance_id=%s", (SOURCE,))
        cur.execute("ANALYZE plugin_lumae_analysis__profile_changes")
    db.commit()
    timings = []
    for i in range(20):
        t0 = time.perf_counter()
        with db.cursor() as cur:
            enrichment.record_profile_change(cur, SOURCE, f"n{i}", "ready", {"track_id": f"n{i}"})
        db.commit()
        timings.append((time.perf_counter() - t0) * 1000)
    with db.cursor() as cur:
        cur.execute(f"""EXPLAIN (ANALYZE, BUFFERS) DELETE FROM plugin_lumae_analysis__profile_changes
             WHERE catalog_instance_id=%s AND (epoch<>%s OR (epoch=%s AND seq<=%s))""", (SOURCE, epoch, epoch, 30))
        plan = "\n".join(r[0] for r in cur.fetchall())
    db.rollback()
    print("per-publication ms (median):", sorted(timings)[10])
    print(plan)


def test_probe_invalidation_cost_full_reconcile(edge_publication_db):
    import time
    db = edge_publication_db
    n = 20000
    with db.cursor() as cur:
        cur.execute(f"""INSERT INTO {TRACKS} (catalog_instance_id, published_generation, track_id, title,
            metadata_fp, media_fp, analysis_eligible, payload, first_seen_at, last_seen_at)
            SELECT %s, 1, 'q'||g, 'q', 'm', 'fp'||g, TRUE, '{{}}'::jsonb, now(), now() FROM generate_series(1,%s) g""", (SOURCE, n))
        cur.execute(f"""INSERT INTO {PROFILES} (catalog_instance_id, track_id, media_signature, sample_rate,
            duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, analyzed_at, status)
            SELECT %s, 'q'||g, 'catalog-media:fp'||g, 1,1,1,'\\x00','\\x00',1,1,now(),'ready' FROM generate_series(1,%s) g""", (SOURCE, n))
        cur.execute(f"""INSERT INTO plugin_lumae_analysis__published_source_profiles (catalog_instance_id, track_id,
            media_signature, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, analyzed_at)
            SELECT %s, 'q'||g, 'catalog-media:fp'||g, 1,1,1,'\\x00','\\x00',1,1,now() FROM generate_series(1,%s) g""", (SOURCE, n))
        cur.execute("ANALYZE")
    db.commit()
    t0 = time.perf_counter()
    with db.cursor() as cur:
        w = publication.invalidate_catalog_changes(cur, SOURCE, 1, [], full_reconcile=True)
    elapsed = time.perf_counter() - t0
    db.rollback()
    print(f"full_reconcile over {n} unchanged published rows: {elapsed:.2f}s withdrawn={w}")
