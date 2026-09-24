"""Publication critical-section bench (new in P0-3).

A waveform publication runs ``profile_publication.complete_attempt``: it locks
the source's ``catalog_state`` row (``FOR UPDATE``), rewrites the published
profile, deletes the old edge, appends a ``profile_changes`` event under the
``profile_stream_state`` row lock (``record_profile_change``) and compacts the
journal back to the 50k retained events, then commits. Every other publisher
for that source waits for the whole transaction, so its wall time *is* the
critical section (budget: <=5 ms p95 with 50k retained events).

Measured per iteration (``N`` distinct published tracks, default 40):

``complete_attempt_ms``        admitted attempt -> committed publication;
``record_profile_change_ms``   append + compaction alone, then commit;
``compaction_delete_ms``       the compaction ``DELETE`` statement alone.

Usage: ``pub_bench.py [N]``. Mutates the fixture: the chosen tracks get new
ramps and lose their edge payloads (as a real waveform republish does); the
retained journal stays at its size.
"""
import os
import sys
import time
import types

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    stub_host.load_plugin()
    from plugins.LumaeAnalysis import catalog_enrichment, profile_publication
    from plugins.LumaeAnalysis.catalog_enrichment import serialize_profile

    aux = stub_host.aux_cursor()
    src = stub_host.default_source(aux)

    def retained():
        aux.execute(f"SELECT count(*) FROM {T}profile_changes WHERE catalog_instance_id=%s", (src,))
        return aux.fetchone()[0]

    events_before = retained()
    aux.execute(
        f"""SELECT p.track_id, t.media_fp, p.duration_ms
              FROM {T}published_source_profiles p
              JOIN {T}catalog_state c USING (catalog_instance_id)
              JOIN {T}catalog_tracks t ON t.catalog_instance_id=p.catalog_instance_id
                   AND t.published_generation=c.published_generation AND t.track_id=p.track_id
             WHERE p.catalog_instance_id=%s AND t.available
             ORDER BY md5(p.track_id) LIMIT %s""",
        (src, 2 * n + 2),
    )
    tracks = aux.fetchall()
    if len(tracks) < 2 * n + 2:
        sys.exit("not enough published profiles for the requested iterations")

    db = stub_host.connect()
    complete_ms, record_ms, record_commit_ms, delete_ms = [], [], [], []
    for i, (track_id, media_fp, duration_ms) in enumerate(tracks[: n + 1]):
        tokens = profile_publication.admit_attempts(db, src, [track_id])
        token = tokens.get(track_id)
        if not token:
            sys.exit(f"admission failed for {track_id}")
        result = types.SimpleNamespace(
            sample_rate=44100, duration_ms=duration_ms, ref_lufs=-11.0 - i / 100.0,
            start_ramp_blob=os.urandom(45), end_ramp_blob=os.urandom(45))
        t0 = time.perf_counter()
        ok = profile_publication.complete_attempt(
            db, src, track_id, token, result, "ready", None, f"catalog-media:{media_fp}", 1, 1)
        elapsed = (time.perf_counter() - t0) * 1000
        if not ok:
            sys.exit(f"complete_attempt refused {track_id}")
        if i:  # first iteration is warm-up
            complete_ms.append(elapsed)

    cur = db.cursor()
    for i, (track_id, _media_fp, duration_ms) in enumerate(tracks[n + 1:]):
        payload = serialize_profile(track_id, 44100, duration_ms, -12.0, os.urandom(45),
                                    os.urandom(45), 1, None, f"bench:{track_id}")
        t0 = time.perf_counter()
        catalog_enrichment.record_profile_change(cur, src, track_id, "ready", payload)
        t1 = time.perf_counter()
        db.commit()
        t2 = time.perf_counter()
        if i:
            record_ms.append((t1 - t0) * 1000)
            record_commit_ms.append((t2 - t0) * 1000)

    # The compaction DELETE alone, in rolled-back transactions (same predicate
    # as catalog.compact_change_journal).
    cur.execute(f"SELECT epoch, head_seq FROM {T}profile_stream_state WHERE catalog_instance_id=%s",
                (src,))
    epoch, head = cur.fetchone()
    db.rollback()
    retention = catalog_enrichment.PROFILE_CHANGE_RETENTION_EVENTS
    for k in range(n + 1):
        floor = head - retention + 1 + k
        t0 = time.perf_counter()
        cur.execute(f"""DELETE FROM {T}profile_changes
                         WHERE catalog_instance_id=%s AND (epoch<>%s OR (epoch=%s AND seq<=%s))""",
                    (src, epoch, epoch, floor))
        elapsed = (time.perf_counter() - t0) * 1000
        db.rollback()
        if k:
            delete_ms.append(elapsed)
    db.close()

    stub_host.emit({
        "bench": "publication",
        "iterations": n,
        "retained_events_before": events_before,
        "retained_events_after": retained(),
        "retention_limit": retention,
        "complete_attempt_ms": stub_host.summary_ms(complete_ms),
        "record_profile_change_ms": stub_host.summary_ms(record_ms),
        "record_profile_change_commit_ms": stub_host.summary_ms(record_commit_ms),
        "compaction_delete_ms": stub_host.summary_ms(delete_ms),
    })


if __name__ == "__main__":
    main()
