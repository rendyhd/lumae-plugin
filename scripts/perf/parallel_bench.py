"""Parallel analysis bench (performance plan 2026-10-16, WP1b).

Times a background waveform batch (``analyze_tracks_task``) on the fixture
from ``seed.py`` (``--scale 0.275`` is about the 36k test library) at K = 1,
2, 4 and 6 analysis threads. The analyzer is ``parallel_stub.cpu_bound``
(``--burn`` CPU seconds per file, about one real waveform analysis) and
downloads are stubbed (a local file), so the numbers isolate what K buys
against the tick thread's own work: claims, the disposition check,
publication, the on-demand check and heartbeats per track, all on the real
database.

Prints one JSON object: per K the batch's seconds, tracks per hour and the
speed-up over K = 1. Each K analyses its own ``--tracks`` published tracks,
which get new ramps.

    LUMAE_PERF_DSN=... python3 scripts/perf/parallel_bench.py --tracks 48 --burn 0.6
"""
import argparse
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402
import parallel_stub  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--tracks", type=int, default=48)
    parser.add_argument("--burn", type=float, default=0.6)
    parser.add_argument("--workers", default="1,2,4,6")
    args = parser.parse_args()
    ks = [int(k) for k in args.workers.split(",")]

    mod = stub_host.load_plugin()
    from plugins.LumaeAnalysis import parallel_analysis as par

    aux = stub_host.aux_cursor()
    src = stub_host.default_source(aux)
    aux.execute(
        f"""SELECT t.track_id, t.media_fp FROM {T}catalog_tracks t
              JOIN {T}catalog_state c USING (catalog_instance_id)
             WHERE t.catalog_instance_id=%s AND t.published_generation=c.published_generation
               AND t.available AND t.analysis_eligible AND t.media_fp IS NOT NULL
             ORDER BY md5(t.track_id) LIMIT %s""",
        (src, args.tracks * len(ks)),
    )
    rows = aux.fetchall()
    if len(rows) < args.tracks * len(ks):
        sys.exit("not enough published tracks: seed a larger fixture")
    media = dict(rows)
    with tempfile.NamedTemporaryFile(suffix=".flac", delete=False) as handle:
        handle.write(b"\0" * 1024)
        path = handle.name

    mod.load_track_file = lambda track_id, catalog_instance_id=None, server_id=None: {
        "track_id": track_id, "file_path": path, "media_signature": f"catalog-media:{media[track_id]}",
        "cleanup_path": None}
    mod.analyze_file = parallel_stub.cpu_bound
    mod.analysis_time_limit_seconds = lambda: 600
    mod.MAX_BACKFILL_BATCH_SIZE = 10 ** 6  # one batch of --tracks, whatever K
    burn = args.burn
    parallel_stub.cpu_bound.__defaults__ = (burn, None)

    results, base = {}, None
    for index, k in enumerate(ks):
        ids = [track for track, _fp in rows[index * args.tracks:(index + 1) * args.tracks]]
        tokens = mod.mark_pending(ids, catalog_instance_id=src, priority="background")
        began = time.monotonic()
        with par.tick_scope(k):
            summary = mod.analyze_tracks_task(ids, catalog_instance_id=src, server_id=stub_host.SERVER_ID,
                                              priority="background", attempt_tokens=tokens)
        seconds = time.monotonic() - began
        base = base or seconds
        results[str(k)] = {"seconds": round(seconds, 2), "ready": summary["ready"],
                           "failed": summary["failed"],
                           "tracks_per_hour": round(len(ids) / seconds * 3600),
                           "speedup": round(base / seconds, 2)}
    os.unlink(path)
    stub_host.emit({"bench": "parallel_analysis", "tracks": args.tracks, "burn_seconds": burn,
                    "cpus": par.available_cpus(), "workers": results,
                    "logged_errors": stub_host.STATE.logged_errors[:5]})


if __name__ == "__main__":
    main()
