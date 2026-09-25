"""Catalogue publication lock bench (new in P2-3).

``catalog.refresh_catalog`` publishes a catalogue generation under the
source's ``catalog_state`` row lock (``FOR UPDATE``). Profile admission and
completion, edge publication and bootstrap creation take the same row lock,
so every analysis request for the source waits while it is held (LUM-008
gap F4). Budget: a full reconcile in which 20k tracks changed media holds
``catalog_state`` for <=1 s.

Scenario, on the full-scale fixture from ``seed.py``:

``setup``  Build a synthetic Navidrome catalogue for every fixture track
           (132k at scale 1), align the seeded profile signatures with its
           normalized media revisions, and publish it once. Every track
           changes fingerprints; nothing is withdrawn.
``runs``   ``--runs`` times, change the media (file size) of the next
           ``--changed`` published tracks and refresh the whole catalogue
           again: ``--changed`` catalogue upserts, ``--changed`` profile
           withdrawals (published row, edge payload, delete event) and as
           many stale attempts.

Measured per refresh:

``hold_ms``           the longest time the row was unavailable to
                      ``SELECT ... FOR UPDATE NOWAIT`` (the lock an admission
                      waits for), probed by a side connection every ~1 ms;
``instrumented_hold_ms``  the same interval from the publisher's side: end of
                      its ``catalog_state ... FOR UPDATE`` statement to the end
                      of its commit;
``statements_held``   statements the publisher ran inside that interval, with
                      the most expensive statement kinds (``held_top``);
``post_commit_ms``    the work after that commit, outside the row lock: the
                      sweep of unreachable edge payloads and the prune of
                      superseded generations (``post_commit_top``);
``elapsed_s``         the whole ``refresh_catalog`` call.

Usage: ``catalog_lock_bench.py [--changed 20000] [--runs 3]``. Mutates the
fixture (new generations, withdrawn profiles): run it on a fresh seed, or on a
copy (``CREATE DATABASE ... TEMPLATE``), once per invocation.
"""
import argparse
import io
import os
import re
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402

import psycopg2  # noqa: E402
import psycopg2.errors  # noqa: E402
import psycopg2.extensions  # noqa: E402

LOG = []  # (start, end, label, statements)
_TABLE = re.compile(r"plugin_lumae_analysis__(\w+)")


def _label(query):
    text = query.decode() if isinstance(query, bytes) else str(query)
    words = text.split()
    verb = words[0].upper() if words else "?"
    table = _TABLE.search(text)
    label = f"{verb} {table.group(1) if table else '-'}"
    if "FOR UPDATE" in text.upper():
        label += " FOR UPDATE"
    return label


class TimedCursor(psycopg2.extensions.cursor):
    def execute(self, query, vars=None):
        t0 = time.perf_counter()
        try:
            return super().execute(query, vars)
        finally:
            LOG.append((t0, time.perf_counter(), _label(query), 1))

    def executemany(self, query, vars_list):
        # psycopg2 runs one statement per parameter row; count them that way.
        t0 = time.perf_counter()
        count = 0
        try:
            for values in vars_list:
                super().execute(query, values)
                count += 1
        finally:
            LOG.append((t0, time.perf_counter(), _label(query), count))


class TimedConnection(psycopg2.extensions.connection):
    def commit(self):
        t0 = time.perf_counter()
        try:
            return super().commit()
        finally:
            LOG.append((t0, time.perf_counter(), "COMMIT", 0))

    def rollback(self):
        t0 = time.perf_counter()
        try:
            return super().rollback()
        finally:
            LOG.append((t0, time.perf_counter(), "ROLLBACK", 0))


class HoldWatch:
    """Probe the catalog_state row with FOR UPDATE NOWAIT; record held intervals."""

    def __init__(self, source, interval_s=0.001):
        self.source = source
        self.interval_s = interval_s
        self.holds_ms = []
        self.samples = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        conn = stub_host.connect()
        cur = conn.cursor()
        held_since = None
        while not self._stop.is_set():
            try:
                cur.execute(
                    f"SELECT 1 FROM {T}catalog_state WHERE catalog_instance_id=%s "
                    "FOR UPDATE NOWAIT",
                    (self.source,),
                )
                held = False
            except psycopg2.errors.LockNotAvailable:
                held = True
            now = time.perf_counter()
            conn.rollback()
            self.samples += 1
            if held and held_since is None:
                held_since = now
            elif not held and held_since is not None:
                self.holds_ms.append((now - held_since) * 1000)
                held_since = None
            time.sleep(self.interval_s)
        if held_since is not None:
            self.holds_ms.append((time.perf_counter() - held_since) * 1000)
        conn.close()

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        time.sleep(0.02)
        self._stop.set()
        self._thread.join()


def held_intervals(entries):
    """Publisher-side intervals from a catalog_state FOR UPDATE to commit/rollback."""
    intervals = []
    since = None
    for start, end, label, _count in entries:
        if since is None and label.endswith("catalog_state FOR UPDATE"):
            since = end
        elif since is not None and label in ("COMMIT", "ROLLBACK"):
            intervals.append((since, end))
            since = None
    return intervals


def _top(entries, limit=8):
    by_label = {}
    for start, end, label, count in entries:
        total = by_label.setdefault(label, [0.0, 0])
        total[0] += (end - start) * 1000
        total[1] += count
    top = sorted(by_label.items(), key=lambda item: -item[1][0])[:limit]
    return [{"statement": label, "ms": round(ms, 1), "count": count} for label, (ms, count) in top]


def summarize_held(entries, interval):
    """Statements inside the held interval, and the work after its commit.

    After the commit the publication sweeps unreachable edge payloads and
    prunes superseded generations (P2-3), each in its own transaction, without
    catalog_state; ``post_commit_ms`` is that work's wall time.
    """
    lo, hi = interval
    inside = [e for e in entries if e[0] >= lo and e[1] <= hi and e[2] not in ("COMMIT", "ROLLBACK")]
    after = [e for e in entries if e[0] >= hi]
    return {
        "statements_held": sum(count for _s, _e, _l, count in inside),
        "held_top": _top(inside),
        "post_commit_ms": round((after[-1][1] - hi) * 1000, 1) if after else 0.0,
        "post_commit_top": _top([e for e in after if e[2] not in ("COMMIT", "ROLLBACK")], 4),
    }


class Bridge:
    """ProviderCatalogBridge double serving a prepared raw catalogue."""

    def __init__(self, raw):
        self.raw = raw

    def list_servers(self):
        return [self.require_server(stub_host.SERVER_ID)]

    def require_server(self, server_id):
        return {"server_id": server_id, "name": "Main", "provider_type": "navidrome",
                "is_default": True, "supported": True}

    def fetch_catalog(self, _server_id):
        return self.raw


def raw_catalog(track_ids, bumps):
    """Navidrome-shaped catalogue; ``bumps`` maps track id -> media size offset."""
    tracks = []
    for index, track_id in enumerate(track_ids, start=1):
        artist = index % 6000
        tracks.append({
            "id": track_id,
            "title": f"Song {index}",
            "album": f"Album {index % 12000 + 1}",
            "albumId": f"al-{index % 12000 + 1}",
            "artist": f"Artist {artist}",
            "artistId": f"ar-{artist}",
            "track": index % 14 + 1,
            "discNumber": 1,
            "duration": 180 + index % 120,
            "year": 2000 + index % 20,
            "genre": "Rock",
            "coverArt": f"ca-{index}",
            "suffix": "flac",
            "bitRate": 1011,
            "sampleRate": 44100,
            "channelCount": 2,
            "size": 31_000_000 + index + bumps.get(track_id, 0),
            "musicFolderId": "lib-1",
        })
    return {"libraries": [{"id": "lib-1", "name": "Music"}], "tracks": tracks}


def align_profiles(db, source, normalized):
    """Point the seeded profiles and edges at the catalogue's normalized media revisions.

    The edges keep matching their published profile, as a real edge
    publication leaves them; otherwise the post-publication sweep would delete
    every seeded edge.
    """
    buf = io.StringIO()
    for row in normalized["tracks"]:
        buf.write(f"{row['track_id']}\tcatalog-media:{row['media_fp']}\n")
    buf.seek(0)
    cur = db.cursor()
    cur.execute("CREATE TEMP TABLE bench_revisions (track_id TEXT PRIMARY KEY, revision TEXT) "
                "ON COMMIT DROP")
    cur.copy_expert("COPY bench_revisions (track_id, revision) FROM STDIN", buf)
    for table_name in ("published_source_profiles", "source_profiles", "edge_profiles"):
        cur.execute(
            f"""UPDATE {T}{table_name} p SET media_signature=r.revision
                  FROM bench_revisions r
                 WHERE p.catalog_instance_id=%s AND p.track_id=r.track_id""",
            (source,),
        )
    db.commit()
    cur.close()


def counts(aux, source):
    aux.execute(
        f"""SELECT (SELECT count(*) FROM {T}published_source_profiles WHERE catalog_instance_id=%s),
                   (SELECT count(*) FROM {T}profile_changes
                     WHERE catalog_instance_id=%s AND operation='delete'),
                   (SELECT count(*) FROM {T}source_profiles
                     WHERE catalog_instance_id=%s AND status='stale')""",
        (source, source, source),
    )
    return aux.fetchone()


def refresh(catalog, db, aux, source, raw):
    before = counts(aux, source)
    LOG.clear()
    with HoldWatch(source) as watch:
        t0 = time.perf_counter()
        result = catalog.refresh_catalog(stub_host.SERVER_ID, db=db, bridge=Bridge(raw))
        elapsed = time.perf_counter() - t0
    after = counts(aux, source)
    entries = list(LOG)
    intervals = held_intervals(entries)
    longest = max(intervals, key=lambda iv: iv[1] - iv[0]) if intervals else None
    out = {
        "elapsed_s": round(elapsed, 2),
        "hold_ms": round(max(watch.holds_ms), 1) if watch.holds_ms else 0.0,
        "hold_intervals": len(watch.holds_ms),
        "probe_samples": watch.samples,
        "instrumented_hold_ms": round((longest[1] - longest[0]) * 1000, 1) if longest else None,
        "statements_total": sum(count for _s, _e, _l, count in entries),
        "generation": result.get("generation"),
        "catalog_changes": result.get("changes"),
        "withdrawn": (after[1] - before[1]),
        "published_after": after[0],
        "stale_attempts_added": after[2] - before[2],
    }
    if longest:
        out.update(summarize_held(entries, longest))
    return out


def main():
    parser = argparse.ArgumentParser(description="catalogue publication lock bench")
    parser.add_argument("--changed", type=int, default=20_000,
                        help="published tracks whose media changes per run (default 20000)")
    parser.add_argument("--runs", type=int, default=3, help="measured refreshes (default 3)")
    args = parser.parse_args()

    stub_host.load_plugin()
    from plugins.LumaeAnalysis import catalog

    aux = stub_host.aux_cursor()
    source = stub_host.default_source(aux)
    aux.execute(f"SELECT published_generation FROM {T}catalog_state WHERE catalog_instance_id=%s",
                (source,))
    generation = aux.fetchone()[0]
    aux.execute(
        f"SELECT track_id FROM {T}catalog_tracks WHERE catalog_instance_id=%s "
        "AND published_generation=%s AND available ORDER BY track_id",
        (source, generation),
    )
    track_ids = [row[0] for row in aux.fetchall()]
    aux.execute(
        f"SELECT track_id FROM {T}published_source_profiles WHERE catalog_instance_id=%s "
        "ORDER BY track_id",
        (source,),
    )
    published = [row[0] for row in aux.fetchall()]
    if len(published) < args.changed * args.runs:
        sys.exit(f"{len(published)} published profiles; need --changed x --runs")

    db = stub_host.connect(connection_factory=TimedConnection, cursor_factory=TimedCursor)
    bumps = {}
    base = raw_catalog(track_ids, bumps)
    align_profiles(db, source, catalog.normalize_provider_catalog(base, "navidrome"))
    result = {
        "bench": "catalog_lock",
        "tracks": len(track_ids),
        "published_profiles": len(published),
        "changed_per_run": args.changed,
        "setup": refresh(catalog, db, aux, source, base),
        "runs": [],
    }
    for run in range(args.runs):
        for track_id in published[run * args.changed:(run + 1) * args.changed]:
            bumps[track_id] = 1_000_000 * (run + 1)
        result["runs"].append(refresh(catalog, db, aux, source, raw_catalog(track_ids, bumps)))
        print(f"run {run + 1}: {result['runs'][-1]}", file=sys.stderr, flush=True)
    holds = [run["hold_ms"] for run in result["runs"]]
    result["hold_ms"] = {"median": round(statistics.median(holds), 1), "max": max(holds)}
    db.close()
    stub_host.emit(result)


if __name__ == "__main__":
    main()
