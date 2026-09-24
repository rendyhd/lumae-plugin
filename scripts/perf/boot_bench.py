"""Profile bootstrap v2 bench: session create and snapshot pages.

Measures, against the seeded fixture (94k profiles with real-size edges at
scale 1):

``create``      ``profile_bootstrap.create_session`` wall time, WAL written,
                snapshot table size, and the longest time the global creator
                advisory lock ``pg_advisory_lock(110094, 10)`` was held (sampled
                from ``pg_locks`` every ~2 ms by a side connection);
``pages``       ``snapshot_page`` wall time for every page of that session
                (server-side: includes the plugin-owned connection setup, not HTTP);
``conn_setup``  cost of the plugin-owned connection alone.

Budgets: create <=5 s with no global lock held >50 ms; page <=50 ms p95.

With production limits a real-edge snapshot larger than
``MAX_SNAPSHOT_BYTES`` (128 MiB) fails with ``bootstrap_snapshot_limit``. The
bench records that failure as the ``create`` result and, unless
``--no-lift``, repeats the create with the limits lifted (``create_lifted``) so
the full-size copy and its pages can still be measured; the page numbers then
come from the lifted session and say so.

Usage: ``boot_bench.py [--page-size 50] [--max-pages N] [--no-lift]``.
"""
import argparse
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402

LOCK_SQL = ("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted "
            "AND classid=110094 AND objid=10")


class LockWatch:
    """Sample the global creator advisory lock and record each held interval."""

    def __init__(self, interval_s=0.002):
        self.interval_s = interval_s
        self.holds_ms = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        cur = stub_host.aux_cursor()
        held_since = None
        while not self._stop.is_set():
            cur.execute(LOCK_SQL)
            now = time.perf_counter()
            held = cur.fetchone()[0] > 0
            if held and held_since is None:
                held_since = now
            elif not held and held_since is not None:
                self.holds_ms.append((now - held_since) * 1000)
                held_since = None
            time.sleep(self.interval_s)
        if held_since is not None:
            self.holds_ms.append((time.perf_counter() - held_since) * 1000)
        cur.connection.close()

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        time.sleep(0.02)
        self._stop.set()
        self._thread.join()


def create(pb, body, aux):
    start_lsn = stub_host.wal_lsn(aux)
    err = None
    with LockWatch() as watch:
        t0 = time.perf_counter()
        try:
            created = pb.create_session(body(page_size=ARGS.page_size))
        except pb.BootstrapError as exc:
            created, err = None, f"{exc.code}/{exc.status}"
        elapsed = time.perf_counter() - t0
    return created, {
        "elapsed_s": round(elapsed, 2),
        "error": err,
        "snapshot_rows": created["snapshot_count"] if created else None,
        "wal_mb": round(stub_host.wal_bytes_since(aux, start_lsn) / 1e6, 1),
        "snapshot_table_mb": round(
            stub_host.relation_bytes(aux, f"{T}profile_bootstrap_snapshot") / 1e6, 1),
        "global_lock_max_hold_ms": round(max(watch.holds_ms), 1) if watch.holds_ms else 0.0,
    }


def pages(pb, body, created):
    samples, total_bytes = [], 0
    token, nxt = created["session_token"], None
    import json

    while True:
        t0 = time.perf_counter()
        page = pb.snapshot_page(body(session_token=token, **({"page_token": nxt} if nxt else {})))
        samples.append((time.perf_counter() - t0) * 1000)
        if len(samples) <= 3:
            total_bytes += len(json.dumps(page, separators=(",", ":")))
        nxt = page["next_page_token"]
        if not nxt or (ARGS.max_pages and len(samples) >= ARGS.max_pages):
            break
    return {**stub_host.summary_ms(samples), "pages": len(samples),
            "first_pages_avg_bytes": total_bytes // min(3, len(samples))}


def main():
    stub_host.load_plugin()
    from plugins.LumaeAnalysis import profile_bootstrap as pb

    aux = stub_host.aux_cursor()
    src = stub_host.default_source(aux)
    aux.execute(f"DELETE FROM {T}profile_bootstrap_sessions")  # stale sessions from earlier runs

    def body(**kw):
        return {"protocol_version": 2, "schema_version": 1,
                "transfer_contract": pb.TRANSFER_CONTRACT, "catalog_instance_id": src, **kw}

    conn_ms = []
    for _ in range(30):
        t0 = time.perf_counter()
        with pb._connection():
            pass
        conn_ms.append((time.perf_counter() - t0) * 1000)

    aux.execute(f"SELECT count(*) FROM {T}published_source_profiles WHERE catalog_instance_id=%s",
                (src,))
    result = {"bench": "bootstrap", "page_size": ARGS.page_size, "profiles": aux.fetchone()[0],
              "conn_setup_ms": stub_host.summary_ms(conn_ms)}
    created, result["create"] = create(pb, body, aux)
    if created is None and not ARGS.no_lift and "snapshot_limit" in (result["create"]["error"] or ""):
        pb.MAX_SNAPSHOT_BYTES = 1 << 62
        pb.MAX_SNAPSHOT_ROWS = 1 << 62
        created, result["create_lifted"] = create(pb, body, aux)
        result["pages_from"] = "create_lifted"
    else:
        result["pages_from"] = "create"
    if created:
        result["pages"] = pages(pb, body, created)
        pb.release_session(body(session_token=created["session_token"]))
    stub_host.emit(result)


ARGS = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="profile bootstrap v2 create/page bench")
    parser.add_argument("--page-size", type=int, default=50,
                        help="v2 page_size (1-500; default 50, the size advised with edges)")
    parser.add_argument("--max-pages", type=int, default=0, help="stop after N pages (0 = all)")
    parser.add_argument("--no-lift", action="store_true",
                        help="do not retry a snapshot-limit failure with the limits lifted")
    ARGS = parser.parse_args()
    main()
