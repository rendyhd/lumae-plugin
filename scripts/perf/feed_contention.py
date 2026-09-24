"""Diagnostic: collections change-feed write contention.

Compares appending to ``collection_changes`` with the sequence default against
the singleton ``collection_feed_state`` head counter (one row every writer
updates), at 1 and 8 writer threads. Usage: ``feed_contention.py [N_PER_THREAD]``.
Leaves the rows it writes (principals ``perf-u*``); the fixture does not
depend on them.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402


def worker(n, singleton, principal, out):
    db = stub_host.connect()
    cur = db.cursor()
    latencies = []
    for _ in range(n):
        t0 = time.perf_counter()
        if singleton:
            cur.execute(f"UPDATE {T}collection_feed_state SET head_seq=head_seq+1 "
                        "WHERE singleton=1 RETURNING head_seq")
            seq = cur.fetchone()[0]
            cur.execute(f"INSERT INTO {T}collection_changes (seq, principal, collection_id, "
                        "entity_kind, entity_id, operation, payload) "
                        "VALUES (%s, %s, 'c', 'collection', 'c', 'upsert', '{}')", (seq, principal))
        else:
            cur.execute(f"INSERT INTO {T}collection_changes (principal, collection_id, entity_kind, "
                        "entity_id, operation, payload) "
                        "VALUES (%s, 'c', 'collection', 'c', 'upsert', '{}')", (principal,))
        db.commit()
        latencies.append((time.perf_counter() - t0) * 1000)
    out.extend(latencies)
    db.close()


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    cases = []
    aux = stub_host.aux_cursor()
    for singleton in (False, True):
        for threads in (1, 8):
            if singleton:
                # Start the singleton head past every sequence-assigned row.
                aux.execute(f"""UPDATE {T}collection_feed_state SET head_seq=GREATEST(head_seq,
                    (SELECT COALESCE(max(seq), 0) FROM {T}collection_changes)) WHERE singleton=1""")
            else:
                # Re-runs: move the sequence past rows a singleton case wrote.
                aux.execute(f"""SELECT setval(pg_get_serial_sequence('{T}collection_changes', 'seq'),
                    GREATEST((SELECT COALESCE(max(seq), 0) FROM {T}collection_changes), 1))""")
            out = []
            workers = [threading.Thread(target=worker, args=(n, singleton, f"perf-u{k}", out))
                       for k in range(threads)]
            t0 = time.perf_counter()
            for thread in workers:
                thread.start()
            for thread in workers:
                thread.join()
            elapsed = time.perf_counter() - t0
            cases.append({"singleton": singleton, "threads": threads,
                          "tx_per_s": round(len(out) / elapsed), **stub_host.summary_ms(out)})
    stub_host.emit({"bench": "feed_contention", "cases": cases})


if __name__ == "__main__":
    main()
