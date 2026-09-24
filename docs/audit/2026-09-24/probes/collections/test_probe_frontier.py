"""Audit probes for the LUM-004 global collection feed frontier (read-only w.r.t. repo)."""
import json
import random
import threading
import time
import uuid

import pytest

from test_collection_mutations_postgres import collection_api  # noqa: F401  (fixture)


def _drain(call, user, cursor, seen):
    while True:
        r = call("GET", f"/api/collections/changes?cursor={cursor}&limit=50", user=user)
        assert r.status_code == 200, r.get_json()
        body = r.get_json()
        for c in body["changes"]:
            seen.append(c["seq"])
        if not body["changes"]:
            return body["next_cursor"]
        cursor = body["next_cursor"]


def test_probe_no_skip_real_routes(collection_api, monkeypatch):
    """8 concurrent writers (distinct principals + one shared principal) + live readers."""
    manager, call, connect = collection_api
    original = manager._record_change

    def jitter(*args):
        original(*args)
        # widen the window between allocation and commit
        if random.random() < 0.3:
            time.sleep(random.random() * 0.004)

    monkeypatch.setattr(manager, "_record_change", jitter)
    users = [f"u{i}" for i in range(6)]
    errors = []
    stop = threading.Event()

    def writer(user, cid, n):
        try:
            r = call("POST", "/api/collections", {"id": cid, "name": cid}, user=user)
            assert r.status_code == 201, r.get_json()
            for j in range(n):
                if j % 7 == 0:
                    items = [{"kind": "track", "track_id": f"{cid}-b{j}-{k}"} for k in range(5)]
                    r = call("POST", f"/api/collections/{cid}/items/batch", {"items": items}, user=user)
                else:
                    r = call("PUT", f"/api/collections/{cid}/items/{uuid.uuid4()}",
                             {"kind": "track", "track_id": f"{cid}-{j}"}, user=user)
                assert r.status_code == 200, r.get_json()
        except Exception as exc:  # pragma: no cover
            errors.append(repr(exc))

    seen = {u: [] for u in ("u0", "u1")}

    def reader(user):
        cursor = 0
        while not stop.is_set():
            cursor = _drain(call, user, cursor, seen[user])
        _drain(call, user, cursor, seen[user])

    writers = [threading.Thread(target=writer, args=(u, f"{u}-c", 40)) for u in users]
    # two extra writers on the same principal u0, different collections
    writers += [threading.Thread(target=writer, args=("u0", f"u0-x{i}", 40)) for i in range(2)]
    readers = [threading.Thread(target=reader, args=(u,)) for u in seen]
    for t in readers + writers:
        t.start()
    for t in writers:
        t.join(120)
    stop.set()
    for t in readers:
        t.join(60)
    assert not errors, errors
    db = connect()
    with db.cursor() as cur:
        for user in seen:
            cur.execute(f"SELECT seq FROM {manager.collection_changes_table()} WHERE principal=%s ORDER BY seq",
                        (f"user:{user}",))
            expected = [r[0] for r in cur.fetchall()]
            got = seen[user]
            print(f"PROBE no-skip {user}: db_events={len(expected)} reader_events={len(got)} "
                  f"dups={len(got)-len(set(got))} missing={len(set(expected)-set(got))}")
            assert got == expected
        cur.execute(f"SELECT head_seq FROM {manager.collection_feed_state_table()}")
        head = cur.fetchone()[0]
        cur.execute(f"SELECT count(*), max(seq) FROM {manager.collection_changes_table()}")
        cnt, mx = cur.fetchone()
        print(f"PROBE frontier head={head} events={cnt} max_seq={mx} (gapless={head == cnt == mx})")
        assert head == cnt == mx
    db.rollback()
    db.close()


def _sim_tx(db, schema_tables, principal, cid, mode, key):
    collections, changes, feed, receipts = schema_tables
    with db.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (hash((principal, key)) & 0x7FFFFFFFFFFFFFFF,))
        cur.execute(f"SELECT deleted_at FROM {collections} WHERE principal=%s AND id=%s FOR UPDATE", (principal, cid))
        cur.execute(f"UPDATE {collections} SET revision=revision+1, updated_at=now() WHERE principal=%s AND id=%s",
                    (principal, cid))
        if mode == "frontier":
            cur.execute(f"UPDATE {feed} SET head_seq=head_seq+1 WHERE singleton=1 RETURNING head_seq")
            seq = cur.fetchone()[0]
            cur.execute(f"INSERT INTO {changes} (seq, principal, collection_id, entity_kind, entity_id, operation, payload)"
                        " VALUES (%s,%s,%s,'collection',%s,'upsert','{}'::jsonb)", (seq, principal, cid, cid))
        else:
            cur.execute(f"INSERT INTO {changes} (principal, collection_id, entity_kind, entity_id, operation, payload)"
                        " VALUES (%s,%s,'collection',%s,'upsert','{}'::jsonb)", (principal, cid, cid))
        cur.execute(f"INSERT INTO {receipts} (principal, idempotency_key, response_payload, status_code,"
                    " request_fingerprint, fingerprint_version) VALUES (%s,%s,'{}'::jsonb,200,'x',1)",
                    (principal, key))
    db.commit()


@pytest.mark.parametrize("mode", ["legacy_bigserial", "frontier"])
@pytest.mark.parametrize("writers", [1, 4, 16])
def test_probe_throughput_sql(collection_api, mode, writers):
    """DB-level throughput of the mutation transaction shape, distinct principals, fsync on."""
    manager, call, connect = collection_api
    tables = (manager.collections_table(), manager.collection_changes_table(),
              manager.collection_feed_state_table(), manager.collection_mutations_table())
    setup = connect()
    with setup.cursor() as cur:
        for w in range(writers):
            cur.execute(f"INSERT INTO {tables[0]} (principal, id, name) VALUES (%s,%s,'x')", (f"p{w}", "c"))
    setup.commit()
    setup.close()
    duration = 3.0
    counts = [0] * writers
    lat = []
    barrier = threading.Barrier(writers)

    def run(w):
        db = connect()
        barrier.wait()
        end = time.perf_counter() + duration
        n = 0
        while time.perf_counter() < end:
            t0 = time.perf_counter()
            _sim_tx(db, tables, f"p{w}", "c", "frontier" if mode == "frontier" else "legacy", f"k{w}-{n}")
            lat.append(time.perf_counter() - t0)
            n += 1
        counts[w] = n
        db.close()

    threads = [threading.Thread(target=run, args=(w,)) for w in range(writers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    lat.sort()
    total = sum(counts)
    p50 = lat[len(lat) // 2] * 1000
    p99 = lat[int(len(lat) * 0.99)] * 1000
    print(f"PROBE throughput mode={mode} writers={writers} tx/s={total / duration:.0f} p50={p50:.2f}ms p99={p99:.2f}ms")


def test_probe_restore_blocks_other_principals(collection_api):
    """A large restore holds the global frontier while it emits every event."""
    manager, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "bobc", "name": "b"}, user="bob").status_code == 201
    n_items = 20_000
    backup = manager._backup_envelope([
        {"name": "Big", "items": [{"kind": "track", "track_id": f"t{i}"} for i in range(n_items)]},
    ], "personal")
    result = {}

    def restore():
        t0 = time.perf_counter()
        result["restore"] = call("POST", "/api/collections/restore", backup, key="big", user="alice")
        result["restore_s"] = time.perf_counter() - t0

    t = threading.Thread(target=restore)
    t.start()
    # wait until the restore holds the frontier (head is locked -> NOWAIT fails)
    probe = connect()
    t_locked = None
    for _ in range(4000):
        with probe.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '1ms'")
            try:
                cur.execute(f"SELECT 1 FROM {manager.collection_feed_state_table()} WHERE singleton=1 FOR UPDATE NOWAIT")
                probe.rollback()
            except Exception:
                probe.rollback()
                t_locked = time.perf_counter()
                break
        time.sleep(0.005)
    probe.close()
    assert t_locked, "restore never reached the frontier"
    t0 = time.perf_counter()
    try:
        r = call("PATCH", "/api/collections/bobc", {"name": "renamed"}, user="bob")
        bob_status = r.status_code
    except Exception as exc:
        bob_status = f"EXC:{type(exc).__name__}:{str(exc).splitlines()[0][:80]}"
    bob_latency = time.perf_counter() - t0
    t.join(300)
    print(f"PROBE restore items={n_items} restore_total={result['restore_s']:.2f}s "
          f"status={result['restore'].status_code} bob_patch_status={bob_status} "
          f"bob_patch_latency_while_restore_holds_frontier={bob_latency:.2f}s")
    db = connect()
    with db.cursor() as cur:
        cur.execute("SELECT n_tup_upd, n_tup_hot_upd, n_dead_tup FROM pg_stat_user_tables WHERE relname=%s",
                    (manager.collection_feed_state_table(),))
        print("PROBE feed_state stats (upd, hot_upd, dead):", cur.fetchone())
        cur.execute("SELECT pg_relation_size(%s::regclass)", (manager.collection_feed_state_table(),))
        print("PROBE feed_state relation bytes after restore:", cur.fetchone()[0])
    db.rollback()
    db.close()
