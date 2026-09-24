import json, sys, threading, time
sys.path.insert(0, '.')
import stub_host
T = "plugin_lumae_analysis__"
def worker(n, singleton, principal, out):
    db = stub_host.connect(); cur = db.cursor(); lat = []
    for i in range(n):
        t0 = time.perf_counter()
        if singleton:
            cur.execute(f"UPDATE {T}collection_feed_state SET head_seq=head_seq+1 WHERE singleton=1 RETURNING head_seq")
            seq = cur.fetchone()[0]
            cur.execute(f"INSERT INTO {T}collection_changes (seq, principal, collection_id, entity_kind, entity_id, operation, payload) VALUES (%s,%s,'c','collection','c','upsert','{{}}')", (seq, principal))
        else:
            cur.execute(f"INSERT INTO {T}collection_changes (principal, collection_id, entity_kind, entity_id, operation, payload) VALUES (%s,'c','collection','c','upsert','{{}}')", (principal,))
        db.commit(); lat.append((time.perf_counter()-t0)*1000)
    out.extend(lat); db.close()
for singleton in (False, True):
    for threads in (1, 8):
        out = []; ts = [threading.Thread(target=worker, args=(300, singleton, f"u{k}", out)) for k in range(threads)]
        t0 = time.perf_counter(); [t.start() for t in ts]; [t.join() for t in ts]; el = time.perf_counter()-t0
        out.sort()
        print(json.dumps({"singleton": singleton, "threads": threads, "tx_per_s": round(len(out)/el), "p50_ms": round(out[len(out)//2],2), "p95_ms": round(out[int(len(out)*0.95)],2)}))
