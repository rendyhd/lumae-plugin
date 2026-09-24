import json
import statistics
import sys
import time

sys.path.insert(0, ".")
import stub_host  # noqa: E402

stub_host.load_plugin()
from plugins.LumaeAnalysis import profile_bootstrap as pb  # noqa: E402
import psycopg2  # noqa: E402

aux = stub_host.connect()
aux.autocommit = True
ac = aux.cursor()
ac.execute("SELECT catalog_instance_id FROM plugin_lumae_analysis__catalog_sources")
SRC = ac.fetchone()[0]
label = sys.argv[1]


def lsn():
    ac.execute("SELECT pg_current_wal_lsn()")
    return ac.fetchone()[0]


def body(**kw):
    return {"protocol_version": 2, "schema_version": 1, "transfer_contract": pb.TRANSFER_CONTRACT,
            "catalog_instance_id": SRC, **kw}


# connection-setup cost of the plugin-owned connection (connect + 2 set_config + commit + close)
conn_ms = []
for _ in range(30):
    t0 = time.perf_counter()
    with pb._connection():
        pass
    conn_ms.append((time.perf_counter() - t0) * 1000)

l0 = lsn()
t0 = time.perf_counter()
err = None
try:
    created = pb.create_session(body(page_size=500))
except pb.BootstrapError as exc:
    created, err = None, f"{exc.code}/{exc.status}"
create_s = time.perf_counter() - t0
ac.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)", (l0,))
wal = int(ac.fetchone()[0])
ac.execute("SELECT pg_total_relation_size('plugin_lumae_analysis__profile_bootstrap_snapshot')")
snap_bytes = ac.fetchone()[0]

page_ms = []
if created:
    token, nxt = created["session_token"], None
    while True:
        t0 = time.perf_counter()
        page = pb.snapshot_page(body(session_token=token, **({"page_token": nxt} if nxt else {})))
        page_ms.append((time.perf_counter() - t0) * 1000)
        nxt = page["next_page_token"]
        if not nxt:
            break
    # second concurrent creator while a capture holds the global advisory lock
    pb.release_session(body(session_token=token))
print(json.dumps({
    "case": label, "env": "PG16.13 local private cluster, fsync on, synthetic",
    "plugin_conn_setup_ms_p50": round(statistics.median(conn_ms), 2),
    "create_session_s": round(create_s, 2), "error": err,
    "snapshot_rows": created["snapshot_count"] if created else None,
    "wal_mb": round(wal / 1e6, 1), "snapshot_table_mb": round(snap_bytes / 1e6, 1),
    "pages": len(page_ms),
    "page_ms_p50": round(statistics.median(page_ms), 1) if page_ms else None,
    "page_ms_p95": round(sorted(page_ms)[int(0.95 * (len(page_ms) - 1))], 1) if page_ms else None,
}))
