import sys, time, json
sys.path.insert(0, '.')
import stub_host
mod = stub_host.load_plugin()
db = stub_host.counting_connect()
aux = stub_host.connect(); aux.autocommit=True; ac=aux.cursor()
ac.execute("SELECT pg_current_wal_lsn()"); l0=ac.fetchone()[0]
t0 = time.perf_counter(); mod.migrate(db); el = time.perf_counter()-t0
ac.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)", (l0,))
print(json.dumps({"migrate_populated_s": round(el,2), "statements": stub_host.STATE.statements, "wal_mb": round(int(ac.fetchone()[0])/1e6,2)}))
