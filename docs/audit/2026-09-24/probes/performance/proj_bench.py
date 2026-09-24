import json
import resource
import sys
import time

sys.path.insert(0, ".")
import stub_host  # noqa: E402

mod = stub_host.load_plugin()
from plugins.LumaeAnalysis import catalog_analysis, core_v3  # noqa: E402


class Adapter(core_v3.AudioMuseV3Adapter):
    # Omit provider_module so the provider-identity guard (upstream ping) is skipped;
    # this isolates projection cost.
    provider_module = None


scenario = sys.argv[1]
aux = stub_host.connect()
aux.autocommit = True
ac = aux.cursor()
if scenario == "delta":
    ac.execute("UPDATE score SET tempo = tempo + 1 WHERE item_id='it-0000001'")


def lsn():
    ac.execute("SELECT pg_current_wal_lsn()")
    return ac.fetchone()[0]


def size(name):
    ac.execute("SELECT pg_total_relation_size(%s)", (name,))
    return ac.fetchone()[0]


rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
start_lsn = lsn()
db = stub_host.get_db()
t0 = time.perf_counter()
result = catalog_analysis.project_analysis(server_id="server-a", db=db, adapter=Adapter())
elapsed = time.perf_counter() - t0
end_lsn = lsn()
ac.execute("SELECT pg_wal_lsn_diff(%s, %s)", (end_lsn, start_lsn))
wal = int(ac.fetchone()[0])
rss1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
out = {
    "scenario": scenario,
    "env": "PG16.13 local, synthetic fixture, 4 vCPU, host conn options",
    "elapsed_s": round(elapsed, 2),
    "statements": stub_host.STATE.statements,
    "wal_mb": round(wal / 1e6, 1),
    "peak_rss_mb": round(rss1 / 1024, 1),
    "rss_growth_mb": round((rss1 - rss0) / 1024, 1),
    "generation": result["generation"],
    "changes": result["changes"],
    "unchanged": result.get("unchanged", False),
    "items": result["item_count"],
    "links": result["link_count"],
    "ready": result["ready_count"],
    "analysis_items_mb": round(size("plugin_lumae_analysis__analysis_items") / 1e6, 1),
    "links_mb": round(size("plugin_lumae_analysis__track_analysis_links") / 1e6, 1),
}
print(json.dumps(out))
