import json, statistics, sys, time
sys.path.insert(0, ".")
import stub_host
stub_host.load_plugin()
from plugins.LumaeAnalysis import collection_library as cl
cases = [("albums","",1),("albums","song",1),("tracks","",1),("tracks","",1000),("tracks","café song",1),("artists","",1),("all","artist 12",1)]
for scope, q, page in cases:
    ms = []
    for _ in range(3):
        t0 = time.perf_counter(); r = cl.browse_library(scope=scope, query=q, page=page, limit=36); ms.append((time.perf_counter()-t0)*1000)
    totals = {k: v["total"] for k, v in r["sections"].items()}
    print(json.dumps({"scope": scope, "q": q, "page": page, "median_ms": round(statistics.median(ms)), "totals": totals}))
