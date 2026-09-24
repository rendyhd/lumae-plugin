import json
import statistics
import sys
import time

sys.path.insert(0, ".")
import stub_host  # noqa: E402

mod = stub_host.load_plugin()
from flask import Flask  # noqa: E402

app = Flask(__name__)
app.register_blueprint(mod.bp)
client = app.test_client()
N = int(sys.argv[2]) if len(sys.argv) > 2 else 20
routes = sys.argv[1].split(",")


def pct(values, p):
    values = sorted(values)
    k = max(0, min(len(values) - 1, int(round(p / 100 * (len(values) - 1)))))
    return values[k]


for route in routes:
    samples, stmts, pings, size = [], [], [], 0
    client.get(route)  # warm
    for _ in range(N):
        s0, p0 = stub_host.STATE.statements, stub_host.STATE.ping_calls
        t0 = time.perf_counter()
        resp = client.get(route)
        samples.append((time.perf_counter() - t0) * 1000)
        stmts.append(stub_host.STATE.statements - s0)
        pings.append(stub_host.STATE.ping_calls - p0)
        size = len(resp.data)
    body = resp.get_json(silent=True) or {}
    extra = {}
    if route.endswith("/catalog/health"):
        srv = (body.get("servers") or [{}])[0]
        rd = srv.get("v3_readiness") or {}
        extra = {"eligible": rd.get("eligible_track_count"), "ready_links": rd.get("ready_link_count"),
                 "status": rd.get("status"), "blockers": rd.get("blockers")}
    print(json.dumps({"route": route, "http": resp.status_code, "n": N,
                      "p50_ms": round(statistics.median(samples), 1), "p95_ms": round(pct(samples, 95), 1),
                      "stmts_per_req": stmts[-1], "upstream_pings": pings[-1], "bytes": size,
                      "ping_delay_s": stub_host.PING_DELAY_S, **extra}))
