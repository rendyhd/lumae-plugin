"""Latency of plugin HTTP routes through a Flask test client.

Usage: ``route_bench.py [ROUTES] [N]`` where ROUTES is comma separated
(default ``/api/health,/api/catalog/health,/settings/status``) and N the number
of timed requests per route after one warm-up (default 30).

The provider ping inside ``/api/catalog/health`` is stubbed; with
``PING_DELAY_S=0`` (default) the numbers exclude the provider ping. Prints one
JSON object ``{"bench": "routes", "routes": {route: stats}}``.
"""
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402

DEFAULT_ROUTES = "/api/health,/api/catalog/health,/settings/status"


def bench_routes(routes, n):
    mod = stub_host.load_plugin()
    from flask import Flask

    app = Flask(__name__)
    app.register_blueprint(mod.bp)
    client = app.test_client()
    results = {}
    for route in routes:
        samples, stmts, pings = [], [], []
        errors_before = len(stub_host.STATE.logged_errors)
        resp = client.get(route)  # warm
        for _ in range(n):
            s0, p0 = stub_host.STATE.statements, stub_host.STATE.ping_calls
            t0 = time.perf_counter()
            resp = client.get(route)
            samples.append((time.perf_counter() - t0) * 1000)
            stmts.append(stub_host.STATE.statements - s0)
            pings.append(stub_host.STATE.ping_calls - p0)
        body = resp.get_json(silent=True) or {}
        extra = {}
        if route.endswith("/catalog/health"):
            srv = (body.get("servers") or [{}])[0]
            readiness = srv.get("v3_readiness") or {}
            extra = {"eligible": readiness.get("eligible_track_count"),
                     "ready_links": readiness.get("ready_link_count"),
                     "readiness_status": readiness.get("status")}
        results[route] = {
            "http": resp.status_code,
            **stub_host.summary_ms(samples),
            "mean_ms": round(statistics.mean(samples), 2),
            "stmts_per_req": stmts[-1] if stmts else None,
            "upstream_pings": pings[-1] if pings else None,
            "bytes": len(resp.data),
            "logged_errors": sorted(set(stub_host.STATE.logged_errors[errors_before:]))[:5],
            **extra,
        }
    return results


def main():
    routes = (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ROUTES).split(",")
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    stub_host.emit({"bench": "routes", "ping_delay_s": stub_host.PING_DELAY_S,
                    "routes": bench_routes(routes, n)})


if __name__ == "__main__":
    main()
