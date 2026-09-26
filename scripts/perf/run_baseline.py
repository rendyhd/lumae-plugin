"""Run the performance benches, write JSON results and check the plan budgets.

Each bench runs in its own process (so projection RSS is per scenario) and
prints one JSON line; this runner collects them with the environment and the
fixture description, evaluates the P0-3 budgets and writes ``--out``.

Examples::

    export LUMAE_PERF_DSN=postgresql://lumae_test@127.0.0.1:<port>/<db>
    python3 scripts/perf/seed.py --reset --scale 1
    python3 scripts/perf/run_baseline.py --out perf.json            # measure, report
    python3 scripts/perf/run_baseline.py --out perf.json --check    # fail on any miss
    python3 scripts/perf/run_baseline.py --check --only publication,health
    python3 scripts/perf/run_baseline.py --from perf.json --check --only projection_nochange

``--check`` exits 1 when a *selected* budget is missed or was not measured.
Without ``--only`` every budget is selected. Budgets outside ``--only`` are
still evaluated and reported, never failed. With ``--only`` and no
``--benches``, only the benches those budgets need are run.

Budgets are absolute. Every budget except ``publication`` depends on the
fixture scale: on a fixture below full scale (``scale < 1`` or fewer than 94,000
profiles) those budgets are NOT MEASURED, and so fail ``--check`` when
selected, unless ``--allow-scale`` is passed (they are then judged and
reported "at scale X"). ``publication`` instead requires >= 50,000 retained
events. ``health`` is NOT MEASURED unless ``PING_DELAY_S`` was 0, and a
route that logged an error (a rendered fallback) is NOT MEASURED.
"""
import argparse
import datetime
import json
import os
import platform
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))

# name: (budget text, benches it needs, scale-dependent, evaluator(results) -> (measured, met))
# ``met`` is True, False, or None for "not measured" (a selected budget that is
# not measured fails --check).
BUDGETS = {}
FULL_SCALE_PROFILES = 94_000
RETAINED_EVENTS = 50_000


def budget(name, text, benches, scale_dependent=True):
    def register(fn):
        BUDGETS[name] = (text, tuple(benches), scale_dependent, fn)
        return fn
    return register


def _route(results, route):
    return (((results.get("routes") or {}).get("routes") or {}).get(route) or {})


def _route_budget(results, route, limit, *, ping_free):
    stats = _route(results, route)
    p95 = stats.get("p95_ms")
    measured = {"p95_ms": p95, "http": stats.get("http")}
    if p95 is None:
        return {**measured, "error": (results.get("routes") or {}).get("error")}, None
    if stats.get("logged_errors"):
        # The route logged an error and rendered a fallback: not the production path.
        return {**measured, "logged_errors": stats["logged_errors"]}, None
    if ping_free:
        delay = (results.get("routes") or {}).get("ping_delay_s")
        if delay is None or float(delay) != 0:
            return {**measured, "ping_delay_s": delay,
                    "note": "health excludes the provider ping: run with PING_DELAY_S=0"}, None
    return measured, stats.get("http") == 200 and p95 <= limit


@budget("health", "/api/catalog/health <=50 ms p95 (provider ping excluded)", ["routes"])
def _health(results):
    measured, met = _route_budget(results, "/api/catalog/health", 50, ping_free=True)
    measured["api_health_p95_ms"] = _route(results, "/api/health").get("p95_ms")
    return measured, met


@budget("settings_status", "/settings/status <=100 ms p95", ["routes"])
def _settings(results):
    return _route_budget(results, "/settings/status", 100, ping_free=False)


@budget("publication", "publication critical section (complete_attempt) <=5 ms p95 at 50k retained events",
        ["publication"], scale_dependent=False)
def _publication(results):
    pub = results.get("publication") or {}
    p95 = (pub.get("complete_attempt_ms") or {}).get("p95_ms")
    retained = pub.get("retained_events_before")
    measured = {"p95_ms": p95, "retained_events": retained,
                "record_profile_change_commit_p95_ms":
                    (pub.get("record_profile_change_commit_ms") or {}).get("p95_ms")}
    if p95 is None:
        return {**measured, "error": pub.get("error")}, None
    if (retained or 0) < RETAINED_EVENTS:
        return {**measured, "note": f"needs >= {RETAINED_EVENTS} retained events"}, None
    return measured, p95 <= 5


@budget("projection_nochange", "projection with no change <=5 s and <=400 MB peak RSS",
        ["projection_nochange"])
def _proj_nochange(results):
    run = results.get("projection_nochange") or {}
    if run.get("elapsed_s") is None or run.get("peak_rss_mb") is None:
        return {"error": run.get("error")}, None
    return ({"elapsed_s": run["elapsed_s"], "peak_rss_mb": run["peak_rss_mb"],
             "unchanged": run.get("unchanged")},
            run["elapsed_s"] <= 5 and run["peak_rss_mb"] <= 400 and bool(run.get("unchanged")))


@budget("projection_delta", "projection with a 1-row delta <=10 s", ["projection_delta"])
def _proj_delta(results):
    run = results.get("projection_delta") or {}
    if run.get("elapsed_s") is None:
        return {"error": run.get("error")}, None
    return ({"elapsed_s": run["elapsed_s"], "peak_rss_mb": run.get("peak_rss_mb"),
             "wal_mb": run.get("wal_mb"), "changes": run.get("changes")},
            run["elapsed_s"] <= 10)


@budget("bootstrap_create", "v2 create at 94k <=5 s, no global lock held >50 ms", ["bootstrap"])
def _boot_create(results):
    boot = results.get("bootstrap") or {}
    create = boot.get("create") or {}
    if create.get("elapsed_s") is None:
        return {"error": boot.get("error")}, None
    hold = create.get("global_lock_max_hold_ms")
    measured = {"elapsed_s": create["elapsed_s"], "error": create.get("error"),
                "global_lock_max_hold_ms": hold,
                "profiles": boot.get("profiles"), "wal_mb": create.get("wal_mb")}
    lifted = boot.get("create_lifted")
    if lifted:
        measured["lifted_limits"] = {k: lifted.get(k) for k in
                                     ("elapsed_s", "global_lock_max_hold_ms", "wal_mb",
                                      "snapshot_table_mb", "error")}
    if hold is None:
        return {**measured, "note": "global lock hold was not sampled"}, None
    return measured, create.get("error") is None and create["elapsed_s"] <= 5 and hold <= 50


@budget("bootstrap_page", "v2 snapshot page <=50 ms p95 server-side", ["bootstrap"])
def _boot_page(results):
    boot = results.get("bootstrap") or {}
    pages = boot.get("pages") or {}
    p95 = pages.get("p95_ms")
    measured = {"p95_ms": p95, "page_size": boot.get("page_size"),
                "pages_from": boot.get("pages_from"), "pages": pages.get("pages")}
    return measured, (p95 <= 50) if p95 is not None else None


CORE_BENCHES = ("routes", "publication", "projection_nochange", "projection_delta", "bootstrap")
DIAGNOSTIC_BENCHES = ("route_floor", "boot_concurrency", "feed_contention")


def bench_command(name, args):
    py = sys.executable
    return {
        "routes": [py, "route_bench.py", "/api/health,/api/catalog/health,/settings/status",
                   str(args.route_n)],
        "route_floor": [py, "route_floor.py", str(args.route_n)],
        "publication": [py, "pub_bench.py", str(args.pub_n)],
        "projection_full": [py, "proj_bench.py", "full"],
        "projection_nochange": [py, "proj_bench.py", "nochange"],
        "projection_delta": [py, "proj_bench.py", "delta"],
        "bootstrap": [py, "boot_bench.py", "--page-size", str(args.page_size),
                      "--max-pages", str(args.max_pages)],
        "boot_concurrency": [py, "boot_concurrency.py", "0.5", str(args.page_size)],
        "feed_contention": [py, "feed_contention.py", "300"],
    }[name]


def run_bench(name, args):
    print(f"[run_baseline] {name} ...", file=sys.stderr, flush=True)
    proc = subprocess.run(bench_command(name, args), cwd=HERE, capture_output=True, text=True)
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if proc.returncode != 0 or not lines:
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-5:]
        print(f"[run_baseline] {name} FAILED: {' | '.join(tail)}", file=sys.stderr, flush=True)
        return {"error": "\n".join(tail), "returncode": proc.returncode}
    try:
        result = json.loads(lines[-1])
    except ValueError:
        print(f"[run_baseline] {name} FAILED: last line is not JSON: {lines[-1][:200]}",
              file=sys.stderr, flush=True)
        return {"error": f"unparseable output: {lines[-1][:500]}", "returncode": proc.returncode}
    if not isinstance(result, dict):
        return {"error": f"unexpected output: {lines[-1][:500]}", "returncode": proc.returncode}
    print(f"[run_baseline] {name}: {lines[-1][:300]}", file=sys.stderr, flush=True)
    return result


def _query(sql):
    import psycopg2

    with psycopg2.connect(os.environ["LUMAE_PERF_DSN"]) as db, db.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def environment():
    env = {"timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
           "python": platform.python_version(), "platform": platform.platform(),
           "cpus": os.cpu_count()}
    try:
        with open("/proc/cpuinfo") as handle:
            env["cpu_model"] = next((line.split(":", 1)[1].strip() for line in handle
                                     if line.startswith("model name")), None)
        with open("/proc/meminfo") as handle:
            kb = int(next(line for line in handle if line.startswith("MemTotal")).split()[1])
            env["ram_gb"] = round(kb / 1024 / 1024, 1)
    except (OSError, StopIteration, ValueError):
        pass
    try:
        env["git_sha"] = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True,
                                        text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        env["git_sha"] = None
    try:
        env["postgres"] = _query("SELECT version()")[0][0]
        env["postgres_settings"] = dict(_query(
            "SELECT name, setting || COALESCE(unit, '') FROM pg_settings WHERE name IN "
            "('shared_buffers','work_mem','fsync','synchronous_commit','max_wal_size',"
            "'effective_cache_size','wal_level')"))
    except Exception as exc:  # environment only; never fail the run on it
        env["postgres_error"] = str(exc)
    return env


def fixture():
    try:
        rows = _query("SELECT value FROM lumae_perf_fixture WHERE key='fixture'")
        return rows[0][0] if rows else None
    except Exception:
        return None


def projection_generation():
    try:
        rows = _query("SELECT max(projection_generation) FROM plugin_lumae_analysis__analysis_state")
        return int(rows[0][0] or 0)
    except Exception:
        return 0


def fixture_scale(data):
    """(scale, profiles) of the measured fixture, or None where unknown."""
    fixture_info = data.get("fixture") or {}
    scale = fixture_info.get("scale")
    profiles = (fixture_info.get("counts") or {}).get("profiles")
    if profiles is None:
        profiles = ((data.get("benches") or {}).get("bootstrap") or {}).get("profiles")
    return scale, profiles


def evaluate(data, allow_scale=False):
    results = data.get("benches") or {}
    scale, profiles = fixture_scale(data)
    full_scale = (scale is not None and float(scale) >= 1
                  and profiles is not None and int(profiles) >= FULL_SCALE_PROFILES)
    out = {}
    for name, (text, _benches, scale_dependent, fn) in BUDGETS.items():
        try:
            measured, met = fn(results)
        except Exception as exc:  # a malformed bench result is "not measured"
            measured, met = {"error": repr(exc)}, None
        entry = {"budget": text, "measured": measured, "met": met}
        if scale_dependent and not full_scale:
            if allow_scale:
                entry["at_scale"] = scale
            else:
                entry["measured_met"] = met
                entry["met"] = None
                entry["not_measured_reason"] = (
                    f"fixture scale {scale} / {profiles} profiles is below full scale "
                    f"(1.0 / {FULL_SCALE_PROFILES}); pass --allow-scale to judge it anyway")
        out[name] = entry
    return out


def report(budgets, selected):
    width = max(len(name) for name in budgets)
    failed = []
    for name, entry in budgets.items():
        met = entry["met"]
        status = "PASS" if met else ("MISS" if met is False else "NOT MEASURED")
        if entry.get("at_scale") is not None and met is not None:
            status += f" at scale {entry['at_scale']}"
        elif entry.get("not_measured_reason"):
            status += " (scale)"
        is_selected = name in selected
        if is_selected and not met:
            failed.append(name)
        tag = "" if is_selected or met else " (reported, not selected)"
        print(f"{name:<{width}}  {status:<20} {json.dumps(entry['measured'], sort_keys=True)}{tag}")
        print(f"{'':<{width}}  budget: {entry['budget']}")
        if entry.get("not_measured_reason"):
            print(f"{'':<{width}}  {entry['not_measured_reason']}")
    return failed


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", help="write the JSON results here")
    parser.add_argument("--from", dest="from_file", help="evaluate an existing results file instead of running")
    parser.add_argument("--check", action="store_true", help="exit 1 if a selected budget is missed")
    parser.add_argument("--only", default="", help=f"comma-separated budgets to select: {','.join(BUDGETS)}")
    parser.add_argument("--benches", default="",
                        help=f"comma-separated benches to run (default: all core, or those --only needs); "
                             f"core {','.join(CORE_BENCHES)}; diagnostics {','.join(DIAGNOSTIC_BENCHES)}")
    parser.add_argument("--allow-scale", action="store_true",
                        help="judge scale-dependent budgets on a fixture below full scale "
                             "(reported 'at scale X'); otherwise they are NOT MEASURED")
    parser.add_argument("--diagnostics", action="store_true", help="also run the diagnostic benches")
    parser.add_argument("--route-n", type=int, default=30, help="timed requests per route (default 30)")
    parser.add_argument("--pub-n", type=int, default=40, help="publication iterations (default 40)")
    parser.add_argument("--page-size", type=int, default=50, help="bootstrap page_size (default 50)")
    parser.add_argument("--max-pages", type=int, default=0, help="bootstrap pages to time (0 = all)")
    args = parser.parse_args()

    only = [name for name in args.only.split(",") if name]
    unknown = [name for name in only if name not in BUDGETS]
    if unknown:
        parser.error(f"unknown budget(s) {unknown}; known: {', '.join(BUDGETS)}")
    selected = set(only or BUDGETS)

    if args.from_file:
        with open(args.from_file) as handle:
            data = json.load(handle)
        results = data.get("benches") or {}
    else:
        if not os.environ.get("LUMAE_PERF_DSN"):
            parser.error("LUMAE_PERF_DSN is not set")
        if args.benches:
            benches = [name for name in args.benches.split(",") if name]
        elif only:
            benches = [b for b in CORE_BENCHES if any(b in BUDGETS[n][1] for n in only)]
        else:
            benches = list(CORE_BENCHES)
        if args.diagnostics:
            benches += [b for b in DIAGNOSTIC_BENCHES if b not in benches]
        data = {"environment": environment(), "fixture": fixture(),
                "options": {"route_n": args.route_n, "pub_n": args.pub_n,
                            "page_size": args.page_size, "max_pages": args.max_pages,
                            "ping_delay_s": float(os.environ.get("PING_DELAY_S", "0") or 0)},
                "benches": {}}
        results = data["benches"]
        if any(b.startswith("projection_") for b in benches) and projection_generation() == 0:
            results["projection_full"] = run_bench("projection_full", args)
        for name in benches:
            results[name] = run_bench(name, args)

    data["budgets"] = evaluate(data, allow_scale=args.allow_scale)
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        print(f"[run_baseline] wrote {args.out}", file=sys.stderr)
    failed = report(data["budgets"], selected)
    if args.check and failed:
        print(f"[run_baseline] --check: {len(failed)} selected budget(s) not met: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
