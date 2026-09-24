"""Analysis projection bench: one ``catalog_analysis.project_analysis`` run.

Scenarios (positional argument):

``full``      first projection of the seeded fixture (seed.py runs this);
``nochange``  re-projection with no AudioMuse change (budget: <=5 s, <=400 MB RSS);
``delta``     one ``score`` row changed first, then projected (budget: <=10 s).

Run each scenario in its own process: ``peak_rss_mb`` is the process peak
(``VmHWM`` from ``/proc/self/status``), which includes importing the plugin.
``delta`` mutates the fixture (it writes a new projection generation).
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402

SCENARIOS = ("full", "nochange", "delta")


def main():
    scenario = sys.argv[1] if len(sys.argv) > 1 else "nochange"
    if scenario not in SCENARIOS:
        sys.exit(f"scenario must be one of {SCENARIOS}")
    stub_host.load_plugin()
    from plugins.LumaeAnalysis import catalog_analysis, core_v3

    class Adapter(core_v3.AudioMuseV3Adapter):
        # Omit provider_module so the provider-identity guard (upstream ping)
        # is skipped; this isolates projection cost.
        provider_module = None

    aux = stub_host.aux_cursor()
    if scenario == "delta":
        aux.execute("SELECT min(item_id) FROM score")
        aux.execute("UPDATE score SET tempo = tempo + 1 WHERE item_id=%s", (aux.fetchone()[0],))

    rss0 = stub_host.peak_rss_mb()
    start_lsn = stub_host.wal_lsn(aux)
    db = stub_host.get_db()
    t0 = time.perf_counter()
    result = catalog_analysis.project_analysis(server_id=stub_host.SERVER_ID, db=db,
                                               adapter=Adapter())
    elapsed = time.perf_counter() - t0
    wal = stub_host.wal_bytes_since(aux, start_lsn)
    rss1 = stub_host.peak_rss_mb()
    stub_host.emit({
        "bench": "projection",
        "scenario": scenario,
        "elapsed_s": round(elapsed, 2),
        "statements": stub_host.STATE.statements,
        "wal_mb": round(wal / 1e6, 1),
        "peak_rss_mb": rss1,
        "rss_growth_mb": round(rss1 - rss0, 1),
        "generation": result.get("generation"),
        "changes": result.get("changes"),
        "unchanged": result.get("unchanged", False),
        "items": result.get("item_count"),
        "links": result.get("link_count"),
        "ready": result.get("ready_count"),
        "analysis_items_mb": round(stub_host.relation_bytes(aux, f"{T}analysis_items") / 1e6, 1),
        "links_mb": round(stub_host.relation_bytes(aux, f"{T}track_analysis_links") / 1e6, 1),
    })


if __name__ == "__main__":
    main()
