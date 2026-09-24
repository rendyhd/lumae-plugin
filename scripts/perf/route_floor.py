"""Diagnostic: ``/api/catalog/health`` floor with its coverage queries stubbed.

Replaces ``catalog_readiness._coverage`` and ``_link_coverage`` with constant
answers, so the remaining latency is the route's fixed cost (core detection,
server summaries, JSON). Compare with ``route_bench.py`` to see how much of
health latency is the coverage SQL. Usage: ``route_floor.py [N]``.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
import route_bench  # noqa: E402


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    stub_host.load_plugin()
    from plugins.LumaeAnalysis import catalog_readiness as cr

    coverage = {"eligible_track_count": 132000, "mapped_track_count": 76000,
                "missing_mapping_count": 56000, "chromaprint_track_count": 76000,
                "chromaprint_missing_count": 0, "chromaprint_coverage": 1.0,
                "latest_chromaprint_at_unix": 0.0}
    links = {"ready_link_count": 76000, "pending_link_count": 0, "suspect_link_count": 14000,
             "missing_link_count": 56000, "evidence_complete_link_count": 62000,
             "verified_link_count": 62000, "provisional_link_count": 0,
             "usable_analysis_coverage": 0.57}
    cr._coverage = lambda db, source: dict(coverage)
    cr._link_coverage = lambda db, source, n=0: dict(links)
    stub_host.emit({"bench": "route_floor", "ping_delay_s": stub_host.PING_DELAY_S,
                    "routes": route_bench.bench_routes(["/api/catalog/health"], n)})


if __name__ == "__main__":
    main()
