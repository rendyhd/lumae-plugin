"""Workbench library browse/search latency (LUM-016, P3-5c).

Usage: ``wb_bench.py [N]`` (default 5 timed runs per case after one warm-up)
against the ``seed.py`` fixture in ``LUMAE_PERF_DSN``. Calls
``collection_library.browse_library`` directly on one connection and prints
one JSON line per case with the median and max in ms and the section totals.

The deep page is page 1000 of 36 tracks by title: through the legacy ``page``
parameter (OFFSET), and, when the plugin has keyset paging, through a
``cursor`` positioned at the same row.
"""
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402

CASES = [
    ("browse albums", dict(scope="albums")),
    ("browse tracks", dict(scope="tracks")),
    ("browse artists", dict(scope="artists")),
    ("search all 'artist 12'", dict(scope="all", query="artist 12")),
    ("search all 'love'", dict(scope="all", query="love")),
    ("search all 'e5d0'", dict(scope="all", query="e5d0")),
    ("search tracks 'café song'", dict(scope="tracks", query="café song")),
    ("search albums 'song'", dict(scope="albums", query="song")),
    ("deep page 1000 (legacy page)", dict(scope="tracks", page=1000)),
]


def _deep_cursor(cl, catalog):
    if not hasattr(cl, "encode_cursor"):
        return None
    cur = stub_host.get_db().cursor()
    cur.execute(
        f"SELECT lower(title), track_id FROM {cl.table('catalog_tracks')} "
        "WHERE catalog_instance_id=%s AND available ORDER BY lower(title), track_id "
        "OFFSET %s LIMIT 1",
        (catalog, 999 * 36 - 1),
    )
    key, ident = cur.fetchone()
    cur.close()
    return cl.encode_cursor(key, ident)


def main():
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    stub_host.load_plugin()
    from plugins.LumaeAnalysis import collection_library as cl

    cases = list(CASES)
    cur = stub_host.get_db().cursor()
    catalog = stub_host.default_source(cur)
    cur.close()
    cursor = _deep_cursor(cl, catalog)
    if cursor:
        cases.append(("deep page 1000 (cursor)", dict(scope="tracks", cursor=cursor)))
    for label, kwargs in cases:
        kwargs = {"limit": 36, **kwargs}
        cl.browse_library(**kwargs)
        samples = []
        for _ in range(runs):
            t0 = time.perf_counter()
            result = cl.browse_library(**kwargs)
            samples.append((time.perf_counter() - t0) * 1000)
        print(json.dumps({
            "case": label,
            "median_ms": round(statistics.median(samples), 1),
            "max_ms": round(max(samples), 1),
            "totals": {k: v["total"] for k, v in result["sections"].items()},
            "first": [
                (v["items"][0].get("title") or "")[:20] if v["items"] else None
                for v in result["sections"].values()
            ],
        }))


if __name__ == "__main__":
    main()
