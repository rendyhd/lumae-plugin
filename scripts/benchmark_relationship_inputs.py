"""Synthetic memory measurement of the real relationship input conversion.

This models a buffered DB cursor with distinct 200D binary vectors and metadata.
It measures conversion only, not provider SQL latency or a full matching run.
"""

import argparse
import importlib
import json
from pathlib import Path
import sys
import threading
import time
import types
import numpy as np
import psutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracks", type=int, default=100000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.tracks <= 500000:
        parser.error("tracks must be 1..500000")
    api = types.ModuleType("plugin.api")
    api.table = lambda name: "plugin_lumae_analysis__" + name
    api.get_db = lambda: None
    api.config = types.SimpleNamespace(
        APP_VERSION="v2.6.3", MEDIASERVER_TYPE="navidrome"
    )
    api.get_setting = lambda name, default=None: default
    api.logger = types.SimpleNamespace(
        warning=lambda *a, **kw: None, exception=lambda *a, **kw: None
    )
    sys.modules["plugin"] = types.ModuleType("plugin")
    sys.modules["plugin.api"] = api
    package = types.ModuleType("_lumae_memory_measurement")
    package.__path__ = [
        str(Path(__file__).resolve().parents[1] / "plugins" / "LumaeAnalysis")
    ]
    sys.modules[package.__name__] = package
    module = importlib.import_module(package.__name__ + ".catalog_enrichment")
    process = psutil.Process()
    initial = process.memory_info().rss
    peak = [initial]
    done = threading.Event()

    def sample():
        while not done.wait(0.01):
            peak[0] = max(peak[0], process.memory_info().rss)

    worker = threading.Thread(target=sample, daemon=True)
    worker.start()
    started = time.monotonic()
    vector = np.linspace(0.01, 1, 200, dtype=np.float32)
    rows = [
        (
            str(i),
            str(i // 10),
            f"Album {i//10}",
            f"Artist {i//50}",
            None,
            1,
            i % 10,
            None,
            {"_lumae": {"year": 2000 + i % 25}},
            None,
            {"energy": 0.5},
            (vector + i * 1e-7).astype("<f4").tobytes(),
            200,
        )
        for i in range(args.tracks)
    ]
    buffered = process.memory_info().rss

    class Cursor:
        def execute(self, *args):
            pass

        def fetchall(self):
            return rows

    conversion = time.monotonic()
    tracks = module._load_relationship_inputs(
        Cursor(),
        {
            "catalog_instance_id": "synthetic",
            "catalog": {"generation": 1},
            "analysis": {"generation": 1},
        },
    )
    final = process.memory_info().rss
    done.set()
    worker.join(1)
    report = {
        "kind": "synthetic-relationship-input-conversion",
        "tracks": len(tracks),
        "initial_rss_bytes": initial,
        "buffered_rows_rss_bytes": buffered,
        "final_rss_bytes": final,
        "peak_rss_bytes": max(peak[0], final),
        "conversion_seconds": time.monotonic() - conversion,
        "total_seconds": time.monotonic() - started,
        "limits": "Excludes PostgreSQL server, provider acquisition, entity construction and candidate scoring.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
