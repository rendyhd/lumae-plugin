"""P3-8 / LUM-018: cost of analysing a file in the isolated child process.

Synthesizes stereo 44.1 kHz FLAC files and times the real analyzers in-process
and through ``analysis_isolation.run_isolated`` (the pooled worker), then
prints one JSON object:

* ``worker_start_s``: the one-time start of a worker (the first call's extra time);
* ``per_file_overhead_ms``: median over ``--files`` small files of
  (isolated - in-process), waveform and edge, with a warm worker;
* ``typical``: medians for one ``--typical-seconds`` track, both ways;
* ``child_rss_mb``: the worker's resident memory after start and its peak.

No database is needed. The plugin modules are loaded through a stand-in
package, as the child itself loads them.

    python3 scripts/perf/isolation_bench.py --files 20 --typical-seconds 240
"""
import argparse
import json
import statistics
import sys
import tempfile
import time
import types
from pathlib import Path

import numpy as np

PLUGIN_DIR = Path(__file__).resolve().parents[2] / "plugins" / "LumaeAnalysis"
package = types.ModuleType("lumae_bench")
package.__path__ = [str(PLUGIN_DIR)]
sys.modules["lumae_bench"] = package

from lumae_bench import analysis_isolation as iso  # noqa: E402
from lumae_bench import edge_profiles as edge  # noqa: E402
from lumae_bench import loudness  # noqa: E402

EDGE_ARGS = dict(catalog_instance_id="bench", track_id="bench", media_revision="sha256:" + "a" * 64)


def write_flac(path, seconds, seed, rate=44100):
    import av

    frames = int(seconds * rate)
    t = np.arange(frames) / rate
    rng = np.random.default_rng(seed)
    mono = (0.3 * np.sin(2 * np.pi * (110 + 40 * seed) * t) + 0.05 * rng.standard_normal(frames))
    mono *= np.minimum(1.0, t / 2.0)
    pcm = (np.stack([mono, mono * 0.9]) * 20000).astype(np.int16)
    with av.open(str(path), "w") as container:
        stream = container.add_stream("flac", rate=rate)
        stream.layout = "stereo"
        for offset in range(0, frames, 4096):
            chunk = np.ascontiguousarray(pcm[:, offset:offset + 4096].T.reshape(1, -1))
            frame = av.AudioFrame.from_ndarray(chunk, format="s16", layout="stereo")
            frame.sample_rate = rate
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode(None):
            container.mux(packet)
    return path


def timed(func, *args, **kwargs):
    started = time.perf_counter()
    result = func(*args, **kwargs)
    return time.perf_counter() - started, result


def rss_mb(pid, field):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith(field + ":"):
            return round(int(line.split()[1]) / 1024, 1)
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--files", type=int, default=20)
    parser.add_argument("--small-seconds", type=float, default=5.0)
    parser.add_argument("--typical-seconds", type=float, default=240.0)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    limit = dict(limit_seconds=iso.DEFAULT_LIMIT_SECONDS)
    with tempfile.TemporaryDirectory(prefix="lumae_isolation_bench_") as scratch:
        scratch = Path(scratch)
        small = [write_flac(scratch / f"small_{i}.flac", args.small_seconds, i) for i in range(args.files)]
        typical = write_flac(scratch / "typical.flac", args.typical_seconds, 99)

        iso.shutdown()
        in_process_first, _ = timed(loudness.analyze_file, str(small[0]))
        cold, _ = timed(iso.run_isolated, loudness.analyze_file, small[0], **limit)
        worker = iso._pooled
        idle_rss = rss_mb(worker.pid, "VmRSS")

        overhead = {"waveform": [], "edge": []}
        for path in small:
            direct, _ = timed(loudness.analyze_file, str(path))
            isolated, _ = timed(iso.run_isolated, loudness.analyze_file, path, **limit)
            overhead["waveform"].append(isolated - direct)
            direct, _ = timed(edge.analyze_edge_file, str(path), **EDGE_ARGS)
            isolated, _ = timed(iso.run_isolated, edge.analyze_edge_file, path, **limit, **EDGE_ARGS)
            overhead["edge"].append(isolated - direct)

        runs = {"waveform": ([], []), "edge": ([], [])}
        for _ in range(args.repeats):
            runs["waveform"][0].append(timed(loudness.analyze_file, str(typical))[0])
            runs["waveform"][1].append(timed(iso.run_isolated, loudness.analyze_file, typical, **limit)[0])
            runs["edge"][0].append(timed(edge.analyze_edge_file, str(typical), **EDGE_ARGS)[0])
            runs["edge"][1].append(
                timed(iso.run_isolated, edge.analyze_edge_file, typical, **limit, **EDGE_ARGS)[0])
        peak_rss = rss_mb(worker.pid, "VmHWM")
        iso.shutdown()

    typical_out = {}
    for name, (direct, isolated) in runs.items():
        d, i = statistics.median(direct), statistics.median(isolated)
        typical_out[name] = {
            "in_process_s": round(d, 3), "isolated_s": round(i, 3),
            "overhead_ms": round((i - d) * 1000, 1), "overhead_pct": round((i - d) / d * 100, 2),
        }
    print(json.dumps({
        "python": sys.version.split()[0],
        "files": args.files,
        "small_seconds": args.small_seconds,
        "typical_seconds": args.typical_seconds,
        "worker_start_s": round(cold - in_process_first, 3),
        "per_file_overhead_ms": {
            name: {"median": round(statistics.median(values) * 1000, 2),
                   "max": round(max(values) * 1000, 2)}
            for name, values in overhead.items()
        },
        "typical": typical_out,
        "child_rss_mb": {"after_start": idle_rss, "peak": peak_rss},
    }, indent=2))


if __name__ == "__main__":
    main()
