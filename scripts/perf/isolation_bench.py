"""P3-8 / LUM-018: cost of analysing a file in the isolated child process.

Synthesizes stereo 44.1 kHz FLAC files and prints one JSON object:

* ``host_like``: the AudioMuse job model. This (single-threaded) process
  imports the analyzers, as a worker with the plugin loaded has, then runs
  each job in a fresh fork of itself, as ``taskqueue/worker.py`` does: an
  edge job of 1 track and a waveform job of 3 tracks (``--typical-seconds``
  each). Every job runs in-process (no isolation), on the fork path and on the
  exec path; modes are interleaved per repetition. Reports median job seconds
  and the overhead per job and per file against in-process.
* ``per_file``: in one long-lived process, the median extra time per small
  file (``--files`` × ``--small-seconds``) on each path, the exec worker warm.
* ``exec_worker``: the exec worker's start and its RSS after start and at peak.

No database is needed. The plugin modules are loaded through a stand-in
package, as the exec worker loads them.

    python3 scripts/perf/isolation_bench.py --files 20 --typical-seconds 240 --repeats 7
"""
import argparse
import json
import os
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
LIMIT = iso.DEFAULT_LIMIT_SECONDS
MODES = ("in_process", "fork", "exec")


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


def analyze(mode, analyzer, path, kwargs):
    if mode == "in_process":
        return analyzer(str(path), **kwargs)
    iso.FORK_FAST_PATH = mode == "fork"
    return iso.run_isolated(analyzer, path, limit_seconds=LIMIT, **kwargs)


def job_in_fresh_fork(mode, analyzer, paths, kwargs):
    """One AudioMuse job: a fork of this process runs every file, then exits."""
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        code = 1
        try:
            os.close(read_fd)
            started = time.perf_counter()
            for path in paths:
                analyze(mode, analyzer, path, kwargs)
            os.write(write_fd, repr(time.perf_counter() - started).encode())
            code = 0
        finally:
            os._exit(code)  # an exec worker dies with the job (PR_SET_PDEATHSIG)
    os.close(write_fd)
    with os.fdopen(read_fd, "rb") as report:
        data = report.read()
    _, status = os.waitpid(pid, 0)
    if os.waitstatus_to_exitcode(status) != 0 or not data:
        raise RuntimeError(f"{mode} job failed")
    return float(data)


def host_like(tracks, repeats):
    jobs = {
        "edge_1_file": (edge.analyze_edge_file, tracks[:1], EDGE_ARGS),
        "waveform_3_files": (loudness.analyze_file, tracks[:3], {}),
    }
    out = {}
    for name, (analyzer, paths, kwargs) in jobs.items():
        runs = {mode: [] for mode in MODES}
        for _ in range(repeats):
            for mode in MODES:
                runs[mode].append(job_in_fresh_fork(mode, analyzer, paths, kwargs))
        base = statistics.median(runs["in_process"])
        entry = {"files": len(paths), "in_process_s": round(base, 3)}
        for mode in ("fork", "exec"):
            median = statistics.median(runs[mode])
            entry[mode] = {
                "job_s": round(median, 3),
                "overhead_per_job_ms": round((median - base) * 1000, 1),
                "overhead_per_file_ms": round((median - base) * 1000 / len(paths), 1),
                "overhead_pct": round((median - base) / base * 100, 1),
            }
        out[name] = entry
    return out


def per_file(small):
    out = {}
    for mode in ("fork", "exec"):
        iso.FORK_FAST_PATH = mode == "fork"
        analyze(mode, loudness.analyze_file, small[0], {})  # warm (the exec worker starts)
        overhead = {"waveform": [], "edge": []}
        for path in small:
            direct, _ = timed(loudness.analyze_file, str(path))
            isolated, _ = timed(analyze, mode, loudness.analyze_file, path, {})
            overhead["waveform"].append(isolated - direct)
            direct, _ = timed(edge.analyze_edge_file, str(path), **EDGE_ARGS)
            isolated, _ = timed(analyze, mode, edge.analyze_edge_file, path, EDGE_ARGS)
            overhead["edge"].append(isolated - direct)
        out[mode] = {
            name: {"median_ms": round(statistics.median(values) * 1000, 2),
                   "max_ms": round(max(values) * 1000, 2)}
            for name, values in overhead.items()
        }
    return out


def exec_worker(small, typical):
    iso.shutdown()
    iso.FORK_FAST_PATH = False
    in_process, _ = timed(loudness.analyze_file, str(small[0]))
    cold, _ = timed(iso.run_isolated, loudness.analyze_file, small[0], limit_seconds=LIMIT)
    worker = iso._pooled
    idle = rss_mb(worker.pid, "VmRSS")
    iso.run_isolated(edge.analyze_edge_file, typical, limit_seconds=LIMIT, **EDGE_ARGS)
    peak = rss_mb(worker.pid, "VmHWM")
    iso.shutdown()
    return {"start_s": round(cold - in_process, 3), "rss_mb_after_start": idle, "rss_mb_peak": peak}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--files", type=int, default=20)
    parser.add_argument("--small-seconds", type=float, default=5.0)
    parser.add_argument("--typical-seconds", type=float, default=240.0)
    parser.add_argument("--repeats", type=int, default=7)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="lumae_isolation_bench_") as scratch:
        scratch = Path(scratch)
        small = [write_flac(scratch / f"small_{i}.flac", args.small_seconds, i) for i in range(args.files)]
        tracks = [write_flac(scratch / f"track_{i}.flac", args.typical_seconds, 90 + i) for i in range(3)]
        result = {
            "python": sys.version.split()[0],
            "cpus": os.cpu_count(),
            "load_before": [round(value, 2) for value in os.getloadavg()],
            "typical_seconds": args.typical_seconds,
            "repeats": args.repeats,
            "host_like": host_like(tracks, args.repeats),
            "per_file": per_file(small),
            "exec_worker": exec_worker(small, tracks[0]),
            "load_after": [round(value, 2) for value in os.getloadavg()],
        }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
