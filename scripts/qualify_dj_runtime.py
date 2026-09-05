"""Measure a local, consented audio corpus with the pinned DJ runtime.

No downloads, network, database, or playback authorization. Each case gets a
fresh process, a hard timeout, measured peak RSS, and both contract projections.
The report contains case indexes and timings, never audio paths or titles.
"""

import argparse
import importlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import threading
import time
import types


def load_runtime():
    package = types.ModuleType("_lumae_qualification")
    package.__path__ = [
        str(Path(__file__).resolve().parents[1] / "plugins" / "LumaeAnalysis")
    ]
    sys.modules[package.__name__] = package
    return importlib.import_module(package.__name__ + ".dj_runtime")


def run_case(args):
    import psutil

    process = psutil.Process()
    peak = [process.memory_info().rss]
    done = threading.Event()

    def sample():
        while not done.wait(0.02):
            peak[0] = max(peak[0], process.memory_info().rss)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    result = {"case": args.case_index, "status": "failed", "playback_qualified": False}
    started = time.monotonic()
    try:
        runtime = load_runtime()
        status = runtime.runtime_status(args.beat_this, args.yamnet, verify_model=True)
        if not status.get("available"):
            result["reason"] = status.get("reason", "runtime_unavailable")
            return result
        corpus = json.loads(args.corpus.read_text(encoding="utf-8"))
        path = Path(corpus["cases"][args.case_index]["path"])
        if not path.is_absolute():
            path = args.corpus.parent / path
        raw = runtime.analyze_evidence(
            path, model_path=args.beat_this, yamnet_model_path=args.yamnet
        )
        result["timings"] = dict(raw.timings)
        result["duration_seconds"] = raw.duration_seconds
        result["codec"] = raw.source["codec"]
        result["payload_bytes"] = {}
        for version in (2, 3):
            module = importlib.import_module(
                "_lumae_qualification.dj_analysis" + ("_v3" if version == 3 else "")
            )
            builder = (
                module.build_dj_analysis_v3
                if version == 3
                else module.build_dj_analysis
            )
            payload = builder(
                **raw.annotation_arguments(),
                catalog_instance_id="qualification",
                track_id="case",
                media_revision="sha256:" + "a" * 64,
            )
            result["payload_bytes"][str(version)] = len(json.dumps(payload).encode())
        result["status"] = "passed"
    except Exception as exc:
        result["reason"] = getattr(exc, "code", type(exc).__name__)
    finally:
        result["elapsed_seconds"] = time.monotonic() - started
        peak[0] = max(peak[0], process.memory_info().rss)
        done.set()
        sampler.join(1)
        result["peak_rss_bytes"] = peak[0]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--beat-this", required=True)
    parser.add_argument("--yamnet", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case-index", type=int)
    args = parser.parse_args()
    if args.case_index is not None:
        result = run_case(args)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return 0 if result["status"] == "passed" else 1
    cases = json.loads(args.corpus.read_text(encoding="utf-8")).get("cases", [])
    if not 1 <= len(cases) <= 32:
        parser.error("corpus must contain between 1 and 32 local cases")
    report = {
        "schema_version": 1,
        "kind": "runtime-measurement",
        "playback_qualified": False,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cases": [],
    }
    with tempfile.TemporaryDirectory() as temporary:
        for index in range(len(cases)):
            output = Path(temporary) / f"{index}.json"
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--corpus",
                str(args.corpus.resolve()),
                "--beat-this",
                args.beat_this,
                "--yamnet",
                args.yamnet,
                "--case-index",
                str(index),
                "--output",
                str(output),
            ]
            try:
                subprocess.run(
                    command,
                    timeout=1020,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
                result = (
                    json.loads(output.read_text(encoding="utf-8"))
                    if output.exists()
                    else {"case": index, "status": "failed", "reason": "worker_exit"}
                )
            except subprocess.TimeoutExpired:
                result = {
                    "case": index,
                    "status": "failed",
                    "reason": "process_deadline_exceeded",
                }
            report["cases"].append(result)
            print(f"Case {index+1}/{len(cases)}: {result['status']}", flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if all(case["status"] == "passed" for case in report["cases"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
