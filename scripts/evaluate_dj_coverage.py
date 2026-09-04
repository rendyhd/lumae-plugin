"""Evaluate sanitized DJ analysis coverage without emitting track identifiers."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import importlib.util  # noqa: E402

MODULE_PATH = ROOT / "plugins" / "LumaeAnalysis" / "dj_coverage.py"
SPEC = importlib.util.spec_from_file_location("lumae_dj_coverage_cli", MODULE_PATH)
COVERAGE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = COVERAGE
SPEC.loader.exec_module(COVERAGE)
evaluate_coverage = COVERAGE.evaluate_coverage


def load_json_or_jsonl(path):
    text = Path(path).read_text(encoding="utf-8")
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    if isinstance(value, list):
        return value
    if isinstance(value, dict) and isinstance(value.get("analyses"), list):
        return value["analyses"]
    raise ValueError("analysis input must be a JSON array, JSONL, or an object with analyses")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analyses", required=True)
    parser.add_argument("--manifest")
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    manifest = (
        json.loads(Path(args.manifest).read_text(encoding="utf-8")) if args.manifest else None
    )
    if isinstance(manifest, dict) and isinstance(manifest.get("manifest"), dict):
        manifest = manifest["manifest"]
    report = evaluate_coverage(load_json_or_jsonl(args.analyses), manifest)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
