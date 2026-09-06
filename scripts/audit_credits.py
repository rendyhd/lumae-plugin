"""Validate a reviewed fifty-album credits audit without changing production settings.

Input: JSON {matching_version:1,synthetic:false,reviewed_by,reviewed_at,albums:[
 {album_id,categories:[well_tagged|sparse|duplicate_edition|compilation],outcome:
 accepted|unresolved|empty,correct:true|false (required for accepted),evidence_url:...}]}.
Keep unresolved matches and verified matches without credits separate. A report
must come from an actual catalog audit; synthetic tests cannot qualify display.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import types

ROOT = Path(__file__).resolve().parents[1]


def load_evaluator():
    package = types.ModuleType("_lumae_credits_audit")
    package.__path__ = [str(ROOT / "plugins" / "LumaeAnalysis")]
    sys.modules[package.__name__] = package
    name = package.__name__ + ".credits_qualification"
    spec = importlib.util.spec_from_file_location(name, ROOT / "plugins/LumaeAnalysis/credits_qualification.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.evaluate_audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = load_evaluator()(json.loads(args.report.read_text(encoding="utf-8")))
    text = json.dumps(result, indent=2)
    print(text)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    return 0 if result["qualified"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
