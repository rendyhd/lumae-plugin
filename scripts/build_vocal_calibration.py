"""Build a reviewed, track-disjoint YAMNet vocal-risk calibration artifact."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path


REVIEW_ACKNOWLEDGEMENT = (
    "I reviewed the track-disjoint vocal-conflict labels and holdout metrics"
)
YAMNET_MODEL_SHA256 = "10c95ea3eb9a7bb4cb8bddf6feb023250381008177ac162ce169694d05c317de"
YAMNET_CLASS_MAP_SHA256 = "cdf24d193e196d9e95912a2667051ae203e92a2ba09449218ccb40ef787c6df2"
MAX_INPUT_BYTES = 100 * 1024 * 1024


def _load_contract():
    path = (
        Path(__file__).resolve().parents[1]
        / "plugins"
        / "LumaeAnalysis"
        / "vocal_calibration.py"
    )
    spec = importlib.util.spec_from_file_location("lumae_vocal_calibration_builder", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_rows(path):
    source_path = Path(path).resolve(strict=True)
    if not source_path.is_file() or source_path.stat().st_size > MAX_INPUT_BYTES:
        raise ValueError("calibration input must be a bounded JSONL file")
    rows = []
    with source_path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}") from exc
    return rows


def build(
    input_path,
    output_path,
    *,
    reviewed=False,
    authorize_cuts=False,
    acknowledgement="",
    qualification_tier=None,
):
    if authorize_cuts and (
        not reviewed or acknowledgement != REVIEW_ACKNOWLEDGEMENT
    ):
        raise ValueError("cut authorization requires the exact review acknowledgement")
    contract = _load_contract()
    artifact = contract.fit_artifact(
        read_rows(input_path),
        yamnet_model_sha256=YAMNET_MODEL_SHA256,
        class_map_sha256=YAMNET_CLASS_MAP_SHA256,
        reviewed=reviewed,
        authorize_cuts=authorize_cuts,
        qualification_tier_name=qualification_tier,
    )
    destination = Path(output_path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".partial")
    temporary.write_text(contract.canonical_json(artifact) + "\n", encoding="utf-8")
    temporary.replace(destination)
    return artifact


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Track-disjoint labeled JSONL")
    parser.add_argument("--output", required=True, help="Destination calibration JSON")
    parser.add_argument("--reviewed", action="store_true")
    parser.add_argument("--authorize-cuts", action="store_true")
    parser.add_argument("--acknowledgement", default="")
    parser.add_argument(
        "--qualification-tier",
        choices=("release", "private-audition"),
        default="release",
        help="Private audition lowers corpus size only; it never authorizes release.",
    )
    args = parser.parse_args(argv)
    artifact = build(
        args.input,
        args.output,
        reviewed=args.reviewed,
        authorize_cuts=args.authorize_cuts,
        acknowledgement=args.acknowledgement,
        qualification_tier=args.qualification_tier,
    )
    print(
        json.dumps(
            {
                "output": str(Path(args.output).resolve()),
                "artifact_digest": artifact["artifact_digest"],
                "cuts_authorized": artifact["authorization"]["cuts_authorized"],
                "calibration_tier": artifact.get("qualification_tier", "release"),
                "release_authorized": (
                    artifact.get("qualification_tier", "release") == "release"
                    and artifact["authorization"]["cuts_authorized"]
                ),
                "holdout": artifact["holdout"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
