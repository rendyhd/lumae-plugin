import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "lumae_private_review_pack_test",
    ROOT / "scripts" / "build_private_vocal_review_pack.py",
)
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


def sources(tmp_path, count=24):
    rows = []
    for index in range(count):
        source = tmp_path / f"track-{index}.flac"
        source.write_bytes(b"audio")
        rows.append(
            {
                "track_id": f"track-{index}",
                "source_path": str(source),
                "duration_ms": 120_000,
                "vocal_risk": {
                    "frames": [
                        {"position_ms": 10_000, "raw_vocal_evidence": 0.05},
                        {"position_ms": 30_000, "raw_vocal_evidence": 0.95},
                        {"position_ms": 60_000, "raw_vocal_evidence": 0.4},
                        {"position_ms": 80_000, "raw_vocal_evidence": 0.85},
                        {"position_ms": 100_000, "raw_vocal_evidence": 0.15},
                    ]
                },
            }
        )
    manifest = tmp_path / "sources.jsonl"
    manifest.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return manifest


def test_pack_is_deterministic_track_disjoint_and_path_free(tmp_path):
    commands = []

    def fake_runner(command, **_kwargs):
        commands.append(command)
        Path(command[-1]).write_bytes(b"clip")

    source_manifest = sources(tmp_path)
    first = review.build_pack(source_manifest, tmp_path / "pack", runner=fake_runner)
    second_items = review.select_review_items(
        review.read_sources(source_manifest), seed=first["seed"]
    )

    assert first["track_count"] == 20
    assert first["clip_count"] == 40
    assert first["evidence_window_ms"] == 6_000
    assert len(commands) == 40
    assert [item["review_id"] for item in first["items"]] == [
        item["review_id"] for item in second_items
    ]
    calibration = {
        item["track_id"] for item in first["items"] if item["split"] == "calibration"
    }
    holdout = {
        item["track_id"] for item in first["items"] if item["split"] == "holdout"
    }
    assert len(calibration) == len(holdout) == 10
    assert calibration.isdisjoint(holdout)
    exported = (tmp_path / "pack" / "review-manifest.json").read_text(encoding="utf-8")
    assert str(tmp_path) not in exported


def test_review_evidence_covers_the_entire_six_second_clip(tmp_path):
    source = tmp_path / "window.flac"
    source.write_bytes(b"audio")
    row = review._normalize_source(
        {
            "track_id": "window-track",
            "source_path": str(source),
            "duration_ms": 30_000,
            "vocal_risk": {
                "frames": [
                    {"position_ms": 10_000, "raw_vocal_evidence": 0.0},
                    {"position_ms": 12_000, "raw_vocal_evidence": 0.8},
                    {"position_ms": 20_000, "raw_vocal_evidence": 0.1},
                ]
            },
        },
        1,
    )

    center = next(frame for frame in row["frames"] if frame["position_ms"] == 10_000)
    assert center["raw_vocal_evidence"] == 0.0
    assert center["review_evidence"] == 0.8


def test_html_has_three_review_choices_and_existing_jsonl_fields(tmp_path):
    def fake_runner(command, **_kwargs):
        Path(command[-1]).write_bytes(b"clip")

    review.build_pack(sources(tmp_path, 20), tmp_path / "pack", runner=fake_runner)
    page = (tmp_path / "pack" / "index.html").read_text(encoding="utf-8")
    for text in ("Vocals present", "No vocals", "Uncertain"):
        assert text in page
    assert "Each clip is one source, not a transition." in page
    assert "localStorage.setItem(storageKey,JSON.stringify(answers))" in page
    assert 'audio controls preload="none"' in page
    for field in (
        "track_id",
        "split",
        "position_ms",
        "raw_vocal_evidence",
        "vocal_conflict",
    ):
        assert field in page
    assert "lines.join('\\n')+'\\n'" in page
    assert "JSON.stringify({version:1,answers},null,2)+'\\n'" in page


def test_replacements_stay_in_the_fixed_track_split_when_counts_are_short(tmp_path):
    source_manifest = sources(tmp_path, 20)
    rows = review.read_sources(source_manifest)
    initial = review.select_review_items(rows)
    answers = {
        item["review_id"]: (
            "vocal"
            if item["split"] == "calibration" and item["polarity"] == "high"
            else "clear"
            if item["split"] == "calibration"
            else "uncertain"
        )
        for item in initial
    }
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"version": 1, "answers": answers}), encoding="utf-8")

    def fake_runner(command, **_kwargs):
        Path(command[-1]).write_bytes(b"clip")

    manifest = review.build_pack(
        source_manifest,
        tmp_path / "replacement-pack",
        review_state_path=state,
        runner=fake_runner,
    )
    initial_ids = {item["review_id"] for item in initial}
    replacements = [item for item in manifest["items"] if item["review_id"] not in initial_ids]
    selected_tracks = {item["track_id"] for item in initial}
    assert replacements
    assert {item["track_id"] for item in replacements} <= selected_tracks
    assert all(item["split"] == "holdout" for item in replacements)


def test_uncertain_replacements_use_the_same_tracks_without_overlapping_audio(tmp_path):
    source_manifest = sources(tmp_path, 20)
    rows = review.read_sources(source_manifest)
    initial = review.select_review_items(rows)
    uncertain = [item for item in initial if item["split"] == "calibration"][:2]
    uncertain_ids = {item["review_id"] for item in uncertain}
    answers = {
        item["review_id"]: (
            "uncertain"
            if item["review_id"] in uncertain_ids
            else "vocal"
            if item["polarity"] == "high"
            else "clear"
        )
        for item in initial
    }

    expanded = review.add_class_count_replacements(initial, rows, answers)
    initial_ids = {item["review_id"] for item in initial}
    replacements = [item for item in expanded if item["review_id"] not in initial_ids]

    assert len(replacements) == 2
    assert {item["track_id"] for item in replacements} == {
        item["track_id"] for item in uncertain
    }
    for replacement in replacements:
        original_positions = [
            item["position_ms"]
            for item in initial
            if item["track_id"] == replacement["track_id"]
        ]
        assert all(
            abs(replacement["position_ms"] - position)
            >= review.CLIP_DURATION_SECONDS * 1_000
            for position in original_positions
        )
