"""Create a deterministic, disposable 40-clip private DJ review pack.

Input is JSONL with one locally acquired source per track::

  {"track_id":"opaque-id","source_path":"/tmp/song.flac","duration_ms":180000,
   "vocal_risk":{"frames":[{"position_ms":12000,"raw_vocal_evidence":0.91}]}}

The source paths are used only by ffmpeg and are never written to the pack.
The browser exports the JSONL contract consumed by build_vocal_calibration.py.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
from pathlib import Path
import subprocess


TRACK_COUNT = 20
TRACKS_PER_SPLIT = 10
CLIPS_PER_TRACK = 2
CLIP_DURATION_SECONDS = 6
EVIDENCE_WINDOW_MS = CLIP_DURATION_SECONDS * 1_000
MAX_INPUT_BYTES = 10 * 1024 * 1024
DEFAULT_SEED = "lumae-private-vocal-audition-v1"


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def read_sources(path):
    source = Path(path).resolve(strict=True)
    if not source.is_file() or not 0 < source.stat().st_size <= MAX_INPUT_BYTES:
        raise ValueError("review input must be a bounded JSONL file")
    rows = []
    with source.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}") from exc
            rows.append(_normalize_source(value, line_number))
    ids = [row["track_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("review input contains duplicate tracks")
    if len(rows) < TRACK_COUNT:
        raise ValueError("private audition requires at least 20 tracks")
    return rows


def _normalize_source(value, line_number):
    if not isinstance(value, dict):
        raise ValueError(f"line {line_number} must be an object")
    track_id = value.get("track_id")
    duration_ms = value.get("duration_ms")
    source_path = value.get("source_path")
    frames = (value.get("vocal_risk") or {}).get("frames")
    if not isinstance(track_id, str) or not track_id or len(track_id) > 512:
        raise ValueError(f"line {line_number} has an invalid track_id")
    if not isinstance(duration_ms, int) or duration_ms < CLIP_DURATION_SECONDS * 2_000:
        raise ValueError(f"line {line_number} has an invalid duration_ms")
    if not isinstance(source_path, str) or not Path(source_path).resolve(strict=True).is_file():
        raise ValueError(f"line {line_number} has an invalid local source_path")
    if not isinstance(frames, list):
        raise ValueError(f"line {line_number} has no vocal-risk frames")
    source_frames = []
    margin = CLIP_DURATION_SECONDS * 500
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        position = frame.get("position_ms")
        evidence = frame.get("raw_vocal_evidence")
        if (
            isinstance(position, int)
            and 0 <= position <= duration_ms
            and isinstance(evidence, (int, float))
            and not isinstance(evidence, bool)
            and 0 <= float(evidence) <= 1
        ):
            source_frames.append(
                {"position_ms": position, "raw_vocal_evidence": float(evidence)}
            )
    normalized_frames = []
    for frame in source_frames:
        if not margin <= frame["position_ms"] <= duration_ms - margin:
            continue
        review_evidence = max(
            candidate["raw_vocal_evidence"]
            for candidate in source_frames
            if abs(candidate["position_ms"] - frame["position_ms"]) <= margin
        )
        normalized_frames.append(
            {
                **frame,
                "review_evidence": review_evidence,
            }
        )
    if len({frame["position_ms"] for frame in normalized_frames}) < CLIPS_PER_TRACK:
        raise ValueError(f"line {line_number} lacks two bounded review frames")
    return {
        "track_id": track_id,
        "source_path": str(Path(source_path).resolve()),
        "duration_ms": duration_ms,
        "frames": normalized_frames,
    }


def select_review_items(rows, seed=DEFAULT_SEED):
    selected = sorted(rows, key=lambda row: _digest(f"{seed}|track|{row['track_id']}"))[
        :TRACK_COUNT
    ]
    items = []
    for index, row in enumerate(selected):
        split = "calibration" if index < TRACKS_PER_SPLIT else "holdout"
        low = min(row["frames"], key=lambda frame: (frame["review_evidence"], frame["position_ms"]))
        high_candidates = sorted(
            row["frames"],
            key=lambda frame: (-frame["review_evidence"], frame["position_ms"]),
        )
        high = next(frame for frame in high_candidates if frame["position_ms"] != low["position_ms"])
        for polarity, frame in (("low", low), ("high", high)):
            review_id = _digest(
                f"{seed}|clip|{row['track_id']}|{frame['position_ms']}|{polarity}"
            )[:20]
            items.append(
                {
                    "review_id": review_id,
                    "track_id": row["track_id"],
                    "source_path": row["source_path"],
                    "split": split,
                    "position_ms": frame["position_ms"],
                    "raw_vocal_evidence": frame["review_evidence"],
                    "polarity": polarity,
                    "clip": f"clips/{review_id}.opus",
                }
            )
    return sorted(items, key=lambda item: _digest(f"{seed}|order|{item['review_id']}"))


def read_review_state(path):
    if not path:
        return {}
    value = json.loads(Path(path).resolve(strict=True).read_text(encoding="utf-8"))
    answers = value.get("answers") if isinstance(value, dict) else None
    if not isinstance(answers, dict) or any(
        not isinstance(key, str) or answer not in ("vocal", "clear", "uncertain")
        for key, answer in answers.items()
    ):
        raise ValueError("invalid private review state")
    return answers


def add_class_count_replacements(items, rows, answers, seed=DEFAULT_SEED):
    """Add non-overlapping same-track clips until every reviewed-frame gate can pass."""
    by_track = {row["track_id"]: row for row in rows}
    used = {(item["track_id"], item["position_ms"]) for item in items}
    additions = []
    for split in ("calibration", "holdout"):
        split_items = [item for item in items if item["split"] == split]
        positive_count = sum(answers.get(item["review_id"]) == "vocal" for item in split_items)
        negative_count = sum(answers.get(item["review_id"]) == "clear" for item in split_items)
        reviewed_count = positive_count + negative_count
        frame_deficit = max(0, TRACKS_PER_SPLIT * CLIPS_PER_TRACK - reviewed_count)
        requests = [
            (item["track_id"], item["polarity"])
            for item in split_items
            if answers.get(item["review_id"]) not in ("vocal", "clear")
        ][:frame_deficit]
        class_polarities = ["high"] * max(0, 5 - positive_count)
        class_polarities += ["low"] * max(0, 5 - negative_count)
        split_tracks = sorted({item["track_id"] for item in split_items})
        while len(requests) < max(frame_deficit, len(class_polarities)):
            index = len(requests)
            requests.append(
                (
                    split_tracks[index % len(split_tracks)],
                    class_polarities[index] if index < len(class_polarities) else ("high" if index % 2 == 0 else "low"),
                )
            )

        for replacement_index, (track_id, polarity) in enumerate(requests):
            row = by_track[track_id]
            used_positions = [position for used_track, position in used if used_track == track_id]
            candidates = [
                frame
                for frame in row["frames"]
                if (track_id, frame["position_ms"]) not in used
                and all(
                    abs(frame["position_ms"] - position) >= CLIP_DURATION_SECONDS * 1_000
                    for position in used_positions
                )
            ]
            candidates.sort(
                key=lambda frame: (
                    -frame["review_evidence"] if polarity == "high" else frame["review_evidence"],
                    _digest(
                        f"{seed}|replacement|{split}|{polarity}|{replacement_index}|"
                        f"{track_id}|{frame['position_ms']}"
                    ),
                )
            )
            if not candidates:
                raise ValueError(
                    f"{split} track lacks a non-overlapping {polarity} replacement timestamp"
                )
            frame = candidates[0]
            used.add((track_id, frame["position_ms"]))
            review_id = _digest(
                f"{seed}|replacement-clip|{track_id}|{frame['position_ms']}|{polarity}"
            )[:20]
            additions.append(
                {
                    "review_id": review_id,
                    "track_id": track_id,
                    "source_path": row["source_path"],
                    "split": split,
                    "position_ms": frame["position_ms"],
                    "raw_vocal_evidence": frame["review_evidence"],
                    "polarity": polarity,
                    "clip": f"clips/{review_id}.opus",
                }
            )
    return sorted(items + additions, key=lambda item: _digest(f"{seed}|order|{item['review_id']}"))


def _render_clip(item, destination, ffmpeg="ffmpeg", runner=subprocess.run):
    start_ms = max(0, item["position_ms"] - CLIP_DURATION_SECONDS * 500)
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-ss",
        f"{start_ms / 1000:.3f}",
        "-i",
        item["source_path"],
        "-t",
        str(CLIP_DURATION_SECONDS),
        "-vn",
        "-ac",
        "2",
        "-ar",
        "48000",
        "-c:a",
        "libopus",
        "-b:a",
        "96k",
        str(destination),
    ]
    runner(command, check=True, capture_output=True)


def _public_item(item):
    return {key: item[key] for key in (
        "review_id",
        "track_id",
        "split",
        "position_ms",
        "raw_vocal_evidence",
        "polarity",
        "clip",
    )}


def build_pack(
    input_path,
    output_dir,
    *,
    seed=DEFAULT_SEED,
    ffmpeg="ffmpeg",
    runner=subprocess.run,
    review_state_path=None,
):
    rows = read_sources(input_path)
    items = select_review_items(rows, seed=seed)
    prior_answers = read_review_state(review_state_path)
    if prior_answers:
        items = add_class_count_replacements(items, rows, prior_answers, seed=seed)
    destination = Path(output_dir).resolve()
    clips = destination / "clips"
    clips.mkdir(parents=True, exist_ok=True)
    for item in items:
        _render_clip(item, destination / item["clip"], ffmpeg=ffmpeg, runner=runner)
    public_items = [_public_item(item) for item in items]
    manifest = {
        "version": 1,
        "qualification_tier": "private-audition",
        "evidence_window_ms": EVIDENCE_WINDOW_MS,
        "seed": seed,
        "track_count": len({item["track_id"] for item in items}),
        "clip_count": len(items),
        "prior_answers": {
            key: value for key, value in prior_answers.items() if key in {item["review_id"] for item in items}
        },
        "items": public_items,
    }
    manifest["manifest_sha256"] = _digest(canonical_json(manifest))
    (destination / "review-manifest.json").write_text(
        canonical_json(manifest) + "\n", encoding="utf-8"
    )
    (destination / "index.html").write_text(_review_html(manifest), encoding="utf-8")
    return manifest


def _review_html(manifest):
    data = json.dumps(manifest["items"], ensure_ascii=False).replace("</", "<\\/")
    prior = json.dumps(manifest.get("prior_answers", {}), ensure_ascii=False).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Lumae private vocal-risk review</title>
<style>
body{{font:16px system-ui;background:#15130f;color:#efeae0;max-width:760px;margin:30px auto;padding:0 18px}}
.card{{background:#1f1c17;padding:18px;border-radius:12px;margin:14px 0}} audio{{width:100%}}
button{{margin:8px 6px 0 0;padding:10px 12px}} .chosen{{outline:3px solid #d4a54a}}
#export{{position:sticky;bottom:10px;background:#d4a54a;color:#15130f;font-weight:700}}
</style></head><body><h1>Private DJ vocal-risk review</h1>
<p>Each clip is one source, not a transition. Choose Vocals present for audible singing, speech, rap, humming, choir, or chant; choose No vocals for instrumental audio; choose Uncertain when unclear. This labels whether the excerpt would be risky to overlap or cut through. Scores are intentionally hidden.</p>
<div id="progress"></div><main id="clips"></main><button id="export">Export labels JSONL</button>
<button id="state">Export review state</button><script>
const items={data},storageKey='lumae-private-vocal-review-{manifest.get('manifest_sha256')}',stored=JSON.parse(localStorage.getItem(storageKey)||'{{}}'),answers={{...{prior},...stored}};const root=document.getElementById('clips');
function update(){{document.getElementById('progress').textContent=`${{Object.keys(answers).length}} / ${{items.length}} reviewed`;}}
for(const [i,item] of items.entries()){{const card=document.createElement('section');card.className='card';
card.innerHTML=`<strong>Clip ${{i+1}}</strong><audio controls preload="none" src="${{item.clip}}"></audio>`;
for(const [label,text] of [['vocal','Vocals present'],['clear','No vocals'],['uncertain','Uncertain']]){{
const b=document.createElement('button');b.textContent=text;if(answers[item.review_id]===label)b.classList.add('chosen');b.onclick=()=>{{answers[item.review_id]=label;localStorage.setItem(storageKey,JSON.stringify(answers));for(const x of card.querySelectorAll('button'))x.classList.remove('chosen');b.classList.add('chosen');update();}};card.appendChild(b);}}root.appendChild(card);}}
function download(name,text){{const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{{type:'application/json'}}));a.download=name;a.click();URL.revokeObjectURL(a.href);}}
document.getElementById('export').onclick=()=>{{const lines=items.filter(x=>answers[x.review_id]&&answers[x.review_id]!=='uncertain').map(x=>JSON.stringify({{track_id:x.track_id,split:x.split,position_ms:x.position_ms,raw_vocal_evidence:x.raw_vocal_evidence,vocal_conflict:answers[x.review_id]==='vocal'?1:0}}));download('vocal-conflict-labels.jsonl',lines.join('\\n')+'\\n');}};
document.getElementById('state').onclick=()=>download('review-state.json',JSON.stringify({{version:1,answers}},null,2)+'\\n');update();
</script></body></html>"""


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--seed", default=DEFAULT_SEED)
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--review-state", help="Prior browser state; adds only needed same-track replacements")
    args = parser.parse_args(argv)
    manifest = build_pack(
        args.input,
        args.output_dir,
        seed=args.seed,
        ffmpeg=args.ffmpeg,
        review_state_path=args.review_state,
    )
    print(canonical_json({
        "output_dir": str(Path(args.output_dir).resolve()),
        "manifest_sha256": manifest["manifest_sha256"],
        "track_count": manifest["track_count"],
        "clip_count": manifest["clip_count"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
