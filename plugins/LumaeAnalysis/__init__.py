import base64
import os
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from plugin.api import enqueue, get_db, get_setting, logger, render_page, set_setting, table

from .loudness import SilentAudioError, analyze_file

SCHEMA_VERSION = 1
ANALYZER_VERSION = 1

bp = Blueprint("lumae_analysis", __name__)


def profiles_table():
    return table("profiles")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def media_signature(path):
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return f"{path}|{stat.st_size}|{int(stat.st_mtime)}"


def configured_backfill_limit():
    raw = get_setting("backfill_batch_size", 25)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 25
    return min(max(value, 1), 250)


def migrate(db):
    cur = db.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {profiles_table()} (
            track_id TEXT PRIMARY KEY,
            sample_rate INTEGER NOT NULL,
            duration_ms INTEGER NOT NULL,
            ref_lufs REAL NOT NULL,
            start_ramp BYTEA NOT NULL,
            end_ramp BYTEA NOT NULL,
            analyzer_ver INTEGER NOT NULL,
            profile_schema_ver INTEGER NOT NULL,
            media_signature TEXT,
            analyzed_at TIMESTAMP NOT NULL DEFAULT now(),
            status TEXT NOT NULL,
            last_error TEXT
        )
        """
    )
    db.commit()
    cur.close()


def parse_ids(value):
    if not value:
        return []
    ids = []
    seen = set()
    for raw in str(value).split(","):
        track_id = raw.strip()
        if track_id and track_id not in seen:
            ids.append(track_id)
            seen.add(track_id)
    return ids[:500]


def fetch_profile_rows(ids):
    if not ids:
        return []
    db = get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
               analyzer_ver, analyzed_at, media_signature, status, last_error
          FROM {profiles_table()}
         WHERE track_id = ANY(%s)
        """,
        (ids,),
    )
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    cur.close()
    return rows


def _bytes(value):
    if value is None:
        return b""
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, bytes):
        return value
    return bytes(value)


def serialize_ready_profile(row):
    return {
        "track_id": row["track_id"],
        "source": "waveform",
        "sample_rate": int(row["sample_rate"]),
        "duration_ms": int(row["duration_ms"]),
        "ref_lufs": float(row["ref_lufs"]),
        "start_ramp": base64.b64encode(_bytes(row["start_ramp"])).decode("ascii"),
        "end_ramp": base64.b64encode(_bytes(row["end_ramp"])).decode("ascii"),
        "analyzer_ver": int(row["analyzer_ver"]),
        "analyzed_at": str(row["analyzed_at"]),
        "media_signature": row.get("media_signature"),
    }


def split_analyze_ids(ids):
    rows = fetch_profile_rows(ids)
    by_id = {row["track_id"]: row for row in rows}
    accepted = []
    already_ready = []
    already_pending = []
    for track_id in ids:
        row = by_id.get(track_id)
        status = row.get("status") if row else None
        if status == "ready":
            already_ready.append(track_id)
        elif status == "pending":
            already_pending.append(track_id)
        else:
            accepted.append(track_id)
    return accepted, already_ready, already_pending


def mark_pending(ids):
    if not ids:
        return
    db = get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {profiles_table()}
            (track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
             analyzer_ver, profile_schema_ver, analyzed_at, status, last_error)
        SELECT unnest(%s::text[]), 0, 0, 0, decode('', 'hex'), decode('', 'hex'), %s, %s, now(), 'pending', NULL
        ON CONFLICT (track_id) DO UPDATE SET
            analyzed_at = EXCLUDED.analyzed_at,
            status = 'pending',
            last_error = NULL
        """,
        (ids, ANALYZER_VERSION, SCHEMA_VERSION),
    )
    db.commit()
    cur.close()


def load_track_file(track_id):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "SELECT item_id, file_path FROM score WHERE item_id = %s",
        (track_id,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    item_id, file_path = row
    if not file_path or not os.path.exists(file_path):
        return None
    return {
        "track_id": str(item_id),
        "file_path": file_path,
        "media_signature": media_signature(file_path),
    }


def upsert_profile(track_id, result, status, last_error=None, media_sig=None):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {profiles_table()}
            (track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
             analyzer_ver, profile_schema_ver, media_signature, analyzed_at, status, last_error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id) DO UPDATE SET
            sample_rate = EXCLUDED.sample_rate,
            duration_ms = EXCLUDED.duration_ms,
            ref_lufs = EXCLUDED.ref_lufs,
            start_ramp = EXCLUDED.start_ramp,
            end_ramp = EXCLUDED.end_ramp,
            analyzer_ver = EXCLUDED.analyzer_ver,
            profile_schema_ver = EXCLUDED.profile_schema_ver,
            media_signature = EXCLUDED.media_signature,
            analyzed_at = EXCLUDED.analyzed_at,
            status = EXCLUDED.status,
            last_error = EXCLUDED.last_error
        """,
        (
            track_id,
            int(getattr(result, "sample_rate", 0)),
            int(getattr(result, "duration_ms", 0)),
            float(getattr(result, "ref_lufs", 0.0)),
            getattr(result, "start_ramp_blob", b""),
            getattr(result, "end_ramp_blob", b""),
            ANALYZER_VERSION,
            SCHEMA_VERSION,
            media_sig,
            utc_now_iso(),
            status,
            last_error,
        ),
    )
    db.commit()
    cur.close()


@bp.get("/api/health")
def health():
    return jsonify(
        {
            "plugin": "lumae_analysis",
            "schema_version": SCHEMA_VERSION,
            "analyzer_version": ANALYZER_VERSION,
            "status": "ok",
        }
    )


@bp.get("/api/profiles")
def profiles():
    ids = parse_ids(request.args.get("ids", ""))
    rows = fetch_profile_rows(ids)
    by_id = {row["track_id"]: row for row in rows}
    ready = []
    failed = []
    missing = []
    for track_id in ids:
        row = by_id.get(track_id)
        if row is None:
            missing.append(track_id)
        elif row["status"] == "ready":
            try:
                ready.append(serialize_ready_profile(row))
            except Exception as exc:
                failed.append({"track_id": track_id, "reason": str(exc)})
        elif row["status"] in ("failed", "skipped_no_file"):
            failed.append({"track_id": track_id, "reason": row.get("last_error") or "failed"})
        else:
            missing.append(track_id)
    return jsonify(
        {
            "schema_version": SCHEMA_VERSION,
            "analyzer_version": ANALYZER_VERSION,
            "profiles": ready,
            "missing": missing,
            "failed": failed,
        }
    )


@bp.post("/api/analyze")
def analyze():
    body = request.get_json(silent=True) or {}
    ids = parse_ids(",".join(body.get("ids", [])))
    accepted, already_ready, already_pending = split_analyze_ids(ids)
    if accepted:
        mark_pending(accepted)
        enqueue(analyze_tracks_task, accepted, queue="default")
    return jsonify(
        {
            "accepted": accepted,
            "already_ready": already_ready,
            "already_pending": already_pending,
        }
    ), 202


def analyze_one_track(track_id):
    info = load_track_file(track_id)
    if info is None:
        upsert_profile(track_id, object(), "skipped_no_file", "missing file path", None)
        return {"track_id": track_id, "status": "skipped_no_file"}
    try:
        result = analyze_file(info["file_path"])
        upsert_profile(track_id, result, "ready", None, info["media_signature"])
        return {"track_id": track_id, "status": "ready"}
    except SilentAudioError as exc:
        upsert_profile(track_id, object(), "failed", str(exc), info["media_signature"])
        return {"track_id": track_id, "status": "failed"}
    except Exception as exc:
        logger.exception("lumae_analysis failed for %s", track_id)
        upsert_profile(track_id, object(), "failed", str(exc), info["media_signature"])
        return {"track_id": track_id, "status": "failed"}


def analyze_tracks_task(ids):
    ids = parse_ids(",".join(ids or []))
    results = [analyze_one_track(track_id) for track_id in ids]
    return {
        "ready": sum(1 for result in results if result["status"] == "ready"),
        "failed": sum(1 for result in results if result["status"] == "failed"),
        "skipped": sum(1 for result in results if result["status"].startswith("skipped")),
    }


def is_backfill_candidate(file_path, stored_sig, analyzer_ver, status):
    current_sig = media_signature(file_path)
    if analyzer_ver is None:
        return True
    if int(analyzer_ver) < ANALYZER_VERSION:
        return True
    if status == "stale":
        return True
    if status == "ready" and current_sig and stored_sig and current_sig != stored_sig:
        return True
    return False


def find_backfill_ids(limit=25):
    batch_limit = int(limit or configured_backfill_limit())
    db = get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT s.item_id, s.file_path, p.media_signature, p.analyzer_ver, p.status
          FROM score s
          LEFT JOIN {profiles_table()} p ON p.track_id = s.item_id
         WHERE s.file_path IS NOT NULL
        """
    )
    ids = []
    for item_id, file_path, stored_sig, analyzer_ver, status in cur.fetchall():
        if is_backfill_candidate(file_path, stored_sig, analyzer_ver, status):
            ids.append(str(item_id))
            if len(ids) >= batch_limit:
                break
    cur.close()
    return ids


def backfill_missing_profiles(limit=None):
    ids = find_backfill_ids(limit or configured_backfill_limit())
    return analyze_tracks_task(ids)


@bp.route("/settings", methods=["GET", "POST"])
def settings():
    if request.method == "POST":
        try:
            batch_size = int(request.form.get("backfill_batch_size") or 25)
        except (TypeError, ValueError):
            batch_size = 25
        set_setting("backfill_batch_size", min(max(batch_size, 1), 250))
        schedule = (request.form.get("backfill_schedule") or "manual").strip().lower()
        if schedule not in {"manual", "daily", "weekly"}:
            schedule = "manual"
        set_setting("backfill_schedule", schedule)
        return "", 204

    batch_size = configured_backfill_limit()
    schedule = get_setting("backfill_schedule", "manual")
    return render_page(
        f"""
        <form method="post">
          <label>Backfill batch size <input name="backfill_batch_size" value="{batch_size}"></label>
          <label>Backfill schedule <input name="backfill_schedule" value="{schedule}"></label>
          <button type="submit">Save</button>
        </form>
        <p>Create the actual daily or weekly run from Administration &gt; Scheduled Tasks using task type plugin.lumae_analysis.backfill.</p>
        """,
        title="Lumae Analysis",
    )


def register(ctx):
    ctx.add_blueprint(bp)
    ctx.set_settings_page("lumae_analysis.settings")
    ctx.on_install(migrate)
    ctx.add_cron_task("backfill", backfill_missing_profiles, queue="default")
