import base64

from flask import Blueprint, jsonify, request

from plugin.api import enqueue, get_db, logger, table

SCHEMA_VERSION = 1
ANALYZER_VERSION = 1

bp = Blueprint("lumae_analysis", __name__)


def profiles_table():
    return table("profiles")


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


def analyze_tracks_task(ids):
    logger.info("lumae_analysis analyze_tracks_task scaffold received %d ids", len(ids or []))
    return {"accepted": len(ids or [])}


def backfill_missing_profiles():
    logger.info("lumae_analysis backfill scaffold started")
    return {"analyzed": 0}


def settings():
    return "Lumae Analysis"


def register(ctx):
    ctx.add_blueprint(bp)
    ctx.set_settings_page("lumae_analysis.settings")
    ctx.on_install(migrate)
    ctx.add_cron_task("backfill", backfill_missing_profiles, queue="default")
