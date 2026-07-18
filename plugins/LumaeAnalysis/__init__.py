import base64
import os
from datetime import datetime, timezone
from html import escape

from flask import Blueprint, jsonify, request

from plugin.api import config, enqueue, get_db, get_setting, logger, render_page, set_setting, table

from .loudness import SilentAudioError, analyze_file
from .core_compat import SUPPORTED_CORE_RANGE, detect_core, sanitized_server_summaries
from .collection_manager import (
    COLLECTIONS_BACKUP_VERSION,
    COLLECTIONS_SCHEMA_VERSION,
    collections_enabled,
    current_collection_scope,
    migrate_collections,
    register_collection_routes,
    render_collections_settings_panel,
)

SCHEMA_VERSION = 1
ANALYZER_VERSION = 1
PLUGIN_VERSION = "0.6.0"
CATALOG_SCHEMA_VERSION = 2
ANALYSIS_SCHEMA_VERSION = 2
CATALOG_FEATURES = (
    "dual_core_compat",
    "stable_catalog_instance",
    "provider_occurrences",
    "rich_metadata",
    "complete_generations",
    "bootstrap_leases",
    "cursor_changes",
    "refresh_on_demand",
    "library_scope",
    "album_ids",
    "artist_credits",
    "soft_deletions",
    "shared_analysis",
    "binary_vectors",
)
BACKFILL_TASK_TYPE = "plugin.lumae_analysis.backfill"
WHOLE_LIBRARY_CHUNK_SIZE = 250
COLLECTIONS_MENU_LABEL = "Living Collections"
COLLECTIONS_MENU_ENDPOINT = "lumae_analysis.collection_manager_page"

bp = Blueprint("lumae_analysis", __name__)
register_collection_routes(bp)


def sync_collections_menu(enabled, manager=None):
    """Apply the collection page's enabled state to the live Plugins menu."""
    if manager is None:
        try:
            from plugin.manager import plugin_manager as manager
        except (ImportError, AttributeError):
            return False
    record = getattr(manager, "records", {}).get("lumae_analysis")
    if record is None:
        return False
    items = [
        item
        for item in record.get("menu_items", [])
        if item.get("endpoint") != COLLECTIONS_MENU_ENDPOINT
    ]
    if enabled:
        items.append(
            {
                "label": COLLECTIONS_MENU_LABEL,
                "endpoint": COLLECTIONS_MENU_ENDPOINT,
                "admin_only": False,
            }
        )
    record["menu_items"] = items
    return True


class MediaDownloadError(Exception):
    pass


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


def media_server_download_available():
    fields_by_type = getattr(config, "MEDIASERVER_FIELDS_BY_TYPE", {})
    media_type = str(getattr(config, "MEDIASERVER_TYPE", "") or "").lower()
    required_fields = fields_by_type.get(media_type)
    if not required_fields:
        return False
    return all(str(getattr(config, field, "") or "").strip() for field in required_fields)


def media_server_item(item_id, file_path=None, title=None, author=None):
    track_id = str(item_id)
    path = str(file_path or "")
    name = str(title or track_id)
    item = {
        "id": track_id,
        "Id": track_id,
        "title": name,
        "Name": name,
        "path": path,
        "Path": path,
        "FilePath": path,
    }
    if author:
        item["artist"] = str(author)
        item["AlbumArtist"] = str(author)
    suffix = os.path.splitext(path)[1].lstrip(".")
    if suffix:
        item["suffix"] = suffix
    return item


def download_track_to_temp(item):
    from tasks.mediaserver import download_track

    return download_track(config.TEMP_DIR, item)


def remove_downloaded_file(path):
    if not path:
        return
    try:
        os.remove(path)
    except OSError:
        logger.warning("lumae_analysis could not remove temporary analysis file %s", path)


def configured_backfill_limit():
    raw = get_setting("backfill_batch_size", 25)
    return normalize_backfill_limit(raw)


def normalize_backfill_limit(raw):
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 25
    return min(max(value, 1), 250)


def format_count(value):
    return f"{int(value):,}"


def track_count_label(value):
    count = int(value)
    suffix = "track" if count == 1 else "tracks"
    return f"{format_count(count)} {suffix}"


def disable_legacy_backfill_schedule(db):
    cur = db.cursor()
    cur.execute(
        "UPDATE cron SET enabled=FALSE WHERE task_type=%s",
        (BACKFILL_TASK_TYPE,),
    )
    cur.close()


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
    cur.close()
    migrate_collections(db)
    disable_legacy_backfill_schedule(db)
    db.commit()


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
        "SELECT item_id, file_path, title, author FROM score WHERE item_id = %s",
        (track_id,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    item_id = row[0]
    file_path = row[1] if len(row) > 1 else None
    title = row[2] if len(row) > 2 else None
    author = row[3] if len(row) > 3 else None
    if file_path and os.path.exists(file_path):
        return {
            "track_id": str(item_id),
            "file_path": file_path,
            "media_signature": media_signature(file_path),
            "cleanup_path": None,
        }

    if not media_server_download_available():
        return None

    try:
        item = media_server_item(item_id, file_path, title, author)
        downloaded_path = download_track_to_temp(item)
    except Exception as exc:
        logger.warning("lumae_analysis could not download %s for analysis: %s", track_id, exc)
        raise MediaDownloadError("media server download failed") from exc

    if not downloaded_path or not os.path.exists(downloaded_path):
        raise MediaDownloadError("media server download failed")
    return {
        "track_id": str(item_id),
        "file_path": downloaded_path,
        "media_signature": media_signature(downloaded_path),
        "cleanup_path": downloaded_path,
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


def catalog_capability():
    return {
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
        "supported_core_range": SUPPORTED_CORE_RANGE,
        "features": list(CATALOG_FEATURES),
    }


@bp.get("/api/health")
def health():
    compatibility = detect_core()
    return jsonify(
        {
            "plugin": "lumae_analysis",
            "plugin_version": PLUGIN_VERSION,
            "core_version": compatibility.core_version,
            "core_adapter": compatibility.adapter,
            "supported_core_range": SUPPORTED_CORE_RANGE,
            "schema_version": SCHEMA_VERSION,
            "analyzer_version": ANALYZER_VERSION,
            "capabilities": {
                "collections": {
                    "schema_version": COLLECTIONS_SCHEMA_VERSION,
                    "backup_version": COLLECTIONS_BACKUP_VERSION,
                    "enabled": collections_enabled(),
                    "scope": current_collection_scope()["mode"],
                },
                "catalog_mirror": catalog_capability(),
            },
            "status": "ok" if compatibility.supported else compatibility.status,
        }
    )


@bp.get("/api/catalog/health")
def catalog_health():
    compatibility = detect_core()
    try:
        servers = sanitized_server_summaries(compatibility)
    except Exception as exc:
        logger.exception("lumae_analysis could not enumerate AudioMuse servers")
        payload = compatibility.as_dict()
        payload.update(
            {
                "plugin": "lumae_analysis",
                "plugin_version": PLUGIN_VERSION,
                "catalog_schema_version": CATALOG_SCHEMA_VERSION,
                "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
                "capability": catalog_capability(),
                "servers": [],
                "status": "server_discovery_failed",
                "reason": str(exc),
            }
        )
        return jsonify(payload), 503

    payload = compatibility.as_dict()
    payload.update(
        {
            "plugin": "lumae_analysis",
            "plugin_version": PLUGIN_VERSION,
            "catalog_schema_version": CATALOG_SCHEMA_VERSION,
            "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
            "capability": catalog_capability(),
            "servers": servers,
        }
    )
    return jsonify(payload), 200 if compatibility.supported else 409


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
    try:
        info = load_track_file(track_id)
    except MediaDownloadError as exc:
        upsert_profile(track_id, object(), "failed", str(exc), None)
        return {"track_id": track_id, "status": "failed"}
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
    finally:
        remove_downloaded_file(info.get("cleanup_path"))


def hook_track_id(song):
    song = song or {}
    media_item = song.get("media_item") or {}
    track_id = song.get("item_id") or media_item.get("Id") or media_item.get("id")
    return str(track_id) if track_id else ""


def hook_source_path(song):
    song = song or {}
    media_item = song.get("media_item") or {}
    metadata = song.get("metadata") or {}
    return (
        media_item.get("FilePath")
        or media_item.get("Path")
        or media_item.get("path")
        or metadata.get("file_path")
        or ""
    )


def hook_media_signature(song, audio_path):
    track_id = hook_track_id(song)
    source_path = hook_source_path(song)
    audio_sig = media_signature(audio_path) or ""
    return f"analysis-hook|{track_id}|{source_path}|{audio_sig}"


def analyze_song_hook(song):
    track_id = hook_track_id(song)
    audio_path = (song or {}).get("audio_path")
    if not track_id:
        logger.warning("lumae_analysis song hook skipped payload without item_id")
        return {"track_id": "", "status": "skipped_no_file"}
    if not audio_path or not os.path.exists(audio_path):
        upsert_profile(track_id, object(), "skipped_no_file", "missing analysis audio path", None)
        return {"track_id": track_id, "status": "skipped_no_file"}
    media_sig = hook_media_signature(song, audio_path)
    try:
        result = analyze_file(audio_path)
        upsert_profile(track_id, result, "ready", None, media_sig)
        return {"track_id": track_id, "status": "ready"}
    except SilentAudioError as exc:
        upsert_profile(track_id, object(), "failed", str(exc), media_sig)
        return {"track_id": track_id, "status": "failed"}
    except Exception as exc:
        logger.exception("lumae_analysis hook failed for %s", track_id)
        upsert_profile(track_id, object(), "failed", str(exc), media_sig)
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
    if status == "pending":
        return False
    current_sig = media_signature(file_path)
    if status == "skipped_no_file":
        return bool(current_sig or media_server_download_available())
    if analyzer_ver is None:
        return True
    if int(analyzer_ver) < ANALYZER_VERSION:
        return True
    if status == "stale":
        return True
    if status == "ready" and current_sig and stored_sig and current_sig != stored_sig:
        return True
    return False


def fetch_analysis_rows():
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
    rows = cur.fetchall()
    cur.close()
    return rows


def find_backfill_ids(limit=25):
    batch_limit = normalize_backfill_limit(limit or configured_backfill_limit())
    ids = []
    for item_id, file_path, stored_sig, analyzer_ver, status in fetch_analysis_rows():
        if is_backfill_candidate(file_path, stored_sig, analyzer_ver, status):
            ids.append(str(item_id))
            if len(ids) >= batch_limit:
                break
    return ids


def find_all_backfill_ids():
    ids = []
    for item_id, file_path, stored_sig, analyzer_ver, status in fetch_analysis_rows():
        if is_backfill_candidate(file_path, stored_sig, analyzer_ver, status):
            ids.append(str(item_id))
    return ids


def analysis_status_counts():
    counts = {
        "total_with_files": 0,
        "ready_current": 0,
        "pending": 0,
        "failed": 0,
        "skipped": 0,
        "needs_analysis": 0,
    }
    for _item_id, file_path, stored_sig, analyzer_ver, status in fetch_analysis_rows():
        counts["total_with_files"] += 1
        if status == "pending":
            counts["pending"] += 1
        elif status == "failed":
            counts["failed"] += 1
        elif is_backfill_candidate(file_path, stored_sig, analyzer_ver, status):
            counts["needs_analysis"] += 1
        elif status == "skipped_no_file":
            counts["skipped"] += 1
        elif status == "ready":
            counts["ready_current"] += 1
        else:
            counts["needs_analysis"] += 1
    return counts


def queue_backfill_batch(limit=None):
    batch_limit = normalize_backfill_limit(limit or configured_backfill_limit())
    ids = find_backfill_ids(batch_limit)
    if ids:
        mark_pending(ids)
        enqueue(analyze_tracks_task, ids, queue="default")
    return {"queued": len(ids), "limit": batch_limit}


def queue_whole_library():
    ids = find_all_backfill_ids()
    jobs = 0
    for start in range(0, len(ids), WHOLE_LIBRARY_CHUNK_SIZE):
        chunk = ids[start:start + WHOLE_LIBRARY_CHUNK_SIZE]
        if not chunk:
            continue
        mark_pending(chunk)
        enqueue(analyze_tracks_task, chunk, queue="default")
        jobs += 1
    return {"queued": len(ids), "jobs": jobs, "chunk_size": WHOLE_LIBRARY_CHUNK_SIZE}


def backfill_missing_profiles(limit=None):
    ids = find_backfill_ids(limit or configured_backfill_limit())
    return analyze_tracks_task(ids)


def render_settings(message=None, error=None):
    batch_size = configured_backfill_limit()
    counts = analysis_status_counts()
    total = int(counts["total_with_files"])
    ready = int(counts["ready_current"])
    coverage = int(round((ready / total) * 100)) if total else 0
    coverage = min(max(coverage, 0), 100)
    queueable = int(counts["needs_analysis"])
    status_cards = [
        ("Needs analysis", queueable, "lumae-status-attention"),
        ("Ready/current", ready, "lumae-status-ready"),
        ("Pending", counts["pending"], "lumae-status-pending"),
        ("Failed", counts["failed"], "lumae-status-failed"),
        ("Skipped", counts["skipped"], "lumae-status-muted"),
        ("Total with files", total, "lumae-status-muted"),
    ]
    cards_html = "\n".join(
        f"""
          <article class="lumae-status-card {state}">
            <span>{escape(label)}</span>
            <strong>{format_count(value)}</strong>
          </article>
        """
        for label, value, state in status_cards
    )
    message_html = (
        f"""
        <div class="lumae-notice lumae-notice-success" role="status">
          {escape(message)}
        </div>
        """
        if message
        else ""
    )
    error_html = (
        f"""
        <div class="lumae-notice lumae-notice-error" role="alert">
          <strong>{escape(error)}</strong>
        </div>
        """
        if error
        else ""
    )
    return render_page(
        f"""
        <style>
          .lumae-analysis-settings {{
            --lumae-ink: #17202a;
            --lumae-muted: #5f6f7f;
            --lumae-line: #d9e2ea;
            --lumae-panel: #ffffff;
            --lumae-soft: #f6f8fb;
            --lumae-accent: #2f6fed;
            --lumae-ready: #247a5a;
            --lumae-warn: #b46b00;
            --lumae-danger: #b42318;
            color: var(--lumae-ink);
            display: grid;
            gap: 18px;
            max-width: 920px;
          }}

          .lumae-hero {{
            border-bottom: 1px solid var(--lumae-line);
            display: grid;
            gap: 10px;
            padding-bottom: 18px;
          }}

          .lumae-kicker {{
            color: var(--lumae-muted);
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0;
            text-transform: uppercase;
          }}

          .lumae-hero h2 {{
            font-size: clamp(1.5rem, 3vw, 2.15rem);
            line-height: 1.1;
            margin: 0;
          }}

          .lumae-hero p,
          .lumae-action-copy,
          .lumae-help {{
            color: var(--lumae-muted);
            line-height: 1.55;
            margin: 0;
          }}

          .lumae-coverage {{
            background: var(--lumae-soft);
            border: 1px solid var(--lumae-line);
            border-radius: 8px;
            display: grid;
            gap: 10px;
            padding: 14px;
          }}

          .lumae-coverage-row {{
            align-items: baseline;
            display: flex;
            gap: 12px;
            justify-content: space-between;
          }}

          .lumae-coverage strong {{
            font-size: 1.1rem;
          }}

          .lumae-meter {{
            background: #dce5ed;
            border-radius: 999px;
            height: 10px;
            overflow: hidden;
          }}

          .lumae-meter-fill {{
            background: linear-gradient(90deg, var(--lumae-ready), var(--lumae-accent));
            height: 100%;
          }}

          .lumae-status-grid {{
            display: grid;
            gap: 10px;
            grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
          }}

          .lumae-status-card {{
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 8px;
            display: grid;
            gap: 8px;
            min-height: 88px;
            padding: 14px;
          }}

          .lumae-status-card span {{
            color: var(--lumae-muted);
            font-size: 0.82rem;
            font-weight: 700;
          }}

          .lumae-status-card strong {{
            font-size: 1.75rem;
            line-height: 1;
          }}

          .lumae-status-attention {{
            border-color: #f2c879;
          }}

          .lumae-status-attention strong {{
            color: var(--lumae-warn);
          }}

          .lumae-status-ready strong {{
            color: var(--lumae-ready);
          }}

          .lumae-status-pending strong {{
            color: var(--lumae-accent);
          }}

          .lumae-status-failed strong {{
            color: var(--lumae-danger);
          }}

          .lumae-status-muted strong {{
            color: #405163;
          }}

          .lumae-panel {{
            border-top: 1px solid var(--lumae-line);
            display: grid;
            gap: 14px;
            padding-top: 18px;
          }}

          .lumae-panel h3 {{
            font-size: 1rem;
            margin: 0;
          }}

          .lumae-form {{
            display: grid;
            gap: 16px;
          }}

          .lumae-field {{
            display: grid;
            gap: 6px;
            max-width: 260px;
          }}

          .lumae-field span {{
            font-weight: 700;
          }}

          .lumae-field input {{
            border: 1px solid var(--lumae-line);
            border-radius: 8px;
            font: inherit;
            padding: 9px 10px;
          }}

          .lumae-toggle {{
            align-items: center;
            display: flex;
            gap: 10px;
            font-weight: 700;
          }}

          .lumae-toggle input {{
            height: 20px;
            width: 20px;
          }}

          .lumae-actions {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
          }}

          .lumae-actions button,
          .lumae-actions .lumae-button {{
            border-radius: 8px;
            cursor: pointer;
            font-weight: 700;
            min-height: 40px;
            padding: 9px 14px;
            text-decoration: none;
          }}

          .lumae-button-primary {{
            background: var(--lumae-accent);
            border: 1px solid var(--lumae-accent);
            color: #ffffff;
          }}

          .lumae-button-secondary {{
            background: #ffffff;
            border: 1px solid var(--lumae-line);
            color: var(--lumae-ink);
          }}

          .lumae-button-caution {{
            background: #fff8eb;
            border: 1px solid #f2c879;
            color: #6f4200;
          }}

          .lumae-action-notes {{
            display: grid;
            gap: 6px;
          }}

          .lumae-notice {{
            border-radius: 8px;
            font-weight: 700;
            padding: 12px 14px;
          }}

          .lumae-notice-success {{
            background: #e9f6ef;
            border: 1px solid #a7d8bd;
            color: #14543c;
          }}

          .lumae-notice-error {{
            background: #fff0ed;
            border: 1px solid #ffb4a8;
            color: var(--lumae-danger);
          }}
        </style>

        <section class="lumae-analysis-settings" aria-label="Lumae analysis settings">
          {message_html}
          {error_html}

          <header class="lumae-hero">
            <span class="lumae-kicker">Waveform profiles</span>
            <h2>Lumae analysis is preparing tracks for smoother playback.</h2>
            <p>New songs are analyzed automatically after AudioMuse processes them. Use these controls to catch up older library items.</p>
          </header>

          <section class="lumae-coverage" aria-label="Profile coverage">
            <div class="lumae-coverage-row">
              <strong>{coverage}% profile coverage</strong>
              <span>{track_count_label(ready)} ready of {track_count_label(total)}</span>
            </div>
            <div class="lumae-meter" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="{coverage}">
              <div class="lumae-meter-fill" style="width: {coverage}%;"></div>
            </div>
          </section>

          <section class="lumae-status-grid" aria-label="Analysis status">
            {cards_html}
          </section>

          <section class="lumae-panel" aria-label="Catch-up controls">
            <h3>Catch-up controls</h3>
            <p class="lumae-action-copy">{track_count_label(queueable)} can be queued now.</p>
            <form class="lumae-form" method="post">
              <label class="lumae-field">
                <span>Tracks per batch</span>
                <input name="backfill_batch_size" value="{batch_size}" inputmode="numeric">
              </label>
              <div class="lumae-actions">
                <button class="lumae-button-secondary" type="submit" name="action" value="save">Save</button>
                <button class="lumae-button-primary" type="submit" name="action" value="catch_up">Analyze next batch</button>
                <button class="lumae-button-caution" type="submit" name="action" value="queue_all">Queue all missing tracks</button>
              </div>
            </form>
            <div class="lumae-action-notes">
              <p class="lumae-help">Runs one controlled batch using the current batch size.</p>
              <p class="lumae-help">Queues all missing, stale, or changed tracks in 250-track jobs.</p>
            </div>
          </section>
          {render_collections_settings_panel()}
        </section>
        """,
        title="Lumae Analysis",
    )


@bp.route("/settings", methods=["GET", "POST"])
def settings():
    message = None
    error = None
    if request.method == "POST":
        try:
            action = request.form.get("action")
            if action == "save_collections":
                enabled = request.form.get("collection_manager_enabled") == "on"
                set_setting("collection_manager_enabled", enabled)
                sync_collections_menu(enabled)
                message = f"Living Collections {'enabled' if enabled else 'disabled'}."
            else:
                batch_size = normalize_backfill_limit(request.form.get("backfill_batch_size") or 25)
                set_setting("backfill_batch_size", batch_size)
            if action == "catch_up":
                result = queue_backfill_batch(batch_size)
                message = f"Queued {track_count_label(result['queued'])} for Lumae analysis."
            elif action == "queue_all":
                result = queue_whole_library()
                message = (
                    f"Queued {track_count_label(result['queued'])} across {format_count(result['jobs'])} jobs "
                    "for Lumae analysis."
                )
            elif action != "save_collections":
                message = "Lumae analysis settings saved."
        except ValueError as exc:
            error = str(exc)

    return render_settings(message=message, error=error)


def register(ctx):
    ctx.add_blueprint(bp)
    ctx.set_settings_page("lumae_analysis.settings")
    if collections_enabled():
        ctx.add_menu_item(COLLECTIONS_MENU_LABEL, COLLECTIONS_MENU_ENDPOINT)
    ctx.on_install(migrate)
    ctx.on_song_analyzed(analyze_song_hook)
