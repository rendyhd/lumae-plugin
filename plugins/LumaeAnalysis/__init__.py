import base64
import hashlib
import os
from datetime import datetime, timezone
from html import escape

from flask import Blueprint, Response, g, jsonify, request

from plugin.api import config, enqueue, get_db, get_setting, logger, render_page, set_setting, table

from .loudness import SilentAudioError, analyze_file
from .core_compat import (
    SUPPORTED_CORE_RANGE,
    detect_core,
    get_core_adapter,
    sanitized_server_summaries,
)
from .catalog import (
    attempt_legacy_rebind,
    CatalogScanError,
    bootstrap_page,
    create_bootstrap_session,
    ensure_catalog_sources,
    migrate_catalog,
    read_catalog_changes,
    refresh_catalog,
    release_bootstrap_session,
    resolve_catalog_source,
    verify_library_scope,
)
from .catalog_analysis import (
    dedup_policy,
    project_analysis,
    read_analysis_changes,
    scalar_batch,
    vector_batch,
)
from .catalog_readiness import (
    acknowledge_v3_release,
    clear_v3_release_acknowledgement,
    v3_release_readiness,
)
from .catalog_providers import ProviderCatalogBridge, SUPPORTED_PROVIDER_TYPES
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
PLUGIN_VERSION = "0.8.1"
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
    "v3_release_readiness",
    "provider_track_scope_verification",
    "source_scoped_profiles",
    "prepare_lumae",
    "analysis_run_finalization",
    "catalog_ready_before_profile_backfill",
    "interactive_profile_priority",
    "bounded_profile_backfill",
)
BACKFILL_TASK_TYPE = "plugin.lumae_analysis.backfill"
CATALOG_REFRESH_TASK_TYPE = "plugin.lumae_analysis.catalog_refresh"
ANALYSIS_PROJECTION_TASK_TYPE = "plugin.lumae_analysis.analysis_projection"
DEFAULT_BACKFILL_BATCH_SIZE = 10
MAX_BACKFILL_BATCH_SIZE = 25
INTERACTIVE_PROFILE_CHUNK_SIZE = 3
MAX_INTERACTIVE_PROFILE_IDS = 12
PREPARATION_STALE_HOURS = 1
BACKFILL_STALE_MINUTES = 30
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


def source_profiles_table():
    return table("source_profiles")


def preparation_state_table():
    return table("preparation_state")


def profile_backfill_state_table():
    return table("profile_backfill_state")


def analysis_runs_table():
    return table("analysis_runs")


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
    raw = get_setting("backfill_batch_size", DEFAULT_BACKFILL_BATCH_SIZE)
    return normalize_backfill_limit(raw)


def normalize_backfill_limit(raw):
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_BACKFILL_BATCH_SIZE
    return min(max(value, 1), MAX_BACKFILL_BATCH_SIZE)


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


def ensure_catalog_refresh_schedule(db):
    cur = db.cursor()
    cur.execute(
        "INSERT INTO cron (name, task_type, cron_expr, enabled) "
        "VALUES (%s, %s, %s, FALSE) ON CONFLICT (task_type) DO NOTHING",
        (CATALOG_REFRESH_TASK_TYPE, CATALOG_REFRESH_TASK_TYPE, "17 */6 * * *"),
    )
    cur.close()


def ensure_analysis_projection_schedule(db):
    cur = db.cursor()
    cur.execute(
        "INSERT INTO cron (name, task_type, cron_expr, enabled) "
        "VALUES (%s, %s, %s, FALSE) ON CONFLICT (task_type) DO NOTHING",
        (ANALYSIS_PROJECTION_TASK_TYPE, ANALYSIS_PROJECTION_TASK_TYPE, "47 */6 * * *"),
    )
    cur.close()


def catalog_refresh_task(server_id=None):
    adapter = get_core_adapter()
    return refresh_catalog(server_id=server_id or adapter.active_server_id())


def analysis_projection_task(server_id=None):
    adapter = get_core_adapter()
    return project_analysis(server_id=server_id or adapter.active_server_id(), adapter=adapter)


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
    migrate_catalog(db)
    ensure_catalog_sources(db)
    cur = db.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {source_profiles_table()} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id)
                ON DELETE CASCADE,
            track_id TEXT NOT NULL,
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
            last_error TEXT,
            PRIMARY KEY (catalog_instance_id, track_id)
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {table('source_profiles_status_idx')} "
        f"ON {source_profiles_table()} (catalog_instance_id, status)"
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table('profile_migrations')} (
            name TEXT PRIMARY KEY,
            completed_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    # Releases before 0.8 could only analyze the default provider. Preserve
    # those expensive waveform results only when exactly one active source
    # makes their ownership unambiguous. Record the one-shot attempt even on a
    # multi-source install so a later default-provider change cannot misassign
    # legacy rows.
    cur.execute(
        f"""
        WITH migration AS (
            INSERT INTO {table('profile_migrations')} (name)
            VALUES ('legacy_default_profiles_v1')
            ON CONFLICT (name) DO NOTHING
            RETURNING name
        ), active_sources AS (
            SELECT catalog_instance_id
              FROM {table('catalog_sources')}
             WHERE rebind_status='active' AND provider_type='navidrome'
        ), default_source AS (
            SELECT catalog_instance_id
              FROM active_sources
             WHERE (SELECT COUNT(*) FROM active_sources)=1
        )
        INSERT INTO {source_profiles_table()}
            (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
             start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
             media_signature, analyzed_at, status, last_error)
        SELECT d.catalog_instance_id, p.track_id, p.sample_rate, p.duration_ms,
               p.ref_lufs, p.start_ramp, p.end_ramp, p.analyzer_ver,
               p.profile_schema_ver, p.media_signature, p.analyzed_at,
               p.status, p.last_error
          FROM migration CROSS JOIN default_source d CROSS JOIN {profiles_table()} p
        ON CONFLICT (catalog_instance_id, track_id) DO NOTHING
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {preparation_state_table()} (
            catalog_instance_id TEXT PRIMARY KEY REFERENCES {table('catalog_sources')}(catalog_instance_id)
                ON DELETE CASCADE,
            server_id TEXT NOT NULL,
            status TEXT NOT NULL,
            phase TEXT NOT NULL,
            queued_profiles INTEGER NOT NULL DEFAULT 0,
            profile_jobs INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {profile_backfill_state_table()} (
            catalog_instance_id TEXT PRIMARY KEY REFERENCES {table('catalog_sources')}(catalog_instance_id)
                ON DELETE CASCADE,
            server_id TEXT NOT NULL,
            status TEXT NOT NULL,
            processed_profiles INTEGER NOT NULL DEFAULT 0,
            queued_profiles INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {analysis_runs_table()} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id)
                ON DELETE CASCADE,
            run_id TEXT NOT NULL,
            server_id TEXT NOT NULL,
            status TEXT NOT NULL,
            songs_seen INTEGER NOT NULL DEFAULT 0,
            finalizer_job_id TEXT,
            queued_profiles INTEGER NOT NULL DEFAULT 0,
            profile_jobs INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            first_seen_at TIMESTAMP NOT NULL DEFAULT now(),
            last_seen_at TIMESTAMP NOT NULL DEFAULT now(),
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, run_id)
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {table('analysis_runs_status_idx')} "
        f"ON {analysis_runs_table()} (status, updated_at)"
    )
    cur.close()
    migrate_collections(db)
    ensure_catalog_refresh_schedule(db)
    ensure_analysis_projection_schedule(db)
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


def fetch_profile_rows(ids, catalog_instance_id=None):
    if not ids:
        return []
    db = get_db()
    cur = db.cursor()
    if catalog_instance_id:
        cur.execute(
            f"""
            SELECT track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                   analyzer_ver, analyzed_at, media_signature, status, last_error
              FROM {source_profiles_table()}
             WHERE catalog_instance_id=%s AND track_id = ANY(%s)
            """,
            (catalog_instance_id, ids),
        )
    else:
        # Compatibility path for pre-0.8 clients on a single-provider install.
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


def split_analyze_ids(ids, catalog_instance_id=None):
    rows = fetch_profile_rows(ids, catalog_instance_id=catalog_instance_id)
    by_id = {row["track_id"]: row for row in rows}
    accepted = []
    already_ready = []
    already_pending = []
    for track_id in ids:
        row = by_id.get(track_id)
        status = row.get("status") if row else None
        if status == "ready":
            already_ready.append(track_id)
        elif is_pending_profile_status(status):
            already_pending.append(track_id)
        else:
            accepted.append(track_id)
    return accepted, already_ready, already_pending


def is_pending_profile_status(status):
    return str(status or "") in ("pending", "pending_interactive")


def mark_pending(ids, catalog_instance_id=None, priority="background"):
    if not ids:
        return
    pending_status = "pending_interactive" if priority == "interactive" else "pending"
    db = get_db()
    cur = db.cursor()
    if catalog_instance_id:
        cur.execute(
            f"""
            INSERT INTO {source_profiles_table()}
                (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                 start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                 analyzed_at, status, last_error)
            SELECT %s, unnest(%s::text[]), 0, 0, 0, decode('', 'hex'), decode('', 'hex'),
                   %s, %s, now(), %s, NULL
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                analyzed_at = EXCLUDED.analyzed_at,
                status = EXCLUDED.status,
                last_error = NULL
            """,
            (catalog_instance_id, ids, ANALYZER_VERSION, SCHEMA_VERSION, pending_status),
        )
    else:
        cur.execute(
            f"""
            INSERT INTO {profiles_table()}
                (track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                 analyzer_ver, profile_schema_ver, analyzed_at, status, last_error)
            SELECT unnest(%s::text[]), 0, 0, 0, decode('', 'hex'), decode('', 'hex'), %s, %s, now(), %s, NULL
            ON CONFLICT (track_id) DO UPDATE SET
                analyzed_at = EXCLUDED.analyzed_at,
                status = EXCLUDED.status,
                last_error = NULL
            """,
            (ids, ANALYZER_VERSION, SCHEMA_VERSION, pending_status),
        )
    db.commit()
    cur.close()


def release_pending(ids, catalog_instance_id=None, reason="Profile job could not be queued"):
    if not ids:
        return
    db = get_db()
    cur = db.cursor()
    if catalog_instance_id:
        cur.execute(
            f"""
            UPDATE {source_profiles_table()}
               SET status='stale', last_error=%s, analyzed_at=now()
             WHERE catalog_instance_id=%s AND track_id=ANY(%s)
               AND status IN ('pending', 'pending_interactive')
            """,
            (str(reason)[:2000], catalog_instance_id, ids),
        )
    else:
        cur.execute(
            f"""
            UPDATE {profiles_table()}
               SET status='stale', last_error=%s, analyzed_at=now()
             WHERE track_id=ANY(%s) AND status IN ('pending', 'pending_interactive')
            """,
            (str(reason)[:2000], ids),
        )
    db.commit()
    cur.close()


def enqueue_profile_analysis(
    ids,
    catalog_instance_id=None,
    server_id=None,
    *,
    priority="background",
):
    queue_name = "high" if priority == "interactive" else "default"
    if catalog_instance_id:
        mark_pending(ids, catalog_instance_id=catalog_instance_id, priority=priority)
    else:
        mark_pending(ids, priority=priority)
    try:
        if catalog_instance_id:
            return enqueue(
                analyze_tracks_task,
                ids,
                catalog_instance_id,
                server_id,
                priority,
                queue=queue_name,
            )
        return enqueue(analyze_tracks_task, ids, None, None, priority, queue=queue_name)
    except Exception as exc:
        release_pending(
            ids,
            catalog_instance_id=catalog_instance_id,
            reason=f"Profile job could not be queued: {exc}",
        )
        raise


def load_track_file(track_id, catalog_instance_id=None, server_id=None):
    db = get_db()
    adapter = get_core_adapter()
    server_id = server_id or adapter.active_server_id()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT t.track_id, t.title, t.artist_display, t.media_fp,
               s.current_core_server_id
          FROM {table('catalog_sources')} s
          JOIN {table('catalog_state')} c USING (catalog_instance_id)
          JOIN {table('catalog_tracks')} t
            ON t.catalog_instance_id=s.catalog_instance_id
           AND t.published_generation=c.published_generation
         WHERE t.track_id=%s AND t.available=TRUE
           AND s.rebind_status='active'
           AND (%s IS NULL OR s.catalog_instance_id=%s)
           AND (%s IS NULL OR s.current_core_server_id=%s)
         ORDER BY s.is_default DESC
         LIMIT 1
        """,
        (track_id, catalog_instance_id, catalog_instance_id, server_id, server_id),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    if len(row) < 5:
        # Rollback-compatible path for a pre-catalogue profile migration. New
        # installations and completed catalogue refreshes always use the rich
        # five-column provider occurrence row below.
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
        item = media_server_item(item_id, file_path, title, author)
        downloaded_path = download_track_to_temp(item)
        if not downloaded_path or not os.path.exists(downloaded_path):
            raise MediaDownloadError("media server download failed")
        return {
            "track_id": str(item_id),
            "file_path": downloaded_path,
            "media_signature": media_signature(downloaded_path),
            "cleanup_path": downloaded_path,
        }
    item_id = row[0]
    title = row[1] if len(row) > 1 else None
    author = row[2] if len(row) > 2 else None
    media_fp = row[3] if len(row) > 3 else None
    source_server_id = row[4] if len(row) > 4 else server_id

    try:
        item = media_server_item(item_id, None, title, author)
        downloaded_path = ProviderCatalogBridge(adapter).download_track(
            source_server_id, config.TEMP_DIR, item
        )
    except Exception as exc:
        logger.warning("lumae_analysis could not download %s for analysis: %s", track_id, exc)
        raise MediaDownloadError("media server download failed") from exc

    if not downloaded_path or not os.path.exists(downloaded_path):
        raise MediaDownloadError("media server download failed")
    return {
        "track_id": str(item_id),
        "file_path": downloaded_path,
        "media_signature": f"catalog-media:{media_fp}" if media_fp else media_signature(downloaded_path),
        "cleanup_path": downloaded_path,
    }


def upsert_profile(
    track_id,
    result,
    status,
    last_error=None,
    media_sig=None,
    catalog_instance_id=None,
):
    db = get_db()
    cur = db.cursor()
    values = (
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
    )
    conflict_target = "track_id"
    target_table = profiles_table()
    columns = "track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp"
    placeholders = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
    if catalog_instance_id:
        target_table = source_profiles_table()
        conflict_target = "catalog_instance_id, track_id"
        columns = "catalog_instance_id, " + columns
        placeholders = "%s, " + placeholders
        values = (catalog_instance_id,) + values
    cur.execute(
        f"""
        INSERT INTO {target_table}
            ({columns}, analyzer_ver, profile_schema_ver, media_signature,
             analyzed_at, status, last_error)
        VALUES ({placeholders})
        ON CONFLICT ({conflict_target}) DO UPDATE SET
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
        values,
    )
    db.commit()
    cur.close()


def catalog_capability():
    return {
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
        "supported_core_range": SUPPORTED_CORE_RANGE,
        "supported_provider_types": sorted(SUPPORTED_PROVIDER_TYPES),
        "features": list(CATALOG_FEATURES),
    }


def resolve_profile_source(catalog_instance_id=None, server_id=None, db=None):
    """Resolve one exact profile namespace; never fall through across providers."""
    sources = resolve_catalog_source(
        db or get_db(),
        server_id=str(server_id) if server_id else None,
        catalog_instance_id=str(catalog_instance_id) if catalog_instance_id else None,
    )
    if len(sources) != 1:
        raise ValueError(
            "An explicit catalog_instance_id is required when multiple music servers are configured."
        )
    source = sources[0]
    if catalog_instance_id and source["catalog_instance_id"] != str(catalog_instance_id):
        raise ValueError("Profile catalogue source identity changed")
    if server_id and source["server_id"] != str(server_id):
        raise ValueError("Profile music-server identity changed")
    return source


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
    db = None
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

    persisted = []
    if compatibility.supported:
        try:
            db = get_db()
            if db is not None:
                persisted = resolve_catalog_source(db)
        except Exception:
            logger.exception("lumae_analysis could not read persisted catalogue health")
    if persisted:
        servers = persisted
    servers = [
        {
            **server,
            "supported": str(server.get("provider_type") or "").strip().lower()
            in SUPPORTED_PROVIDER_TYPES,
            **(
                {"status": "provider_unsupported"}
                if str(server.get("provider_type") or "").strip().lower()
                not in SUPPORTED_PROVIDER_TYPES
                else {}
            ),
        }
        for server in servers
    ]
    policy = dedup_policy()
    if compatibility.adapter == "v3_registry":
        servers = [
            {
                **server,
                **(
                    {
                        "v3_readiness": v3_release_readiness(
                            db,
                            compatibility,
                            server,
                            policy,
                        )
                    }
                    if server["supported"]
                    else {}
                ),
            }
            for server in servers
        ]
    payload = compatibility.as_dict()
    payload.update(
        {
            "plugin": "lumae_analysis",
            "plugin_version": PLUGIN_VERSION,
            "catalog_schema_version": CATALOG_SCHEMA_VERSION,
            "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
            "capability": catalog_capability(),
            "dedup_policy": policy,
            "servers": servers,
        }
    )
    response = jsonify(payload)
    response.headers["Cache-Control"] = "private, no-cache"
    response.headers["Vary"] = "Authorization, Cookie"
    return response, 200 if compatibility.supported else 409


def _catalog_principal_key():
    username = getattr(g, "auth_user", None)
    if username:
        return f"user:{username}"
    return f"client:{request.remote_addr or 'unknown'}"


def _private_json(payload, status=200, no_store=True):
    response = jsonify(payload)
    response.status_code = status
    response.headers["Cache-Control"] = "private, no-store" if no_store else "private, no-cache"
    response.headers["Vary"] = "Authorization, Cookie"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _catalog_error(code, message, status):
    return _private_json({"error": code, "message": message}, status)


def _json_body(max_bytes=16_384):
    if request.content_length and request.content_length > max_bytes:
        raise ValueError("Request body is too large")
    body = request.get_json(silent=True)
    if body is None:
        return {}
    if not isinstance(body, dict):
        raise ValueError("JSON body must be an object")
    return body


@bp.post("/api/catalog/refresh")
def catalog_refresh_api():
    try:
        body = _json_body()
        server_id = body.get("server_id")
        catalog_instance_id = body.get("catalog_instance_id")
        sources = resolve_catalog_source(
            get_db(), server_id=server_id, catalog_instance_id=catalog_instance_id
        )
        if len(sources) != 1:
            return _catalog_error(
                "source_required",
                "An explicit server_id is required when multiple music servers are configured.",
                409,
            )
        source = sources[0]
        if catalog_instance_id and source["catalog_instance_id"] != catalog_instance_id:
            return _catalog_error("source_mismatch", "Catalogue source identity changed.", 409)
        stale_for = max(0, min(int(body.get("if_stale_for_seconds", 0) or 0), 604_800))
        completed_at = source["catalog"].get("completed_at")
        if stale_for and completed_at and source["catalog"]["status"] == "complete":
            try:
                completed = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - completed).total_seconds()
                if age < stale_for:
                    return _private_json(
                        {
                            "status": "fresh",
                            "server_id": source["server_id"],
                            "catalog_instance_id": source["catalog_instance_id"],
                            "generation": source["catalog"]["generation"],
                        },
                        200,
                    )
            except ValueError:
                pass
        enqueue(catalog_refresh_task, source["server_id"], queue="default")
        return _private_json(
            {
                "status": "queued",
                "server_id": source["server_id"],
                "catalog_instance_id": source["catalog_instance_id"],
            },
            202,
        )
    except (KeyError, ValueError, CatalogScanError) as exc:
        return _catalog_error("invalid_refresh", str(exc), 400)


@bp.post("/api/catalog/rebind")
def catalog_rebind_api():
    """Prove and accept an exact v2-to-v3 source continuity match."""
    try:
        body = _json_body()
        catalog_instance_id = str(body.get("catalog_instance_id") or "").strip()
        server_id = str(body.get("server_id") or "").strip()
        if not catalog_instance_id or not server_id:
            return _catalog_error(
                "identity_required", "Catalogue instance and candidate server are required.", 400
            )
        result = attempt_legacy_rebind(get_db(), catalog_instance_id, server_id)
        if result["status"] != "active":
            return _catalog_error(
                "continuity_not_proven",
                "The v3 provider catalogue does not exactly match the stored v2 source.",
                409,
            )
        return _private_json(result)
    except KeyError:
        return _catalog_error("source_not_found", "Catalogue source was not found.", 404)
    except (ValueError, CatalogScanError) as exc:
        return _catalog_error("invalid_rebind", str(exc), 409)


@bp.post("/api/catalog/verify-scope")
def catalog_verify_scope_api():
    try:
        body = _json_body()
        catalog_instance_id = str(body.get("catalog_instance_id") or "").strip()
        if not catalog_instance_id:
            return _catalog_error("identity_required", "Catalogue instance is required.", 400)
        result = verify_library_scope(
            get_db(),
            catalog_instance_id,
            body.get("library_ids"),
            body.get("provider_track_ids") if "provider_track_ids" in body else None,
        )
        return _private_json(result)
    except KeyError:
        return _catalog_error("source_not_found", "Catalogue source was not found.", 404)
    except ValueError as exc:
        return _catalog_error("invalid_scope", str(exc), 400)


@bp.post("/api/catalog/bootstrap-sessions")
def catalog_bootstrap_session_api():
    try:
        body = _json_body()
        result = create_bootstrap_session(
            get_db(),
            _catalog_principal_key(),
            stream=str(body.get("stream") or "catalog"),
            server_id=body.get("server_id"),
            catalog_instance_id=body.get("catalog_instance_id"),
        )
        return _private_json(result, 201)
    except KeyError:
        return _catalog_error("source_not_found", "Catalogue source was not found.", 404)
    except (ValueError, CatalogScanError) as exc:
        return _catalog_error("bootstrap_unavailable", str(exc), 409)


@bp.delete("/api/catalog/bootstrap-sessions")
def catalog_bootstrap_release_api():
    token = request.headers.get("X-Lumae-Bootstrap-Token")
    if not token:
        return _catalog_error("token_required", "Bootstrap token header is required.", 400)
    released = release_bootstrap_session(get_db(), token, _catalog_principal_key())
    return _private_json({"released": released})


@bp.get("/api/catalog/bootstrap")
def catalog_bootstrap_api():
    token = request.headers.get("X-Lumae-Bootstrap-Token")
    if not token:
        return _catalog_error("token_required", "Bootstrap token header is required.", 400)
    try:
        result = bootstrap_page(
            get_db(),
            token,
            _catalog_principal_key(),
            stream=str(request.args.get("stream") or "catalog"),
            page_token=request.args.get("page_token"),
            limit=request.args.get("limit", 500),
        )
        return _private_json(result)
    except KeyError:
        return _catalog_error(
            "bootstrap_required", "Bootstrap lease expired or was released.", 410
        )
    except ValueError as exc:
        return _catalog_error("invalid_page", str(exc), 400)


@bp.get("/api/catalog/changes")
def catalog_changes_api():
    cursor = request.args.get("cursor")
    if not cursor:
        return _catalog_error("cursor_required", "Catalogue cursor is required.", 400)
    try:
        result = read_catalog_changes(
            get_db(),
            cursor,
            server_id=request.args.get("server_id"),
            catalog_instance_id=request.args.get("catalog_instance_id"),
            limit=request.args.get("limit", 500),
        )
        return _private_json(result, no_store=False)
    except KeyError:
        return _catalog_error(
            "bootstrap_required", "Catalogue cursor expired or belongs to an old epoch.", 410
        )
    except ValueError as exc:
        return _catalog_error("invalid_cursor", str(exc), 400)


@bp.get("/api/catalog/analysis/changes")
def analysis_changes_api():
    cursor = request.args.get("cursor")
    if not cursor:
        return _catalog_error("cursor_required", "Analysis cursor is required.", 400)
    try:
        result = read_analysis_changes(
            get_db(),
            cursor,
            server_id=request.args.get("server_id"),
            catalog_instance_id=request.args.get("catalog_instance_id"),
            limit=request.args.get("limit", 500),
        )
        return _private_json(result, no_store=False)
    except KeyError:
        return _catalog_error(
            "bootstrap_required", "Analysis cursor expired or belongs to an old epoch.", 410
        )
    except ValueError as exc:
        return _catalog_error("invalid_cursor", str(exc), 400)


@bp.post("/api/catalog/analysis/scalars")
def analysis_scalars_api():
    try:
        body = _json_body(max_bytes=64_000)
        catalog_instance_id = str(body.get("catalog_instance_id") or "")
        ids = body.get("provider_track_ids") or []
        if not catalog_instance_id or not isinstance(ids, list):
            raise ValueError("catalog_instance_id and provider_track_ids are required")
        return _private_json(
            {
                "catalog_instance_id": catalog_instance_id,
                "items": scalar_batch(get_db(), catalog_instance_id, ids),
            }
        )
    except (KeyError, ValueError) as exc:
        return _catalog_error("invalid_batch", str(exc), 400)


@bp.post("/api/catalog/analysis/vectors")
def analysis_vectors_api():
    try:
        body = _json_body(max_bytes=64_000)
        catalog_instance_id = str(body.get("catalog_instance_id") or "")
        ids = body.get("analysis_ids") or []
        family = str(body.get("family") or "musicnn")
        generation = body.get("generation")
        if not catalog_instance_id or not isinstance(ids, list):
            raise ValueError("catalog_instance_id and analysis_ids are required")
        payload = vector_batch(
            get_db(), catalog_instance_id, ids, family=family, generation=generation
        )
        response = Response(payload, mimetype="application/vnd.lumae.f32le-v1")
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["Vary"] = "Authorization, Cookie"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response
    except (KeyError, ValueError, CatalogScanError) as exc:
        return _catalog_error("invalid_batch", str(exc), 400)


@bp.get("/api/profiles")
def profiles():
    try:
        source = resolve_profile_source(
            catalog_instance_id=request.args.get("catalog_instance_id")
        )
    except (KeyError, ValueError, CatalogScanError) as exc:
        return _catalog_error("source_required", str(exc), 409)
    ids = parse_ids(request.args.get("ids", ""))
    rows = fetch_profile_rows(ids, catalog_instance_id=source["catalog_instance_id"])
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
            "catalog_instance_id": source["catalog_instance_id"],
            "profiles": ready,
            "missing": missing,
            "failed": failed,
        }
    )


@bp.post("/api/analyze")
def analyze():
    body = request.get_json(silent=True) or {}
    try:
        source = resolve_profile_source(catalog_instance_id=body.get("catalog_instance_id"))
    except (KeyError, ValueError, CatalogScanError) as exc:
        return _catalog_error("source_required", str(exc), 409)
    ids = parse_ids(",".join(body.get("ids", [])))[:MAX_INTERACTIVE_PROFILE_IDS]
    catalog_instance_id = source["catalog_instance_id"]
    server_id = source["server_id"]
    accepted, already_ready, already_pending = split_analyze_ids(
        ids, catalog_instance_id=catalog_instance_id
    )
    # A library catch-up may already have marked these rows pending on the
    # default queue. Re-enqueue them in tiny high-priority chunks so current
    # playback is never trapped behind an hours-long library backfill. The
    # default task re-checks readiness before each track and becomes a no-op.
    interactive_ids = accepted + already_pending
    for start in range(0, len(interactive_ids), INTERACTIVE_PROFILE_CHUNK_SIZE):
        chunk = interactive_ids[start : start + INTERACTIVE_PROFILE_CHUNK_SIZE]
        enqueue_profile_analysis(
            chunk,
            catalog_instance_id,
            server_id,
            priority="interactive",
        )
    return jsonify(
        {
            "accepted": accepted,
            "already_ready": already_ready,
            "already_pending": already_pending,
        }
    ), 202


def analyze_one_track(track_id, catalog_instance_id=None, server_id=None):
    try:
        info = load_track_file(
            track_id,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
    except MediaDownloadError as exc:
        upsert_profile(
            track_id,
            object(),
            "failed",
            str(exc),
            None,
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "failed"}
    if info is None:
        upsert_profile(
            track_id,
            object(),
            "skipped_no_file",
            "missing file path",
            None,
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "skipped_no_file"}
    try:
        result = analyze_file(info["file_path"])
        upsert_profile(
            track_id,
            result,
            "ready",
            None,
            info["media_signature"],
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "ready"}
    except SilentAudioError as exc:
        upsert_profile(
            track_id,
            object(),
            "failed",
            str(exc),
            info["media_signature"],
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "failed"}
    except Exception as exc:
        logger.exception("lumae_analysis failed for %s", track_id)
        upsert_profile(
            track_id,
            object(),
            "failed",
            str(exc),
            info["media_signature"],
            catalog_instance_id=catalog_instance_id,
        )
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


def catalog_media_signature(track_id, server_id=None):
    db = get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT t.media_fp
          FROM {table('catalog_sources')} s
          JOIN {table('catalog_state')} c USING (catalog_instance_id)
          JOIN {table('catalog_tracks')} t
            ON t.catalog_instance_id=s.catalog_instance_id
           AND t.published_generation=c.published_generation
         WHERE t.track_id=%s AND t.available=TRUE
           AND (%s IS NULL OR s.current_core_server_id=%s)
         ORDER BY s.is_default DESC LIMIT 1
        """,
        (track_id, server_id, server_id),
    )
    row = cur.fetchone()
    cur.close()
    return f"catalog-media:{row[0]}" if row and row[0] else None


def analysis_run_finalizer_job_id(catalog_instance_id, run_id):
    identity = f"{catalog_instance_id}\0{run_id}".encode("utf-8")
    return f"lumae-analysis-run-{hashlib.sha256(identity).hexdigest()[:40]}"


def enqueue_analysis_run_finalizer(server_id, catalog_instance_id, run_id):
    """Queue one source finalizer behind the AudioMuse parent analysis job."""
    from plugin.api import dotted_path, rq_queue_default
    from rq import Retry
    from rq.exceptions import NoSuchJobError
    from rq.job import Dependency, Job

    job_id = analysis_run_finalizer_job_id(catalog_instance_id, run_id)
    try:
        return Job.fetch(job_id, connection=rq_queue_default.connection)
    except NoSuchJobError:
        pass
    return rq_queue_default.enqueue(
        "plugin.manager.run_plugin_task",
        args=(
            dotted_path(finalize_analysis_run_task),
            server_id,
            catalog_instance_id,
            run_id,
        ),
        depends_on=Dependency(run_id, allow_failure=True),
        job_id=job_id,
        job_timeout=-1,
        retry=Retry(max=2),
        description=f"Finalize Lumae analysis run {run_id}",
    )


def update_analysis_run(
    catalog_instance_id,
    run_id,
    status,
    *,
    finalizer_job_id=None,
    queued_profiles=None,
    profile_jobs=None,
    last_error=None,
    completed=False,
    db=None,
):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {analysis_runs_table()}
           SET status=%s,
               finalizer_job_id=COALESCE(%s, finalizer_job_id),
               queued_profiles=COALESCE(%s, queued_profiles),
               profile_jobs=COALESCE(%s, profile_jobs),
               last_error=%s,
               started_at=CASE WHEN %s='running' THEN COALESCE(started_at, now()) ELSE started_at END,
               completed_at=CASE WHEN %s THEN now() ELSE completed_at END,
               updated_at=now()
         WHERE catalog_instance_id=%s AND run_id=%s
        """,
        (
            status,
            finalizer_job_id,
            queued_profiles,
            profile_jobs,
            str(last_error)[:2000] if last_error else None,
            status,
            bool(completed),
            catalog_instance_id,
            run_id,
        ),
    )
    db.commit()
    cur.close()


def record_analysis_run(server_id, catalog_instance_id, run_id, db=None):
    """Count a per-song hook and atomically admit one finalizer for its run."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {analysis_runs_table()}
            (catalog_instance_id, run_id, server_id, status, songs_seen,
             first_seen_at, last_seen_at, updated_at)
        VALUES (%s, %s, %s, 'registering', 1, now(), now(), now())
        ON CONFLICT (catalog_instance_id, run_id) DO NOTHING
        RETURNING run_id
        """,
        (catalog_instance_id, run_id, server_id),
    )
    should_enqueue = cur.fetchone() is not None
    if not should_enqueue:
        cur.execute(
            f"""
            UPDATE {analysis_runs_table()}
               SET status='registering', last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s AND run_id=%s
               AND (status='enqueue_failed'
                    OR (status='registering'
                        AND updated_at < now() - interval '1 minute'))
            RETURNING run_id
            """,
            (catalog_instance_id, run_id),
        )
        should_enqueue = cur.fetchone() is not None
        cur.execute(
            f"""
            UPDATE {analysis_runs_table()}
               SET songs_seen=songs_seen + 1, last_seen_at=now(), updated_at=now()
             WHERE catalog_instance_id=%s AND run_id=%s
            """,
            (catalog_instance_id, run_id),
        )
    db.commit()
    cur.close()
    if not should_enqueue:
        return {"queued": False, "coalesced": True}

    try:
        job = enqueue_analysis_run_finalizer(server_id, catalog_instance_id, run_id)
    except Exception as exc:
        update_analysis_run(
            catalog_instance_id,
            run_id,
            "enqueue_failed",
            last_error=exc,
            db=db,
        )
        raise
    update_analysis_run(
        catalog_instance_id,
        run_id,
        "queued",
        finalizer_job_id=getattr(job, "id", None),
        db=db,
    )
    return {
        "queued": True,
        "coalesced": False,
        "job_id": getattr(job, "id", None),
    }


def claim_analysis_run(catalog_instance_id, run_id, finalizer_job_id=None, db=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {analysis_runs_table()}
           SET status='running', started_at=COALESCE(started_at, now()),
               last_error=NULL, updated_at=now()
         WHERE catalog_instance_id=%s AND run_id=%s
           AND (status IN ('registering', 'queued', 'enqueue_failed', 'failed')
                OR (status='running' AND finalizer_job_id=%s AND %s IS NOT NULL))
        RETURNING songs_seen
        """,
        (catalog_instance_id, run_id, finalizer_job_id, finalizer_job_id),
    )
    row = cur.fetchone()
    db.commit()
    cur.close()
    return int(row[0] or 0) if row else None


def finalize_analysis_run_task(server_id, catalog_instance_id, run_id):
    """Publish one complete source update after an AudioMuse analysis run settles."""
    try:
        from rq import get_current_job

        current_job = get_current_job()
        current_job_id = getattr(current_job, "id", None)
    except Exception:
        current_job_id = None
    songs_seen = claim_analysis_run(
        catalog_instance_id,
        run_id,
        finalizer_job_id=current_job_id,
    )
    if songs_seen is None:
        return {"status": "already_finalized", "run_id": run_id}
    try:
        source = resolve_profile_source(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        if source["catalog_instance_id"] != catalog_instance_id:
            raise CatalogScanError("Catalogue identity changed before run finalization")
        catalog_result = refresh_catalog(server_id=server_id)
        if catalog_result["catalog_instance_id"] != catalog_instance_id:
            raise CatalogScanError("Catalogue identity changed during run finalization")
        projection_result = project_analysis(
            server_id=server_id,
            adapter=get_core_adapter(),
        )
        if projection_result["catalog_instance_id"] != catalog_instance_id:
            raise CatalogScanError("Analysis identity changed during run finalization")
        profile_result = start_profile_backfill(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        update_analysis_run(
            catalog_instance_id,
            run_id,
            "complete",
            queued_profiles=(profile_result["batch_size"] if profile_result["queued"] else 0),
            profile_jobs=1 if profile_result["queued"] else 0,
            completed=True,
        )
        return {
            "status": "complete",
            "run_id": run_id,
            "songs_seen": songs_seen,
            "catalog": catalog_result,
            "analysis": projection_result,
            "profiles": profile_result,
        }
    except Exception as exc:
        update_analysis_run(
            catalog_instance_id,
            run_id,
            "failed",
            last_error=exc,
        )
        raise


def analyze_song_hook(song):
    event = {}
    source_server_id = None
    catalog_instance_id = None
    try:
        event = get_core_adapter().normalize_analysis_hook(song)
        source_server_id = event["server_id"]
        catalog_instance_id = resolve_profile_source(server_id=source_server_id)[
            "catalog_instance_id"
        ]
    except Exception:
        logger.exception("lumae_analysis could not resolve the analysis source")
    if catalog_instance_id:
        run_id = str((event or {}).get("run_id") or "").strip()
        if run_id:
            try:
                record_analysis_run(source_server_id, catalog_instance_id, run_id)
            except Exception:
                logger.exception(
                    "lumae_analysis could not queue the source finalizer for run %s",
                    run_id,
                )
        else:
            logger.warning("lumae_analysis analysis hook did not include run_id")
    track_id = hook_track_id(song)
    audio_path = (song or {}).get("audio_path")
    if not track_id:
        logger.warning("lumae_analysis song hook skipped payload without item_id")
        return {"track_id": "", "status": "skipped_no_file"}
    if not catalog_instance_id:
        logger.warning("lumae_analysis song hook skipped %s without an exact source", track_id)
        return {"track_id": track_id, "status": "skipped_source_unresolved"}
    if not audio_path or not os.path.exists(audio_path):
        upsert_profile(
            track_id,
            object(),
            "skipped_no_file",
            "missing analysis audio path",
            None,
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "skipped_no_file"}
    media_sig = catalog_media_signature(track_id, source_server_id) or hook_media_signature(
        song, audio_path
    )
    try:
        result = analyze_file(audio_path)
        upsert_profile(
            track_id,
            result,
            "ready",
            None,
            media_sig,
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "ready"}
    except SilentAudioError as exc:
        upsert_profile(
            track_id,
            object(),
            "failed",
            str(exc),
            media_sig,
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "failed"}
    except Exception as exc:
        logger.exception("lumae_analysis hook failed for %s", track_id)
        upsert_profile(
            track_id,
            object(),
            "failed",
            str(exc),
            media_sig,
            catalog_instance_id=catalog_instance_id,
        )
        return {"track_id": track_id, "status": "failed"}


def profile_task_disposition(track_id, catalog_instance_id=None, server_id=None, priority="background"):
    rows = fetch_profile_rows([track_id], catalog_instance_id=catalog_instance_id)
    row = rows[0] if rows else None
    if not row:
        return "analyze"
    if priority != "interactive" and row.get("status") == "pending_interactive":
        return "promoted"
    if row.get("status") != "ready" or int(row.get("analyzer_ver") or 0) < ANALYZER_VERSION:
        return "analyze"
    expected_signature = catalog_media_signature(track_id, server_id)
    stored_signature = row.get("media_signature")
    if expected_signature and stored_signature != expected_signature:
        return "analyze"
    return "already_ready"


def analyze_tracks_task(
    ids,
    catalog_instance_id=None,
    server_id=None,
    priority="background",
):
    ids = parse_ids(",".join(ids or []))
    if priority == "background" and len(ids) > MAX_BACKFILL_BATCH_SIZE:
        # Drain 0.8.0's already-persisted 250-track RQ jobs quickly after an
        # upgrade. Their rows become retryable and one bounded chain owns the
        # remaining work; interactive requests can promote any of them now.
        release_pending(
            ids,
            catalog_instance_id=catalog_instance_id,
            reason="Migrated to bounded 0.8.1 background enrichment",
        )
        if catalog_instance_id or server_id:
            try:
                start_profile_backfill(
                    catalog_instance_id=catalog_instance_id,
                    server_id=server_id,
                )
            except Exception:
                logger.exception("lumae_analysis could not migrate a legacy backfill job")
        return {
            "ready": 0,
            "already_ready": 0,
            "promoted": 0,
            "failed": 0,
            "skipped": 0,
            "deferred": len(ids),
        }
    results = []
    for track_id in ids:
        disposition = profile_task_disposition(
            track_id,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
            priority=priority,
        )
        if disposition != "analyze":
            results.append({"track_id": track_id, "status": disposition})
            continue
        results.append(
            analyze_one_track(
                track_id,
                catalog_instance_id=catalog_instance_id,
                server_id=server_id,
            )
        )
    summary = {
        "ready": sum(1 for result in results if result["status"] == "ready"),
        "already_ready": sum(
            1 for result in results if result["status"] == "already_ready"
        ),
        "promoted": sum(1 for result in results if result["status"] == "promoted"),
        "failed": sum(1 for result in results if result["status"] == "failed"),
        "skipped": sum(1 for result in results if result["status"].startswith("skipped")),
        "deferred": 0,
    }
    if catalog_instance_id:
        finalize_preparation_if_settled(catalog_instance_id)
    return summary


def is_backfill_candidate(file_path, stored_sig, analyzer_ver, status):
    if is_pending_profile_status(status):
        return False
    current_sig = (
        file_path if str(file_path or "").startswith("catalog-media:") else media_signature(file_path)
    )
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


def fetch_analysis_rows(catalog_instance_id=None, server_id=None):
    db = get_db()
    cur = db.cursor()
    profile_table = source_profiles_table() if catalog_instance_id else profiles_table()
    profile_source_join = (
        "AND p.catalog_instance_id=source.catalog_instance_id" if catalog_instance_id else ""
    )
    source_filters = ""
    params = ()
    if catalog_instance_id or server_id:
        source_filters = """
               AND (%s IS NULL OR s.catalog_instance_id=%s)
               AND (%s IS NULL OR s.current_core_server_id=%s)
        """
        params = (catalog_instance_id, catalog_instance_id, server_id, server_id)
    sql = f"""
        WITH source AS (
            SELECT s.catalog_instance_id, c.published_generation
              FROM {table('catalog_sources')} s
              JOIN {table('catalog_state')} c USING (catalog_instance_id)
             WHERE s.rebind_status='active' AND c.status='complete'
               {source_filters}
             ORDER BY s.is_default DESC, s.server_name, s.catalog_instance_id
             LIMIT 1
        )
        SELECT t.track_id, 'catalog-media:' || COALESCE(t.media_fp, ''),
               p.media_signature, p.analyzer_ver, p.status
          FROM source
          JOIN {table('catalog_tracks')} t
            ON t.catalog_instance_id=source.catalog_instance_id
           AND t.published_generation=source.published_generation
          LEFT JOIN {profile_table} p ON p.track_id=t.track_id
               {profile_source_join}
         WHERE t.available=TRUE AND t.analysis_eligible=TRUE
         ORDER BY t.track_id
        """
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return rows


def find_backfill_ids(
    limit=25,
    catalog_instance_id=None,
    server_id=None,
    include_failed=False,
):
    batch_limit = normalize_backfill_limit(limit or configured_backfill_limit())
    ids = []
    rows = (
        fetch_analysis_rows(catalog_instance_id=catalog_instance_id, server_id=server_id)
        if catalog_instance_id or server_id
        else fetch_analysis_rows()
    )
    for item_id, file_path, stored_sig, analyzer_ver, status in rows:
        if (include_failed and status == "failed") or is_backfill_candidate(
            file_path, stored_sig, analyzer_ver, status
        ):
            ids.append(str(item_id))
            if len(ids) >= batch_limit:
                break
    return ids


def find_all_backfill_ids(catalog_instance_id=None, server_id=None, include_failed=False):
    ids = []
    rows = (
        fetch_analysis_rows(catalog_instance_id=catalog_instance_id, server_id=server_id)
        if catalog_instance_id or server_id
        else fetch_analysis_rows()
    )
    for item_id, file_path, stored_sig, analyzer_ver, status in rows:
        if (include_failed and status == "failed") or is_backfill_candidate(
            file_path, stored_sig, analyzer_ver, status
        ):
            ids.append(str(item_id))
    return ids


def analysis_status_counts(catalog_instance_id=None, server_id=None):
    counts = {
        "total_with_files": 0,
        "ready_current": 0,
        "pending": 0,
        "failed": 0,
        "skipped": 0,
        "needs_analysis": 0,
    }
    rows = (
        fetch_analysis_rows(catalog_instance_id=catalog_instance_id, server_id=server_id)
        if catalog_instance_id or server_id
        else fetch_analysis_rows()
    )
    for _item_id, file_path, stored_sig, analyzer_ver, status in rows:
        counts["total_with_files"] += 1
        if is_pending_profile_status(status):
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


def queue_backfill_batch(
    limit=None,
    catalog_instance_id=None,
    server_id=None,
    include_failed=False,
):
    batch_limit = normalize_backfill_limit(limit or configured_backfill_limit())
    ids = (
        find_backfill_ids(
            batch_limit,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
            include_failed=include_failed,
        )
        if catalog_instance_id or server_id
        else find_backfill_ids(batch_limit)
    )
    if ids:
        enqueue_profile_analysis(
            ids,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
    return {"queued": len(ids), "limit": batch_limit}


def profile_backfill_state(catalog_instance_id, db=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT server_id, status, processed_profiles, queued_profiles,
               last_error, started_at, completed_at, updated_at
          FROM {profile_backfill_state_table()}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return {
        "catalog_instance_id": str(catalog_instance_id),
        "server_id": str(row[0]),
        "status": str(row[1]),
        "processed_profiles": int(row[2] or 0),
        "queued_profiles": int(row[3] or 0),
        "last_error": str(row[4]) if row[4] else None,
        "started_at": str(row[5]) if row[5] else None,
        "completed_at": str(row[6]) if row[6] else None,
        "updated_at": str(row[7]) if row[7] else None,
    }


def profile_backfill_is_active(state, now=None):
    if not state or state.get("status") not in ("queued", "running"):
        return False
    try:
        updated_at = datetime.fromisoformat(str(state["updated_at"]).replace("Z", "+00:00"))
        current = now or datetime.now(timezone.utc)
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        return (current - updated_at).total_seconds() < BACKFILL_STALE_MINUTES * 60
    except (KeyError, TypeError, ValueError):
        return True


def claim_profile_backfill(source, db=None):
    """Atomically admit one bounded background chain per catalogue source."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {profile_backfill_state_table()}
            (catalog_instance_id, server_id, status, processed_profiles,
             queued_profiles, last_error, started_at, completed_at, updated_at)
        VALUES (%s, %s, 'queued', 0, 0, NULL, now(), NULL, now())
        ON CONFLICT (catalog_instance_id) DO UPDATE SET
            server_id=EXCLUDED.server_id, status='queued', processed_profiles=0,
            queued_profiles=0, last_error=NULL, started_at=now(),
            completed_at=NULL, updated_at=now()
        WHERE {profile_backfill_state_table()}.status NOT IN ('queued', 'running')
           OR {profile_backfill_state_table()}.updated_at
              < now() - interval '{BACKFILL_STALE_MINUTES} minutes'
        RETURNING catalog_instance_id
        """,
        (source["catalog_instance_id"], source["server_id"]),
    )
    claimed = cur.fetchone() is not None
    db.commit()
    cur.close()
    return claimed


def update_profile_backfill_state(
    catalog_instance_id,
    server_id,
    status,
    *,
    processed_increment=0,
    queued_profiles=0,
    last_error=None,
    completed=False,
    db=None,
):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {profile_backfill_state_table()}
            (catalog_instance_id, server_id, status, processed_profiles,
             queued_profiles, last_error, started_at, completed_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now(),
                CASE WHEN %s THEN now() ELSE NULL END, now())
        ON CONFLICT (catalog_instance_id) DO UPDATE SET
            server_id=EXCLUDED.server_id,
            status=EXCLUDED.status,
            processed_profiles={profile_backfill_state_table()}.processed_profiles
                + EXCLUDED.processed_profiles,
            queued_profiles=EXCLUDED.queued_profiles,
            last_error=EXCLUDED.last_error,
            completed_at=EXCLUDED.completed_at,
            updated_at=now()
        """,
        (
            catalog_instance_id,
            server_id,
            status,
            int(processed_increment or 0),
            int(queued_profiles or 0),
            str(last_error)[:2000] if last_error else None,
            bool(completed),
        ),
    )
    db.commit()
    cur.close()


def enqueue_next_profile_backfill(server_id, catalog_instance_id):
    return enqueue(
        profile_backfill_task,
        server_id,
        catalog_instance_id,
        queue="default",
    )


def start_profile_backfill(catalog_instance_id=None, server_id=None):
    source = resolve_profile_source(
        catalog_instance_id=catalog_instance_id,
        server_id=server_id,
    )
    if not claim_profile_backfill(source):
        return {"queued": False, "coalesced": True, "batch_size": configured_backfill_limit()}
    try:
        job = enqueue_next_profile_backfill(source["server_id"], source["catalog_instance_id"])
    except Exception as exc:
        update_profile_backfill_state(
            source["catalog_instance_id"],
            source["server_id"],
            "failed",
            last_error=exc,
            completed=True,
        )
        raise
    return {
        "queued": True,
        "coalesced": False,
        "batch_size": configured_backfill_limit(),
        "job_id": getattr(job, "id", None),
    }


def profile_backfill_task(server_id, catalog_instance_id):
    """Process one small batch, then yield the worker before queueing the next."""
    claimed_ids = []
    try:
        source = resolve_profile_source(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        update_profile_backfill_state(
            catalog_instance_id,
            server_id,
            "running",
        )
        recover_stale_pending_profiles(catalog_instance_id)
        ids = find_backfill_ids(
            configured_backfill_limit(),
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
            include_failed=False,
        )
        if not ids:
            update_profile_backfill_state(
                catalog_instance_id,
                server_id,
                "complete",
                completed=True,
            )
            return {"status": "complete", "processed": 0, "queued_next": False}
        claimed_ids = ids
        mark_pending(ids, catalog_instance_id=catalog_instance_id, priority="background")
        result = analyze_tracks_task(
            ids,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
            priority="background",
        )
        next_ids = find_backfill_ids(
            1,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
            include_failed=False,
        )
        if not next_ids:
            update_profile_backfill_state(
                catalog_instance_id,
                server_id,
                "complete",
                processed_increment=len(ids),
                completed=True,
            )
            return {"status": "complete", "processed": len(ids), "queued_next": False, **result}
        update_profile_backfill_state(
            catalog_instance_id,
            server_id,
            "queued",
            processed_increment=len(ids),
            queued_profiles=len(next_ids),
        )
        enqueue_next_profile_backfill(server_id, catalog_instance_id)
        return {"status": "queued", "processed": len(ids), "queued_next": True, **result}
    except Exception as exc:
        if claimed_ids:
            try:
                release_pending(
                    claimed_ids,
                    catalog_instance_id=catalog_instance_id,
                    reason=f"Background enrichment batch failed: {exc}",
                )
            except Exception:
                logger.exception("lumae_analysis could not release a failed background batch")
        update_profile_backfill_state(
            catalog_instance_id,
            server_id,
            "failed",
            last_error=exc,
            completed=True,
        )
        raise


def queue_whole_library(catalog_instance_id=None, server_id=None, include_failed=False):
    """Compatibility wrapper; 0.8.1 intentionally starts one bounded chain."""
    del include_failed
    return start_profile_backfill(
        catalog_instance_id=catalog_instance_id,
        server_id=server_id,
    )


def backfill_missing_profiles(limit=None, catalog_instance_id=None, server_id=None):
    requested_limit = limit or configured_backfill_limit()
    ids = (
        find_backfill_ids(
            requested_limit,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        if catalog_instance_id or server_id
        else find_backfill_ids(requested_limit)
    )
    if catalog_instance_id or server_id:
        return analyze_tracks_task(
            ids,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
    return analyze_tracks_task(ids)


def preparation_state(catalog_instance_id, db=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT server_id, status, phase, queued_profiles, profile_jobs,
               last_error, started_at, completed_at, updated_at
          FROM {preparation_state_table()}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return {
        "catalog_instance_id": str(catalog_instance_id),
        "server_id": str(row[0]),
        "status": str(row[1]),
        "phase": str(row[2]),
        "queued_profiles": int(row[3] or 0),
        "profile_jobs": int(row[4] or 0),
        "last_error": str(row[5]) if row[5] else None,
        "started_at": str(row[6]) if row[6] else None,
        "completed_at": str(row[7]) if row[7] else None,
        "updated_at": str(row[8]) if row[8] else None,
    }


def preparation_is_active(state, now=None):
    if not state or state.get("status") not in ("queued", "running"):
        return False
    try:
        updated_at = datetime.fromisoformat(str(state["updated_at"]).replace("Z", "+00:00"))
        current = now or datetime.now(timezone.utc)
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        return (current - updated_at).total_seconds() < PREPARATION_STALE_HOURS * 3600
    except (KeyError, TypeError, ValueError):
        return True


def claim_preparation(source, db=None):
    """Atomically admit one preparation run for an exact catalogue source."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {preparation_state_table()}
            (catalog_instance_id, server_id, status, phase, queued_profiles,
             profile_jobs, last_error, started_at, completed_at, updated_at)
        VALUES (%s, %s, 'queued', 'queued', 0, 0, NULL, now(), NULL, now())
        ON CONFLICT (catalog_instance_id) DO UPDATE SET
            server_id=EXCLUDED.server_id,
            status='queued', phase='queued', queued_profiles=0, profile_jobs=0,
            last_error=NULL, started_at=now(), completed_at=NULL, updated_at=now()
        WHERE {preparation_state_table()}.status NOT IN ('queued', 'running')
           OR {preparation_state_table()}.updated_at < now() - interval '{PREPARATION_STALE_HOURS} hours'
        RETURNING catalog_instance_id
        """,
        (source["catalog_instance_id"], source["server_id"]),
    )
    claimed = cur.fetchone() is not None
    db.commit()
    cur.close()
    return claimed


def recover_stale_pending_profiles(catalog_instance_id, db=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {source_profiles_table()}
           SET status='stale', last_error='Recovered an interrupted preparation job'
         WHERE catalog_instance_id=%s AND status='pending'
           AND analyzed_at < now() - interval '{PREPARATION_STALE_HOURS} hours'
        """,
        (catalog_instance_id,),
    )
    recovered = max(0, int(getattr(cur, "rowcount", 0) or 0))
    db.commit()
    cur.close()
    return recovered


def update_preparation_state(
    catalog_instance_id,
    server_id,
    status,
    phase,
    queued_profiles=0,
    profile_jobs=0,
    last_error=None,
    completed=False,
    db=None,
):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {preparation_state_table()}
            (catalog_instance_id, server_id, status, phase, queued_profiles,
             profile_jobs, last_error, started_at, completed_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, now(),
                CASE WHEN %s THEN now() ELSE NULL END, now())
        ON CONFLICT (catalog_instance_id) DO UPDATE SET
            server_id=EXCLUDED.server_id,
            status=EXCLUDED.status,
            phase=EXCLUDED.phase,
            queued_profiles=EXCLUDED.queued_profiles,
            profile_jobs=EXCLUDED.profile_jobs,
            last_error=EXCLUDED.last_error,
            completed_at=EXCLUDED.completed_at,
            updated_at=now()
        """,
        (
            catalog_instance_id,
            server_id,
            status,
            phase,
            int(queued_profiles or 0),
            int(profile_jobs or 0),
            str(last_error)[:2000] if last_error else None,
            bool(completed),
        ),
    )
    db.commit()
    cur.close()


def finalize_preparation_if_settled(catalog_instance_id):
    state = preparation_state(catalog_instance_id)
    if not state or state["status"] not in ("running", "profiles_queued"):
        return state
    counts = analysis_status_counts(
        catalog_instance_id=catalog_instance_id,
        server_id=state["server_id"],
    )
    if counts["pending"] > 0:
        return state
    ready = (
        counts["needs_analysis"] == 0
        and counts["failed"] == 0
        and counts["skipped"] == 0
    )
    update_preparation_state(
        catalog_instance_id,
        state["server_id"],
        "ready" if ready else "needs_attention",
        "complete" if ready else "profiles_need_attention",
        queued_profiles=state["queued_profiles"],
        profile_jobs=state["profile_jobs"],
        last_error=None if ready else "One or more profiles could not be prepared.",
        completed=True,
    )
    return preparation_state(catalog_instance_id)


def prepare_lumae_task(server_id=None, catalog_instance_id=None):
    """Publish the app-ready catalogue first, then start optional enrichment."""
    resolved_server_id = server_id
    resolved_catalog_instance_id = catalog_instance_id
    try:
        resolved_server_id = resolved_server_id or get_core_adapter().active_server_id()
        source = resolve_profile_source(
            catalog_instance_id=resolved_catalog_instance_id,
            server_id=resolved_server_id,
        )
        resolved_catalog_instance_id = source["catalog_instance_id"]
        resolved_server_id = source["server_id"]
        update_preparation_state(
            resolved_catalog_instance_id,
            resolved_server_id,
            "running",
            "catalog_refresh",
        )
        catalog_result = refresh_catalog(server_id=resolved_server_id)
        if catalog_result["catalog_instance_id"] != resolved_catalog_instance_id:
            raise CatalogScanError("Catalogue identity changed during preparation")
        update_preparation_state(
            resolved_catalog_instance_id,
            resolved_server_id,
            "running",
            "analysis_projection",
        )
        projection_result = project_analysis(
            server_id=resolved_server_id,
            adapter=get_core_adapter(),
        )
        if projection_result["catalog_instance_id"] != resolved_catalog_instance_id:
            raise CatalogScanError("Analysis identity changed during preparation")
        update_preparation_state(
            resolved_catalog_instance_id,
            resolved_server_id,
            "ready",
            "catalog_ready",
            completed=True,
        )
        try:
            profile_result = start_profile_backfill(
                catalog_instance_id=resolved_catalog_instance_id,
                server_id=resolved_server_id,
            )
        except Exception as exc:
            # The catalogue is already safe and usable. Enrichment failures
            # remain visible in their own state and never roll readiness back.
            logger.exception("lumae_analysis could not start background profile enrichment")
            profile_result = {"queued": False, "coalesced": False, "error": str(exc)}
        return {
            "catalog": catalog_result,
            "analysis": projection_result,
            "profiles": profile_result,
            "preparation": preparation_state(resolved_catalog_instance_id),
        }
    except Exception as exc:
        if resolved_catalog_instance_id and resolved_server_id:
            update_preparation_state(
                resolved_catalog_instance_id,
                resolved_server_id,
                "failed",
                "failed",
                last_error=exc,
                completed=True,
            )
        raise


_READINESS_BLOCKER_LABELS = {
    "administrator_acknowledgement_required": "Administrator confirmation is still required.",
    "analysis_projection_incomplete": "The plugin analysis projection is not complete.",
    "catalog_generation_incomplete": "The provider catalogue generation is not complete.",
    "chromaprint_backfill_incomplete": "Mapped tracks are still missing Chromaprint fingerprints.",
    "chromaprint_collection_disabled": "Chromaprint collection is disabled in AudioMuse.",
    "chromaprint_gate_disabled": "Chromaprint duplicate validation is disabled in AudioMuse.",
    "cleaning_predates_chromaprint_completion": "Chromaprint completed after Cleaning; run Cleaning and then Analysis again.",
    "duration_tolerance_too_wide": "The AudioMuse duplicate duration tolerance is wider than one second.",
    "folder_gate_not_active": "The fp_4 folder-aware duplicate rule is not active.",
    "fp_4_not_active": "AudioMuse catalogue ID scheme fp_4 is not active.",
    "no_analysis_mappings": "No provider tracks have AudioMuse analysis mappings yet.",
    "readiness_unavailable": "The plugin could not read AudioMuse repair diagnostics.",
    "upgrade_repair_sequence_incomplete": "Run Analysis, then Cleaning, then Analysis again.",
}


def _v3_readiness_sources():
    compatibility = detect_core()
    if compatibility.adapter != "v3_registry":
        return []
    db = get_db()
    if db is None:
        return []
    policy = dedup_policy()
    return [
        (
            source,
            v3_release_readiness(db, compatibility, source, policy),
        )
        for source in resolve_catalog_source(db)
    ]


def render_v3_readiness_panel():
    try:
        sources = _v3_readiness_sources()
    except Exception:
        logger.exception("lumae_analysis could not render AudioMuse 3 readiness")
        return ""
    if not sources:
        return ""
    cards = []
    for source, readiness in sources:
        blockers = readiness.get("blockers") or []
        blocker_html = "".join(
            f"<li>{escape(_READINESS_BLOCKER_LABELS.get(code, code))}</li>"
            for code in blockers
        )
        mapped = int(readiness.get("mapped_track_count") or 0)
        eligible = int(readiness.get("eligible_track_count") or 0)
        missing = int(readiness.get("missing_mapping_count") or 0)
        fingerprinted = int(readiness.get("chromaprint_track_count") or 0)
        coverage = float(readiness.get("chromaprint_coverage") or 0) * 100
        sequence = bool(
            (readiness.get("task_evidence") or {}).get("upgrade_sequence_complete")
        )
        hidden = (
            f'<input type="hidden" name="server_id" value="{escape(str(source["server_id"]))}">'
            f'<input type="hidden" name="catalog_instance_id" '
            f'value="{escape(str(source["catalog_instance_id"]))}">'
        )
        if readiness.get("administrator_acknowledged"):
            controls = f"""
              <p class="lumae-help">Confirmed as {escape(str(readiness.get('verification_mode')))}
                at {escape(str(readiness.get('acknowledged_at') or 'unknown time'))}.</p>
              <form class="lumae-form" method="post">
                {hidden}
                <button class="lumae-button-secondary" type="submit" name="action"
                  value="clear_v3_readiness">Revoke confirmation</button>
              </form>
            """
        else:
            controls = f"""
              <form class="lumae-form" method="post">
                {hidden}
                <label class="lumae-toggle">
                  <input type="checkbox" name="confirm" required>
                  I confirm this is a fresh AudioMuse 3.0.3 database with no pre-3.x catalogue.
                </label>
                <input type="hidden" name="verification_mode" value="fresh">
                <button class="lumae-button-secondary" type="submit" name="action"
                  value="ack_v3_readiness">Confirm fresh installation</button>
              </form>
              <form class="lumae-form" method="post">
                {hidden}
                <label class="lumae-toggle">
                  <input type="checkbox" name="confirm" required>
                  I completed Analysis, Cleaning, then Analysis again after upgrading to 3.0.3.
                </label>
                <input type="hidden" name="verification_mode" value="upgraded">
                <button class="lumae-button-caution" type="submit" name="action"
                  value="ack_v3_readiness">Confirm upgraded installation</button>
              </form>
            """
        cards.append(
            f"""
            <article class="lumae-coverage">
              <div class="lumae-coverage-row">
                <strong>{escape(str(source.get('name') or source['server_id']))}</strong>
                <span>{escape(str(readiness.get('status') or 'unknown'))}</span>
              </div>
              <p class="lumae-help">Chromaprint: {fingerprinted:,} of {mapped:,} mapped tracks
                ({coverage:.2f}%). Upgrade repair sequence detected: {'yes' if sequence else 'no'}.</p>
              <p class="lumae-help">Provider tracks eligible for analysis: {eligible:,}; currently
                mapped: {mapped:,}; without analysis mapping: {missing:,}. Unmapped provider tracks
                remain in the Lumae catalogue.</p>
              {f'<ul class="lumae-help">{blocker_html}</ul>' if blocker_html else ''}
              {controls}
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="AudioMuse 3 release readiness">
        <h3>AudioMuse 3.0.3 sync readiness</h3>
        <p class="lumae-action-copy">Lumae keeps provider tracks authoritative. Confirmation only
          enables the mobile sync gate after fp_4 policy and Chromaprint coverage checks pass.</p>
        {''.join(cards)}
      </section>
    """


def render_source_preparation_panel(batch_size):
    try:
        sources = resolve_catalog_source(get_db())
    except Exception:
        logger.exception("lumae_analysis could not render source preparation")
        return ""
    cards = []
    auto_refresh = False
    for source in sources:
        catalog_instance_id = source["catalog_instance_id"]
        server_id = source["server_id"]
        counts = analysis_status_counts(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        state = preparation_state(catalog_instance_id)
        backfill = profile_backfill_state(catalog_instance_id)
        total = int(counts["total_with_files"])
        ready = int(counts["ready_current"])
        coverage = min(max(int(round((ready / total) * 100)) if total else 0, 0), 100)
        queueable = int(counts["needs_analysis"])
        preparation_active = preparation_is_active(state)
        backfill_active = profile_backfill_is_active(backfill)
        auto_refresh = auto_refresh or preparation_active or backfill_active
        catalogue_ready = (
            source["catalog"]["status"] == "complete"
            and source["analysis"]["status"] == "complete"
        )
        status = "ready for Lumae" if catalogue_ready else (state["status"] if state else "not prepared")
        phase = state["phase"] if state else "not started"
        backfill_status = backfill["status"] if backfill else "not started"
        if backfill and backfill["status"] in ("queued", "running") and not backfill_active:
            backfill_status = "stalled; safe to restart"
        last_error = state.get("last_error") if state else None
        backfill_error = backfill.get("last_error") if backfill else None
        hidden = (
            f'<input type="hidden" name="server_id" value="{escape(str(server_id))}">'
            f'<input type="hidden" name="catalog_instance_id" '
            f'value="{escape(str(catalog_instance_id))}">'
        )
        prepare_disabled = " disabled" if preparation_active else ""
        backfill_disabled = " disabled" if backfill_active or queueable == 0 else ""
        cards.append(
            f"""
            <article class="lumae-coverage" aria-label="Prepare {escape(str(source['name']))}">
              <div class="lumae-coverage-row">
                <strong>{escape(str(source.get('name') or server_id))}</strong>
                <span>{escape(status.replace('_', ' '))} · {escape(phase.replace('_', ' '))}</span>
              </div>
              <p class="lumae-help">Catalogue: {escape(str(source['catalog']['status']))};
                analysis projection: {escape(str(source['analysis']['status']))}.</p>
              <p class="lumae-help"><strong>App readiness and waveform coverage are independent.</strong>
                Lumae can sync, browse, and play as soon as the catalogue and projection above are
                complete.</p>
              <div class="lumae-meter" role="progressbar" aria-valuemin="0"
                aria-valuemax="100" aria-valuenow="{coverage}">
                <div class="lumae-meter-fill" style="width: {coverage}%;"></div>
              </div>
              <p class="lumae-help">Playback enrichment: {ready:,} ready of {total:,};
                {counts['pending']:,} pending; {queueable:,} need analysis;
                {counts['failed']:,} failed; {counts['skipped']:,} skipped.
                Background worker: {escape(backfill_status.replace('_', ' '))}.</p>
              {f'<p class="lumae-notice lumae-notice-error">{escape(last_error)}</p>' if last_error else ''}
              {f'<p class="lumae-notice lumae-notice-error">Waveform catch-up: {escape(backfill_error)}</p>' if backfill_error else ''}
              <form class="lumae-form" method="post">
                {hidden}
                <label class="lumae-field">
                  <span>Tracks per background batch (1-{MAX_BACKFILL_BATCH_SIZE})</span>
                  <input name="backfill_batch_size" value="{batch_size}" inputmode="numeric">
                </label>
                <div class="lumae-actions">
                  <button class="lumae-button-primary" type="submit" name="action"
                    value="prepare_lumae"{prepare_disabled}>Refresh Lumae catalogue</button>
                  <button class="lumae-button-secondary" type="submit" name="action"
                    value="start_backfill"{backfill_disabled}>Start background enrichment</button>
                </div>
              </form>
            </article>
            """
        )
    if not cards:
        return """
          <section class="lumae-panel" aria-label="Prepare Lumae">
            <h3>Prepare Lumae</h3>
            <p class="lumae-help">No supported AudioMuse music server is available yet.</p>
          </section>
        """
    return f"""
      <section class="lumae-panel" aria-label="Prepare Lumae">
        <h3>Prepare Lumae</h3>
        <p class="lumae-action-copy">The source-safe refresh publishes the provider catalogue and
          AudioMuse projection first. Waveform enrichment then advances in small, fair background
          batches while playback requests use the high-priority worker. AudioMuse 3.0.3 readiness
          confirmation remains a separate administrator safety step.</p>
        {''.join(cards)}
        {"<script>setTimeout(()=>location.reload(),5000)</script>" if auto_refresh else ""}
      </section>
    """


def render_settings(message=None, error=None):
    batch_size = configured_backfill_limit()
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
    readiness_html = render_v3_readiness_panel()
    preparation_html = render_source_preparation_panel(batch_size)
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
            <p>New songs receive profiles while AudioMuse analyzes them. After each analysis run,
              Lumae publishes one source-scoped catalogue and analysis update and queues any
              remaining profile work. Use these controls to catch up older library items.</p>
          </header>

          {preparation_html}
          {readiness_html}
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
            elif action == "ack_v3_readiness":
                if request.form.get("confirm") != "on":
                    raise ValueError("Explicit AudioMuse 3.0.3 confirmation is required")
                compatibility = detect_core()
                db = get_db()
                sources = resolve_catalog_source(db, server_id=request.form.get("server_id"))
                if (
                    len(sources) != 1
                    or sources[0]["catalog_instance_id"]
                    != request.form.get("catalog_instance_id")
                ):
                    raise ValueError("The selected catalogue source changed; reload and retry")
                result = acknowledge_v3_release(
                    db,
                    compatibility,
                    sources[0],
                    dedup_policy(),
                    request.form.get("verification_mode"),
                )
                message = (
                    f"AudioMuse 3.0.3 sync readiness confirmed for {sources[0]['name']} "
                    f"({result['verification_mode']})."
                )
            elif action == "clear_v3_readiness":
                clear_v3_release_acknowledgement(
                    request.form.get("catalog_instance_id") or ""
                )
                message = "AudioMuse 3.0.3 sync readiness confirmation revoked."
            elif action in ("prepare_lumae", "start_backfill", "catch_up", "queue_all"):
                batch_size = normalize_backfill_limit(
                    request.form.get("backfill_batch_size") or DEFAULT_BACKFILL_BATCH_SIZE
                )
                set_setting("backfill_batch_size", batch_size)
                source = resolve_profile_source(
                    catalog_instance_id=request.form.get("catalog_instance_id"),
                    server_id=request.form.get("server_id"),
                )
                catalog_instance_id = source["catalog_instance_id"]
                server_id = source["server_id"]
                if action == "prepare_lumae":
                    if not claim_preparation(source):
                        message = f"Preparation is already running for {source['name']}."
                    else:
                        try:
                            enqueue(
                                prepare_lumae_task,
                                server_id,
                                catalog_instance_id,
                                queue="default",
                            )
                        except Exception as exc:
                            update_preparation_state(
                                catalog_instance_id,
                                server_id,
                                "failed",
                                "queue_failed",
                                last_error=exc,
                                completed=True,
                            )
                            raise
                        message = (
                            f"Preparing {source['name']}: catalogue refresh, analysis projection, "
                            "then fair background waveform enrichment. Lumae can sync as soon as "
                            "the first two phases complete."
                        )
                else:
                    # Legacy catch_up/queue_all form submissions from an open
                    # 0.8.0 page intentionally map to the bounded 0.8.1 chain.
                    result = start_profile_backfill(
                        catalog_instance_id=catalog_instance_id,
                        server_id=server_id,
                    )
                    message = (
                        f"Background enrichment is already running for {source['name']}."
                        if result["coalesced"]
                        else f"Started background enrichment for {source['name']} in batches of "
                        f"{result['batch_size']}. Playback requests are prioritized separately."
                    )
            elif action == "save":
                batch_size = normalize_backfill_limit(
                    request.form.get("backfill_batch_size") or DEFAULT_BACKFILL_BATCH_SIZE
                )
                set_setting("backfill_batch_size", batch_size)
                message = "Lumae analysis settings saved."
        except (KeyError, ValueError, CatalogScanError, RuntimeError) as exc:
            error = str(exc)

    return render_settings(message=message, error=error)


def register(ctx):
    ctx.add_blueprint(bp)
    ctx.set_settings_page("lumae_analysis.settings")
    if collections_enabled():
        ctx.add_menu_item(COLLECTIONS_MENU_LABEL, COLLECTIONS_MENU_ENDPOINT)
    ctx.on_install(migrate)
    ctx.on_song_analyzed(analyze_song_hook)
    ctx.add_task("prepare", prepare_lumae_task, queue="default")
    ctx.add_task("profile_backfill", profile_backfill_task, queue="default")
    ctx.add_task("analysis_projection", analysis_projection_task, queue="default")
    ctx.add_cron_task("catalog_refresh", catalog_refresh_task, queue="default")
    ctx.add_cron_task("analysis_projection", analysis_projection_task, queue="default")
