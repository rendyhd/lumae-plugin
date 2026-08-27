import base64
import json
import os
from datetime import datetime, timezone
from html import escape

from flask import Blueprint, Response, g, jsonify, request, url_for

from plugin.api import config, enqueue, get_db, get_setting, logger, render_page, set_setting, table

from .loudness import (
    ProfileAnalysisTimeout,
    ProfileResourceLimitError,
    SilentAudioError,
    analyze_file,
)
from .core_compat import (
    SUPPORTED_CORE_RANGE,
    detect_core,
    get_core_adapter,
    sanitized_server_summaries,
)
from .catalog import (
    CATALOG_FINGERPRINT_SCHEMA_VERSION,
    attempt_legacy_rebind,
    CATALOG_BUILDER_VERSION,
    CatalogScanError,
    bootstrap_page,
    create_bootstrap_session,
    ensure_catalog_sources,
    migrate_catalog,
    prune_catalog_storage,
    read_catalog_changes,
    refresh_catalog,
    release_bootstrap_session,
    resolve_catalog_source,
    source_matches_server_id,
    verify_library_scope,
)
from .catalog_analysis import (
    dedup_policy,
    project_analysis,
    read_analysis_changes,
    scalar_batch,
    vector_batch,
)
from .catalog_enrichment import (
    RELATIONSHIP_ALGORITHM_VERSION,
    RELATIONSHIP_SCHEMA_VERSION,
    claim_relationship_preparation,
    compact_enrichment_storage,
    migrate_enrichment,
    prepare_relationships,
    profile_bootstrap_page,
    read_profile_changes,
    read_relationship_changes,
    record_profile_change,
    relationship_bootstrap_page,
    relationship_status,
    serialize_profile,
)
from .catalog_readiness import CONTRACT_REVISION, v3_release_readiness
from .catalog_providers import ProviderCatalogBridge, SUPPORTED_PROVIDER_TYPES
from .database_state import collect_database_state, render_database_state
from .settings_ui import SETTINGS_STATUS_SCRIPT
from .provider_identity_guard import (
    TRANSITION_BLOCKER,
    complete_projection_reconcile,
    migrate_provider_identity,
    observe_provider_version,
    projection_reconcile_required,
    provider_transition_health,
    require_projection_reconcile,
)
from .provider_identity_rekey import read_transition_manifest, refresh_audiomuse_health
from .collection_manager import (
    COLLECTIONS_BACKUP_VERSION,
    COLLECTIONS_SCHEMA_VERSION,
    collections_enabled,
    current_collection_scope,
    migrate_collections,
    register_collection_routes,
    render_collections_settings_panel,
)
from .reconcile import (
    arm_reconcile,
    begin_event,
    defer_work,
    discard_event,
    finish_event,
    iso as reconcile_iso,
    migrate_reconcile,
    progress_event,
    read_reconcile_status,
    reconcile_schedule_from_state,
    set_paused as set_reconcile_paused,
    update_work_retry,
)

SCHEMA_VERSION = 1
ANALYZER_VERSION = 1
PLUGIN_VERSION = "1.1.8"
CATALOG_SCHEMA_VERSION = 3
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
    "contract_admission_v1",
    "independent_stream_admission",
    "automatic_sonic_verification",
    "semantic_contracts_v1",
    "progressive_analysis_admission",
    "repair_flagged_analysis_admission",
    "database_state_dashboard",
    "provider_track_scope_verification",
    "source_scoped_profiles",
    "prepare_lumae",
    "catalog_prepare_api",
    "analysis_run_finalization",
    "catalog_ready_before_profile_backfill",
    "interactive_profile_priority",
    "bounded_profile_backfill",
    "automatic_catalog_preparation",
    "catalog_builder_versioning",
    "durable_catalog_reconciliation",
    "preparation_worker_attestation",
    "profile_cursor_stream",
    "server_album_artist_relationships",
    "relationship_cursor_stream",
    "nonblocking_enrichment",
    "provider_identity_transition_shield_v1",
    "provider_identity_rekey_v1",
    "bounded_storage_retention",
)
CATALOG_FEATURE_ROUTES = {
    "bootstrap_leases": (
        ("/api/catalog/bootstrap-sessions", "POST"),
        ("/api/catalog/bootstrap-sessions", "DELETE"),
        ("/api/catalog/bootstrap", "GET"),
    ),
    "cursor_changes": (("/api/catalog/changes", "GET"),),
    "refresh_on_demand": (("/api/catalog/refresh", "POST"),),
    "provider_track_scope_verification": (("/api/catalog/verify-scope", "POST"),),
    "source_scoped_profiles": (("/api/profiles", "GET"),),
    "shared_analysis": (
        ("/api/catalog/analysis/changes", "GET"),
        ("/api/catalog/analysis/scalars", "POST"),
        ("/api/catalog/analysis/vectors", "POST"),
    ),
    "catalog_prepare_api": (
        ("/api/catalog/prepare", "POST"),
        ("/api/catalog/prepare/<operation_id>", "GET"),
    ),
    "provider_identity_rekey_v1": (
        ("/api/catalog/provider-identity/manifest", "GET"),
    ),
}
BACKFILL_TASK_TYPE = "plugin.lumae_analysis.backfill"
CATALOG_REFRESH_TASK_TYPE = "plugin.lumae_analysis.catalog_refresh"
CATALOG_RECONCILE_TASK_TYPE = "plugin.lumae_analysis.catalog_reconcile"
PROVIDER_IDENTITY_RECHECK_TASK_TYPE = "plugin.lumae_analysis.provider_identity_recheck"
ANALYSIS_PROJECTION_TASK_TYPE = "plugin.lumae_analysis.analysis_projection"
DEFAULT_BACKFILL_BATCH_SIZE = 3
MAX_BACKFILL_BATCH_SIZE = 10
INTERACTIVE_PROFILE_CHUNK_SIZE = 3
MAX_INTERACTIVE_PROFILE_IDS = 12
PREPARATION_STALE_HOURS = 1
BACKFILL_STALE_MINUTES = 30
ANALYSIS_RUN_SETTLE_GRACE_MINUTES = 2
ANALYSIS_RUN_STALE_MINUTES = 30
PROFILE_JOB_TIMEOUT_SECONDS = 20 * 60
PROFILE_BACKFILL_JOB_TIMEOUT_SECONDS = 30 * 60
CATALOG_JOB_TIMEOUT_SECONDS = 90 * 60
RELATIONSHIP_JOB_TIMEOUT_SECONDS = 60 * 60
COLLECTIONS_MENU_LABEL = "Living Collections"
COLLECTIONS_MENU_ENDPOINT = "lumae_analysis.collection_manager_page"

bp = Blueprint("lumae_analysis", __name__)
register_collection_routes(bp)


def enqueue_bounded(func, *args, queue="default", timeout=None, **kwargs):
    """Queue root plugin work through AudioMuse's stable public API.

    ``timeout`` remains accepted for call-site and older-core compatibility,
    but queue internals are deliberately not imported by the plugin. AudioMuse
    owns execution limits and queue implementation details.
    """
    del timeout
    return enqueue(func, *args, queue=queue, **kwargs)


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


def maintenance_paused():
    value = get_setting("maintenance_paused", False)
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


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


def ensure_catalog_reconcile_schedule(db):
    """Install additive reconciliation state and an initially quiet schedule."""
    migrate_reconcile(db)


def ensure_provider_identity_recheck_schedule(db):
    cur = db.cursor()
    cur.execute(
        "INSERT INTO cron (name, task_type, cron_expr, enabled) "
        "VALUES (%s, %s, %s, TRUE) ON CONFLICT (task_type) DO UPDATE SET "
        "cron_expr=EXCLUDED.cron_expr, enabled=TRUE",
        (
            PROVIDER_IDENTITY_RECHECK_TASK_TYPE,
            PROVIDER_IDENTITY_RECHECK_TASK_TYPE,
            "2,32 * * * *",
        ),
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


def _resolve_task_server_id(adapter, server_id):
    requested = str(server_id or adapter.active_server_id() or "")
    if adapter.mode != "v3_registry" or requested != "legacy-default":
        return requested or None
    if any(
        str(server.get("server_id") or "") == requested
        for server in adapter.list_servers()
    ):
        return requested
    try:
        aliases = [
            source
            for source in resolve_catalog_source(get_db())
            if source.get("rebind_status") == "active"
            and source.get("continuity_from") == requested
        ]
    except Exception:
        aliases = []
    if len(aliases) == 1:
        return aliases[0]["server_id"]
    logger.warning(
        "lumae_analysis skipped stale v2 task for legacy-default while v3 source rebind is pending"
    )
    return None


def catalog_refresh_task(server_id=None):
    if maintenance_paused():
        return {"status": "paused", "reason": "maintenance_paused"}
    adapter = get_core_adapter()
    resolved_server_id = _resolve_task_server_id(adapter, server_id)
    if not resolved_server_id:
        return {"status": "skipped", "reason": "source_rebind_required"}
    return refresh_catalog(server_id=resolved_server_id)


def next_settled_analysis_run(db=None, server_id=None):
    """Return one durable run whose AudioMuse parent can no longer emit hooks."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT r.server_id, r.catalog_instance_id, r.run_id, r.retry_count
          FROM {analysis_runs_table()} r
          LEFT JOIN task_status parent
            ON parent.task_id=r.run_id AND parent.task_type='main_analysis'
         WHERE (r.status IN ('pending', 'registering', 'queued',
                             'enqueue_failed', 'failed')
                OR (r.status='running' AND r.updated_at
                    < now() - interval '{ANALYSIS_RUN_STALE_MINUTES} minutes'))
           AND (r.next_retry_at IS NULL OR r.next_retry_at <= now())
           AND (%s IS NULL OR r.server_id=%s)
           AND (
                parent.status IN ('SUCCESS', 'FAILURE', 'FAIL', 'REVOKED')
                OR (
                    parent.task_id IS NULL
                    AND r.last_seen_at
                        < now() - interval '{ANALYSIS_RUN_SETTLE_GRACE_MINUTES} minutes'
                    AND NOT EXISTS (
                        SELECT 1 FROM task_status live
                         WHERE live.parent_task_id IS NULL
                           AND live.task_type='main_analysis'
                           AND live.status IN (
                               'NEW', 'QUEUED', 'PENDING', 'STARTED',
                               'RUNNING', 'PROGRESS'
                           )
                    )
                )
           )
         ORDER BY r.last_seen_at, r.catalog_instance_id, r.run_id
         LIMIT 1
        """,
        (server_id, server_id),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return {
        "server_id": str(row[0]),
        "catalog_instance_id": str(row[1]),
        "run_id": str(row[2]),
        "retry_count": int(row[3] or 0) if len(row) > 3 else 0,
    }


def next_preparation_run(db=None, server_id=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT server_id, catalog_instance_id, retry_count
          FROM {preparation_state_table()}
         WHERE (status='queued'
            OR (status='failed' AND (next_retry_at IS NULL OR next_retry_at <= now()))
            OR (status='running' AND updated_at
                < now() - interval '{PREPARATION_STALE_HOURS} hours'))
           AND (%s IS NULL OR server_id=%s)
         ORDER BY updated_at, catalog_instance_id
         LIMIT 1
        """,
        (server_id, server_id),
    )
    row = cur.fetchone()
    cur.close()
    return (str(row[0]), str(row[1]), int(row[2] or 0)) if row else None


def next_relationship_run(db=None, server_id=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT s.current_core_server_id, r.catalog_instance_id, r.retry_count
          FROM {table('relationship_state')} r
          JOIN {table('catalog_sources')} s USING (catalog_instance_id)
         WHERE s.rebind_status='active'
           AND (r.status='queued'
                OR (r.status IN ('failed', 'waiting_for_index')
                    AND (r.next_retry_at IS NULL OR r.next_retry_at <= now()))
                OR (r.status='running' AND r.updated_at
                    < now() - interval '{PREPARATION_STALE_HOURS} hours'))
           AND (%s IS NULL OR s.current_core_server_id=%s)
         ORDER BY r.updated_at, r.catalog_instance_id
         LIMIT 1
        """,
        (server_id, server_id),
    )
    row = cur.fetchone()
    cur.close()
    return (str(row[0]), str(row[1]), int(row[2] or 0)) if row else None


def next_profile_backfill_run(db=None, server_id=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT server_id, catalog_instance_id, retry_count
          FROM {profile_backfill_state_table()}
         WHERE (status='queued'
            OR (status='failed' AND (next_retry_at IS NULL OR next_retry_at <= now()))
            OR (status='running' AND updated_at
                < now() - interval '{BACKFILL_STALE_MINUTES} minutes'))
           AND (%s IS NULL OR server_id=%s)
         ORDER BY updated_at, catalog_instance_id
         LIMIT 1
        """,
        (server_id, server_id),
    )
    row = cur.fetchone()
    cur.close()
    return (str(row[0]), str(row[1]), int(row[2] or 0)) if row else None


def _rollback_if_possible(db):
    rollback = getattr(db, "rollback", None)
    if callable(rollback):
        rollback()


def _safe_reconcile_schedule(db, paused=None):
    try:
        return reconcile_schedule_from_state(
            db,
            paused=maintenance_paused() if paused is None else bool(paused),
        )
    except Exception:
        _rollback_if_possible(db)
        logger.exception("lumae_analysis could not update the adaptive reconcile schedule")
        return None


def _safe_progress(phase, current=None, total=None):
    try:
        progress_event(phase, current=current, total=total)
    except Exception:
        db = get_db()
        _rollback_if_possible(db)
        logger.exception("lumae_analysis could not update reconcile progress")


def _reconcile_result_summary(result):
    if not isinstance(result, dict):
        return {"status": str(result)[:200]}
    allowed = (
        "status",
        "songs_seen",
        "processed",
        "attempted",
        "ready",
        "failed",
        "skipped",
        "already_ready",
        "promoted",
        "queued_next",
        "queued_profiles",
        "generation",
        "changes",
        "album_count",
        "artist_count",
        "track_count",
    )
    return {key: result[key] for key in allowed if key in result}


def _run_reconcile_action(
    db,
    action,
    server_id,
    catalog_instance_id,
    work_key,
    retry_count,
    function,
    *args,
):
    event_id = None
    try:
        event_id = begin_event(
            db,
            action,
            server_id,
            catalog_instance_id,
            work_key=work_key,
            attempt=int(retry_count or 0) + 1,
        )
    except Exception:
        _rollback_if_possible(db)
        logger.exception("lumae_analysis could not start the reconcile status event")
    try:
        _safe_progress("starting")
        result = function(*args)
        result_status = str((result or {}).get("status") or "complete")
        if result_status in ("coalesced", "already_finalized", "paused"):
            try:
                discard_event(db, event_id)
            except Exception:
                _rollback_if_possible(db)
            return result
        if result_status == "waiting_for_index":
            next_retry_at = None
            try:
                next_retry_at = defer_work(
                    db,
                    action,
                    catalog_instance_id,
                    work_key=work_key,
                    minutes=60,
                )
                finish_event(
                    db,
                    event_id,
                    "deferred",
                    phase="waiting for AudioMuse index",
                    summary=_reconcile_result_summary(result),
                    next_retry_at=next_retry_at,
                )
            except Exception:
                _rollback_if_possible(db)
                logger.exception("lumae_analysis could not record deferred reconcile work")
            return result
        if result_status in ("failed", "failure", "error"):
            raise RuntimeError(str((result or {}).get("error") or result_status))
        # Track failures do not mean the batch itself crashed. Keep its normal
        # retry lifecycle, but persist a warning verdict for the journal.
        has_track_failures = (
            action == "profile_backfill" and int((result or {}).get("failed") or 0) > 0
        )
        try:
            update_work_retry(
                db,
                action,
                catalog_instance_id,
                work_key=work_key,
                failed=False,
            )
            finish_event(
                db,
                event_id,
                "success",
                phase="completed with warnings" if has_track_failures else "complete",
                summary=_reconcile_result_summary(result),
            )
        except Exception:
            _rollback_if_possible(db)
            logger.exception("lumae_analysis could not finish reconcile success state")
        return result
    except Exception as exc:
        _rollback_if_possible(db)
        retry = {"next_retry_at": None}
        try:
            retry = update_work_retry(
                db,
                action,
                catalog_instance_id,
                work_key=work_key,
                failed=True,
            )
        except Exception:
            _rollback_if_possible(db)
            logger.exception("lumae_analysis could not record reconcile retry state")
        try:
            finish_event(
                db,
                event_id,
                "failed",
                phase="failed",
                error=exc,
                next_retry_at=retry.get("next_retry_at"),
            )
        except Exception:
            _rollback_if_possible(db)
            logger.exception("lumae_analysis could not finish the reconcile status event")
        raise


def catalog_reconcile_task():
    """Execute at most one durable action for the active source, then retune cadence."""
    db = get_db()
    if maintenance_paused():
        try:
            set_reconcile_paused(db, True)
        except Exception:
            _rollback_if_possible(db)
            logger.exception("lumae_analysis could not pause the reconcile schedule")
        return {
            "status": "paused",
            "requested": 0,
            "plugin_version": PLUGIN_VERSION,
            "catalog_builder_version": CATALOG_BUILDER_VERSION,
        }

    try:
        server_id = str(get_core_adapter().active_server_id() or "") or None
    except Exception:
        server_id = None

    requested = 0
    try:
        run = next_settled_analysis_run(db=db, server_id=server_id)
        if run:
            result = _run_reconcile_action(
                db,
                "analysis_run",
                run["server_id"],
                run["catalog_instance_id"],
                run["run_id"],
                run.get("retry_count", 0),
                finalize_analysis_run_task,
                run["server_id"],
                run["catalog_instance_id"],
                run["run_id"],
            )
            return {"status": "processed", "action": "analysis_run", "result": result}

        requested = enqueue_required_catalog_preparations(db=db, server_id=server_id)
        preparation = next_preparation_run(db=db, server_id=server_id)
        if preparation:
            result = _run_reconcile_action(
                db,
                "catalog_preparation",
                preparation[0],
                preparation[1],
                preparation[1],
                preparation[2] if len(preparation) > 2 else 0,
                prepare_lumae_task,
                *preparation[:2],
            )
            return {
                "status": "processed",
                "action": "catalog_preparation",
                "requested": int(requested),
                "result": result,
            }

        relationship = next_relationship_run(db=db, server_id=server_id)
        if relationship:
            result = _run_reconcile_action(
                db,
                "relationships",
                relationship[0],
                relationship[1],
                relationship[1],
                relationship[2] if len(relationship) > 2 else 0,
                relationship_preparation_task,
                *relationship[:2],
            )
            return {"status": "processed", "action": "relationships", "result": result}

        profile = next_profile_backfill_run(db=db, server_id=server_id)
        if profile:
            result = _run_reconcile_action(
                db,
                "profile_backfill",
                profile[0],
                profile[1],
                profile[1],
                profile[2] if len(profile) > 2 else 0,
                profile_backfill_task,
                *profile[:2],
            )
            return {"status": "processed", "action": "profile_backfill", "result": result}

        return {
            "status": "current",
            "requested": int(requested),
            "plugin_version": PLUGIN_VERSION,
            "catalog_builder_version": CATALOG_BUILDER_VERSION,
        }
    finally:
        _safe_reconcile_schedule(db)


def analysis_projection_task(server_id=None):
    if maintenance_paused():
        return {"status": "paused", "reason": "maintenance_paused"}
    adapter = get_core_adapter()
    resolved_server_id = _resolve_task_server_id(adapter, server_id)
    if not resolved_server_id:
        return {"status": "skipped", "reason": "source_rebind_required"}
    result = project_analysis(server_id=resolved_server_id, adapter=adapter)
    if not result.get("catalog_instance_id"):
        return result
    complete_projection_reconcile(get_db(), result["catalog_instance_id"])
    try:
        result["relationships"] = start_relationship_preparation(
            catalog_instance_id=result["catalog_instance_id"],
            server_id=resolved_server_id,
            enqueue_job=False,
        )
    except Exception as exc:
        logger.exception(
            "lumae_analysis could not request album and artist relationship preparation"
        )
        result["relationships"] = {
            "queued": False,
            "coalesced": False,
            "error": str(exc),
        }
    return result


def enqueue_required_catalog_preparations(db=None, server_id=None):
    """Durably request first-run and builder-upgrade preparation.

    The historical name remains API-compatible. No queue operation occurs;
    ``catalog_reconcile_task`` executes admitted work on a later watchdog tick.
    """
    if maintenance_paused():
        return 0
    db = db or get_db()
    try:
        sources = resolve_catalog_source(db)
    except Exception:
        logger.exception(
            "lumae_analysis could not discover catalogues requiring post-install preparation"
        )
        return 0
    requested = 0
    for source in sources:
        if server_id is not None and str(source.get("server_id") or "") != str(server_id):
            continue
        catalog = source.get("catalog") or {}
        if source.get("rebind_status") != "active":
            continue
        state = preparation_state(source["catalog_instance_id"], db=db)
        requires_publication = (
            int(catalog.get("generation") or 0) <= 0
            or bool(catalog.get("refresh_required", False))
            or not preparation_attestation_is_current(state)
        )
        if not requires_publication:
            continue
        try:
            if not claim_preparation(source, db=db):
                continue
            requested += 1
        except Exception:
            logger.exception(
                "lumae_analysis could not record required catalogue preparation"
            )
    return requested


def provider_identity_recheck_task(server_id=None):
    """Advance pending ID proofs and hand completed AudioMuse migrations off."""

    db = get_db()
    bridge = ProviderCatalogBridge()
    adapter = get_core_adapter()
    candidates = [
        server
        for server in bridge.list_servers()
        if server.get("supported") and (not server_id or server["server_id"] == server_id)
    ]
    results = []
    for server in candidates:
        sources = resolve_catalog_source(db, server_id=server["server_id"])
        transition = (
            provider_transition_health(db, sources[0]["catalog_instance_id"])
            if len(sources) == 1
            else None
        )
        if not transition:
            continue
        if transition.get("state") == "transition_pending":
            results.append(refresh_catalog(server_id=server["server_id"], db=db, bridge=bridge))
            continue
        if transition.get("state") != "applied":
            continue
        previous_health = transition.get("audiomuse_health")
        reconcile_required = projection_reconcile_required(
            db, sources[0]["catalog_instance_id"]
        )
        if previous_health == "ready" and not reconcile_required:
            continue
        result = {
            "catalog_instance_id": sources[0]["catalog_instance_id"],
            "server_id": server["server_id"],
            "previous_health": previous_health,
            "reconcile_required": reconcile_required,
            "projection_queued": False,
            "projection_processed": False,
        }
        try:
            health = previous_health
            if health != "ready":
                health = refresh_audiomuse_health(
                    db,
                    sources[0]["catalog_instance_id"],
                    server["server_id"],
                    adapter,
                    commit=False,
                )
            result["audiomuse_health"] = health
            if health == "ready":
                # Persist the transition-health verdict before projection does
                # its own transactional publication. This task already owns a
                # root worker slot, so it must not enqueue another root task.
                db.commit()
                result["projection"] = analysis_projection_task(server["server_id"])
                result["projection_processed"] = True
            else:
                db.commit()
        except Exception as exc:
            rollback = getattr(db, "rollback", None)
            if callable(rollback):
                rollback()
            logger.exception(
                "lumae_analysis could not reconcile AudioMuse provider migration for %s",
                server["server_id"],
            )
            result["error"] = str(exc)
        results.append(result)
    return {"checked": len(results), "results": results}


def observe_provider_identities_on_start():
    """Bounded, best-effort observation that never delays Flask startup indefinitely."""

    db = get_db()
    if db is None:
        return
    try:
        # AudioMuse records a plugin update even when an install hook fails. In
        # that case PostgreSQL rolls the hook transaction back while the new
        # plugin code remains active. Re-running this additive migration here
        # repairs the provider-identity tables before any startup read needs
        # them, including upgrades from releases that predate the transition
        # shield.
        migrate_provider_identity(db)
        db.commit()
    except Exception:
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        logger.exception("lumae_analysis could not ensure provider identity schema")
        return
    try:
        migrate_reconcile(db)
        db.commit()
    except Exception:
        _rollback_if_possible(db)
        logger.exception("lumae_analysis could not ensure reconcile schema")
    bridge = ProviderCatalogBridge()
    for server in bridge.list_servers():
        if not server.get("supported"):
            continue
        try:
            observe_provider_version(db, bridge, server["server_id"], commit=True)
        except Exception:
            rollback = getattr(db, "rollback", None)
            if callable(rollback):
                rollback()
            logger.exception(
                "lumae_analysis could not observe provider identity for %s",
                server["server_id"],
            )
    enqueue_required_catalog_preparations(db=db)
    _safe_reconcile_schedule(db)
    # The scheduled provider-identity recheck consumes durable transition
    # state. Startup never reaches into queue internals or creates root work.


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
    migrate_provider_identity(db)
    require_projection_reconcile(db)
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
            target_plugin_version TEXT,
            target_catalog_builder_version INTEGER,
            worker_plugin_version TEXT,
            worker_catalog_builder_version INTEGER,
            last_error TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    for column in (
        "target_plugin_version TEXT",
        "target_catalog_builder_version INTEGER",
        "worker_plugin_version TEXT",
        "worker_catalog_builder_version INTEGER",
    ):
        cur.execute(
            f"ALTER TABLE {preparation_state_table()} "
            f"ADD COLUMN IF NOT EXISTS {column}"
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
    # Releases through 1.1.6 could strand rows while importing AudioMuse/RQ
    # queue internals. Re-admit them to the database-driven reconciler.
    cur.execute(
        f"""
        UPDATE {analysis_runs_table()}
           SET status='pending', finalizer_job_id=NULL, last_error=NULL,
               completed_at=NULL, updated_at=now()
         WHERE status IN ('registering', 'queued', 'enqueue_failed', 'failed')
            OR (status='running' AND updated_at
                < now() - interval '{ANALYSIS_RUN_STALE_MINUTES} minutes')
        """
    )
    # Retarget work admitted by an older plugin process so a reloaded 1.1.8
    # worker can attest and execute it. The claim remains coalesced in SQL.
    cur.execute(
        f"""
        UPDATE {preparation_state_table()}
           SET status='queued', phase='queued',
               target_plugin_version=%s,
               target_catalog_builder_version=%s,
               worker_plugin_version=NULL,
               worker_catalog_builder_version=NULL,
               last_error=NULL, completed_at=NULL, updated_at=now()
         WHERE status IN ('queued', 'running', 'failed')
        """,
        (PLUGIN_VERSION, CATALOG_BUILDER_VERSION),
    )
    cur.close()
    migrate_enrichment(db)
    prune_catalog_storage(db)
    compact_enrichment_storage(db)
    migrate_collections(db)
    ensure_catalog_refresh_schedule(db)
    ensure_catalog_reconcile_schedule(db)
    ensure_provider_identity_recheck_schedule(db)
    ensure_analysis_projection_schedule(db)
    disable_legacy_backfill_schedule(db)
    db.commit()
    # Installation is schema work, not a reason to rebuild the full analysis
    # and relationship projections. Only a missing/stale catalogue publication
    # is admitted here; ordinary analysis hooks and explicit prepare requests
    # own later enrichment.
    enqueue_required_catalog_preparations(db=db)
    _safe_reconcile_schedule(db)
    # Provider and catalogue watchdogs consume the durable requests above.
    # Install hooks remain schema-only and never queue root work.


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
            return enqueue_bounded(
                analyze_tracks_task,
                ids,
                catalog_instance_id,
                server_id,
                priority,
                queue=queue_name,
                timeout=PROFILE_JOB_TIMEOUT_SECONDS,
            )
        return enqueue_bounded(
            analyze_tracks_task,
            ids,
            None,
            None,
            priority,
            queue=queue_name,
            timeout=PROFILE_JOB_TIMEOUT_SECONDS,
        )
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
    previous = None
    if catalog_instance_id:
        cur.execute(
            f"""
            SELECT sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                   analyzer_ver, media_signature, status
              FROM {source_profiles_table()}
             WHERE catalog_instance_id=%s AND track_id=%s
            """,
            (catalog_instance_id, track_id),
        )
        previous = cur.fetchone()
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
    if catalog_instance_id:
        public_payload = None
        if status == "ready":
            public_payload = serialize_profile(
                track_id,
                int(getattr(result, "sample_rate", 0)),
                int(getattr(result, "duration_ms", 0)),
                float(getattr(result, "ref_lufs", 0.0)),
                getattr(result, "start_ramp_blob", b""),
                getattr(result, "end_ramp_blob", b""),
                ANALYZER_VERSION,
                values[-3],
                media_sig,
            )
        ready_unchanged = (
            status == "ready"
            and previous is not None
            and previous[7] == "ready"
            and int(previous[0]) == int(getattr(result, "sample_rate", 0))
            and int(previous[1]) == int(getattr(result, "duration_ms", 0))
            and float(previous[2]) == float(getattr(result, "ref_lufs", 0.0))
            and _bytes(previous[3]) == getattr(result, "start_ramp_blob", b"")
            and _bytes(previous[4]) == getattr(result, "end_ramp_blob", b"")
            and int(previous[5]) == ANALYZER_VERSION
            and previous[6] == media_sig
        )
        removing_ready = previous is not None and previous[7] == "ready" and status != "ready"
        if not ready_unchanged and (status == "ready" or removing_ready):
            record_profile_change(
                cur,
                catalog_instance_id,
                track_id,
                status,
                public_payload,
            )
    db.commit()
    cur.close()


def catalog_capability():
    return {
        "contract_revision": CONTRACT_REVISION,
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
        "catalog_builder_version": CATALOG_BUILDER_VERSION,
        "supported_core_range": SUPPORTED_CORE_RANGE,
        "supported_provider_types": sorted(SUPPORTED_PROVIDER_TYPES),
        "features": list(CATALOG_FEATURES),
    }


def sync_contract(compatibility):
    """Describe breaking schemas and semantic formats independently of core version."""
    return {
        "revision": CONTRACT_REVISION,
        "producer": "lumae_analysis",
        "core_api_contract": compatibility.api_contract,
        "streams": {
            "catalog": {
                "schema_version": CATALOG_SCHEMA_VERSION,
                "semantic_contracts": [
                    "provider_track_ids_v1",
                    "complete_catalog_generation_v1",
                    "contiguous_change_journal_v1",
                ],
            },
            "analysis": {
                "schema_version": ANALYSIS_SCHEMA_VERSION,
                "semantic_contracts": [
                    "analysis_link_evidence_v1",
                    "musicnn_f32le_200_v1",
                    "clap_f32le_512_v1",
                    "audiomuse_musicnn_scalars_v1",
                ],
            },
            "profiles": {
                "schema_version": SCHEMA_VERSION,
                "analyzer_version": ANALYZER_VERSION,
                "semantic_contracts": ["lumae_playback_profile_v1"],
            },
            "relationships": {
                "schema_version": RELATIONSHIP_SCHEMA_VERSION,
                "algorithm_version": RELATIONSHIP_ALGORITHM_VERSION,
                "semantic_contracts": ["lumae_album_artist_relationships_v1"],
            },
        },
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
    if server_id and not source_matches_server_id(source, server_id):
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
            "sync_contract": sync_contract(compatibility),
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
                "sync_contract": sync_contract(compatibility),
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
        except Exception:
            db = None
            logger.exception("lumae_analysis could not open catalogue health database")
        if db is not None:
            try:
                bridge = ProviderCatalogBridge()
                for candidate in bridge.list_servers():
                    if candidate.get("supported"):
                        observe_provider_version(
                            db,
                            bridge,
                            candidate["server_id"],
                            commit=True,
                        )
            except Exception:
                logger.exception("lumae_analysis could not observe provider identity")
        try:
            if db is not None:
                persisted = resolve_catalog_source(db)
        except Exception:
            logger.exception("lumae_analysis could not read persisted catalogue health")
    if persisted:
        servers = persisted
        for server in servers:
            catalog_instance_id = server.get("catalog_instance_id")
            if not catalog_instance_id:
                continue
            try:
                state = preparation_state(catalog_instance_id, db=db)
                server["preparation"] = state
                if not preparation_attestation_is_current(state):
                    server["catalog"]["refresh_required"] = True
                    server["catalog"]["refresh_reason"] = "worker_version_mismatch"
            except Exception:
                logger.exception(
                    "lumae_analysis could not read catalogue preparation health"
                )
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
    if db is not None:
        guarded_servers = []
        for server in servers:
            transition = None
            catalog_instance_id = server.get("catalog_instance_id")
            if catalog_instance_id:
                try:
                    transition = provider_transition_health(db, catalog_instance_id)
                except Exception:
                    logger.exception(
                        "lumae_analysis could not read provider transition health for %s",
                        catalog_instance_id,
                    )
            if transition:
                server = {
                    **server,
                    "provider_identity_transition": transition,
                    "catalog_sync_allowed": transition["catalog_sync_allowed"],
                    "analysis_sync_allowed": transition["analysis_sync_allowed"],
                    "audiomuse_projection_ingest_allowed": transition[
                        "audiomuse_projection_ingest_allowed"
                    ],
                    "provider_mutations_allowed": transition["provider_mutations_allowed"],
                    "audiomuse_health": transition.get("audiomuse_health"),
                }
                if not transition["catalog_sync_allowed"] and server.get("v3_readiness"):
                    readiness = dict(server["v3_readiness"])
                    readiness["ready"] = False
                    readiness["analysis_sync_allowed"] = False
                    readiness["blockers"] = list(
                        dict.fromkeys([*(readiness.get("blockers") or []), TRANSITION_BLOCKER])
                    )
                    admission = dict(readiness.get("admission") or {})
                    for stream in ("catalog", "analysis"):
                        stream_admission = dict(admission.get(stream) or {})
                        stream_admission["admitted"] = False
                        stream_admission["status"] = "denied"
                        stream_admission["blockers"] = list(
                            dict.fromkeys(
                                [*(stream_admission.get("blockers") or []), TRANSITION_BLOCKER]
                            )
                        )
                        admission[stream] = stream_admission
                    readiness["admission"] = admission
                    server["v3_readiness"] = readiness
            guarded_servers.append(server)
        servers = guarded_servers
    payload = compatibility.as_dict()
    payload.update(
        {
            "plugin": "lumae_analysis",
            "plugin_version": PLUGIN_VERSION,
            "catalog_schema_version": CATALOG_SCHEMA_VERSION,
            "analysis_schema_version": ANALYSIS_SCHEMA_VERSION,
            "catalog_builder_version": CATALOG_BUILDER_VERSION,
            "capability": catalog_capability(),
            "sync_contract": sync_contract(compatibility),
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


def _preparation_api_payload(source, state=None):
    catalog = source.get("catalog") or {}
    analysis = source.get("analysis") or {}
    state = state if state is not None else preparation_state(source["catalog_instance_id"])
    catalog_ready = int(catalog.get("generation") or 0) > 0
    fingerprint_schema_version = int(
        catalog.get("fingerprint_schema_version", 1) or 1
    )
    fingerprint_current = (
        fingerprint_schema_version == CATALOG_FINGERPRINT_SCHEMA_VERSION
    )
    attestation_current = preparation_attestation_is_current(state)
    effective_refresh_required = (
        bool(catalog.get("refresh_required", False))
        or not fingerprint_current
        or not attestation_current
    )
    effective_refresh_reason = (
        "fingerprint_schema_rebase"
        if not fingerprint_current
        else (
            "worker_version_mismatch"
            if not attestation_current
            else catalog.get("refresh_reason")
        )
    )
    current = (
        catalog_ready
        and int(catalog.get("builder_version") or 0) >= CATALOG_BUILDER_VERSION
        and not effective_refresh_required
    )
    active = preparation_is_active(state)
    if active:
        status = state["status"]
        phase = state["phase"]
    elif current:
        status = "ready"
        phase = "ready"
    elif state and state.get("status") == "failed":
        status = "failed"
        phase = state.get("phase") or "failed"
    else:
        status = "required"
        phase = (
            effective_refresh_reason
            or ("analysis_projection" if catalog_ready else "catalog_refresh")
        )
    attestation_error = None
    if not attestation_current and not active:
        attestation_error = (
            "The catalogue worker did not attest the plugin version requested by the "
            "AudioMuse API. Restart AudioMuse workers; repair will retry automatically."
        )
    return {
        "operation_id": source["catalog_instance_id"],
        "status": status,
        "phase": phase,
        "server_id": source["server_id"],
        "catalog_instance_id": source["catalog_instance_id"],
        "catalog_ready": catalog_ready,
        "publication_current": current,
        "generation": int(catalog.get("generation") or 0),
        "counts": catalog.get("entity_counts") or {},
        "published_builder_version": int(catalog.get("builder_version") or 0),
        "current_builder_version": CATALOG_BUILDER_VERSION,
        "fingerprint_schema_version": fingerprint_schema_version,
        "current_fingerprint_schema_version": CATALOG_FINGERPRINT_SCHEMA_VERSION,
        "snapshot_estimated_bytes": int(
            catalog.get("snapshot_estimated_bytes", 0) or 0
        ),
        "last_scan_change_counts": catalog.get("last_scan_change_counts") or {},
        "last_scan_change_reason": catalog.get("last_scan_change_reason"),
        "last_scan_duration_ms": catalog.get("last_scan_duration_ms"),
        "refresh_required": effective_refresh_required,
        "refresh_reason": effective_refresh_reason,
        "analysis_ready": analysis.get("status") == "complete",
        "target_plugin_version": (state or {}).get("target_plugin_version"),
        "target_catalog_builder_version": (state or {}).get(
            "target_catalog_builder_version"
        ),
        "worker_plugin_version": (state or {}).get("worker_plugin_version"),
        "worker_catalog_builder_version": (state or {}).get(
            "worker_catalog_builder_version"
        ),
        "worker_attested": attestation_current,
        "last_error": (
            (state or {}).get("last_error")
            or attestation_error
            or catalog.get("last_error")
        ),
        "updated_at": (state or {}).get("updated_at"),
    }


def _resolve_preparation_source(body):
    sources = resolve_catalog_source(
        get_db(),
        server_id=body.get("server_id"),
        catalog_instance_id=body.get("catalog_instance_id"),
    )
    if len(sources) != 1:
        raise ValueError(
            "An explicit server_id is required when multiple music servers are configured."
        )
    source = sources[0]
    if source.get("rebind_status") != "active":
        raise CatalogScanError(
            "AudioMuse was upgraded; confirm source continuity before preparing Lumae."
        )
    return source


@bp.post("/api/catalog/prepare")
def catalog_prepare_api():
    try:
        body = _json_body()
        source = _resolve_preparation_source(body)
        initial = _preparation_api_payload(source)
        if initial["publication_current"]:
            return _private_json(initial, 200)
        if not preparation_is_active(preparation_state(source["catalog_instance_id"])):
            if claim_preparation(source):
                try:
                    enqueue_bounded(
                        prepare_lumae_task,
                        source["server_id"],
                        source["catalog_instance_id"],
                        queue="default",
                        timeout=CATALOG_JOB_TIMEOUT_SECONDS,
                    )
                except Exception:
                    logger.exception(
                        "lumae_analysis could not queue catalogue preparation; "
                        "the durable watchdog will retry it"
                    )
        payload = _preparation_api_payload(
            source, preparation_state(source["catalog_instance_id"])
        )
        response = _private_json(payload, 202)
        response.headers["Retry-After"] = "2"
        response.headers["Location"] = (
            f"/plugins/lumae_analysis/api/catalog/prepare/{source['catalog_instance_id']}"
        )
        return response
    except KeyError:
        return _catalog_error("source_not_found", "Catalogue source was not found.", 404)
    except CatalogScanError as exc:
        return _catalog_error("preparation_blocked", str(exc), 409)
    except ValueError as exc:
        return _catalog_error("invalid_preparation", str(exc), 400)
    except Exception:
        logger.exception("lumae_analysis could not queue catalogue preparation")
        return _catalog_error(
            "preparation_queue_failed",
            "Catalogue preparation could not be queued. Try again in a moment.",
            503,
        )


@bp.get("/api/catalog/prepare/<operation_id>")
def catalog_prepare_status_api(operation_id):
    try:
        sources = resolve_catalog_source(get_db(), catalog_instance_id=operation_id)
        if len(sources) != 1:
            raise KeyError("Unknown catalogue source")
        source = sources[0]
        payload = _preparation_api_payload(
            source, preparation_state(source["catalog_instance_id"])
        )
        response = _private_json(payload, 200, no_store=False)
        if payload["status"] in ("queued", "running"):
            response.headers["Retry-After"] = "2"
        return response
    except KeyError:
        return _catalog_error("source_not_found", "Catalogue source was not found.", 404)


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
        if source.get("rebind_status") != "active":
            return _catalog_error(
                "rebind_required",
                "AudioMuse was upgraded; confirm source continuity before refreshing.",
                409,
            )
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
        enqueue_bounded(
            catalog_refresh_task,
            source["server_id"],
            queue="default",
            timeout=CATALOG_JOB_TIMEOUT_SECONDS,
        )
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


@bp.get("/api/catalog/provider-identity/manifest")
def provider_identity_manifest_api():
    try:
        manifest = read_transition_manifest(
            get_db(),
            transition_id=request.args.get("transition_id"),
            catalog_instance_id=request.args.get("catalog_instance_id"),
        )
    except KeyError:
        return _catalog_error(
            "transition_manifest_not_found",
            "No retained provider identity manifest was found.",
            404,
        )
    except ValueError as exc:
        return _catalog_error("identity_required", str(exc), 400)
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2)
    response = Response(payload, mimetype="application/json")
    response.headers["Content-Disposition"] = (
        f'attachment; filename="lumae-provider-rekey-{manifest["transition_id"]}.json"'
    )
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Vary"] = "Authorization, Cookie"
    return response


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


@bp.get("/api/profiles/bootstrap")
def profiles_bootstrap_api():
    try:
        catalog_instance_id = str(request.args.get("catalog_instance_id") or "")
        if not catalog_instance_id:
            raise ValueError("catalog_instance_id is required")
        return _private_json(
            profile_bootstrap_page(
                get_db(),
                catalog_instance_id,
                page_token=request.args.get("page_token"),
                limit=request.args.get("limit", 250),
            ),
            no_store=False,
        )
    except KeyError:
        return _catalog_error(
            "bootstrap_required", "Profile bootstrap cursor expired.", 410
        )
    except (CatalogScanError, ValueError) as exc:
        return _catalog_error("invalid_profile_bootstrap", str(exc), 400)


@bp.get("/api/profiles/changes")
def profile_changes_api():
    cursor = request.args.get("cursor")
    if not cursor:
        return _catalog_error("cursor_required", "Profile cursor is required.", 400)
    try:
        return _private_json(
            read_profile_changes(
                get_db(),
                cursor,
                catalog_instance_id=request.args.get("catalog_instance_id"),
                limit=request.args.get("limit", 250),
            ),
            no_store=False,
        )
    except KeyError:
        return _catalog_error(
            "bootstrap_required", "Profile cursor expired or belongs to an old epoch.", 410
        )
    except (CatalogScanError, ValueError) as exc:
        return _catalog_error("invalid_cursor", str(exc), 400)


@bp.post("/api/catalog/relationships/prepare")
def relationships_prepare_api():
    try:
        body = _json_body(max_bytes=16_000)
        source = resolve_profile_source(
            catalog_instance_id=body.get("catalog_instance_id"),
            server_id=body.get("server_id"),
        )
        result = start_relationship_preparation(
            catalog_instance_id=source["catalog_instance_id"],
            server_id=source["server_id"],
        )
        return _private_json(
            {
                **result,
                "relationships": relationship_status(
                    get_db(), source["catalog_instance_id"]
                ),
            }
        )
    except (CatalogScanError, KeyError, ValueError) as exc:
        return _catalog_error("source_required", str(exc), 409)


@bp.get("/api/catalog/relationships/status")
def relationships_status_api():
    try:
        catalog_instance_id = str(request.args.get("catalog_instance_id") or "")
        if not catalog_instance_id:
            raise ValueError("catalog_instance_id is required")
        return _private_json(relationship_status(get_db(), catalog_instance_id))
    except (CatalogScanError, KeyError, ValueError) as exc:
        return _catalog_error("source_required", str(exc), 409)


@bp.get("/api/catalog/relationships/bootstrap")
def relationships_bootstrap_api():
    try:
        catalog_instance_id = str(request.args.get("catalog_instance_id") or "")
        if not catalog_instance_id:
            raise ValueError("catalog_instance_id is required")
        return _private_json(
            relationship_bootstrap_page(
                get_db(),
                catalog_instance_id,
                page_token=request.args.get("page_token"),
                limit=request.args.get("limit", 100),
            ),
            no_store=False,
        )
    except KeyError:
        return _catalog_error(
            "bootstrap_required", "Relationship bootstrap cursor expired.", 410
        )
    except (CatalogScanError, ValueError) as exc:
        return _catalog_error("invalid_relationship_bootstrap", str(exc), 400)


@bp.get("/api/catalog/relationships/changes")
def relationship_changes_api():
    cursor = request.args.get("cursor")
    if not cursor:
        return _catalog_error(
            "cursor_required", "Relationship cursor is required.", 400
        )
    try:
        return _private_json(
            read_relationship_changes(
                get_db(),
                cursor,
                catalog_instance_id=request.args.get("catalog_instance_id"),
                limit=request.args.get("limit", 250),
            ),
            no_store=False,
        )
    except KeyError:
        return _catalog_error(
            "bootstrap_required",
            "Relationship cursor expired or belongs to an old epoch.",
            410,
        )
    except (CatalogScanError, ValueError) as exc:
        return _catalog_error("invalid_cursor", str(exc), 400)


@bp.post("/api/analyze")
def analyze():
    if maintenance_paused():
        return _catalog_error(
            "maintenance_paused",
            "Lumae background analysis is paused by the administrator.",
            503,
        )
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
    if maintenance_paused():
        release_pending(
            [track_id],
            catalog_instance_id=catalog_instance_id,
            reason="Lumae background maintenance is paused",
        )
        return {"track_id": track_id, "status": "skipped_maintenance_paused"}
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
    except (ProfileAnalysisTimeout, ProfileResourceLimitError) as exc:
        logger.warning("lumae_analysis bounded profile rejection for %s: %s", track_id, exc)
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
    """Count a hook and durably request reconciliation after its parent settles."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        INSERT INTO {analysis_runs_table()}
            (catalog_instance_id, run_id, server_id, status, songs_seen,
             first_seen_at, last_seen_at, updated_at)
        VALUES (%s, %s, %s, 'pending', 1, now(), now(), now())
        ON CONFLICT (catalog_instance_id, run_id) DO UPDATE SET
            server_id=EXCLUDED.server_id,
            songs_seen={analysis_runs_table()}.songs_seen + 1,
            last_seen_at=now(),
            status=CASE
                WHEN {analysis_runs_table()}.status IN ('running', 'complete')
                    THEN {analysis_runs_table()}.status
                ELSE 'pending'
            END,
            finalizer_job_id=CASE
                WHEN {analysis_runs_table()}.status IN ('running', 'complete')
                    THEN {analysis_runs_table()}.finalizer_job_id
                ELSE NULL
            END,
            last_error=CASE
                WHEN {analysis_runs_table()}.status IN ('running', 'complete')
                    THEN {analysis_runs_table()}.last_error
                ELSE NULL
            END,
            retry_count=CASE
                WHEN {analysis_runs_table()}.status IN ('running', 'complete')
                    THEN {analysis_runs_table()}.retry_count
                ELSE 0
            END,
            next_retry_at=CASE
                WHEN {analysis_runs_table()}.status IN ('running', 'complete')
                    THEN {analysis_runs_table()}.next_retry_at
                ELSE NULL
            END,
            updated_at=now()
        RETURNING status, songs_seen
        """,
        (catalog_instance_id, run_id, server_id),
    )
    row = cur.fetchone()
    arm_reconcile(db, "analysis_hook")
    db.commit()
    cur.close()
    return {
        "queued": False,
        "coalesced": bool(row and int(row[1] or 0) > 1),
        "status": str(row[0]) if row else "pending",
    }


def claim_analysis_run(catalog_instance_id, run_id, finalizer_job_id=None, db=None):
    del finalizer_job_id
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {analysis_runs_table()}
           SET status='running', started_at=COALESCE(started_at, now()),
               last_error=NULL, next_retry_at=NULL, updated_at=now()
         WHERE catalog_instance_id=%s AND run_id=%s
           AND (status IN ('pending', 'registering', 'queued',
                           'enqueue_failed', 'failed')
                OR (status='running' AND updated_at
                    < now() - interval '{ANALYSIS_RUN_STALE_MINUTES} minutes'))
        RETURNING songs_seen
        """,
        (catalog_instance_id, run_id),
    )
    row = cur.fetchone()
    db.commit()
    cur.close()
    return int(row[0] or 0) if row else None


def finalize_analysis_run_task(server_id, catalog_instance_id, run_id):
    """Publish one complete source update after an AudioMuse analysis run settles."""
    if maintenance_paused():
        return {
            "status": "paused",
            "reason": "maintenance_paused",
            "run_id": run_id,
        }
    songs_seen = claim_analysis_run(catalog_instance_id, run_id)
    if songs_seen is None:
        return {"status": "already_finalized", "run_id": run_id}
    try:
        _safe_progress("refreshing catalogue")
        source = resolve_profile_source(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        if source["catalog_instance_id"] != catalog_instance_id:
            raise CatalogScanError("Catalogue identity changed before run finalization")
        catalog_result = refresh_catalog(server_id=server_id)
        if catalog_result["catalog_instance_id"] != catalog_instance_id:
            raise CatalogScanError("Catalogue identity changed during run finalization")
        _safe_progress("projecting AudioMuse analysis")
        projection_result = project_analysis(
            server_id=server_id,
            adapter=get_core_adapter(),
        )
        if projection_result["catalog_instance_id"] != catalog_instance_id:
            raise CatalogScanError("Analysis identity changed during run finalization")
        _safe_progress("admitting volume and ramp work")
        profile_result = start_profile_backfill(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
            enqueue_job=False,
        )
        try:
            _safe_progress("admitting relationship work")
            relationship_result = start_relationship_preparation(
                catalog_instance_id=catalog_instance_id,
                server_id=server_id,
                enqueue_job=False,
            )
        except Exception as exc:
            logger.exception(
                "lumae_analysis could not start relationship enrichment after analysis"
            )
            relationship_result = {
                "queued": False,
                "coalesced": False,
                "error": str(exc),
            }
        update_analysis_run(
            catalog_instance_id,
            run_id,
            "complete",
            queued_profiles=(profile_result["batch_size"] if profile_result["queued"] else 0),
            profile_jobs=(
                1
                if profile_result["queued"] and not profile_result.get("deferred")
                else 0
            ),
            completed=True,
        )
        return {
            "status": "complete",
            "run_id": run_id,
            "songs_seen": songs_seen,
            "catalog": catalog_result,
            "analysis": projection_result,
            "profiles": profile_result,
            "relationships": relationship_result,
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
    if catalog_instance_id and maintenance_paused():
        return {
            "track_id": hook_track_id(song),
            "status": "skipped_maintenance_paused",
        }
    if catalog_instance_id:
        run_id = str((event or {}).get("run_id") or "").strip()
        if run_id:
            try:
                record_analysis_run(source_server_id, catalog_instance_id, run_id)
            except Exception:
                logger.exception(
                    "lumae_analysis could not record source finalization for run %s",
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
    if maintenance_paused():
        return {"track_id": track_id, "status": "skipped_maintenance_paused"}
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
    except (ProfileAnalysisTimeout, ProfileResourceLimitError) as exc:
        logger.warning("lumae_analysis bounded profile rejection for %s: %s", track_id, exc)
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
    if maintenance_paused():
        release_pending(
            ids,
            catalog_instance_id=catalog_instance_id,
            reason="Lumae background maintenance is paused",
        )
        return {
            "attempted": 0,
            "ready": 0,
            "already_ready": 0,
            "promoted": 0,
            "failed": 0,
            "skipped": len(ids),
            "deferred": len(ids),
            "paused": True,
        }
    if priority == "background" and len(ids) > MAX_BACKFILL_BATCH_SIZE:
        # Drain 0.8.0's already-persisted 250-track RQ jobs quickly after an
        # upgrade. Their rows become retryable and one bounded chain owns the
        # remaining durable work; interactive requests can promote any of them now.
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
                    enqueue_job=False,
                )
            except Exception:
                logger.exception("lumae_analysis could not migrate a legacy backfill job")
        return {
            "attempted": 0,
            "ready": 0,
            "already_ready": 0,
            "promoted": 0,
            "failed": 0,
            "skipped": 0,
            "deferred": len(ids),
        }
    results = []
    attempted = 0
    for index, track_id in enumerate(ids):
        _safe_progress("analyzing volume and ramps", current=index, total=len(ids))
        if priority == "background" and catalog_instance_id:
            heartbeat_profile_backfill(catalog_instance_id)
        if maintenance_paused():
            remaining = ids[index:]
            release_pending(
                remaining,
                catalog_instance_id=catalog_instance_id,
                reason="Lumae background maintenance was paused during the batch",
            )
            results.extend(
                {"track_id": item_id, "status": "skipped_maintenance_paused"}
                for item_id in remaining
            )
            break
        disposition = profile_task_disposition(
            track_id,
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
            priority=priority,
        )
        if disposition != "analyze":
            results.append({"track_id": track_id, "status": disposition})
            continue
        attempted += 1
        results.append(
            analyze_one_track(
                track_id,
                catalog_instance_id=catalog_instance_id,
                server_id=server_id,
            )
        )
        _safe_progress("analyzing volume and ramps", current=index + 1, total=len(ids))
        if priority == "background" and catalog_instance_id:
            heartbeat_profile_backfill(catalog_instance_id)
    summary = {
        "attempted": attempted,
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


def fetch_backfill_rows(
    limit,
    catalog_instance_id=None,
    server_id=None,
    include_failed=False,
):
    """Select only one retryable profile batch in PostgreSQL."""
    db = get_db()
    cur = db.cursor()
    profile_table = source_profiles_table() if catalog_instance_id else profiles_table()
    profile_source_join = (
        "AND p.catalog_instance_id=source.catalog_instance_id" if catalog_instance_id else ""
    )
    source_filters = ""
    params = []
    if catalog_instance_id or server_id:
        source_filters = """
               AND (%s IS NULL OR s.catalog_instance_id=%s)
               AND (%s IS NULL OR s.current_core_server_id=%s)
        """
        params.extend((catalog_instance_id, catalog_instance_id, server_id, server_id))
    # Published catalogue occurrences are downloaded through
    # ProviderCatalogBridge. This must remain retryable even when a v3 registry
    # source has no matching legacy global MEDIASERVER_* configuration.
    retry_skipped = True
    params.extend(
        (
            ANALYZER_VERSION,
            bool(include_failed),
            retry_skipped,
            max(1, int(limit)),
        )
    )
    cur.execute(
        f"""
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
           AND COALESCE(p.status, '') NOT IN ('pending', 'pending_interactive')
           AND (
                p.track_id IS NULL
                OR p.analyzer_ver IS NULL
                OR p.analyzer_ver < %s
                OR p.status='stale'
                OR (
                    p.status='ready'
                    AND p.media_signature IS DISTINCT FROM
                        ('catalog-media:' || COALESCE(t.media_fp, ''))
                )
                OR (%s AND p.status='failed')
                OR (%s AND p.status='skipped_no_file')
           )
         ORDER BY t.track_id
         LIMIT %s
        """,
        tuple(params),
    )
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
    rows = fetch_backfill_rows(
        batch_limit,
        catalog_instance_id=catalog_instance_id,
        server_id=server_id,
        include_failed=include_failed,
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
    db = get_db()
    cur = db.cursor()
    profile_table = source_profiles_table() if catalog_instance_id else profiles_table()
    profile_source_join = (
        "AND p.catalog_instance_id=source.catalog_instance_id" if catalog_instance_id else ""
    )
    source_filters = ""
    params = []
    if catalog_instance_id or server_id:
        source_filters = """
               AND (%s IS NULL OR s.catalog_instance_id=%s)
               AND (%s IS NULL OR s.current_core_server_id=%s)
        """
        params.extend((catalog_instance_id, catalog_instance_id, server_id, server_id))
    # A published catalogue occurrence is retried through ProviderCatalogBridge,
    # including v3 registry sources whose credentials are not represented by
    # the legacy global MEDIASERVER_* configuration fields.
    retry_skipped = True
    params.extend((ANALYZER_VERSION, retry_skipped))
    cur.execute(
        f"""
        WITH source AS (
            SELECT s.catalog_instance_id, c.published_generation
              FROM {table('catalog_sources')} s
              JOIN {table('catalog_state')} c USING (catalog_instance_id)
             WHERE s.rebind_status='active' AND c.status='complete'
               {source_filters}
             ORDER BY s.is_default DESC, s.server_name, s.catalog_instance_id
             LIMIT 1
        )
        SELECT
            COUNT(*)::BIGINT,
            COUNT(*) FILTER (
                WHERE p.status='ready'
                  AND p.analyzer_ver >= %s
                  AND p.media_signature IS NOT DISTINCT FROM
                      ('catalog-media:' || COALESCE(t.media_fp, ''))
            )::BIGINT,
            COUNT(*) FILTER (
                WHERE p.status IN ('pending', 'pending_interactive')
            )::BIGINT,
            COUNT(*) FILTER (WHERE p.status='failed')::BIGINT,
            COUNT(*) FILTER (
                WHERE p.status='skipped_no_file' AND NOT %s
            )::BIGINT
          FROM source
          JOIN {table('catalog_tracks')} t
            ON t.catalog_instance_id=source.catalog_instance_id
           AND t.published_generation=source.published_generation
          LEFT JOIN {profile_table} p ON p.track_id=t.track_id
               {profile_source_join}
         WHERE t.available=TRUE AND t.analysis_eligible=TRUE
        """,
        tuple(params),
    )
    row = cur.fetchone() or (0, 0, 0, 0, 0)
    cur.close()
    total, ready, pending, failed, skipped = (int(value or 0) for value in row)
    return {
        "total_with_files": total,
        "ready_current": ready,
        "pending": pending,
        "failed": failed,
        "skipped": skipped,
        "needs_analysis": max(0, total - ready - pending - failed - skipped),
    }


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
    """Atomically admit one durable background workflow per catalogue source."""
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
            completed_at=NULL, retry_count=0, next_retry_at=NULL, updated_at=now()
        WHERE {profile_backfill_state_table()}.status NOT IN ('queued', 'running')
           OR {profile_backfill_state_table()}.updated_at
              < now() - interval '{BACKFILL_STALE_MINUTES} minutes'
        RETURNING catalog_instance_id
        """,
        (source["catalog_instance_id"], source["server_id"]),
    )
    claimed = cur.fetchone() is not None
    if claimed:
        arm_reconcile(db, "profile_backfill_admitted")
    db.commit()
    cur.close()
    return claimed


def claim_profile_backfill_batch(catalog_instance_id, db=None):
    """Claim one queued or interrupted batch for exclusive execution."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {profile_backfill_state_table()}
           SET status='running', last_error=NULL, next_retry_at=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
           AND (status='queued'
                OR (status='failed' AND (next_retry_at IS NULL OR next_retry_at <= now()))
                OR (status='running' AND updated_at
                    < now() - interval '{BACKFILL_STALE_MINUTES} minutes'))
        RETURNING catalog_instance_id
        """,
        (catalog_instance_id,),
    )
    claimed = cur.fetchone() is not None
    db.commit()
    cur.close()
    return claimed


def heartbeat_profile_backfill(catalog_instance_id, db=None):
    """Keep a long bounded waveform batch from looking abandoned."""
    db = db or get_db()
    if db is None:
        return False
    try:
        cur = db.cursor()
        cur.execute(
            f"UPDATE {profile_backfill_state_table()} SET updated_at=now() "
            "WHERE catalog_instance_id=%s AND status='running'",
            (catalog_instance_id,),
        )
        cur.close()
        db.commit()
        return True
    except Exception:
        _rollback_if_possible(db)
        logger.exception("lumae_analysis could not heartbeat profile backfill")
        return False


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
    return enqueue_bounded(
        profile_backfill_task,
        server_id,
        catalog_instance_id,
        queue="default",
        timeout=PROFILE_BACKFILL_JOB_TIMEOUT_SECONDS,
    )


def start_profile_backfill(catalog_instance_id=None, server_id=None, enqueue_job=True):
    if maintenance_paused():
        return {
            "queued": False,
            "coalesced": True,
            "paused": True,
            "batch_size": configured_backfill_limit(),
        }
    source = resolve_profile_source(
        catalog_instance_id=catalog_instance_id,
        server_id=server_id,
    )
    if not claim_profile_backfill(source):
        return {"queued": False, "coalesced": True, "batch_size": configured_backfill_limit()}
    if not enqueue_job:
        return {
            "queued": True,
            "coalesced": False,
            "deferred": True,
            "batch_size": configured_backfill_limit(),
        }
    try:
        job = enqueue_next_profile_backfill(source["server_id"], source["catalog_instance_id"])
    except Exception as exc:
        logger.exception(
            "lumae_analysis could not queue the first profile batch; "
            "the durable watchdog will retry it"
        )
        return {
            "queued": True,
            "coalesced": False,
            "deferred": True,
            "batch_size": configured_backfill_limit(),
            "error": str(exc),
        }
    return {
        "queued": True,
        "coalesced": False,
        "batch_size": configured_backfill_limit(),
        "job_id": getattr(job, "id", None),
    }


def profile_backfill_task(server_id, catalog_instance_id):
    """Process one small batch; the watchdog resumes a queued next batch."""
    if maintenance_paused():
        update_profile_backfill_state(
            catalog_instance_id,
            server_id,
            "queued",
            last_error=None,
        )
        return {"status": "paused", "processed": 0, "queued_next": False}
    if not claim_profile_backfill_batch(catalog_instance_id):
        return {
            "status": "coalesced",
            "processed": 0,
            "queued_next": False,
        }
    claimed_ids = []
    try:
        _safe_progress("selecting volume and ramp batch")
        resolve_profile_source(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
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
        return {
            "status": "queued",
            "processed": len(ids),
            "queued_next": False,
            "deferred": True,
            **result,
        }
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
    """Compatibility wrapper that starts one bounded durable workflow."""
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


def claim_relationship_preparation_run(catalog_instance_id, db=None):
    """Claim one queued or interrupted relationship build."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {table('relationship_state')}
           SET status='running', last_error=NULL,
               started_at=COALESCE(started_at, now()), next_retry_at=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
           AND (status='queued'
                OR (status IN ('failed', 'waiting_for_index')
                    AND (next_retry_at IS NULL OR next_retry_at <= now()))
                OR (status='running' AND updated_at
                    < now() - interval '{PREPARATION_STALE_HOURS} hours'))
        RETURNING catalog_instance_id
        """,
        (catalog_instance_id,),
    )
    claimed = cur.fetchone() is not None
    db.commit()
    cur.close()
    return claimed


def relationship_preparation_task(server_id, catalog_instance_id):
    if maintenance_paused():
        return {"status": "paused", "reason": "maintenance_paused"}
    if not claim_relationship_preparation_run(catalog_instance_id):
        return {"status": "coalesced", "reason": "already_running"}
    try:
        _safe_progress("loading relationship inputs")
        source = resolve_profile_source(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        if source["catalog_instance_id"] != catalog_instance_id:
            raise CatalogScanError("Catalogue identity changed before relationship preparation")
        return prepare_relationships(catalog_instance_id, progress=_safe_progress)
    except Exception as exc:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            f"""
            UPDATE {table('relationship_state')}
               SET status='failed', last_error=%s, completed_at=now(), updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (str(exc)[:2000], catalog_instance_id),
        )
        db.commit()
        cur.close()
        raise


def start_relationship_preparation(
    catalog_instance_id=None, server_id=None, enqueue_job=True
):
    """Request one coalesced relationship build without delaying readiness."""
    if maintenance_paused():
        return {
            "queued": False,
            "coalesced": True,
            "paused": True,
            "reason": "maintenance_paused",
        }
    source = resolve_profile_source(
        catalog_instance_id=catalog_instance_id,
        server_id=server_id,
    )
    current = relationship_status(get_db(), source["catalog_instance_id"])
    catalog_generation = int(source["catalog"]["generation"])
    analysis_generation = int(source["analysis"]["generation"])
    already_current = (
        current.get("status") == "complete"
        and int(current.get("source_catalog_generation") or 0) == catalog_generation
        and int(current.get("source_analysis_generation") or 0) == analysis_generation
        and int(current.get("schema_version") or 0) == RELATIONSHIP_SCHEMA_VERSION
        and int(current.get("algorithm_version") or 0) == RELATIONSHIP_ALGORITHM_VERSION
    )
    if already_current:
        return {
            "queued": False,
            "coalesced": True,
            "reason": "already_current",
        }
    if not claim_relationship_preparation(get_db(), source["catalog_instance_id"]):
        return {
            "queued": False,
            "coalesced": True,
            "reason": "already_running",
        }
    if not enqueue_job:
        return {
            "queued": True,
            "coalesced": False,
            "deferred": True,
        }
    try:
        job = enqueue_bounded(
            relationship_preparation_task,
            source["server_id"],
            source["catalog_instance_id"],
            queue="default",
            timeout=RELATIONSHIP_JOB_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.exception(
            "lumae_analysis could not queue relationship preparation; "
            "the durable watchdog will retry it"
        )
        return {
            "queued": True,
            "coalesced": False,
            "deferred": True,
            "error": str(exc),
        }
    return {
        "queued": True,
        "coalesced": False,
        "job_id": getattr(job, "id", None),
    }


def preparation_state(catalog_instance_id, db=None):
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT server_id, status, phase, queued_profiles, profile_jobs,
               target_plugin_version, target_catalog_builder_version,
               worker_plugin_version, worker_catalog_builder_version,
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
        "target_plugin_version": str(row[5]) if row[5] else None,
        "target_catalog_builder_version": int(row[6]) if row[6] is not None else None,
        "worker_plugin_version": str(row[7]) if row[7] else None,
        "worker_catalog_builder_version": int(row[8]) if row[8] is not None else None,
        "last_error": str(row[9]) if row[9] else None,
        "started_at": str(row[10]) if row[10] else None,
        "completed_at": str(row[11]) if row[11] else None,
        "updated_at": str(row[12]) if row[12] else None,
    }


def preparation_attestation_is_current(state):
    """Legacy completed rows are accepted; newly claimed work must attest."""
    if not state or not state.get("target_plugin_version"):
        return True
    target_builder = int(state.get("target_catalog_builder_version") or 0)
    worker_builder = int(state.get("worker_catalog_builder_version") or 0)
    return (
        state.get("worker_plugin_version") == state.get("target_plugin_version")
        and worker_builder >= target_builder
    )


def assert_preparation_worker_current(catalog_instance_id):
    state = preparation_state(catalog_instance_id)
    if not state or not state.get("target_plugin_version"):
        return
    expected_plugin = str(state["target_plugin_version"])
    expected_builder = int(state.get("target_catalog_builder_version") or 0)
    if PLUGIN_VERSION != expected_plugin or CATALOG_BUILDER_VERSION < expected_builder:
        raise RuntimeError(
            "AudioMuse queued catalogue preparation for Lumae Analysis "
            f"{expected_plugin} (builder {expected_builder}), but this worker is still "
            f"running {PLUGIN_VERSION} (builder {CATALOG_BUILDER_VERSION}). "
            "Restart the AudioMuse workers; the repair watchdog will retry automatically."
        )


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
             profile_jobs, target_plugin_version, target_catalog_builder_version,
             worker_plugin_version, worker_catalog_builder_version,
             last_error, started_at, completed_at, updated_at)
        VALUES (%s, %s, 'queued', 'queued', 0, 0, %s, %s, NULL, NULL,
                NULL, now(), NULL, now())
        ON CONFLICT (catalog_instance_id) DO UPDATE SET
            server_id=EXCLUDED.server_id,
            status='queued', phase='queued', queued_profiles=0, profile_jobs=0,
            target_plugin_version=EXCLUDED.target_plugin_version,
            target_catalog_builder_version=EXCLUDED.target_catalog_builder_version,
            worker_plugin_version=NULL, worker_catalog_builder_version=NULL,
            last_error=NULL, started_at=now(), completed_at=NULL,
            retry_count=0, next_retry_at=NULL, updated_at=now()
        WHERE {preparation_state_table()}.status NOT IN ('queued', 'running')
           OR {preparation_state_table()}.updated_at < now() - interval '{PREPARATION_STALE_HOURS} hours'
        RETURNING catalog_instance_id
        """,
        (
            source["catalog_instance_id"],
            source["server_id"],
            PLUGIN_VERSION,
            CATALOG_BUILDER_VERSION,
        ),
    )
    claimed = cur.fetchone() is not None
    if claimed:
        arm_reconcile(db, "catalog_preparation_admitted")
    db.commit()
    cur.close()
    return claimed


def claim_preparation_run(catalog_instance_id, db=None):
    """Claim one queued or interrupted catalogue preparation."""
    db = db or get_db()
    cur = db.cursor()
    cur.execute(
        f"""
        UPDATE {preparation_state_table()}
           SET status='running', phase='starting', last_error=NULL,
               started_at=COALESCE(started_at, now()), next_retry_at=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
           AND (status='queued'
                OR (status='failed' AND (next_retry_at IS NULL OR next_retry_at <= now()))
                OR (status='running' AND updated_at
                    < now() - interval '{PREPARATION_STALE_HOURS} hours'))
        RETURNING catalog_instance_id
        """,
        (catalog_instance_id,),
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
             profile_jobs, target_plugin_version, target_catalog_builder_version,
             worker_plugin_version, worker_catalog_builder_version,
             last_error, started_at, completed_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(),
                CASE WHEN %s THEN now() ELSE NULL END, now())
        ON CONFLICT (catalog_instance_id) DO UPDATE SET
            server_id=EXCLUDED.server_id,
            status=EXCLUDED.status,
            phase=EXCLUDED.phase,
            queued_profiles=EXCLUDED.queued_profiles,
            profile_jobs=EXCLUDED.profile_jobs,
            target_plugin_version=COALESCE(
                {preparation_state_table()}.target_plugin_version,
                EXCLUDED.target_plugin_version
            ),
            target_catalog_builder_version=COALESCE(
                {preparation_state_table()}.target_catalog_builder_version,
                EXCLUDED.target_catalog_builder_version
            ),
            worker_plugin_version=EXCLUDED.worker_plugin_version,
            worker_catalog_builder_version=EXCLUDED.worker_catalog_builder_version,
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
            PLUGIN_VERSION,
            CATALOG_BUILDER_VERSION,
            PLUGIN_VERSION,
            CATALOG_BUILDER_VERSION,
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
    if maintenance_paused():
        return {"status": "paused", "reason": "maintenance_paused"}
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
        if not claim_preparation_run(resolved_catalog_instance_id):
            return {
                "status": "coalesced",
                "reason": "already_running_or_not_requested",
                "catalog_instance_id": resolved_catalog_instance_id,
            }
        assert_preparation_worker_current(resolved_catalog_instance_id)
        _safe_progress("refreshing catalogue")
        update_preparation_state(
            resolved_catalog_instance_id,
            resolved_server_id,
            "running",
            "catalog_refresh",
        )
        catalog_result = refresh_catalog(server_id=resolved_server_id)
        if catalog_result["catalog_instance_id"] != resolved_catalog_instance_id:
            raise CatalogScanError("Catalogue identity changed during preparation")
        if (
            int(catalog_result.get("builder_version") or 0) < CATALOG_BUILDER_VERSION
            or catalog_result.get("refresh_required") is not False
        ):
            raise CatalogScanError(
                "Catalogue refresh finished without publishing the current builder version"
            )
        _safe_progress("projecting AudioMuse analysis")
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
            _safe_progress("admitting volume and ramp work")
            profile_result = start_profile_backfill(
                catalog_instance_id=resolved_catalog_instance_id,
                server_id=resolved_server_id,
                enqueue_job=False,
            )
        except Exception as exc:
            # The catalogue is already safe and usable. Enrichment failures
            # remain visible in their own state and never roll readiness back.
            logger.exception("lumae_analysis could not start background profile enrichment")
            profile_result = {"queued": False, "coalesced": False, "error": str(exc)}
        try:
            _safe_progress("admitting relationship work")
            relationship_result = start_relationship_preparation(
                catalog_instance_id=resolved_catalog_instance_id,
                server_id=resolved_server_id,
                enqueue_job=False,
            )
        except Exception as exc:
            logger.exception(
                "lumae_analysis could not start background relationship enrichment"
            )
            relationship_result = {
                "queued": False,
                "coalesced": False,
                "error": str(exc),
            }
        return {
            "catalog": catalog_result,
            "analysis": projection_result,
            "profiles": profile_result,
            "relationships": relationship_result,
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
    "analysis_links_missing": "Some eligible tracks do not have AudioMuse analysis yet.",
    "analysis_links_need_repair": "Some source-analysis links are flagged for automatic repair.",
    "analysis_links_pending": "Some eligible tracks are still awaiting AudioMuse analysis.",
    "analysis_mapping_incomplete": "Some eligible provider tracks do not have an AudioMuse mapping yet.",
    "analysis_projection_incomplete": "The plugin analysis projection is not complete.",
    "catalog_generation_incomplete": "The provider catalogue generation is not complete.",
    "chromaprint_backfill_incomplete": "Full-library verification is still waiting for Chromaprint; affected collision groups remain usable but provisional.",
    "chromaprint_collection_disabled": "Chromaprint collection is disabled in AudioMuse.",
    "chromaprint_gate_disabled": "Chromaprint duplicate validation is disabled in AudioMuse.",
    "duration_tolerance_too_wide": "The AudioMuse duplicate duration tolerance is wider than one second.",
    "folder_gate_not_active": "The fp_4 folder-aware duplicate rule is not active.",
    "fp_4_not_active": "AudioMuse catalogue ID scheme fp_4 is not active.",
    "no_analysis_mappings": "No provider tracks have AudioMuse analysis mappings yet.",
    "per_link_evidence_unavailable": "This plugin cannot qualify AudioMuse analysis links individually.",
    "provisional_links_remaining": "Some usable source-analysis links still have provisional evidence.",
    "readiness_unavailable": "The plugin could not read AudioMuse repair diagnostics.",
    "sonic_evidence_incomplete": "Full per-track sonic evidence is not complete yet.",
    "source_rebind_required": "Lumae still needs to verify the AudioMuse source identity during app sync.",
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


def _render_basic_source_analysis_panel():
    try:
        sources = resolve_catalog_source(get_db())
    except Exception:
        logger.exception("lumae_analysis could not render basic source analysis")
        sources = []
    cards = []
    for source in sources:
        analysis = source.get("analysis") or {}
        status = str(analysis.get("status") or "not_initialized")
        mapped = int(analysis.get("mapped_track_count") or 0)
        items = int(analysis.get("item_count") or 0)
        if status == "complete" and mapped > 0:
            status_label = "Ready"
            status_class = "lumae-source-state-ready"
            summary = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>AudioMuse source analysis is published for {mapped:,} provider tracks.</strong>
                <span>Lumae can consume these source features without repeating analysis on the
                  phone.</span>
              </div>
            """
        elif status in ("scanning", "building"):
            status_label = "Preparing"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>AudioMuse source analysis is being projected.</strong>
                <span>Available source features will be adopted automatically.</span>
              </div>
            """
        else:
            status_label = "Waiting for analysis"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>No usable AudioMuse source analysis is published yet.</strong>
                <span>Run AudioMuse Analysis or enable its analysis schedule. Lumae will adopt
                  the results automatically.</span>
              </div>
            """
        cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{escape(str(source['catalog_instance_id']))}"
              aria-label="AudioMuse source analysis for {escape(str(source.get('name') or source['server_id']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Music source</span>
                  <h4>{escape(str(source.get('name') or source['server_id']))}</h4>
                </div>
                <span class="lumae-source-state {status_class}">{status_label}</span>
              </header>
              {summary}
              <details>
                <summary>Technical details</summary>
                <div class="lumae-technical-details">
                  <p class="lumae-help">Projection status: {escape(status.replace('_', ' '))};
                    mapped provider tracks: {mapped:,}; AudioMuse analysis items: {items:,}.</p>
                </div>
              </details>
            </article>
            """
        )
    if not cards:
        cards.append(
            """
            <article class="lumae-source-card">
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Waiting for a supported music source.</strong>
                <span>Source-analysis status appears automatically after the library source is
                  initialized.</span>
              </div>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="AudioMuse source analysis status">
        <span class="lumae-section-priority lumae-section-advanced">2 - AudioMuse managed</span>
        <h3>2. AudioMuse source analysis</h3>
        <p class="lumae-action-copy">AudioMuse generates the raw MusiCNN, mood, energy, and
          fingerprint inputs. Lumae adopts them; the app does not repeat this work on the phone.</p>
        {''.join(cards)}
      </section>
    """


def render_v3_readiness_panel():
    try:
        sources = _v3_readiness_sources()
    except Exception:
        logger.exception("lumae_analysis could not render AudioMuse 3 readiness")
        return _render_basic_source_analysis_panel()
    if not sources:
        return _render_basic_source_analysis_panel()
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
        usable_links = int(readiness.get("ready_link_count") or 0)
        verified_links = int(readiness.get("verified_link_count") or 0)
        provisional_links = int(readiness.get("provisional_link_count") or 0)
        pending_links = int(readiness.get("pending_link_count") or 0)
        suspect_links = int(readiness.get("suspect_link_count") or 0)
        missing_links = int(readiness.get("missing_link_count") or 0)
        coverage = float(readiness.get("chromaprint_coverage") or 0) * 100
        task_evidence = readiness.get("task_evidence") or {}
        sequence = bool(task_evidence.get("upgrade_sequence_complete"))
        sequence_label = (
            "unavailable"
            if task_evidence.get("diagnostics_available") is False
            else ("yes" if sequence else "no")
        )
        fully_verified = bool(readiness.get("ready"))
        analysis_sync_allowed = bool(readiness.get("analysis_sync_allowed"))
        if "source_rebind_required" in blockers:
            status_label = "Waiting for source check"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Waiting for Lumae app sync to verify this source automatically.</strong>
                <span>No manual confirmation is needed. The next app sync will prove and adopt
                  the AudioMuse server identity when it matches.</span>
              </div>
            """
        elif fully_verified:
            status_label = "Ready"
            status_class = "lumae-source-state-ready"
            summary = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>AudioMuse source analysis is complete for {verified_links:,} eligible
                  tracks.</strong>
                <span>Mappings and fingerprint evidence are fully verified. Lumae can use the
                  complete source dataset without doing this work on the phone.</span>
              </div>
            """
        elif analysis_sync_allowed:
            status_label = "Preparing"
            status_class = "lumae-source-state-working"
            summary = f"""
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>AudioMuse source analysis is still filling in; {usable_links:,} tracks
                  are usable now.</strong>
                <span>{verified_links:,} are fully verified and {provisional_links:,} remain
                  provisional. AudioMuse’s Analysis task or schedule produces the missing source
                  data; Lumae adopts safe results automatically.</span>
              </div>
            """
        else:
            status_label = "Needs attention"
            status_class = "lumae-source-state-danger"
            summary = """
              <div class="lumae-notice lumae-notice-error" role="alert">
                <strong>AudioMuse source analysis is not safe to use yet.</strong>
                <span>Resolve the measurable issues listed in the technical details below.
                  A manual fresh/upgrade confirmation cannot override them.</span>
              </div>
            """
        cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{escape(str(source['catalog_instance_id']))}"
              aria-label="AudioMuse source analysis for {escape(str(source.get('name') or source['server_id']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Music source</span>
                  <h4>{escape(str(source.get('name') or source['server_id']))}</h4>
                </div>
                <span class="lumae-source-state {status_class}">{status_label}</span>
              </header>
              {summary}
              <details>
                <summary>Technical details</summary>
                <div class="lumae-technical-details">
                  <p class="lumae-help">Chromaprint: {fingerprinted:,} of {mapped:,} mapped tracks
                    ({coverage:.2f}%).</p>
                  <p class="lumae-help">Provider tracks eligible for analysis: {eligible:,};
                    mapped: {mapped:,}; without analysis mapping: {missing:,}. Unmapped provider
                    tracks remain in the Lumae library.</p>
                  <p class="lumae-help">Source-analysis links: {usable_links:,} usable
                    ({verified_links:,} verified; {provisional_links:,} provisional);
                    {pending_links:,} awaiting analysis; {suspect_links:,} flagged for repair;
                    {missing_links:,} not analyzed.</p>
                  <p class="lumae-help">Historical AudioMuse upgrade sequence observed:
                    {sequence_label} (diagnostic only; it does not gate readiness).</p>
                  {f'<ul class="lumae-help">{blocker_html}</ul>' if blocker_html else ''}
                </div>
              </details>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="AudioMuse source analysis status">
        <span class="lumae-section-priority lumae-section-advanced">2 - AudioMuse managed</span>
        <h3>2. AudioMuse source analysis</h3>
        <p class="lumae-action-copy">AudioMuse generates the raw MusiCNN, mood, energy, and
          Chromaprint inputs. Lumae verifies and adopts them progressively; the app does not
          repeat this analysis on the phone.</p>
        {''.join(cards)}
      </section>
    """


def render_relationship_status_panel():
    try:
        db = get_db()
        sources = resolve_catalog_source(db)
    except Exception:
        logger.exception("lumae_analysis could not render relationship preparation")
        return ""
    if not sources:
        return ""
    cards = []
    active_work = False
    for source in sources:
        catalog_instance_id = source["catalog_instance_id"]
        try:
            state = relationship_status(db, catalog_instance_id)
        except Exception as exc:
            logger.exception(
                "lumae_analysis could not read relationship status for %s",
                catalog_instance_id,
            )
            state = {
                "status": "failed",
                "last_error": str(exc),
            }
        status = str(state.get("status") or "not_initialized")
        catalog_generation = int(source.get("catalog", {}).get("generation") or 0)
        analysis_generation = int(source.get("analysis", {}).get("generation") or 0)
        current = (
            status == "complete"
            and int(state.get("source_catalog_generation") or 0) == catalog_generation
            and int(state.get("source_analysis_generation") or 0) == analysis_generation
            and int(state.get("schema_version") or 0) == RELATIONSHIP_SCHEMA_VERSION
            and int(state.get("algorithm_version") or 0) == RELATIONSHIP_ALGORITHM_VERSION
        )
        active = status in ("queued", "running")
        active_work = active_work or active
        albums = int(state.get("album_count") or 0)
        artists = int(state.get("artist_count") or 0)
        if current:
            status_label = "Ready"
            status_class = "lumae-source-state-ready"
            summary = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>Similarities are ready for {albums:,} albums and {artists:,} artists.</strong>
                <span>The plugin calculated these with Lumae’s own ranking algorithm. The app
                  downloads the results and does no relationship matching on the phone.</span>
              </div>
            """
        elif active:
            status_label = "Preparing"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Similar album and artist relationships are being prepared automatically.</strong>
                <span>This runs in the background and does not block library sync, playback,
                  or the currently published relationship generation.</span>
              </div>
            """
        elif status == "waiting_for_index":
            status_label = "Waiting for AudioMuse index"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>The bounded relationship build is waiting for AudioMuse's MusicNN index.</strong>
                <span>The previous published relationship generation remains available. Lumae
                  never falls back to an unbounded all-pairs scan.</span>
              </div>
            """
        elif status == "failed":
            status_label = "Needs attention"
            status_class = "lumae-source-state-danger"
            summary = f"""
              <div class="lumae-notice lumae-notice-error" role="alert">
                <strong>The last relationship build failed.</strong>
                <span>{escape(str(state.get('last_error') or 'The background worker will retry after the next source update.'))}</span>
              </div>
            """
        else:
            status_label = "Waiting for inputs" if catalog_generation == 0 else "Update pending"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>The automatic relationship build is waiting for published inputs.</strong>
                <span>Once the library and AudioMuse source generation are available, the plugin
                  queues Lumae’s album and artist algorithm automatically.</span>
              </div>
            """
        cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{escape(str(catalog_instance_id))}"
              aria-label="Similar albums and artists for {escape(str(source.get('name') or source['server_id']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Music source</span>
                  <h4>{escape(str(source.get('name') or source['server_id']))}</h4>
                </div>
                <span class="lumae-source-state {status_class}">{status_label}</span>
              </header>
              {summary}
              <details>
                <summary>Technical details</summary>
                <div class="lumae-technical-details">
                  <p class="lumae-help">Relationship status: {escape(status.replace('_', ' '))};
                    result generation: {int(state.get('generation') or 0):,};
                    albums: {albums:,}; artists: {artists:,}.</p>
                  <p class="lumae-help">Built from library generation
                    {int(state.get('source_catalog_generation') or 0):,} of {catalog_generation:,}
                    and source-analysis generation
                    {int(state.get('source_analysis_generation') or 0):,} of {analysis_generation:,}.
                    Algorithm version: {int(state.get('algorithm_version') or 0):,}.</p>
                </div>
              </details>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="Similar albums and artists status"
        data-lumae-active="{str(active_work).lower()}">
        <span class="lumae-section-priority lumae-section-optional">4 - Automatic background</span>
        <h3>4. Similar albums &amp; artists</h3>
        <p class="lumae-action-copy">The plugin runs Lumae’s own album and artist relationship
          algorithm from the published library and AudioMuse source inputs. It automatically
          rebuilds when either input generation changes.</p>
        {''.join(cards)}
      </section>
    """


def _published_track_count(source):
    entity_counts = (source.get("catalog") or {}).get("entity_counts") or {}
    value = entity_counts.get("track")
    if value is None:
        value = entity_counts.get("tracks")
    return max(int(value or 0), 0)


def render_source_preparation_sections(batch_size):
    try:
        sources = resolve_catalog_source(get_db())
    except Exception:
        logger.exception("lumae_analysis could not render source preparation")
        return "", ""
    catalogue_cards = []
    waveform_cards = []
    active_work = False
    paused = maintenance_paused()
    for source in sources:
        catalog_instance_id = source["catalog_instance_id"]
        server_id = source["server_id"]
        counts = analysis_status_counts(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        state = preparation_state(catalog_instance_id)
        backfill = profile_backfill_state(catalog_instance_id)
        published_tracks = _published_track_count(source)
        total = int(counts["total_with_files"])
        ready = int(counts["ready_current"])
        coverage = min(max(int(round((ready / total) * 100)) if total else 0, 0), 100)
        queueable = int(counts["needs_analysis"])
        preparation_active = preparation_is_active(state)
        backfill_active = profile_backfill_is_active(backfill)
        active_work = active_work or preparation_active or backfill_active
        catalog_status = str(source["catalog"]["status"] or "not initialized")
        projection_status = str(source["analysis"]["status"] or "not initialized")
        catalogue_ready = catalog_status == "complete" and published_tracks > 0
        app_ready = catalogue_ready and projection_status == "complete"
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
        prepare_disabled = " disabled" if preparation_active or paused else ""
        backfill_disabled = (
            " disabled" if backfill_active or queueable == 0 or paused else ""
        )
        if app_ready:
            source_status = "Ready for app sync"
            source_status_class = "lumae-source-state-ready"
            readiness_notice = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>Ready for app sync: {published_tracks:,} Navidrome tracks are published.</strong>
                <span>The library catalogue and app sync index are complete. Volume, ramp, and
                  sonic coverage are reported separately below.</span>
              </div>
            """
        elif catalog_status == "complete" and published_tracks == 0:
            source_status = "Not ready - empty catalogue"
            source_status_class = "lumae-source-state-danger"
            readiness_notice = """
              <div class="lumae-notice lumae-notice-error" role="alert">
                <strong>Not ready: no Navidrome tracks were published.</strong>
                <span>This is not a usable Lumae catalogue. Check Navidrome access and the
                  <em>Music Libraries</em> selection in AudioMuse, then refresh required data.
                  Lumae will no longer publish a new empty catalogue.</span>
              </div>
            """
        elif preparation_active:
            source_status = "Preparing required data"
            source_status_class = "lumae-source-state-working"
            readiness_notice = f"""
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Not ready yet: preparation is in progress.</strong>
                <span>Current phase: {escape(str(phase).replace("_", " "))}.</span>
              </div>
            """
        else:
            source_status = "Not ready"
            source_status_class = "lumae-source-state-danger"
            readiness_notice = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Not ready for app sync.</strong>
                <span>Publish a non-empty Navidrome catalogue and complete the app sync
                  index by refreshing the required data.</span>
              </div>
            """
        profiles_complete = total > 0 and ready >= total
        if profiles_complete:
            profile_status = "Ready"
            profile_status_class = "lumae-source-state-ready"
        elif backfill_active:
            profile_status = "Preparing"
            profile_status_class = "lumae-source-state-working"
        elif int(counts["failed"]) > 0 and queueable == 0:
            profile_status = "Needs attention"
            profile_status_class = "lumae-source-state-danger"
        elif total == 0:
            profile_status = "Waiting for library"
            profile_status_class = "lumae-source-state-working"
        else:
            profile_status = "Not complete"
            profile_status_class = "lumae-source-state-working"
        catalogue_cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{escape(str(catalog_instance_id))}"
              aria-label="Catalogue readiness for {escape(str(source['name']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Navidrome source</span>
                  <h4>{escape(str(source.get('name') or server_id))}</h4>
                </div>
                <span class="lumae-source-state {source_status_class}">{source_status}</span>
              </header>
              {readiness_notice}
              <div class="lumae-status-grid" aria-label="Required data status">
                <div class="lumae-status-card {'lumae-status-ready' if published_tracks else 'lumae-status-failed'}">
                  <span>Published tracks</span>
                  <strong>{published_tracks:,}</strong>
                </div>
                <div class="lumae-status-card {'lumae-status-ready' if catalogue_ready else 'lumae-status-attention'}">
                  <span>Library catalogue</span>
                  <strong>{escape(catalog_status.replace('_', ' '))}</strong>
                </div>
                <div class="lumae-status-card {'lumae-status-ready' if projection_status == 'complete' else 'lumae-status-attention'}">
                  <span>App sync index</span>
                  <strong>{escape(projection_status.replace('_', ' '))}</strong>
                </div>
              </div>
              {f'<p class="lumae-notice lumae-notice-error">{escape(last_error)}</p>' if last_error else ''}
              <form class="lumae-form" method="post">
                {hidden}
                <div class="lumae-actions">
                  <button class="lumae-button-primary" type="submit" name="action"
                    value="prepare_lumae"{prepare_disabled}>Refresh required data</button>
                </div>
              </form>
              <p class="lumae-help">This refresh imports the selected Navidrome libraries first,
                then publishes the app sync index. Volume, ramp, and sonic work can continue in
                the background after the library becomes ready.</p>
            </article>
            """
        )
        waveform_cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{escape(str(catalog_instance_id))}"
              aria-label="Volume and ramp status for {escape(str(source['name']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Navidrome source</span>
                  <h4>{escape(str(source.get('name') or server_id))}</h4>
                </div>
                <span class="lumae-source-state {profile_status_class}">{profile_status}</span>
              </header>
              <div class="lumae-meter" role="progressbar" aria-label="Ready volume and ramp profiles"
                aria-valuemin="0" aria-valuemax="100" aria-valuenow="{coverage}">
                <div class="lumae-meter-fill" style="width: {coverage}%;"></div>
              </div>
              <p class="lumae-help"><strong>{ready:,} of {total:,} volume and ramp profiles ready.</strong>
                {counts['pending']:,} pending; {queueable:,} need analysis;
                {counts['failed']:,} failed; {counts['skipped']:,} skipped.
                Background worker: {escape(backfill_status.replace('_', ' '))}.</p>
              <p class="lumae-help">These profiles power volume normalization and SmoothFade
                ramps. They are prepared from audio waveforms in the background and do not block
                library sync, AudioMuse source analysis, or Lumae relationships.</p>
              {f'<p class="lumae-notice lumae-notice-error">Volume and ramp preparation: {escape(backfill_error)}</p>' if backfill_error else ''}
              <form class="lumae-form" method="post">
                {hidden}
                <label class="lumae-field">
                  <span>Tracks per background batch (1-{MAX_BACKFILL_BATCH_SIZE})</span>
                  <input name="backfill_batch_size" value="{batch_size}" inputmode="numeric">
                </label>
                <div class="lumae-actions">
                  <button class="lumae-button-secondary" type="submit" name="action"
                    value="start_backfill"{backfill_disabled}>Prepare missing volume &amp; ramps</button>
                </div>
              </form>
            </article>
            """
        )
    if not catalogue_cards:
        return """
          <section class="lumae-panel" aria-label="Library status">
            <span class="lumae-section-priority">1 - Required</span>
            <h3>1. Library status</h3>
            <p class="lumae-help">No supported AudioMuse music server is available yet.</p>
          </section>
        """, ""
    catalogue_html = f"""
      <section class="lumae-panel" aria-label="Library status">
        <span class="lumae-section-priority">1 - Required</span>
        <h3>1. Library status</h3>
        <p class="lumae-action-copy">“Ready for app sync” has one precise meaning: at least one
          Navidrome track is published and the matching app sync index is complete.
          A completed job with zero tracks is not ready.</p>
        {''.join(catalogue_cards)}
      </section>
    """
    waveform_html = f"""
      <section class="lumae-panel" aria-label="Volume and ramp status"
        data-lumae-active="{str(active_work).lower()}">
        <span class="lumae-section-priority lumae-section-optional">3 - Automatic background</span>
        <h3>3. Volume &amp; ramp status</h3>
        <p class="lumae-action-copy">Loudness profiles normalize volume and MixRamp profiles power
          SmoothFade. Their progress is independent from library readiness, AudioMuse source
          analysis, and the similar-album/artist relationship build.</p>
        {''.join(waveform_cards)}
      </section>
    """
    return catalogue_html, waveform_html


def render_source_preparation_panel(batch_size):
    """Return the required and optional source sections as one HTML fragment."""
    catalogue_html, waveform_html = render_source_preparation_sections(batch_size)
    return f"{catalogue_html}{waveform_html}"


def render_provider_identity_panel():
    db = None
    try:
        db = get_db()
        sources = resolve_catalog_source(db) if db is not None else []
        rows = []
        for source in sources:
            transition = provider_transition_health(db, source["catalog_instance_id"])
            if transition:
                rows.append((source, transition))
    except Exception:
        # psycopg2 leaves the entire request transaction aborted after an SQL
        # error. The status panel is optional, so restore the connection before
        # later settings panels call the plugin settings API.
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        logger.exception("lumae_analysis could not render provider identity status")
        return ""
    if not rows:
        return ""

    cards = []
    for source, transition in rows:
        state = escape(str(transition.get("state") or "normal"))
        version = escape(str(transition.get("current_provider_version") or "unverified"))
        action = escape(str(transition.get("required_action") or "No action required"))
        counts = transition.get("counts") or {}
        baseline = (
            "passed"
            if transition.get("baseline_integrity") is True
            else ("pending" if transition.get("baseline_integrity") is None else "failed")
        )
        audiomuse_health = escape(
            str(transition.get("audiomuse_health") or "not checked")
        )
        scan_count = int(transition.get("target_scan_count") or 0)
        manifest_link = ""
        if transition.get("state") == "applied" and transition.get("transition_id"):
            manifest_link = (
                '<a class="lumae-button lumae-button-secondary" '
                'href="/api/catalog/provider-identity/manifest?transition_id='
                f'{escape(str(transition["transition_id"]))}">Download transition manifest</a>'
            )
        cards.append(
            f"""
            <article class="lumae-status-card">
              <span>{escape(source['name'])}</span>
              <strong>{state}</strong>
              <small>Navidrome {version}</small>
              <small>Stable target scans: {scan_count}/2</small>
              <small>Exact changes: {int(counts.get('rekey', 0) or 0):,} rekeys,
                {int(counts.get('addition', 0) or 0):,} additions,
                {int(counts.get('confirmed_removal', 0) or 0):,} removals,
                {int(counts.get('conflict', 0) or 0):,} conflicts</small>
              <small>Stored analysis baseline: {baseline}; AudioMuse: {audiomuse_health}</small>
              <small>{action}</small>
              <div class="lumae-actions">{manifest_link}</div>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="Provider identity transition">
        <h3>Provider identity safety</h3>
        <p class="lumae-help">Lumae freezes publication at the old complete generation,
          requires two identical provider scans, and then applies only the exact Navidrome
          canonical-ID transform in one database transaction. AudioMuse health is checked
          separately and never authorizes the Lumae rekey.</p>
        <div class="lumae-status-grid">{''.join(cards)}</div>
        <div class="lumae-actions">
          <a class="lumae-button lumae-button-secondary" href="/backup">Open AudioMuse Backup</a>
          <a class="lumae-button lumae-button-secondary" href="/provider-migration">Open Provider Migration</a>
          <a class="lumae-button lumae-button-secondary" href="">Check again</a>
        </div>
      </section>
    """


def _reconcile_duration(milliseconds):
    if milliseconds is None:
        return "running"
    value = max(0, int(milliseconds or 0))
    if value < 1000:
        return f"{value} ms"
    seconds = value / 1000
    if seconds < 60:
        return f"{seconds:.1f} s"
    return f"{seconds / 60:.1f} min"


def _reconcile_event_summary(event):
    summary = event.get("summary") or {}
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except (TypeError, ValueError):
            summary = {}
    parts = []
    for key in (
        "songs_seen",
        "processed",
        "attempted",
        "ready",
        "failed",
        "skipped",
        "already_ready",
        "promoted",
        "generation",
        "changes",
        "album_count",
        "artist_count",
        "track_count",
    ):
        if key in summary:
            parts.append(f"{key.replace('_', ' ')}: {escape(str(summary[key]))}")
    return "; ".join(parts) or escape(str(summary.get("status") or event.get("phase") or "complete"))


def render_reconcile_status_panel():
    try:
        snapshot = read_reconcile_status(get_db())
    except Exception:
        db = get_db()
        _rollback_if_possible(db)
        logger.exception("lumae_analysis could not render reconcile status")
        return """
          <section class="lumae-panel" aria-label="Background reconcile status">
            <span class="lumae-section-priority lumae-section-advanced">Scheduler</span>
            <h3>Background reconcile status is unavailable</h3>
            <p class="lumae-help">The operational status tables could not be read. Published
              Lumae data is unaffected; restart AudioMuse after verifying the plugin migration.</p>
          </section>
        """
    control = snapshot["control"]
    mode = control.get("mode") or "unknown"
    cadence = {
        "active": "Every minute while work is ready",
        "waiting": "Every five minutes while AudioMuse analysis finishes",
        "backoff": f"Retry schedule: {control.get('cron_expr') or 'adaptive'}",
        "idle": "Hourly safety sweep at :11",
        "paused": "Paused; hourly safety check at :11",
    }.get(mode, "Schedule unavailable")
    pending = snapshot.get("pending") or {}
    pending_html = "".join(
        f"<li><strong>{int(count):,}</strong> {escape(str(label))}</li>"
        for label, count in pending.items()
    ) or "<li><strong>0</strong> pending actions</li>"
    events = snapshot.get("events") or []
    running = next((event for event in events if event.get("status") == "running"), None)
    running_html = ""
    if running:
        progress = ""
        if running.get("progress_total") is not None:
            progress = (
                f" · {int(running.get('progress_current') or 0):,}/"
                f"{int(running.get('progress_total') or 0):,}"
            )
        running_html = f"""
          <div class="lumae-notice lumae-notice-info" role="status">
            <strong>{escape(str(running.get('action') or 'background work').replace('_', ' ').title())}</strong>
            <span>{escape(str(running.get('phase') or 'running'))}{progress}; attempt
              {int(running.get('attempt') or 1)}.</span>
          </div>
        """
    rows = []
    for event in events:
        if event.get("status") == "running":
            continue
        status = str(event.get("status") or "unknown")
        if status == "success" and event.get("phase") == "completed with warnings":
            status = "completed with warnings"
        retry = (
            f"; retry {escape(reconcile_iso(event.get('next_retry_at')) or '')}"
            if event.get("next_retry_at")
            else ""
        )
        error = (
            f'<div class="lumae-help">{escape(str(event.get("last_error")))}</div>'
            if event.get("last_error")
            else ""
        )
        rows.append(
            f"""
            <li>
              <strong>{escape(str(event.get('action') or '').replace('_', ' ').title())}</strong>
              — {escape(status)},
              {_reconcile_duration(event.get('duration_ms'))}{retry}
              <div class="lumae-help">{_reconcile_event_summary(event)}</div>
              {error}
            </li>
            """
        )
    journal_html = "".join(rows) or "<li>No meaningful background actions recorded yet.</li>"
    next_retry = (
        f"<p class=\"lumae-help\">Next retry: "
        f"{escape(reconcile_iso(control.get('next_retry_at')) or '')}.</p>"
        if control.get("next_retry_at")
        else ""
    )
    return f"""
      <section class="lumae-panel" aria-label="Background reconcile status"
        data-lumae-active="{str(bool(running)).lower()}">
        <span class="lumae-section-priority lumae-section-advanced">Scheduler</span>
        <h3>Background reconcile is {escape(str(mode).replace('_', ' '))}</h3>
        <p class="lumae-action-copy">{escape(cadence)}. AudioMuse may label these tasks
          “Songs analyzed: 0”; the action and phase below are Lumae’s authoritative status.</p>
        {running_html}
        <ul class="lumae-help">{pending_html}</ul>
        {next_retry}
        <details>
          <summary>Recent meaningful background actions</summary>
          <ul>{journal_html}</ul>
        </details>
      </section>
    """


def render_settings_status_panels(batch_size):
    readiness_html = render_v3_readiness_panel()
    relationships_html = render_relationship_status_panel()
    catalogue_html, waveform_html = render_source_preparation_sections(batch_size)
    return {
        "readiness": readiness_html,
        "relationships": relationships_html,
        "catalogue": catalogue_html,
        "waveform": waveform_html,
        "reconcile": render_reconcile_status_panel(),
        "identity": render_provider_identity_panel(),
    }


@bp.get("/settings/status")
def settings_status():
    return _private_json({"panels": render_settings_status_panels(configured_backfill_limit())})


def render_settings(message=None, error=None):
    batch_size = configured_backfill_limit()
    paused = maintenance_paused()
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
    panels = {
        name: (
            f'<div data-lumae-status-panel="{name}"{" hidden" if not html.strip() else ""}>'
            f'{html}</div>'
        )
        for name, html in render_settings_status_panels(batch_size).items()
    }
    maintenance_html = f"""
      <section class="lumae-panel" aria-label="Background maintenance control">
        <span class="lumae-section-priority">Safety control</span>
        <h3>Background maintenance is {'paused' if paused else 'enabled'}</h3>
        <p class="lumae-action-copy">Pausing prevents new catalogue, projection, waveform,
          and relationship work from starting. Already published app data remains available.</p>
        <form class="lumae-form" method="post">
          <button class="lumae-button-secondary" type="submit" name="action"
            value="{'resume_maintenance' if paused else 'pause_maintenance'}">
            {'Resume background maintenance' if paused else 'Pause background maintenance'}
          </button>
        </form>
      </section>
    """
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
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 12px;
            box-sizing: border-box;
            color: var(--lumae-ink);
            display: grid;
            gap: 18px;
            max-width: 920px;
            padding: 20px;
            width: 100%;
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
            color: var(--lumae-ink);
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

          .lumae-source-card {{
            background: var(--lumae-soft);
            border: 1px solid var(--lumae-line);
            border-radius: 10px;
            display: grid;
            gap: 14px;
            padding: 16px;
          }}

          .lumae-source-header {{
            align-items: flex-start;
            display: flex;
            gap: 12px;
            justify-content: space-between;
          }}

          .lumae-source-header h4 {{
            color: var(--lumae-ink);
            font-size: 1.15rem;
            margin: 3px 0 0;
          }}

          .lumae-source-state {{
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 999px;
            color: var(--lumae-ink);
            font-size: 0.76rem;
            font-weight: 800;
            padding: 5px 9px;
            white-space: nowrap;
          }}

          .lumae-source-state-ready {{
            background: #e9f6ef;
            border-color: #a7d8bd;
            color: #14543c;
          }}

          .lumae-source-state-danger {{
            background: #fff0ed;
            border-color: #ffb4a8;
            color: var(--lumae-danger);
          }}

          .lumae-source-state-working {{
            background: #fff8eb;
            border-color: #f2c879;
            color: #6f4200;
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
            color: var(--lumae-ink);
            font-size: 1.15rem;
            line-height: 1.2;
            overflow-wrap: anywhere;
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
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 10px;
            display: grid;
            gap: 14px;
            padding: 18px;
          }}

          .lumae-panel h3 {{
            color: var(--lumae-ink);
            font-size: 1.15rem;
            margin: 0;
          }}

          .lumae-panel details {{
            border-top: 1px solid var(--lumae-line);
            padding-top: 10px;
          }}

          .lumae-panel summary {{
            color: var(--lumae-ink);
            cursor: pointer;
            font-weight: 700;
          }}

          .lumae-technical-details {{
            display: grid;
            gap: 8px;
            padding-top: 10px;
          }}

          .lumae-section-priority {{
            color: var(--lumae-accent);
            font-size: 0.72rem;
            font-weight: 800;
            text-transform: uppercase;
          }}

          .lumae-section-optional {{
            color: var(--lumae-ready);
          }}

          .lumae-section-advanced {{
            color: var(--lumae-warn);
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
            color: var(--lumae-ink);
            font-weight: 700;
          }}

          .lumae-field input {{
            border: 1px solid var(--lumae-line);
            border-radius: 8px;
            color: var(--lumae-ink);
            font: inherit;
            padding: 9px 10px;
          }}

          .lumae-toggle {{
            align-items: center;
            display: flex;
            gap: 10px;
            font-weight: 700;
          }}

          .lumae-toggle span {{
            color: var(--lumae-ink);
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
            display: grid;
            gap: 4px;
            padding: 12px 14px;
          }}

          .lumae-notice strong {{
            color: inherit;
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

          .lumae-notice-warning {{
            background: #fff8eb;
            border: 1px solid #f2c879;
            color: #6f4200;
          }}

          @media (max-width: 620px) {{
            .lumae-analysis-settings {{
              padding: 14px;
            }}

            .lumae-panel,
            .lumae-source-card {{
              padding: 14px;
            }}

            .lumae-source-header {{
              align-items: flex-start;
              flex-direction: column;
            }}

            .lumae-source-state {{
              white-space: normal;
            }}

            .lumae-status-grid {{
              grid-template-columns: 1fr;
            }}

            .lumae-field {{
              max-width: none;
            }}

            .lumae-actions,
            .lumae-actions button,
            .lumae-actions .lumae-button {{
              width: 100%;
            }}
          }}
        </style>

        <section class="lumae-analysis-settings" aria-label="Lumae analysis settings"
          data-status-url="{escape(url_for('lumae_analysis.settings_status'))}">
          {message_html}
          {error_html}

          <header class="lumae-hero">
            <span class="lumae-kicker">Lumae status</span>
            <h2>Four clear stages from library to Lumae recommendations.</h2>
            <p>Library readiness controls app sync. AudioMuse supplies raw source analysis;
              volume and ramp profiles improve playback; Lumae then prepares similar albums and
              artists with its own algorithm. Ready never means a completed but empty library.</p>
            <p class="lumae-help" data-lumae-refresh-notice role="status">Status updates
              automatically without reloading this page.</p>
            <div class="lumae-actions">
              <a class="lumae-button lumae-button-secondary" href="database-state">
                View database state
              </a>
            </div>
          </header>

          {maintenance_html}
          {panels['reconcile']}
          {panels['catalogue']}
          {panels['identity']}
          {panels['readiness']}
          {panels['waveform']}
          {panels['relationships']}
          {render_collections_settings_panel()}
        </section>
        {SETTINGS_STATUS_SCRIPT}
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
            if action in ("pause_maintenance", "resume_maintenance"):
                paused = action == "pause_maintenance"
                set_setting("maintenance_paused", paused)
                _safe_reconcile_schedule(get_db(), paused=paused)
                message = (
                    "Background maintenance paused. Published Lumae data remains available."
                    if paused
                    else "Background maintenance resumed."
                )
            elif action == "save_collections":
                enabled = request.form.get("collection_manager_enabled") == "on"
                set_setting("collection_manager_enabled", enabled)
                sync_collections_menu(enabled)
                message = f"Living Collections {'enabled' if enabled else 'disabled'}."
            elif action in ("ack_v3_readiness", "clear_v3_readiness"):
                message = (
                    "Manual AudioMuse verification is no longer required. "
                    "Lumae now verifies sonic readiness automatically from current evidence."
                )
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
                            enqueue_bounded(
                                prepare_lumae_task,
                                server_id,
                                catalog_instance_id,
                                queue="default",
                                timeout=CATALOG_JOB_TIMEOUT_SECONDS,
                            )
                        except Exception:
                            logger.exception(
                                "lumae_analysis could not queue catalogue preparation; "
                                "the durable watchdog will retry it"
                            )
                        message = (
                            f"Preparing {source['name']}: library refresh and app sync index first, "
                            "then fair background volume and ramp work. Lumae can sync as soon as "
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


@bp.route("/database-state", methods=["GET"])
def database_state_page():
    compatibility = detect_core()
    db = get_db()
    try:
        sources = resolve_catalog_source(db) if db is not None else []
        readiness_by_source = {}
        if db is not None and compatibility.adapter == "v3_registry":
            policy = dedup_policy()
            readiness_by_source = {
                source["catalog_instance_id"]: v3_release_readiness(
                    db, compatibility, source, policy
                )
                for source in sources
            }
        snapshot = collect_database_state(
            db,
            compatibility,
            sources,
            readiness_by_source=readiness_by_source,
        )
    except Exception as exc:
        logger.exception("lumae_analysis could not collect database state")
        snapshot = {
            "captured_at": utc_now_iso(),
            "status": "unavailable",
            "core": compatibility.as_dict(),
            "sources": [],
            "errors": [
                {
                    "section": "database snapshot",
                    "message": str(exc)[:500],
                }
            ],
        }
    return render_page(
        render_database_state(snapshot),
        title="Lumae Database State",
    )


def register(ctx):
    ctx.add_blueprint(bp)
    ctx.set_settings_page("lumae_analysis.settings")
    if collections_enabled():
        ctx.add_menu_item(COLLECTIONS_MENU_LABEL, COLLECTIONS_MENU_ENDPOINT)
    ctx.on_install(migrate)
    ctx.on_flask_start(observe_provider_identities_on_start)
    ctx.on_song_analyzed(analyze_song_hook)
    ctx.add_task("prepare", prepare_lumae_task, queue="default")
    ctx.add_task("profile_backfill", profile_backfill_task, queue="default")
    ctx.add_task("analysis_projection", analysis_projection_task, queue="default")
    ctx.add_task(
        "relationship_preparation", relationship_preparation_task, queue="default"
    )
    ctx.add_cron_task("catalog_reconcile", catalog_reconcile_task, queue="default")
    ctx.add_task("provider_identity_recheck", provider_identity_recheck_task, queue="default")
    ctx.add_cron_task("catalog_refresh", catalog_refresh_task, queue="default")
    ctx.add_cron_task(
        "provider_identity_recheck", provider_identity_recheck_task, queue="default"
    )
    ctx.add_cron_task("analysis_projection", analysis_projection_task, queue="default")
