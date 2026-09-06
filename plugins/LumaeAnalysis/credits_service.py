"""Public host jobs and authenticated API for catalog-wide MusicBrainz credits."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from flask import g, jsonify, request
from plugin.api import get_db, get_setting, table

from . import credits_store as store
from .credits_matching import extract_credits, MATCHING_VERSION
from .credits_qualification import evaluate_audit
from .credits_musicbrainz import Client, MusicBrainzDeferred, resolve
from .catalog import resolve_catalog_source, CatalogScanError
from .reconcile import arm_reconcile


def capability():
    report = get_setting("credits_match_audit", None)
    if isinstance(report, str):
        try:
            report = json.loads(report)
        except (ValueError, TypeError):
            report = None
    qualification = evaluate_audit(report)
    return {"schema_version": 1, "matching_version": MATCHING_VERSION,
            "provider": "musicbrainz", "supported": True,
            "automatic_display_qualified": qualification["qualified"],
            "qualification": qualification}


def paused():
    value = get_setting("maintenance_paused", False)
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


def playback_pending(db):
    cur = db.cursor()
    # Public plugin-owned job tables: no host queue internals or personal data.
    cur.execute(f"SELECT EXISTS(SELECT 1 FROM {table('profiles')} WHERE status='pending_interactive')")
    result = bool(cur.fetchone()[0])
    cur.close()
    db.commit()
    if result:
        return True
    from .dj_analysis_store import interactive_dj_jobs_pending
    from .dj_analysis_v3_store import priority_dj_v3_jobs_pending
    return bool(interactive_dj_jobs_pending(db) or priority_dj_v3_jobs_pending(db))


def run_one(catalog_id, *, db=None, client_factory=Client, critical=playback_pending):
    db = db or get_db()
    store.source(db, catalog_id)
    if paused() or critical(db):
        return {"status": "deferred", "reason": "maintenance_or_playback_priority"}
    job = store.claim(db, catalog_id)
    if not job:
        return {"status": "current"}
    try:
        album = store.load_album(db, catalog_id, job["album_id"])
        if not album or album["input_fp"] != job["input_fp"]:
            store.request_album(db, catalog_id, job["album_id"])
            return {"status": "superseded"}
        allowed = lambda: not paused() and store.lease_current(db, job) and not critical(db)
        match = resolve(album, client_factory(db, allowed=allowed))
        credits = extract_credits(match)
        payload = {"schema_version": 1, "catalog_instance_id": catalog_id, "album_id": album["id"],
            "input_fingerprint": album["input_fp"], "match_status": match["status"],
            "matching": match["evidence"],
            "release_mbid": match.get("release_id"), "release_group_mbid": match.get("release_group_id"),
            "computed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "subjects": [{"subject_type": "album", "subject_id": album["id"], "credits": credits["album"]}] +
                [{"subject_type": "track", "subject_id": track["id"], "credits": credits["tracks"].get(track["id"], [])}
                 for track in album["tracks"]]}
        if not allowed():
            raise MusicBrainzDeferred("Credits work paused or superseded", 60)
        applied = store.finish(db, job, payload)
        store.compact(db, catalog_id)
        return {"status": "published" if applied else "superseded", "album_id": album["id"],
                "match_status": match["status"], "credits": sum(len(subject["credits"]) for subject in payload["subjects"])}
    except Exception as exc:
        db.rollback()
        delay = exc.retry_seconds if isinstance(exc, MusicBrainzDeferred) else min(21600, 60 * 2 ** min(job["attempt"], 8))
        store.finish(db, job, error=exc, retry_seconds=delay)
        return {"status": "deferred" if isinstance(exc, MusicBrainzDeferred) else "failed",
                "album_id": job["album_id"], "retry_seconds": delay}


def reconcile(db, server_id=None, before_background=False):
    """One credits album per turn, with time sharing alongside existing background work."""
    if paused():
        return None
    sources = resolve_catalog_source(db, server_id=server_id)
    for source in sources:
        catalog_id = source["catalog_instance_id"]
        if source.get("rebind_status", "active") != "active":
            continue
        store.sweep(db, catalog_id)
        cur = db.cursor()
        cur.execute(f"""SELECT EXISTS(SELECT 1 FROM {table('credits_jobs')} j WHERE j.catalog_instance_id=s.catalog_instance_id
            AND j.not_before<=now() AND (j.status IN('pending','failed') OR (j.status='running' AND j.lease_until<now()))),
            EXTRACT(EPOCH FROM(now()-COALESCE(s.last_work_at,now()-interval '1 day'))),
            EXISTS(SELECT 1 FROM {table('credits_jobs')} j WHERE j.catalog_instance_id=s.catalog_instance_id
                   AND j.priority>0 AND j.status IN('pending','failed'))
            FROM {table('credits_stream')} s WHERE s.catalog_instance_id=%s""", (catalog_id,))
        row = cur.fetchone()
        cur.close()
        db.commit()
        if row and row[0] and (not before_background or float(row[1]) >= (120 if row[2] else 300)):
            return run_one(catalog_id, db=db)
    return None


def register_routes(bp):
    def private(payload, code=200):
        result = jsonify(payload)
        result.status_code = code
        result.headers["Cache-Control"] = "private, no-store"
        return result

    def catalog_id():
        if not (getattr(g, "auth_method", None) in ("bearer", "session") or getattr(g, "auth_user", None)):
            raise PermissionError("Authentication required")
        body = request.get_json(silent=True) if request.method == "POST" else None
        if request.method == "POST" and not isinstance(body, dict):
            raise ValueError("Credits requests must be objects")
        value = body.get("catalog_instance_id") if body is not None else request.args.get("catalog_instance_id")
        if not isinstance(value, str) or not value or len(value) > 200:
            raise ValueError("catalog_instance_id is required")
        try:
            store.source(get_db(), value)
        except KeyError as exc:
            raise ValueError('Unknown credits catalog') from exc
        return value

    def invoke(work):
        try:
            if request.method == 'POST':
                request.max_content_length = 16000
            if len(request.args.get('page_token', '')) > 4096 or len(request.args.get('cursor', '')) > 4096:
                raise ValueError('Credits cursor too large')
            return private(work(catalog_id()))
        except PermissionError:
            return private({"error": "authentication_required"}, 401)
        except KeyError:
            return private({"error": "bootstrap_required"}, 410)
        except (CatalogScanError, ValueError, TypeError, json.JSONDecodeError):
            return private({"error": "invalid_credits_request"}, 400)

    @bp.post("/api/credits/prepare")
    def credits_prepare_api():
        def prepare(catalog):
            if request.content_length and request.content_length > 16000:
                raise ValueError("Credits request too large")
            body = request.get_json(silent=True) or {}
            ids = body.get("album_ids", [])
            if not isinstance(ids, list) or len(ids) > 20 or any(not isinstance(value, str) or not value or len(value) > 500 for value in ids):
                raise ValueError("At most twenty album IDs are accepted")
            if paused():
                return {"catalog_instance_id": catalog, "status": "paused", "requested": 0}
            count = sum(store.request_album(get_db(), catalog, value, priority=10) for value in dict.fromkeys(ids))
            if not ids:
                count = store.sweep(get_db(), catalog)
            arm_reconcile(get_db(), "credits_requested", commit=True)
            return {"catalog_instance_id": catalog, "status": "queued", "requested": count, "schema_version": 1}
        return invoke(prepare)

    @bp.get("/api/credits/status")
    def credits_status_api():
        return invoke(lambda catalog: {**store.status(get_db(), catalog), "capability": capability(), "paused": paused()})

    @bp.get("/api/credits/bootstrap")
    def credits_bootstrap_api():
        return invoke(lambda catalog: store.bootstrap(get_db(), catalog,
            page_token=request.args.get("page_token"), limit=request.args.get("limit", 100)))

    @bp.get("/api/credits/changes")
    def credits_changes_api():
        return invoke(lambda catalog: store.changes(get_db(), catalog,
            request.args.get("cursor") or "", limit=request.args.get("limit", 250)))
