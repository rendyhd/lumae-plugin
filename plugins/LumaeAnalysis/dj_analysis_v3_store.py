"""Additive persistence and priority scheduling for DJ Analysis V3."""

import json
import uuid

from plugin.api import table

from .dj_analysis import analysis_digest, canonical_json
from .dj_analysis_v3 import METHOD, SCHEMA_VERSION
from .edge_profiles import opaque_revision


DJ_CLAIM_LOCK_ID = 0x4C554D4145444A33
PRIORITY_TIERS = {"queue": 10, "lookahead": 50, "boundary": 100}


def migrate_dj_analysis_v3(db):
    cur = db.cursor()
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table('dj_analyses_v3')} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            media_revision TEXT NOT NULL,
            representation_id TEXT NOT NULL,
            media_signature TEXT NOT NULL,
            analysis_digest TEXT NOT NULL,
            payload JSONB NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, track_id, media_revision, representation_id)
        )"""
    )
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table('dj_analysis_jobs_v3')} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            media_revision TEXT NOT NULL,
            job_token TEXT NOT NULL,
            request_tier TEXT NOT NULL CHECK (request_tier IN ('boundary', 'lookahead', 'queue')),
            priority INTEGER NOT NULL CHECK (priority IN (10, 50, 100)),
            status TEXT NOT NULL,
            progress_frames BIGINT NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            error_code TEXT,
            requested_at TIMESTAMP NOT NULL DEFAULT now(),
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, track_id)
        )"""
    )
    cur.execute(
        f"""CREATE UNIQUE INDEX IF NOT EXISTS {table('dj_analysis_v3_one_running_idx')}
        ON {table('dj_analysis_jobs_v3')} ((true)) WHERE status='running'"""
    )
    cur.execute(
        f"""CREATE INDEX IF NOT EXISTS {table('dj_analysis_v3_queue_idx')}
        ON {table('dj_analysis_jobs_v3')} (status, priority DESC, requested_at, track_id)"""
    )
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs_v3')} SET status='pending', started_at=NULL,
            error_code='worker_restarted', updated_at=now()
        WHERE status='running' AND updated_at < now()-interval '20 minutes'"""
    )
    cur.close()


def _current_sources(cur, catalog_id, ids):
    unique = list(dict.fromkeys(ids))[:100]
    cur.execute(
        f"""SELECT track_id, media_signature FROM {table('source_profiles')}
        WHERE catalog_instance_id=%s AND track_id=ANY(%s)
          AND status='ready' AND media_signature IS NOT NULL ORDER BY track_id""",
        (catalog_id, unique),
    )
    return [(track_id, signature) for track_id, signature in cur.fetchall()]


def claim_dj_v3_requests(
    db,
    catalog_id,
    ids,
    *,
    priority_tier,
    expected_vocal_calibration_cache_key=None,
):
    if priority_tier not in PRIORITY_TIERS:
        raise ValueError("invalid DJ V3 priority tier")
    priority = PRIORITY_TIERS[priority_tier]
    cur = db.cursor()
    accepted = []
    promoted = []
    ready = []
    queued = []
    for track_id, signature in _current_sources(cur, catalog_id, ids):
        revision = opaque_revision(signature)
        if not revision:
            continue
        calibration_clause = (
            "AND payload#>>'{vocal_risk,calibration,cache_key}'=%s"
            if expected_vocal_calibration_cache_key is not None
            else ""
        )
        parameters = [
            catalog_id,
            track_id,
            revision,
            signature,
            METHOD,
            str(SCHEMA_VERSION),
        ]
        if expected_vocal_calibration_cache_key is not None:
            parameters.append(str(expected_vocal_calibration_cache_key))
        cur.execute(
            f"""SELECT 1 FROM {table('dj_analyses_v3')}
            WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
              AND media_signature=%s AND payload->>'method'=%s
              AND payload->>'schema_version'=%s {calibration_clause} LIMIT 1""",
            tuple(parameters),
        )
        if cur.fetchone():
            ready.append(track_id)
            continue
        cur.execute(
            f"""SELECT media_revision, status, priority FROM {table('dj_analysis_jobs_v3')}
            WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
            (catalog_id, track_id),
        )
        existing = cur.fetchone()
        if existing and existing[0] == revision and existing[1] in ("pending", "running"):
            if existing[1] == "pending" and priority > int(existing[2]):
                cur.execute(
                    f"""UPDATE {table('dj_analysis_jobs_v3')}
                    SET priority=%s, request_tier=%s, updated_at=now()
                    WHERE catalog_instance_id=%s AND track_id=%s AND status='pending'""",
                    (priority, priority_tier, catalog_id, track_id),
                )
                promoted.append(track_id)
            else:
                queued.append(track_id)
            continue
        token = str(uuid.uuid4())
        cur.execute(
            f"""INSERT INTO {table('dj_analysis_jobs_v3')} AS job
                (catalog_instance_id, track_id, media_revision, job_token,
                 request_tier, priority, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'pending')
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                media_revision=EXCLUDED.media_revision,
                job_token=EXCLUDED.job_token,
                request_tier=EXCLUDED.request_tier,
                priority=EXCLUDED.priority,
                status='pending', progress_frames=0, attempts=0, error_code=NULL,
                requested_at=now(), started_at=NULL, completed_at=NULL, updated_at=now()
            RETURNING job_token""",
            (catalog_id, track_id, revision, token, priority_tier, priority),
        )
        if cur.fetchone():
            accepted.append(
                {"track_id": track_id, "media_revision": revision, "job_token": token}
            )
    db.commit()
    cur.close()
    return accepted, promoted, ready, queued


def claim_next_dj_job_any(db):
    """Claim one V2/V3 job by priority and request time under one DB election."""
    cur = db.cursor()
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (DJ_CLAIM_LOCK_ID,))
    # The dedicated lumae-dj queue is globally serialized. A running row seen
    # when a new queue task starts belongs to a worker process that exited
    # before it could publish or fail the job. Reclaim it immediately so a
    # supervisor recycle cannot strand every later request behind a 20-minute
    # migration timeout.
    for jobs_table in (table("dj_analysis_jobs_v3"), table("dj_analysis_jobs")):
        cur.execute(
            f"""UPDATE {jobs_table} SET status='pending', started_at=NULL,
                completed_at=NULL, error_code='worker_restarted', updated_at=now()
            WHERE status='running'"""
        )
    cur.execute(
        f"""SELECT analysis_version, catalog_instance_id, track_id
        FROM (
            SELECT 3 AS analysis_version, catalog_instance_id, track_id,
                   priority, requested_at
              FROM {table('dj_analysis_jobs_v3')} WHERE status='pending'
            UNION ALL
            SELECT 2 AS analysis_version, catalog_instance_id, track_id,
                   priority, requested_at
              FROM {table('dj_analysis_jobs')} WHERE status='pending'
        ) candidate
        WHERE NOT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs_v3')} WHERE status='running')
          AND NOT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs')} WHERE status='running')
        ORDER BY priority DESC, requested_at, track_id, analysis_version DESC LIMIT 1"""
    )
    selected = cur.fetchone()
    if not selected:
        db.commit()
        cur.close()
        return None
    version, catalog_id, track_id = selected
    jobs_table = table("dj_analysis_jobs_v3" if version == 3 else "dj_analysis_jobs")
    returning = ", request_tier, priority" if version == 3 else ", NULL, priority"
    cur.execute(
        f"""UPDATE {jobs_table} SET status='running', attempts=attempts+1,
            started_at=now(), completed_at=NULL, updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND status='pending'
        RETURNING catalog_instance_id, track_id, media_revision, job_token,
                  attempts{returning}""",
        (catalog_id, track_id),
    )
    row = cur.fetchone()
    db.commit()
    cur.close()
    if not row:
        return None
    return {
        "catalog_instance_id": row[0],
        "track_id": row[1],
        "media_revision": row[2],
        "job_token": row[3],
        "attempts": row[4],
        "request_tier": row[5],
        "priority": row[6],
        "analysis_version": int(version),
    }

def claim_next_dj_v3_job(db):
    cur = db.cursor()
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (DJ_CLAIM_LOCK_ID,))
    cur.execute(
        f"""WITH candidate AS (
            SELECT catalog_instance_id, track_id FROM {table('dj_analysis_jobs_v3')}
            WHERE status='pending'
              AND NOT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs_v3')} WHERE status='running')
              AND NOT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs')} WHERE status='running')
            ORDER BY priority DESC, requested_at, track_id
            FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE {table('dj_analysis_jobs_v3')} job
        SET status='running', attempts=attempts+1, started_at=now(),
            completed_at=NULL, updated_at=now()
        FROM candidate WHERE job.catalog_instance_id=candidate.catalog_instance_id
          AND job.track_id=candidate.track_id
        RETURNING job.catalog_instance_id, job.track_id, job.media_revision,
                  job.job_token, job.attempts, job.request_tier, job.priority"""
    )
    row = cur.fetchone()
    db.commit()
    cur.close()
    if not row:
        return None
    return {
        "catalog_instance_id": row[0],
        "track_id": row[1],
        "media_revision": row[2],
        "job_token": row[3],
        "attempts": row[4],
        "request_tier": row[5],
        "priority": row[6],
        "analysis_version": 3,
    }


def update_dj_v3_progress(db, job, frames):
    cur = db.cursor()
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs_v3')} SET progress_frames=%s, updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
          AND job_token=%s AND status='running'""",
        (
            max(0, int(frames)),
            job["catalog_instance_id"],
            job["track_id"],
            job["media_revision"],
            job["job_token"],
        ),
    )
    db.commit()
    cur.close()


def dj_v3_job_cancelled(db, job):
    cur = db.cursor()
    cur.execute(
        f"""SELECT status FROM {table('dj_analysis_jobs_v3')}
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
          AND job_token=%s""",
        (
            job["catalog_instance_id"],
            job["track_id"],
            job["media_revision"],
            job["job_token"],
        ),
    )
    row = cur.fetchone()
    cur.close()
    return not row or row[0] == "cancelled"


def finish_dj_v3_job(db, job, status, error_code=None):
    if status not in ("unsupported", "failed", "cancelled"):
        raise ValueError("invalid DJ V3 terminal status")
    cur = db.cursor()
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs_v3')} SET status=%s, error_code=%s,
            completed_at=now(), updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
          AND job_token=%s AND status IN ('pending', 'running') RETURNING job_token""",
        (
            status,
            str(error_code or status)[:80],
            job["catalog_instance_id"],
            job["track_id"],
            job["media_revision"],
            job["job_token"],
        ),
    )
    applied = cur.fetchone() is not None
    db.commit()
    cur.close()
    return applied


def publish_dj_v3_analysis(db, job, payload, media_signature):
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("method") != METHOD
        or payload.get("catalog_instance_id") != job["catalog_instance_id"]
        or payload.get("track_id") != job["track_id"]
        or payload.get("media_revision") != job["media_revision"]
        or analysis_digest(payload) != payload.get("analysis_digest")
        or opaque_revision(media_signature) != job["media_revision"]
    ):
        raise ValueError("DJ V3 publication identity/digest mismatch")
    cur = db.cursor()
    cur.execute(
        f"""SELECT media_signature FROM {table('source_profiles')}
        WHERE catalog_instance_id=%s AND track_id=%s AND status='ready' FOR UPDATE""",
        (job["catalog_instance_id"], job["track_id"]),
    )
    source = cur.fetchone()
    cur.execute(
        f"""SELECT job_token FROM {table('dj_analysis_jobs_v3')}
        WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
          AND job_token=%s AND status='running' FOR UPDATE""",
        (
            job["catalog_instance_id"],
            job["track_id"],
            job["media_revision"],
            job["job_token"],
        ),
    )
    current = cur.fetchone()
    if not source or source[0] != media_signature or not current:
        db.rollback()
        cur.close()
        return False
    cur.execute(
        f"DELETE FROM {table('dj_analyses_v3')} WHERE catalog_instance_id=%s AND track_id=%s",
        (job["catalog_instance_id"], job["track_id"]),
    )
    cur.execute(
        f"""INSERT INTO {table('dj_analyses_v3')}
            (catalog_instance_id, track_id, media_revision, representation_id,
             media_signature, analysis_digest, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)""",
        (
            job["catalog_instance_id"],
            job["track_id"],
            job["media_revision"],
            payload["representation_id"],
            media_signature,
            payload["analysis_digest"],
            canonical_json(payload),
        ),
    )
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs_v3')} SET status='ready', error_code=NULL,
            completed_at=now(), updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND job_token=%s""",
        (job["catalog_instance_id"], job["track_id"], job["job_token"]),
    )
    db.commit()
    cur.close()
    return True


def read_dj_v3_analysis(
    db,
    catalog_id,
    ids,
    *,
    expected_vocal_calibration_cache_key=None,
):
    unique = list(dict.fromkeys(ids))[:100]
    cur = db.cursor()
    calibration_clause = (
        "AND value.payload#>>'{vocal_risk,calibration,cache_key}'=%s"
        if expected_vocal_calibration_cache_key is not None
        else ""
    )
    parameters = [METHOD, str(SCHEMA_VERSION)]
    if expected_vocal_calibration_cache_key is not None:
        parameters.append(str(expected_vocal_calibration_cache_key))
    parameters.extend([catalog_id, unique])
    cur.execute(
        f"""SELECT p.track_id, analysis.payload, job.status, job.error_code,
                   job.progress_frames, job.request_tier, job.priority
        FROM {table('source_profiles')} p
        LEFT JOIN LATERAL (
            SELECT payload FROM {table('dj_analyses_v3')} value
            WHERE value.catalog_instance_id=p.catalog_instance_id
              AND value.track_id=p.track_id AND value.media_signature=p.media_signature
              AND value.payload->>'method'=%s
              AND value.payload->>'schema_version'=%s {calibration_clause}
            ORDER BY value.updated_at DESC LIMIT 1
        ) analysis ON TRUE
        LEFT JOIN {table('dj_analysis_jobs_v3')} job
          ON job.catalog_instance_id=p.catalog_instance_id AND job.track_id=p.track_id
        WHERE p.catalog_instance_id=%s AND p.track_id=ANY(%s) ORDER BY p.track_id""",
        tuple(parameters),
    )
    rows = cur.fetchall()
    cur.close()
    result = {"ready": [], "pending": [], "unsupported": [], "failed": [], "missing": []}
    found = set()
    for track_id, payload, status, error_code, progress_frames, request_tier, priority in rows:
        found.add(track_id)
        if payload:
            result["ready"].append(payload)
        elif status in ("pending", "running"):
            result["pending"].append(
                {
                    "track_id": track_id,
                    "status": status,
                    "progress_frames": int(progress_frames or 0),
                    "priority_tier": request_tier,
                    "priority": int(priority or 0),
                }
            )
        elif status == "unsupported":
            result["unsupported"].append({"track_id": track_id, "reason": error_code})
        elif status in ("failed", "cancelled"):
            result["failed"].append({"track_id": track_id, "reason": error_code})
        else:
            result["missing"].append(track_id)
    result["missing"].extend(track_id for track_id in unique if track_id not in found)
    return result


def dj_v3_jobs_pending(db):
    cur = db.cursor()
    cur.execute(
        f"SELECT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs_v3')} WHERE status='pending')"
    )
    row = cur.fetchone()
    cur.close()
    return bool(row and row[0])


def priority_dj_v3_jobs_pending(db):
    cur = db.cursor()
    cur.execute(
        f"SELECT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs_v3')} WHERE status='pending' AND priority>=50)"
    )
    row = cur.fetchone()
    cur.close()
    return bool(row and row[0])
