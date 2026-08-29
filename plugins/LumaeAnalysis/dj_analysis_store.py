"""Durable, source-scoped DJ analysis jobs and publications."""

import uuid

from plugin.api import table

from .dj_analysis import METHOD, SCHEMA_VERSION, analysis_digest, canonical_json
from .edge_profiles import opaque_revision


def migrate_dj_analysis(db):
    cur = db.cursor()
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table('dj_analyses')} (
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
        f"""CREATE TABLE IF NOT EXISTS {table('dj_analysis_jobs')} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            media_revision TEXT NOT NULL,
            job_token TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
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
    # PostgreSQL enforces the one-worker contract even if two RQ workers race.
    cur.execute(
        f"""CREATE UNIQUE INDEX IF NOT EXISTS {table('dj_analysis_one_running_idx')}
        ON {table('dj_analysis_jobs')} ((true)) WHERE status='running'"""
    )
    cur.execute(
        f"""CREATE INDEX IF NOT EXISTS {table('dj_analysis_jobs_queue_idx')}
        ON {table('dj_analysis_jobs')} (status, priority DESC, requested_at, track_id)"""
    )
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs')} SET status='pending', started_at=NULL,
            error_code='worker_restarted', updated_at=now()
        WHERE status='running' AND updated_at < now()-interval '20 minutes'"""
    )
    cur.close()


def _current_sources(cur, catalog_id, ids):
    cur.execute(
        f"""SELECT track_id, media_signature FROM {table('source_profiles')}
        WHERE catalog_instance_id=%s AND track_id=ANY(%s)
          AND status='ready' AND media_signature IS NOT NULL ORDER BY track_id""",
        (catalog_id, list(dict.fromkeys(ids))[:100]),
    )
    return [(track_id, signature) for track_id, signature in cur.fetchall()]


def claim_dj_requests(db, catalog_id, ids, *, priority=0):
    cur = db.cursor()
    accepted = []
    ready = []
    for track_id, signature in _current_sources(cur, catalog_id, ids):
        revision = opaque_revision(signature)
        if not revision:
            continue
        cur.execute(
            f"""SELECT 1 FROM {table('dj_analyses')}
            WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
              AND media_signature=%s AND payload->>'method'=%s LIMIT 1""",
            (catalog_id, track_id, revision, signature, METHOD),
        )
        if cur.fetchone():
            ready.append(track_id)
            continue
        token = str(uuid.uuid4())
        cur.execute(
            f"""INSERT INTO {table('dj_analysis_jobs')} AS job
                (catalog_instance_id, track_id, media_revision, job_token, priority, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                media_revision=EXCLUDED.media_revision, job_token=EXCLUDED.job_token,
                priority=GREATEST(job.priority, EXCLUDED.priority), status='pending',
                progress_frames=0, error_code=NULL, requested_at=now(), started_at=NULL,
                completed_at=NULL, updated_at=now()
            WHERE job.media_revision<>EXCLUDED.media_revision
               OR (job.status='pending' AND job.updated_at < now()-interval '20 minutes')
               OR (job.status IN ('unsupported', 'failed', 'cancelled')
                   AND job.updated_at < now()-interval '6 hours')
            RETURNING job_token""",
            (catalog_id, track_id, revision, token, int(priority)),
        )
        if cur.fetchone():
            accepted.append(
                {"track_id": track_id, "media_revision": revision, "job_token": token}
            )
    db.commit()
    cur.close()
    return accepted, ready


def claim_next_dj_job(db):
    cur = db.cursor()
    cur.execute(
        f"""WITH candidate AS (
            SELECT catalog_instance_id, track_id FROM {table('dj_analysis_jobs')}
            WHERE status='pending'
              AND NOT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs')} WHERE status='running')
            ORDER BY priority DESC, requested_at, track_id
            FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE {table('dj_analysis_jobs')} job SET status='running', attempts=attempts+1,
            started_at=now(), completed_at=NULL, updated_at=now()
        FROM candidate WHERE job.catalog_instance_id=candidate.catalog_instance_id
          AND job.track_id=candidate.track_id
        RETURNING job.catalog_instance_id, job.track_id, job.media_revision,
                  job.job_token, job.attempts"""
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
    }


def update_dj_progress(db, job, frames):
    cur = db.cursor()
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs')} SET progress_frames=%s, updated_at=now()
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


def dj_job_cancelled(db, job):
    cur = db.cursor()
    cur.execute(
        f"""SELECT status FROM {table('dj_analysis_jobs')}
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


def finish_dj_job(db, job, status, error_code=None):
    if status not in ("unsupported", "failed", "cancelled"):
        raise ValueError("invalid DJ terminal status")
    cur = db.cursor()
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs')} SET status=%s, error_code=%s,
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


def cancel_dj_jobs(db, catalog_id, ids):
    cur = db.cursor()
    cur.execute(
        f"""UPDATE {table('dj_analysis_jobs')} SET status='cancelled',
            error_code='cancelled', completed_at=now(), updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=ANY(%s)
          AND status IN ('pending', 'running') RETURNING track_id""",
        (catalog_id, list(dict.fromkeys(ids))[:100]),
    )
    cancelled = [row[0] for row in cur.fetchall()]
    db.commit()
    cur.close()
    return cancelled


def publish_dj_analysis(db, job, payload, media_signature):
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("method") != METHOD
        or payload.get("catalog_instance_id") != job["catalog_instance_id"]
        or payload.get("track_id") != job["track_id"]
        or payload.get("media_revision") != job["media_revision"]
        or analysis_digest(payload) != payload.get("analysis_digest")
        or opaque_revision(media_signature) != job["media_revision"]
    ):
        raise ValueError("DJ publication identity/digest mismatch")
    cur = db.cursor()
    cur.execute(
        f"""SELECT media_signature FROM {table('source_profiles')}
        WHERE catalog_instance_id=%s AND track_id=%s AND status='ready' FOR UPDATE""",
        (job["catalog_instance_id"], job["track_id"]),
    )
    source = cur.fetchone()
    cur.execute(
        f"""SELECT job_token FROM {table('dj_analysis_jobs')}
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
        f"DELETE FROM {table('dj_analyses')} WHERE catalog_instance_id=%s AND track_id=%s",
        (job["catalog_instance_id"], job["track_id"]),
    )
    cur.execute(
        f"""INSERT INTO {table('dj_analyses')}
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
        f"""UPDATE {table('dj_analysis_jobs')} SET status='ready', error_code=NULL,
            completed_at=now(), updated_at=now()
        WHERE catalog_instance_id=%s AND track_id=%s AND job_token=%s""",
        (job["catalog_instance_id"], job["track_id"], job["job_token"]),
    )
    db.commit()
    cur.close()
    return True


def read_dj_analysis(db, catalog_id, ids):
    cur = db.cursor()
    cur.execute(
        f"""SELECT p.track_id, analysis.payload, job.status, job.error_code,
                   job.progress_frames
        FROM {table('source_profiles')} p
        LEFT JOIN LATERAL (
            SELECT payload FROM {table('dj_analyses')} value
            WHERE value.catalog_instance_id=p.catalog_instance_id
              AND value.track_id=p.track_id AND value.media_signature=p.media_signature
            ORDER BY value.updated_at DESC LIMIT 1
        ) analysis ON TRUE
        LEFT JOIN {table('dj_analysis_jobs')} job
          ON job.catalog_instance_id=p.catalog_instance_id AND job.track_id=p.track_id
        WHERE p.catalog_instance_id=%s AND p.track_id=ANY(%s) ORDER BY p.track_id""",
        (catalog_id, list(dict.fromkeys(ids))[:100]),
    )
    rows = cur.fetchall()
    cur.close()
    result = {"ready": [], "pending": [], "unsupported": [], "failed": [], "missing": []}
    found = set()
    for track_id, payload, status, error_code, progress_frames in rows:
        found.add(track_id)
        if payload:
            result["ready"].append(payload)
        elif status in ("pending", "running"):
            result["pending"].append(
                {"track_id": track_id, "status": status, "progress_frames": int(progress_frames or 0)}
            )
        elif status == "unsupported":
            result["unsupported"].append({"track_id": track_id, "reason": error_code})
        elif status in ("failed", "cancelled"):
            result["failed"].append({"track_id": track_id, "reason": error_code})
        else:
            result["missing"].append(track_id)
    result["missing"].extend(track_id for track_id in ids if track_id not in found)
    return result


def dj_backfill_candidates(db, catalog_id, after="", limit=100):
    """Return source-ready tracks without a current analysis or fresh job."""
    cur = db.cursor()
    cur.execute(
        f"""SELECT p.track_id FROM {table('source_profiles')} p
        LEFT JOIN LATERAL (
            SELECT 1 AS present FROM {table('dj_analyses')} analysis
            WHERE analysis.catalog_instance_id=p.catalog_instance_id
              AND analysis.track_id=p.track_id
              AND analysis.media_signature=p.media_signature
              AND analysis.payload->>'method'=%s LIMIT 1
        ) analysis ON TRUE
        LEFT JOIN {table('dj_analysis_jobs')} job
          ON job.catalog_instance_id=p.catalog_instance_id AND job.track_id=p.track_id
        WHERE p.catalog_instance_id=%s AND p.status='ready'
          AND p.media_signature IS NOT NULL AND p.track_id>%s
          AND analysis.present IS NULL
          AND (job.track_id IS NULL
               OR (job.status='ready' AND job.updated_at < now()-interval '1 minute')
               OR (job.status IN ('unsupported', 'failed', 'cancelled')
                   AND job.updated_at < now()-interval '6 hours'))
        ORDER BY p.track_id LIMIT %s""",
        (METHOD, catalog_id, str(after), max(1, min(100, int(limit)))),
    )
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return ids


def dj_jobs_pending(db):
    cur = db.cursor()
    cur.execute(
        f"SELECT EXISTS (SELECT 1 FROM {table('dj_analysis_jobs')} "
        "WHERE status='pending')"
    )
    row = cur.fetchone()
    cur.close()
    return bool(row and row[0])
