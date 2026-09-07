"""Version-aware DJ job repository and fenced, process-lifetime ownership.

The two published storage namespaces remain compatible. All lifecycle rules
live here; a PostgreSQL session lock covers the entire heavy analysis, across
versions, queues, hosts, and container replicas. Transactions never hold row
locks while models run.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from plugin.api import table
from .dj_contract import METHOD as V2_METHOD
from .dj_evidence import analysis_digest, canonical_json
from .edge_profiles import opaque_revision

LOCK_CLASS = 0x4C554D41
LOCK_KEY = 0x444A
PRIORITY_TIERS = {"queue": 10, "lookahead": 50, "boundary": 100}
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class AnalysisContract:
    version: int
    method: str
    analyses: str
    jobs: str


def contract(version):
    if version == 2:
        return AnalysisContract(
            2, V2_METHOD, table("dj_analyses"), table("dj_analysis_jobs")
        )
    if version == 3:
        from .dj_analysis_v3 import METHOD

        return AnalysisContract(
            3, METHOD, table("dj_analyses_v3"), table("dj_analysis_jobs_v3")
        )
    raise ValueError("unsupported DJ analysis version")


def producer_key(version, calibration_key=None):
    spec = contract(version)
    return f'{spec.version}:{spec.method}:{calibration_key or "uncalibrated-v1"}'


def migrate(db, version):
    spec = contract(version)
    cur = db.cursor()
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {spec.analyses} (
        catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
        track_id TEXT NOT NULL, media_revision TEXT NOT NULL, representation_id TEXT NOT NULL,
        media_signature TEXT NOT NULL, analysis_digest TEXT NOT NULL, payload JSONB NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (catalog_instance_id, track_id, media_revision, representation_id))"""
    )
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {spec.jobs} (
        catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
        track_id TEXT NOT NULL, media_revision TEXT NOT NULL, job_token TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0, request_tier TEXT NOT NULL DEFAULT 'queue',
        status TEXT NOT NULL, progress_frames BIGINT NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0, error_code TEXT,
        producer_key TEXT, next_retry_at TIMESTAMPTZ, metrics JSONB,
        requested_at TIMESTAMP NOT NULL DEFAULT now(), started_at TIMESTAMP,
        completed_at TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (catalog_instance_id, track_id))"""
    )
    for definition in (
        "request_tier TEXT NOT NULL DEFAULT 'queue'",
        "producer_key TEXT",
        "next_retry_at TIMESTAMPTZ",
        "metrics JSONB",
        "media_signature TEXT",
    ):
        cur.execute(f"ALTER TABLE {spec.jobs} ADD COLUMN IF NOT EXISTS {definition}")
    index = table(
        "dj_analysis_one_running_idx"
        if version == 2
        else "dj_analysis_v3_one_running_idx"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {spec.jobs} ((true)) WHERE status='running'"
    )
    cur.execute(
        f"""CREATE INDEX IF NOT EXISTS {spec.jobs}_queue_idx
        ON {spec.jobs} (status, priority DESC, requested_at, track_id)"""
    )
    # Migration must never infer a dead owner from row age. Only a caller that
    # owns the execution lock may recover an interrupted analysis.
    cur.close()


def _lock_present_sql():
    return """SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()
              AND classid=%s AND objid=%s AND objsubid=2 AND granted"""


def owns_execution(db):
    cur = db.cursor()
    try:
        cur.execute(_lock_present_sql(), (LOCK_CLASS, LOCK_KEY))
        return cur.fetchone() is not None
    finally:
        cur.close()


def acquire_execution(db):
    cur = db.cursor()
    try:
        # Session locks are reentrant. Reject reentry rather than accidentally
        # reclaiming our own live work or leaking a second lock acquisition.
        cur.execute(
            f"""SELECT CASE WHEN EXISTS ({_lock_present_sql()}) THEN FALSE
            ELSE pg_try_advisory_lock(%s, %s) END""",
            (LOCK_CLASS, LOCK_KEY, LOCK_CLASS, LOCK_KEY),
        )
        row = cur.fetchone()
        db.commit()
        return bool(row and row[0])
    finally:
        cur.close()


def release_execution(db):
    try:
        db.rollback()
        cur = db.cursor()
        try:
            cur.execute("SELECT pg_advisory_unlock(%s, %s)", (LOCK_CLASS, LOCK_KEY))
            db.commit()
        finally:
            cur.close()
    except Exception:
        # A dead connection has already released its session lock.
        if not getattr(db, "closed", False):
            raise


def _finish_ownership(db, job):
    if job.get("release_lock_on_finish"):
        release_execution(db)


def request_jobs(
    db,
    version,
    catalog_id,
    ids,
    *,
    priority=0,
    priority_tier="queue",
    calibration_key=None,
    force=False,
):
    spec = contract(version)
    if priority_tier not in PRIORITY_TIERS:
        raise ValueError("invalid DJ priority tier")
    if version == 3:
        priority = PRIORITY_TIERS[priority_tier]
    unique = list(dict.fromkeys(ids))[:100]
    key = producer_key(version, calibration_key)
    result = {
        "accepted": [],
        "promoted": [],
        "ready": [],
        "queued": [],
        "blocked": [],
        "missing": [],
    }
    cur = db.cursor()
    try:
        cur.execute(
            f"""SELECT track_id, media_signature FROM {table('source_profiles')}
            WHERE catalog_instance_id=%s AND track_id=ANY(%s)
              AND status='ready' AND media_signature IS NOT NULL ORDER BY track_id""",
            (catalog_id, unique),
        )
        sources = cur.fetchall()
        found = {track for track, _ in sources}
        result["missing"] = [track for track in unique if track not in found]
        for track_id, signature in sources:
            revision = opaque_revision(signature)
            # Serialize absent-row requests as well as updates. FOR UPDATE alone
            # cannot lock a row that has not been inserted yet.
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
                (f"{version}:{catalog_id}:{track_id}", LOCK_CLASS),
            )
            cur.execute(
                f"""SELECT 1 FROM {spec.analyses}
                WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
                  AND media_signature=%s AND payload->>'method'=%s
                  AND payload->>'schema_version'=%s
                  AND payload#>>'{{vocal_risk,calibration,cache_key}}'=%s LIMIT 1""",
                (
                    catalog_id,
                    track_id,
                    revision,
                    signature,
                    spec.method,
                    str(version),
                    calibration_key or "uncalibrated-v1",
                ),
            )
            if cur.fetchone() and not force:
                result["ready"].append(track_id)
                continue
            cur.execute(
                f"""SELECT media_revision, status, priority, producer_key,
                (next_retry_at IS NULL OR next_retry_at <= now()), attempts
                FROM {spec.jobs} WHERE catalog_instance_id=%s AND track_id=%s FOR UPDATE""",
                (catalog_id, track_id),
            )
            existing = cur.fetchone()
            same = bool(existing and existing[0] == revision and existing[3] == key)
            if same and existing[1] in ("pending", "running"):
                if existing[1] == "pending" and priority > existing[2]:
                    cur.execute(
                        f"""UPDATE {spec.jobs} SET priority=%s, request_tier=%s, updated_at=now()
                        WHERE catalog_instance_id=%s AND track_id=%s""",
                        (priority, priority_tier, catalog_id, track_id),
                    )
                    result["promoted"].append(track_id)
                else:
                    result["queued"].append(track_id)
                continue
            if (
                same
                and not force
                and (
                    existing[1] in ("unsupported", "cancelled")
                    or (
                        existing[1] == "failed"
                        and (not existing[4] or existing[5] >= 5)
                    )
                )
            ):
                result["blocked"].append(
                    {
                        "track_id": track_id,
                        "status": existing[1],
                        "reason": (
                            "retry_backoff" if existing[1] == "failed" else existing[1]
                        ),
                    }
                )
                continue
            token = str(uuid.uuid4())
            attempts = existing[5] if same and not force else 0
            cur.execute(
                f"""INSERT INTO {spec.jobs} AS job
                (catalog_instance_id, track_id, media_revision, job_token, priority, request_tier,
                 status, producer_key, attempts, media_signature)
                VALUES (%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s)
                ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                    media_signature=EXCLUDED.media_signature, media_revision=EXCLUDED.media_revision, job_token=EXCLUDED.job_token,
                    priority=EXCLUDED.priority, request_tier=EXCLUDED.request_tier,
                    status='pending', producer_key=EXCLUDED.producer_key, attempts=EXCLUDED.attempts,
                    progress_frames=0, error_code=NULL, metrics=NULL, next_retry_at=NULL,
                    requested_at=now(), started_at=NULL, completed_at=NULL, updated_at=now()
                RETURNING job_token""",
                (
                    catalog_id,
                    track_id,
                    revision,
                    token,
                    priority,
                    priority_tier,
                    key,
                    attempts,
                    signature,
                ),
            )
            if cur.fetchone():
                result["accepted"].append(
                    {
                        "track_id": track_id,
                        "media_revision": revision,
                        "job_token": token,
                    }
                )
        db.commit()
        return result
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def claim_next(db, versions=(2, 3)):
    if not acquire_execution(db):
        return None
    claimed = False
    cur = db.cursor()
    try:
        specs = [contract(version) for version in versions]
        for spec in specs:
            cur.execute(
                f"""UPDATE {spec.jobs} SET status='pending', started_at=NULL,
                completed_at=NULL, error_code='worker_restarted', updated_at=now()
                WHERE status='running' """
            )
            # Transient failures advance without a client having to poll/requeue.
            cur.execute(
                f"""UPDATE {spec.jobs} SET status='pending', updated_at=now()
                WHERE status='failed' AND next_retry_at <= now() AND attempts < 5"""
            )
        union = " UNION ALL ".join(
            f"""SELECT {spec.version} AS analysis_version,
            catalog_instance_id, track_id, priority, requested_at FROM {spec.jobs} WHERE status='pending' """
            for spec in specs
        )
        cur.execute(
            f"""SELECT analysis_version, catalog_instance_id, track_id FROM ({union}) candidate
            ORDER BY priority DESC, requested_at, track_id, analysis_version DESC LIMIT 1"""
        )
        selected = cur.fetchone()
        if not selected:
            db.commit()
            return None
        version, catalog_id, track_id = selected
        spec = contract(version)
        token = str(uuid.uuid4())
        cur.execute(
            f"""UPDATE {spec.jobs} SET status='running', job_token=%s, attempts=attempts+1,
            started_at=now(), completed_at=NULL, next_retry_at=NULL, updated_at=now()
            WHERE catalog_instance_id=%s AND track_id=%s AND status='pending'
            RETURNING catalog_instance_id, track_id, media_revision, job_token,
                      attempts, request_tier, priority, producer_key""",
            (token, catalog_id, track_id),
        )
        row = cur.fetchone()
        db.commit()
        if not row:
            return None
        claimed = True
        return dict(
            zip(
                (
                    "catalog_instance_id",
                    "track_id",
                    "media_revision",
                    "job_token",
                    "attempts",
                    "request_tier",
                    "priority",
                    "producer_key",
                ),
                row,
            ),
            analysis_version=int(version),
            release_lock_on_finish=True,
        )
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()
        if not claimed:
            release_execution(db)


def cancelled(db, job):
    spec = contract(job["analysis_version"])
    cur = db.cursor()
    try:
        cur.execute(
            f"""SELECT status FROM {spec.jobs} WHERE catalog_instance_id=%s AND track_id=%s
            AND media_revision=%s AND job_token=%s""",
            (
                job["catalog_instance_id"],
                job["track_id"],
                job["media_revision"],
                job["job_token"],
            ),
        )
        row = cur.fetchone()
        lost = not row or row[0] != "running" or not owns_execution(db)
        db.commit()  # No idle read transaction while decoding/inference runs.
        return lost
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def update_progress(db, job, frames):
    spec = contract(job["analysis_version"])
    cur = db.cursor()
    try:
        cur.execute(
            f"""UPDATE {spec.jobs} SET progress_frames=%s, updated_at=now()
            WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s
              AND job_token=%s AND status='running' """,
            (
                max(0, int(frames)),
                job["catalog_instance_id"],
                job["track_id"],
                job["media_revision"],
                job["job_token"],
            ),
        )
        db.commit()
    finally:
        cur.close()


def finish(db, job, status, error_code=None):
    if status not in ("unsupported", "failed", "cancelled"):
        raise ValueError("invalid DJ terminal status")
    spec = contract(job["analysis_version"])
    cur = db.cursor()
    try:
        cur.execute(
            f"""UPDATE {spec.jobs} SET status=%s, error_code=%s,
            next_retry_at=CASE WHEN %s='failed' THEN now() +
                CASE WHEN attempts <= 1 THEN interval '1 minute'
                     WHEN attempts = 2 THEN interval '5 minutes'
                     WHEN attempts = 3 THEN interval '15 minutes'
                     ELSE interval '60 minutes' END ELSE NULL END,
            completed_at=now(), updated_at=now()
            WHERE catalog_instance_id=%s AND track_id=%s AND media_revision=%s AND job_token=%s
              AND status IN ('pending','running') RETURNING job_token""",
            (
                status,
                str(error_code or status)[:80],
                status,
                job["catalog_instance_id"],
                job["track_id"],
                job["media_revision"],
                job["job_token"],
            ),
        )
        applied = cur.fetchone() is not None
        db.commit()
        return applied
    finally:
        cur.close()
        _finish_ownership(db, job)


def cancel_jobs(db, version, catalog_id, ids=None):
    spec = contract(version)
    cur = db.cursor()
    try:
        clause = " AND track_id=ANY(%s)" if ids is not None else ""
        params = (
            [catalog_id]
            if ids is None
            else [catalog_id, list(dict.fromkeys(ids))[:100]]
        )
        cur.execute(
            f"""UPDATE {spec.jobs} SET status='cancelled', error_code='cancelled',
            next_retry_at=NULL, completed_at=now(), updated_at=now()
            WHERE catalog_instance_id=%s AND status IN ('pending','running','failed'){clause}
            RETURNING track_id""",
            tuple(params),
        )
        tracks = [row[0] for row in cur.fetchall()]
        db.commit()
        return tracks
    finally:
        cur.close()


def publish(db, job, payload, media_signature, metrics=None):
    spec = contract(job["analysis_version"])
    if (
        payload.get("schema_version") != spec.version
        or payload.get("method") != spec.method
        or any(
            payload.get(key) != job[key]
            for key in ("catalog_instance_id", "track_id", "media_revision")
        )
        or analysis_digest(payload) != payload.get("analysis_digest")
        or opaque_revision(media_signature) != job["media_revision"]
    ):
        raise ValueError("DJ publication identity/digest mismatch")
    cur = db.cursor()
    try:
        if not owns_execution(db):
            db.rollback()
            return False
        cur.execute(
            f"""SELECT media_signature FROM {table('source_profiles')}
            WHERE catalog_instance_id=%s AND track_id=%s AND status='ready' FOR UPDATE""",
            (job["catalog_instance_id"], job["track_id"]),
        )
        source = cur.fetchone()
        cur.execute(
            f"""SELECT job_token FROM {spec.jobs} WHERE catalog_instance_id=%s AND track_id=%s
            AND media_revision=%s AND job_token=%s AND status='running' FOR UPDATE""",
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
            return False
        cur.execute(
            f"DELETE FROM {spec.analyses} WHERE catalog_instance_id=%s AND track_id=%s",
            (job["catalog_instance_id"], job["track_id"]),
        )
        cur.execute(
            f"""INSERT INTO {spec.analyses}
            (catalog_instance_id,track_id,media_revision,representation_id,media_signature,analysis_digest,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)""",
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
            f"""UPDATE {spec.jobs} SET status='ready', error_code=NULL, next_retry_at=NULL,
            metrics=%s::jsonb, completed_at=now(), updated_at=now()
            WHERE catalog_instance_id=%s AND track_id=%s AND job_token=%s""",
            (
                canonical_json(metrics or {}),
                job["catalog_instance_id"],
                job["track_id"],
                job["job_token"],
            ),
        )
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()
        _finish_ownership(db, job)


def jobs_pending(db, version, minimum_priority=None, minimum_age_seconds=None):
    spec = contract(version)
    cur = db.cursor()
    try:
        priority = "" if minimum_priority is None else " AND priority >= %s"
        age = (
            "" if minimum_age_seconds is None
            else " AND requested_at <= now() - (%s * interval '1 second')"
        )
        params = (() if minimum_priority is None else (minimum_priority,)) + (
            () if minimum_age_seconds is None else (minimum_age_seconds,)
        )
        cur.execute(
            f"""SELECT EXISTS (SELECT 1 FROM {spec.jobs} WHERE
            (status IN ('pending','running') OR (status='failed' AND next_retry_at <= now() AND attempts < 5)){priority}{age})""",
            params,
        )
        row = cur.fetchone()
        return bool(row and row[0])
    finally:
        cur.close()


def read_analysis(
    db, version, catalog_id, ids, *, calibration_key=None, max_bytes=MAX_RESPONSE_BYTES
):
    spec = contract(version)
    unique = list(dict.fromkeys(ids))[:100]
    key = calibration_key or "uncalibrated-v1"
    cur = db.cursor()
    result = {
        "ready": [],
        "pending": [],
        "unsupported": [],
        "failed": [],
        "missing": [],
        "next_ids": [],
    }
    try:
        # Inspect sizes before transferring JSONB. A batch of 100 long tracks
        # must not first materialize all payloads in Flask just to paginate them.
        cur.execute(
            f"""SELECT p.track_id, p.media_signature, p.status,
            a.analysis_digest, a.payload_bytes, j.status, j.error_code, j.progress_frames,
            j.request_tier, j.priority, j.media_revision, j.producer_key, j.next_retry_at
            FROM {table('source_profiles')} p LEFT JOIN LATERAL (
                SELECT analysis_digest, octet_length(payload::text) AS payload_bytes
                FROM {spec.analyses} WHERE catalog_instance_id=p.catalog_instance_id AND track_id=p.track_id
                  AND media_signature=p.media_signature AND payload->>'method'=%s
                  AND payload->>'schema_version'=%s AND payload#>>'{{vocal_risk,calibration,cache_key}}'=%s
                ORDER BY updated_at DESC LIMIT 1) a ON p.status='ready'
            LEFT JOIN {spec.jobs} j ON j.catalog_instance_id=p.catalog_instance_id AND j.track_id=p.track_id
            WHERE p.catalog_instance_id=%s AND p.track_id=ANY(%s) ORDER BY p.track_id""",
            (spec.method, str(version), key, catalog_id, unique),
        )
        rows = cur.fetchall()
        selected = []
        found = set()
        used = 0
        for (
            track,
            signature,
            source_status,
            digest,
            size,
            status,
            error,
            progress,
            tier,
            priority,
            revision,
            job_key,
            retry_at,
        ) in rows:
            found.add(track)
            if digest and source_status == "ready":
                if selected and used + int(size) > max_bytes:
                    result["next_ids"].append(track)
                elif int(size) > max_bytes:
                    result["unsupported"].append(
                        {"track_id": track, "reason": "analysis_response_too_large"}
                    )
                else:
                    selected.append((track, digest))
                    used += int(size)
            elif (
                revision != opaque_revision(signature)
                or job_key != producer_key(version, key)
                or source_status != "ready"
            ):
                result["missing"].append(track)
            elif status in ("pending", "running"):
                result["pending"].append(
                    {
                        "track_id": track,
                        "status": status,
                        "progress_frames": int(progress or 0),
                        "priority_tier": tier,
                        "priority": int(priority or 0),
                    }
                )
            elif status == "unsupported":
                result["unsupported"].append({"track_id": track, "reason": error})
            elif status in ("failed", "cancelled"):
                result["failed"].append(
                    {
                        "track_id": track,
                        "reason": error,
                        "next_retry_at": retry_at.isoformat() if retry_at else None,
                    }
                )
            else:
                result["missing"].append(track)
        if selected:
            cur.execute(
                f"""SELECT a.track_id,a.analysis_digest,a.payload FROM {spec.analyses} a
                JOIN {table('source_profiles')} p ON p.catalog_instance_id=a.catalog_instance_id
                  AND p.track_id=a.track_id AND p.media_signature=a.media_signature AND p.status='ready'
                WHERE a.catalog_instance_id=%s AND a.track_id=ANY(%s) AND a.analysis_digest=ANY(%s)
                ORDER BY a.track_id""",
                (catalog_id, [v[0] for v in selected], [v[1] for v in selected]),
            )
            wanted = set(selected)
            delivered = set()
            for track, digest, payload in cur.fetchall():
                if (track, digest) in wanted:
                    result["ready"].append(
                        json.loads(payload) if isinstance(payload, str) else payload
                    )
                    delivered.add(track)
            result["missing"].extend(
                track for track, _ in selected if track not in delivered
            )
        result["missing"].extend(track for track in unique if track not in found)
        return result
    finally:
        cur.close()


def backfill_candidates(
    db, version, catalog_id, after="", limit=100, *, calibration_key=None
):
    spec = contract(version)
    key = calibration_key or "uncalibrated-v1"
    cur = db.cursor()
    try:
        cur.execute(
            f"""SELECT p.track_id FROM {table('source_profiles')} p
            LEFT JOIN {spec.jobs} j ON j.catalog_instance_id=p.catalog_instance_id AND j.track_id=p.track_id
            WHERE p.catalog_instance_id=%s AND p.status='ready' AND p.media_signature IS NOT NULL AND p.track_id>%s
              AND NOT EXISTS (SELECT 1 FROM {spec.analyses} a WHERE a.catalog_instance_id=p.catalog_instance_id
                AND a.track_id=p.track_id AND a.media_signature=p.media_signature AND a.payload->>'method'=%s
                AND a.payload->>'schema_version'=%s AND a.payload#>>'{{vocal_risk,calibration,cache_key}}'=%s)
              AND (j.track_id IS NULL OR j.producer_key IS DISTINCT FROM %s OR j.media_signature IS DISTINCT FROM p.media_signature OR j.status='ready'
                   OR (j.status='failed' AND j.next_retry_at<=now() AND j.attempts<5))
            ORDER BY p.track_id LIMIT %s""",
            (
                catalog_id,
                str(after),
                spec.method,
                str(version),
                key,
                producer_key(version, key),
                max(1, min(100, int(limit))),
            ),
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def status_counts(db):
    cur = db.cursor()
    try:
        union = " UNION ALL ".join(
            f"SELECT status FROM {contract(v).jobs}" for v in (2, 3)
        )
        cur.execute(f"SELECT status,count(*) FROM ({union}) jobs GROUP BY status")
        return {row[0]: int(row[1]) for row in cur.fetchall()}
    finally:
        cur.close()


def claim_companion(db, primary, calibration_key=None):
    """Reuse in-memory evidence only for a pending second contract of this source."""
    if not owns_execution(db):
        db.rollback()
        return None
    version = 3 if primary["analysis_version"] == 2 else 2
    spec = contract(version)
    cur = db.cursor()
    try:
        cur.execute(
            f"""UPDATE {spec.jobs} SET status='running',job_token=%s,attempts=attempts+1,
            started_at=now(),updated_at=now() WHERE catalog_instance_id=%s AND track_id=%s
            AND media_revision=%s AND producer_key=%s AND status='pending'
            RETURNING catalog_instance_id,track_id,media_revision,job_token,attempts,request_tier,priority,producer_key""",
            (
                str(uuid.uuid4()),
                primary["catalog_instance_id"],
                primary["track_id"],
                primary["media_revision"],
                producer_key(version, calibration_key),
            ),
        )
        row = cur.fetchone()
        db.commit()
        if row:
            return dict(
                zip(
                    (
                        "catalog_instance_id",
                        "track_id",
                        "media_revision",
                        "job_token",
                        "attempts",
                        "request_tier",
                        "priority",
                        "producer_key",
                    ),
                    row,
                ),
                analysis_version=version,
                release_lock_on_finish=False,
            )
        return None
    finally:
        cur.close()
