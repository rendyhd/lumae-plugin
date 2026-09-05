"""Compatibility entry points for DJ V2 storage; lifecycle lives in dj_jobs."""

import json
from plugin.api import table
from .dj_contract import METHOD, SCHEMA_VERSION
from .dj_evidence import canonical_json, analysis_digest
from .edge_profiles import opaque_revision
from . import dj_jobs as jobs


def migrate_dj_analysis(db):
    jobs.migrate(db, 2)
    cur = db.cursor()
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS {table('dj_worker_capability')} (
        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
        plugin_version TEXT NOT NULL, method TEXT NOT NULL, payload JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"""
    )
    cur.close()


def write_dj_worker_capability(db, plugin_version, payload):
    """Publish one sanitized capability attestation from the worker host."""
    public = json.loads(canonical_json(payload))
    cur = db.cursor()
    cur.execute(
        f"""INSERT INTO {table('dj_worker_capability')} AS current
            (singleton, plugin_version, method, payload, updated_at)
        VALUES (TRUE, %s, %s, %s::jsonb, now())
        ON CONFLICT (singleton) DO UPDATE SET
            plugin_version=EXCLUDED.plugin_version,
            method=EXCLUDED.method,
            payload=EXCLUDED.payload,
            updated_at=now()
        """,
        (str(plugin_version), METHOD, canonical_json(public)),
    )
    db.commit()
    cur.close()
    return public


def read_dj_worker_capability(db, plugin_version):
    """Read a fresh attestation produced by this exact plugin/DJ contract."""
    cur = db.cursor()
    cur.execute(
        f"""SELECT payload FROM {table('dj_worker_capability')}
        WHERE singleton=TRUE AND plugin_version=%s AND method=%s
          AND updated_at >= now()-interval '10 minutes'""",
        (str(plugin_version), METHOD),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    payload = row[0]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return json.loads(canonical_json(payload)) if isinstance(payload, dict) else None


def claim_dj_requests(
    db,
    catalog_id,
    ids,
    *,
    priority=0,
    expected_vocal_calibration_cache_key=None,
    force=False,
):
    result = jobs.request_jobs(
        db,
        2,
        catalog_id,
        ids,
        priority=priority,
        calibration_key=expected_vocal_calibration_cache_key,
        force=force,
    )
    return result["accepted"], result["ready"]


def claim_next_dj_job(db):
    return jobs.claim_next(db, (2,))


def update_dj_progress(db, job, frames):
    return jobs.update_progress(db, dict(job, analysis_version=2), frames)


def dj_job_cancelled(db, job):
    return jobs.cancelled(db, dict(job, analysis_version=2))


def finish_dj_job(db, job, status, error_code=None):
    return jobs.finish(db, dict(job, analysis_version=2), status, error_code)


def cancel_dj_jobs(db, catalog_id, ids):
    return jobs.cancel_jobs(db, 2, catalog_id, ids)


def publish_dj_analysis(db, job, payload, media_signature):
    return jobs.publish(db, dict(job, analysis_version=2), payload, media_signature)


def read_dj_analysis(db, catalog_id, ids, *, expected_vocal_calibration_cache_key=None):
    return jobs.read_analysis(
        db, 2, catalog_id, ids, calibration_key=expected_vocal_calibration_cache_key
    )


def dj_backfill_candidates(
    db, catalog_id, after="", limit=100, *, expected_vocal_calibration_cache_key=None
):
    return jobs.backfill_candidates(
        db,
        2,
        catalog_id,
        after,
        limit,
        calibration_key=expected_vocal_calibration_cache_key,
    )


def dj_jobs_pending(db):
    return jobs.jobs_pending(db, 2)


def interactive_dj_jobs_pending(db):
    return jobs.jobs_pending(db, 2, minimum_priority=10)
