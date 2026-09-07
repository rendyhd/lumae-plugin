"""Compatibility entry points for DJ V3 storage; lifecycle lives in dj_jobs."""

from .dj_analysis_v3 import METHOD, SCHEMA_VERSION
from .edge_profiles import opaque_revision
from . import dj_jobs as jobs

PRIORITY_TIERS = jobs.PRIORITY_TIERS


def migrate_dj_analysis_v3(db):
    jobs.migrate(db, 3)


def claim_dj_v3_requests(
    db,
    catalog_id,
    ids,
    *,
    priority_tier,
    expected_vocal_calibration_cache_key=None,
    force=False
):
    result = jobs.request_jobs(
        db,
        3,
        catalog_id,
        ids,
        priority_tier=priority_tier,
        calibration_key=expected_vocal_calibration_cache_key,
        force=force,
    )
    return result["accepted"], result["promoted"], result["ready"], result["queued"]


def claim_next_dj_job_any(db):
    return jobs.claim_next(db)


def claim_next_dj_v3_job(db):
    return jobs.claim_next(db, (3,))


def update_dj_v3_progress(db, job, frames):
    return jobs.update_progress(db, dict(job, analysis_version=3), frames)


def dj_v3_job_cancelled(db, job):
    return jobs.cancelled(db, dict(job, analysis_version=3))


def finish_dj_v3_job(db, job, status, error_code=None):
    return jobs.finish(db, dict(job, analysis_version=3), status, error_code)


def cancel_dj_v3_jobs(db, catalog_id, ids):
    return jobs.cancel_jobs(db, 3, catalog_id, ids)


def publish_dj_v3_analysis(db, job, payload, media_signature):
    return jobs.publish(db, dict(job, analysis_version=3), payload, media_signature)


def read_dj_v3_analysis(
    db, catalog_id, ids, *, expected_vocal_calibration_cache_key=None
):
    return jobs.read_analysis(
        db, 3, catalog_id, ids, calibration_key=expected_vocal_calibration_cache_key
    )


def dj_v3_backfill_candidates(
    db, catalog_id, after="", limit=100, *, expected_vocal_calibration_cache_key=None
):
    return jobs.backfill_candidates(
        db,
        3,
        catalog_id,
        after,
        limit,
        calibration_key=expected_vocal_calibration_cache_key,
    )


def dj_v3_jobs_pending(db):
    return jobs.jobs_pending(db, 3)


def priority_dj_v3_jobs_pending(db):
    return jobs.jobs_pending(db, 3, minimum_priority=50)


def aged_dj_v3_jobs_pending(db):
    """Bound DJ queue starvation behind unrelated profile work to five minutes."""
    return jobs.jobs_pending(db, 3, minimum_age_seconds=300)
