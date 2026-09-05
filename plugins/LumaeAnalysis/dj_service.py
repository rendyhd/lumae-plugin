"""Application service joining the DJ repository, runtime, and projections.

The host supplies configuration/source/queue adapters. HTTP routes and core
hooks do not implement the job state machine, and this module never imports the
plugin composition root.
"""

from dataclasses import dataclass
from typing import Any, Callable
import os
import time
from . import dj_jobs as jobs
from . import dj_maintenance as maintenance
from .dj_contract import DjAnalysisError, JOB_DEADLINE_SECONDS
from .dj_control import CancellationPoller
from .dj_runtime import analyze_evidence
from .dj_analysis import build_dj_analysis
from .dj_analysis_v3 import build_dj_analysis_v3


@dataclass(frozen=True)
class WorkerHost:
    get_db: Callable[[], Any]
    paused: Callable[[], bool]
    capability: Callable[[], dict[str, Any]]
    prepare: Callable[[], dict[str, Any]]
    gate_open: Callable[[dict[str, Any], Any], bool]
    resolve_source: Callable[..., dict[str, Any]]
    load_track: Callable[..., dict[str, Any] | None]
    remove_download: Callable[[str | None], Any]
    model_path: Callable[[], str]
    yamnet_path: Callable[[], str]
    calibration: Callable[[], dict[str, Any] | None]
    enqueue: Callable[[], Any]
    enabled: Callable[[], bool]
    remove_models: Callable[[str, str], list[str]]
    setup_lock: Callable[[], tuple[Any, bool]]
    release_setup_lock: Callable[[Any, bool], Any]
    logger: Any
    extract: Callable[..., Any] = analyze_evidence


def request_analysis(
    host, version, source, ids, *, priority=0, priority_tier="queue", force=False
):
    capability = host.capability()
    response = {
        "capability": capability,
        "accepted": [],
        "promoted": [],
        "already_ready": [],
        "already_queued": [],
        "blocked": [],
        "missing": [],
        "deferred": False,
    }
    if host.paused() or not capability.get("worker_available"):
        response.update(
            deferred=True,
            reason="maintenance_paused" if host.paused() else capability.get("reason"),
        )
        return response, 503
    result = jobs.request_jobs(
        host.get_db(),
        version,
        source["catalog_instance_id"],
        ids,
        priority=priority,
        priority_tier=priority_tier,
        force=force,
        calibration_key=capability.get("vocal_calibration", {}).get("cache_key"),
    )
    response.update(
        accepted=[job["track_id"] for job in result["accepted"]],
        promoted=result["promoted"],
        already_ready=result["ready"],
        already_queued=result["queued"],
        blocked=result["blocked"],
        missing=result["missing"],
    )
    if version == 3:
        response["priority_tier"] = priority_tier
    if result["accepted"] or result["promoted"] or result["queued"]:
        try:
            dispatch(host)
        except Exception:
            response["deferred"] = True
            host.logger.exception(
                "DJ request is durable; reconciliation will retry dispatch"
            )
    return response, 202


def dispatch(host):
    db = host.get_db()
    if maintenance.execution_busy(db) or not maintenance.claim_dispatch(db):
        return False
    host.enqueue()
    return True


def reconcile(host):
    db = host.get_db()
    command = maintenance.state(db)
    removal = command.get("status") in ("pending", "running")
    if not removal and (host.paused() or not host.enabled()):
        return {"status": "idle"}
    capability = host.capability() if host.enabled() else {}
    setup_needed = host.enabled() and not capability.get("worker_available")
    if removal or setup_needed or any(jobs.jobs_pending(db, v) for v in (2, 3)):
        try:
            return {"status": "queued" if dispatch(host) else "waiting"}
        except Exception:
            host.logger.exception("DJ reconciliation will retry the durable request")
            return {"status": "deferred", "reason": "dispatch_failed"}
    return {"status": "idle"}


def _remove_on_worker(host, command):
    db = host.get_db()
    if not jobs.acquire_execution(db):
        return {"status": "deferred", "reason": "worker_busy"}
    lock_db = None
    acquired = False
    try:
        lock_db, acquired = host.setup_lock()
        if not acquired:
            return {"status": "deferred", "reason": "setup_in_progress"}
        # Recheck after both locks; a newer remove command must not be marked done.
        current = maintenance.state(db)
        if current.get("token") != command.get("token"):
            return {"status": "superseded"}
        removed = host.remove_models(host.model_path(), host.yamnet_path())
        maintenance.complete_removal(db, command["token"], removed_files=len(removed))
        maintenance.configure_schedule(db, host.enabled())
        return {"status": "models_removed", "removed_files": len(removed)}
    except Exception:
        db.rollback()
        maintenance.complete_removal(db, command["token"], error="model_removal_failed")
        host.logger.exception("DJ model removal failed on its worker host")
        return {"status": "failed", "reason": "model_removal_failed"}
    finally:
        host.release_setup_lock(lock_db, acquired)
        jobs.release_execution(db)


def run_worker(host):
    # Queue misrouting must not turn a web/default worker into a model host.
    if (
        os.environ.get("LUMAE_DJ_WORKER") != "1"
        or os.environ.get("LUMAE_DJ_HOST_CONTRACT") != maintenance.HOST_CONTRACT
    ):
        return {"status": "deferred", "reason": "dedicated_worker_required"}
    db = host.get_db()
    maintenance.worker_started(db)
    command = maintenance.state(db)
    if command.get("status") in ("pending", "running"):
        return _remove_on_worker(host, command)
    if host.paused() or not host.enabled():
        return {
            "status": "deferred",
            "reason": "maintenance_paused" if host.paused() else "disabled",
        }
    capability = host.capability()
    if not host.gate_open(capability, db):
        return {"status": "deferred", "reason": "profile_work_pending"}
    # Setup also needs a wake-up when no track requests exist yet.
    db.commit()
    capability = host.prepare()
    if not capability.get("worker_available"):
        return {
            "status": "deferred",
            "reason": capability.get("reason", "worker_unavailable"),
        }
    job = jobs.claim_next(db)
    if not job:
        return {"status": "idle"}
    job["release_lock_on_finish"] = False
    info = None
    started = time.monotonic()
    companion_active = False
    outcome = {
        "status": "failed",
        "track_id": job["track_id"],
        "analysis_version": job["analysis_version"],
    }
    try:
        source = host.resolve_source(catalog_instance_id=job["catalog_instance_id"])
        info = host.load_track(
            job["track_id"],
            catalog_instance_id=job["catalog_instance_id"],
            server_id=source["server_id"],
        )
        if (
            not info
            or jobs.opaque_revision(info.get("media_signature"))
            != job["media_revision"]
        ):
            raise DjAnalysisError("source_replaced")
        remaining = JOB_DEADLINE_SECONDS - (time.monotonic() - started)
        if remaining <= 0:
            raise DjAnalysisError("deadline_exceeded")
        poll = CancellationPoller(lambda: not host.enabled() or jobs.cancelled(db, job))
        calibration = host.calibration()
        calibration_key = calibration.get("artifact_digest") if calibration else None
        if job.get("producer_key") != jobs.producer_key(
            job["analysis_version"], calibration_key
        ):
            jobs.request_jobs(
                db,
                job["analysis_version"],
                job["catalog_instance_id"],
                [job["track_id"]],
                priority=job["priority"],
                priority_tier=job["request_tier"],
                calibration_key=calibration_key,
                force=True,
            )
            raise DjAnalysisError("producer_replaced")
        raw = host.extract(
            info["file_path"],
            model_path=host.model_path(),
            yamnet_model_path=host.yamnet_path(),
            deadline_seconds=remaining,
            cancelled=poll,
            progress=lambda count: jobs.update_progress(db, job, count),
        )
        if poll(force=True):
            raise DjAnalysisError("cancelled")
        annotation_started = time.monotonic()
        builder = (
            build_dj_analysis_v3 if job["analysis_version"] == 3 else build_dj_analysis
        )
        payload = builder(
            **raw.annotation_arguments(),
            catalog_instance_id=job["catalog_instance_id"],
            track_id=job["track_id"],
            media_revision=job["media_revision"],
            vocal_calibration=calibration
        )
        if poll(force=True) or time.monotonic() - started >= JOB_DEADLINE_SECONDS:
            raise DjAnalysisError("cancelled" if poll() else "deadline_exceeded")
        applied = jobs.publish(
            db,
            job,
            payload,
            info["media_signature"],
            metrics={
                **dict(raw.timings),
                "annotation_seconds": time.monotonic() - annotation_started,
                "source_acquisition_seconds": annotation_started
                - started
                - dict(raw.timings).get("total_seconds", 0),
            },
        )
        if not applied:
            jobs.finish(db, job, "failed", "source_replaced")
        outcome["status"] = "ready" if applied else "superseded"
        if applied:
            companion = jobs.claim_companion(db, job, calibration_key)
            if companion:
                job = companion  # Failure/cancellation handling must fence this owner.
                companion_active = True
                outcome["companion_version"] = job["analysis_version"]
                poll = CancellationPoller(
                    lambda: not host.enabled() or jobs.cancelled(db, job)
                )
                if poll(force=True):
                    raise DjAnalysisError("cancelled")
                builder = (
                    build_dj_analysis_v3
                    if job["analysis_version"] == 3
                    else build_dj_analysis
                )
                projected = builder(
                    **raw.annotation_arguments(),
                    catalog_instance_id=job["catalog_instance_id"],
                    track_id=job["track_id"],
                    media_revision=job["media_revision"],
                    vocal_calibration=calibration
                )
                if poll(force=True):
                    raise DjAnalysisError("cancelled")
                if time.monotonic() - started >= JOB_DEADLINE_SECONDS:
                    raise DjAnalysisError("deadline_exceeded")
                reused = jobs.publish(
                    db,
                    job,
                    projected,
                    info["media_signature"],
                    metrics={"shared_evidence": True},
                )
                if not reused:
                    jobs.finish(db, job, "failed", "source_replaced")
                outcome["companion_version"] = job["analysis_version"]
                outcome["companion_status"] = "ready" if reused else "superseded"
    except DjAnalysisError as exc:
        db.rollback()
        unsupported = {
            "empty_audio",
            "invalid_model_output",
            "model_timeline_mismatch",
            "non_finite_audio",
            "source_duration_unsupported",
            "source_too_short",
            "source_timeline_changed",
            "unsupported_audio_streams",
        }
        status = (
            "cancelled"
            if exc.code == "cancelled"
            else "unsupported" if exc.code in unsupported else "failed"
        )
        jobs.finish(db, job, status, exc.code)
        if companion_active:
            outcome.update(companion_status=status, companion_reason=exc.code)
        else:
            outcome.update(status=status, reason=exc.code)
    except Exception:
        db.rollback()
        jobs.finish(db, job, "failed", "internal_error")
        host.logger.exception("DJ worker analysis failed")
        if companion_active:
            outcome.update(companion_status="failed", companion_reason="internal_error")
        else:
            outcome.update(status="failed", reason="internal_error")
    finally:
        try:
            if info:
                host.remove_download(info.get("cleanup_path"))
        finally:
            jobs.release_execution(db)
    outcome["queued_next"] = False
    try:
        if (
            host.enabled()
            and not host.paused()
            and any(jobs.jobs_pending(db, v) for v in (2, 3))
            and host.gate_open(capability, db)
        ):
            outcome["queued_next"] = dispatch(host)
    except Exception:
        host.logger.exception("DJ reconciliation will resume the pending work")
    return outcome
