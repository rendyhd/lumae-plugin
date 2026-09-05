"""Separate host, artifact, calibration, and playback capability states.

Worker probes are local and lazy. Flask consumes only fresh attestations for
this exact producer. Runtime readiness never authorizes public playback.
"""

from dataclasses import dataclass
from typing import Any, Callable
import json
import os
import re
from . import dj_maintenance
from .dj_contract import (
    SCHEMA_VERSION as DJ_SCHEMA_VERSION,
    METHOD as DJ_METHOD,
    YAMNET_CLASS_MAP_SHA256 as DJ_YAMNET_CLASS_MAP_SHA256,
    YAMNET_MODEL_SHA256 as DJ_YAMNET_MODEL_SHA256,
)
from .dj_analysis_v3 import (
    SCHEMA_VERSION as DJ_V3_SCHEMA_VERSION,
    METHOD as DJ_V3_METHOD,
)
from .vocal_calibration import (
    METHOD as VOCAL_CALIBRATION_METHOD,
    PRIVATE_AUDITION_TIER,
    VocalCalibrationError,
    qualification_tier as vocal_calibration_tier,
)

from .dj_jobs import MAX_RESPONSE_BYTES

DJ_TASK_QUEUE = "lumae-dj"
READ_LIMITS = {
    "max_ids": 100,
    "max_payload_bytes": MAX_RESPONSE_BYTES,
    "continuation": "next_ids",
}


@dataclass(frozen=True)
class CapabilityHost:
    enabled: Callable[[], bool]
    acknowledged: Callable[[], bool]
    setup_state: Callable[[], str]
    model_path: Callable[[], str]
    yamnet_path: Callable[[], str]
    calibration_path: Callable[[], str]
    load_calibration: Callable[[], dict[str, Any] | None]
    runtime_status: Callable[..., dict[str, Any]]
    plugin_version: str
    cache: dict[str, Any]
    get_db: Callable[[], Any]
    reader: Callable[[Any, str], dict[str, Any] | None]
    writer: Callable[[Any, str, dict[str, Any]], dict[str, Any]]
    logger: Any
    probe: Callable[[], dict[str, Any]]


def disabled():
    return {
        "schema_version": DJ_SCHEMA_VERSION,
        "analysis_read_limits": dict(READ_LIMITS),
        "method": DJ_METHOD,
        "enabled": False,
        "acknowledged": False,
        "lifecycle": "disabled",
        "worker_available": False,
        "internal_test_eligible": False,
        "available": False,
        "reference_host_qualified": False,
        "reason": "disabled",
        "calibration_tier": "release",
        "release_authorized": False,
        "vocal_calibration": {
            "method": VOCAL_CALIBRATION_METHOD,
            "status": "unconfigured",
            "artifact_digest": None,
            "cache_key": "uncalibrated-v1",
            "cuts_authorized": False,
            "calibration_tier": "release",
            "release_authorized": False,
        },
        "supported_analysis_versions": [DJ_SCHEMA_VERSION, DJ_V3_SCHEMA_VERSION],
        "supported_plan_versions": [2, 3],
        "analysis_contracts": [
            {"schema_version": DJ_SCHEMA_VERSION, "method": DJ_METHOD},
            {"schema_version": DJ_V3_SCHEMA_VERSION, "method": DJ_V3_METHOD},
        ],
    }


def local_status(host):
    """Probe models and optional dependencies only on the dedicated worker."""
    enabled = host.enabled()
    acknowledged = host.acknowledged() if enabled else False
    if not enabled:
        return disabled()
    if os.environ.get("LUMAE_DJ_WORKER") != "1":
        return {
            **disabled(),
            "enabled": True,
            "acknowledged": acknowledged,
            "lifecycle": host.setup_state(),
            "reason": (
                "acknowledgement_required"
                if not acknowledged
                else "dedicated_worker_required"
            ),
        }
    model_path = host.model_path() if enabled else ""
    yamnet_path = host.yamnet_path() if enabled else ""
    calibration_path = host.calibration_path() if enabled else ""
    if model_path and yamnet_path:
        stats = []
        for path in (model_path, yamnet_path, calibration_path):
            if not path:
                stats.append(("vocal-calibration", None))
                continue
            try:
                info = os.stat(path)
                stats.append(
                    (path, info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
                )
            except OSError:
                stats.append((path, None))
        cache_key = (
            enabled,
            acknowledged,
            host.setup_state(),
            host.plugin_version,
            os.environ.get("LUMAE_DJ_PRIVATE_AUDITION", ""),
            tuple(stats),
        )
    else:
        cache_key = (enabled, False, "disabled", ())
    if host.cache.get("key") == cache_key:
        return json.loads(json.dumps(host.cache["value"]))
    status = host.runtime_status(
        model_path,
        yamnet_path,
        verify_model=enabled,
    )
    status["analysis_read_limits"] = dict(READ_LIMITS)
    status["supported_analysis_versions"] = [DJ_SCHEMA_VERSION, DJ_V3_SCHEMA_VERSION]
    status["supported_plan_versions"] = [2, 3]
    status["analysis_contracts"] = [
        {"schema_version": DJ_SCHEMA_VERSION, "method": DJ_METHOD},
        {"schema_version": DJ_V3_SCHEMA_VERSION, "method": DJ_V3_METHOD},
    ]
    calibration = None
    calibration_error = None
    try:
        calibration = host.load_calibration()
    except (OSError, ValueError, VocalCalibrationError):
        calibration_error = "vocal_calibration_invalid"
    calibration_tier = vocal_calibration_tier(calibration or {})
    cuts_authorized = bool(
        calibration and calibration["authorization"]["cuts_authorized"]
    )
    release_authorized = bool(cuts_authorized and calibration_tier == "release")
    private_audition_authorized = bool(
        cuts_authorized
        and calibration_tier == PRIVATE_AUDITION_TIER
        and os.environ.get("LUMAE_DJ_PRIVATE_AUDITION") == "1"
        and re.fullmatch(r"1\.2\.0-djtest\.[1-9][0-9]*", host.plugin_version)
    )
    status["vocal_calibration"] = {
        "method": VOCAL_CALIBRATION_METHOD,
        "status": (
            "error"
            if calibration_error
            else "ready" if calibration is not None else "unconfigured"
        ),
        "artifact_digest": calibration.get("artifact_digest") if calibration else None,
        "cache_key": (
            calibration["artifact_digest"]
            if calibration is not None
            else "uncalibrated-v1"
        ),
        "cuts_authorized": cuts_authorized,
        "calibration_tier": calibration_tier,
        "release_authorized": release_authorized,
    }
    if calibration_error:
        status["available"] = False
        status["reason"] = calibration_error
    worker_available = bool(status["available"]) if acknowledged else False
    internal_test_eligible = bool(
        worker_available and (release_authorized or private_audition_authorized)
    )
    lifecycle = host.setup_state() if enabled else "disabled"
    if worker_available:
        lifecycle = "ready"
    elif enabled and lifecycle == "ready":
        lifecycle = "error"
    status.update(
        {
            "enabled": enabled,
            "acknowledged": acknowledged,
            "lifecycle": lifecycle,
            "worker_available": worker_available,
            "internal_test_eligible": internal_test_eligible,
            "calibration_tier": calibration_tier,
            "release_authorized": release_authorized,
            # The CPU reference-host and listening gates are evidence gates,
            # not administrator toggles. Keep playback unavailable until a
            # reviewed qualification artifact changes this shipped contract.
            "available": False,
            "reason": (
                "disabled"
                if not enabled
                else (
                    "acknowledgement_required"
                    if not acknowledged
                    else (
                        "private_audition_only"
                        if private_audition_authorized and worker_available
                        else (
                            "vocal_calibration_required"
                            if worker_available and not internal_test_eligible
                            else (
                                "reference_host_unqualified"
                                if worker_available
                                else status["reason"]
                            )
                        )
                    )
                )
            ),
        }
    )
    host.cache.update({"key": cache_key, "value": json.loads(json.dumps(status))})
    return status


def attest(host):
    """Persist a versioned host contract independently of optional model readiness."""
    if os.environ.get("LUMAE_DJ_WORKER") != "1":
        return disabled()
    capability = host.probe()
    capability["host_contract"] = os.environ.get("LUMAE_DJ_HOST_CONTRACT")
    capability["dedicated_queue"] = DJ_TASK_QUEUE
    if capability["host_contract"] != dj_maintenance.HOST_CONTRACT:
        capability.update(
            worker_available=False,
            internal_test_eligible=False,
            reason="dj_host_contract_required",
        )
    return host.writer(host.get_db(), host.plugin_version, capability)


def read_status(host):
    """Project the latest dedicated-worker attestation to Flask and clients."""
    enabled = host.enabled()
    if not enabled:
        return disabled()
    acknowledged = host.acknowledged()
    if not acknowledged:
        return {
            **disabled(),
            "enabled": True,
            "lifecycle": host.setup_state(),
            "reason": "acknowledgement_required",
        }
    try:
        status = host.reader(host.get_db(), host.plugin_version)
    except Exception:
        host.logger.exception("lumae_analysis could not read the DJ worker capability")
        status = None
    if not status:
        return {
            **disabled(),
            "enabled": True,
            "acknowledged": True,
            "lifecycle": "disconnected",
            "reason": "worker_not_attested",
        }
    if status.get("host_contract") != dj_maintenance.HOST_CONTRACT:
        status["reason"] = "dj_host_contract_required"
    status["analysis_read_limits"] = dict(READ_LIMITS)
    worker_available = (
        status.get("worker_available") is True
        and status.get("host_contract") == dj_maintenance.HOST_CONTRACT
    )
    status.update(
        {
            "enabled": True,
            "acknowledged": True,
            "worker_available": worker_available,
            "internal_test_eligible": bool(
                worker_available and status.get("internal_test_eligible") is True
            ),
            # Private test eligibility never promotes the public capability.
            "available": False,
            "reason": (
                status.get("reason")
                or (
                    "private_audition_only"
                    if status.get("calibration_tier") == PRIVATE_AUDITION_TIER
                    else "reference_host_unqualified"
                )
                if status.get("internal_test_eligible") is True
                else (
                    "reference_host_unqualified"
                    if worker_available
                    else status.get("reason") or "worker_unavailable"
                )
            ),
        }
    )
    return status
