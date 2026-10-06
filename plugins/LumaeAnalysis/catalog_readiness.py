"""Runtime catalogue and sonic-analysis admission for AudioMuse 3.

Core versions are diagnostic inputs, never allow-list decisions. Catalogue and
analysis are admitted independently from observable source, projection, policy,
and per-link evidence. V2 never executes these queries.

Coverage and link counts come from the committed status summary
(``status_model``), written when the catalogue or projection is published; a
request never scans the library for them (P2-1).
"""

from . import status_model


CONTRACT_REVISION = 1
CATALOG_SEMANTIC_CONTRACTS = [
    "provider_track_ids_v1",
    "complete_catalog_generation_v1",
    "contiguous_change_journal_v1",
]
ANALYSIS_SEMANTIC_CONTRACTS = [
    "analysis_link_evidence_v1",
    "musicnn_f32le_200_v1",
    "clap_f32le_512_v1",
    "audiomuse_musicnn_scalars_v1",
]


def _detected_core_version(compatibility):
    return str(getattr(compatibility, "core_version", "") or "").strip()


def historical_task_evidence():
    """The retired analysis -> cleaning -> analysis upgrade-sequence diagnostic.

    It read finished ``main_analysis`` and ``cleaning`` rows from AudioMuse's
    ``task_status``, which never keeps that history: every main-task start
    marks the earlier finished root tasks REVOKED, and from 3.2.0 (the minimum
    core) each finished root task deletes all the others. The sequence could
    never be read back, so health keeps the shape and says why instead of
    reporting it as "not observed". Readiness never depended on it.
    """
    return {
        "analysis_before_cleaning": None,
        "cleaning": None,
        "analysis_after_cleaning": None,
        "upgrade_sequence_complete": False,
        "chromaprint_complete_before_cleaning": False,
        "diagnostics_available": False,
        "unavailable_reason": "audiomuse_keeps_latest_task_only",
    }


def _coverage(db, source, summary=None):
    summary = status_model.read_summary(db, source) if summary is None else summary
    row = status_model.coverage_counts(db, source, summary)
    eligible = int(row[0] or 0)
    mapped = int(row[1] or 0)
    fingerprinted = int(row[2] or 0)
    latest_chromaprint_at = float(row[3]) if row[3] is not None else None
    return {
        "eligible_track_count": eligible,
        "mapped_track_count": mapped,
        "missing_mapping_count": max(0, eligible - mapped),
        "chromaprint_track_count": fingerprinted,
        "chromaprint_missing_count": max(0, mapped - fingerprinted),
        "chromaprint_coverage": fingerprinted / mapped if mapped else 0.0,
        "latest_chromaprint_at_unix": latest_chromaprint_at,
    }


def _link_coverage(db, source, eligible_track_count=0, summary=None):
    summary = status_model.read_summary(db, source) if summary is None else summary
    _links, ready, pending, suspect, missing, verified = status_model.link_counts(
        db, source, summary
    )
    eligible = int(eligible_track_count or 0)
    return {
        "ready_link_count": ready,
        "pending_link_count": pending,
        "suspect_link_count": suspect,
        "missing_link_count": missing,
        "evidence_complete_link_count": verified,
        "verified_link_count": verified,
        # evidence_complete is NOT NULL, so ready links split exactly into
        # verified and provisional ones.
        "provisional_link_count": ready - verified,
        "usable_analysis_coverage": ready / eligible if eligible else 0.0,
    }


def _policy_blockers(policy):
    blockers = []
    if policy.get("catalogue_id_scheme_version") != 4:
        blockers.append("fp_4_not_active")
    tolerance = policy.get("duration_tolerance_seconds")
    if tolerance is None or tolerance > 1.0:
        blockers.append("duration_tolerance_too_wide")
    if not policy.get("folder_aware"):
        blockers.append("folder_gate_not_active")
    if policy.get("chromaprint_collection_enabled") is not True:
        blockers.append("chromaprint_collection_disabled")
    if policy.get("chromaprint_gate_enabled") is not True:
        blockers.append("chromaprint_gate_disabled")
    return blockers


def _stream_admission(admitted, semantics, blockers, status=None):
    return {
        "contract_revision": CONTRACT_REVISION,
        "schema_version": 2,
        "status": status or ("ready" if admitted else "not_ready"),
        "admitted": admitted,
        "semantic_contracts": list(semantics),
        "blockers": list(blockers),
    }


def _catalogue_admission(source):
    blockers = []
    if source.get("rebind_status") == "rebind_required":
        blockers.append("source_rebind_required")
    if not source.get("catalog_instance_id") or not source.get("server_id"):
        blockers.append("catalog_not_initialized")
    catalog = source.get("catalog") or {}
    if catalog.get("status") != "complete":
        blockers.append("catalog_generation_incomplete")
    if catalog.get("refresh_required") is True:
        blockers.append("catalog_refresh_required")
    return _stream_admission(
        not blockers,
        CATALOG_SEMANTIC_CONTRACTS,
        blockers,
        blockers[0] if blockers else "ready",
    )


def v3_release_readiness(
    db,
    compatibility,
    source,
    policy,
    acknowledgement=None,
    requested_mode=None,
):
    """Return automatic, source-scoped stream admission.

    The obsolete acknowledgement arguments remain accepted for one plugin
    release so older callers do not break. They never influence admission.
    """

    del acknowledgement, requested_mode
    detected_core_version = _detected_core_version(compatibility)
    base = {
        # These legacy fields remain additive for older app releases. They now
        # report the detected version rather than an allow-listed release.
        "qualified_core_version": detected_core_version,
        "detected_core_version": detected_core_version,
        "applicable": compatibility.adapter == "v3_registry",
        "status": "not_applicable",
        "ready": compatibility.adapter != "v3_registry",
        "fully_verified": compatibility.adapter != "v3_registry",
        "analysis_sync_allowed": compatibility.adapter != "v3_registry",
        "progressive_analysis": False,
        "verification_mode": None,
        "administrator_acknowledged": False,
        "acknowledged_at": None,
        "blockers": [],
    }
    if compatibility.adapter != "v3_registry":
        return base

    catalog_admission = _catalogue_admission(source)
    if not catalog_admission["admitted"]:
        analysis_admission = _stream_admission(
            False,
            ANALYSIS_SEMANTIC_CONTRACTS,
            ["catalog_not_ready"],
        )
        return {
            **base,
            "status": catalog_admission["status"],
            "blockers": list(catalog_admission["blockers"]),
            "admission": {
                "catalog": catalog_admission,
                "analysis": analysis_admission,
            },
        }

    try:
        summary = status_model.read_summary(db, source)
        coverage = _coverage(db, source, summary)
        link_coverage = _link_coverage(
            db,
            source,
            coverage["eligible_track_count"],
            summary,
        )
    except Exception:
        analysis_admission = _stream_admission(
            False,
            ANALYSIS_SEMANTIC_CONTRACTS,
            ["readiness_unavailable"],
        )
        return {
            **base,
            "status": "readiness_unavailable",
            "blockers": ["readiness_unavailable"],
            "admission": {
                "catalog": catalog_admission,
                "analysis": analysis_admission,
            },
        }
    blockers = _policy_blockers(policy)
    admission_blockers = list(blockers)
    if source.get("analysis", {}).get("status") != "complete":
        blockers.append("analysis_projection_incomplete")
        admission_blockers.append("analysis_projection_incomplete")
    if coverage["mapped_track_count"] == 0:
        blockers.append("no_analysis_mappings")
        admission_blockers.append("no_analysis_mappings")
    else:
        if coverage["missing_mapping_count"]:
            blockers.append("analysis_mapping_incomplete")
        if coverage["chromaprint_missing_count"]:
            blockers.append("chromaprint_backfill_incomplete")
    if link_coverage["pending_link_count"]:
        blockers.append("analysis_links_pending")
    if link_coverage["suspect_link_count"]:
        blockers.append("analysis_links_need_repair")
    if link_coverage["missing_link_count"]:
        blockers.append("analysis_links_missing")
    if link_coverage["provisional_link_count"]:
        blockers.append("provisional_links_remaining")
    if (
        link_coverage["verified_link_count"] != coverage["eligible_track_count"]
        and not any(
            code in blockers
            for code in (
                "no_analysis_mappings",
                "analysis_mapping_incomplete",
                "analysis_links_pending",
                "analysis_links_need_repair",
                "analysis_links_missing",
                "provisional_links_remaining",
            )
        )
    ):
        blockers.append("sonic_evidence_incomplete")
    if policy.get("per_link_chromaprint_evidence_available") is not True:
        blockers.append("per_link_evidence_unavailable")
        admission_blockers.append("per_link_evidence_unavailable")

    analysis_sync_allowed = not admission_blockers
    ready = analysis_sync_allowed and not blockers
    if ready:
        status = "ready"
    elif analysis_sync_allowed:
        status = "progressive"
    else:
        status = "repair_incomplete"
    analysis_admission = _stream_admission(
        analysis_sync_allowed,
        ANALYSIS_SEMANTIC_CONTRACTS,
        admission_blockers,
        status,
    )
    return {
        **base,
        **coverage,
        **link_coverage,
        "status": status,
        "ready": ready,
        "fully_verified": ready,
        "analysis_sync_allowed": analysis_sync_allowed,
        "progressive_analysis": analysis_sync_allowed and not ready,
        "verification_mode": "automatic" if analysis_sync_allowed else None,
        "administrator_acknowledged": False,
        "acknowledged_at": None,
        "task_evidence": historical_task_evidence(),
        "blockers": blockers,
        "admission": {
            "catalog": catalog_admission,
            "analysis": analysis_admission,
        },
    }
