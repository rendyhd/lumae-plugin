"""Automatic AudioMuse 3 sonic-analysis readiness diagnostics.

AudioMuse owns analysis identity. Lumae therefore derives readiness from the
current catalogue, mapping, Chromaprint, and per-link evidence instead of asking
an administrator to attest to historical installation steps. V2 never executes
these queries.
"""

import json
import re

from plugin.api import table


QUALIFIED_CORE_VERSIONS = ("v3.0.3", "v3.0.4", "v3.0.5")
LATEST_QUALIFIED_CORE_VERSION = QUALIFIED_CORE_VERSIONS[-1]


def t(name):
    return table(name)


def _normalized_core_version(compatibility):
    raw = str(getattr(compatibility, "core_version", "") or "").strip().lower()
    match = re.fullmatch(r"v?(\d+\.\d+\.\d+)", raw)
    return f"v{match.group(1)}" if match else None


def _qualified_core_version(compatibility):
    version = _normalized_core_version(compatibility)
    return version if version in QUALIFIED_CORE_VERSIONS else None


def _qualified_release(compatibility):
    return _qualified_core_version(compatibility) is not None


def _task_details(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _task_time(end_time, timestamp):
    if end_time is not None:
        try:
            return float(end_time)
        except (TypeError, ValueError):
            pass
    if hasattr(timestamp, "timestamp"):
        return float(timestamp.timestamp())
    return None


def _task_dto(row):
    ended = _task_time(row[3], row[5])
    details = _task_details(row[4])
    return {
        "task_id": str(row[0]),
        "task_type": str(row[1]),
        "completed_at_unix": ended,
        "failed_servers": list(details.get("failed_servers") or []),
    }


def _task_evidence(db):
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT task_id, task_type, status, end_time, details, timestamp "
            "FROM task_status "
            "WHERE parent_task_id IS NULL "
            "AND task_type IN ('cleaning', 'main_analysis') "
            "AND status = 'SUCCESS' "
            "ORDER BY COALESCE(end_time, EXTRACT(EPOCH FROM timestamp)) DESC "
            "LIMIT 100"
        )
        tasks = [_task_dto(row) for row in cur.fetchall()]
    finally:
        cur.close()

    cleanings = [row for row in tasks if row["task_type"] == "cleaning"]
    analyses = [
        row
        for row in tasks
        if row["task_type"] == "main_analysis" and not row["failed_servers"]
    ]
    cleaning = cleanings[0] if cleanings else None
    cleaning_time = cleaning and cleaning["completed_at_unix"]
    before = None
    after = None
    if cleaning_time is not None:
        before = next(
            (
                row
                for row in analyses
                if row["completed_at_unix"] is not None
                and row["completed_at_unix"] < cleaning_time
            ),
            None,
        )
        after = next(
            (
                row
                for row in analyses
                if row["completed_at_unix"] is not None
                and row["completed_at_unix"] > cleaning_time
            ),
            None,
        )
    return {
        "analysis_before_cleaning": before,
        "cleaning": cleaning,
        "analysis_after_cleaning": after,
        "upgrade_sequence_complete": bool(before and cleaning and after),
        "diagnostics_available": True,
    }


def _coverage(db, source):
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT count(*) AS eligible_tracks,
                   count(m.provider_track_id) AS mapped_tracks,
                   count(CASE WHEN cp.fingerprint IS NOT NULL THEN 1 END)
                     AS fingerprinted_tracks,
                   max(EXTRACT(EPOCH FROM cp.updated_at)) AS latest_chromaprint_at
              FROM {t("catalog_tracks")} ct
              LEFT JOIN track_server_map m
                ON m.server_id=%s AND m.provider_track_id=ct.track_id
              LEFT JOIN chromaprint cp
                ON cp.server_id=m.server_id
               AND cp.provider_track_id=m.provider_track_id
             WHERE ct.catalog_instance_id=%s
               AND ct.published_generation=%s
               AND ct.available=TRUE
               AND ct.analysis_eligible=TRUE
            """,
            (
                source["server_id"],
                source["catalog_instance_id"],
                source["catalog"]["generation"],
            ),
        )
        row = cur.fetchone() or (0, 0, 0, None)
    finally:
        cur.close()
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


def _link_coverage(db, source, eligible_track_count=0):
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT count(*) FILTER (WHERE status='ready') AS ready_links,
                   count(*) FILTER (WHERE status='pending') AS pending_links,
                   count(*) FILTER (
                     WHERE status='suspect'
                        OR review_state IN ('needs_repair', 'needs_review')
                   ) AS suspect_links,
                   count(*) FILTER (WHERE status='missing') AS missing_links,
                   count(*) FILTER (
                     WHERE status='ready' AND evidence_complete=TRUE
                   ) AS verified_links,
                   count(*) FILTER (
                     WHERE status='ready' AND evidence_complete=FALSE
                   ) AS provisional_links
              FROM {t("track_analysis_links")}
             WHERE catalog_instance_id=%s AND projection_generation=%s
            """,
            (
                source["catalog_instance_id"],
                source.get("analysis", {}).get("generation", 0),
            ),
        )
        row = cur.fetchone() or (0, 0, 0, 0, 0, 0)
    finally:
        cur.close()
    eligible = int(eligible_track_count or 0)
    ready = int(row[0] or 0)
    return {
        "ready_link_count": ready,
        "pending_link_count": int(row[1] or 0),
        "suspect_link_count": int(row[2] or 0),
        "missing_link_count": int(row[3] or 0),
        "evidence_complete_link_count": int(row[4] or 0),
        "verified_link_count": int(row[4] or 0),
        "provisional_link_count": int(row[5] or 0),
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


def v3_release_readiness(db, compatibility, source, policy):
    """Return source-scoped, fail-closed automatic v3 readiness."""
    qualified_core_version = _qualified_core_version(compatibility)
    detected_core_version = _normalized_core_version(compatibility) or str(
        getattr(compatibility, "core_version", "") or ""
    ).strip()
    base = {
        "qualified_core_version": (
            qualified_core_version or LATEST_QUALIFIED_CORE_VERSION
        ),
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
    if not _qualified_release(compatibility):
        return {
            **base,
            "status": "core_release_unqualified",
            "ready": False,
            "blockers": ["core_release_unqualified"],
        }
    if source.get("rebind_status") == "rebind_required":
        return {
            **base,
            "status": "source_rebind_required",
            "ready": False,
            "blockers": ["source_rebind_required"],
        }
    if not source.get("catalog_instance_id") or not source.get("server_id"):
        return {
            **base,
            "status": "catalog_not_initialized",
            "ready": False,
            "blockers": ["catalog_not_initialized"],
        }

    try:
        coverage = _coverage(db, source)
        link_coverage = _link_coverage(
            db, source, coverage["eligible_track_count"]
        )
    except Exception:
        return {
            **base,
            "status": "readiness_unavailable",
            "ready": False,
            "blockers": ["readiness_unavailable"],
        }
    try:
        tasks = _task_evidence(db)
    except Exception:
        tasks = {
            "analysis_before_cleaning": None,
            "cleaning": None,
            "analysis_after_cleaning": None,
            "upgrade_sequence_complete": False,
            "diagnostics_available": False,
        }

    cleaning = tasks.get("cleaning") or {}
    cleaning_time = cleaning.get("completed_at_unix")
    latest_chromaprint_at = coverage.get("latest_chromaprint_at_unix")
    chromaprint_complete_before_cleaning = bool(
        cleaning_time is not None
        and latest_chromaprint_at is not None
        and latest_chromaprint_at <= cleaning_time
    )
    task_order_complete = tasks["upgrade_sequence_complete"]
    tasks["chromaprint_complete_before_cleaning"] = chromaprint_complete_before_cleaning
    tasks["upgrade_sequence_complete"] = bool(
        task_order_complete and chromaprint_complete_before_cleaning
    )

    blockers = _policy_blockers(policy)
    progressive_blockers = list(blockers)
    if source.get("catalog", {}).get("status") != "complete":
        blockers.append("catalog_generation_incomplete")
        progressive_blockers.append("catalog_generation_incomplete")
    if source.get("analysis", {}).get("status") != "complete":
        blockers.append("analysis_projection_incomplete")
        progressive_blockers.append("analysis_projection_incomplete")
    if coverage["mapped_track_count"] == 0:
        blockers.append("no_analysis_mappings")
        progressive_blockers.append("no_analysis_mappings")
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
        link_coverage["verified_link_count"]
        != coverage["eligible_track_count"]
        and not any(
            code
            in blockers
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
        progressive_blockers.append("per_link_evidence_unavailable")

    analysis_sync_allowed = not progressive_blockers
    ready = analysis_sync_allowed and not blockers
    if ready:
        status = "ready"
    elif analysis_sync_allowed:
        status = "progressive"
    else:
        status = "repair_incomplete"
    return {
        **base,
        **coverage,
        **link_coverage,
        "status": status,
        "ready": ready,
        "fully_verified": ready,
        "analysis_sync_allowed": analysis_sync_allowed,
        "progressive_analysis": analysis_sync_allowed and not ready,
        "verification_mode": "automatic" if ready else None,
        "administrator_acknowledged": False,
        "acknowledged_at": None,
        "task_evidence": tasks,
        "blockers": blockers,
    }
