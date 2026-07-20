"""AudioMuse 3.0.3 catalogue-repair readiness diagnostics.

AudioMuse owns analysis identity, but it does not expose one durable flag that
proves an upgraded installation completed Chromaprint backfill, Cleaning, and
the following analysis run.  This module combines measurable core state with a
source-scoped administrator acknowledgement.  V2 never executes these queries.
"""

from datetime import datetime, timezone
import json

from plugin.api import get_setting, set_setting, table


ACKNOWLEDGEMENT_SETTING = "v3_catalogue_repair_acknowledgements"
QUALIFIED_CORE_VERSION = "v3.0.3"


def t(name):
    return table(name)


def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _qualified_release(compatibility):
    raw = str(getattr(compatibility, "core_version", "") or "").strip().lower()
    return raw in {"3.0.3", QUALIFIED_CORE_VERSION}


def _setting_map():
    raw = get_setting(ACKNOWLEDGEMENT_SETTING, {})
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


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


def _valid_acknowledgement(source, compatibility, acknowledgement):
    return bool(
        isinstance(acknowledgement, dict)
        and acknowledgement.get("core_version") == QUALIFIED_CORE_VERSION
        and acknowledgement.get("catalog_instance_id")
        == source.get("catalog_instance_id")
        and acknowledgement.get("server_id") == source.get("server_id")
        and acknowledgement.get("verification_mode") in {"fresh", "upgraded"}
        and _qualified_release(compatibility)
    )


def v3_release_readiness(
    db,
    compatibility,
    source,
    policy,
    acknowledgement=None,
    requested_mode=None,
):
    """Return source-scoped, fail-closed v3 release readiness."""
    base = {
        "qualified_core_version": QUALIFIED_CORE_VERSION,
        "detected_core_version": compatibility.core_version,
        "applicable": compatibility.adapter == "v3_registry",
        "status": "not_applicable",
        "ready": compatibility.adapter != "v3_registry",
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
    if not source.get("catalog_instance_id") or not source.get("server_id"):
        return {
            **base,
            "status": "catalog_not_initialized",
            "ready": False,
            "blockers": ["catalog_not_initialized"],
        }

    try:
        coverage = _coverage(db, source)
        tasks = _task_evidence(db)
    except Exception:
        return {
            **base,
            "status": "readiness_unavailable",
            "ready": False,
            "blockers": ["readiness_unavailable"],
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

    if acknowledgement is None:
        acknowledgement = _setting_map().get(source["catalog_instance_id"])
    acknowledged = _valid_acknowledgement(source, compatibility, acknowledgement)
    mode = requested_mode or (
        acknowledgement.get("verification_mode") if acknowledged else None
    )
    blockers = _policy_blockers(policy)
    if source.get("catalog", {}).get("status") != "complete":
        blockers.append("catalog_generation_incomplete")
    if source.get("analysis", {}).get("status") != "complete":
        blockers.append("analysis_projection_incomplete")
    if coverage["mapped_track_count"] == 0:
        blockers.append("no_analysis_mappings")
    elif coverage["chromaprint_missing_count"]:
        blockers.append("chromaprint_backfill_incomplete")
    if mode == "upgraded" and not acknowledged:
        if not task_order_complete:
            blockers.append("upgrade_repair_sequence_incomplete")
        elif not chromaprint_complete_before_cleaning:
            blockers.append("cleaning_predates_chromaprint_completion")
    if requested_mode is None and not acknowledged:
        blockers.append("administrator_acknowledgement_required")

    ready = not blockers and (acknowledged or requested_mode is not None)
    if ready:
        status = "ready"
    elif blockers == ["administrator_acknowledgement_required"]:
        status = "acknowledgement_required"
    else:
        status = "repair_incomplete"
    return {
        **base,
        **coverage,
        "status": status,
        "ready": ready,
        "verification_mode": mode,
        "administrator_acknowledged": acknowledged,
        "acknowledged_at": (
            acknowledgement.get("acknowledged_at") if acknowledged else None
        ),
        "task_evidence": tasks,
        "blockers": blockers,
    }


def acknowledge_v3_release(db, compatibility, source, policy, mode):
    if mode not in {"fresh", "upgraded"}:
        raise ValueError("Choose fresh or upgraded AudioMuse 3.0.3 verification")
    candidate = v3_release_readiness(
        db,
        compatibility,
        source,
        policy,
        acknowledgement={},
        requested_mode=mode,
    )
    if not candidate["ready"]:
        raise ValueError(
            "AudioMuse 3.0.3 is not ready: " + ", ".join(candidate["blockers"])
        )
    acknowledgements = _setting_map()
    acknowledgements[source["catalog_instance_id"]] = {
        "core_version": QUALIFIED_CORE_VERSION,
        "catalog_instance_id": source["catalog_instance_id"],
        "server_id": source["server_id"],
        "verification_mode": mode,
        "acknowledged_at": _iso_now(),
        "cleaning_task_id": (
            (candidate.get("task_evidence", {}).get("cleaning") or {}).get("task_id")
        ),
        "post_clean_analysis_task_id": (
            (
                candidate.get("task_evidence", {}).get("analysis_after_cleaning") or {}
            ).get("task_id")
        ),
    }
    set_setting(ACKNOWLEDGEMENT_SETTING, acknowledgements)
    return v3_release_readiness(
        db,
        compatibility,
        source,
        policy,
        acknowledgement=acknowledgements[source["catalog_instance_id"]],
    )


def clear_v3_release_acknowledgement(catalog_instance_id):
    acknowledgements = _setting_map()
    removed = acknowledgements.pop(str(catalog_instance_id), None) is not None
    set_setting(ACKNOWLEDGEMENT_SETTING, acknowledgements)
    return removed
