"""Runtime catalogue and analysis admission for AudioMuse 3.

Core versions are diagnostic inputs, never allow-list decisions.  Catalogue
and analysis are admitted independently from observable source, projection,
policy, and repair evidence.  V2 never executes the AudioMuse 3 queries.
"""

from plugin.api import table


CONTRACT_REVISION = 1


def t(name):
    return table(name)


def _detected_core_version(compatibility):
    return str(getattr(compatibility, "core_version", "") or "").strip()


def _task_details(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        import json

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
        # Kept for older diagnostics; final automatic verification also
        # requires Chromaprint completion to predate Cleaning.
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


def _analysis_policy_blockers(policy):
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


def _catalogue_admission(source):
    blockers = []
    if not source.get("catalog_instance_id") or not source.get("server_id"):
        blockers.append("catalog_not_initialized")
    if (source.get("catalog") or {}).get("status") != "complete":
        blockers.append("catalog_generation_incomplete")
    admitted = not blockers
    return {
        "contract_revision": CONTRACT_REVISION,
        "schema_version": 2,
        "status": "ready" if admitted else "not_ready",
        "admitted": admitted,
        "semantic_contracts": [
            "provider_track_ids_v1",
            "complete_catalog_generation_v1",
            "contiguous_change_journal_v1",
        ],
        "blockers": blockers,
    }


def _analysis_admission(db, source, policy):
    blockers = _analysis_policy_blockers(policy)
    if (source.get("analysis") or {}).get("status") != "complete":
        blockers.append("analysis_projection_incomplete")

    try:
        coverage = _coverage(db, source)
        tasks = _task_evidence(db)
    except Exception:
        return (
            {
                "eligible_track_count": 0,
                "mapped_track_count": 0,
                "missing_mapping_count": 0,
                "chromaprint_track_count": 0,
                "chromaprint_missing_count": 0,
                "chromaprint_coverage": 0.0,
                "latest_chromaprint_at_unix": None,
            },
            {},
            {
                "contract_revision": CONTRACT_REVISION,
                "schema_version": 2,
                "status": "not_ready",
                "admitted": False,
                "semantic_contracts": [
                    "analysis_link_evidence_v1",
                    "musicnn_f32le_200_v1",
                    "clap_f32le_512_v1",
                    "audiomuse_musicnn_scalars_v1",
                ],
                "blockers": ["readiness_unavailable"],
            },
        )

    if coverage["eligible_track_count"] > 0 and coverage["mapped_track_count"] == 0:
        blockers.append("no_analysis_mappings")
    elif coverage["chromaprint_missing_count"]:
        blockers.append("chromaprint_backfill_incomplete")

    cleaning = tasks.get("cleaning") or {}
    analysis_after = tasks.get("analysis_after_cleaning") or {}
    cleaning_time = cleaning.get("completed_at_unix")
    latest_chromaprint_at = coverage.get("latest_chromaprint_at_unix")
    chromaprint_complete_before_cleaning = bool(
        coverage["mapped_track_count"] == 0
        or (
            cleaning_time is not None
            and latest_chromaprint_at is not None
            and latest_chromaprint_at <= cleaning_time
        )
    )
    verification_sequence_complete = bool(
        coverage["mapped_track_count"] == 0
        or (
            chromaprint_complete_before_cleaning
            and analysis_after.get("completed_at_unix") is not None
        )
    )
    tasks["chromaprint_complete_before_cleaning"] = chromaprint_complete_before_cleaning
    tasks["verification_sequence_complete"] = verification_sequence_complete
    tasks["upgrade_sequence_complete"] = verification_sequence_complete
    if coverage["mapped_track_count"] > 0 and not verification_sequence_complete:
        blockers.append("analysis_verification_sequence_incomplete")

    admitted = not blockers
    return (
        coverage,
        tasks,
        {
            "contract_revision": CONTRACT_REVISION,
            "schema_version": 2,
            "status": "ready" if admitted else "not_ready",
            "admitted": admitted,
            "semantic_contracts": [
                "analysis_link_evidence_v1",
                "musicnn_f32le_200_v1",
                "clap_f32le_512_v1",
                "audiomuse_musicnn_scalars_v1",
            ],
            "blockers": blockers,
        },
    )


def v3_release_readiness(
    db,
    compatibility,
    source,
    policy,
    acknowledgement=None,
    requested_mode=None,
):
    """Return automatic source-scoped stream admission.

    ``acknowledgement`` and ``requested_mode`` remain accepted for one plugin
    release so callers compiled against the old helper do not break.  They no
    longer influence admission.
    """

    del acknowledgement, requested_mode
    detected_core_version = _detected_core_version(compatibility)
    base = {
        # Legacy fields remain additive/backwards-compatible.  New clients use
        # the explicit per-stream admission object below.
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
        return {
            **base,
            "status": "catalog_not_ready",
            "blockers": list(catalog_admission["blockers"]),
            "admission": {
                "catalog": catalog_admission,
                "analysis": {
                    "contract_revision": CONTRACT_REVISION,
                    "schema_version": 2,
                    "status": "not_ready",
                    "admitted": False,
                    "semantic_contracts": [],
                    "blockers": ["catalog_not_ready"],
                },
            },
        }

    coverage, tasks, analysis_admission = _analysis_admission(db, source, policy)
    ready = analysis_admission["admitted"]
    return {
        **base,
        **coverage,
        "status": "ready" if ready else "repair_incomplete",
        "ready": ready,
        "fully_verified": ready,
        "analysis_sync_allowed": ready,
        "verification_mode": "automatic",
        "task_evidence": tasks,
        "blockers": list(analysis_admission["blockers"]),
        "admission": {
            "catalog": catalog_admission,
            "analysis": analysis_admission,
        },
    }
