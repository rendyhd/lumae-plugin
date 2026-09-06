"""A reviewed representative audit, not fixture success, gates connection display."""
from __future__ import annotations
import hashlib
import json
from datetime import datetime
from .credits_matching import MATCHING_VERSION

REQUIRED_CATEGORIES = {"well_tagged", "sparse", "duplicate_edition", "compilation"}


def evaluate_audit(report):
    if not isinstance(report, dict) or report.get("matching_version") != MATCHING_VERSION:
        return {"qualified": False, "reason": "audit_required"}
    rows = report.get("albums")
    if not isinstance(rows, list) or len(rows) < 50:
        return {"qualified": False, "reason": "fifty_albums_required"}
    if any(not isinstance(row, dict) or not isinstance(row.get("album_id"), str) or not row["album_id"]\
           or not isinstance(row.get("categories"), list)\
           or any(not isinstance(value, str) for value in row["categories"]) for row in rows):
        return {"qualified": False, "reason": "invalid_audit_sample"}
    ids = {row.get("album_id") for row in rows}
    if len(ids) != len(rows) or None in ids:
        return {"qualified": False, "reason": "unique_album_sample_required"}
    if not isinstance(report.get("reviewed_by"), str) or not report["reviewed_by"].strip() or not isinstance(report.get("reviewed_at"), str) or report.get("synthetic") is not False:
        return {"qualified": False, "reason": "reviewed_catalog_sample_required"}
    try:
        if datetime.fromisoformat(report["reviewed_at"].replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("Audit time needs an offset")
    except ValueError:
        return {"qualified": False, "reason": "reviewed_catalog_sample_required"}
    categories = {category for row in rows for category in row.get("categories", [])}
    if not REQUIRED_CATEGORIES <= categories:
        return {"qualified": False, "reason": "representative_sample_required"}
    if any(not isinstance(row.get("outcome"), str) or row["outcome"] not in {"accepted", "unresolved", "empty"} for row in rows):
        return {"qualified": False, "reason": "invalid_audit_outcomes"}
    accepted = [row for row in rows if row["outcome"] in {"accepted", "empty"}]
    if not accepted or any(type(row.get("correct")) is not bool for row in accepted):
        return {"qualified": False, "reason": "accepted_matches_need_review"}
    correct = sum(row["correct"] for row in accepted)
    precision = correct / len(accepted)
    return {
        "qualified": precision >= 0.98, "reason": "qualified" if precision >= 0.98 else "precision_below_98_percent",
        "sample_albums": len(rows), "accepted": len(accepted), "correct": correct, "precision": precision,
        "unresolved": sum(row["outcome"] == "unresolved" for row in rows),
        "empty": sum(row["outcome"] == "empty" for row in rows),
        "matching_version": MATCHING_VERSION,
        "report_sha256": hashlib.sha256(json.dumps(report, sort_keys=True).encode()).hexdigest(),
    }
