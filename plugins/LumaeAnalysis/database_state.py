"""Read-only, source-scoped database diagnostics for Lumae Analysis.

The dashboard deliberately uses aggregate queries against the currently
published catalogue and analysis generations.  It never enumerates track
metadata, exposes credentials, or mutates database state.

Every diagnostic read runs in a savepoint of the host's request transaction
(or, on an autocommit connection, in a transaction of its own) under
``SET LOCAL statement_timeout`` (``DIAGNOSTIC_STATEMENT_TIMEOUT_MS``), and is
always rolled back to that savepoint. One slow or blocked query therefore
cannot hold the web thread, and the host connection keeps its own
``statement_timeout``. A section whose query failed or timed out is reported
as unavailable (``None`` counts), never as zeros.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from html import escape
import re
from time import monotonic

from plugin.api import logger, table

from .profile_publication import (
    RETRY_LIMIT,
    REVISION_FAILURES,
    SAFE_FAILURES,
    TRANSIENT_FAILURES,
)
from .redaction import redact_error_text

# Per statement. On the representative fixture (scripts/perf/seed.py --scale 1:
# 132k tracks, 94k profiles, 76k mappings) the slowest diagnostic reads, the
# AudioMuse core and waveform work-state aggregates, take 0.34-0.39 s (p50-p95)
# and the whole page about 1 s. 3 s leaves about 8x headroom for a larger
# library or a cold cache, and still bounds a read that waits on a lock (a
# migration's ALTER TABLE) or runs away.
DIAGNOSTIC_STATEMENT_TIMEOUT_MS = 3000
_SAVEPOINT = "lumae_diagnostic_read"

_OPERATION_BY_SECTION = {
    "sonic links": "sonic_links_summary",
    "analysis items": "analysis_items_summary",
    "analysis groups": "analysis_groups_summary",
    "waveform profiles": "waveform_profiles_summary",
    "preparation workflow": "preparation_workflow_summary",
    "profile backfill workflow": "profile_backfill_workflow_summary",
    "analysis run workflow": "analysis_runs_summary",
    "catalogue journal": "catalogue_journal_summary",
    "analysis journal": "analysis_journal_summary",
    "bootstrap leases": "bootstrap_leases_summary",
    "AudioMuse core": "audiomuse_core_summary",
    "release readiness": "release_readiness_summary",
}
_SQLSTATE_RE = re.compile(r"[0-9A-Z]{5}")
_MAX_DIAGNOSTIC_OPERATIONS = 16
_MAX_DB_CALL_TIME_MS = 3_600_000


class _Unavailable:
    """What a failed or timed-out diagnostic read returns (never zeros)."""

    def __repr__(self):
        return "UNAVAILABLE"


UNAVAILABLE = _Unavailable()
# A workflow row that could not be read (no row at all is None, "not started").
WORKFLOW_UNAVAILABLE = {"status": None, "unavailable": True}


def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_sqlstate(exc):
    value = str(getattr(exc, "pgcode", None) or getattr(exc, "sqlstate", None) or "").upper()
    return value if _SQLSTATE_RE.fullmatch(value) else None


def _error_metadata(exc):
    sqlstate = _safe_sqlstate(exc)
    if isinstance(exc, TimeoutError) or sqlstate == "57014":
        error_class = "timeout"
    elif sqlstate and sqlstate.startswith("08"):
        error_class = "connection"
    elif sqlstate and sqlstate.startswith("40"):
        error_class = "transaction"
    else:
        error_class = "database_error"
    return {"error_class": error_class, **({"sqlstate": sqlstate} if sqlstate else {})}


def _record_diagnostic(diagnostics, section, started_at, error=None):
    if len(diagnostics) < _MAX_DIAGNOSTIC_OPERATIONS:
        elapsed_ms = max(0, min(int(round((monotonic() - started_at) * 1000)), _MAX_DB_CALL_TIME_MS))
        diagnostics.append({
            "operation": _OPERATION_BY_SECTION[section],
            "server_db_execute_fetch_ms": elapsed_ms,
            "status": "error" if error else "ok",
            **(_error_metadata(error) if error else {}),
        })


def _error(errors, section, exc):
    errors.append({
        "section": section,
        "operation": _OPERATION_BY_SECTION[section],
        "message": "Database diagnostic query failed.",
        **_error_metadata(exc),
    })


def safe_snapshot_error(exc):
    return {
        "section": "database snapshot",
        "operation": "database_snapshot",
        "message": "Database diagnostic snapshot failed.",
        **_error_metadata(exc),
    }


def _log_failure(section, exc):
    # The error class and SQLSTATE only: driver messages can carry SQL, DSN
    # fragments or paths.
    metadata = _error_metadata(exc)
    logger.warning(
        "lumae_analysis diagnostic read %s unavailable (%s, %s%s)",
        _OPERATION_BY_SECTION[section],
        type(exc).__name__,
        metadata["error_class"],
        f", SQLSTATE {metadata['sqlstate']}" if metadata.get("sqlstate") else "",
    )


def _cursor_or_error(db, errors, section):
    try:
        return db.cursor()
    except Exception as exc:
        _error(errors, section, exc)
        _log_failure(section, exc)
        return None


def _owns_transaction(db):
    # An autocommit connection has no transaction to hold a savepoint.
    return getattr(db, "autocommit", False) is True


def _open_bound(cur, owned):
    """Start the savepoint (or owned transaction); True once it exists."""
    cur.execute("BEGIN" if owned else f"SAVEPOINT {_SAVEPOINT}")
    return True


def _set_bound(cur):
    # SET LOCAL lasts until the savepoint is rolled back, or the owned
    # transaction ends: the host's own statement_timeout is never changed.
    cur.execute(f"SET LOCAL statement_timeout = {int(DIAGNOSTIC_STATEMENT_TIMEOUT_MS)}")


def _close_bound(db, cur, owned, opened):
    """Undo everything since the savepoint, the SET LOCAL included.

    Rolling back to the savepoint also recovers the transaction after a failed
    or cancelled read, without discarding the host's transaction. Without a
    savepoint to return to, the transaction is rolled back instead. Returns
    the exception that prevented recovery, or None.
    """
    if opened:
        try:
            cur.execute(
                "ROLLBACK"
                if owned
                else f"ROLLBACK TO SAVEPOINT {_SAVEPOINT}; RELEASE SAVEPOINT {_SAVEPOINT}"
            )
            return None
        except Exception:
            pass
    rollback = getattr(db, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception as exc:
            return exc
    return None


@contextmanager
def bounded_reads(db):
    """Run the block's read-only statements under the diagnostic timeout.

    For reads outside ``collect_database_state`` (source resolution). The
    block's exceptions propagate; the savepoint is always rolled back.
    """
    owned = _owns_transaction(db)
    cur = db.cursor()
    opened = False
    try:
        opened = _open_bound(cur, owned)
        _set_bound(cur)
        yield
    finally:
        _close_bound(db, cur, owned, opened)
        try:
            cur.close()
        except Exception:
            pass


def _run(db, errors, diagnostics, section, work):
    """Run ``work(cursor)`` bounded; its result, or UNAVAILABLE on failure."""
    cur = _cursor_or_error(db, errors, section)
    if cur is None:
        return UNAVAILABLE
    owned = _owns_transaction(db)
    opened = False
    started_at = monotonic()
    try:
        opened = _open_bound(cur, owned)
        _set_bound(cur)
        result = work(cur)
        _record_diagnostic(diagnostics, section, started_at)
        return result
    except Exception as exc:
        _error(errors, section, exc)
        _record_diagnostic(diagnostics, section, started_at, exc)
        _log_failure(section, exc)
        return UNAVAILABLE
    finally:
        failure = _close_bound(db, cur, owned, opened)
        if failure is not None:
            _error(errors, section, failure)
        try:
            cur.close()
        except Exception as exc:
            _error(errors, section, exc)


def _fetchone(db, sql, params, errors, diagnostics, section, default):
    """The first row, ``default`` when there is none, UNAVAILABLE on failure."""

    def work(cur):
        cur.execute(sql, params)
        return cur.fetchone() or default

    return _run(db, errors, diagnostics, section, work)


def _fetchall(db, sql, params, errors, diagnostics, section):
    """All rows, or UNAVAILABLE on failure."""

    def work(cur):
        cur.execute(sql, params)
        return cur.fetchall()

    return _run(db, errors, diagnostics, section, work)


def _counts(keys, row):
    """Integer counts by key; every count is None when the read failed."""
    if row is UNAVAILABLE:
        return {key: None for key in keys}
    return {key: int(value or 0) for key, value in zip(keys, row)}


def _link_state(db, source, errors, diagnostics):
    row = _fetchone(
        db,
        f"""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status='ready') AS usable,
               count(*) FILTER (
                 WHERE status='ready' AND evidence_complete=TRUE
               ) AS verified,
               count(*) FILTER (
                 WHERE status='ready' AND evidence_complete=FALSE
               ) AS provisional,
               count(*) FILTER (WHERE status='pending') AS pending,
               count(*) FILTER (
                 WHERE status='suspect'
                    OR review_state IN ('needs_repair', 'needs_review')
               ) AS suspect,
               count(*) FILTER (WHERE status='missing') AS missing,
               count(DISTINCT analysis_id) FILTER (
                 WHERE status='ready' AND analysis_id IS NOT NULL
               ) AS usable_analysis_ids
          FROM {table("track_analysis_links")}
         WHERE catalog_instance_id=%s AND projection_generation=%s
        """,
        (
            source["catalog_instance_id"],
            source.get("analysis", {}).get("generation", 0),
        ),
        errors,
        diagnostics,
        "sonic links",
        (0,) * 8,
    )
    keys = (
        "total",
        "usable",
        "verified",
        "provisional",
        "pending",
        "suspect",
        "missing",
        "usable_analysis_ids",
    )
    return _counts(keys, row)


def _analysis_item_state(db, source, errors, diagnostics):
    row = _fetchone(
        db,
        f"""
        SELECT count(*) AS items,
               count(*) FILTER (WHERE musicnn_vector IS NOT NULL) AS musicnn,
               count(*) FILTER (WHERE clap_vector IS NOT NULL) AS clap
          FROM {table("analysis_items")}
         WHERE catalog_instance_id=%s AND projection_generation=%s
        """,
        (
            source["catalog_instance_id"],
            source.get("analysis", {}).get("generation", 0),
        ),
        errors,
        diagnostics,
        "analysis items",
        (0, 0, 0),
    )
    return _counts(("items", "musicnn_vectors", "clap_vectors"), row)


def _group_state(db, source, errors, diagnostics):
    row = _fetchone(
        db,
        f"""
        SELECT count(*) AS analysis_groups,
               count(*) FILTER (WHERE occurrences > 1) AS shared_groups,
               COALESCE(max(occurrences), 0) AS largest_group
          FROM (
            SELECT analysis_id, count(*) AS occurrences
              FROM {table("track_analysis_links")}
             WHERE catalog_instance_id=%s AND projection_generation=%s
               AND status='ready' AND analysis_id IS NOT NULL
             GROUP BY analysis_id
          ) groups
        """,
        (
            source["catalog_instance_id"],
            source.get("analysis", {}).get("generation", 0),
        ),
        errors,
        diagnostics,
        "analysis groups",
        (0, 0, 0),
    )
    return _counts(("analysis_groups", "shared_groups", "largest_group"), row)


# ---- waveform profile work states -------------------------------------------
#
# Each available track of the published catalogue is in exactly one state.
# ``due`` is the background scheduler's selection: ``fetch_backfill_rows``
# (``__init__.py``) called with a ``catalog_instance_id``, mirrored predicate by
# predicate. The categories, attempt limit and version slots come from the
# retry model (``profile_publication``), so a new category is classified here
# as soon as it is added there. The scheduler still spells its predicates
# inline; tests/plugins/test_database_state_postgres.py pins that both select
# the same rows in every state. The one deliberate difference: cooldowns are
# compared with ``statement_timestamp()`` (this read) where the scheduler uses
# ``now()`` (its own short transaction).

PROFILE_WORK_STATES = (
    # The scheduler picks it on its next batch.
    "due",
    # An admitted attempt (``pending`` / ``pending_interactive``).
    "pending",
    # Waiting for a catalogue media fingerprint (``deferred_no_media_revision``).
    "deferred_no_media_revision",
    # ``ready`` and current, or no fingerprint to compare with.
    "ready",
    # Released back to the queue (``queue_unavailable``), in its cooldown.
    "deferred",
    # A transient failure in its cooldown (``retry_after`` in the future).
    "cooling",
    # A transient failure with ``RETRY_LIMIT`` attempts used.
    "exhausted",
    # A failure retried only for new media, analyzer or profile schema.
    "awaiting_revision",
    # Nothing in the scheduler will pick it up (LUM-007 stranded rows).
    "unscheduled",
)

_MEDIA_KNOWN = "NULLIF(t.media_fp, '') IS NOT NULL"
_REVISION_CHANGED = f"""({_MEDIA_KNOWN}
                 AND p.retry_media_signature IS NOT NULL
                 AND p.retry_media_signature IS DISTINCT FROM
                     ('catalog-media:' || t.media_fp))"""
# fetch_backfill_rows(catalog_instance_id=...): its WHERE clause after the
# catalogue join, with ``retry_category IN (...)`` from TRANSIENT_FAILURES and
# the literal ``retry_count < 3`` from RETRY_LIMIT.
_DUE = f"""(
            (COALESCE(p.status, '') NOT IN
                 ('pending', 'pending_interactive', 'deferred_no_media_revision')
             OR (p.status='deferred_no_media_revision' AND {_MEDIA_KNOWN}))
            AND (
                p.track_id IS NULL
                OR p.analyzer_ver IS NULL
                OR p.analyzer_ver < %(analyzer_version)s
                OR (p.status='stale' AND (
                        p.retry_category IS NULL
                        OR (p.retry_category='queue_unavailable'
                            AND p.retry_count < %(retry_limit)s
                            AND p.retry_after <= statement_timestamp())
                        OR {_REVISION_CHANGED}))
                OR (p.status='deferred_no_media_revision' AND {_MEDIA_KNOWN})
                OR (p.status='ready' AND {_MEDIA_KNOWN}
                    AND p.media_signature IS DISTINCT FROM
                        ('catalog-media:' || COALESCE(t.media_fp, '')))
                OR (p.status IN ('failed', 'skipped_no_file') AND (
                        {_REVISION_CHANGED}
                        OR (p.retry_analyzer_ver IS NOT NULL
                            AND p.retry_analyzer_ver < %(analyzer_version)s)
                        OR (p.retry_profile_schema_ver IS NOT NULL
                            AND p.retry_profile_schema_ver < %(schema_version)s)
                        OR (p.retry_category IS NULL AND p.retry_count=0)
                        OR (p.retry_category = ANY(%(transient)s)
                            AND p.retry_count < %(retry_limit)s
                            AND p.retry_after <= statement_timestamp())))
            ))"""


def _profile_work_sql(select):
    """``select`` over ``work``: one row per available published track.

    ``work`` has ``track_id``, ``eligible``, ``stored``, ``status``,
    ``retry_category``, ``state`` (NULL for a track that is not
    analysis-eligible; the scheduler never selects those) and ``schedulable``
    (the scheduler only selects from an active source with a complete
    catalogue, otherwise nothing is due).
    """
    return f"""
        WITH source AS (
            SELECT s.catalog_instance_id, c.published_generation,
                   (s.rebind_status='active' AND c.status='complete') AS schedulable
              FROM {table("catalog_sources")} s
              JOIN {table("catalog_state")} c USING (catalog_instance_id)
             WHERE s.catalog_instance_id=%(source)s
        ),
        work AS (
            SELECT t.track_id, source.schedulable,
                   t.analysis_eligible IS TRUE AS eligible,
                   p.track_id IS NOT NULL AS stored,
                   p.status, p.retry_category,
                   CASE
                     WHEN t.analysis_eligible IS NOT TRUE THEN NULL
                     WHEN {_DUE} THEN 'due'
                     WHEN p.status IN ('pending', 'pending_interactive') THEN 'pending'
                     WHEN p.status='deferred_no_media_revision'
                       THEN 'deferred_no_media_revision'
                     WHEN p.status='ready' THEN 'ready'
                     WHEN p.status='stale' AND p.retry_category='queue_unavailable'
                          AND p.retry_count < %(retry_limit)s
                          AND p.retry_after > statement_timestamp()
                       THEN 'deferred'
                     WHEN p.status IN ('failed', 'skipped_no_file')
                          AND p.retry_category = ANY(%(transient)s)
                          AND p.retry_count < %(retry_limit)s
                          AND p.retry_after > statement_timestamp()
                       THEN 'cooling'
                     WHEN p.status IN ('failed', 'skipped_no_file', 'stale')
                          AND p.retry_category = ANY(%(transient)s)
                          AND p.retry_count >= %(retry_limit)s
                       THEN 'exhausted'
                     WHEN p.status IN ('failed', 'skipped_no_file', 'stale')
                          AND p.retry_category = ANY(%(revision)s)
                       THEN 'awaiting_revision'
                     ELSE 'unscheduled'
                   END AS state
              FROM source
              JOIN {table("catalog_tracks")} t
                ON t.catalog_instance_id=source.catalog_instance_id
               AND t.published_generation=source.published_generation
              LEFT JOIN {table("source_profiles")} p
                ON p.track_id=t.track_id
               AND p.catalog_instance_id=source.catalog_instance_id
             WHERE t.available=TRUE
        )
        {select}
    """


_PROFILE_COUNT_KEYS = (
    "catalogue_tracks", "eligible_tracks", "stored", "published", *PROFILE_WORK_STATES,
)


def _profile_counts_select():
    states = ",\n               ".join(
        f"count(*) FILTER (WHERE state='{state}') AS {state}"
        for state in PROFILE_WORK_STATES
    )
    return f"""
        SELECT count(*) AS catalogue_tracks,
               count(*) FILTER (WHERE eligible) AS eligible_tracks,
               count(*) FILTER (WHERE eligible AND stored) AS stored,
               (SELECT count(*) FROM {table("published_source_profiles")}
                 WHERE catalog_instance_id=%(source)s) AS published,
               {states},
               (SELECT bool_or(schedulable) FROM source) AS schedulable,
               (SELECT COALESCE(jsonb_object_agg(category, tracks), '{{}}'::jsonb)
                  FROM (SELECT COALESCE(retry_category, '') AS category,
                               count(*) AS tracks
                          FROM work
                         WHERE eligible AND status IN ('failed', 'skipped_no_file')
                         GROUP BY 1) categories) AS failure_categories
          FROM work
    """


def profile_work_params(catalog_instance_id):
    # The package defines the analyzer and schema versions after importing
    # this module; read them when the diagnostics run, as the scheduler does.
    from . import ANALYZER_VERSION, SCHEMA_VERSION

    return {
        "source": catalog_instance_id,
        "analyzer_version": int(ANALYZER_VERSION),
        "schema_version": int(SCHEMA_VERSION),
        "retry_limit": int(RETRY_LIMIT),
        "transient": sorted(TRANSIENT_FAILURES),
        "revision": sorted(REVISION_FAILURES),
    }


def _category_retry(category):
    if category in TRANSIENT_FAILURES:
        return "transient"
    if category in REVISION_FAILURES:
        return "revision"
    return "unknown"


def _profile_state(db, source, errors, diagnostics):
    row = _fetchone(
        db,
        _profile_work_sql(_profile_counts_select()),
        profile_work_params(source["catalog_instance_id"]),
        errors,
        diagnostics,
        "waveform profiles",
        None,
    )
    if row is UNAVAILABLE or row is None:
        return {
            **{key: None for key in _PROFILE_COUNT_KEYS},
            "schedulable": None,
            "failure_categories": None,
        }
    counts = _counts(_PROFILE_COUNT_KEYS, row)
    categories = row[len(_PROFILE_COUNT_KEYS) + 1] or {}
    return {
        **counts,
        "schedulable": bool(row[len(_PROFILE_COUNT_KEYS)]),
        "failure_categories": [
            {
                "category": str(category) or "uncategorized",
                "tracks": int(tracks or 0),
                "retry": _category_retry(category),
            }
            for category, tracks in sorted(categories.items())
        ],
    }


def _workflow_state(db, source, errors, diagnostics):
    catalog_instance_id = source["catalog_instance_id"]
    preparation = _fetchone(
        db,
        f"""
        SELECT status, phase, queued_profiles, profile_jobs, last_error,
               started_at, completed_at, updated_at
          FROM {table("preparation_state")}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
        errors,
        diagnostics,
        "preparation workflow",
        None,
    )
    backfill = _fetchone(
        db,
        f"""
        SELECT status, processed_profiles, queued_profiles, last_error,
               started_at, completed_at, updated_at
          FROM {table("profile_backfill_state")}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
        errors,
        diagnostics,
        "profile backfill workflow",
        None,
    )
    runs = _fetchall(
        db,
        f"""
        SELECT status, count(*), max(updated_at)
          FROM {table("analysis_runs")}
         WHERE catalog_instance_id=%s
         GROUP BY status
         ORDER BY status
        """,
        (catalog_instance_id,),
        errors,
        diagnostics,
        "analysis run workflow",
    )
    # No row is "not started" (None); a failed read is WORKFLOW_UNAVAILABLE.
    if preparation is UNAVAILABLE:
        preparation = dict(WORKFLOW_UNAVAILABLE)
    elif preparation:
        preparation = {
            "status": preparation[0],
            "phase": preparation[1],
            "queued_profiles": int(preparation[2] or 0),
            "profile_jobs": int(preparation[3] or 0),
            "last_error": preparation[4],
            "started_at": preparation[5],
            "completed_at": preparation[6],
            "updated_at": preparation[7],
        }
    if backfill is UNAVAILABLE:
        backfill = dict(WORKFLOW_UNAVAILABLE)
    elif backfill:
        backfill = {
            "status": backfill[0],
            "processed_profiles": int(backfill[1] or 0),
            "queued_profiles": int(backfill[2] or 0),
            "last_error": backfill[3],
            "started_at": backfill[4],
            "completed_at": backfill[5],
            "updated_at": backfill[6],
        }
    return {
        "preparation": preparation or None,
        "backfill": backfill or None,
        "analysis_runs": (
            None
            if runs is UNAVAILABLE
            else [
                {
                    "status": str(row[0]),
                    "count": int(row[1] or 0),
                    "updated_at": row[2],
                }
                for row in runs
            ]
        ),
    }


def _journal_state(db, source, errors, diagnostics):
    catalog = source.get("catalog") or {}
    analysis = source.get("analysis") or {}
    catalog_rows = _fetchone(
        db,
        f"""
        SELECT count(*)
          FROM {table("catalog_changes")}
         WHERE catalog_instance_id=%s AND epoch=%s
        """,
        (source["catalog_instance_id"], catalog.get("epoch", "")),
        errors,
        diagnostics,
        "catalogue journal",
        (0,),
    )
    analysis_rows = _fetchone(
        db,
        f"""
        SELECT count(*)
          FROM {table("analysis_changes")}
         WHERE catalog_instance_id=%s AND epoch=%s
        """,
        (source["catalog_instance_id"], analysis.get("epoch", "")),
        errors,
        diagnostics,
        "analysis journal",
        (0,),
    )
    leases = _fetchone(
        db,
        f"""
        SELECT count(*) FILTER (
                 WHERE completed_at IS NULL AND expires_at > now()
               ) AS active,
               count(*) FILTER (WHERE completed_at IS NOT NULL) AS completed
          FROM {table("stream_bootstrap_sessions")}
         WHERE catalog_instance_id=%s
        """,
        (source["catalog_instance_id"],),
        errors,
        diagnostics,
        "bootstrap leases",
        (0, 0),
    )
    return {
        "catalog": {
            "epoch": catalog.get("epoch"),
            "head": int(catalog.get("head_seq") or 0),
            "floor": int(catalog.get("floor_seq") or 0),
            "rows": _counts(("rows",), catalog_rows)["rows"],
        },
        "analysis": {
            "epoch": analysis.get("epoch"),
            "head": int(analysis.get("head_seq") or 0),
            "floor": int(analysis.get("floor_seq") or 0),
            "rows": _counts(("rows",), analysis_rows)["rows"],
        },
        "bootstrap_leases": _counts(("active", "completed"), leases),
    }


def _core_state(db, compatibility, source, errors, diagnostics=None):
    diagnostics = diagnostics if diagnostics is not None else []
    if compatibility.adapter == "v3_registry":
        row = _fetchone(
            db,
            """
            SELECT count(*) AS mapping_rows,
                   count(DISTINCT m.item_id) AS canonical_analysis_ids,
                   count(DISTINCT m.item_id) FILTER (
                     WHERE s.item_id IS NOT NULL
                   ) AS scored,
                   count(DISTINCT m.item_id) FILTER (
                     WHERE e.item_id IS NOT NULL
                   ) AS musicnn,
                   count(DISTINCT m.item_id) FILTER (
                     WHERE c.item_id IS NOT NULL
                   ) AS clap,
                   count(DISTINCT m.provider_track_id) FILTER (
                     WHERE cp.fingerprint IS NOT NULL
                   ) AS chromaprint
              FROM track_server_map m
              LEFT JOIN score s ON s.item_id=m.item_id
              LEFT JOIN embedding e ON e.item_id=m.item_id
              LEFT JOIN clap_embedding c ON c.item_id=m.item_id
              LEFT JOIN chromaprint cp
                ON cp.server_id=m.server_id
               AND cp.provider_track_id=m.provider_track_id
             WHERE m.server_id=%s
            """,
            (source.get("server_id"),),
            errors,
            diagnostics,
            "AudioMuse core",
            (0,) * 6,
        )
        return {
            "mode": "source_scoped",
            **_counts(
                (
                    "mapping_rows",
                    "canonical_analysis_ids",
                    "scored",
                    "musicnn_vectors",
                    "clap_vectors",
                    "chromaprint",
                ),
                row,
            ),
        }

    if compatibility.adapter != "v2_single_server":
        # No supported adapter: nothing was counted.
        return {
            "mode": "unavailable",
            "mapping_rows": None,
            "canonical_analysis_ids": None,
            "scored": None,
            "musicnn_vectors": None,
            "clap_vectors": None,
            "chromaprint": None,
        }

    row = _fetchone(
        db,
        """
        SELECT (SELECT count(*) FROM score),
               (SELECT count(*) FROM embedding),
               (SELECT count(*) FROM clap_embedding)
        """,
        (),
        errors,
        diagnostics,
        "AudioMuse core",
        (0, 0, 0),
    )
    counts = _counts(("scored", "musicnn_vectors", "clap_vectors"), row)
    return {
        "mode": "single_server",
        "mapping_rows": counts["scored"],
        "canonical_analysis_ids": counts["scored"],
        **counts,
        "chromaprint": None,
    }


def _readiness_state(db, source, readiness, errors, diagnostics):
    """The release readiness of one source, bounded like the other reads."""
    result = _run(
        db, errors, diagnostics, "release readiness", lambda _cur: readiness(source)
    )
    return {} if result is UNAVAILABLE else (result or {})


def collect_database_state(
    db, compatibility, sources, readiness_by_source=None, readiness=None
):
    """Collect a resilient logical snapshot using aggregate, read-only queries.

    ``readiness``, when given, is called with each source and runs under the
    same statement timeout; ``readiness_by_source`` supplies precomputed
    results instead.
    """
    readiness_by_source = readiness_by_source or {}
    snapshot = {
        "captured_at": _iso_now(),
        "status": "ready",
        "core": compatibility.as_dict(),
        "sources": [],
        "errors": [],
    }
    if db is None:
        snapshot["status"] = "database_unavailable"
        snapshot["errors"].append(
            {
                "section": "database",
                "message": "AudioMuse did not provide a database connection.",
            }
        )
        return snapshot
    if not compatibility.supported:
        snapshot["status"] = "core_unsupported"

    for source in sources:
        source_errors = []
        source_diagnostics = []
        links = _link_state(db, source, source_errors, source_diagnostics)
        items = _analysis_item_state(db, source, source_errors, source_diagnostics)
        groups = _group_state(db, source, source_errors, source_diagnostics)
        profiles = _profile_state(db, source, source_errors, source_diagnostics)
        workflow = _workflow_state(db, source, source_errors, source_diagnostics)
        journals = _journal_state(db, source, source_errors, source_diagnostics)
        core = _core_state(db, compatibility, source, source_errors, source_diagnostics)
        source_readiness = (
            _readiness_state(db, source, readiness, source_errors, source_diagnostics)
            if readiness is not None
            else readiness_by_source.get(source["catalog_instance_id"]) or {}
        )
        snapshot["sources"].append(
            {
                "identity": {
                    "catalog_instance_id": source["catalog_instance_id"],
                    "server_id": source.get("server_id"),
                    "name": source.get("name") or "Music server",
                    "provider_type": source.get("provider_type") or "unknown",
                    "is_default": bool(source.get("is_default")),
                    "rebind_status": source.get("rebind_status") or "unknown",
                },
                "catalog": source.get("catalog") or {},
                "analysis": source.get("analysis") or {},
                "links": links,
                "items": {**items, **groups},
                "profiles": profiles,
                "workflow": workflow,
                "journals": journals,
                "core": core,
                "readiness": source_readiness,
                "diagnostics": {"scope": "server_db_execute_fetch", "unit": "milliseconds", "operations": source_diagnostics},
                "errors": source_errors,
            }
        )
        snapshot["errors"].extend(source_errors)

    if not sources and snapshot["status"] == "ready":
        snapshot["status"] = "not_initialized"
    elif snapshot["errors"] and snapshot["status"] == "ready":
        snapshot["status"] = "partial"
    return snapshot


def _number(value):
    return f"{int(value or 0):,}"


UNAVAILABLE_TEXT = "unavailable"


def _count(value):
    """A diagnostic count; None means its read failed, never zero."""
    return UNAVAILABLE_TEXT if value is None else _number(value)


def _total(values):
    """The sum of per-source counts, unavailable if any one is."""
    values = list(values)
    if any(value is None for value in values):
        return None
    return sum(int(value) for value in values)


def _percent(numerator, denominator):
    if not denominator:
        return 0.0
    return max(0.0, min(100.0, float(numerator or 0) * 100.0 / float(denominator)))


def _timestamp(value):
    if value is None:
        return "never"
    if hasattr(value, "isoformat"):
        value = value.isoformat().replace("+00:00", "Z")
    return escape(str(value))


def _metric(label, value, tone=""):
    tone_class = f" db-metric-{tone}" if tone else ""
    return (
        f'<div class="db-metric{tone_class}">'
        f"<span>{escape(str(label))}</span><strong>{escape(str(value))}</strong></div>"
    )


def _meter(label, numerator, denominator):
    if numerator is None or denominator is None:
        return f"""
      <div class="db-progress">
        <div><span>{escape(label)}</span><strong>{UNAVAILABLE_TEXT}</strong></div>
      </div>
    """
    percent = _percent(numerator, denominator)
    return f"""
      <div class="db-progress">
        <div><span>{escape(label)}</span><strong>{_number(numerator)} / {_number(denominator)}
          ({percent:.1f}%)</strong></div>
        <div class="db-meter" role="progressbar" aria-label="{escape(label)}"
          aria-valuemin="0" aria-valuemax="100" aria-valuenow="{percent:.1f}">
          <span style="width:{percent:.2f}%"></span>
        </div>
      </div>
    """


def _error_list(errors):
    if not errors:
        return '<p class="db-muted">No recorded errors in this snapshot.</p>'
    rows = []
    for row in errors:
        metadata = " · ".join(
            str(row[key]) for key in ("operation", "error_class", "sqlstate")
            if row.get(key)
        )
        detail = f" <small>({escape(metadata)})</small>" if metadata else ""
        # Every message shown here passes the redactor: stored ``last_error``
        # text is free text (P3-10); query errors are fixed messages already.
        message = redact_error_text(row.get("message"), SAFE_FAILURES) or "Unknown error"
        rows.append(
            f"<li><strong>{escape(str(row.get('section') or 'Unknown'))}:</strong> "
            f"{escape(message)}{detail}</li>"
        )
    return '<ul class="db-errors">' + "".join(rows) + "</ul>"


def _operation_list(diagnostics):
    operations = (diagnostics or {}).get("operations") or []
    if not operations:
        return '<p class="db-muted">No server database query timings recorded.</p>'
    rows = []
    for row in operations[:_MAX_DIAGNOSTIC_OPERATIONS]:
        operation = escape(str(row.get("operation") or "unknown"))
        duration = escape(str(row.get("server_db_execute_fetch_ms", "unknown")))
        status = escape(str(row.get("status") or "unknown"))
        metadata = " · ".join(
            str(row[key]) for key in ("error_class", "sqlstate") if row.get(key)
        )
        detail = f" · {escape(metadata)}" if metadata else ""
        rows.append(
            f"<li><span>{operation}</span><strong>{duration} ms · {status}{detail}</strong></li>"
        )
    return '<ul class="db-workflows">' + "".join(rows) + "</ul>"


def _coverage_list(coverage):
    if not coverage:
        return '<p class="db-muted">No field-coverage report has been published yet.</p>'
    rows = []
    for field, value in sorted(coverage.items()):
        ratio = value.get("ratio") if isinstance(value, dict) else value
        try:
            label = f"{float(ratio) * 100:.1f}%"
        except (TypeError, ValueError):
            label = "unknown"
        rows.append(
            f"<li><span>{escape(str(field).replace('_', ' '))}</span>"
            f"<strong>{escape(label)}</strong></li>"
        )
    return f'<ul class="db-coverage-list">{"".join(rows)}</ul>'


_CATEGORY_RETRY_TEXT = {
    "transient": f"retried after a cooldown, up to {RETRY_LIMIT} attempts",
    "revision": "retried when the media, analyzer or profile schema changes",
    "unknown": "not a retry category the scheduler knows",
}


def _failure_category_list(categories):
    if categories is None:
        return f'<p class="db-muted">Failure categories: {UNAVAILABLE_TEXT}.</p>'
    if not categories:
        return '<p class="db-muted">No failed waveform profiles.</p>'
    rows = [
        f"<li><span>{escape(str(row['category']).replace('_', ' '))}</span>"
        f"<strong>{_number(row['tracks'])} · "
        f"{escape(_CATEGORY_RETRY_TEXT.get(row.get('retry'), _CATEGORY_RETRY_TEXT['unknown']))}"
        "</strong></li>"
        for row in categories
    ]
    return '<ul class="db-workflows">' + "".join(rows) + "</ul>"


def _workflow_line(label, state):
    if not state:
        return f"<li><span>{escape(label)}</span><strong>not started</strong></li>"
    if state.get("unavailable"):
        return f"<li><span>{escape(label)}</span><strong>{UNAVAILABLE_TEXT}</strong></li>"
    phase = f" · {state.get('phase')}" if state.get("phase") else ""
    updated = _timestamp(state.get("updated_at"))
    return (
        f"<li><span>{escape(label)}</span>"
        f"<strong>{escape(str(state.get('status') or 'unknown'))}"
        f"{escape(phase)} · {updated}</strong></li>"
    )


def _catalogue_track_count(source):
    entity_counts = (source.get("catalog") or {}).get("entity_counts") or {}
    value = entity_counts.get("track")
    if value is None:
        value = entity_counts.get("tracks")
    if value is None:
        value = (source.get("profiles") or {}).get("catalogue_tracks")
    return max(int(value or 0), 0)


def _app_sync_ready(source):
    return bool(
        _catalogue_track_count(source) > 0
        and (source.get("catalog") or {}).get("status") == "complete"
        and (source.get("analysis") or {}).get("status") == "complete"
    )


def _source_html(source):
    identity = source["identity"]
    catalog = source["catalog"]
    analysis = source["analysis"]
    links = source["links"]
    items = source["items"]
    profiles = source["profiles"]
    core = source["core"]
    journals = source["journals"]
    workflow = source["workflow"]
    readiness = source["readiness"]
    entity_counts = catalog.get("entity_counts") or {}
    tracks = _catalogue_track_count(source)
    albums = entity_counts.get("album") or entity_counts.get("albums") or 0
    artists = entity_counts.get("artist") or entity_counts.get("artists") or 0
    libraries = entity_counts.get("library") or entity_counts.get("libraries") or 0
    app_ready = _app_sync_ready(source)
    empty_catalogue = catalog.get("status") == "complete" and tracks == 0
    if app_ready:
        source_state = "App sync ready"
        source_state_class = "db-state-ready"
    else:
        source_state = "Not ready"
        source_state_class = "db-state-danger"
    if empty_catalogue:
        catalogue_state = "empty - not ready"
        sonic_state = "blocked by empty catalogue"
        catalogue_notice = """
          <div class="db-alert db-alert-danger" role="alert">
            <strong>No Navidrome tracks were published.</strong>
            <span>This catalogue is not usable by Lumae even though the previous publication job
              recorded “complete.” Check Navidrome access and the Music Libraries selection in
              AudioMuse, then return to settings and refresh required data.</span>
          </div>
        """
    else:
        catalogue_state = str(catalog.get("status") or "unknown")
        sonic_state = str(analysis.get("status") or "unknown")
        catalogue_notice = ""
    preparation_workflow = workflow.get("preparation")
    if (
        empty_catalogue
        and preparation_workflow
        and preparation_workflow.get("status") == "ready"
    ):
        preparation_workflow = {
            **preparation_workflow,
            "status": "invalid (recorded ready)",
            "phase": "empty catalogue",
        }
    readiness_status = readiness.get("status") or (
        "not applicable" if core["mode"] == "single_server" else "unavailable"
    )
    readiness_blockers = [
        str(code).replace("_", " ") for code in readiness.get("blockers") or []
    ]
    readiness_detail = (
        " Current verification conditions: "
        + escape(", ".join(readiness_blockers))
        + "."
        if readiness_blockers
        else ""
    )
    chromaprint = core.get("chromaprint")
    core_mapped = core.get("mapping_rows")
    analysis_runs = workflow.get("analysis_runs")
    run_text = (
        UNAVAILABLE_TEXT
        if analysis_runs is None
        else ", ".join(
            f"{row['status']}: {_number(row['count'])}" for row in analysis_runs
        ) or "none recorded"
    )
    profile_note = ""
    if profiles.get("schedulable") is False:
        profile_note = (
            '<p class="db-muted">The background scheduler selects nothing while the '
            "source is not active or its catalogue is not complete; due tracks wait "
            "for that.</p>"
        )
    errors = list(source.get("errors") or [])
    for label, state in (
        ("catalogue", catalog),
        ("analysis projection", analysis),
        ("preparation", workflow.get("preparation")),
        ("profile backfill", workflow.get("backfill")),
    ):
        if state and state.get("last_error"):
            errors.append({"section": label, "message": state["last_error"]})

    chromaprint_metric = (
        _metric("Chromaprint fingerprints", _number(chromaprint), "pending")
        if chromaprint is not None
        else _metric(
            "Chromaprint",
            "not used by v2" if core["mode"] == "single_server" else "unavailable",
        )
    )
    chromaprint_meter = (
        _meter("Chromaprint coverage", chromaprint, core_mapped)
        if chromaprint is not None
        else ""
    )

    return f"""
      <article class="db-source">
        <header class="db-source-header">
          <div>
            <span class="db-kicker">{escape(str(identity['provider_type']))} source</span>
            <h2>{escape(str(identity['name']))}</h2>
          </div>
          <span class="db-state {source_state_class}">{source_state}</span>
        </header>
        <dl class="db-identity">
          <div><dt>Catalogue instance</dt><dd>{escape(str(identity['catalog_instance_id']))}</dd></div>
          <div><dt>AudioMuse server</dt><dd>{escape(str(identity.get('server_id') or 'unbound'))}</dd></div>
          <div><dt>Source binding</dt><dd>{escape(str(identity['rebind_status']))}</dd></div>
          <div><dt>Default source</dt><dd>{'yes' if identity.get('is_default') else 'no'}</dd></div>
        </dl>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Required for app sync</span><h3>1. Navidrome catalogue</h3></div>
            <span class="db-state {'db-state-danger' if empty_catalogue else ''}">{escape(catalogue_state)}</span>
          </div>
          {catalogue_notice}
          <div class="db-metrics">
            {_metric("Published tracks", _number(tracks), "ready" if tracks else "danger")}
            {_metric("Albums", _number(albums))}
            {_metric("Artists", _number(artists))}
            {_metric("Libraries", _number(libraries))}
            {_metric("Generation", _number(catalog.get("generation")))}
          </div>
          <p class="db-muted">Published {_timestamp(catalog.get('completed_at'))}.
            Catalogue journal head {_number(catalog.get('head_seq'))}; floor
            {_number(catalog.get('floor_seq'))}.</p>
          <details>
            <summary>Metadata field coverage</summary>
            {_coverage_list(catalog.get("field_coverage") or {})}
          </details>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Required projection</span><h3>2. Sonic attribution</h3></div>
            <span class="db-state {'db-state-danger' if empty_catalogue else ''}">{escape(sonic_state)}</span>
          </div>
          {_meter("Usable sonic coverage", links["usable"], tracks)}
          <div class="db-metrics">
            {_metric("Usable links", _count(links["usable"]), "ready")}
            {_metric("Verified", _count(links["verified"]), "ready")}
            {_metric("Provisional", _count(links["provisional"]), "pending")}
            {_metric("Pending", _count(links["pending"]), "pending")}
            {_metric("Usable but flagged", _count(links["suspect"]), "danger")}
            {_metric("Missing", _count(links["missing"]))}
          </div>
          <div class="db-metrics">
            {_metric("Analysis items", _count(items["items"]))}
            {_metric("MusiCNN vectors", _count(items["musicnn_vectors"]))}
            {_metric("CLAP vectors", _count(items["clap_vectors"]))}
            {_metric("Shared groups", _count(items["shared_groups"]))}
            {_metric("Largest group", _count(items["largest_group"]))}
            {_metric("Projection generation", _number(analysis.get("generation")))}
          </div>
          <p class="db-muted">Readiness: {escape(str(readiness_status))}. Published
            {_timestamp(analysis.get('completed_at'))}. Provisional and repair-flagged
            links stay usable with their assigned sonic data; the flags preserve
            attribution uncertainty until AudioMuse repairs and republishes the group.
            {readiness_detail}</p>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Automatic sonic evidence</span><h3>3. AudioMuse core</h3></div>
            <span class="db-state">{escape(str(core['mode']).replace('_', ' '))}</span>
          </div>
          <div class="db-metrics">
            {_metric("Provider mappings", _count(core_mapped))}
            {_metric("Canonical analysis IDs", _count(core["canonical_analysis_ids"]))}
            {_metric("Scores", _count(core["scored"]))}
            {_metric("MusiCNN embeddings", _count(core["musicnn_vectors"]))}
            {_metric("CLAP embeddings", _count(core["clap_vectors"]))}
            {chromaprint_metric}
          </div>
          {chromaprint_meter}
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Optional playback enhancements</span><h3>4. Loudness &amp; SmoothFade</h3></div>
          </div>
          {_meter("Published waveform profiles", profiles.get("published"), profiles.get("eligible_tracks"))}
          <div class="db-metrics">
            {_metric("Published", _count(profiles.get("published")), "ready")}
            {_metric("Ready", _count(profiles.get("ready")), "ready")}
            {_metric("Due for analysis", _count(profiles.get("due")), "pending")}
            {_metric("In progress", _count(profiles.get("pending")), "pending")}
            {_metric("Deferred", _count(profiles.get("deferred")), "pending")}
            {_metric("Cooling down", _count(profiles.get("cooling")), "pending")}
            {_metric("Retries exhausted", _count(profiles.get("exhausted")), "danger")}
            {_metric("Awaiting new media", _count(profiles.get("awaiting_revision")), "danger")}
            {_metric("No media fingerprint", _count(profiles.get("deferred_no_media_revision")))}
            {_metric("Not scheduled", _count(profiles.get("unscheduled")), "danger")}
          </div>
          {profile_note}
          <details>
            <summary>Waveform profile states and failure categories</summary>
            <p class="db-muted">Each analysis-eligible track is in one state, as the
              background scheduler sees it. Due: picked on its next batch. Deferred:
              released back to the queue, waiting out its cooldown. Cooling down: a
              transient failure waiting for its retry time. Retries exhausted: the
              attempt limit is used. Awaiting new media: a failure that is retried only
              when the media, analyzer or profile schema changes. No media fingerprint:
              waiting for the catalogue to fingerprint the file. Not scheduled: no retry
              path applies.</p>
            {_failure_category_list(profiles.get("failure_categories"))}
          </details>
          <ul class="db-workflows">
            {_workflow_line("Prepare Lumae", preparation_workflow)}
            {_workflow_line("Profile backfill", workflow.get("backfill"))}
            <li><span>Analysis runs</span><strong>{escape(run_text)}</strong></li>
          </ul>
          <p class="db-muted">Waveform profiles improve normalization and transitions. Missing
            profiles never remove tracks from the catalogue or make them unplayable.</p>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Incremental sync retention</span><h3>Journals &amp; leases</h3></div>
          </div>
          <div class="db-metrics">
            {_metric("Catalogue journal rows", _count(journals["catalog"]["rows"]))}
            {_metric("Catalogue head / floor", f'{_number(journals["catalog"]["head"])} / {_number(journals["catalog"]["floor"])}')}
            {_metric("Analysis journal rows", _count(journals["analysis"]["rows"]))}
            {_metric("Analysis head / floor", f'{_number(journals["analysis"]["head"])} / {_number(journals["analysis"]["floor"])}')}
            {_metric("Active bootstrap leases", _count(journals["bootstrap_leases"]["active"]))}
            {_metric("Completed bootstraps", _count(journals["bootstrap_leases"]["completed"]))}
          </div>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Actionable diagnostics</span><h3>Errors</h3></div>
          </div>
          {_error_list(errors)}
          <details>
            <summary>Server database query timings (execute + fetch)</summary>
            <p class="db-muted">Measured on this server for each aggregate query. Excludes
              network, client processing, and database connection setup.</p>
            {_operation_list(source.get("diagnostics"))}
          </details>
        </section>
      </article>
    """


def render_database_state(snapshot):
    """Render the database snapshot as a standalone responsive admin screen."""
    sources = snapshot.get("sources") or []
    total_tracks = sum(_catalogue_track_count(row) for row in sources)
    total_usable = _total((row.get("links") or {}).get("usable") for row in sources)
    total_profiles = _total((row.get("profiles") or {}).get("published") for row in sources)
    ready_sources = sum(1 for row in sources if _app_sync_ready(row))
    overall_app_state = (
        "ready" if sources and ready_sources == len(sources) else "not ready"
    )
    empty_sources = [
        row
        for row in sources
        if (row.get("catalog") or {}).get("status") == "complete"
        and _catalogue_track_count(row) == 0
    ]
    compatibility = snapshot.get("core") or {}
    source_html = "".join(_source_html(source) for source in sources)
    if not source_html:
        source_html = """
          <section class="db-empty">
            <h2>No published Lumae catalogue yet</h2>
            <p>Return to settings and run Prepare Lumae. This page will populate as soon as
              the provider-authoritative catalogue has been created.</p>
          </section>
        """
    snapshot_errors = snapshot.get("errors") or []
    recorded_state_errors = sum(
        1
        for source in sources
        for state in (
            source.get("catalog"),
            source.get("analysis"),
            source.get("workflow", {}).get("preparation"),
            source.get("workflow", {}).get("backfill"),
        )
        if state and state.get("last_error")
    )
    total_errors = len(snapshot_errors) + recorded_state_errors
    if empty_sources:
        names = ", ".join(
            escape(str((row.get("identity") or {}).get("name") or "Navidrome"))
            for row in empty_sources
        )
        readiness_notice = f"""
          <section class="db-alert db-alert-danger" role="alert">
            <strong>Lumae is not ready: the published catalogue is empty.</strong>
            <span>{names} contains zero published tracks. A completed empty publication is not
              usable. Check Navidrome access and AudioMuse Music Libraries, then return to settings
              and refresh required data.</span>
          </section>
        """
    elif sources and ready_sources < len(sources):
        readiness_notice = """
          <section class="db-alert" role="status">
            <strong>One or more sources are not ready for app sync.</strong>
            <span>Review the numbered source sections below. Waveform coverage is optional and does
              not affect this status.</span>
          </section>
        """
    else:
        readiness_notice = ""
    partial_notice = (
        """
        <section class="db-alert" role="alert">
          <strong>Some diagnostic queries were unavailable.</strong>
          <span>Review the Errors section for the affected operation.</span>
        </section>
        """
        if snapshot_errors
        else ""
    )
    global_errors_html = (
        f'<section class="db-section"><h3>Snapshot errors</h3>{_error_list(snapshot_errors)}</section>'
        if snapshot_errors and not sources else ""
    )
    return f"""
      <style>
        .lumae-db {{
          --db-ink:#17202a; --db-muted:#5f6f7f; --db-line:#d9e2ea;
          --db-soft:#f6f8fb; --db-ready:#247a5a; --db-warn:#b46b00;
          --db-danger:#b42318; --db-accent:#2f6fed;
          background:#fff; border:1px solid var(--db-line); border-radius:12px;
          box-sizing:border-box; color:var(--db-ink); display:grid; gap:20px;
          max-width:1120px; padding:20px; width:100%;
        }}
        .db-topbar {{align-items:center; display:flex; flex-wrap:wrap; gap:10px;
          justify-content:space-between}}
        .db-button {{background:#fff; border:1px solid var(--db-line); border-radius:8px;
          color:var(--db-ink); display:inline-flex; font-weight:700; min-height:40px;
          padding:9px 14px; text-decoration:none}}
        .db-actions {{display:flex; gap:8px}}
        .db-hero {{border-bottom:1px solid var(--db-line); display:grid; gap:10px;
          padding-bottom:18px}}
        .db-hero h1,.db-source h2,.db-section h3,.db-empty h2 {{
          color:var(--db-ink); margin:0}}
        .db-hero p,.db-muted,.db-empty p {{color:var(--db-muted); line-height:1.55; margin:0}}
        .db-kicker {{color:var(--db-muted); font-size:.75rem; font-weight:800;
          text-transform:uppercase}}
        .db-summary,.db-metrics {{display:grid; gap:10px;
          grid-template-columns:repeat(auto-fit,minmax(135px,1fr))}}
        .db-metric {{background:#fff; border:1px solid var(--db-line); border-radius:8px;
          display:grid; gap:7px; min-width:0; padding:13px}}
        .db-metric span {{color:var(--db-muted); font-size:.78rem; font-weight:700}}
        .db-metric strong {{font-size:1.35rem; line-height:1.05; overflow-wrap:anywhere}}
        .db-metric-ready strong {{color:var(--db-ready)}}
        .db-metric-pending strong {{color:var(--db-warn)}}
        .db-metric-danger strong {{color:var(--db-danger)}}
        .db-alert {{background:#fff8eb; border:1px solid #f2c879; border-radius:8px;
          color:#6f4200; display:grid; gap:4px; padding:13px}}
        .db-alert strong {{color:inherit}}
        .db-alert-danger {{background:#fff0ed; border-color:#ffb4a8; color:var(--db-danger)}}
        .db-source {{border:1px solid var(--db-line); border-radius:10px; display:grid;
          gap:0; overflow:hidden}}
        .db-source-header,.db-section-heading {{align-items:center; display:flex; gap:12px;
          justify-content:space-between}}
        .db-source-header {{background:var(--db-soft); padding:18px}}
        .db-state {{background:#fff; border:1px solid var(--db-line); border-radius:999px;
          color:var(--db-ink); font-size:.76rem; font-weight:800; padding:5px 9px;
          white-space:nowrap}}
        .db-state-ready {{background:#e9f6ef; border-color:#a7d8bd; color:#14543c}}
        .db-state-danger {{background:#fff0ed; border-color:#ffb4a8; color:var(--db-danger)}}
        .db-identity {{background:var(--db-soft); display:grid; gap:8px;
          grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); margin:0; padding:0 18px 18px}}
        .db-identity div {{min-width:0}} .db-identity dt {{color:var(--db-muted);
          font-size:.73rem; font-weight:700}} .db-identity dd {{font-family:monospace;
          font-size:.8rem; margin:3px 0 0; overflow-wrap:anywhere}}
        .db-section {{border-top:1px solid var(--db-line); display:grid; gap:14px; padding:18px}}
        .db-progress {{display:grid; gap:7px}} .db-progress>div:first-child {{
          display:flex; flex-wrap:wrap; gap:8px; justify-content:space-between}}
        .db-meter {{background:#dce5ed; border-radius:999px; height:10px; overflow:hidden}}
        .db-meter span {{background:linear-gradient(90deg,var(--db-ready),var(--db-accent));
          display:block; height:100%}}
        .lumae-db details {{border:1px solid var(--db-line); border-radius:8px;
          color:var(--db-ink); padding:10px 12px}}
        .lumae-db summary {{color:var(--db-ink); cursor:pointer; font-weight:700}}
        .db-coverage-list,.db-workflows,.db-errors {{display:grid; gap:8px; list-style:none;
          margin:10px 0 0; padding:0}}
        .db-coverage-list li,.db-workflows li {{align-items:baseline; display:flex; gap:12px;
          justify-content:space-between}}
        .db-coverage-list span,.db-workflows span {{color:var(--db-muted)}}
        .db-workflows strong {{text-align:right}}
        .db-errors {{list-style:disc; padding-left:20px}}
        .db-empty {{background:var(--db-soft); border:1px solid var(--db-line);
          border-radius:10px; display:grid; gap:8px; padding:20px}}
        @media(max-width:620px) {{
          .lumae-db {{padding:14px}}
          .db-source-header,.db-section-heading {{align-items:flex-start}}
          .db-metrics {{grid-template-columns:repeat(2,minmax(0,1fr))}}
          .db-coverage-list li,.db-workflows li {{align-items:flex-start; flex-direction:column;
            gap:2px}} .db-workflows strong {{text-align:left}}
        }}
      </style>
      <main class="lumae-db" aria-label="Lumae database state">
        <header class="db-hero">
          <span class="db-kicker">Read-only diagnostics</span>
          <h1>Lumae database state</h1>
          <p>This is the currently published, source-scoped view used by Lumae. Counts use
            aggregate queries only and include no track names, credentials, or media paths.</p>
          <p>Captured {escape(str(snapshot.get('captured_at') or 'unknown'))} ·
            AudioMuse {escape(str(compatibility.get('core_version') or 'unknown'))} ·
            {escape(str(compatibility.get('core_adapter') or 'no adapter'))} ·
            diagnostic queries {escape(str(snapshot.get('status') or 'unknown'))} ·
            app sync {overall_app_state}</p>
          <nav class="db-topbar">
            <a class="db-button" href="settings">← Lumae Analysis settings</a>
            <div class="db-actions"><a class="db-button" href="database-state">Refresh snapshot</a></div>
          </nav>
        </header>
        {readiness_notice}
        {partial_notice}
        {global_errors_html}
        <section class="db-summary" aria-label="Database summary">
          {_metric("Sources", _number(len(sources)))}
          {_metric("App-ready sources", f"{ready_sources} / {len(sources)}", "ready" if ready_sources == len(sources) and sources else "danger")}
          {_metric("Published tracks", _number(total_tracks), "ready" if total_tracks else "danger")}
          {_metric("Usable sonic links", _count(total_usable), "ready" if total_usable else "")}
          {_metric("Published waveform profiles", _count(total_profiles), "ready" if total_profiles else "")}
          {_metric("Query / workflow errors", _number(total_errors), "danger" if total_errors else "")}
        </section>
        {source_html}
      </main>
    """
