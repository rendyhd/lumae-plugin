"""Committed status summary: one read model for health, settings and database state.

LUM-011 / plan P2-1. ``/api/catalog/health``, ``/settings/status`` and
``/database-state`` read the summaries below. They do not write, and they do
not scan the library while a summary describes the published state.

Each summary is written by the work that changes what it describes:

``analysis_state`` link counts
    In the transaction that publishes a projection generation (the projection
    and the provider-identity rekey, ``persist_analysis_summary``).
    ``summary_generation`` is the projection generation they are exact for.
``status_summary`` catalogue coverage (eligible, mapped and fingerprinted tracks)
    ``refresh_status_summary``, in its own short transaction right after
    every catalogue refresh (publication or no change), every projection
    (publication or no change) and the rekey publication, and by the watchdog
    tick for a source whose summary is missing. Eligible tracks change with a
    catalogue generation; the AudioMuse mapping and Chromaprint rows are host
    data that change between publications, and every analysis run ends with a
    catalogue refresh and a projection. It is read, compared and written only
    when different, so nothing is locked while counting and an unchanged
    refresh writes nothing. ``coverage_generation`` and
    ``coverage_server_id`` say what the counts describe.
``status_summary`` waveform profile counts
    ``store_profile_counts``, a snapshot taken by background work: at install,
    after every watchdog tick, after the catalogue refresh cron, after an
    interactive analysis and, during an AudioMuse run, at most every
    ``PROFILE_COUNTS_MIN_AGE_SECONDS``. ``profile_generation`` is the catalogue
    generation.

When no summary describes the published state (for example a generation that a
draining older worker published during an upgrade, or the moment between a
publication and its coverage refresh), the reader computes the same aggregate
live and read-only. The writers run the same SQL, so a summary always equals
what the live query returned when it was taken.
"""

from plugin.api import logger, table


PROFILE_COUNTS_MIN_AGE_SECONDS = 30


def t(name):
    return table(name)


# The aggregates the readiness routes ran on every request before P2-1
# (catalog_readiness._coverage and _link_coverage), unchanged apart from the
# parameter slots. They now run when what they count changes.
def coverage_sql(server="%s", source="%s", generation="%s"):
    return f"""
        SELECT count(*) AS eligible_tracks,
               count(m.provider_track_id) AS mapped_tracks,
               count(CASE WHEN cp.fingerprint IS NOT NULL THEN 1 END)
                 AS fingerprinted_tracks,
               max(EXTRACT(EPOCH FROM cp.updated_at)) AS latest_chromaprint_at
          FROM {t("catalog_tracks")} ct
          LEFT JOIN track_server_map m
            ON m.server_id={server} AND m.provider_track_id=ct.track_id
          LEFT JOIN chromaprint cp
            ON cp.server_id=m.server_id
           AND cp.provider_track_id=m.provider_track_id
         WHERE ct.catalog_instance_id={source}
           AND ct.published_generation={generation}
           AND ct.available=TRUE
           AND ct.analysis_eligible=TRUE
    """


def link_counts_sql():
    return f"""
        SELECT count(*) AS links,
               count(*) FILTER (WHERE status='ready') AS ready_links,
               count(*) FILTER (WHERE status='pending') AS pending_links,
               count(*) FILTER (
                 WHERE status='suspect'
                    OR review_state IN ('needs_repair', 'needs_review')
               ) AS suspect_links,
               count(*) FILTER (WHERE status='missing') AS missing_links,
               count(*) FILTER (
                 WHERE status='ready' AND evidence_complete=TRUE
               ) AS evidence_complete_links
          FROM {t("track_analysis_links")}
         WHERE catalog_instance_id=%s AND projection_generation=%s
    """


ANALYSIS_SUMMARY_COLUMNS = (
    ("summary_generation", "BIGINT"),
    ("link_count", "BIGINT"),
    ("ready_link_count", "BIGINT"),
    ("pending_link_count", "BIGINT"),
    ("suspect_link_count", "BIGINT"),
    ("missing_link_count", "BIGINT"),
    ("evidence_complete_link_count", "BIGINT"),
    ("summary_updated_at", "TIMESTAMPTZ"),
)
_LINK_COUNT_COLUMNS = (
    "link_count",
    "ready_link_count",
    "pending_link_count",
    "suspect_link_count",
    "missing_link_count",
    "evidence_complete_link_count",
)
_COVERAGE_COLUMNS = (
    "eligible_track_count",
    "mapped_track_count",
    "fingerprinted_track_count",
    "latest_chromaprint_at",
)
_PROFILE_COLUMNS = (
    "profile_total_count",
    "profile_ready_count",
    "profile_pending_count",
    "profile_failed_count",
    "profile_skipped_count",
)


def migrate_status_summary(cur):
    """Additive, idempotent schema for the summaries (run by ``migrate_catalog``).

    Existing columns are looked up first, so a re-run issues no ALTER and takes
    no ACCESS EXCLUSIVE lock on ``analysis_state``.
    """
    cur.execute(
        "SELECT attname FROM pg_attribute "
        "WHERE attrelid=to_regclass(%s) AND attnum > 0 AND NOT attisdropped",
        (t("analysis_state"),),
    )
    existing = {str(row[0]) for row in cur.fetchall()}
    missing = [
        f"ADD COLUMN IF NOT EXISTS {name} {kind}"
        for name, kind in ANALYSIS_SUMMARY_COLUMNS
        if name not in existing
    ]
    if missing:
        cur.execute(f"ALTER TABLE {t('analysis_state')} {', '.join(missing)}")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {t('status_summary')} (
            catalog_instance_id TEXT PRIMARY KEY
                REFERENCES {t('catalog_sources')}(catalog_instance_id)
                ON DELETE CASCADE,
            coverage_generation BIGINT,
            coverage_server_id TEXT,
            eligible_track_count BIGINT,
            mapped_track_count BIGINT,
            fingerprinted_track_count BIGINT,
            latest_chromaprint_at DOUBLE PRECISION,
            coverage_updated_at TIMESTAMPTZ,
            profile_generation BIGINT,
            profile_total_count BIGINT,
            profile_ready_count BIGINT,
            profile_pending_count BIGINT,
            profile_failed_count BIGINT,
            profile_skipped_count BIGINT,
            profile_counted_at TIMESTAMPTZ
        )
        """
    )


# ---- writers --------------------------------------------------------------


def persist_analysis_summary(cur, catalog_instance_id, generation):
    """Store the link counts of one published projection generation.

    Runs inside the transaction that publishes the generation. A generation is
    immutable once published, so an existing summary for it is left alone.
    """
    # A data-modifying CTE: the counts are evaluated only when the row updates.
    cur.execute(
        f"""
        WITH link_counts AS ({link_counts_sql()}),
        analysis_summary AS (
            UPDATE {t('analysis_state')} AS a
               SET ({', '.join(_LINK_COUNT_COLUMNS)}) = (SELECT * FROM link_counts),
                   summary_generation=%s,
                   summary_updated_at=clock_timestamp()
             WHERE a.catalog_instance_id=%s
               AND a.summary_generation IS DISTINCT FROM %s
            RETURNING 1
        )
        SELECT count(*) FROM analysis_summary
        """,
        (catalog_instance_id, generation, generation, catalog_instance_id, generation),
    )


def _summary_state_sql(with_coverage):
    """What the summaries describe now, what they hold, and the coverage now.

    One statement, so the counts and the generation they describe come from
    one snapshot.
    """
    if with_coverage:
        coverage = (
            "CROSS JOIN LATERAL ("
            + coverage_sql("src.server_id", "src.catalog_instance_id", "src.catalog_generation")
            + ") AS cov"
        )
        values = (
            "cov.eligible_tracks, cov.mapped_tracks, cov.fingerprinted_tracks, "
            "cov.latest_chromaprint_at::double precision"
        )
    else:
        coverage = ""
        values = "NULL, NULL, NULL, NULL"
    return f"""
        WITH src AS (
            SELECT s.catalog_instance_id, s.current_core_server_id AS server_id,
                   s.rebind_status, c.published_generation AS catalog_generation,
                   a.projection_generation, a.summary_generation
              FROM {t('catalog_sources')} s
              JOIN {t('catalog_state')} c USING (catalog_instance_id)
              LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
             WHERE s.catalog_instance_id=%s
        )
        SELECT src.server_id, src.rebind_status, src.catalog_generation,
               src.projection_generation, src.summary_generation,
               {values}, statement_timestamp(),
               ss.catalog_instance_id IS NOT NULL, ss.coverage_generation,
               ss.coverage_server_id, ss.{', ss.'.join(_COVERAGE_COLUMNS)}
          FROM src
          {coverage}
          LEFT JOIN {t('status_summary')} ss
            ON ss.catalog_instance_id=src.catalog_instance_id
    """


# The newest counts win: a newer catalogue generation, or for the same
# generation a later snapshot. A slower concurrent refresh never replaces them.
_COVERAGE_NOT_OLDER = """
        (s.coverage_generation IS NULL
         OR s.coverage_generation < %(generation)s
         OR (s.coverage_generation = %(generation)s
             AND (s.coverage_updated_at IS NULL
                  OR s.coverage_updated_at <= %(counted_at)s)))
"""


def _write_coverage(cur, catalog_instance_id, row_exists, values):
    params = {"catalog_instance_id": catalog_instance_id, **values}
    assignments = ", ".join(
        f"{name}=%({name})s"
        for name in ("coverage_server_id", *_COVERAGE_COLUMNS)
    )
    if row_exists:
        cur.execute(
            f"""
            UPDATE {t('status_summary')} AS s
               SET coverage_generation=%(generation)s, {assignments},
                   coverage_updated_at=%(counted_at)s
             WHERE s.catalog_instance_id=%(catalog_instance_id)s
               AND {_COVERAGE_NOT_OLDER}
            """,
            params,
        )
    else:
        cur.execute(
            f"""
            INSERT INTO {t('status_summary')} AS s
                (catalog_instance_id, coverage_generation, coverage_server_id,
                 {', '.join(_COVERAGE_COLUMNS)}, coverage_updated_at)
            VALUES (%(catalog_instance_id)s, %(generation)s, %(coverage_server_id)s,
                    %(eligible_track_count)s, %(mapped_track_count)s,
                    %(fingerprinted_track_count)s, %(latest_chromaprint_at)s,
                    %(counted_at)s)
            ON CONFLICT (catalog_instance_id) DO UPDATE
               SET coverage_generation=%(generation)s, {assignments},
                   coverage_updated_at=%(counted_at)s
             WHERE {_COVERAGE_NOT_OLDER}
            """,
            params,
        )


def _refresh_summaries(cur, catalog_instance_id, with_coverage):
    """Bring the source's summaries up to date; write only what differs.

    Returns True when a statement wrote. No lock is taken unless it does.
    """
    cur.execute(_summary_state_sql(with_coverage), (catalog_instance_id,))
    row = cur.fetchone()
    if row is None:
        return False
    (server_id, rebind_status, catalog_generation, projection_generation,
     summary_generation, eligible, mapped, fingerprinted, latest, counted_at,
     row_exists, *stored) = row
    wrote = False
    if projection_generation is not None and summary_generation != projection_generation:
        persist_analysis_summary(cur, catalog_instance_id, int(projection_generation))
        wrote = True
    if not (with_coverage and server_id and rebind_status == "active" and catalog_generation):
        return wrote
    values = {
        "generation": int(catalog_generation),
        "coverage_server_id": str(server_id),
        "eligible_track_count": int(eligible or 0),
        "mapped_track_count": int(mapped or 0),
        "fingerprinted_track_count": int(fingerprinted or 0),
        "latest_chromaprint_at": latest,
        "counted_at": counted_at,
    }
    current = (
        values["generation"],
        values["coverage_server_id"],
        values["eligible_track_count"],
        values["mapped_track_count"],
        values["fingerprinted_track_count"],
        values["latest_chromaprint_at"],
    )
    if row_exists and tuple(stored) == current:
        return wrote
    _write_coverage(cur, catalog_instance_id, bool(row_exists), values)
    return True


def _rollback(db):
    rollback = getattr(db, "rollback", None)
    if callable(rollback):
        rollback()


def refresh_status_summary(db, catalog_instance_id, adapter=None):
    """After a catalogue refresh or projection committed: update its summaries.

    Runs in its own short transaction, after the caller's commit, so no
    catalogue or projection lock is held while counting. Only AudioMuse 3
    readiness reads the summaries (coverage needs its per-server mapping and
    Chromaprint tables), so on AudioMuse 2 this does nothing. Best effort: a
    failure leaves the previous summary, which readers then ignore for a newer
    generation, and the watchdog tick retries.
    """
    if getattr(adapter, "mode", None) != "v3_registry":
        return False
    try:
        cur = db.cursor()
        try:
            wrote = _refresh_summaries(cur, catalog_instance_id, True)
        finally:
            cur.close()
        if wrote:
            db.commit()
        else:
            _rollback(db)
        return wrote
    except Exception:
        try:
            _rollback(db)
        except Exception:
            pass
        logger.warning(
            "lumae_analysis could not refresh the status summary of %s",
            catalog_instance_id,
            exc_info=True,
        )
        return False


def backfill_publication_summaries(cur, commit=None):
    """Summarize what is published but not summarized (install, start, watchdog).

    A cheap lookup when every summary is current; nothing is written then.
    Coverage is counted when the AudioMuse 3 mapping and Chromaprint tables
    exist (no core adapter is bound at install time), and only for active
    sources, the ones readiness evaluates. ``commit``, when given, is called
    after each source that was written, so its ``analysis_state`` row lock is
    not held while the next source is counted; the install passes none and
    commits with the rest of the migration.
    """
    cur.execute(
        "SELECT to_regclass('track_server_map') IS NOT NULL "
        "AND to_regclass('chromaprint') IS NOT NULL"
    )
    row = cur.fetchone()
    with_coverage = bool(row and row[0] is True)
    cur.execute(
        f"""
        SELECT s.catalog_instance_id
          FROM {t('catalog_sources')} s
          JOIN {t('catalog_state')} c USING (catalog_instance_id)
          LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
          LEFT JOIN {t('status_summary')} ss USING (catalog_instance_id)
         WHERE a.summary_generation IS DISTINCT FROM a.projection_generation
            OR (%s AND s.rebind_status='active'
                AND s.current_core_server_id IS NOT NULL
                AND c.published_generation > 0
                AND (ss.coverage_generation IS DISTINCT FROM c.published_generation
                     OR ss.coverage_server_id IS DISTINCT FROM s.current_core_server_id))
         ORDER BY s.catalog_instance_id
        """,
        (with_coverage,),
    )
    written = 0
    for (catalog_instance_id,) in cur.fetchall():
        if _refresh_summaries(cur, catalog_instance_id, with_coverage):
            written += 1
            if commit is not None:
                commit()
    return written


def store_profile_counts(cur, catalog_instance_id, counts_sql, params):
    """Snapshot the waveform profile counts of one source.

    ``counts_sql`` is the live ``analysis_status_counts`` query for the source.
    It returns total, ready, pending, failed, skipped and the catalogue
    generation counted, or a NULL generation when the source is not an active,
    complete catalogue (nothing is stored then). Snapshots are ordered by the
    time their statement started, so a slower concurrent refresh never
    replaces a newer one.
    """
    columns = ", ".join(_PROFILE_COLUMNS)
    assignments = ", ".join(f"{name}=EXCLUDED.{name}" for name in _PROFILE_COLUMNS)
    cur.execute(
        f"""
        WITH counts AS ({counts_sql})
        INSERT INTO {t('status_summary')} AS s
            (catalog_instance_id, profile_generation, {columns}, profile_counted_at)
        SELECT %s, c.generation, c.total, c.ready, c.pending, c.failed, c.skipped,
               statement_timestamp()
          FROM counts AS c(total, ready, pending, failed, skipped, generation)
         WHERE c.generation IS NOT NULL
        ON CONFLICT (catalog_instance_id) DO UPDATE
           SET profile_generation=EXCLUDED.profile_generation, {assignments},
               profile_counted_at=EXCLUDED.profile_counted_at
         WHERE s.profile_counted_at IS NULL
            OR s.profile_counted_at <= EXCLUDED.profile_counted_at
        """,
        (*params, catalog_instance_id),
    )


def profile_counts_age_seconds(cur, catalog_instance_id):
    cur.execute(
        f"SELECT EXTRACT(EPOCH FROM statement_timestamp() - profile_counted_at) "
        f"FROM {t('status_summary')} WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


# ---- readers (GET routes) -------------------------------------------------


def read_summary(db, source):
    """Return the committed summaries that describe the source's published state.

    ``coverage`` is ``(eligible, mapped, fingerprinted, latest_chromaprint_at)``,
    ``links`` is ``(links, ready, pending, suspect, missing, evidence_complete)``
    and ``profiles`` the ``analysis_status_counts`` dict plus ``counted_at``.
    Each is None when no summary describes the current generation (coverage:
    and server). One primary-key read.
    """
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT a.summary_generation, a.{', a.'.join(_LINK_COUNT_COLUMNS)},
                   ss.coverage_generation, ss.coverage_server_id,
                   ss.{', ss.'.join(_COVERAGE_COLUMNS)},
                   ss.profile_generation, ss.{', ss.'.join(_PROFILE_COLUMNS)},
                   ss.profile_counted_at
              FROM {t('catalog_sources')} src
              LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
              LEFT JOIN {t('status_summary')} ss USING (catalog_instance_id)
             WHERE src.catalog_instance_id=%s
            """,
            (source["catalog_instance_id"],),
        )
        row = cur.fetchone()
    finally:
        cur.close()
    summary = {"links": None, "coverage": None, "profiles": None}
    analysis_generation = int((source.get("analysis") or {}).get("generation", 0) or 0)
    if analysis_generation == 0:
        # Nothing is projected yet; projection generations start at 1.
        summary["links"] = (0,) * len(_LINK_COUNT_COLUMNS)
    if row is None:
        return summary
    catalog_generation = (source.get("catalog") or {}).get("generation")
    if row[0] is not None and int(row[0]) == analysis_generation:
        summary["links"] = tuple(int(value or 0) for value in row[1:7])
    if (
        row[7] is not None
        and catalog_generation is not None
        and int(row[7]) == int(catalog_generation)
        and row[8] is not None
        and str(row[8]) == str(source.get("server_id"))
    ):
        summary["coverage"] = (*(int(value or 0) for value in row[9:12]), row[12])
    if (
        row[13] is not None
        and catalog_generation is not None
        and int(row[13]) == int(catalog_generation)
    ):
        total, ready, pending, failed, skipped = (int(value or 0) for value in row[14:19])
        summary["profiles"] = {
            "total_with_files": total,
            "ready_current": ready,
            "pending": pending,
            "failed": failed,
            "skipped": skipped,
            "needs_analysis": max(0, total - ready - pending - failed - skipped),
            "counted_at": row[19],
        }
    return summary


def _live_row(db, sql, params):
    cur = db.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchone()
    finally:
        cur.close()


def coverage_counts(db, source, summary):
    """Eligible, mapped, fingerprinted and latest Chromaprint time for the source."""
    if summary.get("coverage") is not None:
        return summary["coverage"]
    # No summary for this generation and server: the same aggregate, read-only.
    row = _live_row(
        db,
        coverage_sql(),
        (
            source["server_id"],
            source["catalog_instance_id"],
            source["catalog"]["generation"],
        ),
    ) or (0, 0, 0, None)
    return (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), row[3])


def link_counts(db, source, summary):
    """Links, ready, pending, suspect, missing and evidence-complete counts."""
    if summary.get("links") is not None:
        return summary["links"]
    row = _live_row(
        db,
        link_counts_sql(),
        (
            source["catalog_instance_id"],
            (source.get("analysis") or {}).get("generation", 0),
        ),
    ) or (0,) * 6
    return tuple(int(value or 0) for value in row[:6])


def profile_counts(db, source):
    """The committed waveform counts for the source, or None to count live.

    None when the source is not an active, complete catalogue (the live count
    is then an empty join) or when no snapshot describes its generation.
    """
    catalog = source.get("catalog") or {}
    if source.get("rebind_status") != "active" or catalog.get("status") != "complete":
        return None
    return read_summary(db, source)["profiles"]
