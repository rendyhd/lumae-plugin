"""Bounded retention of optional measurements for tracks absent from a publication.

An orphan gets a 30-day grace period starting when absence is first observed.
Active jobs are preserved. A returning track clears the marker. Catalogue
bootstrap/history leases are unaffected: this only touches optional results.
"""

from plugin.api import table

TABLES = (
    "edge_profiles",
    "edge_profile_jobs",
    "dj_analyses",
    "dj_analysis_jobs",
    "dj_analyses_v3",
    "dj_analysis_jobs_v3",
)


def migrate(db):
    with db.cursor() as cur:
        for name in TABLES:
            cur.execute(
                f"ALTER TABLE {table(name)} ADD COLUMN IF NOT EXISTS orphaned_at TIMESTAMPTZ"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS {table(name)}_orphan_idx ON {table(name)}(orphaned_at) WHERE orphaned_at IS NOT NULL"
            )


def prune(db, limit=1000):
    limit = max(1, min(1000, int(limit)))
    counts = {}
    with db.cursor() as cur:
        for name in TABLES:
            target = table(name)
            live = f"""EXISTS(SELECT 1 FROM {table('catalog_tracks')} track
                JOIN {table('catalog_state')} state ON state.catalog_instance_id=track.catalog_instance_id
                  AND state.published_generation=track.published_generation
                WHERE track.catalog_instance_id=value.catalog_instance_id AND track.track_id=value.track_id
                  AND track.available=TRUE)"""
            published = f"""EXISTS(SELECT 1 FROM {table('catalog_state')} state
                WHERE state.catalog_instance_id=value.catalog_instance_id AND state.published_generation>0)"""
            active = (
                "AND value.status NOT IN ('pending','running')"
                if name.endswith("jobs")
                else ""
            )
            for condition, assignment in (
                (f"value.orphaned_at IS NULL AND {published} AND NOT {live}", "now()"),
                (f"value.orphaned_at IS NOT NULL AND {live}", "NULL"),
            ):
                cur.execute(
                    f"""WITH selected AS (SELECT value.ctid FROM {target} value
                    WHERE {condition} LIMIT %s) UPDATE {target} value SET orphaned_at={assignment}
                    FROM selected WHERE value.ctid=selected.ctid""",
                    (limit,),
                )
            cur.execute(
                f"""WITH selected AS (SELECT value.ctid FROM {target} value
                WHERE value.orphaned_at<now()-interval '30 days' AND NOT {live} {active} LIMIT %s)
                DELETE FROM {target} value USING selected WHERE value.ctid=selected.ctid""",
                (limit,),
            )
            counts[name] = cur.rowcount
    return counts
