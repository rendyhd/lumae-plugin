"""edge_profile_status on a migrated schema (P3-9 settings poll, Phase 3 exit).

The settings page polls it; at 94k edge profiles the two scans of the wide
``edge_profiles`` table it used to run took about 60 ms per poll. It now
reads one narrow index.
"""

import re

P = "plugin_lumae_analysis__"


def _seed(db):
    with db.cursor() as cur:
        for source in ("src-a", "src-b"):
            cur.execute(
                f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
                "provider_type, server_name, is_default, rebind_status) "
                "VALUES (%s, %s, 'navidrome', 'server', %s, 'active')",
                (source, f"server-{source}", source == "src-a"),
            )
        for source, track, at in (("src-a", "t1", "2026-09-01 10:00"),
                                  ("src-a", "t2", "2026-09-03 10:00"),
                                  ("src-a", "t3", "2026-09-02 10:00"),
                                  ("src-b", "t1", "2026-09-09 10:00")):
            cur.execute(
                f"INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision, "
                "representation_id, media_signature, profile_digest, payload, updated_at) "
                "VALUES (%s, %s, 'rev', 'rep', 'sig', 'digest', '{}', %s)",
                (source, track, at),
            )
        for source, track, status, error, at in (
                ("src-a", "t4", "pending", None, "2026-09-01 10:00"),
                ("src-a", "t5", "running", None, "2026-09-01 10:00"),
                ("src-a", "t6", "failed", "older", "2026-09-01 10:00"),
                ("src-a", "t7", "failed", "newest", "2026-09-04 10:00"),
                ("src-b", "t8", "failed", "other source", "2026-09-09 10:00")):
            cur.execute(
                f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, media_revision, "
                "job_token, status, last_error, updated_at) VALUES (%s, %s, 'rev', 'tok', %s, %s, %s)",
                (source, track, status, error, at),
            )
    db.commit()


def test_edge_status_counts_one_source_and_reads_its_latest_edge(migrated_db):
    from plugins.LumaeAnalysis import edge_profile_store

    _seed(migrated_db)
    assert edge_profile_store.edge_profile_status(migrated_db, "src-a") == {
        "ready": 3, "active": 2, "failed": 2, "last_error": "newest",
        "last_success_at": "2026-09-03T10:00:00",
    }
    with migrated_db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
                    "provider_type, server_name, is_default, rebind_status) "
                    "VALUES ('src-empty', 'server-empty', 'navidrome', 'server', FALSE, 'active')")
    assert edge_profile_store.edge_profile_status(migrated_db, "src-empty") == {
        "ready": 0, "active": 0, "failed": 0, "last_error": None, "last_success_at": None,
    }


def test_edge_status_reads_the_narrow_index_not_the_edge_table(migrated_db):
    from plugins.LumaeAnalysis import edge_profile_store

    _seed(migrated_db)
    statements = []

    class Recording:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            statements.append((sql, params))
            return self._cur.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    class Db:
        def cursor(self):
            return Recording(migrated_db.cursor())

    edge_profile_store.edge_profile_status(Db(), "src-a")
    [(sql, params)] = statements
    with migrated_db.cursor() as cur:
        # A few test rows would be read by any plan; turn off the plans that
        # also scan the table, as at scale.
        cur.execute("SET LOCAL enable_seqscan = off")
        cur.execute("SET LOCAL enable_bitmapscan = off")
        cur.execute("EXPLAIN (COSTS OFF) " + sql, params)
        plan = "\n".join(row[0] for row in cur.fetchall())
    migrated_db.rollback()
    assert f"Index Only Scan using {P}edge_profiles_updated_idx" in plan, plan
    assert len(re.findall(rf"\bon {P}edge_profiles\b", plan)) == 1, plan
