"""Review fixes in edge storage, on a migrated schema.

* A job that failed EDGE_FAILURE_CAP times for the same media under the same
  analyzer is no longer retried every six hours, by the library pass or a
  request, until its media or the analyzer changes.
* A request reads two members of a stored edge, never the payload, and
  still tells a current edge from an old one.
* The settings poll reports the latest edge in real UTC and counts jobs from
  the source's partial indexes.
"""
import json

from test_lumae_analysis import load_plugin  # noqa: F401  (installs the host stubs)
from plugins.LumaeAnalysis import edge_profile_store as store  # noqa: E402
from plugins.LumaeAnalysis.edge_profiles import METHOD, SCHEMA_VERSION, opaque_revision  # noqa: E402

P = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
TRACK = "track-a"
SIGNATURE = "catalog-media:media-1"


def _q(db, sql, params=()):
    cur = db.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall() if cur.description else []
    cur.close()
    db.commit()
    return rows


def _seed(db, signature=SIGNATURE):
    _q(db, f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
           "provider_type, server_name, is_default, rebind_status) "
           "VALUES (%s, 'server-a', 'navidrome', 'server', TRUE, 'active')", (SOURCE,))
    _q(db, f"INSERT INTO {P}published_source_profiles (catalog_instance_id, track_id, sample_rate, "
           "duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, "
           "media_signature, analyzed_at) "
           "VALUES (%s, %s, 44100, 240000, -14, '\\x00', '\\x00', 1, 1, %s, now())",
       (SOURCE, TRACK, signature))


def _fail_once(db, reason="edge-analysis-error"):
    """Claim the track (a due job), start it and fail it; returns the job."""
    _q(db, f"UPDATE {P}edge_profile_jobs SET updated_at=now() - interval '7 hours'")
    jobs, ready, _pending, _unsupported = store.claim_edge_requests(db, SOURCE, [TRACK])
    assert ready == [] and len(jobs) == 1
    job = jobs[0]
    assert store.update_edge_job(db, SOURCE, job, "running")
    assert store.fail_edge_job(db, SOURCE, job, reason) == "failed"
    return job


def _due(db):
    return store.edge_backfill_candidates(db, SOURCE)


def test_failures_of_the_same_media_stop_at_the_cap(migrated_db):
    db = migrated_db
    _seed(db)
    for count in range(1, store.EDGE_FAILURE_CAP + 1):
        _fail_once(db)
        assert _q(db, f"SELECT failures, failure_analyzer FROM {P}edge_profile_jobs") == [
            (count, store.EDGE_FAILURE_ANALYZER)]
        _q(db, f"UPDATE {P}edge_profile_jobs SET updated_at=now() - interval '7 hours'")
        assert _due(db) == ([] if count == store.EDGE_FAILURE_CAP else [TRACK])
    # Capped: six hours later neither the pass nor a request takes it again.
    assert store.claim_edge_requests(db, SOURCE, [TRACK]) == ([], [], [], [])
    assert store.claim_edge_requests(db, SOURCE, [TRACK], "interactive")[0] == []
    assert _q(db, f"SELECT status FROM {P}edge_profile_jobs") == [("failed",)]

    # Another analyzer (or schema): due again, and its failures count afresh.
    _q(db, f"UPDATE {P}edge_profile_jobs SET failure_analyzer='2/lumae-edge-v1/old'")
    assert _due(db) == [TRACK]
    _fail_once(db)
    assert _q(db, f"SELECT failures FROM {P}edge_profile_jobs") == [(1,)]


def test_new_media_resets_a_capped_job(migrated_db):
    db = migrated_db
    _seed(db)
    for _ in range(store.EDGE_FAILURE_CAP):
        _fail_once(db)
    _q(db, f"UPDATE {P}edge_profile_jobs SET updated_at=now() - interval '7 hours'")
    assert _due(db) == []
    _q(db, f"UPDATE {P}published_source_profiles SET media_signature='catalog-media:media-2'")
    assert _due(db) == [TRACK]
    jobs, _ready, _pending, _unsupported = store.claim_edge_requests(db, SOURCE, [TRACK])
    assert [job["media_revision"] for job in jobs] == [opaque_revision("catalog-media:media-2")]
    assert _q(db, f"SELECT status, failures FROM {P}edge_profile_jobs") == [("pending", 0)]


def test_the_due_query_hashes_the_signature_as_opaque_revision(migrated_db):
    db = migrated_db
    signature = "catalog-media:ünïcode/päth:1:2"
    _seed(db, signature)
    (row,) = _q(db, "SELECT 'sha256:' || encode(sha256(convert_to(%s::text, 'UTF8')), 'hex')", (signature,))
    assert row[0] == opaque_revision(signature)


def test_enqueue_refusals_and_successes_do_not_count(migrated_db):
    db = migrated_db
    _seed(db)
    for _ in range(store.EDGE_FAILURE_CAP - 1):
        _fail_once(db)
    _q(db, f"UPDATE {P}edge_profile_jobs SET updated_at=now() - interval '7 hours'")
    jobs, _ready, _pending, _unsupported = store.claim_edge_requests(db, SOURCE, [TRACK])
    assert store.update_edge_job(db, SOURCE, jobs[0], "failed", "edge-enqueue-failed")
    assert _q(db, f"SELECT failures FROM {P}edge_profile_jobs") == [(store.EDGE_FAILURE_CAP - 1,)]
    assert _due(db) == [TRACK]


def test_a_request_reads_two_members_of_the_edge_not_its_payload(migrated_db):
    db = migrated_db
    _seed(db)
    revision = opaque_revision(SIGNATURE)
    payload = {"schema_version": SCHEMA_VERSION, "measurement": {"method": METHOD}, "head": "x" * 1000}
    _q(db, f"INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision, "
           "representation_id, media_signature, profile_digest, payload) "
           "VALUES (%s, %s, %s, 'rep', %s, 'digest', %s::jsonb)",
       (SOURCE, TRACK, revision, SIGNATURE, json.dumps(payload)))
    statements = []

    class Recording:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            statements.append(sql)
            return self._cur.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    class Db:
        def cursor(self):
            return Recording(db.cursor())

        def commit(self):
            db.commit()

    assert store.claim_edge_requests(Db(), SOURCE, [TRACK]) == ([], [TRACK], [], [])
    assert "e.payload," not in statements[0] and "edge.payload" not in statements[0]
    # An older method is not ready: the request queues it.
    payload["measurement"]["method"] = "lumae-edge-old"
    _q(db, f"UPDATE {P}edge_profiles SET payload=%s::jsonb", (json.dumps(payload),))
    jobs, ready, _pending, _unsupported = store.claim_edge_requests(db, SOURCE, [TRACK])
    assert ready == [] and len(jobs) == 1


def test_the_latest_edge_is_reported_in_utc_whatever_the_session_zone(migrated_db):
    db = migrated_db
    _seed(db)
    _q(db, "SET TIME ZONE 'Asia/Kolkata'")
    _q(db, f"INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision, "
           "representation_id, media_signature, profile_digest, payload, updated_at) "
           "VALUES (%s, %s, 'rev', 'rep', %s, 'digest', '{}', '2026-09-03 15:30')",
       (SOURCE, TRACK, SIGNATURE))
    status = store.edge_profile_status(db, SOURCE)
    assert status["last_success_at"] == "2026-09-03T10:00:00Z"
    assert status["ready"] == 1


def test_the_status_job_counts_read_the_source_partial_indexes(migrated_db):
    db = migrated_db
    _seed(db)
    for track, status in (("t1", "pending"), ("t2", "running"), ("t3", "failed"), ("t4", "ready")):
        _q(db, f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, media_revision, "
               "job_token, status, last_error) VALUES (%s, %s, 'rev', 'tok', %s, %s)",
           (SOURCE, track, status, "err" if status == "failed" else None))
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
            return Recording(db.cursor())

    status = store.edge_profile_status(Db(), SOURCE)
    assert (status["active"], status["failed"], status["last_error"]) == (2, 1, "err")
    [(sql, params)] = statements
    cur = db.cursor()
    cur.execute("SET LOCAL enable_seqscan = off")
    cur.execute("SET LOCAL enable_bitmapscan = off")
    cur.execute("EXPLAIN (COSTS OFF) " + sql, params)
    plan = "\n".join(row[0] for row in cur.fetchall())
    db.rollback()
    assert f"{P}edge_profile_jobs_source_active_idx" in plan, plan
    assert f"{P}edge_profile_jobs_source_failed_idx" in plan, plan
    assert f"{P}edge_profile_jobs_pkey" not in plan, plan
