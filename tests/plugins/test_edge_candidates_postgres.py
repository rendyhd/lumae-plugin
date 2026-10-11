"""AUD-14: the edge library pass's candidate query (plan S3).

It left-joined the newest edge per track and the edge job, filtered for no
edge, then sorted. Postgres estimated one row for the filter, read every
published track, and seq-scanned ``edge_profile_jobs`` for each track without
an edge (615 ms on a 36k-track production library; the reporter's 8-minute
ticks). Two ``NOT EXISTS`` anti-joins with the same meaning let the pass walk
the primary key from its cursor and stop at its batch.
"""
import json
import time
from itertools import product


from test_lumae_analysis import load_plugin  # noqa: F401  (installs the host stubs)
from plugins.LumaeAnalysis import edge_profile_store as store  # noqa: E402


P = "plugin_lumae_analysis__"
SOURCE = "catalog-a"
OTHER = "catalog-b"

# The 1.6.1 query, kept as the reference meaning.
LEGACY = f"""SELECT p.track_id FROM {P}published_source_profiles p
    LEFT JOIN LATERAL (
        SELECT e.payload FROM {P}edge_profiles e
         WHERE e.catalog_instance_id=p.catalog_instance_id
           AND e.track_id=p.track_id
           AND e.media_signature=p.media_signature
         ORDER BY e.updated_at DESC, e.profile_digest LIMIT 1
    ) edge ON TRUE
    LEFT JOIN {P}edge_profile_jobs j
      ON j.catalog_instance_id=p.catalog_instance_id AND j.track_id=p.track_id
    WHERE p.catalog_instance_id=%s AND p.media_signature IS NOT NULL AND edge.payload IS NULL
      AND (j.track_id IS NULL OR j.updated_at < now()-interval '6 hours'
           OR (j.status='failed' AND j.last_error='edge-enqueue-failed'))"""

EDGES = ("none", "other_media", "current")
# (status, last_error, hours since the job's last update)
JOBS = (
    None,
    ("pending", None, 0),
    ("running", None, 7),
    ("pending", None, 7),
    ("failed", "edge-enqueue-failed", 0),
    ("failed", "edge-enqueue-failed", 7),
    ("failed", None, 0),
    ("failed", None, 7),
    ("failed", "decode", 0),
    ("unsupported", "no audio stream", 0),
)


def _sources(cur, *sources):
    for source in sources:
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
            "provider_type, server_name, is_default, rebind_status) "
            "VALUES (%s, %s, 'navidrome', 'server', %s, 'active')",
            (source, f"server-{source}", source == SOURCE),
        )


def _published(cur, source, track, signature):
    cur.execute(
        f"INSERT INTO {P}published_source_profiles (catalog_instance_id, track_id, sample_rate, "
        "duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, "
        "media_signature, analyzed_at) "
        "VALUES (%s, %s, 44100, 240000, -14, '\\x00', '\\x00', 1, 1, %s, now())",
        (source, track, signature),
    )


def _edge(cur, source, track, signature, revision="rev"):
    cur.execute(
        f"INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision, "
        "representation_id, media_signature, profile_digest, payload) "
        "VALUES (%s, %s, %s, 'rep', %s, 'digest', '{}')",
        (source, track, revision, signature),
    )


def _job(cur, source, track, status, error, hours):
    cur.execute(
        f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, media_revision, "
        "job_token, status, last_error, updated_at) "
        "VALUES (%s, %s, 'rev', 'tok', %s, %s, now()-make_interval(hours => %s))",
        (source, track, status, error, hours),
    )


def _every_combination(db):
    """One published track per (edge, job) combination, plus a track without
    media and the same tracks in another source."""
    tracks = []
    with db.cursor() as cur:
        _sources(cur, SOURCE, OTHER)
        for index, (edge, job) in enumerate(product(EDGES, JOBS)):
            track = f"t{index:03d}"
            tracks.append(track)
            signature = f"sig-{track}"
            _published(cur, SOURCE, track, signature)
            _published(cur, OTHER, track, signature)
            if edge == "current":
                _edge(cur, SOURCE, track, signature)
            elif edge == "other_media":
                _edge(cur, SOURCE, track, "sig-older", revision="older")
            if job:
                _job(cur, SOURCE, track, *job)
        _published(cur, SOURCE, "t-no-media", None)
        tracks.append("t-no-media")
    db.commit()
    return tracks


def _legacy(db, after="", limit=100):
    with db.cursor() as cur:
        cur.execute(LEGACY + " AND p.track_id>%s ORDER BY p.track_id LIMIT %s",
                    (SOURCE, after, limit))
        ids = [row[0] for row in cur.fetchall()]
    db.commit()
    return ids


def test_candidates_match_the_legacy_query_for_every_edge_and_job_state(migrated_db):
    tracks = _every_combination(migrated_db)
    expected = _legacy(migrated_db)
    # The fixture covers both outcomes for every edge and job state.
    assert 0 < len(expected) < len(tracks)
    assert store.edge_backfill_candidates(migrated_db, SOURCE) == expected
    for after, limit in product([""] + tracks, (1, 3, 100)):
        assert (store.edge_backfill_candidates(migrated_db, SOURCE, after, limit)
                == _legacy(migrated_db, after, limit)), (after, limit)
    # The other source has no edges and no jobs: every track with media.
    assert store.edge_backfill_candidates(migrated_db, OTHER) == tracks[:-1]


def test_the_song_hook_rewind_reads_the_same_candidates(migrated_db):
    """rewind_edge_backfill shares the candidate condition."""
    tracks = _every_combination(migrated_db)
    expected = set(_legacy(migrated_db))
    with migrated_db.cursor() as cur:
        for track in tracks:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {store._edge_due_sql()} "
                        "AND p.track_id=%s)", (SOURCE, track))
            assert cur.fetchone()[0] is (track in expected), track
    migrated_db.rollback()


def _library(db, tracks, *, edges, jobs):
    """``tracks`` published; edges for the first ``edges``; recent failed jobs
    (not candidates) for the ``jobs`` tracks after them."""
    with db.cursor() as cur:
        _sources(cur, SOURCE)
        cur.execute(
            f"""INSERT INTO {P}published_source_profiles (catalog_instance_id, track_id,
                    sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver,
                    profile_schema_ver, media_signature, analyzed_at)
                SELECT %s, 't' || lpad(i::text, 6, '0'), 44100, 240000, -14, '\\x00', '\\x00',
                       1, 1, 'sig-' || i, now()
                  FROM generate_series(1, %s) i""",
            (SOURCE, tracks),
        )
        cur.execute(
            f"""INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision,
                    representation_id, media_signature, profile_digest, payload)
                SELECT %s, 't' || lpad(i::text, 6, '0'), 'rev', 'rep', 'sig-' || i,
                       'digest', '{{}}'
                  FROM generate_series(1, %s) i""",
            (SOURCE, edges),
        )
        cur.execute(
            f"""INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id,
                    media_revision, job_token, status, last_error)
                SELECT %s, 't' || lpad(i::text, 6, '0'), 'rev', 'tok', 'failed', 'decode'
                  FROM generate_series(%s, %s) i""",
            (SOURCE, edges + 1, edges + jobs),
        )
    db.commit()
    # Statistics and a visibility map, as autovacuum keeps them on a library.
    db.autocommit = True
    try:
        with db.cursor() as cur:
            for name in ("published_source_profiles", "edge_profiles", "edge_profile_jobs"):
                cur.execute(f"VACUUM ANALYZE {P}{name}")
    finally:
        db.autocommit = False


def _explained(db, call):
    """The plan of the one statement ``call`` runs, explained instead of run."""
    plans = []

    class Explaining:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            self._cur.execute("EXPLAIN (FORMAT JSON) " + sql, params)
            raw = self._cur.fetchone()[0]
            plans.append((raw if isinstance(raw, list) else json.loads(raw))[0]["Plan"])

        def fetchall(self):
            return []

        def __getattr__(self, name):
            return getattr(self._cur, name)

    class Db:
        def cursor(self):
            return Explaining(db.cursor())

        def __getattr__(self, name):
            return getattr(db, name)

    call(Db())
    db.rollback()
    [plan] = plans
    return plan


def _nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from _nodes(child)


def _driving_scan(plan):
    while plan.get("Plans"):
        plan = plan["Plans"][0]
    return plan


def test_candidate_plan_walks_the_primary_key_and_probes_jobs_by_key(migrated_db):
    """20k published tracks, 15k jobs, the pass mid-library."""
    _library(migrated_db, 20_000, edges=5_000, jobs=15_000)
    plan = _explained(
        migrated_db, lambda db: store.edge_backfill_candidates(db, SOURCE, "", 25))
    nodes = list(_nodes(plan))
    text = json.dumps(plan)
    assert not [node for node in nodes
                if node["Node Type"] == "Seq Scan"
                and node.get("Relation Name") == f"{P}edge_profile_jobs"], text
    assert not [node for node in nodes if node["Node Type"] in ("Sort", "Incremental Sort")], text
    assert plan["Node Type"] == "Limit", text
    driving = _driving_scan(plan)
    assert driving["Node Type"] in ("Index Scan", "Index Only Scan"), text
    assert driving["Relation Name"] == f"{P}published_source_profiles", text
    assert driving["Index Name"] == f"{P}published_source_profiles_pkey", text


def test_candidates_take_under_a_second_at_the_perf_fixture_scale(migrated_db):
    """The perf fixture's 94k published profiles with edges, the last 2k in
    back-off: a re-sweep from the start reads the whole library."""
    _library(migrated_db, 94_000, edges=92_000, jobs=2_000)
    with migrated_db.cursor() as cur:
        # The legacy query can take minutes on a misestimate; fail instead.
        cur.execute("SET LOCAL statement_timeout = '10s'")
    started = time.monotonic()
    assert store.edge_backfill_candidates(migrated_db, SOURCE, "", 25) == []
    elapsed = time.monotonic() - started
    migrated_db.commit()
    assert elapsed < 1.0, f"{elapsed:.2f} s"
