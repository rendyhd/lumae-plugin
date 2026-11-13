"""Relationship build review fixes, on the real migrated schema.

* A build without published inputs waits (``waiting_for_inputs``, a retry time)
  instead of reporting a checkpoint the runner re-runs at once, and a
  publication queues it again.
* A rebuild claimed right after a publication commits is not overwritten.
* Fingerprint steps read only the staged tracks they still need, with the same
  published result as one long step.
* The relationship input query reads the two ``_lumae`` members it uses.
* Re-running the enrichment migration neither waits on nor rewrites current
  stream-state and relationship-state rows.
"""

import numpy as np
import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import catalog_enrichment as e, relationship_build as rb


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"


def _source(db, catalog_status="complete"):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " server_name, is_default, rebind_status) "
            "VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')",
            (SOURCE,),
        )
        cur.execute(
            f"INSERT INTO {P}catalog_state "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, 'server-a', 'navidrome', 1, 'catalog-epoch', %s, '{}'::jsonb)",
            (SOURCE, catalog_status),
        )
        cur.execute(
            f"INSERT INTO {P}analysis_state "
            "(catalog_instance_id, projection_generation, analysis_epoch, status) "
            "VALUES (%s, 1, 'analysis-epoch', 'complete')",
            (SOURCE,),
        )
        cur.execute(
            f"INSERT INTO {P}relationship_state "
            "(catalog_instance_id, relationship_schema_version, algorithm_version, "
            " scoring_revision, epoch, status) "
            "VALUES (%s, %s, %s, %s, 'relationship-epoch', 'queued')",
            (SOURCE, e.RELATIONSHIP_SCHEMA_VERSION, e.RELATIONSHIP_ALGORITHM_VERSION,
             e.RELATIONSHIP_SCORING_REVISION),
        )
        e._profile_stream_state(cur, SOURCE)
    db.commit()


def _library(db, tracks=12, artists=3, payload='{"_lumae":{"year":2000}}'):
    vector = np.linspace(.01, 1, 200, dtype='<f4')
    with db.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {P}catalog_artists
                (catalog_instance_id, published_generation, artist_id, name,
                 cover_art_id, metadata_fp, payload, first_seen_at, last_seen_at)
            SELECT %s, 1, 'ar-'||i, 'Artist '||i, 'cover-'||i, '', '{{}}', now(), now()
              FROM generate_series(0, %s-1) i;
            INSERT INTO {P}catalog_albums
                (catalog_instance_id, published_generation, album_id, name, album_artist_display,
                 metadata_fp, payload, first_seen_at, last_seen_at)
            SELECT %s, 1, 'al-'||i, 'Album '||i, 'Artist '||(i %% %s), '', '{{}}', now(), now()
              FROM generate_series(0, %s-1) i;
            INSERT INTO {P}catalog_tracks
                (catalog_instance_id, published_generation, track_id, album_id, title,
                 artist_display, disc_number, track_number, analysis_eligible, metadata_fp,
                 payload, first_seen_at, last_seen_at)
            SELECT %s, 1, 'tr-'||i, 'al-'||(i/4), 'Track '||i,
                   'Artist '||((i/4) %% %s), 1, i%%4, TRUE, '', %s::jsonb, now(), now()
              FROM generate_series(0, %s-1) i;
            INSERT INTO {P}analysis_items
                (catalog_instance_id, projection_generation, analysis_id,
                 scalar_payload, musicnn_vector, musicnn_dimensions)
            SELECT %s, 1, 'ai-'||i, jsonb_build_object('energy', 0.05 + i * 0.01),
                   %s, 200
              FROM generate_series(0, %s-1) i;
            INSERT INTO {P}track_analysis_links
                (catalog_instance_id, projection_generation, provider_track_id, analysis_id, status)
            SELECT %s, 1, 'tr-'||i, 'ai-'||i, 'ready'
              FROM generate_series(0, %s-1) i;
        """, (SOURCE, artists, SOURCE, artists, (tracks + 3) // 4,
              SOURCE, artists, payload, tracks,
              SOURCE, vector.tobytes(), tracks, SOURCE, tracks))
    db.commit()


def _state(db):
    with db.cursor() as cur:
        cur.execute(f"SELECT status, next_retry_at > now() + interval '4 minutes', "
                    f"next_retry_at IS NULL FROM {P}relationship_state "
                    "WHERE catalog_instance_id=%s", (SOURCE,))
        row = cur.fetchone()
    db.commit()
    return row


def _published(db):
    with db.cursor() as cur:
        cur.execute(f"SELECT entity_type, entity_id, result_fp, payload FROM {P}relationship_results "
                    "WHERE catalog_instance_id=%s ORDER BY entity_type, entity_id", (SOURCE,))
        rows = cur.fetchall()
    db.commit()
    return rows


def _lookup(*_):
    return ['ai-0', 'ai-4', 'ai-8']


def _advance(db, **kwargs):
    return e.prepare_relationships(SOURCE, db=db, candidate_lookup=kwargs.pop('candidate_lookup', _lookup),
                                   time_budget_seconds=60, **kwargs)


def _finish(db, **kwargs):
    for _ in range(200):
        result = _advance(db, **kwargs)
        if result['status'] == 'complete':
            return result
        assert result['status'] == 'queued', result
    pytest.fail('build did not finish')


# ------------------------------------------------------------ finding 1


def test_a_build_without_published_inputs_waits_until_a_publication_queues_it(migrated_db):
    db = migrated_db
    _source(db, catalog_status='running')
    result = _advance(db)
    assert result['status'] == 'waiting_for_inputs'
    assert result['catalog_instance_id'] == SOURCE and result['reason']
    # Not 'queued' (which the runner re-runs at once) and not due for 5 minutes.
    assert _state(db) == ('waiting_for_inputs', True, False)
    # A publication's claim queues it again.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_state SET status='complete' WHERE catalog_instance_id=%s",
                    (SOURCE,))
    db.commit()
    assert e.claim_relationship_preparation(db, SOURCE) is True
    assert _state(db)[0] == 'queued'


def test_inputs_that_stop_being_published_mid_build_still_restart_it(migrated_db, second_connection):
    db = migrated_db
    _source(db)
    _library(db)

    def progress(phase, **_):
        if phase == 'Publishing relationship generation':
            with second_connection.cursor() as cur:
                cur.execute(f"UPDATE {P}analysis_state SET status='running'")
            second_connection.commit()

    result = _advance(db, progress=progress)
    assert result['status'] == 'queued'
    assert _state(db)[0] == 'queued'


# ------------------------------------------------------------ finding 4


def test_a_rebuild_claimed_right_after_the_publication_is_kept(migrated_db, second_connection, monkeypatch):
    db = migrated_db
    _source(db)
    _library(db)
    first = _finish(db)
    assert _state(db) == ('complete', None, True)
    # A second build, published at the minimum gap: a claim lands between the
    # publication's commit and the end of the step.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}analysis_state SET analysis_epoch='analysis-epoch-2'")
    db.commit()
    original = rb.Build.checkpoint
    claimed = []

    def checkpoint(self, cur, phase=None):
        original(self, cur, phase)
        if phase == 'complete' and not claimed:
            claimed.append(e.claim_relationship_preparation(second_connection, SOURCE))

    monkeypatch.setattr(rb.Build, 'checkpoint', checkpoint)
    second = _finish(db)
    assert second['generation'] == first['generation'] + 1
    assert claimed == [True]
    status, _later, no_retry = _state(db)
    assert status == 'queued' and not no_retry


# ------------------------------------------------------------ finding 2


def test_fingerprint_steps_read_only_what_they_need_with_the_same_result(migrated_db):
    db = migrated_db
    _source(db)
    _library(db, tracks=24, artists=4)
    _finish(db)
    whole = _published(db)
    # The same inputs, rebuilt one entity per step (a new epoch restarts it).
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}analysis_state SET analysis_epoch='analysis-epoch-2'")
    db.commit()
    _finish(db, batch_size=1)
    assert [row[:3] for row in _published(db)] == [row[:3] for row in whole]


def test_staged_tracks_have_an_input_order_index(migrated_db):
    with migrated_db.cursor() as cur:
        cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname=%s",
                    (f"{P}relationship_build_tracks_order_idx",))
        (definition,) = cur.fetchone()
    assert "(catalog_instance_id, input_order)" in definition


# ------------------------------------------------------------ finding 3


def test_input_query_reads_only_the_two_lumae_members(migrated_db):
    db = migrated_db
    _source(db)
    _library(db, tracks=4, artists=1,
             payload='{"_lumae":{"album":"From payload","year":1999,"genres":["x"]},'
                     '"provider":{"large":"blob"}}')
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET album_id=NULL WHERE track_id='tr-0'")
        cur.execute(f"UPDATE {P}catalog_tracks SET payload='{{\"_lumae\":\"text\"}}' "
                    "WHERE track_id='tr-1'")
        tracks = e._load_relationship_inputs(cur, {
            "catalog_instance_id": SOURCE, "catalog": {"generation": 1},
            "analysis": {"generation": 1}})
    db.rollback()
    by_id = {track['id']: track for track in tracks}
    assert by_id['tr-0']['album'] == 'From payload' and by_id['tr-0']['year'] == 1999
    assert by_id['tr-1']['album'] == 'Album 0' and by_id['tr-1']['year'] is None
    assert by_id['tr-2']['year'] == 1999


# ------------------------------------------------------------ finding 5


def test_rerun_enrichment_migration_neither_waits_on_nor_rewrites_current_rows(
        migrated_db, second_connection):
    db = migrated_db
    _source(db)
    with db.cursor() as cur:
        cur.execute(f"SELECT xmin::text FROM {P}relationship_state WHERE catalog_instance_id=%s",
                    (SOURCE,))
        before = cur.fetchone()[0]
    db.commit()
    # A publisher holding both rows: the migration must not queue behind it.
    with second_connection.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {P}profile_stream_state WHERE catalog_instance_id=%s FOR UPDATE",
                    (SOURCE,))
        cur.execute(f"SELECT 1 FROM {P}relationship_state WHERE catalog_instance_id=%s FOR UPDATE",
                    (SOURCE,))
    try:
        with db.cursor() as cur:
            cur.execute("SET lock_timeout = '2s'")
        e.migrate_enrichment(db)
        db.commit()
    finally:
        second_connection.rollback()
        with db.cursor() as cur:
            cur.execute("RESET lock_timeout")
        db.commit()
    with db.cursor() as cur:
        cur.execute(f"SELECT xmin::text, status FROM {P}relationship_state "
                    "WHERE catalog_instance_id=%s", (SOURCE,))
        assert cur.fetchone() == (before, 'queued')
    db.commit()


def test_enrichment_migration_still_marks_older_results_stale(migrated_db):
    db = migrated_db
    _source(db)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}relationship_state SET algorithm_version=algorithm_version-1, "
                    "result_generation=3 WHERE catalog_instance_id=%s", (SOURCE,))
        cur.execute(f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
                    "provider_type, server_name, is_default, rebind_status) "
                    "VALUES ('catalog-b', 'server-b', 'navidrome', 'B', FALSE, 'active')")
    db.commit()
    e.migrate_enrichment(db)
    db.commit()
    with db.cursor() as cur:
        cur.execute(f"SELECT catalog_instance_id, status, algorithm_version FROM {P}relationship_state "
                    "ORDER BY catalog_instance_id")
        rows = cur.fetchall()
        cur.execute(f"SELECT count(*) FROM {P}profile_stream_state WHERE catalog_instance_id='catalog-b'")
        streams = cur.fetchone()[0]
    db.commit()
    assert rows == [(SOURCE, 'stale', e.RELATIONSHIP_ALGORITHM_VERSION - 1),
                    ('catalog-b', 'not_initialized', e.RELATIONSHIP_ALGORITHM_VERSION)]
    assert streams == 1
