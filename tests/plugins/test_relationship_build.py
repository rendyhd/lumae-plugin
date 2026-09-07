"""Actual PostgreSQL regressions for generation-safe, resumable relationships."""

import json
import os
from pathlib import Path

import numpy as np
import psycopg2
import pytest

from test_lumae_analysis import lumae_postgres_db, load_plugin, readiness_source
from plugins.LumaeAnalysis import catalog, catalog_enrichment as e, relationship_build as rb


@pytest.fixture
def relationship_db(lumae_postgres_db, monkeypatch):
    db = lumae_postgres_db
    catalog.migrate_catalog(db)
    with db.cursor() as cur:
        cur.execute("""
            INSERT INTO plugin_lumae_analysis__catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type, server_name)
            VALUES ('catalog-a', 'server-a', 'navidrome', 'Test source');
            INSERT INTO plugin_lumae_analysis__catalog_state
                (catalog_instance_id, provider_type, current_core_server_id, published_generation, catalog_epoch, status)
            VALUES ('catalog-a', 'navidrome', 'server-a', 1, 'catalog-epoch', 'complete');
            INSERT INTO plugin_lumae_analysis__analysis_state
                (catalog_instance_id, projection_generation, analysis_epoch, status)
            VALUES ('catalog-a', 1, 'analysis-epoch', 'complete');
        """)
    e.migrate_enrichment(db)
    db.commit()
    seed_library(db, tracks=8, artists=2)
    monkeypatch.setattr(e, 'get_db', lambda: db)
    return db


def seed_library(db, tracks, artists):
    with db.cursor() as cur:
        cur.execute("""
            INSERT INTO plugin_lumae_analysis__catalog_artists
                (catalog_instance_id, published_generation, artist_id, name,
                 cover_art_id, metadata_fp, payload, first_seen_at, last_seen_at)
            SELECT 'catalog-a', 1, 'ar-'||i, 'Artist '||i, 'cover-'||i, '', '{}', now(), now()
              FROM generate_series(0, %s-1) i ON CONFLICT DO NOTHING;
            INSERT INTO plugin_lumae_analysis__catalog_albums
                (catalog_instance_id, published_generation, album_id, name, album_artist_display,
                 metadata_fp, payload, first_seen_at, last_seen_at)
            SELECT 'catalog-a', 1, 'al-'||i, 'Album '||i, 'Artist '||(i %% %s), '', '{}', now(), now()
              FROM generate_series(0, %s-1) i ON CONFLICT DO NOTHING;
            INSERT INTO plugin_lumae_analysis__catalog_tracks
                (catalog_instance_id, published_generation, track_id, album_id, title,
                 artist_display, disc_number, track_number, analysis_eligible, metadata_fp,
                 payload, first_seen_at, last_seen_at)
            SELECT 'catalog-a', 1, 'tr-'||i, 'al-'||(i/4), 'Track '||i,
                   'Artist '||((i/4) %% %s), 1, i%%4, TRUE, '', '{"_lumae":{"year":2000}}', now(), now()
              FROM generate_series(0, %s-1) i ON CONFLICT DO NOTHING;
            INSERT INTO plugin_lumae_analysis__analysis_items
                (catalog_instance_id, projection_generation, analysis_id,
                 scalar_payload, musicnn_vector, musicnn_dimensions)
            SELECT 'catalog-a', 1, 'ai-'||i, '{"energy":0.07}', %s, 200
              FROM generate_series(0, %s-1) i ON CONFLICT DO NOTHING;
            INSERT INTO plugin_lumae_analysis__track_analysis_links
                (catalog_instance_id, projection_generation, provider_track_id, analysis_id, status)
            SELECT 'catalog-a', 1, 'tr-'||i, 'ai-'||i, 'ready'
              FROM generate_series(0, %s-1) i ON CONFLICT DO NOTHING;
        """, (artists, artists, (tracks + 3)//4, artists, tracks,
              np.linspace(.01, 1, 200, dtype='<f4').tobytes(), tracks, tracks))
    db.commit()


def connection_like(db):
    with db.cursor() as cur:
        cur.execute('SHOW search_path')
        search_path = cur.fetchone()[0]
    other = psycopg2.connect(os.environ['LUMAE_POSTGRES_TEST_DSN'])
    with other.cursor() as cur:
        cur.execute("SELECT set_config('search_path', %s, false)", (search_path,))
    other.commit()
    return other


def advance(db, **kwargs):
    return e.prepare_relationships('catalog-a', db=db,
                                  candidate_lookup=kwargs.pop('candidate_lookup', lambda *_: ['tr-0', 'tr-4']),
                                  time_budget_seconds=kwargs.pop('time_budget_seconds', 60), **kwargs)


def finish(db, **kwargs):
    for _ in range(100):
        result = advance(db, **kwargs)
        if result['status'] == 'complete':
            return result
        assert result['status'] == 'queued', result
    pytest.fail('build did not finish')


def scalar(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def published(db):
    with db.cursor() as cur:
        cur.execute('SELECT entity_type, entity_id, payload FROM plugin_lumae_analysis__relationship_results '
                    'ORDER BY entity_type, entity_id')
        return cur.fetchall()


def captured_input_query():
    class Capture:
        def execute(self, sql, args):
            self.sql, self.args = sql, args
        def fetchall(self):
            return []
    cur = Capture()
    e._load_relationship_inputs(cur, {'catalog_instance_id': 'catalog-a',
                                     'catalog': {'generation': 1}, 'analysis': {'generation': 1}})
    return cur.sql, cur.args


def test_input_query_preserves_rows_order_and_duplicate_cover_semantics(relationship_db):
    db = relationship_db
    with db.cursor() as cur:
        # Case-insensitive duplicate names must not multiply tracks. A different
        # generation's smaller cover must not leak into the selected generation.
        cur.execute("""
            INSERT INTO plugin_lumae_analysis__catalog_artists
                (catalog_instance_id, published_generation, artist_id, name, cover_art_id,
                 metadata_fp, payload, first_seen_at, last_seen_at)
            VALUES ('catalog-a', 1, 'duplicate', 'ARTIST 0', 'aaa', '', '{}', now(), now()),
                   ('catalog-a', 2, 'duplicate', 'ARTIST 0', '000', '', '{}', now(), now()),
                   ('other-source', 1, 'duplicate', 'ARTIST 0', '000', '', '{}', now(), now());
            UPDATE plugin_lumae_analysis__catalog_tracks SET available=FALSE WHERE track_id='tr-6';
            UPDATE plugin_lumae_analysis__catalog_tracks SET analysis_eligible=FALSE WHERE track_id='tr-7';
            UPDATE plugin_lumae_analysis__catalog_tracks SET album_id=NULL, artist_display='Unmatched'
                WHERE track_id='tr-5';
        """)
        sql, args = captured_input_query()
        cur.execute(sql, args)
        actual = cur.fetchall()
        cur.execute(Path(__file__).with_name('relationship_inputs_legacy.sql').read_text(), (1, 'catalog-a', 1))
        expected = cur.fetchall()
    assert actual == expected
    assert len(actual) == 6
    assert [r[9] for r in actual if r[0] == 'tr-0'] == ['aaa']
    assert [r[9] for r in actual if r[0] == 'tr-5'] == [None]


def test_large_input_query_scans_artist_catalogue_once(relationship_db):
    db = relationship_db
    seed_library(db, tracks=8000, artists=2000)
    with db.cursor() as cur:
        cur.execute('ANALYZE')
        cur.execute("SET LOCAL statement_timeout = '30s'")
        sql, args = captured_input_query()
        cur.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + sql, args)
        actual = cur.fetchone()[0][0]
        cur.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' +
                    Path(__file__).with_name('relationship_inputs_legacy.sql').read_text(), (1, 'catalog-a', 1))
        legacy = cur.fetchone()[0][0]

    def nodes(plan):
        yield plan
        for child in plan.get('Plans', []):
            yield from nodes(child)
    scans = [n for n in nodes(actual['Plan']) if n.get('Relation Name') == 'plugin_lumae_analysis__catalog_artists']
    old_scans = [n for n in nodes(legacy['Plan']) if n.get('Relation Name') == 'plugin_lumae_analysis__catalog_artists']
    assert actual['Plan']['Actual Rows'] == legacy['Plan']['Actual Rows'] == 8000
    assert sum(n['Actual Loops'] for n in scans) == 1
    assert sum(n['Actual Loops'] for n in old_scans) == 8000
    # Assert the work reduction, not a flaky wall-clock speed ratio across CI hosts.
    print(json.dumps({'tracks': 8000, 'artists': 2000, 'new_ms': actual['Execution Time'],
                      'legacy_ms': legacy['Execution Time'], 'new_artist_scans': 1,
                      'legacy_artist_scans': 8000}))


def test_binary_checkpoint_preserves_vectors_and_ranker_output(relationship_db):
    with relationship_db.cursor() as cur:
        tracks = e._load_relationship_inputs(cur, {'catalog_instance_id': 'catalog-a',
                                                  'catalog': {'generation': 1}, 'analysis': {'generation': 1}})
    albums, artists = e._build_entities(tracks)
    for entities, ranker in ((albums, e._rank_albums), (artists, e._rank_artists)):
        restored = [rb.unpack_entity(rb.pack_entity(entity)) for entity in entities]
        assert ranker(entities[0], entities[1:]) == ranker(restored[0], restored[1:])
    blob = rb.pack_entity({'vectors': [np.array([1.123456789], dtype=np.float64)]})
    assert rb.unpack_entity(blob)['vectors'][0].dtype == np.float64
    assert rb.unpack_entity(blob)['vectors'][0][0] == 1.123456789


def test_relationship_builder_publishes_only_bounded_shortlist_candidates(relationship_db):
    calls = []
    def lookup(_vectors, limit):
        calls.append(limit)
        return ['tr-0', 'tr-4', 'not-in-this-source']
    result = finish(relationship_db, batch_size=2, candidate_lookup=lookup)
    assert result['album_count'] == result['artist_count'] == 2
    assert result['track_count'] == 8
    assert result['changes'] == 4
    assert calls == [e.RELATIONSHIP_CANDIDATE_TRACKS_PER_VECTOR] * 4
    rows = published(relationship_db)
    assert len(rows) == 4
    assert all(len(row[2]['candidates']) == 1 for row in rows)
    assert scalar(relationship_db, 'SELECT count(*) FROM plugin_lumae_analysis__relationship_build_entities') == 0
    assert scalar(relationship_db, 'SELECT count(*) FROM plugin_lumae_analysis__relationship_build_tracks') == 0


def test_scoring_resumes_after_worker_interruption_without_repeating_completed_work(relationship_db, monkeypatch):
    db = relationship_db
    calls = []
    def lookup(*_):
        calls.append(1)
        if len(calls) == 2:
            raise KeyboardInterrupt('worker stopped')
        return ['tr-0', 'tr-4']
    with pytest.raises(KeyboardInterrupt):
        advance(db, candidate_lookup=lookup)
    assert scalar(db, 'SELECT count(*) FROM plugin_lumae_analysis__relationship_build_entities WHERE result_fp IS NOT NULL') == 1
    assert published(db) == []
    # Resume through a new DB connection, as on a restarted worker.
    other = connection_like(db)
    try:
        monkeypatch.setattr(e, '_load_relationship_inputs', lambda *_: pytest.fail('reloaded completed inputs'))
        result = finish(other, candidate_lookup=lambda *_: calls.append(1) or ['tr-0', 'tr-4'])
        assert result['generation'] == 1
        assert len(calls) == 5  # Four entities, plus the interrupted attempt only.
    finally:
        other.close()


def test_fingerprints_resume_from_saved_inputs_without_repeating_completed_entities(relationship_db, monkeypatch):
    db = relationship_db
    result = advance(db, batch_size=1)
    assert result['status'] == 'queued'
    done_key = scalar(db, 'SELECT entity_id FROM plugin_lumae_analysis__relationship_build_entities')
    monkeypatch.setattr(e, '_load_relationship_inputs', lambda *_: pytest.fail('reloaded provider query'))
    original = e._album_fingerprint
    def fingerprint(key, tracks):
        assert key != done_key
        return original(key, tracks)
    monkeypatch.setattr(e, '_album_fingerprint', fingerprint)
    assert finish(db, batch_size=1)['generation'] == 1


def test_input_change_at_publication_is_rechecked_before_publishing(relationship_db):
    db = relationship_db
    first = finish(db)
    original = published(db)
    bump_analysis_epoch(db)
    other = connection_like(db)
    try:
        def progress(phase, **_):
            if phase == 'Publishing relationship generation':
                bump_analysis_epoch(other)
        result = advance(db, progress=progress, candidate_lookup=lambda *_: [])
        assert result['status'] == 'queued'
        assert published(db) == original
        assert e.relationship_status(db, 'catalog-a')['cursor'] == first['cursor']
        assert finish(db)['generation'] == 2
    finally:
        other.close()


def test_missing_index_retains_fingerprints_and_retries(relationship_db, monkeypatch):
    def missing(*_):
        raise e.RelationshipIndexUnavailable('not ready')
    result = advance(relationship_db, candidate_lookup=missing)
    assert result['status'] == 'waiting_for_index'
    monkeypatch.setattr(e, '_load_relationship_inputs', lambda *_: pytest.fail('reloaded prepared inputs'))
    assert finish(relationship_db)['generation'] == 1


def test_expired_time_budget_still_makes_progress_after_loading_checkpoints(relationship_db, monkeypatch):
    monkeypatch.setattr(rb.Build, 'exhausted', lambda _: True)
    assert finish(relationship_db)['generation'] == 1


def test_cleanup_failure_does_not_relabel_published_generation_as_failed(relationship_db):
    db = relationship_db
    with db.cursor() as cur:
        cur.execute("""
            CREATE FUNCTION deny_relationship_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected cleanup failure'; END $$;
            CREATE TRIGGER deny_cleanup BEFORE DELETE ON plugin_lumae_analysis__relationship_build_tracks
                FOR EACH ROW EXECUTE FUNCTION deny_relationship_cleanup();
        """)
    db.commit()
    first = finish(db)
    assert first['generation'] == 1
    assert e.relationship_status(db, 'catalog-a')['status'] == 'complete'
    with db.cursor() as cur:
        cur.execute('DROP TRIGGER deny_cleanup ON plugin_lumae_analysis__relationship_build_tracks')
    db.commit()
    assert finish(db, candidate_lookup=lambda *_: pytest.fail('rescored completed build')) == first
    assert scalar(db, 'SELECT count(*) FROM plugin_lumae_analysis__relationship_build_tracks') == 0


def test_temporarily_unavailable_inputs_do_not_corrupt_completed_checkpoint(relationship_db):
    db = relationship_db
    first = finish(db)
    with db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__analysis_state SET status='running'")
    db.commit()
    assert advance(db)['status'] == 'queued'
    with db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__analysis_state SET status='complete'")
    db.commit()
    assert finish(db) == first


@pytest.mark.parametrize('changed', ['catalog_generation', 'analysis_generation', 'catalog_epoch', 'analysis_epoch'])
def test_input_changes_discard_stale_checkpoints(relationship_db, changed):
    db = relationship_db
    advance(db, batch_size=1)
    old_id = scalar(db, 'SELECT build_id FROM plugin_lumae_analysis__relationship_builds')
    table, column = {
        'catalog_generation': ('catalog_state', 'published_generation'),
        'analysis_generation': ('analysis_state', 'projection_generation'),
        'catalog_epoch': ('catalog_state', 'catalog_epoch'),
        'analysis_epoch': ('analysis_state', 'analysis_epoch'),
    }[changed]
    with db.cursor() as cur:
        value = '2' if 'generation' in changed else "'changed-epoch'"
        cur.execute(f'UPDATE plugin_lumae_analysis__{table} SET {column}={value}')
    db.commit()
    result = advance(db, batch_size=1)
    assert result['status'] in ('queued', 'complete')
    assert scalar(db, 'SELECT build_id FROM plugin_lumae_analysis__relationship_builds') != old_id
    assert scalar(db, 'SELECT count(*) FROM plugin_lumae_analysis__relationship_builds') == 1


def bump_analysis_epoch(db):
    with db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__analysis_state SET analysis_epoch=analysis_epoch||'-next'")
    db.commit()


def test_failed_publication_keeps_previous_rows_and_cursor_and_resumes(relationship_db, monkeypatch):
    db = relationship_db
    finish(db)
    before = published(db)
    before_cursor = e.relationship_status(db, 'catalog-a')['cursor']
    db.commit()
    bump_analysis_epoch(db)
    compact = e.compact_change_journal
    def fail_after_publication_writes(*args, **kwargs):
        raise RuntimeError('injected publication failure')
    monkeypatch.setattr(e, 'compact_change_journal', fail_after_publication_writes)
    with pytest.raises(RuntimeError, match='injected publication failure'):
        advance(db, candidate_lookup=lambda *_: [])
    assert published(db) == before
    assert e.relationship_status(db, 'catalog-a')['cursor'] == before_cursor
    assert scalar(db, 'SELECT count(*) FROM plugin_lumae_analysis__relationship_changes') == 4
    assert e.relationship_bootstrap_page(db, 'catalog-a')['relationships']
    db.commit()
    monkeypatch.setattr(e, 'compact_change_journal', compact)
    result = finish(db, candidate_lookup=lambda *_: pytest.fail('rescored staged results'))
    assert result['generation'] == 2
    assert result['changes'] == 4
    assert all(row[2]['candidates'] == [] for row in published(db))


def test_progress_callback_cannot_commit_partial_publication_and_no_lock_during_scoring(relationship_db):
    db = relationship_db
    other = connection_like(db)
    notifications = []
    def progress(phase, current=None, total=None):
        db.commit()  # Exactly what the production progress callback does.
        notifications.append((phase, current, total))
        assert scalar(other, 'SELECT result_generation FROM plugin_lumae_analysis__relationship_state') == 0
        other.commit()
    def lookup(*_):
        with other.cursor() as cur:
            cur.execute('SELECT * FROM plugin_lumae_analysis__relationship_state FOR UPDATE NOWAIT')
        other.rollback()
        return ['tr-0', 'tr-4']
    try:
        result = finish(db, candidate_lookup=lookup, progress=progress)
        assert result['generation'] == 1
        assert any('Albums' in p[0] and p[2] == 2 for p in notifications)
    finally:
        other.close()


def test_concurrent_worker_coalesces_and_publication_lock_wait_retries_staging(relationship_db):
    db = relationship_db
    other = connection_like(db)
    try:
        with other.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext('lumae.relationships'), hashtext('catalog-a'))")
        other.commit()
        assert advance(db)['status'] == 'coalesced'
        with other.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext('lumae.relationships'), hashtext('catalog-a'))")
        other.commit()
        def progress(phase, **_):
            if phase == 'Publishing relationship generation':
                with other.cursor() as cur:
                    cur.execute('SELECT * FROM plugin_lumae_analysis__analysis_state FOR UPDATE')
        result = advance(db, progress=progress)
        assert result['status'] == 'queued'
        assert scalar(db, "SELECT progress->'error'->>'sqlstate' FROM plugin_lumae_analysis__relationship_builds") == '55P03'
        assert published(db) == []
        other.rollback()
        assert finish(db, candidate_lookup=lambda *_: pytest.fail('rescored after lock wait'))['generation'] == 1
    finally:
        other.close()


def test_real_statement_timeout_records_phase_and_sqlstate_then_recovers(relationship_db, monkeypatch, caplog):
    db = relationship_db
    original = e._load_relationship_inputs
    def slow_query(cur, source):
        cur.execute("SET LOCAL statement_timeout='20ms'")
        cur.execute('SELECT pg_sleep(1)')
    monkeypatch.setattr(e, '_load_relationship_inputs', slow_query)
    with pytest.raises(psycopg2.errors.QueryCanceled):
        advance(db)
    diagnostic = scalar(db, 'SELECT progress FROM plugin_lumae_analysis__relationship_builds')
    assert diagnostic['error']['phase'] == 'input_loading'
    assert diagnostic['error']['sqlstate'] == '57014'
    assert diagnostic['timings_ms']['input_loading'] > 0
    assert 'sqlstate=57014' in caplog.text
    assert published(db) == []
    monkeypatch.setattr(e, '_load_relationship_inputs', original)
    assert finish(db)['generation'] == 1


def test_completed_build_does_not_republish_and_unchanged_rebuild_has_no_delta(relationship_db):
    db = relationship_db
    first = finish(db)
    assert finish(db, candidate_lookup=lambda *_: pytest.fail('rebuilt current generation')) == first
    bump_analysis_epoch(db)
    second = finish(db)
    assert second['generation'] == 2
    assert second['changes'] == 0
    assert second['cursor'] == first['cursor']


def test_removed_entity_produces_delete_delta_and_atomic_empty_generation(relationship_db):
    db = relationship_db
    first = finish(db)
    with db.cursor() as cur:
        cur.execute("UPDATE plugin_lumae_analysis__catalog_tracks SET available=FALSE")
    db.commit()
    bump_analysis_epoch(db)
    second = finish(db)
    assert second['album_count'] == second['artist_count'] == second['track_count'] == 0
    assert second['changes'] == 4
    assert published(db) == []
    delta = e.read_relationship_changes(db, first['cursor'])
    assert len(delta['changes']) == 4
    assert all(row['operation'] == 'delete' for row in delta['changes'])


def test_reconcile_checkpoint_is_deferred_without_failure_backoff(monkeypatch):
    mod = load_plugin()
    events, retries = [], []
    monkeypatch.setattr(mod, 'begin_event', lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(mod, '_safe_progress', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, 'finish_event', lambda *args, **kwargs: events.append((args, kwargs)))
    monkeypatch.setattr(mod, 'update_work_retry', lambda *args, **kwargs: retries.append(kwargs))
    result = mod._run_reconcile_action(object(), 'relationships', 'server-a', 'catalog-a',
                                       'catalog-a', 0, lambda: {'status': 'queued'})
    assert result == {'status': 'queued'}
    assert retries == [{'failed': False}]
    assert events[0][0][2] == 'deferred'


def test_settings_shows_saved_progress(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, 'get_db', lambda: object())
    monkeypatch.setattr(mod, 'resolve_catalog_source', lambda _: [readiness_source()])
    monkeypatch.setattr(mod, 'relationship_status', lambda *_: {
        'status': 'queued', 'build_progress': {'counts': {
            'fingerprints_done': 2970, 'albums': 2100, 'artists': 870,
            'albums_done': 420, 'artists_done': 0,
        }},
    })
    page = mod.render_relationship_status_panel()
    assert 'Album similarities: 420 / 2,100.' in page
    assert 'Artist similarities: 0 / 870.' in page
