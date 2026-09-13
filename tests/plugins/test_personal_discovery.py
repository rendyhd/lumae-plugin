import importlib
import uuid
import pytest
from flask import Flask, g, request
from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401


def uid():
    return str(uuid.uuid4())


@pytest.fixture
def api(lumae_postgres_db, monkeypatch):
    plugin = load_plugin()
    mod = importlib.import_module("plugins.LumaeAnalysis.personal_discovery")
    meta = importlib.import_module("plugins.LumaeAnalysis.music_metadata")
    mod.migrate(lumae_postgres_db)
    meta.migrate(lumae_postgres_db)
    lumae_postgres_db.commit()
    monkeypatch.setattr(mod, "get_db", lambda: lumae_postgres_db)
    monkeypatch.setattr(meta, "get_db", lambda: lumae_postgres_db)
    monkeypatch.setattr(mod, "collections_enabled", lambda: True)
    monkeypatch.setattr(meta, "paused", lambda: False)
    app = Flask(__name__)
    app.register_blueprint(plugin.bp)
    @app.before_request
    def auth():
        g.auth_method = request.headers.get("Test-Auth", "session")
        g.auth_user = request.headers.get("Test-User", "alice") if g.auth_method == "session" else None
    return app.test_client(), mod, meta, lumae_postgres_db


def initial(client, user="alice"):
    return client.get('/api/personal_discovery/bootstrap', headers={"Test-User": user}).get_json()


def mutation(epoch, record=None, **kwargs):
    return {"id": uid(), "epoch": epoch, "recordId": record or uid(), "kind": "want", "operation": "upsert", "baseRevision": 0, "fields": {"note": "original"}, **kwargs}


def post(client, body, user="alice"):
    return client.post('/api/personal_discovery/mutations', json=body, headers={"Test-User": user})


def test_sync_conflicts_tombstones_and_epoch(api):
    client, _, _, _ = api
    epoch = initial(client)["epoch"]
    first = mutation(epoch)
    saved = post(client, first)
    assert saved.status_code == 200
    assert post(client, first).get_json() == saved.get_json()
    assert post(client, {**first, "fields": {"note": "changed"}}).status_code == 409
    record = first['recordId']
    independent = mutation(epoch, record, fields={"intent": "comfort"})
    assert post(client, independent).status_code == 200
    conflict = post(client, mutation(epoch, record, fields={"note": "other"}))
    assert conflict.status_code == 409 and conflict.get_json()['fields'] == ['note']
    deleted = post(client, mutation(epoch, record, operation="delete", fields={}))
    assert deleted.get_json()['record']['deleted']
    assert post(client, mutation(epoch, record, baseRevision=2)).status_code == 409
    assert post(client, mutation(epoch, record, operation="restore", baseRevision=3, fields={})).status_code == 200
    assert post(client, mutation(uid())).status_code == 410
    assert initial(client, 'bob')['records'] == []
    assert client.get('/api/personal_discovery/bootstrap', headers={'Test-Auth': 'none'}).status_code == 401


def test_canonical_saves_preserve_personal_fields_and_aliases(api):
    client, _, _, _ = api
    epoch = initial(client)['epoch']
    key = 'musicbrainz:release-group:' + uid()
    first = mutation(epoch, fields={'canonicalKey': key, 'note': 'keep', 'addedAt': 10})
    post(client, first)
    duplicate = mutation(epoch, fields={'canonicalKey': key, 'note': 'replacement', 'addedAt': 20})
    result = post(client, duplicate).get_json()
    assert result['record']['id'] == first['recordId']
    assert result['record']['fields']['note'] == 'keep'
    assert result['alias'] == duplicate['recordId']
    assert len(initial(client)['records']) == 1
    assert post(client, mutation(epoch, duplicate['recordId'], operation='delete', fields={})).get_json()['record']['deleted']


def test_bootstrap_paging_and_changes_recover_concurrent_edits(api):
    client, _, _, _ = api
    epoch = initial(client)['epoch']
    records = [mutation(epoch) for _ in range(3)]
    for body in records:
        post(client, body)
    first = client.get('/api/personal_discovery/bootstrap?limit=1').get_json()
    post(client, mutation(epoch, records[1]['recordId'], baseRevision=1, fields={'note': 'new'}))
    second = client.get(f"/api/personal_discovery/bootstrap?limit=1&epoch={epoch}&cursor={first['cursor']}&head={first['head']}").get_json()
    assert not second['hasMore']
    changes = client.get(f"/api/personal_discovery/changes?epoch={epoch}&cursor={first['head']}").get_json()
    assert changes['records'][0]['fields']['note'] == 'new'
    assert client.get('/api/personal_discovery/changes?epoch=old').status_code == 410


def test_metadata_budget_and_lease_recovery(api, monkeypatch):
    client, _, meta, db = api
    entity = {'id': uid(), 'kind': 'release-group', 'title': 'Example', 'artist': 'Artist', 'revisions': {'consent': 1}}
    assert client.post('/api/music_metadata/prepare', json={'entities': [entity]}).status_code == 202
    assert client.get('/api/music_metadata/status', headers={'Test-User': 'bob'}).get_json()['jobs'] == []
    assert client.post('/api/music_metadata/prepare', json={'entities': [{**entity, 'title': 'other'}]}).status_code == 409
    for _ in range(80):
        meta.reserve(db, 'user:alice')
    with pytest.raises(meta.MusicBrainzDeferred):
        meta.reserve(db, 'user:alice')
    assert client.get('/api/music_metadata/status').get_json()['remaining_requests'] == 0
    class FakeClient:
        def __init__(self, db, **kwargs):
            pass
        def get(self, kind, entity_id=None, **kwargs):
            return {'release-groups': []}
    assert meta.run_one(db=db, client_factory=FakeClient, critical=lambda db: False)['status'] == 'unresolved'
    assert client.get('/api/music_metadata/status').get_json()['jobs'][0]['revisions'] == {'consent': 1}
    assert client.post('/api/music_metadata/prepare', json=[]).status_code == 400


def test_metadata_limits_cancellation_and_auth(api):
    client, _, _, _ = api
    entities = [{'id': uid(), 'kind': 'artist', 'title': 'Artist'} for _ in range(41)]
    assert client.post('/api/music_metadata/prepare', json={'entities': entities}).status_code == 400
    assert client.post('/api/music_metadata/prepare', json={'entities': entities[:40]}).status_code == 202
    assert client.post('/api/music_metadata/prepare', json={'entities': entities[40:]}).status_code == 429
    assert client.post('/api/music_metadata/cancel', json={'id': entities[0]['id']}).status_code == 200
    assert client.post('/api/music_metadata/prepare', json={'entities': entities[40:]}).status_code == 202
    assert client.get('/api/music_metadata/status', headers={'Test-Auth': 'none'}).status_code == 401


def test_exact_metadata_resolution_does_not_assert_recognition():
    load_plugin()
    mod = importlib.import_module('plugins.LumaeAnalysis.music_metadata')
    identity = uid()
    class FakeClient:
        def get(self, kind, entity_id=None, **kwargs):
            row = {'id': identity, 'title': 'Album', 'artist-credit': [{'artist': {'id': uid(), 'name': 'Artist'}}]}
            return row if entity_id else {'release-groups': [row], 'count': 1}
    result = mod.resolve({'kind': 'release-group', 'title': 'Album', 'artist': 'Artist'}, FakeClient())
    assert result['status'] == 'verified' and result['recognition'] == 'unknown'
    assert result['verifiedFields']['id'] == identity
    assert 'editionDate' not in result['verifiedFields']


def test_expired_lease_and_cancelled_worker_cannot_publish(api):
    client, _, meta, db = api
    from plugin.api import table
    entity = {'id': uid(), 'kind': 'artist', 'title': 'Artist'}
    client.post('/api/music_metadata/prepare', json={'entities': [entity]})
    with db.cursor() as cur:
        cur.execute(f"UPDATE {table('metadata_jobs')} SET status='running',lease='old',lease_until=now()-interval '1 second'")
    db.commit()
    class CancellingClient:
        def __init__(self, db, **kwargs):
            pass
        def get(self, *args, **kwargs):
            with db.cursor() as cur:
                cur.execute(f"UPDATE {table('metadata_jobs')} SET status='cancelled',lease=NULL WHERE id=%s", (entity['id'],))
            db.commit()
            return {'artists': []}
    assert meta.run_one(db=db, client_factory=CancellingClient, critical=lambda db: False)['status'] == 'superseded'
    result = client.get('/api/music_metadata/status').get_json()['jobs'][0]
    assert result['status'] == 'cancelled' and result['result'] is None


def test_http_cache_hits_do_not_spend_metadata_allowance(api):
    _, _, meta, db = api
    from plugin.api import table
    store = importlib.import_module('plugins.LumaeAnalysis.credits_store')
    http_module = importlib.import_module('plugins.LumaeAnalysis.credits_musicbrainz')
    with db.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table('catalog_sources')}(catalog_instance_id TEXT PRIMARY KEY)")
    store.migrate(db)
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {table('metadata_accounts')}(principal) VALUES('cache-user')")
    db.commit()
    identity = uid()
    class Response:
        status_code = 200
        content = b'{}'
        headers = {}
        def json(self):
            return {'id': identity, 'name': 'Artist'}
    class Http:
        calls = 0
        def get(self, *args, **kwargs):
            self.calls += 1
            return Response()
    http = Http()
    client = http_module.Client(db, http=http, reserve_request=lambda: meta.reserve(db, 'cache-user'))
    client.get('artist', identity)
    for _ in range(79):
        meta.reserve(db, 'cache-user')
    client.get('artist', identity)
    assert http.calls == 1
    with pytest.raises(meta.MusicBrainzDeferred):
        client.get('artist', uid())
    assert http.calls == 1


def test_provenance_and_recording_identity_are_protected(api):
    client, _, _, _ = api
    epoch = initial(client)['epoch']
    entity = {'kind': 'track', 'catalogId': 'catalog', 'id': 'provider-id'}
    body = mutation(epoch, kind='introduction', fields={'entity': entity, 'source': 'external-ai', 'introducedAt': 12})
    assert post(client, body).status_code == 200
    assert post(client, mutation(epoch, body['recordId'], kind='introduction', baseRevision=1, fields={'source': 'deliberate'})).status_code == 409
    assert post(client, mutation(epoch, body['recordId'], kind='introduction', operation='delete', fields={})).status_code == 400
    assert post(client, mutation(epoch, kind='rest', fields={'entity': {'kind': 'track', 'id': 'approximate-title'}})).status_code == 400


def test_ambiguous_candidates_never_produce_verified_fields():
    load_plugin()
    meta = importlib.import_module('plugins.LumaeAnalysis.music_metadata')
    class Ambiguous:
        def get(self, *args, **kwargs):
            return {'artists': [{'id': uid(), 'name': 'Artist'}, {'id': uid(), 'name': 'Artist'}], 'count': 2}
    result = meta.resolve({'kind': 'artist', 'title': 'Artist'}, Ambiguous())
    assert result['status'] == 'ambiguous' and result['verifiedFields'] == {}


def test_late_canonical_conflict_preserves_both_saved_notes(api):
    client, _, _, _ = api
    epoch = initial(client)['epoch']
    key = 'musicbrainz:release-group:' + uid()
    first = mutation(epoch, fields={'canonicalKey': key, 'note': 'first'})
    second = mutation(epoch, fields={'note': 'second'})
    post(client, first)
    post(client, second)
    conflict = post(client, mutation(epoch, second['recordId'], baseRevision=1, fields={'canonicalKey': key}))
    assert conflict.status_code == 409
    assert conflict.get_json()['error'] == 'duplicate_save_conflict'
    assert {r['fields']['note'] for r in initial(client)['records']} == {'first', 'second'}


def test_budget_reservation_is_atomic_across_connections(api):
    _, _, meta, db = api
    import psycopg2
    import os
    from psycopg2 import sql
    from concurrent.futures import ThreadPoolExecutor
    from plugin.api import table
    with db.cursor() as cur:
        cur.execute('SELECT current_schema()')
        schema = cur.fetchone()[0]
        cur.execute(f"INSERT INTO {table('metadata_accounts')}(principal,used) VALUES('race',79)")
    db.commit()
    def reserve_once(_):
        conn = psycopg2.connect(os.environ["LUMAE_POSTGRES_TEST_DSN"])
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('SET search_path TO {}, public').format(sql.Identifier(schema)))
            try:
                meta.reserve(conn, 'race')
                return True
            except meta.MusicBrainzDeferred:
                return False
        finally:
            conn.close()
    with ThreadPoolExecutor(max_workers=2) as workers:
        assert sorted(workers.map(reserve_once, range(2))) == [False, True]


def test_metadata_yields_to_playback_and_shared_principal_is_distinct(api):
    client, _, meta, db = api
    assert meta.run_one(db=db, critical=lambda db: True)['status'] == 'deferred'
    personal = initial(client)
    shared = client.get('/api/personal_discovery/bootstrap', headers={'Test-Auth': 'bearer'}).get_json()
    assert personal['epoch'] != shared['epoch']
    assert client.get('/api/personal_discovery/changes', query_string={'epoch': personal['epoch'], 'cursor': 999}).status_code == 410
