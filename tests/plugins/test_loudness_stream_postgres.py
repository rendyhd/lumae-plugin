"""LN.4b: the loudness stream (contract §3.9) on the real migrated schema: publication and its
journal, album records over the published catalogue, bootstrap paging under a pinned head, the
catalogue reconciliation, the provider-identity rekey's epoch rotation, and no profile or catalogue
events."""
import hashlib
import json

import numpy as np
import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from test_lumae_analysis import _edge_payload_for_job, edge_publication_db, load_plugin, lumae_postgres_db  # noqa: F401
from plugins.LumaeAnalysis import catalog, loudness_stream, loudness_v1

SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"


def revision(media_fp):
    return "sha256:" + hashlib.sha256(("catalog-media:" + media_fp).encode("utf-8")).hexdigest()


def _source(db, tracks):
    """A published generation 1 holding ``tracks``: (track_id, album_id, media_fp)."""
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, provider_type, "
            "server_name, is_default, rebind_status) VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')",
            (SOURCE,))
        cur.execute(
            f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, provider_type, "
            "published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, 'server-a', 'navidrome', 1, 'catalog-epoch', 'complete', '{}'::jsonb)", (SOURCE,))
        for track_id, album_id, media_fp in tracks:
            _track(cur, track_id, album_id, media_fp)
    db.commit()


def _track(cur, track_id, album_id, media_fp, music=True):
    cur.execute(
        f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, track_id, album_id, title, "
        "metadata_fp, media_fp, payload, available, analysis_eligible, first_seen_at, last_seen_at) "
        "VALUES (%s, 1, %s, %s, 'Song', 'meta', %s, '{}'::jsonb, TRUE, %s, now(), now())",
        (SOURCE, track_id, album_id, media_fp, music))


def record(gain=1.0, seconds=4.0, rate=8000):
    t = np.arange(int(seconds * rate)) / rate
    tone = gain * 0.3 * np.sin(2 * np.pi * 440 * t)
    meter = loudness_v1.NativeMeter(rate, (1.0, 1.0))
    meter.add(np.stack([tone, tone]))
    return meter.result()


def publish(db, measurements):
    with db.cursor() as cur:
        events = loudness_stream.publish_tracks(cur, SOURCE, measurements)
    db.commit()
    return events


def measurement(track_id, media_fp, gain=1.0):
    return {"track_id": track_id, "media_revision": revision(media_fp), "record": record(gain)}


def changes(db, seq=0):
    epoch = loudness_stream._state(db.cursor(), SOURCE)[0]
    db.commit()
    return loudness_stream.read_changes(db, catalog.opaque_cursor(SOURCE, epoch, seq), SOURCE)


def items(page):
    return [change["item"] for change in page["changes"]]


def test_publication_journals_changed_records_and_pools_complete_albums(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al-1", "m1"), ("t2", "al-1", "m2"), ("t3", None, "m3")])
    assert publish(db, [measurement("t1", "m1")]) == 1  # al-1 still lacks t2
    assert publish(db, [measurement("t2", "m2", 0.5), measurement("t3", "m3")]) == 3
    journal = items(changes(db))
    assert [(item["kind"], item.get("track_id") or item.get("album_id")) for item in journal] == [
        ("track", "t1"), ("track", "t2"), ("track", "t3"), ("album", "al-1")]
    track = journal[0]
    assert set(track) == {"kind", "track_id", "media_revision", "version", "method", "integrated_lufs",
                          "true_peak_dbtp", "sample_peak_dbfs", "lra_lu", "max_short_term_lufs",
                          "max_momentary_lufs", "channels", "sample_rate"}
    assert track["media_revision"] == revision("m1") and track["method"] == "bs1770-4-native"
    album = journal[3]
    pairs = [["t1", revision("m1")], ["t2", revision("m2")]]
    expected = "sha256:" + hashlib.sha256(json.dumps(pairs, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    assert album["track_set_hash"] == expected == loudness_stream.track_set_hash([("t2", revision("m2")), ("t1", revision("m1"))])
    assert (album["members"], album["exact"], album["version"]) == (2, True, 1)
    assert album["loudest_track_lufs"] == track["integrated_lufs"]
    # The same measurement again: nothing to journal.
    assert publish(db, [measurement("t1", "m1")]) == 0
    # Never a profile or catalogue event.
    with db.cursor() as cur:
        cur.execute(f"SELECT (SELECT count(*) FROM {P}profile_changes) + (SELECT count(*) FROM {P}catalog_changes)")
        assert cur.fetchone()[0] == 0
    db.rollback()


def test_bootstrap_pages_tracks_then_albums_under_a_pinned_head(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al-1", "m1"), ("t2", "al-1", "m2"), ("t3", "al-2", "m3")])
    publish(db, [measurement(t, m) for t, m in (("t1", "m1"), ("t2", "m2"), ("t3", "m3"))])
    first = loudness_stream.bootstrap_page(db, SOURCE, limit="2")
    assert [i["track_id"] for i in first["items"]] == ["t1", "t2"] and first["has_more"]
    # A record published while paging: the pinned cursor stays, the replay delivers it.
    publish(db, [measurement("t1", "m1", 0.25)])
    second = loudness_stream.bootstrap_page(db, SOURCE, page_token=first["next_page_token"], limit=2)
    assert second["cursor"] == first["cursor"]
    assert [i.get("track_id") or i.get("album_id") for i in second["items"]] == ["t3", "al-1"]
    third = loudness_stream.bootstrap_page(db, SOURCE, page_token=second["next_page_token"], limit=2)
    assert [i["album_id"] for i in third["items"]] == ["al-2"] and not third["has_more"]
    assert third["next_page_token"] is None
    replay = loudness_stream.read_changes(db, first["cursor"], SOURCE)
    assert [(i["kind"], i.get("track_id") or i.get("album_id")) for i in items(replay)] == [("track", "t1"), ("album", "al-1")]
    assert not replay["has_more"]


def test_bootstrap_and_changes_refuse_bad_input(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al-1", "m1")])
    publish(db, [measurement("t1", "m1")])
    for bad in (dict(limit="x"), dict(page_token="%%%"), dict(page_token="e30")):
        with pytest.raises(ValueError):
            loudness_stream.bootstrap_page(db, SOURCE, **bad)
    with pytest.raises(ValueError):
        loudness_stream.bootstrap_page(db, "")
    with pytest.raises(ValueError):
        loudness_stream.bootstrap_page(db, "catalog-unknown")
    epoch = loudness_stream._state(db.cursor(), SOURCE)[0]
    with pytest.raises(ValueError):
        loudness_stream.read_changes(db, catalog.opaque_cursor(SOURCE, epoch, 99), SOURCE)
    with pytest.raises(ValueError):
        loudness_stream.read_changes(db, catalog.opaque_cursor(SOURCE, epoch, 0), "catalog-b")
    with pytest.raises(ValueError):
        loudness_stream.read_changes(db, catalog.opaque_cursor(SOURCE, epoch, 0), SOURCE, limit="x")
    with pytest.raises(KeyError):
        loudness_stream.read_changes(db, catalog.opaque_cursor(SOURCE, "old-epoch", 0), SOURCE)
    page = loudness_stream.read_changes(db, catalog.opaque_cursor(SOURCE, epoch, 0), SOURCE, limit=5000)
    assert len(page["changes"]) == 2


def test_reconcile_follows_the_catalogue(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al-1", "m1"), ("t2", "al-1", "m2"), ("t3", "al-2", "m3")])
    publish(db, [measurement(t, m) for t, m in (("t1", "m1"), ("t2", "m2"), ("t3", "m3"))])
    head = changes(db)["changes"][-1]["seq"]
    with db.cursor() as cur:
        # t2 leaves the catalogue; al-2 gains an unmeasured member; nothing else changes.
        cur.execute(f"DELETE FROM {P}catalog_tracks WHERE track_id='t2'")
        _track(cur, "t4", "al-2", "m4")
        events = loudness_stream.reconcile(cur, SOURCE)
    db.commit()
    journal = items(changes(db, head))
    assert events == len(journal) == 3
    assert journal[0] == {"kind": "track", "id": "t2", "op": "delete"}
    al1 = next(i for i in journal if i.get("album_id") == "al-1")
    assert al1["members"] == 1 and al1["track_set_hash"] == loudness_stream.track_set_hash([("t1", revision("m1"))])
    assert {"kind": "album", "id": "al-2", "op": "delete"} in journal
    # Nothing moved: no album is pooled again.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}loudness_albums SET record=jsonb_set(record, '{{members}}', '9') WHERE album_id='al-1'")
        assert loudness_stream.reconcile(cur, SOURCE) == 0
        cur.execute(f"SELECT record->>'members' FROM {P}loudness_albums WHERE album_id='al-1'")
        assert cur.fetchone()[0] == "9"
    db.rollback()
    # A member's media changed: its old record stays, the album is withdrawn until it is measured again.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET media_fp='m1b' WHERE track_id='t1'")
        assert loudness_stream.reconcile(cur, SOURCE) == 1
    db.commit()
    assert items(changes(db, head + 3)) == [{"kind": "album", "id": "al-1", "op": "delete"}]
    assert any(i.get("track_id") == "t1" for i in loudness_stream.bootstrap_page(db, SOURCE)["items"])
    with db.cursor() as cur:
        cur.execute(f"SELECT retention_limit FROM {P}loudness_stream_state WHERE catalog_instance_id=%s", (SOURCE,))
        assert cur.fetchone()[0] == loudness_stream.RETENTION_EVENTS
    db.rollback()


def test_rekey_moves_records_and_rotates_the_epoch(migrated_db):
    db = migrated_db
    _source(db, [("old-1", "al-1", "m1"), ("old-2", "al-1", "m2")])
    publish(db, [measurement("old-1", "m1"), measurement("old-2", "m2")])
    before = loudness_stream.bootstrap_page(db, SOURCE)
    with db.cursor() as cur:
        loudness_stream.rekey(cur, SOURCE, [{"entity_type": "track", "old_id": "old-1", "new_id": "new-1"},
                                            {"entity_type": "track", "old_id": "old-2", "new_id": "new-2"}])
        cur.execute(f"UPDATE {P}catalog_tracks SET track_id=replace(track_id, 'old', 'new')")
    db.commit()
    with pytest.raises(KeyError):
        loudness_stream.read_changes(db, before["cursor"], SOURCE)
    after = loudness_stream.bootstrap_page(db, SOURCE)
    assert [i["track_id"] for i in after["items"]] == ["new-1", "new-2"]  # no album until reconciled
    with db.cursor() as cur:
        loudness_stream.reconcile(cur, SOURCE)
    db.commit()
    album = next(i for i in loudness_stream.bootstrap_page(db, SOURCE)["items"] if i["kind"] == "album")
    assert album["track_set_hash"] == loudness_stream.track_set_hash([("new-1", revision("m1")), ("new-2", revision("m2"))])


def test_journal_retention_follows_the_catalogue_not_the_records(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al-1", "m1")])
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, track_id, title, metadata_fp, "
            "media_fp, payload, available, analysis_eligible, first_seen_at, last_seen_at) "
            "SELECT %s, 1, 'bulk-' || n, 'Song', 'meta', 'bulk-' || n, '{}'::jsonb, TRUE, TRUE, now(), now() "
            "FROM generate_series(1, 25000) AS n", (SOURCE,))
        loudness_stream.reconcile(cur, SOURCE)  # nothing is measured yet
        cur.execute(f"SELECT retention_limit FROM {P}loudness_stream_state WHERE catalog_instance_id=%s", (SOURCE,))
        retention = cur.fetchone()[0]
    db.commit()
    assert retention == catalog.change_journal_retention_limit(25001 + 1) > loudness_stream.RETENTION_EVENTS


def test_a_failed_loudness_rekey_never_blocks_the_identity_rekey(migrated_db, monkeypatch):
    from plugins.LumaeAnalysis import provider_identity_rekey

    db = migrated_db
    _source(db, [("old-1", "al-1", "m1")])
    publish(db, [measurement("old-1", "m1")])
    before = loudness_stream.bootstrap_page(db, SOURCE)

    def broken(cur, source, mappings):
        cur.execute(f"DELETE FROM {P}loudness_tracks WHERE catalog_instance_id=%s", (source,))
        raise RuntimeError("loudness rekey bug")

    monkeypatch.setattr(loudness_stream, "rekey", broken)
    with db.cursor() as cur:
        provider_identity_rekey._rekey_plugin_owned_state(
            cur, SOURCE, [{"entity_type": "track", "old_id": "old-1", "new_id": "new-1"}])
        cur.execute(f"UPDATE {P}catalog_tracks SET track_id='new-1' WHERE track_id='old-1'")
    db.commit()
    with pytest.raises(KeyError):  # a new epoch: clients bootstrap again
        loudness_stream.read_changes(db, before["cursor"], SOURCE)
    assert loudness_stream.bootstrap_page(db, SOURCE)["items"] == []  # measured again by the backfill


def test_rekey_locks_the_stream_before_any_record(migrated_db, second_connection):
    """While a publisher holds the stream row, the rekey waits on it before touching a record, so
    the two take their locks in one order and never deadlock."""
    a, b = migrated_db, second_connection
    _source(a, [("t1", "al-1", "m1"), ("t2", "al-1", "m2")])
    publish(a, [measurement("t1", "m1"), measurement("t2", "m2")])
    holder = b.cursor()
    loudness_stream._state(holder, SOURCE, for_update=True)
    with a.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '300ms'")
        with pytest.raises(psycopg2.errors.LockNotAvailable):
            loudness_stream.rekey(cur, SOURCE, [{"entity_type": "track", "old_id": "t1", "new_id": "n1"}])
    # Had the rekey moved t1 or withdrawn the album before waiting, the publisher would wait on them.
    holder.execute("SET LOCAL lock_timeout = '300ms'")
    assert loudness_stream.publish_tracks(holder, SOURCE, [measurement("t1", "m1", 0.5)]) == 2
    b.commit()
    a.rollback()


def test_the_edge_task_stores_its_measurement_beside_the_edge(edge_publication_db, monkeypatch):
    """The edge pass measures in its decode: the record leaves the payload before the edge is
    published, is stored under the job's media revision, and is dropped when the edge is not applied."""
    from plugins.LumaeAnalysis import edge_profile_store as store
    from plugins.LumaeAnalysis.edge_profiles import opaque_revision

    mod = load_plugin()
    db = edge_publication_db
    loudness_stream.migrate_loudness(db)
    with db.cursor() as cur:
        # Catalogue signatures, as profiles carry them since 1.3: the revision clients compare with.
        cur.execute(f"UPDATE {P}published_source_profiles SET media_signature='catalog-media:' || media_signature")
        cur.execute(
            f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, track_id, title, metadata_fp, "
            "media_fp, payload, first_seen_at, last_seen_at) "
            "VALUES ('catalog-a', 1, 'track-b', 'Track B', 'metadata', 'private/path:b', '{}'::jsonb, now(), now())")
        cur.execute(
            f"INSERT INTO {P}published_source_profiles (catalog_instance_id, track_id, media_signature, sample_rate, "
            "duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, analyzed_at) "
            "VALUES ('catalog-a', 'track-b', 'catalog-media:private/path:b', 48000, 209, -14, %s, %s, 1, 1, now())",
            (b"\x01", b"\x04"))
    db.commit()
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "report_edge_runtime", lambda *a, **k: None)
    monkeypatch.setattr(mod, "load_track_file", lambda track_id, **_: {
        "file_path": "x.flac", "cleanup_path": None,
        "media_signature": "catalog-media:" + {"track-a": "private/path:123:456"}.get(track_id, "private/path:b")})
    jobs, _ready = store.claim_edge_jobs(db, "catalog-a", ["track-a", "track-b"])
    by_track = {job["track_id"]: job for job in jobs}

    def analyze(analyzer, path, **kwargs):
        assert analyzer is loudness_v1.analyze_edge_file
        payload = _edge_payload_for_job(by_track[kwargs["track_id"]])
        payload[loudness_v1.LOUDNESS_KEY] = record()
        return payload

    publish_edge = mod.publish_edge_profile
    # track-b's media is replaced while it is analyzed: its edge is not applied.
    monkeypatch.setattr(mod, "publish_edge_profile", lambda db_, source, job, payload, signature: (
        job["track_id"] == "track-a" and publish_edge(db_, source, job, payload, signature)))
    monkeypatch.setattr(mod, "run_file_analysis", analyze)
    outcomes = mod.analyze_edges_task(jobs, "catalog-a", "server-a", "interactive")
    assert sorted(outcomes, key=lambda row: row["track_id"]) == [
        {"track_id": "track-a", "status": "ready"}, {"track_id": "track-b", "status": "superseded"}]
    with db.cursor() as cur:
        cur.execute(f"SELECT track_id, media_revision FROM {P}loudness_tracks")
        assert cur.fetchall() == [("track-a", opaque_revision("catalog-media:private/path:123:456"))]
        cur.execute(f"SELECT payload::text FROM {P}edge_profiles")
        payloads = [row[0] for row in cur.fetchall()]
        assert len(payloads) == 1 and loudness_v1.LOUDNESS_KEY not in payloads[0]
    db.commit()


def test_store_measurement_never_raises(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al-1", "m1")])
    assert loudness_stream.store_measurement(db, SOURCE, "t1", revision("m1"), record())
    broken = dict(record())
    broken["histogram"] = None
    assert not loudness_stream.store_measurement(db, SOURCE, "t1", revision("m1"), broken)
    assert [i["track_id"] for i in loudness_stream.bootstrap_page(db, SOURCE)["items"] if i["kind"] == "track"] == ["t1"]


def test_routes_answer_the_contracts_statuses(migrated_db, monkeypatch):
    from test_lumae_analysis import load_plugin, plugin_client

    db = migrated_db
    _source(db, [("t1", "al-1", "m1")])
    publish(db, [measurement("t1", "m1")])
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    client = plugin_client(mod)
    page = client.get(f"/api/catalog/loudness/bootstrap?catalog_instance_id={SOURCE}")
    assert page.status_code == 200
    body = page.get_json()
    assert body["schema_version"] == 1 and [i["kind"] for i in body["items"]] == ["track", "album"]
    assert client.get("/api/catalog/loudness/bootstrap").get_json()["error"] == "invalid_loudness_bootstrap"
    assert client.get(f"/api/catalog/loudness/bootstrap?catalog_instance_id={SOURCE}&limit=x").status_code == 400
    stale = loudness_stream._encode_token({"catalog_instance_id": SOURCE, "epoch": "gone", "head_seq": 0, "phase": "track", "after": ""})
    assert client.get(f"/api/catalog/loudness/bootstrap?catalog_instance_id={SOURCE}&page_token={stale}").status_code == 410
    replay = client.get(f"/api/catalog/loudness/changes?cursor={body['cursor']}")
    assert replay.status_code == 200 and replay.get_json()["changes"] == []
    missing = client.get("/api/catalog/loudness/changes")
    assert (missing.status_code, missing.get_json()["error"]) == (400, "cursor_required")
    old = catalog.opaque_cursor(SOURCE, "gone", 0)
    assert client.get(f"/api/catalog/loudness/changes?cursor={old}").status_code == 410
    assert client.get("/api/catalog/loudness/changes?cursor=garbage").status_code == 400
    contract = mod.sync_contract(type("Compatibility", (), {"api_contract": "test"})())
    assert contract["streams"]["loudness"] == {"schema_version": 1}
    assert loudness_stream.capability() == {"version": 1, "methods": ["bs1770-4-48k", "bs1770-4-native"], "album": True}


def test_ids_stay_as_they_are_and_a_page_may_end_with_the_tracks(migrated_db):
    db = migrated_db
    odd = " Café "  # spaces and a non-NFC sequence: never trimmed or normalized
    _source(db, [(odd, "al-1", "m1"), ("t2", "al-1", "m2")])
    publish(db, [measurement(odd, "m1"), measurement("t2", "m2")])
    first = loudness_stream.bootstrap_page(db, SOURCE, limit=2)
    assert [i["track_id"] for i in first["items"]] == [odd, "t2"] and first["has_more"]
    second = loudness_stream.bootstrap_page(db, SOURCE, page_token=first["next_page_token"], limit=2)
    assert [i["album_id"] for i in second["items"]] == ["al-1"] and not second["has_more"]
    assert items(changes(db))[0]["track_id"] == odd


def test_albums_pool_their_music_tracks_only(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al-1", "m1")])
    with db.cursor() as cur:
        _track(cur, "spoken", "al-1", "m9", music=False)  # never measured, never a member
    db.commit()
    publish(db, [measurement("t1", "m1")])
    album = next(i for i in loudness_stream.bootstrap_page(db, SOURCE)["items"] if i["kind"] == "album")
    assert album["members"] == 1
    assert album["track_set_hash"] == loudness_stream.track_set_hash([("t1", revision("m1"))])
