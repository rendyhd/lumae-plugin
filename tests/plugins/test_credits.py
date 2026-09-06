"""MusicBrainz matching plus disposable PostgreSQL jobs/stream qualification."""
import json
import time
from types import SimpleNamespace

import pytest
from flask import Flask, g

from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401

load_plugin()
from plugins.LumaeAnalysis import catalog, credits_matching as matching, credits_store as store
from plugins.LumaeAnalysis import credits_musicbrainz as mb, credits_service as service


def uid(value):
    return f"00000000-0000-0000-0000-{value:012d}"


def credited(name="Artist", value=1):
    return [{"name": name, "artist": {"id": uid(value), "name": name}}]


def release(value=20):
    return {"id": uid(value), "title": "Album", "artist-credit": credited(),
        "release-group": {"id": uid(30)},
        "relations": [{"type": "producer", "target-type": "artist", "artist": {"id": uid(40), "name": "Person"}}],
        "media": [{"position": 1, "tracks": [
            {"position": i + 1, "title": f"Song {i}", "length": 200000,
             "recording": {"id": uid(100 + i), "title": f"Song {i}", "length": 200000,
                 "artist-credit": credited(), "relations": [
                     {"type": "instrument", "type-id": uid(50), "target-type": "artist",
                      "artist": {"id": uid(41), "name": "Person"}, "attributes": ["guitar"]},
                     {"type": "performance", "work": {"id": uid(200 + i), "relations": [
                         {"type": "composer", "artist": {"id": uid(42), "name": "Composer"}}]}}]}}
            for i in range(3)]}]}


def album():
    return {"id": "a", "title": "Album", "artist": "Artist", "external_ids": {},
        "tracks": [{"id": f"t{i}", "title": f"Song {i}", "artist": "Artist",
                    "disc_number": 1, "track_number": i+1, "duration_ms": 200000, "external_ids": {}}
                   for i in range(3)]}


def test_exact_id_still_requires_artist_album_order_disc_and_duration_corroboration():
    for mutate in (
        lambda data: data.update(title="Wrong album"),
        lambda data: data.update(**{"artist-credit": credited("Other")}),
        lambda data: data["media"][0]["tracks"].reverse(),
        lambda data: data["media"][0].update(position=2),
        lambda data: data["media"][0]["tracks"][0].update(length=260000),
    ):
        data = release()
        mutate(data)
        assert matching.verify_release(album(), data, trusted_id=True) is None
    assert matching.verify_release(album(), release(), trusted_id=True)


def test_ambiguous_editions_publish_recording_and_work_credits_only_and_keep_people_distinct():
    match = matching.choose_release(album(), [release(20), release(21)])
    assert match["status"] == "recordings_only"
    credits = matching.extract_credits(match)
    assert credits["album"] == []
    assert {c["scope"] for c in credits["tracks"]["t0"]} == {"recording", "work"}
    assert credits["tracks"]["t0"][0]["person_mbid"] == uid(41)
    exact = matching.extract_credits(matching.choose_release(album(), [release(20)], uid(20)))
    assert exact["album"][0]["person_mbid"] != exact["tracks"]["t0"][0]["person_mbid"]
    assert exact["album"][0]["source_url"].endswith("/release/" + uid(20))


def test_truncated_search_cannot_claim_a_unique_edition_and_conflicting_recordings_are_excluded():
    assert matching.choose_release(album(), [release()], candidates_complete=False)["status"] == "recordings_only"
    other = release(21)
    other["media"][0]["tracks"][0]["recording"]["id"] = uid(999)
    assert "t0" not in matching.choose_release(album(), [release(), other])["recordings"]


def test_compilation_uses_each_track_artist_and_empty_metadata_remains_unresolved():
    local, external = album(), release()
    local["artist"] = "Various Artists"
    external["artist-credit"] = credited("Various Artists")
    assert matching.verify_release(local, external)
    external["media"][0]["tracks"][0]["recording"]["artist-credit"] = credited("Wrong person")
    assert matching.verify_release(local, external) is None
    assert matching.choose_release({"title": "", "tracks": []}, [release()])["status"] == "unresolved"


def test_same_name_artist_with_conflicting_mbid_is_not_accepted():
    local = album()
    local["external_ids"]["musicbrainz_artist_id"] = uid(999)
    assert matching.verify_release(local, release(), trusted_id=True) is None


def test_reviewed_audit_requires_fifty_actual_albums_and_98_percent_precision():
    from plugins.LumaeAnalysis.credits_qualification import evaluate_audit
    report = {"matching_version": 1, "synthetic": False, "reviewed_by": "Reviewer",
              "reviewed_at": "2026-09-06T00:00:00Z", "albums": [
                  {"album_id": f"album-{i}", "categories": ["well_tagged", "sparse", "duplicate_edition", "compilation"],
                   "outcome": "accepted", "correct": i != 0} for i in range(50)]}
    assert evaluate_audit(report)["qualified"]
    report["albums"][0]["outcome"] = "empty"
    assert evaluate_audit(report)["accepted"] == 50
    assert evaluate_audit(report)["empty"] == 1
    report["albums"][1]["correct"] = False
    assert not evaluate_audit(report)["qualified"]
    report["synthetic"] = True
    assert not evaluate_audit(report)["qualified"]
    report["albums"] = report["albums"][:49]
    assert not evaluate_audit(report)["qualified"]
    assert not evaluate_audit(None)["qualified"]
    malformed = {**report, "albums": [{"album_id": [], "categories": []}] * 50}
    assert not evaluate_audit(malformed)["qualified"]


def test_verified_recording_provenance_survives_work_and_performer_projection():
    m = matching.choose_release(album(), [release()], verified_release_id=uid(20))
    track_id = next(iter(m["recordings"]))
    evidence = {"matching_version": 1, "method": "verified_recording_id_and_metadata",
                "requested_recording_mbid": uid(70), "resolved_recording_mbid": uid(71)}
    m["recording_evidence"] = {track_id: evidence}
    m["recordings"][track_id]["relations"] = [{"type": "instrument", "artist": {"id": uid(99), "name": "Person"}}]
    credit = matching.extract_credits(m)["tracks"][track_id][0]
    assert credit["matching"] == evidence


def test_typed_ids_remain_distinct_from_generic_legacy_ids_and_release_track_ids():
    row = {"musicBrainzId": uid(1), "musicBrainzAlbumId": uid(2),
           "musicBrainzReleaseGroupId": uid(3), "musicBrainzArtistId": [uid(4), uid(5)],
           "musicBrainzRecordingId": uid(6), "musicBrainzTrackId": uid(7)}
    ids = catalog._external_ids(row, "track")
    assert ids["musicbrainz"] == uid(1)
    assert ids["musicbrainz_release_id"] == uid(2)
    assert ids["musicbrainz_release_group_id"] == uid(3)
    assert ids["musicbrainz_artist_id"] == [uid(4), uid(5)]
    assert ids["musicbrainz_recording_id"] == uid(6)
    assert ids["musicbrainz_release_track_id"] == uid(7)


@pytest.fixture
def credits_db(lumae_postgres_db):
    db = lumae_postgres_db
    catalog.migrate_catalog(db)
    store.migrate(db)
    cur = db.cursor()
    for source in ["c", "other"]:
        cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_sources
            (catalog_instance_id,current_core_server_id,provider_type,server_name)
            VALUES(%s,%s,'navidrome','Test')""", (source, source))
        cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_state
            (catalog_instance_id,provider_type,published_generation,catalog_epoch,status)
            VALUES(%s,'navidrome',1,%s,'complete')""", (source, uid(700)))
        for name in ["a", "b"]:
            cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_albums
                (catalog_instance_id,published_generation,album_id,name,album_artist_display,metadata_fp,payload,first_seen_at,last_seen_at)
                VALUES(%s,1,%s,'Album','Artist','fp','{}',now(),now())""", (source, name))
            for i in range(3):
                cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_tracks
                    (catalog_instance_id,published_generation,track_id,album_id,title,artist_display,disc_number,
                     track_number,duration_ms,metadata_fp,payload,first_seen_at,last_seen_at)
                    VALUES(%s,1,%s,%s,%s,'Artist',1,%s,200000,'fp','{}',now(),now())""",
                    (source, name + str(i), name, "Song " + str(i), i+1))
    cur.close()
    db.commit()
    return db


def payload(db, name):
    data = store.load_album(db, "c", name)
    return {"schema_version": 1, "catalog_instance_id": "c", "album_id": name,
            "input_fingerprint": data["input_fp"], "match_status": "matched",
            "subjects": [{"subject_id": name, "subject_type": "album", "credits": [{"person_mbid": uid(40)}]}]}


def publish(db, name):
    store.request_album(db, "c", name, priority=10)
    job = store.claim(db, "c")
    assert job["album_id"] == name
    assert store.finish(db, job, payload(db, name))
    return job


def test_jobs_coalesce_leases_recover_and_stale_writes_cannot_publish(credits_db):
    db = credits_db
    store.request_album(db, "c", "a", 10)
    store.request_album(db, "c", "a", 10)
    first = store.claim(db, "c")
    assert store.claim(db, "c") is None
    cur = db.cursor()
    cur.execute("UPDATE plugin_lumae_analysis__credits_jobs SET lease_until=now()-interval '1 second'")
    cur.close()
    db.commit()
    second = store.claim(db, "c")
    assert first["token"] != second["token"]
    assert not store.finish(db, first, payload(db, "a"))
    assert store.finish(db, second, payload(db, "a"))
    assert not store.finish(db, second, payload(db, "a"))
    assert store.status(db, "c")["counts"] == {"complete": 1}


def test_bootstrap_pins_versions_while_new_publications_arrive_and_deltas_are_isolated(credits_db):
    db = credits_db
    publish(db, "a")
    publish(db, "b")
    first = store.bootstrap(db, "c", limit=1)
    assert first["has_more"]
    cur = db.cursor()
    cur.execute("UPDATE plugin_lumae_analysis__credits_jobs SET expires_at=now()-interval '1 second' WHERE album_id='b'")
    cur.close()
    db.commit()
    store.request_album(db, "c", "b")
    job = store.claim(db, "c")
    replacement = payload(db, "b")
    replacement["changed"] = True
    store.finish(db, job, replacement)
    second = store.bootstrap(db, "c", first["next_page_token"], limit=1)
    assert second["cursor"] == first["cursor"]
    assert "changed" not in second["records"][0]
    assert store.changes(db, "c", first["cursor"])["changes"][0]["record"]["changed"]
    with pytest.raises(ValueError):
        store.changes(db, "other", first["cursor"])
    with pytest.raises(KeyError):
        store.bootstrap(db, "other", first["next_page_token"])


def test_metadata_correction_publishes_tombstone_and_failure_keeps_valid_prior_result(credits_db):
    db = credits_db
    publish(db, "a")
    cursor = store.status(db, "c")["cursor"]
    cur = db.cursor()
    cur.execute("UPDATE plugin_lumae_analysis__catalog_tracks SET title='Corrected' WHERE catalog_instance_id='c' AND track_id='a0'")
    cur.close()
    db.commit()
    store.request_album(db, "c", "a")
    assert store.changes(db, "c", cursor)["changes"][0]["operation"] == "delete"
    job = store.claim(db, "c")
    store.finish(db, job, error="MusicBrainz unavailable", retry_seconds=600)
    assert store.bootstrap(db, "c")["records"] == []
    cur = db.cursor()
    cur.execute("UPDATE plugin_lumae_analysis__credits_jobs SET not_before=now()")
    cur.close()
    db.commit()
    job = store.claim(db, "c")
    store.finish(db, job, payload(db, "a"))
    cur = db.cursor()
    cur.execute("UPDATE plugin_lumae_analysis__credits_jobs SET expires_at=now()-interval '1 second'")
    cur.close()
    db.commit()
    store.request_album(db, "c", "a")
    store.finish(db, store.claim(db, "c"), error="offline")
    assert len(store.bootstrap(db, "c")["records"]) == 1


def test_unavailable_track_invalidates_album_connections_and_source_removal_cascades(credits_db):
    db = credits_db
    publish(db, "a")
    cur = db.cursor()
    cur.execute("UPDATE plugin_lumae_analysis__catalog_tracks SET available=FALSE WHERE catalog_instance_id='c' AND track_id='a0'")
    cur.close()
    db.commit()
    assert not store.request_album(db, "c", "a")
    assert store.bootstrap(db, "c")["records"] == []
    cur = db.cursor()
    cur.execute("DELETE FROM plugin_lumae_analysis__catalog_sources WHERE catalog_instance_id='c'")
    cur.execute("SELECT COUNT(*) FROM plugin_lumae_analysis__credits_versions")
    assert cur.fetchone()[0] == 0
    cur.close()
    db.commit()


def test_http_cache_throttle_negative_ttl_and_global_backoff(credits_db):
    calls = []
    def get(url, **kwargs):
        calls.append((time.monotonic(), url, kwargs))
        return SimpleNamespace(status_code=200, headers={}, content=b'{}',
                               json=lambda: {"id": uid(20), "title": "Album"})
    http = SimpleNamespace(get=get)
    client = mb.Client(credits_db, http=http)
    client.get("release", uid(20))
    client.get("release", uid(20))
    client.get("release", uid(21))
    assert len(calls) == 2 and calls[1][0] - calls[0][0] >= 0.98
    assert "Lumae" in calls[0][2]["headers"]["User-Agent"]
    assert calls[0][2]["allow_redirects"] is False
    client.http = SimpleNamespace(get=lambda *_args, **_kwargs: SimpleNamespace(
        status_code=503, headers={"Retry-After": "120"}, content=b""))
    with pytest.raises(mb.MusicBrainzDeferred) as raised:
        client.get("release", uid(22))
    assert raised.value.retry_seconds == 120
    with pytest.raises(mb.MusicBrainzDeferred):
        mb.Client(credits_db, http=http).get("release", uid(23))
    assert len(calls) == 2


def test_verified_identity_redirect_is_throttled_and_cannot_leave_musicbrainz(credits_db):
    calls = []
    def get(url, **kwargs):
        calls.append((time.monotonic(), url))
        if url.endswith(uid(20)):
            return SimpleNamespace(status_code=301, headers={"Location": "https://musicbrainz.org/ws/2/recording/" + uid(21)}, content=b"")
        return SimpleNamespace(status_code=200, headers={}, content=b"{}", json=lambda: {"id": uid(21)})
    client = mb.Client(credits_db, http=SimpleNamespace(get=get))
    assert client.get("recording", uid(20))["id"] == uid(21)
    assert calls[1][0] - calls[0][0] >= .98
    client.http = SimpleNamespace(get=lambda *a, **k: SimpleNamespace(status_code=301,
        headers={"Location": "https://example.com/ws/2/recording/" + uid(22)}, content=b""))
    with pytest.raises(mb.MusicBrainzDeferred, match="Unverified"):
        client.get("recording", uid(23))


def test_http_lock_is_shared_across_workers_and_empty_credits_expire_after_seven_days(credits_db):
    import psycopg2
    other = psycopg2.connect(__import__("os").environ["LUMAE_POSTGRES_TEST_DSN"])
    cur = other.cursor()
    cur.execute("SELECT pg_advisory_lock(%s)", (mb.LOCK_ID,))
    other.commit()
    calls = []
    client = mb.Client(credits_db, http=SimpleNamespace(get=lambda *args, **kwargs: calls.append(args)))
    try:
        with pytest.raises(mb.MusicBrainzDeferred):
            client.get("release", uid(20))
        assert calls == []
    finally:
        other.close()
    response = SimpleNamespace(status_code=200, headers={}, content=b'{}',
                               json=lambda: {"id": uid(20), "title": "Album", "relations": []})
    client.http = SimpleNamespace(get=lambda *args, **kwargs: response)
    client.get("release", uid(20), inc=mb.RELEASE_INC)
    cur = credits_db.cursor()
    cur.execute("SELECT EXTRACT(EPOCH FROM(expires_at-now()))/86400 FROM plugin_lumae_analysis__credits_http_cache")
    assert 6.9 < float(cur.fetchone()[0]) <= 7
    cur.close()
    credits_db.commit()


def test_expired_bootstrap_and_compacted_delta_cursors_require_a_fresh_snapshot(credits_db):
    import base64
    import json
    db = credits_db
    beginning = store.status(db, "c")["cursor"]
    publish(db, "a")
    publish(db, "b")
    first = store.bootstrap(db, "c", limit=1)
    token = json.loads(base64.urlsafe_b64decode(first["next_page_token"] + "=" * (-len(first["next_page_token"]) % 4)))
    token["expires"] = 0
    expired = base64.urlsafe_b64encode(json.dumps(token).encode()).decode().rstrip("=")
    with pytest.raises(KeyError):
        store.bootstrap(db, "c", expired)
    cur = db.cursor()
    cur.execute("UPDATE plugin_lumae_analysis__credits_stream SET floor_seq=1 WHERE catalog_instance_id='c'")
    cur.close()
    db.commit()
    with pytest.raises(KeyError):
        store.changes(db, "c", beginning)


def test_maintenance_and_playback_priority_leave_the_credits_job_pending(credits_db, monkeypatch):
    store.request_album(credits_db, "c", "a")
    def forbidden(*args, **kwargs):
        raise AssertionError("MusicBrainz must not be contacted")
    monkeypatch.setattr(service, "paused", lambda: True)
    assert service.run_one("c", db=credits_db, critical=lambda db: False, client_factory=forbidden)["status"] == "deferred"
    monkeypatch.setattr(service, "paused", lambda: False)
    assert service.run_one("c", db=credits_db, critical=lambda db: True, client_factory=forbidden)["status"] == "deferred"
    assert store.claim(credits_db, "c")["album_id"] == "a"


def test_new_routes_require_authentication_and_catalog_scope(credits_db, monkeypatch):
    monkeypatch.setattr(service, "get_db", lambda: credits_db)
    app = Flask(__name__)
    app.register_blueprint(load_plugin().bp)
    @app.before_request
    def authenticate():
        from flask import request
        if request.headers.get("Authorization") == "Bearer test":
            g.auth_method = "bearer"
    client = app.test_client()
    assert client.get("/api/credits/status?catalog_instance_id=c").status_code == 401
    assert client.get("/api/credits/status?catalog_instance_id=c", headers={"Authorization": "Bearer test"}).status_code == 200
    assert client.get("/api/credits/status", headers={"Authorization": "Bearer test"}).status_code == 400
    assert client.post("/api/credits/prepare", json=[], headers={"Authorization": "Bearer test"}).status_code == 400
    assert client.post("/api/credits/prepare", json={"catalog_instance_id": "c", "album_ids": [""]}, headers={"Authorization": "Bearer test"}).status_code == 400
    assert client.get("/api/credits/status?catalog_instance_id=missing", headers={"Authorization": "Bearer test"}).status_code == 400
