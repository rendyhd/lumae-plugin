"""LN.4c: the loudness backfill on the real migrated schema: which tracks are due, back-off, the
pass and its catalogue follow-up, a worker without PyAV, and the scheduler's bookkeeping."""
import hashlib
import time
import wave
from types import SimpleNamespace

import numpy as np
import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import load_plugin, plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import loudness_backfill, loudness_stream, loudness_v1, reconcile

SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"


def revision(media_fp):
    return "sha256:" + hashlib.sha256(("catalog-media:" + media_fp).encode("utf-8")).hexdigest()


def raw_revision(media_fp):
    """A waveform profile published before catalogue signatures: the bare fingerprint."""
    return "sha256:" + hashlib.sha256(media_fp.encode("utf-8")).hexdigest()


def _track(cur, track_id, album_id, media_fp, music=True, generation=1):
    cur.execute(
        f"INSERT INTO {P}catalog_tracks (catalog_instance_id, published_generation, track_id, album_id, title, "
        "metadata_fp, media_fp, payload, available, analysis_eligible, first_seen_at, last_seen_at) "
        "VALUES (%s, %s, %s, %s, 'Song', 'meta', %s, '{}'::jsonb, TRUE, %s, now(), now())",
        (SOURCE, generation, track_id, album_id, media_fp, music))


def _source(db, tracks):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, provider_type, "
            "server_name, is_default, rebind_status) VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')",
            (SOURCE,))
        cur.execute(
            f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, provider_type, "
            "published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, 'server-a', 'navidrome', 1, 'catalog-epoch', 'complete', '{}'::jsonb)", (SOURCE,))
        for row in tracks:
            _track(cur, *row)
    db.commit()


def _edge(db, track_id, media_fp, raw=False, recheck=False):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}edge_profiles (catalog_instance_id, track_id, media_revision, representation_id, "
            "media_signature, profile_digest, payload, v3_recheck) "
            "VALUES (%s, %s, %s, 'rep', %s, 'digest', '{}'::jsonb, %s)",
            (SOURCE, track_id, raw_revision(media_fp) if raw else revision(media_fp),
             media_fp if raw else "catalog-media:" + media_fp, recheck))
    db.commit()


def _profile(db, track_id, signature):
    """A published waveform profile: the edge pass takes only such tracks."""
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}published_source_profiles (catalog_instance_id, track_id, media_signature, sample_rate, "
            "duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, analyzed_at) "
            "VALUES (%s, %s, %s, 48000, 209, -14, %s, %s, 1, 1, now())", (SOURCE, track_id, signature, b"\x01", b"\x04"))
    db.commit()


def _job(db, track_id, media_revision, status, error=None):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}edge_profile_jobs (catalog_instance_id, track_id, media_revision, job_token, status, "
            "last_error) VALUES (%s, %s, %s, 'job', %s, %s)", (SOURCE, track_id, media_revision, status, error))
    db.commit()


def due(db, edges=False, after=""):
    return [track for track, _rev in loudness_backfill.candidates(db, SOURCE, after, 100, edges_enabled=edges)]


def test_due_tracks_are_music_without_a_current_record(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al", "m1"), ("t2", "al", "m2"), ("t3", "al", "m3"), ("spoken", "al", "m9", False)])
    assert due(db) == ["t1", "t2", "t3"]
    record = loudness_v1.derive([1e-3] * 50, 0.5, 0.5, method="bs1770-4-native", channels=2, sample_rate=44100)
    with db.cursor() as cur:
        loudness_stream.publish_tracks(cur, SOURCE, [
            {"track_id": "t1", "media_revision": revision("m1"), "record": record},
            {"track_id": "t2", "media_revision": revision("old"), "record": record},  # older media: due again
        ])
    db.commit()
    assert due(db) == ["t2", "t3"]
    assert due(db, after="t2") == ["t3"]
    # With edges on, a track waits for the edge pass, which measures in the same decode.
    _profile(db, "t2", "catalog-media:m2")
    _profile(db, "t3", "catalog-media:m3")
    assert due(db, edges=True) == []
    _edge(db, "t3", "m3")
    _job(db, "t2", revision("m2"), "unsupported")
    assert due(db, edges=True) == ["t2", "t3"]


def test_with_edges_on_a_track_waits_only_while_the_edge_pass_will_decode_it(migrated_db):
    db = migrated_db
    tracks = [("waits", "al", "m1"), ("edge", "al", "m2"), ("no-profile", "al", "m3"), ("unsupported", "al", "m4"),
              ("failed", "al", "m5"), ("enqueue-failed", "al", "m6"), ("legacy", "al", "m7"),
              ("legacy-waits", "al", "m8"), ("legacy-failed", "al", "m9"), ("recheck", "al", "m10")]
    _source(db, tracks)
    for track_id, _album, media_fp in tracks:
        if track_id != "no-profile":
            _profile(db, track_id, media_fp if track_id.startswith("legacy") else "catalog-media:" + media_fp)
    _edge(db, "edge", "m2")
    _job(db, "unsupported", revision("m4"), "unsupported")
    _job(db, "failed", revision("m5"), "failed", "edge-analysis-failed")  # backing off: may never succeed
    _job(db, "enqueue-failed", revision("m6"), "failed", "edge-enqueue-failed")  # re-queued at once
    _edge(db, "legacy", "m7", raw=True)
    _job(db, "legacy-failed", raw_revision("m9"), "failed", "edge-source-replaced")
    _edge(db, "recheck", "m10", recheck=True)  # SF.3: the edge pass decodes it again
    assert sorted(due(db, edges=True)) == ["edge", "failed", "legacy", "legacy-failed", "no-profile", "unsupported"]
    assert sorted(due(db)) == sorted(track for track, _album, _fp in tracks)


def test_failures_back_off_per_media(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al", "m1")])
    loudness_backfill.record_failure(db, SOURCE, "t1", revision("m1"), "boom")
    assert due(db) == []

    def attempt():
        with db.cursor() as cur:
            cur.execute(f"SELECT attempts, round(extract(epoch FROM next_attempt_at - now()) / 3600) "
                        f"FROM {P}loudness_attempts WHERE track_id='t1'")
            row = cur.fetchone()
        db.commit()
        return int(row[0]), int(row[1])

    assert attempt() == (1, 1)
    loudness_backfill.record_failure(db, SOURCE, "t1", revision("m1"), "boom")
    assert attempt() == (2, 6)
    for _ in range(2):
        loudness_backfill.record_failure(db, SOURCE, "t1", revision("m1"), "boom")
    assert attempt() == (4, 72)
    # W3: the failure after the last step parks the track for its media.
    loudness_backfill.record_failure(db, SOURCE, "t1", revision("m1"), "boom")
    assert parked(db, "t1") and due(db) == []
    # New media starts over, and is due at once until it fails.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET media_fp='m1b' WHERE track_id='t1'")
    db.commit()
    assert due(db) == ["t1"]
    loudness_backfill.record_failure(db, SOURCE, "t1", revision("m1b"), "boom")
    assert attempt() == (1, 1)
    with db.cursor() as cur:
        loudness_backfill.clear_failures(cur, SOURCE, ["t1"])
    db.commit()
    assert due(db) == ["t1"]


def parked(db, track_id):
    with db.cursor() as cur:
        cur.execute(f"SELECT next_attempt_at = 'infinity'::timestamp FROM {P}loudness_attempts "
                    "WHERE track_id=%s", (track_id,))
        row = cur.fetchone()
    db.commit()
    return bool(row and row[0])


def test_a_file_too_long_to_measure_is_never_downloaded(migrated_db):
    """W3: the catalogue's duration keeps a file over the meter's limit out of the pass."""
    db = migrated_db
    _source(db, [("t1", "al", "m1"), ("t2", "al", "m2"), ("t3", "al", "m3")])
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET duration_ms=%s WHERE track_id='t1'",
                    (loudness_backfill.MAX_DURATION_MS + 1,))
        cur.execute(f"UPDATE {P}catalog_tracks SET duration_ms=%s WHERE track_id='t2'",
                    (loudness_backfill.MAX_DURATION_MS,))
    db.commit()
    assert due(db) == ["t2", "t3"]
    assert loudness_backfill.parked_counts(db, SOURCE) == {"too_long": 1, "parked": 0}


def test_failures_that_cannot_succeed_park_until_the_media_changes(migrated_db):
    """W3: unsupported parks at once, a decode error at its second attempt."""
    db = migrated_db
    _source(db, [("t1", "al", "m1"), ("t2", "al", "m2"), ("t3", "al", "m3")])
    loudness_backfill.record_failure(db, SOURCE, "t1", revision("m1"),
                                     "edge-analysis-unavailable fmt=dsf", unsupported=True)
    assert parked(db, "t1")
    decode = "edge-media-error fmt=flac phase=decode err=InvalidDataError"
    loudness_backfill.record_failure(db, SOURCE, "t2", revision("m2"), decode)
    assert not parked(db, "t2")
    loudness_backfill.record_failure(db, SOURCE, "t2", revision("m2"), decode)
    assert parked(db, "t2")
    loudness_backfill.record_failure(db, SOURCE, "t3", revision("m3"), "boom")
    assert not parked(db, "t3")
    assert loudness_backfill.parked_counts(db, SOURCE) == {"too_long": 0, "parked": 2}
    assert due(db) == []

    # A new revision of a parked file is due again and starts over.
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET media_fp='m2b' WHERE track_id='t2'")
    db.commit()
    assert due(db) == ["t2"]
    loudness_backfill.record_failure(db, SOURCE, "t2", revision("m2b"), decode)
    assert not parked(db, "t2")


def wav(path, seconds=4.0, gain=0.3, rate=44100):
    t = np.arange(int(seconds * rate)) / rate
    tone = gain * np.sin(2 * np.pi * 440 * t)
    with wave.open(str(path), "wb") as out:
        out.setnchannels(2)
        out.setsampwidth(2)
        out.setframerate(rate)
        out.writeframes(np.round(np.stack([tone, tone]).T * 32767).astype("<i2").tobytes())


@pytest.fixture
def plugin(migrated_db, monkeypatch, tmp_path):
    pytest.importorskip("av")
    mod = load_plugin()
    files = {}
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: False)
    monkeypatch.setattr(mod, "configured_backfill_limit", lambda: 10)
    monkeypatch.setattr(mod, "serve_deferred_interactive", lambda *a, **k: 0)
    monkeypatch.setattr(mod, "run_file_analysis", lambda analyzer, path, **kw: analyzer(path, **kw))

    def load(track_id, catalog_instance_id=None, server_id=None):
        if track_id not in files:
            return None
        return {"file_path": str(files[track_id][0]), "media_signature": "catalog-media:" + files[track_id][1],
                "cleanup_path": None}

    monkeypatch.setattr(mod, "load_track_file", load)
    for track_id, media_fp, gain in (("t1", "m1", 0.3), ("t2", "m2", 0.1)):
        wav(tmp_path / f"{track_id}.wav", gain=gain)
        files[track_id] = (tmp_path / f"{track_id}.wav", media_fp)
    mod.test_files = files
    return mod


def journal_counts(db):
    """Rows in the catalogue's, the profiles' (edges ride on them) and the relationships' journals."""
    with db.cursor() as cur:
        cur.execute(f"SELECT (SELECT count(*) FROM {P}catalog_changes), (SELECT count(*) FROM {P}profile_changes), "
                    f"(SELECT count(*) FROM {P}relationship_changes), (SELECT count(*) FROM {P}analysis_changes)")
        counts = cur.fetchone()
    db.commit()
    return counts


def test_the_pass_follows_the_catalogue_then_measures(migrated_db, plugin, monkeypatch):
    db = migrated_db
    _source(db, [("t1", "al", "m1"), ("t2", "al", "m2"), ("t3", "al", "m3")])  # t3 has no file
    loudness_backfill.ensure_sources(db)
    assert loudness_backfill.next_due(db) == ("server-a", SOURCE)
    other_journals = journal_counts(db)
    publications = []
    publish = loudness_stream.publish_tracks
    monkeypatch.setattr(loudness_stream, "publish_tracks", lambda cur, source, measured: (
        publications.append([item["track_id"] for item in measured]) or publish(cur, source, measured)))
    first = plugin.loudness_backfill_task("server-a", SOURCE)
    # The whole library fits in the turn (WP1a): it reaches the end and finishes the pass.
    assert (first["status"], first["ready"], first["failed"]) == ("complete", 2, 1)
    assert publications == [["t1", "t2"]]  # the page publishes once
    page = loudness_stream.bootstrap_page(db, SOURCE)
    assert [i["track_id"] for i in page["items"] if i["kind"] == "track"] == ["t1", "t2"]
    assert all(i["method"] == "bs1770-4-native" for i in page["items"] if i["kind"] == "track")
    assert not [i for i in page["items"] if i["kind"] == "album"]  # t3 is still unmeasured
    # The failed track backs off; the pass re-sweeps later.
    assert loudness_backfill.state(db, SOURCE)["cursor"] == ""
    assert loudness_backfill.next_due(db) is None
    # The catalogue moves: t3 leaves, the album is complete and published, without waiting for the re-sweep.
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {P}catalog_tracks WHERE track_id='t3'")
        cur.execute(f"UPDATE {P}catalog_state SET published_generation=2")
        cur.execute(f"UPDATE {P}catalog_tracks SET published_generation=2")
    db.commit()
    assert loudness_backfill.next_due(db) == ("server-a", SOURCE)
    third = plugin.loudness_backfill_task("server-a", SOURCE)
    assert third["status"] == "current" and third["reconciled"] == 1
    album = [i for i in loudness_stream.bootstrap_page(db, SOURCE)["items"] if i["kind"] == "album"]
    assert album and album[0]["members"] == 2
    assert loudness_backfill.next_due(db) is None
    with db.cursor() as cur:  # t3's back-off went with it
        cur.execute(f"SELECT count(*) FROM {P}loudness_attempts")
        assert cur.fetchone()[0] == 0
    db.commit()
    # Loudness records only: no catalogue, profile, edge or relationship event.
    assert journal_counts(db) == other_journals


def test_a_batch_that_fails_is_published_one_track_at_a_time(migrated_db, plugin, monkeypatch):
    db = migrated_db
    _source(db, [("t1", "al", "m1"), ("t2", "al", "m2")])
    loudness_backfill.ensure_sources(db)
    loudness_backfill.record_failure(db, SOURCE, "t1", revision("old"), "boom")  # older media's back-off
    publish = loudness_stream.publish_tracks

    def refuse_t2(cur, source, measured):
        if any(item["track_id"] == "t2" for item in measured):
            raise ValueError("refused")
        return publish(cur, source, measured)

    monkeypatch.setattr(loudness_stream, "publish_tracks", refuse_t2)
    result = plugin.loudness_backfill_task("server-a", SOURCE)
    assert (result["status"], result["ready"], result["failed"], result["processed"]) == ("complete", 1, 1, 2)
    assert [i["track_id"] for i in loudness_stream.bootstrap_page(db, SOURCE)["items"]] == ["t1"]
    with db.cursor() as cur:
        cur.execute(f"SELECT track_id, attempts, last_error FROM {P}loudness_attempts ORDER BY track_id")
        assert cur.fetchall() == [("t2", 1, "loudness-publication-failed")]
    db.commit()
    assert due(db) == []  # t2 backs off


def test_a_turn_pages_at_the_batch_size_and_keeps_to_the_current_media(migrated_db, plugin, monkeypatch):
    db = migrated_db
    _source(db, [("t1", "al", "m1"), ("t2", "al", "m2")])
    loudness_backfill.ensure_sources(db)
    monkeypatch.setattr(plugin, "configured_backfill_limit", lambda: 1)
    plugin.test_files["t1"] = (plugin.test_files["t1"][0], "m1-replaced")  # the file is newer media now
    pages = []
    monkeypatch.setattr(loudness_backfill, "candidates", lambda *a, _read=loudness_backfill.candidates, **k: (
        pages.append(a[2]) or _read(*a, **k)))
    first = plugin.loudness_backfill_task("server-a", SOURCE)
    # One track a page, read from the cursor, until the end (WP1a: no longer one page a turn).
    assert pages == ["", "t1", "t2"]
    assert (first["status"], first["ready"], first["failed"]) == ("complete", 1, 1)
    assert [i["track_id"] for i in loudness_stream.bootstrap_page(db, SOURCE)["items"]] == ["t2"]


def test_a_turn_stops_at_the_tick_deadline_and_keeps_its_cursor(migrated_db, plugin, monkeypatch):
    db = migrated_db
    _source(db, [("t1", "al", "m1"), ("t2", "al", "m2")])
    loudness_backfill.ensure_sources(db)
    clock = [100.0]
    monkeypatch.setattr(plugin, "time", SimpleNamespace(monotonic=lambda: clock[0], time=time.time, sleep=time.sleep))
    measure = plugin._measure_loudness

    def slow(*args):
        clock[0] += 10  # each measurement takes 10 s
        return measure(*args)

    monkeypatch.setattr(plugin, "_measure_loudness", slow)
    first = plugin.loudness_backfill_task("server-a", SOURCE, deadline=105.0)
    assert (first["status"], first["ready"], first["failed"]) == ("queued", 1, 0)
    assert loudness_backfill.state(db, SOURCE)["cursor"] == "t1"
    # Outside a tick a turn gets a tick's budget.
    second = plugin.loudness_backfill_task("server-a", SOURCE)
    assert (second["status"], second["ready"]) == ("complete", 1)


def test_a_turn_that_raises_marks_the_pass_failed(migrated_db, plugin, monkeypatch):
    db = migrated_db
    _source(db, [("t1", "al", "m1")])
    loudness_backfill.ensure_sources(db)
    monkeypatch.setattr(loudness_backfill, "candidates", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        plugin.loudness_backfill_task("server-a", SOURCE)
    assert loudness_backfill.state(db, SOURCE)["status"] == "failed"
    assert db.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE


def test_a_failing_catalogue_follow_backs_off(migrated_db):
    db = migrated_db
    _source(db, [("t1", "al", "m1")])
    loudness_backfill.ensure_sources(db)
    with db.cursor() as cur:  # the follow failed; the watchdog set the retry an hour out
        cur.execute(f"UPDATE {P}loudness_backfill_state SET status='failed', next_retry_at=now() + interval '1 hour'")
    db.commit()
    assert loudness_backfill.next_due(db) is None
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}loudness_backfill_state SET next_retry_at=now() - interval '1 second'")
    db.commit()
    assert loudness_backfill.next_due(db) == ("server-a", SOURCE)


def test_a_worker_without_a_decoder_parks_before_downloading(migrated_db, plugin, monkeypatch):
    db = migrated_db
    _source(db, [("t1", "al", "m1")])
    loudness_backfill.ensure_sources(db)
    monkeypatch.setattr(loudness_backfill, "runtime_available", lambda: False)
    monkeypatch.setattr(plugin, "load_track_file", lambda *a, **k: pytest.fail("downloaded without a decoder"))
    result = plugin.loudness_backfill_task("server-a", SOURCE)
    assert (result["status"], result["reason"]) == ("paused", "loudness_runtime_unavailable")
    assert loudness_backfill.state(db, SOURCE)["status"] == "waiting_runtime"
    assert due(db) == ["t1"]  # not counted as the track's failure
    # A worker's runtime report resumes a parked pass, not a finished one.
    assert loudness_backfill.arm(db, include_complete=False) == 1
    assert loudness_backfill.state(db, SOURCE)["status"] == "queued"


def test_the_scheduler_knows_the_action():
    assert reconcile.action_counts("loudness_backfill", {"ready": 2, "failed": 1}) == (2, 1)
    assert reconcile._retry_target("loudness_backfill") == "loudness_backfill_state"
    assert reconcile.ACTION_UNITS["loudness_backfill"] == ("loudness measurement", "loudness measurements")
    mod = load_plugin()
    assert "loudness_backfill" in mod._COMPLETE_SETTLES
