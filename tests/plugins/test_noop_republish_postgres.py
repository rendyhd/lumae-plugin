"""P1-1 / AUD-03: a no-op re-analysis must not republish or delete edges.

Every test runs on the production schema (``migrated_db``).
"""
from types import SimpleNamespace

import numpy as np
import pytest

from test_lumae_analysis import _edge_payload_for_job, load_plugin
from plugins.LumaeAnalysis import catalog_enrichment as enrichment
from plugins.LumaeAnalysis import edge_profile_store as store
from plugins.LumaeAnalysis import profile_publication as publication
from plugins.LumaeAnalysis.catalog import opaque_cursor
from plugins.LumaeAnalysis.edge_profiles import opaque_revision


SOURCE = "catalog-a"
SERVER = "legacy-default"  # what the v2 core adapter reports for song hooks
P = "plugin_lumae_analysis__"
LUFS64 = -14.123456789  # the analyzer's float64; REAL stores -14.123457


def _seed(db, *tracks):
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type,
                 server_name, is_default, rebind_status)
                VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')""",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_state
                (catalog_instance_id, current_core_server_id, provider_type,
                 published_generation, catalog_epoch, status)
                VALUES (%s, %s, 'navidrome', 1, 'epoch-a', 'complete')""",
            (SOURCE, SERVER),
        )
    db.commit()
    for track in tracks:
        _set_media(db, track, "rev-a")


def _set_media(db, track, fp):
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}catalog_tracks
                (catalog_instance_id, published_generation, track_id, title,
                 metadata_fp, media_fp, analysis_eligible, payload,
                 first_seen_at, last_seen_at)
                VALUES (%s, 1, %s, %s, 'metadata', %s, TRUE, '{{}}'::jsonb, now(), now())
                ON CONFLICT (catalog_instance_id, published_generation, track_id)
                DO UPDATE SET media_fp=EXCLUDED.media_fp""",
            (SOURCE, track, track, fp),
        )
    db.commit()


def _result(ref_lufs=LUFS64, start=b"wave", end=b"tail"):
    return SimpleNamespace(
        sample_rate=48000, duration_ms=1234, ref_lufs=ref_lufs,
        start_ramp_blob=start, end_ramp_blob=end,
    )


def _complete(db, track, result, fp="rev-a"):
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    assert publication.complete_attempt(
        db, SOURCE, track, token, result, "ready", None,
        f"catalog-media:{fp}", 1, 1,
    )


def _publish_edge(db, track, fp="rev-a"):
    jobs, ready = store.claim_edge_jobs(db, SOURCE, [track])
    assert ready == [] and len(jobs) == 1
    job = jobs[0]
    payload = _edge_payload_for_job(job)
    assert store.update_edge_job(db, SOURCE, job, "running")
    assert store.publish_edge_profile(db, SOURCE, job, payload, f"catalog-media:{fp}")
    return payload


def _fetch(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _snapshot(db, track):
    head = _fetch(
        db, f"SELECT head_seq FROM {P}profile_stream_state WHERE catalog_instance_id=%s",
        (SOURCE,),
    )[0][0]
    published = _fetch(
        db,
        f"""SELECT sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                   analyzer_ver, profile_schema_ver, media_signature, analyzed_at
              FROM {P}published_source_profiles
             WHERE catalog_instance_id=%s AND track_id=%s""",
        (SOURCE, track),
    )
    edges = _fetch(
        db,
        f"""SELECT media_revision, representation_id, media_signature,
                   profile_digest, payload, updated_at
              FROM {P}edge_profiles WHERE catalog_instance_id=%s AND track_id=%s""",
        (SOURCE, track),
    )
    jobs = _fetch(
        db,
        f"""SELECT media_revision, job_token, status, last_error, updated_at
              FROM {P}edge_profile_jobs WHERE catalog_instance_id=%s AND track_id=%s""",
        (SOURCE, track),
    )
    events = _fetch(
        db,
        f"""SELECT seq, operation, payload FROM {P}profile_changes
             WHERE catalog_instance_id=%s AND track_id=%s ORDER BY seq""",
        (SOURCE, track),
    )
    published = [tuple(bytes(v) if isinstance(v, memoryview) else v for v in row)
                 for row in published]
    return {"head": head, "published": published, "edges": edges,
            "jobs": jobs, "events": events}


@pytest.fixture
def source_db(migrated_db, monkeypatch):
    mod = load_plugin()
    _seed(migrated_db, "track-a")
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    monkeypatch.setattr(
        mod, "resolve_profile_source",
        lambda **_: {"catalog_instance_id": SOURCE, "server_id": SERVER},
    )
    return migrated_db


def test_identical_completion_keeps_head_published_row_and_edge(source_db):
    db = source_db
    _complete(db, "track-a", _result())
    _publish_edge(db, "track-a")
    before = _snapshot(db, "track-a")
    assert before["head"] == 2 and len(before["edges"]) == 1 and len(before["jobs"]) == 1

    _complete(db, "track-a", _result())

    assert _snapshot(db, "track-a") == before


def test_float64_and_float4_ref_lufs_are_the_same_profile(source_db):
    db = source_db
    stored = float(np.float32(LUFS64))
    _complete(db, "track-a", _result(ref_lufs=stored))
    _publish_edge(db, "track-a")
    before = _snapshot(db, "track-a")

    _complete(db, "track-a", _result(ref_lufs=LUFS64))
    _complete(db, "track-a", _result(ref_lufs=-14.123457))

    assert _snapshot(db, "track-a") == before


def test_waveform_change_on_same_media_keeps_edge_and_embeds_it(source_db):
    db = source_db
    _complete(db, "track-a", _result())
    edge = _publish_edge(db, "track-a")
    before = _snapshot(db, "track-a")

    _complete(db, "track-a", _result(start=b"changed-wave"))

    after = _snapshot(db, "track-a")
    assert after["head"] == before["head"] + 1
    assert after["edges"] == before["edges"]
    assert after["jobs"] == before["jobs"]
    new_events = after["events"][len(before["events"]):]
    assert len(new_events) == 1
    seq, operation, payload = new_events[0]
    assert operation == "upsert"
    # K6 (P3-2): the journal references the kept edge instead of copying it,
    # and /changes serves the event with the edge embedded.
    assert "edge_profile" not in payload
    assert _fetch(db, f"SELECT edge_ref FROM {P}profile_changes "
                      "WHERE catalog_instance_id=%s AND seq=%s", (SOURCE, seq))[0][0] == {
        "profile_digest": edge["profile_digest"], "kept": True}
    epoch = _fetch(db, f"SELECT epoch FROM {P}profile_stream_state "
                       "WHERE catalog_instance_id=%s", (SOURCE,))[0][0]
    served = enrichment.read_profile_changes(db, opaque_cursor(SOURCE, epoch, seq - 1), SOURCE)
    db.commit()
    payload = served["changes"][0]["payload"]
    assert payload["edge_profile"] == edge
    assert payload["edge_profile"]["media_revision"] == opaque_revision("catalog-media:rev-a")
    # The edge stays current, so the follow-up upgrade request is a no-op.
    assert store.claim_edge_jobs(db, SOURCE, ["track-a"]) == ([], ["track-a"])


def test_media_change_deletes_edge_and_schedules_an_upgrade(source_db):
    db = source_db
    _complete(db, "track-a", _result())
    _publish_edge(db, "track-a")
    _set_media(db, "track-a", "rev-b")

    _complete(db, "track-a", _result(), fp="rev-b")

    after = _snapshot(db, "track-a")
    assert after["edges"] == [] and after["jobs"] == []
    assert after["published"][0][7] == "catalog-media:rev-b"
    operation, payload = after["events"][-1][1:]
    assert operation == "upsert" and "edge_profile" not in payload
    jobs, ready = store.claim_edge_jobs(db, SOURCE, ["track-a"])
    assert ready == []
    assert [job["media_revision"] for job in jobs] == [opaque_revision("catalog-media:rev-b")]


def test_event_payload_equals_bootstrap_and_direct_read(source_db):
    mod = load_plugin()
    db = source_db
    _set_media(db, "track-b", "rev-a")
    _complete(db, "track-a", _result())
    _publish_edge(db, "track-a")
    _complete(db, "track-a", _result(start=b"changed-wave"))  # event with edge
    _complete(db, "track-b", _result(ref_lufs=-9.87654321, end=b""))  # no edge

    epoch = _fetch(
        db, f"SELECT epoch FROM {P}profile_stream_state WHERE catalog_instance_id=%s",
        (SOURCE,),
    )[0][0]
    changes = enrichment.read_profile_changes(db, opaque_cursor(SOURCE, epoch, 0), SOURCE)
    db.commit()
    latest = {}
    for change in changes["changes"]:
        latest[change["track_id"]] = change["payload"]
    bootstrap = {
        row["track_id"]: row
        for row in enrichment.profile_bootstrap_page(db, SOURCE)["profiles"]
    }
    db.commit()
    direct = {
        row["track_id"]: mod.serialize_ready_profile(row)
        for row in mod.fetch_published_profile_rows(["track-a", "track-b"], SOURCE)
    }
    db.commit()

    assert set(latest) == {"track-a", "track-b"}
    assert "edge_profile" in latest["track-a"]
    for track in ("track-a", "track-b"):
        assert latest[track] == bootstrap[track] == direct[track]
    # float4 precision, as a decimal that round-trips the float4 value.
    assert latest["track-b"]["ref_lufs"] == -9.876543
    assert np.float32(latest["track-b"]["ref_lufs"]) == np.float32(-9.87654321)
    assert latest["track-b"]["end_ramp"] == ""


def test_probe_identical_reanalysis_no_longer_republishes_or_drops_edge(source_db):
    """Inverted port of the audit integrity probe (AUD-03)."""
    db = source_db
    res = SimpleNamespace(sample_rate=48000, duration_ms=1234, ref_lufs=-14.123456789,
                          start_ramp_blob=b"wave", end_ramp_blob=b"tail")
    tok = publication.admit_attempts(db, SOURCE, ["track-a"])["track-a"]
    assert publication.complete_attempt(db, SOURCE, "track-a", tok, res, "ready", None,
                                        "catalog-media:rev-a", 1, 1)
    with db.cursor() as cur:
        cur.execute(f"""INSERT INTO {P}edge_profiles
            (catalog_instance_id, track_id, media_revision, representation_id,
             media_signature, profile_digest, payload)
            VALUES (%s, 'track-a', 'r', 'rep', 'catalog-media:rev-a', 'd', '{{}}'::jsonb)""",
                    (SOURCE,))
        cur.execute(f"SELECT head_seq FROM {P}profile_stream_state")
        head_before = cur.fetchone()[0]
    db.commit()
    tok = publication.admit_attempts(db, SOURCE, ["track-a"])["track-a"]
    assert publication.complete_attempt(db, SOURCE, "track-a", tok, res, "ready", None,
                                        "catalog-media:rev-a", 1, 1)
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {P}profile_stream_state")
        head_after = cur.fetchone()[0]
        cur.execute(f"SELECT count(*) FROM {P}edge_profiles WHERE track_id='track-a'")
        edges = cur.fetchone()[0]
    db.commit()
    assert head_after == head_before and edges == 1


# --- analyze_song_hook: skip admission when the published row is current ---


def _hook(mod, monkeypatch, tmp_path, real_edge_upgrade=False):
    audio = tmp_path / "hook.flac"
    audio.write_bytes(b"audio")
    seen = {"mark_pending": 0, "analyzed": 0}
    real_mark_pending = mod.mark_pending

    def mark_pending(*args, **kwargs):
        seen["mark_pending"] += 1
        return real_mark_pending(*args, **kwargs)

    def analyze_file(_path):
        seen["analyzed"] += 1
        return _result()

    monkeypatch.setattr(mod, "mark_pending", mark_pending)
    monkeypatch.setattr(mod, "analyze_file", analyze_file)
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "arm_profile_claim_recovery", lambda *a, **k: False)
    if real_edge_upgrade:
        seen["edge_batches"] = []
        monkeypatch.setattr(mod, "edge_profiles_enabled", lambda: True)
        monkeypatch.setattr(
            mod, "enqueue_bounded",
            lambda _task, jobs, *a, **k: seen["edge_batches"].append(
                [job["track_id"] for job in jobs]),
        )
    else:
        monkeypatch.setattr(mod, "_schedule_edge_upgrade", lambda *a, **k: None)
    outcome = mod.analyze_song_hook({"item_id": "track-a", "audio_path": str(audio)})
    return outcome, seen


def test_song_hook_skips_admission_when_published_row_is_current(
    source_db, monkeypatch, tmp_path,
):
    mod = load_plugin()
    db = source_db
    _complete(db, "track-a", _result())
    _publish_edge(db, "track-a")
    before = _snapshot(db, "track-a")
    status_before = _fetch(
        db, f"SELECT status, attempt_token FROM {P}source_profiles WHERE track_id='track-a'",
    )

    outcome, seen = _hook(mod, monkeypatch, tmp_path)

    assert outcome == {"track_id": "track-a", "status": "current"}
    assert seen == {"mark_pending": 0, "analyzed": 0}
    assert _snapshot(db, "track-a") == before
    assert _fetch(
        db, f"SELECT status, attempt_token FROM {P}source_profiles WHERE track_id='track-a'",
    ) == status_before


@pytest.mark.parametrize("change", ["media", "analyzer", "failed", "unpublished"])
def test_song_hook_still_requalifies_stale_or_failed_rows(
    source_db, monkeypatch, tmp_path, change,
):
    mod = load_plugin()
    db = source_db
    if change != "unpublished":
        _complete(db, "track-a", _result())
    with db.cursor() as cur:
        if change == "media":
            cur.execute(f"UPDATE {P}catalog_tracks SET media_fp='rev-b' WHERE track_id='track-a'")
        elif change == "analyzer":
            cur.execute(f"UPDATE {P}published_source_profiles SET analyzer_ver=0")
        elif change == "failed":
            cur.execute(f"UPDATE {P}source_profiles SET status='failed'")
    db.commit()

    outcome, seen = _hook(mod, monkeypatch, tmp_path)

    assert seen == {"mark_pending": 1, "analyzed": 1}
    assert outcome == {"track_id": "track-a", "status": "ready"}


def test_serializer_emits_float4_ref_lufs_on_every_path():
    payload = enrichment.serialize_profile(
        "t", 48000, 1, LUFS64, b"a", b"", 1, "2026-09-24T00:00:00", "sig",
    )
    assert payload["ref_lufs"] == -14.123457
    assert payload["end_ramp"] == ""
    assert enrichment.serialize_profile(
        "t", 48000, 1, float("nan"), b"", b"", 1, "2026-09-24T00:00:00", "sig",
    )["ref_lufs"] is None


def test_song_hook_on_current_row_heals_a_missing_edge(source_db, monkeypatch, tmp_path):
    mod = load_plugin()
    db = source_db
    _complete(db, "track-a", _result())
    assert _snapshot(db, "track-a")["jobs"] == []

    outcome, seen = _hook(mod, monkeypatch, tmp_path, real_edge_upgrade=True)

    assert outcome == {"track_id": "track-a", "status": "current"}
    assert seen["mark_pending"] == 0 and seen["analyzed"] == 0
    assert seen["edge_batches"] == [["track-a"]]
    jobs = _snapshot(db, "track-a")["jobs"]
    assert [(job[0], job[2]) for job in jobs] == [
        (opaque_revision("catalog-media:rev-a"), "pending")
    ]


def test_song_hook_on_current_row_with_current_edge_requests_no_job(
    source_db, monkeypatch, tmp_path,
):
    mod = load_plugin()
    db = source_db
    _complete(db, "track-a", _result())
    _publish_edge(db, "track-a")
    before = _snapshot(db, "track-a")

    outcome, seen = _hook(mod, monkeypatch, tmp_path, real_edge_upgrade=True)

    assert outcome == {"track_id": "track-a", "status": "current"}
    assert seen["edge_batches"] == []
    assert _snapshot(db, "track-a") == before
