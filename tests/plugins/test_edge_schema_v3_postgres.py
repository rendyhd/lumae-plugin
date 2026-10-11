"""SF.3 (contract §4.5): edge profile schema v3 on every profile transfer.

The stored edge stays the v2 payload; v3 is its projection (no ``peak_cdb``,
no ``spectral_flux_q15``, its own digest), plus the whole-file music end (D9)
stored beside it. A client asks with ``edge_schema=3`` (``edge_schema: 3`` on
a v2 session create); v2 transfers are unchanged. Health lists schema 3 once
every stored edge has its v3 digest; the upgrade pass computes those for
edges stored before 1.7.0 and flags tails that open on silence for a recheck.

Runs on the real migrated schema, through the production publication paths
and the HTTP routes (fixtures shared with the K6 suite).
"""

import numpy as np
import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import load_plugin
from test_profile_stream_edge_refs_postgres import (  # noqa: F401  (source_db is a fixture)
    P,
    PROFILE_EPOCH,
    SOURCE,
    TRACKS,
    _app,
    _catchup_events,
    _changes,
    _complete,
    _events,
    _execute,
    _post,
    _publish_edge,
    _stored_edge,
    _v2_body,
    _v2_create,
    _v2_pages,
    source_db,
)
from plugins.LumaeAnalysis import edge_profile_store as store
from plugins.LumaeAnalysis.catalog import opaque_cursor
from plugins.LumaeAnalysis.edge_profiles import (
    V3_ENDING_KEY,
    analyze_edge_blocks,
    edge_profile_v3,
    profile_digest,
)


def _v3(db, track):
    """The v3 projection of ``track``'s stored edge, with its stored music end."""
    row = _execute(db, f"SELECT payload, v3_ending FROM {P}edge_profiles "
                       "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, track))[0]
    return edge_profile_v3(row[0], row[1])


def _get(client, path, **query):
    return client.get(path, query_string={"catalog_instance_id": SOURCE, **query})


def _profiles(client, ids, **query):
    response = _get(client, "/api/profiles", ids=",".join(ids), **query)
    assert response.status_code == 200, response.data[:300]
    return {row["track_id"]: row for row in response.get_json()["profiles"]}


def _health_versions(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "integrity_status", lambda *_args, **_kwargs: None)
    body = _app().get("/api/health").get_json()
    return body["capabilities"]["edge_profiles"]["schema_versions"]


def _assert_v3(edge, expected):
    assert edge == expected
    assert edge["schema_version"] == 3
    for name in ("head", "tail"):
        assert "peak_cdb" not in edge[name] and "spectral_flux_q15" not in edge[name]
    assert edge["profile_digest"] == profile_digest(edge)


def test_health_offers_schema_3_once_every_edge_has_its_v3_digest(source_db, monkeypatch):
    db = source_db
    for track in TRACKS[:3]:
        _complete(db, track, -14.0)
        _publish_edge(db, track)
    assert _health_versions(monkeypatch) == [2, 3]
    # Edges stored before 1.7.0 have no v3 digest: schema 3 waits for them.
    _execute(db, f"UPDATE {P}edge_profiles SET profile_digest_v3=NULL WHERE track_id<>%s",
             (TRACKS[0],))
    assert _health_versions(monkeypatch) == [2]
    assert not store.edge_schema_v3_ready(db)
    assert store.upgrade_stored_edges(db, batch_size=1) == (2, 0)
    assert _health_versions(monkeypatch) == [2, 3]
    for track in TRACKS[:3]:
        digest = _execute(db, f"SELECT profile_digest_v3 FROM {P}edge_profiles "
                              "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, track))[0][0]
        assert digest == edge_profile_v3(_stored_edge(db, track))["profile_digest"]
    # Done: nothing left to upgrade.
    assert store.upgrade_stored_edges(db) == (0, 0)


def test_every_transfer_serves_v3_on_request_and_v2_unchanged(source_db):
    db, client = source_db, _app()
    for track in TRACKS[:3]:
        _complete(db, track, -14.0)
        _publish_edge(db, track)
    ids = TRACKS[:3]
    plain = _get(client, "/api/profiles", ids=",".join(ids))
    for value in ("2", ""):
        assert _get(client, "/api/profiles", ids=",".join(ids), edge_schema=value).data == plain.data
    v2 = _profiles(client, ids)
    v3 = _profiles(client, ids, edge_schema="3")
    for track in ids:
        assert v2[track]["edge_profile"] == _stored_edge(db, track)
        _assert_v3(v3[track]["edge_profile"], _v3(db, track))
        assert {k: v for k, v in v3[track].items() if k != "edge_profile"} == \
            {k: v for k, v in v2[track].items() if k != "edge_profile"}
    for path in ("/api/profiles", "/api/profiles/bootstrap", "/api/profiles/changes"):
        for value in ("4", "v3", "3.0", "1"):
            response = _get(client, path, ids="t01", cursor="x", edge_schema=value)
            assert response.status_code == 400, (path, value)
            assert response.get_json()["error"] == "invalid_edge_schema"
    # Legacy bootstrap pages.
    pages = {}
    for schema in (None, "3"):
        query = {} if schema is None else {"edge_schema": schema}
        body = _get(client, "/api/profiles/bootstrap", limit=500, **query).get_json()
        pages[schema] = {row["track_id"]: row for row in body["profiles"]}
    for track in ids:
        assert pages[None][track]["edge_profile"] == _stored_edge(db, track)
        _assert_v3(pages["3"][track]["edge_profile"], _v3(db, track))
    # /changes, with and without references.
    v2_events = _events(client, 0)
    cursor = opaque_cursor(SOURCE, PROFILE_EPOCH, 0)
    v3_pages = _changes(client, cursor, 4, edge_schema="3")
    v3_events = {c["seq"]: c for _s, _r, body in v3_pages for c in body["changes"]}
    assert v2_events.keys() == v3_events.keys()
    for seq, change in v3_events.items():
        edge = change["payload"] and change["payload"].get("edge_profile")
        if edge:
            _assert_v3(edge, _v3(db, change["track_id"]))
            assert v2_events[seq]["payload"]["edge_profile"]["schema_version"] == 2


def test_v3_references_name_the_v3_digest(source_db):
    db, client, track = source_db, _app(), TRACKS[0]
    _complete(db, track, -14.0)
    _publish_edge(db, track)
    plain = _v2_create(client, 2, edge_refs=True)[0][2]
    v3_session = _v2_create(client, 2, edge_refs=True, edge_schema=3)[0][2]
    assert "edge_schema" not in plain and v3_session["edge_schema"] == 3
    _complete(db, track, -13.0, start=b"wave-2")  # kept: served as a reference
    expected = _v3(db, track)
    v2_refs = _events(client, 0, edge_refs="1")
    cursor = opaque_cursor(SOURCE, PROFILE_EPOCH, 0)
    v3_refs = {c["seq"]: c for _s, _r, body in _changes(client, cursor, 4, edge_refs="1",
                                                        edge_schema="3")
               for c in body["changes"]}
    kept = max(v2_refs)
    assert v2_refs[kept]["payload"]["edge_profile_ref"]["profile_digest"] == \
        _stored_edge(db, track)["profile_digest"]
    assert v3_refs[kept]["payload"]["edge_profile_ref"] == {
        "media_revision": expected["media_revision"], "profile_digest": expected["profile_digest"]}
    # The session keeps its schema for its snapshot pages and catch-up.
    snapshot = [row for _s, _r, body in _v2_pages(client, v3_session["session_token"], "/page")
                for row in body["profiles"]]
    _assert_v3(snapshot[0]["edge_profile"], expected)
    catchup = _catchup_events(client, v3_session["session_token"])
    assert catchup[kept]["payload"]["edge_profile_ref"]["profile_digest"] == expected["profile_digest"]
    plain_catchup = _catchup_events(client, plain["session_token"])
    assert plain_catchup[kept]["payload"]["edge_profile_ref"]["profile_digest"] == \
        _stored_edge(db, track)["profile_digest"]


@pytest.mark.parametrize("value", [4, 1, "3", 3.0, True, [], {}])
def test_v2_create_rejects_another_edge_schema(source_db, value):
    response = _post(_app(), "", _v2_body(page_size=10, edge_schema=value))
    assert response.status_code == 400
    assert response.get_json()["error"] == "invalid_edge_schema"


def test_v2_create_takes_null_and_2_as_v2(source_db):
    client = _app()
    for value in (None, 2):
        created = _v2_create(client, 10, edge_schema=value)[0][2]
        assert "edge_schema" not in created
        assert _execute(source_db, f"SELECT edge_schema FROM {P}profile_bootstrap_sessions "
                                   "WHERE token_hash=encode(sha256(convert_to(%s, 'UTF8')), "
                                   "'hex')", (created["session_token"],)) == [(2,)]


def _hidden_outro_edge(job):
    """A 100 s song: music to 40 s, a silence from before the tail window, a quiet outro."""
    rate = 48000
    t = np.arange(rate * 100, dtype=np.float64) / rate
    pcm = np.where(t < 40, .25 * np.sin(2 * np.pi * 220 * t),
                   np.where((t >= 80) & (t < 90), .01 * np.sin(2 * np.pi * 440 * t), 0.0))
    blocks = [pcm[i:i + 65536].reshape(1, -1).astype(np.float32) for i in range(0, len(pcm), 65536)]
    return analyze_edge_blocks(blocks, rate, catalog_instance_id=SOURCE, track_id=job["track_id"],
                               media_revision=job["media_revision"], content_sha256="d" * 64,
                               channel_layout="mono", timeline_verified=True)


def test_the_music_end_is_stored_beside_the_v2_edge_and_served_only_in_v3(source_db):
    db, client, track = source_db, _app(), TRACKS[3]
    _complete(db, track, -14.0)
    jobs, _ready = store.claim_edge_jobs(db, SOURCE, [track])
    job = jobs[0]
    assert store.update_edge_job(db, SOURCE, job, "running")
    payload = _hidden_outro_edge(job)
    ending = payload[V3_ENDING_KEY]
    assert store.publish_edge_profile(db, SOURCE, job, payload, f"catalog-media:m1-{track}")
    stored = _stored_edge(db, track)
    assert V3_ENDING_KEY not in stored and "ending" not in stored
    assert stored == {k: v for k, v in payload.items() if k != V3_ENDING_KEY}
    v2 = _profiles(client, [track])[track]["edge_profile"]
    assert v2 == stored and "music_end_frame" not in v2["landmarks"]
    v3 = _profiles(client, [track], edge_schema="3")[track]["edge_profile"]
    _assert_v3(v3, edge_profile_v3(stored, ending))
    assert v3["landmarks"]["music_end_frame"] == ending["music_end_frame"]
    assert v3["ending"] == ending["ending"]


def _run_out_edge(job):
    """Measured like a 1.6 edge (no second pass): music to 5 s of 40 s."""
    rate = 48000
    t = np.arange(rate * 40, dtype=np.float64) / rate
    pcm = np.where(t < 5, .25 * np.sin(2 * np.pi * 220 * t), 0.0).reshape(1, -1).astype(np.float32)
    return analyze_edge_blocks(iter([pcm]), rate, catalog_instance_id=SOURCE, track_id=job["track_id"],
                               media_revision=job["media_revision"], content_sha256="e" * 64,
                               channel_layout="mono", timeline_verified=True)


def test_the_upgrade_flags_a_tail_that_opens_on_silence_for_a_recheck(source_db):
    db, track, other = source_db, TRACKS[4], TRACKS[5]
    for name in (track, other):
        _complete(db, name, -14.0)
    jobs, _ready = store.claim_edge_jobs(db, SOURCE, [track])
    assert store.update_edge_job(db, SOURCE, jobs[0], "running")
    assert store.publish_edge_profile(db, SOURCE, jobs[0], _run_out_edge(jobs[0]),
                                      f"catalog-media:m1-{track}")
    _publish_edge(db, other)
    # Both as stored by 1.6, a while ago.
    _execute(db, f"UPDATE {P}edge_profiles SET profile_digest_v3=NULL")
    _execute(db, f"UPDATE {P}edge_profile_jobs SET updated_at=now()-interval '7 hours'")
    assert store.upgrade_stored_edges(db) == (2, 1)
    flags = dict(_execute(db, f"SELECT track_id, v3_recheck FROM {P}edge_profiles"))
    assert flags == {track: True, other: False}
    # The library pass reaches it, and a claim measures it again.
    assert store.edge_backfill_candidates(db, SOURCE) == [track]
    jobs, ready = store.claim_edge_jobs(db, SOURCE, [track, other])
    assert [job["track_id"] for job in jobs] == [track] and ready == [other]
    # The new measurement (with the second pass) clears the flag.
    assert store.update_edge_job(db, SOURCE, jobs[0], "running")
    rate = 48000
    t = np.arange(rate * 40, dtype=np.float64) / rate
    pcm = np.where(t < 5, .25 * np.sin(2 * np.pi * 220 * t), 0.0).reshape(1, -1).astype(np.float32)
    payload = analyze_edge_blocks([pcm], rate, catalog_instance_id=SOURCE, track_id=track,
                                  media_revision=jobs[0]["media_revision"], content_sha256="e" * 64,
                                  channel_layout="mono", timeline_verified=True)
    assert V3_ENDING_KEY in payload
    assert store.publish_edge_profile(db, SOURCE, jobs[0], payload, f"catalog-media:m1-{track}")
    assert dict(_execute(db, f"SELECT track_id, v3_recheck FROM {P}edge_profiles")) == \
        {track: False, other: False}
    assert store.edge_backfill_candidates(db, SOURCE) == []
