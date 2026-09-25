"""P1-5 (AUD-02, K2): v2 snapshots and catch-ups store edge references.

A capture stores each row's waveform payload plus ``edge_ref``, a reference
to the edge row (media_revision, profile_digest); the media_revision is stored
only when it differs from the row's own. Pages resolve the reference against
``edge_profiles``. Pages stay byte-identical to the pre-K2 capture, which
embedded the edge, except that an edge replaced or withdrawn after capture is
absent (the catch-up carries the replacing event). Runs on the real migrated
schema.
"""

import json
import os
import time
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import plugin_api_module
from plugins.LumaeAnalysis import catalog, catalog_enrichment, profile_bootstrap
from plugins.LumaeAnalysis.edge_profile_store import edge_join
from plugins.LumaeAnalysis.edge_profiles import opaque_revision, profile_digest


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
PUBLISHED = P + "published_source_profiles"
EDGES = P + "edge_profiles"
CHANGES = P + "profile_changes"
STATE = P + "profile_stream_state"
SNAPSHOT = P + "profile_bootstrap_snapshot"
CATCHUP = P + "profile_bootstrap_catchup"
SESSIONS = P + "profile_bootstrap_sessions"
TEMPLATE = json.loads((Path(__file__).resolve().parents[2] / (
    "docs/audit/2026-09-24/probes/lum010/edge.json")).read_text())
PROFILE_COLUMNS = ("p.track_id, p.sample_rate, p.duration_ms, p.ref_lufs, p.start_ramp, "
                   "p.end_ramp, p.analyzer_ver, p.analyzed_at, p.media_signature")


@pytest.fixture
def db(migrated_db, monkeypatch):
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
            "provider_type, server_name, is_default, rebind_status) "
            "VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')", (SOURCE,))
        cur.execute(
            f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, "
            "provider_type, published_generation, catalog_epoch, status) "
            "VALUES (%s, 'server-a', 'navidrome', 1, 'epoch-a', 'complete')", (SOURCE,))
        catalog_enrichment._profile_stream_state(cur, SOURCE, for_update=True)
    migrated_db.commit()
    monkeypatch.setattr(
        plugin_api_module.config, "DATABASE_URL",
        psycopg2.extensions.make_dsn(os.environ["LUMAE_POSTGRES_TEST_DSN"],
                                     options=f"-c search_path={schema},public"),
        raising=False)
    return migrated_db


def body(**updates):
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": profile_bootstrap.TRANSFER_CONTRACT,
            "catalog_instance_id": SOURCE, **updates}


def _edge(track_id, signature, **changes):
    payload = dict(TEMPLATE, catalog_instance_id=SOURCE, track_id=track_id,
                   media_revision=opaque_revision(signature), **changes)
    payload["profile_digest"] = profile_digest(payload)
    return payload


def _insert_edge(cur, payload, signature):
    # Columns come from the payload, as publish_edge_profile guarantees.
    cur.execute(
        f"INSERT INTO {EDGES} (catalog_instance_id, track_id, media_revision, "
        "representation_id, media_signature, profile_digest, payload) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)",
        (SOURCE, payload["track_id"], payload["media_revision"],
         payload["representation_id"], signature, payload["profile_digest"],
         json.dumps(payload)))


def _profile(cur, track_id, signature):
    cur.execute(
        f"INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate, duration_ms, "
        "ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, media_signature, "
        "analyzed_at) VALUES (%s, %s, 44100, 240000, -14.5, %s, %s, 1, 1, %s, "
        "'2026-09-01 12:00:00')",
        (SOURCE, track_id, b"\x01\x02\x03" * 15, b"\x04\x05\x06" * 15, signature))


def _profiles_with_edges(db, count, *, edges=True):
    """Bulk-seed ``count`` profiles and real-size (about 19.5 KB) edges in SQL."""
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate,
                    duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver,
                    profile_schema_ver, media_signature, analyzed_at)
                SELECT %s, 'track-' || lpad(g::text, 6, '0'), 44100, 240000, -14.5,
                       decode(repeat(substr(md5(g::text), 1, 6), 15), 'hex'),
                       decode(repeat(substr(md5('e' || g), 1, 6), 15), 'hex'),
                       1, 1, 'sig-' || g, '2026-09-01 12:00:00'
                  FROM generate_series(1, %s) g""", (SOURCE, count))
        if edges:
            cur.execute(
                f"""INSERT INTO {EDGES} (catalog_instance_id, track_id, media_revision,
                        representation_id, media_signature, profile_digest, payload)
                    SELECT %s, t, r, %s, 'sig-' || g, d,
                           %s::jsonb || jsonb_build_object('track_id', t, 'media_revision', r,
                                                         'profile_digest', d,
                                                         'noise_floor_cdb', -9000 - g)
                      FROM generate_series(1, %s) g,
                           LATERAL (SELECT 'track-' || lpad(g::text, 6, '0') AS t,
                                           'sha256:' || encode(sha256(convert_to('sig-' || g, 'UTF8')), 'hex') AS r,
                                           md5(g::text) || md5('d' || g) AS d) k""",
                (SOURCE, TEMPLATE["representation_id"], json.dumps(TEMPLATE), count))
    db.commit()


def _jsonb(db, values):
    """Round-trip values through JSONB exactly as the pre-K2 capture stored them."""
    with db.cursor() as cur:
        cur.execute("SELECT v::jsonb FROM unnest(%s::text[]) WITH ORDINALITY AS u(v, n) "
                    "ORDER BY n", ([json.dumps(v, separators=(",", ":")) for v in values],))
        rows = [row[0] for row in cur.fetchall()]
    db.commit()
    return rows


def _legacy_snapshot(db):
    """What the pre-K2 create_session stored: serialize_profile with edge_join()."""
    with db.cursor() as cur:
        cur.execute(f"SELECT {PROFILE_COLUMNS}, edge.payload FROM {PUBLISHED} p {edge_join()} "
                    "WHERE p.catalog_instance_id=%s ORDER BY p.track_id", (SOURCE,))
        rows = [catalog_enrichment.serialize_profile(*row) for row in cur.fetchall()]
    db.commit()
    return _jsonb(db, rows)


def _legacy_catchup(db, after):
    with db.cursor() as cur:
        cur.execute(f"SELECT seq, track_id, operation, payload, created_at FROM {CHANGES} "
                    "WHERE catalog_instance_id=%s AND seq>%s ORDER BY seq", (SOURCE, after))
        events = [{"seq": int(seq), "track_id": track_id, "operation": operation,
                   "payload": payload,
                   "created_at": created_at.isoformat().replace("+00:00", "Z")}
                  for seq, track_id, operation, payload, created_at in cur.fetchall()]
    db.commit()
    return _jsonb(db, events)


def _pages(created, operation, key):
    token = {"session_token": created["session_token"]}
    items, page_token = [], None
    while True:
        extra = {"page_token": page_token} if page_token else {}
        page = operation(body(**token, **extra))
        items.extend(page[key])
        if not page["has_more"]:
            return items
        page_token = page["next_page_token"]


def _wire(value):
    # Flask serializes the dicts as they come from psycopg2; key order matters.
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _row(cur, track_id):
    cur.execute(f"SELECT {PROFILE_COLUMNS} FROM {PUBLISHED} p "
                "WHERE p.catalog_instance_id=%s AND p.track_id=%s", (SOURCE, track_id))
    return cur.fetchone()


def _replace_edge(db, track_id, signature, **changes):
    """A new edge publication for the same media: new digest, one journal event."""
    replacement = _edge(track_id, signature, **changes)
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {EDGES} WHERE catalog_instance_id=%s AND track_id=%s",
                    (SOURCE, track_id))
        _insert_edge(cur, replacement, signature)
        catalog_enrichment.record_profile_change(
            cur, SOURCE, track_id, "ready",
            catalog_enrichment.serialize_profile(*_row(cur, track_id), edge_profile=replacement))
    db.commit()
    return replacement


def _withdraw_edge(db, track_id):
    """A waveform republish deletes the edge and journals an upsert without it."""
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {EDGES} WHERE catalog_instance_id=%s AND track_id=%s",
                    (SOURCE, track_id))
        catalog_enrichment.record_profile_change(
            cur, SOURCE, track_id, "ready",
            catalog_enrichment.serialize_profile(*_row(cur, track_id)))
    db.commit()


def _mixed_library(db):
    """Edges, no edge, a stale-revision edge and a profile with no media
    revision (serialize_profile embeds no edge and no reference is stored).

    An edge row whose track_id/media_revision columns differ from its payload
    cannot exist (publish_edge_profile checks the payload against the job it
    inserts), so pages match the columns and do not re-check the payload.
    """
    with db.cursor() as cur:
        for index in range(1, 5):
            _profile(cur, f"track-{index}", f"sig-{index}")
            _insert_edge(cur, _edge(f"track-{index}", f"sig-{index}"), f"sig-{index}")
        _profile(cur, "track-5", "sig-5")
        _profile(cur, "track-6", "sig-6")
        _insert_edge(cur, _edge("track-6", "sig-6-old"), "sig-6-old")
        _profile(cur, "track-7", None)
    db.commit()


def test_lum010_real_edges_no_longer_hit_the_byte_cap(db):
    """Inverted LUM-010 probe (was 413 at 7,000 profiles): 10k real-size edges."""
    _profiles_with_edges(db, 10_000)
    started = time.monotonic()
    created = profile_bootstrap.create_session(body(page_size=500))
    elapsed = time.monotonic() - started
    assert created["snapshot_count"] == created["total_profiles"] == 10_000
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*), count(edge_ref), sum(octet_length(payload::text)), "
                    "bool_or(payload ? 'edge_profile') FROM {SNAPSHOT}".format(SNAPSHOT=SNAPSHOT))
        rows, refs, stored, embedded = cur.fetchone()
    db.commit()
    assert rows == refs == 10_000
    assert embedded is False
    # About 0.4 KB of waveform per row; the pre-K2 capture stored about 190 MB.
    assert stored < 10_000 * 1024
    page = profile_bootstrap.snapshot_page(body(session_token=created["session_token"]))
    assert len(page["profiles"]) == 500
    first = page["profiles"][0]
    assert first["edge_profile"]["track_id"] == first["track_id"] == "track-000001"
    assert first["edge_profile"]["media_revision"] == first["media_revision"]
    assert len(json.dumps(first["edge_profile"])) > 19_000
    print(f"\ncreate 10k with edges: {elapsed:.2f}s, snapshot {stored / 1e6:.1f} MB")


def test_snapshot_byte_cap_counts_waveform_bytes_only(db, monkeypatch):
    _profiles_with_edges(db, 20)
    with db.cursor() as cur:
        cur.execute(f"SELECT {PROFILE_COLUMNS} FROM {PUBLISHED} p ORDER BY p.track_id")
        waveform = sum(len(json.dumps(catalog_enrichment.serialize_profile(*row),
                                      separators=(",", ":")).encode())
                       for row in cur.fetchall())
    db.commit()
    assert waveform < 20 * 1024
    monkeypatch.setattr(profile_bootstrap, "MAX_SNAPSHOT_BYTES", waveform - 1)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.create_session(body())
    assert (exc.value.code, exc.value.status) == ("bootstrap_snapshot_limit", 413)
    monkeypatch.setattr(profile_bootstrap, "MAX_SNAPSHOT_BYTES", waveform)
    created = profile_bootstrap.create_session(body(page_size=500))
    profiles = _pages(created, profile_bootstrap.snapshot_page, "profiles")
    assert all("edge_profile" in profile for profile in profiles)


def test_snapshot_and_catchup_pages_are_byte_identical_to_legacy_capture(db):
    _mixed_library(db)
    legacy = _legacy_snapshot(db)
    assert [("edge_profile" in row) for row in legacy] == [True] * 4 + [False] * 3
    created = profile_bootstrap.create_session(body(page_size=3))
    profiles = _pages(created, profile_bootstrap.snapshot_page, "profiles")
    assert _wire(profiles) == _wire(legacy)
    assert profile_bootstrap.snapshot_page(
        body(session_token=created["session_token"]))["profiles"] == profiles[:3]
    with db.cursor() as cur:
        cur.execute(f"SELECT ordinal, edge_ref FROM {SNAPSHOT} ORDER BY ordinal")
        refs = cur.fetchall()
    db.commit()
    assert [ref is not None for _ordinal, ref in refs] == [True] * 4 + [False] * 3
    assert refs[0][1] == {"profile_digest": legacy[0]["edge_profile"]["profile_digest"]}

    # Journal events with and without edges; none is replaced afterwards, so
    # the catch-up equals the legacy one byte for byte.
    with db.cursor() as cur:
        _profile(cur, "track-8", "sig-8")
        edge = _edge("track-8", "sig-8")
        _insert_edge(cur, edge, "sig-8")
        catalog_enrichment.record_profile_change(
            cur, SOURCE, "track-8", "ready",
            catalog_enrichment.serialize_profile(*_row(cur, "track-8"), edge_profile=edge))
        catalog_enrichment.record_profile_change(
            cur, SOURCE, "track-5", "ready",
            catalog_enrichment.serialize_profile(*_row(cur, "track-5")))
        cur.execute(f"DELETE FROM {PUBLISHED} WHERE track_id='track-4'")
        cur.execute(f"DELETE FROM {EDGES} WHERE track_id='track-4'")
        catalog_enrichment.record_profile_change(cur, SOURCE, "track-4", "deleted")
        # A journalled edge for another revision than the payload's: the
        # reference then carries its own media_revision.
        _profile(cur, "track-9", "sig-9")
        foreign = _edge("track-9", "sig-9-next")
        _insert_edge(cur, foreign, "sig-9-next")
        catalog_enrichment.record_profile_change(
            cur, SOURCE, "track-9", "ready",
            dict(catalog_enrichment.serialize_profile(*_row(cur, "track-9")),
                 edge_profile=foreign))
    db.commit()
    changes = _pages(created, profile_bootstrap.catchup_page, "changes")
    legacy_changes = _legacy_catchup(db, created["snapshot_seq"])
    assert "edge_profile" in legacy_changes[0]["payload"]
    assert _wire(changes) == _wire(legacy_changes)
    with db.cursor() as cur:
        cur.execute(f"SELECT edge_ref, payload->'payload' ? 'edge_profile' FROM {CATCHUP} "
                    "ORDER BY ordinal")
        stored = cur.fetchall()
    db.commit()
    assert stored == [({"profile_digest": edge["profile_digest"]}, False),
                      (None, False), (None, False),
                      ({"media_revision": foreign["media_revision"],
                        "profile_digest": foreign["profile_digest"]}, False)]
    assert changes[-1]["payload"]["edge_profile"] == foreign


def test_edge_replaced_after_capture_is_absent_in_snapshot_and_present_in_catchup(db):
    _mixed_library(db)
    legacy = _legacy_snapshot(db)
    created = profile_bootstrap.create_session(body(page_size=2))
    replaced = _replace_edge(db, "track-1", "sig-1", noise_floor_cdb=-7777)
    _withdraw_edge(db, "track-2")
    assert replaced["profile_digest"] != legacy[0]["edge_profile"]["profile_digest"]

    profiles = _pages(created, profile_bootstrap.snapshot_page, "profiles")
    expected = [dict(row) for row in legacy]
    del expected[0]["edge_profile"], expected[1]["edge_profile"]
    assert profiles == expected
    assert _wire(profiles[2:]) == _wire(legacy[2:])

    changes = _pages(created, profile_bootstrap.catchup_page, "changes")
    assert [(event["track_id"], event["operation"]) for event in changes] == [
        ("track-1", "upsert"), ("track-2", "upsert")]
    assert changes[0]["payload"]["edge_profile"] == replaced
    assert "edge_profile" not in changes[1]["payload"]
    assert _wire(changes) == _wire(_legacy_catchup(db, created["snapshot_seq"]))

    # Replaced again after the catch-up was captured: that event's reference
    # is gone too, and the newer event follows in /changes after head_cursor.
    newest = _replace_edge(db, "track-1", "sig-1", noise_floor_cdb=-6666)
    last = profile_bootstrap.catchup_page(body(session_token=created["session_token"]))
    again = _pages(created, profile_bootstrap.catchup_page, "changes")
    assert "edge_profile" not in again[0]["payload"]
    assert again[1:] == changes[1:]
    tail = catalog_enrichment.read_profile_changes(db, last["head_cursor"], SOURCE, 100)
    db.commit()
    assert tail["changes"][-1]["payload"]["edge_profile"] == newest


def test_catchup_byte_cap_counts_waveform_bytes_only(db, monkeypatch):
    _profiles_with_edges(db, 3, edges=False)
    created = profile_bootstrap.create_session(body())
    for index in range(1, 4):
        _replace_edge(db, f"track-{index:06d}", f"sig-{index}")
    legacy = _legacy_catchup(db, created["snapshot_seq"])
    assert all(len(json.dumps(event["payload"]["edge_profile"])) > 19_000 for event in legacy)
    waveform = 0
    for event in legacy:
        event = dict(event, payload={k: v for k, v in event["payload"].items()
                                     if k != "edge_profile"})
        waveform += len(json.dumps(event, separators=(",", ":")).encode())
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_EVENT_BYTES", 0)
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_BYTES", waveform - 1)
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap.catchup_page(body(session_token=created["session_token"]))
    assert exc.value.status == 413
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_BYTES", waveform)
    changes = _pages(created, profile_bootstrap.catchup_page, "changes")
    assert _wire(changes) == _wire(legacy)


def test_catchup_limits_cover_the_floor_hold(db, monkeypatch):
    limits = profile_bootstrap.catchup_limits
    minimum = catalog_enrichment.PROFILE_CHANGE_RETENTION_EVENTS
    multiplier = catalog.MAX_HELD_RETENTION_MULTIPLIER
    assert profile_bootstrap.CATCHUP_RETENTION_MULTIPLIER == multiplier == 4
    assert limits(None) == limits(0) == limits(minimum) == (4 * minimum, 4 * minimum * 1024)
    assert limits(188_000) == (752_000, 752_000 * 1024)
    assert limits(188_000)[0] >= 4 * catalog_enrichment.profile_change_retention_limit(94_000)

    # The limit is read from the source's persisted retention_limit.
    seen = []
    monkeypatch.setattr(profile_bootstrap, "catchup_limits",
                        lambda value: seen.append(value) or limits(value))
    with db.cursor() as cur:
        cur.execute(f"UPDATE {STATE} SET retention_limit=123456 WHERE catalog_instance_id=%s",
                    (SOURCE,))
    db.commit()
    created = profile_bootstrap.create_session(body())
    profile_bootstrap.catchup_page(body(session_token=created["session_token"]))
    assert seen == [123456]


def test_sessions_captured_before_the_upgrade_page_as_before(db, run_plugin_migration):
    """Rows with a full payload and no edge_ref (pre-1.3.0 captures) keep
    paging their frozen JSON after the additive migration."""
    _mixed_library(db)
    legacy = _legacy_snapshot(db)
    created = profile_bootstrap.create_session(body(page_size=3))
    _replace_edge(db, "track-1", "sig-1", noise_floor_cdb=-5555)
    legacy_changes = _legacy_catchup(db, created["snapshot_seq"])
    profile_bootstrap.catchup_page(body(session_token=created["session_token"]))
    with db.cursor() as cur:
        # Rewrite both captures into the pre-K2 form and schema.
        cur.execute(f"ALTER TABLE {SNAPSHOT} DROP COLUMN edge_ref")
        cur.execute(f"ALTER TABLE {CATCHUP} DROP COLUMN edge_ref")
        cur.execute(f"UPDATE {SNAPSHOT} s SET payload=l.v::jsonb FROM unnest(%s::text[]) "
                    "WITH ORDINALITY AS l(v, n) WHERE s.ordinal=l.n-1",
                    ([json.dumps(row) for row in legacy],))
        cur.execute(f"UPDATE {CATCHUP} c SET payload=l.v::jsonb FROM unnest(%s::text[]) "
                    "WITH ORDINALITY AS l(v, n) WHERE c.ordinal=l.n-1",
                    ([json.dumps(row) for row in legacy_changes],))
        # A pre-K2 row pages its frozen edge even after the edge is gone.
        cur.execute(f"DELETE FROM {EDGES}")
    db.commit()
    run_plugin_migration(db)
    with db.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {SESSIONS}")
        assert cur.fetchone()[0] == 1
        cur.execute(f"SELECT count(*) FILTER (WHERE edge_ref IS NULL) FROM {SNAPSHOT}")
        assert cur.fetchone()[0] == len(legacy)
    db.commit()
    profiles = _pages(created, profile_bootstrap.snapshot_page, "profiles")
    assert _wire(profiles) == _wire(legacy)
    changes = _pages(created, profile_bootstrap.catchup_page, "changes")
    assert _wire(changes) == _wire(legacy_changes)
    assert "edge_profile" in changes[0]["payload"]


# Track ids that COPY text format, JSON escaping or both treat specially.
AWKWARD_IDS = [
    "tab\there", "back\\slash", "new\nline", "\\N", "carriage\rreturn", 'quote"d',
    "\\.", "ctl\x01\x1b\x1f\x7f", "line separator", "emoji\U0001F3B5", "CJK曲目",
    "\\", "trailing\\", "\\t literal", "mix\\\t\n\r\\N\\.",
]


def test_awkward_track_ids_round_trip_through_copy_capture(db):
    """Snapshot and catch-up rows are built in SQL (P2-4; COPY before it):
    every id round-trips and the pages equal the pre-K2 wire output, with and
    without edges."""
    with db.cursor() as cur:
        for index, track_id in enumerate(AWKWARD_IDS):
            _profile(cur, track_id, f"awkward-{index}")
            if index % 2 == 0:
                _insert_edge(cur, _edge(track_id, f"awkward-{index}"), f"awkward-{index}")
    db.commit()
    legacy = _legacy_snapshot(db)
    assert sum("edge_profile" in row for row in legacy) == (len(AWKWARD_IDS) + 1) // 2
    created = profile_bootstrap.create_session(body(page_size=4))
    profiles = _pages(created, profile_bootstrap.snapshot_page, "profiles")
    assert _wire(profiles) == _wire(legacy)
    assert sorted(row["track_id"] for row in profiles) == sorted(AWKWARD_IDS)
    for row in profiles:
        if "edge_profile" in row:
            assert row["edge_profile"]["track_id"] == row["track_id"]

    # Journalled: a new edge, a withdrawn edge and a delete, all awkward ids.
    replaced = _replace_edge(db, "tab\there", "awkward-0", noise_floor_cdb=-4444)
    _withdraw_edge(db, "\\N")
    with db.cursor() as cur:
        cur.execute(f"DELETE FROM {PUBLISHED} WHERE track_id=%s", ("mix\\\t\n\r\\N\\.",))
        cur.execute(f"DELETE FROM {EDGES} WHERE track_id=%s", ("mix\\\t\n\r\\N\\.",))
        catalog_enrichment.record_profile_change(cur, SOURCE, "mix\\\t\n\r\\N\\.", "deleted")
    db.commit()
    changes = _pages(created, profile_bootstrap.catchup_page, "changes")
    assert [(event["track_id"], event["operation"]) for event in changes] == [
        ("tab\there", "upsert"), ("\\N", "upsert"), ("mix\\\t\n\r\\N\\.", "delete")]
    assert changes[0]["payload"]["edge_profile"] == replaced
    assert changes[0]["payload"]["track_id"] == "tab\there"
    assert _wire(changes) == _wire(_legacy_catchup(db, created["snapshot_seq"]))
