"""P3-2 (K6): edge references in /profiles/changes and v2 catch-up pages.

The journal stores each upsert's waveform payload plus an ``edge_ref`` (the
edge row's ``profile_digest``) instead of a copy of the edge. A waveform-only
republish whose edge was kept is marked ref-eligible; an edge publication is
not. Readers expand the reference:

* without the opt-in, to the full edge, so an old client's responses are
  byte-identical to the pre-K6 server's for the same history (golden
  responses below, generated from the pre-K6 code);
* with ``edge_refs=1`` (``/changes``) or ``edge_refs: true`` (v2 create), a
  ref-eligible event carries ``edge_profile_ref`` instead.

Runs on the real migrated schema, through the production publication paths
(``complete_attempt``, ``publish_edge_profile``) and the HTTP routes.
"""

import json
import os
import re
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

psycopg2 = pytest.importorskip("psycopg2")

from flask import Flask

from test_lumae_analysis import load_plugin, plugin_api_module
from plugins.LumaeAnalysis import catalog_enrichment as enrichment
from plugins.LumaeAnalysis import edge_profile_store as store
from plugins.LumaeAnalysis import profile_publication as publication
from plugins.LumaeAnalysis.catalog import opaque_cursor
from plugins.LumaeAnalysis.edge_profiles import analyze_edge_blocks


SOURCE = "catalog-a"
SERVER = "server-a"
P = "plugin_lumae_analysis__"
CATALOG_EPOCH = "golden-catalog-epoch"
PROFILE_EPOCH = "golden-profile-epoch"
V2 = "/api/profiles/bootstrap/sessions"
GOLDEN = Path(__file__).resolve().parent / "fixtures" / "k6_old_client_golden.json"
# Set to regenerate the golden file from the code under test (only ever from
# a pre-K6 checkout: the goldens pin what old clients received before K6).
WRITE_GOLDEN = os.environ.get("LUMAE_K6_WRITE_GOLDEN") == "1"
# Track ids sort the same in the C and en_US collations (CI runs the latter).
TRACKS = [f"t{n:02d}" for n in range(1, 13)] + ["t13-é曲"]


# ---------------------------------------------------------------------------
# Fixture: one source, a fixed profile epoch (cursors are deterministic), and
# helpers that publish through the production paths.
# ---------------------------------------------------------------------------


def _execute(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else None
    db.commit()
    return rows


def _set_media(db, track, fp, available=True):
    _execute(
        db,
        f"""INSERT INTO {P}catalog_tracks
            (catalog_instance_id, published_generation, track_id, title,
             metadata_fp, media_fp, analysis_eligible, payload, available,
             first_seen_at, last_seen_at)
            VALUES (%s, 1, %s, %s, 'metadata', %s, TRUE, '{{}}'::jsonb, %s, now(), now())
            ON CONFLICT (catalog_instance_id, published_generation, track_id)
            DO UPDATE SET media_fp=EXCLUDED.media_fp, available=EXCLUDED.available""",
        (SOURCE, track, track, fp, available),
    )


def _result(level, start=b"wave", end=b"tail"):
    return SimpleNamespace(sample_rate=48000, duration_ms=180000, ref_lufs=level,
                           start_ramp_blob=start, end_ramp_blob=end)


def _complete(db, track, level, fp=None, start=b"wave"):
    """Admit and complete one waveform analysis of ``track`` (media ``fp``)."""
    fp = fp or f"m1-{track}"
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    assert publication.complete_attempt(
        db, SOURCE, track, token, _result(level, start=start), "ready", None,
        f"catalog-media:{fp}", 1, 1)


def _edge(job, variant):
    """A deterministic EdgeProfileV2 for the job; ``variant`` changes its digest."""
    frames = np.sin(np.arange(10003, dtype=np.float32) * (0.01 + 0.001 * (ord(variant) % 7)))
    return analyze_edge_blocks(
        [(0.25 * frames).reshape(1, -1).astype(np.float32)], 48000,
        catalog_instance_id=SOURCE, track_id=job["track_id"],
        media_revision=job["media_revision"], content_sha256=variant * 64,
        channel_layout="mono", timeline_verified=True)


def _publish_edge(db, track, fp=None, variant="a"):
    """An edge publication: claim, run and publish the upgrade job."""
    fp = fp or f"m1-{track}"
    jobs, ready = store.claim_edge_jobs(db, SOURCE, [track])
    if not jobs and ready == [track]:
        # The track already has a current edge (a later analyzer version would
        # re-claim it): force a new job, as a method upgrade's claim does.
        revision = _execute(
            db, f"SELECT media_revision FROM {P}edge_profile_jobs "
                "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, track))[0][0]
        token = f"forced-{track}-{variant}"
        _execute(db, f"UPDATE {P}edge_profile_jobs SET job_token=%s, status='pending' "
                     "WHERE catalog_instance_id=%s AND track_id=%s", (token, SOURCE, track))
        jobs = [{"track_id": track, "media_revision": revision, "job_token": token}]
    assert len(jobs) == 1, (jobs, ready)
    job = jobs[0]
    assert store.update_edge_job(db, SOURCE, job, "running")
    payload = _edge(job, variant)
    assert store.publish_edge_profile(db, SOURCE, job, payload, f"catalog-media:{fp}")
    return payload


def _withdraw(db, track):
    """The track leaves the catalogue while an analysis runs: a delete event."""
    token = publication.admit_attempts(db, SOURCE, [track])[track]
    _set_media(db, track, f"m1-{track}", available=False)
    assert not publication.complete_attempt(
        db, SOURCE, track, token, _result(-20.0), "ready", None,
        f"catalog-media:m1-{track}", 1, 1)


def _change_media(db, track, fp, level):
    _set_media(db, track, fp)
    _complete(db, track, level, fp=fp)


def _head(db):
    return _execute(db, f"SELECT head_seq FROM {P}profile_stream_state "
                        "WHERE catalog_instance_id=%s", (SOURCE,))[0][0]


@pytest.fixture
def source_db(migrated_db, monkeypatch):
    mod = load_plugin()
    with migrated_db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        schema = cur.fetchone()[0]
        cur.execute(
            f"""INSERT INTO {P}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type,
                 server_name, is_default, rebind_status)
                VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')""", (SOURCE, SERVER))
        cur.execute(
            f"""INSERT INTO {P}catalog_state
                (catalog_instance_id, current_core_server_id, provider_type,
                 published_generation, catalog_epoch, status)
                VALUES (%s, %s, 'navidrome', 1, %s, 'complete')""",
            (SOURCE, SERVER, CATALOG_EPOCH))
        enrichment._profile_stream_state(cur, SOURCE, for_update=True)
        cur.execute(f"UPDATE {P}profile_stream_state SET epoch=%s "
                    "WHERE catalog_instance_id=%s", (PROFILE_EPOCH, SOURCE))
    migrated_db.commit()
    for track in TRACKS:
        _set_media(migrated_db, track, f"m1-{track}")
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    monkeypatch.setattr(
        plugin_api_module.config, "DATABASE_URL",
        psycopg2.extensions.make_dsn(os.environ["LUMAE_POSTGRES_TEST_DSN"],
                                     options=f"-c search_path={schema},public"),
        raising=False)
    return migrated_db


def _app():
    app = Flask(__name__)
    app.register_blueprint(load_plugin().bp)
    return app.test_client()


# ---------------------------------------------------------------------------
# HTTP helpers (no Accept-Encoding: the raw JSON bytes an old client decodes).
# ---------------------------------------------------------------------------


def _get(client, path, query):
    response = client.get(path, query_string=query)
    assert response.headers.get("Content-Encoding") is None
    return response


def _changes(client, cursor, limit, **extra):
    """Every /changes page from ``cursor`` to head: [(status, raw bytes, json)]."""
    pages = []
    while True:
        response = _get(client, "/api/profiles/changes",
                        {"cursor": cursor, "catalog_instance_id": SOURCE, "limit": limit,
                         **extra})
        pages.append((response.status_code, response.data, response.get_json()))
        body = response.get_json()
        if response.status_code != 200 or not body["has_more"]:
            return pages
        cursor = body["cursor"]


def _bootstrap(client, limit):
    pages, token = [], None
    while True:
        query = {"catalog_instance_id": SOURCE, "limit": limit}
        if token:
            query["page_token"] = token
        response = _get(client, "/api/profiles/bootstrap", query)
        pages.append((response.status_code, response.data, response.get_json()))
        body = response.get_json()
        if response.status_code != 200 or not body["has_more"]:
            return pages
        token = body["next_page_token"]


def _profiles(client, ids):
    response = _get(client, "/api/profiles",
                    {"catalog_instance_id": SOURCE, "ids": ",".join(ids)})
    return [(response.status_code, response.data, response.get_json())]


def _v2_body(**extra):
    return {"protocol_version": 2, "schema_version": 1, "transfer_contract": "source_scoped_v1",
            "catalog_instance_id": SOURCE, **extra}


def _post(client, path, body):
    response = client.post(V2 + path, json=body)
    assert response.headers.get("Content-Encoding") is None
    return response


def _v2_create(client, page_size, **extra):
    response = _post(client, "", _v2_body(page_size=page_size, **extra))
    assert response.status_code == 200, response.data[:300]
    return [(response.status_code, response.data, response.get_json())]


def _v2_pages(client, token, path):
    pages, page_token = [], None
    while True:
        extra = {"page_token": page_token} if page_token else {}
        response = _post(client, path, _v2_body(session_token=token, **extra))
        pages.append((response.status_code, response.data, response.get_json()))
        body = response.get_json()
        if response.status_code != 200 or not body["has_more"]:
            return pages
        page_token = body["next_page_token"]


# ---------------------------------------------------------------------------
# The fixture history. Part A never replaces or removes an edge an earlier
# event carried; part B does (edge re-publication, media change and withdrawal
# of tracks with edges, and a kept edge replaced later).
# ---------------------------------------------------------------------------


def _history_a(db):
    for index, track in enumerate(TRACKS[:8] + TRACKS[12:]):
        _complete(db, track, -14.0 - index / 8)              # first publication: no edge
    for track in TRACKS[:5] + TRACKS[12:]:
        _publish_edge(db, track)                             # edge publication: full edge
    _complete(db, TRACKS[0], -13.25, start=b"wave-2")        # waveform only, edge kept
    _complete(db, TRACKS[1], -12.5, start=b"wave-2")         # waveform only, edge kept
    _complete(db, TRACKS[5], -11.0, start=b"wave-2")         # waveform only, no edge
    _change_media(db, TRACKS[6], f"m2-{TRACKS[6]}", -10.5)   # new media, no edge before
    _withdraw(db, TRACKS[7])                                 # withdrawal, no edge
    _complete(db, TRACKS[0], -13.5, start=b"wave-3")         # kept again (same edge)
    _complete(db, TRACKS[12], -9.75, start=b"wave-2")        # kept, non-ASCII id


HISTORY_B = (
    lambda db: _complete(db, TRACKS[4], -8.5, start=b"wave-2"),     # kept, replaced next
    lambda db: _publish_edge(db, TRACKS[4], variant="b"),           # same media, new edge
    lambda db: _publish_edge(db, TRACKS[1], variant="c"),           # replaces A's kept edge
    lambda db: _change_media(db, TRACKS[2], f"m2-{TRACKS[2]}", -8.0),  # edged, new media
    lambda db: _publish_edge(db, TRACKS[2], fp=f"m2-{TRACKS[2]}"),  # its new edge
    lambda db: _withdraw(db, TRACKS[3]),                            # edged track withdrawn
)


def _history_b(db, after_each=lambda: None):
    for step in HISTORY_B:
        step(db)
        after_each()


def _golden_run(db):
    """Run the history and record every response an old client would see.

    Returns ``[(name, status, raw bytes, json)]`` in request order. The v1
    stream is read as a device that keeps up reads it: part A in one replay,
    part B after each step. A /changes replay of part B after the fact is
    not byte-identical, by design: its events carry edges that later steps
    replaced or removed, so K6 serves the fallback (tested separately).
    """
    client = _app()
    recorded = []

    def record(name, pages):
        recorded.extend((f"{name}[{index}]", *page) for index, page in enumerate(pages))
        return pages

    s0 = record("v2_s0_create", _v2_create(client, 4))[0][2]
    record("v2_s0_snapshot", _v2_pages(client, s0["session_token"], "/page"))
    _history_a(db)
    record("changes_a_from_0", _changes(client, opaque_cursor(SOURCE, PROFILE_EPOCH, 0), 5))
    record("bootstrap_a", _bootstrap(client, 4))
    record("profiles_a", _profiles(client, TRACKS + ["missing-id"]))
    s1 = record("v2_s1_create", _v2_create(client, 3))[0][2]
    record("v2_s1_snapshot_a", _v2_pages(client, s1["session_token"], "/page"))
    device = {"cursor": opaque_cursor(SOURCE, PROFILE_EPOCH, _head(db)), "step": 0}

    def keep_up():
        device["step"] += 1
        pages = record(f"changes_b_step{device['step']}", _changes(client, device["cursor"], 3))
        device["cursor"] = pages[-1][2]["cursor"]
    _history_b(db, keep_up)
    record("bootstrap_b", _bootstrap(client, 5))
    record("profiles_b", _profiles(client, TRACKS))
    record("v2_s1_snapshot_b", _v2_pages(client, s1["session_token"], "/page"))
    record("v2_s1_catchup_b", _v2_pages(client, s1["session_token"], "/catchup"))
    record("v2_s0_catchup_b", _v2_pages(client, s0["session_token"], "/catchup"))
    s2 = record("v2_s2_create", _v2_create(client, 5))[0][2]
    record("v2_s2_snapshot_b", _v2_pages(client, s2["session_token"], "/page"))
    record("v2_s2_catchup_b", _v2_pages(client, s2["session_token"], "/catchup"))
    return recorded


# Values that differ on every run: now() timestamps, v2 session tokens and
# the HMAC page tokens signed with a random secret. Cursors, epochs, seqs and
# the legacy bootstrap tokens are deterministic (fixed epochs).
_VARIABLE_KEYS = {"analyzed_at", "created_at", "expires_at", "session_token"}
_V2_PAGE_TOKEN = re.compile(r"[A-Za-z0-9_-]+\.[0-9a-f]{64}")


def _variable_values(value, found):
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, str) and (
                    key in _VARIABLE_KEYS
                    or key == "next_page_token" and _V2_PAGE_TOKEN.fullmatch(item)):
                found.setdefault(item, len(found))
            else:
                _variable_values(item, found)
    elif isinstance(value, list):
        for item in value:
            _variable_values(item, found)


def _neutralize(recorded):
    """Each variable value becomes ``"<v:N>"``, numbered by first appearance
    over the whole run, so which events share a timestamp is still pinned."""
    found = {}
    for _name, _status, _raw, body in recorded:
        _variable_values(body, found)
    result = []
    for name, status, raw, _body in recorded:
        text = raw.decode("utf-8")
        for value, number in found.items():
            text = text.replace(json.dumps(value), f'"<v:{number}>"')
        result.append({"name": name, "status": status, "body": text})
    return result


def test_old_client_responses_are_byte_identical_to_pre_k6_goldens(source_db):
    """Without the opt-in every response equals the pre-K6 server's for the
    same history, byte for byte (after neutralizing run-specific values)."""
    responses = _neutralize(_golden_run(source_db))
    if WRITE_GOLDEN:
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN.write_text(json.dumps({
            "about": "P3-2 (K6) old-client golden responses: generated from the pre-K6 "
                     "code (phase/3-semantics f36e087) by "
                     "test_profile_stream_edge_refs_postgres.py with LUMAE_K6_WRITE_GOLDEN=1",
            "responses": responses}, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))["responses"]
    assert [r["name"] for r in responses] == [r["name"] for r in golden]
    for actual, expected in zip(responses, golden):
        assert actual == expected, actual["name"]
    bodies = "".join(r["body"] for r in golden)
    # The goldens cover what K6 touches: embedded edges in events, in catch-up
    # pages and in snapshot pages, and an edge absent after its replacement.
    assert bodies.count('"edge_profile":') > 40
    assert '"edge_profile_ref"' not in bodies


# ---------------------------------------------------------------------------
# K6 behaviour, both modes: /profiles/changes and the v2 catch-up.
# ---------------------------------------------------------------------------


def _journal(db, after=0):
    """{seq: (track_id, operation, payload, edge_ref)} of the journal after ``after``."""
    rows = _execute(db, f"SELECT seq, track_id, operation, payload, edge_ref "
                        f"FROM {P}profile_changes WHERE catalog_instance_id=%s AND seq>%s "
                        "ORDER BY seq", (SOURCE, after))
    return {row[0]: row[1:] for row in rows}


def _events(client, after, *, edge_refs=None, limit=4):
    """Every /changes event after seq ``after``, decoded: {seq: change}.
    ``edge_refs`` is the query value sent (None: no parameter)."""
    extra = {} if edge_refs is None else {"edge_refs": edge_refs}
    pages = _changes(client, opaque_cursor(SOURCE, PROFILE_EPOCH, after), limit, **extra)
    assert all(status == 200 for status, _raw, _body in pages), pages[-1][:2]
    return {change["seq"]: change for _s, _r, body in pages for change in body["changes"]}


def _catchup_events(client, token):
    pages = _v2_pages(client, token, "/catchup")
    assert all(status == 200 for status, _raw, _body in pages), pages[-1][:2]
    return {change["seq"]: change for _s, _r, body in pages for change in body["changes"]}


def _stored_edge(db, track):
    rows = _execute(db, f"SELECT payload FROM {P}edge_profiles "
                        "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, track))
    return rows[0][0] if rows else None


def _ref(edge):
    return {"media_revision": edge["media_revision"], "profile_digest": edge["profile_digest"]}


def _without_edge(payload):
    return {k: v for k, v in payload.items() if k not in ("edge_profile", "edge_profile_ref")}


def test_health_advertises_profile_stream_edge_refs(source_db, monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "integrity_status", lambda *_args, **_kwargs: None)
    body = _app().get("/api/health").get_json()
    assert body["capabilities"]["profile_stream"] == {"edge_refs": True}


def test_waveform_only_republish_is_a_reference_only_with_the_opt_in(source_db):
    db, client, track = source_db, _app(), TRACKS[0]
    _complete(db, track, -14.0)
    edge = _publish_edge(db, track)
    _complete(db, track, -13.0, start=b"wave-2")
    journal = _journal(db)
    first, published, kept = sorted(journal)
    # The journal holds waveform parts; edges are referenced, never copied.
    assert all("edge_profile" not in row[2] for row in journal.values())
    assert journal[first][3] is None
    assert journal[published][3] == {"profile_digest": edge["profile_digest"]}
    assert journal[kept][3] == {"profile_digest": edge["profile_digest"], "kept": True}

    plain = _events(client, 0)
    assert "edge_profile" not in plain[first]["payload"]
    for seq in (published, kept):
        assert plain[seq]["payload"]["edge_profile"] == edge
        assert "edge_profile_ref" not in plain[seq]["payload"]
    # Any value but 1/true is the default.
    for value in ("0", "false", "yes", "", "2"):
        assert _events(client, 0, edge_refs=value) == plain, value

    refs = _events(client, 0, edge_refs="1")
    assert _events(client, 0, edge_refs="true") == refs
    assert refs[first] == plain[first]
    # An edge publication always carries the full edge.
    assert refs[published] == plain[published]
    assert refs[kept]["payload"]["edge_profile_ref"] == _ref(edge)
    assert "edge_profile" not in refs[kept]["payload"]
    assert _without_edge(refs[kept]["payload"]) == _without_edge(plain[kept]["payload"])
    assert {k: v for k, v in refs[kept].items() if k != "payload"} == \
        {k: v for k, v in plain[kept].items() if k != "payload"}


def test_media_change_drops_the_edge_and_the_next_publication_is_full_in_both_modes(source_db):
    db, client, track = source_db, _app(), TRACKS[1]
    _complete(db, track, -14.0)
    _publish_edge(db, track)
    plain_session = _v2_create(client, 2)[0][2]
    ref_session = _v2_create(client, 2, edge_refs=True)[0][2]
    after = _head(db)
    _change_media(db, track, f"m2-{track}", -12.0)
    new_edge = _publish_edge(db, track, fp=f"m2-{track}")
    views = [_events(client, after), _events(client, after, edge_refs="1"),
             _catchup_events(client, plain_session["session_token"]),
             _catchup_events(client, ref_session["session_token"])]
    for events in views:
        delete, upsert, published = (events[seq] for seq in sorted(events))
        assert delete["operation"] == "delete" and delete["payload"] is None
        assert upsert["operation"] == "upsert"
        assert "edge_profile" not in upsert["payload"]
        assert "edge_profile_ref" not in upsert["payload"]
        assert published["payload"]["edge_profile"] == new_edge
        assert "edge_profile_ref" not in published["payload"]
        assert published["payload"]["media_revision"] == new_edge["media_revision"]
    assert views[0] == views[1]
    assert views[2] == views[3]


def test_catchup_opt_in_is_per_session_and_snapshots_keep_full_edges(source_db):
    db, client = source_db, _app()
    for track in TRACKS[:3]:
        _complete(db, track, -14.0)
        _publish_edge(db, track)
    plain = _v2_create(client, 2)[0][2]
    refs = _v2_create(client, 2, edge_refs=True)[0][2]
    assert "edge_refs" not in plain and refs["edge_refs"] is True
    for name in ("snapshot_count", "snapshot_seq", "snapshot_cursor", "page_size"):
        assert plain[name] == refs[name]
    _complete(db, TRACKS[0], -13.0, start=b"wave-2")        # kept
    _publish_edge(db, TRACKS[1], variant="b")                # edge publication
    _complete(db, TRACKS[2], -12.0, start=b"wave-2")        # kept
    # Snapshot pages are the baseline: full edges with or without the opt-in.
    plain_pages = _v2_pages(client, plain["session_token"], "/page")
    ref_pages = _v2_pages(client, refs["session_token"], "/page")
    assert [body["profiles"] for _s, _r, body in plain_pages] == \
        [body["profiles"] for _s, _r, body in ref_pages]
    assert b"edge_profile_ref" not in b"".join(raw for _s, raw, _b in ref_pages)
    by_plain = _catchup_events(client, plain["session_token"])
    by_refs = _catchup_events(client, refs["session_token"])
    kept_a, published, kept_c = sorted(by_plain)
    for seq in (kept_a, kept_c):
        edge = by_plain[seq]["payload"]["edge_profile"]
        assert edge == _stored_edge(db, by_plain[seq]["track_id"])
        assert by_refs[seq]["payload"]["edge_profile_ref"] == _ref(edge)
        assert "edge_profile" not in by_refs[seq]["payload"]
        assert _without_edge(by_refs[seq]["payload"]) == _without_edge(by_plain[seq]["payload"])
    assert by_refs[published] == by_plain[published]
    assert "edge_profile" in by_plain[published]["payload"]
    # The catch-up serves what /changes serves over the same interval.
    assert by_plain == _events(client, plain["snapshot_seq"])
    assert by_refs == _events(client, refs["snapshot_seq"], edge_refs="1")


@pytest.mark.parametrize("value", ["yes", 1, 0, "true", [], {}])
def test_v2_create_rejects_a_non_boolean_edge_refs(source_db, value):
    response = _post(_app(), "", _v2_body(page_size=10, edge_refs=value))
    assert response.status_code == 400
    assert response.get_json()["error"] == "invalid_profile_bootstrap"


def test_v2_create_takes_null_and_false_edge_refs_as_absent(source_db):
    client = _app()
    for value in (None, False):
        created = _v2_create(client, 10, edge_refs=value)[0][2]
        assert "edge_refs" not in created
        assert _execute(source_db, f"SELECT edge_refs FROM {P}profile_bootstrap_sessions "
                                   "WHERE token_hash=encode(sha256(convert_to(%s, 'UTF8')), "
                                   "'hex')", (created["session_token"],)) == [(False,)]


def test_replaced_edge_falls_back_to_the_current_edge_then_is_omitted(source_db):
    """/changes without the opt-in: the referenced edge, else the edge current
    for the event's media_revision, else none. The v2 catch-up keeps K2: the
    referenced edge or none. The replacing event always follows."""
    db, client, track = source_db, _app(), TRACKS[2]
    _complete(db, track, -14.0)
    first_edge = _publish_edge(db, track)
    session = _v2_create(client, 3)[0][2]
    ref_session = _v2_create(client, 3, edge_refs=True)[0][2]
    after = _head(db)
    _complete(db, track, -13.0, start=b"wave-2")             # kept: references first_edge
    kept = _head(db)
    assert _events(client, after)[kept]["payload"]["edge_profile"] == first_edge

    second_edge = _publish_edge(db, track, variant="b")      # same media, new edge
    assert second_edge["profile_digest"] != first_edge["profile_digest"]
    plain = _events(client, after)
    assert plain[kept]["payload"]["edge_profile"] == second_edge            # fallback
    assert plain[kept + 1]["payload"]["edge_profile"] == second_edge        # the replacement
    refs = _events(client, after, edge_refs="1")
    assert refs[kept]["payload"]["edge_profile_ref"] == _ref(first_edge)    # as journalled
    assert refs[kept + 1] == plain[kept + 1]

    _change_media(db, track, f"m2-{track}", -12.0)           # removes the edge
    plain = _events(client, after)
    for seq in (kept, kept + 1):
        assert "edge_profile" not in plain[seq]["payload"], seq           # none for m1
    assert [plain[seq]["operation"] for seq in sorted(plain)][2:] == ["delete", "upsert"]
    refs = _events(client, after, edge_refs="1")
    assert refs[kept]["payload"]["edge_profile_ref"] == _ref(first_edge)
    assert "edge_profile" not in refs[kept + 1]["payload"]

    # v2 (K2, unchanged): a replaced reference is omitted, never substituted.
    by_session = _catchup_events(client, session["session_token"])
    assert "edge_profile" not in by_session[kept]["payload"]
    assert "edge_profile" not in by_session[kept + 1]["payload"]
    by_refs = _catchup_events(client, ref_session["session_token"])
    assert by_refs[kept]["payload"]["edge_profile_ref"] == _ref(first_edge)
    assert "edge_profile" not in by_refs[kept + 1]["payload"]
    _assert_replacement_follows(db)


def test_fallback_serves_only_the_events_own_revision(source_db):
    """The fallback never crosses media revisions: after a media change the
    new revision's edge is not served for an event of the old one."""
    db, client, track = source_db, _app(), TRACKS[3]
    _complete(db, track, -14.0)
    _publish_edge(db, track)
    _complete(db, track, -13.0, start=b"wave-2")
    kept = _head(db)
    _change_media(db, track, f"m2-{track}", -12.0)
    new_edge = _publish_edge(db, track, fp=f"m2-{track}")
    events = _events(client, 0)
    assert "edge_profile" not in events[kept]["payload"]
    assert events[max(events)]["payload"]["edge_profile"] == new_edge


def _assert_replacement_follows(db):
    """Every journalled reference whose edge is gone is followed by a later
    event of the same track (the one that replaced or removed the edge)."""
    rows = _execute(
        db,
        f"""SELECT c.seq, c.track_id FROM {P}profile_changes c
             WHERE c.catalog_instance_id=%s AND c.edge_ref IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM {P}edge_profiles e
                    WHERE e.catalog_instance_id=c.catalog_instance_id
                      AND e.track_id=c.track_id
                      AND e.media_revision=c.payload->>'media_revision'
                      AND e.profile_digest=c.edge_ref->>'profile_digest')
               AND NOT EXISTS (
                   SELECT 1 FROM {P}profile_changes later
                    WHERE later.catalog_instance_id=c.catalog_instance_id
                      AND later.epoch=c.epoch AND later.track_id=c.track_id
                      AND later.seq>c.seq)""", (SOURCE,))
    assert rows == [], "a dangling edge reference is its track's last event"


def test_every_edge_removal_is_followed_by_an_event_and_replays_converge(source_db):
    """Each path that replaces or removes an edge journals the change after
    the events that reference it, so a device replaying the stream in either
    mode (with the C-10 rule) ends with the server's profiles and edges."""
    db, client = source_db, _app()
    for index, track in enumerate(TRACKS[:10]):
        _complete(db, track, -14.0 - index)
        _publish_edge(db, track)
        _complete(db, track, -13.0 - index, start=b"wave-2")  # kept
    _publish_edge(db, TRACKS[0], variant="b")                    # edge re-publication
    _change_media(db, TRACKS[1], f"m2-{TRACKS[1]}", -9.0)        # media change
    _withdraw(db, TRACKS[2])                                     # withdrawal (attempt)
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}catalog_tracks SET available=FALSE "
                    "WHERE catalog_instance_id=%s AND track_id=%s", (SOURCE, TRACKS[3]))
        assert publication.invalidate_catalog_changes(           # catalogue withdrawal
            cur, SOURCE, 1, [("track", TRACKS[3], "delete")]) == 1
        assert publication.rekey_published_profiles(             # provider rekey
            cur, SOURCE, [{"old_id": TRACKS[4], "new_id": "t05-rekeyed"}]) == 1
    db.commit()
    _publish_edge(db, TRACKS[5], variant="c")
    _complete(db, TRACKS[5], -8.0, start=b"wave-3")              # kept, new edge
    _assert_replacement_follows(db)
    server = _server_state(client)
    assert len(server) == 8 and sum(v["edge"] is not None for v in server.values()) == 6
    assert _replay(client, 0, edge_refs=False) == server
    fetched = []
    assert _replay(client, 0, edge_refs=True, fetched=fetched) == server
    assert fetched  # a device without the edges fetched them by id
    # A current device then gets a full waveform-only republish (LUM-005):
    # it keeps every edge and fetches none.
    device = _replay(client, 0, edge_refs=False)
    after = _head(db)
    for index, track in enumerate([TRACKS[0]] + TRACKS[5:10]):
        _complete(db, track, -7.0 - index, start=b"wave-4")
    assert all(row[3].get("kept") for row in _journal(db, after).values())
    fetched = []
    assert _replay(client, after, edge_refs=True, local=device, fetched=fetched) \
        == _server_state(client)
    assert fetched == []
    assert _replay(client, 0, edge_refs=False) == _server_state(client)


def _server_state(client):
    body = _profiles(client, TRACKS + ["t05-rekeyed"])[0][2]
    return {p["track_id"]: {"waveform": _without_edge(p), "edge": p.get("edge_profile")}
            for p in body["profiles"]}


def _replay(client, after, *, edge_refs, local=None, fetched=None):
    """A device applying /changes after seq ``after`` (contract §6, C-10)."""
    local = {} if local is None else {k: dict(v) for k, v in local.items()}
    fetched = [] if fetched is None else fetched
    extra = {"edge_refs": "1"} if edge_refs else {}
    for status, _raw, body in _changes(client, opaque_cursor(SOURCE, PROFILE_EPOCH, after),
                                       4, **extra):
        assert status == 200
        misses = {}
        for change in body["changes"]:
            track = change["track_id"]
            misses.pop(track, None)
            if change["operation"] == "delete":
                local.pop(track, None)
                continue
            payload = change["payload"]
            edge, ref = payload.get("edge_profile"), payload.get("edge_profile_ref")
            held = (local.get(track) or {}).get("edge")
            if ref is not None:
                assert edge_refs and edge is None
                assert ref["media_revision"] == payload["media_revision"]
                if held and _ref(held) == ref:
                    edge = held
                else:
                    misses[track] = ref
            if edge is not None:
                assert edge["track_id"] == track
                assert edge["media_revision"] == payload["media_revision"]
            local[track] = {"waveform": _without_edge(payload), "edge": edge}
        ids = sorted(misses)
        for start in range(0, len(ids), 500):
            batch = ids[start:start + 500]
            fetched.extend(batch)
            profiles = {p["track_id"]: p for p in _profiles(client, batch)[0][2]["profiles"]}
            for track in batch:
                edge = (profiles.get(track) or {}).get("edge_profile")
                if edge and edge["media_revision"] == local[track]["waveform"]["media_revision"]:
                    local[track]["edge"] = edge
    return local


# ---------------------------------------------------------------------------
# Mixed journal: rows journalled before K6 embed their edge and have no
# reference. They are never rewritten, and every reader serves them as before.
# ---------------------------------------------------------------------------


def _pre_k6_event(db, track, level, start):
    """What the pre-K6 publication journalled for a waveform-only change:
    the payload with the current edge embedded, and no reference."""
    _complete(db, track, level, start=start)
    seq = _head(db)
    rows = _execute(
        db, f"""SELECT p.track_id, p.sample_rate, p.duration_ms, p.ref_lufs, p.start_ramp,
                       p.end_ramp, p.analyzer_ver, p.analyzed_at, p.media_signature,
                       edge.payload
                  FROM {P}published_source_profiles p {store.edge_join()}
                 WHERE p.catalog_instance_id=%s AND p.track_id=%s""", (SOURCE, track))
    payload = enrichment.serialize_profile(*rows[0][:9], edge_profile=rows[0][9])
    assert "edge_profile" in payload
    _execute(db, f"UPDATE {P}profile_changes SET payload=%s::jsonb, edge_ref=NULL "
                 "WHERE catalog_instance_id=%s AND seq=%s",
             (enrichment._profile_json(payload), SOURCE, seq))
    return seq, payload


def test_mixed_old_and_new_journal_rows_read_correctly_in_both_modes(source_db):
    db, client = source_db, _app()
    for track in TRACKS[:3]:
        _complete(db, track, -14.0)
        _publish_edge(db, track)
    plain = _v2_create(client, 3)[0][2]
    refs = _v2_create(client, 3, edge_refs=True)[0][2]
    after = _head(db)
    # Old format: a pre-K6 1.3.0 waveform event (edge embedded), and a
    # 1.2.5-style event (canonical_json payload, float64 ref_lufs, embedded
    # edge) inserted as the old writer did before the upgrade fence.
    old_seq, old_payload = _pre_k6_event(db, TRACKS[0], -13.0, b"wave-2")
    legacy = dict(old_payload, ref_lufs=-12.123456789, track_id=TRACKS[1],
                  edge_profile=_stored_edge(db, TRACKS[1]))
    legacy["media_revision"] = legacy["media_signature"] = legacy["edge_profile"]["media_revision"]
    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}profile_stream_state SET head_seq=head_seq + 1 "
                    "WHERE catalog_instance_id=%s RETURNING head_seq", (SOURCE,))
        legacy_seq = cur.fetchone()[0]
        cur.execute(f"""INSERT INTO {P}profile_changes (catalog_instance_id, epoch, seq,
                            track_id, operation, payload, writer_generation)
                        VALUES (%s, %s, %s, %s, 'upsert', %s::jsonb, 2)""",
                    (SOURCE, PROFILE_EPOCH, legacy_seq, TRACKS[1],
                     enrichment.canonical_json(legacy)))
    db.commit()
    # New format after the upgrade: kept, and an edge publication.
    _complete(db, TRACKS[2], -12.0, start=b"wave-2")
    kept_seq = _head(db)
    new_edge = _publish_edge(db, TRACKS[0], variant="b")      # replaces old_seq's edge
    published_seq = _head(db)
    journal = _journal(db, after)
    assert journal[old_seq][3] is None and "edge_profile" in journal[old_seq][2]
    assert journal[legacy_seq][3] is None and "edge_profile" in journal[legacy_seq][2]
    assert journal[kept_seq][3]["kept"] is True

    stored = {seq: row[2] for seq, row in journal.items()}
    for mode in (None, "1"):
        events = _events(client, after, edge_refs=mode)
        # Old rows: exactly as stored, full edge, never a reference, even when
        # their edge was replaced since (the frozen pre-K6 payload).
        for seq in (old_seq, legacy_seq):
            assert events[seq]["payload"] == stored[seq], (mode, seq)
        assert events[old_seq]["payload"]["edge_profile"] == old_payload["edge_profile"]
        assert events[published_seq]["payload"]["edge_profile"] == new_edge
    kept_edge = _stored_edge(db, TRACKS[2])
    assert _events(client, after)[kept_seq]["payload"]["edge_profile"] == kept_edge
    assert _events(client, after, edge_refs="1")[kept_seq]["payload"]["edge_profile_ref"] == \
        _ref(kept_edge)

    # v2 catch-up over the mixed interval: old rows are split and resolved
    # (K2), so the replaced edge is omitted; never a reference for them.
    for session, opted in ((plain, False), (refs, True)):
        events = _catchup_events(client, session["session_token"])
        assert "edge_profile" not in events[old_seq]["payload"]
        assert "edge_profile_ref" not in events[old_seq]["payload"]
        assert events[legacy_seq]["payload"]["edge_profile"] == _stored_edge(db, TRACKS[1])
        assert events[legacy_seq]["payload"]["ref_lufs"] == -12.123456789
        assert events[published_seq]["payload"]["edge_profile"] == new_edge
        if opted:
            assert events[kept_seq]["payload"]["edge_profile_ref"] == _ref(kept_edge)
        else:
            assert events[kept_seq]["payload"]["edge_profile"] == kept_edge
    _assert_replacement_follows(db)


def test_upgrade_adds_the_columns_and_never_rewrites_old_rows(source_db, run_plugin_migration):
    """The K6 migration is additive: a pre-K6 schema gains the nullable
    columns, its journal rows keep their payload, and a re-run is a no-op."""
    db, client = source_db, _app()
    _complete(db, TRACKS[0], -14.0)
    _publish_edge(db, TRACKS[0])
    _execute(db, f"ALTER TABLE {P}profile_changes DROP COLUMN edge_ref")
    _execute(db, f"ALTER TABLE {P}profile_bootstrap_sessions DROP COLUMN edge_refs")
    # A pre-K6 row with an embedded edge, written before the upgrade.
    with db.cursor() as cur:
        cur.execute(f"SELECT payload FROM {P}profile_changes ORDER BY seq DESC LIMIT 1")
        payload = dict(cur.fetchone()[0], edge_profile=_stored_edge(db, TRACKS[0]))
        cur.execute(f"UPDATE {P}profile_changes SET payload=%s::jsonb "
                    "WHERE catalog_instance_id=%s AND seq=2",
                    (enrichment._profile_json(payload), SOURCE))
    db.commit()
    before = _execute(db, f"SELECT seq, payload::text, xmin::text FROM {P}profile_changes "
                          "ORDER BY seq")
    run_plugin_migration(db)
    run_plugin_migration(db)
    after = _execute(db, f"SELECT seq, payload::text, xmin::text FROM {P}profile_changes "
                         "ORDER BY seq")
    assert after == before  # not rewritten (same row versions)
    assert _execute(db, f"SELECT count(*) FROM {P}profile_changes WHERE edge_ref IS NULL") \
        == [(2,)]
    columns = _execute(db, """SELECT table_name, column_name, is_nullable, column_default
                                FROM information_schema.columns
                               WHERE table_schema=current_schema()
                                 AND column_name IN ('edge_ref', 'edge_refs')
                                 AND table_name LIKE '%%profile_%%'
                               ORDER BY table_name""")
    assert (f"{P}profile_changes", "edge_ref", "YES", None) in columns
    assert (f"{P}profile_bootstrap_sessions", "edge_refs", "NO", "false") in columns
    events = _events(client, 0, edge_refs="1")
    assert events[2]["payload"] == payload


# ---------------------------------------------------------------------------
# Size, and the miss-fetch route.
# ---------------------------------------------------------------------------

REAL_EDGE = json.loads((Path(__file__).resolve().parents[2]
                        / "docs/audit/2026-09-24/probes/lum010/edge.json").read_text())


def _seed_library(db, count, template, prefix="lib-"):
    """``count`` published profiles on media m1 (45-byte ramps, like the perf
    fixture) with an edge each from ``template``; returns the track ids."""
    from plugins.LumaeAnalysis.edge_profiles import opaque_revision, profile_digest
    from psycopg2.extras import execute_values

    tracks = [f"{prefix}{n:05d}" for n in range(count)]
    with db.cursor() as cur:
        execute_values(cur, f"""INSERT INTO {P}catalog_tracks
            (catalog_instance_id, published_generation, track_id, title, metadata_fp,
             media_fp, analysis_eligible, payload, first_seen_at, last_seen_at)
            VALUES %s""", [(SOURCE, 1, t, t, "metadata", f"m1-{t}", True, "{}")
                           for t in tracks],
                       template="(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, now(), now())")
        execute_values(cur, f"""INSERT INTO {P}published_source_profiles
            (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs, start_ramp,
             end_ramp, analyzer_ver, profile_schema_ver, media_signature, analyzed_at)
            VALUES %s""", [(SOURCE, t, 44100, 240000, -14.5, bytes(range(45)),
                            bytes(range(45, 90)), 1, 1, f"catalog-media:m1-{t}")
                           for t in tracks],
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())")
        edges = []
        for index, track in enumerate(tracks):
            signature = f"catalog-media:m1-{track}"
            edge = dict(template, catalog_instance_id=SOURCE, track_id=track,
                        media_revision=opaque_revision(signature),
                        noise_floor_cdb=-9000 - index)
            edge["profile_digest"] = profile_digest(edge)
            edges.append((SOURCE, track, edge["media_revision"], edge["representation_id"],
                          signature, edge["profile_digest"], json.dumps(edge)))
        execute_values(cur, f"""INSERT INTO {P}edge_profiles (catalog_instance_id, track_id,
            media_revision, representation_id, media_signature, profile_digest, payload)
            VALUES %s""", edges, template="(%s, %s, %s, %s, %s, %s, %s::jsonb)")
    db.commit()
    return tracks


def _wire(client, method, path, **kwargs):
    """(bytes on the wire with gzip accepted, decoded JSON bytes, body)."""
    import gzip

    response = getattr(client, method)(path, headers={"Accept-Encoding": "gzip"}, **kwargs)
    assert response.status_code == 200
    raw = response.data
    decoded = gzip.decompress(raw) if response.headers.get("Content-Encoding") == "gzip" else raw
    return len(raw), len(decoded), json.loads(decoded)


def test_full_waveform_only_republish_is_under_1_kb_per_event_with_the_opt_in(source_db):
    """LUM-005 at test scale: every profile's waveform changes on the same
    media. With the opt-in each event is at most 1 KB on the wire (and
    decoded), on /changes and in a v2 catch-up; without it each carries its
    ~19.5 KB edge."""
    db, client = source_db, _app()
    tracks = _seed_library(db, 300, REAL_EDGE)
    session = _v2_create(client, 100, edge_refs=True)[0][2]
    after = _head(db)
    tokens = publication.admit_attempts(db, SOURCE, tracks)
    for index, track in enumerate(tracks):
        assert publication.complete_attempt(
            db, SOURCE, track, tokens[track], SimpleNamespace(
                sample_rate=44100, duration_ms=240000, ref_lufs=-13.0 - index / 1000,
                start_ramp_blob=bytes(range(1, 46)), end_ramp_blob=bytes(range(45, 90))),
            "ready", None, f"catalog-media:m1-{track}", 1, 1)
    assert all(row[3] and row[3].get("kept") for row in _journal(db, after).values())

    def stream(edge_refs):
        wire = decoded = events = 0
        cursor = opaque_cursor(SOURCE, PROFILE_EPOCH, after)
        while True:
            query = {"cursor": cursor, "catalog_instance_id": SOURCE, "limit": 100}
            if edge_refs:
                query["edge_refs"] = "1"
            sent, size, body = _wire(client, "get", "/api/profiles/changes",
                                     query_string=query)
            wire, decoded, events = wire + sent, decoded + size, events + len(body["changes"])
            cursor = body["cursor"]
            if not body["has_more"]:
                return wire / events, decoded / events, events

    wire, decoded, events = stream(True)
    assert events == len(tracks)
    assert wire <= 1024 and decoded <= 1024, (wire, decoded)
    # Without the opt-in every event carries its edge. (These template edges
    # differ in a few fields only, so gzip hides most of that here; real
    # edges differ throughout: 8.3 KB per event gzipped in the e2e gate.)
    _full_wire, full_decoded, _ = stream(False)
    assert full_decoded > 19_000 > 18 * decoded, (full_decoded, decoded)

    wire = decoded = events = 0
    page_token = None
    while True:
        extra = {"page_token": page_token} if page_token else {}
        sent, size, body = _wire(client, "post", V2 + "/catchup",
                                 json=_v2_body(session_token=session["session_token"], **extra))
        wire, decoded, events = wire + sent, decoded + size, events + len(body["changes"])
        page_token = body["next_page_token"]
        if not body["has_more"]:
            break
    assert events == len(tracks)
    assert wire / events <= 1024 and decoded / events <= 1024, (wire / events, decoded / events)


def test_api_profiles_returns_full_edges_for_the_first_500_ids(source_db):
    """The C-10 miss fetch: /api/profiles?ids= returns each requested
    published profile with its full current edge, for up to 500 ids; ids past
    the 500th are ignored (neither returned nor listed as missing)."""
    small = json.loads((Path(__file__).resolve().parent
                        / "edge_profile_v2_golden.json").read_text())
    db, client = source_db, _app()
    tracks = _seed_library(db, 505, small)
    ids = tracks[::-1] + ["unknown-id"]
    _sent, _size, body = _wire(client, "get", "/api/profiles",
                               query_string={"catalog_instance_id": SOURCE,
                                             "ids": ",".join(ids)})
    assert [p["track_id"] for p in body["profiles"]] == ids[:500]
    assert body["missing"] == [] and body["failed"] == []
    stored = dict(_execute(db, f"SELECT track_id, payload FROM {P}edge_profiles "
                               "WHERE catalog_instance_id=%s", (SOURCE,)))
    for profile in body["profiles"]:
        assert profile["edge_profile"] == stored[profile["track_id"]]
        assert profile["edge_profile"]["media_revision"] == profile["media_revision"]
