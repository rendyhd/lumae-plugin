"""P2-4: the SQL-side v2 capture against the Python capture it replaced.

The pre-P2-4 capture is kept below, verbatim, as the oracle: the COPY helpers,
``_capture_snapshot``, and ``snapshot_page`` and ``catchup_page`` (whose first
call captured the catch-up in Python). It runs against the unchanged helpers of
``profile_bootstrap``. Both captures take the same fixture, full of the values
where SQL and Python part ways, and everything observable is compared:

* the stored snapshot and catch-up rows, column by column: ``payload`` and
  ``edge_ref`` as JSONB text, so a number's scale counts (``-14.0`` is not
  ``-14``), and as decoded JSON; ``ordinal`` and ``seq``;
* the pages over HTTP with gzip off, raw bytes and decoded JSON, after
  replacing the per-session values (page tokens, ``expires_at``);
* the byte counts behind MAX_SNAPSHOT_BYTES and MAX_CATCHUP_BYTES, per row
  and at the caps' boundaries, and the row and event caps.

It also covers what P2-4 changes around the capture: the page queries pick
their rows before the edge lookup (a fresh table without statistics), and a
backend killed during either capture leaves nothing half visible.
"""

import io
import json
import os
import types
import uuid
from fractions import Fraction

import numpy as np
import pytest
from flask import Flask

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import load_plugin, plugin_api_module
from plugins.LumaeAnalysis import catalog_enrichment, profile_bootstrap
from plugins.LumaeAnalysis.edge_profile_store import edge_join
from plugins.LumaeAnalysis.edge_profiles import opaque_revision


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
PUBLISHED = P + "published_source_profiles"
EDGES = P + "edge_profiles"
CHANGES = P + "profile_changes"
STATE = P + "profile_stream_state"
SNAPSHOT = P + "profile_bootstrap_snapshot"
CATCHUP = P + "profile_bootstrap_catchup"
SESSIONS = P + "profile_bootstrap_sessions"
CREATES = P + "profile_bootstrap_creates"
ROUTE = "/api/profiles/bootstrap/sessions"
PROFILE_COLUMNS = ("p.track_id, p.sample_rate, p.duration_ms, p.ref_lufs, p.start_ramp, "
                   "p.end_ramp, p.analyzer_ver, p.analyzed_at, p.media_signature")


# ---------------------------------------------------------------------------
# Oracle: plugins/LumaeAnalysis/profile_bootstrap.py at phase/2-performance
# cd298d7 (before P2-4), lines 428-481, 632-687 and 735-857, verbatim.
# ---------------------------------------------------------------------------
_ORACLE_SOURCE = r'''
_EDGE_REF = '{"profile_digest":%s}'
_json_string = json.encoder.encode_basestring_ascii


def _copy_value(value):
    if value is None:
        return "\\N"
    text = str(value)
    if "\\" in text:
        text = text.replace("\\", "\\\\")
    if "\t" in text or "\n" in text or "\r" in text:
        text = text.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
    return text


def _copy_rows(cur, name, columns, rows):
    """Bulk-append rows with COPY: multi-insert WAL records and no per-row
    statement parsing (most of a 94k capture's time and WAL)."""
    if not rows:
        return
    buffer = io.StringIO()
    for row in rows:
        buffer.write("\t".join(_copy_value(value) for value in row))
        buffer.write("\n")
    buffer.seek(0)
    cur.copy_expert(f"COPY {_table(name)} ({', '.join(columns)}) FROM STDIN", buffer)


def _edge_lookup(alias, profile):
    """Resolve a stored edge_ref to the live edge_profiles payload (K2).

    ``profile`` is the SQL path of the row's waveform payload. The edge row is
    matched on (catalog_instance_id, track_id, media_revision,
    profile_digest). A reference whose edge was replaced or withdrawn after
    capture finds no row, and the row is returned without ``edge_profile``:
    the catch-up (or the later /changes stream) carries the replacing event.
    Rows captured before 1.3.0 have a NULL ``edge_ref`` and embed their edge.

    Invariant: an edge_profiles row's ``track_id`` and ``media_revision``
    columns equal the same fields of its payload (publish_edge_profile checks
    the payload against the job before inserting it, and it is the only
    writer). So matching the columns is the embedding check serialize_profile
    makes on the payload, and pages never detoast the edge to re-check it.
    """
    return f"""LEFT JOIN LATERAL (
        SELECT e.payload FROM {_table('edge_profiles')} e
         WHERE {alias}.edge_ref IS NOT NULL
           AND e.catalog_instance_id=%s
           AND e.track_id={profile}->>'track_id'
           AND e.media_revision=COALESCE({alias}.edge_ref->>'media_revision',
                                         {profile}->>'media_revision')
           AND e.profile_digest={alias}.edge_ref->>'profile_digest'
         ORDER BY e.updated_at DESC LIMIT 1
    ) edge ON TRUE"""


def _capture_snapshot(db, source, session_id, admitted):
    """The REPEATABLE READ part of a capture; returns (count, snapshot_seq)."""
    with db.cursor() as cur:
        # The first statement takes the MVCC snapshot the capture reads.
        state = _state(cur, source)
        if (state[0], state[2], state[3]) != (admitted[0], admitted[2], admitted[3]):
            gone()
        cur.execute(f"SELECT expires_at > now() FROM {_table('profile_bootstrap_sessions')} "
                    "WHERE session_id=%s", (session_id,))
        row = cur.fetchone()
        if row is None or not row[0]:
            gone()  # replaced by a retried create (K5) while it waited
        reader = db.cursor(name=f"profile_snapshot_{session_id.replace('-', '')}")
        reader.itersize = 500
        # The same edge row edge_join() picks, but only its key columns,
        # so the TOASTed edge payload is never detoasted or copied;
        # snapshot_page resolves the reference (K2).
        reader.execute(
            f"""SELECT p.track_id, p.sample_rate, p.duration_ms, p.ref_lufs,
                       p.start_ramp, p.end_ramp, p.analyzer_ver, p.analyzed_at,
                       p.media_signature, edge.media_revision, edge.profile_digest
                  FROM {_table('published_source_profiles')} p
                  {edge_join(columns='e.media_revision, e.profile_digest')}
                 WHERE p.catalog_instance_id=%s ORDER BY p.track_id""",
            (source,))
        ordinal = 0
        byte_count = 0
        while True:
            rows = reader.fetchmany(500)
            if not rows:
                break
            batch = []
            for row in rows:
                payload = serialize_profile(*row[:9])
                text = json.dumps(payload, separators=(",", ":"))
                byte_count += len(text.encode())
                ordinal += 1
                if ordinal > MAX_SNAPSHOT_ROWS or byte_count > MAX_SNAPSHOT_BYTES:
                    raise BootstrapError("bootstrap_snapshot_limit", 413)
                revision = payload.get("media_revision")
                # serialize_profile embeds an edge only for the profile's
                # current revision; snapshot_page re-checks the payload.
                edge_ref = (_EDGE_REF % _json_string(row[10])
                            if revision and row[9] == revision and row[10] else None)
                batch.append((session_id, ordinal - 1, text, edge_ref))
            _copy_rows(cur, "profile_bootstrap_snapshot",
                       ("session_id", "ordinal", "payload", "edge_ref"), batch)
        reader.close()
        # The captured head replaces the admission head the hold used so far.
        snapshot_seq = int(state[4])
        cur.execute(
            f"""UPDATE {_table('profile_bootstrap_sessions')}
                   SET snapshot_seq=%s, snapshot_count=%s, state='ready'
                 WHERE session_id=%s""", (snapshot_seq, ordinal, session_id))
    return ordinal, snapshot_seq


def snapshot_page(body):
    _require_request(body)
    with _connection() as db:
        with db.cursor() as cur:
            session, _ = _session(cur, body, lock=True)
            ordinal = _ordinal(session, "snapshot", body.get("page_token"))
            if ordinal > session[11] or (ordinal != 0 and ordinal % session[8]):
                invalid()
            session = _served(cur, session)
            # Built in SQL, so the JSONB (and so the wire JSON) is exactly what
            # the pre-K2 capture stored with the edge embedded. The lookup
            # matches the row's track_id and media_revision (see _edge_lookup),
            # so the edge payload is detoasted once, only to embed it.
            cur.execute(
                f"""SELECT CASE WHEN edge.payload IS NOT NULL
                            THEN s.payload || jsonb_build_object('edge_profile', edge.payload)
                            ELSE s.payload END
                      FROM {_table('profile_bootstrap_snapshot')} s
                      {_edge_lookup('s', 's.payload')}
                     WHERE s.session_id=%s AND s.ordinal>=%s ORDER BY s.ordinal LIMIT %s""",
                (session[4], session[0], ordinal, session[8]))
            profiles = [row[0] for row in cur.fetchall()]
            following = ordinal + len(profiles)
            more = following < session[11]
            return {"protocol_version": 2, "schema_version": 1,
                    "transfer_contract": TRANSFER_CONTRACT,
                    "catalog_instance_id": session[4], "profiles": profiles,
                    **_metadata(session),
                    "cursor": opaque_cursor(session[4], session[7], session[9]),
                    "next_page_token": _page_token(session, "snapshot", following) if more else None,
                    "has_more": more}


def catchup_page(body):
    _require_request(body)
    with _connection() as db:
        with db.cursor() as cur:
            session, state = _session(cur, body, lock=True)
            ordinal = _ordinal(session, "catchup", body.get("page_token"))
            if session[10] is None:
                if ordinal:
                    invalid()
                if session[9] < state[5]:
                    gone()
                head = int(state[4])
                reader = db.cursor(name=f"profile_catchup_{str(session[0]).replace('-', '')}")
                reader.itersize = 500
                # An upsert's embedded edge is split off in SQL (it never
                # reaches Python) and kept as a reference, like the snapshot.
                reader.execute(
                    f"""SELECT seq, track_id, operation,
                               CASE WHEN split THEN payload - 'edge_profile' ELSE payload END,
                               created_at,
                               CASE WHEN split THEN jsonb_strip_nulls(jsonb_build_object(
                                   'media_revision', NULLIF(payload#>'{{edge_profile,media_revision}}',
                                                            payload->'media_revision'),
                                   'profile_digest', payload#>'{{edge_profile,profile_digest}}')
                               )::text END
                          FROM (SELECT seq, track_id, operation, payload, created_at,
                                       COALESCE(
                                           jsonb_typeof(payload#>'{{edge_profile,media_revision}}')='string'
                                           AND jsonb_typeof(payload#>'{{edge_profile,profile_digest}}')='string',
                                           FALSE) AS split
                                  FROM {_table('profile_changes')}
                                 WHERE catalog_instance_id=%s AND epoch=%s
                                   AND seq>%s AND seq<=%s) j
                         ORDER BY seq""",
                    (session[4], session[7], session[9], head))
                max_events, max_bytes = catchup_limits(state[6])
                count = 0
                bytes_used = 0
                expected_seq = session[9] + 1
                while True:
                    rows = reader.fetchmany(500)
                    if not rows:
                        break
                    batch = []
                    for seq, track_id, operation, payload, created_at, edge_ref in rows:
                        if seq != expected_seq:
                            gone()
                        expected_seq += 1
                        event = {"seq": int(seq), "track_id": track_id,
                                 "operation": operation, "payload": payload,
                                 "created_at": _iso(created_at)}
                        text = json.dumps(event, separators=(",", ":"))
                        bytes_used += len(text.encode())
                        count += 1
                        if count > max_events or bytes_used > max_bytes:
                            raise BootstrapError("bootstrap_snapshot_limit", 413)
                        batch.append((session[0], count - 1, seq, text, edge_ref))
                    _copy_rows(cur, "profile_bootstrap_catchup",
                               ("session_id", "ordinal", "seq", "payload", "edge_ref"), batch)
                reader.close()
                if expected_seq != head + 1:
                    gone()
                cur.execute(f"UPDATE {_table('profile_bootstrap_sessions')} SET head_seq=%s "
                            "WHERE session_id=%s", (head, session[0]))
                session = tuple(session[:10]) + (head,) + tuple(session[11:])
            head = session[10]
            count = head - session[9]
            if ordinal > count or (ordinal != 0 and ordinal % session[8]):
                invalid()
            session = _served(cur, session)
            cur.execute(
                f"""SELECT CASE WHEN edge.payload IS NOT NULL
                            THEN jsonb_set(c.payload, '{{payload,edge_profile}}', edge.payload)
                            ELSE c.payload END
                      FROM {_table('profile_bootstrap_catchup')} c
                      {_edge_lookup('c', "c.payload->'payload'")}
                     WHERE c.session_id=%s AND c.ordinal>=%s ORDER BY c.ordinal LIMIT %s""",
                (session[4], session[0], ordinal, session[8]))
            changes = [row[0] for row in cur.fetchall()]
            following = ordinal + len(changes)
            more = following < count
            next_seq = changes[-1]["seq"] if changes else (head if not more else session[9])
            return {"protocol_version": 2, "schema_version": 1,
                    "transfer_contract": TRANSFER_CONTRACT,
                    "catalog_instance_id": session[4], "changes": changes,
                    **_metadata(session),
                    "cursor": opaque_cursor(session[4], session[7], next_seq),
                    "head_cursor": opaque_cursor(session[4], session[7], head),
                    "next_page_token": _page_token(session, "catchup", following) if more else None,
                    "has_more": more}
'''


def _oracle():
    namespace = dict(vars(profile_bootstrap))
    namespace.update(io=io, json=json, edge_join=edge_join,
                     serialize_profile=catalog_enrichment.serialize_profile)
    exec(compile(_ORACLE_SOURCE, "profile_bootstrap_cd298d7", "exec"), namespace)
    return types.SimpleNamespace(namespace=namespace, **{
        name: namespace[name] for name in (
            "_copy_value", "_capture_snapshot", "snapshot_page", "catchup_page")})


ORACLE = _oracle()


def test_oracle_copy_value_escapes_copy_text_format():
    """The oracle's COPY escaping (moved here from the edge-ref tests)."""
    copy_value = ORACLE._copy_value
    assert copy_value(None) == "\\N"
    assert copy_value("\\N") == "\\\\N"
    assert copy_value("a\tb\nc\rd\\e") == "a\\tb\\nc\\rd\\\\e"
    # The backslash is doubled first, so an escaped tab is not re-escaped.
    assert copy_value("\\\t") == "\\\\\\t"
    assert copy_value("\\.") == "\\\\."
    assert copy_value(12) == "12"
    assert copy_value("plain 曲 \U0001F3B5") == "plain 曲 \U0001F3B5"


# ---------------------------------------------------------------------------
# Fixture: the values where SQL and Python part ways.
# ---------------------------------------------------------------------------

# REAL values as text. serialize_profile writes float4(): numpy's shortest text
# of the float4 psycopg2's double rounds to, as Python's float repr (fixed
# notation with a decimal between 1e-4 and 1e16, exponent notation outside),
# null for NaN and infinity. Plain values are written in SQL, the others
# (NaN, infinities, |x| >= 2^23, 0 < |t| < 1e-4) by serialize_profile.
LUFS = [
    "-14.2", "-14.199999809265137", "-14", "-14.5", "0", "-0", "0.1", "-70.123456",
    "-23.0000019", "1e-5", "-1.5e-05", "0.0001", "0.00009999", "123456.7", "1e6",
    "1234567", "8388607.5", "8388608", "16777217", "33554450", "1e15", "9.999999e15",
    "1e16", "3.4028235e38", "-3.4028235e38", "3.4026e38", "1.4e-45", "1.17549435e-38",
    "NaN", "Infinity", "-Infinity",
    # float4 0x15ae43fd: psycopg2 reads 7.038531e-26, and its double rounds to
    # the neighbouring float4, so float4() gives 7.0385313e-26 (review LOW-2).
    "7.038531e-26", "-7.038531e-26",
]
# analyzed_at (TIMESTAMP): isoformat() drops ".000000"; +-infinity are
# psycopg2's datetime.max/min; years below 1000 keep four digits.
STAMPS = [
    "2026-09-01 12:00:00", "2026-09-01 12:00:00.5", "2026-09-01 12:00:00.000001",
    "2026-09-01 12:00:00.123456", "0099-01-02 03:04:05", "0001-01-01 00:00:00",
    "9999-12-31 23:59:59.999999", "infinity", "-infinity",
]
# encode(..., 'base64') breaks lines every 76 characters (57 bytes).
RAMPS = [b"", b"\x00", b"\x00\x01", b"\xff\xfe\xfd", bytes(range(45)), bytes(range(57)),
         bytes(range(58)), bytes(range(200)), bytes(255 - n for n in range(114))]
INTS = [0, 1, -1, 44100, 2147483647, -2147483648]
# JSON escaping differs in json.dumps (ensure_ascii) and PostgreSQL.
TRACK_IDS = [
    "plain", "tab\there", "back\\slash", "new\nline", "\\N", "carriage\rreturn",
    'quote"d', "\\.", "ctl\x01\x1b\x1f\x7f", "del\x7f", "é-accent", "CJK曲目",
    "emoji\U0001F3B5", "line sep", "\\", "trailing\\", "slash/ok", "", " space ",
]
SIGNATURES = [None, "", "sig", "sïg-曲-\U0001F3B5", "s" * 300]


def _edge(track_id, revision, digest, **extra):
    return {"schema_version": 2, "track_id": track_id, "media_revision": revision,
            "profile_digest": digest, "representation_id": "rep", "loudness": [1, 2.5, -0.0],
            "note": "é\U0001F3B5", **extra}


def _float32_texts(count, seed):
    """Random float32 bit patterns of every magnitude, as REAL input text."""
    bits = np.random.RandomState(seed).randint(0, 2 ** 32, size=count, dtype=np.uint64)
    values = bits.astype(np.uint32).view(np.float32)
    texts = []
    for value in values:
        if np.isnan(value):
            texts.append("NaN")
        elif np.isinf(value):
            texts.append("Infinity" if value > 0 else "-Infinity")
        else:
            texts.append(str(value))
    return texts


def _loudness_texts(count, seed, low=-80.0, high=20.0):
    """Random float32 values of analyzed loudness, as REAL input text."""
    values = np.random.RandomState(seed).uniform(low, high, size=count).astype(np.float32)
    return [str(value) for value in values]


def _seed_profiles(db):
    """Profiles cycling through every trap, edges of every kind, and random
    float32 levels. Returns the number of profiles."""
    rows = []
    for index in range(len(TRACK_IDS) * 3):
        signature = SIGNATURES[index % len(SIGNATURES)]
        rows.append((
            f"{TRACK_IDS[index % len(TRACK_IDS)]}#{index:03d}",
            INTS[index % len(INTS)], INTS[(index + 2) % len(INTS)],
            LUFS[index % len(LUFS)], RAMPS[index % len(RAMPS)],
            RAMPS[(index + 4) % len(RAMPS)], INTS[(index + 3) % len(INTS)],
            f"{signature}{index}" if signature else signature, STAMPS[index % len(STAMPS)]))
    for index, lufs in enumerate(LUFS + _float32_texts(1500, 20260925)
                                 + _loudness_texts(1500, 20260926)):
        rows.append((f"rand-{index:05d}", 44100, 180000 + index, lufs,
                     RAMPS[index % len(RAMPS)], RAMPS[(index + 1) % len(RAMPS)], 1,
                     f"rand-sig-{index}", STAMPS[index % len(STAMPS)]))
    with db.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate, duration_ms, "
            "ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, "
            "media_signature, analyzed_at) "
            "VALUES (%s, %s, %s, %s, %s::real, %s, %s, %s, 1, %s, %s::timestamp)",
            [(SOURCE, *row) for row in rows])
        edges = []
        for index, (track_id, *_rest, signature, _stamp) in enumerate(rows):
            revision = opaque_revision(signature)
            kind = index % 8
            if kind == 0 or signature is None:
                continue  # no edge (and none can match a NULL signature)
            if kind in (1, 2, 3):  # the current edge
                edges.append((track_id, revision or "none", "rep", signature,
                              f"digest-{index}", "2026-09-01 00:00:00"))
            elif kind == 4:  # an edge of an older signature only
                edges.append((track_id, "sha256:old", "rep", f"{signature}-old",
                              f"digest-{index}", "2026-09-01 00:00:00"))
            elif kind == 5:  # matching signature, other revision column
                edges.append((track_id, "sha256:other", "rep", signature,
                              f"digest-{index}", "2026-09-01 00:00:00"))
            elif kind == 6:  # an empty digest is no reference
                edges.append((track_id, revision or "none", "rep", signature, "",
                              "2026-09-01 00:00:00"))
            else:  # several: the newest wins, then the lowest digest
                for rep, digest, stamp in (("a", "digest-b", "2026-09-02 00:00:00"),
                                           ("b", "digest-a", "2026-09-02 00:00:00"),
                                           ("c", "digest-0", "2026-09-01 00:00:00")):
                    edges.append((track_id, revision or "none", rep, signature, digest, stamp))
        cur.executemany(
            f"INSERT INTO {EDGES} (catalog_instance_id, track_id, media_revision, "
            "representation_id, media_signature, profile_digest, payload, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::timestamp)",
            [(SOURCE, track_id, revision, rep, signature, digest,
              json.dumps(_edge(track_id, revision, digest)), stamp)
             for track_id, revision, rep, signature, digest, stamp in edges])
    db.commit()
    return len(rows)


# Journal payloads the Python capture parsed and dumped again: numbers whose
# text is no float repr, nested values, every escape.
ODD_PAYLOAD = {
    "track_id": "odd", "ref_lufs": 1e-05, "tiny": [1e-07, -1.5e-300, 5e-324],
    "huge": [1e16, 1e22, 1.5e300, 1.7976931348623157e308, 123456789012345680.0],
    "repr": [0.30000000000000004, -0.0, 0.0, -14.0, 100.5, 1e15, 0.0001, 9.999e-05],
    "ints": [0, -1, 123456789012345678901234567890],
    "strings": ["x\x7fy", "é", "\U0001F3B5", " ", 'q"b\\s', "tab\t", "", "a: b, c"],
    "é-key": {"emoji\U0001F3B5": {}, "list": [], "t": True, "f": False, "n": None},
}


def _journal(db, profiles):
    """Events after the sessions' snapshot: upserts through the real
    publication serializer (with, without and with a foreign edge), deletes,
    odd payloads, an unsplittable edge, and timestamps of every shape."""
    with db.cursor() as cur:
        cur.execute(f"SELECT {PROFILE_COLUMNS}, edge.payload FROM {PUBLISHED} p {edge_join()} "
                    "ORDER BY p.track_id")
        rows = cur.fetchall()
        for index, row in enumerate(rows[:len(TRACK_IDS) * 3 + len(LUFS)]):
            track_id, signature = row[0], row[8]
            revision = opaque_revision(signature)
            # The stored edge (pages embed it again), none, or a foreign one.
            payload = catalog_enrichment.serialize_profile(
                *row[:9], edge_profile=row[9] if index % 4 != 3 else None)
            if index % 4 == 2 and revision:
                # A journalled edge of another revision than the payload's.
                payload["edge_profile"] = _edge(track_id, "sha256:next", f"next-{index}")
            catalog_enrichment.record_profile_change(cur, SOURCE, track_id, "ready", payload)
            if index % 9 == 3:
                catalog_enrichment.record_profile_change(cur, SOURCE, track_id, "deleted")
        catalog_enrichment.record_profile_deletions(cur, SOURCE, ["gone-1", "gone-\U0001F3B5"])
        catalog_enrichment.record_profile_change(cur, SOURCE, "odd", "ready", ODD_PAYLOAD)
        catalog_enrichment.record_profile_change(
            cur, SOURCE, "unsplit", "ready",
            {"track_id": "unsplit", "edge_profile": {"media_revision": 7, "profile_digest": "d",
                                                     "values": [1.25, 1e-06]}})
        catalog_enrichment.record_profile_change(
            cur, SOURCE, "half", "ready",
            {"track_id": "half", "edge_profile": {"media_revision": "sha256:x"}})
        cur.execute(
            f"""UPDATE {CHANGES} c SET created_at = s.stamp
                  FROM (SELECT seq, (ARRAY['2026-09-01 12:00:00+00', '2026-09-01 12:00:00.5+02',
                                           '2026-09-01 12:00:00.000001-03:30', 'infinity',
                                           '-infinity', '0099-06-01 00:00:00+00'])[seq %% 6 + 1]
                                    ::timestamptz AS stamp
                          FROM {CHANGES}) s
                 WHERE c.seq = s.seq AND c.catalog_instance_id = %s""", (SOURCE,))
        cur.execute(f"SELECT count(*) FROM {CHANGES} WHERE catalog_instance_id=%s", (SOURCE,))
        events = cur.fetchone()[0]
    db.commit()
    return events


def _use_source(migrated_db, monkeypatch, options=""):
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
                                     options=f"-c search_path={schema},public {options}"),
        raising=False)
    return migrated_db


# The owned connections also run with extra_float_digits=0 (psycopg2 then
# reads REAL values with 6 digits, and so does the SQL) and a non-UTC TimeZone,
# and with extra_float_digits=-2, where FLT_MAX reads as 3.403e+38, beyond a
# float4 (review LOW-1: that row is null, never a failed capture).
@pytest.fixture(params=["", "-c extra_float_digits=0 -c TimeZone=Asia/Kolkata",
                        "-c extra_float_digits=-2"],
                ids=["defaults", "efd0-kolkata", "efd-2"])
def db(migrated_db, monkeypatch, request):
    return _use_source(migrated_db, monkeypatch, request.param)


@pytest.fixture
def plain_db(migrated_db, monkeypatch):
    return _use_source(migrated_db, monkeypatch)


def body(**updates):
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": profile_bootstrap.TRANSFER_CONTRACT,
            "catalog_instance_id": SOURCE, **updates}


def _query(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else None
    db.commit()
    return rows


def _app():
    app = Flask(__name__)
    app.register_blueprint(load_plugin().bp)
    return app


def _post(client, path, payload):
    response = client.post(ROUTE + path, json=payload)  # no Accept-Encoding: gzip off
    assert response.status_code == 200, (response.status_code, response.data[:300])
    assert response.headers.get("Content-Encoding") is None
    return response


def _create(client, *, oracle=False):
    """Create a session over HTTP, captured in SQL or by the oracle."""
    with pytest.MonkeyPatch.context() as patch:
        if oracle:
            patch.setattr(profile_bootstrap, "_capture_snapshot", ORACLE._capture_snapshot)
        return _post(client, "", body(page_size=500)).get_json()


def _pages(client, token, path, *, oracle=False):
    """Every page of a session over HTTP: (raw bodies, decoded bodies)."""
    raw, decoded, page_token = [], [], None
    while True:
        with pytest.MonkeyPatch.context() as patch:
            if oracle:
                patch.setattr(profile_bootstrap, "catchup_page", ORACLE.catchup_page)
            extra = {"page_token": page_token} if page_token else {}
            response = _post(client, path, body(session_token=token, **extra))
        raw.append(response.data)
        decoded.append(response.get_json())
        page_token = decoded[-1]["next_page_token"]
        if not decoded[-1]["has_more"]:
            return raw, decoded


def _neutral(raw_pages, decoded_pages):
    """The raw page bytes with each per-session value (page tokens,
    expires_at) replaced by one placeholder."""
    values = set()
    for page in decoded_pages:
        values.update(v for v in (page.get("next_page_token"), page.get("expires_at")) if v)
    joined = b"\n".join(raw_pages)
    for value in values:
        joined = joined.replace(json.dumps(value).encode(), b'"<per-session>"')
    return joined


def _stored(db, table, token, *columns):
    return _query(db, f"""SELECT {', '.join(columns)} FROM {table}
                           WHERE session_id=(SELECT session_id FROM {SESSIONS} WHERE token_hash=
                                             encode(sha256(convert_to(%s, 'UTF8')), 'hex'))
                           ORDER BY ordinal""", (token,))


def _owned_like():
    """A connection with the owned connections' options (extra_float_digits
    and TimeZone decide what psycopg2 reads)."""
    return psycopg2.connect(plugin_api_module.config.DATABASE_URL)


def _python_snapshot_bytes(db):
    """len(json.dumps(serialize_profile(row))) per profile, as the oracle counts."""
    rows = _query(db, f"SELECT {PROFILE_COLUMNS} FROM {PUBLISHED} p "
                      "WHERE p.catalog_instance_id=%s ORDER BY p.track_id", (SOURCE,))
    return [(row[0], len(json.dumps(catalog_enrichment.serialize_profile(*row),
                                    separators=(",", ":")).encode())) for row in rows]


def _python_catchup_bytes(db, after):
    """len(json.dumps(event)) per journal event, as the oracle counts."""
    rows = _query(db, f"""
        SELECT seq, track_id, operation,
               CASE WHEN COALESCE(
                        jsonb_typeof(payload#>'{{edge_profile,media_revision}}')='string'
                        AND jsonb_typeof(payload#>'{{edge_profile,profile_digest}}')='string',
                        FALSE)
                    THEN payload - 'edge_profile' ELSE payload END, created_at
          FROM {CHANGES} WHERE catalog_instance_id=%s AND seq>%s ORDER BY seq""",
                  (SOURCE, after))
    return [len(json.dumps({"seq": int(seq), "track_id": track_id, "operation": operation,
                            "payload": payload, "created_at": profile_bootstrap._iso(created_at)},
                           separators=(",", ":")).encode())
            for seq, track_id, operation, payload, created_at in rows]


def _reset_rate_limit(db):
    _query(db, f"DELETE FROM {CREATES}")


# ---------------------------------------------------------------------------
# Equivalence
# ---------------------------------------------------------------------------


def test_sql_capture_equals_the_python_capture(db):
    """Stored rows and pages of the SQL capture equal the oracle's, with the
    default batches and with batches of 7 profiles and 3 events."""
    profiles = _seed_profiles(db)
    client = _app().test_client()
    old = _create(client, oracle=True)
    new = _create(client)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(profile_bootstrap, "SNAPSHOT_BATCH_ROWS", 7)
        small = _create(client)
    per_session = ("session_token", "expires_at", "next_page_token")
    for created in (new, small):
        assert {k: v for k, v in created.items() if k not in per_session} == \
            {k: v for k, v in old.items() if k not in per_session}
    assert old["snapshot_count"] == profiles
    tokens = {"old": old["session_token"], "new": new["session_token"],
              "small": small["session_token"]}

    # Stored snapshot rows, column by column: JSONB text (scale included)
    # and decoded values.
    columns = ("ordinal", "payload::text", "edge_ref::text", "payload", "edge_ref")
    expected = _stored(db, SNAPSHOT, tokens["old"], *columns)
    assert [row[0] for row in expected] == list(range(profiles))
    assert sum(row[2] is not None for row in expected) > 50
    for name in ("new", "small"):
        assert _stored(db, SNAPSHOT, tokens[name], *columns) == expected, name

    # Pages over HTTP, raw bytes and decoded.
    old_raw, old_pages = _pages(client, tokens["old"], "/page")
    assert sum("edge_profile" in p for page in old_pages for p in page["profiles"]) > 50
    for name in ("new", "small"):
        raw, pages = _pages(client, tokens[name], "/page")
        assert _neutral(raw, pages) == _neutral(old_raw, old_pages), name
        assert [page["profiles"] for page in pages] == \
            [page["profiles"] for page in old_pages], name
    # The pre-P2-4 page query serves the same rows: the plan fix changes no output.
    oracle_page = ORACLE.snapshot_page(body(session_token=tokens["new"]))
    assert oracle_page["profiles"] == old_pages[0]["profiles"]

    # The catch-up of the same journal interval, with 3-event batches too.
    events = _journal(db, profiles)
    old_raw, old_changes = _pages(client, tokens["old"], "/catchup", oracle=True)
    columns = ("ordinal", "seq", "payload::text", "edge_ref::text", "payload", "edge_ref")
    expected = _stored(db, CATCHUP, tokens["old"], *columns)
    assert [row[1] for row in expected] == list(
        range(old["snapshot_seq"] + 1, old["snapshot_seq"] + 1 + events))
    assert sum(row[3] is not None for row in expected) > 10
    assert any(row[3] and "media_revision" in row[5] for row in expected)
    for name, batch in (("new", profile_bootstrap.CATCHUP_BATCH_EVENTS), ("small", 3)):
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(profile_bootstrap, "CATCHUP_BATCH_EVENTS", batch)
            raw, changes = _pages(client, tokens[name], "/catchup")
        assert _stored(db, CATCHUP, tokens[name], *columns) == expected, name
        assert _neutral(raw, changes) == _neutral(old_raw, old_changes), name
        assert [page["changes"] for page in changes] == \
            [page["changes"] for page in old_changes], name
    assert sum(c["payload"] is not None and "edge_profile" in c["payload"]
               for page in old_changes for c in page["changes"]) > 10
    # The pre-P2-4 catch-up page query serves the same events.
    again = ORACLE.catchup_page(body(session_token=tokens["new"]))
    assert again["changes"] == old_changes[0]["changes"]


def _round32(text):
    """The float4 nearest the decimal ``text`` (ties to even), exactly."""
    exact = Fraction(text)
    guess = np.float32(float(text))
    candidates = (np.nextafter(guess, np.float32(-np.inf)), guess,
                  np.nextafter(guess, np.float32(np.inf)))
    return min(candidates, key=lambda c: (abs(Fraction(float(c)) - exact),
                                          int(np.array(c).view(np.uint32)) & 1))


def _plain(bits, text):
    """The plain rule of profile_bootstrap._PLAIN_LUFS, computed in Python
    from the REAL's bits and its text as psycopg2 reads it."""
    value = np.frombuffer(bytes(bits), dtype=">f4")[0]
    if not np.isfinite(value):
        return False
    if value == 0:
        return True
    if not 1e-5 <= abs(float(value)) < 2 ** 23 or abs(Fraction(text)) < Fraction(1, 10_000):
        return False
    return np.float32(float(text)) == _round32(text)


def test_snapshot_bytes_equal_json_dumps_per_row_and_at_the_caps(db, monkeypatch):
    _seed_profiles(db)
    owned = _owned_like()
    python = _python_snapshot_bytes(owned)
    # Per row: which rows are plain, their measured length and the length of
    # their JSON text, plus json.dumps' ASCII escapes. The others are
    # serialized in Python.
    escapes = profile_bootstrap._ascii_escape_extra("b.track_id")
    measured = _query(owned, f"""
        SELECT b.track_id, b.plain, b.bits, b.lufs_text,
               {profile_bootstrap._SNAPSHOT_DOC_LENGTH} + {escapes},
               length({profile_bootstrap._SNAPSHOT_DOC}) + {escapes}
          FROM (SELECT {profile_bootstrap._SNAPSHOT_VALUES}, float4send(p.ref_lufs) AS bits,
                       CASE WHEN p.media_signature <> '' THEN 'sha256:' || encode(sha256(
                           convert_to(p.media_signature, 'UTF8')), 'hex') END AS revision
                  FROM {PUBLISHED} p WHERE p.catalog_instance_id=%s
                 ORDER BY p.track_id LIMIT 1000000) b""", (SOURCE,))
    owned.close()
    assert [row[0] for row in measured] == [track_id for track_id, _size in python]
    assert [row[1] for row in measured] == [_plain(row[2], row[3]) for row in measured]
    assert [row[4:] for row in measured] == [
        (size, size) if row[1] else (None, None) for row, (_t, size) in zip(measured, python)]
    plain = sum(row[1] for row in measured)
    assert 0 < len(measured) - plain < plain
    total = sum(size for _track_id, size in python)
    # The byte cap: 413 one byte short of the total, a capture at the total,
    # the same for the oracle.
    monkeypatch.setattr(profile_bootstrap, "SNAPSHOT_BATCH_ROWS", 100)
    for limit in (total - 1, total):
        for oracle in (True, False):
            _reset_rate_limit(db)
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(profile_bootstrap, "MAX_SNAPSHOT_BYTES", limit)
                if oracle:
                    patch.setitem(ORACLE.namespace, "MAX_SNAPSHOT_BYTES", limit)
                    patch.setattr(profile_bootstrap, "_capture_snapshot",
                                  ORACLE._capture_snapshot)
                if limit == total:
                    created = profile_bootstrap.create_session(body())
                    assert created["snapshot_count"] == len(python)
                    profile_bootstrap.release_session(
                        body(session_token=created["session_token"]))
                else:
                    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
                        profile_bootstrap.create_session(body())
                    assert (exc.value.code, exc.value.status) == ("bootstrap_snapshot_limit", 413)
    assert _query(db, f"SELECT count(*) FROM {SNAPSHOT}") == [(0,)]
    assert _query(db, f"SELECT count(*) FROM {SESSIONS}") == [(0,)]
    # The row cap, across batch boundaries.
    for limit in (len(python) - 1, len(python)):
        _reset_rate_limit(db)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(profile_bootstrap, "MAX_SNAPSHOT_ROWS", limit)
            if limit < len(python):
                with pytest.raises(profile_bootstrap.BootstrapError) as exc:
                    profile_bootstrap.create_session(body())
                assert exc.value.status == 413
            else:
                assert profile_bootstrap.create_session(body())["snapshot_count"] == limit


def _counting_fallback(monkeypatch):
    """Record the rows _snapshot_fallback serializes in Python."""
    rows = []
    original = profile_bootstrap._snapshot_fallback

    def counting(cur, source, session_id, ordinal, positions, track_ids):
        rows.extend(track_ids)
        return original(cur, source, session_id, ordinal, positions, track_ids)

    monkeypatch.setattr(profile_bootstrap, "_snapshot_fallback", counting)
    return rows


def _levels(db, levels):
    """One profile per REAL text in ``levels``, track ids in the same order."""
    with db.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate, duration_ms, "
            "ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, media_signature, "
            "analyzed_at) VALUES (%s, %s, 44100, 1000, %s::real, '\\x01', '\\x02', 1, 1, %s, "
            "'2026-09-01 12:00:00')",
            [(SOURCE, f"level-{index:05d}", level, f"sig-{index}")
             for index, level in enumerate(levels)])
    db.commit()


def _stored_levels(db):
    return [row[0] for row in _query(db, f"SELECT payload->'ref_lufs' FROM {SNAPSHOT} "
                                         "ORDER BY ordinal")]


@pytest.mark.parametrize("options", ["", "-c extra_float_digits=0",
                                     "-c extra_float_digits=3", "-c extra_float_digits=-5"])
def test_analyzed_loudness_takes_no_python_fallback(migrated_db, monkeypatch, options):
    """Analyzed loudness is plain: the whole snapshot is built in SQL,
    whatever the session's extra_float_digits."""
    db = _use_source(migrated_db, monkeypatch, options)
    levels = _loudness_texts(4000, 7) + [
        "-14.2", "-14", "-23", "-70", "0", "-0", "-0.5", "-8.123456", "5", "-0.0001"]
    _levels(db, levels)
    fallback = _counting_fallback(monkeypatch)
    monkeypatch.setattr(profile_bootstrap, "SNAPSHOT_BATCH_ROWS", 1000)
    created = profile_bootstrap.create_session(body())
    assert created["snapshot_count"] == len(levels)
    assert fallback == []
    owned = _owned_like()
    expected = _python_snapshot_payloads(owned)
    owned.close()
    assert _query(db, f"SELECT payload::text FROM {SNAPSHOT} ORDER BY ordinal") == expected


def _python_snapshot_payloads(db):
    """The JSONB text of json.dumps(serialize_profile(row)) per profile."""
    rows = _query(db, f"SELECT {PROFILE_COLUMNS} FROM {PUBLISHED} p "
                      "WHERE p.catalog_instance_id=%s ORDER BY p.track_id", (SOURCE,))
    texts = [json.dumps(catalog_enrichment.serialize_profile(*row), separators=(",", ":"))
             for row in rows]
    return _query(db, "SELECT u.v::jsonb::text FROM unnest(%s::text[]) WITH ORDINALITY "
                      "u(v, n) ORDER BY n", (texts,))


@pytest.mark.parametrize("options, levels, expected", [
    # LOW-2: float4 0x15ae43fd reads as 7.038531e-26, whose double rounds to
    # the neighbouring float4, so serialize_profile writes 7.0385313e-26.
    ("", ["7.038531e-26", "-7.038531e-26", "-14.2"], [7.0385313e-26, -7.0385313e-26, -14.2]),
    # LOW-1: with extra_float_digits=-2 FLT_MAX reads as 3.403e+38, beyond a
    # float4, so serialize_profile writes null (the create failed with 503).
    ("-c extra_float_digits=-2", ["3.4028235e38", "-3.4028235e38", "-14.2"], [None, None, -14.2]),
], ids=["low-2", "low-1"])
def test_review_levels_are_written_by_serialize_profile(migrated_db, monkeypatch, options,
                                                         levels, expected):
    """Levels that are not plain are serialized by serialize_profile itself."""
    assert np.float32(float("7.038531e-26")) != np.frombuffer(
        bytes.fromhex("15ae43fd"), dtype=">f4")[0]
    db = _use_source(migrated_db, monkeypatch, options)
    _levels(db, levels)
    fallback = _counting_fallback(monkeypatch)
    with np.errstate(over="ignore"):
        created = profile_bootstrap.create_session(body())
    assert created["snapshot_count"] == 3
    assert fallback == ["level-00000", "level-00001"]
    assert _stored_levels(db) == expected


def test_changes_between_snapshot_batches_are_not_captured(plain_db, second_connection,
                                                           monkeypatch):
    """Every batch reads the capture's one REPEATABLE READ snapshot (review
    LOW-7): profiles published, updated, deleted or inserted between two
    batches are invisible to the rest of the capture, the fallback's rows
    included, and snapshot_seq stays the head the capture started from."""
    db = plain_db
    _simple_profiles(db, 7)
    # A non-plain row in a later batch: the Python fallback reads it too.
    _query(db, f"UPDATE {PUBLISHED} SET ref_lufs='NaN' WHERE track_id='track-6'")
    with db.cursor() as cur:
        catalog_enrichment.record_profile_change(cur, SOURCE, "track-1", "ready",
                                                 {"track_id": "track-1"})
    db.commit()
    head = _query(db, f"SELECT head_seq FROM {STATE}")[0][0]
    before = _query(db, f"SELECT {PROFILE_COLUMNS} FROM {PUBLISHED} p ORDER BY p.track_id")
    expected = _query(db, "SELECT u.v::jsonb::text FROM unnest(%s::text[]) WITH ORDINALITY "
                          "u(v, n) ORDER BY n", ([json.dumps(
                              catalog_enrichment.serialize_profile(*row), separators=(",", ":"))
                              for row in before],))
    monkeypatch.setattr(profile_bootstrap, "SNAPSHOT_BATCH_ROWS", 2)
    calls = []
    original = profile_bootstrap._snapshot_batch

    def change_after_second_batch(*args):
        result = original(*args)
        calls.append(result[0])
        if len(calls) == 2:
            with second_connection.cursor() as cur:
                # Batches [1, 2] and [3, 4] are captured, [5, 6] and [7] not.
                cur.execute(f"UPDATE {PUBLISHED} SET ref_lufs=-1, duration_ms=1 "
                            "WHERE track_id IN ('track-3', 'track-5', 'track-6')")
                cur.execute(f"DELETE FROM {PUBLISHED} WHERE track_id IN ('track-4', 'track-7')")
                cur.execute(
                    f"INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate, "
                    "duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, "
                    "profile_schema_ver, media_signature, analyzed_at) "
                    f"SELECT catalog_instance_id, track_id || 'a', sample_rate, duration_ms, "
                    "ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, "
                    f"media_signature, analyzed_at FROM {PUBLISHED} "
                    "WHERE track_id IN ('track-4', 'track-6')")
                for track_id in ("track-3", "track-4", "track-4a", "track-6a"):
                    catalog_enrichment.record_profile_change(
                        cur, SOURCE, track_id, "ready", {"track_id": track_id})
            second_connection.commit()
        return result

    monkeypatch.setattr(profile_bootstrap, "_snapshot_batch", change_after_second_batch)
    fallback = _counting_fallback(monkeypatch)
    created = profile_bootstrap.create_session(body())
    assert calls == [2, 2, 2, 1]
    assert fallback == ["track-6"]
    assert created["snapshot_seq"] == head
    assert _query(db, f"SELECT snapshot_seq, snapshot_count FROM {SESSIONS}") == [(head, 7)]
    assert _query(db, f"SELECT payload::text FROM {SNAPSHOT} ORDER BY ordinal") == expected
    assert [row[0]["ref_lufs"] for row in _query(
        db, f"SELECT payload FROM {SNAPSHOT} ORDER BY ordinal")] == [-14.5] * 5 + [None, -14.5]
    assert _query(db, f"SELECT head_seq FROM {STATE}")[0][0] == head + 4


def test_catchup_bytes_equal_json_dumps_per_event_and_at_the_caps(db, monkeypatch):
    profiles = _seed_profiles(db)
    created = profile_bootstrap.create_session(body())
    events = _journal(db, profiles)
    token = created["session_token"]
    owned = _owned_like()
    python = _python_catchup_bytes(owned, created["snapshot_seq"])
    owned.close()
    assert len(python) == events
    # Per event: one event per batch, each batch reporting its bytes. The
    # capture then fails at the event cap, so nothing is kept.
    seen = []
    original = profile_bootstrap._catchup_batch

    def recording(*args):
        result = original(*args)
        seen.append(result[3])
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(profile_bootstrap, "_catchup_batch", recording)
        patch.setattr(profile_bootstrap, "CATCHUP_BATCH_EVENTS", 1)
        patch.setattr(profile_bootstrap, "catchup_limits", lambda _limit: (events - 1, 1 << 60))
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            profile_bootstrap.catchup_page(body(session_token=token))
        assert exc.value.status == 413
    assert seen == python
    # The byte cap: 413 one byte short of the total, a capture at the total,
    # the same for the oracle (MAX_CATCHUP_EVENT_BYTES=0 makes it bind).
    total = sum(python)
    monkeypatch.setattr(profile_bootstrap, "CATCHUP_BATCH_EVENTS", 4)
    monkeypatch.setattr(profile_bootstrap, "MAX_CATCHUP_EVENT_BYTES", 0)
    for limit in (total - 1, total):
        for operation in (ORACLE.catchup_page, profile_bootstrap.catchup_page):
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(profile_bootstrap, "MAX_CATCHUP_BYTES", limit)
                if limit == total:
                    page = operation(body(session_token=token))
                    assert page["changes"][0]["seq"] == created["snapshot_seq"] + 1
                    _query(db, f"UPDATE {SESSIONS} SET head_seq=NULL")
                    _query(db, f"DELETE FROM {CATCHUP}")
                else:
                    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
                        operation(body(session_token=token))
                    assert (exc.value.code, exc.value.status) == ("bootstrap_snapshot_limit", 413)
                    assert _query(db, f"SELECT count(*) FROM {CATCHUP}") == [(0,)]
    # The event cap: 413 at one event short, a capture at the event count.
    for limit in (events - 1, events):
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(profile_bootstrap, "catchup_limits", lambda _l, n=limit: (n, 1 << 60))
            if limit < events:
                with pytest.raises(profile_bootstrap.BootstrapError) as exc:
                    profile_bootstrap.catchup_page(body(session_token=token))
                assert exc.value.status == 413
            else:
                profile_bootstrap.catchup_page(body(session_token=token))
    assert _query(db, f"SELECT count(*) FROM {CATCHUP}") == [(events,)]


def test_catchup_answers_410_for_a_missing_event_before_the_cap(plain_db, monkeypatch):
    """As before P2-4: an interval with a missing event is 410, even when a
    later event would exceed the event cap; at the cap before the gap, 413."""
    db = plain_db
    _query(db, f"INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate, "
               "duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, "
               "media_signature, analyzed_at) VALUES (%s, 't', 1, 1, -1, '', '', 1, 1, 's', "
               "'2026-09-01')", (SOURCE,))
    created = profile_bootstrap.create_session(body())
    with db.cursor() as cur:
        for index in range(9):
            catalog_enrichment.record_profile_change(cur, SOURCE, f"t{index}", "deleted")
    db.commit()
    first = created["snapshot_seq"] + 1
    _query(db, f"DELETE FROM {CHANGES} WHERE seq=%s", (first + 5,))
    token = created["session_token"]
    # Events first..first+4, then first+6: the gap is at index 5, which the
    # Python capture checked before the cap of the same event.
    for batch in (1, 3, 9):
        monkeypatch.setattr(profile_bootstrap, "CATCHUP_BATCH_EVENTS", batch)
        for cap, status in ((8, 410), (6, 410), (5, 410), (4, 413), (1, 413)):
            for operation in (ORACLE.catchup_page, profile_bootstrap.catchup_page):
                with pytest.MonkeyPatch.context() as patch:
                    limits = lambda _l, n=cap: (n, 1 << 60)  # noqa: E731
                    patch.setattr(profile_bootstrap, "catchup_limits", limits)
                    patch.setitem(ORACLE.namespace, "catchup_limits", limits)
                    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
                        operation(body(session_token=token))
                assert exc.value.status == status, (batch, cap, operation)
    # A missing last event (the batch ends early) is 410 too.
    _query(db, f"INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, operation, "
               "writer_generation) SELECT catalog_instance_id, epoch, %s, 'x', 'delete', 2 "
               f"FROM {CHANGES} WHERE seq=%s", (first + 5, first))
    _query(db, f"DELETE FROM {CHANGES} WHERE seq=%s", (first + 8,))
    for operation in (ORACLE.catchup_page, profile_bootstrap.catchup_page):
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            operation(body(session_token=token))
        assert exc.value.status == 410


# ---------------------------------------------------------------------------
# The page queries pick their rows before the edge lookup.
# ---------------------------------------------------------------------------

PAGE_PLAN_PROFILES = 2_000


def _explain_loops(db, statement):
    """The largest Actual Loops of any plan node, and the edge lookups'."""
    query, params = statement
    with db.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + query, params)
        plan = cur.fetchone()[0][0]["Plan"]
    db.rollback()
    loops, edge_loops = [], []

    def walk(node):
        loops.append(node["Actual Loops"])
        if node.get("Relation Name") == EDGES:
            edge_loops.append(node["Actual Loops"])
        for child in node.get("Plans", ()):
            walk(child)

    walk(plan)
    return max(loops), edge_loops


def test_page_queries_look_up_edges_for_the_page_only(plain_db, monkeypatch):
    """A fresh snapshot table has no statistics, and a planner may then join
    every remaining row of the session to its edge before sorting and
    limiting (P2-6 finding 1). The page's rows are now chosen first: however
    the planner scans them, the edge lookup runs at most page_size times."""
    db = plain_db
    with db.cursor() as cur:
        for name in (SNAPSHOT, CATCHUP):
            cur.execute(f"ALTER TABLE {name} SET (autovacuum_enabled = false)")
        cur.execute(
            f"""INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate,
                    duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver,
                    profile_schema_ver, media_signature, analyzed_at)
                SELECT %s, 'track-' || lpad(g::text, 6, '0'), 44100, 240000, -14.5,
                       '\\x0102', '\\x0304', 1, 1, 'sig-' || g, '2026-09-01 12:00:00'
                  FROM generate_series(1, %s) g""", (SOURCE, PAGE_PLAN_PROFILES))
        cur.execute(
            f"""INSERT INTO {EDGES} (catalog_instance_id, track_id, media_revision,
                    representation_id, media_signature, profile_digest, payload)
                SELECT %s, t, r, 'rep', 'sig-' || g, md5(g::text),
                       jsonb_build_object('track_id', t, 'media_revision', r,
                                          'profile_digest', md5(g::text),
                                          'bins', (SELECT jsonb_agg(n) FROM generate_series(1, 300) n))
                  FROM generate_series(1, %s) g,
                       LATERAL (SELECT 'track-' || lpad(g::text, 6, '0') AS t,
                                       'sha256:' || encode(sha256(convert_to('sig-' || g, 'UTF8')),
                                                           'hex') AS r) k""",
            (SOURCE, PAGE_PLAN_PROFILES))
    db.commit()
    statements = []
    actual_connect = psycopg2.connect

    class Recording(psycopg2.extensions.cursor):
        def execute(self, query, vars=None):
            statements.append((query, vars))
            return super().execute(query, vars)

    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                        lambda dsn, **options: actual_connect(
                            dsn, cursor_factory=Recording, **options))
    created = profile_bootstrap.create_session(body(page_size=50))
    # As many journal events, each carrying its track's stored edge: the
    # catch-up stores a reference per event.
    _query(db, f"""INSERT INTO {CHANGES} (catalog_instance_id, epoch, seq, track_id, operation,
                        payload, writer_generation)
                   SELECT s.catalog_instance_id, s.epoch, s.head_seq + row_number() OVER (
                              ORDER BY e.track_id), e.track_id, 'upsert',
                          jsonb_build_object('track_id', e.track_id,
                                             'media_revision', e.media_revision,
                                             'edge_profile', e.payload), 2
                     FROM {STATE} s JOIN {EDGES} e USING (catalog_instance_id)""")
    _query(db, f"UPDATE {STATE} SET head_seq = head_seq + %s", (PAGE_PLAN_PROFILES,))
    assert _query(db, "SELECT count(*) FROM pg_stats WHERE tablename = ANY(%s) "
                      "AND schemaname = current_schema()",
                  ([SNAPSHOT, CATCHUP],)) == [(0,)]
    del statements[:]
    token = created["session_token"]
    page = profile_bootstrap.snapshot_page(body(session_token=token))
    assert all("edge_profile" in profile for profile in page["profiles"])
    catchup = profile_bootstrap.catchup_page(body(session_token=token))
    assert all("edge_profile" in event["payload"] for event in catchup["changes"])
    assert len(page["profiles"]) == len(catchup["changes"]) == 50
    queries = [s for s in statements if "LEFT JOIN LATERAL" in s[0]]
    assert [SNAPSHOT in q for q, _params in queries] == [True, False]
    assert _query(db, f"SELECT count(*) FROM {CATCHUP}") == [(PAGE_PLAN_PROFILES,)]
    for statement in queries:
        largest, edge_loops = _explain_loops(db, statement)
        assert edge_loops and max(edge_loops) >= 1
        assert largest <= 50, statement[0]


class _ExplainedInserts(psycopg2.extensions.cursor):
    """Runs a capture's snapshot INSERTs under EXPLAIN ANALYZE, in the
    capture's own transaction (so with its settings), and keeps the plans."""

    plans = []
    _inserted = None

    def execute(self, query, vars=None):
        self._inserted = None
        if query.lstrip().startswith(f"INSERT INTO {SNAPSHOT}"):
            super().execute("EXPLAIN (ANALYZE, FORMAT JSON) " + query, vars)
            plan = self.fetchone()[0][0]["Plan"]
            self.plans.append(plan)
            self._inserted = plan["Plans"][0]["Actual Rows"]
            return None
        return super().execute(query, vars)

    @property
    def rowcount(self):
        return self._inserted if self._inserted is not None else super().rowcount


def test_capture_joins_edges_once_per_batch_for_a_source_without_statistics(
        plain_db, monkeypatch):
    """A source whose rows came after the last ANALYZE (a new or refilled
    source) is estimated at about one row. The batch's edge join must still be
    hashed or merged: a nested loop would run the edge side once per profile
    (the P2-6 gate's second source took a minute to capture at 94k)."""
    db = plain_db
    other = "catalog-b"
    count = 2_000
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate,
                    duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver,
                    profile_schema_ver, media_signature, analyzed_at)
                SELECT %s, 'track-' || lpad(g::text, 6, '0'), 44100, 240000, -14.5,
                       '\\x0102', '\\x0304', 1, 1, 'sig-' || g, '2026-09-01 12:00:00'
                  FROM generate_series(1, %s) g""", (SOURCE, count))
        cur.execute(
            f"""INSERT INTO {EDGES} (catalog_instance_id, track_id, media_revision,
                    representation_id, media_signature, profile_digest, payload)
                SELECT catalog_instance_id, track_id,
                       'sha256:' || encode(sha256(convert_to(media_signature, 'UTF8')), 'hex'),
                       'rep', media_signature, md5(track_id), '{{}}'::jsonb
                  FROM {PUBLISHED}""")
        cur.execute(f"ANALYZE {PUBLISHED}")
        cur.execute(f"ANALYZE {EDGES}")
        cur.execute(
            f"INSERT INTO {P}catalog_sources (catalog_instance_id, current_core_server_id, "
            "provider_type, server_name, is_default, rebind_status) "
            "VALUES (%s, 'server-b', 'navidrome', 'B', FALSE, 'active')", (other,))
        cur.execute(
            f"INSERT INTO {P}catalog_state (catalog_instance_id, current_core_server_id, "
            "provider_type, published_generation, catalog_epoch, status) "
            "VALUES (%s, 'server-b', 'navidrome', 1, 'epoch-b', 'complete')", (other,))
        catalog_enrichment._profile_stream_state(cur, other, for_update=True)
        for name in (PUBLISHED, EDGES):
            cur.execute(f"SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute "
                        "WHERE attrelid=%s::regclass AND attnum > 0 AND NOT attisdropped",
                        (name,))
            columns = cur.fetchone()[0]
            selected = ", ".join("%s" if c == "catalog_instance_id" else c for c in columns)
            cur.execute(f"INSERT INTO {name} ({', '.join(columns)}) SELECT {selected} "
                        f"FROM {name} WHERE catalog_instance_id=%s", (other, SOURCE))
    db.commit()
    actual_connect = psycopg2.connect
    monkeypatch.setattr(_ExplainedInserts, "plans", [])
    monkeypatch.setattr(profile_bootstrap.psycopg2, "connect",
                        lambda dsn, **options: actual_connect(
                            dsn, cursor_factory=_ExplainedInserts, **options))
    monkeypatch.setattr(profile_bootstrap, "SNAPSHOT_BATCH_ROWS", 1_000)
    created = profile_bootstrap.create_session(body(catalog_instance_id=other))
    assert created["snapshot_count"] == count
    assert len(_ExplainedInserts.plans) == 2

    def nodes(node):
        yield node
        for child in node.get("Plans", ()):
            yield from nodes(child)

    for plan in _ExplainedInserts.plans:
        edge_scans = [n for n in nodes(plan) if n.get("Relation Name") == EDGES]
        assert edge_scans and all(n["Actual Loops"] == 1 for n in edge_scans), plan
    assert _query(db, f"SELECT count(edge_ref) FROM {SNAPSHOT}") == [(count,)]


# ---------------------------------------------------------------------------
# A backend killed during either capture leaves nothing half visible.
# ---------------------------------------------------------------------------


def _simple_profiles(db, count):
    _query(db, f"""INSERT INTO {PUBLISHED} (catalog_instance_id, track_id, sample_rate,
                       duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver,
                       profile_schema_ver, media_signature, analyzed_at)
                   SELECT %s, 'track-' || g, 44100, 1000 + g, -14.5, '\\x01', '\\x02', 1, 1,
                          'sig-' || g, '2026-09-01 12:00:00'
                     FROM generate_series(1, %s) g""", (SOURCE, count))


def _killing(monkeypatch, name, killer):
    """Wrap ``name`` so the owned backend is terminated after its first call."""
    original = getattr(profile_bootstrap, name)
    killed = []

    def kill_after_first(cur, *args):
        result = original(cur, *args)
        if not killed:
            with killer.cursor() as other:
                other.execute("SELECT pg_terminate_backend(%s)",
                              (cur.connection.get_backend_pid(),))
                killed.append(other.fetchone()[0])
            killer.commit()
        return result

    monkeypatch.setattr(profile_bootstrap, name, kill_after_first)
    return killed


def _live_sessions(db):
    return _query(db, f"SELECT count(*) FROM {SESSIONS} s "
                      f"WHERE {catalog_enrichment.live_bootstrap_session_sql('s')}")[0][0]


def test_backend_killed_mid_snapshot_capture(plain_db, second_connection, monkeypatch):
    """The capture's rows go with its backend. The admitted row outlives it
    (its delete ran on the dead connection), holds a slot as 'capturing' for
    at most PROFILE_BOOTSTRAP_CAPTURE_MINUTES, and a K5 retry with the same
    client_request_id replaces it without leaking the slot."""
    db = plain_db
    _simple_profiles(db, 7)
    monkeypatch.setattr(profile_bootstrap, "SNAPSHOT_BATCH_ROWS", 2)
    with pytest.MonkeyPatch.context() as patch:
        killed = _killing(patch, "_snapshot_batch", second_connection)
        request_id = str(uuid.uuid4())
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            profile_bootstrap.create_session(body(client_request_id=request_id))
    assert (killed, exc.value.status) == ([True], 503)
    assert _query(db, f"SELECT count(*) FROM {SNAPSHOT}") == [(0,)]
    assert _query(db, f"SELECT state, snapshot_count, client_request_id FROM {SESSIONS}") == [
        ("capturing", 0, request_id)]
    assert _live_sessions(db) == 1

    retried = profile_bootstrap.create_session(body(client_request_id=request_id))
    assert retried["snapshot_count"] == 7
    assert _query(db, f"SELECT state, snapshot_count, client_request_id FROM {SESSIONS}") == [
        ("ready", 7, request_id)]
    assert _live_sessions(db) == 1
    assert _query(db, f"SELECT count(*) FROM {SNAPSHOT}") == [(7,)]

    # Without a retry, the abandoned capture stops counting after
    # PROFILE_BOOTSTRAP_CAPTURE_MINUTES, and a later create purges it.
    with pytest.MonkeyPatch.context() as patch:
        killed = _killing(patch, "_snapshot_batch", second_connection)
        with pytest.raises(profile_bootstrap.BootstrapError):
            profile_bootstrap.create_session(body())
    assert killed == [True] and _live_sessions(db) == 2
    minutes = catalog_enrichment.PROFILE_BOOTSTRAP_CAPTURE_MINUTES + 1
    _query(db, f"UPDATE {SESSIONS} SET created_at = now() - make_interval(mins => %s) "
               "WHERE state = 'capturing'", (minutes,))
    assert _live_sessions(db) == 1
    profile_bootstrap.create_session(body())
    assert _query(db, f"SELECT state, count(*) FROM {SESSIONS} GROUP BY state") == [("ready", 2)]
    assert _query(db, f"SELECT count(*) FROM {SNAPSHOT}") == [(14,)]


def test_backend_killed_mid_first_catchup_capture(plain_db, second_connection, monkeypatch):
    """The catch-up's rows, its head_seq and the served count go with the
    backend; the session stays ready and the next request captures it all."""
    db = plain_db
    _simple_profiles(db, 3)
    created = profile_bootstrap.create_session(body(page_size=2))
    with db.cursor() as cur:
        for index in range(7):
            catalog_enrichment.record_profile_change(cur, SOURCE, f"track-{index}", "deleted")
    db.commit()
    monkeypatch.setattr(profile_bootstrap, "CATCHUP_BATCH_EVENTS", 2)
    token = created["session_token"]
    with pytest.MonkeyPatch.context() as patch:
        killed = _killing(patch, "_catchup_batch", second_connection)
        with pytest.raises(profile_bootstrap.BootstrapError) as exc:
            profile_bootstrap.catchup_page(body(session_token=token))
    assert (killed, exc.value.status) == ([True], 503)
    assert _query(db, f"SELECT count(*) FROM {CATCHUP}") == [(0,)]
    assert _query(db, f"SELECT state, head_seq, pages_served FROM {SESSIONS}") == [
        ("ready", None, 0)]
    first = profile_bootstrap.catchup_page(body(session_token=token))
    assert [event["seq"] for event in first["changes"]] == [
        created["snapshot_seq"] + 1, created["snapshot_seq"] + 2]
    assert _query(db, f"SELECT count(*), min(seq), max(seq) FROM {CATCHUP}") == [
        (7, created["snapshot_seq"] + 1, created["snapshot_seq"] + 7)]
    assert _query(db, f"SELECT state, head_seq FROM {SESSIONS}") == [
        ("ready", created["snapshot_seq"] + 7)]
