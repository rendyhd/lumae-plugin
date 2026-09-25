"""Durable, bounded v2 profile bootstrap captures.

Every operation owns its PostgreSQL connection. In particular, snapshot capture
never changes the transaction state of the host's request-scoped connection.
"""

import base64
import hashlib
import hmac
import io
import json
import re
import secrets
import uuid
from contextlib import contextmanager

import psycopg2

from plugin.api import config, table

from .catalog import MAX_HELD_RETENTION_MULTIPLIER, opaque_cursor
from .catalog_enrichment import PROFILE_CHANGE_RETENTION_EVENTS, serialize_profile
from .edge_profile_store import edge_join


SESSION_MINUTES = 60
MAX_PAGE = 500
MAX_SESSIONS_SOURCE = 4
MAX_SESSIONS_GLOBAL = 32
MAX_SNAPSHOT_ROWS = 200_000
# Snapshot and catch-up rows store the waveform part plus an edge reference
# (K2); edges are resolved from edge_profiles at page read, so the byte caps
# count compact waveform JSON only.
MAX_SNAPSHOT_BYTES = 128 * 1024 * 1024
# The first catch-up must fit everything the P1-2 floor hold can keep readable
# for an open session: head - floor <= MAX_HELD_RETENTION_MULTIPLIER x the
# source's retention limit, i.e. CATCHUP_RETENTION_MULTIPLIER x
# max(PROFILE_CHANGE_RETENTION_EVENTS, profile_stream_state.retention_limit)
# events (see catchup_limits). The byte cap is MAX_CATCHUP_BYTES, raised to
# MAX_CATCHUP_EVENT_BYTES per admitted event, so a held interval of waveform
# events (about 0.5 KiB each) is never refused for its size either.
CATCHUP_RETENTION_MULTIPLIER = MAX_HELD_RETENTION_MULTIPLIER
MAX_CATCHUP_BYTES = 128 * 1024 * 1024
MAX_CATCHUP_EVENT_BYTES = 1024
TRANSFER_CONTRACT = "source_scoped_v1"
STATEMENT_TIMEOUT_MS = 20_000
LOCK_TIMEOUT_MS = 5_000


class BootstrapError(Exception):
    def __init__(self, code, status):
        self.code = code
        self.status = status
        super().__init__(code)


def invalid():
    raise BootstrapError("invalid_profile_bootstrap", 400)


def gone():
    raise BootstrapError("bootstrap_required", 410)


def _table(name):
    return table(name)


def _encoded(payload):
    return base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).rstrip(b"=").decode("ascii")


def _decoded(value):
    if not isinstance(value, str) or len(value) > 2048 or not value:
        invalid()
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        payload = json.loads(raw)
    except (ValueError, UnicodeError, TypeError):
        invalid()
    if not isinstance(payload, dict):
        invalid()
    return payload


def _page_token(session, phase, ordinal):
    data = _encoded({"s": str(session[0]), "p": phase, "o": ordinal,
                     "z": session[8]})
    signature = hmac.new(bytes.fromhex(session[2]), data.encode(), hashlib.sha256).hexdigest()
    return f"{data}.{signature}"


def _ordinal(session, phase, token):
    if token is None:
        return 0
    if not isinstance(token, str) or len(token) > 2300 or token.count(".") != 1:
        invalid()
    data, signature = token.split(".")
    if (not re.fullmatch(r"[A-Za-z0-9_-]+", data, flags=re.ASCII)
            or not re.fullmatch(r"[0-9a-f]{64}", signature, flags=re.ASCII)):
        invalid()
    expected = hmac.new(bytes.fromhex(session[2]), data.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        invalid()
    payload = _decoded(data)
    if (payload.get("s") != str(session[0]) or payload.get("p") != phase
            or payload.get("z") != session[8]
            or type(payload.get("o")) is not int or payload["o"] < 0):
        invalid()
    return payload["o"]


def _require_request(body, *, creating=False):
    if (not isinstance(body, dict) or type(body.get("protocol_version")) is not int
            or body["protocol_version"] != 2
            or body.get("transfer_contract") != TRANSFER_CONTRACT
            or type(body.get("schema_version")) is not int
            or body["schema_version"] != 1
            or not isinstance(body.get("catalog_instance_id"), str)
            or not body["catalog_instance_id"]
            or len(body["catalog_instance_id"]) > 512):
        invalid()
    if creating:
        size = body.get("page_size", 250)
        if type(size) is not int or size < 1 or size > MAX_PAGE:
            invalid()
        return size
    if "page_size" in body:
        invalid()
    if not isinstance(body.get("session_token"), str) or not re.fullmatch(
            r"[0-9a-f]{64}", body["session_token"]):
        invalid()


@contextmanager
def _connection(*, repeatable=False, creator=False):
    """Own one backend for this operation, separate from the request connection."""
    db = None
    try:
        db = psycopg2.connect(config.DATABASE_URL, connect_timeout=5)
        db.set_session(isolation_level="REPEATABLE READ" if repeatable else "READ COMMITTED")
        with db.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, false)",
                        (str(STATEMENT_TIMEOUT_MS),))
            cur.execute("SELECT set_config('lock_timeout', %s, false)",
                        (str(LOCK_TIMEOUT_MS),))
        db.commit()
        try:
            if creator:
                with db.cursor() as cur:
                    cur.execute("SELECT pg_advisory_lock(110094, 10)")
                # End the acquisition transaction before the MVCC snapshot;
                # the session advisory lock stays on this backend until close.
                db.commit()
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
            db = None
    except BootstrapError:
        raise
    except (psycopg2.Error, AttributeError, TypeError, OSError, ValueError):
        raise BootstrapError("bootstrap_unavailable", 503) from None
    finally:
        if db is not None:
            try:
                db.rollback()
            except psycopg2.Error:
                pass
            db.close()


def catchup_limits(retention_limit):
    """(events, bytes) the first catch-up may capture for this source.

    ``retention_limit`` is ``profile_stream_state.retention_limit``. The event
    limit equals the floor-hold cap of ``compact_change_journal`` as
    ``record_profile_change`` applies it, so a session the hold kept readable
    never gets 413 at catch-up.
    """
    retained = max(PROFILE_CHANGE_RETENTION_EVENTS, int(retention_limit or 0))
    events = CATCHUP_RETENTION_MULTIPLIER * retained
    return events, max(MAX_CATCHUP_BYTES, events * MAX_CATCHUP_EVENT_BYTES)


def _state(cur, source):
    cur.execute(
        f"""SELECT s.current_core_server_id, s.rebind_status, c.catalog_epoch,
                   p.epoch, p.head_seq, p.floor_seq, p.retention_limit
              FROM {_table('catalog_sources')} s
              JOIN {_table('catalog_state')} c USING (catalog_instance_id)
              JOIN {_table('profile_stream_state')} p USING (catalog_instance_id)
             WHERE s.catalog_instance_id=%s""", (source,))
    row = cur.fetchone()
    if row is None or row[1] != "active" or not row[0]:
        gone()
    return row


def _session(cur, body, *, lock=False):
    token_hash = hashlib.sha256(body["session_token"].encode()).hexdigest()
    cur.execute(
        f"""SELECT session_id, token_hash, signing_secret, source_scope,
                   catalog_instance_id, core_server_id, catalog_epoch,
                   profile_epoch, page_size, snapshot_seq, head_seq,
                   snapshot_count, expires_at, schema_version
              FROM {_table('profile_bootstrap_sessions')}
             WHERE token_hash=%s""" + (" FOR UPDATE" if lock else ""),
        (token_hash,))
    row = cur.fetchone()
    if (row is None or row[3] != body["catalog_instance_id"]
            or row[4] != body["catalog_instance_id"]):
        gone()
    cur.execute("SELECT now()")
    if row[12] <= cur.fetchone()[0] or row[13] != 1:
        gone()
    state = _state(cur, row[4])
    if (state[0] != row[5] or state[2] != row[6] or state[3] != row[7]):
        gone()
    return row, state


# edge_ref (K2) names the edge row (media_revision, profile_digest), with the
# media_revision implied by the row's own payload unless stored. A snapshot
# stores a reference only when the edge's media_revision equals the row's (the
# only case serialize_profile embeds), so snapshot references are always
# {"profile_digest": ...} (about 100 bytes less per row, which keeps a 94k
# capture under its WAL budget). Only a catch-up reference whose journalled
# edge names another revision than the event payload carries media_revision.
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


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _metadata(session):
    cursor = opaque_cursor(session[4], session[7], session[9])
    return {"catalog_epoch": session[6], "profile_epoch": session[7],
            "snapshot_cursor": cursor, "snapshot_seq": session[9],
            "total_profiles": session[11], "expires_at": _iso(session[12])}


def create_session(body):
    size = _require_request(body, creating=True)
    token = secrets.token_hex(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    session_id = str(uuid.uuid4())
    secret = secrets.token_hex(32)
    with _connection(repeatable=True, creator=True) as db:
        with db.cursor() as cur:
            # Session advisory lock was acquired before the RR snapshot.
            cur.execute(f"DELETE FROM {_table('profile_bootstrap_sessions')} WHERE expires_at<=now()")
            cur.execute(f"SELECT source_scope, count(*) FROM {_table('profile_bootstrap_sessions')} "
                        "GROUP BY source_scope")
            counts = dict(cur.fetchall())
            if (sum(counts.values()) >= MAX_SESSIONS_GLOBAL
                    or counts.get(body["catalog_instance_id"], 0) >= MAX_SESSIONS_SOURCE):
                raise BootstrapError("bootstrap_session_limit", 429)
            state = _state(cur, body["catalog_instance_id"])
            server, _, catalog_epoch, profile_epoch, snapshot_seq = state[:5]
            cur.execute(
                f"""INSERT INTO {_table('profile_bootstrap_sessions')}
                    (session_id, token_hash, signing_secret, source_scope,
                     catalog_instance_id, core_server_id, catalog_epoch,
                     profile_epoch, schema_version, page_size, snapshot_seq,
                     snapshot_count, expires_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,0,
                            now() + interval '60 minutes') RETURNING expires_at""",
                (session_id, token_hash, secret, body["catalog_instance_id"],
                 body["catalog_instance_id"], server, catalog_epoch,
                 profile_epoch, size, snapshot_seq))
            expires_at = cur.fetchone()[0]
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
                (body["catalog_instance_id"],))
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
            cur.execute(f"UPDATE {_table('profile_bootstrap_sessions')} "
                        "SET snapshot_count=%s WHERE session_id=%s", (ordinal, session_id))
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": TRANSFER_CONTRACT,
            "catalog_instance_id": body["catalog_instance_id"],
            "session_token": token, "page_size": size,
            "snapshot_count": ordinal, "total_profiles": ordinal,
            "catalog_epoch": catalog_epoch, "profile_epoch": profile_epoch,
            "snapshot_seq": snapshot_seq, "expires_at": _iso(expires_at),
            "snapshot_cursor": opaque_cursor(body["catalog_instance_id"], profile_epoch, snapshot_seq),
            "cursor": opaque_cursor(body["catalog_instance_id"], profile_epoch, snapshot_seq),
            "next_page_token": _page_token((session_id, None, secret, None, None, None,
                                             None, None, size), "snapshot", 0)}


def snapshot_page(body):
    _require_request(body)
    with _connection() as db:
        with db.cursor() as cur:
            session, _ = _session(cur, body, lock=True)
            ordinal = _ordinal(session, "snapshot", body.get("page_token"))
            if ordinal > session[11] or (ordinal != 0 and ordinal % session[8]):
                invalid()
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
                                 "created_at": created_at.isoformat().replace("+00:00", "Z")}
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


def release_session(body):
    _require_request(body)
    with _connection() as db:
        with db.cursor() as cur:
            token_hash = hashlib.sha256(body["session_token"].encode()).hexdigest()
            cur.execute(f"SELECT 1 FROM {_table('profile_bootstrap_sessions')} "
                        "WHERE token_hash=%s", (token_hash,))
            if cur.fetchone() is not None:
                _session(cur, body, lock=True)
                cur.execute(f"DELETE FROM {_table('profile_bootstrap_sessions')} WHERE token_hash=%s",
                            (token_hash,))
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": TRANSFER_CONTRACT, "released": True}
