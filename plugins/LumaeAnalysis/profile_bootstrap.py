"""Durable, bounded v2 profile bootstrap captures.

Every operation owns its PostgreSQL connection. In particular, snapshot capture
never changes the transaction state of the host's request-scoped connection.

A create runs in three transactions on that connection (P1-6, AUD-11):

1. **Admission**, under the global transaction lock
   ``pg_advisory_xact_lock(110094, 10)``: rate limit, K5 replacement of an
   unclaimed duplicate, slot count over *live* sessions only, and the insert of
   the session row in state ``capturing`` with the admission head as its
   ``snapshot_seq``. It commits at once, so the global lock is held for a few
   statements and the P1-2 floor hold covers the capture that follows.
2. **Purge** of a few sessions that are no longer live (expired,
   identity-stale, abandoned captures, replaced duplicates), without the
   global lock and with ``SKIP LOCKED``, so it never waits for a page or a
   capture. It is best effort: a failure never fails the create.
3. **Capture**, under the per-source session lock
   ``pg_advisory_lock(110094, hashtext(source))`` (waited for at most
   ``CAPTURE_LOCK_TIMEOUT_MS``): one REPEATABLE READ snapshot of the source,
   the session row updated to ``ready`` with the captured head.
   The lock is always unlocked explicitly. If the capture fails, the admitted
   row is deleted; if even that fails, it stops counting after
   ``PROFILE_BOOTSTRAP_CAPTURE_MINUTES``.

The snapshot rows, and the first catch-up's rows, are built by
``INSERT ... SELECT`` in the owned backend (P2-4), in batches that keep each
statement short. No row passes through Python, so a capture never holds the
web worker's GIL while it runs.
"""

import base64
import hashlib
import hmac
import json
import math
import re
import secrets
import time
import uuid
from contextlib import contextmanager
from datetime import timezone

import psycopg2
import psycopg2.errors
from psycopg2.extras import execute_values

from plugin.api import config, logger, table

from .catalog import MAX_HELD_RETENTION_MULTIPLIER, opaque_cursor
from .catalog_enrichment import (
    PROFILE_BOOTSTRAP_CAPTURE_MINUTES,
    PROFILE_CHANGE_RETENTION_EVENTS,
    live_bootstrap_session_sql,
    serialize_profile,
)
from .edge_profile_store import edge_join


# Session lifetime: the absolute lifetime from admission, and the sliding
# window each page or catch-up renews (K3), up to SLIDING_MAX_HOURS after
# creation.
SESSION_MINUTES = 60
SLIDING_MAX_HOURS = 24
EXPIRY_MODES = ("absolute", "sliding")
MAX_PAGE = 500
MAX_SESSIONS_SOURCE = 4
MAX_SESSIONS_GLOBAL = 32
# Admitted creates per (source, caller) in a rolling window (K4).
CREATE_RATE_LIMIT = 6
CREATE_RATE_WINDOW_MINUTES = 10
RETRY_AFTER_MAX_S = 300
UNAVAILABLE_RETRY_AFTER_S = 5
ADVISORY_LOCK_CLASS = 110094
ADMISSION_LOCK_KEY = 10
APPLICATION_NAME = "lumae-profile-bootstrap"
CONNECT_TIMEOUT_S = 5
# How long a create waits for another capture of the same source. With a
# capture of at most about 5 s it fits a client's 10 s request timeout, so a
# create never finishes after its client gave up and holds a slot unused.
CAPTURE_LOCK_TIMEOUT_MS = 5_000
# At most this many dead sessions are purged per create, so one create never
# pays for a large backlog (dead sessions hold no slot meanwhile).
PURGE_MAX_SESSIONS = 2
# Health `available`: a successful probe is trusted this long; a failed one is
# retried sooner, so recovery shows without probing on every health call. The
# probe connects with a short timeout (libpq's minimum is 2 s), so an
# unreachable database cannot stall /api/health for long.
AVAILABILITY_TTL_S = 60
AVAILABILITY_RETRY_S = 10
PROBE_CONNECT_TIMEOUT_S = 2
ANONYMOUS_CALLER = "anonymous"
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
# Rows per capture batch (P2-4): published profiles (keyset on track_id) or
# journal seqs. A batch is one or two statements of about 0.1 s, far below
# STATEMENT_TIMEOUT_MS, and a catch-up batch's rows stay in work_mem.
SNAPSHOT_BATCH_ROWS = 5_000
CATCHUP_BATCH_EVENTS = 5_000
TRANSFER_CONTRACT = "source_scoped_v1"
STATEMENT_TIMEOUT_MS = 20_000
LOCK_TIMEOUT_MS = 5_000


class BootstrapError(Exception):
    def __init__(self, code, status, retry_after=None):
        self.code = code
        self.status = status
        # Seconds for the HTTP Retry-After header (429 and 503 only).
        if retry_after is None and status == 503:
            retry_after = UNAVAILABLE_RETRY_AFTER_S
        self.retry_after = retry_after
        super().__init__(code)


def _retry_after(seconds):
    """Retry-After for a 429: whole seconds, at least 1, at most 300."""
    try:
        value = int(seconds)
    except (TypeError, ValueError):
        value = RETRY_AFTER_MAX_S
    return max(1, min(RETRY_AFTER_MAX_S, value))


def _limited(seconds):
    raise BootstrapError("bootstrap_session_limit", 429, _retry_after(seconds))


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


def _create_options(body):
    """(page_size, expiry_mode, client_request_id) of a create body.

    ``expiry_mode`` (K3) is ``"absolute"`` (the default) or ``"sliding"``.
    ``client_request_id`` (K5) is an optional UUID, stored in canonical form.
    """
    size = _require_request(body, creating=True)
    mode = body.get("expiry_mode")
    if mode is None:
        mode = "absolute"
    if not isinstance(mode, str) or mode not in EXPIRY_MODES:
        invalid()
    request_id = body.get("client_request_id")
    if request_id is not None:
        if not isinstance(request_id, str) or len(request_id) > 64:
            invalid()
        try:
            request_id = str(uuid.UUID(request_id))
        except ValueError:
            invalid()
    return size, mode, request_id


def _rollback_quietly(db):
    """Roll back without ever masking the exception being handled."""
    if db is None:
        return
    try:
        db.rollback()
    except Exception:
        pass


def _connect(connect_timeout):
    """Open the owned connection. Any failure here is a 503 with a warning
    that names only the error class: libpq puts parts of the DSN, including
    a password, into the messages of malformed-DSN errors."""
    dsn = getattr(config, "DATABASE_URL", None)
    if not dsn:
        raise BootstrapError("bootstrap_unavailable", 503)
    try:
        return psycopg2.connect(
            dsn, connect_timeout=connect_timeout, application_name=APPLICATION_NAME,
            keepalives=1, keepalives_idle=30)
    except (psycopg2.Error, TypeError, ValueError) as exc:
        logger.warning("lumae_analysis profile bootstrap could not connect (%s)",
                       type(exc).__name__)
        raise BootstrapError("bootstrap_unavailable", 503) from None


@contextmanager
def _connection(*, repeatable=False, connect_timeout=CONNECT_TIMEOUT_S):
    """Own one backend for this operation, separate from the request connection.

    Connection-level failures (``OperationalError``: refused, timeouts, lock
    timeouts, serialization; ``InterfaceError``: closed) are 503 with only a
    warning naming the error class. Anything else is a defect: it is logged
    with its class and traceback (never the token or the DSN), and is 503 too.
    """
    db = _connect(connect_timeout)
    try:
        db.set_session(isolation_level="REPEATABLE READ" if repeatable else "READ COMMITTED")
        with db.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, false)",
                        (str(STATEMENT_TIMEOUT_MS),))
            cur.execute("SELECT set_config('lock_timeout', %s, false)",
                        (str(LOCK_TIMEOUT_MS),))
        db.commit()
        yield db
        db.commit()
    except BootstrapError:
        _rollback_quietly(db)
        raise
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
        _rollback_quietly(db)
        logger.warning("lumae_analysis profile bootstrap database unavailable (%s)",
                       type(exc).__name__)
        raise BootstrapError("bootstrap_unavailable", 503) from None
    except Exception as exc:
        _rollback_quietly(db)
        logger.exception("lumae_analysis profile bootstrap failed (%s)", type(exc).__name__)
        raise BootstrapError("bootstrap_unavailable", 503) from None
    finally:
        try:
            db.close()
        except Exception:
            pass


def _monotonic():
    return time.monotonic()


# (dsn, checked_at, available) of the last health probe.
_availability_cache = None
# Every relation and P1-6 column v2 needs: health reports v2 available only
# once the 1.3.0 migration has run.
_REQUIRED_SESSION_COLUMNS = ("state", "expiry_mode", "client_request_id", "pages_served")


def availability():
    """Health ``profile_bootstrap.available`` (K4): the tables are migrated
    and an owned-connection probe succeeded within the last 60 seconds."""
    global _availability_cache
    dsn = getattr(config, "DATABASE_URL", None)
    if not dsn:
        return False
    now = _monotonic()
    cached = _availability_cache
    if cached is not None and cached[0] == dsn:
        ttl = AVAILABILITY_TTL_S if cached[2] else AVAILABILITY_RETRY_S
        if now - cached[1] < ttl:
            return cached[2]
    available = _probe()
    _availability_cache = (dsn, now, available)
    return available


def _probe():
    try:
        with _connection(connect_timeout=PROBE_CONNECT_TIMEOUT_S) as db:
            with db.cursor() as cur:
                cur.execute(
                    """SELECT to_regclass(%s) IS NOT NULL AND to_regclass(%s) IS NOT NULL
                              AND to_regclass(%s) IS NOT NULL
                              AND (SELECT count(*) FROM pg_attribute
                                    WHERE attrelid=to_regclass(%s) AND NOT attisdropped
                                      AND attname=ANY(%s))=%s""",
                    (_table("profile_bootstrap_snapshot"), _table("profile_bootstrap_catchup"),
                     _table("profile_bootstrap_creates"), _table("profile_bootstrap_sessions"),
                     list(_REQUIRED_SESSION_COLUMNS), len(_REQUIRED_SESSION_COLUMNS)))
                return bool(cur.fetchone()[0])
    except BootstrapError:
        return False


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
                   snapshot_count, expires_at, schema_version, state,
                   expiry_mode
              FROM {_table('profile_bootstrap_sessions')}
             WHERE token_hash=%s""" + (" FOR UPDATE" if lock else ""),
        (token_hash,))
    row = cur.fetchone()
    if (row is None or row[3] != body["catalog_instance_id"]
            or row[4] != body["catalog_instance_id"]):
        gone()
    cur.execute("SELECT now()")
    if row[12] <= cur.fetchone()[0] or row[13] != 1 or row[14] != "ready":
        gone()
    state = _state(cur, row[4])
    if (state[0] != row[5] or state[2] != row[6] or state[3] != row[7]):
        gone()
    return row, state


def _served(cur, session):
    """Count a served page or catch-up; a sliding session is renewed (K3).

    A sliding session's ``expires_at`` becomes ``now() + SESSION_MINUTES``,
    never past ``created_at + SLIDING_MAX_HOURS`` and never earlier than it
    already was. Returns the session row with the stored ``expires_at``.
    """
    if session[15] == "sliding":
        cur.execute(
            f"""UPDATE {_table('profile_bootstrap_sessions')}
                   SET pages_served=pages_served + 1,
                       expires_at=GREATEST(expires_at, LEAST(
                           now() + make_interval(mins => %s),
                           created_at + make_interval(hours => %s)))
                 WHERE session_id=%s RETURNING expires_at""",
            (SESSION_MINUTES, SLIDING_MAX_HOURS, session[0]))
        return tuple(session[:12]) + (cur.fetchone()[0],) + tuple(session[13:])
    cur.execute(f"UPDATE {_table('profile_bootstrap_sessions')} "
                "SET pages_served=pages_served + 1 WHERE session_id=%s", (session[0],))
    return session


# edge_ref (K2) names the edge row (media_revision, profile_digest), with the
# media_revision implied by the row's own payload unless stored. A snapshot
# stores a reference only when the edge's media_revision equals the row's (the
# only case serialize_profile embeds), so snapshot references are always
# {"profile_digest": ...} (about 100 bytes less per row, which keeps a 94k
# capture under its WAL budget). Only a catch-up reference whose journalled
# edge names another revision than the event payload carries media_revision.
_EDGE_REF = '{"profile_digest":%s}'
_json_string = json.encoder.encode_basestring_ascii

# P2-4: the captures build their rows in SQL. Each row's JSON is written as the
# text ``json.dumps(..., separators=(",", ":"))`` produced for it before, and
# stored as JSONB from that text, so the stored rows, the pages and the byte
# caps are unchanged (the equivalence test keeps the Python capture as its
# oracle). The SQL spells out what the Python serializers did:
#
# * ``ref_lufs``: a plain value as serialize_profile's float4() writes it
#   (see _PLAIN_LUFS); a row with any other value is serialized in Python.
# * ramps: base64 without the line breaks encode() inserts every 76 characters.
# * timestamps: datetime.isoformat() drops ".000000"; +-infinity are
#   psycopg2's datetime.max and datetime.min.
# * byte counts: json.dumps escapes DEL and every non-ASCII character
#   (ensure_ascii), which PostgreSQL's JSON text keeps as is.

# Each query computes in stages that PostgreSQL may not flatten (LIMIT, OFFSET
# 0 or a window function): the values read from the row, then the JSON text
# built from them. Flattened, every reference to a value would compute it
# again (the JSON text, sha256, to_char and the regexes several times a row).

# A "plain" ref_lufs is written in SQL. Analyzed loudness is plain in practice
# (the analyzer rejects non-finite loudness; only a level within 1e-4 LUFS of 0
# would not be). A row with any other value is serialized in Python by
# serialize_profile itself (_snapshot_fallback).
#
# serialize_profile writes float4(v): v is the double psycopg2 parsed from
# PostgreSQL's float4 text t of the REAL x, and float4() is numpy's shortest
# decimal of float32(v), written as Python's float repr. For a plain x that is
# t in fixed notation with at least one decimal (-14.0, never -14; to_jsonb
# would give -14 and "NaN"), because:
# * float32(v) is t's float4: checked, since the double can round to the
#   neighbour (for 7.038531e-26, float4() gives 7.0385313e-26). An exhaustive
#   search (every float4 with 1e-5 <= |x| < 2^23, every decimal of at most 6
#   digits) finds no plain value where it does; the check keeps the rule from
#   depending on that;
# * numpy's shortest decimal of it is t: with extra_float_digits >= 1, t is
#   PostgreSQL's shortest decimal, and below 2^23 no shorter decimal lies on
#   a rounding boundary, where the two can differ (from 2^25 up); with
#   extra_float_digits < 1, t has at most 6 significant digits, which no
#   shorter decimal shares within a normal float4;
# * 1e-4 <= |t| < 1e16 as a decimal, where repr uses fixed notation.
# Not plain: NaN and infinities, |x| >= 2^23, 0 < |t| < 1e-4 (the subnormals
# included), and a failed check. The CASE casts t only where it is a normal
# float4 (t can overflow one elsewhere: 3.403e+38 with extra_float_digits = -2).
_PLAIN_LUFS = """CASE
    WHEN p.ref_lufs = 0 THEN TRUE
    WHEN abs(p.ref_lufs) >= 0.00001 AND abs(p.ref_lufs) < 8388608
    THEN abs(p.ref_lufs::text::numeric) >= 0.0001
         AND p.ref_lufs::text::float8::real = p.ref_lufs::text::real
    ELSE FALSE END"""

# The profile's values (b): serialize_profile's inputs as they will be written.
_SNAPSHOT_VALUES = f"""
    p.track_id, p.sample_rate, p.duration_ms, p.start_ramp, p.end_ramp,
    p.analyzer_ver, p.analyzed_at, p.media_signature,
    p.ref_lufs::text AS lufs_text, {_PLAIN_LUFS} AS plain,
    to_char(p.analyzed_at, 'YYYY-MM-DD"T"HH24:MI:SS.US') AS stamp"""

# A plain ref_lufs as json.dumps writes it (NULL for any other); the text of a
# negative zero is "-0".
_SNAPSHOT_LUFS = """CASE
    WHEN NOT b.plain THEN NULL
    WHEN b.lufs_text = '-0' THEN '-0.0'
    ELSE round(b.lufs_text::numeric, greatest(scale(b.lufs_text::numeric), 1))::text END"""
_SNAPSHOT_STAMP = """CASE
    WHEN b.analyzed_at = 'infinity' THEN '9999-12-31T23:59:59.999999'
    WHEN b.analyzed_at = '-infinity' THEN '0001-01-01T00:00:00'
    WHEN right(b.stamp, 7) = '.000000' THEN left(b.stamp, -7)
    ELSE b.stamp END"""

# base64 as b64encode writes it: encode() ends a line after every 57 bytes (76
# characters), the last one included, and a ramp is rarely that long.
_SNAPSHOT_BASE64 = (r"CASE WHEN octet_length({ramp}) >= 57 "
                    r"THEN translate(encode({ramp}, 'base64'), E'\n', '') "
                    r"ELSE encode({ramp}, 'base64') END")

# json.dumps(serialize_profile(...), separators=(",", ":")), from b.
_SNAPSHOT_DOC = r"""
    '{"track_id":' || to_jsonb(b.track_id)::text
    || ',"source":"waveform","sample_rate":' || b.sample_rate
    || ',"duration_ms":' || b.duration_ms
    || ',"ref_lufs":' || """ + _SNAPSHOT_LUFS + r"""
    || ',"start_ramp":"' || """ + _SNAPSHOT_BASE64.format(ramp='b.start_ramp') + r"""
    || '","end_ramp":"' || """ + _SNAPSHOT_BASE64.format(ramp='b.end_ramp') + r"""
    || '","analyzer_ver":' || b.analyzer_ver
    || ',"analyzed_at":"' || """ + _SNAPSHOT_STAMP + r"""
    || '","media_signature":' || COALESCE('"' || b.revision || '"', 'null')
    || ',"media_revision":' || COALESCE('"' || b.revision || '"', 'null')
    || '}'"""

# The length of that text without building it: its keys and punctuation, plus
# each value (base64 of n bytes is 4 * ceil(n / 3) characters, analyzed_at 19
# or, with microseconds, 26, a revision is "sha256:" and 64 hex digits). The
# equivalence test checks it against json.dumps.
_SNAPSHOT_DOC_FIXED = len(
    '{"track_id":,"source":"waveform","sample_rate":,"duration_ms":,"ref_lufs":,'
    '"start_ramp":"","end_ramp":"","analyzer_ver":,"analyzed_at":"",'
    '"media_signature":,"media_revision":}')
_SNAPSHOT_DOC_LENGTH = f"""
    {_SNAPSHOT_DOC_FIXED} + length(to_jsonb(b.track_id)::text)
    + length(b.sample_rate::text) + length(b.duration_ms::text)
    + length({_SNAPSHOT_LUFS})
    + 4 * ((octet_length(b.start_ramp) + 2) / 3) + 4 * ((octet_length(b.end_ramp) + 2) / 3)
    + length(b.analyzer_ver::text)
    + CASE WHEN b.analyzed_at = '-infinity' OR b.analyzed_at <> 'infinity'
                AND date_trunc('second', b.analyzed_at) = b.analyzed_at THEN 19 ELSE 26 END
    + CASE WHEN b.media_signature <> '' THEN 2 * 73 ELSE 2 * 4 END"""

# The event's values (a): the journal row with its edge split off. The payload
# is copied as journalled, while the Python capture parsed it (json.loads) and
# dumped it again (json.dumps). That round trip changes no number the journal
# holds: its only writer, _profile_json, writes json.dumps text, so every number
# is a Python float repr or int, which reads back to the same float or int and
# is written again as the same repr, and JSONB keeps its value and scale.
_CATCHUP_VALUES = """
    j.seq, j.track_id, j.operation, j.created_at,
    CASE WHEN j.split THEN j.payload - 'edge_profile' ELSE j.payload END AS part_json,
    to_char(j.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') AS stamp,
    CASE WHEN j.split THEN jsonb_strip_nulls(jsonb_build_object(
        'media_revision', NULLIF(j.payload#>'{edge_profile,media_revision}',
                                 j.payload->'media_revision'),
        'profile_digest', j.payload#>'{edge_profile,profile_digest}'))
    END AS edge_ref"""

# json.dumps of the event dict the Python capture built, from b (b.part is the
# payload part's JSON text).
_CATCHUP_DOC = r"""
    '{"seq":' || b.seq
    || ',"track_id":' || to_jsonb(b.track_id)::text
    || ',"operation":' || to_jsonb(b.operation)::text
    || ',"payload":' || b.part
    || ',"created_at":"' || CASE
           WHEN b.created_at = 'infinity' THEN '9999-12-31T23:59:59.999999'
           WHEN b.created_at = '-infinity' THEN '0001-01-01T00:00:00'
           WHEN right(b.stamp, 7) = '.000000' THEN left(b.stamp, -7)
           ELSE b.stamp END
    || 'Z"}'"""

# A JSON number with a fraction, as PostgreSQL writes it, is json.loads'
# float. For a number that was a float repr (as every journalled number is; see
# _CATCHUP_VALUES) the text equals the repr unless it is below 1e-4: repr used
# an exponent (1e-05), which JSONB keeps as 0.00001. The pattern also matches
# trailing zeros (never in a repr) and 16 or more digits (a long repr, such as
# 0.30000000000000004, whose count comes out unchanged); those are measured
# one by one as well.
_NOT_FLOAT_REPR = r"E'0\\.0000|\\.[0-9]*[0-9]0([^0-9]|$)|[0-9]{16}'"


def _float_repr_extra(number):
    """SQL: len(repr(float(t))) - len(t) for ``number``, a JSON number text
    with a fraction as PostgreSQL writes it.

    Exact only when ``t`` holds the digits of a Python float repr, as every
    number in the journal does (its only writer, _profile_json, writes
    json.dumps text): repr(float(t)) then has the same significant digits,
    and only the notation is worked out here. For any other number (more than
    17 significant digits, or 16 or 17 that are not the shortest repr of their
    float) repr(float(t)) has other digits, and the count is wrong; ``least``
    only keeps the arithmetic bounded for such a number."""
    return f"""(
        SELECT d.neg - length(d.t) + CASE
            WHEN d.n = 0 THEN 3
            WHEN d.e >= -4 AND d.e < 16 AND d.e >= 0 THEN d.e + 2 + greatest(1, d.n - d.e - 1)
            WHEN d.e >= -4 AND d.e < 16 THEN 1 - d.e + d.n
            ELSE d.n + (d.n > 1)::int + 2 + greatest(2, length(abs(d.e)::text)) END
          FROM (SELECT y.t, y.neg, least(length(rtrim(ltrim(y.ip || y.fp, '0'), '0')), 17) AS n,
                       CASE WHEN ltrim(y.ip, '0') <> '' THEN length(ltrim(y.ip, '0')) - 1
                            ELSE length(ltrim(y.fp, '0')) - length(y.fp) - 1 END AS e
                  FROM (SELECT x.t, (left(x.t, 1) = '-')::int AS neg,
                               split_part(ltrim(x.t, '-'), '.', 1) AS ip,
                               split_part(x.t, '.', 2) AS fp
                          FROM (SELECT {number} AS t) x) y) d)"""


def _numbers_extra(numbers):
    """SQL: what json.dumps(json.loads(t)) adds to the floats in ``numbers``,
    a JSON text holding no strings (so every number in it is a number)."""
    return f"""CASE WHEN {numbers} !~ {_NOT_FLOAT_REPR} THEN 0
        ELSE (SELECT COALESCE(sum({_float_repr_extra('m[1]')}), 0)
                FROM regexp_matches({numbers}, E'-?[0-9]+\\\\.[0-9]+', 'g') m) END"""


# The payload part is PostgreSQL's JSON text of the journalled JSONB, which
# the Python capture parsed and dumped again: json.dumps writes its ", " and
# ": " separators without the space, and each float as its repr.
# A flat object without a backslash (every value a scalar, no string holds a
# quote) of k keys has exactly k '": ' and 2k - 1 separators, and its numbers
# are its number values (a serialize_profile payload, as the journal holds
# them). Anything else is measured on its text with the strings removed: then
# the only spaces are separators and the only numbers are numbers.
_FLAT_NUMBERS = """jsonb_path_query_array(b.part_json, 'strict $.* ? (@.type() == "number")')::text"""
_CATCHUP_PART_EXTRA = f"""CASE
    WHEN b.part = 'null' THEN 0
    WHEN left(b.part, 1) = '{{' AND strpos(substr(b.part, 2), '{{') = 0
         AND strpos(b.part, '[') = 0 AND strpos(b.part, E'\\\\') = 0
    THEN -greatest(2 * ((length(b.part) - length(replace(b.part, '": ', ''))) / 3) - 1, 0)
         + {_numbers_extra(_FLAT_NUMBERS)}
    ELSE (SELECT -(length(x.bare) - length(replace(x.bare, ' ', ''))) + {_numbers_extra('x.bare')}
            FROM (SELECT regexp_replace(b.part, E'"(?:[^"\\\\\\\\]|\\\\\\\\.)*"', '', 'g') AS bare) x)
    END"""


def _ascii_escape_extra(text):
    """SQL: the characters ``json.dumps`` (``ensure_ascii``) writes beyond
    PostgreSQL's JSON text for ``text``. Both escape quotes, backslashes and
    control characters alike; Python also writes DEL and every non-ASCII
    character as a 6-character ``\\uXXXX`` escape, and a character beyond the
    Basic Multilingual Plane as two. In a UTF8 database a text without DEL
    whose characters are all one byte long is ASCII, which is checked first."""
    return (f"CASE WHEN octet_length({text}) = length({text}) AND strpos({text}, chr(127)) = 0 "
            "AND (SELECT current_setting('server_encoding') = 'UTF8') THEN 0 "
            f"WHEN {text} ~ E'[^\\\\x01-\\\\x7e]' THEN "
            f"5 * length(regexp_replace({text}, E'[\\\\x01-\\\\x7e]+', '', 'g')) "
            f"+ 6 * length(regexp_replace({text}, E'[^\\\\U00010000-\\\\U0010ffff]+', '', 'g')) "
            "ELSE 0 END")


def _snapshot_batch(cur, source, session_id, ordinal, after):
    """Capture the next SNAPSHOT_BATCH_ROWS profiles after ``after`` (None:
    from the start) as ordinals ``ordinal`` onwards, in statements of the
    capture's snapshot: measure the batch, copy exactly its rows with a plain
    ref_lufs, and serialize the others in Python (_snapshot_fallback).

    Returns ``(rows, json_bytes, last_track_id, unrepresentable)``, where
    ``json_bytes`` counts the rows' waveform JSON as MAX_SNAPSHOT_BYTES does.
    Nothing is copied if a row is unrepresentable (the capture then fails).
    """
    params = {"source": source, "after": after, "limit": SNAPSHOT_BATCH_ROWS,
              "session_id": session_id, "ordinal": ordinal}
    keyset = "" if after is None else " AND p.track_id > %(after)s"
    edge_keyset = "" if after is None else " AND e.track_id > %(after)s"
    cur.execute(
        f"""SELECT count(*),
                   COALESCE(sum({_SNAPSHOT_DOC_LENGTH} + {_ascii_escape_extra('b.track_id')})
                            FILTER (WHERE b.plain), 0),
                   max(b.track_id), COALESCE(bool_or(b.unrepresentable), FALSE),
                   array_agg(b.i ORDER BY b.i) FILTER (WHERE NOT b.plain),
                   array_agg(b.track_id ORDER BY b.i) FILTER (WHERE NOT b.plain)
              FROM (SELECT {_SNAPSHOT_VALUES},
                           -- psycopg2 has no datetime for these: rows the
                           -- Python capture failed on.
                           (p.analyzed_at <> '-infinity' AND p.analyzed_at < '0001-01-01'
                            OR p.analyzed_at <> 'infinity'
                               AND p.analyzed_at >= '10000-01-01') AS unrepresentable,
                           row_number() OVER (ORDER BY p.track_id) - 1 AS i
                      FROM {_table('published_source_profiles')} p
                     WHERE p.catalog_instance_id=%(source)s{keyset}
                     ORDER BY p.track_id LIMIT %(limit)s) b""", params)
    rows, json_bytes, last, unrepresentable, positions, others = cur.fetchone()
    if not rows or unrepresentable:
        return rows, json_bytes, last, unrepresentable
    params["last"] = last
    # The edge each row embeds is the one edge_join() picks: for the row's
    # (track_id, media_signature), the newest updated_at, then the lowest
    # profile_digest. It is picked set-based over the batch's track range, a
    # hash or merge join (see _capture_snapshot) instead of a lookup per row,
    # and only its key columns are read, so the TOASTed edge payload is never
    # detoasted or copied; snapshot_page resolves the reference (K2). The
    # ordinals count every row of the batch, the plain ones are copied here.
    cur.execute(
        f"""INSERT INTO {_table('profile_bootstrap_snapshot')}
                   (session_id, ordinal, payload, edge_ref)
            SELECT %(session_id)s, %(ordinal)s + b.i, ({_SNAPSHOT_DOC})::jsonb,
                   CASE WHEN b.revision IS NOT NULL AND edge.media_revision=b.revision
                             AND edge.profile_digest <> ''
                        THEN jsonb_build_object('profile_digest', edge.profile_digest) END
              FROM (SELECT {_SNAPSHOT_VALUES},
                           CASE WHEN p.media_signature <> ''
                                THEN 'sha256:' || encode(sha256(convert_to(
                                    p.media_signature, 'UTF8')), 'hex')
                           END AS revision,
                           row_number() OVER (ORDER BY p.track_id) - 1 AS i
                      FROM {_table('published_source_profiles')} p
                     WHERE p.catalog_instance_id=%(source)s{keyset}
                       AND p.track_id <= %(last)s) b
              LEFT JOIN (
                SELECT DISTINCT ON (e.track_id, e.media_signature)
                       e.track_id, e.media_signature, e.media_revision, e.profile_digest
                  FROM {_table('edge_profiles')} e
                 WHERE e.catalog_instance_id=%(source)s{edge_keyset}
                   AND e.track_id <= %(last)s
                 ORDER BY e.track_id, e.media_signature, e.updated_at DESC, e.profile_digest
              ) edge ON edge.track_id=b.track_id AND edge.media_signature=b.media_signature
             WHERE b.plain""",
        params)
    if cur.rowcount != rows - len(others or ()):
        raise RuntimeError("snapshot batch changed within its snapshot")
    if others:
        json_bytes += _snapshot_fallback(cur, source, session_id, ordinal, positions, others)
    return rows, json_bytes, last, unrepresentable


def _snapshot_fallback(cur, source, session_id, ordinal, positions, track_ids):
    """Serialize the batch's rows whose ref_lufs is not plain as the capture
    did before P2-4: serialize_profile, json.dumps and the same edge
    reference, from the rows psycopg2 reads in the capture's snapshot. Inserts
    them at their ordinals and returns their JSON bytes. Analyzed loudness is
    plain in practice, so this rarely runs (see _PLAIN_LUFS)."""
    cur.execute(
        f"""SELECT p.track_id, p.sample_rate, p.duration_ms, p.ref_lufs,
                   p.start_ramp, p.end_ramp, p.analyzer_ver, p.analyzed_at,
                   p.media_signature, edge.media_revision, edge.profile_digest
              FROM {_table('published_source_profiles')} p
              {edge_join(columns='e.media_revision, e.profile_digest')}
             WHERE p.catalog_instance_id=%s AND p.track_id=ANY(%s)
             ORDER BY p.track_id""", (source, list(track_ids)))
    found = cur.fetchall()
    if [row[0] for row in found] != list(track_ids):
        raise RuntimeError("snapshot batch changed within its snapshot")
    byte_count = 0
    values = []
    for position, row in zip(positions, found):
        payload = serialize_profile(*row[:9])
        text = json.dumps(payload, separators=(",", ":"))
        byte_count += len(text.encode())
        revision = payload.get("media_revision")
        edge_ref = (_EDGE_REF % _json_string(row[10])
                    if revision and row[9] == revision and row[10] else None)
        values.append((session_id, ordinal + position, text, edge_ref))
    execute_values(
        cur, f"INSERT INTO {_table('profile_bootstrap_snapshot')} "
             "(session_id, ordinal, payload, edge_ref) VALUES %s",
        values, template="(%s, %s, %s::jsonb, %s::jsonb)")
    return byte_count


def _catchup_batch(cur, session, after, upto, count, byte_count, limits):
    """Capture the journal events ``after < seq <= upto`` as ordinals
    ``count`` onwards; ``byte_count`` is what the earlier batches used.

    Returns ``(rows, first_gap, first_over, json_bytes)``: ``first_gap`` and
    ``first_over`` are the batch indexes of the first event that is not the
    next seq, and of the first that exceeds ``limits`` (None if none), and
    ``json_bytes`` counts this batch's events as MAX_CATCHUP_BYTES does.
    """
    max_events, max_bytes = limits
    cur.execute(
        f"""WITH src AS MATERIALIZED (
                SELECT c.seq, c.i, c.doc, c.edge_ref,
                       length(c.doc) + {_ascii_escape_extra('c.doc')} + c.part_extra AS json_bytes
                  FROM (SELECT b.seq, b.i, b.edge_ref, {_CATCHUP_DOC} AS doc,
                               {_CATCHUP_PART_EXTRA} AS part_extra
                          FROM (SELECT a.*, COALESCE(a.part_json::text, 'null') AS part
                                  FROM (SELECT {_CATCHUP_VALUES},
                                               row_number() OVER (ORDER BY j.seq) - 1 AS i
                                          FROM (SELECT seq, track_id, operation, payload, created_at,
                                                       COALESCE(
                                                           jsonb_typeof(payload#>'{{edge_profile,media_revision}}')='string'
                                                           AND jsonb_typeof(payload#>'{{edge_profile,profile_digest}}')='string',
                                                           FALSE) AS split
                                                  FROM {_table('profile_changes')}
                                                 WHERE catalog_instance_id=%(source)s AND epoch=%(epoch)s
                                                   AND seq>%(after)s AND seq<=%(upto)s) j
                                         ORDER BY j.seq OFFSET 0) a
                                OFFSET 0) b
                        OFFSET 0) c
            ), stored AS (
                INSERT INTO {_table('profile_bootstrap_catchup')}
                       (session_id, ordinal, seq, payload, edge_ref)
                SELECT %(session_id)s, %(count)s + i, seq, doc::jsonb, edge_ref FROM src
            )
            SELECT count(*),
                   min(i) FILTER (WHERE seq <> %(after)s + 1 + i),
                   min(i) FILTER (WHERE %(count)s + i + 1 > %(max_events)s
                                     OR %(byte_count)s + total > %(max_bytes)s),
                   COALESCE(max(total), 0)
              FROM (SELECT i, seq, sum(json_bytes) OVER (ORDER BY i) AS total FROM src) acc""",
        {"source": session[4], "epoch": session[7], "after": after, "upto": upto,
         "session_id": session[0], "count": count, "byte_count": byte_count,
         "max_events": max_events, "max_bytes": max_bytes})
    return cur.fetchone()


def _capture_catchup(cur, session, head, limits):
    """Capture the first catch-up, events ``snapshot_seq < seq <= head``, in
    CATCHUP_BATCH_EVENTS batches; returns the event count.

    Like the Python capture it replaced, it answers 410 at the first event
    that is not the next seq (compacted or purged meanwhile), and 413 at the
    first that exceeds ``limits``, whichever comes first. Journal rows never
    change, they only disappear, so batches read in statements of their own
    see the rows a single read would, unless some disappeared: 410 either way.
    """
    count = byte_count = 0
    after = session[9]
    while after < head:
        upto = min(after + CATCHUP_BATCH_EVENTS, head)
        rows, gap, over, json_bytes = _catchup_batch(
            cur, session, after, upto, count, byte_count, limits)
        if gap is not None and (over is None or gap <= over):
            gone()
        if over is not None:
            raise BootstrapError("bootstrap_snapshot_limit", 413)
        if rows != upto - after:
            gone()  # the next event after this batch's last one is missing
        count += rows
        byte_count += json_bytes
        after = upto
    if count != head - session[9]:
        gone()
    return count


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
    """UTC ISO-8601 with ``Z``, whatever the database session's TimeZone."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _metadata(session):
    cursor = opaque_cursor(session[4], session[7], session[9])
    return {"catalog_epoch": session[6], "profile_epoch": session[7],
            "snapshot_cursor": cursor, "snapshot_seq": session[9],
            "total_profiles": session[11], "expires_at": _iso(session[12])}


def _admit(db, source, session_id, token_hash, secret, size, mode, request_id, caller_key):
    """Admission: one short transaction under the global advisory lock.

    Returns ``(state, expires_at)``; the ``capturing`` session row is
    committed, so it holds its slot and (with the admission head as
    ``snapshot_seq``) the journal floor from here on.
    """
    sessions = _table("profile_bootstrap_sessions")
    creates = _table("profile_bootstrap_creates")
    window = f"interval '{int(CREATE_RATE_WINDOW_MINUTES)} minutes'"
    with db.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)",
                    (ADVISORY_LOCK_CLASS, ADMISSION_LOCK_KEY))
        cur.execute(
            f"""SELECT count(*), EXTRACT(EPOCH FROM min(created_at) + {window} - now())
                  FROM {creates}
                 WHERE catalog_instance_id=%s AND caller_key=%s
                   AND created_at > now() - {window}""",
            (source, caller_key))
        created, wait = cur.fetchone()
        if created >= CREATE_RATE_LIMIT:
            _limited(math.ceil(float(wait)))
        if request_id is not None:
            # K5: a retried create replaces its unclaimed session. Only a
            # non-key column changes, so this never waits for a capture's
            # foreign-key locks; a session being paged is claimed and skipped.
            cur.execute(
                f"""UPDATE {sessions} SET expires_at=now()
                     WHERE session_id IN (
                        SELECT session_id FROM {sessions}
                         WHERE source_scope=%s AND client_request_id=%s
                           AND pages_served=0 AND expires_at > now()
                           FOR NO KEY UPDATE SKIP LOCKED)""",
                (source, request_id))
        free_at = ("LEAST(s.expires_at, CASE WHEN s.state='capturing' THEN s.created_at + "
                   f"interval '{int(PROFILE_BOOTSTRAP_CAPTURE_MINUTES)} minutes' END)")
        cur.execute(
            f"""SELECT count(*) FILTER (WHERE s.source_scope=%s), count(*),
                       EXTRACT(EPOCH FROM min({free_at}) FILTER (WHERE s.source_scope=%s)
                                          - now()),
                       EXTRACT(EPOCH FROM min({free_at}) - now())
                  FROM {sessions} s
                 WHERE {live_bootstrap_session_sql('s')}""",
            (source, source))
        in_source, in_total, source_wait, total_wait = cur.fetchone()
        waits = []
        if in_source >= MAX_SESSIONS_SOURCE:
            waits.append(source_wait)
        if in_total >= MAX_SESSIONS_GLOBAL:
            waits.append(total_wait)
        if waits:
            _limited(math.ceil(max(float(value or 0) for value in waits)))
        state = _state(cur, source)
        cur.execute(f"INSERT INTO {creates} (catalog_instance_id, caller_key) VALUES (%s, %s)",
                    (source, caller_key))
        cur.execute(
            f"""INSERT INTO {sessions}
                (session_id, token_hash, signing_secret, source_scope,
                 catalog_instance_id, core_server_id, catalog_epoch,
                 profile_epoch, schema_version, page_size, snapshot_seq,
                 snapshot_count, expires_at, state, expiry_mode, client_request_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,0,
                        now() + make_interval(mins => %s), 'capturing', %s, %s)
                RETURNING expires_at""",
            (session_id, token_hash, secret, source, source, state[0], state[2],
             state[3], size, state[4], SESSION_MINUTES, mode, request_id))
        expires_at = cur.fetchone()[0]
    db.commit()
    return state, expires_at


def _purge(db):
    """Best-effort housekeeping before a capture: a failure is logged (error
    class only) and never fails the create, because dead sessions hold no
    slot and no floor anyway."""
    try:
        _delete_dead(db)
    except Exception as exc:
        _rollback_quietly(db)
        logger.warning("lumae_analysis profile bootstrap purge failed (%s)",
                       type(exc).__name__)


def _delete_dead(db):
    """Delete up to PURGE_MAX_SESSIONS sessions that are no longer live
    (oldest expiry first), and expired rate-limit rows. Runs without the
    global lock; ``SKIP LOCKED`` leaves a row that a page or a capture holds
    to a later purge."""
    sessions = _table("profile_bootstrap_sessions")
    with db.cursor() as cur:
        cur.execute(
            f"""DELETE FROM {sessions} WHERE session_id IN (
                    SELECT s.session_id FROM {sessions} s
                     WHERE NOT {live_bootstrap_session_sql('s')}
                     ORDER BY s.expires_at LIMIT %s
                       FOR UPDATE OF s SKIP LOCKED)""", (PURGE_MAX_SESSIONS,))
        cur.execute(
            f"DELETE FROM {_table('profile_bootstrap_creates')} "
            f"WHERE created_at <= now() - interval '{int(CREATE_RATE_WINDOW_MINUTES)} minutes'")
    db.commit()


def _capture(db, source, session_id, admitted):
    """Capture the source under its per-source lock; returns (count, seq)."""
    with db.cursor() as cur:
        # A create waits for another capture of the same source (not 503).
        cur.execute("SELECT set_config('lock_timeout', %s, true)",
                    (str(CAPTURE_LOCK_TIMEOUT_MS),))
        cur.execute("SELECT pg_advisory_lock(%s, hashtext(%s))",
                    (ADVISORY_LOCK_CLASS, source))
    db.commit()
    try:
        db.set_session(isolation_level="REPEATABLE READ")
        try:
            captured = _capture_snapshot(db, source, session_id, admitted)
        except (psycopg2.errors.SerializationFailure, psycopg2.errors.ForeignKeyViolation):
            # Only this session's own row is changed concurrently: a retried
            # create (K5) replaced it, and a purge may have deleted it.
            gone()
        db.commit()
        return captured
    finally:
        _rollback_quietly(db)
        try:
            db.set_session(isolation_level="READ COMMITTED")
            with db.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s, hashtext(%s))",
                            (ADVISORY_LOCK_CLASS, source))
            db.commit()
        except Exception:
            # The connection is closed next, which releases the lock too.
            _rollback_quietly(db)


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
        # Batches are planned one by one; JIT compilation would cost more
        # than a batch runs. A batch's edge join must be hashed or merged:
        # without statistics for a source's rows (a source added or refilled
        # since the last ANALYZE) both sides are estimated at one row, and a
        # nested loop would run the edge side once per profile (a minute at
        # 94k instead of a second).
        cur.execute("SELECT set_config('jit', 'off', true), "
                    "set_config('enable_nestloop', 'off', true)")
        # Every batch reads this transaction's snapshot.
        ordinal = 0
        byte_count = 0
        after = None
        while True:
            rows, json_bytes, last, unrepresentable = _snapshot_batch(
                cur, source, session_id, ordinal, after)
            if unrepresentable:
                # The Python capture failed on these rows (psycopg2 has no
                # datetime for them); keep failing rather than invent a text.
                raise ValueError("analyzed_at outside the datetime range")
            ordinal += rows
            byte_count += json_bytes
            if ordinal > MAX_SNAPSHOT_ROWS or byte_count > MAX_SNAPSHOT_BYTES:
                raise BootstrapError("bootstrap_snapshot_limit", 413)
            if rows < SNAPSHOT_BATCH_ROWS:
                break
            after = last
        # The captured head replaces the admission head the hold used so far.
        snapshot_seq = int(state[4])
        cur.execute(
            f"""UPDATE {_table('profile_bootstrap_sessions')}
                   SET snapshot_seq=%s, snapshot_count=%s, state='ready'
                 WHERE session_id=%s""", (snapshot_seq, ordinal, session_id))
    return ordinal, snapshot_seq


def _abandon(db, session_id):
    """Delete the admitted row of a failed capture, never raising."""
    _rollback_quietly(db)
    try:
        with db.cursor() as cur:
            cur.execute(f"DELETE FROM {_table('profile_bootstrap_sessions')} "
                        "WHERE session_id=%s", (session_id,))
        db.commit()
    except Exception:
        # It stops counting after PROFILE_BOOTSTRAP_CAPTURE_MINUTES.
        _rollback_quietly(db)


def create_session(body, caller=ANONYMOUS_CALLER):
    """Create a v2 session. ``caller`` identifies the requester for the
    per-(source, caller) create rate limit; only its hash is stored."""
    size, mode, request_id = _create_options(body)
    source = body["catalog_instance_id"]
    token = secrets.token_hex(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    session_id = str(uuid.uuid4())
    secret = secrets.token_hex(32)
    caller_key = hashlib.sha256(str(caller or ANONYMOUS_CALLER).encode()).hexdigest()
    with _connection() as db:
        admitted, expires_at = _admit(db, source, session_id, token_hash, secret, size, mode,
                                      request_id, caller_key)
        try:
            _purge(db)
            ordinal, snapshot_seq = _capture(db, source, session_id, admitted)
        except BaseException:
            _abandon(db, session_id)
            raise
    catalog_epoch, profile_epoch = admitted[2], admitted[3]
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": TRANSFER_CONTRACT,
            "catalog_instance_id": source,
            "session_token": token, "page_size": size,
            "snapshot_count": ordinal, "total_profiles": ordinal,
            "catalog_epoch": catalog_epoch, "profile_epoch": profile_epoch,
            "snapshot_seq": snapshot_seq, "expires_at": _iso(expires_at),
            "snapshot_cursor": opaque_cursor(source, profile_epoch, snapshot_seq),
            "cursor": opaque_cursor(source, profile_epoch, snapshot_seq),
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
            session = _served(cur, session)
            # Built in SQL, so the JSONB (and so the wire JSON) is exactly what
            # the pre-K2 capture stored with the edge embedded. The lookup
            # matches the row's track_id and media_revision (see _edge_lookup),
            # so the edge payload is detoasted once, only to embed it.
            # The page's rows are chosen (ORDER BY ... LIMIT) before the
            # lookup: without statistics on the snapshot table (a fresh
            # install) a planner could otherwise join every remaining row of
            # the session, detoasting each edge, and limit only after sorting.
            cur.execute(
                f"""SELECT CASE WHEN edge.payload IS NOT NULL
                            THEN s.payload || jsonb_build_object('edge_profile', edge.payload)
                            ELSE s.payload END
                      FROM (SELECT ordinal, payload, edge_ref
                              FROM {_table('profile_bootstrap_snapshot')}
                             WHERE session_id=%s AND ordinal>=%s
                             ORDER BY ordinal LIMIT %s) s
                      {_edge_lookup('s', 's.payload')}
                     ORDER BY s.ordinal""",
                (session[0], ordinal, session[8], session[4]))
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
                # An upsert's embedded edge is split off in SQL and kept as a
                # reference, like the snapshot. See _capture_snapshot for jit.
                cur.execute("SELECT set_config('jit', 'off', true)")
                _capture_catchup(cur, session, head, catchup_limits(state[6]))
                cur.execute(f"UPDATE {_table('profile_bootstrap_sessions')} SET head_seq=%s "
                            "WHERE session_id=%s", (head, session[0]))
                session = tuple(session[:10]) + (head,) + tuple(session[11:])
            head = session[10]
            count = head - session[9]
            if ordinal > count or (ordinal != 0 and ordinal % session[8]):
                invalid()
            session = _served(cur, session)
            # The page's rows are chosen before the edge lookup, as in
            # snapshot_page.
            cur.execute(
                f"""SELECT CASE WHEN edge.payload IS NOT NULL
                            THEN jsonb_set(c.payload, '{{payload,edge_profile}}', edge.payload)
                            ELSE c.payload END
                      FROM (SELECT ordinal, payload, edge_ref
                              FROM {_table('profile_bootstrap_catchup')}
                             WHERE session_id=%s AND ordinal>=%s
                             ORDER BY ordinal LIMIT %s) c
                      {_edge_lookup('c', "c.payload->'payload'")}
                     ORDER BY c.ordinal""",
                (session[0], ordinal, session[8], session[4]))
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
    """Delete the session with this token and source, whatever its state.

    Expired and identity-stale sessions are deleted too, so the slot is free
    at once. Always ``released: true`` for a valid request, also for an
    unknown token or another source's token (which deletes nothing).
    """
    _require_request(body)
    token_hash = hashlib.sha256(body["session_token"].encode()).hexdigest()
    with _connection() as db:
        with db.cursor() as cur:
            cur.execute(f"DELETE FROM {_table('profile_bootstrap_sessions')} "
                        "WHERE token_hash=%s AND source_scope=%s AND catalog_instance_id=%s",
                        (token_hash, body["catalog_instance_id"], body["catalog_instance_id"]))
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": TRANSFER_CONTRACT, "released": True}
