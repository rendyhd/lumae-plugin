"""Protocol-faithful Lumae profile sync client for the P2-6 gate.

This is **not** the Auralscape client; plan §H C-8 drives the real app code.
It follows the same protocol, per the sync contract
(``docs/contracts/LUMAE_SYNC_CONTRACT.md``) and the hand-off
(``docs/handoff/AURALSCAPE_SYNC_HARDENING_HANDOFF.md``), so the server half of
the gate can be run and measured without a phone:

* capability detection from ``GET /api/health`` (the v2 selection rule of
  contract §2, plus ``sliding_expiry`` and ``idempotent_create``) and the
  source from ``GET /api/catalog/health``;
* v2 bootstrap: create (``page_size`` 50, ``expiry_mode: "sliding"``, one
  ``client_request_id`` UUID per logical create, reused on a retry), snapshot
  pages, catch-up, release, then ``/profiles/changes`` deltas (limit 100);
* the legacy path: ``/profiles/bootstrap`` (limit 50) then ``/profiles/changes``
  from the pinned cursor;
* ``Accept-Encoding: gzip`` on every request (K1), decoded here, with the
  bytes on the wire counted;
* ``Retry-After`` on 429 and 503 (K4, capped at 300 s), backoff on connection
  errors, one silent reconnect of a kept-alive socket;
* a file-backed SQLite "phone": each page is staged and its checkpoint
  advanced in **one** transaction, so a crash at any point resumes from the
  last committed page; the waveform generation is published atomically;
  edges are written per page (C-4) and are valid only for a matching
  ``media_revision``;
* every edge is verified before it is stored: ``profile_digest`` over the
  canonical re-sorted JSON (contract §4.2), identity, and the ``boundaries``
  rule of §4.4. An upsert without a valid edge deletes the local edge
  (contract §6 rule 1); a ``delete`` removes both.

Edge payloads are stored zlib-compressed (level 1) to save disk on the test
machine; the verified canonical JSON is what is compressed.

CLI::

    client_sim.py sync   --base-url http://127.0.0.1:18080 --db dev1.sqlite [--mode v2|legacy|auto]
    client_sim.py digest --db dev1.sqlite

``poll_routes`` (the status-route poller) is used by ``run_server_matrix.py``;
it has no subcommand.
"""
import argparse
import collections
import gzip
import hashlib
import http.client
import json
import math
import os
import random
import sqlite3
import sys
import threading
import time
import uuid
import zlib
from urllib.parse import urlencode, urlsplit

PREFIX = "/plugins/lumae_analysis"
ENVELOPE = {"protocol_version": 2, "schema_version": 1, "transfer_contract": "source_scoped_v1"}
RETRY_AFTER_CAP_S = 300
SNAPSHOT_META = ("catalog_epoch", "profile_epoch", "snapshot_cursor", "snapshot_seq",
                 "total_profiles")


class TransportError(Exception):
    """Connection-level failure (refused, reset, timeout): retryable."""


class SyncFailed(Exception):
    pass


class SimulatedCrash(Exception):
    """Raised by a hook to simulate the app being killed at that point."""


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)


def pct(values, p):
    values = sorted(values)
    if not values:
        return None
    k = max(0, min(len(values) - 1, int(round(p / 100 * (len(values) - 1)))))
    return values[k]


def latency_summary(samples):
    if not samples:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "max_ms": None}
    return {"n": len(samples), "p50_ms": round(pct(samples, 50), 1),
            "p95_ms": round(pct(samples, 95), 1), "max_ms": round(max(samples), 1)}


# ---- edge verification (contract §4) ---------------------------------------

def _window_reason(window, rate, origin, end):
    if not isinstance(window, dict):
        return "window_missing"
    try:
        w_origin = int(window["origin_frame"])
        w_end = w_origin + int(window["covered_frames"])
        bins = int(window["bin_count"])
        bounds = window["boundaries"]
    except (KeyError, TypeError, ValueError):
        return "window_fields"
    if (w_origin, w_end) != (origin, end):
        return "window_bounds"
    if not isinstance(bounds, list) or len(bounds) != bins + 1:
        return "boundaries_length"
    for index, value in enumerate(bounds):
        if value != min(end, origin + (index * rate) // 10):
            return "boundaries_value"
    return None


def verify_edge(edge, track_id, media_revision):
    """None when the edge may be stored for this row, else the reason."""
    if not isinstance(edge, dict):
        return "not_object"
    if edge.get("schema_version") != 2:
        return "schema_version"
    if edge.get("track_id") != track_id:
        return "track_id"
    if not media_revision or edge.get("media_revision") != media_revision:
        return "media_revision"
    digest = edge.get("profile_digest")
    body = {key: value for key, value in edge.items() if key != "profile_digest"}
    if hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest() != digest:
        return "digest"
    try:
        rate = int(edge["source"]["sample_rate"])
        decoded = int(edge["source"]["decoded_frames"])
    except (KeyError, TypeError, ValueError):
        return "source_fields"
    reason = _window_reason(edge.get("head"), rate, 0, min(decoded, 30 * rate))
    if reason:
        return "head_" + reason
    reason = _window_reason(edge.get("tail"), rate, max(0, decoded - 30 * rate), decoded)
    return "tail_" + reason if reason else None


# ---- statistics --------------------------------------------------------------

class Stats:
    """Thread-safe per-client request statistics."""

    def __init__(self):
        self.lock = threading.Lock()
        self.latency = collections.defaultdict(list)
        self.wire = collections.Counter()
        self.decoded = collections.Counter()
        self.status = collections.Counter()
        self.events = collections.Counter()
        self.sqlite_tx_ms = []

    def record(self, kind, status, ms, wire=0, decoded=0):
        with self.lock:
            self.latency[kind].append(ms)
            self.wire[kind] += wire
            self.decoded[kind] += decoded
            self.status[str(status)] += 1

    def count(self, name, n=1):
        with self.lock:
            self.events[name] += n

    def summary(self):
        with self.lock:
            every = [ms for values in self.latency.values() for ms in values]
            return {
                "requests": {kind: {**latency_summary(values),
                                    "first10_p50_ms": round(pct(values[:10], 50), 1),
                                    "last10_p50_ms": round(pct(values[-10:], 50), 1),
                                    "wire_bytes": self.wire[kind],
                                    "decoded_bytes": self.decoded[kind]}
                             for kind, values in sorted(self.latency.items())},
                "wire_bytes": sum(self.wire.values()),
                "decoded_bytes": sum(self.decoded.values()),
                "max_request_ms": round(max(every), 1) if every else None,
                "status_counts": dict(self.status),
                "events": dict(self.events),
                "sqlite_tx": latency_summary(self.sqlite_tx_ms),
            }


# ---- HTTP transport ------------------------------------------------------------

class Response:
    def __init__(self, status, headers, body, ms, wire, decoded):
        self.status = status
        self.headers = headers
        self.body = body
        self.ms = ms
        self.wire = wire
        self.decoded = decoded

    def header(self, name):
        return self.headers.get(name.lower())

    @property
    def error(self):
        return self.body.get("error") if isinstance(self.body, dict) else None


class Transport:
    """Keep-alive HTTP/1.1 over ``http.client``; counts bytes on the wire."""

    def __init__(self, base_url, stats, token=None, user=None, accept_gzip=True):
        parts = urlsplit(base_url)
        self.host = parts.hostname
        self.port = parts.port or 80
        self.stats = stats
        self.token = token
        self.user = user
        self.accept_gzip = accept_gzip
        self.conn = None
        # The kind of the request on the wire right now (None when idle), so a
        # harness can tell whether a server kill cut a request off.
        self.inflight_kind = None

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def request(self, method, path, *, query=None, body=None, timeout=30.0, kind="other"):
        url = PREFIX + path + ("?" + urlencode(query) if query else "")
        headers = {"Accept": "application/json",
                   "Accept-Encoding": "gzip" if self.accept_gzip else "identity"}
        data = None
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode()
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.user:
            headers["X-E2E-User"] = self.user
        started = time.perf_counter()
        self.inflight_kind = kind
        try:
            for attempt in (0, 1):
                reused = self.conn is not None
                try:
                    if self.conn is None:
                        self.conn = http.client.HTTPConnection(self.host, self.port,
                                                               timeout=timeout)
                    self.conn.timeout = timeout
                    if self.conn.sock is not None:
                        self.conn.sock.settimeout(timeout)
                    self.conn.request(method, url, body=data, headers=headers)
                    resp = self.conn.getresponse()
                    raw = resp.read()
                    break
                except (http.client.RemoteDisconnected, BrokenPipeError,
                        ConnectionResetError) as exc:
                    self.close()
                    if reused and attempt == 0:
                        continue  # the server closed an idle kept-alive socket
                    self._failed(kind, started, exc)
                except (OSError, http.client.HTTPException) as exc:
                    self.close()
                    self._failed(kind, started, exc)
        finally:
            self.inflight_kind = None
        ms = (time.perf_counter() - started) * 1000
        header_list = resp.getheaders()
        head_bytes = 17 + sum(len(k) + len(v) + 4 for k, v in header_list) + 2
        hdrs = {k.lower(): v for k, v in header_list}
        if resp.will_close:
            self.close()
        payload = gzip.decompress(raw) if hdrs.get("content-encoding") == "gzip" else raw
        self.stats.record(kind, resp.status, ms, len(raw) + head_bytes, len(payload))
        content_type = hdrs.get("content-type", "")
        if 300 <= resp.status < 400 or (content_type.startswith("text/html")):
            parsed = {"error": "authentication_required" if resp.status < 400 else "html_error"}
        else:
            try:
                parsed = json.loads(payload) if payload else None
            except ValueError:
                parsed = {"error": "invalid_json"}
        return Response(resp.status, hdrs, parsed, ms, len(raw) + head_bytes, len(payload))

    def _failed(self, kind, started, exc):
        ms = (time.perf_counter() - started) * 1000
        self.stats.record(kind, "conn_error", ms)
        raise TransportError(f"{type(exc).__name__}: {exc}") from None


# ---- local store ("phone") -------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS staged_profiles (track_id TEXT PRIMARY KEY, media_revision TEXT,
                                            payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS published_profiles (track_id TEXT PRIMARY KEY, media_revision TEXT,
                                               payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS edges (track_id TEXT PRIMARY KEY, media_revision TEXT NOT NULL,
                                  profile_digest TEXT NOT NULL, payload BLOB NOT NULL);
"""


class Store:
    def __init__(self, path, stats):
        self.path = path
        self.stats = stats
        self.db = sqlite3.connect(path, isolation_level=None, timeout=60)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        self.counters = collections.Counter()
        self.invalid = collections.Counter()
        self.misses = set()  # K6 references whose edge is not held locally

    def close(self):
        self.db.close()

    def load_state(self):
        row = self.db.execute("SELECT value FROM meta WHERE key='state'").fetchone()
        return json.loads(row[0]) if row else {}

    def transaction(self, state, work):
        """Run ``work(cur)`` and persist ``state`` in one SQLite transaction."""
        started = time.perf_counter()
        cur = self.db.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            result = work(cur) if work else None
            cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('state', ?)",
                        (json.dumps(state, sort_keys=True),))
            cur.execute("COMMIT")
        except BaseException:
            cur.execute("ROLLBACK")
            raise
        ms = (time.perf_counter() - started) * 1000
        with self.stats.lock:
            self.stats.sqlite_tx_ms.append(ms)
        return result

    def apply_upsert(self, cur, table, payload):
        track_id = str(payload["track_id"])
        revision = payload.get("media_revision")
        edge = payload.get("edge_profile")
        ref = payload.get("edge_profile_ref")
        wave = {key: value for key, value in payload.items()
                if key not in ("edge_profile", "edge_profile_ref")}
        cur.execute(f"INSERT OR REPLACE INTO {table} (track_id, media_revision, payload) "
                    "VALUES (?, ?, ?)", (track_id, revision, canonical_json(wave)))
        if edge is None and isinstance(ref, dict):
            # K6 (opt-in, P3-2): the edge is unchanged; keep the local copy when
            # its revision and digest match, otherwise fetch it by id.
            if ref.get("media_revision") == revision and cur.execute(
                    "SELECT 1 FROM edges WHERE track_id=? AND media_revision=? "
                    "AND profile_digest=?",
                    (track_id, revision, ref.get("profile_digest"))).fetchone():
                self.counters["edge_refs_kept"] += 1
                return
            self.counters["edge_refs_missed"] += 1
            self.misses.add(track_id)
        if edge is not None:
            reason = verify_edge(edge, track_id, revision)
            if reason is None:
                cur.execute("INSERT OR REPLACE INTO edges VALUES (?, ?, ?, ?)",
                            (track_id, revision, edge["profile_digest"],
                             zlib.compress(canonical_json(edge).encode("utf-8"), 1)))
                self.counters["edges_stored"] += 1
                return
            self.counters["edges_invalid"] += 1
            self.invalid[reason] += 1
        # Contract §6 rule 1: an upsert without a valid edge deletes the local edge.
        cur.execute("DELETE FROM edges WHERE track_id=?", (track_id,))
        self.counters["upserts_without_edge"] += 1

    def apply_delete(self, cur, table, track_id):
        cur.execute(f"DELETE FROM {table} WHERE track_id=?", (str(track_id),))
        cur.execute("DELETE FROM edges WHERE track_id=?", (str(track_id),))
        self.counters["deletes"] += 1

    def apply_change(self, cur, table, change):
        if change["operation"] == "upsert" and isinstance(change.get("payload"), dict):
            self.apply_upsert(cur, table, change["payload"])
        else:
            self.apply_delete(cur, table, change["track_id"])

    @staticmethod
    def publish(cur):
        """Swap the staged waveform generation in; drop edges it does not own."""
        cur.execute("DELETE FROM published_profiles")
        cur.execute("INSERT INTO published_profiles SELECT track_id, media_revision, payload "
                    "FROM staged_profiles")
        cur.execute("DELETE FROM staged_profiles")
        cur.execute("""DELETE FROM edges WHERE NOT EXISTS (
                           SELECT 1 FROM published_profiles p
                            WHERE p.track_id=edges.track_id
                              AND p.media_revision=edges.media_revision)""")

    def digest(self, per_track=False):
        """Canonical digest of the published dataset (waveform + matching edge)."""
        hasher = hashlib.sha256()
        count = edges = 0
        tracks = {} if per_track else None
        rows = self.db.execute(
            """SELECT p.track_id, p.payload, e.payload FROM published_profiles p
                 LEFT JOIN edges e ON e.track_id=p.track_id AND e.media_revision=p.media_revision
                ORDER BY p.track_id""")
        for track_id, payload, edge in rows:
            value = json.loads(payload)
            if edge is not None:
                value["edge_profile"] = json.loads(zlib.decompress(edge))
                edges += 1
            line = f"{track_id}\t{canonical_json(value)}\n".encode("utf-8")
            hasher.update(line)
            if tracks is not None:
                tracks[track_id] = hashlib.sha256(line).hexdigest()
            count += 1
        return {"profiles": count, "edges": edges, "sha256": hasher.hexdigest(),
                **({"tracks": tracks} if tracks is not None else {})}


# ---- sync client ------------------------------------------------------------------

class SyncClient:
    """Resumable profile sync against one plugin (one device)."""

    def __init__(self, base_url, db_path, *, mode="auto", page_size=50, legacy_limit=50,
                 changes_limit=100, server_id=None, catalog_instance_id=None, token=None,
                 user=None, hook=None, stats=None, create_timeout=10.0, page_timeout=60.0,
                 max_outage_s=600.0, log=None):
        self.stats = stats or Stats()
        self.transport = Transport(base_url, self.stats, token=token, user=user)
        self.store = Store(db_path, self.stats)
        self.mode = mode
        self.page_size = page_size
        self.legacy_limit = legacy_limit
        self.changes_limit = changes_limit
        self.server_id = server_id
        self.catalog_instance_id = catalog_instance_id
        self.hook = hook or (lambda *_args, **_kwargs: None)
        self.create_timeout = create_timeout
        self.page_timeout = page_timeout
        self.max_outage_s = max_outage_s
        self.log = log or (lambda message: None)
        self.state = self.store.load_state()
        self.timings = {}
        self.expires_at = []
        self.phase_started = time.perf_counter()

    def close(self):
        self.transport.close()
        self.store.close()

    def abandon(self):
        """Best-effort release of a v2 session this device still holds, then close.

        What the app does before it drops local v2 state (hand-off C-3 item 5);
        the harness calls it when a scenario fails, so no session leaks.
        """
        token = (self.state.get("session") or {}).get("token")
        if token and self.state.get("catalog_instance_id"):
            try:
                self.transport.request("POST", "/api/profiles/bootstrap/sessions/release",
                                       body=self.envelope(session_token=token), timeout=5,
                                       kind="v2_release")
            except TransportError:
                pass
        self.close()

    # -- HTTP with the client's retry policy (C-3) --------------------------
    def call(self, kind, method, path, *, body=None, query=None, timeout=None):
        timeout = timeout or self.page_timeout
        outage_started = None
        attempt = 0
        while True:
            try:
                resp = self.transport.request(method, path, query=query, body=body,
                                              timeout=timeout, kind=kind)
            except TransportError as exc:
                attempt += 1
                now = time.monotonic()
                outage_started = outage_started or now
                self.stats.count("connection_retries")
                if now - outage_started > self.max_outage_s:
                    raise SyncFailed(f"{kind}: server unreachable for {self.max_outage_s:.0f}s ({exc})")
                time.sleep(min(5.0, 0.2 * 2 ** min(attempt, 5)) * (0.75 + random.random() / 2))
                continue
            if resp.status in (429, 503):
                self.stats.count(f"deferred_{resp.status}")
                try:
                    wait = int(resp.header("Retry-After"))
                except (TypeError, ValueError):
                    wait = None
                if wait is None:
                    self.stats.count(f"missing_retry_after_{resp.status}")
                    wait = min(30, 2 ** min(attempt, 5))
                attempt += 1
                time.sleep(min(RETRY_AFTER_CAP_S, max(1, wait)))
                continue
            if resp.status in (502, 504):
                attempt += 1
                time.sleep(min(5.0, 0.5 * attempt))
                continue
            return resp

    def save(self, work=None):
        return self.store.transaction(self.state, work)

    # -- entry point ---------------------------------------------------------
    def run(self):
        started = time.perf_counter()
        cpu_started = time.process_time()
        self.state.setdefault("counters", {})
        if not self.state.get("catalog_instance_id") or not self.state.get("mode"):
            self.detect()
        self.catalog_instance_id = self.state["catalog_instance_id"]
        while True:
            phase = self.state.get("phase")
            if phase in (None, "create"):
                self.v2_create() if self.state["mode"] == "v2" else self.legacy_start()
            elif phase == "snapshot":
                self.v2_snapshot_page()
            elif phase == "catchup":
                self.v2_catchup_page()
            elif phase == "release":
                self.v2_release()
            elif phase == "legacy":
                self.legacy_page()
            elif phase in ("deltas", "current"):
                if self.delta_page():
                    break
            else:
                raise SyncFailed(f"unknown phase {phase}")
        if self.state.get("release_pending"):
            self.v2_release(after_deltas=True)
        self.timings["total_s"] = round(time.perf_counter() - started, 2)
        self.timings["client_cpu_s"] = round(time.process_time() - cpu_started, 2)
        return self.result()

    def bump(self, name, n=1):
        counters = self.state.setdefault("counters", {})
        counters[name] = counters.get(name, 0) + n

    def result(self):
        return {"mode": self.state.get("mode"), "phase": self.state.get("phase"),
                "catalog_instance_id": self.state.get("catalog_instance_id"),
                "counters": self.state.get("counters", {}),
                "store": dict(self.store.counters),
                "invalid_edges": dict(self.store.invalid),
                "timings": self.timings, "expires_at_seen": self.expires_at[-3:],
                "snapshot_count": self.state.get("snapshot_count"),
                "stats": self.stats.summary()}

    def detect(self):
        health = self.call("health", "GET", "/api/health", timeout=10)
        if health.status != 200 or not isinstance(health.body, dict):
            raise SyncFailed(f"health failed: {health.status} {health.body}")
        caps = health.body.get("capabilities") or {}
        pb = caps.get("profile_bootstrap") or {}
        v2_ok = (health.body.get("status") == "ok" and pb.get("available") is True
                 and pb.get("protocol_version") == 2 and pb.get("schema_version") == 1
                 and pb.get("auth") == "host_authenticated"
                 and pb.get("transfer_contract") == "source_scoped_v1")
        mode = self.mode if self.mode != "auto" else ("v2" if v2_ok else "legacy")
        if mode == "v2" and not v2_ok:
            raise SyncFailed(f"v2 requested but not advertised: {pb}")
        source = self.catalog_instance_id
        if not source:
            catalog = self.call("catalog_health", "GET", "/api/catalog/health", timeout=30)
            servers = (catalog.body or {}).get("servers") or []
            chosen = [s for s in servers if s.get("catalog_instance_id")
                      and (not self.server_id or s.get("server_id") == self.server_id)]
            if not chosen:
                raise SyncFailed(f"no catalogue source in /api/catalog/health: {servers}")
            source = chosen[0]["catalog_instance_id"]
        self.state.update({
            "mode": mode, "catalog_instance_id": source, "phase": "create",
            "sliding": pb.get("sliding_expiry") is True,
            "idempotent": pb.get("idempotent_create") is True,
            "gzip": (caps.get("transport") or {}).get("gzip") is True,
            "edge_refs": (caps.get("profile_stream") or {}).get("edge_refs") is True,
        })
        self.save()

    # -- v2 -------------------------------------------------------------------
    def envelope(self, **extra):
        return {**ENVELOPE, "catalog_instance_id": self.state["catalog_instance_id"], **extra}

    def v2_create(self):
        if not self.state.get("client_request_id"):
            # One UUID per logical create, persisted before sending, so a retry
            # after a lost response replaces the unclaimed session (K5).
            self.state["client_request_id"] = str(uuid.uuid4())
            self.save()
        body = self.envelope(page_size=self.page_size)
        if self.state.get("sliding"):
            body["expiry_mode"] = "sliding"
        if self.state.get("idempotent"):
            body["client_request_id"] = self.state["client_request_id"]
        t0 = time.perf_counter()
        self.hook("create_sending", client=self)
        while True:
            try:
                resp = self.transport.request("POST", "/api/profiles/bootstrap/sessions",
                                              body=body, timeout=self.create_timeout,
                                              kind="v2_create")
            except TransportError:
                # Timed out or the server died: retry with the same id (K5).
                self.stats.count("create_retries")
                if time.perf_counter() - t0 > self.max_outage_s:
                    raise SyncFailed("v2 create: server unreachable") from None
                time.sleep(1.0)
                continue
            if resp.status in (429, 503):
                self.stats.count(f"deferred_{resp.status}")
                try:
                    wait = int(resp.header("Retry-After"))
                except (TypeError, ValueError):
                    wait = 5
                    self.stats.count(f"missing_retry_after_{resp.status}")
                time.sleep(min(RETRY_AFTER_CAP_S, max(1, wait)))
                continue
            break
        if resp.status == 413:
            self.log("v2 create 413: falling back to legacy for this run")
            self.bump("v2_413_fallbacks")
            self.state.update({"mode": "legacy", "phase": "create", "client_request_id": None})
            self.save()
            return
        if resp.status != 200:
            raise SyncFailed(f"v2 create failed: {resp.status} {resp.body}")
        created = resp.body
        self.bump("creates")
        self.timings["create_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        self.state.update({
            "phase": "snapshot",
            "session": {"token": created["session_token"], "page_size": created["page_size"],
                        "snapshot_count": created["snapshot_count"],
                        "expires_at": created["expires_at"],
                        **{key: created[key] for key in SNAPSHOT_META}},
            "snapshot_count": created["snapshot_count"],
            "snapshot_token": created["next_page_token"], "snapshot_pages": 0,
            "snapshot_rows": 0, "catchup_token": None, "catchup_pages": 0,
            "catchup_events": 0, "last_seq": created["snapshot_seq"],
        })
        self.expires_at.append(created["expires_at"])

        def clear(cur):
            cur.execute("DELETE FROM staged_profiles")
        self.save(clear)
        self.phase_started = time.perf_counter()
        self.hook("after_create", client=self)

    def v2_session_gone(self, resp, where):
        """410 (or a 400 while paging): the session is gone; start a new one."""
        self.log(f"v2 {where}: {resp.status} {resp.error}; restarting the v2 bootstrap")
        self.bump("v2_restarts")
        token = (self.state.get("session") or {}).get("token")
        if token:
            try:
                self.transport.request("POST", "/api/profiles/bootstrap/sessions/release",
                                       body=self.envelope(session_token=token), timeout=5,
                                       kind="v2_release")
            except TransportError:
                pass
        self.state.update({"phase": "create", "session": None, "client_request_id": None})
        self.save(lambda cur: cur.execute("DELETE FROM staged_profiles"))

    def check_metadata(self, body):
        session = self.state["session"]
        for key in SNAPSHOT_META:
            if body.get(key) != session[key]:
                raise SyncFailed(f"v2 metadata {key} changed: {body.get(key)} != {session[key]}")
        if body.get("catalog_instance_id") != self.state["catalog_instance_id"]:
            raise SyncFailed("v2 page for another source")
        if body.get("expires_at") != session["expires_at"]:
            if not self.state.get("sliding"):
                raise SyncFailed("absolute session changed expires_at")
            if body["expires_at"] < session["expires_at"]:
                raise SyncFailed("sliding expires_at moved backwards")
            session["expires_at"] = body["expires_at"]
            self.expires_at.append(body["expires_at"])
            self.bump("expiry_extensions")

    def v2_snapshot_page(self):
        session = self.state["session"]
        body = self.envelope(session_token=session["token"])
        if self.state["snapshot_pages"]:
            body["page_token"] = self.state["snapshot_token"]
        resp = self.call("v2_page", "POST", "/api/profiles/bootstrap/sessions/page", body=body)
        if resp.status in (400, 410):
            return self.v2_session_gone(resp, "page")
        if resp.status != 200:
            raise SyncFailed(f"v2 page failed: {resp.status} {resp.body}")
        page = resp.body
        self.check_metadata(page)
        profiles = page["profiles"]

        def stage(cur):
            for profile in profiles:
                self.store.apply_upsert(cur, "staged_profiles", profile)
        self.state["snapshot_pages"] += 1
        self.state["snapshot_rows"] += len(profiles)
        self.state["snapshot_token"] = page["next_page_token"]
        done = not page["has_more"]
        if done:
            if self.state["snapshot_rows"] != session["snapshot_count"]:
                raise SyncFailed(f"snapshot rows {self.state['snapshot_rows']} != "
                                 f"snapshot_count {session['snapshot_count']}")
            self.state["phase"] = "catchup"
        self.bump("snapshot_pages_fetched")
        self.save(stage)
        self.hook("snapshot_page", client=self, index=self.state["snapshot_pages"],
                  total=math.ceil(session["snapshot_count"] / session["page_size"]))
        if done:
            self.timings["snapshot_s"] = round(time.perf_counter() - self.phase_started, 2)
            self.phase_started = time.perf_counter()
            self.hook("snapshot_end", client=self)

    def v2_catchup_page(self):
        session = self.state["session"]
        body = self.envelope(session_token=session["token"])
        if self.state["catchup_pages"]:
            body["page_token"] = self.state["catchup_token"]
        timeout = 600.0 if not self.state["catchup_pages"] else self.page_timeout
        self.hook("catchup_sending", client=self, index=self.state["catchup_pages"])
        resp = self.call("v2_catchup", "POST", "/api/profiles/bootstrap/sessions/catchup",
                         body=body, timeout=timeout)
        if resp.status == 413:
            self.log("v2 catch-up 413: falling back to legacy for this run")
            self.bump("v2_413_fallbacks")
            self.state.update({"mode": "legacy", "phase": "create"})
            return self.save(lambda cur: cur.execute("DELETE FROM staged_profiles"))
        if resp.status in (400, 410):
            return self.v2_session_gone(resp, "catch-up")
        if resp.status != 200:
            raise SyncFailed(f"v2 catch-up failed: {resp.status} {resp.body}")
        page = resp.body
        self.check_metadata(page)
        changes = page["changes"]
        for change in changes:
            if change["seq"] != self.state["last_seq"] + 1:
                raise SyncFailed(f"catch-up seq gap {self.state['last_seq']} -> {change['seq']}")
            self.state["last_seq"] = change["seq"]
        done = not page["has_more"]

        def apply(cur):
            for change in changes:
                self.store.apply_change(cur, "staged_profiles", change)
            if done:
                t0 = time.perf_counter()
                Store.publish(cur)
                self.timings["publish_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        self.state["catchup_pages"] += 1
        self.state["catchup_events"] += len(changes)
        self.state["catchup_token"] = page["next_page_token"]
        if done:
            if page["cursor"] != page["head_cursor"]:
                raise SyncFailed("final catch-up cursor differs from head_cursor")
            self.state.update({"phase": "release", "cursor": page["head_cursor"],
                               "published": True})
        self.bump("catchup_pages_fetched")
        self.save(apply)
        self.hook("catchup_page", client=self, index=self.state["catchup_pages"])
        if done:
            self.timings["catchup_s"] = round(time.perf_counter() - self.phase_started, 2)
            self.hook("catchup_end", client=self)

    def v2_release(self, after_deltas=False):
        if not after_deltas:
            self.hook("before_release", client=self)
        token = (self.state.get("session") or {}).get("token")
        ok = False
        if token:
            try:
                resp = self.transport.request(
                    "POST", "/api/profiles/bootstrap/sessions/release",
                    body=self.envelope(session_token=token), timeout=10, kind="v2_release")
                ok = resp.status == 200 and (resp.body or {}).get("released") is True
            except TransportError:
                ok = False
        if ok or not token:
            self.state["release_pending"] = False
            if ok:
                self.state["session"] = None
                self.bump("releases")
        else:
            # C-3: a pending release retries later and never skips the delta.
            self.state["release_pending"] = True
            self.bump("release_deferred")
        if not after_deltas:
            self.state["phase"] = "deltas"
            self.phase_started = time.perf_counter()
        self.save()

    # -- legacy ----------------------------------------------------------------
    def legacy_start(self):
        self.state.update({"mode": "legacy", "phase": "legacy", "legacy_token": None,
                           "legacy_pages": 0, "legacy_cursor": None})
        self.save(lambda cur: cur.execute("DELETE FROM staged_profiles"))
        self.phase_started = time.perf_counter()

    def legacy_page(self):
        query = {"catalog_instance_id": self.state["catalog_instance_id"],
                 "limit": self.legacy_limit}
        if self.state.get("legacy_token"):
            query["page_token"] = self.state["legacy_token"]
        resp = self.call("legacy_page", "GET", "/api/profiles/bootstrap", query=query)
        if resp.status == 410:
            self.bump("legacy_restarts")
            return self.legacy_start()
        if resp.status != 200:
            raise SyncFailed(f"legacy bootstrap failed: {resp.status} {resp.body}")
        page = resp.body
        if self.state.get("legacy_cursor") and page["cursor"] != self.state["legacy_cursor"]:
            raise SyncFailed("legacy pinned cursor changed between pages")
        done = not page["has_more"]
        profiles = page["profiles"]

        def stage(cur):
            for profile in profiles:
                self.store.apply_upsert(cur, "staged_profiles", profile)
            if done:
                t0 = time.perf_counter()
                Store.publish(cur)
                self.timings["publish_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        self.state["legacy_cursor"] = page["cursor"]
        self.state["legacy_token"] = page["next_page_token"]
        self.state["legacy_pages"] += 1
        if done:
            self.state.update({"phase": "deltas", "cursor": page["cursor"], "published": True})
        self.bump("legacy_pages_fetched")
        self.save(stage)
        self.hook("legacy_page", client=self, index=self.state["legacy_pages"])
        if done:
            self.timings["legacy_bootstrap_s"] = round(time.perf_counter() - self.phase_started, 2)
            self.phase_started = time.perf_counter()

    def fetch_missing_edges(self):
        """K6: fetch edges the device does not hold through /api/profiles (<= 100 ids)."""
        while self.store.misses:
            batch = sorted(self.store.misses)[:100]
            resp = self.call("profiles_fetch", "GET", "/api/profiles",
                             query={"catalog_instance_id": self.state["catalog_instance_id"],
                                    "ids": ",".join(batch)})
            if resp.status != 200:
                raise SyncFailed(f"/api/profiles failed: {resp.status} {resp.body}")
            profiles = resp.body.get("profiles") or []

            def apply(cur):
                for profile in profiles:
                    self.store.apply_upsert(cur, "published_profiles", profile)
            self.save(apply)
            self.bump("edge_fetches", len(batch))
            self.store.misses.difference_update(batch)

    # -- incremental /profiles/changes -----------------------------------------
    def delta_page(self):
        """Apply one /changes page; True when the stream is current."""
        query = {"cursor": self.state["cursor"],
                 "catalog_instance_id": self.state["catalog_instance_id"],
                 "limit": self.changes_limit}
        if self.state.get("edge_refs"):
            query["edge_refs"] = 1
        resp = self.call("changes", "GET", "/api/profiles/changes", query=query)
        if resp.status in (410, 400):
            # 410 bootstrap_required, or 400 cursor ahead of head: resync (rule 6).
            self.log(f"/changes {resp.status} {resp.error}: full resync")
            self.bump("resyncs")
            self.state.update({"phase": "create", "session": None, "client_request_id": None})
            self.save()
            return False
        if resp.status != 200:
            raise SyncFailed(f"/changes failed: {resp.status} {resp.body}")
        page = resp.body
        changes = page["changes"]

        def apply(cur):
            for change in changes:
                self.store.apply_change(cur, "published_profiles", change)
        self.state["cursor"] = page["cursor"]
        done = not page["has_more"]
        self.state["phase"] = "current" if done else "deltas"
        if changes:
            self.bump("delta_pages_fetched")
            self.bump("delta_events", len(changes))
        self.save(apply)
        self.fetch_missing_edges()
        self.hook("delta_page", client=self, index=self.state.get("counters", {}).get(
            "delta_pages_fetched", 0), events=len(changes))
        if done:
            self.timings["deltas_s"] = round(time.perf_counter() - self.phase_started, 2)
        return done


# ---- route poller (health and settings during load) ---------------------------------

POLL_ROUTES = ("/api/health", "/api/catalog/health", "/settings/status")


def poll_routes(base_url, stop, results, interval=0.5, token=None):
    """Poll the status routes until ``stop`` is set; put a summary on ``results``."""
    stats = Stats()
    transport = Transport(base_url, stats, token=token)
    samples = collections.defaultdict(list)
    errors = collections.Counter()
    index = 0
    while not stop.is_set():
        route = POLL_ROUTES[index % len(POLL_ROUTES)]
        index += 1
        try:
            resp = transport.request("GET", route, timeout=30, kind=route)
            if resp.status == 200:
                samples[route].append(resp.ms)
            else:
                errors[f"{route} {resp.status}"] += 1
        except TransportError:
            errors[f"{route} conn_error"] += 1
        stop.wait(interval)
    transport.close()
    results.put({"routes": {route: latency_summary(values) for route, values in samples.items()},
                 "errors": dict(errors)})


# ---- CLI --------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sync = sub.add_parser("sync", help="run (or resume) one device sync")
    sync.add_argument("--base-url", required=True)
    sync.add_argument("--db", required=True, help="SQLite file (the simulated phone)")
    sync.add_argument("--mode", choices=("auto", "v2", "legacy"), default="auto")
    sync.add_argument("--server-id", default=None)
    sync.add_argument("--catalog-instance-id", default=None)
    sync.add_argument("--page-size", type=int, default=50)
    sync.add_argument("--token", default=os.environ.get("LUMAE_E2E_AUTH_TOKEN"))
    sync.add_argument("--user", default=None, help="X-E2E-User (harness host only)")
    sync.add_argument("--verify-digest", action="store_true",
                      help="print the local dataset digest with the result")
    digest = sub.add_parser("digest", help="print the local dataset digest")
    digest.add_argument("--db", required=True)
    args = parser.parse_args(argv)
    if args.command == "digest":
        store = Store(args.db, Stats())
        print(json.dumps(store.digest()))
        return 0
    client = SyncClient(args.base_url, args.db, mode=args.mode, page_size=args.page_size,
                        server_id=args.server_id, catalog_instance_id=args.catalog_instance_id,
                        token=args.token, user=args.user,
                        log=lambda message: print(message, file=sys.stderr, flush=True))
    try:
        result = client.run()
        if args.verify_digest:
            result["digest"] = client.store.digest()
    finally:
        client.close()
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
