"""P2-6 representative-scale end-to-end gate: the server matrix runner.

Serves the real plugin (``host_app.py``) under real gunicorn with the stock
AudioMuse topology (``--worker-class gthread --workers 1 --threads 4``) against
a seeded PostgreSQL database (``seed_representative.py``), or targets the stock
AudioMuse host from ``docker-compose.yml`` (``--host docker``), and runs the
gate scenarios with the protocol-faithful client in ``client_sim.py``.

Every scenario records wall time, bytes on the wire, peak server RSS, the
longest request, per-page p95 and the 429/503 counts, asserts its criteria,
and ends with PASS, FAIL or PENDING (only a criterion whose server work has not
been built yet, e.g. K6/P3-2, failed), or ERROR when it raised. No check is ever
skipped silently: a scenario that needs a synced device loads one itself.
Results go to ``--out`` as JSON.

Usage::

    export LUMAE_E2E_DSN=postgresql://lumae_test@127.0.0.1:55432/e2e_rep
    python3 scripts/e2e/seed_representative.py --reset             # once, ~4 min at scale 1
    python3 scripts/e2e/run_server_matrix.py --out e2e.json        # all scenarios
    python3 scripts/e2e/run_server_matrix.py --scenarios first_load_v2,kill_restart

Scenario names: idle_routes, first_load_v2, first_load_legacy, reanalysis_noop,
concurrent_devices, creators, kill_restart, lum005_k6, capped_catchup.
"""
import argparse
import collections
import contextlib
import json
import math
import multiprocessing
import os
import platform
import random
import signal
import subprocess
import sys
import threading
import time
import types
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
PERF = os.path.abspath(os.path.join(HERE, "..", "perf"))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
for path in (HERE, PERF):
    if path not in sys.path:
        sys.path.insert(0, path)

import client_sim  # noqa: E402
from client_sim import SimulatedCrash, SyncClient, Stats, canonical_json  # noqa: E402

PREFIX = client_sim.PREFIX
ENVELOPE = client_sim.ENVELOPE
BUDGET_HEALTH_MS = 50.0
BUDGET_SETTINGS_MS = 100.0
BUDGET_CREATE_S = 5.0
BUDGET_K6_BYTES_PER_TRACK = 1024


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


# ---- servers -----------------------------------------------------------------

def _proc_rss_kb(pid):
    try:
        with open(f"/proc/{pid}/status") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


def _children(pid):
    found = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as handle:
                fields = handle.read().rsplit(")", 1)[1].split()
            if int(fields[1]) == pid:
                found.append(int(entry))
        except (OSError, IndexError, ValueError):
            continue
    return found


def _wait_http(url, timeout):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    return True
        except Exception as exc:  # noqa: BLE001 - any failure means "not yet"
            last = exc
        time.sleep(0.2)
    raise RuntimeError(f"server not ready at {url}: {last}")


class LocalServer:
    """Real gunicorn (gthread, 1 worker x 4 threads) serving ``host_app``."""

    kind = "local"

    def __init__(self, dsn, port, scratch, *, servers="server-a", auth_token=None):
        self.dsn = dsn
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        self.log_path = os.path.join(scratch, f"gunicorn_{port}.log")
        self.env = {**os.environ, "LUMAE_E2E_DSN": dsn, "LUMAE_PERF_DSN": dsn,
                    "LUMAE_E2E_SERVERS": servers, "LUMAE_E2E_TRUST_USER_HEADER": "1"}
        if auth_token:
            self.env["LUMAE_E2E_AUTH_TOKEN"] = auth_token
        self.proc = None
        self.restarts = 0

    def command(self):
        return [sys.executable, "-m", "gunicorn", "--chdir", HERE,
                "--bind", f"127.0.0.1:{self.port}", "--worker-class", "gthread",
                "--workers", "1", "--threads", "4", "--keep-alive", "5", "--timeout", "300",
                # gunicorn >= 25 opens ~/.gunicorn/gunicorn.ctl by default; several
                # runners on one machine would share it.
                "--no-control-socket",
                "--error-logfile", self.log_path, "host_app:app"]

    def start(self):
        out = open(self.log_path, "a")
        self.proc = subprocess.Popen(self.command(), env=self.env, stdout=out, stderr=out,
                                     start_new_session=True, cwd=HERE)
        out.close()
        _wait_http(self.base_url + PREFIX + "/api/health", 120)

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def kill9(self):
        if self.proc is None:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.proc.pid, signal.SIGKILL)
        self.proc.wait()

    def restart(self, between=None):
        """kill -9, run ``between()`` while the server is down, start again."""
        self.kill9()
        self.restarts += 1
        if between is not None:
            between()
        self.start()

    def stop(self):
        if not self.alive():
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.proc.pid, signal.SIGTERM)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.kill9()

    def pids(self):
        if not self.alive():
            return []
        return [self.proc.pid] + _children(self.proc.pid)

    def describe(self):
        import gunicorn

        return {"kind": "local", "app": "scripts/e2e/host_app.py (plugin blueprint, stub host)",
                "gunicorn": gunicorn.__version__, "topology": "gthread, 1 worker x 4 threads",
                "command": " ".join(self.command()[1:])}


class DockerServer:
    """The stock AudioMuse host from ``docker-compose.yml`` (supervisord + gunicorn)."""

    kind = "docker"

    def __init__(self, compose_files, base_url, *, service="audiomuse-ai-flask",
                 project="lumae-e2e"):
        self.compose = ["docker", "compose", "-p", project]
        for path in compose_files:
            self.compose += ["-f", path]
        self.service = service
        self.base_url = base_url
        self.restarts = 0

    def _run(self, *args, check=True):
        return subprocess.run(self.compose + list(args), check=check, capture_output=True,
                              text=True, cwd=HERE)

    def start(self):
        self._run("up", "-d", self.service)
        _wait_http(self.base_url + PREFIX + "/api/health", 600)

    def gunicorn_pids(self):
        pids = []
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/cmdline", "rb") as handle:
                    cmd = handle.read().replace(b"\0", b" ")
            except OSError:
                continue
            if b"gunicorn" in cmd and b"app:app" in cmd and b"host_app" not in cmd:
                pids.append(int(entry))
        return pids

    def kill9(self):
        # kill -9 of the gunicorn master and worker; supervisord restarts it.
        for pid in self.gunicorn_pids():
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)

    def restart(self, between=None):
        old = set(self.gunicorn_pids())
        self.kill9()
        self.restarts += 1
        if between is not None:
            between()
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            now = set(self.gunicorn_pids())
            if now and not (now & old):
                break
            time.sleep(0.2)
        _wait_http(self.base_url + PREFIX + "/api/health", 600)

    def stop(self):
        self._run("stop", self.service, check=False)

    def alive(self):
        return bool(self.gunicorn_pids())

    def pids(self):
        return self.gunicorn_pids()

    def describe(self):
        info = self._run("images", "--format", "json", check=False).stdout.strip()
        return {"kind": "docker", "compose": " ".join(self.compose[3:]),
                "service": self.service, "images": info[:2000],
                "topology": "supervisord [program:flask]: gunicorn gthread, 1 worker x 4 threads"}


# ---- measurement helpers ------------------------------------------------------

class RssSampler(threading.Thread):
    """Peak RSS of the server processes (sampled every 100 ms)."""

    def __init__(self, server, interval=0.1):
        super().__init__(daemon=True)
        self.server = server
        self.interval = interval
        self.stop_event = threading.Event()
        self.max_total_kb = 0
        self.max_single_kb = 0
        self.samples = 0

    def run(self):
        while not self.stop_event.is_set():
            values = [v for v in (_proc_rss_kb(pid) for pid in self.server.pids()) if v]
            if values:
                self.max_total_kb = max(self.max_total_kb, sum(values))
                self.max_single_kb = max(self.max_single_kb, max(values))
                self.samples += 1
            self.stop_event.wait(self.interval)

    def result(self):
        return {"peak_server_rss_mb": round(self.max_total_kb / 1024, 1),
                "peak_worker_rss_mb": round(self.max_single_kb / 1024, 1),
                "rss_samples": self.samples}


class Measure:
    """Wall time, peak server RSS and (optionally) status-route latency."""

    def __init__(self, ctx, poll=True):
        self.ctx = ctx
        self.poll = poll
        self.result = {}

    def __enter__(self):
        self.sampler = RssSampler(self.ctx.server)
        self.sampler.start()
        self.poller = None
        if self.poll:
            mp = multiprocessing.get_context("spawn")
            self.stop = mp.Event()
            self.queue = mp.Queue()
            self.poller = mp.Process(target=client_sim.poll_routes,
                                     args=(self.ctx.server.base_url, self.stop, self.queue),
                                     kwargs={"interval": self.ctx.args.poll_interval,
                                             "token": self.ctx.args.auth_token})
            self.poller.start()
        self.started = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.result["wall_s"] = round(time.perf_counter() - self.started, 2)
        self.sampler.stop_event.set()
        self.sampler.join()
        self.result.update(self.sampler.result())
        if self.poller is not None:
            self.stop.set()
            try:
                self.result["status_routes"] = self.queue.get(timeout=60)
            except Exception:  # noqa: BLE001
                self.result["status_routes"] = {"error": "poller returned nothing"}
            self.poller.join(timeout=10)
        return False


class Scenario:
    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.assertions = []
        self.metrics = {}
        self.status = None
        self.notes = []

    def check(self, label, ok, detail=None, pending=None):
        """Record an assertion. ``pending`` names the unbuilt work (e.g. "P3-2")
        a criterion waits for: its failure makes the scenario PENDING, not FAIL."""
        self.assertions.append({"check": label, "ok": bool(ok),
                                **({"detail": detail} if detail is not None else {}),
                                **({"pending": pending} if pending else {})})
        if not ok:
            log(f"  {'PENDING' if pending else 'FAIL'} {self.name}: {label} ({detail})")
        return ok

    def finish(self):
        """FAIL if any assertion failed, else PENDING if a pending one did, else PASS."""
        failed = [a for a in self.assertions if not a["ok"]]
        if any(not a.get("pending") for a in failed):
            status = "FAIL"
        elif failed:
            status = "PENDING"
        else:
            status = "PASS"
        self.status = status
        return {"name": self.name, "description": self.description, "status": status,
                "assertions": self.assertions, "metrics": self.metrics, "notes": self.notes}


def route_budget_checks(scenario, routes):
    """Health <=50 ms and settings <=100 ms p95 while the scenario ran."""
    data = (routes or {}).get("routes") or {}
    for route, budget in (("/api/health", BUDGET_HEALTH_MS),
                          ("/api/catalog/health", BUDGET_HEALTH_MS),
                          ("/settings/status", BUDGET_SETTINGS_MS)):
        p95 = (data.get(route) or {}).get("p95_ms")
        scenario.check(f"{route} p95 <= {budget:.0f} ms during load", p95 is not None
                       and p95 <= budget, p95)


def client_metrics(result):
    stats = result["stats"]
    status = stats["status_counts"]
    page_kinds = ("v2_page", "legacy_page", "v2_catchup", "changes")
    return {
        "wire_bytes": stats["wire_bytes"], "decoded_bytes": stats["decoded_bytes"],
        "wire_mb": round(stats["wire_bytes"] / 1e6, 1),
        "decoded_mb": round(stats["decoded_bytes"] / 1e6, 1),
        "max_request_ms": stats["max_request_ms"],
        "page_latency": {kind: {k: stats["requests"][kind][k]
                                for k in ("n", "p50_ms", "p95_ms", "max_ms", "first10_p50_ms",
                                          "last10_p50_ms")}
                         for kind in page_kinds if kind in stats["requests"]},
        "create_ms": (stats["requests"].get("v2_create") or {}).get("max_ms"),
        "http_429": status.get("429", 0), "http_503": status.get("503", 0),
        "conn_errors": status.get("conn_error", 0),
        "sqlite_longest_tx_ms": stats["sqlite_tx"]["max_ms"],
        "timings": result["timings"], "counters": result["counters"],
        "store": result["store"], "invalid_edges": result["invalid_edges"],
    }


# ---- server-side truth and mutations (the plugin's own functions) ------------------

class Plugin:
    """The plugin loaded in this process against the gate database (stub host)."""

    def __init__(self, dsn):
        os.environ["LUMAE_PERF_DSN"] = dsn
        import stub_host

        self.stub = stub_host
        self.mod = stub_host.load_plugin()
        from plugins.LumaeAnalysis import catalog_enrichment, edge_profile_store
        from plugins.LumaeAnalysis import profile_bootstrap, profile_publication
        from plugins.LumaeAnalysis.catalog import JOURNAL_WRITER_GENERATION

        self.enrichment = catalog_enrichment
        self.edges = edge_profile_store
        self.pb = profile_bootstrap
        self.publication = profile_publication
        self.writer_generation = JOURNAL_WRITER_GENERATION
        self.T = stub_host.T
        self.db = stub_host.connect()
        self.rng = random.Random(26)

    def query(self, sql, params=(), one=False):
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else None
        self.db.commit()
        return (rows[0] if rows else None) if one else rows

    def execute(self, sql, params=()):
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            count = cur.rowcount
        self.db.commit()
        return count

    def source(self, server_id="server-a"):
        return self.query(f"SELECT catalog_instance_id FROM {self.T}catalog_sources "
                          "WHERE current_core_server_id=%s", (server_id,), one=True)[0]

    def stream(self, source):
        row = self.query(f"SELECT epoch, head_seq, floor_seq, retention_limit FROM "
                         f"{self.T}profile_stream_state WHERE catalog_instance_id=%s",
                         (source,), one=True)
        return {"epoch": row[0], "head_seq": int(row[1]), "floor_seq": int(row[2]),
                "retention_limit": row[3]}

    def live_sessions(self):
        return self.query(f"SELECT count(*) FROM {self.T}profile_bootstrap_sessions s WHERE "
                          + self.enrichment.live_bootstrap_session_sql("s"), one=True)[0]

    def page_plan(self, token, ordinal=0, page_size=50, analyze=False):
        """EXPLAIN of the v2 snapshot page query for this session (diagnostic).

        Mirrors the SELECT in ``profile_bootstrap.snapshot_page``. Reports whether
        the planner reads the page through the ordered primary key (LIMIT
        applied first) or resolves the edge of every remaining row and sorts.
        Without ``analyze`` the query is only planned, so the measured run is
        not perturbed.
        """
        import hashlib

        row = self.query(f"SELECT session_id, catalog_instance_id FROM "
                         f"{self.T}profile_bootstrap_sessions WHERE token_hash=%s",
                         (hashlib.sha256(token.encode()).hexdigest(),), one=True)
        if row is None:
            return None
        sql = f"""SELECT CASE WHEN edge.payload IS NOT NULL
                        THEN s.payload || jsonb_build_object('edge_profile', edge.payload)
                        ELSE s.payload END
                  FROM {self.pb._table('profile_bootstrap_snapshot')} s
                  {self.pb._edge_lookup('s', 's.payload')}
                 WHERE s.session_id=%s AND s.ordinal>=%s ORDER BY s.ordinal LIMIT %s"""
        plan = self.query(("EXPLAIN (ANALYZE, FORMAT JSON) " if analyze else
                           "EXPLAIN (FORMAT JSON) ") + sql,
                          (row[1], row[0], ordinal, page_size), one=True)[0][0]
        nodes = []

        def walk(node):
            nodes.append(node)
            for child in node.get("Plans", []):
                walk(child)
        walk(plan["Plan"])
        scan = next((n for n in nodes if n.get("Relation Name", "").endswith(
            "profile_bootstrap_snapshot")), {})
        joins = [n for n in nodes if n.get("Node Type") == "Nested Loop"]
        return {"analyzed": analyze,
                **({"execution_ms": round(plan.get("Execution Time", 0), 1)} if analyze else {}),
                "sort_after_join": any(n.get("Node Type") == "Sort" for n in nodes[:3]),
                "snapshot_scan": scan.get("Node Type"),
                "snapshot_rows_estimated": scan.get("Plan Rows"),
                "snapshot_rows_read": scan.get("Actual Rows"),
                "edge_lookups": joins[0].get("Actual Rows") if joins else None}

    def reset_rate_limit(self):
        self.execute(f"DELETE FROM {self.T}profile_bootstrap_creates")

    def wal_lsn(self):
        return self.query("SELECT pg_current_wal_lsn()", one=True)[0]

    def wal_mb_since(self, lsn):
        return round(float(self.query("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)",
                                      (lsn,), one=True)[0]) / 1e6, 1)

    def relation_mb(self, name):
        return round(float(self.query("SELECT pg_total_relation_size(to_regclass(%s))",
                                      (self.T + name,), one=True)[0] or 0) / 1e6, 1)

    def digest(self, source, per_track=False):
        """The server's published dataset in the client's canonical form."""
        import hashlib

        hasher = hashlib.sha256()
        count = edges = 0
        tracks = {} if per_track else None
        cur = self.db.cursor(name=f"digest_{uuid.uuid4().hex}")
        cur.itersize = 500
        cur.execute(
            f"""SELECT p.track_id, p.sample_rate, p.duration_ms, p.ref_lufs, p.start_ramp,
                       p.end_ramp, p.analyzer_ver, p.analyzed_at, p.media_signature, edge.payload
                  FROM {self.T}published_source_profiles p {self.edges.edge_join()}
                 WHERE p.catalog_instance_id=%s ORDER BY p.track_id COLLATE "C" """, (source,))
        for row in cur:
            payload = self.enrichment.serialize_profile(*row[:9], edge_profile=row[9])
            edges += "edge_profile" in payload
            line = f"{row[0]}\t{canonical_json(payload)}\n".encode("utf-8")
            hasher.update(line)
            if tracks is not None:
                tracks[row[0]] = hashlib.sha256(line).hexdigest()
            count += 1
        cur.close()
        self.db.commit()
        return {"profiles": count, "edges": edges, "sha256": hasher.hexdigest(),
                **({"tracks": tracks} if tracks is not None else {})}

    def edge_rows(self, source):
        """Every stored edge row of the source (not only the ones a read serves).

        Returns the row count and a digest of the sorted keys plus
        ``updated_at``, so a deleted, added, duplicated or rewritten edge row
        changes it even when no event is journalled.
        """
        import hashlib

        hasher = hashlib.sha256()
        count = 0
        cur = self.db.cursor(name=f"edges_{uuid.uuid4().hex}")
        cur.itersize = 2000
        cur.execute(
            f"""SELECT track_id, media_revision, representation_id, profile_digest,
                       updated_at::text
                  FROM {self.T}edge_profiles WHERE catalog_instance_id=%s
                 ORDER BY track_id COLLATE "C", media_revision COLLATE "C",
                          representation_id COLLATE "C" """, (source,))
        for row in cur:
            hasher.update(("\t".join(row) + "\n").encode("utf-8"))
            count += 1
        cur.close()
        self.db.commit()
        return {"rows": count, "sha256": hasher.hexdigest()}

    def edge_jobs(self):
        row = self.query(f"SELECT count(*), max(updated_at)::text FROM {self.T}edge_profile_jobs",
                         one=True)
        return {"rows": row[0], "max_updated_at": row[1]}

    def fixture_counts(self, source):
        """Published profiles, and how many of them a read serves with an edge."""
        row = self.query(
            f"""SELECT count(*), count(edge.profile_digest)
                  FROM {self.T}published_source_profiles p
                  {self.edges.edge_join(columns='e.profile_digest')}
                 WHERE p.catalog_instance_id=%s""", (source,), one=True)
        fixture = None
        if self.query("SELECT to_regclass('lumae_perf_fixture') IS NOT NULL", one=True)[0]:
            fixture = self.query("SELECT value FROM lumae_perf_fixture WHERE key='fixture'",
                                 one=True)
        record = fixture[0] if fixture else {}
        seeded = (record.get("counts") or {}).get("profiles")
        seeded_head = seeded + int(record.get("events") or 0) if seeded is not None else None
        return {"profiles": int(row[0]), "edges": int(row[1]), "seeded_profiles": seeded,
                "seeded_head": seeded_head, "head_seq": self.stream(source)["head_seq"]}

    def block_capture(self, table):
        """Hold ACCESS EXCLUSIVE on ``table`` until the returned release() runs.

        A v2 capture that reads (published profiles) or journal-scans
        (``profile_changes``) that table waits inside its capture transaction,
        so a kill lands during the capture deterministically at any scale.
        """
        conn = self.stub.connect()
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '30s'")
                cur.execute(f"LOCK TABLE {self.T}{table} IN ACCESS EXCLUSIVE MODE")
                cur.execute("SELECT pg_backend_pid()")
                pid = cur.fetchone()[0]
        except BaseException:
            # A lock timeout (or anything else) must not leak the connection.
            with contextlib.suppress(Exception):
                conn.close()
            raise

        def blocked_captures():
            with conn.cursor() as cur:
                # pg_stat_activity is snapshotted once per transaction, and this
                # connection stays in the transaction that holds the lock.
                cur.execute("SELECT pg_stat_clear_snapshot()")
                cur.execute("""SELECT count(*) FROM pg_stat_activity
                                WHERE application_name=%s AND %s = ANY(pg_blocking_pids(pid))""",
                            (self.pb.APPLICATION_NAME, pid))
                return cur.fetchone()[0]

        def release():
            with contextlib.suppress(Exception):
                conn.rollback()
            with contextlib.suppress(Exception):
                conn.close()
        return blocked_captures, release

    def published_ids(self, source, limit=None, offset=0, with_edge=None):
        edge_filter = ""
        if with_edge is True:
            edge_filter = " AND edge.payload IS NOT NULL"
        elif with_edge is False:
            edge_filter = " AND edge.payload IS NULL"
        rows = self.query(
            f"""SELECT p.track_id FROM {self.T}published_source_profiles p
                {self.edges.edge_join(columns='e.profile_digest AS payload')}
                 WHERE p.catalog_instance_id=%s {edge_filter}
                 ORDER BY p.track_id OFFSET %s LIMIT %s""",
            (source, offset, limit))
        return [row[0] for row in rows]

    # -- mutations through the real publication path -------------------------
    def _published(self, source, track_id):
        return self.query(
            f"""SELECT sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, media_signature
                  FROM {self.T}published_source_profiles
                 WHERE catalog_instance_id=%s AND track_id=%s""", (source, track_id), one=True)

    def _complete(self, source, track_id, ref_lufs_delta=0.0, signature=None, row=None):
        row = row or self._published(source, track_id)
        tokens = self.publication.admit_attempts(self.db, source, [track_id])
        token = tokens.get(track_id)
        if not token:
            return False
        result = types.SimpleNamespace(
            sample_rate=row[0], duration_ms=row[1], ref_lufs=float(row[2]) + ref_lufs_delta,
            start_ramp_blob=bytes(row[3]), end_ramp_blob=bytes(row[4]))
        return self.publication.complete_attempt(
            self.db, source, track_id, token, result, "ready", None, signature or row[5], 1, 1)

    def republish_waveform(self, source, ids, delta=0.1):
        """Waveform-only change on the same media: an upsert that embeds the edge."""
        return sum(bool(self._complete(source, t, ref_lufs_delta=delta)) for t in ids)

    def noop_complete(self, source, ids):
        """A forced re-analysis whose result is identical (P1-1: no event)."""
        return sum(bool(self._complete(source, t)) for t in ids)

    def change_media(self, source, ids):
        """New media: delete + upsert without an edge (the edge is dropped)."""
        done = 0
        for track_id in ids:
            row = self._published(source, track_id)
            if row is None:
                continue
            fp = self.execute(
                f"""UPDATE {self.T}catalog_tracks t SET media_fp = t.media_fp || '-e2e'
                      FROM {self.T}catalog_state c
                     WHERE c.catalog_instance_id=t.catalog_instance_id
                       AND t.published_generation=c.published_generation
                       AND t.catalog_instance_id=%s AND t.track_id=%s""", (source, track_id))
            if not fp:
                continue
            media = self.query(
                f"""SELECT t.media_fp FROM {self.T}catalog_tracks t JOIN {self.T}catalog_state c
                      ON c.catalog_instance_id=t.catalog_instance_id
                     AND t.published_generation=c.published_generation
                     WHERE t.catalog_instance_id=%s AND t.track_id=%s""",
                (source, track_id), one=True)[0]
            done += bool(self._complete(source, track_id, signature=f"catalog-media:{media}",
                                        row=row))
        return done

    def publish_edges(self, source, ids):
        """Edge publication for tracks without an edge: an upsert carrying it."""
        import seed as perf_seed

        with open(perf_seed.EDGE_TEMPLATE) as handle:
            template = json.load(handle)
        jobs, _ready = self.edges.claim_edge_jobs(self.db, source, ids)
        done = 0
        for job in jobs:
            signature = self.query(
                f"SELECT media_signature FROM {self.T}published_source_profiles "
                "WHERE catalog_instance_id=%s AND track_id=%s", (source, job["track_id"]),
                one=True)[0]
            self.edges.update_edge_job(self.db, source, job, "running")
            generated = next(perf_seed.edge_payloads([(source, job["track_id"], signature)],
                                                     template, self.rng))
            payload = json.loads(generated[5])
            done += bool(self.edges.publish_edge_profile(self.db, source, job, payload, signature))
        return done

    def withdraw(self, source, ids):
        """The track disappears while its analysis runs: a delete event."""
        done = 0
        for track_id in ids:
            row = self._published(source, track_id)
            tokens = self.publication.admit_attempts(self.db, source, [track_id])
            token = tokens.get(track_id)
            if not token or row is None:
                continue
            self.execute(
                f"""UPDATE {self.T}catalog_tracks t SET available=FALSE
                      FROM {self.T}catalog_state c
                     WHERE c.catalog_instance_id=t.catalog_instance_id
                       AND t.published_generation=c.published_generation
                       AND t.catalog_instance_id=%s AND t.track_id=%s""", (source, track_id))
            result = types.SimpleNamespace(sample_rate=row[0], duration_ms=row[1],
                                           ref_lufs=float(row[2]), start_ramp_blob=bytes(row[3]),
                                           end_ramp_blob=bytes(row[4]))
            self.publication.complete_attempt(self.db, source, track_id, token, result, "ready",
                                              None, row[5], 1, 1)
            done += 1
        return done

    def hook_pass(self, source, ids, server_id="server-a"):
        """The AudioMuse re-analysis pass: ``analyze_song_hook`` for every song."""
        statuses = collections.Counter()
        run_id = f"e2e-{uuid.uuid4().hex[:12]}"
        for track_id in ids:
            outcome = self.mod.analyze_song_hook(
                {"item_id": track_id, "server_id": server_id, "run_id": run_id,
                 "audio_path": "/nonexistent/e2e.flac"})
            statuses[outcome.get("status")] += 1
        return dict(statuses)

    def add_second_source(self, source, server_id="server-b"):
        """A second source with the same published rows, for concurrent creates.

        Its edge rows keep the key columns (the capture reads only those) but
        carry a tiny payload, so the second source costs no extra 1.6 GB.
        """
        existing = self.query(f"SELECT catalog_instance_id FROM {self.T}catalog_sources "
                              "WHERE current_core_server_id=%s", (server_id,), one=True)
        if existing:
            return existing[0]
        other = str(uuid.uuid4())
        with self.db.cursor() as cur:
            cur.execute(f"""INSERT INTO {self.T}catalog_sources (catalog_instance_id,
                current_core_server_id, provider_type, server_name, is_default, rebind_status)
                VALUES (%s, %s, 'navidrome', 'E2E second source', FALSE, 'active')""",
                        (other, server_id))
            cur.execute(f"""INSERT INTO {self.T}catalog_state (catalog_instance_id,
                current_core_server_id, provider_type, catalog_epoch) VALUES (%s, %s,
                'navidrome', %s)""", (other, server_id, str(uuid.uuid4())))
            cur.execute(f"""INSERT INTO {self.T}profile_stream_state (catalog_instance_id, epoch,
                head_seq, floor_seq) VALUES (%s, %s, 0, 0)""", (other, str(uuid.uuid4())))
            cur.execute(f"""INSERT INTO {self.T}published_source_profiles (catalog_instance_id,
                track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver,
                profile_schema_ver, media_signature, analyzed_at)
                SELECT %s, track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
                       analyzer_ver, profile_schema_ver, media_signature, analyzed_at
                  FROM {self.T}published_source_profiles WHERE catalog_instance_id=%s""",
                        (other, source))
            cur.execute(f"""INSERT INTO {self.T}edge_profiles (catalog_instance_id, track_id,
                media_revision, representation_id, media_signature, profile_digest, payload)
                SELECT %s, track_id, media_revision, representation_id, media_signature,
                       profile_digest, '{{"e2e": "key columns only"}}'::jsonb
                  FROM {self.T}edge_profiles WHERE catalog_instance_id=%s""", (other, source))
        self.db.commit()
        return other

    def drop_source(self, source):
        with self.db.cursor() as cur:
            for name in ("profile_bootstrap_sessions", "edge_profiles", "published_source_profiles",
                         "profile_changes", "profile_stream_state", "catalog_state"):
                column = "source_scope" if name == "profile_bootstrap_sessions" else \
                    "catalog_instance_id"
                cur.execute(f"DELETE FROM {self.T}{name} WHERE {column}=%s", (source,))
            cur.execute(f"DELETE FROM {self.T}catalog_sources WHERE catalog_instance_id=%s",
                        (source,))
        self.db.commit()

    def append_events(self, source, count, with_edges=False):
        """Bulk-append ``count`` upsert events shaped like serialize_profile output."""
        stream = self.stream(source)
        head = stream["head_seq"]
        edge_sql = ""
        edge_join = ""
        if with_edges:
            edge_sql = " || jsonb_build_object('edge_profile', e.payload)"
            edge_join = (f"JOIN {self.T}edge_profiles e ON e.catalog_instance_id=p.catalog_instance_id "
                         "AND e.track_id=p.track_id")
        with self.db.cursor() as cur:
            cur.execute(f"""
                WITH pub AS (
                    SELECT row_number() OVER (ORDER BY p.track_id) - 1 AS rn, p.*,
                           'sha256:' || encode(sha256(convert_to(p.media_signature, 'UTF8')),
                                               'hex') AS rev{', e.payload AS edge' if with_edges else ''}
                      FROM {self.T}published_source_profiles p {edge_join}
                     WHERE p.catalog_instance_id=%s),
                series AS (SELECT g, (g - 1) %% (SELECT count(*) FROM pub) AS rn
                             FROM generate_series(1, %s) g)
                INSERT INTO {self.T}profile_changes (catalog_instance_id, epoch, seq, track_id,
                                                     operation, writer_generation, payload)
                SELECT %s, %s, %s + g, pub.track_id, 'upsert', %s,
                       jsonb_build_object('track_id', pub.track_id, 'source', 'waveform',
                         'sample_rate', pub.sample_rate, 'duration_ms', pub.duration_ms,
                         'ref_lufs', pub.ref_lufs, 'start_ramp', encode(pub.start_ramp, 'base64'),
                         'end_ramp', encode(pub.end_ramp, 'base64'), 'analyzer_ver',
                         pub.analyzer_ver, 'analyzed_at', '2026-09-25T00:00:00',
                         'media_signature', pub.rev, 'media_revision', pub.rev)
                       {edge_sql.replace('e.payload', 'pub.edge')}
                  FROM series JOIN pub USING (rn)""",
                        (source, count, source, stream["epoch"], head, self.writer_generation))
            inserted = cur.rowcount
            cur.execute(f"UPDATE {self.T}profile_stream_state SET head_seq=%s "
                        "WHERE catalog_instance_id=%s", (head + inserted, source))
        self.db.commit()
        return head, inserted

    def truncate_events_after(self, source, head):
        with self.db.cursor() as cur:
            cur.execute(f"DELETE FROM {self.T}profile_changes WHERE catalog_instance_id=%s "
                        "AND seq > %s", (source, head))
            cur.execute(f"UPDATE {self.T}profile_stream_state SET head_seq=%s "
                        "WHERE catalog_instance_id=%s", (head, source))
        self.db.commit()
        old = self.db.autocommit
        self.db.autocommit = True
        with self.db.cursor() as cur:
            cur.execute(f"VACUUM {self.T}profile_changes")
            cur.execute(f"VACUUM {self.T}profile_bootstrap_catchup")
        self.db.autocommit = old


# ---- scenario context ---------------------------------------------------------------

class Context:
    def __init__(self, args, server, plugin):
        self.args = args
        self.server = server
        self.plugin = plugin
        self.source = plugin.source("server-a")
        self.scratch = args.scratch
        self.keep = {}
        self.sessions_before = 0
        self.open_clients = []  # (client, sqlite path) created by the running scenario
        self.scratch_paths = []  # other SQLite files of the running scenario

    def cleanup(self):
        """After every scenario, also a failed one: release any v2 session a device
        still holds, close it, and delete its SQLite file unless it is kept."""
        kept = {self.keep.get("v2_db")}
        for client, path in self.open_clients:
            with contextlib.suppress(Exception):
                client.abandon()
            if path not in kept:
                self.drop_db(path)
        for path in self.scratch_paths:
            if path not in kept:
                self.drop_db(path)
                with contextlib.suppress(FileNotFoundError):
                    os.remove(path + ".json")
        self.open_clients = []
        self.scratch_paths = []

    def fixture_guard(self, scenario):
        """The server has profiles with edges to sync (0 == 0 proves nothing).

        On a pristine fixture (journal head as seeded) every seeded profile is
        published with its edge; after scenarios that change the library a few
        edges may be pending again, but never most of them.
        """
        counts = self.plugin.fixture_counts(self.source)
        scenario.metrics["fixture"] = counts
        ok = scenario.check("the server publishes profiles", counts["profiles"] > 0, counts)
        ok = scenario.check("the server publishes edges", counts["edges"] > 0, counts) and ok
        if counts["seeded_head"] is not None and counts["head_seq"] == counts["seeded_head"]:
            ok = scenario.check("pristine fixture: every seeded profile is published with its edge",
                                counts["profiles"] == counts["seeded_profiles"]
                                and counts["edges"] == counts["profiles"], counts) and ok
        else:
            ok = scenario.check("at least 90% of the published profiles carry an edge",
                                counts["edges"] >= 0.9 * counts["profiles"], counts) and ok
        return ok

    def no_leak(self, scenario):
        live = self.plugin.live_sessions()
        scenario.check("no leaked live session", live <= self.sessions_before,
                       {"live": live, "before": self.sessions_before})

    def db_path(self, name):
        path = os.path.join(self.scratch, f"e2e_{name}_{os.getpid()}.sqlite")
        for suffix in ("", "-wal", "-shm"):
            with contextlib.suppress(FileNotFoundError):
                os.remove(path + suffix)
        return path

    def drop_db(self, path):
        size = 0
        for suffix in ("", "-wal", "-shm"):
            with contextlib.suppress(FileNotFoundError):
                size += os.path.getsize(path + suffix)
                os.remove(path + suffix)
        return round(size / 1e6, 1)

    def client(self, db_path, **kwargs):
        # The source is discovered like the app does (/api/catalog/health).
        kwargs.setdefault("server_id", "server-a")
        kwargs.setdefault("token", self.args.auth_token)
        client = SyncClient(self.server.base_url, db_path, log=lambda m: log("  client: " + m),
                            **kwargs)
        self.open_clients.append((client, db_path))
        return client

    def compare(self, scenario, client, label="local dataset equals the server's"):
        local = client.store.digest(per_track=True)
        truth = self.plugin.digest(self.source, per_track=True)
        same = local["sha256"] == truth["sha256"]
        detail = {"local": {k: local[k] for k in ("profiles", "edges", "sha256")},
                  "server": {k: truth[k] for k in ("profiles", "edges", "sha256")}}
        if not same:
            diff = [t for t in sorted(set(local["tracks"]) | set(truth["tracks"]))
                    if local["tracks"].get(t) != truth["tracks"].get(t)]
            detail["differing_tracks"] = len(diff)
            detail["first_differences"] = diff[:10]
        scenario.check(label, same, detail)
        return detail


def run_first_load(ctx, mode):
    name = f"first_load_{mode}"
    scenario = Scenario(name, f"Fresh first load of every waveform + edge profile over the "
                              f"{'v2 bootstrap' if mode == 'v2' else 'legacy bootstrap'} path, "
                              "then /profiles/changes; completes once, in one run.")
    ctx.plugin.reset_rate_limit()
    ctx.fixture_guard(scenario)
    path = ctx.db_path(name)
    plans = {}

    def hook(point, client=None, index=None, total=None, **_info):
        # Diagnostic: the planner's choice for the page query at the start of
        # the snapshot and in the middle (EXPLAIN only, unless --explain-analyze).
        if point == "after_create" or (point == "snapshot_page" and total and
                                        index == total // 2):
            ordinal = 0 if point == "after_create" else index * client.page_size
            with contextlib.suppress(Exception):
                plans[f"ordinal_{ordinal}"] = ctx.plugin.page_plan(
                    client.state["session"]["token"], ordinal, client.page_size,
                    analyze=ctx.args.explain_analyze)
    client = ctx.client(path, mode=mode, hook=hook if mode == "v2" else None)
    with Measure(ctx) as measure:
        result = client.run()
    if plans:
        scenario.metrics["page_query_plan"] = plans
    scenario.metrics.update(measure.result)
    scenario.metrics.update(client_metrics(result))
    counters = result["counters"]
    total = ctx.plugin.query(f"SELECT count(*) FROM {ctx.plugin.T}published_source_profiles "
                             "WHERE catalog_instance_id=%s", (ctx.source,), one=True)[0]
    scenario.metrics["profiles"] = total
    scenario.check("sync reached current", result["phase"] == "current", result["phase"])
    requests = result["stats"]["requests"]
    scenario.check("no connection error or retried request",
                   scenario.metrics["conn_errors"] == 0
                   and not result["stats"]["events"].get("connection_retries")
                   and not result["stats"]["events"].get("create_retries"),
                   {"conn_errors": scenario.metrics["conn_errors"],
                    "events": result["stats"]["events"]})
    if mode == "v2":
        pages = math.ceil(total / client.page_size)
        scenario.check("exactly one v2 create", counters.get("creates") == 1, counters)
        scenario.check("one create request and one request per snapshot page",
                       requests.get("v2_create", {}).get("n") == 1
                       and requests.get("v2_page", {}).get("n") == pages,
                       {"create_requests": requests.get("v2_create", {}).get("n"),
                        "page_requests": requests.get("v2_page", {}).get("n"),
                        "pages": pages})
        scenario.check("no v2 restart (410/400) and no legacy fallback",
                       not counters.get("v2_restarts") and not counters.get("v2_413_fallbacks"),
                       counters)
        scenario.check("every snapshot page fetched once",
                       counters.get("snapshot_pages_fetched") == pages,
                       {"fetched": counters.get("snapshot_pages_fetched"), "expected": pages})
        scenario.check("session released", counters.get("releases") == 1, counters)
        scenario.check("sliding expiry advanced", counters.get("expiry_extensions", 0) > 0,
                       result.get("expires_at_seen"))
    else:
        pages = math.ceil(total / client.legacy_limit)
        scenario.check("every legacy page fetched once",
                       counters.get("legacy_pages_fetched") in (pages, pages + 1),
                       {"fetched": counters.get("legacy_pages_fetched"), "expected": pages})
        scenario.check("no legacy restart", not counters.get("legacy_restarts"), counters)
        scenario.check("one request per legacy page",
                       requests.get("legacy_page", {}).get("n")
                       == counters.get("legacy_pages_fetched"),
                       {"page_requests": requests.get("legacy_page", {}).get("n"),
                        "pages": counters.get("legacy_pages_fetched")})
    scenario.check("no invalid edge", not result["invalid_edges"], result["invalid_edges"])
    scenario.metrics["dataset"] = ctx.compare(scenario, client)
    scenario.check("every edge the server publishes is on the device",
                   scenario.metrics["dataset"]["local"]["edges"]
                   == scenario.metrics["dataset"]["server"]["edges"],
                   {"device": scenario.metrics["dataset"]["local"]["edges"],
                    "server": scenario.metrics["dataset"]["server"]["edges"]})
    scenario.check("no 503 during the load", scenario.metrics["http_503"] == 0,
                   scenario.metrics["http_503"])
    ctx.no_leak(scenario)
    route_budget_checks(scenario, measure.result.get("status_routes"))
    client.close()
    if mode == "v2" and "v2_db" not in ctx.keep:
        ctx.keep["v2_db"] = path  # the device that later scenarios keep syncing
        scenario.metrics["sqlite_file_mb"] = round(sum(
            os.path.getsize(path + suffix) for suffix in ("", "-wal")
            if os.path.exists(path + suffix)) / 1e6, 1)
    else:
        scenario.metrics["sqlite_file_mb"] = ctx.drop_db(path)
    return scenario.finish()


def run_idle_routes(ctx):
    scenario = Scenario("idle_routes", "Status routes over HTTP with no sync running "
                                       "(reference for the under-load numbers).")
    # One warm-up request per route: the first request after a (re)start pays
    # one-time costs (the host's lazy imports, the 60 s availability probe).
    for route in client_sim.POLL_ROUTES:
        with contextlib.suppress(Exception):
            urllib.request.urlopen(ctx.server.base_url + PREFIX + route, timeout=60).read()
    with Measure(ctx) as measure:
        time.sleep(ctx.args.idle_seconds)
    scenario.metrics.update(measure.result)
    route_budget_checks(scenario, measure.result.get("status_routes"))
    return scenario.finish()


def run_reanalysis(ctx):
    scenario = Scenario("reanalysis_noop", "An AudioMuse re-analysis pass fires the analysis "
                                           "hook for every song; nothing changed, so it must "
                                           "append no journal event, leave every published row "
                                           "and edge row untouched, schedule no edge job, and a "
                                           "device's next delta downloads nothing.")
    plugin = ctx.plugin
    ctx.fixture_guard(scenario)
    path = ctx.keep.get("v2_db")
    if path is None:
        # No device from first_load_v2: load one first, so the device check
        # always runs (a skipped check must never read as PASS).
        path = ctx.db_path("reanalysis_device")
        device = ctx.client(path, mode="v2")
        loaded = device.run()
        device.close()
        ctx.keep["v2_db"] = path
        scenario.notes.append("first_load_v2 did not run first; loaded a device for the "
                              "delta check")
        scenario.check("device loaded before the pass", loaded["phase"] == "current",
                       loaded["phase"])
    ids = plugin.published_ids(ctx.source)
    before = plugin.stream(ctx.source)
    jobs_before = plugin.edge_jobs()
    edges_before = plugin.edge_rows(ctx.source)
    data_before = plugin.digest(ctx.source)
    started = time.perf_counter()
    statuses = plugin.hook_pass(ctx.source, ids)
    hook_s = time.perf_counter() - started
    after_hook = plugin.stream(ctx.source)
    edges_after_hook = plugin.edge_rows(ctx.source)
    sample = ids[:: max(1, len(ids) // ctx.args.forced_sample)][: ctx.args.forced_sample]
    started = time.perf_counter()
    forced = plugin.noop_complete(ctx.source, sample)
    forced_s = time.perf_counter() - started
    after = plugin.stream(ctx.source)
    jobs_after = plugin.edge_jobs()
    edges_after = plugin.edge_rows(ctx.source)
    data_after = plugin.digest(ctx.source)
    scenario.metrics.update({
        "songs": len(ids), "hook_statuses": statuses, "hook_pass_s": round(hook_s, 1),
        "hook_ms_per_song": round(hook_s * 1000 / max(1, len(ids)), 2),
        "forced_identical_completions": forced, "forced_sample": len(sample),
        "forced_s": round(forced_s, 1), "head_before": before["head_seq"],
        "head_after_hook": after_hook["head_seq"], "head_after": after["head_seq"],
        "edge_jobs_before": jobs_before, "edge_jobs_after": jobs_after,
        "edge_rows_before": edges_before, "edge_rows_after": edges_after,
        "published_before": data_before, "published_after": data_after})
    scenario.check("there are songs to re-analyse", len(ids) > 0 and len(sample) > 0,
                   {"songs": len(ids), "forced_sample": len(sample)})
    scenario.check("hook pass appended 0 events", after_hook["head_seq"] == before["head_seq"],
                   after_hook["head_seq"] - before["head_seq"])
    scenario.check("every song short-circuited as current",
                   statuses.get("current") == len(ids), statuses)
    scenario.check("hook pass left every edge row untouched", edges_after_hook == edges_before,
                   {"before": edges_before, "after": edges_after_hook})
    scenario.check("forced identical completions appended 0 events",
                   after["head_seq"] == after_hook["head_seq"],
                   after["head_seq"] - after_hook["head_seq"])
    scenario.check("forced identical completions all applied", forced == len(sample), forced)
    scenario.check("every edge row untouched (keys and updated_at)",
                   edges_after == edges_before, {"before": edges_before, "after": edges_after})
    scenario.check("every published profile and served edge unchanged",
                   data_after == data_before, {"before": data_before, "after": data_after})
    scenario.check("no edge job scheduled or touched", jobs_after == jobs_before,
                   {"before": jobs_before, "after": jobs_after})
    client = ctx.client(path, mode="v2")
    before_events = client.state.get("counters", {}).get("delta_events", 0)
    result = client.run()
    downloaded = result["counters"].get("delta_events", 0) - before_events
    scenario.metrics["next_delta"] = {
        "events": downloaded,
        "wire_bytes": result["stats"]["requests"].get("changes", {}).get("wire_bytes")}
    scenario.check("the device's next delta downloads nothing", downloaded == 0, downloaded)
    ctx.compare(scenario, client, "device dataset still equals the server's")
    client.close()
    return scenario.finish()


def _device_proc(base_url, db_path, user, source, token, out_path):
    stats = Stats()
    client = SyncClient(base_url, db_path, mode="v2", user=user, catalog_instance_id=source,
                        token=token, stats=stats)
    try:
        result = client.run()
        result["digest"] = client.store.digest()
        client.close()
    except BaseException as exc:  # noqa: BLE001
        result = {"error": f"{type(exc).__name__}: {exc}"}
        client.abandon()  # release the session this device still holds
    with open(out_path, "w") as handle:
        json.dump(result, handle)


def run_concurrent_devices(ctx):
    scenario = Scenario("concurrent_devices", "Two devices do their own v2 first load at the "
                                              "same time (separate processes and SQLite files).")
    ctx.plugin.reset_rate_limit()
    ctx.fixture_guard(scenario)
    mp = multiprocessing.get_context("spawn")
    procs, outs, paths = [], [], []
    with Measure(ctx) as measure:
        for index in (1, 2):
            path = ctx.db_path(f"device{index}")
            out = path + ".json"
            paths.append(path)
            ctx.scratch_paths.append(path)
            outs.append(out)
            proc = mp.Process(target=_device_proc, args=(ctx.server.base_url, path,
                                                         f"device-{index}", ctx.source,
                                                         ctx.args.auth_token, out))
            proc.start()
            procs.append(proc)
        for proc in procs:
            proc.join()
    scenario.metrics.update(measure.result)
    truth = ctx.plugin.digest(ctx.source)
    for index, out in enumerate(outs, start=1):
        if not os.path.exists(out):
            scenario.check(f"device {index} reported a result", False,
                           f"exit code {procs[index - 1].exitcode}")
            continue
        with open(out) as handle:
            result = json.load(handle)
        os.remove(out)
        if "error" in result:
            scenario.check(f"device {index} completed", False, result["error"])
            continue
        scenario.metrics[f"device{index}"] = client_metrics(result)
        counters = result["counters"]
        scenario.check(f"device {index} reached current once",
                       result["phase"] == "current" and counters.get("creates") == 1
                       and not counters.get("v2_restarts"), counters)
        scenario.check(f"device {index} dataset equals the server's",
                       result["digest"]["sha256"] == truth["sha256"],
                       {"device": result["digest"], "server": truth})
        scenario.check(f"device {index} saw no 503",
                       result["stats"]["status_counts"].get("503", 0) == 0,
                       result["stats"]["status_counts"])
    for path in paths:
        ctx.drop_db(path)
    ctx.no_leak(scenario)
    route_budget_checks(scenario, measure.result.get("status_routes"))
    return scenario.finish()


def _create_once(base_url, source, user, token, timeout, page_size=50):
    stats = Stats()
    transport = client_sim.Transport(base_url, stats, token=token, user=user)
    body = {**ENVELOPE, "catalog_instance_id": source, "page_size": page_size,
            "expiry_mode": "sliding", "client_request_id": str(uuid.uuid4())}
    started = time.perf_counter()
    try:
        resp = transport.request("POST", "/api/profiles/bootstrap/sessions", body=body,
                                 timeout=timeout, kind="create")
        status, created = resp.status, resp.body
        retry_after = resp.header("Retry-After")
    except client_sim.TransportError as exc:
        status, created, retry_after = "timeout", {"error": str(exc)}, None
    elapsed = time.perf_counter() - started
    token_value = (created or {}).get("session_token") if status == 200 else None
    if token_value:
        transport.request("POST", "/api/profiles/bootstrap/sessions/release",
                          body={**ENVELOPE, "catalog_instance_id": source,
                                "session_token": token_value}, timeout=30, kind="release")
    transport.close()
    return {"status": status, "s": elapsed, "retry_after": retry_after,
            "snapshot_count": (created or {}).get("snapshot_count")}


def run_creators(ctx):
    scenario = Scenario("creators", "Two concurrent v2 creators, on the same source and on "
                                    "different sources: create latency and 503s (P2-4 input).")
    plugin = ctx.plugin
    other = plugin.add_second_source(ctx.source)
    rounds = ctx.args.creator_rounds
    results = {}
    try:
        with Measure(ctx) as measure:
            for label, sources in (("same_source", (ctx.source, ctx.source)),
                                   ("different_sources", (ctx.source, other))):
                samples = []
                for round_index in range(rounds):
                    plugin.reset_rate_limit()
                    outcome = [None, None]

                    def worker(slot, source):
                        outcome[slot] = _create_once(
                            ctx.server.base_url, source, f"creator-{slot}", ctx.args.auth_token,
                            ctx.args.create_timeout_s)
                    threads = [threading.Thread(target=worker, args=(slot, source))
                               for slot, source in enumerate(sources)]
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join()
                    samples.extend(outcome)
                    time.sleep(0.5)
                times = [s["s"] * 1000 for s in samples if s["status"] == 200]
                statuses = collections.Counter(str(s["status"]) for s in samples)
                results[label] = {"creates": len(samples), "statuses": dict(statuses),
                                  "create_ms": client_sim.latency_summary(times),
                                  "retry_after_503": sorted({s["retry_after"] for s in samples
                                                             if s["status"] == 503} - {None})}
                p95 = results[label]["create_ms"]["p95_ms"]
                scenario.check(f"{label}: create p95 <= {BUDGET_CREATE_S:.0f} s",
                               p95 is not None and p95 <= BUDGET_CREATE_S * 1000, p95)
                scenario.check(f"{label}: no 503", statuses.get("503", 0) == 0, dict(statuses))
                scenario.check(f"{label}: no timeout at {ctx.args.create_timeout_s:.0f} s",
                               statuses.get("timeout", 0) == 0, dict(statuses))
    finally:
        plugin.drop_source(other)
        plugin.reset_rate_limit()
    scenario.metrics.update(measure.result)
    scenario.metrics.update(results)
    scenario.metrics["rounds"] = rounds
    route_budget_checks(scenario, measure.result.get("status_routes"))
    ctx.no_leak(scenario)
    return scenario.finish()


KILL_POINTS = ("create_capture", "after_create", "mid_snapshot", "snapshot_end",
               "catchup_capture", "mid_catchup", "before_release", "during_deltas")


def run_kill_restart(ctx):
    scenario = Scenario("kill_restart", "kill -9 and restart gunicorn at 8 points (during the "
                                        "create's snapshot capture, after create, mid-snapshot, "
                                        "snapshot end, during the first catch-up capture, "
                                        "mid-catch-up, before release, during deltas) while the "
                                        "library changes; the client resumes from its "
                                        "checkpoint, retries the lost create with the same "
                                        "client_request_id, and ends with the server's dataset.")
    plugin = ctx.plugin
    ctx.plugin.reset_rate_limit()
    ctx.fixture_guard(scenario)
    ids = plugin.published_ids(ctx.source, with_edge=True)
    rng = random.Random(626)
    pool = rng.sample(ids, min(len(ids), 1200))
    fired = []
    kills = []
    threads = []
    mutations = collections.Counter()
    k5 = {}
    killed_at = {}

    def restart(entry, started, between=None):
        log(f"  kill -9 at {entry['point']} ({entry['kind']}, in flight: "
            f"{entry.get('request_in_flight')})")
        killed_at[entry["point"]] = time.monotonic()
        ctx.server.restart(between=between)
        entry["restart_s"] = round(time.perf_counter() - started, 2)
        kills.append(entry)

    def kill_between(label):
        # The hook runs between two requests: nothing is in flight. The client
        # process "dies" too (SimulatedCrash) and a new one resumes.
        restart({"point": label, "kind": "between_requests", "request_in_flight": None},
                time.perf_counter())
        raise SimulatedCrash(label)

    def kill_in_flight(label, client):
        # Kill while the client's next request is on the wire (not a timer:
        # wait until the transport has sent it).
        def run():
            started = time.perf_counter()
            deadline = started + 10
            while client.transport.inflight_kind is None and time.perf_counter() < deadline:
                time.sleep(0.0005)
            time.sleep(0.002)
            restart({"point": label, "kind": "in_flight",
                     "request_in_flight": client.transport.inflight_kind}, started)
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        threads.append(thread)

    def kill_in_capture(label, client, table):
        # Hold a lock the capture needs, wait until the capture's backend waits
        # on it (inside its capture transaction), then kill the server and
        # release the lock while it is down.
        blocked, release = plugin.block_capture(table)

        def run():
            # release() is idempotent; the finally frees the lock (and its
            # connection) even if a step below raises.
            try:
                started = time.perf_counter()
                # The capture waits at most its 5 s lock_timeout, then answers 503.
                deadline = started + 4
                waiting = 0
                while time.perf_counter() < deadline:
                    waiting = blocked()
                    if waiting:
                        break
                    time.sleep(0.005)
                if not waiting:
                    release()
                    kills.append({"point": label, "kind": "in_capture", "blocked_capture": False,
                                  "request_in_flight": client.transport.inflight_kind})
                    return
                if label == "create_capture":
                    row = plugin.query(
                        f"SELECT session_id::text FROM {plugin.T}profile_bootstrap_sessions "
                        "WHERE client_request_id=%s AND state='capturing'",
                        (client.state["client_request_id"],), one=True)
                    k5["orphan_session"] = row[0] if row else None
                restart({"point": label, "kind": "in_capture", "blocked_capture": True,
                         "request_in_flight": client.transport.inflight_kind}, started,
                        between=release)
            finally:
                release()
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        threads.append(thread)

    def hook(point, client=None, index=None, total=None, **_info):
        if point == "create_sending" and "create_capture" not in fired:
            fired.append("create_capture")
            k5["request_id"] = client.state.get("client_request_id")
            kill_in_capture("create_capture", client, "published_source_profiles")
        elif point == "after_create" and "after_create" not in fired:
            fired.append("after_create")
            for thread in threads:
                thread.join()
            k5["request_id_after"] = client.state.get("client_request_id")
            if k5.get("orphan_session"):
                row = plugin.query(
                    f"SELECT expires_at <= now(), state FROM {plugin.T}profile_bootstrap_sessions "
                    "WHERE session_id=%s", (k5["orphan_session"],), one=True)
                k5["orphan_after_retry"] = ("deleted" if row is None else
                                            "expired" if row[0] else f"live ({row[1]})")
            mutations["waveform"] += plugin.republish_waveform(ctx.source, pool[0:100])
            mutations["media"] += plugin.change_media(ctx.source, pool[100:120])
            mutations["withdraw"] += plugin.withdraw(ctx.source, pool[120:130])
            kill_between("after_create")
        elif point == "snapshot_page" and "mid_snapshot" not in fired and total and \
                index == max(1, total // 2):
            fired.append("mid_snapshot")
            kill_in_flight("mid_snapshot", client)
        elif point == "snapshot_end" and "snapshot_end" not in fired:
            fired.append("snapshot_end")
            mutations["edges"] += plugin.publish_edges(ctx.source, pool[100:120])
            kill_between("snapshot_end")
        elif point == "catchup_sending" and index == 0 and "catchup_capture" not in fired:
            fired.append("catchup_capture")
            kill_in_capture("catchup_capture", client, "profile_changes")
        elif point == "catchup_page" and "mid_catchup" not in fired and index == 1 and \
                client.state.get("phase") == "catchup":
            fired.append("mid_catchup")
            kill_in_flight("mid_catchup", client)
        elif point == "before_release" and "before_release" not in fired:
            fired.append("before_release")
            mutations["waveform"] += plugin.republish_waveform(ctx.source, pool[200:400], 0.2)
            mutations["media"] += plugin.change_media(ctx.source, pool[400:410])
            mutations["withdraw"] += plugin.withdraw(ctx.source, pool[410:415])
            kill_between("before_release")
        elif point == "delta_page" and "during_deltas" not in fired and index == 1:
            fired.append("during_deltas")
            kill_in_flight("during_deltas", client)

    path = ctx.db_path("kill")
    head_before = plugin.stream(ctx.source)["head_seq"]
    runs = 0
    result = None
    stats = Stats()
    with Measure(ctx) as measure:
        while True:
            runs += 1
            client = ctx.client(path, mode="v2", hook=hook, stats=stats)
            try:
                result = client.run()
                break
            except SimulatedCrash as crash:
                log(f"  client crashed at {crash}; resuming from the checkpoint")
            finally:
                client.close()
            if runs > 20:
                raise RuntimeError("kill_restart: too many resumes")
        for thread in threads:
            thread.join()
    scenario.metrics.update(measure.result)
    scenario.metrics.update(client_metrics(result))
    counters = result["counters"]
    events = result["stats"]["events"]
    total = plugin.query(f"SELECT count(*) FROM {plugin.T}published_source_profiles "
                         "WHERE catalog_instance_id=%s", (ctx.source,), one=True)[0]
    scenario.metrics["profiles_after"] = total
    scenario.metrics.update({"kills": kills, "client_runs": runs, "mutations": dict(mutations),
                             "events_during_sync": plugin.stream(ctx.source)["head_seq"] -
                             head_before, "points_fired": fired, "k5": k5})
    scenario.check(f"all {len(KILL_POINTS)} kill points fired and killed",
                   sorted(fired) == sorted(KILL_POINTS)
                   and sorted(k["point"] for k in kills) == sorted(KILL_POINTS),
                   {"fired": fired, "killed": [k["point"] for k in kills]})
    cut = [k for k in kills if k["kind"] != "between_requests"]
    scenario.check("every in-flight and in-capture kill cut a request off",
                   all(k.get("request_in_flight") for k in cut),
                   [(k["point"], k.get("request_in_flight")) for k in cut])
    scenario.check("both capture kills landed inside the capture transaction",
                   all(k.get("blocked_capture") for k in kills if k["kind"] == "in_capture"),
                   [(k["point"], k.get("blocked_capture")) for k in kills
                    if k["kind"] == "in_capture"])
    # Each cut-off request is matched to its own client-side failure: the
    # first unmatched connection failure of the kind in flight, after the kill.
    # (A retry while the server was down fails too, so counting is not enough.)
    unmatched = sorted(stats.failures, key=lambda failure: failure[1])
    matched = []
    for kill in sorted(cut, key=lambda k: killed_at.get(k["point"], float("inf"))):
        at = killed_at.get(kill["point"])
        hit = None if at is None else next(
            (f for f in unmatched if f[0] == kill.get("request_in_flight") and f[1] >= at), None)
        if hit is not None:
            unmatched.remove(hit)
        matched.append((kill["point"], kill.get("request_in_flight"), hit is not None))
    scenario.check("the client saw each cut-off request fail (matched one to one)",
                   bool(cut) and all(ok for _point, _kind, ok in matched),
                   {"matched": matched, "conn_errors": scenario.metrics["conn_errors"]})
    scenario.check("the lost create was retried with the same client_request_id (K5)",
                   bool(events.get("create_retries", 0) >= 1 and k5.get("request_id")
                        and k5.get("request_id") == k5.get("request_id_after")),
                   {"create_retries": events.get("create_retries"), **k5})
    scenario.check("the orphaned capture's session was replaced, not leaked",
                   bool(k5.get("orphan_session"))
                   and k5.get("orphan_after_retry") in ("deleted", "expired"), k5)
    scenario.check("sync reached current", result["phase"] == "current", result["phase"])
    scenario.check("one create for the whole run (never restarted from zero)",
                   counters.get("creates") == 1 and not counters.get("v2_restarts"), counters)
    requested = result["stats"]["requests"].get("v2_page", {}).get("n", 0)
    pages = counters.get("snapshot_pages_fetched", 0)
    scenario.check("each snapshot page committed once; at most one re-request per kill",
                   requested - pages <= len(kills),
                   {"committed": pages, "requests": requested, "kills": len(kills)})
    scenario.check("catch-up carried the changes made during the snapshot",
                   counters.get("catchup_pages_fetched", 0) >= 2, counters)
    scenario.check("deltas carried the changes made after the catch-up",
                   counters.get("delta_pages_fetched", 0) >= 2, counters)
    scenario.check("no invalid edge", not result["invalid_edges"], result["invalid_edges"])
    view = client_view(path)
    scenario.metrics["dataset"] = ctx.compare(scenario, view)
    view.store.close()
    ctx.no_leak(scenario)
    scenario.metrics["sqlite_file_mb"] = ctx.drop_db(path)
    return scenario.finish()


def client_view(path):
    """A client object over an existing SQLite file, for digests only."""
    return types.SimpleNamespace(store=client_sim.Store(path, Stats()))


def run_lum005(ctx):
    scenario = Scenario("lum005_k6", "LUM-005-style full republish (every waveform changes, "
                                     "same media) must reach a device at <= 1 KB per track "
                                     "with K6 edge references (P3-2).")
    health = json.loads(urllib.request.urlopen(ctx.server.base_url + PREFIX + "/api/health",
                                               timeout=30).read())
    edge_refs = ((health.get("capabilities") or {}).get("profile_stream") or {}).get("edge_refs")
    plugin = ctx.plugin
    ctx.fixture_guard(scenario)
    ids = plugin.published_ids(ctx.source, with_edge=True)
    # With K6 the criterion is about a full republish; without it the no-K6
    # baseline is measured on a sample (per-track bytes do not depend on N).
    sample = ids if edge_refs else ids[: ctx.args.lum005_sample]
    path = ctx.keep.get("v2_db")
    fresh = path is None
    if fresh:
        path = ctx.db_path("lum005")
    client = ctx.client(path, mode="v2")
    client.run()  # current first (a fresh load, or the first_load_v2 device catching up)

    def delta_bytes(summary):
        kinds = [summary["requests"].get(kind, {}) for kind in ("changes", "profiles_fetch")]
        return (sum(k.get("wire_bytes", 0) for k in kinds),
                sum(k.get("decoded_bytes", 0) for k in kinds))
    wire_before, decoded_before = delta_bytes(client.stats.summary())
    republished = plugin.republish_waveform(ctx.source, sample, 0.05)
    result = client.run()
    wire_after, decoded_after = delta_bytes(result["stats"])
    wire = wire_after - wire_before
    per_track = wire / max(1, republished)
    scenario.metrics.update({
        "capability_profile_stream_edge_refs": bool(edge_refs), "republished": republished,
        "delta_wire_bytes": wire, "wire_bytes_per_track_without_k6": round(per_track),
        "decoded_bytes_per_track": round((decoded_after - decoded_before) / max(1, republished)),
        "edge_refs_kept": result["store"].get("edge_refs_kept", 0),
        "edge_fetches": result["counters"].get("edge_fetches", 0),
        "extrapolated_full_republish_wire_mb": round(per_track * len(ids) / 1e6, 1)})
    scenario.check("every sampled track was republished",
                   republished == len(sample) > 0,
                   {"republished": republished, "sample": len(sample)})
    ctx.compare(scenario, client, "device dataset equals the server's after the republish")
    client.close()
    if fresh:
        ctx.drop_db(path)
    if not edge_refs:
        scenario.notes.append("capabilities.profile_stream.edge_refs is absent: K6 (P3-2) is not "
                              "built. The measured delta above is the no-K6 baseline; the "
                              "<= 1 KB/track criterion cannot pass until P3-2 ships.")
        scenario.check(f"<= {BUDGET_K6_BYTES_PER_TRACK} B/track on the wire (pending P3-2)",
                       per_track <= BUDGET_K6_BYTES_PER_TRACK, round(per_track), pending="P3-2")
        return scenario.finish()
    scenario.check(f"<= {BUDGET_K6_BYTES_PER_TRACK} B/track on the wire",
                   per_track <= BUDGET_K6_BYTES_PER_TRACK, round(per_track))
    return scenario.finish()


def run_capped_catchup(ctx):
    scenario = Scenario("capped_catchup", "First v2 catch-up capture of a journal interval "
                                          "up to the P1-2 floor-hold cap (4 x retention): time, "
                                          "WAL, table size, server memory (P2-4 input).")
    plugin = ctx.plugin
    stream = plugin.stream(ctx.source)
    cap_events, cap_bytes = plugin.pb.catchup_limits(stream["retention_limit"])
    scenario.metrics.update({"retention_limit": stream["retention_limit"],
                             "cap_events": cap_events, "cap_bytes": cap_bytes})
    measurements = []
    plan = []
    for spec in ctx.args.catchup_sizes.split(","):
        spec = spec.strip()
        if not spec:
            continue
        with_edges = spec.endswith("e")
        value = spec.rstrip("e")
        count = cap_events if value == "cap" else int(float(value))
        plan.append((min(count, cap_events), with_edges))
    for count, with_edges in plan:
        plugin.reset_rate_limit()
        stats = Stats()
        transport = client_sim.Transport(ctx.server.base_url, stats, token=ctx.args.auth_token)
        body = {**ENVELOPE, "catalog_instance_id": ctx.source, "page_size": 50,
                "expiry_mode": "sliding", "client_request_id": str(uuid.uuid4())}
        created = transport.request("POST", "/api/profiles/bootstrap/sessions", body=body,
                                    timeout=120, kind="create").body
        token = created["session_token"]
        t0 = time.perf_counter()
        head, inserted = plugin.append_events(ctx.source, count, with_edges=with_edges)
        append_s = time.perf_counter() - t0
        journal_mb = plugin.relation_mb("profile_changes")
        lsn = plugin.wal_lsn()
        entry = {"events": inserted, "with_edges": with_edges, "append_s": round(append_s, 1),
                 "journal_mb": journal_mb}
        page_body = {**ENVELOPE, "catalog_instance_id": ctx.source, "session_token": token}
        with Measure(ctx, poll=True) as measure:
            if ctx.args.catchup_client_timeout_s:
                # A client with a short request timeout (Auralscape fast-fails at
                # 10 s): it times out, retries and honours Retry-After on 503.
                first = None
                attempts = collections.Counter()
                while True:
                    try:
                        resp = transport.request("POST", "/api/profiles/bootstrap/sessions/catchup",
                                                 body=page_body,
                                                 timeout=ctx.args.catchup_client_timeout_s,
                                                 kind="catchup")
                    except client_sim.TransportError:
                        attempts["timeout"] += 1
                        transport.close()
                        continue
                    attempts[str(resp.status)] += 1
                    if resp.status == 503:
                        time.sleep(int(resp.header("Retry-After") or 5))
                        continue
                    first = resp
                    break
                entry["short_timeout_client"] = {"timeout_s": ctx.args.catchup_client_timeout_s,
                                                 "attempts": dict(attempts)}
            else:
                first = transport.request("POST", "/api/profiles/bootstrap/sessions/catchup",
                                          body=page_body, timeout=3600, kind="catchup")
        entry.update({"first_catchup_s": measure.result["wall_s"],
                      "status": first.status, "error": first.error,
                      "wal_mb": plugin.wal_mb_since(lsn),
                      "catchup_table_mb": plugin.relation_mb("profile_bootstrap_catchup"),
                      "peak_server_rss_mb": measure.result["peak_server_rss_mb"],
                      "peak_worker_rss_mb": measure.result["peak_worker_rss_mb"],
                      "status_routes": measure.result.get("status_routes")})
        if first.status == 200:
            entry["first_page_changes"] = len(first.body.get("changes") or [])
            entry["first_page_wire_bytes"] = first.wire
        transport.request("POST", "/api/profiles/bootstrap/sessions/release",
                          body=page_body, timeout=600, kind="release")
        transport.close()
        plugin.truncate_events_after(ctx.source, head)
        entry["per_event_us"] = round(entry["first_catchup_s"] * 1e6 / max(1, inserted), 1)
        measurements.append(entry)
        log(f"  capped catch-up {inserted} events (edges={with_edges}): "
            f"{entry['first_catchup_s']} s, WAL {entry['wal_mb']} MB, "
            f"table {entry['catchup_table_mb']} MB, status {first.status}")
    scenario.metrics["measurements"] = measurements
    waveform = [m for m in measurements if not m["with_edges"] and m["status"] == 200]
    if waveform:
        largest = max(waveform, key=lambda m: m["events"])
        factor = cap_events / max(1, largest["events"])
        scenario.metrics["extrapolated_to_cap"] = {
            "basis_events": largest["events"], "cap_events": cap_events,
            "measured_at_cap": largest["events"] >= cap_events,
            "first_catchup_s": round(largest["first_catchup_s"] * factor, 1),
            "wal_mb": round(largest["wal_mb"] * factor),
            "catchup_table_mb": round(largest["catchup_table_mb"] * factor),
            "method": "linear in events from the largest waveform-only measurement"}
    scenario.check("every measured capture succeeded",
                   all(m["status"] == 200 for m in measurements),
                   [m["status"] for m in measurements])
    ctx.no_leak(scenario)
    return scenario.finish()


SCENARIOS = collections.OrderedDict([
    ("idle_routes", run_idle_routes),
    ("first_load_v2", lambda ctx: run_first_load(ctx, "v2")),
    ("first_load_legacy", lambda ctx: run_first_load(ctx, "legacy")),
    ("reanalysis_noop", run_reanalysis),
    ("concurrent_devices", run_concurrent_devices),
    ("creators", run_creators),
    ("kill_restart", run_kill_restart),
    ("lum005_k6", run_lum005),
    ("capped_catchup", run_capped_catchup),
])


def environment(plugin, server):
    info = {"python": platform.python_version(), "platform": platform.platform(),
            "cpus": os.cpu_count()}
    with contextlib.suppress(OSError):
        with open("/proc/meminfo") as handle:
            info["mem_total_gb"] = round(int(handle.readline().split()[1]) / 1024 / 1024, 1)
    with contextlib.suppress(OSError):
        with open("/proc/cpuinfo") as handle:
            for line in handle:
                if line.startswith("model name"):
                    info["cpu"] = line.split(":", 1)[1].strip()
                    break
    rows = plugin.query("SELECT version(), current_setting('shared_buffers'), "
                        "current_setting('max_wal_size'), current_setting('work_mem')", one=True)
    info["postgres"] = {"version": rows[0], "shared_buffers": rows[1], "max_wal_size": rows[2],
                        "work_mem": rows[3]}
    fixture = plugin.query("SELECT value FROM lumae_perf_fixture WHERE key='fixture'", one=True)
    info["fixture"] = fixture[0] if fixture else None
    info["database_mb"] = round(float(plugin.query(
        "SELECT pg_database_size(current_database())", one=True)[0]) / 1e6, 1)
    info["server"] = server.describe()
    with contextlib.suppress(Exception):
        info["git_sha"] = subprocess.run(["git", "-C", REPO, "describe", "--always", "--dirty"],
                                         capture_output=True, text=True).stdout.strip()
    return info


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dsn", default=os.environ.get("LUMAE_E2E_DSN"))
    parser.add_argument("--host", choices=("local", "docker"), default="local")
    parser.add_argument("--port", type=int, default=18080, help="local gunicorn port")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="docker host URL")
    parser.add_argument("--compose", default="docker-compose.yml,docker-compose.external-db.yml",
                        help="compose files for --host docker (relative to scripts/e2e)")
    parser.add_argument("--scenarios", default="all")
    parser.add_argument("--out", default=None, help="write the results JSON here")
    parser.add_argument("--scratch", default=os.environ.get("LUMAE_E2E_SCRATCH", "/tmp"),
                        help="directory for SQLite device files and logs")
    parser.add_argument("--auth-token", default=os.environ.get("LUMAE_E2E_AUTH_TOKEN"))
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--idle-seconds", type=float, default=15.0)
    parser.add_argument("--forced-sample", type=int, default=2000,
                        help="songs re-completed with an identical result (reanalysis_noop)")
    parser.add_argument("--creator-rounds", type=int, default=10)
    parser.add_argument("--create-timeout-s", type=float, default=10.0,
                        help="client timeout for a create (the app fast-fails at 10 s)")
    parser.add_argument("--lum005-sample", type=int, default=1000)
    parser.add_argument("--catchup-sizes", default="100000,cap,10000e",
                        help="first catch-up sizes: counts, 'cap' (4 x retention), suffix e = "
                             "events embedding their edge")
    parser.add_argument("--explain-analyze", action="store_true",
                        help="first loads record EXPLAIN ANALYZE (not just EXPLAIN) of the v2 "
                             "page query at ordinal 0 and mid-snapshot; runs it once more")
    parser.add_argument("--catchup-client-timeout-s", type=float, default=0.0,
                        help="if set, the capped catch-up client uses this request timeout")
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or LUMAE_E2E_DSN is required")
    os.makedirs(args.scratch, exist_ok=True)
    names = list(SCENARIOS) if args.scenarios == "all" else [
        name.strip() for name in args.scenarios.split(",") if name.strip()]
    # "name#label" runs a scenario again under another label (e.g. first_load_v2#warm).
    unknown = [name for name in names if name.split("#")[0] not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenarios: {unknown}")

    plugin = Plugin(args.dsn)
    if args.host == "local":
        server = LocalServer(args.dsn, args.port, args.scratch, auth_token=args.auth_token)
    else:
        server = DockerServer([os.path.join(HERE, name) for name in args.compose.split(",")],
                              args.base_url)
    log(f"starting {args.host} server")
    server.start()
    ctx = Context(args, server, plugin)
    report = {"bench": "e2e_server_matrix", "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                                        time.gmtime()),
              "environment": environment(plugin, server), "source": ctx.source,
              "scenarios": []}
    try:
        for name in names:
            log(f"scenario {name}")
            ctx.sessions_before = plugin.live_sessions()
            started = time.perf_counter()
            try:
                outcome = SCENARIOS[name.split("#")[0]](ctx)
                outcome["name"] = name
            except Exception as exc:  # noqa: BLE001 - one broken scenario must not stop the gate
                import traceback

                outcome = {"name": name, "status": "ERROR",
                           "error": f"{type(exc).__name__}: {exc}",
                           "traceback": traceback.format_exc()[-4000:]}
                if not server.alive():
                    server.start()
            finally:
                ctx.cleanup()
            outcome["elapsed_s"] = round(time.perf_counter() - started, 1)
            report["scenarios"].append(outcome)
            log(f"scenario {name}: {outcome['status']} in {outcome['elapsed_s']} s")
            if args.out:
                with open(args.out, "w") as handle:
                    json.dump(report, handle, indent=1, sort_keys=True, default=str)
    finally:
        path = ctx.keep.get("v2_db")
        if path:
            ctx.drop_db(path)
        if args.host == "local":
            server.stop()
    report["summary"] = {s["name"]: s["status"] for s in report["scenarios"]}
    report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if args.out:
        with open(args.out, "w") as handle:
            json.dump(report, handle, indent=1, sort_keys=True, default=str)
    width = max(len(name) for name in report["summary"]) if report["summary"] else 10
    for name, status in report["summary"].items():
        print(f"{name.ljust(width)}  {status}")
    failed = [n for n, s in report["summary"].items() if s in ("FAIL", "ERROR")]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
