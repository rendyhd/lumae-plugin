"""Minimal AudioMuse host stub for performance measurement (not a test).

Installs a ``plugin.api`` module (and the ``tasks.*`` modules the plugin
imports) so the real ``plugins.LumaeAnalysis`` package can be imported and
driven against a PostgreSQL database named by ``LUMAE_PERF_DSN``.

Every bench imports this module first. It is a harness helper, not a pytest
module: ``scripts/perf`` is never collected (the suite runs ``tests/plugins``).

Environment
-----------
``LUMAE_PERF_DSN``
    Required. libpq DSN or URL of a *disposable* database, e.g.
    ``postgresql://lumae_test@127.0.0.1:<port>/<db>``.
``PING_DELAY_S``
    Optional artificial delay for the stubbed provider ping (default 0, which
    measures health "excluding the provider ping").
"""
import contextlib
import json
import os
import sys
import time
import types

import psycopg2
import psycopg2.extensions

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HERE = os.path.dirname(os.path.abspath(__file__))
# Mirror the host's per-connection options (AudioMuse database.py _CONNECT_OPTIONS).
HOST_OPTIONS = "-c statement_timeout=600000 -c max_parallel_workers_per_gather=0"
SERVER_ID = "server-a"
PING_DELAY_S = float(os.environ.get("PING_DELAY_S", "0") or 0)
T = "plugin_lumae_analysis__"


def dsn():
    value = os.environ.get("LUMAE_PERF_DSN")
    if not value:
        sys.exit("LUMAE_PERF_DSN is not set; point it at a disposable PostgreSQL database")
    return value


DSN = dsn()
# profile_bootstrap opens its own connections from config.DATABASE_URL.
DATABASE_URL = psycopg2.extensions.make_dsn(DSN, options=HOST_OPTIONS)

if REPO not in sys.path:
    sys.path.insert(0, REPO)

STATE = types.SimpleNamespace(db=None, statements=0, ping_calls=0, logged_errors=[])


def connect(**kwargs):
    return psycopg2.connect(DSN, options=HOST_OPTIONS, **kwargs)


class CountingCursor(psycopg2.extensions.cursor):
    def execute(self, query, vars=None):
        STATE.statements += 1
        return super().execute(query, vars)

    def executemany(self, query, vars_list):
        for v in vars_list:
            STATE.statements += 1
            super().execute(query, v)


def counting_connect():
    return connect(cursor_factory=CountingCursor)


def get_db():
    if STATE.db is None or STATE.db.closed:
        STATE.db = counting_connect()
    return STATE.db


def aux_cursor():
    """An autocommit side connection for LSN/size/lock observation."""
    aux = connect()
    aux.autocommit = True
    return aux.cursor()


def wal_lsn(cur):
    cur.execute("SELECT pg_current_wal_lsn()")
    return cur.fetchone()[0]


def wal_bytes_since(cur, start_lsn):
    cur.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)", (start_lsn,))
    return int(cur.fetchone()[0])


def relation_bytes(cur, name):
    cur.execute("SELECT pg_total_relation_size(to_regclass(%s))", (name,))
    return int(cur.fetchone()[0] or 0)


def default_source(cur):
    cur.execute(
        f"SELECT catalog_instance_id FROM {T}catalog_sources "
        "ORDER BY is_default DESC, catalog_instance_id LIMIT 1"
    )
    row = cur.fetchone()
    if row is None:
        sys.exit("no catalogue source: run scripts/perf/seed.py first")
    return row[0]


def pct(values, p):
    """Nearest-rank percentile (p in 0..100) of a non-empty sequence."""
    values = sorted(values)
    if not values:
        return None
    k = max(0, min(len(values) - 1, int(round(p / 100 * (len(values) - 1)))))
    return values[k]


def summary_ms(samples):
    if not samples:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "max_ms": None}
    return {
        "n": len(samples),
        "p50_ms": round(pct(samples, 50), 2),
        "p95_ms": round(pct(samples, 95), 2),
        "max_ms": round(max(samples), 2),
    }


def peak_rss_mb():
    """Peak resident set size of this process image, in MB.

    Uses ``VmHWM`` from ``/proc/self/status`` on Linux: ``ru_maxrss`` survives
    ``fork``+``exec`` and would report a larger parent's peak for a bench
    started as a subprocess.
    """
    try:
        with open("/proc/self/status") as handle:
            for line in handle:
                if line.startswith("VmHWM:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        pass
    import resource

    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def emit(result):
    """Print the bench result as one JSON line (run_baseline reads the last line)."""
    print(json.dumps(result, sort_keys=True), flush=True)


# ---- plugin.api host stub ---------------------------------------------------
plugin_module = types.ModuleType("plugin")
api = types.ModuleType("plugin.api")
api.config = types.SimpleNamespace(
    APP_VERSION="3.3.1",
    MEDIASERVER_TYPE="navidrome",
    DATABASE_URL=DATABASE_URL,
    DUPLICATE_DISTANCE_THRESHOLD_COSINE=0.01,
    CATALOGUE_ID_SCHEME_VERSION=4,
    DURATION_TOLERANCE_SECONDS=1.0,
    CHROMAPRINT_COLLECTION_ENABLED=True,
    CHROMAPRINT_GATE_ENABLED=True,
    CHROMAPRINT_MATCH_THRESHOLD=0.8,
    CHROMAPRINT_MIN_OVERLAP=10,
)
api.enqueue = lambda *a, **k: "job"
api.get_db = get_db
api.get_setting = lambda key, default=None: default
api.set_setting = lambda key, value: None
def _log_error(message, *args, **_kwargs):
    # Plugin code logs and falls back on errors; benches report these so a
    # fallback path is never mistaken for the production one.
    try:
        text = str(message) % args if args else str(message)
    except (TypeError, ValueError):
        text = str(message)
    exc = sys.exc_info()[1]
    STATE.logged_errors.append(f"{text}: {exc!r}" if exc else text)


api.logger = types.SimpleNamespace(
    warning=lambda *a, **k: None,
    exception=_log_error,
    info=lambda *a, **k: None,
    error=_log_error,
    debug=lambda *a, **k: None,
)
api.render_page = lambda body, title=None: body
api.table = lambda name: f"{T}{name}"
api.list_servers = lambda: [
    {"server_id": SERVER_ID, "name": "Main", "provider_type": "navidrome", "is_default": True}
]
api.active_server_id = lambda: SERVER_ID


@contextlib.contextmanager
def use_server(server_id):
    yield


api.use_server = use_server
plugin_module.api = api
sys.modules["plugin"] = plugin_module
sys.modules["plugin.api"] = api

# tasks.mediaserver.navidrome ping stub (upstream probe in /api/catalog/health)
tasks = types.ModuleType("tasks")
mediaserver = types.ModuleType("tasks.mediaserver")
navidrome = types.ModuleType("tasks.mediaserver.navidrome")


def _navidrome_request(endpoint, timeout=None):
    STATE.ping_calls += 1
    if PING_DELAY_S:
        time.sleep(PING_DELAY_S)
    return {"status": "ok", "serverVersion": "0.53.3 (abc)", "type": "navidrome"}


navidrome._navidrome_request = _navidrome_request
chromaprint = types.ModuleType("tasks.chromaprint")
chromaprint.chromaprints_agree = lambda a, b: True
tasks.mediaserver = mediaserver
tasks.chromaprint = chromaprint
mediaserver.navidrome = navidrome
sys.modules["tasks"] = tasks
sys.modules["tasks.mediaserver"] = mediaserver
sys.modules["tasks.mediaserver.navidrome"] = navidrome
sys.modules["tasks.chromaprint"] = chromaprint


def load_plugin():
    import importlib

    return importlib.import_module("plugins.LumaeAnalysis")


# Only queued work is stubbed while migrating. Every schedule helper runs for
# real, because several also create production tables and columns (e.g.
# ensure_catalog_reconcile_schedule -> migrate_reconcile -> reconcile_control);
# they write to the host ``cron`` table that host_schema.sql creates. Source
# discovery also stays real; it reads the stubbed ``api.list_servers``.
# Stubbed: ``enqueue_required_catalog_preparations`` (would enqueue jobs) and
# ``_safe_reconcile_schedule`` (adaptive reschedule from live state).
_MIGRATION_STUBS = ("_safe_reconcile_schedule",)


def run_plugin_migration(db):
    """Run the real ``plugins.LumaeAnalysis.migrate(db)`` with queued work stubbed."""
    mod = load_plugin()
    saved = []

    def patch(owner, name, value):
        saved.append((owner, name, getattr(owner, name)))
        setattr(owner, name, value)

    try:
        patch(mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0)
        for name in _MIGRATION_STUBS:
            patch(mod, name, lambda *_args, **_kwargs: None)
        mod.migrate(db)
        db.commit()
    finally:
        for owner, name, value in reversed(saved):
            setattr(owner, name, value)
    return mod
