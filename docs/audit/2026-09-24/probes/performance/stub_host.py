"""Minimal AudioMuse host stub for synthetic performance measurement (not a test)."""
import contextlib
import os
import sys
import time
import types

import psycopg2

REPO = "/home/user/lumae-plugin"
DSN = os.environ.get("AUDIT_DSN", "host=127.0.0.1 port=55433 user=postgres dbname=audit_perf")
# Mirror the host's per-connection options (database.py _CONNECT_OPTIONS).
HOST_OPTIONS = "-c statement_timeout=600000 -c max_parallel_workers_per_gather=0"
SERVER_ID = "server-a"
PING_DELAY_S = float(os.environ.get("PING_DELAY_S", "0.0"))

sys.path.insert(0, REPO)

STATE = types.SimpleNamespace(db=None, statements=0, ping_calls=0)


def connect():
    return psycopg2.connect(DSN, options=HOST_OPTIONS)


class CountingCursor(psycopg2.extensions.cursor):
    def execute(self, query, vars=None):
        STATE.statements += 1
        return super().execute(query, vars)

    def executemany(self, query, vars_list):
        for v in vars_list:
            STATE.statements += 1
            super().execute(query, v)


def counting_connect():
    return psycopg2.connect(DSN, options=HOST_OPTIONS, cursor_factory=CountingCursor)


def get_db():
    if STATE.db is None or STATE.db.closed:
        STATE.db = counting_connect()
    return STATE.db


plugin_module = types.ModuleType("plugin")
api = types.ModuleType("plugin.api")
api.config = types.SimpleNamespace(
    APP_VERSION="3.3.1",
    MEDIASERVER_TYPE="navidrome",
    DATABASE_URL=DSN + " options='" + HOST_OPTIONS + "'",
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
api.logger = types.SimpleNamespace(
    warning=lambda *a, **k: None,
    exception=lambda *a, **k: None,
    info=lambda *a, **k: None,
    error=lambda *a, **k: None,
    debug=lambda *a, **k: None,
)
api.render_page = lambda body, title=None: body
api.table = lambda name: f"plugin_lumae_analysis__{name}"
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
