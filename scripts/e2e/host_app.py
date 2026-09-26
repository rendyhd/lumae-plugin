"""Minimal AudioMuse-like host that serves the real plugin blueprint (P2-6).

``run_server_matrix.py`` starts this module under real gunicorn with the stock
AudioMuse topology (``--worker-class gthread --workers 1 --threads 4``, see
``supervisord.conf`` of the pinned host), so the plugin's routes run on four
request threads that share one Python process, as in production.

It reuses ``scripts/perf/stub_host.py`` (the ``plugin.api`` stub, the host's
per-connection options and ``DATABASE_URL``) and changes what a threaded
server needs:

* ``get_db`` is **per request** (``flask.g``, closed at app-context teardown),
  like AudioMuse ``database.get_db``; the perf stub shares one connection,
  which is not safe on four threads;
* the plugin logger writes to stderr instead of collecting errors;
* ``list_servers`` returns the servers named in ``LUMAE_E2E_SERVERS``;
* optional host authentication: with ``LUMAE_E2E_AUTH_TOKEN`` set, every
  plugin request needs ``Authorization: Bearer <token>`` (``g.auth_method =
  "bearer"``) and ``AUTH_ENABLED`` is true; otherwise requests are anonymous,
  like ``AUTH_ENABLED=false`` on the stock host.
* harness only: with ``LUMAE_E2E_TRUST_USER_HEADER=1`` the header
  ``X-E2E-User`` becomes ``g.auth_user``, so concurrent simulated devices
  count as different accounts for the v2 create rate limit (K4).

Environment: ``LUMAE_E2E_DSN`` (or ``LUMAE_PERF_DSN``) names the seeded
database. Start it with gunicorn, never with the Flask development server::

    gunicorn --chdir scripts/e2e --worker-class gthread --workers 1 --threads 4 \\
        --bind 127.0.0.1:18080 host_app:app
"""
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PERF = os.path.abspath(os.path.join(HERE, "..", "perf"))
if PERF not in sys.path:
    sys.path.insert(0, PERF)
if not os.environ.get("LUMAE_PERF_DSN") and os.environ.get("LUMAE_E2E_DSN"):
    os.environ["LUMAE_PERF_DSN"] = os.environ["LUMAE_E2E_DSN"]

import stub_host  # noqa: E402  (installs plugin.api before the plugin imports it)
from flask import Flask, abort, g, has_app_context, request  # noqa: E402

PREFIX = "/plugins/lumae_analysis"
AUTH_TOKEN = os.environ.get("LUMAE_E2E_AUTH_TOKEN") or ""
TRUST_USER_HEADER = os.environ.get("LUMAE_E2E_TRUST_USER_HEADER") == "1"
SERVERS = [s.strip() for s in os.environ.get("LUMAE_E2E_SERVERS", stub_host.SERVER_ID).split(",")
           if s.strip()]

logging.basicConfig(level=logging.WARNING, stream=sys.stderr,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
_log = logging.getLogger("lumae_e2e_host")
_fallback_db = None


def get_db():
    """One connection per request (app context), like AudioMuse's host."""
    global _fallback_db
    if has_app_context():
        db = g.get("_lumae_db")
        if db is None or db.closed:
            db = g._lumae_db = stub_host.connect()
        return db
    if _fallback_db is None or _fallback_db.closed:
        _fallback_db = stub_host.connect()
    return _fallback_db


api = stub_host.api
api.get_db = get_db
api.logger = _log
api.config.AUTH_ENABLED = bool(AUTH_TOKEN)
api.list_servers = lambda: [
    {"server_id": server_id, "name": "Main" if index == 0 else f"Server {index + 1}",
     "provider_type": "navidrome", "is_default": index == 0}
    for index, server_id in enumerate(SERVERS)
]
api.active_server_id = lambda: SERVERS[0]

plugin = stub_host.load_plugin()
app = Flask("lumae_e2e_host")
app.register_blueprint(plugin.bp, url_prefix=PREFIX)


@app.before_request
def _authenticate():
    if not request.path.startswith(PREFIX):
        return None
    if AUTH_TOKEN:
        header = request.headers.get("Authorization", "")
        if header != f"Bearer {AUTH_TOKEN}":
            abort(401)
        g.auth_method = "bearer"
    if TRUST_USER_HEADER and request.headers.get("X-E2E-User"):
        g.auth_user = request.headers["X-E2E-User"][:64]
    return None


@app.teardown_appcontext
def _close_db(_exc=None):
    db = g.pop("_lumae_db", None)
    if db is not None:
        try:
            db.close()
        except Exception:
            pass


@app.get("/e2e/ready")
def _ready():
    return {"ok": True, "pid": os.getpid()}
