"""Register the mounted Lumae plugin with the stock AudioMuse host (P2-6 gate).

Runs **inside the pinned AudioMuse image** (``docker compose run --rm
lumae-install <mode>``), with the plugin source bind-mounted at
``/app/plugin/installed/lumae_analysis``:

``init``
    Create the host schema: ``database.init_db()``, exactly what the Flask app
    does when it starts. Run it first on an empty database, then seed with
    ``seed_representative.py --host-schema audiomuse``.
``register`` (default)
    ``init``, then upsert the ``plugins`` row the host's plugin loader reads
    (no checksum, no source URL: the code is the bind mount, so the loader
    never re-downloads it) and run the plugin's install hooks through the
    host's ``PluginManager.run_install_hooks`` (the plugin's ``migrate``), as a
    catalogue install would. Restart the Flask service afterwards so the web
    process loads the blueprint.

The host then serves the plugin at ``/plugins/lumae_analysis`` under its own
supervisord + gunicorn (gthread, 1 worker x 4 threads).
"""
import json
import os
import re
import sys
import time

sys.path.insert(0, "/app")
os.chdir("/app")

PLUGIN_ID = "lumae_analysis"
PLUGIN_DIR = f"/app/plugin/installed/{PLUGIN_ID}"


def wait_for_database(database, seconds=120):
    deadline = time.monotonic() + seconds
    while True:
        try:
            database.connect_raw().close()
            return
        except Exception as exc:  # noqa: BLE001
            if time.monotonic() > deadline:
                raise SystemExit(f"database not reachable: {type(exc).__name__}")
            time.sleep(1)


def manifest():
    with open(os.path.join(PLUGIN_DIR, "plugin.json")) as handle:
        catalog = json.load(handle)
    with open(os.path.join(PLUGIN_DIR, "__init__.py")) as handle:
        version = re.search(r'^PLUGIN_VERSION = "([^"]+)"', handle.read(), re.M).group(1)
    latest = (catalog.get("versions") or [{}])[0]
    return {
        "id": catalog["id"], "name": catalog.get("name") or catalog["id"],
        "author": catalog.get("author"), "description": catalog.get("description"),
        "version": version, "min_core_version": latest.get("min_core_version"),
        "requirements": catalog.get("requirements") or [],
        "capabilities": catalog.get("capabilities") or {},
    }


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "register"
    if mode not in ("init", "register"):
        raise SystemExit("usage: install_plugin.py [init|register]")
    import database
    from flask_app import app

    wait_for_database(database)
    with app.app_context():
        database.init_db()
    print("host schema ready", flush=True)
    if mode == "init":
        return
    info = manifest()
    database.ensure_plugins_table()
    conn = database.connect_raw()
    try:
        database.upsert_plugin(PLUGIN_ID, info["name"], info["version"], info, None, None,
                               info["requirements"], None, conn=conn)
    finally:
        conn.close()
    from plugin.manager import plugin_manager

    # The host installs from a request (an app context is active); the
    # plugin's register() reads its settings through the host database.
    with app.app_context():
        plugin_manager.run_install_hooks(PLUGIN_ID)
    print(f"registered {PLUGIN_ID} {info['version']} from {PLUGIN_DIR}", flush=True)


if __name__ == "__main__":
    main()
