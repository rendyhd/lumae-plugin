import importlib
import json

from flask_app import app
from plugin.api import NAMESPACE
from plugin.manager import plugin_manager

plugin_manager.setup_namespace()
plugin_manager.sync(role="web")
plugin_manager.ensure_requirements(role="web")
plugin_manager.load("web", flask_app=app)
module = importlib.import_module(f"{NAMESPACE}.lumae_analysis")

with app.app_context():
    db = module.get_db()
    cursor = db.cursor()
    cursor.execute("""SELECT catalog_instance_id FROM plugin_lumae_analysis__dj_analyses
        GROUP BY catalog_instance_id ORDER BY COUNT(*) DESC LIMIT 1""")
    row = cursor.fetchone()
    if not row:
        raise RuntimeError("No V2 qualification catalogue is available")
    catalog_id = row[0]
    cursor.execute("""SELECT track_id FROM plugin_lumae_analysis__dj_analyses
        WHERE catalog_instance_id=%s ORDER BY track_id LIMIT 110""", (catalog_id,))
    ids = [item[0] for item in cursor.fetchall()]
    cursor.close()
    source = module.resolve_profile_source(catalog_instance_id=catalog_id)
    totals = {"requested": len(ids), "accepted": 0, "promoted": 0,
              "already_ready": 0, "already_queued": 0, "deferred_batches": 0}
    for offset in range(0, len(ids), 100):
        result, status = module._request_dj_v3_jobs(
            source, ids[offset:offset + 100], priority_tier="lookahead"
        )
        if status != 202:
            raise RuntimeError(f"V3 qualification enqueue failed with status {status}: {result.get('reason')}")
        for key in ("accepted", "promoted", "already_ready", "already_queued"):
            totals[key] += len(result.get(key) or [])
        totals["deferred_batches"] += int(bool(result.get("deferred")))

print(json.dumps(totals, sort_keys=True))
