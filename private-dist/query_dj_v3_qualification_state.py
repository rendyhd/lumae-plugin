import importlib
import json

from flask_app import app
from plugin.api import NAMESPACE
from plugin.manager import plugin_manager

plugin_manager.setup_namespace()
plugin_manager.sync(role="web")
plugin_manager.ensure_requirements(role="web")
plugin_manager.load("web", flask_app=app)
importlib.import_module(f"{NAMESPACE}.lumae_analysis")

with app.app_context():
    from database import connect_raw
    db = connect_raw()
    cursor = db.cursor()
    cursor.execute("""SELECT catalog_instance_id, COUNT(*)
        FROM plugin_lumae_analysis__dj_analyses GROUP BY catalog_instance_id
        ORDER BY COUNT(*) DESC""")
    v2_by_catalog = cursor.fetchall()
    cursor.execute("""SELECT status, request_tier, priority, COUNT(*)
        FROM plugin_lumae_analysis__dj_analysis_jobs_v3
        GROUP BY status, request_tier, priority
        ORDER BY priority DESC, status""")
    v3_jobs = cursor.fetchall()
    cursor.execute("""SELECT COUNT(*), COUNT(DISTINCT track_id)
        FROM plugin_lumae_analysis__dj_analyses_v3""")
    v3_total, v3_tracks = cursor.fetchone()
    cursor.execute("""SELECT COALESCE(error_code, 'none'), COUNT(*)
        FROM plugin_lumae_analysis__dj_analysis_jobs_v3
        WHERE status IN ('failed', 'unsupported')
        GROUP BY error_code ORDER BY error_code""")
    terminal_reasons = cursor.fetchall()
    cursor.close()
    db.close()

print(json.dumps({
    "v2_catalogs": [
        {"ordinal": index + 1, "count": int(count)}
        for index, (_catalog_id, count) in enumerate(v2_by_catalog)
    ],
    "v3_jobs": [
        {
            "status": status,
            "priority_tier": request_tier,
            "priority": int(priority),
            "count": int(count),
        }
        for status, request_tier, priority, count in v3_jobs
    ],
    "v3_analysis_rows": int(v3_total),
    "v3_distinct_tracks": int(v3_tracks),
    "terminal_reasons": {
        str(reason): int(count) for reason, count in terminal_reasons
    },
}, sort_keys=True))
