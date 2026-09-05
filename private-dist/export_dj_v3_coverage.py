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
    cursor.execute("""SELECT analysis.payload, edge.payload IS NOT NULL
        FROM plugin_lumae_analysis__dj_analyses_v3 analysis
        JOIN plugin_lumae_analysis__source_profiles source
          ON source.catalog_instance_id=analysis.catalog_instance_id
         AND source.track_id=analysis.track_id
         AND source.media_signature=analysis.media_signature
        LEFT JOIN LATERAL (
            SELECT value.payload
            FROM plugin_lumae_analysis__edge_profiles value
            WHERE value.catalog_instance_id=source.catalog_instance_id
              AND value.track_id=source.track_id
              AND value.media_signature=source.media_signature
            ORDER BY value.updated_at DESC, value.profile_digest LIMIT 1
        ) edge ON TRUE
        ORDER BY analysis.track_id""")
    payloads = cursor.fetchall()
    cursor.close()
    db.close()

analyses = []
for index, (payload, edge_ready) in enumerate(payloads):
    alias = f"track-{index + 1:03d}"
    rhythm = payload.get("rhythm") if isinstance(payload.get("rhythm"), dict) else {}
    regions = []
    for region in rhythm.get("regions") or []:
        if not isinstance(region, dict):
            continue
        regions.append({
            "normalized_tempo_bpm": region.get("normalized_tempo_bpm"),
            "confidence_tier": region.get("confidence_tier"),
            "rejection_reasons": list(region.get("rejection_reasons") or []),
        })
    cues = []
    for cue in payload.get("cue_candidates") or []:
        if not isinstance(cue, dict):
            continue
        cues.append({
            "role": cue.get("role"),
            "confidence_tier": cue.get("confidence_tier"),
        })
    vocal = payload.get("vocal_risk") if isinstance(payload.get("vocal_risk"), dict) else {}
    calibration = vocal.get("calibration") if isinstance(vocal.get("calibration"), dict) else {}
    analyses.append({
        "schema_version": payload.get("schema_version"),
        "track_id": alias,
        "rhythm": {"regions": regions},
        "cue_candidates": cues,
        "vocal_risk": {"calibration": {
            "status": calibration.get("status"),
            "cuts_authorized": calibration.get("cuts_authorized"),
        }},
        "edge_profile_ready": bool(edge_ready),
    })

ids = [item["track_id"] for item in analyses]
manifest = {
    "pairs": [{"from": left, "to": right} for left, right in zip(ids, ids[1:])],
    "orders": [],
}
print(json.dumps({"analyses": analyses, "manifest": manifest}, sort_keys=True))
