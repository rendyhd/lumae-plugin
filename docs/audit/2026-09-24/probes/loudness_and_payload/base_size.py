import sys, types, json, importlib.util, numpy as np, base64, datetime
pkg = types.ModuleType("la"); pkg.__path__ = ["plugins/LumaeAnalysis"]; sys.modules["la"] = pkg
spec = importlib.util.spec_from_file_location("la.loudness", "plugins/LumaeAnalysis/loudness.py")
m = importlib.util.module_from_spec(spec); sys.modules["la.loudness"] = m; spec.loader.exec_module(m)
rng = np.random.default_rng(0); sr=44100
x = np.vstack([rng.standard_normal(sr*240)*0.1]*2)
r = m.analyze_buffer(x, sr)
payload = {"track_id":"tr-000000000000000000000000","source":"waveform","sample_rate":sr,"duration_ms":r.duration_ms,"ref_lufs":r.ref_lufs,
 "start_ramp":base64.b64encode(r.start_ramp_blob).decode(),"end_ramp":base64.b64encode(r.end_ramp_blob).decode(),"analyzer_ver":1,
 "analyzed_at":"2026-09-24T00:00:00Z","media_signature":"sha256:"+"a"*64,"media_revision":"sha256:"+"a"*64}
print(len(json.dumps(payload,separators=(",",":"))), "bytes without edge")
