import sys, types, json, importlib.util, numpy as np
pkg = types.ModuleType("la"); pkg.__path__ = ["plugins/LumaeAnalysis"]; sys.modules["la"] = pkg
spec = importlib.util.spec_from_file_location("la.edge_profiles", "plugins/LumaeAnalysis/edge_profiles.py")
m = importlib.util.module_from_spec(spec); sys.modules["la.edge_profiles"] = m; spec.loader.exec_module(m)
import inspect; print(inspect.signature(m.analyze_edge_blocks))
rng = np.random.default_rng(0)
for secs in (30, 240):
    sr = 44100
    x = (rng.standard_normal((2, sr*secs))*0.1).astype(np.float32)
    blocks = [x[:, i:i+65536] for i in range(0, x.shape[1], 65536)]
    try:
        p = m.analyze_edge_blocks(iter(blocks), sr, catalog_instance_id="c", track_id="t", media_revision="sha256:"+"a"*64, content_sha256="0"*64)
    except TypeError as e:
        print("sig mismatch", e); break
    s = json.dumps(p, separators=(",",":"))
    print(secs, "s ->", len(s), "bytes;", {k: len(json.dumps(v)) for k,v in p.items() if len(json.dumps(v))>500})
