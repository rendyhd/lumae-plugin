import sys, types, json, gzip, importlib.util, numpy as np, time
from scipy import signal
pkg = types.ModuleType("la"); pkg.__path__ = ["plugins/LumaeAnalysis"]; sys.modules["la"] = pkg
spec = importlib.util.spec_from_file_location("la.edge_profiles", "plugins/LumaeAnalysis/edge_profiles.py")
m = importlib.util.module_from_spec(spec); sys.modules["la.edge_profiles"] = m; spec.loader.exec_module(m)
rng = np.random.default_rng(7); sr = 44100
payloads = []
t = time.time()
for i in range(40):
    secs = int(rng.integers(90, 400))
    # music-like: filtered noise with a slow envelope, fade-in/out, varied level
    n = sr * secs
    x = signal.lfilter([1], [1, -0.97], rng.standard_normal(n)) * 0.02 * 10 ** (rng.uniform(-12, 0) / 20)
    env = 0.6 + 0.4 * np.sin(np.linspace(0, rng.uniform(5, 60), n)) ** 2
    fade = np.minimum(1, np.minimum(np.arange(n), n - np.arange(n)) / (sr * rng.uniform(0.5, 8)))
    y = (x * env * fade).astype(np.float32)
    st = np.vstack([y, np.roll(y, int(rng.integers(1, 200)))])
    blocks = [st[:, j:j + 65536] for j in range(0, n, 65536)]
    p = m.analyze_edge_blocks(iter(blocks), sr, catalog_instance_id="c", track_id=f"t{i}",
                              media_revision="sha256:" + f"{i:064x}", content_sha256=f"{i:064x}")
    payloads.append(p)
print(f"generated {len(payloads)} in {time.time()-t:.1f}s")
def enc(p, drop):
    q = json.loads(json.dumps(p))
    if drop:
        for w in ("head", "tail"): q[w].pop("boundaries", None)
    return json.dumps(q, separators=(",", ":"))
for drop in (False, True):
    page = ("[" + ",".join(enc(p, drop) for p in payloads) + "]").encode()
    per_raw = len(page) / len(payloads); per_gz = len(gzip.compress(page, 6)) / len(payloads)
    print(f"{'no boundaries' if drop else 'current     '}: raw {per_raw:6.0f} B/track, gzip {per_gz:6.0f} B/track -> 94k: raw {per_raw*94e3/1e9:.2f} GB, gzip {per_gz*94e3/1e9:.2f} GB")
