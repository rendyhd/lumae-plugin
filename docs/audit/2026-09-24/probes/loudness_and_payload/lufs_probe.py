import sys, types, numpy as np
sys.path.insert(0, "plugins/LumaeAnalysis")
# load loudness.py standalone (it only imports ramp_codec relatively)
import importlib.util
pkg = types.ModuleType("la"); pkg.__path__ = ["plugins/LumaeAnalysis"]; sys.modules["la"] = pkg
spec = importlib.util.spec_from_file_location("la.loudness", "plugins/LumaeAnalysis/loudness.py")
m = importlib.util.module_from_spec(spec); sys.modules["la.loudness"] = m; spec.loader.exec_module(m)
import pyloudnorm as pyln
from scipy import signal
rng = np.random.default_rng(1)
def pink(n):
    w = rng.standard_normal(n); b,a = [0.049922035,-0.095993537,0.050612699,-0.004408786],[1,-2.494956002,2.017265875,-0.522189400]
    return signal.lfilter(b,a,w)
def run(label, x, sr):
    ours = m.analyze_buffer(x, sr).ref_lufs
    ref = pyln.Meter(sr).integrated_loudness(x.T if x.ndim==2 else x)
    print(f"{label:48s} sr={sr:6d} analyzer={ours:7.2f}  BS.1770={ref:7.2f}  diff={ours-ref:+.2f} dB")
for sr in (44100, 48000, 96000, 192000):
    base = pink(sr*20); base /= np.max(np.abs(base))*4
    run("stereo pink noise (identical L/R)", np.vstack([base, base]), sr)
sr=48000
base = pink(sr*20); base /= np.max(np.abs(base))*4
run("mono pink noise", base[None,:], sr)
# quiet passage: 20s loud + 40s at -30 dB
quiet = np.concatenate([base, pink(sr*40)/np.max(np.abs(base))/4*10**(-30/20)])
run("stereo 20s loud + 40s at -30 dB (gating)", np.vstack([quiet, quiet]), sr)
tone = 0.25*np.sin(2*np.pi*60*np.arange(sr*20)/sr)
run("stereo 60 Hz tone", np.vstack([tone,tone]), sr)
tone = 0.25*np.sin(2*np.pi*60*np.arange(96000*20)/96000)
run("stereo 60 Hz tone", np.vstack([tone,tone]), 96000)
