"""A CPU-bound stand-in for the waveform analyzer (``parallel_bench.py``).

Runs inside the analysis child, which imports it by module name through the
parent's ``sys.path``. It burns CPU for ``burn_seconds`` (about one real
waveform analysis), then returns a real profile from the plugin's own block
analyzer over a short tone, so publication sees the production result type.
"""
import sys
import time

import numpy as np


def cpu_bound(path, burn_seconds=0.6, deadline_seconds=None):
    end = time.process_time() + burn_seconds
    value = 0.0
    while time.process_time() < end:
        value += sum(i * i for i in range(2000)) * 1e-12
    loudness = sys.modules.get("_lumae_analysis_child.loudness")
    if loudness is None:
        from plugins.LumaeAnalysis import loudness
    tone = (0.1 * np.sin(2 * np.pi * 440 * np.arange(48000) / 48000)).astype(np.float32)[None, :]
    return loudness.analyze_blocks(iter([tone] * 4), 48000, channel_count=1)
