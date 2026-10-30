"""Lumae's fixed sonic ranking tables, for album and artist relationships.

AudioMuse's energy and moods are not 0-1 probabilities: v3.4.0 energy puts a
typical song near 0.74, and CLAP gives all six moods of a song nearly the same
score. The phone ranks both through fixed tables (phase SC), and its Album
Dynamics fingerprints (``lumae_album_dynamics_v2``) use those ranks. This module
gives the relationship build the same values:

- ``sonic_calibration_v2.json`` copies ``energy.v34``, ``moods.clap`` and
  ``moods.classifier`` from the app's ``src/constants/sonicCalibration.ts``
  (its SHA-256 is ``sourceSha256``);
- ``rank_of`` is the app's ``rankOf``;
- ``energy_scale`` and ``normalize_energy`` are the app's ``energyCalibration``
  and ``normalizeEnergy`` (``albumDynamicsEnergy``);
- ``mood_ranks`` is the app's ``writeMoodFeatureSlots``.

The app's golden fixtures (``tests/plugins/album_dynamics_*_golden.json``) pin
the values.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np

CALIBRATION_FILE = "sonic_calibration_v2.json"
MOOD_NAMES = ("danceable", "aggressive", "happy", "party", "relaxed", "sad")

# The app's energyCalibration and resolveEnergyScale.
MIN_TRACKS_TO_CALIBRATE = 20
ENERGY_V34_MEDIAN_MIN = 0.3
FALLBACK_ENERGY_SCALE = {"kind": "range", "min": 0.01, "max": 0.15}

_CALIBRATION = json.loads(
    (Path(__file__).resolve().parent / CALIBRATION_FILE).read_text(encoding="utf-8")
)
CALIBRATION_VERSION = _CALIBRATION["version"]
ENERGY_V34 = _CALIBRATION["energy"]["v34"]
MOOD_TABLES = _CALIBRATION["moods"]


def rank_of(table, value):
    """A value's rank: 0 below the first point, 1 above the last, linear
    between, rounded to 1e-6 as JavaScript's Math.round does (Python's round()
    rounds half to even)."""
    if value is None or not math.isfinite(value) or len(table) < 2:
        return None
    if value <= table[0][0]:
        return 0.0
    if value >= table[-1][0]:
        return 1.0
    lo, hi = 0, len(table) - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if table[mid][0] <= value:
            lo = mid
        else:
            hi = mid
    v0, r0 = table[lo]
    v1, r1 = table[hi]
    if value == v0:
        return r0
    return math.floor((r0 + ((value - v0) * (r1 - r0)) / (v1 - v0)) * 1e6 + 0.5) / 1e6


def raw_energy(value):
    """AudioMuse's stored energy as a finite float, or None."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def energy_scale(raw_values):
    """The library's energy scale from its songs' raw energies.

    A median above 0.3 means AudioMuse's v3.4.0 formula, ranked through the
    fixed table, so libraries compare. An older library maps against its own
    p5/p95 (each the value at ``floor(n * q)`` of the sorted energies). Fewer
    than 20 songs, or no spread, keep the theoretical 0.01-0.15.
    """
    values = sorted(value for value in raw_values if value is not None and math.isfinite(value))
    count = len(values)
    if count < MIN_TRACKS_TO_CALIBRATE:
        return dict(FALLBACK_ENERGY_SCALE)
    p5 = values[math.floor(count * 0.05)]
    p50 = values[math.floor(count * 0.5)]
    p95 = values[math.floor(count * 0.95)]
    if p50 > ENERGY_V34_MEDIAN_MIN:
        return {"kind": "v34"}
    if p95 > p5:
        return {"kind": "range", "min": p5, "max": p95}
    return dict(FALLBACK_ENERGY_SCALE)


def normalize_energy(raw, scale):
    """A song's energy, 0-1, on its library's scale; None when it has none."""
    if raw is None or not math.isfinite(raw):
        return None
    if scale["kind"] == "v34":
        return rank_of(ENERGY_V34, raw)
    span = scale["max"] - scale["min"]
    if span <= 0:
        return 0.0
    return max(0.0, min(1.0, (raw - scale["min"]) / span))


def _mood_scores(other_features):
    """The six moods of ``other_features`` (text or a dict). A pair splits at
    its last ':', the label is trimmed, an unparseable or non-finite score is
    missing, and the last valid occurrence of a label wins. Genre tags
    (``mood_vector``) never stand in for a mood."""
    if isinstance(other_features, dict):
        pairs = [(str(label), value) for label, value in other_features.items()]
    else:
        pairs = []
        for part in str(other_features or "").split(","):
            label, separator, value = part.rpartition(":")
            if separator:
                pairs.append((label, value))
    scores = {}
    for label, value in pairs:
        label = label.strip()
        if label not in MOOD_NAMES:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            scores[label] = number
    return scores


def mood_ranks(other_features):
    """A song's six moods as ranks, float32 values as the app stores them, 0
    where a mood is missing. CLAP's table when all six are within 0.40-0.80,
    the pre-0.9 classifier's otherwise."""
    scores = _mood_scores(other_features)
    values = [0.0] * len(MOOD_NAMES)
    if not scores:
        return values
    clap = all(name in scores and 0.4 <= scores[name] <= 0.8 for name in MOOD_NAMES)
    tables = MOOD_TABLES["clap" if clap else "classifier"]
    for index, name in enumerate(MOOD_NAMES):
        rank = rank_of(tables[name], scores.get(name))
        if rank is not None:
            values[index] = float(np.float32(rank))
    return values
