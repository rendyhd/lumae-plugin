"""One child call, three results (performance plan 2026-10-16, WP2 phase A).

``analyze_track`` runs in the analysis child on one downloaded file: the
waveform profile (``loudness.analyze_file``), then, with edges on, the edge
profile with its BS.1770 loudness from the same decode
(``loudness_v1.analyze_edge_file``). When the edge analyzer refuses the file
(more than two channels, unnamed WAV layouts, DSF above 384 kHz) the
standalone meter measures its loudness instead (``loudness_v1.measure_file``).
Each analysis decodes the file itself, so every result is the one its own
call would give; only the download and the child start are shared.

Each part answers on its own: ``{"result": ...}`` or ``{"error": ...}``, the
error described as the child describes a failed call (category, exception
class, safe diagnostics, an edge refusal), so one part failing never loses
the others. Each analysis gets ``deadline_seconds`` divided by the number of
analyses as its soft deadline; the caller's hard limit covers them all.

Runs in the child: imports only the analyzers and the isolation module, never
the plugin package.
"""

from . import analysis_isolation


def _parts(edge):
    return 2 if edge else 1


def limit_for(edge, per_analysis_seconds):
    """The call's soft deadline: each analysis keeps the per-file setting."""
    return per_analysis_seconds * _parts(edge)


def analyze_track(path, *, edge=False, catalog_instance_id=None, track_id=None, media_revision=None,
                  deadline_seconds=900, observer=None):
    """``{"waveform": part, "edge": part, "loudness": part}``; ``edge`` and
    ``loudness`` only when asked for and when needed."""
    from . import loudness, loudness_v1

    share = float(deadline_seconds) / _parts(edge)
    answer = {"waveform": _part(observer, loudness.analyze_file, path, deadline_seconds=share,
                                observer=observer)}
    if not edge:
        return answer
    answer["edge"] = _part(observer, loudness_v1.analyze_edge_file, path,
                           catalog_instance_id=catalog_instance_id, track_id=track_id,
                           media_revision=media_revision, deadline_seconds=share, observer=observer)
    if answer["edge"].get("error", {}).get("refusal"):
        answer["loudness"] = _part(observer, loudness_v1.measure_file, path, deadline_seconds=share,
                                   observer=observer)
    return answer


def _part(probe, analyzer, path, **kwargs):
    try:
        return {"result": analyzer(path, **kwargs)}
    except MemoryError:
        raise  # the process may be unusable: the whole call fails as a crash
    except Exception as exc:  # noqa: BLE001 - described, not raised
        snapshot = probe.snapshot() if hasattr(probe, "snapshot") else {}
        return {"error": analysis_isolation.describe_failure(exc, snapshot)}


def part_value(part):
    """A part's result, or raise its failure as ``IsolatedAnalysisError`` (parent side)."""
    if part is None:
        return None
    if "error" in part:
        raise analysis_isolation.failure_from(part["error"])
    return part.get("result")
