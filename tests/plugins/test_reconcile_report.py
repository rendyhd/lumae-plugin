"""The watchdog tick's report (plan S5, AUD-18): the host's task details.

AudioMuse takes ``status_message``, then ``message``, as the task's message
and, with AM-2, as its Recent tasks note; without AM-2 the note reads
``Songs analyzed: N`` from ``tracks_analyzed``.
"""
import json

from test_lumae_analysis import load_plugin  # noqa: F401  (installs the host stubs)
from plugins.LumaeAnalysis import reconcile  # noqa: E402


def _report(*actions, remaining=None):
    report = reconcile.TickReport()
    for action, result in actions:
        report.add(action, result, 1234)
    report.remaining = remaining
    report.cadence = "* * * * *"
    return report


def test_every_action_shape_produces_the_fields():
    shapes = [
        ("interactive", {"status": "served", "served": 2}, (2, 0)),
        ("analysis_run", {"status": "complete"}, (1, 0)),
        ("catalog_preparation", {"status": "ready"}, (1, 0)),
        ("credits", {"status": "published", "album_id": "a"}, (1, 0)),
        ("credits", {"status": "failed", "album_id": "b"}, (0, 1)),
        ("music_metadata", {"status": "verified", "job_id": "j"}, (1, 0)),
        ("relationships", {"status": "queued"}, (1, 0)),
        ("profile_backfill", {"status": "queued", "ready": 7, "failed": 1}, (7, 1)),
        ("edge_backfill", {"status": "queued", "processed": 3, "ready": 2, "failed": 1}, (2, 1)),
        ("profile_backfill", {"status": "waiting_retry", "processed": 0}, (0, 0)),
    ]
    report = _report(*[(action, result) for action, result, _counts in shapes],
                     remaining={"waveforms": 41200, "edge_passes": 1, "relationships": 0,
                                "credits_albums": 3, "lookups": 0})
    result = report.result("processed")
    expected = {}
    for action, _result, (done, failed) in shapes:
        runs, total_done, total_failed = expected.get(action, (0, 0, 0))
        expected[action] = (runs + 1, total_done + done, total_failed + failed)
    # One entry per action, in the order they first ran.
    assert [(entry["action"], (entry["runs"], entry["count"], entry["failed"]))
            for entry in result["actions"]] == list(expected.items())
    assert all(entry["ms"] == 1234 * entry["runs"] for entry in result["actions"])
    assert result["actions"][-1]["status"] == "queued" and result["actions"][-2]["status"] == (
        "waiting_retry")
    # Waveforms, edges and on-demand tracks are the tracks analysed.
    assert result["tracks_analyzed"] == 2 + 7 + 2
    assert set(result) >= {"status", "message", "status_message", "tracks_analyzed", "actions",
                           "remaining", "yielded_to", "cadence", "requested"}
    assert result["message"] == result["status_message"]
    assert len(result["message"]) <= 100


def test_the_sentence_counts_each_action_once_in_report_order():
    report = _report(("credits", {"status": "published"}),
                     ("profile_backfill", {"status": "queued", "ready": 4}),
                     ("credits", {"status": "published"}),
                     ("profile_backfill", {"status": "queued", "ready": 3}),
                     remaining={"waveforms": 41200})
    message = report.message("processed")
    assert message.startswith("Lumae: 7 waveforms, 2 credits albums in ")
    assert message.endswith(" s; 41,200 tracks left")


def test_yield_idle_paused_and_same_dispatch_sentences():
    report = _report()
    report.yielded_to = "main_analysis"
    assert report.message("yielded") == "Lumae: yielded to main_analysis"
    assert _report().message("current") == "Lumae: up to date"
    assert _report(remaining={"waveforms": 12}).message("current") == (
        "Lumae: nothing ready; 12 tracks left")
    assert _report().message("paused") == "Lumae: paused"
    assert _report().message("same_dispatch") == "Lumae: ran earlier in this task"


def test_the_sentence_never_exceeds_a_hundred_characters():
    report = _report(*[(action, {"status": "queued", "ready": 123456, "failed": 7890,
                                 "served": 123456})
                       for action in reconcile.ACTION_UNITS],
                     remaining={"waveforms": 9876543})
    message = report.message("failed")
    assert len(message) <= 100 and message.endswith("…")
    shorter = _report(("profile_backfill", {"status": "queued", "ready": 12345, "failed": 678}),
                      ("edge_backfill", {"status": "queued", "ready": 12345, "failed": 678}),
                      ("credits", {"status": "published"}),
                      remaining={"waveforms": 9876543})
    # The tail goes first.
    message = shorter.message("processed")
    assert message.startswith("Lumae: 12,345 waveforms (678 failed), 12,345 edges (678 failed), "
                              "1 credits album in ")
    assert message.endswith(" s") and len(message) <= 100


def test_the_details_stay_json_and_under_two_kilobytes():
    report = _report(*[(action, {"status": "x" * 200, "ready": 10 ** 9, "failed": 10 ** 9,
                                 "served": 10 ** 9})
                       for action in reconcile.ACTION_UNITS for _ in range(2)],
                     remaining={key: 10 ** 9 for key in (
                         "waveforms", "edge_passes", "relationships", "credits_albums",
                         "lookups")})
    report.yielded_to = "plugin.some_other_plugin.with_a_long_task_type"
    result = report.result("processed", plugin_version="1.6.2", catalog_builder_version=5)
    encoded = json.dumps(result)
    assert len(encoded.encode()) < 2048, len(encoded)
