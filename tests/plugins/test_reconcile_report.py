"""What a runner turn did, per action (runner plan R3; plan S5's shapes).

``TurnReport`` keeps one entry per action, in the order they first ran, with
its runs, done and failed counts, its last status and its time.
"""
from test_lumae_analysis import load_plugin  # noqa: F401  (installs the host stubs)
from plugins.LumaeAnalysis import reconcile  # noqa: E402


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
    report = reconcile.TurnReport()
    for action, result, _counts in shapes:
        report.add(action, result, 1234)
    expected = {}
    for action, _result, (done, failed) in shapes:
        runs, total_done, total_failed = expected.get(action, (0, 0, 0))
        expected[action] = (runs + 1, total_done + done, total_failed + failed)
    # One entry per action, in the order they first ran.
    assert [(entry["action"], (entry["runs"], entry["count"], entry["failed"]))
            for entry in report.actions] == list(expected.items())
    assert all(entry["ms"] == 1234 * entry["runs"] for entry in report.actions)
    assert report.actions[-1]["status"] == "queued" and report.actions[-2]["status"] == (
        "waiting_retry")


def test_a_status_is_kept_short_and_a_missing_result_counts_as_complete():
    report = reconcile.TurnReport()
    report.add("credits", {"status": "x" * 200}, 5)
    report.add("relationships", None, 5)
    assert report.actions[0]["status"] == "x" * 40
    assert report.actions[1]["status"] == "complete"
