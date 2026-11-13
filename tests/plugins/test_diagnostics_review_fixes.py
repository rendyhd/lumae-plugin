"""Review fixes of the operator diagnostics (no database): the settings page, the
database-state page and error-text redaction."""

import importlib
import importlib.util
import pathlib
import types

import pytest

from test_lumae_analysis import load_plugin, settings_catalog_source

_SPEC = importlib.util.spec_from_file_location(
    "redaction_review_fixes",
    pathlib.Path(__file__).resolve().parents[2]
    / "plugins" / "LumaeAnalysis" / "redaction.py",
)
redaction = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(redaction)
redact = redaction.redact_error_text


# ---------------------------------------------------------------------------
# Finding 8: redaction
# ---------------------------------------------------------------------------

LEAKS = [
    ("password=)hunter2", "hunter2"),
    ("password=]hunter2", "hunter2"),
    ("password=ab,cd", "cd"),
    ("password=ab;cdEF", "cdEF"),
    ("postgresql://lumae:p@ssw0rd@db/x", "ssw0rd"),
    ("postgresql://lumae:pa/ss@db/x", "ss@"),
    ("postgresql://lumae:pa?ss@db/x", "pa?ss"),
    ("postgresql://lumae:pa#ss@db/x", "pa#ss"),
    ("{'u': 'admin', 'p': 'hunter2'}", "hunter2"),
    ("{'t': 'tok123', 's': 'salt456'}", "tok123"),
    ("{'t': 'tok123', 's': 'salt456'}", "salt456"),
    ('{"p": "hunter2"}', "hunter2"),
    ("p=hunter2&u=admin", "hunter2"),
    ("--password hunter2", "hunter2"),
    ("--token abc", "abc"),
    ("/music/Artist/Album (Deluxe)/01 - Song Title.flac: Invalid data", "Deluxe"),
    ("/music/Artist/Album (Deluxe)/01 - Song Title.flac: Invalid data", "Song Title"),
    ("input:/mnt/music/a.flac", "/mnt"),
    ("C:/Users/x/a.flac", "Users"),
    ("./music/A/a.flac", "music"),
]


@pytest.mark.parametrize("text,secret", LEAKS)
def test_credential_and_path_shapes_are_masked(text, secret):
    once = redact(text)
    assert secret not in once
    assert redact(once) == once  # idempotent


def test_database_host_and_prose_survive():
    assert redact("postgresql://lumae:p@ssw0rd@db/x") == "postgresql://[redacted]@db/x"
    assert redact("password=ab,cd") == "password=[redacted]"
    assert redact("(password=abc) failed") == "(password=[redacted]) failed"
    assert redact("/music/Artist/Album (Deluxe)/01 - Song Title.flac: Invalid data") == (
        "[path]: Invalid data"
    )
    assert redact("input:/mnt/music/a.flac") == "input:[path]"


@pytest.mark.parametrize(
    "text",
    [
        "p=0.05 and the t test",
        "http://host:8080/x?e=a@b.com",
        "https://example.test/a/b?x=1",
        "failed to read (exit 1).",
        "--token is required",
        "the s and p keys",
        "user u=admin",
        "{'u': 'admin'}",
        "12:30/45 done",
    ],
)
def test_ordinary_text_is_not_over_masked(text):
    assert redact(text) == text


def test_a_trailing_status_after_a_path_stays():
    assert redact("failed /srv/a/b.flac (exit 1).") == "failed [path] (exit 1)."


# ---------------------------------------------------------------------------
# Finding 1: a failed source read is not an empty installation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", ["unavailable", "database_unavailable"])
def test_a_failed_read_is_never_shown_as_an_empty_catalogue(status):
    state = importlib.import_module("plugins.LumaeAnalysis.database_state")
    snapshot = {
        "captured_at": "2026-11-13T00:00:00Z",
        "status": status,
        "core": {},
        "sources": [],
        "errors": [{"section": "database", "message": "read failed"}],
    }
    body = state.render_database_state(snapshot)
    assert "No published Lumae catalogue yet" not in body
    assert "run Prepare Lumae" not in body
    assert "could not be read" in body
    assert body.count("unavailable") >= 5  # the summary metrics
    assert "app sync unavailable" in body


def test_a_healthy_empty_installation_still_says_so():
    state = importlib.import_module("plugins.LumaeAnalysis.database_state")
    body = state.render_database_state({
        "captured_at": "2026-11-13T00:00:00Z", "status": "not_initialized",
        "core": {}, "sources": [], "errors": [],
    })
    assert "No published Lumae catalogue yet" in body
    assert "could not be read" not in body


# ---------------------------------------------------------------------------
# Settings page: findings 2, 3 and 4
# ---------------------------------------------------------------------------


class _Db:
    def __init__(self):
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


def _reconcile_panel(monkeypatch, runner_age, event_running=True):
    mod = load_plugin()
    db = _Db()
    snapshot = {
        "runners": [{"role": "worker", "heartbeat_age_seconds": runner_age,
                     "current_action": "profile_backfill", "mode": "working",
                     "host": "h", "actions": 1}],
        "workers": [],
        "pending": {},
        "events": ([{"action": "profile_backfill", "status": "running",
                     "phase": "x", "attempt": 1}] if event_running else []),
    }
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "read_reconcile_status", lambda _db: snapshot)
    monkeypatch.setattr(mod, "reconcile_queue_throughput", lambda _db: [])
    monkeypatch.setattr(mod, "parked_files", lambda _db: {})
    monkeypatch.setattr(mod, "background_workers", lambda: 1)
    monkeypatch.setattr(mod, "configured_background_workers", lambda: "auto")
    monkeypatch.setattr(mod, "shared_passes", lambda: False)
    monkeypatch.setattr(mod, "worker_runners_setting", lambda: "one")
    return mod.render_reconcile_status_panel()


def test_a_dead_runners_last_action_does_not_keep_the_page_polling(monkeypatch):
    stale = _reconcile_panel(monkeypatch, runner_age=100_000)
    assert 'data-lumae-active="false"' in stale
    fresh = _reconcile_panel(monkeypatch, runner_age=1)
    assert 'data-lumae-active="true"' in fresh


def test_the_relationship_read_failure_rolls_back_and_is_not_a_failed_build(monkeypatch):
    mod = load_plugin()
    db = _Db()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [settings_catalog_source()])

    def broken(*_args):
        raise RuntimeError("relation does not exist")

    monkeypatch.setattr(mod, "relationship_status", broken)
    body = mod.render_relationship_status_panel()
    assert db.rollbacks == 1
    assert "could not be read" in body
    assert "build failed" not in body
    assert "relation does not exist" not in body


def test_the_readiness_panel_rolls_back_after_a_failure_and_after_an_unavailable_result(monkeypatch):
    mod = load_plugin()
    db = _Db()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [])

    def broken():
        raise RuntimeError("boom")

    monkeypatch.setattr(mod, "_v3_readiness_sources", broken)
    mod.render_v3_readiness_panel()
    assert db.rollbacks == 1

    monkeypatch.undo()
    mod = load_plugin()
    db = _Db()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "detect_core", lambda: types.SimpleNamespace(adapter="v3_registry"))
    monkeypatch.setattr(mod, "dedup_policy", lambda: {})
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [settings_catalog_source()])
    monkeypatch.setattr(
        mod, "v3_release_readiness", lambda *_a: {"status": "readiness_unavailable"}
    )
    assert mod._v3_readiness_sources()
    assert db.rollbacks == 1


def test_a_failing_workers_setting_does_not_break_the_preparation_panel(monkeypatch):
    mod = load_plugin()
    db = _Db()
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "resolve_catalog_source", lambda _db: [])

    def broken():
        raise RuntimeError("settings read failed")

    monkeypatch.setattr(mod, "configured_background_workers", broken)
    catalogue, waveform = mod.render_source_preparation_sections(25)
    assert "No supported AudioMuse music server" in catalogue
    assert db.rollbacks == 1


def test_one_settings_render_reads_each_source_and_state_once(monkeypatch):
    mod = load_plugin()
    db = _Db()
    calls = {}

    def counted(name, value):
        def read(*_args, **_kwargs):
            calls[name] = calls.get(name, 0) + 1
            return value
        return read

    counts = {"total_with_files": 0, "ready_current": 0, "pending": 0, "failed": 0,
              "skipped": 0, "needs_analysis": 0}
    monkeypatch.setattr(mod, "get_db", lambda: db)
    monkeypatch.setattr(mod, "detect_core", lambda: types.SimpleNamespace(adapter="v2"))
    monkeypatch.setattr(mod, "resolve_catalog_source",
                        counted("sources", [settings_catalog_source()]))
    monkeypatch.setattr(mod, "relationship_status", counted("relationship", {"status": "complete"}))
    monkeypatch.setattr(mod, "preparation_state", counted("preparation", None))
    monkeypatch.setattr(mod, "profile_backfill_state", counted("backfill", None))
    monkeypatch.setattr(mod, "_committed_profile_counts", counted("counts", counts))
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "configured_background_workers", lambda: "auto")
    monkeypatch.setattr(mod, "render_reconcile_status_panel", lambda: "")
    monkeypatch.setattr(mod, "render_provider_identity_panel", lambda: "")
    monkeypatch.setattr(mod, "edge_profile_status", counted("edge", {}))

    panels = mod.render_settings_status_panels(25)

    assert set(panels) >= {"readiness", "relationships", "catalogue", "stream_status"}
    for name in ("sources", "relationship", "preparation", "backfill", "counts"):
        assert calls[name] == 1, (name, calls)
