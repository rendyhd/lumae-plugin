"""Review fixes for provider identity and Jellyfin continuity (no database).

1. A new Navidrome version sends an applied transition back to inspection
   without discarding it.
2. An observation is written compare-and-set on the state it read, and a
   failed ping writes only its error.
3. The Jellyfin reader names the libraries it walked.
"""

import pytest

from test_lumae_analysis import (  # noqa: F401 (plugin_api_module: stub)
    IDENTITY_DB,
    ScriptedProbe,
    TransitionRow,
    load_plugin,
    plugin_api_module,
)
from jellyfin_fixtures import LIB_A, jellyfin_library_rows


class RecordingDb:
    """Records the statements of ``_update_observation``; ``rowcount`` is what
    each UPDATE reports (0: the compare-and-set lost)."""

    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.statements = []

    def cursor(self):
        db = self

        class Cursor:
            rowcount = db.rowcount

            def execute(self, sql, params=None):
                db.statements.append((" ".join(sql.split()), tuple(params or ())))

            def close(self):
                pass

        return Cursor()


SOURCE = {
    "catalog_instance_id": "catalog-a",
    "catalog_generation": 8,
    "analysis_generation": 6,
    "transition_id": "transition-a",
    "state": "transition_pending",
}


class LostWrite(TransitionRow):
    """A transition row whose state another writer changed first: the
    observation's compare-and-set matches nothing."""

    def update(self, _db, _source, **_observation):
        self.writes += 1
        return False


# --- 1: an applied transition and a new provider version --------------------


def test_a_new_version_sends_an_applied_transition_back_without_discarding_it(monkeypatch):
    transition = TransitionRow(
        state="applied",
        transition_id="transition-a",
        audiomuse_health="migration_required",
        required_action="run_audiomuse_provider_migration",
    )
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe("0.64.1"), "server-a", refresh_health=False
    )

    # Admission closes until a refresh has inspected the IDs, as for any
    # version change; the applied transition keeps its identity for
    # inspect_catalog_identity to restore.
    assert result["state"] == "transition_pending"
    assert transition.row["state"] == "transition_pending"
    assert transition.row["required_action"] == "inspect_provider_identity"
    assert transition.row["transition_id"] == "transition-a"
    assert transition.row["audiomuse_health"] == "migration_required"


def test_only_a_row_without_a_transition_id_opens_one_on_observation():
    load_plugin()
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    applied = {"state": "applied", "transition_id": "transition-a"}
    assert guard._starts_new_transition(applied, "transition_pending") is False
    assert guard._starts_new_transition({"state": "normal"}, "transition_pending") is True
    assert guard._starts_new_transition({"state": "normal"}, "normal") is False


# --- 2: compare-and-set observations ----------------------------------------


def test_a_failed_ping_writes_only_its_error_onto_the_state_it_read():
    load_plugin()
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = RecordingDb()
    written = guard._update_observation(
        db, SOURCE, state="transition_pending", current_version="0.64.0",
        detection_reason="transition_detected", required_action="wait_for_lumae_rekey",
        last_error=RuntimeError("ping timed out"),
    )

    assert written is True
    [(sql, params)] = db.statements
    assert sql.split(" WHERE ")[0].endswith("SET last_error=%s, updated_at=now()")
    assert sql.endswith("WHERE catalog_instance_id=%s AND state=%s")
    assert params == ("ping timed out", "catalog-a", "transition_pending")


def test_a_verified_observation_is_written_compare_and_set():
    load_plugin()
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = RecordingDb()
    assert guard._update_observation(
        db, SOURCE, state="normal", current_version="0.63.2",
        detection_reason="pre_transition_version", required_action=None,
    ) is True
    [(sql, params)] = db.statements
    assert sql.endswith("WHERE catalog_instance_id=%s AND state=%s")
    assert params[-2:] == ("catalog-a", "transition_pending")

    lost = RecordingDb(rowcount=0)
    assert guard._update_observation(
        lost, SOURCE, state="normal", current_version="0.63.2",
        detection_reason="pre_transition_version", required_action=None,
    ) is False
    assert guard._update_observation(
        lost, SOURCE, state="transition_pending", current_version="0.64.0",
        detection_reason="transition_detected", required_action="wait_for_lumae_rekey",
        last_error=RuntimeError("ping timed out"),
    ) is False


@pytest.mark.parametrize(
    ("generation", "state", "superseded", "gate"),
    [
        (8, "normal", False, "normal"),
        (8, "applied", False, "applied"),
        (8, "normal", True, "transition_pending"),
        (8, "applied", True, "transition_pending"),
        (8, "blocked", True, "blocked"),
        (8, "transition_pending", True, "transition_pending"),
        # Nothing is published yet: nothing to protect.
        (0, "normal", True, "normal"),
    ],
)
def test_a_lost_compare_and_set_gates_its_own_caller_closed(generation, state, superseded, gate):
    load_plugin()
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    assert guard._caller_gate({"catalog_generation": generation}, state, superseded) == gate


def test_a_superseded_verified_observation_holds_the_projection(monkeypatch):
    # The row said applied and ready when this call read it; another writer
    # changed its state before this call's write, which is skipped.
    transition = LostWrite(
        state="applied",
        transition_id="transition-a",
        audiomuse_health="ready",
        detection_reason="provider_version_unverified",
    )
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe("0.64.0"), "server-a", refresh_health=False
    )
    assert transition.writes == 1
    assert result["state"] == "transition_pending"
    assert result["observation"] == "verified"
    assert result["written"] is False

    with pytest.raises(guard.ProviderIdentityTransitionPending):
        guard.assert_analysis_projection_allowed(IDENTITY_DB, ScriptedProbe("0.64.0"), "server-a")


def test_a_superseded_failed_ping_reports_nothing_written(monkeypatch):
    transition = LostWrite(state="transition_pending", transition_id="transition-a",
                           detected=True)
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe(RuntimeError("provider offline")), "server-a"
    )

    assert result["observation"] == "unverified"
    assert result["state"] == "transition_pending"
    assert result["written"] is False


def test_a_failed_ping_with_stale_baselines_is_not_written_again(monkeypatch):
    # The error is compared alone: a normal row whose generation moved since
    # its last verified observation must not be written on every health poll.
    transition = TransitionRow(baseline_catalog_generation=7)
    guard = transition.install(monkeypatch)
    provider = ScriptedProbe(RuntimeError("provider offline"), RuntimeError("provider offline"))

    assert guard.observe_provider_version(IDENTITY_DB, provider, "server-a")["written"] is True
    assert guard.observe_provider_version(IDENTITY_DB, provider, "server-a")["written"] is False
    assert transition.writes == 1


# --- 3: the libraries a Jellyfin scan walked --------------------------------


def test_the_jellyfin_reader_names_the_libraries_it_walked():
    from test_jellyfin_provider import _two_library_module
    from plugins.LumaeAnalysis import jellyfin_provider as jp
    from plugins.LumaeAnalysis.catalog_providers import CatalogProviderError

    module, _http = _two_library_module(target=[LIB_A])
    raw = jp.fetch_catalog(module, error_type=CatalogProviderError)

    # Every library Jellyfin reports is still listed; only LIB_A was read.
    assert raw["libraries"] == jellyfin_library_rows()
    assert len(raw["libraries"]) == 2
    assert raw["scanned_library_ids"] == [LIB_A]
    assert {tuple(row["_lumae_library_ids"]) for row in raw["tracks"]} == {(LIB_A,)}
