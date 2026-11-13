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


def test_a_pre_boundary_version_sends_an_applied_transition_back_to_inspection(monkeypatch):
    # Up to today an observed pre-canonical release wrote normal over
    # applied, which lifted the projection hold while the AudioMuse
    # migration was still owed.
    transition = TransitionRow(
        state="applied",
        transition_id="transition-a",
        audiomuse_health="migration_required",
        required_action="run_audiomuse_provider_migration",
    )
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe("0.63.2"), "server-a", refresh_health=False
    )

    assert result["state"] == "transition_pending"
    assert transition.row["state"] == "transition_pending"
    assert transition.row["detection_reason"] == "pre_transition_version_after_rekey"
    assert transition.row["required_action"] == "inspect_provider_identity"
    assert transition.row["transition_id"] == "transition-a"
    assert transition.row["audiomuse_health"] == "migration_required"


@pytest.mark.parametrize("stored", ["transition_pending", "blocked"])
def test_a_pre_boundary_version_keeps_an_applied_transition_awaiting_inspection(
    monkeypatch, stored
):
    transition = TransitionRow(
        state=stored,
        transition_id="transition-a",
        transition_published=True,
        current_provider_version="0.64.1",
        detection_reason="provider_version_boundary",
        required_action="inspect_provider_identity",
        detected=True,
    )
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe("0.63.2"), "server-a", refresh_health=False
    )

    assert result["state"] == transition.row["state"] == "transition_pending"
    assert transition.row["transition_id"] == "transition-a"
    with pytest.raises(guard.ProviderIdentityTransitionPending):
        guard.assert_analysis_projection_allowed(IDENTITY_DB, ScriptedProbe("0.63.2"), "server-a")


def test_a_pre_boundary_version_an_inspection_checked_keeps_applied(monkeypatch):
    # The refresh found the IDs unchanged at 0.63.2 and restored applied:
    # the same version observed again is not sent back to inspection.
    transition = TransitionRow(
        state="applied",
        transition_id="transition-a",
        audiomuse_health="migration_required",
        current_provider_version="0.63.2",
        last_checked_provider_version="0.63.2",
        detection_reason="provider_ids_unchanged",
        required_action="run_audiomuse_provider_migration",
    )
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe("0.63.2"), "server-a", refresh_health=False
    )

    assert result["state"] == "applied"
    assert transition.writes == 0
    with pytest.raises(guard.ProviderIdentityTransitionPending) as held:
        guard.assert_analysis_projection_allowed(IDENTITY_DB, ScriptedProbe("0.63.2"), "server-a")
    assert held.value.reason == "audiomuse_migration_pending"


def test_a_pre_boundary_version_still_clears_a_block_nothing_was_applied_for(monkeypatch):
    transition = TransitionRow(
        state="blocked",
        transition_id="transition-a",
        transition_published=False,
        detection_reason="incomplete",
        required_action="resolve_provider_identity_conflict",
    )
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe("0.63.2"), "server-a", refresh_health=False
    )

    assert result["state"] == transition.row["state"] == "normal"
    assert transition.row["detection_reason"] == "pre_transition_version"


@pytest.mark.parametrize(
    ("state", "transition_id", "published", "was_applied"),
    [
        ("applied", "t", False, True),
        ("applied", None, True, False),
        ("transition_pending", "t", True, True),
        ("blocked", "t", True, True),
        ("transition_pending", "t", False, False),
        ("blocked", "t", False, False),
        ("normal", "t", True, False),
    ],
)
def test_which_rows_count_as_applied(state, transition_id, published, was_applied):
    load_plugin()
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    assert guard.transition_was_applied(state, transition_id, published) is was_applied


LEGACY_A = "e3b7fc2ae9447bbec37a13bf916e3cf6"
LEGACY_B = "a3b7fc2ae9447bbec37a13bf916e3cf7"
LEGACY_C = "b3b7fc2ae9447bbec37a13bf916e3cf8"
LEGACY_D = "c3b7fc2ae9447bbec37a13bf916e3cf9"


@pytest.mark.parametrize(
    ("scanned", "reverted"),
    [
        # Binary-only downgrade: the canonical IDs stay.
        (["A", "B", "C", "D"], False),
        # The pre-rekey database restored: legacy forms instead of canonical.
        ([LEGACY_A, LEGACY_B, LEGACY_C, LEGACY_D], True),
        # A restore with a few ordinary removals and additions.
        ([LEGACY_A, LEGACY_B, LEGACY_C, "f" * 32], True),
        # One track reverted among others still canonical is not a restore.
        ([LEGACY_A, "B", "C", "D"], False),
        # Both forms of a track in one scan: a conflict, not a reversal.
        ([LEGACY_A, "A", LEGACY_B, LEGACY_C, LEGACY_D], False),
        # Ordinary removals and additions are not evidence.
        (["A", "B", "C", "f" * 32], False),
    ],
)
def test_which_scans_show_ids_reverted_to_legacy(scanned, reverted):
    load_plugin()
    from plugins.LumaeAnalysis import provider_identity_guard as guard
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id

    canonical = {
        "A": canonicalize_navidrome_id(LEGACY_A).value,
        "B": canonicalize_navidrome_id(LEGACY_B).value,
        "C": canonicalize_navidrome_id(LEGACY_C).value,
        "D": canonicalize_navidrome_id(LEGACY_D).value,
    }
    published = list(canonical.values())
    scan = [canonical.get(value, value) for value in scanned]

    assert guard._ids_reverted_to_legacy(published, scan) is reverted


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
    assert sql.endswith(
        "WHERE catalog_instance_id=%s AND state=%s AND transition_id IS NOT DISTINCT FROM %s")
    assert params == ("ping timed out", "catalog-a", "transition_pending", "transition-a")


def test_a_verified_observation_is_written_compare_and_set():
    load_plugin()
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = RecordingDb()
    assert guard._update_observation(
        db, SOURCE, state="normal", current_version="0.63.2",
        detection_reason="pre_transition_version", required_action=None,
    ) is True
    [(sql, params)] = db.statements
    assert sql.endswith(
        "WHERE catalog_instance_id=%s AND state=%s AND transition_id IS NOT DISTINCT FROM %s")
    assert params[-3:] == ("catalog-a", "transition_pending", "transition-a")

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


def test_a_failed_ping_whose_error_write_lost_gates_its_caller_closed(monkeypatch):
    # The row said applied when this call read it; the error write found
    # another state (a health check sent the transition back to inspection).
    transition = LostWrite(
        state="applied",
        transition_id="transition-a",
        audiomuse_health="migration_required",
    )
    guard = transition.install(monkeypatch)

    result = guard.observe_provider_version(
        IDENTITY_DB, ScriptedProbe(RuntimeError("ping timed out")), "server-a"
    )

    assert transition.writes == 1
    assert (result["state"], result["stored_state"], result["written"]) == (
        "transition_pending", "applied", False)


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
