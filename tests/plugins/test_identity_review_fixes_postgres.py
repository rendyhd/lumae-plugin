"""Review fixes for provider identity, Jellyfin continuity and file tags.

1. An applied Navidrome transition survives a new Navidrome version; IDs
   that really changed again still open the next transition.
2. A stale observation never overwrites a state committed since its read.
3. A Jellyfin library deselected in AudioMuse is not held.
4. A fingerprint rekey keeps tags already read under the new ID.
5. The rekey publication writes its journals a page per statement, with the
   rows and sequence numbers of one statement per row.
"""

import json
from datetime import datetime, timedelta, timezone

import psycopg2.errors
import psycopg2.extensions
import pytest

from test_lumae_analysis import (  # noqa: F401 (plugin_api_module: stub)
    RefreshBridge,
    _identity_fixture_catalog,
    load_plugin,
    plugin_api_module,
)
from jellyfin_fixtures import LIB_A, LIB_B, JellyfinBridge, jellyfin_raw_catalog, jid


P = "plugin_lumae_analysis__"
V2 = "provider_identity_rekey_v2"
TRACK_A = "e3b7fc2ae9447bbec37a13bf916e3cf6"
TRACK_B = "a3b7fc2ae9447bbec37a13bf916e3cf7"
ALBUM = "0123456789abcdef0123456789abcdef"
ARTIST = "11111111111111111111111111111111"


class MappingCore:
    """The AudioMuse adapter surface the health check reads (track_server_map)."""

    @staticmethod
    def analysis_mapping_sql():
        return (
            "SELECT provider_track_id, item_id AS analysis_id, match_tier "
            "FROM track_server_map WHERE server_id = %s"
        )


class ProbeBridge(RefreshBridge):
    """A Navidrome bridge that answers the version ping. ``before_answer``
    runs between the observation's read and its write."""

    def __init__(self, payload, version, core=None):
        super().__init__(payload)
        self.version = version
        self.core = core
        self.probe_error = None
        self.before_answer = None

    def probe_server_identity(self, _server_id):
        if self.before_answer:
            self.before_answer()
        if self.probe_error:
            raise self.probe_error
        return {"provider_type": "navidrome", "server_version": self.version}


class RacingJellyfinBridge(JellyfinBridge):
    before_answer = None

    def probe_server_identity(self, server_id):
        if self.before_answer:
            self.before_answer()
        return super().probe_server_identity(server_id)


def _canonical(value):
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id

    return canonicalize_navidrome_id(value).value


def _catalog(track_ids, album_id, artist_id):
    payload = _identity_fixture_catalog(track_ids[0], album_id, artist_id)
    first = payload["tracks"][0]
    payload["tracks"] = [
        {**first, "id": track_id, "title": f"Song {index}", "track": index + 1}
        for index, track_id in enumerate(track_ids)
    ]
    return payload


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _execute(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
    db.commit()


def _refresh(db, bridge, server_id="server-a"):
    from plugins.LumaeAnalysis import catalog

    return catalog.refresh_catalog(server_id, db=db, bridge=bridge)


def _transition(db):
    """The transition row without its clocks."""
    [(row,)] = _rows(db, f"""
        SELECT to_jsonb(p) - 'updated_at' - 'checked_at' - 'recheck_attempted_at'
          FROM {P}provider_identity_transitions p""")
    return row if isinstance(row, dict) else json.loads(row)


def _published_tracks(db):
    return {row[0] for row in _rows(db, f"""
        SELECT t.track_id FROM {P}catalog_tracks t
          JOIN {P}catalog_state s USING (catalog_instance_id)
         WHERE t.published_generation=s.published_generation AND t.available""")}


def _core_tables(db):
    _execute(db, "CREATE TABLE IF NOT EXISTS track_server_map (item_id TEXT NOT NULL, "
                 "server_id TEXT NOT NULL, provider_track_id TEXT NOT NULL, match_tier TEXT, "
                 "PRIMARY KEY (server_id, provider_track_id))")
    _execute(db, "CREATE TABLE IF NOT EXISTS task_status (status TEXT, task_type TEXT)")


def _seed_analysis(db, source, links):
    """One analysis generation: ``{provider_track_id: analysis_id}``."""
    with db.cursor() as cur:
        for analysis_id in sorted(set(links.values())):
            cur.execute(
                f"INSERT INTO {P}analysis_items "
                "(catalog_instance_id, projection_generation, analysis_id) VALUES (%s, 1, %s)",
                (source, analysis_id))
        for track_id, analysis_id in links.items():
            cur.execute(
                f"INSERT INTO {P}track_analysis_links (catalog_instance_id, "
                "projection_generation, provider_track_id, analysis_id, status) "
                "VALUES (%s, 1, %s, %s, 'ready')", (source, track_id, analysis_id))
        cur.execute(
            f"UPDATE {P}analysis_state SET projection_generation=1, item_count=%s, "
            "mapped_track_count=%s, status='complete' WHERE catalog_instance_id=%s",
            (len(set(links.values())), len(links), source))
    db.commit()


def _applied_transition(db):
    """Navidrome moved to canonical IDs and Lumae applied the rekey; AudioMuse
    has not migrated its own mapping yet. TRACK_B kept its legacy ID."""
    load_plugin()
    _core_tables(db)
    bridge = ProbeBridge(_catalog([TRACK_A, TRACK_B], ALBUM, ARTIST), "0.63.2",
                         core=MappingCore())
    source = _refresh(db, bridge)["catalog_instance_id"]
    _seed_analysis(db, source, {TRACK_A: "analysis-1"})

    bridge.version = "0.64.0"
    bridge.payload = _catalog(
        [_canonical(TRACK_A), TRACK_B], _canonical(ALBUM), _canonical(ARTIST))
    assert _refresh(db, bridge)["change_reason"] == "provider_identity_wait"
    applied = _refresh(db, bridge)["provider_identity_transition"]
    assert (applied["state"], applied["audiomuse_health"]) == ("applied", "migration_required")
    row = _transition(db)
    assert row["required_action"] == "run_audiomuse_provider_migration"
    assert row["last_checked_provider_version"] == "0.64.0"
    return bridge, source, applied["transition_id"]


# --- 1: an applied transition and a new provider version --------------------


def test_an_applied_transition_survives_a_new_navidrome_version(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge, source, transition_id = _applied_transition(db)
    applied = _transition(db)

    bridge.version = "0.64.1"
    observation = guard.observe_provider_version(db, bridge, "server-a")

    # Admission closes until the IDs are inspected, as for any new version;
    # the applied transition is carried, not replaced.
    assert observation["state"] == "transition_pending"
    pending = _transition(db)
    assert (pending["state"], pending["required_action"]) == (
        "transition_pending", "inspect_provider_identity")
    for key in ("transition_id", "audiomuse_health", "manifest_sha256", "first_seq",
                "last_seq", "applied_at", "analysis_baseline", "baseline_integrity"):
        assert pending[key] == applied[key], key
    assert guard.provider_transition_health(db, source)["catalog_sync_allowed"] is False
    with pytest.raises(guard.ProviderIdentityTransitionPending) as held:
        guard.assert_analysis_projection_allowed(db, bridge, "server-a")
    assert held.value.reason == "provider_identity_pending"

    # The refresh finds the same IDs: applied again, the migration still owed.
    assert _refresh(db, bridge)["change_reason"] == "no_change"
    restored = _transition(db)
    assert restored["state"] == "applied"
    assert restored["transition_id"] == transition_id
    assert restored["audiomuse_health"] == "migration_required"
    assert restored["required_action"] == "run_audiomuse_provider_migration"
    assert restored["detection_reason"] == "provider_ids_unchanged"
    assert restored["last_checked_provider_version"] == "0.64.1"
    assert restored["manifest_sha256"] == applied["manifest_sha256"]
    health = guard.provider_transition_health(db, source)
    assert health["catalog_sync_allowed"] is True
    assert health["audiomuse_projection_ingest_allowed"] is False
    with pytest.raises(guard.ProviderIdentityTransitionPending) as held:
        guard.assert_analysis_projection_allowed(db, bridge, "server-a")
    assert held.value.reason == "audiomuse_migration_pending"

    # AudioMuse migrates its mapping: the hold lifts.
    _execute(db, "INSERT INTO track_server_map (item_id, server_id, provider_track_id) "
                 "VALUES ('analysis-1', 'server-a', %s)", (_canonical(TRACK_A),))
    assert guard.assert_analysis_projection_allowed(db, bridge, "server-a")["state"] == "applied"
    assert _transition(db)["required_action"] is None


def test_ids_that_change_again_after_an_applied_transition_open_the_next_one(migrated_db):
    from plugins.LumaeAnalysis.provider_identity_rekey import read_transition_manifest

    db = migrated_db
    bridge, source, first_id = _applied_transition(db)
    [(generation, analysis_generation)] = _rows(db, f"""
        SELECT c.published_generation, a.projection_generation
          FROM {P}catalog_state c JOIN {P}analysis_state a USING (catalog_instance_id)""")

    bridge.version = "0.64.1"
    bridge.payload = _catalog(
        [_canonical(TRACK_A), _canonical(TRACK_B)], _canonical(ALBUM), _canonical(ARTIST))
    waiting = _refresh(db, bridge)

    assert waiting["change_reason"] == "provider_identity_wait"
    opened = _transition(db)
    assert opened["state"] == "transition_pending"
    assert opened["transition_id"] not in (None, first_id)
    assert opened["target_scan_count"] == 1
    assert opened["baseline_catalog_generation"] == generation
    assert opened["baseline_analysis_generation"] == analysis_generation
    for key in ("first_seq", "last_seq", "manifest_sha256", "audiomuse_health", "applied_at",
                "baseline_integrity"):
        assert opened[key] is None, key
    assert opened["analysis_baseline"] == {}

    applied = _refresh(db, bridge)["provider_identity_transition"]

    assert applied["state"] == "applied"
    assert applied["transition_id"] == opened["transition_id"]
    assert _published_tracks(db) == {_canonical(TRACK_A), _canonical(TRACK_B)}
    assert read_transition_manifest(db, transition_id=first_id)["mappings"][-1]["old_id"] == TRACK_A
    second = read_transition_manifest(db, transition_id=applied["transition_id"])
    assert second["mappings"] == [
        {"entity_type": "track", "old_id": TRACK_B, "new_id": _canonical(TRACK_B)}]
    assert second["baseline_catalog_generation"] == generation


def test_a_block_after_an_applied_transition_restores_it_once_the_ids_match(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    _bridge, source, transition_id = _applied_transition(db)
    # AudioMuse named another server type for a while (provider_type_changed).
    _execute(db, f"UPDATE {P}provider_identity_transitions SET state='blocked', "
                 "detection_reason='provider_type_changed', "
                 "required_action='set_up_new_catalogue'")

    inspection = guard.inspect_catalog_identity(
        db, source, [_canonical(TRACK_A), TRACK_B], "0.64.0")
    db.commit()

    assert (inspection["state"], inspection["transition_id"]) == ("applied", transition_id)
    assert inspection["required_action"] == "run_audiomuse_provider_migration"
    assert _transition(db)["audiomuse_health"] == "migration_required"


def test_a_pre_boundary_version_after_an_applied_transition_keeps_its_hold(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge, source, transition_id = _applied_transition(db)
    applied = _transition(db)

    # The server that moved to canonical IDs reports a pre-canonical release.
    bridge.version = "0.63.2"
    observation = guard.observe_provider_version(db, bridge, "server-a")

    assert observation["state"] == "transition_pending"
    pending = _transition(db)
    assert (pending["state"], pending["detection_reason"], pending["required_action"]) == (
        "transition_pending", "pre_transition_version_after_rekey", "inspect_provider_identity")
    for key in ("transition_id", "audiomuse_health", "manifest_sha256", "applied_at"):
        assert pending[key] == applied[key], key
    with pytest.raises(guard.ProviderIdentityTransitionPending) as held:
        guard.assert_analysis_projection_allowed(db, bridge, "server-a")
    assert held.value.reason == "provider_identity_pending"

    # The refresh finds the same IDs: applied again, the migration still owed,
    # and the checked version observed again leaves it applied.
    assert _refresh(db, bridge)["change_reason"] == "no_change"
    restored = _transition(db)
    assert (restored["state"], restored["transition_id"]) == ("applied", transition_id)
    assert restored["required_action"] == "run_audiomuse_provider_migration"
    assert restored["last_checked_provider_version"] == "0.63.2"
    assert guard.observe_provider_version(db, bridge, "server-a")["state"] == "applied"
    assert _transition(db)["state"] == "applied"
    with pytest.raises(guard.ProviderIdentityTransitionPending) as held:
        guard.assert_analysis_projection_allowed(db, bridge, "server-a")
    assert held.value.reason == "audiomuse_migration_pending"


@pytest.mark.parametrize(
    ("update", "flagged"),
    [
        ("", True),
        ("state='transition_pending'", True),
        ("state='blocked'", True),
        ("state='transition_pending', applied_at=NULL", False),
        ("state='normal'", False),
    ],
)
def test_an_upgrade_requests_the_projection_check_for_every_applied_transition(
    migrated_db, update, flagged
):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    _bridge, source, _transition_id = _applied_transition(db)
    # An applied transition, also one a version observation or a block sent
    # back to inspection (it keeps its id, manifest and applied_at).
    if update:
        _execute(db, f"UPDATE {P}provider_identity_transitions SET {update}")

    guard.require_projection_reconcile(db)
    db.commit()

    assert guard.projection_reconcile_required(db, source) is flagged
    db.commit()
    row = _transition(db)
    assert guard.transition_was_applied(
        row["state"], row["transition_id"],
        row["applied_at"] is not None and row["manifest_sha256"] is not None) is flagged


def test_the_upgrade_request_locks_the_catalogue_before_the_transition_row(
    migrated_db, second_connection
):
    """A refresh holds catalog_state and waits for the transition row; the
    install must take them in the same order, or the two deadlock."""
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge, _source, _transition_id = _applied_transition(db)
    bridge.version = "0.64.1"
    guard.observe_provider_version(db, bridge, "server-a")
    assert _transition(db)["state"] == "transition_pending"

    # A refresh's inspection has locked the catalogue row.
    with second_connection.cursor() as other:
        other.execute(f"SELECT 1 FROM {P}catalog_state FOR UPDATE")
    with db.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '300ms'")
    with pytest.raises(psycopg2.errors.LockNotAvailable):
        guard.require_projection_reconcile(db)
    # The install waited on the catalogue row without holding the transition row.
    with second_connection.cursor() as other:
        other.execute(f"SELECT 1 FROM {P}provider_identity_transitions FOR UPDATE NOWAIT")
    second_connection.rollback()
    db.rollback()

    assert guard.require_projection_reconcile(db) == 1
    db.commit()
    assert _transition(db)["projection_reconcile_required"] is True


def _restored_legacy_database(db, version):
    """Applied, then Navidrome reports ``version`` with its pre-rekey IDs."""
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    bridge, source, transition_id = _applied_transition(db)
    bridge.version = version
    bridge.payload = _catalog([TRACK_A, TRACK_B], ALBUM, ARTIST)
    assert guard.observe_provider_version(db, bridge, "server-a")["state"] == "transition_pending"
    return bridge, source, transition_id


def test_a_restored_pre_rekey_database_returns_the_catalogue_to_normal(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard
    from plugins.LumaeAnalysis.provider_identity_rekey import read_transition_manifest

    db = migrated_db
    bridge, source, transition_id = _restored_legacy_database(db, "0.63.2")

    _refresh(db, bridge)

    row = _transition(db)
    assert (row["state"], row["detection_reason"], row["required_action"]) == (
        "normal", "provider_ids_reverted", None)
    for key in ("transition_id", "applied_at", "manifest_sha256", "audiomuse_health",
                "first_seq", "last_seq", "baseline_integrity"):
        assert row[key] is None, key
    assert row["last_checked_provider_version"] == "0.63.2"
    assert _published_tracks(db) == {TRACK_A, TRACK_B}
    # The transition stays in the history; nothing holds the projection.
    assert read_transition_manifest(db, transition_id=transition_id)
    assert guard.observe_provider_version(db, bridge, "server-a")["state"] == "normal"
    assert guard.assert_analysis_projection_allowed(db, bridge, "server-a")["state"] == "normal"


def test_legacy_ids_after_a_post_boundary_version_still_restore_applied(migrated_db):
    # Only a pre-canonical version is read as a restored database; the
    # version-change path is as it was.
    db = migrated_db
    bridge, _source, transition_id = _restored_legacy_database(db, "0.64.1")

    _refresh(db, bridge)

    row = _transition(db)
    assert (row["state"], row["transition_id"]) == ("applied", transition_id)


def test_a_jellyfin_transition_blocked_after_it_was_applied_is_restored(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    load_plugin()
    bridge = RacingJellyfinBridge(jellyfin_raw_catalog())
    _refresh(db, bridge, "server-j")
    # An applied v2 rekey, then a block (an unsupported release for a while).
    _execute(db, f"""UPDATE {P}provider_identity_transitions
                        SET state='blocked', transition_id='t-applied', applied_at=now(),
                            manifest_sha256='m', audiomuse_health='busy', rekey_contract=%s,
                            detection_reason='provider_version_unsupported',
                            required_action='upgrade_jellyfin'""", (V2,))
    blocked = _transition(db)

    result = guard.observe_provider_version(db, bridge, "server-j", refresh_health=False)

    assert result["state"] == "applied"
    row = _transition(db)
    assert (row["state"], row["detection_reason"], row["required_action"]) == (
        "applied", guard.JELLYFIN_VERIFIED_REASON, None)
    for key in ("transition_id", "applied_at", "manifest_sha256", "audiomuse_health",
                "rekey_contract"):
        assert row[key] == blocked[key], key

    # A block nothing was applied for still clears to normal.
    _execute(db, f"UPDATE {P}provider_identity_transitions SET state='blocked', "
                 "applied_at=NULL, detection_reason='provider_version_unsupported'")
    assert guard.observe_provider_version(
        db, bridge, "server-j", refresh_health=False)["state"] == "normal"


def test_a_block_of_an_applied_transition_keeps_its_counts_and_target_proof(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge, source, transition_id = _applied_transition(db)
    proof_keys = ("counts", "target_fingerprint", "target_scan_count")
    applied = _transition(db)
    bridge.version = "0.64.1"
    guard.observe_provider_version(db, bridge, "server-a")
    assert _transition(db)["state"] == "transition_pending"

    # TRACK_B is missing: incomplete evidence blocks the catalogue.
    blocked = guard.inspect_catalog_identity(db, source, [_canonical(TRACK_A)], "0.64.1")
    db.commit()
    assert blocked["state"] == "blocked"
    row = _transition(db)
    assert (row["state"], row["transition_id"]) == ("blocked", transition_id)
    for key in proof_keys:
        assert row[key] == applied[key], key

    # The IDs turn out unchanged: applied again, with the transition's own proof.
    restored = guard.inspect_catalog_identity(
        db, source, [_canonical(TRACK_A), TRACK_B], "0.64.1")
    db.commit()
    assert (restored["state"], restored["transition_id"]) == ("applied", transition_id)
    row = _transition(db)
    for key in (*proof_keys, "manifest_sha256", "first_seq", "last_seq", "applied_at"):
        assert row[key] == applied[key], key


def test_an_observation_never_writes_its_transition_id_over_a_newly_opened_one(
    migrated_db, second_connection
):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge, source, first_id = _applied_transition(db)
    bridge.version = "0.64.1"
    guard.observe_provider_version(db, bridge, "server-a")
    bridge.probe_error = RuntimeError("ping timed out")
    guard.observe_provider_version(db, bridge, "server-a")
    assert (_transition(db)["transition_id"], _transition(db)["last_error"]) == (
        first_id, "ping timed out")
    bridge.probe_error = None
    opened = None

    def open_next_transition():
        # This observation has read transition_pending with the first id; a
        # refresh's inspection finds the IDs changed again and opens the next
        # transition (a new id, the state still transition_pending).
        nonlocal opened
        inspection = guard.inspect_catalog_identity(
            second_connection, source, [_canonical(TRACK_A), _canonical(TRACK_B)], "0.64.1")
        second_connection.commit()
        assert inspection["state"] == "transition_pending"
        opened = _transition(second_connection)

    bridge.before_answer = open_next_transition
    result = guard.observe_provider_version(db, bridge, "server-a")

    assert opened["transition_id"] not in (None, first_id)
    assert (result["state"], result["written"]) == ("transition_pending", False)
    assert _transition(db) == opened

    # A failed ping that read the first id does not write over it either.
    stale = {**guard._source_state(db, "server-a"), "transition_id": first_id}
    db.commit()
    assert guard._record_probe_error(db, stale, RuntimeError("late")) is False
    db.commit()
    assert _transition(db) == opened


@pytest.mark.parametrize("error_known", [False, True])
def test_a_projection_waits_when_its_ping_fails_as_the_transition_returns_to_inspection(
    migrated_db, second_connection, error_known
):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge, _source, _transition_id = _applied_transition(db)
    if error_known:
        # The same error is stored already: this call writes nothing, and only
        # the re-read of the stored state can hold it.
        _execute(db, f"UPDATE {P}provider_identity_transitions SET last_error='ping timed out'")
    bridge.probe_error = RuntimeError("ping timed out")
    # The projection read applied; a health check stores the version change
    # (the transition kept, the migration still owed) before its ping fails.
    bridge.before_answer = _commit_from(
        second_connection,
        f"UPDATE {P}provider_identity_transitions SET state='transition_pending', "
        "detection_reason='provider_version_boundary', "
        "required_action='inspect_provider_identity', detected_at=now()")

    with pytest.raises(guard.ProviderIdentityTransitionPending) as held:
        guard.assert_analysis_projection_allowed(db, bridge, "server-a")
    assert held.value.reason == "provider_identity_pending"


def test_a_normal_row_gated_for_one_call_gets_its_baselines_under_the_lock(migrated_db):
    """A failed ping no longer writes baselines from its unlocked read; the
    inspection that opens a transition sets them, and its proof starts over."""
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    load_plugin()
    bridge = ProbeBridge(_catalog([TRACK_A], ALBUM, ARTIST), "0.63.2")
    source = _refresh(db, bridge)["catalog_instance_id"]
    _execute(db, f"UPDATE {P}provider_identity_transitions SET baseline_catalog_generation=0, "
                 "target_fingerprint='fp', target_scan_count=5")

    inspection = guard.inspect_catalog_identity(db, source, [_canonical(TRACK_A)], "0.64.0", "fp")
    db.commit()

    assert (inspection["state"], inspection["target_scan_count"]) == ("transition_pending", 1)
    assert _transition(db)["baseline_catalog_generation"] == 1


# --- 2: compare-and-set observations ----------------------------------------


def _pending_proof(db):
    """A Navidrome source whose first identical target scan is recorded."""
    load_plugin()
    _core_tables(db)
    bridge = ProbeBridge(_catalog([TRACK_A], ALBUM, ARTIST), "0.63.2", core=MappingCore())
    _refresh(db, bridge)
    bridge.version = "0.64.0"
    bridge.payload = _catalog([_canonical(TRACK_A)], _canonical(ALBUM), _canonical(ARTIST))
    assert _refresh(db, bridge)["change_reason"] == "provider_identity_wait"
    assert _transition(db)["state"] == "transition_pending"
    return bridge


def _commit_from(connection, sql, params=()):
    def run():
        with connection.cursor() as cur:
            cur.execute(sql, params)
        connection.commit()

    return run


def test_a_failed_ping_writes_only_its_error(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge = _pending_proof(db)
    before = _transition(db)
    bridge.probe_error = RuntimeError("ping timed out")

    first = guard.observe_provider_version(db, bridge, "server-a")
    second = guard.observe_provider_version(db, bridge, "server-a")

    assert (first["state"], first["observation"], first["written"]) == (
        "transition_pending", "unverified", True)
    assert second["written"] is False
    after = _transition(db)
    assert after.pop("last_error") == "ping timed out"
    assert before.pop("last_error") is None
    assert after == before


def test_a_ping_that_fails_during_a_publication_keeps_the_applied_state(
    migrated_db, second_connection
):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    bridge = _pending_proof(db)
    bridge.probe_error = RuntimeError("ping timed out")
    published = None

    def publish_then_snapshot():
        # The observation has read transition_pending; the rekey publication
        # commits applied before the ping gives up.
        nonlocal published
        _commit_from(
            second_connection,
            f"UPDATE {P}provider_identity_transitions SET state='applied', "
            "required_action='run_audiomuse_provider_migration', "
            "audiomuse_health='migration_required', applied_at=now(), last_error=NULL")()
        published = _transition(second_connection)

    bridge.before_answer = publish_then_snapshot
    result = guard.observe_provider_version(db, bridge, "server-a")

    assert (result["observation"], result["written"]) == ("unverified", False)
    assert _transition(db) == published
    assert published["state"] == "applied" and published["last_error"] is None


def test_a_stale_verified_observation_does_not_replace_another_writers_state(
    migrated_db, second_connection
):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    load_plugin()
    bridge = ProbeBridge(_catalog([TRACK_A], ALBUM, ARTIST), "0.63.2")
    _refresh(db, bridge)
    assert _transition(db)["state"] == "normal"
    # This call read normal; another observer stores the pending transition
    # first. Without the compare-and-set this call replaced its identity.
    bridge.before_answer = _commit_from(
        second_connection,
        f"UPDATE {P}provider_identity_transitions SET state='transition_pending', "
        "transition_id='theirs', detection_reason='provider_version_boundary', "
        "required_action='inspect_provider_identity', current_provider_version='0.64.0', "
        "detected_at=now()")
    bridge.version = "0.64.0"

    result = guard.observe_provider_version(db, bridge, "server-a")

    assert (result["state"], result["written"]) == ("transition_pending", False)
    row = _transition(db)
    assert (row["state"], row["transition_id"]) == ("transition_pending", "theirs")


def test_an_observation_written_from_a_stale_read_is_refused(migrated_db):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    _pending_proof(db)
    stale = guard._source_state(db, "server-a")
    db.commit()
    _execute(db, f"UPDATE {P}provider_identity_transitions SET state='applied', applied_at=now()")
    applied = _transition(db)
    observation = {
        "state": "transition_pending",
        "current_version": "0.64.0",
        "detection_reason": stale["detection_reason"],
        "required_action": stale["required_action"],
    }

    assert guard._update_observation(db, stale, **observation) is False
    assert guard._update_observation(
        db, stale, **observation, last_error=RuntimeError("ping timed out")) is False
    db.commit()
    assert _transition(db) == applied

    fresh = guard._source_state(db, "server-a")
    assert guard._update_observation(
        db, fresh, **{**observation, "state": "applied"},
        last_error=RuntimeError("ping timed out")) is True
    db.commit()
    assert _transition(db) == {**applied, "last_error": "ping timed out"}


def test_a_jellyfin_probe_that_fails_during_a_publication_keeps_the_applied_state(
    migrated_db, second_connection
):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    load_plugin()
    bridge = RacingJellyfinBridge(jellyfin_raw_catalog())
    _refresh(db, bridge, "server-j")
    _execute(db, f"""UPDATE {P}provider_identity_transitions
                        SET state='transition_pending', detection_reason='provider_ids_moved',
                            required_action='wait_for_lumae_rekey', transition_id='t-moved',
                            target_fingerprint='fp-target', target_scan_count=2,
                            rekey_contract=%s""", (V2,))
    published = None

    def publish_then_snapshot():
        nonlocal published
        _commit_from(
            second_connection,
            f"UPDATE {P}provider_identity_transitions SET state='applied', "
            "required_action=NULL, audiomuse_health='ready', applied_at=now()")()
        published = _transition(second_connection)

    bridge.before_answer = publish_then_snapshot
    bridge.probe_error = RuntimeError("connection reset")

    result = guard.observe_provider_version(db, bridge, "server-j")

    assert (result["observation"], result["written"]) == ("unverified", False)
    assert _transition(db) == published
    assert (published["state"], published["transition_id"]) == ("applied", "t-moved")


def test_a_stale_jellyfin_observation_does_not_wipe_a_rekey_proof(migrated_db, second_connection):
    from plugins.LumaeAnalysis import provider_identity_guard as guard

    db = migrated_db
    load_plugin()
    bridge = RacingJellyfinBridge(jellyfin_raw_catalog())
    _refresh(db, bridge, "server-j")
    # A reason the verified probe rewrites, so this call does write.
    _execute(db, f"UPDATE {P}provider_identity_transitions SET detection_reason='stale'")
    proof = None

    def open_proof_then_snapshot():
        nonlocal proof
        _commit_from(
            second_connection,
            f"""UPDATE {P}provider_identity_transitions
                   SET state='transition_pending', detection_reason='provider_ids_moved',
                       required_action='wait_for_lumae_rekey', transition_id='t-moved',
                       target_fingerprint='fp-target', target_scan_count=1, rekey_contract=%s,
                       detected_at=now()""", (V2,))()
        proof = _transition(second_connection)

    bridge.before_answer = open_proof_then_snapshot

    result = guard.observe_provider_version(db, bridge, "server-j", refresh_health=False)

    # The write (state normal: no transition id, no target) is skipped; the
    # caller is held instead of being told the catalogue is normal.
    assert (result["state"], result["written"]) == ("transition_pending", False)
    assert _transition(db) == proof
    assert (proof["transition_id"], proof["target_scan_count"]) == ("t-moved", 1)


# --- 3: a library deselected in AudioMuse -----------------------------------


@pytest.mark.parametrize("names_walked", [True, False])
def test_tracks_of_a_library_deselected_in_audiomuse_are_deleted_at_once(
    migrated_db, names_walked
):
    from test_jellyfin_continuity import (
        MappingCore as JellyfinCore, _album, _held, _published, _raw, _track)

    db = migrated_db
    load_plugin()
    _core_tables(db)
    both = (LIB_A, LIB_B)
    raw = _raw([_track("one"), _track("two", album="other", library=LIB_B)],
               [_album(), _album("other", library=LIB_B)], libraries=both)
    bridge = JellyfinBridge({**raw, "scanned_library_ids": list(both)}, core=JellyfinCore())
    _refresh(db, bridge, "server-j")
    assert _published(db) == {jid("one"), jid("two")}

    # LIB_B is deselected in AudioMuse: Jellyfin still reports it, the scan
    # no longer walks it.
    bridge.raw = _raw([_track("one")], [_album()], libraries=both)
    if names_walked:
        bridge.raw["scanned_library_ids"] = [LIB_A]
    result = _refresh(db, bridge, "server-j")

    if names_walked:
        assert result["change_reason"] == "provider_diff"
        assert _published(db) == {jid("one")}
        assert _held(db) == {}
    else:
        # A raw catalogue without the key is read as before: every listed
        # library counts as read, so the track is held.
        assert _published(db) == {jid("one"), jid("two")}
        assert set(_held(db)) == {jid("two")}


# --- 4: tags read under the new ID ------------------------------------------


def test_a_tag_rekey_keeps_tags_already_read_under_the_new_id(migrated_db):
    from plugins.LumaeAnalysis import file_tags

    db = migrated_db
    load_plugin()
    _execute(db, f"INSERT INTO {P}catalog_sources (catalog_instance_id, "
                 "current_core_server_id, provider_type, server_name, is_default, "
                 "rebind_status) VALUES ('catalog-j', 'server-j', 'jellyfin', 'J', TRUE, "
                 "'active'), ('catalog-k', 'server-k', 'jellyfin', 'K', FALSE, 'active')")
    stored = [
        ("catalog-j", "old-1", {"bpm": 101}),
        ("catalog-j", "new-1", {"bpm": 140}),   # read from the moved file itself
        ("catalog-j", "old-2", {"bpm": 90}),
        ("catalog-j", "other", {"bpm": 60}),
        ("catalog-k", "old-1", {"bpm": 33}),    # another catalogue: untouched
        ("catalog-k", "new-2", {"bpm": 34}),
    ]
    with db.cursor() as cur:
        for source, track_id, tags in stored:
            cur.execute(
                f"INSERT INTO {P}jellyfin_file_tags (catalog_instance_id, track_id, tags, "
                "tags_fp, read_with) VALUES (%s, %s, %s::jsonb, %s, 'test')",
                (source, track_id, json.dumps(tags), f"fp-{tags['bpm']}"))
        file_tags.rekey(cur, "catalog-j", {"old-1": "new-1", "old-2": "new-2"})
    db.commit()

    assert _rows(db, f"SELECT catalog_instance_id, track_id, tags->>'bpm', tags_fp "
                     f"FROM {P}jellyfin_file_tags ORDER BY 1, 2") == [
        ("catalog-j", "new-1", "140", "fp-140"),
        ("catalog-j", "new-2", "90", "fp-90"),
        ("catalog-j", "other", "60", "fp-60"),
        ("catalog-k", "new-2", "34", "fp-34"),
        ("catalog-k", "old-1", "33", "fp-33"),
    ]


# --- 5: batched journals ----------------------------------------------------


def _record_statements(db, log):
    class Recording(psycopg2.extensions.cursor):
        def execute(self, sql, params=None):
            text = sql.decode("utf-8", "replace") if isinstance(sql, bytes) else str(sql)
            log.append(" ".join(text.split()))
            return super().execute(sql, params)

    db.cursor_factory = Recording


def _inserts(log, table):
    return [sql for sql in log if sql.startswith(f"INSERT INTO {P}{table} ")]


def _hex(index):
    return f"{0xabc000 + index:032x}"


LINK_COLUMNS = ("analysis_id, status, match_tier, algorithm, decision_threshold, distance, "
                "evidence_complete, conflict_flags, review_state")


def _publish_rekey_of(db, track_count):
    """Publish ``track_count`` legacy tracks (and one that disappears) with
    varied analysis links, then the real rekey publication to canonical IDs.

    Returns what one statement per row wrote: the plan's events and the
    analysis changes, in order, from the heads before the publication.
    """
    from plugins.LumaeAnalysis import catalog
    from plugins.LumaeAnalysis import provider_identity_rekey as rekey

    load_plugin()
    _core_tables(db)
    old_ids = [_hex(index) for index in range(track_count)]
    gone = _hex(9999)
    source = _refresh(
        db, RefreshBridge(_catalog([*old_ids, gone], ALBUM, ARTIST)))["catalog_instance_id"]
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {P}analysis_items (catalog_instance_id, "
                    "projection_generation, analysis_id) VALUES (%s, 1, 'analysis-shared')",
                    (source,))
        for index, track_id in enumerate([*old_ids, gone]):
            cur.execute(
                f"""INSERT INTO {P}track_analysis_links
                        (catalog_instance_id, projection_generation, provider_track_id,
                         {LINK_COLUMNS})
                    VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)""",
                (source, track_id,
                 None if index % 5 == 4 else "analysis-shared",
                 "pending" if index % 5 == 4 else "ready",
                 ("exact", "chromaprint", None)[index % 3],
                 ("fp_v2", None)[index % 2],
                 (0.35, None, 0.1)[index % 3],
                 (0.1234567, None)[index % 2],
                 index % 2 == 0,
                 json.dumps(["chromaprint_disagreement", "provider_evidence_conflict"][:index % 3]),
                 ("accepted", None)[index % 2]))
        cur.execute(
            f"UPDATE {P}analysis_state SET projection_generation=1, item_count=1, "
            "mapped_track_count=%s, status='complete' WHERE catalog_instance_id=%s",
            (track_count + 1, source))
    db.commit()

    target = catalog.normalize_provider_catalog(
        _catalog([_canonical(track_id) for track_id in old_ids],
                 _canonical(ALBUM), _canonical(ARTIST)), "navidrome")
    target_fp = rekey.target_scan_fingerprint(target)
    with db.cursor() as cur:
        previous = {
            entity_type: catalog._published_fingerprints(cur, entity_type, source, 1)
            for entity_type in catalog.ENTITY_ORDER
        }
        cur.execute(f"SELECT catalog_head_seq FROM {P}catalog_state")
        catalog_head = cur.fetchone()[0]
        cur.execute(f"SELECT analysis_head_seq FROM {P}analysis_state")
        analysis_head = cur.fetchone()[0]
        cur.execute(
            f"""UPDATE {P}provider_identity_transitions
                   SET transition_id='transition-a', state='transition_pending',
                       previous_provider_version='0.63.2', current_provider_version='0.64.0',
                       baseline_catalog_generation=1, baseline_analysis_generation=1,
                       target_fingerprint=%s, target_scan_count=2""", (target_fp,))
    db.commit()
    plan = rekey.build_provider_identity_rekey_plan(previous, target)

    log = []
    _record_statements(db, log)
    result = rekey.publish_provider_identity_rekey(
        db, catalog_instance_id=source, server_id="server-a", normalized=target,
        target_fingerprint=target_fp, current_provider_version="0.64.0",
        adapter=MappingCore())
    db.cursor_factory = None
    assert result["provider_identity_transition"]["state"] == "applied"

    evidence = {
        "transition_id": "transition-a", "provider_version_before": "0.63.2",
        "provider_version_after": "0.64.0", "deterministic": True,
        "analysis_identity_preserved": True,
    }
    expected_events = [
        (catalog_head + 1 + index, 2, event.entity_type, event.entity_id, event.operation,
         rekey.REKEY_REASON, event.old_entity_id,
         json.loads(catalog.canonical_json(event.payload)) if event.payload is not None else None,
         evidence, catalog.JOURNAL_WRITER_GENERATION)
        for index, event in enumerate(plan.events)
    ]
    return source, old_ids, gone, log, expected_events, (catalog_head, analysis_head), result


@pytest.mark.parametrize("page_size", [None, 7])
def test_the_rekey_writes_its_journals_a_page_per_statement_with_the_same_rows(
    migrated_db, monkeypatch, page_size
):
    from plugins.LumaeAnalysis import catalog

    db = migrated_db
    load_plugin()
    if page_size:
        chunks = catalog._chunks
        monkeypatch.setattr(catalog, "_chunks", lambda rows, size=page_size: chunks(rows, size))
    track_count = 23
    source, old_ids, gone, log, expected_events, heads, result = _publish_rekey_of(
        db, track_count)
    catalog_head, analysis_head = heads

    # The catalogue journal: the plan's events, in order, from the head.
    events = _rows(db, f"""
        SELECT seq, generation, entity_type, entity_id, operation, change_reason,
               old_entity_id, payload, evidence, writer_generation
          FROM {P}catalog_changes WHERE seq > %s ORDER BY seq""", (catalog_head,))
    assert events == expected_events
    assert len(events) == track_count + 2 + 1  # tracks, album, artist; the gone track
    transition = result["provider_identity_transition"]
    assert (transition["first_seq"], transition["last_seq"]) == (events[0][0], events[-1][0])
    assert _rows(db, f"SELECT catalog_head_seq FROM {P}catalog_state") == [(events[-1][0],)]

    # The carried links: every column of the old row, under the new ID.
    carried = _rows(db, f"""
        SELECT provider_track_id, {LINK_COLUMNS} FROM {P}track_analysis_links
         WHERE projection_generation=2 ORDER BY provider_track_id""")
    before = {row[0]: row[1:] for row in _rows(db, f"""
        SELECT provider_track_id, {LINK_COLUMNS} FROM {P}track_analysis_links
         WHERE projection_generation=1""")}
    assert carried == sorted(
        (_canonical(track_id), *before[track_id]) for track_id in old_ids)
    assert {row[8] if isinstance(row[8], str) else json.dumps(row[8]) for row in carried} == {
        "[]", '["chromaprint_disagreement"]',
        '["chromaprint_disagreement", "provider_evidence_conflict"]'}

    # The analysis journal: per rekeyed link an upsert of the new ID and a
    # delete of the old one, then the link whose track is gone.
    link_payload = {
        row[0]: {"provider_track_id": row[0], "analysis_id": row[1], "status": row[2],
                 "match_tier": row[3], "algorithm": row[4], "decision_threshold": row[5],
                 "distance": row[6], "evidence_complete": row[7], "conflict_flags": row[8],
                 "review_state": row[9]}
        for row in carried
    }
    expected_changes = []
    for old_id in sorted(old_ids):
        new_id = _canonical(old_id)
        expected_changes.append(("analysis_link", new_id, "upsert", link_payload[new_id]))
        expected_changes.append(("analysis_link", old_id, "delete", None))
    expected_changes.append(("analysis_link", gone, "delete", None))
    changes = _rows(db, f"""
        SELECT seq, generation, entity_type, entity_id, operation, payload
          FROM {P}analysis_changes WHERE seq > %s ORDER BY seq""", (analysis_head,))
    assert [row[0] for row in changes] == list(
        range(analysis_head + 1, analysis_head + 1 + len(expected_changes)))
    assert {row[1] for row in changes} == {2}
    assert [row[2:5] for row in changes] == [row[:3] for row in expected_changes]
    for row, expected in zip(changes, expected_changes):
        if expected[3] is None:
            assert row[5] is None
        else:
            assert {**row[5], "decision_threshold": None, "distance": None} == {
                **expected[3], "decision_threshold": None, "distance": None}
            for key in ("decision_threshold", "distance"):
                assert row[5][key] == pytest.approx(expected[3][key])
    assert _rows(db, f"SELECT analysis_head_seq, mapped_track_count FROM {P}analysis_state") == [
        (changes[-1][0], track_count)]

    pages = lambda rows: -(-rows // (page_size or 1000))  # noqa: E731
    assert len(_inserts(log, "catalog_changes")) == pages(len(events))
    assert len(_inserts(log, "analysis_changes")) == pages(len(changes))
    assert len(_inserts(log, "track_analysis_links")) == 1


def test_persisting_held_tracks_rewrites_only_what_changed(migrated_db):
    from plugins.LumaeAnalysis import jellyfin_continuity

    db = migrated_db
    load_plugin()
    _execute(db, f"INSERT INTO {P}catalog_sources (catalog_instance_id, "
                 "current_core_server_id, provider_type, server_name, is_default, "
                 "rebind_status) VALUES ('catalog-j', 'server-j', 'jellyfin', 'J', TRUE, 'active')")
    since = datetime(2026, 11, 1, tzinfo=timezone.utc)
    held = {jid(f"held-{index}"): (since, None if index % 2 else f"fp-{index}")
            for index in range(40)}
    read = (f"SELECT track_id, missing_since, fingerprint_id, rekey_abandoned, xmin::text "
            f"FROM {P}jellyfin_missing_tracks ORDER BY track_id")

    def persist(hold, release=()):
        continuity = jellyfin_continuity.Continuity(normalized=None)
        continuity.hold = dict(hold)
        continuity.release = set(release)
        log = []
        _record_statements(db, log)
        with db.cursor() as cur:
            jellyfin_continuity.persist(cur, "catalog-j", continuity)
        db.commit()
        db.cursor_factory = None
        return _inserts(log, "jellyfin_missing_tracks")

    assert len(persist(held)) == 1
    first = _rows(db, read)
    assert [(row[0], row[1], row[2], row[3]) for row in first] == [
        (track_id, since, fingerprint, False) for track_id, (since, fingerprint) in sorted(
            held.items())]

    # The next refresh holds the same tracks: no row is written again, and a
    # later scan never moves missing_since.
    later = {track_id: (since + timedelta(days=1), fingerprint)
             for track_id, (_since, fingerprint) in held.items()}
    assert len(persist(later)) == 1
    assert _rows(db, read) == first

    # A fingerprint that becomes known is stored; a known one is never
    # replaced; a released track goes.
    unknown = next(track_id for track_id, (_since, fingerprint) in held.items()
                   if fingerprint is None)
    known = next(track_id for track_id, (_since, fingerprint) in held.items() if fingerprint)
    released = sorted(set(held) - {unknown, known})[0]
    persist({**held, unknown: (since, "fp-found"), known: (since, "fp-other")},
            release={released})
    after = {row[0]: row for row in _rows(db, read)}
    before = {row[0]: row for row in first}
    assert released not in after
    assert after[unknown][2] == "fp-found" and after[unknown][4] != before[unknown][4]
    assert after[known] == before[known]
    assert {track_id for track_id in after if after[track_id] != before[track_id]} == {unknown}
