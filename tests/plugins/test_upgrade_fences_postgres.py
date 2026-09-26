"""P1-3 / AUD-05: fail-closed fences against un-drained 1.2.5 workers.

A rolling restart can leave a 1.2.5 web or RQ worker running next to the
1.3.0 schema. Every journal that such a worker could corrupt now has a column
without a default that only 1.3.0 writers supply, so the old worker's insert
fails and its whole transaction rolls back. The tests use the production
schema (``migrated_db``) and, where it matters, the real 1.2.5 archive code.
"""
import importlib
import pathlib
import sys
import zipfile
from types import SimpleNamespace

import psycopg2
import pytest

from test_lumae_analysis import load_plugin, plugin_client
from test_collection_mutations_postgres import collection_api  # noqa: F401
from plugins.LumaeAnalysis import catalog_enrichment as enrichment
from plugins.LumaeAnalysis import collection_manager as manager
from plugins.LumaeAnalysis import profile_publication as publication


ROOT = pathlib.Path(__file__).resolve().parents[2]
ARCHIVE_125 = ROOT / "dist/lumae_analysis/lumae_analysis_1.2.5.zip"
SOURCE = "catalog-a"
SERVER = "legacy-default"
P = "plugin_lumae_analysis__"


def _seed_source(db, *tracks):
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}catalog_sources
                (catalog_instance_id, current_core_server_id, provider_type,
                 server_name, is_default, rebind_status)
                VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')""",
            (SOURCE, SERVER),
        )
        cur.execute(
            f"""INSERT INTO {P}catalog_state
                (catalog_instance_id, current_core_server_id, provider_type,
                 published_generation, catalog_epoch, status)
                VALUES (%s, %s, 'navidrome', 1, 'epoch-a', 'complete')""",
            (SOURCE, SERVER),
        )
        for track in tracks:
            cur.execute(
                f"""INSERT INTO {P}catalog_tracks
                    (catalog_instance_id, published_generation, track_id, title,
                     metadata_fp, media_fp, analysis_eligible, payload,
                     first_seen_at, last_seen_at)
                    VALUES (%s, 1, %s, %s, 'metadata', 'rev-a', TRUE, '{{}}'::jsonb,
                            now(), now())""",
                (SOURCE, track, track),
            )
    db.commit()
    enrichment.migrate_enrichment(db)  # creates the per-source stream state
    db.commit()


def _result():
    return SimpleNamespace(
        sample_rate=48000, duration_ms=1234, ref_lufs=-14.0,
        start_ramp_blob=b"wave", end_ramp_blob=b"tail",
    )


def _column(db, table_name, column):
    with db.cursor() as cur:
        cur.execute(
            "SELECT is_nullable, column_default FROM information_schema.columns "
            "WHERE table_schema=current_schema() AND table_name=%s AND column_name=%s",
            (table_name, column),
        )
        row = cur.fetchone()
    db.rollback()
    return row


def _scalar(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        value = cur.fetchone()[0]
    db.rollback()
    return value


@pytest.fixture
def plugin_125(tmp_path, monkeypatch):
    """Import the published 1.2.5 archive as its own package."""
    package = tmp_path / "lumae_analysis_125"
    with zipfile.ZipFile(ARCHIVE_125) as archive:
        archive.extractall(package)
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        yield importlib.import_module("lumae_analysis_125")
    finally:
        for name in [name for name in sys.modules if name.startswith("lumae_analysis_125")]:
            del sys.modules[name]


# -- 1. version and the existing preparation attestation ----------------------


def test_plugin_version_is_1_3_0_and_differs_from_the_pinned_release():
    mod = load_plugin()
    assert mod.PLUGIN_VERSION == "1.3.0"
    # release-sources.json stays pinned at 1.2.5 until P4-1, so a 1.2.5 worker
    # and this source are now distinguishable by the attestation.
    with zipfile.ZipFile(ARCHIVE_125) as archive:
        assert b'PLUGIN_VERSION = "1.2.5"' in archive.read("__init__.py")


def test_migration_retargets_preparation_and_fences_a_1_2_5_worker(
    migrated_db, run_plugin_migration, monkeypatch
):
    mod = load_plugin()
    _seed_source(migrated_db)
    with migrated_db.cursor() as cur:
        # Work admitted by the 1.2.5 process before the upgrade.
        cur.execute(
            f"""INSERT INTO {P}preparation_state
                (catalog_instance_id, server_id, status, phase,
                 target_plugin_version, target_catalog_builder_version)
                VALUES (%s, %s, 'queued', 'queued', '1.2.5', %s)""",
            (SOURCE, SERVER, mod.CATALOG_BUILDER_VERSION),
        )
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    state = mod.preparation_state(SOURCE)
    assert state["target_plugin_version"] == "1.3.0"

    mod.assert_preparation_worker_current(SOURCE)  # the 1.3.0 worker runs it
    monkeypatch.setattr(mod, "PLUGIN_VERSION", "1.2.5")
    with pytest.raises(RuntimeError, match="worker is still running 1.2.5"):
        mod.assert_preparation_worker_current(SOURCE)


# -- 2. collections: seq has no default ----------------------------------------


def test_migration_drops_the_collection_seq_default_and_is_idempotent(
    migrated_db, run_plugin_migration
):
    table_name = manager.collection_changes_table()
    assert _column(migrated_db, table_name, "seq") == ("NO", None)
    for name in ("profile_changes", "catalog_changes"):
        assert _column(migrated_db, P + name, "writer_generation") == ("NO", None)
    run_plugin_migration(migrated_db)
    run_plugin_migration(migrated_db)
    assert _column(migrated_db, table_name, "seq") == ("NO", None)
    for name in ("profile_changes", "catalog_changes"):
        assert _column(migrated_db, P + name, "writer_generation") == ("NO", None)
    # The sequence stays owned by the column (a 1.2.5 rollback can re-use it).
    assert _scalar(
        migrated_db, "SELECT pg_get_serial_sequence(%s, 'seq')", (table_name,)
    ) is not None


def test_raw_1_2_5_collection_insert_fails_and_new_writes_continue(
    migrated_db, second_connection
):
    changes = manager.collection_changes_table()
    with second_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.NotNullViolation):
            cur.execute(
                f"INSERT INTO {changes} (principal, collection_id, entity_kind, "
                "entity_id, operation, payload) "
                "VALUES ('user:alice','a','collection','a','upsert','{}')"
            )
    second_connection.rollback()
    with migrated_db.cursor() as cur:
        manager._record_change(cur, "user:alice", "a", "collection", "a", "upsert", {})
        manager._record_change(cur, "user:bob", "b", "collection", "b", "upsert", {})
    migrated_db.commit()
    assert _scalar(migrated_db, f"SELECT array_agg(seq ORDER BY seq) FROM {changes}") == [1, 2]
    assert _scalar(
        migrated_db, f"SELECT head_seq FROM {manager.collection_feed_state_table()}"
    ) == 2


def test_legacy_writer_probe_inverted(collection_api):
    """docs/audit/.../collections/test_probe_legacy_writer.py, inverted."""
    manager_mod, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "a", "name": "a"}).status_code == 201
    db = connect()
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_schema=current_schema() AND table_name=%s AND column_name='seq'",
                (manager_mod.collection_changes_table(),),
            )
            default = cur.fetchone()
            attempts = []
            for _ in range(5):  # what an un-drained 1.2.5 worker does
                cur.execute("SAVEPOINT s")
                try:
                    cur.execute(
                        f"INSERT INTO {manager_mod.collection_changes_table()} "
                        "(principal, collection_id, entity_kind, entity_id, operation, payload)"
                        " VALUES ('user:alice','a','collection','a','upsert','{}') RETURNING seq"
                    )
                    attempts.append("ok")
                except psycopg2.errors.NotNullViolation:
                    cur.execute("ROLLBACK TO SAVEPOINT s")
                    attempts.append("NotNullViolation")
                except psycopg2.Error as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT s")
                    attempts.append(type(exc).__name__)
        db.commit()
    finally:
        db.rollback()
        db.close()
    assert default == (None,)
    assert attempts == ["NotNullViolation"] * 5
    assert [call("PATCH", "/api/collections/a", {"name": f"n{i}"}).status_code
            for i in range(3)] == [200, 200, 200]
    assert call("POST", "/api/collections", {"id": "z", "name": "z"}, user="bob").status_code == 201


def test_real_1_2_5_collection_writer_fails_closed(collection_api, plugin_125, monkeypatch):
    manager_mod, call, connect = collection_api
    old = plugin_125.collection_manager
    db = connect()
    try:
        with db.cursor() as cur:
            with pytest.raises(psycopg2.errors.NotNullViolation):
                old._record_change(cur, "user:alice", "a", "collection", "a", "upsert", {})
    finally:
        db.rollback()
        db.close()
    assert call("POST", "/api/collections", {"id": "a", "name": "a"}).status_code == 201


# -- 4a. collections feed invariant -------------------------------------------


def _collections_feed_ok(connect):
    db = connect()
    try:
        with db.cursor() as cur:
            return manager.collection_feed_integrity(cur)
    finally:
        db.rollback()
        db.close()


def test_feed_invariant_violation_blocks_writes_with_503_and_repair_sql(collection_api, request):
    manager_mod, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "a", "name": "a"}).status_code == 201
    assert _collections_feed_ok(connect) is True
    changes = manager_mod.collection_changes_table()
    state = manager_mod.collection_feed_state_table()
    db = connect()
    request.addfinalizer(db.close)
    with db.cursor() as cur:
        # A row past head, as a pre-fence 1.2.5 worker (or a manual edit) left it.
        cur.execute(
            f"INSERT INTO {changes} (seq, principal, collection_id, entity_kind, "
            "entity_id, operation, payload) "
            "VALUES (5, 'user:alice', 'a', 'collection', 'a', 'upsert', '{}')"
        )
    db.commit()
    assert _collections_feed_ok(connect) is False
    for user in ("alice", "bob"):
        response = call("POST", "/api/collections", {"id": f"x-{user}", "name": "x"}, user=user)
        assert response.status_code == 503
        assert response.get_json() == {"error": "collection_feed_invariant"}
    response = call("PATCH", "/api/collections/a", {"name": "renamed"})
    assert response.status_code == 503
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {state}")
        assert cur.fetchone()[0] == 1  # the rejected writes rolled the head back
        cur.execute(f"SELECT name FROM {manager_mod.collections_table()} WHERE id='a'")
        assert cur.fetchone()[0] == "a"
        # Runbook repair: realign the head past every committed row.
        cur.execute(
            f"UPDATE {state} SET head_seq = GREATEST(head_seq, "
            f"(SELECT COALESCE(MAX(seq), 0) FROM {changes})) WHERE singleton = 1"
        )
    db.commit()
    db.close()
    assert _collections_feed_ok(connect) is True
    assert call("PATCH", "/api/collections/a", {"name": "renamed"}).status_code == 200
    assert call("POST", "/api/collections", {"id": "z", "name": "z"}, user="bob").status_code == 201


# -- 3. profiles: writer_generation fence --------------------------------------


def test_record_profile_change_writes_generation_2(migrated_db):
    _seed_source(migrated_db, "t1")
    with migrated_db.cursor() as cur:
        enrichment.record_profile_change(cur, SOURCE, "t1", "deleted")
    migrated_db.commit()
    assert _scalar(
        migrated_db, f"SELECT array_agg(writer_generation) FROM {P}profile_changes"
    ) == [2]


def test_old_style_profile_change_insert_fails_and_rolls_back_its_publication(
    migrated_db, second_connection
):
    _seed_source(migrated_db, "t1")
    with second_connection.cursor() as cur:
        # 1.2.5 upsert_profile: source row, then journal, one transaction.
        cur.execute(
            f"""INSERT INTO {P}source_profiles
                (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                 start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                 media_signature, analyzed_at, status)
                VALUES (%s, 't1', 48000, 1234, -14, 'a', 'b', 1, 1,
                        'catalog-media:rev-a', now(), 'ready')""",
            (SOURCE,),
        )
        with pytest.raises(psycopg2.errors.NotNullViolation):
            cur.execute(
                f"""INSERT INTO {P}profile_changes
                    (catalog_instance_id, epoch, seq, track_id, operation, payload)
                    VALUES (%s, 'e', 1, 't1', 'upsert', '{{}}')""",
                (SOURCE,),
            )
    second_connection.rollback()
    assert _scalar(migrated_db, f"SELECT count(*) FROM {P}source_profiles") == 0
    assert _scalar(migrated_db, f"SELECT count(*) FROM {P}profile_changes") == 0


def test_real_1_2_5_profile_writer_rolls_back_whole_publication(
    migrated_db, second_connection, plugin_125, monkeypatch
):
    _seed_source(migrated_db, "t1", "t2")
    old = plugin_125
    monkeypatch.setattr(old, "get_db", lambda: second_connection)
    with pytest.raises(psycopg2.errors.NotNullViolation):
        old.upsert_profile(
            "t1", _result(), "ready", media_sig="catalog-media:rev-a",
            catalog_instance_id=SOURCE,
        )
    second_connection.rollback()
    assert _scalar(migrated_db, f"SELECT count(*) FROM {P}source_profiles") == 0
    assert _scalar(migrated_db, f"SELECT count(*) FROM {P}profile_changes") == 0
    # The 1.3.0 publisher is unaffected and publishes row and journal together.
    token = publication.admit_attempts(migrated_db, SOURCE, ["t2"])["t2"]
    assert publication.complete_attempt(
        migrated_db, SOURCE, "t2", token, _result(), "ready", None,
        "catalog-media:rev-a", 1, 1,
    )
    assert _scalar(
        migrated_db, f"SELECT count(*) FROM {P}published_source_profiles WHERE track_id='t2'"
    ) == 1
    assert _scalar(
        migrated_db,
        f"SELECT array_agg(writer_generation) FROM {P}profile_changes WHERE track_id='t2'",
    ) == [2]


def test_old_style_catalog_change_insert_fails(migrated_db, second_connection):
    with second_connection.cursor() as cur:
        with pytest.raises(psycopg2.errors.NotNullViolation):
            # 1.2.5 catalogue publication and rekey omit the column.
            cur.execute(
                f"""INSERT INTO {P}catalog_changes
                    (catalog_instance_id, epoch, seq, generation, entity_type,
                     entity_id, operation, change_reason, payload)
                    VALUES ('c', 'e', 1, 1, 'track', 't', 'upsert', 'provider_diff', NULL)"""
            )
    second_connection.rollback()


def test_real_rekey_publication_writes_the_fenced_catalog_journal(migrated_db):
    """The 1.3.0 provider-identity rekey publishes through catalog_changes."""
    from test_lumae_analysis import RefreshBridge, _identity_fixture_catalog
    from plugins.LumaeAnalysis import catalog
    from plugins.LumaeAnalysis.provider_identity import canonicalize_navidrome_id
    from plugins.LumaeAnalysis import provider_identity_rekey as rekey

    old_ids = ("e3b7fc2ae9447bbec37a13bf916e3cf6", "0123456789abcdef0123456789abcdef",
               "11111111111111111111111111111111")
    new_ids = tuple(canonicalize_navidrome_id(value).value for value in old_ids)
    published = catalog.refresh_catalog(
        "server-a", db=migrated_db, bridge=RefreshBridge(_identity_fixture_catalog(*old_ids)),
    )
    source = published["catalog_instance_id"]
    target = catalog.normalize_provider_catalog(_identity_fixture_catalog(*new_ids), "navidrome")
    target_fp = rekey.target_scan_fingerprint(target)
    with migrated_db.cursor() as cur:
        cur.execute("CREATE TABLE task_status (status TEXT, task_type TEXT)")
        cur.execute(f"SELECT published_generation FROM {P}catalog_state "
                    "WHERE catalog_instance_id=%s", (source,))
        generation = cur.fetchone()[0]
        cur.execute(
            f"""INSERT INTO {P}provider_identity_transitions
                (catalog_instance_id, transition_id, state, previous_provider_version,
                 current_provider_version, baseline_catalog_generation,
                 baseline_analysis_generation, target_fingerprint, target_scan_count)
                VALUES (%s, 'transition-a', 'transition_pending', '0.63.0', '0.64.0',
                        %s, 0, %s, 2)
                ON CONFLICT (catalog_instance_id) DO UPDATE SET
                    transition_id=EXCLUDED.transition_id, state=EXCLUDED.state,
                    previous_provider_version=EXCLUDED.previous_provider_version,
                    current_provider_version=EXCLUDED.current_provider_version,
                    baseline_catalog_generation=EXCLUDED.baseline_catalog_generation,
                    baseline_analysis_generation=EXCLUDED.baseline_analysis_generation,
                    target_fingerprint=EXCLUDED.target_fingerprint,
                    target_scan_count=EXCLUDED.target_scan_count""",
            (source, generation, target_fp),
        )
    migrated_db.commit()

    result = rekey.publish_provider_identity_rekey(
        migrated_db,
        catalog_instance_id=source,
        server_id="server-a",
        normalized=target,
        target_fingerprint=target_fp,
        current_provider_version="0.64.0",
        adapter=SimpleNamespace(analysis_mapping_sql=lambda: "SELECT 1 WHERE FALSE"),
    )

    assert result["provider_identity_transition"]["state"] == "applied"
    rows = _scalar(
        migrated_db,
        f"SELECT array_agg(change_reason || ':' || writer_generation ORDER BY seq) "
        f"FROM {P}catalog_changes WHERE change_reason=%s",
        (rekey.REKEY_REASON,),
    )
    assert rows == [f"{rekey.REKEY_REASON}:2"] * 3


# -- 4b. profiles: ready but unpublished ---------------------------------------


def test_unpublished_ready_counter_flags_an_injected_row(migrated_db):
    mod = load_plugin()
    _seed_source(migrated_db, "t1", "t2", "t3")
    token = publication.admit_attempts(migrated_db, SOURCE, ["t1"])["t1"]
    assert publication.complete_attempt(
        migrated_db, SOURCE, "t1", token, _result(), "ready", None,
        "catalog-media:rev-a", 1, 1,
    )
    assert mod.profiles_unpublished_ready_count(migrated_db) == 0
    with migrated_db.cursor() as cur:
        # What a pre-fence 1.2.5 worker left: a current 'ready' row with no
        # published row (t2), plus rows that must not count: stale media (t3)
        # and a non-ready row.
        for track, signature, status in (
            ("t2", "catalog-media:rev-a", "ready"),
            ("t3", "catalog-media:old", "ready"),
            ("t4", "catalog-media:rev-a", "failed"),
        ):
            cur.execute(
                f"""INSERT INTO {P}source_profiles
                    (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                     start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                     media_signature, analyzed_at, status)
                    VALUES (%s, %s, 48000, 1234, -14, 'a', 'b', 1, 1, %s, now(), %s)""",
                (SOURCE, track, signature, status),
            )
    migrated_db.commit()
    assert mod.profiles_unpublished_ready_count(migrated_db) == 1


def _runbook_sql(marker):
    text = (ROOT / "docs/runbooks/UPGRADE_1.3.md").read_text(encoding="utf-8")
    start = text.index(marker)
    return text[start:text.index("```", start)]


def test_runbook_repair_b_readmits_unpublished_ready_rows(migrated_db):
    mod = load_plugin()
    _seed_source(migrated_db, "t1", "t2")
    token = publication.admit_attempts(migrated_db, SOURCE, ["t1"])["t1"]
    assert publication.complete_attempt(
        migrated_db, SOURCE, "t1", token, _result(), "ready", None,
        "catalog-media:rev-a", 1, 1,
    )
    with migrated_db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {P}source_profiles
                (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                 start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                 media_signature, analyzed_at, status)
                VALUES (%s, 't2', 48000, 1234, -14, 'a', 'b', 1, 1,
                        'catalog-media:rev-a', now(), 'ready')""",
            (SOURCE,),
        )
    migrated_db.commit()
    assert mod.profiles_unpublished_ready_count(migrated_db) == 1
    with migrated_db.cursor() as cur:
        cur.execute(_runbook_sql("-- Repair: re-admit them for republication."))
    migrated_db.commit()
    assert mod.profiles_unpublished_ready_count(migrated_db) == 0
    assert _scalar(
        migrated_db,
        f"SELECT array_agg(track_id || ':' || status ORDER BY track_id) FROM {P}source_profiles",
    ) == ["t1:ready", "t2:stale"]


def test_runbook_repair_a_realigns_the_feed_head(collection_api):
    manager_mod, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "a", "name": "a"}).status_code == 201
    db = connect()
    try:
        with db.cursor() as cur:
            cur.execute(
                f"INSERT INTO {manager_mod.collection_changes_table()} (seq, principal, "
                "collection_id, entity_kind, entity_id, operation, payload) "
                "VALUES (7, 'user:alice', 'a', 'collection', 'a', 'upsert', '{}')"
            )
        db.commit()
        assert call("PATCH", "/api/collections/a", {"name": "b"}).status_code == 503
        sql = _runbook_sql("BEGIN;\nLOCK TABLE plugin_lumae_analysis__collection_changes")
        with db.cursor() as cur:
            cur.execute(sql)
        db.commit()
    finally:
        db.rollback()
        db.close()
    assert _collections_feed_ok(connect) is True
    assert call("PATCH", "/api/collections/a", {"name": "b"}).status_code == 200
    changes = [row["seq"] for row in call("GET", "/api/collections/changes?cursor=0").get_json()["changes"]]
    assert changes == [1, 7, 8]


def test_runbook_repair_c_rotates_the_feed_epoch(collection_api):
    manager_mod, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "a", "name": "a"}).status_code == 201
    before = call("GET", "/api/collections/changes").get_json()
    db = connect()
    try:
        with db.cursor() as cur:
            cur.execute(_runbook_sql(
                "UPDATE plugin_lumae_analysis__collection_feed_state\n   SET epoch = gen_random_uuid()"))
        db.commit()
    finally:
        db.rollback()
        db.close()
    stale = call("GET", f"/api/collections/changes?cursor={before['next_cursor']}"
                        f"&epoch={before['epoch']}")
    assert stale.status_code == 410
    assert stale.get_json() == {"error": "collections_resync_required", "reason": "epoch_mismatch"}
    after = call("GET", "/api/collections/changes").get_json()
    assert after["epoch"] != before["epoch"]
    assert (after["head_seq"], after["floor_seq"]) == (before["head_seq"], before["head_seq"])
    # History and clients that do not echo the epoch are unaffected.
    assert after["changes"] == before["changes"]
    assert call("GET", f"/api/collections/changes?cursor={after['head_seq']}"
                       f"&epoch={after['epoch']}").status_code == 200


# -- health -------------------------------------------------------------------


def test_health_reports_integrity(migrated_db, monkeypatch):
    mod = load_plugin()
    _seed_source(migrated_db, "t1")
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    body = plugin_client(mod).get("/api/health").get_json()
    assert body["plugin_version"] == mod.PLUGIN_VERSION == "1.3.0"
    integrity = body["integrity"]
    assert set(integrity) == {
        "collections_feed_ok", "profiles_unpublished_ready", "profiles_orphaned",
        "profiles_checked_at", "fences_installed",
    }
    assert integrity["collections_feed_ok"] is True
    assert integrity["fences_installed"] is True
    assert integrity["profiles_unpublished_ready"] == 0  # counted by the migration
    assert integrity["profiles_orphaned"] == 0
    assert integrity["profiles_checked_at"].endswith("Z")
    with migrated_db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {manager.collection_changes_table()} (seq, principal, collection_id, "
            "entity_kind, entity_id, operation, payload) "
            "VALUES (9, 'user:alice', 'a', 'collection', 'a', 'upsert', '{}')"
        )
        cur.execute(
            f"""INSERT INTO {P}source_profiles
                (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                 start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                 media_signature, analyzed_at, status)
                VALUES (%s, 't1', 48000, 1234, -14, 'a', 'b', 1, 1,
                        'catalog-media:rev-a', now(), 'ready')""",
            (SOURCE,),
        )
        # P3-7: a published profile of a track the catalogue does not have.
        cur.execute(
            f"""INSERT INTO {P}published_source_profiles
                (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs,
                 start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                 media_signature, analyzed_at)
                VALUES (%s, 'gone', 48000, 1234, -14, 'a', 'b', 1, 1,
                        'catalog-media:rev-a', now())""",
            (SOURCE,),
        )
    migrated_db.commit()
    body = plugin_client(mod).get("/api/health").get_json()
    # The feed check is live; the profile counts are the persisted snapshot.
    assert body["integrity"]["collections_feed_ok"] is False
    assert body["integrity"]["profiles_unpublished_ready"] == 0
    assert body["integrity"]["profiles_orphaned"] == 0
    logged = []
    monkeypatch.setattr(mod, "logger", SimpleNamespace(
        error=lambda *args: logged.append(("error", args[0])),
        warning=lambda *args: logged.append(("warning", args[0])),
        exception=lambda *args: logged.append(("exception", args[0])),
    ))
    status = mod.log_integrity_on_start(migrated_db)  # the web-worker start hook
    assert status["collections_feed_ok"] is False
    assert status["profiles_unpublished_ready"] == 1
    assert status["profiles_orphaned"] == 1
    assert [level for level, _ in logged] == ["error", "warning", "warning"]
    assert "feed invariant" in logged[0][1]
    assert "no longer in the catalogue" in logged[2][1]
    body = plugin_client(mod).get("/api/health").get_json()
    assert body["integrity"]["profiles_unpublished_ready"] == 1
    assert body["integrity"]["profiles_orphaned"] == 1


@pytest.mark.parametrize("undo", [
    "ALTER TABLE {p}profile_changes ALTER COLUMN writer_generation SET DEFAULT 2",
    "ALTER TABLE {p}catalog_changes DROP COLUMN writer_generation",
    "ALTER TABLE {p}collection_changes ALTER COLUMN seq "
    "SET DEFAULT nextval('{p}collection_changes_seq_seq')",
])
def test_fences_installed_detects_a_missing_fence(migrated_db, run_plugin_migration, undo):
    mod = load_plugin()
    assert mod.integrity_status(migrated_db)["fences_installed"] is True
    migrated_db.rollback()
    with migrated_db.cursor() as cur:
        cur.execute(undo.format(p=P))
    migrated_db.commit()
    assert mod.integrity_status(migrated_db)["fences_installed"] is False
    migrated_db.rollback()
    # Re-running the install puts every fence back.
    run_plugin_migration(migrated_db)
    assert mod.integrity_status(migrated_db)["fences_installed"] is True
    migrated_db.rollback()


def test_runbook_fence_check_sql(migrated_db):
    with migrated_db.cursor() as cur:
        cur.execute(_runbook_sql("-- Fence check"))
        rows = cur.fetchall()
    migrated_db.rollback()
    assert sorted(rows) == [
        (P + "catalog_changes.writer_generation", True),
        (P + "collection_changes.seq", True),
        (P + "profile_changes.writer_generation", True),
    ]


def test_health_integrity_stays_cheap(migrated_db, monkeypatch):
    """Health runs two index lookups, never the profile anti-join."""
    mod = load_plugin()
    statements = []
    real_cursor = migrated_db.cursor

    class Recording:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=None):
            statements.append(sql)
            return self._cur.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    db = SimpleNamespace(cursor=lambda: Recording(real_cursor()),
                         rollback=migrated_db.rollback)
    status = mod.integrity_status(db)
    assert status["collections_feed_ok"] is True
    assert not any("source_profiles" in sql for sql in statements)


def test_health_integrity_is_null_without_a_database(monkeypatch):
    mod = load_plugin()
    monkeypatch.setattr(mod, "get_db", lambda: None)
    body = plugin_client(mod).get("/api/health").get_json()
    assert body["integrity"] == {
        "collections_feed_ok": None,
        "profiles_unpublished_ready": None,
        "profiles_orphaned": None,
        "profiles_checked_at": None,
        "fences_installed": None,
    }


# -- 4. P2-3: the edge sweep runs after the published-profile seed -----------


@pytest.fixture
def unmigrated_db():
    """A fresh schema without any plugin migration, for a real 1.2.5 install."""
    import uuid

    from pg_helpers import connect, drop_schema

    schema = f"lumae_upgrade_{uuid.uuid4().hex}"
    db = connect("public")
    try:
        with db.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(f"SET search_path TO {schema}, public")
            cur.execute("CREATE TABLE cron (name TEXT, task_type TEXT UNIQUE, "
                        "cron_expr TEXT, enabled BOOLEAN)")
        db.commit()
        yield db
    finally:
        db.rollback()
        db.close()
        drop_schema(schema)


def _rows(db, sql):
    with db.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    db.rollback()
    return rows


def test_1_2_5_upgrade_keeps_the_edges_of_seeded_publications(
    unmigrated_db, plugin_125, run_plugin_migration, monkeypatch
):
    """A 1.2.5 install has edges for its ready profiles but no published rows:
    migrate seeds those rows from the ready profiles, and only then may the
    maintenance sweep (compact_enrichment_storage) delete edges no published
    profile reaches. Swept: a failed profile's edge and an older revision's."""
    from test_lumae_analysis import RefreshBridge

    db, old = unmigrated_db, plugin_125
    monkeypatch.setattr(old, "enqueue_required_catalog_preparations", lambda **_kw: 0)
    monkeypatch.setattr(old, "_safe_reconcile_schedule", lambda *_a, **_kw: None)
    monkeypatch.setattr(old, "get_db", lambda: db)
    old.migrate(db)
    db.commit()
    with db.cursor() as cur:
        # 1.2.5 created the legacy-default source; its catalogue is server-a.
        for name in ("catalog_sources", "catalog_state"):
            cur.execute(f"UPDATE {P}{name} SET current_core_server_id='server-a' "
                        "WHERE current_core_server_id='legacy-default'")
    db.commit()
    tracks = [{"id": f"t{index}", "title": f"Song {index}", "duration": 100 + index}
              for index in range(4)]
    source = old.catalog.refresh_catalog(
        "server-a", db=db, bridge=RefreshBridge({"tracks": tracks})
    )["catalog_instance_id"]
    revisions = dict(_rows(db, f"SELECT track_id, 'catalog-media:' || media_fp "
                               f"FROM {P}catalog_tracks WHERE catalog_instance_id='{source}'"))
    for track, status in (("t0", "ready"), ("t1", "ready"), ("t2", "ready"), ("t3", "failed")):
        old.upsert_profile(track, _result(), status, media_sig=revisions[track],
                           catalog_instance_id=source)
    with db.cursor() as cur:
        edges = [(track, revisions[track]) for track in ("t0", "t1", "t2", "t3")]
        edges.append(("t0", "catalog-media:older-revision"))
        for track, signature in edges:
            cur.execute(
                f"""INSERT INTO {P}edge_profiles
                    (catalog_instance_id, track_id, media_revision, representation_id,
                     media_signature, profile_digest, payload)
                    VALUES (%s, %s, %s, 'rep', %s, 'digest', '{{}}'::jsonb)""",
                (source, track, "rev:" + signature, signature),
            )
    db.commit()
    edge_sql = f"SELECT track_id, media_signature FROM {P}edge_profiles ORDER BY 1, 2"
    assert len(_rows(db, edge_sql)) == 5
    assert _rows(db, f"SELECT to_regclass('{P}published_source_profiles')") == [(None,)]

    run_plugin_migration(db)
    published_sql = (f"SELECT track_id, media_signature FROM {P}published_source_profiles "
                     "ORDER BY 1")
    ready = [(track, revisions[track]) for track in ("t0", "t1", "t2")]
    assert _rows(db, published_sql) == ready
    assert _rows(db, edge_sql) == ready
    journal_sql = f"SELECT * FROM {P}profile_changes ORDER BY epoch, seq"
    before = (_rows(db, published_sql), _rows(db, edge_sql), _rows(db, journal_sql))
    run_plugin_migration(db)
    assert (_rows(db, published_sql), _rows(db, edge_sql), _rows(db, journal_sql)) == before
