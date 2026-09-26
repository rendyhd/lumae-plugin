"""Real PostgreSQL fencing and public publication regressions."""
from types import SimpleNamespace

import pytest

from test_lumae_analysis import edge_publication_db, lumae_postgres_db, load_plugin
from plugins.LumaeAnalysis import catalog_enrichment, profile_publication


SOURCE = "catalog-a"
PUBLISHED = "plugin_lumae_analysis__published_source_profiles"
CHANGES = "plugin_lumae_analysis__profile_changes"
STATE = "plugin_lumae_analysis__profile_stream_state"
TRACKS = "plugin_lumae_analysis__catalog_tracks"


def _track(db, track_id="new-track", revision="revision-a"):
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {TRACKS}
                (catalog_instance_id, published_generation, track_id, title,
                 metadata_fp, media_fp, analysis_eligible, payload,
                 first_seen_at, last_seen_at)
                VALUES (%s, 1, %s, %s, 'metadata', %s, TRUE,
                        '{{}}'::jsonb, now(), now())
                ON CONFLICT (catalog_instance_id, published_generation, track_id)
                DO UPDATE SET media_fp=EXCLUDED.media_fp""",
            (SOURCE, track_id, track_id, revision),
        )
    db.commit()


def _result(start=b"wave"):
    return SimpleNamespace(
        sample_rate=48000, duration_ms=1234, ref_lufs=-12.5,
        start_ramp_blob=start, end_ramp_blob=b"tail",
    )


def _state(db, track_id):
    with db.cursor() as cur:
        cur.execute(f"SELECT head_seq FROM {STATE} WHERE catalog_instance_id=%s", (SOURCE,))
        head = cur.fetchone()[0]
        cur.execute(
            f"SELECT media_signature, start_ramp FROM {PUBLISHED} "
            "WHERE catalog_instance_id=%s AND track_id=%s",
            (SOURCE, track_id),
        )
        row = cur.fetchone()
        cur.execute(
            f"SELECT seq, operation FROM {CHANGES} "
            "WHERE catalog_instance_id=%s AND track_id=%s ORDER BY seq",
            (SOURCE, track_id),
        )
        events = cur.fetchall()
    db.commit()
    return head, row, events


def test_pending_failure_and_superseded_completion_preserve_published(
    edge_publication_db,
):
    db = edge_publication_db
    _track(db)
    first = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", first, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    baseline = _state(db, "new-track")
    assert baseline[0] == 1 and baseline[2] == [(1, "upsert")]
    stale = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    current = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert not profile_publication.complete_attempt(
        db, SOURCE, "new-track", stale, _result(b"stale"), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", current, object(), "failed", "decode error",
        None, 1, 1,
    )
    assert _state(db, "new-track") == baseline
    # A same-revision retry that produces the same public payload does not
    # rotate its timestamp or append a redundant event.
    retry = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", retry, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert _state(db, "new-track") == baseline
    mod = load_plugin()
    public = mod.fetch_published_profile_rows(["new-track"], SOURCE)
    assert public[0]["track_id"] == "new-track"
    assert catalog_enrichment._profile_rows(db.cursor(), SOURCE, "", 100)


def test_media_revision_admission_withdraws_and_fences_old_attempt(
    edge_publication_db,
):
    db = edge_publication_db
    _track(db)
    first = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", first, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    old = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    _track(db, revision="revision-b")
    new = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert _state(db, "new-track") == (
        2, None, [(1, "upsert"), (2, "delete")]
    )
    assert not profile_publication.complete_attempt(
        db, SOURCE, "new-track", old, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", new, _result(b"new"), "ready", None,
        "catalog-media:revision-b", 1, 1,
    )
    head, row, events = _state(db, "new-track")
    assert head == 3 and row[0] == "catalog-media:revision-b"
    assert events == [(1, "upsert"), (2, "delete"), (3, "upsert")]


def test_tokenless_completion_and_release_cannot_change_publication(
    edge_publication_db,
):
    db = edge_publication_db
    _track(db)
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert not profile_publication.complete_attempt(
        db, SOURCE, "new-track", None, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert profile_publication.release_attempts(
        db, SOURCE, {"new-track": "old-token"}, "old job"
    ) == 0
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert _state(db, "new-track")[0] == 1


def test_catalog_publication_invalidates_known_revision_before_new_analysis(
    edge_publication_db,
):
    db = edge_publication_db
    _track(db)
    first = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", first, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    old = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    with db.cursor() as cur:
        cur.execute(f"UPDATE {TRACKS} SET media_fp='revision-b' WHERE track_id='new-track'")
        withdrawn = profile_publication.invalidate_catalog_changes(
            cur, SOURCE, 1, [("track", "new-track", "upsert", None, False)]
        )
    db.commit()
    assert withdrawn == 1
    assert _state(db, "new-track") == (
        2, None, [(1, "upsert"), (2, "delete")]
    )
    assert not profile_publication.complete_attempt(
        db, SOURCE, "new-track", old, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )


def test_missing_fingerprint_is_not_deletion_evidence(edge_publication_db):
    db = edge_publication_db
    _track(db)
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    baseline = _state(db, "new-track")
    with db.cursor() as cur:
        cur.execute(f"UPDATE {TRACKS} SET media_fp=NULL WHERE track_id='new-track'")
        assert profile_publication.invalidate_catalog_changes(
            cur, SOURCE, 1, [("track", "new-track", "upsert", None, False)]
        ) == 0
    db.commit()
    assert _state(db, "new-track") == baseline


def test_rebase_reconciles_publications_even_without_ordinary_changes(
    edge_publication_db,
):
    db = edge_publication_db
    _track(db)
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    pending = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    with db.cursor() as cur:
        cur.execute(f"UPDATE {TRACKS} SET available=FALSE WHERE track_id='new-track'")
        assert profile_publication.invalidate_catalog_changes(
            cur, SOURCE, 1, [], full_reconcile=True
        ) == 1
    db.commit()
    assert _state(db, "new-track") == (
        2, None, [(1, "upsert"), (2, "delete")]
    )
    assert not profile_publication.complete_attempt(
        db, SOURCE, "new-track", pending, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )


def test_exact_source_rekey_journals_old_delete_new_upsert_and_drops_old_edge(
    edge_publication_db,
):
    from test_lumae_analysis import _edge_payload_for_job
    from plugins.LumaeAnalysis import edge_profile_store

    db = edge_publication_db
    jobs, _ready = edge_profile_store.claim_edge_jobs(db, SOURCE, ["track-a"])
    payload = _edge_payload_for_job(jobs[0])
    assert edge_profile_store.publish_edge_profile(
        db, SOURCE, jobs[0], payload, "private/path:123:456"
    )
    with db.cursor() as cur:
        cur.execute(
            """INSERT INTO plugin_lumae_analysis__catalog_sources
               (catalog_instance_id, current_core_server_id, provider_type,
                server_name, is_default, rebind_status)
               VALUES ('catalog-b', 'server-b', 'navidrome', 'B', FALSE, 'active')"""
        )
        cur.execute(
            f"""INSERT INTO {PUBLISHED}
               SELECT 'catalog-b', track_id, sample_rate, duration_ms, ref_lufs,
                      start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
                      media_signature, analyzed_at
                 FROM {PUBLISHED}
                WHERE catalog_instance_id=%s AND track_id='track-a'""",
            (SOURCE,),
        )
    db.commit()
    with db.cursor() as cur:
        cur.execute(f"UPDATE {TRACKS} SET track_id='track-new' WHERE track_id='track-a'")
        assert profile_publication.rekey_published_profiles(
            cur, SOURCE, [{"old_id": "track-a", "new_id": "track-new"}]
        ) == 1
    db.commit()
    with db.cursor() as cur:
        cur.execute(
            f"SELECT catalog_instance_id, track_id FROM {PUBLISHED} ORDER BY 1, 2"
        )
        assert cur.fetchall() == [
            ("catalog-a", "track-new"), ("catalog-b", "track-a")
        ]
        cur.execute(
            f"SELECT seq, track_id, operation, payload FROM {CHANGES} ORDER BY seq"
        )
        events = cur.fetchall()
        assert [(row[0], row[1], row[2]) for row in events] == [
            (1, "track-a", "upsert"),
            (2, "track-a", "delete"),
            (3, "track-new", "upsert"),
        ]
        assert events[-1][3]["track_id"] == "track-new"
        assert "edge_profile" not in events[-1][3]
        cur.execute(
            "SELECT COUNT(*) FROM plugin_lumae_analysis__edge_profiles "
            "WHERE catalog_instance_id=%s AND track_id='track-a'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT COUNT(*) FROM plugin_lumae_analysis__edge_profile_jobs "
            "WHERE catalog_instance_id=%s AND track_id='track-a'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == 0
    db.commit()
    with db.cursor() as cur:
        page = catalog_enrichment._profile_rows(cur, SOURCE, "", 100)
    assert [row["track_id"] for row in page] == ["track-new"]
    assert "edge_profile" not in page[0]


def test_unknown_media_revision_defers_once_and_reenters_when_known(
    edge_publication_db,
):
    db = edge_publication_db
    mod = load_plugin()
    _track(db, revision=None)
    assert mod.find_backfill_ids(
        10, catalog_instance_id=SOURCE, server_id="server-a"
    ) == ["new-track"]
    assert profile_publication.admit_attempts(db, SOURCE, ["new-track"]) == {}
    with db.cursor() as cur:
        cur.execute(
            "SELECT status, attempt_token FROM plugin_lumae_analysis__source_profiles "
            "WHERE catalog_instance_id=%s AND track_id='new-track'",
            (SOURCE,),
        )
        assert cur.fetchone() == ("deferred_no_media_revision", None)
    db.commit()
    assert mod.find_backfill_ids(
        10, catalog_instance_id=SOURCE, server_id="server-a"
    ) == []
    _track(db, revision="revision-a")
    assert mod.find_backfill_ids(
        10, catalog_instance_id=SOURCE, server_id="server-a"
    ) == ["new-track"]
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert mod.find_backfill_ids(
        10, catalog_instance_id=SOURCE, server_id="server-a"
    ) == []


@pytest.mark.parametrize("unknown_signature", [None, ""])
def test_unknown_stored_signature_survives_claim_rebase_and_failed_replacement(
    edge_publication_db, unknown_signature,
):
    db = edge_publication_db
    _track(db)
    with db.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {PUBLISHED}
                (catalog_instance_id, track_id, sample_rate, duration_ms,
                 ref_lufs, start_ramp, end_ramp, analyzer_ver,
                 profile_schema_ver, media_signature, analyzed_at)
                VALUES (%s, 'new-track', 48000, 1234, -12.5, %s, %s,
                        1, 1, %s, now())""",
            (SOURCE, b"known-wave", b"tail", unknown_signature),
        )
    db.commit()
    baseline = _state(db, "new-track")
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert _state(db, "new-track") == baseline
    with db.cursor() as cur:
        assert profile_publication.invalidate_catalog_changes(
            cur, SOURCE, 1, [], full_reconcile=True
        ) == 0
    db.commit()
    assert _state(db, "new-track") == baseline
    # The rebase fenced the first token. A new attempt can fail without
    # deleting the unknown-signature published waveform.
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, object(), "failed", "decode",
        None, 1, 1,
    )
    assert _state(db, "new-track") == baseline
    stale = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    _track(db, revision="revision-b")
    assert not profile_publication.complete_attempt(
        db, SOURCE, "new-track", stale, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert _state(db, "new-track") == baseline
    replacement = profile_publication.admit_attempts(
        db, SOURCE, ["new-track"]
    )["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", replacement, _result(), "ready", None,
        "catalog-media:revision-b", 1, 1,
    )
    head, row, events = _state(db, "new-track")
    assert head == 1 and row[0] == "catalog-media:revision-b"
    assert events == [(1, "upsert")]


def test_waveform_replacement_invalidates_edge_before_matching_delta(
    edge_publication_db,
):
    from test_lumae_analysis import _edge_payload_for_job
    from plugins.LumaeAnalysis import edge_profile_store

    db = edge_publication_db
    jobs, _ = edge_profile_store.claim_edge_jobs(db, SOURCE, ["track-a"])
    edge = _edge_payload_for_job(jobs[0])
    assert edge_profile_store.publish_edge_profile(
        db, SOURCE, jobs[0], edge, "private/path:123:456",
    )
    token = profile_publication.admit_attempts(db, SOURCE, ["track-a"])["track-a"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "track-a", token, _result(b"replacement"),
        "ready", None, "catalog-media:private/path:123:456", 1, 1,
    )
    with db.cursor() as cur:
        cur.execute(f"SELECT seq, payload, edge_ref FROM {CHANGES} ORDER BY seq")
        changes = cur.fetchall()
        assert [row[0] for row in changes] == [1, 2]
        # K6: the edge publication journals a reference to its edge (never a
        # copy); the replacement references none.
        assert "edge_profile" not in changes[0][1]
        assert changes[0][2] == {"profile_digest": edge["profile_digest"]}
        assert "edge_profile" not in changes[1][1] and changes[1][2] is None
        changes = [(row[0], row[1]) for row in changes]
        page = catalog_enrichment._profile_rows(cur, SOURCE, "", 100)
        assert page == [changes[1][1]]
        cur.execute(
            "SELECT COUNT(*) FROM plugin_lumae_analysis__edge_profiles "
            "WHERE catalog_instance_id=%s AND track_id='track-a'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == 0


def test_source_epoch_change_rejects_old_completion(edge_publication_db):
    db = edge_publication_db
    _track(db)
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    with db.cursor() as cur:
        cur.execute(
            "UPDATE plugin_lumae_analysis__catalog_state "
            "SET catalog_epoch='epoch-replaced' WHERE catalog_instance_id=%s",
            (SOURCE,),
        )
    db.commit()
    assert not profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert _state(db, "new-track") == (0, None, [])


def test_deleted_track_reactivation_readmits_same_revision(
    edge_publication_db,
):
    db = edge_publication_db
    mod = load_plugin()
    _track(db)
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    with db.cursor() as cur:
        cur.execute(f"UPDATE {TRACKS} SET available=FALSE WHERE track_id='new-track'")
        assert profile_publication.invalidate_catalog_changes(
            cur, SOURCE, 1, [("track", "new-track", "delete", None, False)]
        ) == 1
    db.commit()
    assert _state(db, "new-track") == (
        2, None, [(1, "upsert"), (2, "delete")]
    )
    with db.cursor() as cur:
        cur.execute(
            "SELECT status FROM plugin_lumae_analysis__source_profiles "
            "WHERE catalog_instance_id=%s AND track_id='new-track'",
            (SOURCE,),
        )
        assert cur.fetchone()[0] == "stale"
        cur.execute(f"UPDATE {TRACKS} SET available=TRUE WHERE track_id='new-track'")
        assert profile_publication.invalidate_catalog_changes(
            cur, SOURCE, 1, [("track", "new-track", "upsert", None, True)]
        ) == 0
    db.commit()
    assert mod.find_backfill_ids(
        10, catalog_instance_id=SOURCE, server_id="server-a"
    ) == ["new-track"]
    token = profile_publication.admit_attempts(db, SOURCE, ["new-track"])["new-track"]
    assert profile_publication.complete_attempt(
        db, SOURCE, "new-track", token, _result(), "ready", None,
        "catalog-media:revision-a", 1, 1,
    )
    assert _state(db, "new-track")[2] == [
        (1, "upsert"), (2, "delete"), (3, "upsert")
    ]
