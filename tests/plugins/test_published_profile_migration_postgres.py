from datetime import datetime

from test_lumae_analysis import lumae_postgres_db, load_plugin
from plugins.LumaeAnalysis import catalog

SOURCE = "plugin_lumae_analysis__source_profiles"
PUBLISHED = "plugin_lumae_analysis__published_source_profiles"
LEGACY = "plugin_lumae_analysis__profiles"
MARKERS = "plugin_lumae_analysis__profile_migrations"


def _setup(db, monkeypatch, mod, *, two_sources):
    # Each test owns a disposable schema. Keep the real migrate(db) path while
    # disabling external source discovery, scheduling, and queued work.
    catalog.migrate_catalog(db)
    with db.cursor() as cur:
        cur.execute("CREATE TABLE cron (task_type TEXT PRIMARY KEY)")
        cur.execute(
            """INSERT INTO plugin_lumae_analysis__catalog_sources
               (catalog_instance_id, current_core_server_id, provider_type,
                server_name, is_default, rebind_status)
               VALUES ('source-a', 'server-a', 'navidrome', 'A', TRUE, 'active')"""
        )
        if two_sources:
            cur.execute(
                """INSERT INTO plugin_lumae_analysis__catalog_sources
                   (catalog_instance_id, current_core_server_id, provider_type,
                    server_name, is_default, rebind_status)
                   VALUES ('source-b', 'server-b', 'navidrome', 'B', FALSE, 'active')"""
            )
        cur.execute(
            f"""CREATE TABLE {LEGACY} (
                track_id TEXT PRIMARY KEY, sample_rate INTEGER NOT NULL,
                duration_ms INTEGER NOT NULL, ref_lufs REAL NOT NULL,
                start_ramp BYTEA NOT NULL, end_ramp BYTEA NOT NULL,
                analyzer_ver INTEGER NOT NULL, profile_schema_ver INTEGER NOT NULL,
                media_signature TEXT, analyzed_at TIMESTAMP NOT NULL,
                status TEXT NOT NULL, last_error TEXT)"""
        )
        if two_sources:
            cur.execute(
                f"""CREATE TABLE {SOURCE} (
                    catalog_instance_id TEXT NOT NULL
                        REFERENCES plugin_lumae_analysis__catalog_sources(catalog_instance_id)
                        ON DELETE CASCADE,
                    track_id TEXT NOT NULL, sample_rate INTEGER NOT NULL,
                    duration_ms INTEGER NOT NULL, ref_lufs REAL NOT NULL,
                    start_ramp BYTEA NOT NULL, end_ramp BYTEA NOT NULL,
                    analyzer_ver INTEGER NOT NULL, profile_schema_ver INTEGER NOT NULL,
                    media_signature TEXT, analyzed_at TIMESTAMP NOT NULL,
                    status TEXT NOT NULL, last_error TEXT,
                    PRIMARY KEY (catalog_instance_id, track_id))"""
            )
    db.commit()
    monkeypatch.setattr(mod, "ensure_catalog_sources", lambda *_args: None)
    monkeypatch.setattr(mod, "enqueue_required_catalog_preparations", lambda **_kwargs: 0)
    monkeypatch.setattr(mod, "_safe_reconcile_schedule", lambda *_args: None)
    monkeypatch.setattr(mod.music_metadata, "ensure_schedule", lambda *_args: None)
    for name in (
        "ensure_catalog_refresh_schedule",
        "ensure_catalog_reconcile_schedule",
        "ensure_provider_identity_recheck_schedule",
        "ensure_analysis_projection_schedule",
        "disable_legacy_backfill_schedule",
    ):
        monkeypatch.setattr(mod, name, lambda *_args: None)


def _put(cur, table, track, *, source=None, status="ready", start=b"\x00\xff\x10\x80",
         end=b"\x11\x00\xfe", analyzed_at=None, signature="sig:/raw"):
    analyzed_at = analyzed_at or datetime(2023, 8, 9, 10, 11, 12, 345678)
    columns = ("track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, "
               "analyzer_ver, profile_schema_ver, media_signature, analyzed_at, status, last_error")
    values = (track, 48000, 123456, -13.25, start, end, 7, 3,
              signature, analyzed_at, status, "old error" if status == "failed" else None)
    if source is not None:
        columns = "catalog_instance_id, " + columns
        values = (source,) + values
    cur.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({', '.join(['%s'] * len(values))})",
        values,
    )


def _rows(db, table):
    with db.cursor() as cur:
        cur.execute(
            f"""SELECT catalog_instance_id, track_id, sample_rate, duration_ms,
                       ref_lufs, start_ramp, end_ramp, analyzer_ver,
                       profile_schema_ver, media_signature, analyzed_at
                  FROM {table} ORDER BY catalog_instance_id, track_id"""
        )
        return [
            row[:5] + (bytes(row[5]), bytes(row[6])) + row[7:]
            for row in cur.fetchall()
        ]


def test_published_seed_copies_only_ready_source_rows_once(lumae_postgres_db, monkeypatch):
    mod = load_plugin()
    db = lumae_postgres_db
    _setup(db, monkeypatch, mod, two_sources=True)
    stamp = datetime(2024, 2, 29, 23, 59, 58, 987654)
    with db.cursor() as cur:
        _put(cur, SOURCE, "same-track", source="source-a", analyzed_at=stamp,
             start=b"\x00\x80\xff\x01", end=b"\xfe\x00", signature="revision-a")
        _put(cur, SOURCE, "same-track", source="source-b", analyzed_at=stamp,
             start=b"\xff\x00\x7f", end=b"\x00\xff", signature="revision-b")
        for status in ("failed", "pending", "pending_interactive", "stale", "skipped"):
            _put(cur, SOURCE, status, source="source-a", status=status)
    db.commit()

    mod.migrate(db)
    seeded = _rows(db, PUBLISHED)
    assert seeded == [row for row in _rows(db, SOURCE) if row[1] == "same-track"]
    assert len(seeded) == 2
    assert seeded[0][5:7] == (b"\x00\x80\xff\x01", b"\xfe\x00")
    assert seeded[0][-2:] == ("revision-a", stamp)
    with db.cursor() as cur:
        cur.execute(f"SELECT name FROM {MARKERS} ORDER BY name")
        assert [row[0] for row in cur.fetchall()] == [
            "legacy_default_profiles_v1", "published_source_profiles_seed_v1",
            "stale_retry_category_v1",
        ]
        cur.execute(
            f"DELETE FROM {PUBLISHED} WHERE catalog_instance_id='source-b'"
        )
        cur.execute(
            f"UPDATE {SOURCE} SET start_ramp=%s, analyzed_at=now() "
            "WHERE catalog_instance_id='source-a' AND track_id='same-track'",
            (b"replacement",),
        )
        _put(cur, SOURCE, "late-ready", source="source-a")
        cur.execute(
            "UPDATE plugin_lumae_analysis__catalog_sources SET is_default=NOT is_default"
        )
    db.commit()

    mod.migrate(db)
    assert _rows(db, PUBLISHED) == seeded[:1]
    with db.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {SOURCE}")
        assert cur.fetchone()[0] == 8


def test_legacy_one_source_copy_precedes_published_seed(lumae_postgres_db, monkeypatch):
    mod = load_plugin()
    db = lumae_postgres_db
    _setup(db, monkeypatch, mod, two_sources=False)
    stamp = datetime(2021, 7, 6, 5, 4, 3, 210987)
    with db.cursor() as cur:
        _put(cur, LEGACY, "legacy-ready", analyzed_at=stamp,
             start=b"\x01\x00\xff", end=b"\x80\x00", signature="legacy-revision")
        _put(cur, LEGACY, "legacy-failed", status="failed")
    db.commit()

    mod.migrate(db)
    source = _rows(db, SOURCE)
    published = _rows(db, PUBLISHED)
    assert [row[1] for row in source] == ["legacy-failed", "legacy-ready"]
    assert published == [source[1]]
    assert published[0][0] == "source-a"
    assert published[0][5:7] == (b"\x01\x00\xff", b"\x80\x00")
    assert published[0][-2:] == ("legacy-revision", stamp)
    mod.migrate(db)
    assert _rows(db, PUBLISHED) == published
