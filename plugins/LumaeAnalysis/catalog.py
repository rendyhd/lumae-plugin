"""Plugin-owned provider catalogue schema and source identity management."""

from datetime import datetime, timezone
import uuid

from plugin.api import table

from .catalog_providers import ProviderCatalogBridge


CATALOG_SCHEMA_VERSION = 2
ANALYSIS_SCHEMA_VERSION = 2


def t(name):
    return table(name)


def utc_now():
    return datetime.now(timezone.utc)


def migrate_catalog(db):
    """Create the complete v2 catalogue/analysis storage idempotently."""
    cur = db.cursor()
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_sources')} (
            catalog_instance_id TEXT PRIMARY KEY,
            current_core_server_id TEXT,
            provider_type TEXT NOT NULL,
            server_name TEXT NOT NULL,
            is_default BOOLEAN NOT NULL DEFAULT FALSE,
            continuity_from TEXT,
            rebind_status TEXT NOT NULL DEFAULT 'active',
            candidate_core_server_id TEXT,
            provider_instance_fp TEXT,
            library_scope_fp TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {t('idx_catalog_source_core')}
        ON {t('catalog_sources')} (current_core_server_id)
        WHERE current_core_server_id IS NOT NULL AND rebind_status = 'active'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_state')} (
            catalog_instance_id TEXT PRIMARY KEY,
            current_core_server_id TEXT,
            provider_type TEXT NOT NULL,
            catalog_schema_version INTEGER NOT NULL DEFAULT {CATALOG_SCHEMA_VERSION},
            published_generation BIGINT NOT NULL DEFAULT 0,
            catalog_epoch TEXT NOT NULL,
            catalog_head_seq BIGINT NOT NULL DEFAULT 0,
            catalog_floor_seq BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'not_initialized',
            scan_mode TEXT NOT NULL DEFAULT 'full_diff',
            entity_counts JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            field_support JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            field_coverage JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            scope_summary JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        _entity_table_sql(
            "catalog_libraries",
            "library_id",
            "name TEXT NOT NULL, sort_name TEXT, display_order INTEGER",
            include_media_fps=False,
        ),
        _entity_table_sql(
            "catalog_artists",
            "artist_id",
            "name TEXT NOT NULL, sort_name TEXT, identity_provenance TEXT, cover_art_id TEXT",
            include_media_fps=False,
        ),
        _entity_table_sql(
            "catalog_albums",
            "album_id",
            "name TEXT NOT NULL, sort_name TEXT, album_artist_display TEXT, added_at TIMESTAMPTZ, "
            "release_type TEXT, content_kind TEXT, cover_art_id TEXT",
            include_media_fps=False,
            include_artwork_fp=True,
        ),
        _entity_table_sql(
            "catalog_tracks",
            "track_id",
            "album_id TEXT, title TEXT NOT NULL, artist_display TEXT, album_artist_display TEXT, "
            "disc_number INTEGER, track_number INTEGER, duration_ms BIGINT, content_kind TEXT, "
            "release_type TEXT, cover_art_id TEXT, streamable BOOLEAN, downloadable BOOLEAN, "
            "analysis_eligible BOOLEAN",
            include_media_fps=True,
            include_artwork_fp=True,
        ),
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_track_artists')} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            track_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            artist_id TEXT,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'artist',
            identity_provenance TEXT,
            payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, published_generation, track_id, position, role)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_album_artists')} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            album_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            artist_id TEXT,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'album_artist',
            identity_provenance TEXT,
            payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, published_generation, album_id, position, role)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_entity_libraries')} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            library_id TEXT NOT NULL,
            PRIMARY KEY (catalog_instance_id, published_generation, entity_type, entity_id, library_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_disc_titles')} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            album_id TEXT NOT NULL,
            disc_number INTEGER NOT NULL,
            title TEXT,
            cover_art_id TEXT,
            payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, published_generation, album_id, disc_number)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_scans')} (
            scan_id TEXT PRIMARY KEY,
            catalog_instance_id TEXT NOT NULL,
            core_server_id TEXT NOT NULL,
            status TEXT NOT NULL,
            lease_owner TEXT,
            lease_expires_at TIMESTAMPTZ,
            progress JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            last_error TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_scan_entities')} (
            scan_id TEXT NOT NULL,
            catalog_instance_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            metadata_fp TEXT NOT NULL,
            media_fp TEXT,
            artwork_fp TEXT,
            payload JSONB NOT NULL,
            PRIMARY KEY (scan_id, entity_type, entity_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('catalog_changes')} (
            catalog_instance_id TEXT NOT NULL,
            epoch TEXT NOT NULL,
            seq BIGINT NOT NULL,
            generation BIGINT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            old_entity_id TEXT,
            payload JSONB,
            evidence JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, epoch, seq)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('stream_bootstrap_sessions')} (
            token_hash TEXT PRIMARY KEY,
            stream TEXT NOT NULL,
            catalog_instance_id TEXT NOT NULL,
            core_server_id TEXT,
            principal_key TEXT NOT NULL,
            pinned_generation BIGINT NOT NULL,
            snapshot_epoch TEXT NOT NULL,
            snapshot_seq BIGINT NOT NULL,
            totals JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('analysis_state')} (
            catalog_instance_id TEXT PRIMARY KEY,
            analysis_schema_version INTEGER NOT NULL DEFAULT {ANALYSIS_SCHEMA_VERSION},
            projection_generation BIGINT NOT NULL DEFAULT 0,
            analysis_epoch TEXT NOT NULL,
            analysis_head_seq BIGINT NOT NULL DEFAULT 0,
            analysis_floor_seq BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'not_initialized',
            item_count BIGINT NOT NULL DEFAULT 0,
            mapped_track_count BIGINT NOT NULL DEFAULT 0,
            completed_at TIMESTAMPTZ,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('analysis_items')} (
            catalog_instance_id TEXT NOT NULL,
            projection_generation BIGINT NOT NULL,
            analysis_id TEXT NOT NULL,
            scalar_fp TEXT,
            umap_fp TEXT,
            musicnn_fp TEXT,
            clap_fp TEXT,
            scalar_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            musicnn_vector BYTEA,
            clap_vector BYTEA,
            musicnn_dimensions INTEGER,
            clap_dimensions INTEGER,
            model_metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, projection_generation, analysis_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('track_analysis_links')} (
            catalog_instance_id TEXT NOT NULL,
            projection_generation BIGINT NOT NULL,
            provider_track_id TEXT NOT NULL,
            analysis_id TEXT,
            status TEXT NOT NULL,
            match_tier TEXT,
            algorithm TEXT,
            decision_threshold REAL,
            distance REAL,
            evidence_complete BOOLEAN NOT NULL DEFAULT FALSE,
            conflict_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
            review_state TEXT,
            PRIMARY KEY (catalog_instance_id, projection_generation, provider_track_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t('analysis_changes')} (
            catalog_instance_id TEXT NOT NULL,
            epoch TEXT NOT NULL,
            seq BIGINT NOT NULL,
            generation BIGINT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, epoch, seq)
        )
        """,
    ]
    for statement in statements:
        cur.execute(statement)
    cur.close()


def _entity_table_sql(
    table_name,
    id_column,
    fields_sql,
    include_media_fps=False,
    include_artwork_fp=False,
):
    media_sql = ", media_fp TEXT" if include_media_fps else ""
    art_sql = ", artwork_fp TEXT" if include_artwork_fp else ""
    return f"""
        CREATE TABLE IF NOT EXISTS {t(table_name)} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            {id_column} TEXT NOT NULL,
            {fields_sql},
            metadata_fp TEXT NOT NULL{media_sql}{art_sql},
            payload JSONB NOT NULL,
            available BOOLEAN NOT NULL DEFAULT TRUE,
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            deleted_at TIMESTAMPTZ,
            PRIMARY KEY (catalog_instance_id, published_generation, {id_column})
        )
    """


def _new_catalog_instance_id():
    return str(uuid.uuid4())


def ensure_catalog_sources(db, bridge=None):
    """Create source identities for the currently visible core servers."""
    provider_bridge = bridge or ProviderCatalogBridge()
    cur = db.cursor()
    cur.execute(
        f"SELECT catalog_instance_id, current_core_server_id, provider_type, rebind_status "
        f"FROM {t('catalog_sources')}"
    )
    existing = [
        {
            "catalog_instance_id": row[0],
            "current_core_server_id": row[1],
            "provider_type": row[2],
            "rebind_status": row[3],
        }
        for row in cur.fetchall()
    ]
    by_server = {row["current_core_server_id"]: row for row in existing}
    legacy = by_server.get("legacy-default")
    servers = provider_bridge.list_servers()
    results = []

    if legacy and len(servers) == 1 and servers[0]["server_id"] != "legacy-default":
        candidate = servers[0]
        if candidate["provider_type"] == legacy["provider_type"]:
            cur.execute(
                f"UPDATE {t('catalog_sources')} SET rebind_status='rebind_required', "
                "candidate_core_server_id=%s, updated_at=now() WHERE catalog_instance_id=%s",
                (candidate["server_id"], legacy["catalog_instance_id"]),
            )
            results.append({**legacy, "candidate_core_server_id": candidate["server_id"]})
            cur.close()
            return results

    for server in servers:
        source = by_server.get(server["server_id"])
        if source is None:
            source_id = _new_catalog_instance_id()
            cur.execute(
                f"""
                INSERT INTO {t('catalog_sources')}
                    (catalog_instance_id, current_core_server_id, provider_type,
                     server_name, is_default, rebind_status)
                VALUES (%s, %s, %s, %s, %s, 'active')
                """,
                (
                    source_id,
                    server["server_id"],
                    server["provider_type"],
                    server["name"],
                    server["is_default"],
                ),
            )
            cur.execute(
                f"""
                INSERT INTO {t('catalog_state')}
                    (catalog_instance_id, current_core_server_id, provider_type, catalog_epoch)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (catalog_instance_id) DO NOTHING
                """,
                (source_id, server["server_id"], server["provider_type"], str(uuid.uuid4())),
            )
            cur.execute(
                f"""
                INSERT INTO {t('analysis_state')}
                    (catalog_instance_id, analysis_epoch)
                VALUES (%s, %s)
                ON CONFLICT (catalog_instance_id) DO NOTHING
                """,
                (source_id, str(uuid.uuid4())),
            )
            source = {
                "catalog_instance_id": source_id,
                "current_core_server_id": server["server_id"],
                "provider_type": server["provider_type"],
                "rebind_status": "active",
            }
        else:
            cur.execute(
                f"UPDATE {t('catalog_sources')} SET server_name=%s, provider_type=%s, "
                "is_default=%s, updated_at=now() WHERE catalog_instance_id=%s",
                (
                    server["name"],
                    server["provider_type"],
                    server["is_default"],
                    source["catalog_instance_id"],
                ),
            )
        results.append(source)

    cur.close()
    return results


def accept_legacy_rebind(db, catalog_instance_id, core_server_id, evidence):
    """Rebind v2 source ownership only when every continuity proof is true."""
    required = ("provider_type", "provider_instance", "library_scope", "provider_sample")
    if not all(evidence.get(key) is True for key in required):
        raise ValueError("AudioMuse v2-to-v3 catalogue continuity evidence is incomplete")

    cur = db.cursor()
    cur.execute(
        f"SELECT current_core_server_id, rebind_status FROM {t('catalog_sources')} "
        "WHERE catalog_instance_id=%s FOR UPDATE",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        raise KeyError("Unknown catalogue instance")
    if row[0] == core_server_id and row[1] == "active":
        cur.close()
        return False
    if row[0] != "legacy-default" or row[1] != "rebind_required":
        cur.close()
        raise ValueError("Catalogue instance is not awaiting a v2-to-v3 rebind")

    cur.execute(
        f"""
        UPDATE {t('catalog_sources')}
           SET current_core_server_id=%s, continuity_from='legacy-default',
               rebind_status='active', candidate_core_server_id=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (core_server_id, catalog_instance_id),
    )
    cur.execute(
        f"UPDATE {t('catalog_state')} SET current_core_server_id=%s, updated_at=now() "
        "WHERE catalog_instance_id=%s",
        (core_server_id, catalog_instance_id),
    )
    cur.close()
    return True

