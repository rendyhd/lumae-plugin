"""Optional execution tests for the collection browser against real PostgreSQL.

Run with LUMAE_POSTGRES_TEST_DSN set to a test database. The fixture uses an
isolated schema containing the current provider-catalogue projection.
"""

import importlib.util
import os
import pathlib
import sys
import types

import pytest


psycopg2 = pytest.importorskip("psycopg2")
POSTGRES_DSN = os.environ.get("LUMAE_POSTGRES_TEST_DSN")
if not POSTGRES_DSN:
    pytest.skip(
        "set LUMAE_POSTGRES_TEST_DSN to run PostgreSQL integration tests",
        allow_module_level=True,
    )


def _load_collection_library():
    if "plugin.api" not in sys.modules:
        plugin_module = types.ModuleType("plugin")
        plugin_api = types.ModuleType("plugin.api")
        plugin_api.config = types.SimpleNamespace()
        plugin_api.get_db = lambda: None
        plugin_api.logger = types.SimpleNamespace(
            warning=lambda *args, **kwargs: None,
            exception=lambda *args, **kwargs: None,
        )
        plugin_api.table = lambda name: name
        sys.modules["plugin"] = plugin_module
        sys.modules["plugin.api"] = plugin_api

    source = (
        pathlib.Path(__file__).resolve().parents[2]
        / "plugins"
        / "LumaeAnalysis"
        / "collection_library.py"
    )
    spec = importlib.util.spec_from_file_location(
        "lumae_collection_library_postgres_test", source
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def postgres_library():
    connection = psycopg2.connect(POSTGRES_DSN)
    cursor = connection.cursor()
    library = _load_collection_library()
    sources = library.table("catalog_sources")
    state = library.table("catalog_state")
    analysis_state = library.table("analysis_state")
    tracks = library.table("catalog_tracks")
    albums = library.table("catalog_albums")
    links = library.table("track_analysis_links")

    cursor.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
    cursor.execute("DROP SCHEMA IF EXISTS lumae_collection_integration CASCADE")
    cursor.execute("CREATE SCHEMA lumae_collection_integration")
    cursor.execute("SET search_path TO lumae_collection_integration, public")
    cursor.execute(
        f"""
        CREATE TABLE {sources} (
            catalog_instance_id TEXT PRIMARY KEY,
            provider_type TEXT NOT NULL,
            server_name TEXT NOT NULL,
            is_default BOOLEAN NOT NULL,
            rebind_status TEXT NOT NULL
        );
        CREATE TABLE {state} (
            catalog_instance_id TEXT PRIMARY KEY,
            published_generation BIGINT NOT NULL,
            status TEXT NOT NULL
        );
        CREATE TABLE {analysis_state} (
            catalog_instance_id TEXT PRIMARY KEY,
            projection_generation BIGINT NOT NULL
        );
        CREATE TABLE {albums} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            album_id TEXT NOT NULL,
            name TEXT NOT NULL,
            available BOOLEAN NOT NULL,
            PRIMARY KEY (catalog_instance_id, published_generation, album_id)
        );
        CREATE TABLE {tracks} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            track_id TEXT NOT NULL,
            title TEXT NOT NULL,
            artist_display TEXT,
            album_artist_display TEXT,
            album_id TEXT,
            track_number INTEGER,
            disc_number INTEGER,
            duration_ms BIGINT,
            content_kind TEXT,
            release_type TEXT,
            cover_art_id TEXT,
            available BOOLEAN NOT NULL,
            PRIMARY KEY (catalog_instance_id, published_generation, track_id)
        );
        CREATE TABLE {links} (
            catalog_instance_id TEXT NOT NULL,
            projection_generation BIGINT NOT NULL,
            provider_track_id TEXT NOT NULL,
            status TEXT,
            PRIMARY KEY (
                catalog_instance_id, projection_generation, provider_track_id
            )
        )
        """
    )
    cursor.execute(
        f"""
        INSERT INTO {sources}
            (catalog_instance_id, provider_type, server_name, is_default, rebind_status)
        VALUES ('catalog-a', 'navidrome', 'Main Navidrome', TRUE, 'active');
        INSERT INTO {state}
            (catalog_instance_id, published_generation, status)
        VALUES ('catalog-a', 1, 'complete');
        INSERT INTO {analysis_state}
            (catalog_instance_id, projection_generation)
        VALUES ('catalog-a', 1)
        """
    )
    cursor.executemany(
        f"""
        INSERT INTO {albums}
            (catalog_instance_id, published_generation, album_id, name, available)
        VALUES ('catalog-a', 1, %s, %s, TRUE)
        """,
        [
            ("album-rh-rainbows", "In Rainbows"),
            ("album-rh-moon", "A Moon Shaped Pool"),
            ("album-meiko", "The Bright Side"),
            ("album-bey", "Lemonade"),
        ],
    )
    cursor.executemany(
        f"""
        INSERT INTO {tracks}
            (catalog_instance_id, published_generation, track_id, title,
             artist_display, album_artist_display, album_id, available)
        VALUES ('catalog-a', 1, %s, %s, %s, %s, %s, TRUE)
        """,
        [
            ("rh-1", "15 Step", "Radiohead", "Radiohead", "album-rh-rainbows"),
            ("rh-2", "Reckoner", "Radiohead", "Radiohead", "album-rh-rainbows"),
            ("rh-3", "Burn the Witch", "Radiohead", "Radiohead", "album-rh-moon"),
            ("meiko-1", "Reasons to Love You", "Meiko", "Meiko", "album-meiko"),
            ("meiko-2", "Stuck on You", "Meiko", "Meiko", "album-meiko"),
            ("bey-1", "Hold Up", "Beyoncé", "Beyoncé", "album-bey"),
            ("single-1", "Loose Track", "Solo Artist", None, None),
        ],
    )
    connection.commit()
    cursor.close()

    library.get_db = lambda: connection
    try:
        yield library, connection
    finally:
        connection.rollback()
        cursor = connection.cursor()
        cursor.execute("SET search_path TO public")
        cursor.execute("DROP SCHEMA IF EXISTS lumae_collection_integration CASCADE")
        connection.commit()
        cursor.close()
        connection.close()


@pytest.mark.parametrize("scope", ["all", "albums", "tracks", "artists"])
@pytest.mark.parametrize("sort", ["title", "artist", "year"])
@pytest.mark.parametrize("query", ["", "meiko", "beyonce"])
def test_collection_browse_matrix_executes_on_postgresql(
    postgres_library, scope, sort, query
):
    library, _ = postgres_library

    result = library.browse_library(
        scope=scope,
        query=query,
        sort=sort,
        page=1,
        limit=20,
    )

    assert result["scope"] == scope
    assert result["sort"] == sort
    assert result["query"] == query
    assert result["sections"]


def test_collection_browse_counts_search_accents_artist_filter_and_pagination(
    postgres_library,
):
    library, _ = postgres_library

    complete = library.browse_library(scope="all", limit=20)
    assert complete["sections"]["albums"]["total"] == 4
    assert complete["sections"]["tracks"]["total"] == 7
    assert complete["sections"]["artists"]["total"] == 4

    meiko = library.browse_library(scope="all", query="meiko", limit=20)
    assert meiko["sections"]["albums"]["total"] == 1
    assert meiko["sections"]["tracks"]["total"] == 2
    assert meiko["sections"]["artists"]["total"] == 1

    accent_insensitive = library.browse_library(scope="tracks", query="beyonce")
    assert [item["title"] for item in accent_insensitive["sections"]["tracks"]["items"]] == [
        "Hold Up"
    ]

    radiohead_albums = library.browse_library(
        scope="albums", artist="Radiohead", limit=20
    )
    radiohead_tracks = library.browse_library(
        scope="tracks", artist="Radiohead", limit=20
    )
    assert radiohead_albums["sections"]["albums"]["total"] == 2
    assert radiohead_tracks["sections"]["tracks"]["total"] == 3

    first = library.browse_library(scope="albums", page=1, limit=1)
    second = library.browse_library(scope="albums", page=2, limit=1)
    assert first["sections"]["albums"]["total"] == 4
    assert second["sections"]["albums"]["total"] == 4
    assert first["sections"]["albums"]["items"][0]["album_key"] != second[
        "sections"
    ]["albums"]["items"][0]["album_key"]


def test_collection_library_flask_routes_execute_real_queries(postgres_library):
    from flask import Blueprint, Flask

    library, _ = postgres_library
    app = Flask(__name__)
    blueprint = Blueprint("collections_postgres", __name__, url_prefix="/plugin")
    library.register_collection_library_routes(blueprint, lambda view: view)
    app.register_blueprint(blueprint)
    client = app.test_client()

    browse = client.get(
        "/plugin/api/collections/library?scope=all&q=meiko&sort=artist&page=1&limit=36"
    )
    stats = client.get("/plugin/api/collections/library/stats")

    assert browse.status_code == 200
    assert browse.get_json()["sections"]["tracks"]["total"] == 2
    assert stats.status_code == 200
    assert stats.get_json() == {
        "album_count": 4,
        "artist_count": 4,
        "track_count": 7,
    }


def test_collection_search_uses_the_active_catalogue_projection(postgres_library):
    library, connection = postgres_library
    cursor = connection.cursor()
    cursor.execute(
        f"""
        EXPLAIN
        SELECT item_id
          FROM ({library.catalog_track_view_sql()}) score
         WHERE search_u LIKE unaccent(%s)
        """,
        ("%meiko%",),
    )
    plan = "\n".join(row[0] for row in cursor.fetchall())
    cursor.close()

    assert library.table("catalog_tracks") in plan
    assert library.table("catalog_state") in plan
