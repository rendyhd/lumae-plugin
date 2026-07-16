"""Optional execution tests for the collection browser against real PostgreSQL.

Run with LUMAE_POSTGRES_TEST_DSN set to a test database. The fixture uses an
isolated schema containing the score table used by AudioMuse's library browser.
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
    cursor.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
    cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    cursor.execute("DROP SCHEMA IF EXISTS lumae_collection_integration CASCADE")
    cursor.execute("CREATE SCHEMA lumae_collection_integration")
    cursor.execute("SET search_path TO lumae_collection_integration, public")
    cursor.execute(
        """
        CREATE TABLE score (
            item_id TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            album TEXT,
            album_artist TEXT,
            year INTEGER,
            rating INTEGER,
            search_u TEXT
        )
        """
    )
    rows = [
        ("rh-1", "15 Step", "Radiohead", "In Rainbows", "Radiohead", 2007, 5),
        ("rh-2", "Reckoner", "Radiohead", "In Rainbows", "Radiohead", 2007, 5),
        (
            "rh-3",
            "Burn the Witch",
            "Radiohead",
            "A Moon Shaped Pool",
            "Radiohead",
            2016,
            4,
        ),
        ("meiko-1", "Reasons to Love You", "Meiko", "The Bright Side", "Meiko", 2012, 4),
        ("meiko-2", "Stuck on You", "Meiko", "The Bright Side", "Meiko", 2012, 3),
        ("bey-1", "Hold Up", "Beyoncé", "Lemonade", "Beyoncé", 2016, 5),
        ("single-1", "Loose Track", "Solo Artist", None, None, 2020, None),
    ]
    cursor.executemany(
        """
        INSERT INTO score
            (item_id, title, author, album, album_artist, year, rating, search_u)
        VALUES (%s, %s, %s, %s, %s, %s, %s,
                lower(unaccent(concat_ws(' ', %s, %s, %s))))
        """,
        [row + (row[1], row[2], row[3]) for row in rows],
    )
    cursor.execute(
        "CREATE INDEX score_search_u_trgm ON score USING gin (search_u gin_trgm_ops)"
    )
    connection.commit()
    cursor.close()

    library = _load_collection_library()
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


def test_collection_search_condition_can_use_audiomuse_trigram_index(
    postgres_library,
):
    _, connection = postgres_library
    cursor = connection.cursor()
    cursor.execute("SET LOCAL enable_seqscan = off")
    cursor.execute(
        "EXPLAIN SELECT item_id FROM score WHERE search_u LIKE unaccent(%s)",
        ("%meiko%",),
    )
    plan = "\n".join(row[0] for row in cursor.fetchall())
    cursor.close()

    assert "score_search_u_trgm" in plan
