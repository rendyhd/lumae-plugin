"""P3-5c (LUM-016): stored search text, trigram index, keyset paging, capped totals.

- Results equal the pre-LUM-016 queries (the oracle below, verbatim from
  phase/3-semantics 9a68eef) in content and order, with accent and case
  variants, through the stored text and through the inline fallback.
- ``EXPLAIN`` on a migrated, analysed catalogue: search uses the trigram
  index, a cursor page reads the keyset index scoped by catalogue and
  generation with no Sort and no OFFSET, album pages use the album_id index.
- Without pg_trgm or unaccent, migration and search still work.
- Publication writes the search text with the rows.
"""

import importlib
from datetime import datetime, timezone

import pytest

psycopg2 = pytest.importorskip("psycopg2")

CAT = "catalog-a"
OTHER = "catalog-b"

# (album_id, name, album artist). al-3/al-4 differ only by case and accent,
# in the order both the old and the new sorts give them.
ALBUMS = [
    ("al-1", "Café del Mar", "Various"),
    ("al-2", "CAFE Society", "Ella"),
    ("al-3", "zebra", "beyonce"),
    ("al-4", "Zebra", "Beyoncé"),
    ("al-5", "Émile", ""),
    ("al-6", "", "Nobody"),
]
# (track_id, album_id, title, artist, album artist)
TRACKS = [
    ("t01", "al-1", "Hello", "A Artist", None),
    ("t02", "al-1", "HELLO", "B Artist", "Various"),
    ("t03", "al-2", "Cafe Blues", "Ella", "Ella"),
    ("t04", "al-2", "Night Café", "Ella", ""),
    ("t05", "al-3", "Stripes", "beyonce", "beyonce"),
    ("t06", "al-4", "Déjà Vu", "Beyoncé", "Beyoncé"),
    ("t07", "al-4", "Halo World", "Beyoncé", None),
    ("t08", "al-5", "Zola", "Émile Zola", None),
    ("t09", "al-6", "Orphan Title", "Nobody", "Nobody"),
    ("t10", None, "Loose", "Solo", None),
    ("t11", "al-5", "hello wonder", "émile zola", ""),
    ("t12", "al-missing", "Unknown Album", "Ghost", None),
]
QUERIES = ["", "cafe", "CAFÉ", "café", "beyonce", "BEYONCÉ", "zola émile", "hello wo",
           "halo", "nothing-matches"]


@pytest.fixture
def lib(migrated_db, monkeypatch):
    library = importlib.import_module("plugins.LumaeAnalysis.collection_library")
    monkeypatch.setattr(library, "get_db", lambda: migrated_db)
    return library


def mod(name):
    return importlib.import_module(f"plugins.LumaeAnalysis.{name}")


def t(name):
    return mod("collection_library").table(name)


def _source(cur, catalog, generation=1, default=True):
    cur.execute(
        f"INSERT INTO {t('catalog_sources')} (catalog_instance_id, current_core_server_id, "
        "provider_type, server_name, is_default, rebind_status) "
        "VALUES (%s, %s, 'navidrome', %s, %s, 'active')",
        (catalog, f"server-{catalog}", f"Server {catalog}", default),
    )
    cur.execute(
        f"INSERT INTO {t('catalog_state')} (catalog_instance_id, provider_type, "
        "current_core_server_id, published_generation, catalog_epoch, status) "
        "VALUES (%s, 'navidrome', %s, %s, 'epoch', 'complete')",
        (catalog, f"server-{catalog}", generation),
    )


def publish_small(db, catalog=CAT):
    """The small catalogue through the publication writer."""
    catalog_module = mod("catalog")
    now = datetime.now(timezone.utc)
    with db.cursor() as cur:
        _source(cur, catalog)
        catalog_module._insert_generation_rows(cur, "album", catalog, 1, [
            {"album_id": a, "name": n, "album_artist_display": ar, "metadata_fp": "fp",
             "payload": {}} for a, n, ar in ALBUMS], now)
        folded = catalog_module._insert_generation_rows(cur, "track", catalog, 1, [
            {"track_id": i, "album_id": a, "title": ti, "artist_display": ar,
             "album_artist_display": aa, "metadata_fp": "fp", "payload": {}}
            for i, a, ti, ar, aa in TRACKS], now)
        assert folded is True
        # What the publication's catalog_state update records.
        mod("catalog_search").mark_search_text(cur, catalog, 1, folded)
    db.commit()


# --- oracle: the pre-LUM-016 queries (phase/3-semantics 9a68eef) ----------

def oracle(db, library, scope, query="", artist=None, limit=100, offset=0):
    filters, params = library._library_filters(query, artist, True)
    view = library.catalog_track_view_sql(True)
    sql = {
        "albums": f"""
        SELECT album_id, title, artist, cover_item_id, track_count, year, rating,
               COUNT(*) OVER()::INTEGER AS total_count
          FROM (
            SELECT album_id, MIN(album) AS title,
                   {library.album_artist_sql()} AS artist,
                   MIN(item_id) AS cover_item_id,
                   COUNT(*)::INTEGER AS track_count,
                   MIN(year)::INTEGER AS year,
                   MAX(rating)::INTEGER AS rating
              FROM ({view}) score
             WHERE NULLIF(album, '') IS NOT NULL {filters}
             GROUP BY album_id
          ) albums
         ORDER BY lower(title), lower(artist), album_id
         LIMIT %s OFFSET %s""",
        "tracks": f"""
        SELECT item_id AS track_id, title, author AS artist, album,
               COALESCE(NULLIF(album_artist, ''), author) AS album_artist,
               year, rating, item_id AS cover_item_id,
               COUNT(*) OVER()::INTEGER AS total_count
          FROM ({view}) score
         WHERE NULLIF(title, '') IS NOT NULL {filters}
         ORDER BY lower(title), lower(COALESCE(author, '')), lower(COALESCE(album, ''))
         LIMIT %s OFFSET %s""",
        "artists": f"""
        SELECT artist, cover_item_id, album_count, track_count,
               first_year, latest_year,
               COUNT(*) OVER()::INTEGER AS total_count
          FROM (
            SELECT COALESCE(NULLIF(album_artist, ''), author) AS artist,
                   MIN(item_id) AS cover_item_id,
                   COUNT(DISTINCT album_id)
                     FILTER (WHERE NULLIF(album, '') IS NOT NULL)::INTEGER AS album_count,
                   COUNT(*)::INTEGER AS track_count,
                   MIN(year)::INTEGER AS first_year,
                   MAX(year)::INTEGER AS latest_year
              FROM ({view}) score
             WHERE NULLIF(COALESCE(NULLIF(album_artist, ''), author), '') IS NOT NULL
                   {filters}
             GROUP BY COALESCE(NULLIF(album_artist, ''), author)
          ) artists
         ORDER BY lower(artist)
         LIMIT %s OFFSET %s""",
    }[scope]
    with db.cursor() as cur:
        cur.execute(sql, tuple([CAT] + params + [limit, offset]))
        rows = library._all_dicts(cur)
    total = rows[0]["total_count"] if rows else 0
    for row in rows:
        row.pop("total_count")
        if scope == "albums":
            row.update(kind="album", album_key=library._album_key(row["title"], row["artist"]),
                       provider_album_id=row.pop("album_id"))
        elif scope == "tracks":
            row["kind"] = "track"
        else:
            row.update(kind="artist", title=row["artist"])
    return rows, total


def _new(library, scope, **kwargs):
    section = library.browse_library(scope=scope, limit=kwargs.pop("limit", 100),
                                     **kwargs)["sections"][scope]
    return section["items"], section["total"], section


def _assert_matches_oracle(db, library):
    for query in QUERIES:
        for artist in (None, "Beyoncé"):
            for scope in ("albums", "tracks", "artists"):
                if artist and scope == "artists":
                    continue
                items, total, section = _new(library, scope, query=query, artist=artist)
                expected, expected_total = oracle(db, library, scope, query, artist)
                assert items == expected, (scope, query, artist)
                assert (total, section["total_exact"]) == (expected_total, True)
    db.rollback()


def test_results_equal_the_old_queries_through_the_stored_text(migrated_db, lib):
    publish_small(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT search_text_generation, search_text_folded FROM "
                    f"{t('catalog_state')} WHERE catalog_instance_id=%s", (CAT,))
        assert cur.fetchone() == (1, True)
    ctx = lib._search_state(migrated_db.cursor(), CAT, "café", None)
    assert ctx["stored"] is True and ctx["tokens"] == ["cafe"]
    _assert_matches_oracle(migrated_db, lib)
    # Spot checks that the fixture exercises accents and case.
    assert [i["track_id"] for i in _new(lib, "tracks", query="BEYONCÉ")[0]] == ["t06", "t07", "t05"]
    assert [i["provider_album_id"] for i in _new(lib, "albums", query="cafe")[0]] == ["al-2", "al-1"]


def test_results_equal_the_old_queries_through_the_inline_fallback(migrated_db, lib):
    publish_small(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {t('catalog_state')} SET search_text_generation=NULL")
        cur.execute(f"UPDATE {t('catalog_tracks')} SET search_text=NULL")
    migrated_db.commit()
    assert lib._search_state(migrated_db.cursor(), CAT, "x", None)["stored"] is False
    _assert_matches_oracle(migrated_db, lib)


def test_cursor_pages_and_legacy_pages_walk_the_old_order(migrated_db, lib):
    publish_small(migrated_db)
    for scope in ("albums", "tracks", "artists"):
        expected, _ = oracle(migrated_db, lib, scope)
        walked, cursor, pages = [], None, 0
        while True:
            items, total, section = _new(lib, scope, limit=3, cursor=cursor)
            walked += items
            pages += 1
            assert pages <= len(expected), "the cursor does not advance"
            assert (total, section["total_exact"]) == (len(expected), True)
            cursor = section["next_cursor"]
            if cursor is None:
                break
        assert walked == expected and pages == -(-len(expected) // 3), scope
        legacy = _new(lib, scope, limit=3, page=2)[0]
        assert legacy == expected[3:6], scope
    with pytest.raises(lib.CatalogScopeError) as bad:
        lib.browse_library(scope="tracks", cursor="not-a-cursor!")
    assert (bad.value.error, bad.value.status) == ("invalid_cursor", 400)
    migrated_db.rollback()


def test_totals_are_capped_or_published_counts(migrated_db, lib, monkeypatch):
    publish_small(migrated_db)
    monkeypatch.setattr(lib, "TOTAL_CAP", 4)
    with migrated_db.cursor() as cur:
        cur.execute(f"UPDATE {t('catalog_state')} SET entity_counts='{{\"track\": 12}}'")
    items, total, section = _new(lib, "tracks", limit=2)
    assert (len(items), total, section["total_exact"]) == (2, 12, True)
    items, total, section = _new(lib, "tracks", query="e e", limit=2)
    assert (total, section["total_exact"]) == (4, False)
    items, total, section = _new(lib, "albums", limit=2)
    assert (total, section["total_exact"]) == (4, False)
    # "Émile Zola" and "émile zola" are two artists (grouped as written).
    items, total, section = _new(lib, "artists", query="zola", limit=1)
    assert (len(items), total, section["total_exact"]) == (1, 2, True)
    migrated_db.rollback()


# --- plans on a migrated, analysed catalogue ------------------------------

class Recording:
    """A connection whose cursors record every statement."""

    def __init__(self, db):
        self.db, self.statements = db, []

    def cursor(self):
        cur, statements = self.db.cursor(), self.statements

        class Cursor:
            def execute(self, sql, params=None):
                statements.append((sql, params))
                return cur.execute(sql, params)

            def __getattr__(self, name):
                return getattr(cur, name)

        return Cursor()


def publish_large(db):
    """30k tracks in 2k albums, plus a second catalogue and an old generation."""
    catalog_search = mod("catalog_search")
    with db.cursor() as cur:
        _source(cur, CAT, generation=2)
        _source(cur, OTHER, default=False)
        for catalog, generation, tracks in ((CAT, 2, 30000), (CAT, 1, 10000), (OTHER, 1, 10000)):
            cur.execute(
                f"INSERT INTO {t('catalog_albums')} (catalog_instance_id, published_generation, "
                "album_id, name, album_artist_display, metadata_fp, payload, first_seen_at, "
                "last_seen_at) SELECT %s, %s, 'al-'||lpad(g::text, 5, '0'), "
                "'Album '||md5(g::text), 'Artist '||(g %% 500), 'fp', '{}', now(), now() "
                "FROM generate_series(1, 2000) g",
                (catalog, generation),
            )
            cur.execute(
                f"INSERT INTO {t('catalog_tracks')} (catalog_instance_id, published_generation, "
                "track_id, album_id, title, artist_display, payload, available, metadata_fp, "
                "first_seen_at, last_seen_at) SELECT %s, %s, 'tr-'||lpad(g::text, 6, '0'), "
                "'al-'||lpad((g %% 2000 + 1)::text, 5, '0'), 'Song '||md5(g::text), "
                "'Artist '||(g %% 500), '{}', TRUE, 'fp', now(), now() "
                "FROM generate_series(1, %s) g",
                (catalog, generation, tracks),
            )
            catalog_search.refresh_search_text(
                cur, catalog, generation, catalog_search.fold_available(cur))
        cur.execute(f"UPDATE {t('catalog_state')} SET search_text_generation=published_generation")
    db.commit()
    db.autocommit = True
    with db.cursor() as cur:
        cur.execute(f"ANALYZE {t('catalog_tracks')}")
        cur.execute(f"ANALYZE {t('catalog_albums')}")
    db.autocommit = False


def _plan(db, sql, params):
    with db.cursor() as cur:
        cur.execute("SHOW enable_seqscan")
        assert cur.fetchone()[0] == "on"
        cur.execute("EXPLAIN " + sql, params)
        return "\n".join(row[0] for row in cur.fetchall())


def _page_sql(recording, needle):
    return next((sql, params) for sql, params in recording.statements
                if needle in sql and "LIMIT" in sql and "count(*)" not in sql)


def test_plans_use_the_trigram_keyset_and_album_indexes(migrated_db, lib, monkeypatch):
    publish_large(migrated_db)
    recording = Recording(migrated_db)
    monkeypatch.setattr(lib, "get_db", lambda: recording)
    with migrated_db.cursor() as cur:
        cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=current_schema()"
                    " AND indexname LIKE %s", ("%idx_catalog_%",))
        definitions = dict(cur.fetchall())
    for name in ("idx_catalog_tracks_title_keyset", "idx_catalog_tracks_artist_keyset",
                 "idx_catalog_albums_name_keyset", "idx_catalog_tracks_album"):
        definition = definitions[t(name)]
        assert "(catalog_instance_id, published_generation, " in definition, definition
    assert "gin_trgm_ops" in definitions[t("idx_catalog_tracks_search_trgm")]

    # Search: the stored text through the trigram index.
    result = lib.browse_library(scope="tracks", query="e5d0", catalog_instance_id=CAT)
    assert result["sections"]["tracks"]["items"]
    plan = _plan(migrated_db, *_page_sql(recording, "lower(t.title) AS sort_key"))
    assert f"Bitmap Index Scan on {t('idx_catalog_tracks_search_trgm')}" in plan, plan

    # A deep page by cursor: the keyset index, scoped, no Sort, no OFFSET.
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT lower(title), track_id FROM {t('catalog_tracks')} "
                    "WHERE catalog_instance_id=%s AND published_generation=2 "
                    "ORDER BY 1, 2 OFFSET 25000 LIMIT 1", (CAT,))
        cursor = lib.encode_cursor(*cur.fetchone())
    recording.statements.clear()
    page = lib.browse_library(scope="tracks", cursor=cursor, catalog_instance_id=CAT)
    assert len(page["sections"]["tracks"]["items"]) == 36
    sql, params = _page_sql(recording, "lower(t.title) AS sort_key")
    assert "OFFSET" not in sql
    plan = _plan(migrated_db, sql, params)
    assert f"Index Scan using {t('idx_catalog_tracks_title_keyset')}" in plan, plan
    lines = plan.splitlines()
    at = next(i for i, line in enumerate(lines) if t("idx_catalog_tracks_title_keyset") in line)
    keyset_cond = lines[at + 1]
    assert "Index Cond" in keyset_cond, plan
    assert "published_generation = " in keyset_cond and "catalog_instance_id = " in keyset_cond
    assert "Sort" not in plan, plan

    # Albums: name keyset, and the album_id index for their tracks.
    recording.statements.clear()
    lib.browse_library(scope="albums", cursor=lib.encode_cursor("album 8", "al-01000"),
                       catalog_instance_id=CAT)
    plan = _plan(migrated_db, *_page_sql(recording, "lower(al.name) AS sort_key"))
    assert f"{t('idx_catalog_albums_name_keyset')}" in plan, plan
    assert f"{t('idx_catalog_tracks_album')}" in plan, plan
    assert "Sort" not in plan, plan
    migrated_db.rollback()


# --- degraded installs -----------------------------------------------------

def test_without_pg_trgm_migration_and_search_still_work(
        migrated_db, lib, monkeypatch, run_plugin_migration):
    publish_small(migrated_db)
    expected = _new(lib, "tracks", query="café")[0]
    migrations = mod("migrations")
    real = migrations.ensure_extension
    monkeypatch.setattr(migrations, "ensure_extension",
                        lambda cur, name: False if name == "pg_trgm" else real(cur, name))
    with migrated_db.cursor() as cur:
        cur.execute("DROP EXTENSION pg_trgm CASCADE")
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (t("idx_catalog_tracks_search_trgm"),))
        assert cur.fetchone()[0] is None
    assert _new(lib, "tracks", query="café")[0] == expected
    _assert_matches_oracle(migrated_db, lib)


def test_without_unaccent_search_matches_case_but_not_accents(migrated_db, lib):
    publish_small(migrated_db)
    cur = migrated_db.cursor()
    try:
        cur.execute("DROP EXTENSION unaccent")
        # Folded with unaccent at publication, so not usable now: inline text.
        assert lib._search_state(cur, CAT, "x", None)["stored"] is False
        assert [i["track_id"] for i in _new(lib, "tracks", query="BEYONCÉ")[0]] == ["t06", "t07"]
        assert _new(lib, "tracks", query="beyonce")[0][0]["track_id"] == "t05"
    finally:
        migrated_db.rollback()
        cur.close()


def test_publication_writes_search_text_and_a_stale_generation_is_rewritten(
        migrated_db, lib, run_plugin_migration):
    publish_small(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT track_id, search_text FROM {t('catalog_tracks')} "
                    "WHERE track_id IN ('t04', 't10', 't12') ORDER BY 1")
        assert cur.fetchall() == [("t04", "night cafe ella  cafe society"),
                                  ("t10", "loose solo"), ("t12", "unknown album ghost")]
        cur.execute(f"UPDATE {t('catalog_tracks')} SET search_text=NULL WHERE track_id='t04'")
        cur.execute(f"UPDATE {t('catalog_state')} SET search_text_folded=FALSE")
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT search_text FROM {t('catalog_tracks')} WHERE track_id='t04'")
        assert cur.fetchone()[0] == "night cafe ella  cafe society"
        cur.execute(f"SELECT search_text_generation, search_text_folded FROM {t('catalog_state')}")
        assert cur.fetchone() == (1, True)
    migrated_db.commit()
