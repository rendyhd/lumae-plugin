"""K10 workbench catalogue scoping (P3-5a: LUM-013) and LUM-015.

Browse, stats, album, stream and art take ``catalog_instance_id``. Without
it, a single active source is used (old clients are unchanged); with several,
the answer is 400 ``catalog_instance_required``. Collection items carry a
nullable ``catalog_instance_id``.

``workbench_scope_v1_golden.json`` was recorded from phase/3-semantics
6155e27, before K10, by this module's ``_library_transcript`` on a
single-source install.

P3-5b: LUM-014 albums are catalogue albums, ``(catalogue, album_id)``, with
``provider_album_id`` always set; same-name editions stay apart (the
inverted ``probes/collections/explain_editions.py``). Art and stream of a
mixed-catalogue collection follow each item's catalogue.
"""

import importlib
import json
import pathlib
import re

import pytest

from test_collection_feed_epoch_postgres import (  # noqa: F401 (fixture)
    _normalized,
    collections_api,
)


GOLDEN = pathlib.Path(__file__).with_name("workbench_scope_v1_golden.json")
LIBRARY = "/api/collections/library"


def _library_module():
    return importlib.import_module("plugins.LumaeAnalysis.collection_library")


@pytest.fixture
def workbench(collections_api, monkeypatch):
    """``collections_api`` with the library routes on the same per-request connection."""
    library = _library_module()
    manager = collections_api.manager
    monkeypatch.setattr(library, "get_db", lambda: manager.get_db())
    collections_api.library = library
    return collections_api


def _seed(api, catalog_id, provider, albums, *, default=False, status="active"):
    """One published catalogue: ``albums`` is {album_id: (name, artist, [titles])}."""
    t = _library_module().table
    db = api.connect()
    try:
        with db.cursor() as cur:
            cur.execute(
                f"INSERT INTO {t('catalog_sources')} (catalog_instance_id, "
                "current_core_server_id, provider_type, server_name, is_default, "
                "rebind_status) VALUES (%s, %s, %s, %s, %s, %s)",
                (catalog_id, f"server-{catalog_id}", provider, f"Server {catalog_id}",
                 default, status),
            )
            cur.execute(
                f"INSERT INTO {t('catalog_state')} (catalog_instance_id, provider_type, "
                "current_core_server_id, published_generation, catalog_epoch, status) "
                "VALUES (%s, %s, %s, 1, 'epoch', 'complete')",
                (catalog_id, provider, f"server-{catalog_id}"),
            )
            for album_id, (name, artist, titles) in albums.items():
                cur.execute(
                    f"INSERT INTO {t('catalog_albums')} (catalog_instance_id, "
                    "published_generation, album_id, name, album_artist_display, "
                    "metadata_fp, payload, first_seen_at, last_seen_at) "
                    "VALUES (%s, 1, %s, %s, %s, 'fp', '{}', now(), now())",
                    (catalog_id, album_id, name, artist),
                )
                for number, title in enumerate(titles, start=1):
                    cur.execute(
                        f"INSERT INTO {t('catalog_tracks')} (catalog_instance_id, "
                        "published_generation, track_id, album_id, title, artist_display, "
                        "album_artist_display, track_number, disc_number, duration_ms, "
                        "payload, available, analysis_eligible, metadata_fp, "
                        "first_seen_at, last_seen_at) VALUES (%s, 1, %s, %s, %s, %s, %s, "
                        "%s, 1, 200000, '{}', TRUE, TRUE, 'fp', now(), now())",
                        (catalog_id, f"{album_id}-t{number}", album_id, title, artist,
                         artist, number),
                    )
        db.commit()
    finally:
        db.close()


ALBUMS_A = {
    "al-rain": ("In Rainbows", "Radiohead", ["Reckoner", "15 Step", "Nude"]),
    "al-moon": ("A Moon Shaped Pool", "Radiohead", ["Burn the Witch"]),
    "al-bey": ("Lemonade", "Beyoncé", ["Hold Up", "Sorry"]),
}
ALBUMS_B = {
    "al-kid": ("Kid A", "Radiohead", ["Idioteque", "Optimistic"]),
}


def _library_transcript(call, suffix=""):
    """An old client's workbench reads on a single-source install.

    Returns [(label, status, raw body bytes, {})] like the feed transcripts.
    """
    out = []
    requests = [
        ("stats", f"{LIBRARY}/stats"),
        ("albums by title", f"{LIBRARY}?scope=albums&sort=title"),
        ("albums by artist", f"{LIBRARY}?scope=albums&sort=artist&limit=2&page=2"),
        ("albums by year", f"{LIBRARY}?scope=albums&sort=year"),
        ("tracks by artist", f"{LIBRARY}?scope=tracks&sort=artist"),
        ("artists", f"{LIBRARY}?scope=artists"),
        ("search all", f"{LIBRARY}?scope=all&q=radio"),
        ("artist filter", f"{LIBRARY}?scope=albums&artist=Radiohead"),
        ("album by key", f"{LIBRARY}/album?title=In%20Rainbows&artist=Radiohead"),
        ("album by id", f"{LIBRARY}/album?title=Lemonade&artist=Beyonc%C3%A9"
                        "&provider_album_id=al-bey"),
        ("album missing", f"{LIBRARY}/album?title=Nope&artist=Nobody"),
        ("album bad", f"{LIBRARY}/album?title=Nope"),
    ]
    for label, path in requests:
        if suffix:
            path += ("&" if "?" in path else "?") + suffix
        response = call("GET", path)
        out.append((label, response.status_code, response.get_data(), {}))
    return out


def record_golden(api):
    GOLDEN.write_text(
        json.dumps(_normalized(_library_transcript(api.call)), ensure_ascii=False, indent=1)
        + "\n",
        encoding="utf-8",
    )


K10_ECHO = re.compile(rb'"catalog_instance_id":"catalog-a",')
PROVIDER_ALBUM_ID = re.compile(rb'"provider_album_id":"[^"]+"')
READS = (
    f"{LIBRARY}?scope=albums",
    f"{LIBRARY}/stats",
    f"{LIBRARY}/album?title=Kid%20A&artist=Radiohead",
    f"{LIBRARY}/stream/al-kid-t1",
    f"{LIBRARY}/art/al-kid-t1",
    "/api/collections/search?q=radio&kind=album",
)


def _two_sources(api):
    _seed(api, "catalog-a", "navidrome", ALBUMS_A, default=True)
    _seed(api, "catalog-b", "jellyfin", ALBUMS_B)


def _with(path, catalog):
    return path + ("&" if "?" in path else "?") + f"catalog_instance_id={catalog}"


def _album_titles(api, catalog):
    body = api.call("GET", _with(f"{LIBRARY}?scope=albums", catalog)).get_json()
    assert body["catalog_instance_id"] == catalog
    return [item["title"] for item in body["sections"]["albums"]["items"]]


def test_single_source_reads_match_the_pre_k10_golden(workbench):
    """Without the parameter, one active source is used: every body is the
    pre-K10 one plus the additive ``catalog_instance_id`` echo."""
    _seed(workbench, "catalog-a", "navidrome", ALBUMS_A, default=True)
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    for suffix in ("", "catalog_instance_id=catalog-a"):
        stripped = []
        for label, status, body, headers in _library_transcript(workbench.call, suffix):
            if label.startswith("album") and status == 200:
                # The catalogue its tracks came from, else the one requested.
                echo = b'"catalog_instance_id":null' if label == "album missing" and not suffix \
                    else b'"catalog_instance_id":"catalog-a"'
                assert echo in body, label
                body = body.replace(b'"catalog_instance_id":null,', b"")
            if b'"sections"' in body:
                # LUM-014: a browsed album carries its catalogue album_id as
                # provider_album_id (1.2.5 sent null). Only that value is undone.
                albums = [row for section in json.loads(body)["sections"].values()
                          for row in section["items"] if row["kind"] == "album"]
                assert all(row["provider_album_id"] == row["cover_item_id"].rsplit("-t", 1)[0]
                           for row in albums), label
                body = PROVIDER_ALBUM_ID.sub(b'"provider_album_id":null', body)
            stripped.append((label, status, K10_ECHO.sub(b"", body), headers))
        assert _normalized(stripped) == golden, suffix


def test_two_active_sources_require_an_explicit_catalogue(workbench, monkeypatch):
    _two_sources(workbench)
    library = workbench.library
    monkeypatch.setattr(library, "_proxy_stream", lambda target: (None, ("stopped", 502)))
    monkeypatch.setattr(library, "_proxy_art", lambda target: None)
    for path in READS:
        response = workbench.call("GET", path)
        assert response.status_code == 400, path
        body = response.get_json()
        assert body["error"] == "catalog_instance_required"
        assert body["catalogs"] == [
            {"catalog_instance_id": "catalog-a", "provider_type": "navidrome",
             "server_name": "Server catalog-a", "is_default": True},
            {"catalog_instance_id": "catalog-b", "provider_type": "jellyfin",
             "server_name": "Server catalog-b", "is_default": False},
        ]
        assert workbench.call("GET", _with(path, "catalog-z")).status_code == 404, path
    assert _album_titles(workbench, "catalog-a") == [
        "A Moon Shaped Pool", "In Rainbows", "Lemonade"]
    assert _album_titles(workbench, "catalog-b") == ["Kid A"]
    stats = workbench.call("GET", _with(f"{LIBRARY}/stats", "catalog-b")).get_json()
    assert stats == {"album_count": 1, "artist_count": 1, "track_count": 2}
    kid_a = f"{LIBRARY}/album?title=Kid%20A&artist=Radiohead"
    assert workbench.call("GET", _with(kid_a, "catalog-a")).get_json()["tracks"] == []
    detail = workbench.call("GET", _with(kid_a, "catalog-b")).get_json()
    assert detail["catalog_instance_id"] == "catalog-b"
    assert [t["track_id"] for t in detail["tracks"]] == ["al-kid-t1", "al-kid-t2"]
    assert {t["catalog_instance_id"] for t in detail["tracks"]} == {"catalog-b"}
    assert detail["provider_type"] == "jellyfin"
    search = workbench.call(
        "GET", _with("/api/collections/search?q=radio&kind=album", "catalog-b")).get_json()
    assert [row["title"] for row in search["results"]] == ["Kid A"]
    # A source that is no longer active leaves one: the default applies again.
    db = workbench.connect()
    with db.cursor() as cur:
        cur.execute(f"UPDATE {library.table('catalog_sources')} SET rebind_status="
                    "'rebind_required' WHERE catalog_instance_id='catalog-a'")
    db.commit()
    body = workbench.call("GET", f"{LIBRARY}?scope=albums").get_json()
    assert body["catalog_instance_id"] == "catalog-b"
    assert workbench.call("GET", _with(f"{LIBRARY}/stats", "catalog-a")).status_code == 404


def test_stream_and_art_take_the_provider_from_the_source_row(workbench, monkeypatch):
    _two_sources(workbench)
    library = workbench.library
    for name, value in (("MEDIASERVER_TYPE", "emby"), ("EMBY_URL", "http://emby"),
                        ("JELLYFIN_URL", "http://jellyfin"), ("HEADERS", {})):
        monkeypatch.setattr(library.config, name, value, raising=False)
    targets = []
    monkeypatch.setattr(library, "_proxy_stream",
                        lambda target: targets.append(target) or (None, ("stopped", 502)))
    monkeypatch.setattr(library, "_proxy_art", lambda target: targets.append(target))
    stream = workbench.call("GET", _with(f"{LIBRARY}/stream/al-kid-t1", "catalog-b"))
    art = workbench.call("GET", _with(f"{LIBRARY}/art/al-kid-t1?size=100", "catalog-b"))
    assert (stream.status_code, art.status_code) == (502, 404)
    assert [url for url, _, _ in targets] == [
        "http://jellyfin/Items/al-kid-t1/Download",
        "http://jellyfin/Items/al-kid-t1/Images/Primary",
    ]


def _items(db, manager):
    with db.cursor() as cur:
        cur.execute(
            f"SELECT id, catalog_instance_id FROM {manager.collection_items_table()} ORDER BY id"
        )
        return dict(cur.fetchall())


def _seed_unscoped_items(db, manager):
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {manager.collections_table()} (principal, id, name) "
                    "VALUES ('user:alice', 'c1', 'One')")
        for item_id, kind, track, album_id, key in (
            ("i-track", "track", "t1", None, None),
            ("i-album-id", "album", None, "al-1", None),
            ("i-album-key", "album", None, None, "artist::album"),
        ):
            cur.execute(
                f"INSERT INTO {manager.collection_items_table()} (principal, id, "
                "collection_id, kind, track_id, provider_album_id, album_key) "
                "VALUES ('user:alice', %s, 'c1', %s, %s, %s, %s)",
                (item_id, kind, track, album_id, key),
            )
    db.commit()


def _add_source(db, catalog_id, status="active"):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {_library_module().table('catalog_sources')} (catalog_instance_id, "
            "current_core_server_id, provider_type, server_name, is_default, rebind_status) "
            "VALUES (%s, %s, 'navidrome', %s, FALSE, %s)",
            (catalog_id, f"server-{catalog_id}", catalog_id, status),
        )
    db.commit()


def _manager():
    return importlib.import_module("plugins.LumaeAnalysis.collection_manager")


def test_backfill_scopes_items_only_when_one_catalogue_has_existed(
    migrated_db, run_plugin_migration
):
    manager = _manager()
    _seed_unscoped_items(migrated_db, manager)
    run_plugin_migration(migrated_db)
    assert set(_items(migrated_db, manager).values()) == {None}  # no catalogue yet
    _add_source(migrated_db, "catalog-a")
    # A twin scoped after the catalogue appeared keeps its unscoped copy NULL.
    with migrated_db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {manager.collection_items_table()} (principal, id, collection_id, "
            "kind, track_id, catalog_instance_id) "
            "VALUES ('user:alice', 'i-twin', 'c1', 'track', 't1', 'catalog-a')"
        )
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    assert _items(migrated_db, manager) == {
        "i-album-id": "catalog-a", "i-album-key": "catalog-a",
        "i-track": None, "i-twin": "catalog-a",
    }


def test_backfill_leaves_items_unscoped_after_a_second_catalogue(
    migrated_db, run_plugin_migration
):
    manager = _manager()
    _seed_unscoped_items(migrated_db, manager)
    _add_source(migrated_db, "catalog-a")
    # Retired, but it existed: an unscoped item may have come from either.
    _add_source(migrated_db, "catalog-old", status="rebind_required")
    run_plugin_migration(migrated_db)
    assert set(_items(migrated_db, manager).values()) == {None}


def test_membership_is_unique_per_catalogue(migrated_db):
    psycopg2 = pytest.importorskip("psycopg2")
    manager = _manager()
    items = manager.collection_items_table()
    with migrated_db.cursor() as cur:
        cur.execute(f"INSERT INTO {manager.collections_table()} (principal, id, name) "
                    "VALUES ('p', 'c1', 'One')")

        def insert(item_id, catalog, track="t1", album=None, key=None):
            cur.execute("SAVEPOINT item")
            try:
                cur.execute(
                    f"INSERT INTO {items} (principal, id, collection_id, kind, track_id, "
                    "provider_album_id, album_key, catalog_instance_id) "
                    "VALUES ('p', %s, 'c1', %s, %s, %s, %s, %s)",
                    (item_id, "track" if track else "album", track, album, key, catalog),
                )
            except psycopg2.errors.UniqueViolation:
                cur.execute("ROLLBACK TO SAVEPOINT item")
                return False
            cur.execute("RELEASE SAVEPOINT item")
            return True

        assert insert("a", "catalog-a") and insert("b", "catalog-b") and insert("n", None)
        assert not insert("a2", "catalog-a")
        assert not insert("n2", None)  # unscoped rows still conflict, as in 1.2.5
        assert insert("al-a", "catalog-a", None, "al-1")
        assert insert("al-b", "catalog-b", None, "al-1")
        assert not insert("al-a2", "catalog-a", None, "al-1")
        assert insert("k-a", "catalog-a", None, None, "x::y")
        assert insert("k-n", None, None, None, "x::y")
        assert not insert("k-n2", None, None, None, "x::y")
    migrated_db.rollback()


def test_upgrade_replaces_the_unscoped_membership_indexes(migrated_db, run_plugin_migration):
    items = _manager().collection_items_table()
    with migrated_db.cursor() as cur:
        cur.execute(f"ALTER TABLE {items} DROP COLUMN catalog_instance_id")
        cur.execute(f"CREATE UNIQUE INDEX lumae_collection_track_unique_idx ON {items} "
                    "(principal, collection_id, track_id) WHERE kind = 'track'")
    migrated_db.commit()
    run_plugin_migration(migrated_db)
    with migrated_db.cursor() as cur:
        cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() "
                    "AND indexname LIKE 'lumae_collection_%%unique_idx' ORDER BY 1")
        assert [row[0] for row in cur.fetchall()] == [
            "lumae_collection_album_key_scoped_unique_idx",
            "lumae_collection_album_provider_scoped_unique_idx",
            "lumae_collection_track_scoped_unique_idx",
        ]
    migrated_db.rollback()


def _feed_items(api):
    changes = api.call("GET", "/api/collections/changes?cursor=0&limit=200").get_json()
    scopes = {}
    for change in changes["changes"]:
        if change["entity_kind"] == "item":
            scopes[change["entity_id"]] = change["payload"].get("catalog_instance_id", "absent")
    return scopes


def _scopes(items):
    return {item["id"]: item["catalog_instance_id"] for item in items}


def test_items_carry_their_catalogue_in_routes_feed_and_snapshot(workbench):
    _two_sources(workbench)
    call = workbench.call
    assert call("POST", "/api/collections", {"id": "c1", "name": "One"}).status_code == 201
    batch = call("POST", "/api/collections/c1/items/batch", {"items": [
        {"id": "in-a", "kind": "track", "track_id": "t1", "catalog_instance_id": "catalog-a"},
        {"id": "in-b", "kind": "track", "track_id": "t1", "catalog_instance_id": "catalog-b"},
        {"id": "unscoped", "kind": "track", "track_id": "t1"},
    ]})
    assert batch.status_code == 200, batch.get_json()
    scopes = {"in-a": "catalog-a", "in-b": "catalog-b", "unscoped": None}
    assert _scopes(batch.get_json()["items"]) == scopes
    assert _scopes(call("GET", "/api/collections/c1").get_json()["items"]) == scopes
    assert _feed_items(workbench) == scopes
    assert _scopes(call("GET", "/api/collections/snapshot").get_json()["items"]) == scopes
    # K9: the same track again in one catalogue is a membership conflict.
    again = call("POST", "/api/collections/c1/items/batch", {"items": [
        {"id": "again", "kind": "track", "track_id": "t1", "catalog_instance_id": "catalog-b"},
    ]}, headers={"X-Lumae-Collections-Contract": "2"})
    assert again.status_code == 409
    assert again.get_json()["existing_item_id"] == "in-b"
    # An update without the field keeps the item's catalogue.
    kept = call("PUT", "/api/collections/c1/items/in-a",
                {"kind": "track", "track_id": "t1", "title": "Renamed"})
    assert kept.status_code == 200, kept.get_json()
    assert _feed_items(workbench)["in-a"] == "catalog-a"


def test_single_catalogue_install_scopes_unscoped_writes(workbench):
    """An old client on one catalogue gets that catalogue, so its re-adds
    still meet the backfilled item (1.2.5 remap)."""
    _seed(workbench, "catalog-a", "navidrome", ALBUMS_A, default=True)
    call = workbench.call
    call("POST", "/api/collections", {"id": "c1", "name": "One"})
    first = call("PUT", "/api/collections/c1/items/i1", {"kind": "track", "track_id": "t1"})
    assert first.status_code == 200, first.get_json()
    assert _feed_items(workbench) == {"i1": "catalog-a"}
    second = call("PUT", "/api/collections/c1/items/i2", {"kind": "track", "track_id": "t1"})
    assert second.status_code == 200
    assert [i["id"] for i in call("GET", "/api/collections/c1").get_json()["items"]] == ["i1"]


def test_workbench_no_longer_offers_the_newest_year_sort():
    ui = importlib.import_module("plugins.LumaeAnalysis.collection_ui")
    body = ui.render_collection_workbench("Label", "Detail")
    assert 'value="year"' not in body and "newest year" not in body
    assert "Sort: title" in body and "Sort: artist" in body
    assert "withCatalog(new URLSearchParams({title:item.title" in body


EDITIONS = {
    "al-ed-2": ("Blue", "Joni Mitchell", ["All I Want (Demo)", "River (Demo)", "A Case of You"]),
    "al-ed-1": ("Blue", "Joni Mitchell", ["All I Want", "River"]),
    "al-court": ("Court and Spark", "Joni Mitchell", ["Help Me"]),
}


def _album_rows(items):
    return [(row["title"], row["provider_album_id"], row["track_count"]) for row in items]


def test_same_name_editions_are_separate_catalogue_albums(workbench):
    """LUM-014: the editions probe inverted. Albums are (catalogue, album_id)."""
    _seed(workbench, "catalog-a", "navidrome", EDITIONS, default=True)
    # catalog-b reuses an album_id of catalog-a for another album.
    _seed(workbench, "catalog-b", "jellyfin", {"al-ed-1": ("Hejira", "Joni Mitchell", ["Coyote"])})
    call = workbench.call
    blue = [("Blue", "al-ed-1", 2), ("Blue", "al-ed-2", 3)]
    body = call("GET", _with(f"{LIBRARY}?scope=albums", "catalog-a")).get_json()
    albums = body["sections"]["albums"]["items"]
    assert _album_rows(albums) == blue + [("Court and Spark", "al-court", 1)]
    assert [row["album_key"] for row in albums[:2]] == ["joni mitchell::blue"] * 2
    found = call("GET", _with(f"{LIBRARY}?scope=all&q=blue", "catalog-a")).get_json()
    assert _album_rows(found["sections"]["albums"]["items"]) == blue
    search = call("GET", _with("/api/collections/search?q=blue&kind=album", "catalog-a"))
    assert [(r["title"], r["provider_album_id"], r["track_count"])
            for r in search.get_json()["results"]] == blue
    stats = call("GET", _with(f"{LIBRARY}/stats", "catalog-a")).get_json()
    assert stats == {"album_count": 3, "artist_count": 1, "track_count": 6}
    artists = call("GET", _with(f"{LIBRARY}?scope=artists", "catalog-a")).get_json()
    assert artists["sections"]["artists"]["items"][0]["album_count"] == 3

    # Details by (catalogue, album_id): no title or artist needed.
    detail = call("GET", _with(f"{LIBRARY}/album?provider_album_id=al-ed-2", "catalog-a"))
    assert detail.status_code == 200, detail.get_json()
    detail = detail.get_json()
    assert {key: detail["album"][key] for key in ("title", "artist", "provider_album_id",
                                                 "album_key", "track_count")} == {
        "title": "Blue", "artist": "Joni Mitchell", "provider_album_id": "al-ed-2",
        "album_key": "joni mitchell::blue", "track_count": 3}
    assert {track["album_id"] for track in detail["tracks"]} == {"al-ed-2"}
    other = call("GET", _with(f"{LIBRARY}/album?provider_album_id=al-ed-1", "catalog-b"))
    other = other.get_json()
    assert (other["album"]["title"], other["provider_type"], other["catalog_instance_id"]) == (
        "Hejira", "jellyfin", "catalog-b")
    assert [track["track_id"] for track in other["tracks"]] == ["al-ed-1-t1"]

    # LEGACY: title and artist alone (an album_key) still resolve, to one
    # edition (the lowest album_id), whose id the response carries.
    for title in ("Blue", "blue"):
        legacy = call("GET", _with(
            f"{LIBRARY}/album?title={title}&artist=Joni%20Mitchell", "catalog-a")).get_json()
        assert legacy["album"]["provider_album_id"] == "al-ed-1"
        assert legacy["album"]["title"] == "Blue"
        assert [track["track_id"] for track in legacy["tracks"]] == ["al-ed-1-t1", "al-ed-1-t2"]
    assert call("GET", _with(f"{LIBRARY}/album?title=Blue", "catalog-a")).status_code == 400


def test_a_mixed_catalogue_collection_streams_and_shows_art_per_item(workbench, monkeypatch):
    """Art, stream and album details of collection items use each item's
    catalogue; an item without one falls back to the default rule."""
    _two_sources(workbench)
    library, call = workbench.library, workbench.call
    seen = []
    monkeypatch.setattr(library, "_resolve_stream_target", lambda item_id, provider: (
        seen.append(("stream", item_id, provider)) or (None, ("stopped", 502))))
    monkeypatch.setattr(library, "_resolve_art_target", lambda item_id, size, provider: (
        seen.append(("art", item_id, provider))))
    monkeypatch.setattr(library, "_proxy_art", lambda target: None)
    assert call("POST", "/api/collections", {"id": "mix", "name": "Mix"}).status_code == 201
    batch = call("POST", "/api/collections/mix/items/batch", {"items": [
        {"id": "from-a", "kind": "track", "track_id": "al-rain-t1",
         "catalog_instance_id": "catalog-a"},
        {"id": "from-b", "kind": "track", "track_id": "al-kid-t1",
         "catalog_instance_id": "catalog-b"},
        {"id": "album-b", "kind": "album", "provider_album_id": "al-kid",
         "cover_item_id": "al-kid-t2", "catalog_instance_id": "catalog-b"},
        {"id": "unscoped", "kind": "track", "track_id": "al-moon-t1"},
    ]})
    assert batch.status_code == 200, batch.get_json()
    items = {item["id"]: item for item in call("GET", "/api/collections/mix").get_json()["items"]}
    assert {key: item["catalog_instance_id"] for key, item in items.items()} == {
        "from-a": "catalog-a", "from-b": "catalog-b", "album-b": "catalog-b", "unscoped": None}
    providers = {}
    for key, item in items.items():
        media = item["track_id"] or item["cover_item_id"]
        # What the workbench sends: the item's catalogue, else its default.
        scope = item["catalog_instance_id"] or "catalog-a"
        seen.clear()
        assert call("GET", _with(f"{LIBRARY}/stream/{media}", scope)).status_code == 502
        assert call("GET", _with(f"{LIBRARY}/art/{media}?size=120", scope)).status_code == 404
        providers[key] = seen[:]
    assert providers == {
        "from-a": [("stream", "al-rain-t1", "navidrome"), ("art", "al-rain-t1", "navidrome")],
        "from-b": [("stream", "al-kid-t1", "jellyfin"), ("art", "al-kid-t1", "jellyfin")],
        "album-b": [("stream", "al-kid-t2", "jellyfin"), ("art", "al-kid-t2", "jellyfin")],
        "unscoped": [("stream", "al-moon-t1", "navidrome"), ("art", "al-moon-t1", "navidrome")],
    }
    # Without a catalogue the default rule applies: two sources, 400.
    assert call("GET", f"{LIBRARY}/art/al-moon-t1").status_code == 400
    album = call("GET", _with(f"{LIBRARY}/album?provider_album_id=al-kid", "catalog-b"))
    assert [track["track_id"] for track in album.get_json()["tracks"]] == [
        "al-kid-t1", "al-kid-t2"]

    ui = importlib.import_module("plugins.LumaeAnalysis.collection_ui")
    body = ui.render_collection_workbench("Label", "Detail")

    def function(name):
        start = body.index(f"function {name}(")
        return body[start:body.index("\nfunction ", start)]

    assert "const catalogOf=item=>item?.catalog_instance_id||catalog;" in body
    assert "artUrl(cover,120,catalogOf(item))" in function("renderItemRows")
    assert "artUrl(item.cover_item_id||item.track_id,240,catalogOf(item))" in function(
        "collectionMosaic")
    preview = function("playPreview")
    assert "artUrl(cover,120,catalogOf(item))" in preview
    assert "streamUrl(item.track_id,catalogOf(item))" in preview
    assert ("withCatalog(new URLSearchParams({title:item.title,artist:item.artist}),"
            "catalogOf(item))") in function("openAlbum")
    assert "artUrl(cover,480,catalogOf(album))" in function("renderAlbumDetail")
