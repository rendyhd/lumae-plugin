"""JF.8: the plugin mirrors a Jellyfin library.

Jellyfin is admitted next to Navidrome; its reader is always scoped by music
library and runs inside the bound AudioMuse server context; its identity is
the server ``Id`` from ``/System/Info/Public`` (12.0 or later), bound to the
catalogue; Navidrome normalization is byte-for-byte unchanged.
"""

import json
import pathlib

import pytest

from test_lumae_analysis import load_plugin, plugin_api_module  # noqa: F401 (stub)
from jellyfin_fixtures import (
    LIB_A,
    LIB_B,
    SERVER_A,
    SERVER_B,
    FakeCore,
    FakeJellyfinHttp,
    FakeJellyfinModule,
    JellyfinBridge,
    bridge_with_core,
    jellyfin_album_item,
    jellyfin_library_rows,
    jellyfin_public,
    jellyfin_raw_catalog,
    jellyfin_server,
    jellyfin_track_item,
    jid,
    navidrome_catalog,
)

P = "plugin_lumae_analysis__"
GOLDEN = pathlib.Path(__file__).with_name("navidrome_normalization_golden.json")
BASE = "http://jellyfin.test:8096"


def _module(libraries=None, target=None, artists=None, fail_artists=False, public=None,
            **http_kwargs):
    load_plugin()
    http = FakeJellyfinHttp(BASE, libraries or {}, artists=artists, public=public,
                            fail_artists=fail_artists, **http_kwargs)
    return FakeJellyfinModule(http, jellyfin_library_rows(), target=target, base=BASE), http


def _item_walks(http):
    return [call for call in http.calls if call["url"] == f"{BASE}/Items"]


def _two_library_module(**kwargs):
    one = jellyfin_track_item("one", "album-a")
    two = jellyfin_track_item("two", "album-a", index=2, image=True)
    three = jellyfin_track_item("three", "album-b", artist="Guest", album_artist="Various Artists")
    artists = {
        jid("artist:Artist"): {"Id": jid("artist:Artist"), "Type": "MusicArtist",
                               "ImageTags": {"Primary": "p"}},
        jid("artist:Guest"): {"Id": jid("artist:Guest"), "Type": "MusicArtist", "ImageTags": {}},
    }
    libraries = {
        LIB_A: {"Audio": [one, two], "MusicAlbum": [jellyfin_album_item("album-a")]},
        LIB_B: {"Audio": [two, three],
                "MusicAlbum": [jellyfin_album_item("album-b", "Various Artists")]},
    }
    return _module(libraries, artists=artists, **kwargs)


def test_version_parse_and_twelve_minimum():
    from plugins.LumaeAnalysis import jellyfin_provider as jp

    assert jp.parse_version("12.2.0") == (12, 2, 0)
    assert jp.parse_version("v12.0.1-rc2") == (12, 0, 1)
    assert jp.parse_version("12.1") is None
    assert jp.version_supported("12.0.0") is True
    assert jp.version_supported("12.2.0") is True
    assert jp.version_supported("10.11.11") is False
    assert jp.version_supported("") is False
    assert jp.normalize_server_id("5E0D2FD1-C1B5-4D4C-A1D2-B9D2A2C7E4F1") == SERVER_A
    assert jp.normalize_server_id(SERVER_A) == SERVER_A
    assert jp.normalize_server_id("not-a-guid") is None


def test_probe_reads_public_system_info_without_credentials():
    from plugins.LumaeAnalysis import jellyfin_provider as jp

    module, http = _module(public={**jellyfin_public(), "ExtraSecret": "x"})
    identity = jp.probe_identity(module)

    assert identity == {
        "provider_type": "jellyfin",
        "server_type": "jellyfin",
        "server_version": "12.2.0",
        "server_id": SERVER_A,
        "product_name": "Jellyfin Server",
    }
    assert http.calls == [{"url": f"{BASE}/System/Info/Public", "headers": {},
                           "params": {}, "timeout": 5}]


def test_bridge_probe_runs_inside_the_bound_server_context():
    module, _http = _module(public=jellyfin_public())
    bridge = bridge_with_core([jellyfin_server()], {"jellyfin": module})

    identity = bridge.probe_server_identity("server-j")

    assert identity["server_id"] == SERVER_A
    assert bridge.core.bound == ["server-j"]


def test_reader_scopes_every_request_by_library_and_merges_memberships():
    from plugins.LumaeAnalysis import jellyfin_provider as jp
    from plugins.LumaeAnalysis.catalog_providers import CatalogProviderError

    module, http = _two_library_module()
    raw = jp.fetch_catalog(module, error_type=CatalogProviderError)

    walks = _item_walks(http)
    assert {(c["params"]["ParentId"], c["params"]["IncludeItemTypes"]) for c in walks} == {
        (LIB_A, "Audio"), (LIB_A, "MusicAlbum"), (LIB_B, "Audio"), (LIB_B, "MusicAlbum"),
    }
    for call in walks:
        assert call["params"]["Recursive"] == "true"
        assert call["params"]["EnableUserData"] == "false"
        assert call["headers"] == {"Authorization": 'MediaBrowser Token="server-token"'}
        assert "Path" not in call["params"]["Fields"].split(",")
    audio = next(c for c in walks if c["params"]["IncludeItemTypes"] == "Audio")
    assert audio["params"]["Fields"].split(",") == list(jp.TRACK_FIELDS)
    tracks = {row["Id"]: row for row in raw["tracks"]}
    assert set(tracks) == {jid("one"), jid("two"), jid("three")}
    assert tracks[jid("two")]["_lumae_library_ids"] == sorted([LIB_A, LIB_B])
    assert tracks[jid("one")]["_lumae_library_ids"] == [LIB_A]
    for row in raw["tracks"]:
        assert set(row) - set(jp.TRACK_KEYS) <= {"MediaSources", "PrimaryImageItemId",
                                                 "_lumae_library_ids"}
        assert "Path" not in json.dumps(row) and "UserData" not in row
    assert tracks[jid("one")]["MediaSources"] == [{
        "Container": "flac", "Size": 30_000_001, "Bitrate": 1_000_000,
        "MediaStreams": [{"Type": "Audio", "Codec": "flac", "BitRate": 1_000_000,
                          "SampleRate": 44100, "BitDepth": 16, "Channels": 2,
                          "ChannelLayout": "stereo"}],
    }]
    # Its own embedded image, else the album's.
    assert tracks[jid("two")]["PrimaryImageItemId"] == jid("two")
    assert tracks[jid("one")]["PrimaryImageItemId"] == jid("album-a")
    albums = {row["Id"]: row for row in raw["albums"]}
    assert albums[jid("album-b")]["ArtistItems"] == [
        {"Name": "Various Artists", "Id": jid("artist:Various Artists")}]
    assert raw["libraries"] == jellyfin_library_rows()
    # Portraits come from /Artists and /Artists/AlbumArtists per library,
    # never /Persons (its IDs match no ArtistItems ID on 12.2).
    artist_walks = [c for c in http.calls if c["url"] != f"{BASE}/Items"]
    assert {(c["url"], c["params"]["ParentId"]) for c in artist_walks} == {
        (f"{BASE}/Artists", LIB_A), (f"{BASE}/Artists/AlbumArtists", LIB_A),
        (f"{BASE}/Artists", LIB_B), (f"{BASE}/Artists/AlbumArtists", LIB_B),
    }
    for call in artist_walks:
        assert call["headers"] == {"Authorization": 'MediaBrowser Token="server-token"'}
    assert raw["artist_cover_art"] == {
        jid("artist:Artist"): jid("artist:Artist"),
        jid("artist:Guest"): None,
        jid("artist:Various Artists"): None,
    }


def test_reader_honours_the_library_filter_and_refuses_empty_scopes():
    from plugins.LumaeAnalysis import jellyfin_provider as jp
    from plugins.LumaeAnalysis.catalog_providers import CatalogProviderError

    module, http = _two_library_module(target={LIB_B})
    raw = jp.fetch_catalog(module, error_type=CatalogProviderError)
    assert {row["Id"] for row in raw["tracks"]} == {jid("two"), jid("three")}
    assert {c["params"]["ParentId"] for c in http.calls if "ParentId" in c["params"]} == {LIB_B}
    assert all(row["_lumae_library_ids"] == [LIB_B] for row in raw["tracks"])

    module, _http = _two_library_module(target=set())
    with pytest.raises(CatalogProviderError, match="did not match any music library"):
        jp.fetch_catalog(module, error_type=CatalogProviderError)

    module, _http = _module({LIB_A: {"Audio": [], "MusicAlbum": []},
                             LIB_B: {"Audio": [], "MusicAlbum": []}})
    with pytest.raises(CatalogProviderError, match="returned no songs"):
        jp.fetch_catalog(module, error_type=CatalogProviderError)


def test_reader_pages_until_a_short_page(monkeypatch):
    from plugins.LumaeAnalysis import jellyfin_provider as jp

    monkeypatch.setattr(jp, "PAGE_SIZE", 2)
    items = [jellyfin_track_item(f"t{i}", "album-a", index=i) for i in range(5)]
    module, http = _module({LIB_A: {"Audio": items, "MusicAlbum": []},
                            LIB_B: {"Audio": [], "MusicAlbum": []}})
    raw = jp.fetch_catalog(module)
    assert len(raw["tracks"]) == 5
    starts = [c["params"]["StartIndex"] for c in http.calls
              if c["params"].get("ParentId") == LIB_A
              and c["params"].get("IncludeItemTypes") == "Audio"]
    assert starts == [0, 2, 4]


def test_artist_portrait_failure_never_fails_the_scan():
    from plugins.LumaeAnalysis import jellyfin_provider as jp

    module, _http = _two_library_module(fail_artists=True)
    raw = jp.fetch_catalog(module)
    assert raw["artist_cover_art"] == {}
    assert len(raw["tracks"]) == 3


def test_a_library_the_account_may_not_open_fails_the_scan_with_its_own_message():
    """Jellyfin 12.2 answers 401 "... is not permitted to access Library X."
    for a library the account may not open while the token is valid: that is
    not rejected credentials, and the scan stops (a partial catalogue would
    publish the library's tracks as removed)."""
    from plugins.LumaeAnalysis import jellyfin_provider as jp
    from plugins.LumaeAnalysis.catalog_providers import CatalogProviderError

    module, _http = _two_library_module(forbidden={LIB_B})
    with pytest.raises(CatalogProviderError) as forbidden:
        jp.fetch_catalog(module, error_type=CatalogProviderError)
    assert 'may not open the music library "Classical"' in str(forbidden.value)
    assert "kept the published catalogue" in str(forbidden.value)
    assert "sign-in" not in str(forbidden.value)

    module, _http = _two_library_module(unauthorized=True)
    with pytest.raises(CatalogProviderError, match="did not accept AudioMuse's sign-in"):
        jp.fetch_catalog(module, error_type=CatalogProviderError)

    module, _http = _two_library_module(unknown={LIB_A})
    with pytest.raises(CatalogProviderError, match='does not know the music library "Music"'):
        jp.fetch_catalog(module, error_type=CatalogProviderError)


def test_probe_retries_a_starting_server_and_reads_either_casing(monkeypatch):
    """While Jellyfin starts, /System/Info/Public answers 503 and sometimes a
    short-lived camelCase body before the PascalCase one."""
    from plugins.LumaeAnalysis import jellyfin_provider as jp

    slept = []
    monkeypatch.setattr(jp, "_sleep", slept.append)
    starting_body = {"localAddress": "http://10.0.0.2:8096", "version": "12.2.0",
                     "id": SERVER_A, "startupWizardCompleted": True}
    module, http = _module(public=[(503, ""), (200, starting_body), (200, jellyfin_public())])
    assert jp.probe_identity(module)["server_id"] == SERVER_A
    assert len(http.calls) == 3 and slept == [jp.PROBE_RETRY_SECONDS] * 2

    camel = {"version": "12.2.0", "id": SERVER_A.upper(), "productName": "Jellyfin Server"}
    module, http = _module(public=[(200, camel)])
    assert jp.probe_identity(module) == {
        "provider_type": "jellyfin", "server_type": "jellyfin", "server_version": "12.2.0",
        "server_id": SERVER_A, "product_name": "Jellyfin Server",
    }

    module, http = _module(public=[(503, "Jellyfin Server is loading.")] * jp.PROBE_ATTEMPTS)
    with pytest.raises(jp.JellyfinUnavailable, match="still starting"):
        jp.probe_identity(module)
    assert len(http.calls) == jp.PROBE_ATTEMPTS

    module, http = _module(public=[(500, "boom")])
    with pytest.raises(jp.JellyfinHttpError):
        jp.probe_identity(module)
    assert len(http.calls) == 1


def test_rows_drop_folder_parents_and_untagged_sentinel_dates():
    """A track's ParentId is its folder (CD1/CD2 on a multi-disc album); its
    album is AlbumId. Untagged files carry PremiereDate 0001-01-01."""
    from plugins.LumaeAnalysis import jellyfin_provider as jp
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog

    disc_two = jellyfin_track_item("disc-two", "double", index=1, disc=2,
                                   extra={"ParentId": jid("folder:double/CD2")})
    untagged = jellyfin_track_item("untagged", "demo", extra={
        "Album": None, "Artists": [], "ArtistItems": [], "AlbumArtist": None,
        "AlbumArtists": [], "ProductionYear": None,
        "PremiereDate": "0001-01-01T00:00:00.0000000Z",
    })
    loose = jellyfin_track_item("loose", "unused", extra={
        "AlbumId": None, "Album": None, "ParentId": LIB_A})
    rows = [jp.track_row(item, [LIB_A]) for item in (disc_two, untagged, loose)]
    assert "ParentId" not in rows[0] and "ParentId" not in rows[1]
    assert rows[2]["ParentId"] == LIB_A
    assert "PremiereDate" not in rows[1] and "ProductionYear" not in rows[1]
    assert rows[0]["PremiereDate"].startswith("2001")
    album = jp.album_row({**jellyfin_album_item("demo"), "ProductionYear": 1,
                          "PremiereDate": "0001-01-01T00:00:00.0000000Z",
                          "ParentId": jid("folder:x")}, [LIB_A])
    assert "PremiereDate" not in album and "ProductionYear" not in album
    assert "ParentId" not in album
    normalized = normalize_provider_catalog(
        {"libraries": [{"id": LIB_A, "name": "Music"}], "albums": [album],
         "tracks": rows[:2]}, "jellyfin")
    by_id = {row["track_id"]: row for row in normalized["tracks"]}
    assert by_id[jid("disc-two")]["album_id"] == jid("double")
    assert by_id[jid("disc-two")]["disc_number"] == 2
    assert by_id[jid("untagged")]["year"] is None


def test_preview_streams_the_original_without_download_permission():
    """/Items/{id}/Download answers 403 to an account without "allow media
    downloading"; /Audio/{id}/stream?static=true serves the original file
    with byte ranges. Credentials are still sent."""
    from urllib.parse import quote

    from plugins.LumaeAnalysis import jellyfin_provider as jp

    module, _http = _module()
    url, headers, params = jp.stream_target(module, jid("one"), quote)
    assert url == f"{BASE}/Audio/{jid('one')}/stream"
    assert params == {"static": "true"}
    assert headers == {"Authorization": 'MediaBrowser Token="server-token"'}


def test_bridge_fetches_jellyfin_in_the_bound_context():
    module, http = _two_library_module()
    bridge = bridge_with_core([jellyfin_server()], {"jellyfin": module})

    raw = bridge.fetch_catalog("server-j")

    assert bridge.core.bound == ["server-j"]
    assert len(raw["tracks"]) == 3
    assert http.calls


def test_jellyfin_rows_normalize_with_complete_memberships_and_correct_ids():
    from plugins.LumaeAnalysis import jellyfin_provider as jp
    from plugins.LumaeAnalysis.catalog import canonical_json, normalize_provider_catalog
    from plugins.LumaeAnalysis.catalog_providers import _has_complete_library_memberships

    module, _http = _two_library_module()
    raw = jp.fetch_catalog(module)
    assert _has_complete_library_memberships(raw)
    normalized = normalize_provider_catalog(raw, "jellyfin")

    tracks = {row["track_id"]: row for row in normalized["tracks"]}
    three = tracks[jid("three")]
    # A compilation track shows its own artist, not "Various Artists".
    assert three["artist_display"] == "Guest"
    assert three["album_artist_display"] == "Various Artists"
    ids = tracks[jid("one")]["external_ids"]
    assert ids["musicbrainz_recording_id"] == "rec-one"
    assert ids["musicbrainz_release_track_id"] == "reltrack-one"
    assert ids["musicbrainz_release_id"] == "release-album-a"
    assert tracks[jid("one")]["cover_art_id"] == jid("album-a")
    assert tracks[jid("one")]["content_kind"] == "music"
    assert tracks[jid("one")]["audio_properties"]["size"] == 30_000_001
    albums = {row["album_id"]: row for row in normalized["albums"]}
    assert albums[jid("album-b")]["content_kind"] == "music"
    assert albums[jid("album-b")]["cover_art_id"] == jid("album-b")
    album_credits = {(row["album_id"], row["name"]) for row in normalized["album_artists"]}
    assert (jid("album-b"), "Various Artists") in album_credits
    assert (jid("album-b"), "Someone Else") not in album_credits
    memberships = {(row["entity_type"], row["entity_id"], row["library_id"])
                   for row in normalized["entity_libraries"]}
    assert ("track", jid("two"), LIB_A) in memberships
    assert ("track", jid("two"), LIB_B) in memberships
    artists = {row["artist_id"]: row for row in normalized["artists"]}
    assert artists[jid("artist:Artist")]["cover_art_id"] == jid("artist:Artist")
    assert artists[jid("artist:Guest")]["cover_art_id"] is None
    text = canonical_json(normalized)
    for secret in ("/music/", "PlayCount", "LKO2", "server-token"):
        assert secret not in text


def test_navidrome_normalization_is_byte_for_byte_the_1_5_0_output():
    """The Jellyfin branches (artist display, MusicBrainz names) never touch a
    Navidrome row: the golden was produced by the 1.5.0 normalizer."""
    from plugins.LumaeAnalysis.catalog import canonical_json, normalize_provider_catalog

    normalized = normalize_provider_catalog(navidrome_catalog(), "navidrome")
    actual = json.dumps(json.loads(canonical_json(normalized)), indent=1, sort_keys=True,
                        ensure_ascii=False) + "\n"
    assert actual == GOLDEN.read_text(encoding="utf-8")


# --- identity guard and refresh (PostgreSQL) --------------------------------


def _publish(db, bridge):
    from plugins.LumaeAnalysis import catalog

    return catalog.refresh_catalog("server-j", db=db, bridge=bridge)


def _source(db):
    with db.cursor() as cur:
        cur.execute(f"SELECT catalog_instance_id, provider_type, provider_server_id "
                    f"FROM {P}catalog_sources")
        rows = cur.fetchall()
    db.commit()
    return rows


def _transition(db):
    with db.cursor() as cur:
        cur.execute(
            f"SELECT state, detection_reason, required_action, current_provider_version, "
            f"transition_id FROM {P}provider_identity_transitions")
        row = cur.fetchone()
    db.commit()
    return row


def test_a_jellyfin_catalogue_publishes_and_binds_its_server_id(migrated_db):
    from plugins.LumaeAnalysis.catalog import resolve_catalog_source

    bridge = JellyfinBridge(jellyfin_raw_catalog())
    result = _publish(migrated_db, bridge)

    assert result["generation"] == 1
    assert result["counts"]["track"] == 2
    assert _source(migrated_db) == [(result["catalog_instance_id"], "jellyfin", SERVER_A)]
    state, reason, action, version, transition_id = _transition(migrated_db)
    assert (state, action, version, transition_id) == ("normal", None, "12.2.0", None)
    source = resolve_catalog_source(migrated_db, server_id="server-j")[0]
    assert source["provider_type"] == "jellyfin"
    assert source["provider_server_id"] == SERVER_A
    # A second refresh against the same server publishes nothing new.
    again = _publish(migrated_db, bridge)
    assert again["change_reason"] == "no_change"
    assert _transition(migrated_db)[0] == "normal"


def test_another_jellyfin_server_id_blocks_and_is_never_read(migrated_db):
    from plugins.LumaeAnalysis.catalog import CatalogScanError

    bridge = JellyfinBridge(jellyfin_raw_catalog())
    first = _publish(migrated_db, bridge)
    other = JellyfinBridge(jellyfin_raw_catalog(("other",)), server_id=SERVER_B,
                           version="12.3.0")

    with pytest.raises(CatalogScanError, match="provider_server_changed"):
        _publish(migrated_db, other)

    assert other.fetches == 0
    assert _source(migrated_db)[0][2] == SERVER_A
    state, reason, action, version, _transition_id = _transition(migrated_db)
    assert (state, reason, action) == ("blocked", "provider_server_changed",
                                      "restore_provider_server")
    # The other server's version is not recorded as this catalogue's.
    assert version == "12.2.0"
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT published_generation FROM {P}catalog_state")
        assert cur.fetchone()[0] == first["generation"]
    migrated_db.commit()
    # The original server comes back: the block lifts and nothing was lost.
    assert _publish(migrated_db, bridge)["change_reason"] == "no_change"
    assert _transition(migrated_db)[:3] == ("normal", "provider_identity_verified", None)


@pytest.mark.parametrize(
    ("kwargs", "reason", "action"),
    [
        ({"version": "10.11.11"}, "provider_version_unsupported", "upgrade_jellyfin"),
        ({"product": "Emby Server"}, "provider_product_mismatch", "restore_provider_server"),
    ],
)
def test_an_unsupported_jellyfin_identity_blocks_before_any_read(migrated_db, kwargs, reason,
                                                                 action):
    from plugins.LumaeAnalysis.catalog import CatalogScanError

    bridge = JellyfinBridge(jellyfin_raw_catalog(), **kwargs)
    with pytest.raises(CatalogScanError, match=reason):
        _publish(migrated_db, bridge)
    assert bridge.fetches == 0
    assert _transition(migrated_db)[:3] == ("blocked", reason, action)
    # Nothing is bound to a server that failed the identity check.
    assert _source(migrated_db)[0][2] is None


def test_an_unverifiable_published_jellyfin_closes_admission_until_it_answers(migrated_db):
    from plugins.LumaeAnalysis.catalog import CatalogScanError

    bridge = JellyfinBridge(jellyfin_raw_catalog())
    _publish(migrated_db, bridge)
    bridge.probe_error = RuntimeError("connection refused")

    with pytest.raises(CatalogScanError, match="could not verify"):
        _publish(migrated_db, bridge)
    state, reason, action, _version, transition_id = _transition(migrated_db)
    assert (state, reason, action) == ("transition_pending", "provider_version_unverified",
                                      "retry_provider_identity_check")
    assert transition_id
    assert bridge.fetches == 1

    bridge.probe_error = None
    assert _publish(migrated_db, bridge)["change_reason"] == "no_change"
    assert _transition(migrated_db)[0] == "normal"
    assert _transition(migrated_db)[4] is None


def test_a_navidrome_catalogue_whose_server_turns_into_jellyfin_stops(migrated_db):
    """D-JF.2: AudioMuse naming another server type for the same server never
    re-types the catalogue or reads the other server."""
    from test_lumae_analysis import RefreshBridge, _identity_fixture_catalog
    from plugins.LumaeAnalysis import catalog
    from plugins.LumaeAnalysis.catalog import CatalogScanError

    published = catalog.refresh_catalog(
        "server-a", db=migrated_db,
        bridge=RefreshBridge(_identity_fixture_catalog("t-1", "al-1", "ar-1")))

    class TurnedJellyfin(JellyfinBridge):
        def list_servers(self):
            return [self.require_server("server-a")]

        def require_server(self, server_id):
            assert server_id == "server-a"
            return {"server_id": "server-a", "name": "Server", "provider_type": "jellyfin",
                    "is_default": True, "supported": True}

    bridge = TurnedJellyfin(jellyfin_raw_catalog())
    with pytest.raises(CatalogScanError, match="never adopts another server type"):
        catalog.refresh_catalog("server-a", db=migrated_db, bridge=bridge)
    assert bridge.fetches == 0
    with migrated_db.cursor() as cur:
        cur.execute(f"SELECT provider_type FROM {P}catalog_sources")
        assert cur.fetchone()[0] == "navidrome"
        cur.execute(f"SELECT state, detection_reason FROM {P}provider_identity_transitions")
        assert cur.fetchone() == ("blocked", "provider_type_changed")
        cur.execute(f"SELECT published_generation FROM {P}catalog_state")
        assert cur.fetchone()[0] == published["generation"]
    migrated_db.commit()


def test_catalog_health_exposes_the_bound_server_id_and_version(migrated_db, monkeypatch):
    from flask import Flask

    mod = load_plugin()
    bridge = JellyfinBridge(jellyfin_raw_catalog())
    _publish(migrated_db, bridge)
    monkeypatch.setattr(plugin_api_module.config, "APP_VERSION", "v3.6.3")
    monkeypatch.setattr(plugin_api_module, "active_server_id", lambda: "server-j", raising=False)
    monkeypatch.setattr(plugin_api_module, "use_server", lambda _server_id: None, raising=False)
    monkeypatch.setattr(plugin_api_module, "list_servers", lambda: [
        {"server_id": "server-j", "name": "Jelly", "server_type": "jellyfin",
         "is_default": True}], raising=False)
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    monkeypatch.setattr(mod, "ProviderCatalogBridge", lambda *_a, **_k: bridge)
    monkeypatch.setattr(mod, "v3_release_readiness", lambda *_args, **_kwargs: {})
    app = Flask(__name__)
    app.register_blueprint(mod.bp)

    body = app.test_client().get("/api/catalog/health").get_json()

    server = body["servers"][0]
    assert server["provider_type"] == "jellyfin"
    assert server["provider_server_id"] == SERVER_A
    assert server["provider_version"] == "12.2.0"
    assert server["provider_identity_transition"]["state"] == "normal"
    assert server["catalog_sync_allowed"] is True
    assert body["capability"]["supported_provider_types"] == ["jellyfin", "navidrome"]


def test_core_fake_matches_the_production_bridge_shape():
    """Guards the fixture: the production bridge accepts FakeCore."""
    core = FakeCore([jellyfin_server()], {})
    assert bridge_with_core(core.servers, {}).require_server("server-j")["supported"] is True
