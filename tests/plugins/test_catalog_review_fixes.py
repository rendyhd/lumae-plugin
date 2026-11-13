"""Review fixes in catalog.py and catalog_readiness.py that need no database.

* the snapshot size estimate is the same number as before, without running
  ``_safe_payload`` over every row a second time;
* a failed readiness read is undone to a savepoint, so the request's
  transaction stays usable;
* the bootstrap session allowance of a shared principal.
"""

import types

import pytest

from test_lumae_analysis import load_plugin  # noqa: F401  (installs the plugin.api stub)
from plugins.LumaeAnalysis import catalog, catalog_readiness


def _old_estimate(normalized):
    """``_estimate_snapshot_bytes`` as it was: canonical_json of every row."""
    row_groups = (
        *(normalized[catalog.ENTITY_COLLECTIONS[entity]] for entity in catalog.ENTITY_ORDER),
        normalized["track_artists"],
        normalized["album_artists"],
        normalized["entity_libraries"],
    )
    return sum(
        len(catalog.canonical_json(row).encode("utf-8")) + catalog.SNAPSHOT_ENTITY_OVERHEAD_BYTES
        for rows in row_groups
        for row in rows
    )


def _navidrome_catalogue():
    return {
        "libraries": [{"id": "lib-1", "name": " Music "}],
        "artist_cover_art": {"ar-1": "ar-art-1"},
        "albums": [
            {"id": "al-1", "name": "Ålbum", "artist": "Artist 1", "artistId": "ar-1",
             "year": 2001, "genres": [{"name": "Rock"}], "coverArt": "al-1",
             "musicBrainzId": "mb-album", "created": "2024-01-01T00:00:00Z"},
        ],
        "tracks": [
            {
                "id": f"tr-{n}",
                # Decomposed and padded text: the normalizer composes and strips it.
                "title": f"  Café {n} ",
                "album": "Ålbum", "albumId": "al-1",
                "artist": "Artist 1", "artistId": "ar-1",
                "artists": [{"id": "ar-1", "name": "Artist 1"}, {"name": "Guest é"}],
                "duration": 200.5 + n, "musicFolderId": "lib-1", "track": n + 1,
                "discNumber": 1, "size": 1_000 + n, "suffix": "flac", "bitRate": 900,
                "replayGain": {"trackGain": -7.25, "trackPeak": 0.98},
                "genres": [{"name": "Rock"}, {"name": ""}],
                "comment": "", "path": "Artist/Album/file.flac", "userRating": 5,
                "isrc": "USABC1234567", "bpm": 0, "explicitStatus": None,
                "nested": {"password": "x", "keep": ["a", {"url": "u", "b": 1.5}]},
            }
            for n in range(40)
        ],
    }


def _jellyfin_catalogue():
    return {
        "libraries": [{"Id": "lib-j", "Name": "Jelly"}],
        "tracks": [
            {
                "Id": f"jf-{n}", "Name": f"Song {n}", "Album": "LP", "AlbumId": "jf-al",
                "AlbumArtist": "Band", "ArtistItems": [{"Id": "jf-ar", "Name": "Band"}],
                "RunTimeTicks": 2_000_000_000 + n, "IndexNumber": n + 1,
                "ParentIndexNumber": 1, "ProductionYear": 1999, "Genres": ["Jazz"],
                "ProviderIds": {"MusicBrainzTrack": "mb-track", "MusicBrainzRecording": "mb-rec"},
                "MediaSources": [{"Container": "flac", "Size": 5_000 + n, "Bitrate": 800_000,
                                  "MediaStreams": [{"Type": "Audio", "SampleRate": 44100,
                                                    "Channels": 2, "BitDepth": 16}]}],
                "ImageTags": {"Primary": "tag"}, "LibraryId": "lib-j", "Type": "Audio",
                "UserData": {"PlayCount": 3}, "sortName": "song",
            }
            for n in range(25)
        ],
    }


@pytest.mark.parametrize(
    "raw, provider_type",
    [(_navidrome_catalogue(), "navidrome"), (_jellyfin_catalogue(), "jellyfin")],
)
def test_snapshot_estimate_is_unchanged_and_serializes_each_row_once(
    monkeypatch, raw, provider_type,
):
    normalized = catalog.normalize_provider_catalog(raw, provider_type)
    expected = _old_estimate(normalized)
    assert expected > 0

    calls = []
    safe_payload = catalog._safe_payload
    monkeypatch.setattr(
        catalog, "_safe_payload", lambda value: calls.append(1) or safe_payload(value)
    )
    assert catalog._estimate_snapshot_bytes(normalized) == expected
    # The recursive pass that took most of the time is not run again.
    assert calls == []


def test_snapshot_estimate_falls_back_for_a_row_the_encoder_refuses():
    normalized = catalog.normalize_provider_catalog(_navidrome_catalogue(), "navidrome")
    # Values the normalizer never produces; canonical_json maps them to null.
    normalized["tracks"][0]["payload"]["odd"] = float("nan")
    normalized["tracks"][1]["payload"]["odd"] = b"bytes"
    assert catalog._estimate_snapshot_bytes(normalized) == _old_estimate(normalized)


def test_shared_principals_get_the_larger_bootstrap_allowance():
    assert catalog.MAX_PRINCIPAL_BOOTSTRAP_SESSIONS == 4
    assert catalog.MAX_SHARED_BOOTSTRAP_SESSIONS == 32
    assert catalog.bootstrap_session_limit("user:alice") == 4
    assert catalog.bootstrap_session_limit("bearer") == 32
    assert catalog.bootstrap_session_limit("anonymous") == 32
    # Anything else is one caller, as a user is.
    assert catalog.bootstrap_session_limit("client:10.0.0.7") == 4
    assert catalog.bootstrap_session_limit("user:bearer") == 4


# ---------------------------------------------------------------------------
# catalog_readiness: a failed read must not leave the transaction aborted
# ---------------------------------------------------------------------------
class _Cursor:
    def __init__(self, db):
        self.db = db

    def execute(self, sql, params=None):
        self.db.executed.append(sql)
        if any(marker in sql for marker in self.db.failing):
            raise RuntimeError(f"database error in: {sql}")

    def fetchone(self):
        return None

    def close(self):
        pass


class _Db:
    def __init__(self, failing=("status_summary",), autocommit=False):
        self.failing = failing
        self.autocommit = autocommit
        self.executed = []
        self.rollbacks = 0

    def cursor(self):
        return _Cursor(self)

    def rollback(self):
        self.rollbacks += 1


def _readiness(db):
    compatibility = types.SimpleNamespace(core_version="v3.0.5", adapter="v3_registry")
    source = {
        "catalog_instance_id": "catalog-a",
        "server_id": "server-a",
        "catalog": {"generation": 4, "status": "complete"},
        "analysis": {"status": "complete"},
    }
    return catalog_readiness.v3_release_readiness(db, compatibility, source, {})


def test_failed_readiness_read_is_rolled_back_to_its_savepoint():
    db = _Db()
    result = _readiness(db)
    assert result["status"] == "readiness_unavailable"
    assert db.executed[0] == "SAVEPOINT lumae_readiness_read"
    assert db.executed[-2:] == [
        "ROLLBACK TO SAVEPOINT lumae_readiness_read",
        "RELEASE SAVEPOINT lumae_readiness_read",
    ]
    # The caller's transaction is kept: only the reads were undone.
    assert db.rollbacks == 0


def test_failed_readiness_read_rolls_the_transaction_back_without_a_savepoint():
    # The savepoint itself cannot be restored (a broken connection): the
    # transaction is rolled back instead of being left aborted.
    db = _Db(failing=("status_summary", "ROLLBACK TO SAVEPOINT"))
    assert _readiness(db)["status"] == "readiness_unavailable"
    assert db.rollbacks == 1

    # An autocommit connection has no transaction to hold a savepoint.
    db = _Db(autocommit=True)
    assert _readiness(db)["status"] == "readiness_unavailable"
    assert not any("SAVEPOINT" in sql for sql in db.executed)
