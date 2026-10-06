"""JF.10: tags read from the original Jellyfin file.

Jellyfin 12 does not expose release type, compilation/explicit flags, most
credits, ISRC, disc subtitle, original date, BPM, sort names, album
ReplayGain or peaks. The plugin reads them from the file it already holds
and publishes them under exactly the OpenSubsonic keys the Navidrome reader
produces. Only present tags are emitted; Navidrome never takes this path.
"""

import json

import numpy as np
import pytest

from test_lumae_analysis import load_plugin, plugin_api_module  # noqa: F401 (stub)
from jellyfin_fixtures import (
    LIB_A,
    JellyfinBridge,
    jellyfin_album_item,
    jellyfin_track_item,
    jid,
    navidrome_catalog,
)


P = "plugin_lumae_analysis__"
av = pytest.importorskip("av")

TAGS = {
    "title": "One",
    "RELEASETYPE": "album; compilation",
    "compilation": "1",
    "ITUNESADVISORY": "1",
    "COMPOSER": "Clara Composer",
    "LYRICIST": "Lena Lyricist",
    "PRODUCER": "Pete Producer",
    "ENGINEER": "Eve Engineer",
    "MIXER": "Max Mixer",
    "REMIXER": "Rex Remixer",
    "ARRANGER": "Ann Arranger",
    "CONDUCTOR": "Carl Conductor",
    "PERFORMER": "Gus Guitar (guitar)",
    "ISRC": "NLTST2400001",
    "DISCSUBTITLE": "Side A",
    "ORIGINALDATE": "2019-11-01",
    "BPM": "101",
    "REPLAYGAIN_TRACK_GAIN": "-6.60 dB",
    "REPLAYGAIN_TRACK_PEAK": "0.912345",
    "REPLAYGAIN_ALBUM_GAIN": "-7.25 dB",
    "REPLAYGAIN_ALBUM_PEAK": "0.987654",
    "TITLESORT": "One, The",
    "ALBUMSORT": "Album, The",
}
EXPECTED = {
    "release_types": ["album", "compilation"],
    "compilation": True,
    "explicit": True,
    "isrc": ["NLTST2400001"],
    "disc_subtitle": "Side A",
    "original_date": {"year": 2019, "month": 11, "day": 1},
    "bpm": 101,
    "replay_gain": {"track_gain": -6.6, "track_peak": 0.912345,
                    "album_gain": -7.25, "album_peak": 0.987654},
    "title_sort": "One, The",
    "album_sort": "Album, The",
    "credits": [
        ["composer", "Clara Composer", None],
        ["lyricist", "Lena Lyricist", None],
        ["producer", "Pete Producer", None],
        ["engineer", "Eve Engineer", None],
        ["mixer", "Max Mixer", None],
        ["remixer", "Rex Remixer", None],
        ["arranger", "Ann Arranger", None],
        ["conductor", "Carl Conductor", None],
        ["performer", "Gus Guitar", "guitar"],
    ],
}
FORMATS = {
    "flac": ("flac", None, None),
    # ffmpeg writes PERFORMER to ID3 TPE3, which is the conductor frame.
    "mp3": ("libmp3lame", None, None),
    "m4a": ("aac", "ipod", {"movflags": "use_metadata_tags"}),
    "opus": ("libopus", "ogg", None),
}
_DTYPES = {"s16": np.int16, "s16p": np.int16, "s32": np.int32, "s32p": np.int32,
           "flt": np.float32, "fltp": np.float32, "dbl": np.float64, "dblp": np.float64}


def write_audio(path, tags, extension="flac"):
    codec, fmt, options = FORMATS[extension]
    rate = 48000 if codec == "libopus" else 44100
    with av.open(str(path), "w", format=fmt, options=options or {}) as container:
        for key, value in tags.items():
            container.metadata[key] = value
        stream = container.add_stream(codec, rate=rate)
        stream.layout = "stereo"
        samples = stream.codec_context.frame_size or 1024
        dtype = _DTYPES[stream.format.name]
        shape = (2, samples) if stream.format.is_planar else (1, samples * 2)
        for index in range(4):
            frame = av.AudioFrame.from_ndarray(
                np.zeros(shape, dtype=dtype), format=stream.format.name, layout="stereo")
            frame.sample_rate = rate
            frame.pts = index * samples
            for packet in stream.encode(frame):
                container.mux(packet)
        for packet in stream.encode(None):
            container.mux(packet)
    return str(path)


def _expected(extension):
    expected = json.loads(json.dumps(EXPECTED))
    if extension == "mp3":
        expected["credits"] = [
            credit for credit in expected["credits"] if credit[0] != "performer"
        ] + [["conductor", "Gus Guitar (guitar)", None]]
    return expected


@pytest.mark.parametrize("extension", sorted(FORMATS))
def test_pyav_reads_every_published_tag(tmp_path, monkeypatch, extension):
    load_plugin()
    from plugins.LumaeAnalysis import file_tags

    monkeypatch.setattr(file_tags, "_mutagen_entries", lambda _path: None)
    path = write_audio(tmp_path / f"one.{extension}", TAGS, extension)

    result = file_tags.read_file_tags(path)

    assert result["reader"] == "pyav"
    assert result["tags"] == _expected(extension)


def _itunes_tags(path):
    """Tag an MP4 file as iTunes and Picard do (atoms and ``----`` freeform
    names); ffmpeg's ``use_metadata_tags`` writes QuickTime ``mdta`` keys
    instead, which only ffmpeg reads back."""
    from mutagen.mp4 import MP4, MP4FreeForm

    audio = MP4(path)
    audio.tags.clear()
    plain = {"COMPOSER", "compilation", "ITUNESADVISORY", "BPM", "TITLESORT", "ALBUMSORT",
             "title"}
    for key, value in TAGS.items():
        if key not in plain:
            audio.tags[f"----:com.apple.iTunes:{key}"] = [MP4FreeForm(value.encode("utf-8"))]
    audio.tags["\u00a9nam"] = [TAGS["title"]]
    audio.tags["\u00a9wrt"] = [TAGS["COMPOSER"]]
    audio.tags["cpil"] = True
    audio.tags["rtng"] = [1]
    audio.tags["tmpo"] = [int(TAGS["BPM"])]
    audio.tags["sonm"] = [TAGS["TITLESORT"]]
    audio.tags["soal"] = [TAGS["ALBUMSORT"]]
    audio.save()


def _picard_id3(path):
    """Write credits and the original date as Picard does in ID3v2.4: TIPL
    and TMCL people lists, TPE3 for the conductor, TDOR."""
    from mutagen.id3 import ID3, TDOR, TIPL, TMCL, TPE3

    tags = ID3(path)
    for name in ("ORIGINALDATE", "PRODUCER", "ENGINEER", "MIXER", "ARRANGER", "CONDUCTOR"):
        tags.delall(f"TXXX:{name}")
    tags.delall("TPE3")
    tags.add(TDOR(encoding=3, text=["2019-11-01"]))
    tags.add(TIPL(encoding=3, people=[["producer", "Pete Producer"], ["engineer", "Eve Engineer"],
                                      ["mix", "Max Mixer"], ["arranger", "Ann Arranger"]]))
    tags.add(TPE3(encoding=3, text=["Carl Conductor"]))
    tags.add(TMCL(encoding=3, people=[["guitar", "Gus Guitar"]]))
    tags.save()


@pytest.mark.parametrize("extension", sorted(FORMATS))
def test_mutagen_reads_the_same_tags(tmp_path, extension):
    pytest.importorskip("mutagen")
    load_plugin()
    from plugins.LumaeAnalysis import file_tags

    path = write_audio(tmp_path / f"one.{extension}", TAGS, extension)
    if extension == "m4a":
        _itunes_tags(path)
    if extension == "mp3":
        _picard_id3(path)
    result = file_tags.read_file_tags(path)

    assert result["reader"] == "mutagen"
    # Picard's ID3 people lists carry the performer the ffmpeg layout lost.
    assert result["tags"] == (EXPECTED if extension == "mp3" else _expected(extension))


def test_untagged_files_emit_nothing(tmp_path, monkeypatch):
    load_plugin()
    from plugins.LumaeAnalysis import file_tags

    monkeypatch.setattr(file_tags, "_mutagen_entries", lambda _path: None)
    path = write_audio(tmp_path / "plain.flac", {"title": "Plain"})
    tags = file_tags.read_file_tags(path)["tags"]
    assert tags == {}
    assert file_tags.track_keys(tags) == {}
    assert file_tags.album_keys([tags]) == {}


def test_id3_people_lists_and_mp4_atoms_map_to_roles():
    load_plugin()
    from plugins.LumaeAnalysis.file_tags import canonical_tags

    tags = canonical_tags(
        {"tpe3": ["Carl"], "tcmp": ["0"], "rtng": ["2"], "tmpo": ["88"],
         "©wrt": ["Clara"], "musicbrainz album type": ["single"],
         "tdor": ["1999"], "replaygain_track_gain": ["+1.5 dB"], "isrc": ["usabc9900001"]},
        [("producer", "Pete", None), ("performer", "Gus", "bass")],
    )
    assert tags == {
        "release_types": ["single"],
        "compilation": False,
        "explicit": False,
        "isrc": ["USABC9900001"],
        "original_date": {"year": 1999},
        "bpm": 88,
        "replay_gain": {"track_gain": 1.5},
        "credits": [["composer", "Clara", None], ["producer", "Pete", None],
                    ["conductor", "Carl", None], ["performer", "Gus", "bass"]],
    }


def test_opensubsonic_keys_are_exactly_the_navidrome_names():
    load_plugin()
    from plugins.LumaeAnalysis.file_tags import album_keys, track_keys

    assert track_keys(EXPECTED) == {
        "isrc": ["NLTST2400001"],
        "discTitle": "Side A",
        "isCompilation": True,
        "isExplicit": True,
        "explicitStatus": "explicit",
        "replayGain": {"trackGain": -6.6, "trackPeak": 0.912345,
                       "albumGain": -7.25, "albumPeak": 0.987654},
        "bpm": 101,
        "contributors": [
            {"role": "composer", "artist": {"name": "Clara Composer"}},
            {"role": "lyricist", "artist": {"name": "Lena Lyricist"}},
            {"role": "producer", "artist": {"name": "Pete Producer"}},
            {"role": "engineer", "artist": {"name": "Eve Engineer"}},
            {"role": "mixer", "artist": {"name": "Max Mixer"}},
            {"role": "remixer", "artist": {"name": "Rex Remixer"}},
            {"role": "arranger", "artist": {"name": "Ann Arranger"}},
            {"role": "conductor", "artist": {"name": "Carl Conductor"}},
            {"role": "performer", "subRole": "guitar", "artist": {"name": "Gus Guitar"}},
        ],
        "sortName": "One, The",
    }
    assert album_keys([EXPECTED, {**EXPECTED, "explicit": False}]) == {
        "releaseTypes": ["album", "compilation"],
        "isCompilation": True,
        "originalReleaseDate": {"year": 2019, "month": 11, "day": 1},
        "explicitStatus": "explicit",
        "sortName": "Album, The",
    }
    # Tracks that disagree leave the album key out; a clean album says so.
    assert album_keys([{"release_types": ["album"]}, {"release_types": ["ep"]},
                       {"explicit": False}]) == {"explicitStatus": "clean"}


def _jellyfin_raw(tags_by_label=None):
    from plugins.LumaeAnalysis import jellyfin_provider
    from plugins.LumaeAnalysis.file_tags import album_keys, track_keys

    tags_by_label = tags_by_label or {}
    tracks = []
    for index, label in enumerate(("one", "two")):
        row = jellyfin_provider.track_row(jellyfin_track_item(label, index=index + 1), [LIB_A])
        if label in tags_by_label:
            row.update(track_keys(tags_by_label[label]))
        tracks.append(row)
    album = jellyfin_provider.album_row(jellyfin_album_item("album"), [LIB_A])
    if tags_by_label:
        album.update(album_keys(list(tags_by_label.values())))
    return {"libraries": [{"id": LIB_A, "name": "Music"}], "albums": [album],
            "tracks": tracks, "artist_cover_art": {}}


def test_merged_tags_normalize_like_the_navidrome_reader():
    """The same OpenSubsonic keys give the same canonical fields and payload
    keys whether Navidrome or the Jellyfin file served them."""
    load_plugin()
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.file_tags import album_keys, track_keys

    jellyfin = normalize_provider_catalog(_jellyfin_raw({"one": EXPECTED}), "jellyfin")
    navidrome_raw = navidrome_catalog()
    navidrome_raw["tracks"][1].update(track_keys(EXPECTED))
    navidrome_raw["albums"][0].update(album_keys([EXPECTED]))
    navidrome = normalize_provider_catalog(navidrome_raw, "navidrome")

    j_track = {row["track_id"]: row for row in jellyfin["tracks"]}[jid("one")]
    n_track = {row["track_id"]: row for row in navidrome["tracks"]}["tr-2"]
    for field in ("replay_gain", "compilation", "explicit", "disc_title", "release_type"):
        assert j_track[field] == n_track[field], field
    assert j_track["external_ids"]["isrc"] == n_track["external_ids"]["isrc"]
    assert j_track["replay_gain"] == {"track_gain_db": -6.6, "track_peak": 0.912345,
                                      "album_gain_db": -7.25, "album_peak": 0.987654}
    assert (j_track["compilation"], j_track["explicit"], j_track["disc_title"]) == (
        True, True, "Side A")
    for key in ("bpm", "contributors", "sortName", "replayGain", "explicitStatus", "isrc"):
        assert j_track["payload"][key] == n_track["payload"][key], key
    j_album = jellyfin["albums"][0]
    n_album = navidrome["albums"][0]
    assert j_album["release_type"] == n_album["release_type"]
    for key in ("releaseTypes", "originalReleaseDate", "isCompilation"):
        assert j_album["payload"][key] == n_album["payload"][key], key

    # Untagged Jellyfin rows keep their JF.8 fingerprints (a sibling's
    # track-only tag changes nothing for them); tagged rows fold the tags
    # in, so a tag change is an upsert.
    plain = normalize_provider_catalog(_jellyfin_raw(), "jellyfin")
    bpm_only = normalize_provider_catalog(_jellyfin_raw({"one": {"bpm": 101}}), "jellyfin")
    plain_two = {row["track_id"]: row for row in plain["tracks"]}[jid("two")]
    sibling_two = {row["track_id"]: row for row in bpm_only["tracks"]}[jid("two")]
    assert plain_two["metadata_fp"] == sibling_two["metadata_fp"]
    plain_one = {row["track_id"]: row for row in plain["tracks"]}[jid("one")]
    assert plain_one["metadata_fp"] != j_track["metadata_fp"]
    retagged = normalize_provider_catalog(
        _jellyfin_raw({"one": {**EXPECTED, "bpm": 102}}), "jellyfin")
    assert {row["track_id"]: row for row in retagged["tracks"]}[jid("one")]["metadata_fp"] \
        != j_track["metadata_fp"]


def test_navidrome_fingerprints_never_fold_file_tags():
    load_plugin()
    from plugins.LumaeAnalysis.catalog import _with_file_tags
    from plugins.LumaeAnalysis.file_tags import TRACK_KEYS, track_keys

    raw = track_keys(EXPECTED)
    assert _with_file_tags("fp", raw, "navidrome", TRACK_KEYS) == "fp"
    assert _with_file_tags("fp", {}, "jellyfin", TRACK_KEYS) == "fp"
    assert _with_file_tags("fp", raw, "jellyfin", TRACK_KEYS) != "fp"


# --- PostgreSQL ------------------------------------------------------------


def _refresh(db, bridge, server_id="server-j"):
    from plugins.LumaeAnalysis import catalog

    return catalog.refresh_catalog(server_id, db=db, bridge=bridge)


def _payload(db, track_id):
    row = _rows(db, f"""
        SELECT t.payload FROM {P}catalog_tracks t
          JOIN {P}catalog_state s USING (catalog_instance_id)
         WHERE t.published_generation=s.published_generation AND t.track_id=%s""",
                (track_id,))[0][0]
    return row if isinstance(row, dict) else json.loads(row)


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


@pytest.mark.parametrize("isolation", ["fork", "exec"])
def test_tags_are_read_in_the_isolated_child_on_either_path(tmp_path, monkeypatch, isolation):
    """The exec worker imports file_tags without the plugin __init__ or the
    host API (a threaded process: the web tier, threaded tests)."""
    load_plugin()
    from plugins.LumaeAnalysis import analysis_isolation as iso
    from plugins.LumaeAnalysis import file_tags

    if isolation == "exec":
        monkeypatch.setattr(iso, "FORK_FAST_PATH", False)
    path = write_audio(tmp_path / "one.flac", TAGS)
    try:
        result = iso.run_isolated(file_tags.read_file_tags, path, limit_seconds=20)
    finally:
        iso.shutdown()
    assert result["tags"]["bpm"] == 101
    assert result["tags"]["credits"][0] == ["composer", "Clara Composer", None]


def test_captured_tags_publish_as_ordinary_upserts(migrated_db, tmp_path):
    from plugins.LumaeAnalysis import file_tags

    db = migrated_db
    bridge = JellyfinBridge(_jellyfin_raw())
    source = _refresh(db, bridge)["catalog_instance_id"]
    head = _rows(db, f"SELECT catalog_head_seq FROM {P}catalog_state")[0][0]
    path = write_audio(tmp_path / "one.flac", TAGS)

    assert file_tags.capture(db, source, jid("one"), path) == "stored"
    assert file_tags.capture(db, source, jid("one"), path) == "unchanged"
    result = _refresh(db, bridge)

    assert result["change_reason"] == "provider_diff"
    events = _rows(db, f"""SELECT entity_type, entity_id, operation FROM {P}catalog_changes
                            WHERE seq > %s ORDER BY seq""", (head,))
    # The album takes the file's release type, which its other track shows too.
    assert events == [("album", jid("album"), "upsert"),
                      *sorted(("track", jid(label), "upsert") for label in ("one", "two"))]
    payload = _payload(db, jid("one"))
    assert payload["bpm"] == 101
    assert payload["replayGain"]["albumGain"] == -7.25
    assert payload["_lumae"]["replay_gain"]["album_peak"] == 0.987654
    assert payload["_lumae"]["disc_title"] == "Side A"
    assert payload["contributors"][-1] == {
        "role": "performer", "subRole": "guitar", "artist": {"name": "Gus Guitar"}}
    assert "bpm" not in _payload(db, jid("two"))
    assert _refresh(db, bridge)["change_reason"] == "no_change"

    # A retagged file is an ordinary upsert of that track only.
    write_audio(tmp_path / "one.flac", {**TAGS, "BPM": "140"})
    assert file_tags.capture(db, source, jid("one"), path) == "stored"
    head = _rows(db, f"SELECT catalog_head_seq FROM {P}catalog_state")[0][0]
    assert _refresh(db, bridge)["change_reason"] == "provider_diff"
    assert _rows(db, f"SELECT entity_type, entity_id FROM {P}catalog_changes WHERE seq > %s",
                 (head,)) == [("track", jid("one"))]
    assert _payload(db, jid("one"))["bpm"] == 140


def test_a_navidrome_source_never_reads_or_stores_file_tags(migrated_db, tmp_path, monkeypatch):
    from test_lumae_analysis import RefreshBridge, _identity_fixture_catalog
    from plugins.LumaeAnalysis import catalog, file_tags

    db = migrated_db
    source = catalog.refresh_catalog(
        "server-a", db=db,
        bridge=RefreshBridge(_identity_fixture_catalog("t-1", "al-1", "ar-1")),
    )["catalog_instance_id"]
    monkeypatch.setattr(file_tags, "read_file_tags",
                        lambda _path: pytest.fail("Navidrome files are never read"))

    assert file_tags.capture(db, source, "t-1", write_audio(tmp_path / "n.flac", TAGS)) is None
    assert _rows(db, f"SELECT COUNT(*) FROM {P}jellyfin_file_tags") == [(0,)]


def test_tags_move_with_a_fingerprint_rekey(migrated_db, tmp_path):
    from test_jellyfin_continuity import MappingCore, _album, _core_tables, _map, _raw, _track, fp
    from plugins.LumaeAnalysis import file_tags

    db = migrated_db
    _core_tables(db)
    bridge = JellyfinBridge(_raw([_track("one")], [_album()]), core=MappingCore())
    source = _refresh(db, bridge)["catalog_instance_id"]
    _map(db, {jid("one"): fp("one")})
    file_tags.capture(db, source, jid("one"), write_audio(tmp_path / "one.flac", TAGS))
    _refresh(db, bridge)
    assert _payload(db, jid("one"))["bpm"] == 101

    moved = jid("moved/one")
    bridge.raw = _raw([_track("one", track_id=moved)], [_album()])
    _map(db, {moved: fp("one")})
    result = _refresh(db, bridge)

    assert result["provider_identity_transition"]["state"] == "applied"
    assert _rows(db, f"SELECT track_id FROM {P}jellyfin_file_tags") == [(moved,)]
    assert _payload(db, moved)["bpm"] == 101
    assert _refresh(db, bridge)["change_reason"] == "no_change"


def test_the_analysis_hook_and_profile_download_capture_tags(monkeypatch, tmp_path):
    mod = load_plugin()
    seen = []
    monkeypatch.setattr(mod.file_tags, "capture",
                        lambda _db, source, track_id, path: seen.append((source, track_id, path)))
    monkeypatch.setattr(mod, "get_db", lambda: object())
    monkeypatch.setattr(mod, "maintenance_paused", lambda: False)
    monkeypatch.setattr(mod, "upsert_profile", lambda *args, **kwargs: True)
    monkeypatch.setattr(mod, "run_file_analysis", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(mod, "_schedule_edge_upgrade", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "remove_downloaded_file", lambda _path: None)
    audio = tmp_path / "downloaded.flac"
    audio.write_bytes(b"x")
    monkeypatch.setattr(mod, "load_track_file", lambda *args, **kwargs: {
        "track_id": "track-a", "file_path": str(audio), "media_signature": "sig",
        "cleanup_path": None})

    mod.analyze_one_track("track-a", catalog_instance_id="catalog-a", server_id="server-a",
                          attempt_token="token")
    assert seen == [("catalog-a", "track-a", str(audio))]

    seen.clear()
    monkeypatch.setattr(mod, "get_core_adapter", lambda: type("A", (), {
        "normalize_analysis_hook": staticmethod(lambda song: {"server_id": "server-a"})})())
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_kwargs: {"catalog_instance_id": "catalog-a"})
    monkeypatch.setattr(mod, "published_profile_current", lambda *args: True)
    monkeypatch.setattr(mod, "serve_deferred_interactive", lambda **_kwargs: None)
    mod.analyze_song_hook({"item_id": "track-b", "audio_path": str(audio)})
    assert seen == [("catalog-a", "track-b", str(audio))]
