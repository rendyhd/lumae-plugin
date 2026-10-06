"""JF.9: a moved Jellyfin file keeps its identity through fingerprint rekeys.

Jellyfin item IDs are ``MD5(type + path)``. A missing Jellyfin track is held
(published unchanged) for 14 days; when AudioMuse maps a new ID to the same
content fingerprint as exactly one held ID, the move is published through
the atomic provider-identity rekey as ``provider_identity_rekey_v2``.
Navidrome never takes this path.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from test_lumae_analysis import load_plugin, plugin_api_module  # noqa: F401 (stub)
from test_collection_feed_epoch_postgres import collections_api  # noqa: F401 (fixture)
from jellyfin_fixtures import (
    LIB_A,
    LIB_B,
    JellyfinBridge,
    jellyfin_album_item,
    jellyfin_track_item,
    jid,
)


P = "plugin_lumae_analysis__"
SERVER = "server-j"
V2 = "provider_identity_rekey_v2"


class MappingCore:
    """The AudioMuse v3 adapter surface the rekey reads (track_server_map)."""

    mode = "test"

    @staticmethod
    def analysis_mapping_sql():
        return (
            "SELECT provider_track_id, item_id AS analysis_id, match_tier "
            "FROM track_server_map WHERE server_id = %s"
        )


def fp(label):
    """An AudioMuse content fingerprint ID (``fp_2`` + 50 hex)."""
    return "fp_2" + hashlib.sha256(label.encode("utf-8")).hexdigest()[:50]


def _rows(db, sql, params=()):
    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    db.commit()
    return rows


def _core_tables(db):
    with db.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS track_server_map (item_id TEXT NOT NULL, "
            "server_id TEXT NOT NULL, provider_track_id TEXT NOT NULL, match_tier TEXT, "
            "PRIMARY KEY (server_id, provider_track_id))"
        )
        cur.execute("CREATE TABLE IF NOT EXISTS task_status (status TEXT, task_type TEXT)")
    db.commit()


def _map(db, mapping):
    """AudioMuse analysed these provider IDs: ``{provider_track_id: fingerprint}``."""
    with db.cursor() as cur:
        for provider_id, fingerprint in mapping.items():
            cur.execute(
                "INSERT INTO track_server_map (item_id, server_id, provider_track_id, match_tier) "
                "VALUES (%s, %s, %s, 'exact') ON CONFLICT (server_id, provider_track_id) "
                "DO UPDATE SET item_id=EXCLUDED.item_id",
                (fingerprint, SERVER, provider_id),
            )
    db.commit()


def _track(label, *, track_id=None, album="album", album_id=None, index=1, library=LIB_A):
    from plugins.LumaeAnalysis import jellyfin_provider

    item = jellyfin_track_item(label, album, index=index)
    if track_id:
        item["Id"] = track_id
        item["MediaSources"][0]["Id"] = track_id
    if album_id:
        item["AlbumId"] = album_id
    return jellyfin_provider.track_row(item, [library])


def _album(album="album", *, album_id=None, library=LIB_A):
    from plugins.LumaeAnalysis import jellyfin_provider

    item = jellyfin_album_item(album)
    if album_id:
        item["Id"] = album_id
    return jellyfin_provider.album_row(item, [library])


def _raw(tracks, albums, libraries=(LIB_A,)):
    return {
        "libraries": [{"id": library, "name": f"Music {library[:2]}"} for library in libraries],
        "albums": albums,
        "tracks": tracks,
        "artist_cover_art": {},
    }


def _refresh(db, bridge):
    from plugins.LumaeAnalysis import catalog

    return catalog.refresh_catalog(SERVER, db=db, bridge=bridge)


def _published(db):
    return {row[0] for row in _rows(db, f"""
        SELECT t.track_id FROM {P}catalog_tracks t
          JOIN {P}catalog_state s USING (catalog_instance_id)
         WHERE t.published_generation=s.published_generation AND t.available""")}


def _published_albums(db):
    return {row[0] for row in _rows(db, f"""
        SELECT a.album_id FROM {P}catalog_albums a
          JOIN {P}catalog_state s USING (catalog_instance_id)
         WHERE a.published_generation=s.published_generation AND a.available""")}


def _rekeys(db):
    return [
        (row[0], row[1], row[2], row[3], row[4] if isinstance(row[4], dict) else json.loads(row[4]))
        for row in _rows(db, f"""
            SELECT entity_type, old_entity_id, entity_id, change_reason, evidence
              FROM {P}catalog_changes WHERE operation='rekey' ORDER BY seq""")
    ]


def _held(db):
    return {row[0]: (row[1], row[2]) for row in _rows(
        db, f"SELECT track_id, fingerprint_id, rekey_abandoned FROM {P}jellyfin_missing_tracks")}


def _transition(db):
    return _rows(db, f"""
        SELECT state, transition_id, rekey_contract, detection_reason, required_action,
               audiomuse_health
          FROM {P}provider_identity_transitions""")[0]


def _start(db, tracks, albums, mapping=None):
    _core_tables(db)
    bridge = JellyfinBridge(_raw(tracks, albums), core=MappingCore())
    first = _refresh(db, bridge)
    assert first["generation"] == 1
    if mapping:
        _map(db, mapping)
    return bridge, first["catalog_instance_id"]


def _album_tracks(album="album", *, album_id=None, ids=None, labels=("one", "two")):
    ids = ids or {}
    return [
        _track(label, track_id=ids.get(label), album=album, album_id=album_id, index=index + 1)
        for index, label in enumerate(labels)
    ]


# --- pure rules ------------------------------------------------------------


def test_v2_mappings_must_be_one_to_one_hex_ids_never_both_old_and_new():
    load_plugin()
    from plugins.LumaeAnalysis.jellyfin_continuity import validate_mapping

    a, b, c = jid("a"), jid("b"), jid("c")
    validate_mapping({"track": {a: b}, "album": {}, "artist": {}})
    for bad in ({a: c, b: c}, {a: b, b: c}, {a: a}, {a: "not-hex"}, {a.upper(): b}):
        with pytest.raises(ValueError):
            validate_mapping({"track": bad, "album": {}, "artist": {}})


def test_the_v2_target_fingerprint_binds_the_pairs():
    load_plugin()
    from plugins.LumaeAnalysis.catalog import normalize_provider_catalog
    from plugins.LumaeAnalysis.jellyfin_continuity import target_fingerprint

    normalized = normalize_provider_catalog(_raw(_album_tracks(), [_album()]), "jellyfin")
    one = {"artist": {}, "album": {}, "track": {jid("x"): jid("one")}}
    other = {"artist": {}, "album": {}, "track": {jid("y"): jid("one")}}
    assert target_fingerprint(normalized, one) == target_fingerprint(normalized, one)
    assert target_fingerprint(normalized, one) != target_fingerprint(normalized, other)


# --- PostgreSQL ------------------------------------------------------------


def test_a_moved_album_keeps_its_tracks_album_and_collections(migrated_db, collections_api):
    from test_collection_rekey_postgres import _album as album_item
    from test_collection_rekey_postgres import _seed
    from test_collection_rekey_postgres import _track as track_item
    from plugins.LumaeAnalysis.provider_identity_guard import provider_transition_health
    from plugins.LumaeAnalysis.provider_identity_rekey import _manifest_hash, read_transition_manifest

    db = migrated_db
    old = {"one": jid("one"), "two": jid("two")}
    bridge, source = _start(db, _album_tracks(), [_album()],
                            {old["one"]: fp("one"), old["two"]: fp("two")})
    _seed(collections_api, "alice", {
        "c1": [track_item("i1", old["one"]), album_item("a1", jid("album"))],
    })

    new = {"one": jid("moved/one"), "two": jid("moved/two")}
    new_album = jid("moved/album")
    bridge.raw = _raw(_album_tracks(album_id=new_album, ids=new), [_album(album_id=new_album)])
    _map(db, {new["one"]: fp("one"), new["two"]: fp("two")})
    fetches = bridge.fetches
    result = _refresh(db, bridge)

    # The proof's second identical scan ran at once.
    assert bridge.fetches == fetches + 2
    assert result["change_reason"] == "jellyfin_fingerprint_rekey_v2"
    transition = result["provider_identity_transition"]
    assert (transition["state"], transition["contract"]) == ("applied", V2)
    assert transition["counts"]["rekey"] == 3
    assert _published(db) == set(new.values())
    assert _published_albums(db) == {new_album}
    rekeys = _rekeys(db)
    assert [(kind, before, after) for kind, before, after, _reason, _evidence in rekeys] == [
        ("album", jid("album"), new_album),
        *sorted((("track", old[label], new[label]) for label in ("one", "two")),
                key=lambda row: row[2]),
    ]
    for kind, _before, after, reason, evidence in rekeys:
        assert reason == "jellyfin_fingerprint_rekey_v2"
        assert evidence["transition_id"] == transition["transition_id"]
        assert evidence["kind"] == "audiomuse_fingerprint"
        assert evidence["provider_version_before"] == evidence["provider_version_after"] == "12.2.0"
        assert evidence["deterministic"] is False
        assert evidence["analysis_identity_preserved"] is True
        if kind == "track":
            label = "one" if after == new["one"] else "two"
            assert evidence["fingerprint_id"] == fp(label)
        else:
            assert evidence["derived_from"] == "track"
            assert evidence["fingerprint_ids"] == sorted([fp("one"), fp("two")])

    manifest = read_transition_manifest(db, transition_id=transition["transition_id"])
    assert manifest["contract"] == V2
    assert manifest["provider_version_before"] == manifest["provider_version_after"] == "12.2.0"
    assert manifest["mappings"] == [
        {"entity_type": "album", "old_id": jid("album"), "new_id": new_album},
        *[{"entity_type": "track", "old_id": old[label], "new_id": new[label]}
          for label in sorted(old, key=lambda name: old[name])],
    ]
    hashed = {key: value for key, value in manifest.items()
              if key not in ("manifest_sha256", "created_at")}
    assert _manifest_hash(hashed) == manifest["manifest_sha256"]

    items = {row[0]: row[1:] for row in _rows(
        db, f"SELECT id, track_id, provider_album_id FROM {P}collection_items")}
    assert items == {"i1": (new["one"], None), "a1": (None, new_album)}

    health = provider_transition_health(db, source)
    assert (health["state"], health["rekey_contract"], health["audiomuse_health"]) == (
        "applied", V2, "ready")
    assert health["catalog_sync_allowed"] is True
    assert _held(db) == {}
    # Nothing left to rekey: the next scan publishes nothing and stays applied.
    assert _refresh(db, bridge)["change_reason"] == "no_change"
    assert _transition(db)[:3] == ("applied", transition["transition_id"], V2)


@pytest.mark.parametrize("album_moves", [False, True], ids=["file_rename", "folder_case_change"])
def test_a_rename_or_case_change_rekeys_what_changed(migrated_db, album_moves):
    db = migrated_db
    old = {"one": jid("one"), "two": jid("two")}
    bridge, _source = _start(db, _album_tracks(), [_album()],
                             {old["one"]: fp("one"), old["two"]: fp("two")})
    if album_moves:
        # "Album" -> "album": every path below changes, so every ID does.
        new = {"one": jid("case/one"), "two": jid("case/two")}
        album_id = jid("case/album")
    else:
        # One file renamed: only its own ID changes.
        new = {"one": jid("renamed/one"), "two": old["two"]}
        album_id = jid("album")
    bridge.raw = _raw(_album_tracks(album_id=album_id, ids=new), [_album(album_id=album_id)])
    _map(db, {new["one"]: fp("one"), new["two"]: fp("two")})

    result = _refresh(db, bridge)

    assert result["provider_identity_transition"]["state"] == "applied"
    expected = [("track", old[label], new[label]) for label in old if old[label] != new[label]]
    if album_moves:
        expected = [("album", jid("album"), album_id), *sorted(expected, key=lambda row: row[2])]
    assert [(kind, before, after) for kind, before, after, *_ in _rekeys(db)] == expected
    assert _published(db) == set(new.values())


def test_two_moves_in_a_row_are_two_transitions(migrated_db):
    db = migrated_db
    bridge, _source = _start(db, _album_tracks(labels=("one",)), [_album()],
                             {jid("one"): fp("one")})
    ids = [jid("one"), jid("first/one"), jid("second/one")]
    transitions = []
    for step in (1, 2):
        album_id = jid(f"album-{step}")
        bridge.raw = _raw(_album_tracks(labels=("one",), album_id=album_id,
                                        ids={"one": ids[step]}),
                          [_album(album_id=album_id)])
        _map(db, {ids[step]: fp("one")})
        result = _refresh(db, bridge)
        transitions.append(result["provider_identity_transition"]["transition_id"])
        assert _published(db) == {ids[step]}

    assert len(set(transitions)) == 2
    assert [(before, after) for kind, before, after, *_ in _rekeys(db) if kind == "track"] == [
        (ids[0], ids[1]), (ids[1], ids[2]),
    ]


def test_moving_files_back_restores_the_original_ids(migrated_db):
    """Jellyfin IDs are path hashes: moving back brings the old ID back to
    life, which is a rekey B -> A (A is no longer published)."""
    db = migrated_db
    original = {"one": jid("one"), "two": jid("two")}
    bridge, _source = _start(db, _album_tracks(), [_album()],
                             {original["one"]: fp("one"), original["two"]: fp("two")})
    moved = {"one": jid("moved/one"), "two": jid("moved/two")}
    bridge.raw = _raw(_album_tracks(album_id=jid("moved/album"), ids=moved),
                      [_album(album_id=jid("moved/album"))])
    _map(db, {moved["one"]: fp("one"), moved["two"]: fp("two")})
    first = _refresh(db, bridge)["provider_identity_transition"]["transition_id"]
    assert _published(db) == set(moved.values())

    # Back where they were; AudioMuse still maps the original IDs as well.
    bridge.raw = _raw(_album_tracks(), [_album()])
    result = _refresh(db, bridge)

    assert result["provider_identity_transition"]["transition_id"] != first
    assert _published(db) == set(original.values())
    assert _published_albums(db) == {jid("album")}
    assert [(kind, before, after) for kind, before, after, *_ in _rekeys(db)][-3:] == [
        ("album", jid("moved/album"), jid("album")),
        *sorted((("track", moved[label], original[label]) for label in moved),
                key=lambda row: row[2]),
    ]


def test_a_missing_track_is_held_and_its_return_is_no_rekey(migrated_db):
    db = migrated_db
    bridge, _source = _start(db, _album_tracks(), [_album()],
                             {jid("one"): fp("one"), jid("two"): fp("two")})
    head = _rows(db, f"SELECT catalog_head_seq FROM {P}catalog_state")[0][0]

    bridge.raw = _raw(_album_tracks(labels=("one",)), [_album()])
    result = _refresh(db, bridge)

    # Held: published exactly as before, album totals included.
    assert result["change_reason"] == "no_change"
    assert result["jellyfin_continuity"]["held"] == 1
    assert _published(db) == {jid("one"), jid("two")}
    assert _held(db) == {jid("two"): (fp("two"), False)}
    assert _transition(db)[0] == "normal"

    bridge.raw = _raw(_album_tracks(), [_album()])
    assert _refresh(db, bridge)["change_reason"] == "no_change"
    assert _held(db) == {}
    assert _rekeys(db) == []
    assert _rows(db, f"SELECT catalog_head_seq FROM {P}catalog_state")[0][0] == head


@pytest.mark.parametrize("duplicate", ["two_held", "two_new"])
def test_duplicates_never_rekey(migrated_db, duplicate):
    db = migrated_db
    bridge, _source = _start(db, _album_tracks(), [_album()])
    if duplicate == "two_held":
        # Both published tracks carry the same audio and both vanish.
        _map(db, {jid("one"): fp("same"), jid("two"): fp("same")})
        bridge.raw = _raw([_track("one", track_id=jid("new/one"))], [_album()])
        _map(db, {jid("new/one"): fp("same")})
        new_ids = {jid("new/one")}
    else:
        _map(db, {jid("one"): fp("one"), jid("two"): fp("two")})
        bridge.raw = _raw([
            _track("two", index=2),
            _track("one", track_id=jid("copy-a/one")),
            _track("one", track_id=jid("copy-b/one"), index=3),
        ], [_album()])
        _map(db, {jid("copy-a/one"): fp("one"), jid("copy-b/one"): fp("one")})
        new_ids = {jid("copy-a/one"), jid("copy-b/one")}

    result = _refresh(db, bridge)

    assert result["change_reason"] == "provider_diff"
    assert _rekeys(db) == []
    assert _transition(db)[0] == "normal"
    # The new tracks publish as new; the vanished ones stay held.
    assert new_ids <= _published(db)
    assert jid("one") in _published(db) and jid("one") in _held(db)


def test_an_unmatched_held_track_is_deleted_after_the_grace_period(migrated_db):
    db = migrated_db
    bridge, _source = _start(db, _album_tracks(), [_album()], {jid("two"): fp("two")})
    bridge.raw = _raw(_album_tracks(labels=("one",)), [_album()])
    _refresh(db, bridge)
    assert jid("two") in _held(db)

    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}jellyfin_missing_tracks SET missing_since=%s",
                    (datetime.now(timezone.utc) - timedelta(days=13, hours=23),))
    db.commit()
    assert _refresh(db, bridge)["change_reason"] == "no_change"
    assert jid("two") in _published(db)

    with db.cursor() as cur:
        cur.execute(f"UPDATE {P}jellyfin_missing_tracks SET missing_since=%s",
                    (datetime.now(timezone.utc) - timedelta(days=14, minutes=1),))
    db.commit()
    result = _refresh(db, bridge)

    assert result["change_reason"] == "provider_diff"
    assert result["jellyfin_continuity"]["expired"] == 1
    assert _published(db) == {jid("one")}
    assert _held(db) == {}
    deletes = _rows(db, f"SELECT entity_id FROM {P}catalog_changes "
                        "WHERE operation='delete' AND entity_type='track'")
    assert deletes == [(jid("two"),)]


def test_a_track_of_a_library_no_longer_read_is_deleted_at_once(migrated_db):
    db = migrated_db
    _core_tables(db)
    bridge = JellyfinBridge(_raw(
        [_track("one"), _track("two", album="other", library=LIB_B)],
        [_album(), _album("other", library=LIB_B)],
        libraries=(LIB_A, LIB_B),
    ), core=MappingCore())
    _refresh(db, bridge)
    bridge.raw = _raw([_track("one")], [_album()])

    assert _refresh(db, bridge)["change_reason"] == "provider_diff"
    assert _published(db) == {jid("one")}
    assert _held(db) == {}


def test_an_unanalysed_move_target_waits_until_audiomuse_maps_it(migrated_db):
    db = migrated_db
    bridge, _source = _start(db, _album_tracks(), [_album()],
                             {jid("one"): fp("one"), jid("two"): fp("two")})
    new = {"one": jid("moved/one"), "two": jid("moved/two")}
    new_album = jid("moved/album")
    bridge.raw = _raw(_album_tracks(album_id=new_album, ids=new), [_album(album_id=new_album)])

    # AudioMuse has not analysed the moved files yet: nothing changes, the
    # new IDs (and their new album) are not published as new tracks.
    waiting = _refresh(db, bridge)
    assert waiting["change_reason"] == "no_change"
    assert waiting["jellyfin_continuity"]["held_back"] == 2
    assert _published(db) == {jid("one"), jid("two")}
    assert _published_albums(db) == {jid("album")}

    # One file analysed: the album waits for its sibling.
    _map(db, {new["one"]: fp("one")})
    partial = _refresh(db, bridge)
    assert partial["change_reason"] == "no_change"
    assert partial["jellyfin_continuity"]["deferred_pairs"] == 1
    assert _rekeys(db) == []

    _map(db, {new["two"]: fp("two")})
    result = _refresh(db, bridge)
    assert result["provider_identity_transition"]["state"] == "applied"
    assert _published(db) == set(new.values())
    assert ("album", jid("album"), new_album) in [
        (kind, before, after) for kind, before, after, *_ in _rekeys(db)]


def test_a_busy_audiomuse_defers_the_rekey(migrated_db):
    db = migrated_db
    bridge, _source = _start(db, _album_tracks(labels=("one",)), [_album()],
                             {jid("one"): fp("one")})
    bridge.raw = _raw(_album_tracks(labels=("one",), ids={"one": jid("moved/one")}), [_album()])
    _map(db, {jid("moved/one"): fp("one")})
    with db.cursor() as cur:
        cur.execute("INSERT INTO task_status VALUES ('STARTED', 'main_analysis')")
    db.commit()

    deferred = _refresh(db, bridge)
    assert deferred["change_reason"] == "no_change"
    assert _transition(db)[0] == "normal"
    assert _published(db) == {jid("one")}

    with db.cursor() as cur:
        cur.execute("DELETE FROM task_status")
    db.commit()
    assert _refresh(db, bridge)["provider_identity_transition"]["state"] == "applied"
    assert _published(db) == {jid("moved/one")}


def test_a_rekey_that_keeps_failing_is_abandoned_after_three_tries(migrated_db, monkeypatch):
    from plugins.LumaeAnalysis import provider_identity_rekey

    db = migrated_db
    bridge, _source = _start(db, _album_tracks(labels=("one",)), [_album()],
                             {jid("one"): fp("one")})
    bridge.raw = _raw(_album_tracks(labels=("one",), ids={"one": jid("moved/one")}), [_album()])
    _map(db, {jid("moved/one"): fp("one")})

    def broken(*_args, **_kwargs):
        raise ValueError("simulated publication failure")

    monkeypatch.setattr(provider_identity_rekey, "_publish_provider_identity_rekey", broken)
    for attempt in range(3):
        with pytest.raises(ValueError, match="simulated"):
            _refresh(db, bridge)
        assert _published(db) == {jid("one")}
    state, transition_id, contract, reason, _action, _health = _transition(db)
    assert (state, transition_id, contract, reason) == (
        "normal", None, None, "provider_rekey_abandoned")
    assert _held(db) == {jid("one"): (fp("one"), True)}

    # The catalogue moves on: the new ID publishes as new, the old one stays
    # held until its grace period ends.
    monkeypatch.undo()
    result = _refresh(db, bridge)
    assert result["change_reason"] == "provider_diff"
    assert _published(db) == {jid("one"), jid("moved/one")}


def test_navidrome_never_holds_or_rekeys_by_fingerprint(migrated_db):
    from test_lumae_analysis import RefreshBridge, _identity_fixture_catalog
    from plugins.LumaeAnalysis import catalog

    db = migrated_db
    _core_tables(db)
    payload = _identity_fixture_catalog("t-1", "al-1", "ar-1")
    payload["tracks"].append({**payload["tracks"][0], "id": "t-2", "title": "Other"})
    bridge = RefreshBridge(payload)
    catalog.refresh_catalog("server-a", db=db, bridge=bridge)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO track_server_map VALUES (%s, 'server-a', 't-2', 'exact'), "
            "(%s, 'server-a', 't-3', 'exact')", (fp("same"), fp("same")))
    db.commit()
    moved = _identity_fixture_catalog("t-1", "al-1", "ar-1")
    moved["tracks"].append({**moved["tracks"][0], "id": "t-3", "title": "Other"})
    bridge.payload = moved

    result = catalog.refresh_catalog("server-a", db=db, bridge=bridge)

    assert result["change_reason"] == "provider_diff"
    assert "jellyfin_continuity" not in result
    assert _published(db) == {"t-1", "t-3"}
    assert _rekeys(db) == []
    assert _held(db) == {}
