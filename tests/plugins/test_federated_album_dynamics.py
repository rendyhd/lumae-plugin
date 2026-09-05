"""Golden contract tests shared with Auralscape's TypeScript scorer."""

import importlib.util
import json
import sys
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "plugins" / "FederatedAlbums" / "album_dynamics.py"
FIXTURE_PATH = (
    ROOT / "tests" / "plugins" / "album_dynamics_golden.json"
)

spec = importlib.util.spec_from_file_location("federated_album_dynamics", MODULE_PATH)
album_dynamics = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = album_dynamics
spec.loader.exec_module(album_dynamics)


def _fixture_albums():
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    albums = []
    for album in payload["albums"]:
        tracks = [
            album_dynamics.AlbumTrack(
                track_id=f'{album["key"]}:{index}',
                embedding=np.asarray(track["embedding"], dtype=np.float32),
                energy=track["energy"],
                mood=np.asarray(track["mood"], dtype=np.float32),
                order=track["order"],
            )
            for index, track in enumerate(album["tracks"])
        ]
        albums.append(
            {
                "albumKey": album["key"],
                "album": album["album"],
                "artist": album["artist"],
                "fingerprint": album_dynamics.build_fingerprint(album["key"], tracks),
            }
        )
    return payload, albums


def test_matches_lumae_golden_ranking():
    payload, albums = _fixture_albums()
    source = next(item for item in albums if item["albumKey"] == payload["sourceKey"])

    ranked = album_dynamics.rank_similar_albums(source, albums, limit=10)

    assert [item["albumKey"] for item in ranked] == payload["expectedOrder"]


def test_wire_fingerprint_round_trip_preserves_scoring_fields():
    _payload, albums = _fixture_albums()
    source = albums[0]

    encoded = album_dynamics.serialize_fingerprint(source["fingerprint"])
    decoded = album_dynamics.deserialize_fingerprint(encoded, source["albumKey"])

    np.testing.assert_array_equal(decoded["meanVector"], source["fingerprint"]["meanVector"])
    np.testing.assert_array_equal(
        decoded["poles"][0]["embedding"], source["fingerprint"]["poles"][0]["embedding"]
    )
    assert decoded["spread"] == source["fingerprint"]["spread"]


def test_catalogue_and_federation_share_golden_order():
    from test_lumae_analysis import load_plugin
    import importlib
    load_plugin()
    core=importlib.import_module('plugins.LumaeAnalysis.catalog_enrichment')
    data=json.loads(FIXTURE_PATH.read_text(encoding='utf-8'))
    fingerprints={album['key']:core._album_fingerprint(album['key'],[
        {'id':str(index),'order':track['order'],'embedding':np.asarray(track['embedding'],dtype=np.float32),
         'energy':track['energy'],'mood':np.asarray(track['mood'],dtype=np.float32)}
        for index,track in enumerate(album['tracks'])]) for album in data['albums']}
    source=fingerprints[data['sourceKey']]
    ranked=sorted((core._album_score(source,value)[0],key) for key,value in fingerprints.items() if key!=data['sourceKey'])
    assert [key for score,key in ranked]==data['expectedOrder']
