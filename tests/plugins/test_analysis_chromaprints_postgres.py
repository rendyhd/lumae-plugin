"""``POST /api/catalog/analysis/chromaprints`` (Lumae Explore, BK.2).

The route hands a client the raw Chromaprints AudioMuse keeps for a
catalogue's tracks, so the client can pass them on. It reads the host's
``chromaprint`` table, which only AudioMuse 3's registry has.

Runs on the real migrated schema, with the host table created beside it.
"""

import base64
import struct
import sys
from types import SimpleNamespace
import zlib

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import load_plugin, plugin_client  # noqa: F401
from plugins.LumaeAnalysis import catalog_analysis


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"


def _items(count, seed=1):
    return [(seed * 2_654_435_761 + index * 40_503) % 2**32 for index in range(count)]


def _stored(items):
    """A print as AudioMuse stores it: zlib of the host-order uint32 items."""
    return zlib.compress(struct.pack(f"={len(items)}I", *items))


def _wire(items):
    return base64.b64encode(struct.pack(f"<{len(items)}I", *items)).decode("ascii")


def _seed_source(db, server_id="server-a"):
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " server_name, is_default, rebind_status) "
            "VALUES (%s, %s, 'navidrome', 'A', TRUE, 'active')",
            (SOURCE, server_id),
        )
        cur.execute(
            f"INSERT INTO {P}catalog_state "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, %s, 'navidrome', 1, 'catalog-epoch', 'complete', '{}'::jsonb)",
            (SOURCE, server_id),
        )
        cur.execute(
            "CREATE TABLE chromaprint (server_id TEXT NOT NULL, "
            "provider_track_id TEXT NOT NULL, fingerprint BYTEA, "
            "updated_at TIMESTAMPTZ DEFAULT now(), "
            "PRIMARY KEY (server_id, provider_track_id))"
        )
    db.commit()


def _store(db, server_id, track_id, blob):
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO chromaprint (server_id, provider_track_id, fingerprint) "
            "VALUES (%s, %s, %s)",
            (server_id, track_id, psycopg2.Binary(blob) if blob is not None else None),
        )
    db.commit()


@pytest.fixture
def client(migrated_db, monkeypatch):
    mod = load_plugin()
    _seed_source(migrated_db)
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    monkeypatch.setattr(
        catalog_analysis, "get_core_adapter", lambda: SimpleNamespace(mode="v3_registry")
    )
    return plugin_client(mod)


def _post(client, **body):
    return client.post(
        "/api/catalog/analysis/chromaprints",
        json={"catalog_instance_id": SOURCE, **body},
    )


def test_serves_the_prints_of_this_catalogues_tracks_only(client, migrated_db):
    first, second = _items(950, 1), _items(700, 2)
    _store(migrated_db, "server-a", "t1", _stored(first))
    _store(migrated_db, "server-a", "t2", _stored(second))
    _store(migrated_db, "server-a", "t3", None)
    # The same track id on another server is another file.
    _store(migrated_db, "server-b", "t4", _stored(_items(950, 4)))

    response = _post(client, provider_track_ids=["t2", "t1", "t1", "t3", "t4", "t5"])

    assert response.status_code == 200, response.data
    assert response.headers["Cache-Control"] == "private, no-store"
    body = response.get_json()
    assert body["catalog_instance_id"] == SOURCE
    assert body["encoding"] == "u32le-base64"
    # In request order, once each; a track without a print is not listed.
    assert body["items"] == [
        {"provider_track_id": "t2", "chromaprint": _wire(second)},
        {"provider_track_id": "t1", "chromaprint": _wire(first)},
    ]


def test_a_long_print_keeps_its_start_and_a_broken_one_is_left_out(client, migrated_db):
    long = _items(catalog_analysis.CHROMAPRINT_MAX_ITEMS + 300, 7)
    _store(migrated_db, "server-a", "long", _stored(long))
    _store(migrated_db, "server-a", "garbage", b"not zlib at all")
    _store(migrated_db, "server-a", "empty", zlib.compress(b""))
    # Three bytes over whole items: the partial item is dropped.
    _store(migrated_db, "server-a", "ragged", zlib.compress(struct.pack("=2I", 5, 6) + b"\x01\x02\x03"))

    body = _post(client, provider_track_ids=["long", "garbage", "empty", "ragged"]).get_json()

    assert body["items"] == [
        {
            "provider_track_id": "long",
            "chromaprint": _wire(long[: catalog_analysis.CHROMAPRINT_MAX_ITEMS]),
        },
        {"provider_track_id": "ragged", "chromaprint": _wire([5, 6])},
    ]


def test_refuses_a_batch_it_cannot_answer(client):
    too_many = [f"t{index}" for index in range(catalog_analysis.CHROMAPRINT_BATCH_MAX + 1)]
    for body in (
        {"provider_track_ids": too_many},
        {"provider_track_ids": "t1"},
        {"catalog_instance_id": "", "provider_track_ids": ["t1"]},
        {"catalog_instance_id": "another-catalog", "provider_track_ids": ["t1"]},
    ):
        response = _post(client, **body)
        assert response.status_code == 400, body
        assert response.get_json()["error"] == "invalid_batch"

    assert _post(client, provider_track_ids=[]).get_json()["items"] == []


def test_a_host_without_the_table_lists_nothing(client, migrated_db, monkeypatch):
    _store(migrated_db, "server-a", "t1", _stored(_items(950)))
    monkeypatch.setattr(
        catalog_analysis, "get_core_adapter", lambda: SimpleNamespace(mode="v2_single_server")
    )

    assert _post(client, provider_track_ids=["t1"]).get_json()["items"] == []


def test_capability_follows_the_adapter_and_the_hosts_setting(monkeypatch):
    monkeypatch.setattr(catalog_analysis.config, "CHROMAPRINT_COLLECTION_ENABLED", True, raising=False)
    assert catalog_analysis.chromaprint_capability("v3_registry") == {
        "version": 1,
        "available": True,
        "max_ids": catalog_analysis.CHROMAPRINT_BATCH_MAX,
        "max_items": catalog_analysis.CHROMAPRINT_MAX_ITEMS,
        "encoding": "u32le-base64",
    }
    assert catalog_analysis.chromaprint_capability("v2_single_server")["available"] is False
    assert catalog_analysis.chromaprint_capability(None)["available"] is False
    monkeypatch.setattr(catalog_analysis.config, "CHROMAPRINT_COLLECTION_ENABLED", False, raising=False)
    assert catalog_analysis.chromaprint_capability("v3_registry")["available"] is False


def test_raw_chromaprint_is_little_endian_whatever_the_host():
    items = _items(60, 3)
    assert sys.byteorder == "little", "the byte-swap branch is covered below"
    assert catalog_analysis.raw_chromaprint(_stored(items)) == struct.pack("<60I", *items)
    assert catalog_analysis.raw_chromaprint(None) is None
    assert catalog_analysis.raw_chromaprint(b"") is None


def test_raw_chromaprint_swaps_a_big_endian_hosts_items(monkeypatch):
    items = _items(60, 3)
    monkeypatch.setattr(catalog_analysis.sys, "byteorder", "big")
    stored_on_big_endian = zlib.compress(struct.pack(">60I", *items))
    assert catalog_analysis.raw_chromaprint(stored_on_big_endian) == struct.pack("<60I", *items)
