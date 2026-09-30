"""Phase C (F9): vectors after a projection rebuild.

``/api/catalog/analysis/vectors`` accepts any generation up to the current
one. A generation older than current whose rows were pruned (no bootstrap
lease pinned it) is served from the current generation, with
``requested_generation`` in the header; a leased generation is served exactly.
Every page lists requested ids without a served vector under ``missing``.
``strict_generation: true`` turns a pruned generation into 410
``generation_expired``.

Runs on the real migrated schema: generations 1 (leased), 2 (pruned) and
3 (current) are seeded and pruned through ``prune_snapshot_generations``.
"""

import json
import struct

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from test_lumae_analysis import load_plugin, plugin_api_module, plugin_client  # noqa: F401
from plugins.LumaeAnalysis import catalog


SOURCE = "catalog-a"
P = "plugin_lumae_analysis__"
CURRENT = 3
LEASED = 1
PRUNED = 2


def _vector(*values):
    return struct.pack(f"<{len(values)}f", *values)


def _insert_item(cur, generation, analysis_id, vector, checksum):
    cur.execute(
        f"INSERT INTO {P}analysis_items "
        "(catalog_instance_id, projection_generation, analysis_id, musicnn_fp, "
        " musicnn_vector, musicnn_dimensions) VALUES (%s, %s, %s, %s, %s, %s)",
        (
            SOURCE, generation, analysis_id, checksum,
            psycopg2.Binary(vector) if vector is not None else None,
            len(vector) // 4 if vector else None,
        ),
    )


@pytest.fixture
def client(migrated_db, monkeypatch):
    mod = load_plugin()
    with migrated_db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {P}catalog_sources "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " server_name, is_default, rebind_status) "
            "VALUES (%s, 'server-a', 'navidrome', 'A', TRUE, 'active')",
            (SOURCE,),
        )
        cur.execute(
            f"INSERT INTO {P}catalog_state "
            "(catalog_instance_id, current_core_server_id, provider_type, "
            " published_generation, catalog_epoch, status, entity_counts) "
            "VALUES (%s, 'server-a', 'navidrome', 1, 'catalog-epoch', 'complete', "
            "        '{}'::jsonb)",
            (SOURCE,),
        )
        cur.execute(
            f"INSERT INTO {P}analysis_state "
            "(catalog_instance_id, projection_generation, analysis_epoch, status) "
            "VALUES (%s, %s, 'analysis-epoch', 'complete')",
            (SOURCE, CURRENT),
        )
        for generation in (LEASED, PRUNED):
            _insert_item(cur, generation, "a1", _vector(9.0, 9.0), f"old-{generation}")
        # Current generation: a1 has a vector, a2 has a row but no vector,
        # a3 is absent.
        _insert_item(cur, CURRENT, "a1", _vector(0.5, 1.5), "fp-a1")
        _insert_item(cur, CURRENT, "a2", None, None)
        cur.execute(
            f"INSERT INTO {P}stream_bootstrap_sessions "
            "(token_hash, stream, catalog_instance_id, principal_key, "
            " pinned_generation, snapshot_epoch, snapshot_seq, expires_at) "
            "VALUES ('lease', 'analysis', %s, 'user', %s, 'analysis-epoch', 0, "
            "        now() + interval '1 hour')",
            (SOURCE, LEASED),
        )
        catalog.prune_snapshot_generations(cur, SOURCE, "analysis", CURRENT)
        cur.execute(
            f"SELECT projection_generation FROM {P}analysis_items "
            "WHERE catalog_instance_id=%s ORDER BY 1",
            (SOURCE,),
        )
        assert sorted({row[0] for row in cur.fetchall()}) == [LEASED, CURRENT]
    migrated_db.commit()
    monkeypatch.setattr(mod, "get_db", lambda: migrated_db)
    return plugin_client(mod)


def _post(client, **body):
    return client.post(
        "/api/catalog/analysis/vectors",
        json={"catalog_instance_id": SOURCE, "family": "musicnn", **body},
    )


def _decode(response):
    assert response.status_code == 200, response.data
    assert response.mimetype == "application/vnd.lumae.f32le-v1"
    raw = response.data
    (length,) = struct.unpack("<I", raw[:4])
    header_bytes = raw[4:4 + length]
    header = json.loads(header_bytes)
    # Header stays canonical: sorted keys, compact.
    assert header_bytes == catalog.canonical_json(header).encode("utf-8")
    return header, raw[4 + length:]


def test_pruned_generation_is_served_from_current_with_checksums(client):
    header, body = _decode(_post(client, analysis_ids=["a1"], generation=PRUNED))

    assert header["generation"] == CURRENT
    assert header["requested_generation"] == PRUNED
    assert header["missing"] == []
    assert header["vectors"] == [
        {"analysis_id": "a1", "byte_length": 8, "checksum": "fp-a1",
         "dimensions": 2, "offset": 0},
    ]
    assert struct.unpack("<2f", body) == (0.5, 1.5)


def test_missing_lists_both_reasons_and_page_is_never_silently_empty(client):
    header, body = _decode(_post(client, analysis_ids=["a3", "a2", "a1"]))

    assert header["generation"] == CURRENT
    assert "requested_generation" not in header
    assert [v["analysis_id"] for v in header["vectors"]] == ["a1"]
    assert header["missing"] == [
        {"analysis_id": "a3", "reason": "not_in_generation"},
        {"analysis_id": "a2", "reason": "no_vector"},
    ]
    assert len(body) == 8

    header, body = _decode(_post(client, analysis_ids=["a2", "a3"], generation=PRUNED))
    assert header["vectors"] == [] and body == b""
    assert header["requested_generation"] == PRUNED
    assert {m["analysis_id"] for m in header["missing"]} == {"a2", "a3"}


def test_leased_generation_is_served_exactly(client):
    header, body = _decode(_post(client, analysis_ids=["a1", "a2"], generation=LEASED))

    assert header["generation"] == LEASED
    assert "requested_generation" not in header
    assert header["vectors"][0]["checksum"] == f"old-{LEASED}"
    assert header["missing"] == [{"analysis_id": "a2", "reason": "not_in_generation"}]
    assert struct.unpack("<2f", body) == (9.0, 9.0)


@pytest.mark.parametrize("generation", [CURRENT + 1, -1])
def test_future_or_negative_generation_is_400(client, generation):
    response = _post(client, analysis_ids=["a1"], generation=generation)

    assert response.status_code == 400
    assert response.get_json()["error"] == "invalid_batch"


def test_strict_generation_returns_410_for_pruned_generation(client):
    response = _post(
        client, analysis_ids=["a1"], generation=PRUNED, strict_generation=True
    )

    assert response.status_code == 410
    body = response.get_json()
    assert body["error"] == "generation_expired"
    assert body["current_generation"] == CURRENT
    assert body["requested_generation"] == PRUNED

    # A leased or current generation is unaffected by strict mode.
    for generation in (LEASED, CURRENT):
        header, _ = _decode(_post(
            client, analysis_ids=["a1"], generation=generation, strict_generation=True
        ))
        assert header["generation"] == generation
