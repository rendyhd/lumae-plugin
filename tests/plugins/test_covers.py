"""Custom covers: contract and PostgreSQL transport tests in disposable schemas."""
import hashlib
import importlib
import json

import pytest
from flask import Flask, g
from test_lumae_analysis import load_plugin, lumae_postgres_db  # noqa: F401

JPEG = b"\xff\xd8\xff\xe0" + b"jpeg-bytes" * 20
PNG = b"\x89PNG\r\n\x1a\n" + b"png-bytes" * 20
WEBP = b"RIFF\x10\x00\x00\x00WEBPVP8 " + b"webp" * 20


def cover_module():
    load_plugin()
    return importlib.import_module("plugins.LumaeAnalysis.covers")


def put(mutation_id, cover_id="collection:c1", base=0, cover=None):
    return {"id": mutation_id, "operation": "put", "coverId": cover_id, "baseRevision": base,
            "cover": cover if cover is not None else {"type": "album", "itemId": "al-1"}}


def delete(mutation_id, cover_id="collection:c1", base=1, at=1_759_500_000_000):
    return {"id": mutation_id, "operation": "delete", "coverId": cover_id, "baseRevision": base, "at": at}


@pytest.fixture
def covers(lumae_postgres_db, monkeypatch):
    mod = cover_module()
    mod.migrate_covers(lumae_postgres_db)
    lumae_postgres_db.commit()
    monkeypatch.setattr(mod, "get_db", lambda: lumae_postgres_db)
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    monkeypatch.setattr(manager, "collections_enabled", lambda: True)
    app = Flask(__name__)
    app.register_blueprint(load_plugin().bp)

    @app.before_request
    def auth():
        from flask import request
        g.auth_method = request.headers.get("Test-Auth", "session")
        g.auth_user = request.headers.get("Test-User", "alice")

    return app.test_client()


def post(client, body, catalog="catalog-a", user="alice"):
    return client.post(f"/api/covers/mutations?catalog_id={catalog}", json=body, headers={"Test-User": user})


def changes(client, catalog="catalog-a", user="alice", cursor=0, limit=250):
    return client.get(f"/api/covers/changes?catalog_id={catalog}&cursor={cursor}&limit={limit}",
                      headers={"Test-User": user}).get_json()


def upload(client, data=JPEG, content_type="image/jpeg", catalog="catalog-a", user="alice"):
    return client.post(f"/api/covers/images?catalog_id={catalog}", data=data, content_type=content_type,
                       headers={"Test-User": user})


def image(client, image_id, catalog="catalog-a", user="alice", headers=None):
    return client.get(f"/api/covers/image?catalog_id={catalog}&id={image_id}",
                      headers={"Test-User": user, **(headers or {})})


def photo(image_id, **extra):
    return {"type": "photo", "imageId": image_id, **extra}


def test_validation():
    mod = cover_module()
    validate = mod.validate_mutation
    assert validate(put("m"))["operation"] == "put"
    assert validate(put("m", "vibe:palette:chill", cover={"type": "color", "color": "#C08A5A"}))
    assert validate(put("m", "playlist:nd-9", cover=photo("a" * 64, width=1024, height=1024)))
    assert validate(put("m", "vibe:dna_vibe:calm", cover={"type": "gradient", "stops": ["#000000"]}))
    assert validate(delete("m", "vibe:compass_preset:rain"))["operation"] == "delete"
    bad_bodies = [
        None, {}, put(""), put("m", "album:a1"), put("m", "collection:"), put("m", "vibe:shelf:1"),
        put("m", "vibe:palette:"), {**put("m"), "baseRevision": True}, {**put("m"), "baseRevision": -1},
        {**put("m"), "operation": "patch"}, put("m", cover=[]), put("m", cover={"type": ""}),
        put("m", cover={"type": "album"}), put("m", cover={"type": "color", "color": "bronze"}),
        put("m", cover=photo("A" * 64)), put("m", cover=photo("a" * 63)),
        put("m", cover=photo("a" * 64, width=0)), put("m", cover=photo("a" * 64, height=True)),
        put("m", cover={"type": "album", "itemId": "x", "note": "n" * 5_000}),
        {**delete("m"), "at": float("nan")}, {k: v for k, v in delete("m").items() if k != "at"},
    ]
    for body in bad_bodies:
        with pytest.raises(ValueError):
            validate(body)


def test_sniffing_matches_the_declared_type():
    mod = cover_module()
    assert mod.sniff_image_type(JPEG) == "image/jpeg"
    assert mod.sniff_image_type(PNG) == "image/png"
    assert mod.sniff_image_type(WEBP) == "image/webp"
    assert mod.sniff_image_type(b"GIF89a....") is None
    assert mod.sniff_image_type(b"") is None


def test_revisions_conflicts_tombstones_and_restore(covers):
    created = post(covers, put("create"))
    assert created.status_code == 200
    record = created.get_json()["record"]
    assert record == {"id": "collection:c1", "revision": 1, "deletedAt": None,
                      "cover": {"type": "album", "itemId": "al-1"}}

    stale = post(covers, put("other-device", cover={"type": "album", "itemId": "al-2"}))
    assert stale.status_code == 409
    assert stale.get_json() == {"error": "cover_conflict", "record": record}

    edited = post(covers, put("edit", base=1, cover={"type": "album", "itemId": "al-2"})).get_json()["record"]
    assert edited["revision"] == 2 and edited["cover"]["itemId"] == "al-2"

    # Back to the automatic cover.
    removed = post(covers, delete("auto", base=2)).get_json()["record"]
    assert removed == {"id": "collection:c1", "revision": 3, "deletedAt": 1_759_500_000_000, "cover": None}

    restored = post(covers, put("again", base=3)).get_json()["record"]
    assert restored["revision"] == 4 and restored["deletedAt"] is None


def test_receipts_replay_and_bind_their_body(covers):
    first = post(covers, put("create"))
    replay = post(covers, put("create"))
    assert replay.status_code == 200 and replay.get_json() == first.get_json()
    reused = post(covers, put("create", cover={"type": "color", "color": "#000000"}))
    assert reused.status_code == 409 and reused.get_json() == {"error": "idempotency_key_conflict"}


def test_records_are_scoped_by_principal_and_catalogue(covers):
    post(covers, put("a"))
    assert len(changes(covers)["records"]) == 1
    assert changes(covers, user="bob")["records"] == []
    assert changes(covers, catalog="catalog-b")["records"] == []


def test_feed_is_compact_and_pages_by_cursor(covers):
    post(covers, put("one", "playlist:p1"))
    post(covers, put("two", "vibe:palette:two", cover={"type": "color", "color": "#112233"}))
    post(covers, put("one-again", "playlist:p1", base=1, cover={"type": "album", "itemId": "al-9"}))
    feed = changes(covers)
    assert [r["id"] for r in feed["records"]] == ["vibe:palette:two", "playlist:p1"]
    page1 = changes(covers, limit=1)
    assert page1["hasMore"] is True
    page2 = changes(covers, cursor=page1["cursor"], limit=1)
    assert page2["hasMore"] is False and [r["id"] for r in page2["records"]] == ["playlist:p1"]


def test_unknown_cover_types_are_stored_as_sent(covers):
    cover = {"type": "gradient", "stops": ["#112233", "#445566"], "angle": 45, "label": "夜 ✨"}
    post(covers, put("future", "vibe:dna_vibe:night", cover=cover))
    assert changes(covers)["records"][0]["cover"] == cover


def test_image_upload_is_idempotent_and_served_with_a_long_cache(covers):
    first = upload(covers)
    assert first.status_code == 200
    stored = first.get_json()
    assert stored == {"imageId": hashlib.sha256(JPEG).hexdigest(), "contentType": "image/jpeg", "bytes": len(JPEG)}
    assert upload(covers).get_json() == stored

    served = image(covers, stored["imageId"])
    assert served.status_code == 200 and served.data == JPEG
    assert served.mimetype == "image/jpeg"
    assert served.headers["Cache-Control"] == "private, max-age=31536000, immutable"
    assert served.headers["ETag"] == f'"{stored["imageId"]}"'
    assert served.headers["X-Content-Type-Options"] == "nosniff"
    assert image(covers, stored["imageId"], headers={"If-None-Match": served.headers["ETag"]}).status_code == 304

    # Images are scoped like the records that name them.
    assert image(covers, stored["imageId"], user="bob").status_code == 404
    assert image(covers, stored["imageId"], catalog="catalog-b").status_code == 404
    assert image(covers, "nope").status_code == 400

    assert upload(covers, PNG, "image/png").get_json()["contentType"] == "image/png"
    assert upload(covers, WEBP, "image/webp").get_json()["contentType"] == "image/webp"


def test_images_are_refused_when_wrong_or_too_large(covers):
    mod = cover_module()
    assert upload(covers, PNG, "image/jpeg").status_code == 415
    assert upload(covers, b"GIF89a" + b"x" * 20, "image/gif").status_code == 415
    assert upload(covers, JPEG, "application/octet-stream").status_code == 415
    assert upload(covers, b"", "image/jpeg").status_code == 400
    too_large = b"\xff\xd8\xff" + b"x" * mod.MAX_IMAGE_BYTES
    assert upload(covers, too_large).status_code == 413


def test_a_photo_cover_needs_its_image_first(covers):
    image_id = hashlib.sha256(JPEG).hexdigest()
    body = put("early", "playlist:p1", cover=photo(image_id, width=1024, height=1024))
    refused = post(covers, body)
    assert refused.status_code == 409
    assert refused.get_json() == {"error": "cover_image_missing", "imageId": image_id}
    # A refused write stores no receipt: the same mutation applies once the image is there.
    assert upload(covers).get_json()["imageId"] == image_id
    accepted = post(covers, body)
    assert accepted.status_code == 200 and accepted.get_json()["record"]["cover"]["imageId"] == image_id


def test_unused_images_are_purged_after_the_grace_period(covers, lumae_postgres_db):
    mod = cover_module()
    used = upload(covers, JPEG).get_json()["imageId"]
    unused = upload(covers, PNG, "image/png").get_json()["imageId"]
    fresh = upload(covers, WEBP, "image/webp").get_json()["imageId"]
    post(covers, put("cover", "collection:c1", cover=photo(used)))
    with lumae_postgres_db.cursor() as cur:
        cur.execute(f"UPDATE {mod.table('cover_images')} SET created_at = now() - interval '3 days' WHERE id <> %s",
                    (fresh,))
    lumae_postgres_db.commit()
    assert mod.purge_unused_cover_images(lumae_postgres_db) == 1
    assert image(covers, unused).status_code == 404
    assert image(covers, used).status_code == 200
    assert image(covers, fresh).status_code == 200

    # Once its cover goes back to Auto, the photo is unused too.
    post(covers, delete("auto", "collection:c1", base=1))
    assert mod.purge_unused_cover_images(lumae_postgres_db) == 1
    assert image(covers, used).status_code == 404


def test_uploading_again_restarts_the_grace_period(covers, lumae_postgres_db):
    mod = cover_module()
    image_id = upload(covers).get_json()["imageId"]
    with lumae_postgres_db.cursor() as cur:
        cur.execute(f"UPDATE {mod.table('cover_images')} SET created_at = now() - interval '3 days'")
    lumae_postgres_db.commit()
    upload(covers)
    assert mod.purge_unused_cover_images(lumae_postgres_db) == 0
    assert image(covers, image_id).status_code == 200


def test_image_quota(covers, monkeypatch):
    mod = cover_module()
    monkeypatch.setattr(mod, "MAX_IMAGES_PER_SCOPE", 1)
    first = upload(covers, JPEG)
    assert first.status_code == 200
    assert upload(covers, JPEG).status_code == 200  # the same image is not a new one
    refused = upload(covers, PNG, "image/png")
    assert refused.status_code == 409 and refused.get_json() == {"error": "cover_image_quota"}
    assert upload(covers, PNG, "image/png", user="bob").status_code == 200


def test_disabled_malformed_session_and_oversized_requests(covers, monkeypatch):
    assert covers.get("/api/covers/changes?catalog_id=catalog-a", headers={"Test-User": ""}).status_code == 401
    assert covers.get("/api/covers/changes").status_code == 400
    oversized = covers.post("/api/covers/mutations?catalog_id=catalog-a",
                            data=json.dumps(put("big", cover={"type": "album", "itemId": "x", "pad": "y" * 20_000})),
                            content_type="application/json")
    assert oversized.status_code == 400
    manager = importlib.import_module("plugins.LumaeAnalysis.collection_manager")
    monkeypatch.setattr(manager, "collections_enabled", lambda: False)
    assert covers.get("/api/covers/changes?catalog_id=catalog-a").status_code == 404
    assert post(covers, put("off")).status_code == 404
    assert upload(covers).status_code == 404
    assert image(covers, "a" * 64).status_code == 404


def test_rekey_rewrites_album_ids_without_a_new_revision(covers, lumae_postgres_db):
    mod = cover_module()
    post(covers, put("a", cover={"type": "album", "itemId": "old-id"}))
    post(covers, put("b", cover={"type": "album", "itemId": "old-id"}), catalog="catalog-b")
    before = changes(covers)["cursor"]
    with lumae_postgres_db.cursor() as cur:
        mod.rekey_covers(cur, "catalog-a", {"old-id": "new-id"})
    lumae_postgres_db.commit()
    changed = changes(covers, cursor=before)["records"]
    assert len(changed) == 1 and changed[0]["revision"] == 1
    assert changed[0]["cover"] == {"type": "album", "itemId": "new-id"}
    assert changes(covers, catalog="catalog-b")["records"][0]["cover"]["itemId"] == "old-id"
    assert post(covers, put("edit", base=1, cover={"type": "color", "color": "#123456"})).status_code == 200


def test_expired_receipts_are_purged_and_recent_ones_kept(covers, lumae_postgres_db):
    mod = cover_module()
    post(covers, put("old", "playlist:old"))
    post(covers, put("recent", "playlist:recent"))
    with lumae_postgres_db.cursor() as cur:
        cur.execute(f"UPDATE {mod.table('cover_mutations')} SET created_at = now() - interval '31 days' WHERE id='old'")
    lumae_postgres_db.commit()
    assert mod.purge_expired_cover_mutations(lumae_postgres_db) == 1
    assert post(covers, put("old", "playlist:old")).status_code == 409


def test_migration_is_idempotent(lumae_postgres_db):
    mod = cover_module()
    mod.migrate_covers(lumae_postgres_db)
    mod.migrate_covers(lumae_postgres_db)
    lumae_postgres_db.commit()
    with lumae_postgres_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname IN "
                    "('lumae_cover_changes_idx','lumae_cover_mutations_created_idx','lumae_cover_images_created_idx')")
        assert cur.fetchone()[0] == 3
