"""Custom covers: one compact record per cover target, plus the photos they use.

The Lumae app lets a person choose the cover of a saved Vibe, a Living
Collection or a Navidrome playlist: an album from inside it, an orb colour, or
a photo. Each choice is one record keyed by its target (``vibe:<vibe id>``,
``collection:<id>``, ``playlist:<Navidrome playlist id>``), versioned and
synced exactly like saved Vibes (``vibes.py``): a write names the revision it
was based on, a stale one is refused with the current record, and a delete is
a tombstone meaning "back to the automatic cover".

Photos live in a small content-addressed image store in the same scope. The id
is the SHA-256 of the bytes, so an upload is idempotent, and a cover may name
a photo only once it is stored. An image no live cover uses is deleted after a
grace period that covers the upload-then-write sequence.

Covers live apart from the Vibe record on purpose: Lumae Radio rewrites Vibes
with only their recipe, collections drop unknown fields, and the plugin keeps
no playlist store.
"""
import hashlib
import json
import math
import re
from flask import Response, jsonify, request
from plugin.api import get_db, table
from . import migrations
from .collection_manager import current_principal, require_collections_enabled

COVERS_SCHEMA_VERSION = 1
MAX_COVER_BYTES = 4_096
MAX_REQUEST_BYTES = 4 * MAX_COVER_BYTES
MAX_IMAGE_BYTES = 524_288
MAX_IMAGES_PER_SCOPE = 2_000
IMAGE_TYPES = ("image/jpeg", "image/png", "image/webp")
TARGET_KINDS = ("vibe", "collection", "playlist")
VIBE_ID_PREFIXES = ("palette:", "compass_preset:", "dna_vibe:")
RECEIPT_RETENTION_DAYS = 30
# An image is stored before the cover that names it is written; one nobody
# uses is kept this long first.
UNUSED_IMAGE_GRACE_DAYS = 2
RETENTION_BATCH_ROWS = 5_000
RETENTION_MAX_BATCHES = 20
IMAGE_CACHE_CONTROL = "private, max-age=31536000, immutable"

_COLOR = re.compile(r"^#[0-9A-Fa-f]{6}$")
_IMAGE_ID = re.compile(r"^[0-9a-f]{64}$")


class CoverImageError(ValueError):
    """A refused image upload, with its HTTP status."""

    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def migrate_covers(db):
    with db.cursor() as cur:
        cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {table('cover_sequence')}")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('cover_scopes')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL,
            PRIMARY KEY (principal, catalog_id))""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('cover_records')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            revision BIGINT NOT NULL, value JSONB NOT NULL, seq BIGINT NOT NULL,
            PRIMARY KEY (principal, catalog_id, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_cover_changes_idx
            ON {table('cover_records')} (principal,catalog_id,seq)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('cover_mutations')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            response JSONB NOT NULL, request_fingerprint TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (principal, catalog_id, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_cover_mutations_created_idx
            ON {table('cover_mutations')} (created_at)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('cover_images')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            content_type TEXT NOT NULL, data BYTEA NOT NULL, bytes INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (principal, catalog_id, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_cover_images_created_idx
            ON {table('cover_images')} (created_at)""")


def capability(enabled, scope):
    return {
        "schema_version": COVERS_SCHEMA_VERSION,
        "enabled": enabled,
        "scope": scope,
        "max_cover_bytes": MAX_COVER_BYTES,
        "max_image_bytes": MAX_IMAGE_BYTES,
        "image_types": list(IMAGE_TYPES),
    }


def _text(value, name, maximum=512):
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"Invalid {name}")
    return value


def _timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError("Invalid timestamp")
    return value


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def validate_cover_id(value):
    """``vibe:<vibe id>``, ``collection:<id>`` or ``playlist:<id>``."""
    _text(value, "cover id", 600)
    kind, _, target = value.partition(":")
    if kind not in TARGET_KINDS or not target:
        raise ValueError("Invalid cover id")
    if kind == "vibe" and not any(target.startswith(p) and len(target) > len(p) for p in VIBE_ID_PREFIXES):
        raise ValueError("Invalid cover id")
    return value


def _dimension(value):
    if type(value) is not int or not 1 <= value <= 20_000:
        raise ValueError("Invalid image size")


def validate_cover(cover):
    """Checks the known cover types; any other type is stored as sent."""
    if not isinstance(cover, dict):
        raise ValueError("Invalid cover")
    kind = _text(cover.get("type"), "cover type", 32)
    if kind == "album":
        _text(cover.get("itemId"), "cover item id", 256)
    elif kind == "color":
        if not isinstance(cover.get("color"), str) or not _COLOR.match(cover["color"]):
            raise ValueError("Invalid cover colour")
    elif kind == "photo":
        if not isinstance(cover.get("imageId"), str) or not _IMAGE_ID.match(cover["imageId"]):
            raise ValueError("Invalid cover image id")
        for name in ("width", "height"):
            if cover.get(name) is not None:
                _dimension(cover[name])
    if len(_canonical(cover).encode("utf-8")) > MAX_COVER_BYTES:
        raise ValueError("Cover is too large")
    return cover


def validate_mutation(body):
    if not isinstance(body, dict):
        raise ValueError("Expected a mutation object")
    _text(body.get("id"), "mutation id", 200)
    validate_cover_id(body.get("coverId"))
    if type(body.get("baseRevision")) is not int or body["baseRevision"] < 0:
        raise ValueError("Invalid revision")
    operation = body.get("operation")
    if operation == "put":
        validate_cover(body.get("cover"))
    elif operation == "delete":
        _timestamp(body.get("at"))
    else:
        raise ValueError("Invalid operation")
    return body


def request_fingerprint(body):
    """SHA-256 of the canonical mutation body; the receipt key is its ``id``."""
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def sniff_image_type(data):
    """The image type the bytes really are, or None."""
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def _lock_scopes(cur, scopes):
    """Lock cover scopes in (principal, catalogue) order, creating missing rows.

    Seqs are allocated under the lock, as for Vibes, and image retention takes
    the same lock, so a cover never names a photo deleted under it.
    """
    for scope in sorted(set(scopes)):
        cur.execute(f"INSERT INTO {table('cover_scopes')} (principal,catalog_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", scope)
        cur.execute(f"SELECT principal FROM {table('cover_scopes')} WHERE principal=%s AND catalog_id=%s FOR UPDATE", scope)


def _record(identifier, revision, value):
    return {"id": identifier, "revision": revision, "deletedAt": value["deletedAt"], "cover": value["cover"]}


def apply_mutation(cur, scope, body):
    """Called with the scope locked. No commit here: data and receipt are atomic."""
    identifier = body["coverId"]
    cur.execute(f"SELECT revision,value FROM {table('cover_records')} WHERE principal=%s AND catalog_id=%s AND id=%s",
                (*scope, identifier))
    row = cur.fetchone()
    current = row[0] if row else 0
    if current != body["baseRevision"]:
        return {"error": "cover_conflict", "record": _record(identifier, *row) if row else None}, 409
    if body["operation"] == "put":
        cover = body["cover"]
        if cover.get("type") == "photo":
            cur.execute(f"SELECT 1 FROM {table('cover_images')} WHERE principal=%s AND catalog_id=%s AND id=%s",
                        (*scope, cover["imageId"]))
            if cur.fetchone() is None:
                return {"error": "cover_image_missing", "imageId": cover["imageId"]}, 409
        value = {"deletedAt": None, "cover": cover}
    else:
        value = {"deletedAt": body["at"], "cover": None}
    revision = current + 1
    cur.execute(f"""INSERT INTO {table('cover_records')} (principal,catalog_id,id,revision,value,seq)
        VALUES (%s,%s,%s,%s,%s::jsonb,nextval(%s))
        ON CONFLICT(principal,catalog_id,id) DO UPDATE
        SET revision=excluded.revision,value=excluded.value,seq=excluded.seq""",
        (*scope, identifier, revision, json.dumps(value), table("cover_sequence")))
    return {"record": _record(identifier, revision, value)}, 200


def store_image(cur, scope, data, declared_type):
    """Store an image (idempotent by content); ``{imageId, contentType, bytes}``."""
    if not data:
        raise CoverImageError("Empty image")
    if len(data) > MAX_IMAGE_BYTES:
        raise CoverImageError("Image is too large", 413)
    actual = sniff_image_type(data)
    if actual is None or actual not in IMAGE_TYPES or actual != declared_type:
        raise CoverImageError("Unsupported image", 415)
    image_id = hashlib.sha256(data).hexdigest()
    _lock_scopes(cur, [scope])
    # Storing an image again restarts its grace period.
    cur.execute(f"UPDATE {table('cover_images')} SET created_at=now() WHERE principal=%s AND catalog_id=%s AND id=%s",
                (*scope, image_id))
    if cur.rowcount == 0:
        cur.execute(f"SELECT count(*) FROM {table('cover_images')} WHERE principal=%s AND catalog_id=%s", scope)
        if cur.fetchone()[0] >= MAX_IMAGES_PER_SCOPE:
            raise CoverImageError("cover_image_quota", 409)
        cur.execute(f"""INSERT INTO {table('cover_images')} (principal,catalog_id,id,content_type,data,bytes,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,now())""", (*scope, image_id, actual, data, len(data)))
    return {"imageId": image_id, "contentType": actual, "bytes": len(data)}


def rekey_covers(cur, catalog_id, mapping):
    """Rewrite exact provider ids inside stored covers (an album cover's item id).

    Like ``rekey_vibes``: a new seq so every device receives it, the same
    revision because the person's choice did not change.
    """
    from .provider_identity_rekey import _replace_exact
    tables = (
        ("cover_records", ("principal", "catalog_id", "id"), "value"),
        ("cover_mutations", ("principal", "catalog_id", "id"), "response"),
    )
    affected = set()
    for name, _, payload_column in tables:
        cur.execute(f"SELECT principal,{payload_column} FROM {table(name)} WHERE catalog_id=%s", (catalog_id,))
        affected.update(row[0] for row in cur.fetchall() if _replace_exact(row[1], mapping) != row[1])
    if not affected:
        return
    _lock_scopes(cur, [(principal, catalog_id) for principal in affected])
    for name, key_columns, payload_column in tables:
        cur.execute(f"SELECT {','.join(key_columns)},{payload_column} FROM {table(name)} "
                    "WHERE catalog_id=%s AND principal=ANY(%s)", (catalog_id, sorted(affected)))
        for row in cur.fetchall():
            rewritten = _replace_exact(row[-1], mapping)
            if rewritten == row[-1]:
                continue
            sequence = f",seq=nextval('{table('cover_sequence')}')" if name == "cover_records" else ""
            cur.execute(f"UPDATE {table(name)} SET {payload_column}=%s::jsonb{sequence} WHERE " +
                        " AND ".join(f"{key}=%s" for key in key_columns),
                        (json.dumps(rewritten), *row[:-1]))


def purge_expired_cover_mutations(db, batch_rows=RETENTION_BATCH_ROWS, max_batches=RETENTION_MAX_BATCHES):
    """Delete cover receipts older than ``RECEIPT_RETENTION_DAYS`` in bounded
    batches under their scopes' locks, as ``purge_expired_vibe_mutations``."""
    deleted = 0
    relation = table("cover_mutations")
    for _ in range(max_batches):
        with db.cursor() as cur:
            cur.execute(
                f"SELECT principal, catalog_id, id, ctid::text FROM {relation} "
                "WHERE created_at < now() - %s::interval "
                "ORDER BY created_at LIMIT %s",
                (f"{RECEIPT_RETENTION_DAYS} days", batch_rows),
            )
            rows = cur.fetchall()
            if not rows:
                break
            _lock_scopes(cur, [(principal, catalog_id) for principal, catalog_id, _, _ in rows])
            cur.execute(
                f"DELETE FROM {relation} WHERE ctid = ANY(%s::text[]::tid[]) "
                "AND created_at < now() - %s::interval",
                ([row[3] for row in rows], f"{RECEIPT_RETENTION_DAYS} days"),
            )
            deleted += cur.rowcount
        db.commit()
        if len(rows) < batch_rows:
            break
    return deleted


_UNUSED_IMAGE = f"""i.created_at < now() - %s::interval AND NOT EXISTS (
    SELECT 1 FROM {{records}} r WHERE r.principal=i.principal AND r.catalog_id=i.catalog_id
      AND r.value->'cover'->>'type'='photo' AND r.value->'cover'->>'imageId'=i.id)"""


def purge_unused_cover_images(db, batch_rows=RETENTION_BATCH_ROWS, max_batches=RETENTION_MAX_BATCHES):
    """Delete images no live cover in their scope names, once past the grace
    period. Rechecked under the scope lock a cover write takes."""
    deleted = 0
    images = table("cover_images")
    unused = _UNUSED_IMAGE.format(records=table("cover_records"))
    grace = f"{UNUSED_IMAGE_GRACE_DAYS} days"
    for _ in range(max_batches):
        with db.cursor() as cur:
            cur.execute(f"SELECT i.principal, i.catalog_id, i.id FROM {images} i WHERE {unused} "
                        "ORDER BY i.created_at LIMIT %s", (grace, batch_rows))
            rows = cur.fetchall()
            if not rows:
                break
            _lock_scopes(cur, [(principal, catalog_id) for principal, catalog_id, _ in rows])
            for principal, catalog_id, image_id in rows:
                cur.execute(f"DELETE FROM {images} i WHERE i.principal=%s AND i.catalog_id=%s AND i.id=%s AND {unused}",
                            (principal, catalog_id, image_id, grace))
                deleted += cur.rowcount
        db.commit()
        if len(rows) < batch_rows:
            break
    return deleted


def _read_body(max_bytes):
    """The raw body, reading at most ``max_bytes`` + 1 bytes (chunked bodies too)."""
    if request.content_length is not None and request.content_length > max_bytes:
        raise CoverImageError("Image is too large", 413)
    raw = getattr(request, "_cached_data", None)
    if raw is None:
        chunks, size = [], 0
        while size <= max_bytes:
            chunk = request.stream.read(max_bytes + 1 - size)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
        raw = b"".join(chunks)
    if len(raw) > max_bytes:
        raise CoverImageError("Image is too large", 413)
    return raw


def register_cover_routes(bp):
    @bp.get("/api/covers/changes")
    @require_collections_enabled
    def cover_changes():
        try:
            catalog = _text(request.args.get("catalog_id"), "catalogue identity")
            cursor = int(request.args.get("cursor", 0))
            limit = max(1, min(500, int(request.args.get("limit", 250))))
            if cursor < 0:
                raise ValueError("Invalid cursor")
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), catalog)
        with get_db().cursor() as cur:
            cur.execute(f"""SELECT id,revision,value,seq FROM {table('cover_records')}
                WHERE principal=%s AND catalog_id=%s AND seq>%s ORDER BY seq LIMIT %s""", (*scope, cursor, limit + 1))
            rows = cur.fetchall()
        page = rows[:limit]
        response = jsonify(records=[_record(r[0], r[1], r[2]) for r in page],
                           cursor=page[-1][3] if page else cursor, hasMore=len(rows) > limit)
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.post("/api/covers/mutations")
    @require_collections_enabled
    def cover_mutation():
        try:
            if request.content_length is not None and request.content_length > MAX_REQUEST_BYTES:
                raise ValueError("Request too large")
            catalog = _text(request.args.get("catalog_id"), "catalogue identity")
            body = validate_mutation(request.get_json(silent=True))
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), catalog)
        fingerprint = request_fingerprint(body)
        db = get_db()
        try:
            with db.cursor() as cur:
                _lock_scopes(cur, [scope])
                cur.execute(f"SELECT response,request_fingerprint FROM {table('cover_mutations')} WHERE principal=%s AND catalog_id=%s AND id=%s", (*scope, body["id"]))
                receipt = cur.fetchone()
                if receipt and receipt[1] != fingerprint:
                    payload, status = {"error": "idempotency_key_conflict"}, 409
                elif receipt:
                    payload, status = receipt[0], 200
                else:
                    payload, status = apply_mutation(cur, scope, body)
                    if status == 200:
                        cur.execute(f"INSERT INTO {table('cover_mutations')} (principal,catalog_id,id,response,request_fingerprint,created_at) VALUES (%s,%s,%s,%s::jsonb,%s,now())", (*scope, body["id"], json.dumps(payload), fingerprint))
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(payload), status

    @bp.post("/api/covers/images")
    @require_collections_enabled
    def cover_image_upload():
        try:
            catalog = _text(request.args.get("catalog_id"), "catalogue identity")
            declared = (request.mimetype or "").lower()
            if declared not in IMAGE_TYPES:
                raise CoverImageError("Unsupported image", 415)
            data = _read_body(MAX_IMAGE_BYTES)
        except CoverImageError as error:
            return jsonify(error=str(error)), error.status
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), catalog)
        db = get_db()
        try:
            with db.cursor() as cur:
                stored = store_image(cur, scope, data, declared)
            db.commit()
        except CoverImageError as error:
            db.rollback()
            return jsonify(error=str(error)), error.status
        except Exception:
            db.rollback()
            raise
        return jsonify(stored), 200

    @bp.get("/api/covers/image")
    @require_collections_enabled
    def cover_image():
        try:
            catalog = _text(request.args.get("catalog_id"), "catalogue identity")
            image_id = request.args.get("id")
            if not isinstance(image_id, str) or not _IMAGE_ID.match(image_id):
                raise ValueError("Invalid image id")
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        etag = f'"{image_id}"'
        scope = (current_principal(), catalog)
        with get_db().cursor() as cur:
            cur.execute(f"SELECT content_type,data FROM {table('cover_images')} WHERE principal=%s AND catalog_id=%s AND id=%s",
                        (*scope, image_id))
            row = cur.fetchone()
        if row is None:
            return jsonify(error="cover_image_not_found"), 404
        if request.headers.get("If-None-Match") == etag:
            response = Response(status=304)
        else:
            response = Response(bytes(row[1]), mimetype=row[0])
        response.headers["Cache-Control"] = IMAGE_CACHE_CONTROL
        response.headers["ETag"] = etag
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response
