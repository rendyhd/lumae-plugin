"""Saved Vibes: one compact record per Vibe, per principal and catalogue.

A Vibe is a small document the app owns (a name and a recipe). The plugin
stores it as sent and never interprets the recipe, so a new recipe field never
needs a plugin release. Every record carries a revision: a write names the
revision it was based on and a stale one is refused with the current record,
so two devices never overwrite each other silently. Deletes are tombstones
kept forever, so every device learns about them.

Like shelves, a scope row lock serializes writes and receipts in the same
transaction, and the sequence on each latest record is a compact change feed.
"""
import hashlib
import json
import math
from flask import jsonify, request
from plugin.api import get_db, table
from . import migrations
from .collection_manager import current_principal, require_collections_enabled

VIBES_SCHEMA_VERSION = 1
MAX_VIBE_BYTES = 65_536
MAX_REQUEST_BYTES = 2 * MAX_VIBE_BYTES
# The id prefix each kind's local table uses in the app.
VIBE_KIND_PREFIXES = {"palette": "palette:", "compass": "compass_preset:", "dna": "dna_vibe:"}
# Receipts are kept like shelf receipts: a retry with the same mutation id
# after this age is no longer recognised and is checked by revision instead.
RECEIPT_RETENTION_DAYS = 30
RETENTION_BATCH_ROWS = 5_000
RETENTION_MAX_BATCHES = 20


def migrate_vibes(db):
    with db.cursor() as cur:
        cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {table('vibe_sequence')}")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('vibe_scopes')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL,
            PRIMARY KEY (principal, catalog_id))""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('vibe_records')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            revision BIGINT NOT NULL, value JSONB NOT NULL, seq BIGINT NOT NULL,
            PRIMARY KEY (principal, catalog_id, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_vibe_changes_idx
            ON {table('vibe_records')} (principal,catalog_id,seq)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('vibe_mutations')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            response JSONB NOT NULL, request_fingerprint TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (principal, catalog_id, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_vibe_mutations_created_idx
            ON {table('vibe_mutations')} (created_at)""")


def capability(enabled, scope):
    return {
        "schema_version": VIBES_SCHEMA_VERSION,
        "enabled": enabled,
        "scope": scope,
        "max_vibe_bytes": MAX_VIBE_BYTES,
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


def validate_mutation(body):
    if not isinstance(body, dict):
        raise ValueError("Expected a mutation object")
    _text(body.get("id"), "mutation id", 200)
    vibe_id = _text(body.get("vibeId"), "vibe id")
    if not any(vibe_id.startswith(prefix) for prefix in VIBE_KIND_PREFIXES.values()):
        raise ValueError("Invalid vibe id")
    if type(body.get("baseRevision")) is not int or body["baseRevision"] < 0:
        raise ValueError("Invalid revision")
    operation = body.get("operation")
    if operation == "put":
        vibe = body.get("vibe")
        if not isinstance(vibe, dict) or vibe.get("kind") not in VIBE_KIND_PREFIXES:
            raise ValueError("Invalid vibe")
        if not vibe_id.startswith(VIBE_KIND_PREFIXES[vibe["kind"]]):
            raise ValueError("Vibe id does not match its kind")
        _text(vibe.get("name"), "vibe name", 500)
        if vibe.get("createdAt") is not None:
            _text(vibe["createdAt"], "created time", 64)
        if not isinstance(vibe.get("recipe"), dict):
            raise ValueError("Invalid recipe")
        if len(_canonical(vibe).encode("utf-8")) > MAX_VIBE_BYTES:
            raise ValueError("Vibe is too large")
    elif operation == "delete":
        _timestamp(body.get("at"))
    else:
        raise ValueError("Invalid operation")
    return body


def request_fingerprint(body):
    """SHA-256 of the canonical mutation body; the receipt key is its ``id``."""
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def _lock_scopes(cur, scopes):
    """Lock vibe scopes in (principal, catalogue) order, creating missing rows.

    Every seq is allocated under its scope's lock, held to commit, so within a
    scope seqs commit in order and a reader paging by ``seq > cursor`` never
    skips a record that commits late.
    """
    for scope in sorted(set(scopes)):
        cur.execute(f"INSERT INTO {table('vibe_scopes')} (principal,catalog_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", scope)
        cur.execute(f"SELECT principal FROM {table('vibe_scopes')} WHERE principal=%s AND catalog_id=%s FOR UPDATE", scope)


def _record(identifier, revision, value):
    return {"id": identifier, "revision": revision, "deletedAt": value["deletedAt"], "vibe": value["vibe"]}


def apply_mutation(cur, scope, body):
    """Called with the scope locked. No commit here: data and receipt are atomic."""
    identifier = body["vibeId"]
    cur.execute(f"SELECT revision,value FROM {table('vibe_records')} WHERE principal=%s AND catalog_id=%s AND id=%s",
                (*scope, identifier))
    row = cur.fetchone()
    current = row[0] if row else 0
    if current != body["baseRevision"]:
        return {"error": "vibe_conflict", "record": _record(identifier, *row) if row else None}, 409
    if body["operation"] == "put":
        value = {"deletedAt": None, "vibe": body["vibe"]}
    else:
        value = {"deletedAt": body["at"], "vibe": None}
    revision = current + 1
    cur.execute(f"""INSERT INTO {table('vibe_records')} (principal,catalog_id,id,revision,value,seq)
        VALUES (%s,%s,%s,%s,%s::jsonb,nextval(%s))
        ON CONFLICT(principal,catalog_id,id) DO UPDATE
        SET revision=excluded.revision,value=excluded.value,seq=excluded.seq""",
        (*scope, identifier, revision, json.dumps(value), table("vibe_sequence")))
    return {"record": _record(identifier, revision, value)}, 200


def rekey_vibes(cur, catalog_id, mapping):
    """Rewrite exact provider ids inside stored Vibes (palette song seeds).

    A rewrite allocates a new seq, so every device receives the rewritten copy,
    but keeps the revision: the Vibe did not change, only the ids naming its
    songs, so a device's next write based on that revision still applies.
    Locks only the scopes it rewrites, like ``rekey_shelves``.
    """
    from .provider_identity_rekey import _replace_exact
    tables = (
        ("vibe_records", ("principal", "catalog_id", "id"), "value"),
        ("vibe_mutations", ("principal", "catalog_id", "id"), "response"),
    )
    affected = set()
    for name, _, payload_column in tables:
        cur.execute(f"SELECT principal,{payload_column} FROM {table(name)} WHERE catalog_id=%s", (catalog_id,))
        affected.update(row[0] for row in cur.fetchall() if _replace_exact(row[1], mapping) != row[1])
    if not affected:
        return
    _lock_scopes(cur, [(principal, catalog_id) for principal in affected])
    for name, key_columns, payload_column in tables:
        # Read again under the locks: a mutation may have committed meanwhile.
        cur.execute(f"SELECT {','.join(key_columns)},{payload_column} FROM {table(name)} "
                    "WHERE catalog_id=%s AND principal=ANY(%s)", (catalog_id, sorted(affected)))
        for row in cur.fetchall():
            rewritten = _replace_exact(row[-1], mapping)
            if rewritten == row[-1]:
                continue
            sequence = f",seq=nextval('{table('vibe_sequence')}')" if name == "vibe_records" else ""
            cur.execute(f"UPDATE {table(name)} SET {payload_column}=%s::jsonb{sequence} WHERE " +
                        " AND ".join(f"{key}=%s" for key in key_columns),
                        (json.dumps(rewritten), *row[:-1]))


def purge_expired_vibe_mutations(db, batch_rows=RETENTION_BATCH_ROWS, max_batches=RETENTION_MAX_BATCHES):
    """Delete Vibe receipts older than ``RECEIPT_RETENTION_DAYS``, oldest first,
    in bounded batches, each under its scopes' locks (the lock a mutation takes
    before it reads its receipt), as ``purge_expired_shelf_mutations`` does."""
    deleted = 0
    relation = table("vibe_mutations")
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


def register_vibe_routes(bp):
    @bp.get("/api/vibes/changes")
    @require_collections_enabled
    def vibe_changes():
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
            cur.execute(f"""SELECT id,revision,value,seq FROM {table('vibe_records')}
                WHERE principal=%s AND catalog_id=%s AND seq>%s ORDER BY seq LIMIT %s""", (*scope, cursor, limit + 1))
            rows = cur.fetchall()
        page = rows[:limit]
        response = jsonify(records=[_record(r[0], r[1], r[2]) for r in page],
                           cursor=page[-1][3] if page else cursor, hasMore=len(rows) > limit)
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.post("/api/vibes/mutations")
    @require_collections_enabled
    def vibe_mutation():
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
                cur.execute(f"SELECT response,request_fingerprint FROM {table('vibe_mutations')} WHERE principal=%s AND catalog_id=%s AND id=%s", (*scope, body["id"]))
                receipt = cur.fetchone()
                if receipt and receipt[1] != fingerprint:
                    # This mutation id already acknowledged another body.
                    payload, status = {"error": "idempotency_key_conflict"}, 409
                elif receipt:
                    payload, status = receipt[0], 200
                else:
                    payload, status = apply_mutation(cur, scope, body)
                    if status == 200:
                        cur.execute(f"INSERT INTO {table('vibe_mutations')} (principal,catalog_id,id,response,request_fingerprint,created_at) VALUES (%s,%s,%s,%s::jsonb,%s,now())", (*scope, body["id"], json.dumps(payload), fingerprint))
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(payload), status
