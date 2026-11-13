"""Undertone records: the phone's focus and sleep music details, kept per principal and source.

Undertone is an app inside Lumae with a library of its own (Auralscape
docs/undertone-plan-2026-11-02.md). Its records are small documents the app
owns: fields, categories, presets, groups, the owner's details on tracks and
albums, and a light index of the music so a page can show it without the
music server. The plugin stores each as sent and never interprets it, so a new
field needs no plugin release.

Like Saved Vibes (``vibes.py``): every record carries a revision, a write
names the revision it was based on and a stale one is refused with the current
record (409), deletes are tombstones kept forever, a scope row lock serializes
writes and receipts in the same transaction, and the sequence on each latest
record is a compact change feed. Records are scoped by (principal, source):
the source is the app's fingerprint of the server and library Undertone reads,
not the AudioMuse catalogue, so a provider-identity rekey never touches them.
Audio, server sign-ins, the playing session and the Home button's start times
never come here.
"""
import hashlib
import json
import math
from flask import jsonify, request
from plugin.api import get_db, render_page, table
from . import migrations
from .collection_manager import current_collection_scope, current_principal, require_collections_enabled
from .undertone_ui import render_undertone_page

UNDERTONE_SCHEMA_VERSION = 1
MAX_RECORD_BYTES = 32_768
MAX_BATCH = 200
MAX_REQUEST_BYTES = MAX_BATCH * 4_096 + MAX_RECORD_BYTES * 4
# Each kind's id prefix. A detail is `detail:<track|album>:<owner id>:<field id>`;
# an item is the light index of one track or album: `item:t:<id>` or `item:a:<id>`.
UNDERTONE_KIND_PREFIXES = {
    "field": "field:",
    "category": "category:",
    "preset": "preset:",
    "group": "group:",
    "detail": "detail:",
    "item": "item:",
}
RECEIPT_RETENTION_DAYS = 30
RETENTION_BATCH_ROWS = 5_000
RETENTION_MAX_BATCHES = 20


def migrate_undertone(db):
    with db.cursor() as cur:
        cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {table('undertone_sequence')}")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('undertone_scopes')} (
            principal TEXT NOT NULL, source TEXT NOT NULL,
            PRIMARY KEY (principal, source))""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('undertone_records')} (
            principal TEXT NOT NULL, source TEXT NOT NULL, id TEXT NOT NULL,
            revision BIGINT NOT NULL, value JSONB NOT NULL, seq BIGINT NOT NULL,
            PRIMARY KEY (principal, source, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_undertone_changes_idx
            ON {table('undertone_records')} (principal,source,seq)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('undertone_mutations')} (
            principal TEXT NOT NULL, source TEXT NOT NULL, id TEXT NOT NULL,
            response JSONB NOT NULL, request_fingerprint TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (principal, source, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_undertone_mutations_created_idx
            ON {table('undertone_mutations')} (created_at)""")


def capability(enabled, scope):
    return {
        "schema_version": UNDERTONE_SCHEMA_VERSION,
        "enabled": enabled,
        "scope": scope,
        "max_record_bytes": MAX_RECORD_BYTES,
        "max_batch": MAX_BATCH,
        "kinds": sorted(UNDERTONE_KIND_PREFIXES),
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
    record_id = _text(body.get("recordId"), "record id", 600)
    kind = next((k for k, prefix in UNDERTONE_KIND_PREFIXES.items() if record_id.startswith(prefix)), None)
    if kind is None or len(record_id) == len(UNDERTONE_KIND_PREFIXES[kind]):
        raise ValueError("Invalid record id")
    if type(body.get("baseRevision")) is not int or body["baseRevision"] < 0:
        raise ValueError("Invalid revision")
    operation = body.get("operation")
    if operation == "put":
        record = body.get("record")
        if not isinstance(record, dict) or record.get("kind") != kind:
            raise ValueError("Record does not match its id")
        if not isinstance(record.get("body"), dict):
            raise ValueError("Invalid record body")
        if len(_canonical(record).encode("utf-8")) > MAX_RECORD_BYTES:
            raise ValueError("Record is too large")
    elif operation == "delete":
        _timestamp(body.get("at"))
    else:
        raise ValueError("Invalid operation")
    try:  # NaN or Infinity anywhere would fail in jsonb: a 400, not a 500
        json.dumps(body, allow_nan=False)
    except ValueError:
        raise ValueError("Numbers must be finite") from None
    return body


def request_fingerprint(body):
    """SHA-256 of the canonical mutation body; the receipt key is its ``id``."""
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def _lock_scopes(cur, scopes):
    """Lock scopes in (principal, source) order, creating missing rows, as vibes do."""
    for scope in sorted(set(scopes)):
        cur.execute(f"INSERT INTO {table('undertone_scopes')} (principal,source) VALUES (%s,%s) ON CONFLICT DO NOTHING", scope)
        cur.execute(f"SELECT principal FROM {table('undertone_scopes')} WHERE principal=%s AND source=%s FOR UPDATE", scope)


def _record(identifier, revision, value):
    return {"id": identifier, "revision": revision, "deletedAt": value["deletedAt"], "record": value["record"]}


def apply_mutation(cur, scope, body):
    """Called with the scope locked. No commit here: data and receipt are atomic."""
    identifier = body["recordId"]
    cur.execute(f"SELECT revision,value FROM {table('undertone_records')} WHERE principal=%s AND source=%s AND id=%s",
                (*scope, identifier))
    row = cur.fetchone()
    current = row[0] if row else 0
    if current != body["baseRevision"]:
        return {"error": "undertone_conflict", "record": _record(identifier, *row) if row else None}, 409
    if body["operation"] == "put":
        value = {"deletedAt": None, "record": body["record"]}
    else:
        value = {"deletedAt": body["at"], "record": None}
    revision = current + 1
    cur.execute(f"""INSERT INTO {table('undertone_records')} (principal,source,id,revision,value,seq)
        VALUES (%s,%s,%s,%s,%s::jsonb,nextval(%s))
        ON CONFLICT(principal,source,id) DO UPDATE
        SET revision=excluded.revision,value=excluded.value,seq=excluded.seq""",
        (*scope, identifier, revision, json.dumps(value), table("undertone_sequence")))
    return {"record": _record(identifier, revision, value)}, 200


def _apply_with_receipt(cur, scope, body):
    """One mutation under the held scope lock: its receipt replays, binds its body, or it applies."""
    fingerprint = request_fingerprint(body)
    cur.execute(f"SELECT response,request_fingerprint FROM {table('undertone_mutations')} "
                "WHERE principal=%s AND source=%s AND id=%s", (*scope, body["id"]))
    receipt = cur.fetchone()
    if receipt and receipt[1] != fingerprint:
        return {"error": "idempotency_key_conflict"}, 409
    if receipt:
        return receipt[0], 200
    payload, status = apply_mutation(cur, scope, body)
    if status == 200:
        cur.execute(f"INSERT INTO {table('undertone_mutations')} (principal,source,id,response,request_fingerprint,created_at) "
                    "VALUES (%s,%s,%s,%s::jsonb,%s,now())", (*scope, body["id"], json.dumps(payload), fingerprint))
    return payload, status


def purge_expired_undertone_mutations(db, batch_rows=RETENTION_BATCH_ROWS, max_batches=RETENTION_MAX_BATCHES):
    """Delete receipts older than ``RECEIPT_RETENTION_DAYS`` in bounded batches, each under its scopes' locks."""
    deleted = 0
    relation = table("undertone_mutations")
    for _ in range(max_batches):
        with db.cursor() as cur:
            cur.execute(
                f"SELECT principal, source, id, ctid::text FROM {relation} "
                "WHERE created_at < now() - %s::interval "
                "ORDER BY created_at LIMIT %s",
                (f"{RECEIPT_RETENTION_DAYS} days", batch_rows),
            )
            rows = cur.fetchall()
            if not rows:
                break
            _lock_scopes(cur, [(principal, source) for principal, source, _, _ in rows])
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


def _source():
    return _text(request.args.get("source"), "Undertone source", 128)


def list_sources(cur, principal):
    """This principal's Undertone sources, most recently changed first, with their size and name."""
    cur.execute(f"""SELECT source,
            count(*) FILTER (WHERE id LIKE 'item:t:%%' AND value->>'deletedAt' IS NULL),
            count(*) FILTER (WHERE id LIKE 'item:a:%%' AND value->>'deletedAt' IS NULL),
            count(*) FILTER (WHERE id LIKE 'preset:%%' AND value->>'deletedAt' IS NULL),
            max(seq),
            max(CASE WHEN id = 'item:source' THEN value->'record'->'body'->>'libraryName' END),
            max(CASE WHEN id = 'item:source' THEN value->'record'->'body'->>'serverType' END)
        FROM {table('undertone_records')} WHERE principal=%s
        GROUP BY source ORDER BY max(seq) DESC""", (principal,))
    return [
        {"source": r[0], "tracks": r[1], "albums": r[2], "presets": r[3], "seq": r[4],
         "library": r[5], "serverType": r[6]}
        for r in cur.fetchall()
    ]


def register_undertone_routes(bp):
    @bp.get("/undertone")
    @require_collections_enabled
    def undertone_page():
        scope = current_collection_scope()
        detail = (
            "Everyone using the AudioMuse installation token sees this Undertone library."
            if scope["mode"] == "shared"
            else "Only this signed-in AudioMuse user can see it."
        )
        return render_page(render_undertone_page(scope["label"], detail), title="Undertone")

    @bp.get("/api/undertone/sources")
    @require_collections_enabled
    def undertone_sources():
        principal = current_principal()
        with get_db().cursor() as cur:
            sources = list_sources(cur, principal)
        response = jsonify(sources=sources, max_batch=MAX_BATCH)
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.get("/api/undertone/changes")
    @require_collections_enabled
    def undertone_changes():
        try:
            source = _source()
            cursor = int(request.args.get("cursor", 0))
            limit = max(1, min(1000, int(request.args.get("limit", 500))))
            if cursor < 0:
                raise ValueError("Invalid cursor")
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), source)
        with get_db().cursor() as cur:
            cur.execute(f"""SELECT id,revision,value,seq FROM {table('undertone_records')}
                WHERE principal=%s AND source=%s AND seq>%s ORDER BY seq LIMIT %s""", (*scope, cursor, limit + 1))
            rows = cur.fetchall()
        page = rows[:limit]
        response = jsonify(records=[_record(r[0], r[1], r[2]) for r in page],
                           cursor=page[-1][3] if page else cursor, hasMore=len(rows) > limit)
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.post("/api/undertone/mutations")
    @require_collections_enabled
    def undertone_mutation():
        try:
            if request.content_length is not None and request.content_length > MAX_RECORD_BYTES * 2:
                raise ValueError("Request too large")
            source = _source()
            body = validate_mutation(request.get_json(silent=True))
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), source)
        db = get_db()
        try:
            with db.cursor() as cur:
                _lock_scopes(cur, [scope])
                payload, status = _apply_with_receipt(cur, scope, body)
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(payload), status

    @bp.post("/api/undertone/mutations/batch")
    @require_collections_enabled
    def undertone_mutation_batch():
        """Up to ``MAX_BATCH`` mutations in one transaction under one scope lock; each answers
        as the single route would (200 or 409), in order, so a phone sends a library's details
        without a request per record. A malformed entry fails the whole batch with 400."""
        try:
            if request.content_length is not None and request.content_length > MAX_REQUEST_BYTES:
                raise ValueError("Request too large")
            source = _source()
            body = request.get_json(silent=True)
            if not isinstance(body, dict) or not isinstance(body.get("mutations"), list):
                raise ValueError("Expected {mutations: [...]}")
            mutations = body["mutations"]
            if not 1 <= len(mutations) <= MAX_BATCH:
                raise ValueError("Invalid batch size")
            for mutation in mutations:
                validate_mutation(mutation)
            if len({m["id"] for m in mutations}) != len(mutations):
                raise ValueError("Repeated mutation id")
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), source)
        db = get_db()
        results = []
        try:
            with db.cursor() as cur:
                _lock_scopes(cur, [scope])
                for mutation in mutations:
                    payload, status = _apply_with_receipt(cur, scope, mutation)
                    results.append({"status": status, **payload})
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(results=results), 200
