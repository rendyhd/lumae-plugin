"""Personal shelves: principal/catalogue scoped records, periods, and durable evidence.

A scope row lock serializes writes and idempotency acknowledgements in the same
transaction. The sequence on each latest record is a compact, tombstone-preserving
change feed; consumers can restart paging without retaining an unbounded log of
old artwork snapshots or old arrangements.
"""
import json
import math
from flask import jsonify, request
from plugin.api import get_db, table
from .collection_manager import current_principal, require_collections_enabled

SHELVES_SCHEMA_VERSION = 1


def migrate_shelves(db):
    with db.cursor() as cur:
        cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {table('shelf_sequence')}")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('shelf_scopes')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL,
            PRIMARY KEY (principal, catalog_id))""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('shelf_records')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL,
            type TEXT NOT NULL CHECK (type IN ('member','order','evidence')),
            id TEXT NOT NULL, value JSONB NOT NULL, seq BIGINT NOT NULL,
            PRIMARY KEY (principal,catalog_id,type,id))""")
        cur.execute(f"""CREATE INDEX IF NOT EXISTS lumae_shelf_changes_idx
            ON {table('shelf_records')} (principal,catalog_id,seq)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('shelf_mutations')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            response JSONB NOT NULL, PRIMARY KEY (principal,catalog_id,id))""")


def _text(value, name, maximum=512):
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"Invalid {name}")
    return value


def _timestamp(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError("Invalid timestamp")
    return value


def validate_mutation(body):
    if not isinstance(body, dict):
        raise ValueError("Expected a mutation object")
    _text(body.get("id"), "mutation id", 200)
    operation = body.get("operation")
    if operation == "add":
        member = body.get("member")
        if not isinstance(member, dict) or member.get("kind") not in ("album", "artist"):
            raise ValueError("Invalid member")
        for name in ("id", "entityId"):
            _text(member.get(name), name)
        _text(member.get("title"), "title", 4000)
        if not isinstance(member.get("artist"), str) or len(member["artist"]) > 4000:
            raise ValueError("Invalid artist")
        _timestamp(member.get("addedAt"))
        for name in ("artistId", "coverItemId"):
            if member.get(name) is not None:
                _text(member[name], name)
    elif operation in ("remove", "restore"):
        _text(body.get("memberId"), "member id")
        _timestamp(body.get("at"))
    elif operation == "order":
        if body.get("kind") not in ("album", "artist"):
            raise ValueError("Invalid shelf kind")
        if not isinstance(body.get("ids"), list):
            raise ValueError("Invalid arrangement")
        for identifier in body["ids"]:
            _text(identifier, "member id")
        if type(body.get("baseRevision")) is not int or body["baseRevision"] < 0:
            raise ValueError("Invalid revision")
    elif operation == "evidence":
        records = body.get("records")
        if not isinstance(records, list) or not 1 <= len(records) <= 500:
            raise ValueError("Evidence batches contain 1 to 500 records")
        for record in records:
            if not isinstance(record, dict) or record.get("type") not in ("search", "rating", "view", "listen", "coverage"):
                raise ValueError("Invalid evidence")
            if record.get("entityKind") not in ("album", "artist", "track"):
                raise ValueError("Invalid evidence entity")
            for name in ("id", "entityId"):
                _text(record.get(name), name, 2000)
            _timestamp(record.get("at"))
            if record["type"] == "rating" and record.get("rating") not in (None, 0, 1, 2, 3, 4, 5):
                raise ValueError("Invalid rating")
            if record.get("albumId") is not None:
                _text(record["albumId"], "album id")
            if record.get("artistIds") is not None:
                if not isinstance(record["artistIds"], list):
                    raise ValueError("Invalid artist ids")
                for artist_id in record["artistIds"]:
                    _text(artist_id, "artist id")
            if record["type"] == "search":
                _text(record.get("visitId"), "search visit")
    else:
        raise ValueError("Invalid operation")
    return body


def _load(cur, scope, record_type):
    cur.execute(f"SELECT value FROM {table('shelf_records')} WHERE principal=%s AND catalog_id=%s AND type=%s", (*scope, record_type))
    return [row[0] for row in cur.fetchall()]


def _put(cur, scope, record_type, identifier, value):
    cur.execute(f"""INSERT INTO {table('shelf_records')} (principal,catalog_id,type,id,value,seq)
        VALUES (%s,%s,%s,%s,%s::jsonb,nextval(%s))
        ON CONFLICT(principal,catalog_id,type,id) DO UPDATE
        SET value=excluded.value,seq=excluded.seq""",
        (*scope, record_type, identifier, json.dumps(value), table("shelf_sequence")))
    return {"type": record_type, "id": identifier, "value": value}


def _member_key(member):
    return member["kind"], member["entityId"]


def apply_mutation(cur, scope, body):
    """Called with the scope locked. No commit here: data and receipt are atomic."""
    operation = body["operation"]
    if operation == "evidence":
        result = []
        for value in body["records"]:
            cur.execute(f"SELECT value FROM {table('shelf_records')} WHERE principal=%s AND catalog_id=%s AND type='evidence' AND id=%s", (*scope, value["id"]))
            previous = cur.fetchone()
            if not previous or value["at"] > previous[0]["at"]:
                result.append(_put(cur, scope, "evidence", value["id"], value))
            else:
                result.append({"type": "evidence", "id": value["id"], "value": previous[0]})
        return {"records": result}, 200

    members = _load(cur, scope, "member")
    if operation == "order":
        kind = body["kind"]
        order = next((o for o in _load(cur, scope, "order") if o["kind"] == kind),
                     {"kind": kind, "ids": [], "revision": 0})
        if order["revision"] != body["baseRevision"]:
            return {"error": "order_conflict", "order": order}, 409
        available = {m["id"]: m for m in members if m["kind"] == kind and m["deletedAt"] is None}
        aliases = {alias: m["id"] for m in available.values() for alias in m["aliases"]}
        ids = list(dict.fromkeys(aliases.get(identifier, identifier) for identifier in body["ids"]))
        ids = [identifier for identifier in ids if identifier in available]
        present = set(ids)
        ids.extend(m["id"] for m in sorted(available.values(), key=lambda m: (m["position"], m["id"])) if m["id"] not in present)
        value = {"kind": kind, "ids": ids, "revision": order["revision"] + 1}
        return {"records": [_put(cur, scope, "order", kind, value)]}, 200

    identifier = body["member"]["id"] if operation == "add" else body["memberId"]
    member = next((m for m in members if m["id"] == identifier or identifier in m["aliases"]), None)
    if operation == "add":
        if member:  # Replayed or delayed add never resurrects a deleted period.
            return {"records": [{"type": "member", "id": member["id"], "value": member}]}, 200
        incoming = body["member"]
        member = next((m for m in members if m["deletedAt"] is None and _member_key(m) == _member_key(incoming)), None)
        if member:
            member["aliases"].append(identifier)
        else:
            member = {k: incoming[k] for k in ("id", "kind", "entityId", "title", "artist", "addedAt")}
            for name in ("artistId", "coverItemId"):
                if incoming.get(name) is not None:
                    member[name] = incoming[name]
            member.update(position=1 + max((m["position"] for m in members if m["kind"] == incoming["kind"]), default=-1), aliases=[], deletedAt=None)
    elif member:
        if operation == "remove":
            member["deletedAt"] = body["at"]
        elif not any(m["id"] != member["id"] and m["deletedAt"] is None and _member_key(m) == _member_key(member) for m in members):
            member["deletedAt"] = None
    if not member:
        return {"error": "unknown_membership_period"}, 409
    return {"records": [_put(cur, scope, "member", member["id"], member)]}, 200



def rekey_shelves(cur, catalog_id, mapping):
    """Rewrite exact references, publishing new sequence values for other devices."""
    from .provider_identity_rekey import _replace_exact
    for name, key_columns, payload_column in (
        ("shelf_records", ("principal", "catalog_id", "type", "id"), "value"),
        ("shelf_mutations", ("principal", "catalog_id", "id"), "response"),
    ):
        cur.execute(f"SELECT {','.join(key_columns)},{payload_column} FROM {table(name)} WHERE catalog_id=%s", (catalog_id,))
        for row in cur.fetchall():
            rewritten = _replace_exact(row[-1], mapping)
            if rewritten == row[-1]:
                continue
            sequence = f",seq=nextval('{table('shelf_sequence')}')" if name == "shelf_records" else ""
            cur.execute(f"UPDATE {table(name)} SET {payload_column}=%s::jsonb{sequence} WHERE " +
                        " AND ".join(f"{key}=%s" for key in key_columns),
                        (json.dumps(rewritten), *row[:-1]))


def register_shelf_routes(bp):
    @bp.get("/api/shelves/changes")
    @bp.get("/api/shelves/snapshot")
    @require_collections_enabled
    def shelf_changes():
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
            cur.execute(f"""SELECT type,id,value,seq FROM {table('shelf_records')}
                WHERE principal=%s AND catalog_id=%s AND seq>%s ORDER BY seq LIMIT %s""", (*scope, cursor, limit + 1))
            rows = cur.fetchall()
        page = rows[:limit]
        response = jsonify(records=[{"type": r[0], "id": r[1], "value": r[2]} for r in page],
                           cursor=page[-1][3] if page else cursor, hasMore=len(rows) > limit)
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.post("/api/shelves/mutations")
    @require_collections_enabled
    def shelf_mutation():
        try:
            catalog = _text(request.args.get("catalog_id"), "catalogue identity")
            body = validate_mutation(request.get_json(silent=True))
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), catalog)
        db = get_db()
        try:
            with db.cursor() as cur:
                cur.execute(f"INSERT INTO {table('shelf_scopes')} (principal,catalog_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", scope)
                cur.execute(f"SELECT principal FROM {table('shelf_scopes')} WHERE principal=%s AND catalog_id=%s FOR UPDATE", scope)
                cur.execute(f"SELECT response FROM {table('shelf_mutations')} WHERE principal=%s AND catalog_id=%s AND id=%s", (*scope, body["id"]))
                receipt = cur.fetchone()
                if receipt:
                    payload, status = receipt[0], 200
                else:
                    payload, status = apply_mutation(cur, scope, body)
                    if status == 200:
                        cur.execute(f"INSERT INTO {table('shelf_mutations')} (principal,catalog_id,id,response) VALUES (%s,%s,%s,%s::jsonb)", (*scope, body["id"], json.dumps(payload)))
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(payload), status
