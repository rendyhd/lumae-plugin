"""Catalogue-independent personal discovery records and recoverable sync v1."""
import hashlib
import json
import uuid
from flask import abort, g, jsonify, request
from plugin.api import get_db, table
from .collection_manager import current_principal, collections_enabled

SCHEMA_VERSION = 1
FIELDS = {
    "album": {"title", "artist", "aliases", "verification", "releaseGroupId", "releaseId"},
    "want": {"albumId", "intent", "note", "addedAt", "source", "explanation", "acquiredAt", "canonicalKey"},
    "memory": {"entity", "text", "period", "includeInRecommendations", "createdAt"},
    "feedback": {"entity", "recognition", "affection", "updatedAt"},
    "dismissal": {"entity", "until", "createdAt"},
    "rest": {"entity", "until", "createdAt"},
    "introduction": {"entity", "source", "introducedAt", "policyVersion"},
    "order": {"ids"},
}


def principal():
    if not (getattr(g, "auth_method", None) in ("bearer", "session") or getattr(g, "auth_user", None)):
        abort(401)
    return current_principal()


def identifier(value):
    if not isinstance(value, str):
        raise ValueError("UUID required")
    return str(uuid.UUID(value))


def canonical(value):
    if value is None:
        return None
    prefix = "musicbrainz:release-group:"
    if not isinstance(value, str) or not value.startswith(prefix):
        raise ValueError("Only verified release-group identity can merge saves")
    return prefix + identifier(value[len(prefix):])


def validate(body):
    if not isinstance(body, dict) or set(body) - {"id", "epoch", "recordId", "kind", "operation", "baseRevision", "fields"}:
        raise ValueError("Invalid mutation envelope")
    for key in ("id", "epoch", "recordId"):
        identifier(body.get(key))
    kind, operation = body.get("kind"), body.get("operation")
    if kind not in FIELDS or operation not in ("upsert", "delete", "restore"):
        raise ValueError("Invalid record kind or operation")
    if type(body.get("baseRevision")) is not int or body["baseRevision"] < 0:
        raise ValueError("Invalid revision")
    fields = body.get("fields", {})
    if not isinstance(fields, dict) or set(fields) - FIELDS[kind]:
        raise ValueError("Invalid fields")
    # No credentials, logs or recommendation snapshots belong in this protocol.
    encoded = json.dumps(fields, allow_nan=False)
    if len(encoded.encode()) > 16000:
        raise ValueError("Record exceeds 16 KB")
    if "canonicalKey" in fields:
        fields["canonicalKey"] = canonical(fields["canonicalKey"])
    if "ids" in fields and (not isinstance(fields["ids"], list) or len(fields["ids"]) > 2000):
        raise ValueError("Invalid order")
    for key in ("id", "epoch", "recordId"):
        body[key] = identifier(body[key])
    if operation != "upsert" and fields:
        raise ValueError("Delete and restore do not change fields")
    if kind == "introduction" and operation != "upsert":
        raise ValueError("Introduction provenance cannot be removed through sync")
    for key in ("addedAt", "acquiredAt", "createdAt", "updatedAt", "introducedAt", "until"):
        if key in fields and fields[key] is not None and (type(fields[key]) not in (int, float) or fields[key] < 0):
            raise ValueError("Invalid timestamp")
    for key in ("title", "artist", "note", "text", "period", "explanation", "source"):
        if key in fields and fields[key] is not None and not isinstance(fields[key], str):
            raise ValueError("Expected text field")
    for key in ("albumId", "releaseGroupId", "releaseId"):
        if fields.get(key) is not None:
            fields[key] = identifier(fields[key])
    if "aliases" in fields and (not isinstance(fields["aliases"], list) or len(fields["aliases"]) > 200 or any(not isinstance(v, str) or len(v) > 500 for v in fields["aliases"])):
        raise ValueError("Invalid aliases")
    if "recognition" in fields and fields["recognition"] not in ("remembered", "new_to_me", "unknown", None):
        raise ValueError("Invalid recognition")
    if "affection" in fields and fields["affection"] not in ("old_favourite", "not_for_me", "neutral", "unknown", None):
        raise ValueError("Invalid affection")
    if "intent" in fields and fields["intent"] not in (None, "discovery", "comfort", "unclassified"):
        raise ValueError("Invalid saving intent")
    if "includeInRecommendations" in fields and type(fields["includeInRecommendations"]) is not bool:
        raise ValueError("Invalid memory consent")
    if "entity" in fields:
        entity = fields["entity"]
        if not isinstance(entity, dict) or set(entity) - {"kind", "id", "catalogId", "mbid", "name"}:
            raise ValueError("Invalid entity reference")
        if entity.get("kind") == "recording":
            identifier(entity.get("mbid"))
        elif entity.get("kind") == "track":
            if not all(isinstance(entity.get(k), str) and 0 < len(entity[k]) <= 500 for k in ("id", "catalogId")):
                raise ValueError("Tracks require catalogue-scoped identity")
        elif entity.get("kind") in ("album", "artist"):
            if not any(isinstance(entity.get(k), str) and 0 < len(entity[k]) <= 500 for k in ("id", "mbid", "name")):
                raise ValueError("Entity identity required")
        else:
            raise ValueError("Invalid entity kind")
        if kind in ("rest", "introduction") and entity["kind"] not in ("recording", "track"):
            raise ValueError("Recording identity required")
    if "ids" in fields:
        fields["ids"] = [identifier(value) for value in fields["ids"]]
        if len(set(fields["ids"])) != len(fields["ids"]):
            raise ValueError("Order contains duplicates")
    return body


def migrate(db):
    with db.cursor() as cur:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('discovery_scopes')} (
            principal TEXT PRIMARY KEY, epoch TEXT NOT NULL, head BIGINT NOT NULL DEFAULT 0)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('discovery_records')} (
            principal TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL,
            payload JSONB NOT NULL, seq BIGINT NOT NULL,
            PRIMARY KEY(principal,kind,id))""")
        cur.execute(f"CREATE INDEX IF NOT EXISTS lumae_discovery_changes ON {table('discovery_records')}(principal,seq)")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('discovery_receipts')} (
            principal TEXT NOT NULL, id TEXT NOT NULL, fingerprint TEXT NOT NULL,
            payload JSONB NOT NULL, status INTEGER NOT NULL, PRIMARY KEY(principal,id))""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('discovery_aliases')} (
            principal TEXT NOT NULL, kind TEXT NOT NULL, alias TEXT NOT NULL, target TEXT NOT NULL,
            PRIMARY KEY(principal,kind,alias))""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('discovery_canonical')} (
            principal TEXT NOT NULL, identity TEXT NOT NULL, target TEXT NOT NULL,
            PRIMARY KEY(principal,identity))""")


def scope(cur, user):
    cur.execute(f"INSERT INTO {table('discovery_scopes')}(principal,epoch) VALUES(%s,%s) ON CONFLICT DO NOTHING", (user, str(uuid.uuid4())))
    cur.execute(f"SELECT epoch,head FROM {table('discovery_scopes')} WHERE principal=%s FOR UPDATE", (user,))
    return cur.fetchone()


def mutate(db, user, body):
    validate(body)
    fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    with db.cursor() as cur:
        epoch, head = scope(cur, user)
        if body["epoch"] != epoch:
            return {"error": "bootstrap_required", "epoch": epoch}, 410
        cur.execute(f"SELECT fingerprint,payload,status FROM {table('discovery_receipts')} WHERE principal=%s AND id=%s", (user, body["id"]))
        receipt = cur.fetchone()
        if receipt:
            return (receipt[1], receipt[2]) if receipt[0] == fingerprint else ({"error": "mutation_id_reused"}, 409)
        kind, target = body["kind"], body["recordId"]
        fields = body.get("fields", {})
        cur.execute(f"SELECT target FROM {table('discovery_aliases')} WHERE principal=%s AND kind=%s AND alias=%s", (user, kind, target))
        alias = cur.fetchone()
        target = alias[0] if alias else target
        identity = canonical(fields.get("canonicalKey")) if kind == "want" else None
        duplicate = False
        original = None
        if identity:
            cur.execute(f"SELECT payload FROM {table('discovery_records')} WHERE principal=%s AND kind=%s AND id=%s", (user, kind, target))
            original_row = cur.fetchone()
            original = original_row[0] if original_row else None
            cur.execute(f"SELECT target FROM {table('discovery_canonical')} WHERE principal=%s AND identity=%s", (user, identity))
            match = cur.fetchone()
            if match and match[0] != target:
                target, duplicate = match[0], True
        cur.execute(f"SELECT payload FROM {table('discovery_records')} WHERE principal=%s AND kind=%s AND id=%s", (user, kind, target))
        row = cur.fetchone()
        old = row[0] if row else {"id": target, "kind": kind, "revision": 0, "deleted": False, "fields": {}, "fieldRevisions": {}}
        op, base = body["operation"], body["baseRevision"]
        conflicts = [key for key in fields if old["fieldRevisions"].get(key, 0) > base and old["fields"].get(key) != fields[key]]
        protected = (kind == "introduction" and any(k in old["fields"] and old["fields"][k] != v for k, v in fields.items()))
        identity_change = identity and old["fields"].get("canonicalKey") not in (None, identity)
        if duplicate and original and not original["deleted"]:
            payload, status = {"error": "duplicate_save_conflict", "record": old, "localRecord": original}, 409
        elif protected or identity_change:
            payload, status = {"error": "identity_or_provenance_conflict", "record": old}, 409
        elif base > old["revision"] or (op == "restore" and base != old["revision"]):
            payload, status = {"error": "revision_conflict", "record": old}, 409
        elif op == "upsert" and old["deleted"]:
            payload, status = {"error": "record_deleted", "record": old}, 409
        elif op == "upsert" and conflicts and not duplicate:
            payload, status = {"error": "field_conflict", "fields": conflicts, "record": old}, 409
        else:
            new = {**old, "fields": dict(old["fields"]), "fieldRevisions": dict(old["fieldRevisions"]), "revision": old["revision"] + 1}
            if op in ("delete", "restore"):
                new["deleted"] = op == "delete"
            else:
                for key, value in fields.items():
                    # Duplicate saves preserve the first save and existing personal edits.
                    if duplicate and key in new["fields"]:
                        continue
                    new["fields"][key] = value
                    new["fieldRevisions"][key] = new["revision"]
            head += 1
            cur.execute(f"UPDATE {table('discovery_scopes')} SET head=%s WHERE principal=%s", (head, user))
            cur.execute(f"""INSERT INTO {table('discovery_records')}(principal,kind,id,payload,seq) VALUES(%s,%s,%s,%s::jsonb,%s)
                ON CONFLICT(principal,kind,id) DO UPDATE SET payload=excluded.payload,seq=excluded.seq""", (user, kind, target, json.dumps(new), head))
            if identity:
                cur.execute(f"INSERT INTO {table('discovery_canonical')}(principal,identity,target) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING", (user, identity, target))
            if duplicate:
                cur.execute(f"INSERT INTO {table('discovery_aliases')}(principal,kind,alias,target) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING", (user, kind, body["recordId"], target))
            payload, status = {"epoch": epoch, "cursor": head, "record": new, "alias": body["recordId"] if duplicate else None}, 200
        cur.execute(f"INSERT INTO {table('discovery_receipts')}(principal,id,fingerprint,payload,status) VALUES(%s,%s,%s,%s::jsonb,%s)", (user, body["id"], fingerprint, json.dumps(payload), status))
        return payload, status


def page(db, user, args, bootstrap=False):
    limit = max(1, min(250, int(args.get("limit", 100))))
    cursor = int(args.get("cursor", 0))
    if cursor < 0:
        raise ValueError("Invalid cursor")
    with db.cursor() as cur:
        epoch, head = scope(cur, user)
        if not bootstrap and args.get("epoch") != epoch:
            return {"error": "bootstrap_required", "epoch": epoch}, 410
        if bootstrap and cursor and args.get("epoch") != epoch:
            return {"error": "bootstrap_required", "epoch": epoch}, 410
        ceiling = int(args.get("head", head)) if bootstrap else head
        if cursor > head:
            return {"error": "bootstrap_required", "epoch": epoch}, 410
        if ceiling > head or cursor > ceiling:
            raise ValueError("Invalid head")
        cur.execute(f"SELECT payload,seq FROM {table('discovery_records')} WHERE principal=%s AND seq>%s AND seq<=%s ORDER BY seq LIMIT %s", (user, cursor, ceiling, limit + 1))
        rows = cur.fetchall()
        more = len(rows) > limit
        rows = rows[:limit]
        return {"schema_version": 1, "epoch": epoch, "head": ceiling,
                "records": [row[0] for row in rows], "cursor": rows[-1][1] if more else ceiling,
                "hasMore": more}, 200


def register_routes(bp):
    def invoke(work):
        user = principal()
        if not collections_enabled():
            return jsonify(error="personal_discovery_disabled"), 503
        db = get_db()
        try:
            request.max_content_length = 24000
            payload, status = work(db, user)
            db.commit()
        except (ValueError, TypeError, OverflowError):
            db.rollback()
            payload, status = {"error": "invalid_discovery_request"}, 400
        except Exception:
            db.rollback()
            raise
        response = jsonify(payload)
        response.status_code = status
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.post("/api/personal_discovery/mutations")
    def discovery_mutation():
        return invoke(lambda db, user: mutate(db, user, request.get_json(silent=True)))

    @bp.get("/api/personal_discovery/bootstrap")
    def discovery_bootstrap():
        return invoke(lambda db, user: page(db, user, request.args, True))

    @bp.get("/api/personal_discovery/changes")
    def discovery_changes():
        return invoke(lambda db, user: page(db, user, request.args))
