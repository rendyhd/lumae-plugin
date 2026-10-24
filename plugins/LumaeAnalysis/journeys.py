"""Journeys: one record per journey, per principal and catalogue (Lumae JN.10).

A journey is the phone's own data: the plugin stores what phones send and
merges two writes, and never builds or edits a journey itself. Every record
carries the server's revision: a write names the revision it was based on and
a stale one is refused with the current record, so the phone takes that
record's credits and writes again over it.

Storing a write merges it with the stored copy:

* credits, the receipts of journey plays, are a set that only grows, keyed by
  occurrence id;
* an entry's state is the one changed last (its ``stateAt``; a state never
  changed counts as oldest), so a song played on one phone stays played when
  the other phone writes, and a song marked not played stays so;
* the place is the one played last (``lastPlayedAt``).

Deletes are tombstones kept forever, so every phone learns about them. Like
Vibes, a scope row lock serializes writes and receipts in the same
transaction, and the sequence on each latest record is a compact change feed.
"""
import hashlib
import json
import math
from flask import jsonify, request
from plugin.api import get_db, table
from . import migrations
from .collection_manager import current_principal, require_collections_enabled

JOURNEYS_SCHEMA_VERSION = 1
JOURNEY_VERSION = 1
MAX_JOURNEY_BYTES = 1_500_000
MAX_REQUEST_BYTES = 2 * MAX_JOURNEY_BYTES
STATUSES = ("in-progress", "finished")
ENTRY_STATES = ("pending", "played")
OUTCOMES = ("ended", "skipped", "heard")
# Receipts are kept like Vibe receipts: a retry with the same mutation id
# after this age is no longer recognised and is checked by revision instead.
RECEIPT_RETENTION_DAYS = 30
RETENTION_BATCH_ROWS = 5_000
RETENTION_MAX_BATCHES = 20


def migrate_journeys(db):
    with db.cursor() as cur:
        cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {table('journey_sequence')}")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('journey_scopes')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL,
            PRIMARY KEY (principal, catalog_id))""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('journey_records')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            revision BIGINT NOT NULL, deleted BOOLEAN NOT NULL, journey JSONB,
            credits JSONB NOT NULL, seq BIGINT NOT NULL,
            PRIMARY KEY (principal, catalog_id, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_journey_changes_idx
            ON {table('journey_records')} (principal,catalog_id,seq)""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('journey_mutations')} (
            principal TEXT NOT NULL, catalog_id TEXT NOT NULL, id TEXT NOT NULL,
            response JSONB NOT NULL, request_fingerprint TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (principal, catalog_id, id))""")
        migrations.ensure_index(cur, f"""CREATE INDEX IF NOT EXISTS lumae_journey_mutations_created_idx
            ON {table('journey_mutations')} (created_at)""")


def capability(enabled, scope):
    return {
        "schema_version": JOURNEYS_SCHEMA_VERSION,
        "enabled": enabled,
        "scope": scope,
        "max_journey_bytes": MAX_JOURNEY_BYTES,
    }


def _text(value, name, maximum=512):
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"Invalid {name}")
    return value


def _number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"Invalid {name}")
    return value


def _optional_number(value, name):
    return None if value is None else _number(value, name)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _validate_journey(journey, journey_id):
    """Checks what the merge reads; everything else is stored as sent."""
    if not isinstance(journey, dict) or journey.get("version") != JOURNEY_VERSION:
        raise ValueError("Invalid journey")
    if journey.get("id") != journey_id:
        raise ValueError("Journey id does not match")
    if not isinstance(journey.get("name"), str) or len(journey["name"]) > 1000:
        raise ValueError("Invalid journey name")
    if journey.get("status") not in STATUSES:
        raise ValueError("Invalid journey status")
    place = journey.get("place")
    if not isinstance(place, dict):
        raise ValueError("Invalid place")
    if place.get("entryId") is not None:
        _text(place["entryId"], "place entry", 200)
    _number(place.get("positionSeconds"), "place position")
    _optional_number(journey.get("lastPlayedAt"), "last played time")
    entries = journey.get("entries")
    if not isinstance(entries, list):
        raise ValueError("Invalid entries")
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Invalid entry")
        entry_id = _text(entry.get("entryId"), "entry id", 200)
        if entry_id in seen:
            raise ValueError("Duplicate entry id")
        seen.add(entry_id)
        if entry.get("state") not in ENTRY_STATES:
            raise ValueError("Invalid entry state")
        _optional_number(entry.get("stateAt"), "entry state time")


def _validate_credits(credits):
    if not isinstance(credits, list):
        raise ValueError("Invalid credits")
    seen = set()
    for credit in credits:
        if not isinstance(credit, dict):
            raise ValueError("Invalid credit")
        occurrence = _text(credit.get("occurrenceId"), "occurrence id", 300)
        if occurrence in seen:
            raise ValueError("Duplicate credit")
        seen.add(occurrence)
        _text(credit.get("entryId"), "credit entry", 200)
        if credit.get("outcome") not in OUTCOMES:
            raise ValueError("Invalid credit outcome")
        _number(credit.get("heardSeconds"), "heard seconds")


def validate_mutation(body):
    if not isinstance(body, dict):
        raise ValueError("Expected a mutation object")
    _text(body.get("id"), "mutation id", 300)
    journey_id = _text(body.get("journeyId"), "journey id", 200)
    base = body.get("baseRevision", "missing")
    if base is not None and (type(base) is not int or base < 1):
        raise ValueError("Invalid revision")
    operation = body.get("operation")
    if operation == "upsert":
        _validate_journey(body.get("journey"), journey_id)
        _validate_credits(body.get("credits"))
    elif operation == "delete":
        _number(body.get("at"), "timestamp")
    else:
        raise ValueError("Invalid operation")
    # Also refuses NaN anywhere: PostgreSQL's JSON has no such number.
    if len(_canonical(body).encode("utf-8")) > MAX_JOURNEY_BYTES:
        raise ValueError("Journey is too large")
    return body


def request_fingerprint(body):
    """SHA-256 of the canonical mutation body; the receipt key is its ``id``."""
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def _later(this, that):
    """Whether time ``this`` is after ``that``; no time counts as oldest."""
    return this is not None and (that is None or this > that)


def merge_journey(stored, incoming):
    """The incoming body, with the stored copy's newer entry states and newer place."""
    if stored is None:
        return incoming
    before = {entry.get("entryId"): entry for entry in stored.get("entries") or [] if isinstance(entry, dict)}
    entries = []
    for entry in incoming["entries"]:
        old = before.get(entry["entryId"])
        if old and old.get("state") != entry["state"] and _later(old.get("stateAt"), entry.get("stateAt")):
            entry = {**entry, "state": old["state"], "stateBy": old.get("stateBy"), "stateAt": old["stateAt"]}
        entries.append(entry)
    merged = {**incoming, "entries": entries}
    place = stored.get("place")
    if (_later(stored.get("lastPlayedAt"), incoming.get("lastPlayedAt")) and isinstance(place, dict)
            and (place.get("entryId") is None or place["entryId"] in {e["entryId"] for e in entries})):
        merged["place"] = place
        merged["lastPlayedAt"] = stored["lastPlayedAt"]
    return merged


def merge_credits(stored, incoming):
    """The union by occurrence id; a credit already stored is kept as it is."""
    merged = list(stored)
    known = {credit.get("occurrenceId") for credit in stored}
    merged.extend(credit for credit in incoming if credit["occurrenceId"] not in known)
    return merged


def _lock_scopes(cur, scopes):
    """Lock journey scopes in (principal, catalogue) order, creating missing rows.

    Every seq is allocated under its scope's lock, held to commit, so within a
    scope seqs commit in order and a reader paging by ``seq > cursor`` never
    skips a record that commits late.
    """
    for scope in sorted(set(scopes)):
        cur.execute(f"INSERT INTO {table('journey_scopes')} (principal,catalog_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", scope)
        cur.execute(f"SELECT principal FROM {table('journey_scopes')} WHERE principal=%s AND catalog_id=%s FOR UPDATE", scope)


def _record(identifier, revision, deleted, journey, credits):
    return {"journeyId": identifier, "revision": revision, "deleted": deleted,
            "journey": journey, "credits": credits}


def apply_mutation(cur, scope, body):
    """Called with the scope locked. No commit here: data and receipt are atomic."""
    identifier = body["journeyId"]
    cur.execute(f"""SELECT revision,deleted,journey,credits FROM {table('journey_records')}
        WHERE principal=%s AND catalog_id=%s AND id=%s""", (*scope, identifier))
    row = cur.fetchone()
    current = row[0] if row else None
    if current != body["baseRevision"]:
        return {"error": "journey_conflict", "record": _record(identifier, *row) if row else None}, 409
    live = row is not None and not row[1]
    if body["operation"] == "upsert":
        deleted = False
        journey = merge_journey(row[2] if live else None, body["journey"])
        credits = merge_credits(row[3] if live else [], body["credits"])
    else:
        deleted, journey, credits = True, None, []
    revision = (current or 0) + 1
    cur.execute(f"""INSERT INTO {table('journey_records')}
        (principal,catalog_id,id,revision,deleted,journey,credits,seq)
        VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,nextval(%s))
        ON CONFLICT(principal,catalog_id,id) DO UPDATE
        SET revision=excluded.revision,deleted=excluded.deleted,journey=excluded.journey,
            credits=excluded.credits,seq=excluded.seq""",
        (*scope, identifier, revision, deleted, None if journey is None else json.dumps(journey),
         json.dumps(credits), table("journey_sequence")))
    return {"record": _record(identifier, revision, deleted, journey, credits)}, 200


def rekey_journeys(cur, catalog_id, mapping):
    """Rewrite exact provider ids inside stored journeys (songs, albums, sources).

    A rewrite allocates a new seq, so a phone that does not have the journey
    yet receives the rewritten copy, but keeps the revision: the journey did
    not change, only the ids naming its music (the phones rekey their own
    copies), so a phone's next write based on that revision still applies.
    Locks only the scopes it rewrites, like ``rekey_vibes``.
    """
    from .provider_identity_rekey import _replace_exact
    tables = (
        ("journey_records", ("principal", "catalog_id", "id"), "journey"),
        ("journey_mutations", ("principal", "catalog_id", "id"), "response"),
    )
    affected = set()
    for name, _, payload_column in tables:
        cur.execute(f"SELECT principal,{payload_column} FROM {table(name)} "
                    f"WHERE catalog_id=%s AND {payload_column} IS NOT NULL", (catalog_id,))
        affected.update(row[0] for row in cur.fetchall() if _replace_exact(row[1], mapping) != row[1])
    if not affected:
        return
    _lock_scopes(cur, [(principal, catalog_id) for principal in affected])
    for name, key_columns, payload_column in tables:
        # Read again under the locks: a mutation may have committed meanwhile.
        cur.execute(f"SELECT {','.join(key_columns)},{payload_column} FROM {table(name)} "
                    f"WHERE catalog_id=%s AND principal=ANY(%s) AND {payload_column} IS NOT NULL",
                    (catalog_id, sorted(affected)))
        for row in cur.fetchall():
            rewritten = _replace_exact(row[-1], mapping)
            if rewritten == row[-1]:
                continue
            sequence = f",seq=nextval('{table('journey_sequence')}')" if name == "journey_records" else ""
            cur.execute(f"UPDATE {table(name)} SET {payload_column}=%s::jsonb{sequence} WHERE " +
                        " AND ".join(f"{key}=%s" for key in key_columns),
                        (json.dumps(rewritten), *row[:-1]))


def purge_expired_journey_mutations(db, batch_rows=RETENTION_BATCH_ROWS, max_batches=RETENTION_MAX_BATCHES):
    """Delete journey receipts older than ``RECEIPT_RETENTION_DAYS``, oldest
    first, in bounded batches, each under its scopes' locks (the lock a
    mutation takes before it reads its receipt), as Vibes do."""
    deleted = 0
    relation = table("journey_mutations")
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


def register_journey_routes(bp):
    @bp.get("/api/journeys/changes")
    @require_collections_enabled
    def journey_changes():
        try:
            catalog = _text(request.args.get("catalog_id"), "catalogue identity")
            cursor = int(request.args.get("cursor", 0))
            limit = max(1, min(250, int(request.args.get("limit", 100))))
            if cursor < 0:
                raise ValueError("Invalid cursor")
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        scope = (current_principal(), catalog)
        with get_db().cursor() as cur:
            cur.execute(f"""SELECT id,revision,deleted,journey,credits,seq FROM {table('journey_records')}
                WHERE principal=%s AND catalog_id=%s AND seq>%s ORDER BY seq LIMIT %s""", (*scope, cursor, limit + 1))
            rows = cur.fetchall()
        page = rows[:limit]
        response = jsonify(records=[_record(*r[:5]) for r in page],
                           cursor=page[-1][5] if page else cursor, hasMore=len(rows) > limit)
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.post("/api/journeys/mutations")
    @require_collections_enabled
    def journey_mutation():
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
                cur.execute(f"SELECT response,request_fingerprint FROM {table('journey_mutations')} WHERE principal=%s AND catalog_id=%s AND id=%s", (*scope, body["id"]))
                receipt = cur.fetchone()
                if receipt and receipt[1] != fingerprint:
                    # This mutation id already acknowledged another body.
                    payload, status = {"error": "idempotency_key_conflict"}, 409
                elif receipt:
                    payload, status = receipt[0], 200
                else:
                    payload, status = apply_mutation(cur, scope, body)
                    if status == 200:
                        cur.execute(f"INSERT INTO {table('journey_mutations')} (principal,catalog_id,id,response,request_fingerprint,created_at) VALUES (%s,%s,%s,%s::jsonb,%s,now())", (*scope, body["id"], json.dumps(payload), fingerprint))
            db.commit()
        except Exception:
            db.rollback()
            raise
        return jsonify(payload), status
