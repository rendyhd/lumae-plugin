"""External MusicBrainz entities: durable bounded jobs, independent of collected albums."""
import hashlib
import json
import unicodedata
import uuid
from flask import jsonify, request
from plugin.api import get_db, table
from .personal_discovery import principal, identifier
from .credits_musicbrainz import Client, MusicBrainzDeferred, _quoted
from .credits_service import paused, playback_pending

SCHEMA_VERSION = 1
KINDS = {"artist", "release-group", "release", "recording"}


def migrate(db):
    with db.cursor() as cur:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('metadata_accounts')} (
            principal TEXT PRIMARY KEY, budget_day DATE NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')::date,
            used INTEGER NOT NULL DEFAULT 0, last_work TIMESTAMPTZ NOT NULL DEFAULT 'epoch')""")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table('metadata_jobs')} (
            principal TEXT NOT NULL, id TEXT NOT NULL, fingerprint TEXT NOT NULL, input JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', result JSONB, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
            not_before TIMESTAMPTZ NOT NULL DEFAULT now(), lease TEXT, lease_until TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(principal,id))""")
        cur.execute(f"CREATE INDEX IF NOT EXISTS lumae_metadata_due ON {table('metadata_jobs')}(status,not_before)")


def ensure_schedule(db):
    with db.cursor() as cur:
        cur.execute("""INSERT INTO cron(name,task_type,cron_expr,enabled)
            VALUES(%s,%s,%s,TRUE)
            ON CONFLICT(task_type) DO NOTHING""", ('Lumae album metadata', 'plugin.lumae_analysis.music_metadata', '* * * * *'))


def validate_entity(entity):
    if not isinstance(entity, dict) or set(entity) - {"id", "kind", "mbid", "title", "artist", "revisions"}:
        raise ValueError("Invalid entity envelope")
    identifier(entity.get("id"))
    if entity.get("kind") not in KINDS:
        raise ValueError("Invalid entity kind")
    if entity.get("mbid"):
        identifier(entity["mbid"])
    for key in ("title", "artist"):
        if key in entity and (not isinstance(entity[key], str) or not 1 <= len(entity[key]) <= 500):
            raise ValueError("Invalid entity name")
    if not entity.get("mbid") and (not entity.get("title") or (entity["kind"] != "artist" and not entity.get("artist"))):
        raise ValueError("Names or a typed MusicBrainz ID are required")
    revisions = entity.get("revisions", {})
    if not isinstance(revisions, dict) or set(revisions) - {"account", "source", "consent", "seed", "catalogue", "policy"}:
        raise ValueError("Invalid revision context")
    if any(not isinstance(v, (str, int)) or isinstance(v, bool) or len(str(v)) > 200 for v in revisions.values()):
        raise ValueError("Invalid revision value")
    return entity


def submit(db, user, entities):
    if not isinstance(entities, list) or not 1 <= len(entities) <= 40:
        raise ValueError("Submit 1 to 40 entities")
    for entity in entities:
        validate_entity(entity)
    if len({e["id"] for e in entities}) != len(entities):
        raise ValueError("Duplicate job IDs")
    with db.cursor() as cur:
        cur.execute(f"INSERT INTO {table('metadata_accounts')}(principal) VALUES(%s) ON CONFLICT DO NOTHING", (user,))
        cur.execute(f"SELECT principal FROM {table('metadata_accounts')} WHERE principal=%s FOR UPDATE", (user,))
        cur.execute(f"DELETE FROM {table('metadata_jobs')} WHERE principal=%s AND status NOT IN ('pending','running','deferred') AND updated_at<now()-interval '7 days'", (user,))
        cur.execute(f"SELECT id,fingerprint,status FROM {table('metadata_jobs')} WHERE principal=%s AND id=ANY(%s)", (user, [e["id"] for e in entities]))
        existing = {row[0]: row[1:] for row in cur.fetchall()}
        new_count = sum(e["id"] not in existing for e in entities)
        cur.execute(f"SELECT count(*) FROM {table('metadata_jobs')} WHERE principal=%s AND status IN ('pending','running','deferred')", (user,))
        if cur.fetchone()[0] + new_count > 40:
            return {"error": "pending_limit", "limit": 40}, 429
        for entity in entities:
            fingerprint = hashlib.sha256(json.dumps(entity, sort_keys=True).encode()).hexdigest()
            if entity["id"] in existing and existing[entity["id"]][0] != fingerprint:
                return {"error": "job_id_reused"}, 409
        for entity in entities:
            fingerprint = hashlib.sha256(json.dumps(entity, sort_keys=True).encode()).hexdigest()
            cur.execute(f"INSERT INTO {table('metadata_jobs')}(principal,id,fingerprint,input) VALUES(%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING", (user, entity["id"], fingerprint, json.dumps(entity)))
    return {"schema_version": 1, "job_ids": [e["id"] for e in entities], "status": "queued"}, 202


def reserve(db, user):
    with db.cursor() as cur:
        cur.execute(f"""UPDATE {table('metadata_accounts')} SET
            used=CASE WHEN budget_day=(now() AT TIME ZONE 'UTC')::date THEN used+1 ELSE 1 END,
            budget_day=(now() AT TIME ZONE 'UTC')::date WHERE principal=%s
            AND (budget_day<>(now() AT TIME ZONE 'UTC')::date OR used<80) RETURNING used""", (user,))
        allowed = cur.fetchone()
    db.commit()  # Reservation survives HTTP failures and worker loss.
    if not allowed:
        raise MusicBrainzDeferred("daily_request_allowance", 3600)


def normalized(value):
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).casefold().split())


def artist_name(entity):
    return "".join((a.get("name") or (a.get("artist") or {}).get("name", "")) + a.get("joinphrase", "") for a in entity.get("artist-credit", []) if isinstance(a, dict))


def resolve(entity, client):
    kind, mbid = entity["kind"], entity.get("mbid")
    evidence = "typed_identifier"
    if not mbid:
        query = f'{kind.replace("-", "")}:{_quoted(entity["title"])}'
        if kind != "artist":
            query += f' AND artist:{_quoted(entity["artist"])}'
        result = client.get(kind, query=query, limit=5)
        rows = result.get(kind + "s", [])
        matches = [r for r in rows if normalized(r.get("name" if kind == "artist" else "title")) == normalized(entity["title"])
                   and (kind == "artist" or normalized(artist_name(r)) == normalized(entity["artist"]))]
        if not matches:
            return {"status": "unresolved", "verifiedFields": {}, "reason": "no_exact_match"}
        if len(matches) != 1 or int(result.get("count", len(rows))) > 5:
            return {"status": "ambiguous", "verifiedFields": {}, "reason": "multiple_or_truncated_matches"}
        mbid = identifier(matches[0].get("id"))
        evidence = "exact_names_and_entity_lookup"
    details = client.get(kind, mbid, **({"inc": "artist-credits"} if kind != "artist" else {}))
    if not details.get("id"):
        return {"status": "unresolved", "verifiedFields": {}, "reason": "lookup_empty"}
    title = details.get("name" if kind == "artist" else "title")
    if ((entity.get("title") and normalized(title) != normalized(entity["title"])) or
            (kind != "artist" and entity.get("artist") and normalized(artist_name(details)) != normalized(entity["artist"]))):
        return {"status": "ambiguous", "verifiedFields": {}, "reason": "identifier_name_conflict"}
    fields = {"id": identifier(details["id"]), "title": title}
    if kind != "artist":
        fields["artists"] = [{"id": identifier(a["artist"]["id"]), "name": a["artist"].get("name")} for a in details.get("artist-credit", []) if isinstance(a, dict) and a.get("artist", {}).get("id")]
    for source, target in (("first-release-date", "firstReleaseDate"), ("date", "editionDate"), ("country", "country")):
        if details.get(source):
            fields[target] = details[source]
    return {"status": "verified", "kind": kind, "verifiedFields": fields, "evidence": evidence,
            "source": f"https://musicbrainz.org/{kind}/{fields['id']}",
            "recognition": "unknown", "relatedEntities": "not_resolved"}


def run_one(*, db=None, client_factory=Client, critical=playback_pending):
    db = db or get_db()
    if paused() or critical(db):
        return {"status": "deferred"}
    token = str(uuid.uuid4())
    with db.cursor() as cur:
        cur.execute(f"""SELECT j.principal,j.id,j.input FROM {table('metadata_jobs')} j
            JOIN {table('metadata_accounts')} a ON a.principal=j.principal
            WHERE j.not_before<=now() AND (j.status IN ('pending','deferred') OR (j.status='running' AND j.lease_until<now()))
            ORDER BY a.last_work,j.updated_at LIMIT 1 FOR UPDATE OF j SKIP LOCKED""")
        job = cur.fetchone()
        if not job:
            db.commit()
            return {"status": "current"}
        user, job_id, entity = job
        cur.execute(f"UPDATE {table('metadata_jobs')} SET status='running',lease=%s,lease_until=now()+interval '5 minutes',attempts=attempts+1 WHERE principal=%s AND id=%s", (token, user, job_id))
        cur.execute(f"UPDATE {table('metadata_accounts')} SET last_work=now() WHERE principal=%s", (user,))
    db.commit()
    def allowed():
        with db.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {table('metadata_jobs')} WHERE principal=%s AND id=%s AND lease=%s AND status='running' AND lease_until>now()", (user, job_id, token))
            current = bool(cur.fetchone())
        db.commit()
        return current and not paused() and not critical(db)
    try:
        result = resolve(entity, client_factory(db, allowed=allowed, reserve_request=lambda: reserve(db, user)))
        status, error, delay = result["status"], None, 0
    except MusicBrainzDeferred as exc:
        result, status, error, delay = None, "deferred", str(exc), exc.retry_seconds
    except Exception:
        db.rollback()
        result, status, error, delay = None, "failed", "metadata_lookup_failed", 0
    with db.cursor() as cur:
        cur.execute(f"""UPDATE {table('metadata_jobs')} SET status=%s,result=%s::jsonb,error=%s,
            not_before=now()+(%s*interval '1 second'),lease=NULL,lease_until=NULL,updated_at=now()
            WHERE principal=%s AND id=%s AND lease=%s AND status='running' AND lease_until>now()""",
            (status, json.dumps(result), error, delay, user, job_id, token))
        applied = cur.rowcount == 1
    db.commit()
    return {"status": status if applied else "superseded", "job_id": job_id}


def register_routes(bp):
    def invoke(work):
        user, db = principal(), get_db()
        try:
            request.max_content_length = 64000
            payload, status = work(db, user)
            db.commit()
        except (ValueError, TypeError, OverflowError):
            db.rollback()
            payload, status = {"error": "invalid_metadata_request"}, 400
        except Exception:
            db.rollback()
            raise
        response = jsonify(payload)
        response.status_code = status
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @bp.post("/api/music_metadata/prepare")
    def metadata_prepare():
        def prepare(db, user):
            body = request.get_json(silent=True)
            if not isinstance(body, dict) or set(body) != {"entities"}:
                raise ValueError("Invalid request envelope")
            return submit(db, user, body["entities"])
        return invoke(prepare)

    @bp.get("/api/music_metadata/status")
    def metadata_status():
        def read(db, user):
            job_id = request.args.get("id")
            if job_id:
                identifier(job_id)
            with db.cursor() as cur:
                cur.execute(f"SELECT id,status,result,error,input FROM {table('metadata_jobs')} WHERE principal=%s AND (%s IS NULL OR id=%s) ORDER BY updated_at DESC LIMIT 40", (user, job_id, job_id))
                jobs = [{"id": r[0], "status": r[1], "result": r[2], "error": r[3], "revisions": r[4].get("revisions", {})} for r in cur.fetchall()]
                cur.execute(f"SELECT CASE WHEN budget_day=(now() AT TIME ZONE 'UTC')::date THEN used ELSE 0 END FROM {table('metadata_accounts')} WHERE principal=%s", (user,))
                row = cur.fetchone()
            return {"schema_version": 1, "jobs": jobs, "remaining_requests": 80 - (row[0] if row else 0), "paused": paused()}, 200
        return invoke(read)

    @bp.post("/api/music_metadata/cancel")
    def metadata_cancel():
        def cancel(db, user):
            body = request.get_json(silent=True)
            job_id = identifier(body.get("id")) if isinstance(body, dict) else identifier(None)
            with db.cursor() as cur:
                cur.execute(f"UPDATE {table('metadata_jobs')} SET status='cancelled',lease=NULL,lease_until=NULL,updated_at=now() WHERE principal=%s AND id=%s", (user, job_id))
            return {"status": "cancelled"}, 200
        return invoke(cancel)
