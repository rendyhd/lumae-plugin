"""Optional, per-principal Living Collections storage and web manager."""

import json
import uuid
from datetime import date, datetime
from functools import wraps

from flask import g, jsonify, request

from plugin.api import get_db, get_setting, render_page, table

from .collection_library import register_collection_library_routes
from .collection_ui import render_collection_workbench


COLLECTIONS_SCHEMA_VERSION = 1
GLOBAL_PRINCIPAL = "__global__"


def collections_table():
    return table("collections")


def collection_items_table():
    return table("collection_items")


def collection_changes_table():
    return table("collection_changes")


def collection_mutations_table():
    return table("collection_mutations")


def collections_enabled():
    value = get_setting("collection_manager_enabled", False)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def current_principal():
    """JWT/session users are isolated; bearer-token installs share one library."""
    if getattr(g, "auth_method", None) == "bearer":
        return GLOBAL_PRINCIPAL
    username = getattr(g, "auth_user", None)
    return f"user:{username}" if username else GLOBAL_PRINCIPAL


def migrate_collections(db):
    cur = db.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {collections_table()} (
            principal TEXT NOT NULL,
            id TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            revision INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            deleted_at TIMESTAMPTZ,
            PRIMARY KEY (principal, id)
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {collection_items_table()} (
            principal TEXT NOT NULL,
            id TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('album', 'track')),
            track_id TEXT,
            provider_album_id TEXT,
            album_key TEXT,
            title TEXT,
            artist TEXT NOT NULL DEFAULT '',
            album TEXT,
            cover_item_id TEXT,
            position INTEGER NOT NULL DEFAULT 0,
            added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (principal, id),
            FOREIGN KEY (principal, collection_id)
                REFERENCES {collections_table()} (principal, id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {collection_changes_table()} (
            seq BIGSERIAL PRIMARY KEY,
            principal TEXT NOT NULL,
            collection_id TEXT NOT NULL,
            entity_kind TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {collection_mutations_table()} (
            principal TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            response_payload JSONB NOT NULL,
            status_code INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (principal, idempotency_key)
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS lumae_collections_changed_idx "
        f"ON {collection_changes_table()} (principal, seq)"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS lumae_collection_items_order_idx "
        f"ON {collection_items_table()} (principal, collection_id, kind, position)"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS lumae_collection_track_unique_idx "
        f"ON {collection_items_table()} (principal, collection_id, track_id) "
        "WHERE kind = 'track'"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS lumae_collection_album_provider_unique_idx "
        f"ON {collection_items_table()} (principal, collection_id, provider_album_id) "
        "WHERE kind = 'album' AND provider_album_id IS NOT NULL"
    )
    cur.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS lumae_collection_album_key_unique_idx "
        f"ON {collection_items_table()} (principal, collection_id, album_key) "
        "WHERE kind = 'album' AND provider_album_id IS NULL"
    )
    cur.close()


def _json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat().replace("+00:00", "Z")
    return value


def _row_dict(cur, row):
    if row is None:
        return None
    names = [column[0] for column in cur.description]
    return {name: _json_value(value) for name, value in zip(names, row)}


def _all_dicts(cur):
    rows = cur.fetchall()
    names = [column[0] for column in cur.description]
    return [
        {name: _json_value(value) for name, value in zip(names, row)}
        for row in rows
    ]


def _collection_select():
    return f"""
        SELECT c.id, c.name, c.description, c.revision,
               c.created_at, c.updated_at, c.deleted_at,
               COUNT(i.id) FILTER (WHERE i.kind = 'album')::INTEGER AS album_count,
               COUNT(i.id) FILTER (WHERE i.kind = 'track')::INTEGER AS track_count
          FROM {collections_table()} c
          LEFT JOIN {collection_items_table()} i
            ON i.principal = c.principal AND i.collection_id = c.id
    """


def _fetch_collection(cur, principal, collection_id, include_deleted=False):
    deleted_clause = "" if include_deleted else "AND c.deleted_at IS NULL"
    cur.execute(
        _collection_select()
        + f"""
         WHERE c.principal = %s AND c.id = %s {deleted_clause}
         GROUP BY c.principal, c.id
        """,
        (principal, collection_id),
    )
    return _row_dict(cur, cur.fetchone())


def _fetch_items(cur, principal, collection_id):
    cur.execute(
        f"""
        SELECT id, collection_id, kind, track_id, provider_album_id, album_key,
               title, artist, album, cover_item_id, position, added_at, updated_at
          FROM {collection_items_table()}
         WHERE principal = %s AND collection_id = %s
         ORDER BY kind, position, added_at
        """,
        (principal, collection_id),
    )
    return _all_dicts(cur)


def _record_change(cur, principal, collection_id, entity_kind, entity_id, operation, payload):
    cur.execute(
        f"""
        INSERT INTO {collection_changes_table()}
            (principal, collection_id, entity_kind, entity_id, operation, payload)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        """,
        (principal, collection_id, entity_kind, entity_id, operation, json.dumps(payload)),
    )


def _expected_revision(body):
    raw = request.headers.get("If-Match") or body.get("base_revision")
    if raw is None or str(raw).strip() in {"", "*"}:
        return None
    try:
        return int(str(raw).strip().strip('"'))
    except ValueError:
        return -1


def _error(message, status, **extra):
    return {"error": message, **extra}, status


def _mutation_response(handler):
    principal = current_principal()
    key = (request.headers.get("Idempotency-Key") or "").strip()[:200]
    if key:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            f"SELECT response_payload::text, status_code FROM {collection_mutations_table()} "
            "WHERE principal = %s AND idempotency_key = %s",
            (principal, key),
        )
        saved = cur.fetchone()
        cur.close()
        if saved:
            return jsonify(json.loads(saved[0])), saved[1]
    payload, status = handler(principal)
    # Cache only applied mutations. A 409 must be retryable with the same key
    # after the client explicitly rebases or chooses a conflict winner.
    if key and 200 <= status < 300:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            f"""
            INSERT INTO {collection_mutations_table()}
                (principal, idempotency_key, response_payload, status_code)
            VALUES (%s, %s, %s::jsonb, %s)
            ON CONFLICT (principal, idempotency_key) DO NOTHING
            """,
            (principal, key, json.dumps(payload), status),
        )
        cur.close()
        db.commit()
    return jsonify(payload), status


def require_collections_enabled(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not collections_enabled():
            return jsonify({"error": "collection_manager_disabled"}), 404
        return view(*args, **kwargs)

    return wrapped


def _clean_collection_body(body, partial=False):
    name = body.get("name")
    if not partial or name is not None:
        name = str(name or "").strip()
        if not name:
            raise ValueError("Collection name is required.")
        if len(name) > 120:
            raise ValueError("Collection name must be 120 characters or fewer.")
    description = body.get("description")
    if description is not None:
        description = str(description).strip() or None
        if description and len(description) > 1000:
            raise ValueError("Description must be 1,000 characters or fewer.")
    return name, description


def _normalize_item(raw):
    kind = str(raw.get("kind") or "").lower()
    if kind not in {"album", "track"}:
        raise ValueError("Item kind must be album or track.")
    track_id = str(raw.get("track_id") or "").strip() or None
    provider_album_id = str(raw.get("provider_album_id") or "").strip() or None
    album_key = str(raw.get("album_key") or "").strip() or None
    if kind == "track" and not track_id:
        raise ValueError("Track items require track_id.")
    if kind == "album" and not (provider_album_id or album_key):
        raise ValueError("Album items require provider_album_id or album_key.")
    return {
        "id": str(raw.get("id") or uuid.uuid4()),
        "kind": kind,
        "track_id": track_id if kind == "track" else None,
        "provider_album_id": provider_album_id if kind == "album" else None,
        "album_key": album_key if kind == "album" else None,
        "title": str(raw.get("title") or "").strip() or None,
        "artist": str(raw.get("artist") or "").strip(),
        "album": str(raw.get("album") or "").strip() or None,
        "cover_item_id": str(raw.get("cover_item_id") or "").strip() or None,
        "position": max(int(raw.get("position") or 0), 0),
    }


def _upsert_item(cur, principal, collection_id, item):
    if item["kind"] == "track":
        cur.execute(
            f"SELECT id FROM {collection_items_table()} "
            "WHERE principal = %s AND collection_id = %s AND kind = 'track' AND track_id = %s",
            (principal, collection_id, item["track_id"]),
        )
    elif item["provider_album_id"]:
        cur.execute(
            f"SELECT id FROM {collection_items_table()} "
            "WHERE principal = %s AND collection_id = %s AND kind = 'album' "
            "AND provider_album_id = %s",
            (principal, collection_id, item["provider_album_id"]),
        )
    else:
        cur.execute(
            f"SELECT id FROM {collection_items_table()} "
            "WHERE principal = %s AND collection_id = %s AND kind = 'album' "
            "AND provider_album_id IS NULL AND album_key = %s",
            (principal, collection_id, item["album_key"]),
        )
    existing = cur.fetchone()
    if existing:
        item["id"] = existing[0]
    cur.execute(
        f"""
        INSERT INTO {collection_items_table()}
            (principal, id, collection_id, kind, track_id, provider_album_id, album_key,
             title, artist, album, cover_item_id, position)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (principal, id) DO UPDATE SET
            kind = EXCLUDED.kind,
            track_id = EXCLUDED.track_id,
            provider_album_id = EXCLUDED.provider_album_id,
            album_key = EXCLUDED.album_key,
            title = EXCLUDED.title,
            artist = EXCLUDED.artist,
            album = EXCLUDED.album,
            cover_item_id = EXCLUDED.cover_item_id,
            position = EXCLUDED.position,
            updated_at = now()
        """,
        (
            principal,
            item["id"],
            collection_id,
            item["kind"],
            item["track_id"],
            item["provider_album_id"],
            item["album_key"],
            item["title"],
            item["artist"],
            item["album"],
            item["cover_item_id"],
            item["position"],
        ),
    )


def register_collection_routes(bp):
    register_collection_library_routes(bp, require_collections_enabled)

    @bp.get("/api/collections")
    @require_collections_enabled
    def collection_list():
        db = get_db()
        cur = db.cursor()
        cur.execute(
            _collection_select()
            + """
             WHERE c.principal = %s AND c.deleted_at IS NULL
             GROUP BY c.principal, c.id
             ORDER BY c.updated_at DESC, lower(c.name)
            """,
            (current_principal(),),
        )
        rows = _all_dicts(cur)
        cur.close()
        return jsonify({"schema_version": COLLECTIONS_SCHEMA_VERSION, "collections": rows})

    @bp.post("/api/collections")
    @require_collections_enabled
    def collection_create():
        body = request.get_json(silent=True) or {}

        def mutate(principal):
            try:
                name, description = _clean_collection_body(body)
            except ValueError as exc:
                return _error(str(exc), 400)
            collection_id = str(body.get("id") or uuid.uuid4())
            db = get_db()
            cur = db.cursor()
            cur.execute(
                f"""
                INSERT INTO {collections_table()} (principal, id, name, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (principal, id) DO NOTHING
                """,
                (principal, collection_id, name, description),
            )
            collection = _fetch_collection(cur, principal, collection_id)
            if collection and collection["revision"] == 1:
                _record_change(
                    cur, principal, collection_id, "collection", collection_id, "upsert", collection
                )
            cur.close()
            db.commit()
            return {"collection": collection}, 201

        return _mutation_response(mutate)

    @bp.get("/api/collections/<collection_id>")
    @require_collections_enabled
    def collection_detail(collection_id):
        principal = current_principal()
        db = get_db()
        cur = db.cursor()
        collection = _fetch_collection(cur, principal, collection_id)
        if not collection:
            cur.close()
            return jsonify({"error": "collection_not_found"}), 404
        items = _fetch_items(cur, principal, collection_id)
        cur.close()
        return jsonify({"collection": collection, "items": items})

    @bp.patch("/api/collections/<collection_id>")
    @require_collections_enabled
    def collection_update(collection_id):
        body = request.get_json(silent=True) or {}

        def mutate(principal):
            db = get_db()
            cur = db.cursor()
            current = _fetch_collection(cur, principal, collection_id)
            if not current:
                cur.close()
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                cur.close()
                return _error("revision_conflict", 409, current=current)
            try:
                name, description = _clean_collection_body(body, partial=True)
            except ValueError as exc:
                cur.close()
                return _error(str(exc), 400)
            name = current["name"] if name is None else name
            description = current["description"] if "description" not in body else description
            cur.execute(
                f"""
                UPDATE {collections_table()}
                   SET name = %s, description = %s, revision = revision + 1, updated_at = now()
                 WHERE principal = %s AND id = %s AND deleted_at IS NULL
                """,
                (name, description, principal, collection_id),
            )
            updated = _fetch_collection(cur, principal, collection_id)
            _record_change(
                cur, principal, collection_id, "collection", collection_id, "upsert", updated
            )
            cur.close()
            db.commit()
            return {"collection": updated}, 200

        return _mutation_response(mutate)

    @bp.delete("/api/collections/<collection_id>")
    @require_collections_enabled
    def collection_delete(collection_id):
        body = request.get_json(silent=True) or {}

        def mutate(principal):
            db = get_db()
            cur = db.cursor()
            current = _fetch_collection(cur, principal, collection_id)
            if not current:
                cur.close()
                return {"deleted": True}, 200
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                cur.close()
                return _error("revision_conflict", 409, current=current)
            cur.execute(
                f"""
                UPDATE {collections_table()}
                   SET deleted_at = now(), updated_at = now(), revision = revision + 1
                 WHERE principal = %s AND id = %s
                """,
                (principal, collection_id),
            )
            payload = {"id": collection_id, "revision": current["revision"] + 1}
            _record_change(
                cur, principal, collection_id, "collection", collection_id, "delete", payload
            )
            cur.close()
            db.commit()
            return {"deleted": True, **payload}, 200

        return _mutation_response(mutate)

    @bp.put("/api/collections/<collection_id>/items/<item_id>")
    @require_collections_enabled
    def collection_item_upsert(collection_id, item_id):
        body = request.get_json(silent=True) or {}
        body["id"] = item_id
        return _write_items(collection_id, [body], 200)

    @bp.post("/api/collections/<collection_id>/items/batch")
    @require_collections_enabled
    def collection_items_batch(collection_id):
        body = request.get_json(silent=True) or {}
        items = body.get("items") or []
        if not isinstance(items, list) or len(items) > 500:
            return jsonify({"error": "items must be a list of at most 500 entries"}), 400
        return _write_items(collection_id, items, 200)

    def _write_items(collection_id, raw_items, success_status):
        body = request.get_json(silent=True) or {}

        def mutate(principal):
            try:
                items = [_normalize_item(item) for item in raw_items]
            except (TypeError, ValueError) as exc:
                return _error(str(exc), 400)
            db = get_db()
            cur = db.cursor()
            current = _fetch_collection(cur, principal, collection_id)
            if not current:
                cur.close()
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                cur.close()
                return _error("revision_conflict", 409, current=current)
            for item in items:
                _upsert_item(cur, principal, collection_id, item)
            cur.execute(
                f"""
                UPDATE {collections_table()}
                   SET revision = revision + 1, updated_at = now()
                 WHERE principal = %s AND id = %s
                """,
                (principal, collection_id),
            )
            updated = _fetch_collection(cur, principal, collection_id)
            for item in items:
                change_payload = {
                    **item,
                    "collection_revision": updated["revision"],
                    "collection_updated_at": updated["updated_at"],
                }
                _record_change(
                    cur,
                    principal,
                    collection_id,
                    "item",
                    item["id"],
                    "upsert",
                    change_payload,
                )
            cur.close()
            db.commit()
            return {"collection": updated, "items": items}, success_status

        return _mutation_response(mutate)

    @bp.delete("/api/collections/<collection_id>/items/<item_id>")
    @require_collections_enabled
    def collection_item_delete(collection_id, item_id):
        body = request.get_json(silent=True) or {}

        def mutate(principal):
            db = get_db()
            cur = db.cursor()
            current = _fetch_collection(cur, principal, collection_id)
            if not current:
                cur.close()
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                cur.close()
                return _error("revision_conflict", 409, current=current)
            cur.execute(
                f"DELETE FROM {collection_items_table()} "
                "WHERE principal = %s AND collection_id = %s AND id = %s RETURNING id",
                (principal, collection_id, item_id),
            )
            removed = cur.fetchone() is not None
            if removed:
                cur.execute(
                    f"UPDATE {collections_table()} SET revision = revision + 1, updated_at = now() "
                    "WHERE principal = %s AND id = %s",
                    (principal, collection_id),
                )
            updated = _fetch_collection(cur, principal, collection_id)
            if removed:
                _record_change(
                    cur,
                    principal,
                    collection_id,
                    "item",
                    item_id,
                    "delete",
                    {
                        "id": item_id,
                        "collection_id": collection_id,
                        "collection_revision": updated["revision"],
                        "collection_updated_at": updated["updated_at"],
                    },
                )
            cur.close()
            db.commit()
            return {"deleted": removed, "collection": updated}, 200

        return _mutation_response(mutate)

    @bp.delete("/api/collections/<collection_id>/items/batch")
    @require_collections_enabled
    def collection_items_batch_delete(collection_id):
        body = request.get_json(silent=True) or {}
        raw_item_ids = body.get("item_ids")
        if not isinstance(raw_item_ids, list):
            return jsonify({"error": "item_ids must be a list"}), 400
        item_ids = list(dict.fromkeys(str(item_id) for item_id in raw_item_ids if item_id))
        if not item_ids or len(item_ids) > 500:
            return jsonify({"error": "item_ids must contain 1 to 500 entries"}), 400

        def mutate(principal):
            db = get_db()
            cur = db.cursor()
            current = _fetch_collection(cur, principal, collection_id)
            if not current:
                cur.close()
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                cur.close()
                return _error("revision_conflict", 409, current=current)
            cur.execute(
                f"DELETE FROM {collection_items_table()} "
                "WHERE principal = %s AND collection_id = %s AND id = ANY(%s) RETURNING id",
                (principal, collection_id, item_ids),
            )
            removed_ids = [str(row[0]) for row in cur.fetchall()]
            if removed_ids:
                cur.execute(
                    f"UPDATE {collections_table()} SET revision = revision + 1, updated_at = now() "
                    "WHERE principal = %s AND id = %s",
                    (principal, collection_id),
                )
            updated = _fetch_collection(cur, principal, collection_id)
            for removed_id in removed_ids:
                _record_change(
                    cur,
                    principal,
                    collection_id,
                    "item",
                    removed_id,
                    "delete",
                    {
                        "id": removed_id,
                        "collection_id": collection_id,
                        "collection_revision": updated["revision"],
                        "collection_updated_at": updated["updated_at"],
                    },
                )
            cur.close()
            db.commit()
            return {
                "deleted": removed_ids,
                "deleted_count": len(removed_ids),
                "collection": updated,
            }, 200

        return _mutation_response(mutate)

    @bp.get("/api/collections/changes")
    @require_collections_enabled
    def collection_changes():
        try:
            cursor = max(int(request.args.get("cursor", 0)), 0)
            limit = min(max(int(request.args.get("limit", 200)), 1), 500)
        except ValueError:
            return jsonify({"error": "invalid_cursor"}), 400
        db = get_db()
        cur = db.cursor()
        cur.execute(
            f"""
            SELECT seq, collection_id, entity_kind, entity_id, operation,
                   payload, created_at
              FROM {collection_changes_table()}
             WHERE principal = %s AND seq > %s
             ORDER BY seq ASC LIMIT %s
            """,
            (current_principal(), cursor, limit),
        )
        changes = _all_dicts(cur)
        cur.close()
        for change in changes:
            if isinstance(change.get("payload"), str):
                change["payload"] = json.loads(change["payload"])
        next_cursor = changes[-1]["seq"] if changes else cursor
        return jsonify({"changes": changes, "next_cursor": next_cursor})

    @bp.get("/api/collections/search")
    @require_collections_enabled
    def collection_search():
        query = str(request.args.get("q") or "").strip()
        kind = str(request.args.get("kind") or "track").lower()
        if len(query) < 2 or kind not in {"album", "track"}:
            return jsonify({"results": []})
        like = f"%{query}%"
        db = get_db()
        cur = db.cursor()
        if kind == "album":
            cur.execute(
                """
                SELECT MIN(item_id) AS cover_item_id, album,
                       COALESCE(NULLIF(album_artist, ''), author) AS artist,
                       COUNT(*)::INTEGER AS track_count
                  FROM score
                 WHERE album IS NOT NULL
                   AND (album ILIKE %s OR album_artist ILIKE %s OR author ILIKE %s)
                 GROUP BY album, COALESCE(NULLIF(album_artist, ''), author)
                 ORDER BY lower(album) LIMIT 50
                """,
                (like, like, like),
            )
            results = _all_dicts(cur)
            for row in results:
                album_title = row.pop("album")
                row.update(
                    {
                        "kind": "album",
                        "title": album_title,
                        "album_key": f"{str(row['artist']).lower()}::{str(album_title).lower()}",
                    }
                )
        else:
            cur.execute(
                """
                SELECT item_id AS track_id, title, author AS artist, album,
                       item_id AS cover_item_id
                  FROM score
                 WHERE title ILIKE %s OR author ILIKE %s OR album ILIKE %s
                 ORDER BY lower(title) LIMIT 50
                """,
                (like, like, like),
            )
            results = _all_dicts(cur)
            for row in results:
                row["kind"] = "track"
        cur.close()
        return jsonify({"results": results})

    @bp.get("/collections")
    @require_collections_enabled
    def collection_manager_page():
        return render_collections_manager()


def render_collections_settings_panel():
    enabled = collections_enabled()
    checked = "checked" if enabled else ""
    location = (
        '<span class="lumae-help">Enabled. Open <strong>Living Collections</strong> from the Plugins menu.</span>'
        if enabled
        else '<span class="lumae-help">Enable and save to add Living Collections to the Plugins menu.</span>'
    )
    return f"""
      <section class="lumae-panel" aria-label="Living Collections">
        <h3>Living Collections</h3>
        <p class="lumae-help">Manage mixed album-and-track collections in AudioMuse and sync them with Lumae. Turning this off hides the manager and API without deleting anything.</p>
        <form class="lumae-form" method="post">
          <label class="lumae-toggle">
            <input type="checkbox" name="collection_manager_enabled" {checked}>
            <span>Enable the collection manager</span>
          </label>
          <div class="lumae-actions">
            <button class="lumae-button-secondary" type="submit" name="action" value="save_collections">Save collection setting</button>
            {location}
          </div>
        </form>
      </section>
    """

def render_collections_manager():
    principal_label = (
        "Shared bearer-token library"
        if current_principal() == GLOBAL_PRINCIPAL
        else "Your library"
    )
    return render_page(
        render_collection_workbench(principal_label),
        title="Living Collections",
    )
