"""Optional, per-principal Living Collections storage and web manager."""

import json
import uuid
from datetime import date, datetime
from functools import wraps
from html import escape

from flask import g, jsonify, request

from plugin.api import get_db, get_setting, render_page, table


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
    manage = (
        '<a class="lumae-button lumae-button-primary" href="collections">Manage collections</a>'
        if enabled
        else '<span class="lumae-help">Enable and save to open the collection manager.</span>'
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
            {manage}
          </div>
        </form>
      </section>
    """


def render_collections_manager():
    principal_label = "Shared bearer-token library" if current_principal() == GLOBAL_PRINCIPAL else "Your library"
    return render_page(
        f"""
<style>
  .collections-app{{--ink:#efeae0;--muted:#9d9689;--gold:#d4a54a;--bg:#15130f;--panel:#1f1c17;--line:#3b352b;color:var(--ink);background:var(--bg);border-radius:16px;min-height:70vh;overflow:hidden}}
  .collections-top{{display:flex;align-items:center;justify-content:space-between;padding:20px 22px;border-bottom:1px solid var(--line)}}
  .collections-top h2,.collection-detail h2{{margin:0}} .collections-top p,.muted{{color:var(--muted);margin:4px 0 0}}
  .collections-grid{{display:grid;grid-template-columns:minmax(230px,30%) 1fr;min-height:620px}}
  .collection-list{{border-right:1px solid var(--line);padding:14px;display:grid;align-content:start;gap:8px}}
  .collection-button,.item-row{{width:100%;text-align:left;background:transparent;color:inherit;border:0;border-radius:10px;padding:12px;cursor:pointer}}
  .collection-button:hover,.collection-button.active,.item-row:hover{{background:#2a251e}}
  .collection-button strong,.collection-button span{{display:block}} .collection-button span{{color:var(--muted);font-size:.82rem;margin-top:4px}}
  .collection-detail{{padding:clamp(18px,4vw,42px);min-width:0}} .detail-head{{display:flex;gap:18px;align-items:flex-start;justify-content:space-between}}
  .mosaic{{width:112px;height:112px;border-radius:14px;background:linear-gradient(135deg,#765d2e,#2b251b);display:grid;place-items:center;color:#f6d894;font-size:2rem;flex:none}}
  .detail-copy{{flex:1;min-width:0}} .detail-actions,.toolbar{{display:flex;gap:8px;flex-wrap:wrap}}
  button,.button{{min-height:42px;border-radius:9px;border:1px solid var(--line);background:#29241d;color:var(--ink);padding:9px 14px;font-weight:700;cursor:pointer}}
  button.primary,.button.primary{{background:var(--gold);border-color:var(--gold);color:#1a160f}} button.danger{{color:#ffb4a8}}
  .section{{margin-top:32px}} .section-title{{display:flex;justify-content:space-between;align-items:baseline;border-bottom:1px solid var(--line);padding-bottom:10px}}
  .section-title h3{{margin:0}} .item-row{{display:flex;gap:12px;align-items:center;border-bottom:1px solid #2d2821}}
  .item-art{{width:48px;height:48px;border-radius:7px;background:linear-gradient(135deg,#66512b,#29231a);display:grid;place-items:center;color:var(--gold);flex:none}}
  .item-copy{{flex:1;min-width:0}} .item-copy strong,.item-copy span{{display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}} .item-copy span{{color:var(--muted);font-size:.84rem;margin-top:3px}}
  .item-controls{{display:flex;gap:4px;align-items:center}} .item-controls button{{min-width:42px;padding:7px}} button:disabled{{opacity:.38;cursor:not-allowed}}
  dialog{{width:min(720px,calc(100vw - 24px));border:1px solid var(--line);border-radius:16px;background:var(--panel);color:var(--ink);padding:0}} dialog::backdrop{{background:#000a}}
  .dialog-head,.dialog-foot{{display:flex;align-items:center;justify-content:space-between;padding:16px;border-bottom:1px solid var(--line)}} .dialog-foot{{border:0;border-top:1px solid var(--line)}}
  .dialog-body{{padding:16px}} input,textarea{{width:100%;box-sizing:border-box;background:#17140f;color:var(--ink);border:1px solid var(--line);border-radius:9px;padding:11px;font:inherit}}
  .tabs{{display:flex;gap:8px;margin-bottom:12px}} .search-results{{max-height:45vh;overflow:auto;margin-top:12px}} .search-choice{{display:flex;gap:12px;align-items:center;padding:10px;border-bottom:1px solid var(--line)}} .search-choice input{{width:auto}}
  .empty{{color:var(--muted);padding:36px 10px;text-align:center}} .mobile-back{{display:none}}
  .undo-toast{{position:fixed;z-index:20;left:50%;bottom:24px;transform:translateX(-50%);width:min(440px,calc(100vw - 28px));display:flex;align-items:center;justify-content:space-between;gap:12px;background:#2b251d;border:1px solid var(--line);border-radius:12px;padding:12px 14px;box-shadow:0 14px 40px #0008}} .undo-toast[hidden]{{display:none}}
  @media(max-width:700px){{.collections-grid{{display:block}}.collection-list{{border:0}}.collection-detail{{display:none}}.collections-app.detail-open .collection-list{{display:none}}.collections-app.detail-open .collection-detail{{display:block}}.mobile-back{{display:inline-flex}}.detail-head{{flex-wrap:wrap}}.mosaic{{width:88px;height:88px}}}}
</style>
<section class="collections-app" id="collections-app">
  <header class="collections-top"><div><h2>Living Collections</h2><p>{escape(principal_label)}</p></div><button class="primary" id="new-collection">New collection</button></header>
  <div class="collections-grid">
    <aside class="collection-list" id="collection-list"><p class="empty">Loading collections…</p></aside>
    <main class="collection-detail" id="collection-detail"><p class="empty">Choose a collection, or make a new one.</p></main>
  </div>
</section>
<dialog id="edit-dialog"><form method="dialog"><header class="dialog-head"><strong id="edit-title">New collection</strong><button value="cancel" aria-label="Close">×</button></header><div class="dialog-body"><label>Name<input id="collection-name" maxlength="120" required></label><label>Description<textarea id="collection-description" maxlength="1000" rows="3"></textarea></label></div><footer class="dialog-foot"><span></span><button class="primary" id="save-collection" value="cancel">Save</button></footer></form></dialog>
<dialog id="add-dialog"><header class="dialog-head"><strong>Add music</strong><button id="close-add" aria-label="Close">×</button></header><div class="dialog-body"><div class="tabs"><button data-kind="album" class="primary">Albums</button><button data-kind="track">Tracks</button></div><input id="music-search" placeholder="Search albums and tracks" autocomplete="off"><div class="search-results" id="search-results"><p class="empty">Search your analyzed library.</p></div></div><footer class="dialog-foot"><span id="selected-count">0 selected</span><button class="primary" id="add-selected" disabled>Add selected</button></footer></dialog>
<div class="undo-toast" id="undo-toast" role="status" hidden><span id="undo-label">Item removed</span><button id="undo-remove">Undo</button></div>
<script>
(()=>{{
const api='api/collections', app=document.getElementById('collections-app'), list=document.getElementById('collection-list'), detail=document.getElementById('collection-detail');
const edit=document.getElementById('edit-dialog'), add=document.getElementById('add-dialog'),undoToast=document.getElementById('undo-toast'); let collections=[],current=null,items=[],editMode='new',kind='album',results=[],selected=new Set(),timer,undoItem=null,undoTimer;
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
async function req(url,opts={{}}){{const r=await fetch(url,{{headers:{{'Content-Type':'application/json',...(opts.headers||{{}})}},...opts}});const b=await r.json();if(!r.ok)throw Object.assign(new Error(b.error||'Request failed'),{{body:b,status:r.status}});return b}}
function initials(s){{return String(s||'?').split(/\\s+/).slice(0,2).map(x=>x[0]).join('').toUpperCase()}}
async function loadList(selectId){{const b=await req(api);collections=b.collections;list.innerHTML=collections.length?collections.map(c=>`<button class="collection-button ${{current?.id===c.id?'active':''}}" data-id="${{esc(c.id)}}"><strong>${{esc(c.name)}}</strong><span>${{c.album_count}} albums · ${{c.track_count}} tracks</span></button>`).join(''):'<p class="empty">No collections yet.<br>Create one for records, tracks, or both.</p>';list.querySelectorAll('[data-id]').forEach(x=>x.onclick=()=>openCollection(x.dataset.id));if(selectId)await openCollection(selectId)}}
async function openCollection(id){{const b=await req(`${{api}}/${{encodeURIComponent(id)}}`);current=b.collection;items=b.items;app.classList.add('detail-open');renderDetail();await loadList()}}
function renderSection(itemKind,title){{const rows=items.filter(i=>i.kind===itemKind);return `<section class="section"><div class="section-title"><h3>${{title}}</h3><span class="muted">${{rows.length}}</span></div>${{rows.length?rows.map((i,n)=>`<div class="item-row"><div class="item-art">${{esc(initials(i.title))}}</div><div class="item-copy"><strong>${{esc(i.title||'Untitled')}}</strong><span>${{esc(i.artist)}}${{i.album?' · '+esc(i.album):''}}</span></div><div class="item-controls"><button data-move="${{esc(i.id)}}" data-direction="-1" aria-label="Move ${{esc(i.title)}} up" ${{n===0?'disabled':''}}>↑</button><button data-move="${{esc(i.id)}}" data-direction="1" aria-label="Move ${{esc(i.title)}} down" ${{n===rows.length-1?'disabled':''}}>↓</button><button data-remove="${{esc(i.id)}}" aria-label="Remove ${{esc(i.title)}}">Remove</button></div></div>`).join(''):`<p class="empty">No ${{title.toLowerCase()}} yet.</p>`}}</section>`}}
function renderDetail(){{detail.innerHTML=`<button class="mobile-back" id="mobile-back">← Collections</button><div class="detail-head"><div class="mosaic">${{esc(initials(current.name))}}</div><div class="detail-copy"><p class="muted">COLLECTION</p><h2>${{esc(current.name)}}</h2><p class="muted">${{esc(current.description||'Albums and tracks, kept together.')}}</p><div class="toolbar"><button class="primary" id="add-music">Add music</button><button id="edit-collection">Edit</button><button id="duplicate-collection">Duplicate</button><button class="danger" id="delete-collection">Delete</button></div></div></div>${{renderSection('album','Albums')}}${{renderSection('track','Tracks')}}`;document.getElementById('mobile-back').onclick=()=>app.classList.remove('detail-open');document.getElementById('add-music').onclick=openAdd;document.getElementById('edit-collection').onclick=()=>openEdit('edit');document.getElementById('duplicate-collection').onclick=duplicateCollection;document.getElementById('delete-collection').onclick=deleteCollection;detail.querySelectorAll('[data-remove]').forEach(x=>x.onclick=()=>removeItem(x.dataset.remove));detail.querySelectorAll('[data-move]').forEach(x=>x.onclick=()=>moveItem(x.dataset.move,Number(x.dataset.direction)))}}
function openEdit(mode){{editMode=mode;document.getElementById('edit-title').textContent=mode==='new'?'New collection':'Edit collection';document.getElementById('collection-name').value=mode==='edit'?current.name:'';document.getElementById('collection-description').value=mode==='edit'?(current.description||''):'';edit.showModal();setTimeout(()=>document.getElementById('collection-name').focus(),0)}}
async function saveCollection(e){{e.preventDefault();const payload={{name:document.getElementById('collection-name').value,description:document.getElementById('collection-description').value}};try{{const b=editMode==='new'?await req(api,{{method:'POST',body:JSON.stringify(payload)}}):await req(`${{api}}/${{current.id}}`,{{method:'PATCH',headers:{{'If-Match':String(current.revision)}},body:JSON.stringify(payload)}});edit.close();await loadList(b.collection.id)}}catch(err){{alert(err.status===409?'This collection changed elsewhere. Reloading it.':err.message);if(err.status===409)await openCollection(current.id)}}}}
async function deleteCollection(){{if(!confirm(`Delete “${{current.name}}”? This cannot be undone.`))return;await req(`${{api}}/${{current.id}}`,{{method:'DELETE',headers:{{'If-Match':String(current.revision)}}}});current=null;items=[];app.classList.remove('detail-open');detail.innerHTML='<p class="empty">Choose a collection, or make a new one.</p>';await loadList()}}
async function duplicateCollection(){{const created=await req(api,{{method:'POST',body:JSON.stringify({{name:`${{current.name}} copy`,description:current.description}})}});if(items.length)await req(`${{api}}/${{created.collection.id}}/items/batch`,{{method:'POST',headers:{{'If-Match':'1'}},body:JSON.stringify({{items}})}});await loadList(created.collection.id)}}
async function moveItem(id,direction){{const moving=items.find(i=>i.id===id),rows=items.filter(i=>i.kind===moving?.kind),index=rows.findIndex(i=>i.id===id),target=index+direction;if(!moving||target<0||target>=rows.length)return;[rows[index],rows[target]]=[rows[target],rows[index]];const payload=rows.map((i,n)=>({{...i,position:n}}));await req(`${{api}}/${{current.id}}/items/batch`,{{method:'POST',headers:{{'If-Match':String(current.revision)}},body:JSON.stringify({{items:payload}})}});await openCollection(current.id)}}
function showUndo(item){{undoItem=item;document.getElementById('undo-label').textContent=`Removed ${{item.title||'item'}}`;undoToast.hidden=false;clearTimeout(undoTimer);undoTimer=setTimeout(()=>{{undoToast.hidden=true;undoItem=null}},7000)}}
async function removeItem(id){{const removed=items.find(i=>i.id===id);await req(`${{api}}/${{current.id}}/items/${{id}}`,{{method:'DELETE',headers:{{'If-Match':String(current.revision)}}}});await openCollection(current.id);if(removed)showUndo(removed)}}
async function undoRemove(){{if(!undoItem)return;const item=undoItem;undoItem=null;undoToast.hidden=true;clearTimeout(undoTimer);await req(`${{api}}/${{current.id}}/items/${{item.id}}`,{{method:'PUT',headers:{{'If-Match':String(current.revision)}},body:JSON.stringify(item)}});await openCollection(current.id)}}
function openAdd(){{kind='album';selected.clear();results=[];document.getElementById('music-search').value='';document.getElementById('search-results').innerHTML='<p class="empty">Search your analyzed library.</p>';updateSelected();setTab();add.showModal()}}
function setTab(){{document.querySelectorAll('[data-kind]').forEach(x=>x.classList.toggle('primary',x.dataset.kind===kind))}}
async function search(){{const q=document.getElementById('music-search').value.trim();if(q.length<2)return;const b=await req(`${{api}}/search?q=${{encodeURIComponent(q)}}&kind=${{kind}}`);results=b.results;selected.clear();document.getElementById('search-results').innerHTML=results.length?results.map((r,n)=>`<label class="search-choice"><input type="checkbox" data-result="${{n}}"><span><strong>${{esc(r.title)}}</strong><br><span class="muted">${{esc(r.artist)}}${{r.album?' · '+esc(r.album):''}}</span></span></label>`).join(''):'<p class="empty">No matches.</p>';document.querySelectorAll('[data-result]').forEach(x=>x.onchange=()=>{{x.checked?selected.add(Number(x.dataset.result)):selected.delete(Number(x.dataset.result));updateSelected()}});updateSelected()}}
function updateSelected(){{document.getElementById('selected-count').textContent=`${{selected.size}} selected`;document.getElementById('add-selected').disabled=!selected.size}}
async function addSelected(){{const chosen=[...selected].map(n=>results[n]);await req(`${{api}}/${{current.id}}/items/batch`,{{method:'POST',headers:{{'If-Match':String(current.revision)}},body:JSON.stringify({{items:chosen}})}});add.close();await openCollection(current.id)}}
document.getElementById('new-collection').onclick=()=>openEdit('new');document.getElementById('save-collection').onclick=saveCollection;document.getElementById('close-add').onclick=()=>add.close();document.getElementById('music-search').oninput=()=>{{clearTimeout(timer);timer=setTimeout(search,250)}};document.querySelectorAll('[data-kind]').forEach(x=>x.onclick=()=>{{kind=x.dataset.kind;setTab();search()}});document.getElementById('add-selected').onclick=addSelected;document.getElementById('undo-remove').onclick=undoRemove;loadList().catch(e=>list.innerHTML=`<p class="empty">${{esc(e.message)}}</p>`);
}})();
</script>
        """,
        title="Living Collections",
    )
