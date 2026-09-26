"""Optional, per-principal Living Collections storage and web manager."""

import hashlib
import json
import re
import threading
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from functools import wraps

import psycopg2
from flask import Response, abort, current_app, g, jsonify, request

from plugin.api import config, get_db, get_setting, render_page, table

from . import migrations
from .collection_library import catalog_track_view_sql, register_collection_library_routes
from .collection_ui import render_collection_workbench


COLLECTIONS_SCHEMA_VERSION = 1
COLLECTIONS_BACKUP_FORMAT = "lumae-living-collections"
COLLECTIONS_BACKUP_VERSION = 1
MAX_BACKUP_COLLECTIONS = 2_000
MAX_BACKUP_ITEMS_PER_COLLECTION = 50_000
MAX_BACKUP_ITEMS = 100_000
GLOBAL_PRINCIPAL = "__global__"
FINGERPRINT_VERSION = 1
FEED_PROTOCOL_VERSION = 1
# K9: a request that sends this header with the value "2" opts in to
# contract 2 (conflicts answer 409 with the server's state instead of being
# absorbed). Health advertises the highest contract as collections.contract.
COLLECTIONS_CONTRACT = 2
CONTRACT_HEADER = "X-Lumae-Collections-Contract"
# Every mutation transaction waits at most this long for any one lock (the
# idempotency key, the collection row, the feed head); then it answers 503
# collection_busy with Retry-After instead of queueing indefinitely.
MUTATION_LOCK_TIMEOUT = "3s"
LOCK_NOT_AVAILABLE = "55P03"
BUSY_RETRY_AFTER_S = 5
# A restore commits in transactions of at most this many rows: one per
# collection it creates plus one per item. Each transaction appends its events
# as one block, so the feed head is held only for that block's insert.
RESTORE_CHUNK_ROWS = 2_000
# The K8 snapshot reads on its own REPEATABLE READ connection.
SNAPSHOT_APPLICATION_NAME = "lumae-collections-snapshot"
SNAPSHOT_CONNECT_TIMEOUT_S = 5
SNAPSHOT_STATEMENT_TIMEOUT_MS = 30_000
SNAPSHOT_LOCK_TIMEOUT_MS = 5_000
UNAVAILABLE_RETRY_AFTER_S = 5
# One snapshot is built at a time per web worker process (100k items take
# about 250 MB while the JSON is built). Another waits this long, then 503s.
SNAPSHOT_WAIT_S = 2
_SNAPSHOT_SLOT = threading.BoundedSemaphore(1)


class FeedProtocolUnavailable(RuntimeError):
    """The committed collection feed frontier is absent or incompatible."""


class FeedInvariantViolation(RuntimeError):
    """A committed change row sits past the feed head (AUD-05).

    Writing would allocate a seq that already exists, so every collection
    write fails closed with 503 ``collection_feed_invariant`` until the head
    is realigned (docs/runbooks/UPGRADE_1.3.md).
    """


def collections_table():
    return table("collections")


def collection_items_table():
    return table("collection_items")


def collection_changes_table():
    return table("collection_changes")


def collection_mutations_table():
    return table("collection_mutations")


def collection_feed_state_table():
    return table("collection_feed_state")


def collection_restores_table():
    return table("collection_restores")


def collections_enabled():
    value = get_setting("collection_manager_enabled", False)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _resolve_principal():
    """The request's principal, or None when its host auth method is unknown.

    Only the host's own methods name a principal: ``session`` (a user),
    ``bearer`` (the installation token, shared) and no method at all (auth
    disabled, shared). Anything else, such as a plugin-scoped token, names
    none rather than a default principal.
    """
    method = getattr(g, "auth_method", None)
    if method == "bearer":
        return GLOBAL_PRINCIPAL
    if method not in (None, "session"):
        return None
    username = getattr(g, "auth_user", None)
    if username:
        return f"user:{username}"
    if method == "session":
        # Fail closed if host authentication ever presents a malformed session.
        # Falling back to the shared bearer principal here would expose another
        # account's collections.
        abort(401)
    return GLOBAL_PRINCIPAL


def current_principal():
    """JWT/session users are isolated; bearer-token installs share one library.

    An unknown host auth method is denied (401), never mapped to a default
    principal.
    """
    principal = _resolve_principal()
    if principal is None:
        abort(401)
    return principal


def health_scope_mode():
    """The collections ``scope`` for health: "shared", "personal", or None when
    the host auth method names no principal (the collection, shelf and
    discovery routes answer 401 then). Health keeps answering 200 for such a
    caller; a malformed session still aborts with 401, as in 1.2.5.
    """
    principal = _resolve_principal()
    if principal is None:
        return None
    return current_collection_scope()["mode"]


def contract_v2():
    """The request opted in to collections contract 2 (K9)."""
    return (request.headers.get(CONTRACT_HEADER) or "").strip() == str(COLLECTIONS_CONTRACT)


def current_collection_scope():
    """Return a credential-free description suitable for APIs and UI copy."""
    if current_principal() == GLOBAL_PRINCIPAL:
        return {
            "mode": "shared",
            "label": "Shared bearer-token library",
            "detail": "Everyone using the AudioMuse installation token sees these collections.",
        }
    username = str(getattr(g, "auth_user", "") or "").strip()
    return {
        "mode": "personal",
        "label": f"Personal library · {username}" if username else "Personal library",
        "detail": "Only this signed-in AudioMuse user can see these collections.",
    }


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
    # Bound the seed without changing timeouts for the host's later migrations.
    # The change-table lock itself lasts through the host's outer commit.
    cur.execute("SELECT current_setting('lock_timeout'), current_setting('statement_timeout')")
    prior_timeouts = cur.fetchone()
    cur.execute("SET LOCAL statement_timeout = '30s'")
    if not _feed_fence_installed(cur):
        # Seeding the frontier needs the change table to itself. The lock
        # waits DDL_LOCK_TIMEOUT per attempt, with bounded retries (P2-5).
        migrations.lock_table(cur, collection_changes_table())
        # AUD-05 fence: 1.2.5 writers insert without seq and would take the
        # BIGSERIAL default, a number the frontier later allocates again.
        # Without a default their insert fails instead. Idempotent; the
        # sequence stays owned by the column.
        migrations.ensure_no_default(cur, collection_changes_table(), "seq")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {collection_feed_state_table()} (
            singleton SMALLINT PRIMARY KEY CHECK (singleton = 1),
            protocol_version INTEGER NOT NULL,
            epoch UUID NOT NULL,
            head_seq BIGINT NOT NULL CHECK (head_seq >= 0),
            floor_seq BIGINT NOT NULL
        )
        """
    )
    # K8 floor_seq: the head when this epoch's feed was cut over (seeded, or
    # upgraded to 1.3.0). Nothing at or below it is guaranteed to stay in the
    # journal. An upgraded row gets the head it has now, once.
    migrations.ensure_columns(cur, collection_feed_state_table(), "floor_seq BIGINT")
    cur.execute(
        f"INSERT INTO {collection_feed_state_table()} "
        f"(singleton, protocol_version, epoch, head_seq, floor_seq) "
        f"SELECT 1, %s, gen_random_uuid(), head, head "
        f"FROM (SELECT COALESCE(MAX(seq), 0) AS head FROM {collection_changes_table()}) seeded "
        f"ON CONFLICT (singleton) DO NOTHING",
        (FEED_PROTOCOL_VERSION,),
    )
    cur.execute(
        f"UPDATE {collection_feed_state_table()} SET floor_seq = head_seq "
        "WHERE singleton = 1 AND floor_seq IS NULL"
    )
    migrations.ensure_not_null(cur, collection_feed_state_table(), "floor_seq")
    cur.execute(
        f"SELECT protocol_version FROM {collection_feed_state_table()} WHERE singleton = 1"
    )
    state = cur.fetchone()
    if state is None or state[0] != FEED_PROTOCOL_VERSION:
        raise FeedProtocolUnavailable("unknown collection feed protocol")
    cur.execute(
        "SELECT set_config('lock_timeout', %s, true), "
        "set_config('statement_timeout', %s, true)",
        prior_timeouts,
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {collection_mutations_table()} (
            principal TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            response_payload JSONB NOT NULL,
            status_code INTEGER NOT NULL,
            request_fingerprint TEXT,
            fingerprint_version INTEGER,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (principal, idempotency_key)
        )
        """
    )
    # collection_id (1.3.0, K9): the collection a receipt's request applied
    # to, so a key conflict can answer with its current state. NULL for a
    # restore and for receipts written before 1.3.0.
    migrations.ensure_columns(
        cur, collection_mutations_table(),
        "request_fingerprint TEXT",
        "fingerprint_version INTEGER",
        "collection_id TEXT",
    )
    # Progress of a keyed restore that spans several transactions. The row
    # lives from its first chunk until the chunk that stores the receipt.
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {collection_restores_table()} (
            principal TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            request_fingerprint TEXT NOT NULL,
            restore_id UUID NOT NULL,
            chunk_rows INTEGER NOT NULL CHECK (chunk_rows > 0),
            chunk_count INTEGER NOT NULL CHECK (chunk_count > 1),
            chunks_done INTEGER NOT NULL CHECK (chunks_done >= 0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (principal, idempotency_key)
        )
        """
    )
    migrations.ensure_constraint(
        cur, collection_mutations_table(),
        "lumae_collection_mutation_fingerprint_pair",
        "CHECK ((request_fingerprint IS NULL AND fingerprint_version IS NULL) OR "
        "(request_fingerprint IS NOT NULL AND fingerprint_version IS NOT NULL "
        "AND fingerprint_version > 0))",
    )
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS lumae_collections_changed_idx "
        f"ON {collection_changes_table()} (principal, seq)",
    )
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS lumae_collection_items_order_idx "
        f"ON {collection_items_table()} (principal, collection_id, kind, position)",
    )
    migrations.ensure_index(
        cur,
        f"CREATE UNIQUE INDEX IF NOT EXISTS lumae_collection_track_unique_idx "
        f"ON {collection_items_table()} (principal, collection_id, track_id) "
        "WHERE kind = 'track'",
    )
    migrations.ensure_index(
        cur,
        f"CREATE UNIQUE INDEX IF NOT EXISTS lumae_collection_album_provider_unique_idx "
        f"ON {collection_items_table()} (principal, collection_id, provider_album_id) "
        "WHERE kind = 'album' AND provider_album_id IS NOT NULL",
    )
    migrations.ensure_index(
        cur,
        f"CREATE UNIQUE INDEX IF NOT EXISTS lumae_collection_album_key_unique_idx "
        f"ON {collection_items_table()} (principal, collection_id, album_key) "
        "WHERE kind = 'album' AND provider_album_id IS NULL",
    )
    # Library search folds accents with unaccent when it can be installed;
    # without it, search still works, accent-sensitively (collection_library).
    migrations.ensure_extension(cur, "unaccent")
    cur.close()


def _feed_fence_installed(cur):
    """The seq default is gone and the feed frontier is seeded (AUD-05).

    Then the migration has nothing to change on the change table and takes
    no lock on it: 1.2.5 writers already fail without a default, and 1.3.0
    writers allocate from the frontier.
    """
    seq = migrations.column_info(cur, collection_changes_table(), "seq")
    if seq is None or seq[1] is not None:
        return False
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (collection_feed_state_table(),))
    row = cur.fetchone()
    if not row or row[0] is not True:
        return False
    cur.execute(f"SELECT 1 FROM {collection_feed_state_table()} WHERE singleton = 1")
    return cur.fetchone() is not None


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


def _fetch_collections(cur, principal, collection_ids):
    """Collection objects by id, tombstones included, in one query."""
    if not collection_ids:
        return {}
    cur.execute(
        _collection_select()
        + """
         WHERE c.principal = %s AND c.id = ANY(%s)
         GROUP BY c.principal, c.id
        """,
        (principal, list(collection_ids)),
    )
    return {row["id"]: row for row in _all_dicts(cur)}


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


def _backup_checksum(collections):
    encoded = json.dumps(
        collections,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _backup_envelope(collections, scope_mode, exported_at=None):
    exported_at = exported_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    item_count = sum(len(collection.get("items") or []) for collection in collections)
    return {
        "format": COLLECTIONS_BACKUP_FORMAT,
        "version": COLLECTIONS_BACKUP_VERSION,
        "exported_at": exported_at,
        "scope": scope_mode,
        "collection_count": len(collections),
        "item_count": item_count,
        "collections": collections,
        "checksum": _backup_checksum(collections),
    }


def _export_principal_collections(principal, collection_id=None):
    """Read only active collections belonging to one authenticated principal."""
    db = get_db()
    cur = db.cursor()
    if collection_id is not None:
        rows = [_fetch_collection(cur, principal, collection_id)]
        rows = [row for row in rows if row is not None]
    else:
        cur.execute(
            _collection_select()
            + """
             WHERE c.principal = %s AND c.deleted_at IS NULL
             GROUP BY c.principal, c.id
             ORDER BY c.created_at ASC, lower(c.name)
            """,
            (principal,),
        )
        rows = _all_dicts(cur)

    collections = []
    for row in rows:
        collections.append(
            {
                "id": row["id"],
                "name": row["name"],
                "description": row.get("description"),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "items": _fetch_items(cur, principal, row["id"]),
            }
        )
    cur.close()
    return collections


def _backup_response(collections, scope_mode, filename):
    envelope = _backup_envelope(collections, scope_mode)
    body = json.dumps(envelope, ensure_ascii=False, indent=2) + "\n"
    response = Response(body, content_type="application/json; charset=utf-8")
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _backup_filename(name=None):
    stem = re.sub(r"[^a-z0-9]+", "-", str(name or "collections").lower()).strip("-")
    stem = stem[:60] or "collections"
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"lumae-{stem}-{day}.json"


def _normalize_backup_document(document):
    if not isinstance(document, dict):
        raise ValueError("Backup must be a JSON object.")
    if document.get("format") != COLLECTIONS_BACKUP_FORMAT:
        raise ValueError("This is not a Lumae Living Collections backup.")
    if document.get("version") != COLLECTIONS_BACKUP_VERSION:
        raise ValueError("This collection backup version is not supported.")
    raw_collections = document.get("collections")
    if not isinstance(raw_collections, list):
        raise ValueError("Backup collections must be a list.")
    if len(raw_collections) > MAX_BACKUP_COLLECTIONS:
        raise ValueError(f"A backup can contain at most {MAX_BACKUP_COLLECTIONS} collections.")

    checksum = document.get("checksum")
    if not isinstance(checksum, str) or not checksum.startswith("sha256:"):
        raise ValueError("Backup checksum is missing or invalid.")
    if checksum != _backup_checksum(raw_collections):
        raise ValueError("Backup checksum does not match its collection data.")

    normalized = []
    total_items = 0
    for raw_collection in raw_collections:
        if not isinstance(raw_collection, dict):
            raise ValueError("Every backup collection must be an object.")
        name = str(raw_collection.get("name") or "").strip()
        if not name:
            raise ValueError("Every backup collection requires a name.")
        if len(name) > 120:
            raise ValueError("Collection names must be 120 characters or fewer.")
        description = raw_collection.get("description")
        if description is not None:
            description = str(description).strip() or None
            if description and len(description) > 1000:
                raise ValueError("Collection descriptions must be 1,000 characters or fewer.")
        raw_items = raw_collection.get("items") or []
        if not isinstance(raw_items, list):
            raise ValueError(f"Items for {name} must be a list.")
        if len(raw_items) > MAX_BACKUP_ITEMS_PER_COLLECTION:
            raise ValueError(
                f"{name} has more than {MAX_BACKUP_ITEMS_PER_COLLECTION:,} items."
            )
        total_items += len(raw_items)
        if total_items > MAX_BACKUP_ITEMS:
            raise ValueError(f"A backup can contain at most {MAX_BACKUP_ITEMS:,} items.")

        items = []
        membership_keys = set()
        kind_positions = {"album": 0, "track": 0}
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                raise ValueError(f"Every item in {name} must be an object.")
            item = _normalize_item(raw_item)
            if item["kind"] == "track":
                membership_key = ("track", item["track_id"])
            elif item["provider_album_id"]:
                membership_key = ("album-id", item["provider_album_id"])
            else:
                membership_key = ("album-key", item["album_key"].lower())
            if membership_key in membership_keys:
                raise ValueError(f"{name} contains the same media item more than once.")
            membership_keys.add(membership_key)
            item["id"] = str(uuid.uuid4())
            item["position"] = kind_positions[item["kind"]]
            kind_positions[item["kind"]] += 1
            items.append(item)
        normalized.append({"name": name, "description": description, "items": items})
    return normalized


def _plan_restore(collections, restore_id, chunk_rows):
    """Split a normalised backup into restore chunks of at most ``chunk_rows`` rows.

    A row is one collection creation or one item. Ids derive from
    ``restore_id``, so a resumed restore plans exactly the same chunks, ids and
    positions. Each chunk is a list of segments ``{id, name, description,
    create, items}``: ``create`` is true for the segment that inserts the
    collection, and the later segments of a split collection append to it.
    """
    namespace = uuid.UUID(str(restore_id))
    chunks = []
    current = []
    used = 0
    for index, source in enumerate(collections):
        collection_id = str(uuid.uuid5(namespace, f"collection:{index}"))
        items = [
            {**item, "id": str(uuid.uuid5(namespace, f"item:{index}:{offset}"))}
            for offset, item in enumerate(source["items"])
        ]
        create = True
        taken = 0
        while True:
            needed = 1 if create else 0
            if current and used + needed > chunk_rows:
                chunks.append(current)
                current, used = [], 0
            segment_items = items[taken:taken + chunk_rows - used - needed]
            current.append({
                "id": collection_id,
                "name": source["name"],
                "description": source["description"],
                "create": create,
                "items": segment_items,
            })
            used += needed + len(segment_items)
            taken += len(segment_items)
            create = False
            if taken >= len(items):
                break
            chunks.append(current)
            current, used = [], 0
    if current:
        chunks.append(current)
    return chunks


def _restore_principal_collections(cur, principal, collections):
    """Write one restore chunk in the caller's transaction.

    ``collections`` holds segments (see ``_plan_restore``). A segment without
    ``id`` gets a new one, and a segment without ``create`` creates its
    collection. A created collection starts at revision 1, and each segment
    that writes items bumps the revision once. The creating segment emits the
    collection upsert; every item emits an upsert carrying the collection
    revision after its segment. A collection deleted before a later chunk
    reached it keeps its tombstone, and its remaining items are skipped.
    """
    restored = []
    item_count = 0
    staged_changes = []
    for source in collections:
        collection_id = source.get("id") or str(uuid.uuid4())
        create = source.get("create", True)
        if create:
            cur.execute(
                f"INSERT INTO {collections_table()} (principal, id, name, description) "
                "VALUES (%s, %s, %s, %s)",
                (principal, collection_id, source["name"], source["description"]),
            )
        else:
            locked = _lock_collection(cur, principal, collection_id, include_deleted=True)
            if locked is None or locked["deleted_at"] is not None:
                restored.append(locked)
                continue
        for item in source["items"]:
            _upsert_item(cur, principal, collection_id, item)
        if source["items"]:
            cur.execute(
                f"UPDATE {collections_table()} SET revision = revision + 1, updated_at = now() "
                "WHERE principal = %s AND id = %s",
                (principal, collection_id),
            )
        collection = _fetch_collection(cur, principal, collection_id)
        if create:
            staged_changes.append(
                (principal, collection_id, "collection", collection_id, "upsert", collection)
            )
        for item in source["items"]:
            staged_changes.append(
                (
                    principal, collection_id, "item", item["id"], "upsert",
                    {
                        **item,
                        "collection_revision": collection["revision"],
                        "collection_updated_at": collection["updated_at"],
                    },
                )
            )
        item_count += len(source["items"])
        restored.append(collection)
    # Keep event emission after every parent and item write for LUM-004's lock order.
    _record_changes(cur, staged_changes)
    return {"collections": restored, "collection_count": len(restored), "item_count": item_count}


class _RestoreRun:
    """One restore request, applied as ``_plan_restore`` chunks.

    ``step`` applies the next chunk in the caller's transaction. It returns
    ``None`` while chunks remain, then the final response body.

    With an idempotency key, progress is a ``collection_restores`` row written
    in each chunk's transaction, under the key's advisory lock. A retry with
    the same key and body resumes after the last committed chunk, and two
    requests with the same key never apply a chunk twice. While the row
    exists, ``_begin_mutation`` answers any other body under that key with
    ``idempotency_key_conflict``. A restore that fits in one chunk writes no
    progress row. Without a key, progress lives only in this object.
    """

    def __init__(self, collections, key, fingerprint):
        self.collections = collections
        self.key = key
        self.fingerprint = fingerprint
        self.restore_id = None
        self.chunks = None
        self.done = 0

    def _plan(self, restore_id, chunk_rows):
        if self.chunks is None or str(restore_id) != str(self.restore_id):
            self.restore_id = restore_id
            self.chunks = _plan_restore(self.collections, restore_id, chunk_rows)

    def _progress(self, cur, principal):
        """(chunks already done, whether a progress row tracks them)."""
        if not self.key:
            self._plan(self.restore_id or uuid.uuid4(), RESTORE_CHUNK_ROWS)
            return self.done, False
        # _begin_mutation has matched this request's fingerprint to the row.
        cur.execute(
            f"SELECT restore_id, chunk_rows, chunks_done "
            f"FROM {collection_restores_table()} "
            "WHERE principal = %s AND idempotency_key = %s FOR UPDATE",
            (principal, self.key),
        )
        row = cur.fetchone()
        if row is not None:
            restore_id, chunk_rows, done = row
            self._plan(restore_id, chunk_rows)
            return done, True
        if self.done:
            # This request committed a chunk, yet there is neither a receipt
            # (checked before each step) nor progress: it was removed by hand.
            raise RuntimeError("collection restore progress disappeared")
        self._plan(uuid.uuid4(), RESTORE_CHUNK_ROWS)
        if len(self.chunks) == 1:
            return 0, False
        cur.execute(
            f"INSERT INTO {collection_restores_table()} "
            "(principal, idempotency_key, request_fingerprint, restore_id, chunk_rows, "
            "chunk_count, chunks_done) VALUES (%s, %s, %s, %s, %s, %s, 0)",
            (principal, self.key, self.fingerprint, str(self.restore_id),
             RESTORE_CHUNK_ROWS, len(self.chunks)),
        )
        return 0, True

    def step(self, cur, principal):
        done, tracked = self._progress(cur, principal)
        chunk = self.chunks[done]
        last = done == len(self.chunks) - 1
        # Everything except the chunk's own writes happens first: once the
        # chunk appends its events it holds the feed head until commit, and
        # only the receipt insert may run then (nothing per collection).
        if tracked:
            if last:
                cur.execute(
                    f"DELETE FROM {collection_restores_table()} "
                    "WHERE principal = %s AND idempotency_key = %s",
                    (principal, self.key),
                )
            else:
                cur.execute(
                    f"UPDATE {collection_restores_table()} "
                    "SET chunks_done = %s, updated_at = now() "
                    "WHERE principal = %s AND idempotency_key = %s",
                    (done + 1, principal, self.key),
                )
        finished = {}
        if last:
            # The response lists collections finished by earlier chunks (by
            # this request or another with the same key) as they stand now;
            # the last chunk does not touch them.
            written = {segment["id"] for segment in chunk}
            finished = _fetch_collections(cur, principal, [
                segment["id"]
                for earlier in self.chunks[:-1] for segment in earlier
                if segment["create"] and segment["id"] not in written
            ])
        result = _restore_principal_collections(cur, principal, chunk)
        self.done = done + 1
        if not last:
            return None
        states = {segment["id"]: state for segment, state in zip(chunk, result["collections"])}
        restored = [
            states[segment["id"]] if segment["id"] in states else finished.get(segment["id"])
            for planned in self.chunks for segment in planned if segment["create"]
        ]
        return {
            "restored": True,
            "collections": restored,
            "collection_count": len(restored),
            "item_count": sum(len(source["items"]) for source in self.collections),
        }


def _record_changes(cur, changes):
    """Append staged change events as one block of consecutive seqs.

    ``changes`` are ``(principal, collection_id, entity_kind, entity_id,
    operation, payload)`` tuples in feed order. One UPDATE reserves the whole
    block and locks the feed head until the outer transaction commits; every
    caller has finished its parent and item writes before this point. One
    INSERT then writes all the events.
    """
    if not changes:
        return
    count = len(changes)
    cur.execute(
        f"UPDATE {collection_feed_state_table()} SET head_seq = head_seq + %s "
        "WHERE singleton = 1 AND protocol_version = %s RETURNING head_seq",
        (count, FEED_PROTOCOL_VERSION),
    )
    allocated = cur.fetchone()
    if allocated is None:
        raise FeedProtocolUnavailable("collection feed frontier unavailable")
    first = allocated[0] - count + 1
    principals, collection_ids, kinds, entity_ids, operations, payloads = zip(*changes)
    # The invariant MAX(seq) <= head is checked against the head this
    # transaction has just locked: one primary-key probe, evaluated once for
    # the block, so either every event is written or none is.
    cur.execute(
        f"""
        INSERT INTO {collection_changes_table()}
            (seq, principal, collection_id, entity_kind, entity_id, operation, payload)
        SELECT %s + staged.ordinal - 1, staged.principal, staged.collection_id,
               staged.entity_kind, staged.entity_id, staged.operation, staged.payload::jsonb
          FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::text[], %s::text[])
               WITH ORDINALITY AS staged(principal, collection_id, entity_kind, entity_id,
                                         operation, payload, ordinal)
         WHERE NOT EXISTS (
             SELECT 1 FROM {collection_changes_table()} WHERE seq >= %s
         )
        """,
        (
            first, list(principals), list(collection_ids), list(kinds), list(entity_ids),
            list(operations), [json.dumps(payload) for payload in payloads], first,
        ),
    )
    if cur.rowcount != count:
        raise FeedInvariantViolation("collection change rows exist past the feed head")


def _record_change(cur, principal, collection_id, entity_kind, entity_id, operation, payload):
    _record_changes(
        cur, [(principal, collection_id, entity_kind, entity_id, operation, payload)]
    )


def collection_feed_integrity(cur):
    """True when MAX(collection_changes.seq) <= head_seq; None if unknown.

    Two index lookups (the primary-key maximum and the singleton row), so it is
    cheap enough for health and startup.
    """
    cur.execute("SELECT to_regclass(%s), to_regclass(%s)",
                (collection_changes_table(), collection_feed_state_table()))
    if None in cur.fetchone():
        return None
    cur.execute(
        f"SELECT (SELECT COALESCE(MAX(seq), 0) FROM {collection_changes_table()}) <= head_seq "
        f"FROM {collection_feed_state_table()} WHERE singleton = 1"
    )
    row = cur.fetchone()
    return None if row is None else bool(row[0])


def _feed_state(cur):
    """The committed feed ``{epoch, head_seq, floor_seq}``, or None when the
    state row is missing or has an unknown protocol."""
    cur.execute(
        f"SELECT protocol_version, epoch::text, head_seq, floor_seq "
        f"FROM {collection_feed_state_table()} WHERE singleton = 1"
    )
    row = cur.fetchone()
    if row is None or row[0] != FEED_PROTOCOL_VERSION:
        return None
    return {"epoch": row[1], "head_seq": row[2], "floor_seq": row[3]}


def _same_epoch(echoed, epoch):
    try:
        return uuid.UUID(echoed) == uuid.UUID(epoch)
    except ValueError:
        return False


@contextmanager
def _snapshot_connection():
    """Own one read-only REPEATABLE READ backend, apart from the request's.

    The host may already have read settings on the request connection, after
    which its isolation level can no longer change.
    """
    dsn = getattr(config, "DATABASE_URL", None)
    if not dsn:
        raise FeedProtocolUnavailable("no database configured")
    try:
        db = psycopg2.connect(dsn, connect_timeout=SNAPSHOT_CONNECT_TIMEOUT_S,
                              application_name=SNAPSHOT_APPLICATION_NAME)
    except (psycopg2.Error, TypeError, ValueError) as exc:
        # Name only the class: libpq can quote the DSN, password included.
        current_app.logger.warning("Collection snapshot could not connect (%s)", type(exc).__name__)
        raise FeedProtocolUnavailable("snapshot connection failed") from None
    try:
        db.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with db.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, false), "
                "set_config('lock_timeout', %s, false)",
                (str(SNAPSHOT_STATEMENT_TIMEOUT_MS), str(SNAPSHOT_LOCK_TIMEOUT_MS)),
            )
        db.commit()
        yield db
    finally:
        # Read-only: nothing to commit. Never mask the exception in flight.
        for close in (db.rollback, db.close):
            try:
                close()
            except Exception:
                pass


def _read_snapshot(cur, principal):
    """All of a principal's active collections and items, and the feed head.

    One REPEATABLE READ transaction: the feed state is its first read, so the
    rows reflect exactly the events with ``seq <= head_seq`` (every writer
    moves the head in the transaction that writes the rows).
    """
    state = _feed_state(cur)
    if state is None:
        raise FeedProtocolUnavailable("collection feed state unavailable")
    cur.execute(
        _collection_select()
        + """
         WHERE c.principal = %s AND c.deleted_at IS NULL
         GROUP BY c.principal, c.id
         ORDER BY c.created_at, c.id
        """,
        (principal,),
    )
    collections = _all_dicts(cur)
    cur.execute(
        f"""
        SELECT i.id, i.collection_id, i.kind, i.track_id, i.provider_album_id,
               i.album_key, i.title, i.artist, i.album, i.cover_item_id, i.position,
               i.added_at, i.updated_at
          FROM {collection_items_table()} i
          JOIN {collections_table()} c
            ON c.principal = i.principal AND c.id = i.collection_id
         WHERE i.principal = %s AND c.deleted_at IS NULL
         ORDER BY i.collection_id, i.kind, i.position, i.added_at, i.id
        """,
        (principal,),
    )
    items = _all_dicts(cur)
    return {
        **state,
        "collections": collections,
        "items": items,
        "collection_count": len(collections),
        "item_count": len(items),
    }


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


def _request_fingerprint():
    body = request.get_json(silent=True)
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if_match = request.headers.get("If-Match")
    fields = (
        request.method.upper().encode("utf-8"),
        request.path.encode("utf-8"),
        canonical,
        b"absent" if if_match is None else b"present:" + if_match.strip().encode("utf-8"),
    )
    framed = b"".join(len(field).to_bytes(8, "big") + field for field in fields)
    return hashlib.sha256(framed).hexdigest()


def _lock_collection(cur, principal, collection_id, include_deleted=False):
    # Lock the plain row first: the aggregate collection query cannot use FOR UPDATE.
    cur.execute(
        f"SELECT deleted_at FROM {collections_table()} "
        "WHERE principal = %s AND id = %s FOR UPDATE",
        (principal, collection_id),
    )
    locked = cur.fetchone()
    if locked is None or (locked[0] is not None and not include_deleted):
        return None
    return _fetch_collection(cur, principal, collection_id, include_deleted=include_deleted)


def _idempotency_key():
    return (request.headers.get("Idempotency-Key") or "").strip()[:200]


# A handler returns (None, CONTINUE) to commit its transaction and be called
# again in a new one (a chunked restore).
CONTINUE = "continue"


def _key_conflict(db, cur, principal, collection_id):
    """409 ``idempotency_key_conflict``; the transaction is rolled back.

    Under contract 2 the body adds ``current``: the collection the key's
    receipt applied to as it stands now (tombstones included), or null when
    the key belongs to a restore, the receipt predates 1.3.0, or the
    collection does not exist.
    """
    body = {"error": "idempotency_key_conflict"}
    if contract_v2():
        body["current"] = None if collection_id is None else _fetch_collection(
            cur, principal, collection_id, include_deleted=True
        )
    db.rollback()
    return jsonify(body), 409


def _begin_mutation(db, cur, principal, key, fingerprint):
    """Open one mutation transaction; a response here ends the request.

    Checks the isolation level, bounds every lock wait, and with a key takes
    the key's advisory lock and replays or rejects an existing receipt.
    """
    # The host may have queried settings on this request-scoped connection.
    # SHOW is valid after that read; SET TRANSACTION would be rejected then.
    # Row-lock waiters must see the holder's committed revision.
    cur.execute("SHOW transaction_isolation")
    isolation = cur.fetchone()[0].lower()
    if isolation != "read committed" or getattr(db, "autocommit", False):
        db.rollback()
        return jsonify({"error": "unsupported_transaction_isolation"}), 503
    # Transaction-scoped: the host connection's own setting returns at commit.
    cur.execute(f"SET LOCAL lock_timeout = '{MUTATION_LOCK_TIMEOUT}'")
    if not key:
        return None
    identity = json.dumps((1, principal, key), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    lock_id = int.from_bytes(hashlib.sha256(identity).digest()[:8], "big", signed=True)
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
    cur.execute(
        f"SELECT response_payload::text, status_code, request_fingerprint, fingerprint_version, "
        f"collection_id FROM {collection_mutations_table()} "
        "WHERE principal = %s AND idempotency_key = %s",
        (principal, key),
    )
    saved = cur.fetchone()
    if not saved:
        # An unfinished chunked restore owns the key: only a retry of that
        # restore (same fingerprint) may use it, whatever the route.
        cur.execute(
            f"SELECT request_fingerprint FROM {collection_restores_table()} "
            "WHERE principal = %s AND idempotency_key = %s",
            (principal, key),
        )
        restoring = cur.fetchone()
        if restoring is not None and restoring[0] != fingerprint:
            return _key_conflict(db, cur, principal, None)
        return None
    payload_text, status, saved_digest, saved_version, saved_collection = saved
    if saved_digest is None and saved_version is None:
        current_app.logger.warning("Replaying legacy unbound collection receipt")
        headers = {"Idempotency-Replayed": "true", "Idempotency-Fingerprint": "legacy-unbound"}
    elif saved_version != FINGERPRINT_VERSION or saved_digest != fingerprint:
        return _key_conflict(db, cur, principal, saved_collection)
    else:
        headers = {"Idempotency-Replayed": "true"}
    db.rollback()
    return jsonify(json.loads(payload_text)), status, headers


def _mutation_response(handler, collection_id=None):
    """Run ``handler`` as one keyed, bounded mutation; ``collection_id`` is the
    collection the request applies to (None for a restore), kept with its
    receipt."""
    principal = current_principal()
    key = _idempotency_key()
    fingerprint = _request_fingerprint()
    db = get_db()
    cur = db.cursor()
    try:
        while True:
            early = _begin_mutation(db, cur, principal, key, fingerprint)
            if early is not None:
                return early
            payload, status = handler(cur, principal)
            if status == CONTINUE:
                db.commit()
                continue
            if not 200 <= status < 300:
                db.rollback()
                return jsonify(payload), status
            if key:
                cur.execute(
                    f"INSERT INTO {collection_mutations_table()} "
                    "(principal, idempotency_key, response_payload, status_code, "
                    "request_fingerprint, fingerprint_version, collection_id) "
                    "VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s)",
                    (principal, key, json.dumps(payload), status, fingerprint,
                     FINGERPRINT_VERSION, collection_id),
                )
            db.commit()
            return jsonify(payload), status
    except ForeignItemConflict:
        db.rollback()
        return jsonify({"error": "item_id_collection_conflict"}), 409
    except FeedProtocolUnavailable:
        db.rollback()
        return jsonify({"error": "collection_feed_unavailable"}), 503
    except FeedInvariantViolation:
        db.rollback()
        current_app.logger.error(
            "Collection feed invariant violated (MAX(seq) > head_seq); writes are "
            "blocked until the head is realigned (docs/runbooks/UPGRADE_1.3.md)"
        )
        return jsonify({"error": "collection_feed_invariant"}), 503
    except Exception as exc:
        db.rollback()
        if getattr(exc, "pgcode", None) == LOCK_NOT_AVAILABLE:
            return (jsonify({"error": "collection_busy"}), 503,
                    {"Retry-After": str(BUSY_RETRY_AFTER_S)})
        raise
    finally:
        cur.close()


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


class ForeignItemConflict(Exception):
    pass


def _upsert_item(cur, principal, collection_id, item, remap=True):
    """Write one normalised item into a locked collection.

    When another item of the collection already holds the same membership
    (track id, provider album id, or album key without one), ``remap`` keeps
    the 1.2.5 behaviour: ``item["id"]`` becomes that item's id and it is
    updated. Without ``remap`` (contract 2) nothing is written and that item's
    id is returned. Returns None when the item was written.
    """
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
    if existing and existing[0] != item["id"]:
        if not remap:
            return existing[0]
        item["id"] = existing[0]
    cur.execute(
        f"""
        INSERT INTO {collection_items_table()} AS target
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
        WHERE target.collection_id = EXCLUDED.collection_id
        RETURNING id
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
    if cur.fetchone() is None:
        raise ForeignItemConflict()
    return None


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
        return jsonify(
            {
                "schema_version": COLLECTIONS_SCHEMA_VERSION,
                "scope": current_collection_scope()["mode"],
                "collections": rows,
            }
        )

    @bp.get("/api/collections/backup")
    @require_collections_enabled
    def collection_backup():
        principal = current_principal()
        collections = _export_principal_collections(principal)
        return _backup_response(
            collections,
            current_collection_scope()["mode"],
            _backup_filename(),
        )

    @bp.post("/api/collections/restore")
    @require_collections_enabled
    def collection_restore():
        try:
            collections = _normalize_backup_document(request.get_json(silent=True))
        except (TypeError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400
        if not collections:
            return jsonify({"error": "The backup does not contain any collections."}), 400
        run = _RestoreRun(collections, _idempotency_key(), _request_fingerprint())

        def mutate(cur, principal):
            result = run.step(cur, principal)
            if result is None:
                return None, CONTINUE
            return result, 201

        return _mutation_response(mutate)

    @bp.post("/api/collections")
    @require_collections_enabled
    def collection_create():
        body = request.get_json(silent=True) or {}
        collection_id = str(body.get("id") or uuid.uuid4())

        def mutate(cur, principal):
            try:
                name, description = _clean_collection_body(body)
            except ValueError as exc:
                return _error(str(exc), 400)
            cur.execute(
                f"""
                INSERT INTO {collections_table()} (principal, id, name, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (principal, id) DO NOTHING
                RETURNING id
                """,
                (principal, collection_id, name, description),
            )
            inserted = cur.fetchone() is not None
            if not inserted and contract_v2():
                # K9: the id is taken; answer with the collection holding it.
                existing = _fetch_collection(cur, principal, collection_id, include_deleted=True)
                deleted = existing is not None and existing["deleted_at"] is not None
                return _error("collection_deleted" if deleted else "collection_exists", 409,
                              current=existing)
            collection = _fetch_collection(cur, principal, collection_id)
            if inserted:
                _record_change(
                    cur, principal, collection_id, "collection", collection_id, "upsert", collection
                )
            return {"collection": collection}, 201

        return _mutation_response(mutate, collection_id)

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

    @bp.get("/api/collections/<collection_id>/export")
    @require_collections_enabled
    def collection_export(collection_id):
        principal = current_principal()
        collections = _export_principal_collections(principal, collection_id)
        if not collections:
            return jsonify({"error": "collection_not_found"}), 404
        return _backup_response(
            collections,
            current_collection_scope()["mode"],
            _backup_filename(collections[0]["name"]),
        )

    @bp.patch("/api/collections/<collection_id>")
    @require_collections_enabled
    def collection_update(collection_id):
        body = request.get_json(silent=True) or {}

        def mutate(cur, principal):
            current = _lock_collection(cur, principal, collection_id)
            if not current:
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                return _error("revision_conflict", 409, current=current)
            try:
                name, description = _clean_collection_body(body, partial=True)
            except ValueError as exc:
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
            return {"collection": updated}, 200

        return _mutation_response(mutate, collection_id)

    @bp.delete("/api/collections/<collection_id>")
    @require_collections_enabled
    def collection_delete(collection_id):
        body = request.get_json(silent=True) or {}

        def mutate(cur, principal):
            current = _lock_collection(cur, principal, collection_id, include_deleted=True)
            if not current:
                return {"deleted": True}, 200
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                return _error("revision_conflict", 409, current=current)
            if current["deleted_at"] is not None:
                return {"deleted": True}, 200
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
            return {"deleted": True, **payload}, 200

        return _mutation_response(mutate, collection_id)

    @bp.put("/api/collections/<collection_id>/items/<item_id>")
    @require_collections_enabled
    def collection_item_upsert(collection_id, item_id):
        body = dict(request.get_json(silent=True) or {})
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

        def mutate(cur, principal):
            try:
                items = [_normalize_item(item) for item in raw_items]
            except (TypeError, ValueError) as exc:
                return _error(str(exc), 400)
            current = _lock_collection(cur, principal, collection_id)
            if not current:
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
                return _error("revision_conflict", 409, current=current)
            remap = not contract_v2()
            conflicts = []
            for item in items:
                existing_id = _upsert_item(cur, principal, collection_id, item, remap=remap)
                if existing_id is not None:
                    conflicts.append({"item_id": item["id"], "existing_item_id": existing_id})
            if conflicts:
                # K9: nothing is written (the transaction rolls back).
                return _error("membership_conflict", 409, **conflicts[0],
                              conflicts=conflicts, current=current)
            cur.execute(
                f"""
                UPDATE {collections_table()}
                   SET revision = revision + 1, updated_at = now()
                 WHERE principal = %s AND id = %s
                """,
                (principal, collection_id),
            )
            updated = _fetch_collection(cur, principal, collection_id)
            _record_changes(cur, [
                (
                    principal,
                    collection_id,
                    "item",
                    item["id"],
                    "upsert",
                    {
                        **item,
                        "collection_revision": updated["revision"],
                        "collection_updated_at": updated["updated_at"],
                    },
                )
                for item in items
            ])
            return {"collection": updated, "items": items}, success_status

        return _mutation_response(mutate, collection_id)

    @bp.delete("/api/collections/<collection_id>/items/<item_id>")
    @require_collections_enabled
    def collection_item_delete(collection_id, item_id):
        body = request.get_json(silent=True) or {}

        def mutate(cur, principal):
            current = _lock_collection(cur, principal, collection_id)
            if not current:
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
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
            return {"deleted": removed, "collection": updated}, 200

        return _mutation_response(mutate, collection_id)

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

        def mutate(cur, principal):
            current = _lock_collection(cur, principal, collection_id)
            if not current:
                return _error("collection_not_found", 404)
            expected = _expected_revision(body)
            if expected is not None and expected != current["revision"]:
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
            _record_changes(cur, [
                (
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
                for removed_id in removed_ids
            ])
            return {
                "deleted": removed_ids,
                "deleted_count": len(removed_ids),
                "collection": updated,
            }, 200

        return _mutation_response(mutate, collection_id)

    @bp.get("/api/collections/changes")
    @require_collections_enabled
    def collection_changes():
        try:
            cursor = max(int(request.args.get("cursor", 0)), 0)
            limit = min(max(int(request.args.get("limit", 200)), 1), 500)
        except ValueError:
            return jsonify({"error": "invalid_cursor"}), 400
        # K8: only a client that echoes the epoch can be told to resync. An
        # absent or empty epoch keeps the 1.2.5 behaviour (never 410).
        echoed = (request.args.get("epoch") or "").strip()
        db = get_db()
        cur = db.cursor()
        state = _feed_state(cur)
        if state is None:
            db.rollback()
            cur.close()
            return jsonify({"error": "collection_feed_unavailable"}), 503
        principal = current_principal()
        head_seq = state["head_seq"]
        if echoed:
            reason = None
            if not _same_epoch(echoed, state["epoch"]):
                reason = "epoch_mismatch"
            elif cursor > head_seq:
                reason = "cursor_ahead"
            if reason:
                db.rollback()
                cur.close()
                return jsonify({"error": "collections_resync_required", "reason": reason}), 410
        cur.execute(
            f"""
            SELECT seq, collection_id, entity_kind, entity_id, operation,
                   payload, created_at
              FROM {collection_changes_table()}
             WHERE principal = %s AND seq > %s AND seq <= %s
             ORDER BY seq ASC LIMIT %s
            """,
            (principal, cursor, head_seq, limit + 1),
        )
        changes = _all_dicts(cur)
        cur.close()
        has_more = len(changes) > limit
        del changes[limit:]
        for change in changes:
            if isinstance(change.get("payload"), str):
                change["payload"] = json.loads(change["payload"])
        next_cursor = changes[-1]["seq"] if changes else cursor
        return jsonify({
            "changes": changes,
            "next_cursor": next_cursor,
            "epoch": state["epoch"],
            "head_seq": head_seq,
            "floor_seq": state["floor_seq"],
            "has_more": has_more,
        })

    @bp.get("/api/collections/snapshot")
    @require_collections_enabled
    def collection_snapshot():
        principal = current_principal()
        scope = current_collection_scope()["mode"]

        def unavailable():
            return (jsonify({"error": "collection_feed_unavailable"}), 503,
                    {"Retry-After": str(UNAVAILABLE_RETRY_AFTER_S)})

        if not _SNAPSHOT_SLOT.acquire(timeout=SNAPSHOT_WAIT_S):
            return unavailable()
        try:
            with _snapshot_connection() as db:
                with db.cursor() as cur:
                    snapshot = _read_snapshot(cur, principal)
            return jsonify({"schema_version": COLLECTIONS_SCHEMA_VERSION, "scope": scope,
                            **snapshot})
        except (FeedProtocolUnavailable, psycopg2.OperationalError,
                psycopg2.InterfaceError) as exc:
            if not isinstance(exc, FeedProtocolUnavailable):
                current_app.logger.warning(
                    "Collection snapshot database unavailable (%s)", type(exc).__name__
                )
            return unavailable()
        finally:
            _SNAPSHOT_SLOT.release()

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
        # ILIKE on the columns: search_u is unused, so unaccent is not needed.
        if kind == "album":
            cur.execute(
                f"""
                SELECT MIN(item_id) AS cover_item_id, album,
                       COALESCE(NULLIF(album_artist, ''), author) AS artist,
                       COUNT(*)::INTEGER AS track_count
                  FROM ({catalog_track_view_sql(unaccent=False)}) score
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
                f"""
                SELECT item_id AS track_id, title, author AS artist, album,
                       item_id AS cover_item_id
                  FROM ({catalog_track_view_sql(unaccent=False)}) score
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
        <p class="lumae-help">Manage mixed album-and-track collections in AudioMuse and synchronize collections and Personal Shelves with Lumae, including shelf arrangement, recommendation evidence, and listening insights. Shelf management is in the mobile app. This also enables Want Shelf, musical memories, feedback and Rest sync in compatible Lumae apps. Use the same account on each device; server-token users share one shelf. Turning this off hides the manager and sync APIs without deleting anything.</p>
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
        <p class="lumae-help">Album metadata: MusicBrainz lookups need no account or API key. External album checking runs in the background, pauses for playback work and respects Pause background maintenance. Each account gets up to 80 uncached requests per UTC day. Last.fm and AI keys are configured only in the Lumae app.</p>
      </section>
    """


def render_collections_manager():
    scope = current_collection_scope()
    return render_page(
        render_collection_workbench(scope["label"], scope["detail"]),
        title="Living Collections",
    )
