"""Authenticated, privacy-minimised friend album discovery for AudioMuse-AI.

Only album-level Album Dynamics fingerprints and ordinary album metadata cross
an instance boundary. Track embeddings, track identities, listening history,
credentials, and audio remain on the instance that owns them.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import secrets
import socket
import unicodedata
import uuid
import time
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import quote, urlparse

import numpy as np
import requests
from flask import abort, Blueprint, Response, g, jsonify, request, stream_with_context
from psycopg2.extras import DictCursor, Json

from plugin.api import config, enqueue, get_db, logger, render_page, table
from tasks.mediaserver import get_all_songs
from tasks.sonic_fingerprint_manager import calculate_sonic_fingerprint_vector

from . import catalog_store, sync_jobs, source_projection

from .album_dynamics import (
    EMBEDDING_FAMILY,
    FINGERPRINT_METHOD,
    FINGERPRINT_SCHEMA_VERSION,
    AlbumTrack,
    build_fingerprint,
    decode_vector,
    deserialize_fingerprint,
    mood_features,
    normalize_energy,
    rank_for_sonic_fingerprint,
    rank_similar_albums,
    serialize_fingerprint,
)


bp = Blueprint("federated_albums", __name__)

CATALOG_PAGE_SIZE = 250
MAX_REMOTE_ALBUMS = 100_000
MAX_REMOTE_RESPONSE_BYTES = 6 * 1024 * 1024
MAX_ARTWORK_BYTES = 15 * 1024 * 1024
TOKEN_PREFIX = "afa_"
HOST_CAPABILITIES = {"scoped_bearer_auth": False}

CAPABILITY = {
    "schemaVersion": FINGERPRINT_SCHEMA_VERSION,
    "fingerprintMethod": FINGERPRINT_METHOD,
    "embeddingFamily": EMBEDDING_FAMILY,
}


def _tables():
    return {
        "meta": table("meta"),
        "albums": table("albums"),
        "track_order": table("track_order"),
        "tokens": table("share_tokens"),
        "connections": table("connections"),
        "remote": table("remote_albums"),
    }


def migrate(db=None):
    """Create the plugin-owned schema. Safe on every web/worker start."""
    names = _tables()
    db = db if db is not None else get_db()
    with db.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names['meta']} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names['albums']} (
                album_key TEXT PRIMARY KEY,
                album TEXT NOT NULL,
                artist TEXT NOT NULL,
                year INTEGER,
                cover_item_id TEXT,
                track_count INTEGER NOT NULL,
                source_signature TEXT NOT NULL,
                fingerprint JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names['track_order']} (
                track_id TEXT PRIMARY KEY,
                album_key TEXT,
                disc_number INTEGER,
                track_number INTEGER,
                observed_order INTEGER,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names['tokens']} (
                id BIGSERIAL PRIMARY KEY,
                token_hash CHAR(64) UNIQUE NOT NULL,
                token_hint TEXT NOT NULL,
                username TEXT NOT NULL,
                label TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_used_at TIMESTAMPTZ,
                revoked_at TIMESTAMPTZ
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names['connections']} (
                id BIGSERIAL PRIMARY KEY,
                owner TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                access_token TEXT NOT NULL,
                remote_instance_id TEXT,
                last_synced_at TIMESTAMPTZ,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(owner, base_url)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names['remote']} (
                connection_id BIGINT NOT NULL,
                remote_instance_id TEXT NOT NULL,
                album_key TEXT NOT NULL,
                album TEXT NOT NULL,
                artist TEXT NOT NULL,
                year INTEGER,
                track_count INTEGER NOT NULL,
                fingerprint JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(connection_id, album_key)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {names['remote']}_instance_idx "
            f"ON {names['remote']} (remote_instance_id, album_key)"
        )
        cur.execute(
            f"INSERT INTO {names['meta']} (key, value) VALUES ('instance_id', %s) "
            "ON CONFLICT (key) DO NOTHING",
            (str(uuid.uuid4()),),
        )
    catalog_store.migrate(db)
    sync_jobs.migrate(db)
    db.commit()


def _instance_id():
    names = _tables()
    db = get_db()
    with db.cursor() as cur:
        cur.execute(f"SELECT value FROM {names['meta']} WHERE key = 'instance_id'")
        row = cur.fetchone()
    if row:
        return row[0]
    migrate()
    return _instance_id()


def _owner():
    user = getattr(g, "auth_user", None)
    if not user:
        abort(401)
    return str(user)


def _normalise_identity(value):
    text = unicodedata.normalize("NFKC", str(value or "")).casefold().strip()
    return " ".join(text.split())


def album_key(artist, album):
    identity = _normalise_identity(artist) + "\0" + _normalise_identity(album)
    return "v1:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _int_or_none(value):
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _first_value(mapping, *names):
    for name in names:
        value = mapping.get(name)
        if value is not None and value != "":
            return value
    return None


def _media_order_map():
    """Read provider order once; unsupported providers get deterministic fallback order."""
    output = {}
    try:
        with source_projection.bind(get_db(), _tables()["meta"]):
            songs = get_all_songs() or []
    except Exception:
        logger.exception(
            "Friend Album Discovery could not read media-server track order"
        )
        songs = []
    for observed, item in enumerate(songs):
        track_id = _first_value(item, "Id", "id", "item_id")
        if track_id is None:
            continue
        disc = _int_or_none(
            _first_value(item, "ParentIndexNumber", "discNumber", "disc_number", "disc")
        )
        track = _int_or_none(
            _first_value(item, "IndexNumber", "trackNumber", "track_number", "track")
        )
        output[str(track_id)] = (disc, track, observed)
    return output


def _persist_order_map(order_map):
    if not order_map:
        return
    names = _tables()
    db = get_db()
    with db.cursor() as cur:
        for track_id, (disc, track_number, observed) in order_map.items():
            cur.execute(
                f"""
                INSERT INTO {names['track_order']}
                    (track_id, disc_number, track_number, observed_order, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (track_id) DO UPDATE SET
                    disc_number = EXCLUDED.disc_number,
                    track_number = EXCLUDED.track_number,
                    observed_order = EXCLUDED.observed_order,
                    updated_at = NOW()
                """,
                (track_id, disc, track_number, observed),
            )
    db.commit()


def _analysis_rows(album_keys=None):
    names = _tables()
    db = get_db()
    join, params, provider_id = source_projection.mapping(db, names["meta"])
    query = f"""
        SELECT {provider_id} AS item_id, s.album, s.album_artist, s.author, s.year,
               s.energy, s.mood_vector, s.other_features, e.embedding,
               o.disc_number, o.track_number, o.observed_order
        FROM score s
        {join}
        JOIN embedding e ON e.item_id = s.item_id
        LEFT JOIN {names['track_order']} o ON o.track_id = {provider_id}
        WHERE e.embedding IS NOT NULL
          AND NULLIF(BTRIM(s.album), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(s.album_artist, s.author)), '') IS NOT NULL
    """
    if album_keys:
        identities = [
            (str(artist).strip().lower(), str(album).strip().lower())
            for artist, album in album_keys
        ]
        query += " AND (LOWER(BTRIM(COALESCE(s.album_artist, s.author))), LOWER(BTRIM(s.album))) IN %s"
        params.append(tuple(identities))
    with db.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(query, tuple(params))
        return [dict(row) for row in cur.fetchall()]


def _track_order(row, fallback):
    disc = _int_or_none(row.get("disc_number"))
    number = _int_or_none(row.get("track_number"))
    observed = _int_or_none(row.get("observed_order"))
    if disc is not None or number is not None:
        return max(0, disc or 0) * 100_000 + max(0, number or 0)
    return observed if observed is not None else fallback


def _group_rows(rows):
    grouped = defaultdict(list)
    metadata = {}
    for fallback, row in enumerate(rows):
        artist = str(row.get("album_artist") or row.get("author") or "").strip()
        album = str(row.get("album") or "").strip()
        key = album_key(artist, album)
        raw = row.get("embedding")
        if not raw:
            continue
        vector = np.frombuffer(raw, dtype=np.float32).copy()
        if vector.size != 200:
            logger.warning(
                "Skipping %s: expected 200D MusiCNN embedding, got %s",
                row["item_id"],
                vector.size,
            )
            continue
        grouped[key].append(
            AlbumTrack(
                track_id=str(row["item_id"]),
                embedding=vector,
                energy=normalize_energy(row.get("energy")),
                mood=mood_features(row.get("mood_vector"), row.get("other_features")),
                order=_track_order(row, fallback),
            )
        )
        metadata[key] = {
            "album": album,
            "artist": artist,
            "year": _int_or_none(row.get("year")),
            "cover_item_id": str(row["item_id"]),
        }
    return grouped, metadata


def _source_signature(tracks):
    digest = hashlib.sha256()
    for track in sorted(tracks, key=lambda item: (item.order, item.track_id)):
        digest.update(track.track_id.encode("utf-8"))
        digest.update(b"\0")
        digest.update(track.embedding.astype("<f4", copy=False).tobytes())
        digest.update(str(track.energy).encode("ascii"))
        digest.update(
            track.mood.astype("<f4", copy=False).tobytes()
            if track.mood is not None
            else b""
        )
        digest.update(str(track.order).encode("ascii"))
    return digest.hexdigest()


def _upsert_groups(grouped, metadata, delete_missing=False):
    names = _tables()
    db = get_db()
    seen = set()
    changed = 0
    with db.cursor() as cur:
        for key, tracks in grouped.items():
            fingerprint = build_fingerprint(key, tracks)
            if fingerprint is None:
                continue
            seen.add(key)
            signature = _source_signature(tracks)
            cur.execute(
                f"SELECT source_signature, buckets FROM {names['albums']} WHERE album_key = %s",
                (key,),
            )
            existing = cur.fetchone()
            if existing and existing[0] == signature and existing[1] is not None:
                continue
            item = metadata[key]
            cur.execute(
                f"""
                INSERT INTO {names['albums']}
                    (album_key, album, artist, year, cover_item_id, track_count,
                     source_signature, fingerprint, buckets, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (album_key) DO UPDATE SET
                    album = EXCLUDED.album,
                    artist = EXCLUDED.artist,
                    year = EXCLUDED.year,
                    cover_item_id = EXCLUDED.cover_item_id,
                    track_count = EXCLUDED.track_count,
                    source_signature = EXCLUDED.source_signature,
                    fingerprint = EXCLUDED.fingerprint,
                    buckets = EXCLUDED.buckets,
                    updated_at = NOW()
                """,
                (
                    key,
                    item["album"],
                    item["artist"],
                    item["year"],
                    item["cover_item_id"],
                    len(tracks),
                    signature,
                    Json(serialize_fingerprint(fingerprint)),
                    catalog_store.buckets(fingerprint["meanVector"]),
                ),
            )
            changed += 1
        if delete_missing:
            if seen:
                cur.execute(
                    f"DELETE FROM {names['albums']} WHERE NOT (album_key = ANY(%s))",
                    (list(seen),),
                )
            else:
                cur.execute(f"DELETE FROM {names['albums']}")
    db.commit()
    return {"albums": len(seen), "updated": changed}


def rebuild_catalog():
    """Rebuild local album averages. Remote cached rows are deliberately excluded."""
    migrate()
    db = get_db()
    lock_id = 0x46414C42
    with db.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
        if not cur.fetchone()[0]:
            return {"status": "already_running"}
    try:
        order_map = _media_order_map()
        _persist_order_map(order_map)
        grouped, metadata = _group_rows(_analysis_rows())
        result = _upsert_groups(grouped, metadata, delete_missing=True)
        result["status"] = "ok"
        return result
    finally:
        with db.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
        db.commit()


def _album_identity_from_metadata(metadata):
    artist = _first_value(
        metadata or {}, "album_artist", "AlbumArtist", "artist", "Author"
    )
    album = _first_value(metadata or {}, "album", "Album")
    if not artist or not album:
        return None
    return str(artist).strip(), str(album).strip()


def song_analyzed(payload):
    """Incrementally refresh the album touched by an analysis hook."""
    item_id = str((payload or {}).get("item_id") or "")
    identity = _album_identity_from_metadata((payload or {}).get("metadata") or {})
    if not item_id or not identity:
        return
    server_id, registry = source_projection.source(get_db(), _tables()["meta"])
    if registry and str((payload or {}).get("server_id") or "") != server_id:
        return
    media = (payload or {}).get("media_item") or {}
    order_map = {
        item_id: (
            _int_or_none(
                _first_value(
                    media, "ParentIndexNumber", "discNumber", "disc_number", "disc"
                )
            ),
            _int_or_none(
                _first_value(
                    media, "IndexNumber", "trackNumber", "track_number", "track"
                )
            ),
            None,
        )
    }
    _persist_order_map(order_map)
    artist, album = identity
    rows = _analysis_rows([(artist, album)])
    grouped, metadata = _group_rows(rows)
    _upsert_groups(grouped, metadata)


def enqueue_initial_rebuild():
    try:
        enqueue(rebuild_catalog)
    except Exception:
        logger.exception("Could not enqueue Friend Album Discovery catalog rebuild")


def _token_hash(token):
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def authenticate_friend_bearer(token, method, path):
    allowed = path.rstrip(
        "/"
    ) == "/plugins/federated_albums/api/fingerprints" or path.startswith(
        "/plugins/federated_albums/api/artwork/"
    )
    if method != "GET" or not allowed:
        return None
    if not isinstance(token, str) or not token.startswith(TOKEN_PREFIX):
        return None
    names = _tables()
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {names['tokens']} SET last_used_at = NOW()
            WHERE token_hash = %s AND revoked_at IS NULL
            RETURNING username
            """,
            (_token_hash(token),),
        )
        row = cur.fetchone()
    db.commit()
    return {"user": row[0], "role": "user"} if row else None


def _media_artwork_request(item_id, size):
    """Fetch configured media-server artwork without exposing its credential."""
    provider = str(config.MEDIASERVER_TYPE or "").lower()
    size = max(64, min(1200, int(size or 300)))
    headers = {"Accept": "image/*"}
    params = {}
    if provider in ("jellyfin", "emby"):
        base = config.JELLYFIN_URL if provider == "jellyfin" else config.EMBY_URL
        token = config.JELLYFIN_TOKEN if provider == "jellyfin" else config.EMBY_TOKEN
        if not base or not token:
            return None
        headers["X-Emby-Token"] = token
        url = f"{base.rstrip('/')}/Items/{quote(str(item_id), safe='')}/Images/Primary"
        params = {"maxWidth": size, "quality": 90}
    elif provider == "plex":
        if not config.PLEX_URL or not config.PLEX_TOKEN:
            return None
        headers["X-Plex-Token"] = config.PLEX_TOKEN
        url = (
            f"{config.PLEX_URL.rstrip('/')}/library/metadata/"
            f"{quote(str(item_id), safe='')}/thumb"
        )
        params = {"width": size, "height": size}
    elif provider == "navidrome":
        if (
            not config.NAVIDROME_URL
            or not config.NAVIDROME_USER
            or not config.NAVIDROME_PASSWORD
        ):
            return None
        salt = secrets.token_hex(8)
        token = hashlib.md5(
            (config.NAVIDROME_PASSWORD + salt).encode("utf-8")
        ).hexdigest()
        url = f"{config.NAVIDROME_URL.rstrip('/')}/rest/getCoverArt.view"
        params = {
            "u": config.NAVIDROME_USER,
            "t": token,
            "s": salt,
            "v": "1.16.1",
            "c": "AudioMuse-FriendAlbums",
            "id": item_id,
            "size": size,
        }
    else:
        return None
    return requests.get(
        url,
        params=params,
        headers=headers,
        timeout=(5, 20),
        allow_redirects=False,
        stream=True,
    )


def _image_response(upstream):
    if upstream is None:
        return jsonify({"error": "Artwork is unavailable for this media server"}), 404
    content_type = str(upstream.headers.get("Content-Type") or "")
    content_length = _int_or_none(upstream.headers.get("Content-Length"))
    if upstream.status_code != 200 or not content_type.lower().startswith("image/"):
        status = upstream.status_code if 400 <= upstream.status_code < 500 else 502
        upstream.close()
        return jsonify({"error": "Artwork could not be loaded"}), status
    if content_length is not None and content_length > MAX_ARTWORK_BYTES:
        upstream.close()
        return jsonify({"error": "Artwork exceeds the proxy limit"}), 413

    def chunks():
        total = 0
        try:
            for chunk in upstream.iter_content(64 * 1024):
                total += len(chunk)
                if total > MAX_ARTWORK_BYTES:
                    break
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    response = Response(stream_with_context(chunks()), content_type=content_type)
    response.headers["Cache-Control"] = "private, max-age=3600"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.call_on_close(upstream.close)
    return response


@bp.get("/api/artwork/<path:requested_album_key>")
def local_artwork(requested_album_key):
    names = _tables()
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            f"SELECT cover_item_id FROM {names['albums']} WHERE album_key = %s",
            (requested_album_key,),
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return jsonify({"error": "Album artwork not found"}), 404
    try:
        return _image_response(
            _media_artwork_request(row[0], request.args.get("size", 300))
        )
    except Exception:
        logger.exception("Friend Album Discovery artwork proxy failed")
        return jsonify({"error": "Artwork could not be loaded"}), 502


@bp.get("/api/friend-artwork/<remote_instance>/<path:requested_album_key>")
def friend_artwork(remote_instance, requested_album_key):
    names = _tables()
    db = get_db()
    with db.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            f"""
            SELECT c.base_url, c.access_token
            FROM {names['connections']} c
            JOIN {names['remote']} r ON r.connection_id = c.id
            WHERE r.remote_instance_id = %s AND r.album_key = %s AND c.owner = %s
            LIMIT 1
            """,
            (remote_instance, requested_album_key, _owner()),
        )
        row = cur.fetchone()
    if not row:
        return jsonify({"error": "Friend album not found"}), 404
    try:
        base_url = _validate_base_url(row["base_url"])
        encoded_key = quote(requested_album_key, safe="")
        upstream = requests.get(
            f"{base_url}/plugins/federated_albums/api/artwork/{encoded_key}",
            params={
                "size": max(
                    64, min(1200, _int_or_none(request.args.get("size")) or 300)
                )
            },
            headers={
                "Authorization": f"Bearer {row['access_token']}",
                "Accept": "image/*",
            },
            timeout=(5, 25),
            allow_redirects=False,
            stream=True,
        )
        return _image_response(upstream)
    except Exception:
        logger.exception("Friend Album Discovery friend artwork proxy failed")
        return jsonify({"error": "Friend artwork could not be loaded"}), 502


def _json_fingerprint(value):
    return value if isinstance(value, dict) else json.loads(value)


def _local_albums(include_fingerprint=True, **filters):
    instance_id = _instance_id()
    return [
        _album_record(row, instance_id, "local", include_fingerprint)
        for row in catalog_store.read(
            _owner(), fingerprint=include_fingerprint, **filters
        )
    ]


def _remote_albums(include_fingerprint=True, **filters):
    return [
        _album_record(
            row,
            row["remote_instance_id"],
            "friend",
            include_fingerprint,
            source_name=row.get("source_name"),
            base_url=row.get("base_url"),
        )
        for row in catalog_store.read(
            _owner(), remote=True, fingerprint=include_fingerprint, **filters
        )
    ]


def _album_record(
    row, instance_id, availability, include_fingerprint, source_name=None, base_url=None
):
    item = {
        "albumKey": row["album_key"],
        "album": row["album"],
        "artist": row["artist"],
        "year": row.get("year"),
        "trackCount": int(row.get("track_count") or 0),
        "instanceId": instance_id,
        "availability": availability,
        "sourceName": source_name
        or ("This AudioMuse" if availability == "local" else "Friend"),
        "baseUrl": base_url,
        "updatedAt": (
            row["updated_at"].isoformat()
            if hasattr(row.get("updated_at"), "isoformat")
            else str(row.get("updated_at") or "")
        ),
    }
    if availability == "friend":
        encoded_instance = quote(str(instance_id), safe="")
        encoded_album = quote(str(row["album_key"]), safe="")
        item["artworkPath"] = (
            f"/plugins/federated_albums/api/friend-artwork/{encoded_instance}/{encoded_album}"
        )
    if include_fingerprint:
        payload = _json_fingerprint(row["fingerprint"])
        item["fingerprintPayload"] = payload
        item["fingerprint"] = deserialize_fingerprint(payload, row["album_key"])
    return item


def _public_album(item, include_fingerprint=False):
    result = {key: value for key, value in item.items() if key != "fingerprint"}
    if not include_fingerprint:
        result.pop("fingerprintPayload", None)
    return result


def _validate_base_url(value):
    raw = str(value or "").strip().rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Friend URL must be an http(s) AudioMuse base URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(
            "Friend URL cannot contain credentials, a query, or a fragment"
        )
    try:
        default_port = 443 if parsed.scheme == "https" else 80
        addresses = {
            item[4][0]
            for item in socket.getaddrinfo(parsed.hostname, parsed.port or default_port)
        }
    except OSError as exc:
        raise ValueError("Friend hostname could not be resolved") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if (
            ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_unspecified
            or ip.is_reserved
        ):
            raise ValueError("Friend URL resolves to a disallowed address")
    return raw


def _read_limited_json(response, *, deadline=None, budget=None):
    chunks = []
    size = 0
    for chunk in response.iter_content(64 * 1024):
        if deadline is not None and time.monotonic() >= deadline:
            raise ValueError("Friend sync deadline exceeded")
        size += len(chunk)
        if budget is not None:
            budget[0] -= len(chunk)
            if budget[0] < 0:
                raise ValueError("Friend catalogue exceeds total transfer budget")
        if size > MAX_REMOTE_RESPONSE_BYTES:
            raise ValueError("Friend catalog response is too large")
        chunks.append(chunk)
    return json.loads(b"".join(chunks).decode("utf-8"))


def _fetch_remote_catalog(base_url, token):
    endpoint = base_url + "/plugins/federated_albums/api/fingerprints"
    cursor = None
    albums = []
    remote_instance = None
    deadline = time.monotonic() + 300
    seen_cursors = set()
    seen_albums = set()
    budget = [64 * 1024 * 1024]
    for page_number in range(400):
        if time.monotonic() >= deadline:
            raise ValueError("Friend sync deadline exceeded")
        params = {"limit": CATALOG_PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor
        response = requests.get(
            endpoint,
            params=params,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=(5, 30),
            allow_redirects=False,
            stream=True,
        )
        try:
            if response.status_code != 200:
                raise ValueError(
                    f"Friend rejected catalog request ({response.status_code})"
                )
            payload = _read_limited_json(response, deadline=deadline, budget=budget)
        finally:
            response.close()
        if payload.get("capability") != CAPABILITY:
            raise ValueError("Friend uses an incompatible Album Dynamics contract")
        page_instance = str(payload.get("instanceId") or "")
        if not page_instance or (remote_instance and page_instance != remote_instance):
            raise ValueError("Friend catalog returned an invalid instance identity")
        remote_instance = page_instance
        page = payload.get("albums") or []
        if not isinstance(page, list) or len(page) > CATALOG_PAGE_SIZE:
            raise ValueError("Invalid friend catalogue page")
        for item in page:
            key = str(item.get("albumKey") or "")
            if not key or len(key) > 256 or key in seen_albums:
                raise ValueError("Invalid or repeated friend album identity")
            seen_albums.add(key)
            fingerprint = item.get("fingerprint")
            parsed = deserialize_fingerprint(fingerprint, key)
            catalog_store.buckets(parsed["meanVector"])
            albums.append(
                {
                    "album_key": key,
                    "album": str(item.get("album") or "")[:1000],
                    "artist": str(item.get("artist") or "")[:1000],
                    "year": _int_or_none(item.get("year")),
                    "track_count": _int_or_none(item.get("trackCount")) or 0,
                    "fingerprint": fingerprint,
                    "updated_at": item.get("updatedAt")
                    or datetime.now(timezone.utc).isoformat(),
                }
            )
            if len(albums) > MAX_REMOTE_ALBUMS:
                raise ValueError("Friend catalog exceeds the safety limit")
        cursor = payload.get("nextCursor")
        if not cursor:
            return remote_instance, albums
        if (
            not isinstance(cursor, str)
            or len(cursor) > 512
            or cursor in seen_cursors
            or not page
        ):
            raise ValueError("Friend pagination made no progress")
        seen_cursors.add(cursor)
    raise ValueError("Friend catalogue exceeds page limit")


def sync_connection(connection_id, owner, token):
    """Worker-only publication: recheck owner and token after bounded network I/O."""
    names = _tables()
    db = get_db()
    with db.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            f"SELECT * FROM {names['connections']} WHERE id=%s AND owner=%s AND sync_token=%s AND sync_status='running'",
            (connection_id, owner, token),
        )
        row = cur.fetchone()
    db.commit()
    if not row:
        raise LookupError("Friend connection not found")
    base_url = _validate_base_url(row["base_url"])
    remote_instance, albums = _fetch_remote_catalog(base_url, row["access_token"])
    if remote_instance == _instance_id():
        raise ValueError("This connection points back to this AudioMuse instance")
    with db.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {names['connections']} WHERE id=%s AND owner=%s AND sync_token=%s AND sync_status='running' FOR UPDATE",
            (connection_id, owner, token),
        )
        if not cur.fetchone():
            db.rollback()
            raise LookupError("Friend connection was replaced or deleted")
        cur.execute(
            f"DELETE FROM {names['remote']} WHERE connection_id=%s", (connection_id,)
        )
        for item in albums:
            vector = deserialize_fingerprint(item["fingerprint"], item["album_key"])[
                "meanVector"
            ]
            cur.execute(
                f"""INSERT INTO {names['remote']}
                (connection_id,remote_instance_id,album_key,album,artist,year,track_count,fingerprint,buckets,updated_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    connection_id,
                    remote_instance,
                    item["album_key"],
                    item["album"],
                    item["artist"],
                    item["year"],
                    item["track_count"],
                    Json(item["fingerprint"]),
                    catalog_store.buckets(vector),
                    item["updated_at"],
                ),
            )
        cur.execute(
            f"""UPDATE {names['connections']} SET remote_instance_id=%s,last_synced_at=now(),
            last_error=NULL,sync_status='complete',sync_retry_at=NULL WHERE id=%s AND owner=%s AND sync_token=%s""",
            (remote_instance, connection_id, owner, token),
        )
    db.commit()
    return {"instanceId": remote_instance, "albums": len(albums)}


def sync_reconcile_task():
    return sync_jobs.run_one(sync_connection)


def _queue_friend_sync(connection_id, owner):
    result = sync_jobs.request_sync(get_db(), connection_id, owner)
    try:
        enqueue(sync_reconcile_task)
    except Exception:
        logger.exception("Friend sync is durable; reconciliation will retry")
    return result


@bp.get("/api/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "instanceId": _instance_id(),
            "capability": CAPABILITY,
            "hostCapabilities": HOST_CAPABILITIES,
        }
    )


@bp.get("/api/fingerprints")
def fingerprints():
    names = _tables()
    cursor = str(request.args.get("cursor") or "")
    try:
        limit = max(
            1,
            min(CATALOG_PAGE_SIZE, int(request.args.get("limit") or CATALOG_PAGE_SIZE)),
        )
    except (TypeError, ValueError):
        limit = CATALOG_PAGE_SIZE
    db = get_db()
    with db.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            f"""
            SELECT album_key, album, artist, year, track_count, fingerprint, updated_at
            FROM {names['albums']}
            WHERE album_key > %s
            ORDER BY album_key
            LIMIT %s
            """,
            (cursor, limit + 1),
        )
        rows = [dict(row) for row in cur.fetchall()]
    has_more = len(rows) > limit
    rows = rows[:limit]
    albums = []
    for row in rows:
        albums.append(
            {
                "albumKey": row["album_key"],
                "album": row["album"],
                "artist": row["artist"],
                "year": row.get("year"),
                "trackCount": row["track_count"],
                "fingerprint": _json_fingerprint(row["fingerprint"]),
                "updatedAt": row["updated_at"].isoformat(),
            }
        )
    return jsonify(
        {
            "instanceId": _instance_id(),
            "capability": CAPABILITY,
            "albums": albums,
            "nextCursor": rows[-1]["album_key"] if has_more and rows else None,
        }
    )


@bp.route("/api/pairing-tokens", methods=["GET", "POST"])
def pairing_tokens():
    if getattr(g, "auth_method", None) == "plugin_bearer":
        return jsonify({"error": "Scoped friend tokens cannot manage pairing"}), 403
    names = _tables()
    db = get_db()
    username = _owner()
    if request.method == "POST":
        if not HOST_CAPABILITIES["scoped_bearer_auth"]:
            return (
                jsonify(
                    {
                        "error": "Host does not support scoped plugin bearer authentication"
                    }
                ),
                503,
            )
        payload = request.get_json(silent=True) or {}
        label = str(payload.get("label") or "Friend").strip()[:120]
        raw = TOKEN_PREFIX + secrets.token_urlsafe(32)
        with db.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {names['tokens']} (token_hash, token_hint, username, label)
                VALUES (%s, %s, %s, %s) RETURNING id
                """,
                (_token_hash(raw), raw[-6:], username, label),
            )
            token_id = cur.fetchone()[0]
        db.commit()
        return jsonify({"id": token_id, "token": raw, "label": label}), 201
    with db.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, token_hint, label, created_at, last_used_at
            FROM {names['tokens']}
            WHERE username = %s AND revoked_at IS NULL ORDER BY created_at DESC
            """,
            (username,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    return jsonify({"tokens": rows})


@bp.delete("/api/pairing-tokens/<int:token_id>")
def revoke_pairing_token(token_id):
    if getattr(g, "auth_method", None) == "plugin_bearer":
        return jsonify({"error": "Forbidden"}), 403
    names = _tables()
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE {names['tokens']} SET revoked_at = NOW() WHERE id = %s AND username = %s",
            (token_id, _owner()),
        )
        changed = cur.rowcount
    db.commit()
    return (
        (jsonify({"status": "revoked"}), 200)
        if changed
        else (jsonify({"error": "Not found"}), 404)
    )


def _connection_json(row):
    return {
        "id": row["id"],
        "name": row["name"],
        "baseUrl": row["base_url"],
        "instanceId": row.get("remote_instance_id"),
        "lastSyncedAt": (
            row["last_synced_at"].isoformat() if row.get("last_synced_at") else None
        ),
        "lastError": row.get("last_error"),
        "syncStatus": row.get("sync_status"),
    }


@bp.route("/api/connections", methods=["GET", "POST"])
def connections():
    if getattr(g, "auth_method", None) == "plugin_bearer":
        return jsonify({"error": "Forbidden"}), 403
    names = _tables()
    db = get_db()
    owner = _owner()
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        try:
            base_url = _validate_base_url(payload.get("baseUrl"))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        token = str(payload.get("token") or "").strip()
        if not token.startswith(TOKEN_PREFIX):
            return (
                jsonify(
                    {"error": "A Friend Album Discovery pairing token is required"}
                ),
                400,
            )
        name = str(
            payload.get("name") or urlparse(base_url).hostname or "Friend"
        ).strip()[:120]
        try:
            with db.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {names['connections']} (owner, name, base_url, access_token)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (owner, base_url) DO UPDATE SET
                        name = EXCLUDED.name, access_token = EXCLUDED.access_token,
                        last_error = NULL, sync_status='idle', sync_token=NULL
                    RETURNING id
                    """,
                    (owner, name, base_url, token),
                )
                connection_id = cur.fetchone()[0]
            db.commit()
            result = _queue_friend_sync(connection_id, owner)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 502
        return jsonify({"id": connection_id, **result}), 202
    with db.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            f"SELECT * FROM {names['connections']} WHERE owner=%s ORDER BY name",
            (owner,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    return jsonify({"connections": [_connection_json(row) for row in rows]})


@bp.post("/api/connections/<int:connection_id>/sync")
def sync_connection_api(connection_id):
    try:
        return jsonify(_queue_friend_sync(connection_id, _owner())), 202
    except LookupError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@bp.delete("/api/connections/<int:connection_id>")
def delete_connection(connection_id):
    names = _tables()
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {names['connections']} WHERE id = %s AND owner=%s FOR UPDATE",
            (connection_id, _owner()),
        )
        if not cur.fetchone():
            return jsonify({"error": "Not found"}), 404
        cur.execute(
            f"DELETE FROM {names['remote']} WHERE connection_id = %s", (connection_id,)
        )
        cur.execute(
            f"DELETE FROM {names['connections']} WHERE id = %s AND owner=%s",
            (connection_id, _owner()),
        )
    db.commit()
    return jsonify({"status": "deleted"})


@bp.get("/api/albums/search")
def search_albums_api():
    query = _normalise_identity(request.args.get("q"))
    if len(query) < 2:
        return jsonify({"albums": []})
    albums = (
        _local_albums(False, query=query, limit=50)
        + _remote_albums(False, query=query, limit=50)
    )[:50]
    return jsonify({"albums": [_public_album(item) for item in albums]})


def _find_source(payload, albums):
    key = str(payload.get("albumKey") or "")
    instance_id = str(payload.get("instanceId") or _instance_id())
    return next(
        (
            item
            for item in albums
            if item["albumKey"] == key and item["instanceId"] == instance_id
        ),
        None,
    )


@bp.post("/api/similar-albums")
def similar_albums_api():
    payload = request.get_json(silent=True) or {}
    limit = max(1, min(30, _int_or_none(payload.get("limit")) or 12))
    instance = str(payload.get("instanceId") or _instance_id())
    key = str(
        payload.get("albumKey")
        or album_key(payload.get("artist"), payload.get("album"))
    )
    sources = (
        _local_albums(True, album_key=key, limit=1)
        if instance == _instance_id()
        else _remote_albums(True, album_key=key, instance_id=instance, limit=1)
    )
    if not sources:
        return jsonify({"error": "Album not found"}), 404
    source = sources[0]
    vector = source["fingerprint"]["meanVector"]
    albums = _local_albums(True, vector=vector, limit=256) + _remote_albums(
        True, vector=vector, limit=256
    )
    ranked = rank_similar_albums(source, albums, limit=limit)
    return jsonify(
        {
            "source": _public_album(source),
            "albums": [_public_album(item) for item in ranked],
            "capability": CAPABILITY,
            "candidateSearch": "bounded-lsh-v1",
        }
    )


@bp.post("/api/album-recommendations")
def album_recommendations_api():
    payload = request.get_json(silent=True) or {}
    source = str(payload.get("source") or "server")
    try:
        if source == "provided":
            centroid = decode_vector(payload.get("centroid"))
        elif source == "server":
            centroid = calculate_sonic_fingerprint_vector()
        else:
            return jsonify({"error": "source must be 'server' or 'provided'"}), 400
    except (TypeError, ValueError) as exc:
        return jsonify({"error": f"Invalid Sonic Fingerprint: {exc}"}), 400
    if centroid is None or np.asarray(centroid).size == 0:
        return (
            jsonify({"error": "Not enough listening history for a Sonic Fingerprint"}),
            409,
        )
    if np.asarray(centroid).size != 200:
        return (
            jsonify({"error": "Sonic Fingerprint must use a 200D MusiCNN vector"}),
            400,
        )
    try:
        catalog_store.buckets(centroid)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    friends = _remote_albums(True, vector=centroid, limit=512, exclude_owned=True)
    owned = set()
    ranked = rank_for_sonic_fingerprint(centroid, friends, owned, limit=3)
    return jsonify(
        {"albums": [_public_album(item) for item in ranked], "capability": CAPABILITY}
    )


@bp.post("/api/rebuild")
def rebuild_api():
    if (
        getattr(g, "auth_role", None) != "admin"
        and getattr(g, "auth_method", None) != "bearer"
    ):
        return jsonify({"error": "Admin access required"}), 403
    job = enqueue(rebuild_catalog)
    return jsonify({"status": "queued", "jobId": getattr(job, "id", None)}), 202


PAGE = """
<style>
  .fa-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:1rem}
  .fa-card{border:1px solid var(--border-color,#39404d);border-radius:12px;padding:1rem;background:var(--card-bg,#20252d)}
  .fa-row{display:flex;gap:.5rem;flex-wrap:wrap;margin:.5rem 0}.fa-row input{flex:1;min-width:160px}
  .fa-album{padding:.65rem 0;border-bottom:1px solid rgba(127,127,127,.25)}
  .fa-badge{font-size:.75rem;padding:.15rem .4rem;border-radius:99px;background:#6554c0;color:white}
  #fa-status{min-height:1.4rem;margin:.75rem 0}.fa-muted{opacity:.72}
</style>
<p class="fa-muted">Album-level Lumae dynamics are shared. Audio, track identities, listening history, and individual track embeddings stay private.</p>
<div id="fa-status"></div>
<div class="fa-grid">
  <section class="fa-card"><h3>Your three friend picks</h3><button onclick="faRecommend()">Recommend albums</button><div id="fa-recs"></div></section>
  <section class="fa-card"><h3>Find similar albums</h3><div class="fa-row"><input id="fa-query" placeholder="Album or artist"><button onclick="faSearch()">Search</button></div><div id="fa-search"></div><div id="fa-similar"></div></section>
  <section class="fa-card"><h3>Connect a friend</h3><div class="fa-row"><input id="fa-name" placeholder="Friend name"><input id="fa-url" placeholder="https://friend.example"><input id="fa-token" placeholder="afa_ pairing token"><button onclick="faConnect()">Connect</button></div><div id="fa-connections"></div></section>
  <section class="fa-card"><h3>Let a friend connect</h3><div class="fa-row"><input id="fa-label" placeholder="Friend label"><button onclick="faToken()">Create pairing token (shown once)</button></div><pre id="fa-new-token"></pre><div id="fa-tokens"></div></section>
</div>
<script>
const faApi=(path,options={})=>fetch('api/'+path,{headers:{'Content-Type':'application/json',...(options.headers||{})},...options}).then(async r=>{const j=await r.json();if(!r.ok)throw new Error(j.error||r.statusText);return j});
const faEsc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const faAlbum=a=>`<div class="fa-album"><strong>${faEsc(a.album)}</strong><br>${faEsc(a.artist)} ${a.availability==='friend'?`<span class="fa-badge">${faEsc(a.sourceName)}</span>`:''}</div>`;
const faRun=async fn=>{const s=document.getElementById('fa-status');s.textContent='Working...';try{await fn();s.textContent=''}catch(e){s.textContent=e.message}};
async function faRecommend(){faRun(async()=>{const x=await faApi('album-recommendations',{method:'POST',body:JSON.stringify({source:'server'})});document.getElementById('fa-recs').innerHTML=x.albums.map(faAlbum).join('')||'<p>No unowned friend albums found.</p>'})}
async function faSearch(){faRun(async()=>{const x=await faApi('albums/search?q='+encodeURIComponent(document.getElementById('fa-query').value));document.getElementById('fa-search').innerHTML=x.albums.map(a=>`<button style="display:block;width:100%;text-align:left" data-album-key="${faEsc(a.albumKey)}" data-instance-id="${faEsc(a.instanceId)}" onclick="faSimilar(JSON.stringify({albumKey:this.dataset.albumKey,instanceId:this.dataset.instanceId}))">${faAlbum(a)}</button>`).join('')})}
async function faSimilar(raw){faRun(async()=>{const x=await faApi('similar-albums',{method:'POST',body:raw});document.getElementById('fa-similar').innerHTML='<h4>Similar</h4>'+x.albums.map(faAlbum).join('')})}
async function faConnect(){faRun(async()=>{await faApi('connections',{method:'POST',body:JSON.stringify({name:document.getElementById('fa-name').value,baseUrl:document.getElementById('fa-url').value,token:document.getElementById('fa-token').value})});await faLoad()})}
async function faToken(){faRun(async()=>{const x=await faApi('pairing-tokens',{method:'POST',body:JSON.stringify({label:document.getElementById('fa-label').value})});document.getElementById('fa-new-token').textContent=x.token+'\nCopy now; it will not be shown again.';await faLoad()})}
async function faLoad(){const [c,t]=await Promise.all([faApi('connections'),faApi('pairing-tokens')]);document.getElementById('fa-connections').innerHTML=c.connections.map(x=>`<div class="fa-album"><strong>${faEsc(x.name)}</strong><br>${faEsc(x.baseUrl)}<br><small>${x.lastError?faEsc(x.lastError):['pending','running'].includes(x.syncStatus)?faEsc(x.syncStatus):x.lastSyncedAt?'Synced '+faEsc(x.lastSyncedAt):'Not synced'}</small></div>`).join('')||'<p>No friends connected.</p>';document.getElementById('fa-tokens').innerHTML=t.tokens.map(x=>`<div class="fa-album">${faEsc(x.label)} - ...${faEsc(x.token_hint)}</div>`).join('')||'<p>No active tokens.</p>'}
setInterval(()=>{if(!document.hidden)faLoad().catch(()=>{})},15000);
faLoad().catch(e=>document.getElementById('fa-status').textContent=e.message);
</script>
"""


@bp.get("/")
def index():
    return render_page(PAGE, title="Friend Album Discovery")


def register(ctx):
    ctx.add_blueprint(bp)
    ctx.add_menu_item("Friend Albums", "federated_albums.index")
    register_auth = getattr(ctx, "set_bearer_authenticator", None)
    HOST_CAPABILITIES["scoped_bearer_auth"] = callable(register_auth)
    if callable(register_auth):
        register_auth(authenticate_friend_bearer)
    ctx.add_task("rebuild_catalog", rebuild_catalog)
    ctx.add_task("sync_reconcile", sync_reconcile_task)
    ctx.add_cron_task("sync_reconcile", sync_reconcile_task)
    ctx.on_install(migrate)
    ctx.on_flask_start(migrate)
    ctx.on_worker_start(enqueue_initial_rebuild)
    ctx.on_song_analyzed(song_analyzed)
