"""Media-library browsing and credential-safe preview routes for collections."""

import re
from urllib.parse import quote

import requests as http_requests
from flask import Response, jsonify, request, stream_with_context

from plugin.api import config, get_db, logger, table


LIBRARY_SCOPES = {"all", "albums", "tracks", "artists"}
LIBRARY_SORTS = {"title", "artist", "year"}
# A provider item id in a stream or art path. Dot-only ids (".", "..") would
# walk the provider URL they are placed in, so they are refused.
_ITEM_ID_RE = re.compile(r"(?!\.+\Z)[A-Za-z0-9._~-]{1,256}")
_REQUEST_TIMEOUT = (10, 60)
_unaccent_warned = False


def unaccent_available(cur):
    """Whether ``unaccent(text)`` resolves on this connection's search_path.

    The migration creates the extension when the role may; without it, search
    matches case-insensitively but not accent-insensitively (logged once).
    """
    global _unaccent_warned
    cur.execute("SELECT to_regprocedure('unaccent(text)') IS NOT NULL")
    row = cur.fetchone()
    available = bool(row and row[0])
    if not available and not _unaccent_warned:
        _unaccent_warned = True
        logger.warning(
            "Living Collections search is not accent-insensitive: the PostgreSQL "
            "unaccent extension is not installed (a database owner can run "
            "CREATE EXTENSION unaccent)"
        )
    return available


def catalog_track_view_sql(unaccent=True):
    """Current provider catalogue rows with analysis as an optional link.

    ``search_u`` is the lower-cased search text, accent-folded when
    ``unaccent`` is true (see ``unaccent_available``). A query that does not
    filter on ``search_u`` passes ``unaccent=False`` and needs no extension.
    """
    search_text = "concat_ws(' ', t.title, t.artist_display, t.album_artist_display, al.name)"
    if unaccent:
        search_text = f"unaccent({search_text})"
    sources = table("catalog_sources")
    state = table("catalog_state")
    tracks = table("catalog_tracks")
    albums = table("catalog_albums")
    analysis_state = table("analysis_state")
    links = table("track_analysis_links")
    return f"""
        WITH selected_source AS (
            SELECT s.catalog_instance_id, s.provider_type, c.published_generation,
                   COALESCE(a.projection_generation, 0) AS projection_generation
              FROM {sources} s
              JOIN {state} c USING (catalog_instance_id)
              LEFT JOIN {analysis_state} a USING (catalog_instance_id)
             WHERE s.rebind_status='active' AND c.status='complete'
             ORDER BY s.is_default DESC, s.server_name, s.catalog_instance_id
             LIMIT 1
        )
        SELECT t.track_id AS item_id, t.title,
               t.artist_display AS author, al.name AS album,
               t.album_artist_display AS album_artist,
               NULL::INTEGER AS year, NULL::INTEGER AS rating,
               t.track_id AS cover_item_id, t.album_id,
               t.track_number, t.disc_number, t.duration_ms,
               t.content_kind, t.release_type, t.cover_art_id,
               l.status AS analysis_status,
               lower({search_text}) AS search_u,
               source.provider_type
          FROM selected_source source
          JOIN {tracks} t
            ON t.catalog_instance_id=source.catalog_instance_id
           AND t.published_generation=source.published_generation
           AND t.available=TRUE
          LEFT JOIN {albums} al
            ON al.catalog_instance_id=t.catalog_instance_id
           AND al.published_generation=t.published_generation
           AND al.album_id=t.album_id AND al.available=TRUE
          LEFT JOIN {links} l
            ON l.catalog_instance_id=t.catalog_instance_id
           AND l.projection_generation=source.projection_generation
           AND l.provider_track_id=t.track_id
    """


def _bounded_int(value, default, minimum, maximum):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return min(max(parsed, minimum), maximum)


def _json_value(value):
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    return value


def _all_dicts(cur):
    rows = cur.fetchall()
    names = [column[0] for column in cur.description]
    return [
        {name: _json_value(value) for name, value in zip(names, row)}
        for row in rows
    ]


def _album_key(title, artist):
    return f"{str(artist or '').casefold()}::{str(title or '').casefold()}"


def _library_filters(query, artist=None, unaccent=True):
    clauses = []
    params = []
    query = str(query or "").strip()
    if query:
        # search_u is the lower-cased (and, with unaccent, accent-folded)
        # catalogue text. AND-ing normalized tokens makes multi-word queries
        # useful without returning the huge partial-word scans that froze the
        # original UI.
        for token in query.casefold().split()[:8]:
            clauses.append("search_u LIKE unaccent(%s)" if unaccent else "search_u LIKE %s")
            params.append(f"%{token}%")
    if artist:
        clauses.append(
            "(album_artist = %s OR (NULLIF(album_artist, '') IS NULL AND author = %s))"
        )
        params.extend([str(artist), str(artist)])
    return (" AND " + " AND ".join(clauses)) if clauses else "", params


def _browse_albums(cur, query, artist, sort, limit, offset, unaccent=True):
    filters, params = _library_filters(query, artist, unaccent)
    order = {
        "title": "lower(title), lower(artist)",
        "artist": "lower(artist), lower(title)",
        "year": "year DESC NULLS LAST, lower(title)",
    }[sort]
    cur.execute(
        f"""
        SELECT title, artist, cover_item_id, track_count, year, rating,
               COUNT(*) OVER()::INTEGER AS total_count
          FROM (
            SELECT album AS title,
                   COALESCE(NULLIF(album_artist, ''), author) AS artist,
                   MIN(item_id) AS cover_item_id,
                   COUNT(*)::INTEGER AS track_count,
                   MIN(year)::INTEGER AS year,
                   MAX(rating)::INTEGER AS rating
              FROM ({catalog_track_view_sql(unaccent)}) score
             WHERE NULLIF(album, '') IS NOT NULL {filters}
             GROUP BY album, COALESCE(NULLIF(album_artist, ''), author)
          ) albums
         ORDER BY {order}
         LIMIT %s OFFSET %s
        """,
        tuple(params + [limit, offset]),
    )
    rows = _all_dicts(cur)
    total = int(rows[0].pop("total_count", 0)) if rows else 0
    for row in rows:
        row.pop("total_count", None)
        row.update(
            {
                "kind": "album",
                "album_key": _album_key(row.get("title"), row.get("artist")),
                "provider_album_id": None,
            }
        )
    return {"items": rows, "total": total}


def _browse_tracks(cur, query, artist, sort, limit, offset, unaccent=True):
    filters, params = _library_filters(query, artist, unaccent)
    order = {
        # `artist` is a SELECT alias below. PostgreSQL permits a bare output
        # alias in ORDER BY, but not one nested inside lower(...), so sort on
        # the source column here rather than raising UndefinedColumn at runtime.
        "title": "lower(title), lower(COALESCE(author, '')), lower(COALESCE(album, ''))",
        "artist": "lower(COALESCE(author, '')), lower(COALESCE(album, '')), lower(title)",
        "year": "year DESC NULLS LAST, lower(COALESCE(author, '')), lower(title)",
    }[sort]
    cur.execute(
        f"""
        SELECT item_id AS track_id, title, author AS artist, album,
               COALESCE(NULLIF(album_artist, ''), author) AS album_artist,
               year, rating, item_id AS cover_item_id,
               COUNT(*) OVER()::INTEGER AS total_count
          FROM ({catalog_track_view_sql(unaccent)}) score
         WHERE NULLIF(title, '') IS NOT NULL {filters}
         ORDER BY {order}
         LIMIT %s OFFSET %s
        """,
        tuple(params + [limit, offset]),
    )
    rows = _all_dicts(cur)
    total = int(rows[0].pop("total_count", 0)) if rows else 0
    for row in rows:
        row.pop("total_count", None)
        row["kind"] = "track"
    return {"items": rows, "total": total}


def _browse_artists(cur, query, sort, limit, offset, unaccent=True):
    filters, params = _library_filters(query, unaccent=unaccent)
    order = {
        "title": "lower(artist)",
        "artist": "lower(artist)",
        "year": "latest_year DESC NULLS LAST, lower(artist)",
    }[sort]
    cur.execute(
        f"""
        SELECT artist, cover_item_id, album_count, track_count,
               first_year, latest_year,
               COUNT(*) OVER()::INTEGER AS total_count
          FROM (
            SELECT COALESCE(NULLIF(album_artist, ''), author) AS artist,
                   MIN(item_id) AS cover_item_id,
                   COUNT(DISTINCT NULLIF(album, ''))::INTEGER AS album_count,
                   COUNT(*)::INTEGER AS track_count,
                   MIN(year)::INTEGER AS first_year,
                   MAX(year)::INTEGER AS latest_year
              FROM ({catalog_track_view_sql(unaccent)}) score
             WHERE NULLIF(COALESCE(NULLIF(album_artist, ''), author), '') IS NOT NULL
                   {filters}
             GROUP BY COALESCE(NULLIF(album_artist, ''), author)
          ) artists
         ORDER BY {order}
         LIMIT %s OFFSET %s
        """,
        tuple(params + [limit, offset]),
    )
    rows = _all_dicts(cur)
    total = int(rows[0].pop("total_count", 0)) if rows else 0
    for row in rows:
        row.pop("total_count", None)
        row.update({"kind": "artist", "title": row.get("artist")})
    return {"items": rows, "total": total}


def browse_library(scope="albums", query="", artist=None, sort="title", page=1, limit=36):
    """Return one page of analyzed media grouped by a stable library scope."""
    scope = scope if scope in LIBRARY_SCOPES else "albums"
    sort = sort if sort in LIBRARY_SORTS else "title"
    page = _bounded_int(page, 1, 1, 100000)
    limit = _bounded_int(limit, 36, 1, 100)
    query = str(query or "").strip()
    if query and len(query) < 3:
        keys = ("albums", "tracks", "artists") if scope == "all" else (scope,)
        return {
            "scope": scope,
            "query": query,
            "artist": artist,
            "sort": sort,
            "page": page,
            "limit": limit,
            "sections": {key: {"items": [], "total": 0} for key in keys},
        }
    offset = (page - 1) * limit
    db = get_db()
    cur = db.cursor()
    try:
        folded = unaccent_available(cur)
        if scope == "albums":
            sections = {"albums": _browse_albums(cur, query, artist, sort, limit, offset, folded)}
        elif scope == "tracks":
            sections = {"tracks": _browse_tracks(cur, query, artist, sort, limit, offset, folded)}
        elif scope == "artists":
            sections = {"artists": _browse_artists(cur, query, sort, limit, offset, folded)}
        else:
            # A broad search intentionally returns compact categorized sections.
            section_limit = min(limit, 12)
            sections = {
                "albums": _browse_albums(cur, query, artist, sort, section_limit, 0, folded),
                "tracks": _browse_tracks(cur, query, artist, sort, section_limit, 0, folded),
                "artists": _browse_artists(cur, query, sort, section_limit, 0, folded),
            }
    finally:
        cur.close()
    return {
        "scope": scope,
        "query": query,
        "artist": artist,
        "sort": sort,
        "page": page,
        "limit": limit,
        "sections": sections,
    }


def library_stats():
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT COUNT(*)::INTEGER AS track_count,
                   COUNT(DISTINCT (
                     lower(COALESCE(NULLIF(album_artist, ''), author)) || E'\\x1f' ||
                     lower(COALESCE(album, ''))
                   )) FILTER (WHERE NULLIF(album, '') IS NOT NULL)::INTEGER AS album_count,
                   COUNT(DISTINCT lower(COALESCE(NULLIF(album_artist, ''), author)))::INTEGER
                     AS artist_count
              FROM ({catalog_track_view_sql(unaccent=False)}) score
            """
        )
        row = cur.fetchone() or (0, 0, 0)
    finally:
        cur.close()
    return {
        "track_count": int(row[0] or 0),
        "album_count": int(row[1] or 0),
        "artist_count": int(row[2] or 0),
    }


def _pick(item, *keys):
    for key in keys:
        value = item.get(key)
        if value is not None:
            return value
    return None


def _optional_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _duration_seconds(item):
    ticks = _pick(item, "RunTimeTicks", "runTimeTicks")
    if ticks not in (None, ""):
        try:
            return round(float(ticks) / 10_000_000)
        except (TypeError, ValueError):
            pass
    value = _pick(item, "duration", "Duration", "duration_seconds")
    try:
        return round(float(value)) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _normalize_provider_track(item, index=0):
    track_id = _pick(item, "Id", "id", "track_id")
    title = _pick(item, "Name", "name", "title")
    artist = _pick(item, "AlbumArtist", "artist", "author", "trackartist")
    album_artist = _pick(
        item, "OriginalAlbumArtist", "albumArtist", "AlbumArtist", "albumartist"
    )
    return {
        "kind": "track",
        "track_id": str(track_id) if track_id is not None else None,
        "title": title or "Unknown track",
        "artist": artist or album_artist or "Unknown artist",
        "album_artist": album_artist or artist or "Unknown artist",
        "album": _pick(item, "Album", "album"),
        "year": _optional_int(_pick(item, "Year", "year", "ProductionYear")),
        "track_number": _optional_int(
            _pick(item, "IndexNumber", "track_number", "trackNumber", "track")
        ),
        "disc_number": _optional_int(
            _pick(item, "ParentIndexNumber", "disc_number", "discNumber", "disc")
        ),
        "duration_seconds": _duration_seconds(item),
        "cover_item_id": str(
            _pick(
                item,
                "coverArt",
                "CoverArt",
                "artwork_track_id",
                "coverid",
                "Id",
                "id",
                "track_id",
            )
            or ""
        )
        or None,
        "provider_index": index,
    }


def _score_album_tracks(title, artist, provider_album_id=None):
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            f"""
            SELECT item_id AS track_id, title, author AS artist, album,
                   COALESCE(NULLIF(album_artist, ''), author) AS album_artist,
                   year, rating, item_id AS cover_item_id, album_id,
                   track_number, disc_number,
                   CASE WHEN duration_ms IS NULL THEN NULL ELSE round(duration_ms / 1000.0) END
                     AS duration_seconds,
                   analysis_status, provider_type
              FROM ({catalog_track_view_sql(unaccent=False)}) score
             WHERE ((%s IS NOT NULL AND album_id=%s) OR
                    (%s IS NULL AND lower(album) = lower(%s)
                     AND lower(COALESCE(NULLIF(album_artist, ''), author)) = lower(%s)))
             ORDER BY lower(title), item_id
            """,
            (provider_album_id, provider_album_id, provider_album_id, title, artist),
        )
        rows = _all_dicts(cur)
    finally:
        cur.close()
    for index, row in enumerate(rows):
        row.update(
            {
                "kind": "track",
                "analyzed": row.pop("analysis_status", None) in {"ready", "suspect"},
                "provider_index": index,
            }
        )
    return rows


def album_detail(title, artist, provider_album_id=None):
    """Load provider-authoritative order and metadata from the published mirror."""
    tracks = _score_album_tracks(title, artist, provider_album_id=provider_album_id)
    tracks.sort(
        key=lambda item: (
            item.get("disc_number") or 1,
            item.get("track_number") if item.get("track_number") is not None else 1_000_000,
            item.get("provider_index", 0),
        )
    )
    resolved_album_id = provider_album_id or next(
        (item.get("album_id") for item in tracks if item.get("album_id")), None
    )
    provider_type = next(
        (item.get("provider_type") for item in tracks if item.get("provider_type")), "unknown"
    )
    album = {
        "kind": "album",
        "title": title,
        "artist": artist,
        "album_key": _album_key(title, artist),
        "provider_album_id": str(resolved_album_id) if resolved_album_id else None,
        "year": next((item.get("year") for item in tracks if item.get("year")), None),
        "track_count": len(tracks),
        "cover_item_id": next(
            (item.get("cover_item_id") or item.get("track_id") for item in tracks), None
        ),
    }
    return {
        "album": album,
        "tracks": tracks,
        "metadata_source": "provider_catalog",
        "provider_type": provider_type,
    }


def _provider_headers(provider_type):
    if provider_type in {"jellyfin", "emby"}:
        return dict(getattr(config, "HEADERS", {}) or {})
    return {}


def _resolve_stream_target(item_id):
    provider_type = str(getattr(config, "MEDIASERVER_TYPE", "") or "").lower()
    if provider_type == "jellyfin":
        return (
            f"{str(getattr(config, 'JELLYFIN_URL', '')).rstrip('/')}/Items/{quote(item_id)}/Download",
            _provider_headers(provider_type),
            None,
        ), None
    if provider_type == "emby":
        return (
            f"{str(getattr(config, 'EMBY_URL', '')).rstrip('/')}/Items/{quote(item_id)}/Download",
            _provider_headers(provider_type),
            None,
        ), None
    if provider_type == "navidrome":
        from tasks.mediaserver.navidrome import get_navidrome_auth_params

        auth = get_navidrome_auth_params()
        if not auth:
            return None, ("Navidrome credentials are not configured", 500)
        return (
            f"{str(getattr(config, 'NAVIDROME_URL', '')).rstrip('/')}/rest/stream.view",
            {},
            {"id": item_id, **auth},
        ), None
    if provider_type == "lyrion":
        return (
            f"{str(getattr(config, 'LYRION_URL', '')).rstrip('/')}/music/{quote(item_id)}/download",
            {},
            None,
        ), None
    if provider_type == "plex":
        from tasks.mediaserver.plex import _resolve_part

        part_key, _ = _resolve_part(item_id)
        if not part_key:
            return None, ("Track stream was not found", 404)
        return (
            f"{str(getattr(config, 'PLEX_URL', '')).rstrip('/')}{part_key}",
            {"X-Plex-Token": getattr(config, "PLEX_TOKEN", "")},
            None,
        ), None
    return None, ("Preview is not supported for this media server", 501)


def _stream_headers(upstream):
    passthrough = (
        "Content-Type",
        "Content-Length",
        "Content-Range",
        "Accept-Ranges",
        "Last-Modified",
        "ETag",
    )
    headers = {
        name: upstream.headers[name]
        for name in passthrough
        if upstream.headers.get(name) is not None
    }
    headers.setdefault("Content-Type", "audio/mpeg")
    headers.setdefault("Accept-Ranges", "bytes")
    headers["Cache-Control"] = "private, no-store"
    return headers


def _proxy_stream(target):
    url, headers, params = target
    headers = dict(headers)
    if request.headers.get("Range"):
        headers["Range"] = request.headers["Range"]
    try:
        upstream = http_requests.get(
            url,
            params=params,
            headers=headers,
            stream=True,
            timeout=_REQUEST_TIMEOUT,
            allow_redirects=True,
        )
    except http_requests.RequestException:
        logger.exception("Living Collections preview could not reach the media server")
        return None, ("Media server preview is unavailable", 502)
    if upstream.status_code >= 400:
        status = upstream.status_code
        upstream.close()
        logger.warning("Living Collections preview upstream returned HTTP %s", status)
        return None, ("Media server preview failed", 502)
    return upstream, None


def _stream_response(upstream):
    def generate():
        try:
            for chunk in upstream.iter_content(chunk_size=64 * 1024):
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    response = Response(
        stream_with_context(generate()),
        status=upstream.status_code,
        headers=_stream_headers(upstream),
    )
    response.call_on_close(upstream.close)
    return response


def _resolve_art_target(item_id, size):
    provider_type = str(getattr(config, "MEDIASERVER_TYPE", "") or "").lower()
    if provider_type == "navidrome":
        from tasks.mediaserver.navidrome import _navidrome_request, get_navidrome_auth_params

        cover_id = item_id
        try:
            song = (_navidrome_request("getSong", {"id": item_id}) or {}).get("song") or {}
            cover_id = str(song.get("coverArt") or item_id)
        except Exception:
            logger.warning("Could not resolve Navidrome cover id for %s", item_id)
        auth = get_navidrome_auth_params()
        return (
            f"{str(getattr(config, 'NAVIDROME_URL', '')).rstrip('/')}/rest/getCoverArt.view",
            {},
            {"id": cover_id, "size": size, **(auth or {})},
        )
    if provider_type == "jellyfin":
        return (
            f"{str(getattr(config, 'JELLYFIN_URL', '')).rstrip('/')}/Items/{quote(item_id)}/Images/Primary",
            _provider_headers(provider_type),
            {"maxWidth": size, "quality": 90},
        )
    if provider_type == "emby":
        return (
            f"{str(getattr(config, 'EMBY_URL', '')).rstrip('/')}/Items/{quote(item_id)}/Images/Primary",
            _provider_headers(provider_type),
            {"maxWidth": size, "quality": 90},
        )
    if provider_type == "lyrion":
        return (
            f"{str(getattr(config, 'LYRION_URL', '')).rstrip('/')}/music/{quote(item_id)}/cover.jpg",
            {},
            {"size": size},
        )
    if provider_type == "plex":
        base = str(getattr(config, "PLEX_URL", "")).rstrip("/")
        headers = {"Accept": "application/json", "X-Plex-Token": getattr(config, "PLEX_TOKEN", "")}
        metadata = http_requests.get(
            f"{base}/library/metadata/{quote(item_id)}",
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        metadata.raise_for_status()
        items = ((metadata.json().get("MediaContainer") or {}).get("Metadata")) or []
        item = items[0] if items else {}
        thumb = item.get("thumb") or item.get("parentThumb") or item.get("grandparentThumb")
        if not thumb:
            return None
        return (f"{base}{thumb}", headers, {"width": size, "height": size})
    return None


def _proxy_art(target):
    if not target:
        return None
    url, headers, params = target
    try:
        upstream = http_requests.get(
            url,
            params=params,
            headers=headers,
            stream=True,
            timeout=_REQUEST_TIMEOUT,
            allow_redirects=True,
        )
    except http_requests.RequestException:
        return None
    if upstream.status_code >= 400:
        upstream.close()
        return None

    def generate():
        try:
            for chunk in upstream.iter_content(chunk_size=32 * 1024):
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    headers_out = {
        "Content-Type": upstream.headers.get("Content-Type", "image/jpeg"),
        "Cache-Control": "private, max-age=86400",
    }
    if upstream.headers.get("ETag"):
        headers_out["ETag"] = upstream.headers["ETag"]
    response = Response(stream_with_context(generate()), status=200, headers=headers_out)
    response.call_on_close(upstream.close)
    return response


def register_collection_library_routes(bp, require_enabled):
    @bp.get("/api/collections/library")
    @require_enabled
    def collection_library_browse():
        return jsonify(
            browse_library(
                scope=str(request.args.get("scope") or "albums").lower(),
                query=request.args.get("q") or "",
                artist=request.args.get("artist") or None,
                sort=str(request.args.get("sort") or "title").lower(),
                page=request.args.get("page") or 1,
                limit=request.args.get("limit") or 36,
            )
        )

    @bp.get("/api/collections/library/stats")
    @require_enabled
    def collection_library_stats():
        return jsonify(library_stats())

    @bp.get("/api/collections/library/album")
    @require_enabled
    def collection_library_album():
        title = str(request.args.get("title") or "").strip()
        artist = str(request.args.get("artist") or "").strip()
        if not title or not artist:
            return jsonify({"error": "Album title and artist are required"}), 400
        return jsonify(
            album_detail(
                title,
                artist,
                provider_album_id=request.args.get("provider_album_id") or None,
            )
        )

    @bp.get("/api/collections/library/stream/<path:item_id>")
    @require_enabled
    def collection_library_stream(item_id):
        if not _ITEM_ID_RE.fullmatch(item_id):
            return jsonify({"error": "Invalid track id"}), 400
        try:
            target, target_error = _resolve_stream_target(item_id)
            if target_error:
                message, status = target_error
                return jsonify({"error": message}), status
            upstream, upstream_error = _proxy_stream(target)
            if upstream_error:
                message, status = upstream_error
                return jsonify({"error": message}), status
            return _stream_response(upstream)
        except Exception:
            logger.exception("Living Collections preview failed for %s", item_id)
            return jsonify({"error": "Preview failed"}), 500

    @bp.get("/api/collections/library/art/<path:item_id>")
    @require_enabled
    def collection_library_art(item_id):
        if not _ITEM_ID_RE.fullmatch(item_id):
            return "", 404
        size = _bounded_int(request.args.get("size"), 320, 48, 1200)
        try:
            response = _proxy_art(_resolve_art_target(item_id, size))
            return response if response is not None else ("", 404)
        except Exception:
            logger.warning("Living Collections artwork failed for %s", item_id)
            return "", 404
