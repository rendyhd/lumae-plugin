"""Media-library browsing and credential-safe preview routes for collections."""

import base64
import json
import re
from urllib.parse import quote

import requests as http_requests
from flask import Response, jsonify, request, stream_with_context

from plugin.api import config, get_db, logger, table


LIBRARY_SCOPES = {"all", "albums", "tracks", "artists"}
# "year" stays accepted for old clients, but the workbench no longer offers
# it: the catalogue publishes no year yet, so it sorted by title (LUM-015).
LIBRARY_SORTS = {"title", "artist", "year"}
CATALOG_PARAM = "catalog_instance_id"
# A provider item id in a stream or art path. Dot-only ids (".", "..") would
# walk the provider URL they are placed in, so they are refused.
_ITEM_ID_RE = re.compile(r"(?!\.+\Z)[A-Za-z0-9._~-]{1,256}")
_REQUEST_TIMEOUT = (10, 60)
_unaccent_warned = False
_stale_search_warned = False
# Section totals are exact up to this many rows, then "1000+" (LUM-016).
TOTAL_CAP = 1000


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


def search_text_sql(folded=True, tracks="t", album_name="al.name"):
    """The workbench search text of a catalogue track, as SQL.

    ``lower([unaccent](concat_ws(' ', title, artist, album artist, album
    name)))``: what ``catalog_tracks.search_text`` stores (LUM-016, written
    at publication by ``catalog_search``) and what a search computes inline
    while the stored text is not usable (see ``_search_state``).
    """
    text = (f"concat_ws(' ', {tracks}.title, {tracks}.artist_display, "
            f"{tracks}.album_artist_display, {album_name})")
    return f"lower(unaccent({text}))" if folded else f"lower({text})"


def catalog_track_view_sql(unaccent=True, stored=False):
    """Current provider catalogue rows with analysis as an optional link.

    The catalogue is the view's one parameter, and it comes first in the
    query: pass the id ``resolve_catalog`` returns (K10). None reads nothing.

    ``search_u`` is the lower-cased search text, accent-folded when
    ``unaccent`` is true (see ``unaccent_available``). A query that does not
    filter on ``search_u`` passes ``unaccent=False`` and needs no extension.
    ``stored=True`` reads it from ``catalog_tracks.search_text`` instead;
    only when ``_search_state`` says it was folded the same way.
    """
    search_u = "t.search_text" if stored else search_text_sql(unaccent)
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
             WHERE s.catalog_instance_id=%s
               AND s.rebind_status='active' AND c.status='complete'
        )
        SELECT t.track_id AS item_id, t.title,
               t.artist_display AS author, al.name AS album,
               t.album_artist_display AS album_artist,
               al.album_artist_display AS album_row_artist,
               NULL::INTEGER AS year, NULL::INTEGER AS rating,
               t.track_id AS cover_item_id, t.album_id,
               t.track_number, t.disc_number, t.duration_ms,
               t.content_kind, t.release_type, t.cover_art_id,
               l.status AS analysis_status,
               {search_u} AS search_u,
               source.provider_type, source.catalog_instance_id
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


class CatalogScopeError(Exception):
    """A workbench request whose catalogue cannot be resolved (K10)."""

    def __init__(self, error, status, **extra):
        super().__init__(error)
        self.error = error
        self.status = status
        self.extra = extra

    def body(self):
        return {"error": self.error, **self.extra}


def requested_catalog():
    """The request's ``catalog_instance_id`` argument; empty means absent."""
    return str(request.args.get(CATALOG_PARAM) or "").strip() or None


def resolve_catalog(cur, catalog_instance_id=None):
    """``(catalog_instance_id, provider_type)`` a workbench request reads (K10).

    An explicit id must name an active source, else 404
    ``catalog_instance_not_found``. Without one, the only active source is
    used, so a single-source client is unchanged; with several, 400
    ``catalog_instance_required`` lists them. With none, ``(None, None)``,
    and reads are empty as before.
    """
    sources = table("catalog_sources")
    if catalog_instance_id is not None:
        cur.execute(
            f"SELECT catalog_instance_id, provider_type FROM {sources} "
            "WHERE catalog_instance_id=%s AND rebind_status='active'",
            (str(catalog_instance_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise CatalogScopeError("catalog_instance_not_found", 404)
        return row[0], str(row[1] or "").lower()
    cur.execute(
        f"SELECT catalog_instance_id, provider_type, server_name, is_default "
        f"FROM {sources} WHERE rebind_status='active' "
        "ORDER BY is_default DESC, server_name, catalog_instance_id"
    )
    rows = cur.fetchall()
    if len(rows) > 1:
        raise CatalogScopeError(
            "catalog_instance_required",
            400,
            catalogs=[
                {"catalog_instance_id": row[0], "provider_type": row[1],
                 "server_name": row[2], "is_default": bool(row[3])}
                for row in rows
            ],
        )
    if not rows:
        return None, None
    return rows[0][0], str(rows[0][1] or "").lower()


def _route_catalog():
    """Resolve the current request's catalogue on a short-lived cursor."""
    cur = get_db().cursor()
    try:
        return resolve_catalog(cur, requested_catalog())
    finally:
        cur.close()


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


def album_artist_sql(over=""):
    """An album's artist: the catalogue album's, else its tracks' (LUM-014).
    An aggregate, or a window function with ``over="OVER ()"``."""
    return (f"COALESCE(NULLIF(MIN(album_row_artist) {over}, ''), "
            f"MIN(COALESCE(NULLIF(album_artist, ''), author)) {over})")


MAX_CURSOR_OFFSET = 10_000_000


def section_order(section, sort):
    """How ``section`` pages under ``sort``: its keyset order, or "offset" for
    the legacy sorts whose order is not indexable (LUM-016)."""
    if section == "albums":
        return "offset" if sort == "artist" else "name"
    if section == "tracks":
        return "offset" if sort in ("artist", "year") else "title"
    return "name"


def encode_cursor(section, order, *position):
    """An opaque paging position, bound to the section and order it was issued
    for (LUM-016): the last row's ``(sort key, id)`` for a keyset order, or
    the next row's offset for the "offset" order."""
    raw = json.dumps([section, order, *position], ensure_ascii=False, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def decode_cursor(value, section=None, order=None):
    """A cursor's position: ``(sort_key, id)``, or an offset for the "offset"
    order; None when absent. 400 ``invalid_cursor`` when it is unreadable or
    was issued for another section or order (``section`` None checks only
    that it is readable)."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        data = json.loads(base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)))
    except (ValueError, TypeError):
        raise CatalogScopeError("invalid_cursor", 400) from None
    if not isinstance(data, list) or len(data) < 3:
        raise CatalogScopeError("invalid_cursor", 400)
    issued_section, issued_order, *position = data
    if section is not None and (issued_section, issued_order) != (section, order):
        raise CatalogScopeError("invalid_cursor", 400)
    if issued_order == "offset":
        valid = (len(position) == 1 and type(position[0]) is int
                 and 0 < position[0] <= MAX_CURSOR_OFFSET)
        if valid:
            return position[0]
    elif len(position) == 2 and all(isinstance(part, str) for part in position):
        return tuple(position)
    raise CatalogScopeError("invalid_cursor", 400)


def _search_state(cur, catalog, query, artist):
    """What one browse request reads, or None when the catalogue is not published.

    ``stored`` is whether ``catalog_tracks.search_text`` of the published
    generation was folded the way this request folds its query: written at
    publication (or by the migration) with ``unaccent`` exactly when it is
    available now. Otherwise the text is computed inline, as before LUM-016,
    and cannot use the trigram index (logged once).
    """
    global _stale_search_warned
    if catalog is None:
        return None
    cur.execute(
        "SELECT c.published_generation, c.search_text_generation, c.search_text_folded, "
        f"c.entity_counts FROM {table('catalog_state')} c "
        f"JOIN {table('catalog_sources')} s USING (catalog_instance_id) "
        "WHERE c.catalog_instance_id=%s AND s.rebind_status='active' AND c.status='complete'",
        (catalog,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    folded = unaccent_available(cur)
    stored = row[1] is not None and row[1] == row[0] and row[2] is not None and bool(row[2]) == folded
    tokens = str(query or "").casefold().split()[:8]
    if tokens and folded:
        cur.execute(
            "SELECT unaccent(u.token) FROM unnest(%s::text[]) WITH ORDINALITY AS u(token, n) "
            "ORDER BY u.n",
            (tokens,),
        )
        tokens = [item[0] for item in cur.fetchall()]
    if tokens and not stored and not _stale_search_warned:
        _stale_search_warned = True
        logger.warning(
            "Living Collections search reads catalogue %s without its stored search text "
            "(not written for this generation, or folded without unaccent); it is written "
            "at the next publication or migration",
            catalog,
        )
    counts = row[3]
    if isinstance(counts, str):
        try:
            counts = json.loads(counts)
        except ValueError:
            counts = {}
    return {
        "catalog": catalog, "generation": row[0], "stored": stored, "folded": folded,
        "tokens": tokens, "artist": artist,
        "counts": counts if isinstance(counts, dict) else {},
    }


def _track_filters(ctx, tracks="t", album_name="al.name"):
    """The search and artist filters on catalogue tracks, as `` AND ...``."""
    text = f"{tracks}.search_text" if ctx["stored"] else search_text_sql(
        ctx["folded"], tracks, album_name)
    clauses, params = [], []
    for token in ctx["tokens"]:
        # AND-ed tokens, as before. A token under three characters has no
        # trigram: `|| ''` keeps it a filter instead of a full index scan.
        clauses.append(f"{text} LIKE %s" if len(token) >= 3 else f"({text} || '') LIKE %s")
        params.append(f"%{token}%")
    if ctx["artist"]:
        clauses.append(
            f"({tracks}.album_artist_display = %s OR (NULLIF({tracks}.album_artist_display, '')"
            f" IS NULL AND {tracks}.artist_display = %s))"
        )
        params.extend([str(ctx["artist"])] * 2)
    return "".join(f" AND {clause}" for clause in clauses), params


def _album_join(kind="LEFT JOIN"):
    return (f"{kind} {table('catalog_albums')} al "
            "ON al.catalog_instance_id=t.catalog_instance_id "
            "AND al.published_generation=t.published_generation "
            "AND al.album_id=t.album_id AND al.available")


def _after(key_sql, id_sql, after):
    if after is None:
        return "", []
    return f" AND ({key_sql}, {id_sql}) > (%s, %s)", list(after)


def _paged(cur, sql, params, limit, offset):
    """``limit`` rows of a keyset query and the cursor after them, if more.

    The query selects ``sort_key`` and ``sort_id`` (removed here) and has its
    ORDER BY. OFFSET is added only for a legacy ``page`` request: a cursor
    page never has one.
    """
    tail = " LIMIT %s" + (" OFFSET %s" if offset else "")
    cur.execute(sql + tail, tuple(params + [limit + 1] + ([offset] if offset else [])))
    rows = _all_dicts(cur)
    more = len(rows) > limit
    rows = rows[:limit]
    cursor = (rows[-1]["sort_key"], rows[-1]["sort_id"]) if more else None
    keys = [(row.pop("sort_key"), row.pop("sort_id")) for row in rows]
    return rows, cursor, keys


def _section(cur, rows, cursor, first_page, count_sql, params, exact=None):
    """Items, total and paging for one section, without an unbounded count.

    A first page that holds everything is its own exact total; so is an
    ``exact`` count from ``catalog_state.entity_counts``. Otherwise matching
    rows are counted up to ``TOTAL_CAP``: beyond it the total is ``TOTAL_CAP``
    with ``total_exact`` false ("1000+").
    """
    if first_page and cursor is None:
        total = len(rows)
    elif exact is not None:
        total = int(exact)
    else:
        cur.execute(f"SELECT count(*) FROM ({count_sql} LIMIT %s) capped",
                    tuple(params + [TOTAL_CAP + 1]))
        total = int(cur.fetchone()[0])
    capped = exact is None and total > TOTAL_CAP
    return {"items": rows, "total": TOTAL_CAP if capped else total,
            "total_exact": not capped, "next_cursor": cursor}


def _keyset_tracks(cur, ctx, limit, offset, after):
    """Tracks by ``(lower(title), track_id)``."""
    filters, params = _track_filters(ctx)
    base = (f"FROM {table('catalog_tracks')} t {_album_join()} "
            "WHERE t.catalog_instance_id=%s AND t.published_generation=%s AND t.available "
            f"AND NULLIF(t.title, '') IS NOT NULL{filters}")
    params = [ctx["catalog"], ctx["generation"]] + params
    keyset, keyset_params = _after("lower(t.title)", "t.track_id", after)
    rows, cursor, _keys = _paged(
        cur,
        "SELECT t.track_id, t.title, t.artist_display AS artist, al.name AS album, "
        "COALESCE(NULLIF(t.album_artist_display, ''), t.artist_display) AS album_artist, "
        "NULL::INTEGER AS year, NULL::INTEGER AS rating, t.track_id AS cover_item_id, "
        f"lower(t.title) AS sort_key, t.track_id AS sort_id {base}{keyset} "
        "ORDER BY lower(t.title), t.track_id",
        params + keyset_params, limit, offset,
    )
    for row in rows:
        row["kind"] = "track"
    # Every published track is available and titled (the normalizer refuses
    # an untitled one), so the unfiltered total is the published count.
    exact = None
    if not ctx["tokens"] and not ctx["artist"]:
        exact = ctx["counts"].get("track")
    return _section(cur, rows, cursor, offset == 0 and after is None,
                    f"SELECT 1 {base}", params, exact)


def _keyset_albums(cur, ctx, limit, offset, after):
    """Catalogue albums with a matching track, by ``(lower(name), album_id)``."""
    filters, params = _track_filters(ctx)
    base = (f"FROM {table('catalog_albums')} al "
            "WHERE al.catalog_instance_id=%s AND al.published_generation=%s AND al.available "
            "AND NULLIF(al.name, '') IS NOT NULL "
            f"AND EXISTS (SELECT 1 FROM {table('catalog_tracks')} t "
            "WHERE t.catalog_instance_id=al.catalog_instance_id "
            "AND t.published_generation=al.published_generation "
            f"AND t.album_id=al.album_id AND t.available{filters})")
    base_params = [ctx["catalog"], ctx["generation"]] + params
    keyset, keyset_params = _after("lower(al.name)", "al.album_id", after)
    rows, cursor, _keys = _paged(
        cur,
        "SELECT al.album_id, al.name AS title, al.album_artist_display AS album_row_artist, "
        f"lower(al.name) AS sort_key, al.album_id AS sort_id {base}{keyset} "
        "ORDER BY lower(al.name), al.album_id",
        base_params + keyset_params, limit, offset,
    )
    details = {}
    if rows:
        # Aggregates over the page's albums only, through the album_id index.
        cur.execute(
            "SELECT t.album_id, MIN(t.track_id), COUNT(*)::INTEGER, "
            "MIN(COALESCE(NULLIF(t.album_artist_display, ''), t.artist_display)) "
            f"FROM {table('catalog_tracks')} t {_album_join('JOIN')} "
            "WHERE t.catalog_instance_id=%s AND t.published_generation=%s AND t.available "
            f"AND t.album_id = ANY(%s){filters} GROUP BY t.album_id",
            tuple([ctx["catalog"], ctx["generation"], [row["album_id"] for row in rows]]
                  + params),
        )
        details = {item[0]: item[1:] for item in cur.fetchall()}
    items = []
    for row in rows:
        cover, count, track_artist = details.get(row["album_id"], (None, 0, None))
        artist = row["album_row_artist"] or track_artist
        items.append({
            "title": row["title"], "artist": artist, "cover_item_id": cover,
            "track_count": count, "year": None, "rating": None, "kind": "album",
            "album_key": _album_key(row["title"], artist),
            "provider_album_id": str(row["album_id"]),
        })
    return _section(cur, items, cursor, offset == 0 and after is None,
                    f"SELECT 1 {base}", base_params)


ARTIST_KEY_SQL = "COALESCE(NULLIF(t.album_artist_display, ''), t.artist_display)"


def _keyset_artists(cur, ctx, limit, offset, after):
    """Artist names (album artist, else track artist) by ``(lower(name), name)``."""
    key = ARTIST_KEY_SQL
    filters, params = _track_filters(ctx)
    base = (f"FROM {table('catalog_tracks')} t {_album_join()} "
            "WHERE t.catalog_instance_id=%s AND t.published_generation=%s AND t.available "
            f"AND NULLIF({key}, '') IS NOT NULL{filters}")
    base_params = [ctx["catalog"], ctx["generation"]] + params
    keyset, keyset_params = _after(f"lower({key})", key, after)
    _rows, cursor, keys = _paged(
        cur,
        f"SELECT DISTINCT lower({key}) AS sort_key, {key} AS sort_id {base}{keyset} "
        f"ORDER BY lower({key}), {key}",
        base_params + keyset_params, limit, offset,
    )
    details = {}
    if keys:
        cur.execute(
            f"SELECT {key}, MIN(t.track_id), "
            "COUNT(DISTINCT t.album_id) FILTER (WHERE NULLIF(al.name, '') IS NOT NULL)::INTEGER, "
            f"COUNT(*)::INTEGER {base} AND lower({key}) = ANY(%s) AND {key} = ANY(%s) "
            f"GROUP BY {key}",
            tuple(base_params + [[k for k, _ in keys], [name for _, name in keys]]),
        )
        details = {item[0]: item[1:] for item in cur.fetchall()}
    items = []
    for _key, name in keys:
        cover, albums, tracks = details.get(name, (None, 0, 0))
        items.append({
            "artist": name, "title": name, "cover_item_id": cover, "album_count": albums,
            "track_count": tracks, "first_year": None, "latest_year": None, "kind": "artist",
        })
    return _section(cur, items, cursor, offset == 0 and after is None,
                    f"SELECT DISTINCT lower({key}), {key} {base}", base_params)


def _legacy_view(ctx):
    return catalog_track_view_sql(ctx["folded"], stored=ctx["stored"])


def _browse_albums(cur, ctx, sort, limit, offset):
    """Albums by artist: the pre-LUM-016 query, paged by OFFSET (legacy).

    One row per catalogue album of the current generation (LUM-014), grouped
    by ``album_id``; ``album_key`` is for display and legacy callers only.
    """
    filters, params = _library_filters(" ".join(ctx["tokens"]), ctx["artist"], ctx["folded"])
    inner = f"""
            SELECT album_id, MIN(album) AS title,
                   {album_artist_sql()} AS artist,
                   MIN(item_id) AS cover_item_id,
                   COUNT(*)::INTEGER AS track_count,
                   MIN(year)::INTEGER AS year,
                   MAX(rating)::INTEGER AS rating
              FROM ({_legacy_view(ctx)}) score
             WHERE NULLIF(album, '') IS NOT NULL {filters}
             GROUP BY album_id"""
    params = [ctx["catalog"]] + params
    cur.execute(
        f"SELECT * FROM ({inner}) albums "
        "ORDER BY lower(artist), lower(title), album_id LIMIT %s OFFSET %s",
        tuple(params + [limit + 1, offset]),
    )
    rows = _all_dicts(cur)
    more = len(rows) > limit
    rows = rows[:limit]
    for row in rows:
        row.update(
            {
                "kind": "album",
                "album_key": _album_key(row.get("title"), row.get("artist")),
                "provider_album_id": str(row.pop("album_id")),
            }
        )
    return _section(cur, rows, (offset + limit,) if more else None, offset == 0,
                    f"SELECT 1 FROM ({inner}) albums", params)


def _browse_tracks(cur, ctx, sort, limit, offset):
    """Tracks by artist (or the retired year sort): the pre-LUM-016 query,
    paged by OFFSET (legacy)."""
    filters, params = _library_filters(" ".join(ctx["tokens"]), ctx["artist"], ctx["folded"])
    order = {
        # `artist` is a SELECT alias below. PostgreSQL permits a bare output
        # alias in ORDER BY, but not one nested inside lower(...), so sort on
        # the source column here rather than raising UndefinedColumn at runtime.
        "artist": "lower(COALESCE(author, '')), lower(COALESCE(album, '')), lower(title)",
        "year": "year DESC NULLS LAST, lower(COALESCE(author, '')), lower(title)",
    }[sort]
    inner = f"FROM ({_legacy_view(ctx)}) score WHERE NULLIF(title, '') IS NOT NULL {filters}"
    params = [ctx["catalog"]] + params
    cur.execute(
        f"""
        SELECT item_id AS track_id, title, author AS artist, album,
               COALESCE(NULLIF(album_artist, ''), author) AS album_artist,
               year, rating, item_id AS cover_item_id
          {inner}
         ORDER BY {order}
         LIMIT %s OFFSET %s
        """,
        tuple(params + [limit + 1, offset]),
    )
    rows = _all_dicts(cur)
    more = len(rows) > limit
    rows = rows[:limit]
    for row in rows:
        row["kind"] = "track"
    return _section(cur, rows, (offset + limit,) if more else None, offset == 0,
                    f"SELECT 1 {inner}", params)


def _browse_section(cur, ctx, key, sort, limit, offset, after):
    """One section's page, its ``next_cursor`` bound to the section and order."""
    section = _read_section(cur, ctx, key, sort, limit, offset, after)
    if section["next_cursor"] is not None:
        section["next_cursor"] = encode_cursor(key, section_order(key, sort),
                                               *section["next_cursor"])
    return section


def _read_section(cur, ctx, key, sort, limit, offset, after):
    if ctx is None:
        return {"items": [], "total": 0, "total_exact": True, "next_cursor": None}
    if key == "albums":
        if sort == "artist":
            return _browse_albums(cur, ctx, sort, limit, offset)
        # "year" is title order: the catalogue publishes no year (LUM-015).
        return _keyset_albums(cur, ctx, limit, offset, after)
    if key == "tracks":
        if sort in ("artist", "year"):
            return _browse_tracks(cur, ctx, sort, limit, offset)
        return _keyset_tracks(cur, ctx, limit, offset, after)
    # Every artist sort is by name: artists have no year (LUM-015).
    return _keyset_artists(cur, ctx, limit, offset, after)


def browse_library(scope="albums", query="", artist=None, sort="title", page=1, limit=36,
                   catalog_instance_id=None, cursor=None):
    """Return one page of analyzed media grouped by a stable library scope.

    Paging (LUM-016): each section returns ``next_cursor``; pass it back as
    ``cursor`` for the next page, which is read by keyset on ``(lower(key),
    id)``. ``page`` is the legacy offset paging for old clients: it is ignored
    when a cursor is given, and costs OFFSET on deep pages. Totals are exact
    up to ``TOTAL_CAP``; above it ``total`` is ``TOTAL_CAP`` and
    ``total_exact`` is false, except the unfiltered track count, which is the
    published ``entity_counts``.

    Raises ``CatalogScopeError`` when the catalogue cannot be resolved (K10)
    or the cursor is unreadable (400 ``invalid_cursor``).
    """
    scope = scope if scope in LIBRARY_SCOPES else "albums"
    sort = sort if sort in LIBRARY_SORTS else "title"
    page = _bounded_int(page, 1, 1, 100000)
    limit = _bounded_int(limit, 36, 1, 100)
    query = str(query or "").strip()
    order = section_order(scope, sort) if scope != "all" else None
    after = decode_cursor(cursor, scope if scope != "all" else None, order)
    if query and len(query) < 3:
        keys = ("albums", "tracks", "artists") if scope == "all" else (scope,)
        return {
            "catalog_instance_id": catalog_instance_id,
            "scope": scope,
            "query": query,
            "artist": artist,
            "sort": sort,
            "page": page,
            "limit": limit,
            "sections": {key: {"items": [], "total": 0, "total_exact": True,
                               "next_cursor": None} for key in keys},
        }
    if order == "offset" and after is not None:
        offset, after = after, None
    else:
        offset = 0 if after is not None else (page - 1) * limit
    db = get_db()
    cur = db.cursor()
    try:
        catalog, _provider = resolve_catalog(cur, catalog_instance_id)
        ctx = _search_state(cur, catalog, query, artist)
        if scope != "all":
            sections = {scope: _browse_section(cur, ctx, scope, sort, limit, offset, after)}
        else:
            # A broad search intentionally returns compact categorized sections.
            section_limit = min(limit, 12)
            sections = {
                key: _browse_section(cur, ctx, key, sort, section_limit, 0, None)
                for key in ("albums", "tracks", "artists")
            }
            for section in sections.values():
                section["next_cursor"] = None
    finally:
        cur.close()
    return {
        "catalog_instance_id": catalog,
        "scope": scope,
        "query": query,
        "artist": artist,
        "sort": sort,
        "page": page,
        "limit": limit,
        "sections": sections,
    }


def library_stats(catalog_instance_id=None):
    db = get_db()
    cur = db.cursor()
    try:
        catalog, _provider = resolve_catalog(cur, catalog_instance_id)
        cur.execute(
            f"""
            SELECT COUNT(*)::INTEGER AS track_count,
                   COUNT(DISTINCT album_id)
                     FILTER (WHERE NULLIF(album, '') IS NOT NULL)::INTEGER AS album_count,
                   COUNT(DISTINCT lower(COALESCE(NULLIF(album_artist, ''), author)))::INTEGER
                     AS artist_count
              FROM ({catalog_track_view_sql(unaccent=False)}) score
            """,
            (catalog,),
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


def _legacy_album_id(cur, catalog, title, artist):
    """LEGACY (pre-LUM-014 callers): the album an ``album_key`` names.

    Old clients ask for album details by title and artist only. That pair
    can name several catalogue albums (same-name editions); the one with the
    lowest ``album_id`` answers, and the response carries its
    ``provider_album_id``. None when nothing matches.
    """
    cur.execute(
        f"""
        SELECT MIN(album_id)
          FROM ({catalog_track_view_sql(unaccent=False)}) score
         WHERE lower(album) = lower(%s)
           AND lower(COALESCE(NULLIF(album_artist, ''), author)) = lower(%s)
        """,
        (catalog, title, artist),
    )
    return (cur.fetchone() or (None,))[0]


def _score_album_tracks(title, artist, provider_album_id=None, catalog_instance_id=None):
    """``(tracks, (title, artist))`` of one catalogue album; the pair is
    ``(None, None)`` when it has no tracks.

    The album is ``(catalog, provider_album_id)`` (LUM-014); without an id,
    the legacy title-and-artist fallback picks it.
    """
    db = get_db()
    cur = db.cursor()
    try:
        catalog, _provider = resolve_catalog(cur, catalog_instance_id)
        album_id = provider_album_id or _legacy_album_id(cur, catalog, title, artist)
        cur.execute(
            f"""
            SELECT item_id AS track_id, title, author AS artist, album,
                   COALESCE(NULLIF(album_artist, ''), author) AS album_artist,
                   year, rating, item_id AS cover_item_id, album_id,
                   track_number, disc_number,
                   CASE WHEN duration_ms IS NULL THEN NULL ELSE round(duration_ms / 1000.0) END
                     AS duration_seconds,
                   analysis_status, provider_type, catalog_instance_id,
                   MIN(album) OVER () AS album_title,
                   {album_artist_sql("OVER ()")} AS album_display_artist
              FROM ({catalog_track_view_sql(unaccent=False)}) score
             WHERE album_id = %s
             ORDER BY lower(title), item_id
            """,
            (catalog, album_id),
        )
        rows = _all_dicts(cur)
    finally:
        cur.close()
    header = None
    for index, row in enumerate(rows):
        header = (row.pop("album_title"), row.pop("album_display_artist"))
        row.update(
            {
                "kind": "track",
                "analyzed": row.pop("analysis_status", None) in {"ready", "suspect"},
                "provider_index": index,
            }
        )
    return rows, header or (None, None)


def album_detail(title=None, artist=None, provider_album_id=None, catalog_instance_id=None):
    """Load provider-authoritative order and metadata from the published mirror.

    The album is ``(catalogue, provider_album_id)``, the catalogue's
    ``album_id`` (LUM-014); its title and artist come from the catalogue.
    ``title`` and ``artist`` alone are the legacy ``album_key`` fallback
    (``_legacy_album_id``), and are echoed when nothing matches.
    """
    tracks, (album_title, album_artist) = _score_album_tracks(
        title, artist, provider_album_id=provider_album_id,
        catalog_instance_id=catalog_instance_id,
    )
    title, artist = album_title or title, album_artist or artist
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
        "catalog_instance_id": next(
            (item.get("catalog_instance_id") for item in tracks
             if item.get("catalog_instance_id")),
            catalog_instance_id,
        ),
        "album": album,
        "tracks": tracks,
        "metadata_source": "provider_catalog",
        "provider_type": provider_type,
    }


def _provider_headers(provider_type):
    if provider_type in {"jellyfin", "emby"}:
        return dict(getattr(config, "HEADERS", {}) or {})
    return {}


def _resolve_stream_target(item_id, provider_type):
    """Upstream (url, headers, params) for the catalogue's provider (K10:
    ``provider_type`` comes from the source row, not the host setting)."""
    provider_type = str(provider_type or "").lower()
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


def _resolve_art_target(item_id, size, provider_type):
    provider_type = str(provider_type or "").lower()
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
    """Workbench reads. Each takes ``catalog_instance_id`` (K10); see
    ``resolve_catalog`` for the answer when it is absent."""

    @bp.get("/api/collections/library")
    @require_enabled
    def collection_library_browse():
        try:
            return jsonify(
                browse_library(
                    scope=str(request.args.get("scope") or "albums").lower(),
                    query=request.args.get("q") or "",
                    artist=request.args.get("artist") or None,
                    sort=str(request.args.get("sort") or "title").lower(),
                    page=request.args.get("page") or 1,
                    limit=request.args.get("limit") or 36,
                    catalog_instance_id=requested_catalog(),
                    cursor=request.args.get("cursor") or None,
                )
            )
        except CatalogScopeError as exc:
            return jsonify(exc.body()), exc.status

    @bp.get("/api/collections/library/stats")
    @require_enabled
    def collection_library_stats():
        try:
            return jsonify(library_stats(catalog_instance_id=requested_catalog()))
        except CatalogScopeError as exc:
            return jsonify(exc.body()), exc.status

    @bp.get("/api/collections/library/album")
    @require_enabled
    def collection_library_album():
        title = str(request.args.get("title") or "").strip()
        artist = str(request.args.get("artist") or "").strip()
        album_id = str(request.args.get("provider_album_id") or "").strip() or None
        # LUM-014: an album is (catalogue, provider_album_id). Title and
        # artist without an id are the legacy album_key lookup.
        if not album_id and (not title or not artist):
            return jsonify({"error": "Album title and artist are required"}), 400
        try:
            return jsonify(
                album_detail(
                    title or None,
                    artist or None,
                    provider_album_id=album_id,
                    catalog_instance_id=requested_catalog(),
                )
            )
        except CatalogScopeError as exc:
            return jsonify(exc.body()), exc.status

    @bp.get("/api/collections/library/stream/<path:item_id>")
    @require_enabled
    def collection_library_stream(item_id):
        if not _ITEM_ID_RE.fullmatch(item_id):
            return jsonify({"error": "Invalid track id"}), 400
        try:
            _catalog, provider_type = _route_catalog()
            if provider_type is None:
                return jsonify({"error": "catalog_instance_not_found"}), 404
            target, target_error = _resolve_stream_target(item_id, provider_type)
            if target_error:
                message, status = target_error
                return jsonify({"error": message}), status
            upstream, upstream_error = _proxy_stream(target)
            if upstream_error:
                message, status = upstream_error
                return jsonify({"error": message}), status
            return _stream_response(upstream)
        except CatalogScopeError as exc:
            return jsonify(exc.body()), exc.status
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
            _catalog, provider_type = _route_catalog()
            if provider_type is None:
                return "", 404
            response = _proxy_art(_resolve_art_target(item_id, size, provider_type))
            return response if response is not None else ("", 404)
        except CatalogScopeError as exc:
            return jsonify(exc.body()), exc.status
        except Exception:
            logger.warning("Living Collections artwork failed for %s", item_id)
            return "", 404
