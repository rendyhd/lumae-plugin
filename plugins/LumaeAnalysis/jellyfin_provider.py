"""Jellyfin catalogue reader and server identity (1.6.0, JF.8).

Every function here runs inside a bound AudioMuse server context
(``core.bind(server_id)``): the AudioMuse Jellyfin module resolves the bound
server's own URL, token and user from it, so a secondary Jellyfin never falls
back to the host's default server. Credentials stay in that module; callers
get raw catalogue rows (normalized and stripped by ``catalog``) and a
sanitized identity.

Jellyfin 12.0 or later is required (D-JF.1). Item IDs are 32 lowercase hex
characters (``MD5(type + path)``), so a moved or renamed file gets a new ID;
``jellyfin_continuity`` carries ratings, plays and playlists across that.

Measured on Jellyfin 12.2.0 (2026-10-06): only ``Authorization: MediaBrowser
Token=...`` (what AudioMuse sends) or ``ApiKey`` authenticate, legacy headers
answer 401; ``/Persons`` IDs never equal ``ArtistItems`` IDs while
``/Artists`` and ``/Artists/AlbumArtists`` do and honour ``ParentId``; a
track's ``ParentId`` is its folder (a disc folder on multi-disc albums), the
album is ``AlbumId``; untagged dates come back as ``0001-01-01``; a library
the account may not open answers 401 although the token is valid; and while
the server starts ``/System/Info/Public`` answers 503 or a short-lived
camelCase body.
"""

import re
import time

from plugin.api import logger


MINIMUM_JELLYFIN_VERSION = (12, 0, 0)
PAGE_SIZE = 500
REQUEST_TIMEOUT_SECONDS = 120
PROBE_TIMEOUT_SECONDS = 5
# A starting server answers 503 (or a camelCase body without ProductName) for
# a few seconds; the probe tries again before calling the server unreachable.
PROBE_ATTEMPTS = 3
PROBE_RETRY_SECONDS = 1.0
_sleep = time.sleep

# ItemFields the normalizer reads beyond the always-present BaseItemDto keys
# (Id, Name, Album, AlbumId, AlbumArtist, AlbumArtists, Artists, ArtistItems,
# IndexNumber, ParentIndexNumber, RunTimeTicks, ProductionYear, PremiereDate,
# ImageTags, AlbumPrimaryImageTag, MediaType). Path is never requested.
# ParentId is kept on a track only when it has no AlbumId (a loose file);
# otherwise it is a folder, often a CD1/CD2 disc folder, never the album.
TRACK_FIELDS = ("DateCreated", "Genres", "MediaSources", "ParentId", "ProviderIds", "SortName")
ALBUM_FIELDS = ("DateCreated", "Genres", "ProviderIds", "SortName", "Studios")

# Raw keys kept from a Jellyfin item. Everything else (user data, blur
# hashes, paths, server-internal flags) is dropped before normalization so
# the published payload stays compact and deterministic.
TRACK_KEYS = (
    "Id", "Name", "Type", "MediaType", "AlbumId", "Album", "AlbumArtist",
    "AlbumArtists", "Artists", "ArtistItems", "IndexNumber", "ParentIndexNumber",
    "RunTimeTicks", "ProductionYear", "PremiereDate", "Genres", "ProviderIds",
    "ImageTags", "AlbumPrimaryImageTag", "DateCreated", "SortName", "ParentId",
    "HasLyrics", "Container",
)
ALBUM_KEYS = (
    "Id", "Name", "AlbumArtist", "AlbumArtists", "ProductionYear", "PremiereDate",
    "Genres", "ProviderIds", "ImageTags", "DateCreated", "SortName", "Studios",
)
# Jellyfin's "no date" value (an untagged file), treated as absent.
_SENTINEL_DATE_PREFIX = "0001-01-01"
# Both list the MusicArtist items (IDs equal ArtistItems / AlbumArtists IDs)
# of one library; /Persons does not (its IDs never match). Both are marked
# obsolete in the 12.x OpenAPI document and still answer on 12.2.
ARTIST_PATHS = ("/Artists", "/Artists/AlbumArtists")

MEDIA_SOURCE_KEYS = ("Container", "Size", "Bitrate")

# The file suffix clients use for decodability (iOS cannot decode Ogg/Opus)
# and download extensions. Jellyfin's item ``Container`` is ffprobe's demuxer
# list ("mov,mp4,m4a,3gp,3g2,mj2" for an M4A) and its media-source
# ``Container`` calls an Ogg Opus file "ogg", so the suffix is the file's own
# extension, as Navidrome's ``suffix`` is (measured on 12.2.0). Only a single
# lower-case token is ever published, never a comma list.
_SUFFIX_RE = re.compile(r"^[a-z0-9]{1,10}$")
CODEC_SUFFIXES = {
    "aac": "m4a", "alac": "m4a", "ape": "ape", "flac": "flac", "mp2": "mp2", "mp3": "mp3",
    "opus": "opus", "vorbis": "ogg", "wavpack": "wv", "wmav1": "wma", "wmav2": "wma",
    "wmapro": "wma", "wmalossless": "wma",
}
MEDIA_STREAM_KEYS = (
    "Type", "Codec", "BitRate", "SampleRate", "BitDepth", "Channels", "ChannelLayout",
)

_VERSION_RE = re.compile(r"^\s*v?(\d+)\.(\d+)\.(\d+)")
_SERVER_ID_RE = re.compile(r"^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$")
ITEM_ID_RE = re.compile(r"^[0-9a-f]{32}$")


class JellyfinUnavailable(RuntimeError):
    """The bound AudioMuse core cannot reach Jellyfin with per-server credentials."""


class JellyfinHttpError(JellyfinUnavailable):
    """Jellyfin answered with an HTTP error status.

    ``detail`` is a short prefix of the body, kept only to tell a library the
    account may not open (401 "... is not permitted to access Library X.")
    from rejected credentials; it is never shown.
    """

    def __init__(self, status, detail=""):
        super().__init__(f"Jellyfin answered HTTP {status}")
        self.status = status
        self.detail = detail

    @property
    def library_forbidden(self):
        return self.status == 401 and "not permitted to access" in self.detail.lower()


def parse_version(raw):
    """``(major, minor, patch)`` of a Jellyfin ``Version``, or ``None``."""
    match = _VERSION_RE.match(str(raw or ""))
    if not match:
        return None
    return tuple(int(part) for part in match.groups())


def version_supported(raw):
    parsed = parse_version(raw)
    return parsed is not None and parsed >= MINIMUM_JELLYFIN_VERSION


def normalize_server_id(value):
    """A Jellyfin server ``Id`` as 32 lowercase hex characters, or ``None``."""
    text = str(value or "").strip()
    if not _SERVER_ID_RE.match(text):
        return None
    return text.replace("-", "").lower()


def _helpers(module):
    names = ("_jellyfin_base_url", "_jellyfin_headers_from_creds", "_jellyfin_user_id")
    if not all(callable(getattr(module, name, None)) for name in names):
        raise JellyfinUnavailable(
            "This AudioMuse-AI core does not expose per-server Jellyfin access; "
            "Lumae needs AudioMuse-AI 3 for Jellyfin."
        )
    http = getattr(module, "requests", None)
    if http is None or not callable(getattr(http, "get", None)):
        import requests as http
    return http


def connection(module):
    """``(base_url, headers, user_id, http)`` of the bound server."""
    http = _helpers(module)
    base_url = str(module._jellyfin_base_url() or "").rstrip("/")
    if not base_url:
        raise JellyfinUnavailable("The Jellyfin server has no address in AudioMuse")
    return base_url, dict(module._jellyfin_headers_from_creds() or {}), module._jellyfin_user_id(), http


def _response_detail(response):
    try:
        text = getattr(response, "text", "")
        return str(text if not callable(text) else text())[:200]
    except Exception:  # noqa: BLE001 - the detail only classifies the error
        return ""


def _get_json(http, url, headers=None, params=None, timeout=REQUEST_TIMEOUT_SECONDS):
    response = http.get(url, headers=headers or {}, params=params, timeout=timeout)
    try:
        status = getattr(response, "status_code", None)
        if isinstance(status, int) and status >= 400:
            raise JellyfinHttpError(status, _response_detail(response))
        response.raise_for_status()
        return response.json()
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()


def _field(body, name):
    """A PascalCase field, or its camelCase form from a starting server."""
    if name in body:
        return body[name]
    lowered = name.lower()
    for key, value in body.items():
        if str(key).lower() == lowered:
            return value
    return None


def probe_identity(module, timeout=PROBE_TIMEOUT_SECONDS):
    """Read ``/System/Info/Public`` (no credentials) and return a sanitized identity.

    ``server_version`` and ``server_id`` are what the identity guard binds and
    compares; ``product_name`` lets it refuse a server that is not Jellyfin.
    A starting server (503, or a body without ``ProductName``) is asked again
    up to ``PROBE_ATTEMPTS`` times, then reported unreachable, never refused.
    """
    http = _helpers(module)
    base_url = str(module._jellyfin_base_url() or "").rstrip("/")
    if not base_url:
        raise JellyfinUnavailable("The Jellyfin server has no address in AudioMuse")
    starting = "Jellyfin is still starting"
    for attempt in range(PROBE_ATTEMPTS):
        if attempt:
            _sleep(PROBE_RETRY_SECONDS)
        try:
            body = _get_json(http, f"{base_url}/System/Info/Public", timeout=timeout)
        except JellyfinHttpError as exc:
            if exc.status == 503:
                continue
            raise
        if not isinstance(body, dict):
            raise JellyfinUnavailable("Jellyfin did not return its public system information")
        product = str(_field(body, "ProductName") or "").strip()
        if not product:
            continue
        version = str(_field(body, "Version") or "").strip()
        server_id = normalize_server_id(_field(body, "Id"))
        if not version:
            raise JellyfinUnavailable("Jellyfin did not report its version")
        if not server_id:
            raise JellyfinUnavailable("Jellyfin did not report a valid server Id")
        return {
            "provider_type": "jellyfin",
            "server_type": "jellyfin",
            "server_version": version,
            "server_id": server_id,
            "product_name": product,
        }
    raise JellyfinUnavailable(starting)


def _paged_items(http, base_url, headers, params, path="/Items"):
    start = 0
    while True:
        page_params = {
            **params,
            "StartIndex": start,
            "Limit": PAGE_SIZE,
            "EnableTotalRecordCount": "false",
            "EnableUserData": "false",
        }
        body = _get_json(http, f"{base_url}{path}", headers, page_params)
        items = (body or {}).get("Items") if isinstance(body, dict) else None
        if not isinstance(items, list):
            raise JellyfinUnavailable("Jellyfin returned an unexpected item page")
        yield from (item for item in items if isinstance(item, dict))
        if len(items) < PAGE_SIZE:
            return
        start += len(items)


def _single_token(value):
    text = str(value or "").strip().lower()
    return text if _SUFFIX_RE.match(text) else None


def _path_suffix(path):
    name = re.split(r"[\\/]", str(path or ""))[-1]
    if "." not in name.strip("."):
        return None
    return _single_token(name.rsplit(".", 1)[1])


def track_suffix(item):
    """The file suffix of a Jellyfin track, or ``None``.

    The file's extension (``MediaSources[0].Path``, which ``MediaSources``
    carries; the path itself is never published), else a single-token
    container, else the first audio stream's codec.
    """
    sources = [source for source in item.get("MediaSources") or [] if isinstance(source, dict)]
    first = sources[0] if sources else {}
    for candidate in (
        _path_suffix(first.get("Path")),
        _path_suffix(item.get("Path")),
        _single_token(first.get("Container")),
        _single_token(item.get("Container")),
    ):
        if candidate:
            return candidate
    for stream in first.get("MediaStreams") or []:
        if isinstance(stream, dict) and str(stream.get("Type") or "").lower() == "audio":
            codec = str(stream.get("Codec") or "").strip().lower()
            if codec.startswith("pcm_"):
                return "wav"
            return CODEC_SUFFIXES.get(codec)
    return None


def _compact_media_sources(value):
    sources = []
    for source in value if isinstance(value, list) else []:
        if not isinstance(source, dict):
            continue
        compact = {key: source[key] for key in MEDIA_SOURCE_KEYS if source.get(key) is not None}
        if "Container" in compact and not _single_token(compact["Container"]):
            del compact["Container"]
        streams = [
            {key: stream[key] for key in MEDIA_STREAM_KEYS if stream.get(key) is not None}
            for stream in source.get("MediaStreams") or []
            if isinstance(stream, dict) and str(stream.get("Type") or "").lower() == "audio"
        ]
        if streams:
            compact["MediaStreams"] = streams
        sources.append(compact)
    return sources


def _primary_tag(item):
    tags = item.get("ImageTags")
    return tags.get("Primary") if isinstance(tags, dict) else None


def _without_sentinel_dates(row):
    for key in ("PremiereDate", "DateCreated"):
        if str(row.get(key) or "").startswith(_SENTINEL_DATE_PREFIX):
            del row[key]
    year = row.get("ProductionYear")
    if isinstance(year, int) and year <= 1:
        del row["ProductionYear"]
    return row


def track_row(item, library_ids):
    row = _without_sentinel_dates(
        {key: item[key] for key in TRACK_KEYS if item.get(key) is not None}
    )
    if row.get("AlbumId"):
        row.pop("ParentId", None)
    if "Container" in row and not _single_token(row["Container"]):
        del row["Container"]
    suffix = track_suffix(item)
    if suffix:
        # Under Navidrome's OpenSubsonic key, which the normalizer publishes
        # as the track's audio container.
        row["suffix"] = suffix
    media = _compact_media_sources(item.get("MediaSources"))
    if media:
        row["MediaSources"] = media
    # The item whose Primary image shows this track: its own embedded art,
    # else its album's. The normalizer publishes it as cover_art_id.
    if _primary_tag(item):
        row["PrimaryImageItemId"] = str(item["Id"])
    elif item.get("AlbumPrimaryImageTag") and item.get("AlbumId"):
        row["PrimaryImageItemId"] = str(item["AlbumId"])
    row["_lumae_library_ids"] = sorted(library_ids)
    return row


def album_row(item, library_ids):
    row = _without_sentinel_dates(
        {key: item[key] for key in ALBUM_KEYS if item.get(key) is not None}
    )
    # An album's credits are its album artists. The normalizer reads
    # ArtistItems first, which on a MusicAlbum lists its tracks' artists.
    if isinstance(item.get("AlbumArtists"), list) and item["AlbumArtists"]:
        row["ArtistItems"] = item["AlbumArtists"]
    if _primary_tag(item):
        row["PrimaryImageItemId"] = str(item["Id"])
    row["_lumae_library_ids"] = sorted(library_ids)
    return row


def selected_library_ids(module, libraries):
    """The music libraries AudioMuse analyses for this server (its filter)."""
    available = {
        str(row.get("id") or row.get("Id") or row.get("ItemId"))
        for row in libraries
        if isinstance(row, dict) and (row.get("id") or row.get("Id") or row.get("ItemId"))
    }
    target = getattr(module, "_get_target_library_ids", None)
    target_ids = target() if callable(target) else None
    if target_ids is None:
        return available, False
    return {str(value) for value in target_ids}, True


def _library_error(exc, name):
    """A server-neutral, actionable message for a failed library walk."""
    kept = "Lumae kept the published catalogue."
    if isinstance(exc, JellyfinHttpError):
        if exc.library_forbidden:
            return (
                f'The Jellyfin account AudioMuse uses may not open the music library "{name}". '
                f"{kept} Give that account access to the library in Jellyfin, or remove the "
                "library from Music Libraries in AudioMuse."
            )
        if exc.status == 401:
            return (
                "Jellyfin did not accept AudioMuse's sign-in for this server (HTTP 401). "
                f"{kept} Check the Jellyfin token for this server in AudioMuse."
            )
        if exc.status == 400:
            return (
                f'Jellyfin does not know the music library "{name}" (HTTP 400); it may have '
                f"been removed. {kept} Check Music Libraries in AudioMuse."
            )
        return f'Jellyfin answered HTTP {exc.status} while reading the music library "{name}". {kept}'
    return None


def fetch_catalog(module, error_type=RuntimeError):
    """Read every track and album of the selected music libraries.

    Always scoped by library: one paged ``/Items`` walk per library, so each
    row carries its library membership exactly as the Navidrome reader does
    (``_lumae_library_ids``). A track visible in two libraries is one row with
    both memberships. A library that cannot be read fails the whole scan (a
    partial catalogue would publish its tracks as removed).
    """
    libraries = list(module.list_libraries() or [])
    selected, filtered = selected_library_ids(module, libraries)
    if not selected:
        detail = (
            "The configured Jellyfin music-library names did not match any music library."
            if filtered
            else "Jellyfin did not list any music libraries to AudioMuse (listing libraries "
            "needs a Jellyfin administrator account or API key)."
        )
        raise error_type(f"{detail} Check Music Libraries in AudioMuse before preparing Lumae.")
    names = {
        str(row.get("id") or row.get("Id") or row.get("ItemId")): str(row.get("name") or row.get("Name") or "")
        for row in libraries
        if isinstance(row, dict)
    }
    base_url, headers, user_id, http = connection(module)
    tracks = {}
    albums = {}
    for library_id in sorted(selected):
        common = {"userId": user_id, "ParentId": library_id, "Recursive": "true",
                  "SortBy": "SortName", "SortOrder": "Ascending",
                  "EnableImages": "true", "ImageTypeLimit": 1, "EnableImageTypes": "Primary"}
        try:
            for item in _paged_items(http, base_url, headers, {
                **common, "IncludeItemTypes": "Audio", "Fields": ",".join(TRACK_FIELDS),
            }):
                item_id = str(item.get("Id") or "")
                if not item_id:
                    continue
                if item_id in tracks:
                    tracks[item_id]["_lumae_library_ids"] = sorted(
                        set(tracks[item_id]["_lumae_library_ids"]) | {library_id}
                    )
                    continue
                tracks[item_id] = track_row(item, [library_id])
            for item in _paged_items(http, base_url, headers, {
                **common, "IncludeItemTypes": "MusicAlbum", "Fields": ",".join(ALBUM_FIELDS),
            }):
                album_id = str(item.get("Id") or "")
                if not album_id:
                    continue
                if album_id in albums:
                    albums[album_id]["_lumae_library_ids"] = sorted(
                        set(albums[album_id]["_lumae_library_ids"]) | {library_id}
                    )
                    continue
                albums[album_id] = album_row(item, [library_id])
        except JellyfinHttpError as exc:
            raise error_type(_library_error(exc, names.get(library_id) or library_id)) from exc
    if not tracks:
        scope = "the selected music libraries" if filtered else "the music libraries"
        raise error_type(
            f"Jellyfin returned no songs for {scope}. "
            "The empty catalogue was not published; check Jellyfin access and Music Libraries."
        )
    artist_ids = set()
    for row in list(tracks.values()) + list(albums.values()):
        for key in ("ArtistItems", "AlbumArtists"):
            for artist in row.get(key) or []:
                if isinstance(artist, dict) and artist.get("Id"):
                    artist_ids.add(str(artist["Id"]))
    return {
        "libraries": libraries,
        "albums": [albums[key] for key in sorted(albums)],
        "tracks": [tracks[key] for key in sorted(tracks)],
        "artist_cover_art": artist_cover_art(
            http, base_url, headers, user_id, sorted(selected), artist_ids
        ),
    }


def artist_cover_art(http, base_url, headers, user_id, library_ids, artist_ids):
    """Map artist IDs to the item that holds their portrait.

    Read from ``/Artists`` and ``/Artists/AlbumArtists`` per library
    (``ParentId``): their MusicArtist IDs equal the tracks' ``ArtistItems``
    and ``AlbumArtists`` IDs. ``/Persons`` is never used: its Person IDs match
    none of them. ``/Items?IncludeItemTypes=MusicArtist`` matches too but
    ignores the library. An artist with a Primary image maps to its own ID
    (the app loads ``/Items/{id}/Images/Primary``); one listed without maps to
    ``None``, which is authoritative. Portraits are enrichment: a failed walk
    is skipped, never fails the scan, and artists it did not list keep their
    published value.
    """
    wanted = set(artist_ids)
    cover_art = {}
    for library_id in library_ids:
        for path in ARTIST_PATHS:
            try:
                for item in _paged_items(http, base_url, headers, {
                    "userId": user_id,
                    "ParentId": library_id,
                    "SortBy": "SortName",
                    "SortOrder": "Ascending",
                    "EnableImages": "true",
                    "ImageTypeLimit": 1,
                    "EnableImageTypes": "Primary",
                }, path=path):
                    artist_id = str(item.get("Id") or "")
                    if artist_id not in wanted:
                        continue
                    if _primary_tag(item) or cover_art.get(artist_id):
                        cover_art[artist_id] = artist_id
                    else:
                        cover_art[artist_id] = None
            except Exception as exc:  # noqa: BLE001 - enrichment must not fail a scan
                logger.warning(
                    "Jellyfin artist portrait walk %s failed for one library (%s); "
                    "keeping published artist art",
                    path,
                    type(exc).__name__,
                )
    return cover_art


def stream_target(module, item_id, quote):
    """``(url, headers, params)`` for an original-file stream of the bound server.

    ``/Audio/{id}/stream?static=true`` serves the original file with byte
    ranges and, unlike ``/Items/{id}/Download``, does not need the account's
    "allow media downloading" permission (403 without it). Credentials are
    sent although 12.2 serves streams anonymously.
    """
    base_url, headers, _user_id, _http = connection(module)
    return f"{base_url}/Audio/{quote(item_id)}/stream", headers, {"static": "true"}


def art_target(module, item_id, size, quote):
    base_url, headers, _user_id, _http = connection(module)
    return (
        f"{base_url}/Items/{quote(item_id)}/Images/Primary",
        headers,
        {"maxWidth": size, "quality": 90},
    )
