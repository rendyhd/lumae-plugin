"""Credential-contained provider bridge shared by catalogue scanners.

Version-specific dispatcher behavior stays in the core adapters. This module
exposes only sanitized server descriptions and raw provider catalogue objects
to the normalizer; callers must never persist or serialize the bridge itself.
"""

from contextlib import contextmanager, nullcontext

from plugin.api import logger

from . import jellyfin_provider
from .core_compat import get_core_adapter


# The music servers Lumae's mobile catalogue contract supports: Navidrome and,
# from 1.6.0, Jellyfin 12.0 or later. Any other type (Emby, Lyrion, Plex, an
# unknown value) is hidden: never admitted, fetched, rekeyed or deleted.
SUPPORTED_PROVIDER_TYPES = frozenset(("navidrome", "jellyfin"))
PROVIDER_DISPLAY_NAMES = {"navidrome": "Navidrome", "jellyfin": "Jellyfin"}
NAVIDROME_SMALL_SCOPE_ALBUM_LIMIT = 32


def provider_display_name(provider_type):
    """A server type's product name for messages; neutral for anything else."""
    return PROVIDER_DISPLAY_NAMES.get(normalized_provider_type(provider_type), "music server")


class CatalogProviderError(RuntimeError):
    pass


def normalized_provider_type(value):
    return str(value or "").strip().lower()


class ProviderCatalogBridge:
    def __init__(self, core_adapter=None):
        self.core = core_adapter or get_core_adapter()

    def list_servers(self):
        servers = []
        for raw in self.core.list_servers():
            provider_type = normalized_provider_type(raw.get("provider_type"))
            servers.append(
                {
                    "server_id": str(raw["server_id"]),
                    "name": str(raw.get("name") or "Music server"),
                    "provider_type": provider_type,
                    "is_default": bool(raw.get("is_default")),
                    "supported": provider_type in SUPPORTED_PROVIDER_TYPES,
                }
            )
        return servers

    def require_server(self, server_id):
        matches = [server for server in self.list_servers() if server["server_id"] == server_id]
        if not matches:
            raise CatalogProviderError(f"Unknown AudioMuse server: {server_id}")
        server = matches[0]
        if not server["supported"]:
            raise CatalogProviderError(
                f"Provider {server['provider_type'] or 'unknown'} is not supported by Lumae catalogue v2"
            )
        return server

    def list_libraries(self, server_id):
        self.require_server(server_id)
        return self.core.list_libraries(server_id)

    def get_all_songs(self, server_id, apply_filter=True):
        self.require_server(server_id)
        return self.core.get_all_songs(server_id, apply_filter=apply_filter)

    def provider_module(self, server_id):
        server = self.require_server(server_id)
        return self.core.provider_module(server["provider_type"])

    def fetch_catalog(self, server_id):
        """Return the richest provider objects available behind a bound context.

        AudioMuse does not yet publish a stable rich-catalogue interface.  The
        plugin therefore feature-detects that future API, then falls back to a
        tightly-contained compatibility path.  Credentials never leave the
        core's bound context and none of these raw objects are sent to clients.
        """
        server = self.require_server(server_id)
        bind = getattr(self.core, "bind", None)
        context = bind(server_id) if callable(bind) else nullcontext()
        with context:
            module = self.core.provider_module(server["provider_type"])
            public_iterator = getattr(module, "iter_rich_catalog", None)
            if callable(public_iterator):
                result = public_iterator()
                candidate = _coerce_catalog_result(result, self.list_libraries(server_id))
                if _has_complete_library_memberships(candidate):
                    return candidate
                # A rich iterator is only authoritative for Lumae when it
                # preserves provider library membership. Older AudioMuse
                # iterators omit it, so use the credential-contained provider
                # bridge rather than publishing an ambiguous catalogue.
            fetcher = PROVIDER_FETCHERS.get(server["provider_type"])
            if fetcher is None:
                raise CatalogProviderError(
                    f"Provider {server['provider_type'] or 'unknown'} has no Lumae catalogue reader"
                )
            return fetcher(module, self.core, server_id)

    @contextmanager
    def bound_module(self, server_id):
        """Yield ``(server, provider module)`` with ``server_id`` bound.

        Inside, the AudioMuse provider module resolves that server's own v3
        credentials; nothing credentialed leaves the ``with`` block.
        """
        server = self.require_server(server_id)
        bind = getattr(self.core, "bind", None)
        context = bind(server_id) if callable(bind) else nullcontext()
        with context:
            yield server, self.core.provider_module(server["provider_type"])

    def probe_server_identity(self, server_id, timeout_seconds=5):
        """Return credential-free provider identity from a bounded ping.

        Navidrome: ``ping``'s ``serverVersion``. Jellyfin:
        ``/System/Info/Public`` (no credentials), adding the server ``Id``
        (``server_id``) and ``product_name`` the identity guard binds.
        """

        server = self.require_server(server_id)
        if server["provider_type"] == "jellyfin":
            with self.bound_module(server_id) as (_server, module):
                try:
                    return jellyfin_provider.probe_identity(module, timeout=timeout_seconds)
                except jellyfin_provider.JellyfinUnavailable as exc:
                    raise CatalogProviderError(str(exc)) from exc
        if server["provider_type"] != "navidrome":
            return {
                "provider_type": server["provider_type"],
                "server_type": server["provider_type"],
                "server_version": None,
            }
        bind = getattr(self.core, "bind", None)
        context = bind(server_id) if callable(bind) else nullcontext()
        with context:
            module = self.core.provider_module(server["provider_type"])
            request = getattr(module, "_navidrome_request", None)
            if not callable(request):
                raise CatalogProviderError("Navidrome provider does not expose a bounded ping")
            try:
                response = request("ping", timeout=timeout_seconds)
            except TypeError:
                # AudioMuse 2.6's compatibility function may not yet accept a
                # timeout keyword. Its own request timeout remains bounded.
                response = request("ping")
            if not isinstance(response, dict):
                raise CatalogProviderError("Navidrome ping did not return a response envelope")
            server_version = response.get("serverVersion")
            if not server_version:
                raise CatalogProviderError("Navidrome ping did not expose serverVersion")
            return {
                "provider_type": "navidrome",
                "server_type": str(response.get("type") or "navidrome").lower(),
                "server_version": str(server_version),
            }

    def download_track(self, server_id, temp_dir, item):
        self.require_server(server_id)
        with self.core.bind(server_id):
            module = self.core.provider_module(
                self.require_server(server_id)["provider_type"]
            )
            downloader = getattr(module, "download_track", None)
            if not callable(downloader):
                raise CatalogProviderError("Provider does not support track downloads")
            return downloader(temp_dir, item)


def _coerce_catalog_result(result, libraries):
    if isinstance(result, dict):
        coerced = {
            "libraries": list(result.get("libraries") or libraries or []),
            "albums": list(result.get("albums") or []),
            "tracks": list(result.get("tracks") or []),
        }
        if isinstance(result.get("artist_cover_art"), dict):
            coerced["artist_cover_art"] = dict(result["artist_cover_art"])
        return coerced
    return {"libraries": list(libraries or []), "albums": [], "tracks": list(result or [])}


def _row_library_ids(row):
    values = row.get("_lumae_library_ids") if isinstance(row, dict) else None
    if values is None and isinstance(row, dict):
        value = row.get("musicFolderId") or row.get("LibraryId") or row.get("library_id")
        values = [value] if value is not None else []
    if not isinstance(values, (list, tuple, set)):
        values = [values]
    return {str(value) for value in values if value is not None and str(value)}


def _has_complete_library_memberships(catalog):
    tracks = list(catalog.get("tracks") or [])
    libraries = list(catalog.get("libraries") or [])
    if not tracks or not libraries:
        return True
    library_ids = {
        str(row.get("id") or row.get("Id") or row.get("ItemId"))
        for row in libraries
        if isinstance(row, dict) and (row.get("id") or row.get("Id") or row.get("ItemId"))
    }
    return bool(library_ids) and all(
        bool(_row_library_ids(row)) and _row_library_ids(row).issubset(library_ids)
        for row in tracks
    )


def _fetch_navidrome(module, core, server_id):
    request = getattr(module, "_navidrome_request", None)
    if not callable(request):
        return _coerce_catalog_result(core.get_all_songs(server_id, apply_filter=True), [])
    libraries = list(module.list_libraries() or [])
    target_ids = None
    target = getattr(module, "_get_target_music_folder_ids", None)
    if callable(target):
        target_ids = target()
    page_size = 500
    album_library_ids = {}
    albums_by_id = {}
    available_folder_ids = {
        str(row.get("id") or row.get("Id"))
        for row in libraries
        if isinstance(row, dict) and (row.get("id") or row.get("Id")) is not None
    }
    if target_ids is None:
        selected_folder_ids = available_folder_ids
    else:
        selected_folder_ids = {str(value) for value in target_ids}
    if not selected_folder_ids:
        detail = (
            "The configured Navidrome music-library names did not match any music folder."
            if target_ids is not None
            else "Navidrome did not expose any music folders."
        )
        raise CatalogProviderError(
            f"{detail} Check Music Libraries in AudioMuse before preparing Lumae."
        )

    for folder_id in sorted(selected_folder_ids):
        offset = 0
        while True:
            response = request(
                "getAlbumList2",
                {
                    "type": "newest",
                    "size": page_size,
                    "offset": offset,
                    "musicFolderId": folder_id,
                },
            ) or {}
            album_list = response.get("albumList2")
            if not isinstance(album_list, dict):
                raise CatalogProviderError(
                    f"Navidrome did not return albums for music folder {folder_id}."
                )
            page = album_list.get("album") or []
            if isinstance(page, dict):
                page = [page]
            for album in page:
                album_id = album.get("id")
                if album_id is None:
                    continue
                album_id = str(album_id)
                album_library_ids.setdefault(album_id, set()).add(folder_id)
                albums_by_id.setdefault(album_id, dict(album))
                albums_by_id[album_id]["_lumae_library_ids"] = sorted(
                    album_library_ids[album_id]
                )
            if len(page) < page_size:
                break
            offset += len(page)

    tracks = []
    if len(albums_by_id) <= NAVIDROME_SMALL_SCOPE_ALBUM_LIMIT:
        for album_id in albums_by_id:
            payload = request("getAlbum", {"id": album_id}) or {}
            hydrated_album = dict(payload.get("album") or {})
            songs = hydrated_album.pop("song", []) if hydrated_album else []
            if hydrated_album:
                hydrated_album["_lumae_library_ids"] = sorted(album_library_ids[album_id])
                albums_by_id[album_id] = {
                    **albums_by_id[album_id],
                    **hydrated_album,
                }
            if isinstance(songs, dict):
                songs = [songs]
            for song in songs:
                if song.get("id"):
                    tracks.append(
                        {
                            **song,
                            "_lumae_library_ids": sorted(album_library_ids[album_id]),
                        }
                    )
    else:
        offset = 0
        while True:
            response = request(
                "search3",
                {"query": "", "songCount": page_size, "songOffset": offset},
            ) or {}
            page = ((response.get("searchResult3") or {}).get("song") or [])
            if isinstance(page, dict):
                page = [page]
            for song in page:
                album_id = str(song.get("albumId")) if song.get("albumId") is not None else None
                if album_id in album_library_ids and song.get("id"):
                    tracks.append(
                        {
                            **song,
                            "_lumae_library_ids": sorted(album_library_ids[album_id]),
                        }
                    )
            if len(page) < page_size:
                break
            offset += len(page)

    if not tracks:
        scope = "the selected music folders" if target_ids is not None else "the music folders"
        raise CatalogProviderError(
            f"Navidrome returned no songs for {scope}. "
            "The empty catalogue was not published; check Navidrome access and Music Libraries."
        )
    return {
        "libraries": libraries,
        "albums": list(albums_by_id.values()),
        "tracks": tracks,
        "artist_cover_art": _navidrome_artist_cover_art(request, selected_folder_ids),
    }


def _navidrome_artist_cover_art(request, folder_ids):
    """Map Navidrome artist IDs to their portrait art IDs, one getArtists per folder.

    getArtists lists album artists only, so guests are absent; the publisher
    keeps their previous value. An empty coverArt (Navidrome 0.64+: no image)
    is authoritative. Portraits are enrichment: a folder that fails is skipped
    and never fails the scan, and its artists keep their previous value too.
    """
    cover_art = {}
    for folder_id in sorted(folder_ids):
        try:
            response = request("getArtists", {"musicFolderId": folder_id}) or {}
            indexes = (response.get("artists") or {}).get("index") or []
        except Exception as exc:  # noqa: BLE001 - enrichment must not fail a scan
            # Only the type: provider errors can echo the credentialed URL.
            logger.warning(
                "Navidrome getArtists failed for music folder %s (%s); "
                "keeping published artist art",
                folder_id,
                type(exc).__name__,
            )
            continue
        if isinstance(indexes, dict):
            indexes = [indexes]
        for index in indexes:
            artists = (index.get("artist") or []) if isinstance(index, dict) else []
            if isinstance(artists, dict):
                artists = [artists]
            for artist in artists:
                if not isinstance(artist, dict) or artist.get("id") is None:
                    continue
                artist_id = str(artist["id"])
                value = str(artist.get("coverArt") or "") or None
                if value or artist_id not in cover_art:
                    cover_art[artist_id] = value
    return cover_art


def _fetch_jellyfin(module, core, server_id):
    """Jellyfin catalogue, always scoped by music library (JF.8)."""
    try:
        return jellyfin_provider.fetch_catalog(module, error_type=CatalogProviderError)
    except jellyfin_provider.JellyfinUnavailable as exc:
        raise CatalogProviderError(str(exc)) from exc


# Emby and Lyrion are not supported music servers: their readers were removed
# in 1.6.0 (JF.2). A persisted source of either type stays hidden and is never
# fetched, streamed or deleted.
PROVIDER_FETCHERS = {
    "navidrome": _fetch_navidrome,
    "jellyfin": _fetch_jellyfin,
}
