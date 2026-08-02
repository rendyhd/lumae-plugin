"""Credential-contained provider bridge shared by catalogue scanners.

Version-specific dispatcher behavior stays in the core adapters. This module
exposes only sanitized server descriptions and raw provider catalogue objects
to the normalizer; callers must never persist or serialize the bridge itself.
"""

from contextlib import nullcontext

from .core_compat import get_core_adapter


# Lumae's mobile catalogue contract is intentionally Navidrome-only for now.
# Keep the other normalizers below as future-facing compatibility code, but do
# not admit those providers into a source of truth until the app supports them.
SUPPORTED_PROVIDER_TYPES = frozenset(("navidrome",))
NAVIDROME_SMALL_SCOPE_ALBUM_LIMIT = 32


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
                if server["provider_type"] != "navidrome" or _has_complete_library_memberships(
                    candidate
                ):
                    return candidate
                # A rich iterator is only authoritative for Lumae when it
                # preserves provider library membership. Older AudioMuse
                # iterators omit it, so use the credential-contained provider
                # bridge rather than publishing an ambiguous catalogue.
            fetcher = {
                "navidrome": _fetch_navidrome,
                "jellyfin": _fetch_jellyfin_or_emby,
                "emby": _fetch_jellyfin_or_emby,
                "lyrion": _fetch_lyrion,
            }[server["provider_type"]]
            return fetcher(module, self.core, server_id)

    def probe_server_identity(self, server_id, timeout_seconds=5):
        """Return credential-free provider identity from a bounded ping."""

        server = self.require_server(server_id)
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
        return {
            "libraries": list(result.get("libraries") or libraries or []),
            "albums": list(result.get("albums") or []),
            "tracks": list(result.get("tracks") or []),
        }
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
    }


def _fetch_jellyfin_or_emby(module, core, server_id):
    libraries = list(module.list_libraries() or [])
    target = getattr(module, "_get_target_library_ids", None)
    target_ids = target() if callable(target) else None
    fetch_page = getattr(module, "_fetch_songs_paged", None)
    if not callable(fetch_page):
        tracks = core.get_all_songs(server_id, apply_filter=True)
    elif target_ids is None:
        tracks = fetch_page(None)
    else:
        tracks = []
        for library_id in sorted(target_ids):
            page = fetch_page(None, library_id)
            for row in page:
                row.setdefault("LibraryId", str(library_id))
            tracks.extend(page)
    albums = {}
    for row in tracks:
        album_id = row.get("AlbumId") or row.get("ParentId")
        if not album_id:
            continue
        albums.setdefault(
            str(album_id),
            {
                "Id": str(album_id),
                "Name": row.get("Album") or "Unknown Album",
                "AlbumArtist": row.get("AlbumArtist"),
                "ProductionYear": row.get("ProductionYear"),
                "Genres": row.get("Genres"),
                "ProviderIds": row.get("AlbumProviderIds") or {},
                "ImageTags": row.get("AlbumImageTags") or {},
                "LibraryId": row.get("LibraryId"),
            },
        )
    return {"libraries": libraries, "albums": list(albums.values()), "tracks": list(tracks)}


def _fetch_lyrion(module, core, server_id):
    libraries = list(module.list_libraries() or [])
    request = getattr(module, "_jsonrpc_request", None)
    if callable(request):
        response = request("titles", [0, 999999, "tags:galduAyRKNSECTIQZ"])
        tracks = (response or {}).get("titles_loop") or []
    else:
        tracks = core.get_all_songs(server_id, apply_filter=True)
    albums = {}
    for row in tracks:
        album_id = row.get("album_id") or row.get("albumid")
        if album_id:
            albums.setdefault(
                str(album_id),
                {
                    "id": str(album_id),
                    "title": row.get("album"),
                    "albumartist": row.get("albumartist"),
                    "year": row.get("year"),
                },
            )
    return {"libraries": libraries, "albums": list(albums.values()), "tracks": list(tracks)}
