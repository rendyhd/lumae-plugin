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
                return _coerce_catalog_result(result, self.list_libraries(server_id))
            fetcher = {
                "navidrome": _fetch_navidrome,
                "jellyfin": _fetch_jellyfin_or_emby,
                "emby": _fetch_jellyfin_or_emby,
                "lyrion": _fetch_lyrion,
            }[server["provider_type"]]
            return fetcher(module, self.core, server_id)

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


def _fetch_navidrome(module, core, server_id):
    request = getattr(module, "_navidrome_request", None)
    if not callable(request):
        return _coerce_catalog_result(core.get_all_songs(server_id, apply_filter=True), [])
    libraries = list(module.list_libraries() or [])
    target_ids = None
    target = getattr(module, "_get_target_music_folder_ids", None)
    if callable(target):
        target_ids = target()
    tracks = []
    page_size = 500
    albums = []
    hydrated_tracks = {}
    album_library_ids = {}

    if target_ids is None:
        offset = 0
        while True:
            response = request(
                "search3",
                {"query": "", "songCount": page_size, "songOffset": offset},
            ) or {}
            page = ((response.get("searchResult3") or {}).get("song") or [])
            if isinstance(page, dict):
                page = [page]
            tracks.extend(page)
            if len(page) < page_size:
                break
            offset += len(page)
        album_ids = list(
            dict.fromkeys(str(row.get("albumId")) for row in tracks if row.get("albumId"))
        )
    else:
        selected_folder_ids = {str(value) for value in target_ids}
        if not selected_folder_ids:
            raise CatalogProviderError(
                "The configured Navidrome music-library names did not match any music folder. "
                "Check Music Libraries in AudioMuse before preparing Lumae."
            )
        album_ids = []
        albums_by_id = {}
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
                        f"Navidrome did not return albums for selected music folder {folder_id}."
                    )
                page = album_list.get("album") or []
                if isinstance(page, dict):
                    page = [page]
                for album in page:
                    album_id = album.get("id")
                    if album_id is None:
                        continue
                    album_id = str(album_id)
                    album_library_ids.setdefault(album_id, set()).add(str(folder_id))
                    if album_id not in albums_by_id:
                        album_ids.append(album_id)
                        albums_by_id[album_id] = dict(album)
                    albums_by_id[album_id]["_lumae_library_ids"] = sorted(
                        album_library_ids[album_id]
                    )
                if len(page) < page_size:
                    break
                offset += len(page)
        albums.extend(albums_by_id.values())

    for album_id in album_ids:
        payload = request("getAlbum", {"id": album_id}) or {}
        album = dict(payload.get("album") or {})
        songs = album.pop("song", []) if isinstance(album, dict) else []
        selected_library_ids = sorted(album_library_ids.get(str(album_id), set()))
        if selected_library_ids:
            album["_lumae_library_ids"] = selected_library_ids
        if album and target_ids is None:
            albums.append(album)
        if isinstance(songs, dict):
            songs = [songs]
        for song in songs:
            if song.get("id"):
                hydrated = dict(song)
                if selected_library_ids:
                    hydrated["_lumae_library_ids"] = selected_library_ids
                hydrated_tracks[str(song["id"])] = hydrated
    if target_ids is not None:
        tracks = list(hydrated_tracks.values())
    if not tracks:
        scope = "the selected music folders" if target_ids is not None else "the server"
        raise CatalogProviderError(
            f"Navidrome returned no songs for {scope}. "
            "The empty catalogue was not published; check Navidrome access and Music Libraries."
        )
    return {
        "libraries": libraries,
        "albums": albums,
        # getAlbum is richer than search3, but Navidrome commonly omits
        # musicFolderId from the hydrated songs. Merge instead of replacing so
        # the provider-authoritative folder membership survives hydration.
        "tracks": [
            {**row, **hydrated_tracks.get(str(row.get("id")), {})}
            for row in tracks
        ],
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
