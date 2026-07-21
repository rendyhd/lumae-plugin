"""Plugin-owned provider catalogue schema and source identity management."""

from datetime import datetime, timezone
import base64
import hashlib
import hmac
import json
import re
import secrets
import unicodedata
import uuid

from plugin.api import table

from .catalog_providers import ProviderCatalogBridge


CATALOG_SCHEMA_VERSION = 2
ANALYSIS_SCHEMA_VERSION = 2


def t(name):
    return table(name)


def utc_now():
    return datetime.now(timezone.utc)


ENTITY_ORDER = ("library", "artist", "album", "track")
ENTITY_TABLES = {
    "library": ("catalog_libraries", "library_id"),
    "artist": ("catalog_artists", "artist_id"),
    "album": ("catalog_albums", "album_id"),
    "track": ("catalog_tracks", "track_id"),
}
ENTITY_COLLECTIONS = {
    "library": "libraries",
    "artist": "artists",
    "album": "albums",
    "track": "tracks",
}
_PRIVATE_FIELD = re.compile(
    r"(?:password|token|credential|secret|authorization|userdata|playcount|lastplayed|favorite|rating)",
    re.IGNORECASE,
)
_PATH_FIELD = re.compile(r"^(?:path|filepath|url|streamurl|downloadurl)$", re.IGNORECASE)


class CatalogScanError(RuntimeError):
    pass


def _value(row, *names, default=None):
    for name in names:
        if isinstance(row, dict) and row.get(name) is not None:
            return row[name]
    return default


def _text(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", str(value)).strip() or None


def _integer(value):
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _number(value):
    if value in (None, ""):
        return None
    try:
        result = float(value)
        return result if result == result and result not in (float("inf"), float("-inf")) else None
    except (TypeError, ValueError):
        return None


def _string_list(value):
    if value in (None, ""):
        return []
    values = value if isinstance(value, (list, tuple, set)) else [value]
    return [item for item in (_text(raw) for raw in values) if item]


def _duration_ms(row):
    ticks = _integer(_value(row, "RunTimeTicks", "runTimeTicks"))
    if ticks is not None:
        return max(0, ticks // 10_000)
    milliseconds = _integer(_value(row, "duration_ms", "durationMs"))
    if milliseconds is not None:
        return max(0, milliseconds)
    seconds = _value(
        row,
        "duration",
        "Duration",
        "duration_seconds",
        "durationSeconds",
        "DurationSeconds",
    )
    try:
        return max(0, round(float(seconds) * 1000)) if seconds is not None else None
    except (TypeError, ValueError):
        return None


def _safe_payload(value):
    if isinstance(value, dict):
        return {
            _text(key): _safe_payload(item)
            for key, item in value.items()
            if not _PRIVATE_FIELD.search(str(key)) and not _PATH_FIELD.match(str(key))
        }
    if isinstance(value, (list, tuple)):
        return [_safe_payload(item) for item in value]
    if isinstance(value, bytes):
        return None
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return _text(value) if isinstance(value, str) else value
    return _text(value)


def canonical_json(value):
    return json.dumps(
        _safe_payload(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def fingerprint(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _artist_rows(row, fallback_name=None, default_role="artist"):
    raw_items = _value(row, "ArtistItems", "artistItems")
    if not raw_items:
        names = _value(row, "Artists", "artists")
        if isinstance(names, str):
            names = [names]
        if names:
            raw_items = [{"Name": name} for name in names]
    if not raw_items:
        name = _value(row, "artist", "Artist", "AlbumArtist", "albumartist", default=fallback_name)
        artist_id = _value(row, "artistId", "ArtistId", "albumArtistId")
        raw_items = [{"Id": artist_id, "Name": name}] if name else []
    result = []
    for position, item in enumerate(raw_items or []):
        if not isinstance(item, dict):
            item = {"Name": item}
        name = _text(_value(item, "Name", "name"))
        if not name:
            continue
        native_id = _text(_value(item, "Id", "id"))
        provenance = "provider_id" if native_id else "derived_display_name"
        artist_id = native_id or f"derived:{fingerprint({'name': name.casefold()})[:24]}"
        result.append(
            {
                "artist_id": artist_id,
                "name": name,
                "position": position,
                "identity_provenance": provenance,
                "role": _text(_value(item, "Role", "role")) or default_role,
            }
        )
    return result


def _media_payload(row):
    sources = _value(row, "MediaSources", "mediaSources", default=[]) or []
    source = sources[0] if sources and isinstance(sources[0], dict) else {}
    streams = (
        _value(source, "MediaStreams", "mediaStreams", default=[])
        or _value(row, "MediaStreams", "mediaStreams", default=[])
        or []
    )
    audio_stream = next(
        (
            stream
            for stream in streams
            if isinstance(stream, dict) and str(_value(stream, "Type", "type", default="audio")).lower() == "audio"
        ),
        {},
    )
    return {
        "duration_ms": _duration_ms(row),
        "container": _text(
            _value(
                row,
                "suffix",
                "contentType",
                "ContentType",
                "container",
                default=source.get("Container"),
            )
        ),
        "bit_rate": _integer(
            _value(
                row,
                "bitRate",
                "Bitrate",
                default=_value(
                    source,
                    "Bitrate",
                    default=_value(audio_stream, "BitRate", "Bitrate"),
                ),
            )
        ),
        "sample_rate": _integer(
            _value(
                row,
                "sampleRate",
                "SampleRate",
                default=_value(audio_stream, "SampleRate"),
            )
        ),
        "bit_depth": _integer(_value(row, "bitDepth", "BitDepth", default=_value(audio_stream, "BitDepth"))),
        "channels": _integer(
            _value(
                row,
                "channelCount",
                "Channels",
                default=_value(audio_stream, "Channels"),
            )
        ),
        "size": _integer(_value(row, "size", "Size", default=source.get("Size"))),
    }


def _replay_gain_payload(row):
    nested = _value(row, "replayGain", "ReplayGain", default={}) or {}
    return {
        "track_gain_db": _number(
            _value(
                row,
                "replayGainTrackGain",
                "ReplayGainTrackGain",
                default=_value(nested, "trackGain", "TrackGain"),
            )
        ),
        "track_peak": _number(
            _value(
                row,
                "replayGainTrackPeak",
                "ReplayGainTrackPeak",
                default=_value(nested, "trackPeak", "TrackPeak"),
            )
        ),
        "album_gain_db": _number(
            _value(
                row,
                "replayGainAlbumGain",
                "ReplayGainAlbumGain",
                default=_value(nested, "albumGain", "AlbumGain"),
            )
        ),
        "album_peak": _number(
            _value(
                row,
                "replayGainAlbumPeak",
                "ReplayGainAlbumPeak",
                default=_value(nested, "albumPeak", "AlbumPeak"),
            )
        ),
    }


def _external_ids(row):
    result = dict(_safe_payload(_value(row, "ProviderIds", "providerIds", default={})) or {})
    for key, names in {
        "musicbrainz": ("musicBrainzId", "MusicBrainzId"),
        "isrc": ("isrc", "ISRC"),
    }.items():
        found = _text(_value(row, *names))
        if found:
            result.setdefault(key, found)
    return result


def _art_payload(row):
    return {
        "cover_art_id": _text(_value(row, "coverArt", "CoverArt", "coverArtId", "PrimaryImageItemId")),
        "image_tags": _safe_payload(_value(row, "ImageTags", "imageTags", default={})),
    }


def _content_kind(row):
    raw = _text(_value(row, "content_kind", "mediaType", "MediaType", "Type", "type"))
    lowered = (raw or "music").lower()
    if "podcast" in lowered:
        return "podcast"
    if "audio" in lowered or lowered in ("music", "song", "track"):
        return "music"
    if "video" in lowered:
        return "video"
    return lowered


def normalize_provider_catalog(raw_catalog, provider_type):
    """Normalize provider occurrences without consulting AudioMuse score rows."""
    raw_catalog = raw_catalog or {}
    libraries = []
    for raw in raw_catalog.get("libraries") or []:
        library_id = _text(_value(raw, "id", "Id", "ItemId"))
        name = _text(_value(raw, "name", "Name"))
        if not library_id or not name:
            continue
        payload = _safe_payload(raw)
        libraries.append(
            {
                "library_id": library_id,
                "name": name,
                "sort_name": _text(_value(raw, "sortName", "SortName")),
                "display_order": _integer(_value(raw, "displayOrder", "DisplayOrder")),
                "payload": payload,
                "metadata_fp": fingerprint(payload),
            }
        )

    albums_by_id = {}
    artists_by_id = {}
    album_artists = []
    for raw in raw_catalog.get("albums") or []:
        album_id = _text(_value(raw, "id", "Id", "albumId", "AlbumId", "album_id"))
        name = _text(_value(raw, "name", "Name", "title", "album"))
        if not album_id or not name:
            continue
        artists = _artist_rows(
            raw,
            fallback_name=_value(raw, "AlbumArtist", "albumartist"),
            default_role="album_artist",
        )
        for artist in artists:
            artists_by_id.setdefault(artist["artist_id"], artist)
            album_artists.append({"album_id": album_id, **artist})
        art = _art_payload(raw)
        metadata = {
            "name": name,
            "sort_name": _text(_value(raw, "SortName", "sortName")),
            "album_artist_display": _text(_value(raw, "AlbumArtist", "albumArtist", "albumartist"))
            or ", ".join(artist["name"] for artist in artists)
            or None,
            "added_at": _text(_value(raw, "created", "DateCreated", "added_at")),
            "release_type": _text(_value(raw, "releaseTypes", "ReleaseType", "albumType")),
            "content_kind": _content_kind(raw),
            "year": _integer(_value(raw, "year", "ProductionYear")),
            "genres": _string_list(_value(raw, "genre", "Genres", "genres", default=[])),
            "provider_ids": _external_ids(raw),
        }
        payload = _safe_payload(raw)
        albums_by_id[album_id] = {
            "album_id": album_id,
            **metadata,
            **art,
            "payload": payload,
            "metadata_fp": fingerprint(metadata),
            "artwork_fp": fingerprint(art),
        }

    tracks = []
    track_artists = []
    entity_libraries = []
    seen_track_ids = set()
    for raw in raw_catalog.get("tracks") or []:
        track_id = _text(_value(raw, "id", "Id", "track_id"))
        title = _text(_value(raw, "title", "Name", "name"))
        if not track_id or not title:
            raise CatalogScanError("Provider track is missing its stable ID or title")
        if track_id in seen_track_ids:
            raise CatalogScanError(f"Provider returned duplicate track ID {track_id}")
        seen_track_ids.add(track_id)
        album_id = _text(_value(raw, "albumId", "AlbumId", "album_id", "albumid", "ParentId"))
        album_name = _text(_value(raw, "album", "Album"))
        if album_name and not album_id:
            raise CatalogScanError(f"Album-backed track {track_id} has no provider album identity")
        if album_id and album_id not in albums_by_id:
            inferred = {
                "id": album_id,
                "name": album_name or "Unknown Album",
                "AlbumArtist": _value(raw, "AlbumArtist", "albumArtist", "albumartist"),
                "year": _value(raw, "year", "ProductionYear"),
                "Genres": _value(raw, "genre", "Genres", "genres", default=[]),
                "releaseTypes": _value(raw, "releaseType", "albumType"),
                "coverArt": _value(raw, "coverArt", "PrimaryImageItemId"),
            }
            nested = normalize_provider_catalog({"albums": [inferred]}, provider_type)
            albums_by_id[album_id] = nested["albums"][0]
            for artist in nested["artists"]:
                artists_by_id.setdefault(artist["artist_id"], artist)
            album_artists.extend(nested["album_artists"])
        artists = _artist_rows(raw)
        for artist in artists:
            artists_by_id.setdefault(artist["artist_id"], artist)
            track_artists.append({"track_id": track_id, **artist})
        media = _media_payload(raw)
        replay_gain = _replay_gain_payload(raw)
        art = _art_payload(raw)
        kind = _content_kind(raw)
        metadata = {
            "album_id": album_id,
            "title": title,
            "artist_display": _text(_value(raw, "artist", "Artist", "AlbumArtist"))
            or ", ".join(artist["name"] for artist in artists)
            or None,
            "album_artist_display": _text(_value(raw, "albumArtist", "AlbumArtist", "albumartist"))
            or (albums_by_id.get(album_id, {}).get("album_artist_display") if album_id else None),
            "disc_number": _integer(_value(raw, "discNumber", "ParentIndexNumber", "disc", "discnumber")),
            "track_number": _integer(_value(raw, "track", "trackNumber", "IndexNumber", "tracknum")),
            "duration_ms": media["duration_ms"],
            "content_kind": kind,
            "release_type": _text(_value(raw, "releaseType", "albumType"))
            or (albums_by_id.get(album_id, {}).get("release_type") if album_id else None),
            "cover_art_id": art["cover_art_id"],
            "streamable": bool(_value(raw, "streamable", "IsPlayable", default=True)),
            "downloadable": bool(_value(raw, "downloadable", default=True)),
            "analysis_eligible": kind == "music",
            "provider_type": provider_type,
            "album": album_name,
            "year": _integer(_value(raw, "year", "ProductionYear")),
            "genres": _string_list(_value(raw, "genre", "Genres", "genres", default=[])),
            "external_ids": _external_ids(raw),
            "disc_title": _text(_value(raw, "discTitle", "DiscTitle")),
            "track_total": _integer(_value(raw, "trackTotal", "TrackTotal")),
            "disc_total": _integer(_value(raw, "discTotal", "DiscTotal")),
            "compilation": bool(_value(raw, "isCompilation", "Compilation", default=False)),
            "explicit": bool(_value(raw, "isExplicit", "Explicit", default=False)),
        }
        payload = _safe_payload(raw)
        tracks.append(
            {
                "track_id": track_id,
                **metadata,
                "payload": payload,
                "metadata_fp": fingerprint(metadata),
                "media_fp": fingerprint(media),
                "artwork_fp": fingerprint(art),
                "audio_properties": media,
                "replay_gain": replay_gain,
            }
        )
        library_id = _text(_value(raw, "musicFolderId", "LibraryId", "library_id"))
        if library_id:
            entity_libraries.append(
                {
                    "entity_type": "track",
                    "entity_id": track_id,
                    "library_id": library_id,
                }
            )

    track_credits = {}
    for credit in track_artists:
        track_credits.setdefault(credit["track_id"], []).append(
            {
                "artist_id": credit["artist_id"],
                "display_name": credit["name"],
                "position": credit["position"],
                "role": credit.get("role") or "artist",
                "identity_provenance": credit["identity_provenance"],
            }
        )
    album_credits = {}
    for credit in album_artists:
        album_credits.setdefault(credit["album_id"], []).append(
            {
                "artist_id": credit["artist_id"],
                "display_name": credit["name"],
                "position": credit["position"],
                "role": credit.get("role") or "album_artist",
                "identity_provenance": credit["identity_provenance"],
            }
        )
    track_libraries = {}
    for membership in entity_libraries:
        track_libraries.setdefault(membership["entity_id"], set()).add(membership["library_id"])
    album_libraries = {}
    artist_libraries = {}
    for track in tracks:
        library_ids = sorted(track_libraries.get(track["track_id"], set()))
        if track.get("album_id"):
            album_libraries.setdefault(track["album_id"], set()).update(library_ids)
        for credit in track_credits.get(track["track_id"], []):
            artist_libraries.setdefault(credit["artist_id"], set()).update(library_ids)
        catalogue_enrichment = {
            "album": track.get("album"),
            "year": track.get("year"),
            "genres": track.get("genres") or [],
            "external_ids": track.get("external_ids") or {},
            "disc_title": track.get("disc_title"),
            "track_total": track.get("track_total"),
            "disc_total": track.get("disc_total"),
            "compilation": track.get("compilation", False),
            "explicit": track.get("explicit", False),
            "artist_credits": sorted(
                track_credits.get(track["track_id"], []),
                key=lambda item: item["position"],
            ),
            "library_ids": library_ids,
        }
        track["payload"]["_lumae"] = {
            **catalogue_enrichment,
            "audio_properties": track["audio_properties"],
            "replay_gain": track["replay_gain"],
        }
        track["metadata_fp"] = fingerprint({"base": track["metadata_fp"], "enrichment": catalogue_enrichment})

    for album_id, library_ids in album_libraries.items():
        for library_id in library_ids:
            entity_libraries.append(
                {
                    "entity_type": "album",
                    "entity_id": album_id,
                    "library_id": library_id,
                }
            )
    for album_id, credits in album_credits.items():
        for credit in credits:
            artist_libraries.setdefault(credit["artist_id"], set()).update(album_libraries.get(album_id, set()))

    for album in albums_by_id.values():
        album_tracks = [track for track in tracks if track.get("album_id") == album["album_id"]]
        disc_titles = {}
        for track in album_tracks:
            if track.get("disc_number") is None:
                continue
            disc_number = int(track["disc_number"])
            current = disc_titles.setdefault(
                disc_number,
                {
                    "disc_number": disc_number,
                    "title": None,
                    "cover_art_id": track.get("cover_art_id"),
                },
            )
            if track.get("disc_title"):
                current["title"] = track["disc_title"]
        track_count = len(album_tracks)
        disc_count = len({track.get("disc_number") or 1 for track in album_tracks})
        duration_ms = sum(track["duration_ms"] for track in album_tracks if track.get("duration_ms") is not None)
        for track in album_tracks:
            if track.get("track_total") is None:
                track["track_total"] = track_count or None
            if track.get("disc_total") is None:
                track["disc_total"] = disc_count or None
            rich = track["payload"].get("_lumae", {})
            rich["track_total"] = track.get("track_total")
            rich["disc_total"] = track.get("disc_total")
            track["metadata_fp"] = fingerprint(
                {
                    "base": track["metadata_fp"],
                    "derived_totals": [track_count, disc_count],
                }
            )
        enrichment = {
            "year": album.get("year"),
            "genres": album.get("genres") or [],
            "external_ids": album.get("provider_ids") or {},
            "artist_credits": sorted(
                album_credits.get(album["album_id"], []),
                key=lambda item: item["position"],
            ),
            "library_ids": sorted(album_libraries.get(album["album_id"], set())),
            "disc_titles": [disc_titles[key] for key in sorted(disc_titles)],
            "track_count": track_count,
            "disc_count": disc_count,
            "duration_ms": duration_ms if any(track.get("duration_ms") is not None for track in album_tracks) else None,
            "compilation": any(
                track.get("compilation", False) for track in tracks if track.get("album_id") == album["album_id"]
            ),
            "explicit": any(
                track.get("explicit", False) for track in tracks if track.get("album_id") == album["album_id"]
            ),
        }
        album["payload"]["_lumae"] = enrichment
        album["metadata_fp"] = fingerprint({"base": album["metadata_fp"], "enrichment": enrichment})

    artists = []
    for artist in artists_by_id.values():
        metadata = {
            "name": artist["name"],
            "identity_provenance": artist["identity_provenance"],
        }
        library_ids = sorted(artist_libraries.get(artist["artist_id"], set()))
        payload = {**metadata, "_lumae": {"library_ids": library_ids}}
        artists.append({**artist, "payload": payload, "metadata_fp": fingerprint(payload)})
        for library_id in library_ids:
            entity_libraries.append(
                {
                    "entity_type": "artist",
                    "entity_id": artist["artist_id"],
                    "library_id": library_id,
                }
            )
    return {
        "libraries": sorted(libraries, key=lambda row: row["library_id"]),
        "artists": sorted(artists, key=lambda row: row["artist_id"]),
        "albums": sorted(albums_by_id.values(), key=lambda row: row["album_id"]),
        "tracks": sorted(tracks, key=lambda row: row["track_id"]),
        "track_artists": track_artists,
        "album_artists": album_artists,
        "entity_libraries": sorted(
            {(row["entity_type"], row["entity_id"], row["library_id"]): row for row in entity_libraries}.values(),
            key=lambda row: (row["entity_type"], row["entity_id"], row["library_id"]),
        ),
    }


def catalog_scope_evidence(normalized, provider_type):
    """Build non-reversible continuity evidence from provider occurrence IDs.

    AudioMuse v3's analysis de-duplication is deliberately not involved: these
    fingerprints describe the provider catalogue and its visible library
    memberships, not AudioMuse score rows or fuzzy analysis identities.
    """
    track_ids = sorted(str(row["track_id"]) for row in normalized["tracks"])
    memberships = sorted(
        (str(row["entity_id"]), str(row["library_id"]))
        for row in normalized["entity_libraries"]
        if row.get("entity_type") == "track"
    )
    visible_library_ids = sorted(str(row["library_id"]) for row in normalized["libraries"])
    scoped_library_ids = sorted({library_id for _track_id, library_id in memberships})
    library_ids = scoped_library_ids or visible_library_ids
    sample_ids = list(dict.fromkeys(track_ids[:64] + track_ids[-64:]))
    library_ids_fp = fingerprint({"library_ids": library_ids})
    provider_sample_fp = fingerprint({"track_ids": sample_ids})
    return {
        "provider_instance_fp": fingerprint({"provider_type": str(provider_type), "track_ids": track_ids}),
        "library_scope_fp": fingerprint({"library_ids": library_ids, "track_memberships": memberships}),
        "scope_summary": {
            "library_count": len(library_ids),
            "track_count": len(track_ids),
            "mapped_track_count": len({track_id for track_id, _library_id in memberships}),
            "library_ids_fp": library_ids_fp,
            "provider_sample_fp": provider_sample_fp,
        },
    }


SOURCE_VERIFICATION_MIN_TRACKS = 12
SOURCE_VERIFICATION_MAX_TRACKS = 128


def verify_library_scope(db, catalog_instance_id, library_ids, provider_track_ids=None):
    """Prove the client sees this provider catalogue without returning stored IDs.

    ``provider_track_ids`` is optional for compatibility with older Lumae apps.
    New clients submit a small sample fetched directly from their playback
    provider; matching library IDs alone is insufficient because two separate
    Navidrome databases can both number their first library ``1``.
    """
    if not isinstance(library_ids, list) or not 1 <= len(library_ids) <= 1000:
        raise ValueError("One to 1000 provider library IDs are required")
    normalized_ids = sorted(
        {unicodedata.normalize("NFC", str(value)).strip() for value in library_ids if str(value).strip()}
    )
    if not normalized_ids:
        raise ValueError("At least one provider library ID is required")
    cur = db.cursor()
    cur.execute(
        f"SELECT scope_summary FROM {t('catalog_state')} WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        raise KeyError("Unknown catalogue instance")
    summary = _state_counts(row[0])
    expected_fp = summary.get("library_ids_fp")
    actual_fp = fingerprint({"library_ids": normalized_ids})
    library_verified = bool(expected_fp) and hmac.compare_digest(str(expected_fp), actual_fp)
    result = {
        "verified": library_verified,
        "library_verified": library_verified,
        "expected_count": int(summary.get("library_count", 0) or 0),
        "submitted_count": len(normalized_ids),
        "evidence_available": bool(expected_fp),
    }
    # Preserve the v0.7 response contract for older clients. New clients
    # require the extended track-evidence fields and therefore fail closed
    # against an older plugin that silently ignores provider_track_ids.
    if provider_track_ids is None:
        cur.close()
        return result
    if (
        not isinstance(provider_track_ids, list)
        or not 1 <= len(provider_track_ids) <= SOURCE_VERIFICATION_MAX_TRACKS
    ):
        cur.close()
        raise ValueError(
            f"One to {SOURCE_VERIFICATION_MAX_TRACKS} provider track IDs are required"
        )
    normalized_track_ids = sorted(
        {
            unicodedata.normalize("NFC", str(value)).strip()
            for value in provider_track_ids
            if value is not None and str(value).strip()
        }
    )
    if not normalized_track_ids:
        cur.close()
        raise ValueError("At least one provider track ID is required")

    cur.execute(
        f"""
        SELECT c.published_generation,
               (SELECT COUNT(DISTINCT ct.track_id)
                  FROM {t("catalog_tracks")} ct
                 WHERE ct.catalog_instance_id=c.catalog_instance_id
                   AND ct.published_generation=c.published_generation
                   AND ct.available=TRUE
                   AND ct.track_id=ANY(%s))
          FROM {t("catalog_state")} c
         WHERE c.catalog_instance_id=%s
        """,
        (normalized_track_ids, catalog_instance_id),
    )
    track_row = cur.fetchone()
    cur.close()
    if track_row is None:
        raise KeyError("Unknown catalogue instance")

    expected_track_count_raw = summary.get("track_count")
    expected_track_count = int(expected_track_count_raw or 0)
    published_generation = int(track_row[0] or 0)
    matched_track_count = int(track_row[1] or 0)
    required_track_count = min(SOURCE_VERIFICATION_MIN_TRACKS, expected_track_count)
    track_evidence_available = (
        expected_track_count_raw is not None
        and expected_track_count > 0
        and published_generation > 0
    )
    sample_sufficient = (
        required_track_count > 0 and len(normalized_track_ids) >= required_track_count
    )
    tracks_verified = (
        track_evidence_available
        and sample_sufficient
        and matched_track_count == len(normalized_track_ids)
    )
    result.update(
        {
            "verified": library_verified and tracks_verified,
            "track_evidence_available": track_evidence_available,
            "tracks_verified": tracks_verified,
            "expected_track_count": expected_track_count,
            "required_track_count": required_track_count,
            "submitted_track_count": len(normalized_track_ids),
            "matched_track_count": matched_track_count,
            "sample_sufficient": sample_sufficient,
        }
    )
    return result


def migrate_catalog(db):
    """Create the complete v2 catalogue/analysis storage idempotently."""
    cur = db.cursor()
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_sources")} (
            catalog_instance_id TEXT PRIMARY KEY,
            current_core_server_id TEXT,
            provider_type TEXT NOT NULL,
            server_name TEXT NOT NULL,
            is_default BOOLEAN NOT NULL DEFAULT FALSE,
            continuity_from TEXT,
            rebind_status TEXT NOT NULL DEFAULT 'active',
            candidate_core_server_id TEXT,
            provider_instance_fp TEXT,
            library_scope_fp TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {t("idx_catalog_source_core")}
        ON {t("catalog_sources")} (current_core_server_id)
        WHERE current_core_server_id IS NOT NULL AND rebind_status = 'active'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_state")} (
            catalog_instance_id TEXT PRIMARY KEY,
            current_core_server_id TEXT,
            provider_type TEXT NOT NULL,
            catalog_schema_version INTEGER NOT NULL DEFAULT {CATALOG_SCHEMA_VERSION},
            published_generation BIGINT NOT NULL DEFAULT 0,
            catalog_epoch TEXT NOT NULL,
            catalog_head_seq BIGINT NOT NULL DEFAULT 0,
            catalog_floor_seq BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'not_initialized',
            scan_mode TEXT NOT NULL DEFAULT 'full_diff',
            entity_counts JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            field_support JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            field_coverage JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            scope_summary JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        _entity_table_sql(
            "catalog_libraries",
            "library_id",
            "name TEXT NOT NULL, sort_name TEXT, display_order INTEGER",
            include_media_fps=False,
        ),
        _entity_table_sql(
            "catalog_artists",
            "artist_id",
            "name TEXT NOT NULL, sort_name TEXT, identity_provenance TEXT, cover_art_id TEXT",
            include_media_fps=False,
        ),
        _entity_table_sql(
            "catalog_albums",
            "album_id",
            "name TEXT NOT NULL, sort_name TEXT, album_artist_display TEXT, added_at TIMESTAMPTZ, "
            "release_type TEXT, content_kind TEXT, cover_art_id TEXT",
            include_media_fps=False,
            include_artwork_fp=True,
        ),
        _entity_table_sql(
            "catalog_tracks",
            "track_id",
            "album_id TEXT, title TEXT NOT NULL, artist_display TEXT, album_artist_display TEXT, "
            "disc_number INTEGER, track_number INTEGER, duration_ms BIGINT, content_kind TEXT, "
            "release_type TEXT, cover_art_id TEXT, streamable BOOLEAN, downloadable BOOLEAN, "
            "analysis_eligible BOOLEAN",
            include_media_fps=True,
            include_artwork_fp=True,
        ),
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_track_artists")} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            track_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            artist_id TEXT,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'artist',
            identity_provenance TEXT,
            payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, published_generation, track_id, position, role)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_album_artists")} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            album_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            artist_id TEXT,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'album_artist',
            identity_provenance TEXT,
            payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, published_generation, album_id, position, role)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_entity_libraries")} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            library_id TEXT NOT NULL,
            PRIMARY KEY (catalog_instance_id, published_generation, entity_type, entity_id, library_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_disc_titles")} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            album_id TEXT NOT NULL,
            disc_number INTEGER NOT NULL,
            title TEXT,
            cover_art_id TEXT,
            payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, published_generation, album_id, disc_number)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_scans")} (
            scan_id TEXT PRIMARY KEY,
            catalog_instance_id TEXT NOT NULL,
            core_server_id TEXT NOT NULL,
            status TEXT NOT NULL,
            lease_owner TEXT,
            lease_expires_at TIMESTAMPTZ,
            progress JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ,
            last_error TEXT
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_scan_entities")} (
            scan_id TEXT NOT NULL,
            catalog_instance_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            metadata_fp TEXT NOT NULL,
            media_fp TEXT,
            artwork_fp TEXT,
            payload JSONB NOT NULL,
            PRIMARY KEY (scan_id, entity_type, entity_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("catalog_changes")} (
            catalog_instance_id TEXT NOT NULL,
            epoch TEXT NOT NULL,
            seq BIGINT NOT NULL,
            generation BIGINT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            old_entity_id TEXT,
            payload JSONB,
            evidence JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, epoch, seq)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("stream_bootstrap_sessions")} (
            token_hash TEXT PRIMARY KEY,
            stream TEXT NOT NULL,
            catalog_instance_id TEXT NOT NULL,
            core_server_id TEXT,
            principal_key TEXT NOT NULL,
            pinned_generation BIGINT NOT NULL,
            snapshot_epoch TEXT NOT NULL,
            snapshot_seq BIGINT NOT NULL,
            totals JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("analysis_state")} (
            catalog_instance_id TEXT PRIMARY KEY,
            analysis_schema_version INTEGER NOT NULL DEFAULT {ANALYSIS_SCHEMA_VERSION},
            projection_generation BIGINT NOT NULL DEFAULT 0,
            analysis_epoch TEXT NOT NULL,
            analysis_head_seq BIGINT NOT NULL DEFAULT 0,
            analysis_floor_seq BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'not_initialized',
            item_count BIGINT NOT NULL DEFAULT 0,
            mapped_track_count BIGINT NOT NULL DEFAULT 0,
            completed_at TIMESTAMPTZ,
            last_error TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("analysis_items")} (
            catalog_instance_id TEXT NOT NULL,
            projection_generation BIGINT NOT NULL,
            analysis_id TEXT NOT NULL,
            scalar_fp TEXT,
            umap_fp TEXT,
            musicnn_fp TEXT,
            clap_fp TEXT,
            scalar_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            musicnn_vector BYTEA,
            clap_vector BYTEA,
            musicnn_dimensions INTEGER,
            clap_dimensions INTEGER,
            model_metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (catalog_instance_id, projection_generation, analysis_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("track_analysis_links")} (
            catalog_instance_id TEXT NOT NULL,
            projection_generation BIGINT NOT NULL,
            provider_track_id TEXT NOT NULL,
            analysis_id TEXT,
            status TEXT NOT NULL,
            match_tier TEXT,
            algorithm TEXT,
            decision_threshold REAL,
            distance REAL,
            evidence_complete BOOLEAN NOT NULL DEFAULT FALSE,
            conflict_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
            review_state TEXT,
            PRIMARY KEY (catalog_instance_id, projection_generation, provider_track_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("analysis_changes")} (
            catalog_instance_id TEXT NOT NULL,
            epoch TEXT NOT NULL,
            seq BIGINT NOT NULL,
            generation BIGINT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, epoch, seq)
        )
        """,
    ]
    for statement in statements:
        cur.execute(statement)
    cur.close()


def _chunks(rows, size=1000):
    for offset in range(0, len(rows), size):
        yield rows[offset : offset + size]


def _json_param(value):
    return canonical_json(value)


def _insert_generation_rows(cur, entity_type, catalog_instance_id, generation, rows, now):
    table_name, id_column = ENTITY_TABLES[entity_type]
    common = ["catalog_instance_id", "published_generation", id_column]
    if entity_type == "library":
        fields = ["name", "sort_name", "display_order", "metadata_fp", "payload"]
    elif entity_type == "artist":
        fields = [
            "name",
            "sort_name",
            "identity_provenance",
            "cover_art_id",
            "metadata_fp",
            "payload",
        ]
    elif entity_type == "album":
        fields = [
            "name",
            "sort_name",
            "album_artist_display",
            "added_at",
            "release_type",
            "content_kind",
            "cover_art_id",
            "metadata_fp",
            "artwork_fp",
            "payload",
        ]
    else:
        fields = [
            "album_id",
            "title",
            "artist_display",
            "album_artist_display",
            "disc_number",
            "track_number",
            "duration_ms",
            "content_kind",
            "release_type",
            "cover_art_id",
            "streamable",
            "downloadable",
            "analysis_eligible",
            "metadata_fp",
            "media_fp",
            "artwork_fp",
            "payload",
        ]
    columns = common + fields + ["available", "first_seen_at", "last_seen_at", "deleted_at"]
    placeholders = ["%s"] * len(columns)
    placeholders[columns.index("payload")] = "%s::jsonb"
    sql = f"INSERT INTO {t(table_name)} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    params = []
    for row in rows:
        values = [catalog_instance_id, generation, row[id_column]]
        for field in fields:
            value = row.get(field)
            values.append(_json_param(value) if field == "payload" else value)
        values.extend([True, now, now, None])
        params.append(tuple(values))
    for batch in _chunks(params):
        cur.executemany(sql, batch)


def _insert_relationship_rows(cur, catalog_instance_id, generation, normalized):
    relations = (
        (
            "catalog_track_artists",
            normalized["track_artists"],
            ("track_id", "position", "artist_id", "name", "identity_provenance"),
            "artist",
        ),
        (
            "catalog_album_artists",
            normalized["album_artists"],
            ("album_id", "position", "artist_id", "name", "identity_provenance"),
            "album_artist",
        ),
    )
    for table_name, rows, fields, role in relations:
        if not rows:
            continue
        entity_id, position, artist_id, display_name, provenance = fields
        sql = f"""
            INSERT INTO {t(table_name)}
                (catalog_instance_id, published_generation, {entity_id}, position,
                 artist_id, display_name, role, identity_provenance, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """
        params = [
            (
                catalog_instance_id,
                generation,
                row[entity_id],
                row[position],
                row.get(artist_id),
                row[display_name],
                row.get("role") or role,
                row.get(provenance),
                "{}",
            )
            for row in rows
        ]
        for batch in _chunks(params):
            cur.executemany(sql, batch)
    membership = normalized["entity_libraries"]
    if membership:
        sql = f"""
            INSERT INTO {t("catalog_entity_libraries")}
                (catalog_instance_id, published_generation, entity_type, entity_id, library_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.executemany(
            sql,
            [
                (
                    catalog_instance_id,
                    generation,
                    row["entity_type"],
                    row["entity_id"],
                    row["library_id"],
                )
                for row in membership
            ],
        )


def _published_fingerprints(cur, entity_type, catalog_instance_id, generation):
    table_name, id_column = ENTITY_TABLES[entity_type]
    fp_columns = "metadata_fp"
    if entity_type == "album":
        fp_columns += ", artwork_fp"
    elif entity_type == "track":
        fp_columns += ", media_fp, artwork_fp"
    cur.execute(
        f"SELECT {id_column}, {fp_columns} FROM {t(table_name)} "
        "WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE",
        (catalog_instance_id, generation),
    )
    return {str(row[0]): tuple(row[1:]) for row in cur.fetchall()}


def _row_fingerprints(entity_type, row):
    values = [row["metadata_fp"]]
    if entity_type == "album":
        values.append(row["artwork_fp"])
    elif entity_type == "track":
        values.extend((row["media_fp"], row["artwork_fp"]))
    return tuple(values)


def _coverage(normalized):
    fields = {
        "album_id": "album_id",
        "track_number": "track_number",
        "disc_number": "disc_number",
        "track_total": "track_total",
        "disc_total": "disc_total",
        "disc_title": "disc_title",
        "duration_ms": "duration_ms",
        "media_type": "content_kind",
        "release_type": "release_type",
        "year": "year",
        "genres": "genres",
        "external_ids": "external_ids",
        "container": None,
        "bit_rate": None,
        "sample_rate": None,
        "bit_depth": None,
        "channels": None,
        "file_size": None,
        "replay_gain": None,
        "cover_art_id": "cover_art_id",
        "artist_credits": None,
        "library_membership": None,
    }
    total = len(normalized["tracks"])
    result = {}
    for public_name, field in fields.items():
        if public_name == "artist_credits":
            count = len({row["track_id"] for row in normalized["track_artists"]})
        elif public_name == "library_membership":
            count = len({row["entity_id"] for row in normalized["entity_libraries"] if row["entity_type"] == "track"})
        elif public_name == "replay_gain":
            count = sum(
                any(value is not None for value in row.get("replay_gain", {}).values()) for row in normalized["tracks"]
            )
        elif public_name in {
            "container",
            "bit_rate",
            "sample_rate",
            "bit_depth",
            "channels",
            "file_size",
        }:
            media_field = "size" if public_name == "file_size" else public_name
            count = sum(row.get("audio_properties", {}).get(media_field) is not None for row in normalized["tracks"])
        elif public_name in {"genres", "external_ids"}:
            count = sum(bool(row.get(field)) for row in normalized["tracks"])
        else:
            count = sum(row.get(field) is not None for row in normalized["tracks"])
        result[public_name] = {
            "present": count,
            "total": total,
            "ratio": round(count / total, 6) if total else 0.0,
        }
    return result


def _state_counts(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return {}
    return {}


def refresh_catalog(server_id=None, db=None, bridge=None):
    """Fetch, validate, and atomically publish one provider catalogue generation."""
    if db is None:
        from plugin.api import get_db

        db = get_db()
    provider_bridge = bridge or ProviderCatalogBridge()
    servers = provider_bridge.list_servers()
    if not server_id:
        if len(servers) != 1:
            raise CatalogScanError("An explicit server_id is required when multiple servers are configured")
        server_id = servers[0]["server_id"]
    server = provider_bridge.require_server(server_id)
    ensure_catalog_sources(db, bridge=provider_bridge)
    cur = db.cursor()
    cur.execute(
        f"SELECT catalog_instance_id FROM {t('catalog_sources')} "
        "WHERE current_core_server_id=%s AND rebind_status='active'",
        (server_id,),
    )
    source = cur.fetchone()
    if source is None:
        cur.close()
        raise CatalogScanError("Catalogue source is not initialized or is awaiting continuity review")
    catalog_instance_id = str(source[0])
    scan_id = str(uuid.uuid4())
    cur.execute(
        f"SELECT published_generation, catalog_epoch, catalog_head_seq, entity_counts "
        f"FROM {t('catalog_state')} WHERE catalog_instance_id=%s FOR UPDATE",
        (catalog_instance_id,),
    )
    state = cur.fetchone()
    if state is None:
        cur.close()
        raise CatalogScanError("Catalogue state is missing")
    previous_generation, epoch, head_seq, previous_counts = state
    cur.execute(
        f"INSERT INTO {t('catalog_scans')} "
        "(scan_id, catalog_instance_id, core_server_id, status, progress) "
        "VALUES (%s, %s, %s, 'scanning', '{}'::jsonb)",
        (scan_id, catalog_instance_id, server_id),
    )
    cur.execute(
        f"UPDATE {t('catalog_state')} SET status='scanning', started_at=now(), "
        "last_error=NULL, updated_at=now() WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    cur.close()
    db.commit()

    try:
        raw = provider_bridge.fetch_catalog(server_id)
        normalized = normalize_provider_catalog(raw, server["provider_type"])
        counts = {entity: len(normalized[ENTITY_COLLECTIONS[entity]]) for entity in ENTITY_ORDER}
        old_track_count = int(_state_counts(previous_counts).get("track", 0) or 0)
        if old_track_count and counts["track"] == 0:
            raise CatalogScanError("Refusing to replace a non-empty catalogue with an empty scan")

        cur = db.cursor()
        cur.execute(
            f"SELECT published_generation, catalog_epoch, catalog_head_seq "
            f"FROM {t('catalog_state')} WHERE catalog_instance_id=%s FOR UPDATE",
            (catalog_instance_id,),
        )
        locked_generation, locked_epoch, locked_head = cur.fetchone()
        if int(locked_generation) != int(previous_generation) or str(locked_epoch) != str(epoch):
            raise CatalogScanError("Catalogue publication moved while this scan was running")
        generation = int(previous_generation) + 1
        now = utc_now()
        changes = []
        for entity_type in ENTITY_ORDER:
            rows = normalized[ENTITY_COLLECTIONS[entity_type]]
            old = _published_fingerprints(cur, entity_type, catalog_instance_id, previous_generation)
            current = {row[ENTITY_TABLES[entity_type][1]]: row for row in rows}
            for entity_id, row in current.items():
                if old.get(entity_id) != _row_fingerprints(entity_type, row):
                    changes.append((entity_type, entity_id, "upsert", row))
            _insert_generation_rows(cur, entity_type, catalog_instance_id, generation, rows, now)
            for entity_id in sorted(set(old) - set(current)):
                changes.append((entity_type, entity_id, "delete", None))
        _insert_relationship_rows(cur, catalog_instance_id, generation, normalized)

        ordered_changes = [c for c in changes if c[2] == "upsert"]
        ordered_changes.sort(key=lambda c: (ENTITY_ORDER.index(c[0]), c[1]))
        deletes = [c for c in changes if c[2] == "delete"]
        deletes.sort(key=lambda c: (-ENTITY_ORDER.index(c[0]), c[1]))
        ordered_changes.extend(deletes)
        next_seq = int(locked_head)
        for entity_type, entity_id, operation, payload in ordered_changes:
            next_seq += 1
            cur.execute(
                f"""
                INSERT INTO {t("catalog_changes")}
                    (catalog_instance_id, epoch, seq, generation, entity_type,
                     entity_id, operation, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    catalog_instance_id,
                    epoch,
                    next_seq,
                    generation,
                    entity_type,
                    entity_id,
                    operation,
                    _json_param(payload) if payload is not None else None,
                ),
            )
        coverage = _coverage(normalized)
        scope = catalog_scope_evidence(normalized, server["provider_type"])
        field_support = {name: "observed" if value["present"] else "not_observed" for name, value in coverage.items()}
        cur.execute(
            f"""
            UPDATE {t("catalog_state")}
               SET published_generation=%s, catalog_head_seq=%s, status='complete',
                   entity_counts=%s::jsonb, field_support=%s::jsonb,
                   field_coverage=%s::jsonb, scope_summary=%s::jsonb,
                   completed_at=now(), last_error=NULL,
                   updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (
                generation,
                next_seq,
                _json_param(counts),
                _json_param(field_support),
                _json_param(coverage),
                _json_param(scope["scope_summary"]),
                catalog_instance_id,
            ),
        )
        cur.execute(
            f"UPDATE {t('catalog_sources')} SET provider_instance_fp=%s, "
            "library_scope_fp=%s, updated_at=now() WHERE catalog_instance_id=%s",
            (
                scope["provider_instance_fp"],
                scope["library_scope_fp"],
                catalog_instance_id,
            ),
        )
        cur.execute(
            f"UPDATE {t('catalog_scans')} SET status='complete', completed_at=now(), "
            "progress=%s::jsonb WHERE scan_id=%s",
            (_json_param(counts), scan_id),
        )
        cur.close()
        db.commit()
        return {
            "catalog_instance_id": catalog_instance_id,
            "server_id": server_id,
            "generation": generation,
            "cursor": {"epoch": str(epoch), "seq": next_seq},
            "counts": counts,
            "field_coverage": coverage,
            "scope_summary": scope["scope_summary"],
            "changes": len(ordered_changes),
        }
    except Exception as exc:
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        cur = db.cursor()
        cur.execute(
            f"UPDATE {t('catalog_state')} SET status='failed', last_error=%s, "
            "updated_at=now() WHERE catalog_instance_id=%s",
            (str(exc)[:1000], catalog_instance_id),
        )
        cur.execute(
            f"UPDATE {t('catalog_scans')} SET status='failed', completed_at=now(), last_error=%s WHERE scan_id=%s",
            (str(exc)[:1000], scan_id),
        )
        cur.close()
        db.commit()
        raise


def opaque_cursor(catalog_instance_id, epoch, seq):
    payload = canonical_json({"catalog_instance_id": catalog_instance_id, "epoch": epoch, "seq": int(seq)}).encode(
        "utf-8"
    )
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def parse_opaque_cursor(value):
    try:
        raw = str(value or "")
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        payload = json.loads(decoded.decode("utf-8"))
        return {
            "catalog_instance_id": str(payload["catalog_instance_id"]),
            "epoch": str(payload["epoch"]),
            "seq": int(payload["seq"]),
        }
    except (KeyError, TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("Malformed catalogue cursor") from exc


def resolve_catalog_source(db, server_id=None, catalog_instance_id=None, lock=False):
    cur = db.cursor()
    suffix = " FOR UPDATE OF s, c" if lock else ""
    if server_id:
        cur.execute(
            f"""
            SELECT s.catalog_instance_id, s.current_core_server_id, s.provider_type,
                   s.server_name, s.is_default, s.rebind_status,
                   c.published_generation, c.catalog_epoch, c.catalog_head_seq,
                   c.catalog_floor_seq, c.status, c.entity_counts, c.field_support,
                   c.field_coverage, c.started_at, c.completed_at, c.last_error,
                   a.projection_generation, a.analysis_epoch, a.analysis_head_seq,
                   a.analysis_floor_seq, a.status, a.item_count, a.mapped_track_count,
                   a.completed_at, a.last_error, s.continuity_from,
                   s.candidate_core_server_id, s.provider_instance_fp,
                   s.library_scope_fp, c.scope_summary
              FROM {t("catalog_sources")} s
              JOIN {t("catalog_state")} c USING (catalog_instance_id)
              LEFT JOIN {t("analysis_state")} a USING (catalog_instance_id)
             WHERE s.current_core_server_id=%s{suffix}
            """,
            (server_id,),
        )
    elif catalog_instance_id:
        cur.execute(
            f"""
            SELECT s.catalog_instance_id, s.current_core_server_id, s.provider_type,
                   s.server_name, s.is_default, s.rebind_status,
                   c.published_generation, c.catalog_epoch, c.catalog_head_seq,
                   c.catalog_floor_seq, c.status, c.entity_counts, c.field_support,
                   c.field_coverage, c.started_at, c.completed_at, c.last_error,
                   a.projection_generation, a.analysis_epoch, a.analysis_head_seq,
                   a.analysis_floor_seq, a.status, a.item_count, a.mapped_track_count,
                   a.completed_at, a.last_error, s.continuity_from,
                   s.candidate_core_server_id, s.provider_instance_fp,
                   s.library_scope_fp, c.scope_summary
              FROM {t("catalog_sources")} s
              JOIN {t("catalog_state")} c USING (catalog_instance_id)
              LEFT JOIN {t("analysis_state")} a USING (catalog_instance_id)
             WHERE s.catalog_instance_id=%s{suffix}
            """,
            (catalog_instance_id,),
        )
    else:
        cur.execute(
            f"""
            SELECT s.catalog_instance_id, s.current_core_server_id, s.provider_type,
                   s.server_name, s.is_default, s.rebind_status,
                   c.published_generation, c.catalog_epoch, c.catalog_head_seq,
                   c.catalog_floor_seq, c.status, c.entity_counts, c.field_support,
                   c.field_coverage, c.started_at, c.completed_at, c.last_error,
                   a.projection_generation, a.analysis_epoch, a.analysis_head_seq,
                   a.analysis_floor_seq, a.status, a.item_count, a.mapped_track_count,
                   a.completed_at, a.last_error, s.continuity_from,
                   s.candidate_core_server_id, s.provider_instance_fp,
                   s.library_scope_fp, c.scope_summary
              FROM {t("catalog_sources")} s
              JOIN {t("catalog_state")} c USING (catalog_instance_id)
              LEFT JOIN {t("analysis_state")} a USING (catalog_instance_id)
             ORDER BY s.is_default DESC, s.server_name, s.catalog_instance_id{suffix}
            """
        )
    rows = cur.fetchall()
    cur.close()
    if server_id or catalog_instance_id:
        if not rows:
            raise KeyError("Unknown catalogue source")
        rows = rows[:1]
    return [_source_dto(row) for row in rows]


def _iso(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    return str(value)


def _source_dto(row):
    return {
        "catalog_instance_id": str(row[0]),
        "server_id": str(row[1]) if row[1] is not None else None,
        "provider_type": row[2],
        "name": row[3],
        "is_default": bool(row[4]),
        "rebind_status": row[5],
        "continuity_from": row[26] if len(row) > 26 else None,
        "candidate_server_id": row[27] if len(row) > 27 else None,
        "provider_instance_fp": row[28] if len(row) > 28 else None,
        "library_scope_fp": row[29] if len(row) > 29 else None,
        "scope_summary": _state_counts(row[30]) if len(row) > 30 else {},
        "catalog": {
            "generation": int(row[6]),
            "epoch": str(row[7]),
            "head_seq": int(row[8]),
            "floor_seq": int(row[9]),
            "status": row[10],
            "entity_counts": _state_counts(row[11]),
            "field_support": _state_counts(row[12]),
            "field_coverage": _state_counts(row[13]),
            "started_at": _iso(row[14]),
            "completed_at": _iso(row[15]),
            "last_error": row[16],
        },
        "analysis": {
            "generation": int(row[17] or 0),
            "epoch": str(row[18] or ""),
            "head_seq": int(row[19] or 0),
            "floor_seq": int(row[20] or 0),
            "status": row[21] or "not_initialized",
            "item_count": int(row[22] or 0),
            "mapped_track_count": int(row[23] or 0),
            "completed_at": _iso(row[24]),
            "last_error": row[25],
        },
    }


def _session_hash(token):
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def _page_token(session_token, stream, entity_type, after_id):
    body = canonical_json({"stream": stream, "entity_type": entity_type, "after_id": after_id}).encode("utf-8")
    signature = hmac.new(str(session_token).encode("utf-8"), body, hashlib.sha256).hexdigest().encode("ascii")
    return base64.urlsafe_b64encode(body + b"." + signature).rstrip(b"=").decode("ascii")


def _parse_page_token(session_token, value):
    try:
        raw = str(value or "")
        decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        body, signature = decoded.rsplit(b".", 1)
        expected = hmac.new(str(session_token).encode("utf-8"), body, hashlib.sha256).hexdigest().encode("ascii")
        if not hmac.compare_digest(signature, expected):
            raise ValueError
        payload = json.loads(body.decode("utf-8"))
        return payload
    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("Malformed or invalid bootstrap page token") from exc


def create_bootstrap_session(
    db,
    principal_key,
    stream="catalog",
    server_id=None,
    catalog_instance_id=None,
    lifetime_minutes=30,
):
    if stream not in ("catalog", "analysis"):
        raise ValueError("Unknown bootstrap stream")
    sources = resolve_catalog_source(db, server_id=server_id, catalog_instance_id=catalog_instance_id, lock=True)
    if len(sources) != 1:
        raise ValueError("An explicit server_id is required when multiple sources exist")
    source = sources[0]
    if catalog_instance_id and source["catalog_instance_id"] != catalog_instance_id:
        raise ValueError("Catalogue source identity does not match the current server")
    state = source[stream]
    if state["status"] not in ("complete", "ready"):
        raise CatalogScanError(f"{stream.capitalize()} bootstrap is not ready")
    cur = db.cursor()
    cur.execute(f"DELETE FROM {t('stream_bootstrap_sessions')} WHERE expires_at <= now() OR completed_at IS NOT NULL")
    cur.execute(
        f"SELECT COUNT(*) FROM {t('stream_bootstrap_sessions')} "
        "WHERE principal_key=%s AND expires_at > now() AND completed_at IS NULL",
        (principal_key,),
    )
    active_count = int((cur.fetchone() or (0,))[0])
    if active_count >= 4:
        cur.close()
        raise CatalogScanError("Too many active bootstrap sessions")
    token = secrets.token_urlsafe(32)
    generation = state["generation"]
    epoch = state["epoch"]
    seq = state["head_seq"]
    totals = state.get("entity_counts") or {
        "item": state.get("item_count", 0),
        "link": state.get("mapped_track_count", 0),
    }
    cur.execute(
        f"""
        INSERT INTO {t("stream_bootstrap_sessions")}
            (token_hash, stream, catalog_instance_id, core_server_id, principal_key,
             pinned_generation, snapshot_epoch, snapshot_seq, totals, expires_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                now() + (%s * interval '1 minute'))
        """,
        (
            _session_hash(token),
            stream,
            source["catalog_instance_id"],
            source["server_id"],
            principal_key,
            generation,
            epoch,
            seq,
            _json_param(totals),
            max(5, min(int(lifetime_minutes), 60)),
        ),
    )
    cur.close()
    db.commit()
    return {
        "session_token": token,
        "stream": stream,
        "catalog_instance_id": source["catalog_instance_id"],
        "server_id": source["server_id"],
        "generation": generation,
        "snapshot_cursor": opaque_cursor(source["catalog_instance_id"], epoch, seq),
        "snapshot_seq": seq,
        "totals": totals,
        "expires_in_seconds": max(5, min(int(lifetime_minutes), 60)) * 60,
    }


def _load_bootstrap_session(db, session_token, principal_key, stream):
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT catalog_instance_id, core_server_id, pinned_generation,
               snapshot_epoch, snapshot_seq, totals
          FROM {t("stream_bootstrap_sessions")}
         WHERE token_hash=%s AND principal_key=%s AND stream=%s
           AND expires_at > now() AND completed_at IS NULL
         FOR UPDATE
        """,
        (_session_hash(session_token), principal_key, stream),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        raise KeyError("bootstrap_required")
    return cur, {
        "catalog_instance_id": str(row[0]),
        "server_id": str(row[1]) if row[1] else None,
        "generation": int(row[2]),
        "epoch": str(row[3]),
        "seq": int(row[4]),
        "totals": _state_counts(row[5]),
    }


def bootstrap_page(
    db,
    session_token,
    principal_key,
    stream="catalog",
    page_token=None,
    limit=500,
):
    limit = max(1, min(int(limit), 1000))
    cur, session = _load_bootstrap_session(db, session_token, principal_key, stream)
    if page_token:
        position = _parse_page_token(session_token, page_token)
        if position.get("stream") != stream:
            cur.close()
            raise ValueError("Bootstrap page token belongs to another stream")
        entity_type = position.get("entity_type")
        after_id = position.get("after_id") or ""
    else:
        entity_type = "library" if stream == "catalog" else "item"
        after_id = ""
    stream_entities = ENTITY_ORDER if stream == "catalog" else ("item", "link")
    if entity_type not in stream_entities:
        cur.close()
        raise ValueError("Unknown bootstrap entity type")
    if stream == "catalog":
        table_name, id_column = ENTITY_TABLES[entity_type]
        generation_column = "published_generation"
    elif entity_type == "item":
        table_name, id_column, generation_column = (
            "analysis_items",
            "analysis_id",
            "projection_generation",
        )
    else:
        table_name, id_column, generation_column = (
            "track_analysis_links",
            "provider_track_id",
            "projection_generation",
        )
    vector_exclusions = " - 'musicnn_vector' - 'clap_vector'" if stream == "analysis" and entity_type == "item" else ""
    cur.execute(
        f"""
        SELECT {id_column},
               to_jsonb(entity_row) - 'catalog_instance_id' - '{generation_column}'{vector_exclusions} AS dto
          FROM {t(table_name)} entity_row
         WHERE catalog_instance_id=%s AND {generation_column}=%s
           AND {id_column} > %s
           {"AND available=TRUE" if stream == "catalog" else ""}
         ORDER BY {id_column}
         LIMIT %s
        """,
        (session["catalog_instance_id"], session["generation"], after_id, limit + 1),
    )
    rows = cur.fetchall()
    has_more_in_entity = len(rows) > limit
    rows = rows[:limit]
    items = [row[1] if isinstance(row[1], dict) else json.loads(row[1]) for row in rows]
    next_token = None
    completed = False
    if has_more_in_entity:
        next_token = _page_token(session_token, stream, entity_type, str(rows[-1][0]))
    else:
        index = stream_entities.index(entity_type)
        if index + 1 < len(stream_entities):
            next_token = _page_token(session_token, stream, stream_entities[index + 1], "")
        else:
            completed = True
            cur.execute(
                f"UPDATE {t('stream_bootstrap_sessions')} SET completed_at=now(), "
                "last_used_at=now() WHERE token_hash=%s",
                (_session_hash(session_token),),
            )
    if not completed:
        cur.execute(
            f"UPDATE {t('stream_bootstrap_sessions')} SET last_used_at=now() WHERE token_hash=%s",
            (_session_hash(session_token),),
        )
    cur.close()
    db.commit()
    return {
        "stream": stream,
        "catalog_instance_id": session["catalog_instance_id"],
        "server_id": session["server_id"],
        "generation": session["generation"],
        "entity_type": entity_type,
        "items": items,
        "next_page_token": next_token,
        "completed": completed,
        "snapshot_cursor": opaque_cursor(session["catalog_instance_id"], session["epoch"], session["seq"]),
    }


def release_bootstrap_session(db, session_token, principal_key):
    cur = db.cursor()
    cur.execute(
        f"UPDATE {t('stream_bootstrap_sessions')} SET completed_at=now() "
        "WHERE token_hash=%s AND principal_key=%s AND completed_at IS NULL",
        (_session_hash(session_token), principal_key),
    )
    released = bool(getattr(cur, "rowcount", 0))
    cur.close()
    db.commit()
    return released


def read_catalog_changes(db, cursor_value, server_id=None, catalog_instance_id=None, limit=500):
    cursor = parse_opaque_cursor(cursor_value)
    expected_id = catalog_instance_id or cursor["catalog_instance_id"]
    sources = resolve_catalog_source(db, server_id=server_id, catalog_instance_id=None if server_id else expected_id)
    if len(sources) != 1:
        raise ValueError("An explicit server_id is required when multiple sources exist")
    source = sources[0]
    if source["catalog_instance_id"] != cursor["catalog_instance_id"]:
        raise ValueError("Cursor belongs to another catalogue source")
    state = source["catalog"]
    if cursor["epoch"] != state["epoch"] or cursor["seq"] < state["floor_seq"]:
        raise KeyError("bootstrap_required")
    if cursor["seq"] > state["head_seq"]:
        raise ValueError("Cursor is ahead of the catalogue head")
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT seq, generation, entity_type, entity_id, operation, old_entity_id,
               payload, evidence, created_at
          FROM {t("catalog_changes")}
         WHERE catalog_instance_id=%s AND epoch=%s AND seq > %s
         ORDER BY seq
         LIMIT %s
        """,
        (
            source["catalog_instance_id"],
            state["epoch"],
            cursor["seq"],
            max(1, min(int(limit), 1000)),
        ),
    )
    rows = cur.fetchall()
    cur.close()
    changes = [
        {
            "seq": int(row[0]),
            "generation": int(row[1]),
            "entity_type": row[2],
            "entity_id": str(row[3]),
            "operation": row[4],
            "old_entity_id": str(row[5]) if row[5] else None,
            "payload": row[6] if isinstance(row[6], dict) or row[6] is None else json.loads(row[6]),
            "evidence": row[7] if isinstance(row[7], dict) or row[7] is None else json.loads(row[7]),
            "created_at": _iso(row[8]),
        }
        for row in rows
    ]
    next_seq = changes[-1]["seq"] if changes else cursor["seq"]
    return {
        "catalog_instance_id": source["catalog_instance_id"],
        "server_id": source["server_id"],
        "changes": changes,
        "cursor": opaque_cursor(source["catalog_instance_id"], state["epoch"], next_seq),
        "head_cursor": opaque_cursor(source["catalog_instance_id"], state["epoch"], state["head_seq"]),
        "has_more": next_seq < state["head_seq"],
    }


def _entity_table_sql(
    table_name,
    id_column,
    fields_sql,
    include_media_fps=False,
    include_artwork_fp=False,
):
    media_sql = ", media_fp TEXT" if include_media_fps else ""
    art_sql = ", artwork_fp TEXT" if include_artwork_fp else ""
    return f"""
        CREATE TABLE IF NOT EXISTS {t(table_name)} (
            catalog_instance_id TEXT NOT NULL,
            published_generation BIGINT NOT NULL,
            {id_column} TEXT NOT NULL,
            {fields_sql},
            metadata_fp TEXT NOT NULL{media_sql}{art_sql},
            payload JSONB NOT NULL,
            available BOOLEAN NOT NULL DEFAULT TRUE,
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            deleted_at TIMESTAMPTZ,
            PRIMARY KEY (catalog_instance_id, published_generation, {id_column})
        )
    """


def _new_catalog_instance_id():
    return str(uuid.uuid4())


def ensure_catalog_sources(db, bridge=None):
    """Create source identities for the currently visible core servers."""
    provider_bridge = bridge or ProviderCatalogBridge()
    cur = db.cursor()
    cur.execute(
        f"SELECT catalog_instance_id, current_core_server_id, provider_type, rebind_status FROM {t('catalog_sources')}"
    )
    existing = [
        {
            "catalog_instance_id": row[0],
            "current_core_server_id": row[1],
            "provider_type": row[2],
            "rebind_status": row[3],
        }
        for row in cur.fetchall()
    ]
    by_server = {row["current_core_server_id"]: row for row in existing}
    legacy = by_server.get("legacy-default")
    servers = provider_bridge.list_servers()
    results = []

    if legacy and len(servers) == 1 and servers[0]["server_id"] != "legacy-default":
        candidate = servers[0]
        if candidate["provider_type"] == legacy["provider_type"]:
            cur.execute(
                f"UPDATE {t('catalog_sources')} SET rebind_status='rebind_required', "
                "candidate_core_server_id=%s, updated_at=now() WHERE catalog_instance_id=%s",
                (candidate["server_id"], legacy["catalog_instance_id"]),
            )
            results.append({**legacy, "candidate_core_server_id": candidate["server_id"]})
            cur.close()
            return results

    for server in servers:
        source = by_server.get(server["server_id"])
        if source is None:
            source_id = _new_catalog_instance_id()
            cur.execute(
                f"""
                INSERT INTO {t("catalog_sources")}
                    (catalog_instance_id, current_core_server_id, provider_type,
                     server_name, is_default, rebind_status)
                VALUES (%s, %s, %s, %s, %s, 'active')
                """,
                (
                    source_id,
                    server["server_id"],
                    server["provider_type"],
                    server["name"],
                    server["is_default"],
                ),
            )
            cur.execute(
                f"""
                INSERT INTO {t("catalog_state")}
                    (catalog_instance_id, current_core_server_id, provider_type, catalog_epoch)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (catalog_instance_id) DO NOTHING
                """,
                (
                    source_id,
                    server["server_id"],
                    server["provider_type"],
                    str(uuid.uuid4()),
                ),
            )
            cur.execute(
                f"""
                INSERT INTO {t("analysis_state")}
                    (catalog_instance_id, analysis_epoch)
                VALUES (%s, %s)
                ON CONFLICT (catalog_instance_id) DO NOTHING
                """,
                (source_id, str(uuid.uuid4())),
            )
            source = {
                "catalog_instance_id": source_id,
                "current_core_server_id": server["server_id"],
                "provider_type": server["provider_type"],
                "rebind_status": "active",
            }
        else:
            cur.execute(
                f"UPDATE {t('catalog_sources')} SET server_name=%s, provider_type=%s, "
                "is_default=%s, updated_at=now() WHERE catalog_instance_id=%s",
                (
                    server["name"],
                    server["provider_type"],
                    server["is_default"],
                    source["catalog_instance_id"],
                ),
            )
        results.append(source)

    cur.close()
    return results


def accept_legacy_rebind(db, catalog_instance_id, core_server_id, evidence):
    """Rebind v2 source ownership only when every continuity proof is true."""
    required = (
        "provider_type",
        "provider_instance",
        "library_scope",
        "provider_sample",
    )
    if not all(evidence.get(key) is True for key in required):
        raise ValueError("AudioMuse v2-to-v3 catalogue continuity evidence is incomplete")

    cur = db.cursor()
    cur.execute(
        f"SELECT current_core_server_id, rebind_status FROM {t('catalog_sources')} "
        "WHERE catalog_instance_id=%s FOR UPDATE",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        raise KeyError("Unknown catalogue instance")
    if row[0] == core_server_id and row[1] == "active":
        cur.close()
        return False
    if row[0] != "legacy-default" or row[1] != "rebind_required":
        cur.close()
        raise ValueError("Catalogue instance is not awaiting a v2-to-v3 rebind")

    cur.execute(
        f"""
        UPDATE {t("catalog_sources")}
           SET current_core_server_id=%s, continuity_from='legacy-default',
               rebind_status='active', candidate_core_server_id=NULL, updated_at=now()
         WHERE catalog_instance_id=%s
        """,
        (core_server_id, catalog_instance_id),
    )
    cur.execute(
        f"UPDATE {t('catalog_state')} SET current_core_server_id=%s, updated_at=now() WHERE catalog_instance_id=%s",
        (core_server_id, catalog_instance_id),
    )
    cur.close()
    return True


def _persisted_scope_evidence(db, catalog_instance_id):
    """Read stored evidence, deriving it from the last v2 generation if needed."""
    cur = db.cursor()
    cur.execute(
        f"""
        SELECT s.provider_type, s.provider_instance_fp, s.library_scope_fp,
               c.scope_summary, c.published_generation
          FROM {t("catalog_sources")} s
          JOIN {t("catalog_state")} c USING (catalog_instance_id)
         WHERE s.catalog_instance_id=%s
        """,
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    if row is None:
        cur.close()
        raise KeyError("Unknown catalogue instance")
    provider_type, provider_fp, scope_fp, raw_summary, generation = row
    summary = _state_counts(raw_summary)
    if provider_fp and scope_fp and summary.get("provider_sample_fp"):
        cur.close()
        return {
            "provider_instance_fp": str(provider_fp),
            "library_scope_fp": str(scope_fp),
            "scope_summary": summary,
        }

    cur.execute(
        f"SELECT library_id FROM {t('catalog_libraries')} "
        "WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE",
        (catalog_instance_id, generation),
    )
    libraries = [{"library_id": str(item[0])} for item in cur.fetchall()]
    cur.execute(
        f"SELECT track_id FROM {t('catalog_tracks')} "
        "WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE",
        (catalog_instance_id, generation),
    )
    tracks = [{"track_id": str(item[0])} for item in cur.fetchall()]
    cur.execute(
        f"SELECT entity_id, library_id FROM {t('catalog_entity_libraries')} "
        "WHERE catalog_instance_id=%s AND published_generation=%s AND entity_type='track'",
        (catalog_instance_id, generation),
    )
    memberships = [
        {"entity_type": "track", "entity_id": str(item[0]), "library_id": str(item[1])} for item in cur.fetchall()
    ]
    cur.close()
    return catalog_scope_evidence(
        {"libraries": libraries, "tracks": tracks, "entity_libraries": memberships},
        provider_type,
    )


def attempt_legacy_rebind(db, catalog_instance_id, core_server_id, bridge=None):
    """Rebind only when the v3 provider projection exactly matches stored v2 scope."""
    provider_bridge = bridge or ProviderCatalogBridge()
    cur = db.cursor()
    cur.execute(
        f"SELECT current_core_server_id, candidate_core_server_id, provider_type, "
        f"rebind_status FROM {t('catalog_sources')} WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    cur.close()
    if row is None:
        raise KeyError("Unknown catalogue instance")
    current_server_id, candidate_server_id, provider_type, rebind_status = row
    if current_server_id == core_server_id and rebind_status == "active":
        return {
            "status": "active",
            "rebound": False,
            "catalog_instance_id": catalog_instance_id,
        }
    if (
        current_server_id != "legacy-default"
        or rebind_status != "rebind_required"
        or str(candidate_server_id or "") != str(core_server_id)
    ):
        raise ValueError("Catalogue instance is not awaiting this v2-to-v3 rebind")
    candidate = provider_bridge.require_server(core_server_id)
    if candidate["provider_type"] != provider_type:
        raise ValueError("Provider type changed during AudioMuse upgrade")

    stored = _persisted_scope_evidence(db, catalog_instance_id)
    normalized = normalize_provider_catalog(provider_bridge.fetch_catalog(core_server_id), candidate["provider_type"])
    incoming = catalog_scope_evidence(normalized, candidate["provider_type"])
    evidence = {
        "provider_type": True,
        "provider_instance": stored["provider_instance_fp"] == incoming["provider_instance_fp"],
        "library_scope": stored["library_scope_fp"] == incoming["library_scope_fp"],
        "provider_sample": stored["scope_summary"].get("provider_sample_fp")
        == incoming["scope_summary"].get("provider_sample_fp"),
    }
    if not all(evidence.values()):
        return {
            "status": "rebind_required",
            "rebound": False,
            "catalog_instance_id": catalog_instance_id,
            "evidence": evidence,
        }
    changed = accept_legacy_rebind(db, catalog_instance_id, core_server_id, evidence)
    cur = db.cursor()
    cur.execute(
        f"UPDATE {t('catalog_sources')} SET provider_instance_fp=%s, library_scope_fp=%s, "
        "updated_at=now() WHERE catalog_instance_id=%s",
        (
            incoming["provider_instance_fp"],
            incoming["library_scope_fp"],
            catalog_instance_id,
        ),
    )
    cur.execute(
        f"UPDATE {t('catalog_state')} SET scope_summary=%s::jsonb, updated_at=now() WHERE catalog_instance_id=%s",
        (_json_param(incoming["scope_summary"]), catalog_instance_id),
    )
    cur.close()
    db.commit()
    return {
        "status": "active",
        "rebound": changed,
        "catalog_instance_id": catalog_instance_id,
        "server_id": core_server_id,
    }
