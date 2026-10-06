"""Tags read from the original Jellyfin file (1.6.0, JF.10).

Jellyfin 12 does not read, or does not expose, several tags that Navidrome's
OpenSubsonic API serves: release type, compilation and explicit flags,
lyricist/producer/engineer/mixer/remixer/arranger/conductor/performer
credits, ISRC, disc subtitle, original date, BPM, sort names, album
ReplayGain gain and every peak, and any ReplayGain of an MP4 file (measured
on 12.2.0, 2026-10-06). Whenever the plugin holds a Jellyfin track's original
file (AudioMuse's analysis hook, or the plugin's own profile download), it
reads those tags here (mutagen when it is installed, else PyAV's container
metadata) and stores them per track. The catalogue refresh merges them into
the raw Jellyfin rows under exactly the OpenSubsonic keys the Navidrome
reader produces, so the normalizer publishes them the same way; only tags
present in the file are emitted. Navidrome never reads this table.

``read_file_tags`` runs in the analysis child process. On the exec path that
child imports this module through a stand-in package without the plugin
``__init__`` or the host API, so the host API is imported only inside the
functions that run in the plugin process.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict


SCHEMA = 1
READ_LIMIT_SECONDS = 20
# OpenSubsonic keys merged into a raw Jellyfin track and album row. The
# normalizer folds them into the row's metadata fingerprint (Jellyfin only),
# so a tag change is an ordinary upsert.
TRACK_KEYS = (
    "isrc", "discTitle", "isCompilation", "isExplicit", "explicitStatus",
    "replayGain", "bpm", "contributors", "sortName",
)
ALBUM_KEYS = ("releaseTypes", "isCompilation", "originalReleaseDate", "explicitStatus", "sortName")
CREDIT_ROLES = (
    "composer", "lyricist", "producer", "engineer", "mixer", "remixer",
    "arranger", "conductor", "performer",
)

# Lower-cased tag keys as mutagen (ID3 frame IDs, TXXX descriptions, Vorbis
# fields, MP4 atoms and freeform names) and ffmpeg/PyAV name them.
_ALIASES = {
    "release_types": ("releasetype", "musicbrainz album type", "musicbrainz_albumtype",
                      "releasetypes"),
    "compilation": ("compilation", "tcmp", "cpil", "itunescompilation"),
    "explicit": ("itunesadvisory", "rtng", "explicit"),
    "isrc": ("isrc", "tsrc"),
    "disc_subtitle": ("discsubtitle", "tsst", "setsubtitle"),
    "original_date": ("originaldate", "tdor", "originalreleasedate"),
    "original_year": ("originalyear", "tory"),
    "bpm": ("bpm", "tbpm", "tmpo"),
    "track_gain": ("replaygain_track_gain",),
    "track_peak": ("replaygain_track_peak",),
    "album_gain": ("replaygain_album_gain",),
    "album_peak": ("replaygain_album_peak",),
    "title_sort": ("titlesort", "tsot", "sonm", "title-sort", "sort_name"),
    "album_sort": ("albumsort", "tsoa", "soal", "album-sort", "sort_album"),
}
_CREDIT_ALIASES = {
    "composer": ("composer", "tcom", "©wrt"),
    "lyricist": ("lyricist", "text"),
    "producer": ("producer",),
    "engineer": ("engineer",),
    "mixer": ("mixer",),
    "remixer": ("remixer", "tpe4"),
    "arranger": ("arranger",),
    "conductor": ("conductor", "tpe3"),
    "performer": ("performer",),
}
# ID3 TIPL "involved people" roles.
_TIPL_ROLES = {"producer": "producer", "engineer": "engineer", "mix": "mixer",
               "arranger": "arranger"}
_PERFORMER = re.compile(r"^\s*(.+?)\s*\(([^()]+)\)\s*$")
_DATE = re.compile(r"^\s*(\d{4})(?:-(\d{1,2})(?:-(\d{1,2}))?)?")
# ffmpeg joins repeated fields with ";" and no space; "A; B" is one value.
_JOINED = re.compile(r";(?!\s)")
_LIST = re.compile(r"\s*[;/]\s*")


def t(name):
    from plugin.api import table

    return table(name)


def _logger():
    from plugin.api import logger

    return logger


def migrate(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {t('jellyfin_file_tags')} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            tags JSONB NOT NULL,
            tags_fp TEXT NOT NULL,
            read_with TEXT NOT NULL,
            read_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, track_id)
        )
        """
    )


# --- reading ---------------------------------------------------------------


def _text_values(value):
    if isinstance(value, (list, tuple)):
        result = []
        for item in value:
            result.extend(_text_values(item))
        return result
    if isinstance(value, bytes):
        value = value.decode("utf-8", "replace")
    text = getattr(value, "text", None) if not isinstance(value, str) else None
    if isinstance(text, str):
        # mutagen ID3TimeStamp (TDOR, TDRC): its text is the date string.
        value = text
    elif text is not None:
        # A mutagen ID3 text frame: a list of values.
        return _text_values(list(text))
    if isinstance(value, bool):
        return ["1" if value else "0"]
    text = str(value).strip() if value is not None else ""
    return [text] if text else []


def _mutagen_entries(path):
    """``(entries, people, format)`` from mutagen, or ``None`` without it."""
    try:
        import mutagen
    except ImportError:
        return None
    audio = mutagen.File(path)
    if audio is None or audio.tags is None:
        return {}, [], "unknown"
    entries = defaultdict(list)
    people = []
    kind = type(audio.tags).__name__
    for key, value in audio.tags.items():
        name = str(key)
        if name.startswith(("APIC", "covr", "USLT", "SYLT", "PRIV", "GEOB", "UFID")):
            continue
        if name.startswith("TXXX:"):
            entries[name[5:].lower()].extend(_text_values(value))
        elif name.startswith("----:"):
            entries[name.rsplit(":", 1)[-1].lower()].extend(_text_values(value))
        elif name in ("TIPL", "TMCL", "IPLS"):
            for role, person in getattr(value, "people", []) or []:
                role, person = str(role).strip(), str(person).strip()
                if not person:
                    continue
                if name == "TMCL":
                    people.append(("performer", person, role or None))
                elif role.lower() in _TIPL_ROLES:
                    people.append((_TIPL_ROLES[role.lower()], person, None))
        elif name == "rtng":
            entries["rtng"].extend(str(int(item)) for item in value)
        elif name == "tmpo":
            entries["tmpo"].extend(str(int(item)) for item in value)
        elif name == "cpil":
            entries["cpil"].append("1" if value else "0")
        else:
            entries[name.lower()].extend(_text_values(value))
    fmt = "id3" if kind == "ID3" else ("mp4" if kind == "MP4Tags" else "vorbis")
    return dict(entries), people, fmt


def _pyav_entries(path):
    import av

    entries = defaultdict(list)
    with av.open(path, metadata_errors="ignore") as container:
        fmt = "id3" if container.format.name == "mp3" else str(container.format.name)
        sources = [dict(container.metadata)]
        sources += [dict(stream.metadata) for stream in container.streams if stream.type == "audio"]
    for source in sources:
        for key, value in source.items():
            name = str(key).lower()
            if fmt == "id3" and name == "performer":
                # ffmpeg names ID3 TPE3 (conductor) "performer".
                name = "tpe3"
            entries[name].extend(part for part in _JOINED.split(str(value)) if part.strip())
    return dict(entries), [], fmt


def _first(entries, aliases):
    for alias in aliases:
        for value in entries.get(alias, []):
            text = str(value).strip()
            if text:
                return text
    return None


def _all(entries, aliases):
    values = []
    for alias in aliases:
        for value in entries.get(alias, []):
            text = str(value).strip()
            if text and text not in values:
                values.append(text)
    return values


def _flag(value):
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in ("1", "true", "yes"):
        return True
    if text in ("0", "false", "no"):
        return False
    return None


def _number(value, unit=None):
    if value is None:
        return None
    text = str(value).strip()
    if unit and text.lower().endswith(unit):
        text = text[: -len(unit)].strip()
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _date(value):
    match = _DATE.match(str(value or ""))
    if not match:
        return None
    year, month, day = (int(part) if part else None for part in match.groups())
    if not 1 <= year <= 9999:
        return None
    result = {"year": year}
    if month and 1 <= month <= 12:
        result["month"] = month
        if day and 1 <= day <= 31:
            result["day"] = day
    return result


def canonical_tags(entries, people=()):
    """The tags this release reads, as a compact, JSON-stable dict with only
    the fields present in the file."""
    tags = {}
    release_types = []
    for value in _all(entries, _ALIASES["release_types"]):
        for part in _LIST.split(value):
            if part and part not in release_types:
                release_types.append(part)
    if release_types:
        tags["release_types"] = release_types
    compilation = _flag(_first(entries, _ALIASES["compilation"]))
    if compilation is not None:
        tags["compilation"] = compilation
    advisory = _first(entries, _ALIASES["explicit"])
    if advisory is not None:
        text = advisory.strip().lower()
        if text in ("1", "4", "explicit", "true", "yes"):
            tags["explicit"] = True
        elif text in ("2", "clean", "false", "no"):
            tags["explicit"] = False
    isrc = []
    for value in _all(entries, _ALIASES["isrc"]):
        for part in _LIST.split(value):
            code = part.strip().upper()
            if code and code not in isrc:
                isrc.append(code)
    if isrc:
        tags["isrc"] = isrc
    subtitle = _first(entries, _ALIASES["disc_subtitle"])
    if subtitle:
        tags["disc_subtitle"] = subtitle
    original = _date(_first(entries, _ALIASES["original_date"])) or _date(
        _first(entries, _ALIASES["original_year"])
    )
    if original:
        tags["original_date"] = original
    bpm = _number(_first(entries, _ALIASES["bpm"]))
    if bpm and bpm > 0:
        tags["bpm"] = int(round(bpm))
    gain = {}
    for name in ("track_gain", "album_gain"):
        value = _number(_first(entries, _ALIASES[name]), unit="db")
        if value is not None:
            gain[name] = value
    for name in ("track_peak", "album_peak"):
        value = _number(_first(entries, _ALIASES[name]))
        if value is not None and value >= 0:
            gain[name] = value
    if gain:
        tags["replay_gain"] = gain
    for name in ("title_sort", "album_sort"):
        value = _first(entries, _ALIASES[name])
        if value:
            tags[name] = value
    credits = []
    for role in CREDIT_ROLES:
        for value in _all(entries, _CREDIT_ALIASES[role]):
            name, instrument = value, None
            if role == "performer":
                match = _PERFORMER.match(value)
                if match:
                    name, instrument = match.group(1), match.group(2).strip()
            credit = [role, name.strip(), instrument]
            if credit[1] and credit not in credits:
                credits.append(credit)
    for role, name, instrument in people:
        credit = [role, name, instrument]
        if credit not in credits:
            credits.append(credit)
    if credits:
        # One order whichever reader and tag layout supplied them.
        tags["credits"] = sorted(
            credits, key=lambda item: (CREDIT_ROLES.index(item[0]), item[1], item[2] or "")
        )
    return tags


def read_file_tags(path):
    """Canonical tags of one audio file, plus the reader used. Runs in the
    analysis child process (``analysis_isolation.run_isolated``)."""
    read = _mutagen_entries(path)
    reader = "mutagen"
    if read is None:
        read = _pyav_entries(path)
        reader = "pyav"
    entries, people, _fmt = read
    return {"schema": SCHEMA, "reader": reader, "tags": canonical_tags(entries, people)}


# --- OpenSubsonic keys -----------------------------------------------------


def track_keys(tags):
    """The OpenSubsonic keys of one track's tags, only for present tags."""
    keys = {}
    if tags.get("isrc"):
        keys["isrc"] = list(tags["isrc"])
    if tags.get("disc_subtitle"):
        keys["discTitle"] = tags["disc_subtitle"]
    if "compilation" in tags:
        keys["isCompilation"] = bool(tags["compilation"])
    if "explicit" in tags:
        keys["isExplicit"] = bool(tags["explicit"])
        keys["explicitStatus"] = "explicit" if tags["explicit"] else "clean"
    gain = tags.get("replay_gain") or {}
    names = {"track_gain": "trackGain", "track_peak": "trackPeak",
             "album_gain": "albumGain", "album_peak": "albumPeak"}
    if gain:
        keys["replayGain"] = {names[name]: gain[name] for name in names if name in gain}
    if tags.get("bpm"):
        keys["bpm"] = tags["bpm"]
    if tags.get("credits"):
        keys["contributors"] = [
            {
                "role": role,
                **({"subRole": instrument} if instrument else {}),
                "artist": {"name": name},
            }
            for role, name, instrument in tags["credits"]
        ]
    if tags.get("title_sort"):
        keys["sortName"] = tags["title_sort"]
    return keys


def album_keys(tag_sets):
    """Album-level OpenSubsonic keys agreed by every tagged track of the
    album that carries the tag; a disagreement leaves the key out."""

    def agreed(name):
        values = [tags[name] for tags in tag_sets if name in tags]
        if not values:
            return None
        first = json.dumps(values[0], sort_keys=True)
        return values[0] if all(json.dumps(v, sort_keys=True) == first for v in values) else None

    keys = {}
    release_types = agreed("release_types")
    if release_types:
        keys["releaseTypes"] = list(release_types)
    compilation = agreed("compilation")
    if compilation is not None:
        keys["isCompilation"] = bool(compilation)
    original = agreed("original_date")
    if original:
        keys["originalReleaseDate"] = dict(original)
    explicit = [tags["explicit"] for tags in tag_sets if "explicit" in tags]
    if any(explicit):
        keys["explicitStatus"] = "explicit"
    elif explicit:
        keys["explicitStatus"] = "clean"
    album_sort = agreed("album_sort")
    if album_sort:
        keys["sortName"] = album_sort
    return keys


def tags_fingerprint(tags):
    return hashlib.sha256(
        json.dumps(tags, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


# --- storage ---------------------------------------------------------------


def capture(db, catalog_instance_id, track_id, path):
    """Read and store the tags of a Jellyfin track's original file.

    Never raises and never touches a Navidrome source. Returns ``"stored"``,
    ``"unchanged"`` or ``None``. The next catalogue refresh publishes a
    change as an ordinary upsert.
    """
    if not (catalog_instance_id and track_id and path):
        return None
    cur = None
    try:
        cur = db.cursor()
        cur.execute(
            f"SELECT provider_type FROM {t('catalog_sources')} WHERE catalog_instance_id=%s",
            (str(catalog_instance_id),),
        )
        row = cur.fetchone()
        if not row or str(row[0] or "").strip().lower() != "jellyfin":
            return None
        from . import analysis_isolation

        result = analysis_isolation.run_isolated(
            read_file_tags, path, limit_seconds=READ_LIMIT_SECONDS
        )
        tags = (result or {}).get("tags")
        if not isinstance(tags, dict):
            return None
        fingerprint = tags_fingerprint(tags)
        cur.execute(
            f"""
            INSERT INTO {t('jellyfin_file_tags')} AS stored
                (catalog_instance_id, track_id, tags, tags_fp, read_with)
            VALUES (%s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE
               SET tags=EXCLUDED.tags, tags_fp=EXCLUDED.tags_fp,
                   read_with=EXCLUDED.read_with, read_at=now()
             WHERE stored.tags_fp IS DISTINCT FROM EXCLUDED.tags_fp
            RETURNING track_id
            """,
            (str(catalog_instance_id), str(track_id),
             json.dumps(tags, sort_keys=True, separators=(",", ":")), fingerprint,
             str(result.get("reader") or "unknown")),
        )
        stored = cur.fetchone() is not None
        db.commit()
        return "stored" if stored else "unchanged"
    except Exception as exc:  # noqa: BLE001 - tags are enrichment
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        _logger().warning(
            "lumae_analysis could not read file tags of %s (%s)", track_id, type(exc).__name__
        )
        return None
    finally:
        if cur is not None:
            cur.close()


def merge_into_raw(cur, catalog_instance_id, raw, aliases=None):
    """The raw Jellyfin catalogue with stored file tags merged in under
    their OpenSubsonic keys (tracks, then albums from their tracks).

    The keys are replaced, never accumulated, so merging again is exact.
    ``aliases`` maps a new track ID to the ID whose tags it has (the old ID
    of a fingerprint rekey: the same file), used while it has none itself.
    """
    cur.execute(
        f"SELECT track_id, tags FROM {t('jellyfin_file_tags')} WHERE catalog_instance_id=%s",
        (str(catalog_instance_id),),
    )
    stored = {
        str(track_id): (tags if isinstance(tags, dict) else json.loads(tags))
        for track_id, tags in cur.fetchall()
    }
    if not stored:
        return raw
    aliases = aliases or {}
    tracks = []
    album_tags = defaultdict(list)
    for row in raw.get("tracks") or []:
        track_id = str(row.get("Id") or "")
        tags = stored.get(track_id) or stored.get(str(aliases.get(track_id) or ""))
        row = {key: value for key, value in row.items() if key not in TRACK_KEYS}
        if tags:
            row.update(track_keys(tags))
            if row.get("AlbumId"):
                album_tags[str(row["AlbumId"])].append(tags)
        tracks.append(row)
    albums = []
    for row in raw.get("albums") or []:
        row = {key: value for key, value in row.items() if key not in ALBUM_KEYS}
        tag_sets = album_tags.get(str(row.get("Id") or ""))
        if tag_sets:
            row.update(album_keys(tag_sets))
        albums.append(row)
    return {**raw, "tracks": tracks, "albums": albums}


def rekey(cur, catalog_instance_id, track_mapping):
    """Move stored tags with a Jellyfin fingerprint rekey (same file)."""
    if not track_mapping:
        return
    olds = sorted(track_mapping)
    news = [track_mapping[old] for old in olds]
    cur.execute(
        f"DELETE FROM {t('jellyfin_file_tags')} WHERE catalog_instance_id=%s AND track_id = ANY(%s)",
        (catalog_instance_id, news),
    )
    cur.execute(
        f"""
        UPDATE {t('jellyfin_file_tags')} target
           SET track_id=mapped.new_id
          FROM unnest(%s::text[], %s::text[]) AS mapped(old_id, new_id)
         WHERE target.catalog_instance_id=%s AND target.track_id=mapped.old_id
        """,
        (olds, news, catalog_instance_id),
    )


def forget(cur, catalog_instance_id, track_ids):
    """Drop the tags of tracks whose files are gone for good."""
    if track_ids:
        cur.execute(
            f"DELETE FROM {t('jellyfin_file_tags')} "
            "WHERE catalog_instance_id=%s AND track_id = ANY(%s)",
            (catalog_instance_id, sorted(track_ids)),
        )
