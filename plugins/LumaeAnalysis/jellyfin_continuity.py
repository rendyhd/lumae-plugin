"""Jellyfin move continuity: held-missing tracks and fingerprint rekeys (1.6.0, JF.9).

Jellyfin item IDs are ``MD5(type + path)``: moving, renaming or re-casing a
file or folder gives its tracks (and its album) new IDs, the old IDs answer
404, and moving the files back restores the old IDs. Nothing here runs for a
Navidrome catalogue.

On every Jellyfin refresh, before the ordinary diff:

1. A published track missing from a complete scan of a library that is still
   read is *held*: its published rows are carried unchanged into the next
   generation for ``GRACE`` (14 days) from the first refresh that missed it.
   Jellyfin offers no availability flag the plugin could publish, so a held
   track looks exactly as before; it simply cannot play until it is rekeyed
   or comes back. A held ID that reappears is released, never rekeyed. A
   track whose library is no longer read is an ordinary deletion at once.
2. A held track pairs with a new track when AudioMuse maps both to the same
   content fingerprint (``track_server_map.item_id``, ``fp_…``), that
   fingerprint belongs to exactly one held track and to exactly one track of
   the current scan, and the new ID is not published. Duplicates never pair.
   Only IDs of the latest successful scan are ever targets.
3. A new track that could still become a pair target is held back from
   publication: a pair target until its rekey publishes, and an unmapped new
   track whose album artist, album, disc, track number and title match a
   held track until AudioMuse maps it or the held track is released.
4. Pairs publish through the existing atomic provider-identity rekey as
   contract ``provider_identity_rekey_v2`` (two identical scans, one
   transaction, manifest), with album and artist rekeys derived only when
   they are unambiguous. A held track still unpaired after ``GRACE`` is an
   ordinary deletion.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from plugin.api import logger, table

from . import migrations


GRACE = timedelta(days=14)
CONTRACT = "provider_identity_rekey_v2"
CHANGE_REASON = "jellyfin_fingerprint_rekey_v2"
PENDING_REASON = "provider_ids_moved"
PENDING_ACTION = "wait_for_lumae_rekey"
VERIFIED_REASON = "provider_identity_verified"
ABANDONED_REASON = "provider_rekey_abandoned"
EVIDENCE_KIND = "audiomuse_fingerprint"
MAX_PUBLISH_FAILURES = 3
FINGERPRINT_PREFIX = "fp_"
JELLYFIN_ID_RE = re.compile(r"^[0-9a-f]{32}$")
REKEY_ENTITY_ORDER = ("artist", "album", "track")


def t(name):
    return table(name)


def migrate(cur):
    """Additive, idempotent: the held-missing table and the v2 columns."""
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {t('jellyfin_missing_tracks')} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            missing_since TIMESTAMPTZ NOT NULL,
            fingerprint_id TEXT,
            rekey_abandoned BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, track_id)
        )
        """
    )
    migrations.ensure_columns(
        cur, t('provider_identity_transitions'),
        # NULL is v1 (Navidrome) or no rekey; v2 names its contract.
        "rekey_contract TEXT",
        "publish_failures INTEGER NOT NULL DEFAULT 0",
    )
    migrations.ensure_columns(
        cur, t('provider_identity_manifests'), "contract TEXT", "previous_transition_id TEXT"
    )
    from .file_tags import migrate as migrate_file_tags

    migrate_file_tags(cur)


@dataclass
class Continuity:
    """What one Jellyfin refresh carries, holds back and pairs."""

    normalized: dict | None
    # The raw scan to normalize: held tracks added, held-back ones removed.
    raw: dict | None = None
    missing: dict = field(default_factory=dict)
    published: dict = field(default_factory=dict)
    # Published track ID -> current-scan track ID, publishable now.
    pairs: dict = field(default_factory=dict)
    # Pairs found but not publishable now (AudioMuse busy, analysis baseline
    # incomplete); their tracks stay held and their targets held back.
    deferred: str | None = None
    deferred_pairs: dict = field(default_factory=dict)
    fingerprints: dict = field(default_factory=dict)
    mappings: dict = field(default_factory=lambda: {"artist": {}, "album": {}, "track": {}})
    # track_id -> (missing_since, fingerprint_id) to keep in the held table.
    hold: dict = field(default_factory=dict)
    # Held-table rows to delete (reappeared, expired, no longer read).
    release: set = field(default_factory=set)
    held_back: set = field(default_factory=set)
    expired: set = field(default_factory=set)

    def counts(self):
        return {
            "held": len([tid for tid in self.hold if tid not in self.pairs]),
            "held_back": len(self.held_back),
            "expired": len(self.expired),
            "paired": len(self.pairs),
            "deferred_pairs": len(self.deferred_pairs),
        }


def _fold(value):
    return unicodedata.normalize("NFC", str(value or "")).strip().casefold()


def _metadata_key(album_artist, album, disc, track, title):
    return (_fold(album_artist), _fold(album), disc or 0, track or 0, _fold(title))


def _usable_fingerprint(value, track_id):
    text = str(value or "")
    return text if text.startswith(FINGERPRINT_PREFIX) and text != str(track_id) else None


def _held_rows(cur, catalog_instance_id):
    cur.execute(
        f"SELECT track_id, missing_since, fingerprint_id, rekey_abandoned "
        f"FROM {t('jellyfin_missing_tracks')} WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    return {
        str(row[0]): {
            "missing_since": row[1],
            "fingerprint_id": str(row[2]) if row[2] else None,
            "abandoned": bool(row[3]),
        }
        for row in cur.fetchall()
    }


def _published_tracks(cur, catalog_instance_id, generation):
    cur.execute(
        f"""
        SELECT tr.track_id, tr.title, tr.album_id, tr.album_artist_display,
               tr.disc_number, tr.track_number, al.name
          FROM {t('catalog_tracks')} tr
          LEFT JOIN {t('catalog_albums')} al
            ON al.catalog_instance_id=tr.catalog_instance_id
           AND al.published_generation=tr.published_generation
           AND al.album_id=tr.album_id
         WHERE tr.catalog_instance_id=%s AND tr.published_generation=%s
           AND tr.available=TRUE
        """,
        (catalog_instance_id, generation),
    )
    tracks = {
        str(row[0]): {
            "album_id": str(row[2]) if row[2] else None,
            "key": _metadata_key(row[3], row[6], row[4], row[5], row[1]),
            "libraries": set(),
        }
        for row in cur.fetchall()
    }
    cur.execute(
        f"""
        SELECT entity_id, library_id FROM {t('catalog_entity_libraries')}
         WHERE catalog_instance_id=%s AND published_generation=%s AND entity_type='track'
        """,
        (catalog_instance_id, generation),
    )
    for track_id, library_id in cur.fetchall():
        if str(track_id) in tracks:
            tracks[str(track_id)]["libraries"].add(str(library_id))
    return tracks


def _published_ids(cur, entity_type, catalog_instance_id, generation):
    table_name, id_column = {
        "album": ("catalog_albums", "album_id"),
        "artist": ("catalog_artists", "artist_id"),
    }[entity_type]
    cur.execute(
        f"SELECT {id_column} FROM {t(table_name)} "
        "WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE",
        (catalog_instance_id, generation),
    )
    return {str(row[0]) for row in cur.fetchall()}


def _mapped(cur, adapter, server_id, *, provider_ids=None, analysis_ids=None):
    """``[(provider_track_id, analysis_id)]`` from AudioMuse's mapping."""
    sql_builder = getattr(adapter, "analysis_mapping_sql", None)
    values = sorted(provider_ids if provider_ids is not None else analysis_ids or ())
    if not callable(sql_builder) or not values:
        return []
    sql = sql_builder()
    params = (server_id,) if "%s" in sql else ()
    column = "provider_track_id" if provider_ids is not None else "analysis_id"
    cur.execute(
        f"SELECT m.provider_track_id, m.analysis_id FROM ({sql}) AS m "
        f"WHERE m.{column} = ANY(%s)",
        (*params, values),
    )
    return [(str(row[0]), str(row[1]) if row[1] is not None else None) for row in cur.fetchall()]


def _held_fingerprints(cur, catalog_instance_id, server_id, adapter, track_ids):
    """The content fingerprint of each held track: the plugin's own analysis
    link of the published projection first, else AudioMuse's (stale) map."""
    found = {}
    cur.execute(
        f"""
        SELECT l.provider_track_id, l.analysis_id
          FROM {t('track_analysis_links')} l
          JOIN {t('analysis_state')} a
            ON a.catalog_instance_id=l.catalog_instance_id
           AND a.projection_generation=l.projection_generation
         WHERE l.catalog_instance_id=%s AND l.provider_track_id = ANY(%s)
           AND l.analysis_id IS NOT NULL
        """,
        (catalog_instance_id, sorted(track_ids)),
    )
    for track_id, analysis_id in cur.fetchall():
        fingerprint = _usable_fingerprint(analysis_id, track_id)
        if fingerprint:
            found[str(track_id)] = fingerprint
    remaining = set(track_ids) - set(found)
    for track_id, analysis_id in _mapped(cur, adapter, server_id, provider_ids=remaining):
        fingerprint = _usable_fingerprint(analysis_id, track_id)
        if fingerprint and track_id in remaining:
            found[track_id] = fingerprint
    return found


def audiomuse_busy(cur):
    """AudioMuse analysis, cleaning or a migration is running: its mappings
    may be moving, so no pair is acted on now (as v1's health check)."""
    cur.execute("SELECT to_regclass('task_status') IS NOT NULL")
    row = cur.fetchone()
    if not row or not row[0]:
        return False
    cur.execute(
        """
        SELECT COUNT(*) FROM task_status
         WHERE status NOT IN ('SUCCESS', 'FAILURE', 'FAIL', 'REVOKED')
           AND (task_type ILIKE '%migration%'
                OR task_type IN ('main_analysis', 'cleaning'))
        """
    )
    return int((cur.fetchone() or (0,))[0] or 0) > 0


def _baseline_intact(cur, catalog_instance_id, generation):
    from .provider_identity_rekey import _analysis_baseline

    cur.execute(
        f"SELECT projection_generation, item_count, mapped_track_count, status "
        f"FROM {t('analysis_state')} WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    if row is None:
        return False
    baseline = _analysis_baseline(
        cur, catalog_instance_id, generation, int(row[0] or 0),
        (int(row[1] or 0), int(row[2] or 0)), str(row[3] or "not_initialized"),
    )
    return bool(baseline["integrity"])


def _raw_id(row, *names):
    for name in names:
        value = row.get(name) if isinstance(row, dict) else None
        if value not in (None, ""):
            return str(value)
    return None


def _raw_key(row, album_names):
    """The metadata key of a raw Jellyfin track, as ``_published_tracks``
    builds it from the published columns."""
    album_id = _raw_id(row, "AlbumId")
    return _metadata_key(
        row.get("AlbumArtist"),
        album_names.get(album_id) or row.get("Album"),
        row.get("ParentIndexNumber"),
        row.get("IndexNumber"),
        row.get("Name"),
    )


def _carry_raw(cur, catalog_instance_id, generation, raw, track_ids, scanned):
    """Add held tracks (and the albums only they still need) to the raw scan
    from their published payloads, which are the sanitized raw rows, so the
    normalizer rebuilds exactly the published rows, album totals included."""
    if not track_ids:
        return
    cur.execute(
        f"""
        SELECT track_id, payload FROM {t('catalog_tracks')}
         WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE
           AND track_id = ANY(%s)
        """,
        (catalog_instance_id, generation, sorted(track_ids)),
    )
    rows = []
    for track_id, payload in cur.fetchall():
        row = dict(payload or {})
        row["Id"] = str(track_id)
        if isinstance(row.get("_lumae_library_ids"), list):
            row["_lumae_library_ids"] = sorted(
                str(value) for value in row["_lumae_library_ids"] if str(value) in scanned
            )
        rows.append(row)
    present = {_raw_id(album, "Id", "id") for album in raw.get("albums") or []}
    album_ids = sorted({
        _raw_id(row, "AlbumId") for row in rows
        if _raw_id(row, "AlbumId") and _raw_id(row, "AlbumId") not in present
    })
    albums = []
    if album_ids:
        cur.execute(
            f"""
            SELECT album_id, payload FROM {t('catalog_albums')}
             WHERE catalog_instance_id=%s AND published_generation=%s AND available=TRUE
               AND album_id = ANY(%s)
            """,
            (catalog_instance_id, generation, album_ids),
        )
        for album_id, payload in cur.fetchall():
            # The normalizer recomputes the "_lumae" enrichment from tracks.
            row = {key: value for key, value in dict(payload or {}).items() if key != "_lumae"}
            row["Id"] = str(album_id)
            if isinstance(row.get("_lumae_library_ids"), list):
                row["_lumae_library_ids"] = sorted(
                    str(value) for value in row["_lumae_library_ids"] if str(value) in scanned
                )
            albums.append(row)
    raw["tracks"] = list(raw.get("tracks") or []) + rows
    raw["albums"] = list(raw.get("albums") or []) + albums


def _hold_back_raw(raw, track_ids, previous_albums):
    """Remove held-back new tracks, and new albums only they filled, from the
    raw scan; artists follow, as the normalizer derives them from rows."""
    if not track_ids:
        return
    held_back = set(track_ids)
    affected = {_raw_id(row, "AlbumId") for row in raw["tracks"] if _raw_id(row, "Id") in held_back}
    raw["tracks"] = [row for row in raw["tracks"] if _raw_id(row, "Id") not in held_back]
    still_filled = {_raw_id(row, "AlbumId") for row in raw["tracks"]}
    dropped = {
        album_id for album_id in affected
        if album_id and album_id not in still_filled and album_id not in previous_albums
    }
    raw["albums"] = [row for row in raw.get("albums") or [] if _raw_id(row, "Id") not in dropped]


def _derive(cur, catalog_instance_id, generation, normalized, pairs, published):
    """Album and artist rekeys implied by the track pairs, only when
    unambiguous: one-to-one both ways, the old ID gone from the target, the
    new ID never published, and (artists) credited only by rekeyed rows."""
    previous_albums = _published_ids(cur, "album", catalog_instance_id, generation)
    previous_artists = _published_ids(cur, "artist", catalog_instance_id, generation)
    target_albums = {row["album_id"] for row in normalized["albums"]}
    target_artists = {row["artist_id"] for row in normalized["artists"]}
    target_track_album = {row["track_id"]: row.get("album_id") for row in normalized["tracks"]}

    def unique(candidates, old_ids_present, new_ids_published):
        forward = defaultdict(set)
        backward = defaultdict(set)
        for old_id, new_id in candidates:
            forward[old_id].add(new_id)
            backward[new_id].add(old_id)
        accepted = {}
        for old_id, targets in forward.items():
            if len(targets) != 1:
                continue
            new_id = next(iter(targets))
            if (
                len(backward[new_id]) == 1
                and old_id not in old_ids_present
                and new_id not in new_ids_published
                and JELLYFIN_ID_RE.match(old_id)
                and JELLYFIN_ID_RE.match(new_id)
            ):
                accepted[old_id] = new_id
        return accepted

    album_candidates = []
    album_sources = defaultdict(set)
    for old_id, new_id in pairs.items():
        old_album = published[old_id]["album_id"]
        new_album = target_track_album.get(new_id)
        if old_album and new_album and old_album != new_album:
            album_candidates.append((old_album, new_album))
            album_sources[(old_album, new_album)].add(old_id)
    albums = unique(album_candidates, target_albums, previous_albums)

    old_credits = defaultdict(list)
    if pairs:
        cur.execute(
            f"""
            SELECT track_id, position, artist_id FROM {t('catalog_track_artists')}
             WHERE catalog_instance_id=%s AND published_generation=%s AND track_id = ANY(%s)
            """,
            (catalog_instance_id, generation, sorted(pairs)),
        )
        for track_id, position, artist_id in cur.fetchall():
            old_credits[("track", str(track_id))].append((position, str(artist_id or "")))
    if albums:
        cur.execute(
            f"""
            SELECT album_id, position, artist_id FROM {t('catalog_album_artists')}
             WHERE catalog_instance_id=%s AND published_generation=%s AND album_id = ANY(%s)
            """,
            (catalog_instance_id, generation, sorted(albums)),
        )
        for album_id, position, artist_id in cur.fetchall():
            old_credits[("album", str(album_id))].append((position, str(artist_id or "")))
    new_credits = defaultdict(list)
    for row in normalized["track_artists"]:
        new_credits[("track", row["track_id"])].append((row["position"], str(row.get("artist_id") or "")))
    for row in normalized["album_artists"]:
        new_credits[("album", row["album_id"])].append((row["position"], str(row.get("artist_id") or "")))
    artist_candidates = []
    artist_sources = defaultdict(set)
    rekeyed_rows = set()
    for kind, mapping in (("track", pairs), ("album", albums)):
        for old_id, new_id in mapping.items():
            rekeyed_rows.add((kind, new_id))
            before = sorted(old_credits.get((kind, old_id), []))
            after = sorted(new_credits.get((kind, new_id), []))
            if len(before) != len(after) or [p for p, _ in before] != [p for p, _ in after]:
                continue
            for (_position, old_artist), (_same, new_artist) in zip(before, after):
                if old_artist and new_artist and old_artist != new_artist:
                    artist_candidates.append((old_artist, new_artist))
                    artist_sources[(old_artist, new_artist)].add((kind, old_id))
    artists = unique(artist_candidates, target_artists, previous_artists)
    credited_by = defaultdict(set)
    for (kind, entity_id), credits in new_credits.items():
        for _position, artist_id in credits:
            credited_by[artist_id].add((kind, entity_id))
    artists = {
        old_id: new_id for old_id, new_id in artists.items()
        if credited_by[new_id] <= rekeyed_rows
    }
    album_evidence = {
        new_id: sorted(album_sources[(old_id, new_id)]) for old_id, new_id in albums.items()
    }
    artist_evidence = {}
    for old_id, new_id in artists.items():
        tracks = set()
        for kind, source_id in artist_sources[(old_id, new_id)]:
            if kind == "track":
                tracks.add(source_id)
            else:
                for (album_old, album_new), track_ids in album_sources.items():
                    if album_old == source_id:
                        tracks |= track_ids
        artist_evidence[new_id] = sorted(tracks)
    return albums, artists, album_evidence, artist_evidence


def plan(cur, *, catalog_instance_id, server_id, previous_generation, raw, adapter, now=None):
    """Find held, held-back and paired tracks for one raw Jellyfin scan.

    Reads only (``persist`` writes). ``Continuity.raw`` is the adjusted raw
    catalogue to normalize, or the input itself when nothing is held.
    """
    now = now or datetime.now(timezone.utc)
    continuity = Continuity(normalized=None)
    continuity.raw = raw
    if previous_generation <= 0:
        return continuity
    stored = _held_rows(cur, catalog_instance_id)
    published = _published_tracks(cur, catalog_instance_id, previous_generation)
    current = {_raw_id(row, "Id"): row for row in raw.get("tracks") or [] if _raw_id(row, "Id")}
    scanned = {
        _raw_id(row, "id", "Id", "ItemId") for row in raw.get("libraries") or []
        if isinstance(row, dict)
    }
    continuity.release = {
        track_id for track_id in stored if track_id in current or track_id not in published
    }
    missing = {}
    for track_id, info in published.items():
        if track_id in current:
            continue
        if info["libraries"] and not info["libraries"] & scanned:
            # Its library is no longer read: an ordinary deletion now.
            if track_id in stored:
                continuity.release.add(track_id)
            continue
        row = stored.get(track_id) or {}
        missing[track_id] = {
            "missing_since": row.get("missing_since") or now,
            "fingerprint_id": row.get("fingerprint_id"),
            "abandoned": bool(row.get("abandoned")),
        }
    if not missing:
        return continuity
    unknown = [track_id for track_id, held in missing.items() if not held["fingerprint_id"]]
    if unknown:
        for track_id, fingerprint in _held_fingerprints(
            cur, catalog_instance_id, server_id, adapter, unknown
        ).items():
            missing[track_id]["fingerprint_id"] = fingerprint

    new_ids = set(current) - set(published)
    candidates = {
        track_id: held for track_id, held in missing.items()
        if held["fingerprint_id"] and not held["abandoned"] and JELLYFIN_ID_RE.match(track_id)
    }
    pairs = {}
    if candidates and new_ids:
        live = defaultdict(set)
        for provider_id, analysis_id in _mapped(
            cur, adapter, server_id,
            analysis_ids={held["fingerprint_id"] for held in candidates.values()},
        ):
            if provider_id in current:
                live[analysis_id].add(provider_id)
        held_by_fingerprint = defaultdict(set)
        for track_id, held in candidates.items():
            held_by_fingerprint[held["fingerprint_id"]].add(track_id)
        for fingerprint, olds in held_by_fingerprint.items():
            targets = live.get(fingerprint, set())
            if len(olds) != 1 or len(targets) != 1:
                continue
            new_id = next(iter(targets))
            if new_id in new_ids and JELLYFIN_ID_RE.match(new_id):
                pairs[next(iter(olds))] = new_id

    continuity.expired = {
        track_id for track_id, held in missing.items()
        if track_id not in pairs and now - held["missing_since"] >= GRACE
    }
    continuity.release |= continuity.expired
    held = {track_id: info for track_id, info in missing.items() if track_id not in continuity.expired}
    continuity.hold = {
        track_id: (info["missing_since"], info["fingerprint_id"]) for track_id, info in held.items()
    }

    # An unmapped new track that looks like a held, fingerprinted track is
    # probably its move target still waiting for AudioMuse: hold it back.
    held_back = set()
    waiting_by_key = defaultdict(set)
    for track_id in held:
        if track_id in candidates and track_id not in pairs:
            waiting_by_key[published[track_id]["key"]].add(track_id)
    waiting_olds = set()
    unpaired_new = new_ids - set(pairs.values())
    if waiting_by_key and unpaired_new:
        mapped = {provider_id for provider_id, _ in _mapped(
            cur, adapter, server_id, provider_ids=unpaired_new)}
        album_names = {
            _raw_id(row, "Id", "id"): row.get("Name") for row in raw.get("albums") or []
        }
        for track_id in sorted(unpaired_new - mapped):
            key = _raw_key(current[track_id], album_names)
            if key in waiting_by_key:
                held_back.add(track_id)
                waiting_olds |= waiting_by_key[key]

    if pairs:
        if audiomuse_busy(cur):
            continuity.deferred = "audiomuse_busy"
        elif not _baseline_intact(cur, catalog_instance_id, previous_generation):
            continuity.deferred = "analysis_baseline_incomplete"
    if continuity.deferred:
        continuity.deferred_pairs = dict(pairs)
        pairs = {}
    else:
        # An album moves as one: while a sibling's move target still waits
        # for AudioMuse, the album's pairs wait too, so its album rekey can
        # still be derived.
        waiting_albums = {published[track_id]["album_id"] for track_id in waiting_olds}
        for old_id in sorted(pairs):
            if published[old_id]["album_id"] and published[old_id]["album_id"] in waiting_albums:
                continuity.deferred_pairs[old_id] = pairs.pop(old_id)
    held_back |= set(continuity.deferred_pairs.values())

    if pairs or held_back or any(track_id not in pairs for track_id in held):
        adjusted = {**raw, "tracks": list(raw.get("tracks") or []),
                    "albums": list(raw.get("albums") or [])}
        _hold_back_raw(
            adjusted, held_back,
            _published_ids(cur, "album", catalog_instance_id, previous_generation),
        )
        _carry_raw(
            cur, catalog_instance_id, previous_generation, adjusted,
            [track_id for track_id in held if track_id not in pairs], scanned,
        )
        if pairs:
            # A rekey target is the moved file itself: it publishes with the
            # file tags read under its old ID (JF.10), album keys included.
            from .file_tags import merge_into_raw

            adjusted = merge_into_raw(
                cur, catalog_instance_id, adjusted,
                aliases={new_id: old_id for old_id, new_id in pairs.items()},
            )
        continuity.raw = adjusted
    continuity.held_back = held_back
    continuity.pairs = pairs
    continuity.missing = missing
    continuity.published = published
    if continuity.hold or continuity.held_back or continuity.expired:
        logger.warning(
            "lumae_analysis holds missing Jellyfin tracks of %s: %s",
            catalog_instance_id, continuity.counts(),
        )
    return continuity


def derive(cur, catalog_instance_id, previous_generation, continuity, normalized):
    """Album and artist rekeys and evidence for the pairs, from the
    normalized target; sets ``continuity.mappings`` and ``fingerprints``."""
    continuity.normalized = normalized
    pairs = continuity.pairs
    if not pairs:
        return continuity
    missing = continuity.missing
    albums, artists, album_evidence, artist_evidence = _derive(
        cur, catalog_instance_id, previous_generation, normalized, pairs, continuity.published,
    )
    continuity.mappings = {"artist": artists, "album": albums, "track": dict(pairs)}
    continuity.fingerprints = {
        "track": {new_id: missing[old_id]["fingerprint_id"] for old_id, new_id in pairs.items()},
        "album": {
            new_id: sorted({missing[track_id]["fingerprint_id"] for track_id in track_ids})
            for new_id, track_ids in album_evidence.items()
        },
        "artist": {
            new_id: sorted({missing[track_id]["fingerprint_id"] for track_id in track_ids})
            for new_id, track_ids in artist_evidence.items()
        },
    }
    validate_mapping(continuity.mappings)
    return continuity


def validate_mapping(mappings):
    """The v2 rules: 32 lowercase hex Jellyfin IDs, one-to-one, old != new,
    and no ID both old and new anywhere in the manifest."""
    all_old = {old_id for mapping in mappings.values() for old_id in mapping}
    all_new = {new_id for mapping in mappings.values() for new_id in mapping.values()}
    if all_old & all_new:
        raise ValueError("A Jellyfin ID is both old and new in one rekey")
    for entity_type, mapping in mappings.items():
        olds = set(mapping)
        news = list(mapping.values())
        if len(set(news)) != len(news):
            raise ValueError(f"Jellyfin {entity_type} rekey is not one-to-one")
        if olds & set(news):
            raise ValueError(f"A Jellyfin {entity_type} ID is both old and new")
        for old_id, new_id in mapping.items():
            if old_id == new_id or not (JELLYFIN_ID_RE.match(old_id) and JELLYFIN_ID_RE.match(new_id)):
                raise ValueError(f"Invalid Jellyfin {entity_type} rekey")


def persist(cur, catalog_instance_id, continuity, rekeyed=()):
    """Write the held table in the refresh's own transaction."""
    from . import file_tags

    # A file gone for good takes its stored tags with it.
    file_tags.forget(cur, catalog_instance_id, continuity.expired)
    release = set(continuity.release) | set(rekeyed)
    if release:
        cur.execute(
            f"DELETE FROM {t('jellyfin_missing_tracks')} "
            "WHERE catalog_instance_id=%s AND track_id = ANY(%s)",
            (catalog_instance_id, sorted(release)),
        )
    rows = [
        (catalog_instance_id, track_id, since, fingerprint)
        for track_id, (since, fingerprint) in sorted(continuity.hold.items())
        if track_id not in release
    ]
    if rows:
        cur.executemany(
            f"""
            INSERT INTO {t('jellyfin_missing_tracks')} AS held
                (catalog_instance_id, track_id, missing_since, fingerprint_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE
               SET fingerprint_id=COALESCE(held.fingerprint_id, EXCLUDED.fingerprint_id),
                   updated_at=now()
            """,
            rows,
        )


def _sorted_mappings(mappings):
    order = {name: index for index, name in enumerate(("library", "artist", "album", "track"))}
    return sorted(
        (
            {"entity_type": entity_type, "old_id": old_id, "new_id": new_id}
            for entity_type, mapping in mappings.items()
            for old_id, new_id in mapping.items()
        ),
        key=lambda row: (order[row["entity_type"]], row["old_id"]),
    )


def target_fingerprint(normalized, mappings):
    """v1's stable-scan fingerprint of the target, bound to the pairs."""
    from .catalog import canonical_json
    from .provider_identity_rekey import target_scan_fingerprint

    material = target_scan_fingerprint(normalized) + "\n" + canonical_json(_sorted_mappings(mappings))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _purge_stale_targets(cur, catalog_instance_id, new_track_ids):
    """Derived rows a target ID may still hold from an earlier publication
    (an ID that comes back after a move): the rekey must not collide with
    them. Profiles are recomputed; nothing personal lives here."""
    if not new_track_ids:
        return
    from .profile_publication import record_profile_change

    ids = sorted(new_track_ids)
    cur.execute(
        f"DELETE FROM {t('source_profiles')} WHERE catalog_instance_id=%s AND track_id = ANY(%s)",
        (catalog_instance_id, ids),
    )
    cur.execute(
        f"DELETE FROM {t('published_source_profiles')} "
        "WHERE catalog_instance_id=%s AND track_id = ANY(%s) RETURNING track_id",
        (catalog_instance_id, ids),
    )
    for (track_id,) in sorted(cur.fetchall()):
        record_profile_change(cur, catalog_instance_id, str(track_id), "deleted")
    for name in ("edge_profile_jobs", "edge_profiles"):
        cur.execute(
            f"DELETE FROM {t(name)} WHERE catalog_instance_id=%s AND track_id = ANY(%s)",
            (catalog_instance_id, ids),
        )


def rekey_spec(continuity, catalog_instance_id):
    """The ``RekeySpec`` that publishes these pairs as contract v2."""
    from .provider_identity_rekey import RekeySpec

    mappings = continuity.mappings
    fingerprints = continuity.fingerprints

    def event_evidence(event):
        if event.entity_type == "track":
            return {"fingerprint_id": fingerprints["track"][event.entity_id]}
        return {
            "derived_from": "track",
            "fingerprint_ids": fingerprints[event.entity_type][event.entity_id],
        }

    def health(cur, _adapter, _server_id, _plan):
        # The pairs are AudioMuse's own current mappings, so AudioMuse has
        # nothing to migrate; only a running analysis makes it not ready.
        return "busy" if audiomuse_busy(cur) else "ready"

    def before(cur, source, plan):
        _purge_stale_targets(
            cur, source,
            [row["new_id"] for row in plan.mappings if row["entity_type"] == "track"],
        )

    def after(cur, source, _plan, _transition_id):
        from . import file_tags

        # The moved file is the same file: its stored tags move with it.
        file_tags.rekey(cur, source, mappings["track"])
        persist(cur, source, continuity, rekeyed=mappings["track"].keys())

    return RekeySpec(
        contract=CONTRACT,
        change_reason=CHANGE_REASON,
        provider_type="jellyfin",
        mapping=mappings,
        target_fingerprint=lambda normalized: target_fingerprint(normalized, mappings),
        base_evidence={"kind": EVIDENCE_KIND, "deterministic": False},
        event_evidence=event_evidence,
        audiomuse_health=health,
        before_rekey=before,
        after_rekey=after,
    )


def advance_transition(cur, catalog_instance_id, *, observed_version, target, mapping_count):
    """Open (from normal/applied, a new transition ID) or advance the v2
    proof: an identical target on the same baselines counts one more scan."""
    cur.execute(
        f"""
        SELECT p.state, p.transition_id, p.target_fingerprint, p.target_scan_count,
               p.baseline_catalog_generation, p.baseline_analysis_generation,
               p.rekey_contract, p.current_provider_version,
               COALESCE(c.published_generation, 0), COALESCE(a.projection_generation, 0)
          FROM {t('provider_identity_transitions')} p
          JOIN {t('catalog_state')} c USING (catalog_instance_id)
          LEFT JOIN {t('analysis_state')} a USING (catalog_instance_id)
         WHERE p.catalog_instance_id=%s
         FOR UPDATE OF p
        """,
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError("Provider identity transition is missing")
    state, transition_id, stored_target, count = row[0], row[1], row[2], int(row[3] or 0)
    generation, analysis_generation = int(row[8] or 0), int(row[9] or 0)
    counts = {"rekey": mapping_count, "unchanged": 0, "addition": 0,
              "confirmed_removal": 0, "conflict": 0}
    version = str(observed_version or row[7] or "").strip() or None
    continuing = (
        state == "transition_pending" and row[6] == CONTRACT and transition_id
    )
    if continuing:
        same = (
            stored_target == target
            and int(row[4] or 0) == generation
            and int(row[5] or 0) == analysis_generation
        )
        count = count + 1 if same else 1
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET current_provider_version=COALESCE(%s, current_provider_version),
                   baseline_catalog_generation=%s, baseline_analysis_generation=%s,
                   counts=%s::jsonb, target_fingerprint=%s, target_scan_count=%s,
                   checked_at=now(), last_error=NULL, updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (version, generation, analysis_generation, _json(counts), target, count,
             catalog_instance_id),
        )
    else:
        import uuid

        transition_id = str(uuid.uuid4())
        count = 1
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET transition_id=%s, state='transition_pending',
                   previous_provider_version=current_provider_version,
                   current_provider_version=COALESCE(%s, current_provider_version),
                   baseline_catalog_generation=%s, baseline_analysis_generation=%s,
                   detection_reason=%s, required_action=%s, counts=%s::jsonb,
                   target_fingerprint=%s, target_scan_count=1,
                   rekey_contract=%s, publish_failures=0,
                   first_seq=NULL, last_seq=NULL, manifest_sha256=NULL,
                   applied_at=NULL, audiomuse_health=NULL, baseline_integrity=NULL,
                   detected_at=now(), checked_at=now(), last_error=NULL,
                   updated_at=now()
             WHERE catalog_instance_id=%s
            """,
            (transition_id, version, generation, analysis_generation, PENDING_REASON,
             PENDING_ACTION, _json(counts), target, CONTRACT, catalog_instance_id),
        )
    cur.execute(
        f"SELECT current_provider_version FROM {t('provider_identity_transitions')} "
        "WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    current_version = (cur.fetchone() or (None,))[0]
    return {
        "state": "transition_pending",
        "contract": CONTRACT,
        "transition_id": transition_id,
        "inspection": PENDING_REASON,
        "required_action": PENDING_ACTION,
        "counts": counts,
        "target_fingerprint": target,
        "target_scan_count": count,
        "current_provider_version": current_version,
    }


def close_pending_transition(cur, catalog_instance_id, reason=VERIFIED_REASON):
    """A pending v2 proof without publishable pairs any more (the track came
    back, AudioMuse became busy, ...) returns to ``normal``; the next pair
    opens a new transition ID."""
    cur.execute(
        f"""
        UPDATE {t('provider_identity_transitions')}
           SET state='normal', transition_id=NULL, detection_reason=%s,
               required_action=NULL, target_fingerprint=NULL, target_scan_count=0,
               rekey_contract=NULL, publish_failures=0, detected_at=NULL,
               baseline_catalog_generation=(
                   SELECT published_generation FROM {t('catalog_state')}
                    WHERE catalog_instance_id=%s),
               checked_at=now(), updated_at=now()
         WHERE catalog_instance_id=%s AND state='transition_pending'
           AND rekey_contract=%s
        """,
        (reason, catalog_instance_id, catalog_instance_id, CONTRACT),
    )


def record_publish_failure(db, catalog_instance_id, transition_id, continuity, error):
    """Count a failed v2 publication; after ``MAX_PUBLISH_FAILURES`` the pairs
    are abandoned (their tracks stay held until the grace period ends and
    their targets publish as new tracks) so a rekey can never wedge the
    catalogue."""
    from .redaction import redact_error_text

    cur = db.cursor()
    try:
        cur.execute(
            f"""
            UPDATE {t('provider_identity_transitions')}
               SET publish_failures=publish_failures + 1, last_error=%s, updated_at=now()
             WHERE catalog_instance_id=%s AND transition_id=%s AND rekey_contract=%s
            RETURNING publish_failures
            """,
            (redact_error_text(str(error)) or "rekey_failed", catalog_instance_id,
             transition_id, CONTRACT),
        )
        row = cur.fetchone()
        failures = int(row[0]) if row else 0
        if failures >= MAX_PUBLISH_FAILURES:
            old_ids = sorted(continuity.pairs)
            cur.executemany(
                f"""
                INSERT INTO {t('jellyfin_missing_tracks')} AS held
                    (catalog_instance_id, track_id, missing_since, fingerprint_id,
                     rekey_abandoned)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (catalog_instance_id, track_id) DO UPDATE
                   SET rekey_abandoned=TRUE, updated_at=now()
                """,
                [
                    (catalog_instance_id, track_id, *continuity.hold[track_id])
                    for track_id in old_ids
                    if track_id in continuity.hold
                ],
            )
            close_pending_transition(cur, catalog_instance_id, ABANDONED_REASON)
            logger.warning(
                "lumae_analysis abandoned %d Jellyfin rekeys of %s after %d failed "
                "publications", len(old_ids), catalog_instance_id, failures,
            )
        db.commit()
    except Exception:
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        logger.warning("lumae_analysis could not record a failed Jellyfin rekey", exc_info=True)
    finally:
        cur.close()


def transition_contract(cur, catalog_instance_id):
    cur.execute(
        f"SELECT rekey_contract FROM {t('provider_identity_transitions')} "
        "WHERE catalog_instance_id=%s",
        (catalog_instance_id,),
    )
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else None


def _json(value):
    import json

    return json.dumps(value, sort_keys=True, separators=(",", ":"))
