"""The loudness stream (Lumae LN.4b; contract §3.9, K13): published track and album loudness.

Track records come from the edge pass (``bs1770-4-48k``) and the standalone path (the backfill:
``bs1770-4-native`` up to 48 kHz, ``bs1770-4-48k`` above); album records pool their members' histograms when every
member of the album in the source's published catalogue generation has a record of the current
version for its current media. Every change is journaled in ``loudness_changes`` with §3.4's
rules; ``/loudness/bootstrap`` pages the live tables under a pinned head and ``/loudness/changes``
replays the journal.

Never a profile, edge or catalogue event: loudness lives beside them, so a measurement never moves
the catalogue or profile cursors (D11).
"""

import base64
import hashlib
import json
import logging
import uuid
from datetime import timezone

from plugin.api import table

from . import migrations
from .catalog import (
    change_journal_retention_limit,
    compact_change_journal,
    opaque_cursor,
    parse_opaque_cursor,
    read_change_page,
    resolve_catalog_source,
)
from .loudness_v1 import METHODS, VERSION, pool_album, track_record_fields

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
# Minimum journal retention per source; the effective limit is two complete libraries of the
# catalogue's tracks and albums (``retention_limit``, refreshed by ``reconcile``), as the profile
# journal (P1-2).
RETENTION_EVENTS = 50_000
# Most journal rows one publication may compact past the current floor.
COMPACTION_MAX_ADVANCE = 5_000
DEFAULT_PAGE = 500
MAX_PAGE = 1000
TRACK_FIELDS = ("version", "method", "integrated_lufs", "true_peak_dbtp", "sample_peak_dbfs", "lra_lu",
                "max_short_term_lufs", "max_momentary_lufs", "channels", "sample_rate")
ALBUM_FIELDS = ("version", "integrated_lufs", "true_peak_dbtp", "loudest_track_lufs", "lra_lu", "members", "exact")


def t(name):
    return table(name)


def _dumps(value):
    """Canonical JSON that keeps every key and string as it is (``catalog.canonical_json`` scrubs
    private-looking keys and normalizes, trims and nulls strings: never for ids or tokens)."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def capability():
    """``capabilities.loudness``."""
    return {"version": VERSION, "methods": list(METHODS), "album": True}


def migrate_loudness(db):
    cur = db.cursor()
    migrations.apply(cur, [
        f"""
        CREATE TABLE IF NOT EXISTS {t("loudness_stream_state")} (
            catalog_instance_id TEXT PRIMARY KEY
                REFERENCES {t("catalog_sources")}(catalog_instance_id) ON DELETE CASCADE,
            epoch TEXT NOT NULL,
            head_seq BIGINT NOT NULL DEFAULT 0,
            floor_seq BIGINT NOT NULL DEFAULT 0,
            retention_limit BIGINT NOT NULL DEFAULT {RETENTION_EVENTS},
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("loudness_tracks")} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t("catalog_sources")}(catalog_instance_id) ON DELETE CASCADE,
            track_id TEXT NOT NULL,
            media_revision TEXT NOT NULL,
            version INTEGER NOT NULL,
            record JSONB NOT NULL,
            histogram BYTEA NOT NULL,
            measured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, track_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("loudness_albums")} (
            catalog_instance_id TEXT NOT NULL
                REFERENCES {t("catalog_sources")}(catalog_instance_id) ON DELETE CASCADE,
            album_id TEXT NOT NULL,
            track_set_hash TEXT NOT NULL,
            record JSONB NOT NULL,
            published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, album_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {t("loudness_changes")} (
            catalog_instance_id TEXT NOT NULL,
            epoch TEXT NOT NULL,
            seq BIGINT NOT NULL,
            kind TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (catalog_instance_id, epoch, seq)
        )
        """,
    ])
    # Every source has its epoch before a client reads (a GET never has to create one).
    cur.execute(f"SELECT catalog_instance_id FROM {t('catalog_sources')} ORDER BY catalog_instance_id")
    for (catalog_instance_id,) in cur.fetchall():
        _state(cur, str(catalog_instance_id))
    cur.close()


def _state(cur, catalog_instance_id, *, for_update=False):
    """(epoch, head_seq, floor_seq, retention_limit); the row is created on first use."""
    query = (f"SELECT epoch, head_seq, floor_seq, retention_limit FROM {t('loudness_stream_state')} "
             "WHERE catalog_instance_id=%s" + (" FOR UPDATE" if for_update else ""))
    cur.execute(query, (catalog_instance_id,))
    row = cur.fetchone()
    if row is None:
        cur.execute(
            f"INSERT INTO {t('loudness_stream_state')} (catalog_instance_id, epoch) VALUES (%s, %s) "
            "ON CONFLICT (catalog_instance_id) DO NOTHING",
            (catalog_instance_id, str(uuid.uuid4())),
        )
        cur.execute(query, (catalog_instance_id,))
        row = cur.fetchone()
    epoch, head_seq, floor_seq, retention = row
    return str(epoch), int(head_seq), int(floor_seq), int(retention)


def track_set_hash(pairs):
    """§3.9: sha256 of the members' [track_id, media_revision] pairs sorted by track_id (code points)."""
    ordered = sorted(([str(track), str(revision)] for track, revision in pairs), key=lambda pair: pair[0])
    text = json.dumps(ordered, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def track_item(track_id, media_revision, record):
    item = {"kind": "track", "track_id": track_id, "media_revision": media_revision}
    item.update({key: record.get(key) for key in TRACK_FIELDS})
    return item


def album_item(album_id, hash_value, record):
    item = {"kind": "album", "album_id": album_id, "track_set_hash": hash_value}
    item.update({key: record.get(key) for key in ALBUM_FIELDS})
    return item


def deletion(kind, entity_id):
    return {"kind": kind, "id": entity_id, "op": "delete"}


class _Journal:
    """Appends events to one source's journal under its locked state row; ``close`` moves the head."""

    def __init__(self, cur, catalog_instance_id):
        self.cur = cur
        self.source = catalog_instance_id
        self.epoch, self.head, self.floor, self.retention = _state(cur, catalog_instance_id, for_update=True)
        self.start = self.head

    def append(self, kind, entity_id, operation, item):
        self.head += 1
        self.cur.execute(
            f"INSERT INTO {t('loudness_changes')} "
            "(catalog_instance_id, epoch, seq, kind, entity_id, operation, payload) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (self.source, self.epoch, self.head, kind, entity_id, operation, _dumps(item)),
        )

    def close(self):
        if self.head == self.start:
            return 0
        self.cur.execute(
            f"UPDATE {t('loudness_stream_state')} SET head_seq=%s, updated_at=now() "
            "WHERE catalog_instance_id=%s AND epoch=%s",
            (self.head, self.source, self.epoch),
        )
        compact_change_journal(
            self.cur, catalog_instance_id=self.source, state_table="loudness_stream_state",
            changes_table="loudness_changes", epoch_column="epoch", floor_column="floor_seq",
            epoch=self.epoch, head_seq=self.head, retention_limit=self.retention,
            current_floor=self.floor, max_advance=COMPACTION_MAX_ADVANCE)
        return self.head - self.start


def _jsonb(value):
    return json.loads(value) if isinstance(value, str) else value


def publish_tracks(cur, catalog_instance_id, measurements):
    """Store measured track records and journal each one that changed; refresh their albums.

    ``measurements``: dicts with ``track_id``, ``media_revision`` and ``record`` (a
    ``loudness_v1`` record, with its ``histogram``). The caller commits. Returns the number of
    events journaled.
    """
    journal = _Journal(cur, catalog_instance_id)
    touched = []
    for measurement in sorted(measurements, key=lambda item: str(item["track_id"])):
        track_id, revision, record = str(measurement["track_id"]), str(measurement["media_revision"]), measurement["record"]
        fields = track_record_fields(record)
        histogram = bytes(record["histogram"])
        cur.execute(
            f"SELECT media_revision, record FROM {t('loudness_tracks')} "
            "WHERE catalog_instance_id=%s AND track_id=%s",
            (catalog_instance_id, track_id),
        )
        row = cur.fetchone()
        cur.execute(
            f"""
            INSERT INTO {t('loudness_tracks')}
                (catalog_instance_id, track_id, media_revision, version, record, histogram, measured_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (catalog_instance_id, track_id) DO UPDATE SET
                media_revision=EXCLUDED.media_revision, version=EXCLUDED.version, record=EXCLUDED.record,
                histogram=EXCLUDED.histogram, measured_at=now()
            """,
            (catalog_instance_id, track_id, revision, int(fields["version"]), _dumps(fields), histogram),
        )
        touched.append(track_id)
        if row is not None and row[0] == revision and _jsonb(row[1]) == json.loads(_dumps(fields)):
            continue
        journal.append("track", track_id, "upsert", track_item(track_id, revision, fields))
    if touched:
        _refresh_albums(cur, catalog_instance_id, journal, track_ids=touched)
    return journal.close()


def store_measurement(db, catalog_instance_id, track_id, media_revision, record):
    """Publish one measurement in its own transaction. A failure is logged and rolled back: the edge
    job that measured it stands, and the backfill measures the track again."""
    cur = db.cursor()
    try:
        publish_tracks(cur, catalog_instance_id, [{"track_id": track_id, "media_revision": media_revision, "record": record}])
        db.commit()
        return True
    except Exception:  # noqa: BLE001 - loudness never fails the edge pass
        logger.exception("lumae_analysis loudness publication failed for %s", track_id)
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        return False
    finally:
        cur.close()


def _members(cur, catalog_instance_id, *, album_ids=None, track_ids=None):
    """{album_id: [(track_id, media_revision)]}: the music tracks (``analysis_eligible``) of each album
    in the published generation, for the given albums or for the albums of the given tracks (None,
    None: every album)."""
    if album_ids is None and track_ids is not None:
        # Their albums first: one keyed lookup, not a subquery correlated with every catalogue row.
        cur.execute(
            f"""SELECT DISTINCT x.album_id FROM {t('catalog_state')} c
                  JOIN {t('catalog_tracks')} x
                    ON x.catalog_instance_id=c.catalog_instance_id AND x.published_generation=c.published_generation
                 WHERE c.catalog_instance_id=%s AND x.track_id = ANY(%s::text[]) AND x.album_id IS NOT NULL""",
            (catalog_instance_id, [str(track) for track in track_ids]),
        )
        album_ids = [str(row[0]) for row in cur.fetchall()]
        if not album_ids:
            return {}
    where, params = "", [catalog_instance_id]
    if album_ids is not None:
        where, params = " AND m.album_id = ANY(%s::text[])", params + [list(album_ids)]
    cur.execute(
        f"""
        SELECT m.album_id, m.track_id,
               CASE WHEN m.media_fp IS NULL OR m.media_fp = '' THEN NULL ELSE
                 'sha256:' || encode(sha256(convert_to('catalog-media:' || m.media_fp, 'UTF8')), 'hex') END
          FROM {t('catalog_state')} c
          JOIN {t('catalog_tracks')} m
            ON m.catalog_instance_id=c.catalog_instance_id AND m.published_generation=c.published_generation
         WHERE c.catalog_instance_id=%s AND m.available AND m.analysis_eligible IS TRUE
           AND m.album_id IS NOT NULL{where}
         ORDER BY m.album_id, m.track_id
        """,
        params,
    )
    albums = {}
    for album_id, track_id, revision in cur.fetchall():
        albums.setdefault(str(album_id), []).append((str(track_id), revision))
    return albums


def _refresh_albums(cur, catalog_instance_id, journal, *, track_ids=None, everything=False):
    """Publish, update or withdraw the album records of the given tracks' albums (or of all)."""
    members = _members(cur, catalog_instance_id, track_ids=None if everything else track_ids)
    published = {}
    if everything:
        cur.execute(f"SELECT album_id, track_set_hash, record->>'version' FROM {t('loudness_albums')} "
                    "WHERE catalog_instance_id=%s", (catalog_instance_id,))
        published = {str(album_id): (hash_value, version) for album_id, hash_value, version in cur.fetchall()}
    stale = set(published) - set(members)
    changed = 0
    for album_id, tracks in members.items():
        if (album_id in published and all(revision for _track, revision in tracks)
                and published[album_id] == (track_set_hash(tracks), str(VERSION))):
            # Same members, same media, same version: the record stands (a new measurement refreshes
            # its albums itself). So a change to pool_album needs a VERSION bump to reach them.
            continue
        pooled, hash_value = None, None
        if all(revision for _track, revision in tracks):
            cur.execute(
                f"""SELECT track_id, media_revision, record, histogram FROM {t('loudness_tracks')}
                     WHERE catalog_instance_id=%s AND track_id = ANY(%s::text[]) AND version=%s""",
                (catalog_instance_id, [track for track, _revision in tracks], VERSION),
            )
            measured = {str(row[0]): row for row in cur.fetchall()}
            current = [measured.get(track) for track, _revision in tracks]
            if all(row is not None and row[1] == revision for row, (_track, revision) in zip(current, tracks)):
                records = [dict(_jsonb(row[2]), histogram=bytes(row[3])) for row in current]
                pooled = pool_album(records)
                hash_value = track_set_hash(tracks)
                if pooled["integrated_lufs"] is None:
                    pooled = None
        cur.execute(
            f"SELECT track_set_hash, record FROM {t('loudness_albums')} WHERE catalog_instance_id=%s AND album_id=%s",
            (catalog_instance_id, album_id),
        )
        existing = cur.fetchone()
        if pooled is None:
            if existing is not None:
                stale.add(album_id)
            continue
        record = {"version": VERSION, **pooled}
        if existing is not None and existing[0] == hash_value and _jsonb(existing[1]) == json.loads(_dumps(record)):
            continue
        cur.execute(
            f"""
            INSERT INTO {t('loudness_albums')} (catalog_instance_id, album_id, track_set_hash, record, published_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (catalog_instance_id, album_id) DO UPDATE SET
                track_set_hash=EXCLUDED.track_set_hash, record=EXCLUDED.record, published_at=now()
            """,
            (catalog_instance_id, album_id, hash_value, _dumps(record)),
        )
        journal.append("album", album_id, "upsert", album_item(album_id, hash_value, record))
        changed += 1
    for album_id in sorted(stale):
        cur.execute(f"DELETE FROM {t('loudness_albums')} WHERE catalog_instance_id=%s AND album_id=%s",
                    (catalog_instance_id, album_id))
        journal.append("album", album_id, "delete", deletion("album", album_id))
        changed += 1
    return changed


def reconcile(cur, catalog_instance_id):
    """Follow the published catalogue: withdraw records of tracks that left it, and publish, update or
    withdraw every album record. Refreshes the journal's retention. The caller commits."""
    journal = _Journal(cur, catalog_instance_id)
    cur.execute(
        f"""
        SELECT l.track_id FROM {t('loudness_tracks')} l
         WHERE l.catalog_instance_id=%s AND NOT EXISTS (
            SELECT 1 FROM {t('catalog_state')} c JOIN {t('catalog_tracks')} m
              ON m.catalog_instance_id=c.catalog_instance_id AND m.published_generation=c.published_generation
             WHERE c.catalog_instance_id=l.catalog_instance_id AND m.track_id=l.track_id AND m.available)
         ORDER BY l.track_id
        """,
        (catalog_instance_id,),
    )
    gone = [str(row[0]) for row in cur.fetchall()]
    if gone:
        cur.execute(f"DELETE FROM {t('loudness_tracks')} WHERE catalog_instance_id=%s AND track_id = ANY(%s::text[])",
                    (catalog_instance_id, gone))
        for track_id in gone:
            journal.append("track", track_id, "delete", deletion("track", track_id))
    _refresh_albums(cur, catalog_instance_id, journal, everything=True)
    # Two libraries of the catalogue's tracks and albums (§3.9), not of the records measured so far.
    cur.execute(
        f"""SELECT count(*) + count(DISTINCT m.album_id) FROM {t('catalog_state')} c
              JOIN {t('catalog_tracks')} m
                ON m.catalog_instance_id=c.catalog_instance_id AND m.published_generation=c.published_generation
             WHERE c.catalog_instance_id=%s AND m.available""",
        (catalog_instance_id,),
    )
    journal.retention = max(RETENTION_EVENTS, change_journal_retention_limit(int(cur.fetchone()[0])))
    cur.execute(
        f"UPDATE {t('loudness_stream_state')} SET retention_limit=%s WHERE catalog_instance_id=%s "
        "AND retention_limit IS DISTINCT FROM %s",
        (journal.retention, catalog_instance_id, journal.retention),
    )
    return journal.close()


def rekey(cur, catalog_instance_id, track_mappings):
    """Provider-identity rekey (§2.2): move the track records to their new ids without measuring
    again and rotate the epoch, so clients bootstrap under the new ids. Album records are withdrawn
    with the old epoch and pooled again over the new ids by the next ``reconcile``."""
    # The epoch rotates first: its upsert locks the stream row before any record row, in the order
    # publishers and ``reconcile`` take them, so the two never deadlock.
    rotate_epoch(cur, catalog_instance_id)
    mappings = [row for row in track_mappings if str(row["old_id"]) != str(row["new_id"])]
    if mappings:
        old_ids = [str(row["old_id"]) for row in mappings]
        new_ids = [str(row["new_id"]) for row in mappings]
        # A stale record already under a new id gives way to the one that moves there.
        cur.execute(
            f"DELETE FROM {t('loudness_tracks')} WHERE catalog_instance_id=%s "
            "AND track_id = ANY(%s::text[]) AND NOT track_id = ANY(%s::text[])",
            (catalog_instance_id, new_ids, old_ids),
        )
        cur.execute(
            f"""
            UPDATE {t('loudness_tracks')} target SET track_id=mapped.new_id
              FROM unnest(%s::text[], %s::text[]) AS mapped(old_id, new_id)
             WHERE target.catalog_instance_id=%s AND target.track_id=mapped.old_id
            """,
            (old_ids, new_ids, catalog_instance_id),
        )
    cur.execute(f"DELETE FROM {t('loudness_albums')} WHERE catalog_instance_id=%s", (catalog_instance_id,))


def forget(cur, catalog_instance_id):
    """Drop a source's records under a new epoch; the backfill measures its tracks again (a rekey
    whose own move failed). The caller commits."""
    rotate_epoch(cur, catalog_instance_id)
    for name in ("loudness_tracks", "loudness_albums"):
        cur.execute(f"DELETE FROM {t(name)} WHERE catalog_instance_id=%s", (catalog_instance_id,))


def rotate_epoch(cur, catalog_instance_id):
    """A new epoch with an empty journal: every client's cursor and page token answers 410."""
    epoch = str(uuid.uuid4())
    # One upsert, no read: it runs inside the rekey publication's transaction.
    cur.execute(
        f"INSERT INTO {t('loudness_stream_state')} (catalog_instance_id, epoch) VALUES (%s, %s) "
        "ON CONFLICT (catalog_instance_id) DO UPDATE SET epoch=EXCLUDED.epoch, head_seq=0, floor_seq=0, "
        "updated_at=now()",
        (catalog_instance_id, epoch),
    )
    cur.execute(f"DELETE FROM {t('loudness_changes')} WHERE catalog_instance_id=%s AND epoch<>%s",
                (catalog_instance_id, epoch))
    return epoch


def _limit(value, error):
    if value is None or value == "":
        return DEFAULT_PAGE
    try:
        return max(1, min(int(value), MAX_PAGE))
    except (TypeError, ValueError) as exc:
        raise ValueError(error) from exc


def _encode_token(value):
    return base64.urlsafe_b64encode(_dumps(value).encode("utf-8")).decode("ascii").rstrip("=")


def _decode_token(value):
    try:
        text = str(value)
        token = json.loads(base64.urlsafe_b64decode((text + "=" * (-len(text) % 4)).encode("ascii")).decode("utf-8"))
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ValueError("invalid_loudness_bootstrap") from exc
    if (not isinstance(token, dict) or token.get("phase") not in ("track", "album")
            or not isinstance(token.get("after"), str) or not isinstance(token.get("head_seq"), int)):
        raise ValueError("invalid_loudness_bootstrap")
    return token


def _resolve(db, catalog_instance_id, error):
    """The one source ``catalog_instance_id`` names; missing, unknown or ambiguous is a 400 (``error``),
    never the 410 a ``KeyError`` would answer."""
    if not catalog_instance_id:
        raise ValueError(error)
    try:
        sources = resolve_catalog_source(db, catalog_instance_id=catalog_instance_id)
    except KeyError as exc:
        raise ValueError(error) from exc
    if len(sources) != 1 or sources[0]["catalog_instance_id"] != catalog_instance_id:
        raise ValueError(error)
    return sources[0]["catalog_instance_id"]


def bootstrap_page(db, catalog_instance_id, page_token=None, limit=None):
    """``GET /api/catalog/loudness/bootstrap``: keyset pages of the live records under a pinned head.

    ValueError: 400 ``invalid_loudness_bootstrap``; KeyError: 410 ``bootstrap_required``.
    """
    source = _resolve(db, catalog_instance_id, "invalid_loudness_bootstrap")
    limit = _limit(limit, "invalid_loudness_bootstrap")
    cur = db.cursor()
    try:
        cur.execute(f"SELECT 1 FROM {t('loudness_stream_state')} WHERE catalog_instance_id=%s", (source,))
        if cur.fetchone() is None:
            # A source added since the migration: its epoch must outlive this request.
            _state(cur, source)
            db.commit()
        epoch, head_seq, _floor, _retention = _state(cur, source)
        if page_token:
            token = _decode_token(page_token)
            if token.get("catalog_instance_id") != source or token.get("epoch") != epoch:
                raise KeyError("bootstrap_required")
            pinned, phase, after = token["head_seq"], token["phase"], token["after"]
        else:
            pinned, phase, after = head_seq, "track", ""
        items = []
        if phase == "track":
            cur.execute(
                f"SELECT track_id, media_revision, record FROM {t('loudness_tracks')} "
                "WHERE catalog_instance_id=%s AND track_id > %s ORDER BY track_id LIMIT %s",
                (source, after, limit + 1),
            )
            rows = cur.fetchall()
            items = [track_item(str(r[0]), str(r[1]), _jsonb(r[2])) for r in rows[:limit]]
            if len(rows) > limit:
                next_token = {"phase": "track", "after": items[-1]["track_id"]}
            else:
                phase, after, next_token = "album", "", None
        if phase == "album" and len(items) < limit:
            cur.execute(
                f"SELECT album_id, track_set_hash, record FROM {t('loudness_albums')} "
                "WHERE catalog_instance_id=%s AND album_id > %s ORDER BY album_id LIMIT %s",
                (source, after, limit - len(items) + 1),
            )
            rows = cur.fetchall()
            room = limit - len(items)
            albums = [album_item(str(r[0]), str(r[1]), _jsonb(r[2])) for r in rows[:room]]
            items.extend(albums)
            next_token = {"phase": "album", "after": albums[-1]["album_id"]} if len(rows) > room else None
        elif phase == "album":
            next_token = {"phase": "album", "after": after}
    finally:
        cur.close()
    if next_token is not None:
        next_token = _encode_token({"catalog_instance_id": source, "epoch": epoch, "head_seq": pinned, **next_token})
    return {
        "schema_version": SCHEMA_VERSION,
        "catalog_instance_id": source,
        "items": items,
        "cursor": opaque_cursor(source, epoch, pinned),
        "next_page_token": next_token,
        "has_more": next_token is not None,
    }


def read_changes(db, cursor_value, catalog_instance_id=None, limit=None):
    """``GET /api/catalog/loudness/changes`` (§3.4). ValueError: 400; KeyError: 410."""
    if not cursor_value:
        raise ValueError("cursor_required")
    cursor = parse_opaque_cursor(cursor_value)
    if catalog_instance_id and catalog_instance_id != cursor["catalog_instance_id"]:
        raise ValueError("invalid_cursor")
    limit = _limit(limit, "invalid_cursor")
    source = _resolve(db, cursor["catalog_instance_id"], "invalid_cursor")
    cur = db.cursor()
    try:
        epoch, head_seq, rows, _state_row = read_change_page(
            cur, catalog_instance_id=source, cursor=cursor, limit=limit,
            state_table="loudness_stream_state", epoch_column="epoch", head_column="head_seq",
            floor_column="floor_seq", changes_table="loudness_changes",
            columns=("seq", "payload", "created_at"), ahead_message="Cursor is ahead of the loudness head")
    finally:
        cur.close()
    changes = [{"seq": int(row[0]), "created_at": _iso(row[2]), "item": _jsonb(row[1])} for row in rows]
    next_seq = changes[-1]["seq"] if changes else cursor["seq"]
    return {
        "schema_version": SCHEMA_VERSION,
        "catalog_instance_id": source,
        "changes": changes,
        "cursor": opaque_cursor(source, epoch, next_seq),
        "head_cursor": opaque_cursor(source, epoch, head_seq),
        "has_more": next_seq < head_seq,
    }


def _iso(value):
    if getattr(value, "tzinfo", None) is not None:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z") if hasattr(value, "isoformat") else str(value)
