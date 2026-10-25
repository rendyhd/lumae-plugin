"""Catalog-scoped credits jobs, atomic publications and pinned cursor snapshots."""
from __future__ import annotations

from datetime import datetime, timezone
import base64
import hashlib
import json
import uuid

from plugin.api import table
from . import migrations
from .catalog import (
    canonical_json, change_journal_retention_limit, opaque_cursor, parse_opaque_cursor,
    resolve_catalog_source, _external_ids,
)
from .credits_matching import MATCHING_VERSION

SCHEMA_VERSION = 1
MAX_PAGE = 20
MAX_RECORD_BYTES = 2 * 1024 * 1024
# A cached MusicBrainz response outlives one album's deferrals and retries,
# not the month between rematches: full releases are tens of KB to megabytes.
HTTP_CACHE_TTL_DAYS = 3
HTTP_CACHE_PURGE_BATCH = 500
# A found album is matched again after 30 days. One not found (unresolved) or
# without credits (empty) after 30, then 90, then 180 days as its re-checks of
# the same input go by (scheduler plan S7, AUD-16, lengthened from 7, 30, 90 by
# W4 of the runner plan 2026-10-25): weekly re-checks of every unmatched album
# woke the watchdog to minute cadence once a week.
COMPLETE_TTL_DAYS = 30
RECHECK_TTL_DAYS = (30, 90, 180)
# W4: each expiry lands up to this fraction either side of its TTL, so albums
# first checked together do not come due together (they did, as waves).
EXPIRY_JITTER = 0.25
# W4: background re-checks (an unchanged album's, no app asking) that may start
# in an hour, across catalogues; they also wait until nothing else is ready.
RECHECKS_PER_HOUR = 60
# W4: the re-checks already due when the cap arrives spread over this many days.
RECHECK_SPREAD_DAYS = 14
# A finished walk of the catalogue's albums starts again after an hour, or
# at once when a new catalogue generation was published (S8, AUD-19).
SWEEP_REST_SECONDS = 3600


def migrate(db):
    cur = db.cursor()
    statements = [
        f"""CREATE TABLE IF NOT EXISTS {table('credits_stream')} (
            catalog_instance_id TEXT PRIMARY KEY REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            epoch TEXT NOT NULL, head_seq BIGINT NOT NULL DEFAULT 0, floor_seq BIGINT NOT NULL DEFAULT 0,
            sweep_after TEXT NOT NULL DEFAULT '', last_work_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
        f"""CREATE TABLE IF NOT EXISTS {table('credits_jobs')} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            album_id TEXT NOT NULL, input_fp TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending', requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            not_before TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at TIMESTAMPTZ,
            lease_token TEXT, lease_until TIMESTAMPTZ, attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT, PRIMARY KEY(catalog_instance_id,album_id),
            CHECK(status IN('pending','running','complete','unresolved','empty','failed')))""",
        lambda cur: migrations.ensure_index(cur, f"CREATE INDEX IF NOT EXISTS {table('credits_jobs_ready')} ON {table('credits_jobs')}(not_before,priority DESC,requested_at)"),
        # S7: re-checks of the same input since its last change; S8: when the last walk ended.
        lambda cur: migrations.ensure_columns(cur, table('credits_jobs'), "rechecks INTEGER NOT NULL DEFAULT 0"),
        _migrate_recheck_cap,
        lambda cur: migrations.ensure_index(cur, f"CREATE INDEX IF NOT EXISTS {table('credits_jobs_rechecked')} ON {table('credits_jobs')}(rechecked_at)"),
        lambda cur: migrations.ensure_columns(cur, table('credits_stream'), "swept_at TIMESTAMPTZ",
                                              "swept_generation BIGINT"),
        f"""CREATE TABLE IF NOT EXISTS {table('credits_results')} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            album_id TEXT NOT NULL,input_fp TEXT NOT NULL,seq BIGINT NOT NULL,payload JSONB NOT NULL,
            PRIMARY KEY(catalog_instance_id,album_id))""",
        f"""CREATE TABLE IF NOT EXISTS {table('credits_versions')} (
            catalog_instance_id TEXT NOT NULL REFERENCES {table('catalog_sources')}(catalog_instance_id) ON DELETE CASCADE,
            seq BIGINT NOT NULL,album_id TEXT NOT NULL,payload JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),PRIMARY KEY(catalog_instance_id,seq))""",
        lambda cur: migrations.ensure_index(cur, f"CREATE INDEX IF NOT EXISTS {table('credits_versions_subject')} ON {table('credits_versions')}(catalog_instance_id,album_id,seq DESC)"),
        f"""CREATE TABLE IF NOT EXISTS {table('credits_http_cache')} (
            request_key TEXT PRIMARY KEY,payload JSONB NOT NULL,expires_at TIMESTAMPTZ NOT NULL)""",
        lambda cur: migrations.ensure_index(cur, f"CREATE INDEX IF NOT EXISTS {table('credits_http_cache_expiry')} ON {table('credits_http_cache')}(expires_at)"),
        f"""CREATE TABLE IF NOT EXISTS {table('credits_http_control')} (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),next_request_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
        f"INSERT INTO {table('credits_http_control')}(singleton) VALUES(1) ON CONFLICT DO NOTHING",
    ]
    migrations.apply(cur, statements)
    cur.close()


def _migrate_recheck_cap(cur):
    """W4: ``rechecked_at`` (when a re-check of the job last started, for
    RECHECKS_PER_HOUR). Added once, with a one-off spread over RECHECK_SPREAD_DAYS of
    the re-checks due by then: those waiting to run and those whose result expires."""
    if not migrations.ensure_columns(cur, table('credits_jobs'), "rechecked_at TIMESTAMPTZ"):
        return
    jobs = table('credits_jobs')
    cur.execute(f"""UPDATE {jobs} SET not_before=now()+random()*(%s*interval '1 day')
        WHERE rechecks>0 AND priority=0 AND status IN('pending','failed')""", (RECHECK_SPREAD_DAYS,))
    cur.execute(f"""UPDATE {jobs} SET expires_at=now()+random()*(%s*interval '1 day')
        WHERE status IN('complete','unresolved','empty')
          AND expires_at<now()+(%s*interval '1 day')""", (RECHECK_SPREAD_DAYS, RECHECK_SPREAD_DAYS))


def ready_sql(alias="", rechecks=True):
    """The SQL condition, and its parameters, of a job ready to claim. A background
    re-check (W4: an unchanged album's, no app asking) is ready only when ``rechecks``
    allows it and fewer than RECHECKS_PER_HOUR started in the past hour."""
    a = f"{alias}." if alias else ""
    return (f"""{a}not_before<=now()
          AND ({a}status IN('pending','failed') OR ({a}status='running' AND {a}lease_until<now()))
          AND ({a}rechecks=0 OR {a}priority>0 OR (%s AND (
               SELECT count(*) FROM {table('credits_jobs')} recent
                WHERE recent.rechecked_at>now()-interval '1 hour')<%s))""",
            (bool(rechecks), RECHECKS_PER_HOUR))


def source(db, catalog_id):
    sources = resolve_catalog_source(db, catalog_instance_id=catalog_id)
    if len(sources) != 1 or sources[0]["catalog_instance_id"] != catalog_id:
        raise ValueError("Unknown credits catalog")
    if sources[0].get("rebind_status", "active") != "active":
        raise ValueError("Credits source requires verification")
    return sources[0]


def ensure_stream(cur, catalog_id):
    cur.execute(f"INSERT INTO {table('credits_stream')}(catalog_instance_id,epoch) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                (catalog_id, str(uuid.uuid4())))


def load_album(db, catalog_id, album_id):
    cur = db.cursor()
    cur.execute(f"""SELECT a.name,a.album_artist_display,a.payload,s.published_generation
        FROM {table('catalog_albums')} a JOIN {table('catalog_state')} s
          ON s.catalog_instance_id=a.catalog_instance_id AND s.published_generation=a.published_generation
        WHERE a.catalog_instance_id=%s AND a.album_id=%s AND a.available""", (catalog_id, album_id))
    row = cur.fetchone()
    if not row:
        cur.close()
        return None
    payload = row[2] if isinstance(row[2], dict) else json.loads(row[2])
    external = {**((payload.get("_lumae") or {}).get("external_ids") or {}), **_external_ids(payload, "album")}
    cur.execute(f"""SELECT track_id,title,artist_display,disc_number,track_number,duration_ms,payload,available
        FROM {table('catalog_tracks')} WHERE catalog_instance_id=%s AND published_generation=%s AND album_id=%s
        ORDER BY COALESCE(disc_number,1),track_number,track_id LIMIT 501""", (catalog_id, row[3], album_id))
    rows = cur.fetchall()
    cur.close()
    if not rows or len(rows) > 500 or any(not track[7] for track in rows):
        return None
    tracks = []
    for track in rows:
        raw = track[6] if isinstance(track[6], dict) else json.loads(track[6])
        tracks.append({"id": str(track[0]), "title": track[1], "artist": track[2],
                       "disc_number": track[3], "track_number": track[4], "duration_ms": track[5],
                       "external_ids": {**((raw.get("_lumae") or {}).get("external_ids") or {}), **_external_ids(raw, "track")}})
    album = {"id": album_id, "title": row[0], "artist": row[1], "external_ids": external, "tracks": tracks}
    album["input_fp"] = hashlib.sha256(canonical_json({"matching_version": MATCHING_VERSION, **album}).encode()).hexdigest()
    return album


def _publish(cur, catalog_id, album_id, input_fp, payload):
    if payload is not None and len(canonical_json(payload).encode('utf-8')) > MAX_RECORD_BYTES:
        raise ValueError('Credits album exceeds the bounded publication size')
    ensure_stream(cur, catalog_id)
    cur.execute(f"UPDATE {table('credits_stream')} SET head_seq=head_seq+1,updated_at=now() WHERE catalog_instance_id=%s RETURNING head_seq",
                (catalog_id,))
    seq = int(cur.fetchone()[0])
    cur.execute(f"INSERT INTO {table('credits_versions')}(catalog_instance_id,seq,album_id,payload) VALUES(%s,%s,%s,%s::jsonb)",
                (catalog_id, seq, album_id, canonical_json(payload) if payload is not None else None))
    if payload is None:
        cur.execute(f"DELETE FROM {table('credits_results')} WHERE catalog_instance_id=%s AND album_id=%s",
                    (catalog_id, album_id))
    else:
        cur.execute(f"""INSERT INTO {table('credits_results')}(catalog_instance_id,album_id,input_fp,seq,payload)
            VALUES(%s,%s,%s,%s,%s::jsonb) ON CONFLICT(catalog_instance_id,album_id) DO UPDATE
            SET input_fp=excluded.input_fp,seq=excluded.seq,payload=excluded.payload""",
                    (catalog_id, album_id, input_fp, seq, canonical_json(payload)))


def request_album(db, catalog_id, album_id, priority=0):
    album = load_album(db, catalog_id, album_id)
    if not album:
        cur = db.cursor()
        ensure_stream(cur, catalog_id)
        cur.execute(f"SELECT head_seq FROM {table('credits_stream')} WHERE catalog_instance_id=%s FOR UPDATE", (catalog_id,))
        cur.execute(f"SELECT 1 FROM {table('credits_results')} WHERE catalog_instance_id=%s AND album_id=%s", (catalog_id, album_id))
        if cur.fetchone():
            _publish(cur, catalog_id, album_id, "", None)
        cur.execute(f"DELETE FROM {table('credits_jobs')} WHERE catalog_instance_id=%s AND album_id=%s", (catalog_id, album_id))
        cur.close()
        db.commit()
        return False
    cur = db.cursor()
    ensure_stream(cur, catalog_id)
    # Lock publication state before jobs consistently with finish(); stale catalog
    # matching cannot resurrect a connection after an identity correction.
    cur.execute(f"SELECT head_seq FROM {table('credits_stream')} WHERE catalog_instance_id=%s FOR UPDATE", (catalog_id,))
    cur.execute(f"SELECT input_fp FROM {table('credits_results')} WHERE catalog_instance_id=%s AND album_id=%s", (catalog_id, album_id))
    old = cur.fetchone()
    if old and old[0] != album["input_fp"]:
        _publish(cur, catalog_id, album_id, album["input_fp"], None)
    # Written only when something changes (S8): a sweep visits every album, and an
    # unchanged one used to be rewritten each time. An expired result with the same
    # input is a re-check (S7): its expiry is cleared, so it is counted once.
    jobs = table('credits_jobs')
    cur.execute(f"""INSERT INTO {jobs}(catalog_instance_id,album_id,input_fp,priority)
        VALUES(%s,%s,%s,%s) ON CONFLICT(catalog_instance_id,album_id) DO UPDATE SET
        input_fp=excluded.input_fp,priority=GREATEST({jobs}.priority,excluded.priority),
        status=CASE WHEN {jobs}.input_fp<>excluded.input_fp
            OR {jobs}.expires_at<=now() THEN 'pending' ELSE {jobs}.status END,
        not_before=CASE WHEN {jobs}.input_fp<>excluded.input_fp
            OR {jobs}.expires_at<=now() THEN now() ELSE {jobs}.not_before END,
        expires_at=CASE WHEN {jobs}.input_fp<>excluded.input_fp
            OR {jobs}.expires_at<=now() THEN NULL ELSE {jobs}.expires_at END,
        rechecks=CASE WHEN {jobs}.input_fp<>excluded.input_fp THEN 0
            WHEN {jobs}.expires_at<=now() THEN {jobs}.rechecks+1 ELSE {jobs}.rechecks END,
        lease_token=CASE WHEN {jobs}.input_fp<>excluded.input_fp THEN NULL ELSE {jobs}.lease_token END,
        lease_until=CASE WHEN {jobs}.input_fp<>excluded.input_fp THEN NULL ELSE {jobs}.lease_until END,
        attempts=CASE WHEN {jobs}.input_fp<>excluded.input_fp THEN 0 ELSE {jobs}.attempts END
        WHERE {jobs}.input_fp<>excluded.input_fp OR {jobs}.expires_at<=now()
           OR {jobs}.priority<excluded.priority""",
                (catalog_id, album_id, album["input_fp"], max(0, min(int(priority), 10))))
    cur.close()
    db.commit()
    return True


def sweep(db, catalog_id, limit=32):
    """Request the next ``limit`` albums of the published catalogue; returns how many.

    A walk that reached the end rests SWEEP_REST_SECONDS, or until a new
    catalogue generation is published (S8): an idle library was walked again
    and again, a page every tick.
    """
    source(db, catalog_id)
    cur = db.cursor()
    ensure_stream(cur, catalog_id)
    cur.execute(f"""SELECT st.sweep_after,
            st.sweep_after='' AND st.swept_at > now()-(%s*interval '1 second')
            AND st.swept_generation IS NOT DISTINCT FROM c.published_generation,
            c.published_generation
        FROM {table('credits_stream')} st
        LEFT JOIN {table('catalog_state')} c USING (catalog_instance_id)
        WHERE st.catalog_instance_id=%s""", (SWEEP_REST_SECONDS, catalog_id))
    after, resting, generation = cur.fetchone()
    if resting:
        cur.close()
        db.commit()
        return 0
    cur.execute(f"""SELECT a.album_id FROM {table('catalog_albums')} a JOIN {table('catalog_state')} s
        ON s.catalog_instance_id=a.catalog_instance_id AND s.published_generation=a.published_generation
        WHERE a.catalog_instance_id=%s AND a.available AND a.album_id>%s ORDER BY a.album_id LIMIT %s""",
                (catalog_id, after, limit))
    ids = [str(row[0]) for row in cur.fetchall()]
    cur.close()
    db.commit()
    for album_id in ids:
        request_album(db, catalog_id, album_id)
    cur = db.cursor()
    wrapped = len(ids) < limit
    cur.execute(f"""UPDATE {table('credits_stream')} SET sweep_after=%s,
            swept_at=CASE WHEN %s THEN now() ELSE swept_at END,
            swept_generation=CASE WHEN %s THEN %s ELSE swept_generation END
        WHERE catalog_instance_id=%s""",
                ("" if wrapped else ids[-1], wrapped, wrapped, generation, catalog_id))
    # Publish bounded tombstones as unavailable tracks/albums disappear.
    cur.execute(f"""SELECT r.album_id FROM {table('credits_results')} r
        WHERE r.catalog_instance_id=%s AND NOT EXISTS(
          SELECT 1 FROM {table('catalog_albums')} a JOIN {table('catalog_state')} s
          ON s.catalog_instance_id=a.catalog_instance_id AND s.published_generation=a.published_generation
          WHERE a.catalog_instance_id=r.catalog_instance_id AND a.album_id=r.album_id AND a.available)
        ORDER BY r.album_id LIMIT 32""", (catalog_id,))
    for row in cur.fetchall():
        _publish(cur, catalog_id, row[0], "", None)
        cur.execute(f"DELETE FROM {table('credits_jobs')} WHERE catalog_instance_id=%s AND album_id=%s", (catalog_id, row[0]))
    cur.close()
    db.commit()
    return len(ids)


def claim(db, catalog_id, rechecks=True):
    """The next ready job, leased; background re-checks only when ``rechecks`` (W4)."""
    token = str(uuid.uuid4())
    ready, params = ready_sql(rechecks=rechecks)
    cur = db.cursor()
    cur.execute(f"""WITH candidate AS (
        SELECT catalog_instance_id,album_id FROM {table('credits_jobs')}
        WHERE catalog_instance_id=%s AND {ready}
        ORDER BY priority DESC,rechecks=0 DESC,requested_at,album_id FOR UPDATE SKIP LOCKED LIMIT 1)
        UPDATE {table('credits_jobs')} j SET status='running',lease_token=%s,
        lease_until=now()+interval '20 minutes',attempts=attempts+1,last_error=NULL,
        rechecked_at=CASE WHEN j.rechecks>0 THEN now() ELSE j.rechecked_at END
        FROM candidate c WHERE j.catalog_instance_id=c.catalog_instance_id AND j.album_id=c.album_id
        RETURNING j.album_id,j.input_fp,j.attempts""", (catalog_id, *params, token))
    row = cur.fetchone()
    cur.close()
    db.commit()
    return {"catalog_id": catalog_id, "album_id": row[0], "input_fp": row[1], "attempt": row[2], "token": token} if row else None


def lease_current(db, job):
    cur = db.cursor()
    cur.execute(f"""SELECT 1 FROM {table('credits_jobs')} WHERE catalog_instance_id=%s AND album_id=%s
        AND lease_token=%s AND status='running' AND input_fp=%s AND lease_until>now()""",
                (job["catalog_id"], job["album_id"], job["token"], job["input_fp"]))
    result = cur.fetchone() is not None
    cur.close()
    db.commit()
    return result


def finish(db, job, payload=None, error=None, retry_seconds=60):
    cur = db.cursor()
    ensure_stream(cur, job["catalog_id"])
    cur.execute(f"SELECT head_seq FROM {table('credits_stream')} WHERE catalog_instance_id=%s FOR UPDATE", (job["catalog_id"],))
    cur.execute(f"""SELECT input_fp, rechecks FROM {table('credits_jobs')} WHERE catalog_instance_id=%s AND album_id=%s
        AND lease_token=%s AND status='running' AND lease_until>now() FOR UPDATE""",
                (job["catalog_id"], job["album_id"], job["token"]))
    row = cur.fetchone()
    if not row or row[0] != job["input_fp"]:
        cur.close()
        db.rollback()
        return False
    # The before-background share is measured from the end of a turn (AUD-16):
    # from the claim, a long album was due again at once.
    cur.execute(f"UPDATE {table('credits_stream')} SET last_work_at=now() WHERE catalog_instance_id=%s",
                (job["catalog_id"],))
    if error:
        cur.execute(f"""UPDATE {table('credits_jobs')} SET status='failed',lease_token=NULL,lease_until=NULL,
            last_error=%s,not_before=now()+(%s*interval '1 second')
            WHERE catalog_instance_id=%s AND album_id=%s""",
                    (str(error)[:500], max(1, min(21600, int(retry_seconds))), job["catalog_id"], job["album_id"]))
    else:
        current = load_album(db, job["catalog_id"], job["album_id"])
        if not current or current["input_fp"] != job["input_fp"]:
            cur.close()
            db.rollback()
            return False
        _publish(cur, job["catalog_id"], job["album_id"], job["input_fp"], payload)
        status = "unresolved" if payload["match_status"] == "unresolved" else (
            "complete" if any(subject["credits"] for subject in payload["subjects"]) else "empty")
        rechecks = int(row[1] or 0)
        ttl_days = (COMPLETE_TTL_DAYS if status == "complete"
                    else RECHECK_TTL_DAYS[min(rechecks, len(RECHECK_TTL_DAYS) - 1)])
        cur.execute(f"""UPDATE {table('credits_jobs')} SET status=%s,lease_token=NULL,lease_until=NULL,
            priority=0,last_error=NULL,
            expires_at=now()+(%s*(1+%s*(2*random()-1))*interval '1 day')
            WHERE catalog_instance_id=%s AND album_id=%s""",
                    (status, ttl_days, EXPIRY_JITTER, job["catalog_id"], job["album_id"]))
    cur.close()
    db.commit()
    return True


def status(db, catalog_id):
    source(db, catalog_id)
    cur = db.cursor()
    ensure_stream(cur, catalog_id)
    cur.execute(f"SELECT epoch,head_seq,floor_seq FROM {table('credits_stream')} WHERE catalog_instance_id=%s", (catalog_id,))
    epoch, head, floor = cur.fetchone()
    cur.execute(f"SELECT status,count(*) FROM {table('credits_jobs')} WHERE catalog_instance_id=%s GROUP BY status", (catalog_id,))
    counts = {row[0]: int(row[1]) for row in cur.fetchall()}
    cur.close()
    db.commit()
    return {"schema_version": SCHEMA_VERSION, "matching_version": MATCHING_VERSION,
            "catalog_instance_id": catalog_id, "cursor": opaque_cursor(catalog_id, epoch, int(head)),
            "floor_seq": int(floor), "counts": counts}


def bootstrap(db, catalog_id, page_token=None, limit=100):
    state = status(db, catalog_id)
    current = parse_opaque_cursor(state["cursor"])
    token = json.loads(base64.urlsafe_b64decode(page_token + "=" * (-len(page_token) % 4))) if page_token else None
    now = datetime.now(timezone.utc).timestamp()
    if token is not None and not isinstance(token, dict):
        raise ValueError('Invalid credits page token')
    if token and (token.get("catalog_id") != catalog_id or token.get("epoch") != current["epoch"]
                  or float(token.get("expires", 0)) < now or int(token.get("head", -1)) < state["floor_seq"]
                  or int(token.get("head", -1)) > current["seq"]):
        raise KeyError("bootstrap_required")
    head = int(token["head"]) if token else current["seq"]
    after = str(token.get("after", "")) if token else ""
    limit = max(1, min(MAX_PAGE, int(limit)))
    cur = db.cursor()
    cur.execute(f"""SELECT album_id,payload FROM (
        SELECT DISTINCT ON(album_id) album_id,payload FROM {table('credits_versions')}
        WHERE catalog_instance_id=%s AND seq<=%s AND album_id>%s ORDER BY album_id,seq DESC
        ) latest WHERE payload IS NOT NULL ORDER BY album_id LIMIT %s""", (catalog_id, head, after, limit + 1))
    rows = cur.fetchall()
    cur.close()
    db.commit()
    verified = status(db, catalog_id)
    if parse_opaque_cursor(verified["cursor"])["epoch"] != current["epoch"] or verified["floor_seq"] > head:
        raise KeyError("bootstrap_required")
    more = len(rows) > limit
    rows = rows[:limit]
    next_token = None
    if more:
        data = {"catalog_id": catalog_id, "epoch": current["epoch"], "head": head,
                "after": str(rows[-1][0]), "expires": token["expires"] if token else now + 3600}
        next_token = base64.urlsafe_b64encode(canonical_json(data).encode()).decode().rstrip("=")
    return {**state, "records": [row[1] for row in rows], "has_more": more,
            "next_page_token": next_token, "cursor": opaque_cursor(catalog_id, current["epoch"], head)}


def changes(db, catalog_id, cursor_value, limit=250):
    state = status(db, catalog_id)
    cursor, head = parse_opaque_cursor(cursor_value), parse_opaque_cursor(state["cursor"])
    if cursor["catalog_instance_id"] != catalog_id:
        raise ValueError("Credits cursor belongs to another source")
    if cursor["epoch"] != head["epoch"] or cursor["seq"] < state["floor_seq"]:
        raise KeyError("bootstrap_required")
    if cursor["seq"] > head["seq"]:
        raise ValueError("Credits cursor is ahead of publication")
    cur = db.cursor()
    cur.execute(f"""SELECT seq,album_id,payload FROM {table('credits_versions')}
        WHERE catalog_instance_id=%s AND seq>%s AND seq<=%s ORDER BY seq LIMIT %s""",
                (catalog_id, cursor["seq"], head["seq"], max(1, min(MAX_PAGE, int(limit)))))
    rows = cur.fetchall()
    cur.close()
    db.commit()
    verified = status(db, catalog_id)
    if parse_opaque_cursor(verified["cursor"])["epoch"] != head["epoch"] or verified["floor_seq"] > cursor["seq"]:
        raise KeyError("bootstrap_required")
    next_seq = int(rows[-1][0]) if rows else cursor["seq"]
    return {**state, "changes": [{"seq": int(row[0]), "album_id": row[1],
            "operation": "delete" if row[2] is None else "upsert", "record": row[2]} for row in rows],
            "cursor": opaque_cursor(catalog_id, head["epoch"], next_seq), "has_more": next_seq < head["seq"]}


def purge_http_cache(cur, batch=HTTP_CACHE_PURGE_BATCH):
    """Delete up to ``batch`` cached MusicBrainz responses that expired, or that
    an older version cached for longer than HTTP_CACHE_TTL_DAYS (30 days up to
    1.6.0). Returns the number deleted; a response deleted early is refetched."""
    # clock_timestamp: a row a concurrent request cached after this
    # transaction began never looks longer-lived than the TTL.
    cur.execute(f"""DELETE FROM {table('credits_http_cache')} WHERE request_key IN(
        SELECT request_key FROM {table('credits_http_cache')}
        WHERE expires_at<now() OR expires_at>clock_timestamp()+(%s*interval '1 day')
        LIMIT %s FOR UPDATE SKIP LOCKED)""", (HTTP_CACHE_TTL_DAYS, max(1, int(batch))))
    return cur.rowcount


def purge_http_cache_fully(db, batch=HTTP_CACHE_PURGE_BATCH):
    """purge_http_cache until nothing is left, one committed batch at a time."""
    cur = db.cursor()
    try:
        while True:
            deleted = purge_http_cache(cur, batch)
            db.commit()
            if deleted < batch:
                return
    finally:
        cur.close()


def compact(db, catalog_id, retention=None):
    cur = db.cursor()
    if retention is None:
        # Two logical snapshots of the catalogue's albums, as the other journals keep.
        cur.execute(f"""SELECT COALESCE((entity_counts->>'album')::bigint,0) FROM {table('catalog_state')}
            WHERE catalog_instance_id=%s""", (catalog_id,))
        row = cur.fetchone()
        retention = change_journal_retention_limit(row[0] if row else 0)
    cur.execute(f"""UPDATE {table('credits_stream')} SET floor_seq=GREATEST(floor_seq,head_seq-%s)
        WHERE catalog_instance_id=%s RETURNING floor_seq""", (max(1000, int(retention)), catalog_id))
    row = cur.fetchone()
    if row:
        # Retain one baseline version per subject so pinned snapshots remain complete.
        cur.execute(f"""DELETE FROM {table('credits_versions')} v WHERE catalog_instance_id=%s AND seq<%s
            AND (payload IS NULL OR seq NOT IN(SELECT MAX(seq) FROM {table('credits_versions')}
              WHERE catalog_instance_id=%s AND seq<=%s GROUP BY album_id))""", (catalog_id, row[0], catalog_id, row[0]))
    purge_http_cache(cur)
    cur.close()
    db.commit()
