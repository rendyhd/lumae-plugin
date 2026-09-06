"""MusicBrainz-only client. One database-coordinated request at a time across workers."""
from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import time
from urllib.parse import urljoin, urlparse

import requests
from plugin.api import table

from .catalog import canonical_json
from .credits_matching import (
    choose_release, identity_values, mbid, verify_recording,
    name_score, artist_name, SUPPORTED_ROLES,
)

USER_AGENT = "Lumae-Analysis-Credits/1 (+https://github.com/rendyhd/lumae-plugin)"
BASE_URL = "https://musicbrainz.org/ws/2/"
LOCK_ID = 1280658765
RELEASE_INC = "artist-credits+recordings+release-groups+artist-rels+recording-level-rels+work-rels+work-level-rels"
RECORDING_INC = "artist-credits+artist-rels+work-rels+work-level-rels"


class MusicBrainzDeferred(RuntimeError):
    def __init__(self, reason, retry_seconds=60):
        super().__init__(reason)
        self.retry_seconds = retry_seconds


def retry_after(value, fallback=60):
    try:
        return max(1, min(21600, int(value)))
    except (TypeError, ValueError):
        try:
            return max(1, min(21600, int((parsedate_to_datetime(value) -
                                       datetime.now(timezone.utc)).total_seconds())))
        except (TypeError, ValueError, OverflowError):
            return fallback


def has_credit_relationships(entity):
    for relation in entity.get("relations") or []:
        if relation.get("type") in SUPPORTED_ROLES and mbid((relation.get("artist") or {}).get("id")):
            return True
        if relation.get("work") and has_credit_relationships(relation["work"]):
            return True
    return any(has_credit_relationships(track.get("recording") or {})
               for medium in entity.get("media") or [] for track in medium.get("tracks") or [])


class Client:
    def __init__(self, db, allowed=lambda: True, http=None, sleep=time.sleep):
        self.db, self.allowed = db, allowed
        self.http, self.sleep = http or requests.Session(), sleep
        self.requests = 0

    def get(self, entity, entity_id=None, **params):
        visited = set()
        for _ in range(5):
            result = self._get(entity, entity_id, **params)
            redirected = result.get("redirect_mbid")
            if not redirected:
                return result
            if redirected in visited:
                raise MusicBrainzDeferred("MusicBrainz identity redirect cycle", 3600)
            visited.add(redirected)
            entity_id = redirected
        raise MusicBrainzDeferred("MusicBrainz identity redirect limit", 3600)

    def _get(self, entity, entity_id=None, **params):
        if entity not in {"release", "release-group", "recording", "work", "artist"}:
            raise ValueError("Unsupported MusicBrainz entity")
        if entity_id and not mbid(entity_id):
            raise ValueError("Invalid MusicBrainz identity")
        if not self.allowed():
            raise MusicBrainzDeferred("Credits work paused or playback work is waiting", 60)
        path = entity + ("/" + mbid(entity_id) if entity_id else "")
        params = {**params, "fmt": "json"}
        key = hashlib.sha256(json.dumps({"entity_path": path, "params": params}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        cur = self.db.cursor()
        cur.execute(f"SELECT payload FROM {table('credits_http_cache')} WHERE request_key=%s AND expires_at>now()", (key,))
        cached = cur.fetchone()
        cur.close()
        self.db.commit()
        if cached:
            return cached[0] if isinstance(cached[0], dict) else json.loads(cached[0])
        if self.requests >= 160:
            raise MusicBrainzDeferred("Bounded credits request batch completed", 60)
        cur = self.db.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,))
        acquired = cur.fetchone()[0]
        self.db.commit()
        if not acquired:
            cur.close()
            raise MusicBrainzDeferred("Another credits worker owns the MusicBrainz request slot", 2)
        try:
            # Session advisory lock survives commits and is released on worker loss.
            # Keep it through the request: delayed workers cannot bunch reserved slots.
            cur.execute(f"SELECT GREATEST(0,EXTRACT(EPOCH FROM(next_request_at-clock_timestamp()))) FROM {table('credits_http_control')} WHERE singleton=1")
            wait = float(cur.fetchone()[0])
            self.db.commit()
            if wait > 1.1:
                raise MusicBrainzDeferred("MusicBrainz backoff is active", int(wait) + 1)
            if wait > 0:
                self.sleep(wait)
            if not self.allowed():
                raise MusicBrainzDeferred("Credits work paused or superseded", 60)
            cur.execute(f"UPDATE {table('credits_http_control')} SET next_request_at=clock_timestamp()+interval '1 second' WHERE singleton=1")
            self.db.commit()
            self.requests += 1
            try:
                response = self.http.get(BASE_URL + path, params=params,
                    headers={"User-Agent": USER_AGENT, "Accept": "application/json"}, timeout=(5, 20),
                    allow_redirects=False)
            except requests.RequestException as exc:
                raise MusicBrainzDeferred("MusicBrainz service unavailable", 60) from exc
            if response.status_code in (429, 503) or response.status_code >= 500:
                delay = retry_after(response.headers.get("Retry-After"), 60)
                cur.execute(f"UPDATE {table('credits_http_control')} SET next_request_at=clock_timestamp()+(%s*interval '1 second') WHERE singleton=1", (delay,))
                self.db.commit()
                raise MusicBrainzDeferred("MusicBrainz requested backoff", delay)
            if response.status_code in (301, 308):
                target = urlparse(urljoin(BASE_URL + path, response.headers.get("Location", "")))
                parts = target.path.strip("/").split("/")
                if (not entity_id or target.scheme != "https" or target.netloc != "musicbrainz.org"
                        or len(parts) != 4 or parts[:3] != ["ws", "2", entity] or not mbid(parts[3])):
                    raise MusicBrainzDeferred("Unverified MusicBrainz identity redirect", 3600)
                payload = {"redirect_mbid": mbid(parts[3])}
            elif response.status_code == 404:
                payload = {}
            elif response.status_code != 200:
                raise MusicBrainzDeferred(f"MusicBrainz response {response.status_code}", 3600)
            else:
                if len(response.content) > 8 * 1024 * 1024:
                    raise MusicBrainzDeferred("MusicBrainz response exceeded bounded size", 3600)
                payload = response.json()
                if not isinstance(payload, dict):
                    raise MusicBrainzDeferred("MusicBrainz response was not an object", 3600)
            empty = not payload or (not entity_id and not payload.get(entity + "s"))
            if entity_id and "artist-rels" in params.get("inc", "") and not has_credit_relationships(payload):
                empty = True
            cur.execute(f"""INSERT INTO {table('credits_http_cache')}(request_key,payload,expires_at)
                VALUES(%s,%s::jsonb,now()+(%s*interval '1 day')) ON CONFLICT(request_key) DO UPDATE
                SET payload=excluded.payload,expires_at=excluded.expires_at""",
                        (key, canonical_json(payload), 7 if empty else 30))
            self.db.commit()
            return payload
        finally:
            self.db.rollback()
            cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))
            cur.close()
            self.db.commit()


def _quoted(value):
    return '"' + str(value or "").replace("\\", "\\\\").replace('"', '\\"') + '"'


def _ids(album, key):
    values = identity_values((album.get("external_ids") or {}).get(key))
    if not values:
        values = list(dict.fromkeys(value for track in album["tracks"]
            for value in identity_values((track.get("external_ids") or {}).get(key))))
    return values


def resolve(album, client):
    release_ids = _ids(album, "musicbrainz_release_id")
    group_ids = _ids(album, "musicbrainz_release_group_id")
    legacy = identity_values((album.get("external_ids") or {}).get("musicbrainz"))
    releases, verified_release = [], None
    # Conflicting album identifiers are ambiguous, never arbitrarily take the first.
    for value in list(dict.fromkeys(release_ids + legacy))[:4]:
        candidate = client.get("release", value, inc=RELEASE_INC)
        if candidate:
            releases.append(candidate)
            if len(release_ids or legacy) == 1:
                verified_release = mbid(candidate.get("id"))
        elif value in legacy:
            group = client.get("release-group", value, inc="artist-credits")
            if name_score(album["title"], group.get("title")) >= 0.94 and name_score(
                    album["artist"], artist_name(group)) >= 0.90:
                group_ids.append(value)
    if verified_release:
        exact = choose_release(album, releases, verified_release_id=verified_release)
        if exact["status"] == "matched":
            return exact

    if len(set(group_ids)) == 1:
        result = client.get("release", **{"release-group": group_ids[0], "limit": 12})
    else:
        result = client.get("release", query=f"release:{_quoted(album['title'])} AND artist:{_quoted(album['artist'])}", limit=12)
    rows = result.get("releases") or []
    for item in rows[:12]:
        value = mbid(item.get("id"))
        if value and all(release.get("id") != value for release in releases):
            candidate = client.get("release", value, inc=RELEASE_INC)
            if candidate:
                releases.append(candidate)
    complete = int(result.get("release-count", result.get("count", len(rows)))) <= len(rows)
    match = choose_release(album, releases, candidates_complete=complete)
    # Verified recording IDs can survive sparse/missing album tags or edition ambiguity.
    for track in album["tracks"]:
        values = identity_values((track.get("external_ids") or {}).get("musicbrainz_recording_id"))
        if not values:
            values = identity_values((track.get("external_ids") or {}).get("musicbrainz"))
        if len(values) != 1:
            continue
        recording = client.get("recording", values[0], inc=RECORDING_INC)
        if verify_recording(track, recording, trusted_id=True):
            match["recordings"][track["id"]] = recording
            match.setdefault("recording_evidence", {})[track["id"]] = {
                "matching_version": 1, "method": "verified_recording_id_and_metadata",
                "requested_recording_mbid": values[0], "resolved_recording_mbid": recording["id"],
            }
    if match["recordings"] and match["status"] == "unresolved":
        match["status"] = "recordings_only"
    return match
