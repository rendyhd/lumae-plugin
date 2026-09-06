"""Conservative MusicBrainz identity verification and relationship projection.

Pure matching: no network, user ratings, listening history, provider mutation or
name-based person joins. Release, recording and work scope always stay distinct.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from difflib import SequenceMatcher
from uuid import UUID

MATCHING_VERSION = 1
SUPPORTED_ROLES = {
    "instrument": "performer", "vocal": "vocal", "performer": "performer",
    "producer": "producer", "mix": "mixer", "mixing": "mixer",
    "recording": "recording_engineer", "engineer": "engineer",
    "composer": "composer", "lyricist": "lyricist", "writer": "writer",
}


def mbid(value):
    try:
        return str(UUID(str(value).strip()))
    except (ValueError, TypeError, AttributeError):
        return None


def identity_values(value):
    values = value if isinstance(value, (list, tuple)) else [value]
    return list(dict.fromkeys(valid for item in values if (valid := mbid(item))))


def normalized(value):
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    return " ".join(re.sub(r"[^\w]+", " ", "".join(
        c for c in text if not unicodedata.combining(c))).split())


def name_score(left, right):
    a, b = normalized(left), normalized(right)
    return SequenceMatcher(None, a, b, autojunk=False).ratio() if a and b else 0.0


def artist_name(entity):
    return "".join(str(c.get("name") or (c.get("artist") or {}).get("name") or "") +
                   str(c.get("joinphrase") or "") for c in entity.get("artist-credit", [])
                   if isinstance(c, dict))


def artist_ids_match(local, external):
    expected = set(identity_values((local.get("external_ids") or {}).get("musicbrainz_artist_id")))
    actual = {mbid((credit.get("artist") or {}).get("id")) for credit in external.get("artist-credit") or []
              if isinstance(credit, dict)}
    return not expected or expected <= actual


def duration_matches(local_ms, external_ms):
    if not local_ms or not external_ms:
        return None
    try:
        return abs(float(local_ms) - float(external_ms)) <= max(3000, float(local_ms) * 0.02)
    except (ValueError, TypeError):
        return False


def release_tracks(release):
    tracks = []
    for medium in release.get("media") or []:
        for track in medium.get("tracks") or []:
            recording = track.get("recording") or {}
            tracks.append({
                "disc": medium.get("position"),
                "position": track.get("position"),
                "title": track.get("title") or recording.get("title"),
                "length": track.get("length") or recording.get("length"),
                "recording": recording,
            })
    return tracks


def verify_recording(local, recording, trusted_id=False):
    if not mbid(recording.get("id")) or name_score(local.get("title"), recording.get("title")) < 0.94:
        return False
    artist = local.get("artist")
    if not artist or name_score(artist, artist_name(recording)) < 0.90 or not artist_ids_match(local, recording):
        return False
    duration = duration_matches(local.get("duration_ms"), recording.get("length"))
    return duration is not False and (trusted_id or duration is True)


def verify_release(album, release, trusted_id=False):
    """A search score alone is never an accepted match, including for tagged IDs."""
    if not mbid(release.get("id")):
        return None
    if name_score(album.get("title"), release.get("title")) < 0.94:
        return None
    if name_score(album.get("artist"), artist_name(release)) < 0.90 or not artist_ids_match(album, release):
        return None
    local = album.get("tracks") or []
    external = release_tracks(release)
    if not local or len(local) != len(external):
        return None
    # Compare the ordered disc layout, title and measured duration of every track.
    durations = 0
    recordings = {}
    for left, right in zip(local, external):
        if name_score(left.get("title"), right.get("title")) < 0.94:
            return None
        if left.get("disc_number") is not None and int(left["disc_number"]) != int(right["disc"] or 1):
            return None
        if left.get("track_number") is not None and int(left["track_number"]) != int(right["position"] or 0):
            return None
        duration = duration_matches(left.get("duration_ms"), right.get("length"))
        if duration is False:
            return None
        durations += int(duration is True)
        recording = right["recording"]
        if not mbid(recording.get("id")):
            return None
        # Track-level credits make compilations safe; never substitute album artist.
        if left.get("artist") and name_score(left["artist"], artist_name(recording)) < 0.90:
            return None
        if not artist_ids_match(left, recording):
            return None
        recordings[str(left["id"])] = recording
    if not trusted_id and (len(local) < 2 or durations < max(2, len(local) * 0.7)):
        return None
    return {
        "release_id": mbid(release["id"]),
        "release_group_id": mbid((release.get("release-group") or {}).get("id")),
        "recordings": recordings,
        "evidence": {"matching_version": MATCHING_VERSION,
                     "method": "verified_id_and_metadata" if trusted_id else "ordered_tracklist",
                     "compared_tracks": len(local), "matching_durations": durations},
    }


def choose_release(album, releases, verified_release_id=None, candidates_complete=True):
    """Ambiguous editions may share proven recordings, never release personnel."""
    matches = []
    for release in releases:
        match = verify_release(album, release, trusted_id=release.get("id") == verified_release_id)
        if match:
            matches.append((release, match))
    explicit = [pair for pair in matches if pair[1]["release_id"] == verified_release_id]
    if len(explicit) == 1:
        return {"release": explicit[0][0], **explicit[0][1], "status": "matched"}
    if len(matches) == 1 and candidates_complete:
        return {"release": matches[0][0], **matches[0][1], "status": "matched"}
    recordings = {}
    if matches:
        for track_id in matches[0][1]["recordings"]:
            values = [pair[1]["recordings"][track_id] for pair in matches]
            if len({recording["id"] for recording in values}) == 1:
                recordings[track_id] = values[0]
    # Even shared IDs must come from fully corroborated editions. A truncated
    # search cannot prove edition uniqueness, but these actual recordings remain verified.
    return {"status": "recordings_only" if recordings else "unresolved",
            "release": None, "release_id": None, "release_group_id": None,
            "recordings": recordings,
            "evidence": {"matching_version": MATCHING_VERSION, "method": "corroborated_recordings",
                         "edition_candidates": len(matches), "candidate_set_complete": candidates_complete}}


def relationship_credits(entity, scope, source_entity_type, source_id, matching):
    """Preserve exact people, relationship IDs, roles, instruments and source scope."""
    credits = []
    source_id = mbid(source_id)
    if not source_id:
        return credits
    for relation in entity.get("relations") or []:
        relationship_type = str(relation.get("type") or "")
        role = SUPPORTED_ROLES.get(relationship_type)
        person = relation.get("artist") or {}
        person_id = mbid(person.get("id"))
        if not role or not person_id or not person.get("name"):
            continue
        if relation.get("target-type") not in (None, "artist"):
            continue
        # Only work relations describe work authorship; do not expand it to
        # recording/release participation or invent a person from a name.
        credit = {
            "person_mbid": person_id, "person_name": str(person["name"]),
            "role": role, "relationship_type": relationship_type,
            "relationship_type_id": mbid(relation.get("type-id")),
            "instruments": [str(value) for value in relation.get("attributes") or []],
            "attribute_ids": relation.get("attribute-ids") or {},
            "scope": scope, "source": "musicbrainz",
            "source_entity_type": source_entity_type, "source_entity_id": source_id,
            "source_url": f"https://musicbrainz.org/{source_entity_type}/{source_id}",
            "matching": matching,
        }
        credit["id"] = hashlib.sha256(json.dumps(credit, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        credits.append(credit)
    return credits


def extract_credits(match):
    """Result keys are provider subject identities, with release/work scopes intact."""
    album_credits = []
    track_credits = {}
    if match.get("release"):
        album_credits = relationship_credits(match["release"], "release", "release",
                                            match["release_id"], match["evidence"])
    for track_id, recording in (match.get("recordings") or {}).items():
        evidence = (match.get("recording_evidence") or {}).get(track_id, match["evidence"])
        credits = relationship_credits(recording, "recording", "recording",
                                       recording["id"], evidence)
        for relation in recording.get("relations") or []:
            work = relation.get("work") or {}
            if relation.get("type") == "performance" and mbid(work.get("id")):
                credits.extend(relationship_credits(work, "work", "work", work["id"], evidence))
        track_credits[track_id] = credits
    return {"album": album_credits, "tracks": track_credits}
