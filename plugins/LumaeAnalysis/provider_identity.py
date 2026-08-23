"""Pure Navidrome identity-transition primitives.

The codec mirrors Navidrome PR #5824's ``canonicalID`` migration function.
Keep this module free of Flask and database imports so the plugin and its
transition tests can use it before any runtime integration is available.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple


PROVIDER_IDENTITY_REKEY_CONTRACT = "provider_identity_rekey_v1"
LAST_KNOWN_PRE_CANONICAL_NAVIDROME_VERSION = "0.63.2"
FIRST_CANONICAL_NAVIDROME_VERSION = None
MINIMUM_AUDIOMUSE_MIGRATION_VERSION = "3.1.1"
NAVIDROME_BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

_BASE = len(NAVIDROME_BASE62_ALPHABET)
_MAX_128_BIT_VALUE = (1 << 128) - 1
_RELEASE_VERSION_RE = re.compile(
    r"^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\s+\(([0-9a-fA-F]{7,40})\))?$"
)
_BRANCH_VERSION_RE = re.compile(
    r"^(master|main|dev)(?:\s+\(([0-9a-fA-F]{7,40})\))?$", re.IGNORECASE
)
_HEX_32_RE = re.compile(r"^[0-9a-fA-F]{32}$")
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


class ProviderIdentityTransitionState(str, Enum):
    NORMAL = "normal"
    TRANSITION_PENDING = "transition_pending"
    APPLIED = "applied"
    BLOCKED = "blocked"


class AudioMuseIdentityHealth(str, Enum):
    READY = "ready"
    MIGRATION_REQUIRED = "migration_required"
    BUSY = "busy"
    REPAIR_REQUIRED = "repair_required"


@dataclass(frozen=True)
class CanonicalIdResult:
    value: str
    recognized: bool
    changed: bool
    shape: str


@dataclass(frozen=True)
class ParsedNavidromeVersion:
    raw: str
    kind: str
    semantic_version: Optional[Tuple[int, int, int]]
    prerelease: Optional[str]
    commit_sha: Optional[str]
    branch: Optional[str]


def _decode_base62(value: str) -> Optional[int]:
    decoded = 0
    for character in value:
        index = NAVIDROME_BASE62_ALPHABET.find(character)
        if index < 0:
            return None
        decoded = decoded * _BASE + index
    return decoded


def _encode_base62(value: int) -> str:
    if value < 0 or value > _MAX_128_BIT_VALUE:
        raise ValueError("Navidrome canonical IDs require an unsigned 128-bit value")
    encoded = ""
    while value:
        value, remainder = divmod(value, _BASE)
        encoded = NAVIDROME_BASE62_ALPHABET[remainder] + encoded
    return (encoded or "0").rjust(22, "0")


def _result(original: str, value: str, shape: str, recognized: bool) -> CanonicalIdResult:
    return CanonicalIdResult(
        value=value,
        shape=shape,
        recognized=recognized,
        changed=value != original,
    )


def canonicalize_navidrome_id(value: str) -> CanonicalIdResult:
    """Apply Navidrome's deterministic, idempotent canonical-ID transform."""

    if len(value) == 22:
        decoded = _decode_base62(value)
        if decoded is None:
            return _result(value, value, "unrecognized", False)
        if decoded <= _MAX_128_BIT_VALUE:
            return _result(value, value, "base62_128", True)
        digest = hashlib.md5(value.encode("utf-8")).digest()  # noqa: S324 - upstream ID codec
        return _result(
            value,
            _encode_base62(int.from_bytes(digest, byteorder="big", signed=False)),
            "base62_overflow",
            True,
        )

    if _HEX_32_RE.fullmatch(value):
        return _result(value, _encode_base62(int(value, 16)), "hex32", True)

    if _UUID_RE.fullmatch(value):
        return _result(value, _encode_base62(int(value.replace("-", ""), 16)), "uuid", True)

    return _result(value, value, "unrecognized", False)


def parse_navidrome_server_version(raw_value: object) -> ParsedNavidromeVersion:
    raw = str(raw_value or "").strip()
    release = _RELEASE_VERSION_RE.fullmatch(raw)
    if release:
        prerelease = release.group(4)
        return ParsedNavidromeVersion(
            raw=raw,
            kind="prerelease" if prerelease else "release",
            semantic_version=tuple(int(release.group(index)) for index in (1, 2, 3)),
            prerelease=prerelease,
            commit_sha=release.group(5).lower() if release.group(5) else None,
            branch=None,
        )

    branch = _BRANCH_VERSION_RE.fullmatch(raw)
    if branch:
        return ParsedNavidromeVersion(
            raw=raw,
            kind="branch",
            semantic_version=None,
            prerelease=None,
            commit_sha=branch.group(2).lower() if branch.group(2) else None,
            branch=branch.group(1).lower(),
        )

    return ParsedNavidromeVersion(
        raw=raw,
        kind="unknown",
        semantic_version=None,
        prerelease=None,
        commit_sha=None,
        branch=None,
    )


def is_after_last_known_pre_canonical_version(raw_value: object) -> Optional[bool]:
    parsed = parse_navidrome_server_version(raw_value)
    baseline = parse_navidrome_server_version(LAST_KNOWN_PRE_CANONICAL_NAVIDROME_VERSION)
    # A develop/prerelease build can contain the pending canonical-ID migration
    # while retaining the numeric version of the last safe tagged release.
    if (
        parsed.kind != "release"
        or parsed.semantic_version is None
        or baseline.semantic_version is None
    ):
        return None
    return parsed.semantic_version > baseline.semantic_version
