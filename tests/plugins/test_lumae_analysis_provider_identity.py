import importlib.util
import json
import sys
from pathlib import Path

import pytest

FIXTURES = Path(__file__).with_name("navidrome_canonical_ids_golden.json")
MODULE_PATH = Path(__file__).resolve().parents[2] / "plugins" / "LumaeAnalysis" / "provider_identity.py"
SPEC = importlib.util.spec_from_file_location("lumae_provider_identity", MODULE_PATH)
assert SPEC and SPEC.loader
provider_identity = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = provider_identity
SPEC.loader.exec_module(provider_identity)

NAVIDROME_BASE62_ALPHABET = provider_identity.NAVIDROME_BASE62_ALPHABET
canonicalize_navidrome_id = provider_identity.canonicalize_navidrome_id
is_after_last_known_pre_canonical_version = (
    provider_identity.is_after_last_known_pre_canonical_version
)
parse_navidrome_server_version = provider_identity.parse_navidrome_server_version


def test_canonical_id_codec_matches_upstream_golden_vectors_and_is_idempotent():
    fixture = json.loads(FIXTURES.read_text(encoding="utf-8"))
    assert NAVIDROME_BASE62_ALPHABET == fixture["source"]["alphabet"]
    for vector in fixture["vectors"]:
        converted = canonicalize_navidrome_id(vector["input"])
        assert converted.value == vector["expected"], vector["name"]
        assert converted.recognized is vector["recognized"], vector["name"]
        assert converted.changed is vector["changed"], vector["name"]
        assert converted.shape == vector["shape"], vector["name"]
        assert canonicalize_navidrome_id(converted.value).value == converted.value


@pytest.mark.parametrize(
    ("raw", "kind", "semantic_version", "prerelease", "commit_sha"),
    [
        ("0.63.2", "release", (0, 63, 2), None, None),
        ("v0.64.0", "release", (0, 64, 0), None, None),
        (
            "0.64.0-SNAPSHOT (ABCDEF0123)",
            "prerelease",
            (0, 64, 0),
            "SNAPSHOT",
            "abcdef0123",
        ),
        ("master (15c9c899fd0d)", "branch", None, None, "15c9c899fd0d"),
        ("dev", "branch", None, None, None),
        ("custom-build", "unknown", None, None, None),
    ],
)
def test_parse_navidrome_server_version(raw, kind, semantic_version, prerelease, commit_sha):
    parsed = parse_navidrome_server_version(raw)
    assert parsed.kind == kind
    assert parsed.semantic_version == semantic_version
    assert parsed.prerelease == prerelease
    assert parsed.commit_sha == commit_sha


def test_conservative_version_boundary_trusts_only_tagged_releases():
    assert is_after_last_known_pre_canonical_version("0.63.2") is False
    assert is_after_last_known_pre_canonical_version("0.63.2-SNAPSHOT") is None
    assert is_after_last_known_pre_canonical_version("0.63.3") is True
    assert is_after_last_known_pre_canonical_version("0.64.0-SNAPSHOT (abcdef0)") is None
    assert is_after_last_known_pre_canonical_version("master (abcdef0)") is None
    assert is_after_last_known_pre_canonical_version("custom-build") is None
