"""Release-channel guards: private source cannot alter immutable public releases."""

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "catalog_builder", ROOT / "scripts" / "build_catalog.py"
)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


@pytest.fixture
def release_root(tmp_path):
    for folder in ("LumaeAnalysis", "FederatedAlbums"):
        target = tmp_path / "plugins" / folder
        target.mkdir(parents=True)
        shutil.copyfile(
            ROOT / "plugins" / folder / "plugin.json", target / "plugin.json"
        )
        (target / "__init__.py").write_text("PRIVATE_SOURCE = True\n")
    shutil.copyfile(ROOT / "release-sources.json", tmp_path / "release-sources.json")
    # Exercise the historic pinned release independently of the current release.
    metadata_path = tmp_path / "plugins" / "LumaeAnalysis" / "plugin.json"
    metadata = json.loads(metadata_path.read_text())
    pinned = next(item for item in metadata["versions"] if item["version"] == "1.1.8")
    metadata["versions"] = [pinned]
    metadata_path.write_text(json.dumps(metadata))
    policy_path = tmp_path / "release-sources.json"
    policy = json.loads(policy_path.read_text())
    policy["plugins"]["LumaeAnalysis"] = {
        "mode": "pinned-artifact", "version": pinned["version"],
        "checksum": pinned["checksum"],
    }
    policy_path.write_text(json.dumps(policy))
    dest = tmp_path / "dist" / "lumae_analysis"
    dest.mkdir(parents=True)
    shutil.copyfile(
        ROOT / "dist" / "lumae_analysis" / "lumae_analysis_1.1.8.zip",
        dest / "lumae_analysis_1.1.8.zip",
    )
    return tmp_path


def test_private_edits_leave_public_zip_immutable(release_root):
    artifact = release_root / "dist" / "lumae_analysis" / "lumae_analysis_1.1.8.zip"
    before = artifact.read_bytes()
    result = builder.build_catalog(release_root, repository="owner/repo")
    assert [entry["id"] for entry in result["plugins"]] == ["lumae_analysis"]
    assert artifact.read_bytes() == before
    (release_root / "plugins" / "LumaeAnalysis" / "__init__.py").write_text(
        'PRIVATE_SOURCE = "changed"\n'
    )
    assert (
        builder.build_catalog(release_root, repository="owner/repo", check=True)
        == result
    )
    assert artifact.read_bytes() == before


def test_tampered_public_artifact_fails_before_writes(release_root):
    artifact = release_root / "dist" / "lumae_analysis" / "lumae_analysis_1.1.8.zip"
    artifact.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        builder.build_catalog(release_root, repository="owner/repo")
    assert not (release_root / "manifest.json").exists()


def test_source_mode_keeps_immutability_guard(release_root):
    policy = release_root / "release-sources.json"
    value = json.loads(policy.read_text())
    value["plugins"]["LumaeAnalysis"]["mode"] = "source"
    policy.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="immutable"):
        builder.build_catalog(release_root, repository="owner/repo")


def test_undeclared_plugin_cannot_leak_to_public_catalogue(release_root):
    folder = release_root / "plugins" / "Unexpected"
    folder.mkdir()
    (folder / "plugin.json").write_text("{}")
    with pytest.raises(ValueError, match="explicit"):
        builder.build_catalog(release_root, repository="owner/repo")


def test_new_source_release_keeps_previous_archive_immutable(release_root):
    import zipfile

    old = release_root / "dist" / "lumae_analysis" / "lumae_analysis_1.1.8.zip"
    previous_bytes = old.read_bytes()
    metadata_path = release_root / "plugins" / "LumaeAnalysis" / "plugin.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["versions"].insert(0, {"version": "1.2.0", "checksum": ""})
    metadata_path.write_text(json.dumps(metadata))
    policy_path = release_root / "release-sources.json"
    policy = json.loads(policy_path.read_text())
    policy["plugins"]["LumaeAnalysis"] = {"mode": "source", "version": "1.2.0"}
    policy_path.write_text(json.dumps(policy))

    for cache_name in ("__pycache__", ".pytest_cache", ".ruff_cache"):
        cache = release_root / "plugins" / "LumaeAnalysis" / cache_name
        cache.mkdir()
        (cache / "local-state").write_text("local test state")
    result = builder.build_catalog(release_root, repository="owner/repo")

    assert old.read_bytes() == previous_bytes
    assert [entry["id"] for entry in result["plugins"]] == ["lumae_analysis"]
    latest = json.loads(metadata_path.read_text())["versions"][0]
    archive = release_root / "dist" / "lumae_analysis" / "lumae_analysis_1.2.0.zip"
    assert hashlib.md5(archive.read_bytes()).hexdigest() == latest["checksum"]
    with zipfile.ZipFile(archive) as package:
        assert package.namelist() == ["__init__.py"]
    builder.build_catalog(release_root, repository="owner/repo", check=True)
    (release_root / "plugins" / "LumaeAnalysis" / "__init__.py").write_text("CHANGED = True")
    with pytest.raises(ValueError, match="immutable"):
        builder.build_catalog(release_root, repository="owner/repo")
    assert old.read_bytes() == previous_bytes
