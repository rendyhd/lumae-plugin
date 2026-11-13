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
    for folder in ("LumaeAnalysis",):
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
    # Everything else ships: data files and subfolders too.
    plugin = release_root / "plugins" / "LumaeAnalysis"
    (plugin / "data.json").write_text("{}")
    (plugin / "sub").mkdir()
    (plugin / "sub" / "module.py").write_text("")
    result = builder.build_catalog(release_root, repository="owner/repo")

    assert old.read_bytes() == previous_bytes
    assert [entry["id"] for entry in result["plugins"]] == ["lumae_analysis"]
    latest = json.loads(metadata_path.read_text())["versions"][0]
    archive = release_root / "dist" / "lumae_analysis" / "lumae_analysis_1.2.0.zip"
    assert hashlib.md5(archive.read_bytes()).hexdigest() == latest["checksum"]
    with zipfile.ZipFile(archive) as package:
        assert package.namelist() == ["__init__.py", "data.json", "sub/module.py"]
    builder.build_catalog(release_root, repository="owner/repo", check=True)
    (release_root / "plugins" / "LumaeAnalysis" / "__init__.py").write_text("CHANGED = True")
    with pytest.raises(ValueError, match="immutable"):
        builder.build_catalog(release_root, repository="owner/repo")
    assert old.read_bytes() == previous_bytes


def test_current_release_contains_supported_source_only():
    """Whether the archive equals the working source is not asked here: that is
    ``build_catalog.py --check``, a CI step of its own, and a plugin commit waiting
    for its release is not a test failure."""
    import zipfile

    latest = json.loads((ROOT / "plugins/LumaeAnalysis/plugin.json").read_text())["versions"][0]
    version = latest["version"]
    archive = ROOT / f"dist/lumae_analysis/lumae_analysis_{version}.zip"
    assert hashlib.md5(archive.read_bytes()).hexdigest() == latest["checksum"]
    with zipfile.ZipFile(archive) as package:
        names = package.namelist()
        assert not any(Path(name).name.startswith(("dj_", "provision_dj")) for name in names)
        assert f'PLUGIN_VERSION = "{version}"'.encode() in package.read("__init__.py")


def test_earlier_archives_still_match_their_checksums():
    # Back to 1.2.4. ``build_catalog.py --check`` only reads the latest version.
    versions = json.loads((ROOT / "plugins/LumaeAnalysis/plugin.json").read_text())["versions"]
    oldest = next(index for index, item in enumerate(versions) if item["version"] == "1.2.4")
    for item in versions[1 : oldest + 1]:
        archive = ROOT / f"dist/lumae_analysis/lumae_analysis_{item['version']}.zip"
        assert hashlib.md5(archive.read_bytes()).hexdigest() == item["checksum"], item["version"]


def test_release_archive_is_identical_across_platforms(tmp_path, monkeypatch):
    import zipfile

    # ZipInfo otherwise records the host platform in every central directory entry.
    from types import SimpleNamespace

    built = {}
    for platform in ("win32", "linux"):
        monkeypatch.setattr(zipfile, "sys", SimpleNamespace(platform=platform))
        built[platform] = tmp_path / f"{platform}.zip"
        builder.code_zip(ROOT / "plugins/LumaeAnalysis", built[platform])
    assert built["win32"].read_bytes() == built["linux"].read_bytes()
    with zipfile.ZipFile(built["linux"]) as package:
        for info in package.infolist():
            assert info.create_system == 0
            assert info.external_attr == 0o644 << 16
            assert info.date_time == (1980, 1, 1, 0, 0, 0)


def test_source_validation_preserves_archive_with_different_compression(release_root):
    import zipfile

    metadata_path = release_root / "plugins/LumaeAnalysis/plugin.json"
    metadata = json.loads(metadata_path.read_text())
    archive = release_root / "dist/lumae_analysis/lumae_analysis_1.1.8.zip"
    source = release_root / "plugins/LumaeAnalysis/__init__.py"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_STORED) as package:
        package.writestr("__init__.py", source.read_bytes())
    original = archive.read_bytes()
    declared = hashlib.md5(original).hexdigest()
    metadata["versions"][0]["checksum"] = declared
    metadata_path.write_text(json.dumps(metadata))
    policy_path = release_root / "release-sources.json"
    policy = json.loads(policy_path.read_text())
    policy["plugins"]["LumaeAnalysis"]["mode"] = "source"
    policy_path.write_text(json.dumps(policy))

    builder.build_catalog(release_root, repository="owner/repo")
    assert hashlib.md5(archive.read_bytes()).hexdigest() == declared
    assert json.loads(metadata_path.read_text())["versions"][0]["checksum"] == declared
    source.write_bytes(b"changed source")
    with pytest.raises(ValueError, match="immutable"):
        builder.build_catalog(release_root, repository="owner/repo")
