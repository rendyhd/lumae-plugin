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
