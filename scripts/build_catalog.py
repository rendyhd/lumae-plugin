"""Build the public catalogue from explicit release sources.

Pinned releases reuse and verify their immutable archive. Only a source-mode
entry with a new, unchecksummed version may build the current working source.
All validation completes before any output is replaced.
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import tempfile
import zipfile
from pathlib import Path


def checksum(path):
    return hashlib.md5(path.read_bytes()).hexdigest()


def code_zip(source, output):
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(source.rglob("*")):
            if (
                not path.is_file()
                or "__pycache__" in path.parts
                or path.suffix == ".pyc"
            ):
                continue
            name = path.relative_to(source).as_posix()
            if name == "plugin.json":
                continue
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, path.read_bytes())


def build_catalog(root, *, repository, branch="main", check=False):
    root = Path(root).resolve()
    policy = json.loads((root / "release-sources.json").read_text(encoding="utf-8"))
    raw = f"https://raw.githubusercontent.com/{repository}/{branch}"
    entries = policy["plugins"]
    folders = {path.parent.name for path in (root / "plugins").glob("*/plugin.json")}
    if set(entries) != folders:
        raise ValueError("Every plugin must have an explicit release-source policy")
    catalog = []
    writes = []
    with tempfile.TemporaryDirectory() as scratch:
        for folder, release in sorted(entries.items()):
            if release["mode"] == "private":
                continue
            if release["mode"] not in ("pinned-artifact", "source"):
                raise ValueError("Unknown release mode")
            metadata_path = root / "plugins" / folder / "plugin.json"
            meta = json.loads(metadata_path.read_text(encoding="utf-8"))
            versions = meta.get("versions") or []
            if not versions or versions[0]["version"] != release["version"]:
                raise ValueError(
                    f"{folder}: latest metadata and release policy disagree"
                )
            latest = versions[0]
            version = latest["version"]
            if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
                raise ValueError("The public catalogue accepts stable releases only")
            artifact = root / "dist" / meta["id"] / f"{meta['id']}_{version}.zip"
            declared = latest.get("checksum")
            if release["mode"] == "pinned-artifact":
                if (
                    not declared
                    or declared != release.get("checksum")
                    or not artifact.is_file()
                    or checksum(artifact) != declared
                ):
                    raise ValueError(f"{folder}: immutable release checksum mismatch")
            else:
                candidate = Path(scratch) / artifact.name
                code_zip(metadata_path.parent, candidate)
                actual = checksum(candidate)
                if declared and (
                    not artifact.is_file()
                    or checksum(artifact) != declared
                    or actual != declared
                ):
                    raise ValueError(
                        f"{folder}: published versions are immutable; select a new version"
                    )
                if not declared:
                    if artifact.exists():
                        raise ValueError(
                            "Unchecksummed release would replace an existing archive"
                        )
                    writes.append((artifact, candidate.read_bytes()))
                latest["checksum"] = actual
            latest["sourceUrl"] = f'{raw}/dist/{meta["id"]}/{artifact.name}'
            writes.append((metadata_path, (json.dumps(meta, indent=2) + "\n").encode()))
            catalog.append(
                {
                    key: meta.get(key, "")
                    for key in ("id", "name", "author", "description")
                }
                | {"pluginUrl": f"{raw}/plugins/{folder}/plugin.json"}
            )
        catalog.sort(key=lambda entry: entry["id"])
        result = {"plugins": catalog}
        writes.append(
            (root / "manifest.json", (json.dumps(result, indent=2) + "\n").encode())
        )
        if not check:
            for target, payload in writes:
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists() and target.read_bytes() == payload:
                    continue
                with tempfile.NamedTemporaryFile(
                    dir=target.parent, delete=False
                ) as handle:
                    handle.write(payload)
                    temp = Path(handle.name)
                try:
                    os.replace(temp, target)
                finally:
                    temp.unlink(missing_ok=True)
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument(
        "--repository", default=os.environ.get("REPO", "rendyhd/lumae-plugin")
    )
    parser.add_argument("--branch", default=os.environ.get("BRANCH", "main"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            build_catalog(
                args.root,
                repository=args.repository,
                branch=args.branch,
                check=args.check,
            ),
            indent=2,
        )
    )
