"""Build an isolated DJ prerelease without changing the public plugin channel."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse


PRIVATE_VERSION_RE = re.compile(r"1\.2\.0-djtest\.[1-9][0-9]*\Z")
RUNTIME_VERSION_RE = re.compile(r'^PLUGIN_VERSION = "[^"]+"$', re.MULTILINE)


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _validate_base_url(value: str) -> str:
    parsed = urlparse(value.rstrip("/"))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base_url must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("base_url must not contain credentials, a query, or a fragment")
    return value.rstrip("/")


def _build_code_zip(source: Path, output: Path, version: str) -> None:
    members: list[tuple[Path, str]] = []
    for item in source.rglob("*"):
        if not item.is_file() or item.name == "plugin.json":
            continue
        if "__pycache__" in item.parts or item.suffix == ".pyc":
            continue
        members.append((item, item.relative_to(source).as_posix()))
    members.sort(key=lambda member: member[1])
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for path, archive_name in members:
            payload = path.read_bytes()
            if archive_name == "__init__.py":
                source_text = payload.decode("utf-8").replace("\r\n", "\n")
                patched, replacements = RUNTIME_VERSION_RE.subn(
                    f'PLUGIN_VERSION = "{version}"', source_text, count=1
                )
                if replacements != 1:
                    raise ValueError("plugin runtime version declaration was not found exactly once")
                payload = patched.encode("utf-8")
            info = zipfile.ZipInfo(archive_name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, payload)


def build_private_prerelease(
    *,
    repository_root: Path,
    output_root: Path,
    version: str,
    base_url: str,
) -> dict:
    """Create a code zip plus a separate private manifest and plugin metadata."""
    if not PRIVATE_VERSION_RE.fullmatch(version):
        raise ValueError("version must match 1.2.0-djtest.N")
    base_url = _validate_base_url(base_url)
    repository_root = repository_root.resolve()
    source = repository_root / "plugins" / "LumaeAnalysis"
    public_metadata_path = source / "plugin.json"
    if not public_metadata_path.is_file():
        raise FileNotFoundError(public_metadata_path)
    public_metadata = json.loads(public_metadata_path.read_text(encoding="utf-8"))
    if public_metadata["versions"][0]["version"] != "1.2.0":
        raise ValueError("public latest changed; review private prerelease isolation")

    destination = output_root.resolve() / public_metadata["id"] / version
    destination.mkdir(parents=True, exist_ok=True)
    zip_name = f"{public_metadata['id']}_{version}.zip"
    zip_path = destination / zip_name
    with tempfile.NamedTemporaryFile(dir=destination, delete=False) as temporary:
        candidate = Path(temporary.name)
    try:
        _build_code_zip(source, candidate, version)
        candidate.replace(zip_path)
    finally:
        candidate.unlink(missing_ok=True)

    checksum = hashlib.md5(zip_path.read_bytes()).hexdigest()
    private_metadata = {
        key: value for key, value in public_metadata.items() if key != "versions"
    }
    private_metadata["channel"] = "private-dj-test"
    private_metadata["versions"] = [
        {
            "version": version,
            "min_core_version": public_metadata["versions"][0]["min_core_version"],
            "changelog": (
                "Private opt-in DJ Analysis V3 test: localized rhythm and cue evidence, "
                "checksum-pinned Beat This/YAMNet, and source-bound priority analysis."
            ),
            "imageUrl": "",
            "sourceUrl": f"{base_url}/{zip_name}",
            "checksum": checksum,
        }
    ]
    _write_json(destination / "plugin.json", private_metadata)
    _write_json(
        destination / "manifest.json",
        {
            "channel": "private-dj-test",
            "plugins": [
                {
                    "id": public_metadata["id"],
                    "name": public_metadata["name"],
                    "author": public_metadata.get("author", ""),
                    "description": public_metadata.get("description", ""),
                    "pluginUrl": f"{base_url}/plugin.json",
                }
            ],
        },
    )
    return {
        "directory": str(destination),
        "zip": str(zip_path),
        "checksum": checksum,
        "version": version,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Build an isolated Lumae private DJ prerelease channel."
    )
    parser.add_argument("--version", required=True, help="1.2.0-djtest.N")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--output-root", default="private-dist")
    parser.add_argument("--repository-root", default=".")
    args = parser.parse_args(argv)
    result = build_private_prerelease(
        repository_root=Path(args.repository_root),
        output_root=Path(args.output_root),
        version=args.version,
        base_url=args.base_url,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
