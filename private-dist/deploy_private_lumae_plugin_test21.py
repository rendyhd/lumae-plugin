import hashlib
import json
import urllib.request

from flask_app import app
import database
from plugin.manager import plugin_manager
import restart_manager

VERSION = "1.2.0-djtest.21"
ROOT = f"http://192.168.1.172:8765/lumae_analysis/{VERSION}"
METADATA_URL = f"{ROOT}/plugin.json"
PACKAGE_URL = f"{ROOT}/lumae_analysis_{VERSION}.zip"
EXPECTED_MD5 = "ef94cf0b503d9a5d4d0d73842f0a6649"
EXPECTED_SHA256 = "78dedec66b662e62a947b96fffb698a8964304e823cb58769592063c6eca6bdf"

def download(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read()

catalog = json.loads(download(METADATA_URL))
releases = [entry for entry in catalog.get("versions", []) if entry.get("version") == VERSION]
if len(releases) != 1:
    raise RuntimeError(f"Expected one {VERSION} release, found {len(releases)}")
release = releases[0]
if release.get("sourceUrl") != PACKAGE_URL or release.get("checksum") != EXPECTED_MD5:
    raise RuntimeError("Private release metadata does not match the sealed deployment identity")
package = download(PACKAGE_URL)
if hashlib.md5(package, usedforsecurity=False).hexdigest() != EXPECTED_MD5:
    raise RuntimeError("Private package MD5 mismatch")
if hashlib.sha256(package).hexdigest() != EXPECTED_SHA256:
    raise RuntimeError("Private package SHA-256 mismatch")

manifest = {key: value for key, value in catalog.items() if key != "versions"}
manifest.update(
    version=VERSION,
    min_core_version=release.get("min_core_version"),
    changelog=release.get("changelog", ""),
    imageUrl=release.get("imageUrl", ""),
    requirements=release.get("requirements", manifest.get("requirements", [])),
    targets=release.get("targets", manifest.get("targets", [])),
)

with app.app_context():
    previous = database.get_plugin("lumae_analysis")
    installed, deps_ok, deps_error = plugin_manager.install_package(
        package,
        manifest,
        source_url=PACKAGE_URL,
        source_repo=METADATA_URL,
        expected_checksum=EXPECTED_MD5,
        on_registered=lambda _plugin_id: restart_manager.publish_plugin_sync_request(),
    )
    current = database.get_plugin("lumae_analysis")

print(json.dumps({
    "previous_version": previous.get("version") if previous else None,
    "installed_version": installed.get("version"),
    "checksum_matches": current.get("checksum") == EXPECTED_MD5 if current else False,
    "enabled": current.get("enabled") if current else None,
    "load_status": current.get("load_status") if current else None,
    "deps_ok": bool(deps_ok),
    "deps_error": deps_error,
}, sort_keys=True))
