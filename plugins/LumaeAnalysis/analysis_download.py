"""Lumae's own download of a track's original file for analysis (WP2).

The host's ``download_track`` asks Navidrome for Subsonic ``stream`` without
``format=raw``, which may transcode, and it runs inside the host's server
binding, which may not be thread-local, so a parallel analysis thread (WP1b)
must never call it. Instead the tick's thread resolves the original-file
stream of the catalogue's own server under that binding (``resolve``: the
URL and credentials the workbench preview already uses, Navidrome
``rest/stream.view`` with ``format=raw``, Jellyfin
``/Audio/{id}/stream?static=true``), and any thread then fetches it with
plain HTTP (``fetch``) into this task's directory.

Downloads go to a directory of their own per task process, outside the
host's ``TEMP_DIR``, which AudioMuse's ``clean_temp`` empties while it runs
(``task_directory``); directories of processes that are gone are removed
when a worker starts (``remove_leftovers``).

A failure raises ``DownloadError``; callers fall back to the host's
download on the tick's thread.
"""

import hashlib
import logging
import os
import re
import shutil
import tempfile

import requests as http_requests

logger = logging.getLogger("lumae_analysis.download")

ROOT_NAME = "lumae-analysis"
# Connect and read timeouts; a stalled server ends a download, not the tick.
TIMEOUT = (10, 60)
CHUNK_BYTES = 256 * 1024
# A guard against a runaway response filling the disk (a DSD256 album side is about 4 GB).
MAX_BYTES = 8 * 1024 * 1024 * 1024
# A Subsonic error comes back as HTTP 200 with an XML or JSON body.
_ERROR_TYPES = ("text/xml", "application/xml", "application/json", "text/html", "text/plain")
_PID_DIR = re.compile(r"^task-(\d+)$")


class DownloadError(RuntimeError):
    """The original file could not be fetched; the message never holds a URL or a credential."""


class Target:
    """What a thread needs to fetch one file: no host call, no database."""

    __slots__ = ("url", "headers", "params", "path")

    def __init__(self, url, headers, params, path):
        self.url = url
        self.headers = dict(headers or {})
        self.params = dict(params or {})
        self.path = path


def _root(host_temp_dir=None):
    base = os.path.join(tempfile.gettempdir(), ROOT_NAME)
    if host_temp_dir:
        host = os.path.realpath(host_temp_dir)
        try:
            inside = os.path.commonpath([os.path.realpath(base), host]) == host
        except ValueError:
            inside = False
        if inside:
            base = os.path.join(os.path.dirname(host.rstrip(os.sep)) or os.sep, ROOT_NAME)
    return base


def task_directory(host_temp_dir=None):
    """This process's download directory (created), outside ``host_temp_dir``."""
    path = os.path.join(_root(host_temp_dir), f"task-{os.getpid()}")
    os.makedirs(path, mode=0o700, exist_ok=True)
    return path


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def remove_leftovers(host_temp_dir=None):
    """Remove the download directories of task processes that are gone; returns how many."""
    root = _root(host_temp_dir)
    removed = 0
    try:
        names = os.listdir(root)
    except OSError:
        return 0
    for name in names:
        match = _PID_DIR.match(name)
        if not match or int(match.group(1)) == os.getpid() or _alive(int(match.group(1))):
            continue
        shutil.rmtree(os.path.join(root, name), ignore_errors=True)
        removed += 1
    return removed


def file_path(directory, track_id):
    """A download's path: a digest of the track id, so no id reaches the file system."""
    digest = hashlib.sha256(str(track_id).encode("utf-8")).hexdigest()[:32]
    return os.path.join(directory, f"{digest}-{os.urandom(4).hex()}.audio")


def resolve(stream_target, provider_type, item_id, directory):
    """A ``Target`` from the workbench's stream target ``(url, headers, params)``.

    Navidrome gets ``format=raw``: the original file, never a transcode.
    """
    url, headers, params = stream_target
    params = dict(params or {})
    if str(provider_type or "").lower() == "navidrome":
        params["format"] = "raw"
    return Target(url, headers, params, file_path(directory, item_id))


def fetch(target, cancelled=None, session=None):
    """Write the target's file to ``target.path``; returns the path.

    ``cancelled()`` is checked between chunks (a drain cancel stops a long
    download). Raises ``DownloadError``; a partial file is removed.
    """
    http = session or http_requests
    try:
        response = http.get(target.url, params=target.params, headers=target.headers,
                            stream=True, timeout=TIMEOUT, allow_redirects=True)
    except http_requests.RequestException as exc:
        raise DownloadError(f"download failed ({type(exc).__name__})") from None
    try:
        if response.status_code >= 400:
            raise DownloadError(f"download refused (HTTP {response.status_code})")
        kind = str(response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if kind in _ERROR_TYPES:
            raise DownloadError(f"download answered with {kind}, not audio")
        written = 0
        with open(target.path, "wb") as out:
            for chunk in response.iter_content(chunk_size=CHUNK_BYTES):
                if cancelled is not None and cancelled():
                    raise DownloadError("download cancelled")
                if not chunk:
                    continue
                written += len(chunk)
                if written > MAX_BYTES:
                    raise DownloadError("download larger than the analysis limit")
                out.write(chunk)
        if not written:
            raise DownloadError("download was empty")
        return target.path
    except BaseException as exc:
        try:
            os.remove(target.path)
        except OSError:
            pass
        if isinstance(exc, http_requests.RequestException):
            raise DownloadError(f"download failed ({type(exc).__name__})") from None
        raise
    finally:
        response.close()
