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
import time

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
_PID_DIR = re.compile(r"^task-(\d+)(?:-(\d+))?$")


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


def _start_time(pid):
    """A process's start time (Linux ``/proc/<pid>/stat`` field 22, clock ticks
    since boot), or None where there is no such file."""
    try:
        with open(f"/proc/{int(pid)}/stat", "rb") as stat:
            data = stat.read()
        return int(data[data.rindex(b")") + 2:].split()[19])
    except (OSError, ValueError, IndexError):
        return None


def _directory_name(pid=None):
    """``task-<pid>-<start time>``: a later process given the same PID (a
    restarted container's usually is) never takes a dead one's directory for
    its own. ``task-<pid>`` where the start time cannot be read."""
    pid = os.getpid() if pid is None else pid
    started = _start_time(pid)
    return f"task-{pid}" if started is None else f"task-{pid}-{started}"


def task_directory(host_temp_dir=None):
    """This process's download directory (created), outside ``host_temp_dir``."""
    path = os.path.join(_root(host_temp_dir), _directory_name())
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


def _leftover(pid, started, own):
    """Whether ``task-<pid>[-<started>]`` belongs to no running process."""
    if pid == os.getpid():
        # This process's own directory is never a leftover; another one with
        # its PID (an older name, another start time) is a dead process's.
        return own is not None and own != (pid, started)
    if not _alive(pid):
        return True
    # A live process with that PID: a leftover when its start time shows it is
    # another process. Without a start time on either side, it is kept.
    current = _start_time(pid)
    return started is not None and current is not None and current != started


def remove_leftovers(host_temp_dir=None):
    """Remove the download directories of task processes that are gone; returns how many.

    A directory is a dead process's when no process has its PID, or the
    process that has it started at another time (PID reuse), or it carries
    this process's PID under another name (a restart that got the same PID).
    """
    root = _root(host_temp_dir)
    removed = 0
    try:
        names = os.listdir(root)
    except OSError:
        return 0
    own_started = _start_time(os.getpid())
    own = None if own_started is None else (os.getpid(), own_started)
    for name in names:
        match = _PID_DIR.match(name)
        if not match:
            continue
        pid = int(match.group(1))
        started = int(match.group(2)) if match.group(2) is not None else None
        if not _leftover(pid, started, own):
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


def fetch(target, cancelled=None, session=None, deadline_seconds=None):
    """Write the target's file to ``target.path``; returns the path.

    ``cancelled()`` is checked between chunks (a parallel pass that raises
    stops a long download). ``deadline_seconds`` bounds the whole download
    (a server that trickles bytes never stalls long enough for the read
    timeout); callers pass the per-file analysis limit. Raises
    ``DownloadError``; a partial file is removed.
    """
    give_up = None if deadline_seconds is None else time.monotonic() + max(1.0, float(deadline_seconds))
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
                if give_up is not None and time.monotonic() >= give_up:
                    raise DownloadError("download exceeded the per-file analysis limit")
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
