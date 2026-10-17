"""Performance plan WP2: Lumae's own download of the original file.

* Navidrome is asked for ``format=raw`` (no transcode); Jellyfin's
  ``static=true`` target is kept as it is.
* A Subsonic error (HTTP 200 with XML or JSON), an HTTP error, an empty body
  and a dropped connection are ``DownloadError``, with no URL or credential
  in the message, and leave no partial file.
* A cancel between chunks stops a download.
* Downloads live in a directory per task process outside the host's
  ``TEMP_DIR``; directories of processes that are gone are removed at worker
  start, and a simulated ``clean_temp`` of ``TEMP_DIR`` leaves them alone.
"""
import http.server
import os
import shutil
import subprocess
import sys
import threading

import pytest

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import analysis_download as download


class _Handler(http.server.BaseHTTPRequestHandler):
    routes = {}
    seen = []

    def do_GET(self):  # noqa: N802 - http.server API
        self.seen.append(self.path)
        status, kind, body = self.routes.get(self.path.split("?")[0], (404, "text/plain", b"missing"))
        self.send_response(status)
        self.send_header("Content-Type", kind)
        # A dropped connection promises more than it sends.
        self.send_header("Content-Length", str(len(body) if body is not None else 100_000))
        self.end_headers()
        if body is None:
            self.wfile.write(b"partial")
            self.wfile.flush()
            self.connection.shutdown(2)  # dropped mid-body
            return
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


@pytest.fixture
def server():
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    _Handler.routes, _Handler.seen = {}, []
    yield f"http://127.0.0.1:{httpd.server_address[1]}", _Handler
    httpd.shutdown()
    thread.join()


def test_navidrome_gets_format_raw_and_the_file_is_written(server, tmp_path):
    base, handler = server
    handler.routes["/rest/stream.view"] = (200, "audio/flac", b"fLaC" + b"\0" * 300_000)
    target = download.resolve((f"{base}/rest/stream.view", {}, {"id": "tr 1", "u": "admin", "t": "tok"}),
                              "navidrome", "tr 1", str(tmp_path))
    assert target.params["format"] == "raw" and target.params["id"] == "tr 1"
    path = download.fetch(target)
    assert path == target.path and os.path.getsize(path) == 300_004
    assert "format=raw" in handler.seen[0]
    # The id never reaches the file system.
    assert "tr 1" not in os.path.basename(path)


def test_jellyfin_keeps_its_static_target(tmp_path):
    target = download.resolve(("http://jf/Audio/abc/stream", {"Authorization": "MediaBrowser Token=x"},
                               {"static": "true"}), "jellyfin", "abc", str(tmp_path))
    assert target.params == {"static": "true"} and target.headers["Authorization"].endswith("x")


@pytest.mark.parametrize("route", [
    (200, "text/xml", b'<subsonic-response status="failed"><error code="70"/></subsonic-response>'),
    (200, "application/json", b'{"subsonic-response":{"status":"failed"}}'),
    (403, "audio/flac", b"no"),
    (200, "audio/flac", b""),
    (200, "audio/flac", None),
])
def test_failures_raise_without_secrets_and_leave_no_file(server, tmp_path, route):
    base, handler = server
    handler.routes["/rest/stream.view"] = route
    target = download.resolve((f"{base}/rest/stream.view", {}, {"id": "x", "t": "SECRET-TOKEN"}),
                              "navidrome", "x", str(tmp_path))
    with pytest.raises(download.DownloadError) as failed:
        download.fetch(target)
    assert "SECRET-TOKEN" not in str(failed.value) and base not in str(failed.value)
    assert not os.path.exists(target.path)


def test_an_unreachable_server_is_a_download_error(tmp_path):
    target = download.Target("http://127.0.0.1:9/never", {}, {}, str(tmp_path / "a.audio"))
    with pytest.raises(download.DownloadError):
        download.fetch(target)


def test_a_cancel_between_chunks_stops_the_download(server, tmp_path, monkeypatch):
    base, handler = server
    handler.routes["/big"] = (200, "audio/flac", b"\0" * (4 * download.CHUNK_BYTES))
    monkeypatch.setattr(download, "CHUNK_BYTES", 1024)
    chunks = []
    target = download.Target(f"{base}/big", {}, {}, str(tmp_path / "big.audio"))
    with pytest.raises(download.DownloadError, match="cancelled"):
        download.fetch(target, cancelled=lambda: chunks.append(1) or len(chunks) > 3)
    assert not os.path.exists(target.path)


def test_the_task_directory_is_outside_temp_dir_and_survives_clean_temp(tmp_path, monkeypatch):
    monkeypatch.setattr(download.tempfile, "gettempdir", lambda: str(tmp_path / "tmp"))
    host_temp = tmp_path / "tmp" / "audiomuse"
    host_temp.mkdir(parents=True)
    plain = download.task_directory(str(tmp_path / "elsewhere"))
    assert plain.startswith(str(tmp_path / "tmp" / download.ROOT_NAME))
    # A host TEMP_DIR that would contain it: the directory moves beside it.
    monkeypatch.setattr(download.tempfile, "gettempdir", lambda: str(host_temp))
    directory = download.task_directory(str(host_temp))
    assert not os.path.realpath(directory).startswith(os.path.realpath(str(host_temp)) + os.sep)
    open(os.path.join(directory, "a.audio"), "wb").close()
    shutil.rmtree(host_temp)  # AudioMuse's clean_temp
    host_temp.mkdir()
    assert os.path.exists(os.path.join(directory, "a.audio"))


def test_leftovers_of_gone_processes_are_removed_at_worker_start(tmp_path, monkeypatch):
    monkeypatch.setattr(download.tempfile, "gettempdir", lambda: str(tmp_path))
    mine = download.task_directory()
    gone = subprocess.run([sys.executable, "-c", "import os; print(os.getpid())"],
                          capture_output=True, text=True, check=True).stdout.strip()
    stale = tmp_path / download.ROOT_NAME / f"task-{gone}"
    stale.mkdir()
    (stale / "left.audio").write_bytes(b"x")
    other = tmp_path / download.ROOT_NAME / "not-ours"
    other.mkdir()
    assert download.remove_leftovers() == 1
    assert not stale.exists() and os.path.isdir(mine) and other.exists()
