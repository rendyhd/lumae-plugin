"""P2-7: friend artwork proxy is bounded in time and size and backs off failing friends."""

import http.server
import threading
import time

import pytest
import requests

from test_federated_lifecycle import connection, federation, remote_album  # noqa: F401
from test_lumae_analysis import lumae_postgres_db  # noqa: F401

PNG = b"\x89PNG\r\n\x1a\n" + b"x" * 1000
ARTWORK = "/api/friend-artwork/remote/remote-album"


@pytest.fixture
def friend(federation, monkeypatch):
    mod = federation
    cid = connection(mod)
    remote_album(mod, cid)
    # Loopback test servers are rejected by the SSRF guard; bypass it here.
    monkeypatch.setattr(mod, "_validate_base_url", lambda url: url)
    return mod


def _set_friend_url(mod, url):
    with mod.test_db.cursor() as cur:
        cur.execute("UPDATE friend_connections SET base_url=%s", (url,))
    mod.test_db.commit()


@pytest.fixture
def slow_server():
    """A real HTTP server that stalls before sending headers or trickles its body."""
    stop = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.server.mode == "ok":
                body = PNG * 200
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                for start in range(0, len(body), 7000):
                    self.wfile.write(body[start : start + 7000])
                    self.wfile.flush()
                return
            if self.server.mode == "stall":
                stop.wait(12)
                return
            if self.server.mode == "late-headers":
                if stop.wait(4.8):
                    return
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", "1000000")
                self.end_headers()
                self.wfile.write(b"x" * 16)
                self.wfile.flush()
                stop.wait(12)
                return
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", "1000000")
            self.end_headers()
            for _ in range(40):
                if stop.wait(0.5):
                    return
                try:
                    self.wfile.write(b"x" * 16)
                    self.wfile.flush()
                except OSError:
                    return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.mode = "stall"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    stop.set()
    server.shutdown()
    server.server_close()


class FakeResponse:
    def __init__(self, body=PNG, status=200, content_type="image/png", length=None):
        self.status_code = status
        self.body = body
        self.headers = {"Content-Type": content_type}
        if length is not None:
            self.headers["Content-Length"] = str(length)
        self.closed = False
        self.read = 0

    def iter_content(self, size):
        for start in range(0, len(self.body), size):
            chunk = self.body[start : start + size]
            self.read += len(chunk)
            yield chunk

    def close(self):
        self.closed = True


def _recording_get(mod, monkeypatch, result):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        if isinstance(result, BaseException):
            raise result
        return result() if callable(result) else result

    monkeypatch.setattr(mod.requests, "get", get)
    return calls


@pytest.mark.parametrize("mode", ["stall", "trickle", "late-headers"])
def test_slow_friend_errors_within_six_seconds(friend, slow_server, mode):
    mod = friend
    slow_server.mode = mode
    _set_friend_url(mod, f"http://127.0.0.1:{slow_server.server_address[1]}")
    started = time.monotonic()
    response = mod.test_client.get(ARTWORK)
    elapsed = time.monotonic() - started
    assert response.status_code in (502, 504)
    assert "error" in response.get_json()
    assert elapsed < 6.0, elapsed


def test_real_healthy_friend_streams_full_body(friend, slow_server):
    mod = friend
    slow_server.mode = "ok"
    _set_friend_url(mod, f"http://127.0.0.1:{slow_server.server_address[1]}")
    response = mod.test_client.get(ARTWORK)
    assert response.status_code == 200
    assert response.data == PNG * 200
    assert response.headers["Content-Type"] == "image/jpeg"


def test_dns_delay_plus_late_headers_stays_within_deadline(
    friend, slow_server, monkeypatch
):
    mod = friend
    slow_server.mode = "late-headers"
    _set_friend_url(mod, f"http://127.0.0.1:{slow_server.server_address[1]}")

    def slow_resolve(url):
        time.sleep(2)
        return url

    monkeypatch.setattr(mod, "_validate_base_url", slow_resolve)
    started = time.monotonic()
    response = mod.test_client.get(ARTWORK)
    elapsed = time.monotonic() - started
    assert response.status_code == 504
    assert elapsed < 6.0, elapsed


def test_fallback_without_read1_is_stopped_by_watchdog(friend, monkeypatch):
    mod = friend
    closed = threading.Event()

    class BlockingResponse(FakeResponse):
        def iter_content(self, size):
            yield b"\x89PNG"
            closed.wait(10)
            raise requests.exceptions.ConnectionError("closed by watchdog")

        def close(self):
            self.closed = True
            closed.set()

    monkeypatch.setattr(mod, "FRIEND_TOTAL_DEADLINE_SECONDS", 0.5)
    _recording_get(mod, monkeypatch, lambda: BlockingResponse())
    started = time.monotonic()
    response = mod.test_client.get(ARTWORK)
    assert response.status_code in (502, 504)
    assert time.monotonic() - started < 2


def test_friend_request_uses_bounded_timeouts(friend, monkeypatch):
    mod = friend
    calls = _recording_get(mod, monkeypatch, lambda: FakeResponse())
    assert mod.test_client.get(ARTWORK).status_code == 200
    kwargs = calls[0][1]
    connect, read = kwargs["timeout"]
    assert 0 < connect <= 3 and 4 < read <= 5
    assert kwargs["stream"] is True
    assert kwargs["allow_redirects"] is False


def test_oversized_friend_body_is_cut_off_and_errors(friend, monkeypatch):
    mod = friend
    big = FakeResponse(body=b"x" * (mod.MAX_FRIEND_ARTWORK_BYTES + 256 * 1024))
    _recording_get(mod, monkeypatch, big)
    response = mod.test_client.get(ARTWORK)
    assert response.status_code == 502
    assert "error" in response.get_json()
    assert big.closed
    # Reading stops at the first chunk over the cap, not at the end of the body.
    assert big.read <= mod.MAX_FRIEND_ARTWORK_BYTES + 64 * 1024


def test_declared_oversized_friend_body_is_rejected_unread(friend, monkeypatch):
    mod = friend
    big = FakeResponse(length=mod.MAX_FRIEND_ARTWORK_BYTES + 1)
    _recording_get(mod, monkeypatch, big)
    assert mod.test_client.get(ARTWORK).status_code == 413
    assert big.read == 0 and big.closed


def test_negative_cache_short_circuits_then_expires(friend, monkeypatch):
    mod = friend
    now = [1000.0]
    monkeypatch.setattr(mod, "_now", lambda: now[0])
    calls = _recording_get(
        mod, monkeypatch, requests.exceptions.ConnectTimeout("friend down")
    )
    assert mod.test_client.get(ARTWORK).status_code == 504
    assert len(calls) == 1

    now[0] += 10
    cached = mod.test_client.get(ARTWORK)
    assert cached.status_code == 503
    assert "error" in cached.get_json()
    assert int(cached.headers["Retry-After"]) >= 1
    assert len(calls) == 1, "negative cache must skip the outbound call"

    now[0] += mod.FRIEND_NEGATIVE_TTL_SECONDS
    calls_ok = _recording_get(mod, monkeypatch, lambda: FakeResponse())
    assert mod.test_client.get(ARTWORK).status_code == 200
    assert len(calls_ok) == 1
    # Success clears the entry; the next request goes out again.
    assert mod.test_client.get(ARTWORK).status_code == 200
    assert len(calls_ok) == 2


def test_only_unavailable_statuses_are_negatively_cached(friend, monkeypatch):
    mod = friend
    calls = _recording_get(mod, monkeypatch, lambda: FakeResponse(status=404))
    assert mod.test_client.get(ARTWORK).status_code == 404
    assert mod.test_client.get(ARTWORK).status_code == 404
    assert len(calls) == 2
    calls = _recording_get(mod, monkeypatch, lambda: FakeResponse(status=500))
    assert mod.test_client.get(ARTWORK).status_code == 502
    assert mod.test_client.get(ARTWORK).status_code == 502
    assert len(calls) == 2
    for status in (503, 504):
        mod._FRIEND_FAILURES.clear()
        calls = _recording_get(mod, monkeypatch, lambda: FakeResponse(status=status))
        assert mod.test_client.get(ARTWORK).status_code == 502
        assert mod.test_client.get(ARTWORK).status_code == 503
        assert len(calls) == 1


def test_one_album_502_does_not_block_other_albums(friend, monkeypatch):
    mod = friend
    with mod.test_db.cursor() as cur:
        cur.execute("SELECT id FROM friend_connections")
        cid = cur.fetchone()[0]
    remote_album(mod, cid, key="other-album", title="Other")

    def get(url, **kwargs):
        calls.append(url)
        if url.endswith("/remote-album"):
            return FakeResponse(status=502)
        return FakeResponse()

    calls = []
    monkeypatch.setattr(mod.requests, "get", get)
    assert mod.test_client.get(ARTWORK).status_code == 502
    other = mod.test_client.get("/api/friend-artwork/remote/other-album")
    assert other.status_code == 200
    assert other.data == PNG
    assert len(calls) == 2


def test_unresolvable_friend_is_negatively_cached(friend, monkeypatch):
    mod = friend
    resolved = []

    def fail(url):
        resolved.append(url)
        raise ValueError("Friend hostname could not be resolved")

    monkeypatch.setattr(mod, "_validate_base_url", fail)
    calls = _recording_get(mod, monkeypatch, lambda: FakeResponse())
    assert mod.test_client.get(ARTWORK).status_code == 502
    assert mod.test_client.get(ARTWORK).status_code == 503
    assert len(resolved) == 1
    assert calls == []


def test_hanging_dns_is_bounded_and_cached(friend, monkeypatch):
    mod = friend
    release = threading.Event()
    resolved = []

    def hang(url):
        resolved.append(url)
        release.wait(10)
        return url

    monkeypatch.setattr(mod, "_validate_base_url", hang)
    monkeypatch.setattr(mod, "FRIEND_RESOLVE_TIMEOUT_SECONDS", 0.5)
    calls = _recording_get(mod, monkeypatch, lambda: FakeResponse())
    try:
        started = time.monotonic()
        assert mod.test_client.get(ARTWORK).status_code == 504
        assert time.monotonic() - started < 2
        assert mod.test_client.get(ARTWORK).status_code == 503
        assert len(resolved) == 1
        assert calls == []
    finally:
        release.set()


def test_negative_cache_is_bounded(friend):
    mod = friend
    for index in range(mod.FRIEND_NEGATIVE_CACHE_MAX + 50):
        mod._friend_mark_failed(f"https://f{index}.example")
    assert len(mod._FRIEND_FAILURES) == mod.FRIEND_NEGATIVE_CACHE_MAX
    assert mod._friend_backoff_remaining("https://f0.example") == 0


def test_healthy_friend_proxies_bytes_headers_and_content_type(friend, monkeypatch):
    mod = friend
    calls = _recording_get(
        mod, monkeypatch, lambda: FakeResponse(content_type="image/webp")
    )
    response = mod.test_client.get(ARTWORK + "?size=5000")
    assert response.status_code == 200
    assert response.data == PNG
    assert response.headers["Content-Type"] == "image/webp"
    assert response.headers["Cache-Control"] == "private, max-age=3600"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    url, kwargs = calls[0]
    assert (
        url == "https://alice.example/plugins/federated_albums/api/artwork/remote-album"
    )
    assert kwargs["params"] == {"size": 1200}
    assert kwargs["headers"] == {
        "Authorization": "Bearer afa_secret",
        "Accept": "image/*",
    }
