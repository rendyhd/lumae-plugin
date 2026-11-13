"""Review fixes without a database: a skipped refresh settles a turn, the
catalogue bootstrap principal is not the client address, ``/api/profiles``
reads attempts only for unpublished ids, and ``_json_body`` bounds a chunked
body."""
import io

import pytest
from flask import Flask, g

from test_lumae_analysis import load_plugin, plugin_client


def test_a_skipped_action_is_settled_and_no_progress():
    mod = load_plugin()
    assert "skipped" in mod._SETTLED_STATUSES
    assert not mod._moved_on("skipped", {"status": "skipped", "reason": "source_rebind_required"})


@pytest.mark.parametrize("auth, principal", [
    ({}, "anonymous"),
    ({"auth_method": "bearer"}, "bearer"),
    ({"auth_user": "alice", "auth_method": "session"}, "user:alice"),
])
def test_the_bootstrap_principal_does_not_follow_the_client_address(auth, principal):
    mod = load_plugin()
    app = Flask(__name__)
    for address in ("10.0.0.2", "192.168.1.9"):
        with app.test_request_context(environ_base={"REMOTE_ADDR": address}):
            for key, value in auth.items():
                setattr(g, key, value)
            assert mod._catalog_principal_key() == principal


def test_profiles_reads_attempts_only_for_unpublished_ids(monkeypatch):
    mod = load_plugin()
    asked = []
    monkeypatch.setattr(mod, "resolve_profile_source",
                        lambda **_kwargs: {"catalog_instance_id": "catalog-a", "server_id": "server-a"})
    monkeypatch.setattr(mod, "fetch_published_profile_rows",
                        lambda ids, source, schema=2: [{"track_id": "ready-1"}])
    monkeypatch.setattr(mod, "serialize_ready_profile", lambda row: {"track_id": row["track_id"]})
    monkeypatch.setattr(mod, "fetch_profile_rows",
                        lambda *_args, **_kwargs: pytest.fail("the edge payload is not read twice"))

    def status_rows(ids, catalog_instance_id=None):
        asked.append(list(ids))
        return [{"track_id": "failed-1", "status": "failed", "last_error": "decode failed"}]

    monkeypatch.setattr(mod, "fetch_profile_status_rows", status_rows)
    body = plugin_client(mod).get("/api/profiles?ids=ready-1,failed-1,missing-1").get_json()

    assert asked == [["failed-1", "missing-1"]]
    assert [row["track_id"] for row in body["profiles"]] == ["ready-1"]
    assert body["failed"] == [{"track_id": "failed-1", "reason": "decode failed"}]
    assert body["missing"] == ["missing-1"]


def _chunked(mod, payload, max_bytes):
    app = Flask(__name__)
    with app.test_request_context(
        method="POST", input_stream=io.BytesIO(payload), content_type="application/json",
        environ_overrides={"wsgi.input_terminated": True, "CONTENT_LENGTH": "",
                           "HTTP_TRANSFER_ENCODING": "chunked"},
    ):
        from flask import request
        assert request.content_length is None  # no Content-Length to check
        return mod._json_body(max_bytes=max_bytes)


def test_json_body_bounds_a_chunked_body():
    mod = load_plugin()
    with pytest.raises(ValueError, match="too large"):
        _chunked(mod, b'{"a": "' + b"x" * 100_000 + b'"}', 16_384)
    assert _chunked(mod, b'{"a": 1}', 16_384) == {"a": 1}
