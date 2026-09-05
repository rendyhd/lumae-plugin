"""Explicit, resumable provisioner for the optional DJ model stack."""

import argparse
import hashlib
import os
import sys
import time
import urllib.request
from pathlib import Path

from .dj_analysis import (
    MODEL_BYTES,
    MODEL_SHA256,
    MODEL_URL,
    YAMNET_MODEL_BYTES,
    YAMNET_MODEL_SHA256,
    YAMNET_MODEL_URL,
    DjAnalysisError,
    verify_model_artifact,
    verify_yamnet_model_artifact,
)


ACKNOWLEDGEMENT = (
    "I reviewed the Beat This license and the YAMNet/AudioSet training-data "
    "and calibration caveats"
)


def _response_status(response):
    status = getattr(response, "status", None)
    if status is None:
        getcode = getattr(response, "getcode", None)
        status = getcode() if callable(getcode) else None
    return status


def _verified_download(
    url,
    output,
    *,
    expected_bytes,
    expected_sha256,
    verifier,
    opener,
    cancelled=None,
    deadline=None,
):
    def check():
        if cancelled and cancelled():
            raise DjAnalysisError("cancelled")
        if deadline is not None and time.monotonic() >= deadline:
            raise DjAnalysisError("deadline_exceeded")
    check()
    target = Path(output).expanduser().resolve()
    try:
        verifier(target)
        return target
    except DjAnalysisError:
        pass
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".partial")
    offset = partial.stat().st_size if partial.is_file() else 0
    if offset < 0 or offset > expected_bytes:
        partial.unlink(missing_ok=True)
        offset = 0
    request = urllib.request.Request(
        url,
        headers={"Range": f"bytes={offset}-"} if offset else {},
    )
    response = opener(request, timeout=60)
    status = _response_status(response)
    if offset and status != 206:
        offset = 0
    digest = hashlib.sha256()
    if offset:
        with open(partial, "rb") as existing:
            while True:
                check()
                chunk = existing.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
    total = offset
    mode = "ab" if offset else "wb"
    try:
        with response, open(partial, mode) as sink:
            while True:
                check()
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > expected_bytes:
                    raise ValueError("model exceeds pinned byte length")
                digest.update(chunk)
                sink.write(chunk)
            sink.flush()
            os.fsync(sink.fileno())
        if total != expected_bytes or digest.hexdigest() != expected_sha256:
            raise ValueError("model does not match pinned size and SHA-256")
        check()
        os.replace(partial, target)
        verifier(target)
        return target
    except Exception:
        # Preserve bounded partial data for restart-safe range resumption.
        if partial.exists() and partial.stat().st_size >= expected_bytes:
            partial.unlink()
        raise


def provision(output, *, opener=urllib.request.urlopen, cancelled=None, deadline=None):
    """Backward-compatible Beat This provision entry point."""
    return _verified_download(
        MODEL_URL,
        output,
        expected_bytes=MODEL_BYTES,
        expected_sha256=MODEL_SHA256,
        verifier=verify_model_artifact,
        opener=opener, cancelled=cancelled, deadline=deadline,
    )


def provision_yamnet(output, *, opener=urllib.request.urlopen, cancelled=None, deadline=None):
    return _verified_download(
        YAMNET_MODEL_URL,
        output,
        expected_bytes=YAMNET_MODEL_BYTES,
        expected_sha256=YAMNET_MODEL_SHA256,
        verifier=verify_yamnet_model_artifact,
        opener=opener, cancelled=cancelled, deadline=deadline,
    )


def provision_stack(beat_this_output, yamnet_output, *, opener=urllib.request.urlopen, cancelled=None, deadline_seconds=1800):
    deadline = time.monotonic() + max(1, min(1800, deadline_seconds))
    return {
        "beat_this": provision(beat_this_output, opener=opener, cancelled=cancelled, deadline=deadline),
        "yamnet": provision_yamnet(yamnet_output, opener=opener, cancelled=cancelled, deadline=deadline),
    }


def remove_stack(beat_this_output, yamnet_output):
    removed = []
    for value in (beat_this_output, yamnet_output):
        target = Path(value).expanduser().resolve()
        for candidate in (target, target.with_name(target.name + ".partial")):
            if candidate.is_file():
                candidate.unlink()
                removed.append(str(candidate))
    return removed


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Explicitly download and verify the optional Beat This + YAMNet DJ stack."
    )
    parser.add_argument("--beat-this-output", required=True)
    parser.add_argument("--yamnet-output", required=True)
    parser.add_argument("--acknowledgement", required=True)
    args = parser.parse_args(argv)
    if args.acknowledgement != ACKNOWLEDGEMENT:
        parser.error(f"--acknowledgement must exactly equal: {ACKNOWLEDGEMENT!r}")
    installed = provision_stack(args.beat_this_output, args.yamnet_output)
    print(f"Installed verified Beat This model at {installed['beat_this']}")
    print(f"Installed verified YAMNet model at {installed['yamnet']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
