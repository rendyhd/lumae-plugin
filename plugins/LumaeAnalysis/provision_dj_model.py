"""Explicit Beat This model provisioner. Never imported by playback/API code."""

import argparse
import hashlib
import os
import sys
import urllib.request
from pathlib import Path

from .dj_analysis import MODEL_BYTES, MODEL_SHA256, MODEL_URL


ACKNOWLEDGEMENT = "I reviewed the Beat This license and training-data caveat"


def provision(output, *, opener=urllib.request.urlopen):
    target = Path(output).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".partial")
    digest = hashlib.sha256()
    total = 0
    try:
        with opener(MODEL_URL, timeout=60) as response, open(partial, "wb") as sink:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MODEL_BYTES:
                    raise ValueError("model exceeds pinned byte length")
                digest.update(chunk)
                sink.write(chunk)
        if total != MODEL_BYTES or digest.hexdigest() != MODEL_SHA256:
            raise ValueError("model does not match pinned size and SHA-256")
        os.replace(partial, target)
        return target
    finally:
        if partial.exists():
            partial.unlink()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Explicitly download and verify the optional Beat This final0 checkpoint."
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--acknowledgement", required=True)
    args = parser.parse_args(argv)
    if args.acknowledgement != ACKNOWLEDGEMENT:
        parser.error(f"--acknowledgement must exactly equal: {ACKNOWLEDGEMENT!r}")
    path = provision(args.output)
    print(f"Installed verified model at {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
