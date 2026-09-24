"""Prove the qualification harness rejects forged paths and cluster IDs before writes."""

import json
import os
import pathlib
from unittest.mock import patch

from runtime import configure


def main():
    runtime_path = pathlib.Path(os.environ["LUM010_QUAL_RUNTIME"]).resolve()
    data = json.loads(runtime_path.read_text(encoding="utf-8"))
    original = pathlib.Path.read_text
    for label, mutation in (
        ("other work directory", {"work_dir": str(runtime_path.parent.parent)}),
        ("other PostgreSQL cluster", {"pg_system_identifier": "0"}),
    ):
        changed = {**data, **mutation}

        def forged(path, *args, **kwargs):
            if path.resolve() == runtime_path:
                return json.dumps(changed)
            return original(path, *args, **kwargs)

        with patch.object(pathlib.Path, "read_text", forged):
            try:
                configure()
            except RuntimeError:
                pass
            else:
                raise RuntimeError(f"preflight accepted {label}")
    print("qualification preflight rejects forged work directory and PostgreSQL cluster")


if __name__ == "__main__":
    main()
