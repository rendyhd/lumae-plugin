"""Run unmodified stock AudioMuse over real loopback HTTP."""

import os
import pathlib

from runtime import configure


def main():
    config = configure()
    from app import app  # noqa: E402 - environment must precede host imports
    from werkzeug.serving import run_simple

    (pathlib.Path(config["work_dir"]) / "host.pid").write_text(str(os.getpid()))
    run_simple("127.0.0.1", int(config["host_port"]), app,
               use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
