"""Run the frozen AudioMuse app over real loopback HTTP for qualification."""

from runtime import configure


def main():
    config = configure()
    from app import app  # noqa: E402 - environment must precede host imports
    from werkzeug.serving import run_simple

    run_simple("127.0.0.1", int(config["host_port"]), app,
               use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
