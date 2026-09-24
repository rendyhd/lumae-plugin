"""Serve the pg_dump/pg_restore clone on another loopback port for qualification."""

import os

import psycopg2

from runtime import configure


def main():
    config = configure()  # verifies original disposable cluster and frozen sources
    if int(config["clone_port"]) in {
        int(config[name]) for name in ("pg_port", "redis_port", "host_port", "provider_port")
    }:
        raise RuntimeError("clone listener conflicts with another disposable port")
    clone_url = config["database_url"].rsplit("/", 1)[0] + "/lum010_clone"
    with psycopg2.connect(clone_url, connect_timeout=5) as db:
        with db.cursor() as cur:
            cur.execute("""SELECT current_database(),current_user,system_identifier::text,
                to_regclass('public.plugin_lumae_analysis__profile_bootstrap_sessions')::text
                FROM pg_control_system()""")
            name, role, identifier, sessions_table = cur.fetchone()
    if (name != "lum010_clone" or role != "lum010_qual"
            or identifier != config["pg_system_identifier"] or not sessions_table):
        raise RuntimeError("restored database is not the disposable clone")
    os.environ["DATABASE_URL"] = clone_url
    os.environ["POSTGRES_DB"] = "lum010_clone"
    from app import app
    from werkzeug.serving import run_simple

    run_simple("127.0.0.1", int(config["clone_port"]), app,
               use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
