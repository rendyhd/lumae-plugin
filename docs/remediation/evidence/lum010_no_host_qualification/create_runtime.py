"""Create synthetic local credentials for the separately started tmpfs PG17."""

import json
import argparse
import pathlib
import secrets

import psycopg2


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=("baseline", "main"))
    target = parser.parse_args().target
    source = pathlib.Path(__file__).resolve().parents[4]
    work = source / ".pytest-tmp" / (
        "lum010-no-host-runtime" if target == "baseline" else "lum010-no-host-main-runtime")
    work.mkdir(parents=True, exist_ok=True)
    runtime = work / "runtime.json"
    if runtime.exists():
        raise RuntimeError("disposable runtime already exists")
    with psycopg2.connect(
            "postgresql://lum010_no_host:disposable_lum010_20260924"
            "@127.0.0.1:55273/lum010_no_host", connect_timeout=5) as db:
        with db.cursor() as cur:
            cur.execute("SELECT system_identifier::text FROM pg_control_system()")
            identifier = cur.fetchone()[0]
    if target == "main":
        # Create only the named second fixture database on the verified tmpfs cluster.
        admin = psycopg2.connect(
            "postgresql://lum010_no_host:disposable_lum010_20260924"
            "@127.0.0.1:55273/lum010_no_host", connect_timeout=5)
        try:
            admin.autocommit = True
            with admin.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname='lum010_main'")
                if cur.fetchone() is not None:
                    raise RuntimeError("second disposable database already exists")
                cur.execute("CREATE DATABASE lum010_main")
        finally:
            admin.close()
    host = ((source.parent / "AudioMuse-AI") if target == "baseline" else
            (source / ".pytest-tmp" / "AudioMuse-current-main"))
    data = {
        "work_dir": str(work.resolve()),
        "host_source": str(host.resolve()),
        "pg_port": 55273,
        "redis_port": 55274,
        "host_port": 55275 if target == "baseline" else 55277,
        "provider_port": 55276 if target == "baseline" else 55278,
        "pg_password": "disposable_lum010_20260924",
        "pg_system_identifier": identifier,
        "api_token": secrets.token_urlsafe(32),
        "admin_password": secrets.token_urlsafe(24),
        "other_password": secrets.token_urlsafe(24),
        "provider_password": secrets.token_urlsafe(24),
        "jwt_secret": secrets.token_urlsafe(48),
    }
    runtime.write_text(json.dumps(data), encoding="utf-8")
    print("created synthetic disposable runtime")


if __name__ == "__main__":
    main()
