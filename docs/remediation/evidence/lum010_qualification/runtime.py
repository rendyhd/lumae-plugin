"""Load the private disposable runtime file and configure the frozen host."""

import json
import os
import pathlib
import subprocess
import sys
from urllib.parse import quote

import psycopg2

HOST_SHA = "89f2c9d43f0d9eaedfdf87bfc09e50299935d2bd"
PLUGIN_SHA = "4b547927c60ec5aad9f728fb6b67d46c836233a8"
PLUGIN_BOOKKEEPING_SHA = "dbf9297e55490f05406e5ac6b056d10a2a091c92"
CLIENT_SHA = "de4ad73f8c674335f4cb23628af1b4cfd8409cf9"


def _git(path, *args):
    result = subprocess.run(["git", "-C", str(path), *args],
                            capture_output=True, text=True, check=True)
    return result.stdout.strip()


def configure():
    runtime_path = pathlib.Path(os.environ["LUM010_QUAL_RUNTIME"]).resolve()
    data = json.loads(runtime_path.read_text(encoding="utf-8"))
    isolated_root = pathlib.Path(__file__).resolve().parents[5]
    work = pathlib.Path(data["work_dir"]).resolve()
    if (work != isolated_root / "lum010-qualification-runtime-20260924"
            or not work.is_dir() or runtime_path != work / "runtime.json"):
        raise RuntimeError("runtime file is not in the designated disposable directory")
    for role, expected in (("host", HOST_SHA), ("plugin", PLUGIN_BOOKKEEPING_SHA),
                           ("client", CLIENT_SHA)):
        source = pathlib.Path(data[f"{role}_source"]).resolve()
        if source != isolated_root / {
            "host": "AudioMuse-lum010-binding",
            "plugin": "lumae-lum010-capability",
            "client": "Auralscape-lum008-client",
        }[role]:
            raise RuntimeError(f"{role} source is outside the frozen qualification tree")
        if _git(source, "rev-parse", "HEAD") != expected or _git(source, "status", "--porcelain"):
            raise RuntimeError(f"{role} source is not the clean frozen checkpoint")
        data[f"{role}_source"] = str(source)
    if _git(data["plugin_source"], "merge-base", PLUGIN_SHA,
            PLUGIN_BOOKKEEPING_SHA) != PLUGIN_SHA:
        raise RuntimeError("plugin implementation is not in the bookkeeping checkpoint")
    pg_port = int(data["pg_port"])
    redis_port = int(data["redis_port"])
    if not all(1024 <= port <= 65535 for port in (
            pg_port, redis_port, int(data["host_port"]), int(data["provider_port"]))):
        raise RuntimeError("disposable port is outside the allowed range")
    with psycopg2.connect(host="127.0.0.1", port=pg_port, dbname="lum010_qual",
                          user="lum010_qual", password=data["pg_password"],
                          connect_timeout=5) as db:
        with db.cursor() as cur:
            cur.execute("""SELECT current_database(),current_user,
                current_setting('server_version_num')::int,inet_server_port(),
                current_setting('data_directory'),system_identifier::text
                FROM pg_control_system()""")
            name, role, version, server_port, directory, identifier = cur.fetchone()
    if (name != "lum010_qual" or role != "lum010_qual"
            or not 170000 <= version < 180000 or server_port != 5432
            or directory != "/var/lib/postgresql/data"
            or identifier != data["pg_system_identifier"]):
        raise RuntimeError("connected PostgreSQL cluster is not the disposable PG17 fixture")
    plugins = work / "plugins"
    temporary = work / "temp_audio"
    plugins.mkdir(exist_ok=True)
    temporary.mkdir(exist_ok=True)
    data["database_url"] = (
        "postgresql://lum010_qual:"
        + quote(data["pg_password"], safe="")
        + f"@127.0.0.1:{pg_port}/lum010_qual"
    )
    os.environ.update({
        "DATABASE_URL": data["database_url"],
        "POSTGRES_USER": "lum010_qual",
        "POSTGRES_PASSWORD": data["pg_password"],
        "POSTGRES_DB": "lum010_qual",
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": str(pg_port),
        "REDIS_URL": f"redis://127.0.0.1:{redis_port}/0",
        "AUTH_ENABLED": "true",
        "API_TOKEN": data["api_token"],
        "AUDIOMUSE_USER": "qual_admin",
        "AUDIOMUSE_PASSWORD": data["admin_password"],
        "JWT_SECRET": data["jwt_secret"],
        "MEDIASERVER_TYPE": "navidrome",
        "NAVIDROME_URL": f"http://127.0.0.1:{int(data['provider_port'])}",
        "NAVIDROME_USER": "qual_provider",
        "NAVIDROME_PASSWORD": data["provider_password"],
        "PLUGINS_ENABLED": "true",
        "PLUGINS_DIR": str(plugins),
        "PLUGIN_ALLOW_PIP": "false",
        "PLUGIN_DEFAULT_REPO_URL": "",
        "PLUGIN_REPOS": "[]",
        "PLUGIN_CATALOG_REFRESH_INTERVAL": "3600",
        "CLAP_ENABLED": "false",
        "LYRICS_ENABLED": "false",
        "ENABLE_PROXY_FIX": "false",
        "TEMP_DIR": str(temporary),
    })
    sys.path.insert(0, data["host_source"])
    return data
