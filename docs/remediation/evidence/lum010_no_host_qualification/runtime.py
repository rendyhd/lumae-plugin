"""Confine stock-host qualification to one disposable PG17 and loopback host."""

import json
import os
import pathlib
import subprocess
import sys
from urllib.parse import quote

import psycopg2


TARGETS = {
    "baseline": ("f100684b1753e303f2b698b4f56f5b1b8972a5bb",
                 "AudioMuse-AI", "lum010_no_host", 55275, 55276),
    "main": ("ce742938e5ad86be85b978effa1375c4e3e9e633",
             ".pytest-tmp/AudioMuse-current-main", "lum010_main", 55277, 55278),
}


def configure():
    source = pathlib.Path(__file__).resolve().parents[4]
    runtime = pathlib.Path(os.environ["LUM010_NO_HOST_RUNTIME"]).resolve()
    baseline_work = (source / ".pytest-tmp" / "lum010-no-host-runtime").resolve()
    main_work = (source / ".pytest-tmp" / "lum010-no-host-main-runtime").resolve()
    target = ("baseline" if runtime == baseline_work / "runtime.json" else
              "main" if runtime == main_work / "runtime.json" else None)
    if target is None:
        raise RuntimeError("runtime file is outside the disposable qualification directory")
    sha, host_path, database_name, host_port, provider_port = TARGETS[target]
    host = ((source.parent / host_path) if target == "baseline" else
            (source / host_path)).resolve()
    work = baseline_work if target == "baseline" else main_work
    data = json.loads(runtime.read_text(encoding="utf-8"))
    if data["work_dir"] != str(work) or data["host_source"] != str(host):
        raise RuntimeError("qualification paths are not the designated disposable paths")
    if (subprocess.check_output(["git", "-C", str(host), "rev-parse", "HEAD"],
                                text=True).strip() != sha
            or subprocess.check_output(
                ["git", "-C", str(host), "status", "--porcelain", "--untracked-files=no"],
                text=True).strip()):
        raise RuntimeError("stock AudioMuse checkout changed")
    if (int(data["pg_port"]) != 55273 or int(data["redis_port"]) != 55274
            or int(data["host_port"]) != host_port
            or int(data["provider_port"]) != provider_port):
        raise RuntimeError("qualification port changed")
    database_url = ("postgresql://lum010_no_host:"
                    + quote(data["pg_password"], safe="")
                    + f"@127.0.0.1:55273/{database_name}")
    with psycopg2.connect(database_url, connect_timeout=5) as db:
        with db.cursor() as cur:
            cur.execute("""SELECT current_database(),current_user,
                current_setting('server_version_num')::int,
                current_setting('data_directory'),system_identifier::text
                FROM pg_control_system()""")
            name, role, version, directory, identifier = cur.fetchone()
    if (name != database_name or role != "lum010_no_host"
            or not 170000 <= version < 180000
            or directory != "/var/lib/postgresql/data"
            or identifier != data["pg_system_identifier"]):
        raise RuntimeError("connected database is not the disposable PG17 fixture")
    plugins = work / "plugins"
    temporary = work / "temp_audio"
    plugins.mkdir(exist_ok=True)
    temporary.mkdir(exist_ok=True)
    os.environ.update({
        "DATABASE_URL": database_url,
        "POSTGRES_USER": "lum010_no_host",
        "POSTGRES_PASSWORD": data["pg_password"],
        "POSTGRES_DB": database_name,
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": "55273",
        "REDIS_URL": "redis://127.0.0.1:55274/0",
        "AUTH_ENABLED": "true",
        "API_TOKEN": data["api_token"],
        "AUDIOMUSE_USER": "qual_admin",
        "AUDIOMUSE_PASSWORD": data["admin_password"],
        "JWT_SECRET": data["jwt_secret"],
        "MEDIASERVER_TYPE": "navidrome",
        "NAVIDROME_URL": f"http://127.0.0.1:{provider_port}",
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
    data["database_url"] = database_url
    data["plugin_source"] = str(source)
    sys.path.insert(0, str(host))
    return data
