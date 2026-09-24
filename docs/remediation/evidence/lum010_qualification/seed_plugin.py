"""Seed only the disposable database and plugin cache from frozen sources."""

import importlib
import filecmp
import json
import pathlib
import shutil

from runtime import configure


def main():
    config = configure()
    work = pathlib.Path(config["work_dir"])
    target = work / "plugins" / "lumae_analysis"
    source = pathlib.Path(config["plugin_source"]) / "plugins" / "LumaeAnalysis"
    if target.exists():
        source_files = {path.relative_to(source) for path in source.rglob("*")
                        if path.is_file() and not any(part in ("__pycache__", ".pytest_cache")
                                                       for part in path.parts)}
        target_files = {path.relative_to(target) for path in target.rglob("*")
                        if path.is_file() and not any(part in ("__pycache__", ".pytest_cache")
                                                       for part in path.parts)}
        if source_files != target_files or any(
            not filecmp.cmp(source / relative, target / relative, shallow=False)
            for relative in source_files
        ):
            raise RuntimeError("qualification plugin cache differs from frozen source")
    else:
        shutil.copytree(source, target,
                        ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"))

    from flask_app import app
    import database
    from plugin.manager import plugin_manager
    from psycopg2.extras import Json

    with app.app_context():
        database.init_db()
        database.init_db()  # host schema migration must be idempotent
        plugin_manager.setup_namespace()
        plugin = importlib.import_module("audiomuse_plugins.lumae_analysis")
        db = database.get_db()
        plugin.migrate(db)
        db.commit()
        plugin.migrate(db)  # plugin migration must be idempotent
        db.commit()
        manifest = json.loads((source / "plugin.json").read_text(encoding="utf-8"))
        with db.cursor() as cur:
            expected_provider = {
                "url": f"http://127.0.0.1:{config['provider_port']}",
                "user": "qual_provider", "password": config["provider_password"],
            }
            cur.execute("SELECT server_type,creds FROM music_servers WHERE is_default")
            existing_provider = cur.fetchone()
            if existing_provider is None:
                cur.execute(
                    """INSERT INTO music_servers
                       (server_id,name,server_type,creds,is_default)
                       VALUES (%s,%s,'navidrome',%s,TRUE)""",
                    ("qual-server", "Disposable unreachable provider", Json(expected_provider)),
                )
            elif existing_provider != ("navidrome", expected_provider):
                raise RuntimeError("disposable default provider differs from expected loopback fixture")
            cur.execute(
                """INSERT INTO plugins
                   (id,name,version,manifest,requirements,enabled,load_status)
                   VALUES (%s,%s,%s,%s,%s,TRUE,NULL)
                   ON CONFLICT (id) DO NOTHING""",
                ("lumae_analysis", manifest["name"], manifest.get("version", "1.2.5"),
                 Json(manifest), Json(manifest.get("requirements", []))),
            )
        db.commit()
        database.close_db()
    print("disposable host and plugin migrations: first and second passes completed")


if __name__ == "__main__":
    main()
