"""Exercise stock bearer rotation without changing host source or production config."""

import argparse
import json
import os
import pathlib
import secrets

import psycopg2

import http_qualification as http
from runtime import configure


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "apply_db", "verify"))
    phase = parser.parse_args().phase
    config = configure()
    work = pathlib.Path(config["work_dir"])
    runtime_path = pathlib.Path(os.environ["LUM010_NO_HOST_RUNTIME"])
    http.SOURCE = json.loads((work / "http.checkpoint.json").read_text(encoding="utf-8"))[
        "created"]["catalog_instance_id"]
    rotation_path = work / "rotation.json"
    if phase == "prepare":
        if rotation_path.exists():
            raise RuntimeError("rotation fixture already exists")
        client = http.Http(config, bearer=True)
        status, created = http.post(client, "", http.request_body(page_size=1))
        assert status == 200, (status, created)
        status, first = http.post(client, "/page", http.request_body(
            session_token=created["session_token"], page_token=created["next_page_token"]))
        assert status == 200
        rotation_path.write_text(json.dumps({"created": created, "first": first,
                                             "old_token": config["api_token"]}),
                                 encoding="utf-8")
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
        runtime["api_token"] = secrets.token_urlsafe(32)
        runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
        print("disposable bearer rotation prepared; restart stock host before verify")
    elif phase == "apply_db":
        rotation = json.loads(rotation_path.read_text(encoding="utf-8"))
        with psycopg2.connect(config["database_url"], connect_timeout=5) as db:
            with db.cursor() as cur:
                cur.execute("SELECT value FROM app_config WHERE key='API_TOKEN' FOR UPDATE")
                row = cur.fetchone()
                if row != (rotation["old_token"],):
                    raise RuntimeError("disposable stored token is not the expected old value")
                cur.execute("UPDATE app_config SET value=%s,updated_at=now() "
                            "WHERE key='API_TOKEN'", (config["api_token"],))
        print("disposable stock app_config bearer rotated; restart host before verify")
    else:
        rotation = json.loads(rotation_path.read_text(encoding="utf-8"))
        created = rotation["created"]
        body = http.request_body(session_token=created["session_token"],
                                 page_token=created["next_page_token"])
        status, _ = http.Http(config).call(
            http.BASE + "/profiles/bootstrap/sessions/page", body,
            headers={"Authorization": "Bearer " + rotation["old_token"]})
        assert status in (302, 401), status
        current = http.Http(config, bearer=True)
        status, page = http.post(current, "/page", body)
        assert status == 200 and page == rotation["first"]
        status, release = http.post(current, "/release", http.request_body(
            session_token=created["session_token"]))
        assert status == 200 and release["released"]
        print("stock rejected rotated bearer; new valid bearer resumed shared-source session PASS")


if __name__ == "__main__":
    main()
