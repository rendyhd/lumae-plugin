"""Characterize an active v2 session across a disposable PG dump/restore clone."""

import argparse
import hashlib
import json
import pathlib

import psycopg2

from http_qualification import Http, post
from runtime import configure


def body(source, **extra):
    return {"protocol_version": 2, "schema_version": 1,
            "catalog_instance_id": source, **extra}


def before_dump(config):
    source = json.loads((pathlib.Path(config["work_dir"]) / "http.checkpoint.json")
                        .read_text(encoding="utf-8"))["created"]["catalog_instance_id"]
    client = Http(config)
    binding = client.login("qual_admin", config["admin_password"])
    status, created = post(client, "", body(source, page_size=1))
    if status != 200 or created["principal_binding"] != binding:
        raise RuntimeError("disposable source session was not created")
    status, first = post(client, "/page", body(source,
        session_token=created["session_token"], page_token=created["next_page_token"]))
    if status != 200 or not first["profiles"]:
        raise RuntimeError("disposable source first page failed")
    client.cookies.save(client.cookie_path, ignore_discard=True)
    checkpoint = pathlib.Path(config["work_dir"]) / "clone.checkpoint.json"
    checkpoint.write_text(json.dumps({"created": created, "first": first}), encoding="utf-8")
    print("clone before-dump: active account-bound session and snapshot page captured")


def after_restore(config):
    checkpoint = json.loads((pathlib.Path(config["work_dir"]) / "clone.checkpoint.json")
                            .read_text(encoding="utf-8"))
    created, first = checkpoint["created"], checkpoint["first"]
    source, token = created["catalog_instance_id"], created["session_token"]
    clone_url = config["database_url"].rsplit("/", 1)[0] + "/lum010_clone"
    with psycopg2.connect(clone_url, connect_timeout=5) as db:
        with db.cursor() as cur:
            cur.execute("""SELECT current_database(),current_user,system_identifier::text
                FROM pg_control_system()""")
            if cur.fetchone() != ("lum010_clone", "lum010_qual",
                                  config["pg_system_identifier"]):
                raise RuntimeError("connected database is not the disposable clone")
            cur.execute("""SELECT principal,catalog_epoch,profile_epoch,snapshot_seq,snapshot_count
                FROM plugin_lumae_analysis__profile_bootstrap_sessions
                WHERE token_hash=%s""", (hashlib.sha256(token.encode()).hexdigest(),))
            row = cur.fetchone()
            if row is None or row[0] != created["principal_binding"] or row[1] != created["catalog_epoch"]:
                raise RuntimeError("clone lost the account-bound snapshot session")
            if row[2:] != (created["profile_epoch"], created["snapshot_seq"],
                           created["snapshot_count"]):
                raise RuntimeError("clone changed the profile snapshot frontier")
    client = Http({**config, "host_port": config["clone_port"]}, saved=True)
    status, replay = post(client, "/page", body(source,
        session_token=token, page_token=created["next_page_token"]))
    if status != 200 or replay != first:
        raise RuntimeError(f"clone HTTP replay mismatch: status {status}")
    status, _ = post(client, "/release", body(source, session_token=token))
    if status != 200:
        raise RuntimeError("clone release failed")
    original = Http(config, saved=True)
    status, original_replay = post(original, "/page", body(source,
        session_token=token, page_token=created["next_page_token"]))
    if status != 200 or original_replay != first:
        raise RuntimeError("clone release affected original database session")
    status, _ = post(original, "/release", body(source, session_token=token))
    if status != 200:
        raise RuntimeError("original session release failed")
    print("clone after-restore: copied principal/epoch/frontier, exact HTTP replay, "
          "and independent clone/original release PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("before_dump", "after_restore"))
    args = parser.parse_args()
    config = configure()
    if args.phase == "before_dump":
        before_dump(config)
    else:
        after_restore(config)
