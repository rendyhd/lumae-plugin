"""Real-loopback HTTP and PostgreSQL qualification against stock AudioMuse.

Run prepare, restart only host_http.py, then run resume. Secrets and session
tokens stay in the ignored private runtime directory, outside version control.
"""

import argparse
import datetime
import http.cookiejar
import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request

import psycopg2
import jwt

from runtime import configure


BASE = "/plugins/lumae_analysis/api"
SOURCE = None  # bound to the source the real host creates for its synthetic server


def connection(config):
    db = psycopg2.connect(config["database_url"], connect_timeout=5)
    with db.cursor() as cur:
        cur.execute("""SELECT current_database(),current_user,system_identifier::text
            FROM pg_control_system()""")
        if cur.fetchone() != (config["database_url"].rsplit("/", 1)[1], "lum010_no_host",
                              config["pg_system_identifier"]):
            db.close()
            raise RuntimeError("connected database is not the disposable fixture")
    db.rollback()
    return db


class Http:
    def __init__(self, config, *, saved=False, bearer=False):
        self.base = f"http://127.0.0.1:{config['host_port']}"
        self.bearer = config["api_token"] if bearer else None
        self.cookies = http.cookiejar.LWPCookieJar()
        self.cookie_path = pathlib.Path(config["work_dir"]) / "admin.cookies"
        if saved:
            self.cookies.load(self.cookie_path, ignore_discard=True)
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, request, file, code, msg, headers, newurl):
                return None

        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(self.cookies),
            NoRedirect())

    def call(self, path, body=None, *, headers=None, method=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base + path, data=data,
            headers={"Content-Type": "application/json",
                     **({"Authorization": "Bearer " + self.bearer} if self.bearer else {}),
                     **(headers or {})},
            method=method or ("POST" if body is not None else "GET"))
        try:
            response = self.opener.open(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        raw = response.read()
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            payload = None
        return response.status, payload

    def login(self, username, password):
        status, payload = self.call("/auth", {"user": username, "password": password},
                                    headers={"X-Requested-With": "XMLHttpRequest"})
        assert status == 200, (status, payload)


def post(client, operation, body):
    return client.call(BASE + "/profiles/bootstrap/sessions" + operation, body)


def request_body(**extra):
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": "source_scoped_v1",
            "catalog_instance_id": SOURCE, **extra}


def seed_initial(config):
    global SOURCE
    db = connection(config)
    with db.cursor() as cur:
        cur.execute("SELECT server_id FROM music_servers WHERE is_default")
        server = cur.fetchone()
        assert server, "synthetic default provider missing"
        cur.execute("""SELECT catalog_instance_id FROM plugin_lumae_analysis__catalog_sources
            WHERE current_core_server_id=%s AND rebind_status='active'""", server)
        sources = cur.fetchall()
        assert len(sources) == 1, "expected one host-created synthetic source"
        SOURCE = sources[0][0]
        cur.execute("""SELECT count(*) FROM plugin_lumae_analysis__catalog_tracks
            WHERE catalog_instance_id=%s""", (SOURCE,))
        assert cur.fetchone()[0] == 0, "qualification catalog fixture already exists"
        cur.execute("""UPDATE plugin_lumae_analysis__catalog_state
            SET published_generation=1,status='complete' WHERE catalog_instance_id=%s""", (SOURCE,))
        for track in ("track-a", "track-b"):
            cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_tracks
                (catalog_instance_id,published_generation,track_id,title,metadata_fp,
                 media_fp,payload,first_seen_at,last_seen_at)
                VALUES (%s,1,%s,%s,'synthetic-metadata',%s,'{}'::jsonb,now(),now())""",
                (SOURCE, track, track, f"synthetic-media-{track}"))
            cur.execute("""INSERT INTO plugin_lumae_analysis__published_source_profiles
                (catalog_instance_id,track_id,sample_rate,duration_ms,ref_lufs,
                 start_ramp,end_ramp,analyzer_ver,profile_schema_ver,media_signature,analyzed_at)
                VALUES (%s,%s,48000,210,-13,%s,%s,1,1,%s,now())""",
                (SOURCE, track, b"\x01\x02\x03", b"\x04\x05\x06",
                 f"synthetic-media-{track}"))
        cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_sources
            (catalog_instance_id,current_core_server_id,provider_type,server_name,is_default,rebind_status)
            VALUES ('qual-source-b','qual-server-b','navidrome','Second synthetic source',FALSE,'active')""")
        cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_state
            (catalog_instance_id,current_core_server_id,provider_type,
             published_generation,catalog_epoch,status)
            VALUES ('qual-source-b','qual-server-b','navidrome',1,'epoch-b','complete')""")
        cur.execute("""INSERT INTO plugin_lumae_analysis__profile_stream_state
            (catalog_instance_id,epoch,head_seq,floor_seq)
            VALUES ('qual-source-b','profile-epoch-b',0,0)""")
        cur.execute("""INSERT INTO plugin_lumae_analysis__published_source_profiles
            (catalog_instance_id,track_id,sample_rate,duration_ms,ref_lufs,
             start_ramp,end_ramp,analyzer_ver,profile_schema_ver,media_signature,analyzed_at)
            VALUES ('qual-source-b','track-a',48000,210,-18,%s,%s,1,1,'source-b-media',now())""",
            (b"\x01\x02\x03", b"\x04\x05\x06"))
    db.commit()
    db.close()


def prepare(config):
    seed_initial(config)
    client = Http(config, bearer=True)
    status, health = client.call(BASE + "/health")
    assert status == 200 and health["capabilities"]["profile_bootstrap"] == {
        "protocol_version": 2, "schema_version": 1,
        "auth": "host_authenticated", "transfer_contract": "source_scoped_v1",
        "available": True,
    }
    status, created = post(client, "", request_body(page_size=1))
    assert status == 200, (status, created)
    assert created["transfer_contract"] == "source_scoped_v1"
    assert "principal_binding" not in created
    assert created["snapshot_count"] == 2 and created["snapshot_seq"] == 0
    status, first = post(client, "/page", request_body(
        session_token=created["session_token"], page_token=created["next_page_token"]))
    assert status == 200 and [p["track_id"] for p in first["profiles"]] == ["track-a"]
    assert first["has_more"] and first["next_page_token"]
    account = Http(config)
    account.login("qual_admin", config["admin_password"])
    status, account_created = post(account, "", request_body(page_size=1))
    assert status == 200, (status, account_created)
    status, account_first = post(account, "/page", request_body(
        session_token=account_created["session_token"],
        page_token=account_created["next_page_token"]))
    assert status == 200 and account_first["profiles"] == first["profiles"]
    assert post(account, "/release", request_body(
        session_token=account_created["session_token"]))[0] == 200
    status, source_b = post(client, "", request_body(catalog_instance_id="qual-source-b"))
    assert status == 200 and source_b["snapshot_count"] == 1
    status, b_page = post(client, "/page", request_body(
        catalog_instance_id="qual-source-b", session_token=source_b["session_token"]))
    assert status == 200 and b_page["profiles"][0]["track_id"] == "track-a"
    assert b_page["profiles"][0]["ref_lufs"] == -18
    assert post(client, "/release", request_body(
        catalog_instance_id="qual-source-b", session_token=source_b["session_token"]))[0] == 200
    checkpoint = pathlib.Path(config["work_dir"]) / "http.checkpoint.json"
    host_pid = int((pathlib.Path(config["work_dir"]) / "host.pid").read_text())
    checkpoint.write_text(json.dumps({"created": created, "first": first,
                                      "host_pid": host_pid}), encoding="utf-8")
    print("prepare: stock bearer/account auth, two-source capability, durable first page PASS")


def append_change(config, track, operation):
    db = connection(config)
    with db.cursor() as cur:
        cur.execute("""SELECT epoch,head_seq FROM plugin_lumae_analysis__profile_stream_state
            WHERE catalog_instance_id=%s FOR UPDATE""", (SOURCE,))
        epoch, head = cur.fetchone()
        payload = None
        if operation == "upsert":
            cur.execute("""INSERT INTO plugin_lumae_analysis__published_source_profiles
                (catalog_instance_id,track_id,sample_rate,duration_ms,ref_lufs,
                 start_ramp,end_ramp,analyzer_ver,profile_schema_ver,media_signature,analyzed_at)
                VALUES (%s,%s,48000,210,-12,%s,%s,1,1,%s,now())
                ON CONFLICT (catalog_instance_id,track_id) DO UPDATE
                SET ref_lufs=EXCLUDED.ref_lufs,analyzed_at=EXCLUDED.analyzed_at""",
                (SOURCE, track, b"\x01\x02\x03", b"\x04\x05\x06",
                 f"synthetic-media-{track}"))
            from plugins.LumaeAnalysis.catalog_enrichment import serialize_profile
            cur.execute("""SELECT track_id,sample_rate,duration_ms,ref_lufs,start_ramp,
                end_ramp,analyzer_ver,analyzed_at,media_signature
                FROM plugin_lumae_analysis__published_source_profiles
                WHERE catalog_instance_id=%s AND track_id=%s""", (SOURCE, track))
            payload = serialize_profile(*cur.fetchone())
        else:
            cur.execute("""DELETE FROM plugin_lumae_analysis__published_source_profiles
                WHERE catalog_instance_id=%s AND track_id=%s""", (SOURCE, track))
        from psycopg2.extras import Json
        cur.execute("""INSERT INTO plugin_lumae_analysis__profile_changes
            (catalog_instance_id,epoch,seq,track_id,operation,payload)
            VALUES (%s,%s,%s,%s,%s,%s)""",
            (SOURCE, epoch, head + 1, track, operation, Json(payload) if payload else None))
        cur.execute("""UPDATE plugin_lumae_analysis__profile_stream_state
            SET head_seq=%s,updated_at=now() WHERE catalog_instance_id=%s""",
            (head + 1, SOURCE))
    db.commit()
    db.close()


def resume(config):
    global SOURCE
    checkpoint = json.loads((pathlib.Path(config["work_dir"]) / "http.checkpoint.json")
                            .read_text(encoding="utf-8"))
    created, first = checkpoint["created"], checkpoint["first"]
    current_host_pid = int((pathlib.Path(config["work_dir"]) / "host.pid").read_text())
    if checkpoint["host_pid"] == current_host_pid:
        raise RuntimeError("disposable host process has not restarted")
    SOURCE = created["catalog_instance_id"]
    token = created["session_token"]
    client = Http(config, bearer=True)
    status, health = client.call(BASE + "/health")
    assert status == 200 and health["status"] == "ok", (status, health)
    first_body = request_body(session_token=token, page_token=created["next_page_token"])
    status, replay = post(client, "/page", first_body)
    assert status == 200 and replay == first
    assert first["profiles"][0]["media_revision"].startswith("sha256:")
    assert first["profiles"][0]["start_ramp"] and first["profiles"][0]["end_ramp"]
    status, second = post(client, "/page", request_body(
        session_token=token, page_token=first["next_page_token"]))
    assert status == 200 and [p["track_id"] for p in second["profiles"]] == ["track-b"]
    assert not second["has_more"] and second["next_page_token"] is None
    assert first["snapshot_cursor"] == second["snapshot_cursor"] == created["snapshot_cursor"]

    db = connection(config)
    with db.cursor() as cur:
        cur.execute("SELECT head_seq FROM plugin_lumae_analysis__profile_stream_state "
                    "WHERE catalog_instance_id=%s", (SOURCE,))
        head = cur.fetchone()[0]
    db.close()
    if head == 0:
        append_change(config, "track-c", "upsert")
        append_change(config, "track-b", "delete")
    else:
        assert head in (2, 3), f"unexpected fixture head {head}"
    status, catchup1 = post(client, "/catchup", request_body(session_token=token))
    assert status == 200 and [c["seq"] for c in catchup1["changes"]] == [1]
    assert catchup1["changes"][0]["operation"] == "upsert"
    assert catchup1["changes"][0]["payload"]["track_id"] == "track-c"
    assert catchup1["changes"][0]["payload"]["media_revision"].startswith("sha256:")
    assert catchup1["has_more"]
    if head != 3:
        append_change(config, "track-d", "upsert")
    status, catchup2 = post(client, "/catchup", request_body(
        session_token=token, page_token=catchup1["next_page_token"]))
    assert status == 200 and [c["seq"] for c in catchup2["changes"]] == [2]
    assert catchup2["changes"][0]["operation"] == "delete"
    assert catchup2["changes"][0]["payload"] is None
    assert catchup2["cursor"] == catchup2["head_cursor"] == catchup1["head_cursor"]
    assert not catchup2["has_more"]
    assert post(client, "/catchup", request_body(session_token=token)) == (200, catchup1)

    anonymous = Http(config)
    status, _ = post(anonymous, "", request_body())
    assert status in (302, 401), status
    status, _ = anonymous.call(
        BASE + "/profiles/bootstrap/sessions", request_body(),
        headers={"Authorization": "Bearer invalid-disposable-token"})
    assert status in (302, 401), status
    for suffix in ("/page", "/catchup", "/release"):
        status, _ = post(client, suffix, request_body(
            catalog_instance_id="qual-source-b", session_token=token))
        assert status == 410, (suffix, status)
    status, malformed = post(client, "/page", request_body(
        session_token=token, page_token="tampered"))
    assert status == 400, (status, malformed)
    status, wrong_version = post(client, "", {**request_body(), "protocol_version": 3})
    assert status == 400, (status, wrong_version)

    status, _ = client.call("/api/users", {
        "username": "qual_other", "password": config["other_password"],
        "role": "user", "current_password": config["admin_password"]})
    assert status == 201, status
    other = Http(config)
    other.login("qual_other", config["other_password"])
    status, other_page = post(other, "/page", first_body)
    assert status == 200 and other_page == first
    status, release = post(other, "/release", request_body(session_token=token))
    assert status == 200 and release["released"]
    status, _ = post(client, "/page", first_body)
    assert status == 410, status

    db = connection(config)
    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM plugin_lumae_analysis__profile_bootstrap_sessions")
        assert cur.fetchone()[0] == 0
        cur.execute("""SELECT ref_lufs FROM plugin_lumae_analysis__published_source_profiles
            WHERE catalog_instance_id='qual-source-b' AND track_id='track-a'""")
        assert cur.fetchone()[0] == -18
    db.close()
    print("resume: persisted cookie/session, exact page replay, finite head, later event exclusion, "
          "bearer/account shared continuation, source fencing, malformed token/version, "
          "release, owned lock cleanup PASS")


def delta(config):
    checkpoint = json.loads((pathlib.Path(config["work_dir"]) / "http.checkpoint.json")
                            .read_text(encoding="utf-8"))
    created = checkpoint["created"]
    from plugins.LumaeAnalysis.catalog import opaque_cursor
    cursor = opaque_cursor(created["catalog_instance_id"], created["profile_epoch"], 2)
    query = urllib.parse.urlencode({"catalog_instance_id": created["catalog_instance_id"],
                                    "cursor": cursor})
    status, page = Http(config, bearer=True).call(BASE + "/profiles/changes?" + query)
    assert status == 200, (status, page)
    assert [(change["seq"], change["track_id"]) for change in page["changes"]] == [
        (3, "track-d")]
    print("stock bearer subsequent delta receives post-capture event PASS")


def append_delta(config):
    global SOURCE
    checkpoint = json.loads((pathlib.Path(config["work_dir"]) / "http.checkpoint.json")
                            .read_text(encoding="utf-8"))
    SOURCE = checkpoint["created"]["catalog_instance_id"]
    append_change(config, "track-d", "upsert")
    print("disposable post-publication profile event appended")


def auth_failures(config):
    global SOURCE
    checkpoint = json.loads((pathlib.Path(config["work_dir"]) / "http.checkpoint.json")
                            .read_text(encoding="utf-8"))
    SOURCE = checkpoint["created"]["catalog_instance_id"]
    db = connection(config)
    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM plugin_lumae_analysis__profile_bootstrap_sessions")
        before = cur.fetchone()[0]
    db.rollback()
    expired = jwt.encode({"sub": "qual_admin",
                          "iat": datetime.datetime.now(datetime.timezone.utc)
                          - datetime.timedelta(hours=2),
                          "exp": datetime.datetime.now(datetime.timezone.utc)
                          - datetime.timedelta(hours=1)}, config["jwt_secret"], algorithm="HS256")
    for headers in ({"Authorization": "Bearer invalid-disposable-token"},
                    {"Cookie": "audiomuse_jwt=invalid"},
                    {"Cookie": "audiomuse_jwt=" + expired}):
        status, _ = post_with_headers(config, headers)
        assert status in (302, 401), status
    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM plugin_lumae_analysis__profile_bootstrap_sessions")
        assert cur.fetchone()[0] == before
    db.close()
    print("stock invalid bearer, invalid cookie, expired cookie blocked before transfer PASS")


def post_with_headers(config, headers):
    return Http(config).call(BASE + "/profiles/bootstrap/sessions", request_body(),
                             headers=headers)


def empty_snapshot(config):
    """Qualify an authoritative empty source without changing populated fixtures."""
    source = "qual-source-empty"
    db = connection(config)
    with db.cursor() as cur:
        cur.execute("""SELECT count(*) FROM plugin_lumae_analysis__catalog_sources
            WHERE catalog_instance_id=%s""", (source,))
        if cur.fetchone()[0] == 0:
            cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_sources
                (catalog_instance_id,current_core_server_id,provider_type,
                 server_name,is_default,rebind_status)
                VALUES (%s,'qual-server-empty','navidrome',
                        'Empty synthetic source',FALSE,'active')""", (source,))
            cur.execute("""INSERT INTO plugin_lumae_analysis__catalog_state
                (catalog_instance_id,current_core_server_id,provider_type,
                 published_generation,catalog_epoch,status)
                VALUES (%s,'qual-server-empty','navidrome',1,'epoch-empty','complete')""",
                (source,))
            cur.execute("""INSERT INTO plugin_lumae_analysis__profile_stream_state
                (catalog_instance_id,epoch,head_seq,floor_seq)
                VALUES (%s,'profile-epoch-empty',0,0)""", (source,))
    db.commit()
    db.close()
    client = Http(config, bearer=True)
    status, created = post(client, "", request_body(catalog_instance_id=source))
    assert status == 200 and created["snapshot_count"] == 0, (status, created)
    status, page = post(client, "/page", request_body(
        catalog_instance_id=source, session_token=created["session_token"],
        page_token=created["next_page_token"]))
    assert status == 200 and page["profiles"] == [] and not page["has_more"]
    status, catchup = post(client, "/catchup", request_body(
        catalog_instance_id=source, session_token=created["session_token"]))
    assert status == 200 and catchup["changes"] == [] and not catchup["has_more"]
    assert post(client, "/release", request_body(
        catalog_instance_id=source, session_token=created["session_token"]))[0] == 200
    print("stock bearer authoritative empty source PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "resume", "delta", "append_delta",
                                          "auth_failures", "empty_snapshot"))
    args = parser.parse_args()
    config = configure()
    sys.path.insert(0, config["plugin_source"])
    if args.phase == "prepare":
        prepare(config)
    elif args.phase == "resume":
        resume(config)
    elif args.phase == "delta":
        delta(config)
    elif args.phase == "auth_failures":
        auth_failures(config)
    elif args.phase == "empty_snapshot":
        empty_snapshot(config)
    else:
        append_delta(config)
