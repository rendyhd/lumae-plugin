"""Disposable real-host LUM-010 account integration; run with explicit env.

Requires LUMAE_HOST_SOURCE and LUMAE_POSTGRES_TEST_DSN to point to the reviewed
isolated host checkout and a disposable PostgreSQL database. Never uses the
normal AudioMuse configuration or provider stack.
"""

import importlib.util
import os
import pathlib
import subprocess
import sys
import types
import uuid

EXPECTED_HOST = pathlib.Path(
    r"C:\Users\rendy\vscode\AudioMuse-AI\.codex-worktrees\lum010-host-plugin-api"
).resolve()
EXPECTED_HOST_SHA = "3d6b40c8d8417e6907ca8dfc645ea19fd2a552ca"
EXPECTED_DSN = {
    "host": "127.0.0.1", "port": "44794", "dbname": "host_adapter_test",
    "user": "postgres", "password": "host_adapter_disposable_20260923",
}
EXPECTED_SERVER = ("host_adapter_test", "postgres", "10.10.0.3/32", 5432)


def refuse(reason):
    raise RuntimeError(f"Refusing disposable host integration: {reason}")


def git_output(path, *args):
    result = subprocess.run(["git", "-C", str(path), *args],
                            capture_output=True, text=True, check=False)
    if result.returncode:
        refuse("reviewed host Git state could not be verified")
    return result.stdout.strip()


host_source = pathlib.Path(os.environ["LUMAE_HOST_SOURCE"]).resolve()
dsn = os.environ["LUMAE_POSTGRES_TEST_DSN"]
if host_source != EXPECTED_HOST:
    refuse("host source is not the isolated reviewed worktree")
if git_output(host_source, "rev-parse", "HEAD") != EXPECTED_HOST_SHA:
    refuse("host source revision differs from the reviewed commit")
if git_output(host_source, "status", "--porcelain"):
    refuse("host source worktree is not clean")

import psycopg2
from psycopg2.extensions import parse_dsn

try:
    parsed_dsn = parse_dsn(dsn)
except psycopg2.Error:
    refuse("invalid disposable database DSN")
if parsed_dsn != EXPECTED_DSN:
    refuse("database DSN is not the exact disposable endpoint and credential")

os.environ.update(DATABASE_URL=dsn, AUTH_ENABLED="true",
                  API_TOKEN="disposable-integration-bearer",
                  JWT_SECRET="disposable-integration-jwt-secret-20260923")
sys.path.insert(0, str(host_source))

from flask import Flask, g, request
import app_auth
import database
import plugin.api as host_api

source = pathlib.Path(__file__).resolve().parents[3] / "plugins" / "LumaeAnalysis"
namespace = types.ModuleType("audiomuse_plugins")
namespace.__path__ = []
sys.modules["audiomuse_plugins"] = namespace
spec = importlib.util.spec_from_file_location(
    "audiomuse_plugins.lumae_analysis", source / "__init__.py",
    submodule_search_locations=[str(source)])
lumae = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = lumae
spec.loader.exec_module(lumae)
from audiomuse_plugins.lumae_analysis import catalog, catalog_enrichment, edge_profile_store

schema = "lum010_host_" + uuid.uuid4().hex
bootstrap_path = "/plugins/lumae_analysis/api/profiles/bootstrap/sessions"
body = {"protocol_version": 2, "schema_version": 1,
        "catalog_instance_id": "catalog-a"}


def make_app():
    app = Flask(__name__)
    app.secret_key = "disposable-flask-secret"
    app_auth.check_setup_needed = lambda: False
    app_auth.init_app(app, None, lambda: os.environ["JWT_SECRET"])
    @app.before_request
    def record_request_transaction():
        if request.path.startswith(bootstrap_path) and getattr(g, "auth_method", None) == "session":
            with database.get_db().cursor() as cur:
                cur.execute("SELECT pg_backend_pid(), txid_current()")
                g.integration_request_transaction = cur.fetchone()

    @app.after_request
    def assert_request_transaction_untouched(response):
        prior = getattr(g, "integration_request_transaction", None)
        if prior is not None:
            with database.get_db().cursor() as cur:
                cur.execute("SELECT pg_backend_pid(), txid_current()")
                assert cur.fetchone() == prior
        return response
    app.register_blueprint(lumae.bp, url_prefix="/plugins/lumae_analysis")
    app.teardown_appcontext(database.close_db)
    return app


def login(client, password):
    response = client.post("/auth", json={"user": "integration", "password": password},
                           headers={"X-Requested-With": "XMLHttpRequest"})
    assert response.status_code == 200, (response.status_code, response.json)


def post(client, path, *, headers=None, **extra):
    return client.post(bootstrap_path + path, json={**body, **extra}, headers=headers)


admin = psycopg2.connect(dsn)
with admin.cursor() as cur:
    cur.execute("SELECT current_database(), current_user, "
                "inet_server_addr()::text, inet_server_port()")
    if cur.fetchone() != EXPECTED_SERVER:
        refuse("connected PostgreSQL server does not match the disposable instance")
    cur.execute("SELECT s.setconfig FROM pg_db_role_setting s "
                "JOIN pg_database d ON d.oid=s.setdatabase "
                "JOIN pg_roles r ON r.oid=s.setrole "
                "WHERE d.datname=current_database() AND r.rolname=current_user")
    row = cur.fetchone()
    if row and any(setting.startswith("search_path=") for setting in row[0]):
        refuse("disposable role already has a database-specific search_path")
admin.rollback()
schema_created = False
role_changed = False
try:
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        schema_created = True
        cur.execute(f"ALTER ROLE postgres IN DATABASE host_adapter_test "
                    f"SET search_path TO {schema}, public")
        role_changed = True
    app = make_app()
    with app.app_context():
        db = database.get_db()
        with db.cursor() as cur:
            cur.execute("CREATE TABLE audiomuse_users (id SERIAL PRIMARY KEY, "
                        "username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, "
                        "role TEXT NOT NULL DEFAULT 'user', created_at TIMESTAMP DEFAULT now(), "
                        "password_changed_at TIMESTAMP)")
            database.migrate_user_principals(cur)
        db.commit()
        assert app_auth.create_additional_user("integration", "pass-one", "admin")[0]
        catalog.migrate_catalog(db)
        with db.cursor() as cur:
            cur.execute("INSERT INTO plugin_lumae_analysis__catalog_sources "
                        "(catalog_instance_id,current_core_server_id,provider_type,server_name,is_default,rebind_status) "
                        "VALUES ('catalog-a','server-a','navidrome','Disposable',TRUE,'active')")
            cur.execute("INSERT INTO plugin_lumae_analysis__catalog_state "
                        "(catalog_instance_id,current_core_server_id,provider_type,published_generation,catalog_epoch,status) "
                        "VALUES ('catalog-a','server-a','navidrome',1,'epoch-a','complete')")
            cur.execute("CREATE TABLE plugin_lumae_analysis__published_source_profiles "
                        "(catalog_instance_id TEXT NOT NULL,track_id TEXT NOT NULL,sample_rate INTEGER NOT NULL,"
                        "duration_ms INTEGER NOT NULL,ref_lufs REAL NOT NULL,start_ramp BYTEA NOT NULL,"
                        "end_ramp BYTEA NOT NULL,analyzer_ver INTEGER NOT NULL,profile_schema_ver INTEGER NOT NULL,"
                        "media_signature TEXT,analyzed_at TIMESTAMP NOT NULL,PRIMARY KEY(catalog_instance_id,track_id))")
            cur.execute("INSERT INTO plugin_lumae_analysis__published_source_profiles "
                        "VALUES ('catalog-a','track-a',48000,210,-13,%s,%s,1,1,'revision',now())",
                        (b"first", b"last"))
        catalog_enrichment.migrate_enrichment(db)
        edge_profile_store.migrate_edge_profiles(db)
        db.commit()
        with db.cursor() as cur:
            cur.execute("SELECT pg_backend_pid(), current_schema(), current_user")
            request_pid, request_schema, request_role = cur.fetchone()
        db.rollback()
        with host_api.open_db_connection(isolation="repeatable_read") as lease:
            with lease.cursor() as cur:
                cur.execute("SELECT pg_backend_pid(), current_schema(), current_user")
                lease_pid, lease_schema, lease_role = cur.fetchone()
            assert lease_pid != request_pid
            assert lease_schema == request_schema == schema
            assert lease_role == request_role == "postgres"
        print("owned lease: separate backend, expected schema and role")

    bearer = app.test_client()
    with app.app_context():
        with database.get_db().cursor() as cur:
            cur.execute("SELECT count(*) FROM plugin_lumae_analysis__profile_bootstrap_sessions")
            before_bearer = cur.fetchone()[0]
        database.get_db().rollback()
    rejected = post(bearer, "", headers={"Authorization": "Bearer disposable-integration-bearer"})
    assert rejected.status_code == 401, (rejected.status_code, rejected.json)
    with app.app_context():
        with database.get_db().cursor() as cur:
            cur.execute("SELECT count(*) FROM plugin_lumae_analysis__profile_bootstrap_sessions")
            assert cur.fetchone()[0] == before_bearer
        database.get_db().rollback()
    print("bearer-only v2: 401")

    client = app.test_client()
    login(client, "pass-one")
    created_response = post(client, "")
    assert created_response.status_code == 200, (created_response.status_code, created_response.json)
    created = created_response.json
    token = created["session_token"]
    page = post(client, "/page", session_token=token)
    assert page.status_code == 200, (page.status_code, page.json)
    assert [row["track_id"] for row in page.json["profiles"]] == ["track-a"]
    assert post(client, "/page", session_token=token).json == page.json
    with app.app_context():
        with database.get_db().cursor() as cur:
            catalog_enrichment.record_profile_change(cur, "catalog-a", "track-removed", "deleted")
        database.get_db().commit()
    catchup = post(client, "/catchup", session_token=token)
    assert catchup.status_code == 200 and [c["operation"] for c in catchup.json["changes"]] == ["delete"]
    assert post(client, "/catchup", session_token=token).json == catchup.json

    with app.app_context():
        assert app_auth.create_additional_user("second", "second-pass", "admin")[0]
    second = app.test_client()
    second_login = second.post("/auth", json={"user": "second", "password": "second-pass"},
                               headers={"X-Requested-With": "XMLHttpRequest"})
    assert second_login.status_code == 200
    assert post(second, "/page", session_token=token).status_code == 410

    with app.app_context():
        with database.get_db().cursor() as cur:
            cur.execute("UPDATE audiomuse_users SET role='user' WHERE username='integration'")
        database.get_db().commit()
    assert post(client, "/page", session_token=token).status_code == 200

    released = post(client, "")
    assert released.status_code == 200
    assert post(client, "/release", session_token=released.json["session_token"]).status_code == 200

    restarted = make_app().test_client()
    login(restarted, "pass-one")
    assert post(restarted, "/page", session_token=token).json == page.json
    print("account create/page/replay/finite catch-up/release, cross-account and role change: passed")
    print("request transaction and ordinary app restart: passed")

    with app.app_context():
        db = database.get_db()
        with db.cursor() as cur:
            cur.execute("SELECT id, principal_id, authorization_generation FROM audiomuse_users "
                        "WHERE username='integration'")
            user_id, subject, generation = cur.fetchone()
        db.rollback()
        assert app_auth.update_additional_user_password(user_id, "pass-two")[0]
        with db.cursor() as cur:
            cur.execute("SELECT principal_id, authorization_generation FROM audiomuse_users "
                        "WHERE id=%s", (user_id,))
            next_subject, next_generation = cur.fetchone()
        assert next_subject == subject and next_generation != generation
        db.rollback()
    changed = app.test_client()
    login(changed, "pass-two")
    assert post(changed, "/page", session_token=token).status_code == 410
    new_session = post(changed, "")
    assert new_session.status_code == 200, (new_session.status_code, new_session.json)
    assert post(changed, "/release", session_token=new_session.json["session_token"]).status_code == 200
    print("password generation rotation and fresh bootstrap: passed")

    with app.app_context():
        db = database.get_db()
        with db.cursor() as cur:
            cur.execute("DELETE FROM audiomuse_users WHERE username='integration'")
        db.commit()
        assert app_auth.create_additional_user("integration", "pass-three", "user")[0]
        with db.cursor() as cur:
            cur.execute("SELECT principal_id FROM audiomuse_users WHERE username='integration'")
            assert cur.fetchone()[0] != subject
        db.rollback()
    recreated = app.test_client()
    login(recreated, "pass-three")
    assert post(recreated, "/page", session_token=token).status_code == 410
    print("username deletion/recreation subject isolation: passed")
finally:
    try:
        with admin.cursor() as cur:
            if role_changed:
                cur.execute("ALTER ROLE postgres IN DATABASE host_adapter_test RESET search_path")
            if schema_created:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    finally:
        admin.close()
