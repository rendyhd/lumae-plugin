"""Prove plugin-owned and stock request connections stay transactionally separate."""

from runtime import configure


def main():
    config = configure()
    from flask_app import app
    from plugin.api import get_db
    from plugin.manager import plugin_manager

    plugin_manager.setup_namespace()
    from audiomuse_plugins.lumae_analysis import profile_bootstrap

    with app.test_request_context("/plugins/lumae_analysis/api/health"):
        request_db = get_db()
        with request_db.cursor() as cur:
            cur.execute("SELECT pg_backend_pid(),txid_current(),current_user,current_schema(),"
                        "current_setting('search_path')")
            request_pid, request_txid, request_role, request_schema, request_path = cur.fetchone()
            cur.execute("""UPDATE plugin_lumae_analysis__published_source_profiles
                SET ref_lufs=-17 WHERE catalog_instance_id='qual-source-b'
                AND track_id='track-a'""")
        owned_pids = []
        for _ in range(5):
            with profile_bootstrap._connection(repeatable=True, creator=True) as owned:
                with owned.cursor() as cur:
                    cur.execute("SELECT pg_backend_pid(),current_user,current_schema(),"
                                "current_setting('search_path'),"
                                "to_regclass('plugin_lumae_analysis__profile_bootstrap_sessions')")
                    pid, role, schema, path, table = cur.fetchone()
                    owned_pids.append(pid)
                    assert pid != request_pid
                    assert (role, schema, path) == (request_role, request_schema, request_path)
                    assert table is not None
                    cur.execute("""SELECT ref_lufs FROM plugin_lumae_analysis__published_source_profiles
                        WHERE catalog_instance_id='qual-source-b' AND track_id='track-a'""")
                    assert cur.fetchone()[0] == -18  # request write is still uncommitted
                    cur.execute("SELECT pg_backend_pid(),current_setting('transaction_isolation')")
                    assert cur.fetchone() == (pid, "repeatable read")
            with request_db.cursor() as cur:
                cur.execute("SELECT pg_backend_pid(),txid_current(),ref_lufs "
                            "FROM plugin_lumae_analysis__published_source_profiles "
                            "WHERE catalog_instance_id='qual-source-b' AND track_id='track-a'")
                assert cur.fetchone() == (request_pid, request_txid, -17)
                cur.execute("SELECT count(*) FROM pg_stat_activity WHERE pid=%s", (pid,))
                assert cur.fetchone()[0] == 0
        request_db.rollback()
        with request_db.cursor() as cur:
            cur.execute("""SELECT ref_lufs FROM plugin_lumae_analysis__published_source_profiles
                WHERE catalog_instance_id='qual-source-b' AND track_id='track-a'""")
            assert cur.fetchone()[0] == -18
        request_db.rollback()
    print("stock request/owned backend, role, search path, rollback, close, lock cleanup PASS")


if __name__ == "__main__":
    main()
