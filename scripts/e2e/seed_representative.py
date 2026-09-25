"""Seed the representative fixture for the P2-6 end-to-end gate.

A thin wrapper around the P0-3 seeder (``scripts/perf/seed.py``): the real
plugin migration, 132k catalogue tracks, 94k published profiles with
**real-size edges** (about 19.5 KB of JSON each, varied per track), 50k
retained journal events, 150k task rows and the first analysis projection at
``--scale 1``. On top of that it does what a running plugin does at start,
which the stub host never runs: ``compact_enrichment_storage`` persists the
journal retention limit (``profile_stream_state.retention_limit``, P1-2) that
bounds the v2 first catch-up (4 x retention).

Two host modes:

``--host-schema stub`` (default)
    Drop and recreate ``public`` (``--reset``) and create the minimal host
    tables of ``scripts/perf/host_schema.sql``. For the local runner.
``--host-schema audiomuse``
    The stock AudioMuse host already created its schema in this database
    (``docker compose run --rm lumae-install init``). Insert the music server
    ``server-a`` into the host registry with placeholder Navidrome credentials
    (so the host keeps it; ``host/navidrome_stub.py`` answers its ping), then
    seed the same data into the host's tables, with AudioMuse 3.6 content ids
    (``fp_4<hex>``) so the host does not relabel the synthetic items at start.
    Both the local runner and the stock host can then serve this database.

Usage::

    python3 scripts/e2e/seed_representative.py --dsn postgresql://lumae_test@127.0.0.1:55432/e2e_rep --reset
"""
import argparse
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PERF = os.path.abspath(os.path.join(HERE, "..", "perf"))


def ensure_host_server(dsn, url, server_id="server-a", name="Main"):
    """Register a Navidrome server in the stock host's registry (music_servers)."""
    import psycopg2
    from psycopg2.extras import Json

    db = psycopg2.connect(dsn)
    try:
        with db.cursor() as cur:
            cur.execute("SELECT to_regclass('public.music_servers') IS NOT NULL")
            if not cur.fetchone()[0]:
                sys.exit("music_servers is missing: run the host init first "
                         "(docker compose run --rm lumae-install init)")
            cur.execute(
                """INSERT INTO music_servers (server_id, name, server_type, creds,
                                             music_libraries, is_default)
                   VALUES (%s, %s, 'navidrome', %s, '', NOT EXISTS (
                       SELECT 1 FROM music_servers WHERE is_default))
                   ON CONFLICT (server_id) DO NOTHING""",
                (server_id, name, Json({"url": url, "user": "lumae-e2e",
                                        "password": "not-a-real-server"})))
        db.commit()
    finally:
        db.close()


def persist_retention(dsn):
    """What plugin start-up maintenance does: persist the retention limit."""
    import stub_host

    stub_host.load_plugin()
    from plugins.LumaeAnalysis.catalog_enrichment import compact_enrichment_storage

    db = stub_host.connect()
    try:
        compact_enrichment_storage(db)
        db.commit()
        with db.cursor() as cur:
            cur.execute(f"SELECT catalog_instance_id, epoch, head_seq, floor_seq, retention_limit "
                        f"FROM {stub_host.T}profile_stream_state")
            rows = cur.fetchall()
        db.commit()
    finally:
        db.close()
    return [dict(zip(("source", "epoch", "head_seq", "floor_seq", "retention_limit"), row))
            for row in rows]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dsn", default=os.environ.get("LUMAE_E2E_DSN"),
                        help="libpq URL of a disposable database (or LUMAE_E2E_DSN)")
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--events", type=int, default=None)
    parser.add_argument("--reset", action="store_true",
                        help="stub mode only: DROP SCHEMA public CASCADE first")
    parser.add_argument("--no-project", action="store_true")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--host-schema", choices=("stub", "audiomuse"), default="stub")
    parser.add_argument("--navidrome-url", default="http://navidrome-stub:4533",
                        help="audiomuse mode: URL of the Navidrome the host registry names "
                             "(host/navidrome_stub.py; http://127.0.0.1:4533 with the "
                             "external-db compose file)")
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or LUMAE_E2E_DSN is required")
    os.environ["LUMAE_PERF_DSN"] = args.dsn
    if PERF not in sys.path:
        sys.path.insert(0, PERF)
    started = time.perf_counter()
    if args.host_schema == "audiomuse":
        ensure_host_server(args.dsn, args.navidrome_url)
    import seed as perf_seed
    import stub_host

    seed_args = argparse.Namespace(
        scale=args.scale, events=args.events, reset=args.reset and args.host_schema == "stub",
        no_project=args.no_project, seed=args.seed,
        host_schema="stub" if args.host_schema == "stub" else "existing",
        item_ids="canonical" if args.host_schema == "audiomuse" else "legacy")
    result = perf_seed.seed(seed_args)
    result["retention"] = persist_retention(args.dsn)
    result["host_schema"] = args.host_schema
    result["bench"] = "seed_representative"
    result["elapsed_s"] = round(time.perf_counter() - started, 1)
    stub_host.emit(result)


if __name__ == "__main__":
    main()
