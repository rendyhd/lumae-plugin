"""Seed the representative performance fixture onto the real plugin schema.

Builds, in the database named by ``LUMAE_PERF_DSN`` (which must be disposable):

1. the AudioMuse host tables (``host_schema.sql``);
2. the plugin schema, by running the real ``plugins.LumaeAnalysis.migrate(db)``
   with only scheduling/queued work stubbed (see ``stub_host.run_plugin_migration``);
3. at ``--scale 1`` (plan §9.3 / P0-3, synthetic): 132k catalogue tracks,
   69k AudioMuse analysis items (62k singletons + 7k paired), 76k mapped provider
   tracks, 94k published profiles with **real-size edge payloads** (the LUM-010
   ``edge.json`` template, varied per track so compression is realistic),
   50k retained ``profile_changes`` events and 150k ``task_status`` rows;
4. the first analysis projection (132k links, 69k items), unless ``--no-project``.

Counts scale linearly with ``--scale`` except the retained profile events, which
stay at the production retention (50k) so the publication bench measures the
budgeted case; override with ``--events``.

Usage::

    LUMAE_PERF_DSN=postgresql://lumae_test@127.0.0.1:<port>/<db> \\
        python3 scripts/perf/seed.py --reset --scale 1
"""
import argparse
import base64
import copy
import io
import json
import os
import random
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402

EDGE_TEMPLATE = os.path.join(
    stub_host.REPO, "docs", "audit", "2026-09-24", "probes", "lum010", "edge.json"
)
BASE = {
    "tracks": 132_000,
    "items": 69_000,
    "pairs": 7_000,  # items with two provider occurrences
    "profiles": 94_000,
    "albums": 12_000,
    "artists": 6_000,
    "tasks": 150_000,
}
DEFAULT_EVENTS = 50_000  # catalog_enrichment.PROFILE_CHANGE_RETENTION_EVENTS
EDGE_ARRAYS = ("level_cdb", "peak_cdb", "true_peak_cdb", "low_power_cdb", "mid_power_cdb",
               "high_power_cdb", "spectral_flux_q15", "onset_density_q15")


def log(message):
    print(message, file=sys.stderr, flush=True)


class Seeder:
    def __init__(self, db):
        self.db = db
        self.cur = db.cursor()
        self.timings = {}

    def run(self, label, sql, params=None):
        t0 = time.perf_counter()
        self.cur.execute(sql, params)
        rows = self.cur.rowcount
        self.db.commit()
        elapsed = time.perf_counter() - t0
        self.timings[label] = round(elapsed, 2)
        log(f"{label}: {elapsed:.1f}s rows={rows}")


def reset_schema(db):
    cur = db.cursor()
    cur.execute("SELECT current_database()")
    log(f"resetting schema public in database {cur.fetchone()[0]}")
    cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
    cur.execute("CREATE SCHEMA public")
    db.commit()


def edge_payloads(rows, template, rng):
    """Yield (track_id, media_revision, representation_id, signature, digest, json)."""
    import numpy as np
    from plugins.LumaeAnalysis.edge_profiles import (
        canonical_json, opaque_revision, profile_digest,
    )

    def dtype(name):
        # centidB arrays are signed 16-bit; q15 arrays are unsigned.
        return "<i2" if name.endswith("_cdb") else "<u2"

    def bounds(name):
        return (-32768, 32767) if name.endswith("_cdb") else (0, 65535)

    decoded = {
        side: {name: np.frombuffer(base64.b64decode(template[side][name]),
                                   dtype=dtype(name)).astype(np.int32)
               for name in EDGE_ARRAYS}
        for side in ("head", "tail")
    }
    np_rng = np.random.default_rng(rng.randrange(1 << 30))
    for catalog_id, track_id, signature in rows:
        payload = copy.deepcopy(template)
        revision = opaque_revision(signature)
        content = format(rng.getrandbits(256), "064x")
        payload.update({
            "catalog_instance_id": catalog_id,
            "track_id": track_id,
            "media_revision": revision,
            "content_sha256": content,
            "representation_id": "sha256:" + content,
            "noise_floor_cdb": template["noise_floor_cdb"] + rng.randint(-300, 300),
        })
        payload["landmarks"]["confidence_q15"] = rng.randint(12000, 32767)
        for side in ("head", "tail"):
            offset = int(np_rng.integers(-250, 250))
            for name in EDGE_ARRAYS:
                base = decoded[side][name]
                if name == "onset_density_q15":
                    # Mostly zero, like the template, with sparse onsets.
                    values = base + (np_rng.random(base.shape) < 0.05) * np_rng.integers(
                        0, 20000, base.shape)
                else:
                    values = base + offset + np_rng.normal(0, 25, base.shape).astype(np.int32)
                values = np.clip(values, *bounds(name)).astype(dtype(name))
                payload[side][name] = base64.b64encode(values.tobytes()).decode("ascii")
        payload["profile_digest"] = profile_digest(payload)
        yield (track_id, revision, payload["representation_id"], signature,
               payload["profile_digest"], canonical_json(payload))


def copy_rows(cur, table, columns, rows):
    buf = io.StringIO()
    for row in rows:
        buf.write("\t".join(
            str(value).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
            for value in row))
        buf.write("\n")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN", buf)


def seed(args):
    scale = args.scale
    n = {key: max(1, int(round(value * scale))) for key, value in BASE.items()}
    n["pairs"] = min(n["pairs"], n["items"] // 2)
    n["profiles"] = min(n["profiles"], n["tracks"])
    events = DEFAULT_EVENTS if args.events is None else args.events
    rng = random.Random(args.seed)
    started = time.perf_counter()

    db = stub_host.connect()
    cur = db.cursor()
    cur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'"
    )
    if cur.fetchone()[0] and not args.reset:
        sys.exit("database is not empty; pass --reset to drop and recreate schema public")
    db.rollback()
    if args.reset:
        reset_schema(db)

    s = Seeder(db)
    with open(os.path.join(stub_host.HERE, "host_schema.sql")) as handle:
        s.run("host_schema", handle.read())
    t0 = time.perf_counter()
    stub_host.run_plugin_migration(db)
    s.timings["plugin_migrate"] = round(time.perf_counter() - t0, 2)
    log(f"plugin_migrate: {s.timings['plugin_migrate']:.1f}s")
    cur = s.cur
    cur.execute(f"SELECT catalog_instance_id, current_core_server_id FROM {T}catalog_sources")
    src, server = cur.fetchone()
    db.commit()
    from plugins.LumaeAnalysis.catalog import CATALOG_BUILDER_VERSION

    log(f"source {src} server {server} scale {scale} counts {n} events {events}")

    # ---- catalogue generation 1 ---------------------------------------------
    s.run("catalog_state", f"""UPDATE {T}catalog_state SET published_generation=1, status='complete',
        catalog_head_seq=%s, catalog_floor_seq=0, entity_counts=%s::jsonb, completed_at=now(),
        catalog_builder_version=%s, refresh_required=FALSE, refresh_reason=NULL
        WHERE catalog_instance_id=%s""",
        (n["tracks"] + n["albums"] + n["artists"],
         json.dumps({"track": n["tracks"], "album": n["albums"], "artist": n["artists"],
                     "library": 1}), CATALOG_BUILDER_VERSION, src))
    s.run("catalog_artists", f"""INSERT INTO {T}catalog_artists (catalog_instance_id, published_generation,
        artist_id, name, sort_name, metadata_fp, payload, available, first_seen_at, last_seen_at)
        SELECT %s, 1, 'ar-'||g, 'Artist '||g||' '||md5(g::text), NULL, md5('a'||g), '{{}}'::jsonb,
               TRUE, now(), now()
          FROM generate_series(1, %s) g""", (src, n["artists"]))
    s.run("catalog_albums", f"""INSERT INTO {T}catalog_albums (catalog_instance_id, published_generation,
        album_id, name, sort_name, album_artist_display, metadata_fp, payload, available,
        first_seen_at, last_seen_at)
        SELECT %s, 1, 'al-'||g, 'Album '||substr(md5(g::text),1,10)||' Édition', NULL,
               'Artist '||(g %% %s)||' '||md5((g %% %s)::text), md5('al'||g),
               '{{}}'::jsonb, TRUE, now(), now()
          FROM generate_series(1, %s) g""", (src, n["artists"], n["artists"], n["albums"]))
    s.run("catalog_tracks", f"""INSERT INTO {T}catalog_tracks (catalog_instance_id, published_generation,
        track_id, album_id, title, artist_display, album_artist_display, disc_number, track_number,
        duration_ms, content_kind, release_type, cover_art_id, streamable, downloadable,
        analysis_eligible, metadata_fp, media_fp, artwork_fp, payload, available, first_seen_at,
        last_seen_at)
        SELECT %s, 1, 'tr-'||lpad(g::text,7,'0'), 'al-'||(g %% %s + 1),
               'Song '||substr(md5(g::text),1,12)||' Café',
               'Artist '||(g %% %s)||' '||md5((g %% %s)::text),
               NULL, 1, g %% 14 + 1, 180000 + (g %% 120000), 'music', 'album', 'ca-'||g,
               TRUE, TRUE, TRUE, md5('m'||g), 'path/'||g||':123:456', md5('w'||g),
               jsonb_build_object('id','tr-'||g,'suffix','flac','bitRate',1011,'samplingRate',44100,
                    'channelCount',2,'size',31000000+g,'contentType','audio/flac','genre','Rock',
                    'replayGain',jsonb_build_object('trackGain',-7.1,'albumGain',-7.4,
                                                    'trackPeak',0.98,'albumPeak',0.99),
                    'musicBrainzId',md5('mb'||g),'isrc','US'||substr(md5('i'||g),1,10),
                    'created','2024-01-01T00:00:00Z','bpm',120,'comment',repeat('x',120)),
               TRUE, now(), now()
          FROM generate_series(1, %s) g""",
        (src, n["albums"], n["artists"], n["artists"], n["tracks"]))

    # ---- AudioMuse host analysis tables ---------------------------------------
    s.run("score", """INSERT INTO score (item_id, title, author, album, tempo, key, scale,
        mood_vector, energy, other_features)
        SELECT 'it-'||lpad(g::text,7,'0'), 'Song', 'Artist', 'Album', 90 + g % 60, 'C', 'major',
               'rock:0.51,pop:0.32,electronic:0.11,jazz:0.05,classical:0.03,metal:0.02,folk:0.02,soul:0.01',
               0.1 + (g % 50)/100.0,
               'danceable:0.61,aggressive:0.12,happy:0.4,party:0.3,relaxed:0.5,sad:0.2'
          FROM generate_series(1, %s) g""".replace("%", "%%").replace("%%s", "%s"),
        (n["items"],))
    s.run("embedding", """INSERT INTO embedding (item_id, embedding)
        SELECT 'it-'||lpad(g::text,7,'0'), decode(repeat(substr(md5(g::text),1,16), 100), 'hex')
          FROM generate_series(1, %s) g""", (n["items"],))  # 800 bytes = 200 f32
    s.run("clap_embedding", """INSERT INTO clap_embedding (item_id, embedding)
        SELECT 'it-'||lpad(g::text,7,'0'), decode(repeat(md5(g::text), 128), 'hex')
          FROM generate_series(1, %s) g""", (n["items"],))  # 2048 bytes = 512 f32
    # Singleton items map to tracks 1..single; each paired item maps to two tracks.
    single = n["items"] - n["pairs"]
    s.run("track_server_map_single", """INSERT INTO track_server_map
        (item_id, server_id, provider_track_id, match_tier)
        SELECT 'it-'||lpad(g::text,7,'0'), %s, 'tr-'||lpad(g::text,7,'0'), 'direct'
          FROM generate_series(1, %s) g""", (server, single))
    s.run("track_server_map_pairs", """INSERT INTO track_server_map
        (item_id, server_id, provider_track_id, match_tier)
        SELECT 'it-'||lpad((%s + (g+1)/2)::text,7,'0'), %s, 'tr-'||lpad((%s+g)::text,7,'0'),
               'fingerprint'
          FROM generate_series(1, %s) g""", (single, server, single, 2 * n["pairs"]))
    s.run("chromaprint", """INSERT INTO chromaprint (server_id, provider_track_id, fingerprint, updated_at)
        SELECT %s, provider_track_id, decode(repeat(md5(provider_track_id), 96), 'hex'),
               now() - interval '2 days'
          FROM track_server_map""", (server,))
    ids = json.dumps([f"it-{g:07d}" for g in range(1, n["items"] + 1)])
    proj = struct.pack(f"<{n['items'] * 2}f", *[rng.random() for _ in range(n["items"] * 2)])
    s.run("map_projection_data", "INSERT INTO map_projection_data (index_name, projection_data, "
          "id_map_json, embedding_dimension) VALUES ('main_map', %s, %s, 2)", (proj, ids))
    s.run("task_status", """INSERT INTO task_status
        (task_id, parent_task_id, task_type, status, details, timestamp, start_time, end_time)
        SELECT 't-'||g,
               CASE WHEN g % 500 = 0 THEN NULL ELSE 'root-'||(g/500) END,
               CASE WHEN g % 500 = 0 THEN (CASE WHEN g % 5000 = 0 THEN 'cleaning' ELSE 'main_analysis' END)
                    WHEN g % 97 = 0 THEN 'provider_migration' ELSE 'album_analysis' END,
               'SUCCESS', '{"failed_servers":[]}', now() - (g || ' seconds')::interval,
               extract(epoch from now()) - g - 60, extract(epoch from now()) - g
          FROM generate_series(1, %s) g""".replace("%", "%%").replace("%%s", "%s"),
        (n["tasks"],))

    # ---- profiles -------------------------------------------------------------
    # The published signature is the catalogue revision complete_attempt stores.
    s.run("source_profiles", f"""INSERT INTO {T}source_profiles (catalog_instance_id, track_id,
        sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver,
        media_signature, analyzed_at, status)
        SELECT %s, t.track_id, 44100, t.duration_ms, -9.5 - (g %% 90) / 10.0,
               decode(repeat(substr(md5(t.track_id), 1, 6), 15), 'hex'),
               decode(repeat(substr(md5('e'||t.track_id), 1, 6), 15), 'hex'), 1, 1,
               'catalog-media:'||t.media_fp, now(), 'ready'
          FROM generate_series(1, %s) g
          JOIN {T}catalog_tracks t ON t.catalog_instance_id=%s AND t.published_generation=1
                                   AND t.track_id='tr-'||lpad(g::text,7,'0')""",
        (src, n["profiles"], src))
    s.run("published_source_profiles", f"""INSERT INTO {T}published_source_profiles
        (catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
         analyzer_ver, profile_schema_ver, media_signature, analyzed_at)
        SELECT catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs, start_ramp,
               end_ramp, analyzer_ver, profile_schema_ver, media_signature, analyzed_at
          FROM {T}source_profiles WHERE catalog_instance_id=%s""", (src,))

    with open(EDGE_TEMPLATE) as handle:
        template = json.load(handle)
    t0 = time.perf_counter()
    cur.execute(f"SELECT catalog_instance_id, track_id, media_signature FROM "
                f"{T}published_source_profiles WHERE catalog_instance_id=%s ORDER BY track_id",
                (src,))
    profile_rows = cur.fetchall()
    total_json = 0
    batch = []
    for track_id, revision, rep, signature, digest, text in edge_payloads(profile_rows, template, rng):
        total_json += len(text)
        batch.append((src, track_id, revision, rep, signature, digest, text))
        if len(batch) >= 2000:
            copy_rows(cur, f"{T}edge_profiles", ("catalog_instance_id", "track_id",
                      "media_revision", "representation_id", "media_signature",
                      "profile_digest", "payload"), batch)
            batch = []
    if batch:
        copy_rows(cur, f"{T}edge_profiles", ("catalog_instance_id", "track_id", "media_revision",
                  "representation_id", "media_signature", "profile_digest", "payload"), batch)
    db.commit()
    s.timings["edge_profiles"] = round(time.perf_counter() - t0, 2)
    log(f"edge_profiles: {s.timings['edge_profiles']:.1f}s rows={len(profile_rows)} "
        f"avg_json_bytes={total_json // max(1, len(profile_rows))}")

    head = n["profiles"] + events
    floor = head - events
    s.run("profile_stream_state", f"""INSERT INTO {T}profile_stream_state
        (catalog_instance_id, epoch, head_seq, floor_seq) VALUES (%s, 'pepoch', %s, %s)
        ON CONFLICT (catalog_instance_id) DO UPDATE
           SET epoch='pepoch', head_seq=EXCLUDED.head_seq, floor_seq=EXCLUDED.floor_seq""",
        (src, head, floor))
    # Events look like complete_attempt's serialize_profile() payloads (no edge).
    s.run("profile_changes", f"""INSERT INTO {T}profile_changes
        (catalog_instance_id, epoch, seq, track_id, operation, writer_generation, payload)
        SELECT %s, 'pepoch', s, p.track_id, 'upsert', 2,
               jsonb_build_object('track_id', p.track_id, 'source', 'waveform',
                 'sample_rate', p.sample_rate, 'duration_ms', p.duration_ms,
                 'ref_lufs', p.ref_lufs, 'start_ramp', encode(p.start_ramp, 'base64'),
                 'end_ramp', encode(p.end_ramp, 'base64'), 'analyzer_ver', p.analyzer_ver,
                 'analyzed_at', '2026-09-01T00:00:00Z',
                 'media_signature', 'sha256:'||encode(sha256(convert_to(p.media_signature,'UTF8')),'hex'),
                 'media_revision', 'sha256:'||encode(sha256(convert_to(p.media_signature,'UTF8')),'hex'))
          FROM generate_series(%s, %s) s
          JOIN {T}published_source_profiles p
            ON p.catalog_instance_id=%s AND p.track_id='tr-'||lpad((s %% %s + 1)::text,7,'0')""",
        (src, floor + 1, head, src, n["profiles"]))

    s.run("fixture_metadata", """CREATE TABLE IF NOT EXISTS lumae_perf_fixture (
        key TEXT PRIMARY KEY, value JSONB NOT NULL);
        INSERT INTO lumae_perf_fixture VALUES ('fixture', %s::jsonb)
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""",
        (json.dumps({"scale": scale, "counts": n, "events": events, "seed": args.seed,
                     "edge_template": os.path.relpath(EDGE_TEMPLATE, stub_host.REPO)}),))

    db.autocommit = True
    t0 = time.perf_counter()
    cur.execute("VACUUM ANALYZE")
    s.timings["vacuum_analyze"] = round(time.perf_counter() - t0, 2)
    db.autocommit = False

    # The bulk load bypasses publication, so summarize it the way the install
    # hook summarizes existing data (P2-1 committed status summary).
    t0 = time.perf_counter()
    stub_host.load_plugin().refresh_status_summaries(db)
    db.commit()
    s.timings["status_summaries"] = round(time.perf_counter() - t0, 2)
    log(f"status_summaries: {s.timings['status_summaries']:.1f}s")

    projection = None
    if not args.no_project:
        projection = initial_projection()
        db.autocommit = True
        cur.execute("VACUUM ANALYZE")
        db.autocommit = False

    aux = stub_host.aux_cursor()
    sizes = {name: round(stub_host.relation_bytes(aux, f"{T}{name}") / 1e6, 1)
             for name in ("edge_profiles", "published_source_profiles", "profile_changes",
                          "catalog_tracks", "analysis_items", "track_analysis_links")}
    aux.execute("SELECT pg_database_size(current_database())")
    result = {
        "bench": "seed", "scale": scale, "counts": n, "events": events,
        "source": src, "timings_s": s.timings, "initial_projection": projection,
        "table_mb": sizes, "database_mb": round(aux.fetchone()[0] / 1e6, 1),
        "elapsed_s": round(time.perf_counter() - started, 1),
        "edge_json_bytes_avg": total_json // max(1, len(profile_rows)),
    }
    db.close()
    return result


def initial_projection():
    """Run the first projection so links/items exist (generation 1)."""
    import subprocess

    log("initial projection (proj_bench.py full)...")
    out = subprocess.run(
        [sys.executable, os.path.join(stub_host.HERE, "proj_bench.py"), "full"],
        check=True, capture_output=True, text=True,
    ).stdout.strip().splitlines()[-1]
    result = json.loads(out)
    log(f"initial projection: {result.get('elapsed_s')}s")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--scale", type=float, default=1.0,
                        help="fraction of the representative fixture (default 1.0; 0.1 for quick runs)")
    parser.add_argument("--events", type=int, default=None,
                        help=f"retained profile_changes events (default {DEFAULT_EVENTS}, not scaled)")
    parser.add_argument("--reset", action="store_true",
                        help="DROP SCHEMA public CASCADE first (the database must be disposable)")
    parser.add_argument("--no-project", action="store_true",
                        help="skip the initial analysis projection")
    parser.add_argument("--seed", type=int, default=1, help="random seed (default 1)")
    args = parser.parse_args()
    if args.scale <= 0:
        parser.error("--scale must be positive")
    stub_host.emit(seed(args))


if __name__ == "__main__":
    main()
