"""Seed audit_perf with a synthetic representative-scale fixture.

Scale (plan Section 9.3, labelled synthetic): 132k catalogue tracks (all eligible),
69k AudioMuse analysis items, 76k mapped provider tracks (62k singletons + 7k
pairs), 132k analysis links, 94k published profiles (+ edge payloads),
50k retained profile-change events, 150k task_status rows.
"""
import json
import sys
import time

sys.path.insert(0, ".")
import stub_host  # noqa: E402

N_TRACKS = 132_000
N_ITEMS = 69_000
N_PAIRS = 7_000  # items with two provider occurrences
N_PROFILES = 94_000
N_ALBUMS = 12_000
N_ARTISTS = 6_000
N_TASKS = 150_000

T = "plugin_lumae_analysis__"
EDGE = json.load(open("/home/user/lumae-plugin/tests/plugins/edge_profile_v2_golden.json"))

db = stub_host.connect()
cur = db.cursor()
cur.execute(f"SELECT catalog_instance_id, current_core_server_id FROM {T}catalog_sources")
SRC, SERVER = cur.fetchone()
print("source", SRC)


def run(label, sql, params=None):
    t0 = time.perf_counter()
    if params is not None:
        sql = sql.replace(' % ', ' %% ')
    cur.execute(sql, params)
    db.commit()
    print(f"{label}: {time.perf_counter() - t0:.1f}s rows={cur.rowcount}")


# ---- catalogue generation 1 -------------------------------------------------
run("state", f"""UPDATE {T}catalog_state SET published_generation=1, status='complete',
    catalog_head_seq=150000, catalog_floor_seq=0, entity_counts=%s::jsonb,
    catalog_builder_version=(SELECT catalog_builder_version FROM {T}catalog_state LIMIT 1),
    completed_at=now() WHERE catalog_instance_id=%s""",
    (json.dumps({"track": N_TRACKS, "album": N_ALBUMS, "artist": N_ARTISTS, "library": 1}), SRC))
run("artists", f"""INSERT INTO {T}catalog_artists (catalog_instance_id, published_generation, artist_id,
    name, sort_name, metadata_fp, payload, available, first_seen_at, last_seen_at)
    SELECT %s, 1, 'ar-'||g, 'Artist '||g||' '||md5(g::text), NULL, md5('a'||g), '{{}}'::jsonb, TRUE, now(), now()
      FROM generate_series(1,{N_ARTISTS}) g""", (SRC,))
run("albums", f"""INSERT INTO {T}catalog_albums (catalog_instance_id, published_generation, album_id,
    name, sort_name, album_artist_display, metadata_fp, payload, available, first_seen_at, last_seen_at)
    SELECT %s, 1, 'al-'||g, 'Album '||substr(md5(g::text),1,10)||' Édition', NULL,
           'Artist '||(g % {N_ARTISTS})||' '||md5((g % {N_ARTISTS})::text), md5('al'||g),
           '{{}}'::jsonb, TRUE, now(), now()
      FROM generate_series(1,{N_ALBUMS}) g""", (SRC,))
run("tracks", f"""INSERT INTO {T}catalog_tracks (catalog_instance_id, published_generation, track_id,
    album_id, title, artist_display, album_artist_display, disc_number, track_number, duration_ms,
    content_kind, release_type, cover_art_id, streamable, downloadable, analysis_eligible,
    metadata_fp, media_fp, artwork_fp, payload, available, first_seen_at, last_seen_at)
    SELECT %s, 1, 'tr-'||lpad(g::text,7,'0'), 'al-'||(g % {N_ALBUMS} + 1),
           'Song '||substr(md5(g::text),1,12)||' Café', 'Artist '||(g % {N_ARTISTS})||' '||md5((g % {N_ARTISTS})::text),
           NULL, 1, g % 14 + 1, 180000 + (g % 120000), 'music', 'album', 'ca-'||g, TRUE, TRUE, TRUE,
           md5('m'||g), 'path/'||g||':123:456', md5('w'||g),
           jsonb_build_object('id','tr-'||g,'suffix','flac','bitRate',1011,'samplingRate',44100,
                'channelCount',2,'size',31000000+g,'contentType','audio/flac','genre','Rock',
                'replayGain',jsonb_build_object('trackGain',-7.1,'albumGain',-7.4,'trackPeak',0.98,'albumPeak',0.99),
                'musicBrainzId',md5('mb'||g),'isrc','US'||substr(md5('i'||g),1,10),
                'created','2024-01-01T00:00:00Z','bpm',120,'comment',repeat('x',120)),
           TRUE, now(), now()
      FROM generate_series(1,{N_TRACKS}) g""", (SRC,))

# ---- AudioMuse host analysis tables ----------------------------------------
run("score", f"""INSERT INTO score (item_id, title, author, album, tempo, key, scale, mood_vector,
    energy, other_features)
    SELECT 'it-'||lpad(g::text,7,'0'), 'Song', 'Artist', 'Album', 90 + g % 60, 'C', 'major',
           'rock:0.51,pop:0.32,electronic:0.11,jazz:0.05,classical:0.03,metal:0.02,folk:0.02,soul:0.01',
           0.1 + (g % 50)/100.0, 'danceable:0.61,aggressive:0.12,happy:0.4,party:0.3,relaxed:0.5,sad:0.2'
      FROM generate_series(1,{N_ITEMS}) g""")
run("embedding", f"""INSERT INTO embedding (item_id, embedding)
    SELECT 'it-'||lpad(g::text,7,'0'),
           decode(repeat(substr(md5(g::text),1,16), 100), 'hex')  -- 800 bytes = 200 f32
      FROM generate_series(1,{N_ITEMS}) g""")
run("clap", f"""INSERT INTO clap_embedding (item_id, embedding)
    SELECT 'it-'||lpad(g::text,7,'0'),
           decode(repeat(md5(g::text), 128), 'hex')  -- 2048 bytes = 512 f32
      FROM generate_series(1,{N_ITEMS}) g""")
# 62k singleton items (tracks 1..62000 -> items 1..62000); 7k paired items each with 2 tracks
single = N_ITEMS - N_PAIRS
run("tsm_single", f"""INSERT INTO track_server_map (item_id, server_id, provider_track_id, match_tier)
    SELECT 'it-'||lpad(g::text,7,'0'), %s, 'tr-'||lpad(g::text,7,'0'), 'direct'
      FROM generate_series(1,{single}) g""", (SERVER,))
run("tsm_pairs", f"""INSERT INTO track_server_map (item_id, server_id, provider_track_id, match_tier)
    SELECT 'it-'||lpad(({single} + (g+1)/2)::text,7,'0'), %s, 'tr-'||lpad(({single}+g)::text,7,'0'), 'fingerprint'
      FROM generate_series(1,{2 * N_PAIRS}) g""", (SERVER,))
run("chromaprint", f"""INSERT INTO chromaprint (server_id, provider_track_id, fingerprint, updated_at)
    SELECT %s, provider_track_id, decode(repeat(md5(provider_track_id), 96), 'hex'), now() - interval '2 days'
      FROM track_server_map""", (SERVER,))
ids = ",".join(f'"it-{g:07d}"' for g in range(1, N_ITEMS + 1))
import struct, random
random.seed(1)
proj = struct.pack(f"<{N_ITEMS * 2}f", *[random.random() for _ in range(N_ITEMS * 2)])
run("umap", "INSERT INTO map_projection_data (index_name, projection_data, id_map_json, embedding_dimension) "
    "VALUES ('main_map', %s, %s, 2)", (proj, "[" + ids + "]"))
run("task_status", f"""INSERT INTO task_status (task_id, parent_task_id, task_type, status, details, timestamp, start_time, end_time)
    SELECT 't-'||g,
           CASE WHEN g % 500 = 0 THEN NULL ELSE 'root-'||(g/500) END,
           CASE WHEN g % 500 = 0 THEN (CASE WHEN g % 5000 = 0 THEN 'cleaning' ELSE 'main_analysis' END)
                WHEN g % 97 = 0 THEN 'provider_migration' ELSE 'album_analysis' END,
           'SUCCESS', '{{"failed_servers":[]}}', now() - (g || ' seconds')::interval,
           extract(epoch from now()) - g - 60, extract(epoch from now()) - g
      FROM generate_series(1,{N_TASKS}) g""")

# ---- profiles ----------------------------------------------------------------
run("source_profiles", f"""INSERT INTO {T}source_profiles (catalog_instance_id, track_id, sample_rate, duration_ms,
    ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, media_signature, analyzed_at, status)
    SELECT %s, 'tr-'||lpad(g::text,7,'0'), 44100, 200000, -9.5,
           decode(repeat('f40100',15),'hex'), decode(repeat('f40200',15),'hex'), 1, 1,
           'path/'||g||':123:456', now(), 'ready'
      FROM generate_series(1,{N_PROFILES}) g""", (SRC,))
run("published", f"""INSERT INTO {T}published_source_profiles (catalog_instance_id, track_id, sample_rate,
    duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, media_signature, analyzed_at)
    SELECT catalog_instance_id, track_id, sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp,
           analyzer_ver, profile_schema_ver, media_signature, analyzed_at
      FROM {T}source_profiles WHERE catalog_instance_id=%s""", (SRC,))
run("edge", f"""INSERT INTO {T}edge_profiles (catalog_instance_id, track_id, media_revision, representation_id,
    media_signature, profile_digest, payload)
    SELECT p.catalog_instance_id, p.track_id, rev, 'sha256:'||md5(p.track_id)||md5(p.track_id),
           p.media_signature, md5(p.track_id)||md5(p.track_id),
           %s::jsonb || jsonb_build_object('track_id', p.track_id, 'media_revision', rev,
                                          'catalog_instance_id', p.catalog_instance_id)
      FROM {T}published_source_profiles p,
           LATERAL (SELECT 'sha256:'||encode(sha256(convert_to(p.media_signature,'UTF8')),'hex') AS rev) r
     WHERE p.catalog_instance_id=%s""", (json.dumps(EDGE), SRC))
run("stream_state", f"""INSERT INTO {T}profile_stream_state (catalog_instance_id, epoch, head_seq, floor_seq)
    VALUES (%s, 'pepoch', 144000, 94000)
    ON CONFLICT (catalog_instance_id) DO UPDATE SET epoch='pepoch', head_seq=144000, floor_seq=94000""", (SRC,))
run("profile_changes", f"""INSERT INTO {T}profile_changes (catalog_instance_id, epoch, seq, track_id, operation, payload)
    SELECT %s, 'pepoch', s, 'tr-'||lpad((s % {N_PROFILES} + 1)::text,7,'0'), 'upsert',
           jsonb_build_object('track_id','tr-'||s,'source','waveform','sample_rate',44100,'duration_ms',200000,
             'ref_lufs',-9.5,'start_ramp',repeat('A',60),'end_ramp',repeat('B',60),'analyzer_ver',1,
             'analyzed_at','2026-09-01T00:00:00Z','media_signature','sha256:'||md5(s::text)||md5(s::text),
             'media_revision','sha256:'||md5(s::text)||md5(s::text))
      FROM generate_series(94001,144000) s""", (SRC,))
cur.execute("VACUUM ANALYZE") if False else None
db.autocommit = True
t0 = time.perf_counter()
cur.execute("VACUUM ANALYZE")
print(f"vacuum analyze {time.perf_counter() - t0:.1f}s")
