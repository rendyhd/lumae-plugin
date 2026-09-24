DROP SCHEMA IF EXISTS wb CASCADE;
CREATE SCHEMA wb;
SET search_path TO wb, public;
CREATE TABLE catalog_sources (catalog_instance_id TEXT PRIMARY KEY, provider_type TEXT NOT NULL, server_name TEXT NOT NULL, is_default BOOLEAN NOT NULL, rebind_status TEXT NOT NULL);
CREATE TABLE catalog_state (catalog_instance_id TEXT PRIMARY KEY, published_generation BIGINT NOT NULL, status TEXT NOT NULL);
CREATE TABLE analysis_state (catalog_instance_id TEXT PRIMARY KEY, projection_generation BIGINT NOT NULL);
CREATE TABLE catalog_albums (catalog_instance_id TEXT NOT NULL, published_generation BIGINT NOT NULL, album_id TEXT NOT NULL, name TEXT NOT NULL, sort_name TEXT, album_artist_display TEXT, release_type TEXT, content_kind TEXT, cover_art_id TEXT, metadata_fp TEXT NOT NULL DEFAULT '', payload JSONB NOT NULL DEFAULT '{}', available BOOLEAN NOT NULL, PRIMARY KEY (catalog_instance_id, published_generation, album_id));
CREATE TABLE catalog_tracks (catalog_instance_id TEXT NOT NULL, published_generation BIGINT NOT NULL, track_id TEXT NOT NULL, album_id TEXT, title TEXT NOT NULL, artist_display TEXT, album_artist_display TEXT, disc_number INTEGER, track_number INTEGER, duration_ms BIGINT, content_kind TEXT, release_type TEXT, cover_art_id TEXT, streamable BOOLEAN, downloadable BOOLEAN, analysis_eligible BOOLEAN, metadata_fp TEXT NOT NULL DEFAULT '', media_fp TEXT, artwork_fp TEXT, payload JSONB NOT NULL DEFAULT '{}', available BOOLEAN NOT NULL, first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(), deleted_at TIMESTAMPTZ, PRIMARY KEY (catalog_instance_id, published_generation, track_id));
CREATE TABLE track_analysis_links (catalog_instance_id TEXT NOT NULL, projection_generation BIGINT NOT NULL, provider_track_id TEXT NOT NULL, analysis_id TEXT, status TEXT NOT NULL, PRIMARY KEY (catalog_instance_id, projection_generation, provider_track_id));
INSERT INTO catalog_sources VALUES ('cat-main','navidrome','Main',TRUE,'active'), ('cat-second','jellyfin','Second',FALSE,'active');
INSERT INTO catalog_state VALUES ('cat-main',3,'complete'), ('cat-second',2,'complete');
INSERT INTO analysis_state VALUES ('cat-main',5), ('cat-second',1);
-- 8,000 albums by 5,000 artists; 300 albums are a second edition with identical name+artist
INSERT INTO catalog_albums (catalog_instance_id, published_generation, album_id, name, album_artist_display, available, payload)
SELECT 'cat-main', 3, 'al-'||a, 'Album '||md5(a::text)::text, 'Artist '||(a % 5000), TRUE, jsonb_build_object('year', 1960 + a % 60)
FROM generate_series(1, 7700) a;
INSERT INTO catalog_albums (catalog_instance_id, published_generation, album_id, name, album_artist_display, available, payload)
SELECT 'cat-main', 3, 'al-ed-'||a, 'Album '||md5(a::text)::text, 'Artist '||(a % 5000), TRUE, jsonb_build_object('year', 2010 + a % 10)
FROM generate_series(1, 300) a;
-- 100,000 tracks (12-13 per album) with pseudo-words for search
INSERT INTO catalog_tracks (catalog_instance_id, published_generation, track_id, album_id, title, artist_display, album_artist_display, disc_number, track_number, duration_ms, available, payload)
SELECT 'cat-main', 3, 'tr-'||t,
       CASE WHEN t <= 96250 THEN 'al-'||((t-1)/12.5 + 1)::int ELSE 'al-ed-'||((t-96251) % 300 + 1) END,
       (ARRAY['love','night','blue','river','fire','dream','heart','song','light','rain','the','sun'])[1 + t % 12] || ' ' || substr(md5(t::text),1,8),
       'Artist '||(((t-1)/12.5)::int % 5000), 'Artist '||(((t-1)/12.5)::int % 5000), 1, 1 + t % 13, 180000 + t % 120000, TRUE, jsonb_build_object('year', 1960 + t % 60)
FROM generate_series(1, 100000) t;
UPDATE catalog_tracks SET album_artist_display = 'Artist '||(substring(album_id from 7)::int % 5000) WHERE album_id LIKE 'al-ed-%';
-- secondary source 50k tracks
INSERT INTO catalog_tracks (catalog_instance_id, published_generation, track_id, album_id, title, artist_display, album_artist_display, available)
SELECT 'cat-second', 2, 'tr-'||t, 'al-'||(t/12 + 1), 'second '||md5(t::text), 'Other '||(t % 3000), 'Other '||(t % 3000), TRUE FROM generate_series(1, 50000) t;
INSERT INTO track_analysis_links SELECT catalog_instance_id, 5, track_id, 'an-'||track_id, CASE WHEN random() < 0.7 THEN 'ready' ELSE 'pending' END FROM catalog_tracks WHERE catalog_instance_id='cat-main';
ANALYZE;
SELECT count(*) FROM catalog_tracks;
