SET max_parallel_workers_per_gather=0;
\timing on
-- (a) server-side vector fingerprints: returns 64-char hex instead of 2.8KB blobs per item
SELECT count(*), sum(length(m)+length(c)) FROM (
 SELECT s.item_id, encode(sha256(e.embedding),'hex') m, encode(sha256(c.embedding),'hex') c
   FROM score s LEFT JOIN embedding e ON e.item_id=s.item_id LEFT JOIN clap_embedding c ON c.item_id=s.item_id) x;
-- (b) set-based generation copy (what provider_identity_rekey._copy_analysis_generation already does)
BEGIN;
SELECT pg_current_wal_lsn() AS l0 \gset
INSERT INTO plugin_lumae_analysis__analysis_items
 SELECT catalog_instance_id, 99, analysis_id, scalar_fp, umap_fp, musicnn_fp, clap_fp, scalar_payload,
        musicnn_vector, clap_vector, musicnn_dimensions, clap_dimensions, model_metadata
   FROM plugin_lumae_analysis__analysis_items WHERE catalog_instance_id='8f881303-aa56-4484-9a69-8f208c79f533' AND projection_generation=2;
INSERT INTO plugin_lumae_analysis__track_analysis_links
 SELECT catalog_instance_id, 99, provider_track_id, analysis_id, status, match_tier, algorithm,
        decision_threshold, distance, evidence_complete, conflict_flags, review_state
   FROM plugin_lumae_analysis__track_analysis_links WHERE catalog_instance_id='8f881303-aa56-4484-9a69-8f208c79f533' AND projection_generation=2;
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), :'l0')) AS wal;
ROLLBACK;
