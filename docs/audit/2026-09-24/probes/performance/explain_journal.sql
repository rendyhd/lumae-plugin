SET max_parallel_workers_per_gather=0;
BEGIN;
\echo === per-publication compaction DELETE (record_profile_change -> compact_change_journal)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) DELETE FROM plugin_lumae_analysis__profile_changes WHERE catalog_instance_id='8f881303-aa56-4484-9a69-8f208c79f533' AND (epoch<>'pepoch' OR (epoch='pepoch' AND seq<=94001));
\echo === equivalent range-only form
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) DELETE FROM plugin_lumae_analysis__profile_changes WHERE catalog_instance_id='8f881303-aa56-4484-9a69-8f208c79f533' AND epoch='pepoch' AND seq<=94002;
ROLLBACK;
\echo === profile changes feed page (read_profile_changes, 250)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) SELECT seq, track_id, operation, payload, created_at FROM plugin_lumae_analysis__profile_changes WHERE catalog_instance_id='8f881303-aa56-4484-9a69-8f208c79f533' AND epoch='pepoch' AND seq>120000 AND seq<=144000 ORDER BY seq LIMIT 250;
