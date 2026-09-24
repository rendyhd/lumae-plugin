-- EXPLAIN ANALYZE of the profile change-journal statements.
-- Usage: psql "$LUMAE_PERF_DSN" -f scripts/perf/explain_journal.sql
-- The DELETEs run inside a rolled-back transaction.
SET max_parallel_workers_per_gather=0;
SELECT catalog_instance_id AS source, epoch, head_seq, floor_seq,
       head_seq - 50000 + 1 AS compact_to, GREATEST(floor_seq, head_seq - 250) AS page_after
  FROM plugin_lumae_analysis__profile_stream_state ORDER BY head_seq DESC LIMIT 1 \gset
BEGIN;
\echo === per-publication compaction DELETE (record_profile_change -> compact_change_journal)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON)
DELETE FROM plugin_lumae_analysis__profile_changes
 WHERE catalog_instance_id=:'source' AND (epoch<>:'epoch' OR (epoch=:'epoch' AND seq<=:compact_to));
\echo === equivalent range-only form
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON)
DELETE FROM plugin_lumae_analysis__profile_changes
 WHERE catalog_instance_id=:'source' AND epoch=:'epoch' AND seq<=:compact_to + 1;
ROLLBACK;
\echo === profile changes feed page (read_profile_changes, 250)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON)
SELECT seq, track_id, operation, payload, created_at FROM plugin_lumae_analysis__profile_changes
 WHERE catalog_instance_id=:'source' AND epoch=:'epoch' AND seq>:page_after AND seq<=:head_seq
 ORDER BY seq LIMIT 250;
