SET max_parallel_workers_per_gather=0;
\echo === coverage (catalog_readiness._coverage)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) SELECT count(*), count(m.provider_track_id), count(CASE WHEN cp.fingerprint IS NOT NULL THEN 1 END), max(EXTRACT(EPOCH FROM cp.updated_at))
  FROM plugin_lumae_analysis__catalog_tracks ct LEFT JOIN track_server_map m ON m.server_id='server-a' AND m.provider_track_id=ct.track_id
  LEFT JOIN chromaprint cp ON cp.server_id=m.server_id AND cp.provider_track_id=m.provider_track_id
 WHERE ct.catalog_instance_id='8f881303-aa56-4484-9a69-8f208c79f533' AND ct.published_generation=1 AND ct.available AND ct.analysis_eligible;
\echo === link coverage
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) SELECT count(*) FILTER (WHERE status='ready'), count(*) FILTER (WHERE status='pending'),
 count(*) FILTER (WHERE status='suspect' OR review_state IN ('needs_repair','needs_review')), count(*) FILTER (WHERE status='missing'),
 count(*) FILTER (WHERE status='ready' AND evidence_complete), count(*) FILTER (WHERE status='ready' AND NOT evidence_complete)
 FROM plugin_lumae_analysis__track_analysis_links WHERE catalog_instance_id='8f881303-aa56-4484-9a69-8f208c79f533' AND projection_generation=2;
\echo === task evidence
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) SELECT task_id, task_type, status, end_time, details, timestamp FROM task_status
 WHERE parent_task_id IS NULL AND task_type IN ('cleaning','main_analysis') AND status='SUCCESS'
 ORDER BY COALESCE(end_time, EXTRACT(EPOCH FROM timestamp)) DESC LIMIT 100;
\echo === audiomuse health active-task probe (runs when transition state=applied)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) SELECT COUNT(*) FROM task_status WHERE status NOT IN ('SUCCESS','FAILURE','FAIL','REVOKED') AND (task_type ILIKE '%migration%' OR task_type IN ('main_analysis','cleaning'));
\echo === analysis_status_counts (settings poll)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON)
 WITH source AS (SELECT s.catalog_instance_id, c.published_generation FROM plugin_lumae_analysis__catalog_sources s JOIN plugin_lumae_analysis__catalog_state c USING (catalog_instance_id)
   WHERE s.rebind_status='active' AND c.status='complete' ORDER BY s.is_default DESC, s.server_name, s.catalog_instance_id LIMIT 1)
 SELECT COUNT(*), COUNT(*) FILTER (WHERE p.status='ready' AND p.analyzer_ver>=1 AND p.media_signature IS NOT DISTINCT FROM ('catalog-media:'||COALESCE(t.media_fp,''))),
   COUNT(*) FILTER (WHERE p.status IN ('pending','pending_interactive')), COUNT(*) FILTER (WHERE p.status='failed'), COUNT(*) FILTER (WHERE p.status='skipped_no_file' AND NOT true)
 FROM source JOIN plugin_lumae_analysis__catalog_tracks t ON t.catalog_instance_id=source.catalog_instance_id AND t.published_generation=source.published_generation
 LEFT JOIN plugin_lumae_analysis__source_profiles p ON p.track_id=t.track_id AND p.catalog_instance_id=source.catalog_instance_id
 WHERE t.available AND t.analysis_eligible;
