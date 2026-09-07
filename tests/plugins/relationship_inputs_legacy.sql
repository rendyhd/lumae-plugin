-- Compatibility oracle: pre-checkpoint relationship input SQL at 0f16cfb.
SELECT tr.track_id, tr.album_id, al.name, al.album_artist_display,
               tr.artist_display, tr.disc_number, tr.track_number, tr.cover_art_id,
               tr.payload,
               (SELECT MIN(ar.cover_art_id)
                  FROM plugin_lumae_analysis__catalog_artists ar
                 WHERE ar.catalog_instance_id=tr.catalog_instance_id
                   AND ar.published_generation=tr.published_generation
                   AND lower(ar.name)=lower(COALESCE(al.album_artist_display, tr.artist_display))),
               ai.scalar_payload,
               ai.musicnn_vector, ai.musicnn_dimensions
          FROM plugin_lumae_analysis__catalog_tracks tr
          JOIN plugin_lumae_analysis__track_analysis_links ln
            ON ln.catalog_instance_id=tr.catalog_instance_id
           AND ln.projection_generation=%s
           AND ln.provider_track_id=tr.track_id
           AND ln.status='ready'
          JOIN plugin_lumae_analysis__analysis_items ai
            ON ai.catalog_instance_id=ln.catalog_instance_id
           AND ai.projection_generation=ln.projection_generation
           AND ai.analysis_id=ln.analysis_id
          LEFT JOIN plugin_lumae_analysis__catalog_albums al
            ON al.catalog_instance_id=tr.catalog_instance_id
           AND al.published_generation=tr.published_generation
           AND al.album_id=tr.album_id
         WHERE tr.catalog_instance_id=%s AND tr.published_generation=%s
           AND tr.available=TRUE AND tr.analysis_eligible=TRUE
         ORDER BY lower(COALESCE(al.album_artist_display, tr.artist_display)),
                  lower(COALESCE(al.name, '')), COALESCE(tr.disc_number, 0),
                  COALESCE(tr.track_number, 9999), tr.track_id
