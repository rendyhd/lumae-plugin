"""Settings-page panel rendering for the LumaeAnalysis plugin (LUM-020, P3-11 slice 1).

Pure extraction from ``plugins/LumaeAnalysis/__init__.py``: the panel
renderers that build the plugin's ``/settings`` page and their private
helpers. Route handlers (``settings``, ``settings_status``,
``database_state_page``) and ``register`` stay in ``__init__.py``.

Every name this module uses that is defined in ``__init__.py`` (or is
re-exported through it from another extracted module) is looked up on the
package module at call time as ``_pkg.<name>``, not imported directly. That
keeps ``monkeypatch.setattr(plugins.LumaeAnalysis, "name", fake)`` in tests
effective for code that now lives here, since attribute lookups on ``_pkg``
happen when each function runs, not at import time.
"""

from plugins import LumaeAnalysis as _pkg


def _v3_readiness_sources():
    compatibility = _pkg.detect_core()
    if compatibility.adapter != "v3_registry":
        return []
    db = _pkg.get_db()
    if db is None:
        return []
    policy = _pkg.dedup_policy()
    return [
        (
            source,
            _pkg.v3_release_readiness(db, compatibility, source, policy),
        )
        for source in _pkg.resolve_catalog_source(db)
    ]


def _render_basic_source_analysis_panel():
    try:
        sources = _pkg.resolve_catalog_source(_pkg.get_db())
    except Exception:
        _pkg.logger.exception("lumae_analysis could not render basic source analysis")
        sources = []
    cards = []
    for source in sources:
        analysis = source.get("analysis") or {}
        status = str(analysis.get("status") or "not_initialized")
        mapped = int(analysis.get("mapped_track_count") or 0)
        items = int(analysis.get("item_count") or 0)
        if status == "complete" and mapped > 0:
            status_label = "Ready"
            status_class = "lumae-source-state-ready"
            summary = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>AudioMuse source analysis is published for {mapped:,} provider tracks.</strong>
                <span>Lumae can consume these source features without repeating analysis on the
                  phone.</span>
              </div>
            """
        elif status in ("scanning", "building"):
            status_label = "Preparing"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>AudioMuse source analysis is being projected.</strong>
                <span>Available source features will be adopted automatically.</span>
              </div>
            """
        else:
            status_label = "Waiting for analysis"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>No usable AudioMuse source analysis is published yet.</strong>
                <span>Run AudioMuse Analysis or enable its analysis schedule. Lumae will adopt
                  the results automatically.</span>
              </div>
            """
        cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{_pkg.escape(str(source['catalog_instance_id']))}"
              aria-label="AudioMuse source analysis for {_pkg.escape(str(source.get('name') or source['server_id']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Music source</span>
                  <h4>{_pkg.escape(str(source.get('name') or source['server_id']))}</h4>
                </div>
                <span class="lumae-source-state {status_class}">{status_label}</span>
              </header>
              {summary}
              <details>
                <summary>Technical details</summary>
                <div class="lumae-technical-details">
                  <p class="lumae-help">Projection status: {_pkg.escape(status.replace('_', ' '))};
                    mapped provider tracks: {mapped:,}; AudioMuse analysis items: {items:,}.</p>
                </div>
              </details>
            </article>
            """
        )
    if not cards:
        cards.append(
            """
            <article class="lumae-source-card">
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Waiting for a supported music source.</strong>
                <span>Source-analysis status appears automatically after the library source is
                  initialized.</span>
              </div>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="AudioMuse source analysis status">
        <span class="lumae-section-priority lumae-section-advanced">2 - AudioMuse managed</span>
        <h3>2. AudioMuse source analysis</h3>
        <p class="lumae-action-copy">AudioMuse generates the raw MusiCNN, mood, energy, and
          fingerprint inputs. Lumae adopts them; the app does not repeat this work on the phone.</p>
        {''.join(cards)}
      </section>
    """


def render_v3_readiness_panel():
    try:
        sources = _pkg._v3_readiness_sources()
    except Exception:
        _pkg.logger.exception("lumae_analysis could not render AudioMuse 3 readiness")
        return _pkg._render_basic_source_analysis_panel()
    if not sources:
        return _pkg._render_basic_source_analysis_panel()
    cards = []
    for source, readiness in sources:
        blockers = readiness.get("blockers") or []
        blocker_html = "".join(
            f"<li>{_pkg.escape(_pkg._READINESS_BLOCKER_LABELS.get(code, code))}</li>"
            for code in blockers
        )
        mapped = int(readiness.get("mapped_track_count") or 0)
        eligible = int(readiness.get("eligible_track_count") or 0)
        missing = int(readiness.get("missing_mapping_count") or 0)
        fingerprinted = int(readiness.get("chromaprint_track_count") or 0)
        usable_links = int(readiness.get("ready_link_count") or 0)
        verified_links = int(readiness.get("verified_link_count") or 0)
        provisional_links = int(readiness.get("provisional_link_count") or 0)
        pending_links = int(readiness.get("pending_link_count") or 0)
        suspect_links = int(readiness.get("suspect_link_count") or 0)
        missing_links = int(readiness.get("missing_link_count") or 0)
        coverage = float(readiness.get("chromaprint_coverage") or 0) * 100
        task_evidence = readiness.get("task_evidence") or {}
        sequence = bool(task_evidence.get("upgrade_sequence_complete"))
        sequence_label = (
            "unavailable"
            if task_evidence.get("diagnostics_available") is False
            else ("yes" if sequence else "no")
        )
        fully_verified = bool(readiness.get("ready"))
        analysis_sync_allowed = bool(readiness.get("analysis_sync_allowed"))
        if "source_rebind_required" in blockers:
            status_label = "Waiting for source check"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Waiting for Lumae app sync to verify this source automatically.</strong>
                <span>No manual confirmation is needed. The next app sync will prove and adopt
                  the AudioMuse server identity when it matches.</span>
              </div>
            """
        elif fully_verified:
            status_label = "Ready"
            status_class = "lumae-source-state-ready"
            summary = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>AudioMuse source analysis is complete for {verified_links:,} eligible
                  tracks.</strong>
                <span>Mappings and fingerprint evidence are fully verified. Lumae can use the
                  complete source dataset without doing this work on the phone.</span>
              </div>
            """
        elif analysis_sync_allowed:
            status_label = "Preparing"
            status_class = "lumae-source-state-working"
            summary = f"""
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>AudioMuse source analysis is still filling in; {usable_links:,} tracks
                  are usable now.</strong>
                <span>{verified_links:,} are fully verified and {provisional_links:,} remain
                  provisional. AudioMuse’s Analysis task or schedule produces the missing source
                  data; Lumae adopts safe results automatically.</span>
              </div>
            """
        else:
            status_label = "Needs attention"
            status_class = "lumae-source-state-danger"
            summary = """
              <div class="lumae-notice lumae-notice-error" role="alert">
                <strong>AudioMuse source analysis is not safe to use yet.</strong>
                <span>Resolve the measurable issues listed in the technical details below.
                  A manual fresh/upgrade confirmation cannot override them.</span>
              </div>
            """
        cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{_pkg.escape(str(source['catalog_instance_id']))}"
              aria-label="AudioMuse source analysis for {_pkg.escape(str(source.get('name') or source['server_id']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Music source</span>
                  <h4>{_pkg.escape(str(source.get('name') or source['server_id']))}</h4>
                </div>
                <span class="lumae-source-state {status_class}">{status_label}</span>
              </header>
              {summary}
              <details>
                <summary>Technical details</summary>
                <div class="lumae-technical-details">
                  <p class="lumae-help">Chromaprint: {fingerprinted:,} of {mapped:,} mapped tracks
                    ({coverage:.2f}%).</p>
                  <p class="lumae-help">Provider tracks eligible for analysis: {eligible:,};
                    mapped: {mapped:,}; without analysis mapping: {missing:,}. Unmapped provider
                    tracks remain in the Lumae library.</p>
                  <p class="lumae-help">Source-analysis links: {usable_links:,} usable
                    ({verified_links:,} verified; {provisional_links:,} provisional);
                    {pending_links:,} awaiting analysis; {suspect_links:,} flagged for repair;
                    {missing_links:,} not analyzed.</p>
                  <p class="lumae-help">Historical AudioMuse upgrade sequence observed:
                    {sequence_label} (diagnostic only; it does not gate readiness).</p>
                  {f'<ul class="lumae-help">{blocker_html}</ul>' if blocker_html else ''}
                </div>
              </details>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="AudioMuse source analysis status">
        <span class="lumae-section-priority lumae-section-advanced">2 - AudioMuse managed</span>
        <h3>2. AudioMuse source analysis</h3>
        <p class="lumae-action-copy">AudioMuse generates the raw MusiCNN, mood, energy, and
          Chromaprint inputs. Lumae verifies and adopts them progressively; the app does not
          repeat this analysis on the phone.</p>
        {''.join(cards)}
      </section>
    """


def render_relationship_status_panel():
    try:
        db = _pkg.get_db()
        sources = _pkg.resolve_catalog_source(db)
    except Exception:
        _pkg.logger.exception("lumae_analysis could not render relationship preparation")
        return ""
    if not sources:
        return ""
    cards = []
    active_work = False
    for source in sources:
        catalog_instance_id = source["catalog_instance_id"]
        try:
            state = _pkg.relationship_status(db, catalog_instance_id)
        except Exception as exc:
            _pkg.logger.exception(
                "lumae_analysis could not read relationship status for %s",
                catalog_instance_id,
            )
            state = {
                "status": "failed",
                "last_error": str(exc),
            }
        status = str(state.get("status") or "not_initialized")
        catalog_generation = int(source.get("catalog", {}).get("generation") or 0)
        analysis_generation = int(source.get("analysis", {}).get("generation") or 0)
        current = (
            status == "complete"
            and int(state.get("source_catalog_generation") or 0) == catalog_generation
            and int(state.get("source_analysis_generation") or 0) == analysis_generation
            and int(state.get("schema_version") or 0) == _pkg.RELATIONSHIP_SCHEMA_VERSION
            and int(state.get("algorithm_version") or 0) == _pkg.RELATIONSHIP_ALGORITHM_VERSION
        )
        active = status in ("queued", "running")
        active_work = active_work or active
        albums = int(state.get("album_count") or 0)
        artists = int(state.get("artist_count") or 0)
        build_progress = state.get("build_progress") or {}
        build_counts = build_progress.get("counts") or {}
        progress_html = ""
        if active and build_counts:
            if int(build_counts.get("fingerprints_done") or 0) < (
                int(build_counts.get("albums") or 0) + int(build_counts.get("artists") or 0)
            ):
                progress_text = (
                    f"Preparing library signatures: {int(build_counts.get('fingerprints_done') or 0):,} / "
                    f"{int(build_counts.get('albums') or 0) + int(build_counts.get('artists') or 0):,}"
                )
            else:
                progress_text = (
                    f"Album similarities: {int(build_counts.get('albums_done') or 0):,} / "
                    f"{int(build_counts.get('albums') or 0):,}. "
                    f"Artist similarities: {int(build_counts.get('artists_done') or 0):,} / "
                    f"{int(build_counts.get('artists') or 0):,}."
                )
            progress_html = f'<p class="lumae-help" role="status">{_pkg.escape(progress_text)}</p>'
        if current:
            status_label = "Ready"
            status_class = "lumae-source-state-ready"
            summary = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>Similarities are ready for {albums:,} albums and {artists:,} artists.</strong>
                <span>The plugin calculated these with Lumae’s own ranking algorithm. The app
                  downloads the results and does no relationship matching on the phone.</span>
              </div>
            """
        elif active:
            status_label = "Preparing"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Similar album and artist relationships are being prepared automatically.</strong>
                <span>This runs in the background and does not block library sync, playback,
                  or the currently published relationship generation.</span>
              </div>
            """
        elif status == "waiting_for_index":
            status_label = "Waiting for AudioMuse index"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>The bounded relationship build is waiting for AudioMuse's MusicNN index.</strong>
                <span>The previous published relationship generation remains available. Lumae
                  never falls back to an unbounded all-pairs scan.</span>
              </div>
            """
        elif status == "failed":
            status_label = "Needs attention"
            status_class = "lumae-source-state-danger"
            summary = f"""
              <div class="lumae-notice lumae-notice-error" role="alert">
                <strong>The last relationship build failed.</strong>
                <span>{_pkg.escape(_pkg.redact_stored_error(state.get('last_error')) or 'The background worker will retry after the next source update.')}</span>
              </div>
            """
        else:
            status_label = "Waiting for inputs" if catalog_generation == 0 else "Update pending"
            status_class = "lumae-source-state-working"
            summary = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>The automatic relationship build is waiting for published inputs.</strong>
                <span>Once the library and AudioMuse source generation are available, the plugin
                  queues Lumae’s album and artist algorithm automatically.</span>
              </div>
            """
        cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{_pkg.escape(str(catalog_instance_id))}"
              aria-label="Similar albums and artists for {_pkg.escape(str(source.get('name') or source['server_id']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Music source</span>
                  <h4>{_pkg.escape(str(source.get('name') or source['server_id']))}</h4>
                </div>
                <span class="lumae-source-state {status_class}">{status_label}</span>
              </header>
              {summary}
              {progress_html}
              <details>
                <summary>Technical details</summary>
                <div class="lumae-technical-details">
                  <p class="lumae-help">Relationship status: {_pkg.escape(status.replace('_', ' '))};
                    result generation: {int(state.get('generation') or 0):,};
                    albums: {albums:,}; artists: {artists:,}.</p>
                  <p class="lumae-help">Built from library generation
                    {int(state.get('source_catalog_generation') or 0):,} of {catalog_generation:,}
                    and source-analysis generation
                    {int(state.get('source_analysis_generation') or 0):,} of {analysis_generation:,}.
                    Algorithm version: {int(state.get('algorithm_version') or 0):,}.</p>
                </div>
              </details>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="Similar albums and artists status"
        data-lumae-active="{str(active_work).lower()}">
        <span class="lumae-section-priority lumae-section-optional">4 - Automatic background</span>
        <h3>4. Similar albums &amp; artists</h3>
        <p class="lumae-action-copy">The plugin runs Lumae’s own album and artist relationship
          algorithm from the published library and AudioMuse source inputs. It automatically
          rebuilds when either input generation changes.</p>
        {''.join(cards)}
      </section>
    """


def _published_track_count(source):
    entity_counts = (source.get("catalog") or {}).get("entity_counts") or {}
    value = entity_counts.get("track")
    if value is None:
        value = entity_counts.get("tracks")
    return max(int(value or 0), 0)


def render_source_preparation_sections(batch_size):
    try:
        sources = _pkg.resolve_catalog_source(_pkg.get_db())
    except Exception:
        _pkg.logger.exception("lumae_analysis could not render source preparation")
        return "", ""
    catalogue_cards = []
    waveform_cards = []
    active_work = False
    paused = _pkg.maintenance_paused()
    for source in sources:
        catalog_instance_id = source["catalog_instance_id"]
        server_id = source["server_id"]
        # The settings poll reads the committed snapshot (P2-1). The live
        # aggregate runs only when no snapshot describes this generation.
        counts = _pkg._committed_profile_counts(source) or _pkg.analysis_status_counts(
            catalog_instance_id=catalog_instance_id,
            server_id=server_id,
        )
        state = _pkg.preparation_state(catalog_instance_id)
        backfill = _pkg.profile_backfill_state(catalog_instance_id)
        published_tracks = _pkg._published_track_count(source)
        total = int(counts["total_with_files"])
        ready = int(counts["ready_current"])
        coverage = min(max(int(round((ready / total) * 100)) if total else 0, 0), 100)
        queueable = int(counts["needs_analysis"])
        preparation_active = _pkg.preparation_is_active(state)
        backfill_active = _pkg.profile_backfill_is_active(backfill)
        active_work = active_work or preparation_active or backfill_active
        catalog_status = str(source["catalog"]["status"] or "not initialized")
        projection_status = str(source["analysis"]["status"] or "not initialized")
        catalogue_ready = catalog_status == "complete" and published_tracks > 0
        app_ready = catalogue_ready and projection_status == "complete"
        phase = state["phase"] if state else "not started"
        backfill_status = backfill["status"] if backfill else "not started"
        if backfill and backfill["status"] in ("queued", "running") and not backfill_active:
            backfill_status = "stalled; safe to restart"
        last_error = state.get("last_error") if state else None
        backfill_error = backfill.get("last_error") if backfill else None
        hidden = (
            f'<input type="hidden" name="server_id" value="{_pkg.escape(str(server_id))}">'
            f'<input type="hidden" name="catalog_instance_id" '
            f'value="{_pkg.escape(str(catalog_instance_id))}">'
        )
        prepare_disabled = " disabled" if preparation_active or paused else ""
        backfill_disabled = (
            " disabled" if backfill_active or queueable == 0 or paused else ""
        )
        if app_ready:
            source_status = "Ready for app sync"
            source_status_class = "lumae-source-state-ready"
            readiness_notice = f"""
              <div class="lumae-notice lumae-notice-success" role="status">
                <strong>Ready for app sync: {published_tracks:,} Navidrome tracks are published.</strong>
                <span>The library catalogue and app sync index are complete. Volume, ramp, and
                  sonic coverage are reported separately below.</span>
              </div>
            """
        elif catalog_status == "complete" and published_tracks == 0:
            source_status = "Not ready - empty catalogue"
            source_status_class = "lumae-source-state-danger"
            readiness_notice = """
              <div class="lumae-notice lumae-notice-error" role="alert">
                <strong>Not ready: no Navidrome tracks were published.</strong>
                <span>This is not a usable Lumae catalogue. Check Navidrome access and the
                  <em>Music Libraries</em> selection in AudioMuse, then refresh required data.
                  Lumae will no longer publish a new empty catalogue.</span>
              </div>
            """
        elif preparation_active:
            source_status = "Preparing required data"
            source_status_class = "lumae-source-state-working"
            readiness_notice = f"""
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Not ready yet: preparation is in progress.</strong>
                <span>Current phase: {_pkg.escape(str(phase).replace("_", " "))}.</span>
              </div>
            """
        else:
            source_status = "Not ready"
            source_status_class = "lumae-source-state-danger"
            readiness_notice = """
              <div class="lumae-notice lumae-notice-warning" role="status">
                <strong>Not ready for app sync.</strong>
                <span>Publish a non-empty Navidrome catalogue and complete the app sync
                  index by refreshing the required data.</span>
              </div>
            """
        profiles_complete = total > 0 and ready >= total
        if profiles_complete:
            profile_status = "Ready"
            profile_status_class = "lumae-source-state-ready"
        elif backfill_active:
            profile_status = "Preparing"
            profile_status_class = "lumae-source-state-working"
        elif int(counts["failed"]) > 0 and queueable == 0:
            profile_status = "Needs attention"
            profile_status_class = "lumae-source-state-danger"
        elif total == 0:
            profile_status = "Waiting for library"
            profile_status_class = "lumae-source-state-working"
        else:
            profile_status = "Not complete"
            profile_status_class = "lumae-source-state-working"
        catalogue_cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{_pkg.escape(str(catalog_instance_id))}"
              aria-label="Catalogue readiness for {_pkg.escape(str(source['name']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Navidrome source</span>
                  <h4>{_pkg.escape(str(source.get('name') or server_id))}</h4>
                </div>
                <span class="lumae-source-state {source_status_class}">{source_status}</span>
              </header>
              {readiness_notice}
              <div class="lumae-status-grid" aria-label="Required data status">
                <div class="lumae-status-card {'lumae-status-ready' if published_tracks else 'lumae-status-failed'}">
                  <span>Published tracks</span>
                  <strong>{published_tracks:,}</strong>
                </div>
                <div class="lumae-status-card {'lumae-status-ready' if catalogue_ready else 'lumae-status-attention'}">
                  <span>Library catalogue</span>
                  <strong>{_pkg.escape(catalog_status.replace('_', ' '))}</strong>
                </div>
                <div class="lumae-status-card {'lumae-status-ready' if projection_status == 'complete' else 'lumae-status-attention'}">
                  <span>App sync index</span>
                  <strong>{_pkg.escape(projection_status.replace('_', ' '))}</strong>
                </div>
              </div>
              {f'<p class="lumae-notice lumae-notice-error">{_pkg.escape(_pkg.redact_stored_error(last_error))}</p>' if last_error else ''}
              <form class="lumae-form" method="post">
                {hidden}
                <div class="lumae-actions">
                  <button class="lumae-button-primary" type="submit" name="action"
                    value="prepare_lumae"{prepare_disabled}>Refresh required data</button>
                </div>
              </form>
              <p class="lumae-help">This refresh imports the selected Navidrome libraries first,
                then publishes the app sync index. Volume, ramp, and sonic work can continue in
                the background after the library becomes ready.</p>
            </article>
            """
        )
        waveform_cards.append(
            f"""
            <article class="lumae-source-card"
              data-lumae-source="{_pkg.escape(str(catalog_instance_id))}"
              aria-label="Volume and ramp status for {_pkg.escape(str(source['name']))}">
              <header class="lumae-source-header">
                <div>
                  <span class="lumae-kicker">Navidrome source</span>
                  <h4>{_pkg.escape(str(source.get('name') or server_id))}</h4>
                </div>
                <span class="lumae-source-state {profile_status_class}">{profile_status}</span>
              </header>
              <div class="lumae-meter" role="progressbar" aria-label="Ready volume and ramp profiles"
                aria-valuemin="0" aria-valuemax="100" aria-valuenow="{coverage}">
                <div class="lumae-meter-fill" style="width: {coverage}%;"></div>
              </div>
              <p class="lumae-help"><strong>{ready:,} of {total:,} volume and ramp profiles ready.</strong>
                {counts['pending']:,} pending; {queueable:,} need analysis;
                {counts['failed']:,} failed; {counts['skipped']:,} skipped.
                Background worker: {_pkg.escape(backfill_status.replace('_', ' '))}.</p>
              <p class="lumae-help">These profiles power volume normalization and SmoothFade
                ramps. They are prepared from audio waveforms in the background and do not block
                library sync, AudioMuse source analysis, or Lumae relationships.</p>
              {f'<p class="lumae-notice lumae-notice-error">Volume and ramp preparation: {_pkg.escape(_pkg.redact_stored_error(backfill_error))}</p>' if backfill_error else ''}
              <form class="lumae-form" method="post">
                {hidden}
                <label class="lumae-field">
                  <span>Tracks per background batch (1-{_pkg.MAX_BACKFILL_BATCH_SIZE})</span>
                  <input name="backfill_batch_size" value="{batch_size}" inputmode="numeric">
                </label>
                <div class="lumae-actions">
                  <button class="lumae-button-secondary" type="submit" name="action"
                    value="start_backfill"{backfill_disabled}>Prepare missing volume &amp; ramps</button>
                </div>
              </form>
            </article>
            """
        )
    if not catalogue_cards:
        return """
          <section class="lumae-panel" aria-label="Library status">
            <span class="lumae-section-priority">1 - Required</span>
            <h3>1. Library status</h3>
            <p class="lumae-help">No supported AudioMuse music server is available yet.</p>
          </section>
        """, ""
    catalogue_html = f"""
      <section class="lumae-panel" aria-label="Library status">
        <span class="lumae-section-priority">1 - Required</span>
        <h3>1. Library status</h3>
        <p class="lumae-action-copy">“Ready for app sync” has one precise meaning: at least one
          Navidrome track is published and the matching app sync index is complete.
          A completed job with zero tracks is not ready.</p>
        {''.join(catalogue_cards)}
      </section>
    """
    waveform_html = f"""
      <section class="lumae-panel" aria-label="Volume and ramp status"
        data-lumae-active="{str(active_work).lower()}">
        <span class="lumae-section-priority lumae-section-optional">3 - Automatic background</span>
        <h3>3. Volume &amp; ramp status</h3>
        <p class="lumae-action-copy">Loudness profiles normalize volume and MixRamp profiles power
          SmoothFade. Their progress is independent from library readiness, AudioMuse source
          analysis, and the similar-album/artist relationship build.</p>
        {''.join(waveform_cards)}
      </section>
    """
    return catalogue_html, waveform_html


def render_source_preparation_panel(batch_size):
    """Return the required and optional source sections as one HTML fragment."""
    catalogue_html, waveform_html = _pkg.render_source_preparation_sections(batch_size)
    return f"{catalogue_html}{waveform_html}"


def render_provider_identity_panel():
    db = None
    try:
        db = _pkg.get_db()
        sources = _pkg.resolve_catalog_source(db) if db is not None else []
        rows = []
        for source in sources:
            transition = _pkg.provider_transition_health(db, source["catalog_instance_id"])
            if transition:
                rows.append((source, transition))
    except Exception:
        # psycopg2 leaves the entire request transaction aborted after an SQL
        # error. The status panel is optional, so restore the connection before
        # later settings panels call the plugin settings API.
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        _pkg.logger.exception("lumae_analysis could not render provider identity status")
        return ""
    if not rows:
        return ""

    cards = []
    for source, transition in rows:
        state = _pkg.escape(str(transition.get("state") or "normal"))
        version = _pkg.escape(str(transition.get("current_provider_version") or "unverified"))
        action = _pkg.escape(str(transition.get("required_action") or "No action required"))
        counts = transition.get("counts") or {}
        baseline = (
            "passed"
            if transition.get("baseline_integrity") is True
            else ("pending" if transition.get("baseline_integrity") is None else "failed")
        )
        audiomuse_health = _pkg.escape(
            str(transition.get("audiomuse_health") or "not checked")
        )
        scan_count = int(transition.get("target_scan_count") or 0)
        manifest_link = ""
        if transition.get("state") == "applied" and transition.get("transition_id"):
            manifest_link = (
                '<a class="lumae-button lumae-button-secondary" '
                'href="/api/catalog/provider-identity/manifest?transition_id='
                f'{_pkg.escape(str(transition["transition_id"]))}">Download transition manifest</a>'
            )
        cards.append(
            f"""
            <article class="lumae-status-card">
              <span>{_pkg.escape(source['name'])}</span>
              <strong>{state}</strong>
              <small>Navidrome {version}</small>
              <small>Stable target scans: {scan_count}/2</small>
              <small>Exact changes: {int(counts.get('rekey', 0) or 0):,} rekeys,
                {int(counts.get('addition', 0) or 0):,} additions,
                {int(counts.get('confirmed_removal', 0) or 0):,} removals,
                {int(counts.get('conflict', 0) or 0):,} conflicts</small>
              <small>Stored analysis baseline: {baseline}; AudioMuse: {audiomuse_health}</small>
              <small>{action}</small>
              <div class="lumae-actions">{manifest_link}</div>
            </article>
            """
        )
    return f"""
      <section class="lumae-panel" aria-label="Provider identity transition">
        <h3>Provider identity safety</h3>
        <p class="lumae-help">Lumae freezes publication at the old complete generation,
          requires two identical provider scans, and then applies only the exact Navidrome
          canonical-ID transform in one database transaction. AudioMuse health is checked
          separately and never authorizes the Lumae rekey.</p>
        <div class="lumae-status-grid">{''.join(cards)}</div>
        <div class="lumae-actions">
          <a class="lumae-button lumae-button-secondary" href="/backup">Open AudioMuse Backup</a>
          <a class="lumae-button lumae-button-secondary" href="/provider-migration">Open Provider Migration</a>
          <a class="lumae-button lumae-button-secondary" href="">Check again</a>
        </div>
      </section>
    """


# ---------------------------------------------------------------------------
# Per-stream readiness (LUM-017 / P3-9)
#
# Catalogue, analysis projection, waveform profiles, edge profiles and
# relationships are shown as five independent regions: an availability word
# (never colour alone), a last-success time and age, and a job status
# (idle/queued/running/failed/cooling). Every read here reuses an existing,
# already-bounded accessor (P2-1's committed status model, the same
# preparation/backfill/relationship state readers the narrative panels above
# already call); the one addition is ``edge_profile_status``, a single small
# aggregate scoped to one source. No new heavy query is added.
# ---------------------------------------------------------------------------

_READINESS_AVAILABILITY_LABEL = {
    "ready": "Ready", "partial": "Partial", "unavailable": "Unavailable",
}
_READINESS_AVAILABILITY_CLASS = {
    "ready": "lumae-source-state-ready",
    "partial": "lumae-source-state-working",
    "unavailable": "lumae-source-state-danger",
}
_READINESS_JOB_LABEL = {
    "idle": "Idle", "queued": "Queued", "running": "Running",
    "failed": "Failed", "cooling": "Cooling down",
}


def _readiness_availability(ready, partial, ready_text, partial_text, unavailable_text):
    """One of ready / partial / unavailable, with a plain-text reason.

    The caller always renders the word next to any colour, so the state is
    never conveyed by colour alone.
    """
    if ready:
        return "ready", ready_text
    if partial:
        return "partial", partial_text
    return "unavailable", unavailable_text


def _readiness_age_text(iso_value):
    """A short, human age for an ISO timestamp; text, never a bare number."""
    if not iso_value:
        return "no successful run recorded yet"
    try:
        when = _pkg.datetime.fromisoformat(str(iso_value).replace("Z", "+00:00"))
        if when.tzinfo is None:
            when = when.replace(tzinfo=_pkg.timezone.utc)
        seconds = max(0.0, (_pkg.datetime.now(_pkg.timezone.utc) - when).total_seconds())
    except (TypeError, ValueError):
        return "unknown age"
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{int(seconds // 60)} minute(s) ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)} hour(s) ago"
    return f"{int(seconds // 86400)} day(s) ago"


def _readiness_job(status, *, last_error=None, next_retry_at=None, stalled=False):
    """One of idle / queued / running / failed / cooling, with plain-text detail.

    ``last_error`` is redacted here (P3-10's ``redact_stored_error``) so every
    caller gets safe text; the render site still HTML-escapes it.
    """
    if status == "running" and not stalled:
        return "running", "Running now."
    # A queued row with a cooldown still ahead is cooling, not about to start.
    if next_retry_at:
        return "cooling", f"Waiting to retry after a transient failure, until {next_retry_at}."
    if status == "queued" and not stalled:
        return "queued", "Queued to start shortly."
    if last_error:
        return "failed", _pkg.redact_stored_error(last_error) or "Failed."
    return "idle", "Nothing queued."


def _readiness_stream_status(source):
    """The five independent stream states for one catalogue source."""
    catalog_instance_id = source["catalog_instance_id"]
    catalog = source.get("catalog") or {}
    analysis = source.get("analysis") or {}
    paused = _pkg.maintenance_paused()

    try:
        prep = _pkg.preparation_state(catalog_instance_id)
    except Exception:
        _pkg._rollback_if_possible(_pkg.get_db())
        prep = None
    prep_active = _pkg.preparation_is_active(prep)
    prep_stalled = bool(prep) and prep.get("status") in ("queued", "running") and not prep_active

    try:
        backfill = _pkg.profile_backfill_state(catalog_instance_id)
    except Exception:
        _pkg._rollback_if_possible(_pkg.get_db())
        backfill = None
    backfill_active = _pkg.profile_backfill_is_active(backfill)
    backfill_stalled = (
        bool(backfill) and backfill.get("status") in ("queued", "running") and not backfill_active
    )

    counts = _pkg._committed_profile_counts(source)

    try:
        edge = _pkg.edge_profile_status(_pkg.get_db(), catalog_instance_id)
    except Exception:
        _pkg._rollback_if_possible(_pkg.get_db())
        edge = None

    try:
        relationship = _pkg.relationship_status(_pkg.get_db(), catalog_instance_id)
    except Exception:
        _pkg._rollback_if_possible(_pkg.get_db())
        relationship = None

    streams = []

    # 1. Catalogue
    published = _pkg._published_track_count(source)
    catalog_ready = catalog.get("status") == "complete" and published > 0
    catalog_partial = prep_active or (
        catalog.get("status") not in (None, "not_initialized") and not catalog_ready
    )
    availability, availability_text = _pkg._readiness_availability(
        catalog_ready, catalog_partial,
        f"{published:,} tracks published.",
        "The catalogue refresh has not finished yet.",
        "No catalogue has been published yet.",
    )
    job_state, job_text = _pkg._readiness_job(
        prep.get("status") if prep else None,
        last_error=(prep or {}).get("last_error"),
        stalled=prep_stalled,
    )
    streams.append({
        "key": "catalogue", "label": "Catalogue",
        "availability": availability, "availability_text": availability_text,
        "last_success": catalog.get("completed_at"),
        "job_state": job_state, "job_text": job_text,
        "retry_action": "prepare_lumae", "retry_label": "Retry catalogue refresh",
        "retry_disabled": prep_active or paused,
    })

    # 2. Analysis projection
    analysis_ready = catalog_ready and analysis.get("status") == "complete"
    analysis_partial = prep_active and not analysis_ready
    availability, availability_text = _pkg._readiness_availability(
        analysis_ready, analysis_partial,
        f"Projection generation {int(analysis.get('generation') or 0):,} is published.",
        "The analysis projection has not finished yet.",
        "No analysis projection has been published yet.",
    )
    job_state, job_text = _pkg._readiness_job(
        prep.get("status") if prep else None,
        last_error=(prep or {}).get("last_error"),
        stalled=prep_stalled,
    )
    streams.append({
        "key": "analysis", "label": "Analysis projection",
        "availability": availability, "availability_text": availability_text,
        "last_success": analysis.get("completed_at"),
        "job_state": job_state, "job_text": job_text,
        "retry_action": "prepare_lumae", "retry_label": "Retry analysis projection",
        "retry_disabled": prep_active or paused,
    })

    # 3. Waveform profiles
    total = int((counts or {}).get("total_with_files") or 0)
    ready_count = int((counts or {}).get("ready_current") or 0)
    needs = int((counts or {}).get("needs_analysis") or 0)
    waveform_ready = counts is not None and total > 0 and ready_count >= total
    waveform_partial = counts is not None and (
        backfill_active or (total > 0 and 0 < ready_count < total)
    )
    availability, availability_text = _pkg._readiness_availability(
        waveform_ready, waveform_partial,
        f"{ready_count:,} of {total:,} profiles ready.",
        (f"{ready_count:,} of {total:,} profiles ready; {needs:,} still need analysis."
         if counts is not None else "Waveform profile counts could not be read."),
        ("Waveform profile counts could not be read." if counts is None
         else "No waveform profiles have been published yet."),
    )
    next_retry_at = _pkg._future_retry_at((backfill or {}).get("next_retry_at"))
    job_state, job_text = _pkg._readiness_job(
        backfill.get("status") if backfill else None,
        last_error=(backfill or {}).get("last_error"),
        next_retry_at=next_retry_at,
        stalled=backfill_stalled,
    )
    streams.append({
        "key": "waveform", "label": "Waveform profiles",
        "availability": availability, "availability_text": availability_text,
        "last_success": (backfill or {}).get("completed_at") or (counts or {}).get("counted_at"),
        "job_state": job_state, "job_text": job_text,
        "retry_action": "start_backfill", "retry_label": "Retry waveform profiles",
        "retry_disabled": backfill_active or needs == 0 or paused,
    })

    # 4. Edge profiles
    edge_ready_count = int((edge or {}).get("ready") or 0)
    edge_active_count = int((edge or {}).get("active") or 0)
    edge_failed_count = int((edge or {}).get("failed") or 0)
    edge_ready = (
        edge is not None and edge_failed_count == 0 and edge_active_count == 0
        and edge_ready_count > 0
    )
    edge_partial = (
        edge is not None and edge_ready_count > 0
        and (edge_active_count > 0 or edge_failed_count > 0)
    )
    availability, availability_text = _pkg._readiness_availability(
        edge_ready, edge_partial,
        f"{edge_ready_count:,} edge profiles published.",
        f"{edge_ready_count:,} published, {edge_active_count:,} in progress, "
        f"{edge_failed_count:,} failed.",
        ("Edge profile status could not be read." if edge is None
         else "No edge profiles have been published yet."),
    )
    job_state, job_text = _pkg._readiness_job(
        "running" if edge_active_count else None,
        last_error=(edge or {}).get("last_error"),
    )
    streams.append({
        "key": "edge", "label": "Edge profiles",
        "availability": availability, "availability_text": availability_text,
        "last_success": (edge or {}).get("last_success_at"),
        "job_state": job_state, "job_text": job_text,
        "retry_action": "retry_edge_profiles", "retry_label": "Retry edge profiles",
        "retry_disabled": paused or not _pkg.edge_profiles_enabled(),
    })

    # 5. Relationships
    rel_status = str(
        (relationship or {}).get("status")
        or ("unavailable" if relationship is None else "not_initialized")
    )
    rel_active = rel_status in ("queued", "running")
    rel_current = (
        rel_status == "complete"
        and int((relationship or {}).get("source_catalog_generation") or 0)
        == int(catalog.get("generation") or 0)
        and int((relationship or {}).get("source_analysis_generation") or 0)
        == int(analysis.get("generation") or 0)
    )
    availability, availability_text = _pkg._readiness_availability(
        rel_current,
        rel_active or rel_status == "waiting_for_index",
        f"{int((relationship or {}).get('album_count') or 0):,} albums, "
        f"{int((relationship or {}).get('artist_count') or 0):,} artists.",
        ("Relationships are being prepared." if rel_active
         else "Waiting on AudioMuse's index." if rel_status == "waiting_for_index"
         else "Relationship status could not be read." if relationship is None
         else "The published relationships are stale."),
        "No relationships have been built yet.",
    )
    job_state, job_text = _pkg._readiness_job(
        rel_status if rel_active else None,
        last_error=(relationship or {}).get("last_error") if rel_status == "failed" else None,
    )
    streams.append({
        "key": "relationships", "label": "Relationships",
        "availability": availability, "availability_text": availability_text,
        "last_success": (relationship or {}).get("completed_at"),
        "job_state": job_state, "job_text": job_text,
        "retry_action": "retry_relationships", "retry_label": "Retry relationships",
        "retry_disabled": rel_active or paused,
    })

    return streams


def render_readiness_streams_panel():
    try:
        sources = _pkg.resolve_catalog_source(_pkg.get_db())
    except Exception:
        _pkg.logger.exception("lumae_analysis could not render per-stream readiness")
        return ""
    if not sources:
        return ""
    cards = []
    live_parts = []
    for source in sources:
        catalog_instance_id = source["catalog_instance_id"]
        server_id = source["server_id"]
        hidden = (
            f'<input type="hidden" name="server_id" value="{_pkg.escape(str(server_id))}">'
            f'<input type="hidden" name="catalog_instance_id" '
            f'value="{_pkg.escape(str(catalog_instance_id))}">'
        )
        try:
            streams = _pkg._readiness_stream_status(source)
        except Exception:
            _pkg.logger.exception(
                "lumae_analysis could not read per-stream readiness for %s",
                catalog_instance_id,
            )
            continue
        source_name = _pkg.escape(str(source.get("name") or server_id))
        for stream in streams:
            state_class = _pkg._READINESS_AVAILABILITY_CLASS[stream["availability"]]
            state_label = _pkg._READINESS_AVAILABILITY_LABEL[stream["availability"]]
            job_label = _pkg._READINESS_JOB_LABEL.get(
                stream["job_state"], str(stream["job_state"]).title()
            )
            disabled = " disabled" if stream["retry_disabled"] else ""
            cards.append(
                f"""
                <article class="lumae-source-card"
                  data-lumae-source="{_pkg.escape(str(catalog_instance_id))}"
                  aria-label="{_pkg.escape(stream['label'])} readiness for {source_name}">
                  <header class="lumae-source-header">
                    <div>
                      <span class="lumae-kicker">{source_name}</span>
                      <h4>{_pkg.escape(stream['label'])}</h4>
                    </div>
                    <span class="lumae-source-state {state_class}">{state_label}</span>
                  </header>
                  <p class="lumae-help">{_pkg.escape(stream['availability_text'])}</p>
                  <p class="lumae-help">Last success:
                    {_pkg.escape(_pkg._readiness_age_text(stream['last_success']))}.</p>
                  <p class="lumae-help">Job: {job_label}. {_pkg.escape(stream['job_text'])}</p>
                  <form class="lumae-form" method="post">
                    {hidden}
                    <div class="lumae-actions">
                      <button class="lumae-button-secondary" type="submit" name="action"
                        value="{stream['retry_action']}"{disabled}>{_pkg.escape(stream['retry_label'])}</button>
                    </div>
                  </form>
                </article>
                """
            )
            live_parts.append(f"{stream['label']}: {state_label}, {job_label}")
    if not cards:
        return ""
    return f"""
      <section class="lumae-panel" aria-label="Readiness by stream">
        <span class="lumae-section-priority">Overview</span>
        <h3>Readiness by stream</h3>
        <p class="lumae-action-copy">Catalogue, analysis projection, waveform profiles, edge
          profiles and relationships are independent streams: a delay in one never blocks the
          others, and each has its own scoped retry below.</p>
        <p aria-live="polite" class="lumae-help">{_pkg.escape('; '.join(live_parts))}.</p>
        {''.join(cards)}
      </section>
    """


def _reconcile_duration(milliseconds):
    if milliseconds is None:
        return "running"
    value = max(0, int(milliseconds or 0))
    if value < 1000:
        return f"{value} ms"
    seconds = value / 1000
    if seconds < 60:
        return f"{seconds:.1f} s"
    return f"{seconds / 60:.1f} min"


def _reconcile_event_summary(event):
    summary = event.get("summary") or {}
    if isinstance(summary, str):
        try:
            summary = _pkg.json.loads(summary)
        except (TypeError, ValueError):
            summary = {}
    parts = []
    for key in (
        "songs_seen",
        "processed",
        "attempted",
        "ready",
        "failed",
        "skipped",
        "already_ready",
        "promoted",
        "generation",
        "changes",
        "album_count",
        "artist_count",
        "track_count",
    ):
        if key in summary:
            parts.append(f"{key.replace('_', ' ')}: {_pkg.escape(str(summary[key]))}")
    return "; ".join(parts) or _pkg.escape(str(summary.get("status") or event.get("phase") or "complete"))


def render_reconcile_status_panel():
    try:
        snapshot = _pkg.read_reconcile_status(_pkg.get_db())
    except Exception:
        db = _pkg.get_db()
        _pkg._rollback_if_possible(db)
        _pkg.logger.exception("lumae_analysis could not render reconcile status")
        return """
          <section class="lumae-panel" aria-label="Background reconcile status">
            <span class="lumae-section-priority lumae-section-advanced">Scheduler</span>
            <h3>Background reconcile status is unavailable</h3>
            <p class="lumae-help">The operational status tables could not be read. Published
              Lumae data is unaffected; restart AudioMuse after verifying the plugin migration.</p>
          </section>
        """
    control = snapshot["control"]
    mode = control.get("mode") or "unknown"
    cadence = {
        "active": "Every minute while work is ready",
        "waiting": "Every five minutes while AudioMuse analysis finishes",
        "backoff": f"Retry schedule: {control.get('cron_expr') or 'adaptive'}",
        "idle": "Hourly safety sweep at :11",
        "paused": "Paused; hourly safety check at :11",
    }.get(mode, "Schedule unavailable")
    pending = snapshot.get("pending") or {}
    pending_html = "".join(
        f"<li><strong>{int(count):,}</strong> {_pkg.escape(str(label))}</li>"
        for label, count in pending.items()
    ) or "<li><strong>0</strong> pending actions</li>"
    events = snapshot.get("events") or []
    running = next((event for event in events if event.get("status") == "running"), None)
    running_html = ""
    if running:
        progress = ""
        if running.get("progress_total") is not None:
            progress = (
                f" · {int(running.get('progress_current') or 0):,}/"
                f"{int(running.get('progress_total') or 0):,}"
            )
        running_html = f"""
          <div class="lumae-notice lumae-notice-info" role="status">
            <strong>{_pkg.escape(str(running.get('action') or 'background work').replace('_', ' ').title())}</strong>
            <span>{_pkg.escape(str(running.get('phase') or 'running'))}{progress}; attempt
              {int(running.get('attempt') or 1)}.</span>
          </div>
        """
    rows = []
    for event in events:
        if event.get("status") == "running":
            continue
        status = str(event.get("status") or "unknown")
        if status == "success" and event.get("phase") == "completed with warnings":
            status = "completed with warnings"
        retry = (
            f"; retry {_pkg.escape(_pkg.reconcile_iso(event.get('next_retry_at')) or '')}"
            if event.get("next_retry_at")
            else ""
        )
        error = (
            f'<div class="lumae-help">{_pkg.escape(_pkg.redact_stored_error(event.get("last_error")))}</div>'
            if event.get("last_error")
            else ""
        )
        rows.append(
            f"""
            <li>
              <strong>{_pkg.escape(str(event.get('action') or '').replace('_', ' ').title())}</strong>
              — {_pkg.escape(status)},
              {_pkg._reconcile_duration(event.get('duration_ms'))}{retry}
              <div class="lumae-help">{_pkg._reconcile_event_summary(event)}</div>
              {error}
            </li>
            """
        )
    journal_html = "".join(rows) or "<li>No meaningful background actions recorded yet.</li>"
    next_retry = (
        f"<p class=\"lumae-help\">Next retry: "
        f"{_pkg.escape(_pkg.reconcile_iso(control.get('next_retry_at')) or '')}.</p>"
        if control.get("next_retry_at")
        else ""
    )
    return f"""
      <section class="lumae-panel" aria-label="Background reconcile status"
        data-lumae-active="{str(bool(running)).lower()}">
        <span class="lumae-section-priority lumae-section-advanced">Scheduler</span>
        <h3>Background reconcile is {_pkg.escape(str(mode).replace('_', ' '))}</h3>
        <p class="lumae-action-copy">{_pkg.escape(cadence)}. AudioMuse may label these tasks
          “Songs analyzed: 0”; the action and phase below are Lumae’s authoritative status.</p>
        {running_html}
        <ul class="lumae-help">{pending_html}</ul>
        {next_retry}
        <details>
          <summary>Recent meaningful background actions</summary>
          <ul>{journal_html}</ul>
        </details>
      </section>
    """


def render_settings_status_panels(batch_size):
    readiness_html = _pkg.render_v3_readiness_panel()
    relationships_html = _pkg.render_relationship_status_panel()
    catalogue_html, waveform_html = _pkg.render_source_preparation_sections(batch_size)
    return {
        "readiness": readiness_html,
        "relationships": relationships_html,
        "catalogue": catalogue_html,
        "waveform": waveform_html,
        "reconcile": _pkg.render_reconcile_status_panel(),
        "identity": _pkg.render_provider_identity_panel(),
        "stream_status": _pkg.render_readiness_streams_panel(),
    }


def render_publication_repair_form():
    """Repair D (P3-7), offered while the persisted integrity counts say so."""
    try:
        db = _pkg.get_db()
        cur = db.cursor()
    except Exception:
        return ""
    try:
        counts = _pkg._persisted_profile_integrity(cur)
    except Exception:
        # A failed statement aborts the transaction the other panels use.
        _pkg._rollback_if_possible(db)
        return ""
    finally:
        cur.close()
    orphaned = int(counts.get("profiles_orphaned") or 0)
    unpublished = int(counts.get("profiles_unpublished_ready") or 0)
    if not orphaned and not unpublished:
        return ""
    return f"""
        <form class="lumae-form" method="post">
          <p class="lumae-action-copy">{_pkg.format_count(orphaned)} published profiles belong to
            tracks no longer in the catalogue and {_pkg.format_count(unpublished)} ready profiles
            have no published row (counted {_pkg.escape(str(counts.get("profiles_checked_at") or "at start"))}).
            The repair withdraws the first and republishes the second in short batches.</p>
          <button class="lumae-button-secondary" type="submit" name="action"
            value="repair_profile_publications">Repair profile publications</button>
        </form>
    """


def render_settings(message=None, error=None):
    batch_size = _pkg.configured_backfill_limit()
    paused = _pkg.maintenance_paused()
    message_html = (
        f"""
        <div class="lumae-notice lumae-notice-success" role="status">
          {_pkg.escape(message)}
        </div>
        """
        if message
        else ""
    )
    error_html = (
        f"""
        <div class="lumae-notice lumae-notice-error" role="alert">
          <strong>{_pkg.escape(_pkg.redact_stored_error(error))}</strong>
        </div>
        """
        if error
        else ""
    )
    panels = {
        name: (
            f'<div data-lumae-status-panel="{name}"{" hidden" if not html.strip() else ""}>'
            f'{html}</div>'
        )
        for name, html in _pkg.render_settings_status_panels(batch_size).items()
    }
    maintenance_html = f"""
      <section class="lumae-panel" aria-label="Background maintenance control">
        <span class="lumae-section-priority">Safety control</span>
        <h3>Background maintenance is {'paused' if paused else 'enabled'}</h3>
        <p class="lumae-action-copy">Pausing prevents new catalogue, projection, waveform,
          and relationship work from starting. Already published app data remains available.</p>
        <form class="lumae-form" method="post">
          <button class="lumae-button-secondary" type="submit" name="action"
            value="{'resume_maintenance' if paused else 'pause_maintenance'}">
            {'Resume background maintenance' if paused else 'Pause background maintenance'}
          </button>
        </form>
        {_pkg.render_publication_repair_form()}
      </section>
    """
    return _pkg.render_page(
        f"""
        <style>
          .lumae-analysis-settings {{
            --lumae-ink: #17202a;
            --lumae-muted: #5f6f7f;
            --lumae-line: #d9e2ea;
            --lumae-panel: #ffffff;
            --lumae-soft: #f6f8fb;
            --lumae-accent: #2f6fed;
            --lumae-ready: #247a5a;
            --lumae-warn: #b46b00;
            --lumae-danger: #b42318;
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 12px;
            box-sizing: border-box;
            color: var(--lumae-ink);
            display: grid;
            gap: 18px;
            max-width: 920px;
            padding: 20px;
            width: 100%;
          }}

          .lumae-hero {{
            border-bottom: 1px solid var(--lumae-line);
            display: grid;
            gap: 10px;
            padding-bottom: 18px;
          }}

          .lumae-kicker {{
            color: var(--lumae-muted);
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0;
            text-transform: uppercase;
          }}

          .lumae-hero h2 {{
            color: var(--lumae-ink);
            font-size: clamp(1.5rem, 3vw, 2.15rem);
            line-height: 1.1;
            margin: 0;
          }}

          .lumae-hero p,
          .lumae-action-copy,
          .lumae-help {{
            color: var(--lumae-muted);
            line-height: 1.55;
            margin: 0;
          }}

          .lumae-coverage {{
            background: var(--lumae-soft);
            border: 1px solid var(--lumae-line);
            border-radius: 8px;
            display: grid;
            gap: 10px;
            padding: 14px;
          }}

          .lumae-coverage-row {{
            align-items: baseline;
            display: flex;
            gap: 12px;
            justify-content: space-between;
          }}

          .lumae-coverage strong {{
            font-size: 1.1rem;
          }}

          .lumae-source-card {{
            background: var(--lumae-soft);
            border: 1px solid var(--lumae-line);
            border-radius: 10px;
            display: grid;
            gap: 14px;
            padding: 16px;
          }}

          .lumae-source-header {{
            align-items: flex-start;
            display: flex;
            gap: 12px;
            justify-content: space-between;
          }}

          .lumae-source-header h4 {{
            color: var(--lumae-ink);
            font-size: 1.15rem;
            margin: 3px 0 0;
          }}

          .lumae-source-state {{
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 999px;
            color: var(--lumae-ink);
            font-size: 0.76rem;
            font-weight: 800;
            padding: 5px 9px;
            white-space: nowrap;
          }}

          .lumae-source-state-ready {{
            background: #e9f6ef;
            border-color: #a7d8bd;
            color: #14543c;
          }}

          .lumae-source-state-danger {{
            background: #fff0ed;
            border-color: #ffb4a8;
            color: var(--lumae-danger);
          }}

          .lumae-source-state-working {{
            background: #fff8eb;
            border-color: #f2c879;
            color: #6f4200;
          }}

          .lumae-meter {{
            background: #dce5ed;
            border-radius: 999px;
            height: 10px;
            overflow: hidden;
          }}

          .lumae-meter-fill {{
            background: linear-gradient(90deg, var(--lumae-ready), var(--lumae-accent));
            height: 100%;
          }}

          .lumae-status-grid {{
            display: grid;
            gap: 10px;
            grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
          }}

          .lumae-status-card {{
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 8px;
            display: grid;
            gap: 8px;
            min-height: 88px;
            padding: 14px;
          }}

          .lumae-status-card span {{
            color: var(--lumae-muted);
            font-size: 0.82rem;
            font-weight: 700;
          }}

          .lumae-status-card strong {{
            color: var(--lumae-ink);
            font-size: 1.15rem;
            line-height: 1.2;
            overflow-wrap: anywhere;
          }}

          .lumae-status-attention {{
            border-color: #f2c879;
          }}

          .lumae-status-attention strong {{
            color: var(--lumae-warn);
          }}

          .lumae-status-ready strong {{
            color: var(--lumae-ready);
          }}

          .lumae-status-pending strong {{
            color: var(--lumae-accent);
          }}

          .lumae-status-failed strong {{
            color: var(--lumae-danger);
          }}

          .lumae-status-muted strong {{
            color: #405163;
          }}

          .lumae-panel {{
            background: var(--lumae-panel);
            border: 1px solid var(--lumae-line);
            border-radius: 10px;
            display: grid;
            gap: 14px;
            padding: 18px;
          }}

          .lumae-panel h3 {{
            color: var(--lumae-ink);
            font-size: 1.15rem;
            margin: 0;
          }}

          .lumae-panel details {{
            border-top: 1px solid var(--lumae-line);
            padding-top: 10px;
          }}

          .lumae-panel summary {{
            color: var(--lumae-ink);
            cursor: pointer;
            font-weight: 700;
          }}

          .lumae-technical-details {{
            display: grid;
            gap: 8px;
            padding-top: 10px;
          }}

          .lumae-section-priority {{
            color: var(--lumae-accent);
            font-size: 0.72rem;
            font-weight: 800;
            text-transform: uppercase;
          }}

          .lumae-section-optional {{
            color: var(--lumae-ready);
          }}

          .lumae-section-advanced {{
            color: var(--lumae-warn);
          }}

          .lumae-form {{
            display: grid;
            gap: 16px;
          }}

          .lumae-field {{
            display: grid;
            gap: 6px;
            max-width: 260px;
          }}

          .lumae-field span {{
            color: var(--lumae-ink);
            font-weight: 700;
          }}

          .lumae-field input {{
            border: 1px solid var(--lumae-line);
            border-radius: 8px;
            color: var(--lumae-ink);
            font: inherit;
            padding: 9px 10px;
          }}

          .lumae-toggle {{
            align-items: center;
            display: flex;
            gap: 10px;
            font-weight: 700;
          }}

          .lumae-toggle span {{
            color: var(--lumae-ink);
          }}

          .lumae-toggle input {{
            height: 20px;
            width: 20px;
          }}

          .lumae-actions {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
          }}

          .lumae-actions button,
          .lumae-actions .lumae-button {{
            border-radius: 8px;
            cursor: pointer;
            font-weight: 700;
            min-height: 40px;
            padding: 9px 14px;
            text-decoration: none;
          }}

          .lumae-button-primary {{
            background: var(--lumae-accent);
            border: 1px solid var(--lumae-accent);
            color: #ffffff;
          }}

          .lumae-button-secondary {{
            background: #ffffff;
            border: 1px solid var(--lumae-line);
            color: var(--lumae-ink);
          }}

          .lumae-button-caution {{
            background: #fff8eb;
            border: 1px solid #f2c879;
            color: #6f4200;
          }}

          .lumae-action-notes {{
            display: grid;
            gap: 6px;
          }}

          .lumae-notice {{
            border-radius: 8px;
            display: grid;
            gap: 4px;
            padding: 12px 14px;
          }}

          .lumae-notice strong {{
            color: inherit;
          }}

          .lumae-notice-success {{
            background: #e9f6ef;
            border: 1px solid #a7d8bd;
            color: #14543c;
          }}

          .lumae-notice-error {{
            background: #fff0ed;
            border: 1px solid #ffb4a8;
            color: var(--lumae-danger);
          }}

          .lumae-notice-warning {{
            background: #fff8eb;
            border: 1px solid #f2c879;
            color: #6f4200;
          }}

          @media (max-width: 620px) {{
            .lumae-analysis-settings {{
              padding: 14px;
            }}

            .lumae-panel,
            .lumae-source-card {{
              padding: 14px;
            }}

            .lumae-source-header {{
              align-items: flex-start;
              flex-direction: column;
            }}

            .lumae-source-state {{
              white-space: normal;
            }}

            .lumae-status-grid {{
              grid-template-columns: 1fr;
            }}

            .lumae-field {{
              max-width: none;
            }}

            .lumae-actions,
            .lumae-actions button,
            .lumae-actions .lumae-button {{
              width: 100%;
            }}
          }}
        </style>

        <section class="lumae-analysis-settings" aria-label="Lumae analysis settings"
          data-status-url="{_pkg.escape(_pkg.url_for('lumae_analysis.settings_status'))}">
          {message_html}
          {error_html}

          <header class="lumae-hero">
            <span class="lumae-kicker">Lumae status</span>
            <h2>Four clear stages from library to Lumae recommendations.</h2>
            <p>Library readiness controls app sync. AudioMuse supplies raw source analysis;
              volume and ramp profiles improve playback; Lumae then prepares similar albums and
              artists with its own algorithm. Ready never means a completed but empty library.</p>
            <p class="lumae-help" data-lumae-refresh-notice role="status">Status updates
              automatically without reloading this page.</p>
            <div class="lumae-actions">
              <a class="lumae-button lumae-button-secondary" href="database-state">
                View database state
              </a>
            </div>
          </header>

          {maintenance_html}
          {panels['stream_status']}
          {panels['reconcile']}
          {panels['catalogue']}
          {panels['identity']}
          {panels['readiness']}
          {panels['waveform']}
          {panels['relationships']}
          {_pkg.render_collections_settings_panel()}
        </section>
        {_pkg.SETTINGS_STATUS_SCRIPT}
        """,
        title="Lumae Analysis",
    )
