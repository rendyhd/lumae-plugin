"""Read-only, source-scoped database diagnostics for Lumae Analysis.

The dashboard deliberately uses aggregate queries against the currently
published catalogue and analysis generations.  It never enumerates track
metadata, exposes credentials, or mutates database state.
"""

from datetime import datetime, timezone
from html import escape

from plugin.api import table


def _iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _error(errors, section, exc):
    errors.append({"section": section, "message": str(exc)[:500]})


def _fetchone(db, sql, params, errors, section, default):
    cur = db.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchone() or default
    except Exception as exc:
        _error(errors, section, exc)
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        return default
    finally:
        cur.close()


def _fetchall(db, sql, params, errors, section):
    cur = db.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception as exc:
        _error(errors, section, exc)
        rollback = getattr(db, "rollback", None)
        if callable(rollback):
            rollback()
        return []
    finally:
        cur.close()


def _link_state(db, source, errors):
    row = _fetchone(
        db,
        f"""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status='ready') AS usable,
               count(*) FILTER (
                 WHERE status='ready' AND evidence_complete=TRUE
               ) AS verified,
               count(*) FILTER (
                 WHERE status='ready' AND evidence_complete=FALSE
               ) AS provisional,
               count(*) FILTER (WHERE status='pending') AS pending,
               count(*) FILTER (
                 WHERE status='suspect'
                    OR review_state IN ('needs_repair', 'needs_review')
               ) AS suspect,
               count(*) FILTER (WHERE status='missing') AS missing,
               count(DISTINCT analysis_id) FILTER (
                 WHERE status='ready' AND analysis_id IS NOT NULL
               ) AS usable_analysis_ids
          FROM {table("track_analysis_links")}
         WHERE catalog_instance_id=%s AND projection_generation=%s
        """,
        (
            source["catalog_instance_id"],
            source.get("analysis", {}).get("generation", 0),
        ),
        errors,
        "sonic links",
        (0,) * 8,
    )
    keys = (
        "total",
        "usable",
        "verified",
        "provisional",
        "pending",
        "suspect",
        "missing",
        "usable_analysis_ids",
    )
    return {key: int(value or 0) for key, value in zip(keys, row)}


def _analysis_item_state(db, source, errors):
    row = _fetchone(
        db,
        f"""
        SELECT count(*) AS items,
               count(*) FILTER (WHERE musicnn_vector IS NOT NULL) AS musicnn,
               count(*) FILTER (WHERE clap_vector IS NOT NULL) AS clap
          FROM {table("analysis_items")}
         WHERE catalog_instance_id=%s AND projection_generation=%s
        """,
        (
            source["catalog_instance_id"],
            source.get("analysis", {}).get("generation", 0),
        ),
        errors,
        "analysis items",
        (0, 0, 0),
    )
    return {
        "items": int(row[0] or 0),
        "musicnn_vectors": int(row[1] or 0),
        "clap_vectors": int(row[2] or 0),
    }


def _group_state(db, source, errors):
    row = _fetchone(
        db,
        f"""
        SELECT count(*) AS analysis_groups,
               count(*) FILTER (WHERE occurrences > 1) AS shared_groups,
               COALESCE(max(occurrences), 0) AS largest_group
          FROM (
            SELECT analysis_id, count(*) AS occurrences
              FROM {table("track_analysis_links")}
             WHERE catalog_instance_id=%s AND projection_generation=%s
               AND status='ready' AND analysis_id IS NOT NULL
             GROUP BY analysis_id
          ) groups
        """,
        (
            source["catalog_instance_id"],
            source.get("analysis", {}).get("generation", 0),
        ),
        errors,
        "analysis groups",
        (0, 0, 0),
    )
    return {
        "analysis_groups": int(row[0] or 0),
        "shared_groups": int(row[1] or 0),
        "largest_group": int(row[2] or 0),
    }


def _profile_state(db, source, errors):
    row = _fetchone(
        db,
        f"""
        SELECT count(*) AS catalogue_tracks,
               count(p.track_id) AS stored,
               count(*) FILTER (WHERE p.status='ready') AS ready,
               count(*) FILTER (
                 WHERE p.status IN ('pending', 'pending_interactive')
               ) AS pending,
               count(*) FILTER (WHERE p.status='failed') AS failed,
               count(*) FILTER (WHERE p.status='skipped_no_file') AS skipped,
               count(*) FILTER (
                 WHERE p.track_id IS NULL OR p.status IN ('stale', 'missing')
               ) AS needs_attention
          FROM {table("catalog_tracks")} t
          LEFT JOIN {table("source_profiles")} p
            ON p.catalog_instance_id=t.catalog_instance_id
           AND p.track_id=t.track_id
         WHERE t.catalog_instance_id=%s AND t.published_generation=%s
           AND t.available=TRUE
        """,
        (
            source["catalog_instance_id"],
            source.get("catalog", {}).get("generation", 0),
        ),
        errors,
        "waveform profiles",
        (0,) * 7,
    )
    keys = (
        "catalogue_tracks",
        "stored",
        "ready",
        "pending",
        "failed",
        "skipped",
        "needs_attention",
    )
    return {key: int(value or 0) for key, value in zip(keys, row)}


def _workflow_state(db, source, errors):
    catalog_instance_id = source["catalog_instance_id"]
    preparation = _fetchone(
        db,
        f"""
        SELECT status, phase, queued_profiles, profile_jobs, last_error,
               started_at, completed_at, updated_at
          FROM {table("preparation_state")}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
        errors,
        "preparation workflow",
        None,
    )
    backfill = _fetchone(
        db,
        f"""
        SELECT status, processed_profiles, queued_profiles, last_error,
               started_at, completed_at, updated_at
          FROM {table("profile_backfill_state")}
         WHERE catalog_instance_id=%s
        """,
        (catalog_instance_id,),
        errors,
        "profile backfill workflow",
        None,
    )
    runs = _fetchall(
        db,
        f"""
        SELECT status, count(*), max(updated_at)
          FROM {table("analysis_runs")}
         WHERE catalog_instance_id=%s
         GROUP BY status
         ORDER BY status
        """,
        (catalog_instance_id,),
        errors,
        "analysis run workflow",
    )
    return {
        "preparation": (
            {
                "status": preparation[0],
                "phase": preparation[1],
                "queued_profiles": int(preparation[2] or 0),
                "profile_jobs": int(preparation[3] or 0),
                "last_error": preparation[4],
                "started_at": preparation[5],
                "completed_at": preparation[6],
                "updated_at": preparation[7],
            }
            if preparation
            else None
        ),
        "backfill": (
            {
                "status": backfill[0],
                "processed_profiles": int(backfill[1] or 0),
                "queued_profiles": int(backfill[2] or 0),
                "last_error": backfill[3],
                "started_at": backfill[4],
                "completed_at": backfill[5],
                "updated_at": backfill[6],
            }
            if backfill
            else None
        ),
        "analysis_runs": [
            {
                "status": str(row[0]),
                "count": int(row[1] or 0),
                "updated_at": row[2],
            }
            for row in runs
        ],
    }


def _journal_state(db, source, errors):
    catalog = source.get("catalog") or {}
    analysis = source.get("analysis") or {}
    catalog_rows = _fetchone(
        db,
        f"""
        SELECT count(*)
          FROM {table("catalog_changes")}
         WHERE catalog_instance_id=%s AND epoch=%s
        """,
        (source["catalog_instance_id"], catalog.get("epoch", "")),
        errors,
        "catalogue journal",
        (0,),
    )
    analysis_rows = _fetchone(
        db,
        f"""
        SELECT count(*)
          FROM {table("analysis_changes")}
         WHERE catalog_instance_id=%s AND epoch=%s
        """,
        (source["catalog_instance_id"], analysis.get("epoch", "")),
        errors,
        "analysis journal",
        (0,),
    )
    leases = _fetchone(
        db,
        f"""
        SELECT count(*) FILTER (
                 WHERE completed_at IS NULL AND expires_at > now()
               ) AS active,
               count(*) FILTER (WHERE completed_at IS NOT NULL) AS completed
          FROM {table("stream_bootstrap_sessions")}
         WHERE catalog_instance_id=%s
        """,
        (source["catalog_instance_id"],),
        errors,
        "bootstrap leases",
        (0, 0),
    )
    return {
        "catalog": {
            "epoch": catalog.get("epoch"),
            "head": int(catalog.get("head_seq") or 0),
            "floor": int(catalog.get("floor_seq") or 0),
            "rows": int(catalog_rows[0] or 0),
        },
        "analysis": {
            "epoch": analysis.get("epoch"),
            "head": int(analysis.get("head_seq") or 0),
            "floor": int(analysis.get("floor_seq") or 0),
            "rows": int(analysis_rows[0] or 0),
        },
        "bootstrap_leases": {
            "active": int(leases[0] or 0),
            "completed": int(leases[1] or 0),
        },
    }


def _core_state(db, compatibility, source, errors):
    if compatibility.adapter == "v3_registry":
        row = _fetchone(
            db,
            """
            SELECT count(*) AS mapping_rows,
                   count(DISTINCT m.item_id) AS canonical_analysis_ids,
                   count(DISTINCT m.item_id) FILTER (
                     WHERE s.item_id IS NOT NULL
                   ) AS scored,
                   count(DISTINCT m.item_id) FILTER (
                     WHERE e.item_id IS NOT NULL
                   ) AS musicnn,
                   count(DISTINCT m.item_id) FILTER (
                     WHERE c.item_id IS NOT NULL
                   ) AS clap,
                   count(DISTINCT m.provider_track_id) FILTER (
                     WHERE cp.fingerprint IS NOT NULL
                   ) AS chromaprint
              FROM track_server_map m
              LEFT JOIN score s ON s.item_id=m.item_id
              LEFT JOIN embedding e ON e.item_id=m.item_id
              LEFT JOIN clap_embedding c ON c.item_id=m.item_id
              LEFT JOIN chromaprint cp
                ON cp.server_id=m.server_id
               AND cp.provider_track_id=m.provider_track_id
             WHERE m.server_id=%s
            """,
            (source.get("server_id"),),
            errors,
            "AudioMuse core",
            (0,) * 6,
        )
        return {
            "mode": "source_scoped",
            "mapping_rows": int(row[0] or 0),
            "canonical_analysis_ids": int(row[1] or 0),
            "scored": int(row[2] or 0),
            "musicnn_vectors": int(row[3] or 0),
            "clap_vectors": int(row[4] or 0),
            "chromaprint": int(row[5] or 0),
        }

    if compatibility.adapter != "v2_single_server":
        return {
            "mode": "unavailable",
            "mapping_rows": 0,
            "canonical_analysis_ids": 0,
            "scored": 0,
            "musicnn_vectors": 0,
            "clap_vectors": 0,
            "chromaprint": None,
        }

    row = _fetchone(
        db,
        """
        SELECT (SELECT count(*) FROM score),
               (SELECT count(*) FROM embedding),
               (SELECT count(*) FROM clap_embedding)
        """,
        (),
        errors,
        "AudioMuse core",
        (0, 0, 0),
    )
    return {
        "mode": "single_server",
        "mapping_rows": int(row[0] or 0),
        "canonical_analysis_ids": int(row[0] or 0),
        "scored": int(row[0] or 0),
        "musicnn_vectors": int(row[1] or 0),
        "clap_vectors": int(row[2] or 0),
        "chromaprint": None,
    }


def collect_database_state(db, compatibility, sources, readiness_by_source=None):
    """Collect a resilient logical snapshot using aggregate, read-only queries."""
    readiness_by_source = readiness_by_source or {}
    snapshot = {
        "captured_at": _iso_now(),
        "status": "ready",
        "core": compatibility.as_dict(),
        "sources": [],
        "errors": [],
    }
    if db is None:
        snapshot["status"] = "database_unavailable"
        snapshot["errors"].append(
            {
                "section": "database",
                "message": "AudioMuse did not provide a database connection.",
            }
        )
        return snapshot
    if not compatibility.supported:
        snapshot["status"] = "core_unsupported"

    for source in sources:
        source_errors = []
        links = _link_state(db, source, source_errors)
        items = _analysis_item_state(db, source, source_errors)
        groups = _group_state(db, source, source_errors)
        profiles = _profile_state(db, source, source_errors)
        workflow = _workflow_state(db, source, source_errors)
        journals = _journal_state(db, source, source_errors)
        core = _core_state(db, compatibility, source, source_errors)
        readiness = readiness_by_source.get(source["catalog_instance_id"]) or {}
        snapshot["sources"].append(
            {
                "identity": {
                    "catalog_instance_id": source["catalog_instance_id"],
                    "server_id": source.get("server_id"),
                    "name": source.get("name") or "Music server",
                    "provider_type": source.get("provider_type") or "unknown",
                    "is_default": bool(source.get("is_default")),
                    "rebind_status": source.get("rebind_status") or "unknown",
                },
                "catalog": source.get("catalog") or {},
                "analysis": source.get("analysis") or {},
                "links": links,
                "items": {**items, **groups},
                "profiles": profiles,
                "workflow": workflow,
                "journals": journals,
                "core": core,
                "readiness": readiness,
                "errors": source_errors,
            }
        )
        snapshot["errors"].extend(source_errors)

    if not sources and snapshot["status"] == "ready":
        snapshot["status"] = "not_initialized"
    elif snapshot["errors"] and snapshot["status"] == "ready":
        snapshot["status"] = "partial"
    return snapshot


def _number(value):
    return f"{int(value or 0):,}"


def _percent(numerator, denominator):
    if not denominator:
        return 0.0
    return max(0.0, min(100.0, float(numerator or 0) * 100.0 / float(denominator)))


def _timestamp(value):
    if value is None:
        return "never"
    if hasattr(value, "isoformat"):
        value = value.isoformat().replace("+00:00", "Z")
    return escape(str(value))


def _metric(label, value, tone=""):
    tone_class = f" db-metric-{tone}" if tone else ""
    return (
        f'<div class="db-metric{tone_class}">'
        f"<span>{escape(str(label))}</span><strong>{escape(str(value))}</strong></div>"
    )


def _meter(label, numerator, denominator):
    percent = _percent(numerator, denominator)
    return f"""
      <div class="db-progress">
        <div><span>{escape(label)}</span><strong>{_number(numerator)} / {_number(denominator)}
          ({percent:.1f}%)</strong></div>
        <div class="db-meter" role="progressbar" aria-label="{escape(label)}"
          aria-valuemin="0" aria-valuemax="100" aria-valuenow="{percent:.1f}">
          <span style="width:{percent:.2f}%"></span>
        </div>
      </div>
    """


def _error_list(errors):
    if not errors:
        return '<p class="db-muted">No recorded errors in this snapshot.</p>'
    return "<ul class=\"db-errors\">" + "".join(
        f"<li><strong>{escape(str(row.get('section') or 'Unknown'))}:</strong> "
        f"{escape(str(row.get('message') or 'Unknown error'))}</li>"
        for row in errors
    ) + "</ul>"


def _coverage_list(coverage):
    if not coverage:
        return '<p class="db-muted">No field-coverage report has been published yet.</p>'
    rows = []
    for field, value in sorted(coverage.items()):
        ratio = value.get("ratio") if isinstance(value, dict) else value
        try:
            label = f"{float(ratio) * 100:.1f}%"
        except (TypeError, ValueError):
            label = "unknown"
        rows.append(
            f"<li><span>{escape(str(field).replace('_', ' '))}</span>"
            f"<strong>{escape(label)}</strong></li>"
        )
    return f'<ul class="db-coverage-list">{"".join(rows)}</ul>'


def _workflow_line(label, state):
    if not state:
        return f"<li><span>{escape(label)}</span><strong>not started</strong></li>"
    phase = f" · {state.get('phase')}" if state.get("phase") else ""
    updated = _timestamp(state.get("updated_at"))
    return (
        f"<li><span>{escape(label)}</span>"
        f"<strong>{escape(str(state.get('status') or 'unknown'))}"
        f"{escape(phase)} · {updated}</strong></li>"
    )


def _source_html(source):
    identity = source["identity"]
    catalog = source["catalog"]
    analysis = source["analysis"]
    links = source["links"]
    items = source["items"]
    profiles = source["profiles"]
    core = source["core"]
    journals = source["journals"]
    workflow = source["workflow"]
    readiness = source["readiness"]
    entity_counts = catalog.get("entity_counts") or {}
    tracks = int(
        entity_counts.get("track")
        or entity_counts.get("tracks")
        or profiles.get("catalogue_tracks")
        or 0
    )
    albums = entity_counts.get("album") or entity_counts.get("albums") or 0
    artists = entity_counts.get("artist") or entity_counts.get("artists") or 0
    libraries = entity_counts.get("library") or entity_counts.get("libraries") or 0
    readiness_status = readiness.get("status") or (
        "not applicable" if core["mode"] == "single_server" else "unavailable"
    )
    readiness_blockers = [
        str(code).replace("_", " ") for code in readiness.get("blockers") or []
    ]
    readiness_detail = (
        " Current verification conditions: "
        + escape(", ".join(readiness_blockers))
        + "."
        if readiness_blockers
        else ""
    )
    chromaprint = core.get("chromaprint")
    core_mapped = core.get("mapping_rows") or 0
    analysis_runs = workflow.get("analysis_runs") or []
    run_text = ", ".join(
        f"{row['status']}: {_number(row['count'])}" for row in analysis_runs
    ) or "none recorded"
    errors = list(source.get("errors") or [])
    for label, state in (
        ("catalogue", catalog),
        ("analysis projection", analysis),
        ("preparation", workflow.get("preparation")),
        ("profile backfill", workflow.get("backfill")),
    ):
        if state and state.get("last_error"):
            errors.append({"section": label, "message": state["last_error"]})

    chromaprint_metric = (
        _metric("Chromaprint fingerprints", _number(chromaprint), "pending")
        if chromaprint is not None
        else _metric(
            "Chromaprint",
            "not used by v2" if core["mode"] == "single_server" else "unavailable",
        )
    )
    chromaprint_meter = (
        _meter("Chromaprint coverage", chromaprint, core_mapped)
        if chromaprint is not None
        else ""
    )

    return f"""
      <article class="db-source">
        <header class="db-source-header">
          <div>
            <span class="db-kicker">{escape(str(identity['provider_type']))} source</span>
            <h2>{escape(str(identity['name']))}</h2>
          </div>
          <span class="db-state">{escape(str(identity['rebind_status']))}</span>
        </header>
        <dl class="db-identity">
          <div><dt>Catalogue instance</dt><dd>{escape(str(identity['catalog_instance_id']))}</dd></div>
          <div><dt>AudioMuse server</dt><dd>{escape(str(identity.get('server_id') or 'unbound'))}</dd></div>
          <div><dt>Default source</dt><dd>{'yes' if identity.get('is_default') else 'no'}</dd></div>
        </dl>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Published provider truth</span><h3>Catalogue</h3></div>
            <span class="db-state">{escape(str(catalog.get('status') or 'unknown'))}</span>
          </div>
          <div class="db-metrics">
            {_metric("Tracks", _number(tracks), "ready")}
            {_metric("Albums", _number(albums))}
            {_metric("Artists", _number(artists))}
            {_metric("Libraries", _number(libraries))}
            {_metric("Generation", _number(catalog.get("generation")))}
          </div>
          <p class="db-muted">Published {_timestamp(catalog.get('completed_at'))}.
            Catalogue journal head {_number(catalog.get('head_seq'))}; floor
            {_number(catalog.get('floor_seq'))}.</p>
          <details>
            <summary>Metadata field coverage</summary>
            {_coverage_list(catalog.get("field_coverage") or {})}
          </details>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">What Lumae can sync now</span><h3>Sonic attribution</h3></div>
            <span class="db-state">{escape(str(analysis.get('status') or 'unknown'))}</span>
          </div>
          {_meter("Usable sonic coverage", links["usable"], tracks)}
          <div class="db-metrics">
            {_metric("Usable links", _number(links["usable"]), "ready")}
            {_metric("Verified", _number(links["verified"]), "ready")}
            {_metric("Provisional", _number(links["provisional"]), "pending")}
            {_metric("Pending", _number(links["pending"]), "pending")}
            {_metric("Usable but flagged", _number(links["suspect"]), "danger")}
            {_metric("Missing", _number(links["missing"]))}
          </div>
          <div class="db-metrics">
            {_metric("Analysis items", _number(items["items"]))}
            {_metric("MusiCNN vectors", _number(items["musicnn_vectors"]))}
            {_metric("CLAP vectors", _number(items["clap_vectors"]))}
            {_metric("Shared groups", _number(items["shared_groups"]))}
            {_metric("Largest group", _number(items["largest_group"]))}
            {_metric("Projection generation", _number(analysis.get("generation")))}
          </div>
          <p class="db-muted">Readiness: {escape(str(readiness_status))}. Published
            {_timestamp(analysis.get('completed_at'))}. Provisional and repair-flagged
            links stay usable with their assigned sonic data; the flags preserve
            attribution uncertainty until AudioMuse repairs and republishes the group.
            {readiness_detail}</p>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Upstream analysis database</span><h3>AudioMuse core</h3></div>
            <span class="db-state">{escape(str(core['mode']).replace('_', ' '))}</span>
          </div>
          <div class="db-metrics">
            {_metric("Provider mappings", _number(core_mapped))}
            {_metric("Canonical analysis IDs", _number(core["canonical_analysis_ids"]))}
            {_metric("Scores", _number(core["scored"]))}
            {_metric("MusiCNN embeddings", _number(core["musicnn_vectors"]))}
            {_metric("CLAP embeddings", _number(core["clap_vectors"]))}
            {chromaprint_metric}
          </div>
          {chromaprint_meter}
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Optional playback enrichment</span><h3>Loudness &amp; SmoothFade</h3></div>
          </div>
          {_meter("Ready waveform profiles", profiles["ready"], profiles["catalogue_tracks"])}
          <div class="db-metrics">
            {_metric("Stored", _number(profiles["stored"]))}
            {_metric("Ready", _number(profiles["ready"]), "ready")}
            {_metric("Queued", _number(profiles["pending"]), "pending")}
            {_metric("Failed", _number(profiles["failed"]), "danger")}
            {_metric("No source audio", _number(profiles["skipped"]))}
            {_metric("Missing / stale", _number(profiles["needs_attention"]))}
          </div>
          <ul class="db-workflows">
            {_workflow_line("Prepare Lumae", workflow.get("preparation"))}
            {_workflow_line("Profile backfill", workflow.get("backfill"))}
            <li><span>Analysis runs</span><strong>{escape(run_text)}</strong></li>
          </ul>
          <p class="db-muted">Waveform profiles improve normalization and transitions. Missing
            profiles never remove tracks from the catalogue or make them unplayable.</p>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Incremental sync retention</span><h3>Journals &amp; leases</h3></div>
          </div>
          <div class="db-metrics">
            {_metric("Catalogue journal rows", _number(journals["catalog"]["rows"]))}
            {_metric("Catalogue head / floor", f'{_number(journals["catalog"]["head"])} / {_number(journals["catalog"]["floor"])}')}
            {_metric("Analysis journal rows", _number(journals["analysis"]["rows"]))}
            {_metric("Analysis head / floor", f'{_number(journals["analysis"]["head"])} / {_number(journals["analysis"]["floor"])}')}
            {_metric("Active bootstrap leases", _number(journals["bootstrap_leases"]["active"]))}
            {_metric("Completed bootstraps", _number(journals["bootstrap_leases"]["completed"]))}
          </div>
        </section>

        <section class="db-section">
          <div class="db-section-heading">
            <div><span class="db-kicker">Actionable diagnostics</span><h3>Errors</h3></div>
          </div>
          {_error_list(errors)}
        </section>
      </article>
    """


def render_database_state(snapshot):
    """Render the database snapshot as a standalone responsive admin screen."""
    sources = snapshot.get("sources") or []
    total_tracks = sum(
        int(
            (row.get("catalog", {}).get("entity_counts") or {}).get("track")
            or row.get("profiles", {}).get("catalogue_tracks")
            or 0
        )
        for row in sources
    )
    total_usable = sum(int(row.get("links", {}).get("usable") or 0) for row in sources)
    total_profiles = sum(int(row.get("profiles", {}).get("ready") or 0) for row in sources)
    compatibility = snapshot.get("core") or {}
    source_html = "".join(_source_html(source) for source in sources)
    if not source_html:
        source_html = """
          <section class="db-empty">
            <h2>No published Lumae catalogue yet</h2>
            <p>Return to settings and run Prepare Lumae. This page will populate as soon as
              the provider-authoritative catalogue has been created.</p>
          </section>
        """
    snapshot_errors = snapshot.get("errors") or []
    recorded_state_errors = sum(
        1
        for source in sources
        for state in (
            source.get("catalog"),
            source.get("analysis"),
            source.get("workflow", {}).get("preparation"),
            source.get("workflow", {}).get("backfill"),
        )
        if state and state.get("last_error")
    )
    total_errors = len(snapshot_errors) + recorded_state_errors
    partial_notice = (
        """
        <section class="db-alert" role="alert">
          <strong>Some diagnostic queries were unavailable.</strong>
          <span>The rest of the snapshot is still valid. See each source's Errors section.</span>
        </section>
        """
        if snapshot_errors
        else ""
    )
    return f"""
      <style>
        .lumae-db {{
          --db-ink:#17202a; --db-muted:#5f6f7f; --db-line:#d9e2ea;
          --db-soft:#f6f8fb; --db-ready:#247a5a; --db-warn:#b46b00;
          --db-danger:#b42318; --db-accent:#2f6fed;
          color:var(--db-ink); display:grid; gap:20px; max-width:1120px;
        }}
        .db-topbar {{align-items:center; display:flex; flex-wrap:wrap; gap:10px;
          justify-content:space-between}}
        .db-button {{background:#fff; border:1px solid var(--db-line); border-radius:8px;
          color:var(--db-ink); display:inline-flex; font-weight:700; min-height:40px;
          padding:9px 14px; text-decoration:none}}
        .db-actions {{display:flex; gap:8px}}
        .db-hero {{border-bottom:1px solid var(--db-line); display:grid; gap:10px;
          padding-bottom:18px}}
        .db-hero h1,.db-source h2,.db-section h3,.db-empty h2 {{margin:0}}
        .db-hero p,.db-muted,.db-empty p {{color:var(--db-muted); line-height:1.55; margin:0}}
        .db-kicker {{color:var(--db-muted); font-size:.75rem; font-weight:800;
          text-transform:uppercase}}
        .db-summary,.db-metrics {{display:grid; gap:10px;
          grid-template-columns:repeat(auto-fit,minmax(135px,1fr))}}
        .db-metric {{background:#fff; border:1px solid var(--db-line); border-radius:8px;
          display:grid; gap:7px; min-width:0; padding:13px}}
        .db-metric span {{color:var(--db-muted); font-size:.78rem; font-weight:700}}
        .db-metric strong {{font-size:1.35rem; line-height:1.05; overflow-wrap:anywhere}}
        .db-metric-ready strong {{color:var(--db-ready)}}
        .db-metric-pending strong {{color:var(--db-warn)}}
        .db-metric-danger strong {{color:var(--db-danger)}}
        .db-alert {{background:#fff8eb; border:1px solid #f2c879; border-radius:8px;
          display:grid; gap:4px; padding:13px}}
        .db-source {{border:1px solid var(--db-line); border-radius:10px; display:grid;
          gap:0; overflow:hidden}}
        .db-source-header,.db-section-heading {{align-items:center; display:flex; gap:12px;
          justify-content:space-between}}
        .db-source-header {{background:var(--db-soft); padding:18px}}
        .db-state {{background:#fff; border:1px solid var(--db-line); border-radius:999px;
          font-size:.76rem; font-weight:800; padding:5px 9px; white-space:nowrap}}
        .db-identity {{background:var(--db-soft); display:grid; gap:8px;
          grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); margin:0; padding:0 18px 18px}}
        .db-identity div {{min-width:0}} .db-identity dt {{color:var(--db-muted);
          font-size:.73rem; font-weight:700}} .db-identity dd {{font-family:monospace;
          font-size:.8rem; margin:3px 0 0; overflow-wrap:anywhere}}
        .db-section {{border-top:1px solid var(--db-line); display:grid; gap:14px; padding:18px}}
        .db-progress {{display:grid; gap:7px}} .db-progress>div:first-child {{
          display:flex; flex-wrap:wrap; gap:8px; justify-content:space-between}}
        .db-meter {{background:#dce5ed; border-radius:999px; height:10px; overflow:hidden}}
        .db-meter span {{background:linear-gradient(90deg,var(--db-ready),var(--db-accent));
          display:block; height:100%}}
        details {{border:1px solid var(--db-line); border-radius:8px; padding:10px 12px}}
        summary {{cursor:pointer; font-weight:700}}
        .db-coverage-list,.db-workflows,.db-errors {{display:grid; gap:8px; list-style:none;
          margin:10px 0 0; padding:0}}
        .db-coverage-list li,.db-workflows li {{align-items:baseline; display:flex; gap:12px;
          justify-content:space-between}}
        .db-coverage-list span,.db-workflows span {{color:var(--db-muted)}}
        .db-workflows strong {{text-align:right}}
        .db-errors {{list-style:disc; padding-left:20px}}
        .db-empty {{background:var(--db-soft); border:1px solid var(--db-line);
          border-radius:10px; display:grid; gap:8px; padding:20px}}
        @media(max-width:620px) {{
          .db-source-header,.db-section-heading {{align-items:flex-start}}
          .db-metrics {{grid-template-columns:repeat(2,minmax(0,1fr))}}
          .db-coverage-list li,.db-workflows li {{align-items:flex-start; flex-direction:column;
            gap:2px}} .db-workflows strong {{text-align:left}}
        }}
      </style>
      <main class="lumae-db" aria-label="Lumae database state">
        <nav class="db-topbar">
          <a class="db-button" href="settings">← Lumae Analysis settings</a>
          <div class="db-actions"><a class="db-button" href="database-state">Refresh snapshot</a></div>
        </nav>
        <header class="db-hero">
          <span class="db-kicker">Read-only diagnostics</span>
          <h1>Lumae database state</h1>
          <p>This is the currently published, source-scoped view used by Lumae. Counts use
            aggregate queries only and include no track names, credentials, or media paths.</p>
          <p>Captured {escape(str(snapshot.get('captured_at') or 'unknown'))} ·
            AudioMuse {escape(str(compatibility.get('core_version') or 'unknown'))} ·
            {escape(str(compatibility.get('core_adapter') or 'no adapter'))} ·
            snapshot {escape(str(snapshot.get('status') or 'unknown'))}</p>
        </header>
        <section class="db-summary" aria-label="Database summary">
          {_metric("Sources", _number(len(sources)))}
          {_metric("Published tracks", _number(total_tracks))}
          {_metric("Usable sonic links", _number(total_usable), "ready")}
          {_metric("Ready waveform profiles", _number(total_profiles), "ready")}
          {_metric("Recorded errors", _number(total_errors), "danger" if total_errors else "")}
        </section>
        {partial_notice}
        {source_html}
      </main>
    """
