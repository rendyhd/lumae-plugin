"""DJ settings rendering over explicit status data."""

from html import escape


def render_panel(enabled, acknowledged, capability, counts, removal, acknowledgement):
    reason = str(capability.get("reason") or "")
    if not enabled:
        status = "Off"
        status_class = ""
        detail = "No Beat This or YAMNet model will be downloaded or loaded. SmoothFade remains available."
    elif capability.get("worker_available") is True:
        status = "Worker ready"
        status_class = "lumae-source-state-ready"
        detail = "The dedicated worker verified the pinned Beat This and YAMNet models and runtime."
    elif reason in {
        "worker_not_attested",
        "dedicated_worker_required",
        "dj_host_contract_required",
    }:
        status = "Worker disconnected"
        status_class = "lumae-source-state-danger"
        detail = "Connect a dedicated DJ worker that supports this plugin’s queue contract. Pending work will resume after it connects."
    elif reason in {
        "disabled",
        "model_missing",
        "model_size_mismatch",
        "model_checksum_mismatch",
        "yamnet_model_missing",
        "yamnet_model_size_mismatch",
        "yamnet_model_checksum_mismatch",
    }:
        status = "Preparing"
        status_class = "lumae-source-state-working"
        detail = (
            "Model setup is queued on the worker. Its reported phase is "
            + str(capability.get("lifecycle") or "waiting")
            + "."
        )
    else:
        status = "Needs attention"
        status_class = "lumae-source-state-danger"
        detail = {
            "dedicated_worker_required": "A dedicated DJ worker has not connected.",
            "model_download_failed": "The model download failed. Save the enabled switch to retry.",
            "runtime_dependency_mismatch": "The DJ worker does not have the pinned optional runtime.",
            "vocal_calibration_invalid": "Correct the configured vocal calibration artifact before running analysis.",
        }.get(reason, "The DJ worker is not ready yet.")
    calibration = capability.get("vocal_calibration", {})
    calibration_text = (
        "Cuts authorized for " + str(calibration.get("calibration_tier", "release"))
        if calibration.get("cuts_authorized")
        else "Cuts not authorized"
    )
    playback_text = (
        "Qualified"
        if capability.get("reference_host_qualified") and capability.get("available")
        else "Not qualified"
    )
    progress = ", ".join(
        f"{int(counts.get(key,0))} {key}"
        for key in ("pending", "running", "ready", "failed", "unsupported", "cancelled")
    )
    removal_text = (
        (
            "Model removal: "
            + str(removal.get("status"))
            + (
                f" ({removal.get('removed_files',0)} files)"
                if removal.get("status") == "complete"
                else ""
            )
        )
        if removal.get("status")
        else ""
    )
    checked = " checked" if enabled else ""
    return f"""
      <section class="lumae-panel" aria-label="Optional DJ Mode"
        data-lumae-active="{str(enabled and not capability.get('worker_available')).lower()}">
        <span class="lumae-section-priority lumae-section-optional">Optional download</span>
        <header class="lumae-source-header">
          <div>
            <h3>DJ Mode</h3>
            <p class="lumae-action-copy">Adds beat-grid and phrase-boundary analysis for Lumae’s
              best transitions. It is separate from standard SmoothFade.</p>
          </div>
          <span class="lumae-source-state {status_class}">{status}</span>
        </header>
        <p class="lumae-help">{escape(detail)}</p>
        <p class="lumae-help">Analysis jobs: {escape(progress)}<br>Vocal calibration: {escape(calibration_text)}<br>Playback qualification: {escape(playback_text)}<br>{escape(removal_text)}</p>
        <form class="lumae-form" method="post">
          <label class="lumae-toggle">
            <input type="checkbox" role="switch" name="dj_analysis_enabled"{checked}>
            <span>Enable DJ analysis</span>
          </label>
          <label class="lumae-toggle">
            <input type="checkbox" name="dj_models_acknowledged"
              {' checked' if acknowledged else ''}>
            <span>{escape(acknowledgement)}</span>
          </label>
          <p class="lumae-help">Turning this on downloads checksum-pinned Beat This (77.3 MiB)
            and official YAMNet Lite (3.9 MiB) models only on the DJ worker. YAMNet scores are
            uncalibrated AudioSet evidence, not probabilities or permission to cut. Turning this
            off prevents model loading and new DJ analysis. Existing music, sync data, and normal
            analysis remain usable.</p>
          <div class="lumae-actions">
            <button class="lumae-button-secondary" type="submit" name="action"
              value="save_dj_analysis">Save DJ Mode setting</button>
            <button class="lumae-button-secondary" type="submit" name="action"
              value="remove_dj_models">Remove downloaded DJ models</button>
          </div>
        </form>
      </section>
    """
