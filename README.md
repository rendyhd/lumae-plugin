# Lumae AudioMuse-AI Plugin Catalog

This repository publishes the Lumae Analysis plugin for **AudioMuse-AI**.

The catalog is exposed through `manifest.json`. AudioMuse-AI reads that catalog, follows the Lumae `pluginUrl`, downloads the versioned code-only zip from `dist/lumae_analysis/`, and verifies the published checksum.

## Plugin

Lumae Analysis precomputes loudness and MixRamp profiles server-side so Lumae can use volume normalization and SmoothFade without doing that work on the phone.

The plugin provides:

* a health endpoint for app setup checks;
* profile read/request API endpoints for the Lumae app;
* an analysis hook that reuses AudioMuse's per-track analysis audio;
* a source-scoped preparation page that marks the provider catalogue and AudioMuse projection ready before waveform enrichment finishes;
* a read-only database-state dashboard for published catalogue generations, sonic-link evidence, embeddings, Chromaprint, journals, and waveform coverage;
* progressive sonic admission that keeps repair-flagged AudioMuse 3 mappings usable while preserving their uncertainty evidence for later replacement;
* one durable background-enrichment workflow per source, using small batches and a one-action watchdog instead of flooding the queue;
* high-priority, idempotent promotion for the current playback window, so a requested track is not trapped behind a library backfill.
* cursor-based playback-profile delivery, so phones install only newly ready or removed profiles after the first bootstrap;
* server-owned album and artist relationship generations using Lumae's native scoring model, published as resumable snapshots and deltas;
* nonblocking enrichment: a complete provider catalogue is app-ready while waveform and relationship backlogs continue.

### Resource safety in 1.0.1

Waveform analysis now decodes and filters audio incrementally. Its working
memory is bounded by decoder blocks instead of growing with the duration and
native sample rate of the media file. Background batches default to three
tracks and are capped at ten, every heavy queue job has a finite timeout, and
backfill candidates are selected with a SQL `LIMIT`.

Album and artist matching now asks AudioMuse's MusicNN IVF index for a bounded
shortlist and never falls back to an all-pairs comparison. If the index is not
ready, the build waits while the last published relationship generation stays
available. Installing or upgrading the plugin does not force a new projection
when the current catalogue is already valid.

Administrators can pause Lumae background maintenance from the plugin settings
page. Pausing stops new catalogue, projection, waveform, and relationship work;
it does not delete or hide already published catalogue, profile, collection, or
relationship data.

### AudioMuse 3.4 queue compatibility in 1.1.7

Lumae no longer imports RQ queues, jobs, dependencies, or retry objects from
AudioMuse. Song-analysis hooks only record a pending source run in PostgreSQL.
The catalogue watchdog runs every minute and advances at most one settled
analysis finalizer, catalogue preparation, relationship build, or waveform
batch. Each step is claimed atomically, so worker restarts and a manual job
racing the watchdog are safe.

The ONNX Runtime message `No registered plugin EP device found for
'CUDAExecutionProvider'` is not emitted by Lumae. When AudioMuse continues with
album progress immediately afterward, it is a non-fatal execution-provider
discovery warning; CUDA availability and CPU fallback belong to the AudioMuse
container and ONNX Runtime configuration.

### Develop-build transition guard in 1.1.2

Navidrome prerelease, snapshot, branch, and unknown builds are always treated
as uncertain identity builds. This catches develop builds that contain the
canonical-ID migration while still reporting the last safe numeric release,
and forces two exact provider scans before Lumae publishes any ID changes.

### Catalogue refresh fix in 1.1.1

Provider-identity inspection now locks only mandatory catalogue and transition
rows. PostgreSQL no longer receives a row-lock request for the nullable side of
the optional analysis-state join, so refresh retries can publish normally while
the previous complete generation remains available.

## Layout

* `manifest.json` - the AudioMuse plugin catalog.
* `plugins/LumaeAnalysis/plugin.json` - the plugin metadata and release list.
* `plugins/LumaeAnalysis/*.py` - the plugin code.
* `dist/lumae_analysis/` - published Lumae release zip files.
* `tests/plugins/test_lumae_analysis.py` - local regression tests.

## Development

The latest AudioMuse plugin documentation is here:

https://github.com/NeptuneHub/AudioMuse-AI/blob/main/docs/PLUGIN.md

The release zip must contain code only: `__init__.py` and helper files, with no `plugin.json` inside the zip. The GitHub workflow rebuilds the zip, fills the release `sourceUrl` and `checksum`, and regenerates `manifest.json`.

Run the local regression suite with:

```sh
python -m pip install -r requirements-dev.txt
python -m pytest tests/plugins -q
```

Pull requests run the same tests. The release builder rejects source changes
that would alter an already checksummed version; add a new version entry instead.

## License

This repository is licensed under the AGPLv3 license. See `LICENSE`.
