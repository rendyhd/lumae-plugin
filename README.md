# Lumae AudioMuse-AI Plugin Catalog

This repository publishes Lumae plugins for **AudioMuse-AI**.

The catalog is exposed through `manifest.json`. AudioMuse-AI reads that catalog, follows the Lumae `pluginUrl`, downloads the versioned code-only zip from `dist/lumae_analysis/`, and verifies the published checksum.

## Plugins

### Lumae Analysis

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

### Discovery support in 1.2.2

Adds catalogue-independent Want Shelf, memory, feedback, Rest and introduction
provenance synchronization, plus background MusicBrainz checks for external
artists, release groups, editions and recordings. Existing shelf v1 and credits
APIs remain unchanged. This is server support; the corresponding mobile data
and recommendation features must also be implemented in the app.

Enable **the collection manager** in Lumae Analysis settings to enable personal
discovery sync. Sign in with the same AudioMuse account on each device. An
installation token uses shared storage for everyone using that token.
MusicBrainz checking requires no account/key and pauses with background
maintenance or pending playback work. Last.fm/AI keys belong in the mobile app.

See [Discovery API and operations](docs/discovery-api-v1.md) for contracts,
recovery, limits, test commands and release qualification.

### Personal Shelves, credits, and recovery in 1.2.0

This release adds Personal Shelves synchronization and insights, MusicBrainz
credits enrichment, resumable album/artist relationship builds, source-bound
edge profiles, and opt-in DJ analysis with interrupted-work recovery and bounded
queue deferral. It includes the Navidrome identity guard from 1.1.9.

DJ analysis remains disabled by default and requires a compatible dedicated
worker; publishing this plugin does not install or enable that worker. Credits
matching remains subject to its configured audit gate. Friend Album Discovery
remains excluded from the public catalogue.

### Resource safety in 1.0.1

Waveform analysis now decodes and filters audio incrementally. Its working
memory is bounded by decoder blocks instead of growing with the duration and
native sample rate of the media file. Background batches default to three
tracks and are capped at ten, every heavy queue job requests a finite timeout
(AudioMuse does not enforce it; see the execution limits for 1.3.0 below), and
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

### Execution limits for file analysis in 1.3.0

AudioMuse runs plugin tasks without a time limit. RQ-era hosts enqueued them
with an RQ `job_timeout` of -1 (no timeout), and the database task queue that
replaced RQ in August 2026 has no per-task timeout. The `timeout` the plugin
passes when it queues work is not enforced by anyone.

Each waveform and edge analysis therefore runs in a separate worker process
with a hard wall-clock limit. The analyzers' own deadline, checked between
decoded blocks, still ends a slow analysis first. A decoder stuck inside native
code is killed 30 seconds after the limit (SIGTERM, then SIGKILL 5 seconds
later), and a new worker replaces it. The worker starts once per AudioMuse job
(about 1 second) and is reused for the job's files. It receives only the file
path, so audio is never held in two processes, and it holds no database
connection or credentials. Cancelling the AudioMuse job also stops the worker.
Frozen native builds and non-POSIX platforms analyze in-process, with the
analyzers' deadline only.

The limit is the plugin setting `analysis_time_limit_seconds`: 900 by default,
clamped to 60–86400. Set it through AudioMuse's plugin settings API: `GET
/api/plugins/settings/lumae_analysis`, add the key to `settings`, and `POST` the
whole object back. The POST replaces every stored setting.

Failures get their own retry categories:

* `analysis_timeout`: the hard limit or the analyzers' deadline. Retried after
  a cooldown, at most three attempts in all (the LUM-007 retry budget).
* `analysis_crash`: the worker died without an answer (a signal such as
  SIGSEGV, a non-zero exit, or a `MemoryError`). Retried like a timeout.
* `media_error`: the decoder rejected the data (PyAV `InvalidDataError`, for
  example a corrupt FLAC frame; PyAV stops at the first invalid packet).
  Retried only when the media, the analyzer or the schema changes.

`/api/profiles` still reports the 1.2.5 reasons for these (`unsupported_media`
for `media_error`, `analysis_error` for `analysis_crash`). The server keeps
diagnostics for the latest failed analysis: `source_profiles.failure_diagnostics`
(JSON) and, for edge jobs, the `last_error` text. They hold the container,
codec, sample rate, channel layout, byte size, decode position, error class and
exit signal, never a path, URL, tag or exception message.

### Provider-identity reconciliation fix in 1.1.9

Trusted pre-canonical Navidrome releases now publish ordinary track removals and
music-library scope changes through the normal catalogue diff. Exact identity
inspection remains fail-closed for pending, uncertain, or blocked canonical-ID
transitions.

### Adaptive reconciliation in 1.1.8

Lumae keeps the public-API-only queue compatibility introduced in 1.1.7, but
the catalogue reconciler no longer runs every minute while idle. It runs every
minute only when durable work is ready, every five minutes while an AudioMuse
analysis parent is still running, uses 1/5/15/60-minute retry backoff, and falls
back to an hourly safety sweep at minute 11 when current or paused.

The plugin settings page reports the real action, phase, duration, retry, and a
bounded journal of meaningful work. AudioMuse may still label its generic task
row `Songs analyzed: 0`; that core-owned label is not used by Lumae status.

Settings status updates in the background without reloading the page. One
request updates the status panels every five seconds during active work and
every thirty seconds while idle; hidden tabs do not poll. Unsaved batch sizes,
expanded details, and the current scroll position are preserved.

### AudioMuse 3.4 queue compatibility in 1.1.7

Lumae no longer imports RQ queues, jobs, dependencies, or retry objects from
AudioMuse. Song-analysis hooks only record a pending source run in PostgreSQL.
The catalogue watchdog advances at most one settled analysis finalizer,
catalogue preparation, relationship build, or waveform batch per active source
invocation. Each step is claimed atomically, so worker restarts and a manual job
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

### Friend Album Discovery (private development)

This source is excluded from the public catalogue until its required scoped
plugin-bearer host extension is available and qualified. Connection operations
are owner-scoped, and friend sync runs as bounded background work.

Friend Album Discovery connects authenticated AudioMuse-AI instances using
revocable, read-only pairing tokens. It recommends three albums a listener
does not own from Lumae's native Sonic Fingerprint and adds friend results to
sonically similar album searches.

The catalogue contains album metadata and versioned Album Dynamics averages,
not audio, filenames, listening history, track IDs, or individual track
embeddings. Artwork is fetched only through a size-limited authenticated proxy.

## Layout

* `manifest.json` - the AudioMuse plugin catalog.
* `plugins/LumaeAnalysis/plugin.json` - the plugin metadata and release list.
* `plugins/LumaeAnalysis/*.py` - the plugin code.
* `plugins/FederatedAlbums/*.py` - the friend-album plugin code.
* `dist/lumae_analysis/` - published Lumae release zip files.
* `tests/plugins/test_lumae_analysis.py` - local regression tests.

## Development

The latest AudioMuse plugin documentation is here:

https://github.com/NeptuneHub/AudioMuse-AI/blob/main/docs/PLUGIN.md

The release zip contains code only, with no `plugin.json`. `release-sources.json` explicitly selects a pinned immutable archive, a new source release, or private development for each plugin. The public workflow verifies and reuses the immutable Lumae Analysis 1.2.0 archive; subsequent development never rebuilds that archive. A new public release requires both a new metadata version and an explicit source-release policy.

Run the local regression suite with:

```sh
python -m pip install -r requirements-dev.txt
python -m pytest tests/plugins -q
```

CI discovers the complete suite and runs PostgreSQL integration tests. Locally, set
`LUMAE_POSTGRES_TEST_DSN` to a disposable database to enable those tests.
Run `python scripts/build_catalog.py --check` to verify the public release policy.

The [private DJ architecture, host contract and qualification guide](runtime/README.md)
documents the dedicated worker requirements, V2/V3 lifecycle, bounded response
pages, model removal, optional retention and reproducible performance tools.

## License

This repository is licensed under the AGPLv3 license. See `LICENSE`.

## Personal Shelves (API version 1)

The existing Collections setting also enables Personal Shelves synchronization. Health advertises `capabilities.shelves` with `enabled`, `schema_version: 1`, and scope metadata. Management stays in the Lumae mobile app.

Under the Lumae API prefix, `GET /shelves/snapshot` and `GET /shelves/changes` accept `catalog_id`, `cursor`, and `limit` (maximum 500 records per transport page). `POST /shelves/mutations?catalog_id=...` accepts an idempotency ID plus an add, remove, restore, order, or evidence operation. Orders carry a base revision; conflicts return HTTP 409 with the synchronized arrangement for mobile review. Evidence ingestion accepts batches of up to 500 facts. These transport sizes do not limit shelf capacity.

Records and mutation receipts are partitioned by authenticated principal and catalogue identity. Signed-in users have personal shelves; bearer-token users share the installation scope, matching Collections. Membership periods retain deletion tombstones, simultaneous duplicate additions converge, and provider identity rekeys update only the matching catalogue. Search evidence contains selected entity IDs rather than raw queries. Rating changes/removals, active-view cooldowns, and occurrence-identified qualified listening synchronize idempotently. Mobile keeps recommendation batches and scroll positions local.

Shelf storage is additive and independent of the provider catalogue cache. An ordinary refresh does not erase curated membership or durable listening. Disabled or older plugins leave mobile shelves usable offline with pending changes retained. The browser shelf manager, publication, and deployment are outside this implementation.

### Discovery qualification update (1.2.3)

Album memories now retain their artist context. Explicit enjoyment is independent of recognition; “new to me” and “enjoyed” can both be true. Health advertises `album_memory_context` and `enjoyment_feedback`; older mobile clients and existing schema-v1 records remain compatible. Release 1.2.2 remains immutable.
