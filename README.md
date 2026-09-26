# Lumae AudioMuse-AI Plugin Catalog

This repository publishes Lumae plugins for **AudioMuse-AI**.

The catalog is exposed through `manifest.json`. AudioMuse-AI reads that catalog, follows the Lumae `pluginUrl`, downloads the versioned code-only zip from `dist/lumae_analysis/`, and verifies the published checksum.

## Plugins

### Lumae Analysis

Lumae Analysis mirrors a Navidrome catalogue through AudioMuse's own
projection and precomputes loudness/MixRamp and SmoothFade edge profiles
server-side, so the Lumae app can use volume normalization and SmoothFade,
and browse/manage collections, without doing that work — or holding a
decode of the audio — on the phone.

The currently released version is **1.2.5**. **1.3.0 is unreleased**, waiting
on user approval (`docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md`, P4-1); this
README describes the plugin as it behaves once 1.3.0 merges, noting the few
places 1.2.5 still differs. See [CHANGELOG.md](CHANGELOG.md) for the full
release history and
[docs/runbooks/UPGRADE_1.3.md](docs/runbooks/UPGRADE_1.3.md) before upgrading
a running 1.2.5 installation — no 1.2.5 process may keep running once the
1.3.0 migration has applied.

### Capabilities

* **Catalogue.** A source-scoped mirror of the Navidrome catalogue that
  AudioMuse already analyzes: album/artist metadata, credits, soft
  deletions, and cursor-based incremental refresh
  (`docs/contracts/LUMAE_SYNC_CONTRACT.md` §2, `catalog_mirror`). A
  source-scoped preparation page marks the provider catalogue and AudioMuse
  projection ready before waveform enrichment finishes, and a read-only
  database-state dashboard shows published catalogue generations, sonic-link
  evidence, embeddings, Chromaprint, journals and waveform coverage.
* **Waveform and edge profiles.** Per-track loudness/MixRamp profiles reuse
  AudioMuse's own analysis audio; edge (SmoothFade) profiles are a separate,
  source-bound analysis. One durable background-enrichment workflow per
  source uses small batches and a one-action watchdog instead of flooding
  the queue, with high-priority idempotent promotion for the current
  playback window so a requested track is never trapped behind a library
  backfill. Both profile kinds are delivered as a cursor stream, so a phone
  installs only newly ready or removed profiles after its first bootstrap,
  and server-owned album/artist relationship generations use Lumae's native
  scoring model, published as resumable snapshots and deltas. A complete
  provider catalogue is app-ready while waveform and relationship backlogs
  continue in the background.
* **Offline bulk copy.** The legacy keyset bootstrap
  (`GET /api/profiles/bootstrap`) and the v2 create/page session
  (`docs/contracts/LUMAE_SYNC_CONTRACT.md` §3.5) let a phone fetch a full
  library once instead of paging every track individually. From 1.3.0 the v2
  session has a sliding expiry (K3) and an idempotent create (K5), health
  reports a truthful `available` probe and a new `auth_enabled` flag (K4),
  and both the v2 pages and the incremental profile-change stream can carry
  an edge *reference* instead of a full copy when the edge did not change
  (K6, P3-2; §3.7) — the change that makes the LUM-005 loudness-analyzer
  upgrade (K11) affordable to roll out.

  **`AUTH_ENABLED=false`:** every transfer, including the v2 bootstrap, is
  then anonymous — open to anyone who can reach the host. Health still
  reports `capabilities.profile_bootstrap.auth: "host_authenticated"`; that
  string describes the design, not the live setting. From 1.3.0, read
  `capabilities.profile_bootstrap.auth_enabled` instead (K4). Before running
  Lumae Analysis with `AUTH_ENABLED=false` — including on a LAN-only server —
  read the full authentication table in
  `docs/contracts/LUMAE_SYNC_CONTRACT.md` §1.2: anyone who can reach the host
  can read and write every profile, collection and shelf on it.
* **Collections.** Living Collections (mixed album/track membership,
  per-user or shared storage, checksummed backup/restore, incremental sync)
  and Personal Shelves share one collection manager
  (`docs/contracts/LUMAE_SYNC_CONTRACT.md` §5; see also
  [docs/discovery-api-v1.md](docs/discovery-api-v1.md) for shelves). From
  1.3.0: the feed adds `epoch`/`head_seq`/`has_more` and a one-shot,
  transactionally consistent snapshot route (K8, §5.2a); an opt-in contract
  header replaces a silent membership-id remap with an explicit 409
  conflict response (K9, §5.3); and collection items carry an explicit
  `catalog_instance_id`, so the same track can be a distinct item per
  catalogue (K10, LUM-013, §5.1).
* **Diagnostics and readiness.** The settings page shows a per-source
  readiness panel — catalogue, analysis projection, waveform profiles, edge
  profiles and relationships — each with an availability word, a last
  success time and age, and a job status, with a scoped retry action per
  stream (LUM-017). From 1.3.0, health adds an `integrity` block
  (unpublished/orphaned profile counts, collections-feed health, fence
  installation) and stored diagnostic error text is shown through
  redaction, never raw (LUM-021, §2).
* **Analysis time limits.** AudioMuse runs plugin tasks without enforcing
  any timeout of its own. From 1.3.0, each waveform or edge analysis instead
  runs in its own process with a killable wall-clock limit, so a stuck
  decoder cannot hang a worker (LUM-018; see Settings below).

### Execution limits for file analysis in 1.3.0

AudioMuse runs plugin tasks without a time limit. RQ-era hosts enqueued them
with an RQ `job_timeout` of -1 (no timeout), and the database task queue that
replaced RQ in August 2026 has no per-task timeout. The `timeout` the plugin
passes when it queues work is not enforced by anyone.

Each waveform and edge analysis therefore runs in a separate process with a
wall-clock limit. The limit is also the analyzers' own deadline, checked
between decoded blocks: an analysis that is still making progress stops there.
A decoder stuck inside native code never reaches that check, so the process is
killed 30 seconds after the limit (SIGTERM, then SIGKILL 5 seconds later).
Cancelling the AudioMuse job also stops it. Only the file path goes to the
analysis process, so audio is never held in two processes. The process is
started in one of two ways:

* **A fork of the job (AudioMuse jobs).** AudioMuse runs every job in a freshly
  forked process with one thread, so each file is analyzed in a fork of that
  process, which already has the analyzers loaded. With 4-minute FLAC tracks on
  4 CPUs, running each job in a fresh fork as AudioMuse does, a 1-track edge job
  and a 3-track waveform job took as long as without isolation, within noise
  (−29 to +50 ms per job, at most 2%). A quiet micro-benchmark puts the cost at
  about 12 ms per file. The fork shares the job's open descriptors, including
  its database connection. It never uses or closes them and exits without
  running exit handlers, so the job's connection is unaffected.
* **A separate interpreter (processes with more threads).** Forking a process
  with more than one Python thread is unsafe, so there a worker is started with
  `python -c`. Starting it takes 1.1–1.4 seconds, mostly `import scipy.signal`.
  It is then reused for later files from the same thread. In the same job model
  it adds 1.1–1.5 seconds per job (+41–46%). A killed or crashed worker is
  replaced, and a worker killed while idle is replaced without counting a
  failure. Its environment omits credential-like variables.

Neither path is a security boundary. The analysis process runs as the same
user as AudioMuse: a compromised decoder could read AudioMuse's memory or its
`/proc/<pid>/environ`. The environment filtering is hygiene only.

Frozen native builds and non-POSIX platforms analyze in-process: the limit still
applies as the analyzers' deadline, but nothing is killed and no failure
diagnostics are recorded.

The limit is the plugin setting `analysis_time_limit_seconds`: 900 by default
(the fixed deadline of earlier releases), clamped to 60–86400. A new value
applies to the next analyzed file; no worker is restarted. Set it through
AudioMuse's plugin settings API: `GET /api/plugins/settings/lumae_analysis`, add
the key to `settings`, and `POST` the whole object back. The POST replaces every
stored setting.

Failures get their own retry categories:

* `analysis_timeout`: the hard limit or the analyzers' deadline. Retried after
  a cooldown, at most three attempts in all (the LUM-007 retry budget).
* `analysis_crash`: the analysis process died without an answer (a signal
  such as SIGSEGV, a non-zero exit, or a `MemoryError`). Retried like a timeout.
* `media_error`: the decoder rejected the data (PyAV `InvalidDataError`, for
  example a corrupt FLAC frame; PyAV stops at the first invalid packet).
  Retried only when the media, the analyzer or the schema changes.

`/api/profiles` still reports the 1.2.5 reasons for these (`unsupported_media`
for `media_error`, `analysis_error` for `analysis_crash`). The server keeps
diagnostics for the latest failed analysis: `source_profiles.failure_diagnostics`
(JSON) and, for edge jobs, the `last_error` text. They hold the container,
codec, sample rate, channel layout, byte size, decode position, error class and
exit signal, never a path, URL, tag or exception message.

Earlier per-release fixes to provider-identity transition detection,
catalogue-refresh locking and queue compatibility (1.1.1–1.1.9) are in
[CHANGELOG.md](CHANGELOG.md); none of them changed what is described above.

## Settings

Set these through AudioMuse's plugin settings API: `GET
/api/plugins/settings/lumae_analysis`, add the key to `settings`, and `POST`
the whole object back — the POST replaces every stored setting.

| Setting | Default | Range | Effect |
|---|---|---|---|
| `analysis_time_limit_seconds` | 900 | 60–86400 | Per-file wall-clock kill for waveform/edge analysis (Analysis time limits, above). A new value applies to the next analyzed file; no worker restarts. |
| `diagnostic_statement_timeout_ms` | 5000 | 1000–30000 | `SET LOCAL statement_timeout` for each read behind the settings-page diagnostics and `/database-state`; a section whose read timed out is reported as "unavailable", never as zero. |
| `edge_profiles_enabled` | `true` | — | Turns edge (SmoothFade) analysis off without touching already-published waveform profiles. Also gated by the PyAV/libswresample runtime; see `capabilities.edge_profiles.available` in health. |
| `collection_manager_enabled` | `false` | — | Enables Living Collections and Personal Shelves sync (Collections, above). |

## Compatibility

* **Core:** `>=2.6.0,<4.0.0`; provider type `navidrome` only
  (`catalog_mirror.supported_core_range` / `supported_provider_types`).
* **Old clients against a 1.3.0 server:** every 1.3.0 change is additive or
  opt-in; a client that ignores unknown health and response keys sees 1.2.5
  behaviour byte-for-byte. See "Keys that do not exist in 1.2.5" in
  `docs/contracts/LUMAE_SYNC_CONTRACT.md` §2, and the compatibility rules in
  its §8.
* **`AUTH_ENABLED=false`:** see Offline bulk copy, above, and
  `docs/contracts/LUMAE_SYNC_CONTRACT.md` §1.2.
* **Friend Album Discovery** (`plugins/FederatedAlbums/`) is a separate,
  private-development plugin excluded from the public manifest; see its own
  [README](plugins/FederatedAlbums/README.md).

## Layout

* `manifest.json` - the AudioMuse plugin catalog.
* `plugins/LumaeAnalysis/plugin.json` - the plugin metadata and release list.
* `plugins/LumaeAnalysis/*.py` - the plugin code.
* `plugins/FederatedAlbums/*.py` - the friend-album plugin code (private development).
* `dist/lumae_analysis/` - published Lumae release zip files.
* `tests/plugins/test_lumae_analysis.py` - local regression tests.
* `CHANGELOG.md` - release history.
* `docs/contracts/LUMAE_SYNC_CONTRACT.md` - the authoritative wire contract.
* `docs/runbooks/UPGRADE_1.3.md` - the 1.2.5 → 1.3.0 upgrade procedure.

## Development

The latest AudioMuse plugin documentation is here:

https://github.com/NeptuneHub/AudioMuse-AI/blob/main/docs/PLUGIN.md

The release zip contains code only, with no `plugin.json`. `release-sources.json` explicitly selects a pinned immutable archive, a new source release, or private development for each plugin. The public workflow verifies and reuses the pinned immutable Lumae Analysis archive (currently 1.2.5); subsequent development never rebuilds that archive. A new public release requires both a new metadata version and an explicit source-release policy — 1.3.0 switches to source mode at release (P4-1).

Run the local regression suite with:

```sh
python -m pip install -r requirements-dev.txt
python -m pytest tests/plugins -q
```

CI discovers the complete suite and runs PostgreSQL integration tests. Locally, set
`LUMAE_POSTGRES_TEST_DSN` to a disposable database to enable those tests.
Run `python scripts/build_catalog.py --check` to verify the public release policy.

[`runtime/README.md`](runtime/README.md) is **historical**: it documents the
retired private DJ architecture and the Friend Album Discovery host contract
as they stood before Radio DJ's removal in 1.2.1. It is kept for reference
when reading old archives or reviews, not as current runtime guidance.

## License

This repository is licensed under the AGPLv3 license. See `LICENSE`.

## Links

* [`CHANGELOG.md`](CHANGELOG.md) - full release history, including the
  retired Radio DJ era.
* [`docs/contracts/LUMAE_SYNC_CONTRACT.md`](docs/contracts/LUMAE_SYNC_CONTRACT.md) -
  the authoritative, code-verified wire contract (health/capabilities,
  profile and collection endpoints, planned changes K1–K11).
* [`docs/runbooks/UPGRADE_1.3.md`](docs/runbooks/UPGRADE_1.3.md) - the
  1.2.5 → 1.3.0 upgrade procedure.
* [`docs/STATUS.md`](docs/STATUS.md) - current work-package status.
* [`docs/discovery-api-v1.md`](docs/discovery-api-v1.md) - Personal Shelves
  and personal-discovery sync contract, recovery and limits.
* [`runtime/README.md`](runtime/README.md) - historical; see above.
