# Changelog

Notable changes to the `lumae_analysis` plugin. Per-version metadata
(`min_core_version`, checksum, source zip) is authoritative in
[`plugins/LumaeAnalysis/plugin.json`](plugins/LumaeAnalysis/plugin.json); this
file gives the human-readable story, oldest detail first collapsed to what
still matters. Exact, code-verified wire behaviour for every version lives in
[`docs/contracts/LUMAE_SYNC_CONTRACT.md`](docs/contracts/LUMAE_SYNC_CONTRACT.md).

## Unreleased (1.3.0)

**Not yet released.** It merges to `main` only after user approval
(`docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md`, P4-1) and, once released, no
1.2.5 process may keep running against an upgraded database — follow
[`docs/runbooks/UPGRADE_1.3.md`](docs/runbooks/UPGRADE_1.3.md). Work-package-level
progress and test evidence are in [`docs/STATUS.md`](docs/STATUS.md); the
precise wire changes (K1–K11) are in
[`docs/contracts/LUMAE_SYNC_CONTRACT.md`](docs/contracts/LUMAE_SYNC_CONTRACT.md)
§7. Summary:

- **Transport:** gzip for JSON responses of 1 KiB or more (K1).
- **v2 profile bootstrap:** snapshot pages store an edge *reference* instead
  of a copy, resolved at read time (K2); sessions get a sliding expiry
  (K3); health reports a truthful, cached `available` probe and a new
  `auth_enabled` field alongside the unchanged `auth` string (K4); session
  create is idempotent per `client_request_id` (K5).
- **Edge references in the profile stream:** an opt-in (`edge_refs`) lets a
  waveform-only republish skip re-sending an unchanged edge profile (K6,
  P3-2) — the change that makes the LUM-005 loudness-analyzer upgrade (K11)
  affordable to roll out.
- **Collections:** the feed adds `epoch`/`head_seq`/`has_more` and a
  one-shot, consistent snapshot route (K8); a stricter opt-in conflict
  contract replaces silent id remaps with explicit 409 responses (K9);
  collection items carry an explicit `catalog_instance_id` (K10, LUM-013).
- **Diagnostics and readiness:** a health `integrity` block
  (unpublished/orphaned profile counts, fence installation, collections feed
  health); stored error text shown through redaction (LUM-021); per-source
  readiness panels with scoped retries on the settings page (LUM-017).
- **Execution limits:** each waveform/edge analysis runs with a killable
  wall-clock limit, since AudioMuse's own task queue enforces none
  (LUM-018).
- **Retry hardening:** stale catalogue/profile transitions no longer strand
  a retryable profile, and pause or legacy-migration releases no longer
  burn a retry attempt (LUM-007).
- **Correctness:** the `profile_stream_state` publication lock is now
  exercised by a concurrency test that fails if `FOR UPDATE` is removed
  (LUM-001, P3-13); orphan withdrawal and ready-repair gaps closed
  (LUM-008).

## 1.2.5

Set `fully_verified` to `False` when a provider-identity transition blocks
catalogue sync, to preserve the progressive-readiness contract.

## 1.2.4

Verify release type and complete single/EP recording membership, so Lumae's
duplicate-safe recommendations do not merge distinct releases.

## 1.2.3 — Discovery qualification

Album memories now retain their artist context. Explicit enjoyment is
independent of recognition; "new to me" and "enjoyed" can both be true.
Health advertises `album_memory_context` and `enjoyment_feedback`; older
mobile clients and existing schema-v1 records remain compatible.

## 1.2.2 — Personal discovery sync

Added catalogue-independent Want Shelf, memory, feedback, Rest, and
introduction-provenance synchronization, plus background MusicBrainz checks
for external artists, release groups, editions and recordings. Existing
shelf v1 and credits APIs were unchanged. This was (and remains) server
support only; the corresponding mobile data and recommendation features
live in the app. See
[docs/discovery-api-v1.md](docs/discovery-api-v1.md) for the full contract.

## 1.2.1 — Radio DJ retired

Removed the retired Radio DJ analysis, its routes, worker registrations and
dependencies. The install migration drops the legacy DJ tables and DJ cron
schedules on upgrade while preserving catalogue, loudness/edge profiles,
collections, credits and relationship data. Old dedicated DJ workers had to
be stopped before upgrading past this release; running one against a 1.2.1+
database was never supported.

## 1.2.0 — Personal Shelves, credits, and DJ analysis (DJ era)

Added Personal Shelves synchronization and insights, MusicBrainz credits
enrichment, resumable album/artist relationship builds, source-bound edge
profiles, and **opt-in DJ analysis** with interrupted-work recovery and
bounded queue deferral. It carried forward the Navidrome identity guard from
1.1.9.

DJ analysis was disabled by default and required a compatible dedicated
worker; publishing this plugin never installed or enabled that worker on its
own. Credits matching remained subject to its configured audit gate. Friend
Album Discovery (`plugins/FederatedAlbums/`) remained excluded from the
public catalogue, as it still is today.

Radio DJ was removed in 1.2.1 (above); nothing from this era is part of the
plugin's current capabilities.

## 1.1.9 — Provider-identity reconciliation fix

Trusted pre-canonical Navidrome releases now publish ordinary track removals
and music-library scope changes through the normal catalogue diff, instead
of misclassifying them as incomplete provider-ID migrations. Exact identity
inspection stayed fail-closed for pending, uncertain or blocked canonical-ID
transitions.

## 1.1.8 — Adaptive reconciliation

The catalogue reconciler stopped running every minute while idle: it now
runs every minute only when durable work is ready, every five minutes while
an AudioMuse analysis parent is running, uses 1/5/15/60-minute retry
backoff, and falls back to an hourly safety sweep. The settings page started
reporting the real action, phase, duration, retry state and a bounded
journal of meaningful work, refreshed in the background without a page
reload.

## 1.1.7 — AudioMuse 3.4 queue compatibility

Removed all imports of AudioMuse's private RQ queues, jobs, dependencies and
retry objects, replacing task-within-task queueing with a database-driven,
one-action reconciliation watchdog. This recovered stranded analysis runs
and made profile, relationship, catalogue and provider follow-ups
restart-safe, restoring compatibility with AudioMuse 3.4.

## 1.1.6 — 1.1.0

Earlier fixes to provider-identity transition detection, catalogue-refresh
locking, and durable projection/migration recovery. See
`plugins/LumaeAnalysis/plugin.json` for the exact per-version changelog
text; none of it changes current behaviour beyond what
`docs/contracts/LUMAE_SYNC_CONTRACT.md` already describes.

## 1.0.1 — Resource safety

Bounded waveform decoding and filtering memory to decoder blocks instead of
letting it grow with a file's duration and sample rate; capped every
background job and background batch size; limited backfill and catalogue
query batches; replaced all-pairs relationship comparison with bounded IVF
shortlists (falling back to the last published generation while the index
warms up); and added an administrator maintenance pause that stops new
catalogue/projection/waveform/relationship work without deleting or hiding
already-published data.

## 1.0.0 and earlier

Replaced release-number approval with live API and data-contract admission
(1.0.0); introduced the provider-authoritative catalogue, Living
Collections, Personal Shelves-precursor collection backup/restore, and the
original waveform-only Lumae Analysis plugin. Full per-version text is in
`plugins/LumaeAnalysis/plugin.json`.
