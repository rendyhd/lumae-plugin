# Changelog

Notable changes to the `lumae_analysis` plugin. Per-version metadata
(`min_core_version`, checksum, source zip) is authoritative in
[`plugins/LumaeAnalysis/plugin.json`](plugins/LumaeAnalysis/plugin.json); this
file gives the human-readable story, oldest detail first collapsed to what
still matters. Exact, code-verified wire behaviour for every version lives in
[`docs/contracts/LUMAE_SYNC_CONTRACT.md`](docs/contracts/LUMAE_SYNC_CONTRACT.md).

## Unreleased (1.6.0)

- Emby and Lyrion are gone. Neither was ever admitted as a Lumae catalogue
  source; their dormant catalogue readers and the Emby and Lyrion stream and
  artwork branches of the Living Collections workbench are removed, as is the
  Emby artwork branch of the private FederatedAlbums plugin (its Jellyfin
  artwork now authenticates with the `Authorization: MediaBrowser` header,
  which Jellyfin 12 requires by default). A persisted source of either type
  stays hidden as before: it is never fetched, streamed, rekeyed or deleted.

## 1.5.0 (2026-10-05)

- Custom covers (health `capabilities.covers`, contract §5.6). The Lumae app
  and Lumae Radio keep the cover a person chose for a saved Vibe, a Living
  Collection or a playlist in step through `GET /api/covers/changes` and
  `POST /api/covers/mutations`: an album from inside it, an orb colour or a
  photo. Covers are revisioned records scoped by account and catalogue like
  saved Vibes, kept apart from the Vibe record so a client that rewrites a
  recipe cannot erase them; a delete means "back to the automatic cover".
  Photos go to a small image store (`POST /api/covers/images`, `GET
  /api/covers/image`): JPEG, PNG or WebP up to 512 KB, named by their SHA-256
  so an upload is idempotent, at most 2,000 per scope, served with an
  immutable cache. A cover can name a photo only once it is stored; an image
  no cover uses is deleted after two days. Receipts expire with the Vibe
  receipts, and a provider-identity rekey rewrites album ids inside covers.

## 1.4.0 (2026-10-03)

- Saved Vibe sync (health `capabilities.vibes`, contract §5.5). The Lumae app
  keeps its saved Palette, Mood Compass and DNA Vibes in step across devices
  through `GET /api/vibes/changes` and `POST /api/vibes/mutations`, scoped by
  account and catalogue like Album Shelf and switched on by the same Living
  Collections setting. Each Vibe is one compact record with a revision: a
  write based on an older revision is refused with the current record
  (`vibe_conflict`), so two devices never overwrite each other silently, and
  deletes are tombstones every device learns about. The plugin stores the
  Vibe as the app sends it (at most 64 KB) and never interprets the recipe.
  Receipts replay a lost response and expire after 30 days with the shelf
  receipts; a provider-identity rekey rewrites song ids inside stored Vibes
  without changing their revision.

## 1.3.4 (2026-10-02)

- A retried profile-bootstrap create adopts the snapshot its own earlier
  create captured (K12, health `profile_bootstrap.create_adoption`). The v2
  capture runs inside the create request and grows with the library; at
  about 132k profiles on a home server it outlasted the Lumae app's 10 s
  request, and every K5 retry replaced the finished capture and captured
  again, so the first profile load never completed. Now a retry under the
  same `client_request_id` takes over the ready, never-paged session (or
  waits up to 5 s for its capture, then 503 with `Retry-After`) and answers
  at once. Different create options, a changed source epoch, or a session
  that has served a page keep the 1.3.0 replace behaviour. Adoption counts
  against neither the create rate limit nor the session slots.

## 1.3.3 (2026-10-02)

- Artist portraits in the catalogue. A Navidrome scan calls `getArtists` once
  per selected music folder and publishes each album artist's `coverArt` as
  `artists.cover_art_id`, so the Lumae app shows catalogue portraits instead
  of looking up every artist from each phone. The art ID is part of the
  artist fingerprint, so a changed portrait republishes the artist (the first
  scan republishes every artist that has one). An empty `coverArt` (Navidrome
  0.64+: no image) clears it; guests, which `getArtists` does not list, and a
  failed lookup keep the published value. A failed `getArtists` never fails
  the scan.

## 1.3.2 (2026-10-01)

- Saved (`deferred`) on-demand requests are served within about a minute on an
  otherwise idle server. A deferral wakes the reconcile watchdog, and its
  schedule counts deferred work as ready until it is served. In 1.3.0 and 1.3.1
  they waited for the next idle (hourly) tick when no batch or AudioMuse
  analysis was running (seen on the media host: 261 deferred edge requests).

## 1.3.1 (2026-09-30)

- The edge library pass starts within a minute when a worker first reports a
  qualified runtime or a catalogue refresh has changes. In 1.3.0 a fresh
  install or upgrade waited for the watchdog's next idle (hourly) tick.
- A worker's periodic runtime report (every 10 minutes) no longer re-arms a
  finished pass, so the six-hour re-sweep delay holds.

## 1.3.0 (2026-09-30)

Released with user approval (`docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md`,
P4-1). No 1.2.5 process may keep running against an upgraded database — follow
[`docs/runbooks/UPGRADE_1.3.md`](docs/runbooks/UPGRADE_1.3.md). Work-package-level
progress and test evidence are in [`docs/STATUS.md`](docs/STATUS.md); the
precise wire changes (K1–K11) are in
[`docs/contracts/LUMAE_SYNC_CONTRACT.md`](docs/contracts/LUMAE_SYNC_CONTRACT.md)
§7. Summary:

- Health `capabilities.edge_profiles` adds `served: true` and a live `stored: bool|null`, so clients can tell whether profile transfers carry edges; `available`/`enabled` only describe the answering process's PyAV runtime (contract §2).
- **Edge profiles work on a stock AudioMuse worker.** The exact PyAV 16.1.0 pin is replaced by runtime self-qualification against bundled reference measurements (PyAV >= 16; AudioMuse's 17.1.0 is bit-identical). Workers report their runtime, and health's new `analyzable` says whether queued edge jobs will be computed; the web process no longer needs PyAV to queue them.
- **On-demand first, library in the background, for waveforms and edges.** The server walks the library for edges at low priority from the reconcile watchdog (`server_backfill`). On-demand edge requests promote tracks a background batch holds; an on-demand waveform's edge follows at on-demand priority. A request AudioMuse refuses because another task runs is saved and served by the next worker task (`deferred`) instead of failing. Background batches serve that work between tracks and yield after a budget. Unmeasurable media is `unsupported` and not retried. Credits and metadata yield to on-demand work again. `GET /api/profiles?edge_status=1` reports per-track edge state (contract §2, §3.2, §3.6).
- **Vectors after a projection rebuild:** a request for a pruned analysis generation is served from the current one with checksums and an explicit `missing` list, never a silently empty page; `strict_generation` returns 410 `generation_expired` (contract §3.8).

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
