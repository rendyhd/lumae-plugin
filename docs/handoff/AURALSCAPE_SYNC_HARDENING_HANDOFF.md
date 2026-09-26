# Auralscape sync hardening — hand-off for the Lumae app agent

Source: the post-audit final plan (`docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md` in `rendyhd/lumae-plugin`, branch `claude/epic-davinci-q0gj48`). The audit evidence is in `docs/audit/2026-09-24/` of that repo. Section IDs K1–K11 refer to §2 of that plan. They are summarised in H.3 below.

### H.0 Mission

You are the Lumae app agent working in `rendyhd/Auralscape`. Fix the client half of the post-audit plan so that:
1. the first sync of a large library (about 94k tracks, with waveform **and** edge profiles kept on the phone for offline playback) completes **once**, in one run, without being thrown away;
2. later syncs are small deltas;
3. sync failures are reported truthfully;
4. the client works against both the current plugin (1.2.5) and upcoming ones (1.3.0+), enabling new behaviour only through health capability flags.

The server plugin work happens in parallel in `rendyhd/lumae-plugin`. The authoritative contract is `docs/contracts/LUMAE_SYNC_CONTRACT.md` in that repo. Copy it into `docs/contracts/` here when it lands, and treat §H.3 as its summary until then.

### H.1 Ground rules (from this repo's AGENTS.md and CLAUDE.md, plus this plan)

- **Commands:**
  - tests: `npm test`; single file: `npx jest path/to/file.test.ts`
  - typecheck: `npx tsc --noEmit`; lint: `npm run lint`; `npm run test:pristine`
- **Commits:**
  - one per step, local; push only when the user says;
  - format `feat(phase-sync-hardening): Step N.N — Title` with bullet lines;
  - use the branch the user specifies (default: `sync-hardening`, not `main`).
- **Schema policy (AGENTS.md):**
  - any `sonic.db` DDL change bumps `SCHEMA_VERSION` (currently **37**, `src/store/sonicDb.ts:10`) with an explicit upgrade path chained into every `initDb` branch (1929-1945);
  - keep the no-DDL fast path;
  - update the version fingerprint (`sonicDbUpgrade.integration.test.ts:311-323`);
  - test against the frozen fixtures (`sonic-v33`, `sonic-v35`, `sonic-v36`) and a fresh database.
  - Batch all Phase 1 DDL into **v38**, and Phase 3 DDL into **v39**.
- **No mocking of admission in new sync tests.** Use the real `profilePublicationSource`, `providerIdentityAdmission` and `providerIdentityPreflight`, and control time with `jest.spyOn(Date,'now')` or fake timers, as `providerIdentityPreflight.test.ts:262,281` does. Existing mocks (for example `stockAudioMuseProfileV2.integration.test.ts:21-23`, `pluginEnrichmentSync.test.ts:69-76`) hid the P0 bug.
- Every fix starts with a failing test. Reproduce `ttlGuard.test.ts` from `lumae-plugin/docs/audit/2026-09-24/probes/client/`.
- **Do not change** the LUM-008 atomic waveform publication semantics, LUM-009 source and account fencing (except as described in C-1 and C-5), or v2 page/progress atomicity.
- **Report back** per step: files, tests added, full `npm test` result, typecheck and lint, plus any contract question for the plugin orchestrator.

### H.2 Current state you are starting from (verified at `main` 32680b5)

- v2 mode is selected only if health has `status:'ok'`, `profile_bootstrap.available`, protocol 2, schema 1, `auth==='host_authenticated'` and `transfer_contract==='source_scoped_v1'` (`src/services/profileBootstrapV2.ts:69-93`). v2 is used only for bootstrap. Incremental sync always uses legacy `/profiles/changes` (`pluginEnrichmentSync.ts:264-325`).
- An upsert without a valid `edge_profile` **deletes** the local edge (`publishedProfileRepo.ts:266, 351-388`). Keep that rule for servers without K6.
- The server's 1.2.5 edge payload is about 19 KB; the waveform payload about 0.5 KB. At 94k tracks that is 1.82 GB of JSON (about 0.89 GB gzip). The 24,000-character edge budget (`edgeProfile.ts:68,104`) has about 20% headroom.

### H.2a Contract facts to rely on (from the plugin's P0-4 contract doc)

- 1.2.5 ignores unknown body fields, query parameters and headers, so opt-in signals are safe to send to old servers. **Exception:** `page_size` on v2 page, catch-up or release requests returns 400. Send it only on create.
- A v2 route returning 404 means an older plugin without v2. v2 never returns 409.
- `/api/profiles?ids=` silently drops ids beyond the first 500 and does not list them in `missing`. Batch at 100 or fewer.
- `ref_lufs` arrives as float64 in change events and as float4 in reads and snapshots until plugin P1-1. Compare with tolerance, not exact equality.
- A `/profiles/changes` cursor ahead of head returns 400 `invalid_cursor`, not 410. Treat it as needing a full resync.
- Timestamps: `analyzed_at` has no zone; `expires_at` and `created_at` carry the server's offset. Parse offsets explicitly.
- **K6 edge references (plugin 1.3.0, P3-2; contract §3.7) for C-10.** Gate: `capabilities.profile_stream.edge_refs === true`. Send `edge_refs=1` on every `/profiles/changes` request and `edge_refs: true` in the v2 create body (the create response then echoes `edge_refs: true`). Only a waveform-only republish that kept its edge arrives as `payload.edge_profile_ref: {media_revision, profile_digest}` (never together with `edge_profile`); edge publications, snapshot pages, legacy bootstrap rows and `/api/profiles` always carry full edges, and journal events from before the upgrade keep their full edge. Keep the local edge when both fields match; otherwise drop it and queue the track, then fetch through `/api/profiles?ids=` (at most 500 ids, and within the host's 4,094-byte request line: batches of 100 are safe), verify the digest, and store the edge only for the event's `media_revision`. A reference can be stale (the edge was replaced later); the replacing event follows, and a fetched edge with another digest but the same revision is still valid. Without the opt-in nothing changes.

### H.3 Server capabilities you will detect

All live under `GET /plugins/lumae_analysis/api/.../health` → `capabilities`, and every one is optional. K1–K11 are listed in the appendix at the end of this document.

| Key | Meaning |
|---|---|
| `transport.gzip` | Informational. Responses are gzip-encoded when you send `Accept-Encoding` (native stacks do). |
| `profile_bootstrap.sliding_expiry` | Send `expiry_mode:"sliding"` on create; `expires_at` may move forward on each page. |
| `profile_bootstrap.idempotent_create` | Send `client_request_id` (a UUID per create attempt). |
| `profile_bootstrap.auth_enabled` | Informational. |
| `Retry-After` on 429/503 | Honour it. |
| `profile_stream.edge_refs` | Send `edge_refs=1` (query) or `edge_refs:true` (v2 body). Upserts may carry `edge_profile_ref:{media_revision, profile_digest}` instead of `edge_profile`. |
| `edge_profiles.compact_transport` | Optionally send `edge_compact=1`. `boundaries` is omitted; rebuild it before verifying. |
| `collections.feed_epoch` | Echo `epoch`; handle 410 `collections_resync_required` through `GET /plugins/lumae_analysis/api/collections/snapshot`; use `has_more`/`next_cursor`. |
| `collections.contract: 2` | Send `X-Lumae-Collections-Contract: 2` on collection and shelf mutations; handle 409 `membership_conflict {item_id, existing_item_id, conflicts, current}`, `idempotency_key_conflict` with `current` (may be `null`), and `collection_exists` / `collection_deleted` on create (C-13 server note). |
| `collections.source_scoped_items` | Items carry `catalog_instance_id`. |
| `lumae_analysis_profiles.analyzer_versions` includes 2 | Profiles may have `analyzer_ver:2` (BS.1770-4 `ref_lufs`). |

### H.4 Work items

#### Phase 1 — do now; independent of server changes, and backward compatible

**C-1 (P0) — In-run admission must not treat stale evidence as a source change (AUD-01).**
- **Where:**
  - `src/store/profilePublicationSource.ts` (`resolveProfilePublicationSource` 160-201, where lines 193-197 turn the `preflight_required` throw into `null`; `profilePublicationSourceStillAdmitted` 203-217; `captureProfileCatalogBinding` 32-89 at line 63; `profileCatalogBindingStillAdmitted` 91-106);
  - `src/services/pluginEnrichmentSync.ts` (`assertEnrichmentAdmission` 93-103; call sites listed at 195-581; relationships 327-455);
  - `src/services/pluginCatalogSync.ts` (609-624, 882, 985, 1016);
  - `src/store/catalogRepo.ts` (1880-1882, 1999-2001, 2010, 3528, 3609-3615).
- **Change:**
  1. Split "is this still the same source?" from "is the evidence fresh?". The in-transaction checks (`StillAdmitted`, the catalogue binding checks) compare **identity only**: provider fingerprint, source proof match, admission revision, settings identity, catalogue generation and revision, and admission scope (after C-5). They must **not** call the TTL-checking `assertProviderIdentityAdmission`.
  2. Before each page, publication or delta batch (outside any SQLite transaction), call a new `refreshInRunAdmission(access, signal)`. It awaits `ensureProviderIdentityAdmission(access,'sync')` (`src/services/providerIdentityPreflight.ts:414-441`) when the evidence is stale, then re-verifies identity. Treat only an **identity change**, or an admission that is actually denied, as `source_mismatch`. Treat preflight network failure as retryable (keep the checkpoint).
  3. Do the same for catalogue binding: refresh before entering the `catalogRepo` transactions, so `captureProfileCatalogBinding` never returns `null` on age alone. Otherwise the profile owner is deleted and every profile read returns nothing.
  4. Never delete staging or the checkpoint (`abandonProfileV2Session`, legacy staging cleanup at `pluginEnrichmentSync.ts:256-259`) for an age-only condition.
- **Reuse:** `createCatalogueReadAdmission` (`src/services/providerCatalogueAdmission.ts:51-80`). Parameterise its hard-coded `'provider_read'` (line 36) into an `access` argument, and reuse its single-flight and await pattern.
- **Tests:** real modules (no `profilePublicationSource` mock). Drive a v2 bootstrap and a legacy bootstrap across **>5 minutes** of `Date.now` with the preflight refresh succeeding: both complete with staging intact. With a real identity change mid-run, the run aborts as `source_mismatch`. With a preflight network failure, the checkpoint is kept and the run retryable. Also cover the relationship bootstrap and the catalogue delta drain.
- **Done:** `ttlGuard` reproduction inverted; no remaining `source_mismatch` caused by age alone.

**C-2 (P1) — Provider user-state reads must wait for admission (AUD-09).**
- **Where:** `src/services/providerSync/pullUserState.ts:362-397` (`pullProviderUserState` calls `getStarred2`/`getRatedAlbums` directly), called from `src/hooks/useForegroundSync.ts:476-499` (errors 510-523); `SubsonicProvider.buildUrl` asserts synchronously (`subsonicProvider.ts:1701-1745`, at 1724); `getRatedAlbums` (524-562) swallows all errors and returns `[]`.
- **Change:** wrap the user-state reads in `createCatalogueReadAdmission` with the new `access` parameter (the wait-and-retry-once pattern). `getRatedAlbums` must propagate admission errors (not return `[]`), so an admission failure isn't recorded as "no ratings".
- **Test:** the foreground sync with an AudioMuse phase longer than 5 minutes followed by user state succeeds (this reproduces the original incident).

**C-3 (P1) — v2 error handling and session hygiene (AUD-11 client, K4/K5).**
- **Where:**
  - `src/services/profileBootstrapV2.ts`:
    - create sits outside the recovery `try` (193-203); `source_mismatch` abandon is at 269-272;
    - `release_pending` returns 0 without applying the delta (184-187); `locallyExpired` is at 45-48.
  - `src/store/profileBootstrapV2Repo.ts` (`abandonProfileV2Session` 211-226, `discardObsoleteProfileV2States` 68-90);
  - `src/services/audioMuseClient.ts` (`fastFail` 451; timeouts 70-71, 1318, 1334; `canRetry` 1338; v2 methods 855-889).
- **Change:**
  1. Move create into the recovery flow, and release a created session if guard, validation or begin fails.
  2. **413** from create or catch-up: fall back to the legacy bootstrap for this run, and remember `v2_unavailable_until` per source for 24 h (in `sync_metadata`, so no DDL).
  3. **429 and 503**: honour `Retry-After` (cap 300 s) with exponential backoff, and keep the checkpoint. Do not fail the stream permanently. Record the outcome as `deferred`.
  4. **K5:** send `client_request_id` (a UUID persisted with the attempt) when `idempotent_create` is advertised.
  5. Before deleting local v2 state (abandon, obsolete-state discard, legacy-mode abandon at `pluginEnrichmentSync.ts:275-278`), make a best-effort server `release` with the stored token and a short timeout, and ignore failures.
  6. A pending release retries in the background and **does not** skip the delta or report `current`.
  7. A **400** while paging abandons that session and restarts once, instead of retrying the same token until expiry.
  8. Treat a 302, or an HTML response to a plugin API call, as `authentication_required`: AudioMuse redirects unauthenticated non-`/api/` paths to `/login`.
- **Tests:** each status path with a mocked HTTP client, and a real SQLite checkpoint. 413 falls back to legacy and completes; 429 with Retry-After is deferred and then succeeds; a create timeout does not leak (the release is called); a pending release still applies the delta.

**C-4 (P0 for scale) — Bulk edge persistence that works at 94k (AUD-02 client).**
- **Where:**
  - `src/store/publishedProfileRepo.ts`:
    - staging: `stageOne` 83-110, which does per-row DELETE+INSERT, and page commits 143-194;
    - `publishProfileSnapshot` 225-318: it loads **all** staged edge `update_json` at 267-270 and inserts one row at a time at 271-288;
    - `applyPublishedProfileChanges` 320-403.
  - `src/lib/edgeProfile.ts` (`parseEdgeProfile` 256-313, digest 99-106), `src/lib/sha256.ts`, and the re-parse on every read in `src/store/edgeProfileDb.ts:160,202`.
- **Change:**
  1. **Page size:** request v2 `page_size = 50` (and legacy `limit=50`, `/changes` `limit=100`) when edge profiles are available. That is about 1 MB of JSON per page.
  2. **Staging:** chunked multi-row inserts. Reuse `stageEdgeProfileUpdates` (`edgeProfileDb.ts:95`, `CHUNK=100`, multi-row VALUES) and the chunk constants and pattern from `enrichmentRepo.ts:29-140` (`stageProfileChunks`, `upsertProfileChunks`).
  3. **Edges no longer ride the whole-library publication transaction.** Edges are content-addressed: a row is usable only if its `media_revision` matches the track and its digest verifies. So:
     - each committed page (snapshot, catch-up or delta) writes its valid edges straight into `published_edge_profiles` with set-based `INSERT OR REPLACE … SELECT … FROM json_each(?)`, reusing `writeEdgeProfileUpdates` (`edgeProfileDb.ts:36`);
     - it does this in the same page transaction as the staging and progress checkpoint.
     - `publishProfileSnapshot` keeps its atomic waveform, cursor and refresh-marker swap, and then deletes edges only for tracks **absent** from the new generation or with a mismatched `media_revision`, in keyset chunks (`publishEdgeProfileStaging` pattern, `edgeProfileDb.ts:109`).
     - **No `getAllAsync` of all edges; no per-row JS loop over the library.**
     - Stale edges between a page and the publication are harmless, because lookups already require a `media_revision` match.
  4. **Validation cost:**
     - verify the digest once, at ingest; mark rows verified (a v38 column `verified INTEGER`) and skip re-parsing and re-hashing on read (`edgeProfileDb.ts:160,202`);
     - use a native SHA-256 (for example `expo-crypto` `digest`) when available, falling back to `sha256Hex`;
     - yield to the event loop between chunks.
  5. **v38 migration** (per the schema policy): the verified column and any index needed for chunked deletes; fixtures updated.
- **Tests:**
  - a 20k synthetic profile set with real-size edges (use the plugin repo's `docs/audit/2026-09-24/probes/lum010/edge.json` as the template);
  - peak JS heap during publication stays below 150 MB (measured under Node with `--expose-gc` via the existing `scripts/check-large-catalog-memory.cjs` pattern);
  - no single SQLite transaction exceeds 250 ms at 50 rows per page;
  - publication succeeds;
  - offline lookups return edges after publication;
  - a crash between pages resumes without duplicates;
  - a media change drops the stale edge.

**C-5 (P1) — Stable source key and admission scope (client P2 findings).**
- **Where:** `deriveSource` (`profilePublicationSource.ts:108-157`) mixes `authMode` and account into `settingsIdentity` (125-137); the same happens in `captureProfileCatalogBinding` (67-81). `providerIdentityConfigurationKey` (`src/lib/providerIdentityProof.ts:53-59`) is the MD5 of the whole config, including `sourceProof.verifiedAtMs` and the probe counts. It is persisted by `beginProfileV2Session` (`profileBootstrapV2Repo.ts:110`) and compared in `discardObsoleteProfileV2States` (74-78).
- **Change:**
  - Profile data is **source-scoped** on the server, so the publication `source_key` should derive from (provider fingerprint, normalized AudioMuse URL, `serverId`, `catalogInstanceId`, catalogue epoch) **without** auth mode or account.
  - The admission scope hashes only identity-relevant fields: provider type, URL, username and credential fingerprint, AudioMuse URL, auth mode, server and catalogue IDs. It excludes `verifiedAtMs`, probe statistics and unrelated settings.
  - v38 data migration: re-key the published tables' `source_key` from the old to the new key when exactly one old key maps to the new one. Otherwise mark a full refresh.
  - Clean up orphaned published copies under keys that are no longer active.
  - Keep LUM-009 account fencing for **personal** data (collections, ratings, outboxes). This change is for server-derived profile data only.
- **Tests:**
  - switching password ↔ token, or changing username, keeps profiles effective;
  - a 24 h proof refresh does not discard a resume or abort a run;
  - changing server URL or catalogue does abort;
  - the migration against the v36 and v37 fixtures.

**C-6 (P1) — Sliding expiry and clock skew (AUD-12, K3).**
- **Change:**
  - When `profile_bootstrap.sliding_expiry` is advertised, send `expiry_mode:"sliding"` and accept a **changed** `expires_at` on each page. Update the stored value, and relax the metadata-equality validator in `profileBootstrapV2.ts:95-151` for this field only.
  - `locallyExpired` (45-48) must use the server's latest `expires_at`, adjusted by a measured server-clock offset (from the HTTP `Date` header). This avoids restarts when the device clock runs ahead.
- **Test:** a run paused for 90 minutes and resumed completes without restarting (sliding); absolute mode behaves as before.

**C-7 (P1) — Gzip transport readiness (K1).**
- **Where:** the `Content-Length` guards in `audioMuseClient.ts` (798, 1293); `buildHeaders` (1249-1260) sets no `Accept-Encoding`, which is correct.
- **Change:** size guards must count decoded bytes (streamed length, or `text().length`), not the header. OkHttp and NSURLSession strip or keep `Content-Length` inconsistently when they decompress transparently.
- **Test:** a mocked gzipped response with no or compressed `Content-Length` parses correctly. **Native check (user):** on Android and iOS, confirm the request carries `Accept-Encoding: gzip` and that a gzipped plugin page decodes.

#### Phase 2 — measurement and scale gate

**C-8 — Client half of the representative end-to-end gate.**
- Build `scripts/e2e/lumae-sync-scale.test.ts`, run with Node and file-backed SQLite (`node:sqlite` as in `publishedProfileRepo.test.ts:10,49`).
- It drives the real `AudioMuseClient`, `runProfileV2Bootstrap`, the legacy path and the **real admission modules**, against the plugin's `scripts/e2e` server with about 94k profiles and real edges.
- Scenarios:
  - first load completes once;
  - the clock is advanced beyond 5 minutes;
  - kill and restart at 6 points;
  - both v2 and legacy;
  - a later delta, and a re-analysis pass that produces nothing to download.
- Record wall time, bytes transferred, peak heap, the longest SQLite transaction and JS blocking per page. Budgets: import chunk ≤100 ms of JS blocking; publication ≤2 s; no out-of-memory.

**C-9 — On-device performance follow-ups** found by C-8: move parse and validation off the JS thread, or batch it, if the C-4 changes are not enough.

#### Phase 3 — after the plugin ships each capability (all gated)

**C-10 — Edge references (K6).**
- When `profile_stream.edge_refs` is advertised, opt in, and handle `edge_profile_ref`:
  - keep the existing local edge (published or opportunistic) if the digest and `media_revision` match;
  - otherwise queue the track for fetch through `GET /api/profiles?ids=` (batches of up to 100; note the comma-joined ids; the server caps a request at 500 ids and the stock host's request line at 4,094 bytes), writing results through the C-4 path. Store a fetched edge only for the event's `media_revision`, after digest verification (H.2a, contract §3.7).
- Without the capability, keep today's rule.
- This is required before the plugin starts loudness v2 regeneration. Without it, every track would re-download its 19 KB edge.

**C-11 — Loudness analyzer v2 (K11).**
- Accept `analyzer_ver` 1 and 2 (`parseLumaeAnalysisProfile` currently requires ≥1).
- Normalise v2 `ref_lufs` as true integrated loudness. For v1 rows, apply the interim offset from the plugin's P3-1a spike result.
- SmoothFade uses the ramps of whichever version is stored.
- Surface the per-track analyzer version in diagnostics.

**C-12 — Compact edge transport (K7, optional).** When advertised, send `edge_compact=1` and rebuild `boundaries[i] = min(end, origin + floor(i*rate/10))`, using `rate = source.sample_rate` and the `origin_frame`/`covered_frames` of each window (matching `edgeProfile.ts` `validWindow` 134-163), **before** digest verification and storage. The golden fixture `src/lib/__fixtures__/edge-profile-v2.json` round-trips.

**C-13 — Collections client (K8/K9/K10; LUM-002/003/004/013/014 client side).**
- **Where:** `src/services/collectionSync.ts` (`sendMutation` 127-139 builds the reorder body at send time; `responseRevision` 80-85; feed 211-229) and `src/store/collectionsRepo.ts`:
  - reorder payload 554-605;
  - `markCollectionOutboxSucceeded` 676-700 ignores returned ids;
  - `markCollectionConflict` 717-744;
  - `resolveCollectionConflictKeepLocal` 746-777 ("Remote revision is unavailable" at 756; +1 revision rebase at 762-768);
  - global `change_cursor` 795-802;
  - `applyRemoteCollectionChanges` 857-990;
  - unique indexes in `sonicDb.ts:1295-1302`.
- **Change:**
  1. Freeze the reorder body (`ordered_item_ids`) at **enqueue**, and send exactly that, so a retry has the same fingerprint.
  2. Apply server-returned item ids (and the create response) to local rows.
  3. On 409, use `current` when present. Handle `membership_conflict` by adopting `existing_item_id`, and `idempotency_key_conflict` by refetching. Do not assume +1 revision per mutation: use the server's revision.
  4. **K8:**
     - scope the feed cursor per (AudioMuse source, account), a v39 DDL or `sync_metadata` keys;
     - echo `epoch`;
     - page by `has_more`/`next_cursor`, not "fewer than 200 rows";
     - on 410, fetch `GET /plugins/lumae_analysis/api/collections/snapshot` and merge with the outbox, preserving unsent mutations, memberships, order and undo;
     - a failed feed shows a recoverable state, not a sticky string.
  5. **Before the plugin's LUM-014 ships:** align the album unique index with the server. Make it partial where `provider_album_id IS NULL` for `album_key`, so same-name editions with distinct provider ids can coexist. In v39, include `catalog_instance_id` when K10 is advertised.
  6. Send `X-Lumae-Collections-Contract: 2` when `collections.contract==2`.
- **Tests:**
  - reorder retry after a lost response has the same body;
  - a server id remap is adopted and the feed continues;
  - a 410 resync keeps the outbox;
  - two same-name editions coexist.
- **Server note (plugin P3-4a, K8 implemented in 1.3.0, unreleased; contract §5.2, §5.2a, §5.3).**
  - Gate: `capabilities.collections.feed_epoch: true`. Every feed 200 then carries `epoch`, `head_seq`, `floor_seq` and `has_more`, whether or not you echo.
  - 410 `collections_resync_required` (`reason`: `epoch_mismatch` or `cursor_ahead`) comes **only** when you send a non-empty `epoch`. Without it, a cursor past head is still an empty 200. So send `epoch` from the first request after you have one, and treat `cursor_ahead` like a mismatch.
  - Snapshot: `{schema_version, scope, epoch, head_seq, floor_seq, collections, collection_count, items, item_count}`. Items are a flat list with `collection_id`; only active collections are included, so a collection missing from it is deleted. Afterwards continue the feed with `cursor=head_seq` and the snapshot's `epoch`. It is one response, not paged (20k items: 8 MB, 0.7 MB gzipped). A worker builds one snapshot at a time, so it can answer 503 with `Retry-After: 5`; retry then.
  - Collection mutations can now answer 503 `collection_busy` with `Retry-After: 5` (a lock wait over 3 s). Keep the mutation queued and retry with the same `Idempotency-Key`.
  - Restores above 2,000 rows commit in chunks. Send an `Idempotency-Key` and keep it for that backup: a retry with the same key and body resumes the restore. Until it finishes, reusing that key for anything else is 409 `idempotency_key_conflict`. Final revisions can exceed 2, so use the returned revision.
- **Server note (plugin P3-4b, K9 implemented in 1.3.0, unreleased; contract §5.3 "K9", §5.4).**
  - Gate: `capabilities.collections.contract: 2`. Then send `X-Lumae-Collections-Contract: 2` on collection mutations **and** on `POST /api/shelves/mutations`. Only the value `2` opts in; without it every response is 1.2.5's byte for byte. The header is not part of the idempotency fingerprint, and a 409 is never stored, so the same key can be retried after you adapt.
  - `409 {"error":"idempotency_key_conflict","current":<collection>|null}`: `current` is the collection the key was **first** used on (for a create, the created id, including a server-generated one), as it is now, tombstones included. It can differ from the collection your retry names, so match it by `current.id`. It is `null` for a restore's key, a receipt stored before 1.3.0, or a missing collection: refetch then. The key's earlier request succeeded (or is a restore still in progress), so stop retrying under it. Freezing the reorder body at enqueue (change 1) is still what avoids this 409.
  - `409 {"error":"membership_conflict","item_id","existing_item_id","conflicts":[{"item_id","existing_item_id"}…],"current":<collection>}` on item PUT and batch upsert. Nothing was written. `conflicts` lists every conflicting request item in request order (the top-level pair is the first). `existing_item_id` is the server's item for that track, provider album id or album key (exact match), or an earlier item of the same batch. Re-point each local item to its `existing_item_id`, then retry. The server no longer remaps silently under the header, so the "server id remap is adopted" test applies only without it.
  - `POST /api/collections` with a taken id: `409 {"error":"collection_exists","current":…}`, or `collection_deleted` with the tombstone. A create retried without a key after a lost response sees its own collection in `current`.
  - Restores are unchanged by the header (fresh ids; key conflicts carry `current: null`).
  - Shelves: with the header, reusing a mutation `id` with another body is `409 {"error":"idempotency_key_conflict"}` (no `current`) and changes nothing; re-read `/api/shelves/changes`. The same body still replays the stored response.
  - A request whose host auth method is neither the session nor the installation bearer (for example a plugin-scoped token) now gets 401 on every collections, shelves and personal-discovery route, instead of the shared library. Health still answers it 200, with `scope: null` on `collections`, `shelves` and `personal_discovery`: treat a null `scope` as "collections unavailable for this caller".

**C-14 — Readiness UI (LUM-017 client).** Show per-stream states: catalogue, waveform profiles, edge profiles, relationships, collections. Each has availability, freshness and last outcome, including `deferred` from C-3 and truthful partial success, with a scoped retry. Accessibility: screen-reader labels, focus order, and dynamic type or zoom.

**C-15 — Cookie vs bearer.** In token mode, send AudioMuse API requests with `credentials: 'omit'`, or clear the AudioMuse cookie on an auth-mode switch (`audioMuseClient.ts:1345` uses `credentials:'include'`), so a stale password-mode cookie cannot override the bearer on personal endpoints.

**C-16 — Hygiene.**
- Update `docs/remediation/LUM-010_CLIENT_HANDOFF.md`, which still describes `account_session`/`principal_binding` although the code requires `host_authenticated`/`source_scoped_v1`. Point it at the contract doc.
- Delete the unused second edge parser and writer in `src/store/enrichmentRepo.ts` (305-420) if nothing outside tests uses it.
- Optional: move `profile_bootstrap_v2_state.session_token` to secure storage (low risk today).

### H.5 Compatibility matrix you must keep green

| Plugin | Expected client behaviour |
|---|---|
| 1.2.5 (no new capabilities) | Legacy or v2 exactly as today, plus C-1, C-2, C-3 (413/429/503 handling), C-4, C-5, C-7. No new request fields sent. |
| 1.3.0 Phase 1 | Also sliding expiry, idempotent create and Retry-After. Gzip is transparent. |
| 1.3.x Phase 3 | Also edge references, analyzer v2, collections epoch, contract 2 and source-scoped items, each only when advertised. |

### H.6 Sequencing and report-back

1. Order: C-1 → C-2 → C-3 → C-5 → C-4 → C-6 → C-7. Batch the v38 DDL from C-4 and C-5 into one migration step.
2. C-8 once the plugin's `scripts/e2e` exists.
3. C-10, C-11, C-12, C-13 and C-14 as the plugin advertises each capability.
4. C-15 and C-16 at any time.

After each step, report: step ID, commit SHA, tests added, full `npm test`, `tsc` and lint results, and any contract question. Stop and ask the user before any push, device install, or change to the LUM-008/009 guarantees beyond C-1 and C-5.

---

## Appendix — contract changes K1–K11 (plan §2, verbatim)


**Rules:**
- Every server change is **additive or opt-in**. An unchanged client keeps working against every new plugin, and a new client keeps working against 1.2.5 and 1.3.0.
- New client behaviour is enabled **only** by a health capability flag or an explicit request field.
- The authoritative spec is `docs/contracts/LUMAE_SYNC_CONTRACT.md` (P0-4), copied into Auralscape by the app agent.

| ID | Change | Server side | Client side | Gate | Phase |
|---|---|---|---|---|---|
| K1 | Gzip transport for JSON ≥1 KiB | P1-4 | Verify decoding; fix `Content-Length` guards (C-7) | HTTP `Accept-Encoding` (native stacks send it) | 1 |
| K2 | v2 snapshots store an edge *reference* and resolve it at page read. Wire format unchanged, except a row whose edge was replaced after capture arrives without `edge_profile` (the catch-up re-supplies it). | P1-5 | None; already handled | None | 1 |
| K3 | v2 sliding expiry: each page extends `expires_at` to at most `created+24h` | P1-6 | Send `expiry_mode:"sliding"`; accept a changing `expires_at` (C-6) | `capabilities.profile_bootstrap.sliding_expiry:true`; create body field | 1 |
| K4 | `Retry-After` on 429 and 503; truthful `available`; new `auth_enabled` field (the `auth` string is unchanged) | P1-6 | Back off and honour `Retry-After` (C-3) | Always additive | 1 |
| K5 | Optional create `client_request_id`; a duplicate unclaimed session is replaced, not leaked | P1-6 | Send a UUID per create attempt (C-3) | `profile_bootstrap.idempotent_create:true` | 1 |
| K6 | **Edge references in events and pages:** with `edge_refs=1` (query) or `edge_refs:true` (v2 body), an upsert whose edge is unchanged carries `edge_profile_ref:{media_revision, profile_digest}` instead of the full edge. Without the opt-in, the server expands to the full edge exactly as today. | P3-2 | Keep the local edge when digest and revision match; fetch misses through `GET /api/profiles?ids=` (C-10) | `capabilities.profile_stream.edge_refs:true` | 3; must ship before P3-1 regeneration |
| K7 | Compact edge transport (optional): with `edge_compact=1` the server omits the derivable `boundaries`, and the client rebuilds them from `source.sample_rate` and `source.decoded_frames` **before** verifying the unchanged v2 digest | P3-3 | Rebuild, then verify (C-12) | `capabilities.edge_profiles.compact_transport:true` | 3, optional |
| K8 | **Collections feed:** the response adds `epoch`, `head_seq` and `has_more`. 410 `collections_resync_required` is returned **only** when the request echoes `epoch` and it mismatches, or when the cursor is past head. A new snapshot endpoint returns all of the principal's collections, items and head in one REPEATABLE READ transaction. | P3-4 | Echo the epoch; resync on 410; page by `has_more`/`next_cursor` (C-13) | `capabilities.collections.feed_epoch:true` | 3 |
| K9 | **Collections conflicts:** with header `X-Lumae-Collections-Contract: 2`, `idempotency_key_conflict` includes `current`, and a duplicate membership returns 409 `membership_conflict {existing_item_id}` instead of a silent id remap. Create with an existing id returns 409. | P3-4 | Handle both; freeze reorder bodies at enqueue (C-13) | `capabilities.collections.contract:2` | 3 |
| K10 | Collection items carry `catalog_instance_id` (LUM-013, additive); workbench routes take an explicit catalogue | P3-5 | Store and scope items (C-13) | `capabilities.collections.source_scoped_items:true` | 3 |
| K11 | Profiles may carry `analyzer_ver:2` (BS.1770-4 `ref_lufs` and new ramps) | P3-1 | Accept v1 and v2; normalise by version (C-11) | `capabilities.lumae_analysis_profiles.analyzer_versions:[1,2]`, `loudness_method:"bs1770-4"` | 3 |

**Unchanged by design:**
- v2 `page_size` stays client-chosen (1–500). Byte-sized pages are a client choice: use `page_size` about 50 when edges are present.
- The legacy `limit` and `/changes` `limit` also stay client-chosen.


