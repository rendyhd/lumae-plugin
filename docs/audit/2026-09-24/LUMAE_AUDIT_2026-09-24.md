# Lumae — full audit of the plan, the implementation and the current state

**Date:** 24 September 2026
**Scope:** the remediation overview (`LUMAE_FULL_REMEDIATION_OVERVIEW_2026-09-24.md`), the Lumae plugin, the Auralscape client side of the sync protocol, and the AudioMuse-AI host boundary.

**Revisions audited**

| Repository | Revision | Note |
|---|---|---|
| `rendyhd/lumae-plugin` | `main` = `4153207` | Contains every remediation checkpoint, from `816420f` through `48545bd`. |
| `rendyhd/Auralscape` | `main` = `32680b5` | "Merge codex/lum010-no-host-pr into main". Shallow clone, 60 commits deep. |
| `NeptuneHub/AudioMuse-AI` | `f100684`, `ce74293`, and current `main` = `8aa1639c` | Read-only. |

**Method.** One lead reviewer and five parallel read-only reviewers covered:
- LUM-010 and the host boundary;
- server integrity (LUM-001/006/007/008/021);
- collections and the workbench (LUM-002/003/004/013–016);
- the Auralscape client (LUM-008/009/010);
- performance and architecture (LUM-011/012/016/020).

The following were executed (the full list is in Appendix A):
- the full plugin suite on disposable PostgreSQL 16;
- focused suites;
- about 30 targeted probes, each written to *assert the defect*;
- `EXPLAIN (ANALYZE, BUFFERS)` against a synthetic representative-scale fixture (132k tracks, 69k analysis items, 132k links, 94k profiles);
- focused client Jest suites, plus a client reproduction test.

The probes are checked in under [`probes/`](probes/).

**Not executed:**
- a real AudioMuse container;
- a real phone;
- PostgreSQL 17 (CI uses 17; this audit used 16);
- the full client Jest suite;
- production data.

**Confidence labels:**
- **CONFIRMED**: reproduced by a probe or measurement.
- **HIGH**: established by reading code, with a concrete path.
- **MEDIUM** or **SPECULATIVE**: as stated.

---

## 1. Executive summary

1. **The plan is out of date.** The work it says is next (the no-AudioMuse-PR correction for LUM-010) is already implemented, qualified and merged:
   - plugin `48545bd`; Auralscape `4416a61`; the ledger and RESUME record a PASS on stock AudioMuse.
   - It is on `main` in both repositories, which were pushed.

   Do **not** run the plan's Section 14 launch prompt as written; it would redo finished work. §3.1 lists every stale claim.
2. **"No AudioMuse PR is required": agreed.**
   - `plugin.api.config` is exported, and `config.DATABASE_URL` resolves the same tables, on `f100684`, `ce74293` and current `main`.
   - Nothing relevant changed in the host after `ce74293`.
   - The experimental host branches should stay unmerged.
3. **Most of the server correctness work is sound.** LUM-001, 002, 003, 004 and 006 hold up under probing:
   - a concurrent feed probe (8 writers with jitter): no skipped or duplicated events;
   - an exhaustive LUM-006 probe: 20,736 state combinations with zero SQL/Python disagreements;
   - lock ordering is consistent, and no helper commits behind its caller.
4. **The end-to-end sync that motivated the programme is still broken at the user's real scale.** The causes are ones the programme never tested, because its qualification ran on tiny fixtures with the admission guard mocked out:
   - **AUD-01 (P0, client).** A 5-minute identity-evidence timeout is treated as "the source changed" in the middle of a sync. The client then **deletes its staged progress**. Any profile bootstrap longer than 5 minutes cannot complete on either the v2 or the legacy path. The original incident's full-library (94k) enrichment took 6m40s. This is very likely a root cause of the original `succeeded=false` incident.
   - **AUD-02 (P0 for release, both repositories).** The phone needs the full edge copy for offline playback, because many AudioMuse servers are not reachable from outside the home network. But neither bootstrap path can deliver it at real scale. Edge profiles make up **about 97% of every profile payload**: about 19 KB of edge data against about 0.5 KB for the base profile, which is 1.8 GB uncompressed at 94k tracks. Effects:
     - v2 bootstrap returns **413 once a source has about 7k edge-profiled tracks**.
     - The client never falls back to legacy after a 413.
     - The legacy publication loads every staged edge into JavaScript inside one transaction.
     - Nothing is compressed: gzip alone halves the transfer.
     - The "only once" premise is broken too: AUD-01, AUD-03 and AUD-12 each make the full first load happen again.
   - **AUD-03 (P1, server).** A float4-versus-float64 comparison makes almost every no-op re-analysis count as a change:
     - it republishes the profile and **deletes its edge profile**;
     - that triggers a media re-download and an edge re-analysis.
     - An AudioMuse re-analysis pass therefore floods the journal past its 50k-event retention, forcing every client into a full bootstrap, which then runs into AUD-01 and AUD-02.
5. **Rollout hazard (AUD-05, P1).**
   - A single 1.2.5 worker left running after the upgrade **permanently blocks every collection write in the installation** (confirmed by probe).
   - The plugin already has a worker-version fence, but it cannot work because the remediated source still says `PLUGIN_VERSION = "1.2.5"`, and the plan forbids a version bump.
6. **`main` has never been validated by CI since the remediation (AUD-06).**
   - The push ended with a `[skip ci]` commit. The last CI run was on the 1.2.5 release commit.
   - Both CI steps fail on the current tree:
     - pytest: 569 passed, 3 failed (reproduced here);
     - `build_catalog.py --check`: "published versions are immutable".
   - The release model (`main` *is* the public catalogue) conflicts with developing unreleased work on `main`.
7. **No performance work has shipped.** The measurements show cheap, high-yield fixes:
   - per-publication journal compaction: 24 ms, against 0.09 ms for an equivalent range delete;
   - a no-change projection: 20 s and 1.5 GB RSS;
   - health p50: 372 ms, and 1.5 s during a provider transition;
   - an admin settings poll every 5 s at 544 ms.

   On a host that serves everything from **4 gunicorn threads**, these routes are host-wide availability issues.
8. **Loudness (LUM-005) is measurably wrong** against a BS.1770 reference:
   - about −3 dB for stereo (−2.9 to −3.0 dB at 44.1–48 kHz);
   - up to **−8.4 dB** for bass-heavy content at 96 kHz;
   - **−7.7 dB** on a track with a long quiet section.

   The causes are channel averaging, fixed 48 kHz filter coefficients and a missing relative gate.

### Top actions, in order

| # | Action | Fixes | Size |
|---|---|---|---|
| 1 | Stop treating stale identity evidence as a source change inside sync. Refresh the evidence, or compare identity rather than age; never delete staging on age alone. Add wait-and-retry for provider user-state reads. | AUD-01, AUD-09 | Small (client) |
| 2 | Keep the bulk edge copy (offline playback needs it), but make the first load feasible and happen once: gzip responses; no edge JSON copied into v2 session snapshots; byte-sized pages; client writes content-addressed edges page by page instead of in one giant transaction; fallback and backoff on 413, 429 and 503; events reference an unchanged edge by digest instead of re-sending it. | AUD-02 | Medium (both) |
| 3 | Compare `ref_lufs` at float4 precision. Do not delete edge profiles when only the waveform changed but the media revision did not. | AUD-03 | Small |
| 4 | Replace the per-publication full-journal delete with an index range delete. | AUD-04 | Trivial |
| 5 | `ALTER … seq DROP DEFAULT` on `collection_changes`. Bump `PLUGIN_VERSION`, and extend worker attestation to every writer. Add a startup invariant check. | AUD-05 | Small |
| 6 | Make CI meaningful again: run the release-identity tests only in `source` mode or on release tags; protect `main`; no `[skip ci]` on code pushes. | AUD-06 | Small |
| 7 | Serve LUM-011 health and the settings poll from a committed summary; make GET routes read-only. | AUD-08 | Medium |
| 8 | LUM-012 projection: tuple diff, server-side vector hash, set-based copy of unchanged rows. | AUD-07 | Medium |
| 9 | Make LUM-010 create operable: per-source lock, capture off the request thread, logged errors, purge of stale rows. Or collapse to a lease design (§3.3e). | AUD-11 | Medium |
| 10 | Add a representative-scale end-to-end gate: 94k profiles including edges, a run longer than 5 minutes, real admission. | Process | Medium |

---

## 2. Release-blocking findings (P0 and P1)

### AUD-01 — P0, client. A 5-minute identity timeout aborts long syncs and deletes their progress. CONFIRMED (unit reproduction) / HIGH (end to end)

**How the check fails:**
- Every page runs the admission check about 4 times: `assertEnrichmentAdmission` → `profilePublicationSourceStillAdmitted` → `resolveProfilePublicationSource(db, true)`.
  - Files: `src/services/pluginEnrichmentSync.ts:93-103`, `src/store/profilePublicationSource.ts:185-216`.
- In write mode that calls `assertProviderIdentityAdmission('analysis_sync')`. It throws `preflight_required` once the evidence is older than `PROVIDER_IDENTITY_PREFLIGHT_TTL_MS = 5 min` (`src/services/providerIdentityAdmission.ts:24, 332-350`).
- The resolver catches the throw and returns `null`. The caller reports it as `source_mismatch`, which is not retryable.
- On `source_mismatch`, v2 calls `abandonProfileV2Session(…, false)` (`src/services/profileBootstrapV2.ts:269-272`). That **deletes the staged pages and the durable checkpoint**. The legacy path does the same (`pluginEnrichmentSync.ts:212-215, 256-259`).

**Why nothing prevents it:**
- The evidence is refreshed only at launch, on foreground, on reconnect, and once when a sync starts (`useProviderIdentityPreflight.ts:76-95`, `coordinator.ts:589`). Nothing refreshes it during a run.
- A stale check does ask for a refresh (`preflightRequester?.()`), but only *after* the run has already been rejected.

**Evidence:**
- A reproduction test that changes only the clock (`probes/client/ttlGuard.test.ts`): the guard returns `true` at +4 min and `false` at +5 min 1 s.
- Every relevant test mocks `profilePublicationSource`, including the stock-host integration test (`stockAudioMuseProfileV2.integration.test.ts:21-23`), which is why qualification never saw this.

**Consequences:**
- A full-library bootstrap (6m40s in the incident) livelocks on **both** paths, which defeats LUM-010's resume.
- `captureProfileCatalogBinding` can return `null` after a long catalogue transfer. The profile owner is then deleted and every profile read returns nothing. The next sync forces a full catalogue bootstrap again (`pluginCatalogSync.ts:609-624`, `catalogRepo.ts:3609-3615`).
- The outer catch re-runs the check, so the failure is reported as a source change.

**Fix:**
- Inside a run that is already admitted, check the identity itself (provider fingerprint, source proof, admission revision, configuration scope), not the age of the evidence.
- When the evidence goes stale mid-run, refresh it and wait, as `providerCatalogueAdmission.ts:57-77` already does for catalogue reads. Treat only an actual identity change as `source_mismatch`.
- Never delete staging because of age.
- Add a fake-timer regression test that runs past 5 minutes with the real `profilePublicationSource`.

### AUD-02 — P0 for release, plugin and client. Bulk edge payloads break both bootstrap paths at real scale. CONFIRMED (server) / HIGH (client)

**Payload size (measured):**
- A real `analyze_edge_blocks` payload is **18.3–19.5 KB**, almost independent of track duration: 18.9 KB at 30 s and 19.3 KB at 240 s.
- The base profile is **0.5 KB** (`probes/loudness_and_payload/edge_size.py`, `base_size.py`).
- `serialize_profile` puts the edge profile inline in every snapshot row, legacy page and journal event (`catalog_enrichment.py:428-457`, `edge_profile_store.py:155`).
- Edge profiles are on by default (`__init__.py:2763`).

**Server side:**
- `MAX_SNAPSHOT_BYTES` and `MAX_CATCHUP_BYTES` are 128 MiB (`profile_bootstrap.py:31, 33`), so `create_session` returns **413 at roughly 6.5–7k edge-profiled tracks**.
- Probe: 7,000 tracks took 9.4 s of capture under the global creation lock, then returned 413. The failed capture had already written 95 MB of WAL and left a 155 MB dead table.

**Client side:**
- A 413 from create is outside the recovery `try`, so the stream fails on every sync. There is no legacy fallback: the mode comes only from health, where `available = bool(DATABASE_URL)` (`__init__.py:1842`).
- A delta 410 routes into v2 and then gets a 413, so every sync loops 410 → 413.
- A 413 from catch-up loops until the 60-minute expiry, then restarts.
- The legacy publication transaction (`publishedProfileRepo.ts:225-318`):
  - loads **every staged edge `update_json` at once** (about 1.8 GB at 94k);
  - parses and inserts them one row at a time, on the main connection, with UI reads blocked.
  - Before LUM-008, edge publication was chunked.
- Staging also regressed from multi-row chunk inserts to 2 DELETE + 2 INSERT per row.
- Per-edge validation (base64, canonical JSON, pure-JS SHA-256) runs synchronously per page. Estimated at 0.5–1.5 s of blocking per page (SPECULATIVE).

**Requirement.** The bulk copy is required: offline playback needs every edge profile on the phone, because many users' AudioMuse is reachable only on the home network. So the goal is to make a large *first* load feasible and to make sure it really happens only once. The on-demand warmup (`edgeProfileWarmup.ts:53-190`) can stay as a gap filler, but it does not replace the bulk copy.

**First-load size at 94k tracks** (`probes/loudness_and_payload/edge_gzip.py`, 40 distinct synthetic tracks, so real music may compress somewhat differently):

| Encoding | Per track | 94k tracks |
|---|---|---|
| Current JSON, uncompressed (what is sent today) | 19.4 KB | 1.82 GB |
| Current JSON, gzip | 9.4 KB | **0.89 GB** |
| Without the derivable `boundaries` arrays, gzip | 8.2 KB | **0.77 GB** |
| Decoded binary arrays at rest on the phone | about 10 KB | about 0.95 GB |

**Fix, keeping the bulk copy:**

1. **Compress the transfer.** The host sends no `Content-Encoding`. Gzip the profile page, catch-up and changes responses in the plugin when `Accept-Encoding: gzip` is present. React Native's networking stack decompresses gzip transparently. This needs no contract change and halves the transfer.
2. **Do not copy edge JSON into v2 session snapshots.** Each create currently writes up to the whole library's edges into `profile_bootstrap_snapshot`, which is about 1.8 GB of JSONB per session. Instead:
   - Snapshot rows keep the waveform payload plus an edge reference `(media_revision, profile_digest)`.
   - Pages resolve the edge payload from `edge_profiles` at read time.
   - If the edge changed after capture, the catch-up delivers the new one, and the digest tells the client which version it holds.
   - Alternatively, run a separate keyset-paged edge stream. Edges are content-addressed and self-validating, so they do not need snapshot consistency.

   Either way the 128 MiB snapshot cap stops counting edge bytes; bound edge work per page instead.
3. **Size pages by bytes, not rows.** 250 rows is about 5 MB of JSON per page. Aim for about 1 MB per page, and move validation off the JavaScript thread or into batches.
4. **Write edges on the client page by page, not in one giant publication.** An edge row is valid only while its `media_revision` matches the track and its digest verifies, so it does not need the whole-library atomic swap that waveform profiles use:
   - upsert edges as each page commits;
   - drop stale ones by media revision or by tombstone;
   - keep the atomic publication for the waveform and cursor state only.

   This removes the about 1.8 GB JavaScript load and the doubled staging-plus-published storage.
5. **Do not re-send unchanged edges.** Journal events currently embed the full edge on every waveform republish (`edge_profile_store.py:155`, `profile_publication.py:381`). Send the edge only when its digest changed, and otherwise send `edge_profile_digest` so the client keeps its copy. A future analyzer change such as LUM-005 v2 then costs about 0.5 KB per track instead of about 19 KB.
6. **Make "only once" true:**
   - AUD-01: the 5-minute timeout deletes progress.
   - AUD-03: no-op re-analysis re-sends everything and forces a re-bootstrap past 50k events.
   - AUD-12: the 60-minute session expiry restarts from zero.
   - Size the journal retention to cover at least one full library pass (about 94k events), or make it byte-based.
7. **Optional, needs a representation revision.** Drop the `boundaries` arrays, which are derivable from `origin_frame`, `covered_frames` and the sample rate (`edge_profiles.py:146-152`); that is 28% of the uncompressed payload. Store decoded binary on the phone instead of JSON strings. The digest must then be defined over the compact form, or the client must rebuild `boundaries` before verifying.
8. **Client error handling:**
   - fall back to legacy on 413;
   - back off and honour `Retry-After` on 429 and 503;
   - release abandoned sessions.

### AUD-03 — P1, server. A no-op re-analysis republishes the profile, deletes its edge profile and floods the journal. CONFIRMED

- The no-op check (`profile_publication.py:342-346`) compares `float(previous.ref_lufs)` read from a `REAL` (float4) column (`__init__.py:1216`) against the analyzer's float64. They are almost never equal.
- So every identical re-analysis:
  - emits a new journal event;
  - deletes `edge_profiles` and `edge_profile_jobs` (`:348-358`);
  - causes `_schedule_edge_upgrade` to re-download the media and re-run the edge analysis.
- Probe: LUFS −14.123456789 completed twice → the head moved from 1 to 2 and 0 edge rows remained.
- The AudioMuse re-analysis hook (`__init__.py:3187`) admits every song. At 94k tracks, one pass therefore:
  - produces about 94k events, which exceeds the 50k retention (`PROFILE_CHANGE_RETENTION_EVENTS`);
  - sends every client a 410 and a full bootstrap;
  - which then runs into AUD-01 and AUD-02.
- Delta and bootstrap payloads also disagree on `ref_lufs` (float64 versus float4).

**Fix:**
- Compare at stored precision, for example `np.float32(x) == np.float32(y)`, or store the value as `DOUBLE PRECISION`.
- Keep edge profiles whose `media_revision` still matches: the edge profile depends on the media, not on the waveform row.
- Test that identical completions do not change the head.

### AUD-04 — P1, server performance. Every profile publication scans the whole journal. CONFIRMED

- `record_profile_change` → `compact_change_journal` deletes with `epoch<>X OR (epoch=X AND seq<=N)` (`catalog.py:73-80`). That predicate forces a sequential scan of the retained journal: 50k rows, **24 ms per publication**.
- The scan runs while the publisher holds the stream-state and `catalog_state` locks.
- Over a 94k backfill that is about **38 minutes of serialized database time**, and a ceiling of about 40 publications per second per source.
- An index range delete (`epoch=%s AND seq<=%s`) takes **0.09 ms**.

**Fix:** use the range delete per publication, and purge other epochs only on epoch rotation or in `compact_enrichment_storage`. The floor semantics are unchanged, so LUM-001 is preserved.

### AUD-05 — P1, rollout. An old worker wedges the collection feed, and the version fence is disabled. CONFIRMED (probe)

**Collections:**
- The migration keeps `seq BIGSERIAL`'s `nextval` default (`collection_manager.py:134`). There is no `setval` and no `DROP DEFAULT`.
- A 1.2.5 writer inserting without an explicit seq takes the frontier's next number.
- Every later `_record_change` then allocates the same number, hits a UniqueViolation and rolls back, which restores the head. **All collection writes for all users fail, and it does not heal itself.**
- A gunicorn reload or rolling restart is enough to overlap old and new workers.

**Profiles:**
- An old worker still writes `source_profiles` 'ready' rows and journal events, but no `published_source_profiles` row.
- New code treats the track as current and never republishes it, so bootstrap and delta diverge permanently. There is no diagnostic for "ready but not published".

**Existing fence:**
- `assert_preparation_worker_current` and `preparation_attestation_is_current` (`__init__.py:4203-4228`, `:1352`) already refuse work from a worker whose version differs.
- But `PLUGIN_VERSION` is still `"1.2.5"` (`__init__.py:117`), so remediated and old workers look identical.
- The plan's "no release-version bump" rule directly prevents its own "old-worker drain" gate.

**Fix:**
- Bump `PLUGIN_VERSION`, and `CATALOG_BUILDER_VERSION` if the builder changed.
- Extend attestation to the profile publisher and collection writers.
- Run `ALTER TABLE … collection_changes ALTER COLUMN seq DROP DEFAULT` so old writers fail closed.
- Check `MAX(seq) <= head_seq` at startup and in health, returning 503 with a documented repair.

### AUD-06 — P1, release process. `main` is unvalidated and CI would be red. CONFIRMED

- The GitHub Actions history shows no run after `4276398` (the 1.2.5 release). `main` = `4153207` "chore: skip automated plugin build for remediation push [skip ci]", and `main` is unprotected.
- On the current tree:
  - `pytest tests/plugins`: **569 passed, 3 failed**, reproduced on PG16. The 3 are `test_release_channels.py`.
  - `python scripts/build_catalog.py --check`: `ValueError: … published versions are immutable; select a new version`.
- **Root cause.** `release-sources.json` has `LumaeAnalysis` in `source` mode at 1.2.5, and two tests assert "working source == latest published archive". Any development on `main` therefore turns CI red, and a new unchecksummed version *publishes*.
- The programme normalised "3 unchanged archive failures" for about two days, and then bypassed CI.

**Fix:**
- Run the "source == archive" assertions only when `mode == "source"` and HEAD is a release commit or tag.
- Put `main` in `pinned-artifact` mode while it carries unreleased source (the builder already verifies pinned checksums).
- Or develop on a branch and merge to `main` only at release.
- Turn on branch protection with a required check. Never `[skip ci]` a code change.

### AUD-07 — P1, performance (LUM-012). The projection does full-library work for no change. MEASURED

| Scenario | Wall | Statements | WAL | Peak RSS |
|---|---|---|---|---|
| Full build | 88 s | 402,014 | 324 MB | 1.5 GB |
| No change | 20–22 s | 9 | about 0 | 1.5–1.6 GB |
| 1-row delta | 57 s | 201,015 | 320 MB | 1.6 GB |

- Measured with PG16 locally on the synthetic fixture.
- A 1-row delta writes a whole new generation, doubling `analysis_items` on disk until vacuum. It also triggers a full relationship rebuild, because relationship inputs are keyed on the generation number (`relationship_build.py:116-137`).
- About 60% of CPU goes to `fingerprint()` → `canonical_json` → `_safe_payload`, the privacy sanitizer (`catalog.py:295-324`), applied to 402k internal dicts. That includes link fingerprints that are never stored.
- The projection also reads every payload JSONB, every chromaprint and about 196 MB of vectors, and writes row by row (`catalog_analysis.py:145-203, 306-340, 540-625`).

**Fixes:**
- Detect link changes by comparing tuples.
- Hash vectors in SQL with `encode(sha256(embedding),'hex')`. This gives the same values; measured 0.83 s and 8.8 MB.
- Read payloads and chromaprints only for groups with more than one occurrence.
- Copy unchanged rows with `INSERT…SELECT` (measured 1.0 s against 57 s), as `provider_identity_rekey._copy_analysis_generation` already does.
- Later, key the relationship rebuild on a digest of its inputs.

Gate all of this on an equivalence test: identical `analysis_changes` and state from the old and new projector.

### AUD-08 — P1, performance (LUM-011). Health and the settings poll run full-library aggregates on the 4-thread host. MEASURED

**`/api/catalog/health`:**
- p50 372 ms, p95 404 ms (budget 250 ms). 10 statements, including `_coverage` at 340 ms with a temp spill.
- It also runs an UPDATE plus a COMMIT on the host's request connection (`observe_provider_version(commit=True)`, `__init__.py:1899-1910`). That is a GET with a write and a transaction-ownership violation.
- While a provider transition is `applied`, p50 is **1.51 s**, because `refresh_audiomuse_health` runs on every GET (`provider_identity_guard.py:488-497`).
- Serving the two aggregates from stored values gives **5.3 ms p50**.

**`/settings/status`:**
- Polled every 5 s while work is active (`settings_ui.py:13`). p50 544 ms, p95 619 ms.
- It reaches the same readiness aggregates, plus `analysis_status_counts` (235 ms).
- One open admin tab uses about 11% of one of the 4 host threads, and churns about 250 MB of buffers every 5 s.

**Fix:**
- Store link counts and eligibility counts in the same transaction that publishes the projection or catalogue.
- Keep the live identity ping, but write an observation only when it changes.
- Move `refresh_audiomuse_health` to its existing 30-minute cron.
- Have the settings poll reuse the same summary.
- Test that summary-derived blockers equal live-query blockers.

### AUD-09 — P1, client. The original late-`preflight_required` incident is not fixed. HIGH

- LUM-009 moved admission earlier, but the foreground "favourites and ratings" phase still runs **after** the whole AudioMuse phase (`useForegroundSync.ts:476-525`).
- `pullProviderUserState` has no wait-and-retry. Every Subsonic call asserts admission synchronously (`subsonicProvider.ts:1725`).
- So any AudioMuse phase longer than 5 minutes ends as `failed` (`useForegroundSync.ts:522`), which is exactly the incident.

The plan was right to insist that this regression be kept (§4.9); the audit confirms it still reproduces by construction. The fix is the same as AUD-01: wrap provider user-state reads like `providerCatalogueAdmission`.

### AUD-10 — P1, product (LUM-005). Loudness is not integrated loudness. CONFIRMED

Measured with `probes/loudness_and_payload/lufs_probe.py` against the pyloudnorm BS.1770-4 reference:

| Signal | Analyzer | BS.1770 | Difference |
|---|---|---|---|
| Stereo pink noise, 44.1 / 48 kHz | −25.9 | −23.0 / −22.9 | −2.9 / −3.0 dB |
| Stereo pink noise, 96 / 192 kHz | −25.9 / −25.7 | −22.3 / −21.5 | −3.7 / −4.2 dB |
| Mono pink noise, 48 kHz | −25.0 | −25.1 | +0.04 dB |
| Stereo, 20 s loud + 40 s at −30 dB | −29.8 | −22.1 | −7.7 dB |
| Stereo 60 Hz tone, 48 / 96 kHz | −18.6 / −24.1 | −15.7 / −15.7 | −3.0 / −8.4 dB |

**Causes:**
1. **Channels are averaged, not summed** (`np.mean(window*window)` over channels, `loudness.py:239`). The error depends on channel count: mono and stereo differ by 3 dB.
2. **The 48 kHz K-weighting coefficients are used at every native rate** (`loudness.py:31-39, 91-92`; the resampler keeps `rate=sample_rate`). Hi-res and bass-heavy content is badly under-read.
3. **There is no −10 LU relative gate**, and the blocks are 100 ms without overlap instead of 400 ms with 75% overlap. Tracks with quiet passages are boosted by several dB.

**Fix:** a versioned analyzer v2 that resamples to 48 kHz for measurement (or derives coefficients per rate) and applies BS.1770-4 channel weights and both gates. Regenerate lazily, keyed on `analyzer_ver`. The edge analyzer already measures at a canonical 48 kHz. Transport "v2" is not analyzer "v2".

### AUD-11 — P1 cluster, LUM-010 server operability. CONFIRMED where marked

**Creation is serialized and fragile:**
- `create_session` captures the whole snapshot synchronously inside the HTTP request, under the **global** `pg_advisory_lock(110094, 10)` with a 5 s `lock_timeout` (`profile_bootstrap.py:36, 142`).
- Measured: 94k profiles without edges take 5.0 s, and write 68 MB of WAL and a 63 MB table per session. A second concurrent create, from any phone or source, gets **503 after 5.01 s** (CONFIRMED).

**Leaked sessions and lockouts:**
- The configured client uses `fastFail`: a 10 s timeout and no retry. If create takes longer, the client aborts while the server stores the session.
- Abandoned client sessions are never released (`profileBootstrapV2Repo.ts:211-226`). Leaked sessions hold one of 4 per-source slots for 60 minutes, giving **429**.
- After an epoch, rebind or core-server change, `release` returns 410 without deleting the row, and create counts unexpired stale rows. The source is locked out for up to 60 minutes (CONFIRMED). The repo test hides this with a manual `DELETE`.

**No fairness or rate limit:**
- Any authenticated role, including `user`, can hold all slots or loop create/release. Each create writes up to 128 MB.
- With `AUTH_ENABLED=false`, the transfer is anonymous and the health payload still says `"auth": "host_authenticated"`.

**Errors vanish:**
- `_connection` turns any psycopg2, Attribute, Type, OS or Value error raised **anywhere in the body** into 503 `from None`, and nothing is logged (`profile_bootstrap.py:156-157`, `__init__.py:2612`).
- One bad published row therefore makes v2 permanently 503 for that source, with no trace (CONFIRMED). This runs directly against LUM-021.

**No floor hold before the first catch-up.** More than 50k events between create and the first catch-up (for example AUD-03) gives a 410, and the whole snapshot is lost.

**Fix:**
- Per-source lock key.
- Capture as a worker task: 202, then poll.
- Log with an error class.
- Purge rows whose identity is stale during admission, and let release delete any row matching token and source.
- Rate-limit per caller.
- Hold the retention floor while a session is open.

See §3.3e for a simpler design.

### AUD-12 — P1, client. The 60-minute absolute session expiry fights slow or intermittent phones. HIGH

- There is no progress across a gap longer than 60 minutes (`profileBootstrapV2.ts:188-192`).
- The refresh marker hides the delta cursor until a full run completes (`publishedProfileRepo.ts:196-208`), so a phone that syncs in short bursts may never finish.
- Together with AUD-01, this makes durable resume ineffective for the case it was designed for.

---

## 3. The plan document

### 3.1 Status accuracy: plan claims against repository reality

| Plan claim | Reality at audit time |
|---|---|
| "No completion report, new implementation SHA or stock-host end-to-end PASS has yet been supplied" for the no-PR replacement (§1, §6.4, §8.1) | Plugin `48545bd` and client `4416a61` implement `source_scoped_v1`. `docs/remediation/evidence/lum010_no_host_qualification/QUALIFICATION.md` records a PASS on `f100684` and `ce74293`. The ledger and RESUME record verdict "NO AUDIO MUSE PR REQUIRED". |
| `LUM010_NO_HOST_PR_PLUGIN_SHA` / `_CLIENT_SHA` "have no values" (§8.1, §10) | `48545bd…` / `4416a61…` |
| "All relevant changes and qualification commits remain local: no push" (§1) | Every listed plugin checkpoint is an ancestor of `origin/main` (`4153207`). Auralscape `main` merged `codex/lum010-no-host-pr` (`32680b5`), along with Option G work. The ledger and RESUME repeat the same stale "no push" claim. |
| Latest plugin tally "567 passed, 3 failures" | 569 passed / 3 failed (the ledger, and reproduced by this audit on PG16) |
| Latest client tally "8,905 passed, 1 SQL-extraction failure, 3 skipped" | The ledger reports 8,908 passed / 6 skipped, with the extraction script fixed in `4416a61`. The full suite was not re-run here. |
| Upstream main `ce742938` | Now `8aa1639c`, 3 commits later. No change to `plugin/`, `config.py`, `app_auth.py`, `database.py`, `app.py`, `app_setup.py`, `docs/PLUGIN.md` or `deployment/`, so the compatibility claim still holds. |
| Section 14 launch prompt: "Remove the experimental AudioMuse core dependency from LUM-010" | Already done. Running it would duplicate migrations (for example a client schema 38) or re-open settled decisions. It must be rewritten. |
| "No release-version bump" (§13, §14) | Conflicts with the old-worker drain requirement (AUD-05) and keeps CI red (AUD-06). |

### 3.2 What the plan gets right

- It is disciplined about evidence: implemented, reviewed, bounded-qualified and release-qualified are kept distinct; test totals from different revisions are not added up; LUM IDs stay stable.
- Its re-adjudication of the host dependency is correct and well argued: separating authorization, data scope and transfer scope, and "usefulness is not necessity". The audit confirms the source claims on three host revisions.
- It correctly keeps LUM-005, the workbench, UI, timeouts, documentation and structure visible instead of letting LUM-010 swallow them.
- "Measure before optimising" and "no larger pages or new concurrency without evidence" are the right rules. The budgets in §9.3 are reasonable starting points.
- §4.9's insistence on keeping the *late* preflight regression was prescient: AUD-01 and AUD-09 are that regression, and it is still present.

### 3.3 Where the plan is wrong or incomplete

**a. It is stale on arrival.** The plan's central "next step" was completed and merged hours before its status date. A consolidation document must start from `git log` on every repository's `main`, not from pasted reports. §10.1 admits "This compilation did not run `git log`", and that is exactly the failure. Recommendation: generate the status table from git and CI, not by hand.

**b. It normalises a red build.**
- "567 passed, 3 failures" is carried as an acceptable steady state across more than ten checkpoints.
- A permanently red suite hides new failures in the same file and trains everyone to ignore CI. It ended in a `[skip ci]` push of about 2.6k production lines.
- The plan's answer ("eventually select a new release version") is right but should have been step zero. See AUD-06 for the minimal fix.

**c. It treats the old-worker drain as a procedure, not a mechanism.**
- The plan lists the drain as an open gate but never looks at the in-tree fence (worker version attestation), and forbids the version bump that would activate it.
- The failure mode is not "may bypass the protocol". It is **a permanent, installation-wide collection outage** (AUD-05).

**d. Qualification was not representative, and that is where the real defects are.**
- Every blocking defect found here depends on time or scale:
  - more than 5 minutes (AUD-01, AUD-09);
  - more than about 7k edge profiles (AUD-02);
  - more than 50k events (AUD-03, AUD-11);
  - a 94k-row publication (AUD-02, AUD-04).
- The stock-host qualification used a few synthetic tracks, werkzeug `run_simple` instead of gunicorn gthread ×4, PG17 instead of the deployed `postgres:15-alpine`, and a **mocked LUM-009 guard**.
- The plan does list "native/device" as open, but it never asks for a representative-scale run. It should be a standing gate, using the plan's own 94k fixture from §9.3.

**e. LUM-010: a simpler design was never considered.**
- The host detour came from choosing **server-side materialised snapshots under an owned REPEATABLE READ connection**. Account identity was not the root.
- The legacy `GET /api/profiles/bootstrap` (`catalog_enrichment.py:539`) was already a stateless keyset scan with a pinned head. `/api/profiles/changes` replays from that head.
- After LUM-001 (every publication emits a dense, full-payload event in the same transaction) and LUM-008 (the legacy route reads `published_source_profiles`), in-order idempotent replay of `(H0, H_end]` converges exactly. Durable client staging of `(after, H0, epoch)` then gives resume without any server session.
- What legacy lacked was a small **retention lease** that compaction respects, identity fields in the token, and a client-chosen finite head. The catalogue stream already has this pattern ("unless an unexpired bootstrap lease pins them", `catalog.py:100`).
- That design needs no owned connection, advisory lock, per-session materialisation (68 MB+ of WAL per create), byte caps, slot limits or HMAC session secrets, so the host question would never have arisen.
- **Recommendation.** Do not rip out the shipped v2 now. Fix its operability (AUD-11), stop copying edge JSON into per-session snapshots (AUD-02), and measure. If capture cost or operability stays a problem, collapse v2 onto the lease design in a later contract revision. The client staging and publication already fit either approach.

**f. Performance was deferred indefinitely although the user asked for it.** The plan makes performance wait for the no-PR correction, and implicitly for release gates. The measurements in §6 show the largest wins are small, local and testable by equivalence (AUD-04, AUD-07, AUD-08), and several fix correctness-adjacent problems (AUD-03). Performance should run in parallel with release hardening, not after it.

**g. The host's constraints are missing from the architecture analysis.**
- AudioMuse serves every UI and API request from **one gunicorn worker with 4 threads**, and opens a **new database connection per request** with no pool.
- That turns every slow synchronous plugin route into a host-wide availability problem: health polls, settings polls, snapshot capture, the FederatedAlbums artwork proxy with its 25 s upstream timeout. It should drive the priorities.

**h. Process overhead is out of proportion to product change.**
- Since 1.2.5 the plugin diff is:
  - plugins: +2,638 / −1,136
  - tests: +4,373 / −583
  - **docs: +5,620** (628 KB in `docs/remediation`)
- 8 of the 13 remediation commits are bookkeeping or evidence.
- A large share of the LUM-010 implementation was written, reviewed and then reverted (`9914d85`, `4b54792`, then `48545bd`).
- The plan and ledger spend significant space on model routing, self-hash rules and "do not embed a commit's hash into itself".
- Recommendation:
  - one short `STATUS.md` generated from git and CI;
  - PRs with required CI, instead of checkpoint and bookkeeping commit pairs;
  - move the historical ledger to an archive;
  - drop model-routing text from product repositories (for example `.codex/config.toml` and `LUMAE_ASTRA_MASTER_ORCHESTRATION.md`).

**i. Evidence standards were applied to the documents, not the tests.** Reviewers returned PASS on diffs while the tests mocked the guard that fails in production (AUD-01), used a hand-built nullable schema instead of `migrate()`, and, for LUM-001, would still pass with the key `FOR UPDATE` removed (1 of 39 tests fails). Test quality needs the same scrutiny as the prose.

---

## 4. Implementation verdict per finding

| ID | Plan status | Audit verdict | Key evidence or gaps |
|---|---|---|---|
| LUM-001 | Implemented, reviewed | **Sound, with gaps** | No in-tree bypass; consistent lock order. Gaps: the v1 `/changes` reader can silently skip an event when compaction commits between its two statements (CONFIRMED; the v2 catch-up checks density). Cutover is procedural only (AUD-05). Tests barely distinguish the stream-state lock. |
| LUM-002 | Implemented | **Correct** | CAS under the parent row lock; cross-collection ownership guarded for every mutation kind; principal-scoped everywhere. Provider rekey bypasses the protocol (see §5). |
| LUM-003 | Implemented | **Correct** | One transaction owner; length-framed canonical fingerprint; legacy receipts replay. Client edge cases: a reorder body rebuilt from live state gives 409 `idempotency_key_conflict` with no `current`, so the collection is stuck in conflict. |
| LUM-004 | Implemented | **Core invariant correct** (probe: 0 skips, gapless) | Open: old writer wedge (AUD-05); a 20k-item restore holds the global frontier for 9.85 s and other users' writes time out; no epoch, `cursor>head` or 410 signal to clients; no reconciliation path. |
| LUM-005 | Open | **Open, quantified** | AUD-10 |
| LUM-006 | Implemented | **Correct** | Exhaustive probe: 20,736 combinations, 0 disagreements; sentinel handled |
| LUM-007 | Implemented | **Partial** | Rows can be stranded with no retry and no wake after a stale transition with a prior category (CONFIRMED). Maintenance pause and batch aborts use up the 3-attempt budget (CONFIRMED). The release path always uses a 60 s cooldown. |
| LUM-008 (server) | Implemented | **Partial** | AUD-03 (no-op republish deletes edges). The seed copies 1.2.5-era orphan 'ready' rows and never withdraws them. The per-source `catalog_state` lock now gates the `/analyze` request and the hook while catalogue publication does 3 round trips per changed track (about 8 s at 20k). `edge_backfill_candidates` still reads attempt state. |
| LUM-008 (client) | Implemented | **Correct but costly** | Atomic, source-owned; the publication transaction regressed on memory and duration (AUD-02). |
| LUM-009 | Implemented | **Partial** | Early admission and fencing are correct; late expiry is not handled (AUD-01, AUD-09). |
| LUM-010 | Recorded PASS on stock host | **Protocol correct; not usable at scale** | Consistency, HMAC tokens and replay are sound. AUD-02, AUD-11, AUD-12; client resume defeated by AUD-01. |
| LUM-011 | Open | **Open, measured** | AUD-08 |
| LUM-012 | Open | **Open, measured** | AUD-07 |
| LUM-013 | Open | **Open, confirmed** | `selected_source … LIMIT 1`; items carry no catalogue; stream and art use `config.MEDIASERVER_TYPE` |
| LUM-014 | Open | **Open, confirmed by probe** | Same-name editions merged (26 tracks, `provider_album_id: None`); album count 7,700 against 8,000 real |
| LUM-015 | Open | **Open, confirmed** | Year is hard-coded `NULL::INTEGER` (`collection_library.py:41`); "newest year" sorts by title |
| LUM-016 | Open | **Open, measured** | Browse 0.3–0.6 s; `scope=all` search 2.34 s; `search_u` cannot be indexed; `COUNT(*) OVER()`; OFFSET |
| LUM-017 | Open | Open | Not reviewed beyond AUD-08 |
| LUM-018 | Open | Open | The analyzer deadline is checked only between decoded blocks, so a hung native decode is not interruptible |
| LUM-019 | Open | Open | The README still leads with 1.2.0 DJ text; `private-dist/` deploy scripts reference a private LAN IP; `runtime/README.md` is DJ-era |
| LUM-020 | Open | Open | §6.3 |
| LUM-021 | Partial | **Partial** | Query errors are redacted and timed, but state `last_error` text is shown raw (`database_state.py:777-784`); there is no per-query timeout; counts ignore published and retry states; zeros show on failure; the LUM-010 route logs nothing (AUD-11). |

---

## 5. Current state: repositories, CI, release and other findings

**Repository and CI:**
- `main` equals the remediation branch, and the designated working branch is at the same commit.
- There is no CI evidence after 1.2.5 (AUD-06).
- The full suite passes locally except the 3 release-identity tests. That is on PG16; CI pins PG17.
- A `compileall` check passes.

**Release channel:**
- The public catalogue still serves the immutable 1.2.5 archive, so catalogue users are not exposed to the unreleased source.
- Anyone installing from source gets remediated code that reports itself as `1.2.5` (AUD-05).
- The Auralscape `main` client negotiates v2 only when health advertises `source_scoped_v1`, so against 1.2.5 it uses legacy. AUD-01 and AUD-02 affect the legacy path too.

**Other P2 findings:**
- **Collections:**
  - Provider-identity rekey edits collection items outside the collection protocol: no parent lock, no revision bump, no feed event. It is not scoped to a catalogue, and it rewrites delivered history. A collision in one user's collection rolls back the **whole installation's** rekey (CONFIRMED).
  - The server silently remaps item ids (`_upsert_item`), and the client's unconditional unique indexes then make its feed apply fail forever. LUM-014 will make this common, so align the client indexes first.
  - Shelves have the LUM-004 late-commit bug through `rekey_shelves`, which calls `nextval` without the scope lock.
- **Client:**
  - The profile source key includes auth mode and account name, although the transfer is scoped to the source. Switching between password and bearer hides every profile until a full re-bootstrap, and old copies are never cleaned up.
  - The durable admission scope is an MD5 of the whole settings JSON, so unrelated settings changes abort or discard a resume.
- **Server:**
  - The v1 `/changes` race (see LUM-001).
  - Migration: 12 `ADD COLUMN IF NOT EXISTS` statements each take ACCESS EXCLUSIVE, even as no-ops. This happens on install or upgrade, queued behind old-worker transactions.

**P3:**
- Unauthenticated plugin paths get a 302 to `/login` instead of a 401.
- The 16 KB body limit can be bypassed with a chunked body.
- `expires_at` is non-UTC on a non-UTC server.
- `current_principal` fails open to GLOBAL for unknown auth methods.
- `..` is accepted as an item id.
- A collection UI label bug (`media_server` vs `provider_catalog`): the UI always claims track numbers are missing.
- `unaccent` is assumed but never created.
- Collection changes and receipts are never compacted.
- The FederatedAlbums artwork proxy blocks a host thread for up to 25 s.
- The client stores session tokens in plaintext SQLite. Risk is low: host authentication still applies and tokens are short-lived.

**Security posture.** No cross-user collection access was found; everything is principal-scoped. Profile data is shared per source, as the plan intends. The main exposures are availability issues (AUD-11) and anonymous transfers when `AUTH_ENABLED=false`, which should be documented as such.

---

## 6. Performance and architecture

### 6.1 Measured costs

Environment: PG16, local, 4 vCPU, synthetic fixture, warm cache. These are relative indicators, not production claims.

| Path | Now | Achievable | Source |
|---|---|---|---|
| Journal compaction per publication | 24 ms, under locks | 0.09 ms | AUD-04 |
| `/api/catalog/health` p50 | 372 ms (1.51 s during a transition) | about 5 ms plus the ping | AUD-08 |
| `/settings/status`, every 5 s | 544 ms | reuse the summary | AUD-08 |
| Projection, no change | 20–22 s, 1.5 GB | 2–4 s, <400 MB (estimate) | AUD-07 |
| Projection, 1-row delta | 57 s, 320 MB WAL | 3–5 s (estimate) | AUD-07 |
| v2 create at 94k, no edges | 5.0 s in-request, 68 MB WAL | off-thread or lease | AUD-11 |
| First-load payload at 94k (edges needed offline) | 1.82 GB uncompressed JSON, repeated whenever a re-bootstrap is forced | 0.89 GB gzip (0.77 GB compact), once | AUD-02 |
| Workbench browse / search `all` | 0.3–0.6 s / 2.34 s | indexed keyset | LUM-016 |
| Collection frontier ceiling | about 0.7–1.2k tx/s | fine at household scale | LUM-004 |

### 6.2 Where the time actually goes, end to end

For the phone, the dominant costs are data volume and *repeated* full bootstraps, not server query latency:
- Edges are about 40× the base payload. They are needed offline, so the first load is inherently large.
- It should happen once, compressed (about 0.9 GB instead of 1.8 GB at 94k), written page by page.
- It currently repeats because of AUD-03 journal floods, AUD-01 aborts and AUD-12 expiry.

Fixing AUD-01, 02 and 03 should remove most of the original 20-minute sync before any micro-optimisation. After that, steady-state deltas are small, especially once unchanged edges are referenced by digest instead of re-sent. On the server, contention on the 4 host threads (AUD-08, AUD-11) matters more than any single query.

### 6.3 Architecture (LUM-020)

`__init__.py` has 5,988 lines and 159 top-level definitions:

| Lines | Responsibility |
|---|---|
| 1-468 | Constants and helpers |
| 469-1090 | Cron tasks |
| 1110-1379 | Schema |
| 1380-1752 | Profile rows |
| 1753-2032 | Health |
| 2034-2907 | HTTP APIs |
| 2958-3369 | Analysis runs |
| 3370-4085 | Backfill |
| 4086-4526 | Preparation |
| 4527-5930 | Settings HTML (about 1,400 lines) |

**Transaction ownership is spread out:**
- `commit()` call sites: 23 in `__init__.py`, 10 each in `catalog.py`, `reconcile.py` and `relationship_build.py`, and 11 in `credits_store.py`.
- Several modules use `commit=` flags.
- A GET route commits the host's request transaction.

**Duplicated SQL:**
- the 40-column `resolve_catalog_source` SELECT, three times;
- the implicit "selected source" CTE, four times (tied to LUM-013);
- the link SELECT and the generation copy, twice each.

**Constraints on moving code:**
- Queued jobs and cron rows store dotted task paths (`func.__module__.__name__`).
- Flask endpoint names come from function names.
- Tests patch 264 package-level attributes, 70 of them `get_db`.

**Extraction order** (each step behaviour-preserving, no SQL changes mixed in):
1. `settings_render.py`: pure renderers that take DTOs.
2. `status_model.py`: one read model for health, settings and database-state, where the LUM-011 summary lands.
3. One source-query builder, after LUM-013.
4. `preparation.py`, `profile_backfill.py` and `analysis_runs.py`, with thin task shims left in `__init__`.
5. Route modules registering on the same `bp`.
6. `schema.py`, re-exported from `__init__`.

**Transaction convention to adopt:** routes and tasks own the transaction; repository functions take a cursor and never commit; GET routes are read-only; retire the `commit=` flags.

### 6.4 Measurement plan

1. Move `probes/performance/*` into an opt-in `scripts/perf/` driven by `LUMAE_PERF_DSN`. It already has the host stub, a representative seeder, a statement-counting cursor and WAL deltas.
2. Baseline before any fix:
   - health and settings p50/p95 and statement counts;
   - projection full, no-change and delta: wall time, RSS in a fresh process, WAL;
   - the publication critical section;
   - `create_session` at 10k and 94k, with and without edges, and with a concurrent creator;
   - workbench EXPLAIN plans.
3. Budgets:
   - health ≤50 ms p95 excluding the ping;
   - settings poll ≤100 ms;
   - projection no-change ≤5 s and ≤400 MB;
   - publication critical section ≤5 ms;
   - bootstrap page ≤50 ms server-side;
   - no capture on a request thread.
4. Method: 3–5 repeats, report the median, report warm and cold separately, and record PG configuration and hardware. Every optimisation needs an equivalence test alongside the LUM-001/004/006/007/008/010 regressions.
5. Test for thread starvation with a 4-thread concurrent load against a real AudioMuse container.

---

## 7. Recommended plan, re-sequenced

**Phase 1 — stop the bleeding (days; each item is small and independently testable):**
1. Client: fix the identity check inside a run and add wait-and-retry for provider user-state (AUD-01, AUD-09). Add the >5-minute regression test.
2. Keep the bulk edge copy for offline playback, but make the first load feasible and one-time:
   - gzip responses;
   - no edge JSON in v2 session snapshots;
   - byte-sized pages;
   - page-by-page, content-addressed edge writes on the client;
   - reference unchanged edges by digest in events;
   - 413/429/503 fallback and backoff;
   - release abandoned sessions.

   (AUD-02, part of AUD-11.)
3. Fix the precision of the no-op comparison and keep edges when the media is unchanged (AUD-03).
4. Replace the journal compaction with a range delete (AUD-04).
5. Make CI green by construction, protect `main`, and stop using `[skip ci]` on code (AUD-06).
6. Drop the collection seq default, bump `PLUGIN_VERSION`, attest every writer, and add an invariant check (AUD-05).

**Phase 2 — performance (1–2 weeks, in parallel with release hardening):**

7. LUM-011 committed summary; read-only GET routes (AUD-08).
8. LUM-012 projection fixes behind an equivalence test (AUD-07).
9. LUM-010 operability, or the collapse to a lease design (AUD-11, AUD-12, §3.3e).
10. A representative-scale end-to-end gate: 94k profiles with edges, a run longer than 5 minutes, real admission, gunicorn gthread ×4, PG15/17.

**Phase 3 — semantics and product:**

11. LUM-005 analyzer v2 (BS.1770-4), versioned with lazy regeneration (AUD-10).
12. Workbench LUM-013 → 014 → 015 → 016. Align the client unique indexes **before** LUM-014.
13. Collections: epoch, 410 and floor signalling; chunked or capped restores; rekey through the collection protocol; shelves rekey under the scope lock.
14. LUM-007 retry gaps; LUM-017, 018, 019 and 021 closure; LUM-020 extraction.

**Release:** after Phases 1 and 2, cut **1.3.0** as a new immutable archive. Qualify it with the representative gate, a populated 1.2.5 upgrade rehearsal (the old-writer fence must be proven to fail closed), and the client fallback matrix.

---

## Appendix A — What was executed

**Plugin suite:**
- Command: `LUMAE_POSTGRES_TEST_DSN=… python -m pytest tests/plugins -q` on a disposable PG16.
- Result: **569 passed, 3 failed** (`test_release_channels.py`: `test_current_release_contains_supported_source_only`, and `test_release_archive_is_identical_across_platforms[win32|linux]`), in 50.8 s.
- `python -m compileall -q plugins scripts`: OK.
- `python scripts/build_catalog.py --check`: `ValueError … published versions are immutable`.

**Focused plugin suites (reviewers):**
- `test_profile_bootstrap_postgres.py`: 22 passed.
- publication, retry, stream-serialization, migration and diagnostics: 73 passed.
- feed, mutations and library integration: 70 passed.
- shelves and personal-discovery: 63 passed.

**Client (scratch copy, `npm ci`):**
- `profileBootstrapV2`, `publishedProfileRepo`, `sonicDbUpgrade` and `pluginEnrichmentSync`: 72 tests passed.
- The stock integration suite was skipped: it needs its external environment.
- `ttlGuard.test.ts` reproduction: passes, which means the defect is present.

**Host:**
- `git diff ce742938 8aa1639c` over `plugin/`, `config.py`, `app_auth.py`, `database.py`, `app.py`, `app_setup.py`, `docs/PLUGIN.md` and `deployment/`: empty.
- Production web tier: `deployment/supervisord.conf:19` (`--workers 1 --threads 4 --timeout 300`).

**GitHub:**
- The last workflow run is #46, on `4276398`.
- The only branches are `main` and `codex/test-lumae-analysis-1.1.8`.
- `main` is unprotected.

## Appendix B — Probe index

See [`probes/README.md`](probes/README.md). Each probe asserts the defect, so a *passing* probe means the defect is present.

## Appendix C — Host-boundary claims in QUALIFICATION.md and RESUME.md

**Verified:**
- `config` is exported by `plugin.api` on all three revisions.
- `DATABASE_URL` is derived from `POSTGRES_*`. The environment override is honoured on `f100684` and ignored on `ce74293` and `main`.
- The default `search_path` and the unqualified `table()` names resolve the same tables.
- Bearer and cookie requests both pass host authentication before plugin routes; invalid credentials are rejected.
- Non-admin accounts can continue a shared session.
- A token for another source gets 410.
- Closing the owned connection releases its backend and the advisory lock.
- The request connection is never touched.

**Needs qualification:**
- "A request first passes stock AudioMuse authentication" holds only with `AUTH_ENABLED=true`.
- "Bounded" admission does not prevent the stale-row lockout or slot squatting.
- "Always rolls back" is true, but a failing rollback masks the original error, and nothing is logged.

**Not verifiable here:**
- the runtime restart, rotation and client-interruption matrix;
- the `postgres:15-alpine` container topology;
- the harness itself, which needs a sibling Windows checkout, a hand-started PG17 on :55273 and Redis, and ignored runtime files, with no phase runbook.
