# Lumae — final remediation and performance plan (post-audit, 2026-09-24)

## Context

On 24 September 2026 an audit checked the remediation overview, the Lumae plugin (`rendyhd/lumae-plugin`), the Auralscape client (`rendyhd/Auralscape`) and the AudioMuse-AI host. The report is `docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md` on branch `claude/epic-davinci-q0gj48`, and the probes that reproduce each finding are in `docs/audit/2026-09-24/probes/`.

What the audit found:
- The old plan was stale: the no-AudioMuse-PR work is already merged.
- Server correctness work (LUM-001/002/003/004/006) is largely sound.
- The end-to-end sync still fails at real scale (94k profiles with edge profiles):
  - a 5-minute identity check aborts long syncs and deletes their progress;
  - v2 bootstrap refuses more than about 7k edge-profiled tracks;
  - the client's legacy publication loads every edge into memory at once;
  - no-op re-analysis deletes edges and floods the journal.
- An old worker left running can wedge collection writes installation-wide.
- `main` has never passed CI since the remediation.
- No performance work has shipped.
- Loudness is 3–8 dB off BS.1770.

This plan replaces Sections 12–14 of the old overview. It covers **every** audit finding (see the traceability matrix in §7). It is written for a **Claude Code orchestrator with subagents**. **§H is the complete hand-off for the Lumae app (Auralscape) agent.**

## Decisions (user, 2026-09-24)

| Topic | Decision |
|---|---|
| LUM-010 | Harden the current `source_scoped_v1` v2. No redesign. |
| Release flow | Pin `main` to the 1.2.5 archive so CI is green. Develop through PRs with required CI. Cut **1.3.0** as a new immutable archive after Phases 1–2. |
| Loudness (LUM-005) | Analyzer v2 (BS.1770-4) versioned beside v1. Lazy, low-priority background regeneration. The client accepts both. |
| Offline | The phone keeps a full copy of every waveform **and edge** profile, because many AudioMuse servers are LAN-only. Make the first load feasible, compressed and one-time. Deltas stay small. The on-demand warmup remains only as a gap filler. |
| Orchestrator | Claude Code |

---

## 1. How to execute this plan (Claude Code)

### 1.1 Roles

| Role | Where | How |
|---|---|---|
| **Orchestrator** | Main Claude Code session in `rendyhd/lumae-plugin` | Owns the task list (TaskCreate, one task per work package), sequencing, merges, `docs/STATUS.md`, and cross-repo contract decisions. Writes no production code itself. |
| **Mapper** (optional) | `Agent` with `subagent_type: Explore` | Read-only mapping before a large work package. Give it the WP's file list from this plan. |
| **Implementer** | `Agent` with `subagent_type: general-purpose`, `isolation: "worktree"`, background | One work package per agent, on branch `wp/<ID>-<slug>` from latest `main`. Follows §1.2. |
| **Reviewer** | A fresh `Agent` (`general-purpose`, told it is read-only), or the `/code-review high` skill on the WP branch | Checks correctness, the WP's acceptance criteria, **test sensitivity** (reverting the fix must fail a test) and contract compatibility. PASS or CHANGES_REQUIRED, with file:line. |
| **Lumae app agent** | A separate Claude Code session in `rendyhd/Auralscape` | Executes §H. It coordinates only through the contract doc and capability flags; there is no shared branch. |

**Parallelism:**
- At most **2 implementers in parallel**, and only on WPs whose files do not overlap (§1.3).
- A new orchestrator can resume from `docs/STATUS.md` and the task list.
- If the user explicitly says "use a workflow", a whole phase may run as a Workflow script (implement → review → fix loop per WP). Otherwise use individual `Agent` calls.

### 1.2 Definition of done for every work package

1. **Red first.** Write a failing test, or invert the matching audit probe from `docs/audit/2026-09-24/probes/`. Record the failing output in the PR.
2. **Implement the smallest change** that satisfies the WP. No drive-by refactors; LUM-020 extraction is its own WP.
3. **Checks:**
   - `python -m compileall -q plugins scripts`
   - the focused tests
   - the full `python -m pytest tests/plugins -q` on a disposable PG, which must have **0 failures** after P0-1
   - `python scripts/build_catalog.py --check`
   - `git diff --check`
4. **Review loop.** A reviewer agent runs. Fix CHANGES_REQUIRED and re-review until PASS. At most 3 rounds, then escalate to the user.
5. **Performance WPs** must add before/after numbers from `scripts/perf/` (P0-3), plus an equivalence test showing identical outputs.
6. **Contract WPs** update `docs/contracts/LUMAE_SYNC_CONTRACT.md` (P0-4) in the same PR and keep old clients working.
7. **Integration: one PR per phase** (changed 2026-09-24 at the user's request, to cut PR noise for repo watchers).
   - Implementers push an internal `wp/<ID>-<slug>` branch. No PR is opened for it.
   - After review PASS, the orchestrator merges the WP into the phase branch (`phase/<n>-<slug>`) with `git merge --no-ff`, so each WP stays one identifiable unit. Commit messages are `fix|perf|feat(<area>): <WP-ID> — <title>`, ending with the session attribution lines.
   - Each phase has **one** PR to `main`. It is opened as a draft when the phase's first WP lands, CI runs on every push, and it is marked ready and merged (merge commit, not squash) only when the phase exit gate passes. `main` is pinned (P0-1), so merges never publish.
   - Never use `[skip ci]` on code, and never force-push `main`.
8. **Status.** Update the task, and add one line to `docs/STATUS.md` (WP, phase PR, WP merge SHA, tests). No separate bookkeeping commits.

### 1.3 File-conflict groups (serialize within a group)

| Group | Files | WPs |
|---|---|---|
| A | `plugins/LumaeAnalysis/__init__.py` | P1-3, P1-4, P1-6 (routes), P1-8, P2-1, P2-4, P3-4 (hook), P3-6, P3-8, P3-9, P3-10, P3-11 |
| B | `profile_bootstrap.py` | P1-5, P1-6, P3-2 |
| C | `catalog_enrichment.py`, `catalog.py` (stream) | P1-2, P1-3, P1-7, P3-2 |
| D | `profile_publication.py`, `edge_profile_store.py` | P1-1, P3-6, P3-7 |
| E | `collection_manager.py`, `shelves.py`, `provider_identity_rekey.py` | P1-3, P3-4 |
| F | `catalog_analysis.py`, `catalog_readiness.py`, `provider_identity_guard.py` | P2-1, P2-2 |
| G | `loudness.py` (+ new module) | P3-1 |
| H | `collection_library.py`, `collection_ui.py` | P3-5 |

Where different groups touch one file, keep each WP to the functions listed and rebase before merge.

### 1.4 Environment (cloud container or local)

- **PostgreSQL:**
  - Use `/usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/<name>` and start it on a free port.
  - **Never put data directories under `/tmp/claude-*`**: its permissions are reset mid-session, which PANICs the server.
  - Create role `lumae_test` and database `lumae_test`, then export `LUMAE_POSTGRES_TEST_DSN=postgresql://lumae_test:<pw>@127.0.0.1:<port>/lumae_test`.
  - CI uses PG17, so run version-sensitive checks there.
- **Python:** `pip install --ignore-installed blinker -r requirements-dev.txt -r requirements-edge.txt`. P3-1 also needs `pyloudnorm` as a dev dependency.
- Each implementer uses its **own** database (`createdb wp_<id>`) so parallel runs don't collide.

### 1.5 Stop conditions and user-only steps

**Stop and ask the user when:**
- anything would publish a release (a source-mode version bump pushed to `main`);
- any action touches production data, a real server or a device;
- a contract question is not settled here;
- a review is still unresolved after 3 rounds;
- CI is red on `main` for reasons outside the WP.

**User-only steps:**
- **U1.** Enable branch protection on `main`: required check "test", no force push.
- **U2.** Decide whether to delete `private-dist/` deploy scripts (they contain a LAN IP) and the Codex routing files (`.codex/config.toml`, `docs/remediation/LUMAE_ASTRA_MASTER_ORCHESTRATION.md`).
- **U3.** Provide or approve a 200–500 track real-library sample for the LUM-005 mapping spike (P3-1a).
- **U4.** Run the device and native checks from the release gate.
- **U5.** Approve the 1.3.0 publication (P4-1) and perform the server rollout (P4-3).

---

## 2. Shared contract changes (plugin ↔ client)

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

**Contract findings from P0-4 (2026-09-24), folded into the WPs below:**

| # | Finding in 1.2.5 | Handled by |
|---|---|---|
| 1 | `lumae_analysis_profiles` is only in `plugin.json`, not in the health payload, so the K11 gate needs a new health key | P3-1 adds `capabilities.lumae_analysis_profiles` to health |
| 2 | `capabilities.transport` and `capabilities.profile_stream` don't exist | P1-4 and P3-2 add them as new objects |
| 3 | v2 never returns 404 or 409; 404 only means an older plugin without the route | Contract; client treats a v2 404 as "unavailable" |
| 4 | v2 expiry is a hard-coded 60 minutes in SQL; `SESSION_MINUTES` is unused | P1-6 uses one constant for absolute and sliding expiry |
| 5 | Releasing an expired or stale session returns 410 and leaves the row, which holds a slot | P1-6 item 2 (release always deletes and returns 200) |
| 6 | Health `available` is `bool(DATABASE_URL)`; `auth` says host_authenticated even when auth is off | P1-6 item 4 (K4) |
| 7 | `/profiles/changes` cursor ahead of head returns 400 `invalid_cursor`; the collections feed returns an empty 200 and never 410 | Kept for v1; P3-4 (K8) adds 410 for collections |
| 8 | `ref_lufs` is float64 in change events but float4 in reads and snapshots; only events pass the string sanitizer | P1-1 normalises to float4 and applies one serializer on every path |
| 9 | Per-publication retention is a fixed 50k; maintenance keeps max(1000, 2×count) | P1-2 (one persisted retention limit, at least 50k) |
| 10 | `/api/profiles` silently drops ids past 500 without listing them in `missing` | P3-2 lists truncated ids in `missing` (additive); client batches ≤100 (C-10) |
| 11 | Timestamps mix no-zone and server-offset formats | P1-6 item 8 (UTC with `Z` for new fields; `analyzed_at` unchanged, documented) |
| 12 | Collections: create with an existing id returns 201; duplicate membership remaps; no `current`; epoch not returned; journal and receipts never compacted | P3-4 (K8, K9, growth) |
| 13 | Shelves idempotency is keyed by mutation id only | P3-4 item 5 (bind the body fingerprint, K9-gated) |

1.2.5 ignores unknown body fields, query parameters and headers, so every opt-in signal is safe to send to old servers. The exception is `page_size` on v2 page, catch-up and release requests, which returns 400: clients send `page_size` only on create.

**Unchanged by design:**
- v2 `page_size` stays client-chosen (1–500). Byte-sized pages are a client choice: use `page_size` about 50 when edges are present.
- The legacy `limit` and `/changes` `limit` also stay client-chosen.

---

## 3. Work packages

Each WP lists the findings it covers, the files, the change, what to reuse, its tests, and when it is done. "Probe" refers to `docs/audit/2026-09-24/probes/`.

### Phase 0 — Foundation (plugin repo; do first, mostly serial)

**P0-1 — Make CI meaningful again (AUD-06).**
- Findings: `main` unvalidated since remediation; permanently red release tests; `[skip ci]` push.
- Files: `release-sources.json`, `tests/plugins/test_release_channels.py`, `.github/workflows/build.yml` (no functional change expected).
- Change:
  - Set LumaeAnalysis to `{"mode":"pinned-artifact","version":"1.2.5","checksum":<plugin.json versions[0].checksum>}`.
  - In `test_current_release_contains_supported_source_only` and `test_release_archive_is_identical_across_platforms`, assert "working source == archive" **only when the policy mode is `source`**. In pinned mode they assert the archive checksum and the immutability of 1.2.5 and 1.2.4 instead.
  - Add a test that pinned mode passes `build_catalog(check=True)` with modified source.
- Done:
  - full suite 0 failures (currently 569 passed, 3 failed);
  - `scripts/build_catalog.py --check` exits 0;
  - a PR CI run is green;
  - U1 requested from the user.

**P0-2 — Test fixture on the production schema (audit §3.3i).**
- Files: new `tests/plugins/conftest.py`.
- Change:
  - Add a `migrated_db` fixture: per-test schema, then the real `mod.migrate(db)` with scheduling stubbed, using the pattern from `test_published_profile_migration_postgres.py:_setup` (lines 12-66).
  - Add a `second_connection` helper.
  - Rule: every new PostgreSQL test in this plan uses `migrated_db` instead of hand-built tables.
- Done: the fixture is used by at least one test, and the documentation is in the conftest docstring.

**P0-3 — Performance harness and baseline.**
- Files: new `scripts/perf/` (moved from `probes/performance/`: `seed.py`, `stub_host.py`, `proj_bench.py`, `route_bench.py`, `boot_bench.py`, `boot_concurrency.py`, `feed_contention.py`, `explain_*.sql`), plus `scripts/perf/README.md` and `run_baseline.py`.
- Change:
  - DSN from `LUMAE_PERF_DSN`.
  - The seeder produces the representative fixture: 132k tracks, 69k items, 132k links, 94k profiles, **real-size edge payloads** from `probes/lum010/edge.json`, and 50k events.
  - `run_baseline.py` writes JSON. Record the baseline in `docs/perf/BASELINE-<date>.md` with the environment.
- Budgets (asserted by `run_baseline.py --check` in later WPs):

| Measure | Budget |
|---|---|
| Health, excluding the provider ping | ≤50 ms p95 |
| `/settings/status` | ≤100 ms p95 |
| Publication critical section | ≤5 ms |
| Projection, no change | ≤5 s and ≤400 MB RSS |
| Projection, 1-row delta | ≤10 s |
| v2 create at 94k | ≤5 s, with no global lock held longer than 50 ms |
| Bootstrap page | ≤50 ms server-side |

**P0-4 — Contract document.**
- File: new `docs/contracts/LUMAE_SYNC_CONTRACT.md`.
- Content:
  - the current contract per endpoint (health capabilities, `/api/profiles`, `/profiles/bootstrap`, `/profiles/changes`, the v2 create/page/catchup/release routes, `/profiles/edges/analyze`, `/collections/changes` and the mutations);
  - status-code semantics;
  - K1–K11 with their gates and compatibility rules;
  - the edge payload, digest and `boundaries` derivation rule (`edge_profiles.py:146-152`; the client's `edgeProfile.ts` `validWindow`).
- Every contract WP edits this file.

**P0-5 — Status and documentation hygiene (process, LUM-019 part 1).**
- Add `docs/STATUS.md`: one table of WP, state, PR and merge SHA, plus the current test and CI state. This replaces ledger and RESUME bookkeeping.
- Add a "historical record — see docs/STATUS.md" banner to `docs/remediation/RESUME.md` and `LUMAE_REMEDIATION_LEDGER.md`. Do not rewrite their history.
- Correct the audit report's AUD-02 fix item 5: a waveform republish does *not* embed the edge. It deletes it server-side, and clients delete their copy because an upsert without an edge removes it.
- U2 follow-up if the user agrees.

### Phase 1 — Stop the bleeding

Plugin WPs are below. The client runs §H Phase 1 **in parallel**, because it has no server dependency except K3/K5, which are opt-in.

**P1-1 — No-op re-analysis must not republish or delete edges (AUD-03).**
- Files: `profile_publication.py` (`complete_attempt`, around 236-385), `__init__.py` (`analyze_song_hook` around 3162-3200).
- Change:
  1. Normalise `ref_lufs` to float4 (`float(numpy.float32(x))`) before both comparison and insert, so delta and bootstrap payloads agree.
  2. Compare the previous row at stored precision.
  3. Delete `edge_profiles` and `edge_profile_jobs` **only if `media_signature` changed**. When only the waveform changed with the same media, keep the edge and emit the upsert **with the current edge embedded**: `serialize_profile(..., edge_profile=<current edge via edge_join>)`. Without it, current clients would delete their edge.
  4. In `analyze_song_hook`, skip `mark_pending` when the published row is current (same media fingerprint and analyzer version, and not failed), preserving LUM-007 requalification.
- Tests (`migrated_db`):
  - an identical completion leaves the head and edge rows unchanged;
  - a waveform change on the same media emits one event with the edge and keeps the edge rows;
  - a media change deletes the edge and schedules an upgrade;
  - the integrity probe case "no-op republish" now passes inverted.
- Done: the tests above pass and the integrity-probe F1 case inverts.

**P1-2 — Journal compaction, retention and floor hold (AUD-04, AUD-03 amplification, part of AUD-11).**
- Files: `catalog.py` (`compact_change_journal`, 58-96), `catalog_enrichment.py` (`record_profile_change` 460-502, `compact_enrichment_storage` 158-210, constant at line 50).
- Change:
  1. Per publication, delete only `WHERE catalog_instance_id=%s AND epoch=%s AND seq<=%s`, which is an index range delete. Purge other epochs only on epoch rotation and in `compact_enrichment_storage`.
  2. Retention per source = `change_journal_retention_limit(published_count)`: at least 2× the library, minimum 50k, as `compact_enrichment_storage` already computes.
     - Persist it as `profile_stream_state.retention_limit`, refreshed by `compact_enrichment_storage`, so there is no `count(*)` per publication.
     - Replace the fixed `PROFILE_CHANGE_RETENTION_EVENTS`.
  3. Floor hold: `target_floor = min(target_floor, MIN(snapshot_seq) of unexpired v2 sessions for the source without a captured head)`, capped at 4× retention so the journal cannot grow without bound.
  4. Apply fix (1) to every caller of `compact_change_journal`, including the catalogue stream.
- Tests:
  - the existing LUM-001 floor and compaction tests stay green;
  - new: after a 94k-event pass, a client cursor from before the pass is still readable;
  - the floor hold keeps an open session's `snapshot_seq` readable;
  - the statement plan uses the primary-key index (assert via `EXPLAIN` in the test).
- Performance: publication critical section ≤5 ms at 50k retained events (P0-3).
- **Decision (2026-09-24, orchestrator):** measured after P1-1 + P1-2, the critical section is 8.2 ms at p95 (it was 20.9 ms). About 1.9 ms of that is the edge that P1-1 now keeps and embeds (≈16 KB per event), and about 3.5 ms is 17 statement round-trips. Phase 1 accepts this. The ≤5 ms budget is re-checked after P3-2 (K6 edge references). If it still misses then, collapse the stream-state and compaction statements into CTEs (≈1 ms) and merge the two `source_profiles` updates (≈0.4 ms). No-op re-analysis, the common case, no longer publishes at all.
- The `pub_bench.py` `compaction_delete_ms` figure still times the old OR delete. Update it together with the P3-2 re-check.

**P1-3 — Fail-closed fences against old workers; version 1.3.0 (AUD-05).**
- Files:
  - `__init__.py` (`PLUGIN_VERSION` line 117, health, and new `integrity` checks);
  - `collection_manager.py` (`migrate_collections` 91-175, `_mutation_response` 546-605);
  - `catalog_enrichment.py` (`migrate_enrichment`, `record_profile_change`);
  - tests that read `RELEASE_VERSION` (`test_lumae_analysis.py:39`).
- Change:
  1. `PLUGIN_VERSION = "1.3.0"`. `release-sources.json` stays pinned at 1.2.5 until P4-1.
     - Tests: health asserts `PLUGIN_VERSION`; catalogue asserts `RELEASE_VERSION`.
     - The existing preparation attestation (`assert_preparation_worker_current` at 4215, migrate retarget at 1339-1353) then fences old preparation workers.
  2. Collections: `ALTER TABLE <collection_changes> ALTER COLUMN seq DROP DEFAULT`, idempotently. An old 1.2.5 writer inserting without `seq` then fails instead of wedging the frontier.
  3. Profiles: add `profile_changes.writer_generation SMALLINT NOT NULL DEFAULT 2`, then `DROP DEFAULT`. `record_profile_change` inserts 2. An old writer's insert, which omits the column, fails and rolls back its whole transaction.
     - **Verify with the 1.2.5 archive code** that its publication and journal share one transaction, so the source row rolls back too. If not, add the equivalent NOT NULL fence on the table it writes.
  4. Invariant checks, exposed in health `integrity:{collections_feed_ok, profiles_unpublished_ready}` and logged at startup:
     - `MAX(collection_changes.seq) <= head_seq`. If violated, collection writes return 503 `collection_feed_invariant`, and the documented repair SQL (`setval`/head realignment) is in the runbook.
     - The count of `source_profiles` rows that are 'ready' and current but have no `published_source_profiles` row.
  5. Write `docs/runbooks/UPGRADE_1.3.md`: stop web and RQ workers, back up, install, run migrate, start, verify `integrity`, and roll back by forward-fix.
- Tests:
  - the legacy-writer probe (a raw 1.2.5-style insert) now fails, and new writes continue;
  - an old-style `profile_changes` insert fails;
  - the invariant flags an injected violation;
  - the version tests are split.
- Done: the collections probe `test_probe_legacy_writer.py` inverts.

**P1-4 — Transport compression and private headers (AUD-02, K1).**
- Files: `__init__.py` (`_private_json` 2041, `_catalog_error` 2050, `profiles()` 2507-2551, blueprint registration).
- Change:
  - Add a blueprint `after_request` that gzips (level 6) when all of these hold: the request's `Accept-Encoding` contains gzip, the response is `application/json`, the body is ≥1 KiB, the status is 200, and there is no existing `Content-Encoding`. Set `Content-Encoding`, `Vary: Accept-Encoding` (merged with existing values) and `Content-Length`.
  - `profiles()` uses `_private_json`, so it gets private cache headers.
  - Health: `capabilities.transport:{gzip:true}`.
- Tests: gzipped and plain responses round-trip; headers are correct; v2 pages, `/changes`, `/bootstrap` and `/api/profiles` are compressed; small responses are not.
- Measurement: bytes per 50-row page with real edges, before and after (expect about 2×).

**P1-5 — v2 snapshots without edge copies (AUD-02, K2).**
- Files: `profile_bootstrap.py` (`create_session` 220-289, `snapshot_page` 292, `catchup_page` 315), the snapshot and catch-up DDL in `catalog_enrichment.py` (216-316).
- Change:
  1. The capture stores the waveform payload plus `edge_ref(media_revision, profile_digest)` from `edge_join()`. `MAX_SNAPSHOT_BYTES` counts waveform bytes only, and each row is serialized once.
  2. `snapshot_page` joins `edge_profiles` on `(catalog_instance_id, track_id, media_revision, profile_digest)`. If the reference is present, it embeds the edge; if the reference is gone (replaced or withdrawn after capture), the row is returned without `edge_profile`. The catch-up interval contains the replacing event.
  3. Catch-up capture stores event payloads the same way, as a waveform part plus an edge reference. `MAX_CATCHUP_BYTES` excludes edges, and `MAX_CATCHUP_EVENTS` is raised to the retention limit from P1-2.
  4. Additive migration: new column `edge_ref JSONB`; existing sessions are left as they are.
- Tests:
  - create for 10k profiles with real 19 KB edges succeeds (previously 413);
  - pages embed the edges;
  - an edge replaced after capture is absent in the snapshot and present in the catch-up;
  - the byte cap ignores edges;
  - LUM-010 replay tests stay green;
  - the lum010 probe's 413 case inverts.
- Performance: create at 94k with edges ≤5 s and ≤80 MB of WAL (P0-3).

**P1-6 — v2 operability (AUD-11, AUD-12 server side, K3/K4/K5, LUM-010 P3 items).**
- Files: `profile_bootstrap.py`, `__init__.py` (`_profile_bootstrap_v2` 2600-2613, health 1837-1843), bootstrap DDL.
- Change:
  1. **Locking:**
     - Admission runs in a short transaction under `pg_advisory_xact_lock(110094,10)`: purge expired **and identity-stale** rows (core server, epochs, inactive source); count slots; insert a session in state `capturing`.
     - Capture then runs under a **per-source** `pg_advisory_lock(110094, hashtext(source))`.
     - `capturing` rows older than 10 minutes are purged.
  2. **Release** deletes any row matching token hash and source, even if identity-stale, and always returns 200 `released`.
  3. **K5:** optional `client_request_id` (UUID). An unexpired session with the same (source, id) and `pages_served=0` is deleted before a new one is created. New columns `client_request_id` and `pages_served`.
  4. **K4:** `Retry-After` on 429 (seconds until the earliest slot expires, capped at 300) and on 503 (5).
     - Health: `available` = tables migrated **and** an owned-connection probe succeeded within the last 60 s (cached); new field `auth_enabled` = `config.AUTH_ENABLED`.
     - Rate limit: at most 6 creates per 10 minutes per (source, caller), where caller = `g.auth_user` or `bearer` or `anonymous`. Excess gets 429.
  5. **K3:** a create body with `expiry_mode:"sliding"` stores the mode. Each page or catch-up then sets `expires_at = LEAST(now()+60 min, created_at+24 h)`, and the response reflects it. The absolute mode is unchanged. Capability `sliding_expiry:true`, `idempotent_create:true`.
  6. **Errors:**
     - `_connection` maps only `psycopg2.OperationalError` and `InterfaceError` to 503; everything else is logged (`logger.exception` with an error class, never token or DSN) and still returns 503.
     - The rollback in the error path is wrapped so it never masks the original exception.
     - `_profile_bootstrap_v2`'s bare `except` logs as well.
  7. **Owned connection:** `application_name='lumae-profile-bootstrap'`, `keepalives=1`, `keepalives_idle=30`.
  8. `_iso` converts to UTC (also the legacy `_iso` in `catalog_enrichment`).
  9. **Body cap:** read at most 16 KiB from `request.stream`, so chunked bodies are capped too.
  10. Migration test for an account-era `principal TEXT NOT NULL` column **without** a default.
- Tests:
  - two concurrent creates on **different** sources both succeed;
  - on the same source, the second waits rather than returning 503 within the budget;
  - an epoch bump followed by release frees the slots and create succeeds (previously 429);
  - a duplicate `client_request_id` replaces the old session;
  - Retry-After is present;
  - sliding expiry extends the session and absolute mode does not;
  - an injected serializer error is logged;
  - a UTC `expires_at` on a non-UTC server;
  - a chunked 20 KiB body returns 400;
  - health truthfulness.
- Done: the lum010 probe cases for lock, lockout and logging invert.

**P1-7 — The v1 `/changes` reader must not skip events (LUM-001 gap F5).**
- Files: `catalog_enrichment.py` (`read_profile_changes` 583-632).
- Change:
  - Read state and events in **one statement**, a CTE over `profile_stream_state` joined with `profile_changes`, so both come from one snapshot.
  - Verify density: when `cursor<head`, the first returned seq must be `cursor+1` and the sequence contiguous, otherwise 410.
  - The same check applies to catalogue `/changes` if it shares the pattern.
- Tests: the integrity probe "v1 changes skip under concurrent compaction" inverts, and existing `/changes` tests stay green.

**P1-8 — Truthful health for `profile_bootstrap` on old databases, and payload privacy.** Covered by P1-6 (health) and P1-4 (headers). There is no separate PR; keep this entry for traceability.

**Phase 1 exit gate:**
- all P1 WPs merged, CI green;
- the §H Phase 1 client items merged in Auralscape;
- the audit probes for AUD-01 (client), AUD-02 (server 413), AUD-03, AUD-04, AUD-05 and AUD-11 inverted;
- the P0-3 budgets for the publication critical section and v2 create met.

### Phase 2 — Performance and the scale gate

**P2-1 — LUM-011: committed status summary; read-only GET routes (AUD-08).**
- Files: `catalog_readiness.py` (`_coverage` 123-165, `_link_coverage` 168-208, `v3_release_readiness` 257-428), `catalog_analysis.py` (`project_analysis` count computation 527-535, 659-667), `catalog.py` (`analysis_state` DDL 1223-1236), `provider_identity_guard.py` (`observe_provider_version` 409, `refresh_audiomuse_health` call 486-498, `_update_observation` 330-406), `__init__.py` (`catalog_health` 1867-2031, `render_settings_status_panels` 5369, `analysis_status_counts` 3594-3660).
- Change:
  1. Persist the link, ready, pending, missing, suspect and evidence counts in `analysis_state` in the **same** transaction that publishes the projection. They are exact for the published generation.
  2. Persist eligible, mapped and fingerprinted track counts at catalogue publication. Readiness reads the summary, never live full scans.
  3. `observe_provider_version` still pings every call (no stale identity cache), but it writes **only when the state or version changes**, and never commits the request connection. The route owns its transaction.
  4. Remove `refresh_audiomuse_health` from the GET path. The existing cron `provider_identity_recheck_task` (971, runs at "2,32 * * * *") already refreshes it.
  5. New `status_model.py` holds one read model used by health, `/settings/status` and database-state.
- Tests:
  - an equivalence test: summary-derived readiness blockers equal the live-query blockers on the fixture, across states;
  - GET health performs zero writes when nothing changed (statement counter).
- Budgets: health ≤50 ms p95 excluding the ping; settings ≤100 ms p95 (currently 372 ms and 544 ms).

**P2-2 — LUM-012: incremental projection (AUD-07).**
- Files: `catalog_analysis.py` (reads 145-203 and 306-345, link fingerprints 399-426 and 503-506, per-row writes 540-629), `catalog.py` (`fingerprint` 295-323), `relationship_build.py` (`input_identity` 116-137).
- Change:
  - (a) Detect link changes by comparing tuples; drop the link fingerprints, which are internal only.
  - (b) Hash vectors in SQL with `encode(sha256(embedding),'hex')`, which gives the same values. Fetch vector bytes only for new or changed items.
  - (c) Recompute `scalar_fp`/`umap_fp` only for items whose raw row changed; the algorithm is unchanged because clients see these values.
  - (d) Group first, then read payload and chromaprint only for groups with more than one occurrence.
  - (e) Copy unchanged rows set-based (`INSERT…SELECT`, reusing `provider_identity_rekey._copy_analysis_generation` 405-497) and use `execute_values` for changed rows.
  - (f) Key the relationship rebuild on a digest of the inputs it actually uses, not on the generation number.
- Tests: an **equivalence test**, where the old and new projector produce identical `analysis_changes`, `analysis_state` and relationship inputs on no-change, 1-row-delta and full fixtures. Keep the old implementation in the test module as the oracle.
- Budgets: no-change ≤5 s and ≤400 MB; delta ≤10 s (currently 20 s / 1.5 GB and 57 s).

**P2-3 — Catalogue publication lock coupling (LUM-008 gap F4).**
- Files: `catalog.py` (`refresh_catalog`; `invalidate_catalog_changes` around 2015), `profile_publication.py`.
- Change:
  - Rewrite `invalidate_catalog_changes` set-based: one `UPDATE … FROM` per table instead of 3 round trips per track.
  - Measure the time `catalog_state` is held.
  - Evaluate `FOR SHARE` instead of `FOR UPDATE` for attempt admission and completion, which need only fence against publication. Adopt it only with a concurrency test proving LUM-008 invariants.
- Budget: a full reconcile of 20k changed tracks holds `catalog_state` for ≤1 s (currently about 8 s).

**P2-4 — v2 capture off the request thread (conditional).**
- After P1-5 and P1-6, measure create p95 at 94k with edges on gunicorn gthread×4 (P2-6).
- If it is ≤5 s and no 503s occur under 2 concurrent creators, **close as not needed**.
- Otherwise implement create → 202 `{status_url}` with the capture as an RQ task, add a new capability flag and a contract entry, and add client support through §H C-3.

**P2-5 — Migration and lock hygiene (P3 migration items).**
- Files: `migrate_attempts` and the other `ADD COLUMN IF NOT EXISTS` sequences; `collection_manager.py:147-151`.
- Change:
  - Check `information_schema.columns` first and skip no-op `ALTER`s, so no ACCESS EXCLUSIVE lock is taken when there is nothing to do.
  - Retry the `collection_changes` lock with bounded backoff instead of failing the install.
- Test: re-running migrate on a populated fixture takes no ACCESS EXCLUSIVE locks (check `pg_locks` from a second connection).

**P2-6 — Representative-scale end-to-end gate (audit §3.3d).**
- Files: new `scripts/e2e/`:
  - a `docker-compose.yml` with the stock AudioMuse image at a pinned SHA under gunicorn gthread×4, plus `postgres:15-alpine` and PG17 variants, Redis and the plugin mounted;
  - `seed_representative.py` (reusing P0-3's seeder, with real edges);
  - `run_server_matrix.py`.
- The client half is §H C-8, which drives the real Auralscape sync services against this server with file-backed SQLite and **real** admission modules, a clock advanced beyond 5 minutes, interruptions and restarts.
- Must pass:
  - a fresh first load of 94k waveform + edge profiles completes once, and in one run;
  - a re-analysis pass of every song (no-op) produces 0 events (P1-1);
  - an LUM-005-style full republish with K6 delivers ≤1 KB per track;
  - the v2 and legacy paths both work;
  - kill and restart at 6 points;
  - a concurrent second device;
  - the settings poll and health stay within budget during the load.
- Run it on every release candidate.

**P2-7 — FederatedAlbums artwork proxy (perf P3).**
- Files: `plugins/FederatedAlbums/__init__.py` (650-662).
- Change: connect timeout 3 s, read timeout 5 s, a response size cap, and a short negative cache for failing friends. This keeps one slow friend server from pinning the 4 host threads.
- Test: a mocked slow upstream returns within 6 s.

### Phase 3 — Semantics, product and structure

**P3-1 — LUM-005 loudness analyzer v2 (AUD-10, K11).** Depends on P3-2 (K6) being released to clients before regeneration starts.
- (a) **Mapping spike** (U3): run v1 and v2 on 200–500 real tracks and publish the distribution of v1−v2 deltas. It decides the client's interim v1 normalisation offset for C-11.
- (b) **New module `loudness_v2.py`:**
  - measure on 48 kHz PCM, either resampled (`AudioResampler(rate=48000)`) or with coefficients derived per rate from the analog prototype;
  - BS.1770-4 channel weights (L, R and C = 1.0; surrounds 1.41; LFE excluded) — keep mono and stereo only if the channel layout is not supported;
  - 400 ms blocks with 75% overlap;
  - an absolute −70 LUFS gate and a relative −10 LU gate;
  - MixRamp ramps keep the 100 ms chunk method, but chunks are channel-**summed** and taken relative to the v2 integrated value.
- (c) **Versioning:**
  - `ANALYZER_VERSION = 2` for new analyses. Published v1 bytes stay published until v2 replaces them (LUM-008 guarantee).
  - The eligibility predicates treat v1 as upgradeable at low priority: SQL `fetch_backfill_rows` 3480-3529 and Python `is_backfill_candidate` 3370/3382 must stay in agreement (tests `test_lumae_analysis.py:3655, 3715`). Also `profile_task_disposition` 3239, `analysis_status_counts` 3629, the completion fence `profile_publication.py:258`, and `split_analyze_ids` 1463, which currently ignores the version.
  - An admin setting `loudness_v2_regeneration` (on by default) caps the rate at a configurable batch size, runs only when there is no interactive work, and never withdraws v1.
- (d) **Tests:** EBU Tech 3341-style synthetic vectors (1 kHz sine at −23 LUFS stereo, gated sequences, 44.1/48/96/192 kHz, mono/stereo/5.1) within ±0.1 LU of `pyloudnorm`. Invert `probes/loudness_and_payload/lufs_probe.py`.
- Capability: `analyzer_versions:[1,2]`, `loudness_method:"bs1770-4"`.

**P3-2 — Edge references in events and pages (K6).**
- Files: `catalog_enrichment.py` (`record_profile_change`, `serialize_profile`, `read_profile_changes`, `profile_bootstrap_page`), `edge_profile_store.py` (`publish_edge_profile` 100-161), `profile_bootstrap.py` (pages).
- Change:
  - The journal stores the waveform payload plus `edge_ref`.
  - With the opt-in, the reader emits `edge_profile_ref` when the event is a waveform-only change whose edge is unchanged.
  - Without the opt-in, it expands to the full current edge, joined by digest. If the digest is gone, it falls back to the current edge for that `media_revision`, and otherwise omits the edge; the replacing event follows.
  - Edge publications themselves always carry the full edge.
- Tests: both modes; an old-client byte-compatibility test against golden responses; a 94k waveform-only republish producing ≤1 KB per event on the wire with the opt-in.

**P3-3 — Compact edge transport (K7, optional, about 13% after gzip).**
- With `edge_compact=1`, strip `boundaries` (and the other derivable fields only if the client supports it) at serialization. The stored payload and digest are unchanged.
- Tests: the golden fixture round-trips through strip-and-rebuild to the same digest.
- Ship only if C-12 is ready. Otherwise close it as deferred.

**P3-4 — Collections protocol hardening (LUM-002/003/004 gaps, K8, K9).**
- Files: `collection_manager.py`, `shelves.py`, `provider_identity_rekey.py`, `collection_ui.py`, `collection_library.py` (`_ITEM_ID_RE`).
- Changes, one PR each where noted:
  1. **Feed (K8):**
     - add `epoch`, `head_seq`, `has_more` and `next_cursor` (already present) to the response;
     - a 410 `collections_resync_required` when the request's `epoch` mismatches or the cursor is beyond head;
     - `GET /plugins/lumae_analysis/api/collections/snapshot` returns collections, items and `{epoch, head_seq}` in one REPEATABLE READ transaction on an owned connection (reuse the `profile_bootstrap._connection` pattern);
     - publish `floor_seq`, the head at cutover.
  2. **Restore and batch:**
     - allocate a block with `UPDATE feed_state SET head_seq=head_seq+n RETURNING` after staging, then insert the events in one multi-row insert;
     - `SET LOCAL lock_timeout='3s'` in `_mutation_response`, with 503 plus `Retry-After` on timeout;
     - chunk restores into transactions of at most 2,000 items.
     - Budget: another principal's write waits ≤500 ms during a 20k restore (currently about 9.85 s).
  3. **Conflicts (K9, gated by header):**
     - `idempotency_key_conflict` includes `current`;
     - `membership_conflict {existing_item_id}` replaces the silent remap in `_upsert_item` 666-726;
     - create with an existing or deleted id returns 409;
     - without the header, behaviour is unchanged.
  4. **Rekey through the protocol** (`provider_identity_rekey.py` 284, 343-373, 932-941):
     - collection item rekeys lock parents in sorted order;
     - collisions merge (delete the duplicate with a delete event);
     - the revision is bumped and events are emitted through `_record_change`;
     - scope to the matching catalogue once K10 lands;
     - **stop rewriting delivered `collection_changes` and receipt payloads**;
     - a collision in one principal is isolated: that principal's rekey is deferred with a diagnostic, and the installation rekey proceeds.
  5. **Shelves:** `rekey_shelves` (179-194) takes the `shelf_scopes` lock before `nextval`, and shelf receipts bind the request fingerprint (235-238).
  6. **Growth:** compact `collection_changes` below `floor_seq` once K8 has shipped and clients have been observed on it. Receipts get a 30-day TTL.
  7. **Small items:**
     - `current_principal` fails closed for unknown auth methods (60-72);
     - `_ITEM_ID_RE` rejects dot-only ids;
     - the collection UI label checks `provider_catalog`;
     - `CREATE EXTENSION IF NOT EXISTS unaccent` when permitted, otherwise degrade with a diagnostic;
     - delete the dead name-guessing helpers (`collection_library.py` 403-463).
- Tests:
  - the frontier and restore-hold probes invert (budget met);
  - a rekey collision isolation test;
  - the shelves late-commit test;
  - K8 and K9 in both header modes;
  - the old-client compatibility tests.

**P3-5 — Workbench LUM-013 → 014 → 015 → 016 (K10).** Serial, one PR each.
- **LUM-013:**
  - an explicit `catalog_instance_id` on browse, stats, album, stream and art; 400 if it is missing while more than one source is active;
  - the provider is resolved from the source row instead of `config.MEDIASERVER_TYPE` (`collection_library.py` 426, 509, 617);
  - collection items get a nullable `catalog_instance_id`, included in the unique indexes;
  - backfill only when exactly one source has ever existed;
  - feed payloads carry it (K10).
- **LUM-014:**
  - browse `catalog_albums` by `album_id` within the current generation;
  - always emit `provider_album_id`;
  - details by (catalogue, `album_id`);
  - `album_key` for display and legacy use only;
  - **precondition:** client C-13 has aligned its album unique index.
- **LUM-015:** disable "newest year" now. Later, publish a provider year, sort `NULLS LAST` with a `(lower(title), album_id)` tie-break, and label unknowns "Unknown year".
- **LUM-016:**
  - a stored `search_text` column (casefolded, accent-stripped) with a `gin_trgm_ops` index scoped by catalogue and generation;
  - keyset pagination on `(lower(key), id)`;
  - totals from `catalog_state.entity_counts`, or a capped "1000+";
  - an index on `(catalog_instance_id, published_generation, album_id)`.
  - Budgets: browse ≤100 ms, search `all` ≤300 ms, deep page ≤100 ms (currently 0.3–0.6 s, 2.34 s and 0.6 s).
- Tests: the editions probe (`probes/collections/explain_editions.py`) inverts, and there are `EXPLAIN` regression checks.

**P3-6 — LUM-007 retry gaps.**
- Files: `profile_publication.py` (stale transitions 264-270, 411-417, 441-447, `release_attempts` 203-233), `provider_identity_rekey.py` (320-325), `__init__.py` (`release_pending` 1572 and callers 1621, 2862, 3259, 3287, 3319, 4030; selection 3461-3472; `recover_stale_pending_profiles` 4307).
- Change:
  - Stale transitions clear or requalify `retry_category`, or the selection's stale clause includes previously failed categories when the fingerprint returns.
  - Pause, legacy migration and batch abort call `release_attempts(count_failure=False)`.
  - Cooldowns use the slot for the attempt count (60 s, 300 s, 1800 s).
  - Lock rows in sorted id order in `release_attempts` and `recover_stale_pending_profiles`.
- Tests: the integrity probes for "stranded retry" and "pause burns attempts" invert, plus a concurrent admit/release test.

**P3-7 — LUM-008 gaps.**
- A reconcile task withdraws published profiles whose track is no longer in the current catalogue generation (the orphans seeded from 1.2.5), with tombstones.
- `edge_backfill_candidates` (`edge_profile_store.py:164`) reads published profiles.
- A repair action handles "ready but unpublished" rows (from P1-3 detection): republish them through `complete_attempt` semantics.
- Tests: seed an orphan → withdrawn with an event; backfill selects from published.

**P3-8 — LUM-018 execution limits and decoder root cause.**
- Files: `loudness.py` (`analyze_file` 283), `edge_profiles.py` (`analyze_edge_file` 419), the task entry points in `__init__.py`.
- Change:
  - Run each file analysis in a **child process** with a hard wall-clock limit (default 15 minutes, configurable) and kill it on expiry. This covers hung native decoders, which cannot be interrupted in-process.
  - Classify failures (`InvalidDataError` → `media_error`, timeout → `analysis_timeout`, crash → `analysis_crash`) into LUM-007 categories.
  - Record safe diagnostics (container, codec, sample rate, channel layout, byte size and the decode position at failure; no paths or credentials) to support the root cause of the original decoder incident.
  - Document the host's `job_timeout=-1`.
- Tests: a fake analyzer that sleeps past the limit is killed and classified; a decoder error is classified.

**P3-9 — LUM-017 readiness UI (plugin side).**
- The settings page shows independent states per stream (catalogue, analysis, waveform, edge, relationships): availability, freshness and job status.
- Scoped retry buttons.
- Keyboard, focus and zoom checks, and ARIA live regions (the existing `settings_ui.py` polling reuses P2-1).
- Tests: render snapshot tests and an accessibility lint on the HTML.

**P3-10 — LUM-021 diagnostics closure.**
- Files: `database_state.py` (777-784, count queries 150-260), `__init__.py` diagnostics.
- Change:
  - Redact the state `last_error` text with the existing redaction helper.
  - Add a per-query `SET LOCAL statement_timeout`.
  - Counts come from published and retry states, including deferred, cooling, exhausted and `deferred_no_media_revision`.
  - Show "unavailable" instead of zeros when a query fails.
  - v2 route logging comes from P1-6.
- Tests: a secret in `last_error` is redacted; a query timeout renders "unavailable".

**P3-11 — LUM-020 structural extraction.** Behaviour-preserving, one module per PR, with no SQL changes mixed in.
- **Order:**
  1. `settings_render.py`
  2. `status_model.py`, if P2-1 has not already created it
  3. a single source-query builder (after P3-5 LUM-013)
  4. `preparation.py`, `profile_backfill.py` and `analysis_runs.py`, with thin task shims left in `__init__` so dotted task paths are unchanged
  5. route modules registering on the same `bp`
  6. `schema.py`, re-exported from `__init__`
- **Convention:** routes and tasks own transactions; repository functions take a cursor and never commit; GET routes are read-only; retire the `commit=` flags.
- **Guards:**
  - Tests patch 264 package-level attributes (70 of them `get_db`). Keep the names resolvable from `__init__`, or move the patches in the same PR.
  - Add a test asserting every registered cron and RQ dotted path is still importable.

**P3-12 — LUM-019 documentation.**
- The README describes current capabilities: catalogue, profiles, edges, the offline bulk copy, collections and the 1.3.0 upgrade runbook. DJ history moves to the changelog.
- `runtime/README.md` is marked historical.
- Document the AUTH_ENABLED=false behaviour (transfers are anonymous).
- `private-dist/` is handled per U2.

**P3-13 — LUM-001 test strength.** Add a concurrency test that fails when the `profile_stream_state` `FOR UPDATE` is removed: two publishers on different tracks without `catalog_state` serialization (the audit found only 1 of 39 tests fails today).

### Phase 4 — Release 1.3.0

**P4-1 — Release candidate.** Requires U5 approval before merge.
- Update the `plugin.json` 1.3.0 entry: `min_core_version`, and a changelog listing every contract flag.
- `release-sources.json` switches to source mode at 1.3.0, unchecksummed; the CI build job creates the immutable archive.
- Verify that the 1.2.x archives are unchanged.

**P4-2 — Qualification:**
- P2-6 end-to-end gate on the AudioMuse host SHAs `f100684`, `ce742938` and current `main` (record the SHA), in gunicorn gthread×4 container topology, on PG15 and PG17;
- a populated 1.2.5 → 1.3.0 upgrade rehearsal from a fixture: run a 1.2.5 worker concurrently to prove the fences fail closed, migrate, then verify `integrity`;
- the client fallback matrix (§H) against 1.2.5 and 1.3.0;
- device checks (U4).

**P4-3 — Rollout.** Follow `docs/runbooks/UPGRADE_1.3.md`. User-executed (U5).

**P4-4 — Post-release.**
- Watch health `integrity`, v2 429/503 rates (logged) and the journal retention floor.
- Start analyzer v2 regeneration (P3-1) only after the Auralscape release that includes C-10 and C-11 is adopted.

---

## 4. Sequencing summary

```
P0-1 → P0-2 → (P0-3 ∥ P0-4 ∥ P0-5)
Phase 1 plugin: (P1-1 ∥ P1-2) → (P1-7 ∥ P1-4) → P1-3 → P1-5 → P1-6        ∥  client §H Phase 1 (C-1…C-7)
Phase 2: P2-1 ∥ P2-2 ; P2-3 ; P2-5 ; P2-6 (needs P1 + C-8) → P2-4 decision ; P2-7 any time
Phase 3: P3-2 (+C-10) → P3-1 (+C-11; regeneration after client adoption) ; P3-4 (+C-13) ; P3-5 serial (LUM-014 after C-13 index fix) ;
         P3-6 ∥ P3-7 ∥ P3-8 ; P3-9 ; P3-10 ; P3-11 last ; P3-12 ; P3-13 any time
Phase 4: P4-1 → P4-2 → P4-3 (user) → P4-4
```

## 5. Verification (end to end)

- **Every WP:** §1.2 checks, plus a green PR CI run on PG17.
- **Phase exit:**
  - the listed probes invert;
  - `scripts/perf/run_baseline.py --check` meets the budgets reached so far;
  - `docs/STATUS.md` is updated.
- **Scale:** the P2-6 server matrix plus the client C-8 harness, as in §3 P2-6.
- **Compatibility:** golden-response tests for old clients on every contract WP; the Auralscape fallback matrix against 1.2.5 and 1.3.0.
- **Release:** P4-2 in full. No release claim without the representative-scale and upgrade-rehearsal evidence.

## 6. Out of scope, recorded

- Upstream AudioMuse changes. None are required; the experimental host branches stay unmerged.
- Rewriting the durable v2 transfer into a lease design. That is only revisited if P2-4 measurements fail.

## 7. Traceability — every audit finding → WP

| Finding | WP |
|---|---|
| AUD-01 5-minute TTL aborts sync | C-1 |
| AUD-02 bulk edges infeasible | P1-4, P1-5, P3-2, P3-3, C-4, C-7, C-10, C-12 |
| AUD-03 no-op republish, edge deletion | P1-1, P1-2 |
| AUD-04 full-journal compaction | P1-2 |
| AUD-05 old-worker wedge, version fence | P1-3, P4-2 |
| AUD-06 CI and release flow | P0-1, P4-1 |
| AUD-07 projection cost | P2-2 |
| AUD-08 health and settings poll | P2-1 |
| AUD-09 provider user-state late preflight | C-2 |
| AUD-10 loudness | P3-1, C-11 |
| AUD-11 v2 operability | P1-2 (floor hold), P1-5, P1-6, P2-4, C-3 |
| AUD-12 60-minute expiry | P1-6 (K3), C-6 |
| LUM-001 v1 reader skip; test strength | P1-7, P3-13 |
| LUM-007 stranded rows, pause burns attempts, cooldown, lock order | P3-6 |
| LUM-008 no-op republish, seed orphans, backfill source, lock coupling, unpublished-ready | P1-1, P3-7, P2-3, P1-3 |
| LUM-004 restore stall, no epoch/410, legacy writer | P3-4, P1-3 |
| LUM-002/003 rekey bypass, id remap, fingerprint and reorder conflicts, create with existing id | P3-4, C-13 |
| Shelves late-commit, receipts | P3-4 |
| LUM-013/014/015/016 | P3-5, C-13 |
| LUM-017 | P3-9, C-14 |
| LUM-018 and decoder root cause | P3-8 |
| LUM-019 | P0-5, P3-12 |
| LUM-020 | P3-11 |
| LUM-021 | P3-10, P1-6 |
| Migrations: no-op ACCESS EXCLUSIVE, lock timeout | P2-5 |
| LUM-010 P3 items: health overstates, UTC, chunked body cap, no logging, keepalives, migration test | P1-6 |
| 302 instead of 401 on plugin paths (host behaviour) | Documented in the contract (P0-4); client treats 302/HTML as an auth failure (C-3) |
| Collections P3 items: fail-open principal, dot ids, UI label, unaccent, dead code, unbounded growth | P3-4 |
| FederatedAlbums proxy | P2-7 |
| Client: source key includes auth mode, MD5 settings scope, cookie over bearer, pending release skips delta, 400 retry loop, dead parser, stale hand-off doc | C-5, C-15, C-3, C-16 |
| Process: stale plan, bookkeeping overhead, representative scale | P0-4, P0-5, P2-6 |

---

## H. Hand-off for the Lumae app (Auralscape) agent

> Written to `docs/handoff/AURALSCAPE_SYNC_HARDENING_HANDOFF.md` in both repos by the orchestrator right after plan approval. Give the user this text verbatim to pass to the app agent.

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

### H.3 Server capabilities you will detect

All live under `GET /plugins/lumae_analysis/api/.../health` → `capabilities`, and every one is optional. See the plan's §2 for K1–K11.

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
| `collections.contract: 2` | Send `X-Lumae-Collections-Contract: 2`; handle 409 `membership_conflict {existing_item_id}` and `idempotency_key_conflict` with `current`. |
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
  - otherwise queue the track for fetch through `GET /api/profiles?ids=` (batches of up to 100; note the comma-joined ids), writing results through the C-4 path.
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
