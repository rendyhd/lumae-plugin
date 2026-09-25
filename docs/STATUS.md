# Lumae plugin — work status

The single source of truth for the plan in [docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md](plan/LUMAE_FINAL_PLAN_2026-09-24.md). There is one line per work package (WP). Work packages land on a phase branch, and each phase has one PR to `main`. The older `docs/remediation/RESUME.md` and ledger are kept as historical records.

## Current state

| Item | State |
|---|---|
| `main` release policy | LumaeAnalysis pinned to the 1.2.5 archive (P0-1). Merges do not publish. |
| CI on `main` | The `test` job (PG17) runs on every PR push. Making it a required check is pending the user (U1). |
| Next release | 1.3.0 (P4-1): Phases 1–2 are merged; the release waits for user approval (U5) |
| Phase 2 exit gate (2026-09-25, `phase/2-performance` 66ec4c3) | `run_baseline.py --check` at scale 1 (94k profiles with real edges; [docs/perf/PHASE2-EXIT-2026-09-25.json](perf/PHASE2-EXIT-2026-09-25.json)): health p95 3.6 ms (≤50), settings 6.5 ms (≤100), projection no-change 3.0 s / 357 MB (≤5 s / 400 MB), delta 6.2 s (≤10), v2 create 2.1 s with 5.2 ms global lock (≤5 s / 50 ms), page p95 32 ms (≤50). The publication critical section is 6.0 ms against ≤5 ms, an accepted miss (P1-2 decision, 8.2 ms then; re-check after P3-2). AUD-07 (projection) and AUD-08 (health/settings) are inverted. End-to-end gate: [docs/perf/E2E-2026-09-25.md](perf/E2E-2026-09-25.md); the same-source creator decision is in the plan's P2-4 entry. Full suite 859 passed, 1 xfailed. |
| Client hand-off | [docs/handoff/AURALSCAPE_SYNC_HARDENING_HANDOFF.md](handoff/AURALSCAPE_SYNC_HARDENING_HANDOFF.md) |

## Work packages

| WP | State | PR | Merge SHA | Tests |
|---|---|---|---|---|
| Docs: audit, plan, hand-off | merged | rendyhd/lumae-plugin#2 | 629089f | docs only |
| P0-1 CI pinned artifact | merged | rendyhd/lumae-plugin#3 | 746a7b8 | 573 passed; retro review PASS |
| P0-5 status and doc hygiene | merged | rendyhd/lumae-plugin#4 | 5766d68 | docs only; retro review PASS |
| P0-1/P0-5 review follow-ups | merged | rendyhd/lumae-plugin#5 | 38c11a4 | release tests strengthened |
| P0-4 sync contract | merged | rendyhd/lumae-plugin#5 | 38c11a4 | docs only; review fixes applied |
| P0-2 migrated_db fixture | merged | rendyhd/lumae-plugin#5 | 38c11a4 | 582 passed |
| P0-3 perf harness and baseline | merged | rendyhd/lumae-plugin#5 | 38c11a4 | baseline: 6/7 budgets missed (expected) |
| P1-1 no-op republish keeps edges | merged | rendyhd/lumae-plugin#6 | 9552f43 | 596 passed |
| P1-4 gzip transport and private headers | merged | rendyhd/lumae-plugin#6 | 9552f43 | 628 passed; 50-row edge page 1002 KB → 372 KB (2.7×, distinct edges, gzip level 4) |
| P1-2 journal compaction and retention | merged | rendyhd/lumae-plugin#6 | 9552f43 | 607 passed; publication p95 20.9→6.2 ms (P1-2 alone), 8.2 ms with P1-1 |
| P1-7 single-snapshot /changes reader | merged | rendyhd/lumae-plugin#6 | 9552f43 | 674 passed |
| P1-3 fences and 1.3.0 | merged | rendyhd/lumae-plugin#6 | 9552f43 | 696 passed |
| P1-5 v2 snapshots store edge references | merged | rendyhd/lumae-plugin#6 | 9552f43 | 705 passed; create@94k 8.42 s (413; 115.7 s / 2720 MB WAL with limits lifted) → 3.0–4.3 s / 73–75 MB WAL; page p95 31.4→24.8 ms (50 rows); global lock hold 3–4.3 s left for P1-6 |
| P1-6 v2 operability | merged | rendyhd/lumae-plugin#6 | 9552f43 | 737 passed; create@94k 2.87 s, global lock 5.1 ms (same fixture before: 2.95 s, 2946 ms); page p95 31.0 ms |
| P2-7 FederatedAlbums proxy timeouts | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 598 passed |
| P2-2 incremental projection | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 613 passed, 1 xfailed; no-change 30.5→4.0 s (1,596→356 MB), delta 65.0→5.8 s |
| P2-1 committed status summary | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 790 passed, 1 xfailed; health 349→4.2 ms, settings 496→6.9 ms (p95); no-change projection 2.45→2.7 s |
| P2-5 migration lock hygiene | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 803 passed, 1 xfailed; re-run migrate on a populated schema: ACCESS EXCLUSIVE on 17 tables and SHARE on 16 (26 distinct) → no lock above ROW EXCLUSIVE; fresh and upgrade schema identical to base |
| P2-3 catalogue lock coupling | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 830 passed, 1 xfailed; catalog_state hold @20k 147,086→679 ms (median of 3 probed runs; full reconcile of 132k tracks with 20k withdrawals; publisher-side max 149,417→734 ms); post-commit edge sweep at 94k 175 ms (no orphans), 1.1–2.3 s (20k withdrawn) |
| P2-8 FederatedAlbums migration locks | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 811 passed, 1 xfailed; re-run migrate: ACCESS EXCLUSIVE and SHARE on 3 FederatedAlbums tables → no lock above ROW EXCLUSIVE; shared-cluster advisory pg_locks probes filtered by database |
| P2-6 end-to-end scale gate | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 803 passed, 1 xfailed; 94k profiles + edges on gunicorn gthread 1×4 and the stock AudioMuse 8aa1639c host: v2 first load 236 s / legacy 215 s (785 MB gzip), no-op re-analysis 0 events, kill -9 at 6 points, 2nd device PASS; health/settings p95 ≤ 44 ms during loads; LUM-005/K6 pending P3-2 (8.3 KB/track); 2 concurrent creates p95 6.7–9.7 s (1 × 503) → P2-4: implement SQL-side capture; capped catch-up 1,056,000 events 48 s; found v2 page plan bug (docs/perf/E2E-2026-09-25.md); review follow-up: 8 kill points incl. both captures + K5 retry, direct edge-row checks, fixture guard, verified at 0.02 with 2 mutations caught |
| P2-4 v2 capture in SQL | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 849 passed, 1 xfailed; snapshot and first catch-up rows built by INSERT … SELECT (off the web worker's GIL), output byte-identical to the Python capture (oracle equivalence test); create@94k 3.65→2.35 s; e2e 2 creators p95 same source 8.47 s (1 × 503)→4.85 s (0 × 503), different sources 6.55→2.41 s, status routes p95 during creates 35/50/68→12/18/27 ms; capped first catch-up unchanged (1,056,000 events: review re-measure 42.6 vs 42.8 s; the 50.5→39.7 s in the P2-4 commit message did not reproduce); page queries limit before the edge lookup (fresh table, 1,880 edges: 162 ms / 1,880 lookups→5.5 ms / 50) |
| P2-4b capture follow-ups | merged | rendyhd/lumae-plugin#7 | df4d7c8 | 859 passed, 1 xfailed; profiles whose ref_lufs is not plain (NaN/inf, |x| ≥ 2^23, non-zero below 1e-4) are serialized by serialize_profile itself, fixing review LOW-1 (efd=-2: 503 → null) and LOW-2 (7.038531e-26 → 7.0385313e-26); the float4 shortest-decimal search is gone; plain-range float verification 0 mismatches at efd 1/0/3/-5, exhaustive double-rounding scan of the plain range 0; create@94k (boot_bench ×3) 2.12/2.69/1.96 → 2.12/2.04/2.18 s, 0 fallback rows on analyzed loudness; catch-up number assumptions documented; contract lists every hold lapse as a 410 cause; test for changes between snapshot batches (LOW-7); e2e: capture-kill lock always released, bounded 429/503 retries, cut-off requests matched one to one, git_sha from describe --always --dirty |
| P3-13 LUM-001 concurrency test strength | on phase/3-semantics | Phase 3 PR | | 873 passed, 1 xfailed; 14 tests cover both append paths (record_profile_change, record_profile_deletions) and compaction; 7 lock mutants each fail 4–14 of them (helper without `FOR UPDATE`: 14, before: 1 of 705); wait probe stable in 40 runs under 6 CPU burners |
| P3-8 LUM-018 execution limits | review pending | Phase 3 PR | | 925 passed, 1 xfailed; each waveform/edge analysis runs in a pooled subprocess worker killed 30 s after `analysis_time_limit_seconds` (default 900; SIGTERM, SIGKILL after 5 s); new categories `media_error` (revision) and `analysis_crash` (transient), SQL retry selection takes the Python set; safe diagnostics in `source_profiles.failure_diagnostics` and the edge job reason; worker start 0.9–1.1 s per job, warm per-file overhead 1.1–2.0 ms median (20 × 5 s FLAC), 4-min track in-worker 0.876 s vs in-process 0.878 s, worker RSS 124 MB idle / 255–264 MB peak; 11 mutants caught (kill, SIGKILL, crash/timeout/InvalidData misclassified, SQL list, category set, close_fds, select for poll, path leak) |
