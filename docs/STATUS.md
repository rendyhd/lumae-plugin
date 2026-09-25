# Lumae plugin — work status

The single source of truth for the plan in [docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md](plan/LUMAE_FINAL_PLAN_2026-09-24.md). There is one line per work package (WP). Work packages land on a phase branch, and each phase has one PR to `main`. The older `docs/remediation/RESUME.md` and ledger are kept as historical records.

## Current state

| Item | State |
|---|---|
| `main` release policy | LumaeAnalysis pinned to the 1.2.5 archive (P0-1). Merges do not publish. |
| CI on `main` | The `test` job (PG17) runs on every PR push. Making it a required check is pending the user (U1). |
| Next release | 1.3.0 (P4-1), after Phases 1–2 |
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
| P2-7 FederatedAlbums proxy timeouts | on phase/2-performance | Phase 2 PR | | 598 passed |
| P2-2 incremental projection | on phase/2-performance | Phase 2 PR | | 613 passed, 1 xfailed; no-change 30.5→4.0 s (1,596→356 MB), delta 65.0→5.8 s |
| P2-1 committed status summary | on phase/2-performance | Phase 2 PR | | 790 passed, 1 xfailed; health 349→4.2 ms, settings 496→6.9 ms (p95); no-change projection 2.45→2.7 s |
| P2-5 migration lock hygiene | on phase/2-performance | Phase 2 PR | | 803 passed, 1 xfailed; re-run migrate on a populated schema: ACCESS EXCLUSIVE on 17 tables and SHARE on 16 (26 distinct) → no lock above ROW EXCLUSIVE; fresh and upgrade schema identical to base |
| P2-3 catalogue lock coupling | on phase/2-performance | Phase 2 PR | | 830 passed, 1 xfailed; catalog_state hold @20k 147,086→679 ms (median of 3 probed runs; full reconcile of 132k tracks with 20k withdrawals; publisher-side max 149,417→734 ms); post-commit edge sweep at 94k 175 ms (no orphans), 1.1–2.3 s (20k withdrawn) |
| P2-8 FederatedAlbums migration locks | on phase/2-performance | Phase 2 PR | | 811 passed, 1 xfailed; re-run migrate: ACCESS EXCLUSIVE and SHARE on 3 FederatedAlbums tables → no lock above ROW EXCLUSIVE; shared-cluster advisory pg_locks probes filtered by database |
