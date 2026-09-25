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
| P1-1 no-op republish keeps edges | on phase/1-stop-the-bleeding | Phase 1 PR | | 596 passed |
| P1-4 gzip transport and private headers | on phase/1-stop-the-bleeding | Phase 1 PR | | 628 passed; 50-row edge page 1002 KB → 372 KB (2.7×, distinct edges, gzip level 4) |
| P1-2 journal compaction and retention | on phase/1-stop-the-bleeding | Phase 1 PR | | 607 passed; publication p95 20.9→6.2 ms (P1-2 alone), 8.2 ms with P1-1 |
| P1-7 single-snapshot /changes reader | on phase/1-stop-the-bleeding | Phase 1 PR | | 674 passed |
| P1-3 fences and 1.3.0 | on phase/1-stop-the-bleeding | Phase 1 PR | | 696 passed |
| P1-5 v2 snapshots store edge references | on phase/1-stop-the-bleeding | Phase 1 PR | | 705 passed; create@94k 8.42 s (413; 115.7 s / 2720 MB WAL with limits lifted) → 3.0–4.3 s / 73–75 MB WAL; page p95 31.4→24.8 ms (50 rows); global lock hold 3–4.3 s left for P1-6 |
| P1-6 v2 operability | on phase/1-stop-the-bleeding | Phase 1 PR | | 737 passed; create@94k 2.87 s, global lock 5.1 ms (same fixture before: 2.95 s, 2946 ms); page p95 31.0 ms |
