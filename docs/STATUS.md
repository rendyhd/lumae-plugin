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
| P0-1/P0-5 review follow-ups | on phase/0-foundation | Phase 0 PR | | release tests strengthened |
| P0-4 sync contract | on phase/0-foundation | Phase 0 PR | | docs only; review fixes applied |
| P0-2 migrated_db fixture | on phase/0-foundation | Phase 0 PR | | 582 passed |
