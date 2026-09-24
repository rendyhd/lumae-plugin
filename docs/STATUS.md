# Lumae plugin — work status

The single source of truth for the plan in [docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md](plan/LUMAE_FINAL_PLAN_2026-09-24.md). There is one line per work package (WP), added when it merges. The older `docs/remediation/RESUME.md` and ledger are kept as historical records.

## Current state

| Item | State |
|---|---|
| `main` release policy | LumaeAnalysis pinned to the 1.2.5 archive (P0-1). Merges do not publish. |
| CI on `main` | The `test` job on PG17 is required for every PR. |
| Next release | 1.3.0 (P4-1), after Phases 1–2 |
| Client hand-off | [docs/handoff/AURALSCAPE_SYNC_HARDENING_HANDOFF.md](handoff/AURALSCAPE_SYNC_HARDENING_HANDOFF.md) |

## Work packages

| WP | State | PR | Merge SHA | Tests |
|---|---|---|---|---|
| Docs: audit, plan, hand-off | merged | rendyhd/lumae-plugin#2 | 629089f | docs only |
| P0-1 CI pinned artifact | in review | rendyhd/lumae-plugin#3 | | 573 passed |
| P0-5 status and doc hygiene | in review | | | docs only |
