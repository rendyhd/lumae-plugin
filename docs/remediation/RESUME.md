# Lumae remediation — resume checkpoint (2026-09-23)

The authoritative overview is [LUMAE_REMEDIATION_LEDGER.md](LUMAE_REMEDIATION_LEDGER.md). Preserve task IDs 0A/0B, 1A–1C and LUM-001–021, existing decisions, historical evidence and review attribution. Phase 0 is complete. The user’s V2 brief replaced model routing and startup only.

## Exact implementation checkpoint and checkout

- Plugin path: `C:\Users\rendy\vscode\lumae-plugin`; branch `codex/lumae-remediation-20260922`.
- **SERVER_REMEDIATION_CHECKPOINT_SHA:** `816420f944bdac02c620928e46d752fde981d577` (local commit `remediation: checkpoint server integrity and profile lifecycle`). It contains LUM-001/002/003/004/006/007, server-side LUM-008, plugin-side Task 0B/LUM-021, associated migrations, tests and design/implementation evidence. This is the immutable implementation baseline, **not a release-qualified SHA**. Commit 1 was local-only and was not pushed.
- **The commit containing this bookkeeping update is the LUM-010 orchestration/resume baseline.** After creating it, resolve exact HEAD as `LUM010_START_SHA` and check `git status --short`. Its own exact SHA must not be inserted into that commit. At the next normal ledger/RESUME update during LUM-010, record the known value.
- The pre-commit implementation checkout had no unstaged tracked code differences from the staged checkpoint. Affected Python compileall/py_compile and both unstaged/staged `git diff --check` passed. No plugin source changed after the recorded full-suite run. Full serial disposable PostgreSQL 17 plugin suite: **546 passed, 3 failed**, exit 1; the three are unchanged published 1.2.5 archive/source identity checks and remain open release gates. No archive rebuild or new version selection.
- Project `.codex/config.toml` intentionally requests GPT-6 Sol Medium lead, Luna Low default helper and at most two spawned threads. Actual resolved parent/delegate model and effort are unobservable. Global settings were not changed; no Terra work was scheduled. Previous independent P1 reviews and accepted designs remain in the ledger.

## Remaining gates and next safe action

- Real-host integration, old-worker/writer drain, populated-state migration and compatibility, legacy feed-cursor reconciliation, historical Auralscape cached-profile convergence for LUM-008, broader client/integration and release qualification remain open. LUM-021 client diagnostics remain open. Existing personal state, principal/source boundaries, epochs, tombstones, atomic publication and versioned audio/protocol semantics must be preserved.
- Auralscape was previously observed dirty and separately owned; no Auralscape files were edited in this checkpoint. Other plugin worktrees remain untouched. Local ignored full-Jest evidence under `docs/remediation/evidence/phase0-client/` is retained without tracking the large logs/JSON.
- Once both commit SHAs are known and the post-Commit-2 worktree is checked, the next eligible action is the **Astra Medium LUM-010 protocol-design gate**. Do not push, tag, deploy, modify production data, rebuild the published 1.2.5 archive or choose a release version. No work continues after this session ends.
