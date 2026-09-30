# Edge coverage, on-demand priority and vector generations: plan (2026-09-30)

Source: the app handoff `EDGE_PROFILE_ANALYSIS_HANDOFF.md` (A20 in the app's
`OPEN-ACTIONS.md`) and the phone's vector-replay failure. Target release: 1.3.0.

**Status (2026-09-30): implemented** on `claude/epic-davinci-q0gj48`; see
`docs/STATUS.md` rows E1–E4 and C1. Phase 0 found the root cause: the
worker's PyAV 17.1.0 failed the exact 16.1.0 pin, so the pin became runtime
self-qualification (E1).

## Decisions (user, 2026-09-30)

1. Vectors: a request for a pruned generation is served from the current
   generation, with checksums the client verifies. A `strict_generation`
   opt-in gets 410 instead.
2. The server owns the full-library edge pass. The app's `/edges/backfill` walk
   stays for compatibility but is no longer needed.
3. Background edge work runs at low priority on the same mechanism as the
   waveform/volume backfill: small batches, driven by the reconcile watchdog,
   after the waveform backfill, yielding to on-demand work and to AudioMuse's
   own jobs. No cap or time window.
4. An on-demand request the host refuses is saved and served by the next task,
   never lost or failed. This applies to waveform and edges.

## Findings

| # | Finding | Where |
|---|---|---|
| F1 | Web-side edge enqueue requires PyAV in the web process, so `/edges/analyze` and `/edges/backfill` return `available:false` on a stock web container even when the worker can analyse. | `__init__.py` `edge_profiles_enabled`, `enqueue_edge_profiles` |
| F2 | No server-driven edge backfill. Edges for tracks analysed before the worker had PyAV are only computed if a client walks the library. | reconcile has no edge branch |
| F3 | An on-demand edge request for a track already pending in a background batch is silently dropped (not accepted, not reported). | `edge_profile_store.claim_edge_jobs` |
| F4 | The edge follow-up after an interactive waveform analysis is queued as background. | `_schedule_edge_upgrade` |
| F5 | Background edge batches hold up to 100 tracks in one task, so they occupy the plugin's single root-task slot for minutes. | `edge_backfill_api`, `enqueue_edge_profiles` |
| F6 | `credits_service.playback_pending` reads the legacy `profiles` table; interactive admissions live in `source_profiles`, so credits and metadata never yield. | `credits_service.py` |
| F7 | The host refuses `enqueue` from the web process while any `plugin.*` root task or AudioMuse main task is active (`ERR_TASK_IN_PROGRESS`). `/api/analyze` releases the claim and errors; `/edges/analyze` marks jobs `edge-enqueue-failed` and errors. To confirm on the deployed host version. | AudioMuse `plugin/api.py` `enqueue`, `task_types.py` `plugin.` |
| F8 | A `pending_interactive` waveform row with no task behind it is skipped by background batches as "promoted", so after a refused enqueue it can sit until the 1 h stale recovery. | `profile_task_disposition` |
| F9 | `vector_batch` accepts any generation up to the current one and returns an empty page when that generation has been pruned. Change replay holds no lease, so a lagging client asks for pruned generations. | `catalog_analysis.vector_batch`, `prune_snapshot_generations` |

## Phase 0: operations (no code)

- Read `capabilities.edge_profiles` from health on the server the phone uses.
- Confirm PyAV 16.1.0 / libswresample 6.1.100 in the **worker** environment.
- Confirm the `edge_profiles_enabled` setting is not `false`.
- Record the AudioMuse host version; check the web logs for
  `ERR_TASK_IN_PROGRESS` on `/api/analyze` and `/edges/analyze`, and
  `edge_profile_jobs.last_error = 'edge-enqueue-failed'` counts (F7).

## Server-side "done"

- Health on a stock web container reports `analyzable: true`.
- `edge_backfill_state` reaches `complete` for the source; edge count equals the
  published waveform count minus `unsupported`.
- An on-demand request made while a background batch runs is served within
  about one track and never ends `edge-enqueue-failed` or a 5xx.
- A client a day behind a projection rebuild replays changes without an empty
  vector page.

## Phase 1: edge analysis reachable from the web process (F1)

- Worker records its edge runtime (available, PyAV and libswresample versions,
  checked_at) in a small plugin state row, on worker start
  (`ctx.on_worker_start`) and on every edge task.
- Health adds `capabilities.edge_profiles.analyzable: bool|null`: setting on and
  worker reported available within a freshness window; `null` when no worker has
  reported. `available` and `enabled` keep their meaning (answering process).
- Web-side enqueue gates on the setting and the worker state, not on the web
  process's PyAV. The worker task still checks its own runtime.
- Contract answer to Q1: gate analyse/backfill on `analyzable`.

## Phase 2: one priority model for waveform and edges (F2–F8)

### 2a. Saved on-demand demand (decision 4)

1. The route records the demand durably **before** enqueueing:
   waveform `source_profiles.status = 'pending_interactive'` (existing
   `admit_attempts`); edges a new `edge_profile_jobs.priority` column set to
   `interactive`.
2. It then tries `enqueue(..., queue="high")`. If the host refuses because
   another task is active, the rows stay pending and interactive, nothing is
   marked failed, and the route answers 202 with the ids in a new `deferred`
   list. Any other enqueue error keeps today's behaviour (release and error).
   The refusal is detected by the host error code, without importing host
   queue internals.
3. The saved demand is served by whichever runs first:
   - a running background batch (waveform or edge) checks for interactive
     demand before each track and serves it first. Inside a task, enqueue is
     not gated, so it may also hand the demand to a `high` task;
   - the reconcile tick serves interactive demand before any other action;
   - AudioMuse's song hook, when AudioMuse's own analysis is running.
4. Background batches no longer skip a `pending_interactive` row on the
   assumption that a task owns it (F8): the row is either served by the batch
   or left for the interactive drain, never stranded until stale recovery.
5. Response shape: `/api/analyze` and `/edges/analyze` add `deferred: [ids]`;
   `/edges/analyze` also adds `already_pending`.

### 2b. Low-priority server edge backfill (decisions 2, 3)

- Per-source `edge_backfill_state` (cursor, status, counts, next_retry_at),
  modelled on `profile_backfill_state`.
- Run from `catalog_reconcile_task` after the waveform backfill branch; one
  small batch per tick using `backfill_batch_size` (default 3, max 10).
- Re-armed after a catalogue refresh and after new waveform publications;
  completes when no candidates remain; failed jobs follow the existing 6 h
  back-off.
- `/edges/backfill` keeps working (compatibility) and enqueues through the
  same small-batch path.
- Health adds `capabilities.edge_profiles.server_backfill: true`, so the app
  knows it can stop its library walk.
- Terminal `unsupported` status for permanent refusals (for example more than
  two channels), excluded from retries.

### 2c. Priority propagation (F3, F4, F6)

- `claim_edge_jobs` returns `already_pending` and upgrades a pending job to
  interactive instead of dropping it.
- `_schedule_edge_upgrade` inherits the waveform attempt's priority.
- `playback_pending` reads `source_profiles` and interactive edge demand.

### 2d. Audit of the remaining features

Check each against the same rules (on-demand path, background path, on-demand
first, refused enqueue saved): credits (`request_album` priority 10), music
metadata, relationships (`/api/catalog/relationships/prepare`), catalogue
preparation and analysis projection. The last three are whole-library jobs
with no per-track on-demand; confirm that a client-triggered prepare saves its
request when the host refuses it, like 2a.

## Phase 3: status and pacing (Q2, Q3)

- `/api/profiles?ids=` adds `edge_status` per track:
  `ready | pending | failed{retry_at} | unsupported | absent`.
- Health adds edge queue depth (pending, running, interactive).
- Measure per-edge wall time on a host-like fixture (4-min FLAC) and on the
  real server; add `retry_after` to analyse/backfill answers when the queue is
  deep.

## Phase C: vectors after a projection rebuild (F9, decision 1)

- A request for a pruned generation (older than current, no rows, no lease) is
  served from the current generation. The header adds `requested_generation`
  and `generation` (served); every vector keeps its `checksum`.
- A new `missing: [{analysis_id, reason}]` list names absent ids
  (`not_in_generation`, `no_vector`). A page is never silently empty.
- A generation newer than current stays 400. A leased generation is served
  exactly (first full sync unchanged).
- Opt-in `strict_generation: true` returns 410 `generation_expired` with the
  current generation.
- Capability flag for the fallback. Audit the other routes that take a
  generation for the same pattern.

## Tests (every phase)

- Host refusal simulated on both routes: 202 with `deferred`, rows stay pending
  interactive, no job marked failed, next background batch serves them first,
  reconcile tick serves them when nothing runs.
- Pending edge job promoted by an on-demand request; edge follow-up inherits
  interactive priority; credits yield to source-scoped playback demand.
- Edge backfill: small batches, runs after waveform, resumes after a crash,
  re-arms on refresh, `unsupported` never retried.
- Health `analyzable` true/false/null; web without PyAV still enqueues.
- Vectors: pruned generation served from current with checksums, `missing`
  listed, future generation 400, leased generation exact, `strict_generation`
  410.
- Postgres tests wherever SQL changes. Contract (§2, §4, vectors), CHANGELOG,
  STATUS updated.

## Order

Phase 0 → Phase 1 and Phase C in parallel → Phase 2 (2a first) → Phase 3.
