# Lumae Analysis — every worker helps: multi-worker plan (2026-10-30)

## Context

[Issue #10](https://github.com/rendyhd/lumae-plugin/issues/10): a cluster of six AudioMuse
workers (a Raspberry Pi 5 up to a Ryzen 5800X) works through a 130k-track library on one
of them. The report describes 1.8.x (one watchdog task a minute); 1.9.x has the same limit
for a different reason.

What makes 1.9.2 single-worker (all paths at `90a4215`):
- **One worker runner per install.** Each role has one leader, the thread that holds the
  role's advisory lock; every other worker process stands by and tries again every 60 s
  (`runner.py:274-285`, `LOCK_KEYS`). The leader runs the whole heavy lane, K files at once
  (`background_sizing`, `__init__.py:506`; K from that process's cores and memory, 1-8).
- **One row per role.** `runner_state` is keyed by role (`runner.py:78-97`);
  `worker_online` reads that one row.
- **Role-wide on-demand claims.** A claim is marked with its role
  (`RUNNER_CLAIM_MARKERS[role]`). A new leader releases every claim of its role, and the
  server's bookkeeping releases the worker role's claims once its row is 90 s old
  (`release_runner_claims`, `__init__.py:4963`). A second worker runner would release a
  live peer's work.
- **Exclusive passes.** A source's waveform batch is claimed `running` for one runner
  (`claim_profile_backfill_batch`, `__init__.py:6261`); the edge and loudness passes are
  not offered to another runner while `running` (`next_edge_backfill`,
  `edge_profile_store.py:576`; `loudness_backfill.next_due`, `loudness_backfill.py:160`).
  Each walks one cursor per source.
- **Per-track claims differ by pass.** An edge job is an atomic claim with a 30-minute
  lease (`claim_edge_requests`, `edge_profile_store.py:259`). A waveform admission
  replaces a live attempt's token (`admit_attempts`, `profile_publication.py:226`), so a
  second runner would take the track over and the first one's result would be superseded.
  A loudness measurement has no claim at all.

AudioMuse is not the limit: Lumae left its queue in 1.9.0, never writes a `task_status`
row, and steps aside for every AudioMuse job. Spreading work out needs no AudioMuse change,
and must not go back to AudioMuse's queue (a `plugin.*` root task blocks every AudioMuse
start, `task_types.py`).

## Decisions (owner)

- **Default of the new setting.** Recommended: `worker_runners: one` (1.9.2 behaviour) in
  the first release, `every` opt-in, then flip the default once installs report back.
- **Downloads across workers.** Recommended: a cap on files in flight across all runners
  (`max_files_in_flight`, default 16). Each file is the original downloaded from Navidrome
  or Jellyfin, and six workers at K=4 would read 24 files from one NAS at once.

## Design

As built (the first draft's R3 and the loudness lease were dropped; see *Changes from the
first draft*).

### R1 Leader and helpers
- The worker role keeps one leader (the advisory lock, unchanged). It alone runs the
  singleton heavy work: relationship builds, on-demand tracks, the waveform backfill's
  state (queued, complete, retries, stale recovery), loudness's catalogue following, and
  parking a pass when no runner can make it.
- With `every`, a worker runner that misses the lock becomes a **helper** instead of
  standing by (`Runner.can_help`, `__init__._worker_may_help`: `every` and heavy work not
  on the server). A helper's turn (`run_helper_turn`) walks only the per-track passes:
  `profile_backfill` (`helper_profile_batch_task`), `edge_backfill`, `loudness_backfill`.
  It sizes K from its own process, as the leader does, so a Pi takes less than a Ryzen.
- A helper serves no on-demand work (`serve_interactive` returns 0 with `HELPING` set).
  The on-demand claims stay the role's, released by today's rules (R6 of the runner plan).
- A helper steps aside exactly like the leader: for every `WORKER_YIELD_TASK_TYPES` job,
  and while AudioMuse analyses.
- A helper tries the lock every STANDBY_SECONDS: it leads once the leader went away, and
  stands by once helping is switched off.

### R2 Runner identity
- Every runner gets a `runner_id` (host, pid, a start nonce), and `RUNNER_ID` / `HELPING`
  context variables during its turns.
- New table `runner_workers`: one row per runner (role, leader or helper, host, pid, files
  at once, mode, current action, actions, heartbeat), pruned a day after its last
  heartbeat. Only the leader writes `runner_state`, which keeps the status page's role rows
  and `worker_online` as they were.

### R4 Shared passes
- **Batches (`shared_pass.take`).** A runner takes a batch of a few due track IDs (two per
  analysis thread, at most 25) while it holds the pass's row (`FOR UPDATE`), so batches
  never overlap. It hands out first a batch handed back or one whose runner's heartbeat is
  gone (keeping only its tracks still due), then the next due tracks after the cursor,
  which it moves past them. This replaces the pass-level `running` exclusivity for the edge
  and loudness passes; the leader and the helpers take batches the same way.
- **End of a slice (`shared_pass.finish`).** A runner keeps its batches until its slice
  ends, then deletes them, handing back the tracks it did not start or cancelled (and wakes
  the runners). A runner that dies keeps its batches until its heartbeat is gone; then
  another runner takes them over.
- **Completion.** A pass completes only when nothing is due after its cursor and no batch
  is out, inside the same transaction. A pass another runner completed, or one that is
  parked, hands out nothing.
- **Waveform.** The waveform backfill selects due tracks, not a cursor, so it needs no
  batches: a background admission with `keep_live` leaves a track another runner's batch
  admitted since the selection to that attempt (`admit_attempts`), so two batches never
  admit one track. Helpers count what they analysed and leave the backfill's state to the
  leader.

### R5 Edge runtime per process
- A runner without a qualified PyAV skips the edge and loudness passes and leaves their
  state alone (`coalesced`).
- With `every` the leader reports its runtime without parking, and parks a pass only when
  no runner reported a qualified runtime (`edge_profiles_analyzable`) or PyAV
  (`pyav_reported`) within the freshness window.
- A track waits for the edge pass's decode (loudness) when any runner makes edges.

### R6 Files in flight
- Each runner's turn takes up to K slots of `analysis_slots` (`shared_pass.acquire_slots`),
  never more than `max_files_in_flight` across all runners, and analyses that many files at
  once; it gives them back when the turn ends. A runner that went away loses its slots. A
  helper that gets none waits SLOT_WAIT_SECONDS.

### Changes from the first draft
- **No per-runner on-demand claims (draft R3).** Helpers never take on-demand work, so the
  role's claim markers and their release rules stay exactly as in 1.9; nothing to migrate.
  The leader serves on-demand work between its own files, as now.
- **No loudness lease.** Batches are disjoint and a dead runner's batch is taken over only
  once its heartbeat is gone (the heartbeat thread is apart from the turn, so a busy runner
  is never taken for dead); the takeover drops tracks no longer due.
- **No wraparound.** Handed-back and taken-over batches replace it: no track is behind the
  cursor without a batch that holds it.
- **Slots per turn, not per file.** A file's slot would need a wait inside the analysis
  driver; a turn's K slots size its analysis threads instead.
- **Setting change mid-pass.** Switching between `one` and `every` during a pass can let
  one turn of the old mode overlap the new one; edges keep their per-track job claim, so the
  worst case is a loudness track measured twice in that turn.

## Steps

Nothing changes until `worker_runners: every` is set.

- **MW.0 Baseline.** The suite on 1.9.2 in CI's container: green but for one timing flake
  (`test_parallel_analysis.py::test_a_failing_pass_kills_the_files_in_flight_and_finishes_them_cancelled`
  failed once under the full suite's load and passed 3 of 3 alone, on unchanged 1.9.2).
- **MW.1 Identity and helpers (R1, R2).** `runner_id`, `runner_workers`, the helper loop
  and turn, the setting (`one` by default).
- **MW.2 On-demand work stays the leader's** (replaces the draft's per-runner claims).
- **MW.3 Waveform admission (R4).** `keep_live`; the helper's waveform batch.
- **MW.4 Shared edge and loudness passes (R4).** `shared_pass` batches and completion.
- **MW.5 Edge runtime and files in flight (R5, R6).**
- **MW.6 Settings and status.** *Workers* and *Files analyzed at once across all workers* on
  the settings page; one line per worker runner in the background-work panel.
- **MW.7 Verification.** See Tests, and the end-to-end run below.
- **MW.8 Release.** 1.10.0, through the release process (CI's steps in a clean container
  first, then the Actions run on `main`).

## Tests

- `test_multi_worker_runner_postgres.py` (real advisory locks): a worker that misses the
  lock helps with `can_help` and stands by without it; a helper writes its own row and never
  the role's; it leads once the leader goes away; it stands by once helping is switched off
  or the setting cannot be read; old rows are pruned.
- `test_shared_pass_postgres.py`:
  - two takers racing over 40 tracks get disjoint batches and one of them ends the pass;
  - a batch handed back goes first with only its unfinished tracks; a dead runner's batch is
    taken over with its finished tracks dropped; a live runner's batch never is;
  - slots never exceed the cap, also when taken at the same moment, and a dead runner's go;
  - the real edge and loudness passes walked by a leader and a helper at once: each track
    downloaded and published once, the pass complete, no batch left; a runner that dies
    holding a batch loses none of its tracks;
  - a leader's and a helper's waveform batches never admit one track;
  - a helper turn serves no on-demand work, walks only the per-track passes, gives its slots
    back, waits while every slot is taken, and does nothing with `one`.
- `test_multi_worker_settings.py`: the settings, their form and the status lines.
- The 1.9.2 suite unchanged (one-runner behaviour), plus CI's steps.

## Risks

- **The music server becomes the bottleneck.** R6 caps it. The settings page shows
  throughput per runner, so an owner can see it.
- **A slow node holds a chunk.** Chunks are small, and an unfinished tail is released at
  the runner's deadline; wraparound collects it.
- **Postgres connections.** Each runner holds one connection for its lock and LISTEN,
  plus its turn's connections, as the leader does today. Six workers add about twelve.

## Not in scope

- AudioMuse changes, its queue, a child-task API.
- Spreading singleton work (relationship builds, catalogue work).
- Placing work on particular nodes (GPU or not): Lumae's analysis is CPU only.
