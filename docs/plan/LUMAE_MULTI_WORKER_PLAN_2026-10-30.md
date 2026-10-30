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

### R1 Leader and helpers
- The worker role keeps one leader (the advisory lock, unchanged). It alone runs the
  singleton heavy work: relationship builds, pass bookkeeping (start, complete, resweep,
  park), and edge-runtime parking.
- With `every`, a worker runner that misses the lock becomes a **helper** instead of
  standing by. A helper's lane is on-demand work plus the per-track passes
  (`interactive`, `profile_backfill`, `edge_backfill`, `loudness_backfill`) and nothing
  else. It sizes K from its own process, as the leader does, so a Pi takes less than a Ryzen.
- A helper steps aside exactly like the leader: for every `WORKER_YIELD_TASK_TYPES` job,
  and its per-track work pauses while AudioMuse analyses.
- A helper that wins the lock later (the leader went away) becomes the leader through
  today's path.

### R2 Runner identity
- Every runner gets a `runner_id` (host, pid, a start nonce).
- New table `runner_workers`: one row per runner (role, leader or helper, host, pid, K,
  mode, current action, actions, heartbeat). `runner_state` stays as it is for the status
  page and `worker_online`.

### R3 Per-runner claims
- An on-demand claim names its runner, not its role. A claim is released only when its
  runner's heartbeat is gone, by whoever notices (the server's bookkeeping, a new leader).
  A new leader no longer releases a live helper's work.
- Claims written by 1.9.2 (role markers) keep today's release rule, so an upgrade strands
  nothing.

### R4 Shared passes
- **Chunk handout.** A runner takes its next chunk of due track IDs by advancing the
  pass's cursor in one `UPDATE` on the pass row. The row lock serialises takers, so chunks
  never overlap. This replaces the pass-level `running` exclusivity for the edge and
  loudness passes. The leader and the helpers take chunks the same way.
- **A claim per track in every pass.**
  - Edges keep their job claim.
  - Loudness gets a lease per track (owner `runner_id`, 30 minutes, like an edge job).
  - A background waveform admission becomes conditional: it never takes a track whose
    attempt is live and younger than the stale window. Its batch is leased per runner,
    not per source.
  - So no track is analysed twice, whatever the cursors do.
- **Completion by wraparound.** A pass completes only when a walk from the start finds
  nothing due and no chunk is out. A track released by a runner that died or hit its
  deadline is then reached again in the same pass, not after the 6-hour resweep.
- The settings page's counts and ETA read the work tables, as now. The ETA sums K over
  every online runner.

### R5 Edge runtime per process
- A runner without a qualified PyAV skips the edge and loudness passes and leaves their
  state alone.
- Only the leader parks a pass, and only when no runner reported a qualified runtime
  within the fresh window (today's `edge_worker_runtime` rule).

### R6 Files in flight
- A runner takes a slot before each download, from a counted lease table, up to
  `max_files_in_flight`, and gives it back after the file.
- A dead runner's slots go with its heartbeat.

## Steps

Each step has its own commit and leaves the suite green. Nothing changes until a step
reads `worker_runners: every`.

- **MW.0 Spike.** On the e2e harness, start two and three worker containers against one
  Postgres on 1.9.2. Expected: one leads and the others stand by. Measure the leader's
  throughput as a baseline. Check that each container has its own `TEMP_DIR`.
- **MW.1 Identity and helpers (R1, R2).** `runner_id`, `runner_workers`, the helper lane,
  and the setting (`one` by default). Helpers don't take passes yet: with `every` they
  serve only on-demand work.
- **MW.2 Per-runner claims (R3).** Claim markers with `runner_id`, release by heartbeat,
  and the upgrade path for role markers.
- **MW.3 Loudness and waveform claims (R4, part).** The loudness lease, conditional
  waveform admission, waveform batches leased per runner. Single-runner behaviour must not
  change; the existing pass tests prove it.
- **MW.4 Chunk handout and wraparound (R4).** The edge and loudness passes on chunks, then
  helpers take per-track passes.
- **MW.5 Edge runtime and files in flight (R5, R6).**
- **MW.6 Status and docs.** One line per runner on the settings page (host, role, K,
  action, heartbeat, throughput); the summed ETA; `EDGE_PROFILES.md`,
  `LUMAE_SYNC_CONTRACT.md` (additive), README.
- **MW.7 Verification.** See Tests.
- **MW.8 Release.** 1.10.0, through the release process (CI's steps in a clean container
  first, then the Actions run on `main`).

## Tests

- **Unit (fake clock and database).**
  - A worker that misses the lock helps with `every` and stands by with `one`.
  - A helper's lane holds no singleton action.
  - A helper steps aside for AudioMuse jobs.
  - A new leader releases only the claims of runners whose heartbeat is gone.
- **Postgres.**
  - Two takers on one pass get disjoint chunks.
  - A runner that dies mid-chunk: its tracks are done in the same pass, and its on-demand
    claims are released after its heartbeat goes, never a live peer's.
  - A pass completes only when nothing is due and no chunk is out.
  - Two loudness takers never measure one track.
  - Two waveform batches never admit one track.
  - The files-in-flight cap holds across runners.
  - Role markers from 1.9.2 are still released.
- **End to end.** Two and three worker containers on the e2e harness with a seeded
  library.
  - Every runner works, and no track is published twice (journal counts per track).
  - Stopping a worker mid-pass loses nothing.
  - *Start analysis* pauses every runner.
  - `one` behaves like 1.9.2.
- **Full suite.** Under CI's container (Python 3.12, Postgres 17, the `av==17.1.0` edge
  test, `build_catalog.py --check`).

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
