# Lumae Analysis — off AudioMuse's queue: runner plan (2026-10-25)

## Context

AudioMuse refuses every start (the owner's *Start analysis*, its own schedules, the
phone's refresh requests) while any `plugin.*` root task is live, and a task that is only
queued counts as live (`database.get_queue_blocking_task`, host v3.6.4
`database.py:2830`; the `plugin.` prefix has `blocks_starts=True`, `task_types.py`).
Every piece of Lumae's background work is such a task: the watchdog's ticks, its other
schedules, and the jobs the web routes queue.

The [2026-10-11 review](../audit/2026-10-11/LUMAE_REVIEW_2026-10-11.md) found this as
AUD-13. The [scheduler plan](LUMAE_SCHEDULER_PLAN_2026-10-11.md) (S1, S2) and the
[performance plan](LUMAE_PLUGIN_PERFORMANCE_PLAN_2026-10-16.md) (WP1) made the ticks
shorter and fewer, yielded to AudioMuse's retries, and served refused on-demand work from
inside ticks and the song hook. Each fixed a symptom and added a moving part. None can fix
the two remaining failures, because the plugin cannot see either:
- **No worker.** The owner's only worker is a GPU PC that is often off. The first Lumae
  schedule that fires after it goes away queues a task nobody runs, and that task blocks
  AudioMuse until a worker returns or someone cancels it.
- **A person's click.** A refused *Start* leaves no trace, so the yield (S1) never sees it.

**Decision (owner, 2026-10-25).** No AudioMuse change or upstream PR. Plan the full
implementation: move Lumae's background work off AudioMuse's queue, plus the waste cuts
found in the 2026-10-25 audit.

This plan supersedes the cadence and yield parts of the scheduler plan (S1, S2, S5's
cadence reporting) and WP1's tick budget. Its host-compatibility rules still apply: floor
3.2.0, detect features at runtime, fail open, never write into the host's directories
(LUMAE_SCHEDULER_PLAN_2026-10-11.md:32-53).

## Evidence (audit of 2026-10-25)

Sources: AudioMuse's web log on the owner's server since its restart (2026-10-18 07:38 to
2026-10-25 04:06, about 164.5 h; host image `pr947v3-948v3` on v3.6.3, plugin 1.8.0); the
plugin's tables, read-only; host source v3.6.4.

| Finding | Numbers |
|---|---|
| Stranded tasks | `provider_identity_recheck` queued 19 Oct 09:02, still `NEW` at 20 Oct 12:52 (27.8 h); queued 20 Oct 21:02, still `NEW` when the owner cancelled it on 24 Oct 21:44 (96.7 h). **124.5 h of 164.5 h (76 %)** in which AudioMuse refused every start. |
| No worker then | The owner's *Start analysis* at 24 Oct 21:44:44 stayed `NEW` until 21:53; the workers qualified their edge runtime at 19:53:57 UTC (21:53:57 local). Before 19 Oct 09:02 the hourly ticks ran until 08:11. |
| Phone requests refused | 63 catalogue refreshes refused by the stranded recheck (19–24 Oct); 2 by `main_analysis`; 3 by Lumae's own refresh or tick. |
| Idle load | 73 root tasks a day with nothing to do: 24 hourly ticks (`11 * * * *`), 48 rechecks (`2,32 * * * *`), 1 retention. |
| Active load | About 11 h of minute cadence in the week, about 50 ticks an hour; about 8 % of ticks ran past the next minute (28 of 369 on 18 Oct). |
| One analyzed song | The hour after a one-song analysis run (24 Oct 20:00 UTC): 187 actions, 1,725 s of work. A full relationship rebuild (55 slices over 32 min, avg 16.5 s; generation 45: 2,310 albums, 1,094 artists, 37,483 tracks); `analysis_run` finalization 64 s. |
| Bad files | Loudness spent 192 s and 112 s on 8 and 6 files, all failed: four 1–2 GB FLACs that hit `MAX_SECONDS` (6553.6 s), the rest `InvalidDataError`. Each is retried every 72 h forever (`RETRY_HOURS = (1, 6, 24, 72)`), downloaded whole each time. |
| Credits re-checks | 922 re-checks due at once (all `not_before` 24 Oct 20:07). Every tick spends what is left on them, about 1.5–2.5 min an hour while idle; sampled results `unresolved`, 0 credits. 654 complete, 692 unresolved, 47 empty. |
| Backfills | Waveform, edge and loudness passes complete (37,470 loudness tracks). |

## Goals

- **G1.** Lumae's background work never holds AudioMuse's gate: no `plugin.lumae_analysis.*`
  root task from background or on-demand work.
- **G2.** Nothing strands when the workers are off; catalogue, credits, relationships and
  the phone's requests keep working with only the web process up.
- **G3.** A phone request is served within seconds, with no 409, `deferred` or
  `Retry-After` for a busy queue.
- **G4.** No wasted work: no full relationship rebuild per song, no endless retries of a
  file that cannot be analyzed, no credits waves.
- **G5.** Same outputs and wire contract for the app; an app that knows today's answers
  keeps working.

Non-goals: AudioMuse changes; what Lumae computes; the per-song hook's analysis; Jellyfin
specifics beyond keeping them working.

## Design

### R1 — Where the work runs

A **runner**: one loop thread in AudioMuse's web process, started from the plugin's
`on_flask_start` hook, which the plugin already uses unconditionally
(`__init__.py:7288`).

Why the web process:
- It is always up (on the owner's server, `media`), so G2 holds without the GPU PC.
- It never touches the queue, so G1 holds by construction.
- The plugin's analysis isolation already has an exec path for a threaded process "the web
  tier" (`analysis_isolation.py:34`).
- Checked 2026-10-25 in the owner's web container: Python 3.12.3, PyAV 17.1.0 (the workers'
  version, libswresample 6.3.101 per `edge_runtime_state`), numpy 2.4.6, scipy 1.16.3,
  8 CPUs and no cgroup CPU or memory limit, `TEMP_DIR` with 167 GB free. Navidrome runs on
  the same host, so downloads stay local.

Rejected:
- **A thread in the worker** (`on_worker_start`). Also ends gate holding and stranding, but
  work happens only while the PC is on, and it shares processes with AudioMuse's analysis.
- **A Lumae service container.** A deployment burden for every user of a public plugin.
- **Keep the queue and patch** (the audit's P2–P4). More of the same; P2 needs AudioMuse's
  cancel internals.

What stays on the worker: the per-song hook (R7).

### R2 — Lifecycle

Host facts below are from AudioMuse v3.6.4, each checked against v3.2.0 (research of
2026-10-25; host paths are relative to the AudioMuse repository).

- **Start.** `on_flask_start` calls `runner.start()`: idempotent, starts a daemon thread
  `lumae-runner` (every host thread in the web process is a daemon, `app.py:1446-1555`) and
  returns. The hook runs during `app` import, before AudioMuse loads its indexes and
  starts its own threads (`app.py:1240-1555`), and a blocking hook delays all of that and a
  restore's restart (`app_backup.py:606-624`). The thread waits 120 s before its first
  action, as the host's blob reclaim does (`config.py:858`). The hook captures the Flask
  app object (`current_app._get_current_object()`) for the thread.
- **Imports.** The thread imports every plugin module it will use before its first
  action: an upgrade replaces the plugin's files and purges its modules while the process
  keeps running (`plugin/manager.py:639-712`), so a later lazy import would mix versions.
- **Database.** `get_db()` lives on `flask.g` and needs an app context
  (`database.py:212-236`); the hook's context belongs to the boot thread and ends with it.
  So the runner uses two kinds of connection, the two patterns the host's own threads use:
  - **its own raw connection** (`database.connect_raw(application_name="lumae-runner-<pid>")`,
    autocommit, as `taskqueue.listen.Listener` and the blob reclaim do,
    `listen.py:67-80`, `maintenance.py:402-425`) for the lock and `LISTEN`, rebuilt after
    an error. `connect_raw` exists at v3.2.0 (`database.py:1672`), without the later
    `read_only` keyword; the plugin does not pass it.
  - **one `with app.app_context():` per pass**, so every action's `get_db()` is a fresh
    connection that the teardown closes (`app.py:349-357`), as the cron and dashboard
    threads do (`app.py:1486`, `app_dashboard.py:934-957`).
  - Every action commits or rolls back before the next: AudioMuse sets no
    `idle_in_transaction_session_timeout`, its blob VACUUM stands down while any
    transaction is older than 30 s (`maintenance.py:347-354`), and an uninstall's
    `DROP TABLE … CASCADE` waits on the runner's locks (`database.py:2636-2653`).
- **One runner.** On its raw connection the thread takes `pg_try_advisory_lock(<lumae
  runner key>)`, a key of its own outside the host's (host keys: `database.py:2657`,
  `provider_migration_tasks.py:73`, `duplicate_repair.py:58-61`, `sql.py:49-51`). Without
  it, the thread stands by and tries again every 60 s. AudioMuse starts one web process
  (`supervisord.conf:19`: 1 worker, 4 threads, no `--preload`), but the hook runs once per
  process, so the lock is the guarantee. It dies with the connection.
- **Wake.** Work is recorded in the existing durable tables; whoever records it also runs
  `pg_notify('lumae_runner', <reason>)` in the same transaction. The runner `LISTEN`s on
  its raw connection, so a wake from a web route, the worker's song hook (which has an app
  context, `tasks/analysis/album.py:501`) or another process arrives within a second.
  In-process callers also set a `threading.Event`.
- **Sleep.** With nothing ready, the runner waits for a wake, the soonest `next_retry_at`
  or `not_before` in the work tables, or a safety sweep every 15 minutes, whichever is
  first. A sweep is a few statements (S8's idle-cost test stays and is re-pinned).
- **Pause.** Maintenance pause (`maintenance_paused()`) and a conflicting AudioMuse job
  (R4) stop new actions; the runner reports why.
- **Restore.** AudioMuse has no in-process restore signal: a restore stops the whole web
  process (`supervisorctl stop flask`: TERM, then KILL of the process group after 5 s,
  `app_backup.py:450`, `supervisord.conf:20-24`), replaces the schema, and starts it again,
  which runs the hook again. The runner needs no pause for it, only R2's rule that every
  action is idempotent and resumable.
- **Plugin disabled, uninstalled or upgraded.** No hot reload: each needs a web restart
  (`restart_required`, `plugin/blueprint.py:488-538`), and *Apply* restarts the web process
  (`:584-600`). Meanwhile the old thread would keep running old code. Before each pass the
  runner checks that the plugin manager still has the plugin loaded and enabled
  (`plugin_manager.records`) and that its boot checksum still matches the database
  (`restart_pending`, `manager.py:901-910`). If not, it finishes the action in hand,
  releases its lock and stops; the restarted process starts the new code's runner.
- **Stop.** The thread is a daemon and dies with the process; an action cut off mid-way
  is recovered by the existing durable state (running rows go stale, events become
  `interrupted`, `reconcile._recover_interrupted_events`).
- **Errors.** Each action runs inside its own guard. An exception is rolled back and
  journalled, the action backs off by the existing retry ladder (`update_work_retry`), and
  the loop continues. A failure of the loop itself restarts it after 30 s, doubling to 15
  minutes.
- **Config.** AudioMuse reloads its `config` module after setup, a migration or a
  default-server change (`config.py:1805-1808`). The runner reads `plugin.api.config.X` at
  use, never a copy taken at start.

### R3 — The loop

One pass runs the actions in today's priority order (`_next_reconcile_action`,
`__init__.py:1405`): on-demand work, the stored-edge v3 upgrade, analysis-run
finalization, catalogue preparation, a wanted catalogue refresh, the credits and
MusicBrainz share turns, relationships, waveform, edge and loudness passes, then credits
and MusicBrainz lookups. After each action it starts again from the top, as today.

- **Slices.** Each action gets at most `SLICE_SECONDS` (15 s) and stops at its deadline, as
  the elastic actions already do. Actions that today run past the tick (`analysis_run`
  64 s, a single long file) may keep doing so: nothing waits on them now except on-demand
  work, which goes first in the next pass.
- **Pacing.** `PAUSE_SECONDS` (1 s) between actions, so request threads get the
  interpreter.
- **Continuous.** While anything is ready, passes run back to back. There is no cadence,
  no cron, no tick budget.
- **Per source.** Every pass covers every active catalogue source (today's
  `every_source`). The server binding is a context variable that a new thread starts
  without (`tasks/mediaserver/context.py:28-84`); unbound calls fall back to the default
  server. So each source's actions run inside `with use_server(<its server_id>):`, inside
  the pass's app context (`registry.bind` reads the database when called,
  `registry.py:337-358`). This replaces the binding the queue gave a cron task
  (`server_scope='all'`, `plugin/manager.py:958-983`) and `_reconcile_dispatch`'s
  once-per-dispatch guard.

### R4 — Stepping aside for AudioMuse

Without the gate, Lumae keeps AudioMuse's mutual exclusion itself. AudioMuse's admission
counts only `task_status` rows, so its cleaning and migration will not wait for the
runner; the runner waits for them:
- **Pause everything** while an AudioMuse job that rewrites or deletes track IDs or
  mappings is live (`NEW` or `RUNNING`, root rows; `config.py:28-43`):
  - `cleaning` (prunes `track_server_map`, may delete orphan `score` rows);
  - `provider_migration` (rewrites the default server's mappings; live until the workers
    acknowledge the restart);
  - `server_sweep` (writes and prunes `track_server_map`).
  `provider_migration_planner` is read-only and does not pause the runner. The startup
  relabel and duplicate repair run during `app` import, before the hook
  (`app.py:364-481`).
- **Pause per-track audio work** (waveform, edge, loudness passes) while `main_analysis`
  is live. The song hook analyzes the run's songs itself, so this avoids analyzing a song
  twice and avoids racing the hook's writes. Everything else (catalogue, credits,
  relationships of settled data) runs on. Finalization already waits for the run to
  settle (`_work_summary`, 2-minute grace).
- **Check twice.** Before an action starts, and again inside each publishing transaction
  just before commit; a conflicting job found at commit rolls the publication back and
  the action retries later. In the same transaction the runner also tries the provider
  migration's lock (`pg_try_advisory_xact_lock(7421536190082003)`, the probe AudioMuse
  itself uses, `app_provider_migration.py:333`; the same key at v3.2.0): a migration that
  is executing makes the try fail, and one that starts waits the milliseconds until the
  commit. For cleaning and the sweep the window is those milliseconds against jobs whose
  writes start well after they are queued.

### R5 — The web process

- **Decoding** runs in exec children (`analysis_isolation._Worker`), `NICE` 10, as today in
  a threaded process. The web tier's default `background_workers` becomes 2 (auto still
  available); the hook does new songs on the PC, so the runner's audio work is a trickle
  after the backfill.
- **Downloads.** `tasks.mediaserver.download_track` has no role guard and works in the web
  process with the server bound (`tasks/mediaserver/__init__.py:122-155`; AudioMuse
  itself never downloads audio there). The runner downloads into a plugin-owned temporary
  folder outside `TEMP_DIR`, which every analysis run empties at its start
  (`tasks/analysis/main.py:167-174`), and removes its files itself (a provider can leave a
  partial file, e.g. `jellyfin.py:296-307`). `analysis_download.task_directory`'s
  `task-<pid>` naming and `remove_leftovers` move with it.
- **In-thread work** (catalogue refresh, projection, relationship scoring, credits,
  metadata) shares the interpreter with AudioMuse's 4 request threads (gunicorn: 1
  process, 4 gthreads, `deployment/supervisord.conf:19`).
- **Gate (RN.0 and RN.8).** On the end-to-end host, the p95 latency of the phone's
  bootstrap page and of AudioMuse's `/api/active_tasks` during a relationship build and a
  full catalogue refresh stays within 100 ms of idle, and the web process's RSS within
  300 MB of idle. If relationship scoring misses it, its scoring moves into an exec child
  (inputs in, scores out; the parent writes), the pattern the audio analysis uses.
- **Expect relationship scoring to need that child.** It is Python loops over per-pair
  `np.dot` (`catalog_enrichment.py:1204`): about 15 minutes of interpreter time per full
  build on the owner's PC (24 Oct: 55 slices, avg 16.5 s). W2 caps it at four builds a
  day.

### R6 — On-demand requests

Every web route that queues work today records it in its durable table, notifies the
runner (R2) and answers at once. The seven `enqueue_bounded` call sites (inventory §1):

| Route | Today | With the runner |
|---|---|---|
| `POST /api/catalog/refresh` (`catalog_refresh_api`, `__init__.py:3494`) | queues `catalog_refresh_task`; busy: `202 deferred host_busy`, `Retry-After: 60` | `refresh_wanted_at` (SQ.1) and notify; `202 {"status": "queued"}`, or `200 fresh` when the catalogue is newer than asked |
| `POST /api/catalog/prepare` (`:3429`) | claims, then queues `prepare_lumae_task`; errors swallowed | claim and notify; `202` with `Location` and `Retry-After: 2` as today (that one polls the operation, not the queue) |
| `POST /api/analyze` (`:4162`) | queues `analyze_tracks_task` on `high` in chunks of 3; busy: ids in `deferred` | `pending_interactive` rows and notify; every id in `accepted`, `deferred` always empty |
| `POST /api/profiles/edges/analyze` (`:4400`) | queues `analyze_edges_task` on `high`; busy: ids in `deferred` | interactive edge jobs and notify; `deferred` always empty |
| `POST /api/profiles/edges/backfill` (`:4413`) | queues a background page; busy: `edge-enqueue-failed` | arms the edge pass and notifies |
| `POST /settings` (`prepare_lumae`, `start_backfill`/`catch_up`/`queue_all`, `retry_edge_profiles`, `retry_relationships`; `:7104-7237`) | queue or swallow | record and notify |
| `POST /api/catalog/relationships/prepare`, `/api/credits/prepare`, `/api/music_metadata/prepare` | already record and `arm_reconcile` | record and notify |

- The runner serves interactive rows first in every pass (R3).
- No route calls `enqueue`, so none can be refused or strand a task.
- **Contract (§2.5, §3.6).** The `deferred` lists in `/api/analyze` and the edge routes
  stay in the response, always empty; the refresh's `deferred`/`host_busy`/`Retry-After:
  60` is no longer sent. Both stay documented as answers from older plugins. The app already treats `queued` as "check
  again in two minutes" (Auralscape `catalogueRefreshFollowUp.ts`, SQ.4) and still reads
  `deferred` from older plugins.
- **Health** (`__init__.py:2866-2895`): `edge_profiles.analyzable` and `worker_runtime`
  describe the runner's process; `deferred_requests` becomes `pending_requests`, with the
  old name kept one release.

### R7 — What stays on the worker

- **The song hook** (`analyze_song_hook`, `__init__.py:5417`) keeps analyzing each song
  while AudioMuse has its file. It records the analysis run and notifies the runner. It no
  longer serves deferred on-demand work (`serve_deferred_interactive(limit=1)`): the
  runner serves it, analysis or not.
- **`on_worker_start`** keeps reporting the worker's edge runtime (the hook needs it) and
  removing leftover downloads. The runner qualifies the web process's own runtime at start
  and records it under the web host's id, which is what gates the passes.

### R8 — Leaving the queue

- **Schedules.** The install hook deletes Lumae's five cron rows (`catalog_reconcile`,
  `catalog_refresh`, `provider_identity_recheck`, `analysis_projection`,
  `collection_retention`) and their `cron_retry` rows, and no longer registers cron tasks.
  `collection_retention` and the identity recheck become runner actions (W1): retention
  daily, the recheck when a transition is pending.
- **In-flight jobs from the old version.** A job queued by 1.9.x and run after the upgrade
  calls its dotted function. Each removed task function stays one release as a shim that
  notifies the runner and returns.
- **Removed** (inventory §2–§4):
  - cadence: `reconcile_schedule_from_state`, `_set_schedule_locked`, `set_paused`'s cron
    write, `yield_cron`, `lumae_row_minutes`, `avoid_minutes`, `cron_minutes`,
    `_backoff_cron`, `BACKOFF_LADDER` and the cron constants (`reconcile.py:18-37`,
    `:175-299`, `:439-528`), `_safe_reconcile_schedule` (`__init__.py:967`);
  - `arm_reconcile` (`reconcile.py:299`) becomes `runner.notify(reason)` at its 15 call
    sites;
  - `HostYield` (`reconcile.py:1165`), replaced by R4's check; the `cron_retry` reads;
  - `TickBudget`, `TICK_BUDGET_SECONDS`, `ELASTIC_ACTIONS`, `MAX_TICK_ACTIONS`,
    `EDGE_BACKGROUND_BUDGET_SECONDS`; the `deadline` parameters stay, set by R3's slice;
  - `_reconcile_dispatch` and `same_dispatch` (`__init__.py:1254-1288`): the runner loops
    the sources itself;
  - `DEFERRED_MARKER`, `serve_deferred_interactive` and its seven call sites,
    `_wake_for_deferred`, `host_queue_busy`/`HOST_BUSY_ERROR_CODE`,
    `mark_edge_jobs_deferred`/`claim_deferred_edge_jobs` (`__init__.py:4307-4979`,
    `edge_profile_store.py:321-345`);
  - `enqueue_bounded` and `enqueue_profile_analysis`, `enqueue_edge_profiles`,
    `enqueue_next_profile_backfill`, the `enqueue_job` parameters, the unused
    `queue_backfill_batch` and `queue_whole_library`;
  - `TickReport`'s Recent-tasks message shaping and `task_message`
    (`reconcile.py:1000-1150`);
  - the settings page's schedule guide, cadence strings and "disabled in Scheduled Tasks"
    warnings (`settings_render.py:1128-1344`);
  - preparation worker attestation (`assert_preparation_worker_current`,
    `worker_version_mismatch`, `__init__.py:6708-6732`, `:3169-3363`): it exists because a
    queued worker could run older code than the web process; the runner runs the web
    process's own code.
- **Changed**: `_own_download_refused` (`__init__.py:2514-2559`) is a per-process memo
  reset only at a tick's start; in the long-lived runner it expires after 10 minutes.
- **Kept**: the durable work tables and their claims, the event journal
  (`reconcile_events`, now one row per runner action), `action_averages`, and the
  `task_status` reads that watch AudioMuse's jobs (`next_settled_analysis_run`,
  `inspect_audiomuse_health`, `jellyfin_continuity.audiomuse_busy`), which R4 reuses.
- **Not in scope**: `plugins/FederatedAlbums` has its own queue use (`__init__.py:502`,
  `:1138`, `:1466`, `:1516-1521`); it is a separate plugin.
- **`reconcile_control`** becomes the runner's row: `mode` (`working`, `idle`, `paused`,
  `waiting_host`, `standby`), `reason`, `next_wake_at`, `leader`, `heartbeat_at`. The
  `mode` CHECK constraint is replaced in an additive migration; `cron_expr` stays, unused,
  for a downgrade.

### R9 — What people see

- AudioMuse's task list no longer shows Lumae's work.
- Lumae's settings page shows the runner: mode and reason ("waiting for AudioMuse's
  cleaning"), leader host, current action, the journal's last 25 actions, remaining work
  and estimates.
- Logs: a warning for a failed action or a lost lock; nothing per action at INFO.

### R10 — Compatibility

- **Host floor 3.2.0.** Everything the runner uses exists there unchanged: `get_db` and
  its teardown, `connect_raw`, `taskqueue.listen.Listener`, the server context and
  registry, `download_track`, the plugin manager's records and `restart_pending`, the
  migration lock key, the job types R4 watches. `task_types.py` arrives in v3.6.0; R4
  names its types itself. Two of these are not in `plugin.api.__all__` (`database.connect_raw`,
  `tasks.mediaserver.download_track`); each is imported at runner start, and a missing
  one stops the runner with a status-page reason and a log line, never a crash.
- **App**: no change needed (R6). Verified by Auralscape's journey suite against the
  end-to-end host.
- **Downgrade** to 1.9.x: its install hook recreates its cron rows (`ON CONFLICT DO
  NOTHING`) and its watchdog reads the old `reconcile_control` columns.

## Waste cuts (either way)

They do not depend on the runner. Shipped first, they cut the churn while the PC is on;
they do not stop stranding.

### W1 — Identity recheck without a schedule

- The check runs when a provider transition is pending: after the start-up observation
  (`observe_provider_identities_on_start`), after each catalogue refresh, and once a day.
- Interim (before the runner): the watchdog runs it as an action only while a transition
  is pending; the install hook moves the cron row to daily when it still has the shipped
  `2,32 * * * *` (an administrator's own schedule is kept, as
  `ensure_provider_identity_recheck_schedule` promises). With the runner: the row is
  deleted (R8).

### W2 — Relationship rebuilds at most every few hours

- A change (analysis run, catalogue change, the phone's prepare request) requests a build
  with `not_before = max(now + 15 min, last completed + 6 h)`; `next_relationship_run`
  respects `not_before` for `queued` (today only for `failed`/`waiting_for_index`,
  `__init__.py:914`).
- The settings page's *Rebuild now* forces it.
- SQ.2's "finish the generation it started" stays.
- The phone shows the last published set meanwhile (Auralscape SQ.3).

### W3 — Files that cannot be analyzed

- **Too long:** before downloading, a track whose catalogue duration exceeds `MAX_SECONDS`
  (6553.6 s, `loudness_v1.py:63`, `edge_profiles.py:46`) is recorded as `too_long` for its
  media revision, without a download.
- **Retries:** after the last step of `RETRY_HOURS`, a failure parks until the track's
  `media_revision` changes; no 72-hour loop. The same rule for edge jobs.
- **Decode errors** (`InvalidDataError`, `edge-media-error`) park after two attempts per
  revision.
- The status page lists parked files with their reason.

### W4 — Credits re-checks without waves

S7 stepped the intervals but albums first checked together still expire together
(`expires_at = now() + ttl`, `credits_store.py:302-305`).
- Expiry gets ±25 % jitter.
- Re-checks run only when nothing else is ready, at most 60 an hour.
- `unresolved` and `empty` re-check after 30, then 90, then 180 days (was 7, 30, 90);
  `complete` stays at `COMPLETE_TTL_DAYS`.
- A one-off migration spreads today's due re-checks over 14 days.

## Steps

One local commit per step in lumae-plugin, tests in each. Nothing is pushed or released
without the owner.

### Phase 0 — Prove it (no release)

**RN.0 — Spike on the end-to-end host** (`scripts/e2e`, AudioMuse v3.6.3, Navidrome).
A throwaway runner thread in the web process that takes the lock, `LISTEN`s, binds a
server, downloads and analyzes one file through the exec path, runs one relationship slice
and one catalogue refresh. Measure R5's gate. Exit: the numbers in this plan and a go or
no-go.

### Phase 1 — Waste cuts (can ship as 1.9.1 with 1.9.0, SQ.1 and SQ.2)

- **WC.1** — W2, relationship rebuild spacing.
- **WC.2** — W3, files that cannot be analyzed.
- **WC.3** — W4, credits re-checks.
- **WC.4** — W1 interim, identity recheck only while a transition is pending.

### Phase 2 — The runner (1.10.0)

- **RN.1** — Runner skeleton: lifecycle, lock, wake, sleep, pause, status row, journal.
  Off by default (setting `runner_enabled`), so RN.2–RN.4 can land and be tested.
- **RN.2** — The loop (R3) over today's action functions, R4's conflict check, slices.
- **RN.3** — Web routes record and notify (R6); contract §2.5 and §2.6.
- **RN.4** — Song hook: notify, no deferred serving (R7); the runner's edge runtime.
- **RN.5** — Switch: runner on; the install hook deletes the cron rows; shims for
  in-flight jobs (R8); W1's final form.
- **RN.6** — Remove the queue machinery (R8) and the `runner_enabled` setting.
- **RN.7** — Settings page and status (R9). Docs: the contract (§2.5, §2.6, §3.6, the
  cron mentions), `CATALOG_SYNC.md`, `EDGE_PROFILES.md`, `README.md` (38-41, 92-98,
  158-159), `docs/STATUS.md` (E2–E4, PERF, F2, P3-6, new rows for SQ and RN),
  `docs/relationship-build-recovery.md`, CHANGELOG; a "superseded by" note at the top of
  the scheduler plan, its handoff, the AudioMuse plugin-tasks plan and the performance
  plan's WP1; this plan's results.
- **RN.8** — End-to-end verification and R5's gate (below); release commit 1.10.0 when the
  owner approves.

## Tests

- **Runner unit tests** (fake clock and database): start is idempotent; standby without
  the lock and takeover when it frees; a notify wakes within a second; sleep until the
  soonest due time; pause for each conflicting job type and for maintenance; per-track
  pause during `main_analysis`; on-demand work first; slice deadlines; a raising action is
  journalled and backed off; the loop restarts after a crash; an interrupted action
  recovers.
- **PostgreSQL tests**: two runners, one lock; `LISTEN`/`NOTIFY` across connections; the
  publication check rolls back when a conflicting job appears; the idle sweep's statement
  count pinned; migrations of `reconcile_control` up and down.
- **Rewritten tests**: about 120 tests center on the queue and about 50 touch it
  (inventory §7). Whole files that become runner tests or go:
  `test_reconcile_scheduler_postgres.py`, `test_host_compatibility_postgres.py`,
  `test_reconcile_budget_postgres.py`, `test_reconcile_cadence.py`,
  `test_reconcile_report.py`, `test_task_paths.py` (re-pinned to the shims). Partly:
  `test_catalog_refresh_wanted_postgres.py`, `test_edge_priority_postgres.py`,
  `test_throughput_panel_postgres.py`, `test_music_metadata_watchdog_postgres.py`,
  `test_parallel_passes_postgres.py`, `test_idle_cost_postgres.py`,
  `test_bundle_paths_postgres.py`, `test_status_summary_postgres.py`,
  `test_profile_retry_postgres.py`, and about 34 tests in `test_lumae_analysis.py`. The
  shared fixtures (`conftest.py:133-175`, the fake `plugin.api` in
  `test_lumae_analysis.py:21-39`) and `scripts/perf/stub_host.py` change with them.
- **Two loops at once**: no plugin-wide lock serializes actions today; the host's gate
  does. A PostgreSQL test runs two runners and the song hook against one database and
  checks that the advisory lock and the per-action claims (`claim_profile_backfill_batch`,
  `claim_preparation_run`, `claim_analysis_run`, `claim_edge_jobs`, the relationship and
  publication advisory locks) keep every action single.
- **Waste cuts**: rebuild spacing and forcing; `too_long` without a download; parking and
  un-parking on a new revision; jitter spread and the hourly cap; the 14-day spread.

## Verification (end to end)

On the end-to-end host, with the release archive mounted:
1. Upgrade from 1.9.x: Lumae's cron rows and `cron_retry` rows are gone; no
   `plugin.lumae_analysis.*` row appears in `task_status` during a full day of runner work.
2. With the worker container stopped: a phone refresh adds a new song; credits and
   relationships progress; *Start analysis* is accepted at any moment.
3. With the worker running: *Start analysis* is accepted while the runner works; the
   runner pauses per-track work until the run settles, then finalizes it.
4. A cleaning job: the runner pauses, then resumes.
5. Restarting the web container mid-action: the runner resumes, nothing is lost or done
   twice.
6. R5's latency and memory gate.
7. Auralscape's journeys (`npm run test:e2e:journeys`) pass.

On the owner's server (the owner installs), for a week: no AudioMuse refusal that names a
Lumae task in the web log; no Lumae rows in `task_status`; the runner's journal shows its
work; a new song reaches the phone with the PC off as soon as Navidrome has it.

## Owner decisions

- **D1** — The runner in the web process (recommended) or in the worker.
- **D2** — Relationship spacing: 15-minute quiet period, 6 hours between builds.
- **D3** — Credits re-checks: 30/90/180 days, 60 an hour.
- **D4** — The web tier's default analysis workers: 2.
- **D5** — Ship Phase 1 as 1.9.1 before the runner (recommended).
- **D6** — The runner release's number: 1.10.0 (recommended; the wire contract stays
  compatible) or 2.0.0.

## Risks

- **The web process's interpreter is shared.** R5's gate and its fallback.
- **AudioMuse's exclusion is now Lumae's job.** R4; the residual window at commit.
- **A runner that cannot start** on some host leaves Lumae without background work; the
  status page and log say so. There is no fallback to the queue: two schedulers are what
  made this janky.
- **Memory or a native crash** in the web process: decoding stays in children; the runner
  catches per action; RSS is part of the gate.
- **Work is invisible in AudioMuse's task list.** The settings page carries it.

## Traceability

| Audit finding (2026-10-25) | Fixed by |
|---|---|
| Stranded queued task blocks AudioMuse | R1, R6, R8 |
| A click on *Start* refused while a tick runs | R1 |
| 73 idle root tasks a day | R2, R8, W1 |
| Minute cadence, ticks past a minute | R3 |
| Full relationship rebuild per song | W2 |
| Endless retries of unanalyzable files | W3 |
| Credits waves | W4 |
| Phone refreshes refused for days | R6 |
