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

**Decisions (owner, 2026-10-25).**
- No AudioMuse change or upstream PR. Plan the full implementation: move Lumae's
  background work off AudioMuse's queue, plus the waste cuts found in the 2026-10-25
  audit.
- **One release, 1.9.0.** 1.9.0 is not published, so it carries everything: journeys
  (JN.10), SQ.1, SQ.2, the waste cuts and the runner. The local release commit `a0272e7`
  (1.9.0 — journeys) stays in history; the last step re-cuts 1.9.0 under the same version,
  as 1.8.0's first test build was replaced (`docs/STATUS.md`, release policy row).

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

Two **runners**, one loop implementation in two roles, neither touching AudioMuse's queue:

| Runner | Started from | Lives | Does |
|---|---|---|---|
| **Server runner** | `on_flask_start` (already used unconditionally, `__init__.py:7288`) | AudioMuse's web process: always up | the light lane: catalogue refresh and preparation, analysis-run finalization and projection, credits, MusicBrainz, the identity recheck, retention, the stored-edge v3 upgrade, taking phone requests |
| **Worker runner** | `on_worker_start` (already used, `__init__.py:7289`) | a worker process: up while a worker is | the heavy lane: the waveform, edge and loudness passes, on-demand track analysis, relationship builds |

Why split (owner, 2026-10-25): many users run AudioMuse's web side on a light server and
the worker on a heavier machine, so heavy analysis stays where the worker is. Both runners
keep G1 (no `task_status` row, so nothing holds the gate) and G2 (nothing is queued, so
nothing strands: with the worker off, heavy work simply waits).

**Where heavy work runs when no worker is online** (setting `heavy_work`, Settings →
Background work):
- `auto` (default): the server runner does on-demand tracks only (a phone is waiting on
  that song; one file at a time); passes and relationship builds wait for a worker.
- `server`: the server runner does all heavy work too, for a server that can take it
  (the owner's `media`: 8 cores, Navidrome local).
- `worker`: never on the server, on-demand included.

A worker counts as online while its runner's heartbeat is under 90 s old (R2).

Checked 2026-10-25 in the owner's web container, for the `server` setting: Python 3.12.3,
PyAV 17.1.0 (the workers' version, libswresample 6.3.101 per `edge_runtime_state`), numpy
2.4.6, scipy 1.16.3, 8 CPUs and no cgroup CPU or memory limit, `TEMP_DIR` with 167 GB free.
The plugin's analysis isolation already has an exec path for a threaded process "the web
tier" (`analysis_isolation.py:34`).

Rejected:
- **One runner in the web process only.** Fine on the owner's server, but it puts heavy
  analysis on light servers.
- **One runner in the worker only.** No catalogue, credits or phone requests while the
  worker is off.
- **Heavy work as short queue tasks when a worker is idle.** A task still holds the gate
  while it runs, so a *Start* during it is refused: the problem this plan removes.
- **A Lumae service container.** A deployment burden for every user of a public plugin.
- **Keep the queue and patch** (the audit's P2–P4). More of the same; P2 needs AudioMuse's
  cancel internals.

What also stays on the worker: the per-song hook (R7).

**Both runners use the same durable work tables and claims** (`claim_profile_backfill_batch`,
`claim_edge_jobs`, the relationship session lock and the rest): which runner picks a unit
up is a matter of lane and setting, not of separate queues.

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
- **One runner per role.** On its raw connection the thread takes
  `pg_try_advisory_lock(<role key>)`, one key for the server runner and one for the worker
  runner, outside the host's keys (`database.py:2657`, `provider_migration_tasks.py:73`,
  `duplicate_repair.py:58-61`, `sql.py:49-51`). Without it, the thread stands by and tries
  again every 60 s. AudioMuse starts one web process (`supervisord.conf:19`: 1 worker, 4
  threads, no `--preload`) but the hook runs once per process; a worker container runs a
  worker process per queue, and a site may run several worker containers (the owner runs
  two). The lock is the guarantee. It dies with the connection.
- **Heartbeat.** Each leader writes its row in `runner_state` (role, host, pid, mode,
  reason, current action, `heartbeat_at`) every 30 s and at each action. The server runner
  reads the worker row to decide where heavy work runs (R1); the settings page shows both.
- **Worker process.** `on_worker_start` runs in each queue worker's main process
  (`taskqueue/worker.py:1286-1288`), which forks a single-threaded child for every AudioMuse
  job. The worker runner's thread keeps no lock across a blocking call and decodes only in
  exec children (`analysis_isolation._Worker`), so a fork while it runs inherits nothing
  held. The worker process already has threads of its own (its listener session,
  `worker.py:139-150`). RN.8 runs AudioMuse analysis jobs while the worker runner works.
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
- **Restore.** AudioMuse has no in-process restore signal: a restore stops the workers
  (a `worker_control` stop, `app_backup.py:1142`) and the whole web process
  (`supervisorctl stop flask`: TERM, then KILL of the process group after 5 s,
  `app_backup.py:450`, `supervisord.conf:20-24`), replaces the schema, and starts them
  again, which runs both hooks again. The runners need no pause for it, only R2's rule
  that every action is idempotent and resumable.
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
and MusicBrainz lookups. After each action it starts again from the top, as today. Each
runner skips the actions outside its lane (R1); the server runner takes heavy actions only
as `heavy_work` and the worker's heartbeat allow.

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
- **The worker runner pauses for every AudioMuse job** (`main_analysis`,
  `main_clustering`, `cleaning`, `provider_migration`, `server_sweep`, `naming_preview`):
  those jobs need the worker's CPU and GPU. It checks before each action and between files,
  so a job that starts meanwhile waits at most one file's analysis for the CPU, never for a
  start (no task row, so AudioMuse admits it at once).
- **Check twice.** Before an action starts, and again inside each publishing transaction
  just before commit; a conflicting job found at commit rolls the publication back and
  the action retries later. In the same transaction the runner also tries the provider
  migration's lock (`pg_try_advisory_xact_lock(7421536190082003)`, the probe AudioMuse
  itself uses, `app_provider_migration.py:333`; the same key at v3.2.0): a migration that
  is executing makes the try fail, and one that starts waits the milliseconds until the
  commit. For cleaning and the sweep the window is those milliseconds against jobs whose
  writes start well after they are queued.

### R5 — The web process

- **By default the server runner does light work only** (R1); heavy work reaches the web
  process only with `heavy_work=server`, or as on-demand tracks while no worker is online.
- **Decoding** runs in exec children (`analysis_isolation._Worker`), `NICE` 10, in both
  runners (both processes are threaded). In the web process `background_workers` `auto`
  means at most 2; on the worker it keeps today's `auto` (cores less 2, at most 4).
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
  bootstrap page and of AudioMuse's `/api/active_tasks` during a full catalogue refresh
  (default settings), and during a relationship build with `heavy_work=server`, stays
  within 100 ms of idle, and the web process's RSS within 300 MB of idle. If the build
  misses it, `heavy_work=server` keeps relationship builds on the worker lane (they wait
  for a worker), and the settings page says so.
- **Relationship scoring is the heaviest in-thread work**: Python loops over per-pair
  `np.dot` (`catalog_enrichment.py:1204`), about 15 minutes of interpreter time per full
  build on the owner's PC (24 Oct: 55 slices, avg 16.5 s). It is in the heavy lane, and W2
  caps it at four builds a day.

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
- **`on_worker_start`** keeps reporting the worker's edge runtime and removing leftover
  downloads, and starts the worker runner (R1, R2). The server runner qualifies the web
  process's own edge runtime before it takes any heavy action. A runner whose own process
  is not qualified leaves the passes to the other.

### R8 — Leaving the queue

- **Schedules.** The install hook deletes Lumae's five cron rows (`catalog_reconcile`,
  `catalog_refresh`, `provider_identity_recheck`, `analysis_projection`,
  `collection_retention`) and their `cron_retry` rows, and no longer registers cron tasks.
  `collection_retention` and the identity recheck become runner actions (W1): retention
  daily, the recheck when a transition is pending.
- **In-flight jobs from the old version.** A job queued by 1.8.0 and run after the upgrade
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
- **`runner_state`**, a new table keyed by role (`server`, `worker`): host, pid, mode
  (`working`, `idle`, `paused`, `waiting_host`), reason, current action, `next_wake_at`,
  `heartbeat_at`. `reconcile_control` stays as it is, unused, so a downgrade to 1.8.0 finds
  what it expects.

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
- **A host without `on_worker_start`**: no worker runner; heavy work follows `heavy_work`
  as if no worker were online.
- **App**: no change needed (R6). Verified by Auralscape's journey suite against the
  end-to-end host.
- **Downgrade** to 1.8.0: its install hook recreates its cron rows (`ON CONFLICT DO
  NOTHING`) and its watchdog reads the old `reconcile_control` columns.

## Waste cuts (either way)

They do not depend on the runner, so they land first (Phase 1), each worth having even if
the runner slipped. All of them ship in 1.9.0 with the runner.

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

**Result (2026-10-25): go.** The pinned stock host (v3.6.3 tree, gunicorn gthread 1×4)
with a real Navidrome 0.64.2 and 25 generated tracks, host auth on, no worker; a scratch
copy of the plugin whose `on_flask_start` started the spike thread (2 threads in the
process at start, 8 while analyzing):

| Step, from the thread | Result |
|---|---|
| `database.connect_raw` (own connection, autocommit) | 22–41 ms |
| `pg_try_advisory_lock` | granted |
| `LISTEN` / `pg_notify` from another connection | delivered in 1–2 ms |
| `with app.app_context(): get_db()`, `list_servers()` | 20 ms; unbound `active_server_id()` is None |
| `use_server` + `get_all_songs` + `download_track` | 73 ms, a 2.6 MB FLAC into `/tmp/lumae-analysis/task-<pid>` (outside `TEMP_DIR`) |
| Waveform analysis through `run_isolated` | exec path (not fork), 685 ms with the child's start |
| `refresh_catalog` in the thread | 60 ms for 25 tracks, generation 1 |
| `plugin_manager.records["lumae_analysis"]`, `restart_pending(<plugins rows>)` | readable; `False` |

The edge call failed only on the spike's fake media revision (`EdgeProfileError`); the edge
pass is covered in RN.8 with real revisions. R5's latency gate needs a library at scale and
is measured in RN.8. The same stack reproduced the bug: with no worker, the boot-time
`catalog_reconcile` stayed `NEW`, and `/api/analysis/start` answered 1201 "Another queue job
(plugin.lumae_analysis.catalog_reconcile) is still running".

### Phase 1 — Waste cuts (in 1.9.0)

- **WC.1** — W2, relationship rebuild spacing.
- **WC.2** — W3, files that cannot be analyzed.
- **WC.3** — W4, credits re-checks.
- **WC.4** — W1 interim, identity recheck only while a transition is pending. Its
  interim cron change never ships on its own (RN.5 deletes the row in the same release);
  it keeps the step independent and testable.

### Phase 2 — The runner (in 1.9.0)

- **RN.1** — Runner skeleton for both roles: lifecycle, lock, heartbeat, wake, sleep,
  pause, `runner_state`, journal. Off by default (setting `runner_enabled`), so RN.2–RN.4
  can land and be tested.
- **RN.2** — The loop (R3) over today's action functions, lanes and `heavy_work`, R4's
  conflict checks, slices.
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
- **RN.8** — End-to-end verification and R5's gate (below).

### Phase 3 — Re-cut 1.9.0

- **RN.9** — `release(lumae_analysis): 1.9.0 — journeys and the runner`, made when RN.8
  passes; published only with the owner's go-ahead:
  - `release-sources.json` back to `{"mode": "source", "version": "1.9.0"}` (SQ.1 pinned
    it to the journeys archive so unreleased work could land);
  - `python scripts/build_catalog.py`, then `--check`: a new
    `dist/lumae_analysis/lumae_analysis_1.9.0.zip` and its checksum in the 1.9.0 entry of
    `plugin.json`, whose changelog now covers journeys, SQ.1, SQ.2, W1–W4 and the runner;
  - `CHANGELOG.md`: the Unreleased entries fold into `## 1.9.0 (date)`;
  - `docs/STATUS.md` release-policy row, the contract's "unreleased" markers;
  - `tests/plugins/test_release_channels.py`: the archive contains the runner module.
- Then the owner's server canary (Verification, below).

### Results (2026-10-25)

Local commits on `main`, none pushed: WC.1 `5fa7175`, WC.2 `3cba046`, WC.3 `9923808`, WC.4
`9536d18`, RN.1 `d2b666c`, RN.2 `90fa048`, RN.3 `e054ead`, RN.4 `496cdc9`, RN.5–RN.6
`2b8347c`, RN.7 the commit that adds this section. The full suite after RN.6: 1,859 passed,
1 skipped, and the 8 known local failures (`test_profile_bootstrap_edge_refs_postgres`,
`test_profile_bootstrap_sql_capture_postgres`, one in `test_profile_retry_gaps_postgres`).

Where the code differs from the steps above:
- **RN.5 and RN.6 are one commit.** The switch and the removal change the same functions;
  switching first would have converted every queue path twice.
- **Kept, though listed for removal:** 1.8.0's `DEFERRED_MARKER`, read beside the runner's
  own `waiting_runner` marker for one release, so on-demand work 1.8.0 saved is served;
  `task_message` and its sentences, for the queue tasks an older version left, which still
  run once; the preparation worker attestation, because a job 1.8.0 queued can still run in
  a worker that has not restarted onto 1.9.0.
- **Removed, though listed as kept:** `action_averages` and `remaining_work`; nothing
  reads them once `TickBudget` and the tick's report are gone.
- **Added:** `catalog_refresh_hours` and `analysis_projection_hours` (an enabled
  `catalog_refresh` or `analysis_projection` schedule, both shipped off, becomes 6 hours, so
  an administrator's choice survives R8); `runner_marks` for once-a-day work
  (collection retention); the server runner's bookkeeping (at most once a minute: interrupted
  events, required preparations, publication summaries and the waveform snapshot, which
  1.8.0's tick did every tick); the per-source server binding of R3 in
  `_run_reconcile_action`; an AudioMuse analysis run wakes the runners once, not once a
  song; a background edge request makes the library pass due, including a finished one; R10's
  `unavailable` row when a host has no `database.connect_raw`; W3's parked-file line, counted
  at most every 5 minutes because the settings panel is polled; the **Heavy work** form on
  the settings page.
- **Found and fixed in RN.6:** with no retry pending, `_work_summary`'s `GREATEST(0, NULL)`
  answered 0 seconds, so an idle runner would have turned every second.
- **Measured:** an idle server turn runs 17 statements, 22 with the bookkeeping (1.8.0's idle
  tick: 28).

**RN.8 ([report](../perf/RUNNER-E2E-2026-10-25.md)).** Verification items 1–5 and 7 pass on
the pinned stock host, including the upgrade from the journeys-only 1.9.0 (`a0272e7`), a
worker killed mid-track and the web container recreated mid-action. Found and fixed, in the
RN.8 commit:
- **A queued AudioMuse job held the runners.** R4 counted `NEW` jobs as live, so *Start
  analysis* with no worker paused the per-track passes until a worker came. Runners step
  aside only for jobs a worker started (`RUNNING_TASK_STATUSES`), the commit check too.
- **A Lumae job an older version left queued still blocked AudioMuse** (1201 with no
  worker, from 1.8.0 and the journeys-only build alike). The install hook and the web
  process's start revoke `NEW` Lumae root tasks (`revoke_stranded_tasks`).
- **A runner that went away kept its on-demand work for an hour** (half an hour for edges),
  until the stale-claim rule freed it. Claims name the runner's role
  (`claimed_server_runner`, `claimed_worker_runner`); a role's new leader puts its role's
  claims back to waiting on its first turn, and the server's bookkeeping does so for a
  worker whose heartbeat is older than 90 s; a held claim makes the server look again
  within a minute.
- **An action that found nothing to do still counted as work.** With MusicBrainz
  unreachable the server runner deferred one album about every 2 s, forever, with a journal
  event each. A turn whose actions moved nothing on sleeps until the soonest retry
  (`_moved_on`).
- **The status page showed some actions only after they ran** (on-demand, credits, lookups,
  the projection, the v3 upgrade). The turn shows each action while it looks for its work
  and does it, and counts the one that ran.
- **The first preparation's by-design hold logged a traceback**; it is a warning.
- **The perf harness** still stubbed `_safe_reconcile_schedule` (`scripts/perf/stub_host.py`).
- **Measured:** an idle server turn with the bookkeeping runs 23 statements. The full suite
  after RN.8: 1,883 passed, 1 skipped, and the same 8 known local failures.

**R5's gate does not pass at scale.** The catalogue refresh runs in the web process (R1),
and at 132k tracks a refresh with no changes took 71 s with `/api/active_tasks` p95 at
421 ms (idle 37) and RSS from 289 MB to 1.36 GB; one that changed every track took 309 s,
the phone's bootstrap page p95 at 334 ms (idle 95) with an 11 s stall, and RSS to 3 GB. At
the owner's 37.5k tracks the everyday refresh passes (16 s; +68 ms, +42 MB), but one that
changes every track (a first install, a rescan that changes every file) does not (77 s;
page p95 395 ms against 94, RSS 239 MB to 992 MB). Relationship builds could not be
measured on the fixture (they wait for AudioMuse's IVF index); their staging pass was over
the gate too. R5's fallback names only relationship builds. RN.9 waits for the owner's
choice: release with the refresh in the web process and the numbers documented, or first
move the in-thread catalogue work (refresh, preparation, identity recheck, projection,
relationship staging) into a child process, as decoding already is.

## Tests

- **Runner unit tests** (fake clock and database): start is idempotent; standby without
  the lock and takeover when it frees; a notify wakes within a second; sleep until the
  soonest due time; pause for each conflicting job type and for maintenance; per-track
  pause during `main_analysis`; the worker runner's pause for every AudioMuse job;
  on-demand work first; slice deadlines; a raising action is journalled and backed off; the
  loop restarts after a crash; an interrupted action recovers.
- **Lanes**: each role runs only its lane; with the worker's heartbeat fresh the server
  runner takes no heavy action; with it stale, `auto` takes on-demand tracks only,
  `server` takes every heavy action, `worker` none; a stale heartbeat older than 90 s
  counts as offline.
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
1. Upgrade from 1.8.0 (the published version) and from the local journeys-only 1.9.0
   build: Lumae's cron rows and `cron_retry` rows are gone; no
   `plugin.lumae_analysis.*` row appears in `task_status` during a full day of runner work.
2. With the worker container stopped: a phone refresh adds a new song; credits progress;
   an on-demand track is analyzed by the server runner (`auto`); passes and relationship
   builds wait, and run on the server with `heavy_work=server`; *Start analysis* is
   accepted at any moment and stays queued until a worker comes.
3. With the worker running: heavy work runs in the worker runner (its heartbeat and
   journal say so); *Start analysis* is accepted while it works; the worker runner pauses
   for the run, the song hook analyzes the new songs, and the server runner finalizes the
   run; AudioMuse's album jobs fork and finish normally while the worker runner's thread is
   alive.
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
- **Decided 2026-10-25:** one 1.9.0 release with everything (was D5, D6); D1–D4 as
  recommended; heavy analysis stays on the worker, with the server doing it only as
  `heavy_work` allows (default `auto`; R1).

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
