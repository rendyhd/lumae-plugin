# RCA: `plugin.lumae_analysis.analysis_projection` shows FAIL (2026-10-06)

**Report.** A user's AudioMuse task list showed `plugin.lumae_analysis.analysis_projection`
as `FAIL` between two `plugin.lumae_analysis.catalog_reconcile` rows marked `SUCCESS`.
The error text was not in the report. Plugin 1.5.0, AudioMuse fork at 033ea4a.

**Fixed in 1.5.1.** Cause 1 below is fixed and has regression tests. Cause 2 is AudioMuse
behaviour; the plugin has nothing to fix there.

## How the row is produced

- The `analysis_projection` schedule is installed **disabled** (`47 */6 * * *`,
  `ensure_analysis_projection_schedule`). Nothing in the plugin queues that task type, so
  the row means someone enabled the schedule on AudioMuse's Scheduled Tasks page. The
  reconcile watchdog projects without it, after every AudioMuse analysis run and every
  catalogue preparation.
- AudioMuse runs a plugin cron task through `plugin.manager.run_plugin_task` with
  `server_scope='all'`: the function runs once per configured server. The row is `FAIL` when
  the function raises on every server, after AudioMuse's retries (`QUEUE_MAX_ATTEMPTS`
  3, backoff about 30 s then 60 s).
- `catalog_reconcile_task` catches its own errors and always returns normally, so its
  `SUCCESS` rows say nothing about the projection's preconditions.
  `analysis_projection_task` raised every error.

## Cause 1: one failed ping held the projection for up to 30 minutes (fixed)

`assert_analysis_projection_allowed` pings Navidrome with a 5 s timeout to notice an
upgrade that changes track IDs. The health route the Lumae app polls pings Navidrome the
same way, and so does every catalogue refresh.

1. In 1.5.0 a ping that failed or timed out, on a source with a published catalogue in
   state `normal`, **stored** `transition_pending` (`observe_provider_version`).
2. The projection raised "Navidrome provider identity is unresolved; the previous Lumae
   analysis projection is preserved".
3. A later successful ping kept the source pending, because an unresolved state is never
   trusted on version alone. Both AudioMuse retries failed the same way, so the row
   ended `FAIL`.
4. Only a catalogue refresh cleared it: the `provider_identity_recheck` cron at minutes
   2 and 32, so up to 30 minutes later.
5. Meanwhile health reported `catalog_sync_allowed: false` and
   `analysis_sync_allowed: false` with a `provider_identity_transition` blocker, so Lumae
   clients paused catalogue and analysis sync.

A unit reproduction (one failed ping, then two healthy ones) failed the projection guard
all three times.

**Fix (1.5.1).**
- A failed ping is not evidence. It stores only `last_error`; state, reason and action
  keep their stored values, so client admission stays open. The settings card shows the
  error, redacted.
- The call that saw the failure still fails closed. It is gated as `transition_pending`
  for its caller only: a refresh still inspects old against new track IDs before it
  publishes, and the projection waits.
- The next verified ping lifts the hold and clears the `provider_version_unverified` /
  `retry_provider_identity_check` values that 1.5.0 could leave behind.
- `analysis_projection_task` reports by-design holds as `status: "deferred"` with a
  `reason` instead of raising: identity unverified or pending, AudioMuse migration not
  ready, no Lumae catalogue on that server, or a catalogue that never published.
  A deferred projection keeps the durable reconcile request.
- A race (the catalogue was replaced mid-projection) still raises, so AudioMuse's retry
  handles it.

## Cause 2: the scheduled run never started (AudioMuse behaviour)

- Plugin tasks block one another's starts (`task_types`: `plugin.` has `blocks_starts`).
- A schedule that finds a live plugin task is put on `cron_retry` and retried every
  `CRON_RETRY_INTERVAL_MINUTES` (10).
- After `CRON_RETRY_MAX_MINUTES` (240) AudioMuse writes a `FAIL` row without running
  anything. Its message is "Scheduled plugin.lumae_analysis.analysis_projection did not run:
  it was blocked for over 240 minutes and never became free.", and `blocked_by` names the
  blocker.
- This happens when the reconcile watchdog keeps a plugin task live at every retry, for
  example during a long edge-profile pass.

The plugin cannot change this. Operators who do not need the extra schedule can disable it.

## Telling them apart

The failed row's details or the container log give the cause:

| Message contains | Cause |
|---|---|
| `provider identity is unresolved` (1.5.0) | Cause 1, or a real Navidrome upgrade awaiting inspection |
| `did not run: it was blocked for over` | Cause 2 |
| `requires one explicit catalogue source` | The server has no Lumae catalogue. Deferred from 1.5.1. |
| `must be complete before analysis projection` | The first catalogue scan has not published. Deferred from 1.5.1. |
| `changed during the analysis projection; retry it` | A race with a catalogue refresh. Still raises. |

## Tests

- Unit tests: `tests/plugins/test_lumae_analysis.py`. See `TransitionRow` and the tests
  named `*failed_ping*`, `*deferred*`, `*defers*` and
  `test_refresh_during_a_failed_ping_still_inspects_ids_before_publishing`.
- PostgreSQL scenario test: `tests/plugins/test_status_summary_postgres.py::test_a_failed_ping_holds_back_one_projection_and_nothing_else`.
  It covers health with a timing-out ping, the scheduled projection deferring, and
  recovery.
