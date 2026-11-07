# Test suite audit, 2026-11-06

Scope: `tests/plugins` at `8e64642` (later commits up to `f552cec` touch no tests or plugin code).
Per-test verdicts: [test_verdicts.csv](test_verdicts.csv). Line numbers and the CSV describe the suite as audited,
before any cleanup.

**Status:** steps 1, 2 and 5 below are done, and of step 3 only the `pg_indexes` fix. The 26 tests and the hygiene
items are gone, except where noted under Small hygiene items. Steps 4 and 6 have not been started; they add or
rewrite tests rather than delete them.

## Bottom line

The suite is in better shape than its size suggests. Very little of it is plain waste: 26 tests can go now
(405 lines, about 1 second of runtime), and 15 more probably can (740 lines).

The noise is real, but most of it comes from how tests assert, not from tests that have no value. Of the 55
tests that looked most like noise (exact copy, SQL text against fake databases, minified JS), **38 catch bugs
nothing else in the suite catches**. Deleting those would lose protection, so they need fixing, not removing.

Two things matter more than deleting anything:

- **A real gap.** 19 SQL statements can be made invalid with every test still green, because they only ever
  reach a fake database.
- **A possible speed win, not taken.** `pytest -n 4` took the suite from 8-11 minutes to 3-4, against about a
  second from deleting tests. It needs more than a small fix; see Speed.

## The suite in numbers

| | |
|---|---|
| Test functions / collected on Linux | 1,390 / 2,007 |
| Test code / plugin code | 52.5k lines / 54k lines |
| Serial run | 482-633 s (the machine was busy for some runs, so small differences mean nothing) |
| Needs real Postgres | 802 functions, about two thirds of the runtime |
| CI on `main` | 21 of 114 runs failed (see Noise, item 4) |

## What to do, in order

1. **Delete the 26 and fix the small hygiene items.** Done.
2. **Trim the release tests.** Done. They no longer go red on a plugin commit waiting for its release; `--check` in CI still does.
3. **Fix the three `pg_indexes` tests.** Done. `pytest -n 4` in CI and fake clocks for the wall-clock tests were
   tried and dropped, because they add test code and the goal of this cleanup is less of it. See Speed.
4. **Run the bootstrap, rebind and scope SQL on real Postgres.** This closes the 19-statement gap.
   Delete the fake-based test each new test replaces.
5. **Check the 15 "probably" tests.** Done: 12 deleted, the JS half of one cut, and two kept
   (`test_collection_restore_adds_new_records_and_sync_changes_without_overwrite` and
   `test_snapshot_pruning_preserves_generations_pinned_by_active_leases`), whose rules may not be covered elsewhere.
   The 12 were decided from the sample below, not by rechecking each test's rule.
6. **Loosen copy assertions and split `test_lumae_analysis.py`** when you are next in that code. Splitting an
   11k-line file conflicts with active work, so pick a quiet moment.

## Verdicts

| Action | Tests | Lines | Evidence |
|---|---:|---:|---|
| Delete | 26 | 405 | Bugs injected; see below |
| Delete after checking its rule | 15 | 740 | Caught nothing unique in a small sample |
| Merge with a sibling | 17 | 280 | Reviewer reading only |
| Fix, keep the check | 40 | 1,084 | 38 uniquely catch an injected bug |
| Speed or flakiness fix | 27 | 492 | Reviewer reading; 91 s of runtime |
| Protect | 69 | 2,254 | Listed as most valuable by reviewers |
| No verdict | about 1,200 | | Nothing suggested waste; see Limits |

### Delete now (26)

**How these were checked.** Small bugs were injected into the code each test runs, keeping those the test itself
caught, and then the rest of the suite was run against them.

- 19 tests: the rest of the suite caught all 107 of their bugs.
- 1 test: a targeted check (the accent test, below).
- 6 tests: reading and grep only, because they run no plugin code that could be mutated.
- The full suite passes with 25 of them deselected (1,982 passed). The 26th only matches SQL text.

| Test | Lines | Why |
|---|---:|---|
| `test_lumae_analysis.py::test_plugin_runtime_has_no_private_rq_imports` | 6 | Greps for RQ code that no longer exists |
| `test_lumae_analysis.py::test_v1_0_1_has_no_infinite_plugin_job_timeout` | 5 | Same; the plugin has no `job_timeout` |
| `test_upgrade_fences_postgres.py::test_plugin_version_is_1_3_or_later_and_differs_from_the_1_2_5_release` | 7 | Current version is 1.10.1; only a revert could fail it |
| `test_profile_bootstrap_sql_capture_postgres.py::test_oracle_copy_value_escapes_copy_text_format` | 11 | Tests the test file's own oracle, not the plugin |
| `test_jellyfin_provider.py::test_core_fake_matches_the_production_bridge_shape` | 4 | Tests the fixture |
| `test_lumae_analysis.py::test_apply_biquad_uses_vectorized_scipy` | 16 | Only asserts a mock was called |
| `test_lumae_analysis.py::test_v3_readiness_ignores_obsolete_acknowledgement_settings` | 29 | Its trap cannot fire: the module has no `get_setting` |
| `test_lumae_analysis.py::test_collection_catalogue_search_projection_normalizes_accents` | 6 | SQL text only; 5 real-Postgres tests catch lost accent folding |
| `test_lumae_analysis.py::test_analyze_buffer_produces_waveform_profile` | 16 | Smoke test inside the reference-profile test |
| `test_lumae_analysis.py::test_refresh_failure_preserves_published_readiness` | 18 | Its one assert is in the sibling failure test |
| `test_lumae_analysis.py::test_v3_readiness_admits_future_patch_from_complete_runtime_evidence` | 25 | Same inputs as a parametrized case |
| `test_lumae_analysis.py::test_v3_readiness_does_not_depend_on_historical_task_diagnostics` | 29 | Covered by the test that the query never runs |
| `test_profile_stream_serialization.py::test_edge_first_blocks_legacy_publication` | 51 | The real-publisher test covers both orders |
| `test_profile_stream_serialization.py::test_legacy_and_edge_publishers_share_source_order` | 42 | Same; these two use a hand-built `_publish` |
| `test_profile_retry_postgres.py::test_failure_preserves_published_baseline` | 9 | Same scenario in `test_profile_publication_postgres.py` |
| `test_profile_retry_postgres.py::test_missing_revision_defers_until_known` | 7 | Same |
| `test_profile_bootstrap_postgres.py::test_provisional_sessions_invalidated_by_idempotent_migration` | 23 | A stricter version exists in the operability tests |
| `test_noop_republish_postgres.py::test_an_on_demand_waveform_without_its_edge_leaves_it_to_the_pass` | 13 | Same path as the song-hook test |
| `test_migrated_db_fixture.py::test_plugin_migration_is_idempotent` | 5 | `test_rerun_migrate_leaves_schema_identical` does this on a populated schema |
| `test_runner_routes_postgres.py::test_a_preparation_request_is_claimed_not_queued` | 14 | Same stubs and asserts as a `test_lumae_analysis.py` test |
| `test_runner_routes_postgres.py::test_a_relationship_build_and_a_waveform_pass_are_claimed_not_queued` | 10 | Same |
| `test_background_workers_setting.py::test_the_tick_reads_it_and_falls_back_to_one` | 9 | The sibling covers the same function |
| `test_collection_retention_postgres.py::test_an_in_progress_restore_is_never_deleted` | 11 | `test_a_resumable_restore_survives` does this and more |
| `test_database_diagnostics.py::test_an_autocommit_connection_reads_in_a_transaction_it_owns_and_rolls_back` | 13 | Fake cursor; the real-Postgres test checks the outcome |
| `test_release_channels.py::test_pinned_release_ignores_working_source_changes` | 19 | Always skipped on `main`; a fixture-based test covers it |
| `test_release_channels.py::test_source_mode_keeps_immutability_guard` | 7 | Sibling tests hit the same branch |

`test_migrated_db_fixture.py::test_plugin_migration_is_idempotent` was the only test comparing migration markers,
so that check moved into `test_rerun_migrate_leaves_schema_identical` rather than being lost.

**Do not delete**, though a reviewer said to: `test_journeys.py::test_migration_is_idempotent` and
`test_vibes.py::test_migration_is_idempotent`. Dropping the index creation at `journeys.py:62` or `vibes.py:50`
is caught only by these tests; about 50 other tests run those lines and stay green.

### Probably delete (15, all fake-based or stubbed)

Each caught up to 5 sampled bugs and the rest of the suite caught them all, even with every other test in this
group gone. That sample may have missed the rule a test exists for, so check the rule first.

In `test_lumae_analysis.py`:

- `test_reconcile_profile_batch_reports_track_outcomes_without_batch_retry` (95 lines)
- `test_collection_batch_remove_applies_one_revision_and_one_commit` (94)
- `test_enrichment_change_pages_do_not_read_past_their_pinned_head` (82)
- `test_collection_restore_adds_new_records_and_sync_changes_without_overwrite` (74). A reviewer says the
  no-overwrite rule may not be covered elsewhere.
- `test_atomic_publisher_carries_analysis_and_retains_recovery_evidence` (69)
- `test_relationship_bootstrap_keeps_last_published_generation_visible_while_stale` (59)
- `test_settings_page_exposes_manual_catch_up_and_status` (39)
- `test_refresh_catalog_publishes_complete_generation_and_coverage` (32)
- `test_change_journal_compaction_advances_floor_and_deletes_expired_events` (30)
- `test_snapshot_pruning_preserves_generations_pinned_by_active_leases` (23). A reviewer says real tests only
  cover a lease on the analysis stream.
- `test_find_backfill_ids_applies_limit_after_eligibility_filtering` (20)
- `test_first_scan_leaves_an_unlisted_guest_without_a_portrait` (19)
- `test_scoped_profile_writes_use_catalogue_and_track_composite_key` (18)

Elsewhere:

- `test_workbench_catalog_scope_postgres.py::test_a_mixed_catalogue_collection_streams_and_shows_art_per_item` (65).
  Only its second half, which greps JS, looks removable.
- `test_runner_routes_postgres.py::test_every_track_asked_for_is_accepted_and_none_deferred` (21)

## Where the noise is

1. **Release tests go red between commits.** `test_current_release_contains_supported_source_only` and both
   `test_release_archive_is_identical_across_platforms` cases require the published zip to equal the working source.
   This audit saw it happen: all three failed at `b08814a` and passed after your re-cut. There have been 18
   "(unreleased)" commits and 7 re-cuts since 2026-10-25.
   - `build_catalog.py --check` already enforces this: it fails at `b08814a` and passes at `8e64642`.
   - The same test also has a list of file names edited by hand at each release, most recently in `8e64642`.
     The file changed in 25 commits in 3 months.
   - Keep what `--check` does not cover: md5s of older archives, `PLUGIN_VERSION` against `plugin.json`, and zip
     header determinism.

   **Fixed in step 2.** The source comparison and the file lists are gone, and what they protected is kept. The
   module was checked by breaking a copy of the tree 13 ways:
   - An unreleased plugin commit leaves pytest green and `--check` red.
   - A tampered 1.5.0 archive and `code_zip` losing `create_system`, `external_attr` or its fixed date are caught
     only by these tests; `--check` passes them. So are `code_zip` dropping subfolders or shipping caches.
   - A new fixture assertion replaces the file lists: data files and subfolders must ship.
2. **Exact copy and minified JS.** These fail on harmless edits and cannot catch a runtime defect in the JS, because
   it never runs. But many are the only tests reaching `settings_render.py`, so loosen the assertions in place
   (numbers, states, form actions) instead of deleting them.
   Worst: `test_collection_setting_must_be_enabled_before_manager_is_available`, about 40 JS and CSS fragments;
   `test_page_is_served_beside_living_collections`; `test_plugin_manifest_has_lumae_identity`, about 70 lines
   restating feature lists.
3. **Timing assertions likely to flake.** These assert wall-clock results or thread scheduling, and matter more
   if you run in parallel. `test_reconcile_budget_postgres.py` already has a fake clock (`_Clock`, line 238).
   - `test_reconcile_budget_postgres.py`: `test_a_turn_runs_every_due_action_each_in_a_slice_of_its_own`,
     `test_the_edge_pass_pages_on_until_the_deadline`, `test_the_edge_pass_stops_at_the_deadline_and_gives_each_track_what_is_left`
   - `test_parallel_analysis.py::test_k_files_run_at_once_and_everything_else_stays_on_the_calling_thread`
   - `test_edge_candidates_postgres.py::test_candidates_take_under_a_second_at_the_perf_fixture_scale`
   - `test_shared_pass_postgres.py`: `test_takers_that_race_get_disjoint_batches_and_one_ends_the_pass`,
     `test_a_helper_turn_ends_in_time_to_try_the_lock` (also contains a tautology after its patch)
4. **CI failure history.** Of the 21 failed runs on `main`, the ones I could read from test name and error message
   look like environment or pin noise, not product bugs:
   - one golden, red for 15 straight pushes on PostgreSQL 16 vs 17 row order
   - PyAV version differences
   - a test that only failed under password auth
   - a hard-coded version number
   - a missing `psutil`

   I did not root-cause each one, and this history cannot show bugs tests caught before a push.

### Small hygiene items

Done in step 1:

- Removed dead patches of `par.DRAIN_SECONDS`, which no longer exists, and the unused `broken` helper in
  `test_runner_turn_postgres.py`.
- Inlined 11 one-value `parametrize`s. They were left over from an earlier pruning pass, so the comments saying why
  one value is enough stayed. `test_the_route_writes_nothing` also lost a dead branch and its pinned `== 13`.
- Removed the no-op `_enable_legacy_upsert`, and helpers nothing called: `LimitAwareCursor`, `LimitAwareDb`,
  `_stub_edge_reconcile`, `expect_v3_readiness`.
- Moved the identical `schema`, `App` and `until` from the two runner test modules to `runner_helpers.py`. `fast` and
  `make` stay separate because they really differ (heartbeat, signature).

Not done, because it is not small: `test_lumae_analysis.py` and `test_collection_library_postgres_integration.py`
build their own tables, against the `migrated_db` rule in `conftest.py`. `edge_publication_db`, built on that,
feeds several other modules, so this belongs with step 4.

## The real gap: SQL that never meets Postgres

A test that passes with no database cannot have run SQL on a real server, so this can be measured exactly. Of 843
static SQL `execute` calls in the plugins:

| | Statements | |
|---|---:|---:|
| Runs on real Postgres | 766 | 90.9% |
| Reaches only a fake | 26 | 3.1% |
| Never runs at all | 51 | 6.0% |

The 90.9% is good news. To test the 26, each was corrupted so Postgres would reject it (`SELECT` became `SELEKT`):
**19 stayed fully green**, and the 7 caught were mostly tests matching SQL text.

These queries presumably work in production today. The gap is that a future edit could break them with CI green.

| Function | File | Lines |
|---|---|---|
| `create_bootstrap_session` (behind `/api/catalog/bootstrap-sessions`) | `catalog.py` | 3170, 3171, 3188 |
| `_refresh_catalog_once` | `catalog.py` | 2429, 2434, 2657, 2661 |
| `accept_legacy_rebind` | `catalog.py` | 3590, 3606, 3615 |
| `attempt_legacy_rebind` | `catalog.py` | 3726, 3735 |
| `verify_library_scope` | `catalog.py` | 1045, 1089 |
| `ensure_catalog_sources` | `catalog.py` | 3501 |
| `inspect_catalog_identity` | `provider_identity_guard.py` | 1046, 1099 |
| `next_settled_analysis_run` | `plugins/LumaeAnalysis/__init__.py` | 920 |
| `claim_analysis_run` | `plugins/LumaeAnalysis/__init__.py` | 5661 |

The catalogue bootstrap API stands out: in CI its routes run only with their functions stubbed. The e2e scripts
do not call these routes either, and CI does not run e2e.

Of the 51 that never run, 15 are in `FederatedAlbums`, which is private. Some others look like legacy branches,
for example the non-source-scoped path in `mark_pending`, and may be dead code. I did not establish that.

## Speed

| Option | Time | Status |
|---|---|---|
| Delete the 26 | saves about 1 s | Measured |
| Run fewer fork/exec cases, share audio fixtures, one child per multichannel test | saves about 35-45 s | Reviewer estimate, not measured |
| `pytest -n 4` | 167 s (all passed) and 248 s (1 failed) | **Two runs**, on a 20-core machine |

Most tests have their own database, which is why parallel runs mostly work, but it is not a drop-in change. CI stays
serial for now. Five more `-n 4` runs on 4 shared cores, before any fix, gave one clean run:

- **A race on a fresh database.** One run had 541 errors from workers racing on `CREATE EXTENSION`. CI always starts
  with a fresh database, so adding `-n 4` alone could break it badly.
- **Workers sharing a database.** `test_relationship_build.py` failed with `'coalesced' == 'queued'`, which looks like
  workers colliding on database-wide advisory locks, and two credits deadline tests failed too; neither cause was
  confirmed. Giving each worker its own database gave one clean run in 147 s on 4 cores. That change was about 55 lines
  of test setup and was reverted, and one run proves little.
- **A real isolation bug, now fixed.** `test_vibes.py::test_migration_is_idempotent` failed with `assert 4 == 2`.
  Its `lumae_postgres_db` fixture makes a schema inside the shared database, and the test counted `pg_indexes` with no
  schema filter, so it could see another worker's identical indexes. `test_journeys.py` and `test_covers.py` had the same
  query. All three now filter on `schemaname=current_schema()`.
- **Smaller runners.** GitHub's runners have fewer cores and share them with Postgres, so expect a smaller gain.
- **`fork()` warnings.** Parallel workers add multi-threaded `fork()` warnings (84 warnings against 4 serial) in the
  fork-mode isolation tests, so watch those.

## Leave these alone

Coverage calls many of these redundant, and it is wrong. 912 of 1,390 functions hit no unique line and 517 are
line-for-line subsets of another test, but `test_tech_3341_integrated_loudness`, an EBU conformance test, is one of them.
Line overlap says nothing about these classes, which are the strongest part of the suite:

- Real two-connection races and lock checks (`pg_locks`, `pg_blocking_pids`)
- Query-plan tests (`EXPLAIN`)
- Fault injection at each statement boundary
- Old-client goldens and oracle comparisons
- EBU loudness conformance
- The 1.5.0 failed-ping regression tests

## Decision for you

Three modules keep frozen copies of replaced code as oracles: `test_profile_bootstrap_sql_capture_postgres.py`
(37 s), `test_analysis_projection_equivalence.py` (15 s), and the `_INLINE_*` SQL in
`test_profile_retry_gaps_postgres.py`. The old-client goldens are a similar long-term cost, and each new response
field adds another stripping layer. They are valuable now. Whether and when they retire depends on how long you
support those clients, which the code cannot tell me.

## Limits of this audit

- **Reviewer depth varied.** Seven reviewers read the tests. Some read only part closely: profile stream about 60%;
  audio only the slow and flagged tests; collections about a third. Roughly 400-500 tests were skimmed, so more
  duplicates could exist there. No verdict means nothing suggested waste, not that a test was approved.
- **Reviewers made mistakes.** Checks overruled 3 verdicts: the two index tests above, and a claim that nothing else
  tested accent folding. Merge verdicts and the "protect" list were not checked this way.
- **Injected bugs are a sample.** At most 5-6 caught per test, on the code it reaches most rarely, using simple
  changes (flipped conditions, dropped calls, altered SQL). No lost detection is strong evidence, not proof.
- **"Uniquely catches" is group-relative.** It means no test left after the whole group is gone catches it. For 14
  of the 38 it was a single bug, sometimes coarse (`collection_ui.py:156` returning `None` is caught by three JS tests).
- **Child processes are not traced.** Coverage undercounts the isolation tests.
- **Versions.** Coverage ran at `b08814a`, injected bugs at `8e64642`. Only `undertone_ui.py`, `plugin.json` and
  two test files differ between them.
