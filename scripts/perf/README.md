# Performance harness (`scripts/perf/`)

The P0-3 harness from `docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md`. It seeds a
representative fixture onto the schema produced by the real plugin migration,
runs the benches, writes JSON, and checks the plan budgets. Performance work
packages (plan §1.2 item 5) record before/after numbers from here.

These are harness scripts, not tests: `scripts/perf` is never collected by
pytest (the suite runs `tests/plugins`). They need only what
`requirements-dev.txt` already installs (psycopg2, Flask, numpy); `psql` is
needed only for the `explain_*.sql` files.

The audit probes were **copied**, not moved: the originals stay in
`docs/audit/2026-09-24/probes/performance/` as audit evidence. The copies here
take the DSN from the environment, derive ids and generations from the
fixture, and print one JSON object per run.

## Environment

| Variable | Meaning |
|---|---|
| `LUMAE_PERF_DSN` | **Required.** libpq URL/DSN of a **disposable** database. `seed.py --reset` drops and recreates its `public` schema. |
| `PING_DELAY_S` | Optional delay for the stubbed provider ping in `/api/catalog/health` (default `0`, i.e. health *excluding* the provider ping). |

Every bench connects with the host's per-connection options
(`statement_timeout=600000`, `max_parallel_workers_per_gather=0`), and the
plugin runs against `stub_host.py`, a minimal `plugin.api` host stub with one
Navidrome server, `server-a`.

```sh
psql -h 127.0.0.1 -p <port> -U postgres -c "create database wp_perf owner lumae_test"
export LUMAE_PERF_DSN=postgresql://lumae_test@127.0.0.1:<port>/wp_perf
python3 scripts/perf/seed.py --reset --scale 1          # about 10 min and 5 GB at scale 1
python3 scripts/perf/run_baseline.py --out perf.json    # measure and report every budget
```

## Fixture (`seed.py`)

`seed.py` loads `host_schema.sql` (the AudioMuse host tables, including
`cron`), then runs the real `plugins.LumaeAnalysis.migrate(db)`. Only
`enqueue_required_catalog_preparations` (it would queue jobs) and
`_safe_reconcile_schedule` (an adaptive reschedule from live state) are
stubbed. Every other schedule helper runs for real, because several of them
also create production tables and columns, for example
`ensure_catalog_reconcile_schedule` → `migrate_reconcile` → `reconcile_control`.
Source discovery is real too; it reads the host stub's `list_servers`. This is
fewer stubs than the `migrated_db` test fixture uses, so the benches see the
full production schema. After migrating, `seed.py` bulk-loads the following:

| Data | `--scale 1` |
|---|---|
| catalogue tracks (all eligible) / albums / artists | 132,000 / 12,000 / 6,000 |
| AudioMuse items (`score`, `embedding`, `clap_embedding`) | 69,000 (62k singletons and 7k items mapped twice) |
| mapped provider tracks (`track_server_map`, `chromaprint`) | 76,000 |
| published profiles (`source_profiles`, `published_source_profiles`) | 94,000 |
| edge profiles, real-size (`edge_profiles`) | 94,000 at about 19.5 KB of JSON each |
| retained `profile_changes` events | 50,000 (fixed; `--events`) |
| `task_status` rows | 150,000 |
| analysis projection (first run) | 69,000 items and 132,000 links |

- Edge payloads come from the LUM-010 template
  `docs/audit/2026-09-24/probes/lum010/edge.json`. Each track gets its own ids,
  revision, digest and noise floor, a per-track level offset, and per-bin jitter
  on every packed array. The payload keeps its real size, and TOAST
  compression behaves as it would on real data rather than on 94k copies of one
  document.
- `--scale 0.1` gives a quick run: every count is scaled except the retained
  events, which stay at the production retention of 50k. The publication
  budget is defined at 50k events.
- Other options: `--no-project` skips the first projection, and `--seed N` sets
  the random seed.
- The table `lumae_perf_fixture` records scale, counts and seed, and
  `run_baseline.py` copies that into its results.

## Benches

Each bench prints one JSON line. You can run any of them on its own.

| Script | Measures | Budget key |
|---|---|---|
| `route_bench.py [ROUTES] [N]` | Flask test-client latency of `/api/health`, `/api/catalog/health` and `/settings/status`, plus statements per request | `health`, `settings_status` |
| `pub_bench.py [N]` | Publication critical section: `complete_attempt` from an admitted attempt to the committed publication, which holds the `catalog_state` row lock throughout and includes the journal append and compaction at 50k retained events. It also times `record_profile_change` with and without commit, and the compaction `DELETE` alone. | `publication` |
| `proj_bench.py full\|nochange\|delta` | One `project_analysis` run: elapsed time, statements, WAL, peak RSS (`VmHWM`) and table sizes. `delta` changes one `score` row first. | `projection_nochange`, `projection_delta` |
| `boot_bench.py [--page-size 50] [--max-pages N] [--no-lift]` | v2 `create_session`: time, WAL, snapshot size, and the longest hold of the global creator advisory lock (110094, 10), sampled from `pg_locks` every ~2 ms. Then `snapshot_page` for every page, and the setup cost of the plugin-owned connection. | `bootstrap_create`, `bootstrap_page` |
| `route_floor.py [N]` | Diagnostic: `/api/catalog/health` with its coverage SQL stubbed, i.e. the route's fixed cost | — |
| `boot_concurrency.py [DELAY] [PAGE_SIZE]` | Diagnostic: a second v2 creator started while the first holds the global lock | — |
| `feed_contention.py [N]` | Diagnostic: collections feed append using a sequence vs. the singleton head row, at 1 and 8 writers | — |
| `proj_profile.py [TOP]` | Diagnostic: cProfile of one projection | — |
| `explain_health.sql`, `explain_journal.sql` | `EXPLAIN ANALYZE` of the health/settings SQL and the journal compaction and feed page: `psql "$LUMAE_PERF_DSN" -f scripts/perf/explain_health.sql` | — |

**Bootstrap and the snapshot limit.** With real-size edges, a full snapshot is
larger than `MAX_SNAPSHOT_BYTES` (128 MiB). Current code then fails the create
with `bootstrap_snapshot_limit/413`. `boot_bench.py` records that failure as
the `create` result, which the `bootstrap_create` budget judges. It then
repeats the create with the limits lifted (`create_lifted`), so the full copy
and its pages can still be timed. `pages_from` says which session the page
numbers came from.

**Side effects.** Some benches mutate the fixture, as the real operations
would. `proj_bench.py delta` writes a new projection generation.
`pub_bench.py` republishes N tracks, and those tracks lose their edge
payloads. `feed_contention.py` appends collection-change rows. Re-seed for a
pristine fixture.

## `run_baseline.py`

This script runs the core benches, each in its own process, in this order:
routes, publication, projection no-change, projection delta, bootstrap. If no
projection generation exists yet, it runs a `full` projection first. It
collects the environment (CPU, RAM, PostgreSQL version and settings, git SHA)
and the fixture description, evaluates the budgets, and prints a table.

| Budget key | Plan budget |
|---|---|
| `health` | `/api/catalog/health` ≤50 ms p95, provider ping excluded |
| `settings_status` | `/settings/status` ≤100 ms p95 |
| `publication` | publication critical section ≤5 ms p95 at 50k retained events |
| `projection_nochange` | projection, no change: ≤5 s and ≤400 MB peak RSS |
| `projection_delta` | projection, 1-row delta: ≤10 s |
| `bootstrap_create` | v2 create at 94k: ≤5 s, no global lock held >50 ms (no error) |
| `bootstrap_page` | bootstrap page ≤50 ms p95 server-side |

Options:

- `--out FILE`: write the results as JSON, with environment, fixture, options,
  raw bench output and budget verdicts.
- `--check`: exit 1 if a *selected* budget is missed or was not measured.
  Without `--only`, every budget is selected.
- `--only a,b`: select only these budgets. The others are still evaluated and
  reported, but never fail the run. Unless `--benches` is given, only the
  benches the selected budgets need are run. A later WP asserts only what it
  has fixed, for example `run_baseline.py --check --only publication`.
- `--allow-scale`: judge the scale-dependent budgets on a fixture below full
  scale, reported as "PASS/MISS at scale X". Without it, every budget except
  `publication` is NOT MEASURED on such a fixture (scale < 1 or fewer than
  94,000 profiles), which fails `--check` when that budget is selected.
  `publication` needs ≥50,000 retained events instead.
- `--from FILE`: evaluate an existing results file instead of running anything.
- `--benches LIST` and `--diagnostics`: choose benches explicitly, or add the
  diagnostics.
- `--route-n`, `--pub-n`, `--page-size`, `--max-pages`: control how many
  samples each bench takes.

`health` is NOT MEASURED unless `PING_DELAY_S` was 0. A route that logged an
error through the host logger has rendered a fallback, not the production
path, so it is also NOT MEASURED; `route_bench.py` reports these errors as
`logged_errors`.

Budgets are absolute, so compare runs only on the same machine and at the same
scale. Record the environment with the numbers, as
`docs/perf/BASELINE-2026-09-24.md` does.
