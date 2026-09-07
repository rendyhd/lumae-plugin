# Relationship build performance and recovery

Implemented 2026-09-07 for the timeout reported in
[this specific comment](https://github.com/rendyhd/lumae/issues/9#issuecomment-5561223010).
The changes are in the plugin source; public release manifests and archives are
unchanged. Installing an older published archive does not include this work.

## Input query

`catalog_enrichment._load_relationship_inputs` aggregates artist covers once for
the selected catalogue source and generation, grouping by PostgreSQL `lower(name)`
and selecting `MIN(cover_art_id)`. It joins that materialized result to the tracks.
This preserves the previous case-insensitive, duplicate-name, missing-cover and
generation-scoping semantics without rescanning artists for each track. Track
filtering, analysis-generation joins and deterministic track ordering are unchanged.

The PostgreSQL regression fixture has 8,000 tracks and 2,000 artists. A local
PostgreSQL 17 `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` measurement gave:

| Query | Execution time | Artist table scan loops | Output tracks |
| --- | ---: | ---: | ---: |
| Previous correlated lookup | 4,129.909 ms | 8,000 | 8,000 |
| Grouped cover lookup | 37.004 ms | 1 | 8,000 |

These are synthetic fixture measurements, not a prediction for the reporter's
server. The regression asserts equal results and reduced scan work, rather than
a machine-dependent timing ratio. The previous SQL is frozen in
`tests/plugins/relationship_inputs_legacy.sql` as the compatibility oracle.

## Durable build lifecycle

The additive migration creates three plugin-owned tables:

- `relationship_builds`: one build per source, its input identity, phase,
  completion marker, counts, timings and latest error.
- `relationship_build_tracks`: the normalized input snapshot and track-to-entity
  lookup. MusicNN vectors remain little-endian float32 BYTEA values.
- `relationship_build_entities`: completed fingerprints and scored result
  payloads. Fingerprints use a binary NumPy archive with typed JSON metadata and
  `allow_pickle=False`, preserving vector precision and the album trajectory.

A session advisory lock serializes workers for the same source across checkpoint
commits. It does not lock published relationship rows while computing. A competing
worker coalesces immediately; a disconnected worker releases its lock through
PostgreSQL. The watchdog can reclaim interrupted relationship work after two
minutes without a heartbeat, with the advisory lock still preventing overlap
when a slow worker remains alive.

The first run reads and stages a complete normalized input snapshot. Subsequent
runs reuse that snapshot and skip already-persisted fingerprints. Once fingerprints
are complete, candidate lookup and ranking resume only for unfinished entities.
Completed scoring is committed per entity. Fingerprints checkpoint every 16
entities and at each normal yield.

The default run budget is 128 fingerprint/scoring entities or approximately 20
seconds. It is a cooperative budget: an input statement, checkpoint load or single
bounded entity computation is allowed to finish. After loading checkpoints, a
worker always advances at least one entity, so expensive loading cannot cause
an endless sequence of empty retries. PostgreSQL's existing statement timeout
remains in effect; the plugin does not raise or disable it.

A checkpoint returns `queued`, records a deferred reconciliation event, and keeps
the normal active cadence without adding failure backoff. An unavailable index
retains the input/fingerprint checkpoints and uses the existing deferred-index
retry policy. Ordinary failures retain saved work and use existing failure backoff.

Checkpoints are pinned to source identity, both input generations, both epochs,
algorithm version and checkpoint format version. A changed identity discards the
old scratch build and starts a new one. Partially completed work never becomes
a public relationship generation.

## Publication and cleanup

After scoring finishes, publication takes shared locks on the source, catalogue
and analysis state rows and rechecks the exact input identity. It then locks the
relationship state row and performs set-based journal and result writes. Results,
deletions, cursor, generation and build completion marker commit together.

Candidate lookup, fingerprinting and ranking are outside this transaction. A
two-second transaction-local lock timeout defers publication on contention; the
next run retries the saved results. A generation change also defers publication
and causes the next run to start from the new inputs.

The last complete generation remains readable during preparation or failure.
Existing bootstrap generation checks still reject pages raced by publication.
No progress callback runs inside the publication transaction, since the existing
callback commits its database connection.

Scratch inputs and entity fingerprints are deleted after successful publication.
If cleanup fails, the successful generation stays complete and a later invocation
can retry cleanup. There is at most one retained scratch build per source, not an
accumulating history of library snapshots.

## Diagnostics and verification

The `lumae.relationships` logger records checkpoint phase, counts and cumulative
phase durations at INFO, individual phase timings at DEBUG, and failure phase,
SQLSTATE and primary database error at WARNING. No query parameters or vectors
are logged. `relationship_status` includes `build_progress`; the settings page
shows saved signature, album and artist completion counts. SQLSTATE also appears
in the failure message copied from the settings panel.

Tests run against a dedicated disposable PostgreSQL database using
`LUMAE_POSTGRES_TEST_DSN`. The CI workflow already provisions PostgreSQL 17 and
runs the entire plugin test directory, including this new module.

```powershell
python -m pytest tests/plugins/test_relationship_build.py -q -s
python -m pytest tests/plugins -q
python scripts/build_catalog.py --check
```

Coverage includes the real input query and query plans, binary fingerprint/ranker
equivalence, scoped candidate publication, fingerprint and worker restart recovery,
generation/epoch changes, publication-time revalidation, missing indexes, real SQL
statement timeouts, competing workers, lock contention, atomic rollback, preserved
bootstrap data, unchanged and deleted deltas, cleanup failure, progress callbacks
that commit, and a time budget exhausted by checkpoint loading.

The reported server's actual timeout has not been reproduced: this verifies the
query optimization and recovery behavior independently using controlled data.

Final local validation: **506 plugin tests passed**, including **23 targeted
relationship tests** with PostgreSQL enabled. Plugin/script compilation,
release-catalog validation and `git diff --check` also passed.
