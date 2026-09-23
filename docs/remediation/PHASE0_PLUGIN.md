# Phase 0A — Plugin current-state adjudication

Audit date: 2026-09-22
Repository: `lumae-plugin`
Starting/current SHA: `4276398c671f3e2713212488f4e72a32553697d7`
Worktree at audit start: clean (`git status --short` produced no entries).

This is a source and baseline-test audit only. It makes no production, network, or
user-data changes. “Existing test” below never means that a mocked or single-
connection test proves a required concurrency invariant.

## Environment and baseline inventory

* Python dependencies are declared in `requirements-dev.txt`: Flask, NumPy,
  pytest, psycopg2-binary, requests, SciPy and psutil. `requirements-edge.txt`
  additionally requires PyAV 16.1.0 for the optional edge contract.
* `pytest tests/plugins --collect-only -q` collected **397 tests** before the
  disposable-PG suite expanded its conditional integration parametrization.
* PostgreSQL tests gate on `LUMAE_POSTGRES_TEST_DSN` in
  `tests/plugins/test_lumae_analysis.py:48-55` and
  `tests/plugins/test_collection_library_postgres_integration.py:16-20`; absent
  DSN skips use the explicit reason “set LUMAE_POSTGRES_TEST_DSN to run
  PostgreSQL integration tests.” Individual PG suites also use isolated
  schemas, although the collection-library fixture drops its fixed test schema.
* Default command attempted (with isolated `--basetemp`, cache and pycache):
  `python -m pytest tests/plugins -q`. It began normal execution and displayed
  the expected skipped PG markers, but the Windows runner returned no final
  pytest tally. Do **not** treat it as a pass. An initial `--cache-clear`
  attempt was blocked by an ACL on the pre-existing repository `.pytest_cache`;
  it did not remove anything. The rerun used only `.pytest-tmp/phase0a-*`.
* Disposable PG baseline command used (full interpreter path,
  `C:\Users\rendy\AppData\Local\Python\pythoncore-3.14-64\python.exe`):
  `LUMAE_POSTGRES_TEST_DSN=postgresql://lumae_test:disposable_remediation_only@127.0.0.1:55432/lumae_remediation python -m pytest tests/plugins -q -r s --basetemp .pytest-tmp/phase0a-plugin-pg -o cache_dir=.pytest-tmp/phase0a-plugin-pg-cache`.
  The persisted stdout/stderr log and JUnit report show **436 passed in
  40.82s, 0 skipped, exit code 0**. Artifacts are
  `docs/remediation/evidence/plugin-baseline.log`,
  `docs/remediation/evidence/plugin-baseline.junit.xml`, and
  `docs/remediation/evidence/plugin-baseline.exitcode.txt`. The DSN was
  process-only and points to an explicitly disposable PostgreSQL 17 container
  (`lumae-remediation-pg-20260922`, tmpfs, no production mounts). PG is
  therefore available for targeted regressions. The earlier interactive runner
  progress stopped at 82% before its buffered final output arrived; the
  persisted artifacts are the authoritative result.
* No AudioMuse host integration, Android/iOS/device execution, real worker-kill,
  decoder-stall, or production-like large-library benchmark was available in
  this repository audit. Those remain gates.

## Finding dispositions and evidence

| ID | Disposition | Current-source evidence and adjudication |
|---|---|---|
| LUM-001 | PRESENT | `catalog_enrichment.py:110-126` reads/creates `profile_stream_state` without `FOR UPDATE`; `record_profile_change` then derives `head_seq + 1` at `349-375`. `__init__.py:1568-1663` writes profile, change, and commits in one transaction, but two connections can read the same head and collide. No two-connection profile journal/rollback/legacy-vs-edge regression exists. |
| LUM-002 | PRESENT | Collection update reads revision at `collection_manager.py:729-739`, then unconditional `revision = revision + 1` update at `747-754`; item writes have the same pattern at `823-842`. Neither locks nor compares expected revision in the update predicate. No independent-connection one-200/one-409 test exists. |
| LUM-003 | PRESENT | `_mutation_response` reads receipt before handler (`466-481`) then commits receipt separately after the handler has committed (`484-497`); create itself commits at `691-692`. Receipt has no request fingerprint/operation binding. Lost response can leave mutation without receipt. |
| LUM-004 | PRESENT | Collection feed is `seq > cursor ORDER BY seq` (`975-1001`) with no committed frontier. Changes use serial sequence insertion (`441-449`); sequence allocation alone cannot order commit. No late-commit/cursor regression exists. |
| LUM-005 | PRESENT | `loudness.py:32-39` uses fixed K-weight coefficients and `_integrated_lufs` only applies an absolute per-chunk gate (`112-117`); no versioned, independently-qualified integrated-LUFS contract or reference comparison exists. Local tests establish numerical/block invariance, not standards qualification. |
| LUM-006 | PRESENT | SQL applies its `LIMIT` after SQL eligibility (`__init__.py:3371-3412`), but it selects a ready profile whose stored signature is NULL through `IS DISTINCT FROM` (`3397-3400`). Python then rejects that same row because its mismatch branch requires both `current_sig` and `stored_sig` (`3274-3290`); `find_backfill_ids` applies that second predicate after the SQL limit (`3415-3436`). Repeated NULL-signature rows can therefore consume a batch and starve later rows. Existing `test_find_backfill_ids_applies_limit_after_eligibility_filtering` only covers eligible missing/stale rows (`3575-3593`), not this SQL/Python disagreement. |
| LUM-007 | PARTIALLY_FIXED | Retryable stale/skipped/no-file paths and stale-pending recovery exist (`3274-3290`, `4106-4121`), and repaired signatures requalify ready rows. Ordinary failed profiles are excluded unless an explicit prepare selects `include_failed` (`3402`, `3596-3608`); no failure taxonomy/backoff/revision-reenable proof for repaired failed media exists. |
| LUM-008 | PARTIALLY_FIXED | Profile changes are emitted with upsert/delete semantics (`349-387`) and publication occurs with profile write (`1560-1663`). There is no separate durable published-validity state: transition to pending occurs elsewhere without a corresponding publication transition, while bootstrap filters only `status='ready'` (`390-404`). Existing and clean-bootstrap clients can therefore diverge during ready→pending→failed. |
| LUM-010 | PARTIALLY_FIXED | Server endpoint and opaque paging exist at `__init__.py:2452-2493`; page token includes source/epoch/head at `catalog_enrichment.py:424-465`. It is not a durable server session, and rows are selected by current ready state rather than constrained to the pinned head. Client durable staging/resume is outside this repo and unverified. |
| LUM-011 | PRESENT | Routine `/api/catalog/health` observes provider versions with commits (`__init__.py:1773-1815`) then invokes V3 readiness per source (`1852-1871`); readiness executes full coverage and link aggregate queries (`catalog_readiness.py:123-208`). No cheap committed summary or measurement exists. |
| LUM-012 | PRESENT | Projection has no recorded no-change/small-delta/full-rebuild benchmark or representative fixture. Existing small-unit assertions do not measure allocation, writes, transaction duration, or memory at the reviewed scale. |
| LUM-013 | PRESENT | Workbench browse silently chooses `ORDER BY s.is_default ... LIMIT 1` in `collection_library.py:28-37`; browse API has no catalogue scope argument. |
| LUM-014 | PRESENT | Track rows retain `album_id` (`collection_library.py:38-44`), but album browse groups title/artist and explicitly returns `provider_album_id: None` (`115-153`), conflating same-name editions. |
| LUM-015 | PRESENT | The browse source projects `NULL::INTEGER AS year` (`collection_library.py:38-42`), while year sort is advertised and used (`13-14`, `120`, `164`, `192`). |
| LUM-016 | PRESENT | Browse uses `%token%` `LIKE` (`96-112`), `COUNT(*) OVER()` and `LIMIT/OFFSET` for albums/tracks/artists (`115-221`). There is no EXPLAIN ANALYZE evidence or large-library stable-pagination implementation. |
| LUM-017 | PARTIALLY_FIXED | Settings separates catalogue/projection/profiles/relationships in the UI (for example `__init__.py:4743-4931`) and catalogue admission separates analysis (`catalog_readiness.py:238-305`). No independently reported provider/personal state matrix or client qualification is present. |
| LUM-018 | PRESENT | `enqueue_bounded` explicitly discards its timeout argument (`__init__.py:211-219`). Local tests cover analyzer cooperative deadline/resource checks, but not host cancellation, revocation, worker death, or decoder stall. |
| LUM-019 | PARTIALLY_FIXED | `runtime/README.md:3` correctly says Radio DJ is retired and tests cover retired routes/migration. Root `README.md:47-52` still documents opt-in DJ analysis and dedicated-worker enablement as current release behavior, so release-specific documentation is inconsistent. |
| LUM-020 | PRESENT | Transaction ownership remains distributed: route handlers commit in `collection_manager.py` (e.g. `691-692`, `759-760`, `965-966`), receipt wrapper commits separately (`484-497`), and orchestration is concentrated in the very large `plugins/LumaeAnalysis/__init__.py`. Structural cleanup is intentionally deferred until behavioral regressions exist. |
| LUM-021 | PARTIALLY_FIXED | Settings already exposes action/phase/retry and bounded journal (`__init__.py:5050-5155`) and profile API distinguishes ready/missing/failed (`2413-2449`). It lacks a defined diagnostics contract for safe operation/error codes, bootstrap/resume reason, per-stream outcomes, stage timings, and imported/changed/published definitions. |

## Test inventory and missing qualification gates

The suite includes useful mocks and some independent PG tests for shelves,
personal discovery, credits, and relationship build. It does **not** contain the
required LUM-001 two-connection profile publisher regression; LUM-002/LUM-003
concurrent revision and lost-response replay regressions; LUM-004 late-commit
feed regression; LUM-005 external loudness reference vectors; LUM-010 durable
profile resume/kill and concurrent publication coverage; or LUM-011/012/016
representative-size benchmark and plan evidence. Passing mock tests cannot
substitute for those concurrency and host-boundary proofs.
LUM-006 additionally needs a NULL stored-signature row before an eligible later row
under a bounded batch, proving that SQL and Python admit exactly the same rows.

## First bounded diagnostic recommendation

Before changing behavior, add a disposable-PostgreSQL LUM-001 regression using
two independent connections against `profile_stream_state`/`source_profiles`:
publish different tracks, force the second allocator to overlap the first,
exercise rollback, then assert contiguous committed changes and exactly one
advanced head. Run the same invariant through the legacy and edge-profile
publication paths. This establishes the P1 transaction boundary before any
locking design is selected.

## Feasibility note for Task 0B diagnostics

The proposed bounded diagnostics work is feasible as a separate task. The database-state presentation layer currently truncates raw exceptions, so it can be replaced with stable operation IDs, a conservative error class and validated SQLSTATE while preserving source-scoped counts. Bounded server-side timing can be added around query execution plus fetch only, explicitly labelled as such; it must not imply transport, worker, decoding or client timing. Required regressions should inject a raw SQL/path/token-like exception and assert no verbatim data reaches the response, and use a fake monotonic clock to assert the reported operation timing. Raw SQL, parameters and exception text must remain absent from all response and log payloads.
