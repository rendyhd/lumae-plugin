# LUM-010 server resumable profile bootstrap

## Scope and contract

The existing `GET /api/profiles`, `/api/profiles/bootstrap`, and
`/api/profiles/changes` remain available. Protocol v2 adds four POST routes:
`/api/profiles/bootstrap/sessions`, `/sessions/page`, `/sessions/catchup`, and
`/sessions/release`. The latter three paths are under the same
`/api/profiles/bootstrap` prefix. Every request declares protocol version 2,
schema version 1, and one explicit `catalog_instance_id`. A session is bound to
one immutable host account subject, kind, and authorization generation, source, current core server mapping, catalog
epoch, profile epoch, and fixed page size. Unauthenticated v2 requests receive
401. The route never uses a remote address as a principal. Responses are
private and `no-store`.

Session creation accepts a page size from 1 to 500, default 250. The server
returns a random session token once and stores only its SHA-256 hash. Page
tokens are HMAC signed with a separate server-only secret persisted beside the
session. They bind session ID, phase, ordinal, and fixed page size. Session
records expire absolutely after 60 minutes. Release is principal-bound and
idempotent. A missing, expired, foreign-principal, or changed-identity session
returns the same 410 error.

Creation returns `catalog_epoch`, `profile_epoch`, `snapshot_cursor`,
`snapshot_seq`, `total_profiles`, and `expires_at`. Snapshot and catch-up
pages repeat the epochs, snapshot cursor, and expiry. The `cursor` and
`snapshot_count` keys remain as aliases. Non-creation requests reject a
supplied `page_size`; the session's fixed size controls both phases.

## Capture and replay

Creation opens a dedicated PostgreSQL connection, takes a session-level
creator lock before opening a fresh `REPEATABLE READ` transaction, then reads
the profile stream frontier and complete published profile/edge rows in that
one MVCC view. Serialized rows are persisted in track-ID order, with an exact
snapshot cursor at sequence S. Neither the host request connection nor a
publication transaction is committed by this flow. The session holds no open
HTTP-spanning transaction. Its pages replay from durable rows, including the
final page, and remain available after catch-up is captured.

The first catch-up request locks its session record, checks the live source and
epochs, and rejects S below the stream floor. It captures one finite head H and
the complete dense interval S < seq <= H, including tombstones and full upsert
payloads, in the same transaction. A gap or limit aborts that transaction.
Concurrent or retried first requests serialize on the session row and reuse the
stored H. Later compaction cannot remove the persisted interval. Page cursors
advance to the last returned event and the terminal cursor equals H; every
page reports the same `head_cursor`.

The owned connection comes from public `plugin.api.open_db_connection()` with
the host role and default search path. Lumae requests a 5-second connect,
20-second statement, and 5-second lock timeout through that API. It commits
or rolls back only its owned transaction and always closes the backend. The
session advisory lock remains on that backend through capture and is released
on close. Snapshot page reads hold a row lock across metadata and payload
reads, so concurrent release or expiry cleanup cannot split a response.
The route returns 503 if the owned connection cannot be established or either
public host API is absent. An absent account principal returns 401; changed
subject or generation returns 410. The unreleased username-bound development
sessions are deleted by an idempotent migration; legacy profile data stays.

## Limits and errors

Creation serializes admission with a PostgreSQL advisory lock: at most four
live sessions per principal and 32 global. It rejects snapshots above 200,000
rows or 128 MiB. Catch-up rejects more than 50,000 events or 128 MiB. Both
limits roll back the whole capture. API errors are bounded to 400
`invalid_profile_bootstrap`, 401 `authentication_required`, 410
`bootstrap_required`, 429 `bootstrap_session_limit`, 413
`bootstrap_snapshot_limit`, and 503 `bootstrap_unavailable`. The implementation
does not change publication, compaction, source identity, or the legacy routes.

## Verification

The focused disposable PostgreSQL 17 suite covers snapshot and catch-up
replay, a finite head during later publication, compaction after capture,
floor rejection before capture, principal/token/version/epoch failures,
expiry and idempotent release, empty snapshots, limit rollback, interrupted
capture rollback, concurrent duplicate capture, and authenticated routing.
The test database is the isolated disposable instance on port 55432. Client
authoritative replacement is a later task.

An initial sandboxed `tests/plugins` attempt hit a Windows temporary-directory
ACL error and produced no reliable tally. The subsequently escalated,
isolated-temporary-directory serial PostgreSQL 17 run on the pre-P1-correction
source completed in 53.06 seconds: 553 passed and 3 failed. The final
post-correction run completed in 68.77 seconds: **558 passed and 3 failed**.
The three failures are the unchanged published 1.2.5 archive/source identity
release gates. The focused four-file PostgreSQL selection passed **54 tests**,
including 13 new LUM-010 cases covering the page/release race, malformed HMAC
signatures, connect timeout, required wire fields, and safe internal errors.
The final focused run includes one test-only bearer-admin rejection regression
added after the full suite. The independent Astra Medium P1 review returned
PASS after the implementation corrections.

## Host identity integration gate

The adapter now targets the reviewed AudioMuse-AI host API at
`3d6b40c8d8417e6907ca8dfc645ea19fd2a552ca`. The host supplies an
immutable account UUID, kind `account`, authorization generation, and an
independently owned unpooled PostgreSQL backend. Bearer-only and auth-disabled
requests have no durable principal and remain 401 before v2 storage. An older
host lacking either API keeps legacy routes importable but v2 responds 503
`bootstrap_unavailable` with reason `host_api_unavailable`. Ordinary process
restart must retain sessions; restore/clone continuity remains a release gate.

The following source assessment records the earlier host baseline only.

The disposable real-host harness is
`docs/remediation/evidence/lum010_host_adapter_integration.py`. Set
`LUMAE_HOST_SOURCE` to the isolated reviewed host worktree at
`3d6b40c8d8417e6907ca8dfc645ea19fd2a552ca` and
`LUMAE_POSTGRES_TEST_DSN` to the disposable PostgreSQL 17 database on
loopback port 44794, then run the script with Python. It verifies that these
paths identify the intended isolated environment, makes a fresh schema and
role default search path, and removes both in `finally`. Its preflight checks
the exact parsed disposable DSN, connected database/user/server address/port,
exact reviewed host SHA, and clean host worktree before any database mutation.
It refuses to change a role that already has a database-specific search path;
cleanup resets only a setting installed by this harness. A misleading DSN
with a query-string host override was rejected under `python -O` before
connection. A separate `python -O` run with a pre-existing database-specific
role search path refused to start and left `search_path=prior_guard, public`
unchanged; that disposable test setting was then reset by the test driver.
Post-run inspection found no temporary schema or role setting.
It imports the real
host `plugin.api` and `app_auth` middleware, loads Lumae under the host plugin
namespace, and exercises the four v2 routes. The run passed account login,
create/page/replay, finite tombstone catch-up, release, cross-account and
recreated-username rejection, role-change continuity, password-generation
rejection with fresh bootstrap, bearer-only 401 without a session write,
independent backend/schema/role, unchanged request transaction, and replay
after a fresh app instance. It does not simulate database restore/clone.

Final adapter validation on the dedicated disposable PostgreSQL 17 instance:
20 focused LUM-010 tests passed. An affected plugin selection passed 133 tests
with 241 deselected before the last two lease-error cases were added. The
final full serial plugin suite passed 566 tests and failed only the three
unchanged published 1.2.5 archive identity tests; the exit status was 1.
Targeted Python compilation and `git diff --check` passed. The host's earlier
98 focused and 268 affected passes remain separate evidence; its full unit
suite has not completed.

Read-only AudioMuse-AI host source at SHA
`f100684b1753e303f2b698b4f56f5b1b8972a5bb` shows `database.get_db()`
returns a psycopg2 connection, while `app_auth.py` sets `g.auth_user=None` for
bearer-authenticated admin requests. The v2 route intentionally requires a
stable authenticated account principal and returns 401 before database access
for this bearer form. A focused route regression preserves that fail-closed
behavior. Bearer-based client adoption requires a separately reviewed host
principal contract, including account change, credential rotation and
revocation. The source inspection does not qualify an installed host or prove
that a second connection works under its runtime credentials.
