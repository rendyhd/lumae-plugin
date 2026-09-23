# LUM-010 server resumable profile bootstrap

## Scope and contract

The existing `GET /api/profiles`, `/api/profiles/bootstrap`, and
`/api/profiles/changes` remain available. Protocol v2 adds four POST routes:
`/api/profiles/bootstrap/sessions`, `/sessions/page`, `/sessions/catchup`, and
`/sessions/release`. The latter three paths are under the same
`/api/profiles/bootstrap` prefix. Every request declares protocol version 2,
schema version 1, and one explicit `catalog_instance_id`. A session is bound to
one authenticated `g.auth_user`, source, current core server mapping, catalog
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

The owned connection copies connection parameters and current schema from
`plugin.api.get_db()` without committing or rolling back host work. It has
finite connection, statement, and lock timeouts. Snapshot page reads hold a
row lock across metadata and payload reads, so concurrent release or expiry
cleanup cannot split a response across two committed session states. This
requires a psycopg2 host connection
whose connection parameters permit a second connection; host qualification
must verify that contract before release. The route returns 503 if the owned
connection cannot be established.

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
