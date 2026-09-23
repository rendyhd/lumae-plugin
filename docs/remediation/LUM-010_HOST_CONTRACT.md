# LUM-010 host contract qualification

## Status and evidence boundary

**HOLD current-host qualification.** This is a read-only source assessment of
AudioMuse-AI at `f100684b1753e303f2b698b4f56f5b1b8972a5bb`, not a tested
contract on an installed host. No host change, deployment, database operation,
or production action is authorized by this document. Source paths and line
numbers below refer to that host revision. Lumae source references refer to
the local LUM-010 checkpoint lineage.

The supported, author-facing host surface is `plugin.api`: its module docstring
calls it the only module plugins should import, and it exports `get_db` and
`config` (`plugin/api.py:9-19,28-61`). Flask `g.auth_user`, `g.auth_method`,
`database.connect_raw`, and connection internals are observable host
implementation details, not supported principal or owned-connection APIs.

## Identity observed today

| Request condition | Host observation | LUM-010 v2 consequence |
| --- | --- | --- |
| Valid account cookie | JWT `sub` is a username; the host verifies signature, expiry, current user row, password-change time, and current database role. It sets `g.auth_user` to that username and `g.auth_method='session'` (`app_auth.py:572-626,643-669`). | Current plugin binds `user:<username>` (`plugins/LumaeAnalysis/__init__.py:2592-2606`). This works in focused plugin tests, but account lifecycle on the actual host is unqualified. |
| Valid installation bearer with no valid cookie | One configured `API_TOKEN` compares against the bearer value. The request is admin-equivalent with `g.auth_user=None` and `g.auth_method='bearer'` (`app_auth.py:671-682`; `config.py:954-966`). | v2 returns 401 before database access. A bearer credential is not an account subject. If both credentials are sent and the cookie is valid, the earlier cookie branch authenticates the account instead (`app_auth.py:661-669`). |
| Wrong, malformed, removed, or old rotated bearer | No bearer identity is accepted; an API request gets 401, unless a valid cookie authenticated earlier in the barrier (`app_auth.py:661-686`). Setup saves configuration, refreshes it, and requests restart (`app_setup.py:695-699`); disabling auth deletes the token (`app_setup.py:634-645`). | No lease can be created or resumed through that bearer. There is no observed multi-token registry, rotation grace, token ID, or account mapping. Exact cross-process cutover timing is unverified. |
| Auth disabled | Barrier defaults to admin role with no user or auth method (`app_auth.py:653-660`). | v2 returns 401 because there is no personal subject. |
| Deleted, password-changed, or expired account session | Expiry and a missing user row reject the cookie; role comes from the current user row. Password-change time and JWT issue time are compared at second precision (`app_auth.py:177-187,572-626`). | A password change or username delete/recreate within the same second can leave an old cookie valid for the reused username. Current `user:<username>` can then cross an account boundary. Account v2 remains on HOLD pending immutable subject and generation semantics; the current plugin alone cannot establish them. |

The existing collection manager deliberately maps bearer access to shared
`__global__`, account cookies to `user:<username>`, and an auth-disabled request
to shared state (`plugins/LumaeAnalysis/collection_manager.py:24,60-87`). That
legacy collection scope is separate from v2 profile-bootstrap authorization.
It cannot supply a personal v2 principal or turn a shared installation bearer
into an account. Installed plugin pages are reachable by authenticated users;
the manager and plugin settings are admin-gated (`app_auth.py:690-724,765-811`).

## Database connection observed today

`plugin.api.get_db` is the supported database entry point. It returns a
psycopg2 connection cached on Flask `g`, with a 30-second connect timeout,
keepalives, a ten-minute statement timeout, and parallel query plans disabled
(`database.py:66-91`). The host closes it at app-context teardown
(`database.py:94-97`; `app.py:227-229`). The API does not promise that plugin
code owns its transaction or may close it. The host constructs `config.DATABASE_URL`
from PostgreSQL configuration or an environment override (`config.py:403-420`).

The private `database.connect_raw()` opens an independent psycopg2 connection
for boot-time core callers (`database.py:1562-1575`). It is absent from
`plugin.api.__all__` (`plugin/api.py:53-61`). No public plugin factory or lease
specifies independent transaction ownership, search path, session role,
isolation, timeout, cancellation, pool behavior, or cleanup. Runtime values of
these settings on an installed host remain unknown.

Current Lumae v2 reads `current_schema()` on the host request connection,
copies DSN parameters and `host_db.info.password`, then opens and closes a
second connection (`plugins/LumaeAnalysis/profile_bootstrap.py:123-164`). The
schema read can start or participate in a transaction on the host connection;
it does not commit or roll it back. The clone sets its own search path and
timeouts, starts explicit transaction isolation, and uses a session-level
advisory lock for creation. Correct lock behavior requires that one backend
remain pinned from acquisition through release; connection pooling or role and
search-path differences could break the assumption. The clone cannot be
declared compatible merely because `get_db()` returned psycopg2 in source.
Connection failure maps to 503; timeout, cancellation, rollback, lock release,
and close behavior need real-host verification under an owned lease.

## Required contract before qualification

**Proposed host change, not present now:** expose a stable public principal
resolver for plugins. It must return an immutable subject, principal kind
(account or installation), and authentication generation. A deleted and
recreated username must receive a different subject. Credential rotation and
revocation must change or invalidate the appropriate generation, and restores
must define whether subjects/generations survive, advance, or invalidate all
prior leases. Account switch must never reuse the previous principal. An
installation bearer may be supported only as an explicit shared installation
kind with reviewed sharing semantics; it must not silently inherit a named
account or the collection manager's `__global__` behavior.
By default, bearer rotation must advance the installation authentication
generation and force a new bootstrap; an old lease must not resume silently.
Revoking one of several clients that share that bearer is impossible without
a separately identified credential. A restored database and signing secrets
alone cannot prove that revocations or generation changes made after the
backup survived the restore.

**Proposed host change, not present now:** expose an owned connection factory
or lease to plugins with documented credentials, schema/search path, role,
isolation control, dedicated backend behavior for session advisory locks,
finite connect/statement/lock timeouts, cancellation, rollback, and close.
The lease must be independent of the request connection and must define pool
return and cleanup after exceptions. Lumae must bind leases to the immutable
subject, kind, generation, source, catalog and profile epochs and expire them
on identity or epoch change. Current username-only sessions cannot be
upgraded in place without a reviewed migration and invalidation rule.

| Transition | Current behavior | Required compatibility behavior |
| --- | --- | --- |
| Current bearer client against current or new plugin | Current plugin serves legacy routes; v2 returns 401 for bearer-only requests because bearer has no account user. A simultaneously valid cookie takes precedence and authenticates its account. | A new plugin must retain legacy compatibility until a separately reviewed installation-principal contract and client transition are ready. |
| Future v2 client against old plugin | Old plugin has no v2 session routes. | Detect v2 capability before staging or replacing local data; retain current local state and use only a qualified legacy path, never claim a successful resume. |
| Named account, same valid session | Existing routes remain available; v2 create/replay works in focused plugin tests. | Qualify immutable account subject and generation plus owned DB lease on the host. |
| Account switch or username delete/recreate | Legacy username state may retain its prior scope; current v2 uses `user:<username>`. A same-second delete/recreate can leave an old cookie valid under that name. | Bind to a new subject/generation and return 410 for a previous page/catch-up lease; do not mix local personal state. |
| Several clients sharing one installation bearer | Existing collection manager uses one `__global__` scope; current v2 returns 401. | Any future installation principal is explicitly shared, with shared limits and state. Individual client revocation needs separate credentials; the shared bearer cannot provide it. |
| Bearer rotation or revocation | Legacy requests follow host token validity; an old bearer gets 401 and cannot resume current v2. | Advance generation on rotation by default, require a new bootstrap, and define cutover/revocation across host processes. |
| Installation restore or clone | Current restore behavior does not establish identity or revocation continuity. | Define installation ID, generation, source/epoch and secret restore semantics; reject old page/catch-up leases with 410 when continuity is unproven. A restored DB and secrets alone are insufficient proof. |
| Pre-v2 plugin data or client state | Legacy bootstrap/feed routes and existing data remain; no automatic durable v2 stage or promotion exists. | Negotiate version, preserve prior state until authoritative replacement is validated, and separately qualify migration/cutover. |

Safe current behavior is **401** for requests without a usable personal
principal, **410** for a missing, expired, or changed v2 page/catch-up
lease or epoch, and **503** when bootstrap storage or its independent connection is unavailable
(`plugins/LumaeAnalysis/__init__.py:2592-2606`;
`plugins/LumaeAnalysis/profile_bootstrap.py:123-164,181-200`). Release is
idempotent: a missing token, including an expired same-principal/source token
already removed, returns 200; an existing token bound to another principal or
source returns 410 (`plugins/LumaeAnalysis/profile_bootstrap.py:380-393`).
These errors do
not authorize identity fallback or a best-effort snapshot. The tracked host
test compose examples use named persistent volumes, fixed ports, external
providers, and credentials (`test/docker-compose.yaml:1-110`;
`test/provider_testing_stack/docker-compose-test-audiomuse.yaml:1-20,49-104`).
No safe, isolated local host deployment backed only by disposable data has
been verified. Real-host identity and connection tests, populated-state
cutover, old-worker drain, client resume, and release gates remain open.
