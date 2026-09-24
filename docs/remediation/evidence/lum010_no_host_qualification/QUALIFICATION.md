# LUM-010 stock-host, source-scoped qualification (2026-09-24)

## Decision and ownership

The LUM-010 v2 payload consists of published analysis/edge profiles, media
revisions, source epochs and profile-stream changes. Inspection found no
listening history, private collection, preference, account-specific
recommendation or per-account source ACL in this transfer. These rows are
shared source/library-derived state. Personal and collection state remain in
their existing paths. The experimental account-principal/host-connection
decision is preserved in the historical documents, but is superseded here.

The v2 transfer contract is `source_scoped_v1`: a request first passes stock
AudioMuse authentication, then Lumae validates the exact catalog ID, current
core-server mapping, catalog/profile epochs, schema, fixed page size, absolute
expiry, opaque 256-bit session token and signed page tokens. A different valid
credential for the same source may resume the shared transfer; a token is
never authentication. A token presented for another source gets 410. The
capability explicitly names this contract; experimental account-bound clients
cannot negotiate it accidentally. Old plugins select legacy before staging.

Lumae calls `psycopg2.connect(config.DATABASE_URL, connect_timeout=5)` using
the public `plugin.api.config`. Its small owned-connection context sets
repeatable-read for capture, read-committed otherwise, a 20-second statement
timeout and 5-second lock timeout, and always rolls back on error and closes.
It neither reads nor changes the request connection. The creator's advisory
lock stays on its one backend until capture ends; durable rows, not an open
connection, support later HTTP requests. The stock role and search path can
access the plugin tables in both qualified installations.

The server's additive migration deletes only pre-release account-bound v2
sessions, adds source-scope/contract columns, and preserves published profiles,
legacy cursors and personal data. Auralscape schema 37 invalidates unfinished
v36 account-bound resumes and their staging, marks those sources for a full
refresh, and preserves published/personal tables. The migration was exercised
from a frozen real v36 SQL fixture and repeated idempotently.

## Unmodified stock targets and environment

| Target | Exact AudioMuse SHA | Host shape | Result |
| --- | --- | --- | --- |
| Original baseline | `f100684b1753e303f2b698b4f56f5b1b8972a5bb` | Windows native host, loopback HTTP; disposable Docker PostgreSQL 17 and Redis | PASS |
| Upstream main recorded 2026-09-24 | `ce742938e5ad86be85b978effa1375c4e3e9e633` | Same isolated shape, separate database and ports | PASS |

Both host tracked trees remained clean. The runtime scripts verify exact
SHAs, paths, ports, disposable database name/role, PG17 and PostgreSQL system
identifier before mutation. They copy the modified Lumae plugin into each
host's private plugin cache; host source is untouched. Synthetic secrets and
raw HTTP logs stay in ignored `.pytest-tmp` directories. The fixture uses no
real provider account or production data.

On each SHA, the real stock plugin loader and migrations passed twice, and
the actual HTTP path passed stock auth middleware, plugin routes, Lumae's
owned connection and PostgreSQL. Valid bearer and normal account cookie both
discovered v2, created/read/replayed/released source-scoped sessions. A host
process restart retained the transfer. Snapshot pages, finite catch-up,
post-head changes, later bearer delta, overlapping track IDs in two sources,
A-token-on-B 410, and shared-session continuation by another valid account
passed. Invalid bearer and invalid/expired cookies were rejected before the
plugin transfer. On current main, effective `app_config` token rotation
rejected the old bearer and let the new valid bearer resume the same shared
source transfer. Merely changing `API_TOKEN` in the process environment did
not rotate the effective persisted token on that revision.

Stock-host DB instrumentation observed distinct backend PIDs for
`plugin.api.get_db()` and Lumae's connection, equal role/schema/search path,
an uncommitted request write invisible to Lumae, and no request commit,
rollback or close caused by Lumae. Owned close released its backend and
session advisory state; repeated transfers left no owned backend leak.

Actual Auralscape `AudioMuseClient` and `runProfileV2Bootstrap` ran through
the stock HTTP barrier with file-backed SQLite. An account-created transfer
continued with bearer. A bearer transfer interrupted midway, reopened the
database, resumed, atomically replaced the publication, removed a missed
historical profile and then received a later delta. A separate empty source
with an existing local historical profile published an authoritative empty
generation and removed that row on both SHAs. The additional stock-host
client matrix injected interruption after create before local persistence,
before a page transaction committed, after a committed page, before catch-up,
before publication and after publication before release. On both SHAs, local
publication was retained before the authority boundary, and restart either
completed replacement or retried release cleanup. The matrix's catch-up
interruption uses a zero-event finite head; dense multi-event interruption is
covered by the client unit/integration suite.

## Regression results

- Lumae focused PostgreSQL v2 suite: **22 passed**. Full serial disposable
  PostgreSQL 17 plugin suite: **569 passed, 3 failed**; all three failures
  are unchanged published 1.2.5 archive/source identity release checks. The
  published archive was not rebuilt.
- Auralscape full serial Jest: **913 suites passed, 2 skipped; 8,908 tests
  passed, 6 skipped**. TypeScript passed. The earlier SQL extraction failure
  was corrected in the script by expanding the existing template fragments.
  A first full run exposed three stale schema-36 expectations; they were
  corrected and the complete rerun passed.
- Stock client HTTP integration: the account/bearer path, SQLite restart and
  six interruption scenarios passed **8/8 on each SHA**; the authoritative
  empty-withdrawal and later-delta cases were run separately and passed on
  each SHA. Lumae stock HTTP/DB ownership,
  invalid-auth, source-isolation and current-main rotation probes passed.

The checked-in scripts here reproduce the disposable qualification phases.
The full-suite logs and synthetic runtime JSON are intentionally ignored.
Python compilation, TypeScript, changed-file functional ESLint and diff
whitespace checks are part of the final checkpoint record. Existing changed
client files have Prettier formatting debt; full ESLint with its formatting
rule was not clean, while functional rules passed.

## Boundary and rollout gates

This qualifies a native Windows stock host with PostgreSQL/Redis in Docker,
not an AudioMuse application container or a native Windows PostgreSQL server.
Both stock revisions expose `config.DATABASE_URL`; a container deployment
whose effective URL reaches the same plugin tables follows the same public
interface, but that topology was not executed. A custom role/search path or
external pooler that changes transaction/session semantics needs deployment
qualification. No extra installation UUID was required for the tested source
and epoch contract.

Normal process restart resumes while the DB session and epochs remain valid.
A same-cluster restore or a clone with matching epochs may preserve an
outstanding shared session; automatically identifying a logically different
clone requires information stock AudioMuse does not expose. In uncertain
continuity, expiry, missing session or changed epoch returns 410 and a fresh
authoritative bootstrap is safe; published profiles and personal state remain.
Do not claim automatic clone detection or per-client revocation of a shared
bearer. Native mobile cookie/provider admission, populated-state cutover,
old-worker drain, Docker-host topology, performance/audio and release archive
qualification remain separate gates. These are not an AudioMuse core PR gate.

**Conclusion: AudioMuse-AI core modification is not required for LUM-010.**
Experimental host changes at `3d6b40c8d8417e6907ca8dfc645ea19fd2a552ca`
and `89f2c9d43f0d9eaedfdf87bfc09e50299935d2bd` remain reviewed
exploratory history and should not be upstreamed solely for Lumae.
