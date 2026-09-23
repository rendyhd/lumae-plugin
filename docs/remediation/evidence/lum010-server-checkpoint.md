# LUM-010 server checkpoint evidence (2026-09-23)

Implementation base: `LUM010_START_SHA`
`95f56298e4d3c91230c66c1fcffdb75f05c2afb5` on
`codex/lumae-remediation-20260922`. This record accompanies the local-only
server implementation checkpoint. It is not host, client, or release
qualification.

## Review

- A bounded Astra Medium protocol-design review approved durable immutable
  snapshot pages and a one-time finite catch-up head before implementation.
- Explicit Sol Medium implementation was followed by a separate Astra Medium
  P1 review. The reviewer required fixes for a page/release race, malformed
  non-ASCII HMAC signature handling, and connection establishment timeout.
  All three were corrected with regressions; independent follow-up: **PASS**.
- Backend-resolved model and effort are not independently observable. No
  Auralscape files were edited.

## Tests

All PostgreSQL tests used only the disposable PostgreSQL 17 database at
`127.0.0.1:55432`, with serial pytest and isolated temporary directories.

- Bearer fail-closed route regression: **1 passed, 12 deselected** using
  `tests/plugins/test_profile_bootstrap_postgres.py -k bearer_admin_without_account_fails_before_database_access`.
- Focused LUM-010 plus publication/stream/migration selection:
  `test_profile_bootstrap_postgres.py`, `test_profile_publication_postgres.py`,
  `test_profile_stream_serialization.py`, and
  `test_published_profile_migration_postgres.py`: **54 passed** (13 new
  LUM-010 tests).
- Last full serial `tests/plugins` run on the same implementation source:
  **558 passed, 3 failed** in 68.77 seconds. The three failures were exactly
  the unchanged published 1.2.5 archive/source identity checks in
  `test_release_channels.py`. The bearer regression was added after that
  full run and changed tests only. The archive was not rebuilt.
- Targeted Python compilation and `git diff --check` passed before staging.

The current host source at AudioMuse-AI
`f100684b1753e303f2b698b4f56f5b1b8972a5bb` leaves
`g.auth_user=None` for bearer-admin requests. V2 returns 401 before database
access for this auth form. A stable principal contract, owned connection on
the supported host, old-worker/populated-state cutover, client durable resume
and release qualification remain open gates.
