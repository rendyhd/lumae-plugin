# LUM-008 runtime routing evidence (2026-09-23)

Scope: plugin dirty checkout at HEAD 4276398c671f3e2713212488f4e72a32553697d7, branch codex/lumae-remediation-20260922. No commit or deployment.

- Disposable PostgreSQL 17 DSN: lumae_remediation at 127.0.0.1:55432. Fixed-schema tests run serially.
- `tests/plugins/test_profile_publication_postgres.py`: 13 passed. Covers same-revision failure retention, token supersession, tokenless rejection, media revision withdrawal, catalog publication invalidation, unknown fingerprint preservation/deferral, full-rebase withdrawal, exact-source rekey and edge reset, NULL/empty stored signature retention, waveform/edge delta-bootstrap equality, epoch rejection, and deletion/same-revision reactivation.
- `tests/plugins/test_published_profile_migration_postgres.py`: 2 passed; one-shot ready-only seed and legacy ordering.
- `tests/plugins/test_profile_stream_serialization.py`: 26 passed; two-connection publication ordering, rollback boundaries, edge/profile ordering, compaction and reader visibility.
- Final command: set `LUMAE_POSTGRES_TEST_DSN` to the disposable database above, then run `python -m pytest -q tests/plugins --tb=line` using `.pytest-tmp/settings-env/Scripts/python.exe`.
- Final result: 518 passed, 3 failed, 2 pytest cache warnings, exit 1, 45.33 s. Only failures: `test_release_channels.py::test_current_release_contains_supported_source_only`, `test_release_channels.py::test_release_archive_is_identical_across_platforms[win32]`, and `[linux]`. These compare modified source with unchanged published 1.2.5 archive. The archive was not rebuilt.
- `git diff --check` and targeted `py_compile` passed. `scripts/build_catalog.py --check` exits 1 with `published versions are immutable; select a new version`; this is an open release-version gate, and no archive was rebuilt.
- Independent reviewer: explicit gpt-6-sol High, separate read-only context. It returned findings for rekey events, rebase invalidation, edge/delta equivalence, unknown fingerprint retry, missing stored signatures, and deletion/reactivation. Lead corrected all; final reviewer response PASS. Resolved runtime model identity is not observable.

Open: historical Auralscape cached-row convergence, old worker drain, populated-state cutover, host integration and release archive qualification. No old worker, production database, client checkout, remote or device was changed.
