# LUM-007 local verification (2026-09-23)

Checkout: C:\Users\rendy\vscode\lumae-plugin, branch codex/lumae-remediation-20260922, HEAD 4276398c671f3e2713212488f4e72a32553697d7, intentionally dirty and uncommitted.

Environment: disposable PostgreSQL 17 at 127.0.0.1:55432/lumae_remediation using LUMAE_POSTGRES_TEST_DSN. Fixed-schema tests ran serially. No production or running-worker database.

Focused command: Python -m pytest tests/plugins/test_profile_retry_postgres.py tests/plugins/test_profile_publication_postgres.py tests/plugins/test_profile_stream_serialization.py tests/plugins/test_published_profile_migration_postgres.py tests/plugins/test_lumae_analysis.py -q -x. Result before final atomic-arm correction: 351 passed in 17.07 s. After atomic correction, retry plus existing plugin file: 314 passed in 8.80 s. Final full suite includes all affected cases.

Final full command: Python -m pytest tests/plugins -q --tb=short --basetemp .pytest-tmp/lum007-full-final2 -o cache_dir=.pytest-tmp/lum007-full-final2-cache. Result: 546 passed, 3 failed in 54.82 s, process exit 1. The only failures were test_release_channels.py::test_current_release_contains_supported_source_only and test_release_archive_is_identical_across_platforms[win32/linux], comparing the intentionally unchanged published lumae_analysis_1.2.5.zip to the dirty working source. No archive rebuild or version bump.

New tests/plugins/test_profile_retry_postgres.py: 28 cases (27 real-PG and one bounded unit). Independent explicit gpt-6-sol High reviewer, separate read-only context: PASS after the admission/recovery wake was made atomic and rollback was demonstrated. Resolved runtime model identity is not exposed.

git diff --check exit 0; targeted py_compile exit 0. LUM-007 remains IMPLEMENTED_UNVERIFIED until host, old-worker, populated-state and release gates pass. LUM-008 historical-client convergence remains open.
