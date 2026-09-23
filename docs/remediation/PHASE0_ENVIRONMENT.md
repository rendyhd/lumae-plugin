# Phase 0 host and environment evidence

Date: 2026-09-22. No production host, database or device mutations performed.

- Windows PowerShell host. Docker client/server 29.8.0.
- Node v24.12.0; installed Python environment assessed in PHASE0_PLUGIN.md.
- FFmpeg 8.1.2-full_build-www.gyan.dev (gcc 16.1.0). Qualification fixtures and options still open.
- PostgreSQL 17 cached locally. Created isolated container `lumae-remediation-pg-20260922` (`112956e232180432ea04f7a2efb05b753135812cbb1357d4e6c413cae389659f`) using `--rm`, `--tmpfs /var/lib/postgresql/data`, localhost-only `55432:5432`; database/user are disposable. `pg_isready -U lumae_test -d lumae_remediation` reports accepting connections. No existing database reused.
- Existing AudioMuse worker container was inventoried by name/image only. No environment, credentials, volume data or production endpoints were inspected.
- Read-only host source: AudioMuse-AI SHA `f100684b1753e303f2b698b4f56f5b1b8972a5bb`. Existing untracked deployment/docs/reports files left untouched. Source `plugin/api.py:142-151` queues `plugin.manager.run_plugin_task` with `job_timeout=-1`; plugin wrapper `plugins/LumaeAnalysis/__init__.py:211-219` discards `timeout`. Thus call-site timeout constants do not establish enforced execution limits. Actual installed host/runtime and failure qualification remain open.
- Android SDK `C:/Users/rendy/AppData/Local/Android/Sdk/platform-tools/adb.exe` exists. No connected device qualification claimed and no device-mutating commands authorized/run. iOS Xcode execution unavailable on this Windows host.
- No plugin AGENTS.md or AGENTS.md at C:/, C:/Users/, user home or vscode ancestor locations found. Auralscape root AGENTS.md read: preserve main, dirty changes, source identity and personal data; no push/PR/deployment/device mutation; current schema and native runtime declarations are not binary qualification.
- Windows sandbox could not create exec processes (`helper_unknown_error: apply deny-read ACLs`). Auto-reviewed escalated operations were used. This is an environment limitation, not test failure evidence.

## Open qualification gates

Host-backed authentication/authorization/CSRF/request limits; cancellation/revocation/worker death/decoder stalls; matching failing production media; real browser keyboard/focus/dialog/zoom/interruption; Android and iPhone binaries/devices; reference audio comparisons; representative-scale performance and migration snapshots.

A checked-in host source tree or an available worker container does not prove any of these gates passed.
