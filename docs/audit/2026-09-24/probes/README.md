# Audit probes (2026-09-24)

These are the probes behind [LUMAE_AUDIT_2026-09-24.md](../LUMAE_AUDIT_2026-09-24.md).

Most `test_probe_*` files are written to **assert the defect**, so a *passing* probe means the defect is present at the audited revision. Once a finding is fixed, invert the assertion or delete the probe.

These scripts are evidence, not part of the regression suite. Some contain hard-coded local ports or paths from the audit environment, and the DSN is usually read from `LUMAE_POSTGRES_TEST_DSN` or `AUDIT_DSN`. Use a **disposable** PostgreSQL database only. Run from the repository root, for example:

```sh
LUMAE_POSTGRES_TEST_DSN=postgresql://postgres@127.0.0.1:5432/scratch \
  python -m pytest docs/audit/2026-09-24/probes/integrity/test_probe_integrity.py -q
```

| Folder | What it demonstrates | Report IDs |
|---|---|---|
| `loudness_and_payload/lufs_probe.py` | Analyzer `ref_lufs` compared with the BS.1770-4 reference (needs `pyloudnorm`): channel averaging, fixed 48 kHz coefficients, missing relative gate | AUD-10 |
| `loudness_and_payload/edge_size.py`, `base_size.py` | Real edge-profile payload (about 19 KB) against the base profile (about 0.5 KB) | AUD-02 |
| `lum010/test_probe_lum010.py` (+ `edge.json`) | 413 once there are about 7k edge profiles; global creation lock returns 503; stale-epoch lockout returns 429; errors turned into 503 without logging; account-era migration | AUD-02, AUD-11 |
| `integrity/test_probe_integrity.py` | No-op republish deletes edges (float4/float64); stranded retry rows; maintenance pause uses up the attempt budget; `catalog_state` blocking; v1 `/changes` skip under concurrent compaction | AUD-03, LUM-001/007/008 |
| `collections/test_probe_frontier.py` | Concurrent frontier: no skipped or duplicated events; throughput ceiling | LUM-004 |
| `collections/test_probe_legacy_writer.py` | A 1.2.5-style `nextval` writer permanently wedges the frontier | AUD-05 |
| `collections/test_probe_restore_hold.py` | A large restore holds the global frontier; other users' writes time out | LUM-004 |
| `collections/explain_*.py`, `build_catalog.sql` | Workbench plans at 100k tracks; merged editions | LUM-014, LUM-016 |
| `performance/seed.py`, `stub_host.py` | Representative synthetic fixture (132k tracks, 69k items, 132k links, 94k profiles) and host stub | §6 |
| `performance/proj_bench.py`, `proj_profile.py`, `proj_results.jsonl` | LUM-012 projection wall time, RSS, WAL and statement counts | AUD-07 |
| `performance/route_bench.py`, `route_floor.py`, `explain_health.sql` | LUM-011 health and settings-poll latency | AUD-08 |
| `performance/explain_journal.sql` | Full-journal compaction against a range delete | AUD-04 |
| `performance/boot_bench.py`, `boot_concurrency.py` | v2 capture cost and concurrent-creator blocking | AUD-11 |
| `performance/feed_contention.py` | Collection frontier contention | LUM-004 |
| `client/ttlGuard.test.ts` | Auralscape: the in-flight profile guard flips at the 5-minute identity TTL. Copy it into `src/__audit__/` of an Auralscape checkout and run it with Jest. | AUD-01 |
