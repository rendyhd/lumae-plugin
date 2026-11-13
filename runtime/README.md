# Lumae plugin runtime and upgrades

> **Historical.** This file documents the retired private DJ architecture and
> the Friend Album Discovery host contract as they stood before Radio DJ's
> removal in 1.2.1. It is kept for reference when reading old archives or
> reviews. For current capabilities, settings and the 1.2.5 → 1.3.0 upgrade
> procedure, see the top-level [`README.md`](../README.md),
> [`CHANGELOG.md`](../CHANGELOG.md) and
> [`docs/runbooks/UPGRADE_1.3.md`](../docs/runbooks/UPGRADE_1.3.md).

Radio DJ has been retired. Current source provides catalog, profile, SmoothFade,
relationship, and collection services without DJ workers or model dependencies.

## Upgrading a DJ-enabled private installation

Stop and drain the old dedicated DJ workers before upgrading all plugin processes.
The install migration removes the six legacy DJ tables and the two DJ cron
registrations. Catalog data, loudness and edge profiles, collections, and unrelated
scheduled tasks are preserved. Run the migration again safely after an interrupted
installation. Do not run old DJ worker code against the upgraded database.

Old model files are not removed by a database migration. After retiring the old
workers, an administrator can remove their dedicated model cache using the host's
normal storage management. Do not remove shared audio or provider data.

## Releases and validation

`release-sources.json` selects the public release explicitly. Published archives
are immutable: prepare a new version to ship source changes and never overwrite
1.2.0. Historical private archives are retained as upgrade references; they are
not current runtime instructions. No deployment is performed by local tests.

The normal CI workflow discovers `tests/plugins` and runs PostgreSQL integration
checks. Set `LUMAE_POSTGRES_TEST_DSN` to a disposable database for local integration
checks. Ordinary tests must not connect to a production database.

```sh
python -m pytest tests/plugins -q
python scripts/build_catalog.py --check
python scripts/benchmark_relationship_inputs.py --tracks 100000 --output relationship-memory.json
```

## Friend Album Discovery

The Friend Album Discovery plugin was removed.
