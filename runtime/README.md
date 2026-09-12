# Lumae plugin runtime and upgrades

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

Federation remains `private-development` and is excluded from the public manifest. A host without `set_bearer_authenticator` can register/install the plugin, but pairing-token creation fails closed with a specific missing-capability response. Declared minimum core versions alone do not promise that extension exists.

Connections and all derived catalogue/artwork operations are owner-scoped. Sync requests return HTTP 202 after recording durable work. A one-minute worker schedule recovers failed dispatch or worker loss; publication rechecks the connection owner and token after network access. A sync is limited to 300 seconds (checked between network chunks/pages), 400 pages, 100,000 albums, 6 MiB per page and 64 MiB total transfer. Repeated cursors, empty continuing pages and duplicate album identities fail without replacing the prior cache. Socket/DNS behavior can add time outside cooperative checks.

Text search uses indexed prefix tokens in SQL. Similarity uses indexed LSH bands to select at most 512 full fingerprints, then the existing Album Dynamics scorer. The shortlist is approximate and can miss a globally best result; shared golden fixtures guard the scoring math. This avoids full-catalogue fingerprint transfer and per-album instance-ID queries.

On Core 3, choose `FEDERATED_ALBUMS_SERVER_ID` before the first projection when multiple servers exist. The choice is persisted and provider IDs map through that server's `track_server_map`; incidental changes to the active server cannot mix libraries. Changing the selected catalogue requires an explicit migration/rebuild procedure, not silently following a request's active server.
