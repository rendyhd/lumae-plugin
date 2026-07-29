# Catalogue sync operations

Lumae Analysis 0.9.1 normalizes structured provider artist identities before
publishing catalogue display fields and relationship keys. Its catalogue
builder version bump forces existing malformed rows to be rebuilt. The 0.9.0
transfer-cost and scan diagnostics remain additive and idempotent.

## Fingerprint migration

`catalog_state.fingerprint_schema_version` records the algorithm that produced
the active generation. Existing installations are initialized to version 1;
new catalogue states default to the current version.

On the first refresh after a fingerprint-version change, the plugin:

1. fetches and validates the complete provider snapshot;
2. writes the next complete generation in one transaction;
3. rotates `catalog_epoch` and resets its head/floor sequences;
4. retires old bootstrap leases and journal rows;
5. publishes the generation and fingerprint version atomically.

It does not emit one ordinary change event per catalogue entity. If any
publication step fails, the transaction rolls back and the previous generation,
epoch, head, and bootstrap path remain active.

## Preparation API

Authenticated plugin clients can start or coalesce preparation with
`POST /api/catalog/prepare` and poll the returned `operation_id` with
`GET /api/catalog/prepare/<operation_id>`. Responses are private/no-store and
contain only source-safe IDs, counts, states, and versions.

The exact `catalog_prepare_api` capability is advertised only while both routes
are present. Profile and relationship cursor capabilities remain unadvertised
until their complete route contracts exist.

## Diagnostics

Catalogue health exposes these additive fields under each source's `catalog`
object:

- `fingerprint_schema_version`
- `snapshot_estimated_bytes`
- `last_scan_change_counts`
- `last_scan_change_reason`
- `last_scan_duration_ms`

`last_scan_change_counts` includes per-entity snapshot rows, upserts, deletions,
reactivations, and totals. `last_scan_change_reason` is one of
`provider_diff`, `no_change`, `fingerprint_schema_rebase`, or `scan_failed`.

Catalogue change pages additionally report `remaining_events`,
`page_estimated_bytes`, and `estimated_remaining_bytes`. These are planning
hints; cursor epoch/sequence remains the source of truth.

For a failed refresh, inspect the matching `catalog_scans` row. Its `progress`
contains the duration and reason, while `last_error` contains the bounded error
message. A failed scan over an existing catalogue leaves `catalog_state.status`
as `complete`; scan failure and publication availability are deliberately
separate facts.
