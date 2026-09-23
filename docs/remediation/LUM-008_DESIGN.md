# LUM-008 published profile foundation

This change adds `published_source_profiles` with one row per
`(catalog_instance_id, track_id)` and a cascading foreign key to
`catalog_sources`. The row holds the typed payload of a published ready
`source_profiles` result: sample rate, duration, loudness, both ramp byte
arrays, analyzer and profile schema versions, media signature, and the
original analysis timestamp. Attempt status and error fields are excluded.

During `migrate(db)`, the table is created and seeded after the existing
`legacy_default_profiles_v1` copy. A separate
`published_source_profiles_seed_v1` marker in `profile_migrations` gates
one atomic `INSERT ... SELECT` of only the source rows currently marked
`ready`. The marker is recorded even if there are no ready rows. Later
migration runs never refill a deleted publication, copy a later ready
attempt, or infer legacy ownership from a changed default source. Existing
source and profile rows are preserved.

This is a storage foundation only. Runtime code does not yet read or write
this table. The next stage must add attempt-token fencing, publication and
edge-profile routing, and provider rekey routing. It must then add revision
invalidation and client reconciliation so existing clients and clean
bootstraps agree through ready → pending → failed transitions. Until those
stages are complete, LUM-008 remains open.

## Next runtime stage: reviewed lifecycle contract

The existing `source_profiles` row is attempt/selection state; the new
`published_source_profiles` row is the last valid public baseline. Pending,
enqueue failure, interruption and same-revision failure leave that published
row, its bytes, timestamp and stream head unchanged. A successful accepted
replacement updates the published row and emits its schema-v1 profile upsert in
one transaction. Compare against the published baseline for no-op suppression,
not the immediately preceding attempt row.

Each admitted attempt needs a durable UUID claim plus captured source identity,
source epoch, media revision and start time. Completion and queue release must
match the claim. A stale, superseded, removed-track or changed-revision
completion cannot publish or overwrite a newer attempt. Existing queued jobs
without a claim must be drained or safely re-admitted at cutover. Both queued
and direct backfill paths, and hook analysis, must use the claim. Do not hold
database locks during download or decoding.

Transaction order: authoritative source/catalog state, attempt row, published
row, then profile stream state. Edge-profile publication must read/lock the
published baseline and use the same source/revision guard. Exact-source
provider rekey must rekey the published row and invalidate old attempt claims
in its transaction. Bootstrap must read published rows; scheduling and job
counts continue reading attempt state. The global legacy `profiles` branch
keeps its existing ownership rule and is not reinterpreted.

A known different media revision or authoritative deletion must explicitly
invalidate the published row and atomically journal a delete before a later
repair upsert. Missing/empty fingerprints are not evidence of a new revision.
The old client bootstrap merges staged rows rather than removing absent rows;
an epoch reset alone cannot clean previously cached withdrawn profiles.
Historical convergence needs retained/replayed deletes or a separately
coordinated authoritative client replacement. These invalidation/client gates
remain open after same-revision preservation.

Do not deploy the seed alone or allow writes between one-shot seed and routing:
its durable marker intentionally prevents a rerun from filling that gap.
