# Catalogue sync operations

Lumae Analysis 1.1.5 publishes catalogue schema 3. Schema 3 is additive to the
ordinary schema-2 stream and adds one atomic `provider_identity_rekey_v1`
event range for Navidrome's canonical-ID transition.

Version 1.1.5 persists the post-upgrade projection check in the plugin database.
An old worker may safely ignore the first argument-compatible recheck while it
is restarting; the new startup hook or scheduled recheck consumes the same
durable request. Only successful atomic projection publication clears it.

Version 1.1.4 forces one idempotent projection check after a plugin upgrade.
This closes the startup race where the Flask process could mark a completed
AudioMuse migration ready just before the background recheck ran. Recurring
checks still skip work after the projection has been reconciled.

Version 1.1.3 treats the migrated provider track ID as the identity invariant
and leaves analysis grouping to AudioMuse. A complete, non-contradictory
AudioMuse mapping may therefore replace carried analysis IDs after fingerprint
cleanup or re-analysis; the plugin publishes that projection atomically.

Version 1.1.2 treats every Navidrome prerelease, snapshot, branch, or unknown
build as uncertain, even when it retains the numeric version of the last safe
release. That closes admission and forces exact catalogue inspection before
provider IDs can be trusted.

Version 1.1.1 narrowed provider-identity row locks to mandatory catalogue
and transition rows. Optional analysis state remains readable through its outer
join without asking PostgreSQL to lock the nullable side of that join.

Lumae Analysis 1.0.1 is a resource-safety release. Audio decoding and BS.1770
filtering are streaming, waveform backfill queries and batches are bounded,
catalogue writes are batched without constructing a second full parameter
snapshot, and no-change analysis projections retain their current generation.
An install only schedules preparation for a missing, stale, or unattested
catalogue; it does not automatically rebuild an otherwise current projection.

Every heavy RQ job now has a finite timeout. One waveform analysis has a
15-minute deadline and accepts at most eight channels, 384 kHz, and the roughly
109-minute duration representable by the v1 100 ms/uint16 ramp contract.
Out-of-contract media remains playable but does not receive a waveform profile.
Background waveform batches default to three tracks and cannot exceed ten.

Relationship preparation now uses AudioMuse's existing MusicNN IVF index to
cap each entity's shortlist at 384 albums or artists. The published relationship
schema and algorithm contract remain version 1 because the client payload is
unchanged. It does not use an all-pairs fallback. When IVF is unavailable the
relationship state reports `waiting_for_index`; a previously published
generation remains readable until a replacement publishes atomically.

The settings page includes a maintenance pause. It prevents new catalogue,
projection, waveform, and relationship work while preserving all published
data. Resume it from the same page after diagnosing server pressure.

## Storage retention

Catalogue and analysis publications keep the active generation plus any older
generation pinned by an unexpired bootstrap lease. Superseded, unpinned rows
are deleted in the same transaction that publishes their replacement. Plugin
migration runs the same idempotent cleanup so installations created before
1.1.0 shed accumulated historical generations on upgrade.

Catalogue, analysis, profile, and relationship change journals retain at least
1,000 events and normally enough events for two complete logical snapshots.
Compaction advances each stream's floor atomically. A client whose cursor falls
below that floor receives `bootstrap_required` through the existing cursor
contract and cannot silently miss changes.

Lumae Analysis 0.9.1 normalized structured provider artist identities before
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

## Navidrome canonical-ID transition

Version observation closes the provider-ID admission gate; it never authorizes
a write. While the gate is closed, the last complete catalogue and analysis
generations remain published and provider-ID mutations are denied.

The plugin scans a pending source at minutes 2 and 32 of every hour. The
scheduled analysis projection remains at minute 47 of every sixth hour. A
recheck performs a provider scan only when a transition is pending. Publication
requires two independently fetched, byte-canonically equivalent normalized
targets and the exact Navidrome ID codec.

One PostgreSQL transaction then:

1. locks and revalidates catalogue, analysis, version, and transition state;
2. verifies the plugin-owned analysis row/link baseline without AudioMuse;
3. inserts the complete canonical-ID catalogue generation and relationships;
4. rekeys source profiles and Living Collections with collision checks;
5. copies analysis vectors and scalar payloads with `INSERT … SELECT`, keeping
   every `analysis_id` and byte value unchanged;
6. publishes a contiguous full-payload rekey/addition/removal range;
7. advances both stream heads without rotating either epoch;
8. pins the complete pre-transition catalogue generation;
9. stores a compact, downloadable transition manifest; and
10. marks the transition applied.

Any error rolls back the transaction. There is no fuzzy matcher, manual rekey
button, AudioMuse-derived authorization, or plugin-local backup engine.

After publication, catalogue and carried analysis sync are safe. Fresh
AudioMuse projection ingestion remains paused until every provider ID that had
carried analysis has a current AudioMuse mapping. AudioMuse may legitimately
reassign canonical analysis IDs during its later analysis/cleaning pass; once
the provider-ID mapping is complete, the plugin publishes that current grouping
as a normal atomic analysis generation. Health reports `ready`,
`migration_required`, `busy`, or `repair_required`; the ordinary AudioMuse
Provider Migration is sufficient. `repair_required` is reserved for internally
contradictory mappings, not a valid change in canonical analysis grouping.

Download retained evidence from:

```text
GET /api/catalog/provider-identity/manifest?transition_id=<transition-id>
```

Before a production upgrade, create and download an AudioMuse full backup (it
contains the plugin's PostgreSQL tables), create a Navidrome database backup,
and retain plugin/configuration packages separately. Restore AudioMuse and
Navidrome backups as a matched pair in an isolated stack when rehearsing
recovery. File-backed `.nsp` rules are not rewritten and remain an explicit
operator review item.
