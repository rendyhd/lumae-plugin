# LUM-001 profile journal publication design

Status: **PRESENT** at `4276398c671f3e2713212488f4e72a32553697d7`.

This is a design artifact only. It does not change the implementation, schema, migration ledger, or remediation ledger.

## Decision

Serialize profile publications by locking the one `profile_stream_state` row for the source with `SELECT ... FOR UPDATE` before reading its epoch, head, or floor. Hold that row lock until the transaction that writes the source profile (or edge profile), appends its journal event, advances the head, and performs any inline compaction commits or rolls back.

Make state creation conflict-safe: `INSERT ... ON CONFLICT DO NOTHING`, followed by the same `SELECT ... FOR UPDATE`. Do not generate an epoch and return state from an unconfirmed insert. This is the smallest change that supplies a source-local commit order without adding a global sequence, advisory-lock namespace, new publication table, or second commit frontier.

Both publication paths must use the same helper and transaction rule:

- `upsert_profile` writes `source_profiles`, conditionally appends the corresponding event, advances the head, compacts, and commits once.
- `publish_edge_profile` locks and validates the ready legacy profile and edge job, writes `edge_profiles`, appends the complete serialized profile event, advances the head, marks the job ready, compacts, and commits once.

The allocation operation is `next_seq = locked_head + 1`; the event insert and guarded state update occur on the same cursor. The state update should include `catalog_instance_id`, `epoch`, and the expected old `head_seq` in its predicate and require exactly one returned row. This guard detects an accidental caller that did not obtain the required state version instead of silently overwriting a later head.

## Invariants

1. For one source and epoch, committed events are dense from `floor_seq + 1` through `head_seq`, subject only to deletion at or below the committed floor.
2. A visible head `N` implies every event through `N` committed. No reader can advance to `N` while a lower-numbered publication is uncommitted.
3. A publication's durable profile representation, journal event, head update, and inline floor/deletion changes have one transaction outcome.
4. Rollback consumes no sequence and exposes neither the representation nor the event.
5. Sources do not block each other: the serialization key is the `profile_stream_state.catalog_instance_id` primary-key row.
6. Legacy and edge publishers share the same ordering point and publish the same complete client payload shape.
7. Bootstrap and change readers observe only committed state under PostgreSQL `READ COMMITTED`; a head read cannot name an uncommitted event.
8. Compaction never computes or installs a floor from an unlocked, stale head.

## Current-source audit

`catalog_enrichment._stream_state` reads the state row without a lock. If it finds no row, it performs a plain insert. `record_profile_change` therefore lets concurrent transactions read the same head and both choose `head + 1`. The journal primary key means both duplicates cannot commit, but one legitimate publication can fail with a uniqueness error. The initial-state path can likewise fail on the state primary key.

`__init__.upsert_profile` is the sole normal legacy profile publisher. It currently reads the previous profile without a lock, upserts the profile, calls `record_profile_change` when the published representation changes or is removed, and commits once. Its representation/event/head atomicity is already structurally correct, but sequence allocation is not serialized. The unlocked pre-read also permits duplicate events for concurrent identical results; this is inefficient but does not violate stream correctness once allocation is serialized. A minimal implementation may lock the existing profile row or use the upsert result to improve no-change detection, but that is not required to fix LUM-001 and should not broaden this patch.

`edge_profile_store.publish_edge_profile` is the sole edge publisher. It locks the ready legacy profile, then the matching job, replaces the edge row, calls `record_profile_change`, marks the job ready, and commits once. It already rolls back validation failures and relies on the worker error handler to roll back exceptions. It must acquire the source stream lock after its existing profile/job locks and before allocation.

Other direct `source_profiles` writers are not journal publishers by their current semantics: migration copies pre-source legacy rows before stream use; `mark_pending`, `release_pending`, and `recover_stale_pending_profiles` change job/readiness state. They do not create a newly published ready representation. Their treatment remains a separate LUM-007/LUM-008 concern. Any future code that changes the client-visible ready representation must enter the publication helper.

`compact_enrichment_storage` reads state rows and later deletes events/advances floors using the earlier head without locking. Standalone profile compaction must lock each source state row, reread epoch/head/floor and profile count within that transaction, then compact. Inline compaction already runs after allocation while the publisher holds the state lock. Relationship stream behavior is outside LUM-001 and must not be changed incidentally.

`migrate_enrichment` initializes a state row for every existing catalogue source and the top-level migration commits once after additive migrations and compaction. State initialization must use the conflict-safe locked helper. The one-time `legacy_default_profiles_v1` copy occurs before enrichment migration and deliberately creates bootstrap contents rather than synthetic history; a newly initialized stream at head zero correctly instructs clients to obtain those rows by bootstrap.

## Counterexamples the design prevents

### Two current writers

Transactions A and B both read head 7 and choose 8. A inserts event 8. B blocks on the journal primary key and, after A commits, fails with a duplicate key; B's otherwise valid profile publication is lost or retried as an operational failure. With the row lock, B cannot read the head until A commits, so B chooses 9. If A rolls back, B chooses 8.

### Simultaneous first publication

A and B both see no state, generate different epochs, and insert the same source key. One fails. `ON CONFLICT DO NOTHING` followed by a locked select makes both use the single committed epoch; the loser waits for creation to commit, then allocates from its head.

### Head advanced past an earlier transaction

Without one serialization lock, independent allocation machinery can let a later transaction commit and advertise a higher head while an earlier transaction is still capable of rollback. A client can then persist that cursor and miss the late event. Holding the state row lock through transaction end makes lock acquisition order the publication order; a successor cannot allocate or commit its head until its predecessor has committed or rolled back.

### Compaction from stale state

A maintenance transaction reads head 100. Publications advance the head while A counts profiles and computes retention. A then installs a floor based on stale state or deletes against an epoch that changed. Locking and rereading the state before compaction makes deletion and floor advancement part of the same per-source order as publication.

## Lock order and transaction ownership

Use this order wherever the objects are needed:

1. client-visible profile row (`source_profiles`) through row lock or conflicting upsert;
2. edge job row for edge publication only;
3. per-source `profile_stream_state` row;
4. `profile_changes` insert and compaction deletes;
5. edge job terminal update;
6. transaction commit.

Standalone compaction locks only the state row and journal rows, so it cannot form a cycle with a publisher that obtains the state lock after profile/job locks. Migration should process sources in ascending `catalog_instance_id` whenever it locks more than one state row. No code should lock a state row and subsequently wait for a profile or edge-job row.

The public helpers must not commit. The outer legacy or edge operation owns commit/rollback. On any unexpected row count, uniqueness failure, serialization/deadlock error, cancellation, or injected fault, the owner rolls back the whole transaction. Retrying, if added by the caller, retries the complete publication with a fresh transaction and fresh validation; it never retries only the event or head update.

## Rollout compatibility

The schema can remain readable by old plugin processes, and the existing journal primary key prevents two conflicting old/new allocations from both committing. However, old workers do not take the new state-row lock. During mixed-version coexistence they can still race a new worker and cause either side to abort on the journal unique constraint. Therefore the strong success/ordering guarantee begins only after old publishing workers are drained.

Deployment procedure for this behavioral change:

1. pause admission of new legacy and edge profile jobs;
2. drain or stop all old plugin web/worker processes that can call either publisher;
3. run the idempotent additive migration/startup initialization;
4. start only the new version, then resume admission.

Rolling overlap may be tolerated only as a bounded availability risk, with full-transaction retry of uniqueness/deadlock failures and monitoring; it cannot be claimed as qualified atomic publication. A database trigger could force old writers into the new protocol, but that adds hidden transaction behavior and migration risk disproportionate to this fix. Do not use it for LUM-001.

Old readers remain compatible because epoch, cursor, payload, head, and floor formats do not change. No backfill or epoch rotation is required if a consistency check confirms every existing source has one state row, no event above head, no duplicate/gapped retained interval, and floor is not above head. Any violated source must be quarantined for explicit repair/bootstrap invalidation rather than guessed during migration.

## Deterministic PostgreSQL acceptance tests

Use PostgreSQL 17 on `localhost:55432`, a disposable database, autocommit off, and two genuinely independent connections. Coordinate interleavings with row/advisory test barriers, `pg_stat_activity`, events, or futures; do not use timing sleeps as proof. Each test must fail against the old allocator for the intended reason and pass after the change.

1. **Same source, different tracks.** Seed source S and head 0. Connection A publishes track A and pauses after acquiring the state lock. Start connection B publishing track B and prove it is blocked on S's state row. Commit A, then B. Assert both profile rows and payloads, events `(1,A)` and `(2,B)`, head 2, and no errors.
2. **Rollback reuses the next position.** A publishes track A through event/head then raises an injected exception before commit. While A holds the lock, B attempts track B and is blocked. Roll back A, allow B to finish, and assert only track B/event 1/head 1 exists. No sequence gap or A representation survives.
3. **Simultaneous first-state initialization.** Delete S's state row in an otherwise valid disposable fixture. Release two connections together to publish different tracks. Assert one epoch, two ordered events, head 2, and neither transaction fails. Instrument both calls so both enter initialization before either completes; the test must not depend on scheduler luck.
4. **Mixed legacy/edge publishers.** Seed two ready legacy tracks and a valid edge job. Hold the stream lock inside a legacy publication for one track while an edge publication for the other waits, then reverse which publisher goes first in a second case. Assert dense order, correct full payload for each event, edge row/job/profile atomicity, and head 2.
5. **Forced failures at every boundary.** Parameterize faults after profile/edge write, after event insert, after head update, after compaction, and before commit. After explicit rollback on that connection, assert all representation/event/head/floor/job changes from that transaction are absent, then successfully publish on another connection at the next dense sequence.
6. **Source independence.** Hold source A's state lock and publish to source B on another connection. Require B to commit before A is released. Assert separate epochs/heads and no cross-source blocking.
7. **Compaction interaction.** Seed enough committed events to cross the retention threshold. Pause a publisher while holding the state lock and start standalone compaction on the same source; prove compaction waits. Exercise both orders (publisher first and compactor first). Assert the final floor is monotone, all events above floor through head are present and dense, and a cursor below floor requires bootstrap while a cursor equal to or above floor remains valid and receives every retained event after it.
8. **Old/new coexistence characterization.** Run an old unlocked allocator implementation on one connection against the new locked publisher on another with a forced collision. Assert at most one commits at the colliding sequence and the loser rolls back its profile changes. Record the expected transient failure, then prove the same workload succeeds after the old writer is drained. This is a rollout gate, not a claim that mixed versions are fully supported.
9. **Reader frontier.** Pause a publisher after event/head writes but before commit. From a third connection call the changes reader and assert it sees the old committed head. Commit, read again, and assert event and new head appear together. Repeat with rollback and assert neither appears.

Use distinct payload values and track IDs in every concurrent branch so assertions prove which transaction committed. At test end, validate for every source/epoch: `head_seq >= floor_seq`, no event has `seq > head_seq`, and the retained event sequence above floor has no gaps.

## Implementation acceptance

- All profile-state reads used for mutation select the source row `FOR UPDATE` after conflict-safe creation.
- Both publishers retain one outer transaction and follow the documented lock order.
- Standalone compaction locks and rereads each source state; inline compaction reuses the held lock.
- Tests use two or more independent PostgreSQL connections and deterministic barriers.
- Existing edge atomicity/idempotence tests and profile bootstrap/change tests still pass.
- Migration is idempotent on an existing populated database and does not rotate epochs or synthesize history.
- Mixed old/new writers are either operationally excluded by drain/restart or explicitly reported as an availability-limited transition; release evidence must not call overlap fully compatible.
- No changes are made to client cursor format, public DTOs, source identity rules, profile measurement semantics, relationship publication, or unrelated job-state behavior.
