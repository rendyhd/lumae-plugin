# LUM-002/003 collection transaction ownership design

Status at `4276398c671f3e2713212488f4e72a32553697d7`: **PRESENT** for both findings.

This is a design artifact only. It does not implement the change or modify the remediation ledger.

## Options and decision

Three approaches were considered. Conditional `UPDATE ... WHERE revision = expected RETURNING` is a valid CAS for a simple row update, but item batches must bind membership, one revision, change rows, and a receipt to the same decision. `SERIALIZABLE` transactions would be broader, require whole-operation retries, and still would not bind an idempotency key to a request. The chosen design is explicit collection-row locking plus one outer transaction owner.

For every mutation, lock each affected collection row `FOR UPDATE`, check its revision after acquiring the lock, write collection/items/change rows, insert a fingerprint-bound success receipt, and commit once. Serialize requests sharing an idempotency key with a transaction-scoped advisory lock before reading the receipt. This is the smallest design that fixes both findings and can later contain a LUM-004 committed-feed frontier.

## One transaction owner

`_mutation_response` becomes the only transaction owner for collection create/update/delete, single-item put/delete, item upsert/delete batches, and restore. It opens one connection transaction and cursor, derives the authenticated principal and request fingerprint, performs idempotency admission/replay, calls a handler that accepts the cursor, stores the successful receipt, commits once, and only then returns the response.

Mutation handlers and `_restore_principal_collections` must not call `get_db()`, `commit()`, or `rollback()`. An exception rolls back through the owner. An ordinary 4xx or intentional 409 stores no receipt and ends without mutation.

For a non-empty `Idempotency-Key`, acquire `pg_advisory_xact_lock` using a stable 64-bit digest of a versioned `(principal, idempotency_key)` tuple before reading `collection_mutations`. Hash collisions merely add blocking; correctness still rests on the receipt primary key and fingerprint comparison. This closes the current race in which two same-key requests can both see no receipt and mutate before either separate receipt commit.

After admission:

1. A receipt with the same fingerprint returns its exact stored payload/status without running the handler.
2. A receipt with a different fingerprint returns `409 idempotency_key_conflict` without running the handler.
3. With no receipt, the handler runs.
4. An applied 2xx inserts its receipt in the same transaction, then commits state, events, and receipt together.
5. A revision conflict or other non-2xx stores no receipt, so the key remains usable after correction or explicit rebase.

Replay metadata may be exposed in response headers such as `Idempotency-Replayed: true`; it must not alter the stored JSON response.

## Fingerprint contract

Add nullable `request_fingerprint TEXT` and `fingerprint_version INTEGER` columns to `collection_mutations`. New receipts always supply both; nullability exists only to preserve old rows.

Fingerprint version 1 is SHA-256 over an unambiguous length-framed encoding of:

- uppercase HTTP method;
- exact normalized `request.path`, excluding query parameters;
- canonical parsed JSON body encoded as UTF-8 with sorted object keys and compact separators;
- normalized `If-Match`: an explicit absent marker or its trimmed value.

Compute it before route code mutates a local parsed body; item PUT currently injects the path item ID. The path already binds collection/item IDs. The body binds `base_revision`, restore checksum/content, batch order, and item fields. `If-Match` is separate because it overrides `base_revision`. Principal is not in the digest because `(principal, idempotency_key)` is already the receipt identity, and principal must come only from authentication.

Golden vectors must freeze the encoding. Reordered JSON object keys hash equally; changed array order, method, path, body value, or `If-Match` hashes differently. Invalid input rejected before mutation need not create a receipt.

The existing intentionally retryable revision 409 remains so: it is not stored. The client may change `base_revision` or `If-Match` and retry the same key because no completed receipt exists. After a 2xx commits, different fingerprint reuse is rejected explicitly.

## Revision, item, and restore behavior

Update/delete/item routes select by `(principal, collection_id) FOR UPDATE`, then evaluate `If-Match`/`base_revision`. Two requests expecting revision N cannot both commit: one commits N+1; the waiter reads N+1 and returns 409. Requests without a precondition still serialize and apply to the latest locked state.

Item writes hold the parent lock while changing membership, increment revision exactly once per request, reread the result, and append all related changes. Deletes increment only if at least one item was removed, preserving current behavior. A no-op 2xx can receive a receipt because it is a completed result.

Delete must reason from a locked row, including a present tombstone, rather than an unlocked active-only aggregate. Preserve established idempotent-delete behavior and never resurrect data.

Create uses `INSERT ... ON CONFLICT ... RETURNING` to distinguish a newly inserted row from an existing ID. Record a change only for an insertion from this transaction. The current `revision == 1` heuristic can emit a false duplicate create event. Preserve documented create-conflict response behavior, but never claim an existing row was newly mutated.

Restore validates and normalizes the full backup before mutation, then uses the outer cursor for every generated collection, item, change, and receipt. It stays additive with fresh IDs. Any fault rolls back the entire restore. A lost-response retry with the same key/fingerprint replays the stored IDs/result and never creates another copy.

## Invariants

1. Revision checks observe a collection row locked by the transaction that writes it.
2. Incompatible requests expecting revision N cannot both commit.
3. State, one revision decision, related change rows, and a keyed success receipt have one commit outcome.
4. A new success receipt binds one principal/key to one method/path/body/`If-Match` request.
5. Matching replay performs no collection, item, revision, event, or restore write and returns the original payload/status.
6. Different request reuse of a completed key is explicit and performs no write.
7. Retryable 409 does not consume the key.
8. Rollback leaves neither mutation nor success receipt; keyed committed success always has its receipt.
9. Restore is all-or-nothing across the backup.
10. Every lookup, lock, write, receipt, and replay remains principal-scoped.

## Principal and source isolation

Keep `current_principal` unchanged. Session/JWT users remain isolated as `user:<username>`. Bearer-token clients intentionally share `__global__`, including their collection and idempotency-key namespace. Malformed sessions continue to fail closed rather than falling back to that global principal.

Collection storage currently has no source column. LUM-002/003 must not infer or invent one. Catalogue identifiers present in item payloads are fingerprinted and preserved, but receipt scope remains the current principal contract. Later source-identity work must add an explicit schema/protocol.

Tests must show identical keys are independent for two personal principals, while two bearer callers share and replay one global result. No receipt payload or lock identity may cross principal boundaries.

## Lock order and LUM-004 dependency

Use one lock order:

1. idempotency transaction advisory lock, if keyed;
2. affected collection rows sorted by `(principal, collection_id)`;
3. affected item rows if needed;
4. the future per-principal committed-feed state row from LUM-004;
5. collection change rows;
6. success receipt;
7. commit.

Current routes touch one existing parent. Restore inserts new UUID rows; a future restore mode that modifies existing collections must sort locks first. No path may acquire an idempotency lock after a collection lock, or a collection/item lock after the future feed-state lock.

This does not solve LUM-004. `collection_changes.seq` remains `BIGSERIAL`; sequence allocation can still differ from commit order. The later design can allocate/publish a per-principal committed frontier at step 4 inside this owner. Consolidating transaction ownership now avoids another mutation rewrite.

## Additive migration and compatibility

Migration adds the nullable fingerprint/version columns without deleting, rewriting, expiring, or re-keying receipts, collections, items, revisions, or changes. Add a constraint requiring both fields to be null (legacy) or both valid. New code never inserts null fingerprint metadata.

Old receipts cannot be reconstructed safely because method, path, body, and `If-Match` were not stored. Do not guess from the response and do not bind the next request lazily.

For a legacy `(principal, key)` row, preserve lost-response recovery by returning its stored payload/status without executing. Mark it explicitly with `Idempotency-Replayed: true` and `Idempotency-Fingerprint: legacy-unbound`, and emit a bounded diagnostic. This prevents duplicate writes and preserves user data while exposing the ambiguity. A caller intending another operation must use a new key. Never mutate the old receipt in place.

Old processes do not acquire the admission lock, store fingerprints, or commit receipt atomically. Drain old web workers or pause mutation traffic, run the idempotent migration, start only new processes, then reopen writes. The added columns do not break old readers, but rollback to old code suspends these guarantees.

## Deterministic PostgreSQL 17 tests

Use the disposable instance on `localhost:55432`, independent connections with autocommit off, and deterministic barriers/lock observation rather than sleeps.

1. **Collection CAS:** two PATCH requests expect revision 1. Hold A after its row lock; prove B waits. Commit A; B returns 409 with revision 2. One update and one change commit.
2. **CAS route matrix:** parameterize PATCH/delete, item put/delete, upsert/delete batch, and single/batch combinations. Both expect N; assert one 2xx, one 409, one increment, winner changes only.
3. **Same key/same request:** hold A after the advisory lock; prove identical B waits before mutation. Commit A; B replays exact payload/status. One revision and one event set exist.
4. **Same key/different request:** vary method, path, body, and `If-Match` separately after A commits. Each returns idempotency conflict with no write. Validate canonical JSON golden vectors.
5. **Retryable conflict:** stale keyed request returns 409 and no receipt. Rebase using the same key; it commits and creates the first receipt. A third identical call replays.
6. **Crash before receipt:** inject after state/revision/change writes but before receipt; rollback and assert none survive. Retry succeeds once. Repeat after receipt insert but before commit.
7. **Lost response:** commit mutation plus receipt, simulate response loss, then retry on another connection. Assert exact replay, stable generated IDs, and no extra events.
8. **Restore:** inject faults after an early collection, after events, and before receipt. Each leaves no partial data/receipt. Successful retry creates one complete restore; later replay returns the same IDs without duplication.
9. **All routes:** create, patch, delete, item put/delete, both batch routes, and restore each get replay, fingerprint mismatch, and rollback coverage. Instrument transaction control to prove handlers do not commit.
10. **Identity:** same key for Alice and Bob commits independently and cannot expose the other receipt/data. Two bearer clients produce one shared global mutation and one replay.
11. **Legacy migration:** migrate a populated old schema twice. Data remains; a fingerprintless receipt replays exactly with legacy-unbound metadata and no mutation; a new key creates a bound receipt.
12. **LUM-004 handoff:** while a mutation is paused before commit, another connection sees neither state, event, nor receipt; after commit it sees all. Keep a separate expected failure proving BIGSERIAL late-commit cursor behavior remains for LUM-004.

## Acceptance and rollout gates

- One wrapper owns commit/rollback for every mutation route and restore.
- Revision checks occur under principal-scoped row locks and pass independent-connection tests.
- Successful receipts are fingerprint-bound and atomic with state, revision, and changes.
- Matching replay is exact; mismatched reuse is explicit; intentional 409 stays retryable.
- Restore cannot duplicate after a lost response.
- Additive migration preserves every existing row and exposes fingerprintless legacy replay clearly.
- Personal isolation, malformed-session failure, and intentional bearer sharing remain unchanged.
- Lock order reserves the later committed-feed state position.
- Backup limits/checksum, public success bodies, user data, and change payloads remain intact.
- Old mutation writers are drained before claiming the guarantee.
- LUM-004 remains unresolved until its own committed-feed design and regressions pass.


## 2026-09-23 specialist review corrections (required for implementation)

The central row-lock, single-owner transaction, advisory-key admission, and versioned receipt approach is retained. A targeted independent review found these current-source counterexamples and clarified the implementation contract:

1. `_upsert_item` currently conflicts on `(principal, id)` and can update an item owned by a different collection. A mutation of collection B must never change an item of collection A. Constrain conflict updates to `collection_id`, detect a rejected ownership match, and return an explicit conflict with a full rollback. Exercise single PUT and a batch with an earlier valid item followed by a foreign-owned ID; state, revisions, events and receipt all remain unchanged.
2. Lock the plain collection row by `(principal, id)` before calling aggregate `_fetch_collection`; its `GROUP BY` query cannot receive `FOR UPDATE`. The waiter-observes-current-revision contract assumes READ COMMITTED, or requires explicit handling of stricter-isolation serialization failures. Roll back every non-2xx and promptly end replay/fingerprint-conflict transactions, releasing advisory locks before returning. Test connection reuse after validation failure, replay and conflict. Unknown fingerprint versions fail closed; compare both version and digest.
3. Restore currently emits change rows inside the loop before later collection/item writes. Stage restore events until all collection/item writes finish; then emit in order at the reserved LUM-004 frontier position. This preserves the future no-parent/item-lock-after-frontier lock order. It does not implement LUM-004's committed frontier now.
4. New atomic receipts cannot retroactively repair a historical mutation that committed without a receipt. Legacy-unbound receipts replay their stored response without re-executing. Drain old mutation workers before claiming guarantees.

Review verdict: CHANGES_REQUIRED to the original design text, now incorporated here. Implementation and PostgreSQL regressions remain pending; this note is not code acceptance or release approval.
