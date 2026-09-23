# LUM-004 committed collection feed frontier

Status: implemented locally for review; release and existing-state gates remain open.

## Decision and invariant

Collection changes keep their numeric global `seq` and the existing `changes` / `next_cursor` response shape. A singleton `collection_feed_state` row contains protocol version 1, a stable UUID epoch, and `head_seq`. The singleton is global across principals because sequence numbers are global. An event obtains its number with `UPDATE head_seq = head_seq + 1 RETURNING head_seq` inside the mutation transaction, then inserts `collection_changes.seq` explicitly. The row lock remains held through every event, the success receipt, and the outer commit. Thus a later number cannot commit before an earlier number. A rollback restores the counter and removes state, events, and receipt together. PostgreSQL `BIGSERIAL` cache settings are irrelevant to this explicit allocation.

The LUM-002/003 lock order is retained: keyed advisory admission, parent rows, item rows, global feed row, change rows, receipt, commit. Restore stages all events until its parent and item writes finish. Every principal contends on the one feed row; this is deliberate for one numeric global ordering contract. The row is held only for event insertions, receipt insertion, and commit, not backup validation or item work.

A reader captures the committed `head_seq` first and requests only its authenticated principal's rows with `cursor < seq <= head_seq`, ordered by sequence and limited as before. `next_cursor` is the last returned sequence or the incoming cursor for an empty page. A missing state row or unknown protocol version fails closed. The epoch is stored for future protocol transitions but is not exposed in the existing response, so client cursor handling remains unchanged. Tombstones remain events.

## Migration and cutover

The additive migration creates the singleton table and, while holding an `ACCESS EXCLUSIVE` lock on the change table, seeds `head_seq` from the maximum committed existing sequence. Repeating migration preserves both epoch and head. Migration sets finite local lock and statement timeouts only for the bounded seed, then restores the prior transaction settings before the host continues its later migrations. The host owns and commits the outer installation transaction. No existing collection, item, change, or receipt is rewritten. A legacy `BIGSERIAL` sequence with `CACHE > 1` can have unused reserved values; the seed uses the highest actual row, and all new writers insert explicit sequence numbers.

Drain old web workers, transactions, and connection pools before the migration and reopen writes only with frontier-aware workers. Legacy writers after cutover can allocate a number from the old sequence without the singleton lock, violating the invariant. Rollback to old writers is prohibited without a write pause, sequence realignment, and epoch/cursor reconciliation. Already-missed events behind a legacy numeric cursor cannot be repaired by this migration. Client reconciliation of existing cursors and host cutover remain release gates.

## Verification and open gates

Disposable PostgreSQL 17 regressions cover a held first writer and a second principal waiting in the database lock graph, rollback counter reuse, principal isolation, two-collection restore ordering and receipt replay, a commit between reader head capture and page query, pagination and tombstones, fail-closed protocol checks, and populated migration twice with `CACHE 32`. Focused existing collection mutation tests also run serially. Remaining gates are independent review, old-worker drain and host integration, existing-state/client cursor reconciliation, and full release qualification. No deployment or production migration has been performed.
