# Upgrading Lumae Analysis to 1.3.0

1.3.0 changes how the plugin writes its journals, so **no 1.2.5 process may run
next to it**. A rolling restart or a gunicorn reload that leaves an old web or
RQ worker alive used to corrupt shared state without any warning (AUD-05):

- a 1.2.5 collection write took the `seq` default and wedged every later
  collection write, for every user, until someone repaired it by hand;
- a 1.2.5 profile worker wrote `ready` profiles that 1.3.0 never publishes;
- a 1.2.5 catalogue publication or provider rekey skipped the 1.3.0 profile
  withdrawal.
- a 1.2.5 **web** worker reading the 1.3.0 profile journal serves an event
  that carries an edge reference (K6) as an upsert without an edge, so
  devices delete that edge until a later event for the track. No fence
  stops readers; only step 1 does.

1.3.0 fails closed instead. The migration adds fences that make every 1.2.5
writer's insert fail, so its whole transaction rolls back:

| Journal | Fence | 1.2.5 writer that now fails |
|---|---|---|
| `collection_changes` | `seq` has no default any more | every collection mutation (`_record_change`) |
| `profile_changes` | `writer_generation SMALLINT NOT NULL`, no default | waveform profile publication (`upsert_profile`) and edge publication (`publish_edge_profile`); the source-profile row rolls back with it |
| `catalog_changes` | `writer_generation SMALLINT NOT NULL`, no default | catalogue publication with changes, and provider-identity rekey |
| `preparation_state` | the attestation expects plugin `1.3.0` | catalogue preparation (`prepare_lumae_task`) |

These fences are a safety net, not the upgrade procedure. Follow the steps
below. All table names carry the host prefix `plugin_lumae_analysis__`.

## 1. Stop everything that runs plugin code

Stop the AudioMuse web server **and every RQ worker** (default and
high-priority queues) on every host. Check that none is left:

```sh
ps aux | grep -E 'gunicorn|rq worker|rqworker' | grep -v grep
```

A worker that is still draining a long job keeps 1.2.5 code in memory. Wait for
it to exit, or stop it; the reconciler re-admits interrupted work.

## 2. Back up the database

```sh
pg_dump --format=custom --file=audiomuse-before-1.3.0.dump "$DATABASE_URL"
```

Keep the dump until step 6 has passed.

## 3. Install 1.3.0

Install the plugin through the AudioMuse plugin manager (or replace the plugin
directory). Do not start any worker yet.

## 4. Run the migration

The install hook runs `migrate(db)` in one transaction. It is idempotent, so it
is safe to run again. On success it has:

- dropped the `collection_changes.seq` default (the sequence itself remains
  owned by the column);
- added `collection_feed_state.floor_seq`, set once to the feed head at the
  upgrade (the K8 cutover; the feed epoch is kept), and the
  `collection_restores` progress table for chunked restores;
- added `writer_generation` to `profile_changes` and `catalog_changes`, filled
  existing rows with 2 and dropped the column default;
- retargeted queued, running and failed catalogue preparations to plugin
  `1.3.0`;
- withdrawn the published profiles it seeds for tracks the catalogue no
  longer has (1.2.5 never withdrew a removed track's profile), and
  republished current `ready` profiles that have no published row (repair
  D, which runs here too);
- recounted ready-but-unpublished and orphaned profiles into
  `integrity_state`.

Each schema change runs only when it is still missing, so re-running the
migration on an up-to-date database takes no exclusive table lock. A change
that is needed waits at most 5 s for its table lock, and is tried 3 times (1 s
and 2 s apart), about 18 s per needed change. If a long query keeps the table
busy, the install fails at the first change that can't get its lock, with
`canceling statement due to lock timeout` (SQLSTATE `55P03`). Find the
blocking session with `pg_blocking_pids()`, let it finish or end it, and
re-run the install.

**If the install hook fails**, the host still activates the new code, but the
transaction rolled back: none of the fences exist. Starting the web server
does **not** repair this. Its start hook re-runs only the provider-identity and
reconcile migrations, not the catalogue, profile-journal or collection
migrations that hold the fences. Fix the reported error and **re-run the
install until it succeeds**. Do not start any RQ worker, and do not start
1.2.5 again, until the fence check below passes.

Fence check (run in `psql`; every row must say `true`):

```sql
-- Fence check
SELECT c.relname || '.' || a.attname AS fence,
       CASE WHEN a.attname = 'seq' THEN NOT a.atthasdef
            ELSE a.attnotnull AND NOT a.atthasdef END AS installed
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
 WHERE NOT a.attisdropped
   AND ((a.attrelid = to_regclass('plugin_lumae_analysis__profile_changes')
         AND a.attname = 'writer_generation')
     OR (a.attrelid = to_regclass('plugin_lumae_analysis__catalog_changes')
         AND a.attname = 'writer_generation')
     OR (a.attrelid = to_regclass('plugin_lumae_analysis__collection_changes')
         AND a.attname = 'seq'));
```

Expected output, three rows (a missing row also means the fence is missing):

```
 plugin_lumae_analysis__catalog_changes.writer_generation    | t
 plugin_lumae_analysis__collection_changes.seq               | t
 plugin_lumae_analysis__profile_changes.writer_generation    | t
```

Once the web server is up, health reports the same thing as
`integrity.fences_installed` (step 6), and each web worker logs an error at
start while it is `false`.

## 5. Start the web server, then the RQ workers

Start the web server first and confirm `integrity.fences_installed: true`
(step 6). Only then start the RQ workers. Each web worker logs, at start, an
error if the fences are missing or the collection feed invariant is violated,
and a warning if ready profiles are unpublished.

## 6. Verify `integrity`

```sh
curl -s -H "Authorization: Bearer $TOKEN" https://<host>/plugins/lumae_analysis/api/health \
  | jq '{plugin_version, integrity}'
```

Expected:

```json
{
  "plugin_version": "1.3.0",
  "integrity": {
    "collections_feed_ok": true,
    "profiles_unpublished_ready": 0,
    "profiles_orphaned": 0,
    "profiles_checked_at": "2026-…Z",
    "fences_installed": true
  }
}
```

- `fences_installed` is checked on every health call (one catalogue lookup).
  `false` means the 1.3.0 migration did not complete: go back to step 4.
- `collections_feed_ok` is checked on every health call. When it is `false`,
  every collection mutation returns **503 `collection_feed_invariant`** until
  you run repair A.
- `profiles_unpublished_ready` is the count taken at install, at the last web
  worker start or by repair D (`profiles_checked_at`). When it is above 0, run
  repair D.
- `profiles_orphaned` (taken at the same times) counts published profiles
  whose track the current catalogue generation no longer has. The install
  withdraws up to 20,000 of the ones it seeds, so it is normally 0
  afterwards; see repair D.
- `null` means the value could not be read (no database, or the migration has
  not run).

Also confirm in `/api/catalog/health` that no server reports
`refresh_reason: "worker_version_mismatch"`.

## 7. Repairs

Run repairs A to C in `psql` as the database owner, and repair D from the
plugin settings page. All are safe to run when nothing is wrong: they then
change no rows.

### A. Collection feed head behind committed rows

A 1.2.5 worker inserted change rows past the feed head. Those rows belong to
committed collection writes, so the fix is to move the head past them, not to
delete them. Readers only see `seq <= head_seq`, so realigning the head exposes
the rows once, in order. Gaps in `seq` are fine.

```sql
BEGIN;
LOCK TABLE plugin_lumae_analysis__collection_changes IN ACCESS EXCLUSIVE MODE;
-- Inspect: the head and the highest committed seq.
SELECT s.head_seq,
       (SELECT COALESCE(MAX(seq), 0) FROM plugin_lumae_analysis__collection_changes) AS max_seq
  FROM plugin_lumae_analysis__collection_feed_state s WHERE singleton = 1;
-- Realign the head past every committed row.
UPDATE plugin_lumae_analysis__collection_feed_state
   SET head_seq = GREATEST(head_seq,
       (SELECT COALESCE(MAX(seq), 0) FROM plugin_lumae_analysis__collection_changes))
 WHERE singleton = 1;
-- Keep the (now unused) sequence past every row, so a restored 1.2.5 dump or
-- a manual insert cannot reuse a number.
SELECT setval(pg_get_serial_sequence('plugin_lumae_analysis__collection_changes', 'seq'),
              GREATEST((SELECT COALESCE(MAX(seq), 0)
                          FROM plugin_lumae_analysis__collection_changes), 1));
-- Make sure the fence is in place (idempotent).
ALTER TABLE plugin_lumae_analysis__collection_changes ALTER COLUMN seq DROP DEFAULT;
COMMIT;
```

Health reports `collections_feed_ok: true` immediately, and collection writes
succeed again. No restart is needed.

### B. Ready profiles without a published row (SQL)

Prefer repair D, which republishes these rows without analysing them again.
Use this SQL when the web server cannot run it.

A 1.2.5 worker (before the fence) marked profiles `ready` and journaled them,
but never wrote `published_source_profiles`. 1.3.0 treats those tracks as
current and would never republish them, so bootstrap and the change stream
disagree. The fix marks them `stale`; the profile backfill re-admits them and
republishes each one through the normal attempt path, which writes the
published row and its journal event together.

```sql
-- Inspect.
WITH unpublished AS MATERIALIZED (
    SELECT s.catalog_instance_id, s.track_id, s.media_signature
      FROM plugin_lumae_analysis__source_profiles s
     WHERE s.status = 'ready' AND s.analyzer_ver = 1 AND s.profile_schema_ver = 1
       AND NOT EXISTS (
           SELECT 1 FROM plugin_lumae_analysis__published_source_profiles p
            WHERE p.catalog_instance_id = s.catalog_instance_id
              AND p.track_id = s.track_id)
)
SELECT u.catalog_instance_id, count(*)
  FROM unpublished u
  JOIN plugin_lumae_analysis__catalog_sources src
    ON src.catalog_instance_id = u.catalog_instance_id AND src.rebind_status = 'active'
  JOIN plugin_lumae_analysis__catalog_state c
    ON c.catalog_instance_id = u.catalog_instance_id
 CROSS JOIN LATERAL (
       SELECT 1 FROM plugin_lumae_analysis__catalog_tracks t
        WHERE t.catalog_instance_id = u.catalog_instance_id
          AND t.published_generation = c.published_generation
          AND t.track_id = u.track_id
          AND t.available AND COALESCE(t.media_fp, '') <> ''
          AND u.media_signature = 'catalog-media:' || t.media_fp
        LIMIT 1) t
 GROUP BY 1;

-- Repair: re-admit them for republication.
UPDATE plugin_lumae_analysis__source_profiles s
   SET status = 'stale', last_error = 'unpublished_ready_repair',
       attempt_token = NULL, retry_category = NULL, retry_count = 0,
       retry_after = NULL
 WHERE s.status = 'ready'
   AND NOT EXISTS (
       SELECT 1 FROM plugin_lumae_analysis__published_source_profiles p
        WHERE p.catalog_instance_id = s.catalog_instance_id
          AND p.track_id = s.track_id);
```

**No delete event is sent.** The repair only marks the rows `stale`; it does
not journal anything. A client that already received the 1.2.5 `ready` event
keeps that profile until the republication emits the new upsert. If the
republication fails (for example the media is gone or analysis fails), the
client keeps the 1.2.5 profile, and the server's bootstrap does not contain
it, until the catalogue deletes the track or a later analysis succeeds. The
1.2.5 values were analysed from the same media, so this is stale-but-correct
data, not wrong data.

The repair deliberately covers every unpublished `ready` row, not only the
current ones the health count shows: a row for old media is re-admitted and
then resolved against the current catalogue. Then run **Prepare Lumae** (or
wait for the reconcile schedule) so the backfill picks the rows up. Restart the
web server, or wait for its next start, to refresh
`profiles_unpublished_ready`.

### C. Rotate the collections feed epoch (after restoring a database backup)

A restored database keeps the collections feed `epoch` of the dump, but its
history ends where the dump does. Clients that sync with the K8 epoch
(`capabilities.collections.feed_epoch`) detect this on their own only when
their cursor is past the restored head. Rotate the epoch after any restore of
the plugin tables, so every such client resyncs from the snapshot:

```sql
UPDATE plugin_lumae_analysis__collection_feed_state
   SET epoch = gen_random_uuid(), floor_seq = head_seq
 WHERE singleton = 1;
```

It takes effect immediately; no restart is needed. Clients that do not echo
the epoch (older apps) are unaffected.

### D. Repair profile publications (settings page)

Run it when health reports `profiles_unpublished_ready` or `profiles_orphaned`
above 0 (each web worker also logs a warning at start). While either count is
above 0, **Settings → Lumae Analysis → Background maintenance** shows **Repair
profile publications**. Re-running the install (step 4) runs the same repair.

For each active source it:

1. withdraws published profiles whose track the current catalogue generation
   no longer has: deletes the published row, marks the analysis attempt
   `stale` and journals a `delete` event, then deletes the track's edge;
2. republishes the rows `profiles_unpublished_ready` counts: `ready` for the
   current analyzer and for the media of the published generation, with no
   published row. It applies the checks of a completed analysis and writes
   the published row with the stored result and its original analysis time,
   plus an `upsert` event. Any old edge of the track is dropped first (as for
   every first publication) and the edge backfill measures it again. A
   `ready` row for other media, for a track the catalogue no longer has or
   for an older analyzer is left alone; the profile backfill re-analyses the
   ones still in the catalogue;
3. recounts both health counts and reports what is left.

Each batch (1,000 withdrawals, or 25 republications) is one short
transaction under the source's catalogue row lock (at most about 70 ms and
300 ms at 94k profiles), so the repair is safe while analysis and catalogue
refreshes run. A run withdraws at most 20,000 profiles (about 2 s) and
republishes at most 2,000 (about 5 s); run it again while the page reports
rows left. Clients need nothing: they apply the events like any other.

**Orphans between repairs.** Every catalogue refresh, also one without
changes, ends with the same withdrawal (step 1), bounded the same way. Each
withdrawal is an ordinary `delete` event in `/api/profiles/changes`, and the
worker that ran the refresh logs `lumae_analysis withdrew N published profiles
of <source> whose tracks are no longer in the catalogue`. `profiles_orphaned`
in health is not live: it is the count of the last install, web-worker start
or repair, and it should be 0. A withdrawal does not change
`profiles_unpublished_ready` (the withdrawn track's attempt becomes `stale`,
not `ready`).

## Known gap: 1.2.5 fingerprint rebase

The `catalog_changes` fence stops a 1.2.5 catalogue publication only when it
writes change rows. A 1.2.5 **fingerprint-schema rebase** can publish a new
generation with no change rows, and then it skips the 1.3.0 profile
withdrawal (`invalidate_catalog_changes(full_reconcile=True)`). This is not
reachable on a plain 1.2.5 → 1.3.0 upgrade: both use catalogue fingerprint
schema 2, so a 1.2.5 worker never sees a schema mismatch and never rebases. It
would matter only if a later release changes the fingerprint schema while a
1.2.5 worker is still running, which step 1 rules out.

## Rollback

There is no downgrade path. **Roll forward.** Fix the problem in a 1.3.x
release, or run the repairs above.

If you must go back to 1.2.5 anyway (for example, 1.3.0 does not start at all),
stop everything, restore the step 2 dump with `pg_restore --clean`, and install
1.2.5. Do **not** run 1.2.5 against the migrated database: every 1.2.5 write to
a fenced journal fails by design, so collections, profiles and catalogue
refreshes stop working. Writes made while 1.3.0 was running are lost with the
restore.
