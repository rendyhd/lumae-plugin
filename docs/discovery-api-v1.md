# Discovery APIs — Lumae Analysis 1.2.2

All paths below are relative to `/plugins/lumae_analysis`. Host authentication is
required. Password/session users are isolated by principal; installation bearer
tokens share the installation principal. Responses use private/no-store caching.
Keys and raw listening logs are not part of these protocols. No catalogue ID is
required to own a wish, memory, feedback record or metadata job.

## Setup and compatibility

Enable the collection manager in plugin settings for `personal_discovery` v1.
Turning it off hides sync without removing stored records. `music_metadata` v1
is available after migration and is paused by Pause background maintenance.
Health `/api/health` reports both versioned capabilities, their enablement and
personal/shared sync scope. Existing `/api/shelves/*` v1, credits, catalogue and
playback APIs are unchanged. No new Python dependency or dedicated worker is
required. The ordinary default worker runs one metadata entity per minute;
it yields before network work when interactive playback profiles are pending.

No MusicBrainz API key/account is needed. Last.fm and AI services are configured
in the app, never on this server. Follow [MusicBrainz API requirements](https://musicbrainz.org/doc/MusicBrainz_API),
including meaningful User-Agent, one request/second and applicable service terms.
The existing global HTTP cache, advisory lock and backoff controller are shared
with credits, but credits album matching/qualification is not reused.

## Personal records

`GET /api/personal_discovery/bootstrap?limit=100` returns `schema_version`,
`epoch`, `head`, `cursor`, `hasMore` and `records`. Continue with the returned
`epoch`, fixed `head` and advancing `cursor`. After bootstrap, fetch changes
from `head` to reconcile records changed during paging.

`GET /api/personal_discovery/changes?epoch=UUID&cursor=N&limit=100` returns the
same envelope. Maximum page size is 250. An unknown epoch or a cursor ahead of
a restored server returns 410 `bootstrap_required`. Rebootstrap even if a
restored backup reports the same epoch. Preserve pending local operations and
conflict drafts; never erase them during bootstrap. Durable discovery tables
have no catalogue foreign keys and survive catalogue rebuilding.

`POST /api/personal_discovery/mutations` accepts:

```json
{
  "id": "unique-mutation-uuid",
  "epoch": "bootstrap-epoch-uuid",
  "recordId": "stable-record-uuid",
  "kind": "want",
  "operation": "upsert",
  "baseRevision": 0,
  "fields": {"albumId": "album-record-uuid", "intent": "comfort", "note": "Teenage favourite"}
}
```

Record kinds and allowed fields are centrally defined in `personal_discovery.FIELDS`:
`album`, `want`, `memory`, `feedback`, `dismissal`, `rest`, `introduction`, `order`.
Saving intent is discovery/comfort/unclassified (or null). Dates are nonnegative
Unix milliseconds; nullable dates are accepted. Fields are limited to 16 KB and
mutation requests to 24 KB. Order lists contain unique UUIDs, maximum 2,000.

Responses return `record` with `revision`, `fieldRevisions`, `fields`, `deleted`,
plus epoch/cursor and an optional alias. Mutation UUIDs are idempotent: the same
payload replays its recorded outcome; reusing an ID with different content is
409. Use a new mutation ID when resolving a conflict.

Independent fields merge against `baseRevision`. Changed fields return 409
`field_conflict` with the current record and conflicting field names. Preserve
the draft and offer Keep mine / Use synced; Keep mine resubmits only the chosen
fields against the returned revision. Order is one atomic field.

`delete` accepts empty fields and creates a tombstone. Delayed upserts cannot
restore it. `restore` requires empty fields and the exact current revision.
Removal is not dislike. `introduction` provenance cannot be deleted or rewritten
through this API; recording familiarity/recognition belongs in separate feedback.

Strong save deduplication accepts only `musicbrainz:release-group:<UUID>` as
`canonicalKey`. A duplicate new save aliases to the existing want and preserves
its date/notes/acquisition state. Name-only saves remain separate. If two already
saved wants are later assigned the same identity, `duplicate_save_conflict`
returns both records without losing either draft. Resolve the personal fields
on the survivor and explicitly delete the redundant saved record. Canonical
identity changes are rejected rather than silently moving an existing alias.

Recording references use `{kind: "recording", mbid: UUID}` or
`{kind: "track", catalogId: string, id: providerTrackId}`. Rest and introduction
records require one of these; approximate title/artist matches cannot propagate
permanent recording state. Album/artist memories can reference an ID or name.
Unscoped legacy mobile wishes require explicit import before upload; the server
cannot establish which account owned an old device record.

## External metadata jobs

`POST /api/music_metadata/prepare` accepts `{entities: [...]}`, 1–40 entities,
64 KB maximum. Each entity has a fresh UUID `id`, `kind` (artist, release-group,
release or recording), and either typed `mbid` or `title` plus `artist` (artist
search needs only title). Optional `revisions` contains opaque account/source/
consent/seed/catalogue/policy revision values, echoed in status for stale-result
rejection. Do not send keys or raw listening observations.

The response is 202 with job IDs. Repeating identical IDs is safe; changed
content under the same ID is 409. Maximum 40 pending/running/deferred jobs per
principal; more return 429. Completed jobs are retained for seven days and
pruned on subsequent submissions. Use a new job UUID to retry a failed lookup.

`GET /api/music_metadata/status?id=UUID` returns `jobs`, `remaining_requests`
and `paused`. Without an ID, at most 40 recent jobs are returned. Job states:
pending, running, deferred, verified, ambiguous, unresolved, failed, cancelled.
`POST /api/music_metadata/cancel` with `{id: UUID}` invalidates a pending/running
job's lease. Expired five-minute leases are reclaimable after worker loss;
stale/cancelled workers cannot publish. Account fairness uses last-served time.

Verification is conservative: typed ID lookup, or a unique exact normalized
name/artist search followed by entity lookup. Ambiguous/truncated matches have
no verified fields. Missing search results are unresolved, not proof of
nonexistence. Results contain the requested entity kind, `verifiedFields`, source
link and evidence. Release-group dates and edition dates remain distinct.
Related editions/recordings are not implicitly enumerated: request each typed
entity separately within the same limits. Verified identity never establishes
personal recognition (`recognition: unknown`) or automatic playback eligibility.

The persistent allowance is 80 uncached HTTP attempts per principal per UTC day.
Cache hits cost zero. Reservations occur immediately before an actual HTTP
attempt, survive failure/restarts and are atomic. MusicBrainz backoff and the
shared one-request/second lock apply across workers and credits. Work exceeding
the allowance remains deferred. No AI/Last.fm generation happens in this plugin.

## Qualification and release

Run `python -m pytest tests/plugins -q` with `LUMAE_POSTGRES_TEST_DSN` pointing
only to a disposable PostgreSQL database. Tests create/drop isolated schemas.
The new tests cover auth isolation, duplicate saves, independent edits/conflicts,
delete/restore, epoch/paging recovery, immutable introduction provenance,
request limits, cancellation, expired leases, cache-free budget accounting and
conservative identity matching. HTTP is mocked; no personal profile or live
provider key is required. Live identity-quality benchmarking remains separate
from deterministic contract qualification.

Build only the new release using `python scripts/build_catalog.py`, then run
`python scripts/build_catalog.py --check`. Existing archives are immutable.
This repository update does not push, install or deploy the plugin. Upgrading
only the plugin does not finish the pending mobile Want Shelf/prediction work.

## Validation recorded 2026-09-13

- Full plugin suite: **430 passed**, with PostgreSQL enabled; no skipped tests.
- New discovery contracts: 13 tests, including concurrent request reservations.
- Python compilation and catalogue archive integrity checks passed.
- Existing release archives and private test builds were preserved.
- MusicBrainz HTTP behavior was tested with controlled responses; this is not a
  live recommendation-quality benchmark.

## Additive capabilities in 1.2.3

`personal_discovery.features` includes `album_memory_context` and `enjoyment_feedback`. Album memory entities accept an optional bounded `artist` string. Feedback affection accepts `enjoyed` independently of `recognition` (including `new_to_me`). Clients retain these edits locally with an update message when the capability is absent.
