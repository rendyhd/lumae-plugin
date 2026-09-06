# MusicBrainz credits implementation

Credits enrichment is MusicBrainz only and catalog scoped. Navidrome supplies owned
album/track identity; the mobile app retains personal ratings, history and taste.
No enrichment request changes Navidrome, audio tags or public plugin releases.

The implementation uses the public AudioMuse job API and existing maintenance and
catalog reconciler. A dedicated lease coalesces album requests and recovers after
worker loss. Explicit mobile requests receive priority; credits take at most one
turn every two minutes ahead of other background work (five minutes for sweeps),
or an idle turn. Interactive profile/DJ work and maintenance pause take priority.
Request cache and PostgreSQL advisory locking limit MusicBrainz to one request per
second across plugin workers. Positive metadata lasts thirty days; unmatched and
empty-credit work retries after seven days. Throttling/service errors retain prior
valid publication and use durable backoff.

## Matching and scope

Typed artist, release, release-group, recording and release-track identifiers remain
distinct. Legacy generic IDs are verified by entity endpoint. Exact IDs still need
corroborating artist/title/ordered-track/disc/duration evidence. Truncated search or
duplicate editions cannot establish unique release personnel. Corroborated shared
recordings and their work credits can publish independently. People are keyed by
MusicBrainz identity, never joined by name. Source URLs, relationship IDs, instruments,
scope and matching provenance accompany each assertion.

The initial policy searches at most twelve release candidates and verifies up to
five hundred ordered tracks, with 160 uncached requests per job attempt. Cached
requests allow restart progress. Publications are capped at 2 MiB per album and
twenty records per sync page. Oversized publications remain failed/pending work,
never silently truncated connections.

## Mobile contract, schema version 1

All paths are under the existing Lumae Analysis plugin prefix and require host
authentication. Health exposes an independent `credits` capability; older plugins
remain usable without it. `sync_contract.streams.credits` also advertises the stream.

- POST `/api/credits/prepare`: `catalog_instance_id`, optional `album_ids` (max 20).
- GET `/api/credits/status?catalog_instance_id=…`: cursor, job counts, capability, pause.
- GET `/api/credits/bootstrap?catalog_instance_id=…&page_token=…&limit=…`:
  `records`, `cursor`, `has_more`, `next_page_token`.
- GET `/api/credits/changes?catalog_instance_id=…&cursor=…&limit=…`:
  `changes` containing `seq`, `album_id`, `operation` (upsert/delete), `record`;
  plus `cursor` and `has_more`. Cursor expiry returns 410; wrong source is rejected.

Each album record includes catalog identity, input fingerprint, match status,
matching provenance and subject records. Subjects are provider album/track IDs;
each contains verified credits. Recording and work scope remain distinct from
release-level album personnel. See the identical cross-language fixture
`tests/plugins/credits_contract_v1.json`; this synthetic fixture does not qualify matching.

Bootstrap pins an immutable journal boundary for an hour while publications continue.
Mobile stages pages and atomically replaces its prior snapshot only when complete.
Metadata corrections and removal publish tombstones. History compaction retains a
baseline per live subject and rejects expired cursors. Source deletion cascades
through plugin-owned records.

## Precision qualification

Automatic connection display is **disabled until a real reviewed audit passes**.
`python scripts/audit_credits.py report.json` validates at least fifty unique albums,
including well-tagged, sparse, duplicate-edition and compilation cases, at least 98%
precision among accepted reviewed matches, and separate unresolved/empty counts.
The report must identify its reviewer/time and explicitly be a non-synthetic sample.
During the separately authorized release/setup action, store the validated report in
the plugin's `credits_match_audit` setting using the host setting API. The health/status
capability reads that report and exposes its qualification.
A change to matching policy version invalidates older audits.

The authorized read-only production audit completed on 2026-09-06. Its fifty
preselected albums included 22 with an album MusicBrainz ID, 28 without, twelve
duplicate-name edition cases and nine compilations (overlapping categories).
All **25 accepted matches were correct in individual metadata review (100%)**:
thirteen release matches and twelve recording-only matches. Twenty-one albums
provided **2,905 credits**; four accepted matches had no supported credits and
twenty-five remained unresolved. Empty-credit matches count toward precision.

This is a balanced metadata stress sample, not a catalog-wide coverage estimate
or an audio/physical-edition audit. Review included same-entity merged-ID redirects,
combined/hidden recordings, and a soundtrack release-track duration that differs
from its linked recording's aggregate duration. The matching scope remained
conservative in each case.

The private report is retained locally at
`.pytest-tmp/credits-production-audit-report.json`, with a Markdown overview and
complete source responses beside it. `scripts/audit_credits.py` qualifies the report.
The report SHA-256 is
`f95953a9f1775a57fa0c67270dc325e7c8c2efbd19be553264eac4a7d038a821`.
Production settings were not changed.

Validation: **476 tests passed**, including disposable PostgreSQL integration;
plugin/script compilation and public catalog integrity checks passed. The mobile
codec accepted all fifty actual result records and all 2,905 credit assertions.
Public 1.1.8 archives and `release-sources.json` remain unchanged.
Packaging/deployment and applying the audit report are separate release actions.

References: [MusicBrainz API](https://musicbrainz.org/doc/MusicBrainz_API),
[rate policy](https://musicbrainz.org/doc/MusicBrainz_API/Rate_Limiting).
