# Lumae sync contract (plugin ↔ Auralscape)

- **Status:** authoritative. Every contract work package edits this file in the same PR.
- **Describes:** plugin `lumae_analysis` **1.2.5** (`PLUGIN_VERSION`, `plugins/LumaeAnalysis/__init__.py:117`), verified against `main` at 629089f.
- **Planned changes:** §7 lists K1–K11. Each one is `planned` until its work package flips it to `shipped in <version>`.
- **Sources:** everything below was taken from the code, with file:line citations (paths are relative to `plugins/LumaeAnalysis/` unless shown otherwise). Where the plan (`docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md`) or the hand-off says something different, **the code wins**. The difference is recorded in a "Note" and collected in §9.
- **Auralscape copy:** the app agent copies this file to `docs/contracts/` in `rendyhd/Auralscape`. The copy in this repo is the master.

Contents:
1. Conventions
2. Health and capabilities
3. Profile endpoints
4. Edge profile payload
5. Collections and shelves
6. Client rules already relied on
7. Planned changes (K1–K11)
8. Compatibility rules
9. Code-versus-plan notes

---

## 1. Conventions

### 1.1 Base path

All paths in this document are relative to **`/plugins/lumae_analysis`**, the prefix the AudioMuse host mounts the blueprint at (`Blueprint("lumae_analysis", …)`, `__init__.py:206`; the test harness uses the same prefix at `tests/plugins/test_lumae_analysis.py:8831`). For example, health is `GET /plugins/lumae_analysis/api/health`.

### 1.2 Authentication

The plugin has no authentication of its own. Every route is behind the AudioMuse host's authentication.

| Host mode | Behaviour | Collections and shelves principal (`collection_manager.py:60-72`) |
|---|---|---|
| `AUTH_ENABLED=true`, password/JWT session | Authenticated | `user:<username>`, private to that user |
| `AUTH_ENABLED=true`, installation bearer token | Authenticated | `__global__` (shared library) |
| `AUTH_ENABLED=true`, malformed session (session method, no user) | `abort(401)`: Flask HTML 401 | none (fails closed) |
| `AUTH_ENABLED=false` | **Anonymous.** Every transfer, including the v2 profile bootstrap, is open to anyone who can reach the host. | `__global__` |

Rules for clients:
- Health still reports `capabilities.profile_bootstrap.auth: "host_authenticated"` when `AUTH_ENABLED=false` (`__init__.py:1840`). That string describes the design, not the live setting, and it is unchanged in 1.3.0. **From 1.3.0 (K4, P1-6):** `capabilities.profile_bootstrap.auth_enabled` is the host's live `AUTH_ENABLED` (`false` when the host does not set it). With `auth_enabled: false`, every transfer is anonymous (last row above).
- The host redirects an unauthenticated request for a non-`/api/` host path to `/login` with **HTTP 302** instead of returning 401. This is host behaviour, not visible in this repo; it was *verified in the 2026-09-24 audit* (`docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md`, P3 list under AUD-12). Every plugin path starts with `/plugins/`, so plugin API calls get this redirect too. A client must treat any **3xx response, or any `text/html` body where JSON was expected, as `authentication_required`**. It must never parse the login page as data and must never follow the redirect as success.
- Profile data (waveform and edge) is shared by everyone who can reach the catalogue source; it is scoped by `catalog_instance_id`, not by user. Collections and shelves are scoped by principal. Shelves are also scoped by catalogue.

### 1.3 Requests

- JSON request bodies need `Content-Type: application/json`. Every handler uses `request.get_json(silent=True)`, so without that header the body is read as empty. The v2 bootstrap then answers 400, and collection creation answers 400 "Collection name is required."
- Body size limits: v2 bootstrap 16,384 bytes (`__init__.py:2604`); edge analyze and backfill 64,000 bytes (`__init__.py:2800, 2813`). These limits are checked against `Content-Length` only (`__init__.py:2054-2057`). **From 1.3.0 (P1-6):** the v2 bootstrap also reads at most 16,384 bytes from the request stream (`_v2_body`), so a chunked body without `Content-Length` that is larger gets the same 400 `invalid_profile_bootstrap` ("Invalid bootstrap request.").
- Unknown JSON fields and unknown query parameters are **ignored** everywhere, with one exception: v2 `page`/`catchup`/`release` bodies reject `page_size` (`profile_bootstrap.py:119-120`). §8 depends on this.

### 1.4 Error envelopes

There are three shapes. Clients must accept all three.

| Family | Shape | Helper |
|---|---|---|
| Profiles, v2 bootstrap, edges, catalogue | `{"error": "<code>", "message": "<text>"}`. The v2 bootstrap sets `message` to the same value as `error`, except when the body is too large or is JSON but not an object (`"Invalid bootstrap request."`, `__init__.py:2605-2606`). | `_catalog_error`, `__init__.py:2050-2051` |
| Collections | `{"error": "<code or human sentence>"}`, sometimes with extra keys (`current`) | `_error`, `collection_manager.py:515-516`, plus inline `jsonify` |
| Shelves | `{"error": "<human sentence or code>"}`, sometimes with extra keys (`order`) | `shelves.py:209, 228, 143` |

- Collection and shelf validation errors put a **human sentence** in `error`, for example `"Item kind must be album or track."`. Branch only on the codes listed in §5.
- An unhandled exception is the host's HTML 500, and an unknown route is Flask's HTML 404/405. Treat an HTML body on a non-2xx status as a server error. Treat a 404 on a route this contract documents as "the plugin is older than this route".

### 1.5 Response headers

`_private_json` (`__init__.py:2041-2047`) sets `Cache-Control: private, no-store` (the v2 bootstrap, edges and errors) or `private, no-cache` (legacy `/profiles/bootstrap` and `/profiles/changes`), plus `Vary: Authorization, Cookie` and `X-Content-Type-Options: nosniff`. Health and the collection routes use plain `jsonify`, with no cache headers. Shelves set `private, no-store` on reads (`shelves.py:218`).

- **1.2.5:** `GET /api/profiles` used plain `jsonify`, with no cache headers.
- **1.3.0 (unreleased, P1-4):** `GET /api/profiles` uses `_private_json`: `Cache-Control: private, no-store`, `Vary: Authorization, Cookie`, `X-Content-Type-Options: nosniff`.

**`Retry-After` (K4).** 1.2.5 never sends it. From 1.3.0 (unreleased, P1-6) every v2 bootstrap **429** carries `Retry-After: <seconds>` (a whole number, 1–300) and every v2 bootstrap **503** carries `Retry-After: 5` (§3.5). Other routes and statuses do not send it. Clients wait at least that long before retrying, and use their own backoff when the header is absent (1.2.5).

**Compression (K1).** 1.2.5 never compresses. From 1.3.0 a blueprint `after_request` hook (`_compress_json_response`, 1.3.0 `__init__.py:249-273`) gzips a plugin response (level 4) when **all** of these hold:
- the request's `Accept-Encoding` allows gzip: `gzip` or `x-gzip` with q>0, or, when neither is listed, `*` with q>0. `gzip;q=0` (or `*;q=0` with no explicit gzip) refuses it; names are case-insensitive;
- the status is 200;
- the mimetype is `application/json`;
- the uncompressed body is at least 1,024 bytes;
- the response has no `Content-Encoding` yet and is neither streamed nor `direct_passthrough`.

A compressed response carries `Content-Encoding: gzip` and `Content-Length` set to the **compressed** size. Every response that meets the last four conditions (whether or not the client accepted gzip) gets `Accept-Encoding` appended to its existing `Vary` (for example `Vary: Authorization, Cookie, Accept-Encoding`), never duplicated. Errors, small bodies, binary vectors and host routes outside the plugin blueprint are never compressed. The two JSON download attachments are also plugin JSON, so they are gzip-eligible like any other route: the collection backup download (`_backup_response`, `collection_manager.py:358`, `application/json; charset=utf-8`) and the provider-identity transition manifest download (`lumae-provider-rekey-<id>.json`, 1.3.0 `__init__.py:2482-2505`). `Content-Disposition` is unchanged; a client or browser saving the file receives the decoded JSON. The JSON after decoding is byte-for-byte the uncompressed body.

Client rules: send `Accept-Encoding: gzip` only when the HTTP stack decodes it (native stacks do this transparently). Never use `Content-Length` as the decoded size: a transparent decoder may drop it or leave the compressed size (C-7). Measured on a 50-row page with real-sized (~20 KB) edges: about 1,002 KB → 372 KB (2.7×, level 4) when the rows' edges differ; pages whose edges repeat compress far more.

### 1.6 Timestamps

Timestamps are ISO-8601 strings, but the zone varies. Clients must accept every form below.

| Field | Column type | Wire form |
|---|---|---|
| profile `analyzed_at` | `TIMESTAMP` without zone (`__init__.py:1222`) | **no zone designator**, for example `2026-09-24T10:11:12.123456`; wall-clock time in the database session zone (`catalog_enrichment.py:96-99`). **Unchanged in 1.3.0**: the column has no zone, so the server cannot convert it, and it is part of the stored profile payload that events, snapshots and reads must agree on. |
| profile change `created_at` (`/profiles/changes` and v2 catch-up), v2 `expires_at`, relationship `computed_at`/`started_at`/`completed_at`/`updated_at` | `TIMESTAMPTZ` | 1.2.5: `…Z` when the server runs in UTC, otherwise with a numeric offset such as `+02:00` (`profile_bootstrap.py:209-210`). **From 1.3.0 (P1-6): always UTC with `Z`**, for example `2026-09-24T08:11:12.123456Z`, whatever the database TimeZone (`_iso` in `profile_bootstrap.py` and `catalog_enrichment.py`). Catch-up events captured by a 1.2.5 server keep the form they were captured with. |
| collection `created_at`/`updated_at`/`deleted_at`/`added_at`, change `created_at` | `TIMESTAMPTZ` | `…Z` in UTC, otherwise a numeric offset, in 1.2.5 and 1.3.0 (`collection_manager.py:234-237`) |
| shelf `addedAt`/`at` | client-supplied number | stored and echoed as sent (`shelves.py:41-44`) |

Never compare server timestamps with the device clock. `expires_at` is advisory; the server decides expiry with its own `now()` (`profile_bootstrap.py:195-197`). Parse timestamps as ISO-8601 with an optional zone (`Z` or an offset), and keep accepting offsets from 1.2.5 servers.

---

## 2. Health and capabilities

### `GET /api/health` (`__init__.py:1823-1864`)

This route always answers 200 (unless an exception occurs). It has no side effects. From 1.3.0 it also reads the `integrity` object below (two index lookups, well under 1 ms at 94k profiles and 200k collection events).

| Key | Value in 1.2.5 | Source |
|---|---|---|
| `plugin` | `"lumae_analysis"` | 1828 |
| `plugin_version` | `"1.2.5"`; `"1.3.0"` from 1.3.0 (unreleased, P1-3) | 1829 |
| `core_version`, `core_adapter`, `supported_core_range` | host detection; the range is `">=2.6.0,<4.0.0"` | 1830-1832 |
| `sync_contract` | `{revision, producer, core_api_contract, streams:{catalog, analysis, profiles:{schema_version:1, analyzer_version:1, semantic_contracts:["lumae_playback_profile_v1"]}, credits, relationships}}` | `sync_contract()`, 1765-1801 |
| `schema_version` | `1` (profile schema) | 1834 |
| `analyzer_version` | `1` (waveform analyzer) | 1835 |
| `status` | `"ok"` when the core is supported, otherwise the core compatibility status | 1862 |
| `capabilities` | the table below | 1836-1861 |
| `integrity` | absent | **New in 1.3.0 (unreleased, P1-3, AUD-05).** Operator diagnostics, not a client gate: `{collections_feed_ok: bool\|null, profiles_unpublished_ready: int\|null, profiles_checked_at: string\|null, fences_installed: bool\|null}`. `fences_installed` is live (one catalogue lookup): `false` means the 1.3.0 migration has not installed every old-writer fence (`writer_generation` NOT NULL without default on `profile_changes` and `catalog_changes`, no default on `collection_changes.seq`). `collections_feed_ok` is live: `false` means a collection change row sits past the feed head, and every collection mutation then returns 503 `collection_feed_invariant` (§5.3) until an operator repairs it. `profiles_unpublished_ready` counts current `ready` source profiles with no published row; it is recounted at install and at web-worker start, and `profiles_checked_at` (UTC, `Z`) says when. `null` means unknown (no database or an older schema). Repair SQL: `docs/runbooks/UPGRADE_1.3.md`. |

`capabilities`: every key that exists today, with exact names.

| Key | Fields (1.2.5) | Notes |
|---|---|---|
| `profile_bootstrap` | `protocol_version: 2`, `schema_version: 1`, `auth: "host_authenticated"`, `transfer_contract: "source_scoped_v1"`, `available: bool`. **1.3.0 adds** `auth_enabled: bool`, `sliding_expiry: true`, `idempotent_create: true` (P1-6). | 1.2.5: `available` is only `bool(config.DATABASE_URL)` (1842). It is **not** a probe. **From 1.3.0 (K4):** `available` is `true` only when `DATABASE_URL` is set, the v2 tables and the 1.3.0 session columns exist, and a probe on the plugin's own connection succeeded; a success is cached for 60 s and a failure for 10 s, and the probe connects with a 2 s timeout (`profile_bootstrap.availability`). `auth_enabled` is the host's live `AUTH_ENABLED` (§1.2); `auth` keeps its 1.2.5 value. `sliding_expiry` gates K3 and `idempotent_create` gates K5 (§3.5). |
| `edge_profiles` | `schema_version: 2`, `method: "lumae-edge-kweighted-bands-48k-k4-v2"`, `available: bool`, `enabled: bool` | `available`: the PyAV 16.1.0 / libswresample 6.1.100 runtime imports (`edge_profiles.py:86-98`). `enabled`: the setting `edge_profiles_enabled` and `available` (`__init__.py:2762-2763`). |
| `personal_discovery` | `schema_version: 1`, `enabled`, `scope: "shared"\|"personal"`, `features: ["album_memory_context","enjoyment_feedback"]` | out of scope here; see `docs/discovery-api-v1.md` |
| `music_metadata` | `schema_version: 1`, `enabled`, `provider: "musicbrainz"`, `daily_request_limit: 80`, `recording_membership: true` | out of scope |
| `shelves` | `schema_version: 1`, `enabled`, `scope` | `enabled` is the collection-manager setting |
| `collections` | `schema_version: 1`, `backup_version: 1`, `enabled`, `scope` | `collection_manager.py:18-20, 53-57` |
| `catalog_mirror` | `contract_revision`, `catalog_schema_version: 3`, `analysis_schema_version: 2`, `catalog_builder_version`, `supported_core_range`, `supported_provider_types: ["navidrome"]`, `features: [...]` | `catalog_capability()`, 1753-1762. `features` is the static `CATALOG_FEATURES` list (120-162), which includes `profile_cursor_stream` and `source_scoped_profiles`. |
| `credits` | `credits_service.capability()` | out of scope |
| `transport` | `gzip: true` | **New in 1.3.0 (unreleased, K1).** Informational: gzip is negotiated per request through `Accept-Encoding` (§1.5). Absent in 1.2.5. |

**Keys that do not exist in 1.2.5.** A client must treat each of these as absent/false: `integrity` (top level, added in 1.3.0, P1-3), `transport` (added in 1.3.0, K1), `profile_stream`, `profile_bootstrap.sliding_expiry`, `profile_bootstrap.idempotent_create`, `profile_bootstrap.auth_enabled` (these three added in 1.3.0, P1-6), `edge_profiles.compact_transport`, `collections.feed_epoch`, `collections.contract`, `collections.source_scoped_items`, and `lumae_analysis_profiles`.

> Note: `lumae_analysis_profiles` is a **manifest** capability in `plugin.json` (with `schema_version`, `analyzer_version`, `profile_source`, `features`). It is not part of the health payload. See §9 item 1.

### Choosing v2 bootstrap (what Auralscape does today)

The client uses v2 only when all of these hold: `status == "ok"`, `profile_bootstrap.available`, `protocol_version == 2`, `schema_version == 1`, `auth == "host_authenticated"` and `transfer_contract == "source_scoped_v1"`. Otherwise it uses the legacy `/profiles/bootstrap`. Incremental sync always uses legacy `/profiles/changes`. This client behaviour (`profileBootstrapV2.ts:69-93`, `pluginEnrichmentSync.ts:264-325`) was *verified in the 2026-09-24 audit* (`docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md`; hand-off H.2); it cannot be checked from this repo.

### Discovering `catalog_instance_id`

Every profile route is scoped by `catalog_instance_id`. Clients get it from `GET /api/catalog/health` → `servers[].catalog_instance_id` (`__init__.py:1867` onwards).

### `GET /api/catalog/health` is read-only from 1.3.0 (unreleased, P2-1)

The response shape and values are unchanged. What changed is when its values are computed:
- The route still pings the provider on every request. It writes the provider-identity observation only when a stored field of it would change, and then commits that change itself, per server, before reading on. That is the case when:
  - the transition state, provider version, detection reason or required action changes;
  - a failed ping reports an error text different from the stored one (a ping that keeps failing the same way writes once);
  - while the state is `normal`, the observation's baseline follows the published catalogue and projection generations. So the first request after either publishes writes once.
- The route no longer re-inspects an applied transition's AudioMuse migration (`audiomuse_health`); the `provider_identity_recheck` cron refreshes it at minutes 2 and 32 of every hour.
- The counts behind `servers[].v3_readiness` (eligible, mapped and fingerprinted tracks, link counts) and therefore its `blockers` and `admission` come from a committed summary. It holds exactly what the former live query returned when it was taken:
  - link counts are written in the transaction that publishes a projection generation;
  - catalogue coverage is recounted right after every catalogue refresh and every projection, including one that publishes nothing.
  So an AudioMuse mapping or Chromaprint change appears with the next catalogue refresh or projection; every analysis run and every provider-migration recheck ends with one. For the moment between a publication and its recount, or when no summary describes the published generation and server, the route counts live, read-only, as 1.2.5 did.
- 1.2.5 answered in about 0.4 s p95 at 94k tracks, and the route wrote to the database on every request. 1.3.0 answers in about 5 ms p95, excluding the provider ping, and performs no write when nothing changed.

---

## 3. Profile endpoints

### 3.1 Waveform profile object (`serialize_profile`, `catalog_enrichment.py:428-457`)

| Field | Type | Meaning |
|---|---|---|
| `track_id` | string | provider track id |
| `source` | `"waveform"` | constant |
| `sample_rate`, `duration_ms` | int | |
| `ref_lufs` | number or null | analyzer v1 reference loudness at the stored `REAL` (float4) precision, emitted as a decimal that round-trips the float4 value; equal to PostgreSQL's text form for the LUFS range (`catalog_enrichment.float4`). Every path emits the same value since P1-1; events recorded by 1.2.5 still in the journal carry the analyzer's float64, which can differ in low digits. A non-finite value is `null`. |
| `start_ramp`, `end_ramp` | base64 string | MixRamp blobs |
| `analyzer_ver` | int | `1` in 1.2.5 |
| `analyzed_at` | ISO string without a zone (§1.6) | |
| `media_signature` | string or null | **equal to `media_revision`**. The internal signature is never published. |
| `media_revision` | `"sha256:<64 hex>"` or null | `opaque_revision(signature)` = `"sha256:" + sha256_hex(utf8(signature))` (`edge_profiles.py:49-52`) |
| `edge_profile` | object, optional | present **only** when the stored edge's `track_id` equals this `track_id` **and** its `media_revision` equals this row's `media_revision` (`catalog_enrichment.py:454-456`). The edge is joined on an exactly equal internal media signature, newest first (`edge_profile_store.py:39-47`). |

### 3.2 `GET /api/profiles` (direct fetch; `__init__.py:2507-2551`)

| Parameter | Rule |
|---|---|
| `catalog_instance_id` | optional when exactly one source exists; otherwise required. If it is missing, ambiguous or unknown: **409** `source_required` (2510-2514). |
| `ids` | comma-separated track ids. Each is trimmed; empty entries are dropped; duplicates are removed in first-seen order; the result is **truncated to the first 500** (`parse_ids`, 1380-1390). Ids past 500 are **silently ignored** and do not appear in `missing`. |

Response 200: `{schema_version:1, analyzer_version:1, catalog_instance_id, profiles:[profile…], missing:[id…], failed:[{track_id, reason}…]}`.
- `profiles`: published rows (`published_source_profiles`), with an edge when one matches (§3.1).
- `failed`: the latest attempt is `failed` or `skipped_no_file`, with `last_error` as the reason, or serialization failed.
  - A reason written by 1.2.5 is one of its failure categories: `download_unavailable`, `media_unavailable`, `silent_audio`, `unsupported_media`, `resource_limit`, `analysis_timeout`, `analysis_error`, `queue_unavailable`. It is `failed` when none is stored; rows carried over from pre-0.8 installs can still hold older free text.
  - From 1.3.0 (P3-8, LUM-018) the server records two finer categories, `media_error` (the decoder rejected the data) and `analysis_crash` (the analysis worker died). It reports them as `unsupported_media` and `analysis_error`, so no new reason reaches clients (§8.5). A hard-limit kill is reported as `analysis_timeout`, as before.
- `missing`: everything else, including pending work.

Headers: from 1.3.0 the 200 response has private cache headers (§1.5); in 1.2.5 it has none. It is gzipped under K1 when large enough (§1.5).

This route is read-only: it never schedules analysis. Use `POST /api/analyze` (below) for that.

### 3.3 `GET /api/profiles/bootstrap` (legacy keyset bootstrap; `__init__.py:2554-2574`, `catalog_enrichment.py:539-580`)

| Parameter | Rule |
|---|---|
| `catalog_instance_id` | **required**. Missing: 400 `invalid_profile_bootstrap`. Unknown or ambiguous: 400. |
| `page_token` | omitted on the first page; afterwards the `next_page_token` from the previous page |
| `limit` | default 250, clamped to **1–500**. Not an integer: 400. |

Response 200: `{schema_version:1, catalog_instance_id, profiles:[…], cursor, next_page_token|null, has_more}`.
- The first page pins the profile stream head. `cursor` is `opaque_cursor(source, epoch, pinned_head)` and is the same on every page.
- Pages walk the **live** published table in `track_id` order. This is not a snapshot: rows published during paging may or may not appear. Replaying `/profiles/changes` from `cursor` after the last page makes the result consistent.
- **410** `bootstrap_required`: the token's source or epoch no longer matches (551-553). **400**: malformed token.

### 3.4 `GET /api/profiles/changes` (profile journal; `__init__.py:2577-2597`, `catalog_enrichment.py:583-632`)

| Parameter | Rule |
|---|---|
| `cursor` | **required**. Missing: 400 `cursor_required`. Opaque: base64url (no padding) of canonical JSON `{"catalog_instance_id","epoch","seq"}` (`catalog.py:2118-2136`). Clients must not build or edit cursors. |
| `catalog_instance_id` | optional. If given, it must match the cursor's source, otherwise 400 `invalid_cursor`. |
| `limit` | default 250, clamped to **1–1000** (609). Not an integer: 400. |

Response 200: `{schema_version:1, catalog_instance_id, changes:[{seq, track_id, operation, payload, created_at}…], cursor, head_cursor, has_more}`.
- `operation` is `"upsert"` (with `payload` = the profile object from §3.1, frozen when the event was recorded) or `"delete"` (with `payload: null`) (`catalog_enrichment.py:465-481`).
- `cursor` = the last returned seq (or the input seq if nothing was returned); `has_more = next_seq < head_seq`.

Status codes:

| Status | Code | When |
|---|---|---|
| 410 | `bootstrap_required` | the cursor epoch ≠ the current epoch, **or** `cursor.seq < floor_seq` (the journal was compacted past it) (591-593), **or** (from P1-7) the page is not dense: with `cursor.seq < head_seq` the returned seqs are not exactly `cursor.seq+1 … cursor.seq+min(limit, head_seq − cursor.seq)` (an event is missing) |
| 400 | `invalid_cursor` | malformed cursor; the cursor belongs to another source; **the cursor is ahead of head** (594-596; unchanged by P1-7) |

From P1-7 (unreleased, ships in 1.3.0): the reader takes the stream state (epoch, head, floor) and the page of events in **one statement** (`catalog.read_change_page`: a CTE over `profile_stream_state` laterally joined with `profile_changes`), so both come from one snapshot. Before, the state and the events were two statements, and a compaction committing between them could return a page that silently started after a deleted event (LUM-001 gap F5). The page is then checked for density as above; a violation is the same 410 `bootstrap_required` the floor and epoch checks return. The response shape is unchanged. The catalogue (`GET /api/catalog/changes`), analysis (`GET /api/catalog/analysis/changes`) and relationship (`GET /api/catalog/relationships/changes`) change readers had the same two-statement pattern and use the same single-snapshot read and density check, with the same 410 `bootstrap_required` and 400 `invalid_cursor` (cursor ahead of head) codes; their `head_cursor`, `has_more` and, for the catalogue, `remaining_events`, `snapshot_generation`, `snapshot_entity_counts`, `snapshot_estimated_bytes` and `fingerprint_schema_version` now come from that same snapshot.

Retention: each publication compacts the journal to its last **50,000** events (`PROFILE_CHANGE_RETENTION_EVENTS`, `catalog_enrichment.py:50, 497`). The maintenance path uses `max(1000, 2 × profile_count)` (`catalog_enrichment.py:158-210`, line 193; `catalog.py:50-55`). A client more than about 50k events behind gets a 410 and must bootstrap again.

From P1-2 (unreleased, ships in 1.3.0): one retention limit per source, `profile_stream_state.retention_limit` = max(50,000, 2 × library), where the library is the larger of the published profiles and the catalogue's tracks. It is refreshed when the catalogue publishes a new generation (from that generation's track count) and by maintenance (`compact_enrichment_storage`, run at plugin start); the limit never drops below 50,000; publication reads it and never counts. Each publication deletes only the expired range of the current epoch; other epochs are purged in maintenance or on catalogue epoch rotation. The floor does not advance past the `snapshot_seq` of an unexpired v2 session whose catch-up has not captured its head, but never keeps more than 4 × the retention limit. So a client is at least one full library of events (and at least 50k) behind before it gets a 410. The client-visible contract (410 `bootstrap_required` when `cursor.seq < floor_seq`) is unchanged.

Since P1-1, event payloads are stored exactly as `serialize_profile` returns them (`catalog_enrichment._profile_json`: sorted keys, compact separators, no NaN), the same serializer that direct reads, bootstraps and v2 snapshots use, so an event and a read of the same row are equal. Events recorded by 1.2.5 went through `catalog.canonical_json` (`catalog.py:295-319`), which NFC-normalises and trims strings and turns empty strings into `null` (for example an empty ramp); such events can stay in the journal until compacted.

### 3.5 v2 profile bootstrap (`profile_bootstrap.py`; routes at `__init__.py:2600-2633`)

All four routes are `POST` with a JSON body of at most 16 KiB. Every body carries this **envelope**, which is validated by `_require_request` (104-123):

```json
{"protocol_version": 2, "schema_version": 1, "transfer_contract": "source_scoped_v1",
 "catalog_instance_id": "<1–512 chars>"}
```

The integers must be JSON integers; `true` or `2.0` is rejected.

| Route | Extra body fields | Success (200) body |
|---|---|---|
| `POST /api/profiles/bootstrap/sessions` (create; 220-289) | `page_size`: int **1–500**, default 250. It is fixed for the whole session. **From 1.3.0 (P1-6), both optional:** `expiry_mode`: `"absolute"` (default) or `"sliding"` (K3); `client_request_id`: a UUID string, at most 64 characters, compared case-insensitively (K5). Any other value of either field is 400. `null` means absent. | envelope + `session_token` (64 hex), `page_size`, `snapshot_count`, `total_profiles`, `catalog_epoch`, `profile_epoch`, `snapshot_seq`, `expires_at`, `snapshot_cursor`, `cursor` (= `snapshot_cursor`), `next_page_token` (always present, for ordinal 0) |
| `POST …/sessions/page` (292-312) | `session_token`, optional `page_token`; **`page_size` is forbidden** | envelope + `profiles:[…]`, metadata¹, `cursor` (= snapshot cursor), `next_page_token\|null`, `has_more` |
| `POST …/sessions/catchup` (315-382) | `session_token`, optional `page_token`; `page_size` forbidden | envelope + `changes:[{seq,track_id,operation,payload,created_at}…]`, metadata¹, `cursor` (last seq in this page), `head_cursor`, `next_page_token\|null`, `has_more` |
| `POST …/sessions/release` (385-397) | `session_token` | **only** `{protocol_version, schema_version, transfer_contract, released: true}`, with **no `catalog_instance_id`** (396-397). Clients must not apply full-envelope validation to the release response. |

¹ metadata = `catalog_epoch`, `profile_epoch`, `snapshot_cursor`, `snapshot_seq`, `total_profiles`, `expires_at` (213-217).

**Semantics:**
- **Create.**
  - 1.2.5: runs synchronously in the request, on its own connection, under the **global** session advisory lock `pg_advisory_lock(110094, 10)` (142), held for the whole capture, so a second create for *any* source waits up to 5 s and then gets 503.
  - **From 1.3.0 (P1-6, AUD-11)** it still runs synchronously on its own connection (`application_name` `lumae-profile-bootstrap`, TCP keepalives on), in three short steps:
    1. *Admission*, one transaction under `pg_advisory_xact_lock(110094, 10)` (held for a few statements, not the capture): the rate limit, K5 replacement, the slot check over **live** sessions only (below), and the insert of the session in state `capturing`. It commits before the capture, so the session holds its slot, and with the admission head as a lower bound of `snapshot_seq` it also holds the P1-2 journal floor while the capture runs.
    2. *Purge* of sessions that are not live, without the global lock: expired, identity-stale (source inactive or rebound, core server, catalogue epoch or profile epoch changed), replaced by K5, or still `capturing` 10 minutes after admission (an abandoned capture). Their snapshot and catch-up rows go with them. Each create purges at most 2 such sessions (oldest expiry first), and a failed purge is logged and never fails the create; dead sessions hold no slot while they wait.
    3. *Capture* under a **per-source** lock `pg_advisory_lock(110094, hashtext(catalog_instance_id))`, unlocked explicitly afterwards. Creates for different sources capture concurrently. A second create for the same source waits for the first capture for **up to 5 s**, instead of failing at once, and then gets 503 with `Retry-After: 5`. The wait plus a capture (about 3 s at 94k profiles) fits a 10 s client request timeout, so a create does not finish after its client gave up. If the capture fails, its session is deleted.
  - A session is **live** when it is unexpired, not an abandoned capture, and its source is still `active` with the session's core server, catalogue epoch and profile epoch. Only live sessions count toward the 4-per-source and 32-global limits or hold the journal floor. The rest would answer 410 anyway.
  - Captures every published profile of the source in one REPEATABLE READ snapshot, **with each row's edge embedded at capture time** (250-275), and pins `snapshot_seq` = the profile head at capture.
  - From P1-5 (K2, unreleased, ships in 1.3.0): the capture stores each row's waveform payload plus an edge **reference** (column `edge_ref`), instead of a copy of the edge. The reference names the edge row `(media_revision, profile_digest)` that `edge_join()` picks. A snapshot stores a reference only when that edge's `media_revision` equals the row's own (the only case in which an edge is embedded), so snapshot references are always `{"profile_digest": …}` and the row's `media_revision` completes the key. The edge is resolved at page read (below). This is server-internal; the wire format does not change.
  - From P2-4 (unreleased): the snapshot rows and the first catch-up's rows are built in SQL (`INSERT … SELECT`, batches of 5,000 profiles or journal events) instead of one row at a time in the web worker's Python. The stored JSON, the pages and the byte caps are the same as before, and an equivalence test keeps the Python capture as its oracle. A 94k capture takes about 2 s and no longer holds the worker's GIL, so captures of different sources run in parallel. From P2-4b, a profile whose `ref_lufs` is not an ordinary level (NaN or infinite, at least 2^23 in magnitude, or non-zero below 1e-4; analyzed loudness is in practice never one of these, only a level within 1e-4 LUFS of 0 could be) is still serialized in Python, by `serialize_profile` itself. The page queries choose their rows before the edge lookup, so a fresh table without statistics can no longer make a page look up every remaining row's edge. Server-internal; no wire change. The snapshot batches share the capture's REPEATABLE READ snapshot. The first catch-up reads its interval in several statements rather than through one cursor. An event deleted meanwhile still answers 410 as a gap, never a silent one. The floor hold keeps every event of the interval while the session is live and the interval is within the hold's cap (4 × `retention_limit`). The hold stops covering the interval, and a compaction during the capture can delete events of it, when:
    - at least (cap − interval) more events are published during the capture;
    - the session stops being live (it expires, or its source's identity changes);
    - maintenance lowers `retention_limit`.
  - 1.2.5 deletes expired sessions first (229); 1.3.0 purges as in step 2.
  - **Idempotent create (K5, 1.3.0; gate `idempotent_create`).** When the body has a `client_request_id`, an unexpired session of the same source with the same id that has not served a page or catch-up yet (`pages_served = 0`) is **replaced**: it expires at once and is purged, and the new session takes its slot. This also applies while the earlier create is still capturing; that request then ends with 410. A session that has served a page is claimed and never replaced. Send one UUID per logical create and reuse it when retrying a create that timed out.
  - **Rate limit (K4, 1.3.0).** At most **6 admitted creates per 10 minutes per (source, caller)**. The caller is the host account (`g.auth_user`), else `bearer` for the installation token, else `anonymous` (all anonymous clients share one budget). A create over the limit is 429 `bootstrap_session_limit` with `Retry-After` = the seconds until the oldest counted create leaves the window (capped at 300). Refused creates are not counted.
- **Session lifetime.**
  - 1.2.5: fixed at **60 minutes from creation** (`now() + interval '60 minutes'`, 245). Pages do **not** extend it. (`SESSION_MINUTES` at line 26 is not used.)
  - 1.3.0: one constant, `SESSION_MINUTES = 60`, for both modes. **Absolute** (the default): unchanged, 60 minutes from creation. **Sliding** (K3, `expiry_mode: "sliding"`, gate `sliding_expiry`): every successful page or catch-up sets `expires_at = LEAST(now() + 60 minutes, created_at + 24 hours)` (never earlier than it already was), and that response's `expires_at` shows the new value. A sliding session therefore lives while the client keeps paging at least once an hour, and at most 24 hours in total. Clients must accept a changing `expires_at` in sliding mode.
- **Tokens.**
  - `session_token` is a bearer secret; the server stores only its SHA-256.
  - A `page_token` is `base64url(json{s,p,o,z}).hex_hmac_sha256`, at most 2,300 characters (77-101). It is bound to the session, the phase (`snapshot`/`catchup`), the ordinal and the page size.
  - The first page of either phase may omit `page_token` (ordinal 0).
- **Page.**
  - Reads the frozen snapshot in ordinal order.
  - The ordinal must be ≤ `snapshot_count` and a multiple of `page_size`, otherwise 400 (298-299).
  - Profiles are the frozen JSON, including an edge that may have been replaced since.
  - From P1-5 (K2): the page joins each row's reference to `edge_profiles` on `(catalog_instance_id, track_id, media_revision, profile_digest)`. While that edge is still published, the row is **byte-identical** to the pre-K2 frozen JSON with the edge embedded. **If the edge was replaced or withdrawn after capture, the row arrives without `edge_profile`**; the catch-up interval (or `/profiles/changes` after `head_cursor`) contains the replacing event. Clients already treat an upsert without an edge as "no edge". Sessions captured before the upgrade (NULL `edge_ref`) keep paging their frozen JSON unchanged.
  - From P2-3 (unreleased, ships in 1.3.0): when a catalogue publication withdraws a profile (new media or a removed track), its edge payload is deleted in a short transaction right after the publication commits, not inside it: a sweep of the edges that no published profile of the same signature reaches. Until that sweep runs (if it fails: until the next publication or the next maintenance run, `compact_enrichment_storage`), a snapshot or catch-up page can still embed the withdrawn track's own old-revision edge in its older rows, as the pre-K2 frozen JSON did; the withdrawal's delete event follows in the journal either way. Direct fetches and the legacy bootstrap read edges through the published profile, so they never return it. The wire format is unchanged.
- **Catch-up.**
  - The first call (ordinal 0) materialises the journal from `snapshot_seq` to the **current** head into the session. It returns 410 if `snapshot_seq < floor_seq` or if a seq gap is found (324-360).
  - Later calls page that frozen set.
  - From P1-5 (K2): an upsert event is stored as its waveform payload plus an edge reference and resolved at page read like the snapshot. The reference also carries `media_revision`, but only when the journalled edge names a different revision than the event payload. So an event whose edge was replaced after it was journalled arrives without `edge_profile`; the later event that replaced it (in the interval or after `head_cursor`) carries the current edge. Delete events are unchanged.
  - The final `cursor` equals `head_cursor`. Continue incremental sync with legacy `/profiles/changes` from it.
- **Session validity (every page and catch-up; in 1.2.5 also release; `_session`, 181-201).** The token must exist, the body's `catalog_instance_id` must match, the session must not be expired and must be schema 1, and the source must still be `active` with an unchanged core server id, catalog epoch and profile epoch. Otherwise the answer is **410**.
- **Release.** 1.2.5 validates the session like a page first, so releasing an expired or stale session returns 410 and **leaves the row**, which keeps its slot until it expires (AUD-11). **From 1.3.0 (P1-6)** release deletes the session matching the token hash **and** `catalog_instance_id`, whether it is live, expired or identity-stale, and always answers 200 `released: true` for a valid request: also for an unknown or already-released token, and for another source's token (which deletes nothing). Release is the way to free a slot at once; always release a session you no longer page.

**Status codes** (the `error` code equals the `message`):

| Status | Code | Cause |
|---|---|---|
| 400 | `invalid_profile_bootstrap` | bad envelope; `page_size` outside 1–500 or not an int; `page_size` on page/catchup/release; malformed, forged, wrong-phase or misaligned `page_token`; `session_token` not 64 lowercase hex; body > 16 KiB (from 1.3.0 also a chunked body), not JSON or not an object; from 1.3.0 an invalid `expiry_mode` or `client_request_id` on create |
| 410 | `bootstrap_required` | **create:** the source is unknown, inactive or has no core server id (`_state` 176-177, called at 236). The 429 slot check (233-235) runs first, so a host whose slots are full answers 429 even for an unknown source. From 1.3.0 also: the source's identity changed between admission and capture, or a retried create (K5) replaced this one. **Page/catchup/release:** unknown token (page/catchup), expired session, source inactive or rebound, epoch changed, catch-up floor passed or gap. **Release** in 1.2.5 of an existing but expired or stale session also returns 410, and the row is **not** deleted: it keeps its slot until it expires (393). From 1.3.0 release never returns 410 (see Release above). |
| 413 | `bootstrap_snapshot_limit` | create: > **200,000** rows or > **128 MiB** of compact JSON (edges included). The first catch-up: > **50,000** events or > 128 MiB (270-271, 352-353). Nothing is kept, so a retry fails the same way. Fall back to legacy bootstrap. **From P1-5 (1.3.0):** both byte caps count waveform JSON only (edges excluded), so a 94k library with edges fits (about 45 MB of waveform JSON). The first catch-up admits `4 × max(50,000, retention_limit)` events, which is the P1-2 floor-hold cap, and `max(128 MiB, 1 KiB × that event limit)` bytes. A session the floor hold kept readable is therefore never refused. |
| 429 | `bootstrap_session_limit` | ≥ **4** unexpired sessions for the source or ≥ **32** globally (233-235). There is **no `Retry-After`** in 1.2.5. **From 1.3.0 (K4):** only live sessions count (identity-stale and abandoned ones never do), and the same code also answers a create over the rate limit (6 per 10 minutes per source and caller). Every 429 carries `Retry-After`: for a full source or host, the seconds until the earliest counted session expires (an abandoned capture counts until 10 minutes after admission); for the rate limit, until the oldest counted create leaves the window. Always a whole number from 1 to 300. |
| 503 | `bootstrap_unavailable` | `config.DATABASE_URL` is unset (2601-2602); any database error, statement timeout (20 s), lock timeout (5 s; a second concurrent create waits up to 5 s for the global lock), or any other unexpected exception (156-157, 2612-2613). Nothing is logged. There is no `Retry-After`. **From 1.3.0 (K4, P1-6):** every 503 carries `Retry-After: 5`. The admission lock is held only briefly, and a create waits up to 5 s for another capture of the same source before a 503. Connection failures (including a malformed `DATABASE_URL`) and timeouts are logged as a warning with only the error class; any other exception is logged with its class and traceback (never the session token or the DSN) and is still 503. |

> Note: the v2 routes never return **404** or **409**. A 404 means the route does not exist (the plugin predates v2): treat v2 as unsupported. 409 is not used by v2.

Release is idempotent for tokens the server does not know: it returns 200 `released:true` for an unknown or already-released token. From 1.3.0 it also deletes expired and stale sessions and never returns 410 (see Release above).

### 3.6 Analysis requests

- **`POST /api/analyze`** (`__init__.py:2721-2758`). Body: `{catalog_instance_id?, ids:[…]}`. The ids are parsed like §3.2, then capped at **12** (`MAX_INTERACTIVE_PROFILE_IDS`). The work is enqueued at high priority in chunks of 3.
  - 202: `{accepted, already_ready, already_pending}`.
  - 409 `source_required`; 503 `maintenance_paused`.
- **`POST /api/profiles/edges/analyze`** (`__init__.py:2797-2807`). Body: `{catalog_instance_id?, ids:[string 1–512 chars], ≤100}`.
  - 202: `{available, accepted:[track_id…], already_ready:[track_id…]}`.
  - When edges are disabled or maintenance is paused: 202 with `{available:false, accepted:[], already_ready:[]}`.
  - 400 `invalid_edge_request` for a bad body, or a missing, ambiguous or unknown source.
  - Only ids with a published waveform row and a non-null revision are considered. A row counts as "ready" when its current edge has schema 2 and the current method.
  - Jobs coalesce. They are claimed again when the revision changes, after 30 minutes stale pending/running, 2 s after an enqueue failure, or 6 hours after any other terminal state (`edge_profile_store.py:50-83`).
  - **If enqueueing raises**, the claimed jobs are marked failed with `edge-enqueue-failed` and the exception propagates (`__init__.py:2778-2781`). The analyze and backfill routes catch only `KeyError`/`ValueError`/`CatalogScanError` (`__init__.py:2806, 2825`), so the client gets the **host's HTML 500**, not a JSON envelope. The jobs can be claimed again 2 s later.
- **`POST /api/profiles/edges/backfill`** (`__init__.py:2810-2826`). Body: `{catalog_instance_id?, after?: string ≤512, limit?: int 1–100}`.
  - 202: the analyze body plus `next_after`.

A successful waveform analysis also schedules an edge upgrade automatically (`_schedule_edge_upgrade`, `__init__.py:2785-2794`).

---

## 4. Edge profile payload (EdgeProfileV2)

Producer: `analyze_edge_blocks` (`edge_profiles.py:320-416`). Golden fixture: `tests/plugins/edge_profile_v2_golden.json`. Measurement semantics: `plugins/LumaeAnalysis/EDGE_PROFILES.md`.

### 4.1 Fields

| Field | Content |
|---|---|
| `schema_version` | `2` |
| `analyzer_version` | `"lumae-edge-v2.0.0"` |
| `catalog_instance_id`, `track_id` | source identity (1–512 chars) |
| `media_revision` | `"sha256:<64 hex>"`. It equals the owning waveform row's `media_revision` (§4.3). |
| `representation_id` | `"sha256:" + content_sha256` |
| `content_sha256` | 64 hex: SHA-256 of the analysed file bytes, checked before and after decoding |
| `source` | `{sample_rate, channel_layout: "mono"\|"stereo", decoded_frames, decoder, padding: "decoder-output-v1", timeline_verified}`. `sample_rate` is the **source** rate (10–384,000). |
| `measurement` | `{method, sample_rate: 48000, channel_rule: "mean-power", quantization: "s16le-centidb-u16le-q15-v2", resampler, true_peak_oversample: 4, crossover_hz: [150, 2500]}` |
| `leading_silence` | `{frames, method: "digital-zero", verified: true}` |
| `noise_floor_cdb` | int |
| `landmarks` | `{audible_start_frame, body_start_frame, body_end_frame, audible_end_frame, leading_padding_frames, trailing_padding_frames, confidence_q15, terminal_silence_confidence_q15, hidden_content_guard}` |
| `head`, `tail` | window: `{origin_frame, covered_frames, bin_count, boundaries:[int…], valid, level_cdb, peak_cdb, true_peak_cdb, low_power_cdb, mid_power_cdb, high_power_cdb, spectral_flux_q15, onset_density_q15}` (`edge_profiles.py:238-252`) |
| `profile_digest` | 64 hex (§4.2) |

- Windows:
  - `head`: origin `0`, end `min(decoded_frames, 30·sample_rate)`.
  - `tail`: origin `max(0, decoded_frames − 30·sample_rate)`, end `decoded_frames` (`edge_profiles.py:395-396`).
  - `covered_frames = end − origin`. On short files the head and tail are identical.
- Arrays are base64 of little-endian int16 centidB (`-32768` = exact digital zero) or uint16 Q15. `valid` is an LSB-first bitmask with `bin_count` bits.
- The payload contains only strings, integers, booleans, lists and objects, with **no floats**. Its size is about 19 KB of compact JSON for a full-length track.

### 4.2 Digest

```
profile_digest = sha256_hex( utf8( canonical_json( payload without the key "profile_digest" ) ) )
canonical_json = JSON with keys sorted at every level, separators "," and ":",
                 no insignificant whitespace, non-ASCII emitted as UTF-8 (ensure_ascii=False),
                 NaN/Infinity forbidden
```

Source: `edge_profiles.py:45-56`. Everything else is hashed, **including both `boundaries` arrays**, identity, `source` and `measurement`. The server checks the digest again before publication (`edge_profile_store.py:103-106`). Payloads are stored as JSONB, so wire key order is arbitrary: a verifier must sort the keys again.

### 4.3 `media_revision` rule

- `media_revision = "sha256:" + sha256_hex(utf8(internal_media_signature))` (`edge_profiles.py:49-52`). A catalogue-backed signature is `catalog-media:<media_fp>`.
- An edge is published only if all of these hold: the job's revision equals the revision of the waveform row's current signature; that signature equals the catalogue's current `media_fp`; and the job token is still current (`edge_profile_store.py:100-161`).
- An edge is served only next to a waveform row with the **same** `media_revision` and `track_id` (§3.1).

### 4.4 `boundaries` derivation (verified)

The code (`edge_profiles.py:146-152`) is:

```python
def _boundaries(sample_rate, origin, end):
    result = [origin]
    index = 1
    while result[-1] < end:
        result.append(min(end, origin + index * sample_rate // 10))
        index += 1
    return result
```

Python evaluates `index * sample_rate // 10` as `(index * sample_rate) // 10`. So, for each window:

```
rate   = source.sample_rate            (the SOURCE rate, not measurement.sample_rate)
origin = window.origin_frame
end    = window.origin_frame + window.covered_frames
boundaries[i] = min(end, origin + floor(i * rate / 10))     for i = 0, 1, …, bin_count
len(boundaries) = bin_count + 1;  boundaries[0] = origin;  boundaries[bin_count] = end
```

- Use integer arithmetic (`floor` of an exact integer product).
- The equivalent form in the plan's K7 derives each window's `origin`/`end` from `source.decoded_frames` and the 30-second rule in §4.1. It gives identical results.
- This was checked against the code for rates 10 Hz–384 kHz and window lengths from 1 frame to 61 s, and against the golden fixture (48 kHz, 10,003 frames, 4 boundaries per window). The fixture's digest also verifies with the §4.2 rule.

---

## 5. Collections and shelves

All of these routes return **404 `{"error":"collection_manager_disabled"}`** when the collection manager setting is off (`collection_manager.py:611-618`). Data is scoped by principal (§1.2).

### 5.1 Collection and item objects

- Collection: `{id, name, description, revision, created_at, updated_at, deleted_at, album_count, track_count}` (`collection_manager.py:256-265`).
- Item: `{id, collection_id, kind: "album"|"track", track_id, provider_album_id, album_key, title, artist, album, cover_item_id, position, added_at, updated_at}` (281-292).
- Items carry **no `catalog_instance_id`** in 1.2.5 (K10).

### 5.2 `GET /api/collections/changes` (`collection_manager.py:1062-1097`)

| Parameter | Rule |
|---|---|
| `cursor` | integer; default 0; negative values clamp to 0. Not an integer: 400 `invalid_cursor`. |
| `limit` | integer, default 200, clamped to **1–500**. Not an integer: also 400 `invalid_cursor` (same `try` block, 1065-1069). |

Response 200: `{changes:[{seq, collection_id, entity_kind: "collection"|"item", entity_id, operation: "upsert"|"delete", payload, created_at}…], next_cursor}`.
- `seq` values come from one installation-wide counter (`collection_feed_state.head_seq`, 484-501). One principal's seqs therefore have **gaps**. They are strictly increasing.
- Only events with `seq ≤ head_seq` are returned.
- `next_cursor` = the last returned seq, or the request cursor if the page is empty.
- There is **no `has_more`, no `epoch` and no `head_seq`** in the response. Page until `changes` is empty.
- It **never returns 410**. A cursor past head returns an empty page and echoes the cursor, so a client cannot detect a server-side reset (K8).
- 503 `collection_feed_unavailable`: the feed state row is missing or has an unknown protocol.
- The journal and idempotency receipts are never compacted.

Payloads:
- collection upsert: the collection object;
- collection delete: `{id, revision}`;
- item upsert: the normalised item plus `collection_revision` and `collection_updated_at`;
- item delete: `{id, collection_id, collection_revision, collection_updated_at}`.

### 5.3 Mutations

Every mutation runs through `_mutation_response` (546-608).

**Idempotency.**
- Optional header `Idempotency-Key`, trimmed and truncated to 200 characters. It is scoped per principal and serialised with a transaction advisory lock.
- The request fingerprint is SHA-256 over the method, the path, the canonical JSON body and the `If-Match` header (present or absent, and its value) (519-530).
- A replay with the same fingerprint returns the stored status and body, with the header `Idempotency-Replayed: true`.
- A different fingerprint under the same key returns **409 `{"error":"idempotency_key_conflict"}`**, without `current` (K9).
- Only **2xx** results are stored. A 4xx is not recorded, so retrying the same key runs the request again.
- A receipt from before fingerprinting existed replays with `Idempotency-Fingerprint: legacy-unbound`.

**Optimistic concurrency.**
- The expected revision comes from `If-Match` or body `base_revision` (505-512). If it is absent, empty or `*`, it is not checked. A value that is not an integer never matches.
- A mismatch returns **409 `{"error":"revision_conflict","current":<collection>}`**.

**Other errors:**
- 503 `unsupported_transaction_isolation` (the host connection is not READ COMMITTED);
- 503 `collection_feed_unavailable`;
- **503 `collection_feed_invariant`** (new in 1.3.0, P1-3): a committed change row sits past the feed head (`MAX(seq) > head_seq`, for example left by a 1.2.5 worker before the upgrade fence). The check runs inside each mutation against the head it has just locked; the mutation rolls back and nothing is written. Every collection write, for every principal, returns this until an operator runs the repair in `docs/runbooks/UPGRADE_1.3.md`; health reports it as `integrity.collections_feed_ok: false`. There is no `Retry-After`. Clients treat it like `collection_feed_unavailable`: keep the mutation queued and retry later. 1.2.5 instead failed these writes with a 500 (a unique violation) indefinitely (AUD-05);
- 409 `item_id_collection_conflict` (the item id already belongs to another collection of this principal).

| Route | Body | Success | Notes |
|---|---|---|---|
| `POST /api/collections` (783-810) | `{id?, name (1–120), description? (≤1000)}` | 201 `{collection}` | **An existing id is not an error:** `ON CONFLICT DO NOTHING`, 201 with the existing collection (or `collection: null` if it is tombstoned), and no change event. K9 changes this to 409. |
| `PATCH /api/collections/<id>` (840-871) | `{name?, description?, base_revision?}` | 200 `{collection}` | 404 `collection_not_found`; revision +1 |
| `DELETE /api/collections/<id>` (874-901) | `{base_revision?}` | 200 `{deleted:true, id, revision}` | Soft delete (tombstone). A **missing** collection always returns 200 `{deleted:true}` (880-882). An **already-deleted** collection returns 200 `{deleted:true}` only if no revision is sent or it matches. The revision check comes **before** the tombstone check, so a mismatched `If-Match`/`base_revision` returns **409 `revision_conflict` with `current`** (the tombstoned collection) (883-887). |
| `PUT /api/collections/<id>/items/<item_id>` (904-908) | item fields | 200 `{collection, items:[item]}` | same as batch with one item. See the returned-item shape below. |
| `POST /api/collections/<id>/items/batch` (911-917) | `{items:[≤500], base_revision?}` | 200 `{collection, items}` | More than 500 items or not a list: 400. Revision +1 per request, **even when `items` is empty**: an empty batch still bumps the revision and returns 200 (915, 928-935). |
| `DELETE /api/collections/<id>/items/<item_id>` (964-1005) | `{base_revision?}` | 200 `{deleted: bool, collection}` | A missing item gives `deleted:false` and no revision bump. |
| `DELETE /api/collections/<id>/items/batch` (1008-1059) | `{item_ids:[1–500], base_revision?}` | 200 `{deleted:[ids], deleted_count, collection}` | |
| `POST /api/collections/restore` (767-780) | backup document | 201 `{restored:true, …}` | additive restore |

Read routes:
- `GET /api/collections` (732-753) returns `{schema_version, scope, collections}`;
- `GET /api/collections/<id>` (813-824) returns `{collection, items}`, or 404;
- backup, export and search are also available.

**Item normalisation and membership (`_normalize_item` 637-659, `_upsert_item` 666-726).**
- `kind` must be `album` or `track`. A track needs `track_id`. An album needs `provider_album_id` or `album_key`.
- `id` defaults to a new UUID. `position` is clamped to ≥ 0.
- **Returned items (PUT and batch) are the `_normalize_item` shape, not the stored row** (648-659, returned from the handler after 945): `{id, kind, track_id, provider_album_id, album_key, title, artist, album, cover_item_id, position}`.
  - There is **no `collection_id`, `added_at` or `updated_at`**.
  - `artist` is `""` (never `null`) when absent.
  - `position` is the clamped request value, not a re-read.
  - Item upsert change events carry this shape plus `collection_revision` and `collection_updated_at`.
- **Duplicate membership is remapped silently.** If the collection already holds the same track (or album key), the request's item `id` is **replaced by the existing item's id**. The response and the change event carry the existing id, and no error is returned (K9 changes this to 409 `membership_conflict`).

### 5.4 Shelves (`shelves.py`)

- **`GET /api/shelves/changes`**, and the alias **`GET /api/shelves/snapshot`**, which behaves identically (198-219).
  - Parameters: `catalog_id` (required, 1–512 chars), `cursor` (int ≥ 0), `limit` (1–500, default 250).
  - Response: `{records:[{type: "member"|"order"|"evidence", id, value}…], cursor, hasMore}`.
  - The feed is compact: each record keeps only its latest value and seq, and tombstones are kept as `deletedAt`.
  - 400 for bad parameters.
- **`POST /api/shelves/mutations?catalog_id=…`** (221-247). Body: `{id, operation: "add"|"remove"|"restore"|"order"|"evidence", …}`, validated by `validate_mutation` (47-102).
  - 200 `{records:[…]}`.
  - 409 `order_conflict` (with `order`) or `unknown_membership_period`.
  - 400 for validation errors.
  - **Idempotency:** the receipt is keyed by the body `id` alone, scoped to (principal, catalogue). A replay returns the stored response even if the rest of the body is different; there is no fingerprint check.

---

## 6. Client rules already relied on

These rules hold against 1.2.5 and **must keep holding** for clients that do not opt in to anything.

1. **An upsert without a valid `edge_profile` deletes the local edge.** A profile upsert (bootstrap row, v2 snapshot row, `/changes` or catch-up `upsert` event, or direct fetch) that has no `edge_profile`, or has one that fails validation or digest verification, **removes** the client's stored edge for that track (Auralscape `publishedProfileRepo.ts:266, 351-388`; *verified in the 2026-09-24 audit*, `docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md`, not checkable from this repo). The server relies on this in three places:
   - **Waveform republish** (P1-1). The published 8-tuple (`sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, media_signature`) is compared at stored precision (`ref_lufs` as float4). An identical completion is a no-op: no head change, no event, edge rows untouched. If the tuple differs and `media_signature` changed (or there was no published row), the server deletes the edge and its job and emits an upsert **without** an edge; the edge upgrade is then scheduled for the new media. If only the waveform changed on the same media, the edge and its job are kept and the upsert **embeds the current edge** (`serialize_profile(..., edge_profile=<edge_join>)`), so clients keep it. The analysis hook also skips admission when the published row is already current (same media fingerprint, analyzer and schema version, not failed).
   - **Rekey.** A provider-identity rekey emits `delete(old)` + `upsert(new)` without an edge (`profile_publication.py:462-503`).
   - **Edge publication** emits a new `upsert` carrying the waveform fields **and** the edge (`edge_profile_store.py:155`).

   K6 is the only planned exception, and only for clients that opt in.
2. **`delete` removes the profile and its edge** (`profile_publication.py:64-77`).
3. **Apply events in `seq` order and advance the cursor atomically with the local write.** An event payload is frozen when it is recorded. A later event for the same track supersedes it.
4. **Handoff between v2 and legacy.** After the v2 catch-up finishes, continue with `GET /api/profiles/changes?cursor=<final catch-up cursor>`.
5. **An edge is valid only for the waveform row's `media_revision` and `track_id`.** Verify `profile_digest` (§4.2) before storing an edge.
6. **Resync triggers.** For profiles, a 410 `bootstrap_required` from `/changes` or legacy bootstrap means bootstrap again. A 410 from v2 means the session is gone: create a new one or fall back to legacy. Treat a 400 "cursor ahead of head" on `/changes` as a resync too (the server lost state).
7. **Authentication** is handled as in §1.2: a 3xx or HTML response means `authentication_required`.
8. **Collections: after a successful write, adopt the item ids the server returns**, because it may have remapped them (§5.3).

---

## 7. Planned changes (K1–K11)

Copied from plan §2. **Every server change is additive or opt-in.** Each work package that ships one changes its **Status** to `shipped in <version>`, and updates §2–§6 in the same PR.

| ID | Change | Server WP | Client | Gate | Phase | Status |
|---|---|---|---|---|---|---|
| K1 | Gzip transport for JSON ≥1 KiB | P1-4 | Verify decoding; fix `Content-Length` guards (C-7) | HTTP `Accept-Encoding` (native stacks send it); informational `capabilities.transport.gzip` | 1 | shipped in 1.3.0 (unreleased) |
| K2 | v2 snapshots store an edge *reference* and resolve it at page read. Wire format unchanged, except a row whose edge was replaced after capture arrives without `edge_profile` (the catch-up re-supplies it). | P1-5 | None; already handled | None | 1 | shipped in 1.3.0 (unreleased): snapshot and catch-up rows store `edge_ref`; a row or event whose edge was replaced or withdrawn after capture arrives without `edge_profile`, and the replacing event re-supplies it (§3.5) |
| K3 | v2 sliding expiry: each page extends `expires_at` to at most `created+24h` | P1-6 | Send `expiry_mode:"sliding"`; accept a changing `expires_at` (C-6) | `capabilities.profile_bootstrap.sliding_expiry:true`; create body field | 1 | shipped in 1.3.0 (unreleased): each page or catch-up of a sliding session sets `expires_at` to `LEAST(now()+60 min, created_at+24 h)` and returns it; absolute mode unchanged (§3.5) |
| K4 | `Retry-After` on 429 and 503; truthful `available`; new `auth_enabled` field (the `auth` string is unchanged) | P1-6 | Back off and honour `Retry-After` (C-3) | Always additive (`capabilities.profile_bootstrap.auth_enabled`) | 1 | shipped in 1.3.0 (unreleased): `Retry-After` 1–300 on 429 and 5 on 503; create rate limit 6 per 10 min per (source, caller); release always deletes and answers 200; `available` is a cached probe; `auth_enabled`; UTC timestamps (§1.5, §1.6, §2, §3.5) |
| K5 | Optional create `client_request_id`; a duplicate unclaimed session is replaced, not leaked | P1-6 | Send a UUID per create attempt (C-3) | `capabilities.profile_bootstrap.idempotent_create:true` | 1 | shipped in 1.3.0 (unreleased): a create with the same `client_request_id` replaces the source's unexpired session with that id that has served no page yet, also while it is still capturing (§3.5) |
| K6 | **Edge references in events and pages:** with `edge_refs=1` (query) or `edge_refs:true` (v2 body), an upsert whose edge is unchanged carries `edge_profile_ref:{media_revision, profile_digest}` instead of the full edge. Without the opt-in, the server expands to the full edge exactly as today. | P3-2 | Keep the local edge when digest and revision match; fetch misses through `GET /api/profiles?ids=` (C-10) | `capabilities.profile_stream.edge_refs:true` (new `profile_stream` object) | 3; must ship before P3-1 regeneration | planned |
| K7 | Compact edge transport (optional): with `edge_compact=1` the server omits the derivable `boundaries`, and the client rebuilds them (§4.4) **before** verifying the unchanged v2 digest | P3-3 | Rebuild, then verify (C-12) | `capabilities.edge_profiles.compact_transport:true` | 3, optional | planned |
| K8 | **Collections feed:** the response adds `epoch`, `head_seq` and `has_more`. 410 `collections_resync_required` is returned **only** when the request echoes `epoch` and it mismatches, or when the cursor is past head. A new snapshot endpoint, **planned path `GET /plugins/lumae_analysis/api/collections/snapshot`** (P3-4 implements exactly this path), returns all of the principal's collections, items and head in one REPEATABLE READ transaction. | P3-4 | Echo the epoch; resync on 410; page by `has_more`/`next_cursor` (C-13) | `capabilities.collections.feed_epoch:true` | 3 | planned |
| K9 | **Collections conflicts:** with header `X-Lumae-Collections-Contract: 2`, `idempotency_key_conflict` includes `current`, and a duplicate membership returns 409 `membership_conflict {existing_item_id}` instead of a silent id remap. Create with an existing id returns 409. | P3-4 | Handle both; freeze reorder bodies at enqueue (C-13) | `capabilities.collections.contract:2` | 3 | planned |
| K10 | Collection items carry `catalog_instance_id` (LUM-013, additive); workbench routes take an explicit catalogue | P3-5 | Store and scope items (C-13) | `capabilities.collections.source_scoped_items:true` | 3 | planned |
| K11 | Profiles may carry `analyzer_ver:2` (BS.1770-4 `ref_lufs` and new ramps) | P3-1 | Accept v1 and v2; normalise by version (C-11) | `capabilities.lumae_analysis_profiles.analyzer_versions:[1,2]`, `loudness_method:"bs1770-4"` (**new health key**; see §9 item 1) | 3 | planned |

**Unchanged by design:**
- v2 `page_size` stays client-chosen (1–500). Byte-sized pages are a client choice: use `page_size` about 50 when edges are present.
- The legacy bootstrap `limit` (1–500) and the `/changes` `limit` (1–1000) also stay client-chosen.

---

## 8. Compatibility rules

1. **Additive or opt-in only.** An unchanged client keeps working against every new plugin, and a new client keeps working against 1.2.5 and 1.3.0. No field is removed or renamed, and no existing status code changes meaning for a request that has not opted in.
2. **Capability-gated.** A client enables new behaviour **only** when a health capability flag (§2, §7) is present and truthy, or when it sends an explicit request field or header. A missing key means "not supported".
3. **Opt-in signals are safe against old servers.** 1.2.5 ignores unknown query parameters (`edge_refs`, `edge_compact`, `epoch`), unknown body fields (`expiry_mode`, `client_request_id`, `edge_refs`) and unknown headers (`X-Lumae-Collections-Contract`) (§1.3). The one exception: **never send `page_size` on v2 page/catchup/release** (400). Even so, a client must not *assume* a behaviour because it sent the signal. It checks the capability first, and the response shape second.
4. **Default output is frozen.** Without an opt-in, a newer server emits byte-for-byte the same shape as today, apart from additive keys. This includes full `edge_profile` expansion, `boundaries`, and the collection feed shape.
5. **New keys and values.** Clients ignore unknown JSON keys, unknown capability keys and unknown `features` entries. Enum-like values (`operation`, `entity_kind`, error codes) may gain members only behind a gate.
6. **Versions.** `schema_version` or `protocol_version` changes only for a breaking change, and a breaking change needs a new route or a new gated protocol, never an in-place change. `analyzer_ver` can take new values only behind K11.
7. **Every contract WP** updates this file (§2–§7 and the K status) in the same PR, with tests that pin the old default behaviour.

---

## 9. Code-versus-plan notes

The code wins. Each item names the WP expected to act on it.

1. **K11 gate location.** `lumae_analysis_profiles` exists only in `plugin.json` (manifest capabilities). **Health has no such key.** P3-1 must add a new `capabilities.lumae_analysis_profiles` object to `/api/health` (additive) for the gate in plan §2/H.3 to work. Until then the analyzer version is visible only as top-level `analyzer_version` and `sync_contract.streams.profiles.analyzer_version`.
2. **New capability objects.** `capabilities.transport` (K1) and `capabilities.profile_stream` (K6) do not exist in 1.2.5. They are new objects, not new fields on existing ones. P1-4 added `capabilities.transport: {gzip: true}` in 1.3.0 (unreleased); `profile_stream` is still planned (P3-2).
3. **The v2 bootstrap has no 404 or 409.** The only statuses are 200/400/410/413/429/503 (§3.5). A 404 means the route is missing.
4. **The v2 session is absolute (60 minutes)**, hard-coded in SQL (`profile_bootstrap.py:245`); `SESSION_MINUTES` is unused. Release of an expired or stale session answers 410 and **leaves the row in place**, so it holds one of the 4 per-source slots until it expires (audit AUD-11). K5 alone does not fix this; P1-6 should. **Fixed in 1.3.0 (P1-6):** `SESSION_MINUTES` is the one lifetime constant for absolute and sliding (K3) sessions; release deletes expired and stale rows and answers 200; stale rows never count toward the slots (§3.5).
5. **`profile_bootstrap.available`** is `bool(DATABASE_URL)`, not a probe; `auth` is constant even when `AUTH_ENABLED=false` (K4). **Fixed in 1.3.0 (P1-6):** `available` is a cached probe of the migrated tables on the plugin's own connection, and `auth_enabled` reports the live setting; `auth` is unchanged (§2).
6. **Cursor ahead of head** on `/profiles/changes` is **400 `invalid_cursor`**, not 410. The collections feed returns an empty 200 in the same case. K8 makes collections answer 410; profiles are unchanged.
7. **The boundaries formula** in plan K7 (`source.sample_rate` + `source.decoded_frames`) and in C-12 (`origin_frame`/`covered_frames`) are equivalent. Both were verified against `edge_profiles.py:146-152` and the golden fixture. §4.4 is the exact statement. `rate` is the **source** rate, not the 48 kHz measurement rate.
8. **Payloads differ by path.**
   - Fixed for new events by P1-1: every path emits float4 `ref_lufs` and the same serializer (§3.4).
   - Events recorded by 1.2.5 and still in the journal carry float64 `ref_lufs` and went through the `catalog.canonical_json` sanitizer (NFC, trim, `""`→`null`).
   - Clients should keep comparing `ref_lufs` with a tolerance and treating `null` ramps like empty ramps, for 1.2.5 servers and old journal entries.
   - Rows published with a bare `media_fp` signature (no `catalog-media:` prefix) are not "current" for the analysis hook and compare as changed media, so after the upgrade each such row is re-analysed once and loses its edge once (the edge upgrade is then rescheduled for the prefixed signature).
9. **Profile journal retention** is a fixed 50,000 events per publication, but `max(1000, 2×count)` in maintenance compaction. At 94k profiles, publication-time compaction is the binding limit. P1-2 replaces both with one persisted limit of at least 50k and 2× the library (§3.4).
10. **`/api/profiles` silently truncates `ids` to 500.** K6 clients fetching misses must batch ≤500 ids and must not treat an unlisted id as "missing".
11. **Timestamps** mix zone-less (`analyzed_at`) and offset (`expires_at`, `created_at`) forms (§1.6). The audit's "`expires_at` is non-UTC on a non-UTC server" is confirmed. **1.3.0 (P1-6):** the profile and v2 `TIMESTAMPTZ` fields are always UTC with `Z`; `analyzed_at` stays zone-less by design, and collection timestamps are unchanged (§1.6).
12. **Collections today** (the baseline for K8/K9):
    - creating an existing id returns 201 with the existing (or `null`) collection and no event;
    - a duplicate membership silently remaps the item id;
    - `idempotency_key_conflict` has no `current`;
    - the feed has no epoch in the response (an epoch exists internally in `collection_feed_state`);
    - journal and receipts are never compacted.
13. **Shelves idempotency** is keyed by mutation `id` only, with no body fingerprint. This is not covered by K1–K11; it is recorded here for completeness.
