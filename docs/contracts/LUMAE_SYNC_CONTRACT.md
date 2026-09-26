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
| `AUTH_ENABLED=true`, any other host auth method (`g.auth_method` not `session` or `bearer`, for example a plugin-scoped token) | Authenticated by the host. **From 1.3.0 (P3-4b):** `abort(401)`: Flask HTML 401 | 1.2.5: `user:<username>` when the host set a user, otherwise `__global__` (fails **open**). 1.3.0: none (fails closed) |
| `AUTH_ENABLED=false` | **Anonymous.** Every transfer, including the v2 profile bootstrap, is open to anyone who can reach the host. | `__global__` |

Rules for clients:
- Health still reports `capabilities.profile_bootstrap.auth: "host_authenticated"` when `AUTH_ENABLED=false` (`__init__.py:1840`). That string describes the design, not the live setting, and it is unchanged in 1.3.0. **From 1.3.0 (K4, P1-6):** `capabilities.profile_bootstrap.auth_enabled` is the host's live `AUTH_ENABLED` (`false` when the host does not set it). With `auth_enabled: false`, every transfer is anonymous (last row above).
- The host redirects an unauthenticated request for a non-`/api/` host path to `/login` with **HTTP 302** instead of returning 401. This is host behaviour, not visible in this repo; it was *verified in the 2026-09-24 audit* (`docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md`, P3 list under AUD-12). Every plugin path starts with `/plugins/`, so plugin API calls get this redirect too. A client must treat any **3xx response, or any `text/html` body where JSON was expected, as `authentication_required`**. It must never parse the login page as data and must never follow the redirect as success.
- Profile data (waveform and edge) is shared by everyone who can reach the catalogue source; it is scoped by `catalog_instance_id`, not by user. Collections and shelves are scoped by principal. Shelves are also scoped by catalogue.
- The two fail-closed rows answer 401 on every route that resolves this principal: collections, shelves, personal discovery, and `GET /api/health` (it reports the collections `scope`).

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

**`Retry-After` (K4).** 1.2.5 never sends it. From 1.3.0 (unreleased, P1-6) every v2 bootstrap **429** carries `Retry-After: <seconds>` (a whole number, 1–300) and every v2 bootstrap **503** carries `Retry-After: 5` (§3.5). From 1.3.0 (unreleased, P3-4a) a collection mutation's **503 `collection_busy`** (§5.3) and every **503 from the collections snapshot** (§5.2a) also carry `Retry-After: 5`. Other routes and statuses do not send it. Clients wait at least that long before retrying, and use their own backoff when the header is absent (1.2.5).

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

This route always answers 200 (unless an exception occurs, or the host auth method names no collections principal: then 401, §1.2). It has no side effects. From 1.3.0 it also reads the `integrity` object below (two index lookups, well under 1 ms at 94k profiles and 200k collection events).

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
| `collections` | `schema_version: 1`, `backup_version: 1`, `enabled`, `scope`. **1.3.0 adds** `feed_epoch: true` (K8, P3-4a) and `contract: 2` (K9, P3-4b). | `collection_manager.py:18-20, 53-57`. `feed_epoch` gates the feed `epoch` echo and 410 (§5.2) and the snapshot route (§5.2a). `contract` is the collections contract a request can opt in to with the header `X-Lumae-Collections-Contract` (K9, §5.3); it also covers shelf mutations (§5.4). |
| `catalog_mirror` | `contract_revision`, `catalog_schema_version: 3`, `analysis_schema_version: 2`, `catalog_builder_version`, `supported_core_range`, `supported_provider_types: ["navidrome"]`, `features: [...]` | `catalog_capability()`, 1753-1762. `features` is the static `CATALOG_FEATURES` list (120-162), which includes `profile_cursor_stream` and `source_scoped_profiles`. |
| `credits` | `credits_service.capability()` | out of scope |
| `transport` | `gzip: true` | **New in 1.3.0 (unreleased, K1).** Informational: gzip is negotiated per request through `Accept-Encoding` (§1.5). Absent in 1.2.5. |
| `profile_stream` | `edge_refs: true` | **New in 1.3.0 (unreleased, K6, P3-2).** The server accepts the edge-reference opt-in on `/profiles/changes` and on v2 create (§3.7). Absent in 1.2.5. |

**Keys that do not exist in 1.2.5.** A client must treat each of these as absent/false: `integrity` (top level, added in 1.3.0, P1-3), `transport` (added in 1.3.0, K1), `profile_stream` (added in 1.3.0, K6), `profile_bootstrap.sliding_expiry`, `profile_bootstrap.idempotent_create`, `profile_bootstrap.auth_enabled` (these three added in 1.3.0, P1-6), `edge_profiles.compact_transport`, `collections.feed_epoch` (added in 1.3.0, P3-4a), `collections.contract` (added in 1.3.0, P3-4b), `collections.source_scoped_items`, and `lumae_analysis_profiles`.

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
- `missing`: everything else, including pending work.

Headers: from 1.3.0 the 200 response has private cache headers (§1.5); in 1.2.5 it has none. It is gzipped under K1 when large enough (§1.5).

This is the K6 miss fetch (§3.7, C-10). Each profile in `profiles` carries the **full current edge** of its published row, whether or not the client uses K6; the route has no opt-in. A client fetching misses sends at most 500 ids per request, comma-joined (`,` may be sent unescaped), and keeps the request line within the host's limit (below).

**Request line.** The stock AudioMuse host runs gunicorn with its default `--limit-request-line` of **4,094 bytes** (method, path, query and protocol; AudioMuse-AI 8aa1639c `deployment/supervisord.conf` does not change it); a longer line gets a host 400 before the plugin runs (measured: 350 ids of 10 characters, a 3,960-byte line, answer 200; 500 of them, 5,610 bytes, answer 400). With 22-character Navidrome ids that is about 150 ids per request, so the 500-id server cap does not bind: batch by both. A fixed count is not enough for arbitrary ids: budget the request line in bytes, as Auralscape's `batchIdsForUrl` (`src/services/syncReconcile.ts`, a 3,500-byte budget) already does. The hand-off's 100 ids fit only when ids are short ASCII, about 32 characters or fewer.

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
| `edge_refs` | **From 1.3.0 (K6, P3-2), optional.** `1` or `true` (case-insensitive) opts in to edge references (§3.7). Any other value, or none, is the default: every edge in full. Gate: `capabilities.profile_stream.edge_refs`. |

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

From K6 (P3-2, 1.3.0): the journal stores an upsert's waveform payload plus a reference to its edge, and this route joins the edge back when it serves the event (§3.7). Without `edge_refs` the response is byte-identical to the pre-K6 server's for the same history while the referenced edge is still published; §3.7 gives the one difference, for an edge replaced or removed later.

### 3.5 v2 profile bootstrap (`profile_bootstrap.py`; routes at `__init__.py:2600-2633`)

All four routes are `POST` with a JSON body of at most 16 KiB. Every body carries this **envelope**, which is validated by `_require_request` (104-123):

```json
{"protocol_version": 2, "schema_version": 1, "transfer_contract": "source_scoped_v1",
 "catalog_instance_id": "<1–512 chars>"}
```

The integers must be JSON integers; `true` or `2.0` is rejected.

| Route | Extra body fields | Success (200) body |
|---|---|---|
| `POST /api/profiles/bootstrap/sessions` (create; 220-289) | `page_size`: int **1–500**, default 250. It is fixed for the whole session. **From 1.3.0 (P1-6), both optional:** `expiry_mode`: `"absolute"` (default) or `"sliding"` (K3); `client_request_id`: a UUID string, at most 64 characters, compared case-insensitively (K5). Any other value of either field is 400. `null` means absent. **From 1.3.0 (K6, P3-2), optional:** `edge_refs`: a JSON boolean, default `false`; `true` opts the session's catch-up in to edge references (§3.7). Any other value is 400; `null` means absent. | envelope + `session_token` (64 hex), `page_size`, `snapshot_count`, `total_profiles`, `catalog_epoch`, `profile_epoch`, `snapshot_seq`, `expires_at`, `snapshot_cursor`, `cursor` (= `snapshot_cursor`), `next_page_token` (always present, for ordinal 0). **From 1.3.0 (K6):** also `edge_refs: true`, only when the create sent `edge_refs: true`. |
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
  - From K6 (P3-2): the journal itself already stores the waveform payload and the reference, and the capture copies both (events journalled before K6 are still split as above). Pages are unchanged for a session created without `edge_refs`. A session created with `edge_refs: true` gets a ref-eligible upsert as `edge_profile_ref` instead of `edge_profile` (§3.7); snapshot pages of that session still carry full edges.
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

### 3.7 Edge references (K6; 1.3.0, unreleased, P3-2)

A waveform-only republish (the analyzer changed the waveform fields, the media did not change) keeps the track's edge (P1-1), and its upsert must still carry that edge, because a client deletes its edge on an upsert without one (§6 rule 1). That is about 19 KB per event (LUM-005 would republish every profile). With K6 a client that opts in receives a reference to the edge instead and keeps its own copy.

**Gate and opt-in.** `capabilities.profile_stream.edge_refs: true` (§2). Opt in with `edge_refs=1` (or `true`) on `GET /api/profiles/changes` (§3.4), and with `edge_refs: true` in the v2 create body (§3.5), which the session keeps for its catch-up. `/profiles/changes` is the only legacy stream route that takes it. A server without K6 ignores both (§8 rule 3), so a client sends them only when the capability is advertised and must still accept full edges.

**What is ref-eligible.** Only an upsert journalled by a waveform-only republish whose edge was kept. The server records this with the event (the journal's `edge_ref` has `"kept": true`, `catalog_enrichment.journal_edge_ref`). Everything else is served as without the opt-in:
- an **edge publication** (`publish_edge_profile`) always carries the full edge, in both modes;
- an upsert without an edge (first publication, new media, rekey), and `delete` events, are unchanged;
- **snapshot pages** (v2 `…/sessions/page`) and the legacy `/profiles/bootstrap` always carry full edges: they are the device's baseline, and a device starting from nothing holds no edge a reference could match;
- `GET /api/profiles` (§3.2) always carries full edges;
- events journalled before the upgrade (by 1.2.5, or by 1.3.0 before K6) are served exactly as stored, with their embedded edge.

**Wire shape with the opt-in.** A ref-eligible upsert's `payload` has no `edge_profile` and one new key:

```json
{"seq": 812, "track_id": "tr-0000042", "operation": "upsert", "created_at": "2026-09-25T10:00:00.123456Z",
 "payload": {"track_id": "tr-0000042", "source": "waveform", "sample_rate": 44100, "duration_ms": 240000,
             "ref_lufs": -13.5, "start_ramp": "…", "end_ramp": "…", "analyzer_ver": 1,
             "analyzed_at": "2026-09-25T10:00:00.123456",
             "media_signature": "sha256:<64 hex>", "media_revision": "sha256:<64 hex>",
             "edge_profile_ref": {"media_revision": "sha256:<64 hex>", "profile_digest": "<64 hex>"}}}
```

- `edge_profile_ref.media_revision` always equals the payload's `media_revision`; `profile_digest` is the §4.2 digest of the edge.
- `edge_profile` and `edge_profile_ref` never appear together.
- The reference names the edge that was current when the event was recorded. It is served as recorded, also when that edge was replaced or removed since; a later event in the stream then carries the replacement (an edge publication) or the removal (a new-media upsert without an edge, or a `delete`).
- A ref-eligible event is about 0.8 KB of JSON instead of about 20 KB. Measured on the wire (gzip): 124 B per event in `/changes` pages of 100 (and in v2 catch-up pages); in the end-to-end gate's full waveform-only republish of 94,000 tracks, 150 B per track (14 MB) including headers and 50 miss fetches, against 8,329 B per track (783 MB) for a device without K6 (`scripts/e2e`, `lum005_k6`).

**Without the opt-in (every client that does not send it).** The server expands each reference to the full edge, so the response is **byte-identical** to the pre-K6 server's for the same history while the referenced edge is still published (pinned by golden responses generated from the pre-K6 code, `tests/plugins/fixtures/k6_old_client_golden.json`). Old clients see no change, with one difference for an edge that was replaced or removed **after** the event:
- `/profiles/changes`: the event carries the edge of that digest (for the event's track and `media_revision`); if that edge is gone, the edge **currently** published for the same `media_revision`; if there is none, no `edge_profile`. Before K6 the server replayed the historical edge instead. Either way the event that replaced or removed the edge follows in the stream (every path that replaces or removes an edge row journals an event for the track after the ones that referenced it), so a client that applies the stream in order ends with the same edge.
- v2 catch-up: unchanged from K2 (§3.5): the edge of that digest, or no `edge_profile`.

**Client rule (C-10).**
1. When `capabilities.profile_stream.edge_refs` is true, send `edge_refs=1` on every `/profiles/changes` request and `edge_refs: true` on v2 create. Otherwise keep today's rule (§6 rule 1).
2. For an upsert with `edge_profile_ref`, apply the waveform fields as usual. If the local edge of the track (published or opportunistic) has the same `media_revision` **and** `profile_digest` as the reference, **keep it**. Otherwise drop the local edge and queue the track for a miss fetch. A later event for the track replaces its queued fetch.
3. Fetch misses through `GET /api/profiles?catalog_instance_id=…&ids=…` (§3.2) in batches of **at most 500 ids** (the server ignores ids past the 500th and does not list them in `missing`) and within the host's request-line limit (§3.2). Verify each returned edge (§4.2 digest, §6 rule 5) and store it only when its `media_revision` equals the event's (the queued) `media_revision`. Its digest can differ from the reference's when the edge was replaced after the event; that edge is still valid for the revision, and the replacing event follows. A profile without an edge, or listed in `missing` or `failed`, leaves no edge. Keep the queue durable with the page's write (the cursor may advance past the event) and retry a failed fetch later; until it succeeds the track has no edge, as after any upsert without one.

**Server side (informational).** The journal row stores the waveform payload plus `edge_ref` (`{"profile_digest", "kept"?}`) instead of a copy of the edge; publication therefore no longer copies about 16 KB per event, and the journal no longer holds edges (an edge row is shared by every event that references it). Rows journalled before the upgrade keep their embedded edge (they are not rewritten, and every reader serves both forms). The migration adds `profile_changes.edge_ref` (nullable) and `profile_bootstrap_sessions.edge_refs` (`false` by default).

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
| `epoch` | **1.3.0 (K8), optional.** The `epoch` of an earlier feed or snapshot response, echoed. Absent or empty means "not echoed". 1.2.5 ignores it. |

Response 200: `{changes:[{seq, collection_id, entity_kind: "collection"|"item", entity_id, operation: "upsert"|"delete", payload, created_at}…], next_cursor}`, plus from 1.3.0 `epoch`, `head_seq`, `floor_seq` and `has_more` (K8, below).
- `seq` values come from one installation-wide counter (`collection_feed_state.head_seq`, 484-501). One principal's seqs therefore have **gaps**. They are strictly increasing.
- Only events with `seq ≤ head_seq` are returned.
- `next_cursor` = the last returned seq, or the request cursor if the page is empty.
- 1.2.5: there is **no `has_more`, no `epoch` and no `head_seq`** in the response. Page until `changes` is empty.
- 1.2.5: it **never returns 410**. A cursor past head returns an empty page and echoes the cursor, so a client cannot detect a server-side reset. From 1.3.0 this still holds for a request that does not echo `epoch`.
- 503 `collection_feed_unavailable`: the feed state row is missing or has an unknown protocol.
- The journal and idempotency receipts are never compacted.

**K8: epoch, head and paging (1.3.0, unreleased, P3-4a; gate `capabilities.collections.feed_epoch: true`).** Every 200 carries four more keys, whether or not the request echoes `epoch`. The 1.2.5 keys and their values are unchanged byte for byte (`tests/plugins/collection_feed_v1_golden.json` pins them).

| Key | Meaning |
|---|---|
| `epoch` | UUID string identifying this feed's lifetime: `collection_feed_state.epoch`. It changes only when the feed is re-seeded (a new database, or the state row recreated) or rotated by an operator (`docs/runbooks/UPGRADE_1.3.md`, repair C). Upgrading to 1.3.0 keeps the existing epoch. Compare it as a UUID. |
| `head_seq` | The installation-wide feed head this page was read at (an integer ≥ 0). Every event with `seq ≤ head_seq` is committed and visible. It is global, so it is usually above the principal's last seq. |
| `floor_seq` | The head at cutover: when this epoch's feed was seeded, or when 1.3.0 first migrated an older installation (and set to the head by a rotation). Nothing at or below it is guaranteed to stay in the journal. 1.3.0 never deletes events, so today the whole journal is present; a later release may compact below it (plan P3-4 item 6), and will say how that is signalled. |
| `has_more` | `true` when this principal has more events with `seq` above `next_cursor` and at or below `head_seq`. Page with `cursor=next_cursor` until it is `false`; do not infer the end from a short page. |

**410 `{"error":"collections_resync_required","reason":…}`** is returned **only when the request echoes `epoch`** (non-empty), and then when:
- `reason: "epoch_mismatch"`: the echoed value is not this feed's epoch (including a value that is not a UUID). Checked first.
- `reason: "cursor_ahead"`: `cursor > head_seq`. The server lost history the client has seen, for example after a database restore.

A request without `epoch` never gets 410: a cursor past head is still the 1.2.5 empty 200 that echoes the cursor (now with `has_more: false`). The plan says the cursor rule applies "or when the cursor is past head" without the echo; that would change an existing status for a client that has not opted in (§8 rule 1), so both 410 causes need the echo. `cursor == head_seq` is a normal empty page.

**Client rules (K8).**
1. Use K8 only when `capabilities.collections.feed_epoch` is `true`. Store the cursor and the epoch together.
2. Without a stored epoch, call without `epoch` (or fetch the snapshot) and store the returned `epoch`. Echo it on every later feed request.
3. On 410, discard the cursor, fetch the snapshot (§5.2a), merge it with unsent local mutations, then continue with `cursor = snapshot.head_seq` and `epoch = snapshot.epoch`.
4. A database restored from a backup keeps its epoch. A client whose cursor is past the restored head gets `cursor_ahead`; one whose cursor is not past it cannot tell, unless the operator rotates the epoch after the restore (runbook repair C).

Payloads:
- collection upsert: the collection object;
- collection delete: `{id, revision}`;
- item upsert: the normalised item plus `collection_revision` and `collection_updated_at`;
- item delete: `{id, collection_id, collection_revision, collection_updated_at}`.

### 5.2a `GET /api/collections/snapshot` (1.3.0, unreleased, K8, P3-4a)

The full path is `GET /plugins/lumae_analysis/api/collections/snapshot`. 1.2.5 has no such route (Flask 404). Auth, principal and the 404 `collection_manager_disabled` rule are the same as the feed's. No parameters.

Response 200:
```
{schema_version: 1, scope: "personal"|"shared",
 epoch, head_seq, floor_seq,
 collections: [<collection object>…], collection_count,
 items: [<item object>…], item_count}
```
- `collections`: the principal's **active** collections (tombstones are left out, as in `GET /api/collections`), each exactly the §5.1 object that `GET /api/collections/<id>` returns, ordered by `created_at`, then `id`.
- `items`: every item of those collections, exactly the §5.1 stored-row shape of `GET /api/collections/<id>` (with `collection_id`, `added_at`, `updated_at`), ordered by `collection_id`, `kind`, `position`, `added_at`, `id`.
- `epoch`, `head_seq`, `floor_seq`: as in the feed (§5.2).
- **Consistency.** The route reads on its own connection in one read-only REPEATABLE READ transaction, and the feed state is its first read. Every writer moves the head in the transaction that writes the rows, so the snapshot reflects **exactly** the events with `seq ≤ head_seq`: a write that commits during the snapshot is either entirely in it (and in `head_seq`) or entirely absent. Continue the feed with `cursor = head_seq`.
- **503 `{"error":"collection_feed_unavailable"}` with `Retry-After: 5`**: the feed state row is missing or has an unknown protocol, `DATABASE_URL` is unset, the database is unreachable or timed out (statement timeout 30 s, lock timeout 5 s), or the worker is busy with another snapshot (below). A connection failure is logged as a warning naming only the error class.
- **One at a time.** A web worker process builds one snapshot at a time (a process-wide slot): a request that cannot start within 2 s gets the 503 above, so concurrent requests cannot each hold a large snapshot in memory. Retry after `Retry-After`.

**Size.** One response, not paged. Measured on the test database with realistic item rows: 20,000 items take 0.38 s and 8.0 MB of JSON (0.7 MB with K1 gzip); 100,000 items (the backup limit) take 1.6 s and 40 MB (3.7 MB gzip), with about 200–255 MB of transient memory in the web worker while it is built (hence the one-at-a-time slot). At household sizes no page shape is needed, so none is defined. If one is needed later it would be additive, for example `?limit=&after=<opaque>` pages that each run their own REPEATABLE READ read and return `has_more`, with the client continuing the feed from the **first** page's `head_seq`: every event carries the entity's full state or a delete by id, so replaying events a later page already reflects is harmless.

### 5.3 Mutations

Every mutation runs through `_mutation_response` (546-608).

**Idempotency.**
- Optional header `Idempotency-Key`, trimmed and truncated to 200 characters. It is scoped per principal and serialised with a transaction advisory lock.
- The request fingerprint is SHA-256 over the method, the path, the canonical JSON body and the `If-Match` header (present or absent, and its value) (519-530).
- A replay with the same fingerprint returns the stored status and body, with the header `Idempotency-Replayed: true`.
- A different fingerprint under the same key returns **409 `{"error":"idempotency_key_conflict"}`**. With contract 2 (K9, below) the body adds `current`.
- Only **2xx** results are stored. A 4xx is not recorded, so retrying the same key runs the request again.
- A receipt from before fingerprinting existed replays with `Idempotency-Fingerprint: legacy-unbound`.

**Optimistic concurrency.**
- The expected revision comes from `If-Match` or body `base_revision` (505-512). If it is absent, empty or `*`, it is not checked. A value that is not an integer never matches.
- A mismatch returns **409 `{"error":"revision_conflict","current":<collection>}`**.

**Other errors:**
- 503 `unsupported_transaction_isolation` (the host connection is not READ COMMITTED);
- 503 `collection_feed_unavailable`;
- **503 `collection_feed_invariant`** (new in 1.3.0, P1-3): a committed change row sits past the feed head (`MAX(seq) > head_seq`, for example left by a 1.2.5 worker before the upgrade fence). The check runs inside each mutation against the head it has just locked; the mutation rolls back and nothing is written. Every collection write, for every principal, returns this until an operator runs the repair in `docs/runbooks/UPGRADE_1.3.md`; health reports it as `integrity.collections_feed_ok: false`. There is no `Retry-After`. Clients treat it like `collection_feed_unavailable`: keep the mutation queued and retry later. 1.2.5 instead failed these writes with a 500 (a unique violation) indefinitely (AUD-05);
- 409 `item_id_collection_conflict` (the item id already belongs to another collection of this principal);
- **503 `{"error":"collection_busy"}` with `Retry-After: 5`** (new in 1.3.0, P3-4a): the mutation waited more than **3 s** for one lock (the idempotency key's advisory lock, the collection row, or the feed head). Each mutation transaction runs with `SET LOCAL lock_timeout = '3s'`, so the host connection's own setting is untouched. The transaction rolls back: nothing from it is written and no receipt is stored, so retrying with the same `Idempotency-Key` runs the mutation again. In a chunked restore (below) the chunks committed before it stay, and that retry resumes after them. 1.2.5 waited without a bound (the host's timeout, if any, gave a 500). Keep the mutation queued and retry after `Retry-After`.

**Feed writes (1.3.0, P3-4a).** A mutation stages all its change events, then reserves them as one block, `UPDATE collection_feed_state SET head_seq = head_seq + n RETURNING head_seq` (seqs `head−n+1 … head`), and writes them with one multi-row INSERT in feed order. This replaces one head update and one insert per event. The seqs a client sees are unchanged: strictly increasing, gapless at the head, global. The P1-3 fence (no `seq` default) and the invariant check are kept: the INSERT writes nothing if any row already sits at or above the block's first seq, and the mutation answers `collection_feed_invariant`. The feed head is locked only from the block's reservation to commit, and only the event INSERT and (in a restore's last chunk, or any mutation with a key) the receipt INSERT run in that window: nothing per collection or per item. On the test database that is about 17 ms for 500 events, and at most 67 ms per chunk for a 20,000-item restore into one collection, 89 ms for 2,000 collections of 10 items and 180 ms for 2,000 collections of 50 items (the last chunk 4, 73 and 63 ms). Another principal's write during those restores waited at most 76, 98 and 201 ms.

| Route | Body | Success | Notes |
|---|---|---|---|
| `POST /api/collections` (783-810) | `{id?, name (1–120), description? (≤1000)}` | 201 `{collection}` | **An existing id is not an error:** `ON CONFLICT DO NOTHING`, 201 with the existing collection (or `collection: null` if it is tombstoned), and no change event. With contract 2 (K9, below): 409 `collection_exists` or `collection_deleted`. |
| `PATCH /api/collections/<id>` (840-871) | `{name?, description?, base_revision?}` | 200 `{collection}` | 404 `collection_not_found`; revision +1 |
| `DELETE /api/collections/<id>` (874-901) | `{base_revision?}` | 200 `{deleted:true, id, revision}` | Soft delete (tombstone). A **missing** collection always returns 200 `{deleted:true}` (880-882). An **already-deleted** collection returns 200 `{deleted:true}` only if no revision is sent or it matches. The revision check comes **before** the tombstone check, so a mismatched `If-Match`/`base_revision` returns **409 `revision_conflict` with `current`** (the tombstoned collection) (883-887). |
| `PUT /api/collections/<id>/items/<item_id>` (904-908) | item fields | 200 `{collection, items:[item]}` | same as batch with one item. See the returned-item shape below. |
| `POST /api/collections/<id>/items/batch` (911-917) | `{items:[≤500], base_revision?}` | 200 `{collection, items}` | More than 500 items or not a list: 400. Revision +1 per request, **even when `items` is empty**: an empty batch still bumps the revision and returns 200 (915, 928-935). |
| `DELETE /api/collections/<id>/items/<item_id>` (964-1005) | `{base_revision?}` | 200 `{deleted: bool, collection}` | A missing item gives `deleted:false` and no revision bump. |
| `DELETE /api/collections/<id>/items/batch` (1008-1059) | `{item_ids:[1–500], base_revision?}` | 200 `{deleted:[ids], deleted_count, collection}` | |
| `POST /api/collections/restore` (767-780) | backup document | 201 `{restored:true, collections, collection_count, item_count}` | additive restore; chunked from 1.3.0 (below) |

**Restore (1.3.0, unreleased, P3-4a).** The response body and status are unchanged. A restore commits in transactions of at most **2,000 rows**, where a row is one collection it creates or one item (`RESTORE_CHUNK_ROWS`). A backup that fits in one chunk behaves exactly as in 1.2.5: one transaction, each collection at revision 2 (1 if it has no items). A larger backup is split in backup order; a collection's items may span chunks.
- **Events.** The chunk that creates a collection emits its `collection` upsert, and then the items of that chunk. A later chunk that adds items to the same collection bumps its revision once and emits only `item` upserts, each carrying the new `collection_revision`, like a batch upsert. The events of the whole restore are in the same order as in 1.2.5. The final revision of a collection is 1 plus the number of chunks that wrote its items (a 5,000-item collection ends at revision 4, not 2); clients must use the returned revision, not assume 2.
- **Between chunks**, other requests (feed, snapshot, list and detail) see the restored collections grow chunk by chunk. Each committed chunk is a consistent prefix of the restore. A collection deleted by a client before the restore finished stays deleted, and its remaining items are skipped.
- **The response** lists every restored collection as it stands when the last chunk commits (a collection deleted meanwhile is listed as its tombstone); `collection_count` and `item_count` count the backup. It is stored as the receipt in the last chunk's transaction.
- **Interruption and retry.** With an `Idempotency-Key`, each chunk's transaction also records progress (`collection_restores`: the restore's id, chunk size and chunks done) under the key's advisory lock. Collection and item ids derive from that restore id, so a retry with the same key and body resumes after the last committed chunk and ends in the same final state and the same events as an uninterrupted restore, without duplicates. Two requests with the same key never apply a chunk twice; both return the same 201 (one of them with `Idempotency-Replayed: true`). While a keyed restore is unfinished, any request that reuses its key with another fingerprint (another restore body, or any other mutation route) is 409 `idempotency_key_conflict` and changes nothing; only a retry of the same restore resumes it. After it finishes, the stored receipt decides as for any other mutation. The progress row is deleted with the chunk that stores the receipt; one left by a client that never retries stays until a later clean-up (plan P3-4 item 6). **Without a key**, a restore that fails after its first chunk keeps the chunks already committed, and a retry restores another full copy. Always send a key. The AudioMuse web manager keeps one key per backup checksum for the page's lifetime and forgets it when that restore succeeds (1.3.0, P3-4b). Choosing *Restore copies* again after a failure resumes the restore, also after closing the dialog and choosing the same backup again; after a page reload, restoring it again adds another copy, and the failure message says so.

Read routes:
- `GET /api/collections` (732-753) returns `{schema_version, scope, collections}`;
- `GET /api/collections/<id>` (813-824) returns `{collection, items}`, or 404;
- `GET /api/collections/snapshot` (1.3.0, K8) returns every active collection and item with the feed head (§5.2a);
- backup, export and search are also available.

**Item normalisation and membership (`_normalize_item` 637-659, `_upsert_item` 666-726).**
- `kind` must be `album` or `track`. A track needs `track_id`. An album needs `provider_album_id` or `album_key`.
- `id` defaults to a new UUID. `position` is clamped to ≥ 0.
- **Returned items (PUT and batch) are the `_normalize_item` shape, not the stored row** (648-659, returned from the handler after 945): `{id, kind, track_id, provider_album_id, album_key, title, artist, album, cover_item_id, position}`.
  - There is **no `collection_id`, `added_at` or `updated_at`**.
  - `artist` is `""` (never `null`) when absent.
  - `position` is the clamped request value, not a re-read.
  - Item upsert change events carry this shape plus `collection_revision` and `collection_updated_at`.
- **Duplicate membership is remapped silently.** If the collection already holds the same track (or album key), the request's item `id` is **replaced by the existing item's id**. The response and the change event carry the existing id, and no error is returned. With contract 2 (K9, below) this is 409 `membership_conflict` instead; restores keep the remap in both modes.

**K9: contract 2 (1.3.0, unreleased, P3-4b; gate `capabilities.collections.contract: 2`).** A mutation request opts in with the header **`X-Lumae-Collections-Contract: 2`**: the value `2`, surrounding spaces ignored. Any other value, or no header, gets the 1.2.5 behaviour, and every collection and shelf response is then 1.2.5's byte for byte (`tests/plugins/collection_conflicts_v1_golden.json`, recorded before K9, pins them). The header is not part of the request fingerprint, so a keyed retry replays its stored receipt with or without it. A 409 is never stored, so the same key can be retried after the client has adapted.

| Case | Without the header (1.2.5) | Contract 2 |
|---|---|---|
| An `Idempotency-Key` reused with another request (fingerprint mismatch, or a key held by an unfinished restore) | 409 `{"error":"idempotency_key_conflict"}` | 409 `{"error":"idempotency_key_conflict", "current": <collection> \| null}` |
| `PUT …/items/<item_id>` or `POST …/items/batch`: another item of the collection already holds the membership | the request item takes that item's id; 200 | 409 `{"error":"membership_conflict", "item_id", "existing_item_id", "conflicts": [{"item_id", "existing_item_id"}…], "current": <collection>}` |
| `POST /api/collections` with the id of an existing collection | 201 with that collection, no event | 409 `{"error":"collection_exists", "current": <collection>}` |
| `POST /api/collections` with the id of a deleted collection | 201 `{"collection": null}`, no event | 409 `{"error":"collection_deleted", "current": <tombstone>}` |

- **`current` on `idempotency_key_conflict`** is the collection the key's stored request applied to, as it stands now: the path collection of PATCH, DELETE and the item routes, or the created id of a create (the generated one when the body had no `id`). It is the §5.1 collection object that `revision_conflict` returns, and a deleted collection is its tombstone (`deleted_at` set). It is `null` when the key belongs to a restore (finished, or unfinished and holding the key), when the receipt was stored before 1.3.0 (receipts now record `collection_mutations.collection_id`), or when no such collection exists (for example a DELETE of a missing id). It names the key's collection, which can differ from the one this request names, so match it by `current.id`.
- **Membership** means, within one collection: the same `track_id` for a track; the same `provider_album_id` for an album that has one; the same `album_key` (exact, case-sensitive) for an album without one. Writing an item under the id that already holds the membership is an ordinary update. Items are checked in request order. `conflicts` lists every conflicting request item, and `item_id` and `existing_item_id` repeat its first entry. `existing_item_id` is the stored item that holds the membership, or an earlier item of the same request with the same membership. `current` is the collection before the request. **Nothing is written**: no item, revision bump, event or receipt. An item id that belongs to another collection of the principal is still 409 `item_id_collection_conflict` (without `current`). A request that has both answers `item_id_collection_conflict`: an item whose membership conflicts is not written, so only the other items can raise it.
- **Create conflicts** write no event and no receipt. A create retried without an `Idempotency-Key` after a lost response gets `collection_exists` with its own collection in `current`.
- **Unchanged by the header:** `revision_conflict`, `item_id_collection_conflict`, `collection_not_found`, every 400 and 503, the two item delete routes (they have no membership) apart from `current` on key conflicts, and **restores**. A restore creates new collections with new ids and rejects a backup collection that repeats a membership (400), so its own rows can raise neither `membership_conflict` nor `collection_exists`. Between the chunks of a chunked restore, a restore item whose membership a client added meanwhile takes that item's id, as in 1.2.5, with or without the header. A restore's key conflicts carry `current: null` under the header.
- **Shelves** read the same header (§5.4).

**Client rules (K9).**
1. Send the header only when `capabilities.collections.contract` is `2`.
2. `membership_conflict`: re-point each local item in `conflicts` (and anything that references it) to its `existing_item_id`, then retry.
3. `idempotency_key_conflict`: another request already used the key and succeeded (or is an unfinished restore). Stop retrying under that key. Take `current`, matched by `id`, as the server's state of that collection; when it is `null`, refetch (the feed, or the snapshot §5.2a).
4. `collection_exists` / `collection_deleted`: if `current` is the client's own earlier create, adopt it; otherwise the id is taken.

### 5.4 Shelves (`shelves.py`)

- **`GET /api/shelves/changes`**, and the alias **`GET /api/shelves/snapshot`**, which behaves identically (198-219).
  - Parameters: `catalog_id` (required, 1–512 chars), `cursor` (int ≥ 0), `limit` (1–500, default 250).
  - Response: `{records:[{type: "member"|"order"|"evidence", id, value}…], cursor, hasMore}`.
  - The feed is compact: each record keeps only its latest value and seq, and tombstones are kept as `deletedAt`.
  - 400 for bad parameters.
  - **Ordering (1.3.0, P3-4b).** Every write allocates its record's seq while it holds its (principal, catalogue) scope row lock, and keeps the lock until commit, so within a scope seqs commit in order and a reader paging by `cursor` never skips a record that commits late (LUM-004). 1.2.5 broke this for the provider-identity rekey, which allocated seqs without the lock. The rekey now locks, in principal order, only the scopes it rewrites.
- **`POST /api/shelves/mutations?catalog_id=…`** (221-247). Body: `{id, operation: "add"|"remove"|"restore"|"order"|"evidence", …}`, validated by `validate_mutation` (47-102).
  - 200 `{records:[…]}`.
  - 409 `order_conflict` (with `order`) or `unknown_membership_period`.
  - 400 for validation errors.
  - **Idempotency:** the receipt is keyed by the body `id`, scoped to (principal, catalogue). In 1.2.5 a replay returns the stored response even if the rest of the body is different; there is no fingerprint check.
  - **Receipt fingerprints (1.3.0, P3-4b).** Each new receipt also stores `request_fingerprint`, the SHA-256 of the canonical body (keys sorted, no whitespace, UTF-8). A replay with the same body returns the stored response in both modes. Another body under the same `id` returns the stored response (200, as in 1.2.5) unless the request sends **`X-Lumae-Collections-Contract: 2`** (K9, §5.3): then it is **409 `{"error":"idempotency_key_conflict"}`**, without `current`, and nothing changes. Re-read the changes feed and send the new body under a new `id`. A receipt stored before 1.3.0 has no fingerprint and replays in both modes.

---

## 6. Client rules already relied on

These rules hold against 1.2.5 and **must keep holding** for clients that do not opt in to anything.

1. **An upsert without a valid `edge_profile` deletes the local edge.** A profile upsert (bootstrap row, v2 snapshot row, `/changes` or catch-up `upsert` event, or direct fetch) that has no `edge_profile`, or has one that fails validation or digest verification, **removes** the client's stored edge for that track (Auralscape `publishedProfileRepo.ts:266, 351-388`; *verified in the 2026-09-24 audit*, `docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md`, not checkable from this repo). The server relies on this in three places:
   - **Waveform republish** (P1-1). The published 8-tuple (`sample_rate, duration_ms, ref_lufs, start_ramp, end_ramp, analyzer_ver, profile_schema_ver, media_signature`) is compared at stored precision (`ref_lufs` as float4). An identical completion is a no-op: no head change, no event, edge rows untouched. If the tuple differs and `media_signature` changed (or there was no published row), the server deletes the edge and its job and emits an upsert **without** an edge; the edge upgrade is then scheduled for the new media. If only the waveform changed on the same media, the edge and its job are kept and the upsert **embeds the current edge** (`serialize_profile(..., edge_profile=<edge_join>)`), so clients keep it. The analysis hook also skips admission when the published row is already current (same media fingerprint, analyzer and schema version, not failed).
   - **Rekey.** A provider-identity rekey emits `delete(old)` + `upsert(new)` without an edge (`profile_publication.py:462-503`).
   - **Edge publication** emits a new `upsert` carrying the waveform fields **and** the edge (`edge_profile_store.py:155`).

   K6 (1.3.0, §3.7) is the only exception, and only for clients that opt in: a waveform-only republish then carries `edge_profile_ref` instead of the edge, and the client keeps its matching edge (C-10). Since K6 the journal stores these events with a reference to the edge and the server embeds it again for every other client.
2. **`delete` removes the profile and its edge** (`profile_publication.py:64-77`).
3. **Apply events in `seq` order and advance the cursor atomically with the local write.** An event payload is frozen when it is recorded. A later event for the same track supersedes it.
4. **Handoff between v2 and legacy.** After the v2 catch-up finishes, continue with `GET /api/profiles/changes?cursor=<final catch-up cursor>`.
5. **An edge is valid only for the waveform row's `media_revision` and `track_id`.** Verify `profile_digest` (§4.2) before storing an edge.
6. **Resync triggers.** For profiles, a 410 `bootstrap_required` from `/changes` or legacy bootstrap means bootstrap again. A 410 from v2 means the session is gone: create a new one or fall back to legacy. Treat a 400 "cursor ahead of head" on `/changes` as a resync too (the server lost state).
7. **Authentication** is handled as in §1.2: a 3xx or HTML response means `authentication_required`.
8. **Collections: after a successful write, adopt the item ids the server returns**, because it may have remapped them (§5.3). With contract 2 (K9) the server never remaps a request item; it answers 409 `membership_conflict` instead.

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
| K6 | **Edge references in events and pages:** with `edge_refs=1` (query) or `edge_refs:true` (v2 body), an upsert whose edge is unchanged carries `edge_profile_ref:{media_revision, profile_digest}` instead of the full edge. Without the opt-in, the server expands to the full edge exactly as today. | P3-2 | Keep the local edge when digest and revision match; fetch misses through `GET /api/profiles?ids=` (C-10) | `capabilities.profile_stream.edge_refs:true` (new `profile_stream` object) | 3; must ship before P3-1 regeneration | shipped in 1.3.0 (unreleased): `edge_refs=1` on `/profiles/changes` and `edge_refs: true` on v2 create (echoed); only a waveform-only republish that kept its edge is served as `edge_profile_ref`; edge publications, snapshot pages, the legacy bootstrap and `/api/profiles` always carry full edges; without the opt-in responses are byte-identical to pre-K6, except that a `/changes` event whose edge was replaced or removed later carries the edge current for its revision, or none (§3.7) |
| K7 | Compact edge transport (optional): with `edge_compact=1` the server omits the derivable `boundaries`, and the client rebuilds them (§4.4) **before** verifying the unchanged v2 digest | P3-3 | Rebuild, then verify (C-12) | `capabilities.edge_profiles.compact_transport:true` | 3, optional | planned |
| K8 | **Collections feed:** the response adds `epoch`, `head_seq` and `has_more`. 410 `collections_resync_required` is returned **only** when the request echoes `epoch` and it mismatches, or when the cursor is past head. A new snapshot endpoint, **planned path `GET /plugins/lumae_analysis/api/collections/snapshot`** (P3-4 implements exactly this path), returns all of the principal's collections, items and head in one REPEATABLE READ transaction. | P3-4 | Echo the epoch; resync on 410; page by `has_more`/`next_cursor` (C-13) | `capabilities.collections.feed_epoch:true` | 3 | shipped in 1.3.0 (unreleased, P3-4a): every feed 200 adds `epoch`, `head_seq`, `floor_seq` (the head at cutover) and `has_more`; 410 `collections_resync_required` with `reason` `epoch_mismatch` or `cursor_ahead`, **both only when the request echoes `epoch`** (without it a cursor past head stays the empty 200); `GET /api/collections/snapshot` on an owned read-only REPEATABLE READ connection, one at a time per worker; collection mutations bound lock waits to 3 s (503 `collection_busy`, `Retry-After: 5`) and write events as one seq block; restores commit in resumable chunks of at most 2,000 rows (§5.2, §5.2a, §5.3) |
| K9 | **Collections conflicts:** with header `X-Lumae-Collections-Contract: 2`, `idempotency_key_conflict` includes `current`, and a duplicate membership returns 409 `membership_conflict {existing_item_id}` instead of a silent id remap. Create with an existing id returns 409. | P3-4 | Handle both; freeze reorder bodies at enqueue (C-13) | `capabilities.collections.contract:2` | 3 | shipped in 1.3.0 (unreleased, P3-4b): opt-in header `X-Lumae-Collections-Contract: 2`; `idempotency_key_conflict` adds `current` (the collection the key's receipt applied to, as it is now, or `null` for a restore, a pre-1.3.0 receipt or a missing collection); a duplicate membership on item PUT or batch upsert is 409 `membership_conflict` `{item_id, existing_item_id, conflicts, current}` and writes nothing; a create with an existing or deleted id is 409 `collection_exists` / `collection_deleted` with `current`; restores are unchanged; with the header, a shelf mutation `id` reused with another body is 409 `idempotency_key_conflict` (§5.3, §5.4) |
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
4. **Default output is frozen.** Without an opt-in, a newer server emits byte-for-byte the same shape as today, apart from additive keys. This includes full `edge_profile` expansion, `boundaries`, and the collection feed shape. The one content difference is K2/K6's: an event or snapshot row whose edge was replaced or removed after it was recorded carries the current edge of its revision (`/profiles/changes`) or none (v2), and the replacing event follows (§3.5, §3.7).
5. **New keys and values.** Clients ignore unknown JSON keys, unknown capability keys and unknown `features` entries. Enum-like values (`operation`, `entity_kind`, error codes) may gain members only behind a gate.
6. **Versions.** `schema_version` or `protocol_version` changes only for a breaking change, and a breaking change needs a new route or a new gated protocol, never an in-place change. `analyzer_ver` can take new values only behind K11.
7. **Every contract WP** updates this file (§2–§7 and the K status) in the same PR, with tests that pin the old default behaviour.

---

## 9. Code-versus-plan notes

The code wins. Each item names the WP expected to act on it.

1. **K11 gate location.** `lumae_analysis_profiles` exists only in `plugin.json` (manifest capabilities). **Health has no such key.** P3-1 must add a new `capabilities.lumae_analysis_profiles` object to `/api/health` (additive) for the gate in plan §2/H.3 to work. Until then the analyzer version is visible only as top-level `analyzer_version` and `sync_contract.streams.profiles.analyzer_version`.
2. **New capability objects.** `capabilities.transport` (K1) and `capabilities.profile_stream` (K6) do not exist in 1.2.5. They are new objects, not new fields on existing ones. P1-4 added `capabilities.transport: {gzip: true}` in 1.3.0 (unreleased); P3-2 added `capabilities.profile_stream: {edge_refs: true}` in 1.3.0 (unreleased).
3. **The v2 bootstrap has no 404 or 409.** The only statuses are 200/400/410/413/429/503 (§3.5). A 404 means the route is missing.
4. **The v2 session is absolute (60 minutes)**, hard-coded in SQL (`profile_bootstrap.py:245`); `SESSION_MINUTES` is unused. Release of an expired or stale session answers 410 and **leaves the row in place**, so it holds one of the 4 per-source slots until it expires (audit AUD-11). K5 alone does not fix this; P1-6 should. **Fixed in 1.3.0 (P1-6):** `SESSION_MINUTES` is the one lifetime constant for absolute and sliding (K3) sessions; release deletes expired and stale rows and answers 200; stale rows never count toward the slots (§3.5).
5. **`profile_bootstrap.available`** is `bool(DATABASE_URL)`, not a probe; `auth` is constant even when `AUTH_ENABLED=false` (K4). **Fixed in 1.3.0 (P1-6):** `available` is a cached probe of the migrated tables on the plugin's own connection, and `auth_enabled` reports the live setting; `auth` is unchanged (§2).
6. **Cursor ahead of head** on `/profiles/changes` is **400 `invalid_cursor`**, not 410. The collections feed returns an empty 200 in the same case. K8 makes collections answer 410 (`cursor_ahead`), but **only when the request echoes `epoch`** (1.3.0, P3-4a): the plan says "or when the cursor is past head" without the echo, which would change the status an unchanged client gets (§8 rule 1). Profiles are unchanged.
7. **The boundaries formula** in plan K7 (`source.sample_rate` + `source.decoded_frames`) and in C-12 (`origin_frame`/`covered_frames`) are equivalent. Both were verified against `edge_profiles.py:146-152` and the golden fixture. §4.4 is the exact statement. `rate` is the **source** rate, not the 48 kHz measurement rate.
8. **Payloads differ by path.**
   - Fixed for new events by P1-1: every path emits float4 `ref_lufs` and the same serializer (§3.4).
   - Events recorded by 1.2.5 and still in the journal carry float64 `ref_lufs` and went through the `catalog.canonical_json` sanitizer (NFC, trim, `""`→`null`).
   - Clients should keep comparing `ref_lufs` with a tolerance and treating `null` ramps like empty ramps, for 1.2.5 servers and old journal entries.
   - Rows published with a bare `media_fp` signature (no `catalog-media:` prefix) are not "current" for the analysis hook and compare as changed media, so after the upgrade each such row is re-analysed once and loses its edge once (the edge upgrade is then rescheduled for the prefixed signature).
9. **Profile journal retention** is a fixed 50,000 events per publication, but `max(1000, 2×count)` in maintenance compaction. At 94k profiles, publication-time compaction is the binding limit. P1-2 replaces both with one persisted limit of at least 50k and 2× the library (§3.4).
10. **`/api/profiles` silently truncates `ids` to 500.** K6 clients fetching misses must batch ≤500 ids and must not treat an unlisted id as "missing". The stock host's request-line limit (4,094 bytes, gunicorn's default) binds first with real ids: about 150 ids of 22 characters per request (§3.2, §3.7).
11. **Timestamps** mix zone-less (`analyzed_at`) and offset (`expires_at`, `created_at`) forms (§1.6). The audit's "`expires_at` is non-UTC on a non-UTC server" is confirmed. **1.3.0 (P1-6):** the profile and v2 `TIMESTAMPTZ` fields are always UTC with `Z`; `analyzed_at` stays zone-less by design, and collection timestamps are unchanged (§1.6).
12. **Collections today** (the baseline for K8/K9):
    - creating an existing id returns 201 with the existing (or `null`) collection and no event;
    - a duplicate membership silently remaps the item id;
    - `idempotency_key_conflict` has no `current`;
    - 1.3.0 K9 (P3-4b) changes the three items above, but only for requests that send `X-Lumae-Collections-Contract: 2` (§5.3);
    - the feed has no epoch in the response (an epoch exists internally in `collection_feed_state`; 1.3.0 K8 returns it, §5.2);
    - journal and receipts are never compacted.
13. **Shelves idempotency** is keyed by mutation `id` only, with no body fingerprint. This is not covered by K1–K11; it is recorded here for completeness. **1.3.0 (P3-4b):** receipts bind the body fingerprint, and the plan puts the check behind the K9 header: with `X-Lumae-Collections-Contract: 2` another body under the same `id` is 409 `idempotency_key_conflict`; without it the 1.2.5 replay stays (§5.4).
