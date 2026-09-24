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
- Health still reports `capabilities.profile_bootstrap.auth: "host_authenticated"` when `AUTH_ENABLED=false` (`__init__.py:1840`). That string describes the design, not the live setting. K4 adds a truthful `auth_enabled`.
- The host redirects an unauthenticated request for a non-`/api/` host path to `/login` with **HTTP 302** instead of returning 401. This is host behaviour, not visible in this repo; it was *verified in the 2026-09-24 audit* (`docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md`, P3 list under AUD-12). Every plugin path starts with `/plugins/`, so plugin API calls get this redirect too. A client must treat any **3xx response, or any `text/html` body where JSON was expected, as `authentication_required`**. It must never parse the login page as data and must never follow the redirect as success.
- Profile data (waveform and edge) is shared by everyone who can reach the catalogue source; it is scoped by `catalog_instance_id`, not by user. Collections and shelves are scoped by principal. Shelves are also scoped by catalogue.

### 1.3 Requests

- JSON request bodies need `Content-Type: application/json`. Every handler uses `request.get_json(silent=True)`, so without that header the body is read as empty. The v2 bootstrap then answers 400, and collection creation answers 400 "Collection name is required."
- Body size limits: v2 bootstrap 16,384 bytes (`__init__.py:2604`); edge analyze and backfill 64,000 bytes (`__init__.py:2800, 2813`). These limits are checked against `Content-Length` only (`__init__.py:2054-2057`).
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
| profile `analyzed_at` | `TIMESTAMP` without zone (`__init__.py:1222`) | **no zone designator**, for example `2026-09-24T10:11:12.123456`; wall-clock time in the database session zone (`catalog_enrichment.py:96-99`) |
| profile change `created_at`, v2 `expires_at` | `TIMESTAMPTZ` | `…Z` when the server runs in UTC, otherwise with a numeric offset such as `+02:00` (`profile_bootstrap.py:209-210`) |
| collection `created_at`/`updated_at`/`deleted_at`/`added_at`, change `created_at` | `TIMESTAMPTZ` | same as the row above (`collection_manager.py:234-237`) |
| shelf `addedAt`/`at` | client-supplied number | stored and echoed as sent (`shelves.py:41-44`) |

Never compare server timestamps with the device clock. `expires_at` is advisory; the server decides expiry with its own `now()` (`profile_bootstrap.py:195-197`).

---

## 2. Health and capabilities

### `GET /api/health` (`__init__.py:1823-1864`)

This route always answers 200 (unless an exception occurs). It has no side effects.

| Key | Value in 1.2.5 | Source |
|---|---|---|
| `plugin` | `"lumae_analysis"` | 1828 |
| `plugin_version` | `"1.2.5"` | 1829 |
| `core_version`, `core_adapter`, `supported_core_range` | host detection; the range is `">=2.6.0,<4.0.0"` | 1830-1832 |
| `sync_contract` | `{revision, producer, core_api_contract, streams:{catalog, analysis, profiles:{schema_version:1, analyzer_version:1, semantic_contracts:["lumae_playback_profile_v1"]}, credits, relationships}}` | `sync_contract()`, 1765-1801 |
| `schema_version` | `1` (profile schema) | 1834 |
| `analyzer_version` | `1` (waveform analyzer) | 1835 |
| `status` | `"ok"` when the core is supported, otherwise the core compatibility status | 1862 |
| `capabilities` | the table below | 1836-1861 |

`capabilities`: every key that exists today, with exact names.

| Key | Fields (1.2.5) | Notes |
|---|---|---|
| `profile_bootstrap` | `protocol_version: 2`, `schema_version: 1`, `auth: "host_authenticated"`, `transfer_contract: "source_scoped_v1"`, `available: bool` | `available` is only `bool(config.DATABASE_URL)` (1842). It is **not** a probe. |
| `edge_profiles` | `schema_version: 2`, `method: "lumae-edge-kweighted-bands-48k-k4-v2"`, `available: bool`, `enabled: bool` | `available`: the PyAV 16.1.0 / libswresample 6.1.100 runtime imports (`edge_profiles.py:86-98`). `enabled`: the setting `edge_profiles_enabled` and `available` (`__init__.py:2762-2763`). |
| `personal_discovery` | `schema_version: 1`, `enabled`, `scope: "shared"\|"personal"`, `features: ["album_memory_context","enjoyment_feedback"]` | out of scope here; see `docs/discovery-api-v1.md` |
| `music_metadata` | `schema_version: 1`, `enabled`, `provider: "musicbrainz"`, `daily_request_limit: 80`, `recording_membership: true` | out of scope |
| `shelves` | `schema_version: 1`, `enabled`, `scope` | `enabled` is the collection-manager setting |
| `collections` | `schema_version: 1`, `backup_version: 1`, `enabled`, `scope` | `collection_manager.py:18-20, 53-57` |
| `catalog_mirror` | `contract_revision`, `catalog_schema_version: 3`, `analysis_schema_version: 2`, `catalog_builder_version`, `supported_core_range`, `supported_provider_types: ["navidrome"]`, `features: [...]` | `catalog_capability()`, 1753-1762. `features` is the static `CATALOG_FEATURES` list (120-162), which includes `profile_cursor_stream` and `source_scoped_profiles`. |
| `credits` | `credits_service.capability()` | out of scope |
| `transport` | `gzip: true` | **New in 1.3.0 (unreleased, K1).** Informational: gzip is negotiated per request through `Accept-Encoding` (§1.5). Absent in 1.2.5. |

**Keys that do not exist in 1.2.5.** A client must treat each of these as absent/false: `transport` (added in 1.3.0, K1), `profile_stream`, `profile_bootstrap.sliding_expiry`, `profile_bootstrap.idempotent_create`, `profile_bootstrap.auth_enabled`, `edge_profiles.compact_transport`, `collections.feed_epoch`, `collections.contract`, `collections.source_scoped_items`, and `lumae_analysis_profiles`.

> Note: `lumae_analysis_profiles` is a **manifest** capability in `plugin.json` (with `schema_version`, `analyzer_version`, `profile_source`, `features`). It is not part of the health payload. See §9 item 1.

### Choosing v2 bootstrap (what Auralscape does today)

The client uses v2 only when all of these hold: `status == "ok"`, `profile_bootstrap.available`, `protocol_version == 2`, `schema_version == 1`, `auth == "host_authenticated"` and `transfer_contract == "source_scoped_v1"`. Otherwise it uses the legacy `/profiles/bootstrap`. Incremental sync always uses legacy `/profiles/changes`. This client behaviour (`profileBootstrapV2.ts:69-93`, `pluginEnrichmentSync.ts:264-325`) was *verified in the 2026-09-24 audit* (`docs/audit/2026-09-24/LUMAE_AUDIT_2026-09-24.md`; hand-off H.2); it cannot be checked from this repo.

### Discovering `catalog_instance_id`

Every profile route is scoped by `catalog_instance_id`. Clients get it from `GET /api/catalog/health` → `servers[].catalog_instance_id` (`__init__.py:1867` onwards).

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
| 410 | `bootstrap_required` | the cursor epoch ≠ the current epoch, **or** `cursor.seq < floor_seq` (the journal was compacted past it) (591-593) |
| 400 | `invalid_cursor` | malformed cursor; the cursor belongs to another source; **the cursor is ahead of head** (594-596) |

Retention: each publication compacts the journal to its last **50,000** events (`PROFILE_CHANGE_RETENTION_EVENTS`, `catalog_enrichment.py:50, 497`). The maintenance path uses `max(1000, 2 × profile_count)` (`catalog_enrichment.py:158-210`, line 193; `catalog.py:50-55`). A client more than about 50k events behind gets a 410 and must bootstrap again.

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
| `POST /api/profiles/bootstrap/sessions` (create; 220-289) | `page_size`: int **1–500**, default 250. It is fixed for the whole session. | envelope + `session_token` (64 hex), `page_size`, `snapshot_count`, `total_profiles`, `catalog_epoch`, `profile_epoch`, `snapshot_seq`, `expires_at`, `snapshot_cursor`, `cursor` (= `snapshot_cursor`), `next_page_token` (always present, for ordinal 0) |
| `POST …/sessions/page` (292-312) | `session_token`, optional `page_token`; **`page_size` is forbidden** | envelope + `profiles:[…]`, metadata¹, `cursor` (= snapshot cursor), `next_page_token\|null`, `has_more` |
| `POST …/sessions/catchup` (315-382) | `session_token`, optional `page_token`; `page_size` forbidden | envelope + `changes:[{seq,track_id,operation,payload,created_at}…]`, metadata¹, `cursor` (last seq in this page), `head_cursor`, `next_page_token\|null`, `has_more` |
| `POST …/sessions/release` (385-397) | `session_token` | **only** `{protocol_version, schema_version, transfer_contract, released: true}`, with **no `catalog_instance_id`** (396-397). Clients must not apply full-envelope validation to the release response. |

¹ metadata = `catalog_epoch`, `profile_epoch`, `snapshot_cursor`, `snapshot_seq`, `total_profiles`, `expires_at` (213-217).

**Semantics:**
- **Create.**
  - Runs synchronously in the request, on its own connection, under the **global** session advisory lock `pg_advisory_lock(110094, 10)` (142).
  - Captures every published profile of the source in one REPEATABLE READ snapshot, **with each row's edge embedded at capture time** (250-275), and pins `snapshot_seq` = the profile head at capture.
  - Deletes expired sessions first (229).
- **Session lifetime.** Fixed at **60 minutes from creation** (`now() + interval '60 minutes'`, 245). Pages do **not** extend it. (`SESSION_MINUTES` at line 26 is not used.)
- **Tokens.**
  - `session_token` is a bearer secret; the server stores only its SHA-256.
  - A `page_token` is `base64url(json{s,p,o,z}).hex_hmac_sha256`, at most 2,300 characters (77-101). It is bound to the session, the phase (`snapshot`/`catchup`), the ordinal and the page size.
  - The first page of either phase may omit `page_token` (ordinal 0).
- **Page.**
  - Reads the frozen snapshot in ordinal order.
  - The ordinal must be ≤ `snapshot_count` and a multiple of `page_size`, otherwise 400 (298-299).
  - Profiles are the frozen JSON, including an edge that may have been replaced since.
- **Catch-up.**
  - The first call (ordinal 0) materialises the journal from `snapshot_seq` to the **current** head into the session. It returns 410 if `snapshot_seq < floor_seq` or if a seq gap is found (324-360).
  - Later calls page that frozen set.
  - The final `cursor` equals `head_cursor`. Continue incremental sync with legacy `/profiles/changes` from it.
- **Session validity (every page, catch-up and release; `_session`, 181-201).** The token must exist, the body's `catalog_instance_id` must match, the session must not be expired and must be schema 1, and the source must still be `active` with an unchanged core server id, catalog epoch and profile epoch. Otherwise the answer is **410**.

**Status codes** (the `error` code equals the `message`):

| Status | Code | Cause |
|---|---|---|
| 400 | `invalid_profile_bootstrap` | bad envelope; `page_size` outside 1–500 or not an int; `page_size` on page/catchup/release; malformed, forged, wrong-phase or misaligned `page_token`; `session_token` not 64 lowercase hex; body > 16 KiB, not JSON or not an object |
| 410 | `bootstrap_required` | **create:** the source is unknown, inactive or has no core server id (`_state` 176-177, called at 236). The 429 slot check (233-235) runs first, so a host whose slots are full answers 429 even for an unknown source. **Page/catchup/release:** unknown token (page/catchup), expired session, source inactive or rebound, epoch changed, catch-up floor passed or gap. **Release** of an existing but expired or stale session also returns 410, and the row is **not** deleted: it keeps its slot until it expires (393). |
| 413 | `bootstrap_snapshot_limit` | create: > **200,000** rows or > **128 MiB** of compact JSON (edges included). The first catch-up: > **50,000** events or > 128 MiB (270-271, 352-353). Nothing is kept, so a retry fails the same way. Fall back to legacy bootstrap. |
| 429 | `bootstrap_session_limit` | ≥ **4** unexpired sessions for the source or ≥ **32** globally (233-235). There is **no `Retry-After`** in 1.2.5. |
| 503 | `bootstrap_unavailable` | `config.DATABASE_URL` is unset (2601-2602); any database error, statement timeout (20 s), lock timeout (5 s; a second concurrent create waits up to 5 s for the global lock), or any other unexpected exception (156-157, 2612-2613). Nothing is logged. There is no `Retry-After`. |

> Note: the v2 routes never return **404** or **409**. A 404 means the route does not exist (the plugin predates v2): treat v2 as unsupported. 409 is not used by v2.

Release is idempotent for tokens the server does not know: it returns 200 `released:true` for an unknown or already-released token.

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
| K2 | v2 snapshots store an edge *reference* and resolve it at page read. Wire format unchanged, except a row whose edge was replaced after capture arrives without `edge_profile` (the catch-up re-supplies it). | P1-5 | None; already handled | None | 1 | planned |
| K3 | v2 sliding expiry: each page extends `expires_at` to at most `created+24h` | P1-6 | Send `expiry_mode:"sliding"`; accept a changing `expires_at` (C-6) | `capabilities.profile_bootstrap.sliding_expiry:true`; create body field | 1 | planned |
| K4 | `Retry-After` on 429 and 503; truthful `available`; new `auth_enabled` field (the `auth` string is unchanged) | P1-6 | Back off and honour `Retry-After` (C-3) | Always additive (`capabilities.profile_bootstrap.auth_enabled`) | 1 | planned |
| K5 | Optional create `client_request_id`; a duplicate unclaimed session is replaced, not leaked | P1-6 | Send a UUID per create attempt (C-3) | `capabilities.profile_bootstrap.idempotent_create:true` | 1 | planned |
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
4. **The v2 session is absolute (60 minutes)**, hard-coded in SQL (`profile_bootstrap.py:245`); `SESSION_MINUTES` is unused. Release of an expired or stale session answers 410 and **leaves the row in place**, so it holds one of the 4 per-source slots until it expires (audit AUD-11). K5 alone does not fix this; P1-6 should.
5. **`profile_bootstrap.available`** is `bool(DATABASE_URL)`, not a probe; `auth` is constant even when `AUTH_ENABLED=false` (K4).
6. **Cursor ahead of head** on `/profiles/changes` is **400 `invalid_cursor`**, not 410. The collections feed returns an empty 200 in the same case. K8 makes collections answer 410; profiles are unchanged.
7. **The boundaries formula** in plan K7 (`source.sample_rate` + `source.decoded_frames`) and in C-12 (`origin_frame`/`covered_frames`) are equivalent. Both were verified against `edge_profiles.py:146-152` and the golden fixture. §4.4 is the exact statement. `rate` is the **source** rate, not the 48 kHz measurement rate.
8. **Payloads differ by path.**
   - Fixed for new events by P1-1: every path emits float4 `ref_lufs` and the same serializer (§3.4).
   - Events recorded by 1.2.5 and still in the journal carry float64 `ref_lufs` and went through the `catalog.canonical_json` sanitizer (NFC, trim, `""`→`null`).
   - Clients should keep comparing `ref_lufs` with a tolerance and treating `null` ramps like empty ramps, for 1.2.5 servers and old journal entries.
   - Rows published with a bare `media_fp` signature (no `catalog-media:` prefix) are not "current" for the analysis hook and compare as changed media, so after the upgrade each such row is re-analysed once and loses its edge once (the edge upgrade is then rescheduled for the prefixed signature).
9. **Profile journal retention** is a fixed 50,000 events per publication, but `max(1000, 2×count)` in maintenance compaction. At 94k profiles, publication-time compaction is the binding limit.
10. **`/api/profiles` silently truncates `ids` to 500.** K6 clients fetching misses must batch ≤500 ids and must not treat an unlisted id as "missing".
11. **Timestamps** mix zone-less (`analyzed_at`) and offset (`expires_at`, `created_at`) forms (§1.6). The audit's "`expires_at` is non-UTC on a non-UTC server" is confirmed.
12. **Collections today** (the baseline for K8/K9):
    - creating an existing id returns 201 with the existing (or `null`) collection and no event;
    - a duplicate membership silently remaps the item id;
    - `idempotency_key_conflict` has no `current`;
    - the feed has no epoch in the response (an epoch exists internally in `collection_feed_state`);
    - journal and receipts are never compacted.
13. **Shelves idempotency** is keyed by mutation `id` only, with no body fingerprint. This is not covered by K1–K11; it is recorded here for completeness.
