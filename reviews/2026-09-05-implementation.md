**Implementation record — 5 September 2026**

Implemented the fixes for all 16 findings in the [original review](C:/Users/rendy/vscode/lumae-plugin/reviews/2026-09-05-plugin-review.md), including the pre-existing Friend Album Discovery work. This is a source implementation and validation record; no production deployment or model/listening qualification was performed.

The DJ architecture now separates [capability negotiation](C:/Users/rendy/vscode/lumae-plugin/plugins/LumaeAnalysis/dj_capabilities.py), [job storage and ownership](C:/Users/rendy/vscode/lumae-plugin/plugins/LumaeAnalysis/dj_jobs.py), [worker orchestration](C:/Users/rendy/vscode/lumae-plugin/plugins/LumaeAnalysis/dj_service.py), [durable maintenance](C:/Users/rendy/vscode/lumae-plugin/plugins/LumaeAnalysis/dj_maintenance.py), [model execution](C:/Users/rendy/vscode/lumae-plugin/plugins/LumaeAnalysis/dj_runtime.py), [shared evidence](C:/Users/rendy/vscode/lumae-plugin/plugins/LumaeAnalysis/dj_evidence.py), and [settings rendering](C:/Users/rendy/vscode/lumae-plugin/plugins/LumaeAnalysis/dj_ui.py). V2 and V3 independently project immutable evidence and retain their separate wire/storage contracts. Compatibility entry points remain available. Pending V2/V3 requests for the same source and calibration share inference.

| Review finding | Implemented behavior |
| --- | --- |
| 1. Flat speech marked safe | Reject flat, negative, degenerate and insufficiently reduced voice-band evidence. Quiet intervals must include observed guards. |
| 2. Cut authorization bypass | Check explicit cut authorization before accepting either low calibrated risk or a speech minimum. Unauthorized payloads contain no eligible cuts or verified loops. |
| 3. Live-worker reclamation | A PostgreSQL session lock covers the analysis lifetime. Only its owner can reclaim abandoned work; each claim gets a new token, and publication checks both ownership and current source. |
| 4. Missing wake-up schedule | Install an opt-in one-minute reconciler. Coalesce dispatch and recover enqueue failures, profile deferral, setup requests and interrupted running jobs. |
| 5. Public build failure | Select release sources explicitly. Verify/reuse immutable public 1.1.8, preserve the immutability guard for source releases, and exclude private development. |
| 6. V3 retry loop | Share retry semantics across versions: transient backoff, five automatic attempts, cached unsupported/cancelled results, source/method/calibration invalidation and explicit boolean force. |
| 7. Duplicate cue slots | Deduplicate each role by timestamp and retain supporting region provenance. |
| 8. Lost long-track tail | Sample bounded regions, band windows, structural boundaries and speech minima across the full timeline, including the tail. |
| 9. Excessive cancellation SQL | Cache cancellation for 500 ms, retain frequent local deadlines, force checks at stage/publication boundaries and close cancellation read transactions. |
| 10. Removal on wrong host | Settings record a durable command and disable analysis. The dedicated worker removes artifacts after acquiring analysis and setup ownership, and records the result. |
| 11. Missing V3 cancellation | Add a V3 cancel route using the shared repository, tested against a running job in another PostgreSQL session. |
| 12. Incomplete CI | Discover the full plugin suite; trigger on scripts/release/runtime changes; retain PostgreSQL 17; add a separate manual real-runtime measurement workflow. |
| 13. Federation ownership | Bind connections, sync requests/publication, deletion, catalogue search, similarity and artwork to the authenticated owner. |
| 14. Host installation incompatibility | Accept the host-supplied migration database and probe the scoped-bearer extension. Registration works without it; token creation fails closed with a useful reason. Federation remains private. |
| 15. Full-catalogue interactive work | Resolve the local instance once; use indexed SQL prefix search with metadata-only projection; use a bounded LSH shortlist before full dynamics scoring. |
| 16. Unbounded HTTP sync | Return after durable enqueue. Worker sync enforces page, time, transfer, identity and cursor-progress limits; token-fenced publication preserves the prior cache on failure. |

Additional design changes preserve exact source start/EOF separately from estimated audible boundaries, keep energy-only loops unverified, avoid claiming playback timeline verification, and evaluate coverage using each cue's local tempo, speech and section evidence. Read responses budget full annotation transfer to 2 MiB with `next_ids` continuation; capabilities advertise those limits. Settings distinguish worker availability, job counts, calibration authorization, playback qualification and model-removal status.

Optional edge/DJ records now have a bounded, 30-day orphan-retention policy. Federation's Core 3 projection pins an explicit server and uses provider-to-analysis mappings instead of mixing registry sources. Shared golden fixtures verify the catalogue and federation album-ranking math.

**Validation**

- **458 passed, no skips**, using the complete suite and an isolated PostgreSQL 17 database.
- New integration coverage exercises independent live sessions, lost connections, new ownership tokens, stale publication, concurrent first requests, retries, source/calibration changes, cancellation through Flask, removal through settings, dispatch recovery, shared V2/V3 inference, byte budgets and retention.
- Federation integration covers cross-user access denial, missing host authentication support, durable sync, deletion during fetch, cursor failure, transfer deadlines, bounded queries, shortlist ordering and server selection.
- Python compilation, `git diff --check`, deterministic/private packaging tests, and public release-policy validation pass. The public archive still matches its published checksum.
- Real PyAV decode of a 60-second WAV with inference stubbed: **1,419 per-frame cancellation callbacks versus 1 throttled callback**. The real worker additionally forces checks at stage boundaries. This measures callback overhead, not inference throughput or database latency.
- Synthetic conversion of **100,000 relationship inputs** peaked at **363,905,024 bytes (~347 MiB)** process RSS, with 0.375 seconds spent converting the buffered inputs on this machine. This excludes PostgreSQL, provider acquisition, entity construction and candidate scoring.

Raw measurement summaries are in [performance evidence](C:/Users/rendy/vscode/lumae-plugin/reviews/2026-09-05-performance-evidence.json). Reproduction tools and exact operating requirements are documented in the [runtime guide](C:/Users/rendy/vscode/lumae-plugin/runtime/README.md).

**Deployment and qualification limits**

Use a new private prerelease and drain/update all DJ workers together: old worker code does not implement the new ownership protocol. The dedicated host must attest `lumae-dj-host-v1`; installing Python dependencies or setting an environment variable on a stock queue implementation does not add DJ routing. Clients making large analysis reads must consume the advertised continuation IDs.

Per-job model initialization and two bounded decoder passes remain, with stage metrics to guide further optimization. The inference deadline is cooperative; hard interruption of blocked provider/native calls belongs to host process supervision. The supplied manual qualification workflow measures an administrator-provided local corpus in isolated processes, with hard timeouts and peak RSS. No representative corpus, rendered PCM seam evaluation, player/decoder alignment or listening qualification was available in this task, so public playback remains unqualified.

Federation's shortlist is approximate and can omit a globally best scoring album. The scoped-bearer host extension is still an external requirement; the plugin now detects its absence instead of failing installation or suggesting pairing will work. These limitations are explicit in the private development and runtime documentation.
