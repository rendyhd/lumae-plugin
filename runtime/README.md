# Private DJ host contract and qualification

Further DJ source development is separate from the immutable public Lumae Analysis 1.2.0 release. Build a new `1.2.0-djtest.N` with `scripts/build_private_dj_prerelease.py`; do not republish an earlier private version or overwrite the public archive. `release-sources.json` selects the public source explicitly. The public builder verifies the pinned archive and excludes private plugins.

## Host contract

The dedicated worker must attest `lumae-dj-host-v1`. This declaration means the host provides all of these behaviors:

- `plugin.api.enqueue(..., queue="lumae-dj")` actually routes to the dedicated DJ queue.
- DJ task execution and worker hooks have the correct plugin/database context and a stable PostgreSQL session for each job. Transaction-pooling proxies are unsuitable for the execution lock.
- Only the dedicated model host sets `LUMAE_DJ_WORKER=1` and `LUMAE_DJ_HOST_CONTRACT=lumae-dj-host-v1`.
- The host calls `attest_dj_worker_capability()` periodically, including while DJ mode is disabled. The existing private DJ test image's two-minute attestation loop meets this requirement; attestations expire after ten minutes.
- The host has the pinned CPU dependencies from `requirements-dj.txt`, local model storage, and an appropriate process/container resource limit. Optional packages are not installed from a playback request.

The inspected `lumae/audiomuse-ai:3.3.1-dj-v2-test.9` image supplies custom DJ routing and attestation. Its dedicated worker needs the additional contract environment variable for this source revision. Setting that variable on a stock image does not add the missing routing implementation. Configure the custom core consistently on the web and worker sides.

Stop/drain existing DJ workers before installing this revision, then update all DJ workers together. Older plugin code predates the session-lock protocol and must not participate in the same durable queue during rollout. Installation is additive; it preserves V2 and V3 tables. Methods change to `...v2.4` and `...v3.1`, so old annotations become stale and can be requested again.

Enabling DJ Mode installs/enables a one-minute reconciliation schedule on the ordinary queue. Dispatch is coalesced for two minutes and waits for live analysis/setup ownership. Gated work, failed enqueue attempts, abandoned running work, and missing setup all receive another wake-up. Disabled mode stops analysis dispatch; a pending model-removal command keeps reconciliation enabled until its worker result is available.

A PostgreSQL session lock covers acquisition, inference, projection and publication. Claiming abandoned work issues a new token. Cancellation, source identity and token checks fence publication; cancellation is polled at most twice per second during compute, with forced checks between stages and before publication. Model removal waits for analysis ownership and the setup lock, then deletes artifacts on the worker host.

The analysis deadline is cooperative and includes elapsed source acquisition when allocating the remaining inference budget. It cannot interrupt a blocked provider call or native inference call. Model downloads have a separate bounded deadline and socket timeout. Host process supervision supplies any required hard end-to-end job limit; the plugin does not claim that AudioMuse enforces its ignored queue timeout argument.

## Client semantics

V2 and V3 keep separate API/storage contracts. Both use the shared job repository and immutable raw evidence. If both versions are pending for the same current source/calibration, the worker performs inference once and builds both projections. Per-job model construction and two bounded decoder passes remain; changing those requires measurements on the target host.

- V3 cancellation: `POST /api/dj/v3/analysis/cancel`, with the same source and `ids` fields as analysis requests.
- Both analyze routes accept an optional boolean `force`. Unsupported and cancelled results stay cached for the same source/producer; explicit force or a changed source/method/calibration permits a new attempt. Transient failures use 1/5/15/60-minute delays and stop automatic retries after five attempts.
- Analysis reads budget JSON payload transfer to 2 MiB per response. Consume `next_ids` until empty. An individual oversized analysis returns `analysis_response_too_large`; envelope/status fields add a small amount of overhead.
- Exact source start and EOF remain in `natural_boundaries`. `suggested_audible_boundaries` are estimates with `trim_authorized=false`.
- Scalar energy seam evidence creates loop candidates with `loop_verified=false`. Precise playback representation verification and public playback qualification remain false.
- Coverage reports use candidate-level tempo, section and speech evidence and identify themselves as eligibility estimates. They are not a rendered-transition or listening qualification.
- Optional edge/DJ data receives a 30-day grace period after absence is first observed in a published catalogue. Refresh maintenance processes at most 1,000 rows per table per pass, preserves active jobs, and clears orphan markers for returning tracks.

## Runtime measurements

The normal CI workflow discovers every test under `tests/plugins` and uses PostgreSQL 17. Model inference is a separate manual workflow, `Optional DJ runtime qualification`, on an administrator-provided runner labelled `lumae-dj-qualification`.

Install the exact CPU runtime on that runner and supply verified local model files and a consented corpus. No model or audio downloads occur in the qualification script. The corpus format is:

```json
{"cases": [{"path": "audio/speech.flac"}, {"path": "audio/long-track.flac"}]}
```

Paths are relative to the corpus file or absolute. Include speech, singing, instrumental music, quiet/flat passages, irregular rhythm, short sources, long tracks and the codecs/representations used by the player.

```sh
python scripts/qualify_dj_runtime.py --corpus /srv/lumae-qualification/corpus.json --beat-this /srv/lumae-qualification/final0.ckpt --yamnet /srv/lumae-qualification/yamnet.tflite --output qualification-report.json
python scripts/benchmark_relationship_inputs.py --tracks 100000 --output relationship-memory.json
```

Each audio case runs in a fresh process with a hard timeout and sampled peak RSS, builds both projections, and reports stage timings and payload sizes. Reports omit audio paths/titles and never authorize playback. Calibration approval, rendered PCM seam checks, listening evaluation and actual player/decoder alignment require separate evidence.

## Friend Album Discovery

Federation remains `private-development` and is excluded from the public manifest. A host without `set_bearer_authenticator` can register/install the plugin, but pairing-token creation fails closed with a specific missing-capability response. Declared minimum core versions alone do not promise that extension exists.

Connections and all derived catalogue/artwork operations are owner-scoped. Sync requests return HTTP 202 after recording durable work. A one-minute worker schedule recovers failed dispatch or worker loss; publication rechecks the connection owner and token after network access. A sync is limited to 300 seconds (checked between network chunks/pages), 400 pages, 100,000 albums, 6 MiB per page and 64 MiB total transfer. Repeated cursors, empty continuing pages and duplicate album identities fail without replacing the prior cache. Socket/DNS behavior can add time outside cooperative checks.

Text search uses indexed prefix tokens in SQL. Similarity uses indexed LSH bands to select at most 512 full fingerprints, then the existing Album Dynamics scorer. The shortlist is approximate and can miss a globally best result; shared golden fixtures guard the scoring math. This avoids full-catalogue fingerprint transfer and per-album instance-ID queries.

On Core 3, choose `FEDERATED_ALBUMS_SERVER_ID` before the first projection when multiple servers exist. The choice is persisted and provider IDs map through that server's `track_server_map`; incidental changes to the active server cannot mix libraries. Changing the selected catalogue requires an explicit migration/rebuild procedure, not silently following a request's active server.
