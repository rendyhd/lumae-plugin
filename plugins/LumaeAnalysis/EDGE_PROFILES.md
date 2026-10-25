# Optional EdgeProfileV2

EdgeProfileV2 adds source-bound transition evidence to waveform profiles. It is
additive: legacy loudness, MixRamp, sync and existing databases continue to work
without it. Direct fetches, bootstrap and deltas expose the same optional
`edge_profile` object.

The producer needs PyAV >= 16.0 (`requirements-edge.txt`; an AudioMuse image
that already ships a newer PyAV, such as 17.1.0, needs nothing installed). The
version is not pinned: on first use each process qualifies its runtime by
running the bundled reference inputs in `edge_qualification_v2.json` through
the production code: the published golden PCM input, plus deterministic
44.1 kHz stereo and 96 kHz mono WAVs through the real demuxer, decoder and
libswresample. The reference was recorded with PyAV 16.1.0 / libswresample
6.1.100. Frame counts, boundaries, digital-zero positions and all flags must
match exactly; centidB values may differ by at most 2 (0.02 dB) and Q15 values
by at most 4. PyAV 17.1.0 / libswresample 6.3.101 matched exactly (a difference
of 0, and a bit-identical resampler output). The tolerance only absorbs
floating-point rounding flips across CPUs and builds. Qualification takes about
0.1–0.2 s and is cached for the process. `edge_runtime_status()` reports
`available`, `pyav`, `libswresample`, `reason` (`pyav_missing`,
`pyav_too_old`, `libswresample_missing`, `runtime_unqualified`,
`qualification_error`) and `qualified_at`. Payloads record the actual runtime
in `measurement.resampler` (`pyav-<version>-swr-<version>`) and
`source.decoder` (`pyav-<version>:<codec>`). No request installs packages or
downloads models. If the runtime is absent or unqualified, health reports
`capabilities.edge_profiles.available=false`; normal analysis remains available.
The setting `edge_profiles_enabled=false` disables scheduling.

With edges on, one analysis call makes a track's waveform, edge and loudness
from one download; a waveform analysed without its edge leaves it to the
library pass. A failed V2
job never changes a ready legacy row. An opaque media revision and unique job
token protect publication from source replacement, duplicate workers and stale
retries. Edge data, job readiness and the profile cursor commit atomically.
Internal paths and media signatures are never published.

Authenticated endpoints:

- `POST /api/profiles/edges/analyze`: `catalog_instance_id`, `ids` (maximum 100)
- `POST /api/profiles/edges/backfill`: `catalog_instance_id`, optional `after`
  track ID and `limit` (1–100)

On-demand requests (`analyze`) run at high priority and promote a track a
background batch already holds. From 1.9.0 a request is recorded for Lumae's
runners and served first in their next pass, between the files of a library
pass; nothing is refused, so `deferred` is always empty. The worker runner (the
web process with `heavy_work=server`) walks the library itself at low
priority; the `backfill` route remains for older clients and makes that pass
due. Media the analyzer can never
measure (more than two channels, unsupported rate, duration limit, no audio)
is marked `unsupported` and not retried until it changes. Workers report their
runtime so health's `analyzable` is true even when the web process has no PyAV.

Pending or running jobs coalesce. Abandoned jobs retry after 30 minutes and
failed jobs back off for six hours. Source revision changes may retry
immediately. Each file has a 15 minute deadline and a 6,553.6 second decoded
duration ceiling.

## Schema v3 (1.7.0)

Clients that ask for it (`edge_schema=3`; capability
`edge_profiles.schema_versions: [2, 3]`) get schema v3, a deterministic
projection of the stored edge: the same fields without the windows'
`peak_cdb` and `spectral_flux_q15` (no client planner reads them), with its own
`profile_digest`. When a song's music ends before the last 30 seconds (the tail
window opens on a silence that began before it: a hidden track after a long
silence, or a long silent run-out; about 0.1% of a library), v3 adds
`landmarks.music_end_frame`, where that silence begins, and an `ending` window
measured like `tail` over the 30 seconds that end there. Clients end the song
there and plan its fade on `ending`. v2 transfers and v2 digests are unchanged.
The exact rule and the negotiation are in `docs/contracts/LUMAE_SYNC_CONTRACT.md`
§4.5.

How it is built:

- **Measurement.** While it streams, the analyzer also keeps the whole file's
  100 ms level series (from frame 0, measured like `level_cdb`; a few bytes per
  second). At the end it applies the contract's rule. Only when the rule holds
  does it decode the file a second time, through the same path, up to a second
  past the music end, and measure `ending` there like `tail`; the second decode
  reproduces the first exactly. Other songs cost only the level series. A
  second pass that fails or runs out of time leaves the v2 edge without the
  music end. Timed on a Mac against 1.6.1's analyzer: the qualification corpus
  within ±1%, a 240 s song +4.2% (the level series), a 240 s song whose music
  ends at 150 s +70% (its second decode).
- **Storage.** `edge_profiles.payload` stays the v2 edge, byte for byte (1.6.1
  can read it after a downgrade). Beside it: `profile_digest_v3` (the v3
  projection's digest), `v3_ending` (`{"music_end_frame", "ending"}`, or NULL)
  and `v3_recheck`. Every transfer builds v3 from these in SQL
  (`edge_payload_sql`); an edge reference in v3 names the v3 digest.
- **Upgrade.** Edges stored before 1.7.0 get their v3 digest after the
  migration and in runner turns (reconcile ticks up to 1.8.0), in short batches. Health lists schema 3 only
  once none is missing. The same pass flags an edge whose tail window opens
  under the excerpt floor (`v3_recheck`); the library pass measures those again
  (about 0.1% of a library), so their music end is found, and the new edge
  reaches clients as an edge publication.

## Measurement contract

The analyzer retains only the first and last 30 seconds, including partial
100 ms bins. The decoder and 48 kHz resampler remain continuous for the complete
source. It publishes:

- continuous K-weighted mean power and source sample peak
- four-times polyphase oversampled true peak
- continuous complementary fourth-order low, mid and high analysis bands at
  150 Hz and 2.5 kHz
- bounded spectral-flux and onset-density evidence in unsigned Q15
- an adaptive noise floor, ramp/body landmarks and confidence
- exact leading and trailing digital-zero counts
- a hidden-content guard when late audio follows a long quiet span

Exact digital zero is the only padding evidence. Adaptive quiet, a noise floor,
or a ramp landmark never authorizes destructive trim. Raw source peaks also
protect quiet material that K-weighting attenuates. Unknown layouts, non-finite
PCM, changed source identity, timeline inconsistencies and quantization overflow
fail only the optional upgrade.

All positions use decoded source frames. Values are base64 little-endian signed
int16 centidB (`-32768` is exact zero) or unsigned Q15. Validity masks are LSB
first. Peak bounds round upward. Short files may have identical head and tail
windows. `profile_digest` is SHA-256 over sorted-key compact UTF-8 JSON excluding
only the digest itself.

`tests/plugins/edge_profile_v2_golden.json` is the published producer fixture.
The source hash is checked before and after decode. `timeline_verified=true` is
limited to the declared lossless decoder path. A client must still bind the
profile to the exact playback representation, decoder and seek behavior before
executing a trim or transition. A provider transcode is a different
representation and invalidates the plan.

Tests need `requirements-dev.txt` and `requirements-edge.txt`. Set
`LUMAE_POSTGRES_TEST_DSN` to a disposable database for transaction tests. CPU and
RSS soak, native route safety, transition rendering and listening preference are
separate qualification gates.
