# Optional EdgeProfileV2

EdgeProfileV2 adds source-bound transition evidence to waveform profiles. It is
additive: legacy loudness, MixRamp, sync and existing databases continue to work
without it. Direct fetches, bootstrap and deltas expose the same optional
`edge_profile` object.

Install `requirements-edge.txt` on the analysis worker and restart it. The
producer pins PyAV 16.1.0 and requires libswresample 6.1.100. No request installs
packages or downloads models. If the runtime is absent, health reports
`capabilities.edge_profiles.available=false`; normal analysis remains available.
The setting `edge_profiles_enabled=false` disables scheduling.

Successful legacy analysis schedules a separate optional upgrade. A failed V2
job never changes a ready legacy row. An opaque media revision and unique job
token protect publication from source replacement, duplicate workers and stale
retries. Edge data, job readiness and the profile cursor commit atomically.
Internal paths and media signatures are never published.

Authenticated endpoints:

- `POST /api/profiles/edges/analyze`: `catalog_instance_id`, `ids` (maximum 100)
- `POST /api/profiles/edges/backfill`: `catalog_instance_id`, optional `after`
  track ID and `limit` (1–100)

Pending or running jobs coalesce. Abandoned jobs retry after 30 minutes and
failed jobs back off for six hours. Source revision changes may retry
immediately. Each file has a 15 minute deadline and a 6,553.6 second decoded
duration ceiling.

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
