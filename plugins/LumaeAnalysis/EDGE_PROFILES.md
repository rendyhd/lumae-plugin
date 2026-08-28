# Optional edge profiles

EdgeProfileV1 adds source-bound head/tail measurements to waveform profiles. It does not replace legacy loudness or MixRamp data. Direct profile fetches, profile bootstrap and profile deltas publish the same optional `edge_profile` object. The outer schema/analyzer versions remain 1.

Install `requirements-edge.txt` explicitly in the worker environment and restart it. This pins PyAV 16.1.0; the producer requires libswresample 6.1.100. No request installs packages or downloads models. Without that runtime, health reports `capabilities.edge_profiles.available=false`; normal legacy analysis remains available. The setting `edge_profiles_enabled=false` disables scheduling.

Successful legacy analysis schedules a separate optional upgrade. Ready legacy rows are never marked failed because an upgrade failed. Source revision and a unique job token protect publication from source replacement, retries and duplicate workers. Edge data, job readiness and the profile delta/cursor commit together. Internal path/size/mtime signatures are published only as opaque SHA-256 tokens in both `media_signature` and the additive `media_revision`.

Within the existing authenticated plugin API:

- `POST /api/profiles/edges/analyze`: `catalog_instance_id`, `ids` (at most 100). Uses interactive priority.
- `POST /api/profiles/edges/backfill`: `catalog_instance_id`, optional `after` track ID and `limit` (integer 1–100). Returns `next_after`; restart from the beginning after pending jobs finish or failed jobs become retryable. This is a bounded pass, not proof all profiles are ready.

Pending/running jobs coalesce, abandoned jobs can retry after 30 minutes, and failed upgrades back off six hours. Changed source revisions may retry immediately. Workers reacquire the authorized source instead of depending on expired hook downloads. Each file measurement has a 15-minute deadline and a 6,553.6-second source-duration cap.

Measurement keeps the first/last 30 seconds, including partial 100 ms bins. It uses a canonical 48 kHz resampler with continuous state, fixed two-stage weighting, and ungated channel-mean power. Source sample peaks and exact digital silence are measured before resampling/filtering. V1 accepts explicit mono/stereo layouts only. This is versioned weighted power, not standardized LUFS. Unknown layouts, non-finite samples and centidB overflow fail the optional upgrade without changing the ready legacy profile.

Values are base64 signed int16 little-endian centidB, with `-32768` reserved for exactly zero. Separate validity masks use least-significant-bit-first indexing. Peaks round upward; missing bins cannot be treated as silence. Bin boundaries refer to decoded source frames. Short sources can have identical head and tail coverage.

The golden contract fixture is `tests/plugins/edge_profile_v1_golden.json`; the app carries the same fixture. SHA-256 covers UTF-8 sorted-key compact JSON excluding only `profile_digest`. The content hash is checked before and after decode. Producer timeline verification is limited to the declared lossless decoder path; a client must still independently bind the actual playback representation and seek behavior before trimming or overlap decisions. Transcoded streams and duration guesses are not verified originals.

Tests require `requirements-dev.txt` and `requirements-edge.txt`. Set `LUMAE_POSTGRES_TEST_DSN` to a disposable database to run transaction tests. CI provisions its own database. No production database is required. CPU/RSS performance, native route safety and listening preference remain separate qualification gates.
