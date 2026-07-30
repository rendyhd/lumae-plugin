# Lumae Analysis 1.0.1 release validation

Validated on 2026-07-31 from `codex/v1.0.1-resource-safety`.

## Root cause

Version 1.0.0 loaded an entire native-rate media file with librosa, converted
each channel to float64 twice for the two BS.1770 filters, and stacked another
full filtered array. Peak working memory therefore grew with duration, sample
rate, and channel count. A stereo 48 kHz file needed roughly
`40 * sample_rate * seconds` bytes across the dominant arrays; a 90-minute file
is about 9.66 GiB before decoder and Python overhead. This matches the reported
Python resident set near 9.78 GiB and explains the host's swap pressure and
eventual lockup.

The same release also had secondary amplification paths: infinite RQ timeouts,
whole-catalogue backfill/status reads, install-triggered projections, unbatched
parameter snapshots, repeated album-to-track scans, and all-pairs album/artist
relationship ranking.

## Implemented controls

- PyAV decodes audio incrementally; stateful SciPy filters and exact 100 ms
  windows carry across decoder block boundaries.
- Per-track limits are 15 minutes of analysis time, eight channels, 384 kHz,
  and the approximately 109-minute range representable by the unchanged
  v1 ramp format.
- Every heavy RQ job has a finite timeout. Background waveform batches default
  to three tracks and are capped at ten.
- Backfill selection and status counts execute as bounded/aggregate PostgreSQL
  queries.
- Installing the release does not project current data. No-change projection
  calls retain their existing generation and write no rows, while failed
  projections remain eligible to recover and publish.
- Backfill retries `skipped_no_file` rows through the source-scoped
  `ProviderCatalogBridge`, including AudioMuse v3 registry sources that do not
  populate legacy global media-server credentials.
- Catalogue normalization indexes tracks by album once and writes generated
  parameters in batches.
- Relationship ranking uses AudioMuse's MusicNN IVF index, scores at most 384
  candidate entities per source entity, caps within-entity pairwise samples at
  160 tracks, and has no all-pairs fallback.
- If IVF is unavailable, the state becomes `waiting_for_index` and the last
  published generation remains readable.
- The settings maintenance switch prevents new catalogue, projection,
  waveform, finalizer, and relationship work while preserving published data.

## Verification evidence

- Plugin suite without a PostgreSQL DSN: `204 passed, 3 skipped` (the skipped
  cases are the optional database-backed modules).
- Full Lumae Analysis suite with PostgreSQL enabled in the actual AudioMuse
  image: `206 passed`.
- PostgreSQL integration suite against a temporary PostgreSQL 16 instance:
  `39 passed`.
- Lumae mobile contract suites: `42 passed` across enrichment sync, enrichment
  persistence, profile decoding, and ramp/loudness behavior.
- Python bytecode compilation and `git diff --check`: clean.
- Actual AudioMuse runtime image (`PyAV 13.1.0`, pinned NumPy/SciPy stack):
  72 randomized streaming cases across six sample rates and one, two, and six
  channels matched the v1 whole-buffer reference exactly (`LUFS delta 0`,
  identical start/end ramps and duration).
- Real decoder coverage passed WAV, FLAC, MP3, AAC/M4A, Opus/Ogg, six-channel
  WAV, and 192 kHz WAV. Video-only input and sources above the eight-channel or
  384 kHz limits were rejected before unbounded analysis.
- Actual PyAV peak-memory probes at stereo 48 kHz:

  | Duration | Baseline | Peak | Analysis delta |
  | --- | ---: | ---: | ---: |
  | 60 seconds | 108.4 MiB | 127.6 MiB | 19.2 MiB |
  | 900 seconds | 108.6 MiB | 128.0 MiB | 19.4 MiB |

  The essentially flat delta confirms that decoded/filtered working memory no
  longer scales with media duration.
- AudioMuse API inspection confirmed that every supported core version from
  2.6.0 exposes the app-context task wrapper, direct RQ queues, PyAV 13.1, and
  `ivf_manager.multi_query_ids`.
- Relationship schema and advertised algorithm contract remain version 1, so
  no coordinated mobile release or forced relationship rebuild is required.

## Artifact

- File: `dist/lumae_analysis/lumae_analysis_1.0.1.zip`
- Contents: 16 deterministic code/helper files; `plugin.json`, caches, and
  bytecode are excluded.
- MD5 (AudioMuse catalog checksum):
  `3c2fc6306d3caaff48110d675cabbd65`
- SHA-256:
  `cdf0dcb5ca360a2417bb14e3301ab30aa8bc52959456dbff79083fef80aa4032`

The optional PostgreSQL cases were run separately and passed in full as
recorded above.
