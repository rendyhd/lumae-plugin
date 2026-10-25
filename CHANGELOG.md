# Changelog

Notable changes to the `lumae_analysis` plugin. Per-version metadata
(`min_core_version`, checksum, source zip) is authoritative in
[`plugins/LumaeAnalysis/plugin.json`](plugins/LumaeAnalysis/plugin.json); this
file gives the human-readable story, oldest detail first collapsed to what
still matters. Exact, code-verified wire behaviour for every version lives in
[`docs/contracts/LUMAE_SYNC_CONTRACT.md`](docs/contracts/LUMAE_SYNC_CONTRACT.md).

## Unreleased

New songs and their analysis reach the phone without waiting for a busy
AudioMuse (contract §2.5; Auralscape RCA 2026-10-24, SQ.1).

- `POST /api/catalog/refresh` no longer loses a request while AudioMuse's
  queue runs another job: it answers 202 `deferred` (`reason: "host_busy"`,
  `Retry-After: 60`) and keeps the request, and the watchdog runs it once the
  queue is free. A refresh already on the queue serves a new request (202
  `queued`, `coalesced: true`). Up to 1.9.0 the phone got a 409 and read the
  old catalogue.
- A refresh that changed the catalogue projects AudioMuse's analysis right
  after, so a new song never arrives without the analysis AudioMuse already
  has.

Similar albums and artists get published on a library that keeps changing
(contract §2.6; SQ.2).

- A relationship build that has staged its inputs finishes and publishes the
  generation it started when the catalogue or analysis changes, and leaves
  the state `queued` so the next build starts from the newer inputs. Before,
  every catalogue change restarted it from zero, so a library that changed
  more often than a build took never got a generation.
- A build step does as much as the watchdog tick's time allows (up to 1,024
  albums and artists, was 128).
- `POST /api/catalog/relationships/prepare` records the request and leaves
  the build to the watchdog; it no longer queues a task that did one
  20-second step and was refused, with an error in the log, at every sync
  while AudioMuse analysed.

Less repeated work (runner plan 2026-10-25, W2).

- Similar albums and artists are rebuilt at most every 6 hours, and no sooner
  than 15 minutes after the change that asked for it: one analysed song used to
  rebuild every album and artist (24 Oct on the owner's server: 55 slices over
  32 minutes). A source's first generation starts at once, and the settings
  page's rebuild still starts one now.
- A song longer than the loudness meter can measure (6,553.6 s) is no longer
  downloaded to find that out. A file that cannot be measured stops being
  retried until it changes: at once when the analyzer calls it unsupported,
  after its second decode error, and after the fifth failure of any other
  kind. It was downloaded whole and retried every 72 hours, forever (four
  1–2 GB files on the owner's server). Ramps and fades treat a second decode
  error of the same file as unsupported too; they retried every 6 hours.
- MusicBrainz credits re-check an album it could not match, or that had no
  credits, after 30, then 90, then 180 days (was 7, 30, 90). Each expiry is
  spread ±25 %, so albums first checked together no longer come due together,
  and re-checks run only when nothing else waits, at most 60 an hour. The
  re-checks already due spread over the next 14 days.

## 1.9.0 (2026-10-24)

Journeys (contract §5.7; Auralscape JN.10). Clients that don't know them never
read them.

- Lumae's listening journeys (an artist's albums in order, a collection, songs
  from anywhere, each song's played state and the place to continue from)
  are kept on the server, so they survive a reinstall and follow you to
  another phone. `GET /api/journeys/changes` and
  `POST /api/journeys/mutations`, announced as `capabilities.journeys`
  (`schema_version: 1`, `max_journey_bytes: 1500000`).
- The phone owns a journey; the plugin stores what it sends. A write names the
  revision it was based on; a stale one is refused with the current record
  (409 `journey_conflict`), and the phone writes again over it.
- Two phones' writes merge: the receipts of journey plays (credits) are a set
  that only grows; per song, the state changed last wins, so a song played on
  one phone stays played and a song marked not played stays so; the place is
  the one played last.
- Gated by Living Collections and scoped per account and catalogue, like
  Vibes and covers. Deletes are tombstones; receipts expire after 30 days in
  the daily collection cleanup; a provider-identity rekey rewrites the song
  and album ids inside stored journeys.

## 1.8.0 (2026-10-18)

Whole-track loudness for Lumae's volume normalization (contract §3.9, K13;
Auralscape LN.3/LN.4). Clients that don't know the new stream never read it.

- The edge pass now measures each song's loudness the industry's way (ITU-R
  BS.1770-4: integrated loudness, true peak, EBU Tech 3342 loudness range,
  largest short-term and momentary loudness) in the same decode, from its
  48 kHz series. The edge payload is byte-identical with or without it.
- Files the edge pass refuses (more than two channels, unnamed WAV layouts,
  DSF above 384 kHz) and songs whose edge was computed before this release
  are measured by a low-priority backfill: at their own sample rate up to
  48 kHz, and through a 48 kHz conversion above it, so ultrasonic content
  doesn't count (DSD's shaped noise read 4.7-5.7 LU loud on two DSD256
  files). It runs after the reconcile tick's other track work, at most the
  background batch size per turn, one download and one decode per song,
  each turn's songs published together, backing off after a failure.
- Album loudness is pooled from the members' measurements (the album played
  whole) once every music track of the album is measured.
- A new stream carries it: `GET /api/catalog/loudness/bootstrap` and
  `/api/catalog/loudness/changes`, announced as
  `sync_contract.streams.loudness` and `capabilities.loudness`. Measuring
  never publishes a profile, edge or catalogue event, so it never moves those
  cursors. A provider-identity rekey keeps the measurements under the new ids
  and rotates the stream's epoch.

Faster background analysis (performance plan 2026-10-16, WP1a).

- The edge pass and the loudness backfill work until the reconcile tick's
  deadline instead of a fixed 30 s, reading due tracks a page at a time
  (25 edges, the admin batch size for loudness), so they get the tick's 45 s
  instead of about 30 s of every minute. A pass that reaches the end of the
  library finishes in the same turn. Like the waveform batch, either starts
  once 5 s of the tick are left, whatever its recent average (the average of
  a pass that fills the tick says nothing about what it needs).
- The waveform batch, the edge pass and the loudness backfill analyse
  several files at once inside the one reconcile tick (WP1b). The tick stays
  one AudioMuse task, so AudioMuse is blocked no more than before: only the
  analysis runs on K threads, each with its own analysis process at a low
  CPU priority (nice 10), reused for the whole tick; claims, downloads,
  publication and on-demand work stay on the tick's thread. On-demand work
  is served whenever a thread frees. No file starts once the tick's deadline
  is nearer than the pass's average time per file. The files in flight at
  the deadline run to completion (each within its per-file time limit), as
  with one file at a time, while on-demand work is served every 5 s. The
  first 1.8.0 test build stopped them 5 s after the deadline and started
  them again next tick, so a file longer than a tick (a two-hour DJ set)
  never completed, held its thread for good, and the edge pass never
  finished, which kept the loudness backfill from starting.
- K is the new setting "Files analyzed at once" (`background_workers`):
  `auto` (the default) takes the AudioMuse worker's cores minus two, a
  quarter of its (or its container's) memory at 512 MiB per file, and at
  most 4; 1 to 8 fixes it. 1 analyses one file at a time exactly as before.
  With K above 1 a waveform batch claims K times 10 tracks. A CPU-bound
  bench on 8 cores ran 1.86x faster at K=2, 3.38x at K=4 and 4.32x at K=6
  (`scripts/perf/parallel_bench.py`); not yet measured on the test instance.

One download and one analysis call per track (WP2).

- With edges on, the waveform backfill, on-demand work and AudioMuse's
  per-song hook make a song's waveform, edge and loudness in one analysis
  call on one download, instead of downloading and decoding it again for the
  edge pass and the loudness backfill. Each analysis still decodes the file
  itself, so every result equals what its own call gives (tested on 44.1,
  48 and 96 kHz files). The song hook's file serves all three, so AudioMuse's
  analysis runs about one edge analysis longer per song. Each analysis keeps
  the per-file time limit; the call's hard limit covers both.
- On-demand work no longer queues an AudioMuse task per track for its edge.
- A file the edge analyzer refuses (more than two channels, unnamed WAV
  layouts, DSF above 384 kHz) gets its waveform, an `unsupported` edge and
  its loudness from the standalone meter in the same call. Through the
  analysis child a refusal used to arrive without its reason, so such edges
  were retried every 6 hours instead of being marked unsupported; the
  refusal now crosses the process boundary (an allowlisted reason only).
- The edge pass and the loudness backfill become a mop-up: songs analysed
  with edges off, failed edges past their back-off, replaced media, the
  6-hour re-sweep, and on existing installs every song whose edge predates
  the loudness measurement.
- Analysis downloads fetch the original file through the catalogue server's
  own stream: Navidrome with `format=raw` (AudioMuse's download asks for a
  stream that may be transcoded), Jellyfin `static=true`. A download that
  fails falls back to AudioMuse's for the rest of the task. Files go to a
  directory per task outside AudioMuse's `TEMP_DIR`, which its `clean_temp`
  empties; a worker start removes those of tasks that are gone. With K
  analysis threads each thread fetches its own file.

Less work while idle (WP4: the scheduler plan's S7 and S8).

- Credits: an album MusicBrainz could not match, or matched without
  credits, is looked up again after 7 days, then 30, then 90, instead of
  every week, and such a re-check runs on the hourly idle schedule instead
  of putting the watchdog on minute cadence (one wave a week of every
  unmatched album). A changed album is looked up at once, as before.
- An idle tick does less: the credits walk over the catalogue's albums rests
  an hour after it reaches the end (at once after a new catalogue), and a
  tick walks once although it asks for a credits turn twice; an album whose
  job did not change is no longer rewritten; the settings page's waveform
  counts are refreshed by a tick that analysed nothing at most every
  5 minutes; a worker reports its edge runtime at most every 10 minutes
  (every tick reported, as each AudioMuse job is a fresh process). An idle
  tick on a small test library went from 59 to 26 database statements.

The scheduler panel shows throughput (WP5: the scheduler plan's S12).

- For waveforms, edges, loudness, relationships and credits: what was done
  in the last hour and the last 24 hours (from the reconcile journal), what
  is left (about, for edges and loudness), and the time left at the last
  hour's rate.
- The number of files analyzed at once, and the analysis queues' time left
  at the next higher number this host's cores allow, estimated from the
  bench (an estimate, not a measurement of this host).
- AudioMuse schedules waiting for their retry, which Lumae yields to, and
  how long ago each was refused (AudioMuse 3.3.0 and later).

AudioMuse's Scheduled Tasks lists only Lumae's five schedules.

- Lumae registers only `catalog_reconcile`, `catalog_refresh`,
  `provider_identity_recheck`, `analysis_projection` and
  `collection_retention`. The watchdog's one-action jobs (prepare, waveform
  and edge backfill, relationships, credits, and the loudness pass of the
  first 1.8.0 test build) were listed under Plugin tasks with an Enable box,
  and a schedule for one would have run it without its source arguments.
  Queued jobs still run by their dotted path.
- The migration removes schedule rows an administrator made for those names.
- On hosts whose `add_cron_task` takes a description, each schedule gets an
  info tooltip; the settings page's Scheduler panel lists the five, their
  state, what each does and whether to touch it.

## 1.7.1 (2026-10-13)

Follow-ups from testing 1.7.0 against AudioMuse's scheduler changes
(NeptuneHub/AudioMuse-AI#944). Nothing changes on the wire, and AudioMuse
3.2.0 is still the minimum.

- The catalogue refresh that finalizes an AudioMuse analysis run took 109 s
  on a 35k-song test library, more than twice the watchdog's 45 s tick, and
  AudioMuse refuses to start its own schedules while a plugin task runs.
  Most of that was Lumae's own database work. The check of which profiles a
  publication withdraws looked up each changed track in the new catalogue
  generation separately; that generation is not committed yet, so
  PostgreSQL had no statistics for it and scanned the whole generation for
  every changed track (41-58 s for 5,500 changes). It now reads them in one
  pass. The generation is also written up to 1,000 rows per statement
  instead of one row per statement (about 150,000 statements for a 36k-track
  library). In local tests such a refresh went from 86-103 s to 34 s. A very
  large library (100k+ tracks) still runs over the tick while it refreshes.
- Lumae's other schedules (`catalog_refresh`, `analysis_projection`,
  `provider_identity_recheck`, `collection_retention`) say what they did in
  AudioMuse's task message, like the watchdog, instead of "Plugin task
  completed successfully." They still set no `tracks_analyzed`.
- The scheduler panel on the settings page no longer says AudioMuse labels
  Lumae's tasks "Songs analyzed: 0": older AudioMuse versions show a real
  count, newer ones Lumae's own sentence.

## 1.7.0 (2026-10-12)

SmoothFade and the scheduler. A client can ask for edge profiles in schema
v3, which drops fields Lumae never used and says where a song's music
really ends. The catalogue watchdog now steps aside for AudioMuse, which it
used to starve of its own schedules, holds each tick to about 45 s of work
and says what each tick did. AudioMuse 3.2.0 is still the minimum. Schema v3 is
opt-in: v2 transfers are unchanged, and the scheduler changes nothing on
the wire.

- Edge profile schema v3 (SF.3). A client that asks for it (`edge_schema=3`
  on `/api/profiles`, bootstrap and changes, or `edge_schema: 3` when it
  creates a profile session; health lists `edge_profiles.schema_versions:
  [2, 3]`) gets the stored edge without the windows' sample peak and spectral
  flux, with its own digest. When a song's music ends before the last 30
  seconds (a hidden track after a long silence, or a long silent run-out),
  the v3 edge also says where (`landmarks.music_end_frame`) and carries an
  `ending` window measured over the 30 seconds before it, so Lumae and Lumae
  Radio end the song there and fade on its real ending. The analyzer keeps
  the whole file's level series and decodes a second time only for those
  songs (about 0.1% of a library). v2 transfers, v2 digests and the stored
  payload are unchanged, so 1.6.1 still reads the database after a
  downgrade. The upgrade gives every stored edge its v3 digest in short
  batches and flags tails that open on silence for the library pass to
  measure again; schema 3 is offered once every edge has its digest. Details
  in `plugins/LumaeAnalysis/EDGE_PROFILES.md` and the contract's §4.5.
- The watchdog yields to AudioMuse. While Lumae had background work it
  ticked every minute, and AudioMuse queued it milliseconds before its own
  retry pass, so a schedule AudioMuse refused while a Lumae task ran (its
  nightly analysis, clustering, Lumae's own identity recheck and retention)
  never got another chance and failed after 240 minutes ("Blocked for over
  240 minutes; not run"). Now, while AudioMuse has a refused schedule
  waiting to be retried or one of its main jobs is running, the tick does
  nothing, and the next one comes at a random minute 5–15 minutes later
  (every 5 minutes on AudioMuse before 3.3.0, which does not retry), so it
  never meets AudioMuse's retry pass systematically. Lumae resumes once
  AudioMuse's work has started or finished. Work that arrives meanwhile (an
  analysed song, a request from the app) and a restart wait for that tick
  too.
- Minute cadence leaves out the minutes at which Lumae's other schedules are
  due (with the defaults :02, :23 and :32), so the watchdog no longer blocks
  its own identity recheck and retention.
- A tick works for at most 45 s, over as many actions as fit, instead of one
  action of any length. The waveform batch, the edge pass, the relationship
  build and credits and MusicBrainz lookups each stop at the tick's end; a
  credits album resumes from the response cache at the next tick. The edge
  pass no longer runs past its 30 s (production averaged 81 s), a
  relationship step gets about 40 s instead of 20 s per checkpoint reload,
  and the stored-edge v3 upgrade takes one turn of at most 20 s a tick,
  right after on-demand work.
- The edge pass finds its next tracks by walking the library's key instead
  of scanning every job per track: 639 → 125 ms on a 36k-track library
  (this query was behind ticks that lasted up to eight minutes).
- A finished waveform backfill whose remaining tracks wait for a retry keeps
  that retry time. Before, the watchdog stayed on minute cadence after the
  backfill was done and the edge pass behind it never ran.
- Credits take their turn 300 s after the previous turn ended, not after it
  started.
- With several media servers, AudioMuse calls the watchdog once per server;
  the first call now does the work for every source and the others return
  at once.
- AudioMuse shows what each tick did: its message is one sentence ("Lumae: 7
  waveforms, 2 credits albums in 44 s; 41,200 tracks left", "Lumae: yielded
  to analysis", "Lumae: up to date"), and the "Songs analyzed" note counts
  waveforms, edges and on-demand tracks instead of always 0. Each tick also
  writes one INFO line, credits, MusicBrainz lookups, on-demand work and the
  v3 upgrade are journaled, and the journal keeps a day of events (25 to
  1,000 per source) instead of the last 25.
- Disabling `plugin.lumae_analysis.catalog_reconcile` or
  `plugin.lumae_analysis.provider_identity_recheck` on AudioMuse's Scheduled
  Tasks page now lasts; before, Lumae re-enabled both. The settings page says
  when either is disabled and what waits while it is.
- During an AudioMuse analysis run, the per-song hook no longer queues one
  edge task per analysed song. The host admits those tasks from inside its
  own run, so a 2,300-album pass put hundreds on the queue, and AudioMuse's
  analysis page, which shows the newest top-level task, kept switching from
  the run to a queued "Plugin task queued." edge task. The hook now makes the
  library pass due and, when the pass is already past the song, sends it back
  to the start. The pass makes the edge once the reconcile watchdog runs
  again; the host holds plugin crons until its own run ends. Nothing on the
  wire changes.
- The plugin's own waveform backfill no longer queues one edge task per
  track either: those edges come from the library pass too. With several
  waveform batches per tick, a task per track kept a Lumae task on
  AudioMuse's queue about 85% of the time on a library mid-backfill (about
  10% on 1.6), and AudioMuse refused its schedules meanwhile. On such a
  library background edges now follow once its waveforms are done. An
  on-demand request (`/api/profiles/edges/analyze`, or an on-demand
  waveform) still queues its edge at once.

## 1.6.1 (2026-10-07)

Storage. On a 36k-track production library the plugin held 3.0 GB of a
3.7 GB AudioMuse backup, a third of it MusicBrainz responses kept for later.
After the 1.6.1 upgrade the same database backs up to 2.5 GB (plugin
1.8 GB) and is 350 MB smaller on disk. Nothing on the wire changes.

- MusicBrainz responses (full releases with every credit relationship, tens
  of KB to megabytes each) are cached for 3 days instead of 30, enough for
  one album's deferrals and retries; the credits themselves are published
  and kept as before. Expired responses are deleted at once (up to 1.6.0, a
  week after expiry) by the credits watchdog even while every album is
  current, and the upgrade deletes those an older version cached for longer
  (on that library 13,624 of 14,722 rows: 1.1 GB less in the backup,
  319 → 24 MB on disk).
- Profile journal rows written before 1.3.0's edge references still carried
  their own copy of the edge, about 20 KB each, until compaction reached them,
  which a stable library may not do for months. The upgrade rewrites each one
  whose edge is still stored to the reference form, which every reader serves
  byte-identically (contract §3.7). On that library: 4,105 rows, 176 → 104 MB
  on disk, and every one of its 71,429 events read the same before and after.
  A row whose edge was replaced since keeps its copy.
- Credits versions are compacted to two snapshots of the catalogue's albums,
  as the catalogue, analysis and profile journals are, instead of only past
  50,000 versions. A monthly rematch republishes every album, so the old
  limit let them grow for years (about 12 KB each, about 600 MB at the old
  limit). A client further behind (about two months; less when many albums
  are unresolved, which rematch weekly) bootstraps credits again (410
  `bootstrap_required`).

## 1.6.0 (2026-10-06)

Jellyfin. The plugin mirrors a Jellyfin 12.0+ library next to Navidrome,
keeps a moved Jellyfin file's identity through fingerprint rekeys, and fills
in the tags Jellyfin does not expose from the original files. Emby and
Lyrion are gone. Navidrome behaviour is unchanged, and every 1.5.1 fix is
included. Minimum AudioMuse core stays 3.2.0 (per-server Jellyfin access
with `Authorization: MediaBrowser`).

- Emby and Lyrion are gone. Neither was ever admitted as a Lumae catalogue
  source; their dormant catalogue readers and the Emby and Lyrion stream and
  artwork branches of the Living Collections workbench are removed, as is the
  Emby artwork branch of the private FederatedAlbums plugin (its Jellyfin
  artwork now authenticates with the `Authorization: MediaBrowser` header,
  which Jellyfin 12 requires by default). A persisted source of either type
  stays hidden as before: it is never fetched, streamed, rekeyed or deleted.
- Jellyfin catalogues (Jellyfin 12.0 or later; contract §2.1).
  `supported_provider_types` is `["jellyfin", "navidrome"]`. The Jellyfin
  reader walks every selected music library on its own (one scoped `/Items`
  walk per library, tracks and albums), records library membership exactly
  as the Navidrome reader does, requests every field the normalizer reads and
  publishes compact payloads without user data or paths. Artist portraits
  come from `/Artists` and `/Artists/AlbumArtists` per library (their IDs
  equal the tracks' artist IDs; `/Persons` IDs never do). Jellyfin
  MusicBrainz IDs map correctly: `MusicBrainzRecording` is the recording,
  `MusicBrainzTrack` the release track, so the analysis projection's
  suspect-group check reads only `MusicBrainzRecording` (and ISRC) as
  recording evidence for a Jellyfin catalogue: one recording on two releases
  is no conflict. Measured against a Jellyfin 12.2.0
  server: a track's album is its `AlbumId` (its `ParentId` is a folder, a
  disc folder on multi-disc albums), untagged `0001-01-01` dates are treated
  as absent, a library the AudioMuse account may not open (401 "not
  permitted") stops the scan with its own message instead of being read as
  rejected credentials, and the identity probe asks a starting server again
  (503, or a camelCase body) before calling it unreachable. A track's
  `suffix` (its published container, which clients use for decodability and
  download extensions) is the file's own extension (`m4a`, `opus`, ...), not
  Jellyfin's `Container`, which is ffprobe's demuxer list for MP4
  (`mov,mp4,m4a,3gp,3g2,mj2`) and `ogg` for an Opus file; a comma list is
  never published.
- Jellyfin identity guard: `/System/Info/Public` binds the server `Id` to the
  catalogue at the first verified probe; another `Id`, a release before 12.0,
  a product that is not Jellyfin or a changed server type blocks the
  catalogue before any read, and a different server is never adopted as the
  same catalogue. `/api/catalog/health` adds `provider_server_id` and
  `provider_version` per source. A catalogue never changes server type. A
  probe that fails gets 1.5.1's protection: only its error is stored (the
  state, reason and action keep their values, so a block is never lifted
  and a proven move keeps its pending rekey), client admission stays open,
  and the call that saw it fails closed: the refresh never reads an
  unverified server and the projection defers.
- The Living Collections stream and artwork proxies use each catalogue's own
  AudioMuse server credentials instead of the host's global settings (the
  unadmitted Plex branches are gone). A Jellyfin preview streams
  `/Audio/{id}/stream?static=true` (the original file, with byte ranges),
  which unlike `/Items/{id}/Download` works for an account without
  Jellyfin's download permission. Settings, the database-state page and
  catalogue errors name the source's own server type instead of Navidrome.
- Error redaction also masks the quoted token of Jellyfin's
  `Authorization: MediaBrowser Token="..."` header.
- A moved, renamed or re-cased Jellyfin file keeps its identity (contract
  §2.2, `provider_identity_rekey_v2`). A track missing from a complete scan
  is held, published unchanged, for 14 days; when AudioMuse maps a new ID to
  the same content fingerprint as exactly one held ID, the move publishes
  through the existing atomic provider-identity rekey (two identical scans,
  one transaction, manifest), with album and artist rekeys derived only when
  unambiguous: collections, shelves, Vibes, covers and profiles follow it on
  the server, and the journal's rekey events let the app move its own
  ratings and plays. Duplicates never rekey, a track that comes back is no
  rekey, moving files back is a rekey to the original IDs, and an unpaired
  held track is deleted after 14 days. Likely move targets that AudioMuse
  has not analysed yet are held back instead of being published as new.
  Health adds `provider_identity_transition.rekey_contract`; the manifest
  route reports each manifest's own `contract`. Navidrome is unaffected.
- Health never loses an applied provider-identity transition:
  `provider_identity_transition` adds `last_applied_transition_id`,
  `last_applied_contract`, `last_applied_first_seq`, `last_applied_last_seq`,
  `last_applied_manifest_sha256` and `retained_transitions` (the newest 100,
  oldest first), whatever the current state; every v2 manifest names its
  `previous_transition_id` (hashed), so a client can walk the chain.
- Tags from Jellyfin files (contract §2.3). Whenever the plugin holds a
  Jellyfin track's original file (AudioMuse's analysis hook or its own
  profile download) it reads, with mutagen if installed and PyAV otherwise,
  the tags Jellyfin 12 does not expose: release type, compilation, explicit,
  credits with roles (composer, lyricist, producer, engineer, mixer,
  remixer, arranger, conductor, performer with instrument), ISRC, disc
  subtitle, original date, ReplayGain track and album gain and peak, BPM
  and title/album sort names. The refresh publishes them under exactly the
  OpenSubsonic keys the Navidrome reader produces, only when present; a tag
  change is an ordinary upsert. An album sort tag (ALBUMSORT, TSOA, `soal`)
  is the album's canonical `sort_name`, ahead of the `SortName` Jellyfin
  computes for every album. Navidrome is byte-for-byte unaffected.

## 1.5.1 (2026-10-06)

- MusicBrainz lookups (`/api/music_metadata/prepare`) run on the catalogue
  watchdog instead of their own every-minute cron task. That task fired
  1,440 times a day, nearly always on an empty queue, and AudioMuse records
  every firing in its ten-row "Recent tasks" history ("Songs analyzed: 0"),
  so real host tasks dropped out of it within ten minutes. The migration
  deletes the `plugin.lumae_analysis.music_metadata` cron row and the task
  is no longer registered. An accepted lookup wakes the watchdog in the same
  transaction; while a library backfill runs, lookups and backfill batches
  take turns tick by tick, and once background work is done a due lookup
  runs on every tick (one a minute, as before). An idle install
  now shows about three Lumae tasks an hour (the :11 sweep and the :02/:32
  identity recheck). The settings scheduler panel counts pending lookups.
- The watchdog's retry backoff also looks at how far away the soonest retry
  is: it never ticks faster than that retry needs, so a lookup the daily
  MusicBrainz allowance deferred for an hour waits on the hourly sweep
  instead of an empty task every minute. Each cadence still ticks at or
  before the retry is due.
- The "Historical AudioMuse upgrade sequence observed" diagnostic is retired.
  It looked for a finished analysis, then cleaning, then analysis in
  AudioMuse's `task_status`, which never keeps them: every main-task start
  marks the earlier finished tasks REVOKED, and from AudioMuse 3.2.0 (the
  minimum core) each finished task deletes the others. It could only read
  "no". The settings page drops the line; health keeps
  `servers[].v3_readiness.task_evidence` with the same keys, now always
  `diagnostics_available: false` and a new `unavailable_reason:
  "audiomuse_keeps_latest_task_only"`, and no longer queries `task_status`
  for it. Readiness never depended on it.
- A slow or failed Navidrome ping no longer pauses sync (contract §2,
  `GET /api/catalog/health`). The plugin pings Navidrome (5 s timeout) on
  every catalogue health request and before every catalogue refresh and
  analysis projection, to notice an upgrade that changes track IDs. Up to
  1.5.0 one failed ping stored a pending provider-identity transition: health
  told the Lumae app that catalogue and analysis sync were not admitted, and
  every later successful ping kept it pending until the
  `provider_identity_recheck` task refreshed the catalogue, up to 30 minutes
  later. Now a failed ping stores only its error, shown on the settings page
  under Provider identity safety and in health
  `provider_identity_transition.last_error`; state, reason and action keep
  their stored values. The call that saw the failure still fails closed: a
  catalogue refresh inspects the old and new track IDs before it publishes,
  and an analysis projection waits for a verified version. A verified ping
  clears the `provider_version_unverified` / `retry_provider_identity_check`
  values 1.5.0 could leave behind.
- The `analysis_projection` task no longer shows as FAIL when it is held back
  by design (RCA: [docs/analysis-projection-fail-rca-2026-10-06.md](docs/analysis-projection-fail-rca-2026-10-06.md)):
  the provider identity is unverified or mid-transition, AudioMuse's provider
  migration is not ready, the server has no Lumae catalogue (not Navidrome, or
  not prepared yet), or its catalogue has never published. It returns
  `status: "deferred"` with a `reason` and a message for the task list; the
  published projection is kept and the next trigger projects. Raising made
  AudioMuse retry twice and then mark the task FAIL. A projection that loses a
  race with a catalogue refresh still raises, so AudioMuse's retry covers it.
  Catalogue preparation and analysis-run finalization retry as before, and the
  provider-identity recheck keeps its projection request when the projection
  defers.

## 1.5.0 (2026-10-05)

- Custom covers (health `capabilities.covers`, contract §5.6). The Lumae app
  and Lumae Radio keep the cover a person chose for a saved Vibe, a Living
  Collection or a playlist in step through `GET /api/covers/changes` and
  `POST /api/covers/mutations`: an album from inside it, an orb colour or a
  photo. Covers are revisioned records scoped by account and catalogue like
  saved Vibes, kept apart from the Vibe record so a client that rewrites a
  recipe cannot erase them; a delete means "back to the automatic cover".
  Photos go to a small image store (`POST /api/covers/images`, `GET
  /api/covers/image`): JPEG, PNG or WebP up to 512 KB, named by their SHA-256
  so an upload is idempotent, at most 2,000 per scope, served with an
  immutable cache. A cover can name a photo only once it is stored; an image
  no cover uses is deleted after two days. Receipts expire with the Vibe
  receipts, and a provider-identity rekey rewrites album ids inside covers.

## 1.4.0 (2026-10-03)

- Saved Vibe sync (health `capabilities.vibes`, contract §5.5). The Lumae app
  keeps its saved Palette, Mood Compass and DNA Vibes in step across devices
  through `GET /api/vibes/changes` and `POST /api/vibes/mutations`, scoped by
  account and catalogue like Album Shelf and switched on by the same Living
  Collections setting. Each Vibe is one compact record with a revision: a
  write based on an older revision is refused with the current record
  (`vibe_conflict`), so two devices never overwrite each other silently, and
  deletes are tombstones every device learns about. The plugin stores the
  Vibe as the app sends it (at most 64 KB) and never interprets the recipe.
  Receipts replay a lost response and expire after 30 days with the shelf
  receipts; a provider-identity rekey rewrites song ids inside stored Vibes
  without changing their revision.

## 1.3.4 (2026-10-02)

- A retried profile-bootstrap create adopts the snapshot its own earlier
  create captured (K12, health `profile_bootstrap.create_adoption`). The v2
  capture runs inside the create request and grows with the library; at
  about 132k profiles on a home server it outlasted the Lumae app's 10 s
  request, and every K5 retry replaced the finished capture and captured
  again, so the first profile load never completed. Now a retry under the
  same `client_request_id` takes over the ready, never-paged session (or
  waits up to 5 s for its capture, then 503 with `Retry-After`) and answers
  at once. Different create options, a changed source epoch, or a session
  that has served a page keep the 1.3.0 replace behaviour. Adoption counts
  against neither the create rate limit nor the session slots.

## 1.3.3 (2026-10-02)

- Artist portraits in the catalogue. A Navidrome scan calls `getArtists` once
  per selected music folder and publishes each album artist's `coverArt` as
  `artists.cover_art_id`, so the Lumae app shows catalogue portraits instead
  of looking up every artist from each phone. The art ID is part of the
  artist fingerprint, so a changed portrait republishes the artist (the first
  scan republishes every artist that has one). An empty `coverArt` (Navidrome
  0.64+: no image) clears it; guests, which `getArtists` does not list, and a
  failed lookup keep the published value. A failed `getArtists` never fails
  the scan.

## 1.3.2 (2026-10-01)

- Saved (`deferred`) on-demand requests are served within about a minute on an
  otherwise idle server. A deferral wakes the reconcile watchdog, and its
  schedule counts deferred work as ready until it is served. In 1.3.0 and 1.3.1
  they waited for the next idle (hourly) tick when no batch or AudioMuse
  analysis was running (seen on the media host: 261 deferred edge requests).

## 1.3.1 (2026-09-30)

- The edge library pass starts within a minute when a worker first reports a
  qualified runtime or a catalogue refresh has changes. In 1.3.0 a fresh
  install or upgrade waited for the watchdog's next idle (hourly) tick.
- A worker's periodic runtime report (every 10 minutes) no longer re-arms a
  finished pass, so the six-hour re-sweep delay holds.

## 1.3.0 (2026-09-30)

Released with user approval (`docs/plan/LUMAE_FINAL_PLAN_2026-09-24.md`,
P4-1). No 1.2.5 process may keep running against an upgraded database — follow
[`docs/runbooks/UPGRADE_1.3.md`](docs/runbooks/UPGRADE_1.3.md). Work-package-level
progress and test evidence are in [`docs/STATUS.md`](docs/STATUS.md); the
precise wire changes (K1–K11) are in
[`docs/contracts/LUMAE_SYNC_CONTRACT.md`](docs/contracts/LUMAE_SYNC_CONTRACT.md)
§7. Summary:

- Health `capabilities.edge_profiles` adds `served: true` and a live `stored: bool|null`, so clients can tell whether profile transfers carry edges; `available`/`enabled` only describe the answering process's PyAV runtime (contract §2).
- **Edge profiles work on a stock AudioMuse worker.** The exact PyAV 16.1.0 pin is replaced by runtime self-qualification against bundled reference measurements (PyAV >= 16; AudioMuse's 17.1.0 is bit-identical). Workers report their runtime, and health's new `analyzable` says whether queued edge jobs will be computed; the web process no longer needs PyAV to queue them.
- **On-demand first, library in the background, for waveforms and edges.** The server walks the library for edges at low priority from the reconcile watchdog (`server_backfill`). On-demand edge requests promote tracks a background batch holds; an on-demand waveform's edge follows at on-demand priority. A request AudioMuse refuses because another task runs is saved and served by the next worker task (`deferred`) instead of failing. Background batches serve that work between tracks and yield after a budget. Unmeasurable media is `unsupported` and not retried. Credits and metadata yield to on-demand work again. `GET /api/profiles?edge_status=1` reports per-track edge state (contract §2, §3.2, §3.6).
- **Vectors after a projection rebuild:** a request for a pruned analysis generation is served from the current one with checksums and an explicit `missing` list, never a silently empty page; `strict_generation` returns 410 `generation_expired` (contract §3.8).

- **Transport:** gzip for JSON responses of 1 KiB or more (K1).
- **v2 profile bootstrap:** snapshot pages store an edge *reference* instead
  of a copy, resolved at read time (K2); sessions get a sliding expiry
  (K3); health reports a truthful, cached `available` probe and a new
  `auth_enabled` field alongside the unchanged `auth` string (K4); session
  create is idempotent per `client_request_id` (K5).
- **Edge references in the profile stream:** an opt-in (`edge_refs`) lets a
  waveform-only republish skip re-sending an unchanged edge profile (K6,
  P3-2) — the change that makes the LUM-005 loudness-analyzer upgrade (K11)
  affordable to roll out.
- **Collections:** the feed adds `epoch`/`head_seq`/`has_more` and a
  one-shot, consistent snapshot route (K8); a stricter opt-in conflict
  contract replaces silent id remaps with explicit 409 responses (K9);
  collection items carry an explicit `catalog_instance_id` (K10, LUM-013).
- **Diagnostics and readiness:** a health `integrity` block
  (unpublished/orphaned profile counts, fence installation, collections feed
  health); stored error text shown through redaction (LUM-021); per-source
  readiness panels with scoped retries on the settings page (LUM-017).
- **Execution limits:** each waveform/edge analysis runs with a killable
  wall-clock limit, since AudioMuse's own task queue enforces none
  (LUM-018).
- **Retry hardening:** stale catalogue/profile transitions no longer strand
  a retryable profile, and pause or legacy-migration releases no longer
  burn a retry attempt (LUM-007).
- **Correctness:** the `profile_stream_state` publication lock is now
  exercised by a concurrency test that fails if `FOR UPDATE` is removed
  (LUM-001, P3-13); orphan withdrawal and ready-repair gaps closed
  (LUM-008).

## 1.2.5

Set `fully_verified` to `False` when a provider-identity transition blocks
catalogue sync, to preserve the progressive-readiness contract.

## 1.2.4

Verify release type and complete single/EP recording membership, so Lumae's
duplicate-safe recommendations do not merge distinct releases.

## 1.2.3 — Discovery qualification

Album memories now retain their artist context. Explicit enjoyment is
independent of recognition; "new to me" and "enjoyed" can both be true.
Health advertises `album_memory_context` and `enjoyment_feedback`; older
mobile clients and existing schema-v1 records remain compatible.

## 1.2.2 — Personal discovery sync

Added catalogue-independent Want Shelf, memory, feedback, Rest, and
introduction-provenance synchronization, plus background MusicBrainz checks
for external artists, release groups, editions and recordings. Existing
shelf v1 and credits APIs were unchanged. This was (and remains) server
support only; the corresponding mobile data and recommendation features
live in the app. See
[docs/discovery-api-v1.md](docs/discovery-api-v1.md) for the full contract.

## 1.2.1 — Radio DJ retired

Removed the retired Radio DJ analysis, its routes, worker registrations and
dependencies. The install migration drops the legacy DJ tables and DJ cron
schedules on upgrade while preserving catalogue, loudness/edge profiles,
collections, credits and relationship data. Old dedicated DJ workers had to
be stopped before upgrading past this release; running one against a 1.2.1+
database was never supported.

## 1.2.0 — Personal Shelves, credits, and DJ analysis (DJ era)

Added Personal Shelves synchronization and insights, MusicBrainz credits
enrichment, resumable album/artist relationship builds, source-bound edge
profiles, and **opt-in DJ analysis** with interrupted-work recovery and
bounded queue deferral. It carried forward the Navidrome identity guard from
1.1.9.

DJ analysis was disabled by default and required a compatible dedicated
worker; publishing this plugin never installed or enabled that worker on its
own. Credits matching remained subject to its configured audit gate. Friend
Album Discovery (`plugins/FederatedAlbums/`) remained excluded from the
public catalogue, as it still is today.

Radio DJ was removed in 1.2.1 (above); nothing from this era is part of the
plugin's current capabilities.

## 1.1.9 — Provider-identity reconciliation fix

Trusted pre-canonical Navidrome releases now publish ordinary track removals
and music-library scope changes through the normal catalogue diff, instead
of misclassifying them as incomplete provider-ID migrations. Exact identity
inspection stayed fail-closed for pending, uncertain or blocked canonical-ID
transitions.

## 1.1.8 — Adaptive reconciliation

The catalogue reconciler stopped running every minute while idle: it now
runs every minute only when durable work is ready, every five minutes while
an AudioMuse analysis parent is running, uses 1/5/15/60-minute retry
backoff, and falls back to an hourly safety sweep. The settings page started
reporting the real action, phase, duration, retry state and a bounded
journal of meaningful work, refreshed in the background without a page
reload.

## 1.1.7 — AudioMuse 3.4 queue compatibility

Removed all imports of AudioMuse's private RQ queues, jobs, dependencies and
retry objects, replacing task-within-task queueing with a database-driven,
one-action reconciliation watchdog. This recovered stranded analysis runs
and made profile, relationship, catalogue and provider follow-ups
restart-safe, restoring compatibility with AudioMuse 3.4.

## 1.1.6 — 1.1.0

Earlier fixes to provider-identity transition detection, catalogue-refresh
locking, and durable projection/migration recovery. See
`plugins/LumaeAnalysis/plugin.json` for the exact per-version changelog
text; none of it changes current behaviour beyond what
`docs/contracts/LUMAE_SYNC_CONTRACT.md` already describes.

## 1.0.1 — Resource safety

Bounded waveform decoding and filtering memory to decoder blocks instead of
letting it grow with a file's duration and sample rate; capped every
background job and background batch size; limited backfill and catalogue
query batches; replaced all-pairs relationship comparison with bounded IVF
shortlists (falling back to the last published generation while the index
warms up); and added an administrator maintenance pause that stops new
catalogue/projection/waveform/relationship work without deleting or hiding
already-published data.

## 1.0.0 and earlier

Replaced release-number approval with live API and data-contract admission
(1.0.0); introduced the provider-authoritative catalogue, Living
Collections, Personal Shelves-precursor collection backup/restore, and the
original waveform-only Lumae Analysis plugin. Full per-version text is in
`plugins/LumaeAnalysis/plugin.json`.
