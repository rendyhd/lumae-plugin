# Multi-worker runners on the owner's server, 2026-10-31

The 1.10.0 build (`worker_runners`, issue #10, [plan](../plan/LUMAE_MULTI_WORKER_PLAN_2026-10-30.md),
[first end-to-end run](MULTI-WORKER-E2E-2026-10-30.md)) installed on the owner's AudioMuse and
timed on four sets of 300 real tracks. With `every` the three worker containers analysed each
set 1.7 times as fast as one worker and downloaded each file once. The run found one defect: the
leader kept its share of the files in flight through a relationship build, which analyses no
file. It is fixed in the re-cut, which was then timed again and run through a helper restart
and a killed leader.

## Setup

| | |
|---|---|
| Host | AudioMuse-AI `devel`: Flask, PostgreSQL 15 and Redis on the media host (a VM, 8 cores, 16 GB, shared with Plex, Jellyfin and Frigate); three worker containers (`devel-nvidia`) on the PC (20 cores, 62.7 GB), sharing one plugin volume |
| Music | Navidrome on the media host, FLAC on an SMB share |
| Library | 36,752 songs, 37,912 published waveform profiles; every pass complete before the run |
| Before | LumaeAnalysis 1.9.0 (`f6756477…`), `background_workers: 8`, `heavy_work: auto`. Its code is kept as `lumae_analysis.before_1.10.0_20261031` on both plugin volumes |
| Install | Workers stopped; `install_package` in the Flask container, as the private deploys did (no source URL), 3 s including the migrations; Flask restarted (web down about 6 s); the code written to the worker volume by the host's own `_materialize_one`; workers started. AudioMuse reported `load_status ok` |
| Work | Four disjoint sets of 300 tracks (published, 1–10 minutes, with edge and loudness). Each set's waveform rows were set to analyzer version 0, as an analyzer upgrade leaves them, and the waveform pass queued as an admission does. The published profiles stayed served throughout |
| Measured | From queueing to the last track ready again; downloads from Navidrome's `Streaming file` lines by the plugin's user |

## Results

| Run | Build | Runners | Files at once | 300 tracks in | Downloads |
|---|---|---|---|---|---|
| A `one` | first cut (`5f16924c`) | 1 | 8 | 700 s | 300 × 1 |
| B `every` | first cut | 3, two at a time (the cap) | 16; 8 while the leader built relationships | 455 s | 300 × 1 |
| C `every` | re-cut (`3f43c682`) | 3, two at a time | 16 | 415 s | 300 × 1 |
| D `every`, a helper restarted and the leader killed | re-cut | 3 | 16 | 460 s | 284 × 1, 16 × 2 |

After each run the edge and loudness passes had nothing to do, and no batch or slot was left.
All 1,200 tracks ended ready on analyzer version 1, and the profile status counts matched the
start (ready 37,912, stale 4,297, skipped 121, failed 102).

All three containers share the PC's 20 cores. At 16 files at once they used about 15 cores
(about 6.7 at 8), and a file's median time went from 12.5 s (A) to 17 s (C). On one machine,
raising `background_workers` adds files at once in the same way (not measured here); `every`
adds capacity when the worker containers are on different machines.

After B and C the waveform pass showed `queued` for a while, as the first run's report
describes: the helpers' last admission came after the leader completed it. After D it was
`complete`.

## The defect

A leader with `every` took its K slots at the start of each turn and kept them until the turn
ended. In B a turn began with the relationship rebuild that 1.9.2 asks for after an upgrade.
That turn held 8 of the 16 slots for the last 2.5 minutes of B, and then for 18 minutes more
until the workers were stopped. Meanwhile the third container waited at the cap. B ran at 0.73
tracks a second before the build and 0.58 during it; C held 0.72–0.74 throughout.

The fix (`52abbbb`): the leader takes up to K slots before each waveform, edge or loudness pass
and gives them back when the pass returns. With none free it leaves the passes to the helpers,
as before. On-demand tracks never used the turn's analysis threads. In C the slots were full in
69 of 75 samples, and the leader held none while it had no pass. Two tests cover it (both fail
on the first cut): `test_a_leader_holds_analysis_slots_only_while_a_per_track_pass_runs` and
`test_a_leader_whose_helpers_hold_every_slot_leaves_the_passes_to_them`.

## Resilience (run D)

| Event | Result |
|---|---|
| Helper container restarted (`docker restart -t 30`) at 64 tracks | Back in 4 s. Its 8 files in flight were downloaded again 100 s later, once its heartbeat was 90 s old |
| Leader container killed at 135 tracks, started 21 s later | The restarted container's runner led 56 s after the kill. Its 8 files in flight were downloaded again 97 s later |
| End | 300 ready, the pass complete, no batch and no slot left; 16 files downloaded twice, the 8 in flight on each container that went away |

A container stopped cleanly does not hand its batch back: its files wait for the heartbeat like a
killed one's. That is not changed in this build.

## Also seen

- The upgrade from 1.9.0 includes 1.9.2's relationship rebuild. The leader started it at 12:50
  and was scoring (849 of 1,094 artists) when the workers were stopped at 13:08 for the re-cut.
  As 1.9.2 intends, it resumes from its checkpoint on the app's next similar-albums request;
  generation 65 is served until then.
- Seven streams by the plugin's user during A were other tracks, about 30 s apart: playback,
  not the pass.

## Not covered

- Worker containers on separate machines, and a GPU worker next to CPU workers.
- The settings page on this server, which needs the owner's login. It was checked on the stock
  host in the first run.
- An AudioMuse analysis during a pass, covered in the first run.

The server was left on the re-cut with `worker_runners: every`. Setting Workers back to one
worker restores the 1.9 behaviour without a reinstall. Going back to 1.9.0's code
(`dist/lumae_analysis/lumae_analysis_1.9.0.zip`, `f6756477…`, also kept on both volumes) was not
tried.
