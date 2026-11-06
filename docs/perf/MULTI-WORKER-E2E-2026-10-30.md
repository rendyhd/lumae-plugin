# Multi-worker runners end to end, 2026-10-30

`worker_runners: every` (issue #10, [plan](../plan/LUMAE_MULTI_WORKER_PLAN_2026-10-30.md))
on a stock AudioMuse with three worker containers, against `one` on the same stack. The
first build analysed each track once and finished 4.7 times faster, but the run found four
defects, fixed before the release:

1. Every AudioMuse worker container starts **two** worker runners (task queues `default`
   and `high`). Both helped, so a container analysed twice the files it was sized for.
2. A runner that found the waveform pass's tracks all taken went on to the loudness pass,
   which downloaded **9 of 120** tracks the waveform pass downloaded again.
3. The waveform tracks a killed runner had admitted waited **an hour** as stale attempts,
   and the waveform pass the killed leader had claimed waited 30 minutes more.
4. A helper tried the lock only between five-minute turns: the role went **four minutes**
   without a leader.

With the fixes, three containers (one runner each) finished 3.1 times faster than one,
downloaded every file once, and a run with an AudioMuse analysis, a cancelled analysis, a
killed helper container and a killed leader container ended with every track published.

## Setup

| | |
|---|---|
| Host | AudioMuse-AI v3.6.3 by digest (`sha256:b91af8d0…`), one web container and three worker containers, PostgreSQL 15, Navidrome 0.64.2 (debug log: one `Streaming RAW file` line per download) |
| Library | 120 generated FLACs, 12 albums of 10, 150–210 s each |
| Machine | The PC, 20 cores; the plugin mounted from the branch |
| Settings | `background_workers: 1` (one file at a time per runner, so the runner count is what varies), `edge_profiles_enabled: true`, `max_files_in_flight: 16` |
| Each run | A new AudioMuse database, `lumae-install register`, the settings, then the workers; timing from the first to the last waveform published |

## Results

| Run | Code | Runners working | 120 tracks in | Downloads | Left behind |
|---|---|---|---|---|---|
| `one` (1.9 behaviour) | branch | 1 | 446 s | 120 × 1 | nothing |
| `every`, first build | `beaa362` | 6 (2 per container) | 94 s | 111 × 1, 9 × 2 | nothing |
| `every` | final | 3 (1 per container) | 144 s | 120 × 1 | nothing |
| `every`, second timing | final | 3 | 143 s | 120 × 1 | nothing |

In every run the waveform pass also published the edge and loudness of each track (120 of
each), the edge and loudness passes then processed nothing, and `pass_batches` and
`analysis_slots` were empty at the end. The 9 double downloads of the first build were the
loudness pass's (`processed` 9), on tracks whose waveform was not yet published.

With `every` the waveform pass can end `queued` with a retry an hour later instead of
`complete`: a helper's last admission came after the leader had completed the pass, and
admission arms that wake for an abandoned attempt (1.9 does the same after an on-demand
admission). The leader then runs the pass once more, finds nothing and completes it.

## Resilience

The scenario: at 15 tracks start an AudioMuse analysis (one album), cancel it
40 s later, kill a helper container 30 s after that, and the leader's container a minute
later.

| Check | Final code |
|---|---|
| Step aside for an analysis | Pass. All three runners `waiting_host audiomuse:main_analysis` within 13 s (each finished its file in flight first). Meanwhile AudioMuse's own song hook published the waveforms of the songs it analysed. |
| Cancelling the analysis | It killed the worker process that ran the job, which was also a runner (the leader here). Its container's other runner took its host's lock and the role's lead within about 25 s. |
| Helper container killed | Its waveform attempts went back at the next turn of a live runner once its heartbeat was gone (90 s); its last file was downloaded again. |
| Leader container killed | A helper led about 65 s later and queued the waveform pass the dead leader had claimed. |
| End state | 120 waveforms, 120 edges and 120 loudness rows; every pass complete; no batch and no slot left; 117 files downloaded once and 3 twice (one in flight on each runner killed). Done 2.6 minutes after the leader was killed. |

Before fixes 3 and 4 the same scenario ended with 6 tracks pending (an hour's wait) and,
with fix 3 alone, 3 tracks released but untouched behind the dead leader's claim.

## Fixes

| Defect | Fix |
|---|---|
| Two runners per container | With `every` a runner first takes its host's advisory lock (role and host name); a helper also stands by while the leader is on its host. A runner that stops leading clears its row's `leader`. |
| Later passes downloaded what the waveform pass would | A source's edge and loudness passes wait while its waveform pass has a track due or in flight (admitted within a turn and a file's time limit), and its loudness pass while edge batches are out; the runner comes back in 30 s. |
| A killed runner's waveform tracks waited an hour | Each waveform batch's attempts are recorded with their tokens in `pass_batches` and forgotten when the slice ends; any live runner releases a gone runner's (uncounted, due at once) and queues the pass. A new leader queues the waveform pass its predecessor claimed. |
| Four minutes without a leader | A helper's turn lasts at most a minute; it tries the lock between turns. |

## Not covered

- A GPU worker next to CPU workers, and workers on separate machines: the containers here
  share one PC. Each runner sizes its own K, so a mixed cluster is not expected to differ.
- `background_workers` above 1 with several runners, and the `max_files_in_flight` cap
  under load: covered by the tests, not measured here.
- The owner's server.
