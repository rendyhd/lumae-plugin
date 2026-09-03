# Optional DJ Analysis V2 worker

Status: DJ Analysis V2 is implemented behind an unavailable-by-default private
prerelease capability. Reference-host, calibration, corpus, listening, and device
qualification are still required before the app may execute a DJ transition.

## Explicit opt-in

The ordinary Lumae Analysis plugin does not bundle PyTorch, Beat This, YAMNet, or
their model artifacts. A DJ-capable worker image installs the exact packages in
`requirements-dj.txt`, but it still contains no model. DJ analysis defaults off.
While it is off, plugin startup, health checks, app requests, playback requests,
and scheduled tasks neither download nor load either model.

An administrator reviews `BEAT_THIS_LICENSE.txt` and `YAMNET_NOTICE.md`, checks
the combined acknowledgment, and enables **DJ analysis** on the plugin settings
page. Until both controls are set, no setup job is queued. The worker downloads
the 77.3 MiB Beat This checkpoint and 3.9 MiB official YAMNet Lite artifact,
resumes bounded partial downloads, and verifies the pinned byte lengths and
SHA-256 digests before loading either model. Capability moves through `disabled`,
`downloading`, `initializing`, `ready`, or `error`.

Turning DJ analysis off immediately prevents model loading and new DJ work. The
separate removal action deletes the two configured model files and partials but
does not delete normal analysis, synced music, or existing DJ database rows.

The equivalent administrator-run setup command remains available for controlled
deployments:

```bash
python -m pip install torch==2.6.0 torchaudio==2.6.0 \
  --index-url https://download.pytorch.org/whl/cpu
python -m pip install -r requirements-dj.txt
python -m plugins.LumaeAnalysis.provision_dj_model \
  --beat-this-output /var/lib/audiomuse/lumae-models/beat-this-final0.ckpt \
  --yamnet-output /var/lib/audiomuse/lumae-models/yamnet-classification-tflite-1.tflite \
  --acknowledgement "I reviewed the Beat This license and the YAMNet/AudioSet training-data and calibration caveats"
```

The worker accepts the official `2.6.0+cpu` wheel metadata variant and rejects
CUDA/ROCm builds. The package index is an explicit administrator choice and is
never contacted by an API or playback request. The settings action and explicit
command are the only model-provisioning paths.

The worker accepts only:

- Beat This 1.1.0 `final0`: 81,058,141 bytes, SHA-256
  `8c328b45f59d8dd3dff219253ff6a8d6482be57d0133a29140e2febbf8eb8331`.
- Official YAMNet Lite v1: 4,126,810 bytes, SHA-256
  `10c95ea3eb9a7bb4cb8bddf6feb023250381008177ac162ce169694d05c317de`.

Beat This code and published weights declare MIT licensing. Upstream also warns
that some training audio is copyrighted or has limited Creative Commons terms;
the MIT declaration is not a legal assessment for every deployment or library.
See `YAMNET_NOTICE.md` for the separate YAMNet and AudioSet caveats.

## Worker contract

- One durable job globally, 1 GiB process RSS cap, 15-minute deadline, and
  30-minute source-duration cap.
- Source files are acquired inside the background job and bound by opaque media
  revision plus decoded-representation SHA-256.
- Beat This uses streaming PyAV decode into a disposable disk spool, exact
  centered log-mel context, and bounded 1,500-frame inference windows with the
  upstream six-frame padding and keep-first ownership. No whole-track waveform
  is held in RAM.
- YAMNet evaluates deterministic 15,600-sample windows at 16 kHz and publishes
  selected vocal-class evidence only. Raw scores remain explicitly uncalibrated,
  are not called probabilities, and do not authorize cuts.
- Low, mid, and high log-mel energy is reduced into bounded bar windows before
  publication; frame-rate band arrays are never sent to a mobile client.
- Database state, progress, cancellation, and unsupported versus failed outcomes
  are durable and resumable.
- Raw downbeat alignment is tested before snapping can be used.
- Stable 4/4 regions need at least 32 beats, four beat intervals per bar,
  interval CV at most .06, and raw downbeat alignment within 50 ms.
- Half/double-tempo ambiguity is rejected. Eight-bar novelty boundaries over 2.5
  local standard deviations are capped at eight entries and eight exits.
- Unqualified key metadata never authorizes pitch shifting. Natural beginning
  and EOF remain available independently of structural candidates.

Ordinary profile work retains priority. Missing or unavailable DJ analysis does
not affect EdgeProfileV2, recommendation membership, normal sync, or SmoothFade.

## Vocal-risk calibration

Structural cuts remain locked until an administrator supplies a reviewed
`lumae-yamnet-vocal-risk-isotonic-v2` artifact through
`LUMAE_DJ_VOCAL_CALIBRATION`. Build it from JSONL labels with:

```bash
python scripts/build_vocal_calibration.py \
  --input vocal-conflict-labels.jsonl \
  --output yamnet-vocal-calibration-v1.json \
  --reviewed --authorize-cuts \
  --acknowledgement "I reviewed the track-disjoint vocal-conflict labels and holdout metrics"
```

Each JSONL row contains `track_id`, `split` (`calibration` or `holdout`),
`position_ms`, `raw_vocal_evidence`, and binary `vocal_conflict`. The builder
requires at least 100 calibration tracks and 200 disjoint holdout tracks, both
positive and negative labels in each split, a monotonic isotonic mapping, and
fixed holdout gates for Brier score, expected calibration error, and unsafe
false negatives at the cut threshold. The artifact binds to the exact YAMNet
and class-map hashes and has its own digest. Installing or replacing it changes
the DJ analysis cache key, so uncalibrated or differently calibrated rows are
recomputed. An invalid configured artifact makes the worker fail closed.

For an isolated listening build, `--qualification-tier private-audition`
accepts the smaller exploratory gate: at least 10 calibration tracks, 10
disjoint holdout tracks, and five positive plus five negative reviewed frames
in each split. The holdout metric limits do not change. This tier reports
`release_authorized=false` and becomes internally eligible only when the worker
runs a `1.2.0-djtest.N` package with `LUMAE_DJ_PRIVATE_AUDITION=1`.

Create its disposable review pack from 20 locally acquired tracks and their
uncalibrated `vocal_risk.frames` with:

```bash
python scripts/build_private_vocal_review_pack.py \
  --input private-vocal-sources.jsonl \
  --output-dir private-vocal-review
```

The input uses local paths only for bounded ffmpeg extraction; paths are not
written to the pack. The page presents one high-evidence and one low-evidence
six-second clip per track, hides the model score during review, and exports the
existing label JSONL. If uncertain answers leave either class short, rerun with
`--review-state review-state.json`; replacements are selected only from the
same fixed tracks and split. Generated clips and review files are audition
artifacts and must not be committed or treated as release evidence.

## Private prerelease packaging

DJ development is not added to the public `plugin.json`, official catalog, or
`latest` release. Build a separate channel with:

```bash
python scripts/build_private_dj_prerelease.py \
  --version 1.2.0-djtest.1 \
  --base-url https://private.example/lumae/djtest.1
```

The builder creates a code-only zip, private `plugin.json`, and private
`manifest.json`. It injects the prerelease version into the copied zip only and
refuses any version outside `1.2.0-djtest.N`; the official source metadata and
official 1.1.8 artifact remain unchanged.

## Qualification still required

Register a corpus and run the worker on the intended CPU-only host. Record OS,
Python, package/model hashes, source duration, wall time, peak RSS, and region
eligibility for every item. The host gate is p95 wall time no greater than source
duration, with RSS below 1 GiB and no deadline or duration-limit violations.

This gate alone is insufficient. Vocal-risk calibration, transition corpus,
listening, compatibility, and physical-device gates still apply. Until reviewed
qualification artifacts exist, health keeps `reference_host_qualified=false` and
the app must not advertise DJ playback.
