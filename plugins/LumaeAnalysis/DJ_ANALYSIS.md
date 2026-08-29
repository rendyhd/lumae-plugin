# Optional DJ analysis worker

Status: implemented behind an unavailable-by-default capability; reference-host
and musical qualification are still required.

## Explicit installation

The ordinary Lumae Analysis plugin does not install PyTorch or a model. An
administrator must install the exact packages in `requirements-dj.txt`, review
`BEAT_THIS_LICENSE.txt` and the upstream training-data caveat, then run:

```bash
python -m pip install torch==2.6.0 torchaudio==2.6.0 \
  --index-url https://download.pytorch.org/whl/cpu
python -m pip install -r requirements-dj.txt
python -m plugins.LumaeAnalysis.provision_dj_model \
  --output /var/lib/audiomuse/lumae-models/beat-this-final0.ckpt \
  --acknowledgement "I reviewed the Beat This license and training-data caveat"
```

The worker accepts the official `2.6.0+cpu` wheel metadata variant and rejects
CUDA/ROCm builds. The package index is an explicit administrator choice and is
never contacted by an API or playback request.

Configure that absolute local path as `dj_model_path` and explicitly enable
`dj_analysis_enabled`. API/playback requests cannot download or select a model.
The worker accepts only Beat This 1.1.0 `final0`: 81,058,141 bytes, SHA-256
`8c328b45f59d8dd3dff219253ff6a8d6482be57d0133a29140e2febbf8eb8331`.

Beat This code and published weights declare MIT licensing. Upstream also warns
that some training audio is copyrighted or has limited Creative Commons terms;
the MIT declaration is not a legal assessment for every deployment or library.

## Worker contract

- one durable job globally, 1 GiB process RSS cap, 15-minute deadline and
  30-minute source-duration cap;
- source files are acquired inside the background job and bound by opaque media
  revision plus decoded-representation SHA-256;
- streaming PyAV decode into a disposable disk spool, exact centered log-mel
  context, and bounded 1,500-frame inference windows with upstream's six-frame
  padding and keep-first ownership; no whole-track waveform is held in RAM;
- resumable database state, progress, cancellation and distinct unsupported
  versus failed outcomes;
- uncalibrated logits remain labeled as logits. Raw downbeat alignment is tested
  before snapping can be used;
- stable 4/4 regions need at least 32 beats, four beat intervals per bar,
  interval CV at most .06 and raw downbeat alignment within 50 ms;
- half/double-tempo ambiguity is rejected; 8-bar novelty boundaries over 2.5
  local standard deviations are capped at eight entries and eight exits;
- unqualified key metadata never authorizes pitch shifting. Natural beginning
  and EOF remain available independently of structural candidates.

Ordinary profile work retains priority. Missing/unavailable DJ analysis does not
affect EdgeProfileV1, recommendation membership or normal SmoothFade.

## Qualification still required

Register a corpus and run the worker on an Intel N100, 16 GiB RAM, CPU-only
Linux x86-64 host. Record OS, Python, package/model hashes, source duration,
wall time, peak RSS and region eligibility for every item. The gate is p95 wall
time no greater than source duration, with RSS below 1 GiB and no deadline or
duration limit violations. Until that report exists, health must keep
`reference_host_qualified=false` and the app must not advertise DJ playback.
