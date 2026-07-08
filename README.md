# Lumae AudioMuse-AI Plugin Catalog

This repository publishes the Lumae Analysis plugin for **AudioMuse-AI**.

The catalog is exposed through `manifest.json`. AudioMuse-AI reads that catalog, follows the Lumae `pluginUrl`, downloads the versioned code-only zip from `dist/lumae_analysis/`, and verifies the published checksum.

## Plugin

Lumae Analysis precomputes loudness and MixRamp profiles server-side so Lumae can use volume normalization and SmoothFade without doing that work on the phone.

The plugin provides:

* a health endpoint for app setup checks;
* profile read/request API endpoints for the Lumae app;
* an analysis hook that reuses AudioMuse's per-track analysis audio;
* a catch-up settings page for existing libraries;
* a Queue Whole Library action that chunks large libraries into worker jobs.

## Layout

* `manifest.json` - the AudioMuse plugin catalog.
* `plugins/LumaeAnalysis/plugin.json` - the plugin metadata and release list.
* `plugins/LumaeAnalysis/*.py` - the plugin code.
* `dist/lumae_analysis/` - published Lumae release zip files.
* `tests/plugins/test_lumae_analysis.py` - local regression tests.

## Development

The latest AudioMuse plugin documentation is here:

https://github.com/NeptuneHub/AudioMuse-AI/blob/main/docs/PLUGIN.md

The release zip must contain code only: `__init__.py` and helper files, with no `plugin.json` inside the zip. The GitHub workflow rebuilds the zip, fills the release `sourceUrl` and `checksum`, and regenerates `manifest.json`.

## License

This repository is licensed under the AGPLv3 license. See `LICENSE`.
