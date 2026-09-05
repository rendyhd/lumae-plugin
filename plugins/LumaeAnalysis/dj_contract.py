"""Pinned DJ measurement constants and shared errors; no model imports."""

import os

SCHEMA_VERSION = 2


METHOD = "beat-this-1.1.0-yamnet-lite-1-lumae-dj-v2.4"


BEAT_THIS_VERSION = "1.1.0"


MODEL_NAME = "final0"


MODEL_URL = "https://cloud.cp.jku.at/public.php/dav/files/7ik4RrBKTS273gp/final0.ckpt"


MODEL_BYTES = 81_058_141


MODEL_SHA256 = "8c328b45f59d8dd3dff219253ff6a8d6482be57d0133a29140e2febbf8eb8331"


YAMNET_MODEL_NAME = "yamnet-classification-tflite-1"


YAMNET_MODEL_URL = (
    "https://tfhub.dev/google/lite-model/yamnet/classification/tflite/1"
    "?lite-format=tflite"
)


YAMNET_MODEL_BYTES = 4_126_810


YAMNET_MODEL_SHA256 = "10c95ea3eb9a7bb4cb8bddf6feb023250381008177ac162ce169694d05c317de"


YAMNET_SAMPLE_RATE = 16_000


YAMNET_WINDOW_SAMPLES = 15_600


YAMNET_HOP_SAMPLES = 7_680


YAMNET_CLASS_COUNT = 521


YAMNET_CLASS_MAP_SHA256 = (
    "cdf24d193e196d9e95912a2667051ae203e92a2ba09449218ccb40ef787c6df2"
)


YAMNET_VOCAL_CLASSES = {
    0: "Speech",
    1: "Child speech, kid speaking",
    2: "Conversation",
    3: "Narration, monologue",
    4: "Babbling",
    5: "Speech synthesizer",
    12: "Whispering",
    24: "Singing",
    25: "Choir",
    27: "Chant",
    28: "Mantra",
    29: "Child singing",
    30: "Synthetic singing",
    31: "Rapping",
    32: "Humming",
    63: "Chatter",
    65: "Hubbub, speech noise, speech babble",
    249: "Vocal music",
    250: "A capella",
    261: "Song",
}


MODEL_SAMPLE_RATE = 22_050


MODEL_FPS = 50


MODEL_N_FFT = 1_024


MODEL_HOP_SAMPLES = 441


MODEL_MEL_BINS = 128


MODEL_WINDOW_FRAMES = 1_500


MODEL_BORDER_FRAMES = 6


MODEL_STEP_FRAMES = MODEL_WINDOW_FRAMES - 2 * MODEL_BORDER_FRAMES


MAX_SOURCE_SECONDS = 30 * 60


JOB_DEADLINE_SECONDS = 15 * 60


DEFAULT_RSS_CAP_BYTES = 1 * 1024 * 1024 * 1024


MIN_RSS_CAP_BYTES = 512 * 1024 * 1024


MAX_RSS_CAP_BYTES = 4 * 1024 * 1024 * 1024


def configured_rss_cap_bytes(value=None):
    """Return the bounded worker RSS ceiling; invalid overrides fail to default."""
    raw = os.environ.get("LUMAE_DJ_RSS_CAP_BYTES", "") if value is None else value
    if raw in (None, ""):
        return DEFAULT_RSS_CAP_BYTES
    try:
        parsed = int(str(raw).strip())
    except (TypeError, ValueError):
        return DEFAULT_RSS_CAP_BYTES
    return max(MIN_RSS_CAP_BYTES, min(MAX_RSS_CAP_BYTES, parsed))


RSS_CAP_BYTES = configured_rss_cap_bytes()


MIN_REGION_BEATS = 32


MAX_INTERVAL_CV = 0.06


MAX_RAW_DOWNBEAT_ALIGNMENT_SECONDS = 0.05


NOVELTY_Z_THRESHOLD = 0.0


MAX_ENTRY_CANDIDATES = 8


MAX_EXIT_CANDIDATES = 8


PINNED_PACKAGES = {
    "beat-this": "1.1.0",
    "torch": "2.6.0",
    "torchaudio": "2.6.0",
    "einops": "0.8.1",
    "rotary-embedding-torch": "0.8.6",
    "soxr": "0.5.0.post1",
    "av": "16.1.0",
    "numpy": "2.1.3",
    "psutil": "6.1.1",
    "ai-edge-litert": "2.2.0",
}


PINNED_PACKAGE_VARIANTS = {
    "torch": ("2.6.0", "2.6.0+cpu"),
    "torchaudio": ("2.6.0", "2.6.0+cpu"),
}


class DjAnalysisError(ValueError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code
