# YAMNet Lite model notice

The optional DJ worker uses the versioned official YAMNet Lite classification
artifact published by Google at
`https://tfhub.dev/google/lite-model/yamnet/classification/tflite/1`.

- Model size: 4,126,810 bytes
- SHA-256: `10c95ea3eb9a7bb4cb8bddf6feb023250381008177ac162ce169694d05c317de`
- Runtime: `ai-edge-litert==2.2.0`
- Input/output: float32; the artifact uses dynamic-range weight quantization
- Training ontology: AudioSet

YAMNet is published by the TensorFlow Models project under Apache-2.0. The
AudioSet ontology and source data carry their own provenance and usage caveats.
Lumae aggregates selected speech and vocal class scores as uncalibrated evidence.
Those raw scores are not probabilities and never authorize a structural cut on
their own. A held-out calibration artifact is required before vocal-risk evidence
can qualify a DJ transition.
