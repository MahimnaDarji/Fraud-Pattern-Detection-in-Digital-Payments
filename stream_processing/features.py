"""Feature engineering placeholders for real-time fraud model inputs."""

from typing import Any


def build_feature_vector(event: dict[str, Any]) -> dict[str, float]:
    """Transform a raw transaction event into model-ready numeric features."""
    raise NotImplementedError("Feature engineering logic is not implemented yet.")
