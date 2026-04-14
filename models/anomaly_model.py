"""Unsupervised anomaly detection model scaffold for fraud signals."""

import pandas as pd


class AnomalyDetector:
    """Placeholder for an unsupervised detector (e.g., IsolationForest)."""

    def train(self, training_frame: pd.DataFrame) -> None:
        """Fit the anomaly model on historical normal behavior."""
        raise NotImplementedError("Anomaly training logic is not implemented yet.")

    def score(self, frame: pd.DataFrame) -> list[float]:
        """Return anomaly scores for incoming events."""
        raise NotImplementedError("Anomaly scoring logic is not implemented yet.")
