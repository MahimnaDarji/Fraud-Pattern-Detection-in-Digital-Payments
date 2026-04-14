"""Supervised fraud model scaffold for gradient-boosting classifiers."""

import pandas as pd


class FraudClassifier:
    """Placeholder for XGBoost/LightGBM style fraud classification."""

    def train(self, training_frame: pd.DataFrame, labels: pd.Series) -> None:
        """Fit the classifier on labeled fraud datasets."""
        raise NotImplementedError("Fraud model training logic is not implemented yet.")

    def predict_proba(self, frame: pd.DataFrame) -> list[float]:
        """Return fraud probabilities for each event."""
        raise NotImplementedError("Fraud probability logic is not implemented yet.")
