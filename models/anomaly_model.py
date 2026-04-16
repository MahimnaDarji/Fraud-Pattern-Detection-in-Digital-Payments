"""Isolation Forest training and inference utilities for streaming anomaly scoring."""

from __future__ import annotations

from pathlib import Path
import pickle
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
from sklearn.ensemble import IsolationForest


DEFAULT_ANOMALY_FEATURE_COLUMNS = [
    "amount",
    "transaction_hour",
    "transaction_day_of_week",
    "is_high_amount",
    "transaction_count_per_user",
    "total_amount_per_user",
    "avg_amount_per_user",
    "transaction_count_per_merchant",
    "historical_transaction_count",
    "historical_avg_amount",
    "historical_total_amount",
    "historical_max_amount",
]


def train_isolation_forest_model(
    dataset_path: str,
    model_path: str = "artifacts/anomaly_model.pkl",
    sample_size: int = 50_000,
    contamination: float = 0.02,
    random_state: int = 42,
    feature_columns: list[str] | None = None,
) -> str:
    """Train Isolation Forest on parquet sample and persist model artifact."""
    selected_features = feature_columns or DEFAULT_ANOMALY_FEATURE_COLUMNS
    training_frame = _load_training_sample_from_parquet(
        dataset_path=dataset_path,
        sample_size=sample_size,
        feature_columns=selected_features,
    )

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(training_frame)

    artifact = {
        "model": model,
        "feature_columns": selected_features,
        "contamination": contamination,
    }

    output_path = Path(model_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as model_file:
        pickle.dump(artifact, model_file)

    return str(output_path)


def load_anomaly_model(model_path: str = "artifacts/anomaly_model.pkl") -> dict[str, Any]:
    """Load saved anomaly model artifact for inference."""
    artifact_path = Path(model_path)
    if not artifact_path.exists():
        raise FileNotFoundError(
            f"Anomaly model file not found at {model_path}. Train and save the model first."
        )

    with artifact_path.open("rb") as model_file:
        artifact = pickle.load(model_file)

    if not isinstance(artifact, dict) or "model" not in artifact or "feature_columns" not in artifact:
        raise ValueError("Invalid anomaly model artifact format.")

    return artifact


class AnomalyDetector:
    """Backward-compatible wrapper around Isolation Forest training and scoring."""

    def __init__(self, contamination: float = 0.02, random_state: int = 42) -> None:
        self.contamination = contamination
        self.random_state = random_state
        self.feature_columns = list(DEFAULT_ANOMALY_FEATURE_COLUMNS)
        self.model = IsolationForest(
            n_estimators=200,
            contamination=self.contamination,
            random_state=self.random_state,
            n_jobs=-1,
        )

    def train(self, training_frame: pd.DataFrame) -> None:
        """Fit Isolation Forest using normalized numeric features."""
        features = _prepare_feature_frame(training_frame, self.feature_columns)
        self.model.fit(features)

    def score(self, frame: pd.DataFrame) -> list[float]:
        """Return anomaly scores where larger values indicate more anomalous behavior."""
        features = _prepare_feature_frame(frame, self.feature_columns)
        return (-self.model.decision_function(features)).tolist()


def _load_training_sample_from_parquet(
    dataset_path: str,
    sample_size: int,
    feature_columns: list[str],
) -> pd.DataFrame:
    """Load feature-aligned pandas frame from parquet with optional deterministic sampling."""
    parquet_path = Path(dataset_path)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Dataset not found at {dataset_path}")

    parquet_file = pq.ParquetFile(parquet_path)
    available_columns = set(parquet_file.schema.names)
    readable_columns = [column for column in feature_columns if column in available_columns]

    if not readable_columns:
        raise ValueError("No requested anomaly feature columns were found in dataset.")

    frame = pd.read_parquet(parquet_path, columns=readable_columns)
    frame = _derive_training_features(frame)
    frame = _prepare_feature_frame(frame, feature_columns)

    if sample_size > 0 and len(frame) > sample_size:
        frame = frame.sample(n=sample_size, random_state=42)

    return frame


def _prepare_feature_frame(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    """Convert incoming frame to stable numeric feature matrix for model use."""
    features = frame.copy()

    for column in feature_columns:
        if column not in features.columns:
            features[column] = 0.0

    features = features[feature_columns]
    features = features.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return features


def _derive_training_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive numeric fraud-signal features from historical raw columns when possible."""
    derived = frame.copy()

    if "timestamp" in derived.columns:
        parsed_timestamp = pd.to_datetime(derived["timestamp"], errors="coerce")
        if "transaction_hour" not in derived.columns:
            derived["transaction_hour"] = parsed_timestamp.dt.hour.fillna(0)
        if "transaction_day_of_week" not in derived.columns:
            derived["transaction_day_of_week"] = parsed_timestamp.dt.dayofweek.fillna(0)

    if "amount" in derived.columns:
        amount_series = pd.to_numeric(derived["amount"], errors="coerce").fillna(0.0)
        derived["amount"] = amount_series
        if "is_high_amount" not in derived.columns:
            derived["is_high_amount"] = (amount_series >= 1000.0).astype(float)

        if "user_id" in derived.columns or "account_id" in derived.columns:
            join_key = None
            if "user_id" in derived.columns:
                join_key = derived["user_id"].astype(str).str.strip().replace("", pd.NA)
            if "account_id" in derived.columns:
                account_key = derived["account_id"].astype(str).str.strip().replace("", pd.NA)
                join_key = join_key.fillna(account_key) if join_key is not None else account_key

            if join_key is not None:
                derived["_user_join_key"] = join_key
                user_stats = (
                    derived.dropna(subset=["_user_join_key"])
                    .groupby("_user_join_key")["amount"]
                    .agg(["count", "sum", "mean", "max"])
                    .rename(
                        columns={
                            "count": "transaction_count_per_user",
                            "sum": "total_amount_per_user",
                            "mean": "avg_amount_per_user",
                            "max": "historical_max_amount",
                        }
                    )
                )
                derived = derived.merge(user_stats, how="left", left_on="_user_join_key", right_index=True)
                if "historical_transaction_count" not in derived.columns:
                    derived["historical_transaction_count"] = derived["transaction_count_per_user"]
                if "historical_total_amount" not in derived.columns:
                    derived["historical_total_amount"] = derived["total_amount_per_user"]
                if "historical_avg_amount" not in derived.columns:
                    derived["historical_avg_amount"] = derived["avg_amount_per_user"]

        if "merchant_id" in derived.columns and "transaction_count_per_merchant" not in derived.columns:
            merchant_counts = (
                derived["merchant_id"].astype(str).str.strip().replace("", pd.NA).value_counts(dropna=True)
            )
            derived["transaction_count_per_merchant"] = (
                derived["merchant_id"].astype(str).str.strip().map(merchant_counts).fillna(0)
            )

    if "_user_join_key" in derived.columns:
        derived = derived.drop(columns=["_user_join_key"])

    return derived
