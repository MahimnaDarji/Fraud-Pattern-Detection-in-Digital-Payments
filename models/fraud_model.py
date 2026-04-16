"""Supervised fraud model training and inference utilities."""

from __future__ import annotations

from pathlib import Path
import pickle
from typing import Any

import pandas as pd


DEFAULT_FRAUD_FEATURE_COLUMNS = [
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

TARGET_CANDIDATE_COLUMNS = ["is_fraud", "fraud_label", "fraud", "label", "target"]


def train_fraud_model(
    dataset_path: str,
    model_path: str = "artifacts/fraud_model.pkl",
    target_column: str | None = None,
    feature_columns: list[str] | None = None,
    random_state: int = 42,
) -> str:
    """Train a supervised fraud classifier from parquet and save an artifact."""
    frame = pd.read_parquet(dataset_path)
    selected_target = target_column or _resolve_target_column(frame)
    selected_features = feature_columns or list(DEFAULT_FRAUD_FEATURE_COLUMNS)

    if selected_target not in frame.columns:
        raise ValueError(f"Target column '{selected_target}' was not found in dataset.")

    frame = _derive_training_features(frame)
    labels = _coerce_binary_target(frame[selected_target])
    feature_frame = _prepare_feature_frame(frame, selected_features)

    positive_count = int(labels.sum())
    negative_count = int(len(labels) - positive_count)
    if positive_count == 0 or negative_count == 0:
        raise ValueError("Fraud target must contain both positive and negative classes.")

    scale_pos_weight = max(1.0, negative_count / positive_count)
    model, model_type = _build_classifier(scale_pos_weight=scale_pos_weight, random_state=random_state)
    model.fit(feature_frame, labels)

    artifact = {
        "model": model,
        "model_type": model_type,
        "feature_columns": selected_features,
        "target_column": selected_target,
        "scale_pos_weight": float(scale_pos_weight),
        "total_samples": int(len(labels)),
        "positive_samples": positive_count,
        "negative_samples": negative_count,
    }

    output_path = Path(model_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as model_file:
        pickle.dump(artifact, model_file)

    return str(output_path)


def load_model(model_path: str = "artifacts/fraud_model.pkl") -> dict[str, Any]:
    """Load the trained fraud model artifact from disk."""
    artifact_path = Path(model_path)
    if not artifact_path.exists():
        raise FileNotFoundError(
            f"Fraud model file not found at {model_path}. Train and save the model first."
        )

    with artifact_path.open("rb") as model_file:
        artifact = pickle.load(model_file)

    if not isinstance(artifact, dict) or "model" not in artifact or "feature_columns" not in artifact:
        raise ValueError("Invalid fraud model artifact format.")

    return artifact


def predict_proba(
    features: pd.DataFrame,
    model_artifact: dict[str, Any] | None = None,
    model_path: str = "artifacts/fraud_model.pkl",
) -> list[float]:
    """Predict fraud probabilities in [0, 1] for the provided feature frame."""
    artifact = model_artifact or load_model(model_path)
    model = artifact["model"]
    feature_columns = list(artifact["feature_columns"])

    prepared = _prepare_feature_frame(features, feature_columns)
    probabilities = _predict_positive_probability(model, prepared)
    return probabilities.tolist()


class FraudClassifier:
    """Backward-compatible wrapper around supervised fraud model functions."""

    def __init__(self, model_path: str = "artifacts/fraud_model.pkl") -> None:
        self.model_path = model_path
        self.artifact: dict[str, Any] | None = None

    def train(self, training_frame: pd.DataFrame, labels: pd.Series) -> None:
        """Fit an in-memory classifier from a prepared frame and labels."""
        selected_features = list(DEFAULT_FRAUD_FEATURE_COLUMNS)
        prepared = _prepare_feature_frame(training_frame, selected_features)
        y = _coerce_binary_target(labels)

        positive_count = int(y.sum())
        negative_count = int(len(y) - positive_count)
        if positive_count == 0 or negative_count == 0:
            raise ValueError("Fraud labels must contain both positive and negative classes.")

        scale_pos_weight = max(1.0, negative_count / positive_count)
        model, model_type = _build_classifier(scale_pos_weight=scale_pos_weight, random_state=42)
        model.fit(prepared, y)

        self.artifact = {
            "model": model,
            "model_type": model_type,
            "feature_columns": selected_features,
            "target_column": "in_memory",
            "scale_pos_weight": float(scale_pos_weight),
        }

    def load(self) -> dict[str, Any]:
        """Load classifier artifact from configured model path."""
        self.artifact = load_model(self.model_path)
        return self.artifact

    def predict_proba(self, frame: pd.DataFrame) -> list[float]:
        """Return fraud probabilities for each event."""
        if self.artifact is None:
            self.artifact = load_model(self.model_path)
        return predict_proba(frame, model_artifact=self.artifact)


def _resolve_target_column(frame: pd.DataFrame) -> str:
    """Resolve fraud target column from common label names."""
    for candidate in TARGET_CANDIDATE_COLUMNS:
        if candidate in frame.columns:
            return candidate
    raise ValueError(
        "No fraud target column found. Expected one of: " + ", ".join(TARGET_CANDIDATE_COLUMNS)
    )


def _build_classifier(scale_pos_weight: float, random_state: int) -> tuple[Any, str]:
    """Create preferred boosting classifier with class-imbalance handling."""
    try:
        from xgboost import XGBClassifier

        model = XGBClassifier(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.8,
            objective="binary:logistic",
            eval_metric="logloss",
            random_state=random_state,
            n_jobs=-1,
            scale_pos_weight=scale_pos_weight,
        )
        return model, "xgboost"
    except Exception:
        from lightgbm import LGBMClassifier

        model = LGBMClassifier(
            n_estimators=300,
            learning_rate=0.05,
            random_state=random_state,
            n_jobs=-1,
            scale_pos_weight=scale_pos_weight,
        )
        return model, "lightgbm"


def _predict_positive_probability(model: Any, frame: pd.DataFrame) -> pd.Series:
    """Return positive-class probabilities while handling model API differences."""
    if hasattr(model, "predict_proba"):
        raw = model.predict_proba(frame)
        if raw.ndim == 2 and raw.shape[1] >= 2:
            return pd.Series(raw[:, 1], index=frame.index).clip(0.0, 1.0)
        return pd.Series(raw.ravel(), index=frame.index).clip(0.0, 1.0)

    raw_predictions = pd.Series(model.predict(frame), index=frame.index)
    return raw_predictions.clip(0.0, 1.0)


def _coerce_binary_target(labels: pd.Series) -> pd.Series:
    """Normalize target values to numeric binary labels."""
    normalized = labels.copy()
    if normalized.dtype == object:
        mapping = {
            "true": 1,
            "false": 0,
            "yes": 1,
            "no": 0,
            "fraud": 1,
            "legit": 0,
            "legitimate": 0,
        }
        normalized = normalized.astype(str).str.strip().str.lower().map(lambda value: mapping.get(value, value))

    numeric = pd.to_numeric(normalized, errors="coerce")
    if numeric.isna().any():
        raise ValueError("Target column contains non-numeric labels that cannot be normalized to binary values.")

    numeric = (numeric > 0).astype(int)
    return numeric


def _prepare_feature_frame(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    """Align and coerce model features to a stable numeric matrix."""
    features = frame.copy()
    for column in feature_columns:
        if column not in features.columns:
            features[column] = 0.0

    features = features[feature_columns]
    features = features.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return features


def _derive_training_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive engineered and historical-friendly features when absent in raw data."""
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
