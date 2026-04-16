"""Application entry point for model training and streaming orchestration."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from config.settings import AppSettings, get_settings
from models.fraud_model import load_model, train_fraud_model
from stream_processing.spark_pipeline import run_streaming_pipeline
from utils.logger import configure_logging, get_logger


VALID_MODES = {"train_fraud_model", "run_streaming_pipeline"}


def _parse_args() -> argparse.Namespace:
    """Parse CLI mode selection for training or streaming orchestration."""
    parser = argparse.ArgumentParser(description="Fraud detection pipeline orchestrator")
    parser.add_argument(
        "mode",
        nargs="?",
        default="run_streaming_pipeline",
        help="Execution mode: train_fraud_model or run_streaming_pipeline",
    )
    return parser.parse_args()


def _run_train_fraud_model_mode(settings: AppSettings, logger) -> None:
    """Train and persist supervised fraud model artifact via model module."""
    dataset_path = Path(settings.dataset_path)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found at {dataset_path}")

    logger.info("Training started mode=train_fraud_model dataset=%s", dataset_path)
    model_path = train_fraud_model(
        dataset_path=str(dataset_path),
        model_path=settings.fraud_model_path,
    )
    artifact = load_model(model_path)
    logger.info("Training finished mode=train_fraud_model")
    logger.info("Fraud model saved path=%s", model_path)
    logger.info(
        "Training metrics: model_type=%s target_column=%s total_samples=%s positive_samples=%s negative_samples=%s scale_pos_weight=%.4f feature_count=%d",
        artifact.get("model_type", "unknown"),
        artifact.get("target_column", "unknown"),
        artifact.get("total_samples", "unknown"),
        artifact.get("positive_samples", "unknown"),
        artifact.get("negative_samples", "unknown"),
        float(artifact.get("scale_pos_weight", 0.0)),
        len(artifact.get("feature_columns", [])),
    )


def _run_streaming_pipeline_mode(settings: AppSettings, logger) -> None:
    """Validate required artifact and start structured streaming pipeline."""
    fraud_model_path = Path(settings.fraud_model_path)
    if not fraud_model_path.exists():
        raise FileNotFoundError(
            "Fraud model artifact not found at "
            f"{fraud_model_path}. Run mode=train_fraud_model before run_streaming_pipeline."
        )

    logger.info(
        "Streaming startup mode=run_streaming_pipeline fraud_model_path=%s fraud_prediction_threshold=%.4f",
        fraud_model_path,
        settings.fraud_prediction_threshold,
    )
    run_streaming_pipeline()


def main() -> None:
    """Dispatch CLI mode to supervised training or streaming pipeline runtime."""
    args = _parse_args()
    settings = get_settings()
    configure_logging(debug_mode=settings.debug_mode)
    logger = get_logger(__name__)

    mode = str(args.mode).strip()
    logger.info("Starting %s mode=%s", settings.project_name, mode)

    if mode not in VALID_MODES:
        logger.error(
            "Invalid mode '%s'. Valid modes: train_fraud_model, run_streaming_pipeline",
            mode,
        )
        sys.exit(2)

    try:
        if mode == "train_fraud_model":
            _run_train_fraud_model_mode(settings, logger)
            return

        _run_streaming_pipeline_mode(settings, logger)
    except Exception:
        logger.exception("Execution failed mode=%s", mode)
        sys.exit(1)


if __name__ == "__main__":
    main()
