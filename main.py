"""Application entry point for streaming transaction records from Parquet to Kafka."""

from pathlib import Path
import sys
import importlib

import pyarrow.parquet as pq

from config.settings import get_settings
from producer.transaction_producer import KafkaTransactionProducer
from utils.logger import get_logger


def _get_kafka_error_types() -> tuple[type[Exception], type[Exception]]:
    """Load Kafka exception types with safe fallbacks when dependency is unavailable."""
    try:
        errors_module = importlib.import_module("kafka.errors")
        return errors_module.NoBrokersAvailable, errors_module.KafkaError
    except Exception:
        class FallbackKafkaError(Exception):
            """Fallback Kafka base error."""

        class FallbackNoBrokersAvailable(FallbackKafkaError):
            """Fallback broker unavailable error."""

        return FallbackNoBrokersAvailable, FallbackKafkaError


def main() -> None:
    """Validate configuration and orchestrate Kafka streaming."""
    dry_run = True
    dry_run_limit = 20

    no_brokers_error, kafka_error = _get_kafka_error_types()
    settings = get_settings()
    logger = get_logger(__name__)

    dataset_path = Path(settings.dataset_path)
    if not dataset_path.exists():
        logger.error("Dataset not found at %s", dataset_path)
        sys.exit(1)

    dataset_size_bytes = dataset_path.stat().st_size
    dataset_size_mb = dataset_size_bytes / (1024 * 1024)

    total_rows: int | None = None
    try:
        total_rows = pq.ParquetFile(dataset_path).metadata.num_rows
    except Exception:
        logger.warning("Could not determine parquet row count for %s", dataset_path)

    logger.info("Starting %s", settings.project_name)
    logger.info(
        "Producer configuration: bootstrap_servers=%s, topic=%s, rows_per_second=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.producer_rows_per_second,
    )
    logger.info("Dataset path: %s", dataset_path)
    logger.info("Dataset size: %.2f MB", dataset_size_mb)
    if total_rows is not None:
        logger.info("Dataset rows: %d", total_rows)

    producer = KafkaTransactionProducer(dataset_path=str(dataset_path), config=settings, dry_run=dry_run)

    try:
        if dry_run:
            logger.info("DRY RUN MODE ENABLED")
            logger.info("Previewing first %d records only", dry_run_limit)
            producer.stream_data(limit=dry_run_limit, preview=True)
            logger.info("Test run completed successfully")
        else:
            logger.info("Streaming started")
            producer.stream_data()
            logger.info("Streaming stopped")
    except no_brokers_error:
        logger.error(
            "Kafka is not available at %s. Please start Kafka broker(s) and retry.",
            settings.kafka_bootstrap_servers,
        )
        sys.exit(1)
    except kafka_error:
        logger.exception("Kafka error occurred while streaming records.")
        sys.exit(1)
    except Exception:
        logger.exception("Unexpected error while streaming records.")
        sys.exit(1)


if __name__ == "__main__":
    main()
