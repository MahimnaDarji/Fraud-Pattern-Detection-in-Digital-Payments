"""Kafka producer for streaming Parquet transaction records as JSON messages."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import time
from typing import Any
import importlib

import pandas as pd
import pyarrow.parquet as pq

from config.settings import AppSettings, get_settings
from utils.logger import get_logger
from utils.schema import validate_transaction_record


class KafkaTransactionProducer:
    """Load transaction records, transform them, and publish to Kafka."""

    def __init__(
        self,
        dataset_path: str,
        config: AppSettings | None = None,
        batch_size: int = 10_000,
    ) -> None:
        settings = config or get_settings()
        self.dataset_path = Path(dataset_path)
        self.bootstrap_servers = settings.kafka_bootstrap_servers
        self.topic = settings.kafka_topic or "transactions"
        self.batch_size = batch_size
        self.rows_per_second = settings.producer_streaming_rate
        self.send_delay_seconds = settings.producer_send_delay_seconds
        self.log_every_n = max(1, settings.producer_log_frequency or settings.log_frequency)
        self._logger = get_logger(__name__)
        self._producer: Any | None = None
        self._dataset_columns: set[str] = set()
        self.total_records_read = 0
        self.valid_records_sent = 0
        self.invalid_records_skipped = 0

        kafka_module = importlib.import_module("kafka")
        kafka_producer_cls = getattr(kafka_module, "KafkaProducer")
        self._producer = kafka_producer_cls(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda message: json.dumps(message, default=str).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8"),
        )

    def load_data(self) -> Iterator[dict[str, Any]]:
        """Load Parquet rows lazily using Arrow batches for large-file efficiency."""
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.dataset_path}")

        parquet_file = pq.ParquetFile(self.dataset_path)
        self._dataset_columns = set(parquet_file.schema.names)
        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            frame = batch.to_pandas()
            for _, row in frame.iterrows():
                yield row.to_dict()

    def preprocess_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Clean raw row values into JSON-safe primitives before validation."""
        processed: dict[str, Any] = {}

        for column, value in row.items():
            processed[column] = self._normalize_value(column_name=column, value=value)

        return processed

    def validate_record(self, row: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        """Validate cleaned record using schema rules and dynamic dataset columns."""
        return validate_transaction_record(row, dataset_columns=self._dataset_columns)

    def send_to_kafka(self, message: dict[str, Any]) -> None:
        """Send a processed message to Kafka using a stable partition key."""
        partition_key = self._get_partition_key(message)
        if self._producer is None:
            raise RuntimeError("Kafka producer is not initialized.")

        self._producer.send(self.topic, key=partition_key, value=message).get(timeout=30)

    def stream_data(self) -> None:
        """Stream all Parquet records to Kafka topic as processed JSON messages."""
        self.total_records_read = 0
        self.valid_records_sent = 0
        self.invalid_records_skipped = 0

        for row_index, row in enumerate(self.load_data(), start=1):
            self.total_records_read += 1
            try:
                preprocessed = self.preprocess_row(row)
                message, validation_error = self.validate_record(preprocessed)
                if message is None:
                    self.invalid_records_skipped += 1
                    self._logger.warning(
                        "Validation failed at row_index=%d reason=%s context=%s",
                        row_index,
                        validation_error,
                        self._safe_row_context(preprocessed),
                    )
                    self._log_progress_if_needed()
                    continue

                self.send_to_kafka(message)
                self.valid_records_sent += 1

                self._throttle()
                self._log_progress_if_needed()
            except Exception:
                self.invalid_records_skipped += 1
                self._logger.exception(
                    "Failed to process/send row_index=%d context=%s",
                    row_index,
                    self._safe_row_context(row),
                )
                self._log_progress_if_needed()

        if self._producer is not None:
            self._producer.flush()
        self._logger.info(
            "Streaming completed. total_records_read=%d valid_records_sent=%d invalid_records_skipped=%d",
            self.total_records_read,
            self.valid_records_sent,
            self.invalid_records_skipped,
        )

    def _get_partition_key(self, message: dict[str, Any]) -> str:
        """Build partition key with user/account preference and transaction fallback."""
        for candidate in ("user_id", "account_id", "transaction_id"):
            value = message.get(candidate)
            if value is not None and str(value) != "":
                return str(value)
        return "unknown-key"

    def _normalize_value(self, column_name: str, value: Any) -> Any:
        """Convert nulls and temporal values to JSON-compatible forms."""
        if pd.isna(value):
            return None

        if isinstance(value, (datetime, date, pd.Timestamp)):
            return pd.Timestamp(value).isoformat()

        lowered_name = column_name.lower()
        if any(token in lowered_name for token in ("time", "date", "timestamp")) and isinstance(value, str):
            parsed = pd.to_datetime(value, errors="coerce")
            if not pd.isna(parsed):
                return parsed.isoformat()

        if isinstance(value, Decimal):
            return float(value)

        return value

    def _log_progress_if_needed(self) -> None:
        """Log compact pipeline progress every configured record interval."""
        if self.total_records_read % self.log_every_n != 0:
            return
        self._logger.info(
            "Progress: read=%d valid=%d invalid=%d topic=%s",
            self.total_records_read,
            self.valid_records_sent,
            self.invalid_records_skipped,
            self.topic,
        )

    def _throttle(self) -> None:
        """Control send speed using rows-per-second or fixed-delay settings."""
        if self.rows_per_second > 0:
            time.sleep(1 / self.rows_per_second)
            return

        if self.send_delay_seconds > 0:
            time.sleep(self.send_delay_seconds)

    def _safe_row_context(self, row: dict[str, Any]) -> dict[str, Any]:
        """Return minimal row context for failure logs without large payloads."""
        context_keys = ("transaction_id", "user_id", "account_id", "timestamp")
        return {key: row.get(key) for key in context_keys if key in row}
