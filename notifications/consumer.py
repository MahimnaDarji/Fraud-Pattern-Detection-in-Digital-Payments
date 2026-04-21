"""Kafka consumer that reads from the fraud_alerts topic and drives notification delivery."""

from __future__ import annotations

import json
from typing import Any

from config.settings import AppSettings, get_settings
from notifications.models import AlertPayload
from utils.logger import get_logger


class AlertKafkaConsumer:
    """Blocking consumer loop that reads fraud_alerts messages and yields AlertPayloads.

    Usage::

        consumer = AlertKafkaConsumer()
        for payload in consumer.consume():
            ...
    """

    def __init__(self, settings: AppSettings | None = None) -> None:
        self._settings = settings or get_settings()
        self._logger = get_logger(__name__)
        self._kafka_consumer = self._build_consumer()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def consume(self):
        """Yield AlertPayload objects from the fraud_alerts Kafka topic indefinitely.

        Only messages whose decoded JSON contains ``alert_triggered = true``
        are yielded — all others are silently skipped.
        """
        self._logger.info(
            "Alert consumer started bootstrap_servers=%s topic=%s group_id=%s",
            self._settings.alert_kafka_bootstrap_servers,
            self._settings.alert_kafka_topic,
            self._settings.alert_notification_group_id,
        )

        for message in self._kafka_consumer:
            record = self._decode_message(message)
            if record is None:
                continue

            if not self._is_triggered(record):
                continue

            try:
                payload = AlertPayload.from_dict(record)
            except Exception:
                self._logger.exception(
                    "Failed to parse AlertPayload from message offset=%s",
                    getattr(message, "offset", "unknown"),
                )
                continue

            yield payload

    def close(self) -> None:
        """Close the underlying Kafka consumer cleanly."""
        try:
            self._kafka_consumer.close()
            self._logger.info("Alert Kafka consumer closed")
        except Exception:
            self._logger.exception("Error closing alert Kafka consumer")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_consumer(self):
        """Construct and return a configured kafka-python KafkaConsumer."""
        import importlib

        kafka_module = importlib.import_module("kafka")
        consumer_cls = getattr(kafka_module, "KafkaConsumer")
        return consumer_cls(
            self._settings.alert_kafka_topic,
            bootstrap_servers=self._settings.alert_kafka_bootstrap_servers,
            group_id=self._settings.alert_notification_group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda raw: raw,  # decode lazily in _decode_message
        )

    def _decode_message(self, message: Any) -> dict[str, Any] | None:
        """Decode a raw Kafka message value to a Python dict."""
        try:
            raw = message.value
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8")
            return json.loads(raw)
        except Exception:
            self._logger.exception(
                "Failed to decode Kafka message offset=%s",
                getattr(message, "offset", "unknown"),
            )
            return None

    @staticmethod
    def _is_triggered(record: dict[str, Any]) -> bool:
        """Return True only if the alert_triggered field is explicitly true."""
        value = record.get("alert_triggered")
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes"}
        return bool(value)
