"""Kafka consumer placeholder for transaction event ingestion."""

from collections.abc import Iterator
from typing import Any


class KafkaTransactionConsumer:
    """Consumes transaction events from a Kafka topic."""

    def __init__(self, bootstrap_servers: str, topic: str, group_id: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

    def stream(self) -> Iterator[dict[str, Any]]:
        """Yield transaction events from Kafka."""
        raise NotImplementedError("Consumer stream logic is not implemented yet.")
