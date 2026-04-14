"""Consumer package for ingesting transaction events from Kafka."""

from .transaction_consumer import KafkaTransactionConsumer

__all__ = ["KafkaTransactionConsumer"]
