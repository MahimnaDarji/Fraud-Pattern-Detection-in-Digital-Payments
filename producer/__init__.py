"""Producer package for publishing transaction events to Kafka."""

from .transaction_producer import KafkaTransactionProducer

__all__ = ["KafkaTransactionProducer"]
