"""Spark Structured Streaming pipeline placeholders for fraud scoring flow."""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "fraud_detection_system") -> SparkSession:
    """Create and return a Spark session configured for streaming."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def run_streaming_pipeline() -> None:
    """Entry point for Spark stream ingestion, transformation, and sink operations."""
    raise NotImplementedError("Streaming pipeline logic is not implemented yet.")
