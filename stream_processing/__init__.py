"""Streaming feature engineering package built for Spark pipelines."""

from .features import build_feature_vector
from .spark_pipeline import create_spark_session, run_streaming_pipeline

__all__ = ["create_spark_session", "run_streaming_pipeline", "build_feature_vector"]
