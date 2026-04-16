"""Structured Streaming feature engineering for real-time fraud signals."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    dayofweek,
    hour,
    lit,
    sum as spark_sum,
    to_timestamp,
    trim,
    when,
    window,
)

from config.settings import AppSettings, get_settings
from utils.logger import get_logger


class TransactionFeatureEngineer:
    """Create first-layer streaming fraud features from parsed transaction events."""

    def __init__(self, settings: AppSettings | None = None) -> None:
        self.settings = settings or get_settings()
        self._logger = get_logger(__name__)

    def enrich_stream(self, parsed_stream_df: DataFrame) -> DataFrame:
        """Return an enriched streaming DataFrame with temporal and windowed features."""
        self._logger.info("Feature engineering started")
        self._validate_required_columns(parsed_stream_df)

        user_key_mode = self._resolve_user_key_mode(parsed_stream_df)
        self._logger.info("User aggregation key strategy: %s", user_key_mode)

        prepared_df = self._prepare_base_features(parsed_stream_df)
        watermark_df = prepared_df.withWatermark("event_timestamp", self.settings.feature_watermark_delay)

        base_with_window = watermark_df.withColumn(
            "feature_window",
            window(
                col("event_timestamp"),
                self.settings.feature_window_duration,
                self.settings.feature_slide_duration,
            ),
        )

        user_window_agg_df = self._build_user_window_aggregations(watermark_df)
        merchant_window_agg_df = self._build_merchant_window_aggregations(watermark_df)

        enriched_df = (
            base_with_window.alias("base")
            .join(
                user_window_agg_df.alias("user_agg"),
                on=[
                    col("base.feature_window") == col("user_agg.window"),
                    col("base.user_agg_key") == col("user_agg.user_agg_key"),
                ],
                how="left",
            )
            .join(
                merchant_window_agg_df.alias("merchant_agg"),
                on=[
                    col("base.feature_window") == col("merchant_agg.window"),
                    col("base.merchant_agg_key") == col("merchant_agg.merchant_agg_key"),
                ],
                how="left",
            )
            .select(
                col("base.*"),
                col("user_agg.transaction_count_per_user"),
                col("user_agg.total_amount_per_user"),
                col("user_agg.avg_amount_per_user"),
                col("merchant_agg.transaction_count_per_merchant"),
            )
        )

        self._logger.info("Enriched streaming DataFrame created successfully")
        return enriched_df

    def _prepare_base_features(self, parsed_stream_df: DataFrame) -> DataFrame:
        """Create base event-time and identity columns used for all feature derivations."""
        timestamped_df = parsed_stream_df.withColumn("event_timestamp", to_timestamp(col("timestamp")))

        return (
            timestamped_df.withColumn(
                "user_agg_key",
                coalesce(
                    when(trim(col("user_id")) != "", trim(col("user_id"))),
                    when(trim(col("account_id")) != "", trim(col("account_id"))),
                ),
            )
            .withColumn(
                "merchant_agg_key",
                when(trim(col("merchant_id")) != "", trim(col("merchant_id"))).otherwise(lit(None)),
            )
            .withColumn("transaction_hour", hour(col("event_timestamp")))
            .withColumn("transaction_day_of_week", dayofweek(col("event_timestamp")))
            .withColumn(
                "is_high_amount",
                when(col("amount") >= lit(float(self.settings.feature_high_amount_threshold)), lit(1)).otherwise(
                    lit(0)
                ),
            )
        )

    def _build_user_window_aggregations(self, stream_df: DataFrame) -> DataFrame:
        """Compute user/account-level windowed amount and count statistics."""
        return (
            stream_df.filter(col("event_timestamp").isNotNull() & col("user_agg_key").isNotNull())
            .groupBy(
                window(
                    col("event_timestamp"),
                    self.settings.feature_window_duration,
                    self.settings.feature_slide_duration,
                ),
                col("user_agg_key"),
            )
            .agg(
                count(lit(1)).alias("transaction_count_per_user"),
                spark_sum(col("amount")).alias("total_amount_per_user"),
                avg(col("amount")).alias("avg_amount_per_user"),
            )
        )

    def _build_merchant_window_aggregations(self, stream_df: DataFrame) -> DataFrame:
        """Compute merchant-level windowed transaction counts."""
        return (
            stream_df.filter(col("event_timestamp").isNotNull() & col("merchant_agg_key").isNotNull())
            .groupBy(
                window(
                    col("event_timestamp"),
                    self.settings.feature_window_duration,
                    self.settings.feature_slide_duration,
                ),
                col("merchant_agg_key"),
            )
            .agg(count(lit(1)).alias("transaction_count_per_merchant"))
        )

    def _resolve_user_key_mode(self, parsed_stream_df: DataFrame) -> str:
        """Describe user-level identity mode based on available columns."""
        has_user_id = "user_id" in parsed_stream_df.columns
        has_account_id = "account_id" in parsed_stream_df.columns

        if has_user_id and has_account_id:
            return "user_id primary with account_id fallback"
        if has_user_id:
            return "user_id only"
        if has_account_id:
            return "account_id only"

        raise ValueError("Neither user_id nor account_id exists in parsed stream for user-level aggregation.")

    def _validate_required_columns(self, parsed_stream_df: DataFrame) -> None:
        """Fail clearly when required columns for feature engineering are missing."""
        required_columns = {"timestamp", "amount", "merchant_id"}
        available_columns = set(parsed_stream_df.columns)
        missing = sorted(required_columns - available_columns)

        if missing:
            raise ValueError(
                "Missing required columns for feature engineering: " + ", ".join(missing)
            )

        if "user_id" not in available_columns and "account_id" not in available_columns:
            raise ValueError(
                "Missing identity columns for user aggregations: provide user_id and/or account_id."
            )


def build_feature_vector(event: dict[str, Any]) -> dict[str, float]:
    """Legacy batch-style vector helper intentionally unsupported in stream-first path."""
    raise NotImplementedError("Use TransactionFeatureEngineer.enrich_stream for structured streaming features.")
