"""Spark Structured Streaming pipeline for Kafka transaction ingestion and parsing."""

from __future__ import annotations

from datetime import datetime, timezone
from collections import Counter
import json
import pandas as pd

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    from_json,
    max as spark_max,
    pandas_udf,
    struct,
    sum as spark_sum,
    trim,
    when,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    MapType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config.settings import AppSettings, get_settings
from models.anomaly_model import load_anomaly_model
from models.fraud_model import load_model as load_fraud_model
from models.graph_model import TransactionGraphBuilder
from stream_processing.features import TransactionFeatureEngineer
from utils.logger import configure_logging, get_logger


class TransactionSparkPipeline:
    """Kafka-backed Structured Streaming pipeline for real-time fraud detection."""

    def __init__(self, settings: AppSettings | None = None) -> None:
        self.settings = settings or get_settings()
        configure_logging(debug_mode=self.settings.debug_mode)
        self._logger = get_logger(__name__)
        self.spark = self._create_spark_session()
        self._feature_engineer = TransactionFeatureEngineer(settings=self.settings)
        self._anomaly_artifact = self._load_anomaly_model_artifact()
        self._fraud_artifact = self._load_fraud_model_artifact()
        self._graph_builder = TransactionGraphBuilder()
        self._alert_producer = None
        self._audit_store = self._init_audit_store()

    #: Ordered fields written to every fraud_alerts Kafka message.
    _ALERT_FIELDS: tuple[str, ...] = (
        "transaction_id",
        "user_id",
        "account_id",
        "merchant_id",
        "amount",
        "timestamp",
        "fraud_probability",
        "anomaly_score",
        "propagated_risk_score",
        "alert_reason",
        "alert_severity",
        "alert_timestamp",
    )

    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session configured for Structured Streaming and Kafka source use."""
        try:
            builder = SparkSession.builder.appName(self.settings.project_name)
            builder = builder.config("spark.sql.streaming.schemaInference", "true")

            if self.settings.spark_kafka_packages:
                builder = builder.config("spark.jars.packages", self.settings.spark_kafka_packages)

            spark = builder.getOrCreate()
            self._logger.info("Spark session created")
            self._assert_kafka_source_available(spark)
            return spark
        except Exception as exc:
            self._logger.exception("Failed to create Spark session with Kafka integration")
            raise RuntimeError(
                "Spark session setup failed. Verify pyspark installation and Kafka connector configuration."
            ) from exc

    def _load_anomaly_model_artifact(self) -> dict:
        """Load trained anomaly model artifact exactly once for pipeline lifetime."""
        try:
            artifact = load_anomaly_model(self.settings.anomaly_model_path)
            self._logger.info("Loaded anomaly model from %s", self.settings.anomaly_model_path)
            return artifact
        except Exception as exc:
            self._logger.exception("Failed to load anomaly model from %s", self.settings.anomaly_model_path)
            raise RuntimeError(
                "Unable to load anomaly model for streaming inference. Train the model and verify artifact path."
            ) from exc

    def _load_fraud_model_artifact(self) -> dict:
        """Load trained supervised fraud model artifact once for pipeline lifetime."""
        try:
            artifact = load_fraud_model(self.settings.fraud_model_path)
            self._logger.info("Loaded fraud model from %s", self.settings.fraud_model_path)
            return artifact
        except Exception as exc:
            self._logger.exception("Failed to load fraud model from %s", self.settings.fraud_model_path)
            raise RuntimeError(
                "Unable to load fraud model for streaming inference. Train the model and verify artifact path."
            ) from exc

    def _assert_kafka_source_available(self, spark: SparkSession) -> None:
        """Fail early if the spark-sql-kafka connector is missing or incompatible."""
        try:
            spark.readStream.format("kafka")
        except Exception as exc:
            raise RuntimeError(
                "Kafka source is unavailable in Spark. Add a compatible spark-sql-kafka package "
                "(for example with --packages or SPARK_KAFKA_PACKAGES)."
            ) from exc

    def _transaction_schema(self) -> StructType:
        """Schema for validated core transaction fields emitted by the producer."""
        return StructType(
            [
                StructField("transaction_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("merchant_id", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )

    def get_parsed_stream_df(self) -> DataFrame:
        """Return a parsed streaming DataFrame with transaction and Kafka metadata columns."""
        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
            .option("subscribe", self.settings.spark_kafka_topic)
            .option("startingOffsets", self.settings.spark_starting_offsets)
            .load()
        )
        self._logger.info(
            "Kafka stream connected bootstrap_servers=%s topic=%s startingOffsets=%s",
            self.settings.kafka_bootstrap_servers,
            self.settings.spark_kafka_topic,
            self.settings.spark_starting_offsets,
        )

        parsed_df = (
            kafka_df.select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("kafka_value"),
                col("timestamp").cast(TimestampType()).alias("kafka_timestamp"),
            )
            .withColumn("transaction", from_json(col("kafka_value"), self._transaction_schema()))
            .withColumn(
                "transaction_payload",
                from_json(col("kafka_value"), MapType(StringType(), StringType())),
            )
            .select(
                "kafka_key",
                "kafka_timestamp",
                "kafka_value",
                col("transaction.*"),
                "transaction_payload",
            )
        )

        return parsed_df

    def get_enriched_stream_df(self) -> DataFrame:
        """Return parsed stream enriched with first-layer real-time fraud features."""
        parsed_df = self.get_parsed_stream_df()
        enriched_df = self._feature_engineer.enrich_stream(parsed_df)
        self._logger.info("Enriched stream is ready")
        return enriched_df

    def _load_historical_transactions_df(self) -> DataFrame:
        """Load static historical transaction data from parquet."""
        try:
            historical_df = self.spark.read.parquet(self.settings.dataset_path)
            self._logger.info("Loaded historical dataset from %s", self.settings.dataset_path)
            return historical_df
        except Exception as exc:
            self._logger.exception("Failed to load historical dataset from %s", self.settings.dataset_path)
            raise RuntimeError(
                f"Unable to read historical dataset parquet at {self.settings.dataset_path}."
            ) from exc

    def _build_historical_profile_df(self, historical_df: DataFrame) -> DataFrame:
        """Build user-level historical profile metrics with user_id/account_id fallback."""
        required_columns = {"amount"}
        missing_required = sorted(required_columns - set(historical_df.columns))
        if missing_required:
            raise ValueError(
                "Historical dataset missing required columns: " + ", ".join(missing_required)
            )

        if "user_id" not in historical_df.columns and "account_id" not in historical_df.columns:
            raise ValueError("Historical dataset must include user_id or account_id for profile keying.")

        key_candidates = []
        if "user_id" in historical_df.columns:
            key_candidates.append(when(trim(col("user_id")) != "", trim(col("user_id"))))
        if "account_id" in historical_df.columns:
            key_candidates.append(when(trim(col("account_id")) != "", trim(col("account_id"))))

        historical_with_key_df = historical_df.withColumn(
            "historical_user_join_key",
            coalesce(*key_candidates),
        )

        profile_df = (
            historical_with_key_df.filter(col("historical_user_join_key").isNotNull())
            .groupBy("historical_user_join_key")
            .agg(
                count(col("amount")).alias("historical_transaction_count"),
                avg(col("amount")).alias("historical_avg_amount"),
                spark_sum(col("amount")).alias("historical_total_amount"),
                spark_max(col("amount")).alias("historical_max_amount"),
            )
        )

        self._logger.info("Built historical profile table")
        return profile_df

    def get_final_enriched_stream_df(self) -> DataFrame:
        """Return streaming DataFrame enriched with historical user behavior features."""
        streaming_enriched_df = self.get_enriched_stream_df()

        stream_with_join_key_df = streaming_enriched_df.withColumn(
            "historical_user_join_key",
            coalesce(
                when(trim(col("user_id")) != "", trim(col("user_id"))),
                when(trim(col("account_id")) != "", trim(col("account_id"))),
            ),
        )

        historical_df = self._load_historical_transactions_df()
        historical_profile_df = self._build_historical_profile_df(historical_df)

        final_enriched_df = stream_with_join_key_df.join(
            historical_profile_df,
            on="historical_user_join_key",
            how="left",
        )

        self._logger.info("Historical enrichment joined to real-time stream")
        return final_enriched_df

    def get_scored_stream_df(self) -> DataFrame:
        """Return final stream appended with anomaly and fraud scoring columns."""
        enriched_df = self.get_final_enriched_stream_df()
        model = self._anomaly_artifact["model"]
        feature_columns = list(self._anomaly_artifact["feature_columns"])
        fraud_model = self._fraud_artifact["model"]
        fraud_feature_columns = list(self._fraud_artifact["feature_columns"])
        fraud_threshold = float(self.settings.fraud_prediction_threshold)

        missing_features = sorted(set(feature_columns) - set(enriched_df.columns))
        if missing_features:
            raise ValueError(
                "Streaming DataFrame is missing anomaly feature columns: " + ", ".join(missing_features)
            )

        missing_fraud_features = sorted(set(fraud_feature_columns) - set(enriched_df.columns))
        if missing_fraud_features:
            raise ValueError(
                "Streaming DataFrame is missing fraud feature columns: " + ", ".join(missing_fraud_features)
            )

        score_schema = StructType(
            [
                StructField("anomaly_score", DoubleType(), False),
                StructField("is_anomaly", IntegerType(), False),
            ]
        )

        fraud_schema = StructType(
            [
                StructField("fraud_probability", DoubleType(), False),
                StructField("fraud_prediction", IntegerType(), False),
            ]
        )

        model_broadcast = self.spark.sparkContext.broadcast(model)
        fraud_model_broadcast = self.spark.sparkContext.broadcast(fraud_model)

        @pandas_udf(score_schema)
        def score_anomaly_udf(feature_structs: pd.Series) -> pd.DataFrame:
            features = pd.DataFrame(feature_structs.tolist())
            features = features[feature_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)

            current_model = model_broadcast.value
            anomaly_scores = -current_model.decision_function(features)
            anomaly_predictions = current_model.predict(features)
            anomaly_flags = (anomaly_predictions == -1).astype(int)

            return pd.DataFrame(
                {
                    "anomaly_score": anomaly_scores.astype(float),
                    "is_anomaly": anomaly_flags.astype(int),
                }
            )

        @pandas_udf(fraud_schema)
        def score_fraud_udf(feature_structs: pd.Series) -> pd.DataFrame:
            features = pd.DataFrame(feature_structs.tolist())
            features = features[fraud_feature_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)

            current_model = fraud_model_broadcast.value
            if hasattr(current_model, "predict_proba"):
                probabilities = current_model.predict_proba(features)
                if probabilities.ndim == 2 and probabilities.shape[1] >= 2:
                    positive_probability = probabilities[:, 1]
                else:
                    positive_probability = probabilities.ravel()
            else:
                positive_probability = current_model.predict(features)

            positive_probability = pd.Series(positive_probability).clip(0.0, 1.0)
            fraud_prediction = (positive_probability >= fraud_threshold).astype(int)

            return pd.DataFrame(
                {
                    "fraud_probability": positive_probability.astype(float),
                    "fraud_prediction": fraud_prediction.astype(int),
                }
            )

        score_input = struct(*[col(feature).cast(DoubleType()).alias(feature) for feature in feature_columns])
        fraud_score_input = struct(
            *[col(feature).cast(DoubleType()).alias(feature) for feature in fraud_feature_columns]
        )
        scored_anomaly_df = (
            enriched_df.withColumn("anomaly_result", score_anomaly_udf(score_input))
            .withColumn("anomaly_score", col("anomaly_result.anomaly_score"))
            .withColumn("is_anomaly", col("anomaly_result.is_anomaly"))
            .drop("anomaly_result")
        )
        scored_df = (
            scored_anomaly_df.withColumn("fraud_result", score_fraud_udf(fraud_score_input))
            .withColumn("fraud_probability", col("fraud_result.fraud_probability"))
            .withColumn("fraud_prediction", col("fraud_result.fraud_prediction"))
            .drop("fraud_result")
        )

        self._logger.info("Anomaly and fraud scoring appended to streaming DataFrame")
        return scored_df

    def _log_scored_batch_summary(self, batch_df: DataFrame, batch_id: int) -> None:
        """Log compact batch metrics for production monitoring."""
        batch_timestamp = datetime.now(timezone.utc).isoformat()

        if batch_df.rdd.isEmpty():
            self._logger.info(
                "Batch batch_id=%s timestamp=%s has no records",
                batch_id,
                batch_timestamp,
            )
            return

        summary_row = batch_df.agg(
            count("*").alias("records_processed"),
            spark_sum(col("is_anomaly")).alias("anomalies_detected"),
            spark_sum(col("fraud_prediction")).alias("fraud_predictions"),
        ).collect()[0]

        records_processed = int(summary_row["records_processed"] or 0)
        anomalies_detected = int(summary_row["anomalies_detected"] or 0)
        fraud_predictions = int(summary_row["fraud_predictions"] or 0)

        self._logger.info(
            "Batch batch_id=%s timestamp=%s records_processed=%d anomalies_detected=%d fraud_predictions=%d",
            batch_id,
            batch_timestamp,
            records_processed,
            anomalies_detected,
            fraud_predictions,
        )

        graph_update_frame = batch_df.select(
            "transaction_id",
            "user_id",
            "account_id",
            "merchant_id",
            "amount",
            "timestamp",
            "is_anomaly",
            "fraud_probability",
            "fraud_prediction",
            "anomaly_score",
        ).toPandas()

        graph_stats = self._graph_builder.update_from_pandas(
            frame=graph_update_frame,
            batch_id=batch_id,
        )
        pagerank_scores = self._graph_builder.compute_pagerank()
        community_assignments = self._graph_builder.compute_communities()
        triangle_counts = self._graph_builder.compute_triangle_counts()
        component_assignments, component_sizes = self._graph_builder.compute_connected_components()
        propagated_risk_scores = self._graph_builder.compute_risk_propagation()
        graph_update_frame["customer_graph_node_id"] = graph_update_frame.apply(
            self._resolve_customer_graph_node_id,
            axis=1,
        )
        graph_update_frame["merchant_graph_node_id"] = graph_update_frame.apply(
            self._resolve_merchant_graph_node_id,
            axis=1,
        )
        graph_update_frame["pagerank_score"] = graph_update_frame["customer_graph_node_id"].map(
            lambda node_id: float(pagerank_scores.get(node_id, 0.0)) if isinstance(node_id, str) else 0.0
        )
        graph_update_frame["customer_community_id"] = graph_update_frame["customer_graph_node_id"].map(
            lambda node_id: community_assignments.get(node_id) if isinstance(node_id, str) else None
        )
        graph_update_frame["merchant_community_id"] = graph_update_frame["merchant_graph_node_id"].map(
            lambda node_id: community_assignments.get(node_id) if isinstance(node_id, str) else None
        )
        graph_update_frame["community_id"] = graph_update_frame["customer_community_id"].where(
            graph_update_frame["customer_community_id"].notna(),
            graph_update_frame["merchant_community_id"],
        )
        graph_update_frame["customer_triangle_count"] = graph_update_frame["customer_graph_node_id"].map(
            lambda node_id: int(triangle_counts.get(node_id, 0)) if isinstance(node_id, str) else 0
        )
        graph_update_frame["merchant_triangle_count"] = graph_update_frame["merchant_graph_node_id"].map(
            lambda node_id: int(triangle_counts.get(node_id, 0)) if isinstance(node_id, str) else 0
        )
        graph_update_frame["triangle_count"] = graph_update_frame[
            ["customer_triangle_count", "merchant_triangle_count"]
        ].max(axis=1)
        graph_update_frame["customer_component_id"] = graph_update_frame["customer_graph_node_id"].map(
            lambda node_id: component_assignments.get(node_id) if isinstance(node_id, str) else None
        )
        graph_update_frame["merchant_component_id"] = graph_update_frame["merchant_graph_node_id"].map(
            lambda node_id: component_assignments.get(node_id) if isinstance(node_id, str) else None
        )
        graph_update_frame["component_id"] = graph_update_frame["customer_component_id"].where(
            graph_update_frame["customer_component_id"].notna(),
            graph_update_frame["merchant_component_id"],
        )
        graph_update_frame["customer_component_size"] = graph_update_frame["customer_component_id"].map(
            lambda component_id: int(component_sizes.get(int(component_id), 0)) if pd.notna(component_id) else 0
        )
        graph_update_frame["merchant_component_size"] = graph_update_frame["merchant_component_id"].map(
            lambda component_id: int(component_sizes.get(int(component_id), 0)) if pd.notna(component_id) else 0
        )
        graph_update_frame["component_size"] = graph_update_frame[
            ["customer_component_size", "merchant_component_size"]
        ].max(axis=1)
        graph_update_frame["customer_propagated_risk_score"] = graph_update_frame["customer_graph_node_id"].map(
            lambda node_id: float(propagated_risk_scores.get(node_id, 0.0)) if isinstance(node_id, str) else 0.0
        )
        graph_update_frame["merchant_propagated_risk_score"] = graph_update_frame["merchant_graph_node_id"].map(
            lambda node_id: float(propagated_risk_scores.get(node_id, 0.0)) if isinstance(node_id, str) else 0.0
        )
        graph_update_frame["propagated_risk_score"] = graph_update_frame[
            ["customer_propagated_risk_score", "merchant_propagated_risk_score"]
        ].max(axis=1)

        alert_timestamp = datetime.now(timezone.utc).isoformat()
        graph_update_frame[["alert_triggered", "alert_reason", "alert_severity"]] = graph_update_frame.apply(
            lambda row: pd.Series(self._evaluate_alert_row(row)),
            axis=1,
        )
        graph_update_frame["alert_timestamp"] = graph_update_frame["alert_triggered"].map(
            lambda triggered: alert_timestamp if bool(triggered) else None
        )

        top_ranked_nodes = sorted(
            pagerank_scores.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        top_ranked_nodes_text = ", ".join(
            [f"{node_id}:{score:.6f}" for node_id, score in top_ranked_nodes]
        )

        community_size_counter = Counter(community_assignments.values())
        top_communities = community_size_counter.most_common(5)
        top_communities_text = ", ".join(
            [f"community_{community_id}:{size}" for community_id, size in top_communities]
        )
        top_triangle_nodes = sorted(
            triangle_counts.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        top_triangle_nodes_text = ", ".join(
            [f"{node_id}:{count}" for node_id, count in top_triangle_nodes]
        )
        largest_components = sorted(
            component_sizes.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        largest_components_text = ", ".join(
            [f"component_{component_id}:{size}" for component_id, size in largest_components]
        )
        top_propagated_risk_nodes = sorted(
            propagated_risk_scores.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        top_propagated_risk_nodes_text = ", ".join(
            [f"{node_id}:{score:.6f}" for node_id, score in top_propagated_risk_nodes]
        )

        total_alerts = int(graph_update_frame["alert_triggered"].fillna(False).astype(bool).sum())
        severity_counts = (
            graph_update_frame.loc[
                graph_update_frame["alert_triggered"].fillna(False).astype(bool),
                "alert_severity",
            ]
            .value_counts()
            .to_dict()
        )
        low_alerts = int(severity_counts.get("Low", 0))
        medium_alerts = int(severity_counts.get("Medium", 0))
        high_alerts = int(severity_counts.get("High", 0))

        self._logger.info(
            "Graph update batch_id=%d skipped_rows=%d new_nodes=%d new_edges=%d total_nodes=%d total_edges=%d",
            graph_stats.batch_id,
            graph_stats.skipped_rows,
            graph_stats.new_nodes,
            graph_stats.new_edges,
            graph_stats.total_nodes,
            graph_stats.total_edges,
        )
        self._logger.info(
            "Top PageRank nodes batch_id=%d nodes=%s",
            graph_stats.batch_id,
            top_ranked_nodes_text or "none",
        )
        self._logger.info(
            "Communities batch_id=%d count=%d top_sizes=%s",
            graph_stats.batch_id,
            len(community_size_counter),
            top_communities_text or "none",
        )
        self._logger.info(
            "Triangle counts batch_id=%d top_nodes=%s",
            graph_stats.batch_id,
            top_triangle_nodes_text or "none",
        )
        self._logger.info(
            "Connected components batch_id=%d count=%d largest=%s",
            graph_stats.batch_id,
            len(component_sizes),
            largest_components_text or "none",
        )
        self._logger.info(
            "Risk propagation batch_id=%d top_nodes=%s",
            graph_stats.batch_id,
            top_propagated_risk_nodes_text or "none",
        )
        self._logger.info(
            "Alerts batch_id=%d total_records=%d total_alerts=%d severity_counts={Low:%d, Medium:%d, High:%d}",
            graph_stats.batch_id,
            records_processed,
            total_alerts,
            low_alerts,
            medium_alerts,
            high_alerts,
        )

        self._persist_batch_to_db(graph_update_frame, batch_id)
        self._publish_alerts_to_kafka(graph_update_frame, batch_id)

    def _init_audit_store(self):
        """Initialise the shared audit repository; log and re-raise on failure."""
        from api.repository import get_repository
        try:
            repo = get_repository(db_path=self.settings.api_db_path)
            self._logger.info(
                "Audit store connected db_path=%s", self.settings.api_db_path
            )
            return repo
        except Exception:
            self._logger.exception(
                "Failed to initialise audit store db_path=%s — batch writes disabled",
                self.settings.api_db_path,
            )
            return None

    def _persist_batch_to_db(self, frame: pd.DataFrame, batch_id: int) -> None:
        """Write scored transactions and triggered alerts to the audit database.

        Both writes use batch executemany inside a single transaction.
        Write failures are logged and never propagate — the streaming query
        continues uninterrupted.
        """
        if self._audit_store is None:
            return

        # --- scored_transactions (all rows) ---
        try:
            inserted_txns = self._audit_store.insert_transaction_batch(
                frame=frame, batch_id=batch_id
            )
            self._logger.info(
                "Audit write batch_id=%d table=scored_transactions inserted=%d",
                batch_id,
                inserted_txns,
            )
        except Exception:
            self._logger.exception(
                "Audit write failed batch_id=%d table=scored_transactions",
                batch_id,
            )

        # --- fraud_alerts (alert rows only) ---
        try:
            inserted_alerts = self._audit_store.insert_alert_batch(
                frame=frame, batch_id=batch_id
            )
            self._logger.info(
                "Audit write batch_id=%d table=fraud_alerts inserted=%d",
                batch_id,
                inserted_alerts,
            )
        except Exception:
            self._logger.exception(
                "Audit write failed batch_id=%d table=fraud_alerts",
                batch_id,
            )

    def _build_alert_producer(self) -> None:
        """Lazily initialise the Kafka producer used for the fraud-alerts topic."""
        if self._alert_producer is not None:
            return
        import importlib

        kafka_module = importlib.import_module("kafka")
        kafka_producer_cls = getattr(kafka_module, "KafkaProducer")
        self._alert_producer = kafka_producer_cls(
            bootstrap_servers=self.settings.alert_kafka_bootstrap_servers,
            value_serializer=lambda message: json.dumps(message, default=str).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8") if key else b"unknown",
        )
        self._logger.info(
            "Alert Kafka producer initialised bootstrap_servers=%s topic=%s",
            self.settings.alert_kafka_bootstrap_servers,
            self.settings.alert_kafka_topic,
        )

    def _publish_alerts_to_kafka(self, frame: pd.DataFrame, batch_id: int) -> None:
        """Filter triggered alerts from batch frame and publish each to the fraud_alerts topic."""
        alert_frame = frame[frame["alert_triggered"].fillna(False).astype(bool)]
        if alert_frame.empty:
            return

        try:
            self._build_alert_producer()
        except Exception:
            self._logger.exception(
                "Alert Kafka producer failed to initialise batch_id=%d — alerts not published",
                batch_id,
            )
            return

        topic = self.settings.alert_kafka_topic
        published = 0
        failed = 0

        for _, row in alert_frame.iterrows():
            message: dict = {}
            for field in self._ALERT_FIELDS:
                value = row.get(field)
                message[field] = None if (value is None or (isinstance(value, float) and pd.isna(value))) else value

            partition_key = (
                str(message["transaction_id"]).strip()
                if message.get("transaction_id")
                else None
            )

            try:
                self._alert_producer.send(topic, key=partition_key, value=message).get(timeout=10)
                published += 1
            except Exception:
                failed += 1
                self._logger.exception(
                    "Failed to publish alert to Kafka batch_id=%d topic=%s transaction_id=%s",
                    batch_id,
                    topic,
                    message.get("transaction_id"),
                )

        if published:
            self._alert_producer.flush()

        self._logger.info(
            "Alert publish batch_id=%d topic=%s published=%d failed=%d",
            batch_id,
            topic,
            published,
            failed,
        )

    def _evaluate_alert_row(self, row: pd.Series) -> tuple[bool, str | None, str | None]:
        """Evaluate deterministic alert conditions from final transaction risk signals."""
        fraud_probability = self._safe_float_from_row(row, "fraud_probability")
        anomaly_score = self._safe_float_from_row(row, "anomaly_score")
        propagated_risk_score = self._safe_float_from_row(row, "propagated_risk_score")

        reasons: list[str] = []
        if fraud_probability >= float(self.settings.alert_fraud_probability_threshold):
            reasons.append("fraud_probability")
        if anomaly_score >= float(self.settings.alert_anomaly_score_threshold):
            reasons.append("anomaly_score")
        if propagated_risk_score >= float(self.settings.alert_propagated_risk_threshold):
            reasons.append("propagated_risk_score")

        if not reasons:
            return False, None, None

        required_high_signals = max(1, int(self.settings.alert_high_severity_signal_count))
        if len(reasons) >= required_high_signals:
            severity = "High"
        elif "fraud_probability" in reasons or "propagated_risk_score" in reasons:
            severity = "Medium"
        else:
            severity = "Low"

        return True, ", ".join(reasons), severity

    @staticmethod
    def _safe_float_from_row(row: pd.Series, field_name: str) -> float:
        """Read numeric field from pandas row with safe fallback to 0.0."""
        value = row.get(field_name)
        if value is None or pd.isna(value):
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _resolve_customer_graph_node_id(row: pd.Series) -> str | None:
        """Resolve graph customer node id using user_id with account_id fallback."""
        user_id = row.get("user_id")
        if user_id is not None and not pd.isna(user_id):
            user_id_value = str(user_id).strip()
            if user_id_value:
                return f"customer:{user_id_value}"

        account_id = row.get("account_id")
        if account_id is not None and not pd.isna(account_id):
            account_id_value = str(account_id).strip()
            if account_id_value:
                return f"customer:{account_id_value}"

        return None

    @staticmethod
    def _resolve_merchant_graph_node_id(row: pd.Series) -> str | None:
        """Resolve graph merchant node id from merchant_id when available."""
        merchant_id = row.get("merchant_id")
        if merchant_id is None or pd.isna(merchant_id):
            return None

        merchant_id_value = str(merchant_id).strip()
        if not merchant_id_value:
            return None

        return f"merchant:{merchant_id_value}"

    def start_stream(self) -> StreamingQuery:
        """Start final stream with real-time, historical, and anomaly enrichment."""
        enriched_df = self.get_scored_stream_df()

        try:
            query = (
                enriched_df.writeStream.outputMode("append")
                .option("checkpointLocation", self.settings.spark_checkpoint_location)
                .foreachBatch(self._log_scored_batch_summary)
                .start()
            )
            self._logger.info("Anomaly and fraud scoring stream started")
            self._logger.info("Active query status: %s", query.status)
            return query
        except Exception:
            self._logger.exception("Failed to start parsed transaction stream")
            raise

    def await_termination(self, query: StreamingQuery) -> None:
        """Block until termination and raise/log if query fails."""
        try:
            query.awaitTermination()
        except Exception:
            self._logger.exception("Streaming query terminated with error")
            raise

        if query.exception() is not None:
            self._logger.error("Streaming query exception: %s", query.exception())
            raise RuntimeError(str(query.exception()))


def create_spark_session(app_name: str = "fraud_detection_system") -> SparkSession:
    """Create and return a Spark session configured for streaming."""
    settings = get_settings()
    if app_name != settings.project_name:
        settings = settings.model_copy(update={"project_name": app_name})
    return TransactionSparkPipeline(settings=settings).spark


def run_streaming_pipeline() -> None:
    """Entry point for Kafka read, feature enrichment, and anomaly scoring."""
    pipeline = TransactionSparkPipeline()
    query = pipeline.start_stream()
    pipeline.await_termination(query)
