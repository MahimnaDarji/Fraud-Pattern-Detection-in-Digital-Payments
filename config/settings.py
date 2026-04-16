"""Environment-driven settings for local and production deployments."""

from functools import lru_cache
import os

from dotenv import load_dotenv
from pydantic import BaseModel


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment variable with common true/false forms."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class AppSettings(BaseModel):
    """Runtime settings used by Kafka, Spark, and model serving components."""

    project_name: str = "fraud_detection_system"
    debug_mode: bool = False
    dataset_path: str = "final_dataset.parquet"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "transactions"
    kafka_group_id: str = "fraud-consumers"
    producer_streaming_rate: float = 10.0
    producer_send_delay_seconds: float = 0.0
    producer_log_frequency: int = 1000
    log_frequency: int = 1000
    spark_kafka_topic: str = "transactions"
    spark_checkpoint_location: str = "artifacts/checkpoints/transactions_console"
    spark_starting_offsets: str = "latest"
    spark_output_mode: str = "append"
    spark_console_num_rows: int = 50
    spark_kafka_packages: str | None = None
    feature_high_amount_threshold: float = 1000.0
    feature_window_duration: str = "10 minutes"
    feature_slide_duration: str = "5 minutes"
    feature_watermark_delay: str = "30 minutes"
    anomaly_model_path: str = "artifacts/anomaly_model.pkl"
    fraud_model_path: str = "artifacts/fraud_model.pkl"
    fraud_prediction_threshold: float = 0.5
    alert_fraud_probability_threshold: float = 0.8
    alert_anomaly_score_threshold: float = 0.7
    alert_propagated_risk_threshold: float = 0.75
    alert_high_severity_signal_count: int = 2
    anomaly_contamination: float = 0.02
    anomaly_training_sample_size: int = 50000

    @classmethod
    def from_env(cls) -> "AppSettings":
        """Load settings from environment variables with safe defaults."""
        load_dotenv()
        return cls(
            project_name=os.getenv("PROJECT_NAME", "fraud_detection_system"),
            debug_mode=_env_bool("DEBUG_MODE", False),
            dataset_path=os.getenv("DATASET_PATH", "final_dataset.parquet"),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "transactions"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "fraud-consumers"),
            producer_streaming_rate=float(
                os.getenv("PRODUCER_STREAMING_RATE", os.getenv("PRODUCER_ROWS_PER_SECOND", "10"))
            ),
            producer_send_delay_seconds=float(os.getenv("PRODUCER_SEND_DELAY_SECONDS", "0")),
            producer_log_frequency=int(
                os.getenv("PRODUCER_LOG_FREQUENCY", os.getenv("PRODUCER_LOG_EVERY_N", "1000"))
            ),
            log_frequency=int(os.getenv("LOG_FREQUENCY", "1000")),
            spark_kafka_topic=os.getenv("SPARK_KAFKA_TOPIC", os.getenv("KAFKA_TOPIC", "transactions")),
            spark_checkpoint_location=os.getenv(
                "SPARK_CHECKPOINT_LOCATION",
                "artifacts/checkpoints/transactions_console",
            ),
            spark_starting_offsets=os.getenv("SPARK_STARTING_OFFSETS", "latest"),
            spark_output_mode=os.getenv("SPARK_OUTPUT_MODE", "append"),
            spark_console_num_rows=int(os.getenv("SPARK_CONSOLE_NUM_ROWS", "50")),
            spark_kafka_packages=os.getenv("SPARK_KAFKA_PACKAGES"),
            feature_high_amount_threshold=float(os.getenv("FEATURE_HIGH_AMOUNT_THRESHOLD", "1000")),
            feature_window_duration=os.getenv("FEATURE_WINDOW_DURATION", "10 minutes"),
            feature_slide_duration=os.getenv("FEATURE_SLIDE_DURATION", "5 minutes"),
            feature_watermark_delay=os.getenv("FEATURE_WATERMARK_DELAY", "30 minutes"),
            anomaly_model_path=os.getenv("ANOMALY_MODEL_PATH", "artifacts/anomaly_model.pkl"),
            fraud_model_path=os.getenv("FRAUD_MODEL_PATH", "artifacts/fraud_model.pkl"),
            fraud_prediction_threshold=float(os.getenv("FRAUD_PREDICTION_THRESHOLD", "0.5")),
            alert_fraud_probability_threshold=float(os.getenv("ALERT_FRAUD_PROBABILITY_THRESHOLD", "0.8")),
            alert_anomaly_score_threshold=float(os.getenv("ALERT_ANOMALY_SCORE_THRESHOLD", "0.7")),
            alert_propagated_risk_threshold=float(os.getenv("ALERT_PROPAGATED_RISK_THRESHOLD", "0.75")),
            alert_high_severity_signal_count=int(os.getenv("ALERT_HIGH_SEVERITY_SIGNAL_COUNT", "2")),
            anomaly_contamination=float(os.getenv("ANOMALY_CONTAMINATION", "0.02")),
            anomaly_training_sample_size=int(os.getenv("ANOMALY_TRAINING_SAMPLE_SIZE", "50000")),
        )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return cached application settings."""
    return AppSettings.from_env()
