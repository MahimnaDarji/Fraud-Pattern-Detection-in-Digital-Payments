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
    dataset_path: str = "final_dataset.parquet"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "transactions"
    kafka_group_id: str = "fraud-consumers"
    producer_rows_per_second: float = 10.0
    producer_send_delay_seconds: float = 0.0
    producer_log_every_n: int = 1000
    producer_dry_run: bool = False

    @classmethod
    def from_env(cls) -> "AppSettings":
        """Load settings from environment variables with safe defaults."""
        load_dotenv()
        return cls(
            project_name=os.getenv("PROJECT_NAME", "fraud_detection_system"),
            dataset_path=os.getenv("DATASET_PATH", "final_dataset.parquet"),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "transactions"),
            kafka_group_id=os.getenv("KAFKA_GROUP_ID", "fraud-consumers"),
            producer_rows_per_second=float(os.getenv("PRODUCER_ROWS_PER_SECOND", "10")),
            producer_send_delay_seconds=float(os.getenv("PRODUCER_SEND_DELAY_SECONDS", "0")),
            producer_log_every_n=int(os.getenv("PRODUCER_LOG_EVERY_N", "1000")),
            producer_dry_run=_env_bool("PRODUCER_DRY_RUN", False),
        )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return cached application settings."""
    return AppSettings.from_env()
