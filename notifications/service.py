"""Alert notification service: wires channels to the fraud_alerts Kafka consumer."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Protocol

from api.repository import get_repository
from config.settings import AppSettings, get_settings
from notifications.channels.email_channel import SmtpEmailChannel
from notifications.channels.webhook_channel import HttpWebhookChannel
from notifications.consumer import AlertKafkaConsumer
from notifications.models import AlertPayload
from utils.logger import configure_logging, get_logger


# ---------------------------------------------------------------------------
# Channel protocol — any object with a deliver(AlertPayload) -> bool method
# ---------------------------------------------------------------------------

class NotificationChannel(Protocol):
    """Structural protocol for a pluggable delivery channel."""

    def deliver(self, payload: AlertPayload) -> bool:
        """Deliver *payload* and return True on success, False on failure."""
        ...


# ---------------------------------------------------------------------------
# AlertNotifier
# ---------------------------------------------------------------------------

class AlertNotifier:
    """Dispatch a single AlertPayload concurrently to all registered channels.

    Channels are registered at construction time via keyword arguments and
    built from ``AppSettings`` values. You can also pass pre-built channel
    instances through ``extra_channels`` for testing or custom integrations.
    """

    def __init__(
        self,
        settings: AppSettings | None = None,
        extra_channels: list[Any] | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._logger = get_logger(__name__)
        self._channels: list[tuple[str, Any]] = []  # (name, channel)
        self._counts: dict[str, int] = defaultdict(int)

        self._register_channels_from_settings()
        for ch in (extra_channels or []):
            self._channels.append((type(ch).__name__, ch))

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def notify(self, payload: AlertPayload) -> None:
        """Deliver *payload* to every registered channel and update counters."""
        if not self._channels:
            self._logger.warning(
                "No notification channels configured — alert not delivered %s",
                payload.identity,
            )
            return

        for channel_name, channel in self._channels:
            try:
                ok = channel.deliver(payload)
            except Exception:
                ok = False
                self._logger.exception(
                    "Unhandled exception in channel=%s %s",
                    channel_name,
                    payload.identity,
                )

            if ok:
                self._counts[f"{channel_name}:success"] += 1
            else:
                self._counts[f"{channel_name}:failure"] += 1

    def log_channel_summary(self) -> None:
        """Log cumulative delivery counts for all channels to the structured logger."""
        if not self._counts:
            self._logger.info("Notification summary: no alerts delivered yet")
            return
        for label, count in sorted(self._counts.items()):
            self._logger.info("Notification summary channel=%s count=%d", label, count)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _register_channels_from_settings(self) -> None:
        """Build and register channels based on the active AppSettings."""
        s = self._settings

        # E-mail channel — only registered when SMTP host is configured
        if s.notification_smtp_host:
            recipients = [r.strip() for r in s.notification_email_recipients.split(",") if r.strip()]
            if recipients:
                email_ch = SmtpEmailChannel(
                    host=s.notification_smtp_host,
                    port=s.notification_smtp_port,
                    username=s.notification_smtp_username,
                    password=s.notification_smtp_password,
                    sender=s.notification_smtp_sender,
                    recipients=recipients,
                    use_tls=s.notification_smtp_use_tls,
                )
                self._channels.append(("SmtpEmailChannel", email_ch))
                self._logger.info(
                    "Email channel registered smtp_host=%s recipients=%d",
                    s.notification_smtp_host,
                    len(recipients),
                )
            else:
                self._logger.warning(
                    "SMTP host is set but no valid recipients configured — email channel skipped"
                )

        # Webhook channel — only registered when at least one URL is configured
        webhook_urls = [u.strip() for u in s.notification_webhook_urls.split(",") if u.strip()]
        if webhook_urls:
            webhook_ch = HttpWebhookChannel(
                endpoints=webhook_urls,
                timeout_seconds=s.notification_webhook_timeout_seconds,
            )
            self._channels.append(("HttpWebhookChannel", webhook_ch))
            self._logger.info(
                "Webhook channel registered endpoints=%d",
                len(webhook_urls),
            )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_notification_service(settings: AppSettings | None = None) -> None:
    """Start the blocking notification service loop.

    Reads from the ``fraud_alerts`` Kafka topic, persists each alert to the
    shared SQLite database so the API can serve it immediately, then dispatches
    it through all configured notification channels.

    This function blocks indefinitely and is intended to run in a dedicated
    thread or process.

    Example::

        import threading
        from notifications.service import run_notification_service
        t = threading.Thread(target=run_notification_service, daemon=True)
        t.start()
    """
    cfg = settings or get_settings()
    configure_logging(debug_mode=cfg.debug_mode)
    logger = get_logger(__name__)

    repo = get_repository(db_path=cfg.api_db_path)
    notifier = AlertNotifier(settings=cfg)
    consumer = AlertKafkaConsumer(settings=cfg)

    logger.info(
        "Notification service started topic=%s channels=%d db_path=%s",
        cfg.alert_kafka_topic,
        len(notifier._channels),
        cfg.api_db_path,
    )

    alert_count = 0
    try:
        for payload in consumer.consume():
            alert_count += 1
            logger.info(
                "Alert received count=%d %s",
                alert_count,
                payload.identity,
            )

            # Persist to SQLite so the API can serve the alert immediately.
            try:
                repo.insert_alert(payload)
            except Exception:
                logger.exception(
                    "Failed to persist alert to DB %s",
                    payload.identity,
                )

            notifier.notify(payload)

            # Log a rolling channel summary every 100 alerts
            if alert_count % 100 == 0:
                notifier.log_channel_summary()
    except KeyboardInterrupt:
        logger.info("Notification service interrupted — shutting down")
    finally:
        consumer.close()
        notifier.log_channel_summary()
        logger.info("Notification service stopped total_alerts_processed=%d", alert_count)
