"""Canonical alert payload passed to every notification channel."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


#: Ordered field names that must be present in a raw Kafka alert message.
ALERT_PAYLOAD_FIELDS: tuple[str, ...] = (
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
)


@dataclass(frozen=True, slots=True)
class AlertPayload:
    """Immutable, strongly-typed representation of a triggered fraud alert."""

    transaction_id: str | None
    user_id: str | None
    account_id: str | None
    merchant_id: str | None
    amount: float | None
    timestamp: str | None
    fraud_probability: float | None
    anomaly_score: float | None
    propagated_risk_score: float | None
    alert_reason: str | None
    alert_severity: str | None

    @classmethod
    def from_dict(cls, record: dict[str, Any]) -> "AlertPayload":
        """Build an AlertPayload from a raw Kafka message dict."""
        return cls(
            transaction_id=_str_or_none(record.get("transaction_id")),
            user_id=_str_or_none(record.get("user_id")),
            account_id=_str_or_none(record.get("account_id")),
            merchant_id=_str_or_none(record.get("merchant_id")),
            amount=_float_or_none(record.get("amount")),
            timestamp=_str_or_none(record.get("timestamp")),
            fraud_probability=_float_or_none(record.get("fraud_probability")),
            anomaly_score=_float_or_none(record.get("anomaly_score")),
            propagated_risk_score=_float_or_none(record.get("propagated_risk_score")),
            alert_reason=_str_or_none(record.get("alert_reason")),
            alert_severity=_str_or_none(record.get("alert_severity")),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dictionary representation of the alert payload."""
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "account_id": self.account_id,
            "merchant_id": self.merchant_id,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "fraud_probability": self.fraud_probability,
            "anomaly_score": self.anomaly_score,
            "propagated_risk_score": self.propagated_risk_score,
            "alert_reason": self.alert_reason,
            "alert_severity": self.alert_severity,
        }

    @property
    def identity(self) -> str:
        """Return a compact human-readable identity string for logging."""
        return (
            f"transaction_id={self.transaction_id} "
            f"severity={self.alert_severity} "
            f"reason={self.alert_reason}"
        )


# ---------------------------------------------------------------------------
# Private coercion helpers
# ---------------------------------------------------------------------------

def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
        return None if result != result else result  # guard against NaN
    except (TypeError, ValueError):
        return None
