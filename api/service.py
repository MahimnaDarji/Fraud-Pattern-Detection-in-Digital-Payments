"""Business logic layer between routes and the alert repository."""

from __future__ import annotations

from fastapi import HTTPException

from api.models import (
    AlertListResponse,
    AlertResponse,
    MetricsResponse,
    SeverityBreakdown,
)
from api.repository import AuditRepository as AlertRepository


class AlertService:
    """Query, validate, and shape alert data for the API layer.

    All business rules live here — route handlers call exactly one method
    and return what they receive.
    """

    _MAX_LIMIT = 500

    def __init__(self, repository: AlertRepository) -> None:
        self._repo = repository

    # ------------------------------------------------------------------
    # GET /alerts
    # ------------------------------------------------------------------

    def list_alerts(
        self,
        limit: int = 50,
        offset: int = 0,
        severity: str | None = None,
    ) -> AlertListResponse:
        """Return a paginated, descending-time-ordered list of stored alerts."""
        limit = max(1, min(limit, self._MAX_LIMIT))
        offset = max(0, offset)

        if severity is not None:
            normalised = severity.strip().capitalize()
            if normalised not in {"High", "Medium", "Low"}:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid severity '{severity}'. Valid values: High, Medium, Low.",
                )
            severity = normalised

        total, rows = self._repo.list_alerts(limit=limit, offset=offset, severity=severity)
        return AlertListResponse(
            total=total,
            limit=limit,
            offset=offset,
            alerts=[AlertResponse(**row) for row in rows],
        )

    # ------------------------------------------------------------------
    # GET /alerts/{transaction_id}
    # ------------------------------------------------------------------

    def get_alert(self, transaction_id: str) -> AlertResponse:
        """Return a single alert by transaction_id or raise 404."""
        txn_id = transaction_id.strip()
        if not txn_id:
            raise HTTPException(status_code=422, detail="transaction_id cannot be empty.")

        row = self._repo.get_alert_by_transaction_id(txn_id)
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No alert found for transaction_id='{txn_id}'.",
            )
        return AlertResponse(**row)

    # ------------------------------------------------------------------
    # GET /metrics
    # ------------------------------------------------------------------

    def get_metrics(self) -> MetricsResponse:
        """Return aggregated alert statistics."""
        raw = self._repo.get_metrics()
        return MetricsResponse(
            total_alerts=raw["total_alerts"],
            total_scored_transactions=raw["total_scored_transactions"],
            severity_breakdown=SeverityBreakdown(
                high=raw["high_count"],
                medium=raw["medium_count"],
                low=raw["low_count"],
                unknown=raw["unknown_count"],
            ),
            top_merchants=raw["top_merchants"],
            avg_fraud_probability=raw["avg_fraud_probability"],
            avg_anomaly_score=raw["avg_anomaly_score"],
            avg_propagated_risk_score=raw["avg_propagated_risk_score"],
        )
