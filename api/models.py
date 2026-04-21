"""Pydantic response schemas for the fraud alert API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class AlertResponse(BaseModel):
    """Full alert record returned by GET /alerts and GET /alerts/{transaction_id}."""

    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(..., description="Internal row identifier")
    transaction_id: str | None = Field(None, description="Unique transaction identifier")
    user_id: str | None = Field(None, description="User identifier")
    account_id: str | None = Field(None, description="Account identifier")
    merchant_id: str | None = Field(None, description="Merchant identifier")
    amount: float | None = Field(None, description="Transaction amount")
    timestamp: str | None = Field(None, description="Transaction timestamp (ISO 8601)")
    fraud_probability: float | None = Field(None, description="Supervised fraud model score [0–1]")
    anomaly_score: float | None = Field(None, description="Isolation Forest anomaly score")
    propagated_risk_score: float | None = Field(None, description="Graph-propagated risk score [0–1]")
    alert_reason: str | None = Field(None, description="Comma-separated signal names that triggered the alert")
    alert_severity: str | None = Field(None, description="Alert severity: Low | Medium | High")
    alert_timestamp: str | None = Field(None, description="UTC timestamp when alert was generated")
    pagerank_score: float | None = Field(None, description="Customer node PageRank in transaction graph")
    community_id: float | None = Field(None, description="Graph community the customer node belongs to")
    triangle_count: int | None = Field(None, description="Triangle count for customer or merchant node")
    component_id: float | None = Field(None, description="Connected-component identifier")
    component_size: int | None = Field(None, description="Size of the connected component")
    batch_id: int | None = Field(None, description="Streaming micro-batch that produced this alert")
    received_at: str | None = Field(None, description="UTC timestamp when the alert was stored")


class AlertListResponse(BaseModel):
    """Paginated list of alerts with total count."""

    total: int = Field(..., description="Total number of stored alerts matching the query")
    limit: int = Field(..., description="Maximum records returned in this response")
    offset: int = Field(..., description="Pagination offset applied")
    alerts: list[AlertResponse] = Field(..., description="Alert records")


class SeverityBreakdown(BaseModel):
    """Alert counts grouped by severity level."""

    high: int = Field(0, description="Number of High severity alerts")
    medium: int = Field(0, description="Number of Medium severity alerts")
    low: int = Field(0, description="Number of Low severity alerts")
    unknown: int = Field(0, description="Alerts with no severity assigned")


class MetricsResponse(BaseModel):
    """Summary statistics for the stored fraud alerts."""

    total_alerts: int = Field(..., description="Total number of stored triggered alerts")
    total_scored_transactions: int = Field(0, description="Total scored transactions in the audit trail")
    severity_breakdown: SeverityBreakdown = Field(..., description="Counts per severity level")
    top_merchants: list[dict[str, Any]] = Field(
        ..., description="Top 5 merchants by alert count [{merchant_id, alert_count}]"
    )
    avg_fraud_probability: float | None = Field(
        None, description="Mean fraud_probability across all stored alerts"
    )
    avg_anomaly_score: float | None = Field(
        None, description="Mean anomaly_score across all stored alerts"
    )
    avg_propagated_risk_score: float | None = Field(
        None, description="Mean propagated_risk_score across all stored alerts"
    )
