"""Metrics route handler — GET /metrics."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from api.models import MetricsResponse
from api.service import AlertService
from api.dependencies import get_alert_service

router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get(
    "",
    response_model=MetricsResponse,
    summary="Alert summary statistics",
    description=(
        "Returns aggregate statistics over all stored fraud alerts: "
        "total count, severity breakdown, top-5 merchants by alert count, "
        "and mean risk scores."
    ),
)
def get_metrics(
    service: Annotated[AlertService, Depends(get_alert_service)],
) -> MetricsResponse:
    return service.get_metrics()
