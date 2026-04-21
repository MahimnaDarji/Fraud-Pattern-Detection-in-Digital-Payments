"""Alert route handlers — GET /alerts and GET /alerts/{transaction_id}."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from api.models import AlertListResponse, AlertResponse
from api.service import AlertService
from api.dependencies import get_alert_service

router = APIRouter(prefix="/alerts", tags=["Alerts"])


@router.get(
    "",
    response_model=AlertListResponse,
    summary="List recent fraud alerts",
    description=(
        "Returns stored fraud alerts ordered by receipt time descending. "
        "Supports pagination via `limit` and `offset`, and optional filtering by `severity`."
    ),
)
def list_alerts(
    service: Annotated[AlertService, Depends(get_alert_service)],
    limit: Annotated[int, Query(ge=1, le=500, description="Maximum records to return")] = 50,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
    severity: Annotated[
        str | None,
        Query(description="Filter by severity: High, Medium, or Low"),
    ] = None,
) -> AlertListResponse:
    return service.list_alerts(limit=limit, offset=offset, severity=severity)


@router.get(
    "/{transaction_id}",
    response_model=AlertResponse,
    summary="Get alert detail by transaction ID",
    description="Returns the full alert record for the given `transaction_id`. Returns 404 if not found.",
)
def get_alert(
    transaction_id: str,
    service: Annotated[AlertService, Depends(get_alert_service)],
) -> AlertResponse:
    return service.get_alert(transaction_id)
