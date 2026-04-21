"""FastAPI dependency providers for shared resources.

Using FastAPI's dependency injection rather than global singletons keeps
the service layer testable — tests can override ``get_alert_service`` with
``app.dependency_overrides`` to inject a mock repository.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from api.repository import AuditRepository, get_repository
from api.service import AlertService
from config.settings import AppSettings, get_settings


@lru_cache(maxsize=1)
def _get_repository(db_path: str) -> AlertRepository:
    """Return the shared AlertRepository instance (cached by db_path)."""
    return get_repository(db_path=db_path)


def get_alert_service(
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> AlertService:
    """Provide a fully wired AlertService to route handlers."""
    repo = _get_repository(db_path=settings.api_db_path)
    return AlertService(repository=repo)
