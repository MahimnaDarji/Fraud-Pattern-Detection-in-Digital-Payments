"""FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes.alerts import router as alerts_router
from api.routes.metrics import router as metrics_router
from utils.logger import get_logger

_logger = get_logger(__name__)


def create_app() -> FastAPI:
    """Build and return the configured FastAPI application instance.

    Separating app creation into a factory function makes the app importable
    for testing (``TestClient(create_app())``) without side-effects.
    """
    app = FastAPI(
        title="Fraud Detection Alert API",
        description=(
            "Real-time API for querying triggered fraud alerts and investigation metrics. "
            "Data is written by the streaming pipeline and queryable immediately."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # ------------------------------------------------------------------
    # CORS — allow all origins by default; tighten in production via env.
    # ------------------------------------------------------------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------------
    # Global exception handler — ensures all 500s return JSON, not HTML.
    # ------------------------------------------------------------------
    @app.exception_handler(Exception)
    async def _global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        _logger.exception("Unhandled exception path=%s", request.url.path)
        return JSONResponse(
            status_code=500,
            content={"detail": "An internal server error occurred."},
        )

    # ------------------------------------------------------------------
    # Routers
    # ------------------------------------------------------------------
    app.include_router(alerts_router)
    app.include_router(metrics_router)

    # ------------------------------------------------------------------
    # Health check — responds before DB is initialised so load-balancers
    # can probe the process immediately after startup.
    # ------------------------------------------------------------------
    @app.get("/health", tags=["Health"], summary="Health check")
    async def health() -> dict:
        return {"status": "ok"}

    _logger.info("FastAPI app created routes=%d", len(app.routes))
    return app
