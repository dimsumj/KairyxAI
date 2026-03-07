from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse

from app.api.routers import connectors, experiments, exports, health, imports, mappings, predictions
from app.application.imports import ImportService
from app.core.db import get_session_factory, init_db
from app.core.settings import get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository


def create_app() -> FastAPI:
    settings = get_settings()
    frontend_dir = Path(__file__).resolve().parents[3] / "frontend"
    frontend_index = frontend_dir / "index.html"
    app = FastAPI(title=settings.app_name)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    def _startup() -> None:
        init_db()
        session = get_session_factory()()
        try:
            repository = SqlAlchemyControlPlaneRepository(session)
            ImportService(repository, settings).reconcile_jobs_after_restart()
        finally:
            session.close()

    @app.get("/")
    def root():
        response = FileResponse(frontend_index)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.get("/health")
    def root_health():
        return health.health()

    app.include_router(health.router, prefix=settings.api_v1_prefix)
    app.include_router(connectors.router, prefix=settings.api_v1_prefix)
    app.include_router(mappings.router, prefix=settings.api_v1_prefix)
    app.include_router(imports.router, prefix=settings.api_v1_prefix)
    app.include_router(predictions.router, prefix=settings.api_v1_prefix)
    app.include_router(exports.router, prefix=settings.api_v1_prefix)
    app.include_router(experiments.router, prefix=settings.api_v1_prefix)
    return app


app = create_app()
