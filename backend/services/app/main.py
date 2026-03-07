from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routers import connectors, experiments, exports, health, imports, mappings, predictions
from app.core.db import init_db
from app.core.settings import get_settings


def create_app() -> FastAPI:
    settings = get_settings()
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

    @app.get("/")
    def root():
        return {"service": settings.app_name, "api_prefix": settings.api_v1_prefix}

    app.include_router(health.router, prefix=settings.api_v1_prefix)
    app.include_router(connectors.router, prefix=settings.api_v1_prefix)
    app.include_router(mappings.router, prefix=settings.api_v1_prefix)
    app.include_router(imports.router, prefix=settings.api_v1_prefix)
    app.include_router(predictions.router, prefix=settings.api_v1_prefix)
    app.include_router(exports.router, prefix=settings.api_v1_prefix)
    app.include_router(experiments.router, prefix=settings.api_v1_prefix)
    return app


app = create_app()
