from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from starlette.responses import FileResponse, JSONResponse
from starlette.staticfiles import StaticFiles

from app.api.routers import activation, audit, cohorts, connectors, copilot, experiments, exports, health, imports, mappings, predictions, sql_workspace, templates, workflows
from app.application.imports import ImportService
from app.application.control_loop import ControlLoopService
from app.application.health_monitor import HealthMonitorService
from app.application.predictions import PredictionService
from app.core.db import get_session_factory, init_db
from app.core.errors import is_database_locked_error
from app.core.logging import configure_access_log_filters
from app.core.runtime import clear_shutdown_requested, mark_shutdown_requested
from app.core.settings import get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service


logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    settings = get_settings()
    frontend_dir = Path(__file__).resolve().parents[3] / "frontend"
    frontend_index = frontend_dir / "index.html"
    frontend_static_dir = frontend_dir / "assets"
    configure_access_log_filters()
    app = FastAPI(title=settings.app_name)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def _api_key_guard(request: Request, call_next):
        protected_prefix = settings.api_v1_prefix.rstrip("/")
        if settings.api_access_key and request.url.path.startswith(protected_prefix):
            if request.url.path not in {
                f"{protected_prefix}/health",
                f"{protected_prefix}/health/live",
                "/health",
                "/health/live",
            }:
                provided = str(request.headers.get("x-api-key") or "").strip()
                if provided != settings.api_access_key:
                    return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key."})
        return await call_next(request)

    @app.middleware("http")
    async def _sqlite_lock_guard(request: Request, call_next):
        try:
            return await call_next(request)
        except (SQLAlchemyOperationalError, sqlite3.OperationalError) as exc:
            if is_database_locked_error(exc):
                logger.warning("Control plane database lock while handling %s %s", request.method, request.url.path)
                return JSONResponse(
                    status_code=423,
                    headers={"Retry-After": "1"},
                    content={"detail": "Control plane database is busy; retry shortly."},
                )
            raise

    @app.on_event("startup")
    def _startup() -> None:
        if getattr(app.state, "restart_reconciliation_complete", False):
            return
        clear_shutdown_requested()
        clear_shared_bigquery_service_cache()
        init_db()
        session = get_session_factory()()
        try:
            repository = SqlAlchemyControlPlaneRepository(session)
            bigquery_service = get_shared_bigquery_service()
            try:
                ImportService(repository, settings, bigquery_service=bigquery_service).reconcile_jobs_after_restart()
                ImportService(repository, settings, bigquery_service=bigquery_service).cleanup_expired_jobs()
                PredictionService(repository, settings, bigquery_service=bigquery_service).cleanup_expired_jobs()
                ControlLoopService(repository, settings, bigquery_service).ensure_default_jobs()
            except Exception:
                logger.exception("Import restart reconciliation failed during startup. Continuing without blocking API startup.")
        finally:
            session.close()
        if getattr(app.state, "health_warmup_thread", None) is None:
            def _warm_health_snapshot() -> None:
                session = get_session_factory()()
                try:
                    repository = SqlAlchemyControlPlaneRepository(session)
                    HealthMonitorService(repository, get_shared_bigquery_service()).snapshot(persist=True)
                except Exception:
                    logger.exception("Health snapshot warm-up failed.")
                finally:
                    session.close()

            thread = threading.Thread(target=_warm_health_snapshot, name="kairyx-health-warmup", daemon=True)
            app.state.health_warmup_thread = thread
            thread.start()
        if settings.scheduler_enabled and getattr(app.state, "control_loop_thread", None) is None:
            stop_event = threading.Event()

            def _run_control_loop() -> None:
                while not stop_event.wait(settings.scheduler_interval_seconds):
                    session = get_session_factory()()
                    try:
                        repository = SqlAlchemyControlPlaneRepository(session)
                        ControlLoopService(repository, settings, get_shared_bigquery_service()).tick()
                    except Exception:
                        logger.exception("Control loop tick failed.")
                    finally:
                        session.close()

            thread = threading.Thread(target=_run_control_loop, name="kairyx-control-loop", daemon=True)
            app.state.control_loop_stop_event = stop_event
            app.state.control_loop_thread = thread
            thread.start()
        app.state.restart_reconciliation_complete = True

    @app.on_event("shutdown")
    def _shutdown() -> None:
        mark_shutdown_requested()
        stop_event = getattr(app.state, "control_loop_stop_event", None)
        thread = getattr(app.state, "control_loop_thread", None)
        if stop_event is not None:
            stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)

    @app.get("/")
    def root():
        response = FileResponse(frontend_index)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.get("/health")
    def root_health():
        session = get_session_factory()()
        try:
            repository = SqlAlchemyControlPlaneRepository(session)
            return health.health(service=HealthMonitorService(repository, get_shared_bigquery_service()))
        finally:
            session.close()

    @app.get("/health/live")
    def root_health_live():
        return health.health_live()

    if frontend_static_dir.exists():
        app.mount("/static", StaticFiles(directory=frontend_static_dir), name="frontend-static")

    app.include_router(health.router, prefix=settings.api_v1_prefix)
    app.include_router(connectors.router, prefix=settings.api_v1_prefix)
    app.include_router(mappings.router, prefix=settings.api_v1_prefix)
    app.include_router(imports.router, prefix=settings.api_v1_prefix)
    app.include_router(predictions.router, prefix=settings.api_v1_prefix)
    app.include_router(exports.router, prefix=settings.api_v1_prefix)
    app.include_router(experiments.router, prefix=settings.api_v1_prefix)
    app.include_router(cohorts.router, prefix=settings.api_v1_prefix)
    app.include_router(sql_workspace.router, prefix=settings.api_v1_prefix)
    app.include_router(workflows.workflow_router, prefix=settings.api_v1_prefix)
    app.include_router(workflows.orchestrator_router, prefix=settings.api_v1_prefix)
    app.include_router(activation.router, prefix=settings.api_v1_prefix)
    app.include_router(copilot.router, prefix=settings.api_v1_prefix)
    app.include_router(audit.router, prefix=settings.api_v1_prefix)
    app.include_router(templates.router, prefix=settings.api_v1_prefix)
    return app


app = create_app()
