from __future__ import annotations

import logging
import sqlite3
import threading
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from starlette.responses import FileResponse, JSONResponse
from starlette.staticfiles import StaticFiles

from app.api.routers import (
    activation,
    audit,
    auth,
    cohorts,
    connectors,
    copilot,
    experiments,
    exports,
    health,
    imports,
    mappings,
    onboarding,
    predictions,
    projects,
    provider_connections,
    sql_workspace,
    templates,
    tenants,
    workflows,
)
from app.application.imports import ImportService
from app.application.control_loop import ControlLoopService
from app.application.health_monitor import HealthMonitorService
from app.application.predictions import PredictionService
from app.core.db import get_session_factory, init_db
from app.core.auth import get_authenticator
from app.core.errors import is_database_locked_error
from app.core.governance import GovernanceContext
from app.core.logging import configure_access_log_filters, emit_structured_log
from app.core.request_context import RequestContext, request_context
from app.core.runtime import clear_shutdown_requested, mark_shutdown_requested
from app.core.settings import get_settings, validate_runtime_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service


logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    settings = get_settings()
    validate_runtime_settings(settings)
    frontend_dir = Path(__file__).resolve().parents[3] / "frontend"
    frontend_index = frontend_dir / "index.html"
    frontend_static_dir = frontend_dir / "assets"
    configure_access_log_filters()
    app = FastAPI(title=settings.app_name)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_allowed_origins),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def _governance_context_guard(request: Request, call_next):
        correlation_id = str(request.headers.get("x-correlation-id") or f"req_{uuid.uuid4().hex[:20]}").strip()
        request.state.correlation_id = correlation_id
        public_paths = {
            "/health",
            "/health/live",
            f"{settings.api_v1_prefix}/health/live",
            f"{settings.api_v1_prefix}/auth/oidc-config",
            "/",
        }
        if request.url.path.startswith("/static/") or request.url.path in public_paths:
            context = GovernanceContext(
                actor_role="admin",
                actor_id="system",
                tenant_id=settings.bootstrap_tenant_id,
                project_id=settings.bootstrap_project_id,
                correlation_id=correlation_id,
                platform_admin=True,
                org_role="owner",
                project_role="admin",
                auth_mode="public",
            )
        else:
            try:
                context = _build_governance_context(request, settings, correlation_id)
            except HTTPException as exc:
                emit_structured_log(
                    "request_rejected",
                    path=request.url.path,
                    method=request.method,
                    tenant_id=settings.bootstrap_tenant_id,
                    project_id=settings.bootstrap_project_id,
                    actor_id="anonymous",
                    resource_type="http_request",
                    correlation_id=correlation_id,
                    status_code=exc.status_code,
                    detail=exc.detail,
                )
                return JSONResponse(
                    status_code=exc.status_code,
                    content={"detail": exc.detail, "correlation_id": correlation_id},
                )

        request.state.governance_context = context
        scoped_context = RequestContext(
            actor_id=context.actor_id,
            actor_role=context.actor_role,
            tenant_id=context.tenant_id,
            project_id=context.project_id,
            correlation_id=context.correlation_id,
            platform_admin=context.platform_admin,
            org_role=context.org_role,
            project_role=context.project_role,
            auth_mode=context.auth_mode,
        )
        with request_context(scoped_context):
            response = await call_next(request)
        emit_structured_log(
            "http_request",
            path=request.url.path,
            method=request.method,
            tenant_id=context.tenant_id,
            project_id=context.project_id,
            actor_id=context.actor_id,
            resource_type="http_request",
            correlation_id=correlation_id,
            status_code=response.status_code,
            job_id=request.path_params.get("job_id") or request.query_params.get("job_id"),
        )
        response.headers["X-Correlation-ID"] = correlation_id
        return response

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
        with request_context(
            RequestContext(
                actor_id="system",
                actor_role="admin",
                tenant_id=settings.bootstrap_tenant_id,
                project_id=settings.bootstrap_project_id,
                correlation_id="startup",
                platform_admin=True,
                org_role="owner",
                project_role="admin",
                auth_mode="system",
            )
        ):
            init_db()
            session = get_session_factory()()
            try:
                repository = SqlAlchemyControlPlaneRepository(session)
                repository.ensure_tenant(settings.bootstrap_tenant_id, settings.bootstrap_tenant_name)
                repository.ensure_project(settings.bootstrap_tenant_id, settings.bootstrap_project_id, settings.bootstrap_project_name)
                bigquery_service = get_shared_bigquery_service()
                try:
                    ImportService(repository, settings, bigquery_service=bigquery_service).reconcile_jobs_after_restart()
                    ImportService(repository, settings, bigquery_service=bigquery_service).cleanup_expired_jobs()
                    PredictionService(repository, settings, bigquery_service=bigquery_service).cleanup_expired_jobs()
                    ControlLoopService(repository, settings, bigquery_service).ensure_default_jobs()
                    session.commit()
                except Exception:
                    session.rollback()
                    logger.exception("Import restart reconciliation failed during startup. Continuing without blocking API startup.")
            finally:
                session.close()
        if getattr(app.state, "health_warmup_thread", None) is None:
            def _warm_health_snapshot() -> None:
                with request_context(
                    RequestContext(
                        actor_id="system",
                        actor_role="admin",
                        tenant_id=settings.bootstrap_tenant_id,
                        project_id=settings.bootstrap_project_id,
                        correlation_id="health-warmup",
                        platform_admin=True,
                        org_role="owner",
                        project_role="admin",
                        auth_mode="system",
                    )
                ):
                    session = get_session_factory()()
                    try:
                        repository = SqlAlchemyControlPlaneRepository(session)
                        HealthMonitorService(repository, get_shared_bigquery_service()).snapshot(persist=True)
                        session.commit()
                    except Exception:
                        session.rollback()
                        logger.exception("Health snapshot warm-up failed.")
                    finally:
                        session.close()

            thread = threading.Thread(target=_warm_health_snapshot, name="kairyx-health-warmup", daemon=True)
            app.state.health_warmup_thread = thread
            thread.start()
        scheduler_allowed = settings.scheduler_enabled and (settings.app_env != "prod" or settings.service_role == "scheduler-worker")
        if scheduler_allowed and getattr(app.state, "control_loop_thread", None) is None:
            stop_event = threading.Event()

            def _run_control_loop() -> None:
                with request_context(
                    RequestContext(
                        actor_id="system",
                        actor_role="admin",
                        tenant_id=settings.bootstrap_tenant_id,
                        project_id=settings.bootstrap_project_id,
                        correlation_id="scheduler-loop",
                        platform_admin=True,
                        org_role="owner",
                        project_role="admin",
                        auth_mode="system",
                    )
                ):
                    while not stop_event.wait(settings.scheduler_interval_seconds):
                        session = get_session_factory()()
                        try:
                            repository = SqlAlchemyControlPlaneRepository(session)
                            ControlLoopService(repository, settings, get_shared_bigquery_service()).tick()
                            session.commit()
                        except Exception:
                            session.rollback()
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

    app.include_router(auth.router, prefix=settings.api_v1_prefix)
    app.include_router(health.router, prefix=settings.api_v1_prefix)
    app.include_router(tenants.router, prefix=settings.api_v1_prefix)
    app.include_router(provider_connections.router, prefix=settings.api_v1_prefix)
    app.include_router(connectors.router, prefix=settings.api_v1_prefix)
    app.include_router(mappings.router, prefix=settings.api_v1_prefix)
    app.include_router(onboarding.router, prefix=settings.api_v1_prefix)
    app.include_router(projects.router, prefix=settings.api_v1_prefix)
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


def _normalize_org_role(raw_role: str | None) -> str:
    normalized = str(raw_role or "").strip().lower()
    if normalized in {"owner", "admin"}:
        return normalized
    return "member"


def _is_tenant_optional_path(path: str, settings) -> bool:
    api = settings.api_v1_prefix.rstrip("/")
    return path in {
        f"{api}/auth/me",
        f"{api}/project-invites/redeem",
    } or path.startswith(f"{api}/onboarding")


def _is_project_optional_path(path: str, settings) -> bool:
    api = settings.api_v1_prefix.rstrip("/")
    return path in {
        f"{api}/auth/me",
        f"{api}/projects",
        f"{api}/project-invites/redeem",
    } or path.startswith(f"{api}/onboarding") or path.endswith("/invites")


def _build_governance_context(request: Request, settings, correlation_id: str) -> GovernanceContext:
    protected_prefix = settings.api_v1_prefix.rstrip("/")
    if not request.url.path.startswith(protected_prefix):
        return GovernanceContext(
            actor_role="admin",
            actor_id="system",
            tenant_id=settings.bootstrap_tenant_id,
            project_id=settings.bootstrap_project_id,
            correlation_id=correlation_id,
            platform_admin=True,
            org_role="owner",
            project_role="admin",
            auth_mode="public",
        )

    auth_header = str(request.headers.get("authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
        try:
            principal = get_authenticator().authenticate_token(token)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc))
        requested_tenant = str(
            request.headers.get("x-kairyx-tenant")
            or request.headers.get("x-tenant-id")
            or principal.claims.get("tenant_id")
            or ""
        ).strip()
        requested_project = str(
            request.headers.get("x-kairyx-project")
            or request.headers.get("x-project-id")
            or principal.claims.get("project_id")
            or ""
        ).strip()
        requested_tenant = str(
            requested_tenant
        ).strip()
        session = get_session_factory()()
        try:
            repository = SqlAlchemyControlPlaneRepository(session)
            repository.ensure_tenant(settings.bootstrap_tenant_id, settings.bootstrap_tenant_name)
            repository.ensure_project(settings.bootstrap_tenant_id, settings.bootstrap_project_id, settings.bootstrap_project_name)
            repository.upsert_platform_user(
                principal.subject,
                email=principal.email,
                display_name=principal.display_name,
            )
            if principal.platform_admin:
                effective_tenant = requested_tenant or settings.bootstrap_tenant_id
                effective_project = requested_project or settings.bootstrap_project_id
                repository.ensure_tenant(effective_tenant, effective_tenant)
                repository.ensure_project(effective_tenant, effective_project, effective_project)
                session.commit()
                return GovernanceContext(
                    actor_role="admin",
                    actor_id=principal.subject,
                    tenant_id=effective_tenant,
                    project_id=effective_project,
                    correlation_id=correlation_id,
                    platform_admin=True,
                    org_role="owner",
                    project_role="admin",
                    auth_mode="jwt",
                )

            tenant_memberships = [
                membership
                for membership in repository.list_user_tenant_memberships(principal.subject)
                if str(membership.get("status") or "").lower() == "active"
            ]
            memberships_by_tenant = {str(item["tenant_id"]): item for item in tenant_memberships}
            allow_missing_tenant = _is_tenant_optional_path(request.url.path, settings)
            allow_missing_project = _is_project_optional_path(request.url.path, settings)

            if not memberships_by_tenant:
                if not allow_missing_tenant:
                    raise HTTPException(status_code=403, detail="No organization space membership is active for this user.")
                session.commit()
                return GovernanceContext(
                    actor_role="operator",
                    actor_id=principal.subject,
                    tenant_id=None,
                    project_id=None,
                    correlation_id=correlation_id,
                    platform_admin=False,
                    org_role=None,
                    project_role=None,
                    auth_mode="jwt",
                )

            selected_tenant = None
            if requested_tenant:
                if requested_tenant not in memberships_by_tenant:
                    raise HTTPException(status_code=403, detail=f"Tenant membership for '{requested_tenant}' is missing or inactive.")
                selected_tenant = requested_tenant
            elif len(memberships_by_tenant) == 1:
                selected_tenant = next(iter(memberships_by_tenant))
            elif not allow_missing_tenant:
                raise HTTPException(status_code=409, detail="Organization space selection is required.")

            org_membership = memberships_by_tenant.get(selected_tenant) if selected_tenant else None
            org_role = _normalize_org_role((org_membership or {}).get("role"))
            selected_project = None
            project_role = None

            if selected_tenant:
                project_memberships = [
                    membership
                    for membership in repository.list_project_memberships(tenant_id=selected_tenant, user_id=principal.subject)
                    if str(membership.get("status") or "").lower() == "active"
                ]
                memberships_by_project = {str(item["project_id"]): item for item in project_memberships}
                if requested_project:
                    membership = memberships_by_project.get(requested_project)
                    if membership is None:
                        raise HTTPException(status_code=403, detail=f"Project membership for '{requested_project}' is missing or inactive.")
                    selected_project = requested_project
                    project_role = str(membership.get("role") or "operator")
                elif len(memberships_by_project) == 1:
                    selected_project, membership = next(iter(memberships_by_project.items()))
                    project_role = str(membership.get("role") or "operator")
                elif not memberships_by_project:
                    if not allow_missing_project:
                        raise HTTPException(status_code=403, detail=f"Project membership is missing or inactive for organization space '{selected_tenant}'.")
                elif not allow_missing_project:
                    raise HTTPException(status_code=409, detail="Project selection is required.")

            session.commit()
            return GovernanceContext(
                actor_role=str(project_role or "operator"),
                actor_id=principal.subject,
                tenant_id=selected_tenant,
                project_id=selected_project,
                correlation_id=correlation_id,
                platform_admin=False,
                org_role=org_role,
                project_role=project_role,
                auth_mode="jwt",
            )
        finally:
            session.close()

    if settings.legacy_header_auth_enabled:
        if settings.api_access_key:
            provided = str(request.headers.get("x-api-key") or "").strip()
            if provided != settings.api_access_key:
                raise HTTPException(status_code=401, detail="Invalid or missing API key.")
        actor_role = str(request.headers.get("x-actor-role") or "admin").strip().lower() or "admin"
        actor_id = str(request.headers.get("x-actor-id") or actor_role).strip() or actor_role
        tenant_id = str(request.headers.get("x-tenant-id") or settings.bootstrap_tenant_id).strip() or settings.bootstrap_tenant_id
        project_id = str(request.headers.get("x-project-id") or settings.bootstrap_project_id).strip() or settings.bootstrap_project_id
        return GovernanceContext(
            actor_role=actor_role,
            actor_id=actor_id,
            tenant_id=tenant_id,
            project_id=project_id,
            correlation_id=correlation_id,
            platform_admin=(actor_role == "admin"),
            org_role="owner" if actor_role == "admin" else "member",
            project_role=actor_role,
            auth_mode="legacy_headers",
        )
    raise HTTPException(status_code=401, detail="Missing bearer token.")


app = create_app()
