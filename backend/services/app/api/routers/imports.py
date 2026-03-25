from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.api.schemas.jobs import build_job_response
from app.application.imports import ImportService
from app.core.api_paths import build_request_api_path
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_import_service


class ImportJobCreateRequest(BaseModel):
    source_name: str | None = None
    connector_id: str | None = None
    start_date: str
    end_date: str
    page_size: int | None = None


class ImportBackfillRequest(BaseModel):
    source_name: str
    start_date: str
    end_date: str
    mode: str = "replay_rejected_rows"
    limit_jobs: int = 50


router = APIRouter(prefix="/imports", tags=["imports"])


@router.get("")
def list_imports(request: Request, service: ImportService = Depends(get_import_service)):
    base_path = build_request_api_path(request, "/imports")
    jobs = [
        build_job_response(
            job,
            base_path=base_path,
            extra_links={"checkpoints": f"{base_path}/{job['id']}/checkpoints"},
        )
        for job in service.list_jobs()
    ]
    return {"items": jobs}


@router.post("", status_code=status.HTTP_201_CREATED)
def create_import(request: ImportJobCreateRequest, http_request: Request, service: ImportService = Depends(get_import_service)):
    source_ref = request.connector_id or request.source_name
    if not source_ref:
        raise HTTPException(status_code=409, detail="source_name or connector_id is required.")
    try:
        job = service.create_job(source_ref, request.start_date, request.end_date, request.page_size)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Connector '{source_ref}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    base_path = build_request_api_path(http_request, "/imports")
    return build_job_response(job, base_path=base_path, extra_links={"checkpoints": f"{base_path}/{job['id']}/checkpoints"})


@router.get("/backfills")
def list_import_backfills(request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.backfills.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="imports_backfills_read",
        resource_type="import_backfill",
        resource_id=None,
        payload=service.list_backfills(),
    )


@router.post("/backfills")
def create_import_backfill(request: ImportBackfillRequest, http_request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "imports.backfills.create")
    return build_audited_response(
        service.repository,
        context,
        action_type="imports_backfill_create",
        resource_type="import_backfill",
        resource_id=None,
        payload=service.create_backfill(
            source_name=request.source_name,
            start_date=request.start_date,
            end_date=request.end_date,
            mode=request.mode,
            limit_jobs=request.limit_jobs,
        ),
    )


@router.get("/backfills/{backfill_id}")
def get_import_backfill(backfill_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.backfills.read")
    try:
        return build_audited_response(
            service.repository,
            context,
            action_type="imports_backfill_read",
            resource_type="import_backfill",
            resource_id=backfill_id,
            payload=service.get_backfill(backfill_id),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import backfill '{backfill_id}' not found.")


@router.get("/schema-contracts")
def list_import_schema_contracts(request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.schema.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="imports_schema_contracts_read",
        resource_type="import_schema_contract",
        resource_id=None,
        payload=service.list_schema_contracts(),
    )


@router.get("/schema-contracts/{alias}")
def get_import_schema_contract(alias: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.schema.read")
    try:
        payload = service.get_schema_contract(alias)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Schema contract '{alias}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="imports_schema_contract_read",
        resource_type="import_schema_contract",
        resource_id=alias,
        payload=payload,
    )


@router.get("/{job_id}")
def get_import(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    base_path = build_request_api_path(request, "/imports")
    return build_job_response(job, base_path=base_path, extra_links={"checkpoints": f"{base_path}/{job['id']}/checkpoints"})


@router.post("/{job_id}/run")
def run_import(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    base_path = build_request_api_path(request, "/imports")
    try:
        job = service.run_job(job_id)
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    except Exception as exc:
        service.rollback_session()
        failed_job = None
        try:
            failed_job = service.get_job(job_id)
        except Exception:
            service.rollback_session()
        payload = {"detail": str(exc)}
        if failed_job is not None:
            payload["job"] = build_job_response(
                failed_job,
                base_path=base_path,
                extra_links={"checkpoints": f"{base_path}/{failed_job['id']}/checkpoints"},
            ).model_dump(mode="json")
        return JSONResponse(status_code=500, content=payload)
    return build_job_response(job, base_path=base_path, extra_links={"checkpoints": f"{base_path}/{job['id']}/checkpoints"})


@router.post("/{job_id}/stop")
def stop_import(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    try:
        job = service.stop_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    base_path = build_request_api_path(request, "/imports")
    return build_job_response(job, base_path=base_path, extra_links={"checkpoints": f"{base_path}/{job['id']}/checkpoints"})


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_import(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    ensure_permission(get_governance_context(request), "imports.delete")
    try:
        service.delete_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return None


@router.get("/{job_id}/checkpoints")
def get_import_checkpoints(job_id: str, service: ImportService = Depends(get_import_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    return {"items": service.repository.list_checkpoints(job_id)}


@router.get("/{job_id}/quality")
def get_import_quality(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.quality.read")
    try:
        return build_audited_response(
            service.repository,
            context,
            action_type="imports_quality_read",
            resource_type="import_job",
            resource_id=job_id,
            payload=service.get_quality(job_id),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")


@router.get("/{job_id}/manifests")
def get_import_manifests(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.operations.read")
    try:
        payload = service.list_manifests(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="imports_manifests_read",
        resource_type="import_job",
        resource_id=job_id,
        payload=payload,
    )


@router.get("/{job_id}/operations")
def get_import_operations(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.operations.read")
    try:
        return build_audited_response(
            service.repository,
            context,
            action_type="imports_operations_read",
            resource_type="import_job",
            resource_id=job_id,
            payload=service.get_operations(job_id),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")


@router.get("/{job_id}/identity-links")
def get_import_identity_links(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.identity_links.read")
    try:
        return build_audited_response(
            service.repository,
            context,
            action_type="imports_identity_links_read",
            resource_type="import_job",
            resource_id=job_id,
            payload=service.get_identity_links(job_id),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")


@router.get("/{job_id}/conflicts")
def get_import_conflicts(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.conflicts.read")
    try:
        return build_audited_response(
            service.repository,
            context,
            action_type="imports_conflicts_read",
            resource_type="import_job",
            resource_id=job_id,
            payload=service.get_conflicts(job_id),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")


@router.get("/{job_id}/rejected")
def get_import_rejected(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.rejected.read")
    try:
        return build_audited_response(
            service.repository,
            context,
            action_type="imports_rejected_read",
            resource_type="import_job",
            resource_id=job_id,
            payload=service.get_rejected(job_id),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")


@router.post("/{job_id}/resume")
def resume_import(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.resume")
    try:
        job = service.resume_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="imports_resume",
        resource_type="import_job",
        resource_id=job_id,
        payload=build_job_response(
            job,
            base_path=build_request_api_path(request, "/imports"),
            extra_links={"checkpoints": f"{build_request_api_path(request, '/imports')}/{job['id']}/checkpoints"},
        ).model_dump(mode="json"),
    )


@router.post("/{job_id}/replay")
def replay_import(job_id: str, request: Request, service: ImportService = Depends(get_import_service)):
    context = get_governance_context(request)
    ensure_permission(context, "imports.replay")
    try:
        return build_audited_response(
            service.repository,
            context,
            action_type="imports_replay",
            resource_type="import_job",
            resource_id=job_id,
            payload=service.replay_job(job_id),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
