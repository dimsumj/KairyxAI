from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.schemas.copilot import (
    CopilotExplainRequest,
    CopilotQueryRequest,
    CopilotReportReviewRequest,
    CopilotRecommendRequest,
    CopilotReportRequest,
    CopilotResponse,
)
from app.application.copilot import CopilotService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_copilot_service


router = APIRouter(prefix="/copilot", tags=["copilot"])


@router.post("/query", response_model=CopilotResponse)
def copilot_query(request: CopilotQueryRequest, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.query")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_query",
        resource_type="copilot_query_log",
        resource_id=None,
        payload=service.query(request.question, time_window=request.time_window, filters=request.filters),
    )


@router.get("/metrics", response_model=dict)
def list_copilot_metrics(http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.metrics.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_metrics_read",
        resource_type="copilot_metric_registry",
        resource_id=None,
        payload=service.get_metrics(),
    )


@router.get("/overview", response_model=dict)
def get_copilot_overview(http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.overview.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_overview_read",
        resource_type="copilot_report",
        resource_id=None,
        payload=service.get_overview(),
    )


@router.post("/explain", response_model=CopilotResponse)
def copilot_explain(request: CopilotExplainRequest, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.explain")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_explain",
        resource_type="copilot_query_log",
        resource_id=None,
        payload=service.explain(request.metric_id, time_window=request.time_window, dimensions=request.dimensions),
    )


@router.post("/recommend", response_model=CopilotResponse)
def copilot_recommend(request: CopilotRecommendRequest, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.query")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_recommend",
        resource_type="copilot_query_log",
        resource_id=None,
        payload=service.recommend(request.insight, request.metric_context),
    )


@router.post("/report", response_model=CopilotResponse)
def copilot_report(request: CopilotReportRequest, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.report")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_report",
        resource_type="copilot_report",
        resource_id=None,
        payload=service.report(request.report_type, time_window=request.time_window),
    )


@router.get("/query-logs/{query_id}", response_model=dict)
def get_copilot_query_log(query_id: str, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.query_log.read")
    payload = service.get_query_log(query_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Copilot query log '{query_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_query_log_read",
        resource_type="copilot_query_log",
        resource_id=query_id,
        payload=payload,
    )


@router.get("/anomalies", response_model=dict)
def list_copilot_anomalies(http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.anomalies.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_anomalies_read",
        resource_type="copilot_anomaly",
        resource_id=None,
        payload={"items": service.list_anomalies()},
    )


@router.get("/anomalies/{anomaly_id}", response_model=dict)
def get_copilot_anomaly(anomaly_id: str, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.anomaly.read")
    payload = service.get_anomaly(anomaly_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Copilot anomaly '{anomaly_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_anomaly_read",
        resource_type="copilot_anomaly",
        resource_id=anomaly_id,
        payload=payload,
    )


@router.get("/reports", response_model=dict)
def list_copilot_reports(http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.reports.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_reports_read",
        resource_type="copilot_report",
        resource_id=None,
        payload={"items": service.list_reports()},
    )


@router.get("/reports/{report_id}", response_model=dict)
def get_copilot_report(report_id: str, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.reports.read")
    payload = service.get_report(report_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Copilot report '{report_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_report_read",
        resource_type="copilot_report",
        resource_id=report_id,
        payload=payload,
    )


@router.get("/reports/{report_id}/runs", response_model=dict)
def list_copilot_report_runs(report_id: str, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.reports.read")
    try:
        payload = service.list_report_runs(report_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Copilot report '{report_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_report_runs_read",
        resource_type="copilot_report_run",
        resource_id=report_id,
        payload=payload,
    )


@router.post("/reports/{report_id}/retry", response_model=CopilotResponse)
def retry_copilot_report(report_id: str, http_request: Request, service: CopilotService = Depends(get_copilot_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.report.retry")
    try:
        payload = service.retry_report(report_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Copilot report '{report_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_report_retry",
        resource_type="copilot_report",
        resource_id=report_id,
        payload=payload,
    )


@router.post("/reports/{report_id}/review", response_model=dict)
def review_copilot_report(
    report_id: str,
    request: CopilotReportReviewRequest,
    http_request: Request,
    service: CopilotService = Depends(get_copilot_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.report.review")
    try:
        payload = service.review_report(
            report_id,
            reviewed_by=context.actor_id,
            disposition=request.disposition,
            notes=request.notes,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Copilot report '{report_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_report_review",
        resource_type="copilot_report",
        resource_id=report_id,
        payload=payload,
    )
