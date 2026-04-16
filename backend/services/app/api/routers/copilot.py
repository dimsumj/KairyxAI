from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.schemas.copilot import (
    AgentModelProfileCreateRequest,
    AgentModelProfileResponse,
    AgentModelProfileUpdateRequest,
    AgentTurn,
    CopilotAgentConfirmRequest,
    CopilotAgentMessageRequest,
    CopilotAgentMessageResponse,
    CopilotAgentSessionCreateRequest,
    CopilotAgentSessionResponse,
    CopilotAgentTurnsResponse,
    CopilotExplainRequest,
    CopilotQueryRequest,
    CopilotReportReviewRequest,
    CopilotRecommendRequest,
    CopilotReportRequest,
    CopilotResponse,
)
from app.application.copilot import CopilotService
from app.application.agent_model_profiles import AgentModelProfileService
from app.application.copilot_agent import CopilotAgentService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_agent_model_profile_service, get_copilot_agent_service, get_copilot_service


router = APIRouter(prefix="/copilot", tags=["copilot"])


@router.post("/agent/sessions", response_model=CopilotAgentSessionResponse, status_code=201)
def create_copilot_agent_session(
    request: CopilotAgentSessionCreateRequest,
    http_request: Request,
    service: CopilotAgentService = Depends(get_copilot_agent_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.read")
    try:
        payload = service.create_session(
            title=request.title,
            model_profile_id=request.model_profile_id,
            ui_context=request.ui_context,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Agent model profile '{request.model_profile_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_agent_session_create",
        resource_type="copilot_agent_session",
        resource_id=payload["session_state"]["session_id"],
        payload=payload,
    )


@router.get("/agent/sessions/{session_id}", response_model=CopilotAgentSessionResponse)
def get_copilot_agent_session(
    session_id: str,
    http_request: Request,
    service: CopilotAgentService = Depends(get_copilot_agent_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.read")
    try:
        payload = service.get_session(session_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Copilot agent session '{session_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_agent_session_read",
        resource_type="copilot_agent_session",
        resource_id=session_id,
        payload=payload,
    )


@router.get("/agent/sessions/{session_id}/turns", response_model=CopilotAgentTurnsResponse)
def list_copilot_agent_turns(
    session_id: str,
    http_request: Request,
    service: CopilotAgentService = Depends(get_copilot_agent_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.read")
    try:
        payload = service.list_turns(session_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Copilot agent session '{session_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_agent_turns_read",
        resource_type="copilot_agent_turn",
        resource_id=session_id,
        payload=payload,
    )


@router.post("/agent/sessions/{session_id}/messages", response_model=CopilotAgentMessageResponse)
def send_copilot_agent_message(
    session_id: str,
    request: CopilotAgentMessageRequest,
    http_request: Request,
    service: CopilotAgentService = Depends(get_copilot_agent_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.run")
    try:
        payload = service.handle_message(
            session_id,
            message=request.message,
            ui_context=request.ui_context,
            context=context,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Copilot agent session '{session_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_agent_message",
        resource_type="copilot_agent_turn",
        resource_id=session_id,
        payload=payload,
    )


@router.post("/agent/actions/{action_id}/confirm", response_model=CopilotAgentMessageResponse)
def confirm_copilot_agent_action(
    action_id: str,
    request: CopilotAgentConfirmRequest,
    http_request: Request,
    service: CopilotAgentService = Depends(get_copilot_agent_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.confirm")
    try:
        payload = service.confirm_action(action_id, note=request.note, context=context)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Copilot agent action '{action_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_agent_action_confirm",
        resource_type="copilot_agent_action_run",
        resource_id=action_id,
        payload=payload,
    )


@router.get("/agent/model-profiles", response_model=dict)
def list_agent_model_profiles(
    http_request: Request,
    service: AgentModelProfileService = Depends(get_agent_model_profile_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.read")
    return {"items": service.list_profiles()}


@router.post("/agent/model-profiles", response_model=AgentModelProfileResponse, status_code=201)
def create_agent_model_profile(
    request: AgentModelProfileCreateRequest,
    http_request: Request,
    service: AgentModelProfileService = Depends(get_agent_model_profile_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.write")
    try:
        payload = service.create_profile(
            name=request.name,
            provider=request.provider,
            model_name=request.model_name,
            config=request.config,
            is_default=request.is_default,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="agent_model_profile_create",
        resource_type="agent_model_profile",
        resource_id=payload["model_profile_id"],
        payload=payload,
    )


@router.get("/agent/model-profiles/{model_profile_id}", response_model=AgentModelProfileResponse)
def get_agent_model_profile(
    model_profile_id: str,
    http_request: Request,
    service: AgentModelProfileService = Depends(get_agent_model_profile_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.read")
    payload = service.get_profile(model_profile_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Agent model profile '{model_profile_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="agent_model_profile_read",
        resource_type="agent_model_profile",
        resource_id=model_profile_id,
        payload=payload,
    )


@router.patch("/agent/model-profiles/{model_profile_id}", response_model=AgentModelProfileResponse)
def update_agent_model_profile(
    model_profile_id: str,
    request: AgentModelProfileUpdateRequest,
    http_request: Request,
    service: AgentModelProfileService = Depends(get_agent_model_profile_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.write")
    try:
        payload = service.update_profile(model_profile_id, request.model_dump(exclude_none=True))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Agent model profile '{model_profile_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="agent_model_profile_update",
        resource_type="agent_model_profile",
        resource_id=model_profile_id,
        payload=payload,
    )


@router.delete("/agent/model-profiles/{model_profile_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_agent_model_profile(
    model_profile_id: str,
    http_request: Request,
    service: AgentModelProfileService = Depends(get_agent_model_profile_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "copilot.agent.write")
    try:
        deleted = service.delete_profile(model_profile_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Agent model profile '{model_profile_id}' not found.")
    service.repository.record_action(
        "agent_model_profile_delete",
        "agent_model_profile",
        model_profile_id,
        {
            "actor_role": context.actor_role,
            "actor_id": context.actor_id,
            "tenant_id": context.tenant_id,
            "project_id": context.project_id,
            "correlation_id": context.correlation_id,
        },
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
    try:
        payload = service.report(request.report_type, time_window=request.time_window)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="copilot_report",
        resource_type="copilot_report",
        resource_id=None,
        payload=payload,
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
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
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
