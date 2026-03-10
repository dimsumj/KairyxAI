from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.schemas.copilot import (
    CopilotExplainRequest,
    CopilotQueryRequest,
    CopilotRecommendRequest,
    CopilotReportRequest,
    CopilotResponse,
)
from app.application.copilot import CopilotService
from app.core.deps import get_copilot_service


router = APIRouter(prefix="/copilot", tags=["copilot"])


@router.post("/query", response_model=CopilotResponse)
def copilot_query(request: CopilotQueryRequest, service: CopilotService = Depends(get_copilot_service)):
    return service.query(request.question, time_window=request.time_window, filters=request.filters)


@router.post("/explain", response_model=CopilotResponse)
def copilot_explain(request: CopilotExplainRequest, service: CopilotService = Depends(get_copilot_service)):
    return service.explain(request.metric_id, time_window=request.time_window, dimensions=request.dimensions)


@router.post("/recommend", response_model=CopilotResponse)
def copilot_recommend(request: CopilotRecommendRequest, service: CopilotService = Depends(get_copilot_service)):
    return service.recommend(request.insight, request.metric_context)


@router.post("/report", response_model=CopilotResponse)
def copilot_report(request: CopilotReportRequest, service: CopilotService = Depends(get_copilot_service)):
    return service.report(request.report_type, time_window=request.time_window)
