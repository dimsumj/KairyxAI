from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class CopilotQueryRequest(BaseModel):
    question: str
    time_window: str | None = None
    filters: Dict[str, Any] = Field(default_factory=dict)


class CopilotExplainRequest(BaseModel):
    metric_id: str
    time_window: str = "7d"
    dimensions: List[str] = Field(default_factory=lambda: ["platform", "country", "campaign"])


class CopilotRecommendRequest(BaseModel):
    insight: Dict[str, Any] = Field(default_factory=dict)
    metric_context: Dict[str, Any] = Field(default_factory=dict)


class CopilotReportRequest(BaseModel):
    report_type: str = "daily"
    time_window: str = "7d"


class CopilotReportReviewRequest(BaseModel):
    disposition: str = "acknowledged"
    notes: str = ""


class CopilotResponse(BaseModel):
    conclusion: str
    evidence: List[Dict[str, Any]] = Field(default_factory=list)
    key_evidence: List[Dict[str, Any]] = Field(default_factory=list)
    impact_scope: Dict[str, Any] = Field(default_factory=dict)
    recommended_action: Dict[str, Any] = Field(default_factory=dict)
    suggested_action: Dict[str, Any] = Field(default_factory=dict)
    confidence: str = "medium"
    metric_window: str = "7d"
    risk_notes: List[str] = Field(default_factory=list)
    methodology: Dict[str, Any] = Field(default_factory=dict)
    query_id: str | None = None
    anomaly_id: str | None = None
    report_id: str | None = None
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)


class CopilotAgentSessionCreateRequest(BaseModel):
    title: str = ""
    model_profile_id: str | None = None
    ui_context: Dict[str, Any] = Field(default_factory=dict)


class CopilotAgentMessageRequest(BaseModel):
    message: str
    ui_context: Dict[str, Any] = Field(default_factory=dict)


class CopilotAgentSecureInputRequest(BaseModel):
    values: Dict[str, Any] = Field(default_factory=dict)
    ui_context: Dict[str, Any] = Field(default_factory=dict)


class CopilotAgentConfirmRequest(BaseModel):
    note: str = ""


class AgentArtifactLink(BaseModel):
    resource_type: str
    resource_id: str
    label: str = ""
    module_id: str = ""
    page_id: str = ""
    api_path: str = ""
    focus: Dict[str, Any] = Field(default_factory=dict)
    status: str = ""
    resume_ready: bool = False
    resume_message: str = ""
    status_detail: str = ""


class AgentClarification(BaseModel):
    key: str
    label: str
    question: str
    required: bool = True
    input_type: str = "text"
    options: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AgentExecutionPreviewStep(BaseModel):
    action_id: str = ""
    action_type: str
    title: str
    summary: str = ""
    status: str = "pending"
    requires_confirmation: bool = False
    risk_level: str = "low"


class AgentExecutionPreview(BaseModel):
    intent: str
    title: str
    summary: str = ""
    risk_level: str = "low"
    ready: bool = False
    missing_fields: List[str] = Field(default_factory=list)
    blockers: List[str] = Field(default_factory=list)
    steps: List[AgentExecutionPreviewStep] = Field(default_factory=list)


class AgentActionRun(BaseModel):
    action_id: str
    session_id: str
    action_type: str
    title: str
    status: str
    requires_confirmation: bool = False
    risk_level: str = "low"
    parameters: Dict[str, Any] = Field(default_factory=dict)
    result: Dict[str, Any] = Field(default_factory=dict)
    summary: str = ""
    artifacts: List[AgentArtifactLink] = Field(default_factory=list)
    confirmation_id: str | None = None
    confirmation_note: str = ""
    is_async: bool = False
    status_detail: str = ""
    created_at: str | None = None
    updated_at: str | None = None


class AgentSessionState(BaseModel):
    session_id: str
    title: str
    status: str
    current_intent: str | None = None
    last_user_message: str = ""
    ui_context: Dict[str, Any] = Field(default_factory=dict)
    latest_execution_preview: AgentExecutionPreview | None = None
    latest_artifacts: List[AgentArtifactLink] = Field(default_factory=list)
    latest_clarifications: List[AgentClarification] = Field(default_factory=list)
    pending_confirmation_count: int = 0
    model_profile_id: str | None = None
    effective_provider: str = "deterministic"
    effective_model_name: str = ""
    model_selection_source: str = "deterministic_fallback"
    async_status: str = ""
    waiting_for_action_type: str | None = None
    waiting_for_resource_id: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class AgentTurn(BaseModel):
    turn_id: str
    session_id: str
    user_message: str
    assistant_message: str
    intent: str
    status: str
    clarifications: List[AgentClarification] = Field(default_factory=list)
    execution_preview: AgentExecutionPreview | None = None
    completed_actions: List[AgentActionRun] = Field(default_factory=list)
    pending_confirmations: List[AgentActionRun] = Field(default_factory=list)
    artifacts: List[AgentArtifactLink] = Field(default_factory=list)
    ui_context: Dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class CopilotAgentSessionResponse(BaseModel):
    session_state: AgentSessionState
    pending_confirmations: List[AgentActionRun] = Field(default_factory=list)
    latest_turn: AgentTurn | None = None
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)


class CopilotAgentTurnsResponse(BaseModel):
    items: List[AgentTurn] = Field(default_factory=list)
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)


class CopilotAgentMessageResponse(BaseModel):
    assistant_message: str
    session_state: AgentSessionState
    clarifications: List[AgentClarification] = Field(default_factory=list)
    execution_preview: AgentExecutionPreview | None = None
    completed_actions: List[AgentActionRun] = Field(default_factory=list)
    pending_confirmations: List[AgentActionRun] = Field(default_factory=list)
    artifacts: List[AgentArtifactLink] = Field(default_factory=list)
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)


class AgentModelProfileCreateRequest(BaseModel):
    name: str
    provider: str
    model_name: str | None = None
    config: Dict[str, Any] = Field(default_factory=dict)
    is_default: bool = False


class AgentModelProfileUpdateRequest(BaseModel):
    name: str | None = None
    provider: str | None = None
    model_name: str | None = None
    config: Dict[str, Any] | None = None
    status: str | None = None
    is_default: bool | None = None


class AgentModelProfileResponse(BaseModel):
    model_profile_id: str
    name: str
    provider: str
    model_name: str | None = None
    status: str
    is_default: bool = False
    system_managed: bool = False
    config: Dict[str, Any] = Field(default_factory=dict)
    tenant_id: str | None = None
    project_id: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    created_at: str | None = None
    updated_at: str | None = None
    model_selection_source: str = "profile"
