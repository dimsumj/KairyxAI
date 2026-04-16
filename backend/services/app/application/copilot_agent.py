from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Protocol
from urllib.parse import quote

import requests
from fastapi import HTTPException

from app.application.agent_model_profiles import AgentModelProfileService
from app.application.email_campaigns import EmailCampaignService
from app.application.sendgrid_provider import SendGridProviderService
from app.application.braze_provider import BrazeProviderService
from app.application.workflows import WorkflowService
from app.application.predictions import PredictionService
from bigquery_service import BigQueryService, get_shared_bigquery_service
from gemini_client import GeminiClient

from app.application.cohorts import CohortService
from app.application.connectors import ConnectorService
from app.application.copilot import CopilotService
from app.application.experiments import ExperimentConfigService
from app.application.copilot_help_catalog import build_help_support_answer
from app.application.health_monitor import HealthMonitorService
from app.application.provider_connections import ProviderConnectionService
from app.application.secret_refs import redact_secret_values
from app.application.sql_workspace import SqlWorkspaceService
from app.core.governance import GovernanceContext, ensure_permission


SESSION_RESOURCE_TYPE = "copilot_agent_session"
TURN_RESOURCE_TYPE = "copilot_agent_turn"
ACTION_RESOURCE_TYPE = "copilot_agent_action_run"
CONFIRMATION_RESOURCE_TYPE = "copilot_agent_confirmation_request"


CONNECTOR_REQUIRED_FIELDS: Dict[str, List[str]] = {
    "amplitude": ["api_key", "secret_key"],
    "adjust": ["api_token"],
    "appsflyer": ["api_token", "app_id"],
    "bigquery": ["project_id"],
    "google": ["api_key"],
}

PROVIDER_CONNECTION_REQUIRED_FIELDS: Dict[str, List[str]] = {
    "braze": ["api_key", "rest_endpoint"],
    "sendgrid": ["api_key"],
    "webhook": ["webhook_url"],
    "simulator": [],
}

CONNECTOR_TYPE_SYNONYMS: Dict[str, List[str]] = {
    "amplitude": ["amplitude"],
    "adjust": ["adjust"],
    "appsflyer": ["appsflyer", "appsflyer"],
    "bigquery": ["bigquery", "big query"],
    "google": ["google", "gemini", "google gemini"],
}

PROVIDER_CONNECTION_TYPE_SYNONYMS: Dict[str, List[str]] = {
    "braze": ["braze"],
    "sendgrid": ["sendgrid", "send grid"],
    "webhook": ["webhook", "hook"],
    "simulator": ["simulator"],
}

RISK_ORDER = {
    "low": 0,
    "medium": 1,
    "high": 2,
}


@dataclass(frozen=True)
class AgentActionSpec:
    action_type: str
    title: str
    permissions: tuple[str, ...]
    requires_confirmation: bool = False
    risk_level: str = "low"


ACTION_REGISTRY: Dict[str, AgentActionSpec] = {
    "summarize_dashboard": AgentActionSpec(
        action_type="summarize_dashboard",
        title="Summarize dashboard state",
        permissions=("copilot.agent.read",),
        risk_level="low",
    ),
    "upsert_connector": AgentActionSpec(
        action_type="upsert_connector",
        title="Create or update connector",
        permissions=("copilot.agent.run", "connectors.write"),
        risk_level="low",
    ),
    "check_connector_health": AgentActionSpec(
        action_type="check_connector_health",
        title="Run connector health check",
        permissions=("copilot.agent.run", "connectors.write"),
        risk_level="low",
    ),
    "upsert_provider_connection": AgentActionSpec(
        action_type="upsert_provider_connection",
        title="Create or update provider connection",
        permissions=("copilot.agent.run", "provider_connections.write"),
        risk_level="low",
    ),
    "preview_sql": AgentActionSpec(
        action_type="preview_sql",
        title="Preview SQL query",
        permissions=("copilot.agent.run", "sql_workspace.preview"),
        risk_level="low",
    ),
    "save_query": AgentActionSpec(
        action_type="save_query",
        title="Save SQL query",
        permissions=("copilot.agent.run", "sql_workspace.queries.create"),
        risk_level="low",
    ),
    "draft_sql_from_prompt": AgentActionSpec(
        action_type="draft_sql_from_prompt",
        title="Draft SQL from prompt",
        permissions=("copilot.agent.run", "sql_workspace.preview"),
        risk_level="low",
    ),
    "run_prediction": AgentActionSpec(
        action_type="run_prediction",
        title="Prepare prediction job",
        permissions=("copilot.agent.run", "predictions.create", "predictions.run"),
        risk_level="low",
    ),
    "list_provider_messaging_assets": AgentActionSpec(
        action_type="list_provider_messaging_assets",
        title="List provider messaging assets",
        permissions=("copilot.agent.run", "provider_connections.read"),
        risk_level="low",
    ),
    "setup_email_campaign": AgentActionSpec(
        action_type="setup_email_campaign",
        title="Create draft email campaign",
        permissions=("copilot.agent.run", "provider_connections.read", "email_campaigns.write"),
        risk_level="low",
    ),
    "setup_workflow": AgentActionSpec(
        action_type="setup_workflow",
        title="Create draft workflow",
        permissions=("copilot.agent.run", "workflows.create"),
        risk_level="low",
    ),
    "setup_operator_flow": AgentActionSpec(
        action_type="setup_operator_flow",
        title="Set up prediction-to-campaign draft flow",
        permissions=(
            "copilot.agent.run",
            "predictions.create",
            "predictions.run",
            "sql_workspace.preview",
            "sql_workspace.queries.create",
            "cohorts.create",
            "provider_connections.read",
            "email_campaigns.write",
            "workflows.create",
        ),
        risk_level="low",
    ),
    "create_cohort_sql": AgentActionSpec(
        action_type="create_cohort_sql",
        title="Create SQL cohort draft",
        permissions=("copilot.agent.run", "cohorts.create"),
        risk_level="low",
    ),
    "create_cohort_definition": AgentActionSpec(
        action_type="create_cohort_definition",
        title="Create cohort draft",
        permissions=("copilot.agent.run", "cohorts.create"),
        risk_level="low",
    ),
    "update_cohort_definition": AgentActionSpec(
        action_type="update_cohort_definition",
        title="Update cohort draft",
        permissions=("copilot.agent.run", "cohorts.update"),
        risk_level="low",
    ),
    "save_experiment_config": AgentActionSpec(
        action_type="save_experiment_config",
        title="Save experiment config",
        permissions=("copilot.agent.run", "experiments.config.write"),
        risk_level="low",
    ),
    "activate_cohort": AgentActionSpec(
        action_type="activate_cohort",
        title="Activate cohort",
        permissions=("copilot.agent.confirm", "cohorts.activate"),
        requires_confirmation=True,
        risk_level="high",
    ),
    "pause_cohort": AgentActionSpec(
        action_type="pause_cohort",
        title="Pause cohort",
        permissions=("copilot.agent.confirm", "cohorts.pause"),
        requires_confirmation=True,
        risk_level="high",
    ),
    "archive_cohort": AgentActionSpec(
        action_type="archive_cohort",
        title="Archive cohort",
        permissions=("copilot.agent.confirm", "cohorts.archive"),
        requires_confirmation=True,
        risk_level="high",
    ),
    "restore_cohort": AgentActionSpec(
        action_type="restore_cohort",
        title="Restore cohort",
        permissions=("copilot.agent.confirm", "cohorts.restore"),
        requires_confirmation=True,
        risk_level="high",
    ),
    "start_experiment": AgentActionSpec(
        action_type="start_experiment",
        title="Start experiment",
        permissions=("copilot.agent.confirm", "experiments.start"),
        requires_confirmation=True,
        risk_level="high",
    ),
    "stop_experiment": AgentActionSpec(
        action_type="stop_experiment",
        title="Stop experiment",
        permissions=("copilot.agent.confirm", "experiments.stop"),
        requires_confirmation=True,
        risk_level="high",
    ),
    "record_experiment_decision": AgentActionSpec(
        action_type="record_experiment_decision",
        title="Record experiment decision",
        permissions=("copilot.agent.confirm", "experiments.decision"),
        requires_confirmation=True,
        risk_level="high",
    ),
}


class CopilotAgentModelAdapter(Protocol):
    def parse_message(
        self,
        message: str,
        *,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        ...

    def compose_message(self, payload: Dict[str, Any]) -> str:
        ...

    def draft_sql(self, prompt: str, *, session_state: Dict[str, Any], ui_context: Dict[str, Any], hint: Dict[str, Any]) -> Dict[str, Any]:
        ...


class ConfiguredCopilotAgentModel:
    def __init__(self, profile: Dict[str, Any] | None):
        self.profile = dict(profile or {})
        self.provider = str(self.profile.get("provider") or "deterministic").strip().lower() or "deterministic"
        self.model_name = str(self.profile.get("model_name") or "").strip()
        self.config = dict(self.profile.get("config") or {})
        self.gemini_client = self._build_gemini_client()

    def parse_message(
        self,
        message: str,
        *,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        heuristic = deterministic_agent_parse(message, ui_context=ui_context)
        if not self._is_ai_enabled():
            return heuristic
        prompt = {
            "task": "Classify the operator request and extract structured slots for the Kytrics/Kairyx control plane.",
            "instructions": [
                "Return JSON only.",
                "Keep the intent one of summarize_dashboard, setup_cohort, setup_experiment, setup_connection, run_prediction, setup_email_campaign, setup_workflow, list_provider_messaging_assets, draft_sql_from_prompt, setup_operator_flow, help_support, activate_cohort, pause_cohort, archive_cohort, restore_cohort, start_experiment, stop_experiment, record_experiment_decision, unsupported.",
                "Fill slots only when explicitly present or strongly implied.",
                "Do not invent SQL, identifiers, or credentials.",
            ],
            "session_state": {
                "status": session_state.get("status"),
                "current_intent": session_state.get("current_intent"),
                "last_user_message": session_state.get("last_user_message"),
            },
            "ui_context": ui_context,
            "message": message,
            "fallback": heuristic,
        }
        try:
            raw = self._request_text(prompt)
            parsed = extract_json_object(raw)
            if not isinstance(parsed, dict):
                return heuristic
            merged = dict(heuristic)
            merged["intent"] = str(parsed.get("intent") or heuristic.get("intent") or "unsupported")
            merged_slots = dict(heuristic.get("slots") or {})
            merged_slots.update(parsed.get("slots") or {})
            merged["slots"] = merged_slots
            if isinstance(parsed.get("notes"), list):
                merged["notes"] = [str(item) for item in parsed.get("notes") if str(item).strip()]
            return merged
        except Exception:
            return heuristic

    def compose_message(self, payload: Dict[str, Any]) -> str:
        fallback = deterministic_agent_message(payload)
        if not self._is_ai_enabled():
            return fallback
        prompt = {
            "task": "Write a concise operator-facing response for a constrained control-plane agent.",
            "instructions": [
                "Return JSON only.",
                "Keep the message to 2-4 sentences.",
                "Do not claim actions were executed unless completed_actions says completed.",
                "Mention missing fields when clarifications are present.",
            ],
            "response_contract": {"assistant_message": "string"},
            "payload": payload,
            "fallback": {"assistant_message": fallback},
        }
        try:
            raw = self._request_text(prompt)
            parsed = extract_json_object(raw)
            message = str(parsed.get("assistant_message") or "").strip()
            return message or fallback
        except Exception:
            return fallback

    def draft_sql(
        self,
        prompt: str,
        *,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
        hint: Dict[str, Any],
    ) -> Dict[str, Any]:
        fallback = heuristic_sql_from_prompt(prompt, hint=hint)
        if not self._is_ai_enabled():
            return fallback
        payload = {
            "task": "Draft a safe SQL audience query for the Kairyx control plane.",
            "instructions": [
                "Return JSON only.",
                "Use only the `prediction_results` table unless hint.override_table says otherwise.",
                "Always include `canonical_user_id` in the SELECT projection.",
                "Include `email` when it is available and relevant.",
                "When prediction_job_id is provided, filter on it.",
                "Do not use destructive SQL.",
            ],
            "response_contract": {
                "sql": "string",
                "query_name": "string",
                "cohort_name": "string",
            },
            "session_state": {
                "status": session_state.get("status"),
                "current_intent": session_state.get("current_intent"),
            },
            "ui_context": ui_context,
            "hint": hint,
            "message": prompt,
            "fallback": fallback,
        }
        try:
            raw = self._request_text(payload)
            parsed = extract_json_object(raw)
            if not isinstance(parsed, dict):
                return fallback
            merged = dict(fallback)
            for key in ("sql", "query_name", "cohort_name"):
                value = str(parsed.get(key) or "").strip()
                if value:
                    merged[key] = value
            return merged
        except Exception:
            return fallback

    def _is_ai_enabled(self) -> bool:
        if self.provider == "gemini":
            return self.gemini_client is not None
        if self.provider in {"openai", "anthropic"}:
            return bool(str(self.config.get("api_key") or "").strip()) and bool(self.model_name)
        return False

    def _build_gemini_client(self) -> GeminiClient | None:
        if self.provider != "gemini":
            return None
        api_key = str(self.config.get("api_key") or "").strip()
        if not api_key:
            return None
        try:
            return GeminiClient(
                api_key=api_key,
                model_name=self.model_name or None,
                circuit_namespace="copilot_agent",
            )
        except Exception:
            return None

    def _request_text(self, payload: Dict[str, Any]) -> str:
        if self.provider == "gemini" and self.gemini_client is not None:
            return self.gemini_client.get_ai_response(json.dumps(payload))
        prompt = json.dumps(payload)
        if self.provider == "openai":
            return self._call_openai(prompt)
        if self.provider == "anthropic":
            return self._call_anthropic(prompt)
        return ""

    def _call_openai(self, prompt: str) -> str:
        api_key = str(self.config.get("api_key") or "").strip()
        base_url = str(self.config.get("base_url") or "https://api.openai.com").strip().rstrip("/")
        response = requests.post(
            f"{base_url}/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": self.model_name,
                "temperature": 0,
                "messages": [
                    {"role": "system", "content": "Return JSON only."},
                    {"role": "user", "content": prompt},
                ],
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        return str((((payload.get("choices") or [{}])[0].get("message") or {}).get("content")) or "")

    def _call_anthropic(self, prompt: str) -> str:
        api_key = str(self.config.get("api_key") or "").strip()
        base_url = str(self.config.get("base_url") or "https://api.anthropic.com").strip().rstrip("/")
        response = requests.post(
            f"{base_url}/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": self.model_name,
                "max_tokens": 1200,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        content = payload.get("content") or []
        if not content:
            return ""
        first_block = content[0] or {}
        return str(first_block.get("text") or "")


def extract_json_object(raw_response: Any) -> Any:
    text = str(raw_response or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = min((index for index in (text.find("{"), text.find("[")) if index >= 0), default=-1)
        end = max(text.rfind("}"), text.rfind("]"))
        if start >= 0 and end > start:
            return json.loads(text[start : end + 1])
    return {}


def deterministic_agent_parse(message: str, *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
    normalized = str(message or "").strip()
    lowered = normalized.lower()
    slots: Dict[str, Any] = {}
    slots.update(parse_named_fields(normalized))
    slots.update(parse_json_blocks(normalized))
    sql = extract_sql_block(normalized)
    if sql:
        slots["sql"] = sql
    selected_cohort_id = str(ui_context.get("selected_cohort_id") or "").strip()
    current_experiment_id = str(ui_context.get("current_experiment_id") or "").strip()
    is_help_support_request = any(
        phrase in lowered
        for phrase in (
            "how do i",
            "how to ",
            "how can i",
            "what does this page",
            "what does this do",
            "what is this page",
            "give me a sample",
            "show me a sample",
            "show me an example",
            "sample payload",
            "example payload",
            "why is this failing",
            "why did this fail",
            "not working",
            "troubleshoot",
            "where do i",
            "where can i",
            "where should i",
            "which page",
            "what page",
            "how do i use this",
        )
    )
    if "permanent delete" in lowered or re.search(r"\bdelete\b", lowered):
        return {"intent": "unsupported", "slots": slots, "notes": ["Destructive delete flows are out of scope for the v1 agent."]}
    if is_help_support_request:
        return {"intent": "help_support", "slots": slots}
    if any(token in lowered for token in ("sendgrid", "braze", "template", "email campaign", "workflow")) and any(
        token in lowered for token in ("high risk", "churn", "prediction", "cohort", "audience")
    ):
        slots.setdefault("wants_prediction", True)
        slots.setdefault("wants_cohort", True)
        if any(token in lowered for token in ("sendgrid", "braze", "template", "email campaign")):
            slots["wants_email_campaign"] = True
        if "workflow" in lowered:
            slots["wants_workflow"] = True
        return {"intent": "setup_operator_flow", "slots": slots}
    if any(token in lowered for token in ("run prediction", "predict churn", "prediction", "score users")) and any(
        token in lowered for token in ("run", "start", "create", "reuse", "refresh", "fresh", "rerun")
    ):
        return {"intent": "run_prediction", "slots": slots}
    if any(token in lowered for token in ("write sql", "draft sql", "generate sql", "query for", "build sql")):
        return {"intent": "draft_sql_from_prompt", "slots": slots}
    if any(token in lowered for token in ("email campaign", "sendgrid", "braze", "template")) and any(
        token in lowered for token in ("set up", "setup", "create", "configure", "draft")
    ):
        return {"intent": "setup_email_campaign", "slots": slots}
    if "workflow" in lowered and any(token in lowered for token in ("set up", "setup", "create", "configure", "draft")):
        return {"intent": "setup_workflow", "slots": slots}
    if any(token in lowered for token in ("list templates", "list campaigns", "messaging assets", "provider assets", "templates available")):
        return {"intent": "list_provider_messaging_assets", "slots": slots}
    if re.search(r"\bactivate\b", lowered) and "cohort" in lowered:
        slots.setdefault("cohort_id", extract_resource_id(normalized, prefix="cohort_") or selected_cohort_id or None)
        return {"intent": "activate_cohort", "slots": slots}
    if re.search(r"\bpause\b", lowered) and "cohort" in lowered:
        slots.setdefault("cohort_id", extract_resource_id(normalized, prefix="cohort_") or selected_cohort_id or None)
        return {"intent": "pause_cohort", "slots": slots}
    if re.search(r"\barchive\b", lowered) and "cohort" in lowered:
        slots.setdefault("cohort_id", extract_resource_id(normalized, prefix="cohort_") or selected_cohort_id or None)
        return {"intent": "archive_cohort", "slots": slots}
    if re.search(r"\brestore\b", lowered) and "cohort" in lowered:
        slots.setdefault("cohort_id", extract_resource_id(normalized, prefix="cohort_") or selected_cohort_id or None)
        return {"intent": "restore_cohort", "slots": slots}
    if re.search(r"\bstart\b", lowered) and ("experiment" in lowered or "a/b" in lowered or "ab test" in lowered):
        slots.setdefault("experiment_id", extract_experiment_id(normalized) or current_experiment_id or None)
        return {"intent": "start_experiment", "slots": slots}
    if re.search(r"\bstop\b", lowered) and "experiment" in lowered:
        slots.setdefault("experiment_id", extract_experiment_id(normalized) or current_experiment_id or None)
        return {"intent": "stop_experiment", "slots": slots}
    if "record decision" in lowered and "experiment" in lowered:
        slots.setdefault("experiment_id", extract_experiment_id(normalized) or current_experiment_id or None)
        return {"intent": "record_experiment_decision", "slots": slots}
    if any(phrase in lowered for phrase in ("summarize dashboard", "summarise dashboard", "dashboard summary", "summarize the dashboard", "summarise the dashboard")):
        return {"intent": "summarize_dashboard", "slots": slots}
    if any(phrase in lowered for phrase in ("set up a connection", "setup a connection", "create a connection", "set up connection", "setup connection")):
        scope, connection_type = detect_connection_scope_and_type(lowered)
        if scope:
            slots["connection_scope"] = scope
        if connection_type:
            slots["connection_type"] = connection_type
        return {"intent": "setup_connection", "slots": slots}
    if "connector" in lowered or any(token in lowered for values in CONNECTOR_TYPE_SYNONYMS.values() for token in values):
        scope, connection_type = detect_connection_scope_and_type(lowered)
        if scope == "connector" or connection_type in CONNECTOR_REQUIRED_FIELDS:
            if scope:
                slots["connection_scope"] = scope
            if connection_type:
                slots["connection_type"] = connection_type
            return {"intent": "setup_connection", "slots": slots}
    if "provider connection" in lowered or any(token in lowered for values in PROVIDER_CONNECTION_TYPE_SYNONYMS.values() for token in values):
        scope, connection_type = detect_connection_scope_and_type(lowered)
        if scope == "provider_connection" or connection_type in PROVIDER_CONNECTION_REQUIRED_FIELDS:
            if scope:
                slots["connection_scope"] = scope
            if connection_type:
                slots["connection_type"] = connection_type
            return {"intent": "setup_connection", "slots": slots}
    if any(token in lowered for token in ("a/b test", "ab test", "a b test", "experiment")) and any(token in lowered for token in ("set up", "setup", "create", "configure")):
        slots.setdefault("experiment_id", extract_experiment_id(normalized) or current_experiment_id or None)
        slots.setdefault("cohort_id", extract_resource_id(normalized, prefix="cohort_") or selected_cohort_id or None)
        return {"intent": "setup_experiment", "slots": slots}
    if any(token in lowered for token in ("cohort", "audience")) and any(token in lowered for token in ("set up", "setup", "create", "configure")):
        slots.setdefault("cohort_id", extract_resource_id(normalized, prefix="cohort_") or selected_cohort_id or None)
        if "select" in lowered or sql:
            slots["cohort_type"] = "sql"
        elif "rule" in lowered:
            slots["cohort_type"] = "rule"
        elif "list" in lowered or "members" in lowered:
            slots["cohort_type"] = "list"
        return {"intent": "setup_cohort", "slots": slots}
    return {"intent": "unsupported", "slots": slots}


def deterministic_agent_message(payload: Dict[str, Any]) -> str:
    support_answer = str(payload.get("support_answer") or "").strip()
    if support_answer:
        return support_answer
    async_status = str(payload.get("async_status") or "").strip().lower()
    if async_status == "waiting_for_prediction":
        return "I started the prediction job. Continue once it finishes and I will build the remaining draft artifacts."
    if async_status == "ready_to_resume":
        return "The prediction has completed. Continue and I will build the saved query, cohort, and draft delivery assets."
    clarifications = payload.get("clarifications") or []
    completed_actions = payload.get("completed_actions") or []
    pending_confirmations = payload.get("pending_confirmations") or []
    if clarifications:
        questions = ", ".join(str(item.get("label") or item.get("key") or "detail") for item in clarifications[:3])
        return f"I can handle this, but I still need: {questions}."
    if pending_confirmations:
        action = pending_confirmations[0]
        return f"I prepared the action `{action.get('title')}` and held it for confirmation because it is high risk."
    if completed_actions:
        action = completed_actions[-1]
        summary = str(action.get("summary") or "").strip()
        if summary:
            return summary
        return f"Completed `{action.get('title')}`."
    preview = payload.get("execution_preview") or {}
    if preview.get("summary"):
        return str(preview["summary"])
    return "I reviewed the request and prepared the next step."


class CopilotAgentService:
    def __init__(self, repository, settings, bigquery_service: BigQueryService | None = None):
        self.repository = repository
        self.settings = settings
        self.bigquery_service = bigquery_service or get_shared_bigquery_service()
        self.copilot = CopilotService(repository, settings, self.bigquery_service)
        self.cohorts = CohortService(repository, self.bigquery_service)
        self.experiments = ExperimentConfigService(repository)
        self.connectors = ConnectorService(repository)
        self.provider_connections = ProviderConnectionService(repository)
        self.sql_workspace = SqlWorkspaceService(repository, settings, self.bigquery_service)
        self.health_monitor = HealthMonitorService(repository, self.bigquery_service)
        self.email_campaigns = EmailCampaignService(repository, settings, self.bigquery_service)
        self.workflows = WorkflowService(repository)
        self.predictions = PredictionService(repository, settings, self.bigquery_service)
        self.sendgrid_provider = SendGridProviderService(repository)
        self.braze_provider = BrazeProviderService(repository)
        self.model_profiles = AgentModelProfileService(repository)

    def create_session(
        self,
        *,
        title: str = "",
        model_profile_id: str | None = None,
        ui_context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        resolved_model = self._resolve_session_model(model_profile_id)
        session_id = f"cpa_{uuid.uuid4().hex[:20]}"
        payload = {
            "session_id": session_id,
            "title": str(title or "Operator Agent Session").strip() or "Operator Agent Session",
            "status": "active",
            "current_intent": None,
            "last_user_message": "",
            "ui_context": dict(ui_context or {}),
            "latest_execution_preview": None,
            "latest_artifacts": [],
            "latest_clarifications": [],
            "draft_slots": {},
            "pending_confirmation_count": 0,
            "model_profile_id": resolved_model.get("model_profile_id"),
            "effective_provider": resolved_model.get("effective_provider", "deterministic"),
            "effective_model_name": resolved_model.get("effective_model_name", ""),
            "model_selection_source": resolved_model.get("model_selection_source", "deterministic_fallback"),
            "async_status": "",
            "waiting_for_action_type": None,
            "waiting_for_resource_id": None,
            "pending_flow": None,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(SESSION_RESOURCE_TYPE, session_id, status="active", name=payload["title"], payload=payload)
        self.repository.record_resource_event(SESSION_RESOURCE_TYPE, session_id, event_type="session_created", payload=payload)
        return {
            "session_state": self._session_state(payload),
            "pending_confirmations": [],
            "latest_turn": None,
        }

    def get_session(self, session_id: str) -> Dict[str, Any]:
        payload = self._decorate_session_async_state(self._get_session_payload(session_id))
        turns = self.list_turns(session_id)["items"]
        return {
            "session_state": self._session_state(payload),
            "pending_confirmations": self._pending_confirmation_actions(session_id),
            "latest_turn": turns[-1] if turns else None,
        }

    def list_turns(self, session_id: str) -> Dict[str, Any]:
        self._get_session_payload(session_id)
        turns = [
            self._turn_from_record(record)
            for record in self.repository.list_resources(TURN_RESOURCE_TYPE)
            if str((record.get("payload") or {}).get("session_id") or "") == session_id
        ]
        turns.sort(key=lambda item: str(item.get("created_at") or ""))
        return {"items": turns}

    def handle_message(
        self,
        session_id: str,
        *,
        message: str,
        ui_context: Dict[str, Any] | None,
        context: GovernanceContext,
    ) -> Dict[str, Any]:
        session = self._decorate_session_async_state(self._get_session_payload(session_id))
        merged_ui_context = dict(session.get("ui_context") or {})
        merged_ui_context.update(dict(ui_context or {}))
        model_adapter = self._model_adapter_for_session(session)

        resumed = self._maybe_resume_pending_flow(
            session,
            message=message,
            ui_context=merged_ui_context,
            context=context,
            model_adapter=model_adapter,
        )
        if resumed is not None:
            return resumed

        parsed = self._normalize_parsed_request(
            session,
            model_adapter.parse_message(message, session_state=session, ui_context=merged_ui_context),
            message=message,
        )
        plan = self._build_plan(message=message, parsed=parsed, ui_context=merged_ui_context, context=context)

        completed_actions: List[Dict[str, Any]] = []
        pending_confirmations: List[Dict[str, Any]] = []
        artifacts: List[Dict[str, Any]] = []
        session_status = "active"
        execution_result: Dict[str, Any] = {}

        if plan["clarifications"]:
            session_status = "awaiting_input"
        else:
            execution_result = self._execute_plan(
                session_id=session_id,
                plan=plan,
                context=context,
                session=session,
                ui_context=merged_ui_context,
                model_adapter=model_adapter,
            )
            completed_actions = execution_result["completed_actions"]
            pending_confirmations = execution_result["pending_confirmations"]
            artifacts = execution_result["artifacts"]
            session_status = execution_result["session_status"]

        response_payload = {
            "assistant_message": "",
            "session_state": {},
            "clarifications": plan["clarifications"],
            "execution_preview": plan["execution_preview"],
            "completed_actions": completed_actions,
            "pending_confirmations": pending_confirmations,
            "artifacts": artifacts or collect_artifacts_from_actions(completed_actions, pending_confirmations),
        }
        assistant_message = str(plan.get("assistant_message") or "").strip()
        if assistant_message:
            response_payload["assistant_message"] = assistant_message
        else:
            response_payload["assistant_message"] = model_adapter.compose_message(
                {
                    **response_payload,
                    "async_status": session_status if session_status in {"waiting_for_prediction", "ready_to_resume"} else "",
                }
            )

        turn_payload = {
            "turn_id": f"cpat_{uuid.uuid4().hex[:20]}",
            "session_id": session_id,
            "user_message": str(message or "").strip(),
            "assistant_message": response_payload["assistant_message"],
            "intent": plan["intent"],
            "status": session_status,
            "clarifications": plan["clarifications"],
            "execution_preview": plan["execution_preview"],
            "completed_actions": completed_actions,
            "pending_confirmations": pending_confirmations,
            "artifacts": response_payload["artifacts"],
            "ui_context": merged_ui_context,
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(TURN_RESOURCE_TYPE, turn_payload["turn_id"], status=session_status, name=plan["intent"], payload=turn_payload)
        self.repository.record_resource_event(SESSION_RESOURCE_TYPE, session_id, event_type="turn_recorded", payload={"turn_id": turn_payload["turn_id"], "intent": plan["intent"], "status": session_status})

        session.update(
            {
                "status": session_status,
                "current_intent": plan["intent"],
                "last_user_message": turn_payload["user_message"],
                "ui_context": merged_ui_context,
                "latest_execution_preview": plan["execution_preview"],
                "latest_artifacts": response_payload["artifacts"],
                "latest_clarifications": plan["clarifications"],
                "draft_slots": dict(plan["slots"] or {}) if plan["clarifications"] else {},
                "pending_confirmation_count": len(pending_confirmations),
                "async_status": execution_result.get("async_status", session.get("async_status") or ""),
                "waiting_for_action_type": execution_result.get("waiting_for_action_type", session.get("waiting_for_action_type")),
                "waiting_for_resource_id": execution_result.get("waiting_for_resource_id", session.get("waiting_for_resource_id")),
                "pending_flow": execution_result.get("pending_flow", session.get("pending_flow")),
                "updated_at": datetime.utcnow().isoformat(),
            }
        )
        self.repository.upsert_resource(SESSION_RESOURCE_TYPE, session_id, status=session_status, name=session.get("title"), payload=session)
        response_payload["session_state"] = self._session_state(session)
        return response_payload

    def _normalize_parsed_request(self, session: Dict[str, Any], parsed: Dict[str, Any], *, message: str) -> Dict[str, Any]:
        normalized = dict(parsed or {})
        incoming_intent = str(normalized.get("intent") or "unsupported")
        incoming_slots = merge_slots({}, normalized.get("slots") or {})
        session_intent = str(session.get("current_intent") or "").strip()
        session_status = str(session.get("status") or "").strip().lower()
        draft_slots = dict(session.get("draft_slots") or {})

        if session_status == "awaiting_input" and session_intent and incoming_intent in {"", "unsupported", session_intent}:
            normalized["intent"] = session_intent
            normalized["slots"] = merge_slots(draft_slots, incoming_slots)
        else:
            normalized["slots"] = incoming_slots

        normalized["slots"]["source_message"] = str(message or "").strip()
        return normalized

    def confirm_action(self, action_id: str, *, note: str, context: GovernanceContext) -> Dict[str, Any]:
        action_record = self.repository.get_resource(ACTION_RESOURCE_TYPE, action_id)
        if action_record is None:
            raise KeyError(action_id)
        action_payload = dict(action_record.get("payload") or {})
        if str(action_payload.get("status") or "") != "awaiting_confirmation":
            raise ValueError(f"Agent action '{action_id}' is not awaiting confirmation.")
        session_id = str(action_payload.get("session_id") or "")
        session = self._get_session_payload(session_id)
        confirmation_id = str(action_payload.get("confirmation_id") or "")
        confirmation_record = self.repository.get_resource(CONFIRMATION_RESOURCE_TYPE, confirmation_id)
        if confirmation_record is None:
            raise KeyError(confirmation_id)
        confirmation_payload = dict(confirmation_record.get("payload") or {})
        ensure_permissions_for_action(action_payload["action_type"], context)

        result = self._execute_action(
            action_payload["action_type"],
            action_payload.get("parameters") or {},
            context=context,
            session=session,
            ui_context=dict(session.get("ui_context") or {}),
            model_adapter=self._model_adapter_for_session(session),
        )
        artifacts = result.get("artifacts") or []
        action_payload.update(
            {
                "status": "completed",
                "result": result.get("result") or {},
                "summary": result.get("summary") or deterministic_action_summary(action_payload["action_type"], result.get("result") or {}),
                "artifacts": artifacts,
                "updated_at": datetime.utcnow().isoformat(),
                "confirmation_note": str(note or ""),
            }
        )
        self.repository.upsert_resource(ACTION_RESOURCE_TYPE, action_id, status="completed", name=action_payload.get("title"), payload=action_payload)
        self.repository.record_action(
            "copilot_agent_action_completed",
            ACTION_RESOURCE_TYPE,
            action_id,
            {
                "action_type": action_payload["action_type"],
                "artifacts": artifacts,
                "parameters": action_payload.get("parameters") or {},
                "result": action_payload.get("result") or {},
            },
        )

        confirmation_payload.update(
            {
                "status": "confirmed",
                "confirmed_at": datetime.utcnow().isoformat(),
                "note": str(note or ""),
                "result_status": "completed",
            }
        )
        self.repository.upsert_resource(CONFIRMATION_RESOURCE_TYPE, confirmation_id, status="confirmed", name=confirmation_payload.get("title"), payload=confirmation_payload)

        model_adapter = self._model_adapter_for_session(session)
        assistant_message = model_adapter.compose_message(
            {
                "completed_actions": [action_payload],
                "pending_confirmations": [],
                "clarifications": [],
                "execution_preview": self._preview_from_actions([action_payload]),
            }
        )
        turn_payload = {
            "turn_id": f"cpat_{uuid.uuid4().hex[:20]}",
            "session_id": session_id,
            "user_message": f"Confirmed action {action_payload.get('title')}",
            "assistant_message": assistant_message,
            "intent": action_payload["action_type"],
            "status": "active",
            "clarifications": [],
            "execution_preview": self._preview_from_actions([action_payload]),
            "completed_actions": [action_payload],
            "pending_confirmations": [],
            "artifacts": artifacts,
            "ui_context": dict(session.get("ui_context") or {}),
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(TURN_RESOURCE_TYPE, turn_payload["turn_id"], status="active", name=action_payload["action_type"], payload=turn_payload)

        session.update(
            {
                "status": "active",
                "current_intent": action_payload["action_type"],
                "latest_execution_preview": self._preview_from_actions([action_payload]),
                "latest_artifacts": artifacts,
                "latest_clarifications": [],
                "draft_slots": {},
                "pending_confirmation_count": len(self._pending_confirmation_actions(session_id)),
                "async_status": "",
                "waiting_for_action_type": None,
                "waiting_for_resource_id": None,
                "pending_flow": None,
                "updated_at": datetime.utcnow().isoformat(),
            }
        )
        self.repository.upsert_resource(SESSION_RESOURCE_TYPE, session_id, status="active", name=session.get("title"), payload=session)
        return {
            "assistant_message": assistant_message,
            "session_state": self._session_state(session),
            "clarifications": [],
            "execution_preview": self._preview_from_actions([action_payload]),
            "completed_actions": [action_payload],
            "pending_confirmations": self._pending_confirmation_actions(session_id),
            "artifacts": artifacts,
        }

    def _build_plan(
        self,
        *,
        message: str,
        parsed: Dict[str, Any],
        ui_context: Dict[str, Any],
        context: GovernanceContext,
    ) -> Dict[str, Any]:
        intent = str(parsed.get("intent") or "unsupported")
        slots = dict(parsed.get("slots") or {})
        clarifications: List[Dict[str, Any]] = []
        actions: List[Dict[str, Any]] = []
        notes: List[str] = [str(item) for item in parsed.get("notes") or [] if str(item).strip()]
        assistant_message = ""
        if intent in {"help_support", "unsupported"}:
            intent = "help_support"
            assistant_message = build_help_support_answer(message, ui_context=ui_context)
            if notes:
                assistant_message = f"{notes[0]}\n\n{assistant_message}"
        elif intent == "summarize_dashboard":
            actions.append(
                {
                    "action_type": "summarize_dashboard",
                    "title": ACTION_REGISTRY["summarize_dashboard"].title,
                    "parameters": {"ui_context": ui_context},
                }
            )
        elif intent == "setup_connection":
            clarifications.extend(self._connection_clarifications(slots))
            if not clarifications:
                actions.append(self._connection_action(slots))
                if slots.get("connection_scope") == "connector":
                    actions.append(
                        {
                            "action_type": "check_connector_health",
                            "title": ACTION_REGISTRY["check_connector_health"].title,
                            "parameters": {
                                "name": slots["name"],
                            },
                            "optional": True,
                        }
                    )
        elif intent == "setup_cohort":
            clarifications.extend(self._cohort_clarifications(slots, context=context))
            if not clarifications:
                actions.extend(self._cohort_actions(slots, context=context))
        elif intent == "setup_experiment":
            clarifications.extend(self._experiment_clarifications(slots, ui_context=ui_context))
            if not clarifications:
                actions.append(self._experiment_action(slots, ui_context=ui_context))
        elif intent == "run_prediction":
            clarifications.extend(self._prediction_clarifications(slots, ui_context=ui_context))
            if not clarifications:
                actions.append(self._prediction_action(slots, ui_context=ui_context))
        elif intent == "draft_sql_from_prompt":
            clarifications.extend(self._sql_draft_clarifications(slots, ui_context=ui_context))
            if not clarifications:
                actions.append(self._sql_draft_action(slots, ui_context=ui_context))
        elif intent == "list_provider_messaging_assets":
            clarifications.extend(self._provider_asset_clarifications(slots, ui_context=ui_context))
            if not clarifications:
                actions.append(self._provider_asset_action(slots, ui_context=ui_context))
        elif intent == "setup_email_campaign":
            clarifications.extend(self._email_campaign_clarifications(slots, ui_context=ui_context))
            if not clarifications:
                actions.append(self._email_campaign_action(slots, ui_context=ui_context))
        elif intent == "setup_workflow":
            clarifications.extend(self._workflow_clarifications(slots, ui_context=ui_context))
            if not clarifications:
                actions.append(self._workflow_action(slots, ui_context=ui_context))
        elif intent == "setup_operator_flow":
            clarifications.extend(self._operator_flow_clarifications(slots, ui_context=ui_context))
            if not clarifications:
                actions.append(self._operator_flow_action(slots, ui_context=ui_context))
        elif intent in {"activate_cohort", "pause_cohort", "archive_cohort", "restore_cohort"}:
            cohort_id = str(slots.get("cohort_id") or "").strip()
            if not cohort_id:
                clarifications.append(
                    {
                        "key": "cohort_id",
                        "label": "Cohort ID",
                        "question": "Which cohort should I target?",
                        "required": True,
                        "input_type": "text",
                    }
                )
            else:
                risky_action = {
                    "activate_cohort": "activate_cohort",
                    "pause_cohort": "pause_cohort",
                    "archive_cohort": "archive_cohort",
                    "restore_cohort": "restore_cohort",
                }[intent]
                actions.append(
                    {
                        "action_type": risky_action,
                        "title": ACTION_REGISTRY[risky_action].title,
                        "parameters": {"cohort_id": cohort_id},
                    }
                )
        elif intent in {"start_experiment", "stop_experiment", "record_experiment_decision"}:
            experiment_id = str(slots.get("experiment_id") or "").strip()
            if not experiment_id:
                clarifications.append(
                    {
                        "key": "experiment_id",
                        "label": "Experiment ID",
                        "question": "Which experiment should I target?",
                        "required": True,
                        "input_type": "text",
                    }
                )
            else:
                risky_action = {
                    "start_experiment": "start_experiment",
                    "stop_experiment": "stop_experiment",
                    "record_experiment_decision": "record_experiment_decision",
                }[intent]
                actions.append(
                    {
                        "action_type": risky_action,
                        "title": ACTION_REGISTRY[risky_action].title,
                        "parameters": {"experiment_id": experiment_id, "decided_by": context.actor_id},
                    }
                )

        execution_preview = self._build_execution_preview(intent, actions, clarifications, notes)
        return {
            "intent": intent,
            "slots": slots,
            "clarifications": clarifications,
            "actions": actions,
            "execution_preview": execution_preview,
            "notes": notes,
            "user_message": message,
            "assistant_message": assistant_message,
        }

    def _connection_clarifications(self, slots: Dict[str, Any]) -> List[Dict[str, Any]]:
        clarifications: List[Dict[str, Any]] = []
        scope = str(slots.get("connection_scope") or "").strip()
        connection_type = str(slots.get("connection_type") or "").strip()
        if not scope:
            clarifications.append(
                {
                    "key": "connection_scope",
                    "label": "Connection Type",
                    "question": "Should I set up an upstream data connector or a downstream provider connection?",
                    "required": True,
                    "input_type": "choice",
                    "options": ["connector", "provider_connection"],
                }
            )
            return clarifications
        if not connection_type:
            clarifications.append(
                {
                    "key": "connection_type",
                    "label": "Provider",
                    "question": "Which connector or provider should I configure?",
                    "required": True,
                    "input_type": "choice",
                    "options": sorted(CONNECTOR_REQUIRED_FIELDS.keys()) if scope == "connector" else sorted(PROVIDER_CONNECTION_REQUIRED_FIELDS.keys()),
                }
            )
            return clarifications
        slots.setdefault("name", slots.get("name") or default_named_resource(prefix=connection_type, suffix="connection"))
        required_fields = CONNECTOR_REQUIRED_FIELDS.get(connection_type, []) if scope == "connector" else PROVIDER_CONNECTION_REQUIRED_FIELDS.get(connection_type, [])
        config = dict(slots.get("config") or {})
        for field in required_fields:
            if str(config.get(field) or "").strip():
                continue
            clarifications.append(
                {
                    "key": field,
                    "label": humanize_field(field),
                    "question": f"What should I use for `{field}` on the {connection_type} {scope.replace('_', ' ')}?",
                    "required": True,
                    "input_type": "text",
                }
            )
        return clarifications

    def _connection_action(self, slots: Dict[str, Any]) -> Dict[str, Any]:
        scope = str(slots["connection_scope"])
        connection_type = str(slots["connection_type"])
        config = dict(slots.get("config") or {})
        if scope == "connector":
            return {
                "action_type": "upsert_connector",
                "title": ACTION_REGISTRY["upsert_connector"].title,
                "parameters": {
                    "name": slots["name"],
                    "connector_type": connection_type,
                    "config": config,
                },
            }
        return {
            "action_type": "upsert_provider_connection",
            "title": ACTION_REGISTRY["upsert_provider_connection"].title,
            "parameters": {
                "name": slots["name"],
                "provider": connection_type,
                "config": config,
                "update_existing": bool(slots.get("update_existing")),
            },
        }

    def _cohort_clarifications(self, slots: Dict[str, Any], *, context: GovernanceContext) -> List[Dict[str, Any]]:
        clarifications: List[Dict[str, Any]] = []
        cohort_type = str(slots.get("cohort_type") or "").strip().lower()
        if not cohort_type:
            clarifications.append(
                {
                    "key": "cohort_type",
                    "label": "Cohort Type",
                    "question": "Should this cohort be SQL, rule-based, or list-based?",
                    "required": True,
                    "input_type": "choice",
                    "options": ["sql", "rule", "list"],
                }
            )
            return clarifications
        slots.setdefault("name", slots.get("name") or default_named_resource(prefix="agent", suffix="cohort"))
        slots.setdefault("refresh_mode", "daily" if "daily" in str(slots.get("source_message") or "").lower() else "manual")
        slots.setdefault("owner", context.actor_id)
        if cohort_type == "sql":
            if not str(slots.get("sql") or "").strip():
                clarifications.append(
                    {
                        "key": "sql",
                        "label": "SQL",
                        "question": "What SQL should I use to build the cohort? Include `canonical_user_id` in the result.",
                        "required": True,
                        "input_type": "code",
                    }
                )
        elif cohort_type == "rule":
            if not isinstance(slots.get("definition_json"), dict) or not slots.get("definition_json"):
                clarifications.append(
                    {
                        "key": "definition_json",
                        "label": "Rule Definition JSON",
                        "question": "Send the rule cohort definition JSON so I can create the draft safely.",
                        "required": True,
                        "input_type": "code",
                    }
                )
        elif cohort_type == "list":
            members = slots.get("members")
            definition_json = slots.get("definition_json")
            if not isinstance(members, list) and not (isinstance(definition_json, dict) and isinstance(definition_json.get("members"), list)):
                clarifications.append(
                    {
                        "key": "members",
                        "label": "Members JSON",
                        "question": "Send the list cohort members JSON. Each member should include `canonical_user_id` and any optional identifiers like `email`.",
                        "required": True,
                        "input_type": "code",
                    }
                )
        return clarifications

    def _cohort_actions(self, slots: Dict[str, Any], *, context: GovernanceContext) -> List[Dict[str, Any]]:
        cohort_type = str(slots["cohort_type"]).lower()
        name = str(slots["name"])
        refresh_mode = str(slots.get("refresh_mode") or "manual")
        owner = str(slots.get("owner") or context.actor_id)
        description = str(slots.get("description") or f"Created by the operator agent from: {slots.get('source_message') or 'user request'}")
        tags = list(slots.get("tags") or ["agent"])
        if cohort_type == "sql":
            sql = str(slots["sql"])
            safe_query_name = str(slots.get("saved_query_name") or f"{name}_query")
            return [
                {
                    "action_type": "preview_sql",
                    "title": ACTION_REGISTRY["preview_sql"].title,
                    "parameters": {
                        "sql": sql,
                        "limit": 20,
                        "timeout_seconds": 30,
                    },
                },
                {
                    "action_type": "save_query",
                    "title": ACTION_REGISTRY["save_query"].title,
                    "parameters": {
                        "name": safe_query_name,
                        "description": f"Saved by the operator agent for cohort {name}.",
                        "sql": sql,
                    },
                },
                {
                    "action_type": "create_cohort_sql",
                    "title": ACTION_REGISTRY["create_cohort_sql"].title,
                    "parameters": {
                        "name": name,
                        "cohort_type": "sql",
                        "definition": {"sql": sql},
                        "refresh_mode": refresh_mode,
                        "owner": owner,
                        "description": description,
                        "tags": tags,
                        "activate": False,
                    },
                },
            ]
        definition = (
            dict(slots.get("definition_json") or {})
            if cohort_type == "rule"
            else {"members": list(slots.get("members") or (slots.get("definition_json") or {}).get("members") or [])}
        )
        action_type = "update_cohort_definition" if str(slots.get("cohort_id") or "").strip() and bool(slots.get("update_existing")) else "create_cohort_definition"
        parameters = {
            "name": name,
            "cohort_type": cohort_type,
            "definition": definition,
            "refresh_mode": refresh_mode,
            "owner": owner,
            "description": description,
            "tags": tags,
            "activate": False,
        }
        if action_type == "update_cohort_definition":
            parameters["cohort_id"] = slots["cohort_id"]
        return [
            {
                "action_type": action_type,
                "title": ACTION_REGISTRY[action_type].title,
                "parameters": parameters,
            }
        ]

    def _experiment_clarifications(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        clarifications: List[Dict[str, Any]] = []
        cohort_id = str(slots.get("cohort_id") or ui_context.get("selected_cohort_id") or "").strip()
        if not cohort_id:
            clarifications.append(
                {
                    "key": "cohort_id",
                    "label": "Linked Cohort",
                    "question": "Which cohort should this experiment use?",
                    "required": True,
                    "input_type": "text",
                }
            )
        return clarifications

    def _experiment_action(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        experiment_id = str(slots.get("experiment_id") or ui_context.get("current_experiment_id") or default_named_resource(prefix="agent", suffix="experiment")).replace(" ", "_")
        cohort_id = str(slots.get("cohort_id") or ui_context.get("selected_cohort_id") or "")
        primary_metric = str(slots.get("primary_metric") or "return_rate")
        guardrail_metrics = list(slots.get("guardrail_metrics") or ["engagement_rate", "policy_block_rate"])
        min_sample_size = int(slots.get("min_sample_size") or 20)
        min_runtime_hours = int(slots.get("min_runtime_hours") or 24)
        holdout_pct = float(slots.get("holdout_pct") or 0.10)
        b_variant_pct = float(slots.get("b_variant_pct") or 0.50)
        return {
            "action_type": "save_experiment_config",
            "title": ACTION_REGISTRY["save_experiment_config"].title,
            "parameters": {
                "experiment_id": experiment_id,
                "enabled": False,
                "primary_metric": primary_metric,
                "guardrail_metrics": guardrail_metrics,
                "min_sample_size": min_sample_size,
                "min_runtime_hours": min_runtime_hours,
                "cohort_id": cohort_id,
                "holdout_pct": holdout_pct,
                "b_variant_pct": b_variant_pct,
                "scenario_type": "agent_setup",
            },
        }

    def _prediction_clarifications(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        clarifications: List[Dict[str, Any]] = []
        target = self._prediction_target_from_slots(slots, ui_context=ui_context)
        if target.get("prediction_job_id"):
            return clarifications
        if target.get("import_job_id") or target.get("source_name"):
            return clarifications
        source_options = self._prediction_source_options()
        clarifications.append(
            {
                "key": "source_name",
                "label": "Prediction Source",
                "question": "Which source should I use for the churn prediction audience?",
                "required": True,
                "input_type": "choice" if source_options else "text",
                "options": source_options,
            }
        )
        return clarifications

    def _prediction_action(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        target = self._prediction_target_from_slots(slots, ui_context=ui_context)
        prediction_mode = str(
            slots.get("prediction_mode")
            or ui_context.get("selected_prediction_mode")
            or "local"
        ).strip().lower() or "local"
        return {
            "action_type": "run_prediction",
            "title": ACTION_REGISTRY["run_prediction"].title,
            "parameters": {
                **target,
                "prediction_mode": prediction_mode,
                "force_prediction_refresh": bool(slots.get("force_prediction_refresh")),
                "source_message": str(slots.get("source_message") or ""),
            },
        }

    def _sql_draft_clarifications(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        target = self._prediction_target_from_slots(slots, ui_context=ui_context)
        if target.get("prediction_job_id"):
            return []
        return self._prediction_clarifications(slots, ui_context=ui_context)

    def _sql_draft_action(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        target = self._prediction_target_from_slots(slots, ui_context=ui_context)
        return {
            "action_type": "draft_sql_from_prompt",
            "title": ACTION_REGISTRY["draft_sql_from_prompt"].title,
            "parameters": {
                **target,
                "source_message": str(slots.get("source_message") or ""),
                "include_risks": list(slots.get("include_risks") or ["high"]),
                "query_name": str(slots.get("saved_query_name") or ""),
                "cohort_name": str(slots.get("cohort_name") or slots.get("name") or ""),
            },
        }

    def _provider_asset_clarifications(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        clarifications: List[Dict[str, Any]] = []
        provider_connection = self._resolve_provider_connection_candidate(slots, ui_context=ui_context)
        if provider_connection is None:
            options = [str(item.get("provider_connection_id") or "") for item in self.provider_connections.list_connections()]
            clarifications.append(
                {
                    "key": "provider_connection_id",
                    "label": "Provider Connection",
                    "question": "Which SendGrid or Braze provider connection should I use?",
                    "required": True,
                    "input_type": "choice" if options else "text",
                    "options": options,
                }
            )
        return clarifications

    def _provider_asset_action(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        provider_connection = self._resolve_provider_connection_candidate(slots, ui_context=ui_context)
        return {
            "action_type": "list_provider_messaging_assets",
            "title": ACTION_REGISTRY["list_provider_messaging_assets"].title,
            "parameters": {
                "provider_connection_id": str((provider_connection or {}).get("provider_connection_id") or slots.get("provider_connection_id") or ""),
                "template_hint": str(slots.get("template_hint") or slots.get("template_id") or ""),
            },
        }

    def _email_campaign_clarifications(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        clarifications = self._provider_asset_clarifications(slots, ui_context=ui_context)
        provider_connection = self._resolve_provider_connection_candidate(slots, ui_context=ui_context)
        if provider_connection is not None and not str(slots.get("template_id") or "").strip():
            options = [str(item.get("asset_id") or item.get("id") or "") for item in self._list_provider_assets(str(provider_connection.get("provider_connection_id") or ""))]
            clarifications.append(
                {
                    "key": "template_id",
                    "label": "Template / Campaign Asset",
                    "question": "Which existing provider messaging asset should I use for the draft email campaign?",
                    "required": True,
                    "input_type": "choice" if options else "text",
                    "options": options,
                }
            )
        audience_cohort_id = str(slots.get("cohort_id") or ui_context.get("selected_cohort_id") or "").strip()
        prediction_target = self._prediction_target_from_slots(slots, ui_context=ui_context)
        if not audience_cohort_id and not prediction_target.get("prediction_job_id"):
            clarifications.append(
                {
                    "key": "cohort_id",
                    "label": "Audience Cohort",
                    "question": "Which cohort should the draft email campaign target?",
                    "required": True,
                    "input_type": "text",
                }
            )
        return clarifications

    def _email_campaign_action(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        provider_connection = self._resolve_provider_connection_candidate(slots, ui_context=ui_context)
        return {
            "action_type": "setup_email_campaign",
            "title": ACTION_REGISTRY["setup_email_campaign"].title,
            "parameters": {
                "provider_connection_id": str((provider_connection or {}).get("provider_connection_id") or slots.get("provider_connection_id") or ""),
                "template_id": str(slots.get("template_id") or ""),
                "cohort_id": str(slots.get("cohort_id") or ui_context.get("selected_cohort_id") or ""),
                "prediction_job_id": str(slots.get("prediction_job_id") or ""),
                "campaign_name": str(slots.get("campaign_name") or slots.get("name") or ""),
                "schedule_at": slots.get("schedule_at"),
                "include_risks": list(slots.get("include_risks") or ["high"]),
            },
        }

    def _workflow_clarifications(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        cohort_id = str(slots.get("cohort_id") or ui_context.get("selected_cohort_id") or "").strip()
        if cohort_id:
            return []
        return [
            {
                "key": "cohort_id",
                "label": "Audience Cohort",
                "question": "Which cohort should the workflow draft target?",
                "required": True,
                "input_type": "text",
            }
        ]

    def _workflow_action(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "action_type": "setup_workflow",
            "title": ACTION_REGISTRY["setup_workflow"].title,
            "parameters": {
                "cohort_id": str(slots.get("cohort_id") or ui_context.get("selected_cohort_id") or ""),
                "workflow_name": str(slots.get("workflow_name") or slots.get("name") or ""),
                "email_campaign_id": str(slots.get("email_campaign_id") or ""),
                "experiment_id": str(slots.get("experiment_id") or ui_context.get("current_experiment_id") or ""),
            },
        }

    def _operator_flow_clarifications(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        clarifications = self._prediction_clarifications(slots, ui_context=ui_context)
        wants_email_campaign = bool(slots.get("wants_email_campaign")) or bool(slots.get("wants_workflow"))
        if wants_email_campaign:
            clarifications.extend(self._provider_asset_clarifications(slots, ui_context=ui_context))
            provider_connection = self._resolve_provider_connection_candidate(slots, ui_context=ui_context)
            if provider_connection is not None and not str(slots.get("template_id") or "").strip():
                options = [str(item.get("asset_id") or item.get("id") or "") for item in self._list_provider_assets(str(provider_connection.get("provider_connection_id") or ""))]
                clarifications.append(
                    {
                        "key": "template_id",
                        "label": "Template / Campaign Asset",
                        "question": "Which existing provider messaging asset should I use for the campaign draft?",
                        "required": True,
                        "input_type": "choice" if options else "text",
                        "options": options,
                    }
                )
        return dedupe_clarifications(clarifications)

    def _operator_flow_action(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        provider_connection = self._resolve_provider_connection_candidate(slots, ui_context=ui_context)
        target = self._prediction_target_from_slots(slots, ui_context=ui_context)
        return {
            "action_type": "setup_operator_flow",
            "title": ACTION_REGISTRY["setup_operator_flow"].title,
            "parameters": {
                **target,
                "prediction_mode": str(slots.get("prediction_mode") or ui_context.get("selected_prediction_mode") or "local").strip().lower() or "local",
                "force_prediction_refresh": bool(slots.get("force_prediction_refresh")),
                "include_risks": list(slots.get("include_risks") or ["high"]),
                "campaign_name": str(slots.get("campaign_name") or ""),
                "workflow_name": str(slots.get("workflow_name") or ""),
                "cohort_name": str(slots.get("cohort_name") or slots.get("name") or ""),
                "saved_query_name": str(slots.get("saved_query_name") or ""),
                "provider_connection_id": str((provider_connection or {}).get("provider_connection_id") or slots.get("provider_connection_id") or ""),
                "template_id": str(slots.get("template_id") or ""),
                "source_message": str(slots.get("source_message") or ""),
                "wants_prediction": True,
                "wants_cohort": True,
                "wants_email_campaign": bool(slots.get("wants_email_campaign")),
                "wants_workflow": bool(slots.get("wants_workflow")),
            },
        }

    def _prediction_target_from_slots(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any]:
        prediction_job_id = str(slots.get("prediction_job_id") or "").strip()
        if prediction_job_id:
            return {"prediction_job_id": prediction_job_id}
        audience_scope = str(
            slots.get("audience_scope")
            or ui_context.get("selected_prediction_audience_scope")
            or ("source" if ui_context.get("selected_prediction_audience_key") else "")
            or ("source" if slots.get("source_name") else "import")
        ).strip().lower()
        import_job_id = str(slots.get("import_job_id") or ui_context.get("selected_import_job_id") or "").strip()
        source_name = str(slots.get("source_name") or "").strip()
        selected_prediction_key = str(ui_context.get("selected_prediction_audience_key") or "").strip()
        if not source_name and audience_scope == "source":
            source_name = selected_prediction_key
        if audience_scope == "source":
            return {"audience_scope": "source", "source_name": source_name}
        if import_job_id:
            return {"audience_scope": "import", "import_job_id": import_job_id}
        if source_name:
            return {"audience_scope": "source", "source_name": source_name}
        return {}

    def _prediction_source_options(self) -> List[str]:
        options = sorted(
            {
                str(((job.get("spec") or {}).get("source_name") or "")).strip()
                for job in self.repository.list_import_jobs()
                if str(job.get("status") or "").lower() == "completed"
                and str(((job.get("spec") or {}).get("source_name") or "")).strip()
            }
        )
        return options

    def _resolve_provider_connection_candidate(self, slots: Dict[str, Any], *, ui_context: Dict[str, Any]) -> Dict[str, Any] | None:
        requested_id = str(slots.get("provider_connection_id") or ui_context.get("selected_email_provider_connection_id") or "").strip()
        if requested_id:
            return self.provider_connections.get_connection(requested_id)
        requested_provider = str(
            slots.get("messaging_provider")
            or ui_context.get("selected_email_provider_type")
            or ""
        ).strip().lower()
        connections = [
            item
            for item in self.provider_connections.list_connections()
            if str(item.get("status") or "").lower() == "active"
            and (not requested_provider or str(item.get("provider") or "").lower() == requested_provider)
        ]
        if len(connections) == 1:
            return connections[0]
        return None

    def _list_provider_assets(self, provider_connection_id: str) -> List[Dict[str, Any]]:
        connection = self.provider_connections.get_connection(provider_connection_id)
        if connection is None:
            return []
        provider = str(connection.get("provider") or "").strip().lower()
        if provider == "sendgrid":
            items = self.sendgrid_provider.list_dynamic_templates(provider_connection_id)
        elif provider == "braze":
            items = self.braze_provider.list_api_campaigns(provider_connection_id)
        else:
            return []
        normalized: List[Dict[str, Any]] = []
        for item in items:
            normalized.append(
                {
                    "asset_id": str(item.get("id") or item.get("template_id") or item.get("campaign_id") or ""),
                    "label": str(item.get("name") or item.get("title") or item.get("subject") or item.get("id") or ""),
                    "provider": provider,
                    "raw": item,
                }
            )
        return normalized

    def _build_execution_preview(
        self,
        intent: str,
        actions: List[Dict[str, Any]],
        clarifications: List[Dict[str, Any]],
        notes: List[str],
    ) -> Dict[str, Any]:
        steps = []
        for item in actions:
            spec = ACTION_REGISTRY[item["action_type"]]
            steps.append(
                {
                    "action_id": "",
                    "action_type": spec.action_type,
                    "title": item.get("title") or spec.title,
                    "summary": preview_step_summary(spec.action_type, item.get("parameters") or {}),
                    "status": "awaiting_confirmation" if spec.requires_confirmation else ("pending" if not clarifications else "blocked"),
                    "requires_confirmation": spec.requires_confirmation,
                    "risk_level": spec.risk_level,
                }
            )
        if not steps and clarifications:
            steps.append(
                {
                    "action_id": "",
                    "action_type": "collect_input",
                    "title": "Collect missing input",
                    "summary": "Wait for the missing fields before executing any control-plane changes.",
                    "status": "blocked",
                    "requires_confirmation": False,
                    "risk_level": "low",
                }
            )
        return {
            "intent": intent,
            "title": humanize_intent(intent),
            "summary": preview_summary(intent, clarifications, notes),
            "risk_level": highest_risk_level(ACTION_REGISTRY[item["action_type"]].risk_level for item in actions),
            "ready": not clarifications,
            "missing_fields": [str(item.get("key") or "") for item in clarifications],
            "blockers": notes,
            "steps": steps,
        }

    def _execute_plan(
        self,
        *,
        session_id: str,
        plan: Dict[str, Any],
        context: GovernanceContext,
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        model_adapter: CopilotAgentModelAdapter,
    ) -> Dict[str, Any]:
        completed_actions: List[Dict[str, Any]] = []
        pending_confirmations: List[Dict[str, Any]] = []
        artifacts: List[Dict[str, Any]] = []
        session_status = "active"
        async_status = ""
        waiting_for_action_type = None
        waiting_for_resource_id = None
        pending_flow = None
        for planned_action in plan["actions"]:
            action_type = str(planned_action["action_type"])
            parameters = dict(planned_action.get("parameters") or {})
            action_payload = self._create_action_run(session_id, action_type, planned_action.get("title") or ACTION_REGISTRY[action_type].title, parameters)
            spec = ACTION_REGISTRY[action_type]
            if spec.requires_confirmation:
                ensure_permissions_for_action(action_type, context)
                pending_confirmations.append(action_payload)
                session_status = "awaiting_confirmation"
                continue
            try:
                ensure_permissions_for_action(action_type, context)
                result = self._execute_action(
                    action_type,
                    parameters,
                    context=context,
                    session=session,
                    ui_context=ui_context,
                    model_adapter=model_adapter,
                )
                next_status = "completed"
                if bool(result.get("is_async")):
                    next_status = str(result.get("status") or "running")
                action_payload.update(
                    {
                        "status": next_status,
                        "result": result.get("result") or {},
                        "summary": result.get("summary") or deterministic_action_summary(action_type, result.get("result") or {}),
                        "artifacts": result.get("artifacts") or [],
                        "is_async": bool(result.get("is_async")),
                        "status_detail": str(result.get("status_detail") or ""),
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                )
                self.repository.upsert_resource(ACTION_RESOURCE_TYPE, action_payload["action_id"], status=next_status, name=action_payload["title"], payload=action_payload)
                self.repository.record_action(
                    "copilot_agent_action_completed",
                    ACTION_RESOURCE_TYPE,
                    action_payload["action_id"],
                    {
                        "action_type": action_type,
                        "parameters": parameters,
                        "result": action_payload["result"],
                        "artifacts": action_payload["artifacts"],
                    },
                )
                completed_actions.append(action_payload)
                artifacts.extend(action_payload["artifacts"])
                if bool(result.get("is_async")):
                    session_status = str(result.get("session_status") or "waiting_for_prediction")
                    async_status = str(result.get("async_status") or session_status)
                    waiting_for_action_type = action_type
                    waiting_for_resource_id = str(result.get("waiting_for_resource_id") or "")
                    pending_flow = result.get("pending_flow")
                    if isinstance(pending_flow, dict) and not str(pending_flow.get("action_id") or "").strip():
                        pending_flow["action_id"] = action_payload["action_id"]
                    break
            except HTTPException as exc:
                action_payload.update(
                    {
                        "status": "blocked",
                        "summary": str(exc.detail),
                        "result": {"error": str(exc.detail), "status_code": exc.status_code},
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                )
                self.repository.upsert_resource(ACTION_RESOURCE_TYPE, action_payload["action_id"], status="blocked", name=action_payload["title"], payload=action_payload)
                completed_actions.append(action_payload)
                session_status = "active"
                break
            except Exception as exc:
                action_payload.update(
                    {
                        "status": "failed",
                        "summary": str(exc),
                        "result": {"error": str(exc)},
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                )
                self.repository.upsert_resource(ACTION_RESOURCE_TYPE, action_payload["action_id"], status="failed", name=action_payload["title"], payload=action_payload)
                completed_actions.append(action_payload)
                session_status = "active"
                break
        return {
            "completed_actions": completed_actions,
            "pending_confirmations": pending_confirmations,
            "artifacts": dedupe_artifacts(artifacts),
            "session_status": session_status,
            "async_status": async_status,
            "waiting_for_action_type": waiting_for_action_type,
            "waiting_for_resource_id": waiting_for_resource_id,
            "pending_flow": pending_flow,
        }

    def _create_action_run(self, session_id: str, action_type: str, title: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        spec = ACTION_REGISTRY[action_type]
        action_id = f"cpaa_{uuid.uuid4().hex[:20]}"
        payload = {
            "action_id": action_id,
            "session_id": session_id,
            "action_type": action_type,
            "title": title,
            "status": "awaiting_confirmation" if spec.requires_confirmation else "running",
            "requires_confirmation": spec.requires_confirmation,
            "risk_level": spec.risk_level,
            "parameters": sanitize_action_parameters(parameters),
            "result": {},
            "summary": "",
            "artifacts": [],
            "confirmation_id": None,
            "is_async": False,
            "status_detail": "",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        if spec.requires_confirmation:
            confirmation_id = f"cpac_{uuid.uuid4().hex[:20]}"
            payload["confirmation_id"] = confirmation_id
            self.repository.upsert_resource(
                CONFIRMATION_RESOURCE_TYPE,
                confirmation_id,
                status="pending",
                name=title,
                payload={
                    "confirmation_id": confirmation_id,
                    "session_id": session_id,
                    "action_id": action_id,
                    "title": title,
                    "status": "pending",
                    "created_at": datetime.utcnow().isoformat(),
                },
            )
        self.repository.upsert_resource(ACTION_RESOURCE_TYPE, action_id, status=payload["status"], name=title, payload=payload)
        self.repository.record_action("copilot_agent_action_prepared", ACTION_RESOURCE_TYPE, action_id, {"action_type": action_type, "parameters": parameters, "requires_confirmation": spec.requires_confirmation})
        return payload

    def _execute_action(
        self,
        action_type: str,
        parameters: Dict[str, Any],
        *,
        context: GovernanceContext,
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        model_adapter: CopilotAgentModelAdapter,
    ) -> Dict[str, Any]:
        if action_type == "summarize_dashboard":
            return self._execute_dashboard_summary(parameters)
        if action_type == "run_prediction":
            return self._execute_prediction_action(parameters)
        if action_type == "draft_sql_from_prompt":
            return self._execute_sql_draft_action(parameters, session=session, ui_context=ui_context, model_adapter=model_adapter)
        if action_type == "list_provider_messaging_assets":
            return self._execute_list_provider_assets_action(parameters)
        if action_type == "setup_email_campaign":
            return self._execute_email_campaign_action(parameters)
        if action_type == "setup_workflow":
            return self._execute_workflow_action(parameters)
        if action_type == "setup_operator_flow":
            return self._execute_operator_flow(
                parameters,
                session=session,
                ui_context=ui_context,
                context=context,
                model_adapter=model_adapter,
            )
        if action_type == "upsert_connector":
            connector = self.connectors.create_connector(parameters["name"], parameters["connector_type"], parameters["config"])
            return {
                "summary": f"Created connector `{connector['name']}` for `{connector['type']}`.",
                "result": {"connector": connector},
                "artifacts": [artifact_for_connector(connector)],
            }
        if action_type == "check_connector_health":
            try:
                health = self.connectors.health_check(parameters["name"])
                return {
                    "summary": f"Ran health check for `{parameters['name']}` and the connector reported `{health.get('message') or ('ok' if health.get('ok') else 'issue')}`.",
                    "result": {"health": health},
                    "artifacts": [],
                }
            except Exception as exc:
                return {
                    "summary": f"Created the connector, but the optional health check could not complete: {exc}",
                    "result": {"health_error": str(exc)},
                    "artifacts": [],
                }
        if action_type == "upsert_provider_connection":
            existing = None
            if parameters.get("update_existing"):
                existing = next(
                    (
                        item
                        for item in self.provider_connections.list_connections()
                        if str(item.get("name") or "") == str(parameters["name"])
                        and str(item.get("provider") or "") == str(parameters["provider"])
                    ),
                    None,
                )
            if existing is not None:
                connection = self.provider_connections.update_connection(existing["provider_connection_id"], {"name": parameters["name"], "config": parameters["config"]})
                summary = f"Updated provider connection `{connection['name']}` for `{connection['provider']}`."
            else:
                connection = self.provider_connections.create_connection(parameters["name"], parameters["provider"], parameters["config"])
                summary = f"Created provider connection `{connection['name']}` for `{connection['provider']}`."
            return {
                "summary": summary,
                "result": {"provider_connection": connection},
                "artifacts": [artifact_for_provider_connection(connection)],
            }
        if action_type == "preview_sql":
            preview = self.sql_workspace.preview(
                parameters["sql"],
                limit=int(parameters.get("limit") or 20),
                timeout_seconds=int(parameters.get("timeout_seconds") or 30),
            )
            return {
                "summary": f"Previewed the SQL query and returned {int(preview.get('row_count') or 0)} row(s).",
                "result": {"preview": preview},
                "artifacts": [],
            }
        if action_type == "save_query":
            saved_query = self.sql_workspace.create_saved_query(parameters["name"], parameters["sql"], parameters.get("description") or "")
            return {
                "summary": f"Saved SQL query `{saved_query['name']}`.",
                "result": {"saved_query": saved_query},
                "artifacts": [artifact_for_saved_query(saved_query)],
            }
        if action_type == "create_cohort_sql":
            cohort = self.cohorts.create_cohort(
                name=parameters["name"],
                cohort_type="sql",
                definition=dict(parameters["definition"] or {}),
                refresh_mode=parameters.get("refresh_mode") or "manual",
                owner=parameters.get("owner") or context.actor_id,
                description=parameters.get("description") or "",
                tags=list(parameters.get("tags") or []),
                activate=False,
            )
            return {
                "summary": f"Created draft SQL cohort `{cohort['name']}` with {int(cohort.get('member_count') or 0)} member(s).",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "create_cohort_definition":
            cohort = self.cohorts.create_cohort(
                name=parameters["name"],
                cohort_type=parameters["cohort_type"],
                definition=dict(parameters["definition"] or {}),
                refresh_mode=parameters.get("refresh_mode") or "manual",
                owner=parameters.get("owner") or context.actor_id,
                description=parameters.get("description") or "",
                tags=list(parameters.get("tags") or []),
                activate=False,
            )
            return {
                "summary": f"Created draft {parameters['cohort_type']} cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "update_cohort_definition":
            cohort = self.cohorts.update_cohort(
                parameters["cohort_id"],
                {
                    "name": parameters["name"],
                    "type": parameters["cohort_type"],
                    "definition": dict(parameters["definition"] or {}),
                    "refresh_mode": parameters.get("refresh_mode") or "manual",
                    "owner": parameters.get("owner") or context.actor_id,
                    "description": parameters.get("description") or "",
                    "tags": list(parameters.get("tags") or []),
                },
            )
            return {
                "summary": f"Updated draft cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "save_experiment_config":
            experiment = self.experiments.save_config(parameters, experiment_id=parameters["experiment_id"])
            return {
                "summary": f"Saved experiment config `{experiment['experiment_id']}` in a non-running state.",
                "result": {"experiment": experiment},
                "artifacts": [artifact_for_experiment(experiment)],
            }
        if action_type == "activate_cohort":
            cohort = self.cohorts.activate_cohort(parameters["cohort_id"])
            return {
                "summary": f"Activated cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "pause_cohort":
            cohort = self.cohorts.pause_cohort(parameters["cohort_id"])
            return {
                "summary": f"Paused cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "archive_cohort":
            cohort = self.cohorts.archive_cohort(parameters["cohort_id"])
            return {
                "summary": f"Archived cohort `{cohort['name']}`.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "restore_cohort":
            cohort = self.cohorts.restore_cohort(parameters["cohort_id"])
            return {
                "summary": f"Restored cohort `{cohort['name']}` to draft status.",
                "result": {"cohort": cohort},
                "artifacts": [artifact_for_cohort(cohort)],
            }
        if action_type == "start_experiment":
            experiment = self.experiments.start(parameters["experiment_id"])
            return {
                "summary": f"Started experiment `{parameters['experiment_id']}`.",
                "result": {"experiment": experiment},
                "artifacts": [artifact_for_experiment(experiment)],
            }
        if action_type == "stop_experiment":
            experiment = self.experiments.stop(parameters["experiment_id"])
            return {
                "summary": f"Stopped experiment `{parameters['experiment_id']}`.",
                "result": {"experiment": experiment},
                "artifacts": [artifact_for_experiment(experiment)],
            }
        if action_type == "record_experiment_decision":
            decision = self.experiments.decide(parameters["experiment_id"], decided_by=parameters.get("decided_by") or context.actor_id)
            return {
                "summary": f"Recorded an experiment decision for `{parameters['experiment_id']}`.",
                "result": {"decision": decision},
                "artifacts": [artifact_for_experiment({"experiment_id": parameters["experiment_id"]})],
            }
        raise ValueError(f"Unsupported agent action '{action_type}'.")

    def _execute_prediction_action(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        prediction_job_id = str(parameters.get("prediction_job_id") or "").strip()
        if prediction_job_id:
            prediction_job = self.predictions.get_job(prediction_job_id)
            if prediction_job is None:
                raise HTTPException(status_code=404, detail=f"Prediction job '{prediction_job_id}' not found.")
            return {
                "summary": f"Using prediction job `{prediction_job_id}`.",
                "result": {"prediction_job": prediction_job, "reused": True},
                "artifacts": [artifact_for_prediction_job(prediction_job)],
            }

        reusable_job = None
        if not bool(parameters.get("force_prediction_refresh")):
            reusable_job = self._find_reusable_prediction_job(parameters)
        if reusable_job is not None:
            return {
                "summary": f"Reused completed prediction job `{reusable_job['id']}`.",
                "result": {"prediction_job": reusable_job, "reused": True},
                "artifacts": [artifact_for_prediction_job(reusable_job)],
            }

        prediction_job = self.predictions.create_job(
            import_job_id=parameters.get("import_job_id"),
            source_name=parameters.get("source_name"),
            audience_scope=parameters.get("audience_scope"),
            prediction_mode=str(parameters.get("prediction_mode") or "local"),
        )
        self.predictions.start_job_async(prediction_job["id"])
        running_job = self.predictions.get_job(prediction_job["id"]) or prediction_job
        return {
            "summary": f"Started prediction job `{running_job['id']}` in the background.",
            "result": {"prediction_job": running_job, "reused": False},
            "artifacts": [
                artifact_for_prediction_job(
                    running_job,
                    resume_message="Continue with the prediction results.",
                )
            ],
            "is_async": True,
            "status": "running",
            "status_detail": "Prediction is running in the background.",
            "session_status": "waiting_for_prediction",
            "async_status": "waiting_for_prediction",
            "waiting_for_resource_id": running_job["id"],
        }

    def _execute_sql_draft_action(
        self,
        parameters: Dict[str, Any],
        *,
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        model_adapter: CopilotAgentModelAdapter,
    ) -> Dict[str, Any]:
        prediction_job = self._resolve_prediction_job_for_parameters(parameters)
        if prediction_job is None:
            raise HTTPException(status_code=409, detail="A completed prediction job is required before drafting SQL from prompt.")
        drafted = self._draft_prediction_sql(
            source_message=str(parameters.get("source_message") or ""),
            prediction_job=prediction_job,
            include_risks=list(parameters.get("include_risks") or ["high"]),
            session=session,
            ui_context=ui_context,
            model_adapter=model_adapter,
            query_name=str(parameters.get("query_name") or ""),
            cohort_name=str(parameters.get("cohort_name") or ""),
        )
        preview = self.sql_workspace.preview(drafted["sql"], limit=20, timeout_seconds=30)
        validate_prediction_preview(preview)
        return {
            "summary": f"Drafted and previewed SQL for prediction job `{prediction_job['id']}` with {int(preview.get('row_count') or 0)} matching row(s).",
            "result": {"draft": drafted, "preview": preview, "prediction_job": prediction_job},
            "artifacts": [artifact_for_prediction_job(prediction_job)],
        }

    def _execute_list_provider_assets_action(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        provider_connection_id = str(parameters.get("provider_connection_id") or "").strip()
        assets = self._list_provider_assets(provider_connection_id)
        return {
            "summary": f"Found {len(assets)} messaging asset(s) on provider connection `{provider_connection_id}`.",
            "result": {"provider_connection_id": provider_connection_id, "items": assets},
            "artifacts": [],
        }

    def _execute_email_campaign_action(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        provider_connection_id = str(parameters.get("provider_connection_id") or "").strip()
        resolved_asset = self._resolve_provider_asset(provider_connection_id, str(parameters.get("template_id") or "").strip())
        audience: Dict[str, Any]
        cohort_id = str(parameters.get("cohort_id") or "").strip()
        if cohort_id:
            audience = {"cohort_id": cohort_id}
        else:
            prediction_job_id = str(parameters.get("prediction_job_id") or "").strip()
            if not prediction_job_id:
                raise HTTPException(status_code=409, detail="Draft email campaigns require cohort_id or prediction_job_id.")
            audience = {
                "prediction_job_id": prediction_job_id,
                "include_risks": list(parameters.get("include_risks") or ["high"]),
                "include_churned": False,
            }
        campaign = self.email_campaigns.create_campaign(
            {
                "name": str(parameters.get("campaign_name") or default_named_resource(prefix="agent", suffix="email_campaign")),
                "provider_connection_id": provider_connection_id,
                "template_id": str(resolved_asset.get("asset_id") or ""),
                "audience": audience,
                "schedule_at": parameters.get("schedule_at"),
            }
        )
        return {
            "summary": f"Created draft email campaign `{campaign['name']}`.",
            "result": {"email_campaign": campaign},
            "artifacts": [artifact_for_email_campaign(campaign)],
        }

    def _execute_workflow_action(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        cohort_id = str(parameters.get("cohort_id") or "").strip()
        if not cohort_id:
            raise HTTPException(status_code=409, detail="Workflow drafts require cohort_id.")
        email_campaign_id = str(parameters.get("email_campaign_id") or "").strip()
        action_payload = {
            "type": "email_campaign" if email_campaign_id else "draft_follow_up",
            "email_campaign_id": email_campaign_id or None,
        }
        workflow = self.workflows.create_workflow(
            name=str(parameters.get("workflow_name") or default_named_resource(prefix="agent", suffix="workflow")),
            cohort_id=cohort_id,
            schedule={"type": "manual_test"},
            action=action_payload,
            policy={"goal": "churn_rescue", "mode": "draft"},
            experiment_id=str(parameters.get("experiment_id") or "").strip() or None,
            channel_config=action_payload,
            steps=[{"type": "action", "action": action_payload}],
            requires_confirmation=False,
        )
        return {
            "summary": f"Created draft workflow `{workflow['name']}`.",
            "result": {"workflow": workflow},
            "artifacts": [artifact_for_workflow(workflow)],
        }

    def _execute_operator_flow(
        self,
        parameters: Dict[str, Any],
        *,
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        context: GovernanceContext,
        model_adapter: CopilotAgentModelAdapter,
    ) -> Dict[str, Any]:
        reusable_prediction = None
        if str(parameters.get("prediction_job_id") or "").strip():
            reusable_prediction = self.predictions.get_job(str(parameters["prediction_job_id"]))
        elif not bool(parameters.get("force_prediction_refresh")):
            reusable_prediction = self._find_reusable_prediction_job(parameters)
        if reusable_prediction is None:
            prediction_result = self._execute_prediction_action(parameters)
            prediction_job = (prediction_result.get("result") or {}).get("prediction_job") or {}
            pending_flow = {
                "type": "prediction_to_campaign",
                "parameters": parameters,
                "prediction_job_id": str(prediction_job.get("id") or ""),
                "source_message": str(parameters.get("source_message") or ""),
            }
            return {
                **prediction_result,
                "summary": "Started the prediction job. Continue when the prediction completes to build the saved query, cohort, campaign, and workflow drafts.",
                "pending_flow": pending_flow,
            }
        return self._complete_operator_flow_from_prediction(
            prediction_job=reusable_prediction,
            parameters=parameters,
            session=session,
            ui_context=ui_context,
            context=context,
            model_adapter=model_adapter,
        )

    def _complete_operator_flow_from_prediction(
        self,
        *,
        prediction_job: Dict[str, Any],
        parameters: Dict[str, Any],
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        context: GovernanceContext,
        model_adapter: CopilotAgentModelAdapter,
    ) -> Dict[str, Any]:
        drafted = self._draft_prediction_sql(
            source_message=str(parameters.get("source_message") or ""),
            prediction_job=prediction_job,
            include_risks=list(parameters.get("include_risks") or ["high"]),
            session=session,
            ui_context=ui_context,
            model_adapter=model_adapter,
            query_name=str(parameters.get("saved_query_name") or ""),
            cohort_name=str(parameters.get("cohort_name") or ""),
        )
        preview = self.sql_workspace.preview(drafted["sql"], limit=20, timeout_seconds=30)
        validate_prediction_preview(preview)
        saved_query = self.sql_workspace.create_saved_query(
            drafted["query_name"],
            drafted["sql"],
            f"Saved by the operator agent from prediction job {prediction_job['id']}.",
        )
        cohort = self.cohorts.create_cohort(
            name=drafted["cohort_name"],
            cohort_type="sql",
            definition={"sql": drafted["sql"]},
            refresh_mode="manual",
            owner=context.actor_id,
            description=f"Created by the operator agent from prediction job {prediction_job['id']}.",
            tags=["agent", "prediction"],
            activate=False,
        )
        artifacts = [
            artifact_for_prediction_job(prediction_job),
            artifact_for_saved_query(saved_query),
            artifact_for_cohort(cohort),
        ]
        result_payload: Dict[str, Any] = {
            "prediction_job": prediction_job,
            "draft": drafted,
            "preview": preview,
            "saved_query": saved_query,
            "cohort": cohort,
        }
        summary_parts = [f"Saved query `{saved_query['name']}` and draft cohort `{cohort['name']}` from prediction job `{prediction_job['id']}`."]

        email_campaign = None
        if bool(parameters.get("wants_email_campaign")):
            email_campaign_result = self._execute_email_campaign_action(
                {
                    **parameters,
                    "cohort_id": cohort["cohort_id"],
                    "prediction_job_id": prediction_job["id"],
                }
            )
            email_campaign = (email_campaign_result.get("result") or {}).get("email_campaign")
            if email_campaign is not None:
                result_payload["email_campaign"] = email_campaign
                artifacts.extend(email_campaign_result.get("artifacts") or [])
                summary_parts.append(f"Created draft email campaign `{email_campaign['name']}`.")

        if bool(parameters.get("wants_workflow")):
            workflow_result = self._execute_workflow_action(
                {
                    **parameters,
                    "cohort_id": cohort["cohort_id"],
                    "email_campaign_id": str((email_campaign or {}).get("email_campaign_id") or ""),
                }
            )
            workflow = (workflow_result.get("result") or {}).get("workflow")
            if workflow is not None:
                result_payload["workflow"] = workflow
                artifacts.extend(workflow_result.get("artifacts") or [])
                summary_parts.append(f"Created draft workflow `{workflow['name']}`.")

        return {
            "summary": " ".join(summary_parts),
            "result": result_payload,
            "artifacts": dedupe_artifacts(artifacts),
        }

    def _draft_prediction_sql(
        self,
        *,
        source_message: str,
        prediction_job: Dict[str, Any],
        include_risks: List[str],
        session: Dict[str, Any],
        ui_context: Dict[str, Any],
        model_adapter: CopilotAgentModelAdapter,
        query_name: str,
        cohort_name: str,
    ) -> Dict[str, Any]:
        draft = model_adapter.draft_sql(
            source_message,
            session_state=session,
            ui_context=ui_context,
            hint={
                "prediction_job_id": str(prediction_job.get("id") or ""),
                "include_risks": include_risks,
                "query_name": query_name,
                "cohort_name": cohort_name,
                "override_table": "prediction_results",
            },
        )
        sql = str(draft.get("sql") or "").strip()
        if not sql:
            raise HTTPException(status_code=409, detail="The agent could not draft a valid SQL query from the request.")
        return {
            "sql": sql,
            "query_name": str(draft.get("query_name") or query_name or default_named_resource(prefix="agent", suffix="high_risk_query")),
            "cohort_name": str(draft.get("cohort_name") or cohort_name or default_named_resource(prefix="agent", suffix="high_risk_cohort")),
        }

    def _resolve_prediction_job_for_parameters(self, parameters: Dict[str, Any]) -> Dict[str, Any] | None:
        prediction_job_id = str(parameters.get("prediction_job_id") or "").strip()
        if prediction_job_id:
            job = self.predictions.get_job(prediction_job_id)
            if job is None:
                raise HTTPException(status_code=404, detail=f"Prediction job '{prediction_job_id}' not found.")
            return job if str(job.get("status") or "").lower() == "completed" else None
        return self._find_reusable_prediction_job(parameters)

    def _find_reusable_prediction_job(self, parameters: Dict[str, Any]) -> Dict[str, Any] | None:
        audience_scope = str(parameters.get("audience_scope") or "").strip().lower()
        prediction_mode = str(parameters.get("prediction_mode") or "local").strip().lower() or "local"
        import_job_id = str(parameters.get("import_job_id") or "").strip()
        source_name = str(parameters.get("source_name") or "").strip()
        jobs = self.predictions.list_jobs()
        for job in jobs:
            if str(job.get("status") or "").lower() != "completed":
                continue
            spec = job.get("spec") or {}
            details = ((job.get("progress") or {}).get("details") or {})
            if bool(details.get("stale")):
                continue
            if str(spec.get("prediction_mode") or "").strip().lower() != prediction_mode:
                continue
            if audience_scope == "source" and str(spec.get("source_name") or "").strip() == source_name:
                return job
            if audience_scope == "import" and str(spec.get("import_job_id") or "").strip() == import_job_id:
                return job
        return None

    def _resolve_provider_asset(self, provider_connection_id: str, template_hint: str) -> Dict[str, Any]:
        normalized_hint = str(template_hint or "").strip()
        assets = self._list_provider_assets(provider_connection_id)
        if not assets:
            raise HTTPException(status_code=409, detail=f"No provider messaging assets are available on provider connection '{provider_connection_id}'.")
        if not normalized_hint:
            raise HTTPException(status_code=409, detail="template_id is required.")
        exact = next(
            (
                item for item in assets
                if str(item.get("asset_id") or "").strip() == normalized_hint
                or str(item.get("label") or "").strip().lower() == normalized_hint.lower()
            ),
            None,
        )
        if exact is not None:
            return exact
        partial = [
            item for item in assets
            if normalized_hint.lower() in str(item.get("label") or "").lower()
            or normalized_hint.lower() in str(item.get("asset_id") or "").lower()
        ]
        if len(partial) == 1:
            return partial[0]
        raise HTTPException(status_code=409, detail=f"Could not uniquely resolve template '{normalized_hint}' on provider connection '{provider_connection_id}'.")

    def _resolve_session_model(self, requested_model_profile_id: str | None) -> Dict[str, Any]:
        profile = self.model_profiles.resolve_profile(requested_model_profile_id)
        if profile is None:
            return {
                "model_profile_id": None,
                "effective_provider": "deterministic",
                "effective_model_name": "",
                "model_selection_source": "deterministic_fallback",
            }
        return {
            "model_profile_id": str(profile.get("model_profile_id") or ""),
            "effective_provider": str(profile.get("provider") or "deterministic"),
            "effective_model_name": str(profile.get("model_name") or ""),
            "model_selection_source": str(profile.get("model_selection_source") or "profile"),
        }

    def _model_adapter_for_session(self, session: Dict[str, Any]) -> CopilotAgentModelAdapter:
        profile_id = str(session.get("model_profile_id") or "").strip() or None
        try:
            profile = self.model_profiles.resolve_profile(profile_id)
        except KeyError:
            profile = None
        return ConfiguredCopilotAgentModel(profile)

    def _decorate_session_async_state(self, session: Dict[str, Any]) -> Dict[str, Any]:
        decorated = dict(session or {})
        pending_flow = dict(decorated.get("pending_flow") or {})
        prediction_job_id = str(
            pending_flow.get("prediction_job_id")
            or decorated.get("waiting_for_resource_id")
            or ""
        ).strip()
        if not prediction_job_id:
            return decorated
        prediction_job = self.predictions.get_job(prediction_job_id)
        if prediction_job is None:
            decorated["async_status"] = "missing_prediction"
            decorated["status"] = "active"
            return decorated
        status = str(prediction_job.get("status") or "").lower()
        latest_artifacts = [item for item in list(decorated.get("latest_artifacts") or []) if str(item.get("resource_type") or "") != "prediction_job"]
        latest_artifacts.insert(
            0,
            artifact_for_prediction_job(
                prediction_job,
                resume_ready=status == "completed",
                resume_message="Continue with the prediction results." if status == "completed" else "Continue after the prediction completes.",
            ),
        )
        decorated["latest_artifacts"] = dedupe_artifacts(latest_artifacts)
        if status == "completed":
            decorated["async_status"] = "ready_to_resume"
            decorated["status"] = "ready_to_resume"
        elif status in {"failed", "stopped"}:
            decorated["async_status"] = f"prediction_{status}"
            decorated["status"] = "active"
        else:
            decorated["async_status"] = "waiting_for_prediction"
            decorated["status"] = "waiting_for_prediction"
        decorated["waiting_for_action_type"] = "run_prediction"
        decorated["waiting_for_resource_id"] = prediction_job_id
        return decorated

    def _maybe_resume_pending_flow(
        self,
        session: Dict[str, Any],
        *,
        message: str,
        ui_context: Dict[str, Any],
        context: GovernanceContext,
        model_adapter: CopilotAgentModelAdapter,
    ) -> Dict[str, Any] | None:
        pending_flow = dict(session.get("pending_flow") or {})
        if not pending_flow:
            return None
        normalized_message = str(message or "").strip().lower()
        if normalized_message not in {"continue", "resume", "continue with the prediction results.", "continue with prediction results"}:
            return None
        prediction_job_id = str(pending_flow.get("prediction_job_id") or "").strip()
        prediction_job = self.predictions.get_job(prediction_job_id)
        if prediction_job is None:
            assistant_message = f"Prediction job `{prediction_job_id}` is no longer available, so I cannot continue the draft flow."
            return self._persist_async_follow_up_turn(
                session=session,
                user_message=message,
                assistant_message=assistant_message,
                status="active",
                completed_actions=[],
                artifacts=session.get("latest_artifacts") or [],
            )
        prediction_status = str(prediction_job.get("status") or "").lower()
        if prediction_status != "completed":
            assistant_message = (
                f"Prediction job `{prediction_job_id}` is still `{prediction_status}`. "
                "Continue again after it completes."
            )
            return self._persist_async_follow_up_turn(
                session=session,
                user_message=message,
                assistant_message=assistant_message,
                status="waiting_for_prediction",
                completed_actions=[],
                artifacts=[artifact_for_prediction_job(prediction_job, resume_message="Continue with the prediction results.")],
            )
        parameters = dict(pending_flow.get("parameters") or {})
        result = self._complete_operator_flow_from_prediction(
            prediction_job=prediction_job,
            parameters=parameters,
            session=session,
            ui_context=ui_context,
            context=context,
            model_adapter=model_adapter,
        )
        action_payload = {
            "action_id": str(pending_flow.get("action_id") or f"cpaa_{uuid.uuid4().hex[:20]}"),
            "session_id": str(session.get("session_id") or ""),
            "action_type": "setup_operator_flow",
            "title": ACTION_REGISTRY["setup_operator_flow"].title,
            "status": "completed",
            "requires_confirmation": False,
            "risk_level": "low",
            "parameters": sanitize_action_parameters(parameters),
            "result": result.get("result") or {},
            "summary": result.get("summary") or "",
            "artifacts": result.get("artifacts") or [],
            "confirmation_id": None,
            "confirmation_note": "",
            "is_async": False,
            "status_detail": "",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(
            ACTION_RESOURCE_TYPE,
            action_payload["action_id"],
            status="completed",
            name=action_payload["title"],
            payload=action_payload,
        )
        assistant_message = model_adapter.compose_message(
            {
                "completed_actions": [action_payload],
                "pending_confirmations": [],
                "clarifications": [],
                "execution_preview": self._preview_from_actions([action_payload]),
            }
        )
        response = self._persist_async_follow_up_turn(
            session=session,
            user_message=message,
            assistant_message=assistant_message,
            status="active",
            completed_actions=[action_payload],
            artifacts=action_payload["artifacts"],
        )
        return response

    def _persist_async_follow_up_turn(
        self,
        *,
        session: Dict[str, Any],
        user_message: str,
        assistant_message: str,
        status: str,
        completed_actions: List[Dict[str, Any]],
        artifacts: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        session_id = str(session.get("session_id") or "")
        execution_preview = self._preview_from_actions(completed_actions) if completed_actions else (session.get("latest_execution_preview") or None)
        turn_payload = {
            "turn_id": f"cpat_{uuid.uuid4().hex[:20]}",
            "session_id": session_id,
            "user_message": str(user_message or "").strip(),
            "assistant_message": assistant_message,
            "intent": "setup_operator_flow",
            "status": status,
            "clarifications": [],
            "execution_preview": execution_preview,
            "completed_actions": completed_actions,
            "pending_confirmations": [],
            "artifacts": artifacts,
            "ui_context": dict(session.get("ui_context") or {}),
            "created_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(TURN_RESOURCE_TYPE, turn_payload["turn_id"], status=status, name=turn_payload["intent"], payload=turn_payload)
        session.update(
            {
                "status": status,
                "current_intent": "setup_operator_flow",
                "last_user_message": turn_payload["user_message"],
                "latest_execution_preview": execution_preview,
                "latest_artifacts": artifacts,
                "latest_clarifications": [],
                "draft_slots": {},
                "pending_confirmation_count": 0,
                "async_status": "" if status == "active" else "waiting_for_prediction",
                "waiting_for_action_type": None if status == "active" else "run_prediction",
                "waiting_for_resource_id": None if status == "active" else session.get("waiting_for_resource_id"),
                "pending_flow": None if status == "active" else session.get("pending_flow"),
                "updated_at": datetime.utcnow().isoformat(),
            }
        )
        self.repository.upsert_resource(SESSION_RESOURCE_TYPE, session_id, status=status, name=session.get("title"), payload=session)
        return {
            "assistant_message": assistant_message,
            "session_state": self._session_state(session),
            "clarifications": [],
            "execution_preview": execution_preview,
            "completed_actions": completed_actions,
            "pending_confirmations": [],
            "artifacts": artifacts,
        }

    def _execute_dashboard_summary(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        overview = self.copilot.get_overview()
        health = self.health_monitor.snapshot(persist=True)
        cohorts = self.cohorts.list_cohorts()
        workflows = self.repository.list_resources("workflow")
        experiments = [item.get("payload") or {} for item in self.repository.list_resources("experiment")]
        imports = self.repository.list_import_jobs()
        active_cohorts = [item for item in cohorts if str(item.get("status") or "") == "active"]
        open_alerts = [item for item in health.get("alerts") or [] if str(item.get("status") or "open") == "open"]
        active_experiments = [item for item in experiments if str(item.get("status") or "") == "active"]
        published_workflows = [item for item in workflows if str((item.get("payload") or {}).get("status") or item.get("status") or "") == "published"]
        blocked_imports = [item for item in imports if str(item.get("status") or "") in {"awaiting_mapping", "failed"}]
        top_risks = [
            str(item.get("message") or "")
            for item in open_alerts[:3]
            if str(item.get("message") or "").strip()
        ]
        suggested_next_steps = build_summary_next_steps(open_alerts, blocked_imports, overview)
        headline = (
            f"{len(open_alerts)} open alert(s), {len(active_cohorts)} active cohort(s), "
            f"{len(published_workflows)} published workflow(s), and {len(active_experiments)} active experiment(s)."
        )
        summary = {
            "headline": headline,
            "counts": {
                "open_alerts": len(open_alerts),
                "active_cohorts": len(active_cohorts),
                "published_workflows": len(published_workflows),
                "active_experiments": len(active_experiments),
                "blocked_imports": len(blocked_imports),
                "recent_reports": int((overview.get("report_counts") or {}).get("total") or 0),
            },
            "top_risks": top_risks,
            "suggested_next_steps": suggested_next_steps,
            "recent_reports": overview.get("recent_reports") or [],
            "recent_anomalies": overview.get("recent_anomalies") or [],
            "module_statuses": health.get("modules") or {},
            "blocked_imports": blocked_imports[:5],
            "active_cohorts": active_cohorts[:5],
        }
        return {
            "summary": headline if not top_risks else f"{headline} Top risk: {top_risks[0]}",
            "result": {"dashboard_summary": summary},
            "artifacts": [],
        }

    def _preview_from_actions(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "intent": actions[0]["action_type"] if actions else "summary",
            "title": "Confirmed Action",
            "summary": "Executed a previously prepared action.",
            "risk_level": highest_risk_level(str(item.get("risk_level") or "low") for item in actions),
            "ready": True,
            "missing_fields": [],
            "blockers": [],
            "steps": [
                {
                    "action_id": item.get("action_id"),
                    "action_type": item.get("action_type"),
                    "title": item.get("title"),
                    "summary": item.get("summary"),
                    "status": item.get("status"),
                    "requires_confirmation": bool(item.get("requires_confirmation")),
                    "risk_level": item.get("risk_level") or "low",
                }
                for item in actions
            ],
        }

    def _session_state(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "session_id": payload.get("session_id"),
            "title": payload.get("title") or "Operator Agent Session",
            "status": payload.get("status") or "active",
            "current_intent": payload.get("current_intent"),
            "last_user_message": payload.get("last_user_message") or "",
            "ui_context": dict(payload.get("ui_context") or {}),
            "latest_execution_preview": payload.get("latest_execution_preview"),
            "latest_artifacts": payload.get("latest_artifacts") or [],
            "latest_clarifications": payload.get("latest_clarifications") or [],
            "pending_confirmation_count": int(payload.get("pending_confirmation_count") or 0),
            "model_profile_id": payload.get("model_profile_id"),
            "effective_provider": payload.get("effective_provider") or "deterministic",
            "effective_model_name": payload.get("effective_model_name") or "",
            "model_selection_source": payload.get("model_selection_source") or "deterministic_fallback",
            "async_status": payload.get("async_status") or "",
            "waiting_for_action_type": payload.get("waiting_for_action_type"),
            "waiting_for_resource_id": payload.get("waiting_for_resource_id"),
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at"),
        }

    def _pending_confirmation_actions(self, session_id: str) -> List[Dict[str, Any]]:
        items = [
            self._action_from_record(record)
            for record in self.repository.list_resources(ACTION_RESOURCE_TYPE)
            if str((record.get("payload") or {}).get("session_id") or "") == session_id
            and str((record.get("payload") or {}).get("status") or "") == "awaiting_confirmation"
        ]
        items.sort(key=lambda item: str(item.get("created_at") or ""))
        return items

    def _get_session_payload(self, session_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource(SESSION_RESOURCE_TYPE, session_id)
        if record is None:
            raise KeyError(session_id)
        return dict(record.get("payload") or {})

    @staticmethod
    def _turn_from_record(record: Dict[str, Any]) -> Dict[str, Any]:
        return dict(record.get("payload") or {})

    @staticmethod
    def _action_from_record(record: Dict[str, Any]) -> Dict[str, Any]:
        return dict(record.get("payload") or {})


def ensure_permissions_for_action(action_type: str, context: GovernanceContext) -> None:
    spec = ACTION_REGISTRY[action_type]
    for permission in spec.permissions:
        ensure_permission(context, permission)


def preview_step_summary(action_type: str, parameters: Dict[str, Any]) -> str:
    if action_type == "summarize_dashboard":
        return "Aggregate copilot, cohort, workflow, experiment, import, and health state for the current workspace."
    if action_type in {"upsert_connector", "upsert_provider_connection"}:
        return f"Use `{parameters.get('name')}` with `{parameters.get('connector_type') or parameters.get('provider')}`."
    if action_type == "run_prediction":
        return "Reuse a recent completed prediction when possible, otherwise start a fresh background prediction job."
    if action_type == "draft_sql_from_prompt":
        return "Draft and preview a SQL audience query from the prompt using prediction results."
    if action_type == "list_provider_messaging_assets":
        return "List existing SendGrid templates or Braze API campaigns on the selected provider connection."
    if action_type == "setup_email_campaign":
        return "Create a draft provider-backed email campaign without sending it."
    if action_type == "setup_workflow":
        return "Create a draft workflow linked to the selected cohort or email campaign."
    if action_type == "setup_operator_flow":
        return "Set up the prediction, saved query, cohort, email campaign, and workflow draft chain."
    if action_type == "preview_sql":
        return "Run a read-only SQL preview before creating the cohort."
    if action_type == "save_query":
        return f"Persist the SQL as `{parameters.get('name')}` for reuse."
    if action_type in {"create_cohort_sql", "create_cohort_definition", "update_cohort_definition"}:
        return f"Create or update draft cohort `{parameters.get('name')}` without auto-activating it."
    if action_type == "save_experiment_config":
        return f"Save experiment `{parameters.get('experiment_id')}` in a non-running state."
    if action_type in {"activate_cohort", "pause_cohort", "archive_cohort", "restore_cohort"}:
        return f"Apply `{action_type}` to cohort `{parameters.get('cohort_id')}` after confirmation."
    if action_type in {"start_experiment", "stop_experiment", "record_experiment_decision"}:
        return f"Apply `{action_type}` to experiment `{parameters.get('experiment_id')}` after confirmation."
    return "Execute the requested control-plane action."


def preview_summary(intent: str, clarifications: List[Dict[str, Any]], notes: List[str]) -> str:
    if clarifications:
        return "Collect the missing details before executing any changes."
    if intent == "help_support":
        return "Provide grounded product guidance, sample payloads, and troubleshooting without changing control-plane state."
    if intent == "summarize_dashboard":
        return "Read the current workspace state and return an operator summary with risks and next steps."
    if intent == "setup_connection":
        return "Create or update the connection and optionally verify connector health."
    if intent == "setup_cohort":
        return "Preview inputs, persist any supporting query, and create a draft cohort."
    if intent == "run_prediction":
        return "Prepare a prediction job, reusing a recent completed run when possible."
    if intent == "draft_sql_from_prompt":
        return "Draft and preview SQL from the prompt without executing any destructive changes."
    if intent == "setup_email_campaign":
        return "Create a provider-backed email campaign in draft only."
    if intent == "setup_workflow":
        return "Create a workflow in draft only and leave publish/run as separate actions."
    if intent == "setup_operator_flow":
        return "Build the prediction-to-campaign draft flow with prediction, SQL, cohort, email campaign, and optional workflow steps."
    if intent == "setup_experiment":
        return "Save the experiment config in a non-running state and leave start as a separate confirmed action."
    if notes:
        return notes[0]
    return "Prepare the requested control-plane change."


def humanize_intent(intent: str) -> str:
    return str(intent or "task").replace("_", " ").strip().title()


def humanize_field(field_name: str) -> str:
    return str(field_name or "").replace("_", " ").strip().title()


def parse_named_fields(message: str) -> Dict[str, Any]:
    slots: Dict[str, Any] = {"config": {}}
    name_patterns = [
        r"\b(?:named|called)\s+[\"']?([A-Za-z0-9_.\- ]{3,80})[\"']?(?=\s+(?:with|using|for|on|in|that)\b|$)",
        r"\bname\s*(?:is|=|:)\s*[\"']?([A-Za-z0-9_.\- ]{3,80})[\"']?",
    ]
    for pattern in name_patterns:
        match = re.search(pattern, message, flags=re.IGNORECASE)
        if match:
            slots["name"] = match.group(1).strip()
            break
    config_patterns = {
        "api_key": [r"\bapi[_ ]key(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "secret_key": [r"\bsecret[_ ]key(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "api_token": [r"\bapi[_ ]token(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "project_id": [r"\bproject[_ ]id(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "app_id": [r"\bapp[_ ]id(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "rest_endpoint": [r"\brest[_ ]endpoint(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "webhook_url": [r"\bwebhook[_ ]url(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "webhook_token": [r"\bwebhook[_ ]token(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
        "model_name": [r"\bmodel(?:[_ ]name)?(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?"],
    }
    for field_name, patterns in config_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, message, flags=re.IGNORECASE)
            if match:
                slots["config"][field_name] = match.group(1).strip()
                break
    experiment_match = re.search(r"\bexperiment(?: id)?\s*(?:is|=|:)?\s*([A-Za-z0-9_.\-]+)", message, flags=re.IGNORECASE)
    if experiment_match:
        slots["experiment_id"] = experiment_match.group(1).strip()
    for cohort_match in re.finditer(r"\b(cohort_[A-Za-z0-9]+)\b", message):
        candidate = cohort_match.group(1).strip()
        if candidate.lower() == "cohort_id":
            continue
        slots["cohort_id"] = candidate
        break
    prediction_job_match = re.search(r"\b(pred_[A-Za-z0-9]+)\b", message)
    if prediction_job_match:
        slots["prediction_job_id"] = prediction_job_match.group(1).strip()
    provider_connection_match = re.search(r"\b(pc_[A-Za-z0-9]+)\b", message)
    if provider_connection_match:
        slots["provider_connection_id"] = provider_connection_match.group(1).strip()
    import_job_match = re.search(r"\b(imp_[A-Za-z0-9]+)\b", message)
    if import_job_match:
        slots["import_job_id"] = import_job_match.group(1).strip()
    primary_metric_match = re.search(r"\bprimary metric(?: is|=|:)?\s*([A-Za-z0-9_.\-]+)", message, flags=re.IGNORECASE)
    if primary_metric_match:
        slots["primary_metric"] = primary_metric_match.group(1).strip()
    guardrail_match = re.search(r"\bguardrails?(?: are| is|=|:)?\s*([A-Za-z0-9_,.\- ]+)", message, flags=re.IGNORECASE)
    if guardrail_match:
        slots["guardrail_metrics"] = [item.strip() for item in guardrail_match.group(1).split(",") if item.strip()]
    sample_match = re.search(r"\b(?:sample size|min sample size)(?: is|=|:)?\s*(\d+)", message, flags=re.IGNORECASE)
    if sample_match:
        slots["min_sample_size"] = int(sample_match.group(1))
    runtime_match = re.search(r"\b(?:runtime|min runtime)(?: is|=|:)?\s*(\d+)\s*(?:h|hour|hours)?", message, flags=re.IGNORECASE)
    if runtime_match:
        slots["min_runtime_hours"] = int(runtime_match.group(1))
    holdout_match = re.search(r"\bholdout(?: pct| %| percentage)?(?: is|=|:)?\s*(\d+(?:\.\d+)?)%?", message, flags=re.IGNORECASE)
    if holdout_match:
        slots["holdout_pct"] = normalize_percent_value(holdout_match.group(1))
    b_variant_match = re.search(r"\b(?:b variant|variant b)(?: pct| %| percentage)?(?: is|=|:)?\s*(\d+(?:\.\d+)?)%?", message, flags=re.IGNORECASE)
    if b_variant_match:
        slots["b_variant_pct"] = normalize_percent_value(b_variant_match.group(1))
    source_name_match = re.search(r"\bsource(?:[_ ]name)?(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?", message, flags=re.IGNORECASE)
    if source_name_match:
        slots["source_name"] = source_name_match.group(1).strip()
    template_match = re.search(r"\btemplate(?: id)?(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?", message, flags=re.IGNORECASE)
    if template_match:
        template_value = template_match.group(1).strip()
        slots["template_id"] = template_value
        slots.setdefault("template_hint", template_value)
    campaign_match = re.search(r"\bcampaign(?: id)?(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?", message, flags=re.IGNORECASE)
    if campaign_match and "template_id" not in slots:
        template_value = campaign_match.group(1).strip()
        slots["template_id"] = template_value
        slots.setdefault("template_hint", template_value)
    campaign_name_match = re.search(r"\bcampaign[_ ]name(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?", message, flags=re.IGNORECASE)
    if campaign_name_match:
        slots["campaign_name"] = campaign_name_match.group(1).strip()
    workflow_name_match = re.search(r"\bworkflow[_ ]name(?: is|=|:)?\s*[\"']?([^\"'\n,]+)[\"']?", message, flags=re.IGNORECASE)
    if workflow_name_match:
        slots["workflow_name"] = workflow_name_match.group(1).strip()
    if any(token in message.lower() for token in ("rerun", "re-run", "fresh prediction", "new prediction")):
        slots["force_prediction_refresh"] = True
    lowered = message.lower()
    if "high risk" in lowered:
        slots["include_risks"] = ["high"]
    elif "medium risk" in lowered:
        slots["include_risks"] = ["medium"]
    elif "low risk" in lowered:
        slots["include_risks"] = ["low"]
    if "local model" in lowered or re.search(r"\blocal\b", lowered):
        slots["prediction_mode"] = "local"
    elif "ai + cloud" in lowered or "parallel" in lowered:
        slots["prediction_mode"] = "parallel"
    elif "cloud" in lowered:
        slots["prediction_mode"] = "cloud"
    elif " ai " in f" {lowered} " or "gemini" in lowered or "chatgpt" in lowered or "opus" in lowered:
        slots["prediction_mode"] = "ai"
    if "sendgrid" in lowered:
        slots["messaging_provider"] = "sendgrid"
    elif "braze" in lowered:
        slots["messaging_provider"] = "braze"
    typed_fields = {
        "connection_scope": r"\bconnection[_ ]scope(?: is|=|:)?\s*([A-Za-z_ -]+)",
        "connection_type": r"\bconnection[_ ]type(?: is|=|:)?\s*([A-Za-z0-9_.\- ]+)",
        "cohort_type": r"\bcohort[_ ]type(?: is|=|:)?\s*([A-Za-z_ -]+)",
        "refresh_mode": r"\brefresh[_ ]mode(?: is|=|:)?\s*([A-Za-z_ -]+)",
    }
    for field_name, pattern in typed_fields.items():
        match = re.search(pattern, message, flags=re.IGNORECASE)
        if match:
            slots[field_name] = match.group(1).strip().lower().replace(" ", "_")
    return slots


def parse_json_blocks(message: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    code_block = re.search(r"```(?:json)?\s*(.*?)```", message, flags=re.DOTALL | re.IGNORECASE)
    if not code_block:
        return payload
    raw = code_block.group(1).strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return payload
    if isinstance(parsed, dict):
        payload["definition_json"] = parsed
        if isinstance(parsed.get("members"), list):
            payload["members"] = parsed["members"]
    elif isinstance(parsed, list):
        payload["members"] = parsed
    return payload


def extract_sql_block(message: str) -> str | None:
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", message, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    inline = re.search(r"`([^`]*\bselect\b[^`]*)`", message, flags=re.IGNORECASE | re.DOTALL)
    if inline:
        return inline.group(1).strip()
    cte_match = re.search(r"((?:with)\s+[A-Za-z_][A-Za-z0-9_]*\s+as\s*\(.+)", message, flags=re.IGNORECASE | re.DOTALL)
    if cte_match:
        return cte_match.group(1).strip()
    select_match = re.search(r"((?:select)\b.+\bfrom\b.+)", message, flags=re.IGNORECASE | re.DOTALL)
    if select_match:
        return select_match.group(1).strip()
    return None


def detect_connection_scope_and_type(lowered: str) -> tuple[str | None, str | None]:
    connection_type = None
    for key, values in CONNECTOR_TYPE_SYNONYMS.items():
        if any(value in lowered for value in values):
            connection_type = key
            break
    if connection_type:
        return "connector", connection_type
    for key, values in PROVIDER_CONNECTION_TYPE_SYNONYMS.items():
        if any(value in lowered for value in values):
            connection_type = key
            break
    if connection_type:
        return "provider_connection", connection_type
    if "connector" in lowered:
        return "connector", None
    if "provider connection" in lowered:
        return "provider_connection", None
    return None, None


def extract_resource_id(message: str, *, prefix: str) -> str | None:
    for match in re.finditer(rf"\b({re.escape(prefix)}[A-Za-z0-9]+)\b", message):
        candidate = match.group(1).strip()
        if candidate.lower() == f"{prefix}id":
            continue
        return candidate
    return None


def extract_experiment_id(message: str) -> str | None:
    explicit = re.search(r"\bexperiment(?: id)?\s*(?:is|=|:)?\s*([A-Za-z0-9_.\-]+)", message, flags=re.IGNORECASE)
    if explicit:
        return explicit.group(1).strip()
    generic = re.search(r"\b([A-Za-z0-9_.\-]*exp[A-Za-z0-9_.\-]*)\b", message, flags=re.IGNORECASE)
    return generic.group(1).strip() if generic else None


def default_named_resource(*, prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"


def normalize_percent_value(raw_value: str) -> float:
    value = float(raw_value)
    return value / 100.0 if value > 1 else value


def highest_risk_level(levels) -> str:
    resolved = "low"
    for level in levels:
        candidate = str(level or "low").strip().lower() or "low"
        if RISK_ORDER.get(candidate, 0) > RISK_ORDER.get(resolved, 0):
            resolved = candidate
    return resolved


def merge_slots(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base or {})
    for key, value in dict(incoming or {}).items():
        if key in {"config", "definition_json"} and isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = {**merged.get(key, {}), **value}
            continue
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        merged[key] = value
    return merged


def sanitize_action_parameters(parameters: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = dict(parameters or {})
    if isinstance(sanitized.get("config"), dict):
        sanitized["config"] = redact_secret_values(dict(sanitized["config"]))
    return sanitized


def deterministic_action_summary(action_type: str, result: Dict[str, Any]) -> str:
    if action_type == "summarize_dashboard":
        return str(((result.get("dashboard_summary") or {}).get("headline")) or "Summarized the dashboard.")
    return f"Completed `{action_type}`."


def build_summary_next_steps(alerts: List[Dict[str, Any]], blocked_imports: List[Dict[str, Any]], overview: Dict[str, Any]) -> List[str]:
    steps: List[str] = []
    if blocked_imports:
        steps.append("Resolve blocked imports before relying on downstream cohorts or experiments.")
    if any(str(item.get("module") or "") == "experiment_hub" for item in alerts):
        steps.append("Review experiment integrity warnings before making rollout decisions.")
    if any(str(item.get("module") or "") == "audience_engine" for item in alerts):
        steps.append("Inspect recent cohort refresh failures and rerun any stale audience definitions.")
    if int((overview.get("report_counts") or {}).get("pending_review") or 0) > 0:
        steps.append("Review pending Copilot reports so anomalies and recommendations do not age out.")
    if not steps:
        steps.append("The workspace looks healthy; the next safe step is to prepare the next cohort or experiment draft.")
    return steps


def artifact_for_cohort(cohort: Dict[str, Any]) -> Dict[str, Any]:
    cohort_id = str(cohort.get("cohort_id") or "")
    return {
        "resource_type": "cohort",
        "resource_id": cohort_id,
        "label": str(cohort.get("name") or cohort_id or "Cohort"),
        "module_id": "audience-engine",
        "page_id": "audience-engine",
        "api_path": f"/api/v1/cohorts/{quote(cohort_id)}" if cohort_id else "",
        "focus": {"cohort_id": cohort_id},
        "status": str(cohort.get("status") or ""),
    }


def artifact_for_experiment(experiment: Dict[str, Any]) -> Dict[str, Any]:
    experiment_id = str(experiment.get("experiment_id") or "")
    return {
        "resource_type": "experiment",
        "resource_id": experiment_id,
        "label": experiment_id or "Experiment",
        "module_id": "experiment-hub",
        "page_id": "experiment-hub",
        "api_path": f"/api/v1/experiments/config?experiment_id={quote(experiment_id)}" if experiment_id else "",
        "focus": {"experiment_id": experiment_id},
        "status": str(experiment.get("status") or ""),
    }


def artifact_for_connector(connector: Dict[str, Any]) -> Dict[str, Any]:
    connector_name = str(connector.get("name") or "")
    return {
        "resource_type": "connector",
        "resource_id": str(connector.get("connector_id") or connector_name),
        "label": connector_name or "Connector",
        "module_id": "data-core",
        "page_id": "connectors",
        "api_path": f"/api/v1/connectors/{quote(connector_name)}/health" if connector_name else "",
        "focus": {"connector_name": connector_name},
        "status": "configured",
    }


def artifact_for_provider_connection(connection: Dict[str, Any]) -> Dict[str, Any]:
    connection_id = str(connection.get("provider_connection_id") or "")
    return {
        "resource_type": "provider_connection",
        "resource_id": connection_id,
        "label": str(connection.get("name") or connection_id or "Provider Connection"),
        "module_id": "data-core",
        "page_id": "connectors",
        "api_path": f"/api/v1/provider-connections/{quote(connection_id)}" if connection_id else "",
        "focus": {"provider_connection_id": connection_id},
        "status": str(connection.get("status") or ""),
    }


def artifact_for_saved_query(saved_query: Dict[str, Any]) -> Dict[str, Any]:
    query_id = str(saved_query.get("query_id") or "")
    return {
        "resource_type": "saved_query",
        "resource_id": query_id,
        "label": str(saved_query.get("name") or query_id or "Saved Query"),
        "module_id": "audience-engine",
        "page_id": "audience-engine",
        "api_path": "",
        "focus": {"query_id": query_id},
        "status": "saved",
    }


def artifact_for_prediction_job(
    prediction_job: Dict[str, Any],
    *,
    resume_ready: bool | None = None,
    resume_message: str = "",
) -> Dict[str, Any]:
    job_id = str(prediction_job.get("id") or prediction_job.get("prediction_job_id") or "")
    status = str(prediction_job.get("status") or "").strip().lower()
    if resume_ready is None:
        resume_ready = status == "completed"
    progress_details = ((prediction_job.get("progress") or {}).get("details") or {})
    return {
        "resource_type": "prediction_job",
        "resource_id": job_id,
        "label": job_id or "Prediction Job",
        "module_id": "data-core",
        "page_id": "operator-hub",
        "api_path": f"/api/v1/predictions/{quote(job_id)}" if job_id else "",
        "focus": {"prediction_job_id": job_id},
        "status": status or "queued",
        "resume_ready": bool(resume_ready),
        "resume_message": str(resume_message or ""),
        "status_detail": str(progress_details.get("stale_reason") or ""),
    }


def artifact_for_email_campaign(campaign: Dict[str, Any]) -> Dict[str, Any]:
    campaign_id = str(campaign.get("email_campaign_id") or "")
    return {
        "resource_type": "email_campaign",
        "resource_id": campaign_id,
        "label": str(campaign.get("name") or campaign_id or "Email Campaign"),
        "module_id": "action-orchestrator",
        "page_id": "action-orchestrator",
        "api_path": f"/api/v1/email-campaigns/{quote(campaign_id)}" if campaign_id else "",
        "focus": {"email_campaign_id": campaign_id},
        "status": str(campaign.get("status") or ""),
    }


def artifact_for_workflow(workflow: Dict[str, Any]) -> Dict[str, Any]:
    workflow_id = str(workflow.get("workflow_id") or "")
    return {
        "resource_type": "workflow",
        "resource_id": workflow_id,
        "label": str(workflow.get("name") or workflow_id or "Workflow"),
        "module_id": "action-orchestrator",
        "page_id": "action-orchestrator",
        "api_path": f"/api/v1/workflows/{quote(workflow_id)}" if workflow_id else "",
        "focus": {"workflow_id": workflow_id},
        "status": str(workflow.get("status") or ""),
    }


def validate_prediction_preview(preview: Dict[str, Any]) -> None:
    rows = list(preview.get("rows") or [])
    if int(preview.get("row_count") or 0) <= 0:
        raise HTTPException(status_code=409, detail="The generated SQL preview returned no rows, so the cohort draft was not created.")
    sample_row = rows[0] if rows else {}
    if "canonical_user_id" not in sample_row:
        raise HTTPException(status_code=409, detail="The generated SQL preview must include canonical_user_id in the result.")


def heuristic_sql_from_prompt(prompt: str, *, hint: Dict[str, Any]) -> Dict[str, Any]:
    prediction_job_id = str(hint.get("prediction_job_id") or "").strip()
    include_risks = [str(item or "").strip().lower() for item in list(hint.get("include_risks") or ["high"]) if str(item or "").strip()]
    if not include_risks:
        include_risks = ["high"]
    quoted_risks = ", ".join(f"'{risk}'" for risk in include_risks)
    conditions = [f"prediction_job_id = '{prediction_job_id}'"] if prediction_job_id else []
    conditions.append(f"predicted_churn_risk IN ({quoted_risks})")
    lowered = str(prompt or "").lower()
    if "exclude churned" in lowered or "active high risk" in lowered or "win back" in lowered:
        conditions.append("COALESCE(churn_state, 'active') <> 'churned'")
    where_clause = " AND ".join(conditions)
    return {
        "sql": (
            "SELECT canonical_user_id, email\n"
            "FROM prediction_results\n"
            f"WHERE {where_clause}\n"
            "ORDER BY completed_at DESC"
        ),
        "query_name": str(hint.get("query_name") or default_named_resource(prefix="agent", suffix="high_risk_query")),
        "cohort_name": str(hint.get("cohort_name") or default_named_resource(prefix="agent", suffix="high_risk_cohort")),
    }


def dedupe_clarifications(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for item in items:
        key = str(item.get("key") or "").strip()
        if not key:
            continue
        deduped[key] = item
    return list(deduped.values())


def collect_artifacts_from_actions(*action_groups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    artifacts: List[Dict[str, Any]] = []
    for group in action_groups:
        for item in group:
            artifacts.extend(item.get("artifacts") or [])
    return dedupe_artifacts(artifacts)


def dedupe_artifacts(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for item in items:
        key = (str(item.get("resource_type") or ""), str(item.get("resource_id") or ""))
        deduped[key] = item
    return list(deduped.values())
