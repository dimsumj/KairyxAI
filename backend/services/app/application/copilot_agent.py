from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Protocol
from urllib.parse import quote

from fastapi import HTTPException

from bigquery_service import BigQueryService, get_shared_bigquery_service
from gemini_client import GeminiClient

from app.application.cohorts import CohortService
from app.application.connectors import ConnectorService
from app.application.copilot import CopilotService
from app.application.experiments import ExperimentConfigService
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


class GeminiCopilotAgentModel:
    def __init__(self, repository):
        self.repository = repository
        self.client = self._build_gemini_client()

    def parse_message(
        self,
        message: str,
        *,
        session_state: Dict[str, Any],
        ui_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        heuristic = deterministic_agent_parse(message, ui_context=ui_context)
        if self.client is None:
            return heuristic
        prompt = {
            "task": "Classify the operator request and extract structured slots for the Kytrics/Kairyx control plane.",
            "instructions": [
                "Return JSON only.",
                "Keep the intent one of summarize_dashboard, setup_cohort, setup_experiment, setup_connection, activate_cohort, pause_cohort, archive_cohort, restore_cohort, start_experiment, stop_experiment, record_experiment_decision, unsupported.",
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
            raw = self.client.get_ai_response(json.dumps(prompt))
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
        if self.client is None:
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
            raw = self.client.get_ai_response(json.dumps(prompt))
            parsed = extract_json_object(raw)
            message = str(parsed.get("assistant_message") or "").strip()
            return message or fallback
        except Exception:
            return fallback

    def _build_gemini_client(self) -> GeminiClient | None:
        google_connectors = [
            connector
            for connector in self.repository.list_connectors()
            if str(connector.get("type") or "").lower() == "google"
            and str((connector.get("config") or {}).get("api_key") or "").strip()
        ]
        if google_connectors:
            connector = max(google_connectors, key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""))
            config = connector.get("config") or {}
            api_key = str(config.get("api_key") or "").strip()
            model_name = str(config.get("model_name") or "").strip() or None
            if api_key:
                try:
                    return GeminiClient(api_key=api_key, model_name=model_name, circuit_namespace="copilot_agent")
                except Exception:
                    return None
        if str(os.getenv("GOOGLE_API_KEY") or "").strip():
            try:
                return GeminiClient(circuit_namespace="copilot_agent")
            except Exception:
                return None
        return None


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
    if "permanent delete" in lowered or re.search(r"\bdelete\b", lowered):
        return {"intent": "unsupported", "slots": slots, "notes": ["Destructive delete flows are out of scope for the v1 agent."]}
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
    return {"intent": "unsupported", "slots": slots, "notes": ["The agent supports summaries plus cohort, experiment, and connection setup in v1."]}


def deterministic_agent_message(payload: Dict[str, Any]) -> str:
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
        self.model_adapter: CopilotAgentModelAdapter = GeminiCopilotAgentModel(repository)

    def create_session(self, *, title: str = "", ui_context: Dict[str, Any] | None = None) -> Dict[str, Any]:
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
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        self.repository.upsert_resource(SESSION_RESOURCE_TYPE, session_id, status="active", name=payload["title"], payload=payload)
        self.repository.record_resource_event(SESSION_RESOURCE_TYPE, session_id, event_type="session_created", payload=payload)
        return self.get_session(session_id)

    def get_session(self, session_id: str) -> Dict[str, Any]:
        payload = self._get_session_payload(session_id)
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
        session = self._get_session_payload(session_id)
        merged_ui_context = dict(session.get("ui_context") or {})
        merged_ui_context.update(dict(ui_context or {}))
        parsed = self._normalize_parsed_request(
            session,
            self.model_adapter.parse_message(message, session_state=session, ui_context=merged_ui_context),
            message=message,
        )
        plan = self._build_plan(message=message, parsed=parsed, ui_context=merged_ui_context, context=context)

        completed_actions: List[Dict[str, Any]] = []
        pending_confirmations: List[Dict[str, Any]] = []
        artifacts: List[Dict[str, Any]] = []
        session_status = "active"

        if plan["clarifications"]:
            session_status = "awaiting_input"
        else:
            execution_result = self._execute_plan(
                session_id=session_id,
                plan=plan,
                context=context,
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
        response_payload["assistant_message"] = self.model_adapter.compose_message(response_payload)

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

        result = self._execute_action(action_payload["action_type"], action_payload.get("parameters") or {}, context=context)
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

        assistant_message = self.model_adapter.compose_message(
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
        if intent == "unsupported":
            clarifications.append(
                {
                    "key": "supported_scope",
                    "label": "Task",
                    "question": "I can summarize the dashboard or help set up a cohort, experiment, or connection. What do you want to do?",
                    "required": True,
                    "input_type": "text",
                    "options": ["summarize dashboard", "set up a cohort", "set up an experiment", "set up a connection"],
                }
            )
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

    def _execute_plan(self, *, session_id: str, plan: Dict[str, Any], context: GovernanceContext) -> Dict[str, Any]:
        completed_actions: List[Dict[str, Any]] = []
        pending_confirmations: List[Dict[str, Any]] = []
        artifacts: List[Dict[str, Any]] = []
        session_status = "active"
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
                result = self._execute_action(action_type, parameters, context=context)
                action_payload.update(
                    {
                        "status": "completed",
                        "result": result.get("result") or {},
                        "summary": result.get("summary") or deterministic_action_summary(action_type, result.get("result") or {}),
                        "artifacts": result.get("artifacts") or [],
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                )
                self.repository.upsert_resource(ACTION_RESOURCE_TYPE, action_payload["action_id"], status="completed", name=action_payload["title"], payload=action_payload)
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

    def _execute_action(self, action_type: str, parameters: Dict[str, Any], *, context: GovernanceContext) -> Dict[str, Any]:
        if action_type == "summarize_dashboard":
            return self._execute_dashboard_summary(parameters)
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
    if intent == "summarize_dashboard":
        return "Read the current workspace state and return an operator summary with risks and next steps."
    if intent == "setup_connection":
        return "Create or update the connection and optionally verify connector health."
    if intent == "setup_cohort":
        return "Preview inputs, persist any supporting query, and create a draft cohort."
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
