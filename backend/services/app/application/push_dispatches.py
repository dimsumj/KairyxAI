from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List

from app.application.secret_refs import redact_secret_values
from app.application.workflows import WorkflowService
from app.core.request_context import get_request_context


class PushDispatchService:
    _RESOURCE_TYPE = "push_dispatch"
    _AUDIENCE_EXPLICIT = "explicit_user_ids"
    _AUDIENCE_ALL_PLAYERS = "provider_broadcast_all_players"

    def __init__(self, repository):
        self.repository = repository
        self.workflows = WorkflowService(repository)

    def list_dispatches(self) -> List[Dict[str, Any]]:
        items = [self._to_response(item) for item in self.repository.list_resources(self._RESOURCE_TYPE)]
        return sorted(
            items,
            key=lambda item: str(item.get("created_at") or ""),
            reverse=True,
        )

    def get_dispatch(self, push_dispatch_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource(self._RESOURCE_TYPE, push_dispatch_id)
        return self._to_response(record) if record else None

    def archive_dispatch(self, push_dispatch_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource(self._RESOURCE_TYPE, push_dispatch_id)
        if record is None:
            raise KeyError(push_dispatch_id)
        payload = dict(record.get("payload") or {})
        status = str(payload.get("status") or "").strip().lower()
        if status == "archived":
            raise ValueError("Push dispatch is already archived.")
        if status not in {"sent", "failed"}:
            raise ValueError("Only completed push dispatches can be archived.")
        archived_at = datetime.utcnow().isoformat()
        payload["status"] = "archived"
        payload["archived_at"] = archived_at
        saved = self.repository.upsert_resource(
            self._RESOURCE_TYPE,
            push_dispatch_id,
            status="archived",
            name=payload.get("name"),
            payload=payload,
            tenant_id=payload.get("tenant_id"),
            project_id=payload.get("project_id"),
        )
        archive_payload = {
            "push_dispatch_id": push_dispatch_id,
            "status": "archived",
            "archived_at": archived_at,
        }
        self.repository.record_resource_event(self._RESOURCE_TYPE, push_dispatch_id, event_type="push_dispatch_archived", payload=archive_payload)
        self.repository.record_action("push_dispatch_archived", self._RESOURCE_TYPE, push_dispatch_id, archive_payload)
        return self._to_response(saved)

    def send_now(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = self._normalize_payload(payload or {})
        push_dispatch_id = f"pd_{uuid.uuid4().hex[:20]}"
        request_context = get_request_context()
        started_at = datetime.utcnow().isoformat()

        action = self.workflows._resolve_provider_connection_config(dict(normalized["action"]))
        action = self.workflows._validate_action_for_execution(action, workflow_name=normalized["name"])
        provider_name = self.workflows._resolve_provider_name(action) or "simulator"
        provider_request_id = push_dispatch_id
        action_payload = self._build_action_payload(
            push_dispatch_id=push_dispatch_id,
            user_ids=normalized["user_ids"],
            audience_mode=normalized["audience_mode"],
            action=action,
            provider_request_id=provider_request_id,
        )
        provider_result = self.workflows._execute_action_with_retry(action_payload, action)
        completed_at = datetime.utcnow().isoformat()
        status = "sent" if provider_result.get("ok") else "failed"

        record_payload = {
            "push_dispatch_id": push_dispatch_id,
            "name": normalized["name"],
            "status": status,
            "channel": "push_notification",
            "user_id": normalized["user_id"],
            "user_ids": normalized["user_ids"],
            "audience_mode": normalized["audience_mode"],
            "provider": provider_result.get("provider") or provider_name,
            "provider_mode": provider_result.get("provider_mode") or ("simulator" if provider_name == "simulator" else "live"),
            "provider_backend": provider_result.get("provider_backend") or provider_result.get("provider") or provider_name,
            "provider_connection_id": action.get("provider_connection_id"),
            "campaign_name": action.get("campaign_name"),
            "title": action.get("title"),
            "body": action.get("body") or action.get("content"),
            "deep_link": action.get("deep_link"),
            "deep_link_token": action.get("deep_link_token"),
            "data": dict(action.get("data") or {}),
            "provider_options": dict(action.get("provider_options") or {}),
            "provider_request_id": provider_request_id,
            "provider_campaign_id": provider_result.get("provider_campaign_id"),
            "provider_accepted": provider_result.get("accepted"),
            "simulated": bool(provider_result.get("simulated")),
            "send_attempts": int(provider_result.get("attempt_count") or 1),
            "last_send_started_at": started_at,
            "last_send_completed_at": completed_at,
            "last_error": provider_result.get("error"),
            "callback_count": 0,
            "last_callback_at": None,
            "last_provider_event": None,
            "result_summary": {
                "accepted": provider_result.get("accepted"),
                "duplicate": provider_result.get("duplicate"),
                "status_code": provider_result.get("status_code"),
                "error": provider_result.get("error"),
                "fallback_reason": provider_result.get("fallback_reason"),
                "failure_classification": provider_result.get("failure_classification"),
                "retry_schedule_seconds": provider_result.get("retry_schedule_seconds", []),
                "attempts": provider_result.get("attempts", []),
                "provider_response": provider_result.get("provider_response_body"),
            },
            "tenant_id": request_context.tenant_id if request_context else None,
            "project_id": request_context.project_id if request_context else None,
        }

        saved = self.repository.upsert_resource(
            self._RESOURCE_TYPE,
            push_dispatch_id,
            status=status,
            name=record_payload["name"],
            payload=record_payload,
            tenant_id=record_payload.get("tenant_id"),
            project_id=record_payload.get("project_id"),
        )
        event_type = "push_dispatch_sent" if status == "sent" else "push_dispatch_failed"
        self.repository.record_resource_event(self._RESOURCE_TYPE, push_dispatch_id, event_type=event_type, payload=record_payload)
        self.repository.record_action(event_type, self._RESOURCE_TYPE, push_dispatch_id, record_payload)
        return self._to_response(saved)

    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        name = str(payload.get("name") or "").strip() or f"one_time_push_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        user_ids = self._normalize_user_ids(payload)
        audience_mode = self._AUDIENCE_EXPLICIT if user_ids else self._AUDIENCE_ALL_PLAYERS

        body = str(payload.get("body") or payload.get("content") or "").strip()
        if not body:
            raise ValueError("One-time push send requires body.")

        data = payload.get("data")
        if data in (None, ""):
            data = {}
        if not isinstance(data, dict):
            raise ValueError("Push data must be a JSON object.")

        provider_options = payload.get("provider_options")
        if provider_options in (None, ""):
            provider_options = {}
        if not isinstance(provider_options, dict):
            raise ValueError("Provider options must be a JSON object.")

        action = {
            "channel": "push_notification",
            "provider_connection_id": str(payload.get("provider_connection_id") or "").strip() or None,
            "campaign_name": str(payload.get("campaign_name") or "").strip() or name,
            "title": str(payload.get("title") or "").strip(),
            "body": body,
            "content": body,
            "deep_link": str(payload.get("deep_link") or "").strip(),
            "deep_link_token": str(payload.get("deep_link_token") or "").strip(),
            "data": data,
            "provider_options": provider_options,
        }
        resolved_action = self.workflows._resolve_provider_connection_config(dict(action))
        if not user_ids and not self.workflows._is_live_provider_push_action(resolved_action):
            raise ValueError("Broadcast push sends require a live Wynn PushNotifier provider connection.")
        return {
            "name": name,
            "user_id": user_ids[0] if len(user_ids) == 1 else None,
            "user_ids": user_ids,
            "audience_mode": audience_mode,
            "action": action,
        }

    @staticmethod
    def _normalize_user_ids(payload: Dict[str, Any]) -> List[str]:
        raw_user_ids = payload.get("user_ids")
        if raw_user_ids in (None, ""):
            raw_values = []
        elif isinstance(raw_user_ids, list):
            raw_values = raw_user_ids
        else:
            raise ValueError("user_ids must be an array.")
        legacy_user_id = str(payload.get("user_id") or "").strip()
        combined = [*raw_values, legacy_user_id] if legacy_user_id else list(raw_values)
        seen: set[str] = set()
        normalized: List[str] = []
        for value in combined:
            user_id = str(value or "").strip()
            if not user_id or user_id in seen:
                continue
            seen.add(user_id)
            normalized.append(user_id)
        return normalized

    def _build_action_payload(
        self,
        *,
        push_dispatch_id: str,
        user_ids: List[str],
        audience_mode: str,
        action: Dict[str, Any],
        provider_request_id: str,
    ) -> Dict[str, Any]:
        request_context = get_request_context()
        outbound_context = {
            "push_dispatch_id": push_dispatch_id,
            "audience_mode": audience_mode,
            "provider_connection_id": action.get("provider_connection_id"),
            "tenant_id": request_context.tenant_id if request_context else None,
            "project_id": request_context.project_id if request_context else None,
        }
        if self.workflows._is_live_provider_push_action(action):
            callback_context = self.workflows._build_wynn_callback_context(action)
            if callback_context:
                outbound_context["kairyx_callback"] = callback_context
        return {
            "decision": "ACT",
            "channel": "push_notification",
            "content": action.get("content") or action.get("body") or "",
            "title": action.get("title"),
            "body": action.get("body") or action.get("content") or "",
            "campaign_name": action.get("campaign_name"),
            "data": dict(action.get("data") or {}),
            "deep_link": action.get("deep_link"),
            "deep_link_token": action.get("deep_link_token") or action.get("default_deep_link_token"),
            "provider_options": dict(action.get("provider_options") or {}),
            "player_id": list(user_ids),
            "player_ids": list(user_ids),
            "audience_mode": audience_mode,
            "api_token": action.get("api_token"),
            "base_url": action.get("base_url"),
            "provider": action.get("provider"),
            "provider_connection_id": action.get("provider_connection_id"),
            "provider_request_id": provider_request_id,
            "execution_id": push_dispatch_id,
            "tenant_id": request_context.tenant_id if request_context else None,
            "project_id": request_context.project_id if request_context else None,
            "context": outbound_context,
        }

    def _to_response(self, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        push_dispatch_id = str(payload.get("push_dispatch_id") or record.get("resource_id") or "").strip()
        raw_user_ids = payload.get("user_ids")
        user_ids = raw_user_ids if isinstance(raw_user_ids, list) else []
        user_ids = [str(item).strip() for item in user_ids if str(item or "").strip()]
        legacy_user_id = str(payload.get("user_id") or "").strip()
        if legacy_user_id and legacy_user_id not in user_ids:
            user_ids = [legacy_user_id, *user_ids]
        provider = str(payload.get("provider") or payload.get("provider_backend") or "").strip()
        if not provider:
            provider = "simulator" if bool(payload.get("simulated")) else "unknown"
        provider_mode = str(payload.get("provider_mode") or "").strip()
        if not provider_mode:
            provider_mode = "simulator" if provider == "simulator" else "live"
        payload["push_dispatch_id"] = push_dispatch_id
        payload["name"] = str(payload.get("name") or record.get("name") or push_dispatch_id or "Push Dispatch")
        payload["status"] = str(payload.get("status") or record.get("status") or "unknown")
        payload["channel"] = str(payload.get("channel") or "push_notification")
        payload["user_ids"] = user_ids
        payload["user_id"] = legacy_user_id or (user_ids[0] if len(user_ids) == 1 else None)
        payload["audience_mode"] = str(
            payload.get("audience_mode")
            or (self._AUDIENCE_EXPLICIT if user_ids else self._AUDIENCE_ALL_PLAYERS)
        )
        payload["provider"] = provider
        payload["provider_mode"] = provider_mode
        payload["provider_backend"] = str(payload.get("provider_backend") or provider)
        if not isinstance(payload.get("data"), dict):
            payload["data"] = {}
        if not isinstance(payload.get("provider_options"), dict):
            payload["provider_options"] = {}
        payload["send_attempts"] = self._safe_int(payload.get("send_attempts"))
        payload["callback_count"] = self._safe_int(payload.get("callback_count"))
        if not isinstance(payload.get("result_summary"), dict):
            payload["result_summary"] = {}
        payload["callback_summary"] = self._build_callback_summary(payload)
        payload["created_at"] = payload.get("created_at") or record.get("created_at")
        payload["updated_at"] = payload.get("updated_at") or record.get("updated_at")
        payload["archived_at"] = payload.get("archived_at")
        payload["tenant_id"] = payload.get("tenant_id") or record.get("tenant_id")
        payload["project_id"] = payload.get("project_id") or record.get("project_id")
        payload["created_by"] = payload.get("created_by") or record.get("created_by") or "system"
        payload["updated_by"] = payload.get("updated_by") or record.get("updated_by") or "system"
        payload["correlation_id"] = payload.get("correlation_id") or record.get("correlation_id") or ""
        return redact_secret_values(payload)

    @staticmethod
    def _safe_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _build_callback_summary(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        provider_request_id = str(payload.get("provider_request_id") or "").strip()
        push_dispatch_id = str(payload.get("push_dispatch_id") or "").strip()
        counts = {"opened": 0, "clicked": 0, "claimed": 0, "returned": 0, "purchase": 0}
        users = {key: set() for key in counts}
        if not provider_request_id and not push_dispatch_id:
            return {"event_counts": counts, "unique_user_counts": {key: 0 for key in counts}}
        for record in self.repository.list_resources(
            "provider_callback",
            tenant_id=payload.get("tenant_id") or None,
            project_id=payload.get("project_id") or None,
        ):
            callback_payload = dict(record.get("payload") or {})
            if provider_request_id and str(callback_payload.get("provider_request_id") or "").strip() == provider_request_id:
                pass
            elif push_dispatch_id and str(callback_payload.get("push_dispatch_id") or "").strip() == push_dispatch_id:
                pass
            else:
                continue
            event_type = str(callback_payload.get("event_type") or "").strip().lower()
            outcome_name = str(callback_payload.get("outcome_name") or "").strip().lower()
            user_id = str(callback_payload.get("user_id") or "").strip()
            if event_type in counts:
                counts[event_type] += 1
                if user_id:
                    users[event_type].add(user_id)
            if outcome_name == "purchase":
                counts["purchase"] += 1
                if user_id:
                    users["purchase"].add(user_id)
        return {
            "event_counts": counts,
            "unique_user_counts": {key: len(value) for key, value in users.items()},
        }
