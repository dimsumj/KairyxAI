from __future__ import annotations

import uuid
from typing import Any, Dict, List

from app.application.secret_refs import contains_inline_secret, materialize_secret_refs, redact_secret_values
from app.core.settings import get_settings

SENSITIVE_FIELDS = {
    "api_key",
    "api_token",
    "callback_signing_secret",
    "password",
    "secret_key",
    "signing_secret",
    "webhook_token",
}


class ProviderConnectionService:
    def __init__(self, repository):
        self.repository = repository

    def list_connections(self) -> List[Dict[str, Any]]:
        items = []
        for record in self.repository.list_resources("provider_connection"):
            items.append(self._to_response(record))
        return items

    def get_connection(self, provider_connection_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource("provider_connection", provider_connection_id)
        return self._to_response(record) if record else None

    def create_connection(self, name: str, provider: str, config: Dict[str, Any]) -> Dict[str, Any]:
        settings = get_settings()
        if settings.app_env == "prod" and contains_inline_secret(config):
            raise ValueError("Inline provider secrets are not allowed in production; use *_ref fields.")
        payload = self._build_payload(
            provider_connection_id=f"pc_{uuid.uuid4().hex[:20]}",
            name=name,
            provider=provider,
            config=config,
        )
        record = self.repository.upsert_resource(
            "provider_connection",
            payload["provider_connection_id"],
            status="active",
            name=name,
            payload=payload,
        )
        self.repository.record_action("provider_connection_created", "provider_connection", payload["provider_connection_id"], payload)
        return self._to_response(record)

    def update_connection(self, provider_connection_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        record = self.repository.get_resource("provider_connection", provider_connection_id)
        if record is None:
            raise KeyError(provider_connection_id)
        settings = get_settings()
        payload = dict(record.get("payload") or {})
        if patch.get("name") is not None:
            payload["name"] = patch["name"]
        if patch.get("config") is not None:
            if settings.app_env == "prod" and contains_inline_secret(patch["config"]):
                raise ValueError("Inline provider secrets are not allowed in production; use *_ref fields.")
            payload["config"] = self._sanitize_config(patch["config"])
        saved = self.repository.upsert_resource(
            "provider_connection",
            provider_connection_id,
            status=str(payload.get("status") or "active"),
            name=payload.get("name"),
            payload=payload,
        )
        self.repository.record_action("provider_connection_updated", "provider_connection", provider_connection_id, patch)
        return self._to_response(saved)

    def delete_connection(self, provider_connection_id: str) -> bool:
        deleted = self.repository.delete_resource("provider_connection", provider_connection_id)
        if deleted:
            self.repository.record_action("provider_connection_deleted", "provider_connection", provider_connection_id, {"provider_connection_id": provider_connection_id})
        return deleted

    def resolve_connection(self, provider_connection_id: str) -> Dict[str, Any]:
        record = self.repository.get_resource("provider_connection", provider_connection_id)
        if record is None:
            raise KeyError(provider_connection_id)
        payload = self._to_response(record)
        payload["config"] = materialize_secret_refs(dict((record.get("payload") or {}).get("config") or {}))
        return payload

    def _build_payload(self, *, provider_connection_id: str, name: str, provider: str, config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "provider_connection_id": provider_connection_id,
            "name": name,
            "provider": str(provider).lower(),
            "config": self._sanitize_config(config),
            "status": "active",
        }

    def _sanitize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = dict(config or {})
        for field in SENSITIVE_FIELDS:
            if field in sanitized and sanitized[field]:
                sanitized[f"{field}_stored_inline"] = True
        return sanitized

    @staticmethod
    def _to_response(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            "provider_connection_id": payload.get("provider_connection_id") or record.get("resource_id"),
            "name": payload.get("name") or record.get("name"),
            "provider": payload.get("provider"),
            "status": record.get("status") or payload.get("status") or "active",
            "config": redact_secret_values(dict(payload.get("config") or {})),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
        }
