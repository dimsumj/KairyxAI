from __future__ import annotations

import uuid
from typing import Any, Dict, List

from app.application.secret_refs import (
    SECRET_ENCRYPTED_SUFFIX,
    SECRET_METADATA_SUFFIX,
    SECRET_REF_SUFFIX,
    SENSITIVE_FIELDS,
    contains_inline_secret,
    materialize_secret_refs,
    redact_secret_values,
    secure_inline_secret_values,
)
from app.core.settings import get_settings


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
        normalized_provider = str(provider or "").lower()
        self._validate_provider_config(normalized_provider, dict(config or {}))
        config = self._persist_inline_secrets(config)
        self._validate_provider_config(normalized_provider, config)
        if settings.app_env == "prod" and contains_inline_secret(config, secret_fields=SENSITIVE_FIELDS):
            raise ValueError(
                "Inline provider secrets are not allowed in production; configure CONTROL_PLANE_SECRET_KEY or use *_ref fields."
            )
        payload = self._build_payload(
            provider_connection_id=f"pc_{uuid.uuid4().hex[:20]}",
            name=name,
            provider=normalized_provider,
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
            current_config = dict(payload.get("config") or {})
            next_config_input = self._merge_config(current_config, dict(patch["config"] or {}))
            self._validate_provider_config(str(payload.get("provider") or ""), next_config_input)
            next_config = self._persist_inline_secrets(next_config_input)
            self._validate_provider_config(str(payload.get("provider") or ""), next_config)
            if settings.app_env == "prod" and contains_inline_secret(next_config, secret_fields=SENSITIVE_FIELDS):
                raise ValueError(
                    "Inline provider secrets are not allowed in production; configure CONTROL_PLANE_SECRET_KEY or use *_ref fields."
                )
            payload["config"] = next_config
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
        active_campaigns = self._list_active_referencing_campaigns(provider_connection_id)
        if active_campaigns:
            campaign_names = ", ".join(
                str(item.get("name") or item.get("email_campaign_id") or "unnamed_campaign")
                for item in active_campaigns[:3]
            )
            raise ValueError(
                "Provider connection is still used by active email campaigns. "
                f"Cancel or delete those campaigns first: {campaign_names}."
            )
        deleted = self.repository.delete_resource("provider_connection", provider_connection_id)
        if deleted:
            self.repository.record_action("provider_connection_deleted", "provider_connection", provider_connection_id, {"provider_connection_id": provider_connection_id})
        return deleted

    def _list_active_referencing_campaigns(self, provider_connection_id: str) -> List[Dict[str, Any]]:
        active_statuses = {"draft", "scheduled", "sending"}
        items: List[Dict[str, Any]] = []
        for record in self.repository.list_resources("email_campaign"):
            payload = dict(record.get("payload") or {})
            if str(payload.get("provider_connection_id") or "").strip() != str(provider_connection_id or "").strip():
                continue
            status = str(payload.get("status") or record.get("status") or "").strip().lower()
            if status in active_statuses:
                items.append(
                    {
                        "email_campaign_id": payload.get("email_campaign_id") or record.get("resource_id"),
                        "name": payload.get("name") or record.get("name"),
                        "status": status,
                    }
                )
        return items

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
            "config": config,
            "status": "active",
        }

    @staticmethod
    def _persist_inline_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return secure_inline_secret_values(config, secret_fields=SENSITIVE_FIELDS)
        except RuntimeError as exc:
            if contains_inline_secret(config, secret_fields=SENSITIVE_FIELDS):
                raise ValueError(
                    "Secure provider secret storage is not configured; set CONTROL_PLANE_SECRET_KEY or use *_ref fields."
                ) from exc
            raise

    @staticmethod
    def _merge_config(current_config: Dict[str, Any], patch_config: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(current_config or {})
        merged.update(dict(patch_config or {}))
        for field in SENSITIVE_FIELDS:
            if not merged.get(f"{field}{SECRET_METADATA_SUFFIX}"):
                continue
            if ProviderConnectionService._has_secret_reference(merged, field):
                continue
            if ProviderConnectionService._has_secret_reference(current_config, field):
                for suffix in (SECRET_REF_SUFFIX, SECRET_ENCRYPTED_SUFFIX):
                    current_key = f"{field}{suffix}"
                    if current_key in current_config:
                        merged[current_key] = current_config[current_key]
        return merged

    @staticmethod
    def _validate_provider_config(provider: str, config: Dict[str, Any]) -> None:
        normalized_provider = str(provider or "").strip().lower()
        if normalized_provider == "sendgrid":
            if not ProviderConnectionService._has_secret_reference(config, "api_key"):
                raise ValueError("SendGrid provider connections require api_key.")
            from_email = str((config or {}).get("from_email") or "").strip()
            if not from_email:
                raise ValueError("SendGrid provider connections require from_email.")
            base_url = str((config or {}).get("base_url") or "").strip()
            if base_url and not base_url.startswith(("https://", "http://")):
                raise ValueError("SendGrid provider base_url must start with https:// or http://.")
            return
        if normalized_provider != "braze":
            return
        if not ProviderConnectionService._has_secret_reference(config, "api_key"):
            raise ValueError("Braze provider connections require api_key.")
        rest_endpoint = str((config or {}).get("rest_endpoint") or "").strip()
        if not rest_endpoint:
            raise ValueError("Braze provider connections require rest_endpoint.")
        if not rest_endpoint.startswith(("https://", "http://")):
            raise ValueError("Braze provider rest_endpoint must start with https:// or http://.")

    @staticmethod
    def _has_secret_reference(config: Dict[str, Any] | None, field: str) -> bool:
        payload = dict(config or {})
        raw_value = payload.get(field)
        if isinstance(raw_value, str) and raw_value.strip():
            return True
        return any(
            isinstance(payload.get(f"{field}{suffix}"), str) and str(payload.get(f"{field}{suffix}") or "").strip()
            for suffix in (SECRET_REF_SUFFIX, SECRET_ENCRYPTED_SUFFIX)
        )

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
