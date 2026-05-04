from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List

from app.application.ai_runtime_network import normalize_and_validate_runtime_base_url
from app.application.secret_refs import (
    SECRET_STORAGE_SUFFIXES,
    SENSITIVE_FIELDS,
    contains_inline_secret,
    materialize_secret_refs,
    redact_secret_values,
    secure_inline_secret_values,
)
from app.core.settings import get_settings


AGENT_MODEL_PROFILE_RESOURCE_TYPE = "agent_model_profile"
SYSTEM_GEMINI_PROFILE_ID = "agent_model_profile_system_gemini"
SUPPORTED_AGENT_MODEL_PROVIDERS = {"gemini", "openai", "anthropic"}


class AgentModelProfileService:
    def __init__(self, repository):
        self.repository = repository

    def list_profiles(self) -> List[Dict[str, Any]]:
        items = [self._to_response(record) for record in self.repository.list_resources(AGENT_MODEL_PROFILE_RESOURCE_TYPE)]
        explicit_default_id = next(
            (
                str(item.get("model_profile_id") or "")
                for item in items
                if bool(item.get("is_default"))
            ),
            "",
        )
        system_profile = self._build_system_gemini_profile()
        if system_profile is not None:
            system_profile["is_default"] = not explicit_default_id
            items.append(system_profile)
        if not explicit_default_id and not any(bool(item.get("is_default")) for item in items) and items:
            preferred = next((item for item in items if str(item.get("provider") or "") == "gemini"), items[0])
            preferred["is_default"] = True
        items.sort(
            key=lambda item: (
                0 if bool(item.get("is_default")) else 1,
                0 if bool(item.get("system_managed")) else 1,
                str(item.get("name") or "").lower(),
            )
        )
        return items

    def get_profile(self, model_profile_id: str) -> Dict[str, Any] | None:
        if str(model_profile_id or "").strip() == SYSTEM_GEMINI_PROFILE_ID:
            return self._build_system_gemini_profile()
        record = self.repository.get_resource(AGENT_MODEL_PROFILE_RESOURCE_TYPE, model_profile_id)
        return self._to_response(record) if record else None

    def resolve_profile(self, model_profile_id: str | None = None) -> Dict[str, Any] | None:
        requested_id = str(model_profile_id or "").strip()
        if requested_id == SYSTEM_GEMINI_PROFILE_ID:
            return self._materialize_system_gemini_profile()
        if requested_id:
            record = self.repository.get_resource(AGENT_MODEL_PROFILE_RESOURCE_TYPE, requested_id)
            if record is None:
                raise KeyError(requested_id)
            return self._materialize_record(record)
        default_profile = self.get_default_profile()
        if default_profile is None:
            return None
        if str(default_profile.get("model_profile_id") or "") == SYSTEM_GEMINI_PROFILE_ID:
            return self._materialize_system_gemini_profile()
        record = self.repository.get_resource(
            AGENT_MODEL_PROFILE_RESOURCE_TYPE,
            str(default_profile.get("model_profile_id") or ""),
        )
        if record is None:
            return None
        return self._materialize_record(record)

    def get_default_profile(self) -> Dict[str, Any] | None:
        items = self.list_profiles()
        return items[0] if items else None

    def create_profile(
        self,
        *,
        name: str,
        provider: str,
        model_name: str | None,
        config: Dict[str, Any],
        is_default: bool = False,
    ) -> Dict[str, Any]:
        normalized_provider = str(provider or "").strip().lower()
        normalized_name = str(name or "").strip()
        normalized_model_name = str(model_name or "").strip() or None
        normalized_config = self._normalize_profile_config(normalized_provider, dict(config or {}))
        self._validate_profile(
            provider=normalized_provider,
            model_name=normalized_model_name,
            config=normalized_config,
        )
        persisted_config = self._persist_inline_secrets(normalized_config)
        payload = {
            "model_profile_id": f"amp_{uuid.uuid4().hex[:20]}",
            "name": normalized_name,
            "provider": normalized_provider,
            "model_name": normalized_model_name,
            "config": persisted_config,
            "status": "active",
            "is_default": bool(is_default),
            "system_managed": False,
        }
        if bool(is_default):
            self._unset_default_profile()
        record = self.repository.upsert_resource(
            AGENT_MODEL_PROFILE_RESOURCE_TYPE,
            payload["model_profile_id"],
            status="active",
            name=payload["name"],
            payload=payload,
        )
        self.repository.record_resource_event(
            AGENT_MODEL_PROFILE_RESOURCE_TYPE,
            payload["model_profile_id"],
            event_type="agent_model_profile_created",
            payload={"provider": normalized_provider, "model_name": normalized_model_name, "is_default": bool(is_default)},
        )
        self.repository.record_action(
            "agent_model_profile_created",
            AGENT_MODEL_PROFILE_RESOURCE_TYPE,
            payload["model_profile_id"],
            {"provider": normalized_provider, "model_name": normalized_model_name, "is_default": bool(is_default)},
        )
        return self._to_response(record)

    def update_profile(self, model_profile_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
        record = self.repository.get_resource(AGENT_MODEL_PROFILE_RESOURCE_TYPE, model_profile_id)
        if record is None:
            raise KeyError(model_profile_id)
        payload = dict(record.get("payload") or {})
        if patch.get("name") is not None:
            payload["name"] = str(patch.get("name") or "").strip()
        if patch.get("provider") is not None:
            payload["provider"] = str(patch.get("provider") or "").strip().lower()
        if "model_name" in patch:
            payload["model_name"] = str(patch.get("model_name") or "").strip() or None
        if patch.get("config") is not None:
            merged_config = self._merge_profile_config(
                dict(payload.get("config") or {}),
                dict(patch.get("config") or {}),
            )
            normalized_config = self._normalize_profile_config(str(payload.get("provider") or ""), merged_config)
            payload["config"] = self._persist_inline_secrets(normalized_config)
        if patch.get("status") is not None:
            payload["status"] = str(patch.get("status") or "active").strip().lower() or "active"
        if patch.get("is_default") is not None:
            next_is_default = bool(patch.get("is_default"))
            if next_is_default:
                self._unset_default_profile(except_profile_id=model_profile_id)
            payload["is_default"] = next_is_default
        self._validate_profile(
            provider=str(payload.get("provider") or ""),
            model_name=payload.get("model_name"),
            config=dict(payload.get("config") or {}),
        )
        saved = self.repository.upsert_resource(
            AGENT_MODEL_PROFILE_RESOURCE_TYPE,
            model_profile_id,
            status=str(payload.get("status") or "active"),
            name=payload.get("name"),
            payload=payload,
        )
        self.repository.record_resource_event(
            AGENT_MODEL_PROFILE_RESOURCE_TYPE,
            model_profile_id,
            event_type="agent_model_profile_updated",
            payload={"patch": patch},
        )
        self.repository.record_action(
            "agent_model_profile_updated",
            AGENT_MODEL_PROFILE_RESOURCE_TYPE,
            model_profile_id,
            {"patch": patch},
        )
        return self._to_response(saved)

    def delete_profile(self, model_profile_id: str) -> bool:
        if str(model_profile_id or "").strip() == SYSTEM_GEMINI_PROFILE_ID:
            raise ValueError("System-managed Gemini default cannot be deleted.")
        deleted = self.repository.delete_resource(AGENT_MODEL_PROFILE_RESOURCE_TYPE, model_profile_id)
        if deleted:
            self.repository.record_action(
                "agent_model_profile_deleted",
                AGENT_MODEL_PROFILE_RESOURCE_TYPE,
                model_profile_id,
                {"model_profile_id": model_profile_id},
            )
        return deleted

    def _build_system_gemini_profile(self) -> Dict[str, Any] | None:
        api_key = str(os.getenv("GOOGLE_API_KEY") or "").strip()
        if not api_key:
            return None
        model_name = str(os.getenv("GOOGLE_GEMINI_MODEL") or "gemini-2.5-flash").strip() or "gemini-2.5-flash"
        return {
            "model_profile_id": SYSTEM_GEMINI_PROFILE_ID,
            "name": "Gemini (System Default)",
            "provider": "gemini",
            "model_name": model_name,
            "status": "active",
            "is_default": False,
            "system_managed": True,
            "config": {"api_key": None, "api_key_configured": True},
            "tenant_id": None,
            "project_id": None,
            "created_by": "system",
            "updated_by": "system",
            "correlation_id": "",
            "created_at": None,
            "updated_at": None,
            "model_selection_source": "system_default",
        }

    def _materialize_system_gemini_profile(self) -> Dict[str, Any] | None:
        profile = self._build_system_gemini_profile()
        if profile is None:
            return None
        return {
            **profile,
            "config": {"api_key": str(os.getenv("GOOGLE_API_KEY") or "").strip()},
            "model_selection_source": "system_default",
        }

    def _materialize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        materialized_config = materialize_secret_refs(dict(payload.get("config") or {}))
        return {
            **self._to_response(record),
            "config": materialized_config,
            "model_selection_source": "profile",
        }

    def _unset_default_profile(self, *, except_profile_id: str | None = None) -> None:
        for record in self.repository.list_resources(AGENT_MODEL_PROFILE_RESOURCE_TYPE):
            payload = dict(record.get("payload") or {})
            profile_id = str(payload.get("model_profile_id") or record.get("resource_id") or "")
            if profile_id == str(except_profile_id or ""):
                continue
            if not bool(payload.get("is_default")):
                continue
            payload["is_default"] = False
            self.repository.upsert_resource(
                AGENT_MODEL_PROFILE_RESOURCE_TYPE,
                profile_id,
                status=str(payload.get("status") or "active"),
                name=payload.get("name"),
                payload=payload,
            )

    @staticmethod
    def _persist_inline_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return secure_inline_secret_values(config, secret_fields=SENSITIVE_FIELDS)
        except RuntimeError as exc:
            if contains_inline_secret(config, secret_fields=SENSITIVE_FIELDS):
                raise ValueError(
                    "Secure agent model profile secret storage is not configured; set CONTROL_PLANE_SECRET_KEY or use *_ref fields."
                ) from exc
            raise

    @staticmethod
    def _merge_profile_config(current_config: Dict[str, Any], patch_config: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(current_config or {})
        incoming = dict(patch_config or {})
        for field in SENSITIVE_FIELDS:
            secret_keys = [field, *(f"{field}{suffix}" for suffix in SECRET_STORAGE_SUFFIXES)]
            if not any(key in incoming for key in secret_keys):
                continue
            for key in secret_keys:
                merged.pop(key, None)
        merged.update(incoming)
        return merged

    @staticmethod
    def _validate_profile(*, provider: str, model_name: str | None, config: Dict[str, Any]) -> None:
        normalized_provider = str(provider or "").strip().lower()
        if normalized_provider not in SUPPORTED_AGENT_MODEL_PROVIDERS:
            raise ValueError(f"Unsupported agent model provider '{provider}'.")
        name = str(model_name or "").strip()
        payload = dict(config or {})
        has_api_key = AgentModelProfileService._has_secret_reference(payload, "api_key")
        base_url = str(payload.get("base_url") or "").strip()
        if normalized_provider in {"gemini", "anthropic"} and not has_api_key:
            raise ValueError(f"{normalized_provider.title()} agent model profiles require api_key.")
        if normalized_provider == "openai" and not has_api_key and not base_url:
            raise ValueError("OpenAI agent model profiles require api_key or base_url.")
        if normalized_provider in {"openai", "anthropic"} and not name:
            raise ValueError(f"{normalized_provider.title()} agent model profiles require model_name.")
        if base_url and not base_url.startswith(("https://", "http://")):
            raise ValueError("base_url must start with https:// or http://.")
        if normalized_provider == "openai" and base_url:
            normalize_and_validate_runtime_base_url(
                base_url,
                allow_private_network_hosts=get_settings().app_env != "prod",
            )
        settings = get_settings()
        if (
            settings.app_env == "prod"
            and contains_inline_secret(payload, secret_fields=SENSITIVE_FIELDS)
            and not str(os.getenv("CONTROL_PLANE_SECRET_KEY") or "").strip()
        ):
            raise ValueError(
                "Inline agent model secrets are not allowed in production; configure CONTROL_PLANE_SECRET_KEY or use *_ref fields."
            )

    @staticmethod
    def _normalize_profile_config(provider: str, config: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(config or {})
        if str(provider or "").strip().lower() == "openai":
            normalized.setdefault("api_key", None)
            runtime_preset = str(normalized.get("runtime_preset") or "").strip().lower()
            if runtime_preset:
                normalized["runtime_preset"] = runtime_preset
            base_url = str(normalized.get("base_url") or "").strip()
            if base_url:
                normalized["base_url"] = normalize_and_validate_runtime_base_url(
                    base_url,
                    allow_private_network_hosts=get_settings().app_env != "prod",
                )
        return normalized

    @staticmethod
    def _has_secret_reference(config: Dict[str, Any] | None, field: str) -> bool:
        payload = dict(config or {})
        raw_value = payload.get(field)
        if isinstance(raw_value, str) and raw_value.strip():
            return True
        return any(
            isinstance(payload.get(key), str) and str(payload.get(key) or "").strip()
            for key in (f"{field}_ref", f"{field}_encrypted")
        )

    @staticmethod
    def _to_response(record: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if record is None:
            return None
        payload = dict(record.get("payload") or {})
        provider = str(payload.get("provider") or "")
        config = redact_secret_values(dict(payload.get("config") or {}))
        if provider in SUPPORTED_AGENT_MODEL_PROVIDERS and "api_key_configured" not in config:
            config["api_key"] = None
            config["api_key_configured"] = False
        return {
            "model_profile_id": payload.get("model_profile_id") or record.get("resource_id"),
            "name": payload.get("name") or record.get("name") or "",
            "provider": provider,
            "model_name": payload.get("model_name"),
            "status": record.get("status") or payload.get("status") or "active",
            "is_default": bool(payload.get("is_default")),
            "system_managed": bool(payload.get("system_managed")),
            "config": config,
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "model_selection_source": "profile",
        }
