from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict
from urllib.parse import urlsplit

import requests

from app.application.ai_runtime_network import normalize_and_validate_runtime_base_url
from app.application.agent_model_profiles import AgentModelProfileService
from app.application.secret_refs import materialize_secret_refs
from app.core.settings import get_settings
from gemini_client import GeminiClient


@dataclass(frozen=True)
class ResolvedTextModelSelection:
    model_profile_id: str | None
    provider: str
    model_name: str
    selection_source: str
    runtime: "ConfiguredTextModelRuntime | None"

    def as_session_selection(self) -> Dict[str, Any]:
        return {
            "model_profile_id": self.model_profile_id,
            "effective_provider": self.provider or "deterministic",
            "effective_model_name": self.model_name,
            "model_selection_source": self.selection_source or "deterministic_fallback",
        }


class ConfiguredTextModelRuntime:
    def __init__(self, profile: Dict[str, Any] | None, *, circuit_namespace: str = "default"):
        self.profile = dict(profile or {})
        self.provider = str(self.profile.get("provider") or "deterministic").strip().lower() or "deterministic"
        self.model_name = str(self.profile.get("model_name") or "").strip()
        self.config = dict(self.profile.get("config") or {})
        self.circuit_namespace = circuit_namespace
        self.gemini_client = self._build_gemini_client()

    def is_enabled(self) -> bool:
        if self.provider == "gemini":
            return self.gemini_client is not None
        if self.provider == "openai":
            return bool(
                self.model_name
                and (
                    str(self.config.get("api_key") or "").strip()
                    or str(self.config.get("base_url") or "").strip()
                )
            )
        if self.provider == "anthropic":
            return bool(str(self.config.get("api_key") or "").strip()) and bool(self.model_name)
        return False

    def request_text(self, payload: Dict[str, Any] | str) -> str:
        prompt = payload if isinstance(payload, str) else json.dumps(payload)
        if self.provider == "gemini" and self.gemini_client is not None:
            return self.gemini_client.get_ai_response(prompt)
        if self.provider == "openai":
            return self._call_openai(prompt)
        if self.provider == "anthropic":
            return self._call_anthropic(prompt)
        return ""

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
                circuit_namespace=self.circuit_namespace,
            )
        except Exception:
            return None

    def _call_openai(self, prompt: str) -> str:
        base_url = self._resolve_openai_base_url()
        if not base_url or not self.model_name:
            return ""
        headers = {"Content-Type": "application/json"}
        api_key = str(self.config.get("api_key") or "").strip()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        response = requests.post(
            self._build_api_url(base_url, "/v1/chat/completions"),
            headers=headers,
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
        if not api_key or not self.model_name:
            return ""
        base_url = str(self.config.get("base_url") or "https://api.anthropic.com").strip().rstrip("/")
        response = requests.post(
            self._build_api_url(base_url, "/v1/messages"),
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

    def _resolve_openai_base_url(self) -> str:
        return normalize_and_validate_runtime_base_url(
            str(self.config.get("base_url") or "https://api.openai.com").strip().rstrip("/"),
            allow_private_network_hosts=get_settings().app_env != "prod",
        )

    @staticmethod
    def _build_api_url(base_url: str, path: str) -> str:
        normalized_base = str(base_url or "").strip().rstrip("/")
        normalized_path = str(path or "").strip()
        if not normalized_base:
            return normalized_path
        split = urlsplit(normalized_base)
        base_path = split.path.rstrip("/")
        if normalized_path.startswith("/v1/") and base_path.endswith("/v1"):
            normalized_path = normalized_path[len("/v1") :]
        return f"{normalized_base}{normalized_path}"


class TextModelRuntimeResolver:
    def __init__(self, repository, *, circuit_namespace: str = "default"):
        self.repository = repository
        self.model_profiles = AgentModelProfileService(repository)
        self.circuit_namespace = circuit_namespace

    def resolve(self, requested_model_profile_id: str | None = None) -> ResolvedTextModelSelection:
        requested_id = str(requested_model_profile_id or "").strip()
        if requested_id:
            profile = self.model_profiles.resolve_profile(requested_id)
            runtime = ConfiguredTextModelRuntime(profile, circuit_namespace=self.circuit_namespace)
            return ResolvedTextModelSelection(
                model_profile_id=str(profile.get("model_profile_id") or "") or None,
                provider=str(profile.get("provider") or "deterministic"),
                model_name=str(profile.get("model_name") or ""),
                selection_source=str(profile.get("model_selection_source") or "profile"),
                runtime=runtime if runtime.is_enabled() else None,
            )
        default_profile = self.model_profiles.get_default_profile()
        if default_profile is not None and not bool(default_profile.get("system_managed")):
            profile = self.model_profiles.resolve_profile(str(default_profile.get("model_profile_id") or ""))
            runtime = ConfiguredTextModelRuntime(profile, circuit_namespace=self.circuit_namespace)
            return ResolvedTextModelSelection(
                model_profile_id=str(profile.get("model_profile_id") or "") or None,
                provider=str(profile.get("provider") or "deterministic"),
                model_name=str(profile.get("model_name") or ""),
                selection_source=str(profile.get("model_selection_source") or "profile"),
                runtime=runtime if runtime.is_enabled() else None,
            )
        legacy_profile = self._resolve_legacy_google_connector_profile()
        if legacy_profile is not None:
            runtime = ConfiguredTextModelRuntime(legacy_profile, circuit_namespace=self.circuit_namespace)
            if runtime.is_enabled():
                return ResolvedTextModelSelection(
                    model_profile_id=None,
                    provider="gemini",
                    model_name=str(legacy_profile.get("model_name") or ""),
                    selection_source="legacy_connector",
                    runtime=runtime,
                )
        profile = self.model_profiles.resolve_profile(None)
        if profile is not None:
            runtime = ConfiguredTextModelRuntime(profile, circuit_namespace=self.circuit_namespace)
            if runtime.is_enabled():
                return ResolvedTextModelSelection(
                    model_profile_id=str(profile.get("model_profile_id") or "") or None,
                    provider=str(profile.get("provider") or "deterministic"),
                    model_name=str(profile.get("model_name") or ""),
                    selection_source=str(profile.get("model_selection_source") or "profile"),
                    runtime=runtime,
                )
        return ResolvedTextModelSelection(
            model_profile_id=None,
            provider="deterministic",
            model_name="",
            selection_source="deterministic_fallback",
            runtime=None,
        )

    def _resolve_legacy_google_connector_profile(self) -> Dict[str, Any] | None:
        google_connectors = [
            {**connector, "config": materialize_secret_refs(dict(connector.get("config") or {}))}
            for connector in self.repository.list_connectors()
            if str(connector.get("type") or "").lower() == "google"
        ]
        google_connectors = [
            connector
            for connector in google_connectors
            if str((connector.get("config") or {}).get("api_key") or "").strip()
        ]
        if not google_connectors:
            return None
        connector = max(
            google_connectors,
            key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""),
        )
        config = dict(connector.get("config") or {})
        return {
            "provider": "gemini",
            "model_name": str(config.get("model_name") or "").strip(),
            "config": {"api_key": str(config.get("api_key") or "").strip()},
            "model_selection_source": "legacy_connector",
        }
