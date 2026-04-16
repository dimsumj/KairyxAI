from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import requests

from app.application.provider_connections import ProviderConnectionService


class SendGridApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 502, payload: Dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = int(status_code)
        self.payload = dict(payload or {})


class SendGridProviderService:
    _DEFAULT_BASE_URL = "https://api.sendgrid.com"
    _MAX_TEMPLATE_PAGE_SIZE = 200

    def __init__(self, repository):
        self.repository = repository
        self.provider_connections = ProviderConnectionService(repository)

    def list_dynamic_templates(self, provider_connection_id: str) -> List[Dict[str, Any]]:
        config = self._resolve_provider_config(provider_connection_id)
        page_token: str | None = None
        collected: List[Dict[str, Any]] = []
        pages_fetched = 0

        while pages_fetched < 10:
            response = self._request(
                "GET",
                config,
                "/v3/templates",
                params={
                    "generations": "dynamic",
                    "page_size": self._MAX_TEMPLATE_PAGE_SIZE,
                    **({"page_token": page_token} if page_token else {}),
                },
            )
            payload = response.json()
            collected.extend(self._template_list_item_to_summary(item) for item in list(payload.get("result") or []))
            page_token = self._extract_page_token(dict(payload.get("_metadata") or {}))
            pages_fetched += 1
            if not page_token:
                break

        return collected

    def get_template_summary(self, provider_connection_id: str, template_id: str) -> Dict[str, Any]:
        config = self._resolve_provider_config(provider_connection_id)
        response = self._request("GET", config, f"/v3/templates/{template_id}")
        payload = response.json()
        return self._template_detail_to_summary(dict(payload or {}))

    def send_templated_mail(
        self,
        provider_connection_id: str,
        *,
        template_id: str,
        personalizations: List[Dict[str, Any]],
        from_email: str | None = None,
        from_name: str | None = None,
        subject: str | None = None,
    ) -> Dict[str, Any]:
        config = self._resolve_provider_config(provider_connection_id)
        sender_email = str(from_email or config.get("from_email") or "").strip()
        sender_name = str(from_name or config.get("from_name") or "").strip()
        if not sender_email:
            raise ValueError("SendGrid provider connection is missing from_email.")
        if not personalizations:
            raise ValueError("At least one personalization is required.")

        payload: Dict[str, Any] = {
            "template_id": str(template_id).strip(),
            "personalizations": list(personalizations),
            "from": {"email": sender_email, **({"name": sender_name} if sender_name else {})},
        }
        if subject:
            payload["subject"] = str(subject)

        response = self._request("POST", config, "/v3/mail/send", json=payload)
        headers = {str(key).lower(): value for key, value in response.headers.items()}
        return {
            "ok": True,
            "status_code": response.status_code,
            "message_id": headers.get("x-message-id") or headers.get("x-message-id".lower()),
        }

    def _resolve_provider_config(self, provider_connection_id: str) -> Dict[str, Any]:
        connection = self.provider_connections.resolve_connection(provider_connection_id)
        provider = str(connection.get("provider") or "").strip().lower()
        if provider != "sendgrid":
            raise ValueError(f"Provider connection '{provider_connection_id}' is not a SendGrid connection.")
        config = dict(connection.get("config") or {})
        api_key = str(config.get("api_key") or "").strip()
        if not api_key:
            raise ValueError(f"Provider connection '{provider_connection_id}' is missing api_key.")
        return config

    def _request(
        self,
        method: str,
        config: Dict[str, Any],
        path: str,
        *,
        params: Dict[str, Any] | None = None,
        json: Dict[str, Any] | None = None,
    ) -> requests.Response:
        base_url = str(config.get("base_url") or self._DEFAULT_BASE_URL).rstrip("/")
        url = f"{base_url}{path}"
        headers = {
            "Authorization": f"Bearer {str(config.get('api_key') or '').strip()}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.request(
                str(method or "GET").upper(),
                url,
                headers=headers,
                params=params,
                json=json,
                timeout=20,
            )
        except requests.Timeout as exc:
            raise SendGridApiError("SendGrid request timed out.", status_code=504) from exc
        except requests.RequestException as exc:
            raise SendGridApiError(
                f"SendGrid request failed: {exc.__class__.__name__}.",
                status_code=502,
            ) from exc

        if response.status_code >= 400:
            raise SendGridApiError(
                self._error_message(response),
                status_code=response.status_code if response.status_code >= 500 else 409,
                payload=self._safe_json(response),
            )
        return response

    @staticmethod
    def _safe_json(response: requests.Response) -> Dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            return {}
        return dict(payload or {}) if isinstance(payload, dict) else {"data": payload}

    def _error_message(self, response: requests.Response) -> str:
        payload = self._safe_json(response)
        errors = payload.get("errors")
        if isinstance(errors, list):
            messages = [str(item.get("message") or "").strip() for item in errors if isinstance(item, dict)]
            messages = [message for message in messages if message]
            if messages:
                return "; ".join(messages[:3])
        text = str(getattr(response, "text", "") or "").strip()
        return text[:400] if text else f"SendGrid request failed with status {response.status_code}."

    @staticmethod
    def _extract_page_token(metadata: Dict[str, Any]) -> str | None:
        next_url = str((metadata or {}).get("next") or "").strip()
        if not next_url:
            return None
        parsed = urlparse(next_url)
        tokens = parse_qs(parsed.query).get("page_token") or []
        token = str(tokens[0]).strip() if tokens else ""
        return token or None

    @classmethod
    def _template_list_item_to_summary(cls, item: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(item or {})
        active_version = cls._select_active_version(list(payload.get("versions") or []))
        return {
            "id": payload.get("id"),
            "name": payload.get("name"),
            "generation": payload.get("generation"),
            "updated_at": payload.get("updated_at"),
            "active_version": cls._version_summary(active_version),
        }

    @classmethod
    def _template_detail_to_summary(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        active_version = cls._select_active_version(list(payload.get("versions") or []))
        return {
            "id": payload.get("id"),
            "name": payload.get("name"),
            "generation": payload.get("generation"),
            "updated_at": payload.get("updated_at"),
            "active_version": cls._version_summary(active_version),
        }

    @staticmethod
    def _select_active_version(versions: List[Dict[str, Any]]) -> Dict[str, Any]:
        version_items = [dict(item or {}) for item in versions]
        active = next((item for item in version_items if int(item.get("active") or 0) == 1), None)
        if active is not None:
            return active
        if not version_items:
            return {}
        return sorted(version_items, key=lambda item: str(item.get("updated_at") or ""), reverse=True)[0]

    @staticmethod
    def _version_summary(version: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(version or {})
        return {
            "id": payload.get("id"),
            "name": payload.get("name"),
            "subject": payload.get("subject"),
            "updated_at": payload.get("updated_at"),
            "active": bool(int(payload.get("active") or 0)),
            "editor": payload.get("editor"),
        }
