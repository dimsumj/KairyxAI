from __future__ import annotations

from typing import Any, Dict, List

import requests

from app.application.provider_connections import ProviderConnectionService


class BrazeApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 502, payload: Dict[str, Any] | None = None):
        super().__init__(message)
        self.status_code = int(status_code)
        self.payload = dict(payload or {})


class BrazeProviderService:
    _MAX_CAMPAIGN_PAGES = 10
    _PAGE_SIZE = 100

    def __init__(self, repository):
        self.repository = repository
        self.provider_connections = ProviderConnectionService(repository)

    def list_api_campaigns(self, provider_connection_id: str) -> List[Dict[str, Any]]:
        config = self._resolve_provider_config(provider_connection_id)
        collected: List[Dict[str, Any]] = []
        for page in range(self._MAX_CAMPAIGN_PAGES):
            response = self._request(
                "GET",
                config,
                "/campaigns/list",
                params={
                    "page": page,
                    "include_archived": "false",
                    "sort_direction": "desc",
                },
            )
            payload = self._safe_json(response)
            campaigns = list(payload.get("campaigns") or [])
            collected.extend(
                self._campaign_list_item_to_summary(item)
                for item in campaigns
                if bool((item or {}).get("is_api_campaign"))
            )
            if len(campaigns) < self._PAGE_SIZE:
                break
        return collected

    def get_campaign_summary(self, provider_connection_id: str, campaign_id: str) -> Dict[str, Any]:
        config = self._resolve_provider_config(provider_connection_id)
        response = self._request(
            "GET",
            config,
            "/campaigns/details",
            params={"campaign_id": str(campaign_id or "").strip()},
        )
        summary = self._campaign_detail_to_summary(self._safe_json(response))
        if not summary.get("is_api_campaign"):
            raise ValueError("Braze email campaigns require an API-triggered campaign.")
        return summary

    def send_campaign(
        self,
        provider_connection_id: str,
        *,
        campaign_id: str,
        recipients: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        config = self._resolve_provider_config(provider_connection_id)
        if not recipients:
            raise ValueError("At least one Braze recipient is required.")
        response = self._request(
            "POST",
            config,
            "/campaigns/trigger/send",
            json={
                "campaign_id": str(campaign_id or "").strip(),
                "broadcast": False,
                "recipients": list(recipients),
            },
        )
        payload = self._safe_json(response)
        return {
            "ok": True,
            "status_code": response.status_code,
            "dispatch_id": payload.get("dispatch_id"),
            "message": payload.get("message"),
            "notice": payload.get("notice"),
        }

    def _resolve_provider_config(self, provider_connection_id: str) -> Dict[str, Any]:
        connection = self.provider_connections.resolve_connection(provider_connection_id)
        provider = str(connection.get("provider") or "").strip().lower()
        if provider != "braze":
            raise ValueError(f"Provider connection '{provider_connection_id}' is not a Braze connection.")
        config = dict(connection.get("config") or {})
        api_key = str(config.get("api_key") or "").strip()
        rest_endpoint = str(config.get("rest_endpoint") or "").strip()
        if not api_key:
            raise ValueError(f"Provider connection '{provider_connection_id}' is missing api_key.")
        if not rest_endpoint:
            raise ValueError(f"Provider connection '{provider_connection_id}' is missing rest_endpoint.")
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
        base_url = str(config.get("rest_endpoint") or "").rstrip("/")
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
            raise BrazeApiError("Braze request timed out.", status_code=504) from exc
        except requests.RequestException as exc:
            raise BrazeApiError(
                f"Braze request failed: {exc.__class__.__name__}.",
                status_code=502,
            ) from exc

        if response.status_code >= 400:
            raise BrazeApiError(
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
        message = str(payload.get("message") or "").strip()
        if message and message.lower() != "success":
            return message
        errors = payload.get("errors")
        if isinstance(errors, list):
            messages = [str(item).strip() for item in errors if str(item).strip()]
            if messages:
                return "; ".join(messages[:3])
        text = str(getattr(response, "text", "") or "").strip()
        return text[:400] if text else f"Braze request failed with status {response.status_code}."

    @staticmethod
    def _campaign_list_item_to_summary(item: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(item or {})
        return {
            "id": payload.get("id"),
            "name": payload.get("name"),
            "updated_at": payload.get("last_edited"),
            "asset_type": "braze_campaign",
            "is_api_campaign": bool(payload.get("is_api_campaign")),
            "tags": list(payload.get("tags") or []),
        }

    @staticmethod
    def _campaign_detail_to_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": payload.get("id"),
            "name": payload.get("name"),
            "updated_at": payload.get("last_edited"),
            "asset_type": "braze_campaign",
            "is_api_campaign": bool(payload.get("is_api_campaign")),
            "tags": list(payload.get("tags") or []),
            "description": payload.get("description"),
            "draft": bool(payload.get("draft")),
            "archived": bool(payload.get("archived")),
        }
