from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List

import requests

from .normalizer import canonical_attribution_event


class AppsFlyerConnector:
    connector_type = "appsflyer"

    def __init__(self, config: Dict[str, Any]):
        self.api_token = config.get("api_token")
        self.app_id = config.get("app_id")
        self.pull_api_url = (config.get("pull_api_url") or os.getenv("APPSFLYER_PULL_API_URL") or "").strip()

    def health_check(self) -> Dict[str, Any]:
        ok = bool(self.api_token and self.app_id)
        details = "configured" if ok else "missing api_token/app_id"
        if ok and self.pull_api_url:
            details += f", pull_api_url={self.pull_api_url}"
        return {"ok": ok, "connector": self.connector_type, "message": details}

    def _mock_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        now = datetime.utcnow().isoformat()
        raw = {
            "customer_user_id": "af_user_2001",
            "event_name": "attribution_install",
            "timestamp": now,
            "campaign_name": "af_campaign_x",
            "adset_name": "set_7",
            "media_source": "google_ads",
            "app_id": self.app_id or "demo_app",
            "start_date": start_date,
            "end_date": end_date,
        }
        return [canonical_attribution_event("appsflyer", raw)]

    def fetch_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        if os.getenv("DATA_BACKEND_MODE", "mock").lower() == "mock":
            return self._mock_events(start_date, end_date)

        if not self.api_token or not self.app_id:
            raise ValueError("AppsFlyer connector missing api_token/app_id")
        if not self.pull_api_url:
            raise ValueError("AppsFlyer connector missing pull_api_url (set in connector config or APPSFLYER_PULL_API_URL)")

        resp = requests.get(
            self.pull_api_url,
            headers={"Authorization": f"Bearer {self.api_token}"},
            params={"app_id": self.app_id, "from": start_date, "to": end_date},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = data if isinstance(data, list) else data.get("data", [])
        return [canonical_attribution_event("appsflyer", r) for r in rows]
