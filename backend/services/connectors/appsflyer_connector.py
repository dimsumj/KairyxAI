from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List


class AppsFlyerConnector:
    connector_type = "appsflyer"

    def __init__(self, config: Dict[str, Any]):
        self.api_token = config.get("api_token")
        self.app_id = config.get("app_id")

    def health_check(self) -> Dict[str, Any]:
        ok = bool(self.api_token and self.app_id)
        return {
            "ok": ok,
            "connector": self.connector_type,
            "message": "configured" if ok else "missing api_token/app_id",
        }

    def fetch_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        # Local-demo friendly mock payload; replace with real AppsFlyer Pull API in prod.
        if os.getenv("DATA_BACKEND_MODE", "mock").lower() == "mock":
            now = datetime.utcnow().isoformat()
            return [
                {
                    "player_id": "af_user_2001",
                    "event_type": "attribution_install",
                    "event_time": now,
                    "source": "appsflyer",
                    "event_properties": {
                        "campaign": "af_campaign_x",
                        "adset": "set_7",
                        "media_source": "google_ads",
                        "app_id": self.app_id or "demo_app",
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                }
            ]
        if not self.api_token or not self.app_id:
            raise ValueError("AppsFlyer connector missing api_token/app_id")
        # Placeholder for real API integration path
        return []
