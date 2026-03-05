from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List


class AdjustConnector:
    connector_type = "adjust"

    def __init__(self, config: Dict[str, Any]):
        self.api_token = config.get("api_token")

    def health_check(self) -> Dict[str, Any]:
        return {"ok": bool(self.api_token), "connector": self.connector_type, "message": "configured" if self.api_token else "missing api_token"}

    def fetch_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        # Local-demo friendly mock payload; replace with real Adjust API ingestion in prod.
        if os.getenv("DATA_BACKEND_MODE", "mock").lower() == "mock":
            now = datetime.utcnow().isoformat()
            return [
                {
                    "player_id": "adjust_user_1001",
                    "event_type": "attribution_install",
                    "event_time": now,
                    "source": "adjust",
                    "event_properties": {
                        "campaign": "ua_campaign_a",
                        "adgroup": "adgroup_1",
                        "network": "meta",
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                }
            ]
        if not self.api_token:
            raise ValueError("Adjust connector missing api_token")
        # Placeholder for real API integration path
        return []
