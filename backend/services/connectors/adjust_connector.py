from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List

import requests

from .normalizer import canonical_attribution_event
from .parsers import extract_rows


class AdjustConnector:
    connector_type = "adjust"

    def __init__(self, config: Dict[str, Any]):
        self.api_token = config.get("api_token")
        self.api_url = (config.get("api_url") or os.getenv("ADJUST_API_URL") or "").strip()
        self.field_mapping = config.get("field_mapping") or {}

    def health_check(self) -> Dict[str, Any]:
        ok = bool(self.api_token)
        details = "configured" if ok else "missing api_token"
        if ok and self.api_url:
            details += f", api_url={self.api_url}"
        return {"ok": ok, "connector": self.connector_type, "message": details}

    def _mock_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        now = datetime.utcnow().isoformat()
        raw = {
            "player_id": "adjust_user_1001",
            "event_name": "attribution_install",
            "timestamp": now,
            "campaign": "ua_campaign_a",
            "adgroup": "adgroup_1",
            "network": "meta",
            "start_date": start_date,
            "end_date": end_date,
        }
        return [canonical_attribution_event("adjust", raw, self.field_mapping)]

    def fetch_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        if os.getenv("DATA_BACKEND_MODE", "mock").lower() == "mock":
            return self._mock_events(start_date, end_date)

        if not self.api_token:
            raise ValueError("Adjust connector missing api_token")
        if not self.api_url:
            raise ValueError("Adjust connector missing api_url (set in connector config or ADJUST_API_URL)")

        resp = requests.get(
            self.api_url,
            headers={"Authorization": f"Bearer {self.api_token}"},
            params={"start_date": start_date, "end_date": end_date},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = extract_rows(data)
        return [canonical_attribution_event("adjust", r, self.field_mapping) for r in rows]
