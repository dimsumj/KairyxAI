from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List

import requests

from .normalizer import canonical_attribution_event
from .parsers import extract_rows


class AppsFlyerConnector:
    connector_type = "appsflyer"

    def __init__(self, config: Dict[str, Any]):
        self.api_token = config.get("api_token")
        self.app_id = config.get("app_id")
        self.pull_api_url = (config.get("pull_api_url") or os.getenv("APPSFLYER_PULL_API_URL") or "").strip()
        self.field_mapping = config.get("field_mapping") or {}

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
        return [canonical_attribution_event("appsflyer", raw, self.field_mapping)]

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
        rows = extract_rows(data)
        return [canonical_attribution_event("appsflyer", r, self.field_mapping) for r in rows]

    def fetch_events_page(
        self,
        start_date: str,
        end_date: str,
        cursor: str | None = None,
        page_size: int | None = None,
    ) -> Dict[str, Any]:
        page_number = int(cursor or "0")
        rows = self.fetch_events(start_date, end_date)
        size = max(1, int(page_size or len(rows) or 1))
        start = page_number * size
        page = rows[start:start + size]
        next_cursor = str(page_number + 1) if (start + size) < len(rows) else None
        return {"events": page, "next_cursor": next_cursor, "has_more": next_cursor is not None}

    def iter_event_pages(self, start_date: str, end_date: str, page_size: int | None = None):
        cursor = None
        while True:
            page = self.fetch_events_page(start_date, end_date, cursor=cursor, page_size=page_size)
            if not page["events"]:
                break
            yield page["events"]
            if not page["has_more"]:
                break
            cursor = page["next_cursor"]
