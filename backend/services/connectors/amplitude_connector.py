from __future__ import annotations

from typing import Any, Dict, List
from amplitude_service import AmplitudeService


class AmplitudeConnector:
    connector_type = "amplitude"

    def __init__(self, config: Dict[str, Any]):
        self.client = AmplitudeService(
            api_key=config.get("api_key"),
            secret_key=config.get("secret_key"),
        )

    def health_check(self) -> Dict[str, Any]:
        return {"ok": True, "connector": self.connector_type}

    def fetch_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        return self.client.export_events(start_date, end_date)

    def iter_event_pages(self, start_date: str, end_date: str, page_size: int | None = None):
        yield from self.client.iter_export_event_pages(start_date, end_date, page_size=page_size or 1000)

    def fetch_events_page(
        self,
        start_date: str,
        end_date: str,
        cursor: str | None = None,
        page_size: int | None = None,
    ) -> Dict[str, Any]:
        page_index = int(cursor or "0")
        for index, page in enumerate(self.iter_event_pages(start_date, end_date, page_size=page_size), start=0):
            if index == page_index:
                next_cursor = str(index + 1) if len(page) >= max(1, int(page_size or 1000)) else None
                return {
                    "events": page,
                    "next_cursor": next_cursor,
                    "has_more": next_cursor is not None,
                }
        return {"events": [], "next_cursor": None, "has_more": False}
