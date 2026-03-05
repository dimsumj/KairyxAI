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
