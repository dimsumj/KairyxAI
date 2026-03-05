from __future__ import annotations

from typing import Any, Dict, List, Protocol


class BaseConnector(Protocol):
    connector_type: str

    def health_check(self) -> Dict[str, Any]:
        ...

    def fetch_events(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        ...
