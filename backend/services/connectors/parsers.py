from __future__ import annotations

from typing import Any, Dict, List


def extract_rows(payload: Any) -> List[Dict[str, Any]]:
    """Best-effort extractor for common API response shapes."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    # common wrappers
    for key in ["data", "results", "items", "rows", "installs", "events"]:
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]

    # nested data.records
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ["rows", "records", "items", "events"]:
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]

    # single dict event fallback
    return [payload]
