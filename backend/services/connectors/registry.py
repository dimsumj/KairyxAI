from __future__ import annotations

from typing import Any, Dict

from .amplitude_connector import AmplitudeConnector
from .adjust_connector import AdjustConnector
from .appsflyer_connector import AppsFlyerConnector


def create_connector(connector_type: str, config: Dict[str, Any]):
    ctype = (connector_type or "").lower()
    if ctype == "amplitude":
        return AmplitudeConnector(config)
    if ctype == "adjust":
        return AdjustConnector(config)
    if ctype == "appsflyer":
        return AppsFlyerConnector(config)
    raise ValueError(f"Unsupported connector type: {connector_type}")
