from __future__ import annotations

from datetime import datetime
from typing import Any, Dict


def to_iso(ts: Any) -> str:
    if ts is None:
        return datetime.utcnow().isoformat()
    if isinstance(ts, (int, float)):
        return datetime.utcfromtimestamp(float(ts)).isoformat()
    s = str(ts)
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return datetime.fromisoformat(s).isoformat()
    except Exception:
        return datetime.utcnow().isoformat()


def canonical_attribution_event(source: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    player_id = raw.get("player_id") or raw.get("customer_user_id") or raw.get("appsflyer_id") or raw.get("idfa") or raw.get("adid") or "unknown_user"
    event_type = raw.get("event_type") or raw.get("event_name") or raw.get("name") or "attribution_event"
    event_time = to_iso(raw.get("event_time") or raw.get("timestamp") or raw.get("install_time") or raw.get("time"))

    props = {
        "campaign": raw.get("campaign") or raw.get("campaign_name"),
        "adset": raw.get("adset") or raw.get("adset_name") or raw.get("adgroup"),
        "media_source": raw.get("media_source") or raw.get("network") or raw.get("channel"),
        "raw": raw,
    }

    return {
        "player_id": str(player_id),
        "event_type": str(event_type),
        "event_time": event_time,
        "source": source,
        "event_properties": props,
    }
