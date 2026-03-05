from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pipeline_models import (
    PIPELINE_SCHEMA_VERSION,
    build_event_fingerprint,
    derive_event_date,
)


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


def _get_path(raw: Dict[str, Any], path: str) -> Any:
    cur: Any = raw
    for part in (path or "").split('.'):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _pick(raw: Dict[str, Any], keys: List[str], override_path: Optional[str] = None) -> Any:
    if override_path:
        v = _get_path(raw, override_path)
        if v is not None:
            return v
    for k in keys:
        v = raw.get(k)
        if v is not None:
            return v
    return None


def canonical_attribution_event(source: str, raw: Dict[str, Any], field_mapping: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    mapping = field_mapping or {}

    player_id = _pick(raw, ["player_id", "user_id", "uid", "PID", "customer_user_id", "appsflyer_id", "idfa", "adid"], mapping.get("canonical_user_id")) or "unknown_user"
    event_type = _pick(raw, ["event_type", "event_name", "name"], mapping.get("event_name")) or "attribution_event"
    event_time = to_iso(_pick(raw, ["event_time", "timestamp", "install_time", "time"], mapping.get("event_time")))
    source_event_id = _pick(raw, ["event_id", "id", "insert_id", "uuid"], mapping.get("source_event_id"))

    props = {
        "campaign": _pick(raw, ["campaign", "campaign_name"], mapping.get("campaign")),
        "adset": _pick(raw, ["adset", "adset_name", "adgroup"], mapping.get("adset")),
        "media_source": _pick(raw, ["media_source", "network", "channel"], mapping.get("media_source")),
        "raw": raw,
    }

    event = {
        "player_id": str(player_id),
        "event_type": str(event_type),
        "event_time": event_time,
        "event_date": derive_event_date(event_time),
        "source": source,
        "source_event_id": str(source_event_id) if source_event_id is not None else None,
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "event_properties": props,
    }
    event["event_fingerprint"] = build_event_fingerprint(event)
    return event
