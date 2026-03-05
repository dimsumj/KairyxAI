# event_semantic_normalizer.py

from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any

from pipeline_models import (
    PIPELINE_SCHEMA_VERSION,
    build_event_fingerprint,
    derive_event_date,
)


class EventSemanticNormalizer:
    """
    Cleans and normalizes raw event data from analytics platforms.

    This component standardizes event names/property keys and performs
    basic cleanup/type coercion with quality flags.
    """

    def __init__(self, event_name_map: Dict[str, str], property_key_map: Dict[str, str]):
        self.event_name_map = event_name_map
        self.property_key_map = property_key_map

    def _normalize_event_name(self, event_name: str) -> str:
        if not event_name:
            return "unknown_event"
        return self.event_name_map.get(str(event_name), str(event_name))

    def _normalize_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(properties, dict):
            return {}

        normalized_props = {}
        for key, value in properties.items():
            new_key = self.property_key_map.get(key, key)
            normalized_props[new_key] = value
        return normalized_props

    def _to_iso(self, ts: Any) -> tuple[str, bool]:
        if ts is None:
            return datetime.utcnow().isoformat(), False
        if isinstance(ts, (int, float)):
            try:
                # support unix seconds and ms
                val = float(ts)
                if val > 1e12:
                    val = val / 1000.0
                return datetime.utcfromtimestamp(val).isoformat(), True
            except Exception:
                return datetime.utcnow().isoformat(), False
        s = str(ts)
        if s.endswith("Z"):
            s = s[:-1]
        try:
            return datetime.fromisoformat(s).isoformat(), True
        except Exception:
            return datetime.utcnow().isoformat(), False

    def _to_float(self, value: Any) -> tuple[float | None, bool]:
        if value is None:
            return None, False
        try:
            return float(value), True
        except Exception:
            return None, False

    def normalize_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not events:
            return []

        player_id_keys = {
            "userId", "user_id", "user_Id", "player_id", "player_Id", "PlayerID", "PlayerId", "uid", "PID"
        }

        normalized_events = []
        for event in events:
            if not isinstance(event, dict):
                continue

            new_event = event.copy()
            quality_flags: list[str] = []

            for key in player_id_keys:
                if key in new_event and new_event.get(key) not in (None, ""):
                    new_event['player_id'] = new_event.pop(key)
                    break

            if not new_event.get("player_id"):
                new_event["player_id"] = "unknown_user"
                quality_flags.append("missing_player_id")

            new_event['event_type'] = self._normalize_event_name(new_event.get('event_type') or new_event.get('event_name'))

            props = self._normalize_properties(new_event.get('event_properties', {}))
            user_props = self._normalize_properties(new_event.get('user_properties', {}))

            event_time_iso, valid_time = self._to_iso(new_event.get("event_time") or new_event.get("timestamp") or new_event.get("time"))
            new_event["event_time"] = event_time_iso
            if not valid_time:
                quality_flags.append("invalid_event_time")

            # revenue coercion
            raw_revenue = props.get("revenue_usd", props.get("revenue", props.get("value")))
            revenue, ok_rev = self._to_float(raw_revenue)
            if raw_revenue is not None and not ok_rev:
                quality_flags.append("malformed_revenue")
            if revenue is not None:
                props["revenue_usd"] = revenue

            # currency normalization
            if props.get("currency") is not None:
                props["currency"] = str(props.get("currency")).upper()

            new_event['event_properties'] = props
            new_event['user_properties'] = user_props
            new_event['data_quality_flags'] = quality_flags
            new_event['schema_version'] = PIPELINE_SCHEMA_VERSION
            new_event['event_date'] = derive_event_date(new_event["event_time"])
            new_event['event_fingerprint'] = build_event_fingerprint(new_event)

            normalized_events.append(new_event)

        print(f"Normalized {len(normalized_events)} events.")
        return normalized_events
