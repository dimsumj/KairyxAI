from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, Mapping, Optional

PIPELINE_SCHEMA_VERSION = "v1"


def utcnow_iso() -> str:
    return datetime.utcnow().isoformat()


def derive_event_date(event_time: Any) -> Optional[str]:
    if event_time is None:
        return None
    text = str(event_time)
    return text[:10] if len(text) >= 10 else None


def build_event_fingerprint(
    event: Mapping[str, Any],
    canonical_user_id: Optional[str] = None,
) -> str:
    props = event.get("event_properties")
    if not isinstance(props, dict):
        props = {}

    identity = (
        canonical_user_id
        or event.get("canonical_user_id")
        or event.get("player_id")
        or "unknown_user"
    )
    payload = {
        "source": str(event.get("source") or "unknown"),
        "identity": str(identity),
        "source_event_id": (
            str(event.get("source_event_id"))
            if event.get("source_event_id") is not None
            else None
        ),
        "event_type": str(event.get("event_type") or event.get("event_name") or "unknown_event"),
        "event_time": str(event.get("event_time") or event.get("timestamp") or ""),
        "campaign": event.get("campaign") or props.get("campaign"),
        "adset": event.get("adset") or props.get("adset"),
        "media_source": event.get("media_source") or props.get("media_source"),
        "transaction_id": (
            props.get("transaction_id")
            or props.get("order_id")
            or props.get("purchase_token")
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass
class ShardManifest:
    job_id: str
    source: str
    gcs_uri: str
    event_count: int
    start_date: str
    end_date: str
    shard_index: int = 1
    source_config_id: str = "default"
    schema_version: str = PIPELINE_SCHEMA_VERSION
    published_at: str = field(default_factory=utcnow_iso)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
