from __future__ import annotations

from typing import Any, Dict

from secret_manager_service import SecretManagerService


SENSITIVE_FIELDS = {
    "api_key",
    "api_token",
    "callback_signing_secret",
    "client_secret",
    "password",
    "secret_key",
    "signing_secret",
    "webhook_token",
}
REDACTED_ONLY_FIELDS = {
    "service_account_info_json",
    "service_account_json",
}
REDACTED_FIELDS = SENSITIVE_FIELDS | REDACTED_ONLY_FIELDS
SECRET_METADATA_SUFFIX = "_configured"


def contains_inline_secret(config: Dict[str, Any]) -> bool:
    for key, value in (config or {}).items():
        if isinstance(value, dict) and contains_inline_secret(value):
            return True
        if isinstance(value, list) and any(isinstance(item, dict) and contains_inline_secret(item) for item in value):
            return True
        if key in SENSITIVE_FIELDS and value not in (None, "", False):
            return True
    return False


def materialize_secret_refs(config: Dict[str, Any]) -> Dict[str, Any]:
    service = SecretManagerService()
    return _materialize_node(dict(config or {}), service)


def redact_secret_values(payload: Any) -> Any:
    if isinstance(payload, dict):
        redacted: Dict[str, Any] = {}
        for key, value in payload.items():
            if key in REDACTED_FIELDS:
                redacted[key] = None
                redacted[f"{key}{SECRET_METADATA_SUFFIX}"] = value not in (None, "", False)
                continue
            redacted[key] = redact_secret_values(value)
        return redacted
    if isinstance(payload, list):
        return [redact_secret_values(item) for item in payload]
    return payload


def _materialize_node(node: Any, service: SecretManagerService) -> Any:
    if isinstance(node, dict):
        resolved: Dict[str, Any] = {}
        for key, value in node.items():
            if key.endswith("_ref"):
                resolved[key] = value
                raw_target = key[: -len("_ref")]
                secret_value = service.resolve_secret(str(value))
                if secret_value not in (None, ""):
                    resolved[raw_target] = secret_value
                continue
            resolved[key] = _materialize_node(value, service)
        return resolved
    if isinstance(node, list):
        return [_materialize_node(item, service) for item in node]
    return node
