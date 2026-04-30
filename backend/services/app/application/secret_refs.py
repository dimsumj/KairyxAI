from __future__ import annotations

from typing import Any, Dict

from secret_manager_service import SecretManagerService


SENSITIVE_FIELDS = {
    "api_key",
    "api_token",
    "callback_bearer_token",
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
SECRET_REF_SUFFIX = "_ref"
SECRET_ENCRYPTED_SUFFIX = "_encrypted"
SECRET_STORED_INLINE_SUFFIX = "_stored_inline"
SECRET_STORAGE_SUFFIXES = (
    SECRET_REF_SUFFIX,
    SECRET_ENCRYPTED_SUFFIX,
    SECRET_STORED_INLINE_SUFFIX,
)
SECRET_METADATA_SUFFIX = "_configured"


def contains_inline_secret(config: Dict[str, Any], *, secret_fields: set[str] | None = None) -> bool:
    fields = set(secret_fields or REDACTED_FIELDS)
    for key, value in (config or {}).items():
        if isinstance(value, dict) and contains_inline_secret(value, secret_fields=fields):
            return True
        if isinstance(value, list) and any(
            isinstance(item, dict) and contains_inline_secret(item, secret_fields=fields)
            for item in value
        ):
            return True
        if key in fields and _has_secret_value(value):
            return True
    return False


def secure_inline_secret_values(config: Dict[str, Any], *, secret_fields: set[str] | None = None) -> Dict[str, Any]:
    service = SecretManagerService()
    return _secure_node(dict(config or {}), service, set(secret_fields or REDACTED_FIELDS))


def materialize_secret_refs(config: Dict[str, Any], *, secret_fields: set[str] | None = None) -> Dict[str, Any]:
    service = SecretManagerService()
    return _materialize_node(dict(config or {}), service, set(secret_fields or REDACTED_FIELDS))


def redact_secret_values(payload: Any, *, secret_fields: set[str] | None = None) -> Any:
    fields = set(secret_fields or REDACTED_FIELDS)
    if isinstance(payload, dict):
        redacted: Dict[str, Any] = {}
        secret_state: Dict[str, bool] = {}
        for key, value in payload.items():
            base_key = _secret_base_key(key, fields)
            if base_key:
                secret_state[base_key] = secret_state.get(base_key, False) or _has_secret_value(value)
                continue
            redacted[key] = redact_secret_values(value, secret_fields=fields)
        for key, configured in secret_state.items():
            redacted[key] = None
            redacted[f"{key}{SECRET_METADATA_SUFFIX}"] = configured
        return redacted
    if isinstance(payload, list):
        return [redact_secret_values(item, secret_fields=fields) for item in payload]
    return payload


def _secure_node(node: Any, service: SecretManagerService, secret_fields: set[str]) -> Any:
    if isinstance(node, dict):
        secured: Dict[str, Any] = {}
        inline_secret_keys = {
            key
            for key, value in node.items()
            if key in secret_fields and _has_secret_value(value)
        }
        for key, value in node.items():
            base_key = _secret_base_key(key, secret_fields)
            if base_key in inline_secret_keys and key != base_key:
                continue
            if key in secret_fields and _has_secret_value(value):
                secured[f"{key}{SECRET_ENCRYPTED_SUFFIX}"] = service.encrypt_secret_value(value)
                continue
            secured[key] = _secure_node(value, service, secret_fields)
        return secured
    if isinstance(node, list):
        return [_secure_node(item, service, secret_fields) for item in node]
    return node


def _materialize_node(node: Any, service: SecretManagerService, secret_fields: set[str]) -> Any:
    if isinstance(node, dict):
        resolved: Dict[str, Any] = {}
        delayed_values: Dict[str, Any] = {}
        for key, value in node.items():
            base_key = _secret_base_key(key, secret_fields)
            if key.endswith(SECRET_REF_SUFFIX) and base_key:
                resolved[key] = value
                secret_value = service.resolve_secret(str(value))
                if _has_secret_value(secret_value):
                    delayed_values[base_key] = _materialize_node(secret_value, service, secret_fields)
                continue
            if key.endswith(SECRET_ENCRYPTED_SUFFIX) and base_key:
                resolved[key] = value
                secret_value = service.decrypt_secret_value(str(value))
                if _has_secret_value(secret_value):
                    delayed_values[base_key] = _materialize_node(secret_value, service, secret_fields)
                continue
            resolved[key] = _materialize_node(value, service, secret_fields)
        resolved.update(delayed_values)
        return resolved
    if isinstance(node, list):
        return [_materialize_node(item, service, secret_fields) for item in node]
    return node


def _secret_base_key(key: str, secret_fields: set[str]) -> str | None:
    if key in secret_fields:
        return key
    for suffix in SECRET_STORAGE_SUFFIXES:
        if key.endswith(suffix):
            candidate = key[: -len(suffix)]
            if candidate in secret_fields:
                return candidate
    return None


def _has_secret_value(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (bytes, bytearray, list, tuple, set, dict)):
        return bool(value)
    return True
