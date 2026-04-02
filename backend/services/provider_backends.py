from __future__ import annotations

import os
from typing import Literal

from runtime_paths import normalize_env_text


DataBackendMode = Literal["mock", "gcp", "aws"]
WarehouseBackend = Literal["mock", "bigquery", "redshift"]
ObjectStorageBackend = Literal["mock", "gcs", "s3"]
MessageBackend = Literal["mock", "pubsub", "eventbridge_sqs"]
SecretBackend = Literal["env", "gcp_secret_manager", "aws_secrets_manager"]


def _normalized(value: str | None) -> str:
    return normalize_env_text(value).strip().lower()


def resolve_data_backend_mode(raw_value: str | None = None) -> str:
    value = _normalized(raw_value if raw_value is not None else os.getenv("DATA_BACKEND_MODE", "mock"))
    if value in {"", "mock"}:
        return "mock"
    if value in {"gcp", "google", "google_cloud"}:
        return "gcp"
    if value in {"aws", "amazon", "amazon_web_services"}:
        return "aws"
    raise ValueError("DATA_BACKEND_MODE must be 'mock', 'gcp', or 'aws'.")


def _resolve_backend(explicit_env: str, mapping: dict[str, str]) -> str:
    explicit = _normalized(os.getenv(explicit_env))
    if explicit:
        return explicit
    return mapping[resolve_data_backend_mode()]


def resolve_warehouse_backend() -> str:
    backend = _resolve_backend(
        "WAREHOUSE_BACKEND",
        {"mock": "mock", "gcp": "bigquery", "aws": "redshift"},
    )
    if backend not in {"mock", "bigquery", "redshift"}:
        raise ValueError("WAREHOUSE_BACKEND must be 'mock', 'bigquery', or 'redshift'.")
    return backend


def resolve_object_storage_backend() -> str:
    backend = _resolve_backend(
        "OBJECT_STORAGE_BACKEND",
        {"mock": "mock", "gcp": "gcs", "aws": "s3"},
    )
    if backend not in {"mock", "gcs", "s3"}:
        raise ValueError("OBJECT_STORAGE_BACKEND must be 'mock', 'gcs', or 's3'.")
    return backend


def resolve_message_backend() -> str:
    backend = _resolve_backend(
        "MESSAGE_BACKEND",
        {"mock": "mock", "gcp": "pubsub", "aws": "eventbridge_sqs"},
    )
    if backend not in {"mock", "pubsub", "eventbridge_sqs"}:
        raise ValueError("MESSAGE_BACKEND must be 'mock', 'pubsub', or 'eventbridge_sqs'.")
    return backend


def resolve_secret_backend() -> str:
    explicit = _normalized(os.getenv("SECRET_BACKEND"))
    if explicit:
        backend = explicit
    else:
        mode = resolve_data_backend_mode()
        if mode == "gcp":
            backend = "gcp_secret_manager"
        elif mode == "aws":
            backend = "aws_secrets_manager"
        else:
            backend = "env"
    if backend not in {"env", "gcp_secret_manager", "aws_secrets_manager"}:
        raise ValueError("SECRET_BACKEND must be 'env', 'gcp_secret_manager', or 'aws_secrets_manager'.")
    return backend


def backend_is_aws() -> bool:
    return (
        resolve_warehouse_backend() == "redshift"
        or resolve_object_storage_backend() == "s3"
        or resolve_message_backend() == "eventbridge_sqs"
        or resolve_secret_backend() == "aws_secrets_manager"
    )

