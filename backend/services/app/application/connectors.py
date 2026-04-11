from __future__ import annotations

from connectors.bigquery_connector import BigQueryConnector
from typing import Any, Dict, List

from connectors import create_connector
from app.application.secret_refs import (
    REDACTED_FIELDS,
    contains_inline_secret,
    materialize_secret_refs,
    redact_secret_values,
    secure_inline_secret_values,
)
from app.core.errors import ResourceLockedError
from app.core.settings import get_settings


CONNECTOR_SECRET_FIELDS = REDACTED_FIELDS


class ConnectorService:
    def __init__(self, repository):
        self.repository = repository

    def list_connectors(self) -> List[Dict[str, Any]]:
        return [self._to_response(item) for item in self.repository.list_connectors()]

    def create_connector(self, name: str, connector_type: str, config: Dict[str, Any], connector_id: str | None = None) -> Dict[str, Any]:
        settings = get_settings()
        self._validate_connector_config(connector_type, config)
        config = self._persist_inline_secrets(config)
        if settings.app_env == "prod" and contains_inline_secret(config, secret_fields=CONNECTOR_SECRET_FIELDS):
            raise ValueError(
                "Inline connector secrets are not allowed in production; configure CONTROL_PLANE_SECRET_KEY or use *_ref fields."
            )
        return self._to_response(
            self.repository.upsert_connector(name=name, connector_type=connector_type, config=config, connector_id=connector_id)
        )

    def delete_connector(self, name: str) -> bool:
        blocking_jobs = [
            job["id"]
            for job in self.repository.list_import_jobs()
            if name in {
                str(job.get("source_name") or ""),
                str((job.get("spec") or {}).get("source_name") or ""),
                str((job.get("spec") or {}).get("connector_id") or ""),
            }
            and str(job.get("status") or "").lower() not in {"completed", "cancelled"}
        ]
        if blocking_jobs:
            raise ResourceLockedError(
                f"Connector '{name}' is locked by import jobs: {', '.join(sorted(blocking_jobs)[:5])}."
            )
        return self.repository.delete_connector(name)

    def health_check(self, name: str) -> Dict[str, Any]:
        connector_record = self.repository.get_connector(name)
        if connector_record is None:
            raise KeyError(name)
        connector = create_connector(connector_record["type"], materialize_secret_refs(connector_record["config"]))
        health = connector.health_check()
        return {
            "tenant_id": connector_record.get("tenant_id"),
            "project_id": connector_record.get("project_id"),
            "connector_id": connector_record.get("connector_id"),
            "name": connector_record["name"],
            "type": connector_record["type"],
            "ok": bool(health.get("ok", False)),
            "message": health.get("message"),
        }

    def list_tables(self, name: str) -> Dict[str, Any]:
        connector_record = self.repository.get_connector(name)
        if connector_record is None:
            raise KeyError(name)
        connector = create_connector(connector_record["type"], materialize_secret_refs(connector_record["config"]))
        if not hasattr(connector, "list_tables"):
            raise ValueError(f"Connector '{name}' does not support table discovery.")
        return {
            "tenant_id": connector_record.get("tenant_id"),
            "project_id": connector_record.get("project_id"),
            "connector_id": connector_record.get("connector_id"),
            "name": connector_record["name"],
            "type": connector_record["type"],
            "items": list(connector.list_tables()),
        }

    @staticmethod
    def _validate_connector_config(connector_type: str, config: Dict[str, Any]) -> None:
        if str(connector_type or "").strip().lower() != "bigquery":
            return
        project_id = str((config or {}).get("project_id") or "").strip()
        dataset_id = str((config or {}).get("dataset_id") or "").strip()
        if not project_id or not dataset_id:
            raise ValueError("BigQuery connectors require project_id and dataset_id.")
        raw_service_account = (config or {}).get("service_account_json")
        if raw_service_account in (None, ""):
            raw_service_account = (config or {}).get("service_account_info_json")
        if raw_service_account in (None, ""):
            return
        service_account_info = BigQueryConnector.parse_service_account_info(raw_service_account)
        missing_fields = [
            field_name
            for field_name in ("client_email", "private_key", "token_uri")
            if not str(service_account_info.get(field_name) or "").strip()
        ]
        if missing_fields:
            raise ValueError(
                "BigQuery service account JSON is missing required fields: "
                + ", ".join(missing_fields)
                + "."
            )
        service_account_type = str(service_account_info.get("type") or "").strip()
        if service_account_type and service_account_type != "service_account":
            raise ValueError("BigQuery service account JSON must use type 'service_account'.")

    @staticmethod
    def _persist_inline_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return secure_inline_secret_values(config, secret_fields=CONNECTOR_SECRET_FIELDS)
        except RuntimeError as exc:
            if contains_inline_secret(config, secret_fields=CONNECTOR_SECRET_FIELDS):
                raise ValueError(
                    "Secure connector secret storage is not configured; set CONTROL_PLANE_SECRET_KEY or use *_ref fields."
                ) from exc
            raise

    @staticmethod
    def _to_response(connector_record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(connector_record or {})
        payload["config"] = redact_secret_values(dict(payload.get("config") or {}))
        return payload
