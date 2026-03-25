from __future__ import annotations

from typing import Any, Dict, List

from connectors import create_connector
from app.application.secret_refs import contains_inline_secret, materialize_secret_refs, redact_secret_values
from app.core.errors import ResourceLockedError
from app.core.settings import get_settings


class ConnectorService:
    def __init__(self, repository):
        self.repository = repository

    def list_connectors(self) -> List[Dict[str, Any]]:
        return [self._to_response(item) for item in self.repository.list_connectors()]

    def create_connector(self, name: str, connector_type: str, config: Dict[str, Any], connector_id: str | None = None) -> Dict[str, Any]:
        settings = get_settings()
        if settings.app_env == "prod" and contains_inline_secret(config):
            raise ValueError("Inline connector secrets are not allowed in production; use *_ref fields.")
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

    @staticmethod
    def _to_response(connector_record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(connector_record or {})
        payload["config"] = redact_secret_values(dict(payload.get("config") or {}))
        return payload
