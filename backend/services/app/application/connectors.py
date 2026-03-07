from __future__ import annotations

from typing import Any, Dict, List

from connectors import create_connector


class ConnectorService:
    def __init__(self, repository):
        self.repository = repository

    def list_connectors(self) -> List[Dict[str, Any]]:
        return self.repository.list_connectors()

    def create_connector(self, name: str, connector_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        return self.repository.upsert_connector(name=name, connector_type=connector_type, config=config)

    def delete_connector(self, name: str) -> bool:
        return self.repository.delete_connector(name)

    def health_check(self, name: str) -> Dict[str, Any]:
        connector_record = self.repository.get_connector(name)
        if connector_record is None:
            raise KeyError(name)
        connector = create_connector(connector_record["type"], connector_record["config"])
        health = connector.health_check()
        return {
            "name": connector_record["name"],
            "type": connector_record["type"],
            "ok": bool(health.get("ok", False)),
            "message": health.get("message"),
        }
